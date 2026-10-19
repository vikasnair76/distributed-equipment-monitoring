"""Real process/socket faults; no log-only success assertions. Standard library only."""
import concurrent.futures
import os
from pathlib import Path
import socket
import subprocess
import sys
import socketserver
import threading
import tempfile
import time

SERVER, DEVICE = map(os.path.abspath, sys.argv[1:3])

def port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]

def request(p, message, timeout=3):
    with socket.create_connection(('127.0.0.1', p), timeout) as s:
        s.settimeout(timeout)
        s.sendall((message+'\n').encode())
        result = b''
        while b'\n' not in result:
            chunk = s.recv(8192)
            if not chunk:
                raise ConnectionError('EOF')
            result += chunk
        return result.split(b'\n')[0].decode()

def eventually(check, seconds=8):
    end = time.monotonic()+seconds
    last = None
    while time.monotonic() < end:
        try:
            if check():
                return
        except (OSError, AssertionError, IndexError) as e:
            last = e
        time.sleep(.03)
    raise AssertionError(f'condition timed out: {last}')

def telemetry(seq, device='machine', temp=70):
    return f'TELE {device} {seq} {temp} 30 2000 RUNNING'

with tempfile.TemporaryDirectory(prefix='monitor-integration-') as temp:
    root = Path(temp)
    ports = [port() for _ in range(4)]
    processes, logs, devices = {}, [], []
    partition = threading.Event()
    class Proxy(socketserver.StreamRequestHandler):
        def handle(self):
            line = self.rfile.readline(4097)
            if partition.is_set():
                return
            time.sleep(.01)  # deterministic transit delay
            try:
                response = request(ports[0], line.decode().strip())
                self.wfile.write((response+'\n').encode())
            except OSError:
                pass
    proxy = socketserver.TCPServer(('127.0.0.1', 0), Proxy)
    proxy_thread = threading.Thread(target=proxy.serve_forever)
    proxy_thread.start()
    def start(i, delay=0, workers=3, queue=8):
        cfg = root/f'{i}.conf'
        leader_port = proxy.server_address[1] if i == 2 else (ports[0] if i<3 else ports[i])
        cfg.write_text(f'id=node{i}\nlisten=127.0.0.1:{ports[i]}\nleader=127.0.0.1:{leader_port}\ndata={root/i.__str__()}\npeers='+','.join(f'127.0.0.1:{p}' for p in ports[:3] if p!=ports[i])+f'\nworkers={workers}\nqueue={queue}\nrotate=5\nheartbeat_ms=50\nsuspect_ms=200\ndelay_ms={delay}\n')
        output = open(root/f'{i}.log', 'ab')
        logs.append(output)
        processes[i] = subprocess.Popen([SERVER, str(cfg)], stdout=output, stderr=output)
        eventually(lambda: request(ports[i], 'PING').startswith('PONG'))
    def stop(i, kill=False):
        p = processes.pop(i)
        p.kill() if kill else p.terminate()
        p.wait(timeout=10)
        assert kill or p.returncode == 0, p.returncode
    try:
        for i in range(3): start(i)
        # framing, strict parsing, duplicate/out-of-order rules
        with socket.create_connection(('127.0.0.1',ports[0])) as s:
            f=s.makefile('rb')
            s.sendall(b'TELE machine 1 70 ')
            time.sleep(.03)
            s.sendall(b'30 2000 RUNNING\n'+(telemetry(3)+'\n'+telemetry(2)+'\n').encode())
            assert f.readline().decode().startswith('ACK machine 1 ')
            assert f.readline().decode().startswith('ACK machine 3 ')
            assert f.readline().decode().startswith('ACK machine 3 ')
        assert request(ports[0], 'TELE bad 1 nan 30 1 RUNNING').startswith('ERR')
        assert request(ports[1],telemetry(4)).startswith('ACK machine 4 ')
        eventually(lambda: all(request(p,'STATE machine').split()[2]=='4' for p in ports[:3]))
        # Live leader remains reachable, but a deterministic proxy partitions replica C.
        partition.set()
        assert request(ports[2],telemetry(1,'partitioned')).startswith('RETRY')
        assert request(ports[0],'STATE partitioned')=='MISSING'
        assert request(ports[0],telemetry(1,'partitioned')).startswith('ACK')
        partition.clear()
        eventually(lambda: request(ports[2],'STATE partitioned').startswith('TELE partitioned 1 '))
        # Abrupt replica failure and restart with catch-up.
        stop(2, True)
        assert request(ports[1],telemetry(5)).startswith('ACK machine 5 ')
        start(2)
        eventually(lambda: request(ports[2],'STATE machine').split()[2]=='5')
        # Disconnection after submitting work cannot redirect a response to a reused descriptor.
        for seq in range(6,26):
            s=socket.create_connection(('127.0.0.1',ports[0]));s.sendall((telemetry(seq)+'\n').encode());s.close()
            assert request(ports[0],'PING').startswith('PONG')
        assert request(ports[0],telemetry(30)).startswith('ACK machine 30 ')
        # Leader crash: no follower may acknowledge writes; previously durable ACK survives.
        stop(0,True)
        for p in ports[1:3]: assert request(p,telemetry(31)).startswith('RETRY')
        start(0)
        assert request(ports[0],'STATE machine').split()[2]=='30'
        assert request(ports[2],telemetry(31)).startswith('ACK machine 31 ')
        # Persisted command IDs are monotonic, duplicates return latest command.
        assert request(ports[0],telemetry(32,temp=95)).endswith('SLOW_DOWN')
        assert request(ports[0],telemetry(31)).endswith('32 SLOW_DOWN')
        assert request(ports[0],'CACK machine 31').startswith('ERR')
        assert request(ports[1],'CACK machine 32')=='OK'
        assert request(ports[0],telemetry(33,temp=100)).endswith('SHUTDOWN')
        # Real simulator survives ingress loss, keeps stable identity and advances sequence.
        state=root/'device.state'
        dlog=open(root/'device.log','ab');logs.append(dlog)
        d=subprocess.Popen([DEVICE,'sim',','.join(f'127.0.0.1:{p}' for p in [ports[1],ports[2],ports[0]]),str(state),'20','20'],stdout=dlog,stderr=dlog);devices.append(d)
        eventually(lambda: request(ports[0],'STATE sim').startswith('TELE sim '))
        before=int(request(ports[0],'STATE sim').split()[2]);stop(1,True)
        eventually(lambda: int(request(ports[0],'STATE sim').split()[2])>before)
        d.wait(timeout=15);assert d.returncode==0
        saved=state.read_text().split();assert int(saved[0])>int(saved[1]);start(1)
        stopped_state=root/'stopped.state'
        stopped_state.write_text('11 10 SHUTDOWN\n')
        latched=subprocess.run([DEVICE,'latched',f'127.0.0.1:{ports[0]}',str(stopped_state),'20','1'],capture_output=True,timeout=3)
        assert latched.returncode==0
        assert request(ports[0],'STATE latched')=='MISSING'
        # Bounded queue under deterministic processing delay and overload.
        start(3, delay=30,workers=1,queue=2)
        def flood(i): return request(ports[3],telemetry(1,'flood'+str(i)),10)
        with concurrent.futures.ThreadPoolExecutor(max_workers=24) as executor:
            replies=list(executor.map(flood,range(24)))
        assert all(r.startswith('ACK ') for r in replies)
        with socket.create_connection(('127.0.0.1',ports[3])) as s:
            s.sendall(b'GET /metrics HTTP/1.1\r\n\r\n');s.settimeout(3);data=b''
            while True:
                b=s.recv(8192)
                if not b:break
                data+=b
        assert b'monitor_queue_depth ' in data
        depth=int(data.split(b'monitor_queue_depth ')[1].split(b'\n')[0]);assert depth<=2
        # Truncated WAL tail recovers, corrupt complete record fails closed.
        stop(3)
        wal=root/'3'/'events.wal'
        with wal.open('ab') as f:f.write(b'partial')
        start(3)
        assert request(ports[3],'STATE flood23').startswith('TELE flood23 1 ')
        assert request(ports[3],telemetry(1,'corrupt')).startswith('ACK ')
        stop(3)
        target=wal if wal.stat().st_size else root/'3'/'checkpoint'
        raw=bytearray(target.read_bytes());raw[-1]^=1;target.write_bytes(raw)
        cfg=root/'3.conf'
        bad=subprocess.run([SERVER,str(cfg)],capture_output=True,timeout=5)
        assert bad.returncode!=0 and b'corrupt_wal_checksum' in bad.stderr
        # Graceful shutdown under active connections; accepted writes remain replayable.
        sockets=[socket.create_connection(('127.0.0.1',ports[0])) for _ in range(20)]
        for i,s in enumerate(sockets):s.sendall((telemetry(1,'shutdown'+str(i))+'\n').encode())
        stop(0)
        for s in sockets:s.close()
        start(0)
        assert request(ports[0],'STATE machine').split()[2]=='33'
        print('integration: framing, stale/duplicate events, forwarding, delayed proxy partition/heal, replica kill/rejoin, leader refusal/recovery, command ACKs, device failover, overload, WAL corruption, active shutdown PASS')
    finally:
        for d in devices:
            if d.poll() is None:d.terminate();d.wait(timeout=5)
        for i in list(processes):stop(i)
        for f in logs:f.close()
        proxy.shutdown();proxy.server_close();proxy_thread.join()
