"""Kill/restart a real server during a six-second 100-client load."""
import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time

build=Path(sys.argv[1] if len(sys.argv)>1 else 'build').resolve()
def rpc(port,line):
    with socket.create_connection(('127.0.0.1',port),1) as s:
        s.settimeout(1);s.sendall((line+'\n').encode());b=b''
        while b'\n' not in b:
            chunk=s.recv(8192)
            if not chunk:raise OSError('EOF')
            b+=chunk
        return b.split(b'\n')[0].decode()
with tempfile.TemporaryDirectory() as tmp:
    with socket.socket() as s:s.bind(('127.0.0.1',0));port=s.getsockname()[1]
    cfg=Path(tmp)/'node.conf'
    cfg.write_text(f'id=recovery\nlisten=127.0.0.1:{port}\nleader=127.0.0.1:{port}\ndata={tmp}/data\nsync=always\n')
    log=open(Path(tmp)/'server.log','w')
    def start():return subprocess.Popen([str(build/'server'),str(cfg)],stdout=log,stderr=log)
    def wait():
        end=time.monotonic()+10
        while time.monotonic()<end:
            try:
                if rpc(port,'PING').startswith('PONG'):return
            except OSError:pass
            time.sleep(.01)
        raise RuntimeError('restart failed')
    server=start()
    load=None
    try:
        wait();assert rpc(port,'TELE durable_probe 1 70 30 2000 RUNNING').startswith('ACK')
        load=subprocess.Popen([str(build/'load_generator'),f'127.0.0.1:{port}','100','6','20'],stdout=subprocess.PIPE,text=True)
        time.sleep(2);killed=time.monotonic();server.kill();server.wait();time.sleep(.3)
        restarted=time.monotonic();server=start();wait();ready=time.monotonic()
        preserved=rpc(port,'STATE durable_probe').startswith('TELE durable_probe 1 ')
        assert preserved
        out,_=load.communicate(timeout=10);assert load.returncode==0
        result={'workload':json.loads(out),'kill_to_ready_ms':(ready-killed)*1000,'restart_to_ready_ms':(ready-restarted)*1000,'intentional_downtime_seconds':.3,'pre_crash_durable_probe_preserved':preserved,'method':'SIGKILL with active traffic; same data directory; PING polled every 10ms; client reconnect samples measure first failed attempt until next successful ACK'}
        Path('docs/recovery_results.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
    finally:
        if load and load.poll() is None:load.terminate();load.wait()
        server.terminate();server.wait(timeout=10);log.close()
