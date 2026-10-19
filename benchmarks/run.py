"""Reproducible local baseline: ACK latency, getrusage CPU/peak RSS, sampled queue depth.
Writes JSON evidence. Use a fresh process/data directory per workload.
"""
import argparse
import json
import os
from pathlib import Path
import platform
import socket
import subprocess
import tempfile
import time

p=argparse.ArgumentParser()
p.add_argument('--build',default='build')
p.add_argument('--seconds',type=int,default=5)
p.add_argument('--clients',type=int,nargs='+',default=[100,500,1000])
p.add_argument('--output',default='docs/benchmark_results.json')
a=p.parse_args()
build=Path(a.build).resolve()
results=[]

def metrics(port):
    with socket.create_connection(('127.0.0.1',port),.5) as s:
        s.sendall(b'GET /metrics HTTP/1.1\r\n\r\n');s.settimeout(.5);data=b''
        while True:
            b=s.recv(8192)
            if not b: break
            data+=b
    return {k:float(v) for k,v in (line.split() for line in data.decode().split('\r\n\r\n',1)[1].splitlines() if line and not line.startswith('#') and '{' not in line)}

for clients in a.clients:
    with tempfile.TemporaryDirectory() as tmp:
        with socket.socket() as s:
            s.bind(('127.0.0.1',0));port=s.getsockname()[1]
        cfg=Path(tmp)/'node.conf'
        cfg.write_text(f'id=benchmark\nlisten=127.0.0.1:{port}\nleader=127.0.0.1:{port}\ndata={tmp}/data\nworkers=4\nqueue=256\nsync=always\nrotate=10000\n')
        with (Path(tmp)/'server.log').open('w') as log:
            server=subprocess.Popen([str(build/'server'),str(cfg)],stdout=log,stderr=log)
            try:
                for _ in range(100):
                    try: metrics(port);break
                    except OSError:time.sleep(.05)
                load=subprocess.Popen([str(build/'load_generator'),f'127.0.0.1:{port}',str(clients),str(a.seconds),'10'],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
                samples=[]
                previous=None
                while load.poll() is None:
                    try:
                        m=metrics(port)
                        cpu=None
                        if previous:
                            elapsed=m['monitor_process_uptime_seconds']-previous['monitor_process_uptime_seconds']
                            if elapsed>0:cpu=100*(m['monitor_process_cpu_seconds_total']-previous['monitor_process_cpu_seconds_total'])/elapsed
                        samples.append({'server_cpu_percent':cpu,'server_max_rss_bytes':m['monitor_process_max_rss_bytes'],'queue_depth':m['monitor_queue_depth']})
                        previous=m
                    except (OSError,ValueError,subprocess.CalledProcessError):pass
                    time.sleep(.2)
                out,err=load.communicate()
                if load.returncode:raise RuntimeError(err or out)
                row=json.loads(out)
                row.update({'hz_per_client':10,'padding':0,'burst':1,'sync':'always','workers':4,'queue_capacity':256,'samples':samples,'final_metrics':metrics(port)})
                results.append(row)
                print(json.dumps({k:v for k,v in row.items() if k not in ('samples','final_metrics')}),flush=True)
            finally:
                server.terminate();server.wait(timeout=10)
report={'platform':platform.platform(),'machine':platform.machine(),'logical_cpus':os.cpu_count(),'compiler':subprocess.check_output(['c++','--version'],text=True).splitlines()[0],'build':'Release','network':'loopback','method':'one server; stop-and-wait client threads; latency of successful ACK attempts only; getrusage CPU/peak RSS and HTTP queue samples every ~200ms; sync=always; no host failure simulated','results':results}
Path(a.output).write_text(json.dumps(report,indent=2)+'\n')
