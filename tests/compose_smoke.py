"""Manual Compose smoke test. Run against this project's own test deployment only.
Environment controls Compose host ports/project; leaves all services running.
"""
import json
import os
from pathlib import Path
import socket
import subprocess
import time
import urllib.request

project=os.environ.get('COMPOSE_PROJECT_NAME','equipment-validation')
a=int(os.environ.get('MONITOR_A_PORT','8080'))
b=int(os.environ.get('MONITOR_B_PORT','8081'))
c=int(os.environ.get('MONITOR_C_PORT','8082'))
prom=int(os.environ.get('PROMETHEUS_PORT','9090'))
def compose(*args):subprocess.run(['docker','compose','-p',project,*args],check=True,capture_output=True)
def get(port,path):
    with urllib.request.urlopen(f'http://127.0.0.1:{port}{path}',timeout=3) as r:return r.read().decode()
def metrics(port):return {k:float(v) for k,v in (line.split() for line in get(port,'/metrics').splitlines() if line and not line.startswith('#') and '{' not in line)}
def state():
    with socket.create_connection(('127.0.0.1',a),2) as s:
        s.settimeout(2);s.sendall(b'STATE demo\n');return int(s.makefile('rb').readline().split()[2])
def wait(check,timeout=20):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        try:
            if check():return
        except (OSError,ValueError,IndexError):pass
        time.sleep(.1)
    raise AssertionError('smoke test timed out')
compose('restart','device-simulator')
wait(lambda: metrics(b)['monitor_active_connections']>=2)
initial=state();before=metrics(c)['monitor_failover_events_total']
started=time.monotonic();compose('stop','monitor-b')
try:
    wait(lambda: metrics(c)['monitor_failover_events_total']>before and state()>initial)
    failover_ms=(time.monotonic()-started)*1000
finally:
    compose('start','monitor-b')
wait(lambda: metrics(b)['monitor_connected_nodes']==2)
wait(lambda: len(json.loads(get(prom,'/api/v1/targets'))['data']['activeTargets'])==3)
wait(lambda: all(t['health']=='up' for t in json.loads(get(prom,'/api/v1/targets'))['data']['activeTargets']))
report={'all_three_nodes_healthy':True,'replica_ingress_stop_then_device_advanced':True,'stop_command_to_observed_progress_ms':failover_ms,'prometheus_targets_up':3,'ports':[a,b,c,prom],'note':'Includes docker stop duration, device cadence, and 100ms polling; not a pure network failover latency benchmark.'}
Path('docs/deployment_validation.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
