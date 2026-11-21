"""Opt-in, real systemd/Fleet/Ray/Docker/relay node upgrade acceptance.

Requires passwordless sudo and explicit companion binaries. All services, files,
credentials and workloads are test-owned; production units are never touched.
"""
import base64
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.skipif(os.environ.get("RUN_FLEET_NODE_UPGRADE") != "1",
    reason="requires isolated systemd cluster and companion binaries")

SERVER = r'''
import base64, hashlib, json, struct, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
jobs = []
class App(BaseHTTPRequestHandler):
    def log_message(self, *args): pass
    def reply(self, data):
        data=json.dumps(data).encode(); self.send_response(200)
        self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
    def do_POST(self):
        self.rfile.read(int(self.headers.get('Content-Length',0)))
        if self.headers.get('Authorization') != 'Bearer business-key': self.send_error(403);return
        jobs.append(len(jobs)+1);self.reply(jobs)
    def do_GET(self):
        if self.path == '/health': self.reply('ok');return
        if self.headers.get('Authorization') != 'Bearer business-key': self.send_error(403);return
        if self.path == '/events':
            self.send_response(200);self.send_header('Content-Type','text/event-stream');self.end_headers()
            try:
                i=0
                while True:
                    self.wfile.write(('data: %d\n\n'%i).encode());self.wfile.flush();i+=1;time.sleep(.05)
            except (BrokenPipeError,ConnectionResetError): pass
        elif self.path == '/ws':
            key=self.headers['Sec-WebSocket-Key']+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11'
            self.send_response(101);self.send_header('Upgrade','websocket');self.send_header('Connection','Upgrade')
            self.send_header('Sec-WebSocket-Accept',base64.b64encode(hashlib.sha1(key.encode()).digest()).decode());self.end_headers()
            while True:
                header=self.rfile.read(2)
                if len(header)!=2:break
                size=header[1]&127
                if header[0]&15==8:break
                if size>=126:break
                mask=self.rfile.read(4);data=self.rfile.read(size)
                data=bytes(c^mask[i%4] for i,c in enumerate(data))
                self.wfile.write(bytes([129,len(data)])+data);self.wfile.flush()
        else:self.reply(jobs)
ThreadingHTTPServer(('0.0.0.0',8080),App).serve_forever()
'''


def port():
    with socket.socket() as s:
        s.bind(('127.0.0.1',0));return s.getsockname()[1]


def wait(fn, seconds=90):
    deadline=time.monotonic()+seconds
    while time.monotonic()<deadline:
        try:
            value=fn()
            if value:return value
        except (OSError,ValueError,urllib.error.URLError):pass
        time.sleep(.2)
    raise AssertionError('isolated node upgrade cluster did not become ready')


def http(url, token='', data=None, method=None, headers=None):
    request=urllib.request.Request(url, data=json.dumps(data).encode() if data is not None else None,
        headers={'Authorization':'Bearer '+token,'Content-Type':'application/json',**(headers or {})},method=method)
    try:
        response=urllib.request.urlopen(request,timeout=60)
    except urllib.error.HTTPError as error:
        error.msg=error.reason+': '+error.read().decode(errors='replace')
        raise
    with response as r:
        body=r.read()
        if not body:return None
        try:return json.loads(body)
        except ValueError:return body.decode()


def test_upgrade_and_rollback_preserve_runtime_sse_websocket_and_job():
    ray = None
    fleet_bin=Path(os.environ['TRUSTED_FLEET_TEST_BIN'])
    shim_bin=Path(os.environ['TRUSTED_SHIM_TEST_BINARY'])
    router_bin=Path(os.environ['TRUSTED_ROUTER_TEST_BINARY'])
    with tempfile.TemporaryDirectory(prefix='fleet-upgrade-') as directory:
        root=Path(directory);prefix=root/'node';prefix.mkdir()
        (prefix/'bin').mkdir();(prefix/'etc').mkdir()
        shutil.copy2(fleet_bin/'fleet-node',prefix/'bin/fleet-node')
        shutil.copytree(ROOT/'bridge',prefix/'bridge',ignore=shutil.ignore_patterns('__pycache__','.pytest_cache'))
        (prefix/'venv').symlink_to(Path(os.sys.executable).parent.parent)
        processes=[];logs=[];units=[];stream=None;ws=None;reader=None;executor=None
        errors=[];events=[];finish=threading.Event()
        suffix=secrets.token_hex(5);node_unit=f'fleet-upgrade-node-{suffix}.service'
        shim_unit=f'fleet-upgrade-shim-{suffix}.service'
        def write(name, value):
            path=root/name;path.write_text(value);path.chmod(0o600);return path
        def spawn(name, command, env=None):
            log=open(root/(name+'.log'),'w');logs.append(log)
            process=subprocess.Popen([str(x) for x in command],stdout=log,stderr=log,env=env)
            processes.append(process);return process
        def unit(name, command, env):
            text='[Unit]\nDescription=Isolated Fleet upgrade acceptance\n[Service]\nType=simple\n'
            text+=f'User={os.getuid()}\nKillMode=control-group\nTimeoutStopSec=30\n'
            text+='ExecStart='+ ' '.join(json.dumps(str(x)) for x in command)+'\n'
            for key,value in env.items():text+='Environment='+json.dumps(key+'='+value)+'\n'
            path=write(name,text)
            subprocess.run(['sudo','-n','install','-m','644',str(path),'/run/systemd/system/'+name],check=True)
            units.append(name)
            subprocess.run(['sudo','-n','systemctl','daemon-reload'],check=True)
            subprocess.run(['sudo','-n','systemctl','start',name],check=True)
        def pid(name):return int(subprocess.check_output(['systemctl','show','--property=MainPID','--value',name]))
        def inspect(name):return json.loads(subprocess.check_output(['docker','inspect',name]))[0]
        saved_env={k:os.environ.get(k) for k in ['RAY_AUTH_TOKEN','RAY_AUTH_MODE','RAY_AUTH_TOKEN_PATH']}
        try:
            ray_secret=write('ray.secret',secrets.token_hex(32))
            os.environ.pop('RAY_AUTH_TOKEN',None);os.environ['RAY_AUTH_MODE']='token';os.environ['RAY_AUTH_TOKEN_PATH']=str(ray_secret)
            import ray
            from fleet_bridge import shim
            from ray._raylet import AuthenticationTokenLoader
            AuthenticationTokenLoader.instance().reset_cache()
            control,api_port,gateway_port,relay_port=[port() for _ in range(4)]
            api=f'http://127.0.0.1:{api_port}';gateway=f'http://127.0.0.1:{gateway_port}';relay=f'http://127.0.0.1:{relay_port}'
            cluster=write('cluster.secret',secrets.token_hex(32))
            config=write('fleet.yaml',yaml.safe_dump({'control_addr':f'127.0.0.1:{control}','http_addr':f'127.0.0.1:{api_port}',
                'data_dir':str(root/'scheduler'),'cluster_token_file':str(cluster),'heartbeat_secs':1,'node_stale_secs':30}))
            spawn('scheduler',[fleet_bin/'fleet-scheduler','--config',config])
            wait(lambda:http(api+'/healthz'))
            admin=(root/'scheduler/admin.token').read_text().strip()
            namespace=http(api+'/namespaces',admin,{'name':'node-upgrade','quota':{'gpus':0,'cpus':4}})['id']
            def credential(name, permissions):
                token=secrets.token_hex(48);identity=secrets.token_hex(16)
                http(api+'/tokens',admin,{'id':identity,'address':secrets.token_hex(20),'namespace':namespace,
                    'permissions':permissions,'token_hash':hashlib.sha256(token.encode()).hexdigest()})
                return identity,write(name+'.token',token)
            sync_id,sync_file=credential('sync',['router_sync'])
            _,delegate=credential('delegate',['read','router_delegate'])
            http(api+'/routers/upgrade',admin,{'issuer':'upgrade-test','token_id':sync_id},'PUT')
            http(api+'/routers/upgrade/access-policy',admin,{'namespace':namespace,'workload':'*','service':'http',
                'version':1,'sha256':'ab'*32,'enabled':True},'PUT')
            private=root/'gateway.key';public=root/'gateway.pub'
            subprocess.run(['openssl','genpkey','-algorithm','ED25519','-out',str(private)],check=True)
            private.chmod(0o600)
            subprocess.run(['openssl','pkey','-in',str(private),'-pubout','-out',str(public)],check=True)
            node_claim={'iss':'upgrade-test','aud':'trusted-runtime-node/v1','sub':socket.gethostname(),
                'namespaces':[namespace],'nbf':int(time.time()),'exp':int(time.time())+3600}
            node_token=subprocess.check_output([str(router_bin),'enroll-node',str(private)],input=json.dumps(node_claim).encode()).decode().strip()
            node_file=write('node.jwt',node_token)
            env={**os.environ,'TRUSTED_GATEWAY_FLEET_URL':api,'TRUSTED_GATEWAY_RELAY_URL':relay,
                'TRUSTED_GATEWAY_ISSUER':'upgrade-test','TRUSTED_GATEWAY_SIGNING_KEY_FILE':str(private),
                'TRUSTED_GATEWAY_PUBLIC_KEY_FILE':str(public),'TRUSTED_GATEWAY_FLEET_TOKEN_FILE':str(delegate),
                'TRUSTED_GATEWAY_ROUTER_ID':'upgrade','TRUSTED_ROUTER_FLEET_URL':api,
                'TRUSTED_ROUTER_ID':'upgrade','TRUSTED_ROUTER_TOKEN_FILE':str(sync_file)}
            spawn('router',[router_bin,'relay',f'127.0.0.1:{relay_port}',root/'routes.db',public,'upgrade-test'],env)
            spawn('gateway',[router_bin,'gateway',f'127.0.0.1:{gateway_port}'],env)
            wait(lambda:http(api+'/routers/upgrade',admin).get('applied_authorization_revision')==1)
            apps=prefix/'etc/apps.yaml';state=str(root/'shim')
            apps.write_text(yaml.safe_dump({'log_dir':str(root/'logs'),'shim_binary':str(shim_bin),'shim_state_dir':state,
                'shim_daemon':True,'shim_network':{'gateway_url':gateway,'relay_url':relay.replace('http:','ws:'),
                'token_file':str(node_file),'public_key_file':str(public),'issuer':'upgrade-test'},'apps':[]}))
            batches=write('batches.yaml','batches: {}\n')
            unit(shim_unit,[shim_bin,'--serve','--state-dir',state,'--node-id',socket.gethostname()],{})
            wait(lambda:Path(state,'control.sock').exists())
            shim_pid=pid(shim_unit)
            ray.init(address='local',num_cpus=2,num_gpus=0,include_dashboard=False,object_store_memory=80*1024*1024,
                _node_ip_address='127.0.0.1',_temp_dir=str(root/'ray'),namespace='zkm-fleet',log_to_driver=False)
            ray_address='127.0.0.1:'+ray.get_runtime_context().gcs_address.rsplit(':',1)[1]
            node_args=[prefix/'bin/fleet-node','--scheduler',f'http://127.0.0.1:{control}',
                '--cluster-token-file',cluster,'--name',socket.gethostname(),'--apps',apps,'--batches',batches,
                '--jobs-dir',root/'jobs','--bridge',prefix/'bridge','--python',os.sys.executable,'--ray-address',ray_address]
            unit(node_unit,node_args,{'RAY_AUTH_MODE':'token','RAY_AUTH_TOKEN_PATH':str(ray_secret),'RUST_LOG':'info'})
            wait(lambda:any(n.get('report',{}).get('ready',n.get('ready',False)) for n in http(api+'/status',admin).get('nodes',[])))
            name='upgrade-web-'+suffix
            path=api+'/apps/'+name+'?namespace='+namespace
            http(api+'/apps',admin,{'name':name,'namespace':namespace,'spec':{'kind':'app','image':'python:3.12-slim',
                'runtime':{'executor':'shim','driver':'docker'},'services':['http'],'port_base':0,'container_port':8080,
                'entrypoint':'python3','command':['-u','-c',SERVER.replace('{','{{').replace('}','}}')],
                'cpus':1,'gpus':0,'memory_gb':.125,'replicas':1,'health_path':'/health','ready_timeout_s':60,'stop_timeout_s':1}})
            http(api+'/apps/'+name+'/disabled?namespace='+namespace,admin,method='DELETE')
            view=wait(lambda:(v if v.get('runtime_instances') else None) if (v:=http(path,admin)) else None)
            execution=view['runtime_instances'][0]['execution']
            status=shim.daemon_call(state,socket.gethostname(),{'operation':'inspect','execution':execution})
            before=inspect(status['runtime_id'])
            reservations={key for key,value in ray.util.placement_group_table().items() if value['state']=='CREATED'}
            available_cpus=ray.available_resources().get('CPU',0)
            connection=http(gateway+'/v1/runtime/connections',delegate.read_text().strip(),{
                'service':{'namespace':namespace,'workload':name,'service':'http'},
                'identity':{'principal':hashlib.sha256(b'film-workspace').hexdigest(),'session':hashlib.sha256(b'upgrade-test').hexdigest()}})[0]
            route=connection['route_id'];headers={'X-Trusted-Access':connection['token']}
            wait(lambda:(http(relay+'/v1/ready/'+route,connection['token']),True)[1])
            base=connection['base_url']
            assert http(base+'/jobs','business-key',{},headers=headers)==[1]
            request=urllib.request.Request(base+'/events',headers={'Authorization':'Bearer business-key',**headers})
            stream=urllib.request.urlopen(request,timeout=3)
            def read_stream():
                try:
                    while not finish.is_set():
                        line=stream.readline()
                        if not line:raise AssertionError('SSE disconnected')
                        if line.startswith(b'data: '):
                            number=int(line[6:]);assert number==len(events);events.append(number)
                except Exception as error:
                    if not finish.is_set():errors.append(error)
            reader=threading.Thread(target=read_stream,daemon=True);reader.start()
            ws=socket.create_connection(('127.0.0.1',relay_port),timeout=3)
            ws.sendall((f'GET /v1/http/{route}/ws HTTP/1.1\r\nHost: 127.0.0.1:{relay_port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
                f'Sec-WebSocket-Key: {base64.b64encode(secrets.token_bytes(16)).decode()}\r\nSec-WebSocket-Version: 13\r\n'
                f'Authorization: Bearer business-key\r\nX-Trusted-Access: {connection["token"]}\r\n\r\n').encode())
            handshake=b''
            while not handshake.endswith(b'\r\n\r\n'):handshake+=ws.recv(1)
            assert b' 101 ' in handshake
            def echo_ws():
                data=b'upgrade-continuity';mask=secrets.token_bytes(4)
                ws.sendall(bytes([129,128+len(data)])+mask+bytes(c^mask[i%4] for i,c in enumerate(data)))
                assert ws.recv(2,socket.MSG_WAITALL)==bytes([129,len(data)])
                assert ws.recv(len(data),socket.MSG_WAITALL)==data
            wait(lambda:len(events)>5);echo_ws()
            ctl_env={**os.environ,'FLEET_PREFIX':str(prefix),'FLEET_CTL_ENV':str(root/'unused.env'),
                'FLEET_ETC':str(prefix/'etc'),'FLEET_NODE_SSH':'','FLEET_NODE_UNIT':node_unit}
            ctl=['bash',str(ROOT/'deploy/fleet-ctl.sh'),'node']
            old_pid=pid(node_unit)
            result=subprocess.run([*ctl,'upgrade',str(fleet_bin/'fleet-node'),str(ROOT/'bridge')],env=ctl_env,capture_output=True,text=True,timeout=120)
            assert result.returncode==0,result.stderr
            assert pid(node_unit)!=old_pid
            echo_ws()
            wait(lambda:http(path,admin).get('runtime_instances'))
            upgraded_pid=pid(node_unit)
            result=subprocess.run([*ctl,'rollback'],env=ctl_env,capture_output=True,text=True,timeout=120)
            assert result.returncode==0,result.stderr
            assert pid(node_unit)!=upgraded_pid
            wait(lambda:http(path,admin).get('runtime_instances'));echo_ws()
            # Rotate the node bootstrap credential on disk and stay connected
            # across the shim's 30-second renewal interval.
            node_claim['nbf']=int(time.time());node_claim['exp']=int(time.time())+3600
            replacement=subprocess.check_output([str(router_bin),'enroll-node',str(private)],input=json.dumps(node_claim).encode()).decode().strip()
            rotated=write('node.next',replacement);rotated.replace(node_file)
            deadline=time.monotonic()+35
            while time.monotonic()<deadline:
                assert not errors,errors
                echo_ws();time.sleep(.5)
            assert len(events)>100 and not errors
            assert pid(shim_unit)==shim_pid
            after=inspect(status['runtime_id'])
            assert (after['Id'],after['State']['Pid'],after['State']['StartedAt'])==(before['Id'],before['State']['Pid'],before['State']['StartedAt'])
            assert http(path,admin)['runtime_instances'][0]['execution']==execution
            assert {key for key,value in ray.util.placement_group_table().items() if value['state']=='CREATED'}==reservations
            assert ray.available_resources().get('CPU',0)==available_cpus
            assert http(base+'/jobs','business-key',headers=headers)==[1]
            # An incompatible candidate cannot stop the node or affect traffic.
            incompatible=root/'incompatible';(incompatible/'fleet_bridge').mkdir(parents=True)
            for module in ['__init__','node','shim']:(incompatible/'fleet_bridge'/f'{module}.py').write_text('')
            current_pid=pid(node_unit)
            rejected=subprocess.run([*ctl,'upgrade',str(fleet_bin/'fleet-node'),str(incompatible)],env=ctl_env,capture_output=True,text=True,timeout=20)
            assert rejected.returncode!=0 and pid(node_unit)==current_pid
            echo_ws()
            assert not errors
        finally:
            finish.set()
            if ws:ws.close()
            if reader:reader.join(timeout=3)
            if stream:stream.close()
            if node_unit in units:
                subprocess.run(['sudo','-n','systemctl','stop',node_unit],check=False)
            if ray is not None and ray.is_initialized():
                try:
                    from fleet_bridge.node import NodeExecutor
                    from fleet_bridge import reconcile
                    executor=NodeExecutor(str(prefix/'etc/apps.yaml'),str(root/'batches.yaml'),str(root/'jobs'))
                    executor._remove_replicas(list(reconcile.replica_specs(reconcile.load_apps(yaml.safe_load((prefix/'etc/apps.yaml').read_text())))))
                except Exception as error:
                    errors.append(error)
                finally:
                    if executor:executor.pool.shutdown()
                    ray.shutdown()
            for name in reversed(units):
                journal=subprocess.run(['journalctl','-u',name,'--no-pager','-n','200'],capture_output=True,text=True)
                (root/(name+'.log')).write_text(journal.stdout)
                subprocess.run(['sudo','-n','systemctl','stop',name],check=False)
                subprocess.run(['sudo','-n','rm','-f','/run/systemd/system/'+name],check=False)
                subprocess.run(['sudo','-n','systemctl','reset-failed',name],capture_output=True,check=False)
            subprocess.run(['sudo','-n','systemctl','daemon-reload'],check=False)
            for process in reversed(processes):
                process.terminate()
                try:process.wait(timeout=10)
                except subprocess.TimeoutExpired:process.kill();process.wait()
            for log in logs:log.close()
            if artifact_dir := os.environ.get('FLEET_UPGRADE_ARTIFACTS'):
                destination=Path(artifact_dir);destination.mkdir(parents=True,exist_ok=True)
                for log in root.glob('*.log'):shutil.copy2(log,destination/log.name)
            for key,value in saved_env.items():
                if value is None:os.environ.pop(key,None)
                else:os.environ[key]=value
