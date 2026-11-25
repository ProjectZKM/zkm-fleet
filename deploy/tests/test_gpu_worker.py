"""apps/gpu-worker.sh against a stand-in server and worker; no container runs."""
import http.server
import os
from pathlib import Path
import signal
import socket
import subprocess
import threading
import time
import urllib.request

import pytest

ENTRYPOINT = Path(__file__).resolve().parents[2] / 'apps/gpu-worker.sh'


def script(path, body):
    path.write_text('#!/bin/bash\n' + body)
    path.chmod(0o755)
    return path


@pytest.fixture
def box(tmp_path):
    """A stand-in server on PATH that runs until told to stop, its readiness endpoint,
    which answers ready once `ready` exists, and the process on a free health port."""
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir()
    script(bin_dir / 'zkm-gpu-server', f'''echo "server SHARD_SIZE=$SHARD_SIZE port=$ZIREN_SERVER_PORT" >> {tmp_path}/events
trap 'echo server-stopped >> {tmp_path}/events; exit 0' TERM
while [ ! -e {tmp_path}/server-fails ]; do sleep 0.05; done
exit 3
''')
    script(bin_dir / 'sleep', 'exec /bin/sleep 0.05\n')
    worker = script(tmp_path / 'worker', f'''env | sort > {tmp_path}/worker-env
echo "worker-started $*" >> {tmp_path}/events
trap 'echo worker-stopped >> {tmp_path}/events; exit 0' TERM
while [ ! -e {tmp_path}/worker-ends ]; do /bin/sleep 0.05; done
exit 7
''')
    class Ready(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            while (tmp_path / 'silent').exists():  # takes the request and does not answer
                time.sleep(0.05)
            ready = self.path == '/twirp/api.ProverService/Ready' and (tmp_path / 'ready').exists()
            self.send_response(200)
            self.send_header('Content-Type', 'application/protobuf')
            self.end_headers()
            self.wfile.write(b'\x08\x01' if ready else b'')

        def log_message(self, *args):
            pass

    gpu = http.server.HTTPServer(('127.0.0.1', 0), Ready)
    threading.Thread(target=gpu.serve_forever, daemon=True).start()
    with socket.socket() as free:
        free.bind(('127.0.0.1', 0))
        port = free.getsockname()[1]
    env = {'PATH': f'{bin_dir}:{os.environ["PATH"]}', 'HOME': '/home/worker', 'SHARD_SIZE': '16777216',
           'ZIREN_SERVER_PORT': str(gpu.server_port),
           'FLEET_APP': 'ethproofs', 'FLEET_APP_ELF_ID': 'a' * 64, 'WORKER_THREADS': '48', 'HEALTH_PORT': str(port)}
    process = subprocess.Popen([ENTRYPOINT, worker, '--block-interval', '1'], env=env, start_new_session=True,
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    yield tmp_path, process, env
    try:  # outside a container nothing ends what the entrypoint leaves behind
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    gpu.shutdown()


def until(condition, what):
    deadline = time.monotonic() + 10
    while not condition():
        assert time.monotonic() < deadline, what
        time.sleep(0.02)


def events(tmp_path):
    path = tmp_path / 'events'
    return path.read_text().split('\n')[:-1] if path.exists() else []


def healthy(env):
    try:
        with urllib.request.urlopen(f'http://127.0.0.1:{env["HEALTH_PORT"]}/healthz', timeout=2) as response:
            return response.status == 200
    except OSError:
        return False


def test_the_worker_starts_after_the_server_is_ready_with_its_own_environment(box):
    tmp_path, process, env = box
    until(lambda: events(tmp_path), 'the server starts')
    time.sleep(0.3)
    assert events(tmp_path) == [f'server SHARD_SIZE=16777216 port={env["ZIREN_SERVER_PORT"]}']  # no worker before the server is ready
    assert not healthy(env)
    (tmp_path / 'ready').touch()
    until(lambda: healthy(env), 'health answers')
    assert events(tmp_path)[1] == 'worker-started --block-interval 1'
    worker_env = dict(line.split('=', 1) for line in (tmp_path / 'worker-env').read_text().splitlines())
    for name in ('PWD', 'SHLVL', '_'):  # the stand-in worker's own shell
        worker_env.pop(name, None)
    assert worker_env == {'PATH': env['PATH'], 'HOME': '/home/worker', 'FLEET_APP': 'ethproofs',
                          'FLEET_APP_ELF_ID': 'a' * 64, 'FLEET_GPU_ENDPOINT': f'http://127.0.0.1:{env["ZIREN_SERVER_PORT"]}/twirp/',
                          'RAYON_NUM_THREADS': '48'}

    process.send_signal(signal.SIGTERM)  # the worker is told first; the server outlives it
    assert process.wait(timeout=10) == 0
    assert events(tmp_path)[2:] == ['worker-stopped', 'server-stopped']


def test_a_server_that_ends_before_it_is_ready_starts_no_worker(box):
    tmp_path, process, _ = box
    (tmp_path / 'server-fails').touch()
    assert process.wait(timeout=10) == 1
    assert 'ended before it was ready' in process.stdout.read()
    assert not (tmp_path / 'worker-env').exists()


def test_a_server_that_takes_the_probe_and_never_answers_is_not_ready(box):
    tmp_path, process, _ = box
    (tmp_path / 'silent').touch()
    try:
        until(lambda: events(tmp_path), 'the server starts')
        (tmp_path / 'server-fails').touch()
        assert process.wait(timeout=15) == 1, 'the probe gave up, and the loop saw the server end'
        assert 'ended before it was ready' in process.stdout.read()
    finally:
        (tmp_path / 'silent').unlink()


def test_a_client_that_sends_nothing_does_not_hold_the_health_port(box):
    tmp_path, _, env = box
    (tmp_path / 'ready').touch()
    until(lambda: healthy(env), 'health answers')
    with socket.create_connection(('127.0.0.1', int(env['HEALTH_PORT']))):
        until(lambda: healthy(env), 'health answers while a silent client is connected')


def test_the_container_ends_with_the_worker_and_stops_the_server(box):
    tmp_path, process, _ = box
    (tmp_path / 'ready').touch()
    until(lambda: len(events(tmp_path)) == 2, 'the worker starts')
    (tmp_path / 'worker-ends').touch()
    assert process.wait(timeout=10) == 7
    assert events(tmp_path)[2:] == ['server-stopped']


def test_a_server_that_ends_takes_the_worker_with_it(box):
    tmp_path, process, _ = box
    (tmp_path / 'ready').touch()
    until(lambda: len(events(tmp_path)) == 2, 'the worker starts')
    (tmp_path / 'server-fails').touch()
    assert process.wait(timeout=10) == 1
    assert events(tmp_path)[2:] == ['worker-stopped']
