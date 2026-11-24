"""The GPU worker image's entrypoint, against a stand-in server and worker; no container runs."""
import os
from pathlib import Path
import signal
import subprocess
import time

import pytest

ENTRYPOINT = Path(__file__).resolve().parents[2] / 'deploy/images/gpu-worker/entrypoint.sh'


def script(path, body):
    path.write_text('#!/bin/bash\n' + body)
    path.chmod(0o755)
    return path


@pytest.fixture
def box(tmp_path):
    """Stand-ins on PATH: a server that runs until told to stop, a readiness probe that
    answers once `ready` exists, and a health server that records how it was started."""
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir()
    script(bin_dir / 'zkm-gpu-server', f'''echo "server env SHARD_SIZE=$SHARD_SIZE port=$ZIREN_SERVER_PORT" >> {tmp_path}/events
trap 'echo server-stopped >> {tmp_path}/events; exit 0' TERM
while [ ! -e {tmp_path}/server-fails ]; do sleep 0.05; done
exit 3
''')
    script(bin_dir / 'curl', f'''[ -e {tmp_path}/ready ] && printf '\\x08\\x01'
exit 0
''')
    script(bin_dir / 'busybox', f'echo "$@" > {tmp_path}/health-args; ls "${{@: -1}}" > {tmp_path}/health-files; exec sleep 600\n')
    script(bin_dir / 'sleep', 'exec /bin/sleep 0.05\n')
    worker = script(tmp_path / 'worker', f'''env | sort > {tmp_path}/worker-env
echo "worker-started $*" >> {tmp_path}/events
trap 'echo worker-stopped >> {tmp_path}/events; exit 0' TERM
while [ ! -e {tmp_path}/worker-ends ]; do /bin/sleep 0.05; done
exit 7
''')
    env = {'PATH': f'{bin_dir}:{os.environ["PATH"]}', 'HOME': '/home/worker', 'SHARD_SIZE': '16777216',
           'FLEET_APP': 'ethproofs', 'FLEET_APP_ELF_ID': 'a' * 64, 'WORKER_THREADS': '48', 'HEALTH_PORT': '8090'}
    return tmp_path, worker, env


def start(box, **extra):
    tmp_path, worker, env = box
    return subprocess.Popen([ENTRYPOINT, worker, '--block-interval', '1'], env={**env, **extra},
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


def until(condition, what):
    deadline = time.monotonic() + 10
    while not condition():
        assert time.monotonic() < deadline, what
        time.sleep(0.02)


def events(tmp_path):
    path = tmp_path / 'events'
    return path.read_text().split('\n')[:-1] if path.exists() else []


def test_the_worker_starts_after_the_server_is_ready_with_its_own_environment(box, tmp_path):
    private = tmp_path / 'worker.env'
    private.write_text('# credentials\nexport API_TOKEN=secret-1\nRPC_URL="https://rpc.example/?a=1&b=$x"\n'
                       "NOTE='two words'\nFLEET_APP=other\nRAYON_NUM_THREADS=1\n")
    process = start(box, WORKER_ENV_FILE=str(private))
    until(lambda: events(tmp_path), 'the server starts')
    time.sleep(0.3)
    assert events(tmp_path) == ['server env SHARD_SIZE=16777216 port=3000']  # no worker before the server is ready
    assert not (tmp_path / 'health-args').exists()
    (tmp_path / 'ready').touch()
    until(lambda: (tmp_path / 'health-files').exists(), 'health is served')
    assert events(tmp_path)[1] == 'worker-started --block-interval 1'
    worker_env = dict(line.split('=', 1) for line in (tmp_path / 'worker-env').read_text().splitlines())
    for name in ('PWD', 'SHLVL', '_'):  # the stand-in worker's own shell
        worker_env.pop(name, None)
    assert worker_env == {
        'PATH': box[2]['PATH'], 'HOME': '/home/worker', 'API_TOKEN': 'secret-1',
        'RPC_URL': 'https://rpc.example/?a=1&b=$x', 'NOTE': 'two words',
        'FLEET_APP': 'ethproofs', 'FLEET_APP_ELF_ID': 'a' * 64,  # the file cannot replace Fleet's values
        'FLEET_GPU_ENDPOINT': 'http://127.0.0.1:3000/twirp/', 'RAYON_NUM_THREADS': '48'}
    assert (tmp_path / 'health-args').read_text().split()[:4] == ['httpd', '-f', '-p', '8090']
    assert (tmp_path / 'health-files').read_text() == 'healthz\n'

    process.send_signal(signal.SIGTERM)  # the worker is told first; the server outlives it
    assert process.wait(timeout=10) == 0
    assert events(tmp_path)[2:] == ['worker-stopped', 'server-stopped']


def test_a_server_that_ends_before_it_is_ready_starts_no_worker(box, tmp_path):
    (tmp_path / 'server-fails').touch()
    process = start(box)
    assert process.wait(timeout=10) == 1
    assert 'ended before it was ready' in process.stdout.read()
    assert not (tmp_path / 'worker-env').exists()


def test_the_container_ends_with_the_worker_and_stops_the_server(box, tmp_path):
    (tmp_path / 'ready').touch()
    process = start(box)
    until(lambda: len(events(tmp_path)) == 2, 'the worker starts')
    (tmp_path / 'worker-ends').touch()
    assert process.wait(timeout=10) == 7
    assert events(tmp_path)[2:] == ['server-stopped']


def test_a_server_that_ends_takes_the_worker_with_it(box, tmp_path):
    (tmp_path / 'ready').touch()
    process = start(box)
    until(lambda: len(events(tmp_path)) == 2, 'the worker starts')
    (tmp_path / 'server-fails').touch()
    assert process.wait(timeout=10) == 1
    assert events(tmp_path)[2:] == ['worker-stopped']


@pytest.mark.parametrize('line', ['TOKEN=two words', 'TOKEN=a # note', 'TOKEN="a\\"b"', 'not a setting', '1BAD=x'])
def test_a_private_file_line_that_is_not_one_value_is_refused_without_showing_it(box, tmp_path, line):
    private = tmp_path / 'worker.env'
    private.write_text(f'OK=1\n{line}\n')
    process = start(box, WORKER_ENV_FILE=str(private))
    assert process.wait(timeout=10) == 2
    output = process.stdout.read()
    assert f'{private} line 2' in output and line not in output
    assert not events(tmp_path)  # nothing was started
