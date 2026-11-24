"""Image process ownership and lossless offline cutover; no prover is launched."""
import importlib.util
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
MODULE = importlib.util.spec_from_file_location('proof_migration', ROOT / 'deploy/migrate-proof-runtime.py')
migration = importlib.util.module_from_spec(MODULE)
MODULE.loader.exec_module(migration)
IMAGE = 'registry.example/proof@sha256:' + 'a' * 64


def configuration(tmp_path):
    return dict(scheduler='https://scheduler.example', cluster_token_file='/private/node.token',
        shim_binary='/opt/shim', shim_state_dir='/var/lib/shim', shim_daemon=True,
        apps=[dict(name='policy-node', app='policy', namespace='alice', kind='ziren-gpu',
            disabled=True, machine='gpu-1', elf_id='b' * 64, replicas=2, cpus=8, gpus=2,
            proof_storage_root=str(tmp_path / 'proofs'),
            image='old-gpu:1', env={'SHARD_SIZE':'16777216'}, volumes=['/data/models:ro'])])


def test_cutover_preserves_configuration_identity_and_all_storage(tmp_path):
    cfg = configuration(tmp_path)
    apps = tmp_path / 'apps.yaml'
    original = yaml.safe_dump(cfg).encode()
    apps.write_bytes(original)
    storage = tmp_path / 'proofs'
    storage.mkdir()
    artifacts = {'input.bin': b'input', 'proof.bin': b'proof', 'completion.json': b'{"lease":"original"}'}
    for name, content in artifacts.items():
        (storage / name).write_bytes(content)
    output = tmp_path / 'review'
    result = subprocess.run([os.sys.executable, str(ROOT / 'deploy/migrate-proof-runtime.py'),
        '--apps', str(apps), '--name', 'policy-node', '--image', IMAGE, '--output-dir', str(output)],
        capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert result.stdout == ''
    assert apps.read_bytes() == original
    assert {p.name:p.read_bytes() for p in storage.iterdir()} == artifacts
    updated = yaml.safe_load((output / 'apps.yaml').read_text())['apps'][0]
    for key in ('name','app','namespace','machine','elf_id','replicas','cpus','gpus','proof_storage_root','disabled'):
        assert updated[key] == cfg['apps'][0][key]
    assert updated['env']['SHARD_SIZE'] == '16777216'
    assert updated['shm_gb'] == 64
    assert updated['env']['FLEET_NODE_NAME'] == '{name}'
    assert json.loads((output / 'updates.json').read_text())[0]['app'] == 'policy'
    assert (output.stat().st_mode & 0o777) == 0o700
    assert ((output / 'apps.yaml').stat().st_mode & 0o777) == 0o600
    # No credential file needs to exist, let alone be read, during preparation.


@pytest.mark.parametrize('update', [dict(disabled=False), dict(kind='app'),
    dict(elf_id=''), dict(proof_storage_root='relative')])
def test_unsafe_cutover_is_refused_without_mutation(tmp_path, update):
    cfg = configuration(tmp_path)
    cfg['apps'][0].update(update)
    original = json.dumps(cfg)
    with pytest.raises(ValueError):
        migration.convert(cfg, ['policy-node'], IMAGE)
    assert json.dumps(cfg) == original


def test_cutover_requires_immutable_image_and_accounts_for_all_binary_overrides(tmp_path):
    cfg = configuration(tmp_path)
    with pytest.raises(ValueError, match='digest'):
        migration.convert(cfg, ['policy-node'], 'proof:latest')


def test_cutover_reuses_safe_mounts_and_refuses_readonly_storage(tmp_path):
    cfg = configuration(tmp_path)
    root = cfg['apps'][0]['proof_storage_root']
    cfg['apps'][0]['volumes'] = [root + ':rslave', '/private/node.token:ro']
    updated, _ = migration.convert(cfg, ['policy-node'], IMAGE)
    assert updated['apps'][0]['volumes'] == cfg['apps'][0]['volumes']
    for volumes in ([root + ':ro'], ['/private/node.token:rw'], ['/private/node.token:ro,rw'], [root, root + ':rslave']):
        cfg['apps'][0]['volumes'] = volumes
        with pytest.raises(ValueError, match='mounts'):
            migration.convert(cfg, ['policy-node'], IMAGE)


@pytest.mark.parametrize('legacy,converted', [('/data/models:/data/models:ro', '/data/models:ro'),
    ('/data/models:/data/models', '/data/models'), ('/data/models:ro', '/data/models:ro'),
    ('/data/models', '/data/models'), ('/data/models:rslave,ro', '/data/models:rslave,ro')])
def test_cutover_writes_volumes_the_way_the_shim_mounts_them(tmp_path, legacy, converted):
    cfg = configuration(tmp_path)
    root = cfg['apps'][0]['proof_storage_root']
    cfg['apps'][0]['volumes'] = [legacy, f'{root}:{root}:rslave']
    updated, updates = migration.convert(cfg, ['policy-node'], IMAGE)
    volumes = [converted, root + ':rslave', '{cluster_token_file}:ro']
    assert updated['apps'][0]['volumes'] == volumes and updates[0]['set']['volumes'] == volumes


@pytest.mark.parametrize('volume', ['/data/models:/models:ro', '/data/models:/models', 'models:/models',
    '/data/models:{proof_storage_root}', '/data/models:/data/models:ro:extra'])
def test_cutover_refuses_a_volume_the_shim_would_mount_at_another_path(tmp_path, volume):
    cfg = configuration(tmp_path)
    cfg['apps'][0]['volumes'] = ['/data/cache:ro', volume]
    original = json.dumps(cfg)
    with pytest.raises(ValueError, match=re.escape(f'policy-node: volume {volume!r}')):
        migration.convert(cfg, ['policy-node'], IMAGE)
    assert json.dumps(cfg) == original


@pytest.mark.parametrize('cpus,carried', [(8, 8), (None, 1)])
def test_cutover_carries_each_entrys_cpus_as_its_reservation(tmp_path, cpus, carried):
    cfg = configuration(tmp_path)
    if cpus is None:
        del cfg['apps'][0]['cpus']
    else:
        cfg['apps'][0]['cpus'] = cpus
    updated, updates = migration.convert(cfg, ['policy-node'], IMAGE)
    # The entry keeps what it had; an entry without cpus reserves the default one.
    assert updated['apps'][0].get('cpus') == cpus and updates[0]['cpus'] == carried
    # The scheduler takes CPUs only through a scale, never as an update setting.
    assert 'cpus' not in updates[0]['set'] and updated['apps'][0]['env']['FLEET_CPUS'] == '{cpu_count}'


@pytest.mark.parametrize('cpus', [0, -1, 0.5, 2.5, 8.0, '8', True])
def test_cutover_requires_whole_cpus(tmp_path, cpus):
    cfg = configuration(tmp_path)
    cfg['apps'][0]['cpus'] = cpus
    # As the scheduler requires; `{cpu_count}` sets RAYON_NUM_THREADS from it.
    with pytest.raises(ValueError, match=f"policy-node: cpus must be a whole number of CPUs, at least 1, as the "
                       f"scheduler requires; it is {re.escape(repr(cpus))}"):
        migration.convert(cfg, ['policy-node'], IMAGE)


def test_cli_migrates_a_gpu_entry_without_cpus(tmp_path):
    cfg = configuration(tmp_path)
    del cfg['apps'][0]['cpus']
    apps = tmp_path / 'apps.yaml'
    apps.write_text(yaml.safe_dump(cfg))
    result = subprocess.run([os.sys.executable, str(ROOT / 'deploy/migrate-proof-runtime.py'), '--apps', str(apps),
        '--name', 'policy-node', '--image', IMAGE, '--output-dir', str(tmp_path / 'review')], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert 'cpus' not in yaml.safe_load((tmp_path / 'review' / 'apps.yaml').read_text())['apps'][0]
    assert json.loads((tmp_path / 'review' / 'updates.json').read_text())[0]['cpus'] == 1


@pytest.mark.parametrize('failed,code', [('zkm-gpu-server',7), ('fleet-ziren-node',0), (None,None)])
def test_image_supervisor_stops_both_processes_on_failure_or_signal(tmp_path, failed, code):
    fixture = Path(__file__).with_name('fixtures') / 'proof_process.py'
    for name in ('zkm-gpu-server', 'fleet-ziren-node'):
        path = tmp_path / name
        path.symlink_to(fixture)
    env = {**os.environ, 'PATH':str(tmp_path)+':'+os.environ['PATH'],
        'TEST_CHILD_STATE':str(tmp_path), 'FLEET_BACKEND':'cuda'}
    if failed:
        env.update(TEST_FAIL_PROCESS=failed, TEST_EXIT_CODE=str(code))
    process = subprocess.Popen(['bash', str(ROOT / 'deploy/images/prover/entrypoint.sh')], env=env)
    try:
        if not failed:
            deadline = time.monotonic() + 5
            while len(list(tmp_path.glob('*.pid'))) != 2 and time.monotonic() < deadline:
                time.sleep(.02)
            assert len(list(tmp_path.glob('*.pid'))) == 2
            process.send_signal(signal.SIGTERM)
        assert process.wait(timeout=8) == (143 if not failed else code or 1)
        for path in tmp_path.glob('*.pid'):
            with pytest.raises(ProcessLookupError):
                os.kill(int(path.read_text()), 0)
        assert (tmp_path / ('fleet-ziren-node.stopped' if failed == 'zkm-gpu-server' else 'zkm-gpu-server.stopped')).exists()
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
        for path in tmp_path.glob('*.pid'):
            try:
                os.kill(int(path.read_text()), signal.SIGKILL)
            except ProcessLookupError:
                pass


def integrated(tmp_path, **update):
    """A node's configuration whose entry runs the app's own worker on the legacy executor."""
    cfg = dict(shim_binary='/opt/shim', shim_state_dir='/var/lib/shim', shim_daemon=True,
        apps=[dict(name='stream-gpu', app='stream', namespace='alice', kind='ziren-gpu', disabled=True,
            machine='gpu-1', elf_id='c' * 64, replicas=1, cpus=36, gpus=4, shm_gb=64, rust_log='debug',
            proof_storage_root='/srv/stream/state', image='gpu-server@sha256:' + 'd' * 64,
            env={'RUST_LOG': 'warn', 'TMPDIR': '/dev/shm', 'JSON': '{"a":1}'},
            worker_command=['/opt/stream/worker', '--block-interval', '1'],
            worker_env_file='/etc/stream/worker.env', worker_workdir='/srv/stream/state/run',
            worker_stop_timeout_s=600)])
    cfg['apps'][0].update(update)
    return cfg


def test_an_integrated_worker_moves_with_its_command_and_private_file_and_keeps_the_servers_environment(tmp_path):
    cfg = integrated(tmp_path)
    before = json.loads(json.dumps(cfg['apps'][0]))
    result, updates = migration.convert(cfg, ['stream-gpu'], IMAGE)
    entry = result['apps'][0]
    # What the worker is stays in the entry as it was: the image runs it.
    for key in ('name', 'app', 'namespace', 'machine', 'elf_id', 'cpus', 'gpus', 'proof_storage_root', 'disabled',
                'worker_command', 'worker_env_file', 'worker_workdir', 'worker_stop_timeout_s', 'rust_log'):
        assert entry[key] == before[key], key
    assert (entry['image'], entry['runtime'], entry['command']) == (IMAGE, {'executor': 'shim', 'driver': 'docker'}, [])
    # The GPU server's port, on the node's loopback at a port the shim picks.
    assert (entry['port_base'], entry['container_port']) == (0, 3000)
    # The server keeps its environment, braces and all; Fleet's identity and the worker's settings ride beside it.
    assert entry['env'] == {'RUST_LOG': 'warn', 'TMPDIR': '/dev/shm', 'JSON': '{{"a":1}}',
        'FLEET_NODE_NAME': '{name}', 'FLEET_APP_NAMESPACE': '{namespace}', 'FLEET_APP': '{app}',
        'FLEET_APP_ELF_ID': '{elf_id}', 'FLEET_APP_STORAGE_ROOT': '{proof_storage_root}', 'FLEET_CPUS': '{cpu_count}',
        'FLEET_WORKER_RUST_LOG': 'debug', 'FLEET_WORKER_READY_TIMEOUT_S': '{ready_timeout_s}',
        'FLEET_WORKER_ENV_FILE': '{worker_env_file}', 'ZIREN_SERVER_PORT': '3000',
        'CUDA_VISIBLE_DEVICES': '{gpus}', 'ZKM_GPU_DEVICES': '{gpus}'}
    assert entry['env']['JSON'].format() == '{"a":1}'
    # Its state is writable, its private file read-only; the working directory lies in the state.
    assert entry['volumes'] == ['/srv/stream/state', '{worker_env_file}:ro']
    assert updates == [dict(entry='stream-gpu', app='stream', namespace='alice', cpus=36,
        set={k: entry[k] for k in ('image', 'runtime', 'env', 'volumes', 'port_base', 'command', 'container_port', 'shm_gb')})]
    # It needs neither the scheduler nor the node's credential.
    assert not any('token' in v or 'scheduler' in v.lower() for v in [*entry['volumes'], *entry['env']])
    assert cfg == integrated(tmp_path), 'the source configuration is untouched'


@pytest.mark.parametrize('update, problem', [
    (dict(proof_storage_root='/opt/stream'), 'would hide'),
    (dict(volumes=['/opt:ro']), 'would hide'),
    (dict(worker_stop_timeout_s=3601), '3600'),
    (dict(kind='ziren-cpu'), 'integrated worker'),
    (dict(disabled=False), 'disable and drain'),
])
def test_an_integrated_worker_the_image_could_not_run_is_refused(tmp_path, update, problem):
    with pytest.raises(ValueError, match=problem):
        migration.convert(integrated(tmp_path, **update), ['stream-gpu'], IMAGE)


def test_a_working_directory_outside_the_state_is_mounted_too(tmp_path):
    result, _ = migration.convert(integrated(tmp_path, worker_workdir='/srv/stream/run', worker_env_file=None),
        ['stream-gpu'], IMAGE)
    assert result['apps'][0]['volumes'] == ['/srv/stream/state', '/srv/stream/run']
    assert 'FLEET_WORKER_ENV_FILE' not in result['apps'][0]['env']


SERVER = """#!/usr/bin/env python3
import os, signal, sys, time
from pathlib import Path
state = Path(os.environ['TEST_STATE'])
def stop(*_):
    (state / 'server.stopped').write_text('after the worker' if (state / 'worker.stopped').exists() else 'first')
    sys.exit(0)
signal.signal(signal.SIGTERM, stop)
(state / 'server.pid').write_text(str(os.getpid()))
while True:
    if os.environ.get('TEST_SERVER_EXIT') and (state / 'worker.pid').exists():
        sys.exit(int(os.environ['TEST_SERVER_EXIT']))
    time.sleep(.02)
"""
WORKER = """#!/usr/bin/env python3
import json, os, signal, sys, time
from pathlib import Path
state = Path(sys.argv[1])
def stop(*_):
    (state / 'worker.stopped').write_text('server alive' if not (state / 'server.stopped').exists() else 'server gone')
    sys.exit(0)
signal.signal(signal.SIGTERM, stop)
(state / 'worker.env').write_text(json.dumps(dict(os.environ)))
(state / 'worker.pid').write_text(str(os.getpid()))
if len(sys.argv) > 2:
    sys.exit(int(sys.argv[2]))
while True:
    time.sleep(.02)
"""
# The GPU server's readiness answer, once the test says it is ready.
CURL = """#!/bin/bash
[ -e "$TEST_STATE/ready" ] && printf '\\010\\001' || exit 7
"""


class WorkerImage:
    """The worker image's entrypoint over a fake GPU server and worker."""

    def __init__(self, tmp_path, *, ready=True, env_file=None, **env):
        self.state = tmp_path / 'state'
        self.state.mkdir()
        bin_dir = tmp_path / 'bin'
        bin_dir.mkdir()
        for name, text in (('zkm-gpu-server', SERVER), ('curl', CURL), ('worker', WORKER)):
            (bin_dir / name).write_text(text)
            (bin_dir / name).chmod(0o755)
        self.worker = str(bin_dir / 'worker')
        if ready:
            (self.state / 'ready').touch()
        self.env = {'PATH': f"{bin_dir}:{os.environ['PATH']}", 'HOME': str(tmp_path), 'TEST_STATE': str(self.state),
            'SHARD_SIZE': '16777216', 'CUDA_VISIBLE_DEVICES': '0,1', 'FLEET_NODE_NAME': 'stream-gpu-0',
            'FLEET_APP': 'stream', 'FLEET_CPUS': '36', 'FLEET_WORKER_RUST_LOG': 'debug', **env}
        if env_file is not None:
            (tmp_path / 'worker.env').write_text(env_file)
            self.env['FLEET_WORKER_ENV_FILE'] = str(tmp_path / 'worker.env')
        self.process = None

    def start(self, *arguments, command=None):
        self.process = subprocess.Popen(['bash', str(ROOT / 'deploy/images/worker/entrypoint.sh'),
            *(command if command is not None else [self.worker, str(self.state), *arguments])],
            env=self.env, stderr=subprocess.PIPE, text=True)
        return self

    def wait_for(self, name, timeout=8):
        deadline = time.monotonic() + timeout
        while not (self.state / name).exists():
            assert time.monotonic() < deadline and self.process.poll() is None, name
            time.sleep(.02)

    def close(self):
        if self.process and self.process.poll() is None:
            self.process.kill()
        if self.process:
            self.process.wait()
            self.process.stderr.close()
        for path in self.state.glob('*.pid'):
            try:
                os.kill(int(path.read_text()), signal.SIGKILL)
            except ProcessLookupError:
                pass


@pytest.fixture
def worker_image(tmp_path):
    images = []
    def make(**settings):
        root = tmp_path / f'image-{len(images)}'
        root.mkdir()
        images.append(WorkerImage(root, **settings))
        return images[-1]
    yield make
    for image in images:
        image.close()


def test_the_worker_starts_once_its_server_is_ready_with_its_own_environment_and_drains_first(worker_image):
    image = worker_image(ready=False, env_file=(
        "# the worker's private settings\n"
        "API_TOKEN=secret-1\n"
        'export RPC_URL="http://rpc.example/?a=1&b=2"  # quoted\n'
        "NOTE='two words # kept'\n"
        "EMPTY=\n"
        "FLEET_APP=another\n"
        "FLEET_APP_TENANT=legacy-label\n")).start()
    image.wait_for('server.pid')
    time.sleep(.5)
    assert not (image.state / 'worker.pid').exists(), 'the worker waits for its GPU server'
    (image.state / 'ready').touch()
    image.wait_for('worker.pid')
    env = json.loads((image.state / 'worker.env').read_text())
    # Its private file, Fleet's identity over it, and what the legacy executor gives a worker.
    assert {k: env[k] for k in ('API_TOKEN', 'RPC_URL', 'NOTE', 'EMPTY', 'FLEET_APP_TENANT')} == {
        'API_TOKEN': 'secret-1', 'RPC_URL': 'http://rpc.example/?a=1&b=2', 'NOTE': 'two words # kept', 'EMPTY': '',
        'FLEET_APP_TENANT': 'legacy-label'}
    assert {k: v for k, v in env.items() if k.startswith(('FLEET_', 'RUST_', 'RAYON_'))} == {
        'FLEET_NODE_NAME': 'stream-gpu-0', 'FLEET_APP': 'stream', 'FLEET_CPUS': '36', 'FLEET_APP_TENANT': 'legacy-label',
        'FLEET_WORKER_ROLE': 'app', 'FLEET_EXIT_WITH_PARENT': 'true',
        'FLEET_GPU_ENDPOINT': 'http://127.0.0.1:3000/twirp/', 'RUST_LOG': 'debug', 'RAYON_NUM_THREADS': '36'}
    # Nothing of the GPU server's settings.
    assert not {'SHARD_SIZE', 'CUDA_VISIBLE_DEVICES', 'TEST_STATE'} & set(env)
    image.process.send_signal(signal.SIGTERM)
    assert image.process.wait(timeout=8) == 143
    assert (image.state / 'worker.stopped').read_text() == 'server alive'
    assert (image.state / 'server.stopped').read_text() == 'after the worker'


@pytest.mark.parametrize('worker_exit, server_exit, code', [('5', None, 5), ('0', None, 1), (None, '7', 7)])
def test_either_process_ending_ends_the_container_and_the_other(worker_image, worker_exit, server_exit, code):
    image = worker_image(**({'TEST_SERVER_EXIT': server_exit} if server_exit else {}))
    image.start(*([worker_exit] if worker_exit else []))
    assert image.process.wait(timeout=8) == code
    assert (image.state / ('worker.stopped' if server_exit else 'server.stopped')).exists()
    for path in image.state.glob('*.pid'):
        with pytest.raises(ProcessLookupError):
            os.kill(int(path.read_text()), 0)


@pytest.mark.parametrize('line', ['API TOKEN=x', 'TOKEN=two words', 'TOKEN="a\\"b"', "TOKEN='open", 'TOKEN=$(id)x y', 'just-a-word'])
def test_a_private_file_the_entrypoint_cannot_read_exactly_starts_nothing_and_shows_no_value(worker_image, line):
    image = worker_image(env_file=f'GOOD=1\n{line}\n').start()
    assert image.process.wait(timeout=8) == 64
    message = image.process.stderr.read()
    assert 'line 2' in message and 'two words' not in message and '$(id)' not in message
    assert not list(image.state.glob('*.pid'))


def test_a_server_that_is_never_ready_or_a_command_that_is_not_the_workers_ends_the_container(worker_image):
    image = worker_image(ready=False, FLEET_WORKER_READY_TIMEOUT_S='1.0').start()
    assert image.process.wait(timeout=10) == 1 and 'did not become ready' in image.process.stderr.read()
    assert not (image.state / 'worker.pid').exists() and (image.state / 'server.stopped').exists()
    for command in ([], ['worker'], ['/no/such/worker']):
        refused = worker_image().start(command=command)
        assert refused.process.wait(timeout=8) == 64
