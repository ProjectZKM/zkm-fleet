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
            prover_binary='/old/sdk', proof_storage_root=str(tmp_path / 'proofs'),
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
    assert 'prover_binary' not in updated
    assert updated['env']['SHARD_SIZE'] == '16777216'
    assert updated['shm_gb'] == 64
    assert updated['env']['FLEET_NODE_NAME'] == '{name}'
    assert json.loads((output / 'updates.json').read_text())[0]['app'] == 'policy'
    assert (output.stat().st_mode & 0o777) == 0o700
    assert ((output / 'apps.yaml').stat().st_mode & 0o777) == 0o600
    # No credential file needs to exist, let alone be read, during preparation.


@pytest.mark.parametrize('update', [dict(disabled=False), dict(worker_command=['/app/worker']),
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
    cfg['apps'].append({**cfg['apps'][0], 'name':'another'})
    with pytest.raises(ValueError, match='every remaining'):
        migration.convert(cfg, ['policy-node'], IMAGE)
    converted, _ = migration.convert(cfg, ['policy-node','another'], IMAGE)
    assert all('prover_binary' not in app for app in converted['apps'])


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


@pytest.mark.parametrize('cpus', [None, 0, 0.5, True, '8'])
def test_gpu_cutover_needs_explicit_cpus_because_one_container_caps_both_processes(tmp_path, cpus):
    cfg = configuration(tmp_path)
    if cpus is None:
        del cfg['apps'][0]['cpus']
    else:
        cfg['apps'][0]['cpus'] = cpus
    original = json.dumps(cfg)
    with pytest.raises(ValueError, match='policy-node: set an explicit cpus.*--cpus N.*one container'):
        migration.convert(cfg, ['policy-node'], IMAGE)
    assert json.dumps(cfg) == original
    updated, updates = migration.convert(cfg, ['policy-node'], IMAGE, 16)
    assert updated['apps'][0]['cpus'] == 16 and updates[0]['cpus'] == 16
    # The scheduler takes CPUs only through a scale, never as an update setting.
    assert 'cpus' not in updates[0]['set'] and updated['apps'][0]['env']['FLEET_CPUS'] == '{cpu_count}'


def test_cutover_keeps_explicit_cpus_and_a_cpu_prover_needs_none(tmp_path):
    cfg = configuration(tmp_path)
    updated, updates = migration.convert(cfg, ['policy-node'], IMAGE)
    assert updated['apps'][0]['cpus'] == 8 and updates[0]['cpus'] == 8
    cfg['apps'][0].update(kind='ziren-cpu')
    del cfg['apps'][0]['cpus']
    updated, updates = migration.convert(cfg, ['policy-node'], IMAGE)
    assert 'cpus' not in updated['apps'][0] and updates[0]['cpus'] == 1


def test_cli_takes_cpus_for_the_selected_entries(tmp_path):
    cfg = configuration(tmp_path)
    del cfg['apps'][0]['cpus']
    apps = tmp_path / 'apps.yaml'
    apps.write_text(yaml.safe_dump(cfg))
    command = [os.sys.executable, str(ROOT / 'deploy/migrate-proof-runtime.py'), '--apps', str(apps),
        '--name', 'policy-node', '--image', IMAGE]
    refused = subprocess.run(command + ['--output-dir', str(tmp_path / 'refused')], capture_output=True, text=True)
    assert refused.returncode != 0 and 'explicit cpus' in refused.stderr
    assert not (tmp_path / 'refused').exists()
    for invalid in ('0', '1.5', '-2'):
        result = subprocess.run(command + ['--cpus', invalid, '--output-dir', str(tmp_path / 'bad')], capture_output=True, text=True)
        assert result.returncode == 2 and 'CPU cores' in result.stderr, invalid
    result = subprocess.run(command + ['--cpus', '24', '--output-dir', str(tmp_path / 'review')], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert yaml.safe_load((tmp_path / 'review' / 'apps.yaml').read_text())['apps'][0]['cpus'] == 24


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
