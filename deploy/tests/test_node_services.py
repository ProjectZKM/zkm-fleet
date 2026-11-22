"""Installation failure must preserve control supervision and runtime ownership."""
import fcntl
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def services():
    with tempfile.TemporaryDirectory(prefix='fns-') as short:
        root=Path(short);prefix=root/'node';etc=root/'etc';commands=root/'commands'
        (prefix/'venv/bin').mkdir(parents=True);etc.mkdir();commands.mkdir()
        shutil.copytree(ROOT/'bridge/fleet_bridge',prefix/'bridge/fleet_bridge')
        python=prefix/'venv/bin/python'
        python.write_text(f'#!{sys.executable}\n' + (FIXTURES / 'shim_check.py').read_text())
        python.chmod(0o755)
        capture=root/'units';capture.mkdir();log=root/'calls'
        sudo=commands/'sudo'
        sudo.write_text(f'#!{sys.executable}\n' + (FIXTURES / 'sudo.py').read_text())
        sudo.chmod(0o755)
        binary=root/'trusted-shim-runtime';binary.write_bytes(b'first-runtime');binary.chmod(0o755)
        config=etc/'apps.yaml';config.write_text('# operator comment\napps: []\noperator_setting: preserve-me\n');config.chmod(0o640)
        (etc/'watchdog.env').write_text('# custom settings\nFLEET_OPERATOR_SETTING=yes\n')
        env={**os.environ,'PATH':str(commands)+':'+os.environ['PATH']}
        command=['bash',str(ROOT/'deploy/install-node-services.sh'),str(prefix),str(etc),str(root/'work'),str(binary)]
        def run(**overrides):
            return subprocess.run(command,env={**env,**overrides},text=True,capture_output=True,timeout=20)
        yield root,prefix,etc,capture,log,binary,config,run


def test_runtime_install_is_idempotent_and_never_restarts_services(services):
    root,prefix,etc,capture,log,binary,config,run=services
    first=run();assert first.returncode==0,first.stderr
    settings=yaml.safe_load(config.read_text())
    pinned=prefix/'libexec/trusted-shim'/hashlib.sha256(b'first-runtime').hexdigest()/'trusted-shim-runtime'
    assert settings['shim_binary']==str(pinned) and settings['shim_daemon'] is True
    assert '# operator comment\napps: []\noperator_setting: preserve-me\n' in config.read_text()
    assert config.stat().st_mode & 0o777 == 0o640
    before=config.read_bytes()
    binary.write_bytes(b'second-runtime')
    second=run();assert second.returncode==0,second.stderr
    assert config.read_bytes()==before and pinned.read_bytes()==b'first-runtime'
    calls=log.read_text()
    assert ' restart ' not in calls and ' stop ' not in calls
    assert len(list(capture.glob('*.service')))==3
    for unit in capture.glob('*.service'):
        text=unit.read_text()
        mode = 'process' if unit.name == 'zkm-fleet-ray.service' else 'control-group'
        assert f'KillMode={mode}' in text and 'StartLimitIntervalSec=0' in text
        assert 'PartOf=' not in text and 'BindsTo=' not in text
    assert 'ray-watch.sh' in (capture/'zkm-fleet-ray.service').read_text()
    assert 'shim-start.sh' in (capture/'zkm-fleet-shim.service').read_text()
    assert 'Wants=zkm-fleet-ray.service zkm-fleet-shim.service' in (capture/'zkm-fleet-node.service').read_text()
    # Existing direct-executor allocations cannot silently change transport.
    config.write_text('shim_binary: /old/shim\nshim_state_dir: /old/state\napps: []\n')
    log.unlink();rejected=run()
    assert rejected.returncode!=0 and 'Explicitly drain' in rejected.stderr
    assert not log.exists()


@pytest.mark.parametrize('failure',['FAIL_SHIM','FAIL_NODE'])
def test_failed_handover_keeps_committed_configuration_and_retry_finishes(services,failure):
    root,prefix,etc,capture,log,binary,config,run=services
    before=config.read_bytes();watchdog=(etc/'watchdog.env').read_bytes()
    failed=run(**{failure:'1'})
    assert failed.returncode!=0
    assert config.read_bytes() != before and config.stat().st_mode & 0o777 == 0o640
    assert yaml.safe_load(config.read_text())['shim_daemon'] is True
    assert (etc/'watchdog.env').read_bytes().startswith(watchdog)
    # Simulate a node write after handover: retry must not restore stale YAML.
    config.write_text(config.read_text() + 'later_setting: retain-me\n')
    retried = run()
    assert retried.returncode == 0, retried.stderr
    assert yaml.safe_load(config.read_text())['later_setting'] == 'retain-me'
    assert ' restart ' not in log.read_text() and ' stop ' not in log.read_text()


def test_operator_unit_and_corrupted_pinned_artifact_are_handled(services):
    root,prefix,etc,capture,log,binary,config,run=services
    custom=capture/'zkm-fleet-node.service';custom.write_text('operator unit with its environment\n')
    pinned=prefix/'libexec/trusted-shim'/hashlib.sha256(binary.read_bytes()).hexdigest()/'trusted-shim-runtime'
    pinned.parent.mkdir(parents=True);pinned.write_bytes(b'interrupted copy')
    result=run();assert result.returncode==0,result.stderr
    assert pinned.read_bytes()==binary.read_bytes() and pinned.stat().st_mode & 0o777 == 0o755
    assert custom.read_text()=='operator unit with its environment\n'


def test_live_control_agent_is_rejected_before_changes(services):
    root,prefix,etc,capture,log,binary,config,run=services
    (root/'work').mkdir();before=config.read_bytes()
    with (root/'work/node-agent.lock').open('w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        result=run()
    assert result.returncode!=0 and 'control agent is running' in result.stderr
    assert config.read_bytes()==before and not log.exists()


def test_failure_before_service_start_restores_config_and_partial_units(services):
    root,prefix,etc,capture,log,binary,config,run=services
    before=config.read_bytes();watchdog=(etc/'watchdog.env').read_bytes()
    failed=run(FAIL_INSTALL='zkm-fleet-shim.service')
    assert failed.returncode != 0
    assert config.read_bytes() == before and (etc/'watchdog.env').read_bytes() == watchdog
    assert not list(capture.glob('*.service'))
    assert 'systemctl start' not in log.read_text()


def test_generated_legacy_ray_unit_is_upgraded(services):
    root,prefix,etc,capture,log,binary,config,run=services
    assert run().returncode == 0
    unit=capture/'zkm-fleet-ray.service'
    old=(unit.read_text().replace('# Managed by zkm-fleet install-node-services\n','')
        .replace('StartLimitIntervalSec=0\n','').replace('Description=Fleet local Ray watchdog','Description=Fleet local Ray')
        .replace('Type=simple','Type=oneshot').replace('/ray-watch.sh','/fleet-watchdog.sh')
        .replace('KillMode=process','KillMode=control-group')
        .replace('Restart=on-failure\nRestartSec=2\n','RemainAfterExit=yes\nEnvironment=FLEET_NODE_SUPERVISED=1\n'))
    unit.write_text(old)
    result=run(); assert result.returncode == 0,result.stderr
    assert 'ray-watch.sh' in unit.read_text() and 'KillMode=process' in unit.read_text()


def test_symlink_workdir_keeps_the_inherited_lock(services):
    root,prefix,etc,capture,log,binary,config,run=services
    real=root/'real-work';real.mkdir();(root/'work').symlink_to(real,target_is_directory=True)
    args=['bash','-c','exec 9>"$1"; flock -n 9; shift; exec "$@"','lock-test',str(real/'node-agent.lock'),
        'bash',str(ROOT/'deploy/install-node-services.sh'),str(prefix),str(etc),str(root/'work'),str(binary)]
    result=subprocess.run(args,env={**os.environ,'PATH':str(root/'commands')+':'+os.environ['PATH']},capture_output=True,text=True,timeout=20)
    assert result.returncode == 0,result.stderr
