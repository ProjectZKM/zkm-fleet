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


UNITS={'zkm-fleet-ray.service','zkm-fleet-shim.service','zkm-fleet-node.service'}


def enabled(root):
    path=root/'enabled'
    return set(path.read_text().split()) if path.exists() else set()


@pytest.mark.parametrize('failure',['FAIL_SHIM','FAIL_NODE','FAIL_ACTIVE'])
def test_failed_handover_returns_the_agent_to_cron_and_retry_finishes(services,failure):
    root,prefix,etc,capture,log,binary,config,run=services
    before=config.read_bytes();watchdog=(etc/'watchdog.env').read_bytes()
    failed=run(**{failure:'1'})
    assert failed.returncode!=0
    # Nothing is committed until the node unit is active: cron keeps supervising the agent.
    assert config.read_bytes() == before and config.stat().st_mode & 0o777 == 0o640
    assert (etc/'watchdog.env').read_bytes() == watchdog
    assert 'FLEET_NODE_SUPERVISED' not in (etc/'watchdog.env').read_text()
    assert enabled(root) == set() and not list(capture.glob('*.service'))
    assert 'cron watchdog keeps supervising' in failed.stderr
    calls=log.read_text()
    if failure == 'FAIL_SHIM':
        assert 'systemctl enable' not in calls and 'start zkm-fleet-node.service' not in calls
    else:
        assert 'systemctl disable' in calls and 'systemctl stop zkm-fleet-node.service' in calls
    # The Ray watcher and shim daemon this run started do not outlive the restored settings,
    # and they stop before their units are removed.
    stop = 'systemctl stop zkm-fleet-ray.service zkm-fleet-shim.service'
    assert stop in calls
    assert calls.index(stop) < calls.index('systemctl daemon-reload', calls.index('systemctl start zkm-fleet-ray'))
    assert not (root/'active').read_text().split()
    # A node write between the failure and the retry survives the retry.
    config.write_text(config.read_text() + 'later_setting: retain-me\n')
    retried = run()
    assert retried.returncode == 0, retried.stderr
    settings=yaml.safe_load(config.read_text())
    assert settings['later_setting'] == 'retain-me' and settings['shim_daemon'] is True
    assert (etc/'watchdog.env').read_text().endswith('FLEET_NODE_SUPERVISED=1\n')
    assert enabled(root) == UNITS
    assert ' restart ' not in log.read_text()


def test_failed_handover_leaves_already_running_services_alone(services):
    root,prefix,etc,capture,log,binary,config,run=services
    failed=run(FAIL_SHIM='1', PREACTIVE='zkm-fleet-ray.service zkm-fleet-shim.service')
    assert failed.returncode != 0 and 'cron watchdog keeps supervising' in failed.stderr
    assert ' stop ' not in log.read_text()
    assert set((root/'active').read_text().split()) == {'zkm-fleet-ray.service','zkm-fleet-shim.service'}


def test_failed_rerun_keeps_units_a_previous_handover_enabled(services):
    root,prefix,etc,capture,log,binary,config,run=services
    assert run().returncode == 0
    config_before=config.read_bytes();watchdog=(etc/'watchdog.env').read_bytes()
    log.unlink();failed=run(FAIL_NODE='1')
    assert failed.returncode != 0
    # This run enabled nothing, so it disables and stops nothing: systemd keeps retrying the agent.
    assert enabled(root) == UNITS
    assert 'systemctl disable' not in log.read_text() and ' stop ' not in log.read_text()
    assert config.read_bytes() == config_before and (etc/'watchdog.env').read_bytes() == watchdog
    assert watchdog.decode().endswith('FLEET_NODE_SUPERVISED=1\n')


def test_supervision_flag_is_written_only_after_the_node_unit_is_active(services):
    root,prefix,etc,capture,log,binary,config,run=services
    result=run();assert result.returncode==0,result.stderr
    calls=log.read_text().splitlines()
    order=[c for c in calls if c.startswith('systemctl ')]
    assert order.index('systemctl start zkm-fleet-ray.service zkm-fleet-shim.service') \
        < order.index('systemctl enable zkm-fleet-ray.service zkm-fleet-shim.service zkm-fleet-node.service') \
        < order.index('systemctl start zkm-fleet-node.service') \
        < order.index('systemctl is-active --quiet zkm-fleet-node.service')
    assert (root/'supervised-at-active').read_text() == 'no'
    assert (etc/'watchdog.env').read_text().endswith('FLEET_NODE_SUPERVISED=1\n')


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


def test_bundled_tunnel_helper_is_staged_immutably_without_switching_transport(services):
    root,prefix,etc,capture,log,binary,config,run=services
    helper=root/'trusted-router-tunnel';helper.write_bytes(b'first-helper');helper.chmod(0o755)
    first=run();assert first.returncode==0,first.stderr
    staged=lambda content: prefix/'libexec/trusted-router-tunnel'/hashlib.sha256(content).hexdigest()/'trusted-router-tunnel'
    assert staged(b'first-helper').read_bytes()==b'first-helper'
    assert staged(b'first-helper').stat().st_mode & 0o777 == 0o755
    assert str(staged(b'first-helper')) in first.stderr
    # Staging never selects Chisel: transport choice stays an operator edit.
    assert 'tunnel_binary' not in config.read_text() and 'shim_network' not in yaml.safe_load(config.read_text())
    before=config.read_bytes()
    helper.write_bytes(b'second-helper')
    second=run();assert second.returncode==0,second.stderr
    # A live shim may still execute the old helper; both stay addressable.
    assert staged(b'first-helper').read_bytes()==b'first-helper'
    assert staged(b'second-helper').read_bytes()==b'second-helper'
    assert config.read_bytes()==before
    assert ' restart ' not in log.read_text() and ' stop ' not in log.read_text()


def test_non_executable_bundled_tunnel_helper_is_rejected_before_changes(services):
    root,prefix,etc,capture,log,binary,config,run=services
    helper=root/'trusted-router-tunnel';helper.write_bytes(b'helper');helper.chmod(0o644)
    before=config.read_bytes()
    result=run()
    assert result.returncode!=0 and 'trusted-router-tunnel is not an executable' in result.stderr
    assert config.read_bytes()==before and not log.exists()
    assert not (prefix/'libexec').exists()


def test_stage_only_publishes_runtime_binaries_beside_a_live_agent(services):
    root,prefix,etc,capture,log,binary,config,run=services
    helper=root/'trusted-router-tunnel';helper.write_bytes(b'helper');helper.chmod(0o755)
    (root/'work').mkdir();before=config.read_bytes();watchdog=(etc/'watchdog.env').read_bytes()
    command=['bash',str(ROOT/'deploy/install-node-services.sh'),str(prefix),'','',str(binary),'--stage-only']
    env={**os.environ,'PATH':str(root/'commands')+':'+os.environ['PATH']}
    # fleet-ctl stages while the agent still holds its lock.
    with (root/'work/node-agent.lock').open('w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        result=subprocess.run(command,env=env,text=True,capture_output=True,timeout=20)
    assert result.returncode==0,result.stderr
    shim=prefix/'libexec/trusted-shim'/hashlib.sha256(b'first-runtime').hexdigest()/'trusted-shim-runtime'
    tunnel=prefix/'libexec/trusted-router-tunnel'/hashlib.sha256(b'helper').hexdigest()/'trusted-router-tunnel'
    assert shim.read_bytes()==b'first-runtime' and tunnel.read_bytes()==b'helper'
    assert str(shim) in result.stderr and str(tunnel) in result.stderr
    # Never selects, switches or restarts anything.
    assert config.read_bytes()==before and (etc/'watchdog.env').read_bytes()==watchdog
    assert not log.exists() and not list(capture.glob('*.service'))
    assert not (prefix/'libexec/node-services').exists()
