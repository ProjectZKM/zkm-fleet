"""Installation owns units and immutable artifacts, not running task lifetimes."""
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

import yaml

ROOT = Path(__file__).resolve().parents[2]


def test_runtime_install_is_idempotent_and_never_restarts_services(tmp_path):
    with tempfile.TemporaryDirectory(prefix='fns-') as short:
        root=Path(short);prefix=root/'node';etc=root/'etc';commands=root/'commands'
        (prefix/'venv/bin').mkdir(parents=True);etc.mkdir();commands.mkdir()
        shutil.copytree(ROOT/'bridge/fleet_bridge',prefix/'bridge/fleet_bridge')
        python=prefix/'venv/bin/python'
        # Only the live readiness check is substituted. Configuration and unit
        # generation execute their real Python code.
        python.write_text(f'#!{sys.executable}\nimport os,sys\n'
            'if sys.argv[1:4]==["-m","fleet_bridge.shim","check"]:sys.exit(0)\n'
            f'os.execv({sys.executable!r},[{sys.executable!r},*sys.argv[1:]])\n')
        python.chmod(0o755)
        capture=root/'units';capture.mkdir();log=root/'calls'
        sudo=commands/'sudo'
        sudo.write_text(f'#!{sys.executable}\nimport pathlib,shutil,sys\n'
            f'with open({str(log)!r},"a") as f:f.write(" ".join(sys.argv[1:])+"\\n")\n'
            f'if sys.argv[1]=="install":\n for path in sys.argv[4:-1]:shutil.copy2(path,{str(capture)!r})\n')
        sudo.chmod(0o755)
        binary=root/'trusted-shim-runtime';binary.write_bytes(b'first-runtime');binary.chmod(0o755)
        config=etc/'apps.yaml';config.write_text('apps: []\noperator_setting: preserve-me\n')
        env={**os.environ,'PATH':str(commands)+':'+os.environ['PATH']}
        command=['bash',str(ROOT/'deploy/install-node-services.sh'),str(prefix),str(etc),str(root/'work'),str(binary)]
        first=subprocess.run(command,env=env,text=True,capture_output=True)
        assert first.returncode==0,first.stderr
        settings=yaml.safe_load(config.read_text())
        pinned=prefix/'libexec/trusted-shim'/hashlib.sha256(b'first-runtime').hexdigest()/'trusted-shim-runtime'
        assert settings['shim_binary']==str(pinned) and settings['shim_daemon'] is True
        assert settings['operator_setting']=='preserve-me'
        before=config.read_bytes()
        binary.write_bytes(b'second-runtime')
        second=subprocess.run(command,env=env,text=True,capture_output=True)
        assert second.returncode==0,second.stderr
        assert config.read_bytes()==before and pinned.read_bytes()==b'first-runtime'
        calls=log.read_text()
        assert ' restart ' not in calls and ' stop ' not in calls
        assert len(list(capture.glob('*.service')))==3
        for unit in capture.glob('*.service'):
            text=unit.read_text()
            assert 'KillMode=control-group' in text
            assert 'PartOf=' not in text and 'BindsTo=' not in text
        assert 'shim-start.sh' in (capture/'zkm-fleet-shim.service').read_text()
        assert 'Wants=zkm-fleet-ray.service zkm-fleet-shim.service' in (capture/'zkm-fleet-node.service').read_text()
        # Installing support cannot change an existing direct-executor transport.
        config.write_text('shim_binary: /old/shim\nshim_state_dir: /old/state\napps: []\n')
        log.unlink()
        rejected=subprocess.run(command,env=env,text=True,capture_output=True)
        assert rejected.returncode!=0
        assert 'Explicitly drain' in rejected.stderr
        assert not log.exists()
