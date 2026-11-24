"""Image process ownership; no prover is launched."""
import os
from pathlib import Path
import signal
import subprocess
import time

import pytest

ROOT = Path(__file__).resolve().parents[2]


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
