"""Exercise deploy/fleet-ctl.sh against stand-in docker, curl and fleetctl."""
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]


def executable(path, body):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/bin/bash\nset -eu\n" + body)
    path.chmod(0o755)


def status(tmp_path, override):
    prefix = tmp_path / "fleet"
    (prefix / "deploy").mkdir(parents=True)
    (prefix / "compose.env").write_text("FLEET_SCHEDULER_IMAGE=scheduler:1\n")
    (prefix / "deploy" / "docker-compose.yml").write_text("services: {}\n")
    if override:
        (prefix / "deploy" / "docker-compose.override.yml").write_text("services: {}\n")
    executable(prefix / "bin" / "fleetctl", "exit 0\n")
    commands, log = tmp_path / "commands", tmp_path / "docker.log"
    executable(commands / "docker", f'echo "$*" >> {log}\n')
    executable(commands / "curl", "echo ok\n")
    env = {**os.environ, "PATH": f"{commands}:{os.environ['PATH']}", "FLEET_PREFIX": str(prefix),
           "FLEET_CTL_ENV": "/dev/null"}
    result = subprocess.run(["bash", str(ROOT / "deploy" / "fleet-ctl.sh"), "status"], env=env,
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    return log.read_text(), prefix / "deploy"


def test_a_site_override_is_loaded_after_the_release_compose_file(tmp_path):
    calls, deploy = status(tmp_path, override=True)
    assert f"-f {deploy}/docker-compose.yml -f {deploy}/docker-compose.override.yml ps" in calls


def test_without_an_override_only_the_release_compose_file_is_loaded(tmp_path):
    calls, deploy = status(tmp_path, override=False)
    assert f"-f {deploy}/docker-compose.yml ps" in calls and "docker-compose.override.yml" not in calls
