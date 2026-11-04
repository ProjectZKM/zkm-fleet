"""Exercise fleet-ctl.sh's compose calls with stand-in docker, curl and fleetctl."""
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]


def executable(path, body):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/bin/bash\n" + body)
    path.chmod(0o755)


def status(tmp_path):
    """`fleet-ctl.sh status` on a control prefix; returns the docker command lines it ran."""
    prefix, commands, log = tmp_path / "fleet", tmp_path / "commands", tmp_path / "docker.log"
    (prefix / "deploy").mkdir(parents=True, exist_ok=True)
    (prefix / "deploy/docker-compose.yml").write_text("services: {}\n")
    (prefix / "compose.env").write_text("")
    executable(commands / "docker", f'echo "$*" >> {log}\n')
    executable(commands / "curl", "exit 1\n")
    executable(prefix / "bin/fleetctl", "exit 0\n")
    environment = {**os.environ, "PATH": f"{commands}:{os.environ['PATH']}", "FLEET_PREFIX": str(prefix),
                   "FLEET_CTL_ENV": str(tmp_path / "none.env"), "FLEET_NODE_SSH": ""}
    result = subprocess.run(["bash", str(ROOT / "deploy/fleet-ctl.sh"), "status"],
                            env=environment, capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    return prefix, log.read_text().splitlines()


def test_compose_uses_the_release_file_alone(tmp_path):
    prefix, calls = status(tmp_path)
    assert calls == [f"compose --env-file {prefix}/compose.env -p zkm-fleet -f {prefix}/deploy/docker-compose.yml ps"]


def test_compose_applies_the_site_override_after_the_release_file(tmp_path):
    override = tmp_path / "fleet/deploy/docker-compose.override.yml"
    override.parent.mkdir(parents=True)
    override.write_text("services: {}\n")
    prefix, calls = status(tmp_path)
    assert calls == [f"compose --env-file {prefix}/compose.env -p zkm-fleet -f {prefix}/deploy/docker-compose.yml "
                     f"-f {override} ps"]
