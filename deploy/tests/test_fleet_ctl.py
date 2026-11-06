"""Exercise fleet-ctl.sh's compose calls with stand-in docker, curl and fleetctl."""
import os
from pathlib import Path
import shlex
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


def test_a_failed_upgrade_leaves_no_stage_directory(tmp_path):
    prefix = tmp_path / "fleet"
    (prefix / "bin").mkdir(parents=True)
    executable(prefix / "bin/fleetctl", "exit 0\n")
    commands = tmp_path / "bin"
    executable(commands / "curl", "exit 1\n")  # the download fails
    tmpdir = tmp_path / "tmp"
    tmpdir.mkdir()
    environment = {**os.environ, "PATH": f"{commands}:{os.environ['PATH']}", "FLEET_PREFIX": str(prefix),
                   "FLEET_CTL_ENV": str(tmp_path / "none.env"), "FLEET_NODE_SSH": "", "TMPDIR": str(tmpdir)}
    result = subprocess.run(["bash", str(ROOT / "deploy/fleet-ctl.sh"), "scheduler", "upgrade", "v9.9.9"],
                            env=environment, capture_output=True, text=True, timeout=60)
    assert result.returncode != 0
    assert list(tmpdir.iterdir()) == [], "the release stage directory leaked"


def node_fixture(tmp_path):
    """A node prefix with the old binaries, bridge and launch scripts, plus fakes for sudo and friends."""
    prefix = tmp_path / "node"
    (prefix / "bin").mkdir(parents=True)
    (prefix / "bridge" / "fleet_bridge").mkdir(parents=True)
    (prefix / "venv" / "bin").mkdir(parents=True)
    (prefix / "deploy").mkdir()
    executable(prefix / "bin/fleet-node", "echo old-node\n")
    executable(prefix / "venv/bin/python", "exit 0\n")
    for name, body in [("node-start.sh", "old node-start"), ("ray-start.sh", "old ray-start"),
                       ("fleet-watchdog.sh", "old watchdog")]:
        (prefix / "deploy" / name).write_text(body + "\n")
    commands = tmp_path / "bin"
    executable(commands / "sudo", "exec \"$@\"\n")
    executable(commands / "systemctl", "exit 0\n")
    executable(commands / "journalctl", "echo 'node connected to control plane'\nexit 0\n")
    return prefix, commands


def test_node_upgrade_replaces_the_launch_scripts_and_rollback_restores_them(tmp_path):
    prefix, commands = node_fixture(tmp_path)
    newbin = tmp_path / "fleet-node"
    executable(newbin, "echo new-node\n")
    newbridge = tmp_path / "bridge"
    (newbridge / "fleet_bridge").mkdir(parents=True)
    (newbridge / "fleet_bridge" / "__init__.py").write_text("")
    newscripts = tmp_path / "scripts"
    newscripts.mkdir()
    for name in ["node-start.sh", "ray-start.sh", "fleet-watchdog.sh"]:
        (newscripts / name).write_text(f"new {name}\n")
    environment = {**os.environ, "PATH": f"{commands}:{os.environ['PATH']}", "FLEET_PREFIX": str(prefix),
                   "FLEET_CTL_ENV": str(tmp_path / "none.env"), "FLEET_CTL_ON_NODE": "1", "FLEET_NODE_SSH": ""}
    args = ["bash", str(ROOT / "deploy/fleet-ctl.sh"), "node", "upgrade", str(newbin), str(newbridge), "", str(newscripts)]
    result = subprocess.run(args, env=environment, capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr
    assert (prefix / "deploy/ray-start.sh").read_text() == "new ray-start.sh\n"
    assert (prefix / "bin/fleet-node").read_text() == "#!/bin/bash\necho new-node\n"
    backup = next((prefix / "backup").glob("node-*"))
    assert (backup / "scripts/ray-start.sh").read_text() == "old ray-start\n"
    result = subprocess.run(["bash", str(ROOT / "deploy/fleet-ctl.sh"), "node", "rollback"],
                            env=environment, capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr
    assert (prefix / "deploy/ray-start.sh").read_text() == "old ray-start\n"
    assert (prefix / "deploy/fleet-watchdog.sh").read_text() == "old watchdog\n"
    assert (prefix / "bin/fleet-node").read_text() == "#!/bin/bash\necho old-node\n"


def test_watchdog_repairs_a_dead_ray_link(tmp_path):
    home = tmp_path / "home"
    work = home / "work"
    (work / "ray").mkdir(parents=True)
    (work / "logs").mkdir()
    (home / "deploy").mkdir()
    executable(home / "deploy/ray-start.sh", f"echo started >> {tmp_path / 'ray.log'}\n")
    (home / "etc").mkdir()
    (home / "etc/watchdog.env").write_text(
        f"FLEET_HOME={home}\nFLEET_LOG={work / 'logs'}\nFLEET_WORKDIR={work}\nFLEET_NODE_SUPERVISED=1\n")
    link = tmp_path / "zkf-ray-link"
    link.symlink_to(tmp_path / "gone")  # a dead target, as after a cleaned-up temp workspace
    environment = {**os.environ, "FLEET_ENV": str(home / "etc/watchdog.env"), "FLEET_RAY_LINK": str(link)}
    result = subprocess.run(["bash", str(ROOT / "deploy/fleet-watchdog.sh")],
                            env=environment, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    if link.resolve() != (work / "ray").resolve():
        import pytest
        pytest.skip("a live fleet watchdog on this machine held the lock; nothing ran")
    assert link.is_symlink() and link.resolve() == (work / "ray").resolve()
    assert (tmp_path / "ray.log").exists(), "the repaired link let the watchdog start Ray"


def test_remote_upgrade_preserves_scripts_position_without_a_proof_app(tmp_path):
    prefix, commands = node_fixture(tmp_path)
    newbin = tmp_path / "fleet-node"
    executable(newbin, "exit 0\n")
    newbridge = tmp_path / "bridge"
    (newbridge / "fleet_bridge").mkdir(parents=True)
    newscripts = tmp_path / "scripts"
    newscripts.mkdir()
    for name in ("node-start.sh", "ray-start.sh", "fleet-watchdog.sh"):
        (newscripts / name).write_text("#!/bin/bash\n")
    remote_command = tmp_path / "remote-command"
    executable(commands / "scp", "exit 0\n")
    executable(commands / "ssh", 'case "$2" in *"bash -s"*) printf "%s\\n" "$2" > "$SSH_LOG";; esac\n')
    environment = {**os.environ, "PATH": f"{commands}:{os.environ['PATH']}", "FLEET_PREFIX": str(prefix),
                   "FLEET_CTL_ENV": str(tmp_path / "none.env"), "FLEET_CTL_ON_NODE": "0",
                   "FLEET_NODE_SSH": "gpu-node", "SSH_LOG": str(remote_command)}
    result = subprocess.run(
        ["bash", str(ROOT / "deploy/fleet-ctl.sh"), "node", "upgrade", str(newbin), str(newbridge), "", str(newscripts)],
        env=environment, capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    args = shlex.split(remote_command.read_text())
    upgrade = args[args.index("--") + 1:]
    assert upgrade[:2] == ["node", "upgrade"]
    stage = str(Path(upgrade[2]).parent)
    assert upgrade[2:] == [f"{stage}/fleet-node", f"{stage}/bridge", "", f"{stage}/scripts"]
