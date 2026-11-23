"""Exercise fleet-ctl.sh's compose calls with stand-in docker, curl and fleetctl."""
import os
from pathlib import Path
import shlex
import subprocess

import pytest

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


def test_ray_watch_retries_a_failed_pass_without_exiting(tmp_path):
    calls=tmp_path/'calls'; home=tmp_path/'node'; commands=tmp_path/'bin'
    executable(home/'deploy/fleet-watchdog.sh', f'echo pass >> {calls}\n[ "$(wc -l < {calls})" -gt 1 ]\n')
    executable(commands/'sleep', f'if [ "$(wc -l < {calls})" -ge 2 ]; then kill -TERM "$PPID"; fi\n')
    env=tmp_path/'watchdog.env';env.write_text(f'FLEET_HOME={home}\n')
    result=subprocess.run(['bash',str(ROOT/'deploy/ray-watch.sh')],
        env={**os.environ,'PATH':str(commands)+':'+os.environ['PATH'],'FLEET_ENV':str(env)},capture_output=True,text=True,timeout=10)
    assert calls.read_text().splitlines() == ['pass','pass']
    assert 'retaining Ray and retrying' in result.stderr


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


def rollback(tmp_path, healthy_after, later_state="running 0", wait=None, trustee=False):
    """`fleet-ctl.sh scheduler rollback` with stand-ins: healthz answers on curl's
    `healthy_after`th call, and the scheduler's container reports "running 0" first and
    `later_state` after that. Sleeps take no time."""
    prefix, commands = tmp_path / "fleet", tmp_path / "commands"
    backup = prefix / "backup/control-20260101T000000Z"
    for name in ["fleetctl", "docker-compose.yml", "gateway.yaml.in", "compose.env"]:
        executable(backup / name, "")
    (prefix / "deploy").mkdir(parents=True)
    if trustee:
        for parent, text in [(backup, "original AS config"), (prefix / "deploy", "new AS config")]:
            (parent / "trustee").mkdir()
            (parent / "trustee/as.json").write_text(text)
    (prefix / "bin").mkdir()
    curls, inspects = tmp_path / "curls", tmp_path / "inspects"
    executable(commands / "curl", f'n=$(($(cat {curls} 2>/dev/null || echo 0) + 1)); echo $n > {curls}\n'
                                  f'[ "$n" -ge {healthy_after} ]\n')
    executable(commands / "docker", f'case "$*" in\n'
                                    f'  *" ps -aq scheduler") echo abc123 ;;\n'
                                    f'  inspect*) n=$(($(cat {inspects} 2>/dev/null || echo 0) + 1)); echo $n > {inspects}\n'
                                    f'    if [ "$n" = 1 ]; then echo "running 0"; else echo {shlex.quote(later_state)}; fi ;;\n'
                                    f'esac\n')
    executable(commands / "sleep", "")
    environment = {**os.environ, "PATH": f"{commands}:{os.environ['PATH']}", "FLEET_PREFIX": str(prefix),
                   "FLEET_CTL_ENV": str(tmp_path / "none.env"), "FLEET_NODE_SSH": ""}
    if wait is not None:
        environment["FLEET_CTL_HEALTH_WAIT"] = str(wait)
    result = subprocess.run(["bash", str(ROOT / "deploy/fleet-ctl.sh"), "scheduler", "rollback"],
                            env=environment, capture_output=True, text=True, timeout=60)
    return result, int(curls.read_text())


@pytest.mark.parametrize("trustee", [False, True])
def test_a_scheduler_that_starts_slowly_is_waited_for(tmp_path, trustee):
    # Forty checks two seconds apart is longer than the minute the script used to wait.
    result, curls = rollback(tmp_path, healthy_after=40, trustee=trustee)
    assert result.returncode == 0, result.stderr
    assert curls == 40
    assert result.stdout.count("waiting up to 300s for the scheduler") == 1
    assert "restored" in result.stdout
    if trustee:
        assert (tmp_path / "fleet/deploy/trustee/as.json").read_text() == "original AS config"


def test_a_scheduler_that_restarts_fails_at_once(tmp_path):
    result, curls = rollback(tmp_path, healthy_after=10**6, later_state="restarting 1")
    assert result.returncode != 0
    assert "the scheduler container stopped (restarting 1)" in result.stderr
    assert curls == 1


def test_a_scheduler_that_never_answers_fails_at_the_deadline(tmp_path):
    result, curls = rollback(tmp_path, healthy_after=10**6, wait=0)
    assert result.returncode != 0
    assert "scheduler is not healthy after rollback" in result.stderr
    assert curls == 1

def node_fixture(tmp_path):
    """A node prefix with the old binaries, bridge and launch scripts, plus fakes for sudo and friends."""
    prefix = tmp_path / "node"
    (prefix / "bin").mkdir(parents=True)
    (prefix / "bridge" / "fleet_bridge").mkdir(parents=True)
    (prefix / "venv" / "bin").mkdir(parents=True)
    (prefix / "deploy").mkdir()
    (prefix / "etc").mkdir()
    (prefix / "etc/apps.yaml").write_text("apps: []\n")
    executable(prefix / "bin/fleet-node", "echo old-node\n")
    executable(prefix / "venv/bin/python", "exit 0\n")
    for name, body in [("node-start.sh", "old node-start"), ("ray-start.sh", "old ray-start"),
                       ("fleet-watchdog.sh", "old watchdog"), ("shim-start.sh", "old shim"),
                       ("ray-watch.sh", "old ray watch"), ("install-node-services.sh", "old install")]:
        (prefix / "deploy" / name).write_text(body + "\n")
    commands = tmp_path / "bin"
    executable(commands / "sudo", "exec \"$@\"\n")
    executable(commands / "systemctl", 'case "$*" in *"--property=InvocationID"*) echo 0123456789abcdef0123456789abcdef;; esac\nexit 0\n')
    executable(commands / "journalctl", 'case "$*" in *"_SYSTEMD_INVOCATION_ID=0123456789abcdef0123456789abcdef"*) echo "node connected to control plane";; *) exit 1;; esac\n')
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
    for name in ["node-start.sh", "ray-start.sh", "fleet-watchdog.sh", "shim-start.sh", "ray-watch.sh", "install-node-services.sh"]:
        (newscripts / name).write_text(f"new {name}\n")
    environment = {**os.environ, "PATH": f"{commands}:{os.environ['PATH']}", "FLEET_PREFIX": str(prefix),
                   "FLEET_CTL_ENV": str(tmp_path / "none.env"), "FLEET_CTL_ON_NODE": "1", "FLEET_NODE_SSH": ""}
    args = ["bash", str(ROOT / "deploy/fleet-ctl.sh"), "node", "upgrade", str(newbin), str(newbridge), "", str(newscripts)]
    result = subprocess.run(args, env=environment, capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr
    assert (prefix / "deploy/ray-start.sh").read_text() == "new ray-start.sh\n"
    for name in ["shim-start.sh", "ray-watch.sh", "install-node-services.sh"]:
        assert (prefix / "deploy" / name).read_text() == f"new {name}\n"
    assert (prefix / "bin/fleet-node").read_text() == "#!/bin/bash\necho new-node\n"
    backup = next((prefix / "backup").glob("node-*"))
    assert (backup / "scripts/ray-start.sh").read_text() == "old ray-start\n"
    result = subprocess.run(["bash", str(ROOT / "deploy/fleet-ctl.sh"), "node", "rollback"],
                            env=environment, capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr
    assert (prefix / "deploy/ray-start.sh").read_text() == "old ray-start\n"
    assert (prefix / "deploy/fleet-watchdog.sh").read_text() == "old watchdog\n"
    assert (prefix / "deploy/shim-start.sh").read_text() == "old shim\n"
    assert (prefix / "deploy/ray-watch.sh").read_text() == "old ray watch\n"
    assert (prefix / "deploy/install-node-services.sh").read_text() == "old install\n"
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


def test_node_upgrade_and_rollback_do_not_restart_proof_endpoints(tmp_path):
    prefix, commands = node_fixture(tmp_path)
    calls = tmp_path / "systemctl.calls"
    executable(commands / "systemctl", f'echo "$*" >> {calls}\ncase "$*" in *"--property=InvocationID"*) echo 0123456789abcdef0123456789abcdef;; esac\n')
    old_proof = prefix / "bin/fleet-proof-app"
    executable(old_proof, "echo old-proof\n")
    new_proof = tmp_path / "new-proof"
    executable(new_proof, "echo new-proof\n")
    new_node = tmp_path / "new-node"
    executable(new_node, "echo new-node\n")
    new_bridge = tmp_path / "bridge"
    (new_bridge / "fleet_bridge").mkdir(parents=True)
    env = {**os.environ, "PATH": f"{commands}:{os.environ['PATH']}", "FLEET_PREFIX": str(prefix),
        "FLEET_CTL_ENV": str(tmp_path / "none.env"), "FLEET_NODE_SSH": "", "FLEET_PROOF_APP_UNITS": "active-proof.service"}
    ctl = ["bash", str(ROOT / "deploy/fleet-ctl.sh"), "node"]
    subprocess.run([*ctl, "upgrade", str(new_node), str(new_bridge), str(new_proof)], env=env, check=True, capture_output=True)
    assert old_proof.read_text() == new_proof.read_text()
    subprocess.run([*ctl, "rollback"], env=env, check=True, capture_output=True)
    assert "old-proof" in old_proof.read_text()
    assert "active-proof" not in calls.read_text() and "restart" not in calls.read_text()


def test_missing_node_configuration_fails_before_stopping_agent(tmp_path):
    prefix,commands=node_fixture(tmp_path)
    (prefix/'etc/apps.yaml').unlink()
    candidate=tmp_path/'candidate';executable(candidate,'exit 0\n')
    calls=tmp_path/'systemctl.calls'
    executable(commands/'systemctl',f'echo "$*" >> {calls}\n')
    env={**os.environ,'PATH':f'{commands}:{os.environ["PATH"]}','FLEET_PREFIX':str(prefix),
        'FLEET_CTL_ENV':str(tmp_path/'none.env'),'FLEET_NODE_SSH':''}
    result=subprocess.run(['bash',str(ROOT/'deploy/fleet-ctl.sh'),'node','upgrade',str(candidate),str(prefix/'bridge')],
        env=env,capture_output=True,text=True,timeout=10)
    assert result.returncode!=0 and 'cannot locate apps.yaml' in result.stderr
    assert not calls.exists() or 'stop ' not in calls.read_text()


def test_rollback_restores_proof_binary_even_if_agent_cannot_reconnect(tmp_path):
    prefix,commands=node_fixture(tmp_path)
    backup=prefix/'backup/node-20260101T000000Z';backup.mkdir(parents=True)
    executable(backup/'fleet-node','echo restored-node\n')
    executable(backup/'fleet-proof-app','echo restored-proof\n')
    (backup/'bridge/fleet_bridge').mkdir(parents=True)
    executable(prefix/'bin/fleet-proof-app','echo upgraded-proof\n')
    executable(commands/'journalctl','exit 1\n')
    executable(commands/'sleep','exit 0\n')
    env={**os.environ,'PATH':f'{commands}:{os.environ["PATH"]}','FLEET_PREFIX':str(prefix),
        'FLEET_CTL_ENV':str(tmp_path/'none.env'),'FLEET_NODE_SSH':'','FLEET_ETC':str(prefix/'etc')}
    # Explicit FLEET_ETC must win even when watchdog.env omits that variable.
    (prefix/'etc/watchdog.env').write_text('FLEET_HOME=/unused\n')
    result=subprocess.run(['bash',str(ROOT/'deploy/fleet-ctl.sh'),'node','rollback'],
        env=env,capture_output=True,text=True,timeout=10)
    assert result.returncode!=0 and 'restored node did not reconnect' in result.stderr
    assert 'restored-proof' in (prefix/'bin/fleet-proof-app').read_text()
    assert 'restored-node' in (prefix/'bin/fleet-node').read_text()


def test_a_dry_run_on_the_node_takes_the_files_it_never_shipped_for_local_files(tmp_path):
    """A remote dry run ships nothing, so the node sees paths to no files: it prints the local
    upgrade of just those files, not a download of a release named after them."""
    prefix, commands = node_fixture(tmp_path)
    executable(commands / "curl", f"echo called >> {tmp_path / 'curl.log'}\nexit 1\n")
    stage = tmp_path / "fleet-ctl-stage"  # never created
    environment = {**os.environ, "PATH": f"{commands}:{os.environ['PATH']}", "FLEET_PREFIX": str(prefix),
                   "FLEET_CTL_ENV": str(tmp_path / "none.env"), "FLEET_CTL_ON_NODE": "1", "FLEET_NODE_SSH": ""}
    result = subprocess.run(
        ["bash", str(ROOT / "deploy/fleet-ctl.sh"), "-n", "node", "upgrade", f"{stage}/fleet-node", f"{stage}/bridge"],
        env=environment, capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    assert not (tmp_path / "curl.log").exists() and "curl" not in result.stdout, result.stdout
    assert f"+ install -m 755 {stage}/fleet-node {prefix}/bin/fleet-node.new" in result.stdout, result.stdout
    assert f"+ cp -a {stage}/bridge/fleet_bridge/. {prefix}/bridge/fleet_bridge/" in result.stdout, result.stdout
    assert "fleet-proof-app" not in result.stdout and "ray-start.sh" not in result.stdout, result.stdout
    assert (prefix / "bin/fleet-node").read_text() == "#!/bin/bash\necho old-node\n", "a dry run changes nothing"


def test_remote_upgrade_preserves_scripts_position_without_a_proof_app(tmp_path):
    prefix, commands = node_fixture(tmp_path)
    newbin = tmp_path / "fleet-node"
    executable(newbin, "exit 0\n")
    newbridge = tmp_path / "bridge"
    (newbridge / "fleet_bridge").mkdir(parents=True)
    newscripts = tmp_path / "scripts"
    newscripts.mkdir()
    for name in ("node-start.sh", "ray-start.sh", "fleet-watchdog.sh", "shim-start.sh", "ray-watch.sh", "install-node-services.sh"):
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
