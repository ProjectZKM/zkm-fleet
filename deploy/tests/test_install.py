"""Exercise node installation without a daemon, GPUs or a native prover."""
import os
from pathlib import Path
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[2]


def executable(path, body):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/bin/bash\nset -eu\n" + body)
    path.chmod(0o755)


@pytest.fixture
def installation(tmp_path):
    prefix = tmp_path / "fleet"
    binaries = tmp_path / "binaries"
    commands = tmp_path / "commands"
    executable(binaries / "fleet-node", "exit 0\n")
    # Prevent host daemon discovery, background agents, and Ray's shared /tmp link.
    for name, result in [("docker", 0), ("pgrep", 1), ("nohup", 0), ("ln", 0)]:
        executable(commands / name, f"exit {result}\n")
    executable(commands / "python3", '''
if [ "$1" = -m ]; then
    mkdir -p "$3/bin"
    printf '#!/bin/bash\nexit 0\n' > "$3/bin/python"
    printf '#!/bin/bash\nprintf "%%s\\n" "$@" > "$FLEET_TEST_PIP_ARGS"\n' > "$3/bin/pip"
    printf '#!/bin/bash\nexit 0\n' > "$3/bin/ray"
    chmod +x "$3/bin/"*
fi
''')
    token = tmp_path / "cluster.token"
    token.write_text("a" * 64)
    environment = {**os.environ, "PATH": f"{commands}:{os.environ['PATH']}",
                   "FLEET_TEST_PIP_ARGS": str(tmp_path / "pip.args")}

    def run(*extra):
        return subprocess.run(["bash", str(ROOT / "deploy/install.sh"), "node",
            "--scheduler", "https://scheduler.example:8905", "--cluster-token", str(token),
            "--prefix", str(prefix), "--etc", str(prefix / "etc"), "--bin", str(binaries),
            "--name", "cpu-node", "--cpus", "8", "--no-cron", *extra],
            env=environment, capture_output=True, text=True, timeout=20)

    return prefix, binaries, tmp_path, run


def test_generic_node_installs_only_agent_and_declared_python_dependencies(installation):
    prefix, _, temporary, run = installation
    result = run()
    assert result.returncode == 0, result.stderr
    assert (prefix / "bin/fleet-node").is_file()
    assert not (prefix / "bin/fleet-ziren-node").exists()
    assert not (prefix / "bin/fleet-proof-app").exists()
    assert not (prefix / "deploy/docker-compose.yml").exists()
    assert not (prefix / "deploy/gateway.yaml.in").exists()
    assert not (prefix / "deploy/Dockerfile.scheduler").exists()
    assert not (prefix / "bridge/.venv").exists()
    assert not (prefix / "bridge/.mypy_cache").exists()
    assert not (prefix / "bridge/tests").exists()
    assert (temporary / "pip.args").read_text().splitlines()[-1] == str(ROOT / "bridge")
    configuration = prefix / "etc/apps.yaml"
    configuration.write_text(configuration.read_text() + "# owner configuration\n")
    assert run().returncode == 0
    assert configuration.read_text().endswith("# owner configuration\n")


def test_proof_storage_handler_is_required_only_when_requested(installation):
    prefix, binaries, temporary, run = installation
    roots = temporary / "roots"
    roots.write_text("/srv/proofs\n")
    result = run("--proof-storage-roots", str(roots))
    assert result.returncode != 0
    assert "optional fleet-ziren-node storage handler" in result.stderr
    executable(binaries / "fleet-ziren-node", "exit 0\n")
    result = run("--proof-storage-roots", str(roots))
    assert result.returncode == 0, result.stderr
    assert (prefix / "bin/fleet-ziren-node").is_file()
    assert (prefix / "etc/proof-storage-roots").read_text() == roots.read_text()


def install_node(installation, *args):
    """install.sh node with only the required flags, unlike the fixture's runner."""
    prefix, binaries, temporary, _ = installation
    environment = {**os.environ, "PATH": f"{temporary / 'commands'}:{os.environ['PATH']}",
                   "FLEET_TEST_PIP_ARGS": str(temporary / "pip.args")}
    return subprocess.run(["bash", str(ROOT / "deploy/install.sh"), "node",
        "--scheduler", "https://scheduler.example:8905", "--cluster-token", str(temporary / "cluster.token"),
        "--prefix", str(prefix), "--etc", str(prefix / "etc"), "--bin", str(binaries), *args],
        env=environment, capture_output=True, text=True, timeout=60)


def node_settings(path):
    import shlex
    values = {}
    for line in path.read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            key, raw = line.split("=", 1)
            values[key] = (shlex.split(raw) or [""])[0]
    return values


GPU_NODE = ("--name", "gpu-a", "--cuda", "0,1", "--cpus", "16")


def real_python3(commands):
    """The fixture's python3 fakes the venv; the installer's other python3 calls must really run."""
    import sys
    stub = commands / "python3"
    if "exec " not in stub.read_text():
        stub.write_text(stub.read_text() + f'[ "${{1:-}}" = -m ] || exec {sys.executable} "$@"\n')


def gpu_installation(installation):
    """A first install with every node setting given, as an operator would run it."""
    prefix, binaries, temporary, _ = installation
    commands = temporary / "commands"
    real_python3(commands)
    executable(commands / "nvidia-smi", "exit 0\n")
    executable(commands / "docker", 'echo "Runtimes: nvidia runc"\n')
    ca = temporary / "ca.pem"
    ca.write_text("private CA\n")
    first = install_node(installation, *GPU_NODE, "--scheduler-ca", str(ca), "--workdir", str(temporary / "work dir"),
                         "--jobs-dir", str(temporary / "jobs"), "--no-cron")
    assert first.returncode == 0, first.stderr
    return prefix / "etc/watchdog.env"


def test_reinstall_without_flags_keeps_the_node_identity_capacity_and_paths(installation):
    watchdog = gpu_installation(installation)
    original = node_settings(watchdog)
    assert original["FLEET_NODE_NAME"] == "gpu-a" and original["CUDA_VISIBLE_DEVICES"] == "0,1"
    again = install_node(installation, "--no-cron")
    assert again.returncode == 0, again.stderr
    assert node_settings(watchdog) == original
    for key in original:
        assert watchdog.read_text().count(f"\n{key}=") + watchdog.read_text().startswith(f"{key}=") == 1, key
    assert "node gpu-a uses" in again.stdout


def test_reinstall_flags_still_change_their_own_settings(installation):
    watchdog = gpu_installation(installation)
    original = node_settings(watchdog)
    result = install_node(installation, "--name", "gpu-b", "--cuda", "2", "--cpus", "4", "--no-cron")
    assert result.returncode == 0, result.stderr
    changed = node_settings(watchdog)
    assert (changed["FLEET_NODE_NAME"], changed["CUDA_VISIBLE_DEVICES"], changed["RAY_NUM_CPUS"]) == ("gpu-b", "2", "4")
    for key in ("FLEET_SCHEDULER_CA", "FLEET_WORKDIR", "FLEET_JOBS_DIR", "FLEET_LOG"):
        assert changed[key] == original[key], key


def test_reinstall_writes_a_missing_setting_from_its_default(installation):
    watchdog = gpu_installation(installation)
    watchdog.write_text("".join(l for l in watchdog.read_text().splitlines(keepends=True) if not l.startswith("RAY_NUM_CPUS=")))
    assert install_node(installation, "--no-cron").returncode == 0
    assert node_settings(watchdog)["RAY_NUM_CPUS"] == "" and node_settings(watchdog)["CUDA_VISIBLE_DEVICES"] == "0,1"


def test_reinstall_checks_the_live_agent_lock_in_the_existing_workdir(installation):
    import fcntl
    prefix, binaries, temporary, _ = installation
    gpu_installation(installation)
    executable(binaries / "trusted-shim-runtime", "exit 0\n")
    executable(temporary / "commands/systemctl", "exit 0\n")
    apps = (prefix / "etc/apps.yaml").read_bytes()
    with (temporary / "work dir/node-agent.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = install_node(installation, "--with-shim")
    assert result.returncode != 0 and "control agent is running" in result.stderr
    assert (prefix / "etc/apps.yaml").read_bytes() == apps


def working_venv(prefix):
    import sys
    executable(prefix / "venv/bin/python", f'exec {sys.executable} "$@"\n')


def test_a_new_scheduler_reaches_apps_yaml_and_keeps_the_rest_of_it(installation):
    prefix, _, _, run = installation
    assert run().returncode == 0
    working_venv(prefix)
    apps = prefix / "etc/apps.yaml"
    original = apps.read_text().replace("scheduler: https://scheduler.example:8905\n",
                                        "# operator notes\nscheduler: https://scheduler.example:8905 # primary\n")
    apps.write_text(original)
    result = run("--scheduler", "https://new.example:8905")
    assert result.returncode == 0, result.stderr
    assert apps.read_text() == original.replace("scheduler: https://scheduler.example:8905 # primary",
                                                'scheduler: "https://new.example:8905" # primary')
    assert node_settings(prefix / "etc/watchdog.env")["FLEET_SCHEDULER"] == "https://new.example:8905"
    # The same URL again changes nothing.
    before = apps.read_bytes()
    assert run("--scheduler", "https://new.example:8905").returncode == 0 and apps.read_bytes() == before


def test_a_scheduler_change_that_is_not_safe_is_refused(installation):
    prefix, _, _, run = installation
    assert run().returncode == 0
    working_venv(prefix)
    apps, watchdog = prefix / "etc/apps.yaml", prefix / "etc/watchdog.env"
    apps.write_text('{scheduler: "https://scheduler.example:8905", apps: []}\n')
    before, settings = apps.read_bytes(), watchdog.read_bytes()
    result = run("--scheduler", "https://new.example:8905")
    assert result.returncode != 0 and "cannot change scheduler" in result.stderr
    assert apps.read_bytes() == before and watchdog.read_bytes() == settings


def test_no_cron_starts_nothing_and_names_what_to_supervise(installation):
    prefix, _, temporary, run = installation
    result = run()
    assert result.returncode == 0, result.stderr
    assert not (prefix / "work/watchdog.lock").exists(), "the cron watchdog ran"
    assert f"{prefix}/deploy/node-start.sh" in result.stdout and f"{prefix}/deploy/ray-start.sh" in result.stdout
    # Without --no-cron, the installer starts the watchdog and installs its cron line.
    crontab = temporary / "crontab"
    executable(temporary / "commands/crontab", f'if [ "${{1:-}}" = -l ]; then cat {crontab} 2>/dev/null; else cat > {crontab}; fi\n')
    cron = install_node(installation, "--name", "cpu-node")
    assert cron.returncode == 0, cron.stderr
    assert (prefix / "work/watchdog.lock").exists() and "fleet-watchdog.sh" in crontab.read_text()


def test_reinstall_refreshes_node_settings_and_keeps_operator_values(installation):
    prefix, _, _, run = installation
    assert run().returncode == 0
    watchdog = prefix / "etc/watchdog.env"
    watchdog.write_text(watchdog.read_text() + "FLEET_OPERATOR_SETTING=yes\nFLEET_NODE_SUPERVISED=1\n")
    result = run("--cpus", "32", "--scheduler", "https://new.example:8905", "--name", "new-name")
    assert result.returncode == 0, result.stderr
    text = watchdog.read_text()
    for expected in ["RAY_NUM_CPUS=32", "FLEET_SCHEDULER=https://new.example:8905", "FLEET_NODE_NAME=new-name",
                     "FLEET_OPERATOR_SETTING=yes", "FLEET_NODE_SUPERVISED=1"]:
        assert expected in text
    assert text.count("RAY_NUM_CPUS=") == 1


def link_home(tmp_path, target, link):
    """Run install.sh's own link_home with a private HOME."""
    script = (ROOT / "deploy/install.sh").read_text()
    body = script[script.index("link_home(){"):]
    body = body[:body.index("\n}\n") + 3]
    return subprocess.run(["bash", "-c", body + 'link_home "$1" "$2"', "-", str(target), str(link)],
        capture_output=True, text=True, timeout=10, env={**os.environ, "HOME": str(tmp_path)})


def test_second_installation_never_redirects_existing_home_links(tmp_path):
    live, rehearsal = tmp_path / "live/etc/admin.token", tmp_path / "rehearsal/etc/admin.token"
    for token in (live, rehearsal):
        token.parent.mkdir(parents=True)
        token.write_text("x")
    link = tmp_path / ".config/zkm-fleet/admin.token"
    link.parent.mkdir(parents=True)
    # A fresh user gets the link.
    assert link_home(tmp_path, live, link).returncode == 0
    assert os.readlink(link) == str(live)
    # Re-installing the same prefix is silent and unchanged.
    again = link_home(tmp_path, live, link)
    assert again.returncode == 0 and again.stderr == "" and os.readlink(link) == str(live)
    # Another installation keeps the live one and says how to switch.
    other = link_home(tmp_path, rehearsal, link)
    assert other.returncode == 0 and os.readlink(link) == str(live)
    assert f"ln -sfn {rehearsal} {link}" in other.stderr
    # Once the live installation is gone, a later install repairs the dangling link.
    live.unlink()
    assert link_home(tmp_path, rehearsal, link).returncode == 0
    assert os.readlink(link) == str(rehearsal)
    # An operator's own file is never replaced by a link.
    own = tmp_path / ".local/bin/fleetctl"
    own.parent.mkdir(parents=True)
    own.write_text("#!/bin/sh\n")
    assert link_home(tmp_path, rehearsal, own).returncode == 0
    assert not own.is_symlink() and own.read_text() == "#!/bin/sh\n"
