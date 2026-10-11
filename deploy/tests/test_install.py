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
    configuration = prefix / "etc/services.yaml"
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
