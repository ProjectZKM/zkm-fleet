"""Validate the rendered optional gateway deployment without starting services."""
import json
import os
from pathlib import Path
import re
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("enabled", [False, True])
def test_openshell_profile_keeps_control_host_and_health_port_private(tmp_path, enabled):
    if not shutil.which("docker") or subprocess.run(
        ["docker", "compose", "version"], capture_output=True
    ).returncode:
        pytest.skip("Docker Compose v2 is required; no daemon is needed")
    # Do not inherit the operator's deployment paths or enabled profiles.
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("FLEET_", "COMPOSE_"))}
    env.update(FLEET_SCHEDULER_IMAGE="fleet-test:local", FLEET_ETC=str(tmp_path),
               FLEET_UID="12345", FLEET_GID="12346")
    args = ["docker", "compose", "--env-file", "/dev/null", "-p", "fleet-config-test",
            "-f", str(ROOT / "deploy/docker-compose.yml")]
    if enabled:
        args += ["--profile", "openshell"]
    result = subprocess.run(args + ["config", "--format", "json"], env=env,
                            capture_output=True, text=True, check=True)
    services = json.loads(result.stdout)["services"]
    assert ("openshell-gateway" in services) == enabled
    if not enabled:
        return
    gateway = services["openshell-gateway"]
    upstream = re.search(r'\[\[package\]\]\nname = "openshell-core"\n.*?source = "[^"]+#([0-9a-f]+)"',
                         (ROOT / "Cargo.lock").read_text(), re.S)
    assert upstream and gateway["image"].endswith(":" + upstream[1])
    assert gateway["user"] == services["scheduler"]["user"] == "12345:12346"
    assert gateway["read_only"] and "ALL" in gateway["cap_drop"]
    assert len(gateway["ports"]) == 1
    assert gateway["ports"][0]["host_ip"] == "127.0.0.1"
    assert gateway["ports"][0]["target"] == 17670
    mounts = {v["target"]: v for v in gateway["volumes"]}
    assert set(mounts) == {"/etc/openshell", "/var/lib/openshell", "/run/zkm-fleet/openshell"}
    assert mounts["/etc/openshell"]["read_only"]
    socket = next(v for v in services["scheduler"]["volumes"]
                  if v["target"] == "/run/zkm-fleet/openshell")
    assert socket["source"] == mounts[socket["target"]]["source"]
    # Image copy-up must not reset the operator-initialized UID on empty volumes.
    for volume in (socket, mounts[socket["target"]], mounts["/var/lib/openshell"]):
        assert volume["volume"]["nocopy"]
    command = gateway["command"]
    assert "--disable-tls" not in command
    for flag in ("--enable-mtls-auth", "--enable-operator-auth"):
        assert command[command.index(flag) + 1] == "true"
