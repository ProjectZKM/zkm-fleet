"""Validate isolation and persistence in the rendered control-plane deployment."""
import json
import os
from pathlib import Path
import re
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def control_compose(tmp_path, request):
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
    if getattr(request, "param", None):
        args += ["--profile", request.param]
    result = subprocess.run(args + ["config", "--format", "json"], env=env,
                            capture_output=True, text=True, check=True)
    return json.loads(result.stdout)


def test_default_stack_leaves_the_optional_gateways_off(control_compose):
    # The OpenShell gateway exits without the scheduler's compute socket, which only
    # policy_gateway.openshell creates; fleet-ctl.sh openshell up turns it on.
    services = control_compose["services"]
    assert set(services) == {"postgres", "scheduler", "gateway"}


def test_scheduler_health_check_allows_a_slow_start(control_compose):
    # A busy scheduler loads its records before it listens; FLEET_CTL_HEALTH_WAIT is 300 s.
    check = control_compose["services"]["scheduler"]["healthcheck"]
    assert check["start_period"] in ("5m0s", "300s")
    healthcheck = next(line for line in (ROOT / "deploy/Dockerfile.scheduler").read_text().splitlines()
                       if line.startswith("HEALTHCHECK"))
    assert "--start-period=300s" in healthcheck.split(" CMD ")[0]


@pytest.mark.parametrize("control_compose", ["openshell"], indirect=True)
def test_openshell_gateway_keeps_control_host_and_health_port_private(control_compose):
    services = control_compose["services"]
    assert "trustee-as" not in services
    gateway = services["openshell-gateway"]
    assert gateway["profiles"] == ["openshell"]
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


@pytest.mark.parametrize("control_compose", ["trustee"], indirect=True)
def test_trustee_has_persistent_state_and_no_public_or_privileged_access(control_compose):
    service = control_compose["services"]["trustee-as"]
    assert service["profiles"] == ["trustee"]
    assert "@sha256:" in service["image"]
    assert not service.get("ports")
    assert service["read_only"] and service["cap_drop"] == ["ALL"]
    assert "no-new-privileges:true" in service["security_opt"]
    assert not service.get("privileged") and not service.get("devices")
    assert service["command"] == ["grpc-as", "--socket", "0.0.0.0:50004", "--config-file", "/etc/trustee/as.json"]
    config_mount = next(v for v in service["volumes"] if v["target"] == "/etc/trustee/as.json")
    assert config_mount["type"] == "bind" and config_mount["read_only"]
    assert not config_mount["bind"].get("create_host_path", False)
    config = json.loads(Path(config_mount["source"]).read_text())
    assert config["rvps_config"]["type"] == "BuiltIn"
    assert config["storage_backend"]["storage_type"] == "LocalFs"
    assert config["storage_backend"]["backends"]["local_fs"]["dir_path"] == "/opt/confidential-containers/attestation-service"
    assert service["environment"]["QCNL_CONF_PATH"] == "/run/dcap/qcnl.conf"
    assert len(service["volumes"]) == 2
    mounts = [v for v in service["volumes"] if v["type"] == "volume"]
    assert len(mounts) == 1
    assert mounts[0]["type"] == "volume"
    assert mounts[0]["source"] == "trustee-data"
    assert mounts[0]["target"] == "/opt/confidential-containers/attestation-service"
    assert "trustee-data" in control_compose["volumes"]
