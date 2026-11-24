"""A batch job's way out: an internal Docker network whose only other member is the
node's egress proxy, which tunnels to the hosts of the job's batch and to no other.

`serve` is `zkm-fleet-egress.service` (`python -m fleet_bridge.egress serve`): it makes the network and the node's key, starts
the proxy, shows that the network reaches the proxy and nothing of the node, and then
stays with the proxy. `environment` is what the bridge gives a job on that network.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import ipaddress
import json
import os
import secrets
import subprocess
import sys
from pathlib import Path

from .model import BatchEgressConfig, ModelError, from_dict

DOCKER = ("docker", "--host=unix:///var/run/docker.sock")
# The network a job joins, and the proxy's container: its name on that network and its port.
NETWORK, PROXY, PORT = "fleet-egress", "fleet-egress-proxy", 3128
# The bridge of the network has no address on the node: a job has no route to the node
# or beyond it, and nothing of the node listens where a job could reach it.
NETWORK_OPTIONS = ("--internal", "--opt", "com.docker.network.bridge.inhibit_ipv4=true")
KEY_PATH = "/run/fleet-egress.key"


def config(apps: Path) -> BatchEgressConfig | None:
    """The node's `batch_egress` settings, read alone like the shim's own."""
    import yaml
    data = yaml.safe_load(apps.read_text()) or {}
    if not isinstance(data, dict):
        raise ValueError("apps.yaml must contain a mapping")
    return from_dict(BatchEgressConfig, data["batch_egress"]) if data.get("batch_egress") is not None else None


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def token(key: bytes, hosts: list[str]) -> str:
    """A batch's host list, signed for this node's proxy (`fleet-node egress-proxy`)."""
    listed = ("v1\n" + ",".join(hosts)).encode()
    return f"{_b64(listed)}.{_b64(hmac.new(key, listed, hashlib.sha256).digest())}"


def environment(settings: BatchEgressConfig, hosts: list[str]) -> dict[str, str]:
    """What points a job's HTTPS clients at the proxy, with its batch's list as credentials."""
    key = Path(settings.key_file).read_bytes()
    if len(key) < 32:
        raise ValueError("the node's egress key is shorter than 32 bytes")
    proxy = f"http://fleet:{token(key, hosts)}@{PROXY}:{PORT}"
    return {name: proxy for name in ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy")} | {"NO_PROXY": "", "no_proxy": ""}


def _docker(*args: str, check: bool = True, timeout: float = 120) -> subprocess.CompletedProcess[str]:
    env = {k: v for k, v in os.environ.items() if k not in ("DOCKER_HOST", "DOCKER_CONTEXT")}
    result = subprocess.run([*DOCKER, *args], capture_output=True, text=True, env=env, timeout=timeout)
    if check and result.returncode != 0:
        raise RuntimeError(f"docker {args[0]} {args[1] if len(args) > 1 else ''}: {result.stderr.strip()[-500:]}")
    return result


def _network() -> dict:
    """The network's description, made first where the node lacks it."""
    if _docker("network", "inspect", NETWORK, check=False).returncode != 0:
        _docker("network", "create", *NETWORK_OPTIONS, NETWORK)
    network = json.loads(_docker("network", "inspect", NETWORK).stdout)[0]
    if network.get("Internal") is not True or network.get("Driver") != "bridge":
        raise RuntimeError(f"the Docker network {NETWORK} is not an internal bridge network; remove it and start again")
    return network


def _node_addresses(network: dict) -> list[str]:
    """Addresses a job must not reach: the gateways of this network and of Docker's own,
    and every address of the node."""
    found: set[str] = set()
    for described in (network, json.loads(_docker("network", "inspect", "bridge").stdout)[0]):
        for pool in (described.get("IPAM") or {}).get("Config") or []:
            subnet = ipaddress.ip_network(pool["Subnet"])
            found.add(pool.get("Gateway") or str(subnet.network_address + 1))
    hosts = subprocess.run(["hostname", "-I"], capture_output=True, text=True, timeout=10)
    found.update(hosts.stdout.split())
    return sorted(str(ipaddress.ip_address(a.split("/")[0])) for a in found)


def start(settings: BatchEgressConfig) -> None:
    """Brings up the network and its proxy, or raises with nothing left running."""
    key = Path(settings.key_file)
    if not key.exists():
        descriptor = os.open(key, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as out:
            out.write(secrets.token_bytes(32))
    if key.stat().st_mode & 0o077 or key.stat().st_size < 32:
        raise RuntimeError(f"the egress key {key} must hold at least 32 bytes that only its owner reads")
    network = _network()
    user = f"{os.getuid()}:{os.getgid()}"
    confined = ("--user", user, "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges", "--pull=missing")
    _docker("rm", "--force", PROXY, check=False)
    # The proxy leaves through Docker's default network and listens on the batches' one.
    # `--init`: the proxy is not the container's first process, so a stop signal ends it.
    _docker("create", "--name", PROXY, "--restart=no", "--init", *confined, "--env", "RUST_LOG=info",
            "--volume", f"{key}:{KEY_PATH}:ro", "--network", "bridge", settings.image,
            "egress-proxy", "--listen", f"0.0.0.0:{PORT}", "--key-file", KEY_PATH, timeout=900)
    try:
        _docker("network", "connect", NETWORK, PROXY)
        _docker("start", PROXY)
        # A job's view of the network: the proxy answers, and nothing of the node does.
        _docker("run", "--rm", *confined, "--network", NETWORK, settings.image, "egress-check",
                "--proxy", f"{PROXY}:{PORT}", *_node_addresses(network))
    except BaseException:
        _docker("rm", "--force", PROXY, check=False)
        raise


def serve(apps: Path) -> None:
    settings = config(apps)
    if settings is None:
        raise SystemExit("apps.yaml has no batch_egress settings")
    start(settings)
    # Stay with the proxy: its output is this service's, and a stop of the service stops it.
    os.execvp(DOCKER[0], [*DOCKER, "attach", "--no-stdin", PROXY])


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["serve"])
    parser.add_argument("--apps", type=Path, required=True)
    args = parser.parse_args()
    try:
        serve(args.apps)
    except (RuntimeError, ValueError, ModelError, OSError, subprocess.SubprocessError) as error:
        print(f"egress: {error}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
