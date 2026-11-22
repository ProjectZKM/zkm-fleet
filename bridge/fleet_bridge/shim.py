"""Local stdio adapter to trusted-shim. No scheduling, proof or Docker implementation.

Only explicit ordinary app selections enter this module. Persisted allocations
choose their cleanup executable independently of the current apps.yaml.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import socket
import stat
import struct
import subprocess
import tempfile
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import ray

from . import actors
from .model import AppReplicaSpec, ControlPlaneConfig, ExecutorKind, Record, ReplicaHealth, RuntimeIdentity, ServiceProtocol, to_dict

CAPABILITY = "shim-docker-v1"


def binding_hash(spec: AppReplicaSpec) -> str:
    return hashlib.sha256(json.dumps({k:v for k,v in spec.config.items() if k != "priority"},sort_keys=True).encode()).hexdigest()


def selected(spec: AppReplicaSpec) -> bool:
    return bool(spec.app.runtime and spec.app.runtime.executor is ExecutorKind.SHIM)


def required_capability(spec: AppReplicaSpec) -> str:
    if spec.app.service_protocol is ServiceProtocol.TCP:
        return "shim-tcp-v1"
    if any(rule.single_use for rule in spec.app.ingress.values()):
        return "shim-single-use-ingress-v1"
    if spec.app.ingress or "control" in spec.app.services:
        return "shim-management-v1"
    if spec.app.port_base == 0:
        return "shim-automatic-port-v1"
    if spec.app.services:
        return "shim-network-v1"
    if spec.app.container_port is not None or spec.app.entrypoint is not None:
        return "shim-runtime-v2"
    return CAPABILITY


def capabilities(config: ControlPlaneConfig) -> list[str]:
    if not config.shim_binary or not config.shim_state_dir:
        return []
    try:
        if config.shim_daemon:
            value = daemon_call(config.shim_state_dir, socket.gethostname(), {"operation": "capabilities"}, timeout=5)
        else:
            result = subprocess.run([config.shim_binary, "--capabilities"], capture_output=True, timeout=5, check=True)
            value = json.loads(result.stdout)
        if value.get("protocol") != 1 or "docker" not in value.get("drivers", []):
            return []
        modern = "data-plane-v1" in value.get("features", [])
        management = "runtime-management-v1" in value.get("features", [])
        return (["shim-tcp-v1"] if config.shim_network and modern and management else []) + (["shim-single-use-ingress-v1"] if config.shim_network and modern and management and "single-use-ingress-v1" in value.get("features", []) else []) + [CAPABILITY] + (["shim-automatic-port-v1"] if management else []) + (["shim-runtime-v2"] if modern else []) + (["shim-network-v1"] if config.shim_network and modern else []) + (["shim-management-v1"] if config.shim_network and modern and management else [])
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError):
        return []


def daemon_call(state_dir: str, node_id: str, command: dict[str, Any], timeout: float = 150) -> dict[str, Any]:
    """No TCP listener or credentials in argv. Never fall back to direct Docker."""
    path = Path(state_dir) / "control.sock"
    try:
        info = path.lstat()
        if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise ValueError("untrusted shim socket")
        payload = json.dumps({"protocol": 1, "node_id": node_id, "command": to_dict(command)}).encode()
        if len(payload) > 1024 * 1024:
            raise ValueError("oversize request")
        with socket.socket(socket.AF_UNIX) as connection:
            connection.settimeout(timeout)
            connection.connect(str(path))
            _, uid, _ = struct.unpack("3i", connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
            if uid != os.geteuid():
                raise ValueError("untrusted shim peer")
            connection.sendall(payload)
            connection.shutdown(socket.SHUT_WR)
            data = bytearray()
            while chunk := connection.recv(65536):
                data.extend(chunk)
                if len(data) > 1024 * 1024:
                    raise ValueError("oversize response")
        response = json.loads(data)
        if not isinstance(response, dict) or response.get("protocol") != 1 or not isinstance(response.get("result"), dict) or "error" in response:
            raise ValueError("shim rejected operation")
        return response["result"]
    except (OSError, ValueError, TypeError):
        raise RuntimeError("shim daemon unavailable or rejected operation; no legacy fallback") from None


def invoke(binary: str, state_dir: str, node_id: str, command: dict[str, Any], *, daemon: bool = False) -> dict[str, Any]:
    if daemon:
        return daemon_call(state_dir, node_id, command)
    # Environment/credentials travel over private stdin, never shell text or argv.
    try:
        result = subprocess.run([binary, "--state-dir", state_dir, "--node-id", node_id],
            input=json.dumps(to_dict(command)), capture_output=True, text=True, timeout=150, check=True)
        return json.loads(result.stdout)
    except (OSError, ValueError, subprocess.SubprocessError):
        raise RuntimeError("shim operation failed; no legacy fallback") from None


@dataclass(kw_only=True)
class Allocation(Record):
    executor: ExecutorKind
    execution: RuntimeIdentity
    spec: str
    binary: str
    state_dir: str
    daemon: bool = False
    cleaned: bool = False


class RuntimeBook:
    def __init__(self, root: Path) -> None:
        self.root = root

    def path(self, name: str) -> Path:
        return self.root / (hashlib.sha256(name.encode()).hexdigest() + ".json")

    def read(self, name: str) -> Allocation | None:
        try:
            return Allocation.read(str(self.path(name)))
        except FileNotFoundError:
            return None

    def write(self, name: str, record: Allocation) -> None:
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        with tempfile.NamedTemporaryFile(mode="w", dir=self.root, delete=False) as f:
            tmp = f.name
            try:
                json.dump(record.dump(), f)
                f.flush()
                os.fsync(f.fileno())
                os.replace(tmp, self.path(name))
                fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            finally:
                if os.path.exists(tmp):
                    os.unlink(tmp)

    def bind(self, spec: AppReplicaSpec, command_id: str | None) -> AppReplicaSpec:
        previous = self.read(spec.name)
        if not selected(spec):
            if previous and not previous.cleaned:
                raise ValueError("previous shim allocation requires confirmed cleanup before switching executors")
            return spec
        if not command_id:
            raise ValueError("shim launch requires a durable command id")
        if previous and not previous.cleaned:
            if (previous.spec != binding_hash(spec) or previous.binary != spec.cluster.shim_binary
                    or previous.state_dir != spec.cluster.shim_state_dir or previous.daemon != spec.cluster.shim_daemon):
                raise ValueError("active shim allocation differs; drain and clean it before changing settings")
            return replace(spec, execution=previous.execution)
        if previous and previous.execution.lease_id == command_id:
            raise ValueError("completed allocation cannot be relaunched by replay")
        execution = RuntimeIdentity(namespace=spec.app.namespace, workload=spec.app.app,
            allocation_id=spec.name, lease_id=command_id,
            generation=previous.execution.generation + 1 if previous else 1,
            node_id=socket.gethostname())
        if not spec.cluster.shim_binary or not spec.cluster.shim_state_dir:
            raise ValueError("shim is not configured")
        self.write(spec.name, Allocation(executor=ExecutorKind.SHIM, execution=execution,
            spec=binding_hash(spec), binary=spec.cluster.shim_binary, state_dir=spec.cluster.shim_state_dir,
            daemon=spec.cluster.shim_daemon))
        return replace(spec, execution=execution)

    def cleanup(self, name: str) -> None:
        record = self.read(name)
        if not record or record.cleaned:
            return
        result = invoke(record.binary, record.state_dir, socket.gethostname(),
            {"operation": "delete", "execution": record.execution}, daemon=record.daemon)
        if result.get("cleaned") is not True:
            raise RuntimeError("shim cleanup is not confirmed")
        record.cleaned = True
        self.write(name, record)


def launch_request(spec: AppReplicaSpec, gpus: list[str]) -> dict[str, Any]:
    app = spec.app
    if spec.execution is None or app.port_base is None or not app.image:
        raise ValueError("shim needs a persisted execution, image and port_base")
    port = app.port_base + spec.index if app.port_base else 0
    fill = {"port": app.container_port or port, "index": spec.index, "gpus": ",".join(str(i) for i in range(len(gpus)))}
    volumes = []
    for volume in app.volumes:
        path, _, options = volume.partition(":")
        volumes.append(f"{path}:{path}" + (f":{options}" if options else ""))
    return {"execution": spec.execution, "driver": "docker", "image": app.image,
        "command": [v.format(**fill) for v in app.command],
        "environment": {k: v.format(**fill) for k, v in app.env.items()},
        "limits": {"cpu_millicores": round(app.cpus * 1000),
            "memory_bytes": round(app.memory_gb * 2**30) if app.memory_gb is not None else None},
        # This lifecycle path makes no policy/attestation claim. Proof-required
        # execution is a separate admission path; never silently turn it off.
        "policy": {"issuer": "fleet", "policy_id": "unproved-container-lifecycle", "version": 0, "sha256": ""},
        "trust": {"attestation": {"mode": "skip_non_tee"}, "policy_proof": "not_required", "attestation_proof": "not_required"},
        "services": [{"id": {"namespace": spec.execution.namespace, "workload": spec.execution.workload, "service": name},
            "protocol": "http1" if name == "control" else app.service_protocol.value,
            "endpoint_ref": "control" if name == "control" else f"ingress:{name}" if name in app.ingress else "app"} for name in app.services],
        "driver_options": {"port": port,
            **({"container_port": app.container_port} if app.container_port is not None else {}),
            **({"entrypoint": app.entrypoint} if app.entrypoint is not None else {}),
            **({"ingress": {name:{k:v for k,v in to_dict(rule).items() if k!="single_use" or v} for name,rule in app.ingress.items()}} if app.ingress else {}),
            "gpus": gpus, "volumes": volumes,
            "user": app.user, "workdir": app.workdir, "stop_timeout_s": app.stop_timeout_s,
            "shm_bytes": (app.shm_gb if app.shm_gb is not None else 16) * 2**30}}


class _ShimApp:
    def __init__(self, spec: AppReplicaSpec) -> None:
        self.spec = spec
        self.gpus = actors._granted_gpus() if spec.app.gpus else []
        request = launch_request(spec, self.gpus)
        status = self.call({"operation": "launch", "request": request})
        if status.get("state") != "ready":
            raise RuntimeError("shim execution is not running; explicit cleanup is required")
        self.port = status.get("port") or request["driver_options"]["port"]
        if not self.port:
            raise RuntimeError("shim did not report its assigned service port")
        self.health_url = f"http://127.0.0.1:{self.port}{spec.app.health_path}"
        self.network = None
        self.start_network()
        deadline = time.monotonic() + spec.app.ready_timeout_s
        while not self.application_ready():
            if time.monotonic() >= deadline or self.call({"operation": "inspect", "execution": spec.execution}).get("state") != "ready":
                raise RuntimeError("shim app did not become healthy; allocation retained for confirmed cleanup")
            time.sleep(2)

    def start_network(self) -> None:
        spec = self.spec
        if spec.app.services:
            if spec.cluster.shim_daemon:
                self.call({"operation": "network", "execution": spec.execution, "config": spec.cluster.shim_network})
                return
            # The Rust shim owns endpoints, grant verification, reconnect and
            # stream drain. Ray supervises only this local process.
            self.network = subprocess.Popen([spec.cluster.shim_binary, "--state-dir", spec.cluster.shim_state_dir,
                "--node-id", socket.gethostname()], stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.network.stdin.write(json.dumps(to_dict({"operation": "network", "execution": spec.execution,
                "config": spec.cluster.shim_network})).encode())
            self.network.stdin.close()

    def call(self, command: dict[str, Any]) -> dict[str, Any]:
        return invoke(self.spec.cluster.shim_binary, self.spec.cluster.shim_state_dir, socket.gethostname(), command,
            daemon=self.spec.cluster.shim_daemon)

    def health(self) -> dict[str, Any]:
        status = self.call({"operation": "inspect", "execution": self.spec.execution})
        if status.get("state") == "ready" and (self.spec.cluster.shim_daemon or
                self.network is not None and self.network.poll() is not None):
            try:
                self.start_network()
            except (RuntimeError, OSError, ValueError):
                # The observed container and repair endpoint remain valid even
                # if re-registering its connector temporarily fails. Router
                # leases independently fence actual reachability.
                logging.getLogger(__name__).warning("shim connector recovery failed for %s", self.spec.name)
        return ReplicaHealth(name=self.spec.name, spec=actors.spec_hash(self.spec), executor=ExecutorKind.SHIM,
            execution=self.spec.execution,
            gpus=self.gpus, port=self.port, machine=socket.gethostname(),
            management_ready=status.get("state") == "ready",
            ready=status.get("state") == "ready" and self.application_ready()).dump()

    def application_ready(self) -> bool:
        if self.spec.app.service_protocol is ServiceProtocol.HTTP1:
            return actors._http_ok(self.health_url)
        try:
            with socket.create_connection(("127.0.0.1", self.port), timeout=3):
                return True
        except OSError:
            return False


ShimApp = ray.remote(_ShimApp)


def daemon_config(path: Path) -> ControlPlaneConfig:
    """Service bootstrap reads only its own settings; bad app entries remain
    repairable through the node's control connection."""
    import yaml
    from .model import from_dict
    data = yaml.safe_load(path.read_text()) or {}
    if not isinstance(data, dict):
        raise ValueError("apps.yaml must contain a mapping")
    return from_dict(ControlPlaneConfig, {key:data[key] for key in
        ("shim_binary", "shim_state_dir", "shim_daemon", "shim_openshell") if key in data})


def daemon_options(config: ControlPlaneConfig) -> dict[str, Any]:
    return {"openshell": to_dict(config.shim_openshell)} if config.shim_openshell is not None else {}


def check_openshell(config: ControlPlaneConfig, value: dict[str, Any]) -> None:
    expected = None
    if config.shim_openshell is not None:
        encoded = json.dumps(to_dict(config.shim_openshell), sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        expected = hashlib.sha256(encoded).hexdigest()
        if "openshell-provisioning-v1" not in value.get("features", []):
            raise RuntimeError("running shim does not support native OpenShell provisioning")
    if value.get("openshell_config_sha256") != expected:
        raise RuntimeError("running shim OpenShell settings differ; reconcile the companion explicitly before starting the node")


def check_daemon(config: ControlPlaneConfig, wait_seconds: float = 0) -> None:
    if not config.shim_daemon:
        return
    deadline = time.monotonic() + wait_seconds
    while True:
        try:
            value = daemon_call(config.shim_state_dir, socket.gethostname(), {"operation":"capabilities"}, timeout=5)
            break
        except RuntimeError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(.2)
    if value.get("protocol") != 1 or "runtime-daemon-v1" not in value.get("features", []):
        raise RuntimeError("running shim has an incompatible protocol; node was not changed")
    check_openshell(config, value)


def main() -> None:
    """Installer/service entry points use the existing apps.yaml exclusively."""
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=["check", "serve"])
    parser.add_argument("--apps", required=True)
    parser.add_argument("--wait-seconds", type=float, default=0)
    args = parser.parse_args()
    config = daemon_config(Path(args.apps))
    if not config.shim_daemon:
        if args.operation == "serve":
            parser.error("shim_daemon must be enabled in apps.yaml")
        return
    if args.operation == "serve":
        options = daemon_options(config)
        command = [config.shim_binary, "--serve", "--state-dir", config.shim_state_dir,
            "--node-id", socket.gethostname()]
        if options:
            command += ["--options", json.dumps(options)]
        os.execv(config.shim_binary, command)
    check_daemon(config, args.wait_seconds)


if __name__ == "__main__":
    main()
