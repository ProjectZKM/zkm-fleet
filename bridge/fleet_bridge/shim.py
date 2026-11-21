"""Local stdio adapter to trusted-shim. No scheduling, proof or Docker implementation.

Only explicit ordinary app selections enter this module. Persisted allocations
choose their cleanup executable independently of the current apps.yaml.
"""
from __future__ import annotations

import hashlib
import json
import os
import socket
import subprocess
import tempfile
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import ray

from . import actors
from .model import AppReplicaSpec, ControlPlaneConfig, ExecutorKind, Record, ReplicaHealth, RuntimeIdentity, to_dict

CAPABILITY = "shim-docker-v1"


def binding_hash(spec: AppReplicaSpec) -> str:
    return hashlib.sha256(json.dumps({k:v for k,v in spec.config.items() if k != "priority"},sort_keys=True).encode()).hexdigest()


def selected(spec: AppReplicaSpec) -> bool:
    return bool(spec.app.runtime and spec.app.runtime.executor is ExecutorKind.SHIM)


def required_capability(spec: AppReplicaSpec) -> str:
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
        result = subprocess.run([config.shim_binary, "--capabilities"], capture_output=True, timeout=5, check=True)
        value = json.loads(result.stdout)
        if value.get("protocol") != 1 or "docker" not in value.get("drivers", []):
            return []
        modern = "data-plane-v1" in value.get("features", [])
        management = "runtime-management-v1" in value.get("features", [])
        return [CAPABILITY] + (["shim-automatic-port-v1"] if management else []) + (["shim-runtime-v2"] if modern else []) + (["shim-network-v1"] if config.shim_network and modern else []) + (["shim-management-v1"] if config.shim_network and modern and management else [])
    except (OSError, ValueError, subprocess.SubprocessError):
        return []


def invoke(binary: str, state_dir: str, node_id: str, command: dict[str, Any]) -> dict[str, Any]:
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
                    or previous.state_dir != spec.cluster.shim_state_dir):
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
            spec=binding_hash(spec), binary=spec.cluster.shim_binary, state_dir=spec.cluster.shim_state_dir))
        return replace(spec, execution=execution)

    def cleanup(self, name: str) -> None:
        record = self.read(name)
        if not record or record.cleaned:
            return
        result = invoke(record.binary, record.state_dir, socket.gethostname(),
            {"operation": "delete", "execution": record.execution})
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
            "protocol": "http1", "endpoint_ref": "control" if name == "control" else f"ingress:{name}" if name in app.ingress else "app"} for name in app.services],
        "driver_options": {"port": port,
            **({"container_port": app.container_port} if app.container_port is not None else {}),
            **({"entrypoint": app.entrypoint} if app.entrypoint is not None else {}),
            **({"ingress": app.ingress} if app.ingress else {}),
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
        while not actors._http_ok(self.health_url):
            if time.monotonic() >= deadline or self.call({"operation": "inspect", "execution": spec.execution}).get("state") != "ready":
                raise RuntimeError("shim app did not become healthy; allocation retained for confirmed cleanup")
            time.sleep(2)

    def start_network(self) -> None:
        spec = self.spec
        if spec.app.services:
            # The Rust shim owns endpoints, grant verification, reconnect and
            # stream drain. Ray supervises only this local process.
            self.network = subprocess.Popen([spec.cluster.shim_binary, "--state-dir", spec.cluster.shim_state_dir,
                "--node-id", socket.gethostname()], stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.network.stdin.write(json.dumps(to_dict({"operation": "network", "execution": spec.execution,
                "config": spec.cluster.shim_network})).encode())
            self.network.stdin.close()

    def call(self, command: dict[str, Any]) -> dict[str, Any]:
        return invoke(self.spec.cluster.shim_binary, self.spec.cluster.shim_state_dir, socket.gethostname(), command)

    def health(self) -> dict[str, Any]:
        status = self.call({"operation": "inspect", "execution": self.spec.execution})
        if status.get("state") == "ready" and self.network is not None and self.network.poll() is not None:
            self.start_network()
        return ReplicaHealth(name=self.spec.name, spec=actors.spec_hash(self.spec), executor=ExecutorKind.SHIM,
            execution=self.spec.execution,
            gpus=self.gpus, port=self.port, machine=socket.gethostname(),
            ready=status.get("state") == "ready" and actors._http_ok(self.health_url)).dump()


ShimApp = ray.remote(_ShimApp)
