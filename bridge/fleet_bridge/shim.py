"""Local stdio adapter to trusted-shim. No scheduling, proof or Docker implementation.

Only explicit shim selections enter this module. Persisted allocations
choose their cleanup executable independently of the current apps.yaml.
"""
from __future__ import annotations

import calendar
import hashlib
import http.client
import json
import logging
import os
import re
import socket
import stat
import struct
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Iterator

import ray

from . import actors
from .model import AppReplicaSpec, AppRuntime, ControlPlaneConfig, ExecutorKind, Record, ReplicaHealth, RuntimeIdentity, ServiceProtocol, to_dict

CAPABILITY = "shim-docker-v1"

# How a launch request is formed. Revision 2 runs a Docker container the way the
# legacy executor runs an app: `cpus` and `memory_gb` are Ray reservations, not
# container limits, and a GPU app gets every NVIDIA driver capability. Revision 1
# sets them as hard limits and adds nothing to the environment. The shim replays
# a launch only when the request is identical, so an allocation keeps the
# revision it was launched with.
LAUNCH_REVISION = 2
# The CPU weight of a container that reserves no CPU (`cpus: 0`), Docker's
# `--cpu-shares`; the default weight is 1024. Under contention such a replica
# yields to reserved ones, and its `cpu_limit` caps it either way.
UNRESERVED_CPU_SHARES = 64
# A node whose bridge sets an entry's cpu_limit and memory_limit_gb as hard limits.
LIMITS_CAPABILITY = "shim-limits-v1"
# A node whose shim gives a container a CPU weight.
CPU_SHARES_CAPABILITY = "shim-cpu-shares-v1"
# A TEE node whose operator lets tenants that reserve no CPU share its VM.
TEE_CPU_OVERCOMMIT_CAPABILITY = "shim-tee-cpu-overcommit-v1"


def binding_hash(spec: AppReplicaSpec) -> str:
    # The full digest: persisted allocations compare against this exact value.
    return actors.config_digest(spec)


def selected(spec: AppReplicaSpec) -> bool:
    return bool(spec.app.runtime and spec.app.runtime.executor is ExecutorKind.SHIM)


def required_capability(spec: AppReplicaSpec) -> str:
    if spec.kind is not AppRuntime.APP:
        return "shim-prover-v1"
    if spec.app.native_access:
        if spec.app.service_protocol is ServiceProtocol.TCP:
            return "shim-openshell-tcp-services-v1"
        return "shim-openshell-services-v1" if spec.app.services else "shim-openshell-provisioning-v1"
    if spec.app.runtime and spec.app.runtime.driver in ("tee", "tee-simulation"):
        return "shim-" + spec.app.runtime.driver + "-v1"
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


def required_capabilities(spec: AppReplicaSpec) -> set[str]:
    """Requirements compose; automatic ports never replace network support."""
    app = spec.app
    required = {required_capability(spec)}
    if app.native_access and (app.gpus or app.mounts):
        required.add("shim-openshell-attachments-v1")
    if app.native_access and (app.ingress or "control" in app.services):
        required.add("shim-openshell-ingress-v1")
    if app.runtime and app.runtime.driver == "docker":
        required.add(CAPABILITY)
    if app.access_policy_sha256 is not None:
        required.add("shim-openshell-access-v1")
        if app.runtime.driver in ("tee", "tee-simulation"):
            required.add("shim-" + app.runtime.driver + "-v1")
        return required
    if app.runtime and app.runtime.driver in ("docker", "tee", "tee-simulation"):
        required.update(name for name, needed in (
            ("shim-network-v1", bool(app.services)),
            ("shim-runtime-v2", app.container_port is not None or app.entrypoint is not None),
            ("shim-automatic-port-v1", app.port_base == 0),
            ("shim-private-env-v1", any(r.bearer_env for r in app.ingress.values())),
            ("shim-tcp-v1", app.runtime.driver != "docker" and app.service_protocol is ServiceProtocol.TCP),
            ("shim-management-v1", app.runtime.driver != "docker" and (bool(app.ingress) or "control" in app.services)),
            ("shim-single-use-ingress-v1", app.runtime.driver != "docker" and any(r.single_use for r in app.ingress.values())),
            (LIMITS_CAPABILITY, app.cpu_limit is not None or app.memory_limit_gb is not None),
            (CPU_SHARES_CAPABILITY, app.cpus == 0),
            (TEE_CPU_OVERCOMMIT_CAPABILITY, app.cpus == 0 and app.runtime.driver in TEE_DRIVERS),
        ) if needed)
    return required


def _helper_problem(path: str) -> str | None:
    """Why shim would refuse this helper, or None for a regular executable nobody else can modify."""
    try:
        mode = os.stat(path).st_mode
    except OSError as error:
        return error.strerror or "cannot be read"
    if not stat.S_ISREG(mode):
        return "not a regular file"
    if mode & 0o022:
        return "group or others can write it"
    if not mode & 0o111:
        return "not executable"
    return None


def _operator_executable(path: str) -> bool:
    """Shim's own helper check: a regular executable nobody else can modify."""
    return _helper_problem(path) is None


def capabilities(config: ControlPlaneConfig) -> list[str]:
    if not all(isinstance(p, str) and os.path.isabs(p) and "\x00" not in p
               for p in (config.shim_binary, config.shim_state_dir)):
        return []
    try:
        if config.shim_daemon:
            value = daemon_call(config.shim_state_dir, socket.gethostname(), {"operation": "capabilities"}, timeout=5)
        else:
            result = subprocess.run([config.shim_binary, "--capabilities"], capture_output=True, timeout=5, check=True)
            value = json.loads(result.stdout)
        if (not isinstance(value, dict) or type(value.get("protocol")) is not int or value["protocol"] != 1
                or any(not isinstance(value.get(k, []), list) or not all(isinstance(v, str) for v in value.get(k, []))
                       for k in ("drivers", "features"))):
            return []
        if config.shim_daemon:
            check_machine(config, value)
        features = value.get("features", [])
        # A configured tunnel helper gates only the network capabilities; runtimes
        # without services never use it.
        upstream = not config.shim_network or config.shim_network.tunnel_binary is None or (
            "chisel-network-v1" in features and _operator_executable(config.shim_network.tunnel_binary))
        modern = "data-plane-v1" in features
        management = "runtime-management-v1" in features
        network = bool(config.shim_network) and modern and upstream
        managed_network = network and management
        native = ["shim-openshell-access-v1"] if "openshell-access-v1" in features else []
        if config.shim_daemon and config.shim_openshell is not None:
            check_openshell(config, value)
            if "openshell-allocation-v1" in features:
                native.append("shim-openshell-provisioning-v1")
                if "openshell-attachments-v1" in features:
                    native.append("shim-openshell-attachments-v1")
                if config.shim_network and upstream and "openshell-service-health-v1" in features:
                    native.append("shim-openshell-services-v1")
                    if "openshell-tcp-services-v1" in features:
                        native.append("shim-openshell-tcp-services-v1")
                    if "openshell-ingress-v1" in features:
                        native.append("shim-openshell-ingress-v1")
        local = "docker" in value.get("drivers", [])
        if config.shim_tee and config.shim_tee.driver in value.get("drivers", []) and "tee-startup-v1" in features:
            native.append("shim-" + config.shim_tee.driver + "-v1")
            if config.shim_tee.allow_cpu_overcommit:
                native.append(TEE_CPU_OVERCOMMIT_CAPABILITY)
            local = True
        return native + [name for name, supported in (
            ("shim-chisel-network-v1", network and config.shim_network.tunnel_binary is not None),
            ("shim-cleanup-v1", True),
            ("shim-resume-v1", config.shim_daemon and "runtime-resume-v1" in features and "docker" in value.get("drivers", [])),
            # A TEE restart is admitted afresh: new evidence and a proof each time.
            ("shim-resume-tee-v1", config.shim_daemon and "runtime-resume-tee-v1" in features and config.shim_tee is not None
                and config.shim_tee.driver in value.get("drivers", [])),
            ("shim-private-env-v1", managed_network),
            ("shim-prover-v1", modern and management and "docker" in value.get("drivers", [])),
            ("shim-tcp-v1", managed_network),
            ("shim-single-use-ingress-v1", managed_network and "single-use-ingress-v1" in features),
            (CAPABILITY, "docker" in value.get("drivers", [])),
            ("shim-automatic-port-v1", modern and management),
            ("shim-runtime-v2", modern),
            ("shim-network-v1", network),
            ("shim-management-v1", managed_network),
            (LIMITS_CAPABILITY, True),
            (CPU_SHARES_CAPABILITY, "runtime-cpu-shares-v1" in features),
        ) if supported and local]
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError):
        return []


class ShimPending(RuntimeError):
    """The daemon took the operation but did not answer within the caller's
    timeout. It runs daemon operations to completion, so this one continues."""


def daemon_call(state_dir: str, node_id: str, command: dict[str, Any], timeout: float = 150) -> dict[str, Any]:
    """No TCP listener or credentials in argv. Never fall back to direct Docker."""
    path = Path(state_dir) / "control.sock"
    sent = False
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
            sent = True
            data = bytearray()
            while chunk := connection.recv(65536):
                data.extend(chunk)
                if len(data) > 1024 * 1024:
                    raise ValueError("oversize response")
        response = json.loads(data)
        if not isinstance(response, dict) or response.get("protocol") != 1 or not isinstance(response.get("result"), dict) or "error" in response:
            raise ValueError("shim rejected operation")
        return response["result"]
    except TimeoutError:
        if sent:
            raise ShimPending("shim daemon did not answer in time; the operation continues there") from None
        raise RuntimeError("shim daemon unavailable or rejected operation; no legacy fallback") from None
    except (OSError, ValueError, TypeError):
        raise RuntimeError("shim daemon unavailable or rejected operation; no legacy fallback") from None


# A health probe's connector recovery must finish within the probe's own budget.
RECOVERY_TIMEOUT_S = 3
# Resuming a crashed container also runs inside a health probe (30 s budget).
# An unanswered resume keeps going in the daemon; the next probe observes it.
RESUME_TIMEOUT_S = 20
# Consecutive resumes back off from the first to the last delay, in seconds.
# A runtime that stays healthy this long after a resume counts as recovered.
RESUME_BACKOFF_S = (5, 300)
# Each TEE resume costs an attestation admission, with its own proof, of up to
# 300 s; the daemon continues an admission whose caller stopped waiting.
TEE_RESUME_BACKOFF_S = (300, 3600)
RESUME_STABLE_S = 60
TEE_DRIVERS = ("tee", "tee-simulation")


def invoke(binary: str, state_dir: str, node_id: str, command: dict[str, Any], *, daemon: bool = False,
           timeout: float | None = None) -> dict[str, Any]:
    if daemon:
        if timeout is None:
            # A TEE launch or resume waits for its attestation admission (300 s).
            driver = (command.get("request", {}) if command.get("operation") == "launch" else command).get("driver")
            timeout = 330 if command.get("operation") in ("launch", "resume") and driver in TEE_DRIVERS else 150
        return daemon_call(state_dir, node_id, command, timeout=timeout)
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
    driver: str = "docker"
    access_policy_sha256: str | None = None
    daemon: bool = False
    cleaned: bool = False
    # A record without one holds a revision 1 launch.
    launch_revision: int = 1


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
            return replace(spec, execution=previous.execution, launch_revision=previous.launch_revision)
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
            daemon=spec.cluster.shim_daemon, driver=spec.app.runtime.driver, access_policy_sha256=spec.app.access_policy_sha256,
            launch_revision=LAUNCH_REVISION))
        return replace(spec, execution=execution, launch_revision=LAUNCH_REVISION)

    def cleanup(self, name: str) -> None:
        record = self.read(name)
        if not record or record.cleaned:
            return
        if record.driver == "openshell" or record.access_policy_sha256 is not None:
            # Bind-before-actor failures still need a durable cancellation fence.
            invoke(record.binary, record.state_dir, socket.gethostname(),
                {"operation": "allocate", "execution": record.execution, "driver": record.driver}, daemon=record.daemon)
        result = invoke(record.binary, record.state_dir, socket.gethostname(),
            {"operation": "delete", "execution": record.execution, **({"driver": record.driver} if record.driver != "docker" else {})}, daemon=record.daemon)
        if result.get("cleaned") is not True:
            raise RuntimeError("shim cleanup is not confirmed")
        record.cleaned = True
        self.write(name, record)


def launch_limits(spec: AppReplicaSpec) -> dict[str, int | None]:
    """The container's hard CPU and memory limits; the shim sets none for a null.
    An entry's `cpu_limit` and `memory_limit_gb`, when it sets them, are those
    limits. Otherwise a revision 2 Docker launch has none: Ray's reservation of
    `cpus` and `memory_gb` alone accounts for them; revision 1 and TEE launches
    set those. A replica that reserves no CPU also gets a low CPU weight; an
    entry without the new settings sends exactly the request it always has."""
    app = spec.app
    if app.runtime.driver == "docker" and spec.launch_revision >= 2:
        limits: dict[str, int | None] = {"cpu_millicores": None, "memory_bytes": None}
    else:
        limits = {"cpu_millicores": round(app.cpus * 1000),
            "memory_bytes": round(app.memory_gb * 2**30) if app.memory_gb is not None else None}
    if app.cpu_limit is not None:
        limits["cpu_millicores"] = round(app.cpu_limit * 1000)
    if app.memory_limit_gb is not None:
        limits["memory_bytes"] = round(app.memory_limit_gb * 2**30)
    if app.cpus == 0 and spec.launch_revision >= 2:
        limits["cpu_shares"] = UNRESERVED_CPU_SHARES
    return limits


def launch_request(spec: AppReplicaSpec, gpus: list[str]) -> dict[str, Any]:
    app = spec.app
    if spec.execution is None or app.port_base is None or not app.image:
        raise ValueError("shim needs a persisted execution, image and port_base")
    port = app.port_base + spec.index if app.port_base else 0
    revision = spec.launch_revision
    # Explicit manifest substitution, shared by all containers. No credential
    # contents or worker-specific environment is injected into workloads.
    # Revision 2 counts at least one CPU, as the legacy executor sets RAYON_NUM_THREADS.
    fill = {**{k: v for k, v in to_dict(app).items() if isinstance(v, (str, int, float))},
        "port": app.container_port or port, "index": spec.index, "name": spec.name,
        "machine": socket.gethostname(), "gpu_count": len(gpus),
        "cpu_count": max(1, int(app.cpus)) if revision >= 2 else int(app.cpus),
        "gpus": ",".join(str(i) for i in range(len(gpus))),
        "scheduler": spec.cluster.scheduler or "",
        "scheduler_ca_file": spec.cluster.scheduler_ca_file or "",
        "cluster_token_file": spec.cluster.cluster_token_file or ""}
    volumes = []
    for volume in app.volumes:
        path, _, options = volume.format(**fill).partition(":")
        volumes.append(f"{path}:{path}" + (f":{options}" if options else ""))
    driver = app.runtime.driver
    trust = {"attestation": {"mode": "skip_non_tee"}, "policy_proof": "not_required", "attestation_proof": "not_required"}
    if driver in ("tee", "tee-simulation"):
        tee = spec.cluster.shim_tee
        if not spec.cluster.shim_daemon or not tee or tee.driver != driver:
            raise ValueError("TEE launch requires matching node configuration")
        trust.update(attestation={"mode": "required", "profile": tee.platform}, attestation_proof="required")
    environment = {k: v.format(**fill) for k, v in app.env.items()}
    if driver == "docker" and revision >= 2 and gpus and spec.kind is AppRuntime.APP:
        # As the legacy executor runs a GPU app; the app's own value wins.
        environment.setdefault("NVIDIA_DRIVER_CAPABILITIES", "all")
    return {"execution": spec.execution, "driver": driver, "image": app.image,
        **({"access_policy_sha256": app.access_policy_sha256} if app.access_policy_sha256 is not None else {}),
        "command": [v.format(**fill) for v in app.command],
        "environment": environment,
        "limits": launch_limits(spec),
        # Application policy admission is separate from the synchronous TEE RA gate.
        "policy": {"issuer": "fleet", "policy_id": "unproved-container-lifecycle", "version": 0, "sha256": ""},
        "trust": trust,
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


# The Docker daemon the shim's driver uses; the environment cannot select another.
DOCKER_SOCKET = "/var/run/docker.sock"
DOCKER = ("docker", f"--host=unix://{DOCKER_SOCKET}")
# How long `docker image inspect` may take before the image counts as missing.
IMAGE_CHECK_TIMEOUT_S = 30
# A pull's own budget. As for a legacy app, whose `docker run` pulls before its
# readiness clock starts, it is separate from the app's ready_timeout_s.
PULL_TIMEOUT_S = 3600
# The shim's Docker driver names each container after its allocation's slot.
CONTAINER_NAME = re.compile(r"trusted-shim-[0-9a-f]{64}")


def _docker_env() -> dict[str, str]:
    return {k: v for k, v in os.environ.items() if k not in ("DOCKER_HOST", "DOCKER_CONTEXT")}


def image_present(image: str) -> bool:
    """Whether the shim's Docker holds `image`; an unanswered check counts as no."""
    try:
        return subprocess.run([*DOCKER, "image", "inspect", "--format", "{{.Id}}", "--", image], capture_output=True,
                              env=_docker_env(), timeout=IMAGE_CHECK_TIMEOUT_S).returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


def pull_image(name: str, image: str, timeout: float = PULL_TIMEOUT_S) -> None:
    """Fetches `image` into the shim's Docker within `timeout` seconds."""
    try:
        result = subprocess.run([*DOCKER, "pull", "--", image], capture_output=True, text=True,
                                env=_docker_env(), timeout=timeout)
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"{name}: pulling image {image} did not finish within {timeout:g} s") from None
    except OSError as error:
        raise RuntimeError(f"{name}: could not pull image {image}: {error}") from None
    if result.returncode != 0:
        detail = (result.stderr.strip() or result.stdout.strip())[-500:]
        raise RuntimeError(f"{name}: could not pull image {image}: {detail or f'docker pull exited with {result.returncode}'}")


# How long connecting to Docker and its first answer may take; a followed log
# then waits as long as the container stays quiet.
DOCKER_API_TIMEOUT_S = 10
# How often the follower records how far it has written.
CURSOR_INTERVAL_S = 1.0
# A follower that fails sooner than this, with no output, retries after a
# backoff that doubles from the first value up to the second.
FOLLOW_STABLE_S = 60
FOLLOW_BACKOFF_S = (30, 900)
# Docker's RFC 3339 timestamp, which `timestamps=1` puts before each line.
_STAMP = re.compile(rb"(\d{4})-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d)(?:\.(\d{1,9}))?(Z|[+-]\d\d:\d\d)")


def _stamp_ns(raw: bytes) -> int | None:
    match = _STAMP.fullmatch(raw)
    if not match:
        return None
    seconds = calendar.timegm(tuple(int(v) for v in match.groups()[:6]) + (0, 0, 0))
    zone = match.group(8)
    if zone != b"Z":
        seconds -= (1 if zone[:1] == b"+" else -1) * (int(zone[1:3]) * 3600 + int(zone[4:6]) * 60)
    return seconds * 10**9 + int((match.group(7) or b"0").ljust(9, b"0"))


class _DockerConnection(http.client.HTTPConnection):
    """The Docker Engine API over its unix socket."""

    def __init__(self, socket_path: str, timeout: float) -> None:
        super().__init__("localhost", timeout=timeout)
        self.socket_path = socket_path

    def connect(self) -> None:
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.socket_path)


class LogFollower:
    """Appends a container's stdout and stderr to the replica's node log, the file
    `fleetctl app logs` reads, as a legacy app's node log holds its process output.

    A thread of this actor streams the container's log from the Docker daemon
    the shim uses, while the container runs, and stops with the actor. A cursor
    file beside the log holds the timestamp of the last line written, so a
    resumed container or a restarted actor appends what follows it. Nothing is
    skipped; after a crash, up to CURSOR_INTERVAL_S of output can appear twice.
    The follower's own failures go to the bridge log, never the node log, and
    never reach the app's health; repeated ones back off (FOLLOW_BACKOFF_S)."""

    def __init__(self, container: str, path: str, socket_path: str = DOCKER_SOCKET) -> None:
        self.container, self.path, self.socket_path = container, path, socket_path
        self.cursor_path = path + ".cursor"
        self.thread: threading.Thread | None = None
        self.failures = 0
        self.retry_at = 0.0

    def ensure(self) -> None:
        """Follows the container unless a follower already does or is backing off."""
        try:
            if (self.thread is None or not self.thread.is_alive()) and time.monotonic() >= self.retry_at:
                self.thread = threading.Thread(target=self.run, daemon=True)
                self.thread.start()
        except Exception as error:
            logging.getLogger(__name__).warning("could not follow the logs of %s: %s", self.container, error)

    def run(self) -> None:
        started, delivered = time.monotonic(), False
        try:
            delivered = bool(self.follow())
        except Exception as error:
            logging.getLogger(__name__).warning("could not follow the logs of %s: %s", self.container, error)
        if delivered or time.monotonic() - started >= FOLLOW_STABLE_S:
            self.failures, self.retry_at = 0, 0.0
        else:
            self.failures += 1
            first, last = FOLLOW_BACKOFF_S
            self.retry_at = time.monotonic() + min(last, first * 2 ** (self.failures - 1))

    def cursor(self) -> int:
        """Nanoseconds since the epoch of the last line written; without a cursor
        file, the log's last write, or 0 for a new log."""
        try:
            with open(self.cursor_path) as file:
                return int(file.read().strip())
        except (OSError, ValueError):
            pass
        try:
            return os.stat(self.path).st_mtime_ns
        except OSError:
            return 0

    def save_cursor(self, stamp: int) -> None:
        temp = f"{self.cursor_path}.{os.getpid()}.tmp"
        with open(temp, "w") as file:
            file.write(f"{stamp}\n")
        os.replace(temp, self.cursor_path)

    def follow(self) -> bool:
        """Streams the container's output into the node log until the stream
        ends, which it does when the container stops. Returns whether any arrived."""
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        cursor = written = self.cursor()
        delivered = False
        query = "follow=1&stdout=1&stderr=1&timestamps=1"
        if cursor:
            query += f"&since={cursor // 10**9}.{cursor % 10**9:09d}"
        connection = _DockerConnection(self.socket_path, DOCKER_API_TIMEOUT_S)
        saved = time.monotonic()
        try:
            connection.request("GET", f"/containers/{self.container}/logs?{query}")
            # The response reads from this socket, even once the connection lets it go.
            stream = connection.sock
            response = connection.getresponse()
            if response.status != 200:
                detail = response.read(500).decode(errors="replace").strip()
                raise RuntimeError(f"Docker answered {response.status}: {detail}")
            stream.settimeout(None)
            with open(self.path, "ab") as log:
                for message in self.messages(response):
                    stamp_text, _, line = message.partition(b" ")
                    stamp = _stamp_ns(stamp_text)
                    if stamp is None:
                        stamp, line = written, message
                    elif stamp <= written:
                        continue  # written before the cursor was saved, or before `since`
                    log.write(line)
                    log.flush()
                    delivered, written = True, stamp
                    if time.monotonic() - saved >= CURSOR_INTERVAL_S:
                        self.save_cursor(written)
                        saved = time.monotonic()
        finally:
            connection.close()
            if written != cursor:
                self.save_cursor(written)
        return delivered

    @staticmethod
    def messages(response: http.client.HTTPResponse) -> Iterator[bytes]:
        """One log message per item. The shim's containers have no TTY, so Docker
        frames each message with its stream and size; a raw stream is split into lines."""
        if response.getheader("Content-Type") == "application/vnd.docker.raw-stream":
            while line := response.readline(1 << 20):
                yield line
            return
        while len(header := response.read(8)) == 8:
            payload = response.read(int.from_bytes(header[4:], "big"))
            if len(payload) < int.from_bytes(header[4:], "big"):
                return
            yield payload


class _ShimApp:
    # Crash recovery state; class defaults keep pre-upgrade instances valid.
    resumable = False
    resumes = 0
    resume_at = 0.0
    resumed_at = 0.0
    logs: LogFollower | None = None

    def __init__(self, spec: AppReplicaSpec) -> None:
        self.spec = spec
        supported = capabilities(spec.cluster)
        if not required_capabilities(spec).issubset(supported):
            raise RuntimeError("shim no longer supports the admitted runtime")
        docker = spec.app.runtime.driver == "docker"
        # Only a daemon-managed runtime can be resumed in place; a TEE one only
        # behind a fresh attestation admission.
        self.resumable = spec.cluster.shim_daemon and (docker and "shim-resume-v1" in supported
            or spec.app.runtime.driver in TEE_DRIVERS and "shim-resume-tee-v1" in supported)
        self.gpus = actors._granted_gpus() if spec.app.gpus and spec.kind.takes_gpus else []
        request = launch_request(spec, self.gpus)
        status = self.launch(request, pull=docker)
        resumed = status.get("state") == "stopped" and self.resumable
        if resumed:
            # The actor started again (a Ray restart, a reboot) after this
            # replica's container exited. The replayed launch starts nothing:
            # start that same container again, as a health probe would.
            status = self.resume(status, timeout=None)
        state = status.get("state")
        # A resume still being admitted continues in the daemon; readiness waits for it.
        if state != "ready" and not (resumed and state == "starting"):
            feature = "runtime-resume-v1" if docker else "runtime-resume-tee-v1"
            reason = ("its container exited and the shim did not start it again" if state == "stopped" and self.resumable
                else f"its container exited; only the persistent shim with {feature} starts it again"
                if state == "stopped" else "its container is missing or failed" if state == "failed" else f"it is {state}")
            raise RuntimeError(f"shim execution of {spec.name} is not running: {reason}; explicit cleanup is required")
        if docker and CONTAINER_NAME.fullmatch(str(status.get("runtime_id"))):
            self.logs = LogFollower(status["runtime_id"], spec.node_log_path())
            self.logs.ensure()
        self.port = status.get("port") or request["driver_options"]["port"]
        if not self.port:
            raise RuntimeError("shim did not report its assigned service port")
        self.health_url = f"http://127.0.0.1:{self.port}{spec.app.health_path}"
        self.network = None
        self.start_network()
        deadline = time.monotonic() + spec.app.ready_timeout_s
        while not self.application_ready():
            # `starting` is a container being created or started, or a resume being admitted.
            if time.monotonic() >= deadline or self.call({"operation": "inspect", "execution": spec.execution}).get(
                    "state") not in ("ready", "starting"):
                raise RuntimeError("shim app did not become healthy; allocation retained for confirmed cleanup")
            time.sleep(2)

    def launch(self, request: dict[str, Any], *, pull: bool) -> dict[str, Any]:
        """Launches the allocation, or replays its launch, which finds the
        container it made and needs no image or registry.

        The shim creates containers with `--pull=never` and records a launch
        before creating its container, so a first launch on a node without the
        image fails having created nothing, and the same launch can be made
        again. Only then is the image pulled, as `docker run` would."""
        name = self.spec.name
        try:
            return self.call({"operation": "launch", "request": request})
        except RuntimeError as error:
            if not pull or image_present(self.spec.app.image):
                raise self.launch_failed(error) from None
        pull_image(name, self.spec.app.image)
        try:
            return self.call({"operation": "launch", "request": request})
        except RuntimeError as error:
            raise self.launch_failed(error) from None

    def launch_failed(self, error: Exception) -> RuntimeError:
        return RuntimeError(f"shim did not launch {self.spec.name} ({error}); if its allocation was stopped or "
                            "deleted, it needs explicit cleanup and a new generation")

    def start_network(self, timeout: float | None = None) -> None:
        spec = self.spec
        if spec.app.services:
            if spec.cluster.shim_daemon:
                # Idempotent: a running connector answers at once.
                self.call({"operation": "network", "execution": spec.execution, "config": spec.cluster.shim_network},
                          timeout=timeout)
                return
            # The Rust shim owns endpoints, grant verification, reconnect and
            # stream drain. Ray supervises only this local process.
            self.network = subprocess.Popen([spec.cluster.shim_binary, "--state-dir", spec.cluster.shim_state_dir,
                "--node-id", socket.gethostname()], stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.network.stdin.write(json.dumps(to_dict({"operation": "network", "execution": spec.execution,
                "config": spec.cluster.shim_network})).encode())
            self.network.stdin.close()

    def call(self, command: dict[str, Any], timeout: float | None = None) -> dict[str, Any]:
        if command["operation"] != "launch" and self.spec.app.runtime.driver != "docker":
            command = {**command, "driver": self.spec.app.runtime.driver}
        return invoke(self.spec.cluster.shim_binary, self.spec.cluster.shim_state_dir, socket.gethostname(), command,
            daemon=self.spec.cluster.shim_daemon, timeout=timeout)

    def resume(self, status: dict[str, Any], timeout: float | None = RESUME_TIMEOUT_S) -> dict[str, Any]:
        """Start this replica's own exited container again, keeping its state.

        Without this a crashed runtime stays down: the shim never restarts it
        and its routes are gone, so nothing remote can reach it. The shim
        refuses anything but a running allocation's existing container, so a
        stop or delete that raced this probe stays final."""
        now = time.monotonic()
        if now < self.resume_at:
            return status
        self.resumes += 1
        self.resumed_at = now
        first, last = TEE_RESUME_BACKOFF_S if self.spec.app.runtime.driver in TEE_DRIVERS else RESUME_BACKOFF_S
        self.resume_at = now + min(last, first * 2 ** (self.resumes - 1))
        logging.getLogger(__name__).warning("shim runtime for %s exited; resuming it (attempt %d)",
                                            self.spec.name, self.resumes)
        try:
            return self.call({"operation": "resume", "execution": self.spec.execution}, timeout=timeout)
        except ShimPending:
            # A TEE resume outlasts a probe while it is admitted; the shim
            # reports it as starting until it ends.
            logging.getLogger(__name__).info("shim resume of %s continues in the daemon", self.spec.name)
            return {**status, "state": "starting"}
        except (RuntimeError, OSError, ValueError):
            logging.getLogger(__name__).warning("shim resume failed for %s", self.spec.name)
            return status

    def health(self) -> dict[str, Any]:
        status = self.call({"operation": "inspect", "execution": self.spec.execution})
        if status.get("state") == "stopped" and self.resumable:
            status = self.resume(status)
        if status.get("state") == "ready" and (self.spec.cluster.shim_daemon or
                self.network is not None and self.network.poll() is not None):
            try:
                self.start_network(timeout=RECOVERY_TIMEOUT_S)
            except (RuntimeError, OSError, ValueError):
                # The observed container and repair endpoint remain valid even
                # if re-registering its connector temporarily fails. Router
                # leases independently fence actual reachability.
                logging.getLogger(__name__).warning("shim connector recovery failed for %s", self.spec.name)
        if status.get("state") == "ready" and self.logs is not None:
            # A followed log ends with its container; a resumed one is followed again.
            self.logs.ensure()
        ready = status.get("state") == "ready" and self.application_ready()
        if ready and self.resumes and time.monotonic() - self.resumed_at >= RESUME_STABLE_S:
            self.resumes, self.resume_at = 0, 0.0
        return ReplicaHealth(name=self.spec.name, spec=actors.spec_hash(self.spec), executor=ExecutorKind.SHIM,
            execution=self.spec.execution,
            gpus=self.gpus, port=self.port, machine=socket.gethostname(),
            management_ready=status.get("state") == "ready", ready=ready).dump()

    def application_ready(self) -> bool:
        if self.spec.app.service_protocol is ServiceProtocol.HTTP1:
            return actors._http_ok(self.health_url)
        try:
            with socket.create_connection(("127.0.0.1", self.port), timeout=3):
                return True
        except OSError:
            return False


ShimApp = ray.remote(_ShimApp)


class _ProvisionedApp:
    """Holds Ray resources; private creation credentials never enter the actor."""
    def __init__(self, spec: AppReplicaSpec) -> None:
        self.spec = spec
        # Reported to Fleet, which requires the native launch to attach exactly these.
        self.gpus = actors._granted_gpus() if spec.app.gpus else []
        status = self.call("allocate")
        if status.get("phase") not in ("reserved", "authorizing", "dispatched", "created"):
            raise RuntimeError("native allocation requires explicit cleanup")

    def call(self, operation: str, timeout: float | None = None, **fields: Any) -> dict[str, Any]:
        return invoke(self.spec.cluster.shim_binary, self.spec.cluster.shim_state_dir, socket.gethostname(),
            {"operation": operation, "execution": self.spec.execution, "driver": self.spec.app.runtime.driver, **fields},
            daemon=True, timeout=timeout)

    def health(self) -> dict[str, Any]:
        status = self.call("inspect")
        if self.spec.app.services and status.get("platform_ready"):
            try:
                self.call("network", timeout=RECOVERY_TIMEOUT_S, config=self.spec.cluster.shim_network)
            except (RuntimeError, OSError, ValueError):
                logging.getLogger(__name__).warning("native shim connector recovery failed for %s", self.spec.name)
        # A local health verdict is independent of connector registration.
        # Fleet joins it with authenticated gateway policy readiness.
        ready = bool(self.spec.app.services and status.get("platform_ready") and status.get("application_ready"))
        return ReplicaHealth(name=self.spec.name, spec=actors.spec_hash(self.spec), executor=ExecutorKind.SHIM,
            execution=self.spec.execution, machine=socket.gethostname(), ready=ready, gpus=self.gpus,
            native_observation_base64=status.get("native_observation_base64"),
            allocation_reserved=status.get("phase") in ("reserved", "authorizing", "dispatched", "created")).dump()


ProvisionedApp = ray.remote(_ProvisionedApp)


def daemon_config(path: Path) -> ControlPlaneConfig:
    """Service bootstrap reads only its own settings; bad app entries remain
    repairable through the node's control connection."""
    import yaml
    from .model import from_dict
    data = yaml.safe_load(path.read_text()) or {}
    if not isinstance(data, dict):
        raise ValueError("apps.yaml must contain a mapping")
    return from_dict(ControlPlaneConfig, {key:data[key] for key in
        ("shim_binary", "shim_state_dir", "shim_daemon", "shim_openshell", "shim_machine_labels", "shim_tee") if key in data})


def daemon_options(config: ControlPlaneConfig) -> dict[str, Any]:
    options = {"machine": machine_options(config)} if config.shim_machine_labels or config.shim_tee else {}
    if config.shim_openshell is None:
        return options
    native = to_dict(config.shim_openshell)
    # Preserve the running provisioning-only companion's configuration digest.
    if native["supervisor_image"] is None:
        del native["supervisor_image"]
    return {**options, "openshell": native}


def machine_options(config: ControlPlaneConfig) -> dict[str, Any]:
    # The CPU overcommit opt-in is the bridge's to enforce, not the shim's: the running
    # shim's machine configuration, and its digest, stay as they are.
    tee = {k: v for k, v in to_dict(config.shim_tee).items() if k != "allow_cpu_overcommit"} if config.shim_tee else None
    return {"labels": config.shim_machine_labels, **({"tee": tee} if tee is not None else {})}


def check_machine(config: ControlPlaneConfig, value: dict[str, Any]) -> None:
    actual = value.get("machine_config_sha256")
    # Old companions remain compatible only when no machine selection was requested.
    if actual is None and not config.shim_machine_labels and not config.shim_tee:
        return
    encoded = json.dumps(machine_options(config), sort_keys=True,
        separators=(",", ":"), ensure_ascii=False).encode()
    if actual != hashlib.sha256(encoded).hexdigest():
        raise RuntimeError("running shim machine settings differ; reconcile the companion explicitly before starting the node")


def check_openshell(config: ControlPlaneConfig, value: dict[str, Any]) -> None:
    expected = None
    if config.shim_openshell is not None:
        encoded = json.dumps(daemon_options(config)["openshell"], sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        expected = hashlib.sha256(encoded).hexdigest()
        if "openshell-provisioning-v1" not in value.get("features", []):
            raise RuntimeError("running shim does not support native OpenShell provisioning")
    if value.get("openshell_config_sha256") != expected:
        raise RuntimeError("running shim OpenShell settings differ; reconcile the companion explicitly before starting the node")


def check_daemon(config: ControlPlaneConfig, wait_seconds: float = 0) -> dict[str, Any] | None:
    """The running companion's capabilities, once they match `config`; None without one."""
    if not config.shim_daemon:
        return None
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
    check_machine(config, value)
    return value


def check(path: Path, wait_seconds: float = 0, *, network: bool = False) -> list[str]:
    """The `check` operation. The node-wide settings must load the way the node's report
    reads them, and a persistent companion must match them. A bad app entry is a warning:
    node startup runs this check, and an agent that stays offline cannot have that entry
    fixed or disabled through the scheduler. A tunnel helper that shim cannot use withdraws
    only the network capabilities, so those problems are also returned as warnings, or
    raised with `network`."""
    import yaml
    from .model import AppsConfig, from_dict
    data = yaml.safe_load(path.read_text()) or {}
    if not isinstance(data, dict):
        raise ValueError("apps.yaml must contain a mapping")
    config = from_dict(ControlPlaneConfig, {k: v for k, v in data.items() if k != "apps"})
    warnings = []
    try:
        AppsConfig.parse(data, validate_launch=False)
    except (ValueError, TypeError, KeyError, AttributeError) as error:
        warnings.append(f"apps.yaml: {error}; the node still starts, so the entry can be fixed or disabled "
                        "through the scheduler")
    value = check_daemon(config, wait_seconds)
    helper = config.shim_network.tunnel_binary if config.shim_network else None
    if helper is None:
        return warnings
    problems = []
    reason = _helper_problem(helper) if os.path.isabs(helper) else "not an absolute path"
    if reason:
        problems.append(f"shim_network.tunnel_binary {helper}: {reason}; it must be an absolute path "
            "to a regular executable that group and others cannot write")
    features: Any = []
    try:
        if value is None and config.shim_binary and os.path.isabs(config.shim_binary):
            # A per-call shim answers for itself, the way capabilities() asks it.
            value = json.loads(subprocess.run([config.shim_binary, "--capabilities"],
                capture_output=True, timeout=5, check=True).stdout)
        features = value.get("features") if isinstance(value, dict) else []
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        # Say why: a shim that cannot answer is not one that lacks the feature.
        problems.append(f"could not ask {config.shim_binary} for its capabilities ({error}), so "
                        "chisel-network-v1, which tunnel_binary needs, is unconfirmed")
    else:
        if not isinstance(features, list) or "chisel-network-v1" not in features:
            problems.append("the running shim does not report chisel-network-v1, which tunnel_binary needs")
    if not problems:
        return warnings
    message = "; ".join(problems) + "; the node withdraws its network capabilities until this is fixed"
    if network:
        raise RuntimeError(message)
    return warnings + [message]


def main() -> None:
    """Installer/service entry points use the existing apps.yaml exclusively."""
    import argparse
    import sys

    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=["check", "serve", "socket"])
    parser.add_argument("--apps", required=True)
    parser.add_argument("--wait-seconds", type=float, default=0)
    parser.add_argument("--network", action="store_true", help="check only: fail instead of warning when "
        "shim_network.tunnel_binary is not an absolute regular executable that group and others cannot write, "
        "or the running shim does not report chisel-network-v1")
    args = parser.parse_args()
    if args.operation == "check":
        try:
            warnings = check(Path(args.apps), args.wait_seconds, network=args.network)
        except Exception as error:
            raise SystemExit(f"shim check failed: {error}") from None
        for warning in warnings:
            print(f"WARNING: {warning}", file=sys.stderr)
        return
    config = daemon_config(Path(args.apps))
    if not config.shim_daemon:
        if args.operation == "serve":
            parser.error("shim_daemon must be enabled in apps.yaml")
        return
    if args.operation == "socket":
        if config.shim_openshell is not None:
            check_daemon(config, args.wait_seconds)
            if "shim-openshell-provisioning-v1" not in capabilities(config):
                raise RuntimeError("running companion does not support durable native allocations")
            print(Path(config.shim_state_dir) / "control.sock")
        return
    if args.operation == "serve":
        options = daemon_options(config)
        command = [config.shim_binary, "--serve", "--state-dir", config.shim_state_dir,
            "--node-id", socket.gethostname()]
        if options:
            command += ["--options", json.dumps(options)]
        os.execv(config.shim_binary, command)


if __name__ == "__main__":
    main()
