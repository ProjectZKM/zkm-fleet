"""The bridge's data model: typed records, and one codec that reads and writes them.

Execution records, workload configuration, replicas and health reports use
dataclasses, with enums for closed sets. The codecs validate persisted records
and configuration against their field types. Node command and heartbeat
envelopes remain protocol mappings.

Loading is lenient about extra keys (an older or newer record still loads) and
strict about types; `merge` refuses a field the record does not have.
"""

from __future__ import annotations

import dataclasses
import enum
import json
import os
import re
import types
import typing
from dataclasses import dataclass, field, fields, is_dataclass
from typing import Any, Mapping, TypeVar
from urllib.parse import urlsplit

T = TypeVar("T")
D = TypeVar("D", bound="Record")

_NONE = type(None)


class ModelError(ValueError):
    """A record that does not fit its type."""


# ── reflection: dict <-> dataclass ───────────────────────────────────────────

def _hints(cls: type) -> dict[str, Any]:
    return typing.get_type_hints(cls, include_extras=False)


def _is_union(tp: Any) -> bool:
    return typing.get_origin(tp) in (typing.Union, types.UnionType)


def _coerce(tp: Any, value: Any, where: str) -> Any:
    """`value` as a `tp`, converting what JSON and YAML flatten (tuples, enums,
    nested records); raises ModelError naming the field otherwise."""
    if tp is Any:
        return value
    if _is_union(tp):
        options = typing.get_args(tp)
        if value is None and _NONE in options:
            return None
        errors = []
        for opt in options:
            if opt is _NONE:
                continue
            try:
                return _coerce(opt, value, where)
            except ModelError as e:
                errors.append(str(e))
        raise ModelError(f"{where}: {value!r} fits none of {tp} ({'; '.join(errors)})")
    origin = typing.get_origin(tp)
    if origin in (list, set, frozenset):
        if not isinstance(value, (list, tuple, set, frozenset)):
            raise ModelError(f"{where}: expected a list, got {type(value).__name__}")
        (item,) = typing.get_args(tp) or (Any,)
        return origin(_coerce(item, v, f"{where}[{i}]") for i, v in enumerate(value))
    if origin is tuple:
        if not isinstance(value, (list, tuple)):
            raise ModelError(f"{where}: expected a tuple, got {type(value).__name__}")
        args = typing.get_args(tp)
        if len(args) == 2 and args[1] is Ellipsis:
            return tuple(_coerce(args[0], v, f"{where}[{i}]") for i, v in enumerate(value))
        if len(args) != len(value):
            raise ModelError(f"{where}: expected {len(args)} items, got {len(value)}")
        return tuple(_coerce(a, v, f"{where}[{i}]") for i, (a, v) in enumerate(zip(args, value)))
    if origin is dict or origin is Mapping:
        if not isinstance(value, Mapping):
            raise ModelError(f"{where}: expected a mapping, got {type(value).__name__}")
        kt, vt = typing.get_args(tp) or (Any, Any)
        return {_coerce(kt, k, f"{where}.key"): _coerce(vt, v, f"{where}[{k!r}]") for k, v in value.items()}
    if origin is typing.Literal:
        if value not in typing.get_args(tp):
            raise ModelError(f"{where}: {value!r} is not one of {typing.get_args(tp)}")
        return value
    if isinstance(tp, type):
        if is_dataclass(tp):
            if isinstance(value, tp):
                return value
            if not isinstance(value, Mapping):
                raise ModelError(f"{where}: expected a {tp.__name__} mapping, got {type(value).__name__}")
            return from_dict(tp, value, strict=getattr(tp, "STRICT_FIELDS", False), where=where)
        if issubclass(tp, enum.Enum):
            if isinstance(value, tp):
                return value
            try:
                return tp(value)
            except ValueError:
                raise ModelError(f"{where}: {value!r} is not one of {[m.value for m in tp]}") from None
        if tp is bool:
            if isinstance(value, bool):
                return value
            raise ModelError(f"{where}: expected a bool, got {type(value).__name__}")
        if tp is float and isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
        if tp is int and isinstance(value, float) and value.is_integer():
            return int(value)
        if isinstance(value, tp) and not (tp is int and isinstance(value, bool)):
            return value
        raise ModelError(f"{where}: expected {tp.__name__}, got {type(value).__name__} {value!r}")
    raise ModelError(f"{where}: unsupported type {tp!r}")


def from_dict(cls: type[T], data: Mapping[str, Any], *, strict: bool = False, where: str | None = None) -> T:
    """A `cls` from a mapping; missing fields take their defaults, extra keys are
    ignored (strict=False) or refused."""
    where = where or cls.__name__
    if not isinstance(data, Mapping):
        raise ModelError(f"{where}: expected a mapping")
    hints = _hints(cls)
    names = {f.name for f in fields(cls)}  # type: ignore[arg-type]
    extra = set(data) - names
    if strict and extra:
        raise ModelError(f"{where}: unknown fields {sorted(extra)}")
    kwargs: dict[str, Any] = {}
    for f in fields(cls):  # type: ignore[arg-type]
        if f.name in data:
            kwargs[f.name] = _coerce(hints[f.name], data[f.name], f"{where}.{f.name}")
        elif f.default is dataclasses.MISSING and f.default_factory is dataclasses.MISSING:
            raise ModelError(f"{where}: missing field {f.name}")
    return cls(**kwargs)


def to_dict(obj: Any) -> Any:
    """JSON-ready: records become mappings, enums their values, tuples lists."""
    if is_dataclass(obj) and not isinstance(obj, type):
        return {f.name: to_dict(getattr(obj, f.name)) for f in fields(obj)
                if not (f.metadata.get("omit_none") and getattr(obj, f.name) is None)}
    if isinstance(obj, enum.Enum):
        return obj.value
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [to_dict(v) for v in obj]
    if isinstance(obj, Mapping):
        return {str(k): to_dict(v) for k, v in obj.items()}
    return obj


def merge(obj: D, **changes: Any) -> D:
    """A copy of `obj` with the given fields replaced; a name it has no field for, or a
    value of the wrong type, is an error (the record's shape is its type)."""
    hints = _hints(type(obj))
    names = {f.name for f in fields(obj)}
    bad = set(changes) - names
    if bad:
        raise ModelError(f"{type(obj).__name__} has no field {sorted(bad)}")
    coerced = {k: _coerce(hints[k], v, f"{type(obj).__name__}.{k}") for k, v in changes.items()}
    return dataclasses.replace(obj, **coerced)


def to_env(obj: Any) -> dict[str, str]:
    """Environment variables from a record whose fields carry `metadata={"env": NAMES}`
    (one name or several, for a value that goes out under aliases); None is left out."""
    out: dict[str, str] = {}
    for f in fields(obj):
        names = f.metadata.get("env")
        value = getattr(obj, f.name)
        if not names or value is None:
            continue
        text = json.dumps(value) if isinstance(value, bool) else str(to_dict(value))  # true/false, as a parser expects
        for name in ([names] if isinstance(names, str) else names):
            out[name] = text
    return out


# ── records ──────────────────────────────────────────────────────────────────

@dataclass(kw_only=True)
class Record:
    """A dataclass that knows how to load and save itself."""

    @classmethod
    def load(cls: type[D], data: Mapping[str, Any], *, strict: bool = False) -> D:
        return from_dict(cls, data, strict=strict)

    def dump(self) -> dict[str, Any]:
        return dict(to_dict(self))

    @classmethod
    def read(cls: type[D], path: str) -> D:
        with open(path) as f:
            return cls.load(json.load(f))

    def write(self, path: str) -> None:
        """Atomic: a reader sees the old record or the new one, never half."""
        tmp = f"{path}.tmp"
        with open(tmp, "w") as f:
            json.dump(self.dump(), f, indent=2)
        os.replace(tmp, path)

    def merged(self: D, **changes: Any) -> D:
        return merge(self, **changes)


# ── batch jobs ────────────────────────────────────────────────────────────

class JobState(str, enum.Enum):
    WAITING_DEPENDENCY = "waiting_dependency"
    WAITING_QUOTA = "waiting_quota"
    QUEUED = "queued"
    WAITING_VRAM = "waiting_vram"
    RUNNING = "running"
    RETRYING = "retrying"
    PREEMPTED = "preempted"  # being stopped so an app gets its cards; it queues again
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    LOST = "lost"

    @property
    def terminal(self) -> bool:
        return self in (JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELLED)


class FailureCause(str, enum.Enum):
    """Why an attempt ended short of exit 0."""

    GPU_OOM = "gpu-oom"
    HOST_OOM = "host-oom"
    EXIT_CODE = "exit-code"
    DOCKER_RUN = "docker-run"
    RANK_LOST = "rank-lost"
    PREEMPTED = "preempted"  # an app took the cards; spends no --retry

    @property
    def oom(self) -> bool:
        return self in (FailureCause.GPU_OOM, FailureCause.HOST_OOM)


class MountMode(str, enum.Enum):
    RO = "ro"
    RW = "rw"


@dataclass(kw_only=True)
class Mount(Record):
    path: str
    mode: MountMode = MountMode.RW

    @classmethod
    def parse(cls, text: str) -> Mount:
        """`PATH` or `PATH:ro` / `PATH:rw` -> a normalised absolute path and its mode."""
        path, _, mode = text.partition(":")
        if mode and mode not in (m.value for m in MountMode):
            raise SystemExit(f"--mount {text}: mode must be ro or rw")
        if not path.startswith("/") or ".." in path.split("/"):
            raise SystemExit(f"--mount {text}: needs an absolute path without '..'")
        return cls(path=os.path.normpath(path), mode=MountMode(mode) if mode else MountMode.RW)

    def docker(self) -> str:
        return f"{self.path}:{self.path}" + (":ro" if self.mode is MountMode.RO else "")


@dataclass(kw_only=True)
class Batch(Record):
    """A batches.yaml batch: what a request may ask for, and the container it gets."""

    image: str
    # Operator-managed YAML defaults to the reserved system namespace.
    # New API registrations must supply an existing namespace explicitly.
    namespace: str = "00000000000000000000000000000000"
    priority: int = 0
    user: str | None = None
    max_gpus: int = 8
    default_gpu_mem_gb: int = 0
    shm_gb: int = 16
    allow: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    disabled: bool = False  # takes no new jobs (fleetctl batch disable); the ones going finish
    # A higher-priority requester in this namespace may stop a job once it has run
    # min_runtime_s; the job queues again (FLEET_ATTEMPT goes on) and spends no --retry.
    preemptible: bool = True
    min_runtime_s: int = 300
    # SIGTERM, then SIGKILL this much later: on cancel and on preemption (time to checkpoint).
    stop_timeout_s: int = 30

    def __post_init__(self) -> None:
        if type(self.priority) is not int or not -(2**31) <= self.priority < 2**31:
            raise ModelError("priority must be a signed 32-bit integer")


@dataclass(kw_only=True)
class Batches(Record):
    """batches.yaml."""

    batches: dict[str, Batch] = field(default_factory=dict)


@dataclass(kw_only=True)
class JobSpec(Record):
    """Everything a job was asked to be; written once at submit, read by its actor."""

    id: str
    batch: str
    owner: str | None = None
    namespace: str | None = None
    tag: str | None = None
    policy: Batch
    command: list[str]
    gpus: float
    cpus: int
    out: str
    mounts: list[Mount] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    log: str | None = None
    workdir: str | None = None
    gpu_mem_gb: int = 0
    vram_wait_s: int = 1800
    retry: int = 0
    restart_on_oom: int = 0
    nodes: int = 1

    def expand(self, text: str) -> str:
        """`{out}` in a command, env value or workdir."""
        return text.replace("{out}", self.out)

    @property
    def need_mib(self) -> int | None:
        """Free card memory the job needs, None for the whole card."""
        return self.gpu_mem_gb * 1024 or None

    @property
    def log_path(self) -> str:
        return self.log or os.path.join(os.path.dirname(self.out), "log")

    def rank_log_path(self, rank: int) -> str:
        return self.log_path if rank == 0 else f"{self.log_path}.rank{rank}"


@dataclass(kw_only=True)
class GpuProcess(Record):
    pid: int
    mib: int
    cmd: str

    def __str__(self) -> str:
        return f"pid {self.pid} {self.mib} MiB ({self.cmd})"


@dataclass(kw_only=True)
class GpuMemoryState(Record):
    """A host card as nvidia-smi sees it."""

    index: str
    free_mib: int
    total_mib: int
    holders: list[GpuProcess] = field(default_factory=list)

    def held_by(self) -> str:
        return ", ".join(map(str, self.holders)) or "no process"


@dataclass(kw_only=True)
class TaskPlacement(Record):
    """Where one rank of a job lives."""

    rank: int
    machine: str
    node_ip: str
    gpus: list[str]
    container: str


@dataclass(kw_only=True)
class JobAttempt(Record):
    """How one start of the job ended."""

    exit_code: int | None
    cause: FailureCause | None = None
    evidence: str | None = None
    error: str | None = None
    card_holders: str | None = None
    rank: int | None = None
    machine: str | None = None

    @property
    def ok(self) -> bool:
        return self.exit_code == 0

    @property
    def reason(self) -> str | None:
        return self.error or self.evidence


@dataclass(kw_only=True)
class JobStatus(Record):
    """status.json: written by submit, the waiter, the actor and cancel; read by all."""

    id: str
    state: JobState
    namespace: str | None = None
    tag: str | None = None
    priority: int = 0  # inherited from the app at submission
    preempt_requeue: bool = False  # durable restart intent after confirmed cleanup
    launch_pending: bool = False  # durable intent, acknowledged by the actor before execution
    quota_released: bool = False  # terminal cleanup is recorded once, across API restarts
    submitted_at: float
    gpus_requested: float
    cpus: int
    gpu_mem_gb: int | None = None  # None: the whole card
    after: str | None = None
    nodes: int = 1
    machine: str | None = None
    node_ip: str | None = None
    ranks: list[TaskPlacement] = field(default_factory=list)
    gpus: list[str] = field(default_factory=list)
    container: str | None = None
    attempt: int | None = None
    attempts: list[JobAttempt] = field(default_factory=list)
    preemptions: int = 0
    started_at: float | None = None
    finished_at: float | None = None
    exit_code: int | None = None
    cause: FailureCause | None = None
    error: str | None = None
    detail: str | None = None
    # filled in by `status` for the reader, never stored
    metrics: dict[str, Any] | None = None
    out: str | None = None

    def places(self) -> list[TaskPlacement]:
        """Every rank's place (rank 0's from the top-level fields for a single-node job)."""
        if self.ranks:
            return self.ranks
        return [TaskPlacement(rank=0, machine=self.machine or "", node_ip=self.node_ip or "", gpus=self.gpus,
                         container=self.container or "")]


# ── apps ─────────────────────────────────────────────────────────────────────

class AppRuntime(str, enum.Enum):
    ZIREN_GPU = "ziren-gpu"
    ZIREN_CPU = "ziren-cpu"
    APP = "app"

    @property
    def takes_gpus(self) -> bool:
        return self is not AppRuntime.ZIREN_CPU


@dataclass(kw_only=True)
class TrusteeTlsConfig(Record):
    STRICT_FIELDS: typing.ClassVar[bool] = True
    ca_file: str
    cert_file: str
    key_file: str

    def __post_init__(self) -> None:
        if any(not os.path.isabs(p) for p in (self.ca_file, self.cert_file, self.key_file)):
            raise ModelError("Trustee TLS paths must be absolute")


@dataclass(kw_only=True)
class ShimDcapConfig(Record):
    STRICT_FIELDS: typing.ClassVar[bool] = True
    collateral_file: str
    min_tcb_evaluation_data_number: int

    def __post_init__(self) -> None:
        if not os.path.isabs(self.collateral_file) or not 0 < self.min_tcb_evaluation_data_number <= 0xffffffff:
            raise ModelError("DCAP requires an absolute collateral path and positive TCB floor")


@dataclass(kw_only=True)
class ShimTeeConfig(Record):
    STRICT_FIELDS: typing.ClassVar[bool] = True
    mode: typing.Literal["hardware", "simulation"]
    platform: str
    evidence_endpoint: str
    trustee_endpoint: str = ""
    trustee_policy_ids: list[str] = field(default_factory=list)
    trustee_tls: TrusteeTlsConfig | None = None
    dcap: ShimDcapConfig | None = None
    init_data_sha256: str
    endpoint_key_sha256: str
    expected_measurement: str
    proof_endpoint: str
    proof_token_file: str
    program: dict[str, str]
    program_vkey_hash: str
    elf_file: str
    proof_signer_file: str | None = None

    def __post_init__(self) -> None:
        endpoint = urlsplit(self.trustee_endpoint)
        if not self.trustee_endpoint:
            if self.trustee_tls is not None or self.trustee_policy_ids or self.dcap is None:
                raise ModelError("AS-free attestation requires DCAP; orphan Trustee settings are invalid")
        elif (endpoint.username is not None or endpoint.password is not None or endpoint.query or endpoint.fragment
                or endpoint.path not in ("", "/") or not endpoint.hostname
                or not ((endpoint.scheme == "https" and self.trustee_tls is not None)
                        or (endpoint.scheme == "http" and endpoint.hostname in ("127.0.0.1", "::1") and self.trustee_tls is None))):
            raise ModelError("Trustee requires HTTPS with node TLS credentials, or loopback HTTP")
        if self.dcap is not None and (self.mode != "hardware" or self.platform != "tdx"):
            raise ModelError("DCAP startup requires hardware TDX")
        if self.dcap is not None and not re.fullmatch(r"[0-9a-f]{64}", self.expected_measurement):
            raise ModelError("DCAP requires the complete measured-profile SHA256")
        if ((self.mode == "simulation" and self.platform != "sample")
                or (self.mode == "hardware" and self.platform not in ("snp", "tdx"))):
            raise ModelError("TEE mode and platform disagree")
        if any(not re.fullmatch(r"[0-9a-f]{64}", h) for h in (self.init_data_sha256, self.endpoint_key_sha256)):
            raise ModelError("TEE bindings require SHA256 digests")
        if self.program.get("kind") != "remote_attestation" or set(self.program) != {"kind", "program_sha256", "verifying_key_sha256"}:
            raise ModelError("TEE requires a pinned remote-attestation program")
        if any(not os.path.isabs(p) for p in (self.proof_token_file, self.elf_file, *([self.proof_signer_file] if self.proof_signer_file else []))):
            raise ModelError("TEE artifact and credential paths must be absolute")

    @property
    def driver(self) -> str:
        return "tee" if self.mode == "hardware" else "tee-simulation"


@dataclass(kw_only=True)
class ControlPlaneConfig(Record):
    """The top of apps.yaml: what every replica shares."""

    scheduler: str | None = None
    scheduler_ca_file: str | None = None
    cluster_token_file: str | None = None
    node_binary: str | None = None
    log_dir: str = "/tmp/zkm-fleet"
    gpu_port_range: tuple[int, int] = (30100, 30199)
    shim_binary: str | None = None
    shim_state_dir: str | None = None
    shim_daemon: bool = False
    shim_network: ShimNetworkConfig | None = None
    shim_openshell: ShimOpenShellConfig | None = None
    shim_machine_labels: dict[str, str] = field(default_factory=dict)
    shim_tee: ShimTeeConfig | None = None

    def __post_init__(self) -> None:
        if (self.shim_machine_labels or self.shim_tee) and not self.shim_daemon:
            raise ModelError("machine labels require the persistent shim daemon")
        if self.shim_openshell is not None and not self.shim_daemon:
            raise ModelError("OpenShell requires the persistent shim daemon")
        if self.shim_daemon:
            if not all(p and os.path.isabs(p) and "\x00" not in p for p in (self.shim_binary, self.shim_state_dir)):
                raise ModelError("shim daemon requires absolute shim_binary and shim_state_dir")
            if len(os.fsencode(os.path.join(self.shim_state_dir, "control.sock"))) >= 108:
                raise ModelError("shim daemon requires a Unix socket path under 108 bytes")


@dataclass(kw_only=True)
class ShimOpenShellConfig(Record):
    """Operator endpoints only; per-allocation credentials never enter apps.yaml."""
    STRICT_FIELDS: typing.ClassVar[bool] = True
    socket: str
    admission_policy: str
    ledger_endpoint: str
    ledger_ca_file: str | None = None
    supervisor_image: str | None = None

    def __post_init__(self) -> None:
        from urllib.parse import urlsplit
        url = urlsplit(self.ledger_endpoint)
        if (url.scheme != "https" and not (url.scheme == "http" and url.hostname in ("127.0.0.1", "::1"))
                or not url.hostname or url.username or url.password or url.query or url.fragment or url.path not in ("", "/")):
            raise ModelError("OpenShell policy gateway requires TLS without credentials or a path")
        if (not os.path.isabs(self.socket) or "\x00" in self.socket or len(os.fsencode(self.socket)) >= 108
                or not self.admission_policy or len(self.admission_policy.encode()) > 65536):
            raise ModelError("OpenShell needs an absolute private driver socket and admission acknowledgement")
        if self.ledger_ca_file is not None and (url.scheme != "https" or not os.path.isabs(self.ledger_ca_file) or "\x00" in self.ledger_ca_file):
            raise ModelError("OpenShell private CA requires HTTPS and an absolute certificate path")
        if self.supervisor_image is not None and not re.fullmatch(r"sha256:[0-9a-f]{64}", self.supervisor_image):
            raise ModelError("OpenShell supervisor requires an immutable local Docker image ID")


@dataclass(kw_only=True)
class ShimNetworkConfig(Record):
    STRICT_FIELDS: typing.ClassVar[bool] = True
    gateway_url: str
    relay_url: str
    token_file: str
    public_key_file: str
    issuer: str
    tunnel_binary: str | None = field(default=None, metadata={"omit_none": True})

    def __post_init__(self) -> None:
        from urllib.parse import urlsplit
        for value, secure, local in [(self.gateway_url,"https","http"),(self.relay_url,"wss","ws")]:
            url=urlsplit(value)
            if (url.scheme != secure and not (url.scheme == local and url.hostname in ("127.0.0.1","::1"))
                    or not url.hostname or url.username or url.password or url.query or url.fragment or url.path not in ("","/")):
                raise ModelError("shim network requires TLS URLs without credentials or paths")
        if not self.issuer or not all(os.path.isabs(p) and "\x00" not in p for p in (self.token_file,self.public_key_file)):
            raise ModelError("shim network needs an issuer and absolute credential/key paths")
        if self.tunnel_binary is not None and (not os.path.isabs(self.tunnel_binary) or "\x00" in self.tunnel_binary):
            raise ModelError("upstream tunnel helper requires an absolute node-owned path")


class ServiceProtocol(str, enum.Enum):
    HTTP1 = "http1"
    TCP = "tcp"


class ExecutorKind(str, enum.Enum):
    LEGACY = "legacy"
    SHIM = "shim"


@dataclass(kw_only=True)
class RuntimeSelection(Record):
    STRICT_FIELDS: typing.ClassVar[bool] = True
    executor: ExecutorKind
    driver: str | None = None

    def __post_init__(self) -> None:
        if (self.executor is ExecutorKind.LEGACY and self.driver is not None
                or self.executor is ExecutorKind.SHIM and self.driver not in ("docker", "openshell", "tee", "tee-simulation")):
            raise ModelError("runtime must select legacy or shim with driver docker, openshell, tee or tee-simulation")


@dataclass(kw_only=True)
class HttpIngress(Record):
    STRICT_FIELDS: typing.ClassVar[bool] = True
    paths: list[str]
    methods: list[str]
    bearer_env: str | None = None
    single_use: bool = False

    def __post_init__(self) -> None:
        if (not 1 <= len(self.paths) <= 32 or any(not p.startswith("/") or len(p)>256 or not p.isascii()
                or any(c in p for c in "%?#\\") or any(c in p.split("/") for c in (".",".."))
                or any(ord(c)<32 or ord(c)==127 for c in p) for p in self.paths)
                or not 1 <= len(self.methods) <= 7 or any(m not in ("GET","HEAD","POST","PUT","PATCH","DELETE","OPTIONS") for m in self.methods)
                or self.bearer_env is not None and not re.fullmatch(r"[A-Za-z0-9_]{1,128}", self.bearer_env)):
            raise ModelError("invalid HTTP ingress")


@dataclass(kw_only=True)
class DurableMount(Record):
    """A native replica's workload-scoped named volume; never a host path.
    The node's shim derives the volume from the execution and `name`."""

    STRICT_FIELDS: typing.ClassVar[bool] = True
    name: str
    target: str
    read_only: bool = False

    def __post_init__(self) -> None:
        parts = self.target[1:].split("/")
        if (not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", self.name)
                or len(self.target) > 1024 or not self.target.startswith("/")
                or any(not 33 <= ord(c) <= 126 or c in "\\,:" for c in self.target)
                or any(p in ("", ".", "..") for p in parts)):
            raise ModelError("invalid durable mount")


# At most this many GPUs or durable mounts per native replica, as in shim.
MAX_NATIVE_ATTACHMENTS = 16


@dataclass(kw_only=True)
class AppSpec(Record):
    """One entry of `apps:`."""

    name: str
    runtime: RuntimeSelection | None = None
    access_policy_sha256: str | None = None
    services: list[str] = field(default_factory=list)
    service_protocol: ServiceProtocol = ServiceProtocol.HTTP1
    ingress: dict[str, HttpIngress] = field(default_factory=dict)
    container_port: int | None = None
    entrypoint: str | None = None
    namespace: str = "00000000000000000000000000000000"
    tag: str | None = None
    priority: int = 0
    kind: AppRuntime = AppRuntime.ZIREN_GPU
    replicas: int = 1
    disabled: bool = False  # release resources until enabled
    # The app this entry runs, by its name in the entry's namespace; the entry's own
    # name by default. A Ziren prover's is a proof app.
    app: str = ""
    # The program a dedicated Ziren prover proves (its app's full ELF SHA-256): required for a
    # GPU prover, optional for a CPU one.
    elf_id: str = ""
    proof_storage_root: str | None = None
    gpus: float = 1
    cpus: float = 1
    memory_gb: float | None = None
    machine: str | None = None
    image: str | None = None
    env: dict[str, str] = field(default_factory=dict)
    volumes: list[str] = field(default_factory=list)
    # Native OpenShell only: durable named volumes that outlive generations.
    mounts: list[DurableMount] = field(default_factory=list)
    shm_gb: int | None = None
    ready_timeout_s: float = 600
    # Integrated app worker sharing the replica's dedicated GPU server.
    worker_command: list[str] = field(default_factory=list)
    worker_env_file: str | None = None
    worker_workdir: str | None = None
    worker_stop_timeout_s: int = 120
    # apps
    port_base: int | None = None
    command: list[str] = field(default_factory=list)
    user: str | None = None
    workdir: str | None = None
    health_path: str = "/"
    health_tcp_banner: str | None = None
    stop_timeout_s: int = 30
    # CPU provers
    backend: str = "cpu"
    rust_log: str = "info"
    @property
    def native_access(self) -> bool:
        return self.access_policy_sha256 is not None or bool(self.runtime and self.runtime.driver == "openshell")

    def __post_init__(self) -> None:
        if self.access_policy_sha256 is not None:
            if (not re.fullmatch(r"[0-9a-f]{64}", self.access_policy_sha256)
                    or not self.runtime or self.runtime.executor is not ExecutorKind.SHIM):
                raise ModelError("access policy requires a SHA256 commitment and shim runtime")
            # The scheduler admits user rules only on these drivers; legacy
            # `openshell` records are never placed with one.
            if self.runtime.driver not in ("docker", "tee", "tee-simulation"):
                raise ModelError("access policy requires the docker, tee or tee-simulation driver")
        if self.health_tcp_banner is not None and not (self.native_access and self.service_protocol is ServiceProtocol.TCP):
            raise ModelError("TCP banner health requires native OpenShell TCP services")
        if self.mounts and not self.native_access:
            raise ModelError("durable mounts require native OpenShell")
        if self.native_access:
            if (len(self.mounts) > MAX_NATIVE_ATTACHMENTS
                    or len({m.name for m in self.mounts}) != len(self.mounts)
                    or len({m.target for m in self.mounts}) != len(self.mounts)):
                raise ModelError("invalid durable mounts")
            # Whole GPUs only: shim attaches exactly the device ids Ray granted.
            if (self.gpus or self.mounts) and self.runtime.driver not in ("docker", "openshell"):
                raise ModelError("native GPUs and durable mounts require a non-TEE driver")
            if (not 0 <= self.gpus <= MAX_NATIVE_ATTACHMENTS or self.gpus != int(self.gpus) or not 0 < self.cpus <= 2**32 / 1000 or self.cpus != int(self.cpus)
                    or not self.memory_gb or not 6 * 1024**2 <= self.memory_gb * 2**30 < 2**64
                    # A native node holds no application credential: callers send their own.
                    or any(rule.bearer_env for rule in self.ingress.values())
                    or self.volumes or self.shm_gb is not None
                    or any(v is not None for v in (self.port_base, self.entrypoint, self.user, self.workdir))
                    # `control` is the logs-only service beside an application service.
                    or self.services and all(s == "control" for s in self.services)
                    or self.service_protocol is ServiceProtocol.TCP and (
                        self.health_path != "/" or not self.health_tcp_banner
                        or len(self.health_tcp_banner) > 128
                        or not all(32 <= ord(c) <= 126 for c in self.health_tcp_banner))
                    or bool(self.services) != (self.container_port is not None)
                    or not self.health_path.startswith("/") or self.health_path.startswith("//")
                    or len(self.health_path.encode()) > 2048 or any(ord(c) < 32 or ord(c) == 127 or c in "\\#" for c in self.health_path)
                    or not self.services and self.health_path != "/"):
                raise ModelError("native OpenShell requires CPU/memory limits, private HTTP services (ingress without bearer_env, control beside an application service) or TCP services with banner health")
        if self.service_protocol is ServiceProtocol.TCP:
            if (self.runtime is None or self.runtime.executor is not ExecutorKind.SHIM
                    or not any(s != "control" for s in self.services) or self.ingress):
                raise ModelError("TCP requires a shim application service without HTTP ingress")
        if self.services or self.ingress or self.container_port is not None or self.entrypoint is not None:
            if self.runtime is None or self.runtime.executor is not ExecutorKind.SHIM:
                raise ModelError("services, container_port and entrypoint require explicit shim execution")
        if (len(self.services) != len(set(self.services)) or len(self.services) > 8
                or any(not re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", s) for s in self.services)):
            raise ModelError("invalid runtime services")
        if any(name not in self.services or name == "control" for name in self.ingress):
            raise ModelError("ingress must reference declared application services")
        if any(rule.bearer_env and not self.env.get(rule.bearer_env) for rule in self.ingress.values()):
            raise ModelError("ingress bearer environment is missing")
        if self.container_port is not None and not 1 <= self.container_port <= 65535:
            raise ModelError("invalid container_port")
        if self.port_base == 0 and self.runtime and self.runtime.executor is ExecutorKind.SHIM and self.container_port is None:
            raise ModelError("automatic host ports require shim and container_port")
        if self.runtime is not None and self.kind is not AppRuntime.APP:
            if self.runtime != RuntimeSelection(executor=ExecutorKind.SHIM, driver="docker") or self.worker_command:
                raise ModelError("packaged provers require shim Docker and an image-owned worker")
            if self.port_base is None or not self.container_port or not self.image:
                raise ModelError("packaged provers require an image and health port")
            if self.services or self.ingress or self.service_protocol is not ServiceProtocol.HTTP1:
                raise ModelError("packaged SDK workers expose only a private HTTP health port")
        if type(self.priority) is not int or not -(2**31) <= self.priority < 2**31:
            raise ModelError("priority must be a signed 32-bit integer")
        if self.worker_command:
            if (self.kind is not AppRuntime.ZIREN_GPU or self.replicas != 1
                    or not self.machine or not self.proof_storage_root):
                raise ModelError("integrated worker requires a machine-pinned ziren-gpu app with one replica and persistent proof_storage_root")
            if not os.path.isabs(self.worker_command[0]) or self.worker_stop_timeout_s <= 0:
                raise ModelError("worker executable must be absolute and stop timeout positive")
        elif self.worker_env_file or self.worker_workdir:
            raise ModelError("worker environment and workdir require worker_command")
        self.app = self.app or self.name
        if not re.fullmatch(r"[A-Za-z0-9_.-]{1,63}", self.app):
            raise ModelError(f"{self.name}: app must be a name: letters, digits, _ . -, at most 63")
        if self.kind in (AppRuntime.ZIREN_GPU, AppRuntime.ZIREN_CPU):
            if self.elf_id and not re.fullmatch(r"[0-9a-fA-F]{64}", self.elf_id):
                raise ModelError(f"{self.name}: elf_id must be the program's full ELF SHA-256")
            if self.kind is AppRuntime.ZIREN_GPU and not self.elf_id:
                raise ModelError(f"{self.name}: a ziren-gpu app proves one program: set its elf_id")
            self.elf_id = self.elf_id.lower()
        else:
            if self.app != self.name:
                raise ModelError(f"{self.name}: a {self.kind.value} entry is its own app")
            if self.elf_id:
                raise ModelError(f"{self.name}: only a Ziren prover proves a program (elf_id)")


@dataclass(kw_only=True)
class AppsConfig(Record):
    """apps.yaml as a whole."""

    cluster: ControlPlaneConfig
    apps: list[AppSpec]
    raw: dict[str, Any] = field(default_factory=dict)  # the file's own mapping, for the spec hash

    @classmethod
    def parse(cls, data: Mapping[str, Any], *, validate_launch: bool = True) -> AppsConfig:
        cluster = from_dict(ControlPlaneConfig, {k: v for k, v in data.items() if k != "apps"})
        apps = [from_dict(AppSpec, a, where=f"app {a.get('name', '?')}") for a in data.get("apps", [])]
        for a in data.get("apps", []):
            if "runtime" in a:
                from_dict(RuntimeSelection, a["runtime"], strict=True)
        result = cls(cluster=cluster, apps=apps, raw=dict(data))
        if validate_launch:
            for app in apps:
                result.validate_launch(app)
        return result

    def validate_launch(self, app: AppSpec) -> None:
        cluster = self.cluster
        if app.runtime and app.runtime.executor is ExecutorKind.SHIM:
            if not all(p and os.path.isabs(p) and "\x00" not in p for p in (cluster.shim_binary, cluster.shim_state_dir)):
                raise ModelError("shim requires absolute shim_binary and shim_state_dir in existing apps.yaml")
        if app.services and cluster.shim_network is None:
            raise ModelError("runtime services require node-owned shim_network settings in existing apps.yaml")
        if app.runtime and app.runtime.driver in ("tee", "tee-simulation"):
            if not cluster.shim_daemon or not cluster.shim_tee or cluster.shim_tee.driver != app.runtime.driver:
                raise ModelError("TEE apps require a matching node TEE mode and persistent shim")
        if app.native_access and (not cluster.shim_daemon or cluster.shim_openshell is None):
            raise ModelError("OpenShell requires the configured persistent companion")


@dataclass(kw_only=True)
class RuntimeIdentity(Record):
    namespace: str
    workload: str
    allocation_id: str
    lease_id: str
    generation: int
    node_id: str

    def __post_init__(self) -> None:
        if self.generation < 1 or not all((self.namespace,self.workload,self.allocation_id,self.lease_id,self.node_id)):
            raise ModelError("runtime identity requires a positive generation and complete allocation")


@dataclass(kw_only=True)
class RuntimeInstance(Record):
    entry_name: str
    execution: RuntimeIdentity
    ready: bool
    services: list[str] = field(default_factory=list)
    native_observation_base64: str | None = None
    # Ray's device grant to a native reservation; the launch must attach exactly these.
    gpu_ids: list[str] = field(default_factory=list)


@dataclass(kw_only=True)
class AppReplicaSpec(Record):
    """One actor: an app's replica `index`, named `<app>-<index>`, with the
    cluster settings it needs. `config` is the file's own mapping for this replica,
    which the spec hash is taken from: a change in the bridge's code never counts as
    a change of the app, only an edit of the file does."""

    name: str
    index: int
    app: AppSpec
    cluster: ControlPlaneConfig
    config: dict[str, Any] = field(default_factory=dict)
    # Pinned machine allocation, supplied before entering Ray; absent for legacy.
    execution: RuntimeIdentity | None = None
    # How the shim launch request for `execution` is formed (shim.LAUNCH_REVISION),
    # supplied with it; specs pickled for a running actor carry none and mean 1.
    launch_revision: int = 1

    @property
    def kind(self) -> AppRuntime:
        return self.app.kind

    @property
    def machine(self) -> str | None:
        return self.app.machine

    def node_log_path(self) -> str:
        return os.path.join(self.cluster.log_dir, f"{self.name}.node.log")


@dataclass(kw_only=True)
class ResourceRequest(Record):
    """One machine's share of a placement group, in Ray's resource names."""

    cpu: float
    gpu: float = 0
    memory: float | None = None
    pin: str | None = None  # a `node:<ip>` resource that fixes the machine

    def to_ray(self) -> dict[str, float]:
        out: dict[str, float] = {"CPU": self.cpu}
        if self.gpu:
            out["GPU"] = self.gpu
        if self.memory:
            out["memory"] = self.memory
        if self.pin:
            out[self.pin] = 0.001
        return out


@dataclass(kw_only=True)
class NodeEnvironment(Record):
    """What fleet-ziren-node reads from its environment."""

    name: str = field(metadata={"env": "FLEET_NODE_NAME"})
    backend: str = field(metadata={"env": "FLEET_BACKEND"})
    scheduler: str = field(metadata={"env": "FLEET_SCHEDULER"})
    scheduler_ca_file: str | None = field(default=None, metadata={"env": "FLEET_SCHEDULER_CA_FILE"})
    cluster_token_file: str = field(metadata={"env": "FLEET_CLUSTER_TOKEN_FILE"})
    machine: str = field(metadata={"env": "FLEET_MACHINE"})
    app_namespace: str | None = field(default=None, metadata={"env": "FLEET_APP_NAMESPACE"})
    app: str | None = field(default=None, metadata={"env": "FLEET_APP"})
    app_elf_id: str | None = field(default=None, metadata={"env": "FLEET_APP_ELF_ID"})
    rust_log: str = field(metadata={"env": "RUST_LOG"})
    exit_with_parent: bool = field(default=True, metadata={"env": "FLEET_EXIT_WITH_PARENT"})
    app_storage_root: str | None = field(default=None, metadata={"env": "FLEET_APP_STORAGE_ROOT"})
    cpus: int = field(default=0, metadata={"env": "FLEET_CPUS"})
    gpus: int | None = field(default=None, metadata={"env": "FLEET_GPUS"})
    gpu_endpoint: str | None = field(default=None, metadata={"env": "FLEET_GPU_ENDPOINT"})
    image: str | None = field(default=None, metadata={"env": "FLEET_IMAGE"})
    rayon_threads: int | None = field(default=None, metadata={"env": "RAYON_NUM_THREADS"})


@dataclass(kw_only=True)
class ReplicaHealth(Record):
    """What an actor answers to `health()`."""

    name: str
    executor: ExecutorKind = ExecutorKind.LEGACY
    execution: RuntimeIdentity | None = None
    spec: str | None = None  # the replica's spec hash
    machine: str | None = None
    gpus: list[str] = field(default_factory=list)
    gpu_port: int | None = None
    gpu_ready: bool | None = None
    port: int | None = None
    ready: bool | None = None
    # Shim's authenticated local management listener can be available while the
    # application is unhealthy (for example during state restore).
    management_ready: bool = False
    # Durable native ownership may precede compute creation and route readiness.
    allocation_reserved: bool = False
    native_observation_base64: str | None = None
    stopping: bool = False
    error: str | None = None

    @property
    def is_ready(self) -> bool:
        if self.stopping or self.error or self.spec is None:
            return False
        if self.ready is not None:
            return self.ready
        if self.gpu_ready is not None:
            return self.gpu_ready
        return True  # CPU prover health already checks its local process.
