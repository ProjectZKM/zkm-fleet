"""The bridge's data model: typed records, and one codec that reads and writes them.

Everything the bridge keeps or passes around is a dataclass here (a run's spec and
status, a service replica, the cluster settings, a health report), with enums for
the closed sets. Nothing else in the package spells a field name as a string:
records go to and from JSON / YAML / argparse / environment variables through the
reflection in `from_dict`, `to_dict`, `merge`, `to_env` and `argparser`, which
read the dataclass fields and their type hints.

Loading is lenient about extra keys (an older or newer record still loads) and
strict about types; `merge` refuses a field the record does not have.
"""

from __future__ import annotations

import argparse
import dataclasses
import enum
import json
import os
import types
import typing
from dataclasses import dataclass, field, fields, is_dataclass
from typing import Any, Callable, ClassVar, Iterator, Mapping, TypeVar

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
            return from_dict(tp, value, where=where)
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
        return {f.name: to_dict(getattr(obj, f.name)) for f in fields(obj)}
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


def argparser(cls: type, parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """Adds one argument per field of `cls`: the field's type decides the parser's
    (bool = a flag, list = repeatable), its default is the argument's, and its
    metadata gives `help`, `flags` (e.g. ("-f", "--follow")) or `positional`."""
    hints = _hints(cls)
    for f in fields(cls):
        md = f.metadata
        tp = hints[f.name]
        default: Any = f.default if f.default is not dataclasses.MISSING else (
            f.default_factory() if f.default_factory is not dataclasses.MISSING else None)
        kwargs: dict[str, Any] = {"help": md.get("help")}
        if md.get("positional"):
            flags = [f.name]
            if _is_union(tp):
                kwargs["nargs"] = "?"
                tp = next(a for a in typing.get_args(tp) if a is not _NONE)
            if "choices" in md:
                kwargs["choices"] = md["choices"]
        else:
            flags = list(md.get("flags", ("--" + f.name.replace("_", "-"),)))
            kwargs["dest"] = f.name
        if _is_union(tp):
            tp = next(a for a in typing.get_args(tp) if a is not _NONE)
        if tp is bool:
            kwargs["action"] = "store_true"
        elif typing.get_origin(tp) is list:
            (item,) = typing.get_args(tp)
            kwargs.update(action="append", type=item, default=None)
        else:
            kwargs["type"] = tp
        if "default" not in kwargs:
            kwargs["default"] = default
        if "choices" in md and "choices" not in kwargs:
            kwargs["choices"] = md["choices"]
        parser.add_argument(*flags, **kwargs)
    return parser


def from_args(cls: type[T], ns: argparse.Namespace) -> T:
    """The dataclass a parser built by `argparser` filled in (None for an unused
    repeatable argument becomes the field's empty default)."""
    data = {k: v for k, v in vars(ns).items() if v is not None}
    return from_dict(cls, data)


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


# ── one-shot runs ────────────────────────────────────────────────────────────

class RunState(str, enum.Enum):
    WAITING_DEPENDENCY = "waiting_dependency"
    QUEUED = "queued"
    WAITING_VRAM = "waiting_vram"
    RUNNING = "running"
    RETRYING = "retrying"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    LOST = "lost"

    @property
    def terminal(self) -> bool:
        return self in (RunState.SUCCEEDED, RunState.FAILED, RunState.CANCELLED)


class Cause(str, enum.Enum):
    """Why an attempt ended short of exit 0."""

    GPU_OOM = "gpu-oom"
    HOST_OOM = "host-oom"
    EXIT_CODE = "exit-code"
    DOCKER_RUN = "docker-run"
    RANK_LOST = "rank-lost"

    @property
    def oom(self) -> bool:
        return self in (Cause.GPU_OOM, Cause.HOST_OOM)


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
class Profile(Record):
    """A runs.yaml profile: what a request may ask for, and the container it gets."""

    image: str
    user: str | None = None
    max_gpus: int = 8
    default_gpu_mem_gb: int = 0
    shm_gb: int = 16
    allow: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)


@dataclass(kw_only=True)
class RunsConfig(Record):
    """runs.yaml."""

    profiles: dict[str, Profile] = field(default_factory=dict)
    model_store: str | None = None
    hf_token_file: str | None = None
    stage_profile: str | None = None


@dataclass(kw_only=True)
class CodeRecord(Record):
    """What `--code` captured into the run."""

    source: str
    repo: str | None = None
    ref: str | None = None
    commit: str | None = None
    sha256: str | None = None
    bytes: int | None = None
    note: str | None = None


@dataclass(kw_only=True)
class RunSpec(Record):
    """Everything a run was asked to be; written once at submit, read by its actor."""

    id: str
    profile_name: str
    profile: Profile
    command: list[str]
    gpus: float
    cpus: int
    out: str
    mounts: list[Mount] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    log: str | None = None
    workdir: str | None = None
    code_dir: str | None = None
    code: CodeRecord | None = None
    gpu_mem_gb: int = 0
    vram_wait_s: int = 1800
    retry: int = 0
    restart_on_oom: int = 0
    after: str | None = None
    nodes: int = 1
    machines: list[str] = field(default_factory=list)
    machine_resources: list[str] = field(default_factory=list)

    def expand(self, text: str) -> str:
        """`{out}` and `{code}` in a command, env value or workdir."""
        return text.replace("{out}", self.out).replace("{code}", self.code_dir or "{code}")

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
class Holder(Record):
    pid: int
    mib: int
    cmd: str

    def __str__(self) -> str:
        return f"pid {self.pid} {self.mib} MiB ({self.cmd})"


@dataclass(kw_only=True)
class CardState(Record):
    """A host card as nvidia-smi sees it."""

    index: str
    free_mib: int
    total_mib: int
    holders: list[Holder] = field(default_factory=list)

    def held_by(self) -> str:
        return ", ".join(map(str, self.holders)) or "no process"


@dataclass(kw_only=True)
class RankInfo(Record):
    """Where one rank of a run lives."""

    rank: int
    machine: str
    node_ip: str
    gpus: list[str]
    container: str


@dataclass(kw_only=True)
class Attempt(Record):
    """How one start of the job ended."""

    exit_code: int | None
    cause: Cause | None = None
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
class RunStatus(Record):
    """status.json: written by submit, the waiter, the actor and cancel; read by all."""

    id: str
    state: RunState
    submitted_at: float
    gpus_requested: float
    cpus: int
    gpu_mem_gb: int | None = None  # None: the whole card
    after: str | None = None
    code: CodeRecord | None = None
    nodes: int = 1
    machine: str | None = None
    node_ip: str | None = None
    ranks: list[RankInfo] = field(default_factory=list)
    gpus: list[str] = field(default_factory=list)
    container: str | None = None
    attempt: int | None = None
    attempts: list[Attempt] = field(default_factory=list)
    started_at: float | None = None
    finished_at: float | None = None
    exit_code: int | None = None
    cause: Cause | None = None
    error: str | None = None
    detail: str | None = None
    # filled in by `status` for the reader, never stored
    metrics: dict[str, Any] | None = None
    out: str | None = None

    def places(self) -> list[RankInfo]:
        """Every rank's place (rank 0's from the top-level fields for a single-node run)."""
        if self.ranks:
            return self.ranks
        return [RankInfo(rank=0, machine=self.machine or "", node_ip=self.node_ip or "", gpus=self.gpus,
                         container=self.container or "")]


@dataclass(kw_only=True)
class GangEnv(Record):
    """Where the ranks of a --nodes run meet. Each value goes out under the fleet's
    own name and the names torchrun reads, so `torchrun train.py` joins as is."""

    nnodes: int = field(metadata={"env": ("FLEET_NNODES", "NNODES", "PET_NNODES")})
    master_addr: str = field(metadata={"env": ("FLEET_MASTER_ADDR", "MASTER_ADDR", "PET_MASTER_ADDR")})
    master_port: int = field(metadata={"env": ("FLEET_MASTER_PORT", "MASTER_PORT", "PET_MASTER_PORT")})
    nproc_per_node: int = field(metadata={"env": "PET_NPROC_PER_NODE"})
    node_rank: int | None = field(default=None, metadata={"env": ("FLEET_NODE_RANK", "NODE_RANK", "PET_NODE_RANK")})


@dataclass(kw_only=True)
class RunArgs(Record):
    """`run.sh` command line (one field per argument; see `argparser`)."""

    action: str = field(metadata={"positional": True, "choices": ["submit", "status", "cancel", "list", "logs", "fetch",
                                                                   "purge", "render", "stage-model"]})
    run_id: str | None = field(default=None, metadata={"positional": True,
                                                       "help": "a run id (stage-model: the Hugging Face repo)"})
    profiles: str = field(default_factory=lambda: os.environ.get("FLEET_RUN_PROFILES", "/etc/zkm-fleet/runs.yaml"))
    runs_dir: str = field(default_factory=lambda: os.environ.get("FLEET_RUNS_DIR", "/var/lib/zkm-fleet/runs"))
    address: str = "127.0.0.1:6379"
    profile: str | None = None
    name: str | None = None
    gpus: float = field(default=1, metadata={"help": "cards; a fraction (0.5) shares one card and needs a --gpu-mem"})
    cpus: int = 8
    gpu_mem: int = field(default=0, metadata={"help": "GiB free each granted card must have before the job starts "
                                                      "(default: the profile's, else the whole card)"})
    vram_wait: int = field(default=1800, metadata={"help": "seconds to wait for that memory before failing "
                                                           "(the run shows waiting_vram meanwhile)"})
    machine: list[str] = field(default_factory=list, metadata={
        "help": "the machine (host name or IP) the run must land on; with --nodes N, repeat for each"})
    nodes: int = field(default=1, metadata={"help": "spread the run over N machines: --gpus and --cpus on each, one "
                                                    "container per machine, ranks wired through FLEET_MASTER_ADDR / "
                                                    "FLEET_NODE_RANK (and torchrun's PET_*)"})
    after: str | None = field(default=None, metadata={"help": "run only after this run succeeded"})
    retry: int = field(default=0, metadata={"help": "try a failed run again up to N times"})
    restart_on_oom: int = field(default=0, metadata={"help": "try again up to N times when an attempt ran out of memory"})
    code: str | None = field(default=None, metadata={"help": "git:/repo@ref or FILE.tar.gz, captured into the run "
                                                             "(FLEET_CODE, {code})"})
    mount: list[str] = field(default_factory=list, metadata={"help": "PATH[:ro|:rw], mounted at its own path; repeat"})
    env: list[str] = field(default_factory=list, metadata={"help": "K=V for the job ({out} / {code} expand); repeat"})
    log: str | None = field(default=None, metadata={"help": "file the job's output is appended to (default RUNS_DIR/<id>/log)"})
    workdir: str | None = None
    follow: bool = field(default=False, metadata={"flags": ("-f", "--follow"), "help": "logs: keep printing until the run ends"})
    lines: int = field(default=50, metadata={"flags": ("-n", "--lines"), "help": "logs: lines of history first"})
    rank: int = field(default=0, metadata={"help": "logs: a --nodes run's rank k (its output is log.rank<k>)"})
    older_than: float | None = field(default=None, metadata={"help": "purge: finished runs older than this many days"})
    revision: str = field(default="main", metadata={"help": "stage-model: branch, tag or commit"})
    include: list[str] = field(default_factory=list, metadata={"help": "stage-model: only files matching this glob; repeat"})
    host_gpus: str = field(default="0", metadata={"help": "render: host cards to show in the command"})

    @property
    def env_map(self) -> dict[str, str]:
        return dict(e.split("=", 1) for e in self.env if "=" in e)


# ── services ─────────────────────────────────────────────────────────────────

class ServiceKind(str, enum.Enum):
    ZIREN_GPU = "ziren-gpu"
    ZIREN_CPU = "ziren-cpu"
    APP = "app"
    HOLD = "hold"
    STREAM = "stream"

    @property
    def takes_gpus(self) -> bool:
        return self not in (ServiceKind.ZIREN_CPU, ServiceKind.STREAM)


@dataclass(kw_only=True)
class StreamProvider(Record):
    """The commands a stream worker runs for its stream. `{seq}`, `{stream}`, `{work}`,
    `{prev_seq}`, `{prev_artifact}` and `{prev_job_id}` expand in them and in env.
    `head` prints the head sequence number; `fetch` (optional) readies seq's input;
    `prove` proves seq (through the SDK, against the scheduler); `cleanup` (optional)
    runs after a prove ended either way."""

    head: list[str]
    prove: list[str]
    fetch: list[str] = field(default_factory=list)
    cleanup: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    env_file: str | None = None  # KEY=VALUE lines read on the worker's machine (secrets stay there)
    work: str = "/tmp/zkm-fleet/stream"
    cwd: str | None = None
    artifact: str | None = None  # path a prove leaves, recorded with the step when it exists
    keep_logs: str = "failed"  # failed | all | none
    job_id_pattern: str = r"(?:job|proof)[ _=:]+([0-9a-f]{8}-[0-9a-f-]{27})"


@dataclass(kw_only=True)
class ClusterConfig(Record):
    """The top of services.yaml: what every replica shares."""

    scheduler: str | None = None
    cluster_token_file: str | None = None
    node_binary: str | None = None
    log_dir: str = "/tmp/zkm-fleet"
    node_port_range: tuple[int, int] = (31000, 31099)
    gpu_port_range: tuple[int, int] = (30100, 30199)
    scheduler_admin: str = "http://127.0.0.1:9191"
    admin_token_file: str | None = None
    drain_timeout_s: float = 900

    @property
    def admin_token_path(self) -> str:
        return self.admin_token_file or os.path.join(os.path.dirname(self.cluster_token_file or "/etc/zkm-fleet/x"),
                                                     "admin.token")


@dataclass(kw_only=True)
class ServiceSpec(Record):
    """One entry of `services:`."""

    name: str
    kind: ServiceKind = ServiceKind.ZIREN_GPU
    replicas: int = 1
    gpus: float = 1
    cpus: float = 1
    memory_gb: float | None = None
    machine: str | None = None
    image: str | None = None
    env: dict[str, str] = field(default_factory=dict)
    volumes: list[str] = field(default_factory=list)
    shm_gb: int | None = None
    ready_timeout_s: float = 600
    # apps
    port_base: int | None = None
    command: list[str] = field(default_factory=list)
    user: str | None = None
    workdir: str | None = None
    health_path: str = "/"
    stop_timeout_s: int = 30
    # CPU provers
    backend: str = "cpu"
    rust_log: str = "info"
    # stream workers
    stream: str | None = None
    slots: int = 1  # tasks this worker runs at once (the scheduler bounds the stream's parallelism)
    provider: StreamProvider | None = None


@dataclass(kw_only=True)
class ServicesFile(Record):
    """services.yaml as a whole."""

    cluster: ClusterConfig
    services: list[ServiceSpec]
    raw: dict[str, Any] = field(default_factory=dict)  # the file's own mapping, for the spec hash

    @classmethod
    def parse(cls, data: Mapping[str, Any]) -> ServicesFile:
        cluster = from_dict(ClusterConfig, {k: v for k, v in data.items() if k != "services"})
        services = [from_dict(ServiceSpec, s, where=f"service {s.get('name', '?')}") for s in data.get("services", [])]
        return cls(cluster=cluster, services=services, raw=dict(data))


@dataclass(kw_only=True)
class ReplicaSpec(Record):
    """One actor: a service's replica `index`, named `<service>-<index>`, with the
    cluster settings it needs. `config` is the file's own mapping for this replica,
    which the spec hash is taken from: a change in the bridge's code never counts as
    a change of the service, only an edit of the file does."""

    name: str
    index: int
    service: ServiceSpec
    cluster: ClusterConfig
    config: dict[str, Any] = field(default_factory=dict)

    @property
    def kind(self) -> ServiceKind:
        return self.service.kind

    @property
    def machine(self) -> str | None:
        return self.service.machine

    def node_log_path(self) -> str:
        return os.path.join(self.cluster.log_dir, f"{self.name}.node.log")


@dataclass(kw_only=True)
class Bundle(Record):
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
class NodeEnv(Record):
    """What fleet-ziren-node reads from its environment."""

    name: str = field(metadata={"env": "FLEET_NODE_NAME"})
    backend: str = field(metadata={"env": "FLEET_BACKEND"})
    port_range: str = field(metadata={"env": "FLEET_PORT_RANGE"})
    advertise_host: str = field(metadata={"env": "FLEET_ADVERTISE_HOST"})
    scheduler: str = field(metadata={"env": "FLEET_SCHEDULER"})
    cluster_token_file: str = field(metadata={"env": "FLEET_CLUSTER_TOKEN_FILE"})
    machine: str = field(metadata={"env": "FLEET_MACHINE"})
    rust_log: str = field(metadata={"env": "RUST_LOG"})
    exit_with_parent: bool = field(default=True, metadata={"env": "FLEET_EXIT_WITH_PARENT"})
    cpus: int = field(default=0, metadata={"env": "FLEET_CPUS"})
    gpus: int | None = field(default=None, metadata={"env": "FLEET_GPUS"})
    gpu_endpoint: str | None = field(default=None, metadata={"env": "FLEET_GPU_ENDPOINT"})
    image: str | None = field(default=None, metadata={"env": "FLEET_IMAGE"})
    rayon_threads: int | None = field(default=None, metadata={"env": "RAYON_NUM_THREADS"})


@dataclass(kw_only=True)
class Health(Record):
    """What an actor answers to `health()`."""

    name: str
    spec: str | None = None  # the replica's spec hash
    machine: str | None = None
    gpus: list[str] = field(default_factory=list)
    gpu_port: int | None = None
    gpu_ready: bool | None = None
    port: int | None = None
    ready: bool | None = None
    stopping: bool = False
    error: str | None = None
    # stream workers
    stream: str | None = None
    slots: int | None = None
    tasks_done: int | None = None
    tasks_failed: int | None = None
    last_lease_age_s: float | None = None
    last_error: str | None = None


class StreamTask(str, enum.Enum):
    HEAD = "head"
    FETCH = "fetch"
    PROVE = "prove"
    NONE = "none"


@dataclass(kw_only=True)
class Lease(Record):
    """What the scheduler hands a stream worker."""

    task: StreamTask
    seq: int | None = None
    prev_seq: int | None = None
    prev_artifact: str | None = None
    prev_job_id: str | None = None
    timeout_s: float | None = None
    retry_after_s: float | None = None


@dataclass(kw_only=True)
class StreamReport(Record):
    """How a leased task went."""

    worker: str
    task: StreamTask
    ok: bool
    seq: int | None = None
    error: str = ""
    wall_ms: int | None = None
    artifact: str | None = None
    job_id: str | None = None
    head: int | None = None


@dataclass(kw_only=True)
class ApplyReport(Record):
    """What one apply did, by replica name."""

    started: list[str] = field(default_factory=list)
    stopped: list[str] = field(default_factory=list)
    replaced: list[str] = field(default_factory=list)
    healthy: list[str] = field(default_factory=list)
    unhealthy: list[str] = field(default_factory=list)
    held: list[str] = field(default_factory=list)
    planned: list[str] = field(default_factory=list)  # --dry-run: what would change


class Fields:
    """Field names of a record as attributes, for callers that must name one (a
    JSON key to show, a column to select) without spelling it as a string."""

    def __init__(self, cls: type) -> None:
        self._names = [f.name for f in fields(cls)]

    def __getattr__(self, name: str) -> str:
        if name in self._names:
            return name
        raise AttributeError(name)

    def __iter__(self) -> Iterator[str]:
        return iter(self._names)
