"""Local batch execution, container cleanup and replay-safe Ray actors.

The Rust control plane admits jobs, resolves dependencies and owns resource policy.
This module executes commands already authorized by fleet-node. Inputs and outputs
belong to the workload owner's storage; no submission or file-upload CLI lives here.
"""

from __future__ import annotations

import fcntl
import io
import os
import re
import socket
import subprocess
import threading
import time
from dataclasses import replace
from typing import Any


import ray
import yaml
from ray.util.placement_group import placement_group, remove_placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

from .rayapi import alive_nodes, node_ip
from .model import (Attempt, Bundle, CardState, Cause, Holder,
                    RankInfo, RunsConfig, RunSpec, RunState, RunStatus)

NAMESPACE = "zkm-fleet"
GPU_OOM = re.compile(r"CUDA out of memory|OutOfMemoryError|CUBLAS_STATUS_ALLOC_FAILED|cudaErrorMemoryAllocation"
                     r"|CUDA error: out of memory", re.I)
SPEC_FILE, STATUS_FILE, OUT_DIR, METRICS_FILE = "spec.json", "status.json", "out", "metrics.jsonl"


def _sh(argv: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(argv, capture_output=True, text=True)


# ── the cards as they really are ─────────────────────────────────────────────

def card_memory(cards: list[str]) -> dict[str, CardState]:
    """Physical state of host cards: free / total MiB and the processes holding memory
    (pid, MiB, command). Ray's grant is logical; this is what is really on the card."""
    if not cards:
        return {}
    q = _sh(["nvidia-smi", "--query-gpu=index,uuid,memory.free,memory.total",
             "--format=csv,noheader,nounits", "-i", ",".join(cards)])
    out: dict[str, CardState] = {}
    by_uuid: dict[str, str] = {}
    for line in q.stdout.strip().splitlines():
        idx, uuid_, free, total = [x.strip() for x in line.split(",")]
        out[idx] = CardState(index=idx, free_mib=int(free), total_mib=int(total))
        by_uuid[uuid_] = idx
    apps = _sh(["nvidia-smi", "--query-compute-apps=gpu_uuid,pid,used_memory", "--format=csv,noheader,nounits"])
    for line in apps.stdout.strip().splitlines():
        uuid_, pid, used = [x.strip() for x in line.split(",")]
        if uuid_ in by_uuid:
            try:
                with open(f"/proc/{pid}/cmdline", "rb") as f:
                    cmd = f.read().replace(b"\0", b" ").decode(errors="replace").strip()[:80]
            except OSError:
                cmd = "?"
            out[by_uuid[uuid_]].holders.append(Holder(pid=int(pid), mib=int(used), cmd=cmd))
    return out


def _holders(cards: list[str]) -> str:
    return "; ".join(f"card {i}: {c.held_by()}" for i, c in card_memory(cards).items())


def vram_shortfall(cards: list[str], need_mib: int | None) -> list[str]:
    """Why the granted cards cannot take the job yet ([] when they can). Without a
    stated need the job wants the whole card (95% free)."""
    short = []
    for idx, c in card_memory(cards).items():
        need = need_mib if need_mib else int(c.total_mib * 0.95)
        if c.free_mib < need:
            short.append(f"card {idx}: {c.free_mib} MiB free, needs {need} MiB; held by {c.held_by()}")
    return short


# ── records on disk ──────────────────────────────────────────────────────────

class NotFound(SystemExit):
    """No local execution record exists."""


class Conflict(SystemExit):
    """Refused because of the state it is in (the API answers 409)."""


# A run id: what a submit makes of the name, never `.`/`..` or a dot entry of the runs dir.
RUN_ID = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]*")


class RunDir:
    """A run's record directory and the files in it."""

    def __init__(self, runs_dir: str, run_id: str, *, must_exist: bool = True) -> None:
        if not RUN_ID.fullmatch(run_id or ""):
            raise NotFound(f"no run {run_id!r} (not a run id)")
        self.id = run_id
        self.path = os.path.join(runs_dir, run_id)
        if must_exist and not os.path.isdir(self.path):
            raise NotFound(f"no run {run_id}")

    @property
    def spec_path(self) -> str:
        return os.path.join(self.path, SPEC_FILE)

    @property
    def status_path(self) -> str:
        return os.path.join(self.path, STATUS_FILE)

    @property
    def out(self) -> str:
        return os.path.join(self.path, OUT_DIR)


    def spec(self) -> RunSpec:
        return RunSpec.read(self.spec_path)

    def status(self) -> RunStatus:
        return RunStatus.read(self.status_path)

    def update(self, *, force: bool = False, expected_state: RunState | None = None, **changes: Any) -> RunStatus:
        """Merges typed changes into status.json, under the run's lock (the actor, a
        cancel and a preemption write it). Once the run has ended, a late report from the
        actor that it is going (or ended otherwise) is dropped: a cancel wins. While it
        is being preempted, so is everything but a cancel and the preemption's own
        writes (force): the actor being stopped reports nothing more."""
        with open(os.path.join(self.path, ".lock"), "a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            st = self.status()
            if expected_state is not None and st.state is not expected_state:
                raise Conflict(f"{self.id}: state changed during preemption")
            new_state = changes.get("state")
            if st.state.terminal and new_state not in (None, st.state):
                return st
            if st.state is RunState.PREEMPTED and not force and new_state is not RunState.CANCELLED:
                return st
            if new_state is not None and new_state.terminal:
                changes.setdefault("preempt_requeue", False)
            if new_state is not None and new_state != st.state and not new_state.terminal:
                changes.setdefault("quota_released", False)
            st = st.merged(**changes)
            st.write(self.status_path)
            return st


    def state(self) -> RunState | None:
        try:
            return self.status().state
        except (OSError, ValueError):
            return None


# Execution actor names, including waiters retained from earlier records. The services'
# reconciler leaves every actor with these prefixes alone, and refuses services named so.
RUN_ACTOR_PREFIX, WAIT_ACTOR_PREFIX = "run-", "wait-"


def is_run_actor(name: str) -> bool:
    return name.startswith((RUN_ACTOR_PREFIX, WAIT_ACTOR_PREFIX))


def container_name(run_id: str, rank: int = 0) -> str:
    return f"fleet-run-{run_id}" + (f"-r{rank}" if rank else "")


def actor_name(run_id: str, rank: int = 0) -> str:
    return f"{RUN_ACTOR_PREFIX}{run_id}" + (f"-r{rank}" if rank else "")


def waiter_name(run_id: str) -> str:
    return f"{WAIT_ACTOR_PREFIX}{run_id}"


def pg_name(run_id: str) -> str:
    return f"pg-run-{run_id}"


def docker_command(spec: RunSpec, gpus: list[str], attempt: int = 1, rank: int = 0) -> list[str]:
    """The `docker run` for a run (or one rank of it) on the given host cards."""
    p = spec.profile
    cmd = ["docker", "run", "-d", "--name", container_name(spec.id, rank), "--network", "none" if spec.namespace or spec.owner else "host",
           f"--shm-size={p.shm_gb}g", "--label", "zkm-fleet=run", "--label", f"zkm-fleet.run={spec.id}"]
    if gpus:
        cmd += ["--runtime=nvidia", "--gpus", f'"device={",".join(gpus)}"', "-e", "NVIDIA_DRIVER_CAPABILITIES=all"]
    if spec.cpus:
        cmd += [f"--cpus={spec.cpus}", "-e", f"OMP_NUM_THREADS={spec.cpus}"]
    if p.user:
        cmd += ["--user", p.user]
    if spec.workdir:
        cmd += ["-w", spec.expand(spec.workdir)]
    for m in spec.mounts:
        cmd += ["-v", m.docker()]
    cmd += ["-v", f"{spec.out}:{spec.out}"]
    env: dict[str, str] = {"PYTHONUNBUFFERED": "1", **p.env, **spec.env,
                           "FLEET_RUN_ID": spec.id, "FLEET_OUT": spec.out,
                           "FLEET_METRICS": os.path.join(spec.out, METRICS_FILE), "FLEET_ATTEMPT": str(attempt)}
    for k, v in env.items():
        cmd += ["-e", f"{k}={spec.expand(v)}"]
    cmd.append(p.image)
    cmd += [spec.expand(a) for a in spec.command]
    return cmd


def missing_mounts(spec: RunSpec) -> list[str]:
    """Mounts that do not exist where the run landed; the run fails instead of docker
    creating them, owned by root, in someone's tree."""
    return [m.path for m in spec.mounts if not os.path.exists(m.path)]


# ── the actors ───────────────────────────────────────────────────────────────

class _Rank:
    """The cards Ray granted to one local batch container."""

    def __init__(self, spec: RunSpec, rank: int = 0) -> None:
        self.spec, self.rank = spec, rank
        self.gpus: list[str] = [str(int(g)) for g in ray.get_gpu_ids()]
        self.name = container_name(spec.id, rank)
        self.log_path = spec.rank_log_path(rank)
        self.follower: subprocess.Popen[bytes] | None = None
        self.log: io.BufferedWriter | None = None
        self.offset = 0

    def info(self) -> RankInfo:
        return RankInfo(rank=self.rank, machine=socket.gethostname(), node_ip=node_ip(),
                        gpus=self.gpus, container=self.name)

    def check(self, need: int | None) -> str | None:
        """Why this machine cannot run the job at all (None when it can)."""
        missing = missing_mounts(self.spec)
        if missing:
            return f"mounts missing on {socket.gethostname()}: {missing}"
        too_big = [f"card {i}: {c.total_mib} MiB in all, the job asks {need} MiB"
                   for i, c in card_memory(self.gpus).items() if need and need > c.total_mib]
        return "--gpu-mem exceeds the card: " + "; ".join(too_big) if too_big else None

    def shortfall(self, need: int | None) -> list[str]:
        return vram_shortfall(self.gpus, need) if self.gpus else []

    def start(self, attempt: int) -> str | None:
        """Starts the container; returns docker's error, or None."""
        with open(os.path.join(os.path.dirname(self.spec.out), ".lock"), "a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            st = RunStatus.read(os.path.join(os.path.dirname(self.spec.out), STATUS_FILE))
            if st.state.terminal or st.state is RunState.PREEMPTED:
                return "run stopped before container creation"
            _sh(["docker", "rm", "-f", self.name])
            out = _sh(docker_command(self.spec, self.gpus, attempt, self.rank))
            if out.returncode != 0:
                return out.stderr.strip()[:500]
        self.offset = os.path.getsize(self.log_path) if os.path.exists(self.log_path) else 0
        self.log = open(self.log_path, "ab")
        self.follower = subprocess.Popen(["docker", "logs", "-f", self.name], stdout=self.log, stderr=subprocess.STDOUT)
        return None

    def wait(self) -> Attempt:
        """Waits for the container to end and says how it ended."""
        waited = _sh(["docker", "wait", self.name])
        time.sleep(1)  # let the follower drain the last lines
        if self.follower and self.log:
            self.follower.terminate()
            self.log.close()
        text = waited.stdout.strip()
        code = int(text) if text.lstrip("-").isdigit() else None
        result = Attempt(exit_code=code)
        if code != 0:
            insp = _sh(["docker", "inspect", "-f", "{{.State.OOMKilled}}", self.name])
            with open(self.log_path, "rb") as f:
                f.seek(self.offset)
                this_attempt = f.read()[-65536:].decode(errors="replace")
            m = GPU_OOM.search(this_attempt)
            if m:
                result = replace(result, cause=Cause.GPU_OOM, evidence=m.group(0), card_holders=_holders(self.gpus))
            elif insp.stdout.strip() == "true":
                result = replace(result, cause=Cause.HOST_OOM, evidence="docker: OOMKilled")
            else:
                result = replace(result, cause=Cause.EXIT_CODE)
        _sh(["docker", "rm", "-f", self.name])
        return result

    def kill(self) -> None:
        _sh(["docker", "rm", "-f", self.name])


def stop_command(run_id: str, rank: int, grace_s: int) -> list[str]:
    """SIGTERM to the job, SIGKILL grace_s later (docker stop): a cancel or a preemption
    gives it the time to checkpoint into out/ (FLEET_ATTEMPT tells the next attempt)."""
    return ["docker", "stop", "-t", str(grace_s), container_name(run_id, rank)]


class _GpuRun:
    """Execute one finite job on this machine, including its retry budgets."""

    def __init__(self, spec: RunSpec, run_dir: str) -> None:
        self.spec = spec
        self.dir = RunDir(os.path.dirname(run_dir), os.path.basename(run_dir))
        self.local = _Rank(spec)
        self.gpus = self.local.gpus
        info = self.local.info()
        self.dir.update(machine=info.machine, node_ip=info.node_ip, launch_pending=False)
        threading.Thread(target=self._run, daemon=True).start()

    def ping(self) -> RankInfo:
        return self.local.info()

    def _wait_vram(self, need: int | None) -> bool:
        deadline = time.time() + self.spec.vram_wait_s
        while True:
            if self.dir.state() in (RunState.CANCELLED, RunState.PREEMPTED):
                return False
            short = self.local.shortfall(need)
            if not short:
                return True
            if time.time() > deadline:
                self.dir.update(state=RunState.FAILED, gpus=self.gpus, finished_at=time.time(),
                                error="cards not free in time: " + "; ".join(short))
                return False
            self.dir.update(state=RunState.WAITING_VRAM, gpus=self.gpus, detail="; ".join(short))
            time.sleep(15)

    def _attempt(self, attempt: int) -> Attempt:
        error = self.local.start(attempt)
        if error:
            self.local.kill()
            return Attempt(exit_code=None, cause=Cause.DOCKER_RUN, error=error)
        self.dir.update(state=RunState.RUNNING, gpus=self.gpus, container=self.local.name, attempt=attempt,
                        started_at=time.time(), detail=None)
        try:
            return self.local.wait()
        except Exception as error:
            self.local.kill()
            return Attempt(exit_code=None, cause=Cause.RANK_LOST, error=repr(error)[:300])

    def _run(self) -> None:
        code: int | None = None
        try:
            if self.dir.status().state.terminal or self.dir.state() is RunState.PREEMPTED:
                return
            need = self.spec.need_mib
            problem = self.local.check(need)
            if problem:
                self.dir.update(state=RunState.FAILED, gpus=self.gpus, finished_at=time.time(), error=problem)
                return
            # a run queued again after a preemption goes on from its record: FLEET_ATTEMPT
            # counts on, and the budgets are what its earlier attempts left
            attempts: list[Attempt] = list(self.dir.status().attempts)
            attempt = len(attempts)
            retries, oom_restarts = budgets(self.spec, attempts)
            while True:
                attempt += 1
                if not self._wait_vram(need):
                    return
                result = self._attempt(attempt)
                attempts.append(result)
                code = result.exit_code
                if result.ok:
                    self.dir.update(state=RunState.SUCCEEDED, exit_code=0, attempts=attempts, finished_at=time.time())
                    return
                if self.dir.state() in (RunState.CANCELLED, RunState.PREEMPTED):
                    return  # the canceller or the preemption records how it ended
                if result.cause is not None and result.cause.oom and oom_restarts > 0:
                    oom_restarts -= 1
                elif retries > 0:
                    retries -= 1
                else:
                    self.dir.update(state=RunState.FAILED, exit_code=code, cause=result.cause, error=result.reason,
                                    attempts=attempts, finished_at=time.time())
                    return
                self.dir.update(state=RunState.RETRYING, attempts=attempts,
                                detail=f"attempt {attempt} ended {result.cause.value if result.cause else '?'} "
                                       f"(exit {code}); trying again")
        except Exception as e:  # report, never hang the cards
            self.dir.update(state=RunState.FAILED, error=repr(e), finished_at=time.time())
        finally:
            try:
                self.local.kill()
                remove_placement_group(ray.util.get_placement_group(pg_name(self.spec.id)))
            finally:
                os._exit(0 if code == 0 else 1)


GpuRun = ray.remote(_GpuRun)


def budgets(spec: RunSpec, attempts: list[Attempt]) -> tuple[int, int]:
    """--retry and --restart-on-oom left after these attempts: an out-of-memory failure
    spends a restart first, any other failure a retry; a preempted attempt spends none."""
    retries, oom_restarts = spec.retry, spec.restart_on_oom
    for a in attempts:
        if a.ok or a.cause is Cause.PREEMPTED:
            continue
        if a.cause is not None and a.cause.oom and oom_restarts > 0:
            oom_restarts -= 1
        else:
            retries -= 1
    return retries, oom_restarts


@ray.remote(num_cpus=0, max_retries=0)
def _on_node(argv: list[str]) -> str:
    return _sh(argv).stdout


def _on_run_machine(place: RankInfo, argv: list[str], timeout_s: float = 60) -> str | None:
    """Runs argv on the machine a rank landed on (its container is there) and returns
    its stdout; None when that machine has left the cluster."""
    if not place.node_ip:  # never landed (or a record from before node_ip): only this machine can have it
        return _sh(argv).stdout
    if not any(n.get("NodeManagerAddress") == place.node_ip for n in alive_nodes()):
        return None
    try:
        return str(ray.get(_on_node.options(resources={f"node:{place.node_ip}": 0.001}).remote(argv), timeout=timeout_s))
    except Exception:
        return None


def _stop_ranks(st: RunStatus, run_id: str, grace_s: int) -> None:
    """Every rank's container stopped with grace, all at once (a gang's ranks end together)."""
    threads = [threading.Thread(target=_on_run_machine, args=(r, stop_command(run_id, r.rank, grace_s), grace_s + 60))
               for r in st.places()]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def bundle(spec: RunSpec) -> Bundle:
    """CPU/GPU units already assigned to this execution machine."""
    return Bundle(cpu=float(max(spec.cpus, 1)), gpu=float(spec.gpus))


def run_placement(spec: RunSpec) -> Any:
    """Stable local placement identity lets a launch intent survive an agent crash."""
    if spec.nodes != 1:
        raise ValueError("batch execution requires exactly one machine")
    try:
        return ray.util.get_placement_group(pg_name(spec.id))
    except ValueError:
        return placement_group([bundle(spec).to_ray()], strategy="STRICT_PACK",
                               name=pg_name(spec.id), lifetime="detached")


def discard_placement(run_id: str) -> None:
    try:
        remove_placement_group(ray.util.get_placement_group(pg_name(run_id)))
    except ValueError:
        pass


def prepare_launch(spec: RunSpec) -> bool:
    """Claim actual capacity first; a blocked run must not occupy namespace units."""
    pg = run_placement(spec)
    ready, _ = ray.wait([pg.ready()], timeout=0.05)
    if ready:
        return True
    discard_placement(spec.id)
    return False


def launch(spec: RunSpec, run_dir: str) -> None:
    b = bundle(spec)
    pg = run_placement(spec)
    GpuRun.options(
        name=actor_name(spec.id), namespace=NAMESPACE, lifetime="detached", num_cpus=b.cpu, num_gpus=b.gpu,
        get_if_exists=True,
        max_restarts=0, max_task_retries=0,
        scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg, placement_group_bundle_index=0),
    ).remote(spec, run_dir)


# ── submit ───────────────────────────────────────────────────────────────────

def load_config(path: str) -> RunsConfig:
    with open(path) as f:
        return RunsConfig.load(yaml.safe_load(f) or {})


def _inside(path: str, roots: list[str]) -> bool:
    path = os.path.normpath(path)
    return any(path == r.rstrip("/") or path.startswith(r.rstrip("/") + "/") for r in roots)


def _machine_resource(machine: str) -> str:
    """Ray's per-node resource for a machine given by host name or IP."""
    for n in alive_nodes():
        if machine in (n.get("NodeManagerHostname"), n.get("NodeManagerAddress")):
            return f"node:{n['NodeManagerAddress']}"
    alive = [f"{n.get('NodeManagerHostname')} ({n.get('NodeManagerAddress')})" for n in alive_nodes()]
    raise SystemExit(f"--machine {machine}: no live Ray node of that name or address (have: {', '.join(alive)})")


# ── other actions ────────────────────────────────────────────────────────────

def _alive(name: str) -> bool:
    """Whether the actor exists. Without a connection to the cluster nobody can tell:
    then it counts as alive (a run is not called lost for that), and get_actor is not
    asked, which would start a Ray instance of its own on this machine."""
    if not ray.is_initialized():
        return True
    try:
        ray.get_actor(name, namespace=NAMESPACE)
        return True
    except ValueError:
        return False


def _going(run_id: str) -> bool:
    return _alive(actor_name(run_id)) or _alive(waiter_name(run_id))


@ray.remote(num_cpus=0, max_retries=0)
def _container_stopped(run_id: str, rank: int) -> bool:
    out = _sh(["docker", "inspect", "-f", "{{.State.Running}}", container_name(run_id, rank)])
    return (out.returncode == 0 and out.stdout.strip() == "false"
            or out.returncode != 0 and ("No such object" in out.stderr or "No such container" in out.stderr))


def execution_stopped(run: RunDir) -> bool:
    """Actor disappearance alone is not proof that detached Docker work stopped."""
    st = run.status()
    if _going(run.id) or any(_alive(actor_name(run.id, i)) for i in range(1, st.nodes)):
        return False
    live = {n["NodeManagerAddress"] for n in alive_nodes()} if any(p.node_ip for p in st.places()) else set()
    for place in st.places():
        if not place.node_ip:
            if place.machine or place.container or st.started_at is not None:
                return False
            continue  # actors publish location before starting Docker
        if place.node_ip not in live:
            return False
        try:
            task = _container_stopped.options(resources={f"node:{place.node_ip}": 0.001})
            if not ray.get(task.remote(run.id, place.rank), timeout=5):
                return False
        except Exception:
            return False
    try:
        discard_placement(run.id)
    except Exception:
        return False
    return True


def confirm_cleanup(run: RunDir) -> bool:
    st = run.status()
    if not _going(run.id) and not st.launch_pending and st.state in (RunState.CANCELLED,RunState.PREEMPTED,RunState.WAITING_QUOTA):
        for p in st.places():
            if p.node_ip:
                _on_run_machine(p,["docker","rm","-f",container_name(run.id,p.rank)],5)
    return execution_stopped(run)


def _state(run_id: str, st: RunStatus) -> RunState:
    """The state as it is: a run that has not ended but has no actor any more is lost
    (one being preempted has none for a moment, between its stop and its requeue)."""
    if (st.state.terminal or st.launch_pending or st.state in (RunState.PREEMPTED, RunState.WAITING_QUOTA)
            or (st.state is RunState.WAITING_DEPENDENCY and st.namespace is not None)):
        return st.state
    return RunState.LOST if not _going(run_id) else st.state


def cancel(runs_dir: str, run_id: str) -> RunStatus:
    run = RunDir(runs_dir, run_id)
    st = run.update(state=RunState.CANCELLED, finished_at=time.time())
    if st.state is not RunState.CANCELLED:
        raise Conflict(f"{run_id} already {st.state.value}: nothing to cancel")
    if st.node_ip or st.container or st.ranks:  # location is written before container creation
        _stop_ranks(st, run_id, run.spec().profile.stop_timeout_s)
    _end_actors(run_id, st.nodes)
    gone = [r.machine for r in st.places()
            if _on_run_machine(r, ["docker", "rm", "-f", container_name(run_id, r.rank)]) is None]
    if gone:
        st = run.update(container=f"not removed: {', '.join(gone)} left the cluster")
    try:
        remove_placement_group(ray.util.get_placement_group(pg_name(run_id)))
    except Exception:
        pass
    return st


def _end_actors(run_id: str, nodes: int) -> None:
    names = [actor_name(run_id), waiter_name(run_id)] + [actor_name(run_id, i) for i in range(1, nodes)]
    for name in names:
        try:
            ray.kill(ray.get_actor(name, namespace=NAMESPACE), no_restart=True)
        except ValueError:
            pass
    # the names are free once the kills land (a run queued again reuses them)
    deadline = time.time() + 30
    while any(_alive(n) for n in names) and time.time() < deadline:
        time.sleep(0.5)


# ── preemption ───────────────────────────────────────────────────────────────
#
# The control plane selects victims before starting a higher-priority workload. A run is
# stopped with grace (SIGTERM, SIGKILL its app's stop_timeout_s later), its attempt
# recorded as preempted, and it queues again for cards with the same id, out/ and
# FLEET_ATTEMPT counting on; a preemption spends no --retry. Only runs of a
# `preemptible` app that have run its min_runtime_s are eligible (a run still waiting
# for card memory has no running work to lose). This module performs the stop,
# cleanup; the Rust scheduler chooses the plan and authorizes the next launch.

PREEMPTIBLE = (RunState.RUNNING, RunState.WAITING_VRAM, RunState.RETRYING)


def preempt(runs_dir: str, run_id: str, reason: str) -> RunStatus:
    """Execute an admitted preemption; retain resources until cleanup is confirmed."""
    run = RunDir(runs_dir, run_id)
    spec, before = run.spec(), run.status()
    if before.state not in PREEMPTIBLE or not _alive(actor_name(run_id)):
        raise Conflict(f"{run_id} is {_state(run_id, before).value}: it holds no card to preempt")
    attempts = list(before.attempts)
    if before.state is RunState.RUNNING:
        attempts.append(Attempt(exit_code=None, cause=Cause.PREEMPTED, evidence=reason))
    st = run.update(expected_state=before.state, state=RunState.PREEMPTED, preempt_requeue=True,
                    attempts=attempts, preemptions=before.preemptions + 1, detail=f"preempted: {reason}")
    if st.state is not RunState.PREEMPTED:
        raise Conflict(f"{run_id} is {st.state.value}: nothing to preempt")
    if before.state is RunState.RUNNING:
        _stop_ranks(st, run_id, spec.profile.stop_timeout_s)
    _end_actors(run_id, st.nodes)
    if not confirm_cleanup(run):
        return run.update(force=True, detail=f"preempted ({reason}): waiting for confirmed container cleanup")
    return run.status()


def requeue_run(runs_dir: str, run_id: str) -> None:
    """Persist requeue intent; never restart while a previous container may be alive."""
    run = RunDir(runs_dir, run_id)
    if run.state() not in (RunState.QUEUED, RunState.PREEMPTED):
        return
    run.update(force=True, preempt_requeue=True)
    if not confirm_cleanup(run):
        return
    run.update(force=True, state=RunState.WAITING_QUOTA, preempt_requeue=False, quota_released=run.status().quota_released, started_at=None,
               container=None, detail="preempted: waiting for resources again")
