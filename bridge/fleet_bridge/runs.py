"""One-shot jobs ("runs"): a command in a container, on cards Ray grants, until it exits.

    run.sh submit --profile NAME [--name N] [--gpus N] [--cpus N] [--gpu-mem GiB]
                  [--vram-wait S] [--nodes N] [--machine HOST]... [--after ID] [--retry N]
                  [--restart-on-oom N] [--code git:REPO@REF | --code FILE.tar.gz]
                  [--mount PATH:ro|rw]... [--env K=V]... [--log FILE] [--workdir DIR]
                  -- CMD ARG...
    run.sh status|cancel|preempt|logs [-f] [-n N] [--rank K]|fetch|purge ID     run.sh list
    run.sh purge --older-than DAYS
    run.sh stage-model REPO [--revision R] [--include GLOB]...

A run's record is RUNS_DIR/<id>/: spec.json (a RunSpec), status.json (a RunStatus),
log (its output), out/ (mounted read-write into the job: FLEET_OUT, `{out}`;
metrics.jsonl there is what `status` reports as metrics) and, with --code, code/
(read-only: FLEET_CODE, `{code}`). `fetch` streams out/ as a tar.gz; `purge`
deletes the whole record.

Every other path a job touches is in its request (`--mount PATH:ro|rw`, own path,
existing, under the profile's `allow` roots). The profile (runs.yaml) fixes the
image, the user, a GPU cap, base env and default_gpu_mem_gb.

Placement: one STRICT_PACK bundle per run, so its cards and CPUs come from one
machine (--machine pins which). --nodes N spreads a run over N machines (STRICT_SPREAD:
one bundle of --gpus cards and --cpus CPUs on each of N different machines, --machine
repeated N times pins them): one container per machine, started together, all
ended when one fails; every container sees the GangEnv variables (FLEET_NNODES /
FLEET_NODE_RANK / FLEET_MASTER_ADDR / FLEET_MASTER_PORT, also under the names
torchrun reads, so `torchrun train.py` joins the gang as is); rank k's output goes
to log.rank<k>. `--gpus 0.5` asks for half a card: runs whose fractions fit share
it, each started only if its --gpu-mem is free then (the check is at start; size
--gpu-mem for the job's peak). Ray's grant is logical, so before starting the actor
checks the cards' real free memory against --gpu-mem (default: the profile's, else
the whole card) and waits as waiting_vram (naming who holds them) up to --vram-wait.
--after waits for another run in a CPU-free waiter that holds no card, then
launches only if it succeeded.

A failed attempt gets a Cause (gpu-oom from the log, host-oom from docker,
exit-code; with who held the card at that moment). --retry N repeats any failure,
--restart-on-oom N only out-of-memory ones; FLEET_ATTEMPT tells the job which
attempt it is, so it can resume from a checkpoint in its out/.

Cancel and preemption stop a job with grace: SIGTERM, then SIGKILL its app's
stop_timeout_s later, time to checkpoint. A service that needs cards a run holds
preempts it (see "preemption" below): the run queues again with the same id and
out/, FLEET_ATTEMPT counting on, and spends no --retry; `preempt ID` does the same
by hand.

Several machines: RUNS_DIR must be shared storage mounted at the same path on
all of them (the run's actor writes its record where it lands; the CLI reads it
wherever it runs), and so must every --mount. The container lives on the run's
machine: cancel and the lost-run check reach it through a Ray task there. With
an unshared RUNS_DIR on a multi-machine cluster, runs stay on the submitting
machine.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import io
import json
import os
import queue
import re
import shlex
import shutil
import socket
import subprocess
import sys
import tarfile
import threading
import time
import uuid
from dataclasses import dataclass, field, replace
from typing import Any, Callable

import logging

import ray
import yaml
from ray.util.placement_group import placement_group, remove_placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

from .rayapi import alive_nodes, current_placement_group, node_ip, nodes
from .model import (Attempt, Bundle, CardState, Cause, CodeRecord, GangEnv, Holder, LogChunk, Mount, MountMode,
                    Profile, RankInfo, Record, RunArgs, RunsConfig, RunSpec, RunState, RunStatus, argparser,
                    from_args, from_dict, to_env)

NAMESPACE = "zkm-fleet"
NETWORK_FS = ("nfs", "cifs", "smb2", "smb3", "ceph", "lustre", "gpfs", "beegfs", "glusterfs", "fuse")
GPU_OOM = re.compile(r"CUDA out of memory|OutOfMemoryError|CUBLAS_STATUS_ALLOC_FAILED|cudaErrorMemoryAllocation"
                     r"|CUDA error: out of memory", re.I)
SPEC_FILE, STATUS_FILE, OUT_DIR, CODE_DIR, LOG_FILE, METRICS_FILE = "spec.json", "status.json", "out", "code", "log", "metrics.jsonl"
UPLOADS_DIR = ".uploads"  # code tarballs sent through the runs API
QUOTA_ACTIVE_DIR = ".quota-active"


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
    """No such run or app (run.sh prints it like any refusal; the API answers 404)."""


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

    @property
    def code(self) -> str:
        return os.path.join(self.path, CODE_DIR)

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
            # Publish membership before state, so a crash cannot lose active work.
            if st.namespace is not None or (os.path.isfile(self.spec_path) and self.spec().owner):
                self.track_quota()
            st.write(self.status_path)
            if st.quota_released and (st.state.terminal or (st.state is RunState.PREEMPTED and not st.preempt_requeue)):
                self.untrack_quota()
            return st

    def track_quota(self) -> None:
        directory = os.path.join(os.path.dirname(self.path), QUOTA_ACTIVE_DIR)
        os.makedirs(directory, exist_ok=True)
        with open(os.path.join(directory, self.id), "a"):
            pass

    def untrack_quota(self) -> None:
        try:
            os.unlink(os.path.join(os.path.dirname(self.path), QUOTA_ACTIVE_DIR, self.id))
        except FileNotFoundError:
            pass

    def state(self) -> RunState | None:
        try:
            return self.status().state
        except (OSError, ValueError):
            return None

    def last_metrics(self) -> dict[str, Any] | None:
        for line in reversed(_tail(os.path.join(self.spec().out, METRICS_FILE), 16384).splitlines()):
            try:
                parsed = json.loads(line)
            except ValueError:
                continue
            return parsed if isinstance(parsed, dict) else {"value": parsed}
        return None


# Actor names of runs (the run, its other ranks, its --after waiter). The services'
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


def docker_command(spec: RunSpec, gpus: list[str], attempt: int = 1, rank: int = 0,
                   gang: GangEnv | None = None) -> list[str]:
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
    if spec.code_dir:
        cmd += ["-v", f"{spec.code_dir}:{spec.code_dir}:ro"]
    env: dict[str, str] = {"PYTHONUNBUFFERED": "1", **p.env, **spec.env,
                           "FLEET_RUN_ID": spec.id, "FLEET_OUT": spec.out,
                           "FLEET_METRICS": os.path.join(spec.out, METRICS_FILE), "FLEET_ATTEMPT": str(attempt)}
    if spec.code_dir:
        env["FLEET_CODE"] = spec.code_dir
    if gang:
        env.update(to_env(gang))
    for k, v in env.items():
        cmd += ["-e", f"{k}={spec.expand(v)}"]
    cmd.append(p.image)
    cmd += [spec.expand(a) for a in spec.command]
    return cmd


def missing_mounts(spec: RunSpec) -> list[str]:
    """Mounts that do not exist where the run landed; the run fails instead of docker
    creating them, owned by root, in someone's tree."""
    return [m.path for m in spec.mounts if not os.path.exists(m.path)]


def _tail(path: str, nbytes: int = 65536) -> str:
    try:
        with open(path, "rb") as f:
            f.seek(0, 2)
            f.seek(max(0, f.tell() - nbytes))
            return f.read().decode(errors="replace")
    except OSError:
        return ""


# ── the actors ───────────────────────────────────────────────────────────────

class _Rank:
    """One machine's share of a run: the cards Ray granted there and one container.
    Rank 0 lives inside the run's actor; on a run over several machines (--nodes)
    every other rank is a RankActor on its own machine."""

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

    def start(self, attempt: int, gang: GangEnv | None) -> str | None:
        """Starts the container; returns docker's error, or None."""
        with open(os.path.join(os.path.dirname(self.spec.out), ".lock"), "a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            st = RunStatus.read(os.path.join(os.path.dirname(self.spec.out), STATUS_FILE))
            if st.state.terminal or st.state is RunState.PREEMPTED:
                return "run stopped before container creation"
            _sh(["docker", "rm", "-f", self.name])
            mine = replace(gang, node_rank=self.rank) if gang else None
            out = _sh(docker_command(self.spec, self.gpus, attempt, self.rank, mine))
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


RankActor = ray.remote(_Rank)


class _GpuRun:
    """Runs one job to completion (with its retries). Not restarted by Ray: a run that
    dies is reported, not silently repeated. With --nodes N it is rank 0 and starts
    one RankActor per other bundle of its placement group (one per machine); every
    attempt starts all ranks together, and the first rank to fail ends the others."""

    def __init__(self, spec: RunSpec, run_dir: str) -> None:
        self.spec = spec
        self.dir = RunDir(os.path.dirname(run_dir), os.path.basename(run_dir))
        self.local = _Rank(spec, 0)  # rank 0, in this process
        self.others: list[Any] = []  # RankActor handles, one per further machine
        self.gpus = self.local.gpus
        self.infos: list[RankInfo] = []
        if spec.nodes > 1:
            pg = current_placement_group()
            for i in range(1, spec.nodes):
                b = bundle(spec, i)
                # kill() must reach a rank while it blocks in wait(): hence the concurrency
                self.others.append(RankActor.options(
                    name=actor_name(spec.id, i), namespace=NAMESPACE, num_cpus=b.cpu, num_gpus=b.gpu,
                    max_restarts=0, max_task_retries=0, max_concurrency=4,
                    scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg, placement_group_bundle_index=i),
                ).remote(spec, i))
        # where the containers will live: cancel and status reach them there
        info = self.local.info()
        self.dir.update(machine=info.machine, node_ip=info.node_ip, launch_pending=False)
        threading.Thread(target=self._run, daemon=True).start()

    @property
    def n(self) -> int:
        return 1 + len(self.others)

    def ping(self) -> RankInfo:
        return self.local.info()

    def _all(self, method: str, *args: Any) -> list[Any]:
        """Calls a method on every rank (the others in parallel), results in rank order."""
        refs = [getattr(r, method).remote(*args) for r in self.others]
        return [getattr(self.local, method)(*args)] + list(ray.get(refs, timeout=300))

    def _label(self, i: int) -> str:
        return f"rank {i} ({self.infos[i].machine}): " if self.n > 1 else ""

    def _wait_vram(self, need: int | None) -> bool:
        deadline = time.time() + self.spec.vram_wait_s
        while True:
            if self.dir.state() in (RunState.CANCELLED, RunState.PREEMPTED):
                return False
            short = [self._label(i) + s for i, ss in enumerate(self._all("shortfall", need)) for s in ss]
            if not short:
                return True
            if time.time() > deadline:
                self.dir.update(state=RunState.FAILED, gpus=self.gpus, finished_at=time.time(),
                                error="cards not free in time: " + "; ".join(short))
                return False
            self.dir.update(state=RunState.WAITING_VRAM, gpus=self.gpus, detail="; ".join(short))
            time.sleep(15)

    def _gang_env(self) -> GangEnv | None:
        """Where the ranks meet: rank 0's machine and a port free there."""
        if self.n == 1:
            return None
        with socket.socket() as sock:
            sock.bind(("", 0))
            port = int(sock.getsockname()[1])
        return GangEnv(nnodes=self.n, master_addr=self.infos[0].node_ip, master_port=port,
                       nproc_per_node=max(len(self.gpus), 1))

    def _attempt(self, attempt: int) -> Attempt:
        """One start of every rank; how the first rank to fail ended, or exit 0."""
        n = self.n
        errors: list[str | None] = self._all("start", attempt, self._gang_env())
        bad = [(i, e) for i, e in enumerate(errors) if e]
        if bad:
            self._kill_all()
            return Attempt(exit_code=None, cause=Cause.DOCKER_RUN, error="; ".join(self._label(i) + e for i, e in bad))
        self.dir.update(state=RunState.RUNNING, gpus=self.gpus, container=self.local.name, attempt=attempt,
                        started_at=time.time(), detail=None)
        done: queue.Queue[tuple[int, Attempt]] = queue.Queue()

        def wait(i: int) -> None:
            try:
                res: Attempt = self.local.wait() if i == 0 else ray.get(self.others[i - 1].wait.remote())
            except Exception as e:  # its machine or actor died
                res = Attempt(exit_code=None, cause=Cause.RANK_LOST, error=repr(e)[:300])
            done.put((i, res))

        for i in range(n):
            threading.Thread(target=wait, args=(i,), daemon=True).start()
        failed: tuple[int, Attempt] | None = None
        for _ in range(n):
            i, res = done.get()
            if not res.ok and failed is None:
                failed = (i, res)
                if n > 1:
                    self._kill_all()  # a gang without one rank only hangs
        if failed is None:
            return Attempt(exit_code=0)
        i, res = failed
        return replace(res, rank=i, machine=self.infos[i].machine) if n > 1 else res

    def _kill_all(self) -> None:
        self.local.kill()
        for r in self.others:
            r.kill.remote()

    def _run(self) -> None:
        code: int | None = None
        try:
            if self.dir.status().state.terminal or self.dir.state() is RunState.PREEMPTED:
                return
            need = self.spec.need_mib
            self.infos = self._all("info")
            if self.n > 1:
                self.dir.update(ranks=self.infos)
            problems = [self._label(i) + p for i, p in enumerate(self._all("check", need)) if p]
            if problems:
                self.dir.update(state=RunState.FAILED, gpus=self.gpus, finished_at=time.time(), error="; ".join(problems))
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
                self._kill_all()
                time.sleep(2)  # let the other ranks' kills land before their actors go with the group
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


class _RunWaiter:
    """Holds a run until the run it depends on ends, without holding any card (a run
    waiting on another with the only free card would otherwise deadlock), then
    launches it if that run succeeded."""

    def __init__(self, spec: RunSpec, run_dir: str, runs_dir: str) -> None:
        self.spec, self.runs_dir = spec, runs_dir
        self.dir = RunDir(runs_dir, os.path.basename(run_dir))
        threading.Thread(target=self._wait, daemon=True).start()

    def _wait(self) -> None:
        dep = self.spec.after or ""
        try:
            while True:
                if self.dir.state() is RunState.CANCELLED:
                    return
                try:
                    st: RunState | str = RunDir(self.runs_dir, dep).status().state
                except (SystemExit, OSError, ValueError):
                    st = "missing"
                if st is RunState.SUCCEEDED:
                    self.dir.update(state=RunState.QUEUED, detail=None)
                    launch(self.spec, self.dir.path)
                    return
                if st == "missing" or st in (RunState.FAILED, RunState.CANCELLED, RunState.LOST):
                    ended = st.value if isinstance(st, RunState) else st
                    self.dir.update(state=RunState.FAILED, finished_at=time.time(),
                                    error=f"dependency {dep} ended {ended}; not started")
                    return
                time.sleep(10)
        finally:
            os._exit(0)


RunWaiter = ray.remote(_RunWaiter)


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


def _shared_storage(path: str) -> bool:
    """Whether every machine sees path (a network file system; FLEET_RUNS_SHARED=1 says
    so for one the type check does not know, e.g. a bind mount of one)."""
    if os.environ.get("FLEET_RUNS_SHARED") == "1":
        return True
    return _sh(["stat", "-f", "-c", "%T", path]).stdout.strip().startswith(NETWORK_FS)


def bundle(spec: RunSpec, rank: int = 0) -> Bundle:
    """One machine's share: --gpus cards and --cpus CPUs (per machine with --nodes)."""
    return Bundle(cpu=float(max(spec.cpus, 1)), gpu=float(spec.gpus),
                  pin=spec.machine_resources[rank] if spec.machine_resources else None)


def run_placement(spec: RunSpec) -> Any:
    """Stable placement identity lets a launch intent survive an API crash."""
    try:
        return ray.util.get_placement_group(pg_name(spec.id))
    except ValueError:
        return placement_group([bundle(spec, i).to_ray() for i in range(spec.nodes)],
                               strategy="STRICT_SPREAD" if spec.nodes > 1 else "STRICT_PACK",
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


def _extract(data: bytes, code_dir: str) -> None:
    """A code tarball into code/: regular files and directories only, every one inside
    code/ (no absolute or `..` paths, links or devices: the bridge user writes them)."""
    with tarfile.open(fileobj=io.BytesIO(data)) as t:
        members = t.getmembers()
        root = os.path.realpath(code_dir)
        for m in members:
            dest = os.path.realpath(os.path.join(root, m.name))
            if not (m.isfile() or m.isdir()) or os.path.isabs(m.name) or not dest.startswith(root + os.sep) and dest != root:
                raise SystemExit(f"--code: refusing tarball member {m.name!r} (only files and directories inside the tree)")
            m.mode &= 0o755
        for m in members:
            t.extract(m, root, set_attrs=m.isfile())


def _capture_code(source: str, code_dir: str, roots: list[str]) -> CodeRecord:
    """--code git:/repo@ref (git archive of that commit) or FILE.tar.gz, extracted into
    the run's code/ so the run executes exactly that and records what it was. The repo or
    file must be inside `roots` (the profile's allowed roots, or the uploads dir)."""
    path = source[4:].rpartition("@")[0] or source[4:] if source.startswith("git:") else source
    if not _inside(os.path.realpath(path), [os.path.realpath(r) for r in roots]):
        raise SystemExit(f"--code {source}: outside what the profile allows ({', '.join(roots)})")
    os.makedirs(code_dir)
    if source.startswith("git:"):
        repo, _, ref = source[4:].rpartition("@")
        if not repo:
            repo, ref = source[4:], "HEAD"
        commit = _sh(["git", "-C", repo, "rev-parse", "--verify", f"{ref}^{{commit}}"])
        if commit.returncode != 0:
            raise SystemExit(f"--code {source}: {commit.stderr.strip()}")
        sha = commit.stdout.strip()
        arch = subprocess.run(["git", "-C", repo, "archive", "--format=tar", sha], capture_output=True)
        _extract(arch.stdout, code_dir)
        dirty = _sh(["git", "-C", repo, "status", "--porcelain"]).stdout.strip()
        return CodeRecord(source=source, repo=repo, ref=ref, commit=sha,
                          note="working-tree changes are NOT included" if dirty else None)
    if not os.path.isfile(source):
        raise SystemExit(f"--code {source}: no such file (a tarball on this machine, or git:/repo@ref)")
    with open(source, "rb") as f:
        data = f.read()
    _extract(data, code_dir)
    return CodeRecord(source=source, sha256=hashlib.sha256(data).hexdigest(), bytes=len(data))


@dataclass(kw_only=True)
class Placement(Record):
    """Where a submit may put the run, after the cluster's shape is checked."""

    nodes: int
    machines: list[str]
    machine_resources: list[str]
    note: str | None = None


def _placement(a: RunArgs) -> Placement:
    alive = len(alive_nodes())
    machines = list(a.machine)
    note = None
    if a.nodes < 1:
        raise SystemExit("--nodes: at least 1")
    if a.nodes > alive:
        raise SystemExit(f"--nodes {a.nodes}: the cluster has {alive} machine(s)")
    if machines and len(machines) != a.nodes:
        raise SystemExit(f"--machine: give one per node ({a.nodes}) or none")
    if alive > 1 and not _shared_storage(a.runs_dir):
        here = node_ip()
        if a.nodes > 1:
            raise SystemExit(f"--nodes {a.nodes}: the runs dir {a.runs_dir} is not shared storage")
        if machines and _machine_resource(machines[0]) != f"node:{here}":
            raise SystemExit(f"--machine {machines[0]}: the runs dir {a.runs_dir} is not shared storage, "
                             "so a run can only land on this machine")
        machines, note = [here], f"runs dir {a.runs_dir} is not shared storage: the run stays on this machine"
    resources = [_machine_resource(m) for m in machines]
    if len(set(resources)) != len(resources):
        raise SystemExit("--machine: the nodes of one run are different machines")
    return Placement(nodes=a.nodes, machines=machines, machine_resources=resources, note=note)


@dataclass(kw_only=True)
class Submitted(Record):
    id: str
    state: RunState
    priority: int = 0
    dir: str
    out: str
    note: str | None = None


def submit(a: RunArgs, cmd: list[str], config: RunsConfig) -> Submitted:
    if a.profile is None or a.profile not in config.profiles:
        raise SystemExit(f"unknown profile {a.profile!r} (have: {', '.join(config.profiles) or 'none'})")
    profile = config.profiles[a.profile]
    if a.namespace is not None and profile.namespace != a.namespace:
        raise Conflict("run app belongs to another namespace")
    if profile.disabled:
        raise Conflict(f"run app {a.profile} is disabled: it takes no new runs (fleetctl app enable {a.profile})")
    if not cmd:
        raise SystemExit("no command after --")
    if a.gpus > profile.max_gpus:
        raise SystemExit(f"profile {a.profile} allows at most {profile.max_gpus} GPUs")
    if a.gpus > 1 and not float(a.gpus).is_integer():
        raise SystemExit("--gpus: whole cards, or one fraction of a card (0 < gpus < 1)")
    gpus = int(a.gpus) if float(a.gpus).is_integer() else a.gpus
    if 0 < gpus < 1 and not (a.gpu_mem or profile.default_gpu_mem_gb):
        raise SystemExit("a fractional --gpus shares the card with other runs: give --gpu-mem (the job's peak GiB)")
    mounts = [Mount.parse(m) for m in a.mount]
    for m in mounts:
        if not _inside(m.path, profile.allow):
            raise SystemExit(f"--mount {m.path} is outside what profile {a.profile} allows ({', '.join(profile.allow) or 'nothing'})")
        if not os.path.exists(m.path):
            raise SystemExit(f"--mount {m.path} does not exist (create it before submitting)")
    if a.after:
        RunDir(a.runs_dir, a.after)
    place = _placement(a)

    base = re.sub(r"[^A-Za-z0-9_.-]", "-", a.name or a.profile)[:40].lstrip(".") or "run"
    run = RunDir(a.runs_dir, f"{base}-{uuid.uuid4().hex[:8]}", must_exist=False)
    os.makedirs(run.out)
    writable = [m.path for m in mounts if m.mode is MountMode.RW] + [run.out]
    if a.log and not _inside(a.log, writable):
        shutil.rmtree(run.path)
        raise SystemExit(f"--log {a.log} is outside the run's writable mounts")
    code = None
    if a.code:
        try:
            code = _capture_code(a.code, run.code, profile.allow + [os.path.join(a.runs_dir, UPLOADS_DIR)])
        except BaseException:
            shutil.rmtree(run.path)
            raise
    gpu_mem = a.gpu_mem or profile.default_gpu_mem_gb
    spec = RunSpec(id=run.id, owner=a.owner, namespace=a.namespace, tag=a.tag, quota_managed=a.namespace is not None, profile_name=a.profile, profile=profile, command=cmd, gpus=gpus, cpus=a.cpus,
                   mounts=mounts, env=a.env_map, log=a.log, workdir=a.workdir, out=run.out,
                   code_dir=run.code if code else None, code=code, gpu_mem_gb=gpu_mem, vram_wait_s=a.vram_wait,
                   retry=a.retry, restart_on_oom=a.restart_on_oom, after=a.after, nodes=place.nodes,
                   machines=place.machines, machine_resources=place.machine_resources)
    roots = [m.path for m in mounts] + [run.out] + ([run.code] if code else [])
    if a.workdir and not _inside(spec.expand(a.workdir), roots):
        shutil.rmtree(run.path)
        raise SystemExit(f"--workdir {a.workdir} is outside the run's mounts")
    spec.write(run.spec_path)
    if a.namespace:
        run.track_quota()
    state = RunState.WAITING_DEPENDENCY if a.after else RunState.WAITING_QUOTA if a.namespace else RunState.QUEUED
    RunStatus(id=run.id, state=state, priority=profile.priority, namespace=a.namespace, tag=a.tag, quota_released=bool(a.namespace), submitted_at=time.time(), gpus_requested=gpus, cpus=a.cpus,
              gpu_mem_gb=gpu_mem or None, after=a.after, code=code, nodes=place.nodes).write(run.status_path)
    if a.namespace:
        return Submitted(id=run.id, state=state, priority=profile.priority, dir=run.path, out=run.out, note=place.note)
    if a.after:
        pin = place.machine_resources[0] if place.machine_resources else None
        RunWaiter.options(name=waiter_name(run.id), namespace=NAMESPACE, lifetime="detached", num_cpus=0,
                          max_restarts=0, resources={pin: 0.001} if pin else None).remote(spec, run.path, a.runs_dir)
    else:
        launch(spec, run.path)
    return Submitted(id=run.id, state=state, priority=profile.priority, dir=run.path, out=run.out, note=place.note)


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


def status(a: RunArgs, run_id: str) -> RunStatus:
    run = RunDir(a.runs_dir, run_id)
    st = run.status()
    if _state(run_id, st) is RunState.LOST:
        # The actor is gone without reporting (a Ray restart): say what the container does.
        st = st.merged(state=RunState.LOST, container=_container_states(st, run_id))
    spec = run.spec()
    return st.merged(metrics=run.last_metrics(), out=spec.out, priority=spec.profile.priority)


def _container_states(st: RunStatus, run_id: str) -> str:
    states = []
    for r in st.places():
        out = _on_run_machine(r, ["docker", "inspect", "-f", "{{.State.Status}}", container_name(run_id, r.rank)])
        state = (out.strip() or "gone") if out is not None else f"unknown ({r.machine} left the cluster)"
        states.append(f"rank {r.rank} {state}" if st.ranks else state)
    return "; ".join(states)


def cancel(a: RunArgs, run_id: str) -> RunStatus:
    run = RunDir(a.runs_dir, run_id)
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
# A service that needs cards a run holds takes them (reconcile.start): the run is
# stopped with grace (SIGTERM, SIGKILL its app's stop_timeout_s later), its attempt
# recorded as preempted, and it queues again for cards with the same id, out/ and
# FLEET_ATTEMPT counting on; a preemption spends no --retry. Only runs of a
# `preemptible` app that have run its min_runtime_s are taken (one still waiting
# for card memory at any time: it has nothing to lose), the newest first, on the
# machine where the fewest of them free enough (Slurm's REQUEUE with PreemptExemptTime;
# Kueue's "most recently admitted first").

PREEMPTIBLE = (RunState.RUNNING, RunState.WAITING_VRAM, RunState.RETRYING)


@dataclass(kw_only=True)
class Candidate(Record):
    """A run's share of one machine, as a preemption sees it."""

    id: str
    namespace: str
    priority: int
    tag: str | None = None
    node_ip: str
    gpus: float
    cpus: float
    started_at: float | None  # its attempt's start; None while it waits for card memory
    preemptible: bool
    min_runtime_s: int

    def takeable(self, now: float) -> bool:
        return self.preemptible and (self.started_at is None or now - self.started_at >= self.min_runtime_s)


@dataclass(kw_only=True)
class Victims(Record):
    """Where a service fits once these runs are preempted (none: it fits as is)."""

    node_ip: str
    runs: list[str] = field(default_factory=list)


def choose_victims(need_gpus: float, need_cpus: float, free: dict[str, tuple[float, float]],
                   candidates: list[Candidate], now: float, pin: str | None = None, *, namespace: str, priority: int) -> Victims | None:
    """Choose only lower-priority runs in this namespace, lowest priority first.
    Ties prefer fewer victims, then unstarted and newer executions.
    """
    best: Victims | None = None
    best_score: tuple[int, ...] | None = None
    for ip, (gpus, cpus) in sorted(free.items()):
        if pin is not None and ip != pin:
            continue
        if gpus >= need_gpus and cpus >= need_cpus:
            return Victims(node_ip=ip)
        pool = sorted((c for c in candidates if c.node_ip == ip and c.namespace == namespace and c.priority < priority and c.takeable(now)),
                      key=lambda c: (c.priority, c.started_at is not None, -(c.started_at or 0)))
        picked: list[str] = []
        priorities: list[int] = []
        for c in pool:
            if gpus >= need_gpus and cpus >= need_cpus:
                break
            picked.append(c.id)
            priorities.append(c.priority)
            gpus, cpus = gpus + c.gpus, cpus + c.cpus
        score = tuple(sorted(priorities, reverse=True))
        if gpus >= need_gpus and cpus >= need_cpus and (best_score is None or score < best_score):
            best = Victims(node_ip=ip, runs=picked)
            best_score = score
    return best


def plan_preemption(spec: RunSpec, free: dict[str, tuple[float, float]], pool: list[Candidate],
                    now: float, quota_fits: Callable[[set[str]], bool]) -> list[str] | None:
    """Plan a complete placement and quota fit before stopping any run.

    Multi-node jobs need distinct nodes. Stopping a run credits all its ranks,
    while namespace/tag quota is credited once for the whole execution.
    """
    eligible = [c for c in pool if c.namespace == spec.namespace and c.priority < spec.profile.priority and c.takeable(now)]
    if not quota_fits({c.id for c in eligible}):
        return None
    available = dict(free)
    selected: set[str] = set()
    order: list[str] = []
    placed: list[str] = []

    def select(run_id: str) -> None:
        if run_id in selected:
            return
        selected.add(run_id)
        order.append(run_id)
        for c in eligible:
            if c.id == run_id and c.node_ip in available:
                g, cpu = available[c.node_ip]
                available[c.node_ip] = g + c.gpus, cpu + c.cpus

    for rank in range(spec.nodes):
        pin = spec.machine_resources[rank].split(":", 1)[1] if spec.machine_resources else None
        victims = choose_victims(spec.gpus, max(spec.cpus, 1), available,
            [c for c in eligible if c.id not in selected], now, pin,
            namespace=spec.namespace or "", priority=spec.profile.priority)
        if victims is None:
            return None
        for run_id in victims.runs:
            select(run_id)
        placed.append(victims.node_ip)
        del available[victims.node_ip]
    for c in sorted(eligible, key=lambda c: (c.priority, c.started_at is not None, -(c.started_at or 0), c.id)):
        if quota_fits(selected):
            break
        select(c.id)
    if not quota_fits(selected):
        return None
    # Avoid evicting runs that cannot help the occupied tag cap, or redundant ranks.
    for run_id in reversed(order):
        remaining = selected - {run_id}
        if quota_fits(remaining) and all(
            free[ip][0] + sum(c.gpus for c in eligible if c.id in remaining and c.node_ip == ip) >= spec.gpus
            and free[ip][1] + sum(c.cpus for c in eligible if c.id in remaining and c.node_ip == ip) >= max(spec.cpus, 1)
            for ip in placed):
            selected = remaining
    return [run_id for run_id in order if run_id in selected]


def candidates(runs_dir: str) -> list[Candidate]:
    """Every run holding cards or CPUs now (its actor placed), per machine it is on."""
    out = []
    active = os.path.join(runs_dir, QUOTA_ACTIVE_DIR)
    for i in os.listdir(active) if os.path.isdir(active) else []:
        if i.startswith("."):
            continue
        try:
            run = RunDir(runs_dir, i)
            st, spec = run.status(), run.spec()
        except (SystemExit, OSError, ValueError):
            continue
        if not spec.namespace or st.state not in PREEMPTIBLE or not _alive(actor_name(i)):
            continue
        started = st.started_at if st.state is RunState.RUNNING else None
        for r in st.places():
            if r.node_ip:
                out.append(Candidate(id=i, namespace=spec.namespace, priority=spec.profile.priority, tag=spec.tag, node_ip=r.node_ip, gpus=float(spec.gpus), cpus=float(max(spec.cpus, 1)),
                                     started_at=started, preemptible=spec.profile.preemptible,
                                     min_runtime_s=spec.profile.min_runtime_s))
    return out


def preempt(runs_dir: str, run_id: str, reason: str, requeue: bool = True, *,
            namespace: str | None = None, priority: int | None = None, defer_requeue: bool = False) -> RunStatus:
    """Stop with grace, retaining reservations until every container is confirmed stopped.
    Automatic callers must supply the requesting namespace and priority.
    """
    run = RunDir(runs_dir, run_id)
    spec, before = run.spec(), run.status()
    if namespace is not None or priority is not None:
        candidate = Candidate(id=run_id, namespace=spec.namespace or "", priority=spec.profile.priority,
            node_ip="", gpus=spec.gpus, cpus=spec.cpus, started_at=before.started_at if before.state is RunState.RUNNING else None,
            preemptible=spec.profile.preemptible, min_runtime_s=spec.profile.min_runtime_s)
        if namespace != spec.namespace or priority is None or candidate.priority >= priority or not candidate.takeable(time.time()):
            raise Conflict(f"{run_id}: not an eligible lower-priority run in this namespace")
    if before.state not in PREEMPTIBLE or not _alive(actor_name(run_id)):
        raise Conflict(f"{run_id} is {_state(run_id, before).value}: it holds no card to preempt")
    attempts = list(before.attempts)
    if before.state is RunState.RUNNING:
        attempts.append(Attempt(exit_code=None, cause=Cause.PREEMPTED, evidence=reason))
    st = run.update(expected_state=before.state, state=RunState.PREEMPTED, preempt_requeue=requeue,
                    attempts=attempts, preemptions=before.preemptions + 1, detail=f"preempted: {reason}")
    if st.state is not RunState.PREEMPTED:
        raise Conflict(f"{run_id} is {st.state.value}: nothing to preempt")
    if before.state is RunState.RUNNING:
        _stop_ranks(st, run_id, spec.profile.stop_timeout_s)
    _end_actors(run_id, st.nodes)
    if not confirm_cleanup(run):
        return run.update(force=True, detail=f"preempted ({reason}): waiting for confirmed container cleanup")
    if requeue and not defer_requeue:
        requeue_run(runs_dir, run_id)
    return run.status()


def requeue_run(runs_dir: str, run_id: str) -> None:
    """Persist requeue intent; never restart while a previous container may be alive."""
    run = RunDir(runs_dir, run_id)
    if run.state() not in (RunState.QUEUED, RunState.PREEMPTED):
        return
    run.update(force=True, preempt_requeue=True)
    if not confirm_cleanup(run):
        return
    spec = run.spec()
    if spec.namespace:
        run.update(force=True, state=RunState.WAITING_QUOTA, preempt_requeue=False, quota_released=run.status().quota_released, started_at=None,
                   container=None, detail="preempted: waiting for resources again")
    else:
        run.update(force=True, state=RunState.QUEUED, preempt_requeue=False, started_at=None, container=None)
        launch(spec, run.path)


def log_chunk(runs_dir: str, run_id: str, rank: int = 0, offset: int | None = None, lines: int = 50,
              max_bytes: int = 4 << 20) -> LogChunk:
    """The run's output: with no offset its last `lines` lines, else what was written
    after `offset` (at most max_bytes); `offset` of the result is where to go on."""
    run = RunDir(runs_dir, run_id)
    log_path = run.spec().rank_log_path(rank)
    size = os.path.getsize(log_path) if os.path.exists(log_path) else 0
    if offset is None:
        data = "\n".join(_tail(log_path, 1 << 20).splitlines()[-lines:] if lines else [])
        data, end = (data + "\n" if data else ""), size
    else:
        start = min(offset, size)
        with open(log_path, "rb") if size else io.BytesIO() as f:
            f.seek(start)
            raw = f.read(max_bytes)
        data, end = raw.decode(errors="replace"), start + len(raw)
    try:
        state = _state(run_id, run.status())
    except (OSError, ValueError):
        state = RunState.LOST
    done = end >= size and not state.active
    return LogChunk(data=data, offset=end, done=done)


def logs(a: RunArgs, run_id: str) -> None:
    """The run's output: the last -n lines, then with -f everything new until it ends."""
    chunk = log_chunk(a.runs_dir, run_id, a.rank, None, a.lines)
    sys.stdout.write(chunk.data)
    while a.follow and not chunk.done:
        chunk = log_chunk(a.runs_dir, run_id, a.rank, chunk.offset)
        sys.stdout.write(chunk.data)
        sys.stdout.flush()
        if not chunk.data:
            time.sleep(1)


def fetch(a: RunArgs, run_id: str, out: Any = None) -> None:
    """out/ as a tar.gz, to `out` (a binary stream; stdout by default): `run.sh fetch ID > out.tgz`."""
    run = RunDir(a.runs_dir, run_id)
    with tarfile.open(fileobj=out or sys.stdout.buffer, mode="w|gz") as t:
        t.add(run.out, arcname=f"{run_id}/{OUT_DIR}")


def _dir_bytes(d: str) -> int:
    return sum(os.path.getsize(os.path.join(r, f)) for r, _, fs in os.walk(d) for f in fs
               if not os.path.islink(os.path.join(r, f)))


@dataclass(kw_only=True)
class Purged(Record):
    id: str
    purged: bool
    state: RunState | None = None
    bytes: int | None = None
    reason: str | None = None


def purge(a: RunArgs, run_id: str | None) -> list[Purged]:
    """Deletes finished runs' records (spec, status, log, out/, code/): one ID, or every
    finished run older than --older-than days. A run still going is refused."""
    if run_id:
        targets = [RunDir(a.runs_dir, run_id)]
    elif a.older_than is not None:
        cut = time.time() - a.older_than * 86400
        targets = [RunDir(a.runs_dir, d) for d in os.listdir(a.runs_dir) if not d.startswith(".")
                   and os.path.isdir(os.path.join(a.runs_dir, d)) and os.path.getmtime(os.path.join(a.runs_dir, d)) < cut]
        uploads = os.path.join(a.runs_dir, UPLOADS_DIR)
        for root, _, files in os.walk(uploads):
            for name in files:
                path = os.path.join(root, name)
                if os.path.getmtime(path) < cut:
                    os.remove(path)
    else:
        raise SystemExit("purge ID, or purge --older-than DAYS")
    done = []
    for run in targets:
        state = run.state()
        if state in (RunState.WAITING_QUOTA, RunState.WAITING_DEPENDENCY):
            done.append(Purged(id=run.id, purged=False, state=state, reason="still waiting: cancel it first"))
            continue
        if (run.spec().namespace or run.spec().owner) and not run.status().quota_released:
            done.append(Purged(id=run.id,purged=False,state=state,reason="execution cleanup and quota release pending"))
            continue
        if (state is None or not state.terminal or run.spec().namespace or run.spec().owner) and _going(run.id):
            done.append(Purged(id=run.id, purged=False, reason=f"still {state.value if state else '?'}: cancel it first"))
            continue
        if state is not None and not state.terminal:  # lost: its containers may still hold cards
            containers = _container_states(run.status(), run.id)
            if "running" in containers or "unknown" in containers:
                done.append(Purged(id=run.id, purged=False, state=RunState.LOST,
                                   reason=f"lost, container {containers}: cancel it first (that removes it)"))
                continue
        size = _dir_bytes(run.path)
        shutil.rmtree(run.path)
        done.append(Purged(id=run.id, purged=True, state=state, bytes=size))
    return done


@dataclass(kw_only=True)
class RunSummary(Record):
    """The columns `list` shows: a projection of RunStatus, loaded from its dump (the
    codec drops the rest)."""

    id: str
    state: RunState
    namespace: str | None = None
    tag: str | None = None
    priority: int = 0
    profile: str | None = None  # its app
    gpus: list[str] = field(default_factory=list)
    exit_code: int | None = None
    cause: Cause | None = None
    after: str | None = None
    submitted_at: float | None = None
    started_at: float | None = None
    finished_at: float | None = None


def in_namespace(spec: RunSpec, namespace: str, tag: str | None = None, all_tags: bool = False) -> bool:
    """Access requires the recorded namespace and optional tag to match exactly."""
    return spec.namespace == namespace and (all_tags or spec.tag == tag)


def list_runs(a: RunArgs, n: int = 20, active: bool = False, profile: str | None = None) -> list[RunSummary]:
    """The newest n runs by submit time, of one app and only the active ones when asked
    (every run is read, as `apps` does, so an old run still going is not missed)."""
    if not os.path.isdir(a.runs_dir):
        return []
    out = []
    for i in os.listdir(a.runs_dir):
        if i.startswith("."):
            continue
        try:
            run = RunDir(a.runs_dir, i)
            st = run.status()
            spec = run.spec()
            if a.namespace is not None and not in_namespace(spec, a.namespace, a.tag, a.namespace_all_tags):
                continue
            if a.owner is not None and spec.owner != a.owner:
                continue
            s = from_dict(RunSummary, {**st.dump(), "state": _state(i, st), "profile": spec.profile_name,
                                      "namespace": spec.namespace, "tag": spec.tag, "priority": spec.profile.priority})
        except (SystemExit, OSError, ValueError):
            continue
        if (not active or s.state.active) and (profile is None or s.profile == profile):
            out.append(s)
    out.sort(key=lambda s: s.submitted_at or 0, reverse=True)
    return out[:n]


@dataclass(kw_only=True)
class RunApp(Record):
    """A run app: a runs.yaml profile (what a run may be), and how its jobs (runs) went."""

    name: str
    namespace: str
    priority: int
    image: str
    user: str | None
    max_gpus: int
    default_gpu_mem_gb: int
    allow: list[str]
    shm_gb: int = 16
    env: dict[str, str] = field(default_factory=dict)
    jobs: int = 0
    active: int = 0
    succeeded: int = 0
    failed: int = 0  # failed or lost
    cancelled: int = 0
    last_submitted_at: float | None = None
    disabled: bool = False  # takes no new runs
    removed: bool = False  # no longer in runs.yaml: only its past runs are left


def apps(a: RunArgs, config: RunsConfig) -> list[RunApp]:
    """Every profile, with the runs submitted under it (the runs dir is scanned once)."""
    by_name = {name: _run_app(name, p) for name, p in config.profiles.items()
               if a.namespace is None or p.namespace == a.namespace}
    for d in os.listdir(a.runs_dir) if os.path.isdir(a.runs_dir) else []:
        if d.startswith("."):
            continue
        try:
            run = RunDir(a.runs_dir, d)
            spec, st = run.spec(), run.status()
            if a.namespace is not None and not in_namespace(spec, a.namespace, a.tag, a.namespace_all_tags):
                continue
            if a.owner is not None and spec.owner != a.owner:
                continue
        except (SystemExit, OSError, ValueError):
            continue
        app = by_name.get(spec.profile_name)
        if app is None:  # a profile since removed from runs.yaml (or model staging's own)
            app = by_name[spec.profile_name] = _run_app(spec.profile_name, spec.profile, removed=True)
        state = _state(d, st)
        app.jobs += 1
        app.active += state.active
        app.succeeded += state is RunState.SUCCEEDED
        app.failed += state in (RunState.FAILED, RunState.LOST)
        app.cancelled += state is RunState.CANCELLED
        app.last_submitted_at = max(app.last_submitted_at or 0, st.submitted_at)
    return sorted(by_name.values(), key=lambda x: x.last_submitted_at or 0, reverse=True)


@dataclass(kw_only=True)
class AppDeleted(Record):
    app: str
    profiles: str  # the file it was removed from
    runs_kept: int  # its past runs stay listed (under an app that no longer exists)


# A run app's name: it heads its runs' ids, so it is one too.
APP_NAME = RUN_ID


def _indent(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def _is_text(line: str) -> bool:
    body = line.strip()
    return bool(body) and not body.startswith("#")


def _profiles_top(lines: list[str]) -> int:
    top = next((i for i, l in enumerate(lines) if re.match(r"profiles:\s*(#.*)?$", l)), None)
    if top is None:
        raise SystemExit("runs.yaml: no block-style `profiles:` mapping to edit")
    return top


def _profile_span(lines: list[str], name: str) -> tuple[int, int, int]:
    """(profiles line, first line, line after the last) of one profile's block in
    runs.yaml's text: from its `name:` line (and the comment right above it) to the next
    line at its indentation or less. Comments heading the next entry are left to it."""
    top = _profiles_top(lines)
    key = re.compile(rf"""(\s+)(?:{re.escape(name)}|"{re.escape(name)}"|'{re.escape(name)}'):\s*(#.*)?$""")
    start = next((i for i in range(top + 1, len(lines)) if key.match(lines[i].rstrip("\n"))), None)
    if start is None:
        raise SystemExit(f"runs.yaml: profile {name} is not written as a block under `profiles:`; edit it by hand")
    indent = _indent(lines[start])
    end = start + 1
    while end < len(lines) and not (_is_text(lines[end]) and _indent(lines[end]) <= indent):
        end += 1
    while end > start + 1 and lines[end - 1].strip().startswith("#"):
        end -= 1
    while start - 1 > top and lines[start - 1].startswith(" " * indent + "#"):
        start -= 1
    return top, start, end


def _without_profile(text: str, name: str) -> str:
    """runs.yaml's text with one profile's block cut out, the rest (comments included)
    as it was."""
    lines = text.splitlines(keepends=True)
    _, start, end = _profile_span(lines, name)
    rest = lines[:start] + lines[end:]
    # the blank line that set the block apart, now doubled or at the end
    if start > 0 and not rest[start - 1].strip() and (start >= len(rest) or not rest[start].strip()):
        del rest[start - 1]
    return "".join(rest)


def _with_profile_disabled(text: str, name: str, disabled: bool) -> str:
    """runs.yaml's text with `disabled: true` as the profile's first line, or without
    any `disabled:` line of its own."""
    lines = text.splitlines(keepends=True)
    _, start, end = _profile_span(lines, name)
    key = next(i for i in range(start, end) if _is_text(lines[i]))  # past its heading comment
    inner = next((_indent(lines[i]) for i in range(key + 1, end) if _is_text(lines[i])), _indent(lines[key]) + 2)
    block = [l for l in lines[key + 1:end] if not (_indent(l) == inner and re.match(r"\s*disabled\s*:", l))]
    if disabled:
        block.insert(0, " " * inner + "disabled: true\n")
    return "".join(lines[:key + 1] + block + lines[end:])


def set_app_priority(a: RunArgs, name: str, priority: int) -> RunApp:
    config = load_config(a.profiles)
    if name not in config.profiles:
        raise Conflict(f"no run app {name}")
    profile = config.profiles[name]
    profile.priority = priority
    profile.__post_init__()

    def edit(text: str) -> str:
        root = yaml.compose(text)
        profiles = next(v for k, v in root.value if k.value == "profiles")
        entry = next(v for k, v in profiles.value if k.value == name)
        if entry.flow_style:
            raise Conflict("profiles must use block YAML for priority changes")
        value = next((v for k, v in entry.value if k.value == "priority"), None)
        if value is not None:
            return text[:value.start_mark.index] + str(priority) + text[value.end_mark.index:]
        pos = entry.start_mark.index
        return text[:pos] + f"priority: {priority}\n" + " " * entry.start_mark.column + text[pos:]

    config = _edit_profiles(a, edit,
        lambda b: {**b, "profiles": {**b["profiles"], name: {**b["profiles"][name], "priority": priority}}},
        f"setting {name} priority")
    return _run_app(name, config.profiles[name])


def _profile_mapping(profile: Profile) -> dict[str, Any]:
    """The profile as runs.yaml writes it: its fields, less the ones at their defaults."""
    return {k: v for k, v in profile.dump().items()
            if v not in (None, [], {}, False) and not (k == "default_gpu_mem_gb" and v == 0)}


def _with_profile_added(text: str, name: str, profile: Profile) -> str:
    """runs.yaml's text with a profile's block appended under `profiles:` (before the
    next top-level key), written the way the file writes the others."""
    lines = text.splitlines(keepends=True)
    top = _profiles_top(lines)
    end = next((i for i in range(top + 1, len(lines)) if _is_text(lines[i]) and _indent(lines[i]) == 0), len(lines))
    inner = next((_indent(lines[i]) for i in range(top + 1, end) if _is_text(lines[i])), 2)
    rendered = yaml.safe_dump({name: _profile_mapping(profile)}, sort_keys=False, default_flow_style=False,
                              allow_unicode=True)
    block = [" " * inner + l + "\n" for l in rendered.splitlines()]
    while end > top + 1 and not lines[end - 1].strip():
        end -= 1
    if lines[end - 1] and not lines[end - 1].endswith("\n"):
        lines[end - 1] += "\n"
    sep = ["\n"] if end > top + 1 else []
    tail = lines[end:]
    if tail and _is_text(tail[0]):
        tail = ["\n"] + tail
    return "".join(lines[:end] + sep + block + tail)


def _write_profiles(path: str, text: str) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        f.write(text)
    shutil.copymode(path, tmp)
    os.replace(tmp, path)


def _edit_profiles(a: RunArgs, edit: Callable[[str], str], expect: Callable[[dict[str, Any]], dict[str, Any]],
                   what: str) -> RunsConfig:
    """Applies a text edit to runs.yaml, writes it only when the parsed result is exactly
    `expect(before)` (nothing else moved), and returns the new config."""
    with open(a.profiles) as f:
        text = f.read()
    new = edit(text)
    before, after = yaml.safe_load(text) or {}, yaml.safe_load(new) or {}
    if after != expect(before):
        raise SystemExit(f"runs.yaml: {what} would change more than that; edit it by hand")
    config = RunsConfig.load(after)  # what the scheduler will read: refused before writing if it does not load
    _write_profiles(a.profiles, new)
    return config


def _run_app(name: str, p: Profile, **counts: Any) -> RunApp:
    return RunApp(name=name, namespace=p.namespace, priority=p.priority, image=p.image, user=p.user, max_gpus=p.max_gpus, default_gpu_mem_gb=p.default_gpu_mem_gb,
                  allow=p.allow, shm_gb=p.shm_gb, env=p.env, disabled=p.disabled, **counts)


def add_app(a: RunArgs, name: str, body: dict[str, Any]) -> RunApp:
    """Adds a run app: a new profile in runs.yaml, from the request's fields (strictly
    Profile's). Every allowed root is an absolute path."""
    if not APP_NAME.fullmatch(name):
        raise SystemExit(f"not an app name: {name!r} (letters, digits, _ . -; it heads its runs' ids)")
    config = load_config(a.profiles)
    if name in config.profiles:
        raise Conflict(f"run app {name} exists (fleetctl app lists it); delete it first to replace it")
    if not body.get("namespace"):
        raise SystemExit("namespace: an existing namespace is required")
    profile = from_dict(Profile, body, strict=True, where="app")
    if not profile.image:
        raise SystemExit("image: the container image a run gets")
    bad = [r for r in profile.allow if not os.path.isabs(r)]
    if bad:
        raise SystemExit(f"allow: absolute paths on the server ({', '.join(bad)})")
    _edit_profiles(a, lambda t: _with_profile_added(t, name, profile),
                   lambda b: {**b, "profiles": {**b.get("profiles", {}), name: _profile_mapping(profile)}},
                   f"adding {name}")
    print(f"run app {name} added", flush=True)
    return _run_app(name, load_config(a.profiles).profiles[name])


def set_app_disabled(a: RunArgs, name: str, disabled: bool) -> RunApp:
    """Disables a run app (its new runs are refused; the ones going finish) or enables
    it again: `disabled: true` in its profile."""
    config = load_config(a.profiles)
    if name not in config.profiles:
        raise NotFound(f"no run app {name} in runs.yaml (have: {', '.join(config.profiles) or 'none'})")

    def expect(b: dict[str, Any]) -> dict[str, Any]:
        p = {k: v for k, v in b["profiles"][name].items() if k != "disabled"}
        return {**b, "profiles": {**b["profiles"], name: {**p, "disabled": True} if disabled else p}}

    config = _edit_profiles(a, lambda t: _with_profile_disabled(t, name, disabled), expect,
                            f"{'disabling' if disabled else 'enabling'} {name}")
    print(f"run app {name} {'disabled' if disabled else 'enabled'}", flush=True)
    return _run_app(name, config.profiles[name])


def delete_app(a: RunArgs, name: str) -> AppDeleted:
    """Removes a run app (its profile in runs.yaml), refused while any of its runs is
    queued or running. Its past runs stay, with their logs and out/."""
    config = load_config(a.profiles)
    if name not in config.profiles:
        raise NotFound(f"no run app {name} in runs.yaml (have: {', '.join(config.profiles) or 'none'})")
    if config.stage_profile == name:
        raise Conflict(f"{name} is runs.yaml's stage_profile (model staging runs in it): change that first")
    going = [s.id for s in list_runs(a, 1000, active=True, profile=name)]
    if going:
        raise Conflict(f"{name} has {len(going)} run(s) queued or running ({', '.join(going[:5])}): "
                       "delete it when they are done, or cancel them")
    _edit_profiles(a, lambda t: _without_profile(t, name),
                   lambda b: {**b, "profiles": {k: v for k, v in b["profiles"].items() if k != name}},
                   f"removing {name}")
    kept = len(list_runs(a, 1 << 30, profile=name))
    print(f"run app {name} deleted ({kept} past runs kept)", flush=True)
    return AppDeleted(app=name, profiles=a.profiles, runs_kept=kept)


def stage_model(a: RunArgs, config: RunsConfig) -> Submitted:
    """Downloads a Hugging Face repo into the shared, read-only model store, as a queued
    CPU run (logged, cancellable like any other) with the fleet's token. Only when a
    person asks for that model."""
    store, token_file, repo = config.model_store, config.hf_token_file, a.run_id or ""
    if not store or not os.path.isdir(store):
        raise SystemExit("runs.yaml has no model_store directory: set model_store (and hf_token_file for gated repos)")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repo):
        raise SystemExit(f"not a Hugging Face repo id: {repo!r}")
    dest = os.path.join(store, repo, a.revision)
    os.makedirs(dest, exist_ok=True)
    stager = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stage_model.py")
    base_name = config.stage_profile or a.profile or next(iter(config.profiles), None)
    if base_name is None or base_name not in config.profiles:
        raise SystemExit(f"stage-model: no profile to run in (have: {', '.join(config.profiles) or 'none'})")
    if config.profiles[base_name].disabled:
        raise Conflict(f"stage-model: run app {base_name} is disabled")
    token_dir = os.path.dirname(token_file) if token_file else None
    profile = replace(config.profiles[base_name], allow=[dest] + ([token_dir] if token_dir else []), default_gpu_mem_gb=0)
    staging = replace(a, profile="_stage", name=f"stage-{repo.replace('/', '-')}", gpus=0, cpus=2, gpu_mem=0,
                      mount=[f"{dest}:rw"] + ([f"{token_dir}:ro"] if token_dir else []),
                      env=[f"HF_TOKEN_FILE={token_file}"] if token_file else [], log=None, workdir=None, after=None,
                      retry=2, restart_on_oom=0, nodes=1)
    stage_cmd = ["python3", "{code}/stage_model.py", repo, a.revision, dest] + [f"--include={g}" for g in a.include]
    # the stager travels as the run's pinned code
    tgz = io.BytesIO()
    with tarfile.open(fileobj=tgz, mode="w:gz") as t:
        t.add(stager, arcname="stage_model.py")
    tmp = os.path.join(a.runs_dir, f".stage-{uuid.uuid4().hex[:8]}.tar.gz")
    with open(tmp, "wb") as f:
        f.write(tgz.getvalue())
    try:
        return submit(replace(staging, code=tmp), stage_cmd, RunsConfig(profiles={"_stage": profile}))
    finally:
        os.remove(tmp)


# ── CLI ──────────────────────────────────────────────────────────────────────

def _emit(value: Any, compact: bool = False) -> None:
    data = value.dump() if isinstance(value, Record) else [v.dump() for v in value]
    print(json.dumps(data, indent=None if compact else 2))


def main(argv: list[str]) -> None:
    if "--" in argv:
        i = argv.index("--")
        argv, cmd = argv[:i], argv[i + 1:]
    else:
        cmd = []
    a = from_args(RunArgs, argparser(RunArgs, argparse.ArgumentParser(prog="fleet_bridge run")).parse_args(argv))
    os.makedirs(a.runs_dir, exist_ok=True)
    config = load_config(a.profiles)
    actions: dict[str, Callable[[], None]] = {
        "render": lambda: _render(a, cmd, config),
        "list": lambda: _emit(list_runs(a)),
        "logs": lambda: logs(a, a.run_id or ""),
        "fetch": lambda: fetch(a, a.run_id or ""),
        "submit": lambda: _emit(submit(a, cmd, config), compact=True),
        "status": lambda: _emit(status(a, a.run_id or "")),
        "cancel": lambda: _emit(cancel(a, a.run_id or "")),
        "preempt": lambda: _emit(preempt(a.runs_dir, a.run_id or "", "by the operator")),
        "purge": lambda: _emit(purge(a, a.run_id)),
        "stage-model": lambda: _emit(stage_model(a, config), compact=True),
    }
    if a.action in ("list", "logs"):  # they read files; the cluster only says which runs are lost
        try:
            ray.init(address=a.address, namespace=NAMESPACE, log_to_driver=False, logging_level=logging.ERROR)
        except Exception:
            pass
    elif a.action not in ("render", "fetch"):
        ray.init(address=a.address, namespace=NAMESPACE, log_to_driver=False, logging_level=logging.ERROR)
    actions[a.action]()
    sys.stdout.flush()


def _render(a: RunArgs, cmd: list[str], config: RunsConfig) -> None:
    """The `docker run` a request would get, on the given host cards, without Ray."""
    if a.profile is None or a.profile not in config.profiles:
        raise SystemExit(f"unknown profile {a.profile!r} (have: {', '.join(config.profiles) or 'none'})")
    out = os.path.join(a.runs_dir, "<id>", OUT_DIR)
    spec = RunSpec(id=a.name or "render", profile_name=a.profile, profile=config.profiles[a.profile], command=cmd,
                   gpus=a.gpus, cpus=a.cpus, out=out, workdir=a.workdir, mounts=[Mount.parse(m) for m in a.mount],
                   env=a.env_map)
    print(shlex.join(docker_command(spec, a.host_gpus.split(",") if a.gpus else [])))
