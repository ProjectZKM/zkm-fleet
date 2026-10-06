"""One-shot jobs ("runs"): a command in a container, on cards Ray grants, until it exits.

    run.sh submit --profile NAME [--name N] [--gpus N] [--cpus N] [--gpu-mem GiB]
                  [--vram-wait S] [--nodes N] [--machine HOST]... [--after ID] [--retry N]
                  [--restart-on-oom N] [--code git:REPO@REF | --code FILE.tar.gz]
                  [--mount PATH:ro|rw]... [--env K=V]... [--log FILE] [--workdir DIR]
                  -- CMD ARG...
    run.sh status|cancel|logs [-f] [-n N] [--rank K]|fetch|purge ID     run.sh list     run.sh purge --older-than DAYS
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

Several machines: RUNS_DIR must be shared storage mounted at the same path on
all of them (the run's actor writes its record where it lands; the CLI reads it
wherever it runs), and so must every --mount. The container lives on the run's
machine: cancel and the lost-run check reach it through a Ray task there. With
an unshared RUNS_DIR on a multi-machine cluster, runs stay on the submitting
machine.
"""

from __future__ import annotations

import argparse
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
from .model import (Attempt, Bundle, CardState, Cause, CodeRecord, GangEnv, Holder, Mount, MountMode, Profile,
                    RankInfo, Record, RunArgs, RunsConfig, RunSpec, RunState, RunStatus, argparser, from_args,
                    from_dict, to_env)

NAMESPACE = "zkm-fleet"
NETWORK_FS = ("nfs", "cifs", "smb2", "smb3", "ceph", "lustre", "gpfs", "beegfs", "glusterfs", "fuse")
GPU_OOM = re.compile(r"CUDA out of memory|OutOfMemoryError|CUBLAS_STATUS_ALLOC_FAILED|cudaErrorMemoryAllocation"
                     r"|CUDA error: out of memory", re.I)
SPEC_FILE, STATUS_FILE, OUT_DIR, CODE_DIR, LOG_FILE, METRICS_FILE = "spec.json", "status.json", "out", "code", "log", "metrics.jsonl"


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

class RunDir:
    """A run's record directory and the files in it."""

    def __init__(self, runs_dir: str, run_id: str, *, must_exist: bool = True) -> None:
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", run_id or ""):
            raise SystemExit(f"not a run id: {run_id!r}")
        self.id = run_id
        self.path = os.path.join(runs_dir, run_id)
        if must_exist and not os.path.isdir(self.path):
            raise SystemExit(f"no run {run_id}")

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

    def update(self, **changes: Any) -> RunStatus:
        """Merges typed changes into status.json. A cancel wins over whatever the actor
        reports afterwards."""
        st = self.status()
        new_state = changes.get("state")
        if st.state is RunState.CANCELLED and new_state not in (None, RunState.CANCELLED):
            return st
        st = st.merged(**changes)
        st.write(self.status_path)
        return st

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


def container_name(run_id: str, rank: int = 0) -> str:
    return f"fleet-run-{run_id}" + (f"-r{rank}" if rank else "")


def actor_name(run_id: str, rank: int = 0) -> str:
    return f"run-{run_id}" + (f"-r{rank}" if rank else "")


def waiter_name(run_id: str) -> str:
    return f"wait-{run_id}"


def pg_name(run_id: str) -> str:
    return f"pg-run-{run_id}"


def docker_command(spec: RunSpec, gpus: list[str], attempt: int = 1, rank: int = 0,
                   gang: GangEnv | None = None) -> list[str]:
    """The `docker run` for a run (or one rank of it) on the given host cards."""
    p = spec.profile
    cmd = ["docker", "run", "-d", "--name", container_name(spec.id, rank), "--network", "host",
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
        self.dir.update(machine=info.machine, node_ip=info.node_ip)
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
            need = self.spec.need_mib
            self.infos = self._all("info")
            if self.n > 1:
                self.dir.update(ranks=self.infos)
            problems = [self._label(i) + p for i, p in enumerate(self._all("check", need)) if p]
            if problems:
                self.dir.update(state=RunState.FAILED, gpus=self.gpus, finished_at=time.time(), error="; ".join(problems))
                return
            retries, oom_restarts = self.spec.retry, self.spec.restart_on_oom
            attempts: list[Attempt] = []
            attempt = 0
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
                if self.dir.state() is RunState.CANCELLED:
                    return
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


def _on_run_machine(place: RankInfo, argv: list[str]) -> str | None:
    """Runs argv on the machine a rank landed on (its container is there) and returns
    its stdout; None when that machine has left the cluster."""
    if not place.node_ip:  # never landed (or a record from before node_ip): only this machine can have it
        return _sh(argv).stdout
    if not any(n.get("NodeManagerAddress") == place.node_ip for n in alive_nodes()):
        return None
    try:
        return str(ray.get(_on_node.options(resources={f"node:{place.node_ip}": 0.001}).remote(argv), timeout=60))
    except Exception:
        return None


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


def launch(spec: RunSpec, run_dir: str) -> None:
    b = bundle(spec)
    # one bundle per machine; several only on DIFFERENT machines
    pg = placement_group([bundle(spec, i).to_ray() for i in range(spec.nodes)],
                         strategy="STRICT_SPREAD" if spec.nodes > 1 else "STRICT_PACK",
                         name=pg_name(spec.id), lifetime="detached")
    GpuRun.options(
        name=actor_name(spec.id), namespace=NAMESPACE, lifetime="detached", num_cpus=b.cpu, num_gpus=b.gpu,
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


def _capture_code(source: str, code_dir: str) -> CodeRecord:
    """--code git:/repo@ref (git archive of that commit) or FILE.tar.gz, extracted into
    the run's code/ so the run executes exactly that and records what it was."""
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
        tarfile.open(fileobj=io.BytesIO(arch.stdout)).extractall(code_dir)
        dirty = _sh(["git", "-C", repo, "status", "--porcelain"]).stdout.strip()
        return CodeRecord(source=source, repo=repo, ref=ref, commit=sha,
                          note="working-tree changes are NOT included" if dirty else None)
    if not os.path.isfile(source):
        raise SystemExit(f"--code {source}: no such file (a tarball on this machine, or git:/repo@ref)")
    with open(source, "rb") as f:
        data = f.read()
    tarfile.open(fileobj=io.BytesIO(data)).extractall(code_dir)
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
    dir: str
    out: str
    note: str | None = None


def submit(a: RunArgs, cmd: list[str], config: RunsConfig) -> Submitted:
    if a.profile is None or a.profile not in config.profiles:
        raise SystemExit(f"unknown profile {a.profile!r} (have: {', '.join(config.profiles) or 'none'})")
    profile = config.profiles[a.profile]
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

    base = re.sub(r"[^A-Za-z0-9_.-]", "-", a.name or a.profile)[:40]
    run = RunDir(a.runs_dir, f"{base}-{uuid.uuid4().hex[:8]}", must_exist=False)
    os.makedirs(run.out)
    writable = [m.path for m in mounts if m.mode is MountMode.RW] + [run.out]
    if a.log and not _inside(a.log, writable):
        shutil.rmtree(run.path)
        raise SystemExit(f"--log {a.log} is outside the run's writable mounts")
    code = None
    if a.code:
        try:
            code = _capture_code(a.code, run.code)
        except BaseException:
            shutil.rmtree(run.path)
            raise
    gpu_mem = a.gpu_mem or profile.default_gpu_mem_gb
    spec = RunSpec(id=run.id, profile_name=a.profile, profile=profile, command=cmd, gpus=gpus, cpus=a.cpus,
                   mounts=mounts, env=a.env_map, log=a.log, workdir=a.workdir, out=run.out,
                   code_dir=run.code if code else None, code=code, gpu_mem_gb=gpu_mem, vram_wait_s=a.vram_wait,
                   retry=a.retry, restart_on_oom=a.restart_on_oom, after=a.after, nodes=place.nodes,
                   machines=place.machines, machine_resources=place.machine_resources)
    roots = [m.path for m in mounts] + [run.out] + ([run.code] if code else [])
    if a.workdir and not _inside(spec.expand(a.workdir), roots):
        shutil.rmtree(run.path)
        raise SystemExit(f"--workdir {a.workdir} is outside the run's mounts")
    spec.write(run.spec_path)
    state = RunState.WAITING_DEPENDENCY if a.after else RunState.QUEUED
    RunStatus(id=run.id, state=state, submitted_at=time.time(), gpus_requested=gpus, cpus=a.cpus,
              gpu_mem_gb=gpu_mem or None, after=a.after, code=code, nodes=place.nodes).write(run.status_path)
    if a.after:
        pin = place.machine_resources[0] if place.machine_resources else None
        RunWaiter.options(name=waiter_name(run.id), namespace=NAMESPACE, lifetime="detached", num_cpus=0,
                          max_restarts=0, resources={pin: 0.001} if pin else None).remote(spec, run.path, a.runs_dir)
    else:
        launch(spec, run.path)
    return Submitted(id=run.id, state=state, dir=run.path, out=run.out, note=place.note)


# ── other actions ────────────────────────────────────────────────────────────

def _alive(name: str) -> bool:
    try:
        ray.get_actor(name, namespace=NAMESPACE)
        return True
    except ValueError:
        return False


def _going(run_id: str) -> bool:
    return _alive(actor_name(run_id)) or _alive(waiter_name(run_id))


def status(a: RunArgs, run_id: str) -> RunStatus:
    run = RunDir(a.runs_dir, run_id)
    st = run.status()
    if not st.state.terminal and not _going(run_id):
        # The actor is gone without reporting (a Ray restart): say what the container does.
        st = st.merged(state=RunState.LOST, container=_container_states(st, run_id))
    return st.merged(metrics=run.last_metrics(), out=run.spec().out)


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
    names = [actor_name(run_id), waiter_name(run_id)] + [actor_name(run_id, i) for i in range(1, st.nodes)]
    for name in names:
        try:
            ray.kill(ray.get_actor(name, namespace=NAMESPACE), no_restart=True)
        except ValueError:
            pass
    gone = [r.machine for r in st.places()
            if _on_run_machine(r, ["docker", "rm", "-f", container_name(run_id, r.rank)]) is None]
    if gone:
        st = st.merged(container=f"not removed: {', '.join(gone)} left the cluster")
    try:
        remove_placement_group(ray.util.get_placement_group(pg_name(run_id)))
    except Exception:
        pass
    return st


def logs(a: RunArgs, run_id: str) -> None:
    """The run's output: the last -n lines, then with -f everything new until it ends."""
    run = RunDir(a.runs_dir, run_id)
    log_path = run.spec().rank_log_path(a.rank)
    for line in _tail(log_path, 1 << 20).splitlines()[-a.lines:]:
        print(line)
    if not a.follow:
        return
    pos = os.path.getsize(log_path) if os.path.exists(log_path) else 0
    while True:
        if os.path.exists(log_path) and os.path.getsize(log_path) > pos:
            with open(log_path, "rb") as f:
                f.seek(pos)
                chunk = f.read()
            pos += len(chunk)
            sys.stdout.write(chunk.decode(errors="replace"))
            sys.stdout.flush()
        elif (run.state() or RunState.LOST).terminal:
            return
        else:
            time.sleep(1)


def fetch(a: RunArgs, run_id: str) -> None:
    """out/ as a tar.gz on stdout: `ssh box run.sh fetch ID > out.tgz`."""
    run = RunDir(a.runs_dir, run_id)
    with tarfile.open(fileobj=sys.stdout.buffer, mode="w|gz") as t:
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
        targets = [RunDir(a.runs_dir, d) for d in os.listdir(a.runs_dir)
                   if os.path.isdir(os.path.join(a.runs_dir, d)) and os.path.getmtime(os.path.join(a.runs_dir, d)) < cut]
    else:
        raise SystemExit("purge ID, or purge --older-than DAYS")
    done = []
    for run in targets:
        state = run.state()
        if (state is None or not state.terminal) and _going(run.id):
            done.append(Purged(id=run.id, purged=False, reason=f"still {state.value if state else '?'}: cancel it first"))
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
    gpus: list[str] = field(default_factory=list)
    exit_code: int | None = None
    cause: Cause | None = None
    after: str | None = None
    submitted_at: float | None = None
    started_at: float | None = None
    finished_at: float | None = None


def list_runs(a: RunArgs, n: int = 20) -> list[RunSummary]:
    if not os.path.isdir(a.runs_dir):
        return []
    ids = sorted(os.listdir(a.runs_dir), key=lambda d: os.path.getmtime(os.path.join(a.runs_dir, d)), reverse=True)
    out = []
    for i in ids[:n]:
        try:
            out.append(from_dict(RunSummary, RunDir(a.runs_dir, i).status().dump()))
        except (SystemExit, OSError, ValueError):
            pass
    return out


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
        "purge": lambda: _emit(purge(a, a.run_id)),
        "stage-model": lambda: _emit(stage_model(a, config), compact=True),
    }
    if a.action not in ("render", "list", "logs", "fetch"):
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
