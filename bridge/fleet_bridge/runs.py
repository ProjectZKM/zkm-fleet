"""One-shot jobs ("runs"): a command in a container, on cards Ray grants, until it exits.

    run.sh submit --profile NAME [--name N] [--gpus N] [--cpus N] [--gpu-mem GiB]
                  [--vram-wait S] [--nodes N] [--machine HOST]... [--after ID] [--retry N]
                  [--restart-on-oom N] [--code git:REPO@REF | --code FILE.tar.gz]
                  [--mount PATH:ro|rw]... [--env K=V]... [--log FILE] [--workdir DIR]
                  -- CMD ARG...
    run.sh status|cancel|logs [-f] [-n N] [--rank K]|fetch|purge ID     run.sh list     run.sh purge --older-than DAYS
    run.sh stage-model REPO [--revision R] [--include GLOB]...

A run's record is RUNS_DIR/<id>/: spec.json, status.json, log (its output),
out/ (mounted read-write into the job: FLEET_OUT, `{out}`; metrics.jsonl there is
what `status` reports as metrics) and, with --code, code/ (read-only: FLEET_CODE,
`{code}`). `fetch` streams out/ as a tar.gz; `purge` deletes the whole record.

Every other path a job touches is in its request (`--mount PATH:ro|rw`, own path,
existing, under the profile's `allow` roots). The profile (runs.yaml) fixes the
image, the user, a GPU cap, base env and default_gpu_mem_gb.

Placement: one STRICT_PACK bundle per run, so its cards and CPUs come from one
machine (--machine pins which). --nodes N spreads a run over N machines (STRICT_SPREAD:
one bundle of --gpus cards and --cpus CPUs on each of N different machines, --machine
repeated N times pins them): one container per machine, started together, all
ended when one fails; every container sees FLEET_NNODES / FLEET_NODE_RANK /
FLEET_MASTER_ADDR / FLEET_MASTER_PORT (and MASTER_ADDR, MASTER_PORT, NODE_RANK,
NNODES and torchrun's PET_* so `torchrun train.py` joins the gang as is), rank
k's output goes to log.rank<k>. `--gpus 0.5` asks for half a card: runs whose
fractions fit share it, each started only if its --gpu-mem is free then (the
check is at start; size --gpu-mem for the job's peak). While none can take it the run is queued. Ray's
grant is logical, so before starting the actor checks the cards' real free memory
against --gpu-mem (default: the profile's, else the whole card) and waits as
waiting_vram (naming who holds them) up to --vram-wait. --after waits for another
run in a CPU-free waiter that holds no card, then launches only if it succeeded.

A failed attempt gets a cause (gpu-oom from the log, host-oom from docker,
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

import ray
from ray.util.placement_group import placement_group, remove_placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

NAMESPACE = "zkm-fleet"
TERMINAL = ("succeeded", "failed", "cancelled")
NETWORK_FS = ("nfs", "cifs", "smb2", "smb3", "ceph", "lustre", "gpfs", "beegfs", "glusterfs", "fuse")
GPU_OOM = re.compile(r"CUDA out of memory|OutOfMemoryError|CUBLAS_STATUS_ALLOC_FAILED|cudaErrorMemoryAllocation"
                     r"|CUDA error: out of memory", re.I)


# ── the cards as they really are ─────────────────────────────────────────────

def card_memory(cards: list[str]) -> dict[str, dict]:
    """Physical state of host cards: free / total MiB and the processes holding memory
    (pid, MiB, command). Ray's grant is logical; this is what is really on the card."""
    if not cards:
        return {}
    q = subprocess.run(["nvidia-smi", "--query-gpu=index,uuid,memory.free,memory.total",
                        "--format=csv,noheader,nounits", "-i", ",".join(cards)], capture_output=True, text=True)
    out, by_uuid = {}, {}
    for line in q.stdout.strip().splitlines():
        idx, uuid_, free, total = [x.strip() for x in line.split(",")]
        out[idx] = {"free_mib": int(free), "total_mib": int(total), "holders": []}
        by_uuid[uuid_] = idx
    apps = subprocess.run(["nvidia-smi", "--query-compute-apps=gpu_uuid,pid,used_memory",
                           "--format=csv,noheader,nounits"], capture_output=True, text=True)
    for line in apps.stdout.strip().splitlines():
        uuid_, pid, used = [x.strip() for x in line.split(",")]
        if uuid_ in by_uuid:
            try:
                with open(f"/proc/{pid}/cmdline", "rb") as f:
                    cmd = f.read().replace(b"\0", b" ").decode(errors="replace").strip()[:80]
            except OSError:
                cmd = "?"
            out[by_uuid[uuid_]]["holders"].append({"pid": int(pid), "mib": int(used), "cmd": cmd})
    return out


def _holders(cards: list[str]) -> str:
    return "; ".join(f"card {i}: " + (", ".join(f"pid {h['pid']} {h['mib']} MiB ({h['cmd']})" for h in c["holders"])
                                     or "no process") for i, c in card_memory(cards).items())


def vram_shortfall(cards: list[str], need_mib: int | None) -> list[str]:
    """Why the granted cards cannot take the job yet ([] when they can). Without a
    stated need the job wants the whole card (95% free)."""
    short = []
    for idx, c in card_memory(cards).items():
        need = need_mib if need_mib else int(c["total_mib"] * 0.95)
        if c["free_mib"] < need:
            who = ", ".join(f"pid {h['pid']} {h['mib']} MiB ({h['cmd']})" for h in c["holders"]) or "no listed process"
            short.append(f"card {idx}: {c['free_mib']} MiB free, needs {need} MiB; held by {who}")
    return short


# ── records ──────────────────────────────────────────────────────────────────

def _write_status(run_dir: str, **fields):
    path = os.path.join(run_dir, "status.json")
    try:
        with open(path) as f:
            status = json.load(f)
    except (OSError, ValueError):
        status = {}
    if status.get("state") == "cancelled" and fields.get("state") not in (None, "cancelled"):
        return status  # a cancel wins over whatever the actor reports afterwards
    status.update(fields)
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        json.dump(status, f, indent=2)
    os.replace(tmp, path)
    return status


def _read_status(run_dir: str) -> dict:
    with open(os.path.join(run_dir, "status.json")) as f:
        return json.load(f)


def _run_dir(runs_dir: str, run_id: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", run_id or ""):
        raise SystemExit(f"not a run id: {run_id!r}")
    d = os.path.join(runs_dir, run_id)
    if not os.path.isdir(d):
        raise SystemExit(f"no run {run_id}")
    return d


def container_name(run_id: str, rank: int = 0) -> str:
    return f"fleet-run-{run_id}" + (f"-r{rank}" if rank else "")


def _expand(text: str, spec: dict) -> str:
    return text.replace("{out}", spec["out"]).replace("{code}", spec.get("code_dir") or "{code}")


def docker_command(spec: dict, gpus: list[str], attempt: int = 1, rank: int = 0, gang: dict | None = None) -> list[str]:
    """The `docker run` for a run (or one rank of it) on the given host cards."""
    p = spec["profile"]
    cmd = ["docker", "run", "-d", "--name", container_name(spec["id"], rank), "--network", "host",
           f"--shm-size={p.get('shm_gb', 16)}g", "--label", "zkm-fleet=run", "--label", f"zkm-fleet.run={spec['id']}"]
    if gpus:
        cmd += ["--runtime=nvidia", "--gpus", f'"device={",".join(gpus)}"', "-e", "NVIDIA_DRIVER_CAPABILITIES=all"]
    if spec.get("cpus"):
        cmd += [f"--cpus={spec['cpus']}", "-e", f"OMP_NUM_THREADS={spec['cpus']}"]
    if p.get("user"):
        cmd += ["--user", str(p["user"])]
    if spec.get("workdir"):
        cmd += ["-w", _expand(spec["workdir"], spec)]
    for path, mode in spec.get("mounts", []):
        cmd += ["-v", f"{path}:{path}" + (":ro" if mode == "ro" else "")]
    cmd += ["-v", f"{spec['out']}:{spec['out']}"]
    if spec.get("code_dir"):
        cmd += ["-v", f"{spec['code_dir']}:{spec['code_dir']}:ro"]
    env = {"PYTHONUNBUFFERED": "1", **p.get("env", {}), **spec.get("env", {}),
           "FLEET_RUN_ID": spec["id"], "FLEET_OUT": spec["out"], "FLEET_METRICS": os.path.join(spec["out"], "metrics.jsonl"),
           "FLEET_ATTEMPT": str(attempt), **(gang or {})}
    if spec.get("code_dir"):
        env["FLEET_CODE"] = spec["code_dir"]
    for k, v in env.items():
        cmd += ["-e", f"{k}={_expand(str(v), spec)}"]
    cmd.append(p["image"])
    cmd += [_expand(a, spec) for a in spec["command"]]
    return cmd


def parse_mount(text: str) -> tuple[str, str]:
    """`PATH` or `PATH:ro` / `PATH:rw` -> (normalised absolute path, mode)."""
    path, _, mode = text.partition(":")
    mode = mode or "rw"
    if mode not in ("ro", "rw"):
        raise SystemExit(f"--mount {text}: mode must be ro or rw")
    if not path.startswith("/") or ".." in path.split("/"):
        raise SystemExit(f"--mount {text}: needs an absolute path without '..'")
    return os.path.normpath(path), mode


def missing_mounts(spec: dict) -> list[str]:
    """Mounts that do not exist where the run landed; the run fails instead of docker
    creating them, owned by root, in someone's tree."""
    return [path for path, _ in spec.get("mounts", []) if not os.path.exists(path)]


def _tail(path: str, nbytes: int = 65536) -> str:
    try:
        with open(path, "rb") as f:
            f.seek(0, 2)
            f.seek(max(0, f.tell() - nbytes))
            return f.read().decode(errors="replace")
    except OSError:
        return ""


def _last_metrics(spec: dict) -> dict | None:
    for line in reversed(_tail(os.path.join(spec["out"], "metrics.jsonl"), 16384).splitlines()):
        try:
            return json.loads(line)
        except ValueError:
            continue
    return None


# ── the actors ───────────────────────────────────────────────────────────────

class _Rank:
    """One machine's share of a run: the cards Ray granted there and one container.
    Rank 0 lives inside the run's actor; on a run over several machines (--nodes)
    every other rank is a RankActor on its own machine."""

    def __init__(self, spec: dict, run_dir: str, rank: int = 0):
        self.spec, self.run_dir, self.rank = spec, run_dir, rank
        self.gpus = [str(int(g)) for g in ray.get_gpu_ids()]
        self.name = container_name(spec["id"], rank)
        base = spec.get("log") or os.path.join(run_dir, "log")
        self.log_path = base if rank == 0 else f"{base}.rank{rank}"
        self.follower, self.log, self.offset = None, None, 0

    def info(self) -> dict:
        return {"rank": self.rank, "machine": socket.gethostname(), "node_ip": ray.util.get_node_ip_address(),
                "gpus": self.gpus, "container": self.name}

    def check(self, need: int | None) -> str | None:
        """Why this machine cannot run the job at all (None when it can)."""
        missing = missing_mounts(self.spec)
        if missing:
            return f"mounts missing on {socket.gethostname()}: {missing}"
        too_big = [f"card {i}: {c['total_mib']} MiB in all, the job asks {need} MiB"
                   for i, c in card_memory(self.gpus).items() if need and need > c["total_mib"]]
        return "--gpu-mem exceeds the card: " + "; ".join(too_big) if too_big else None

    def shortfall(self, need: int | None) -> list[str]:
        return vram_shortfall(self.gpus, need) if self.gpus else []

    def start(self, attempt: int, gang: dict) -> str | None:
        """Starts the container; returns docker's error, or None."""
        subprocess.run(["docker", "rm", "-f", self.name], capture_output=True)
        env = dict(gang)
        if gang:
            env.update(FLEET_NODE_RANK=self.rank, NODE_RANK=self.rank, PET_NODE_RANK=self.rank)
        out = subprocess.run(docker_command(self.spec, self.gpus, attempt, self.rank, env), capture_output=True, text=True)
        if out.returncode != 0:
            return out.stderr.strip()[:500]
        self.offset = os.path.getsize(self.log_path) if os.path.exists(self.log_path) else 0
        self.log = open(self.log_path, "ab")
        self.follower = subprocess.Popen(["docker", "logs", "-f", self.name], stdout=self.log, stderr=subprocess.STDOUT)
        return None

    def wait(self) -> dict:
        """Waits for the container to end; returns {exit_code, cause, ...}."""
        waited = subprocess.run(["docker", "wait", self.name], capture_output=True, text=True)
        time.sleep(1)  # let the follower drain the last lines
        if self.follower:
            self.follower.terminate()
            self.log.close()
        code = int(waited.stdout.strip()) if waited.stdout.strip().lstrip("-").isdigit() else None
        result = {"exit_code": code}
        if code != 0:
            insp = subprocess.run(["docker", "inspect", "-f", "{{.State.OOMKilled}}", self.name],
                                  capture_output=True, text=True)
            with open(self.log_path, "rb") as f:
                f.seek(self.offset)
                this_attempt = f.read()[-65536:].decode(errors="replace")
            m = GPU_OOM.search(this_attempt)
            if m:
                result.update(cause="gpu-oom", evidence=m.group(0), card_holders=_holders(self.gpus))
            elif insp.stdout.strip() == "true":
                result.update(cause="host-oom", evidence="docker: OOMKilled")
            else:
                result.update(cause="exit-code")
        subprocess.run(["docker", "rm", "-f", self.name], capture_output=True)
        return result

    def kill(self):
        subprocess.run(["docker", "rm", "-f", self.name], capture_output=True)


# kill() must reach a rank while it blocks in wait()
RankActor = ray.remote(max_restarts=0, max_task_retries=0, max_concurrency=4)(_Rank)


@ray.remote(max_restarts=0, max_task_retries=0)
class GpuRun:
    """Runs one job to completion (with its retries). Not restarted by Ray: a run that
    dies is reported, not silently repeated. With --nodes N it is rank 0 and starts
    one RankActor per other bundle of its placement group (one per machine); every
    attempt starts all ranks together, and the first rank to fail ends the others."""

    def __init__(self, spec: dict, run_dir: str):
        self.spec, self.run_dir = spec, run_dir
        self.ranks = [_Rank(spec, run_dir, 0)]
        self.gpus = self.ranks[0].gpus
        if int(spec.get("nodes", 1)) > 1:
            pg = ray.util.get_current_placement_group()
            for i in range(1, int(spec["nodes"])):
                b = _bundle(spec, i)
                self.ranks.append(RankActor.options(
                    name=f"run-{spec['id']}-r{i}", namespace=NAMESPACE, num_cpus=b["CPU"], num_gpus=b.get("GPU", 0),
                    scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg, placement_group_bundle_index=i),
                ).remote(spec, run_dir, i))
        # where the containers will live: cancel and status reach them there
        info = self.ranks[0].info()
        _write_status(run_dir, machine=info["machine"], node_ip=info["node_ip"])
        threading.Thread(target=self._run, daemon=True).start()

    def ping(self) -> dict:
        return {"id": self.spec["id"], "gpus": self.gpus, "machine": socket.gethostname()}

    def _all(self, method: str, *args) -> list:
        """Calls a method on every rank (the others in parallel), results in rank order."""
        refs = [getattr(r, method).remote(*args) for r in self.ranks[1:]]
        return [getattr(self.ranks[0], method)(*args)] + ray.get(refs, timeout=300)

    def _label(self, i: int) -> str:
        return f"rank {i} ({self.infos[i]['machine']}): " if len(self.ranks) > 1 else ""

    def _wait_vram(self, need: int | None) -> bool:
        deadline = time.time() + int(self.spec.get("vram_wait_s", 1800))
        while True:
            short = [self._label(i) + s for i, ss in enumerate(self._all("shortfall", need)) for s in ss]
            if not short:
                return True
            if time.time() > deadline:
                _write_status(self.run_dir, state="failed", gpus=self.gpus, finished_at=time.time(),
                              error="cards not free in time: " + "; ".join(short))
                return False
            _write_status(self.run_dir, state="waiting_vram", gpus=self.gpus, detail="; ".join(short))
            time.sleep(15)

    def _gang_env(self) -> dict:
        """Where the ranks meet: rank 0's machine and a port free there. Also as torchrun's
        PET_* variables, so a plain `torchrun train.py` joins the gang."""
        if len(self.ranks) == 1:
            return {}
        with socket.socket() as sock:
            sock.bind(("", 0))
            port = sock.getsockname()[1]
        n, addr, per = len(self.ranks), self.infos[0]["node_ip"], max(len(self.gpus), 1)
        return {"FLEET_NNODES": n, "FLEET_MASTER_ADDR": addr, "FLEET_MASTER_PORT": port,
                "MASTER_ADDR": addr, "MASTER_PORT": port, "NNODES": n,
                "PET_NNODES": n, "PET_MASTER_ADDR": addr, "PET_MASTER_PORT": port, "PET_NPROC_PER_NODE": per}

    def _attempt(self, attempt: int) -> dict:
        """One start of every rank; returns {exit_code, cause, ...} of the first that failed."""
        n = len(self.ranks)
        errors = self._all("start", attempt, self._gang_env())
        bad = [(i, e) for i, e in enumerate(errors) if e]
        if bad:
            self._kill_all()
            return {"exit_code": None, "cause": "docker-run", "error": "; ".join(self._label(i) + e for i, e in bad)}
        _write_status(self.run_dir, state="running", gpus=self.gpus, container=self.ranks[0].name,
                      attempt=attempt, started_at=time.time(), detail=None)
        done: queue.Queue = queue.Queue()

        def wait(i: int):
            try:
                res = self.ranks[0].wait() if i == 0 else ray.get(self.ranks[i].wait.remote())
            except Exception as e:  # its machine or actor died
                res = {"exit_code": None, "cause": "rank-lost", "error": repr(e)[:300]}
            done.put((i, res))

        for i in range(n):
            threading.Thread(target=wait, args=(i,), daemon=True).start()
        failed = None
        for _ in range(n):
            i, res = done.get()
            if res.get("exit_code") != 0 and failed is None:
                failed = (i, res)
                if n > 1:
                    self._kill_all()  # a gang without one rank only hangs
        if failed is None:
            return {"exit_code": 0}
        i, res = failed
        return {**res, "rank": i, "machine": self.infos[i]["machine"]} if n > 1 else res

    def _kill_all(self):
        self.ranks[0].kill()
        for r in self.ranks[1:]:
            r.kill.remote()

    def _run(self):
        code = None
        try:
            need = int(self.spec.get("gpu_mem_gb") or 0) * 1024 or None
            self.infos = self._all("info")
            if len(self.ranks) > 1:
                _write_status(self.run_dir, ranks=self.infos)
            problems = [self._label(i) + p for i, p in enumerate(self._all("check", need)) if p]
            if problems:
                _write_status(self.run_dir, state="failed", gpus=self.gpus, finished_at=time.time(),
                              error="; ".join(problems))
                return
            retries, oom_restarts, attempts = int(self.spec.get("retry", 0)), int(self.spec.get("restart_on_oom", 0)), []
            attempt = 0
            while True:
                attempt += 1
                if not self._wait_vram(need):
                    return
                result = self._attempt(attempt)
                attempts.append(result)
                code = result.get("exit_code")
                if code == 0:
                    _write_status(self.run_dir, state="succeeded", exit_code=0, attempts=attempts, finished_at=time.time())
                    return
                if _read_status(self.run_dir).get("state") == "cancelled":
                    return
                oom = result.get("cause") in ("gpu-oom", "host-oom")
                if oom and oom_restarts > 0:
                    oom_restarts -= 1
                elif retries > 0:
                    retries -= 1
                else:
                    _write_status(self.run_dir, state="failed", exit_code=code, cause=result.get("cause"),
                                  error=result.get("error") or result.get("evidence"), attempts=attempts,
                                  finished_at=time.time())
                    return
                _write_status(self.run_dir, state="retrying", attempts=attempts,
                              detail=f"attempt {attempt} ended {result.get('cause')} (exit {code}); trying again")
        except Exception as e:  # report, never hang the cards
            _write_status(self.run_dir, state="failed", error=repr(e), finished_at=time.time())
        finally:
            try:
                self._kill_all()
                time.sleep(2)  # let the other ranks' kills land before their actors go with the group
                remove_placement_group(ray.util.get_placement_group(f"pg-run-{self.spec['id']}"))
            finally:
                os._exit(0 if code == 0 else 1)


@ray.remote(num_cpus=0, max_restarts=0)
class RunWaiter:
    """Holds a run until the run it depends on ends, without holding any card (a run
    waiting on another with the only free card would otherwise deadlock), then
    launches it if that run succeeded."""

    def __init__(self, spec: dict, run_dir: str, runs_dir: str):
        self.spec, self.run_dir, self.runs_dir = spec, run_dir, runs_dir
        threading.Thread(target=self._wait, daemon=True).start()

    def _wait(self):
        dep = self.spec["after"]
        try:
            while True:
                if _read_status(self.run_dir).get("state") == "cancelled":
                    return
                try:
                    st = _read_status(os.path.join(self.runs_dir, dep)).get("state")
                except (OSError, ValueError):
                    st = "missing"
                if st == "succeeded":
                    _write_status(self.run_dir, state="queued", detail=None)
                    _launch(self.spec, self.run_dir)
                    return
                if st in ("failed", "cancelled", "missing", "lost"):
                    _write_status(self.run_dir, state="failed", finished_at=time.time(),
                                  error=f"dependency {dep} ended {st}; not started")
                    return
                time.sleep(10)
        finally:
            os._exit(0)


@ray.remote(num_cpus=0, max_retries=0)
def _on_node(argv: list[str]) -> str:
    return subprocess.run(argv, capture_output=True, text=True).stdout


def _on_run_machine(st: dict, argv: list[str]) -> str | None:
    """Runs argv on the machine the run landed on (its container is there) and returns
    its stdout; None when that machine has left the cluster."""
    ip = st.get("node_ip")
    if not ip:  # never landed (or a record from before node_ip): only this machine can have it
        return subprocess.run(argv, capture_output=True, text=True).stdout
    if not any(n.get("Alive") and n.get("NodeManagerAddress") == ip for n in ray.nodes()):
        return None
    try:
        return ray.get(_on_node.options(resources={f"node:{ip}": 0.001}).remote(argv), timeout=60)
    except Exception:
        return None


def _shared_storage(path: str) -> bool:
    """Whether every machine sees path (a network file system; FLEET_RUNS_SHARED=1 says
    so for one the type check does not know, e.g. a bind mount of one)."""
    if os.environ.get("FLEET_RUNS_SHARED") == "1":
        return True
    fs = subprocess.run(["stat", "-f", "-c", "%T", path], capture_output=True, text=True).stdout.strip()
    return fs.startswith(NETWORK_FS)


def _bundle(spec: dict, rank: int = 0) -> dict:
    """One machine's share: --gpus cards and --cpus CPUs (per machine with --nodes)."""
    b = {"CPU": float(max(spec["cpus"], 1))}
    if spec["gpus"]:
        b["GPU"] = float(spec["gpus"])
    pins = spec.get("machine_resources") or ([spec["machine_resource"]] if spec.get("machine_resource") else [])
    if pins:
        b[pins[rank]] = 0.001
    return b


def _launch(spec: dict, run_dir: str):
    n = int(spec.get("nodes", 1))
    b = _bundle(spec)
    # one bundle per machine; several only on DIFFERENT machines
    pg = placement_group([_bundle(spec, i) for i in range(n)], strategy="STRICT_SPREAD" if n > 1 else "STRICT_PACK",
                         name=f"pg-run-{spec['id']}", lifetime="detached")
    GpuRun.options(
        name=f"run-{spec['id']}", namespace=NAMESPACE, lifetime="detached",
        num_cpus=b["CPU"], num_gpus=b.get("GPU", 0),
        scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg, placement_group_bundle_index=0),
    ).remote(spec, run_dir)


# ── submit ───────────────────────────────────────────────────────────────────

def _load_config(path: str) -> dict:
    import yaml

    with open(path) as f:
        return yaml.safe_load(f) or {}


def _inside(path: str, roots: list[str]) -> bool:
    path = os.path.normpath(path)
    return any(path == r.rstrip("/") or path.startswith(r.rstrip("/") + "/") for r in roots)


def _machine_resource(machine: str) -> str:
    """Ray's per-node resource for a machine given by host name or IP."""
    for n in ray.nodes():
        if n.get("Alive") and machine in (n.get("NodeManagerHostname"), n.get("NodeManagerAddress")):
            return f"node:{n['NodeManagerAddress']}"
    alive = [f"{n.get('NodeManagerHostname')} ({n.get('NodeManagerAddress')})" for n in ray.nodes() if n.get("Alive")]
    raise SystemExit(f"--machine {machine}: no live Ray node of that name or address (have: {', '.join(alive)})")


def _capture_code(source: str, code_dir: str) -> dict:
    """--code git:/repo@ref (git archive of that commit) or FILE.tar.gz, extracted into
    the run's code/ so the run executes exactly that and records what it was."""
    os.makedirs(code_dir)
    if source.startswith("git:"):
        repo, _, ref = source[4:].rpartition("@")
        if not repo:
            repo, ref = source[4:], "HEAD"
        commit = subprocess.run(["git", "-C", repo, "rev-parse", "--verify", f"{ref}^{{commit}}"],
                                capture_output=True, text=True)
        if commit.returncode != 0:
            raise SystemExit(f"--code {source}: {commit.stderr.strip()}")
        sha = commit.stdout.strip()
        arch = subprocess.run(["git", "-C", repo, "archive", "--format=tar", sha], capture_output=True)
        tarfile.open(fileobj=io.BytesIO(arch.stdout)).extractall(code_dir)
        dirty = subprocess.run(["git", "-C", repo, "status", "--porcelain"], capture_output=True, text=True).stdout.strip()
        return {"source": source, "repo": repo, "ref": ref, "commit": sha,
                "note": "working-tree changes are NOT included" if dirty else None}
    if not os.path.isfile(source):
        raise SystemExit(f"--code {source}: no such file (a tarball on this machine, or git:/repo@ref)")
    data = open(source, "rb").read()
    tarfile.open(fileobj=io.BytesIO(data)).extractall(code_dir)
    return {"source": source, "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}


def submit(a, cmd: list[str], config: dict) -> dict:
    profiles = config.get("profiles", {})
    if a.profile not in profiles:
        raise SystemExit(f"unknown profile {a.profile!r} (have: {', '.join(profiles) or 'none'})")
    profile = profiles[a.profile]
    if not cmd:
        raise SystemExit("no command after --")
    max_gpus = int(profile.get("max_gpus", 8))
    if a.gpus > max_gpus:
        raise SystemExit(f"profile {a.profile} allows at most {max_gpus} GPUs")
    if a.gpus > 1 and a.gpus != int(a.gpus):
        raise SystemExit("--gpus: whole cards, or one fraction of a card (0 < gpus < 1)")
    a.gpus = int(a.gpus) if a.gpus == int(a.gpus) else a.gpus
    if 0 < a.gpus < 1 and not (a.gpu_mem or profile.get("default_gpu_mem_gb")):
        raise SystemExit("a fractional --gpus shares the card with other runs: give --gpu-mem (the job's peak GiB)")
    mounts = [parse_mount(m) for m in a.mount]
    allow = profile.get("allow", [])
    for path, _ in mounts:
        if not _inside(path, allow):
            raise SystemExit(f"--mount {path} is outside what profile {a.profile} allows ({', '.join(allow) or 'nothing'})")
        if not os.path.exists(path):
            raise SystemExit(f"--mount {path} does not exist (create it before submitting)")
    env = dict(e.split("=", 1) for e in a.env if "=" in e)
    if a.after:
        _run_dir(a.runs_dir, a.after)
    note = None
    nodes = int(a.nodes)
    alive = len([n for n in ray.nodes() if n.get("Alive")])
    if nodes < 1:
        raise SystemExit("--nodes: at least 1")
    if nodes > alive:
        raise SystemExit(f"--nodes {nodes}: the cluster has {alive} machine(s)")
    if a.machine and len(a.machine) not in (0, nodes):
        raise SystemExit(f"--machine: give one per node ({nodes}) or none")
    if alive > 1 and not _shared_storage(a.runs_dir):
        here = ray.util.get_node_ip_address()
        if nodes > 1:
            raise SystemExit(f"--nodes {nodes}: the runs dir {a.runs_dir} is not shared storage")
        if a.machine and _machine_resource(a.machine[0]) != f"node:{here}":
            raise SystemExit(f"--machine {a.machine[0]}: the runs dir {a.runs_dir} is not shared storage, "
                             "so a run can only land on this machine")
        a.machine, note = [here], f"runs dir {a.runs_dir} is not shared storage: the run stays on this machine"
    machine_resources = [_machine_resource(m) for m in a.machine] if a.machine else []
    if len(set(machine_resources)) != len(machine_resources):
        raise SystemExit("--machine: the nodes of one run are different machines")
    machine_resource = machine_resources[0] if machine_resources else None

    base = re.sub(r"[^A-Za-z0-9_.-]", "-", a.name or a.profile)[:40]
    run_id = f"{base}-{uuid.uuid4().hex[:8]}"
    run_dir = os.path.join(a.runs_dir, run_id)
    out = os.path.join(run_dir, "out")
    os.makedirs(out)
    if a.log and not (_inside(a.log, [p for p, mode in mounts if mode == "rw"]) or _inside(a.log, [out])):
        shutil.rmtree(run_dir)
        raise SystemExit(f"--log {a.log} is outside the run's writable mounts")
    code = None
    if a.code:
        try:
            code = _capture_code(a.code, os.path.join(run_dir, "code"))
        except BaseException:
            shutil.rmtree(run_dir)
            raise
    gpu_mem = a.gpu_mem or int(profile.get("default_gpu_mem_gb", 0) or 0)
    spec = {"id": run_id, "profile_name": a.profile, "profile": profile, "command": cmd, "gpus": a.gpus,
            "cpus": a.cpus, "mounts": mounts, "env": env, "log": a.log, "workdir": a.workdir, "out": out,
            "code_dir": os.path.join(run_dir, "code") if code else None, "code": code,
            "gpu_mem_gb": gpu_mem, "vram_wait_s": a.vram_wait, "retry": a.retry, "restart_on_oom": a.restart_on_oom,
            "after": a.after, "machine": a.machine[0] if a.machine else None, "machine_resource": machine_resource,
            "nodes": nodes, "machines": a.machine or None, "machine_resources": machine_resources or None}
    if a.workdir and not _inside(_expand(a.workdir, spec), [p for p, _ in mounts] + [out] + ([spec["code_dir"]] if code else [])):
        shutil.rmtree(run_dir)
        raise SystemExit(f"--workdir {a.workdir} is outside the run's mounts")
    with open(os.path.join(run_dir, "spec.json"), "w") as f:
        json.dump(spec, f, indent=2)
    _write_status(run_dir, id=run_id, state="waiting_dependency" if a.after else "queued", submitted_at=time.time(),
                  gpus_requested=a.gpus, cpus=a.cpus, gpu_mem_gb=gpu_mem or "whole card", after=a.after, code=code,
                  **({"nodes": nodes} if nodes > 1 else {}))
    if a.after:
        RunWaiter.options(name=f"wait-{run_id}", namespace=NAMESPACE, lifetime="detached",
                          resources={machine_resource: 0.001} if machine_resource else None,
                          ).remote(spec, run_dir, a.runs_dir)
    else:
        _launch(spec, run_dir)
    res = {"id": run_id, "state": "waiting_dependency" if a.after else "queued", "dir": run_dir, "out": out}
    if note:
        res["note"] = note
    return res


# ── other actions ────────────────────────────────────────────────────────────

def _alive(name: str) -> bool:
    try:
        ray.get_actor(name, namespace=NAMESPACE)
        return True
    except ValueError:
        return False


def status(a, run_id: str) -> dict:
    run_dir = _run_dir(a.runs_dir, run_id)
    st = _read_status(run_dir)
    if st.get("state") not in TERMINAL and not _alive(f"run-{run_id}") and not _alive(f"wait-{run_id}"):
        # The actor is gone without reporting (a Ray restart): say what the container does.
        st["state"] = "lost"
        st["container"] = _container_states(st, run_id)
    with open(os.path.join(run_dir, "spec.json")) as f:
        spec = json.load(f)
    metrics = _last_metrics(spec)
    if metrics is not None:
        st["metrics"] = metrics
    st["out"] = spec["out"]
    return st


def _rank_places(st: dict, run_id: str) -> list[tuple[dict, str]]:
    """(where, container name) for every rank of the run."""
    ranks = st.get("ranks") or [{"rank": 0, "node_ip": st.get("node_ip"), "machine": st.get("machine")}]
    return [(r, container_name(run_id, r["rank"])) for r in ranks]


def _container_states(st: dict, run_id: str) -> str:
    states = []
    for r, name in _rank_places(st, run_id):
        out = _on_run_machine(r, ["docker", "inspect", "-f", "{{.State.Status}}", name])
        state = (out.strip() or "gone") if out is not None else f"unknown ({r.get('machine')} left the cluster)"
        states.append(f"rank {r['rank']} {state}" if st.get("ranks") else state)
    return "; ".join(states)


def cancel(a, run_id: str) -> dict:
    run_dir = _run_dir(a.runs_dir, run_id)
    st = _write_status(run_dir, state="cancelled", finished_at=time.time())
    names = [f"run-{run_id}", f"wait-{run_id}"] + [f"run-{run_id}-r{i}" for i in range(1, int(st.get("nodes") or 1))]
    for name in names:
        try:
            ray.kill(ray.get_actor(name, namespace=NAMESPACE), no_restart=True)
        except ValueError:
            pass
    gone = [r.get("machine") for r, name in _rank_places(st, run_id)
            if _on_run_machine(r, ["docker", "rm", "-f", name]) is None]
    if gone:
        st["container"] = f"not removed: {', '.join(map(str, gone))} left the cluster"
    try:
        remove_placement_group(ray.util.get_placement_group(f"pg-run-{run_id}"))
    except Exception:
        pass
    return st


def logs(a, run_id: str):
    """The run's output: the last -n lines, then with -f everything new until it ends."""
    run_dir = _run_dir(a.runs_dir, run_id)
    with open(os.path.join(run_dir, "spec.json")) as f:
        log_path = json.load(f).get("log") or os.path.join(run_dir, "log")
    if a.rank:
        log_path += f".rank{a.rank}"
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
        elif _read_status(run_dir).get("state") in TERMINAL:
            return
        else:
            time.sleep(1)


def fetch(a, run_id: str):
    """out/ as a tar.gz on stdout: `ssh box run.sh fetch ID > out.tgz`."""
    run_dir = _run_dir(a.runs_dir, run_id)
    with tarfile.open(fileobj=sys.stdout.buffer, mode="w|gz") as t:
        t.add(os.path.join(run_dir, "out"), arcname=f"{run_id}/out")


def _dir_bytes(d: str) -> int:
    return sum(os.path.getsize(os.path.join(r, f)) for r, _, fs in os.walk(d) for f in fs
               if not os.path.islink(os.path.join(r, f)))


def purge(a, run_id: str | None) -> list[dict]:
    """Deletes finished runs' records (spec, status, log, out/, code/): one ID, or every
    finished run older than --older-than days. A run still going is refused."""
    if run_id:
        targets = [_run_dir(a.runs_dir, run_id)]
    elif a.older_than is not None:
        cut = time.time() - a.older_than * 86400
        targets = [os.path.join(a.runs_dir, d) for d in os.listdir(a.runs_dir)
                   if os.path.isdir(os.path.join(a.runs_dir, d)) and os.path.getmtime(os.path.join(a.runs_dir, d)) < cut]
    else:
        raise SystemExit("purge ID, or purge --older-than DAYS")
    done = []
    for d in targets:
        try:
            st = _read_status(d)
        except (OSError, ValueError):
            st = {}
        state = st.get("state")
        rid = os.path.basename(d)
        if state not in TERMINAL and (_alive(f"run-{rid}") or _alive(f"wait-{rid}")):
            done.append({"id": os.path.basename(d), "purged": False, "reason": f"still {state}: cancel it first"})
            continue
        size = _dir_bytes(d)
        shutil.rmtree(d)
        done.append({"id": os.path.basename(d), "purged": True, "state": state, "bytes": size})
    return done


def list_runs(a, n: int = 20) -> list[dict]:
    if not os.path.isdir(a.runs_dir):
        return []
    ids = sorted(os.listdir(a.runs_dir), key=lambda d: os.path.getmtime(os.path.join(a.runs_dir, d)), reverse=True)
    out = []
    for i in ids[:n]:
        try:
            st = _read_status(os.path.join(a.runs_dir, i))
            out.append({k: st.get(k) for k in ("id", "state", "gpus", "exit_code", "cause", "after", "submitted_at",
                                               "started_at", "finished_at")})
        except (OSError, ValueError):
            pass
    return out


def stage_model(a, config: dict) -> dict:
    """Downloads a Hugging Face repo into the shared, read-only model store, as a queued
    CPU run (logged, cancellable like any other) with the fleet's token. Only when a
    person asks for that model."""
    store, token_file = config.get("model_store"), config.get("hf_token_file")
    if not store or not os.path.isdir(store):
        raise SystemExit("runs.yaml has no model_store directory: set model_store (and hf_token_file for gated repos)")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", a.repo or ""):
        raise SystemExit(f"not a Hugging Face repo id: {a.repo!r}")
    dest = os.path.join(store, a.repo, a.revision)
    os.makedirs(dest, exist_ok=True)
    stager = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stage_model.py")
    profile = dict(config["profiles"][config.get("stage_profile", a.profile or next(iter(config["profiles"])))])
    profile["allow"] = [dest] + ([os.path.dirname(token_file)] if token_file else [])
    profile["default_gpu_mem_gb"] = 0
    staging = argparse.Namespace(**{**vars(a), "profile": "_stage", "name": f"stage-{a.repo.replace('/', '-')}",
                                    "gpus": 0, "cpus": 2, "gpu_mem": 0, "mount": [f"{dest}:rw"]
                                    + ([f"{os.path.dirname(token_file)}:ro"] if token_file else []),
                                    "env": [f"HF_TOKEN_FILE={token_file}"] if token_file else [], "log": None,
                                    "workdir": None, "after": None, "retry": 2, "restart_on_oom": 0,
                                    "code": None, "machine": a.machine, "nodes": 1})
    stage_cmd = ["python3", "{code}/stage_model.py", a.repo, a.revision, dest] + [f"--include={g}" for g in a.include]
    # the stager travels as the run's pinned code
    tgz = io.BytesIO()
    with tarfile.open(fileobj=tgz, mode="w:gz") as t:
        t.add(stager, arcname="stage_model.py")
    tmp = os.path.join(a.runs_dir, f".stage-{uuid.uuid4().hex[:8]}.tar.gz")
    with open(tmp, "wb") as f:
        f.write(tgz.getvalue())
    try:
        staging.code = tmp
        return submit(staging, stage_cmd, {"profiles": {"_stage": profile}})
    finally:
        os.remove(tmp)


# ── CLI ──────────────────────────────────────────────────────────────────────

def main(argv: list[str]):
    if "--" in argv:
        i = argv.index("--")
        argv, cmd = argv[:i], argv[i + 1:]
    else:
        cmd = []
    p = argparse.ArgumentParser(prog="fleet_bridge run")
    p.add_argument("action", choices=["submit", "status", "cancel", "list", "logs", "fetch", "purge", "render",
                                      "stage-model"])
    p.add_argument("run_id", nargs="?", help="a run id (stage-model: the Hugging Face repo)")
    p.add_argument("--profiles", default=os.environ.get("FLEET_RUN_PROFILES", "/etc/zkm-fleet/runs.yaml"))
    p.add_argument("--runs-dir", default=os.environ.get("FLEET_RUNS_DIR", "/var/lib/zkm-fleet/runs"))
    p.add_argument("--address", default="127.0.0.1:6379")
    p.add_argument("--profile")
    p.add_argument("--name")
    p.add_argument("--gpus", type=float, default=1, help="cards; a fraction (0.5) shares one card and needs a --gpu-mem")
    p.add_argument("--cpus", type=int, default=8)
    p.add_argument("--gpu-mem", type=int, default=0,
                   help="GiB free each granted card must have before the job starts (default: the profile's, else the whole card)")
    p.add_argument("--vram-wait", type=int, default=1800,
                   help="seconds to wait for that memory before failing (the run shows waiting_vram meanwhile)")
    p.add_argument("--machine", action="append", help="the machine (host name or IP) the run must land on; with --nodes N, repeat for each")
    p.add_argument("--nodes", type=int, default=1,
                   help="spread the run over N machines: --gpus and --cpus on each, one container per machine, "
                        "ranks wired through FLEET_MASTER_ADDR / FLEET_NODE_RANK (and torchrun's PET_*)")
    p.add_argument("--after", help="run only after this run succeeded")
    p.add_argument("--retry", type=int, default=0, help="try a failed run again up to N times")
    p.add_argument("--restart-on-oom", type=int, default=0, help="try again up to N times when an attempt ran out of memory")
    p.add_argument("--code", help="git:/repo@ref or FILE.tar.gz, captured into the run (FLEET_CODE, {code})")
    p.add_argument("--mount", action="append", default=[], help="PATH[:ro|:rw], mounted at its own path; repeat")
    p.add_argument("--env", action="append", default=[], help="K=V for the job ({out} / {code} expand); repeat")
    p.add_argument("--log", help="file the job's output is appended to (default RUNS_DIR/<id>/log)")
    p.add_argument("--workdir")
    p.add_argument("-f", "--follow", action="store_true", help="logs: keep printing until the run ends")
    p.add_argument("-n", "--lines", type=int, default=50, help="logs: lines of history first")
    p.add_argument("--rank", type=int, default=0, help="logs: a --nodes run's rank k (its output is log.rank<k>)")
    p.add_argument("--older-than", type=float, help="purge: finished runs older than this many days")
    p.add_argument("--revision", default="main", help="stage-model: branch, tag or commit")
    p.add_argument("--include", action="append", default=[], help="stage-model: only files matching this glob; repeat")
    p.add_argument("--host-gpus", default="0", help="render: host cards to show in the command")
    a = p.parse_args(argv)
    os.makedirs(a.runs_dir, exist_ok=True)
    config = _load_config(a.profiles)
    if a.action == "render":
        profile = config["profiles"][a.profile]
        spec = {"id": a.name or "render", "profile": profile, "command": cmd, "cpus": a.cpus, "workdir": a.workdir,
                "mounts": [parse_mount(m) for m in a.mount], "out": os.path.join(a.runs_dir, "<id>", "out"),
                "env": dict(e.split("=", 1) for e in a.env if "=" in e)}
        print(shlex.join(docker_command(spec, a.host_gpus.split(",") if a.gpus else [])))
        return
    if a.action == "list":
        print(json.dumps(list_runs(a), indent=2))
        return
    if a.action == "logs":
        return logs(a, a.run_id)
    if a.action == "fetch":
        return fetch(a, a.run_id)
    ray.init(address=a.address, namespace=NAMESPACE, log_to_driver=False, logging_level="ERROR")
    if a.action == "submit":
        print(json.dumps(submit(a, cmd, config)))
    elif a.action == "status":
        print(json.dumps(status(a, a.run_id), indent=2))
    elif a.action == "cancel":
        print(json.dumps(cancel(a, a.run_id), indent=2))
    elif a.action == "purge":
        print(json.dumps(purge(a, a.run_id), indent=2))
    elif a.action == "stage-model":
        a.repo = a.run_id
        print(json.dumps(stage_model(a, config)))
    sys.stdout.flush()
