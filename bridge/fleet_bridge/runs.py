"""One-shot GPU jobs ("runs"): a command in a container, on cards Ray grants, until it exits.

    python -m fleet_bridge run submit --profiles runs.yaml --runs-dir DIR --profile NAME \\
        [--name NAME] [--gpus N] [--cpus N] [--log FILE] -- CMD ARG...
    python -m fleet_bridge run status|cancel ID      python -m fleet_bridge run list

A profile (runs.yaml) fixes what a run may touch: the image, the user, and the
volumes, mounted at their own paths so absolute paths in the job keep working.
Submitting creates a placement group for the run's cards and CPUs and a
detached actor in it; while no card is free the group waits, and the run is
"queued". The actor runs the container with exactly the granted cards, capped
at the run's CPUs, appends its output to the log file, records the exit code,
and then removes its placement group, which frees the cards and ends the actor.

State lives in RUNS_DIR/<id>/status.json, written by the submitter (queued)
and the actor (running, then succeeded / failed); cancel writes "cancelled".
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import socket
import subprocess
import sys
import threading
import time
import uuid

import ray
from ray.util.placement_group import placement_group, remove_placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

NAMESPACE = "zkm-fleet"
TERMINAL = ("succeeded", "failed", "cancelled")


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


def container_name(run_id: str) -> str:
    return f"fleet-run-{run_id}"


def docker_command(spec: dict, gpus: list[str]) -> list[str]:
    """The `docker run` for a run on the given host cards."""
    p = spec["profile"]
    cmd = ["docker", "run", "-d", "--name", container_name(spec["id"]), "--network", "host",
           f"--shm-size={p.get('shm_gb', 16)}g", "--label", "zkm-fleet=run", "--label", f"zkm-fleet.run={spec['id']}"]
    if gpus:
        cmd += ["--runtime=nvidia", "--gpus", f'"device={",".join(gpus)}"', "-e", "NVIDIA_DRIVER_CAPABILITIES=all"]
    if spec.get("cpus"):
        cmd += [f"--cpus={spec['cpus']}", "-e", f"OMP_NUM_THREADS={spec['cpus']}"]
    if p.get("user"):
        cmd += ["--user", str(p["user"])]
    if spec.get("workdir"):
        cmd += ["-w", spec["workdir"]]
    for path, opts in mounts(p):
        if os.path.exists(path):
            cmd += ["-v", f"{path}:{path}" + (f":{opts}" if opts else "")]
    for k, v in p.get("env", {}).items():
        cmd += ["-e", f"{k}={v}"]
    cmd.append(p["image"])
    cmd += spec["command"]
    return cmd


def mounts(profile: dict) -> list[tuple[str, str]]:
    """(path, options) per volume; a leading `?` marks a volume that may be absent."""
    return [(v.lstrip("?").partition(":")[0], v.partition(":")[2]) for v in profile.get("volumes", [])]


def missing_volumes(profile: dict) -> list[str]:
    """Required volumes that do not exist here. Docker would create them, owned by root,
    in someone's tree; a run fails instead."""
    return [v.lstrip("?").partition(":")[0] for v in profile.get("volumes", [])
            if not v.startswith("?") and not os.path.exists(v.partition(":")[0])]


@ray.remote(max_restarts=0, max_task_retries=0)
class GpuRun:
    """Runs one job to completion. Not restarted: a run that dies is reported, not repeated."""

    def __init__(self, spec: dict, run_dir: str):
        self.spec, self.run_dir = spec, run_dir
        self.gpus = [str(int(g)) for g in ray.get_gpu_ids()]
        threading.Thread(target=self._run, daemon=True).start()

    def ping(self) -> dict:
        return {"id": self.spec["id"], "gpus": self.gpus, "machine": socket.gethostname()}

    def _run(self):
        name = container_name(self.spec["id"])
        code = None
        try:
            subprocess.run(["docker", "rm", "-f", name], capture_output=True)
            missing = missing_volumes(self.spec["profile"])
            if missing:
                _write_status(self.run_dir, state="failed", error=f"volumes missing on {socket.gethostname()}: {missing}",
                              finished_at=time.time())
                return
            cmd = docker_command(self.spec, self.gpus)
            out = subprocess.run(cmd, capture_output=True, text=True)
            if out.returncode != 0:
                _write_status(self.run_dir, state="failed", error=f"docker run: {out.stderr.strip()}", finished_at=time.time())
                return
            _write_status(self.run_dir, state="running", gpus=self.gpus, machine=socket.gethostname(),
                          container=name, started_at=time.time())
            log_path = self.spec.get("log") or os.path.join(self.run_dir, "log")
            with open(log_path, "ab") as log:
                follower = subprocess.Popen(["docker", "logs", "-f", name], stdout=log, stderr=subprocess.STDOUT)
                waited = subprocess.run(["docker", "wait", name], capture_output=True, text=True)
                time.sleep(1)  # let the follower drain the last lines
                follower.terminate()
            code = int(waited.stdout.strip()) if waited.stdout.strip().lstrip("-").isdigit() else None
            _write_status(self.run_dir, state="succeeded" if code == 0 else "failed", exit_code=code,
                          finished_at=time.time())
        except Exception as e:  # report, never hang the cards
            _write_status(self.run_dir, state="failed", error=repr(e), finished_at=time.time())
        finally:
            subprocess.run(["docker", "rm", "-f", name], capture_output=True)
            try:
                remove_placement_group(ray.util.get_placement_group(f"pg-run-{self.spec['id']}"))
            finally:
                os._exit(0 if code == 0 else 1)


def _load_profiles(path: str) -> dict:
    import yaml

    with open(path) as f:
        return (yaml.safe_load(f) or {}).get("profiles", {})


def _inside(path: str, volumes: list[str]) -> bool:
    roots = [v.lstrip("?").partition(":")[0].rstrip("/") for v in volumes]
    return any(path == r or path.startswith(r + "/") for r in roots)


def submit(a, cmd: list[str]) -> dict:
    profiles = _load_profiles(a.profiles)
    if a.profile not in profiles:
        raise SystemExit(f"unknown profile {a.profile!r} (have: {', '.join(profiles) or 'none'})")
    profile = profiles[a.profile]
    if not cmd:
        raise SystemExit("no command after --")
    max_gpus = int(profile.get("max_gpus", 8))
    if a.gpus > max_gpus:
        raise SystemExit(f"profile {a.profile} allows at most {max_gpus} GPUs")
    if a.log and not _inside(a.log, profile.get("volumes", [])):
        raise SystemExit(f"--log {a.log} is outside the profile's volumes")
    base = re.sub(r"[^A-Za-z0-9_.-]", "-", a.name or a.profile)[:40]
    run_id = f"{base}-{uuid.uuid4().hex[:8]}"
    run_dir = os.path.join(a.runs_dir, run_id)
    os.makedirs(run_dir)
    spec = {"id": run_id, "profile_name": a.profile, "profile": profile, "command": cmd, "gpus": a.gpus,
            "cpus": a.cpus, "log": a.log, "workdir": a.workdir}
    with open(os.path.join(run_dir, "spec.json"), "w") as f:
        json.dump(spec, f, indent=2)
    _write_status(run_dir, id=run_id, state="queued", submitted_at=time.time(), gpus_requested=a.gpus, cpus=a.cpus)

    bundle = {"CPU": float(max(a.cpus, 1))}
    if a.gpus:
        bundle["GPU"] = float(a.gpus)
    pg = placement_group([bundle], strategy="STRICT_PACK", name=f"pg-run-{run_id}", lifetime="detached")
    GpuRun.options(
        name=f"run-{run_id}", namespace=NAMESPACE, lifetime="detached",
        num_cpus=bundle["CPU"], num_gpus=bundle.get("GPU", 0),
        scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg, placement_group_bundle_index=0),
    ).remote(spec, run_dir)
    return {"id": run_id, "state": "queued", "dir": run_dir}


def status(a, run_id: str) -> dict:
    run_dir = os.path.join(a.runs_dir, run_id)
    st = _read_status(run_dir)
    if st.get("state") not in TERMINAL:
        try:
            ray.get_actor(f"run-{run_id}", namespace=NAMESPACE)
        except ValueError:
            # The actor is gone without reporting (a Ray restart): say what the container does.
            out = subprocess.run(["docker", "inspect", "-f", "{{.State.Status}}", container_name(run_id)],
                                 capture_output=True, text=True)
            st["state"] = "lost"
            st["container"] = out.stdout.strip() or "gone"
    return st


def cancel(a, run_id: str) -> dict:
    run_dir = os.path.join(a.runs_dir, run_id)
    st = _write_status(run_dir, state="cancelled", finished_at=time.time())
    try:
        ray.kill(ray.get_actor(f"run-{run_id}", namespace=NAMESPACE), no_restart=True)
    except ValueError:
        pass
    subprocess.run(["docker", "rm", "-f", container_name(run_id)], capture_output=True)
    try:
        remove_placement_group(ray.util.get_placement_group(f"pg-run-{run_id}"))
    except Exception:
        pass
    return st


def list_runs(a, n: int = 20) -> list[dict]:
    if not os.path.isdir(a.runs_dir):
        return []
    ids = sorted(os.listdir(a.runs_dir), key=lambda d: os.path.getmtime(os.path.join(a.runs_dir, d)), reverse=True)
    out = []
    for i in ids[:n]:
        try:
            st = _read_status(os.path.join(a.runs_dir, i))
            out.append({k: st.get(k) for k in ("id", "state", "gpus", "exit_code", "submitted_at", "started_at", "finished_at")})
        except (OSError, ValueError):
            pass
    return out


def main(argv: list[str]):
    if "--" in argv:
        i = argv.index("--")
        argv, cmd = argv[:i], argv[i + 1:]
    else:
        cmd = []
    p = argparse.ArgumentParser(prog="fleet_bridge run")
    p.add_argument("action", choices=["submit", "status", "cancel", "list", "render"])
    p.add_argument("run_id", nargs="?")
    p.add_argument("--profiles", default=os.environ.get("FLEET_RUN_PROFILES", "/etc/zkm-fleet/runs.yaml"))
    p.add_argument("--runs-dir", default=os.environ.get("FLEET_RUNS_DIR", "/var/lib/zkm-fleet/runs"))
    p.add_argument("--address", default="127.0.0.1:6379")
    p.add_argument("--profile")
    p.add_argument("--name")
    p.add_argument("--gpus", type=int, default=1)
    p.add_argument("--cpus", type=int, default=8)
    p.add_argument("--log", help="file the job's output is appended to (default RUNS_DIR/<id>/log)")
    p.add_argument("--workdir")
    p.add_argument("--host-gpus", default="0", help="render: host cards to show in the command")
    a = p.parse_args(argv)
    os.makedirs(a.runs_dir, exist_ok=True)
    if a.action == "render":
        profile = _load_profiles(a.profiles)[a.profile]
        spec = {"id": a.name or "render", "profile": profile, "command": cmd, "cpus": a.cpus, "workdir": a.workdir}
        print(shlex.join(docker_command(spec, a.host_gpus.split(",") if a.gpus else [])))
        return
    if a.action == "list":
        print(json.dumps(list_runs(a), indent=2))
        return
    ray.init(address=a.address, namespace=NAMESPACE, log_to_driver=False, logging_level="ERROR")
    if a.action == "submit":
        print(json.dumps(submit(a, cmd)))
    elif a.action == "status":
        print(json.dumps(status(a, a.run_id), indent=2))
    elif a.action == "cancel":
        print(json.dumps(cancel(a, a.run_id), indent=2))
    sys.stdout.flush()
