"""Execution adapter for one machine's local Ray. Stdio carries typed commands
and metadata to fleet-node; the adapter has no HTTP API or user tokens.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import logging
import math
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import ray
import yaml

from . import actors, reconcile, runs
from .model import Profile, RunSpec, RunState, RunStatus, Mount
from .rayapi import alive_nodes, resources

log = logging.getLogger(__name__)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    with temporary.open("w") as file:
        json.dump(value, file)
        file.flush()
        os.fsync(file.fileno())
    os.replace(temporary, path)


def read_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        return default


def container_gone(name: str) -> bool:
    result = subprocess.run(["docker", "inspect", "--format", "{{.State.Running}}", name], capture_output=True, text=True)
    return (result.returncode == 0 and result.stdout.strip() == "false"
            or result.returncode != 0 and ("No such object" in result.stderr or "No such container" in result.stderr))


def set_disabled(path: Path, name: str, disabled: bool, resources: dict[str, Any] | None = None) -> None:
    """Persist control-plane settings without rewriting machine-specific configuration."""
    text = path.read_text()
    for key, setting in {"disabled": disabled, **(resources or {})}.items():
        root = yaml.compose(text)
        sequence = next(v for k, v in root.value if k.value == "services")
        entry = next(e for e in sequence.value if any(k.value == "name" and v.value == name for k, v in e.value))
        value = next((v for k, v in entry.value if k.value == key), None)
        scalar = str(setting).lower()
        if value is not None:
            text = text[:value.start_mark.index] + scalar + text[value.end_mark.index:]
        elif entry.flow_style:
            position = entry.end_mark.index - 1
            text = text[:position] + f", {key}: {scalar}" + text[position:]
        else:
            position = entry.start_mark.index
            text = text[:position] + f"{key}: {scalar}\n" + " " * entry.start_mark.column + text[position:]
    temporary = path.with_suffix(".node-tmp")
    with temporary.open("w") as file:
        file.write(text)
        file.flush()
        os.fsync(file.fileno())
    os.chmod(temporary, path.stat().st_mode & 0o777)
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


class Executor:
    def __init__(self, services: str, profiles: str, runs_dir: str) -> None:
        self.services, self.profiles, self.runs_dir = Path(services), profiles, Path(runs_dir)
        self.journal = self.runs_dir / ".node" / "commands"
        self.journal.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self.running: set[str] = set()
        self.resource_locks: dict[str, threading.Lock] = {}
        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=4)
        self.output_lock = threading.Lock()
        self.services_lock = threading.Lock()

    def resource_lock(self, key: str) -> threading.Lock:
        with self.lock:
            return self.resource_locks.setdefault(key, threading.Lock())

    def cleanup(self, run: runs.RunDir) -> bool:
        # There is only one execution machine. A persisted old Ray IP must not
        # make local Docker cleanup depend on a vanished Ray node identity.
        if runs._going(run.id) or not container_gone(runs.container_name(run.id)):
            return False
        runs.discard_placement(run.id)
        return True

    def receive(self, command: dict[str, Any]) -> None:
        ident = command["id"]
        if not runs._inside(str(self.journal / ident), [str(self.journal)]) or not ident or "/" in ident or ".." in ident:
            raise ValueError("invalid command id")
        if command["operation"] in (2, 3):
            p = command["parameters"]
            write_json(self.runs_dir / ".node" / "stops" / f"{p['id']}-{p['lease']}.json", {"operation": command["operation"]})
        with self.lock:
            existing = read_json(self.journal / f"{ident}.json")
            if existing and existing["command"] != command:
                raise ValueError("command id reused with different parameters")
            if existing and existing.get("done") or ident in self.running:
                return
            write_json(self.journal / f"{ident}.json", {"command": command, "done": False})
            self.running.add(ident)
        self.pool.submit(self.execute, command)

    def execute(self, command: dict[str, Any]) -> None:
        params = command["parameters"]
        key = params.get("id") or params.get("service", {}).get("name") or command["id"]
        result: Any = {"cleaned": False}
        error = None
        try:
            with self.resource_lock(key):
                operation = command["operation"]
                if operation == 1:
                    result = self.launch(params)
                elif operation in (2, 3):
                    result = self.stop(params, preempt=operation == 3)
                elif operation == 4:
                    with self.services_lock:
                        result = self.service(params)
                elif operation == 5:
                    directory = self.runs_dir / params["id"]
                    if directory.exists():
                        current = read_json(directory / ".fleet-lease.json", {"lease": "imported"})["lease"]
                        if current != params["lease"]:
                            raise ValueError("stale purge lease")
                        run = runs.RunDir(str(self.runs_dir), params["id"])
                        if not self.cleanup(run):
                            raise ValueError("run cleanup is not confirmed")
                        shutil.rmtree(run.path)
                    result = {"cleaned": True}
                else:
                    raise ValueError("unsupported node operation")
        except BaseException as e:
            error = str(e)
            ident = params.get("id")
            if ident:
                try:
                    run = runs.RunDir(str(self.runs_dir), ident)
                    result = {"cleaned": self.cleanup(run)}
                except (runs.NotFound, FileNotFoundError):
                    result = {"cleaned": not runs._going(ident) and container_gone(runs.container_name(ident))}
                except BaseException:
                    pass
            log.exception("node command failed: %s", command["id"])
        finally:
            with self.lock:
                write_json(self.journal / f"{command['id']}.json", {"command": command, "done": True, "result": result, "error": error})
                self.running.discard(command["id"])

    def launch(self, p: dict[str, Any]) -> dict[str, Any]:
        if len(alive_nodes()) != 1:
            raise ValueError("Ray must contain only this machine")
        ident, lease = p["id"], p["lease"]
        if not ident or "/" in ident or ".." in ident or not lease or "/" in lease:
            raise ValueError("invalid execution id")
        if (self.runs_dir / ".node" / "stops" / f"{ident}-{lease}.json").exists():
            raise ValueError("execution lease was stopped before launch")
        directory = self.runs_dir / ident
        lease_file = directory / ".fleet-lease.json"
        previous = read_json(lease_file)
        if previous:
            run = runs.RunDir(str(self.runs_dir), ident)
            if previous["lease"] != lease:
                if run.status().state is not RunState.PREEMPTED or not self.cleanup(run):
                    raise ValueError("another execution lease is still present")
                runs.requeue_run(str(self.runs_dir), ident)
            elif run.status().state.terminal or runs._going(ident):
                return {"id": ident, "lease": lease}
            elif run.status().started_at is not None or not container_gone(runs.container_name(ident)):
                raise ValueError("execution is uncertain; refusing to relaunch")
        request = p["request"]
        if "code" in request:
            raise ValueError("run inputs must be prepared by the app owner in storage")
        profile = Profile.load(p["profile"])
        if profile.namespace != p["namespace"] or request.get("nodes", 1) != 1:
            raise ValueError("invalid local execution scope")
        directory.mkdir(parents=True, exist_ok=True)
        run = runs.RunDir(str(self.runs_dir), ident, must_exist=False)
        os.makedirs(run.out, exist_ok=True)
        mounts = [Mount.parse(m) for m in request.get("mount", [])]
        protected = [str(self.runs_dir), str(self.services.parent)]
        for mount in mounts:
            real = os.path.realpath(mount.path)
            if not runs._inside(real, profile.allow) or any(os.path.commonpath([real, root]) in (real, root) for root in protected):
                raise ValueError("mount outside allowed roots or overlaps fleet state")
            if not os.path.exists(real):
                raise ValueError("mount is missing on this machine")
        spec = RunSpec(id=ident, owner=p["owner"], namespace=p["namespace"], tag=p.get("tag"),
            profile_name=p["profile_name"], profile=profile, command=request["command"], gpus=request["gpus"], cpus=request["cpus"],
            out=run.out, mounts=mounts, env=dict(e.split("=", 1) for e in request.get("env", []) if "=" in e),
            log=request.get("log"), workdir=request.get("workdir"),
            gpu_mem_gb=request.get("gpu_mem", 0), vram_wait_s=request.get("vram_wait", 1800),
            retry=request.get("retry", 0), restart_on_oom=request.get("restart_on_oom", 0), nodes=1)
        roots = [m.path for m in mounts] + [run.out]
        if spec.workdir and not runs._inside(spec.expand(spec.workdir), roots):
            raise ValueError("workdir outside job mounts")
        if spec.log and not runs._inside(spec.log, [m.path for m in mounts if m.mode.value == "rw"] + [run.out]):
            raise ValueError("log outside writable job mounts")
        spec.write(run.spec_path)
        if not os.path.isfile(run.status_path):
            RunStatus(id=ident, state=RunState.QUEUED, namespace=p["namespace"], tag=p.get("tag"), priority=profile.priority,
                submitted_at=p["submitted_at"], gpus_requested=spec.gpus, cpus=spec.cpus, launch_pending=True).write(run.status_path)
        write_json(lease_file, {"lease": lease})
        if not runs.prepare_launch(spec):
            run.update(state=RunState.FAILED, launch_pending=False, finished_at=time.time(), error="local capacity changed")
            raise ValueError("local Ray capacity changed before launch")
        # Stable Ray actor name + get_if_exists makes heartbeat replay safe.
        try:
            runs.launch(spec, run.path)
        except BaseException:
            if not runs._going(ident):
                run.update(state=RunState.FAILED, launch_pending=False, finished_at=time.time(), error="actor launch failed")
                runs.discard_placement(ident)
            raise
        return {"id": ident, "lease": lease}

    def stop(self, p: dict[str, Any], preempt: bool) -> dict[str, Any]:
        if not (self.runs_dir / p["id"]).exists():
            if runs._going(p["id"]) or not container_gone(runs.container_name(p["id"])):
                raise ValueError("execution exists without a local record")
            return {"cleaned": True, "state": "cancelled"}
        run = runs.RunDir(str(self.runs_dir), p["id"])
        lease = read_json(Path(run.path) / ".fleet-lease.json", {"lease": "imported"})["lease"]
        if lease != p["lease"]:
            raise ValueError("stale execution lease")
        state = run.status().state
        if not state.terminal and state is not RunState.PREEMPTED:
            if preempt:
                runs.preempt(str(self.runs_dir), p["id"], "control-plane preemption")
            else:
                runs.cancel(str(self.runs_dir), p["id"])
        if not self.cleanup(run) and state.terminal:
            runs._end_actors(run.id, run.status().nodes)
            subprocess.run(["docker", "stop", "--time", str(run.spec().profile.stop_timeout_s), runs.container_name(run.id)], capture_output=True, timeout=run.spec().profile.stop_timeout_s + 30)
        return {"cleaned": self.cleanup(run), "state": run.status().state.value}

    def service(self, p: dict[str, Any]) -> dict[str, Any]:
        if len(alive_nodes()) != 1:
            raise ValueError("Ray must contain only this machine")
        service = p["service"]
        cfg = reconcile.load_services(yaml.safe_load(self.services.read_text()))
        configured = next(s for s in cfg.services if s.name == service["name"])
        if configured.namespace != service.get("namespace_ref", service["namespace"]) or configured.proof_app != service.get("proof_app"):
            raise ValueError("service scope differs from configured scope")
        if service.get("kind") != configured.kind.value or service.get("tag") != configured.tag:
            raise ValueError("service kind or tag differs from configured scope")
        disabled = p["disabled"]
        resources = {key: service.get(key, getattr(configured, key)) for key in ("gpus", "cpus", "replicas")}
        if (any(type(resources[k]) not in (int, float) or not math.isfinite(resources[k]) for k in ("gpus", "cpus"))
                or resources["gpus"] < 0 or resources["cpus"] <= 0
                or type(resources["replicas"]) is not int or not 1 <= resources["replicas"] <= 64
                or (configured.kind.value == "ziren-gpu" and (resources["gpus"] < 1 or resources["gpus"] % 1))):
            raise ValueError("invalid service resources")
        if configured.worker_command and resources["replicas"] != 1:
            raise ValueError("integrated app worker supports one machine-pinned replica")
        old_count = configured.replicas
        updates = {k: v for k, v in resources.items() if k in service}
        # Keep the previous replica count durable until every removed replica is
        # gone. A crash/replay must not forget an actor outside the new count.
        if not disabled:
            for index in range(resources["replicas"], old_count):
                name = f"{configured.name}-{index}"
                reconcile.stop(name)
                ray.get(actors.clean_after.remote(name), timeout=120)
                if not container_gone(actors.gpu_container(name)) or not container_gone(actors.app_container(name)):
                    raise ValueError("removed replica cleanup is uncertain")
        set_disabled(self.services, configured.name, disabled, None if disabled else updates)
        cfg = reconcile.load_services(yaml.safe_load(self.services.read_text()))
        for entry in cfg.services:
            if entry.name == configured.name:
                entry.disabled = False  # include every replica for cleanup, even when disabled
        replicas = [s for s in reconcile.replica_specs(cfg).values() if s.service.name == configured.name]
        if disabled:
            for index in range(max(old_count, resources["replicas"])):
                name = f"{configured.name}-{index}"
                reconcile.stop(name)
                ray.get(actors.clean_after.remote(name), timeout=120)
                if not container_gone(actors.gpu_container(name)) or not container_gone(actors.app_container(name)):
                    raise ValueError("service container cleanup is uncertain")
            set_disabled(self.services, configured.name, True, updates)
            return {"cleaned": True, "disabled": True}
        existing = reconcile.running()
        for spec in replicas:
            if spec.name in existing:
                health = reconcile.health_of(existing[spec.name])
                if health.is_ready and health.spec == actors.spec_hash(spec):
                    continue
                reconcile.stop(spec.name)
            reconcile.start(spec)
        return {"disabled": False, "ready": True}

    def report(self) -> dict[str, Any]:
        nodes = alive_nodes()
        ready = len(nodes) == 1
        total, free = resources()
        config = runs.load_config(self.profiles)
        services = reconcile.load_services(yaml.safe_load(self.services.read_text())).raw.get("services", [])
        running = reconcile.running()
        for service in services:
            names = [f"{service['name']}-{i}" for i in range(service.get("replicas", 1))]
            service.setdefault("namespace", "0" * 32)
            service.setdefault("priority", 0)
            service.setdefault("cpus", 1)
            service["gpus"] = 0 if service["kind"] == "ziren-cpu" else service.get("gpus", 1)
            service["ready"] = True
            for name in names:
                try:
                    health = reconcile.health_of(running[name], timeout_s=5) if name in running else None
                    healthy = health is not None and health.is_ready
                except Exception as error:
                    # Starting or restarting one actor must not suppress the
                    # machine heartbeat or another command's cleanup acknowledgment.
                    log.warning("service health unavailable for %s: %s", name, type(error).__name__)
                    healthy = False
                service["ready"] &= healthy
            service["cleaned"] = not any(name in running for name in names) and all(container_gone(actors.gpu_container(name)) and container_gone(actors.app_container(name)) for name in names)
        jobs = []
        for entry in self.runs_dir.iterdir():
            if entry.name.startswith(".") or not entry.is_dir() or not (entry / "spec.json").is_file():
                continue
            try:
                run = runs.RunDir(str(self.runs_dir), entry.name)
                spec, status = run.spec(), run.status()
                lease = read_json(entry / ".fleet-lease.json", {"lease": "imported"})["lease"]
                if (entry / f".fleet-reported-{lease}").exists() and (status.state.terminal or status.state is RunState.LOST):
                    continue
                cleaned_file = entry / f".fleet-cleaned-{lease}"
                cleaned = cleaned_file.exists()
                if not cleaned and (status.state.terminal or status.state in (RunState.PREEMPTED, RunState.LOST)):
                    cleaned = self.cleanup(run)
                    if cleaned:
                        cleaned_file.touch()
                if not status.state.terminal and status.state is not RunState.PREEMPTED and not status.launch_pending and not runs._going(run.id):
                    status = run.update(state=RunState.LOST, error="local Ray actor disappeared; cleanup must be confirmed")
                jobs.append({"id": run.id, "lease": lease, "status": status.dump(), "spec": spec.dump(), "cleaned": cleaned,
                    "logs_ready": (entry / f".fleet-logs-{lease}").exists()})
            except (OSError, ValueError, SystemExit):
                log.exception("reading local run %s", entry.name)
        with self.lock:
            results = [{"id": p.stem, "result": v["result"], "error": v["error"]}
                for p in self.journal.glob("*.json") if (v := read_json(p)).get("done")]
        return {"version": 1, "machine": socket.gethostname(), "ready": ready,
            "total_gpus": total.get("GPU", 0), "total_cpus": total.get("CPU", 0),
            "free_gpus": free.get("GPU", 0), "free_cpus": free.get("CPU", 0),
            "profiles": {k: v.dump() for k, v in config.profiles.items()},
            "services": services, "runs": jobs, "results": results}

    def publish(self) -> None:
        while True:
            try:
                report = self.report()
                with self.output_lock:
                    print(json.dumps({"report": report}), flush=True)
            except BaseException:
                log.exception("local Ray snapshot unavailable")
            time.sleep(2)


def main() -> None:
    parser = argparse.ArgumentParser()
    for name in ["services", "profiles", "runs-dir"]:
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--address", default="127.0.0.1:6379")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)
    if not args.address.startswith(("127.0.0.1:", "localhost:")):
        raise SystemExit("Ray must be local")
    ray.init(address=args.address, namespace=reconcile.NAMESPACE, log_to_driver=False)
    executor = Executor(args.services, args.profiles, args.runs_dir)
    for path in executor.journal.glob("*.json"):
        entry = read_json(path)
        if not entry.get("done"):
            executor.receive(entry["command"])
    threading.Thread(target=executor.publish, daemon=True).start()
    for line in sys.stdin:
        message = json.loads(line)
        if "command" in message:
            executor.receive(message["command"])
        elif "ack" in message:
            with executor.lock:
                (executor.journal / f"{message['ack']}.json").unlink(missing_ok=True)
        elif "reported" in message:
            (executor.runs_dir / message["reported"] / f".fleet-reported-{message['lease']}").touch()
        elif "logs_ready" in message:
            (executor.runs_dir / message["logs_ready"] / f".fleet-logs-{message['lease']}").touch()


if __name__ == "__main__":
    main()
