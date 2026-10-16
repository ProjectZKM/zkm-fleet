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

from . import actors, reconcile, jobs
from .model import Batch, JobSpec, JobState, JobStatus, Mount
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
    _replace(path, text)


def _replace(path: Path, text: str) -> None:
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


def _is_content(line: str) -> bool:
    return bool(line.strip()) and not line.lstrip().startswith("#")


def remove_entry(path: Path, section: str, name: str) -> bool:
    """Removes one `services` entry or `batches` key from a configuration file and keeps
    every other line as written, with the comment lines directly above the entry. The
    previous file stays as <file>.removed-<UTC time>. False when there is no such entry."""
    text = path.read_text()
    root = yaml.compose(text)
    node = next((v for k, v in root.value if k.value == section), None) if root else None
    if isinstance(node, yaml.SequenceNode) and not node.flow_style:
        found = next(((e, e) for e in node.value
                      if any(k.value == "name" and v.value == name for k, v in e.value)), None)
    elif isinstance(node, yaml.MappingNode) and not node.flow_style:
        found = next(((k, v) for k, v in node.value if k.value == name), None)
    else:
        found = None
    if found is None:
        return False
    first, last = found
    start = text.rfind("\n", 0, first.start_mark.index) + 1
    while start > 0:
        above = text.rfind("\n", 0, start - 1) + 1
        line = text[above:start - 1]
        if not line.lstrip().startswith("#"):
            break
        start = above
    # An entry's node runs on to the next entry; it ends with its last line of content.
    end = offset = start
    for line in text[start:last.end_mark.index].splitlines(keepends=True):
        offset += len(line)
        if _is_content(line):
            end = offset
    if last.end_mark.column and not text[end - 1:end] == "\n":
        end = text.find("\n", end) + 1 or len(text)
    remaining = text[:start] + text[end:]
    if (yaml.safe_load(remaining) or {}).get(section) is None:
        # Removing the last entry would leave the section null; keep it an empty collection.
        key = next(k for k, _ in yaml.compose(remaining).value if k.value == section)
        line_end = remaining.find("\n", key.end_mark.index)
        line_end = len(remaining) if line_end < 0 else line_end
        remaining = remaining[:key.end_mark.index] + (": []" if isinstance(node, yaml.SequenceNode) else ": {}") + remaining[line_end:]
    shutil.copy2(path, path.with_name(f"{path.name}.removed-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}"))
    _replace(path, remaining)
    return True


class NodeExecutor:
    def __init__(self, services: str, batches: str, jobs_dir: str) -> None:
        self.services, self.batches, self.jobs_dir = Path(services), batches, Path(jobs_dir)
        self.journal = self.jobs_dir / ".node" / "commands"
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

    def cleanup(self, job: jobs.JobDirectory) -> bool:
        # There is only one execution machine. A persisted old Ray IP must not
        # make local Docker cleanup depend on a vanished Ray node identity.
        if jobs._going(job.id) or not container_gone(jobs.container_name(job.id)):
            return False
        jobs.discard_placement(job.id)
        return True

    def receive(self, command: dict[str, Any]) -> None:
        ident = command["id"]
        if not jobs._inside(str(self.journal / ident), [str(self.journal)]) or not ident or "/" in ident or ".." in ident:
            raise ValueError("invalid command id")
        if command["operation"] in (2, 3):
            p = command["parameters"]
            write_json(self.jobs_dir / ".node" / "stops" / f"{p['id']}-{p['lease']}.json", {"operation": command["operation"]})
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
        key = params.get("id") or params.get("service", {}).get("name") or params.get("app") or params.get("name") or command["id"]
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
                    directory = self.jobs_dir / params["id"]
                    if directory.exists():
                        current = read_json(directory / ".fleet-lease.json", {"lease": "imported"})["lease"]
                        if current != params["lease"]:
                            raise ValueError("stale purge lease")
                        job = jobs.JobDirectory(str(self.jobs_dir), params["id"])
                        if not self.cleanup(job):
                            raise ValueError("job cleanup is not confirmed")
                        shutil.rmtree(job.path)
                    result = {"cleaned": True}
                elif operation == 7:
                    with self.services_lock:
                        result = self.remove_app(params)
                elif operation == 8:
                    result = {"removed": remove_entry(Path(self.batches), "batches", params["name"])}
                else:
                    raise ValueError("unsupported node operation")
        except BaseException as e:
            error = str(e)
            ident = params.get("id")
            if ident:
                try:
                    job = jobs.JobDirectory(str(self.jobs_dir), ident)
                    result = {"cleaned": self.cleanup(job)}
                except (jobs.NotFound, FileNotFoundError):
                    result = {"cleaned": not jobs._going(ident) and container_gone(jobs.container_name(ident))}
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
        if (self.jobs_dir / ".node" / "stops" / f"{ident}-{lease}.json").exists():
            raise ValueError("execution lease was stopped before launch")
        directory = self.jobs_dir / ident
        lease_file = directory / ".fleet-lease.json"
        previous = read_json(lease_file)
        if previous:
            job = jobs.JobDirectory(str(self.jobs_dir), ident)
            if previous["lease"] != lease:
                if job.status().state is not JobState.PREEMPTED or not self.cleanup(job):
                    raise ValueError("another execution lease is still present")
                jobs.requeue_job(str(self.jobs_dir), ident)
            elif job.status().state.terminal or jobs._going(ident):
                return {"id": ident, "lease": lease}
            elif job.status().started_at is not None or not container_gone(jobs.container_name(ident)):
                raise ValueError("execution is uncertain; refusing to relaunch")
        request = p["request"]
        if "code" in request:
            raise ValueError("job inputs must be prepared by the app owner in storage")
        policy = Batch.load(p["policy"])
        if policy.namespace != p["namespace"] or request.get("nodes", 1) != 1:
            raise ValueError("invalid local execution scope")
        directory.mkdir(parents=True, exist_ok=True)
        job = jobs.JobDirectory(str(self.jobs_dir), ident, must_exist=False)
        os.makedirs(job.out, exist_ok=True)
        mounts = [Mount.parse(m) for m in request.get("mount", [])]
        protected = [str(self.jobs_dir), str(self.services.parent)]
        for mount in mounts:
            real = os.path.realpath(mount.path)
            if not jobs._inside(real, policy.allow) or any(os.path.commonpath([real, root]) in (real, root) for root in protected):
                raise ValueError("mount outside allowed roots or overlaps fleet state")
            if not os.path.exists(real):
                raise ValueError("mount is missing on this machine")
        spec = JobSpec(id=ident, owner=p["owner"], namespace=p["namespace"], tag=p.get("tag"),
            batch=p["batch"], policy=policy, command=request["command"], gpus=request["gpus"], cpus=request["cpus"],
            out=job.out, mounts=mounts, env=dict(e.split("=", 1) for e in request.get("env", []) if "=" in e),
            log=request.get("log"), workdir=request.get("workdir"),
            gpu_mem_gb=request.get("gpu_mem", 0), vram_wait_s=request.get("vram_wait", 1800),
            retry=request.get("retry", 0), restart_on_oom=request.get("restart_on_oom", 0), nodes=1)
        roots = [m.path for m in mounts] + [job.out]
        if spec.workdir and not jobs._inside(spec.expand(spec.workdir), roots):
            raise ValueError("workdir outside job mounts")
        if spec.log and not jobs._inside(spec.log, [m.path for m in mounts if m.mode.value == "rw"] + [job.out]):
            raise ValueError("log outside writable job mounts")
        spec.write(job.spec_path)
        if not os.path.isfile(job.status_path):
            JobStatus(id=ident, state=JobState.QUEUED, namespace=p["namespace"], tag=p.get("tag"), priority=policy.priority,
                submitted_at=p["submitted_at"], gpus_requested=spec.gpus, cpus=spec.cpus, launch_pending=True).write(job.status_path)
        write_json(lease_file, {"lease": lease})
        if not jobs.prepare_launch(spec):
            job.update(state=JobState.FAILED, launch_pending=False, finished_at=time.time(), error="local capacity changed")
            raise ValueError("local Ray capacity changed before launch")
        # Stable Ray actor name + get_if_exists makes heartbeat replay safe.
        try:
            jobs.launch(spec, job.path)
        except BaseException:
            if not jobs._going(ident):
                job.update(state=JobState.FAILED, launch_pending=False, finished_at=time.time(), error="actor launch failed")
                jobs.discard_placement(ident)
            raise
        return {"id": ident, "lease": lease}

    def stop(self, p: dict[str, Any], preempt: bool) -> dict[str, Any]:
        if not (self.jobs_dir / p["id"]).exists():
            if jobs._going(p["id"]) or not container_gone(jobs.container_name(p["id"])):
                raise ValueError("execution exists without a local record")
            return {"cleaned": True, "state": "cancelled"}
        job = jobs.JobDirectory(str(self.jobs_dir), p["id"])
        lease = read_json(Path(job.path) / ".fleet-lease.json", {"lease": "imported"})["lease"]
        if lease != p["lease"]:
            raise ValueError("stale execution lease")
        state = job.status().state
        if not state.terminal and state is not JobState.PREEMPTED:
            if preempt:
                jobs.preempt(str(self.jobs_dir), p["id"], "control-plane preemption")
            else:
                jobs.cancel(str(self.jobs_dir), p["id"])
        if not self.cleanup(job) and state.terminal:
            jobs._end_actors(job.id, job.status().nodes)
            subprocess.run(["docker", "stop", "--time", str(job.spec().policy.stop_timeout_s), jobs.container_name(job.id)], capture_output=True, timeout=job.spec().policy.stop_timeout_s + 30)
        return {"cleaned": self.cleanup(job), "state": job.status().state.value}

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

    def remove_app(self, p: dict[str, Any]) -> dict[str, Any]:
        """An app was deleted: every service here that runs it (a proof app's server by its
        `tenant/elf_id`, a serving app by its name) has what is left of its replicas cleaned up
        the way disabling does, then is dropped from services.yaml. App data stays: storage
        roots, workdirs, env files and images belong to the operator."""
        cfg = reconcile.load_services(yaml.safe_load(self.services.read_text()))
        namespaces = {p["namespace"], p.get("namespace_name") or p["namespace"]}
        removed = []
        for service in cfg.services:
            if service.namespace not in namespaces or p["app"] not in (service.proof_app, service.name if service.kind.value == "app" else None):
                continue
            for index in range(service.replicas):
                replica = f"{service.name}-{index}"
                reconcile.stop(replica)
                ray.get(actors.clean_after.remote(replica), timeout=120)
                if not container_gone(actors.gpu_container(replica)) or not container_gone(actors.app_container(replica)):
                    raise ValueError(f"{service.name}: service container cleanup is uncertain")
            if remove_entry(self.services, "services", service.name):
                removed.append(service.name)
        return {"removed": removed}

    def report(self) -> dict[str, Any]:
        nodes = alive_nodes()
        ready = len(nodes) == 1
        total, free = resources()
        config = jobs.load_config(self.batches)
        apps = reconcile.load_services(yaml.safe_load(self.services.read_text()))
        services = apps.raw.get("services", [])
        running = reconcile.running()
        for service in services:
            names = [f"{service['name']}-{i}" for i in range(service.get("replicas", 1))]
            service.setdefault("namespace", "0" * 32)
            service.setdefault("priority", 0)
            service.setdefault("cpus", 1)
            service["gpus"] = 0 if service["kind"] == "ziren-cpu" else service.get("gpus", 1)
            service["ready"] = True
            # Where `fleetctl app logs` reads from: one node log per replica.
            service["logs"] = [os.path.join(apps.cluster.log_dir, f"{name}.node.log") for name in names]
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
        job_reports = []
        for entry in self.jobs_dir.iterdir():
            if entry.name.startswith(".") or not entry.is_dir() or not (entry / "spec.json").is_file():
                continue
            try:
                job = jobs.JobDirectory(str(self.jobs_dir), entry.name)
                spec, status = job.spec(), job.status()
                lease = read_json(entry / ".fleet-lease.json", {"lease": "imported"})["lease"]
                if (entry / f".fleet-reported-{lease}").exists() and (status.state.terminal or status.state is JobState.LOST):
                    continue
                cleaned_file = entry / f".fleet-cleaned-{lease}"
                cleaned = cleaned_file.exists()
                if not cleaned and (status.state.terminal or status.state in (JobState.PREEMPTED, JobState.LOST)):
                    cleaned = self.cleanup(job)
                    if cleaned:
                        cleaned_file.touch()
                if not status.state.terminal and status.state is not JobState.PREEMPTED and not status.launch_pending and not jobs._going(job.id):
                    status = job.update(state=JobState.LOST, error="local Ray actor disappeared; cleanup must be confirmed")
                job_reports.append({"id": job.id, "lease": lease, "status": status.dump(), "spec": spec.dump(), "cleaned": cleaned,
                    "logs_ready": (entry / f".fleet-logs-{lease}").exists()})
            except (OSError, ValueError, SystemExit):
                log.exception("reading local job %s", entry.name)
        with self.lock:
            results = [{"id": p.stem, "result": v["result"], "error": v["error"]}
                for p in self.journal.glob("*.json") if (v := read_json(p)).get("done")]
        return {"version": 2, "machine": socket.gethostname(), "ready": ready,
            "total_gpus": total.get("GPU", 0), "total_cpus": total.get("CPU", 0),
            "free_gpus": free.get("GPU", 0), "free_cpus": free.get("CPU", 0),
            "batches": {k: v.dump() for k, v in config.batches.items()},
            "services": services, "jobs": job_reports, "results": results}

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
    for name in ["services", "batches", "jobs-dir"]:
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--address", default="127.0.0.1:6379")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)
    if not args.address.startswith(("127.0.0.1:", "localhost:")):
        raise SystemExit("Ray must be local")
    ray.init(address=args.address, namespace=reconcile.NAMESPACE, log_to_driver=False)
    executor = NodeExecutor(args.services, args.batches, args.jobs_dir)
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
            (executor.jobs_dir / message["reported"] / f".fleet-reported-{message['lease']}").touch()
        elif "logs_ready" in message:
            (executor.jobs_dir / message["logs_ready"] / f".fleet-logs-{message['lease']}").touch()


if __name__ == "__main__":
    main()
