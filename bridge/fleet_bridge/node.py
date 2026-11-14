"""Execution adapter for one machine's local Ray. Stdio carries typed commands
and metadata to fleet-node; the adapter has no HTTP API or user tokens.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import logging
import os
import re
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
from .model import AppSpec, Batch, JobSpec, JobState, JobStatus, Mount
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


def _entry(root: Any, section: str, name: str) -> Any:
    """The `apps` entry named `name`, or the `batches` value under key `name`."""
    node = next((v for k, v in root.value if k.value == section), None) if root else None
    if isinstance(node, yaml.SequenceNode):
        return next((e for e in node.value if any(k.value == "name" and v.value == name for k, v in e.value)), None)
    if isinstance(node, yaml.MappingNode):
        return next((v for k, v in node.value if k.value == name), None)
    return None


def set_fields(path: Path, section: str, name: str, fields: dict[str, Any]) -> None:
    """Writes `fields` into one `apps` entry or `batches` value, keeping every other line
    as written. A value is written in YAML flow style (JSON), so maps and lists fit on its line."""
    text = path.read_text()
    for key, setting in fields.items():
        entry = _entry(yaml.compose(text), section, name)
        if not isinstance(entry, yaml.MappingNode):
            raise ValueError(f"{section}: no entry {name}")
        name_node, value = next(((k, v) for k, v in entry.value if k.value == key), (None, None))
        written = json.dumps(setting)
        if value is not None:
            # A block value runs on to the next key; it ends with its last line of content.
            end = offset = value.start_mark.index
            for line in text[offset:value.end_mark.index].splitlines(keepends=True):
                offset += len(line)
                if _is_content(line):
                    end = offset - (1 if line.endswith("\n") else 0)
            start = value.start_mark.index
            if isinstance(value, yaml.CollectionNode) and not value.flow_style:
                # A block list or map starts on the next line, and a list's items may sit at
                # the key's own indentation, so the new value goes right after the key's colon.
                colon = text.index(":", name_node.end_mark.index) + 1
                rest = text[colon:text.index("\n", colon)].strip()
                written = " " + written + (f"  {rest}" if rest else "")
                start = colon
            text = text[:start] + written + text[end:]
        elif entry.flow_style:
            position = entry.end_mark.index - 1
            text = text[:position] + f", {key}: {written}" + text[position:]
        else:
            position = entry.start_mark.index
            text = text[:position] + f"{key}: {written}\n" + " " * entry.start_mark.column + text[position:]
    # The file is replaced only with text that reads back as the values asked for.
    try:
        data = (yaml.safe_load(text) or {}).get(section)
    except yaml.YAMLError as error:
        raise ValueError(f"{section}: writing {', '.join(fields)} into {name} would break {path.name}: {error}")
    got = (next((e for e in data if isinstance(e, dict) and e.get("name") == name), None)
           if isinstance(data, list) else (data or {}).get(name))
    if not isinstance(got, dict) or any(got.get(k) != v for k, v in fields.items()):
        raise ValueError(f"{section}: {name} doesn't read back {', '.join(fields)} as written")
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
    """Removes one `apps` entry or `batches` key from a configuration file and keeps
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


def append_entry(path: Path, section: str, entry: dict[str, Any]) -> None:
    """Appends one entry to an `apps` list, keeping every other line as written. The entry
    goes on one line in YAML flow style, under a block list, an empty list or a missing key.
    It follows the list's last entry, so keys after the list stay after it."""
    text = path.read_text()
    root = yaml.compose(text)
    node = next((v for k, v in root.value if k.value == section), None) if root else None
    item = f"- {json.dumps(entry)}\n"
    if isinstance(node, yaml.SequenceNode) and node.value and not node.flow_style:
        last = node.value[-1]
        end = offset = last.start_mark.index
        for line in text[offset:last.end_mark.index].splitlines(keepends=True):
            offset += len(line)
            if _is_content(line):
                end = offset
        if not text[:end].endswith("\n"):
            # The last entry ends mid-line, as a one-line flow entry does: the new one goes
            # after the rest of that line, or after a newline added at the end of the file.
            newline = text.find("\n", end)
            if newline < 0:
                text, end = text + "\n", len(text) + 1
            else:
                end = newline + 1
        text = text[:end] + " " * (last.start_mark.column - 2) + item + text[end:]
    elif isinstance(node, yaml.SequenceNode) and not node.value:
        text = text[:node.start_mark.index].rstrip(" ") + "\n  " + item + text[node.end_mark.index:]
    elif node is None and root is not None:
        text = text if text.endswith("\n") or not text else text + "\n"
        text += f"{section}:\n  " + item
    else:
        raise ValueError(f"{section}: must be a list to add an entry to")
    _replace(path, text)

def _holds(outer: str, inner: str) -> bool:
    """Whether directory `outer` is `inner` or holds it."""
    return os.path.commonpath([outer, inner]) == outer


class NodeExecutor:
    def __init__(self, apps: str, batches: str, jobs_dir: str) -> None:
        self.apps_file, self.batches, self.jobs_dir = Path(apps), batches, Path(jobs_dir)
        self.journal = self.jobs_dir / ".node" / "commands"
        self.journal.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self.running: set[str] = set()
        self.resource_locks: dict[str, threading.Lock] = {}
        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=4)
        self.output_lock = threading.Lock()
        self.apps_lock = threading.Lock()

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
        key = params.get("id") or (params.get("app") or params.get("entry") or params.get("batch") or {}).get("name") or command["id"]
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
                    with self.apps_lock:
                        result = self.set_app(params)
                elif operation == 7:
                    with self.apps_lock:
                        result, then = self.app(params)
                    # Deleting files can take long; other app changes needn't wait for it.
                    result.update(then())
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
        # Batch policy is supplied by the scheduler, but these host resources are
        # never batch storage, even in a previously accepted or stale launch.
        protected = [os.path.realpath(root) for root in (
            self.jobs_dir, self.apps_file.parent, "/dev", "/proc", "/sys", "/run",
            "/var/run", "/var/lib/docker", "/var/lib/containerd",
        )]
        for mount in mounts:
            real = os.path.realpath(mount.path)
            if not jobs._inside(real, policy.allow) or any(os.path.commonpath([real, root]) in (real, root) for root in protected):
                raise ValueError("mount outside allowed roots or overlaps fleet state")
            if not os.path.exists(real):
                raise ValueError("mount is missing on this machine")
            if not (os.path.isfile(real) or os.path.isdir(real)):
                raise ValueError("batch mounts must be regular files or directories")
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

    def set_app(self, p: dict[str, Any]) -> dict[str, Any]:
        """Starts or stops an app's replicas as apps.yaml configures them; the scheduler
        writes that configuration (enabled state, resources, settings) with NODE_OPERATION_APP
        first. A stop removes every replica of the app, also one beyond its configured
        count, and confirms its containers are gone."""
        if len(alive_nodes()) != 1:
            raise ValueError("Ray must contain only this machine")
        app = p["app"]
        cfg = reconcile.load_apps(yaml.safe_load(self.apps_file.read_text()))
        configured = next((a for a in cfg.apps if a.name == app["name"]), None)
        if configured is None:
            raise ValueError(f"{app['name']} is not configured on this node")
        if (configured.namespace != app.get("namespace_ref", app["namespace"]) or configured.app != app.get("app")
                or configured.elf_id != (app.get("elf_id") or "").lower()):
            raise ValueError("app scope differs from configured scope")
        if app.get("kind") != configured.kind.value or app.get("tag") != configured.tag:
            raise ValueError("app kind or tag differs from configured scope")
        running = [n for n in reconcile.running()
                   if n.rpartition("-")[0] == configured.name and n.rpartition("-")[2].isdigit()]
        if p["disabled"]:
            self._remove_replicas(sorted(set(running) | {f"{configured.name}-{i}" for i in range(configured.replicas)}))
            return {"cleaned": True, "disabled": True}
        if configured.disabled:
            raise ValueError(f"{configured.name} is disabled in apps.yaml")
        replicas = [s for s in reconcile.replica_specs(cfg).values() if s.app.name == configured.name]
        self._remove_replicas([n for n in running if n not in {s.name for s in replicas}])
        existing = reconcile.running()
        for spec in replicas:
            if spec.name in existing:
                health = reconcile.health_of(existing[spec.name])
                if health.is_ready and health.spec == actors.spec_hash(spec):
                    continue
                reconcile.stop(spec.name)
            reconcile.start(spec)
        return {"disabled": False, "ready": True}

    @staticmethod
    def _remove_replicas(names: list[str]) -> None:
        for name in names:
            reconcile.stop(name)
            ray.get(actors.clean_after.remote(name), timeout=120)
            if not container_gone(actors.gpu_container(name)) or not container_gone(actors.app_container(name)):
                raise ValueError(f"{name}: app container cleanup is uncertain")

    def app(self, p: dict[str, Any]) -> tuple[dict[str, Any], Any]:
        """An app-level change from the scheduler, for an app (`p["app"]`: the entries that
        run the app of that name, in a namespace given by id and by name), a batch (`p["batch"]`, by name), or the one entry a change of an app stopped
        (`p["entry"]`, by name, set only): `remove` it, or `set` some of its configuration.
        Settings are written to the configuration only: the scheduler drains and stops an
        app first and starts it again afterwards. `p["create"]` adds a new app's
        entry to the configuration, disabled until the app is enabled.

        A removed entry has what is left of its replicas cleaned up the way disabling does.
        Removing an app from this node alone (a scale left it no replicas here) keeps its
        data. Deleting it (`p["delete"]`) removes what this node holds of it too: its
        replicas' logs, its worker's directory and the files of its proof jobs
        (`delete["files"]`) in its proof storage. A deleted batch's jobs (`delete["jobs"]`)
        lose their directories: record, logs and outputs. Volumes, mounts, env files,
        storage roots and images stay, and so does whatever holds or lies in a path another
        app or batch uses, here or (by `delete["keep"]`) on another node. Returns the
        result, and what deletes the files, for after the configuration lock."""
        nothing = lambda: {}
        if "create" in p:
            entry = p["create"]
            AppSpec.load(entry)
            entries = (yaml.safe_load(self.apps_file.read_text()) or {}).get("apps") or []
            if any(e.get("name") == entry["name"] for e in entries):
                raise ValueError(f"{entry['name']} is already configured on this node")
            append_entry(self.apps_file, "apps", entry)
            return {"created": [entry["name"]]}, nothing
        if "batch" in p:
            name, path = p["batch"]["name"], Path(self.batches)
            if p.get("remove"):
                removed = [name] if remove_entry(path, "batches", name) else []
                if "delete" not in p:
                    return {"removed": removed}, nothing
                return {"removed": removed}, lambda: self._delete_jobs(name, p["delete"].get("jobs") or [])
            entry = (yaml.safe_load(path.read_text()) or {}).get("batches", {}).get(name)
            if entry is None:
                return {"updated": []}, nothing
            Batch.load({**entry, **p["set"]})
            set_fields(path, "batches", name, p["set"])
            return {"updated": [name]}, nothing
        if "entry" in p:
            # The one entry a change of an app stopped: its configuration only.
            name = p["entry"]["name"]
            entries = {e["name"]: e for e in (yaml.safe_load(self.apps_file.read_text()) or {}).get("apps", [])}
            if p.get("remove") or name not in entries:
                return {"updated": []}, nothing
            AppSpec.load({**entries[name], **p["set"]})
            set_fields(self.apps_file, "apps", name, p["set"])
            return {"updated": [name]}, nothing
        target = p["app"]
        namespaces = {target["namespace"], target.get("namespace_name") or target["namespace"]}
        entries = {e["name"]: e for e in (yaml.safe_load(self.apps_file.read_text()) or {}).get("apps", [])}
        cfg = reconcile.load_apps(yaml.safe_load(self.apps_file.read_text()))
        apps = [a for a in cfg.apps if a.namespace in namespaces
                and a.app == target["name"]]
        if not p.get("remove"):
            for app in apps:
                AppSpec.load({**entries[app.name], **p["set"]})
            for app in apps:
                set_fields(self.apps_file, "apps", app.name, p["set"])
            return {"updated": [a.name for a in apps]}, nothing
        removed = []
        for app in apps:
            self._remove_replicas([f"{app.name}-{index}" for index in range(app.replicas)])
            if remove_entry(self.apps_file, "apps", app.name):
                removed.append(app)
        if "delete" not in p:
            return {"removed": [a.name for a in removed]}, nothing
        # What the node's other apps and batches use, read under the lock.
        in_use = self._paths_in_use(p["delete"].get("keep") or [], removed)
        log_dir = cfg.cluster.log_dir
        return ({"removed": [a.name for a in removed]},
                lambda: self._delete_app_files(removed, log_dir, in_use, p["delete"].get("files") or []))

    def _paths_in_use(self, keep: list[str], removed: list[AppSpec]) -> list[str]:
        """The host paths that a deletion keeps, with what holds or lies in one: those of
        the apps and batches this node still configures, `keep` (the ones on other nodes),
        and the volumes and env files of the `removed` entries."""
        paths = list(keep)
        for e in (yaml.safe_load(self.apps_file.read_text()) or {}).get("apps") or []:
            paths += [e.get(k) for k in ("workdir", "worker_workdir", "proof_storage_root", "worker_env_file")]
            paths += [str(v).split(":")[0] for v in e.get("volumes") or []]
        for b in ((yaml.safe_load(Path(self.batches).read_text()) or {}).get("batches") or {}).values():
            paths += list((b or {}).get("allow") or [])
        for a in removed:
            paths += [v.split(":")[0] for v in a.volumes] + [a.worker_env_file]
        return [os.path.realpath(p) for p in paths if isinstance(p, str) and os.path.isabs(p)]

    def _refusal(self, path: str, in_use: list[str], log_dir: str, *, inside_ok: bool = False) -> str | None:
        """Why `path` must stay, or None when it can go. Fleet's own directories stay, with
        what holds or lies in one; so do the filesystem's top level and what holds the home
        directory, and what holds a path in use or (unless `inside_ok`) lies in one."""
        if not os.path.isabs(path) or ".." in Path(path).parts:
            return "not an absolute path"
        if os.path.islink(path):
            return "a symbolic link"
        real = os.path.realpath(path)
        if len(Path(real).parts) < 3 or _holds(real, os.path.realpath(Path.home())):
            return "too close to the root of the filesystem"
        for own in (self.jobs_dir, self.apps_file.parent, Path(self.batches).parent, log_dir, Path.cwd()):
            own = os.path.realpath(own)
            if _holds(real, own) or _holds(own, real):
                return f"holds or lies in fleet's own {own}"
        for used in in_use:
            if _holds(real, used) or not inside_ok and _holds(used, real):
                return f"another app or batch uses {used}"
        return None

    def _delete(self, path: str, reason: str | None, deleted: list[str], kept: list[dict[str, str]]) -> None:
        if reason is None:
            try:
                if os.path.isdir(path) and not os.path.islink(path):
                    shutil.rmtree(path)
                else:
                    os.unlink(path)
                deleted.append(path)
                return
            except OSError as e:
                reason = f"{type(e).__name__}: {e.strerror or e}"
        kept.append({"path": path, "reason": reason})

    def _delete_app_files(self, removed: list[AppSpec], log_dir: str, in_use: list[str], files: list[str]) -> dict[str, Any]:
        """Deletes what the `removed` entries of a deleted app held: the files of its proof
        jobs in their proof storage, their replicas' logs and their workers' directories."""
        deleted: list[str] = []
        kept: list[dict[str, str]] = []
        roots = [os.path.realpath(a.proof_storage_root) for a in removed if a.proof_storage_root]
        for path in files if roots else []:
            if not os.path.lexists(path):
                continue
            real = os.path.realpath(path)
            if not any(real != root and _holds(root, real) for root in roots):
                kept.append({"path": path, "reason": "not in the app's proof storage on this node"})
                continue
            self._delete(path, self._refusal(path, in_use, log_dir, inside_ok=True), deleted, kept)
        logs = sorted(os.listdir(log_dir)) if os.path.isdir(log_dir) else []
        for a in removed:
            pattern = re.compile(rf"{re.escape(a.name)}-\d+\.node\.log")
            for name in filter(pattern.fullmatch, logs):
                self._delete(os.path.join(log_dir, name), None, deleted, kept)
            if a.worker_workdir and os.path.lexists(a.worker_workdir):
                self._delete(a.worker_workdir, self._refusal(a.worker_workdir, in_use, log_dir), deleted, kept)
        if deleted or kept:
            log.info("deleted app's files: %d deleted, kept %s", len(deleted), kept)
        return {"deleted": deleted, "kept": kept}

    def _delete_jobs(self, batch: str, ids: list[str]) -> dict[str, Any]:
        """Deletes the directories of the jobs `ids` of deleted batch `batch`: each one's
        record, logs and outputs. One whose container may still be there stays."""
        deleted, kept = 0, []
        for ident in ids:
            try:
                job = jobs.JobDirectory(str(self.jobs_dir), ident)
            except jobs.NotFound:
                continue
            directory = Path(job.path)
            try:
                try:
                    owner = job.spec().batch
                except Exception:
                    owner = batch  # a record half written: the scheduler lists it as the batch's
                if owner != batch:
                    kept.append({"path": job.path, "reason": f"a job of batch {owner}"})
                    continue
                lease = read_json(directory / ".fleet-lease.json", {"lease": "imported"})["lease"]
                if not (directory / f".fleet-cleaned-{lease}").exists() and not self.cleanup(job):
                    kept.append({"path": job.path, "reason": "its container may still be there"})
                    continue
                shutil.rmtree(directory)
                for stop in (self.jobs_dir / ".node" / "stops").glob(f"{ident}-*.json"):
                    stop.unlink(missing_ok=True)
                deleted += 1
            except Exception as e:
                kept.append({"path": job.path, "reason": f"{type(e).__name__}: {e}"})
        if deleted or kept:
            log.info("deleted batch %s: %d job directories deleted, kept %s", batch, deleted, kept)
        return {"jobs_deleted": deleted, "kept": kept}

    def report(self) -> dict[str, Any]:
        # The results first: a command finished by then has changed the configuration read
        # below, so the scheduler never sees a create done before the node reports its entry.
        with self.lock:
            results = [{"id": p.stem, "result": v["result"], "error": v["error"]}
                for p in self.journal.glob("*.json") if (v := read_json(p)).get("done")]
        nodes = alive_nodes()
        ready = len(nodes) == 1
        total, free = resources()
        config = jobs.load_config(self.batches)
        cfg = reconcile.load_apps(yaml.safe_load(self.apps_file.read_text()))
        apps = cfg.raw.get("apps", [])
        running = reconcile.running()
        for app in apps:
            names = [f"{app['name']}-{i}" for i in range(app.get("replicas", 1))]
            app.setdefault("namespace", "0" * 32)
            app.setdefault("app", app["name"])
            app.setdefault("priority", 0)
            app.setdefault("cpus", 1)
            app["gpus"] = 0 if app["kind"] == "ziren-cpu" else app.get("gpus", 1)
            app["ready"] = True
            # Where `fleetctl app logs` reads from: one node log per replica.
            app["logs"] = [os.path.join(cfg.cluster.log_dir, f"{name}.node.log") for name in names]
            for name in names:
                try:
                    health = reconcile.health_of(running[name], timeout_s=5) if name in running else None
                    healthy = health is not None and health.is_ready
                except Exception as error:
                    # Starting or restarting one actor must not suppress the
                    # machine heartbeat or another command's cleanup acknowledgment.
                    log.warning("app health unavailable for %s: %s", name, type(error).__name__)
                    healthy = False
                app["ready"] &= healthy
            app["cleaned"] = not any(name in running for name in names) and all(container_gone(actors.gpu_container(name)) and container_gone(actors.app_container(name)) for name in names)
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
        return {"version": 3, "machine": socket.gethostname(), "ready": ready,
            "total_gpus": total.get("GPU", 0), "total_cpus": total.get("CPU", 0),
            "free_gpus": free.get("GPU", 0), "free_cpus": free.get("CPU", 0),
            "batches": {k: v.dump() for k, v in config.batches.items()},
            "apps": apps, "jobs": job_reports, "results": results}

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
    for name in ["apps", "batches", "jobs-dir"]:
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--address", default="127.0.0.1:6379")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)
    if not args.address.startswith(("127.0.0.1:", "localhost:")):
        raise SystemExit("Ray must be local")
    ray.init(address=args.address, namespace=reconcile.NAMESPACE, log_to_driver=False)
    executor = NodeExecutor(args.apps, args.batches, args.jobs_dir)
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
