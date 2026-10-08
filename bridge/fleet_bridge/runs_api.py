"""One-shot runs over HTTP, for `fleetctl job`: the same actions as `run.sh`, behind
an admin or scoped user token, so runs can be submitted from any machine that reaches the port.

    POST   /runs                     submit (JSON: the RunArgs fields marked `remote`, plus "command")
    GET    /runs?n=N[&active=1][&profile=P]  the newest runs (only the active ones, one app's)
    GET    /apps                     the run apps (runs.yaml profiles) and their runs
    POST   /apps                     add a run app (JSON: "name" and the profile's fields: image, allow, ...)
    PUT    /apps/{name}/disabled     the app takes no new runs (the ones going finish); DELETE enables it again
    DELETE /apps/{name}              remove a run app (its profile), refused while it has runs going
    GET    /runs/{id}                status
    GET    /runs/{id}/logs           ?rank=K&lines=N (the tail) or &offset=O (what came after O)
    POST   /runs/{id}/cancel           SIGTERM, SIGKILL the app's stop_timeout_s later; the run ends
    POST   /runs/{id}/preempt          the same stop, then the run queues again (its cards go to others meanwhile)
    DELETE /runs/{id}                purge one finished run
    DELETE /runs?older_than=DAYS     purge finished runs older than that
    GET    /runs/{id}/out.tar.gz     the run's out/
    PUT    /uploads                  a code tarball (body); the answer's path goes in "code"
    POST   /models                   stage a Hugging Face repo ({"repo", "revision", "include"})
    GET    /healthz                  no token

It runs as a `kind: runs-api` service (a Ray actor on the machine that holds the
profiles file and the runs dir), so a submit goes through the same code as run.sh:
paths in `mount`, `log`, `workdir` and a `git:` code source are paths on that
machine; a code tarball on the client's machine is uploaded first (fleetctl does it).
"""

from __future__ import annotations

import hashlib
import hmac
import json
import math
import time
import logging
import os
import re
import socket
import threading
import urllib.parse
import urllib.request
import urllib.error
import uuid
import tempfile

import yaml
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import ray

from . import runs
from .actors import _STOPPING, _stopping, _watch, spec_hash
from .model import Health, ModelError, Record, ReplicaSpec, RunArgs, RunState, Uploaded, from_dict

MAX_BODY = 512 << 20  # a code tarball


class ApiError(Exception):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status


class Api:
    """The actions, on a fixed profiles file and runs dir."""

    def __init__(self, profiles: str, runs_dir: str, scheduler_url: str = "http://127.0.0.1:9191") -> None:
        self.profiles, self.runs_dir = profiles, runs_dir
        self.scheduler_url = scheduler_url.rstrip("/")
        self.quota_lock = threading.Lock()
        self.admin_token = ""
        self.quota_stop = threading.Event()
        self.quota_thread: threading.Thread | None = None
        self.accounted: set[tuple[str, str]] = set()
        self.lock = threading.Lock()  # a submit and an app's delete do not interleave
        os.makedirs(os.path.join(runs_dir, runs.UPLOADS_DIR), exist_ok=True)
        os.makedirs(os.path.join(runs_dir, runs.QUOTA_ACTIVE_DIR), exist_ok=True)
        # Recovery scans history once; dispatch and submit use the durable active index.
        for name in os.listdir(runs_dir):
            if name.startswith("."):
                continue
            try:
                run = runs.RunDir(runs_dir, name)
                spec, st = run.spec(), run.status()
                if spec.namespace and (st.preempt_requeue or not (st.quota_released and (st.state.terminal or st.state is RunState.PREEMPTED))):
                    run.track_quota()
            except (SystemExit, OSError, ValueError):
                continue

    def authenticate(self, bearer: str) -> dict[str, Any]:
        req = urllib.request.Request(self.scheduler_url + "/access", headers={"Authorization": bearer})
        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                c = json.load(response)
        except urllib.error.HTTPError as e:
            raise ApiError(401 if e.code == 401 else 503, "credential validation failed") from None
        except (OSError, ValueError):
            raise ApiError(503, "credential validation unavailable") from None
        if not isinstance(c, dict):
            raise ApiError(503, "invalid credential validation response")
        address = c.get("address", "")
        if (not isinstance(address, str) or len(address) != 40
                or any(ch not in "0123456789abcdef" for ch in address)
                or not isinstance(c.get("apps"), list) or c.get("revoked_ms") is not None):
            raise ApiError(401, "invalid credential")
        self.validate_namespace(c)
        self.validate_quota(c)
        if c.get("tag") is not None and (not isinstance(c["tag"],str) or not re.fullmatch(r"[a-z][a-z0-9-]{0,62}",c["tag"])):
            raise ApiError(401,"invalid tag identity")
        self.validate_quota({"quota":c.get("tag_quota")})
        return c

    @staticmethod
    def validate_namespace(c: dict[str, Any]) -> None:
        namespace = c.get("namespace", "")
        if (not isinstance(namespace, str) or len(namespace) != 32
                or any(ch not in "0123456789abcdef" for ch in namespace)):
            raise ApiError(401, "invalid namespace identity")

    @staticmethod
    def validate_quota(scope: dict[str, Any]) -> None:
        quota = scope.get("quota")
        if not isinstance(quota, dict) or any(
            quota.get(k) is not None and (type(quota[k]) not in (int, float)
                or not math.isfinite(quota[k]) or quota[k] < 0) for k in ("gpus", "cpus")):
            raise ApiError(503, "invalid namespace quota response")

    def scheduler(self, method: str, path: str, body: Any = None) -> Any:
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(self.scheduler_url + "/admin" + path, data=data, method=method,
            headers={"Authorization": f"Bearer {self.admin_token}", "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                raw = response.read()
                return json.loads(raw) if raw else None
        except urllib.error.HTTPError as e:
            raise ApiError(e.code if e.code in (404, 422, 429) else 503, e.read().decode(errors="replace")) from None
        except (OSError, ValueError):
            raise ApiError(503, "namespace quota service unavailable") from None

    def release_quota(self, namespace: str, run_id: str) -> None:
        self.scheduler("DELETE", f"/namespaces/{namespace}/allocations/{run_id}")

    def finish_quota(self, run: runs.RunDir, namespace: str) -> bool:
        if not run.status().quota_released and not runs.confirm_cleanup(run):
            return False
        if not run.status().quota_released:
            self.release_quota(namespace, run.id)
            run.update(force=True, quota_released=True, launch_pending=False)
        self.accounted.discard((namespace, run.id))
        if run.status().state.terminal or (run.status().state is RunState.PREEMPTED and not run.status().preempt_requeue):
            run.untrack_quota()
        return True

    def dispatch_quotas(self) -> None:
        """Reconcile durable launch intents; physical waiters occupy no namespace units."""
        with self.quota_lock:
            jobs = []
            directory = os.path.join(self.runs_dir, runs.QUOTA_ACTIVE_DIR)
            for name in os.listdir(directory):
                try:
                    run = runs.RunDir(self.runs_dir, name)
                    spec, status = run.spec(), run.status()
                except (SystemExit, OSError, ValueError):
                    continue
                namespace = spec.namespace
                if namespace:
                    jobs.append((status.submitted_at, run, spec, status, namespace))
            for _, run, spec, status, namespace in jobs:
                if status.state.terminal or status.state is RunState.PREEMPTED:
                    if self.finish_quota(run, namespace) and status.state is RunState.PREEMPTED and status.preempt_requeue:
                        runs.requeue_run(self.runs_dir, run.id)
                elif (status.state is RunState.WAITING_QUOTA or (status.state is RunState.WAITING_DEPENDENCY and spec.quota_managed)) and not status.launch_pending and not status.quota_released:
                    self.finish_quota(run, namespace)
            for _, run, spec, _, namespace in sorted(jobs, key=lambda j: (-j[2].profile.priority, j[0], j[1].id)):
                status = run.status()
                if status.state.terminal or status.state is RunState.PREEMPTED:
                    continue
                waiting = status.state is RunState.WAITING_QUOTA or (status.state is RunState.WAITING_DEPENDENCY and spec.quota_managed)
                if waiting and spec.after:
                    try:
                        dep = runs.RunDir(self.runs_dir, spec.after).status().state
                    except (SystemExit, OSError, ValueError):
                        dep = RunState.LOST
                    if dep in (RunState.FAILED, RunState.CANCELLED, RunState.LOST):
                        run.update(state=RunState.FAILED, finished_at=time.time(), error=f"dependency {spec.after} ended {dep.value}; not started")
                        continue
                    if dep is not RunState.SUCCEEDED:
                        continue
                if waiting and not status.launch_pending and not status.quota_released:
                    continue  # an earlier execution still needs confirmed cleanup
                launching = waiting or status.launch_pending
                if launching:
                    if runs._going(run.id):
                        run.update(launch_pending=False)
                        continue
                    run.update(launch_pending=True)
                    ready = runs.prepare_launch(spec)
                    if not ready and self.preempt_for(spec):
                        ready = runs.prepare_launch(spec)
                    if not ready:
                        if status.launch_pending or (namespace, run.id) in self.accounted or not status.quota_released:
                            self.release_quota(namespace, run.id)
                        self.accounted.discard((namespace, run.id))
                        run.update(state=RunState.WAITING_QUOTA, launch_pending=False, quota_released=True,
                                   detail="waiting for physical Ray GPU/CPU capacity")
                        continue
                key = namespace, run.id
                if launching or key not in self.accounted:
                    try:
                        self.scheduler("POST", f"/namespaces/{namespace}/allocations", {
                            "id": run.id, "tag": spec.tag, "gpus": spec.gpus * spec.nodes, "cpus": max(spec.cpus, 1) * spec.nodes,
                            "restore": not waiting})
                    except ApiError as e:
                        if launching:
                            runs.discard_placement(run.id)
                        if waiting and e.status == 429:
                            self.preempt_for(spec)  # next pass retries admission after confirmed cleanup
                        if waiting and e.status in (422, 429):
                            run.update(state=RunState.FAILED if e.status == 422 else RunState.WAITING_QUOTA,
                                       launch_pending=False, error=str(e) if e.status == 422 else None,
                                       detail="waiting for namespace/tag GPU/CPU quota")
                            continue
                        raise
                    self.accounted.add(key)
                    run.update(quota_released=False)
                if not launching:
                    continue
                st = run.update(state=RunState.QUEUED, detail=None)
                if st.state is not RunState.QUEUED:
                    self.finish_quota(run, namespace)
                    continue
                try:
                    runs.launch(spec, run.path)
                except Exception as e:
                    # A failed Ray response does not prove actor creation failed.
                    run.update(detail=f"reconciling launch: {e}")

    def preempt_for(self, spec: Any) -> bool:
        """Policy stays in Fleet; Ray supplies placement and physical availability."""
        pool = [c for c in runs.candidates(self.runs_dir)
                if c.namespace == spec.namespace and c.priority < spec.profile.priority and c.takeable(time.time())]
        if not pool:
            return False
        scope = self.admin_namespace(spec.namespace, self.admin_token, spec.tag)
        if scope.get("suspended_ms") is not None:
            return False
        allocations = self.scheduler("GET", f"/namespaces/{spec.namespace}/allocations")

        def fits(victims: set[str]) -> bool:
            remaining = [a for a in allocations if a["id"] != "run:" + spec.id and a["id"] not in {"run:" + i for i in victims}]
            for quota, entries in ((scope["quota"], remaining),
                                   (scope["tag_quota"], [a for a in remaining if a.get("tag") == spec.tag])):
                for key, need in (("gpus", spec.gpus * spec.nodes), ("cpus", max(spec.cpus, 1) * spec.nodes)):
                    if quota.get(key) is not None and sum(a[key] for a in entries) + need > quota[key] + 1e-9:
                        return False
            return True

        from .rayapi import free_by_node_ip
        victims = runs.plan_preemption(spec, free_by_node_ip(), pool, time.time(), fits)
        if not victims:
            return False
        for run_id in victims:
            try:
                runs.preempt(self.runs_dir, run_id, f"job {spec.id} (priority {spec.profile.priority}) needs resources",
                             namespace=spec.namespace, priority=spec.profile.priority)
                victim = runs.RunDir(self.runs_dir, run_id)
                self.finish_quota(victim, spec.namespace)
            except runs.Conflict:
                continue  # completion/cancel won the race; retry physical admission
        return True

    def start_quota_dispatcher(self) -> None:
        def poll() -> None:
            while not self.quota_stop.is_set():
                try:
                    self.dispatch_quotas()
                except Exception:
                    logging.exception("namespace quota dispatch failed; reservations retained")
                self.quota_stop.wait(5)
        self.quota_thread = threading.Thread(target=poll, daemon=True)
        self.quota_thread.start()

    def namespace_for(self, spec: Any) -> str | None:
        return spec.namespace

    def purge_runs(self, older_than: float | None) -> Any:
        with self.quota_lock:
            for name in os.listdir(self.runs_dir):
                if name.startswith("."):
                    continue
                try:
                    run = runs.RunDir(self.runs_dir, name)
                    spec, status = run.spec(), run.status()
                except (SystemExit, OSError, ValueError):
                    continue
                if status.state.terminal:
                    namespace = self.namespace_for(spec)
                    if namespace:
                        self.finish_quota(run, namespace)
            return runs.purge(self.args(older_than=older_than), None)

    def manage_run(self, action: str, run_id: str) -> Any:
        with self.quota_lock:
            run = runs.RunDir(self.runs_dir, run_id)
            spec = run.spec()
            namespace = self.namespace_for(spec)
            scope = self.admin_namespace(namespace, self.admin_token) if namespace else None
            if action == "purge":
                if scope:
                    if not run.status().state.terminal or not self.finish_quota(run, scope["namespace"]):
                        raise ApiError(409, "finish or cancel the run and wait for its actors before purging")
                result = runs.purge(self.args(), run_id)
            elif action == "cancel":
                result = runs.cancel(self.args(), run_id)
            else:
                result = runs.preempt(self.runs_dir, run_id, "by the operator (fleetctl job preempt)")
            if scope and action != "purge":
                self.finish_quota(run, scope["namespace"])
            return result

    def admin_namespace(self, reference: str, token: str, tag: str | None = None, *, all_tags: bool = False) -> dict[str, Any]:
        if tag is not None and (not isinstance(tag,str) or not re.fullmatch(r"[a-z][a-z0-9-]{0,62}",tag)):
            raise ApiError(400,"invalid tag")
        selector = "" if all_tags else "?" + urllib.parse.urlencode({"tag": tag or ""})
        req = urllib.request.Request(self.scheduler_url + "/admin/namespaces/" + urllib.parse.quote(reference, safe="") + selector,
                                     headers={"Authorization": f"Bearer {token}"})
        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                n = json.load(response)
        except urllib.error.HTTPError as e:
            raise ApiError(404 if e.code == 404 else 503, "namespace lookup failed") from None
        except (OSError, ValueError):
            raise ApiError(503, "namespace lookup unavailable") from None
        scope = {"namespace": n["id"], "namespace_name": n["name"], "quota": n.get("quota"), "suspended_ms": n.get("suspended_ms")}
        self.validate_namespace(scope)
        self.validate_quota(scope)
        scope["tag"] = tag
        scope["all_tags"] = all_tags
        scope["tag_quota"] = n.get("tag_quota", {"gpus":None,"cpus":None})
        self.validate_quota({"quota":scope["tag_quota"]})
        return scope

    def scoped_args(self, scope: dict[str, Any] | None) -> RunArgs:
        return self.args(namespace=scope["namespace"] if scope else None, tag=scope.get("tag") if scope else None,
                         namespace_all_tags=scope.get("all_tags",False) if scope else False)

    def own_run(self, run_id: str, principal: dict[str, Any]) -> None:
        try:
            spec = runs.RunDir(self.runs_dir, run_id).spec()
        except (SystemExit, OSError, ValueError):
            raise ApiError(404, "no such run") from None
        if not runs.in_namespace(spec, principal["namespace"], principal.get("tag"), principal.get("all_tags",False)):
            raise ApiError(404, "no such run")

    def user_submit(self, body: dict[str, Any], principal: dict[str, Any]) -> Record:
        if body.get("profile") not in principal["apps"]:
            raise ApiError(403, "app is not granted to this credential")
        if body.get("nodes", 1) != 1:
            raise ApiError(400, "user jobs currently support one machine per job")
        if body.get("after"):
            self.own_run(body["after"], principal)
        # Never expose managed job storage or fleet configuration through a broad
        # profile allow-list, including via symlinks or an ancestor mount.
        protected = [os.path.realpath(self.runs_dir), os.path.realpath(os.path.dirname(self.profiles))]
        upload_roots = [os.path.realpath(self.upload_root(principal["namespace"], principal["tag"]))]
        paths = [m.rsplit(":", 1)[0] if m.endswith((":ro", ":rw")) else m for m in body.get("mount", [])]
        paths += [body[k] for k in ("log", "workdir") if body.get(k) and "{" not in body[k]]
        code = body.get("code")
        if code:
            if code.startswith("git:"):
                paths.append(code[4:].rsplit("@", 1)[0])
            elif os.path.dirname(os.path.realpath(code)) not in upload_roots:
                raise ApiError(403, "user code must come from this tag's uploads")
        for path in paths:
            real = os.path.realpath(path)
            if any(os.path.commonpath([real, root]) in (real, root) for root in protected):
                raise ApiError(403, "path overlaps fleet configuration or job storage")
        return self.submit(body, owner=principal["address"], namespace=principal["namespace"], tag=principal["tag"])

    def args(self, **changes: Any) -> RunArgs:
        """RunArgs for an action: the server's paths, the caller's choices."""
        return from_dict(RunArgs, {"action": "submit", "profiles": self.profiles, "runs_dir": self.runs_dir, **changes})

    def submit(self, body: dict[str, Any], owner: str | None = None, namespace: str | None = None, tag: str | None = None) -> Record:
        command = body.pop("command", None)
        if not isinstance(command, list) or not command or not all(isinstance(c, str) for c in command):
            raise ApiError(400, "command: a non-empty list of strings")
        unknown = set(body) - RunArgs.remote_fields()
        if unknown:
            raise ApiError(400, f"unknown or server-side fields: {sorted(unknown)}")
        # Resolve the app and submit under the same lock as app deletion/disable.
        with self.lock:
            config = runs.load_config(self.profiles)
            profile = config.profiles.get(body.get("profile"))
            if profile is None:
                raise ApiError(404, "unknown run app")
            scope = self.admin_namespace(profile.namespace, self.admin_token, tag)
            if scope.get("suspended_ms") is not None:
                raise ApiError(403, "namespace is suspended")
            if namespace is not None and namespace not in (scope["namespace"], scope["namespace_name"]):
                raise ApiError(403, "run app belongs to another namespace")
            namespace = scope["namespace"]
            profile.namespace = namespace
            nodes = body.get("nodes", 1)
            if type(nodes) is not int or nodes < 1:
                raise ApiError(400, "nodes must be a positive integer")
            gpus, cpus = body.get("gpus", 1), body.get("cpus", 8)
            if any(type(v) not in (int, float) or not math.isfinite(v) or v < 0 for v in (gpus, cpus)):
                raise ApiError(400, "GPU/CPU requests must be finite and non-negative")
            if type(cpus) is not int or cpus < 1:
                raise ApiError(400, "cpus must be a positive integer")
            if any(q.get(k) is not None and v * nodes > q[k] + 1e-9 for q in (scope["quota"],scope["tag_quota"]) for k,v in (("gpus",gpus),("cpus",cpus))):
                raise ApiError(422, "job exceeds the namespace or tag GPU/CPU quota")
            result = runs.submit(self.args(owner=owner, namespace=namespace, tag=tag, **body), command, config)
        try:
            self.dispatch_quotas()
        except ApiError:
            pass  # Accepted work remains waiting; the background dispatcher retries privately.
        result.state = runs.RunDir(self.runs_dir, result.id).status().state
        return result

    def delete_app(self, name: str) -> Record:
        with self.lock:
            return runs.delete_app(self.args(), name)

    def add_app(self, body: dict[str, Any], namespace: str | None = None) -> Record:
        reference = namespace or body.get("namespace")
        if not isinstance(reference, str) or not reference:
            raise ApiError(400, "namespace: an existing namespace is required")
        scope = self.admin_namespace(reference, self.admin_token)
        if scope.get("suspended_ms") is not None:
            raise ApiError(403, "namespace is suspended")
        if body.get("namespace") not in (None, scope["namespace"], scope["namespace_name"]):
            raise ApiError(400, "conflicting app namespace")
        body["namespace"] = scope["namespace"]
        name = body.pop("name", None)
        if not isinstance(name, str):
            raise ApiError(400, "name: the app's name")
        with self.lock:
            return runs.add_app(self.args(), name, body)

    def service_apps(self) -> list[dict[str, Any]]:
        path = os.path.join(os.path.dirname(self.profiles), "services.yaml")
        if not os.path.exists(path):
            return []
        from . import reconcile
        from .model import ServiceKind
        with open(path) as f:
            cfg = reconcile.load_services(yaml.safe_load(f) or {})
        return [{"name": s.name, "namespace": s.namespace, "type": "run", "persistent": True, "disabled": s.disabled, "priority": s.priority,
                 "image": s.image, "max_gpus": s.gpus * s.replicas,
                 "replicas": s.replicas} for s in cfg.services if s.kind is ServiceKind.APP]

    def apps(self, principal: dict[str, Any] | None = None, scope: dict[str, Any] | None = None) -> list[Any]:
        selected = scope or principal
        config = runs.load_config(self.profiles)
        if selected:
            for p in config.profiles.values():
                if p.namespace == selected.get("namespace_name"):
                    p.namespace = selected["namespace"]
        profiles = runs.apps(self.scoped_args(selected), config)
        if principal is not None:
            return [p.dump() for p in profiles if p.name in principal["apps"]]
        services = self.service_apps()
        if selected:
            services = [s for s in services if s["namespace"] in (selected["namespace"], selected.get("namespace_name"))]
        if {p.name for p in profiles} & {s["name"] for s in services}:
            raise ApiError(409, "run profile and persistent service have the same name")
        return [p.dump() for p in profiles] + services

    def set_app_disabled(self, name: str, disabled: bool) -> Any:
        return self.set_app_field(name, "disabled", disabled)

    def set_app_priority(self, name: str, priority: Any) -> Any:
        if type(priority) is not int or not -(2**31) <= priority < 2**31:
            raise ApiError(400, "priority must be a signed 32-bit integer")
        return self.set_app_field(name, "priority", priority)

    def set_app_field(self, name: str, field: str, desired: Any) -> Any:
        with self.lock:
            services = self.service_apps()
            if not any(s["name"] == name for s in services):
                return (runs.set_app_disabled if field == "disabled" else runs.set_app_priority)(self.args(), name, desired)
            if any(p.name == name for p in runs.apps(self.args(), runs.load_config(self.profiles))):
                raise ApiError(409, "run profile and persistent service have the same name")
            from . import reconcile
            path = os.path.join(os.path.dirname(self.profiles), "services.yaml")
            with open(path) as f:
                text = f.read()
            data = yaml.safe_load(text)
            for svc in data["services"]:
                if svc["name"] == name:
                    svc[field] = desired
            cfg = reconcile.load_services(data)
            # Persist first: a watchdog or future apply must respect the disabled state.
            fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), prefix=".services-")
            try:
                with os.fdopen(fd, "w") as f:
                    # Edit only the scalar/entry, retaining comments and unrelated formatting.
                    root = yaml.compose(text)
                    sequence = next(v for k, v in root.value if k.value == "services")
                    entry = next(node for node in sequence.value
                                 if any(k.value == "name" and v.value == name for k, v in node.value))
                    if entry.flow_style:
                        raise ApiError(409, "persistent app entries must use block YAML for policy changes")
                    value = next((v for k, v in entry.value if k.value == field), None)
                    scalar = str(desired).lower()
                    if value is not None:
                        text = text[:value.start_mark.index] + scalar + text[value.end_mark.index:]
                    else:
                        pos = entry.start_mark.index
                        text = text[:pos] + f"{field}: {scalar}\n" + " " * entry.start_mark.column + text[pos:]
                    f.write(text)
                    f.flush()
                    os.fsync(f.fileno())
                os.chmod(tmp, os.stat(path).st_mode & 0o777)
                os.replace(tmp, path)
            finally:
                if os.path.exists(tmp):
                    os.unlink(tmp)
            if field == "priority":
                return {"name": name, "type": "run", "persistent": True, "priority": desired}
            report = reconcile.reconcile_once(cfg, only=[name])
            if report.unhealthy:
                raise ApiError(503, f"{name}: reconciliation failed; desired state saved")
            return {"name": name, "type": "run", "persistent": True, "disabled": desired, "report": report.dump()}

    def upload_root(self, namespace: str | None, tag: str | None) -> str:
        root = os.path.join(self.runs_dir, runs.UPLOADS_DIR)
        if namespace is not None:
            root = os.path.join(root, "namespaces", namespace)
            root = os.path.join(root, "untagged") if tag is None else os.path.join(root, "tags", tag)
        return root

    def upload(self, data: bytes, namespace: str | None = None, tag: str | None = None) -> Uploaded:
        if not data:
            raise ApiError(400, "empty body")
        sha = hashlib.sha256(data).hexdigest()
        root = self.upload_root(namespace, tag)
        os.makedirs(root, exist_ok=True)
        path = os.path.join(root, f"{sha}.tar.gz")
        if os.path.exists(path):
            os.utime(path)  # uploaded again now: `purge --older-than` keeps it
        else:
            tmp = f"{path}.{uuid.uuid4().hex[:8]}.tmp"
            with open(tmp, "wb") as f:
                f.write(data)
            os.replace(tmp, path)
        return Uploaded(path=path, sha256=sha, bytes=len(data))

    def stage_model(self, body: dict[str, Any]) -> Record:
        repo = body.get("repo")
        if not isinstance(repo, str):
            raise ApiError(400, "repo: a Hugging Face repo id")
        a = self.args(action="stage-model", run_id=repo, revision=body.get("revision", "main"),
                      include=body.get("include", []), profile=body.get("profile"))
        return runs.stage_model(a, runs.load_config(self.profiles))


def _query(path: str) -> tuple[list[str], dict[str, str]]:
    url = urllib.parse.urlsplit(path)
    parts = [urllib.parse.unquote(p) for p in url.path.strip("/").split("/") if p]
    return parts, {k: v[-1] for k, v in urllib.parse.parse_qs(url.query).items()}


def _int(q: dict[str, str], key: str, default: int | None) -> int | None:
    try:
        v = int(q[key]) if key in q else default
    except ValueError:
        raise ApiError(400, f"{key}: an integer") from None
    if v is not None and v < 0:
        raise ApiError(400, f"{key}: not negative")
    return v


def _float(q: dict[str, str], key: str) -> float:
    try:
        return float(q[key])
    except ValueError:
        raise ApiError(400, f"{key}: a number") from None


class _Chunked:
    """A response body in HTTP/1.1 chunks: a stream that breaks off is seen by the client
    as broken (no last chunk), not as a short, complete file."""

    def __init__(self, w: Any) -> None:
        self.w = w

    def write(self, b: bytes) -> int:
        if b:
            self.w.write(f"{len(b):x}\r\n".encode() + bytes(b) + b"\r\n")
        return len(b)

    def flush(self) -> None:
        self.w.flush()

    def end(self) -> None:
        self.w.write(b"0\r\n\r\n")


def _flag(q: dict[str, str], key: str) -> bool:
    return q.get(key, "").lower() in ("1", "true", "yes")


def handler(api: Api, token: str) -> type[BaseHTTPRequestHandler]:
    api.admin_token = token
    class Handler(BaseHTTPRequestHandler):
        server_version = "zkm-fleet-runs/1"
        protocol_version = "HTTP/1.1"  # for the chunked fetch; every connection still serves one request
        streaming = False  # the headers of a streamed answer are out: an error can only end it

        def log_message(self, fmt: str, *args: Any) -> None:  # the actor log, one line per call
            print(f"{self.address_string()} {fmt % args}", flush=True)

        def _send(self, status: int, body: bytes, ctype: str = "application/json") -> None:
            self.send_response(status)
            self.send_header("content-type", ctype)
            self.send_header("content-length", str(len(body)))
            self.send_header("connection", "close")  # a body left unread cannot become the next request
            self.close_connection = True
            self.end_headers()
            self.wfile.write(body)

        def _json(self, status: int, value: Any) -> None:
            data = value.dump() if isinstance(value, Record) else [v.dump() if isinstance(v, Record) else v for v in value] if isinstance(value, list) else value
            self._send(status, json.dumps(data, indent=2).encode())

        def _body(self) -> bytes:
            n = int(self.headers.get("content-length") or 0)
            if n > MAX_BODY:
                raise ApiError(413, f"body over {MAX_BODY >> 20} MiB")
            return self.rfile.read(n)

        def _json_body(self) -> dict[str, Any]:
            try:
                body = json.loads(self._body() or b"{}")
            except ValueError as e:
                raise ApiError(400, f"not JSON: {e}") from None
            if not isinstance(body, dict):
                raise ApiError(400, "a JSON object")
            return body

        def _authorized(self) -> bool:
            got = self.headers.get("authorization", "")
            return hmac.compare_digest(got.encode(), f"Bearer {token}".encode())

        def _dispatch(self, method: str) -> None:
            parts, q = _query(self.path)
            if parts == ["healthz"]:
                return self._send(200, b"ok", "text/plain")
            try:
                if "group" in q:
                    raise ApiError(400,"unsupported selector; use tag")
                self.principal = None if self._authorized() else api.authenticate(self.headers.get("authorization", ""))
                self.scope = self.principal
                if self.principal is not None:
                    if q.get("namespace") is not None and q["namespace"] not in (self.principal["namespace"], self.principal.get("namespace_name")):
                        raise ApiError(403, "another namespace cannot be selected")
                    if q.get("tag") is not None and q["tag"] != self.principal["tag"]:
                        raise ApiError(403,"another tag cannot be selected")
                    allowed = ((method == "GET" and parts == ["apps"])
                        or (method in ("GET", "POST") and parts == ["runs"])
                        or (method == "PUT" and parts == ["uploads"])
                        or (len(parts) == 2 and parts[0] == "runs" and method in ("GET", "DELETE"))
                        or (len(parts) == 3 and parts[0] == "runs" and
                            ((method == "GET" and parts[2] in ("logs", "out.tar.gz"))
                             or (method == "POST" and parts[2] in ("cancel", "preempt")))))
                    if not allowed:
                        raise ApiError(403, "administrator access required")
                elif q.get("namespace"):
                    self.scope = api.admin_namespace(q["namespace"], token, q.get("tag"), all_tags="tag" not in q)
                if q.get("tag") is not None and self.scope is None:
                    raise ApiError(400,"tag requires namespace")
                if self.scope is not None and len(parts) >= 2 and parts[0] == "apps":
                    if not any(a["name"] == parts[1] for a in api.apps(scope=self.scope)):
                        raise ApiError(404, "no app in this namespace")
                if self.scope is not None and len(parts) >= 2 and parts[0] == "runs":
                    api.own_run(parts[1], self.scope)
                if self.scope is not None and method == "DELETE" and parts == ["runs"]:
                    raise ApiError(403, "bulk cleanup requires unfiltered administrator access")
                self._route(method, parts, q)
            except (Exception, SystemExit) as e:  # a refusal is a SystemExit (what run.sh prints), not an Exception
                if self.streaming:  # a broken stream: close it without its last chunk
                    print(f"{self.path}: stream broken off: {type(e).__name__}: {e}", flush=True)
                    self.close_connection = True
                    return
                self._error(e)

        def _error(self, e: BaseException) -> None:
            if isinstance(e, ApiError):
                self._send(e.status, str(e).encode(), "text/plain")
            elif isinstance(e, (runs.NotFound, runs.Conflict)):
                self._send(404 if isinstance(e, runs.NotFound) else 409, str(e.code).encode(), "text/plain")
            elif isinstance(e, (SystemExit, ModelError)):  # the same refusals run.sh prints
                self._send(400, str(e.code if isinstance(e, SystemExit) else e).encode(), "text/plain")
            else:  # report, keep serving
                self._send(500, f"{type(e).__name__}: {e}".encode(), "text/plain")

        def _route(self, method: str, parts: list[str], q: dict[str, str]) -> None:
            head, n = (parts[0] if parts else ""), len(parts)
            if (method, head, n) == ("POST", "runs", 1):
                return self._json(201, api.user_submit(self._json_body(), self.principal) if self.principal else api.submit(self._json_body(), namespace=self.scope["namespace"] if self.scope else None, tag=self.scope.get("tag") if self.scope else None))
            if (method, head, n) == ("GET", "apps", 1):
                return self._json(200, api.apps(self.principal, self.scope))
            if (method, head, n) == ("POST", "apps", 1):
                return self._json(201, api.add_app(self._json_body(), self.scope["namespace"] if self.scope else None))
            if (method, head, n) == ("DELETE", "apps", 2):
                return self._json(200, api.delete_app(parts[1]))
            if (method, head, n) == ("PUT", "apps", 3) and parts[2] == "priority":
                body = self._json_body()
                if set(body) != {"priority"}:
                    raise ApiError(400, "expected priority only")
                return self._json(200, api.set_app_priority(parts[1], body["priority"]))
            if (head, n) == ("apps", 3) and parts[2] == "disabled" and method in ("PUT", "DELETE"):
                return self._json(200, api.set_app_disabled(parts[1], method == "PUT"))
            if (method, head, n) == ("GET", "runs", 1):
                return self._json(200, runs.list_runs(api.scoped_args(self.scope), _int(q, "n", 20) or 0, active=_flag(q, "active"),
                                                      profile=q.get("profile")))
            if (method, head, n) == ("DELETE", "runs", 1) and "older_than" in q:
                return self._json(200, api.purge_runs(_float(q, "older_than")))
            if (method, head, n) == ("GET", "runs", 2):
                return self._json(200, runs.status(api.args(), parts[1]))
            if (method, head, n) == ("DELETE", "runs", 2):
                return self._json(200, api.manage_run("purge", parts[1]))
            if (method, head, n) == ("POST", "runs", 3) and parts[2] == "cancel":
                return self._json(200, api.manage_run("cancel", parts[1]))
            if (method, head, n) == ("POST", "runs", 3) and parts[2] == "preempt":
                return self._json(200, api.manage_run("preempt", parts[1]))
            if (method, head, n) == ("GET", "runs", 3):
                return self._logs_or_out(parts, q)
            if (method, head, n) == ("PUT", "uploads", 1):
                return self._json(201, api.upload(self._body(), self.scope["namespace"] if self.scope else None, self.scope.get("tag") if self.scope else None))
            if (method, head, n) == ("POST", "models", 1):
                return self._json(201, api.stage_model(self._json_body()))
            return self._missing()

        def _logs_or_out(self, parts: list[str], q: dict[str, str]) -> None:
            if parts[2] == "logs":
                chunk = runs.log_chunk(api.runs_dir, parts[1], _int(q, "rank", 0) or 0, _int(q, "offset", None),
                                       _int(q, "lines", 50) or 0)
                return self._json(200, chunk)
            if parts[2] == "out.tar.gz":
                runs.RunDir(api.runs_dir, parts[1])  # 404 before the headers go out
                self.send_response(200)
                self.send_header("content-type", "application/gzip")
                self.send_header("transfer-encoding", "chunked")
                self.send_header("connection", "close")
                self.close_connection = True
                self.end_headers()
                self.streaming = True
                body = _Chunked(self.wfile)
                runs.fetch(api.args(), parts[1], out=body)
                body.end()
                return None
            return self._missing()

        def _missing(self) -> None:
            raise ApiError(404, f"no such endpoint: {self.command} {self.path.split('?')[0]}")

        def do_GET(self) -> None:
            self._dispatch("GET")

        def do_POST(self) -> None:
            self._dispatch("POST")

        def do_PUT(self) -> None:
            self._dispatch("PUT")

        def do_DELETE(self) -> None:
            self._dispatch("DELETE")

    return Handler


class _RunsApi:
    """The runs API as a fleet service: serves on `listen` on the machine it was placed
    on, with that machine's profiles file and runs dir."""

    def __init__(self, spec: ReplicaSpec) -> None:
        self.spec, self.name = spec, spec.name
        svc = spec.service
        if not svc.profiles or not svc.runs_dir:
            raise RuntimeError(f"{self.name}: a runs-api service needs `profiles` and `runs_dir`")
        with open(spec.cluster.admin_token_path) as f:
            token = f.read().strip()
        host, _, port = svc.listen.rpartition(":")
        self.port = int(port)
        self.api = Api(svc.profiles, svc.runs_dir, spec.cluster.scheduler_admin)
        self.server = ThreadingHTTPServer((host or "127.0.0.1", self.port), handler(self.api, token))
        self.api.start_quota_dispatcher()
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        _watch(lambda: self.thread.is_alive() and self.api.quota_thread.is_alive(), self.shutdown)

    def health(self) -> dict[str, Any]:
        if _stopping():
            return Health(name=self.name, stopping=True).dump()
        return Health(name=self.name, spec=spec_hash(self.spec), machine=socket.gethostname(), port=self.port,
                      ready=self.thread.is_alive() and self.api.quota_thread.is_alive()).dump()

    def shutdown(self) -> None:
        _STOPPING["on"] = True
        self.api.quota_stop.set()
        self.server.shutdown()
        self.server.server_close()


RunsApi = ray.remote(_RunsApi)


def serve(profiles: str, runs_dir: str, listen: str, token_file: str) -> None:
    """Runs the API in the foreground (a driver of its own), for testing without a service."""
    with open(token_file) as f:
        token = f.read().strip()
    host, _, port = listen.rpartition(":")
    api = Api(profiles, runs_dir)
    server = ThreadingHTTPServer((host or "127.0.0.1", int(port)), handler(api, token))
    api.start_quota_dispatcher()
    print(f"runs API on {listen}", flush=True)
    server.serve_forever()
