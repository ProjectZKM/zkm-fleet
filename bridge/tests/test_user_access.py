"""User tokens are checked on every request; jobs and uploaded code stay owned."""
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from fleet_bridge import runs
from fleet_bridge.model import RunState
from fleet_bridge.runs_api import Api, handler


def record_launch(launched):
    def launch(spec, path):
        launched.append(spec.id)
        runs.RunDir(os.path.dirname(path), spec.id).update(launch_pending=False)
    return launch


@pytest.fixture
def servers(tmp_path, monkeypatch):
    etc = tmp_path / "etc"
    etc.mkdir()
    profiles = etc / "runs.yaml"
    profiles.write_text("profiles:\n  training:\n    namespace: '" + "1" * 32 + "'\n    image: python:3\n    max_gpus: 2\n    allow: [/tmp]\n  training-bob:\n    namespace: '" + "2" * 32 + "'\n    image: python:3\n    max_gpus: 2\n    allow: [/tmp]\n  training-system:\n    image: python:3\n    allow: [/tmp]\n  private:\n    image: python:3\n")
    identities = {"alice": {"id": "1" * 32, "address": "a" * 40, "namespace": "1" * 32, "namespace_name": "alice", "tenant": "eth", "apps": ["training"], "revoked_ms": None, "quota": {"gpus": None, "cpus": None}, "tag":None, "tag_quota":{"gpus":None,"cpus":None}},
                  "bob": {"id": "2" * 32, "address": "b" * 40, "namespace": "2" * 32, "namespace_name": "bob", "tenant": "eth", "apps": ["training-bob"], "revoked_ms": None, "quota": {"gpus": None, "cpus": None}, "tag":None, "tag_quota":{"gpus":None,"cpus":None}}}
    checks = []
    allocations = {}
    quota_lock = threading.Lock()

    class Scheduler(BaseHTTPRequestHandler):
        def reply(self, code, payload=None):
            self.send_response(code)
            self.end_headers()
            if payload is not None:
                self.wfile.write(json.dumps(payload).encode())

        def do_GET(self):
            token = self.headers.get("Authorization", "").removeprefix("Bearer ")
            if token == "admin" and self.path == "/admin/tokens":
                return self.reply(200, list(identities.values()))
            if token == "admin" and self.path.startswith("/admin/namespaces/"):
                from urllib.parse import urlsplit, parse_qs
                parsed = urlsplit(self.path)
                ref = parsed.path.split("/")[3]
                query = parse_qs(parsed.query, keep_blank_values=True)
                if parsed.path.endswith("/allocations"):
                    return self.reply(200, [{**a, "id":"run:" + key[1]} for key,a in allocations.items() if key[0] == ref])
                members = [c for c in identities.values() if ref in (c["namespace"], c["namespace_name"])]
                if not members and ref in ("system", "0" * 32):
                    return self.reply(200, {"id": "0" * 32, "name": "system", "quota": {"gpus": None, "cpus": None}, "tag_quota": {"gpus": None, "cpus": None}})
                if not members:
                    return self.reply(404,{})
                principal = members[0]
                payload = {"id":principal["namespace"], "name":principal["namespace_name"], "quota":principal["quota"]}
                if "tag" in query:
                    tag = query["tag"][0] or None
                    members = [c for c in members if c["tag"] == tag]
                    payload.update(tag=tag, tag_quota=members[0]["tag_quota"] if members else {"gpus":None,"cpus":None})
                return self.reply(200,payload)
            checks.append(token)
            self.reply(200 if token in identities else 401, identities.get(token, {}))

        def do_POST(self):
            assert self.headers["Authorization"] == "Bearer admin"
            if self.path == "/admin/preemption/plan":
                # The scheduler owns the policy decision. Tests that exercise a
                # selected victim replace Api.scheduler with an explicit plan.
                return self.reply(200, {"possible": False, "runs": []})
            namespace = self.path.split("/")[3]
            claim = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            principal = next(c for c in identities.values() if c["namespace"] == namespace)
            key = namespace, claim["id"]
            with quota_lock:
                if not claim["restore"]:
                    for resource in ("gpus", "cpus"):
                        cap = principal["quota"][resource]
                        used = sum(a[resource] for k, a in allocations.items() if k[0] == namespace and k != key)
                        if cap is not None and claim[resource] > cap:
                            return self.reply(422, "job exceeds quota")
                        if cap is not None and used + claim[resource] > cap:
                            return self.reply(429, "quota occupied")
                    tag = claim.get("tag")
                    member = next(c for c in identities.values() if c["namespace"] == namespace and c["tag"] == tag)
                    for resource in ("gpus","cpus"):
                        cap = member["tag_quota"][resource]
                        used = sum(a[resource] for k,a in allocations.items() if k[0] == namespace and k != key and a.get("tag") == tag)
                        if cap is not None and claim[resource] > cap:
                            return self.reply(422,"job exceeds tag quota")
                        if cap is not None and used + claim[resource] > cap:
                            return self.reply(429,"tag quota occupied")
                allocations[key] = claim
            self.reply(200, claim)

        def do_DELETE(self):
            assert self.headers["Authorization"] == "Bearer admin"
            parts = self.path.split("/")
            with quota_lock:
                allocations.pop((parts[3], parts[5]), None)
            self.reply(204)

        def log_message(self, *args):
            pass

    scheduler = ThreadingHTTPServer(("127.0.0.1", 0), Scheduler)
    scheduler_url = f"http://127.0.0.1:{scheduler.server_port}"
    api = Api(str(profiles), str(tmp_path / "runs"), scheduler_url)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler(api, "admin"))
    monkeypatch.setattr(runs, "_placement", lambda a: runs.Placement(nodes=a.nodes, machines=[], machine_resources=[]))
    monkeypatch.setattr(runs, "launch", record_launch([]))
    monkeypatch.setattr(runs, "prepare_launch", lambda spec: True)
    monkeypatch.setattr(runs, "discard_placement", lambda run_id: None)
    monkeypatch.setattr(runs, "confirm_cleanup", lambda run: True)
    monkeypatch.setattr(runs, "_going", lambda run_id: False)
    from fleet_bridge import rayapi
    monkeypatch.setattr(rayapi, "free_by_node_ip", lambda: {})
    for s in (scheduler, server):
        threading.Thread(target=s.serve_forever, daemon=True).start()

    def request(token, method, path, body=None):
        data = body if isinstance(body, bytes) else json.dumps(body).encode() if body is not None else None
        req = Request(f"http://127.0.0.1:{server.server_port}{path}", data=data, method=method,
                      headers={"Authorization": f"Bearer {token}"})
        try:
            with urlopen(req, timeout=5) as r:
                raw = r.read()
                return r.status, json.loads(raw) if r.headers.get_content_type() == "application/json" else raw
        except HTTPError as e:
            return e.code, e.read()

    yield api, request, identities, checks
    for s in (server, scheduler):
        s.shutdown()
        s.server_close()


def submit(request, token, **extra):
    return request(token, "POST", "/runs", {"profile": "training-bob" if token == "bob" else "training-system" if token == "admin" else "training", "gpus": 0, "cpus": 1,
                                             "command": ["python", "--version"], **extra})


def test_apps_require_existing_namespace_and_reject_cross_namespace_use(servers):
    api, request, identities, _ = servers
    body = {"name": "new-app", "image": "python:3", "allow": ["/tmp"]}
    assert request("admin", "POST", "/apps", body)[0] == 400
    assert request("admin", "POST", "/apps?namespace=missing", body)[0] == 404
    assert request("alice", "POST", "/apps?namespace=alice", body)[0] == 403
    assert "new-app" not in runs.load_config(api.profiles).profiles
    code, app = request("admin", "POST", "/apps?namespace=alice", body)
    assert code == 201
    assert app["namespace"] == identities["alice"]["namespace"]
    saved = runs.load_config(api.profiles).profiles["new-app"]
    assert saved.namespace == app["namespace"]
    # A mistaken grant cannot change ownership or move billing to another namespace.
    identities["bob"]["apps"].extend(["training", "new-app"])
    assert submit(request, "bob", profile="training")[0] == 403
    assert submit(request, "bob", profile="new-app")[0] == 403
    assert {a["name"] for a in request("bob", "GET", "/apps")[1]} == {"training-bob"}
    assert request("admin", "PUT", "/apps/training/disabled?namespace=bob")[0] == 404
    assert request("admin", "DELETE", "/apps/training?namespace=bob")[0] == 404
    assert not runs.load_config(api.profiles).profiles["training"].disabled
    code, job = request("admin", "POST", "/runs", {
        "profile": "new-app", "gpus": 0, "cpus": 1, "command": ["true"]})
    assert code == 201
    assert runs.RunDir(api.runs_dir, job["id"]).spec().namespace == app["namespace"]
    identities["alice"]["quota"] = {"gpus": 1, "cpus": 2}
    assert request("admin", "POST", "/runs", {
        "profile": "new-app", "nodes": 2, "gpus": 1, "cpus": 1, "command": ["true"]})[0] == 422
    code, job = request("admin", "POST", "/runs", {
        "profile": "new-app", "nodes": 2, "gpus": 0, "cpus": 1, "command": ["true"]})
    assert code == 201
    assert runs.RunDir(api.runs_dir, job["id"]).spec().nodes == 2


def test_owned_runs_and_counts_and_no_owner_spoofing(servers):
    api, request, _, _ = servers
    code, alice = submit(request, "alice")
    assert code == 201
    code, bob = submit(request, "bob")
    assert code == 201
    assert runs.RunDir(api.runs_dir, alice["id"]).spec().owner == "1" * 32
    assert request("alice", "GET", f'/runs/{alice["id"]}')[0] == 200
    for method, suffix in [("GET", ""), ("GET", "/logs"), ("GET", "/out.tar.gz"),
                           ("POST", "/cancel"), ("POST", "/preempt"), ("DELETE", "")]:
        code, body = request("bob", method, f'/runs/{alice["id"]}{suffix}')
        assert (code, body) == (404, b"no such run")
    assert [j["id"] for j in request("bob", "GET", "/runs?n=1")[1]] == [bob["id"]]
    assert len(request("admin", "GET", "/runs")[1]) == 2
    apps = request("alice", "GET", "/apps")[1]
    assert len(apps) == 1 and apps[0]["name"] == "training" and apps[0]["jobs"] == 1
    assert submit(request, "alice", profile="private")[0] == 403
    assert submit(request, "alice", owner="b" * 40)[0] == 400
    assert submit(request, "alice", after=bob["id"])[0] == 404
    assert submit(request, "alice", nodes=2)[0] == 400
    spec = runs.RunDir(api.runs_dir, alice["id"]).spec()
    command = runs.docker_command(spec, [])
    assert command[command.index("--network") + 1] == "none"
    for method, path in [("POST", "/apps"), ("DELETE", "/apps/training"),
                         ("PUT", "/apps/training/disabled"), ("POST", "/models"),
                         ("DELETE", "/runs?older_than=0")]:
        assert request("alice", method, path, {})[0] == 403


def test_uploads_and_paths_cannot_cross_users(servers, tmp_path):
    api, request, _, _ = servers
    a = request("alice", "PUT", "/uploads", b"archive")[1]
    b = request("bob", "PUT", "/uploads", b"archive")[1]
    assert a["sha256"] == b["sha256"] and a["path"] != b["path"]
    assert submit(request, "bob", code=a["path"])[0] == 403
    for mount in [api.runs_dir, os.path.dirname(api.runs_dir), os.path.dirname(api.profiles)]:
        assert submit(request, "alice", mount=[mount + ":ro"])[0] == 403
    link = tmp_path / "job-link"
    link.symlink_to(api.runs_dir, target_is_directory=True)
    assert submit(request, "alice", mount=[str(link) + ":ro"])[0] == 403
    assert submit(request, "alice", code="git:" + api.runs_dir + "@HEAD")[0] == 403
    assert submit(request, "alice", log=os.path.join(api.runs_dir, "foreign.log"))[0] == 403


def test_revocation_rechecks_each_request_and_fails_closed(servers):
    _, request, identities, checks = servers
    code, job = submit(request, "alice")
    assert code == 201
    assert request("alice", "GET", f'/runs/{job["id"]}')[0] == 200
    del identities["alice"]
    for method, path in [("GET", f'/runs/{job["id"]}'), ("GET", f'/runs/{job["id"]}/logs'),
                         ("POST", f'/runs/{job["id"]}/cancel'), ("PUT", "/uploads")]:
        assert request("alice", method, path, b"x")[0] == 401
    assert checks.count("alice") == 6
    assert request("admin", "GET", f'/runs/{job["id"]}')[0] == 200
    assert request("bad", "GET", "/runs")[0] == 401
    identities["alice"] = {"address": "../escape", "apps": ["training"]}
    assert request("alice", "GET", "/runs")[0] == 401


def test_operator_runs_are_isolated_and_own_outputs_and_purge_work(servers):
    api, request, _, _ = servers
    code, operator_job = submit(request, "admin")
    assert code == 201
    assert request("alice", "GET", f'/runs/{operator_job["id"]}')[0] == 404
    code, own = submit(request, "alice")
    assert code == 201
    run = runs.RunDir(api.runs_dir, own["id"])
    with open(os.path.join(run.out, "result.txt"), "w") as f:
        f.write("done")
    assert request("alice", "GET", f'/runs/{own["id"]}/out.tar.gz')[0] == 200
    status = run.status()
    status.state = RunState.SUCCEEDED
    status.write(run.status_path)
    assert request("alice", "DELETE", f'/runs/{own["id"]}')[0] == 200
    assert request("alice", "GET", f'/runs/{own["id"]}')[0] == 404


def test_own_archive_upload_is_captured_and_old_uploads_can_be_cleaned(servers):
    import io
    import tarfile
    import time

    api, request, _, _ = servers
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w:gz") as tar:
        info = tarfile.TarInfo("hello.py")
        data = b"print('hello')\n"
        info.size = len(data)
        tar.addfile(info, io.BytesIO(data))
    code, upload = request("alice", "PUT", "/uploads", archive.getvalue())
    assert code == 201
    code, job = submit(request, "alice", code=upload["path"])
    assert code == 201
    spec = runs.RunDir(api.runs_dir, job["id"]).spec()
    assert spec.owner == "1" * 32
    with open(os.path.join(spec.code_dir, "hello.py"), "rb") as captured:
        assert captured.read() == data
    old = time.time() - 3 * 86400
    os.utime(upload["path"], (old, old))
    assert request("admin", "DELETE", "/runs?older_than=1")[0] == 200
    assert not os.path.exists(upload["path"])


def test_token_validation_outage_fails_closed(servers, monkeypatch):
    from fleet_bridge import runs_api
    api, _, _, _ = servers

    def unavailable(*args, **kwargs):
        raise OSError("scheduler unavailable")

    monkeypatch.setattr(runs_api.urllib.request, "urlopen", unavailable)
    with pytest.raises(runs_api.ApiError) as error:
        api.authenticate("Bearer alice")
    assert error.value.status == 503


def test_refresh_preserves_runs_and_uploads(servers):
    import io
    import tarfile

    api, request, identities, _ = servers
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w:gz") as tar:
        info = tarfile.TarInfo("hello.py")
        info.size = 1
        tar.addfile(info, io.BytesIO(b"x"))
    code, upload = request("alice", "PUT", "/uploads", archive.getvalue())
    assert code == 201 and "/namespaces/" + "1" * 32 + "/" in upload["path"]
    code, job = submit(request, "alice", code=upload["path"])
    assert code == 201
    run = runs.RunDir(api.runs_dir, job["id"])
    identities["replacement"] = {**identities["alice"], "id": "3" * 32, "address": "c" * 40}
    del identities["alice"]
    assert request("alice", "GET", f'/runs/{job["id"]}')[0] == 401
    assert request("replacement", "GET", f'/runs/{job["id"]}')[0] == 200
    assert request("bob", "GET", f'/runs/{job["id"]}')[0] == 404
    listed = request("replacement", "GET", "/runs")[1]
    assert [(j["id"], j["namespace"]) for j in listed] == [(job["id"], "1" * 32)]
    assert submit(request, "replacement", code=upload["path"])[0] == 201
    assert submit(request, "bob", code=upload["path"])[0] == 403
    new_upload = request("replacement", "PUT", "/uploads", archive.getvalue())[1]
    assert new_upload["path"] == upload["path"]
    assert run.spec().owner == "1" * 32


def test_namespace_selectors_are_verified_for_users_and_operators(servers):
    api, request, _, _ = servers
    _, alice = submit(request, "alice")
    _, bob = submit(request, "bob")
    assert request("alice", "GET", "/runs?namespace=bob")[0] == 403
    assert request("alice", "GET", "/runs?namespace=alice")[0] == 200
    assert [r["id"] for r in request("admin", "GET", "/runs?namespace=alice")[1]] == [alice["id"]]
    assert request("admin", "GET", f'/runs/{bob["id"]}?namespace=alice')[0] == 404
    assert request("admin", "GET", "/runs?namespace=missing")[0] == 404
    assert request("admin", "DELETE", "/runs?namespace=alice&older_than=1")[0] == 403
    code, job = request("admin", "POST", "/runs?namespace=alice", {
        "profile": "training", "gpus": 0, "cpus": 1, "command": ["true"]})
    assert code == 201
    assert runs.RunDir(api.runs_dir, job["id"]).spec().namespace == "1" * 32
    assert request("alice", "GET", f'/runs/{job["id"]}')[0] == 200
    assert request("bob", "GET", f'/runs/{job["id"]}')[0] == 404


def test_namespace_quota_queues_runs_and_is_shared_across_tokens(servers, monkeypatch):
    api, request, identities, _ = servers
    identities["alice"]["quota"] = {"gpus": 1, "cpus": 2}
    identities["replacement"] = {**identities["alice"], "id": "3" * 32, "address": "c" * 40}
    launched = []
    monkeypatch.setattr(runs, "launch", record_launch(launched))
    code, first = submit(request, "alice", gpus=1, cpus=2)
    assert code == 201 and first["state"] == "queued"
    code, waiting = submit(request, "replacement", gpus=0, cpus=1)
    assert code == 201 and waiting["state"] == "waiting_quota"
    assert request("replacement", "GET", f'/runs/{waiting["id"]}')[1]["state"] == "waiting_quota"
    assert launched == [first["id"]]
    assert submit(request, "alice", gpus=2)[0] == 422
    assert submit(request, "alice", cpus=3)[0] == 422
    assert submit(request, "alice", quota={"gpus": 100})[0] == 400
    # Time does not release an admitted job's units.
    run = runs.RunDir(api.runs_dir, first["id"])
    run.update(state=RunState.RUNNING, started_at=1)
    api.dispatch_quotas()
    assert launched == [first["id"]]
    restarted = Api(api.profiles, api.runs_dir, api.scheduler_url)
    handler(restarted, "admin")
    restarted.dispatch_quotas()
    assert launched == [first["id"]], "API restart must reuse the existing reservation without relaunching"
    _, bob = submit(request, "bob", gpus=1, cpus=2)
    assert bob["id"] in launched
    run.update(state=RunState.SUCCEEDED, finished_at=2)
    api.dispatch_quotas()
    assert waiting["id"] in launched
    assert runs.RunDir(api.runs_dir, waiting["id"]).status().state is RunState.QUEUED
    assert launched.count(waiting["id"]) == 1


def test_fractional_gpu_quota_and_dependency_waiters_hold_no_units(servers, monkeypatch):
    api, request, identities, _ = servers
    identities["alice"]["quota"] = {"gpus": 1, "cpus": 4}
    launched = []
    monkeypatch.setattr(runs, "launch", record_launch(launched))
    _, first = submit(request, "alice", gpus=0.5, gpu_mem=1)
    _, second = submit(request, "alice", gpus=0.5, gpu_mem=1)
    _, dependent = submit(request, "alice", gpus=0.5, gpu_mem=1, after=first["id"])
    assert dependent["state"] == "waiting_dependency"
    _, waiting = submit(request, "alice", gpus=0.5, gpu_mem=1)
    assert waiting["state"] == "waiting_quota"
    assert launched == [first["id"], second["id"]]
    assert not runs.purge(api.args(), dependent["id"])[0].purged
    assert not runs.purge(api.args(), waiting["id"])[0].purged
    runs.RunDir(api.runs_dir, first["id"]).update(state=RunState.SUCCEEDED, finished_at=2)
    api.dispatch_quotas()
    assert dependent["id"] in launched and waiting["id"] not in launched
    runs.RunDir(api.runs_dir, second["id"]).update(state=RunState.CANCELLED, finished_at=3)
    api.dispatch_quotas()
    assert waiting["id"] in launched


def test_quota_service_outage_keeps_accepted_work_waiting(servers, monkeypatch):
    from fleet_bridge.runs_api import ApiError
    api, request, _, _ = servers
    launched = []
    monkeypatch.setattr(runs, "launch", record_launch(launched))

    original_scheduler = api.scheduler
    def unavailable(method, *args, **kwargs):
        if method == "GET":
            return original_scheduler(method,*args,**kwargs)
        raise ApiError(503, "quota service unavailable")

    monkeypatch.setattr(api, "scheduler", unavailable)
    code, job = submit(request, "alice")
    assert code == 201 and job["state"] == "waiting_quota"
    assert launched == []
    assert request("alice", "GET", f'/runs/{job["id"]}')[1]["state"] == "waiting_quota"


def test_preemption_releases_quota_and_requeue_and_cancel_obey_it(servers, monkeypatch):
    api, request, identities, _ = servers
    identities["alice"]["quota"] = {"gpus": 1, "cpus": 2}
    launched = []
    monkeypatch.setattr(runs, "launch", record_launch(launched))
    monkeypatch.setattr(runs, "_alive", lambda name: True)
    monkeypatch.setattr(runs, "_stop_ranks", lambda *args: None)
    monkeypatch.setattr(runs, "_end_actors", lambda *args: None)
    monkeypatch.setattr(runs, "_on_run_machine", lambda *args: "")
    monkeypatch.setattr(runs.ray.util, "get_placement_group", lambda name: None)
    monkeypatch.setattr(runs, "remove_placement_group", lambda tag: None)
    _, first = submit(request, "alice", gpus=1)
    _, second = submit(request, "alice", gpus=1)
    run = runs.RunDir(api.runs_dir, first["id"])
    run.update(state=RunState.RUNNING, gpus=["0"], container="fake")
    stopped = runs.preempt(api.runs_dir, first["id"], "service needs cards", requeue=False)
    assert stopped.state is RunState.PREEMPTED
    api.dispatch_quotas()
    assert launched == [first["id"], second["id"]]
    runs.requeue_run(api.runs_dir, first["id"])
    api.dispatch_quotas()
    assert run.status().state is RunState.WAITING_QUOTA
    assert request("alice", "POST", f'/runs/{second["id"]}/cancel')[0] == 200
    api.dispatch_quotas()
    assert launched == [first["id"], second["id"], first["id"]]


def test_launch_intent_recovers_after_crash_without_stranding_quota(servers, monkeypatch):
    api, request, identities, _ = servers
    identities['alice']['quota'] = {'gpus': 1, 'cpus': 2}
    def crash(*args):
        raise KeyboardInterrupt('API dies before actor creation')
    monkeypatch.setattr(runs, 'launch', crash)
    with pytest.raises(KeyboardInterrupt):
        api.user_submit({'profile':'training','gpus':1,'cpus':1,'command':['true']},identities['alice'])
    run_id = next(n for n in os.listdir(api.runs_dir) if not n.startswith('.'))
    assert runs.RunDir(api.runs_dir,run_id).status().launch_pending
    launched = []
    monkeypatch.setattr(runs,'launch',record_launch(launched))
    restarted = Api(api.profiles,api.runs_dir,api.scheduler_url)
    handler(restarted,'admin')
    restarted.dispatch_quotas()
    restarted.dispatch_quotas()
    assert launched == [run_id]
    assert not runs.RunDir(api.runs_dir,run_id).status().launch_pending
    assert submit(request,'alice',gpus=1)[1]['state'] == 'waiting_quota'


def test_existing_actor_acknowledges_interrupted_launch_without_duplicate(servers,monkeypatch):
    api, request, _, _ = servers
    _, job = submit(request,'alice')
    run = runs.RunDir(api.runs_dir,job['id'])
    run.update(launch_pending=True)
    monkeypatch.setattr(runs,'_going',lambda run_id: True)
    launched=[]
    monkeypatch.setattr(runs,'launch',record_launch(launched))
    api.dispatch_quotas()
    assert not run.status().launch_pending and launched == []


def test_unknown_cleanup_keeps_quota_and_record_until_confirmed(servers,monkeypatch):
    api, request, identities, _ = servers
    identities['alice']['quota']={'gpus':1,'cpus':2}
    _, first = submit(request,'alice',gpus=1)
    _, second = submit(request,'alice',gpus=1)
    run=runs.RunDir(api.runs_dir,first['id'])
    run.update(state=RunState.CANCELLED)
    monkeypatch.setattr(runs,'confirm_cleanup',lambda run: False)
    api.dispatch_quotas()
    assert not run.status().quota_released
    assert runs.RunDir(api.runs_dir,second['id']).status().state is RunState.WAITING_QUOTA
    assert request('alice','DELETE',f"/runs/{first['id']}")[0] == 409
    assert not runs.purge(api.args(),first['id'])[0].purged
    monkeypatch.setattr(runs,'confirm_cleanup',lambda run: True)
    api.dispatch_quotas()
    assert run.status().quota_released
    assert runs.RunDir(api.runs_dir,second['id']).status().state is RunState.QUEUED


def test_physical_waiter_holds_no_quota_and_does_not_block_proofs(servers,monkeypatch):
    api, request, identities, _ = servers
    identities['alice']['quota']={'gpus':1,'cpus':2}
    monkeypatch.setattr(runs,'prepare_launch',lambda spec: spec.gpus == 0)
    _, blocked = submit(request,'alice',gpus=1)
    run=runs.RunDir(api.runs_dir,blocked['id'])
    assert run.status().state is RunState.WAITING_QUOTA and run.status().quota_released
    # Stand in for a proof admitted by the same scheduler ledger: it still fits.
    api.scheduler('POST',f"/namespaces/{identities['alice']['namespace']}/allocations",{'id':'proof-claim','tag':None,'gpus':1,'cpus':1,'restore':False})
    monkeypatch.setattr(runs,'prepare_launch',lambda spec: True)
    api.dispatch_quotas()
    assert run.status().state is RunState.WAITING_QUOTA
    api.release_quota(identities['alice']['namespace'],'proof-claim')
    api.dispatch_quotas()
    assert run.status().state is RunState.QUEUED


def test_dispatch_uses_active_index_and_releases_terminal_history_once(servers,monkeypatch):
    api,request,_,_=servers
    _,job=submit(request,'alice')
    run=runs.RunDir(api.runs_dir,job['id'])
    run.update(state=RunState.SUCCEEDED)
    calls=[]
    original=api.scheduler
    def observe(method,path,*args):
        calls.append((method,path))
        return original(method,path,*args)
    monkeypatch.setattr(api,'scheduler',observe)
    api.dispatch_quotas()
    assert len([c for c in calls if c[0]=='DELETE']) == 1
    calls.clear()
    original_spec=runs.RunDir.spec
    def spec(r):
        assert r.id != job['id'], 'completed history must not be read on a normal poll'
        return original_spec(r)
    monkeypatch.setattr(runs.RunDir,'spec',spec)
    api.dispatch_quotas()
    submit(request,'alice')
    assert not any(method=='DELETE' and path.endswith(job['id']) for method,path in calls)


def test_tags_isolate_runs_uploads_and_dependencies_and_share_namespace_cap(servers,monkeypatch):
    api,request,identities,_=servers
    # Two tags of the same billing namespace with independent tag limits.
    identities['alice']['tag']='research'
    identities['alice']['tag_quota']={'gpus':1,'cpus':2}
    identities['alice']['quota']={'gpus':2,'cpus':4}
    identities['other']={**identities['alice'],'address':'c'*40,'tag':'production', 'tag_quota':{'gpus':2,'cpus':4}}
    identities['replacement']={**identities['alice'],'address':'d'*40}
    _,a=submit(request,'alice',gpus=1)
    _,b=submit(request,'other',gpus=1)
    assert request('other','GET',f"/runs/{a['id']}")[0] == 404
    assert request('alice','GET',f"/runs/{b['id']}")[0] == 404
    assert request('replacement','GET',f"/runs/{a['id']}")[0] == 200
    assert request('alice','GET','/runs?group=production')[0] == 400
    assert request('alice','GET','/runs?tag=production')[0] == 403
    assert request('alice','GET','/runs?tag=research')[0] == 200
    assert request('alice','GET','/runs?tag=research&group=production')[0] == 400
    assert submit(request,'alice',after=b['id'])[0] == 404
    assert submit(request,'alice',gpus=2)[0] == 422
    # Production has tag room, but the namespace's two GPUs are occupied.
    assert submit(request,'other',gpus=1)[1]['state'] == 'waiting_quota'
    assert submit(request,'alice',tag='production')[0] == 400
    import io,tarfile
    data=io.BytesIO()
    with tarfile.open(fileobj=data,mode='w:gz') as t:
        info=tarfile.TarInfo('main.py');info.size=4;t.addfile(info,io.BytesIO(b'pass'))
    _,upload=request('alice','PUT','/uploads',data.getvalue())
    assert '/tags/research/' in upload['path']
    assert submit(request,'other',code=upload['path'])[0] == 403
    assert submit(request,'replacement',code=upload['path'])[0] == 201
    own=request('alice','GET','/runs')[1]
    assert all(j['tag']=='research' for j in own)
    admin=request('admin','GET','/runs?namespace=alice&tag=production')[1]
    assert all(j['tag']=='production' for j in admin)


def test_untagged_access_is_exact_and_uploads_are_tag_scoped(servers):
    import io
    import tarfile
    api,request,identities,_=servers
    identities['tagged']={**identities['alice'],'address':'c'*40,'tag':'research'}
    identities['named-default']={**identities['alice'],'address':'d'*40,'tag':'default'}
    _,untagged=submit(request,'alice')
    _,tagged=submit(request,'tagged')
    status=request('alice','GET',f"/runs/{untagged['id']}")[1]
    assert status['tag'] is None
    assert 'group' not in status
    assert request('alice','GET',f"/runs/{tagged['id']}")[0] == 404
    assert request('tagged','GET',f"/runs/{untagged['id']}")[0] == 404
    assert [r['id'] for r in request('alice','GET','/runs')[1]] == [untagged['id']]
    assert len(request('admin','GET','/runs?namespace=alice')[1]) == 2
    assert submit(request,'alice',namespace_all_tags=True)[0] == 400
    for job, tag in [(untagged, None), (tagged, 'research')]:
        directory = runs.RunDir(api.runs_dir, job['id'])
        assert directory.spec().tag == tag
        directory.update(detail='tag retained')
        saved = json.load(open(os.path.join(directory.path, 'status.json')))
        assert saved['tag'] == tag
    data=io.BytesIO()
    with tarfile.open(fileobj=data,mode='w:gz') as archive:
        info=tarfile.TarInfo('main.py');info.size=4;archive.addfile(info,io.BytesIO(b'pass'))
    _,uploaded=request('alice','PUT','/uploads',data.getvalue())
    assert '/untagged/' in uploaded['path']
    assert submit(request,'alice',code=uploaded['path'])[0] == 201
    assert submit(request,'named-default',code=uploaded['path'])[0] == 403
    # A literal "default" tag is distinct from untagged work.
    assert request('named-default','GET',f"/runs/{untagged['id']}")[0] == 404


def test_lost_reservation_reply_is_released_if_physical_capacity_disappears(servers,monkeypatch):
    from fleet_bridge.runs_api import ApiError
    api,request,identities,_=servers
    identities['alice']['quota']={'gpus':1,'cpus':2}
    original=api.scheduler
    def lost_reply(method,path,body=None):
        result=original(method,path,body)
        if method=='POST':
            raise ApiError(503,'reservation committed but reply was lost')
        return result
    monkeypatch.setattr(api,'scheduler',lost_reply)
    _,job=submit(request,'alice',gpus=1)
    run=runs.RunDir(api.runs_dir,job['id'])
    assert run.status().launch_pending
    monkeypatch.setattr(api,'scheduler',original)
    monkeypatch.setattr(runs,'prepare_launch',lambda spec:False)
    api.dispatch_quotas()
    assert run.status().quota_released and not run.status().launch_pending
    # The uncertain reservation must not be stranded behind physical waiting.
    original('POST',f"/namespaces/{identities['alice']['namespace']}/allocations",{'id':'proof','tag':None,'gpus':1,'cpus':1,'restore':False})


def test_jobs_inherit_app_priority_and_users_cannot_override_it(servers):
    api, request, _, _ = servers
    assert request("alice", "PUT", "/apps/training/priority", {"priority":10})[0] == 403
    assert submit(request, "alice", priority=100)[0] == 400
    assert request("admin", "PUT", "/apps/training/priority?namespace=bob", {"priority":10})[0] == 404
    for bad in (True, 1.5, "10", 2**31):
        assert request("admin", "PUT", "/apps/training/priority", {"priority":bad})[0] == 400
    assert request("admin", "PUT", "/apps/training/priority", {"priority":10})[0] == 200
    code, job = submit(request, "alice")
    assert code == 201 and job["priority"] == 10
    run = runs.RunDir(api.runs_dir, job["id"])
    assert run.spec().profile.priority == 10
    assert request("admin", "PUT", "/apps/training/priority", {"priority":-2})[0] == 200
    assert run.spec().profile.priority == 10
    assert request("alice", "GET", "/runs/" + run.id)[1]["priority"] == 10
    assert request("alice", "GET", "/apps")[1][0]["priority"] == -2
    assert submit(request, "alice")[1]["priority"] == -2


def test_waiters_use_inherited_priority_then_fifo(servers, monkeypatch):
    api, request, _, _ = servers
    monkeypatch.setattr(runs, "prepare_launch", lambda spec: False)
    low = submit(request, "alice")[1]
    request("admin", "PUT", "/apps/training/priority", {"priority":10})
    first = submit(request, "alice")[1]
    second = submit(request, "alice")[1]
    request("admin", "PUT", "/apps/training/priority", {"priority":-10})
    launched = []
    monkeypatch.setattr(runs, "prepare_launch", lambda spec: True)
    monkeypatch.setattr(runs, "launch", record_launch(launched))
    api.dispatch_quotas()
    assert launched == [first["id"], second["id"], low["id"]]


def test_high_priority_job_preempts_only_own_namespace_and_waits_for_cleanup(servers, monkeypatch):
    from fleet_bridge import rayapi
    api, request, identities, _ = servers
    identities["alice"]["quota"] = {"gpus":1,"cpus":2}
    low = submit(request, "alice", gpus=1)[1]
    victim = runs.RunDir(api.runs_dir, low["id"])
    victim.update(state=RunState.RUNNING, node_ip="worker", started_at=1)
    foreign = runs.RunDir(api.runs_dir, submit(request, "bob", gpus=1)[1]["id"])
    foreign.update(state=RunState.RUNNING, node_ip="other-worker", started_at=1)
    request("admin", "PUT", "/apps/training/priority", {"priority":10})
    # Actual preempt(), with uncertain Docker cleanup and retained quota.
    monkeypatch.setattr(runs, "_alive", lambda _: True)
    monkeypatch.setattr(runs, "_stop_ranks", lambda *a: None)
    monkeypatch.setattr(runs, "_end_actors", lambda *a: None)
    monkeypatch.setattr(runs, "confirm_cleanup", lambda run: run.id != victim.id)
    monkeypatch.setattr(rayapi, "free_by_node_ip", lambda: {"worker":(1,8)})
    original_scheduler = api.scheduler
    def scheduler_with_plan(method, path, body=None):
        if method == "POST" and path == "/preemption/plan":
            assert body["namespace"] == identities["alice"]["namespace"]
            assert body["candidates"]
            return {"possible": True, "runs": [victim.id]}
        return original_scheduler(method, path, body)
    monkeypatch.setattr(api, "scheduler", scheduler_with_plan)
    code, high = submit(request, "alice", gpus=1)
    assert code == 201 and high["state"] == "waiting_quota"
    assert victim.status().state is RunState.PREEMPTED
    assert victim.status().preempt_requeue and not victim.status().quota_released
    assert foreign.status().state is RunState.RUNNING
    # Recovery confirms cleanup, then admits high before restarting low.
    monkeypatch.setattr(runs, "confirm_cleanup", lambda run: True)
    api.dispatch_quotas()
    assert runs.RunDir(api.runs_dir, high["id"]).status().state is RunState.QUEUED
    assert victim.status().state is RunState.WAITING_QUOTA
    assert victim.status().quota_released


def test_requeue_intent_survives_restart_after_quota_release(servers, monkeypatch):
    api, request, _, _ = servers
    job = submit(request, "alice")[1]
    run = runs.RunDir(api.runs_dir, job["id"])
    run.update(state=RunState.PREEMPTED, preempt_requeue=True)
    assert api.finish_quota(run, run.spec().namespace)
    assert os.path.exists(os.path.join(api.runs_dir, runs.QUOTA_ACTIVE_DIR, run.id))
    # Restart in the gap between releasing quota and moving back to waiting_quota.
    restarted = Api(api.profiles, api.runs_dir, api.scheduler_url)
    restarted.admin_token = "admin"
    monkeypatch.setattr(runs, "prepare_launch", lambda _: False)
    restarted.dispatch_quotas()
    assert run.status().state is RunState.WAITING_QUOTA
    assert run.status().quota_released
    assert not run.status().preempt_requeue
