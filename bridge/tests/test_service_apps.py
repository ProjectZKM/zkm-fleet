"""Persistent apps release their actors and stay disabled across reconciliation."""
import json
from unittest.mock import Mock

import pytest
import yaml

from fleet_bridge import reconcile
from fleet_bridge.model import ApplyReport
from fleet_bridge.runs_api import Api, ApiError


@pytest.fixture
def api(tmp_path):
    (tmp_path / "runs.yaml").write_text("profiles: {}\n")
    (tmp_path / "services.yaml").write_text(yaml.safe_dump({"services": [
        {"name": "comfyui", "kind": "app", "replicas": 1, "gpus": 1},
        {"name": "sam3", "kind": "app", "replicas": 1, "gpus": 1},
    ]}))
    return Api(str(tmp_path / "runs.yaml"), str(tmp_path / "runs"))


def test_disable_is_persisted_before_targeted_stop_and_enable_restores(api, monkeypatch):
    calls = []
    def apply(cfg, only):
        saved = yaml.safe_load(open(api.profiles.replace("runs.yaml", "services.yaml")))
        assert saved["services"][0]["disabled"] == cfg.services[0].disabled
        assert only == ["comfyui"]
        calls.append(set(reconcile.replica_specs(cfg)))
        return ApplyReport()
    monkeypatch.setattr(reconcile, "reconcile_once", apply)
    result = api.set_app_disabled("comfyui", True)
    assert result["disabled"]
    assert result["type"] == "run"
    assert result["persistent"] is True
    assert api.apps()[0]["disabled"]
    assert api.apps()[0]["type"] == "run"
    assert api.apps()[0]["persistent"] is True
    assert not api.set_app_disabled("comfyui", False)["disabled"]
    assert calls == [{"sam3-0"}, {"comfyui-0", "sam3-0"}]


def test_disabled_app_is_removed_without_touching_other_services(api, monkeypatch):
    api_path = api.profiles.replace("runs.yaml", "services.yaml")
    data = yaml.safe_load(open(api_path))
    data["services"][0]["disabled"] = True
    monkeypatch.setattr(reconcile, "running", lambda: {"comfyui-0": Mock(), "sam3-0": Mock()})
    stop = Mock()
    monkeypatch.setattr(reconcile, "stop", stop)
    monkeypatch.setattr(reconcile.Scheduler, "drain", lambda *args: False)
    report = reconcile.reconcile_once(reconcile.load_services(data), only=["comfyui"])
    assert stop.call_count == 1
    assert stop.call_args.args[0] == "comfyui-0"
    assert isinstance(stop.call_args.args[1], reconcile.Scheduler)
    assert report.stopped == ["comfyui-0"]
    # Disabling an already stopped service remains idempotent.
    monkeypatch.setattr(reconcile, "running", lambda: {})
    reconcile.reconcile_once(reconcile.load_services(data), only=["comfyui"])


def test_enable_failure_is_reported_and_desired_state_remains(api, monkeypatch):
    monkeypatch.setattr(reconcile, "reconcile_once", lambda *a, **k: ApplyReport(unhealthy=["comfyui-0"]))
    with pytest.raises(ApiError, match="desired state saved"):
        api.set_app_disabled("comfyui", False)


def test_disable_preserves_comments_and_other_service_settings(api, monkeypatch):
    path = api.profiles.replace("runs.yaml", "services.yaml")
    text = """# Production services
services:
- name: comfyui # render worker
  kind: app
  replicas: 1
  command: [python, main.py]
- name: sam3 # segmentation
  kind: app
  gpus: 1
"""
    with open(path, "w") as f:
        f.write(text)
    monkeypatch.setattr(reconcile, "reconcile_once", lambda *a, **k: ApplyReport())
    api.set_app_disabled("comfyui", True)
    api.set_app_disabled("comfyui", False)
    with open(path) as f:
        changed = f.read()
    assert changed.replace("disabled: false\n  ", "") == text


def test_stop_kills_actor_without_restart_and_removes_reservation(monkeypatch):
    actor, pg = Mock(), Mock()
    monkeypatch.setattr(reconcile.ray, "get_actor", lambda *a, **k: actor)
    monkeypatch.setattr(reconcile, "actor_node_id", lambda a: None)
    kill, remove = Mock(), Mock()
    monkeypatch.setattr(reconcile.ray, "kill", kill)
    monkeypatch.setattr(reconcile.ray, "get", lambda *a, **k: None)
    monkeypatch.setattr(reconcile, "clean_after", Mock())
    monkeypatch.setattr(
        reconcile.ray.util,
        "placement_group_table",
        lambda: {"pg": {"name": "pg-comfyui-0-old", "state": "CREATED"}},
    )
    monkeypatch.setattr(reconcile.ray.util, "get_placement_group", lambda *a: pg)
    monkeypatch.setattr(reconcile, "remove_placement_group", remove)
    reconcile.stop("comfyui-0")
    kill.assert_called_once_with(actor, no_restart=True)
    reconcile.clean_after.remote.assert_called_once_with("comfyui-0")
    remove.assert_called_once_with(pg)


def test_persistent_app_checks_namespace_before_reserving_resources(monkeypatch):
    cfg = reconcile.load_services({"services": [{"name": "comfyui", "kind": "app", "namespace": "alice"}]})
    spec = reconcile.replica_specs(cfg)["comfyui-0"]
    reserve = Mock()
    monkeypatch.setattr(reconcile, "placement_group", reserve)
    def missing(self, reference):
        assert reference == "alice"
        raise ValueError("unknown namespace")
    monkeypatch.setattr(reconcile.Scheduler, "require_namespace", missing)
    with pytest.raises(ValueError, match="unknown namespace"):
        reconcile.start(spec)
    reserve.assert_not_called()


def test_reconciler_reserves_persistent_replica_units(monkeypatch, tmp_path):
    cfg = reconcile.load_services({"scheduler_admin": "http://scheduler", "services": [
        {"name": "comfyui", "kind": "app", "namespace": "alice", "tag": "render", "gpus": 1, "cpus": 8},
    ]})
    token_file = tmp_path / "admin.token"
    token_file.write_text("admin-secret")
    cfg.cluster.admin_token_file = str(token_file)
    scheduler = reconcile.Scheduler(cfg.cluster)
    monkeypatch.setattr(scheduler, "require_namespace", lambda name: "a" * 32)
    requests = []

    class Response:
        def __enter__(self): return self
        def __exit__(self, *_): return False
        def read(self): return b"{}"

    def open_request(req, timeout):
        requests.append(req)
        return Response()

    monkeypatch.setattr(reconcile.urllib.request, "urlopen", open_request)
    scheduler.reserve_service(reconcile.replica_specs(cfg)["comfyui-0"])
    assert requests[0].full_url == "http://scheduler/admin/namespaces/" + "a" * 32 + "/allocations"
    assert json.loads(requests[0].data) == {
        "id": "service:comfyui-0", "tag": "render", "gpus": 1.0, "cpus": 8.0, "restore": False,
    }
    scheduler.release_service("comfyui-0")
    assert requests[1].method == "DELETE"
    assert requests[1].full_url.endswith("/allocations/service%3Acomfyui-0")


def test_gpu_services_must_be_bound_to_one_proof_app():
    with pytest.raises(ValueError, match="dedicated proof_app"):
        reconcile.load_services({"services": [{"name": "shared-gpu", "kind": "ziren-gpu"}]})
    with pytest.raises(ValueError, match="proof_app must be"):
        reconcile.load_services({"services": [{"name": "wrong-kind", "kind": "app",
                                                 "proof_app": "eth/" + "a" * 64}]})


def test_proof_app_disable_stops_only_its_namespace_server_and_enable_starts_before_admission(tmp_path, monkeypatch):
    profiles = tmp_path / "runs.yaml"
    profiles.write_text("profiles: {}\n")
    services_path = tmp_path / "services.yaml"
    services_path.write_text(yaml.safe_dump({"services": [
        {"name": "alice-eth", "kind": "ziren-gpu", "namespace": "alice", "proof_app": "eth/" + "a" * 64},
        {"name": "bob-eth", "kind": "ziren-gpu", "namespace": "bob", "proof_app": "eth/" + "a" * 64},
    ]}))
    api = Api(str(profiles), str(tmp_path / "runs"))
    order = []

    class FakeScheduler:
        def __init__(self, _cluster):
            pass

        def require_namespace(self, reference):
            return {"alice": "a" * 32, "bob": "b" * 32}[reference]

        def resolve_namespace_id(self, reference):
            return {"id": self.require_namespace(reference), "suspended_ms": None}

        def set_proof_app_disabled(self, namespace, tenant, elf_id, disabled):
            order.append(("admission", namespace, tenant, elf_id, disabled))

        def undrain(self, name):
            order.append(("undrain", name))

    def apply(cfg, only):
        saved = yaml.safe_load(services_path.read_text())
        assert only == ["alice-eth"]
        assert saved["services"][0]["disabled"] == (f"{only[0]}-0" not in reconcile.replica_specs(cfg))
        order.append(("reconcile", only[0], saved["services"][0]["disabled"]))
        return ApplyReport()

    monkeypatch.setattr(reconcile, "Scheduler", FakeScheduler)
    monkeypatch.setattr(reconcile, "reconcile_once", apply)

    available = api.proof_app_available("alice", "eth", "a" * 64)
    assert available["servers"] == ["alice-eth"]
    with pytest.raises(ApiError, match="no dedicated"):
        api.proof_app_available("bob", "eth", "c" * 64)

    disabled = api.set_proof_app_disabled("alice", "eth", "a" * 64, True)
    assert disabled["disabled"] is True
    assert disabled["servers"] == ["alice-eth"]
    assert order[:2] == [("admission", "alice", "eth", "a" * 64, True),
                         ("reconcile", "alice-eth", True)]

    order.clear()
    enabled = api.set_proof_app_disabled("alice", "eth", "a" * 64, False)
    assert enabled["disabled"] is False
    assert order == [("reconcile", "alice-eth", False),
                     ("undrain", "alice-eth-0"),
                     ("admission", "alice", "eth", "a" * 64, False)]


def test_priority_edit_preserves_actor_identity_and_does_not_reconcile(api, monkeypatch):
    from fleet_bridge.actors import spec_hash
    path = api.profiles.replace("runs.yaml", "services.yaml")
    text = "services:\n- name: comfyui # keep comment\n  kind: app\n  priority: 1 # scheduling only\n"
    with open(path, "w") as f:
        f.write(text)
    before = reconcile.replica_specs(reconcile.load_services(yaml.safe_load(text)))["comfyui-0"]
    apply = Mock()
    monkeypatch.setattr(reconcile, "reconcile_once", apply)
    assert api.set_app_priority("comfyui", 100)["priority"] == 100
    with open(path) as f:
        updated = f.read()
    assert updated == text.replace("priority: 1 #", "priority: 100 #")
    after = reconcile.replica_specs(reconcile.load_services(yaml.safe_load(updated)))["comfyui-0"]
    assert spec_hash(before) == spec_hash(after)
    assert api.apps()[0]["priority"] == 100
    apply.assert_not_called()


def test_starting_service_preempts_only_lower_priority_runs_in_its_namespace(monkeypatch):
    from fleet_bridge import runs
    cfg = reconcile.load_services({"services":[{"name":"comfyui","kind":"app","namespace":"alice","priority":10,"gpus":1,"cpus":1}]})
    spec = reconcile.replica_specs(cfg)["comfyui-0"]
    monkeypatch.setattr(reconcile.Scheduler, "require_namespace", lambda self, ref: "1"*32)
    monkeypatch.setattr(reconcile, "free_by_node_ip", lambda: {"worker":(0,8)})
    pool = [runs.Candidate(id=id, namespace=ns, priority=p, node_ip="worker", gpus=1, cpus=1,
                          started_at=1, preemptible=True, min_runtime_s=0)
            for id,ns,p in [("other", "2"*32, -10), ("equal", "1"*32, 10), ("low", "1"*32, 0)]]
    monkeypatch.setattr(runs, "candidates", lambda _: pool)
    preempt = Mock()
    monkeypatch.setattr(runs, "preempt", preempt)
    assert reconcile._preempt_runs_for(spec, reconcile.bundle(spec), "/runs") == ["low"]
    assert preempt.call_args.args[1] == "low"
    assert preempt.call_args.kwargs == {"requeue":True,"defer_requeue":True,"namespace":"1"*32,"priority":10}
