"""Persistent apps release their actors and stay disabled across reconciliation."""
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
    stop.assert_called_once_with("comfyui-0")
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
    monkeypatch.setattr(reconcile.ray.util, "get_placement_group", lambda *a: pg)
    monkeypatch.setattr(reconcile, "remove_placement_group", remove)
    reconcile.stop("comfyui-0")
    kill.assert_called_once_with(actor, no_restart=True)
    reconcile.clean_after.remote.assert_called_once_with("comfyui-0")
    remove.assert_called_once_with(pg)
