"""Local service execution keeps dedicated proof servers and confirms cleanup."""
from unittest.mock import Mock
import pytest
from fleet_bridge import reconcile

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


def test_gpu_services_must_be_bound_to_one_proof_app():
    with pytest.raises(ValueError, match="dedicated proof_app"):
        reconcile.load_services({"services": [{"name": "shared-gpu", "kind": "ziren-gpu"}]})
    with pytest.raises(ValueError, match="proof_app must be"):
        reconcile.load_services({"services": [{"name": "wrong-kind", "kind": "app",
                                                 "proof_app": "eth/" + "a" * 64}]})


def test_proof_replicas_each_get_one_server_and_the_same_app_storage_root():
    from fleet_bridge import actors
    cfg = reconcile.load_services({"scheduler": "https://scheduler:8905", "cluster_token_file": "/etc/cluster.token", "services": [{
        "name": "eth", "kind": "ziren-gpu", "namespace": "a"*32,
        "proof_app": "eth/"+"b"*64, "proof_storage_root": "/srv/eth", "replicas": 2, "gpus": 2,
    }]})
    specs = reconcile.replica_specs(cfg)
    assert list(specs) == ["eth-0", "eth-1"]
    for name, spec in specs.items():
        env = actors._node_env(spec, "cuda", gpu_endpoint="http://127.0.0.1:30100/twirp/")
        assert env["FLEET_NODE_NAME"] == name
        assert env["FLEET_APP_STORAGE_ROOT"] == "/srv/eth"
        assert env["FLEET_APP_NAMESPACE"] == "a"*32
        assert env["FLEET_APP_ELF_ID"] == "b"*64
        assert reconcile.bundle(spec).gpu == 2
