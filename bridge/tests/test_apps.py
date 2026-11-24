"""Local app entries, their replicas and reservations."""
from unittest.mock import Mock
import pytest
from fleet_bridge import reconcile
from fleet_bridge.model import RuntimeSelection

SHIM = {"shim_binary": "/opt/trusted-shim-runtime", "shim_state_dir": "/var/lib/trusted-shim"}
DOCKER = {"executor": "shim", "driver": "docker"}
WEB = {"name": "web", "kind": "app", "runtime": DOCKER, "image": "web:latest", "port_base": 8080, "gpus": 0, "cpus": 2}
PROVER = {"kind": "ziren-gpu", "runtime": DOCKER, "image": "prover:latest", "port_base": 0, "container_port": 8080}


def load(*apps):
    return reconcile.load_apps({**SHIM, "apps": list(apps)})


def test_an_entry_selects_its_runtime():
    with pytest.raises(ValueError, match=r"web: an app entry selects its runtime, e\.g\. runtime: \{executor: shim, driver: docker\}"):
        reconcile.load_apps({"apps": [{"name": "web", "kind": "app"}]})
    for runtime in ({"executor": "legacy"}, {"executor": "shim"}, {"executor": "shim", "driver": "podman"}):
        with pytest.raises(ValueError):
            load({**WEB, "runtime": runtime})
    for driver in ("docker", "openshell", "tee", "tee-simulation"):
        assert RuntimeSelection.load({"executor": "shim", "driver": driver}, strict=True).driver == driver


@pytest.mark.parametrize("gpus", [0, 2])
def test_a_replica_reserves_the_gpus_its_entry_asks_for(gpus):
    spec = reconcile.replica_specs(load({**WEB, "gpus": gpus}))["web-0"]
    assert reconcile.bundle(spec).gpu == gpus


def test_stop_kills_actor_without_restart_and_removes_reservation(monkeypatch):
    actor, pg = Mock(), Mock()
    monkeypatch.setattr(reconcile.ray, "get_actor", lambda *a, **k: actor)
    kill, remove = Mock(), Mock()
    monkeypatch.setattr(reconcile.ray, "kill", kill)
    monkeypatch.setattr(
        reconcile.ray.util,
        "placement_group_table",
        lambda: {"abc123": {"name": "pg-comfyui-0-old", "state": "CREATED"}},
    )
    monkeypatch.setattr(reconcile, "PlacementGroupID", Mock(from_hex=lambda h: h))
    monkeypatch.setattr(reconcile, "PlacementGroup", lambda pg_id: pg if pg_id == "abc123" else None)
    monkeypatch.setattr(reconcile, "remove_placement_group", remove)
    reconcile.stop("comfyui-0")
    kill.assert_called_once_with(actor, no_restart=True)
    remove.assert_called_once_with(pg)


def test_gpu_provers_must_be_bound_to_one_app():
    with pytest.raises(ValueError, match="proves one program"):
        load({**PROVER, "name": "shared-gpu"})
    with pytest.raises(ValueError, match="full ELF SHA-256"):
        load({**PROVER, "name": "short", "elf_id": "abc"})
    with pytest.raises(ValueError, match="must be a name"):
        load({**PROVER, "name": "old", "app": "eth/" + "a" * 64, "elf_id": "a" * 64})
    with pytest.raises(ValueError, match="is its own app"):
        load({**WEB, "name": "wrong-kind", "app": "eth"})
    with pytest.raises(ValueError, match="only a Ziren prover"):
        load({**WEB, "elf_id": "a" * 64})
    assert load(WEB).apps[0].app == "web"
    prover = load({**PROVER, "name": "eth-gpu", "app": "eth", "elf_id": "A" * 64}).apps[0]
    assert (prover.app, prover.elf_id) == ("eth", "a" * 64)


def test_proof_replicas_each_reserve_their_own_cards():
    specs = reconcile.replica_specs(load({**PROVER, "name": "eth", "namespace": "a" * 32, "app": "eth",
        "elf_id": "b" * 64, "proof_storage_root": "/srv/eth", "replicas": 2, "gpus": 2}))
    assert list(specs) == ["eth-0", "eth-1"]
    for spec in specs.values():
        assert spec.app.proof_storage_root == "/srv/eth"
        assert reconcile.bundle(spec).gpu == 2


def test_failed_start_removes_its_placement_group(monkeypatch):
    spec = reconcile.replica_specs(load(WEB))["web-0"]
    pg, remove = Mock(), Mock()
    monkeypatch.setattr(reconcile, "placement_group", lambda *a, **k: pg)
    monkeypatch.setattr(reconcile, "remove_placement_group", remove)
    monkeypatch.setattr(reconcile.ray, "get", Mock(side_effect=TimeoutError("not ready")))
    stop = Mock()
    monkeypatch.setattr(reconcile, "stop", stop)
    with pytest.raises(TimeoutError):
        reconcile.start(spec)
    stop.assert_not_called()
    remove.assert_called_once_with(pg)


def test_a_refused_actor_submission_releases_its_placement_group(monkeypatch):
    # Ray raises from `remote()` itself, e.g. for a taken actor name: no
    # process can own the group, so its resources must not stay reserved.
    spec = reconcile.replica_specs(load(WEB))["web-0"]
    pg, remove, stop = Mock(), Mock(), Mock()
    monkeypatch.setattr(reconcile, "placement_group", lambda *a, **k: pg)
    monkeypatch.setattr(reconcile.ray, "get", lambda *a, **k: None)
    monkeypatch.setattr(reconcile, "remove_placement_group", remove)
    monkeypatch.setattr(reconcile, "stop", stop)
    factory = Mock()
    factory.options.return_value.remote.side_effect = ValueError("actor name already taken")
    monkeypatch.setattr(reconcile.shim, "ShimApp", factory)
    with pytest.raises(ValueError, match="already taken"):
        reconcile.start(spec, ready_timeout_s=1)
    remove.assert_called_once_with(pg)
    stop.assert_not_called()
