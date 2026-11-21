import copy
import socket
from dataclasses import replace
from unittest.mock import Mock

import pytest
import yaml

from fleet_bridge import actors, reconcile, shim, node
from fleet_bridge.model import ExecutorKind, ReplicaHealth, AppSpec


def config(tmp_path, selected=True):
    app = {"name":"web", "kind":"app", "image":"python:3.12-slim", "gpus":0,
        "cpus":0.5, "memory_gb":0.125, "port_base":18080, "command":["serve", "{port}"]}
    if selected:
        app["runtime"] = {"executor":"shim", "driver":"docker"}
    return {"shim_binary":"/opt/trusted-shim-runtime", "shim_state_dir":str(tmp_path/"shim"), "apps":[app]}


def spec(tmp_path):
    return reconcile.replica_specs(reconcile.load_apps(config(tmp_path)))["web-0"]


def test_installing_shim_does_not_change_legacy_hash_or_execution(tmp_path):
    raw = config(tmp_path, False)
    before = copy.deepcopy(raw)
    before.pop("shim_binary"); before.pop("shim_state_dir")
    old = reconcile.replica_specs(reconcile.load_apps(before))["web-0"]
    upgraded = reconcile.replica_specs(reconcile.load_apps(raw))["web-0"]
    assert old.config == upgraded.config
    assert actors.spec_hash(old) == actors.spec_hash(upgraded)
    book = shim.RuntimeBook(tmp_path/"runtimes")
    assert book.bind(upgraded, "new-command").execution is None
    assert not book.root.exists()


@pytest.mark.parametrize("runtime", [None, {"executor":"unknown"}, {"executor":"shim", "driver":"openshell"},
    {"executor":"shim", "driver":"docker", "policy_proof":"required"}])
def test_unsupported_or_misspelled_runtime_is_not_ignored(tmp_path, runtime):
    cfg = config(tmp_path); cfg["apps"][0]["runtime"] = runtime
    with pytest.raises((ValueError, TypeError)):
        reconcile.load_apps(cfg)
    if runtime is not None:
        with pytest.raises(ValueError):
            AppSpec.load(cfg["apps"][0])


def test_lease_is_persisted_and_recovery_cannot_change_request_or_executor(tmp_path):
    s = spec(tmp_path); book = shim.RuntimeBook(tmp_path/"runtimes")
    first = book.bind(s, "command-one")
    recovered = shim.RuntimeBook(book.root).bind(s, "different-heartbeat-command")
    assert recovered.execution == first.execution
    assert recovered.execution.node_id == socket.gethostname()
    assert shim.launch_request(first, [])["limits"] == {"cpu_millicores":500,"memory_bytes":134217728}
    changed = replace(s, config={**s.config,"image":"different"})
    with pytest.raises(ValueError, match="active shim allocation"):
        book.bind(changed,"command-two")
    legacy = reconcile.replica_specs(reconcile.load_apps(config(tmp_path, False)))["web-0"]
    with pytest.raises(ValueError, match="confirmed cleanup"):
        book.bind(legacy,"command-two")


def test_cleanup_uses_persisted_binary_and_failure_keeps_allocation(tmp_path, monkeypatch):
    s=spec(tmp_path); book=shim.RuntimeBook(tmp_path/"runtimes")
    first=book.bind(s,"first")
    call=Mock(side_effect=RuntimeError("offline"));monkeypatch.setattr(shim,"invoke",call)
    with pytest.raises(RuntimeError): book.cleanup(s.name)
    assert not book.read(s.name).cleaned
    call.side_effect=None;call.return_value={"cleaned":True}
    book.cleanup(s.name)
    assert call.call_args.args[:2] == ("/opt/trusted-shim-runtime",str(tmp_path/"shim"))
    assert book.read(s.name).cleaned
    with pytest.raises(ValueError,match="replay"): book.bind(s,"first")
    second=book.bind(s,"second")
    assert second.execution.generation==first.execution.generation+1


def executor(tmp_path):
    apps=tmp_path/"apps.yaml";apps.write_text(yaml.safe_dump(config(tmp_path)))
    batches=tmp_path/"batches.yaml";batches.write_text("batches: {}\n")
    e=node.NodeExecutor(str(apps),str(batches),str(tmp_path/"jobs"));e.pool.shutdown()
    return e


def test_direct_executor_change_cannot_stop_a_legacy_actor(tmp_path,monkeypatch):
    e=executor(tmp_path)
    app=config(tmp_path)["apps"][0];app.update(namespace="0"*32,app="web",elf_id="")
    monkeypatch.setattr(node,"alive_nodes",lambda:[{}])
    monkeypatch.setattr(shim,"capabilities",lambda _: [shim.CAPABILITY])
    monkeypatch.setattr(reconcile,"running",lambda:{"web-0":object()})
    monkeypatch.setattr(reconcile,"health_of",lambda _:ReplicaHealth(name="web-0",spec="old",ready=True))
    stop=Mock();monkeypatch.setattr(reconcile,"stop",stop)
    with pytest.raises(ValueError,match="explicit drain"):e.set_app({"app":app,"disabled":False},"start")
    stop.assert_not_called()


def test_failed_shim_does_not_call_legacy_executor(tmp_path,monkeypatch):
    e=executor(tmp_path)
    app=config(tmp_path)["apps"][0];app.update(namespace="0"*32,app="web",elf_id="")
    monkeypatch.setattr(node,"alive_nodes",lambda:[{}]);monkeypatch.setattr(reconcile,"running",lambda:{})
    monkeypatch.setattr(node,"container_gone",lambda _:True)
    monkeypatch.setattr(shim,"capabilities",lambda _:[shim.CAPABILITY])
    start=Mock(side_effect=RuntimeError("shim unavailable"));monkeypatch.setattr(reconcile,"start",start)
    with pytest.raises(RuntimeError):e.set_app({"app":app,"disabled":False},"start")
    assert start.call_count==1 and shim.selected(start.call_args.args[0])
    assert not e.runtimes.read("web-0").cleaned


def test_ray_reservation_is_retained_when_shim_cleanup_is_uncertain(monkeypatch):
    actor=Mock();kill=Mock();remove=Mock()
    monkeypatch.setattr(reconcile.ray,"get_actor",lambda *a,**k:actor)
    monkeypatch.setattr(reconcile.ray,"kill",kill)
    monkeypatch.setattr(reconcile,"remove_placement_group",remove)
    with pytest.raises(RuntimeError,match="uncertain"):
        reconcile.stop("web-0",cleanup=Mock(side_effect=RuntimeError("uncertain")))
    kill.assert_called_once_with(actor,no_restart=True)
    remove.assert_not_called()


@pytest.mark.parametrize("message", ["No such object: missing", "error: no such object: missing", "No such container: missing"])
def test_missing_container_detection_handles_docker_error_case(monkeypatch, message):
    monkeypatch.setattr(node.subprocess,"run",lambda *a,**k:Mock(returncode=1,stdout="",stderr=message))
    assert node.container_gone("missing")


def test_unreachable_docker_is_not_confirmed_cleanup(monkeypatch):
    monkeypatch.setattr(node.subprocess,"run",lambda *a,**k:Mock(returncode=1,stdout="",stderr="Cannot connect to Docker daemon"))
    assert not node.container_gone("unknown")


def test_services_require_network_capability_and_are_bound_into_request(tmp_path, monkeypatch):
    cfg = config(tmp_path)
    cfg["apps"][0].update(services=["gateway"], container_port=18789, entrypoint="/bin/sh")
    with pytest.raises(ValueError, match="shim_network"):
        reconcile.load_apps(cfg)
    cfg["shim_network"] = {"gateway_url":"https://control.example", "relay_url":"wss://data.example",
        "token_file":"/private/node.jwt", "public_key_file":"/private/gateway.pem", "issuer":"control"}
    parsed=reconcile.load_apps(cfg)
    replica=shim.RuntimeBook(tmp_path/"book").bind(reconcile.replica_specs(parsed)["web-0"],"launch")
    request=shim.launch_request(replica,[])
    assert request["services"]==[{"id":{"namespace":replica.execution.namespace,"workload":replica.execution.workload,"service":"gateway"},"protocol":"http1","endpoint_ref":"app"}]
    assert request["driver_options"]["container_port"]==18789
    assert shim.required_capability(replica)=="shim-network-v1"
    monkeypatch.setattr(shim.subprocess,"run",lambda *a,**k:Mock(stdout='{"protocol":1,"drivers":["docker"]}'))
    assert shim.capabilities(parsed.cluster)==[shim.CAPABILITY]
    monkeypatch.setattr(shim.subprocess,"run",lambda *a,**k:Mock(stdout='{"protocol":1,"drivers":["docker"],"features":["data-plane-v1"]}'))
    assert "shim-network-v1" in shim.capabilities(parsed.cluster)
    legacy=config(tmp_path,False)
    before=actors.spec_hash(reconcile.replica_specs(reconcile.load_apps(legacy))["web-0"])
    legacy["shim_network"]=cfg["shim_network"]
    assert actors.spec_hash(reconcile.replica_specs(reconcile.load_apps(legacy))["web-0"])==before


@pytest.mark.parametrize("services", [["../control"], ["http","http"], ["http"]*9])
def test_invalid_services_never_reach_an_executor(tmp_path,services):
    cfg=config(tmp_path);cfg["apps"][0]["services"]=services
    with pytest.raises(ValueError):reconcile.load_apps(cfg)
