import copy
import socket
from dataclasses import replace
from unittest.mock import Mock

import pytest
import yaml

from fleet_bridge import actors, reconcile, shim, node
from fleet_bridge.model import ExecutorKind, ReplicaHealth, AppSpec


def test_tcp_service_uses_tcp_health_and_requires_new_capability(tmp_path, monkeypatch):
    from fleet_bridge.model import ServiceProtocol
    s = spec(tmp_path)
    s = replace(s, app=replace(s.app, services=["ssh", "control"], service_protocol=ServiceProtocol.TCP))
    bound = shim.RuntimeBook(tmp_path / "book").bind(s, "launch")
    assert shim.required_capability(bound) == "shim-tcp-v1"
    assert [v["protocol"] for v in shim.launch_request(bound, [])["services"]] == ["tcp", "http1"]
    actor = object.__new__(shim._ShimApp)
    actor.spec, actor.port = bound, 12345
    connected = Mock()
    connected.return_value.__enter__ = Mock()
    connected.return_value.__exit__ = Mock()
    monkeypatch.setattr(shim.socket, "create_connection", connected)
    monkeypatch.setattr(actors, "_http_ok", lambda _: pytest.fail("TCP must not send HTTP to SSH"))
    assert actor.application_ready()
    connected.assert_called_once_with(("127.0.0.1", 12345), timeout=3)
    monkeypatch.setattr(shim._ShimApp, "call", lambda self, command: {"state":"ready","port":12345})
    monkeypatch.setattr(shim._ShimApp, "start_network", lambda self: None)
    # Initial admission must use the same probe as subsequent heartbeats.
    assert shim._ShimApp(bound).port == 12345
    assert connected.call_count == 2
    connected.side_effect = OSError()
    assert not actor.application_ready()
    for updates in ({"runtime":None}, {"services":["control"]}, {"ingress":{"ssh":object()}}):
        with pytest.raises(ValueError):
            replace(s.app, **updates)
    raw = config(tmp_path)
    raw["apps"][0]["service_protocol"] = "udp"
    with pytest.raises(ValueError):
        reconcile.load_apps(raw)


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


def test_daemon_transport_is_persisted_and_never_silently_migrated(tmp_path, monkeypatch):
    s = spec(tmp_path)
    book = shim.RuntimeBook(tmp_path / "runtimes")
    first = book.bind(s, "launch")
    daemon_spec = replace(s, cluster=replace(s.cluster, shim_daemon=True))
    with pytest.raises(ValueError, match="active shim allocation"):
        book.bind(daemon_spec, "upgrade")
    assert book.read(s.name).execution == first.execution
    call = Mock(return_value={"cleaned": True})
    monkeypatch.setattr(shim, "invoke", call)
    book.cleanup(s.name)
    assert call.call_args.kwargs["daemon"] is False
    book.bind(daemon_spec, "new-allocation")
    assert book.read(s.name).daemon is True
    book.cleanup(s.name)
    assert call.call_args.kwargs["daemon"] is True


def test_missing_daemon_never_falls_back_to_executable(tmp_path, monkeypatch):
    run = Mock()
    monkeypatch.setattr(shim.subprocess, "run", run)
    with pytest.raises(RuntimeError, match="no legacy fallback"):
        shim.invoke("/unused/binary", str(tmp_path), socket.gethostname(), {"operation":"capabilities"}, daemon=True)
    run.assert_not_called()


def test_daemon_network_is_owned_by_service_not_ray_subprocess(tmp_path, monkeypatch):
    s = spec(tmp_path)
    s = replace(s, app=replace(s.app, services=["http"]), cluster=replace(s.cluster, shim_daemon=True))
    actor = object.__new__(shim._ShimApp)
    actor.spec = s
    actor.call = Mock(return_value={"connected_worker":True})
    spawn = Mock()
    monkeypatch.setattr(shim.subprocess, "Popen", spawn)
    actor.start_network()
    spawn.assert_not_called()
    assert actor.call.call_args.args[0]["operation"] == "network"


def test_network_recovery_failure_preserves_observed_runtime_and_repair(tmp_path, monkeypatch):
    s=spec(tmp_path)
    actor=object.__new__(shim._ShimApp)
    actor.spec=replace(s,cluster=replace(s.cluster,shim_daemon=True))
    actor.port=12345;actor.gpus=[];actor.network=None
    actor.call=Mock(return_value={"state":"ready"})
    actor.start_network=Mock(side_effect=RuntimeError('temporary connector failure'))
    actor.application_ready=Mock(return_value=False)
    value=actor.health()
    assert value['management_ready'] is True and value['ready'] is False
    actor.application_ready.return_value=True
    assert actor.health()['ready'] is True


def test_service_bootstrap_ignores_invalid_apps_but_validates_daemon_settings(tmp_path,monkeypatch):
    path=tmp_path/'apps.yaml'
    # Invalid application and network settings must remain remotely repairable.
    path.write_text('apps: [broken]\nshim_network: {relay_url: invalid}\n')
    assert shim.daemon_config(path).shim_daemon is False
    path.write_text('shim_daemon: true\nshim_binary: /bin/shim\nshim_state_dir: /tmp/shim\napps: [broken]\n')
    config=shim.daemon_config(path)
    call=Mock(side_effect=[RuntimeError('starting'),{'protocol':1,'features':['runtime-daemon-v1']}])
    monkeypatch.setattr(shim,'daemon_call',call)
    monkeypatch.setattr(shim.time,'sleep',lambda _:None)
    shim.check_daemon(config,wait_seconds=1)
    assert call.call_count==2
    for binary in [None,'relative',123]:
        data={'shim_daemon':True,'shim_state_dir':'/tmp/shim'}
        if binary is not None:data['shim_binary']=binary
        path.write_text(yaml.safe_dump(data))
        with pytest.raises(ValueError):shim.daemon_config(path)


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


def test_unhealthy_app_reports_only_its_owned_management_service(tmp_path, monkeypatch):
    e = executor(tmp_path)
    raw = yaml.safe_load(e.apps_file.read_text())
    raw['shim_network'] = {'gateway_url': 'https://control.example', 'relay_url': 'wss://data.example',
        'token_file': '/private/node.jwt', 'public_key_file': '/private/gateway.pem', 'issuer': 'test'}
    raw['apps'][0]['services'] = ['http', 'control']
    e.apps_file.write_text(yaml.safe_dump(raw))
    replica = e.runtimes.bind(reconcile.replica_specs(reconcile.load_apps(raw))['web-0'], 'launch')
    health = ReplicaHealth(name='web-0', spec=actors.spec_hash(replica), executor=ExecutorKind.SHIM,
        execution=replica.execution, ready=False, management_ready=True)
    monkeypatch.setattr(node, 'alive_nodes', lambda: [{}])
    monkeypatch.setattr(node, 'resources', lambda: ({'GPU': 0, 'CPU': 1}, {'GPU': 0, 'CPU': 0}))
    monkeypatch.setattr(reconcile, 'running', lambda: {'web-0': object()})
    monkeypatch.setattr(reconcile, 'health_of', lambda *a, **kw: health)
    monkeypatch.setattr(shim, 'capabilities', lambda _: [])
    report = e.report()
    assert not report['apps'][0]['ready']
    assert report['runtime_instances'][0]['services'] == ['control']
    health.ready = True
    assert e.report()['runtime_instances'][0]['services'] == ['http', 'control']
    health.ready = False
    health.management_ready = False
    assert e.report().get('runtime_instances', []) == []
    health.management_ready = True
    health.execution = replace(replica.execution, lease_id='obsolete')
    assert e.report().get('runtime_instances', []) == []


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


def test_management_ingress_and_automatic_port_require_modern_shim(tmp_path):
    raw=config(tmp_path)
    raw['shim_network']={'gateway_url':'https://control.example','relay_url':'wss://relay.example',
        'token_file':'/private/node.jwt','public_key_file':'/private/public.pem','issuer':'test'}
    app=raw['apps'][0]
    app.update(port_base=0,container_port=18789,services=['chat','control'],env={'TOKEN':'private'},
        ingress={'chat':{'paths':['/v1/chat/completions'],'methods':['POST'],'bearer_env':'TOKEN'}})
    parsed=reconcile.load_apps(raw)
    replica=shim.RuntimeBook(tmp_path/'book').bind(reconcile.replica_specs(parsed)['web-0'],'launch')
    request=shim.launch_request(replica,[])
    assert request['driver_options']['port']==0
    assert request['command']==['serve','18789']
    assert [s['endpoint_ref'] for s in request['services']]==['ingress:chat','control']
    assert shim.required_capability(replica)=='shim-management-v1'
    assert 'single_use' not in request['driver_options']['ingress']['chat']
    app['ingress']['chat']['single_use']=True
    single=reconcile.replica_specs(reconcile.load_apps(raw))['web-0']
    assert shim.required_capability(single)=='shim-single-use-ingress-v1'
    app['ingress']['chat']['single_use']='true'
    with pytest.raises(ValueError):reconcile.load_apps(raw)
    del app['ingress']['chat']['single_use']

    app['ingress']['chat']['paths']=['/../exec']
    with pytest.raises(ValueError):reconcile.load_apps(raw)
    app['ingress']['chat']['paths']=['/v1/chat/completions']
    app['ingress']['chat']['bearer_env']='MISSING'
    with pytest.raises(ValueError):reconcile.load_apps(raw)


def test_native_companion_settings_use_existing_config_and_cannot_change_during_node_upgrade(tmp_path, monkeypatch):
    import hashlib
    import json
    from fleet_bridge.model import to_dict
    path = tmp_path / "apps.yaml"
    raw = config(tmp_path)
    raw["shim_daemon"] = True
    raw["shim_openshell"] = {"socket":"/run/openshell/compute.sock", "admission_policy":"approved",
        "ledger_endpoint":"https://fleet.example"}
    path.write_text(yaml.safe_dump(raw))
    parsed = shim.daemon_config(path)
    options = shim.daemon_options(parsed)
    assert options["openshell"]["ledger_ca_file"] is None
    expected = hashlib.sha256(json.dumps(to_dict(parsed.shim_openshell),sort_keys=True,separators=(",", ":"),ensure_ascii=False).encode()).hexdigest()
    active = {"protocol":1,"features":["runtime-daemon-v1","openshell-provisioning-v1"],"openshell_config_sha256":expected}
    monkeypatch.setattr(shim, "daemon_call", lambda *a, **kw: active)
    shim.check_daemon(parsed)
    for value in [None,"other"]:
        active["openshell_config_sha256"] = value
        with pytest.raises(RuntimeError, match="settings differ"):
            shim.check_daemon(parsed)
    active["openshell_config_sha256"] = expected
    with pytest.raises(RuntimeError, match="settings differ"):
        shim.check_daemon(replace(parsed,shim_openshell=None))
    for field, value in [("socket","relative.sock"),("ledger_endpoint","http://remote.example"),
            ("ledger_endpoint","https://secret@fleet.example"),("ledger_endpoint","https://fleet.example/path"),
            ("ledger_ca_file","relative.pem"),("ledger_token","must-not-be-configured")]:
        invalid = copy.deepcopy(raw); invalid["shim_openshell"][field] = value
        path.write_text(yaml.safe_dump(invalid))
        with pytest.raises(ValueError): shim.daemon_config(path)
    raw["shim_daemon"] = False
    path.write_text(yaml.safe_dump(raw))
    with pytest.raises(ValueError,match="persistent shim daemon"): shim.daemon_config(path)


def test_real_openshell_daemon_bootstrap_matches_running_configuration(tmp_path):
    import os
    import subprocess
    import sys
    import tempfile
    from pathlib import Path
    binary = os.environ.get("TRUSTED_SHIM_OPENSHELL_TEST_BINARY")
    if not binary:
        pytest.skip("requires companion built with --features openshell")
    # Keep the Unix socket within the kernel's pathname bound.
    with tempfile.TemporaryDirectory(prefix="fleet-native-") as root:
        path = tmp_path / "native-apps.yaml"
        raw = {"shim_binary":binary,"shim_state_dir":str(Path(root)/"state"),"shim_daemon":True,
            "shim_openshell":{"socket":str(Path(root)/"driver.sock"),"admission_policy":"approved",
                "ledger_endpoint":"https://fleet.example"},"apps":["invalid app deliberately ignored by daemon bootstrap"]}
        path.write_text(yaml.safe_dump(raw))
        parsed = shim.daemon_config(path)
        process = subprocess.Popen([sys.executable,"-m","fleet_bridge.shim","serve","--apps",str(path)],
            stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            shim.check_daemon(parsed,wait_seconds=10)
            value = shim.daemon_call(parsed.shim_state_dir,socket.gethostname(),{"operation":"capabilities"})
            assert value["drivers"] == ["docker","openshell"]
            assert value["pid"] == process.pid
            changed = replace(parsed,shim_openshell=replace(parsed.shim_openshell,ledger_endpoint="https://other.example"))
            with pytest.raises(RuntimeError, match="settings differ"):shim.check_daemon(changed)
            # An unavailable native driver/gateway does not stop the companion.
            response = shim.daemon_call(parsed.shim_state_dir,socket.gethostname(),{"operation":"capabilities"})
            assert response["pid"] == process.pid
        finally:
            process.terminate()
            process.communicate(timeout=15)
        assert process.returncode == 0


def native_config(tmp_path):
    data = config(tmp_path)
    data.update(shim_daemon=True, shim_openshell={"socket":str(tmp_path/"compute.sock"),
        "admission_policy":"approved", "ledger_endpoint":"http://127.0.0.1:8905"})
    app = data["apps"][0]
    app.pop("port_base")
    app["cpus"] = 1
    app["runtime"]["driver"] = "openshell"
    return data


def test_native_allocation_never_creates_via_ray_or_claims_app_readiness(tmp_path, monkeypatch):
    data = native_config(tmp_path)
    spec = reconcile.replica_specs(reconcile.load_apps(data))["web-0"]
    book = shim.RuntimeBook(tmp_path/"book")
    spec = book.bind(spec, "launch")
    calls = []
    def invoke(binary, state, node, command, **kwargs):
        assert kwargs == {"daemon":True}
        assert command["driver"] == "openshell"
        assert command["execution"] == spec.execution
        calls.append(command["operation"])
        return {"phase":"created", "platform_ready":True, "cleaned":True}
    monkeypatch.setattr(shim,"invoke",invoke)
    actor = shim._ProvisionedApp(spec)
    health = ReplicaHealth.load(actor.health())
    assert health.allocation_reserved and not health.is_ready and not health.management_ready
    assert reconcile.matches(spec,health)
    assert calls == ["allocate", "inspect"]
    assert book.read(spec.name).driver == "openshell"
    book.cleanup(spec.name)
    assert calls[-2:] == ["allocate", "delete"]
    assert book.read(spec.name).cleaned
    with pytest.raises(ValueError): book.bind(spec,"launch")
    next_spec = book.bind(spec,"replacement")
    assert next_spec.execution.generation == spec.execution.generation + 1


@pytest.mark.parametrize("key,value", [("port_base",123), ("services",["http"]), ("gpus",1),
    ("volumes",["/tmp"]),("memory_gb",0),("cpus",0.5),("user","root"),("entrypoint","/bin/sh")])
def test_native_profile_rejects_unmapped_settings(tmp_path,key,value):
    data=native_config(tmp_path);data["apps"][0][key]=value
    with pytest.raises(ValueError): reconcile.load_apps(data)


def test_native_capability_requires_matching_configuration_and_allocation_protocol(tmp_path,monkeypatch):
    config=reconcile.load_apps(native_config(tmp_path)).cluster
    digest=shim.hashlib.sha256(shim.json.dumps(shim.to_dict(config.shim_openshell),sort_keys=True,separators=(",",":"),ensure_ascii=False).encode()).hexdigest()
    value={"protocol":1,"drivers":["docker","openshell"],"openshell_config_sha256":digest,
        "features":["openshell-provisioning-v1"]}
    monkeypatch.setattr(shim,"daemon_call",lambda *a,**k:value)
    assert "shim-openshell-provisioning-v1" not in shim.capabilities(config)
    value["features"].append("openshell-allocation-v1")
    assert "shim-openshell-provisioning-v1" in shim.capabilities(config)
    value["openshell_config_sha256"]="wrong"
    assert shim.capabilities(config)==[]


def test_native_selection_cannot_be_swapped_for_docker_in_admitted_command(tmp_path,monkeypatch):
    import yaml
    data=native_config(tmp_path)
    apps=tmp_path/"apps.yaml";apps.write_text(yaml.safe_dump(data))
    batches=tmp_path/"batches.yaml";batches.write_text("batches: {}\n")
    executor=node.NodeExecutor(str(apps),str(batches),str(tmp_path/"jobs"))
    monkeypatch.setattr(node,"alive_nodes",lambda:[{"Alive":True,"NodeManagerAddress":"127.0.0.1"}])
    app=copy.deepcopy(data["apps"][0]);app.update(namespace="0"*32,app="web")
    app["runtime"]["driver"]="docker"
    try:
        with pytest.raises(ValueError,match="runtime selection differs"):
            executor.set_app({"app":app,"disabled":False},"bad-command")
    finally: executor.pool.shutdown()
