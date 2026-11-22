import copy
import socket
from dataclasses import replace
from unittest.mock import Mock

import pytest
import yaml

from fleet_bridge import actors, reconcile, shim, node
from fleet_bridge.model import ExecutorKind, ReplicaHealth, AppSpec


def test_packaged_prover_uses_shared_shim_lifecycle_and_explicit_manifest_inputs(tmp_path):
    raw = config(tmp_path)
    raw.update(node_binary="/old/host-worker", scheduler="https://scheduler.example",
        cluster_token_file="/private/node.token")
    app = raw['apps'][0]
    app.update(kind="ziren-gpu", elf_id="a" * 64, gpus=2, cpus=4,
        proof_storage_root="/srv/proofs", port_base=0, container_port=8080,
        env={"NAME":"{name}", "MACHINE":"{machine}", "CPUS":"{cpu_count}",
             "GPUS":"{gpu_count}", "TOKEN_FILE":"{cluster_token_file}"},
        volumes=["{proof_storage_root}", "{cluster_token_file}:ro"])
    first = reconcile.replica_specs(reconcile.load_apps(raw))['web-0']
    raw['node_binary'] = '/new/host-worker'
    assert actors.spec_hash(first) == actors.spec_hash(reconcile.replica_specs(reconcile.load_apps(raw))['web-0'])
    bound = shim.RuntimeBook(tmp_path / 'book').bind(first, 'admitted-command')
    request = shim.launch_request(bound, ['4', '6'])
    assert shim.required_capability(bound) == 'shim-prover-v1'
    assert request['environment'] == dict(NAME='web-0', MACHINE=socket.gethostname(),
        CPUS='4', GPUS='2', TOKEN_FILE='/private/node.token')
    assert request['driver_options']['gpus'] == ['4', '6']
    assert request['driver_options']['volumes'] == ['/srv/proofs:/srv/proofs', '/private/node.token:/private/node.token:ro']
    assert request['limits']['cpu_millicores'] == 4000
    for update in ({'prover_binary':'/old/override'}, {'worker_command':['/old/worker']},
                   {'runtime':{'executor':'shim','driver':'openshell'}}, {'container_port':None},
                   {'services':['http']}, {'service_protocol':'tcp'}):
        invalid = copy.deepcopy(raw)
        invalid['apps'][0].update(update)
        with pytest.raises(ValueError):
            reconcile.load_apps(invalid)


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
    monkeypatch.setattr(shim, "capabilities", lambda _: list(shim.required_capabilities(bound)))
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


@pytest.mark.parametrize("value", [[], None, {"protocol":True,"drivers":["docker"]},
    {"protocol":1,"drivers":None}, {"protocol":1,"drivers":"docker"},
    {"protocol":1,"drivers":["docker"],"features":None},
    {"protocol":1,"drivers":["docker"],"features":"data-plane-v1 runtime-management-v1"}])
def test_malformed_capabilities_fail_closed_without_breaking_reporting(tmp_path, monkeypatch, value):
    monkeypatch.setattr(shim.subprocess, "run", lambda *a, **k: Mock(stdout=shim.json.dumps(value)))
    assert shim.capabilities(spec(tmp_path).cluster) == []


def test_relative_shim_path_never_advertises_capability(tmp_path, monkeypatch):
    probe = Mock(side_effect=AssertionError("must validate before spawning"))
    monkeypatch.setattr(shim.subprocess, "run", probe)
    assert shim.capabilities(replace(spec(tmp_path).cluster, shim_binary="./runtime")) == []
    probe.assert_not_called()


def test_automatic_service_ports_require_all_features_on_actor_relaunch(tmp_path, monkeypatch):
    s = spec(tmp_path)
    s = replace(s, app=replace(s.app, port_base=0, container_port=8080, services=["http"]))
    bound = shim.RuntimeBook(tmp_path / "book").bind(s, "launch")
    assert shim.required_capabilities(bound) == {"shim-docker-v1", "shim-automatic-port-v1", "shim-runtime-v2", "shim-network-v1"}
    monkeypatch.setattr(shim, "capabilities", lambda _: ["shim-automatic-port-v1", "shim-runtime-v2"])
    invoke = Mock(); monkeypatch.setattr(shim, "invoke", invoke)
    with pytest.raises(RuntimeError, match="no longer supports"):
        shim._ShimApp(bound)
    invoke.assert_not_called()


def test_bad_service_config_is_rejected_before_writing_and_secret_refs_stay_local(tmp_path):
    e = executor(tmp_path)
    before = e.apps_file.read_bytes()
    app = {**config(tmp_path)["apps"][0], "name":"new", "services":["http"], "port_base":0, "container_port":8080}
    with pytest.raises(ValueError, match="shim_network"):
        e.app({"create":app})
    assert e.apps_file.read_bytes() == before
    app.update(env={"TOKEN":"private-value", "PUBLIC":"kept"}, ingress={"http":{"bearer_env":"TOKEN"}})
    public = {**app, "env":{**app["env"], **node.private_environment(app)}}
    assert "private-value" not in shim.json.dumps(public)
    assert node.local_settings(app, public)["env"] == app["env"]
    with pytest.raises(ValueError):
        node.local_settings({}, public)


def test_disabled_cleanup_does_not_require_current_launch_configuration(tmp_path, monkeypatch):
    e = executor(tmp_path)
    app = config(tmp_path)["apps"][0]
    bound = e.runtimes.bind(spec(tmp_path), "launch")
    e.apps_file.write_text(yaml.safe_dump({"apps":[app]}))
    monkeypatch.setattr(node, "alive_nodes", lambda: [{}])
    monkeypatch.setattr(reconcile, "running", lambda: {})
    cleanup = Mock(); monkeypatch.setattr(e, "_remove_replicas", cleanup)
    e.set_app({"app":{**app,"namespace":"0"*32,"app":"web"},"disabled":True}, "stop")
    cleanup.assert_called_once()
    assert e.runtimes.read(bound.name).execution == bound.execution


def test_legacy_zero_port_stays_loadable():
    cfg = reconcile.load_apps({"apps":[{"name":"legacy","kind":"app","port_base":0}]})
    assert cfg.apps[0].port_base == 0


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
    assert set(shim.capabilities(parsed.cluster)) == {"shim-cleanup-v1", shim.CAPABILITY}
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
    path = tmp_path / "apps.yaml"
    raw = config(tmp_path)
    raw["shim_daemon"] = True
    raw["shim_openshell"] = {"socket":"/run/openshell/compute.sock", "admission_policy":"approved",
        "ledger_endpoint":"https://fleet.example"}
    path.write_text(yaml.safe_dump(raw))
    parsed = shim.daemon_config(path)
    options = shim.daemon_options(parsed)
    assert options["openshell"]["ledger_ca_file"] is None
    assert "supervisor_image" not in options["openshell"]
    expected = hashlib.sha256(json.dumps(options["openshell"],sort_keys=True,separators=(",", ":"),ensure_ascii=False).encode()).hexdigest()
    active = {"protocol":1,"features":["runtime-daemon-v1","openshell-provisioning-v1"],"openshell_config_sha256":expected}
    monkeypatch.setattr(shim, "daemon_call", lambda *a, **kw: active)
    shim.check_daemon(parsed)
    for value in [None,"other"]:
        active["openshell_config_sha256"] = value
        with pytest.raises(RuntimeError, match="settings differ"):
            shim.check_daemon(parsed)
    active["openshell_config_sha256"] = expected
    enabled = replace(parsed, shim_openshell=replace(parsed.shim_openshell, supervisor_image="sha256:" + "a" * 64))
    assert shim.daemon_options(enabled)["openshell"]["supervisor_image"] == "sha256:" + "a" * 64
    with pytest.raises(RuntimeError, match="settings differ"):
        shim.check_daemon(enabled)
    with pytest.raises(RuntimeError, match="settings differ"):
        shim.check_daemon(replace(parsed,shim_openshell=None))
    for field, value in [("socket","relative.sock"),("ledger_endpoint","http://remote.example"),
            ("ledger_endpoint","https://secret@fleet.example"),("ledger_endpoint","https://fleet.example/path"),
            ("ledger_ca_file","relative.pem"),("ledger_token","must-not-be-configured"),
            ("supervisor_image","supervisor:latest")]:
        invalid = copy.deepcopy(raw); invalid["shim_openshell"][field] = value
        path.write_text(yaml.safe_dump(invalid))
        with pytest.raises(ValueError): shim.daemon_config(path)
    raw["shim_daemon"] = False
    path.write_text(yaml.safe_dump(raw))
    with pytest.raises(ValueError,match="persistent shim daemon"): shim.daemon_config(path)


@pytest.mark.parametrize("supervisor_image", [None, "sha256:" + "a" * 64])
def test_real_openshell_daemon_bootstrap_matches_running_configuration(tmp_path, supervisor_image):
    import os
    import subprocess
    import sys
    import tempfile
    from pathlib import Path
    binary = os.environ.get("TRUSTED_SHIM_OPENSHELL_TEST_BINARY")
    if not binary:
        pytest.skip("requires TRUSTED_SHIM_OPENSHELL_TEST_BINARY")
    # Keep the Unix socket within the kernel's pathname bound.
    with tempfile.TemporaryDirectory(prefix="fleet-native-") as root:
        path = tmp_path / "native-apps.yaml"
        raw = {"shim_binary":binary,"shim_state_dir":str(Path(root)/"state"),"shim_daemon":True,
            "shim_openshell":{"socket":str(Path(root)/"driver.sock"),"admission_policy":"approved",
                "ledger_endpoint":"https://fleet.example"},"apps":["invalid app deliberately ignored by daemon bootstrap"]}
        if supervisor_image is not None:
            raw["shim_openshell"]["supervisor_image"] = supervisor_image
        path.write_text(yaml.safe_dump(raw))
        parsed = shim.daemon_config(path)
        process = subprocess.Popen([sys.executable,"-m","fleet_bridge.shim","serve","--apps",str(path)],
            stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            shim.check_daemon(parsed,wait_seconds=10)
            value = shim.daemon_call(parsed.shim_state_dir,socket.gethostname(),{"operation":"capabilities"})
            assert value["drivers"] == ["docker"]
            assert ("openshell-local-services-v1" in value["features"]) == (supervisor_image is not None)
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
        return {"phase":"created", "platform_ready":True, "cleaned":True,
                "native_observation_base64":"c2FuaXRpemVk"}
    monkeypatch.setattr(shim,"invoke",invoke)
    actor = shim._ProvisionedApp(spec)
    health = ReplicaHealth.load(actor.health())
    assert health.allocation_reserved and not health.is_ready and not health.management_ready
    assert health.native_observation_base64 == "c2FuaXRpemVk"
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
    digest=shim.hashlib.sha256(shim.json.dumps(shim.daemon_options(config)["openshell"],sort_keys=True,separators=(",",":"),ensure_ascii=False).encode()).hexdigest()
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


def test_native_observation_report_remains_unready_and_allocation_owned(tmp_path, monkeypatch):
    e = executor(tmp_path)
    raw = native_config(tmp_path)
    e.apps_file.write_text(yaml.safe_dump(raw))
    replica = e.runtimes.bind(reconcile.replica_specs(reconcile.load_apps(raw))["web-0"], "launch")
    health = ReplicaHealth(name="web-0", spec=actors.spec_hash(replica), executor=ExecutorKind.SHIM,
        execution=replica.execution, ready=False, allocation_reserved=True,
        native_observation_base64="c2FuaXRpemVk")
    monkeypatch.setattr(node, "alive_nodes", lambda: [{}])
    monkeypatch.setattr(node, "resources", lambda: ({"GPU": 0, "CPU": 1}, {"GPU": 0, "CPU": 0}))
    monkeypatch.setattr(reconcile, "running", lambda: {"web-0": object()})
    monkeypatch.setattr(reconcile, "health_of", lambda *a, **kw: health)
    monkeypatch.setattr(shim, "capabilities", lambda _: [])
    record = e.report()["runtime_instances"][0]
    assert record["native_observation_base64"] == health.native_observation_base64
    assert not record["ready"] and record["services"] == []
    health.execution = replace(replica.execution, lease_id="obsolete")
    assert e.report().get("runtime_instances", []) == []


def native_service_config(tmp_path):
    raw = native_config(tmp_path)
    raw["apps"][0].update(services=["http"], container_port=8080, health_path="/health")
    raw["shim_openshell"]["supervisor_image"] = "sha256:" + "a" * 64
    raw["shim_network"] = {"gateway_url":"http://127.0.0.1:9191", "relay_url":"ws://127.0.0.1:9999",
        "token_file":str(tmp_path/"token"),"public_key_file":str(tmp_path/"public"),"issuer":"test"}
    return raw


def test_native_services_report_local_health_without_waiting_for_registration(tmp_path, monkeypatch):
    raw = native_service_config(tmp_path)
    cfg = reconcile.load_apps(raw)
    spec = shim.RuntimeBook(tmp_path/"book").bind(reconcile.replica_specs(cfg)["web-0"], "launch")
    assert shim.required_capability(spec) == "shim-openshell-services-v1"
    commands = []
    status = {"phase":"created", "platform_ready":True, "application_ready":False,
        "native_observation_base64":"c2FuaXRpemVk"}
    def invoke(binary, state, node, command, **kwargs):
        assert kwargs == {"daemon":True}
        commands.append(command)
        if command["operation"] == "network":
            assert command["driver"] == "openshell" and command["config"] == cfg.cluster.shim_network
            return {"connected_worker":True}
        return dict(status)
    monkeypatch.setattr(shim,"invoke",invoke)
    actor = shim._ProvisionedApp(spec)
    assert not ReplicaHealth.load(actor.health()).is_ready
    status["application_ready"] = True
    assert ReplicaHealth.load(actor.health()).is_ready
    status["platform_ready"] = False
    assert not ReplicaHealth.load(actor.health()).is_ready
    assert [c["operation"] for c in commands] == ["allocate","inspect","network","inspect","network","inspect"]


def test_native_service_capability_requires_health_protocol_and_network_config(tmp_path, monkeypatch):
    cfg = reconcile.load_apps(native_service_config(tmp_path)).cluster
    digest=shim.hashlib.sha256(shim.json.dumps(shim.daemon_options(cfg)["openshell"],sort_keys=True,separators=(",",":"),ensure_ascii=False).encode()).hexdigest()
    value={"protocol":1,"drivers":["docker","openshell"],"openshell_config_sha256":digest,
        "features":["openshell-provisioning-v1","openshell-allocation-v1","openshell-local-services-v1"]}
    monkeypatch.setattr(shim,"daemon_call",lambda *a,**k:value)
    assert "shim-openshell-services-v1" not in shim.capabilities(cfg)
    value["features"].append("openshell-service-health-v1")
    assert "shim-openshell-services-v1" in shim.capabilities(cfg)
    assert "shim-openshell-services-v1" not in shim.capabilities(replace(cfg,shim_network=None))


@pytest.mark.parametrize("key,value", [("services",["control"]),("container_port",None),
    ("service_protocol","tcp"),("health_path","//elsewhere/health"),("health_path","/a\\b"),
    ("health_path","/a#b"),("health_path","/a\n"),("port_base",0)])
def test_native_service_profile_rejects_unbound_targets(tmp_path,key,value):
    raw=native_service_config(tmp_path);raw["apps"][0][key]=value
    with pytest.raises(ValueError): reconcile.load_apps(raw)


def test_native_service_report_keeps_compute_observation_when_app_becomes_healthy(tmp_path, monkeypatch):
    e=executor(tmp_path);raw=native_service_config(tmp_path)
    e.apps_file.write_text(yaml.safe_dump(raw))
    replica=e.runtimes.bind(reconcile.replica_specs(reconcile.load_apps(raw))["web-0"],"launch")
    health=ReplicaHealth(name="web-0",spec=actors.spec_hash(replica),executor=ExecutorKind.SHIM,
        execution=replica.execution,ready=True,allocation_reserved=True,native_observation_base64="c2FuaXRpemVk")
    monkeypatch.setattr(node,"alive_nodes",lambda:[{}])
    monkeypatch.setattr(node,"resources",lambda:({"GPU":0,"CPU":1},{"GPU":0,"CPU":0}))
    monkeypatch.setattr(reconcile,"running",lambda:{"web-0":object()})
    monkeypatch.setattr(reconcile,"health_of",lambda *a,**k:health)
    monkeypatch.setattr(shim,"capabilities",lambda _:[])
    report=e.report();instance=report["runtime_instances"][0]
    assert report["apps"][0]["ready"] and instance["ready"] and instance["services"]==["http"]
    assert instance["native_observation_base64"]==health.native_observation_base64
    health.ready=False
    instance=e.report()["runtime_instances"][0]
    assert not instance["ready"] and instance["services"]==[]


def tee_config(mode="simulation"):
    return dict(mode=mode, platform="sample" if mode == "simulation" else "snp",
        evidence_endpoint="http://127.0.0.1:8006/aa/evidence", trustee_endpoint="http://127.0.0.1:50004",
        trustee_policy_ids=["default"], init_data_sha256="11"*32, endpoint_key_sha256="22"*32,
        expected_measurement="abcde", proof_endpoint="https://proof.example", proof_token_file="/private/proof.token",
        program=dict(kind="remote_attestation", program_sha256="aa"*32, verifying_key_sha256="bb"*32),
        program_vkey_hash="0x0000", elf_file="/opt/ra.elf")


@pytest.mark.parametrize("mode,driver", [("simulation","tee-simulation"),("hardware","tee")])
def test_tee_configuration_capabilities_and_launch_are_bound(tmp_path, monkeypatch, mode, driver):
    import hashlib
    raw = config(tmp_path)
    raw.update(shim_daemon=True, shim_tee=tee_config(mode), shim_machine_labels={"runtime.driver":driver})
    raw["apps"][0]["runtime"]["driver"] = driver
    spec = reconcile.replica_specs(reconcile.load_apps(raw))["web-0"]
    machine = shim.daemon_options(spec.cluster)["machine"]
    digest = hashlib.sha256(shim.json.dumps(machine, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
    response = dict(protocol=1, drivers=[driver], machine_config_sha256=digest,
        features=["runtime-daemon-v1", "tee-startup-v1", "data-plane-v1", "runtime-management-v1"])
    monkeypatch.setattr(shim, "daemon_call", lambda *a, **k: response)
    assert shim.required_capability(spec) in shim.capabilities(spec.cluster)
    assert "shim-docker-v1" not in shim.capabilities(spec.cluster)
    docker_spec = replace(spec, app=replace(spec.app, runtime=replace(spec.app.runtime, driver="docker")))
    assert not shim.required_capabilities(docker_spec).issubset(shim.capabilities(spec.cluster))
    assert "shim-prover-v1" not in shim.capabilities(spec.cluster)
    other = "shim-tee-v1" if mode == "simulation" else "shim-tee-simulation-v1"
    assert other not in shim.capabilities(spec.cluster)
    bound = shim.RuntimeBook(tmp_path / "book").bind(spec, "launch")
    request = shim.launch_request(bound, [])
    assert request["driver"] == driver
    assert request["trust"]["attestation"] == {"mode":"required","profile":raw["shim_tee"]["platform"]}
    assert request["trust"]["attestation_proof"] == "required"
    response["machine_config_sha256"] = "changed"
    assert shim.capabilities(spec.cluster) == []
    with pytest.raises(RuntimeError, match="machine settings differ"):
        shim.check_daemon(spec.cluster)
    for changes in ({"shim_daemon":False}, {"shim_tee":None}, {"shim_tee":tee_config("hardware" if mode == "simulation" else "simulation")}):
        with pytest.raises(ValueError):
            reconcile.load_apps({**raw, **changes})


def test_tee_lifecycle_preserves_driver_and_extends_launch_timeout(tmp_path, monkeypatch):
    raw = config(tmp_path); raw.update(shim_daemon=True, shim_tee=tee_config())
    raw["apps"][0]["runtime"]["driver"] = "tee-simulation"
    spec = reconcile.replica_specs(reconcile.load_apps(raw))["web-0"]
    bound = shim.RuntimeBook(tmp_path / "book").bind(spec, "launch")
    actor = object.__new__(shim._ShimApp); actor.spec = bound
    call = Mock(return_value={"cleaned":True}); monkeypatch.setattr(shim, "daemon_call", call)
    actor.call({"operation":"launch", "request":shim.launch_request(bound, [])})
    assert call.call_args.kwargs["timeout"] == 330
    for operation in ("inspect", "stop", "delete", "network"):
        actor.call({"operation":operation,"execution":bound.execution})
        assert call.call_args.args[2]["driver"] == "tee-simulation"
    shim.RuntimeBook(tmp_path / "book").cleanup(bound.name)
    assert call.call_args.args[2]["driver"] == "tee-simulation"


@pytest.mark.parametrize("mode", ["simulation", "hardware"])
def test_native_openshell_composes_with_node_tee_profile(tmp_path, monkeypatch, mode):
    import hashlib
    raw = native_config(tmp_path)
    raw.update(shim_tee=tee_config(mode), shim_machine_labels={"runtime.driver":"auto"})
    spec = reconcile.replica_specs(reconcile.load_apps(raw))["web-0"]
    options = shim.daemon_options(spec.cluster)
    digest = lambda value: hashlib.sha256(shim.json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
    active = dict(protocol=1, drivers=["openshell"],
        features=["runtime-daemon-v1", "tee-startup-v1", "openshell-provisioning-v1", "openshell-allocation-v1"],
        machine_config_sha256=digest(options["machine"]), openshell_config_sha256=digest(options["openshell"]))
    monkeypatch.setattr(shim, "daemon_call", lambda *a, **kw: active)
    assert "shim-openshell-provisioning-v1" in shim.capabilities(spec.cluster)
    assert "shim-docker-v1" not in shim.capabilities(spec.cluster)
    active["machine_config_sha256"] = "mismatched-profile"
    assert shim.capabilities(spec.cluster) == []


@pytest.mark.parametrize("driver", ["docker", "tee", "tee-simulation"])
def test_access_rules_do_not_change_runtime_driver(tmp_path, monkeypatch, driver):
    raw = native_config(tmp_path)
    raw["apps"][0]["runtime"]["driver"] = driver
    raw["apps"][0]["access_policy_sha256"] = "ab" * 32
    if driver != "docker":
        raw["shim_tee"] = tee_config("hardware" if driver == "tee" else "simulation")
    spec = reconcile.replica_specs(reconcile.load_apps(raw))["web-0"]
    assert spec.app.native_access
    spec = shim.RuntimeBook(tmp_path / "book").bind(spec, "launch")
    required = shim.required_capabilities(spec)
    assert "shim-openshell-access-v1" in required
    assert "shim-openshell-provisioning-v1" in required
    assert "shim-" + driver + "-v1" in required
    allocation = shim.RuntimeBook(tmp_path / "book").read(spec.name)
    assert allocation.driver == driver
    assert allocation.access_policy_sha256 == "ab" * 32
    calls = []
    def invoke(*args, **kwargs):
        calls.append(args[3]); return {"phase":"reserved", "cleaned":True}
    monkeypatch.setattr(shim, "invoke", invoke)
    shim._ProvisionedApp(spec)
    shim.RuntimeBook(tmp_path / "book").cleanup(spec.name)
    assert all(c.get("driver", "docker") == driver for c in calls)
    assert [c["operation"] for c in calls] == ["allocate", "allocate", "delete"]
