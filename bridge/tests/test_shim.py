import copy
import re
import socket
from dataclasses import replace
from unittest.mock import Mock

import pytest
import yaml

from fleet_bridge import reconcile, shim, node
from fleet_bridge.model import ExecutorKind, ReplicaHealth, AppSpec

PULL_IMAGE, IMAGE_PRESENT, FOLLOW = shim.pull_image, shim.image_present, shim.LogFollower.follow
MULTIPLEXED = "application/vnd.docker.multiplexed-stream"


@pytest.fixture(autouse=True)
def no_docker(monkeypatch):
    """No actor here reaches a Docker daemon: tests of pulls and log followers
    drive the real functions against their own fakes. Every image counts as
    present. Returns the pulls asked for."""
    pulls = []
    monkeypatch.setattr(shim, "image_present", lambda image: True)
    monkeypatch.setattr(shim, "pull_image", lambda *args: pulls.append(args))
    monkeypatch.setattr(shim.LogFollower, "follow", lambda self: None)
    return pulls


@pytest.fixture
def docker_api(tmp_path):
    """A Docker Engine API on a unix socket. Each request gets the next
    (status, content type, body), the last one repeating. Returns the socket
    path and the request paths it saw."""
    import http.server
    import socketserver
    import threading
    servers = []
    def serve(answers):
        seen = []
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                seen.append(self.path)
                status, kind, body = answers.pop(0) if len(answers) > 1 else answers[0]
                self.send_response(status)
                self.send_header("Content-Type", kind)
                self.end_headers()
                self.wfile.write(body)
            def log_message(self, *args):
                pass
        class Server(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
            daemon_threads = True
        server = Server(str(tmp_path / f"docker-{len(servers)}.sock"), Handler)
        servers.append(server)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        return server.server_address, seen
    yield serve
    for server in servers:
        server.shutdown()
        server.server_close()


def frame(stream, payload):
    """One message of Docker's multiplexed log stream."""
    return bytes([stream, 0, 0, 0]) + len(payload).to_bytes(4, "big") + payload


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
    assert shim.spec_hash(first) == shim.spec_hash(reconcile.replica_specs(reconcile.load_apps(raw))['web-0'])
    bound = shim.RuntimeBook(tmp_path / 'book').bind(first, 'admitted-command')
    request = shim.launch_request(bound, ['4', '6'])
    assert shim.required_capability(bound) == 'shim-prover-v1'
    assert request['environment'] == dict(NAME='web-0', MACHINE=socket.gethostname(),
        CPUS='4', GPUS='2', TOKEN_FILE='/private/node.token')
    assert request['driver_options']['gpus'] == ['4', '6']
    assert request['driver_options']['volumes'] == ['/srv/proofs:/srv/proofs', '/private/node.token:/private/node.token:ro']
    # CPUs are a Ray reservation and a template value, not a container limit.
    assert request['limits'] == {'cpu_millicores': None, 'memory_bytes': None}
    for update in ({'runtime':{'executor':'shim','driver':'openshell'}}, {'container_port':None},
                   {'services':['http']}, {'service_protocol':'tcp'}):
        invalid = copy.deepcopy(raw)
        invalid['apps'][0].update(update)
        with pytest.raises(ValueError):
            reconcile.load_apps(invalid)


def test_a_prover_on_the_host_network_listens_on_its_own_port(tmp_path):
    raw = config(tmp_path)
    raw['apps'][0].update(kind="ziren-gpu", elf_id="a" * 64, proof_storage_root="/srv/proofs",
        port_base=0, container_port=8080, network="host", command=["serve", "{port}"])
    bound = shim.RuntimeBook(tmp_path / 'book').bind(
        reconcile.replica_specs(reconcile.load_apps(raw))['web-0'], 'admitted-command')
    request = shim.launch_request(bound, [])
    options = request['driver_options']
    # Nothing maps a port there, so the shim gets the one the prover listens on.
    assert (options['network'], options['port'], 'container_port' in options) == ('host', 8080, False)
    assert request['command'] == ['serve', '8080']
    assert shim.HOST_NETWORK_CAPABILITY in shim.required_capabilities(bound)
    for update in ({'network': 'none'}, {'port_base': 18080}, {'replicas': 2}, {'kind': 'app', 'elf_id': None}):
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
    monkeypatch.setattr(shim, "_http_ok", lambda _: pytest.fail("TCP must not send HTTP to SSH"))
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


def config(tmp_path):
    app = {"name":"web", "kind":"app", "image":"python:3.12-slim", "gpus":0,
        "cpus":0.5, "memory_gb":0.125, "port_base":18080, "command":["serve", "{port}"],
        "runtime":{"executor":"shim", "driver":"docker"}}
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
    with pytest.raises(RuntimeError, match="shim daemon unavailable"):
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


def resumable_actor(tmp_path, monkeypatch, states):
    s = spec(tmp_path)
    actor = object.__new__(shim._ShimApp)
    actor.spec = replace(s, cluster=replace(s.cluster, shim_daemon=True))
    actor.port, actor.gpus, actor.network, actor.resumable = 12345, [], None, True
    calls = []
    def call(command, timeout=None):
        calls.append(command["operation"])
        if command["operation"] == "resume":
            return {"state": states.pop(0) if states else "ready"}
        return {"state": "stopped"} if "resume" not in calls or states else {"state": "ready"}
    actor.call = call
    actor.start_network = Mock()
    actor.application_ready = Mock(return_value=True)
    clock = [1000.0]
    monkeypatch.setattr(shim.time, "monotonic", lambda: clock[0])
    return actor, calls, clock


def test_crashed_daemon_runtime_is_resumed_in_place_and_reconnected(tmp_path, monkeypatch):
    actor, calls, _ = resumable_actor(tmp_path, monkeypatch, ["ready"])
    value = actor.health()
    assert calls == ["inspect", "resume"]
    assert value["ready"] is True and value["management_ready"] is True
    # The resumed runtime's routes are registered again in the same probe.
    actor.start_network.assert_called_once()


def test_resume_backs_off_and_recovers_after_a_stable_period(tmp_path, monkeypatch):
    actor, calls, clock = resumable_actor(tmp_path, monkeypatch, ["stopped", "stopped", "stopped"])
    assert actor.health()["ready"] is False
    actor.health()
    assert calls.count("resume") == 1  # within the first 5 s delay
    clock[0] += 5
    actor.health()
    clock[0] += 5
    actor.health()
    assert calls.count("resume") == 2  # now 10 s
    clock[0] += 5
    actor.health()
    assert calls.count("resume") == 3
    assert actor.resume_at - clock[0] == 20
    # A failing resume is logged and the observed state is still reported.
    actor.call = Mock(side_effect=lambda command, timeout=None: {"state": "stopped"}
        if command["operation"] == "inspect" else (_ for _ in ()).throw(RuntimeError("refused")))
    clock[0] += 3600
    assert actor.health()["ready"] is False
    assert actor.resumes == 4 and actor.resume_at - clock[0] == 40
    for _ in range(10):
        actor.resume_at = 0
        actor.health()
    assert actor.resume_at - clock[0] == 300
    # Healthy long enough after the last resume: the next crash starts over.
    actor.call = Mock(return_value={"state": "ready"})
    actor.health()
    assert actor.resumes == 14
    clock[0] += shim.RESUME_STABLE_S
    actor.health()
    assert actor.resumes == 0 and actor.resume_at == 0


@pytest.mark.parametrize("state", ["starting", "stopping", "failed", "ready"])
def test_only_an_exited_runtime_is_resumed(tmp_path, monkeypatch, state):
    actor, _, _ = resumable_actor(tmp_path, monkeypatch, [])
    actor.call = Mock(return_value={"state": state})
    actor.health()
    assert [c.args[0]["operation"] for c in actor.call.call_args_list] == ["inspect"]


def test_resume_requires_daemon_docker_and_advertised_support(tmp_path, monkeypatch):
    actor, _, _ = resumable_actor(tmp_path, monkeypatch, [])
    actor.resumable = False
    actor.call = Mock(return_value={"state": "stopped"})
    assert actor.health()["ready"] is False
    assert [c.args[0]["operation"] for c in actor.call.call_args_list] == ["inspect"]
    # Instances created before this upgrade never resume.
    legacy = object.__new__(shim._ShimApp)
    assert legacy.resumable is False and legacy.resumes == 0
    s = spec(tmp_path)
    daemon = replace(s.cluster, shim_daemon=True)
    value = {"protocol": 1, "drivers": ["docker"], "features": ["data-plane-v1", "runtime-management-v1"]}
    monkeypatch.setattr(shim, "daemon_call", lambda *a, **k: value)
    monkeypatch.setattr(shim, "check_machine", lambda *a: None)
    assert "shim-resume-v1" not in shim.capabilities(daemon)
    value["features"].append("runtime-resume-v1")
    assert "shim-resume-v1" in shim.capabilities(daemon)
    # The one-shot CLI cannot keep a resumed runtime connected.
    monkeypatch.setattr(shim.subprocess, "run", lambda *a, **k: Mock(stdout=shim.json.dumps(value)))
    assert "shim-resume-v1" not in shim.capabilities(s.cluster)
    value["drivers"] = ["tee"]
    assert "shim-resume-v1" not in shim.capabilities(daemon)


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


NETWORK = {"gateway_url":"https://control.example", "relay_url":"wss://data.example",
    "token_file":"/private/node.jwt", "public_key_file":"/private/gateway.pem", "issuer":"control"}


def checked_node(tmp_path, features):
    """An apps.yaml with a tunnel helper and a per-call shim that reports `features`."""
    import json
    runtime = tmp_path / "trusted-shim-runtime"
    runtime.write_text("#!/bin/sh\necho '" + json.dumps({"protocol":1,"drivers":["docker"],"features":features}) + "'\n")
    runtime.chmod(0o755)
    helper = tmp_path / "trusted-router-tunnel"
    helper.write_bytes(b"fixture")
    helper.chmod(0o755)
    raw = config(tmp_path)
    raw.update(shim_binary=str(runtime), shim_network={**NETWORK, "tunnel_binary":str(helper)})
    path = tmp_path / "apps.yaml"
    path.write_text(yaml.safe_dump(raw))
    return path, raw, helper


def run_check(path, *flags):
    import subprocess
    import sys
    from pathlib import Path
    # This checkout's bridge, not an installed copy.
    return subprocess.run([sys.executable, "-m", "fleet_bridge.shim", "check", "--apps", str(path), *flags],
        cwd=Path(shim.__file__).parents[1], capture_output=True, text=True, timeout=60)


def test_check_reads_the_whole_file_and_warns_about_the_tunnel_helper_unless_network(tmp_path):
    path, raw, helper = checked_node(tmp_path, ["data-plane-v1", "chisel-network-v1"])
    for flags in ((), ("--network",)):
        result = run_check(path, *flags)
        assert (result.returncode, result.stderr) == (0, ""), result.stderr
    # Node startup runs the check: a helper shim can't use only withdraws network capabilities.
    helper.chmod(0o775)
    result = run_check(path)
    assert result.returncode == 0 and result.stderr.startswith("WARNING: ")
    assert "group or others can write it" in result.stderr and "network capabilities" in result.stderr
    result = run_check(path, "--network")
    assert result.returncode == 1 and "shim check failed:" in result.stderr
    assert "group or others can write it" in result.stderr
    # A bad app entry is a warning: the agent must start so the scheduler can repair it.
    helper.chmod(0o755)
    raw["apps"] = [*raw.get("apps", []), {"name": "stray", "kind": "app", "runtime": {"executor": "shim", "driver": "docker"}, "elf_id": "a" * 64}]
    path.write_text(yaml.safe_dump(raw))
    for flags in ((), ("--network",)):
        result = run_check(path, *flags)
        assert result.returncode == 0, result.stderr
        assert "WARNING: apps.yaml: stray: only a Ziren prover proves a program" in result.stderr
        assert "fixed or disabled through the scheduler" in result.stderr
    raw["apps"].pop()
    # What the node-wide settings can't parse fails in both modes, with the reason.
    raw["shim_network"]["tunnel_binary"] = "relative/helper"
    path.write_text(yaml.safe_dump(raw))
    result = run_check(path)
    assert result.returncode == 1 and "shim check failed:" in result.stderr and "absolute" in result.stderr
    assert "Traceback" not in result.stderr


def test_check_requires_chisel_from_the_running_shim_only_with_network(tmp_path, monkeypatch):
    path, raw, helper = checked_node(tmp_path, ["data-plane-v1"])
    [warning] = shim.check(path)
    assert "does not report chisel-network-v1" in warning
    with pytest.raises(RuntimeError, match="chisel-network-v1"):
        shim.check(path, network=True)
    helper.unlink()
    with pytest.raises(RuntimeError, match="trusted-router-tunnel: No such file.*chisel-network-v1"):
        shim.check(path, network=True)
    # A shim that cannot answer is reported as such, not as one lacking the feature.
    helper.write_bytes(b"fixture"); helper.chmod(0o755)
    monkeypatch.setattr(shim.subprocess, "run", Mock(side_effect=OSError("Exec format error")))
    [warning] = shim.check(path)
    assert "could not ask" in warning and "Exec format error" in warning
    assert "does not report" not in warning
    monkeypatch.undo()
    helper.unlink()
    # A persistent companion reports its own features.
    helper.write_bytes(b"fixture"); helper.chmod(0o755)
    raw.update(shim_daemon=True, shim_binary="/opt/trusted-shim-runtime")
    path.write_text(yaml.safe_dump(raw))
    value = {"protocol":1, "features":["runtime-daemon-v1"]}
    monkeypatch.setattr(shim, "daemon_call", lambda *a, **k: value)
    monkeypatch.setattr(shim.subprocess, "run", Mock(side_effect=AssertionError("ask the running companion")))
    with pytest.raises(RuntimeError, match="chisel-network-v1"):
        shim.check(path, network=True)
    value["features"].append("chisel-network-v1")
    assert shim.check(path, network=True) == []
    del raw["shim_network"]["tunnel_binary"]
    value["features"].remove("chisel-network-v1")
    path.write_text(yaml.safe_dump(raw))
    assert shim.check(path, network=True) == []
    raw["apps"].append({"name":"broken", "kind":"app", "runtime":{"executor":"shim", "driver":"docker"}, "elf_id":"abc"})
    path.write_text(yaml.safe_dump(raw))
    [warning] = shim.check(path, network=True)
    assert "broken: only a Ziren prover" in warning


@pytest.mark.parametrize("runtime", [None, {"executor":"unknown"}, {"executor":"shim", "driver":"openshell"},
    {"executor":"shim", "driver":"docker", "policy_proof":"required"}])
def test_unsupported_or_misspelled_runtime_is_not_ignored(tmp_path, runtime):
    cfg = config(tmp_path); cfg["apps"][0]["runtime"] = runtime
    with pytest.raises((ValueError, TypeError)):
        reconcile.load_apps(cfg)
    if runtime is not None:
        with pytest.raises(ValueError):
            AppSpec.load(cfg["apps"][0])


def test_lease_is_persisted_and_recovery_cannot_change_request(tmp_path):
    s = spec(tmp_path); book = shim.RuntimeBook(tmp_path/"runtimes")
    first = book.bind(s, "command-one")
    recovered = shim.RuntimeBook(book.root).bind(s, "different-heartbeat-command")
    assert recovered.execution == first.execution
    assert recovered.execution.node_id == socket.gethostname()
    assert recovered.launch_revision == first.launch_revision == shim.LAUNCH_REVISION
    assert shim.launch_request(recovered, []) == shim.launch_request(first, [])
    changed = replace(s, config={**s.config,"image":"different"})
    with pytest.raises(ValueError, match="active shim allocation"):
        book.bind(changed,"command-two")


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


def test_cleanup_waits_for_the_grace_the_app_was_launched_with(tmp_path, monkeypatch):
    s = spec(tmp_path)
    s = replace(s, app=replace(s.app, stop_timeout_s=600))
    book = shim.RuntimeBook(tmp_path / "runtimes")
    book.bind(s, "first")
    call = Mock(return_value={"cleaned": True})
    monkeypatch.setattr(shim, "invoke", call)
    book.cleanup(s.name)
    assert call.call_args.kwargs["timeout"] == 720


def executor(tmp_path):
    apps=tmp_path/"apps.yaml";apps.write_text(yaml.safe_dump(config(tmp_path)))
    batches=tmp_path/"batches.yaml";batches.write_text("batches: {}\n")
    e=node.NodeExecutor(str(apps),str(batches),str(tmp_path/"jobs"));e.pool.shutdown()
    return e


def test_a_start_of_a_started_shim_replica_that_is_not_ready_changes_nothing(tmp_path,monkeypatch):
    e=executor(tmp_path)
    app=config(tmp_path)["apps"][0];app.update(namespace="0"*32,app="web",elf_id="")
    admitted=shim.spec_hash(spec(tmp_path))
    monkeypatch.setattr(node,"alive_nodes",lambda:[{}])
    monkeypatch.setattr(shim,"capabilities",lambda _:[shim.CAPABILITY])
    monkeypatch.setattr(reconcile,"running",lambda:{"web-0":object()})
    start,stop=Mock(),Mock();monkeypatch.setattr(reconcile,"start",start);monkeypatch.setattr(reconcile,"stop",stop)
    unready=lambda digest:ReplicaHealth(name="web-0",spec=digest,executor=shim.ExecutorKind.SHIM,management_ready=True,ready=False)
    # Its runtime makes it ready: the start has nothing to do, and is no error.
    monkeypatch.setattr(reconcile,"health_of",lambda _:unready(admitted))
    assert e.set_app({"app":app,"disabled":False},"start")=={"disabled":False,"ready":True}
    # One that runs something else stays until it is stopped.
    monkeypatch.setattr(reconcile,"health_of",lambda _:unready("another"))
    with pytest.raises(ValueError,match="differs from the admitted one; retained until explicit cleanup"):
        e.set_app({"app":app,"disabled":False},"start")
    start.assert_not_called();stop.assert_not_called()
    assert e.runtimes.read("web-0") is None


def test_a_failed_start_keeps_its_allocation(tmp_path,monkeypatch):
    e=executor(tmp_path)
    app=config(tmp_path)["apps"][0];app.update(namespace="0"*32,app="web",elf_id="")
    monkeypatch.setattr(node,"alive_nodes",lambda:[{}]);monkeypatch.setattr(reconcile,"running",lambda:{})
    monkeypatch.setattr(shim,"capabilities",lambda _:[shim.CAPABILITY])
    start=Mock(side_effect=RuntimeError("shim unavailable"));monkeypatch.setattr(reconcile,"start",start)
    with pytest.raises(RuntimeError):e.set_app({"app":app,"disabled":False},"start")
    assert start.call_count==1
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


def test_unhealthy_app_reports_only_its_owned_management_service(tmp_path, monkeypatch):
    e = executor(tmp_path)
    raw = yaml.safe_load(e.apps_file.read_text())
    raw['shim_network'] = {'gateway_url': 'https://control.example', 'relay_url': 'wss://data.example',
        'token_file': '/private/node.jwt', 'public_key_file': '/private/gateway.pem', 'issuer': 'test'}
    raw['apps'][0]['services'] = ['http', 'control']
    e.apps_file.write_text(yaml.safe_dump(raw))
    replica = e.runtimes.bind(reconcile.replica_specs(reconcile.load_apps(raw))['web-0'], 'launch')
    health = ReplicaHealth(name='web-0', spec=shim.spec_hash(replica), executor=ExecutorKind.SHIM,
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
    assert set(shim.capabilities(parsed.cluster)) == {"shim-cleanup-v1", shim.CAPABILITY, shim.LIMITS_CAPABILITY}
    monkeypatch.setattr(shim.subprocess,"run",lambda *a,**k:Mock(stdout='{"protocol":1,"drivers":["docker"],"features":["data-plane-v1"]}'))
    assert "shim-network-v1" in shim.capabilities(parsed.cluster)
    # An entry without services is hashed without the node's network settings.
    plain=config(tmp_path)
    before=shim.spec_hash(reconcile.replica_specs(reconcile.load_apps(plain))["web-0"])
    plain["shim_network"]=cfg["shim_network"]
    assert shim.spec_hash(reconcile.replica_specs(reconcile.load_apps(plain))["web-0"])==before


def test_chisel_selection_preserves_legacy_wire_and_requires_a_ready_companion(tmp_path, monkeypatch):
    from fleet_bridge.model import to_dict
    cfg = config(tmp_path)
    cfg["shim_network"] = {"gateway_url":"https://control.example", "relay_url":"wss://data.example",
        "token_file":"/private/node.jwt", "public_key_file":"/private/gateway.pem", "issuer":"control"}
    parsed = reconcile.load_apps(cfg)
    assert to_dict(parsed.cluster.shim_network) == cfg["shim_network"]
    helper = tmp_path / "trusted-router-tunnel"
    helper.write_bytes(b"fixture")
    helper.chmod(0o755)
    cfg["shim_network"]["tunnel_binary"] = str(helper)
    parsed = reconcile.load_apps(cfg)
    monkeypatch.setattr(shim.subprocess,"run",lambda *a,**k:Mock(stdout='{"protocol":1,"drivers":["docker"],"features":["data-plane-v1"]}'))
    assert "shim-network-v1" not in shim.capabilities(parsed.cluster)
    monkeypatch.setattr(shim.subprocess,"run",lambda *a,**k:Mock(stdout='{"protocol":1,"drivers":["docker"],"features":["data-plane-v1","chisel-network-v1"]}'))
    assert "shim-chisel-network-v1" in shim.capabilities(parsed.cluster)
    # Shim refuses a helper others can modify; never advertise one it would reject.
    # Runtimes without services never use the helper and stay supported.
    monkeypatch.setattr(shim.subprocess,"run",lambda *a,**k:Mock(stdout='{"protocol":1,"drivers":["docker"],'
        '"features":["data-plane-v1","chisel-network-v1","runtime-management-v1"]}'))
    unrelated = {"shim-runtime-v2", "shim-automatic-port-v1", "shim-prover-v1"}
    assert unrelated <= set(shim.capabilities(parsed.cluster))
    for mode in (0o775, 0o757, 0o644):
        helper.chmod(mode)
        advertised = set(shim.capabilities(parsed.cluster))
        assert not advertised & {"shim-network-v1", "shim-chisel-network-v1", "shim-management-v1"}, oct(mode)
        assert unrelated <= advertised, oct(mode)
    helper.chmod(0o755)
    helper.unlink()
    advertised = set(shim.capabilities(parsed.cluster))
    assert "shim-network-v1" not in advertised and unrelated <= advertised
    cfg["shim_network"]["tunnel_binary"] = "relative/helper"
    with pytest.raises(ValueError, match="absolute"):
        reconcile.load_apps(cfg)


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
        assert kwargs["daemon"] is True and kwargs.get("timeout") == (150 if command["operation"] == "delete" else None)
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
    assert health.spec == shim.spec_hash(spec)
    assert calls == ["allocate", "inspect"]
    assert book.read(spec.name).driver == "openshell"
    book.cleanup(spec.name)
    assert calls[-2:] == ["allocate", "delete"]
    assert book.read(spec.name).cleaned
    with pytest.raises(ValueError): book.bind(spec,"launch")
    next_spec = book.bind(spec,"replacement")
    assert next_spec.execution.generation == spec.execution.generation + 1


@pytest.mark.parametrize("key,value", [("port_base",123), ("services",["http"]), ("gpus",0.5), ("gpus",17),
    ("volumes",["/tmp"]),("memory_gb",0),("cpus",0.5),("user","root"),("entrypoint","/bin/sh")])
def test_native_profile_rejects_unmapped_settings(tmp_path,key,value):
    data=native_config(tmp_path);data["apps"][0][key]=value
    with pytest.raises(ValueError): reconcile.load_apps(data)


def attached_config(tmp_path):
    data = native_config(tmp_path)
    data["apps"][0].update(gpus=2, mounts=[{"name":"data","target":"/data"},
        {"name":"models","target":"/models","read_only":True}])
    return data


@pytest.mark.parametrize("mounts", [[{"name":"Data","target":"/d"}], [{"name":"d","target":"d"}],
    [{"name":"d","target":"/"}], [{"name":"d","target":"/a/../b"}], [{"name":"d","target":"/a:ro"}],
    [{"name":"d","target":"/a b"}], [{"name":"d","target":"/d","source":"/host"}],
    [{"name":"d","target":"/a"},{"name":"d","target":"/b"}], [{"name":"a","target":"/d"},{"name":"b","target":"/d"}],
    [{"name":f"v{i}","target":f"/v{i}"} for i in range(17)]])
def test_native_durable_mounts_reject_unscoped_shapes(tmp_path, mounts):
    data = attached_config(tmp_path); data["apps"][0]["mounts"] = mounts
    with pytest.raises(ValueError): reconcile.load_apps(data)


def test_native_attachments_require_node_capability_and_stay_out_of_docker_and_tee(tmp_path, monkeypatch):
    spec = reconcile.replica_specs(reconcile.load_apps(attached_config(tmp_path)))["web-0"]
    assert [m.target for m in spec.app.mounts] == ["/data", "/models"] and spec.app.mounts[1].read_only
    assert "shim-openshell-attachments-v1" in shim.required_capabilities(spec)
    plain = reconcile.replica_specs(reconcile.load_apps(native_config(tmp_path)))["web-0"]
    assert "shim-openshell-attachments-v1" not in shim.required_capabilities(plain)
    docker = config(tmp_path); docker["apps"][0]["mounts"] = [{"name":"d","target":"/d"}]
    with pytest.raises(ValueError): reconcile.load_apps(docker)
    tee = attached_config(tmp_path)
    tee["apps"][0].update(access_policy_sha256="ab"*32, runtime={"executor":"shim","driver":"tee"})
    with pytest.raises(ValueError, match="non-TEE"): reconcile.load_apps(tee)
    config_ = reconcile.load_apps(native_config(tmp_path)).cluster
    digest = shim.hashlib.sha256(shim.json.dumps(shim.daemon_options(config_)["openshell"],sort_keys=True,separators=(",",":"),ensure_ascii=False).encode()).hexdigest()
    value = {"protocol":1,"drivers":["docker"],"openshell_config_sha256":digest,
        "features":["openshell-provisioning-v1","openshell-allocation-v1"]}
    monkeypatch.setattr(shim,"daemon_call",lambda *a,**k:value)
    assert "shim-openshell-attachments-v1" not in shim.capabilities(config_)
    value["features"].append("openshell-attachments-v1")
    assert "shim-openshell-attachments-v1" in shim.capabilities(config_)


def test_native_reservation_reports_its_ray_granted_gpu_ids(tmp_path, monkeypatch):
    e = executor(tmp_path)
    raw = attached_config(tmp_path)
    e.apps_file.write_text(yaml.safe_dump(raw))
    spec = e.runtimes.bind(reconcile.replica_specs(reconcile.load_apps(raw))["web-0"], "launch")
    monkeypatch.setattr(shim, "_granted_gpus", lambda: ["3", "5"])
    monkeypatch.setattr(shim, "invoke", lambda *a, **k: {"phase":"reserved"})
    health = ReplicaHealth.load(shim._ProvisionedApp(spec).health())
    assert health.gpus == ["3", "5"] and health.allocation_reserved
    monkeypatch.setattr(node, "alive_nodes", lambda: [{}])
    monkeypatch.setattr(node, "resources", lambda: ({"GPU": 8, "CPU": 1}, {"GPU": 6, "CPU": 0}))
    monkeypatch.setattr(reconcile, "running", lambda: {"web-0": object()})
    monkeypatch.setattr(reconcile, "health_of", lambda *a, **kw: health)
    monkeypatch.setattr(shim, "capabilities", lambda _: [])
    assert e.report()["runtime_instances"][0]["gpu_ids"] == ["3", "5"]
    # Without GPUs the actor asks Ray for none.
    plain = native_config(tmp_path)
    monkeypatch.setattr(shim, "_granted_gpus", Mock(side_effect=AssertionError))
    plain_spec = shim.RuntimeBook(tmp_path/"plain").bind(reconcile.replica_specs(reconcile.load_apps(plain))["web-0"], "plain")
    assert ReplicaHealth.load(shim._ProvisionedApp(plain_spec).health()).gpus == []


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
    health = ReplicaHealth(name="web-0", spec=shim.spec_hash(replica), executor=ExecutorKind.SHIM,
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
        commands.append(command)
        if command["operation"] == "network":
            # Recovery stays within the health probe's own budget.
            assert kwargs == {"daemon":True, "timeout":shim.RECOVERY_TIMEOUT_S}
            assert command["driver"] == "openshell" and command["config"] == cfg.cluster.shim_network
            return {"connected_worker":True}
        assert kwargs == {"daemon":True, "timeout":None}
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
    assert "shim-openshell-tcp-services-v1" not in shim.capabilities(cfg)
    value["features"].append("openshell-tcp-services-v1")
    assert "shim-openshell-tcp-services-v1" in shim.capabilities(cfg)
    assert "shim-openshell-services-v1" not in shim.capabilities(replace(cfg,shim_network=None))
    assert "shim-openshell-tcp-services-v1" not in shim.capabilities(replace(cfg,shim_network=None))


def test_native_ingress_and_control_forward_caller_credentials_and_need_node_capability(tmp_path, monkeypatch):
    raw = native_service_config(tmp_path)
    raw["apps"][0].update(services=["http", "control"],
        ingress={"http":{"paths":["/v1/chat/completions"],"methods":["POST"],"single_use":True}})
    cfg = reconcile.load_apps(raw)
    spec = reconcile.replica_specs(cfg)["web-0"]
    assert "shim-openshell-ingress-v1" in shim.required_capabilities(spec)
    plain = reconcile.replica_specs(reconcile.load_apps(native_service_config(tmp_path)))["web-0"]
    assert "shim-openshell-ingress-v1" not in shim.required_capabilities(plain)
    control = copy.deepcopy(raw); control["apps"][0].pop("ingress")
    assert "shim-openshell-ingress-v1" in shim.required_capabilities(
        reconcile.replica_specs(reconcile.load_apps(control))["web-0"])
    # A native node holds no application credential; control needs an application.
    for change in ({"ingress":{"http":{"paths":["/"],"methods":["POST"],"bearer_env":"TOKEN"}},"env":{"TOKEN":"x"}},
                   {"services":["control"],"ingress":{}}, {"ingress":{"control":{"paths":["/"],"methods":["GET"]}}},
                   {"service_protocol":"tcp","health_path":"/","health_tcp_banner":"SSH-2.0-"}):
        bad = copy.deepcopy(raw); bad["apps"][0].update(change)
        with pytest.raises(ValueError):
            reconcile.load_apps(bad)
    digest=shim.hashlib.sha256(shim.json.dumps(shim.daemon_options(cfg.cluster)["openshell"],sort_keys=True,separators=(",",":"),ensure_ascii=False).encode()).hexdigest()
    value={"protocol":1,"drivers":["docker","openshell"],"openshell_config_sha256":digest,
        "features":["openshell-provisioning-v1","openshell-allocation-v1","openshell-local-services-v1","openshell-service-health-v1"]}
    monkeypatch.setattr(shim,"daemon_call",lambda *a,**k:value)
    assert "shim-openshell-ingress-v1" not in shim.capabilities(cfg.cluster)
    value["features"].append("openshell-ingress-v1")
    assert "shim-openshell-ingress-v1" in shim.capabilities(cfg.cluster)
    assert "shim-openshell-ingress-v1" not in shim.capabilities(replace(cfg.cluster,shim_network=None))


def test_native_tcp_requires_banner_health_and_distinct_node_capability(tmp_path):
    raw = native_service_config(tmp_path)
    raw["apps"][0].update(service_protocol="tcp", health_path="/", health_tcp_banner="SSH-2.0-", services=["ssh"])
    cfg = reconcile.load_apps(raw)
    spec = reconcile.replica_specs(cfg)["web-0"]
    assert shim.required_capability(spec) == "shim-openshell-tcp-services-v1"
    for banner in (None, "", "a" * 129, "SSH\n", "非ASCII"):
        bad = copy.deepcopy(raw)
        bad["apps"][0]["health_tcp_banner"] = banner
        with pytest.raises(ValueError):
            reconcile.load_apps(bad)
    for field, value in (("health_path", "/ignored"), ("service_protocol", "http1")):
        bad = copy.deepcopy(raw)
        bad["apps"][0][field] = value
        with pytest.raises(ValueError):
            reconcile.load_apps(bad)


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
    health=ReplicaHealth(name="web-0",spec=shim.spec_hash(replica),executor=ExecutorKind.SHIM,
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
    # A resume waits for its own admission too; other operations do not.
    actor.call({"operation":"resume", "execution":bound.execution})
    assert call.call_args.kwargs["timeout"] == 330 and call.call_args.args[2]["driver"] == "tee-simulation"
    actor.call({"operation":"inspect", "execution":bound.execution})
    assert call.call_args.kwargs["timeout"] == 150
    for operation in ("inspect", "stop", "delete", "network"):
        actor.call({"operation":operation,"execution":bound.execution})
        assert call.call_args.args[2]["driver"] == "tee-simulation"
    shim.RuntimeBook(tmp_path / "book").cleanup(bound.name)
    assert call.call_args.args[2]["driver"] == "tee-simulation"


@pytest.mark.parametrize("mode,driver", [("simulation","tee-simulation"),("hardware","tee")])
def test_tee_resume_needs_the_daemons_tee_resume_for_the_nodes_driver(tmp_path, monkeypatch, mode, driver):
    import hashlib
    raw = config(tmp_path)
    raw.update(shim_daemon=True, shim_tee=tee_config(mode), shim_machine_labels={"runtime.driver":driver})
    raw["apps"][0]["runtime"]["driver"] = driver
    spec = reconcile.replica_specs(reconcile.load_apps(raw))["web-0"]
    machine = shim.daemon_options(spec.cluster)["machine"]
    digest = hashlib.sha256(shim.json.dumps(machine, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
    response = dict(protocol=1, drivers=[driver], machine_config_sha256=digest,
        features=["runtime-daemon-v1", "tee-startup-v1", "data-plane-v1", "runtime-management-v1", "runtime-resume-v1"])
    monkeypatch.setattr(shim, "daemon_call", lambda *a, **k: response)
    # Docker's resume says nothing about a TEE runtime.
    assert not {"shim-resume-v1", "shim-resume-tee-v1"} & set(shim.capabilities(spec.cluster))
    response["features"].append("runtime-resume-tee-v1")
    assert "shim-resume-tee-v1" in shim.capabilities(spec.cluster)
    response["drivers"] = ["docker"]
    assert "shim-resume-tee-v1" not in shim.capabilities(spec.cluster)


def tee_bound(tmp_path):
    raw = config(tmp_path)
    raw.update(shim_daemon=True, shim_tee=tee_config())
    raw["apps"][0]["runtime"]["driver"] = "tee-simulation"
    return shim.RuntimeBook(tmp_path / "tee").bind(reconcile.replica_specs(reconcile.load_apps(raw))["web-0"], "launch")


PENDING = shim.ShimPending("shim daemon did not answer in time; the operation continues there")


def test_a_restarted_tee_actor_resumes_its_container_behind_a_fresh_admission(tmp_path, monkeypatch):
    bound = tee_bound(tmp_path)
    calls = start_actor(monkeypatch, bound, {"launch": [{"state": "stopped", "port": 18080}],
        "resume": [{"state": "ready", "port": 18080}]})
    actor = shim._ShimApp(bound)
    # The whole admission's budget (see the lifecycle test), not a probe's.
    assert calls == [("launch", None), ("resume", None)] and actor.resumable and actor.logs is None
    start_actor(monkeypatch, bound, {"launch": [{"state": "stopped", "port": 18080}]}, resumable=False)
    with pytest.raises(RuntimeError, match="web-0 is not running: its container exited; only the persistent "
                       "shim with runtime-resume-tee-v1 starts it again; explicit cleanup is required"):
        shim._ShimApp(bound)


def test_a_resume_that_outlasts_its_call_is_waited_for_as_starting(tmp_path, monkeypatch):
    bound = tee_bound(tmp_path)
    calls = start_actor(monkeypatch, bound, {"launch": [{"state": "stopped", "port": 18080}], "resume": [PENDING],
        "inspect": [{"state": "starting"}, {"state": "starting"}, {"state": "ready"}]})
    ready = iter([False, False, False, True])
    monkeypatch.setattr(shim._ShimApp, "application_ready", lambda self: next(ready))
    monkeypatch.setattr(shim.time, "sleep", lambda seconds: None)
    actor = shim._ShimApp(bound)
    assert [operation for operation, _ in calls] == ["launch", "resume", "inspect", "inspect", "inspect"]
    # A probe while it is admitted: the shim reports it starting, and nothing resumes it again.
    actor.call = Mock(return_value={"state": "starting"})
    assert actor.health()["ready"] is False
    assert [c.args[0]["operation"] for c in actor.call.call_args_list] == ["inspect"]


def test_a_tee_resume_backs_off_longer_and_a_slow_one_is_not_a_failure(tmp_path, monkeypatch, caplog):
    bound = tee_bound(tmp_path)
    start_actor(monkeypatch, bound, {"launch": [{"state": "ready", "port": 18080}]})
    actor = shim._ShimApp(bound)
    clock = [1000.0]
    monkeypatch.setattr(shim.time, "monotonic", lambda: clock[0])
    actor.call = Mock(side_effect=lambda command, timeout=None: {"state": "stopped", "port": 18080}
        if command["operation"] == "inspect" else (_ for _ in ()).throw(PENDING))
    health = actor.health()
    assert health["ready"] is False and health["management_ready"] is False
    assert actor.call.call_args.kwargs["timeout"] == shim.RESUME_TIMEOUT_S
    assert "shim resume failed" not in caplog.text
    # Each attempt costs an admission and a proof: 5 min, doubling to an hour.
    assert actor.resume_at == 1000 + 300
    clock[0] += 299
    actor.health()
    clock[0] += 1
    actor.health()
    assert [c.args[0]["operation"] for c in actor.call.call_args_list].count("resume") == 2
    assert actor.resume_at == clock[0] + 600
    for _ in range(10):
        actor.resume_at = 0
        actor.health()
    assert actor.resume_at - clock[0] == 3600


def test_daemon_call_tells_an_unanswered_operation_from_an_unavailable_daemon(tmp_path):
    import threading
    state = tmp_path / "state"
    state.mkdir(mode=0o700)
    path = state / "control.sock"
    server = socket.socket(socket.AF_UNIX)
    server.bind(str(path))
    shim.os.chmod(path, 0o600)
    server.listen(1)
    held = []
    def take():
        connection, _ = server.accept()
        held.append(connection)
        connection.recv(65536)  # and never answers
    threading.Thread(target=take, daemon=True).start()
    with pytest.raises(shim.ShimPending, match="the operation continues there"):
        shim.daemon_call(str(state), "node", {"operation": "resume"}, timeout=0.5)
    server.close()
    for connection in held:
        connection.close()
    path.unlink()
    with pytest.raises(RuntimeError, match="unavailable") as unavailable:
        shim.daemon_call(str(state), "node", {"operation": "resume"}, timeout=0.5)
    assert not isinstance(unavailable.value, shim.ShimPending)


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


def test_access_rules_are_refused_on_the_legacy_openshell_driver(tmp_path):
    # The scheduler never places this pairing; refuse it where it is written.
    raw = native_config(tmp_path)
    raw["apps"][0]["access_policy_sha256"] = "ab" * 32
    assert raw["apps"][0]["runtime"]["driver"] == "openshell"
    with pytest.raises(ValueError, match="docker, tee or tee-simulation"):
        reconcile.load_apps(raw)


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


def test_trustee_tls_credentials_are_node_owned_and_reach_daemon(tmp_path):
    raw = config(tmp_path)
    tls = dict(ca_file="/private/ca.pem", cert_file="/private/node.pem", key_file="/private/node.key")
    raw.update(shim_daemon=True, shim_tee={**tee_config(), "trustee_endpoint":"https://fleet.example:8905", "trustee_tls":tls})
    spec = reconcile.replica_specs(reconcile.load_apps(raw))["web-0"]
    assert shim.daemon_options(spec.cluster)["machine"]["tee"]["trustee_tls"] == tls
    for changes in ({"trustee_tls":None}, {"trustee_endpoint":"http://fleet.example:50004"},
                    {"trustee_endpoint":"http://127.0.0.1:50004"},
                    {"trustee_tls":{**tls, "key_file":"relative.key"}}):
        broken = {**raw, "shim_tee":{**raw["shim_tee"], **changes}}
        with pytest.raises(ValueError):
            reconcile.load_apps(broken)


def test_dcap_guest_configuration_reaches_shim_without_as(tmp_path):
    tee = tee_config("hardware")
    tee.update(platform="tdx", expected_measurement="55" * 32,
               dcap=dict(collateral_file="/var/lib/fleet/dcap.abi", min_tcb_evaluation_data_number=20))
    del tee["trustee_endpoint"]
    del tee["trustee_policy_ids"]
    raw = config(tmp_path)
    raw.update(shim_daemon=True, shim_tee=tee)
    spec = reconcile.replica_specs(reconcile.load_apps(raw))["web-0"]
    options = shim.daemon_options(spec.cluster)["machine"]["tee"]
    assert options["dcap"] == tee["dcap"]
    assert not options["trustee_endpoint"]
    for changes in ({"dcap": None}, {"platform": "snp"},
                    {"dcap": {**tee["dcap"], "collateral_file": "relative"}},
                    {"dcap": {**tee["dcap"], "min_tcb_evaluation_data_number": 0}}):
        with pytest.raises(ValueError):
            reconcile.load_apps({**raw, "shim_tee": {**tee, **changes}})


def test_persisted_binding_digest_is_unchanged_and_shares_the_spec_hash(tmp_path):
    # Allocations already on disk compare against this exact full digest.
    spec = reconcile.replica_specs(reconcile.load_apps(config(tmp_path)))["web-0"]
    digest = shim.hashlib.sha256(shim.json.dumps({k: v for k, v in spec.config.items() if k != "priority"},
        sort_keys=True).encode()).hexdigest()
    assert shim.binding_hash(spec) == digest
    assert shim.spec_hash(spec) == digest[:16]


def bound_spec(tmp_path, daemon=True, **app):
    s = spec(tmp_path)
    s = replace(s, app=replace(s.app, **app), cluster=replace(s.cluster, shim_daemon=daemon, log_dir=str(tmp_path / "logs")))
    return shim.RuntimeBook(tmp_path / "book").bind(s, "launch")


def start_actor(monkeypatch, bound, answers, resumable=True):
    """A mocked shim answering each operation from its list (the last answer repeats).
    Returns the (operation, timeout) calls the actor makes."""
    calls = []
    def call(self, command, timeout=None):
        calls.append((command["operation"], timeout))
        replies = answers[command["operation"]]
        reply = replies.pop(0) if len(replies) > 1 else replies[0]
        if isinstance(reply, Exception):
            raise reply
        return reply
    monkeypatch.setattr(shim._ShimApp, "call", call)
    monkeypatch.setattr(shim._ShimApp, "start_network", lambda self, timeout=None: None)
    monkeypatch.setattr(shim._ShimApp, "application_ready", lambda self: True)
    supported = set(shim.required_capabilities(bound)) | ({"shim-resume-v1", "shim-resume-tee-v1"} if resumable else set())
    monkeypatch.setattr(shim, "capabilities", lambda _: sorted(supported))
    return calls


def test_docker_launch_reserves_cpu_and_memory_without_container_limits(tmp_path):
    raw = config(tmp_path)
    raw["apps"][0].update(gpus=2, cpus=4, memory_gb=8, shm_gb=32)
    s = reconcile.replica_specs(reconcile.load_apps(raw))["web-0"]
    book = shim.RuntimeBook(tmp_path / "book")
    bound = book.bind(s, "launch")
    assert bound.launch_revision == book.read(s.name).launch_revision == shim.LAUNCH_REVISION
    request = shim.launch_request(bound, ["1", "5"])
    assert request["limits"] == {"cpu_millicores": None, "memory_bytes": None}
    assert request["driver_options"]["shm_bytes"] == 32 * 2**30
    assert request["driver_options"]["gpus"] == ["1", "5"]
    # Ray reserves the entry's CPUs, cards and memory for the replica.
    assert reconcile.bundle(bound).to_ray() == {"CPU": 4.0, "GPU": 2.0, "memory": 8 * 2**30}
    # The revision is not part of the admitted deployment or its persisted binding.
    assert shim.spec_hash(bound) == shim.spec_hash(s) and book.read(s.name).spec == shim.binding_hash(s)
    # `cpus` the shim would refuse as a limit (0, under a millicore) is a reservation only;
    # a replica that reserves none also gets a low CPU weight.
    for cpus, weight in ((0, {"cpu_shares": shim.UNRESERVED_CPU_SHARES}), (0.0004, {})):
        assert shim.launch_limits(replace(bound, app=replace(bound.app, cpus=cpus))) == {**request["limits"], **weight}
    # A TEE runtime keeps its limits.
    raw.update(shim_daemon=True, shim_tee=tee_config())
    raw["apps"][0].update(gpus=0, runtime={"executor": "shim", "driver": "tee-simulation"})
    tee = shim.RuntimeBook(tmp_path / "tee").bind(reconcile.replica_specs(reconcile.load_apps(raw))["web-0"], "launch")
    assert shim.launch_request(tee, [])["limits"] == {"cpu_millicores": 4000, "memory_bytes": 8 * 2**30}


def capped(tmp_path, **app):
    """config() with a shim app that reserves no CPU and is capped, updated by `app`."""
    raw = config(tmp_path)
    raw["apps"][0].update(cpus=0, cpu_limit=1.5, memory_gb=1, memory_limit_gb=2, **app)
    return raw


def test_an_entry_without_hard_limits_keeps_its_digest_and_its_launch_request(tmp_path):
    # Allocations on disk compare against this full digest of the entry as written: the
    # new settings are no keys of an entry that doesn't set them.
    entry = {"name": "web", "kind": "app", "image": "web:1", "gpus": 0, "cpus": 2, "memory_gb": 4,
        "port_base": 18080, "command": ["serve", "{port}"], "runtime": {"executor": "shim", "driver": "docker"}}
    raw = {"shim_binary": "/opt/trusted-shim-runtime", "shim_state_dir": "/srv/fleet-shim", "apps": [entry]}
    s = reconcile.replica_specs(reconcile.load_apps(raw))["web-0"]
    assert "cpu_limit" not in s.config and "memory_limit_gb" not in s.config
    assert shim.binding_hash(s) == "1d9ef2c57b88c6f8bc19d1f14bc3109dc793838ed919b1524bbcb516efe089e4"
    # Its limits, at each revision and on TEE, are the ones it had: no CPU weight either.
    bound = shim.RuntimeBook(tmp_path / "book").bind(s, "launch")
    assert shim.launch_request(bound, [])["limits"] == {"cpu_millicores": None, "memory_bytes": None}
    first = replace(bound, launch_revision=1)
    assert shim.launch_request(first, [])["limits"] == {"cpu_millicores": 2000, "memory_bytes": 4 * 2**30}
    raw.update(shim_daemon=True, shim_tee=tee_config())
    entry["runtime"]["driver"] = "tee-simulation"
    tee = shim.RuntimeBook(tmp_path / "tee").bind(reconcile.replica_specs(reconcile.load_apps(raw))["web-0"], "launch")
    assert shim.launch_request(tee, [])["limits"] == {"cpu_millicores": 2000, "memory_bytes": 4 * 2**30}


def test_hard_limits_cap_the_container_and_an_unreserved_one_gets_a_low_cpu_weight(tmp_path):
    raw = config(tmp_path)
    raw["apps"][0].update(cpus=2, memory_gb=4, cpu_limit=2.5)
    bound = shim.RuntimeBook(tmp_path / "book").bind(reconcile.replica_specs(reconcile.load_apps(raw))["web-0"], "launch")
    assert shim.launch_request(bound, [])["limits"] == {"cpu_millicores": 2500, "memory_bytes": None}
    assert shim.required_capabilities(bound) >= {shim.LIMITS_CAPABILITY} and shim.CPU_SHARES_CAPABILITY not in shim.required_capabilities(bound)
    raw["apps"][0].update(memory_limit_gb=6.5)
    bound = shim.RuntimeBook(tmp_path / "both").bind(reconcile.replica_specs(reconcile.load_apps(raw))["web-0"], "launch")
    assert shim.launch_request(bound, [])["limits"] == {"cpu_millicores": 2500, "memory_bytes": round(6.5 * 2**30)}
    # A replica that reserves no CPU: Ray reserves its memory only, the shim caps it and
    # gives it a low weight.
    bound = shim.RuntimeBook(tmp_path / "zero").bind(reconcile.replica_specs(reconcile.load_apps(capped(tmp_path)))["web-0"], "launch")
    assert shim.UNRESERVED_CPU_SHARES == 64
    assert shim.launch_request(bound, [])["limits"] == {"cpu_millicores": 1500, "memory_bytes": 2 * 2**30, "cpu_shares": 64}
    assert reconcile.bundle(bound).to_ray() == {"CPU": 0.0, "memory": 2**30}
    assert shim.required_capabilities(bound) >= {shim.LIMITS_CAPABILITY, shim.CPU_SHARES_CAPABILITY}
    assert shim.TEE_CPU_OVERCOMMIT_CAPABILITY not in shim.required_capabilities(bound)
    # On TEE the limits are the entry's too.
    raw = capped(tmp_path, gpus=0, runtime={"executor": "shim", "driver": "tee-simulation"})
    raw.update(shim_daemon=True, shim_tee={**tee_config(), "allow_cpu_overcommit": True})
    tee = shim.RuntimeBook(tmp_path / "tee").bind(reconcile.replica_specs(reconcile.load_apps(raw))["web-0"], "launch")
    assert shim.launch_request(tee, [])["limits"] == {"cpu_millicores": 1500, "memory_bytes": 2 * 2**30, "cpu_shares": 64}
    assert shim.TEE_CPU_OVERCOMMIT_CAPABILITY in shim.required_capabilities(tee)


@pytest.mark.parametrize("app,message", [
    ({"runtime": {"executor": "shim", "driver": "openshell"}, "cpus": 1, "port_base": None, "cpu_limit": 1}, "native OpenShell"),
    ({"cpus": 2, "cpu_limit": 1.5}, "at least cpus"),
    ({"cpu_limit": 0.001}, "at least 0.01 CPUs"),
    ({"memory_gb": 4, "memory_limit_gb": 2}, "at least memory_gb"),
    ({"memory_limit_gb": 0.001}, "at least 6 MiB"),
])
def test_hard_limits_are_refused_where_nothing_enforces_them(tmp_path, app, message):
    raw = config(tmp_path)
    raw["apps"][0].update(app)
    if raw["apps"][0].get("port_base", 0) is None:
        del raw["apps"][0]["port_base"]
    with pytest.raises(ValueError, match=re.escape(message)):
        reconcile.load_apps(raw, validate_launch=False)


@pytest.mark.parametrize("change", [{"cpu_limit": None}, {"memory_gb": None}, {"memory_limit_gb": None}, {"gpus": 1}])
def test_a_replica_that_reserves_no_cpu_is_always_capped(tmp_path, change):
    raw = capped(tmp_path)
    for key, value in change.items():
        if value is None:
            del raw["apps"][0][key]
        else:
            raw["apps"][0][key] = value
    reconcile.load_apps(raw, validate_launch=False)
    with pytest.raises(ValueError, match="cpus 0 reserves no CPU"):
        reconcile.load_apps(raw)


def test_a_tee_node_takes_an_unreserved_tenant_only_with_its_opt_in(tmp_path, monkeypatch):
    import hashlib
    raw = capped(tmp_path, runtime={"executor": "shim", "driver": "tee-simulation"})
    raw.update(shim_daemon=True, shim_tee=tee_config())
    with pytest.raises(ValueError, match="allow_cpu_overcommit"):
        reconcile.load_apps(raw)
    machine = shim.machine_options(reconcile.load_apps(raw, validate_launch=False).cluster)
    raw["shim_tee"]["allow_cpu_overcommit"] = True
    cluster = reconcile.load_apps(raw).cluster
    # The opt-in is the bridge's: the running shim's machine configuration is unchanged.
    assert shim.machine_options(cluster) == machine and "allow_cpu_overcommit" not in machine["tee"]
    digest = hashlib.sha256(shim.json.dumps(machine, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
    response = dict(protocol=1, drivers=["tee-simulation"], machine_config_sha256=digest,
        features=["runtime-daemon-v1", "tee-startup-v1", "data-plane-v1", "runtime-management-v1"])
    monkeypatch.setattr(shim, "daemon_call", lambda *a, **k: response)
    assert shim.TEE_CPU_OVERCOMMIT_CAPABILITY in shim.capabilities(cluster)
    assert shim.TEE_CPU_OVERCOMMIT_CAPABILITY not in shim.capabilities(replace(cluster, shim_tee=replace(cluster.shim_tee, allow_cpu_overcommit=False)))
    assert shim.CPU_SHARES_CAPABILITY not in shim.capabilities(cluster)
    response["features"].append("runtime-cpu-shares-v1")
    assert {shim.CPU_SHARES_CAPABILITY, shim.LIMITS_CAPABILITY} <= set(shim.capabilities(cluster))


def test_the_cpu_weight_capability_follows_the_shim_feature(tmp_path, monkeypatch):
    cluster = spec(tmp_path).cluster
    reply = {"protocol": 1, "drivers": ["docker"], "features": ["data-plane-v1"]}
    monkeypatch.setattr(shim.subprocess, "run", lambda *a, **k: Mock(stdout=shim.json.dumps(reply)))
    assert shim.CPU_SHARES_CAPABILITY not in shim.capabilities(cluster)
    reply["features"].append("runtime-cpu-shares-v1")
    assert shim.CPU_SHARES_CAPABILITY in shim.capabilities(cluster)


def test_a_node_refuses_an_unreserved_app_its_shim_cannot_weight_and_a_tee_limit_beyond_its_cpus(tmp_path, monkeypatch):
    e = executor(tmp_path)
    raw = capped(tmp_path)
    app = raw["apps"][0]; app.update(namespace="0"*32, app="web", elf_id="")
    e.apps_file.write_text(yaml.safe_dump(raw))
    monkeypatch.setattr(node, "alive_nodes", lambda: [{}]); monkeypatch.setattr(reconcile, "running", lambda: {})
    monkeypatch.setattr(shim, "capabilities", lambda _: [shim.CAPABILITY, shim.LIMITS_CAPABILITY])
    start = Mock(); monkeypatch.setattr(reconcile, "start", start)
    with pytest.raises(ValueError, match="it lacks shim-cpu-shares-v1"):
        e.set_app({"app": app, "disabled": False}, "start")
    start.assert_not_called()
    monkeypatch.setattr(shim, "capabilities", lambda _: [shim.CAPABILITY, shim.LIMITS_CAPABILITY, shim.CPU_SHARES_CAPABILITY])
    e.set_app({"app": app, "disabled": False}, "start")
    assert start.call_count == 1
    # A TEE node's CPUs are its VM's: a limit beyond them is refused before anything starts.
    app.update(cpus=1, cpu_limit=9, runtime={"executor": "shim", "driver": "tee-simulation"})
    raw.update(shim_daemon=True, shim_tee=tee_config())
    e.apps_file.write_text(yaml.safe_dump(raw))
    monkeypatch.setattr(node, "resources", lambda: ({"CPU": 8.0}, {"CPU": 8.0}))
    with pytest.raises(ValueError, match="exceeds this TEE node's 8.0 CPUs"):
        e.set_app({"app": app, "disabled": False}, "start")
    assert start.call_count == 1


def test_allocation_launched_before_launch_revisions_replays_its_original_request(tmp_path):
    import pickle
    raw = config(tmp_path)
    raw["apps"][0].update(gpus=1, env={"MODE": "serve"})
    s = reconcile.replica_specs(reconcile.load_apps(raw))["web-0"]
    book = shim.RuntimeBook(tmp_path / "book")
    first = book.bind(s, "launch")
    assert shim.launch_request(first, ["2"])["environment"] == {"MODE": "serve", "NVIDIA_DRIVER_CAPABILITIES": "all"}
    # An allocation recorded without a revision holds a revision 1 launch.
    record = shim.json.loads(book.path(s.name).read_text())
    del record["launch_revision"]
    book.path(s.name).write_text(shim.json.dumps(record))
    replayed = book.bind(s, "heartbeat")
    assert replayed.execution == first.execution and replayed.launch_revision == 1
    # The shim replays only an identical request: the one launched before.
    request = shim.launch_request(replayed, ["2"])
    assert request["environment"] == {"MODE": "serve"}
    assert request["limits"] == {"cpu_millicores": 500, "memory_bytes": 134217728}
    # So does the spec a running actor was created with, which has no revision.
    old = copy.copy(replayed)
    del old.__dict__["launch_revision"]
    assert pickle.loads(pickle.dumps(old)).launch_revision == 1


def test_cpu_count_is_at_least_one(tmp_path):
    bound = bound_spec(tmp_path, cpus=0.5, env={"THREADS": "{cpu_count}"})
    # A fraction of a CPU still gets one thread.
    assert shim.launch_request(bound, [])["environment"] == {"THREADS": "1"}
    assert shim.launch_request(replace(bound, app=replace(bound.app, cpus=4)), [])["environment"] == {"THREADS": "4"}
    # A revision 1 allocation replays exactly the request it was launched with.
    assert shim.launch_request(replace(bound, launch_revision=1), [])["environment"] == {"THREADS": "0"}


def test_gpu_apps_get_every_nvidia_driver_capability_unless_their_env_sets_one(tmp_path):
    raw = config(tmp_path)
    raw["apps"][0].update(gpus=1)
    bound = shim.RuntimeBook(tmp_path / "book").bind(reconcile.replica_specs(reconcile.load_apps(raw))["web-0"], "launch")
    assert shim.launch_request(bound, ["3"])["environment"] == {"NVIDIA_DRIVER_CAPABILITIES": "all"}
    chosen = replace(bound, app=replace(bound.app, env={"NVIDIA_DRIVER_CAPABILITIES": "compute,utility"}))
    assert shim.launch_request(chosen, ["3"])["environment"] == {"NVIDIA_DRIVER_CAPABILITIES": "compute,utility"}
    # CPU apps and revision 1 launches have none; nor do packaged provers (see the first test).
    assert shim.launch_request(replace(bound, app=replace(bound.app, gpus=0)), [])["environment"] == {}
    assert shim.launch_request(replace(bound, launch_revision=1), ["3"])["environment"] == {}


def test_image_check_and_pull_use_the_shims_docker(monkeypatch):
    import subprocess
    monkeypatch.setenv("DOCKER_HOST", "tcp://docker.example:2375")
    image = "registry.example/web@sha256:" + "a" * 64
    run = Mock(return_value=Mock(returncode=0, stdout="", stderr=""))
    monkeypatch.setattr(shim.subprocess, "run", run)
    assert IMAGE_PRESENT(image) is True
    assert run.call_args.args[0] == ["docker", "--host=unix:///var/run/docker.sock", "image", "inspect",
                                     "--format", "{{.Id}}", "--", image]
    assert run.call_args.kwargs["timeout"] == shim.IMAGE_CHECK_TIMEOUT_S
    assert "DOCKER_HOST" not in run.call_args.kwargs["env"]
    for answer in (Mock(returncode=1), subprocess.TimeoutExpired("docker", 30), FileNotFoundError(2, "No such file")):
        run.side_effect = [answer]
        assert IMAGE_PRESENT(image) is False
    run.side_effect = None
    PULL_IMAGE("web-0", image)
    assert run.call_args.args[0] == ["docker", "--host=unix:///var/run/docker.sock", "pull", "--", image]
    # Its own hour, apart from the app's readiness wait.
    assert run.call_args.kwargs["timeout"] == shim.PULL_TIMEOUT_S == 3600
    assert "DOCKER_HOST" not in run.call_args.kwargs["env"]


@pytest.mark.parametrize("pull,message", [
    (Mock(returncode=1, stdout="", stderr="manifest unknown"), "could not pull image web:1: manifest unknown"),
    (Mock(returncode=1, stdout="", stderr=""), "could not pull image web:1: docker pull exited with 1"),
    ("timeout", "pulling image web:1 did not finish within 5 s"),
    (FileNotFoundError(2, "No such file or directory"), "could not pull image web:1")])
def test_a_failed_pull_names_the_image(monkeypatch, pull, message):
    import re
    import subprocess
    pull = subprocess.TimeoutExpired("docker", 5) if pull == "timeout" else pull
    monkeypatch.setattr(shim.subprocess, "run", Mock(side_effect=[pull]))
    with pytest.raises(RuntimeError, match=re.escape("web-0: " + message)):
        PULL_IMAGE("web-0", "web:1", 5)


REFUSED = RuntimeError("shim daemon unavailable or rejected operation")


def test_only_a_first_launch_without_its_image_pulls(tmp_path, monkeypatch, no_docker):
    bound = bound_spec(tmp_path)
    present = Mock(return_value=False)
    monkeypatch.setattr(shim, "image_present", present)
    # A restarted actor's replay finds its container: no image check and no
    # registry, so a registry outage after a reboot cannot block a resume.
    calls = start_actor(monkeypatch, bound, {"launch": [{"state": "stopped", "port": 18080}],
        "resume": [{"state": "ready", "port": 18080}]})
    shim._ShimApp(bound)
    assert calls == [("launch", None), ("resume", None)] and no_docker == [] and not present.called
    # A first launch on a node without the image creates nothing, and the same
    # launch follows the pull.
    requests = []
    calls = start_actor(monkeypatch, bound, {"launch": [REFUSED, {"state": "ready", "port": 18080}]})
    recorded = shim._ShimApp.call
    monkeypatch.setattr(shim._ShimApp, "call", lambda self, command, timeout=None:
        requests.append(command) or recorded(self, command, timeout))
    shim._ShimApp(bound)
    assert calls == [("launch", None), ("launch", None)] and requests[0] == requests[1]
    assert no_docker == [("web-0", "python:3.12-slim")]
    present.assert_called_once_with("python:3.12-slim")


def test_a_refused_launch_pulls_only_a_missing_image(tmp_path, monkeypatch, no_docker):
    bound = bound_spec(tmp_path)
    calls = start_actor(monkeypatch, bound, {"launch": [REFUSED]})
    # The image is there, so the refusal has another cause: nothing is pulled.
    with pytest.raises(RuntimeError, match="shim did not launch web-0 .*rejected operation.*explicit cleanup"):
        shim._ShimApp(bound)
    assert calls == [("launch", None)] and no_docker == []
    monkeypatch.setattr(shim, "image_present", lambda image: False)
    monkeypatch.setattr(shim, "pull_image", Mock(side_effect=RuntimeError("web-0: could not pull image python:3.12-slim: denied")))
    calls.clear()
    with pytest.raises(RuntimeError, match="^web-0: could not pull image python:3.12-slim: denied$"):
        shim._ShimApp(bound)
    assert calls == [("launch", None)]
    # A launch the pull did not fix reports the shim's answer.
    monkeypatch.setattr(shim, "pull_image", Mock())
    calls.clear()
    with pytest.raises(RuntimeError, match="shim did not launch web-0 .*rejected operation"):
        shim._ShimApp(bound)
    assert calls == [("launch", None), ("launch", None)]
    # Other drivers never pull.
    raw = config(tmp_path)
    raw.update(shim_daemon=True, shim_tee=tee_config())
    raw["apps"][0]["runtime"]["driver"] = "tee-simulation"
    tee = shim.RuntimeBook(tmp_path / "tee").bind(reconcile.replica_specs(reconcile.load_apps(raw))["web-0"], "launch")
    start_actor(monkeypatch, tee, {"launch": [REFUSED]})
    shim.pull_image.reset_mock()
    with pytest.raises(RuntimeError, match="shim did not launch web-0"):
        shim._ShimApp(tee)
    shim.pull_image.assert_not_called()


def test_the_readiness_wait_starts_after_the_pull_and_launch(tmp_path, monkeypatch):
    bound = bound_spec(tmp_path)
    start_actor(monkeypatch, bound, {"launch": [REFUSED, {"state": "ready", "port": 18080}], "inspect": [{"state": "ready"}]})
    monkeypatch.setattr(shim._ShimApp, "application_ready", lambda self: False)
    clock = [0.0]
    monkeypatch.setattr(shim.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(shim.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    monkeypatch.setattr(shim, "image_present", lambda image: False)
    monkeypatch.setattr(shim, "pull_image", lambda name, image: clock.__setitem__(0, clock[0] + 1800))
    with pytest.raises(RuntimeError, match="did not become healthy"):
        shim._ShimApp(bound)
    # The app's whole ready_timeout_s follows a half-hour pull.
    assert bound.app.ready_timeout_s % 2 == 0 and clock[0] == 1800 + bound.app.ready_timeout_s


def test_restarted_actor_resumes_its_exited_container(tmp_path, monkeypatch):
    bound = bound_spec(tmp_path)
    calls = start_actor(monkeypatch, bound, {"launch": [{"state": "stopped", "port": 18080}],
        "resume": [{"state": "ready", "port": 18080}], "inspect": [{"state": "ready"}]})
    actor = shim._ShimApp(bound)
    # Resumed with the daemon's full budget, not a health probe's.
    assert calls == [("launch", None), ("resume", None)]
    assert actor.port == 18080 and actor.resumes == 1
    assert actor.health()["ready"] is True
    assert calls[-1] == ("inspect", None)


@pytest.mark.parametrize("daemon,resumable", [(False, True), (True, False)])
def test_exited_container_without_resume_support_needs_cleanup(tmp_path, monkeypatch, daemon, resumable):
    bound = bound_spec(tmp_path, daemon=daemon)
    calls = start_actor(monkeypatch, bound, {"launch": [{"state": "stopped", "port": 18080}]}, resumable=resumable)
    with pytest.raises(RuntimeError, match="web-0 is not running: its container exited; only the persistent "
                       "shim with runtime-resume-v1 starts it again; explicit cleanup is required"):
        shim._ShimApp(bound)
    assert calls == [("launch", None)]


@pytest.mark.parametrize("resumed,reason", [
    (RuntimeError("shim daemon unavailable or rejected operation"), "the shim did not start it again"),
    ({"state": "stopping"}, "it is stopping")])
def test_a_stop_or_delete_that_raced_the_restart_stays_final(tmp_path, monkeypatch, resumed, reason):
    bound = bound_spec(tmp_path)
    calls = start_actor(monkeypatch, bound, {"launch": [{"state": "stopped", "port": 18080}], "resume": [resumed]})
    with pytest.raises(RuntimeError, match=f"web-0 is not running: .*{reason}; explicit cleanup is required"):
        shim._ShimApp(bound)
    # Nothing is launched or started again once the shim refused.
    assert [operation for operation, _ in calls] == ["launch", "resume"]


def test_a_deleted_or_missing_container_is_never_replaced(tmp_path, monkeypatch):
    bound = bound_spec(tmp_path)
    calls = start_actor(monkeypatch, bound, {"launch": [RuntimeError("shim daemon unavailable or rejected operation")]})
    with pytest.raises(RuntimeError, match="shim did not launch web-0 .*rejected operation.*stopped or deleted.*explicit cleanup"):
        shim._ShimApp(bound)
    calls = start_actor(monkeypatch, bound, {"launch": [{"state": "failed", "port": 18080}]})
    with pytest.raises(RuntimeError, match="its container is missing or failed; explicit cleanup is required"):
        shim._ShimApp(bound)
    assert calls == [("launch", None)]


def test_log_follower_streams_docker_into_the_node_log_and_resumes_after_its_cursor(tmp_path, docker_api):
    import calendar
    container = "trusted-shim-" + "b" * 64
    socket_path, seen = docker_api([
        (200, MULTIPLEXED, frame(1, b"2026-11-23T10:00:00.000000001Z first\n") + frame(2, b"2026-11-23T10:00:01.5Z to stderr\n")),
        # `since` includes the cursor's own line; it is written once.
        (200, MULTIPLEXED, frame(2, b"2026-11-23T10:00:01.5Z to stderr\n") + frame(1, b"2026-11-23T10:00:02Z after a restart\n"))])
    path = tmp_path / "logs" / "web-0.node.log"
    assert FOLLOW(shim.LogFollower(container, str(path), socket_path)) is True
    assert path.read_bytes() == b"first\nto stderr\n"
    cursor = calendar.timegm((2026, 11, 23, 10, 0, 1, 0, 0, 0)) * 10**9 + 500_000_000
    assert (tmp_path / "logs" / "web-0.node.log.cursor").read_text() == f"{cursor}\n"
    # A restarted actor's new follower continues after the last line written.
    assert FOLLOW(shim.LogFollower(container, str(path), socket_path)) is True
    assert path.read_bytes() == b"first\nto stderr\nafter a restart\n"
    query = "follow=1&stdout=1&stderr=1&timestamps=1"
    assert seen == [f"/containers/{container}/logs?{query}",
                    f"/containers/{container}/logs?{query}&since={cursor // 10**9}.500000000"]


def test_a_log_without_a_cursor_continues_after_its_last_write(tmp_path, docker_api):
    container = "trusted-shim-" + "b" * 64
    socket_path, seen = docker_api([(200, "application/vnd.docker.raw-stream",
        b"2026-11-01T00:00:00Z before the last write\n2026-11-23T10:00:03Z raw line\n")])
    path = tmp_path / "web-0.node.log"
    path.write_bytes(b"earlier\n")
    written = 1_795_000_000_123_456_789
    shim.os.utime(path, ns=(written, written))
    assert FOLLOW(shim.LogFollower(container, str(path), socket_path)) is True
    assert seen[0].endswith(f"&since={written // 10**9}.123456789")
    # A raw stream is split into lines; what predates the log's last write is skipped.
    assert path.read_bytes() == b"earlier\nraw line\n"


def test_docker_log_timestamps():
    import calendar
    base = calendar.timegm((2026, 11, 23, 10, 0, 0, 0, 0, 0)) * 10**9
    assert shim._stamp_ns(b"2026-11-23T10:00:00Z") == base
    assert shim._stamp_ns(b"2026-11-23T10:00:00.123456789Z") == base + 123456789
    assert shim._stamp_ns(b"2026-11-23T11:30:00.5+01:30") == base + 500_000_000
    assert shim._stamp_ns(b"2026-11-23T08:00:00-02:00") == base
    assert shim._stamp_ns(b"not a stamp") is None


def test_follower_failures_stay_in_the_bridge_log_and_back_off(tmp_path, docker_api, monkeypatch, caplog):
    container = "trusted-shim-" + "c" * 64
    socket_path, seen = docker_api([(501, "application/json", b'{"message":"configured logging driver does not support reading"}')])
    path = tmp_path / "web-0.node.log"
    follower = shim.LogFollower(container, str(path), socket_path)
    monkeypatch.setattr(shim.LogFollower, "follow", FOLLOW)
    clock = [100.0]
    monkeypatch.setattr(shim.time, "monotonic", lambda: clock[0])
    follower.run()
    assert (f"could not follow the logs of {container}: Docker answered 501: "
            '{"message":"configured logging driver does not support reading"}') in caplog.text
    # Docker's answer never reaches the app's own log.
    assert not path.exists() and (follower.failures, follower.retry_at) == (1, 130.0)
    # Health probes during the backoff start nothing.
    follower.ensure()
    assert follower.thread is None and len(seen) == 1
    clock[0] = 130.0
    follower.ensure()
    follower.thread.join(10)
    assert len(seen) == 2 and (follower.failures, follower.retry_at) == (2, 190.0)
    follower.failures = 9
    follower.run()
    assert follower.retry_at == 130.0 + 900
    # Without Docker at all, the same.
    follower.socket_path = str(tmp_path / "missing.sock")
    follower.run()
    assert f"could not follow the logs of {container}: [Errno 2]" in caplog.text
    # A stream that delivered output, or stayed up, clears the backoff.
    monkeypatch.setattr(shim.LogFollower, "follow", lambda self: True)
    follower.run()
    assert (follower.failures, follower.retry_at) == (0, 0.0)


def test_one_log_follower_at_a_time(tmp_path, monkeypatch, caplog):
    import threading
    follower = shim.LogFollower("trusted-shim-" + "c" * 64, str(tmp_path / "web-0.node.log"))
    gate = threading.Event()
    monkeypatch.setattr(shim.LogFollower, "follow", lambda self: gate.wait(10))
    follower.ensure()
    first = follower.thread
    follower.ensure()
    assert follower.thread is first
    gate.set()
    first.join(10)
    follower.ensure()
    assert follower.thread is not first
    follower.thread.join(10)
    follower.thread = Mock(is_alive=Mock(side_effect=RuntimeError("can't start new thread")))
    follower.ensure()
    assert caplog.text.count("could not follow the logs of trusted-shim-") == 1


def test_actor_follows_its_container_into_the_node_log(tmp_path, monkeypatch):
    bound = bound_spec(tmp_path)
    container = "trusted-shim-" + "d" * 64
    start_actor(monkeypatch, bound, {"launch": [{"state": "ready", "port": 18080, "runtime_id": container}],
        "inspect": [{"state": "ready", "runtime_id": container}]})
    ensure = Mock()
    monkeypatch.setattr(shim.LogFollower, "ensure", ensure)
    actor = shim._ShimApp(bound)
    # Where `fleetctl app logs` reads a replica's log.
    assert (actor.logs.container, actor.logs.path) == (container, str(tmp_path / "logs" / "web-0.node.log"))
    assert ensure.call_count == 1
    # Each ready probe follows again a container that was resumed.
    assert actor.health()["ready"] is True and ensure.call_count == 2
    actor.call = Mock(return_value={"state": "stopped"})
    actor.resumable = False
    assert actor.health()["ready"] is False and ensure.call_count == 2


def test_a_follower_that_cannot_start_leaves_health_alone(tmp_path):
    container = "trusted-shim-" + "d" * 64
    actor = object.__new__(shim._ShimApp)
    actor.spec, actor.port, actor.gpus, actor.network = bound_spec(tmp_path), 18080, [], None
    actor.call = Mock(return_value={"state": "ready"})
    actor.start_network = Mock()
    actor.application_ready = Mock(return_value=True)
    actor.logs = shim.LogFollower(container, str(tmp_path / "web-0.node.log"))
    actor.logs.thread = Mock(is_alive=Mock(side_effect=RuntimeError("can't start new thread")))
    assert actor.health()["ready"] is True


@pytest.mark.parametrize("status", [{"state": "ready", "port": 18080},
    {"state": "ready", "port": 18080, "runtime_id": "../trusted-shim-" + "e" * 64}])
def test_no_follower_without_the_shims_own_container_name(tmp_path, monkeypatch, status):
    bound = bound_spec(tmp_path)
    start_actor(monkeypatch, bound, {"launch": [status]})
    assert shim._ShimApp(bound).logs is None
