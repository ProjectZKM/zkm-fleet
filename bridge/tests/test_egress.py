"""A batch that lists hosts runs its jobs on the node's egress network, with the list
as their proxy credentials; a batch that lists none keeps its jobs without a network."""
import base64
import hashlib
import hmac
import json
import os
import subprocess
from unittest.mock import Mock

import pytest

from fleet_bridge import egress, jobs, node, shim
from fleet_bridge.model import Batch, BatchEgressConfig, ControlPlaneConfig, ModelError
from test_shim_jobs import ALL_FEATURES, ON_SHIM, FakeShim, queued

HOSTS = ["pypi.org", "files.pythonhosted.org"]
IMAGE = "ghcr.io/projectzkm/zkm-fleet-node:1.2.3"


@pytest.fixture(autouse=True)
def local(monkeypatch):
    monkeypatch.setattr(jobs.ray, "get_gpu_ids", lambda: [3])
    monkeypatch.setattr(shim, "image_present", lambda image: True)


def settings(tmp_path, **changes):
    key = tmp_path / "egress.key"
    if not key.exists():
        key.write_bytes(b"k" * 32)
        key.chmod(0o600)
    return BatchEgressConfig(**{"image": IMAGE, "key_file": str(key), **changes})


def cluster(tmp_path, **changes):
    return ControlPlaneConfig(shim_binary="/opt/trusted-shim-runtime", shim_state_dir=str(tmp_path / "shim"), shim_daemon=True,
                              batch_egress=settings(tmp_path), **changes)


def test_a_job_of_a_batch_with_egress_is_on_the_nodes_network_with_its_list_as_credentials(tmp_path, monkeypatch):
    fake = FakeShim(monkeypatch)
    policy = {"image": "trainer:1", "runtime": ON_SHIM, "egress": HOSTS, "env": {"HTTPS_PROXY": "http://elsewhere:1"}}
    job, spec = queued(tmp_path, policy=Batch.load(policy), env={"https_proxy": "http://elsewhere:2"})
    assert jobs._ShimRank(spec, job.path, cluster(tmp_path)).start(1) is None
    request = fake.runs[1]["request"]
    assert request["driver_options"]["network"] == {"named": "fleet-egress"}
    # Every HTTPS client's proxy setting names the node's proxy, whatever the batch or the job set.
    proxy = request["environment"]["HTTPS_PROXY"]
    assert all(request["environment"][name] == proxy for name in ("https_proxy", "HTTP_PROXY", "http_proxy"))
    assert (request["environment"]["NO_PROXY"], request["environment"]["no_proxy"]) == ("", "")
    assert proxy.startswith("http://fleet:") and proxy.endswith("@fleet-egress-proxy:3128")
    # The credentials are the batch's list, signed with the node's key: what the proxy checks.
    listed, signature = proxy[len("http://fleet:"):-len("@fleet-egress-proxy:3128")].split(".")
    decode = lambda text: base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))
    assert decode(listed) == b"v1\npypi.org,files.pythonhosted.org"
    assert decode(signature) == hmac.new(b"k" * 32, decode(listed), hashlib.sha256).digest()
    assert all(c.isalnum() or c in "-_." for c in listed + signature)

    # A batch that lists no host: no network and no proxy setting of the node's.
    fake = FakeShim(monkeypatch)
    job, spec = queued(tmp_path / "plain")
    assert jobs._ShimRank(spec, job.path, cluster(tmp_path)).start(1) is None
    request = fake.runs[1]["request"]
    assert request["driver_options"]["network"] == "none" and "HTTPS_PROXY" not in request["environment"]


def test_a_node_without_its_egress_settings_or_key_starts_no_job_with_egress(tmp_path, monkeypatch):
    fake = FakeShim(monkeypatch)
    job, spec = queued(tmp_path, policy=Batch.load({"image": "trainer:1", "runtime": ON_SHIM, "egress": HOSTS}))
    plain = ControlPlaneConfig(shim_binary="/opt/trusted-shim-runtime", shim_state_dir=str(tmp_path / "shim"), shim_daemon=True)
    assert "no egress" in jobs._ShimRank(spec, job.path, plain).start(1)
    configured = cluster(tmp_path)
    os.unlink(configured.batch_egress.key_file)
    assert "egress.key" in jobs._ShimRank(spec, job.path, configured).start(1)
    assert "launch" not in fake.calls


def test_egress_lists_full_host_names_for_a_batch_on_the_shim():
    Batch.load({"image": "trainer:1", "runtime": ON_SHIM, "egress": ["pypi.org", "cdn-lfs-us-1.hf.co", "a.b.example"]})
    for hosts in (["PyPI.org"], ["*.pypi.org"], ["pypi.org:443"], ["10.0.0.1"], ["localhost"], ["pypi.org."], ["pypi.org/simple"],
                  ["pypi.org", "pypi.org"], ["-a.example"], [""], [1], [f"h{i}.example" for i in range(33)], ["a" * 64 + ".example"]):
        with pytest.raises(ModelError):
            Batch.load({"image": "trainer:1", "runtime": ON_SHIM, "egress": hosts})
    for runtime in (None, {"executor": "legacy"}):
        with pytest.raises(ModelError, match="needs a batch on the shim"):
            Batch.load({"image": "trainer:1", "egress": HOSTS, **({"runtime": runtime} if runtime else {})})


def test_a_node_reports_egress_only_with_its_settings_and_a_shim_that_names_networks(tmp_path, monkeypatch):
    features = [*ALL_FEATURES, "runtime-named-network-v1"]
    plain = ControlPlaneConfig(shim_binary="/opt/trusted-shim-runtime", shim_state_dir=str(tmp_path / "shim"), shim_daemon=True)
    for config, offered, expected in [(cluster(tmp_path), features, True), (cluster(tmp_path), ALL_FEATURES, False), (plain, features, False)]:
        monkeypatch.setattr(shim, "daemon_call", lambda *a, **k: {"protocol": 1, "drivers": ["docker"], "features": offered})
        supported = shim.capabilities(config)
        assert shim.BATCH_CAPABILITY in supported and (shim.EGRESS_CAPABILITY in supported) is expected

    # The node's launch reads the settings with the shim's own, and refuses where it reports no egress.
    (tmp_path / "batches.yaml").write_text("batches: {}\n")
    (tmp_path / "apps.yaml").write_text(
        f"shim_binary: /opt/trusted-shim-runtime\nshim_state_dir: {tmp_path}/shim\nshim_daemon: true\n"
        f"batch_egress: {{image: '{IMAGE}', key_file: {tmp_path}/egress.key}}\napps: []\n")
    executor = node.NodeExecutor(str(tmp_path / "apps.yaml"), str(tmp_path / "batches.yaml"), str(tmp_path / "jobs"))
    executor.pool.shutdown()
    monkeypatch.setattr(node.ray, "nodes", lambda: [{"Alive": True}])
    monkeypatch.setattr(jobs, "prepare_launch", lambda _: True)
    launch = Mock()
    monkeypatch.setattr(jobs, "launch", launch)
    parameters = {"id": "job-1234abcd", "lease": "lease-1", "owner": "owner", "namespace": "a" * 32, "batch": "build",
        "submitted_at": 1, "request": {"command": ["true"], "gpus": 0, "cpus": 1},
        "policy": {"image": "trainer:1", "namespace": "a" * 32, "runtime": ON_SHIM, "egress": HOSTS}}
    monkeypatch.setattr(shim, "capabilities", lambda config: [shim.BATCH_CAPABILITY])
    with pytest.raises(ValueError, match="no egress"):
        executor.launch(parameters)
    launch.assert_not_called()
    monkeypatch.setattr(shim, "capabilities", lambda config: [shim.BATCH_CAPABILITY, shim.EGRESS_CAPABILITY])
    executor.launch(parameters)
    assert launch.call_args.args[2].batch_egress == settings(tmp_path)


@pytest.mark.parametrize("changes", [{"network": "bridge"}, {"network": "host"}, {"network": "Fleet"}, {"proxy": "a b"},
    {"proxy": "fleet-egress"}, {"key_file": "egress.key"}, {"image": ""}, {"image": "--privileged"}, {"port": 0}, {"port": "3128"}])
def test_the_nodes_egress_settings_name_a_network_of_their_own(tmp_path, changes):
    with pytest.raises(ModelError):
        settings(tmp_path, **changes)


class FakeDocker:
    """Docker as the egress service sees it: its networks, and each command it is given."""

    def __init__(self, monkeypatch, networks):
        self.commands, self.networks, self.fail = [], networks, None
        monkeypatch.setattr(egress.subprocess, "run", self)

    def __call__(self, command, **options):
        if command[0] == "hostname":
            return subprocess.CompletedProcess(command, 0, "10.60.0.7 172.17.0.1 fd00::7\n", "")
        assert tuple(command[:2]) == egress.DOCKER and "DOCKER_HOST" not in options["env"]
        args = command[2:]
        self.commands.append(args)
        if args[:2] == ["network", "inspect"]:
            found = self.networks.get(args[2])
            return subprocess.CompletedProcess(command, 0 if found else 1, json.dumps([found]) if found else "", "no such network")
        if args[:2] == ["network", "create"]:
            self.networks[args[-1]] = {"Driver": "bridge", "Internal": "--internal" in args,
                "IPAM": {"Config": [{"Subnet": "172.19.0.0/16", "Gateway": "172.19.0.1"}]}}
        if self.fail and args[0] == self.fail:
            return subprocess.CompletedProcess(command, 1, "", "this network reaches 172.19.0.1 (Connection refused)")
        return subprocess.CompletedProcess(command, 0, "", "")


DEFAULT_BRIDGE = {"Driver": "bridge", "Internal": False, "IPAM": {"Config": [{"Subnet": "172.17.0.0/16"}]}}


def test_the_service_makes_the_network_starts_the_proxy_and_checks_a_jobs_view(tmp_path, monkeypatch):
    docker = FakeDocker(monkeypatch, {"bridge": dict(DEFAULT_BRIDGE)})
    config = BatchEgressConfig(image=IMAGE, key_file=str(tmp_path / "egress.key"))
    egress.start(config)
    # The node's key is made once, for its owner alone.
    key = tmp_path / "egress.key"
    assert len(key.read_bytes()) == 32 and key.stat().st_mode & 0o777 == 0o600
    first = key.read_bytes()
    commands = docker.commands
    # An internal network whose bridge has no address on the node.
    assert ["network", "create", "--internal", "--opt", "com.docker.network.bridge.inhibit_ipv4=true", "fleet-egress"] in commands
    create = next(c for c in commands if c[0] == "create")
    assert create[-6:] == [IMAGE, "egress-proxy", "--listen", "0.0.0.0:3128", "--key-file", egress.KEY_PATH]
    assert f"{key}:{egress.KEY_PATH}:ro" in create and create[create.index("--network") + 1] == "bridge"
    assert {"--init", "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges", f"{os.getuid()}:{os.getgid()}"} <= set(create)
    order = [c[0] if c[0] != "network" else " ".join(c[:2]) for c in commands]
    assert order[order.index("create"):] == ["create", "network connect", "start", "network inspect", "run"]
    # The check runs where a job runs, against the proxy and every address of the node.
    check = commands[-1]
    assert check[check.index("--network") + 1] == "fleet-egress"
    assert check[check.index(IMAGE) + 1:] == ["egress-check", "--proxy", "fleet-egress-proxy:3128",
        "10.60.0.7", "172.17.0.1", "172.19.0.1", "fd00::7"]

    # A later start keeps the network and the key.
    docker.commands.clear()
    egress.start(config)
    assert not any(c[:2] == ["network", "create"] for c in docker.commands) and key.read_bytes() == first


def test_a_network_that_reaches_the_node_or_is_not_internal_leaves_no_proxy(tmp_path, monkeypatch):
    docker = FakeDocker(monkeypatch, {"bridge": dict(DEFAULT_BRIDGE)})
    docker.fail = "run"
    with pytest.raises(RuntimeError, match="reaches 172.19.0.1"):
        egress.start(settings(tmp_path))
    assert docker.commands[-1] == ["rm", "--force", "fleet-egress-proxy"]

    # A network of that name made some other way is not used.
    docker = FakeDocker(monkeypatch, {"bridge": dict(DEFAULT_BRIDGE), "fleet-egress": {"Driver": "bridge", "Internal": False}})
    with pytest.raises(RuntimeError, match="not an internal bridge network"):
        egress.start(settings(tmp_path))
    assert not any(c[0] in ("create", "start", "run") for c in docker.commands)

    # A key others can read is refused before anything starts.
    docker = FakeDocker(monkeypatch, {"bridge": dict(DEFAULT_BRIDGE)})
    (tmp_path / "egress.key").chmod(0o644)
    with pytest.raises(RuntimeError, match="only its owner"):
        egress.start(settings(tmp_path))
    assert docker.commands == []
