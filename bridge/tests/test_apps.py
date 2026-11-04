"""Local app execution keeps dedicated proof servers and confirms cleanup."""
from unittest.mock import Mock
import pytest
from fleet_bridge import reconcile

@pytest.mark.parametrize("gpus", [[], ["2", "3"]])
def test_apps_use_nvidia_only_when_ray_grants_gpus(gpus):
    from fleet_bridge import actors
    cfg = reconcile.load_apps({"apps": [{"name": "web", "kind": "app",
        "image": "web:latest", "port_base": 8080, "gpus": len(gpus), "cpus": 2,
        "command": ["serve", "--port", "{port}"]}]})
    spec = reconcile.replica_specs(cfg)["web-0"]
    command, port = actors.app_command(spec, gpus)
    assert port == 8080 and command[-3:] == ["serve", "--port", "8080"]
    assert reconcile.bundle(spec).gpu == len(gpus)
    if gpus:
        assert "--runtime=nvidia" in command
        assert command[command.index("--gpus") + 1] == '"device=2,3"'
    else:
        assert not any("nvidia" in arg.lower() for arg in command)
        assert "--gpus" not in command

def test_cpu_app_actor_starts_without_gpu_discovery(monkeypatch):
    from fleet_bridge import actors
    cfg = reconcile.load_apps({"apps": [{"name": "web", "kind": "app",
        "image": "web:latest", "port_base": 8080, "gpus": 0, "cpus": 2}]})
    spec = reconcile.replica_specs(cfg)["web-0"]
    discover = Mock(side_effect=AssertionError("CPU app must not request GPUs"))
    monkeypatch.setattr(actors, "_granted_gpus", discover)
    monkeypatch.setattr(actors, "_sh", lambda _: Mock(returncode=0))
    monkeypatch.setattr(actors, "_warn_foreign", lambda *_: None)
    monkeypatch.setattr(actors, "_http_ok", lambda _: True)
    monkeypatch.setattr(actors, "_watch", lambda *_: None)
    actor = actors._GpuApp(spec)
    assert actor.gpus == []
    discover.assert_not_called()

@pytest.mark.parametrize("integrated", [False, True])
def test_busy_gpu_probe_does_not_restart_integrated_worker(monkeypatch, integrated):
    from fleet_bridge import actors
    from fleet_bridge.model import ReplicaHealth
    actor = object.__new__(actors._ZirenGpuProver)
    actor.spec = Mock()
    actor.spec.app.worker_command = ["/opt/eth-proofs"] if integrated else None
    actor.name, actor.container = "eth-0", "fleet-gpu-eth-0"
    actor.gpus, actor.gpu_port = ["0"], 30100
    actor.node = Mock()
    actor.node.alive.return_value = True
    monkeypatch.setattr(actors, "_stopping", lambda: False)
    monkeypatch.setattr(actors, "_container_running", lambda _: True)
    monkeypatch.setattr(actors, "spec_hash", lambda _: "unchanged")
    probe = Mock(return_value=False)
    monkeypatch.setattr(actors, "_gpu_ready", probe)
    health = ReplicaHealth.load(actor.health())
    assert health.is_ready == integrated
    if integrated:
        probe.assert_not_called()
    else:
        probe.assert_called_once_with(30100)

def test_integrated_worker_needs_no_native_prover_binary_or_control_credentials(tmp_path, monkeypatch):
    from fleet_bridge import actors
    cfg = reconcile.load_apps({"log_dir": str(tmp_path), "apps": [{
        "name": "worker", "kind": "ziren-gpu", "image": "prover:latest",
        "namespace": "a" * 32, "app": "worker", "elf_id": "b" * 64,
        "worker_command": ["/opt/worker"], "proof_storage_root": str(tmp_path / "state"),
        "machine": "local", "gpus": 1}]})
    spec = reconcile.replica_specs(cfg)["worker-0"]
    monkeypatch.setattr(actors, "_granted_gpus", lambda: ["0"])
    monkeypatch.setattr(actors, "_kill_orphans", lambda _: None)
    monkeypatch.setattr(actors, "_warn_foreign", lambda *_: None)
    monkeypatch.setattr(actors, "_sh", lambda _: Mock(returncode=0))
    monkeypatch.setattr(actors, "_gpu_ready", lambda _: True)
    monkeypatch.setattr(actors, "_watch", lambda *_: None)
    process = Mock()
    monkeypatch.setattr(actors, "_NodeProcess", process)
    actors._ZirenGpuProver(spec)
    command, env, _ = process.call_args.args[:3]
    assert command == ["/opt/worker"]
    assert env["FLEET_GPU_ENDPOINT"] == "http://127.0.0.1:30100/twirp/"
    assert "FLEET_CLUSTER_TOKEN_FILE" not in env and "FLEET_SCHEDULER" not in env

def test_stop_kills_actor_without_restart_and_removes_reservation(monkeypatch):
    actor, pg = Mock(), Mock()
    monkeypatch.setattr(reconcile.ray, "get_actor", lambda *a, **k: actor)
    kill, remove = Mock(), Mock()
    monkeypatch.setattr(reconcile.ray, "kill", kill)
    monkeypatch.setattr(reconcile.ray, "get", lambda *a, **k: None)
    monkeypatch.setattr(reconcile, "clean_after", Mock())
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
    reconcile.clean_after.remote.assert_called_once_with("comfyui-0")
    remove.assert_called_once_with(pg)


def test_gpu_provers_must_be_bound_to_one_app():
    with pytest.raises(ValueError, match="proves one program"):
        reconcile.load_apps({"apps": [{"name": "shared-gpu", "kind": "ziren-gpu"}]})
    with pytest.raises(ValueError, match="full ELF SHA-256"):
        reconcile.load_apps({"apps": [{"name": "short", "kind": "ziren-gpu", "elf_id": "abc"}]})
    with pytest.raises(ValueError, match="must be a name"):
        reconcile.load_apps({"apps": [{"name": "old", "kind": "ziren-gpu", "app": "eth/" + "a" * 64, "elf_id": "a" * 64}]})
    with pytest.raises(ValueError, match="is its own app"):
        reconcile.load_apps({"apps": [{"name": "wrong-kind", "kind": "app", "app": "eth"}]})
    with pytest.raises(ValueError, match="only a Ziren prover"):
        reconcile.load_apps({"apps": [{"name": "web", "kind": "app", "elf_id": "a" * 64}]})
    assert reconcile.load_apps({"apps": [{"name": "web", "kind": "app"}]}).apps[0].app == "web"
    prover = reconcile.load_apps({"apps": [{"name": "eth-gpu", "kind": "ziren-gpu", "app": "eth", "elf_id": "A" * 64}]}).apps[0]
    assert (prover.app, prover.elf_id) == ("eth", "a" * 64)


def test_proof_replicas_each_get_one_server_and_the_same_app_storage_root():
    from fleet_bridge import actors
    cfg = reconcile.load_apps({"scheduler": "https://scheduler:8905", "cluster_token_file": "/etc/cluster.token", "apps": [{
        "name": "eth", "kind": "ziren-gpu", "namespace": "a"*32,
        "app": "eth", "elf_id": "b"*64, "proof_storage_root": "/srv/eth", "replicas": 2, "gpus": 2,
    }]})
    specs = reconcile.replica_specs(cfg)
    assert list(specs) == ["eth-0", "eth-1"]
    for name, spec in specs.items():
        env = actors._node_env(spec, "cuda", gpu_endpoint="http://127.0.0.1:30100/twirp/")
        assert env["FLEET_NODE_NAME"] == name
        assert env["FLEET_APP_STORAGE_ROOT"] == "/srv/eth"
        assert env["FLEET_APP_NAMESPACE"] == "a"*32
        assert env["FLEET_APP"] == "eth"
        assert env["FLEET_APP_ELF_ID"] == "b"*64
        assert reconcile.bundle(spec).gpu == 2


def test_integrated_worker_requires_single_machine_pinned_replica():
    config = {"name": "eth", "kind": "ziren-gpu", "app": "eth", "elf_id": "b" * 64,
              "worker_command": ["/opt/eth-proofs"], "proof_storage_root": "/srv/eth",
              "machine": "ant", "replicas": 1}
    assert reconcile.load_apps({"apps": [config]}).apps[0].worker_command == ["/opt/eth-proofs"]
    for update in [{"replicas": 2}, {"machine": None}, {"proof_storage_root": None}]:
        with pytest.raises(ValueError, match="machine-pinned"):
            reconcile.load_apps({"apps": [{**config, **update}]})


def test_integrated_worker_environment_keeps_replica_scope_and_credentials_separate(tmp_path, monkeypatch):
    from fleet_bridge import actors
    env_file = tmp_path / "worker.env"
    env_file.write_text('ETH_PROOFS_API_TOKEN="worker token"\nFLEET_GPU_ENDPOINT=http://evil\nFLEET_APP_ELF_ID=wrong\n')
    cfg = reconcile.load_apps({"apps": [{"name": "eth", "kind": "ziren-gpu",
        "namespace": "a" * 32, "app": "eth", "elf_id": "b" * 64, "proof_storage_root": "/srv/eth",
        "machine": "ant", "worker_command": ["/opt/eth-proofs"], "worker_env_file": str(env_file)}]})
    spec = reconcile.replica_specs(cfg)["eth-0"]
    monkeypatch.setenv("FLEET_CLUSTER_TOKEN_FILE", "/etc/cluster.token")
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "0")
    env = actors._app_worker_env(spec, "http://127.0.0.1:30100/twirp/")
    assert env["ETH_PROOFS_API_TOKEN"] == "worker token"
    assert env["FLEET_GPU_ENDPOINT"] == "http://127.0.0.1:30100/twirp/"
    assert (env["FLEET_APP"], env["FLEET_APP_ELF_ID"]) == ("eth", "b" * 64)
    assert env["FLEET_EXIT_WITH_PARENT"] == "true"
    assert "FLEET_CLUSTER_TOKEN_FILE" not in env
    assert "CUDA_VISIBLE_DEVICES" not in env


def test_worker_process_uses_argument_vector_and_waits_after_forced_stop(tmp_path, monkeypatch):
    import subprocess
    from fleet_bridge import actors
    monkeypatch.setattr(actors, "_kill_orphans", lambda name: None)
    process = Mock()
    process.poll.return_value = None
    process.wait.side_effect = [subprocess.TimeoutExpired("worker", 2), 0]
    popen = Mock(return_value=process)
    monkeypatch.setattr(actors.subprocess, "Popen", popen)
    worker = actors._NodeProcess(["/opt/eth-proofs", "--block-interval", "1"],
                                {"FLEET_NODE_NAME": "eth-0"}, str(tmp_path / "worker.log"),
                                workdir=str(tmp_path), stop_timeout_s=2)
    assert popen.call_args.args[0] == ["/opt/eth-proofs", "--block-interval", "1"]
    assert popen.call_args.kwargs["cwd"] == str(tmp_path)
    worker.stop()
    process.kill.assert_called_once()
    assert process.wait.call_count == 2
    assert worker.log.closed

def test_failed_start_removes_its_placement_group(monkeypatch):
    cfg = reconcile.load_apps({"apps": [{"name": "web", "kind": "app",
        "image": "web:latest", "port_base": 8080, "gpus": 0, "cpus": 2}]})
    spec = reconcile.replica_specs(cfg)["web-0"]
    monkeypatch.setattr(reconcile, "placement_group", lambda *a, **k: Mock())
    monkeypatch.setattr(reconcile.ray, "get", Mock(side_effect=TimeoutError("not ready")))
    stop = Mock()
    monkeypatch.setattr(reconcile, "stop", stop)
    with pytest.raises(TimeoutError):
        reconcile.start(spec)
    stop.assert_called_once_with(spec.name)

def test_worker_workdir_is_created(tmp_path, monkeypatch):
    from fleet_bridge import actors
    monkeypatch.setattr(actors, "_kill_orphans", lambda _: None)
    workdir = tmp_path / "missing" / "worker"
    proc = actors._NodeProcess(["true"], {"FLEET_NODE_NAME": "n"}, str(tmp_path / "log"), str(workdir))
    proc.proc.wait()
    assert workdir.is_dir()
