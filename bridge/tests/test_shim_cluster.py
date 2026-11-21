"""Opt-in acceptance with an isolated local Ray cluster and real Docker.

TRUSTED_SHIM_TEST_BINARY must name the built companion trusted-shim-runtime.
Requires preloaded python:3.12-slim. No production scheduler/node is contacted.
"""
import json
import os
import socket
import subprocess
import tempfile
import uuid

import pytest
import ray
import yaml

from fleet_bridge import actors, node, reconcile, shim


def port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def inspect(name):
    return json.loads(subprocess.check_output(["docker", "inspect", name]))[0]


@pytest.mark.skipif(not os.environ.get("TRUSTED_SHIM_TEST_BINARY"), reason="requires companion binary and local Docker")
@pytest.mark.parametrize("daemon", [False, True])
def test_local_cluster_keeps_legacy_container_and_recovers_shim_allocation(tmp_path, daemon):
    suffix=uuid.uuid4().hex[:8]
    legacy=f"shim-legacy-{suffix}"; modern=f"shim-optin-{suffix}"
    legacy_app={"name":legacy,"namespace":"0"*32,"app":legacy,"kind":"app", "gpus":0,"cpus":0.5,
        "memory_gb":0.125,"image":"python:3.12-slim","port_base":port(),"stop_timeout_s":1,"ready_timeout_s":30,
        "command":["python","-m","http.server","{port}","--bind","127.0.0.1"]}
    new_app={**legacy_app,"name":modern,"app":modern,"port_base":0,"container_port":8080,
        "runtime":{"executor":"shim","driver":"docker"},
        "command":["python","-m","http.server","{port}","--bind","0.0.0.0"]}
    apps=tmp_path/"apps.yaml";batches=tmp_path/"batches.yaml";batches.write_text("batches: {}\n")
    cfg={"log_dir":str(tmp_path/"logs"),"apps":[legacy_app]}
    apps.write_text(yaml.safe_dump(cfg))
    executor=node.NodeExecutor(str(apps),str(batches),str(tmp_path/"jobs"))
    recovered=None
    service=None
    private_state=tempfile.TemporaryDirectory(prefix="shim-state-")
    with tempfile.TemporaryDirectory(prefix="shim-ray-") as ray_dir:
        ray.init(address="local",num_cpus=2,num_gpus=0,include_dashboard=False,
            object_store_memory=80*1024*1024,_temp_dir=ray_dir,log_to_driver=False,
            namespace=reconcile.NAMESPACE)
        try:
            executor.set_app({"app":legacy_app,"disabled":False},"legacy-start")
            old=inspect(actors.app_container(legacy+"-0"))
            # Installing shim and adding a new opted-in app leaves legacy config intact.
            cfg.update(shim_binary=os.environ["TRUSTED_SHIM_TEST_BINARY"],shim_state_dir=private_state.name,shim_daemon=daemon)
            if daemon:
                service=subprocess.Popen([cfg['shim_binary'],'--serve','--state-dir',private_state.name,
                    '--node-id',socket.gethostname()])
                import time
                from pathlib import Path
                for _ in range(100):
                    if Path(private_state.name,'control.sock').exists():break
                    time.sleep(.05)
            cfg["apps"].append(new_app);apps.write_text(yaml.safe_dump(cfg))
            executor.set_app({"app":legacy_app,"disabled":False},"legacy-reconcile")
            executor.set_app({"app":new_app,"disabled":False},"shim-start")
            allocation=executor.runtimes.read(modern+"-0")
            status=shim.invoke(allocation.binary,allocation.state_dir,socket.gethostname(),
                {"operation":"inspect","execution":allocation.execution},daemon=allocation.daemon)
            current=inspect(status["runtime_id"])
            assert status["port"] > 0
            assert current["NetworkSettings"]["Ports"]["8080/tcp"][0]["HostPort"] == str(status["port"])
            assert current["HostConfig"]["NanoCpus"]==500_000_000
            assert current["HostConfig"]["Memory"]==134_217_728
            assert all(p["HostIp"]=="127.0.0.1" for ports in current["HostConfig"]["PortBindings"].values() for p in ports)
            recovered=node.NodeExecutor(str(apps),str(batches),str(tmp_path/"jobs"))
            recovered.set_app({"app":new_app,"disabled":False},"after-adapter-restart")
            assert recovered.runtimes.read(modern+"-0").execution==allocation.execution
            # This fixture declares no service: no data route should be advertised.
            assert recovered.report().get("runtime_instances", []) == []
            assert inspect(status["runtime_id"])["State"]["StartedAt"]==current["State"]["StartedAt"]
            assert inspect(actors.app_container(legacy+"-0"))["Id"]==old["Id"]
            assert inspect(actors.app_container(legacy+"-0"))["State"]["StartedAt"]==old["State"]["StartedAt"]
            recovered.set_app({"app":new_app,"disabled":True},"shim-stop")
            assert recovered.runtimes.read(modern+"-0").cleaned
            assert node.container_gone(status["runtime_id"])
            assert inspect(actors.app_container(legacy+"-0"))["Id"]==old["Id"]
        finally:
            # Only this test's named allocations; never `ray stop` or Docker prune.
            try:
                errors=[]
                for name in (modern+"-0",legacy+"-0"):
                    try: executor._remove_replicas([name])
                    except Exception as error: errors.append(error)
                if errors: raise errors[0]
            finally:
                executor.pool.shutdown()
                if recovered: recovered.pool.shutdown()
                ray.shutdown()
                if service:
                    service.terminate();service.wait(timeout=10)
                private_state.cleanup()
