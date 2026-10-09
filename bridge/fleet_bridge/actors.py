"""Ray actors that run prover nodes, and GPU apps that serve on their own port.

A ZirenGpuProver owns one GPU server: the container on exactly the cards Ray
granted the actor (all on one machine: the server's per-card workers share
memory and keep state on the cards), plus a fleet-ziren-node process that proves
through the Ziren SDK against it and registers with the scheduler.

If either process dies, the actor exits and Ray restarts it (max_restarts=-1).
Every actor takes a ReplicaSpec and answers health() with a Health.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import shlex
import socket
import subprocess
import threading
import time
import urllib.request
from typing import Any, Callable

import ray

from .model import Health, NodeEnv, ReplicaSpec, to_env
from .rayapi import node_ip

NODE_BINARY_NAMES = ("fleet-ziren-node", "fleet-node")  # the current name and the one before it


def spec_hash(spec: ReplicaSpec) -> str:
    """Over the replica's own mapping from the services file: a bridge upgrade never
    replaces a running service by itself. Priority is scheduling metadata and
    does not require restarting a container."""
    return hashlib.sha256(json.dumps({k: v for k, v in spec.config.items() if k != "priority"}, sort_keys=True).encode()).hexdigest()[:16]


def _sh(argv: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(argv, capture_output=True, text=True)


def gpu_container(name: str) -> str:
    return f"fleet-gpu-{name}"


def app_container(name: str) -> str:
    return f"fleet-app-{name}"


def _gpu_ready(port: int) -> bool:
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/twirp/api.ProverService/Ready",
        data=b"",
        headers={"content-type": "application/protobuf"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=3) as r:
            return bool(r.status == 200 and r.read() == b"\x08\x01")
    except Exception:
        return False


def _node_env(spec: ReplicaSpec, backend: str, service_env: bool = False, **extra: object) -> dict[str, str]:
    """The node's environment: the process's own, plus a NodeEnv (its fields carry the
    variable names); with service_env the service's env too (a CPU prover's node is
    the service; a GPU prover's env goes to its server container instead)."""
    c = spec.cluster
    if not (c.scheduler and c.cluster_token_file):
        raise RuntimeError(f"{spec.name}: services.yaml needs scheduler and cluster_token_file for a prover")
    app_namespace = None
    app_tenant = None
    app_elf_id = None
    if spec.service.proof_app:
        app_tenant, app_elf_id = spec.service.proof_app.split("/", 1)
        app_namespace = spec.service.namespace
    node = NodeEnv(name=spec.name, backend=backend, scheduler=c.scheduler,
                   cluster_token_file=c.cluster_token_file, machine=socket.gethostname(),
                   app_namespace=app_namespace, app_tenant=app_tenant, app_elf_id=app_elf_id,
                   rust_log=spec.service.rust_log, cpus=int(spec.service.cpus),
                   output_dir=c.proof_output_dir or os.path.join(c.log_dir, "proofs"), **extra)  # type: ignore[arg-type]
    env = dict(os.environ)
    env.update(to_env(node))
    if service_env:
        env.update(spec.service.env)
    # The node itself does not touch the GPUs; the GPU server does.
    env.pop("CUDA_VISIBLE_DEVICES", None)
    return env


def _warn_foreign(name: str, gpus: list[str]) -> None:
    """Ray's grant is logical: say so in the actor log when something outside the fleet
    already holds memory on the granted cards (a service still starts; a one-shot run
    waits for the memory instead, see runs.py)."""
    from .runs import card_memory

    for idx, c in card_memory(gpus).items():
        if c.holders:
            print(f"WARNING {name}: card {idx} granted by Ray already holds {c.held_by()}", flush=True)


# Set by a deliberate shutdown (one actor per process): the watcher and health() then
# leave the process to Ray's kill instead of exiting, which Ray would answer with a
# restart (max_restarts=-1) racing the replacement for the container name and port.
_STOPPING = {"on": False}  # a dict, not an Event: Ray pickles the class with this global


def _stopping() -> bool:
    return _STOPPING["on"]


def _watch(alive: Callable[[], bool], cleanup: Callable[[], None], every_s: float = 5.0) -> None:
    """Exits the actor process when what it owns dies, so Ray restarts the actor."""

    def loop() -> None:
        while not _stopping():
            time.sleep(every_s)
            if _stopping():
                return
            if not alive() and not _stopping():  # a deliberate stop takes what it owns away too
                cleanup()
                os._exit(1)

    threading.Thread(target=loop, daemon=True).start()


@ray.remote(num_cpus=0, max_retries=0)
def clean_after(name: str) -> str:
    """Removes what an actor of this name owned on this machine (its containers and
    node process), after the actor itself was killed: nothing of it then survives to
    race its replacement for the container name or the GPU server port."""
    for c in (gpu_container(name), app_container(name)):
        _sh(["docker", "rm", "-f", c])
    _kill_orphans(name)
    return socket.gethostname()


def _kill_orphans(name: str) -> None:
    """Kills node processes left by an earlier actor of this name (`ray stop` takes
    the actor but not its children; nodes since then exit with their parent): an orphan
    keeps the name registered and points it at a GPU server port a new container may reuse.
    Names are unique per actor, so any such process is stale."""
    comm_names = {n[:15] for n in NODE_BINARY_NAMES}  # /proc/<pid>/comm holds 15 bytes of the name
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            with open(f"/proc/{pid}/comm") as f:
                if f.read().strip() not in comm_names:
                    continue
            with open(f"/proc/{pid}/environ", "rb") as f:
                if f"FLEET_NODE_NAME={name}".encode() not in f.read().split(b"\0"):
                    continue
        except OSError:
            continue
        os.kill(int(pid), 15)
        for _ in range(20):
            if not os.path.exists(f"/proc/{pid}"):
                break
            time.sleep(0.5)
        else:
            os.kill(int(pid), 9)


class _NodeProcess:
    def __init__(self, binary: str, env: dict[str, str], log_path: str) -> None:
        _kill_orphans(env["FLEET_NODE_NAME"])
        self.log: io.BufferedWriter = open(log_path, "ab")
        self.proc = subprocess.Popen([binary], env=env, stdout=self.log, stderr=subprocess.STDOUT)

    def alive(self) -> bool:
        return self.proc.poll() is None

    def stop(self) -> None:
        if self.alive():
            self.proc.terminate()
            try:
                self.proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.proc.kill()


def _container_running(container: str) -> bool:
    return _sh(["docker", "inspect", "-f", "{{.State.Running}}", container]).stdout.strip() == "true"


def _container_logs(container: str) -> str:
    logs = _sh(["docker", "logs", "--tail", "40", container])
    return logs.stdout + logs.stderr


def _granted_gpus() -> list[str]:
    gpus = [str(int(g)) for g in ray.get_gpu_ids()]
    if not gpus:
        raise RuntimeError("actor was granted no GPU")
    return gpus


def _node_binary(spec: ReplicaSpec) -> str:
    if not spec.cluster.node_binary:
        raise RuntimeError(f"{spec.name}: services.yaml needs node_binary for a prover")
    return spec.cluster.node_binary


class _ZirenGpuProver:
    def __init__(self, spec: ReplicaSpec) -> None:
        self.spec = spec
        self.name = spec.name
        self.container = gpu_container(self.name)
        self.gpus = _granted_gpus()
        os.makedirs(spec.cluster.log_dir, exist_ok=True)
        svc = spec.service
        if not svc.image:
            raise RuntimeError(f"{self.name}: a ziren-gpu service needs an image")

        _sh(["docker", "rm", "-f", self.container])
        _warn_foreign(self.name, self.gpus)
        env_args: list[str] = []
        for k, v in {"ZIREN_SERVER_PORT": "3000", **svc.env}.items():
            env_args += ["-e", f"{k}={v}"]
        for k in ("CUDA_VISIBLE_DEVICES", "ZKM_GPU_DEVICES"):
            env_args += ["-e", f"{k}={','.join(str(i) for i in range(len(self.gpus)))}"]
        lo, hi = spec.cluster.gpu_port_range
        for port in range(lo, hi + 1):
            cmd = [
                "docker", "run", "-d", "--name", self.container,
                "--runtime=nvidia", "--gpus", f'"device={",".join(self.gpus)}"',
                f"--shm-size={svc.shm_gb if svc.shm_gb is not None else 64}g",
                "-p", f"127.0.0.1:{port}:3000",
                "--label", "zkm-fleet=gpu-server", "--label", f"zkm-fleet.node={self.name}",
                *env_args,
            ]
            for v in svc.volumes:
                cmd += ["-v", v]
            cmd.append(svc.image)
            out = _sh(cmd)
            if out.returncode == 0:
                self.gpu_port = port
                break
            _sh(["docker", "rm", "-f", self.container])
            if "port is already allocated" not in out.stderr and "address already in use" not in out.stderr:
                raise RuntimeError(f"docker run failed: {out.stderr.strip()} ({shlex.join(cmd)})")
        else:
            raise RuntimeError(f"no free GPU server port in {lo}-{hi}")

        deadline = time.time() + svc.ready_timeout_s
        while not _gpu_ready(self.gpu_port):
            if time.time() > deadline or not _container_running(self.container):
                raise RuntimeError(f"GPU server did not become ready:\n{_container_logs(self.container)}")
            time.sleep(2)

        env = _node_env(spec, "cuda", gpu_endpoint=f"http://127.0.0.1:{self.gpu_port}/twirp/",
                        gpus=len(self.gpus), image=svc.image)
        self.node = _NodeProcess(_node_binary(spec), env, spec.node_log_path())
        _watch(lambda: self.node.alive() and _container_running(self.container), self.shutdown)

    def health(self) -> dict[str, Any]:
        container = _container_running(self.container)
        node = self.node.alive()
        if _stopping():
            return Health(name=self.name, stopping=True).dump()
        if not (container and node):
            self.shutdown()
            os._exit(1)  # Ray restarts the actor
        return Health(name=self.name, spec=spec_hash(self.spec), gpus=self.gpus, gpu_port=self.gpu_port,
                      gpu_ready=_gpu_ready(self.gpu_port), machine=socket.gethostname()).dump()

    def shutdown(self) -> None:
        _STOPPING["on"] = True
        self.node.stop()
        _sh(["docker", "rm", "-f", self.container])


def app_command(spec: ReplicaSpec, gpus: list[str]) -> tuple[list[str], int]:
    """`docker run` for one replica of an app service, and the port it serves on.

    Volumes are mounted at the same path inside, so absolute paths baked into the app's
    tree (a venv's interpreter, model links, database URLs) keep working; `:ro` and
    `:rslave` (an NFS mount remounted later still shows up inside) pass through.
    `{port}`, `{index}` and `{gpus}` in the command and env are filled in per replica.
    """
    svc = spec.service
    if svc.port_base is None or not svc.image:
        raise RuntimeError(f"{spec.name}: an app service needs image and port_base")
    port = svc.port_base + spec.index
    fill = {"port": port, "index": spec.index, "gpus": ",".join(str(i) for i in range(len(gpus)))}
    cmd = [
        "docker", "run", "-d", "--name", app_container(spec.name),
        "--runtime=nvidia", "--gpus", f'"device={",".join(gpus)}"',
        "-e", "NVIDIA_DRIVER_CAPABILITIES=all",
        "--network", "host",
        f"--shm-size={svc.shm_gb if svc.shm_gb is not None else 16}g",
        "--label", "zkm-fleet=app", "--label", f"zkm-fleet.node={spec.name}",
    ]
    if svc.user:
        cmd += ["--user", svc.user]
    if svc.workdir:
        cmd += ["-w", svc.workdir]
    for v in svc.volumes:
        path, _, opts = v.partition(":")
        cmd += ["-v", f"{path}:{path}" + (f":{opts}" if opts else "")]
    for k, v in svc.env.items():
        cmd += ["-e", f"{k}={v.format(**fill)}"]
    cmd.append(svc.image)
    cmd += [a.format(**fill) for a in svc.command]
    return cmd, port


def _http_ok(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            return bool(r.status == 200)
    except Exception:
        return False


ZirenGpuProver = ray.remote(_ZirenGpuProver)


class _GpuApp:
    """A GPU container that serves on its own port (ComfyUI, an inference server).

    It does not register with the scheduler: its clients address it by port, which is
    fixed per replica (`port_base` + replica index), on the machine Ray placed it on.
    """

    def __init__(self, spec: ReplicaSpec) -> None:
        self.spec = spec
        self.name = spec.name
        self.container = app_container(self.name)
        self.gpus = _granted_gpus()
        cmd, self.port = app_command(spec, self.gpus)
        self.health_url = f"http://127.0.0.1:{self.port}{spec.service.health_path}"
        _sh(["docker", "rm", "-f", self.container])
        _warn_foreign(self.name, self.gpus)
        out = _sh(cmd)
        if out.returncode != 0:
            raise RuntimeError(f"docker run failed: {out.stderr.strip()} ({shlex.join(cmd)})")
        deadline = time.time() + spec.service.ready_timeout_s
        while not _http_ok(self.health_url):
            if time.time() > deadline or not _container_running(self.container):
                logs = _container_logs(self.container)
                self.shutdown()
                raise RuntimeError(f"{self.name} did not answer {self.health_url}:\n{logs}")
            time.sleep(2)
        _watch(lambda: _container_running(self.container), self.shutdown)

    def health(self) -> dict[str, Any]:
        if _stopping():
            return Health(name=self.name, stopping=True).dump()
        if not _container_running(self.container):
            self.shutdown()
            os._exit(1)
        return Health(name=self.name, spec=spec_hash(self.spec), gpus=self.gpus, port=self.port,
                      ready=_http_ok(self.health_url), machine=socket.gethostname()).dump()

    def shutdown(self) -> None:
        _STOPPING["on"] = True
        _sh(["docker", "stop", "-t", str(self.spec.service.stop_timeout_s), self.container])
        _sh(["docker", "rm", "-f", self.container])


GpuApp = ray.remote(_GpuApp)


class _ZirenCpuProver:
    """A native (CPU) prover node: the Ziren SDK's CPU prover in fleet-ziren-node."""

    def __init__(self, spec: ReplicaSpec) -> None:
        self.spec = spec
        self.name = spec.name
        os.makedirs(spec.cluster.log_dir, exist_ok=True)
        cpus = int(spec.service.cpus)
        env = _node_env(spec, spec.service.backend, service_env=True, rayon_threads=cpus or None)
        self.node = _NodeProcess(_node_binary(spec), env, spec.node_log_path())
        _watch(self.node.alive, self.shutdown)

    def health(self) -> dict[str, Any]:
        if _stopping():
            return Health(name=self.name, stopping=True).dump()
        if not self.node.alive():
            os._exit(1)
        return Health(name=self.name, spec=spec_hash(self.spec), machine=socket.gethostname()).dump()

    def shutdown(self) -> None:
        _STOPPING["on"] = True
        self.node.stop()


ZirenCpuProver = ray.remote(_ZirenCpuProver)
