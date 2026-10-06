"""Ray actors that run prover nodes, and GPU apps that serve on their own port.

A ZirenGpuProver owns one GPU server: the container on exactly the cards Ray
granted the actor (all on one machine: the server's per-card workers share
memory and keep state on the cards), plus a fleet-ziren-node process that proves
through the Ziren SDK against it and registers with the scheduler.

If either process dies, the actor exits and Ray restarts it (max_restarts=-1).
"""

from __future__ import annotations

import hashlib
import json
import os
import shlex
import socket
import subprocess
import threading
import time
import urllib.request

import ray


def spec_hash(spec: dict) -> str:
    return hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()[:16]


def _gpu_ready(port: int) -> bool:
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/twirp/api.ProverService/Ready",
        data=b"",
        headers={"content-type": "application/protobuf"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=3) as r:
            return r.status == 200 and r.read() == b"\x08\x01"
    except Exception:
        return False


def _node_env(spec: dict, name: str, backend: str, extra: dict) -> dict:
    env = dict(os.environ)
    lo, hi = spec.get("node_port_range", [31000, 31099])
    env.update(
        {
            "FLEET_NODE_NAME": name,
            "FLEET_BACKEND": backend,
            "FLEET_PORT_RANGE": f"{lo}-{hi}",
            "FLEET_ADVERTISE_HOST": ray.util.get_node_ip_address(),
            "FLEET_SCHEDULER": spec["scheduler"],
            "FLEET_CLUSTER_TOKEN_FILE": spec["cluster_token_file"],
            "FLEET_MACHINE": socket.gethostname(),
            "RUST_LOG": spec.get("rust_log", "info"),
        }
    )
    env.update({k: str(v) for k, v in extra.items()})
    # The node itself does not touch the GPUs; the GPU server does.
    env.pop("CUDA_VISIBLE_DEVICES", None)
    return env


def _warn_foreign(name: str, gpus: list[str]):
    """Ray's grant is logical: say so in the actor log when something outside the fleet
    already holds memory on the granted cards (a service still starts; a one-shot run
    waits for the memory instead, see runs.py)."""
    from .runs import card_memory

    for idx, c in card_memory(gpus).items():
        if c["holders"]:
            held = ", ".join(f"pid {h['pid']} {h['mib']} MiB ({h['cmd']})" for h in c["holders"])
            print(f"WARNING {name}: card {idx} granted by Ray already holds {held}", flush=True)


# Set by a deliberate shutdown (one actor per process): the watcher and health() then
# leave the process to Ray's kill instead of exiting, which Ray would answer with a
# restart (max_restarts=-1) racing the replacement for the container name and port.
_STOPPING = {"on": False}  # a dict, not an Event: Ray pickles the class with this global


def _watch(alive, cleanup, every_s: float = 5.0):
    """Exits the actor process when what it owns dies, so Ray restarts the actor."""

    def loop():
        while not _STOPPING["on"]:
            time.sleep(every_s)
            if _STOPPING["on"]:
                return
            if not alive() and not _STOPPING["on"]:  # a deliberate stop takes what it owns away too
                cleanup()
                os._exit(1)

    threading.Thread(target=loop, daemon=True).start()


@ray.remote(num_cpus=0, max_retries=0)
def clean_after(name: str):
    """Removes what an actor of this name owned on this machine (its containers and
    node process), after the actor itself was killed: nothing of it then survives to
    race its replacement for the container name or the GPU server port."""
    for c in (f"fleet-gpu-{name}", f"fleet-app-{name}"):
        subprocess.run(["docker", "rm", "-f", c], capture_output=True)
    _kill_orphans(name)
    return socket.gethostname()


def _kill_orphans(name: str):
    """Kills fleet-ziren-node processes left by an earlier actor of this name (`ray stop` takes
    the actor but not its children; nodes since then exit with their parent): an orphan
    keeps the name registered and points it at a GPU server port a new container may reuse.
    Names are unique per actor, so any such process is stale."""
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            with open(f"/proc/{pid}/comm") as f:
                # comm holds the first 15 bytes of the binary's name (fleet-node = the old name)
                if f.read().strip() not in ("fleet-ziren-node"[:15], "fleet-node"):
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
    def __init__(self, binary: str, env: dict, log_path: str):
        _kill_orphans(env["FLEET_NODE_NAME"])
        self.log = open(log_path, "ab")
        env = {**env, "FLEET_EXIT_WITH_PARENT": "true"}
        self.proc = subprocess.Popen([binary], env=env, stdout=self.log, stderr=subprocess.STDOUT)

    def alive(self) -> bool:
        return self.proc.poll() is None

    def stop(self):
        if self.alive():
            self.proc.terminate()
            try:
                self.proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.proc.kill()


@ray.remote(max_restarts=-1, max_task_retries=0)
class ZirenGpuProver:
    def __init__(self, spec: dict):
        self.spec = spec
        self.name = spec["name"]
        self.container = f"fleet-gpu-{self.name}"
        gpus = [str(int(g)) for g in ray.get_gpu_ids()]
        if not gpus:
            raise RuntimeError("actor was granted no GPU")
        self.gpus = gpus
        log_dir = spec.get("log_dir", "/tmp/zkm-fleet")
        os.makedirs(log_dir, exist_ok=True)

        subprocess.run(["docker", "rm", "-f", self.container], capture_output=True)
        _warn_foreign(self.name, gpus)
        env_args = []
        for k, v in {"ZIREN_SERVER_PORT": "3000", **spec.get("env", {})}.items():
            env_args += ["-e", f"{k}={v}"]
        for k in ("CUDA_VISIBLE_DEVICES", "ZKM_GPU_DEVICES"):
            env_args += ["-e", f"{k}={','.join(str(i) for i in range(len(gpus)))}"]
        lo, hi = spec.get("gpu_port_range", [30100, 30199])
        for port in range(lo, hi + 1):
            cmd = [
                "docker", "run", "-d", "--name", self.container,
                "--runtime=nvidia", "--gpus", f'"device={",".join(gpus)}"',
                f"--shm-size={spec.get('shm_gb', 64)}g",
                "-p", f"127.0.0.1:{port}:3000",
                "--label", "zkm-fleet=gpu-server", "--label", f"zkm-fleet.node={self.name}",
                *env_args,
            ]
            for v in spec.get("volumes", []):
                cmd += ["-v", v]
            cmd.append(spec["image"])
            out = subprocess.run(cmd, capture_output=True, text=True)
            if out.returncode == 0:
                self.gpu_port = port
                break
            subprocess.run(["docker", "rm", "-f", self.container], capture_output=True)
            if "port is already allocated" not in out.stderr and "address already in use" not in out.stderr:
                raise RuntimeError(f"docker run failed: {out.stderr.strip()} ({shlex.join(cmd)})")
        else:
            raise RuntimeError(f"no free GPU server port in {lo}-{hi}")

        deadline = time.time() + spec.get("ready_timeout_s", 600)
        while not _gpu_ready(self.gpu_port):
            if time.time() > deadline or not self._container_running():
                logs = subprocess.run(["docker", "logs", "--tail", "40", self.container], capture_output=True, text=True)
                raise RuntimeError(f"GPU server did not become ready:\n{logs.stdout}{logs.stderr}")
            time.sleep(2)

        env = _node_env(
            spec, self.name, "cuda",
            {
                "FLEET_GPU_ENDPOINT": f"http://127.0.0.1:{self.gpu_port}/twirp/",
                "FLEET_GPUS": len(gpus),
                "FLEET_CPUS": spec.get("cpus", 0),
                "FLEET_IMAGE": spec["image"],
            },
        )
        self.node = _NodeProcess(spec["node_binary"], env, f"{log_dir}/{self.name}.node.log")
        _watch(lambda: self.node.alive() and self._container_running(), self.shutdown)

    def _container_running(self) -> bool:
        out = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", self.container], capture_output=True, text=True
        )
        return out.stdout.strip() == "true"

    def health(self) -> dict:
        container = self._container_running()
        node = self.node.alive()
        if _STOPPING["on"]:
            return {"name": self.name, "stopping": True}
        if not (container and node):
            self.shutdown()
            os._exit(1)  # Ray restarts the actor
        return {
            "name": self.name,
            "spec": spec_hash(self.spec),
            "gpus": self.gpus,
            "gpu_port": self.gpu_port,
            "gpu_ready": _gpu_ready(self.gpu_port),
            "machine": socket.gethostname(),
        }

    def shutdown(self):
        _STOPPING["on"] = True
        self.node.stop()
        subprocess.run(["docker", "rm", "-f", self.container], capture_output=True)


def app_command(spec: dict, gpus: list[str], index: int) -> tuple[list[str], int]:
    """`docker run` for one replica of an app service, and the port it serves on.

    Volumes are mounted at the same path inside, so absolute paths baked into the app's
    tree (a venv's interpreter, model links, database URLs) keep working; `:ro` and
    `:rslave` (an NFS mount remounted later still shows up inside) pass through.
    `{port}`, `{index}` and `{gpus}` in the command and env are filled in per replica.
    """
    port = int(spec["port_base"]) + index
    fill = {"port": port, "index": index, "gpus": ",".join(str(i) for i in range(len(gpus)))}
    cmd = [
        "docker", "run", "-d", "--name", f"fleet-app-{spec['name']}",
        "--runtime=nvidia", "--gpus", f'"device={",".join(gpus)}"',
        "-e", "NVIDIA_DRIVER_CAPABILITIES=all",
        "--network", "host",
        f"--shm-size={spec.get('shm_gb', 16)}g",
        "--label", "zkm-fleet=app", "--label", f"zkm-fleet.node={spec['name']}",
    ]
    if spec.get("user"):
        cmd += ["--user", str(spec["user"])]
    if spec.get("workdir"):
        cmd += ["-w", spec["workdir"]]
    for v in spec.get("volumes", []):
        path, _, opts = v.partition(":")
        cmd += ["-v", f"{path}:{path}" + (f":{opts}" if opts else "")]
    for k, v in spec.get("env", {}).items():
        cmd += ["-e", f"{k}={str(v).format(**fill)}"]
    cmd.append(spec["image"])
    cmd += [str(a).format(**fill) for a in spec["command"]]
    return cmd, port


def _http_ok(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            return r.status == 200
    except Exception:
        return False


@ray.remote(max_restarts=-1, max_task_retries=0)
class GpuApp:
    """A GPU container that serves on its own port (ComfyUI, an inference server).

    It does not register with the scheduler: its clients address it by port, which is
    fixed per replica (`port_base` + replica index), on the machine Ray placed it on.
    """

    def __init__(self, spec: dict):
        self.spec = spec
        self.name = spec["name"]
        self.container = f"fleet-app-{self.name}"
        gpus = [str(int(g)) for g in ray.get_gpu_ids()]
        if not gpus:
            raise RuntimeError("actor was granted no GPU")
        self.gpus = gpus
        cmd, self.port = app_command(spec, gpus, int(spec.get("index", 0)))
        self.health_url = f"http://127.0.0.1:{self.port}{spec.get('health_path', '/')}"
        subprocess.run(["docker", "rm", "-f", self.container], capture_output=True)
        _warn_foreign(self.name, gpus)
        out = subprocess.run(cmd, capture_output=True, text=True)
        if out.returncode != 0:
            raise RuntimeError(f"docker run failed: {out.stderr.strip()} ({shlex.join(cmd)})")
        deadline = time.time() + spec.get("ready_timeout_s", 600)
        while not _http_ok(self.health_url):
            if time.time() > deadline or not self._container_running():
                logs = subprocess.run(["docker", "logs", "--tail", "40", self.container], capture_output=True, text=True)
                self.shutdown()
                raise RuntimeError(f"{self.name} did not answer {self.health_url}:\n{logs.stdout}{logs.stderr}")
            time.sleep(2)
        _watch(self._container_running, self.shutdown)

    def _container_running(self) -> bool:
        out = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", self.container], capture_output=True, text=True
        )
        return out.stdout.strip() == "true"

    def health(self) -> dict:
        if _STOPPING["on"]:
            return {"name": self.name, "stopping": True}
        if not self._container_running():
            self.shutdown()
            os._exit(1)
        return {
            "name": self.name,
            "spec": spec_hash(self.spec),
            "gpus": self.gpus,
            "port": self.port,
            "ready": _http_ok(self.health_url),
            "machine": socket.gethostname(),
        }

    def shutdown(self):
        _STOPPING["on"] = True
        subprocess.run(["docker", "stop", "-t", str(self.spec.get("stop_timeout_s", 30)), self.container], capture_output=True)
        subprocess.run(["docker", "rm", "-f", self.container], capture_output=True)


@ray.remote(max_restarts=-1, max_task_retries=0)
class ZirenCpuProver:
    """A native (CPU) prover node: the Ziren SDK's CPU prover in fleet-ziren-node."""

    def __init__(self, spec: dict):
        self.spec = spec
        self.name = spec["name"]
        log_dir = spec.get("log_dir", "/tmp/zkm-fleet")
        os.makedirs(log_dir, exist_ok=True)
        backend = spec.get("backend", "cpu")
        env = _node_env(
            spec, self.name, backend,
            {"FLEET_CPUS": spec.get("cpus", 0), "RAYON_NUM_THREADS": spec.get("cpus", 0) or "", **spec.get("env", {})},
        )
        if not env.get("RAYON_NUM_THREADS"):
            env.pop("RAYON_NUM_THREADS")
        self.node = _NodeProcess(spec["node_binary"], env, f"{log_dir}/{self.name}.node.log")
        _watch(self.node.alive, self.shutdown)

    def health(self) -> dict:
        if _STOPPING["on"]:
            return {"name": self.name, "stopping": True}
        if not self.node.alive():
            os._exit(1)
        return {"name": self.name, "spec": spec_hash(self.spec), "machine": socket.gethostname()}

    def shutdown(self):
        _STOPPING["on"] = True
        self.node.stop()
