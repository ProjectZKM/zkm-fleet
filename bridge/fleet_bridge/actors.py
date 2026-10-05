"""Ray actors that run prover nodes.

A ZirenGpuProver owns one GPU server: the container on exactly the cards Ray
granted the actor (all on one machine: the server's per-card workers share
memory and keep state on the cards), plus a fleet-node process that proves
through the Ziren SDK against it and registers with the scheduler.

If either process dies, the actor exits and Ray restarts it (max_restarts=-1).
"""

from __future__ import annotations

import os
import shlex
import socket
import subprocess
import time
import urllib.request

import ray


def _free_port(lo: int, hi: int) -> int:
    for port in range(lo, hi + 1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("0.0.0.0", port))
                return port
            except OSError:
                continue
    raise RuntimeError(f"no free port in {lo}-{hi}")


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


def _node_env(spec: dict, name: str, backend: str, node_port: int, extra: dict) -> dict:
    env = dict(os.environ)
    env.update(
        {
            "FLEET_NODE_NAME": name,
            "FLEET_BACKEND": backend,
            "FLEET_LISTEN": f"0.0.0.0:{node_port}",
            "FLEET_ADVERTISE": f"http://{ray.util.get_node_ip_address()}:{node_port}",
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


class _NodeProcess:
    def __init__(self, binary: str, env: dict, log_path: str):
        self.log = open(log_path, "ab")
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
        lo, hi = spec.get("gpu_port_range", [30100, 30199])
        self.gpu_port = _free_port(lo, hi)
        lo, hi = spec.get("node_port_range", [31000, 31099])
        self.node_port = _free_port(lo, hi)
        log_dir = spec.get("log_dir", "/tmp/zkm-fleet")
        os.makedirs(log_dir, exist_ok=True)

        subprocess.run(["docker", "rm", "-f", self.container], capture_output=True)
        env_args = []
        for k, v in {"ZIREN_SERVER_PORT": "3000", **spec.get("env", {})}.items():
            env_args += ["-e", f"{k}={v}"]
        for k in ("CUDA_VISIBLE_DEVICES", "ZKM_GPU_DEVICES"):
            env_args += ["-e", f"{k}={','.join(str(i) for i in range(len(gpus)))}"]
        cmd = [
            "docker", "run", "-d", "--name", self.container,
            "--runtime=nvidia", "--gpus", f'"device={",".join(gpus)}"',
            f"--shm-size={spec.get('shm_gb', 64)}g",
            "-p", f"127.0.0.1:{self.gpu_port}:3000",
            "--label", "zkm-fleet=gpu-server", "--label", f"zkm-fleet.node={self.name}",
            *env_args,
        ]
        for v in spec.get("volumes", []):
            cmd += ["-v", v]
        cmd.append(spec["image"])
        out = subprocess.run(cmd, capture_output=True, text=True)
        if out.returncode != 0:
            raise RuntimeError(f"docker run failed: {out.stderr.strip()} ({shlex.join(cmd)})")

        deadline = time.time() + spec.get("ready_timeout_s", 600)
        while not _gpu_ready(self.gpu_port):
            if time.time() > deadline or not self._container_running():
                logs = subprocess.run(["docker", "logs", "--tail", "40", self.container], capture_output=True, text=True)
                raise RuntimeError(f"GPU server did not become ready:\n{logs.stdout}{logs.stderr}")
            time.sleep(2)

        env = _node_env(
            spec, self.name, "cuda", self.node_port,
            {
                "FLEET_GPU_ENDPOINT": f"http://127.0.0.1:{self.gpu_port}/twirp/",
                "FLEET_GPUS": len(gpus),
                "FLEET_CPUS": spec.get("cpus", 0),
                "FLEET_IMAGE": spec["image"],
            },
        )
        self.node = _NodeProcess(spec["node_binary"], env, f"{log_dir}/{self.name}.node.log")

    def _container_running(self) -> bool:
        out = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", self.container], capture_output=True, text=True
        )
        return out.stdout.strip() == "true"

    def health(self) -> dict:
        container = self._container_running()
        node = self.node.alive()
        if not (container and node):
            self.shutdown()
            os._exit(1)  # Ray restarts the actor
        return {
            "name": self.name,
            "gpus": self.gpus,
            "gpu_port": self.gpu_port,
            "node_port": self.node_port,
            "gpu_ready": _gpu_ready(self.gpu_port),
            "machine": socket.gethostname(),
        }

    def shutdown(self):
        self.node.stop()
        subprocess.run(["docker", "rm", "-f", self.container], capture_output=True)


@ray.remote(max_restarts=-1, max_task_retries=0)
class ZirenCpuProver:
    """A native (CPU) prover node: the Ziren SDK's CPU prover in fleet-node."""

    def __init__(self, spec: dict):
        self.spec = spec
        self.name = spec["name"]
        lo, hi = spec.get("node_port_range", [31000, 31099])
        self.node_port = _free_port(lo, hi)
        log_dir = spec.get("log_dir", "/tmp/zkm-fleet")
        os.makedirs(log_dir, exist_ok=True)
        backend = spec.get("backend", "cpu")
        env = _node_env(
            spec, self.name, backend, self.node_port,
            {"FLEET_CPUS": spec.get("cpus", 0), "RAYON_NUM_THREADS": spec.get("cpus", 0) or ""},
        )
        if not env.get("RAYON_NUM_THREADS"):
            env.pop("RAYON_NUM_THREADS")
        self.node = _NodeProcess(spec["node_binary"], env, f"{log_dir}/{self.name}.node.log")

    def health(self) -> dict:
        if not self.node.alive():
            os._exit(1)
        return {"name": self.name, "node_port": self.node_port, "machine": socket.gethostname()}

    def shutdown(self):
        self.node.stop()
