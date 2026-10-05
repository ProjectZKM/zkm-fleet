"""python -m fleet_bridge {apply|status|down} --services services.yaml [--address 127.0.0.1:6379]
   python -m fleet_bridge render --services services.yaml --actor <name>-<i> --gpus 0[,1]

apply makes the prover actors match the services file (start, stop, replace on a
spec change). The actors are detached and Ray restarts them itself, so nothing
needs to keep running between applies. render prints the `docker run` an app replica
would get on the given cards, without Ray: to debug it, or to try it out by hand.
"""

from __future__ import annotations

import argparse
import json
import logging
import shlex

import ray
import yaml

from . import reconcile


def main():
    p = argparse.ArgumentParser(prog="fleet_bridge")
    p.add_argument("command", choices=["apply", "status", "down", "render"])
    p.add_argument("--services", default="services.yaml")
    p.add_argument("--address", default="127.0.0.1:6379", help="Ray cluster address")
    p.add_argument("--actor", help="render: replica name, e.g. comfyui-0")
    p.add_argument("--gpus", default="0", help="render: host cards, e.g. 0 or 2,3")
    a = p.parse_args()
    if a.command == "render":
        from .actors import app_command

        with open(a.services) as f:
            spec = reconcile._replica_specs(yaml.safe_load(f))[a.actor]
        if spec["kind"] != "app":
            raise SystemExit(f"{a.actor} is a {spec['kind']} service; render covers apps")
        cmd, _ = app_command(spec, a.gpus.split(","), spec["index"])
        print(shlex.join(cmd))
        return
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ray.init(address=a.address, namespace=reconcile.NAMESPACE, log_to_driver=False)

    if a.command == "apply":
        with open(a.services) as f:
            print(json.dumps(reconcile.reconcile_once(yaml.safe_load(f)), indent=2))
    elif a.command == "status":
        out = {}
        for name, actor in sorted(reconcile._running().items()):
            try:
                out[name] = ray.get(actor.health.remote(), timeout=30)
            except Exception as e:
                out[name] = {"error": str(e)}
        print(json.dumps(out, indent=2))
    elif a.command == "down":
        for name in sorted(reconcile._running()):
            reconcile.stop(name)
            print(f"stopped {name}")


if __name__ == "__main__":
    main()
