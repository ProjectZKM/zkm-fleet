"""python -m fleet_bridge {apply|status|down} --services services.yaml [--address 127.0.0.1:6379]

apply makes the prover actors match the services file (start, stop, replace on a
spec change). The actors are detached and Ray restarts them itself, so nothing
needs to keep running between applies.
"""

from __future__ import annotations

import argparse
import json
import logging

import ray
import yaml

from . import reconcile


def main():
    p = argparse.ArgumentParser(prog="fleet_bridge")
    p.add_argument("command", choices=["apply", "status", "down"])
    p.add_argument("--services", default="services.yaml")
    p.add_argument("--address", default="127.0.0.1:6379", help="Ray cluster address")
    a = p.parse_args()
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
