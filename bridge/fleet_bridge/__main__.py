"""python -m fleet_bridge {run|once|status|down} --services services.yaml [--address auto]"""

from __future__ import annotations

import argparse
import json
import logging

import ray
import yaml

from . import reconcile


def main():
    p = argparse.ArgumentParser(prog="fleet_bridge")
    p.add_argument("command", choices=["run", "once", "status", "down"])
    p.add_argument("--services", default="services.yaml")
    p.add_argument("--address", default="auto", help="Ray cluster address")
    p.add_argument("--period", type=float, default=15.0)
    a = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ray.init(address=a.address, namespace=reconcile.NAMESPACE, log_to_driver=False)

    def load():
        with open(a.services) as f:
            return yaml.safe_load(f)

    if a.command == "run":
        reconcile.reconcile_forever(load, a.period)
    elif a.command == "once":
        print(json.dumps(reconcile.reconcile_once(load()), indent=2))
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
