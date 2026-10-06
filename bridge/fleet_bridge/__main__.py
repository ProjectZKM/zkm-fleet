"""python -m fleet_bridge {apply [--dry-run]|status|down} --services services.yaml [--address 127.0.0.1:6379]
   python -m fleet_bridge render --services services.yaml --actor <name>-<i> --gpus 0[,1]
   python -m fleet_bridge run submit|status|cancel|list|render ...   (one-shot jobs: see runs.py)
   python -m fleet_bridge runs-api --profiles runs.yaml --runs-dir DIR --listen 127.0.0.1:9192 --admin-token-file F
                                     (the runs HTTP API in the foreground; as a service: kind runs-api)

apply makes the prover actors match the services file (start, stop, replace on a
spec change); --dry-run only says what it would do. The actors are detached and Ray
restarts them itself, so nothing needs to keep running between applies. render
prints the `docker run` an app replica would get on the given cards, without Ray:
to debug it, or to try it out by hand.
"""

from __future__ import annotations

import argparse
import json
import logging
import shlex
import sys
from dataclasses import dataclass, field

import ray
import yaml

from . import reconcile
from .model import Health, Record, ServiceKind, ServicesFile, argparser, from_args


@dataclass(kw_only=True)
class BridgeArgs(Record):
    command: str = field(metadata={"positional": True, "choices": ["apply", "status", "down", "render"]})
    services: str = "services.yaml"
    address: str = field(default="127.0.0.1:6379", metadata={"help": "Ray cluster address"})
    dry_run: bool = field(default=False, metadata={"help": "apply: only report what would change"})
    actor: str | None = field(default=None, metadata={"help": "render: replica name, e.g. comfyui-0"})
    gpus: str = field(default="0", metadata={"help": "render: host cards, e.g. 0 or 2,3"})


@dataclass(kw_only=True)
class ServeArgs(Record):
    profiles: str
    runs_dir: str
    admin_token_file: str
    listen: str = "127.0.0.1:9192"
    address: str = field(default="127.0.0.1:6379", metadata={"help": "Ray cluster address"})


def _services(path: str) -> ServicesFile:
    with open(path) as f:
        return reconcile.load_services(yaml.safe_load(f) or {})


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "run":
        from . import runs

        return runs.main(sys.argv[2:])
    if len(sys.argv) > 1 and sys.argv[1] == "runs-api":
        from . import runs_api

        s = from_args(ServeArgs, argparser(ServeArgs, argparse.ArgumentParser(prog="fleet_bridge runs-api")).parse_args(sys.argv[2:]))
        ray.init(address=s.address, namespace=reconcile.NAMESPACE, log_to_driver=False)
        return runs_api.serve(s.profiles, s.runs_dir, s.listen, s.admin_token_file)
    a = from_args(BridgeArgs, argparser(BridgeArgs, argparse.ArgumentParser(prog="fleet_bridge")).parse_args())
    if a.command == "render":
        from .actors import app_command

        spec = reconcile.replica_specs(_services(a.services))[a.actor or ""]
        if spec.kind is not ServiceKind.APP:
            raise SystemExit(f"{a.actor} is a {spec.kind.value} service; render covers apps")
        cmd, _ = app_command(spec, a.gpus.split(","))
        print(shlex.join(cmd))
        return
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ray.init(address=a.address, namespace=reconcile.NAMESPACE, log_to_driver=False)

    if a.command == "apply":
        print(json.dumps(reconcile.reconcile_once(_services(a.services), dry_run=a.dry_run).dump(), indent=2))
    elif a.command == "status":
        out = {}
        for name, actor in sorted(reconcile.running().items()):
            try:
                out[name] = reconcile.health_of(actor).dump()
            except Exception as e:
                out[name] = Health(name=name, error=str(e)).dump()
        print(json.dumps(out, indent=2))
    elif a.command == "down":
        for name in sorted(reconcile.running()):
            reconcile.stop(name)
            print(f"stopped {name}")


if __name__ == "__main__":
    main()
