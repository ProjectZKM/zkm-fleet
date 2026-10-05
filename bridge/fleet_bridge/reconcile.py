"""Turns a services file into running Ray actors, and keeps them running.

    services:
      - name: ethproofs-gpu        # replica i runs as actor "<name>-<i>"
        kind: ziren-gpu            # ziren-gpu | ziren-cpu
        replicas: 1
        gpus: 2
        cpus: 48
        memory_gb: 256
        image: ghcr.io/projectzkm/ziren-gpu@sha256:...
        env: {VERIFY_VK: "true", ZIREN_LEAF_HOST_THREADS: "3"}

Each replica gets its own placement group with one STRICT_PACK bundle, so all
of a GPU server's cards land on one machine.
"""

from __future__ import annotations

import logging
import time

import ray
from ray.util.placement_group import placement_group, remove_placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

from .actors import ZirenCpuProver, ZirenGpuProver, spec_hash

NAMESPACE = "zkm-fleet"
log = logging.getLogger("fleet_bridge")

KINDS = {"ziren-gpu": ZirenGpuProver, "ziren-cpu": ZirenCpuProver}


def _replica_specs(cfg: dict) -> dict[str, dict]:
    common = {
        k: cfg[k]
        for k in ("scheduler", "cluster_token_file", "node_binary", "log_dir", "node_port_range", "gpu_port_range")
        if k in cfg
    }
    out = {}
    for svc in cfg.get("services", []):
        kind = svc.get("kind", "ziren-gpu")
        if kind not in KINDS:
            raise ValueError(f"service {svc['name']}: unknown kind {kind}")
        for i in range(int(svc.get("replicas", 1))):
            name = f"{svc['name']}-{i}"
            out[name] = {**common, **svc, "name": name, "kind": kind}
    return out


def _bundle(spec: dict) -> dict:
    b = {"CPU": float(spec.get("cpus", 1))}
    if spec["kind"] == "ziren-gpu":
        b["GPU"] = float(spec["gpus"])
    if spec.get("memory_gb"):
        b["memory"] = float(spec["memory_gb"]) * 2**30
    return b


def _running() -> dict[str, ray.actor.ActorHandle]:
    names = [a["name"] for a in ray.util.list_named_actors(all_namespaces=True) if a["namespace"] == NAMESPACE]
    out = {}
    for n in names:
        try:
            out[n] = ray.get_actor(n, namespace=NAMESPACE)
        except ValueError:
            pass
    return out


def _pg_name(actor: str) -> str:
    return f"pg-{actor}"


def start(spec: dict, ready_timeout_s: float = 900) -> ray.actor.ActorHandle:
    pg = placement_group([_bundle(spec)], strategy="STRICT_PACK", name=_pg_name(spec["name"]), lifetime="detached")
    ray.get(pg.ready(), timeout=ready_timeout_s)
    cls = KINDS[spec["kind"]]
    b = _bundle(spec)
    actor = cls.options(
        name=spec["name"],
        namespace=NAMESPACE,
        lifetime="detached",
        num_cpus=b["CPU"],
        num_gpus=b.get("GPU", 0),
        memory=b.get("memory"),
        scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg, placement_group_bundle_index=0),
    ).remote(spec)
    ray.get(actor.health.remote(), timeout=ready_timeout_s)
    return actor


def stop(name: str):
    try:
        actor = ray.get_actor(name, namespace=NAMESPACE)
        try:
            ray.get(actor.shutdown.remote(), timeout=60)
        except Exception as e:  # the actor may already be gone
            log.warning("shutdown of %s: %s", name, e)
        ray.kill(actor, no_restart=True)
    except ValueError:
        pass
    try:
        remove_placement_group(ray.util.get_placement_group(_pg_name(name)))
    except Exception:
        pass


def reconcile_once(cfg: dict) -> dict:
    want = _replica_specs(cfg)
    have = _running()
    report = {"started": [], "stopped": [], "replaced": [], "healthy": [], "unhealthy": []}
    for name in sorted(set(have) - set(want)):
        log.info("stopping %s (no longer wanted)", name)
        stop(name)
        report["stopped"].append(name)
    for name, spec in sorted(want.items()):
        if name not in have:
            log.info("starting %s", name)
            try:
                start(spec)
                report["started"].append(name)
            except Exception as e:
                log.error("starting %s failed: %s", name, e)
                stop(name)
                report["unhealthy"].append(name)
            continue
        try:
            h = ray.get(have[name].health.remote(), timeout=30)
        except Exception as e:
            log.warning("%s unhealthy (Ray restarts it): %s", name, e)
            report["unhealthy"].append(name)
            continue
        if h.get("spec") != spec_hash(spec):
            log.info("replacing %s (spec changed)", name)
            stop(name)
            try:
                start(spec)
                report["replaced"].append(name)
            except Exception as e:
                log.error("restarting %s failed: %s", name, e)
                stop(name)
                report["unhealthy"].append(name)
        else:
            report["healthy"].append(name)
    return report


def reconcile_forever(load_cfg, period_s: float = 15):
    while True:
        try:
            r = reconcile_once(load_cfg())
            if r["started"] or r["stopped"] or r["replaced"] or r["unhealthy"]:
                log.info("reconcile: %s", r)
        except Exception as e:
            log.exception("reconcile failed: %s", e)
        time.sleep(period_s)
