"""Turns a services file into running Ray actors, and keeps them running.

    services:
      - name: ethproofs-gpu        # replica i runs as actor "<name>-<i>"
        kind: ziren-gpu            # ziren-gpu | ziren-cpu | app | hold
        replicas: 1
        gpus: 2
        cpus: 48
        memory_gb: 256
        image: ghcr.io/projectzkm/ziren-gpu@sha256:...
        env: {VERIFY_VK: "true", ZIREN_LEAF_HOST_THREADS: "3"}

A `hold` (`gpus: N`, no image) keeps the next free cards out of reach of the
services listed after it while they start, then frees them: on one machine Ray
hands out the lowest free cards, so `goat (1) / hold (1) / ethproofs (6)` puts
GOAT on 0, EthProofs on 2-7 and leaves 1 for one-shot runs.

An `app` is any GPU container serving on a port of its own (ComfyUI, an inference
server): see GpuApp and deploy/services.example.yaml.

Each replica gets its own placement group with one STRICT_PACK bundle, so all
of a GPU server's cards land on one machine.
"""

from __future__ import annotations

import logging

import ray
from ray.util.placement_group import placement_group, remove_placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

from .actors import GpuApp, ZirenCpuProver, ZirenGpuProver, spec_hash

NAMESPACE = "zkm-fleet"
log = logging.getLogger("fleet_bridge")

KINDS = {"ziren-gpu": ZirenGpuProver, "ziren-cpu": ZirenCpuProver, "app": GpuApp}


def _replica_specs(cfg: dict) -> dict[str, dict]:
    common = {
        k: cfg[k]
        for k in ("scheduler", "cluster_token_file", "node_binary", "log_dir", "node_port_range", "gpu_port_range")
        if k in cfg
    }
    out = {}
    for svc in cfg.get("services", []):
        kind = svc.get("kind", "ziren-gpu")
        if kind not in KINDS and kind != "hold":
            raise ValueError(f"service {svc['name']}: unknown kind {kind}")
        for i in range(int(svc.get("replicas", 1))):
            name = f"{svc['name']}-{i}"
            out[name] = {**common, **svc, "name": name, "kind": kind}
            if kind == "app":
                out[name]["index"] = i
    return out


def _bundle(spec: dict) -> dict:
    b = {"CPU": float(spec.get("cpus", 1))}
    if spec["kind"] in ("ziren-gpu", "app", "hold"):
        b["GPU"] = float(spec.get("gpus", 1))
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
    report = {"started": [], "stopped": [], "replaced": [], "healthy": [], "unhealthy": [], "held": []}
    try:
        _reconcile(want, have, report)
    finally:
        # Holds only steer which cards the services after them get; release them.
        for pg in report.pop("_holds", []):
            remove_placement_group(pg)
    return report


def _reconcile(want: dict, have: dict, report: dict):
    for name in sorted(set(have) - set(want)):
        log.info("stopping %s (no longer wanted)", name)
        stop(name)
        report["stopped"].append(name)
    # In the order of the services file: on one machine Ray hands out the lowest free
    # cards, so the order decides which service gets which cards.
    names = list(want)
    for i, (name, spec) in enumerate(want.items()):
        if spec["kind"] == "hold":
            # A hold takes the next free cards while the services after it start (Ray hands
            # out the lowest free cards), so they land past it; the cards are freed at the
            # end, for one-shot runs. Only when one of those services has to start.
            later = [n for n in names[i + 1:] if want[n]["kind"] != "hold" and n not in have]
            if later:
                pg = placement_group([_bundle(spec)], strategy="STRICT_PACK", name=f"hold-{name}")
                try:
                    ray.get(pg.ready(), timeout=60)
                    report.setdefault("_holds", []).append(pg)
                    report["held"].append(name)
                except Exception as e:
                    log.warning("hold %s not placed (%s); the services after it take the lowest cards", name, e)
                    remove_placement_group(pg)
            continue
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
