"""Turns a services file into running Ray actors, and keeps them running.

    services:
      - name: ethproofs-gpu        # replica i runs as actor "<name>-<i>"
        kind: ziren-gpu            # ziren-gpu | ziren-cpu | app | hold | runs-api
        replicas: 1
        gpus: 2
        cpus: 48
        memory_gb: 256
        image: ghcr.io/projectzkm/ziren-gpu@sha256:...
        env: {VERIFY_VK: "true", ZIREN_LEAF_HOST_THREADS: "3"}

The file is a ServicesFile (model.py): the cluster settings at the top, then one
ServiceSpec per entry; each replica becomes a ReplicaSpec.

A `hold` (`gpus: N`, no image) keeps the next free cards out of reach of the
services listed after it while they start, then frees them: on one machine Ray
hands out the lowest free cards, so `goat (1) / hold (1) / ethproofs (6)` puts
GOAT on 0, EthProofs on 2-7 and leaves 1 for one-shot runs.

An `app` is any GPU container serving on a port of its own (ComfyUI, an inference
server): see GpuApp and deploy/services.example.yaml. A `runs-api` serves one-shot
runs over HTTP for `fleetctl job` (runs_api.py).

A service that cannot get its cards because one-shot runs hold them preempts
those runs (runs.py, "preemption"): they stop with grace and queue again once the
service has started. `apply --only NAME` (a service or a replica; repeat) leaves
every other actor as it is: to start one service without restarting others that
were stopped on purpose.

Each replica gets its own placement group with one STRICT_PACK bundle: its
cards, CPUs and memory all come from ONE machine (Ray never splits a replica's
cards across machines: two machines with 4 free cards each cannot host an
8-card replica). Ray picks the machine unless the service names one
(`machine: <host name or IP>`, pinned through Ray's per-node resource).
Services start one after another, each waiting for its placement group and its
health check (`pg.ready()` gives up after 15 min and the next apply retries),
so the order also decides which cards the later ones get. A `hold` steers card
choice only on a single machine (Ray hands out the lowest free cards there); it
is no affinity guarantee across machines.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.parse
import urllib.request
from typing import Any, Mapping

import ray
from ray.util.placement_group import PlacementGroup, placement_group, remove_placement_group
from ray.actor import ActorHandle
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy, PlacementGroupSchedulingStrategy

from .actors import GpuApp, ZirenCpuProver, ZirenGpuProver, clean_after, spec_hash
from .runs_api import RunsApi
from .model import ApplyReport, Bundle, ClusterConfig, Health, ReplicaSpec, ServiceKind, ServicesFile
from .rayapi import actor_node_id, free_by_node_ip

NAMESPACE = "zkm-fleet"
log = logging.getLogger("fleet_bridge")

ACTORS: dict[ServiceKind, Any] = {ServiceKind.ZIREN_GPU: ZirenGpuProver, ServiceKind.ZIREN_CPU: ZirenCpuProver,
                                  ServiceKind.APP: GpuApp, ServiceKind.RUNS_API: RunsApi}
PREEMPT_AFTER_S = 20  # a service's cards not granted this long: runs hold them
# the cluster settings a replica's hash covers, as the file names them
_SHARED_KEYS = ("scheduler", "cluster_token_file", "node_binary", "log_dir", "node_port_range", "gpu_port_range")


def service_of(replica: str) -> str:
    """ethproofs-gpu-0 -> ethproofs-gpu (a replica is named <service>-<index>)."""
    base, _, index = replica.rpartition("-")
    return base if base and index.isdigit() else replica


def load_services(data: Mapping[str, Any]) -> ServicesFile:
    return ServicesFile.parse(data)


def replica_specs(file: ServicesFile) -> dict[str, ReplicaSpec]:
    """One ReplicaSpec per replica, in file order. `config` is the replica's mapping as
    the file gives it (shared keys, the entry, its name and index), which the hash
    covers."""
    raw_services = {s["name"]: s for s in file.raw.get("services", [])}
    shared = {k: file.raw[k] for k in _SHARED_KEYS if k in file.raw}
    from .runs import is_run_actor

    out: dict[str, ReplicaSpec] = {}
    for svc in file.services:
        if svc.disabled:
            continue
        if is_run_actor(svc.name):
            raise ValueError(f"service {svc.name}: the run-/wait- prefixes are one-shot runs' actor names")
        for i in range(svc.replicas):
            name = f"{svc.name}-{i}"
            config = {**shared, **raw_services[svc.name], "name": name, "kind": svc.kind.value}
            if svc.kind is ServiceKind.APP:
                config["index"] = i
            out[name] = ReplicaSpec(name=name, index=i, service=svc, cluster=file.cluster, config=config)
    return out


def bundle(spec: ReplicaSpec) -> Bundle:
    svc = spec.service
    pin = None
    if svc.machine:
        # Ray's per-node resource: the replica can only be placed on that machine.
        from .runs import _machine_resource

        pin = _machine_resource(svc.machine)
    return Bundle(cpu=float(svc.cpus), gpu=float(svc.gpus) if svc.kind.takes_gpus else 0,
                  memory=svc.memory_gb * 2**30 if svc.memory_gb else None, pin=pin)


def running() -> dict[str, ActorHandle[Any]]:
    """The services' actors: every named actor in the namespace except one-shot runs'
    (those belong to their runs, not to the services file)."""
    from .runs import is_run_actor

    names = [a["name"] for a in ray.util.list_named_actors(all_namespaces=True)
             if a["namespace"] == NAMESPACE and not is_run_actor(a["name"])]
    out = {}
    for n in names:
        try:
            out[n] = ray.get_actor(n, namespace=NAMESPACE)
        except ValueError:
            pass
    return out


def health_of(actor: ActorHandle[Any], timeout_s: float = 30) -> Health:
    h = ray.get(actor.health.remote(), timeout=timeout_s)
    return h if isinstance(h, Health) else Health.load(h)


def _pg_name(actor: str) -> str:
    return f"pg-{actor}"


def start(spec: ReplicaSpec, ready_timeout_s: float = 900, runs_dir: str | None = None) -> ActorHandle[Any]:
    """Starts a replica once its cards and CPUs are granted. Not granted within
    PREEMPT_AFTER_S and one-shot runs (in runs_dir) hold them: those runs are
    preempted, and queued again once the replica has its grant."""
    if spec.kind is ServiceKind.APP:
        Scheduler(spec.cluster).require_namespace(spec.service.namespace)
    b = bundle(spec)
    pg = placement_group([b.to_ray()], strategy="STRICT_PACK", name=_pg_name(spec.name), lifetime="detached")
    requeue: list[str] = []
    try:
        try:
            ray.get(pg.ready(), timeout=PREEMPT_AFTER_S)
        except ray.exceptions.GetTimeoutError:
            if runs_dir:
                requeue = _preempt_runs_for(spec, b, runs_dir)
            ray.get(pg.ready(), timeout=ready_timeout_s)
    finally:
        if requeue:
            from .runs import requeue_run

            for run_id in requeue:
                try:
                    requeue_run(runs_dir or "", run_id)
                except Exception as e:
                    log.error("queueing %s again failed: %s (it is queued in its record; cancel it)", run_id, e)
    actor: ActorHandle[Any] = ACTORS[spec.kind].options(
        name=spec.name,
        namespace=NAMESPACE,
        lifetime="detached",
        max_restarts=-1,
        max_task_retries=0,
        num_cpus=b.cpu,
        num_gpus=b.gpu,
        memory=b.memory,
        scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg, placement_group_bundle_index=0),
    ).remote(spec)
    health_of(actor, ready_timeout_s)
    return actor


def _preempt_runs_for(spec: ReplicaSpec, b: Bundle, runs_dir: str) -> list[str]:
    """Preempts the runs that keep a replica's bundle from its machine; returns their ids
    (queued again by the caller, after the replica got the cards)."""
    from .runs import candidates, choose_victims, preempt

    namespace = Scheduler(spec.cluster).require_namespace(spec.service.namespace)
    pin = b.pin.split(":", 1)[1] if b.pin else None
    victims = choose_victims(b.gpu, b.cpu, free_by_node_ip(), candidates(runs_dir), time.time(), pin, namespace=namespace, priority=spec.service.priority)
    if victims is None:
        log.warning("%s: its %g card(s) and %g CPUs are not free and no run can be preempted for them; waiting",
                    spec.name, b.gpu, b.cpu)
        return []
    if not victims.runs:  # they freed up meanwhile: Ray places it
        return []
    done = []
    for run_id in victims.runs:
        log.info("preempting run %s on %s for %s", run_id, victims.node_ip, spec.name)
        try:
            preempt(runs_dir, run_id, f"service {spec.name} (priority {spec.service.priority}) needs resources",
                    requeue=True, defer_requeue=True, namespace=namespace, priority=spec.service.priority)
            done.append(run_id)
        except SystemExit as e:  # it ended meanwhile
            log.info("run %s not preempted: %s", run_id, e)
    return done


def stop(name: str) -> None:
    """Kills the actor, then removes what it owned on its machine. The kill comes first:
    an actor asked to shut down and left alive restarts itself (max_restarts=-1) when
    its watcher sees the container gone, and the restart races the replacement."""
    try:
        actor = ray.get_actor(name, namespace=NAMESPACE)
    except ValueError:
        actor = None
    if actor is not None:
        node_id = actor_node_id(actor)
        if node_id is None:
            log.warning("where %s runs is unknown; cleaning up on this machine", name)
        ray.kill(actor, no_restart=True)
        if node_id:
            try:
                task = clean_after.options(scheduling_strategy=NodeAffinitySchedulingStrategy(node_id, soft=False))
                ray.get(task.remote(name), timeout=120)
            except Exception as e:
                log.warning("cleaning up after %s: %s", name, e)
        else:  # the actor's machine is unknown: clean here, which is right on one machine
            ray.get(clean_after.remote(name), timeout=120)
    try:
        remove_placement_group(ray.util.get_placement_group(_pg_name(name)))
    except Exception:
        pass


class Scheduler:
    """Drains a prover node through the scheduler's admin API before its actor stops,
    so no proof is cut off: the scheduler sends it nothing new and its job in flight
    finishes; jobs queue meanwhile. Without the admin token (or a reachable
    scheduler) replacing falls back to stopping at once."""

    def __init__(self, cfg: ClusterConfig) -> None:
        self.url = cfg.scheduler_admin.rstrip("/") + "/admin"
        try:
            with open(cfg.admin_token_path) as f:
                self.token: str | None = f.read().strip()
        except OSError:
            self.token = None
        self.timeout_s = cfg.drain_timeout_s

    def _call(self, method: str, path: str) -> dict[str, Any]:
        req = urllib.request.Request(self.url + path, method=method, headers={"Authorization": f"Bearer {self.token}"})
        with urllib.request.urlopen(req, timeout=10) as r:
            body = r.read()
        parsed = json.loads(body) if body.strip().startswith(b"{") else {}
        return parsed if isinstance(parsed, dict) else {}

    def require_namespace(self, reference: str) -> str:
        if not self.token:
            raise ValueError("app namespace validation requires the administrator token")
        namespace = self._call("GET", "/namespaces/" + urllib.parse.quote(reference, safe=""))
        if not namespace.get("id") or namespace.get("suspended_ms") is not None:
            raise ValueError("app requires an existing, active namespace created by an administrator")
        return str(namespace["id"])

    def drain(self, name: str) -> bool:
        """True once `name` takes no job and has none running."""
        if not self.token:
            return False
        try:
            self._call("POST", f"/nodes/{name}/drain")
            deadline = time.time() + self.timeout_s
            while time.time() < deadline:
                nodes = {n["name"]: n for n in self._call("GET", "/status").get("nodes", [])}
                if name not in nodes or not nodes[name].get("busy"):
                    log.info("%s drained", name)
                    return True
                time.sleep(2)
            log.warning("%s still busy after %.0f s; replacing anyway", name, self.timeout_s)
        except Exception as e:
            log.warning("draining %s: %s; replacing without draining", name, e)
        return False

    def undrain(self, name: str) -> None:
        if self.token:
            try:
                self._call("DELETE", f"/nodes/{name}/drain")
            except Exception as e:
                log.warning("undraining %s: %s (run: fleetctl node undrain %s)", name, e, name)


def _runs_dir(file: ServicesFile) -> str | None:
    """Where one-shot runs keep their records: the runs API's, else FLEET_RUNS_DIR."""
    for svc in file.services:
        if svc.kind is ServiceKind.RUNS_API and svc.runs_dir:
            return svc.runs_dir
    return os.environ.get("FLEET_RUNS_DIR")


def reconcile_once(file: ServicesFile, dry_run: bool = False, only: list[str] | None = None) -> ApplyReport:
    """Makes the actors match the file. With dry_run nothing changes: `planned` lists
    what an apply would start, stop or replace. With `only` (service or replica names),
    every other actor is left as it is (holds still steer the cards of what starts)."""
    want = replica_specs(file)
    have = running()
    if only:
        unknown = set(only) - {s.name for s in file.services} - set(want) - {service_of(n) for n in want} - set(have) - {service_of(n) for n in have}
        if unknown:
            raise SystemExit(f"--only: no service or replica {sorted(unknown)} in the file or running")

        def picked(n: str) -> bool:
            return n in only or service_of(n) in only

        want = {n: s for n, s in want.items() if picked(n) or s.kind is ServiceKind.HOLD}
        have = {n: a for n, a in have.items() if picked(n)}
    report = ApplyReport()
    holds: list[PlacementGroup] = []
    try:
        _reconcile(want, have, report, Scheduler(file.cluster), holds, dry_run, _runs_dir(file))
    finally:
        # Holds only steer which cards the services after them get; release them.
        for pg in holds:
            remove_placement_group(pg)
    return report


def _replace(name: str, spec: ReplicaSpec, sched: Scheduler, report: ApplyReport, why: str,
             runs_dir: str | None) -> None:
    log.info("replacing %s (%s): draining first", name, why)
    drained = sched.drain(name)
    stop(name)
    try:
        start(spec, runs_dir=runs_dir)
        report.replaced.append(name)
    except Exception as e:
        log.error("restarting %s failed: %s", name, e)
        stop(name)
        report.unhealthy.append(name)
    finally:
        if drained:
            sched.undrain(name)


def _reconcile(want: dict[str, ReplicaSpec], have: dict[str, ActorHandle[Any]], report: ApplyReport,
               sched: Scheduler, holds: list[PlacementGroup], dry_run: bool, runs_dir: str | None) -> None:
    for name in sorted(set(have) - set(want)):
        if dry_run:
            report.planned.append(f"stop {name}")
            continue
        log.info("stopping %s (no longer wanted)", name)
        drained = sched.drain(name)
        stop(name)
        if drained:
            sched.undrain(name)  # the name is free for a later replica
        report.stopped.append(name)
    # In the order of the services file: on one machine Ray hands out the lowest free
    # cards, so the order decides which service gets which cards.
    names = list(want)
    for i, (name, spec) in enumerate(want.items()):
        if spec.kind is ServiceKind.HOLD:
            # A hold takes the next free cards while the services after it start (Ray hands
            # out the lowest free cards), so they land past it; the cards are freed at the
            # end, for one-shot runs. Only when one of those services has to start.
            later = [n for n in names[i + 1:] if want[n].kind is not ServiceKind.HOLD and n not in have]
            if later and not dry_run:
                pg = placement_group([bundle(spec).to_ray()], strategy="STRICT_PACK", name=f"hold-{name}")
                try:
                    ray.get(pg.ready(), timeout=60)
                    holds.append(pg)
                    report.held.append(name)
                except Exception as e:
                    log.warning("hold %s not placed (%s); the services after it take the lowest cards", name, e)
                    remove_placement_group(pg)
            continue
        if name not in have:
            if dry_run:
                report.planned.append(f"start {name}")
                continue
            log.info("starting %s", name)
            try:
                start(spec, runs_dir=runs_dir)
                report.started.append(name)
            except Exception as e:
                log.error("starting %s failed: %s", name, e)
                stop(name)
                report.unhealthy.append(name)
            continue
        try:
            h = health_of(have[name])
        except Exception as e:
            log.warning("%s unhealthy (Ray restarts it): %s", name, e)
            report.unhealthy.append(name)
            continue
        if h.spec != spec_hash(spec):
            if dry_run:
                report.planned.append(f"replace {name} (spec changed: {h.spec} -> {spec_hash(spec)})")
            else:
                _replace(name, spec, sched, report, "spec changed", runs_dir)
        else:
            report.healthy.append(name)
