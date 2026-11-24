"""Local app inventory, placement and actor lifecycle; every replica runs through the shim.

fleet-node invokes these operations for commands admitted by the Rust scheduler.
Namespace validation, quotas, draining and preemption stay in the control plane.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any, Mapping, Callable

import ray
from ray._raylet import PlacementGroupID
from ray.util.placement_group import PlacementGroup, placement_group, remove_placement_group
from ray.actor import ActorHandle
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

from . import shim
from .jobs import NAMESPACE, is_job_actor
from .model import ResourceRequest, ReplicaHealth, AppReplicaSpec, AppRuntime, AppsConfig

# The cluster settings a replica's hash covers, as the file names them. `node_binary` and
# `gpu_port_range` are among them because running replicas were hashed with them.
_SHARED_KEYS = ("scheduler", "scheduler_ca_file", "cluster_token_file", "node_binary", "log_dir", "gpu_port_range")


def load_apps(data: Mapping[str, Any], *, validate_launch: bool = True) -> AppsConfig:
    return AppsConfig.parse(data, validate_launch=validate_launch)


def replica_specs(file: AppsConfig) -> dict[str, AppReplicaSpec]:
    """One AppReplicaSpec per replica, in file order. `config` is the replica's mapping as
    the file gives it (shared keys, the entry, its name and index), which the hash
    covers."""
    raw_apps = {a["name"]: a for a in file.raw.get("apps", [])}
    shared = {k: file.raw[k] for k in _SHARED_KEYS if k in file.raw}

    out: dict[str, AppReplicaSpec] = {}
    for app in file.apps:
        if app.disabled:
            continue
        if is_job_actor(app.name):
            raise ValueError(f"app {app.name}: the job- prefix is reserved for batch job actors")
        cluster = file.cluster
        for i in range(app.replicas):
            name = f"{app.name}-{i}"
            config = {**shared, **raw_apps[app.name], "name": name, "kind": app.kind.value}
            if app.kind is AppRuntime.APP:
                config["index"] = i
            if app.kind is not AppRuntime.APP:
                # A prover's image carries its own executable: its hash leaves `node_binary` out.
                config.pop("node_binary", None)
            config.update(shim_binary=file.cluster.shim_binary, shim_state_dir=file.cluster.shim_state_dir)
            if app.services:
                config["shim_network"] = file.cluster.shim_network.dump() if file.cluster.shim_network else None
            out[name] = AppReplicaSpec(name=name, index=i, app=app, cluster=cluster, config=config)
    return out


def bundle(spec: AppReplicaSpec) -> ResourceRequest:
    app = spec.app
    pin = None
    if app.machine:
        # Ray's per-node resource: the replica can only be placed on that machine.
        from .jobs import _machine_resource

        pin = _machine_resource(app.machine)
    return ResourceRequest(cpu=float(app.cpus), gpu=float(app.gpus) if app.kind.takes_gpus else 0,
                  memory=app.memory_gb * 2**30 if app.memory_gb else None, pin=pin)


def running() -> dict[str, ActorHandle[Any]]:
    """The apps' actors: every named actor in the namespace except batch jobs'
    (those belong to their jobs, not to the apps file)."""
    names = [a["name"] for a in ray.util.list_named_actors(all_namespaces=True)
             if a["namespace"] == NAMESPACE and not is_job_actor(a["name"])]
    out = {}
    for n in names:
        try:
            out[n] = ray.get_actor(n, namespace=NAMESPACE)
        except ValueError:
            pass
    return out


def health_of(actor: ActorHandle[Any], timeout_s: float = 30) -> ReplicaHealth:
    h = ray.get(actor.health.remote(), timeout=timeout_s)
    return h if isinstance(h, ReplicaHealth) else ReplicaHealth.load(h)


def _pg_name(actor: str) -> str:
    # Ray keeps named placement-group records after removal. Reusing the same
    # name on a later app restart is rejected by the GCS even when the old
    # group is REMOVED, so each actor start needs a fresh name.
    return f"pg-{actor}-{uuid.uuid4().hex}"


def start(spec: AppReplicaSpec, ready_timeout_s: float = 900) -> ActorHandle[Any]:
    """Start an admitted replica using this machine's Ray-granted resources."""
    b = bundle(spec)
    pg = placement_group([b.to_ray()], strategy="STRICT_PACK", name=_pg_name(spec.name), lifetime="detached")
    actor_submitted = False
    try:
        ray.get(pg.ready(), timeout=ready_timeout_s)
        factory = shim.ProvisionedApp if spec.app.native_access else shim.ShimApp
        # A distinct worker environment loads the adapter in a worker of its own,
        # without restarting Ray or any detached app during bridge upgrades.
        adapter = "openshell-allocation-v1" if spec.app.native_access else "runtime-parity-v1" if spec.cluster.shim_daemon else "runtime-parity-recovery-v1"
        options = factory.options(
            name=spec.name,
            namespace=NAMESPACE,
            lifetime="detached",
            max_restarts=-1,
            max_task_retries=0,
            num_cpus=b.cpu,
            num_gpus=b.gpu,
            memory=b.memory,
            scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg, placement_group_bundle_index=0),
            runtime_env={"env_vars": {"FLEET_SHIM_ADAPTER": adapter}},
        )
        actor: ActorHandle[Any] = options.remote(spec)
        # Only now can a process own the group: `remote()` itself may raise,
        # for example when the actor name is already taken.
        actor_submitted = True
        health_of(actor, ready_timeout_s)
    except BaseException:
        if not actor_submitted:
            # No process can own this group yet. Retaining it leaks resources
            # every time placement times out before actor submission.
            remove_placement_group(pg)
        # A submitted replica retains its reservation until its durable
        # allocation has confirmed cleanup.
        raise
    return actor


def stop(name: str, *, cleanup: Callable[[], None] | None = None) -> None:
    """Kills the actor, runs `cleanup` (what removes the replica's allocation), then
    removes its placement groups. The kill comes first: a live actor restarts
    (max_restarts=-1), and the restart races the cleanup."""
    try:
        actor = ray.get_actor(name, namespace=NAMESPACE)
    except ValueError:
        actor = None
    if actor is not None:
        ray.kill(actor, no_restart=True)
    if cleanup is not None:
        cleanup()
    # Find all generations because an actor can have been restarted several
    # times. Only remove live groups; Ray retains removed group names forever.
    prefix = f"pg-{name}-"
    for pg_id, info in ray.util.placement_group_table().items():
        group_name = info.get("name", "")
        if group_name.startswith(prefix) and info.get("state") != "REMOVED":
            try:
                # By id: a name lookup only sees groups of the caller's Ray namespace.
                remove_placement_group(PlacementGroup(PlacementGroupID.from_hex(pg_id)))
            except Exception:
                logging.getLogger(__name__).exception("could not remove placement group %s", group_name)
