"""Typed faces of the few Ray calls the bridge makes whose stubs are untyped."""

from __future__ import annotations

from typing import Any

import ray
from ray.util.placement_group import PlacementGroup


def node_ip() -> str:
    """This machine's address as Ray knows it."""
    return str(ray.util.get_node_ip_address())  # type: ignore[no-untyped-call]


def nodes() -> list[dict[str, Any]]:
    """Every Ray node's record (NodeManagerAddress, NodeManagerHostname, Alive, ...)."""
    return [dict(n) for n in ray.nodes()]  # type: ignore[no-untyped-call]


def alive_nodes() -> list[dict[str, Any]]:
    return [n for n in nodes() if n.get("Alive")]


def current_placement_group() -> PlacementGroup:
    pg = ray.util.get_current_placement_group()
    if pg is None:
        raise RuntimeError("not inside a placement group")
    return pg


def actor_node_id(actor: Any) -> str | None:
    """The node an actor runs on, from the GCS (works with the dashboard API off)."""
    try:
        info = ray._private.state.actors(actor_id=actor._actor_id.hex())
        return str(info["Address"]["NodeID"])
    except Exception:
        return None


def free_by_node_ip() -> dict[str, tuple[float, float]]:
    """Each live machine's unclaimed GPUs and CPUs (node IP -> (GPUs, CPUs)), from the GCS."""
    per_node = ray._private.state.available_resources_per_node()  # type: ignore[no-untyped-call]
    out = {}
    for n in alive_nodes():
        free = per_node.get(n["NodeID"], {})
        out[str(n["NodeManagerAddress"])] = (float(free.get("GPU", 0)), float(free.get("CPU", 0)))
    return out
