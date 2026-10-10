"""Typed faces of the few Ray calls the bridge makes whose stubs are untyped."""

from __future__ import annotations

from typing import Any

import ray


def node_ip() -> str:
    """This machine's address as Ray knows it."""
    return str(ray.util.get_node_ip_address())  # type: ignore[no-untyped-call]


def nodes() -> list[dict[str, Any]]:
    """Every Ray node's record (NodeManagerAddress, NodeManagerHostname, Alive, ...)."""
    return [dict(n) for n in ray.nodes()]  # type: ignore[no-untyped-call]


def alive_nodes() -> list[dict[str, Any]]:
    return [n for n in nodes() if n.get("Alive")]


def actor_node_id(actor: Any) -> str | None:
    """The node an actor runs on, from the GCS (works with the dashboard API off)."""
    try:
        info = ray._private.state.actors(actor_id=actor._actor_id.hex())
        return str(info["Address"]["NodeID"])
    except Exception:
        return None
