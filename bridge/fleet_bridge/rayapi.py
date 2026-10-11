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

def resources() -> tuple[dict[str, float], dict[str, float]]:
    """Total and currently available CPU/GPU/resource units on the local runtime."""
    return dict(ray.cluster_resources()), dict(ray.available_resources())  # type: ignore[no-untyped-call]
