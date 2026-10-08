"""Which runs a service preempts, what a preempted run keeps, and its record's rules."""

from __future__ import annotations

import time
from typing import Any

from fleet_bridge.model import Attempt, Cause, Profile, RunSpec, RunState, RunStatus
from fleet_bridge.runs import Candidate, RunDir, budgets, choose_victims


def cand(id: str, ip: str = "a", gpus: float = 1, cpus: float = 8, started: float | None = 0.0,
         preemptible: bool = True, min_runtime_s: int = 300, namespace: str = "n", priority: int = 0) -> Candidate:
    return Candidate(id=id, namespace=namespace, priority=priority, node_ip=ip, gpus=gpus, cpus=cpus, started_at=started, preemptible=preemptible,
                     min_runtime_s=min_runtime_s)


NOW = 10_000.0


def test_no_preemption_when_the_bundle_fits() -> None:
    v = choose_victims(2, 8, {"a": (1, 40), "b": (2, 40)}, [cand("r1")], NOW, namespace="n", priority=1)
    assert v is not None and (v.node_ip, v.runs) == ("b", [])


def test_the_newest_runs_go_first_and_only_as_many_as_needed() -> None:
    runs = [cand("old", started=NOW - 5000), cand("new", started=NOW - 400), cand("mid", started=NOW - 1000)]
    v = choose_victims(2, 4, {"a": (0, 40)}, runs, NOW, namespace="n", priority=1)
    assert v is not None and v.runs == ["new", "mid"]


def test_a_run_waiting_for_card_memory_goes_before_any_started_one() -> None:
    runs = [cand("new", started=NOW - 400), cand("waiting", started=None)]
    v = choose_victims(1, 4, {"a": (0, 40)}, runs, NOW, namespace="n", priority=1)
    assert v is not None and v.runs == ["waiting"]


def test_young_and_unpreemptible_runs_are_left_alone() -> None:
    runs = [cand("young", started=NOW - 10), cand("pinned", preemptible=False)]
    assert choose_victims(1, 4, {"a": (0, 40)}, runs, NOW, namespace="n", priority=1) is None, "the service waits, as before"


def test_cpus_count_too() -> None:
    runs = [cand("gpu-run", gpus=1, cpus=2), cand("cpu-run", gpus=0, cpus=30, started=NOW - 999)]
    v = choose_victims(1, 32, {"a": (0, 4)}, runs, NOW, namespace="n", priority=1)
    assert v is not None and set(v.runs) == {"gpu-run", "cpu-run"}


def test_the_machine_with_the_fewest_victims_and_the_pin() -> None:
    runs = [cand("a1", ip="a"), cand("a2", ip="a"), cand("b1", ip="b", gpus=2)]
    v = choose_victims(2, 4, {"a": (0, 40), "b": (0, 40)}, runs, NOW, namespace="n", priority=1)
    assert v is not None and (v.node_ip, v.runs) == ("b", ["b1"])
    v = choose_victims(2, 4, {"a": (0, 40), "b": (0, 40)}, runs, NOW, pin="a", namespace="n", priority=1)
    assert v is not None and (v.node_ip, sorted(v.runs)) == ("a", ["a1", "a2"])


def spec(**kw: Any) -> RunSpec:
    base: dict[str, Any] = dict(id="r", profile_name="p", profile=Profile(image="i"), command=["x"], gpus=1, cpus=1,
                                out="/tmp/out")
    base.update(kw)
    return RunSpec(**base)


def test_a_preempted_attempt_spends_no_retry() -> None:
    s = spec(retry=2, restart_on_oom=1)
    attempts = [Attempt(exit_code=1, cause=Cause.GPU_OOM), Attempt(exit_code=None, cause=Cause.PREEMPTED),
                Attempt(exit_code=1, cause=Cause.EXIT_CODE), Attempt(exit_code=1, cause=Cause.GPU_OOM)]
    assert budgets(s, attempts) == (0, 0), "oom: restart then retry; exit: retry; preempted: nothing"
    assert budgets(s, [Attempt(exit_code=None, cause=Cause.PREEMPTED)] * 5) == (2, 1)


def test_a_run_being_preempted_takes_only_the_preemptions_writes_and_a_cancel(tmp_path: Any) -> None:
    run = RunDir(str(tmp_path), "train-1", must_exist=False)
    import os

    os.makedirs(run.path)
    RunStatus(id="train-1", state=RunState.RUNNING, submitted_at=time.time(), gpus_requested=1, cpus=1).write(run.status_path)
    assert run.update(state=RunState.PREEMPTED).state is RunState.PREEMPTED
    assert run.update(state=RunState.RETRYING).state is RunState.PREEMPTED, "the stopped actor's late report is dropped"
    assert run.update(state=RunState.FAILED, error="x").state is RunState.PREEMPTED
    assert run.update(force=True, state=RunState.QUEUED, preemptions=1).state is RunState.QUEUED
    run.update(state=RunState.PREEMPTED)
    assert run.update(state=RunState.CANCELLED).state is RunState.CANCELLED, "a cancel still wins"


def test_namespace_and_priority_boundaries():
    pool = [cand("other", namespace="other", priority=-10), cand("equal", priority=5), cand("higher", priority=6)]
    assert choose_victims(1, 1, {"a": (0, 10)}, pool, NOW, namespace="n", priority=5) is None
    pool += [cand("low", priority=-2, started=NOW-5000), cand("medium", priority=1, started=NOW-400)]
    assert choose_victims(1, 1, {"a": (0, 10)}, pool, NOW, namespace="n", priority=5).runs == ["low"]


def test_complete_multinode_plan_and_shared_quota():
    from fleet_bridge.runs import plan_preemption
    request = spec(namespace="n", nodes=2, profile=Profile(image="i", priority=10))
    pool = [cand("distributed", ip="a"), cand("distributed", ip="b"), cand("protected", ip="a", priority=10)]
    assert plan_preemption(request, {"a": (0, 8), "b": (0, 8)}, pool, NOW,
        lambda victims: "distributed" in victims) == ["distributed"]
    # Never evict part of a plan that still cannot fit, physically or within quota.
    assert plan_preemption(request, {"a": (0, 8)}, pool, NOW, lambda _: True) is None
    assert plan_preemption(request, {"a": (0, 8), "b": (0, 8)}, pool, NOW, lambda _: False) is None


def test_quota_only_preemption_stays_in_namespace():
    from fleet_bridge.runs import plan_preemption
    request = spec(namespace="n", profile=Profile(image="i", priority=3))
    pool = [cand("low", priority=0), cand("other", namespace="other", priority=-1)]
    assert plan_preemption(request, {"a": (1, 8)}, pool, NOW, lambda victims: "low" in victims) == ["low"]
    assert plan_preemption(request, {"a": (1, 8)}, pool, NOW, lambda victims: "other" in victims) is None


def test_automatic_preempt_rechecks_namespace_priority_and_protection(tmp_path, monkeypatch):
    import os
    import pytest
    from fleet_bridge import runs
    run = RunDir(str(tmp_path), "protected", must_exist=False)
    os.makedirs(run.out)
    spec(namespace="n", id=run.id, out=run.out, profile=Profile(image="i", priority=3, preemptible=False)).write(run.spec_path)
    RunStatus(id=run.id, state=RunState.RUNNING, submitted_at=1, started_at=1, gpus_requested=1, cpus=1).write(run.status_path)
    monkeypatch.setattr(runs, "_alive", lambda _: True)
    for namespace, priority in (("other", 10), ("n", 3), ("n", 10)):
        with pytest.raises(runs.Conflict):
            runs.preempt(str(tmp_path), run.id, "automatic", namespace=namespace, priority=priority)
        assert run.status().state is RunState.RUNNING


def test_tag_cap_does_not_evict_runs_that_cannot_help():
    from fleet_bridge.runs import plan_preemption
    request = spec(namespace="n", profile=Profile(image="i", priority=10))
    pool = [cand("irrelevant-tag", priority=-10), cand("matching-tag", priority=0)]
    assert plan_preemption(request, {"a": (1, 8)}, pool, NOW,
                           lambda victims: "matching-tag" in victims) == ["matching-tag"]
