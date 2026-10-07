"""Which runs a service preempts, what a preempted run keeps, and its record's rules."""

from __future__ import annotations

import time
from typing import Any

from fleet_bridge.model import Attempt, Cause, Profile, RunSpec, RunState, RunStatus
from fleet_bridge.runs import Candidate, RunDir, budgets, choose_victims


def cand(id: str, ip: str = "a", gpus: float = 1, cpus: float = 8, started: float | None = 0.0,
         preemptible: bool = True, min_runtime_s: int = 300) -> Candidate:
    return Candidate(id=id, node_ip=ip, gpus=gpus, cpus=cpus, started_at=started, preemptible=preemptible,
                     min_runtime_s=min_runtime_s)


NOW = 10_000.0


def test_no_preemption_when_the_bundle_fits() -> None:
    v = choose_victims(2, 8, {"a": (1, 40), "b": (2, 40)}, [cand("r1")], NOW)
    assert v is not None and (v.node_ip, v.runs) == ("b", [])


def test_the_newest_runs_go_first_and_only_as_many_as_needed() -> None:
    runs = [cand("old", started=NOW - 5000), cand("new", started=NOW - 400), cand("mid", started=NOW - 1000)]
    v = choose_victims(2, 4, {"a": (0, 40)}, runs, NOW)
    assert v is not None and v.runs == ["new", "mid"]


def test_a_run_waiting_for_card_memory_goes_before_any_started_one() -> None:
    runs = [cand("new", started=NOW - 400), cand("waiting", started=None)]
    v = choose_victims(1, 4, {"a": (0, 40)}, runs, NOW)
    assert v is not None and v.runs == ["waiting"]


def test_young_and_unpreemptible_runs_are_left_alone() -> None:
    runs = [cand("young", started=NOW - 10), cand("pinned", preemptible=False)]
    assert choose_victims(1, 4, {"a": (0, 40)}, runs, NOW) is None, "the service waits, as before"


def test_cpus_count_too() -> None:
    runs = [cand("gpu-run", gpus=1, cpus=2), cand("cpu-run", gpus=0, cpus=30, started=NOW - 999)]
    v = choose_victims(1, 32, {"a": (0, 4)}, runs, NOW)
    assert v is not None and set(v.runs) == {"gpu-run", "cpu-run"}


def test_the_machine_with_the_fewest_victims_and_the_pin() -> None:
    runs = [cand("a1", ip="a"), cand("a2", ip="a"), cand("b1", ip="b", gpus=2)]
    v = choose_victims(2, 4, {"a": (0, 40), "b": (0, 40)}, runs, NOW)
    assert v is not None and (v.node_ip, v.runs) == ("b", ["b1"])
    v = choose_victims(2, 4, {"a": (0, 40), "b": (0, 40)}, runs, NOW, pin="a")
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
