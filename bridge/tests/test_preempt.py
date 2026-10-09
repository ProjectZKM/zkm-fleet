"""Which runs a service preempts, what a preempted run keeps, and its record's rules."""

from __future__ import annotations

import time
from typing import Any

from fleet_bridge.model import Attempt, Cause, Profile, RunSpec, RunState, RunStatus
from fleet_bridge.runs import RunDir, budgets


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
