"""Which jobs a service preempts, what a preempted run keeps, and its record's rules."""

from __future__ import annotations

import time
from typing import Any

from fleet_bridge.model import JobAttempt, FailureCause, JobTemplate, JobSpec, JobState, JobStatus
from fleet_bridge.jobs import JobDirectory, budgets


def spec(**kw: Any) -> JobSpec:
    base: dict[str, Any] = dict(id="r", profile_name="p", profile=JobTemplate(image="i"), command=["x"], gpus=1, cpus=1,
                                out="/tmp/out")
    base.update(kw)
    return JobSpec(**base)


def test_a_preempted_attempt_spends_no_retry() -> None:
    s = spec(retry=2, restart_on_oom=1)
    attempts = [JobAttempt(exit_code=1, cause=FailureCause.GPU_OOM), JobAttempt(exit_code=None, cause=FailureCause.PREEMPTED),
                JobAttempt(exit_code=1, cause=FailureCause.EXIT_CODE), JobAttempt(exit_code=1, cause=FailureCause.GPU_OOM)]
    assert budgets(s, attempts) == (0, 0), "oom: restart then retry; exit: retry; preempted: nothing"
    assert budgets(s, [JobAttempt(exit_code=None, cause=FailureCause.PREEMPTED)] * 5) == (2, 1)


def test_a_job_being_preempted_takes_only_the_preemptions_writes_and_a_cancel(tmp_path: Any) -> None:
    job = JobDirectory(str(tmp_path), "train-1", must_exist=False)
    import os

    os.makedirs(job.path)
    JobStatus(id="train-1", state=JobState.RUNNING, submitted_at=time.time(), gpus_requested=1, cpus=1).write(job.status_path)
    assert job.update(state=JobState.PREEMPTED).state is JobState.PREEMPTED
    assert job.update(state=JobState.RETRYING).state is JobState.PREEMPTED, "the stopped actor's late report is dropped"
    assert job.update(state=JobState.FAILED, error="x").state is JobState.PREEMPTED
    assert job.update(force=True, state=JobState.QUEUED, preemptions=1).state is JobState.QUEUED
    job.update(state=JobState.PREEMPTED)
    assert job.update(state=JobState.CANCELLED).state is JobState.CANCELLED, "a cancel still wins"


def test_preempted_execution_waits_for_control_plane_admission(tmp_path, monkeypatch):
    import os
    from unittest.mock import Mock
    from fleet_bridge import jobs
    job = JobDirectory(str(tmp_path), "protected", must_exist=False)
    os.makedirs(job.out)
    spec(namespace="n", id=job.id, out=job.out).write(job.spec_path)
    JobStatus(id=job.id, state=JobState.RUNNING, submitted_at=1, started_at=1, gpus_requested=1, cpus=1).write(job.status_path)
    monkeypatch.setattr(jobs, "_alive", lambda _: True)
    monkeypatch.setattr(jobs, "_stop_ranks", lambda *args: None)
    monkeypatch.setattr(jobs, "_end_actors", lambda *args: None)
    monkeypatch.setattr(jobs, "confirm_cleanup", lambda _: True)
    launch = Mock()
    monkeypatch.setattr(jobs, "launch", launch)
    result = jobs.preempt(str(tmp_path), job.id, "control-plane preemption")
    assert result.state is JobState.PREEMPTED and result.preempt_requeue
    jobs.requeue_job(str(tmp_path), job.id)
    assert job.status().state is JobState.WAITING_QUOTA
    launch.assert_not_called()
