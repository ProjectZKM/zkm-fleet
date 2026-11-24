"""Admission reserves physical capacity; uncertain execution never frees quota."""
import os
from types import SimpleNamespace
import pytest

from fleet_bridge import jobs, shim
from fleet_bridge.model import Batch, ControlPlaneConfig, JobSpec, JobState, JobStatus, RuntimeIdentity

CLUSTER = ControlPlaneConfig(shim_binary="/opt/trusted-shim-runtime", shim_state_dir="/var/lib/trusted-shim")


def shim_run(job, monkeypatch, answer):
    """Gives the job a launched shim run whose shim answers each question with `answer()`."""
    shim.JobRun(job.path).book.write(shim.JobRun.NAME, shim.Allocation(executor=shim.ExecutorKind.SHIM, spec=job.id,
        binary=CLUSTER.shim_binary, state_dir=CLUSTER.shim_state_dir, execution=RuntimeIdentity(namespace="n",
            workload="p", allocation_id=job.id, lease_id="run-1", generation=1, node_id="node")))
    monkeypatch.setattr(shim, "invoke", lambda *args, **kwargs: answer())


def test_prepare_launch_discards_blocked_placement(monkeypatch):
    pg=SimpleNamespace(ready=lambda:'ready-ref')
    removed=[]
    monkeypatch.setattr(jobs,'job_placement',lambda spec:pg)
    monkeypatch.setattr(jobs.ray,'wait',lambda refs,timeout:([],refs))
    monkeypatch.setattr(jobs,'discard_placement',removed.append)
    assert not jobs.prepare_launch(SimpleNamespace(id='job'))
    assert removed==['job']
    monkeypatch.setattr(jobs.ray,'wait',lambda refs,timeout:(refs,[]))
    assert jobs.prepare_launch(SimpleNamespace(id='job'))
    assert removed==['job']


def test_launch_reuses_placement_and_atomically_deduplicates_actor_creation(monkeypatch):
    pg=object()
    created=[]
    class Actor:
        def options(self,**kw):
            created.append(kw)
            return self
        def remote(self,*args):
            pass
    monkeypatch.setattr(jobs,'job_placement',lambda spec:pg)
    monkeypatch.setattr(jobs,'JobActor',Actor())
    monkeypatch.setattr(jobs,'PlacementGroupSchedulingStrategy',lambda **kw:kw)
    spec=JobSpec(id='job',batch='p',policy=Batch(image='python:3'),command=['true'],gpus=0,cpus=1,out='/tmp/job/out')
    jobs.launch(spec,'/tmp/job',CLUSTER)
    jobs.launch(spec,'/tmp/job',CLUSTER)
    assert all(c['get_if_exists'] and c['name']=='job-job' for c in created)
    assert all(c['scheduling_strategy']['placement_group'] is pg for c in created)


def test_cleanup_requires_positive_container_confirmation(tmp_path,monkeypatch):
    job=jobs.JobDirectory(str(tmp_path),'job',must_exist=False)
    os.makedirs(job.path)
    JobStatus(id='job',state=JobState.CANCELLED,submitted_at=1,gpus_requested=1,cpus=1,node_ip='worker',container='trusted-shim-run').write(job.status_path)
    monkeypatch.setattr(jobs,'_going',lambda _:False)
    removed=[]
    monkeypatch.setattr(jobs,'discard_placement',removed.append)
    answers=[RuntimeError('shim daemon unavailable'),{'state':'ready'},{'state':'stopped'}]
    def answer():
        value=answers.pop(0)
        if isinstance(value,Exception):
            raise value
        return value
    shim_run(job,monkeypatch,answer)
    assert not jobs.execution_stopped(job), 'a shim that does not answer can still be running the container'
    assert not jobs.execution_stopped(job)
    assert removed==[]
    assert jobs.execution_stopped(job)
    assert removed==['job']


def test_cancelled_actor_cannot_start_container_after_cleanup(tmp_path,monkeypatch):
    job=jobs.JobDirectory(str(tmp_path),'job',must_exist=False)
    os.makedirs(job.out)
    JobStatus(id='job',state=JobState.CANCELLED,submitted_at=1,gpus_requested=0,cpus=1).write(job.status_path)
    spec=JobSpec(id='job',batch='p',policy=Batch(image='python:3'),command=['true'],gpus=0,cpus=1,out=job.out)
    monkeypatch.setattr(jobs.ray,'get_gpu_ids',lambda:[])
    monkeypatch.setattr(shim,'image_present',lambda image:True)
    called=[]
    monkeypatch.setattr(shim,'invoke',lambda *args,**kwargs:called.append(args))
    rank=jobs._Rank(spec,job.path,CLUSTER)
    assert rank.start(1)=='job stopped before container creation'
    assert called==[]


def test_preemption_does_not_release_placement_when_container_is_unreachable(tmp_path, monkeypatch):
    job = jobs.JobDirectory(str(tmp_path), "job", must_exist=False)
    os.makedirs(job.out)
    spec = JobSpec(id="job", namespace="n", batch="p", policy=Batch(image="i"), command=["x"], gpus=1, cpus=1, out=job.out)
    spec.write(job.spec_path)
    JobStatus(id="job", state=JobState.RUNNING, submitted_at=1, started_at=1,
              gpus_requested=1, cpus=1, node_ip="unreachable").write(job.status_path)
    monkeypatch.setattr(jobs, "_alive", lambda _: True)
    monkeypatch.setattr(jobs, "_going", lambda _: False)
    monkeypatch.setattr(jobs, "_end_actors", lambda *a: None)
    def unreachable():
        raise RuntimeError("shim daemon unavailable")
    shim_run(job, monkeypatch, unreachable)
    released = []
    monkeypatch.setattr(jobs, "discard_placement", released.append)
    result = jobs.preempt(str(tmp_path), "job", "higher priority")
    assert result.state is JobState.PREEMPTED and result.preempt_requeue
    assert not result.quota_released and released == []


def test_single_machine_actor_retries_oom_and_releases_local_placement(tmp_path, monkeypatch):
    from unittest.mock import Mock
    from fleet_bridge.model import JobAttempt, FailureCause

    job = jobs.JobDirectory(str(tmp_path), "job", must_exist=False)
    os.makedirs(job.out)
    spec = JobSpec(id="job", namespace="n", batch="p", policy=Batch(image="i"),
                   command=["x"], gpus=1, cpus=1, out=job.out, restart_on_oom=1)
    spec.write(job.spec_path)
    JobStatus(id="job", state=JobState.QUEUED, submitted_at=1, gpus_requested=1, cpus=1).write(job.status_path)
    actor = jobs._JobActor.__new__(jobs._JobActor)
    actor.spec, actor.dir, actor.gpus = spec, job, ["0"]
    actor.local = Mock(name="container")
    actor.local.name = "trusted-shim-run"
    actor.local.check.return_value = None
    actor.local.shortfall.return_value = []
    actor.local.start.return_value = None
    actor.local.wait.side_effect = [JobAttempt(exit_code=1, cause=FailureCause.GPU_OOM), JobAttempt(exit_code=0)]
    removed, exits = [], []
    monkeypatch.setattr(jobs.ray.util, "get_placement_group", lambda name: name)
    monkeypatch.setattr(jobs, "remove_placement_group", removed.append)
    monkeypatch.setattr(jobs.os, "_exit", exits.append)
    actor._run()
    assert job.status().state is JobState.SUCCEEDED
    assert [call.args for call in actor.local.start.call_args_list] == [(1,), (2,)]
    assert len(job.status().attempts) == 2
    actor.local.kill.assert_called_once()
    assert removed == ["pg-job-job"] and exits == [0]


def test_multiple_machine_execution_is_rejected_before_ray_allocation(monkeypatch):
    spec = JobSpec(id="job", batch="p", policy=Batch(image="i"), command=["x"],
                   gpus=1, cpus=1, out="/tmp/job/out", nodes=2)
    allocated = []
    monkeypatch.setattr(jobs, "placement_group", allocated.append)
    import pytest
    with pytest.raises(ValueError, match="exactly one machine"):
        jobs.job_placement(spec)
    assert allocated == []


def test_local_rank_reports_machine_identity_and_ray_granted_cards(monkeypatch):
    monkeypatch.setattr(jobs.ray, "get_gpu_ids", lambda: [1.0])
    monkeypatch.setattr(jobs, "node_ip", lambda: "127.0.0.1")
    spec = JobSpec(id="job", batch="p", policy=Batch(image="i"), command=["true"],
                   gpus=1, cpus=1, out="/tmp/job/out")
    info = jobs._Rank(spec, "/tmp/job", CLUSTER).info()
    assert info.machine and info.node_ip == "127.0.0.1"
    assert info.gpus == ["1"] and info.container == ""
