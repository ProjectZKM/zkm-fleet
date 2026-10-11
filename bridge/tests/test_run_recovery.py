"""Admission reserves physical capacity; uncertain execution never frees quota."""
import os
from types import SimpleNamespace

from fleet_bridge import runs
from fleet_bridge.model import Profile, RunSpec, RunState, RunStatus


def test_prepare_launch_discards_blocked_placement(monkeypatch):
    pg=SimpleNamespace(ready=lambda:'ready-ref')
    removed=[]
    monkeypatch.setattr(runs,'run_placement',lambda spec:pg)
    monkeypatch.setattr(runs.ray,'wait',lambda refs,timeout:([],refs))
    monkeypatch.setattr(runs,'discard_placement',removed.append)
    assert not runs.prepare_launch(SimpleNamespace(id='job'))
    assert removed==['job']
    monkeypatch.setattr(runs.ray,'wait',lambda refs,timeout:(refs,[]))
    assert runs.prepare_launch(SimpleNamespace(id='job'))
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
    monkeypatch.setattr(runs,'run_placement',lambda spec:pg)
    monkeypatch.setattr(runs,'GpuRun',Actor())
    monkeypatch.setattr(runs,'PlacementGroupSchedulingStrategy',lambda **kw:kw)
    spec=RunSpec(id='job',profile_name='p',profile=Profile(image='python:3'),command=['true'],gpus=0,cpus=1,out='/tmp/job/out')
    runs.launch(spec,'/tmp/job')
    runs.launch(spec,'/tmp/job')
    assert all(c['get_if_exists'] and c['name']=='run-job' for c in created)
    assert all(c['scheduling_strategy']['placement_group'] is pg for c in created)


def test_cleanup_requires_positive_container_confirmation(tmp_path,monkeypatch):
    run=runs.RunDir(str(tmp_path),'job',must_exist=False)
    os.makedirs(run.path)
    RunStatus(id='job',state=RunState.CANCELLED,submitted_at=1,gpus_requested=1,cpus=1,node_ip='worker',container='fleet-run-job').write(run.status_path)
    monkeypatch.setattr(runs,'_going',lambda _:False)
    monkeypatch.setattr(runs,'alive_nodes',lambda:[])
    removed=[]
    monkeypatch.setattr(runs,'discard_placement',removed.append)
    assert not runs.execution_stopped(run), 'an unreachable node can still be running Docker'
    monkeypatch.setattr(runs,'alive_nodes',lambda:[{'NodeManagerAddress':'worker'}])
    class Probe:
        def options(self,**kw):return self
        def remote(self,*args):return 'probe'
    monkeypatch.setattr(runs,'_container_stopped',Probe())
    monkeypatch.setattr(runs.ray,'get',lambda *args,**kw:False)
    assert not runs.execution_stopped(run)
    assert removed==[]
    monkeypatch.setattr(runs.ray,'get',lambda *args,**kw:True)
    assert runs.execution_stopped(run)
    assert removed==['job']


def test_cancelled_actor_cannot_start_container_after_cleanup(tmp_path,monkeypatch):
    run=runs.RunDir(str(tmp_path),'job',must_exist=False)
    os.makedirs(run.out)
    RunStatus(id='job',state=RunState.CANCELLED,submitted_at=1,gpus_requested=0,cpus=1).write(run.status_path)
    spec=RunSpec(id='job',profile_name='p',profile=Profile(image='python:3'),command=['true'],gpus=0,cpus=1,out=run.out)
    monkeypatch.setattr(runs.ray,'get_gpu_ids',lambda:[])
    called=[]
    monkeypatch.setattr(runs,'_sh',called.append)
    rank=runs._Rank(spec)
    assert rank.start(1)=='run stopped before container creation'
    assert called==[]


def test_preemption_does_not_release_placement_when_container_is_unreachable(tmp_path, monkeypatch):
    run = runs.RunDir(str(tmp_path), "job", must_exist=False)
    os.makedirs(run.out)
    spec = RunSpec(id="job", namespace="n", profile_name="p", profile=Profile(image="i"), command=["x"], gpus=1, cpus=1, out=run.out)
    spec.write(run.spec_path)
    RunStatus(id="job", state=RunState.RUNNING, submitted_at=1, started_at=1,
              gpus_requested=1, cpus=1, node_ip="unreachable").write(run.status_path)
    monkeypatch.setattr(runs, "_alive", lambda _: True)
    monkeypatch.setattr(runs, "_going", lambda _: False)
    monkeypatch.setattr(runs, "_stop_ranks", lambda *a: None)
    monkeypatch.setattr(runs, "_end_actors", lambda *a: None)
    monkeypatch.setattr(runs, "_on_run_machine", lambda *a: None)
    monkeypatch.setattr(runs, "alive_nodes", lambda: [])
    released = []
    monkeypatch.setattr(runs, "discard_placement", released.append)
    result = runs.preempt(str(tmp_path), "job", "higher priority")
    assert result.state is RunState.PREEMPTED and result.preempt_requeue
    assert not result.quota_released and released == []


def test_single_machine_actor_retries_oom_and_releases_local_placement(tmp_path, monkeypatch):
    from unittest.mock import Mock
    from fleet_bridge.model import Attempt, Cause

    run = runs.RunDir(str(tmp_path), "job", must_exist=False)
    os.makedirs(run.out)
    spec = RunSpec(id="job", namespace="n", profile_name="p", profile=Profile(image="i"),
                   command=["x"], gpus=1, cpus=1, out=run.out, restart_on_oom=1)
    spec.write(run.spec_path)
    RunStatus(id="job", state=RunState.QUEUED, submitted_at=1, gpus_requested=1, cpus=1).write(run.status_path)
    actor = runs._GpuRun.__new__(runs._GpuRun)
    actor.spec, actor.dir, actor.gpus = spec, run, ["0"]
    actor.local = Mock(name="container")
    actor.local.name = "fleet-run-job"
    actor.local.check.return_value = None
    actor.local.shortfall.return_value = []
    actor.local.start.return_value = None
    actor.local.wait.side_effect = [Attempt(exit_code=1, cause=Cause.GPU_OOM), Attempt(exit_code=0)]
    removed, exits = [], []
    monkeypatch.setattr(runs.ray.util, "get_placement_group", lambda name: name)
    monkeypatch.setattr(runs, "remove_placement_group", removed.append)
    monkeypatch.setattr(runs.os, "_exit", exits.append)
    actor._run()
    assert run.status().state is RunState.SUCCEEDED
    assert [call.args for call in actor.local.start.call_args_list] == [(1,), (2,)]
    assert len(run.status().attempts) == 2
    actor.local.kill.assert_called_once()
    assert removed == ["pg-run-job"] and exits == [0]


def test_multiple_machine_execution_is_rejected_before_ray_allocation(monkeypatch):
    spec = RunSpec(id="job", profile_name="p", profile=Profile(image="i"), command=["x"],
                   gpus=1, cpus=1, out="/tmp/job/out", nodes=2)
    allocated = []
    monkeypatch.setattr(runs, "placement_group", allocated.append)
    import pytest
    with pytest.raises(ValueError, match="exactly one machine"):
        runs.run_placement(spec)
    assert allocated == []


def test_local_rank_reports_machine_identity_and_ray_granted_cards(monkeypatch):
    monkeypatch.setattr(runs.ray, "get_gpu_ids", lambda: [1.0])
    monkeypatch.setattr(runs, "node_ip", lambda: "127.0.0.1")
    spec = RunSpec(id="job", profile_name="p", profile=Profile(image="i"), command=["true"],
                   gpus=1, cpus=1, out="/tmp/job/out")
    info = runs._Rank(spec).info()
    assert info.machine and info.node_ip == "127.0.0.1"
    assert info.gpus == ["1"] and info.container == "fleet-run-job"
