"""A batch job runs to completion through the shim as it does under `docker run`."""
import base64
import os
from unittest.mock import Mock

import pytest

from fleet_bridge import jobs, node, shim
from fleet_bridge.model import Batch, ControlPlaneConfig, FailureCause, JobSpec, JobState, JobStatus, Mount, ModelError

ON_SHIM = {"executor": "shim", "driver": "docker"}
ALL_FEATURES = ["data-plane-v1", *shim.BATCH_FEATURES]


class FakeShim:
    """The daemon's side of one node: each launched run, its output and how it ends."""

    def __init__(self, monkeypatch):
        self.calls, self.runs, self.down = [], {}, False
        # What the next launched run writes, and how its process ends.
        self.output, self.exit = [b"working\n"], {"code": 0, "oom_killed": False, "started_at": "t0", "finished_at": "t1"}
        monkeypatch.setattr(shim, "invoke", self)
        monkeypatch.setattr(shim.time, "sleep", lambda _: None)

    def __call__(self, binary, state_dir, node_id, command, *, daemon=False, timeout=None):
        operation = command["operation"]
        self.calls.append(operation)
        if self.down:
            raise RuntimeError("shim daemon unavailable or rejected operation; no legacy fallback")
        execution = command["request"]["execution"] if operation == "launch" else command["execution"]
        key = execution.generation
        if operation == "launch":
            assert all(r["state"] == "deleted" for r in self.runs.values()), "the run before it is removed first"
            assert key > max(self.runs, default=0)
            self.runs[key] = {"state": "ready", "request": command["request"], "output": list(self.output),
                              "exit": dict(self.exit), "polls": 0}
            return {"state": "ready", "runtime_id": "trusted-shim-" + "a" * 64}
        run = self.runs.get(key)
        if operation == "delete":
            if run:
                run["state"] = "deleted"
            return {"cleaned": True}
        assert run and run["state"] != "deleted", "only a launched run is asked about"
        if operation == "stop":
            run["state"], run["exit"]["code"] = "stopped", 143
            return {}
        if operation == "inspect":
            run["polls"] += 1
            if run["polls"] > 2 and run["state"] == "ready":  # its process ends after two looks
                run["state"] = "stopped"
            return {"state": run["state"], **({"exit": run["exit"]} if run["state"] == "stopped" else {})}
        if operation == "logs":
            assert ("since" in command) == bool(run.get("read")), "each read starts where the last one ended"
            lines, run["output"] = run["output"][:1], run["output"][1:]
            run["read"] = True
            window = b"".join(b"2026-11-24T00:00:00.000000001Z " + line for line in lines)
            return {"output": base64.b64encode(window).decode(), "next_since": "1795000000.000000002", "more": bool(run["output"])}
        raise AssertionError(operation)


@pytest.fixture(autouse=True)
def local(monkeypatch):
    monkeypatch.setattr(jobs.ray, "get_gpu_ids", lambda: [3])
    monkeypatch.setattr(shim, "image_present", lambda image: True)
    monkeypatch.setattr(shim, "pull_image", lambda *a: pytest.fail("the image is here"))


def cluster(tmp_path):
    return ControlPlaneConfig(shim_binary="/opt/trusted-shim-runtime", shim_state_dir=str(tmp_path / "shim"), shim_daemon=True)


def queued(tmp_path, **settings):
    """A queued job of a batch on the shim, with its directory."""
    job = jobs.JobDirectory(str(tmp_path), "job-1", must_exist=False)
    os.makedirs(job.out)
    spec = JobSpec(**{**dict(id="job-1", namespace="a" * 32, owner="owner", batch="training",
        policy=Batch.load({"image": "trainer:1", "user": "1000:1000", "env": {"A": "batch", "B": "batch"}, "stop_timeout_s": 120, "runtime": ON_SHIM}),
        command=["train", "--out", "{out}"], gpus=1, cpus=2, out=job.out, env={"B": "job"},
        mounts=[Mount.parse("/data/models:ro")], workdir="{out}"), **settings})
    spec.write(job.spec_path)
    JobStatus(id="job-1", state=JobState.QUEUED, submitted_at=1, gpus_requested=1, cpus=2).write(job.status_path)
    return job, spec


def test_a_job_on_the_shim_gets_what_docker_run_gives_it_and_ends_with_its_process(tmp_path, monkeypatch):
    fake = FakeShim(monkeypatch)
    fake.output = [b"epoch 1\n", b"epoch 2\n"]
    job, spec = queued(tmp_path)
    rank = jobs._ShimRank(spec, job.path, cluster(tmp_path))
    assert rank.start(1) is None and rank.name == "trusted-shim-" + "a" * 64
    request = fake.runs[1]["request"]
    execution = request["execution"]
    assert (execution.namespace, execution.workload, execution.allocation_id, execution.generation) == ("a" * 32, "training", "job-1", 1)
    assert (request["driver"], request["image"], request["command"], request["services"]) == ("docker", "trainer:1", ["train", "--out", job.out], [])
    assert request["limits"] == {"cpu_millicores": 2000, "memory_bytes": None}
    assert request["driver_options"] == {"run_to_completion": True, "network": "none", "gpus": ["3"],
        "volumes": ["/data/models:/data/models:ro", f"{job.out}:{job.out}"], "user": "1000:1000", "workdir": job.out,
        "stop_timeout_s": 120, "shm_bytes": 16 * 2**30}
    # The same environment as `docker run -e`, the job's values over the batch's.
    docker = jobs.docker_command(spec, ["3"], 1)
    assert request["environment"] == dict(docker[i + 1].split("=", 1) for i, arg in enumerate(docker) if arg == "-e")
    assert (request["environment"]["B"], request["environment"]["FLEET_ATTEMPT"]) == ("job", "1")
    result = rank.wait()
    assert result.ok and result.cause is None
    # Its output is in the job's log, each line once and without the shim's timestamps.
    assert open(spec.log_path, "rb").read() == b"epoch 1\nepoch 2\n"
    # Its container is removed, and nothing of it runs.
    assert fake.calls[-1] == "delete" and shim.JobRun(job.path).record().cleaned
    fake.calls.clear()
    assert shim.JobRun(job.path).gone() and node.job_gone(tmp_path, "job-1") is not None and fake.calls == []


@pytest.mark.parametrize("exit_status, output, cause", [
    ({"code": 137, "oom_killed": True}, [b"loading\n"], FailureCause.HOST_OOM),
    ({"code": 1, "oom_killed": False}, [b"RuntimeError: CUDA out of memory.\n"], FailureCause.GPU_OOM),
    ({"code": 3, "oom_killed": False}, [b"bad input\n"], FailureCause.EXIT_CODE),
])
def test_an_attempt_reports_how_it_ended_and_the_next_one_is_a_new_run(tmp_path, monkeypatch, exit_status, output, cause):
    fake = FakeShim(monkeypatch)
    monkeypatch.setattr(jobs, "_holders", lambda gpus: "none")
    job, spec = queued(tmp_path)
    rank = jobs._ShimRank(spec, job.path, cluster(tmp_path))
    fake.exit, fake.output = exit_status, output
    assert rank.start(1) is None
    result = rank.wait()
    assert (result.exit_code, result.cause) == (exit_status["code"], cause)
    fake.exit, fake.output = {"code": 0, "oom_killed": False}, [b"again\n"]
    assert rank.start(2) is None and rank.wait().ok
    assert [r["request"]["environment"]["FLEET_ATTEMPT"] for r in fake.runs.values()] == ["1", "2"]
    assert sorted(fake.runs) == [1, 2] and open(spec.log_path, "rb").read() == output[0] + b"again\n"


def test_a_missing_image_is_pulled_first_and_a_stopped_job_starts_nothing(tmp_path, monkeypatch):
    fake = FakeShim(monkeypatch)
    job, spec = queued(tmp_path)
    rank = jobs._ShimRank(spec, job.path, cluster(tmp_path))
    pulls = []
    monkeypatch.setattr(shim, "image_present", lambda image: False)
    monkeypatch.setattr(shim, "pull_image", lambda name, image: pulls.append(image))
    assert rank.start(1) is None and pulls == ["trainer:1"] and fake.calls == ["launch"]
    def refused(name, image):
        raise RuntimeError(f"{name}: could not pull image {image}: denied")
    monkeypatch.setattr(shim, "pull_image", refused)
    assert "could not pull image trainer:1" in rank.start(2) and fake.calls == ["launch"]
    monkeypatch.setattr(shim, "image_present", lambda image: True)
    job.update(state=JobState.CANCELLED)
    assert rank.start(2) == "job stopped before container creation" and fake.calls == ["launch"]


def test_a_shim_that_refuses_the_launch_fails_the_attempt_and_leaves_nothing_behind(tmp_path, monkeypatch):
    fake = FakeShim(monkeypatch)
    job, spec = queued(tmp_path)
    rank = jobs._ShimRank(spec, job.path, cluster(tmp_path))
    fake.down = True
    assert "shim daemon unavailable" in rank.start(1)
    run = shim.JobRun(job.path)
    # The record is written before the launch, so the run can still be fenced.
    assert run.record().execution.generation == 1 and not run.gone() and not run.remove()
    fake.down = False
    rank.kill()
    assert run.record().cleaned and run.gone()
    assert rank.start(2) is None and sorted(fake.runs) == [2], "a later run never reuses a generation"
    assert jobs._ShimRank(spec, job.path, ControlPlaneConfig()).start(3) == "shim is not configured"


def test_a_run_whose_shim_stops_answering_is_lost_and_one_whose_container_is_gone_has_failed(tmp_path, monkeypatch):
    fake = FakeShim(monkeypatch)
    job, spec = queued(tmp_path)
    rank = jobs._ShimRank(spec, job.path, cluster(tmp_path))
    assert rank.start(1) is None
    fake.down = True
    with pytest.raises(RuntimeError):
        rank.wait()
    assert fake.calls.count("inspect") > shim.JOB_UNANSWERED_S / 2
    fake.down = False
    fake.runs[1]["state"] = "failed"
    rank.log = open(spec.log_path, "ab")
    result = rank.wait()
    assert (result.exit_code, result.cause) == (None, FailureCause.EXIT_CODE)


def test_a_cancel_stops_the_shim_run_with_its_grace_and_removes_it(tmp_path, monkeypatch):
    fake = FakeShim(monkeypatch)
    job, spec = queued(tmp_path)
    assert jobs._ShimRank(spec, job.path, cluster(tmp_path)).start(1) is None
    job.update(state=JobState.RUNNING, node_ip="10.0.0.1", started_at=2)
    monkeypatch.setattr(jobs, "_alive", lambda _: False)
    monkeypatch.setattr(jobs, "_stop_ranks", lambda *a: None)
    monkeypatch.setattr(jobs, "_end_actors", lambda *a: None)
    monkeypatch.setattr(jobs, "_on_job_node", lambda *a: "")
    monkeypatch.setattr(jobs, "remove_placement_group", lambda pg: None)
    monkeypatch.setattr(jobs.ray.util, "get_placement_group", lambda name: name)
    monkeypatch.setattr(jobs, "alive_nodes", lambda: [{"NodeManagerAddress": "10.0.0.1"}])
    assert not jobs.execution_stopped(job), "its container still runs"
    fake.calls.clear()
    assert jobs.cancel(str(tmp_path), "job-1").state is JobState.CANCELLED
    assert fake.calls == ["stop", "delete"] and fake.runs[1]["state"] == "deleted"
    # A job that never ran on the shim asks it nothing.
    fake.calls.clear()
    other = jobs.JobDirectory(str(tmp_path), "job-2", must_exist=False)
    os.makedirs(other.path)
    run = shim.JobRun(other.path)
    run.stop(30)
    assert run.gone() and run.remove() and fake.calls == [] and not os.path.exists(os.path.join(other.path, ".shim"))


def test_only_a_shim_with_every_batch_feature_takes_batch_jobs(tmp_path, monkeypatch):
    config = cluster(tmp_path)
    for features, expected in [(ALL_FEATURES, True), *[([f for f in ALL_FEATURES if f != missing], False) for missing in shim.BATCH_FEATURES]]:
        monkeypatch.setattr(shim, "daemon_call", lambda *a, **k: {"protocol": 1, "drivers": ["docker"], "features": features})
        assert (shim.BATCH_CAPABILITY in shim.capabilities(config)) is expected, features
    with pytest.raises(ModelError):
        Batch.load({"image": "trainer:1", "runtime": {"executor": "shim", "driver": "tee"}})
    assert not Batch(image="trainer:1").on_shim and not Batch.load({"image": "trainer:1", "runtime": {"executor": "legacy"}}).on_shim


def test_a_node_launches_a_shim_batch_only_when_its_shim_runs_batch_jobs(tmp_path, monkeypatch):
    (tmp_path / "batches.yaml").write_text("batches: {}\n")
    (tmp_path / "apps.yaml").write_text(f"shim_binary: /opt/trusted-shim-runtime\nshim_state_dir: {tmp_path}/shim\nshim_daemon: true\napps: []\n")
    executor = node.NodeExecutor(str(tmp_path / "apps.yaml"), str(tmp_path / "batches.yaml"), str(tmp_path / "jobs"))
    executor.pool.shutdown()
    monkeypatch.setattr(node.ray, "nodes", lambda: [{"Alive": True}])
    monkeypatch.setattr(jobs, "prepare_launch", lambda _: True)
    launch = Mock()
    monkeypatch.setattr(jobs, "launch", launch)
    parameters = lambda ident, runtime: {"id": ident, "lease": "lease-1", "owner": "owner", "namespace": "a" * 32,
        "batch": "training", "submitted_at": 1, "request": {"command": ["true"], "gpus": 0, "cpus": 1},
        "policy": {"image": "trainer:1", "namespace": "a" * 32, **({"runtime": runtime} if runtime else {})}}
    monkeypatch.setattr(shim, "capabilities", lambda config: ["shim-docker-v1"])
    with pytest.raises(ValueError, match="does not run batch jobs"):
        executor.launch(parameters("job-1234abcd", ON_SHIM))
    launch.assert_not_called()
    monkeypatch.setattr(shim, "capabilities", lambda config: ["shim-docker-v1", shim.BATCH_CAPABILITY])
    executor.launch(parameters("job-1234abcd", ON_SHIM))
    spec, _, config = launch.call_args.args
    assert spec.policy.on_shim and (config.shim_binary, config.shim_daemon) == ("/opt/trusted-shim-runtime", True)
    # A batch on the legacy executor needs nothing of the shim.
    monkeypatch.setattr(shim, "capabilities", lambda config: pytest.fail("not asked"))
    executor.launch(parameters("job-5678abcd", None))
    assert launch.call_args.args[2] is None
