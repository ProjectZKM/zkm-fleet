"""Local execution fences are independent of user authentication and admission."""
from unittest.mock import Mock
import pytest
import yaml

from fleet_bridge import actors, node, reconcile, jobs
from fleet_bridge.model import Batch, JobSpec, JobState, JobStatus


@pytest.fixture
def executor(tmp_path, monkeypatch):
    (tmp_path / "batches.yaml").write_text("batches: {}\n")
    (tmp_path / "services.yaml").write_text(yaml.safe_dump({"services": [{
        "name": "proof", "kind": "ziren-gpu", "namespace": "a"*32,
        "proof_app": "eth/"+"b"*64, "disabled": True, "replicas": 2,
    }]}))
    e=node.NodeExecutor(str(tmp_path/"services.yaml"),str(tmp_path/"batches.yaml"),str(tmp_path/"jobs"))
    e.pool.shutdown()
    e.pool=Mock()
    monkeypatch.setattr(node.ray,"nodes",lambda:[{"Alive":True}])
    return e


def command(operation=1):
    return {"id":"command-1","operation":operation,"parameters":{"id":"job-1234abcd","lease":"lease-1"}}


def test_duplicate_command_is_queued_once_and_result_is_durable(executor):
    c=command();executor.receive(c);executor.receive(c)
    executor.pool.submit.assert_called_once()
    executor.launch=Mock(return_value={"id":c["parameters"]["id"]})
    executor.execute(c)
    entry=node.read_json(executor.journal/"command-1.json")
    assert entry["done"] and not entry["error"]
    executor.receive(c)
    executor.pool.submit.assert_called_once()


def test_changed_parameters_for_same_command_are_refused(executor):
    executor.receive(command());other=command();other["parameters"]["lease"]="another"
    with pytest.raises(ValueError,match="different parameters"):
        executor.receive(other)


def test_cancel_fences_launch_even_if_commands_execute_out_of_order(executor):
    executor.receive(command(2))
    with pytest.raises(ValueError,match="stopped before launch"):
        executor.launch(command()["parameters"])


def test_cancel_before_local_record_confirms_absence_without_launch(executor,monkeypatch):
    monkeypatch.setattr(jobs,"_going",lambda _:False)
    monkeypatch.setattr(node,"container_gone",lambda _:True)
    assert executor.stop(command()["parameters"],False)["cleaned"]
    monkeypatch.setattr(node,"container_gone",lambda _:False)
    with pytest.raises(ValueError,match="without a local record"):
        executor.stop(command()["parameters"],False)


def test_disable_removes_every_replica_even_when_already_disabled(executor,monkeypatch):
    cfg=yaml.safe_load(executor.services.read_text())
    stop=Mock();clean=Mock();monkeypatch.setattr(reconcile,"stop",stop)
    monkeypatch.setattr(actors,"clean_after",clean);monkeypatch.setattr(node.ray,"get",lambda *a,**k:None)
    monkeypatch.setattr(node,"container_gone",lambda _:True)
    result=executor.service({"service":cfg["services"][0],"disabled":True})
    assert result["cleaned"]
    assert [c.args[0] for c in stop.call_args_list]==["proof-0","proof-1"]
    assert clean.remote.call_count==2
    assert yaml.safe_load(executor.services.read_text())["services"][0]["disabled"]


def test_disable_failure_retains_desired_state_and_does_not_confirm_cleanup(executor,monkeypatch):
    service=yaml.safe_load(executor.services.read_text())["services"][0]
    monkeypatch.setattr(reconcile,"stop",lambda _:None)
    monkeypatch.setattr(actors,"clean_after",Mock());monkeypatch.setattr(node.ray,"get",lambda *a,**k:None)
    monkeypatch.setattr(node,"container_gone",lambda _:False)
    with pytest.raises(ValueError,match="uncertain"):
        executor.service({"service":service,"disabled":True})
    assert yaml.safe_load(executor.services.read_text())["services"][0]["disabled"]


def test_enable_uses_local_executor_and_no_admin_token(executor,monkeypatch):
    service=yaml.safe_load(executor.services.read_text())["services"][0]
    monkeypatch.setattr(reconcile,"running",lambda:{})
    start=Mock();monkeypatch.setattr(reconcile,"start",start)
    assert executor.service({"service":service,"disabled":False})["ready"]
    assert start.call_count==2
    assert all(not c.kwargs for c in start.call_args_list)


def test_purge_refuses_an_old_execution_lease(executor):
    c=command(5);path=executor.jobs_dir/c["parameters"]["id"];path.mkdir(parents=True)
    node.write_json(path/".fleet-lease.json",{"lease":"new-lease"})
    executor.execute(c)
    assert path.exists()
    assert "stale purge" in node.read_json(executor.journal/"command-1.json")["error"]


def test_local_ray_must_contain_only_this_machine(executor,monkeypatch):
    monkeypatch.setattr(node.ray,"nodes",lambda:[{"Alive":True},{"Alive":True}])
    with pytest.raises(ValueError,match="only this machine"):
        executor.service({"service":{},"disabled":False})


def test_pending_launch_cannot_recover_into_a_shared_ray_cluster(executor, monkeypatch):
    monkeypatch.setattr(node.ray, "nodes", lambda: [{"Alive": True}, {"Alive": True}])
    with pytest.raises(ValueError, match="only this machine"):
        executor.launch(command()["parameters"])


def test_pending_launch_refuses_removed_input_capture_without_starting_a_job(executor):
    c = command()
    c["parameters"]["request"] = {"code": "upload:" + "a" * 64}
    with pytest.raises(ValueError, match="app owner in storage"):
        executor.launch(c["parameters"])
    assert not (executor.jobs_dir / c["parameters"]["id"]).exists()


def test_storage_inputs_are_passed_to_the_app_without_capturing_code(executor, monkeypatch):
    p = command()["parameters"]
    p.update({"owner": "owner", "namespace": "a" * 32, "tag": "scope=default",
              "batch": "training", "policy": {"image": "training:latest", "namespace": "a" * 32},
              "submitted_at": 1,
              "request": {"command": ["train", "--input", "s3://owner/dataset"], "gpus": 0,
                          "cpus": 1, "env": ["INPUT_URI=s3://owner/dataset"]}})
    monkeypatch.setattr(jobs, "prepare_launch", lambda _: True)
    launch = Mock()
    monkeypatch.setattr(jobs, "launch", launch)
    executor.launch(p)
    spec = launch.call_args.args[0]
    assert spec.command == p["request"]["command"]
    assert spec.env["INPUT_URI"] == "s3://owner/dataset"
    assert "code" not in spec.dump() and "code_dir" not in spec.dump()


def test_finished_job_reports_log_completion_without_archiving_outputs(executor, monkeypatch):
    directory = executor.jobs_dir / "job-output"
    (directory / "out").mkdir(parents=True)
    (directory / "out/result.txt").write_text("app-owned output")
    JobSpec(id="job-output", batch="training", policy=Batch(image="training:latest"),
            command=["true"], gpus=0, cpus=1, out=str(directory / "out")).write(str(directory / "spec.json"))
    JobStatus(id="job-output", state=JobState.SUCCEEDED, submitted_at=1, gpus_requested=0, cpus=1).write(str(directory / "status.json"))
    node.write_json(directory / ".fleet-lease.json", {"lease": "lease-1"})
    monkeypatch.setattr(node.ray, "cluster_resources", lambda: {"CPU": 1})
    monkeypatch.setattr(node.ray, "available_resources", lambda: {"CPU": 1})
    monkeypatch.setattr(reconcile, "running", lambda: {})
    monkeypatch.setattr(node, "container_gone", lambda _: True)
    monkeypatch.setattr(jobs, "_going", lambda _: False)
    monkeypatch.setattr(jobs, "discard_placement", lambda _: None)
    report = executor.report()["jobs"][0]
    assert report["cleaned"] and not report["logs_ready"]
    assert not list(directory.glob("*.tar.gz"))
    (directory / ".fleet-logs-lease-1").touch()
    assert executor.report()["jobs"][0]["logs_ready"]


def test_healthy_proof_service_is_ready_and_enable_preserves_actors(executor, monkeypatch):
    from fleet_bridge.model import ReplicaHealth
    cfg=yaml.safe_load(executor.services.read_text())
    cfg['services'][0]['disabled']=False
    executor.services.write_text(yaml.safe_dump(cfg))
    specs=reconcile.replica_specs(reconcile.load_services(cfg))
    monkeypatch.setattr(reconcile,'running',lambda:{name:name for name in specs})
    monkeypatch.setattr(reconcile,'health_of',lambda name, **kwargs:ReplicaHealth(name=name,spec=actors.spec_hash(specs[name]),gpu_ready=True))
    start,stop=Mock(),Mock()
    monkeypatch.setattr(reconcile,'start',start);monkeypatch.setattr(reconcile,'stop',stop)
    monkeypatch.setattr(node.ray,'cluster_resources',lambda:{'GPU':8,'CPU':48})
    monkeypatch.setattr(node.ray,'available_resources',lambda:{'GPU':4,'CPU':16})
    assert executor.report()['services'][0]['ready'] is True
    assert executor.service({'service':cfg['services'][0],'disabled':False})['ready']
    start.assert_not_called();stop.assert_not_called()
    assert not ReplicaHealth(name='gpu',spec='hash',gpu_ready=False).is_ready
    assert not ReplicaHealth(name='gpu',spec='hash',gpu_ready=True,stopping=True).is_ready
    assert ReplicaHealth(name='cpu',spec='hash').is_ready


def test_actor_health_timeout_keeps_machine_heartbeat_and_command_results(executor, monkeypatch):
    from fleet_bridge.model import ReplicaHealth
    monkeypatch.setattr(node.ray, 'cluster_resources', lambda: {'GPU': 8, 'CPU': 48})
    monkeypatch.setattr(node.ray, 'available_resources', lambda: {'GPU': 4, 'CPU': 16})
    monkeypatch.setattr(reconcile, 'running', lambda: {'proof-0': 'starting', 'proof-1': 'healthy'})
    health = Mock(side_effect=[node.ray.exceptions.GetTimeoutError('starting'),
                             ReplicaHealth(name='proof-1', spec='hash', gpu_ready=True)])
    monkeypatch.setattr(reconcile, 'health_of', health)
    node.write_json(executor.journal / 'cleanup.json', {'command': {'id': 'cleanup'},
                                                     'done': True, 'result': {'cleaned': True}, 'error': None})
    report = executor.report()
    assert report['ready'] and report['total_gpus'] == 8
    assert not report['services'][0]['ready']
    assert report['results'][0]['result']['cleaned']
    assert all(call.kwargs == {'timeout_s': 5} for call in health.call_args_list)


def test_scale_applies_control_resources_but_preserves_machine_config(executor, monkeypatch):
    from fleet_bridge import actors
    service=yaml.safe_load(executor.services.read_text())["services"][0]
    service.update(gpus=2,cpus=8,replicas=1)
    monkeypatch.setattr(reconcile,"stop",lambda _:None)
    monkeypatch.setattr(actors,"clean_after",Mock());monkeypatch.setattr(node.ray,"get",lambda *a,**k:None)
    monkeypatch.setattr(node,"container_gone",lambda _:True)
    monkeypatch.setattr(reconcile,"running",lambda:{})
    start=Mock();monkeypatch.setattr(reconcile,"start",start)
    result=executor.service({"service":service,"disabled":False,"scale_id":"resize"})
    assert result["ready"]
    assert start.call_count==1
    replica=start.call_args.args[0]
    assert (replica.service.gpus,replica.service.cpus)==(2,8)
    cfg=yaml.safe_load(executor.services.read_text())
    assert cfg["services"][0]["proof_app"]==service["proof_app"]
    assert cfg["services"][0]["replicas"]==1
    # The removed replica is cleaned even though it no longer appears in desired config.
    # This test uses the same cleanup mocks as an enable after the scheduler's stop phase.


def test_scale_down_cleans_removed_replicas(executor,monkeypatch):
    service=yaml.safe_load(executor.services.read_text())["services"][0]
    service["replicas"]=1
    stop=Mock();monkeypatch.setattr(reconcile,"stop",stop)
    monkeypatch.setattr(actors,"clean_after",Mock());monkeypatch.setattr(node.ray,"get",lambda *a,**k:None)
    monkeypatch.setattr(node,"container_gone",lambda _:True)
    result=executor.service({"service":service,"disabled":True,"scale_id":"resize"})
    assert result["cleaned"]
    assert [c.args[0] for c in stop.call_args_list]==["proof-0","proof-1"]
    assert yaml.safe_load(executor.services.read_text())["services"][0]["replicas"]==1


@pytest.mark.parametrize("field,value",[("gpus",-1),("gpus",0.5),("cpus",0),("replicas",0),("replicas",1.5),("replicas",True),("replicas",65)])
def test_scale_invalid_resources_leave_config_unchanged(executor,field,value):
    before=executor.services.read_bytes()
    service=yaml.safe_load(before)["services"][0];service[field]=value
    with pytest.raises(ValueError,match="invalid service resources"):
        executor.service({"service":service,"disabled":True})
    assert executor.services.read_bytes()==before


@pytest.mark.parametrize("field,value",[("namespace","another"),("proof_app","eth/"+"c"*64),("tag","other"),("kind","app")])
def test_scale_cannot_change_machine_service_scope(executor,field,value):
    before=executor.services.read_bytes()
    service=yaml.safe_load(before)["services"][0];service[field]=value
    with pytest.raises(ValueError,match="scope"):
        executor.service({"service":service,"disabled":True})
    assert executor.services.read_bytes()==before


def test_resource_update_preserves_comments_and_handles_flow_yaml(tmp_path):
    for text in ["# machine config\nservices:\n- name: app # owner label\n  kind: app\n  image: image:latest\n",
                 "# machine config\nservices: [{name: app, kind: app, image: 'image:latest'}]\n"]:
        p=tmp_path/"services.yaml";p.write_text(text)
        node.set_disabled(p,"app",True,{"gpus":2,"cpus":8,"replicas":3})
        data=yaml.safe_load(p.read_text())["services"][0]
        assert (data["gpus"],data["cpus"],data["replicas"],data["disabled"])==(2,8,3,True)
        assert data["image"]=="image:latest" and "# machine config" in p.read_text()


def test_scale_down_replay_does_not_forget_replicas_after_uncertain_cleanup(executor,monkeypatch):
    service=yaml.safe_load(executor.services.read_text())["services"][0];service["replicas"]=1
    stop=Mock();monkeypatch.setattr(reconcile,"stop",stop)
    monkeypatch.setattr(actors,"clean_after",Mock());monkeypatch.setattr(node.ray,"get",lambda *a,**k:None)
    monkeypatch.setattr(node,"container_gone",lambda _:False)
    with pytest.raises(ValueError,match="uncertain"):
        executor.service({"service":service,"disabled":True,"scale_id":"resize"})
    assert yaml.safe_load(executor.services.read_text())["services"][0]["replicas"]==2
    stop.reset_mock();monkeypatch.setattr(node,"container_gone",lambda _:True)
    assert executor.service({"service":service,"disabled":True,"scale_id":"resize"})["cleaned"]
    assert [c.args[0] for c in stop.call_args_list]==["proof-0","proof-1"]
    assert yaml.safe_load(executor.services.read_text())["services"][0]["replicas"]==1


SERVICES = """# machine config
cluster: {log_dir: /tmp}
services:
# the GOAT worker
- name: goat
  kind: ziren-gpu
  env:
    X: "1"

# ethproofs: retired
- name: eth
  kind: ziren-gpu   # owner label
- {name: flow, kind: app}
# trailing note
"""


def test_remove_entry_keeps_every_other_line(tmp_path):
    p=tmp_path/"services.yaml";p.write_text(SERVICES)
    assert node.remove_entry(p,"services","eth")
    text=p.read_text()
    assert [s["name"] for s in yaml.safe_load(text)["services"]]==["goat","flow"]
    assert "ethproofs: retired" not in text and "owner label" not in text
    assert "# machine config" in text and "# the GOAT worker" in text and "# trailing note" in text
    assert len(list(tmp_path.glob("services.yaml.removed-*")))==1
    assert node.remove_entry(p,"services","flow")
    assert node.remove_entry(p,"services","goat")
    assert yaml.safe_load(p.read_text())["services"]==[]
    assert "# trailing note" in p.read_text()
    assert not node.remove_entry(p,"services","goat")


def test_remove_entry_drops_one_batch(tmp_path):
    p=tmp_path/"batches.yaml"
    p.write_text("batches:\n  trainer:\n    image: a\n  # retired\n  old:\n    image: b\n    allow: [/srv]\n  last: {image: c}\n")
    assert node.remove_entry(p,"batches","old")
    assert sorted(yaml.safe_load(p.read_text())["batches"])==["last","trainer"] and "retired" not in p.read_text()
    assert node.remove_entry(p,"batches","last") and node.remove_entry(p,"batches","trainer")
    assert yaml.safe_load(p.read_text())["batches"]=={}


def test_remove_entry_leaves_flow_sections_and_unknown_names_alone(tmp_path):
    p=tmp_path/"services.yaml";text="services: [{name: a, kind: app}]\n";p.write_text(text)
    assert not node.remove_entry(p,"services","a") and p.read_text()==text
    assert not node.remove_entry(p,"batches","a")


def test_remove_commands_clean_up_and_drop_the_entry(executor,monkeypatch):
    stop=Mock();monkeypatch.setattr(reconcile,"stop",stop)
    monkeypatch.setattr(actors,"clean_after",Mock());monkeypatch.setattr(node.ray,"get",lambda *a,**k:None)
    monkeypatch.setattr(node,"container_gone",lambda _:True)
    services=yaml.safe_load(executor.services.read_text())
    services["services"]+=[{"name":"other-ns","kind":"ziren-gpu","namespace":"zkm","proof_app":"eth/"+"b"*64},
                           {"name":"web","kind":"app","namespace":"zkm"}]
    executor.services.write_text(yaml.safe_dump(services))
    remove=lambda ident,app,ns="a"*32,name="":executor.execute({"id":ident,"operation":7,"parameters":{"namespace":ns,"namespace_name":name,"app":app}})
    remove("remove-1","eth/"+"b"*64)
    done=node.read_json(executor.journal/"remove-1.json")
    assert (done["result"],done["error"])==({"removed":["proof"]},None)
    assert [c.args[0] for c in stop.call_args_list]==["proof-0","proof-1"]
    remove("remove-2","eth/"+"b"*64)
    assert node.read_json(executor.journal/"remove-2.json")["result"]=={"removed":[]}
    remove("remove-3","web","1"*32,"zkm")
    assert node.read_json(executor.journal/"remove-3.json")["result"]=={"removed":["web"]}
    assert [s["name"] for s in yaml.safe_load(executor.services.read_text())["services"]]==["other-ns"]
    (executor.services.parent/"batches.yaml").write_text("batches:\n  old: {image: a}\n  kept: {image: b}\n")
    executor.execute({"id":"remove-4","operation":8,"parameters":{"name":"old"}})
    assert node.read_json(executor.journal/"remove-4.json")["result"]=={"removed":True}
    assert list(yaml.safe_load((executor.services.parent/"batches.yaml").read_text())["batches"])==["kept"]


def test_a_service_whose_cleanup_is_uncertain_stays_configured(executor,monkeypatch):
    monkeypatch.setattr(reconcile,"stop",Mock());monkeypatch.setattr(actors,"clean_after",Mock())
    monkeypatch.setattr(node.ray,"get",lambda *a,**k:None);monkeypatch.setattr(node,"container_gone",lambda _:False)
    executor.execute({"id":"remove-1","operation":7,"parameters":{"namespace":"a"*32,"app":"eth/"+"b"*64}})
    assert "uncertain" in node.read_json(executor.journal/"remove-1.json")["error"]
    assert [s["name"] for s in yaml.safe_load(executor.services.read_text())["services"]]==["proof"]
