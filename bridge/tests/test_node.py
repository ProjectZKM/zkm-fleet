"""Local execution fences are independent of user authentication and admission."""
from unittest.mock import Mock
import pytest
import yaml

from fleet_bridge import actors, node, reconcile, jobs
from fleet_bridge.model import Batch, JobSpec, JobState, JobStatus


@pytest.fixture
def executor(tmp_path, monkeypatch):
    (tmp_path / "batches.yaml").write_text("batches: {}\n")
    (tmp_path / "apps.yaml").write_text(yaml.safe_dump({"apps": [{
        "name": "proof", "kind": "ziren-gpu", "namespace": "a"*32,
        "app": "eth", "elf_id": "b"*64, "disabled": True, "replicas": 2,
    }]}))
    e=node.NodeExecutor(str(tmp_path/"apps.yaml"),str(tmp_path/"batches.yaml"),str(tmp_path/"jobs"))
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


def test_stop_removes_every_replica_and_writes_nothing(executor,monkeypatch):
    cfg=yaml.safe_load(executor.apps_file.read_text())
    before=executor.apps_file.read_bytes()
    stop=Mock();clean=Mock();monkeypatch.setattr(reconcile,"stop",stop)
    monkeypatch.setattr(reconcile,"running",lambda:{"proof-2":"left over","other-0":"another app"})
    monkeypatch.setattr(actors,"clean_after",clean);monkeypatch.setattr(node.ray,"get",lambda *a,**k:None)
    monkeypatch.setattr(node,"container_gone",lambda _:True)
    result=executor.set_app({"app":cfg["apps"][0],"disabled":True})
    assert result["cleaned"]
    assert [c.args[0] for c in stop.call_args_list]==["proof-0","proof-1","proof-2"]
    assert clean.remote.call_count==3
    assert executor.apps_file.read_bytes()==before


def test_stop_failure_does_not_confirm_cleanup(executor,monkeypatch):
    app=yaml.safe_load(executor.apps_file.read_text())["apps"][0]
    monkeypatch.setattr(reconcile,"stop",lambda _:None);monkeypatch.setattr(reconcile,"running",lambda:{})
    monkeypatch.setattr(actors,"clean_after",Mock());monkeypatch.setattr(node.ray,"get",lambda *a,**k:None)
    monkeypatch.setattr(node,"container_gone",lambda _:False)
    with pytest.raises(ValueError,match="uncertain"):
        executor.set_app({"app":app,"disabled":True})
    assert yaml.safe_load(executor.apps_file.read_text())["apps"][0]["disabled"]


def test_start_uses_local_executor_and_no_admin_token(executor,monkeypatch):
    app=yaml.safe_load(executor.apps_file.read_text())["apps"][0]
    monkeypatch.setattr(reconcile,"running",lambda:{})
    start=Mock();monkeypatch.setattr(reconcile,"start",start)
    with pytest.raises(ValueError,match="disabled in apps.yaml"):
        executor.set_app({"app":app,"disabled":False})
    node.set_fields(executor.apps_file,"apps","proof",{"disabled":False})
    assert executor.set_app({"app":app,"disabled":False})["ready"]
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
        executor.set_app({"app":{},"disabled":False})


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


def test_healthy_proof_app_is_ready_and_enable_preserves_actors(executor, monkeypatch):
    from fleet_bridge.model import ReplicaHealth
    cfg=yaml.safe_load(executor.apps_file.read_text())
    cfg['apps'][0]['disabled']=False
    executor.apps_file.write_text(yaml.safe_dump(cfg))
    specs=reconcile.replica_specs(reconcile.load_apps(cfg))
    monkeypatch.setattr(reconcile,'running',lambda:{name:name for name in specs})
    monkeypatch.setattr(reconcile,'health_of',lambda name, **kwargs:ReplicaHealth(name=name,spec=actors.spec_hash(specs[name]),gpu_ready=True))
    start,stop=Mock(),Mock()
    monkeypatch.setattr(reconcile,'start',start);monkeypatch.setattr(reconcile,'stop',stop)
    monkeypatch.setattr(node.ray,'cluster_resources',lambda:{'GPU':8,'CPU':48})
    monkeypatch.setattr(node.ray,'available_resources',lambda:{'GPU':4,'CPU':16})
    assert executor.report()['apps'][0]['ready'] is True
    assert executor.set_app({'app':cfg['apps'][0],'disabled':False})['ready']
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
    assert not report['apps'][0]['ready']
    assert report['results'][0]['result']['cleaned']
    assert all(call.kwargs == {'timeout_s': 5} for call in health.call_args_list)


def test_start_takes_resources_from_apps_yaml_and_stops_extra_replicas(executor,monkeypatch):
    app=yaml.safe_load(executor.apps_file.read_text())["apps"][0]
    node.set_fields(executor.apps_file,"apps","proof",{"disabled":False,"gpus":2,"cpus":8,"replicas":1})
    stop=Mock();monkeypatch.setattr(reconcile,"stop",stop)
    monkeypatch.setattr(reconcile,"running",lambda:{"proof-1":"outside the new count"})
    monkeypatch.setattr(actors,"clean_after",Mock());monkeypatch.setattr(node.ray,"get",lambda *a,**k:None)
    monkeypatch.setattr(node,"container_gone",lambda _:True)
    start=Mock();monkeypatch.setattr(reconcile,"start",start)
    assert executor.set_app({"app":app,"disabled":False,"scale_id":"resize"})["ready"]
    assert [c.args[0] for c in stop.call_args_list]==["proof-1"]
    replica=start.call_args.args[0]
    assert (start.call_count,replica.name,replica.app.gpus,replica.app.cpus)==(1,"proof-0",2,8)


@pytest.mark.parametrize("field,value",[("namespace","another"),("app","other-app"),("elf_id","c"*64),("tag","other"),("kind","app")])
def test_scale_cannot_change_machine_app_scope(executor,field,value):
    before=executor.apps_file.read_bytes()
    app=yaml.safe_load(before)["apps"][0];app[field]=value
    with pytest.raises(ValueError,match="scope"):
        executor.set_app({"app":app,"disabled":True})
    assert executor.apps_file.read_bytes()==before


def test_resource_update_preserves_comments_and_handles_flow_yaml(tmp_path):
    for text in ["# machine config\napps:\n- name: app # owner label\n  kind: app\n  image: image:latest\n",
                 "# machine config\napps: [{name: app, kind: app, image: 'image:latest'}]\n"]:
        p=tmp_path/"apps.yaml";p.write_text(text)
        node.set_fields(p,"apps","app",{"disabled":True,"gpus":2,"cpus":8,"replicas":3})
        data=yaml.safe_load(p.read_text())["apps"][0]
        assert (data["gpus"],data["cpus"],data["replicas"],data["disabled"])==(2,8,3,True)
        assert data["image"]=="image:latest" and "# machine config" in p.read_text()




APPS = """# machine config
cluster: {log_dir: /tmp}
apps:
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
    p=tmp_path/"apps.yaml";p.write_text(APPS)
    assert node.remove_entry(p,"apps","eth")
    text=p.read_text()
    assert [s["name"] for s in yaml.safe_load(text)["apps"]]==["goat","flow"]
    assert "ethproofs: retired" not in text and "owner label" not in text
    assert "# machine config" in text and "# the GOAT worker" in text and "# trailing note" in text
    assert len(list(tmp_path.glob("apps.yaml.removed-*")))==1
    assert node.remove_entry(p,"apps","flow")
    assert node.remove_entry(p,"apps","goat")
    assert yaml.safe_load(p.read_text())["apps"]==[]
    assert "# trailing note" in p.read_text()
    assert not node.remove_entry(p,"apps","goat")


def test_remove_entry_drops_one_batch(tmp_path):
    p=tmp_path/"batches.yaml"
    p.write_text("batches:\n  trainer:\n    image: a\n  # retired\n  old:\n    image: b\n    allow: [/srv]\n  last: {image: c}\n")
    assert node.remove_entry(p,"batches","old")
    assert sorted(yaml.safe_load(p.read_text())["batches"])==["last","trainer"] and "retired" not in p.read_text()
    assert node.remove_entry(p,"batches","last") and node.remove_entry(p,"batches","trainer")
    assert yaml.safe_load(p.read_text())["batches"]=={}


def test_remove_entry_leaves_flow_sections_and_unknown_names_alone(tmp_path):
    p=tmp_path/"apps.yaml";text="apps: [{name: a, kind: app}]\n";p.write_text(text)
    assert not node.remove_entry(p,"apps","a") and p.read_text()==text
    assert not node.remove_entry(p,"batches","a")


def app_message(executor, ident, change, app=None, batch=None, ns="a"*32, ns_name=""):
    target = {"batch": {"name": batch}} if batch else {"app": {"namespace": ns, "namespace_name": ns_name, "name": app}}
    executor.execute({"id": ident, "operation": 7, "parameters": {**target, **change}})
    done = node.read_json(executor.journal/f"{ident}.json")
    return done["result"], done["error"]


def test_app_removal_cleans_up_and_drops_the_entry(executor,monkeypatch):
    stop=Mock();monkeypatch.setattr(reconcile,"stop",stop)
    monkeypatch.setattr(actors,"clean_after",Mock());monkeypatch.setattr(node.ray,"get",lambda *a,**k:None)
    monkeypatch.setattr(node,"container_gone",lambda _:True)
    apps=yaml.safe_load(executor.apps_file.read_text())
    apps["apps"]+=[{"name":"other-ns","kind":"ziren-gpu","namespace":"zkm","app":"eth","elf_id":"b"*64},
                           {"name":"web","kind":"app","namespace":"zkm"}]
    executor.apps_file.write_text(yaml.safe_dump(apps))
    assert app_message(executor,"remove-1",{"remove":True},app="eth")==({"removed":["proof"]},None)
    assert [c.args[0] for c in stop.call_args_list]==["proof-0","proof-1"]
    assert app_message(executor,"remove-2",{"remove":True},app="eth")==({"removed":[]},None)
    assert app_message(executor,"remove-3",{"remove":True},app="web",ns="1"*32,ns_name="zkm")==({"removed":["web"]},None)
    assert [s["name"] for s in yaml.safe_load(executor.apps_file.read_text())["apps"]]==["other-ns"]
    (executor.apps_file.parent/"batches.yaml").write_text("batches:\n  old: {image: a}\n  kept: {image: b}\n")
    assert app_message(executor,"remove-4",{"remove":True},batch="old")==({"removed":["old"]},None)
    assert list(yaml.safe_load((executor.apps_file.parent/"batches.yaml").read_text())["batches"])==["kept"]


def test_an_app_whose_cleanup_is_uncertain_stays_configured(executor,monkeypatch):
    monkeypatch.setattr(reconcile,"stop",Mock());monkeypatch.setattr(actors,"clean_after",Mock())
    monkeypatch.setattr(node.ray,"get",lambda *a,**k:None);monkeypatch.setattr(node,"container_gone",lambda _:False)
    result,error=app_message(executor,"remove-1",{"remove":True},app="eth")
    assert "uncertain" in error
    assert [s["name"] for s in yaml.safe_load(executor.apps_file.read_text())["apps"]]==["proof"]


SETTINGS = """# machine config
apps:
# the web app
- name: web # owner label
  kind: app
  namespace: zkm
  image: web:1
  env:
    A: "1"
    B: "2"
  replicas: 1
- {name: other, kind: app, namespace: zkm, image: other:1}
"""


def test_app_settings_are_written_without_touching_other_lines(executor):
    executor.apps_file.write_text(SETTINGS)
    change={"set":{"image":"web:2","env":{"A":"3"},"shm_gb":8}}
    assert app_message(executor,"set-1",change,app="web",ns="1"*32,ns_name="zkm")==({"updated":["web"]},None)
    text=executor.apps_file.read_text()
    web,other=yaml.safe_load(text)["apps"]
    assert (web["image"],web["env"],web["shm_gb"],web["replicas"])==("web:2",{"A":"3"},8,1)
    assert other=={"name":"other","kind":"app","namespace":"zkm","image":"other:1"}
    assert "# machine config" in text and "# the web app" in text and "owner label" in text
    assert app_message(executor,"set-2",{"set":{"image":"x:1"}},app="missing",ns="zkm")==({"updated":[]},None)


def test_invalid_app_settings_leave_the_configuration_alone(executor):
    executor.apps_file.write_text(SETTINGS)
    result,error=app_message(executor,"set-1",{"set":{"replicas":"many"}},app="web",ns="zkm")
    assert error and executor.apps_file.read_text()==SETTINGS


def test_batch_settings_are_written_and_validated(executor):
    path=executor.apps_file.parent/"batches.yaml"
    path.write_text("batches:\n  # trainer\n  train:\n    image: t:1\n    allow: [/srv]\n  other: {image: o:1}\n")
    assert app_message(executor,"set-1",{"set":{"image":"t:2","allow":["/srv","/data"]}},batch="train")==({"updated":["train"]},None)
    batches=yaml.safe_load(path.read_text())["batches"]
    assert batches["train"]=={"image":"t:2","allow":["/srv","/data"]} and batches["other"]=={"image":"o:1"}
    assert "# trainer" in path.read_text()
    before=path.read_text()
    result,error=app_message(executor,"set-2",{"set":{"priority":"high"}},batch="train")
    assert error and path.read_text()==before
    assert app_message(executor,"set-3",{"set":{"image":"x"}},batch="missing")==({"updated":[]},None)


def test_a_stopped_entry_gets_its_configuration_by_name(executor):
    executor.apps_file.write_text(SETTINGS)
    executor.execute({"id":"set-1","operation":7,"parameters":{"entry":{"name":"web"},"set":{"disabled":True,"replicas":2,"image":"web:3"}}})
    done=node.read_json(executor.journal/"set-1.json")
    assert (done["result"],done["error"])==({"updated":["web"]},None)
    web=yaml.safe_load(executor.apps_file.read_text())["apps"][0]
    assert (web["disabled"],web["replicas"],web["image"])==(True,2,"web:3")
    executor.execute({"id":"set-2","operation":7,"parameters":{"entry":{"name":"missing"},"set":{"disabled":True}}})
    assert node.read_json(executor.journal/"set-2.json")["result"]=={"updated":[]}


@pytest.mark.parametrize("mount", ["/run", "/var/run", "/dev", "/proc", "/sys", "/var/lib/docker", "/var/lib/containerd"])
def test_batch_launch_refuses_runtime_mounts_even_when_policy_allows_them(executor, monkeypatch, mount):
    p = command()["parameters"]
    p.update({"owner": "owner", "namespace": "a" * 32, "batch": "training", "submitted_at": 1,
              "policy": {"image": "test", "namespace": "a" * 32, "allow": [mount]},
              "request": {"command": ["true"], "gpus": 0, "cpus": 1, "mount": [mount]}})
    launch = Mock()
    monkeypatch.setattr(jobs, "launch", launch)
    with pytest.raises(ValueError, match="mount outside allowed roots"):
        executor.launch(p)
    launch.assert_not_called()


def test_batch_launch_refuses_a_socket_under_an_allowed_storage_root(executor, monkeypatch, tmp_path_factory):
    import socket
    storage = tmp_path_factory.mktemp("batch-storage")
    path = storage / "daemon.sock"
    p = command()["parameters"]
    p.update({"owner": "owner", "namespace": "a" * 32, "batch": "training", "submitted_at": 1,
              "policy": {"image": "test", "namespace": "a" * 32, "allow": [str(storage)]},
              "request": {"command": ["true"], "gpus": 0, "cpus": 1, "mount": [str(path)]}})
    launch = Mock()
    monkeypatch.setattr(jobs, "launch", launch)
    with socket.socket(socket.AF_UNIX) as server:
        server.bind(str(path))
        with pytest.raises(ValueError, match="regular files or directories"):
            executor.launch(p)
    launch.assert_not_called()


@pytest.mark.parametrize("kind", ["file", "directory"])
def test_batch_launch_keeps_approved_regular_storage_usable(executor, monkeypatch, tmp_path_factory, kind):
    storage = tmp_path_factory.mktemp("approved-storage")
    path = storage / "input"
    if kind == "file":
        path.write_text("input data")
    else:
        path.mkdir()
    p = command()["parameters"]
    p.update({"owner": "owner", "namespace": "a" * 32, "batch": "training", "submitted_at": 1,
              "policy": {"image": "test", "namespace": "a" * 32, "allow": [str(storage)]},
              "request": {"command": ["true"], "gpus": 0, "cpus": 1, "mount": [str(path) + ":ro"]}})
    monkeypatch.setattr(jobs, "prepare_launch", lambda _: True)
    launch = Mock()
    monkeypatch.setattr(jobs, "launch", launch)
    executor.launch(p)
    assert launch.call_args.args[0].mounts[0].path == str(path)


@pytest.mark.parametrize("text", [
    APPS,
    "# machine config\napps:\n- name: only\n  kind: app\n  image: a:1\n# trailing note\n",
    "# machine config\napps: []\n",
])
def test_append_entry_keeps_every_other_line(tmp_path, text):
    p=tmp_path/"apps.yaml";p.write_text(text)
    node.append_entry(p,"apps",{"name":"new","kind":"app","namespace":"zkm","disabled":True,"image":"web:1"})
    apps=yaml.safe_load(p.read_text())["apps"]
    assert apps[-1]=={"name":"new","kind":"app","namespace":"zkm","disabled":True,"image":"web:1"}
    assert [s["name"] for s in apps[:-1]]==[s["name"] for s in yaml.safe_load(text)["apps"] or []]
    for line in text.splitlines():
        if line.startswith("#"):
            assert line in p.read_text()


def test_create_adds_a_disabled_app_and_refuses_a_taken_name(executor):
    entry={"name":"web","kind":"app","namespace":"a"*32,"disabled":True,"image":"web:1","port_base":50100,"gpus":1}
    assert app_message(executor,"create-1",{"create":entry},app="web",ns="a"*32)==({"created":["web"]},None)
    assert yaml.safe_load(executor.apps_file.read_text())["apps"][-1]==entry
    result,error=app_message(executor,"create-2",{"create":entry},app="web",ns="a"*32)
    assert error and "already configured" in str(error)


def test_create_adds_a_prover_and_refuses_an_invalid_entry(executor):
    prover={"name":"proofs","kind":"ziren-gpu","namespace":"a"*32,"disabled":True,"image":"ziren-gpu:1","elf_id":"b"*64}
    assert app_message(executor,"create-1",{"create":prover},app="proofs",ns="a"*32)==({"created":["proofs"]},None)
    before=executor.apps_file.read_text()
    result,error=app_message(executor,"create-2",{"create":{**prover,"name":"other","elf_id":""}},app="other",ns="a"*32)
    assert error and "elf_id" in str(error) and executor.apps_file.read_text()==before


def test_create_refuses_an_entry_whose_files_are_not_on_this_machine(executor, tmp_path):
    worker = tmp_path/"eth-proofs"; worker.write_text(""); state = tmp_path/"state"; state.mkdir()
    entry = {"name":"eth","kind":"ziren-gpu","namespace":"a"*32,"disabled":True,"image":"ziren-gpu:1","elf_id":"b"*64,
             "machine":"m1","worker_command":[str(worker)],"worker_env_file":str(tmp_path/"worker.env"),"proof_storage_root":str(state),
             "volumes":[f"{tmp_path/'models'}:ro"]}
    before = executor.apps_file.read_text()
    result, error = app_message(executor,"create-1",{"create":entry},app="eth",ns="a"*32)
    assert error and str(tmp_path/"worker.env") in str(error) and str(tmp_path/"models") in str(error)
    assert str(worker) not in str(error) and executor.apps_file.read_text() == before
    (tmp_path/"worker.env").write_text(""); (tmp_path/"models").mkdir()
    assert app_message(executor,"create-2",{"create":entry},app="eth",ns="a"*32) == ({"created":["eth"]},None)
