use super::*;
use crate::{config::SchedulerConfig, quota::Quota, store::MemoryStore};
use axum::body::Body;
use fleet_proto::model::v1::{JobReport, CommandResult, ScaleRequest};
use sha2::Digest;

/// The administrator's token the tests' scheduler has.
const ADMIN: &str = "adminadminadminadminadminadminadminadminadminadminadminadminadmin1";

/// The proof app the tests' provers serve, and its program.
const PROOF: &str = "proof";

fn proof_elf() -> String {
    "a".repeat(64)
}

/// Registers the tests' proof app (program `elf`) in the namespace.
async fn register(c: &WorkloadController, ns: &str, name: &str, elf: &str) {
    let proof = fleet_proto::model::v1::AppProof { elf_id: elf.into(), ..Default::default() };
    c.fleet.add_app(ns, name, 0, &proof).await.unwrap();
}

/// A request through the whole API router: authorized by its token, then handled.
async fn serve(c: &Arc<WorkloadController>, request: Request) -> Response {
    use tower::ServiceExt;
    crate::http::router(crate::http::HttpState { fleet: c.fleet.clone(), workloads: c.clone() })
        .oneshot(request).await.unwrap()
}

async fn setup() -> (tempfile::TempDir, Arc<WorkloadController>, String) {
    let dir = tempfile::tempdir().unwrap();
    let cfg: SchedulerConfig = serde_yaml::from_str(&format!("data_dir: {}\ncluster_token_file: cluster\n",dir.path().display())).unwrap();
    let fleet = Scheduler::new(
        cfg,
        Arc::new(MemoryStore::default()),
        "cluster".into(),
    );
    fleet.load_tokens().await.unwrap();
    fleet.add_user_token(&"ad".repeat(16), &"ad".repeat(20), "admin", None,
        Some(hex::encode(sha2::Sha256::digest(ADMIN.as_bytes()))), None, None, vec!["admin".into()]).await.unwrap();
    let namespace = fleet
        .create_namespace(
            "test",
            Quota {
                gpus: Some(2.0),
                cpus: Some(8.0),
            },
        )
        .await
        .unwrap()
        .id;
    let control = Arc::new(WorkloadController::new(fleet, "cluster".into()).unwrap());
    (dir, control, namespace)
}
fn inventory(namespace: &str) -> NodeReport {
    serde_json::from_value(json!({"version":3,"machine":"machine","ready":true,"total_gpus":2,"total_cpus":8,"free_gpus":2,"free_cpus":8,"batches":{"training":{"image":"test","namespace":namespace,"priority":20,"max_gpus":2,"min_runtime_s":0}}})).unwrap()
}
fn incoming(job: &Job, state: &str, cleaned: bool) -> JobReport {
    JobReport {
        id: job.id.clone(),
        lease: job.lease.clone(),
        status: json!({"state":state,"submitted_at":1,"started_at":1}).into(),
        spec: json!({"batch":job.batch}).into(),
        cleaned,
        logs_ready: cleaned,
    }
}

#[tokio::test]
async fn apps_and_batches_have_separate_lifecycles_after_restart() {
    let (_dir, c, ns) = setup().await;
    let mut report = scale_inventory(&ns, false, true);
    report.batches.get_mut("training").unwrap()["type"] = json!("job");
    c.report("gpu", "session", report).await.unwrap();
    let mut job = submit(&c).await;
    job.status["type"] = json!("job");
    c.save("jobs", &job.id, &job).unwrap();
    let restarted = Arc::new(
        WorkloadController::new(c.fleet.clone(), "cluster".into()).unwrap());
    for path in [
        "/apps".to_string(),
        "/batches".to_string(),
        "/batches/training".to_string(),
        "/apps/model".to_string(),
        "/batches/training/jobs".to_string(),
        format!("/batches/training/jobs/{}", job.id),
    ] {
        let response = scale_call(restarted.clone(), "admin", "GET", &path, Value::Null).await;
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        let value: Value = serde_json::from_slice(
            &to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
        let rows = value.as_array().cloned().unwrap_or_else(|| vec![value]);
        assert!(!rows.is_empty(), "{path}");
        for row in rows {
            if path.starts_with("/apps") {
                assert_eq!(row["type"], "app", "{path}: {row}");
                assert!(row.get("kind").is_none() && row["spec"].is_object(), "{path}: {row}");
                assert_eq!(row["name"], "model");
            } else {
                assert_eq!(row["type"], "batch", "{path}: {row}");
            }
            assert_eq!(row["namespace"], ns);
            let one_job = path.contains("/jobs/");
            assert_eq!(row.get("request").is_some(), one_job, "only one job shows its submission: {path}: {row}");
            if one_job {
                assert_eq!((row["request"]["command"].clone(), row["request"]["gpus"].as_f64()), (json!(["true"]), Some(1.0)));
            }
        }
    }
}

#[tokio::test]
async fn app_resource_views_keep_disabled_settings_and_shared_quotas_after_restart() {
    for proof in [false, true] {
        let (_dir, c, ns) = setup().await;
        c.report("gpu", "session", scale_inventory(&ns, proof, true)).await.unwrap();
        c.fleet.set_tag_quota(&ns, "scope=default", Quota {gpus:Some(1.0),cpus:None,
                },
            ).await.unwrap();
        let app = if proof {PROOF.to_string()} else {"model".into()};
        let views = c.app_resources(&ns, &app, Some("scope=default"));
        assert_eq!(views["resources"], json!({"gpus":2.0,"cpus":4.0,"replicas":2,"nodes":["gpu"]}));
        assert_eq!(views["namespace_quota"],json!({"gpus":2.0,"cpus":8.0}));
        assert_eq!(views["tag_quota"],json!({"gpus":1.0,"cpus":null}));
        assert_eq!(views["tags"], json!(["scope=default"]));
        assert_eq!(views["images"], json!(["runner:1"]));
        assert_eq!(views["state"], "disabled");
        let restarted = WorkloadController::new(c.fleet.clone(), "cluster".into()).unwrap();
        let restored = restarted.app_resources(&ns, &app, Some("scope=default"));
        for field in ["resources", "namespace_quota", "tag_quota", "tags", "images"] {
            assert_eq!(restored[field], views[field]);
        }
        assert_eq!(restored["state"], "offline");
        if proof { register(&c, &ns, PROOF, &proof_elf()).await; }
        let app_view = |apps: &Value| apps.as_array().unwrap().iter().find(|a| a["name"] == app).cloned().unwrap_or_else(|| panic!("{app} is not listed: {apps}"));
        let listed: Value = serde_json::from_slice(&axum::body::to_bytes(
            scale_call(c.clone(), "admin", "GET", "/apps", Value::Null).await.into_body(), usize::MAX).await.unwrap()).unwrap();
        let shown = app_view(&listed);
        assert_eq!(shown[if proof { "proof" } else { "spec" }]["image"], "runner:1", "an app shows the image it runs in: {shown}");
        let mut second = scale_inventory(&ns,proof,true);
        second.machine="second-machine".into();
        second.apps[0]["image"]=json!("runner:2");
        c.report("gpu-2","second-session",second).await.unwrap();
        assert_eq!(c.app_resources(&ns,&app,None)["resources"],json!({"gpus":4.0,"cpus":8.0,"replicas":4,"nodes":["gpu","gpu-2"]}));
        assert_eq!(c.app_resources(&ns,&app,None)["images"],json!(["runner:1","runner:2"]));
        if proof {
            let listed: Value = serde_json::from_slice(&axum::body::to_bytes(
                scale_call(c.clone(), "admin", "GET", "/apps", Value::Null).await.into_body(), usize::MAX).await.unwrap()).unwrap();
            assert_eq!(app_view(&listed)["proof"]["image"], json!(["runner:1","runner:2"]), "while its deployments differ, each image");
        }
        c.fleet.set_quota(&ns,Quota {gpus:None,cpus:Some(0.0),
                },
            ).await.unwrap();
        assert_eq!(c.app_resources(&ns,&app,None)["namespace_quota"],json!({"gpus":null,"cpus":0.0}));
        assert!(c.app_resources(&ns,"unconfigured",None)["resources"].is_null());
        if !proof {
            let response = scale_call(c.clone(), "admin", "GET", "/apps", Value::Null).await;
            let apps:Value=serde_json::from_slice(&axum::body::to_bytes(response.into_body(),usize::MAX).await.unwrap(),
            ).unwrap();
            assert_eq!(apps[0]["resources"],c.app_resources(&ns,&app,None)["resources"]);
        }
    }
}

#[tokio::test]
async fn app_readiness_follows_fresh_reports_for_both_runners() {
    for proof in [false, true] {
        let (_dir, c, ns) = setup().await;
        let name = if proof { PROOF.to_string() } else { "model".into() };
        let mut report = scale_inventory(&ns, proof, false);
        report.apps[0]["ready"] = json!(false);
        c.report("gpu", "session", report).await.unwrap();
        assert_eq!(c.app_resources(&ns, &name, None)["state"], "starting");
        c.report("gpu", "session", scale_inventory(&ns, proof, false)).await.unwrap();
        let view = c.app_resources(&ns, &name, None);
        assert_eq!(view["state"], "running");
        assert_eq!(view["tags"], json!(["scope=default"]));
        c.inner.lock().workers.get_mut("gpu").unwrap().seen = now() - 31.0;
        assert_eq!(c.app_resources(&ns, &name, None)["state"], "offline");
    }
}

#[tokio::test]
async fn app_and_batch_tag_selectors_scope_visibility_and_job_counts() {
    let (_dir, c, ns) = setup().await;
    c.report("gpu", "session", scale_inventory(&ns, false, true)).await.unwrap();
    for tag in [Some("scope=default"), Some("team=research"), None] {
        let q = tag.map(|tag| [("tag".into(), tag.into())].into_iter().collect()).unwrap_or_default();
        c.submit("training", json!({"command":["true"],"gpus":0,"cpus":1}), None, &q,
        ).await.unwrap();
    }
    for (path, count) in [
        (format!("/apps?namespace={ns}&tag=scope%3Ddefault"), 1),
        (format!("/apps?namespace={ns}&tag=team%3Dresearch"), 0),
        (format!("/batches?namespace={ns}&tag=team%3Dresearch"), 1,
        ),
    ] {
        let response = scale_call(c.clone(), "admin", "GET", &path, Value::Null).await;
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        let rows: Value = serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
        assert_eq!(rows.as_array().unwrap().len(), count, "{path}: {rows}");
        if path.starts_with("/batches") { assert_eq!(rows[0]["jobs"]["total"], 1); }
    }
    let secret = "ab".repeat(32);
    c.fleet.add_user_token(&"a".repeat(32), &format!("0x{}", "b".repeat(40)), "reader",
        Some(vec!["training".into(), "model".into()]),
        Some(hex::encode(sha2::Sha256::digest(secret.as_bytes()))), Some(ns.clone()), None, Vec::new(),
        ).await.unwrap();
    let response = scale_call(c.clone(), &secret, "GET", "/batches/training", Value::Null,
    ).await;
    assert_eq!(response.status(), StatusCode::OK);
    let batch: Value = serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!((batch["type"].as_str(), batch["state"].as_str()), (Some("batch"), Some("enabled")),
        "a batch has the app record's shape: {batch}");
    assert_eq!((batch["jobs"]["total"].as_u64(), batch["jobs"]["active"].as_u64()), (Some(1), Some(1)));
    assert_eq!((batch["batch"]["image"].as_str(), batch["batch"]["max_gpus"].as_u64()), (Some("test"), Some(2)));
    assert!(batch["batch"].get("name").is_none() && batch.get("persistent").is_none() && batch.get("removed").is_none(), "{batch}");
    assert_eq!(scale_call(c.clone(), &secret, "GET", "/apps/model", Value::Null).await.status(), StatusCode::NOT_FOUND);
    assert_eq!(scale_call(c, "admin", "GET", &format!("/apps?namespace={ns}&tag=bare"), Value::Null).await.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn bare_tags_are_rejected_even_if_present_in_stored_quotas() {
    let (_dir, c, ns) = setup().await;
    assert!(c.fleet.set_tag_quota(&ns, "default", Quota::default()).await.is_err());
    c.fleet.store.set_tag_quota(&ns, "default", &Quota::default()).await.unwrap();
    c.fleet.load_tokens().await.unwrap();
    assert!(c.fleet.set_tag_quota("test", "default", Quota::default()).await.is_err());
    assert!(c.fleet.set_tag_quota(&ns, "research", Quota::default()).await.is_err());
    assert!(c.fleet.set_tag_quota(&ns, "team=research", Quota::default()).await.is_ok());
    c.report("gpu", "session", scale_inventory(&ns, false, true)).await.unwrap();
    let mut invalid = scale_inventory(&ns, false, true);
    invalid.apps[0]["tag"] = json!("bare");
    assert!(c.report("gpu", "session", invalid).await.is_err());
}

#[tokio::test]
async fn proof_replica_location_is_diagnostic_and_resolves_without_a_live_actor() {
    use crate::store::{ProofJob, ProofJobState};
    use fleet_proto::fleet::v1::ProofKind;
    let (_dir,c,ns)=setup().await;
    c.report("gpu", "session", scale_inventory(&ns,true,true)).await.unwrap();
    let mut job=ProofJob{inputs:None,outputs:None,id:"proof".into(),app:PROOF.into(),owner:"b".repeat(40),namespace:ns.clone(),tag:String::new(),client_id:"proof".into(),elf_id:"a".repeat(64),kind:ProofKind::Compressed as i32,priority:0,state:ProofJobState::Running as i32,
        fail_status: None,error:String::new(),node:Some("model-0".into()),attempts:1,failed_on:vec!["model-1".into()],cycles:0,prove_ms:0,created_ms:1,started_ms:Some(1),finished_ms:None,
    };
    for control in [&*c, &WorkloadController::new(c.fleet.clone(), "cluster".into()).unwrap(),
    ] {
        let view=control.proof_job_value(&job);
        assert_eq!(view["node"],"gpu");
        assert_eq!(view["failed_on"],json!(["gpu"]));
        assert_eq!(view["diagnostics"]["execution_replica"],"model-0");
    }
    let mut second = scale_inventory(&ns,true,true);
    second.machine="second-machine".into();
    c.report("gpu-2","second-session",second).await.unwrap();
    assert!(c.proof_job_value(&job)["node"].is_null(),"ambiguous replica names must not guess a machine");
    // Storage metadata resolves historical jobs even after their app is removed.
    job.outputs=Some(fleet_proto::fleet::v1::ProofOutputs{machine:"machine".into(),..Default::default()});
    c.inner.lock().deployments.clear();
    assert_eq!(c.proof_job_value(&job)["node"],"gpu");
    job.outputs=None;
    assert!(c.proof_job_value(&job)["node"].is_null());
    assert_eq!(job.node.as_deref(),Some("model-0"),"presentation must not change execution routing");
}

#[tokio::test]
async fn lifecycle_routes_reject_cross_type_access_and_batch_submissions_to_apps() {
    let (_dir, c, ns) = setup().await;
    c.report("gpu", "session", scale_inventory(&ns, false, true)).await.unwrap();
    for (method, path, body, status) in [
        ("GET", "/apps/training", Value::Null, StatusCode::NOT_FOUND),
        ("GET", "/batches/model", Value::Null, StatusCode::NOT_FOUND,
        ),
        ("PUT", "/apps/training/disabled", Value::Null, StatusCode::NOT_FOUND,
        ),
        ("PUT", "/batches/model/priority", json!({"priority":100}), StatusCode::NOT_FOUND,
        ),
        ("DELETE", "/batches/model", Value::Null, StatusCode::NOT_FOUND,
        ),
        ("PUT", "/batches/model/scale", json!({"gpus":2}), StatusCode::BAD_REQUEST,
        ),
        ("POST", "/apps", json!({"name":"finite","image":"test","namespace":ns}), StatusCode::BAD_REQUEST,
        ),
        ("POST", "/batches/model/jobs", json!({"command":["true"]}), StatusCode::CONFLICT,
        ),
    ] {
        let response = scale_call(c.clone(), "admin", method, path, body).await;
        assert_eq!(response.status(), status, "{method} {path}");
    }
    assert!(c.inner.lock().jobs.is_empty());
    assert!(c.inner.lock().commands.is_empty());
    assert_eq!(c.inner.lock().apps["model"].spec["priority"], 0);
}

#[tokio::test]
async fn batch_policy_and_job_lifecycle_use_batch_routes() {
    let (_dir, c, ns) = setup().await;
    c.report("gpu", "session", inventory(&ns)).await.unwrap();
    let response = scale_call(c.clone(), "admin", "PUT", "/batches/training/priority",
        json!({"priority":31}),
    ).await;
    assert_eq!(response.status(), StatusCode::OK);
    let response = scale_call(c.clone(), "admin", "POST", "/batches/training/jobs",
        json!({"command":["true"],"gpus":1}),
    ).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let job: Value = serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(job["type"], "batch");
    assert_eq!(job["namespace"], ns);
    assert_eq!(job["priority"], 31);
    assert_eq!(scale_call(c.clone(), "admin", "DELETE", "/batches/training", Value::Null)
        .await.status(), StatusCode::CONFLICT);
    assert_eq!(scale_call(c.clone(), "admin", "PUT", "/batches/training/disabled", Value::Null)
        .await.status(), StatusCode::OK);
    assert_eq!(scale_call(c.clone(), "admin", "POST", "/batches/training/jobs",
        json!({"command":["true"]})).await.status(), StatusCode::CONFLICT);
    assert_eq!(c.job(text(&job,"id"), None, &BTreeMap::new()).unwrap().state(), "waiting_quota");
    assert_eq!(scale_call(c.clone(), "admin", "POST", &format!("/batches/training/jobs/{}/cancel", text(&job,"id")), Value::Null)
        .await.status(), StatusCode::OK);
    assert_eq!(scale_call(c.clone(), "admin", "DELETE", "/batches/training", Value::Null)
        .await.status(), StatusCode::OK);
    assert_eq!(scale_call(c.clone(), "admin", "GET", &format!("/batches/training/jobs/{}", text(&job,"id")), Value::Null)
        .await.status(), StatusCode::OK);
}

#[tokio::test]
async fn completed_job_logs_finish_without_output_artifacts() {
    let (_dir, c, ns) = setup().await;
    c.report("gpu", "session", inventory(&ns)).await.unwrap();
    let job = submit(&c).await;
    c.dispatch().await.unwrap();
    let job = c.job(&job.id, None, &BTreeMap::new()).unwrap();
    let mut report = inventory(&ns);
    let mut finished = incoming(&job, "succeeded", true);
    finished.logs_ready = false;
    report.jobs = vec![finished];
    c.report("gpu", "session", report).await.unwrap();
    let job = c.job(&job.id, None, &BTreeMap::new()).unwrap();
    let path = c.dir.join("artifacts").join(&job.id).join(&job.lease);
    atomic(&path.join("logs"), b"app wrote output to owner storage\n").unwrap();
    let read = |r: Response| async move {
        serde_json::from_slice::<Value>(&to_bytes(r.into_body(), 1 << 20).await.unwrap()).unwrap()
    };
    let first = read(log_response(&c, &job, &BTreeMap::new()).await.unwrap()).await;
    assert_eq!(first["done"], false);
    let mut report = inventory(&ns);
    report.jobs = vec![incoming(&job, "succeeded", true)];
    c.report("gpu", "session", report).await.unwrap();
    let job = c.job(&job.id, None, &BTreeMap::new()).unwrap();
    let last = read(log_response(&c, &job, &BTreeMap::new()).await.unwrap()).await;
    assert_eq!(last["done"], true);
    assert_eq!(last["data"], "app wrote output to owner storage\n");
    assert!(!job.quota_held);
    assert!(!path.join("out.tar.gz").exists());
}
async fn submit(control: &WorkloadController) -> Job {
    let job = control
        .submit(
            "training", json!({"command":["true"],"gpus":1,"cpus":1}),
            None,
            &BTreeMap::new(),
        )
        .await
        .unwrap();
    control
        .job(text(&job, "id"), None, &BTreeMap::new())
        .unwrap()
}
#[tokio::test]
async fn commands_survive_restart_and_completed_launch_is_not_replayed() {
    let (_dir, c, ns) = setup().await;
    c.report("gpu", "session", inventory(&ns)).await.unwrap();
    let job = submit(&c).await;
    c.dispatch().await.unwrap();
    let assigned = c.job(&job.id, None, &BTreeMap::new()).unwrap();
    let command = c.inner.lock().commands.values().next().unwrap().clone();
    let restarted = WorkloadController::new(c.fleet.clone(), "cluster".into()).unwrap();
    restarted.dispatch().await.unwrap();
    assert_eq!(restarted.inner.lock().commands.len(), 1);
    assert_eq!(
        restarted
            .job(&job.id, None, &BTreeMap::new())
            .unwrap()
            .lease,
        assigned.lease
    );
    let mut report = inventory(&ns);
    report.results.push(CommandResult {
        id: command.id.clone(),
        result: json!({}).into(),
        error: None,
    });
    restarted.report("gpu", "session", report).await.unwrap();
    restarted.dispatch().await.unwrap();
    assert_eq!(restarted.inner.lock().commands.len(), 1);
    assert!(restarted.inner.lock().commands[&command.id].done);
}
#[tokio::test]
async fn scheduling_passes_see_only_open_records_and_unchanged_reports_write_nothing() {
    use std::os::unix::fs::MetadataExt;
    let (_dir, c, ns) = setup().await;
    c.report("gpu", "session", inventory(&ns)).await.unwrap();
    let job = submit(&c).await;
    c.dispatch().await.unwrap();
    let job = c.job(&job.id, None, &BTreeMap::new()).unwrap();
    let launch = c.inner.lock().commands.values().next().unwrap().id.clone();
    let open = |c: &WorkloadController| {
        let i = c.inner.lock();
        (i.jobs.open().count(), i.commands.open().count())
    };
    assert_eq!(open(&c), (1, 1));

    // The node runs the launch; the job is open while it runs and holds its quota.
    let mut report = inventory(&ns);
    report.results.push(CommandResult { id: launch.clone(), result: json!({}).into(), error: None });
    report.jobs.push(incoming(&job, "running", false));
    c.report("gpu", "session", report.clone()).await.unwrap();
    assert_eq!(open(&c), (1, 0));

    // Each heartbeat reports the job again: unchanged, its record is not written again.
    let path = c.dir.join("jobs").join(format!("{}.json", job.id));
    let written = fs::metadata(&path).unwrap().ino();
    report.results.clear();
    c.report("gpu", "session", report.clone()).await.unwrap();
    c.dispatch().await.unwrap();
    assert_eq!(fs::metadata(&path).unwrap().ino(), written);
    assert_eq!(c.fleet.allocations(&ns).len(), 1);

    // Finished and cleaned up, it is history: kept, and no pass looks at it, before or after a restart.
    report.jobs[0] = incoming(&job, "succeeded", true);
    c.report("gpu", "session", report).await.unwrap();
    assert_eq!(c.job(&job.id, None, &BTreeMap::new()).unwrap().state(), "succeeded");
    assert_eq!(open(&c), (0, 0));
    let restarted = WorkloadController::new(c.fleet.clone(), "cluster".into()).unwrap();
    assert_eq!(open(&restarted), (0, 0));
    let held = |c: &WorkloadController| {
        let i = c.inner.lock();
        (i.jobs.len(), i.commands.len())
    };
    assert_eq!(held(&restarted), (1, 1));
    restarted.dispatch().await.unwrap();
    assert!(restarted.inner.lock().commands[&launch].done, "a finished job's launch is not sent again");
    assert_eq!(restarted.inner.lock().commands.len(), 1);
}
#[tokio::test]
async fn a_job_its_quota_cannot_take_waits_without_rewriting_its_record() {
    use std::os::unix::fs::MetadataExt;
    let (_dir, c, ns) = setup().await;
    let mut report = inventory(&ns);
    (report.total_gpus, report.free_gpus) = (4.0, 4.0);
    c.report("gpu", "session", report).await.unwrap();
    let jobs = [submit(&c).await, submit(&c).await, submit(&c).await];
    c.dispatch().await.unwrap();
    let placed = |c: &WorkloadController| {
        jobs.iter().filter(|j| c.job(&j.id, None, &BTreeMap::new()).unwrap().worker.is_some()).count()
    };
    assert_eq!(placed(&c), 2, "the namespace's 2-GPU quota takes two of the 1-GPU jobs");
    let waiting = jobs.iter().map(|j| c.job(&j.id, None, &BTreeMap::new()).unwrap()).find(|j| j.worker.is_none()).unwrap();
    assert_eq!(waiting.state(), "waiting_quota");
    assert!(waiting.status["detail"].is_null(), "{:?}", waiting.status);
    let path = c.dir.join("jobs").join(format!("{}.json", waiting.id));
    let written = fs::metadata(&path).unwrap().ino();
    c.dispatch().await.unwrap();
    c.dispatch().await.unwrap();
    assert_eq!(fs::metadata(&path).unwrap().ino(), written, "a pass that cannot place it leaves its record alone");
    assert_eq!(placed(&c), 2);
}
#[tokio::test]
async fn a_backlog_of_purges_does_not_hold_back_a_launch() {
    let (_dir, c, ns) = setup().await;
    c.report("gpu", "session", inventory(&ns)).await.unwrap();
    // A bulk purge of history: more purges than one heartbeat carries, all ahead in id order.
    for n in 0..20 {
        let id = format!("0000-{n:02}");
        c.inner.lock().commands.insert(id.clone(), CommandRecord {
            id, worker: "gpu".into(), operation: NodeOperation::PurgeJob as i32,
            parameters: json!({"id": format!("old-{n}"), "lease": "lease"}).into(),
            done: false, result: Value::Null.into(), error: None, admitted: true,
        });
    }
    let job = submit(&c).await;
    c.dispatch().await.unwrap();
    let launch = c.inner.lock().commands.about(&job.id).next().unwrap().id.clone();

    let sent = heartbeat_commands(&c.inner.lock(), "gpu");
    assert_eq!(sent.len(), 16);
    assert_eq!(sent[0].id, launch, "the launch goes first");
    assert!(sent[1..].iter().all(|c| c.operation == NodeOperation::PurgeJob as i32));
    assert_eq!(sent[1].id, "0000-00", "purges keep their order");
    assert!(heartbeat_commands(&c.inner.lock(), "other").is_empty());
}
/// Everything the tests log, from a process-wide subscriber: a scoped one can miss events
/// while tests on other threads run the same log lines.
fn test_logs() -> &'static parking_lot::Mutex<Vec<u8>> {
    static LOGS: std::sync::OnceLock<&'static parking_lot::Mutex<Vec<u8>>> = std::sync::OnceLock::new();
    LOGS.get_or_init(|| {
        let logs: &'static parking_lot::Mutex<Vec<u8>> = Box::leak(Box::default());
        struct Writer(&'static parking_lot::Mutex<Vec<u8>>);
        impl std::io::Write for Writer {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let subscriber = tracing_subscriber::fmt().with_ansi(false).with_writer(move || Writer(logs)).finish();
        tracing::subscriber::set_global_default(subscriber).expect("no other test installs a subscriber");
        logs
    })
}

#[tokio::test]
async fn a_bulk_purge_takes_every_finished_job_in_chunks_and_records_who_asked() {
    let logs = test_logs();
    let (_dir, c, ns) = setup().await;
    c.report("gpu", "session", inventory(&ns)).await.unwrap();
    let template = submit(&c).await;
    // More finished jobs than one chunk holds, and one that finished just now.
    for n in 0..150 {
        let mut r = template.clone();
        r.id = format!("old-{n:03}");
        r.worker = Some("gpu".into());
        r.status = json!({"state":"succeeded","submitted_at":1,"finished_at":(now() - 7200.0)}).into();
        c.inner.lock().jobs.insert(r.id.clone(), r);
    }
    let mut recent = template.clone();
    recent.id = "recent".into();
    recent.status = json!({"state":"succeeded","submitted_at":1,"finished_at":now()}).into();
    c.inner.lock().jobs.insert(recent.id.clone(), recent);

    let response = scale_call(c.clone(), "admin", "DELETE", "/batches/training/jobs?older_than=0.0517", json!(null)).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(body.as_array().unwrap().len(), 150);
    {
        let i = c.inner.lock();
        assert!((0..150).all(|n| i.jobs[&format!("old-{n:03}")].purged));
        assert!(!i.jobs["recent"].purged && !i.jobs[&template.id].purged);
        let purges: HashSet<&str> = i.commands.open()
            .filter(|c| c.worker == "gpu" && c.operation == NodeOperation::PurgeJob as i32)
            .filter_map(|c| c.parameters["id"].as_str())
            .collect();
        assert_eq!(purges.len(), 150, "each purged job's node drops its copy");
    }

    // The request is on record with its caller; a read is not.
    scale_call(c.clone(), "admin", "GET", "/batches/training/jobs", json!(null)).await;
    let logs = String::from_utf8(logs.lock().clone()).unwrap();
    let lines: Vec<&str> = logs.lines().filter(|l| l.contains("api request") && l.contains("older_than=0.0517")).collect();
    assert_eq!(lines.len(), 1, "{logs}");
    assert!(!logs.lines().any(|l| l.contains("api request") && l.contains("method=GET")), "reads are not logged");
    for part in ["method=DELETE", "uri=/batches/training/jobs?older_than=0.0517", "status=200", &format!("token=\"{}\"", "ad".repeat(16)), "label=\"admin\""] {
        assert!(lines[0].contains(part), "{part} in {}", lines[0]);
    }
    assert!(!lines[0].contains(ADMIN), "the token itself is never logged");
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bulk_purge_lets_node_reports_in_between_its_chunks() {
    let (_dir, c, ns) = setup().await;
    c.report("gpu", "session", inventory(&ns)).await.unwrap();
    let template = submit(&c).await;
    for n in 0..400 {
        let mut r = template.clone();
        r.id = format!("old-{n:03}");
        r.worker = Some("gpu".into());
        r.status = json!({"state":"succeeded","submitted_at":1,"finished_at":1}).into();
        c.inner.lock().jobs.insert(r.id.clone(), r);
    }
    let purged = |c: &WorkloadController| c.inner.lock().jobs.values().filter(|r| r.purged).count();
    let purge = tokio::spawn(scale_call(c.clone(), "admin", "DELETE", "/batches/training/jobs?older_than=0", json!(null)));
    while purged(&c) == 0 {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    // What a node report waits for: it gets the lock before the purge has finished.
    let during = {
        let _ops = c.ops.lock().await;
        purged(&c)
    };
    assert_eq!(purge.await.unwrap().status(), StatusCode::OK);
    assert!(during < 400, "the report waited for the whole purge");
    assert_eq!(purged(&c), 400);
}
#[tokio::test]
async fn offline_worker_and_terminal_without_cleanup_keep_reservations() {
    let (_dir, c, ns) = setup().await;
    c.report("gpu", "session", inventory(&ns)).await.unwrap();
    let job = submit(&c).await;
    c.dispatch().await.unwrap();
    let job = c.job(&job.id, None, &BTreeMap::new()).unwrap();
    c.inner.lock().workers.get_mut("gpu").unwrap().seen -= 60.0;
    c.dispatch().await.unwrap();
    assert_eq!(
        c.job(&job.id, None, &BTreeMap::new())
            .unwrap()
            .worker
            .as_deref(),
        Some("gpu")
    );
    assert_eq!(c.fleet.allocations(&ns).len(), 1);
    let mut report = inventory(&ns);
    report.jobs.push(incoming(&job, "failed", false));
    c.report("gpu", "session", report.clone()).await.unwrap();
    assert_eq!(c.fleet.allocations(&ns).len(), 1);
    report.jobs[0].cleaned = true;
    c.report("gpu", "session", report).await.unwrap();
    assert!(c.fleet.allocations(&ns).is_empty());
}
#[tokio::test]
async fn stale_lease_is_ignored_and_purged_history_cannot_be_imported_again() {
    let (_dir, c, ns) = setup().await;
    c.report("gpu", "session", inventory(&ns)).await.unwrap();
    let job = submit(&c).await;
    c.dispatch().await.unwrap();
    let job = c.job(&job.id, None, &BTreeMap::new()).unwrap();
    let mut report = inventory(&ns);
    let mut stale = incoming(&job, "succeeded", true);
    stale.lease = "old".into();
    report.jobs.push(stale);
    c.report("gpu", "session", report.clone()).await.unwrap();
    assert_eq!(c.fleet.allocations(&ns).len(), 1);
    report.jobs[0] = incoming(&job, "succeeded", true);
    c.report("gpu", "session", report.clone()).await.unwrap();
    c.purge(c.job(&job.id, None, &BTreeMap::new()).unwrap())
        .unwrap();
    c.report("gpu", "session", report).await.unwrap();
    assert!(c.job(&job.id, None, &BTreeMap::new()).is_err());
}
#[tokio::test]
async fn quota_priority_and_namespace_scoped_preemption_are_central() {
    let (_dir, c, ns) = setup().await;
    c.report("gpu", "session", inventory(&ns)).await.unwrap();
    let low = submit(&c).await;
    assert_eq!(low.value()["priority"], 20);
    c.dispatch().await.unwrap();
    let low = c.job(&low.id, None, &BTreeMap::new()).unwrap();
    let mut report = inventory(&ns);
    report.free_gpus = 0.0;
    report.jobs.push(incoming(&low, "running", false));
    c.report("gpu", "session", report).await.unwrap();
    let other = c
        .fleet
        .create_namespace("other", Quota::default())
        .await
        .unwrap();
    assert!(!c
        .preempt_for(
            &other.id,
            None,
            100,
            1.0,
            1.0,
            vec![Some("gpu".into())],
            None
        )
        .await
        .unwrap());
    assert!(c
        .preempt_for(&ns, None, 100, 1.0, 1.0, vec![Some("gpu".into())], None)
        .await
        .unwrap());
    assert_eq!(
        c.job(&low.id, None, &BTreeMap::new())
            .unwrap()
            .stop
            .as_deref(),
        Some("preempt")
    );
    assert_eq!(c.fleet.allocations(&ns).len(), 1);
}
#[tokio::test]
async fn cancellation_before_launch_ack_releases_only_after_node_confirms() {
    let (_dir, c, ns) = setup().await;
    c.report("gpu", "session", inventory(&ns)).await.unwrap();
    let job = submit(&c).await;
    c.dispatch().await.unwrap();
    let job = c.job(&job.id, None, &BTreeMap::new()).unwrap();
    c.stop_job(job.clone(), false).await.unwrap();
    assert_eq!(c.fleet.allocations(&ns).len(), 1);
    let stop = c
        .inner
        .lock()
        .commands
        .values()
        .find(|c| c.operation == NodeOperation::CancelJob as i32)
        .unwrap()
        .clone();
    let mut report = inventory(&ns);
    report.results.push(CommandResult {
        id: stop.id,
        result: json!({"cleaned":true}).into(),
        error: None,
    });
    c.report("gpu", "session", report).await.unwrap();
    assert!(c.fleet.allocations(&ns).is_empty());
    assert_eq!(
        c.job(&job.id, None, &BTreeMap::new()).unwrap().state(),
        "cancelled"
    );
}
#[tokio::test]
async fn invalid_requests_and_cross_machine_jobs_are_refused_before_creation() {
    let (_dir, c, ns) = setup().await;
    c.report("gpu", "session", inventory(&ns)).await.unwrap();
    for (field, value, code) in [
        ("nodes", json!(2), 422),
        ("cpus", json!(-1), 400),
        ("gpus", json!(3), 400),
        ("priority", json!(100), 400),
        ("env", json!("bad"), 400),
        ("code", json!("upload:removed"), 400),
    ] {
        let mut request = json!({"command":["true"],"gpus":1,"cpus":1});
        request[field] = value;
        let err = c.submit("training", request, None, &BTreeMap::new()).await.unwrap_err();
        assert_eq!(
            err.downcast_ref::<crate::fleet::RequestRejection>().unwrap().status,
            code
        );
    }
    assert!(c.inner.lock().jobs.is_empty());
}
#[tokio::test]
async fn tag_scopes_jobs_and_never_widens_a_token() {
    let (_dir, c, ns) = setup().await;
    c.report("gpu", "session", inventory(&ns)).await.unwrap();
    let token = Token {
        id: "owner".into(),
        address: "internal".into(),
        label: "owner".into(),
        created_ms: 0,
        revoked_ms: None,
        apps: None,
        token_hash: None,
        namespace: Some(ns.clone()),
        tag: Some("scope=default".into()),
        permissions: vec!["read".into(),"submit".into(),"manage".into()],
    };
    let job = c
        .submit(
            "training", json!({"command":["true"],"gpus":0,"cpus":1}),
            Some(&token),
            &BTreeMap::new(),
        )
        .await
        .unwrap();
    let mut other = token.clone();
    other.tag = Some("scope=research".into());
    assert!(c
        .job(text(&job, "id"), Some(&other), &BTreeMap::new())
        .is_err());
    let mut q = BTreeMap::new();
    q.insert("tag".into(), "scope=research".into());
    assert!(c.scope(Some(&token), &q, None).is_err());
}
#[tokio::test]
async fn heartbeat_fences_duplicate_sessions_and_registers_local_node_capacity() {
    let (_dir, c, ns) = setup().await;
    let mut report = inventory(&ns);
    report.free_gpus = 0.5;
    report.free_cpus = 0.0;
    report.agent_version = Some("v0.1.6".into());
    c.report("gpu", "first", report).await.unwrap();
    assert!(c.report("gpu", "second", inventory(&ns)).await.is_err());
    assert_eq!(c.worker_status()[0]["backend"], "ray-local");
    assert_eq!(c.worker_status()[0]["agent_version"], "v0.1.6");
    assert_eq!(c.worker_status()[0]["gpus"].as_f64(), Some(2.0));
    assert_eq!(c.worker_status()[0]["cpus"].as_f64(), Some(8.0));
    assert_eq!(c.worker_status()[0]["free_gpus"].as_f64(), Some(0.5));
    assert_eq!(c.worker_status()[0]["free_cpus"].as_f64(), Some(0.0));
    c.drain_worker("gpu", true);
    assert!(!c.worker_status()[0]["ready"].as_bool().unwrap());
    assert!(c.capacity("gpu").is_none());
}

#[tokio::test]
async fn http_user_auth_app_management_and_artifacts_are_checked_in_rust() {
    use sha2::Digest;
    let (_dir, c, ns) = setup().await;
    let secret = "1".repeat(64);
    c.fleet
        .add_user_token(&"a".repeat(32),
            &format!("0x{}", "b".repeat(40)),
            "owner",
            None,
            Some(hex::encode(sha2::Sha256::digest(secret.as_bytes()))),
            Some(ns.clone()),
            Some("scope=default".into()),
            vec!["read".into(),"submit".into(),"manage".into()],
        )
        .await
        .unwrap();
    async fn call(
        c: Arc<WorkloadController>,
        method: &str,
        path: &str,
        token: Option<&str>,
        body: Value,
    ) -> Response {
        let mut builder = Request::builder().method(method).uri(path);
        if let Some(token) = token {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }
        serve(
            &c,
            builder
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
    }
    assert_eq!(
        call(c.clone(), "GET", "/batches", None, Value::Null)
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            c.clone(),
            "PUT",
            "/uploads",
            Some(&secret),
            json!({"input":"removed"})
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            c.clone(),
            "POST",
            "/batches",
            Some(&secret),
            json!({"name":"user-app","image":"python:3"})
        )
        .await
        .status(),
        StatusCode::CREATED
    );
    assert_eq!(
        call(
            c.clone(),
            "POST",
            "/batches",
            Some(&secret),
            json!({"name":"mounted","image":"python:3","namespace":ns,"allow":["/data/models"],"priority":17})
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            c.clone(),
            "PUT",
            "/batches/user-app/priority",
            Some(&secret),
            json!({"priority":100})
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        call(
            c.clone(),
            "GET",
            "/batches?tag=scope=research",
            Some(&secret),
            Value::Null
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
    let response = call(
        c.clone(),
        "POST",
        "/batches/user-app/jobs",
        Some(&secret),
        json!({"gpus":0,"cpus":1,"command":["train","--input","s3://owner-bucket/dataset"],"env":["INPUT_URI=s3://owner-bucket/dataset"]}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    let id = text(&body, "id");
    assert_eq!(body["namespace"], ns);
    assert_eq!(body["tag"], "scope=default");
    assert_eq!(
        c.job(id, None, &BTreeMap::new()).unwrap().request["env"],
        json!(["INPUT_URI=s3://owner-bucket/dataset"])
    );
    c.report("gpu", "session", inventory(&ns)).await.unwrap();
    c.dispatch().await.unwrap();
    let assigned = c.job(id, None, &BTreeMap::new()).unwrap();
    let removed_input = Request::builder()
        .uri(format!("/internal/jobs/{id}/input"))
        .header("authorization", "Bearer cluster")
        .header("x-fleet-worker", assigned.worker.as_deref().unwrap())
        .header("x-fleet-lease", &assigned.lease)
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        serve(&c, removed_input).await.status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            c.clone(),
            "GET",
            &format!("/batches/user-app/jobs/{id}/out.tar.gz"),
            Some(&secret),
            Value::Null
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
    let removed_output = Request::builder()
        .method("PUT")
        .uri(format!("/internal/jobs/{id}/out.tar.gz"))
        .header("authorization", "Bearer cluster")
        .header("x-fleet-worker", assigned.worker.as_deref().unwrap())
        .header("x-fleet-lease", &assigned.lease)
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        serve(&c, removed_output).await.status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            c.clone(),
            "GET",
            &format!("/internal/jobs/{id}/logs-offset"),
            Some(&secret),
            Value::Null
        )
        .await
        .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            c.clone(),
            "GET",
            &format!("/internal/jobs/{id}/logs-offset"),
            Some("cluster"),
            Value::Null
        )
        .await
        .status(),
        StatusCode::CONFLICT
    );
    assert_eq!(
        call(
            c.clone(),
            "GET",
            "/batches?namespace=other",
            Some(&secret),
            Value::Null
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn completion_racing_preemption_does_not_restart_completed_work() {
    let (_dir, c, ns) = setup().await;
    c.report("gpu", "session", inventory(&ns)).await.unwrap();
    let job = submit(&c).await;
    c.dispatch().await.unwrap();
    let job = c.job(&job.id, None, &BTreeMap::new()).unwrap();
    c.stop_job(job.clone(), true).await.unwrap();
    let command = c
        .inner
        .lock()
        .commands
        .values()
        .find(|c| c.operation == NodeOperation::PreemptJob as i32)
        .unwrap()
        .clone();
    let mut report = inventory(&ns);
    report.jobs.push(incoming(&job, "succeeded", true));
    report.results.push(CommandResult {
        id: command.id,
        result: json!({"cleaned":true}).into(),
        error: None,
    });
    c.report("gpu", "session", report).await.unwrap();
    let done = c.job(&job.id, None, &BTreeMap::new()).unwrap();
    assert_eq!(done.state(), "succeeded");
    assert_eq!(done.lease, job.lease);
    assert!(c.fleet.allocations(&ns).is_empty());
}
#[tokio::test]
async fn restart_repairs_stop_intent_written_before_its_command() {
    let (_dir, c, ns) = setup().await;
    c.report("gpu", "session", inventory(&ns)).await.unwrap();
    let job = submit(&c).await;
    c.dispatch().await.unwrap();
    let mut job = c.job(&job.id, None, &BTreeMap::new()).unwrap();
    job.stop = Some("cancel".into());
    c.save("jobs", &job.id, &job).unwrap();
    let restarted = WorkloadController::new(c.fleet.clone(), "cluster".into()).unwrap();
    restarted.dispatch().await.unwrap();
    assert!(restarted
        .inner
        .lock()
        .commands
        .values()
        .any(|c| c.operation == NodeOperation::CancelJob as i32
            && c.parameters["lease"] == job.lease));
}

fn scale_inventory(ns: &str, proof: bool, disabled: bool) -> NodeReport {
    let mut report = inventory(ns);
    report.apps = vec![json!({"name":"model","kind":if proof {"ziren-gpu"}else{"app"},
        "namespace":ns,"tag":"scope=default","app":if proof {PROOF.to_string()}else{"model".into()},"elf_id":if proof {proof_elf()}else{String::new()},
        "gpus":1,"cpus":2,"replicas":2,"disabled":disabled,"ready":!disabled,"cleaned":disabled,"image":"runner:1"}).into()];
    if !disabled { report.free_gpus=0.0;report.free_cpus=4.0; }
    report
}
async fn scale_call(c: Arc<WorkloadController>, token: &str, method: &str, path: &str, body: Value,
) -> Response {
    let token = if token == "admin" { ADMIN } else { token };
    serve(&c, Request::builder().method(method).uri(path)
        .header("authorization",format!("Bearer {token}"))
        .body(Body::from(serde_json::to_vec(&body).unwrap())).unwrap(),
    ).await
}
/// A user token; `manage` adds the manage permission to read and submit.
async fn user_token(c: &WorkloadController, ns: &str, seed: u8, tag: &str, apps: Option<Vec<String>>, manage: bool,
) -> String {
    let permissions = if manage { vec!["read".into(), "submit".into(), "manage".into()] } else { Vec::new() };
    let secret = format!("{seed:02x}").repeat(32);
    c.fleet.add_user_token(&format!("{seed:02x}").repeat(16), &format!("{seed:02x}").repeat(20), "test",
        apps, Some(hex::encode(sha2::Sha256::digest(secret.as_bytes()))), Some(ns.into()), Some(tag.into()), permissions,
    ).await.unwrap();
    secret
}

/// A submission's checks run in a fixed order, so a body with several faults gets the first
/// one's status and message; a batch's request takes only its own settings.
#[tokio::test]
async fn submissions_and_new_batches_are_checked_field_by_field() {
    let (_dir, c, ns) = setup().await;
    let created = scale_call(c.clone(), "admin", "POST", &format!("/batches?namespace={ns}"), json!({"name":"training","image":"python:3"})).await;
    assert_eq!(created.status(), StatusCode::CREATED);
    let unknown = scale_call(c.clone(), "admin", "POST", &format!("/batches?namespace={ns}"),
        json!({"name":"other","image":"python:3","gpus":1})).await;
    assert_eq!(unknown.status(), StatusCode::BAD_REQUEST);
    assert_eq!(to_bytes(unknown.into_body(), 1 << 20).await.unwrap(), "unknown batch fields");
    let path = format!("/batches/training/jobs?namespace={ns}");
    for (body, status, message) in [
        (json!([]), 400, "expected object"),
        (json!({"command":["true"],"priority":1}), 400, "unknown or server-side submission fields"),
        (json!({"cpus":-1,"gpus":"one"}), 400, "cpus: expected non-negative integer"),
        (json!({"retry":1.5}), 400, "retry: expected non-negative integer"),
        (json!({"gpus":"one","mount":[1]}), 400, "gpus: expected number"),
        (json!({"env":"A=1"}), 400, "env: expected string array"),
        (json!({"machine":"gpu"}), 400, "machine: expected string array"),
        (json!({"machine":["a","b"],"name":3}), 422, "one machine may be selected for a local job"),
        (json!({"name":3,"after":4}), 400, "name: expected string"),
        (json!({"workdir":["/"]}), 400, "workdir: expected string"),
        (json!({"command":[]}), 400, "command: a non-empty list of strings"),
        (json!({"command":["true"],"nodes":2}), 422, "cross-machine jobs are not supported by local Ray; use one node"),
        (json!({"command":["true"],"gpus":0.5,"cpus":1}), 400, "fractional GPUs require a VRAM limit"),
    ] {
        let response = scale_call(c.clone(), "admin", "POST", &path, body.clone()).await;
        assert_eq!(response.status().as_u16(), status, "{body}");
        assert_eq!(to_bytes(response.into_body(), 1 << 20).await.unwrap(), message, "{body}");
    }
    let response = scale_call(c.clone(), "admin", "POST", &path, json!({"command":["true"],"gpus":0,"cpus":1,"machine":["gpu"]})).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let job: Value = serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    let request = c.job(text(&job, "id"), None, &BTreeMap::new()).unwrap().request.0;
    assert_eq!(request, json!({"command":["true"],"gpus":0.0,"cpus":1,"machine":["gpu"],"gpu_mem":0,"nodes":1}));
}

#[tokio::test]
async fn scoped_managers_cannot_change_app_execution_or_batch_host_access() {
    for proof in [false, true] {
        let (_dir, c, ns) = setup().await;
        register(&c, &ns, PROOF, &proof_elf()).await;
        c.report("gpu", "session", scale_inventory(&ns, proof, true)).await.unwrap();
        let owner = user_token(&c, &ns, 1, "scope=default", None, true).await;
        let app = if proof { PROOF } else { "model" };
        let path = format!("/apps/{app}/settings");
        let before = c.inner.lock().deployments["gpu-model"].clone();
        for set in [
            json!({"image":"untrusted:latest"}), json!({"command":["sh"]}),
            json!({"env":{"LD_PRELOAD":"/data/owner.so"}}), json!({"user":"0:0"}),
            json!({"workdir":"/etc/zkm-fleet"}), json!({"volumes":["/var/run/docker.sock","/etc/zkm-fleet:ro"]}),
            json!({"stop_timeout_s":10,"command":["sh"]}),
        ] {
            assert_eq!(scale_call(c.clone(), &owner, "PUT", &path, set.clone()).await.status(), StatusCode::FORBIDDEN, "{set}");
            assert_eq!(c.inner.lock().deployments["gpu-model"], before, "a refused update must not create a transition");
        }
        assert_eq!(scale_call(c.clone(), "admin", "PUT", &format!("{path}?namespace={ns}"),
            json!({"image":"trusted:2","user":"1000:1000","volumes":["/srv/app:ro"]})).await.status(), StatusCode::ACCEPTED);
    }
    let (_dir, c, ns) = setup().await;
    c.report("gpu", "session", scale_inventory(&ns, false, true)).await.unwrap();
    let owner = user_token(&c, &ns, 1, "scope=default", None, true).await;
    assert_eq!(scale_call(c.clone(), &owner, "PUT", "/apps/model/settings", json!({"stop_timeout_s":10})).await.status(), StatusCode::ACCEPTED);
    for body in [
        json!({"name":"unsafe","image":"test","allow":["/var/run"]}),
        json!({"name":"unsafe","image":"test","user":"0:0"}),
    ] {
        assert_eq!(scale_call(c.clone(), &owner, "POST", "/batches", body).await.status(), StatusCode::FORBIDDEN);
        assert!(!c.inner.lock().apps.contains_key("unsafe"));
    }
    assert_eq!(scale_call(c.clone(), &owner, "POST", "/batches", json!({"name":"safe","image":"test","allow":[]})).await.status(), StatusCode::CREATED);
    let path = "/batches/safe/settings";
    for set in [json!({"allow":["/var/run"]}), json!({"allow":[]}), json!({"user":"0:0"}),
        json!({"image":"changed","allow":["/etc"]})] {
        let before = c.inner.lock().apps["safe"].clone();
        assert_eq!(scale_call(c.clone(), &owner, "PUT", path, set).await.status(), StatusCode::FORBIDDEN);
        assert_eq!(c.inner.lock().apps["safe"], before);
    }
    assert_eq!(scale_call(c.clone(), &owner, "POST", "/batches/safe/jobs",
        json!({"command":["true"],"gpus":0,"cpus":1,"mount":["/var/run/docker.sock"]})).await.status(), StatusCode::FORBIDDEN);
    assert_eq!(scale_call(c.clone(), "admin", "PUT", &format!("{path}?namespace={ns}"),
        json!({"allow":["/srv/app"],"user":"1000:1000"})).await.status(), StatusCode::OK);
    assert_eq!(scale_call(c.clone(), &owner, "PUT", path, json!({"image":"test:2","env":{"A":"1"}})).await.status(), StatusCode::OK);
    assert_eq!(c.inner.lock().apps["safe"].spec["allow"], json!(["/srv/app"]));
    assert_eq!(c.inner.lock().apps["safe"].spec["user"], "1000:1000");
}

#[tokio::test]
async fn batch_metadata_belongs_to_scoped_owners_and_job_policy_is_inherited() {
    let (_dir, c, ns) = setup().await;
    let owner = user_token(&c, &ns, 1, "scope=default", None, true).await;
    let consumer = user_token(&c, &ns, 2, "scope=default", Some(vec!["training".into()]), false).await;
    let restricted = user_token(&c, &ns, 3, "scope=default", Some(vec!["other".into()]), true).await;
    let foreign_tag = user_token(&c, &ns, 4, "scope=research", None, true).await;
    let other_ns = c.fleet.create_namespace("other", Quota::default()).await.unwrap().id;
    let foreign_ns = user_token(&c, &other_ns, 5, "scope=default", None, true).await;
    assert_eq!(scale_call(c.clone(), &owner, "POST", "/batches",
        json!({"name":"training","image":"python:3","priority":17})).await.status(), StatusCode::CREATED);
    assert_eq!(scale_call(c.clone(), "admin", "PUT", &format!("/batches/training/settings?namespace={ns}"),
        json!({"allow":["/data/models"],"user":"1000:1000"})).await.status(), StatusCode::OK);
    for token in [&consumer, &restricted, &foreign_tag, &foreign_ns] {
        for (method, path, body) in [
            ("PUT", "/batches/training/priority", json!({"priority":99})),
            ("PUT", "/batches/training/disabled", Value::Null),
            ("DELETE", "/batches/training/disabled", Value::Null),
            ("DELETE", "/batches/training", Value::Null),
        ] {
            let status = scale_call(c.clone(), token, method, path, body).await.status();
            assert!(matches!(status, StatusCode::FORBIDDEN | StatusCode::NOT_FOUND), "{method} {path}: {status}");
        }
    }
    assert_eq!(scale_call(c.clone(), &consumer, "POST", "/batches",
        json!({"name":"consumer","image":"test"})).await.status(), StatusCode::FORBIDDEN);
    for body in [json!({"name":"invalid","image":"test","allow":["/"]}),
        json!({"name":"invalid","image":"--privileged"}),
        json!({"name":"invalid","image":"test","allow":["/data/../etc"]}),
        json!({"name":"invalid","image":"test","priority":2147483648_i64}),
    ] {
        assert_eq!(scale_call(c.clone(), &owner, "POST", "/batches", body).await.status(), StatusCode::BAD_REQUEST);
    }
    assert_eq!(scale_call(c.clone(), &owner, "PUT", "/batches/training/disabled", Value::Null).await.status(), StatusCode::OK);
    assert_eq!(scale_call(c.clone(), &owner, "POST", "/batches/training/jobs",
        json!({"command":["true"]})).await.status(), StatusCode::CONFLICT);
    assert_eq!(scale_call(c.clone(), &owner, "DELETE", "/batches/training/disabled", Value::Null).await.status(), StatusCode::OK);
    let response = scale_call(c.clone(), &consumer, "POST", "/batches/training/jobs",
        json!({"gpus":0,"cpus":1,"mount":["/data/models:ro"],"command":["true"]})).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let job: Value = serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(job["priority"], 17);
    // Only those who can see the batch submit to it; another tag is told what another namespace is.
    let submit = json!({"gpus":0,"cpus":1,"mount":["/data/models:ro"],"command":["env"]});
    for token in [&foreign_tag, &foreign_ns] {
        let status = scale_call(c.clone(), token, "POST", "/batches/training/jobs", submit.clone()).await.status();
        assert_eq!(status, StatusCode::NOT_FOUND);
    }
    assert_eq!(scale_call(c.clone(), &restricted, "POST", "/batches/training/jobs", submit.clone()).await.status(), StatusCode::FORBIDDEN);
    assert_eq!(scale_call(c.clone(), "admin", "POST", &format!("/batches/training/jobs?namespace={ns}&tag=scope=research"),
        submit.clone()).await.status(), StatusCode::NOT_FOUND);
    // An admin's job on it is the batch's tag's, which its owner then sees and cancels.
    let response = scale_call(c.clone(), "admin", "POST", &format!("/batches/training/jobs?namespace={ns}"), submit).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let admin_job: Value = serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(c.job(text(&admin_job, "id"), None, &BTreeMap::new()).unwrap().tag.as_deref(), Some("scope=default"));
    assert_eq!(scale_call(c.clone(), &owner, "POST", &format!("/batches/training/jobs/{}/cancel", text(&admin_job, "id")),
        Value::Null).await.status(), StatusCode::OK);
    assert_eq!(scale_call(c.clone(), &owner, "PUT", "/batches/training/priority", json!({"priority":31})).await.status(), StatusCode::OK);
    assert_eq!(c.job(text(&job, "id"), None, &BTreeMap::new()).unwrap().policy["priority"], 17);
    assert_eq!(c.inner.lock().apps["training"].spec["allow"], json!(["/data/models"]));
    assert_eq!(scale_call(c.clone(), &consumer, "POST", "/batches/training/jobs",
        json!({"gpus":0,"cpus":1,"mount":["/etc:ro"],"command":["true"]})).await.status(), StatusCode::FORBIDDEN);
    assert_eq!(scale_call(c.clone(), &owner, "POST", &format!("/batches/training/jobs/{}/cancel", text(&job, "id")), Value::Null).await.status(), StatusCode::OK);
    assert_eq!(scale_call(c, &owner, "DELETE", "/batches/training", Value::Null).await.status(), StatusCode::OK);
}

/// A proof app is changed only by the tag it was created for, also before it is placed, when no
/// deployment records that tag yet.
#[tokio::test]
async fn an_unplaced_proof_app_is_changed_only_by_its_own_tag() {
    let (_dir, c, ns) = setup().await;
    let owner = user_token(&c, &ns, 1, "scope=default", None, true).await;
    let foreign_tag = user_token(&c, &ns, 4, "scope=research", None, true).await;
    let body = json!({"name":"eth","namespace":ns,"spec":{"image":"ziren-gpu:1","gpus":1},
        "proof":{"elf_id":"AB".repeat(32),"class":"latency"}});
    assert_eq!(scale_call(c.clone(), &owner, "POST", "/apps", body).await.status(), StatusCode::CREATED);
    for (method, path, body) in [
        ("PUT", "/apps/eth/priority", json!({"priority":31})),
        ("DELETE", "/apps/eth/disabled", Value::Null),
        ("PUT", "/apps/eth/disabled", Value::Null),
        ("DELETE", "/apps/eth", Value::Null),
    ] {
        let status = scale_call(c.clone(), &foreign_tag, method, path, body).await.status();
        assert_eq!(status, StatusCode::FORBIDDEN, "{method} {path}");
    }
    assert!(c.fleet.app_known(&ns, "eth") && !c.inner.lock().apps["eth"].removed, "the app is still there");
    assert_eq!(c.fleet.app(&ns, "eth").unwrap().priority, 0);
    assert_eq!(scale_call(c.clone(), &owner, "PUT", "/apps/eth/priority", json!({"priority":31})).await.status(), StatusCode::OK);
    assert_eq!(c.fleet.app(&ns, "eth").unwrap().priority, 31);
    assert_eq!(scale_call(c.clone(), &owner, "DELETE", "/apps/eth", Value::Null).await.status(), StatusCode::OK);
    assert!(!c.fleet.app_known(&ns, "eth"));
}

#[tokio::test]
async fn app_owners_manage_apps_and_scale_without_widening_grants_or_tags() {
    for proof in [false, true] {
        let (_dir, c, ns) = setup().await;
        let elf = "a".repeat(64);
        register(&c, &ns, PROOF, &elf).await;
        c.report("gpu", "session", scale_inventory(&ns, proof, false)).await.unwrap();
        let app = if proof { PROOF.to_string() } else { "model".into() };
        let owner = user_token(&c, &ns, 1, "scope=default", None, true).await;
        let consumer = user_token(&c, &ns, 2, "scope=default", Some(vec![app]), false).await;
        let restricted = user_token(&c, &ns, 3, "scope=default", Some(vec!["other".into()]), true).await;
        let foreign_tag = user_token(&c, &ns, 4, "scope=research", None, true).await;
        let other_signer = user_token(&c, &ns, 5, "scope=default", Some(vec!["model".into()]), false).await;
        let scale = if proof { format!("/apps/{PROOF}/scale") } else { "/apps/model/scale".into() };
        let disabled = if proof { format!("/apps/{PROOF}/disabled") } else { "/apps/model/disabled".into() };
        for token in [&consumer, &restricted, &foreign_tag] {
            for (method, path, body) in [
                ("PUT", scale.as_str(), json!({"replicas":1})),
                ("PUT", disabled.as_str(), Value::Null),
                ("DELETE", disabled.as_str(), Value::Null),
            ] {
                let status = scale_call(c.clone(), token, method, path, body).await.status();
                assert!(matches!(status, StatusCode::FORBIDDEN | StatusCode::NOT_FOUND), "{method} {path}: {status}");
            }
        }
        assert_eq!(scale_call(c.clone(), &consumer, "GET", &scale, Value::Null).await.status(), StatusCode::OK);
        assert_eq!(scale_call(c.clone(), &owner, "GET", &scale, Value::Null).await.status(), StatusCode::OK);
        if proof {
            assert_eq!(scale_call(c.clone(), &other_signer, "GET", &scale, Value::Null).await.status(), StatusCode::FORBIDDEN);
        }
        for token in [&restricted, &foreign_tag] {
            let status = scale_call(c.clone(), token, "GET", &scale, Value::Null).await.status();
            assert!(matches!(status, StatusCode::FORBIDDEN | StatusCode::NOT_FOUND));
        }
        c.fleet.create_namespace("other", Quota::default()).await.unwrap();
        assert_eq!(scale_call(c.clone(), &owner, "PUT", &format!("{scale}?namespace=other"),
            json!({"replicas":1})).await.status(), StatusCode::FORBIDDEN);
        if !proof {
            assert_eq!(scale_call(c.clone(), &owner, "PUT", "/apps/model/priority", json!({"priority":31})).await.status(), StatusCode::OK);
            assert_eq!(c.inner.lock().apps["model"].spec["priority"], 31);
        }
        for (method, disabled_state) in [("PUT", true), ("DELETE", false)] {
            let control = c.clone();
            let token = owner.clone();
            let path = disabled.clone();
            let request = tokio::spawn(async move { scale_call(control, &token, method, &path, Value::Null).await });
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                while !c.inner.lock().deployments.values().any(|s| s.scale.as_ref().is_some_and(|s| s.pending() && s.disabled == disabled_state)) {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            }).await.unwrap();
            node_completes_changes(&c, &ns, proof).await;
            assert_eq!(request.await.unwrap().status(), StatusCode::OK);
        }
        assert_eq!(scale_call(c.clone(), &owner, "PUT", &scale, json!({"replicas":1})).await.status(), StatusCode::ACCEPTED);
        assert_eq!(scale_call(c, &owner, "PUT", &disabled, Value::Null).await.status(), StatusCode::CONFLICT);
    }
}

#[tokio::test]
async fn namespace_reads_are_scoped_for_owners_and_app_tokens_while_writes_require_admin() {
    let (_dir, c, ns) = setup().await;
    let other = c.fleet.create_namespace("other", Quota::default()).await.unwrap().id;
    c.fleet.set_tag_quota(&ns, "scope=default", Quota { gpus: Some(0.5), cpus: Some(2.0) }).await.unwrap();
    c.fleet.set_tag_quota(&ns, "scope=research", Quota { gpus: Some(1.0), cpus: Some(4.0) }).await.unwrap();
    let owner = user_token(&c, &ns, 1, "scope=default", None, true).await;
    let consumer = user_token(&c, &ns, 2, "scope=default", Some(vec!["training".into()]), false).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let router = crate::http::router(crate::http::HttpState { fleet: c.fleet.clone(), workloads: c.clone() });
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap(); });
    let client = reqwest::Client::new();
    for token in [&owner, &consumer] {
        for selector in [String::new(), "?namespace=test".into(), format!("?namespace={ns}&tag=scope%3Ddefault")] {
            let response = client.get(format!("{url}/namespaces{selector}")).bearer_auth(token).send().await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let rows: Value = response.json().await.unwrap();
            assert_eq!(rows.as_array().unwrap().len(), 1);
            assert_eq!(rows[0]["id"], ns);
            assert_eq!(rows[0]["quota"], json!({"gpus":2.0,"cpus":8.0}));
            assert_eq!(rows[0]["tag_quotas"], json!({"scope=default":{"gpus":0.5,"cpus":2.0}}));
        }
        for selector in ["test".to_string(), ns.clone(), "test?tag=scope%3Ddefault".into()] {
            let response = client.get(format!("{url}/namespaces/{selector}")).bearer_auth(token).send().await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let record: Value = response.json().await.unwrap();
            assert_eq!(record["id"], ns);
            assert_eq!(record["quota"], json!({"gpus":2.0,"cpus":8.0}));
            assert_eq!(record["usage"], json!({"gpus":0.0,"cpus":0.0}));
            assert_eq!(record["tag"], "scope=default");
            assert_eq!(record["tag_quota"], json!({"gpus":0.5,"cpus":2.0}));
            assert_eq!(record["tag_quotas"].as_object().unwrap().len(), 1);
            assert!(record.get("tags").is_none());
        }
        for path in ["/namespaces/other".into(), format!("/namespaces/{other}"),
            "/namespaces?namespace=other".into(), "/namespaces/test?namespace=other".into(),
            "/namespaces?tag=scope%3Dresearch".into(), "/namespaces/test?tag=scope%3Dresearch".into(),
            "/namespaces/test/allocations".into(), "/namespaces/test/quota".into(), "/tokens".into(),
        ] {
            assert_eq!(client.get(format!("{url}{path}")).bearer_auth(token).send().await.unwrap().status(), StatusCode::FORBIDDEN, "{path}");
        }
        for (method, path) in [
            (reqwest::Method::POST, "/namespaces"),
            (reqwest::Method::PUT, "/namespaces/test/quota"),
            (reqwest::Method::PUT, "/namespaces/test/quota?tag=scope%3Ddefault"),
            (reqwest::Method::PUT, "/namespaces/test/suspended"),
            (reqwest::Method::DELETE, "/namespaces/test/suspended"),
        ] {
            assert_eq!(client.request(method, format!("{url}{path}")).bearer_auth(token).json(&json!({"gpus":0,"cpus":0})).send().await.unwrap().status(), StatusCode::FORBIDDEN);
        }
    }
    let all: Value = client.get(format!("{url}/namespaces")).bearer_auth(ADMIN).send().await.unwrap().json().await.unwrap();
    assert_eq!(all.as_array().unwrap().len(), 3);
    let full: Value = client.get(format!("{url}/namespaces/test")).bearer_auth(ADMIN).send().await.unwrap().json().await.unwrap();
    assert_eq!(full["tag_quotas"].as_object().unwrap().len(), 2);
    assert!(full["tags"].as_array().unwrap().contains(&json!("scope=research")));
    assert_eq!(client.get(format!("{url}/namespaces")).send().await.unwrap().status(), StatusCode::UNAUTHORIZED);
    c.fleet.revoke_token(&"01".repeat(16)).await.unwrap();
    assert_eq!(client.get(format!("{url}/namespaces")).bearer_auth(&owner).send().await.unwrap().status(), StatusCode::UNAUTHORIZED);
    c.fleet.suspend_namespace(&ns, true).await.unwrap();
    assert_eq!(client.get(format!("{url}/namespaces/test")).bearer_auth(&consumer).send().await.unwrap().status(), StatusCode::UNAUTHORIZED);
    server.abort();
}

#[tokio::test]
async fn app_http_metadata_is_owner_managed_and_quotas_are_admin_managed() {
    let (_dir, c, ns) = setup().await;
    let elf = "a".repeat(64);
    let other_elf = "b".repeat(64);
    register(&c, &ns, PROOF, &elf).await;
    register(&c, &ns, "other-proof", &other_elf).await;
    let other_ns = c.fleet.create_namespace("other", Quota::default()).await.unwrap().id;
    register(&c, &other_ns, PROOF, &elf).await;
    let owner = user_token(&c, &ns, 1, "scope=default", None, true).await;
    let consumer = user_token(&c, &ns, 2, "scope=default", Some(vec![PROOF.to_string()]), false).await;
    let restricted = user_token(&c, &ns, 3, "scope=default", Some(vec![PROOF.to_string()]), true).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let router = crate::http::router(crate::http::HttpState { fleet: c.fleet.clone(), workloads: c.clone() });
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap(); });
    let client = reqwest::Client::new();
    for (method, leaf) in [
        (reqwest::Method::PUT, "/priority"),
        (reqwest::Method::PUT, "/disabled"),
        (reqwest::Method::DELETE, "/disabled"),
        (reqwest::Method::DELETE, ""),
    ] {
        for (token, app, selector) in [
            (&consumer, PROOF, ""),
            (&owner, PROOF, "?namespace=other"),
            (&owner, PROOF, "?tag=scope=research"),
            (&restricted, "other-proof", ""),
        ] {
            let response = client.request(method.clone(), format!("{url}/apps/{app}{leaf}{selector}"))
                .bearer_auth(token).json(&json!({"priority":31})).send().await.unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN, "{method} {app}{leaf}{selector}");
        }
    }
    assert_eq!(client.put(format!("{url}/apps/{PROOF}/priority")).bearer_auth(&restricted).json(&json!({"priority":31}))
        .send().await.unwrap().status(), StatusCode::OK);
    for (method, disabled) in [(reqwest::Method::PUT, true), (reqwest::Method::DELETE, false)] {
        let response = client.request(method, format!("{url}/apps/{PROOF}/disabled"))
            .bearer_auth(&owner).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let app: Value = response.json().await.unwrap();
        assert_eq!(app["disabled"], disabled);
        assert!(app["proof"].is_object());
        assert_eq!(c.fleet.app(&ns, PROOF).unwrap().priority, 31);
    }
    for selector in [String::new(), "?tag=scope=default".into()] {
        let path = format!("{url}/namespaces/{ns}/quota{selector}");
        for token in [&owner, &consumer, &restricted] {
            assert_eq!(client.put(&path).bearer_auth(token).json(&json!({"gpus":1,"cpus":4})).send().await.unwrap().status(), StatusCode::FORBIDDEN);
        }
        assert_eq!(client.put(&path).bearer_auth(ADMIN).json(&json!({"gpus":1,"cpus":4})).send().await.unwrap().status(), StatusCode::OK);
    }
    assert_eq!(client.delete(format!("{url}/apps/{PROOF}")).bearer_auth(&owner).send().await.unwrap().status(), StatusCode::OK);
    assert!(!c.fleet.app_known(&ns, PROOF));
    assert!(c.fleet.app_known(&other_ns, PROOF));
    let again = client.post(format!("{url}/apps")).bearer_auth(&owner)
        .json(&json!({"name":PROOF,"priority":17,"spec":{"image":"ziren-gpu:1"},
            "proof":{"elf_id":elf,"class":"latency","anchor":"exclusive","backends":["cuda"]}})).send().await.unwrap();
    assert_eq!(again.status(), StatusCode::CREATED);
    let app: Value = again.json().await.unwrap();
    assert_eq!((app["priority"].clone(), app["proof"]["class"].clone(), app["proof"]["anchor"].clone()), (json!(17), json!("latency"), json!("exclusive")));
    for (body, status) in [
        (json!({"name":"twice","spec":{"image":"ziren-gpu:1"},"proof":{"elf_id":elf}}), StatusCode::CONFLICT),
        (json!({"name":PROOF,"spec":{"image":"ziren-gpu:1"},"proof":{"elf_id":"c".repeat(64)}}), StatusCode::CONFLICT),
        (json!({"name":"bad","spec":{"image":"ziren-gpu:1"},"proof":{"elf_id":"c".repeat(64),"class":"fast"}}), StatusCode::BAD_REQUEST),
    ] {
        assert_eq!(client.post(format!("{url}/apps")).bearer_auth(&owner).json(&body).send().await.unwrap().status(), status, "{body}");
    }
    assert_eq!(client.post(format!("{url}/apps")).bearer_auth(&consumer).json(&json!({"name":"mine","spec":{"image":"ziren-gpu:1"},"proof":{"elf_id":"d".repeat(64)}}))
        .send().await.unwrap().status(), StatusCode::FORBIDDEN);
    server.abort();
}
fn request_resources(body: Value) -> ScaleRequest { serde_json::from_value(body).unwrap() }
/// The node writes the configuration a change sends between stopping and starting a entry,
/// answering with `report`.
async fn node_writes_config(c: &WorkloadController, mut report: NodeReport) {
    c.dispatch().await.unwrap();
    let command=c.inner.lock().commands.values().find(|v|!v.done&&v.operation==NodeOperation::App as i32).cloned().expect("configuration step");
    report.results=vec![CommandResult{id:command.id.clone(),result:json!({"updated":["model"]}).into(),error:None}];
    c.report("gpu","session",report).await.unwrap();
    c.dispatch().await.unwrap();
}
/// Plays the node through the pending changes of an app until none is pending.
async fn node_completes_changes(c: &WorkloadController, ns: &str, proof: bool) {
    for _ in 0..100 {
        c.dispatch().await.unwrap();
        let command=c.inner.lock().commands.values().find(|v|!v.done&&v.admitted).cloned();
        match command {
            Some(command) if command.operation==NodeOperation::App as i32 => node_writes_config(c,scale_inventory(ns,proof,true)).await,
            Some(command) => {
                let disabled=command.parameters["disabled"].as_bool().unwrap();
                let mut observed=command.parameters["app"].clone();
                observed["disabled"]=json!(disabled);observed["ready"]=json!(!disabled);observed["cleaned"]=json!(disabled);
                finish_app_command(c,ns,proof,&command,disabled,Some(observed)).await;
            }
            None if c.inner.lock().deployments.values().all(|s|!s.scale.as_ref().is_some_and(ScaleStatus::pending)) => return,
            None => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
        }
    }
    panic!("the change did not finish");
}
async fn finish_app_command(c: &WorkloadController, ns: &str, proof: bool, command: &CommandRecord, disabled: bool, target: Option<Value>,
) {
    let mut report=scale_inventory(ns,proof,disabled);
    if let Some(target)=target { report.apps[0]=target.into(); }
    report.results=vec![CommandResult {id:command.id.clone(),result: (if disabled {json!({"cleaned":true})}else{json!({"ready":true})})
        .into(),error:None,
    }];
    c.report("gpu","session",report).await.unwrap();
}

#[tokio::test]
async fn owner_scale_rejects_invalid_capacity_quota_and_batches() {
    use sha2::Digest;
    let (_dir,c,ns)=setup().await;
    c.report("gpu","session",scale_inventory(&ns,false,false)).await.unwrap();
    let secret="1".repeat(64);
    c.fleet.add_user_token(&"a".repeat(32),&"b".repeat(40),"owner",None,
        Some(hex::encode(sha2::Sha256::digest(secret.as_bytes()))),Some(ns.clone()),Some("scope=default".into()),vec!["read".into(),"submit".into(),"manage".into()],
        ).await.unwrap();
    let path=format!("/apps/model/scale?namespace={ns}");
    assert_eq!(scale_call(c.clone(),&secret,"GET",&path,Value::Null).await.status(),StatusCode::OK);
    assert_eq!(scale_call(c.clone(),"cluster","PUT",&path,json!({"gpus":1})).await.status(),StatusCode::UNAUTHORIZED);
    for body in [json!({"replicas":0}),json!({"replicas":65}),json!({"cpus":0}),json!({"gpus":-1}),json!({"image":"other"}),json!({}),
    ] {
        assert_eq!(scale_call(c.clone(),&secret,"PUT",&path,body).await.status(),StatusCode::BAD_REQUEST);
    }
    for (body, requested) in [
        (json!({"gpus":2}), "requested 4 GPUs and 4 CPUs (replicas: 2)",
        ),
        (json!({"cpus":5}), "requested 2 GPUs and 10 CPUs (replicas: 2)",
        ),
        (json!({"replicas":5}), "requested 5 GPUs and 10 CPUs (replicas: 5)",
        ),
    ] {
        let response = scale_call(c.clone(), &secret, "PUT", &path, body).await;
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let message = String::from_utf8(to_bytes(response.into_body(), 1 << 20).await.unwrap().to_vec(),
        ).unwrap();
        assert!(message.contains("exceeds node gpu capacity"), "{message}");
        assert!(message.contains(requested), "{message}");
        assert!(message.contains("node total 2 GPUs and 8 CPUs"), "{message}");
        assert!(message.contains("currently free 0 GPUs and 4 CPUs"), "{message}");
    }
    c.fleet.set_quota(&ns,Quota{gpus:Some(1.0),cpus:Some(8.0),
            },
        ).await.unwrap();
    assert_eq!(scale_call(c.clone(),&secret,"PUT",&path,json!({"cpus":3})).await.status(),StatusCode::UNPROCESSABLE_ENTITY);
    let other=c.fleet.create_namespace("other",Quota::default()).await.unwrap();
    assert_eq!(scale_call(c.clone(),"admin","PUT",&format!("/apps/model/scale?namespace={}",other.id),json!({"replicas":1})).await.status(),StatusCode::NOT_FOUND);
    assert_eq!(scale_call(c.clone(),"admin","PUT",&format!("/apps/training/scale?namespace={ns}"),json!({"replicas":1})).await.status(),StatusCode::BAD_REQUEST);
    assert!(c.inner.lock().commands.is_empty());
    assert!(c.inner.lock().deployments.values().all(|s|s.scale.is_none()));
}

#[tokio::test]
async fn persistent_scale_survives_restarts_releases_old_units_and_waits_for_new_capacity() {
    let (_dir,c,ns)=setup().await;
    c.report("gpu","session",scale_inventory(&ns,false,false)).await.unwrap();
    assert_eq!(c.fleet.usage(&ns).gpus,2.0);
    let value=c.request_scale(&ns,"model",request_resources(json!({"gpus":2,"cpus":4,"replicas":1})),
        ).await.unwrap();
    assert_eq!(value["state"],"draining");
    for (method, path) in [("PUT", format!("/apps/model/disabled?namespace={ns}")),
        ("DELETE", format!("/apps/model/disabled?namespace={ns}")),
        ("DELETE", format!("/apps/model?namespace={ns}")),
    ] {
        assert_eq!(scale_call(c.clone(),"admin",method,&path,json!({})).await.status(),StatusCode::CONFLICT);
    }
    assert_eq!(c.inner.lock().apps["model"].spec["disabled"],false);
    assert!(c.request_scale(&ns,"model",request_resources(json!({"replicas":1}))).await.is_err());
    let c=Arc::new(WorkloadController::new(c.fleet.clone(), "cluster".into()).unwrap());
    c.report("gpu","session",scale_inventory(&ns,false,false)).await.unwrap();
    c.dispatch().await.unwrap();
    let stop=c.inner.lock().commands.values().find(|v|v.parameters["scale_phase"]=="stop").unwrap().clone();
    assert!(stop.admitted);
    // Replaying a report cannot reopen the old allocation or change the target.
    c.report("gpu","session",scale_inventory(&ns,false,false)).await.unwrap();
    assert_eq!(c.scale_status(&ns,"model",None).unwrap()["gpus"],2.0);
    assert_eq!(c.app_resources(&ns,"model",None)["resources"]["gpus"],2.0,"listings show the durable requested settings while scaling");
    assert_eq!(c.inner.lock().commands.len(),1);
    finish_app_command(&c,&ns,false,&stop,true,None).await;
    assert_eq!(c.fleet.usage(&ns).gpus,0.0);
    // Another workload consumes the freed GPUs; scale waits rather than starting over capacity.
    let mut occupied=scale_inventory(&ns,false,true);occupied.free_gpus=0.0;
    node_writes_config(&c,occupied).await;
    let start=c.inner.lock().commands.values().find(|v|v.parameters["scale_phase"]=="apply").unwrap().clone();
    assert!(!start.admitted);
    assert_eq!(c.fleet.usage(&ns).gpus,0.0);
    let c=Arc::new(WorkloadController::new(c.fleet.clone(), "cluster".into()).unwrap());
    c.report("gpu","session",scale_inventory(&ns,false,true)).await.unwrap();c.dispatch().await.unwrap();
    assert_eq!(c.inner.lock().commands.len(),3);
    assert!(c.inner.lock().commands[&start.id].admitted);
    assert_eq!(c.fleet.usage(&ns).gpus,2.0);
    let mut observed=start.parameters["app"].clone();observed["disabled"]=json!(false);observed["ready"]=json!(true);observed["cleaned"]=json!(false);
    finish_app_command(&c,&ns,false,&start,false,Some(observed)).await;c.dispatch().await.unwrap();
    assert_eq!(c.scale_status(&ns,"model",None).unwrap()["state"],"complete");
    assert_eq!(c.inner.lock().apps["model"].spec["replicas"],1);
    assert_eq!(c.fleet.usage(&ns).gpus,2.0);
    // A node-local edit after completion cannot overwrite the durable desired allocation.
    c.report("gpu","session",scale_inventory(&ns,false,false)).await.unwrap();
    let repaired=c.scale_status(&ns,"model",None).unwrap();
    assert_eq!(repaired["state"],"draining");
    assert_eq!(repaired["gpus"],2.0);
    assert_eq!(repaired["replicas"],1);
    c.dispatch().await.unwrap();
    assert_eq!(c.inner.lock().commands.values().filter(|c|!c.done).count(),1);
}

#[tokio::test]
async fn app_stops_drain_live_and_disconnected_proofs_regardless_of_runtime() {
    use crate::store::{ProofJob, ProofJobState};
    use fleet_proto::fleet::v1::{NodeInfo, ProofKind};

    for (kind, backend, gpus) in [("ziren-gpu", "cuda", 1), ("ziren-cpu", "cpu", 0), ("app", "cpu", 0)] {
        let (_dir, c, ns) = setup().await;
        register(&c, &ns, "model", &proof_elf()).await;
        let mut report = scale_inventory(&ns, true, false);
        report.apps[0]["kind"] = json!(kind);
        report.apps[0]["gpus"] = json!(gpus);
        report.apps[0]["app"] = json!("model");
        c.report("gpu", "session", report.clone()).await.unwrap();
        let node = c.fleet.register(NodeInfo {
            name: "model-0".into(), machine: "machine".into(), backend: backend.into(),
            gpus, cpus: 2, control_version: 1, app_namespace: ns.clone(),
            app: "model".into(), app_elf_id: proof_elf(), ..Default::default()
        }).unwrap();
        c.fleet.heartbeat(&node, true, vec![], true, "active-proof");
        let stop = c.command("gpu", NodeOperation::SetApp,
            json!({"app":report.apps[0],"disabled":true})).unwrap();

        // The live prover's busy report alone fences the stop.
        c.dispatch().await.unwrap();
        assert!(!c.inner.lock().commands[&stop].admitted, "{kind}: live proof");

        let mut job = ProofJob {
            inputs: None, outputs: None, id: "active-proof".into(), app: "model".into(),
            owner: "b".repeat(40), namespace: ns.clone(), tag: String::new(),
            client_id: "active-proof".into(), elf_id: proof_elf(),
            kind: ProofKind::Compressed as i32, priority: 0,
            state: ProofJobState::Running as i32, fail_status: None, error: String::new(),
            node: Some("model-0".into()), attempts: 1, failed_on: vec![], cycles: 0,
            prove_ms: 0, created_ms: 1, started_ms: Some(1), finished_ms: None,
        };
        c.fleet.store.upsert(&job).await.unwrap();
        c.fleet.deregister(&node);
        // A disconnected prover's persisted execution must still fence the stop.
        c.dispatch().await.unwrap();
        assert!(!c.inner.lock().commands[&stop].admitted, "{kind}: disconnected proof");

        job.state = ProofJobState::Succeeded as i32;
        job.finished_ms = Some(2);
        c.fleet.store.upsert(&job).await.unwrap();
        c.dispatch().await.unwrap();
        assert!(c.inner.lock().commands[&stop].admitted, "{kind}: drained");
    }
}

#[tokio::test]
async fn proof_scale_drains_running_execution_before_delivery_and_keeps_disabled_apps_disabled() {
    use crate::store::{ProofJob,ProofJobState};
    use fleet_proto::fleet::v1::ProofKind;
    let (_dir,c,ns)=setup().await;let elf="a".repeat(64);
    register(&c, &ns, PROOF, &elf).await;
    c.report("gpu","session",scale_inventory(&ns,true,false)).await.unwrap();
    let mut job=ProofJob{inputs:None,outputs:None,id:"active-proof".into(),app:PROOF.into(),owner:"b".repeat(40),namespace:ns.clone(),tag:String::new(),client_id:"active-proof".into(),elf_id:elf.clone(),kind:ProofKind::Compressed as i32,priority:0,state:ProofJobState::Running as i32,
        fail_status: None,error:String::new(),node:Some("model-0".into()),attempts:1,failed_on:vec![],cycles:0,prove_ms:0,created_ms:1,started_ms:Some(1),finished_ms:None,
    };
    c.fleet.store.upsert(&job).await.unwrap();
    c.request_scale(&ns,PROOF,request_resources(json!({"replicas":1})),
    ).await.unwrap();
    for method in ["PUT", "DELETE"] {
        assert_eq!(scale_call(c.clone(),"admin",method,&format!("/apps/{PROOF}/disabled?namespace={ns}"),json!({})).await.status(),StatusCode::CONFLICT);
    }
    assert_eq!(scale_call(c.clone(),"admin","DELETE",&format!("/apps/{PROOF}?namespace={ns}"),Value::Null).await.status(),StatusCode::CONFLICT);
    assert!(c.fleet.app_known(&ns, PROOF));
    assert!(!c.fleet.app_disabled(&ns, PROOF));
    c.dispatch().await.unwrap();
    assert!(c.fleet.app_disabled(&ns, PROOF));
    let stop=c.inner.lock().commands.values().find(|v|v.parameters["scale_phase"]=="stop").unwrap().clone();
    assert!(!stop.admitted);
    job.state=ProofJobState::Succeeded as i32;job.finished_ms=Some(2);c.fleet.store.upsert(&job).await.unwrap();
    c.dispatch().await.unwrap();assert!(c.inner.lock().commands[&stop.id].admitted);
    finish_app_command(&c,&ns,true,&stop,true,None).await;c.dispatch().await.unwrap();
    node_writes_config(&c,scale_inventory(&ns,true,true)).await;
    let start=c.inner.lock().commands.values().find(|v|v.parameters["scale_phase"]=="apply").unwrap().clone();
    let mut observed=start.parameters["app"].clone();observed["disabled"]=json!(false);observed["ready"]=json!(true);
    finish_app_command(&c,&ns,true,&start,false,Some(observed)).await;
    assert!(c.fleet.app_disabled(&ns, PROOF));
    c.dispatch().await.unwrap();
    assert!(!c.fleet.app_disabled(&ns, PROOF));
    c.fleet.set_app_disabled(&ns, PROOF,true).await.unwrap();
    // The authoritative lifecycle intent, not an old observed flag, determines whether scaling resumes the app.
    let mut stopped=start.parameters["app"].clone();
    stopped["disabled"]=json!(true);stopped["ready"]=json!(false);stopped["cleaned"]=json!(true);
    c.command("gpu",NodeOperation::SetApp,json!({"app":stopped,"disabled":true}),
    ).unwrap();
    let normal=c.inner.lock().commands.values().find(|v|!v.done).unwrap().clone();
    finish_app_command(&c,&ns,true,&normal,true,Some(stopped)).await;
    c.request_scale(&ns,PROOF,request_resources(json!({"gpus":2,"replicas":1})),
    ).await.unwrap();
    c.dispatch().await.unwrap();
    let stop=c.inner.lock().commands.values().find(|v|!v.done).unwrap().clone();
    finish_app_command(&c,&ns,true,&stop,true,None).await;c.dispatch().await.unwrap();
    node_writes_config(&c,scale_inventory(&ns,true,true)).await;
    let apply=c.inner.lock().commands.values().find(|v|!v.done).unwrap().clone();
    assert_eq!(apply.parameters["disabled"],true);
    let mut observed=apply.parameters["app"].clone();observed["disabled"]=json!(true);observed["cleaned"]=json!(true);observed["ready"]=json!(false);
    finish_app_command(&c,&ns,true,&apply,true,Some(observed)).await;c.dispatch().await.unwrap();
    assert!(c.fleet.app_disabled(&ns, PROOF));
    assert_eq!(c.scale_status(&ns,PROOF,None).unwrap()["state"],"complete");
}

#[tokio::test]
async fn scale_failure_retains_new_reservation_until_confirmed_cleanup_and_can_be_retried() {
    let (_dir,c,ns)=setup().await;
    c.report("gpu","session",scale_inventory(&ns,false,false)).await.unwrap();
    c.request_scale(&ns,"model",request_resources(json!({"gpus":2,"cpus":4,"replicas":1})),
    ).await.unwrap();
    c.dispatch().await.unwrap();
    let stop=c.inner.lock().commands.values().find(|v|!v.done).unwrap().clone();
    finish_app_command(&c,&ns,false,&stop,true,None).await;c.dispatch().await.unwrap();
    node_writes_config(&c,scale_inventory(&ns,false,true)).await;
    let apply=c.inner.lock().commands.values().find(|v|!v.done).unwrap().clone();
    assert!(apply.admitted);assert_eq!(c.fleet.usage(&ns).gpus,2.0);
    let mut failed=scale_inventory(&ns,false,true);
    let mut partial=apply.parameters["app"].clone();partial["disabled"]=json!(false);partial["ready"]=json!(false);partial["cleaned"]=json!(false);
    failed.apps=vec![partial.clone().into()];
    failed.results=vec![CommandResult {id:apply.id,error:Some("container start failed".into()),result:Value::Null.into(),
    }];
    c.report("gpu","session",failed).await.unwrap();c.dispatch().await.unwrap();
    assert_eq!(c.scale_status(&ns,"model",None).unwrap()["state"],"failed");
    assert_eq!(c.fleet.usage(&ns).gpus,2.0);
    let mut report=scale_inventory(&ns,false,true);report.apps=vec![partial.into()];
    c.report("gpu","session",report).await.unwrap();c.dispatch().await.unwrap();
    // The failed app stays down: the node writes it disabled and stops it.
    let config=command_where(&c,|v|!v.done&&v.operation==NodeOperation::App as i32).unwrap();
    assert_eq!(config.parameters["set"],json!({"disabled":true}));
    let cleanup=command_where(&c,|v|!v.done&&v.operation==NodeOperation::SetApp as i32).unwrap();
    assert_eq!(cleanup.parameters["disabled"],true);
    let mut observed=cleanup.parameters["app"].clone();observed["disabled"]=json!(true);observed["ready"]=json!(false);observed["cleaned"]=json!(true);
    finish_app_command(&c,&ns,false,&cleanup,true,Some(observed)).await;
    assert_eq!(c.fleet.usage(&ns).gpus,0.0);
    assert_eq!(c.scale_status(&ns,"model",None).unwrap()["state"],"failed");
    let retried=c.request_scale(&ns,"model",request_resources(json!({"replicas":1}))).await.unwrap();
    assert_eq!(retried["state"],"draining");assert_eq!(retried["disabled"],false);
}

#[tokio::test]
async fn public_nodes_are_machines_and_machine_drain_covers_current_and_future_proof_replicas() {
    use fleet_proto::fleet::v1::NodeInfo;
    let (_dir,c,ns)=setup().await;
    let elf="a".repeat(64);
    register(&c, &ns, PROOF, &elf).await;
    let mut report=inventory(&ns);
    report.apps=vec![json!({"name":"proof","kind":"ziren-gpu","namespace":ns,
        "app":PROOF.to_string(),"gpus":1,"cpus":2,"replicas":1,"disabled":false,"ready":true}).into()];
    c.report("gpu","session",report).await.unwrap();
    let replica=NodeInfo{name:"proof-0".into(),machine:"machine".into(),backend:"cuda".into(),
        gpus:1,cpus:2,control_version:1,app_namespace:ns,app:PROOF.into(),app_elf_id:elf.clone(),..Default::default()};
    let id=c.fleet.register(replica.clone()).unwrap();
    c.fleet.heartbeat(&id,true,vec![],true,"proof-job");
    c.fleet.register(NodeInfo{name:"other-0".into(),machine:"other-machine".into(),..replica.clone()}).unwrap();
    let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url=format!("http://{}",listener.local_addr().unwrap());
    let router=crate::http::router(crate::http::HttpState{fleet:c.fleet.clone(),workloads:c.clone()
    });
    let server=tokio::spawn(async move { axum::serve(listener,router).await.unwrap(); });
    let client=reqwest::Client::new();
    let status:Value=client.get(format!("{url}/status")).bearer_auth(ADMIN).send().await.unwrap().json().await.unwrap();
    assert_eq!(status["nodes"].as_array().unwrap().len(),1);
    assert_eq!(status["nodes"][0]["name"],"gpu");
    assert_eq!(status["nodes"][0]["gpus"],2.0);
    assert_eq!(status["nodes"][0]["cpus"],8.0);
    assert_eq!(status["nodes"][0]["busy"],"~proof-job");
    assert_eq!(status["nodes"][0]["apps"],json!([PROOF.to_string()]));
    for method in [reqwest::Method::POST,reqwest::Method::DELETE] {
        assert_eq!(client.request(method,format!("{url}/nodes/proof-0/drain")).bearer_auth(ADMIN).send().await.unwrap().status(),StatusCode::NOT_FOUND);
    }
    assert_eq!(client.post(format!("{url}/nodes/gpu/drain")).bearer_auth(ADMIN).send().await.unwrap().status(),StatusCode::NO_CONTENT);
    assert!(c.capacity("gpu").is_none());
    let new=c.fleet.register(NodeInfo{name:"proof-1".into(),..replica}).unwrap();
    c.fleet.heartbeat(&new,true,vec![],false,"");
    assert!(c.fleet.status().nodes.iter().all(|n|n.draining==(n.machine=="machine")));
    assert_eq!(c.worker_status()[0]["busy"],"~proof-job","draining must preserve the active proof");
    assert_eq!(client.delete(format!("{url}/nodes/gpu/drain")).bearer_auth(ADMIN).send().await.unwrap().status(),StatusCode::NO_CONTENT);
    assert!(c.capacity("gpu").is_some());
    assert!(c.fleet.status().nodes.iter().all(|n|!n.draining));
    server.abort();
}

#[tokio::test]
async fn obsolete_routes_and_nonlocal_log_ranks_are_refused() {
    let (_dir, c, ns) = setup().await;
    c.report("gpu", "session", inventory(&ns)).await.unwrap();
    let job = submit(&c).await;
    for path in ["/jobs", "/models"] {
        for method in ["GET", "POST", "DELETE"] {
            let response = scale_call(c.clone(), "admin", method, path, json!({})).await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{method} {path}");
        }
    }
    let path = format!("/batches/training/jobs/{}/logs?rank=1", job.id);
    let response = scale_call(c, "admin", "GET", &path, Value::Null).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn integrated_workers_reserve_resources_reject_scaling_and_restore_admission_guard() {
    let (_dir, c, ns) = setup().await;
    let mut report = scale_inventory(&ns, true, false);
    report.apps[0]["worker_command"] = json!(["/opt/eth-proofs"]);
    report.apps[0]["machine"] = json!("machine");
    report.apps[0]["replicas"] = json!(1);
    c.report("gpu", "session", report).await.unwrap();
    assert!(c.fleet.app_worker_owned(&ns, PROOF));
    assert!(!c.fleet.app_worker_owned(&ns, "other"));
    assert_eq!(c.fleet.usage(&ns).gpus, 1.0);
    assert_eq!(c.fleet.usage(&ns).cpus, 2.0);
    assert!(c.request_scale(&ns, PROOF, request_resources(json!({"replicas":2}))).await.is_err());
    c.fleet.set_worker_owned_apps(HashSet::new());
    let _restarted = WorkloadController::new(c.fleet.clone(), "cluster".into()).unwrap();
    assert!(c.fleet.app_worker_owned(&ns, PROOF));
}

#[tokio::test]
async fn integrated_worker_owner_must_be_cleaned_before_moving() {
    let (_dir, c, ns) = setup().await;
    let mut report = scale_inventory(&ns, true, false);
    report.apps[0]["worker_command"] = json!(["/opt/eth-proofs"]);
    report.apps[0]["replicas"] = json!(1);
    let mut invalid = report.clone();
    invalid.apps[0]["replicas"] = json!(2);
    assert!(c.report("gpu", "session", invalid).await.is_err());
    let mut duplicate = report.clone();
    let mut another = duplicate.apps[0].clone();
    another["name"] = json!("second");
    duplicate.apps.push(another);
    assert!(c.report("gpu", "session", duplicate).await.is_err());
    c.report("gpu", "session", report.clone()).await.unwrap();
    assert!(c.report("other", "session", report.clone()).await.is_err());
    let entry = report.apps[0].clone();
    assert!(c.command("other", NodeOperation::SetApp,
        json!({"app":entry,"disabled":false})).is_err());
    c.inner.lock().deployments.get_mut("gpu-model").unwrap().disabled = true;
    assert!(c.command("other", NodeOperation::SetApp,
        json!({"app":entry,"disabled":false})).is_err());
    c.fleet.release_job(&ns, "deployment:gpu-model").await.unwrap();
    assert!(c.command("other", NodeOperation::SetApp,
        json!({"app":entry,"disabled":false})).is_ok());
}

#[tokio::test]
async fn an_app_operation_replaces_an_enable_still_waiting_for_capacity() {
    let (_dir, c, ns) = setup().await;
    let report = scale_inventory(&ns, false, true);
    let entry = report.apps[0].clone();
    c.report("gpu", "session", report).await.unwrap();
    let enable = c.command("gpu", NodeOperation::SetApp, json!({"app":entry,"disabled":false})).unwrap();
    assert!(!c.inner.lock().commands[&enable].admitted, "an enable waits until the node can fit it");
    let disable = c.command("gpu", NodeOperation::SetApp, json!({"app":entry,"disabled":true})).unwrap();
    let commands = c.inner.lock().commands.clone();
    assert!(commands[&enable].done && commands[&enable].error.as_deref().unwrap().contains("replaced"));
    assert!(!commands[&disable].done && c.inner.lock().deployments["gpu-model"].disabled);
    // Once the node has been sent an operation, it is not replaced.
    c.inner.lock().commands.update(&disable, |c| c.admitted = true).unwrap();
    let error = c.command("gpu", NodeOperation::SetApp, json!({"app":entry,"disabled":false})).unwrap_err();
    assert!(error.to_string().contains("already pending"), "{error}");
}

#[tokio::test]
async fn app_logs_are_relayed_from_the_node_and_follow_app_scope() {
    for proof in [false, true] {
        let (_dir, c, ns) = setup().await;
        let elf = "a".repeat(64);
        register(&c, &ns, PROOF, &elf).await;
        c.report("gpu", "session", scale_inventory(&ns, proof, false)).await.unwrap();
        c.fleet.log_relay.heartbeat("gpu", "session");
        let app = if proof { PROOF.to_string() } else { "model".into() };
        let consumer = user_token(&c, &ns, 2, "scope=default", Some(vec![app]), false).await;
        let restricted = user_token(&c, &ns, 3, "scope=default", Some(vec!["other".into()]), true).await;
        let foreign_tag = user_token(&c, &ns, 4, "scope=research", None, true).await;
        let logs = if proof { format!("/apps/{PROOF}/logs") } else { "/apps/model/logs".into() };
        let query = |extra: &str| format!("{logs}?{extra}");
        for token in [&restricted, &foreign_tag] {
            let status = scale_call(c.clone(), token, "GET", &logs, Value::Null).await.status();
            assert!(matches!(status, StatusCode::FORBIDDEN | StatusCode::NOT_FOUND), "{status}");
        }
        // A write needs manage; even with it, logs are only read.
        assert_eq!(scale_call(c.clone(), &consumer, "POST", &logs, Value::Null).await.status(), StatusCode::FORBIDDEN);
        assert_eq!(scale_call(c.clone(), "admin", "POST", &format!("{logs}?namespace={ns}"), Value::Null).await.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(scale_call(c.clone(), &consumer, "GET", &query("lines=many"), Value::Null).await.status(), StatusCode::BAD_REQUEST);

        let (control, token, path) = (c.clone(), consumer.clone(), query("lines=5"));
        let request = tokio::spawn(async move { scale_call(control, &token, "GET", &path, Value::Null).await });
        let asked = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if let Some(r) = c.fleet.log_relay.heartbeat("gpu", "session").into_iter().next() { break r; }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }).await.unwrap();
        assert_eq!((asked.entry.as_str(), asked.lines), ("model", 5));
        c.fleet.log_relay.claim(&asked.ticket, "gpu", "session").unwrap().send(Ok(b"hello\n".to_vec())).unwrap();
        let response = request.await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(axum::body::to_bytes(response.into_body(), 64).await.unwrap().as_ref(), b"hello\n");
    }
}

#[tokio::test]
async fn app_logs_of_an_offline_node_are_unavailable() {
    let (_dir, c, ns) = setup().await;
    c.report("gpu", "session", scale_inventory(&ns, false, false)).await.unwrap();
    let status = scale_call(c, "admin", "GET", &format!("/apps/model/logs?namespace={ns}"), Value::Null).await.status();
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn a_starting_app_says_why_it_is_not_running() {
    let (_dir, c, ns) = setup().await;
    let mut report = scale_inventory(&ns, false, true);
    report.free_gpus = 1.0;
    report.free_cpus = 3.0;
    let entry = report.apps[0].clone();
    c.report("gpu", "session", report).await.unwrap();
    c.command("gpu", NodeOperation::SetApp, json!({"app":entry,"disabled":false})).unwrap();
    let view = c.app_resources(&ns, "model", None);
    assert_eq!(view["state"], "starting");
    assert_eq!(view["detail"], "waiting for capacity: needs 2 GPU and 4 CPU, gpu has 1 GPU and 3 CPU free");
    c.inner.lock().commands.update_where(|_| true, |c| c.admitted = true);
    assert_eq!(c.app_resources(&ns, "model", None)["detail"], "starting on gpu");
    c.inner.lock().commands.update_where(|_| true, |c| c.done = true);
    assert_eq!(c.app_resources(&ns, "model", None)["detail"], "waiting for gpu to report it ready");
}

#[tokio::test]
async fn enabling_an_integrated_worker_without_a_registered_app_keeps_heartbeats_accepted() {
    let (_dir,c,ns)=setup().await;
    let report=scale_inventory(&ns,true,true);
    assert!(!c.fleet.app_known(&ns, PROOF));
    c.report("gpu","session",report.clone()).await.unwrap();
    let mut entry:Value=report.apps[0].clone().into();entry["disabled"]=json!(false);
    let id=c.command("gpu",NodeOperation::SetApp,json!({"app":entry,"disabled":false})).unwrap();
    let command=c.inner.lock().commands.get(&id).cloned().unwrap();
    finish_app_command(&c,&ns,true,&command,false,None).await;
    assert!(c.inner.lock().commands[&id].done,"the enable result must be recorded, not rejected with the whole heartbeat");
    c.report("gpu","session",scale_inventory(&ns,true,false)).await.unwrap();
}

#[tokio::test]
async fn a_scale_waiting_to_start_says_why() {
    let (_dir,c,ns)=setup().await;
    let mut report=scale_inventory(&ns,false,true);
    report.free_gpus=1.0;report.free_cpus=3.0;
    let entry=report.apps[0].clone();
    c.report("gpu","session",report).await.unwrap();
    let id=c.command("gpu",NodeOperation::SetApp,json!({"app":entry,"disabled":false})).unwrap();
    {
        let mut i=c.inner.lock();
        let intent=i.deployments.get_mut("gpu-model").unwrap();
        let mut scale=scaling::new_scale(intent.spec.clone(),false);
        scale.phase=fleet_proto::model::v1::ScalePhase::Starting as i32;scale.start=Some(id);
        intent.scale=Some(scale);
    }
    let view=c.app_resources(&ns,"model",None);
    assert_eq!(view["state"],"starting");
    assert_eq!(view["detail"],"waiting for capacity: needs 2 GPU and 4 CPU, gpu has 1 GPU and 3 CPU free");
}

#[tokio::test]
async fn a_scale_waiting_for_capacity_to_start_can_be_resized() {
    let (_dir,c,ns)=setup().await;
    let mut report=scale_inventory(&ns,false,true);
    report.free_gpus=1.0;report.free_cpus=3.0;
    let entry=report.apps[0].clone();
    c.report("gpu","session",report).await.unwrap();
    let start=c.command("gpu",NodeOperation::SetApp,json!({"app":entry,"disabled":false})).unwrap();
    {
        let mut i=c.inner.lock();
        let intent=i.deployments.get_mut("gpu-model").unwrap();
        let mut scale=scaling::new_scale(intent.spec.clone(),false);
        scale.phase=fleet_proto::model::v1::ScalePhase::Starting as i32;scale.start=Some(start.clone());
        intent.scale=Some(scale);
    }
    let path=format!("/apps/model/scale?namespace={ns}");
    assert_eq!(scale_call(c.clone(),"admin","PUT",&path,json!({"gpus":0,"cpus":1})).await.status(),StatusCode::ACCEPTED);
    c.advance_scales().await.unwrap();
    let old=c.inner.lock().commands[&start].clone();
    assert!(old.done && old.error.as_deref().is_some_and(|e|e.contains("replaced")),"{old:?}");
    c.inner.lock().commands.update(&start, |c| { c.admitted = true; c.done = false; }).unwrap();
    c.inner.lock().deployments.get_mut("gpu-model").unwrap().scale.as_mut().unwrap().start=Some(start);
    c.inner.lock().deployments.get_mut("gpu-model").unwrap().scale.as_mut().unwrap().phase=fleet_proto::model::v1::ScalePhase::Starting as i32;
    assert_eq!(scale_call(c.clone(),"admin","PUT",&path,json!({"cpus":2})).await.status(),StatusCode::CONFLICT,"a start already sent to the node must finish first");
}

/// One integrated worker (a dedicated proof server running the app's own worker).
fn integrated_inventory(ns: &str) -> NodeReport {
    let mut report=scale_inventory(ns,true,false);
    report.apps[0]["worker_command"]=json!(["/opt/eth-proofs"]);
    report.apps[0]["replicas"]=json!(1);
    report.apps[0]["machine"]=json!("machine");
    report
}

#[tokio::test]
async fn one_conflicting_scale_defers_without_stalling_the_dispatch() {
    let (_dir,c,ns)=setup().await;
    // "gpu" keeps an integrated worker of `model` running: it owns the app.
    c.report("gpu","session",integrated_inventory(&ns)).await.unwrap();
    // "cpu" reports the same app disabled and cleaned, plus an unrelated plain app.
    let mut report=integrated_inventory(&ns);
    report.free_gpus=2.0;report.free_cpus=8.0;
    report.apps[0]["disabled"]=json!(true);
    report.apps[0]["cleaned"]=json!(true);
    let mut other=report.apps[0].clone();
    other["name"]=json!("other");
    other["app"]=json!("other");
    other["kind"]=json!("app");
    other["worker_command"]=json!(null);
    other["replicas"]=json!(1);
    report.apps.push(other);
    c.report("cpu","session",report).await.unwrap();
    // Both deployments hold a start: `model` on cpu conflicts with the owner on gpu, `other` does not.
    for key in ["cpu-model","cpu-other"] {
        let mut i=c.inner.lock();
        let intent=i.deployments.get_mut(key).unwrap();
        let mut scale=scaling::new_scale(intent.spec.clone(),false);
        scale.phase=fleet_proto::model::v1::ScalePhase::Starting as i32;
        intent.scale=Some(scale);
    }
    // The conflict defers instead of stalling: the pass and the whole dispatch succeed, the
    // conflicting start is still uncreated, and the other app's start was created.
    c.advance_scales().await.unwrap();
    c.dispatch().await.unwrap();
    let i=c.inner.lock();
    let stuck=i.deployments["cpu-model"].scale.as_ref().unwrap();
    assert!(stuck.pending() && stuck.start.is_none());
    assert!(i.deployments["cpu-other"].scale.as_ref().unwrap().start.is_some());
}

#[tokio::test]
async fn a_start_whose_deployment_went_away_is_retired_before_it_places_the_app() {
    let (_dir,c,ns)=setup().await;
    let report=scale_inventory(&ns,false,true);
    c.report("gpu","session",report.clone()).await.unwrap();
    c.report("cpu","session",report).await.unwrap();
    // A start of gpu's scale, not yet admitted.
    {
        let mut i=c.inner.lock();
        let intent=i.deployments.get_mut("gpu-model").unwrap();
        let mut scale=scaling::new_scale(intent.spec.clone(),false);
        scale.phase=fleet_proto::model::v1::ScalePhase::Starting as i32;
        intent.scale=Some(scale);
    }
    c.advance_scales().await.unwrap();
    let old=c.inner.lock().deployments["gpu-model"].scale.as_ref().unwrap().start.clone().unwrap();
    // The app moves to cpu, as a placed start does: gpu's deployment goes away and cpu's
    // takes the current operation. The same-worker replacement in command() cannot see this.
    {
        let mut i=c.inner.lock();
        i.deployments.remove("gpu-model");
        let intent=i.deployments.get_mut("cpu-model").unwrap();
        intent.scale=Some(scaling::new_scale(intent.spec.clone(),false));
    }
    c.dispatch().await.unwrap();
    let retired=c.inner.lock().commands[&old].clone();
    assert!(retired.done && !retired.admitted);
    assert_eq!(retired.error.as_deref(),Some("superseded by a newer change"));
    assert!(c.inner.lock().deployments["cpu-model"].scale.as_ref().unwrap().pending());
}

#[tokio::test]
async fn a_disabled_app_its_node_stopped_reporting_is_forgotten() {
    let (_dir,c,ns)=setup().await;
    let record=c.dir.join("deployments").join("gpu-model.json");
    c.report("gpu","session",integrated_inventory(&ns)).await.unwrap();
    c.report("gpu","session",inventory(&ns)).await.unwrap();
    assert!(c.inner.lock().deployments.contains_key("gpu-model") && record.exists(),"an enabled app may only be missing for now");
    c.inner.lock().deployments.get_mut("gpu-model").unwrap().disabled=true;
    c.fleet.release_job(&ns,"deployment:gpu-model").await.unwrap();
    c.report("gpu","session",inventory(&ns)).await.unwrap();
    assert!(!c.inner.lock().deployments.contains_key("gpu-model"));
    assert!(!record.exists());
}

fn remove_commands(c: &WorkloadController, operation: NodeOperation) -> Vec<(String, Value)> {
    let mut commands: Vec<(String, Value)> = c.inner.lock().commands.values()
        .filter(|cmd| cmd.operation == operation as i32 && !cmd.done && cmd.admitted)
        .map(|cmd| (cmd.worker.clone(), (*cmd.parameters).clone()))
        .collect();
    commands.sort_by(|a, b| a.0.cmp(&b.0));
    commands
}

#[tokio::test]
async fn deleting_a_registered_app_tells_every_node_which_app_was_deleted() {
    let (_dir,c,ns)=setup().await;
    c.report("gpu","session",inventory(&ns)).await.unwrap();
    c.report("cpu","session",inventory(&ns)).await.unwrap();
    let app=PROOF.to_string();
    c.remove_app(&ns,&app).unwrap();
    c.remove_app(&ns,&app).unwrap();
    let expected=json!({"app":{"namespace":ns,"namespace_name":"test","name":app},"remove":true});
    assert_eq!(remove_commands(&c,NodeOperation::App),vec![("cpu".to_string(),expected.clone()),("gpu".to_string(),expected)]);
}

#[tokio::test]
async fn deleting_an_app_tells_its_nodes() {
    let (_dir,c,ns)=setup().await;
    c.report("gpu","session",scale_inventory(&ns,false,true)).await.unwrap();
    c.inner.lock().apps.get_mut("model").unwrap().spec["disabled"]=json!(true);
    assert_eq!(scale_call(c.clone(),"admin","DELETE",&format!("/apps/model?namespace={ns}"),Value::Null).await.status(),StatusCode::OK);
    assert_eq!(remove_commands(&c,NodeOperation::App),vec![("gpu".to_string(),json!({"app":{"namespace":ns,"namespace_name":"test","name":"model"},"remove":true}))]);
}

#[tokio::test]
async fn deleting_a_batch_sends_remove_batch_and_keeps_its_jobs() {
    let (_dir,c,ns)=setup().await;
    c.report("gpu","session",inventory(&ns)).await.unwrap();
    let job=submit(&c).await;
    c.dispatch().await.unwrap();
    let job=c.job(&job.id,None,&BTreeMap::new()).unwrap();
    let mut report=inventory(&ns);
    report.jobs.push(incoming(&job,"succeeded",true));
    c.report("gpu","session",report).await.unwrap();
    assert_eq!(scale_call(c.clone(),"admin","DELETE",&format!("/batches/training?namespace={ns}"),Value::Null).await.status(),StatusCode::OK);
    assert_eq!(remove_commands(&c,NodeOperation::App),vec![("gpu".to_string(),json!({"batch":{"name":"training"},"remove":true}))]);
    assert!(c.job(&job.id,None,&BTreeMap::new()).is_ok(),"job history and outputs follow the job's own lifecycle");
    let mut late=incoming(&job,"succeeded",true);late.id="other-job".into();
    let mut report=inventory(&ns);report.jobs.push(late);
    c.report("gpu","session",report).await.unwrap();
    assert!(c.job("other-job",None,&BTreeMap::new()).is_err(),"a deleted batch imports no jobs");
}

fn command_where(c: &WorkloadController, pick: impl Fn(&CommandRecord) -> bool) -> Option<CommandRecord> {
    c.inner.lock().commands.values().find(|cmd| pick(cmd)).cloned()
}
async fn node_answers(c: &WorkloadController, ns: &str, command: &CommandRecord, result: Value) {
    let mut report=scale_inventory(ns,false,true);
    report.results=vec![CommandResult{id:command.id.clone(),result:result.into(),error:None}];
    c.report("gpu","session",report).await.unwrap();
    c.dispatch().await.unwrap();
}

#[tokio::test]
async fn an_app_update_drains_has_the_node_write_the_settings_then_restarts() {
    let (_dir,c,ns)=setup().await;
    c.report("gpu","session",scale_inventory(&ns,false,false)).await.unwrap();
    for bad in [json!({}),json!({"gpus":1}),json!({"image":""}),json!({"image":"--privileged"}),json!({"env":{"A":1}})] {
        let refused=c.request_update(&ns,"model",None,bad.clone()).await.unwrap_err();
        assert_eq!(refused.downcast_ref::<crate::fleet::RequestRejection>().unwrap().status,400,"{bad}");
    }
    let set=json!({"image":"web:2","env":{"A":"1"}});
    assert_eq!(c.request_update(&ns,"model",None,set.clone()).await.unwrap()["state"],"draining");
    assert!(c.request_update(&ns,"model",None,json!({"image":"web:3"})).await.is_err(),"one operation at a time");
    c.dispatch().await.unwrap();
    let stop=command_where(&c,|cmd|cmd.parameters["scale_phase"]=="stop").unwrap();
    finish_app_command(&c,&ns,false,&stop,true,None).await;
    c.dispatch().await.unwrap();
    let update=command_where(&c,|cmd|cmd.operation==NodeOperation::App as i32).unwrap();
    assert_eq!(*update.parameters,json!({"entry":{"name":"model"},
        "set":{"image":"web:2","env":{"A":"1"},"gpus":1,"cpus":2,"replicas":2,"disabled":false}}));
    assert!(command_where(&c,|cmd|cmd.parameters["scale_phase"]=="apply").is_none(),"the app starts only after the node wrote the settings");
    node_answers(&c,&ns,&update,json!({"updated":["model"]})).await;
    let start=command_where(&c,|cmd|cmd.parameters["scale_phase"]=="apply").unwrap();
    assert_eq!(start.parameters["app"]["image"],"web:2");
    let mut observed=start.parameters["app"].clone();
    observed["disabled"]=json!(false);observed["ready"]=json!(true);observed["cleaned"]=json!(false);
    finish_app_command(&c,&ns,false,&start,false,Some(observed)).await;c.dispatch().await.unwrap();
    assert_eq!(c.scale_status(&ns,"model",None).unwrap()["state"],"complete");
    assert_eq!(c.inner.lock().apps["model"].spec["image"],"web:2");
    assert_eq!(c.inner.lock().apps["model"].spec["env"],json!({"A":"1"}));
}

#[tokio::test]
async fn an_update_the_node_cannot_write_fails_and_leaves_the_app_stopped() {
    let (_dir,c,ns)=setup().await;
    c.report("gpu","session",scale_inventory(&ns,false,false)).await.unwrap();
    c.request_update(&ns,"model",None,json!({"image":"web:2"})).await.unwrap();
    c.dispatch().await.unwrap();
    let stop=command_where(&c,|cmd|cmd.parameters["scale_phase"]=="stop").unwrap();
    finish_app_command(&c,&ns,false,&stop,true,None).await;c.dispatch().await.unwrap();
    let update=command_where(&c,|cmd|cmd.operation==NodeOperation::App as i32).unwrap();
    node_answers(&c,&ns,&update,json!({"updated":[]})).await;
    let status=c.scale_status(&ns,"model",None).unwrap();
    assert_eq!((status["state"].as_str(),status["error"].as_str()),(Some("failed"),Some("node did not write the new configuration")));
    assert!(command_where(&c,|cmd|cmd.parameters["scale_phase"]=="apply").is_none());
}

#[tokio::test]
async fn a_batch_update_is_checked_kept_by_the_scheduler_and_sent_to_its_nodes() {
    let (_dir,c,ns)=setup().await;
    let mut report=inventory(&ns);
    report.batches.insert("trainer".into(),json!({"image":"t:1","namespace":ns,"priority":0,"user":null,"max_gpus":2,
        "default_gpu_mem_gb":0,"shm_gb":16,"allow":["/srv"],"env":{},"disabled":false,"preemptible":true,
        "min_runtime_s":0,"stop_timeout_s":30}).into());
    c.report("gpu","session",report).await.unwrap();
    let path=format!("/batches/trainer/settings?namespace={ns}");
    for bad in [json!({}),json!({"name":"other"}),json!({"allow":["relative"]}),json!({"image":"--privileged"})] {
        assert_eq!(scale_call(c.clone(),"admin","PUT",&path,bad.clone()).await.status(),StatusCode::BAD_REQUEST,"{bad}");
    }
    assert_eq!(c.inner.lock().apps["trainer"].spec["image"],"t:1","a refused update changes nothing");
    let set=json!({"image":"t:2","allow":["/srv","/data"]});
    assert_eq!(scale_call(c.clone(),"admin","PUT",&path,set.clone()).await.status(),StatusCode::OK);
    assert_eq!(c.inner.lock().apps["trainer"].spec["image"],"t:2");
    assert_eq!(remove_commands(&c,NodeOperation::App),vec![("gpu".to_string(),json!({"batch":{"name":"trainer"},"set":set}))]);
    let job=c.submit("trainer",json!({"command":["true"],"gpus":0,"cpus":1}),None,&BTreeMap::new()).await.unwrap();
    assert_eq!(c.job(text(&job,"id"),None,&BTreeMap::new()).unwrap().policy["image"],"t:2","new jobs take the new settings");
}

#[tokio::test]
async fn a_registered_app_update_changes_its_server() {
    let (_dir,c,ns)=setup().await;
    let elf="a".repeat(64);
    register(&c, &ns, PROOF, &elf).await;
    c.report("gpu","session",scale_inventory(&ns,true,false)).await.unwrap();
    let path=format!("/apps/{PROOF}/settings?namespace={ns}");
    assert_eq!(scale_call(c.clone(),"admin","GET",&path,Value::Null).await.status(),StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(scale_call(c.clone(),"admin","PUT",&path,json!({"image":"prover:2"})).await.status(),StatusCode::ACCEPTED);
    let scale=c.inner.lock().deployments["gpu-model"].scale.clone().unwrap();
    assert_eq!((scale.set.as_ref().map(|s|s.0.clone()),scale.target["image"].as_str()),(Some(json!({"image":"prover:2"})),Some("prover:2")));
}

async fn answer(c: &WorkloadController, mut report: NodeReport, command: &CommandRecord, result: Value) {
    report.results=vec![CommandResult{id:command.id.clone(),result:result.into(),error:None}];
    c.report("gpu","session",report).await.unwrap();
    c.dispatch().await.unwrap();
}
async fn change_pending(c: &WorkloadController, disabled: bool) {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !c.inner.lock().deployments.values().any(|s| s.scale.as_ref().is_some_and(|s| s.pending() && s.disabled == disabled)) {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }).await.unwrap();
}

#[tokio::test]
async fn disabling_an_app_whose_node_is_offline_is_kept_until_it_returns() {
    let (_dir,c,ns)=setup().await;
    c.report("gpu","session",scale_inventory(&ns,false,false)).await.unwrap();
    c.inner.lock().workers.get_mut("gpu").unwrap().seen=0.0;
    assert_eq!(scale_call(c.clone(),"admin","PUT",&format!("/apps/model/disabled?namespace={ns}"),Value::Null).await.status(),StatusCode::OK);
    let scale=c.inner.lock().deployments["gpu-model"].scale.clone().unwrap();
    assert!(scale.pending() && scale.disabled,"the node stops the app when it reconnects");
}

#[tokio::test]
async fn an_enable_waiting_for_capacity_can_be_cancelled() {
    let (_dir,c,ns)=setup().await;
    let mut occupied=scale_inventory(&ns,false,true);occupied.free_gpus=0.0;occupied.free_cpus=0.0;
    c.report("gpu","session",occupied.clone()).await.unwrap();
    let path=format!("/apps/model/disabled?namespace={ns}");
    let enable=tokio::spawn({let (c,path)=(c.clone(),path.clone());async move{scale_call(c,"admin","DELETE",&path,Value::Null).await}});
    change_pending(&c,false).await;
    c.dispatch().await.unwrap();
    let stop=command_where(&c,|v|!v.done&&v.parameters["scale_phase"]=="stop").unwrap();
    answer(&c,occupied.clone(),&stop,json!({"cleaned":true,"disabled":true})).await;
    node_writes_config(&c,occupied).await;
    let start=command_where(&c,|v|!v.done&&v.parameters["scale_phase"]=="apply").unwrap();
    assert!(!start.admitted,"no capacity");
    let disable=tokio::spawn({let (c,path)=(c.clone(),path.clone());async move{scale_call(c,"admin","PUT",&path,Value::Null).await}});
    change_pending(&c,true).await;
    node_completes_changes(&c,&ns,false).await;
    assert_eq!(disable.await.unwrap().status(),StatusCode::OK);
    enable.abort();
    assert!(c.inner.lock().commands[&start.id].error.as_deref().is_some_and(|e|e.contains("replaced")),"the start was never sent");
    assert!(c.inner.lock().deployments["gpu-model"].disabled);
}

#[tokio::test]
async fn an_enable_carries_the_apps_current_priority() {
    let (_dir,c,ns)=setup().await;
    c.report("gpu","session",scale_inventory(&ns,false,true)).await.unwrap();
    assert_eq!(scale_call(c.clone(),"admin","PUT",&format!("/apps/model/priority?namespace={ns}"),json!({"priority":31})).await.status(),StatusCode::OK);
    let path=format!("/apps/model/disabled?namespace={ns}");
    let enable=tokio::spawn({let c=c.clone();async move{scale_call(c,"admin","DELETE",&path,Value::Null).await}});
    change_pending(&c,false).await;
    node_completes_changes(&c,&ns,false).await;
    assert_eq!(enable.await.unwrap().status(),StatusCode::OK);
    let start=command_where(&c,|v|v.parameters["scale_phase"]=="apply").unwrap();
    assert_eq!(start.parameters["app"]["priority"],31,"admission and preemption use the app's priority");
}

#[tokio::test]
async fn an_admin_token_has_no_namespace_and_others_need_one() {
    let (_dir, c, ns) = setup().await;
    let hash = |seed: &str| Some(hex::encode(sha2::Sha256::digest(seed.repeat(64).as_bytes())));
    let add = |id: &str, permissions: &[&str], namespace: Option<&str>, tag: Option<&str>, apps: Option<Vec<String>>| {
        let (id, permissions) = (id.repeat(16), permissions.iter().map(|p| p.to_string()).collect::<Vec<_>>());
        let (namespace, tag) = (namespace.map(str::to_owned), tag.map(str::to_owned));
        let fleet = c.fleet.clone();
        let token_hash = hash(&id[..1]);
        async move { fleet.add_user_token(&id, &id[..2].repeat(20), "t", apps, token_hash, namespace, tag, permissions).await }
    };
    assert!(add("a1", &["admin"], None, None, None).await.is_ok());
    for (id, permissions, namespace, tag) in [
        ("b2", &["admin", "read"][..], None, None),
        ("c3", &["admin"], Some(ns.as_str()), None),
        ("d4", &["read", "submit"], None, None),
        ("e5", &["root"], Some(ns.as_str()), None),
        ("f6", &["read", "manage"], Some(ns.as_str()), None),
    ] {
        assert!(add(id, permissions, namespace, tag, Some(vec!["proof".into()])).await.is_err(), "{id}");
    }
    let reader = add("07", &["submit", "read", "read"], Some(&ns), Some("scope=default"), Some(vec!["proof".into()])).await.unwrap();
    assert_eq!(reader.permissions, vec!["read", "submit"], "canonical order, once each");
    assert!(add("08", &["read"], Some(&ns), None, Some(vec!["proof/aaaa".into()])).await.is_err(), "a grant names an app");
    assert!(add("09", &["read"], Some(&ns), None, None).await.is_err(), "without manage a token needs grants");
}

#[tokio::test]
async fn a_token_issued_or_revoked_outside_the_scheduler_takes_effect_on_reload() {
    let (_dir, c, _) = setup().await;
    let (token, secret) = crate::token::new_admin_token("from the host");
    c.fleet.store.add_token(&token).await.unwrap();
    let bearer = format!("Bearer {secret}");
    assert!(c.fleet.token_for_token(Some(&bearer)).is_none(), "not until the store is read again");
    c.fleet.reload_tokens().await;
    assert_eq!(c.fleet.token_for_token(Some(&bearer)).unwrap().permissions, vec!["admin"]);
    c.fleet.store.revoke_token(&token.id, 1).await.unwrap();
    c.fleet.reload_tokens().await;
    assert!(c.fleet.token_for_token(Some(&bearer)).is_none());
}

#[tokio::test]
async fn a_deployment_record_follows_the_app_its_node_configures() {
    let (_dir, c, ns) = setup().await;
    let mut report = scale_inventory(&ns, true, false);
    report.apps[0]["app"] = json!(format!("eth/{}", proof_elf()));
    report.apps[0].as_object_mut().unwrap().remove("elf_id");
    c.report("gpu", "session", report).await.unwrap();
    c.report("gpu", "session", scale_inventory(&ns, true, false)).await.unwrap();
    let entry = c.inner.lock().deployments["gpu-model"].spec.clone();
    assert_eq!((entry["app"].clone(), entry["elf_id"].clone()), (json!(PROOF), json!(proof_elf())));
    let saved: AppDeployment = serde_json::from_slice(&std::fs::read(c.dir.join("deployments/gpu-model.json")).unwrap()).unwrap();
    assert_eq!(saved.spec["app"], PROOF, "and so does its saved record");
}

#[tokio::test]
async fn an_app_named_like_a_batch_does_not_attach_to_it() {
    let (_dir, c, ns) = setup().await;
    let mut report = inventory(&ns);
    report.apps = vec![json!({"name":"training","kind":"app","namespace":ns,"app":"training","gpus":1,"cpus":1,
        "replicas":1,"disabled":true,"ready":false,"cleaned":true,"image":"runner:1"}).into()];
    c.report("gpu", "session", report).await.unwrap();
    assert!(!c.inner.lock().apps["training"].persistent, "the batch keeps its record");
    assert!(!c.inner.lock().deployments.contains_key("gpu-training"), "and the app is not attached to it");
}

/// Places `name` as enabling it does (`deploy`), the chosen node writing the entry it is sent
/// and reporting it: that node and the entry.
async fn deployed(c: &Arc<WorkloadController>, ns: &str, name: &str) -> anyhow::Result<(String, Value)> {
    let task = tokio::spawn({
        let (c, ns, name) = (c.clone(), ns.to_owned(), name.to_owned());
        async move { c.deploy(&ns, &name).await }
    });
    let create = loop {
        let sent = c.inner.lock().commands.values()
            .find(|cmd| !cmd.done && cmd.parameters["create"]["name"] == name).cloned();
        if let Some(sent) = sent {
            break sent;
        }
        if task.is_finished() {
            return task.await.unwrap().map(|_| unreachable!("placed without a create"));
        }
        tokio::task::yield_now().await;
    };
    let entry = create.parameters["create"].clone();
    let (session, mut report) = {
        let i = c.inner.lock();
        let w = &i.workers[&create.worker];
        (w.session.clone(), w.report.clone())
    };
    let mut observed = entry.clone();
    for (key, value) in [("app", json!(name)), ("ready", json!(false)), ("cleaned", json!(true))] {
        observed[key] = value;
    }
    report.apps.push(observed.into());
    report.results = vec![CommandResult { id: create.id.clone(), result: json!({"created":[name]}).into(), error: None }];
    c.report(&create.worker, &session, report).await.unwrap();
    task.await.unwrap()?;
    Ok((create.worker, entry))
}

#[tokio::test]
async fn an_app_is_created_on_no_node_and_placed_when_enabled() {
    let (_dir, c, ns) = setup().await;
    c.report("gpu", "session", inventory(&ns)).await.unwrap();
    let body = json!({"name":"model","namespace":ns,"spec":{"image":"serve:1","gpus":1,"cpus":2,
        "port_base":50100,"health_path":"/health","command":["serve","--port","{port}"],"env":{"HF_HUB_OFFLINE":"1"}}});
    let response = scale_call(c.clone(), "admin", "POST", "/apps", body.clone()).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let created: Value = serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!((created["state"].as_str(), created["spec"]["workers"].clone()), (Some("disabled"), json!([])));
    assert!(remove_commands(&c, NodeOperation::App).is_empty(), "no node is told before the app is enabled");
    assert_eq!(scale_call(c.clone(), "admin", "POST", "/apps", body).await.status(), StatusCode::CONFLICT);
    assert_eq!(scale_call(c.clone(), "admin", "POST", "/apps", json!({"name":"other","namespace":ns,"spec":{"port_base":1}})).await.status(),
        StatusCode::BAD_REQUEST);
    assert_eq!(scale_call(c.clone(), "admin", "POST", "/apps", json!({"name":"training","namespace":ns,"spec":{"image":"x","port_base":1}})).await.status(),
        StatusCode::CONFLICT, "a batch's name is taken");
    assert_eq!(scale_call(c.clone(), "admin", "POST", "/apps", json!({"name":"pinned","namespace":ns,"node":"gpu","spec":{"image":"x","port_base":1}})).await.status(),
        StatusCode::BAD_REQUEST, "the scheduler picks the node");
    let (node, entry) = deployed(&c, &ns, "model").await.unwrap();
    assert_eq!((node.as_str(), entry), ("gpu", json!({"name":"model","kind":"app","namespace":ns,"disabled":true,"priority":0,
        "replicas":1,"gpus":1,"cpus":2,"image":"serve:1","port_base":50100,"health_path":"/health",
        "command":["serve","--port","{port}"],"env":{"HF_HUB_OFFLINE":"1"}})));
    assert_eq!(c.inner.lock().apps["model"].workers, ["gpu"]);
    assert!(c.inner.lock().deployments.contains_key("gpu-model"), "enabling it starts that deployment");
    c.deploy(&ns, "model").await.unwrap();
    assert!(remove_commands(&c, NodeOperation::App).iter().all(|(_, m)| m["create"].is_null()), "a placed app is not placed again");
    let huge = json!({"name":"huge","namespace":ns,"spec":{"image":"x","port_base":1,"gpus":2,"replicas":2}});
    assert_eq!(scale_call(c.clone(), "admin", "POST", "/apps", huge).await.status(), StatusCode::CREATED);
    let refused = deployed(&c, &ns, "huge").await.unwrap_err();
    assert!(refused.to_string().contains("no ready node has 4 GPUs"), "{refused}");
}

#[tokio::test]
async fn a_proof_app_is_an_app_whose_container_proves_its_program() {
    let (_dir, c, ns) = setup().await;
    c.report("gpu", "session", inventory(&ns)).await.unwrap();
    let body = json!({"name":"eth","namespace":ns,"priority":5,"spec":{"image":"ziren-gpu:1","gpus":1,"proof_storage_root":"/proofs"},
        "proof":{"elf_id":"AB".repeat(32),"class":"latency"}});
    assert_eq!(scale_call(c.clone(), "admin", "POST", "/apps", body).await.status(), StatusCode::CREATED);
    let proof = c.fleet.app(&ns, "eth").unwrap();
    assert!(!proof.disabled, "it takes jobs, which wait for its prover");
    assert_eq!((proof.elf_id.as_str(), proof.class.as_str(), proof.priority), ("ab".repeat(32).as_str(), "latency", 5));
    let listed: Value = serde_json::from_slice(&to_bytes(
        scale_call(c.clone(), "admin", "GET", "/apps", Value::Null).await.into_body(), 1 << 20).await.unwrap()).unwrap();
    let rows: Vec<&Value> = listed.as_array().unwrap().iter().filter(|a| a["name"] == "eth").collect();
    assert_eq!(rows.len(), 1, "listed once: {listed}");
    assert_eq!((rows[0]["proof"]["class"].as_str(), rows[0]["spec"]["image"].as_str()), (Some("latency"), Some("ziren-gpu:1")));
    let (node, entry) = deployed(&c, &ns, "eth").await.unwrap();
    assert_eq!((node.as_str(), entry["kind"].as_str(), entry["elf_id"].as_str(), entry["proof_storage_root"].as_str()),
        ("gpu", Some("ziren-gpu"), Some("ab".repeat(32).as_str()), Some("/proofs")));
    assert_eq!(scale_call(c.clone(), "admin", "DELETE", "/apps/eth", Value::Null).await.status(), StatusCode::OK);
    assert!(!c.fleet.app_known(&ns, "eth") && c.inner.lock().apps["eth"].removed, "deleting it removes both records");
}

/// Runs the commands sent to `nodes`, one at a time, until none is left: each node writes,
/// starts and stops app entries in its report as its command asks. A `create` sent to one of
/// `failing_creates` fails.
async fn nodes_run_commands(c: &WorkloadController, nodes: &mut BTreeMap<String, NodeReport>, failing_creates: &[&str]) {
    for _ in 0..100 {
        c.dispatch().await.unwrap();
        let Some(command) = c.inner.lock().commands.values().find(|v| !v.done && v.admitted).cloned() else {
            return;
        };
        let report = nodes.get_mut(&command.worker).unwrap();
        let p = &command.parameters;
        let mut error = None;
        let result = if command.operation == NodeOperation::SetApp as i32 {
            let disabled = p["disabled"].as_bool().unwrap();
            let mut configured = false;
            for a in report.apps.iter_mut().filter(|a| a["name"] == p["app"]["name"]) {
                (a["ready"], a["cleaned"], configured) = (json!(!disabled), json!(disabled), true);
            }
            if !configured {
                error = Some(format!("{} is not configured on this node", text(&p["app"], "name")));
                Value::Null
            } else if disabled { json!({"cleaned":true}) } else { json!({"ready":true}) }
        } else if !p["create"].is_null() {
            if failing_creates.contains(&command.worker.as_str()) {
                error = Some("apps.yaml is not writable".to_string());
                Value::Null
            } else {
                let mut entry = p["create"].clone();
                (entry["ready"], entry["cleaned"]) = (json!(false), json!(true));
                report.apps.push(entry.into());
                json!({"created":[p["create"]["name"]]})
            }
        } else if p["remove"] == json!(true) {
            // Like the bridge, it names the entries it removed.
            let removed: Vec<Value> = report.apps.iter().filter(|a| a["app"] == p["app"]["name"]).map(|a| a["name"].clone()).collect();
            report.apps.retain(|a| a["app"] != p["app"]["name"]);
            json!({"removed":removed})
        } else {
            for a in report.apps.iter_mut().filter(|a| a["name"] == p["entry"]["name"]) {
                for (k, v) in p["set"].as_object().unwrap() {
                    a[k.as_str()] = v.clone();
                }
            }
            json!({"updated":[p["entry"]["name"]]})
        };
        let mut sent = report.clone();
        sent.results = vec![CommandResult { id: command.id.clone(), result: result.into(), error }];
        c.report(&command.worker, "session", sent).await.unwrap();
    }
    panic!("the nodes never finished their commands");
}

#[tokio::test]
async fn an_app_start_is_placed_on_a_node_with_room() {
    let (_dir, c, ns) = setup().await;
    c.report("gpu", "session", inventory(&ns)).await.unwrap();
    let body = json!({"name":"model","namespace":ns,"spec":{"image":"serve:1","gpus":1,"cpus":2,"port_base":50100}});
    assert_eq!(scale_call(c.clone(), "admin", "POST", "/apps", body).await.status(), StatusCode::CREATED);
    assert_eq!(deployed(&c, &ns, "model").await.unwrap().0, "gpu");
    let report_of = |machine: &str, free_gpus: f64| {
        let mut report = inventory(&ns);
        (report.machine, report.free_gpus) = (machine.into(), free_gpus);
        report
    };
    // Its node's cards are taken by then; a node that cannot write its entry has room.
    let mut nodes = BTreeMap::from([("gpu".to_string(), c.inner.lock().workers["gpu"].report.clone()), ("bare".into(), report_of("bare", 2.0))]);
    nodes.get_mut("gpu").unwrap().free_gpus = 0.0;
    for (id, report) in &nodes {
        c.report(id, "session", report.clone()).await.unwrap();
    }
    c.begin_toggle(&["gpu-model".into()], false).unwrap();
    nodes_run_commands(&c, &mut nodes, &["bare"]).await;
    let waiting_starts = |c: &WorkloadController| c.inner.lock().commands.values()
        .filter(|v| !v.done && v.operation == NodeOperation::SetApp as i32).map(|v| v.worker.clone()).collect::<Vec<_>>();
    assert_eq!(waiting_starts(&c), ["gpu"], "a node that failed to create the app leaves it waiting on its own");
    c.dispatch().await.unwrap();
    assert!(remove_commands(&c, NodeOperation::App).is_empty(), "and is not picked again");

    // A start that preempted on its own node keeps the room that frees, though another node
    // has room by then.
    let mut preempted = submit(&c).await;
    (preempted.worker, preempted.stop) = (Some("gpu".into()), Some("preempt".into()));
    preempted.status["state"] = json!("running");
    c.inner.lock().jobs.insert(preempted.id.clone(), preempted.clone());
    nodes.insert("big".into(), report_of("big", 2.0));
    c.report("big", "session", nodes["big"].clone()).await.unwrap();
    c.dispatch().await.unwrap();
    assert!(remove_commands(&c, NodeOperation::App).is_empty(), "placed on its own node while it preempts there");
    preempted.status["state"] = json!("cancelled");
    c.inner.lock().jobs.insert(preempted.id.clone(), preempted);
    c.inner.lock().commands.update_where(|v| v.operation == NodeOperation::PreemptJob as i32, |command| command.done = true);

    // Otherwise it takes the room another node has.
    c.dispatch().await.unwrap();
    let created = remove_commands(&c, NodeOperation::App);
    assert_eq!(created.len(), 1);
    let (to, entry) = (&created[0].0, &created[0].1["create"]);
    assert_eq!((to.as_str(), entry["disabled"].clone(), entry["image"].as_str()), ("big", json!(true), Some("serve:1")));
    assert!(["ready", "cleaned", "logs", "namespace_ref"].iter().all(|k| entry.get(*k).is_none()), "{entry}");
    nodes_run_commands(&c, &mut nodes, &["bare"]).await;
    assert!(waiting_starts(&c).is_empty());
    assert!(nodes["gpu"].apps.is_empty(), "its own node removed the entry");
    let ready = nodes["big"].apps.iter().any(|a| a["name"] == "model" && a["ready"] == json!(true) && a["disabled"] == json!(false));
    assert!(ready, "it runs on the node that had room: {:?}", nodes["big"].apps);
    let i = c.inner.lock();
    assert_eq!(i.apps["model"].workers, ["big"]);
    assert!(!i.deployments.contains_key("gpu-model") && !i.deployments["big-model"].disabled);
}

/// App `model` runs on node `gpu`, its entry anchored to `machine` when given; a job then has
/// the node's other card, and node `big` has room.
async fn running_on_a_full_node(machine: Option<&str>) -> (tempfile::TempDir, Arc<WorkloadController>, BTreeMap<String, NodeReport>) {
    let (dir, c, ns) = setup().await;
    c.report("gpu", "session", inventory(&ns)).await.unwrap();
    let body = json!({"name":"model","namespace":ns,"spec":{"image":"serve:1","gpus":1,"cpus":2,"port_base":50100}});
    assert_eq!(scale_call(c.clone(), "admin", "POST", "/apps", body).await.status(), StatusCode::CREATED);
    assert_eq!(deployed(&c, &ns, "model").await.unwrap().0, "gpu");
    let mut nodes = BTreeMap::from([("gpu".to_string(), c.inner.lock().workers["gpu"].report.clone())]);
    if let Some(machine) = machine {
        nodes.get_mut("gpu").unwrap().apps[0]["machine"] = json!(machine);
    }
    c.report("gpu", "session", nodes["gpu"].clone()).await.unwrap();
    c.begin_toggle(&["gpu-model".into()], false).unwrap();
    nodes_run_commands(&c, &mut nodes, &[]).await;
    assert_eq!(nodes["gpu"].apps[0]["ready"], json!(true), "{:?}", nodes["gpu"].apps);
    nodes.get_mut("gpu").unwrap().free_gpus = 0.0;
    let mut big = inventory(&ns);
    big.machine = "big".into();
    nodes.insert("big".into(), big);
    (dir, c, nodes)
}

/// The nodes ever sent a `create` of an app.
fn created_on(c: &WorkloadController) -> Vec<String> {
    c.inner.lock().commands.values().filter(|v| !v.parameters["create"].is_null()).map(|v| v.worker.clone()).collect()
}

/// An app's replica stops and is cleaned up while a job fills its node, so its start moves to a
/// node with room. The node it leaves frees its room once it removed the app, and the start
/// runs where it was placed: it is not sent back to the node it left, where it no longer is.
#[tokio::test]
async fn an_app_moved_to_a_node_with_room_frees_the_old_one_and_runs_there() {
    let (_dir, c, mut nodes) = running_on_a_full_node(None).await;
    let ns = c.inner.lock().apps["model"].namespace.clone();
    let held = |c: &WorkloadController| {
        let mut ids: Vec<String> = c.fleet.allocations(&ns).iter().map(|a| a.id.clone()).collect();
        ids.sort();
        ids
    };
    assert_eq!(held(&c), ["deployment:gpu-model"]);
    let gpu = nodes.get_mut("gpu").unwrap();
    (gpu.apps[0]["ready"], gpu.apps[0]["cleaned"]) = (json!(false), json!(true));
    for (id, report) in &nodes {
        c.report(id, "session", report.clone()).await.unwrap();
    }
    nodes_run_commands(&c, &mut nodes, &[]).await;
    assert!(nodes["gpu"].apps.is_empty(), "the node it left removed it");
    let running = nodes["big"].apps.iter().any(|a| a["name"] == "model" && a["ready"] == json!(true) && a["disabled"] == json!(false));
    assert!(running, "it runs on the node that had room: {:?}", nodes["big"].apps);
    assert_eq!(c.inner.lock().apps["model"].workers, ["big"]);
    assert_eq!(held(&c), ["deployment:big-model"], "the node it left freed its room");
    // That node's record goes with its next report.
    c.report("gpu", "session", nodes["gpu"].clone()).await.unwrap();
    assert!(!c.inner.lock().deployments.contains_key("gpu-model"));
}

/// An entry its node anchors to the machine (`machine`) is never moved. Once its replica is
/// gone and its node has no room, its start waits there, and runs once room frees.
#[tokio::test]
async fn an_app_anchored_to_its_machine_restarts_only_there() {
    let (_dir, c, mut nodes) = running_on_a_full_node(Some("machine")).await;
    let gpu = nodes.get_mut("gpu").unwrap();
    (gpu.apps[0]["ready"], gpu.apps[0]["cleaned"]) = (json!(false), json!(true));
    for (id, report) in &nodes {
        c.report(id, "session", report.clone()).await.unwrap();
    }
    nodes_run_commands(&c, &mut nodes, &[]).await;
    assert_eq!(created_on(&c), ["gpu"], "no other node is sent it");
    let starts: Vec<(String, bool)> = c.inner.lock().commands.values()
        .filter(|v| !v.done && v.operation == NodeOperation::SetApp as i32).map(|v| (v.worker.clone(), v.admitted)).collect();
    assert_eq!(starts, [("gpu".to_string(), false)], "its start waits for room on its node");

    nodes.get_mut("gpu").unwrap().free_gpus = 1.0;
    c.report("gpu", "session", nodes["gpu"].clone()).await.unwrap();
    nodes_run_commands(&c, &mut nodes, &[]).await;
    assert_eq!(nodes["gpu"].apps[0]["ready"], json!(true), "{:?}", nodes["gpu"].apps);
    assert_eq!(nodes["gpu"].apps[0]["machine"], json!("machine"));
    assert!(nodes["big"].apps.is_empty());
    assert_eq!(c.inner.lock().apps["model"].workers, ["gpu"]);
}

/// App `model` runs one replica on node `gpu`, anchored to `machine` when given, and one on node
/// `big`, which also configures it, as a node that took it over while `gpu` was away would.
async fn running_on_two_nodes(machine: Option<&str>) -> (tempfile::TempDir, Arc<WorkloadController>, BTreeMap<String, NodeReport>, String) {
    let (dir, c, mut nodes) = running_on_a_full_node(machine).await;
    let ns = c.inner.lock().apps["model"].namespace.clone();
    let mut copy = nodes["gpu"].apps[0].clone();
    copy.as_object_mut().unwrap().remove("machine");
    let big = nodes.get_mut("big").unwrap();
    (big.free_gpus, big.free_cpus) = (1.0, 6.0);
    big.apps.push(copy);
    for (id, report) in &nodes {
        c.report(id, "session", report.clone()).await.unwrap();
    }
    assert!(!c.inner.lock().deployments["big-model"].disabled);
    (dir, c, nodes, ns)
}

async fn json_of(response: Response) -> Value {
    serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap()
}

/// An app's replicas are the app's, wherever they run: its scale counts them on all its nodes
/// and takes no node, and scaling it down stops the extra ones. A node left with none removes
/// the app, frees its room and drops out of the app; the app's data stays there.
#[tokio::test]
async fn an_apps_replicas_are_its_own_and_a_node_its_scale_leaves_none_removes_it() {
    let (_dir, c, mut nodes, ns) = running_on_two_nodes(None).await;
    let held = |c: &WorkloadController| {
        let mut ids: Vec<String> = c.fleet.allocations(&ns).iter().map(|a| a.id.clone()).collect();
        ids.sort();
        ids
    };
    assert_eq!(held(&c), ["deployment:big-model", "deployment:gpu-model"]);
    let path = format!("/apps/model/scale?namespace={ns}");
    let view = json_of(scale_call(c.clone(), "admin", "GET", &path, Value::Null).await).await;
    assert_eq!((view["replicas"].clone(), view["nodes"].clone()), (json!(2), json!(["big", "gpu"])), "{view}");
    let part = json_of(scale_call(c.clone(), "admin", "GET", &format!("{path}&node=gpu"), Value::Null).await).await;
    assert_eq!((part["replicas"].clone(), part["node"].clone()), (json!(1), json!("gpu")), "{part}");
    let refused = scale_call(c.clone(), "admin", "PUT", &path, json!({"replicas":1,"node":"gpu"})).await;
    assert_eq!(refused.status(), StatusCode::BAD_REQUEST, "a scale names no node");
    for replicas in [0, 65] {
        let refused = scale_call(c.clone(), "admin", "PUT", &path, json!({"replicas":replicas})).await;
        assert_eq!(refused.status(), StatusCode::BAD_REQUEST, "{replicas} replicas");
    }

    let scaled = scale_call(c.clone(), "admin", "PUT", &path, json!({"replicas":1})).await;
    assert_eq!(scaled.status(), StatusCode::ACCEPTED);
    let scaled = json_of(scaled).await;
    assert_eq!((scaled["replicas"].clone(), scaled["nodes"].clone()), (json!(1), json!(["big"])), "{scaled}");
    assert_eq!(c.inner.lock().apps["model"].entry().replicas(), 1, "the app's count");
    assert_eq!(c.app_resources(&ns, "model", None)["resources"]["replicas"], json!(1));
    assert!(c.inner.lock().deployments["big-model"].scale.is_none(), "the node that keeps its replica is not changed");
    nodes_run_commands(&c, &mut nodes, &[]).await;
    assert!(nodes["gpu"].apps.is_empty(), "the node left with none removed the app");
    let running = nodes["big"].apps.iter().any(|a| a["name"] == "model" && a["ready"] == json!(true) && a["disabled"] == json!(false));
    assert!(running, "the other keeps running it: {:?}", nodes["big"].apps);
    assert_eq!(held(&c), ["deployment:big-model"], "its room is free");
    assert_eq!(c.app_resources(&ns, "model", None)["resources"]["replicas"], json!(1));
    c.report("gpu", "session", nodes["gpu"].clone()).await.unwrap();
    assert!(!c.inner.lock().deployments.contains_key("gpu-model"));
    assert_eq!(c.inner.lock().apps["model"].workers, ["big"]);

    // More replicas go to the node that runs it.
    let scaled = scale_call(c.clone(), "admin", "PUT", &path, json!({"replicas":2})).await;
    assert_eq!(scaled.status(), StatusCode::ACCEPTED);
    let target = c.inner.lock().deployments["big-model"].scale.as_ref().map(|s| s.target_entry().replicas());
    assert_eq!(target, Some(2));
    assert_eq!(c.inner.lock().apps["model"].entry().replicas(), 2);
}

/// A node back with a copy of an app that runs all its replicas elsewhere since does not start
/// it again: it removes it. An entry its node anchors to the machine is the node's own and
/// starts there anyway.
#[tokio::test]
async fn a_node_back_with_a_copy_of_an_app_that_runs_its_replicas_elsewhere_removes_it() {
    for machine in [None, Some("machine")] {
        let (_dir, c, mut nodes, _ns) = running_on_two_nodes(machine).await;
        // Its replica went with the node, which then reports its entry stopped.
        let gpu = nodes.get_mut("gpu").unwrap();
        (gpu.apps[0]["ready"], gpu.apps[0]["cleaned"]) = (json!(false), json!(true));
        c.report("gpu", "session", nodes["gpu"].clone()).await.unwrap();
        nodes_run_commands(&c, &mut nodes, &[]).await;
        let running = nodes["big"].apps.iter().any(|a| a["name"] == "model" && a["ready"] == json!(true));
        assert!(running, "{machine:?}: {:?}", nodes["big"].apps);
        let starts_sent = c.inner.lock().commands.values()
            .filter(|v| v.worker == "gpu" && v.admitted && v.operation == NodeOperation::SetApp as i32 && v.starts())
            .count();
        if machine.is_none() {
            assert!(nodes["gpu"].apps.is_empty(), "its node removed it");
            assert_eq!(starts_sent, 1, "only the first start, before the node went away");
            c.report("gpu", "session", nodes["gpu"].clone()).await.unwrap();
            assert!(!c.inner.lock().deployments.contains_key("gpu-model"));
            assert_eq!(c.inner.lock().apps["model"].workers, ["big"]);
        } else {
            assert_eq!(nodes["gpu"].apps.len(), 1, "an anchored entry stays");
            let waiting = c.inner.lock().commands.values()
                .any(|v| !v.done && v.worker == "gpu" && v.operation == NodeOperation::SetApp as i32 && v.starts());
            assert!(waiting, "and its start waits for room there");
        }
    }
}

#[tokio::test]
async fn an_app_a_node_refuses_to_create_is_placed_again_on_the_next_enable() {
    let (_dir, c, ns) = setup().await;
    c.report("gpu", "session", inventory(&ns)).await.unwrap();
    let body = json!({"name":"model","namespace":ns,"spec":{"image":"serve:1","gpus":1,"port_base":50100}});
    assert_eq!(scale_call(c.clone(), "admin", "POST", "/apps", body).await.status(), StatusCode::CREATED);
    let task = tokio::spawn({
        let (c, ns) = (c.clone(), ns.clone());
        async move { c.deploy(&ns, "model").await }
    });
    let create = loop {
        let sent = c.inner.lock().commands.values().find(|v| !v.done && !v.parameters["create"].is_null()).cloned();
        if let Some(sent) = sent {
            break sent;
        }
        tokio::task::yield_now().await;
    };
    let mut report = inventory(&ns);
    report.results = vec![CommandResult { id: create.id, result: Value::Null.into(), error: Some("apps.yaml is not writable".into()) }];
    c.report("gpu", "session", report).await.unwrap();
    let refused = tokio::time::timeout(std::time::Duration::from_secs(5), task).await.expect("enabling answers").unwrap().unwrap_err();
    assert!(refused.to_string().contains("could not create the app: apps.yaml is not writable"), "{refused}");
    assert!(c.inner.lock().apps["model"].workers.is_empty());
}

#[tokio::test]
async fn an_app_goes_to_the_node_its_replicas_fit_most_tightly() {
    let (_dir, c, ns) = setup().await;
    let node = |total: u32, free: u32| -> NodeReport {
        let mut report = inventory(&ns);
        (report.total_gpus, report.free_gpus) = (total as f64, free as f64);
        report
    };
    c.report("big", "session", node(8, 8)).await.unwrap();
    c.report("busy", "session", node(8, 0)).await.unwrap();
    c.report("small", "session", node(2, 2)).await.unwrap();
    let mut placed = Vec::new();
    for (name, gpus) in [("one", 1), ("four", 4), ("eight", 8)] {
        let body = json!({"name":name,"namespace":ns,"spec":{"image":"serve:1","port_base":50100,"gpus":gpus}});
        assert_eq!(scale_call(c.clone(), "admin", "POST", "/apps", body).await.status(), StatusCode::CREATED, "{name}");
        placed.push(deployed(&c, &ns, name).await.unwrap().0);
    }
    // One card fits the small node's free cards most tightly; four and eight fit only the big
    // node's.
    assert_eq!(placed, ["small", "big", "big"]);
    // With no eight cards free anywhere, the app waits on a node that has them, the one with
    // the most free cards, the first by name on a tie.
    c.report("big", "session", node(8, 0)).await.unwrap();
    let body = json!({"name":"later","namespace":ns,"spec":{"image":"serve:1","port_base":50100,"gpus":8}});
    assert_eq!(scale_call(c.clone(), "admin", "POST", "/apps", body).await.status(), StatusCode::CREATED);
    assert_eq!(deployed(&c, &ns, "later").await.unwrap().0, "big");
}

#[tokio::test]
async fn an_app_with_a_mistyped_setting_is_refused_before_it_is_recorded() {
    let (_dir, c, ns) = setup().await;
    c.report("gpu", "session", inventory(&ns)).await.unwrap();
    for (key, value) in [("gpus", json!("2")), ("replicas", json!("2")), ("port_base", json!(-1)), ("port_base", Value::Null),
        ("workdir", json!(1)), ("kind", json!("vm")), ("backend", json!("cpu"))] {
        let mut body = json!({"name":"model","namespace":ns,"spec":{"image":"serve:1","port_base":50100}});
        body["spec"][key] = value;
        assert_eq!(scale_call(c.clone(), "admin", "POST", "/apps", body).await.status(), StatusCode::BAD_REQUEST, "{key}");
    }
    let flat = json!({"name":"model","namespace":ns,"image":"serve:1","port_base":50100});
    assert_eq!(scale_call(c.clone(), "admin", "POST", "/apps", flat).await.status(), StatusCode::BAD_REQUEST, "settings belong under spec");
    assert!(!c.inner.lock().apps.contains_key("model"));
}

#[tokio::test]
async fn a_namespace_owner_creates_its_app_but_not_host_mounts_or_users() {
    let (_dir, c, ns) = setup().await;
    c.report("gpu", "session", inventory(&ns)).await.unwrap();
    let owner = user_token(&c, &ns, 7, "scope=default", None, true).await;
    let reader = user_token(&c, &ns, 8, "scope=default", Some(vec!["model".into()]), false).await;
    let body = json!({"name":"model","spec":{"image":"serve:1","port_base":50100,"command":["serve","{port}"],
        "env":{"A":"1"},"workdir":"/srv"}});
    for (key, value) in [("volumes", json!(["/models:ro"])), ("user", json!("0:0"))] {
        let mut admin_only = body.clone();
        admin_only["spec"][key] = value;
        assert_eq!(scale_call(c.clone(), &owner, "POST", "/apps", admin_only).await.status(), StatusCode::FORBIDDEN, "{key}");
    }
    let prover = json!({"name":"eth","spec":{"image":"ziren-gpu:1","proof_storage_root":"/proofs"},"proof":{"elf_id":"ab".repeat(32)}});
    assert_eq!(scale_call(c.clone(), &owner, "POST", "/apps", prover).await.status(), StatusCode::FORBIDDEN, "proof_storage_root");
    assert_eq!(scale_call(c.clone(), &reader, "POST", "/apps", body.clone()).await.status(), StatusCode::FORBIDDEN, "creation needs manage");
    assert_eq!(scale_call(c.clone(), &owner, "POST", "/apps", body).await.status(), StatusCode::CREATED);
    let app = c.inner.lock().apps["model"].clone();
    assert_eq!((app.namespace.as_str(), app.tag.as_deref()), (ns.as_str(), Some("scope=default")), "in the token's namespace and tag");
    assert_eq!(deployed(&c, &ns, "model").await.unwrap().1["tag"], "scope=default");
}

#[test]
fn a_failed_record_write_leaves_no_temporary_file() {
    let dir = tempfile::tempdir().unwrap();
    // A directory in the record's place makes the final rename fail after the write.
    let path = dir.path().join("app.json");
    std::fs::create_dir(&path).unwrap();
    std::fs::write(path.join("keep"), b"").unwrap();
    assert!(atomic(&path, b"{}").is_err());
    let left: Vec<_> = std::fs::read_dir(dir.path()).unwrap().map(|e| e.unwrap().file_name()).collect();
    assert_eq!(left, vec![std::ffi::OsString::from("app.json")], "only the record's own path remains");
    atomic(&dir.path().join("other.json"), b"{}").unwrap();
    assert_eq!(std::fs::read(dir.path().join("other.json")).unwrap(), b"{}");
}
