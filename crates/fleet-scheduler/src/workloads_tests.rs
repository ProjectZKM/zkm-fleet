use super::*;
use crate::{blobs::BlobStore, config::SchedulerConfig, quota::Quota, store::MemoryStore};
use axum::body::Body;
use fleet_proto::model::v1::{JobReport, CommandResult, ScaleRequest};
use sha2::Digest;

async fn setup() -> (tempfile::TempDir, Arc<WorkloadController>, String) {
    let dir = tempfile::tempdir().unwrap();
    let cfg: SchedulerConfig = serde_yaml::from_str(&format!("public_url: http://localhost\ndata_dir: {}\ncluster_token_file: cluster\nadmin_token_file: admin\ntenants: [{{name: eth}}, {{name: goat}}]\n",dir.path().display())).unwrap();
    let fleet = Scheduler::new(
        cfg,
        Arc::new(MemoryStore::default()),
        BlobStore::new(dir.path()).unwrap(),
        "cluster".into(),
    );
    fleet.load_tokens().await.unwrap();
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
    let control = Arc::new(WorkloadController::new(fleet, "admin".into(), "cluster".into()).unwrap());
    (dir, control, namespace)
}
fn inventory(namespace: &str) -> NodeReport {
    serde_json::from_value(json!({"version":2,"machine":"machine","ready":true,"total_gpus":2,"total_cpus":8,"free_gpus":2,"free_cpus":8,"profiles":{"training":{"image":"test","namespace":namespace,"priority":20,"max_gpus":2,"min_runtime_s":0}}})).unwrap()
}
fn incoming(job: &Job, state: &str, cleaned: bool) -> JobReport {
    JobReport {
        id: job.id.clone(),
        lease: job.lease.clone(),
        status: json!({"state":state,"submitted_at":1,"started_at":1}).into(),
        spec: json!({"profile_name":job.profile}).into(),
        cleaned,
        logs_ready: cleaned,
    }
}

#[tokio::test]
async fn apps_and_batches_have_separate_lifecycles_after_restart() {
    let (_dir, c, ns) = setup().await;
    let mut report = scale_inventory(&ns, false, true);
    report.profiles.get_mut("training").unwrap()["type"] = json!("job");
    c.report("gpu", "session", report).await.unwrap();
    let mut job = submit(&c).await;
    job.status["type"] = json!("job");
    c.save("jobs", &job.id, &job).unwrap();
    let restarted = Arc::new(
        WorkloadController::new(c.fleet.clone(), "admin".into(), "cluster".into()).unwrap());
    for path in [
        "/apps".to_string(),
        "/batch-profiles".to_string(),
        "/batch-profiles/training".to_string(),
        "/apps/service".to_string(),
        "/batches".to_string(),
        format!("/batches/{}", job.id),
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
                assert!(row.get("kind").is_none(), "{path}: {row}");
                assert_eq!(row["name"], "service");
            } else {
                assert_eq!(row["type"], "batch", "{path}: {row}");
            }
            assert_eq!(row["namespace"], ns);
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
        let app = if proof {format!("eth/{}", "a".repeat(64))} else {"service".into()};
        let views = c.app_resources(&ns, &app, Some("scope=default"));
        assert_eq!(views["resources"], json!({"gpus":2.0,"cpus":4.0,"replicas":2,"nodes":["gpu"]}));
        assert_eq!(views["namespace_quota"],json!({"gpus":2.0,"cpus":8.0}));
        assert_eq!(views["tag_quota"],json!({"gpus":1.0,"cpus":null}));
        assert_eq!(views["tags"], json!(["scope=default"]));
        assert_eq!(views["state"], "disabled");
        let restarted = WorkloadController::new(c.fleet.clone(), "admin".into(), "cluster".into()).unwrap();
        let restored = restarted.app_resources(&ns, &app, Some("scope=default"));
        for field in ["resources", "namespace_quota", "tag_quota", "tags"] {
            assert_eq!(restored[field], views[field]);
        }
        assert_eq!(restored["state"], "offline");
        let mut second = scale_inventory(&ns,proof,true);
        second.machine="second-machine".into();
        c.report("gpu-2","second-session",second).await.unwrap();
        assert_eq!(c.app_resources(&ns,&app,None)["resources"],json!({"gpus":4.0,"cpus":8.0,"replicas":4,"nodes":["gpu","gpu-2"]}));
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
        let name = if proof { format!("eth/{}", "a".repeat(64)) } else { "service".into() };
        let mut report = scale_inventory(&ns, proof, false);
        report.services[0]["ready"] = json!(false);
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
        c.submit(json!({"profile":"training","command":["true"],"gpus":0,"cpus":1}), None, &q,
        ).await.unwrap();
    }
    for (path, count) in [
        (format!("/apps?namespace={ns}&tag=scope%3Ddefault"), 1),
        (format!("/apps?namespace={ns}&tag=team%3Dresearch"), 0),
        (format!("/batch-profiles?namespace={ns}&tag=team%3Dresearch"), 1,
        ),
    ] {
        let response = scale_call(c.clone(), "admin", "GET", &path, Value::Null).await;
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        let rows: Value = serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
        assert_eq!(rows.as_array().unwrap().len(), count, "{path}: {rows}");
        if path.starts_with("/batch-profiles") { assert_eq!(rows[0]["jobs"], 1); }
    }
    let secret = "ab".repeat(32);
    c.fleet.add_user_token("eth", &"a".repeat(32), &format!("0x{}", "b".repeat(40)), "reader",
        Some(vec!["training".into(), "service".into()]),
        Some(hex::encode(sha2::Sha256::digest(secret.as_bytes()))), Some(ns.clone()), None, false,
        ).await.unwrap();
    let response = scale_call(c.clone(), &secret, "GET", "/batch-profiles/training", Value::Null,
    ).await;
    assert_eq!(response.status(), StatusCode::OK);
    let profile: Value = serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(profile["jobs"], 1);
    assert_eq!(scale_call(c.clone(), &secret, "GET", "/apps/service", Value::Null).await.status(), StatusCode::NOT_FOUND);
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
    invalid.services[0]["tag"] = json!("bare");
    assert!(c.report("gpu", "session", invalid).await.is_err());
}

#[tokio::test]
async fn proof_replica_location_is_diagnostic_and_resolves_without_a_live_actor() {
    use crate::store::{ProofJob, ProofJobState};
    use fleet_proto::fleet::v1::ProofKind;
    let (_dir,c,ns)=setup().await;
    c.report("gpu", "session", scale_inventory(&ns,true,true)).await.unwrap();
    let mut job=ProofJob{inputs:None,outputs:None,id:"proof".into(),tenant:"eth".into(),owner:"b".repeat(40),namespace:ns.clone(),tag:String::new(),client_id:"proof".into(),elf_id:"a".repeat(64),kind:ProofKind::Compressed as i32,priority:0,state:ProofJobState::Running as i32,
        fail_status: None,error:String::new(),node:Some("service-0".into()),attempts:1,failed_on:vec!["service-1".into()],cycles:0,prove_ms:0,created_ms:1,started_ms:Some(1),finished_ms:None,
    };
    for control in [&*c, &WorkloadController::new(c.fleet.clone(),"admin".into(),"cluster".into()).unwrap(),
    ] {
        let view=control.proof_job_value(&job);
        assert_eq!(view["node"],"gpu");
        assert_eq!(view["failed_on"],json!(["gpu"]));
        assert_eq!(view["diagnostics"]["execution_replica"],"service-0");
    }
    let mut second = scale_inventory(&ns,true,true);
    second.machine="second-machine".into();
    c.report("gpu-2","second-session",second).await.unwrap();
    assert!(c.proof_job_value(&job)["node"].is_null(),"ambiguous replica names must not guess a machine");
    // Storage metadata resolves historical jobs even after their service is removed.
    job.outputs=Some(fleet_proto::fleet::v1::ProofOutputs{machine:"machine".into(),..Default::default()});
    c.inner.lock().services.clear();
    assert_eq!(c.proof_job_value(&job)["node"],"gpu");
    job.outputs=None;
    assert!(c.proof_job_value(&job)["node"].is_null());
    assert_eq!(job.node.as_deref(),Some("service-0"),"presentation must not change execution routing");
}

#[tokio::test]
async fn lifecycle_routes_reject_cross_type_access_and_serving_batch_submissions() {
    let (_dir, c, ns) = setup().await;
    c.report("gpu", "session", scale_inventory(&ns, false, true)).await.unwrap();
    for (method, path, body, status) in [
        ("GET", "/apps/training", Value::Null, StatusCode::NOT_FOUND),
        ("GET", "/batch-profiles/service", Value::Null, StatusCode::NOT_FOUND,
        ),
        ("PUT", "/apps/training/disabled", Value::Null, StatusCode::NOT_FOUND,
        ),
        ("PUT", "/batch-profiles/service/priority", json!({"priority":100}), StatusCode::NOT_FOUND,
        ),
        ("DELETE", "/batch-profiles/service", Value::Null, StatusCode::NOT_FOUND,
        ),
        ("PUT", "/batch-profiles/service/scale", json!({"gpus":2}), StatusCode::BAD_REQUEST,
        ),
        ("POST", "/apps", json!({"name":"finite","image":"test","namespace":ns}), StatusCode::BAD_REQUEST,
        ),
        ("POST", "/batches", json!({"profile":"service","command":["true"]}), StatusCode::CONFLICT,
        ),
    ] {
        let response = scale_call(c.clone(), "admin", method, path, body).await;
        assert_eq!(response.status(), status, "{method} {path}");
    }
    assert!(c.inner.lock().jobs.is_empty());
    assert!(c.inner.lock().commands.is_empty());
    assert_eq!(c.inner.lock().apps["service"].profile["priority"], 0);
}

#[tokio::test]
async fn batch_profile_policy_and_job_lifecycle_use_batch_routes() {
    let (_dir, c, ns) = setup().await;
    c.report("gpu", "session", inventory(&ns)).await.unwrap();
    let response = scale_call(c.clone(), "admin", "PUT", "/batch-profiles/training/priority",
        json!({"priority":31}),
    ).await;
    assert_eq!(response.status(), StatusCode::OK);
    let response = scale_call(c.clone(), "admin", "POST", "/batches",
        json!({"profile":"training","command":["true"],"gpus":1}),
    ).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let job: Value = serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(job["type"], "batch");
    assert_eq!(job["namespace"], ns);
    assert_eq!(job["priority"], 31);
    assert_eq!(scale_call(c.clone(), "admin", "DELETE", "/batch-profiles/training", Value::Null)
        .await.status(), StatusCode::CONFLICT);
    assert_eq!(scale_call(c.clone(), "admin", "PUT", "/batch-profiles/training/disabled", Value::Null)
        .await.status(), StatusCode::OK);
    assert_eq!(scale_call(c.clone(), "admin", "POST", "/batches",
        json!({"profile":"training","command":["true"]})).await.status(), StatusCode::CONFLICT);
    assert_eq!(c.job(text(&job,"id"), None, &BTreeMap::new()).unwrap().state(), "waiting_quota");
    assert_eq!(scale_call(c.clone(), "admin", "POST", &format!("/batches/{}/cancel", text(&job,"id")), Value::Null)
        .await.status(), StatusCode::OK);
    assert_eq!(scale_call(c.clone(), "admin", "DELETE", "/batch-profiles/training", Value::Null)
        .await.status(), StatusCode::OK);
    assert_eq!(scale_call(c.clone(), "admin", "GET", &format!("/batches/{}", text(&job,"id")), Value::Null)
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
            json!({"profile":"training","command":["true"],"gpus":1,"cpus":1}),
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
    let restarted = WorkloadController::new(c.fleet.clone(), "admin".into(), "cluster".into()).unwrap();
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
        let mut request = json!({"profile":"training","command":["true"],"gpus":1,"cpus":1});
        request[field] = value;
        let err = c.submit(request, None, &BTreeMap::new()).await.unwrap_err();
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
        tenant: "eth".into(),
        label: "owner".into(),
        created_ms: 0,
        revoked_ms: None,
        apps: None,
        token_hash: None,
        namespace: Some(ns.clone()),
        tag: Some("scope=default".into()),
        manage_apps: true,
    };
    let job = c
        .submit(
            json!({"profile":"training","command":["true"],"gpus":0,"cpus":1}),
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
    c.report("gpu", "first", report).await.unwrap();
    assert!(c.report("gpu", "second", inventory(&ns)).await.is_err());
    assert_eq!(c.worker_status()[0]["backend"], "ray-local");
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
        .add_user_token(
            "eth",
            &"a".repeat(32),
            &format!("0x{}", "b".repeat(40)),
            "owner",
            None,
            Some(hex::encode(sha2::Sha256::digest(secret.as_bytes()))),
            Some(ns.clone()),
            Some("scope=default".into()),
            true,
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
        handle(
            State(c),
            builder
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
    }
    assert_eq!(
        call(c.clone(), "GET", "/batch-profiles", None, Value::Null)
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
            "/batch-profiles",
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
            "/batch-profiles",
            Some(&secret),
            json!({"name":"mounted","image":"python:3","namespace":ns,"allow":["/data/models"],"priority":17})
        )
        .await
        .status(),
        StatusCode::CREATED
    );
    assert_eq!(
        call(
            c.clone(),
            "PUT",
            "/batch-profiles/user-app/priority",
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
            "/batch-profiles?tag=scope=research",
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
        "/batches",
        Some(&secret),
        json!({"profile":"user-app","gpus":0,"cpus":1,"command":["train","--input","s3://owner-bucket/dataset"],"env":["INPUT_URI=s3://owner-bucket/dataset"]}),
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
        handle(State(c.clone()), removed_input).await.status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            c.clone(),
            "GET",
            &format!("/batches/{id}/out.tar.gz"),
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
        handle(State(c.clone()), removed_output).await.status(),
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
    let restarted = WorkloadController::new(c.fleet.clone(), "admin".into(), "cluster".into()).unwrap();
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
    report.services = vec![json!({"name":"service","kind":if proof {"ziren-gpu"}else{"app"},
        "namespace":ns,"tag":"scope=default","proof_app":if proof {json!(format!("eth/{}","a".repeat(64)))}else{Value::Null},
        "gpus":1,"cpus":2,"replicas":2,"disabled":disabled,"ready":!disabled,"cleaned":disabled}).into()];
    if !disabled { report.free_gpus=0.0;report.free_cpus=4.0; }
    report
}
async fn scale_call(c: Arc<WorkloadController>, token: &str, method: &str, path: &str, body: Value,
) -> Response {
    handle(State(c),Request::builder().method(method).uri(path)
        .header("authorization",format!("Bearer {token}"))
        .body(Body::from(serde_json::to_vec(&body).unwrap())).unwrap(),
    ).await
}
async fn user_token(c: &WorkloadController, ns: &str, seed: u8, tenant: &str, tag: &str, apps: Option<Vec<String>>, manage_apps: bool,
) -> String {
    let secret = format!("{seed:02x}").repeat(32);
    c.fleet.add_user_token(tenant, &format!("{seed:02x}").repeat(16), &format!("{seed:02x}").repeat(20), "test",
        apps, Some(hex::encode(sha2::Sha256::digest(secret.as_bytes()))), Some(ns.into()), Some(tag.into()), manage_apps,
    ).await.unwrap();
    secret
}

#[tokio::test]
async fn batch_metadata_belongs_to_scoped_owners_and_job_policy_is_inherited() {
    let (_dir, c, ns) = setup().await;
    let owner = user_token(&c, &ns, 1, "eth", "scope=default", None, true).await;
    let consumer = user_token(&c, &ns, 2, "eth", "scope=default", Some(vec!["training".into()]), false).await;
    let restricted = user_token(&c, &ns, 3, "eth", "scope=default", Some(vec!["other".into()]), true).await;
    let foreign_tag = user_token(&c, &ns, 4, "eth", "scope=research", None, true).await;
    let other_ns = c.fleet.create_namespace("other", Quota::default()).await.unwrap().id;
    let foreign_ns = user_token(&c, &other_ns, 5, "eth", "scope=default", None, true).await;
    assert_eq!(scale_call(c.clone(), &owner, "POST", "/batch-profiles",
        json!({"name":"training","image":"python:3","allow":["/data/models"],"priority":17})).await.status(), StatusCode::CREATED);
    for token in [&consumer, &restricted, &foreign_tag, &foreign_ns] {
        for (method, path, body) in [
            ("PUT", "/batch-profiles/training/priority", json!({"priority":99})),
            ("PUT", "/batch-profiles/training/disabled", Value::Null),
            ("DELETE", "/batch-profiles/training/disabled", Value::Null),
            ("DELETE", "/batch-profiles/training", Value::Null),
        ] {
            let status = scale_call(c.clone(), token, method, path, body).await.status();
            assert!(matches!(status, StatusCode::FORBIDDEN | StatusCode::NOT_FOUND), "{method} {path}: {status}");
        }
    }
    assert_eq!(scale_call(c.clone(), &consumer, "POST", "/batch-profiles",
        json!({"name":"consumer","image":"test"})).await.status(), StatusCode::FORBIDDEN);
    for body in [json!({"name":"invalid","image":"test","allow":["/"]}),
        json!({"name":"invalid","image":"test","allow":["/data/../etc"]}),
        json!({"name":"invalid","image":"test","priority":2147483648_i64}),
    ] {
        assert_eq!(scale_call(c.clone(), &owner, "POST", "/batch-profiles", body).await.status(), StatusCode::BAD_REQUEST);
    }
    assert_eq!(scale_call(c.clone(), &owner, "PUT", "/batch-profiles/training/disabled", Value::Null).await.status(), StatusCode::OK);
    assert_eq!(scale_call(c.clone(), &owner, "POST", "/batches",
        json!({"profile":"training","command":["true"]})).await.status(), StatusCode::CONFLICT);
    assert_eq!(scale_call(c.clone(), &owner, "DELETE", "/batch-profiles/training/disabled", Value::Null).await.status(), StatusCode::OK);
    let response = scale_call(c.clone(), &consumer, "POST", "/batches",
        json!({"profile":"training","gpus":0,"cpus":1,"mount":["/data/models:ro"],"command":["true"]})).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let job: Value = serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap();
    assert_eq!(job["priority"], 17);
    assert_eq!(scale_call(c.clone(), &owner, "PUT", "/batch-profiles/training/priority", json!({"priority":31})).await.status(), StatusCode::OK);
    assert_eq!(c.job(text(&job, "id"), None, &BTreeMap::new()).unwrap().policy["priority"], 17);
    assert_eq!(c.inner.lock().apps["training"].profile["allow"], json!(["/data/models"]));
    assert_eq!(scale_call(c.clone(), &consumer, "POST", "/batches",
        json!({"profile":"training","gpus":0,"cpus":1,"mount":["/etc:ro"],"command":["true"]})).await.status(), StatusCode::FORBIDDEN);
    assert_eq!(scale_call(c.clone(), &owner, "POST", &format!("/batches/{}/cancel", text(&job, "id")), Value::Null).await.status(), StatusCode::OK);
    assert_eq!(scale_call(c, &owner, "DELETE", "/batch-profiles/training", Value::Null).await.status(), StatusCode::OK);
}

#[tokio::test]
async fn app_owners_manage_services_and_scale_without_widening_grants_or_tags() {
    for proof in [false, true] {
        let (_dir, c, ns) = setup().await;
        let elf = "a".repeat(64);
        c.fleet.add_app(&ns, "eth", &elf, 0).await.unwrap();
        c.report("gpu", "session", scale_inventory(&ns, proof, false)).await.unwrap();
        let app = if proof { format!("eth/{elf}") } else { "service".into() };
        let owner = user_token(&c, &ns, 1, "goat", "scope=default", None, true).await;
        let consumer = user_token(&c, &ns, 2, "eth", "scope=default", Some(vec![app]), false).await;
        let restricted = user_token(&c, &ns, 3, "eth", "scope=default", Some(vec!["other".into()]), true).await;
        let foreign_tag = user_token(&c, &ns, 4, "eth", "scope=research", None, true).await;
        let other_signer = user_token(&c, &ns, 5, "goat", "scope=default", Some(vec!["service".into()]), false).await;
        let scale = if proof { format!("/proof-apps/scale?tenant=eth&elf_id={elf}") } else { "/apps/service/scale".into() };
        let disabled = if proof { format!("/proof-apps/disabled?tenant=eth&elf_id={elf}") } else { "/apps/service/disabled".into() };
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
        assert_eq!(scale_call(c.clone(), &owner, "PUT", &format!("{scale}{}namespace=other", if proof { "&" } else { "?" }),
            json!({"replicas":1})).await.status(), StatusCode::FORBIDDEN);
        if !proof {
            assert_eq!(scale_call(c.clone(), &owner, "PUT", "/apps/service/priority", json!({"priority":31})).await.status(), StatusCode::OK);
            assert_eq!(c.inner.lock().apps["service"].profile["priority"], 31);
        }
        for (method, disabled_state) in [("PUT", true), ("DELETE", false)] {
            let control = c.clone();
            let token = owner.clone();
            let path = disabled.clone();
            let request = tokio::spawn(async move { scale_call(control, &token, method, &path, Value::Null).await });
            let command = tokio::time::timeout(std::time::Duration::from_secs(2), async {
                loop {
                    if let Some(command) = c.inner.lock().commands.values().find(|v| !v.done).cloned() { break command; }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            }).await.unwrap();
            let mut observed = command.parameters["service"].clone();
            observed["disabled"] = json!(disabled_state);
            observed["ready"] = json!(!disabled_state);
            observed["cleaned"] = json!(disabled_state);
            finish_service_command(&c, &ns, proof, &command, disabled_state, Some(observed)).await;
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
    let owner = user_token(&c, &ns, 1, "eth", "scope=default", None, true).await;
    let consumer = user_token(&c, &ns, 2, "eth", "scope=default", Some(vec!["training".into()]), false).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let router = crate::http::router(crate::http::HttpState { fleet: c.fleet.clone(), workloads: c.clone(), admin_token: Arc::new("admin".into()) });
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap(); });
    let client = reqwest::Client::new();
    for token in [&owner, &consumer] {
        for selector in [String::new(), "?namespace=test".into(), format!("?namespace={ns}&tag=scope%3Ddefault")] {
            let response = client.get(format!("{url}/admin/namespaces{selector}")).bearer_auth(token).send().await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let rows: Value = response.json().await.unwrap();
            assert_eq!(rows.as_array().unwrap().len(), 1);
            assert_eq!(rows[0]["id"], ns);
            assert_eq!(rows[0]["quota"], json!({"gpus":2.0,"cpus":8.0}));
            assert_eq!(rows[0]["tag_quotas"], json!({"scope=default":{"gpus":0.5,"cpus":2.0}}));
        }
        for selector in ["test".to_string(), ns.clone(), "test?tag=scope%3Ddefault".into()] {
            let response = client.get(format!("{url}/admin/namespaces/{selector}")).bearer_auth(token).send().await.unwrap();
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
        for path in ["/admin/namespaces/other".into(), format!("/admin/namespaces/{other}"),
            "/admin/namespaces?namespace=other".into(), "/admin/namespaces/test?namespace=other".into(),
            "/admin/namespaces?tag=scope%3Dresearch".into(), "/admin/namespaces/test?tag=scope%3Dresearch".into(),
            "/admin/namespaces/test/allocations".into(), "/admin/namespaces/test/quota".into(), "/admin/tokens".into(),
        ] {
            assert_eq!(client.get(format!("{url}{path}")).bearer_auth(token).send().await.unwrap().status(), StatusCode::FORBIDDEN, "{path}");
        }
        for (method, path) in [
            (reqwest::Method::POST, "/admin/namespaces"),
            (reqwest::Method::PUT, "/admin/namespaces/test/quota"),
            (reqwest::Method::PUT, "/admin/namespaces/test/quota?tag=scope%3Ddefault"),
            (reqwest::Method::PUT, "/admin/namespaces/test/suspended"),
            (reqwest::Method::DELETE, "/admin/namespaces/test/suspended"),
        ] {
            assert_eq!(client.request(method, format!("{url}{path}")).bearer_auth(token).json(&json!({"gpus":0,"cpus":0})).send().await.unwrap().status(), StatusCode::FORBIDDEN);
        }
    }
    let all: Value = client.get(format!("{url}/admin/namespaces")).bearer_auth("admin").send().await.unwrap().json().await.unwrap();
    assert_eq!(all.as_array().unwrap().len(), 3);
    let full: Value = client.get(format!("{url}/admin/namespaces/test")).bearer_auth("admin").send().await.unwrap().json().await.unwrap();
    assert_eq!(full["tag_quotas"].as_object().unwrap().len(), 2);
    assert!(full["tags"].as_array().unwrap().contains(&json!("scope=research")));
    assert_eq!(client.get(format!("{url}/admin/namespaces")).send().await.unwrap().status(), StatusCode::UNAUTHORIZED);
    c.fleet.revoke_token(&"01".repeat(16)).await.unwrap();
    assert_eq!(client.get(format!("{url}/admin/namespaces")).bearer_auth(&owner).send().await.unwrap().status(), StatusCode::UNAUTHORIZED);
    c.fleet.suspend_namespace(&ns, true).await.unwrap();
    assert_eq!(client.get(format!("{url}/admin/namespaces/test")).bearer_auth(&consumer).send().await.unwrap().status(), StatusCode::UNAUTHORIZED);
    server.abort();
}

#[tokio::test]
async fn proof_app_http_metadata_is_owner_managed_and_quotas_are_admin_managed() {
    let (_dir, c, ns) = setup().await;
    let elf = "a".repeat(64);
    let other_elf = "b".repeat(64);
    c.fleet.add_app(&ns, "eth", &elf, 0).await.unwrap();
    c.fleet.add_app(&ns, "eth", &other_elf, 0).await.unwrap();
    c.fleet.add_app(&ns, "goat", &elf, 0).await.unwrap();
    let other_ns = c.fleet.create_namespace("other", Quota::default()).await.unwrap().id;
    c.fleet.add_app(&other_ns, "eth", &elf, 0).await.unwrap();
    let owner = user_token(&c, &ns, 1, "goat", "scope=default", None, true).await;
    let consumer = user_token(&c, &ns, 2, "eth", "scope=default", Some(vec![format!("eth/{elf}")]), false).await;
    let restricted = user_token(&c, &ns, 3, "eth", "scope=default", Some(vec![format!("eth/{elf}")]), true).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let router = crate::http::router(crate::http::HttpState { fleet: c.fleet.clone(), workloads: c.clone(), admin_token: Arc::new("admin".into()) });
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap(); });
    let client = reqwest::Client::new();
    let own = format!("namespace={ns}&tenant=eth&elf_id={elf}");
    for (method, path) in [
        (reqwest::Method::PUT, "apps/priority"),
        (reqwest::Method::PUT, "apps/disabled"),
        (reqwest::Method::DELETE, "apps/disabled"),
        (reqwest::Method::DELETE, "apps"),
    ] {
        for (token, selector) in [
            (&consumer, own.clone()),
            (&owner, format!("namespace={other_ns}&tenant=eth&elf_id={elf}")),
            (&owner, format!("{own}&tag=scope=research")),
            (&restricted, format!("namespace={ns}&tenant=eth&elf_id={other_elf}")),
            (&restricted, format!("namespace={ns}&tenant=goat&elf_id={elf}")),
        ] {
            let response = client.request(method.clone(), format!("{url}/admin/{path}?{selector}&priority=31"))
                .bearer_auth(token).send().await.unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN, "{method} {path}?{selector}");
        }
    }
    assert_eq!(client.put(format!("{url}/admin/apps/priority?{own}&priority=31")).bearer_auth(&restricted).send().await.unwrap().status(), StatusCode::OK);
    for (method, disabled) in [(reqwest::Method::PUT, true), (reqwest::Method::DELETE, false)] {
        let response = client.request(method, format!("{url}/admin/apps/disabled?{own}"))
            .bearer_auth(&owner).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let app: Value = response.json().await.unwrap();
        assert_eq!(app["disabled"], disabled);
        let apps = c.fleet.apps().await.unwrap();
        assert_eq!(apps.iter().find(|a| a["namespace"] == ns && a["elf_id"] == elf).unwrap()["priority"], 31);
    }
    for selector in [String::new(), "?tag=scope=default".into()] {
        let path = format!("{url}/admin/namespaces/{ns}/quota{selector}");
        for token in [&owner, &consumer, &restricted] {
            assert_eq!(client.put(&path).bearer_auth(token).json(&json!({"gpus":1,"cpus":4})).send().await.unwrap().status(), StatusCode::FORBIDDEN);
        }
        assert_eq!(client.put(&path).bearer_auth("admin").json(&json!({"gpus":1,"cpus":4})).send().await.unwrap().status(), StatusCode::OK);
    }
    assert_eq!(client.delete(format!("{url}/admin/apps?{own}")).bearer_auth(&owner).send().await.unwrap().status(), StatusCode::OK);
    assert!(c.fleet.app_known(&other_ns, "eth", &elf));
    assert_eq!(client.post(format!("{url}/admin/apps?{own}&tag=scope=default&priority=17")).bearer_auth(&owner).send().await.unwrap().status(), StatusCode::OK);
    server.abort();
}
fn request_resources(body: Value) -> ScaleRequest { serde_json::from_value(body).unwrap() }
async fn finish_service_command(c: &WorkloadController, ns: &str, proof: bool, command: &CommandRecord, disabled: bool, target: Option<Value>,
) {
    let mut report=scale_inventory(ns,proof,disabled);
    if let Some(target)=target { report.services[0]=target.into(); }
    report.results=vec![CommandResult {id:command.id.clone(),result: (if disabled {json!({"cleaned":true})}else{json!({"ready":true})})
        .into(),error:None,
    }];
    c.report("gpu","session",report).await.unwrap();
}

#[tokio::test]
async fn owner_scale_rejects_invalid_capacity_quota_and_one_shot_profiles() {
    use sha2::Digest;
    let (_dir,c,ns)=setup().await;
    c.report("gpu","session",scale_inventory(&ns,false,false)).await.unwrap();
    let secret="1".repeat(64);
    c.fleet.add_user_token("eth",&"a".repeat(32),&"b".repeat(40),"owner",None,
        Some(hex::encode(sha2::Sha256::digest(secret.as_bytes()))),Some(ns.clone()),Some("scope=default".into()),true,
        ).await.unwrap();
    let path=format!("/apps/service/scale?namespace={ns}");
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
    assert_eq!(scale_call(c.clone(),"admin","PUT",&format!("/apps/service/scale?namespace={}",other.id),json!({"replicas":1})).await.status(),StatusCode::NOT_FOUND);
    assert_eq!(scale_call(c.clone(),"admin","PUT",&format!("/apps/training/scale?namespace={ns}"),json!({"replicas":1})).await.status(),StatusCode::BAD_REQUEST);
    assert!(c.inner.lock().commands.is_empty());
    assert!(c.inner.lock().services.values().all(|s|s.scale.is_none()));
}

#[tokio::test]
async fn persistent_scale_survives_restarts_releases_old_units_and_waits_for_new_capacity() {
    let (_dir,c,ns)=setup().await;
    c.report("gpu","session",scale_inventory(&ns,false,false)).await.unwrap();
    assert_eq!(c.fleet.usage(&ns).gpus,2.0);
    let value=c.request_scale(&ns,"service",request_resources(json!({"gpus":2,"cpus":4,"replicas":1})),
        ).await.unwrap();
    assert_eq!(value["state"],"draining");
    for (method, path) in [("PUT", format!("/apps/service/disabled?namespace={ns}")),
        ("DELETE", format!("/apps/service/disabled?namespace={ns}")),
        ("DELETE", format!("/apps/service?namespace={ns}")),
    ] {
        assert_eq!(scale_call(c.clone(),"admin",method,&path,json!({})).await.status(),StatusCode::CONFLICT);
    }
    assert_eq!(c.inner.lock().apps["service"].profile["disabled"],false);
    assert!(c.request_scale(&ns,"service",request_resources(json!({"replicas":1}))).await.is_err());
    let c=Arc::new(WorkloadController::new(c.fleet.clone(),"admin".into(),"cluster".into()).unwrap());
    c.report("gpu","session",scale_inventory(&ns,false,false)).await.unwrap();
    c.dispatch().await.unwrap();
    let stop=c.inner.lock().commands.values().find(|v|v.parameters["scale_phase"]=="stop").unwrap().clone();
    assert!(stop.admitted);
    // Replaying a report cannot reopen the old allocation or change the target.
    c.report("gpu","session",scale_inventory(&ns,false,false)).await.unwrap();
    assert_eq!(c.scale_status(&ns,"service",None).unwrap()["gpus"],2.0);
    assert_eq!(c.app_resources(&ns,"service",None)["resources"]["gpus"],2.0,"listings show the durable requested settings while scaling");
    assert_eq!(c.inner.lock().commands.len(),1);
    finish_service_command(&c,&ns,false,&stop,true,None).await;
    assert_eq!(c.fleet.usage(&ns).gpus,0.0);
    // Another workload consumes the freed GPUs; scale waits rather than starting over capacity.
    let mut occupied=scale_inventory(&ns,false,true);occupied.free_gpus=0.0;
    c.report("gpu","session",occupied).await.unwrap();c.dispatch().await.unwrap();
    let start=c.inner.lock().commands.values().find(|v|v.parameters["scale_phase"]=="apply").unwrap().clone();
    assert!(!start.admitted);
    assert_eq!(c.fleet.usage(&ns).gpus,0.0);
    let c=Arc::new(WorkloadController::new(c.fleet.clone(),"admin".into(),"cluster".into()).unwrap());
    c.report("gpu","session",scale_inventory(&ns,false,true)).await.unwrap();c.dispatch().await.unwrap();
    assert_eq!(c.inner.lock().commands.len(),2);
    assert!(c.inner.lock().commands[&start.id].admitted);
    assert_eq!(c.fleet.usage(&ns).gpus,2.0);
    let mut observed=start.parameters["service"].clone();observed["disabled"]=json!(false);observed["ready"]=json!(true);observed["cleaned"]=json!(false);
    finish_service_command(&c,&ns,false,&start,false,Some(observed)).await;c.dispatch().await.unwrap();
    assert_eq!(c.scale_status(&ns,"service",None).unwrap()["state"],"complete");
    assert_eq!(c.inner.lock().apps["service"].profile["replicas"],1);
    assert_eq!(c.fleet.usage(&ns).gpus,2.0);
    // A node-local edit after completion cannot overwrite the durable desired allocation.
    c.report("gpu","session",scale_inventory(&ns,false,false)).await.unwrap();
    let repaired=c.scale_status(&ns,"service",None).unwrap();
    assert_eq!(repaired["state"],"draining");
    assert_eq!(repaired["gpus"],2.0);
    assert_eq!(repaired["replicas"],1);
    c.dispatch().await.unwrap();
    assert_eq!(c.inner.lock().commands.values().filter(|c|!c.done).count(),1);
}

#[tokio::test]
async fn proof_scale_drains_running_execution_before_delivery_and_keeps_disabled_apps_disabled() {
    use crate::store::{ProofJob,ProofJobState};
    use fleet_proto::fleet::v1::ProofKind;
    let (_dir,c,ns)=setup().await;let elf="a".repeat(64);
    c.fleet.add_app(&ns,"eth",&elf,0).await.unwrap();
    c.report("gpu","session",scale_inventory(&ns,true,false)).await.unwrap();
    let mut job=ProofJob{inputs:None,outputs:None,id:"active-proof".into(),tenant:"eth".into(),owner:"b".repeat(40),namespace:ns.clone(),tag:String::new(),client_id:"active-proof".into(),elf_id:elf.clone(),kind:ProofKind::Compressed as i32,priority:0,state:ProofJobState::Running as i32,
        fail_status: None,error:String::new(),node:Some("service-0".into()),attempts:1,failed_on:vec![],cycles:0,prove_ms:0,created_ms:1,started_ms:Some(1),finished_ms:None,
    };
    c.fleet.store.upsert(&job).await.unwrap();
    c.request_scale(&ns,&format!("eth/{elf}"),request_resources(json!({"replicas":1})),
    ).await.unwrap();
    for method in ["PUT", "DELETE"] {
        assert_eq!(scale_call(c.clone(),"admin",method,&format!("/proof-apps/disabled?namespace={ns}&tenant=eth&elf_id={elf}"),json!({})).await.status(),StatusCode::CONFLICT);
    }
    let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url=format!("http://{}",listener.local_addr().unwrap());
    let router=crate::http::router(crate::http::HttpState{fleet:c.fleet.clone(),workloads:c.clone(),admin_token:Arc::new("admin".into()),
    });
    let server=tokio::spawn(async move { axum::serve(listener,router).await.unwrap(); });
    for (method,path) in [(reqwest::Method::PUT,"/admin/apps/disabled"),
        (reqwest::Method::DELETE,"/admin/apps/disabled"),(reqwest::Method::DELETE,"/admin/apps"),
    ] {
        let response=reqwest::Client::new().request(method,format!("{url}{path}"))
            .bearer_auth("admin").query(&[("namespace",&ns),("tenant",&"eth".to_string()),("elf_id",&elf),
            ]).send().await.unwrap();
        assert_eq!(response.status(),StatusCode::CONFLICT);
    }
    server.abort();
    assert!(c.fleet.app_known(&ns,"eth",&elf));
    assert!(!c.fleet.app_disabled(&ns,"eth",&elf));
    c.dispatch().await.unwrap();
    assert!(c.fleet.app_disabled(&ns,"eth",&elf));
    let stop=c.inner.lock().commands.values().find(|v|v.parameters["scale_phase"]=="stop").unwrap().clone();
    assert!(!stop.admitted);
    job.state=ProofJobState::Succeeded as i32;job.finished_ms=Some(2);c.fleet.store.upsert(&job).await.unwrap();
    c.dispatch().await.unwrap();assert!(c.inner.lock().commands[&stop.id].admitted);
    finish_service_command(&c,&ns,true,&stop,true,None).await;c.dispatch().await.unwrap();
    let start=c.inner.lock().commands.values().find(|v|v.parameters["scale_phase"]=="apply").unwrap().clone();
    let mut observed=start.parameters["service"].clone();observed["disabled"]=json!(false);observed["ready"]=json!(true);
    finish_service_command(&c,&ns,true,&start,false,Some(observed)).await;
    assert!(c.fleet.app_disabled(&ns,"eth",&elf));
    c.dispatch().await.unwrap();
    assert!(!c.fleet.app_disabled(&ns,"eth",&elf));
    c.fleet.set_app_disabled(&ns,"eth",&elf,true).await.unwrap();
    // The authoritative lifecycle intent, not an old observed flag, determines whether scaling resumes the app.
    let mut stopped=start.parameters["service"].clone();
    stopped["disabled"]=json!(true);stopped["ready"]=json!(false);stopped["cleaned"]=json!(true);
    c.command("gpu",NodeOperation::SetService,json!({"service":stopped,"disabled":true}),
    ).unwrap();
    let normal=c.inner.lock().commands.values().find(|v|!v.done).unwrap().clone();
    finish_service_command(&c,&ns,true,&normal,true,Some(stopped)).await;
    c.request_scale(&ns,&format!("eth/{elf}"),request_resources(json!({"gpus":2,"replicas":1})),
    ).await.unwrap();
    c.dispatch().await.unwrap();
    let stop=c.inner.lock().commands.values().find(|v|!v.done).unwrap().clone();
    finish_service_command(&c,&ns,true,&stop,true,None).await;c.dispatch().await.unwrap();
    let apply=c.inner.lock().commands.values().find(|v|!v.done).unwrap().clone();
    assert_eq!(apply.parameters["disabled"],true);
    let mut observed=apply.parameters["service"].clone();observed["disabled"]=json!(true);observed["cleaned"]=json!(true);observed["ready"]=json!(false);
    finish_service_command(&c,&ns,true,&apply,true,Some(observed)).await;c.dispatch().await.unwrap();
    assert!(c.fleet.app_disabled(&ns,"eth",&elf));
    assert_eq!(c.scale_status(&ns,&format!("eth/{elf}"),None).unwrap()["state"],"complete");
}

#[tokio::test]
async fn scale_failure_retains_new_reservation_until_confirmed_cleanup_and_can_be_retried() {
    let (_dir,c,ns)=setup().await;
    c.report("gpu","session",scale_inventory(&ns,false,false)).await.unwrap();
    c.request_scale(&ns,"service",request_resources(json!({"gpus":2,"cpus":4,"replicas":1})),
    ).await.unwrap();
    c.dispatch().await.unwrap();
    let stop=c.inner.lock().commands.values().find(|v|!v.done).unwrap().clone();
    finish_service_command(&c,&ns,false,&stop,true,None).await;c.dispatch().await.unwrap();
    let apply=c.inner.lock().commands.values().find(|v|!v.done).unwrap().clone();
    assert!(apply.admitted);assert_eq!(c.fleet.usage(&ns).gpus,2.0);
    let mut failed=scale_inventory(&ns,false,true);
    let mut partial=apply.parameters["service"].clone();partial["disabled"]=json!(false);partial["ready"]=json!(false);partial["cleaned"]=json!(false);
    failed.services=vec![partial.clone().into()];
    failed.results=vec![CommandResult {id:apply.id,error:Some("container start failed".into()),result:Value::Null.into(),
    }];
    c.report("gpu","session",failed).await.unwrap();c.dispatch().await.unwrap();
    assert_eq!(c.scale_status(&ns,"service",None).unwrap()["state"],"failed");
    assert_eq!(c.fleet.usage(&ns).gpus,2.0);
    let mut report=scale_inventory(&ns,false,true);report.services=vec![partial.into()];
    c.report("gpu","session",report).await.unwrap();c.dispatch().await.unwrap();
    let cleanup=c.inner.lock().commands.values().find(|v|!v.done).unwrap().clone();
    assert_eq!(cleanup.parameters["disabled"],true);
    let mut observed=cleanup.parameters["service"].clone();observed["disabled"]=json!(true);observed["ready"]=json!(false);observed["cleaned"]=json!(true);
    finish_service_command(&c,&ns,false,&cleanup,true,Some(observed)).await;
    assert_eq!(c.fleet.usage(&ns).gpus,0.0);
    assert_eq!(c.scale_status(&ns,"service",None).unwrap()["state"],"failed");
    let retried=c.request_scale(&ns,"service",request_resources(json!({"replicas":1}))).await.unwrap();
    assert_eq!(retried["state"],"draining");assert_eq!(retried["disabled"],false);
}

#[tokio::test]
async fn public_nodes_are_machines_and_machine_drain_covers_current_and_future_proof_replicas() {
    use fleet_proto::fleet::v1::NodeInfo;
    let (_dir,c,ns)=setup().await;
    let elf="a".repeat(64);
    c.fleet.add_app(&ns,"eth",&elf,0).await.unwrap();
    let mut report=inventory(&ns);
    report.services=vec![json!({"name":"proof","kind":"ziren-gpu","namespace":ns,
        "proof_app":format!("eth/{elf}"),"gpus":1,"cpus":2,"replicas":1,"disabled":false,"ready":true}).into()];
    c.report("gpu","session",report).await.unwrap();
    let replica=NodeInfo{name:"proof-0".into(),machine:"machine".into(),backend:"cuda".into(),
        gpus:1,cpus:2,control_version:1,app_namespace:ns,app_tenant:"eth".into(),app_elf_id:elf.clone(),..Default::default()};
    let id=c.fleet.register(replica.clone()).unwrap();
    c.fleet.heartbeat(&id,true,vec![],true,"proof-job");
    c.fleet.register(NodeInfo{name:"other-0".into(),machine:"other-machine".into(),..replica.clone()}).unwrap();
    let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url=format!("http://{}",listener.local_addr().unwrap());
    let router=crate::http::router(crate::http::HttpState{fleet:c.fleet.clone(),workloads:c.clone(),admin_token:Arc::new("admin".into()),
    });
    let server=tokio::spawn(async move { axum::serve(listener,router).await.unwrap(); });
    let client=reqwest::Client::new();
    let status:Value=client.get(format!("{url}/admin/status")).bearer_auth("admin").send().await.unwrap().json().await.unwrap();
    assert_eq!(status["nodes"].as_array().unwrap().len(),1);
    assert_eq!(status["nodes"][0]["name"],"gpu");
    assert_eq!(status["nodes"][0]["gpus"],2.0);
    assert_eq!(status["nodes"][0]["cpus"],8.0);
    assert_eq!(status["nodes"][0]["busy"],"~proof-job");
    assert_eq!(status["nodes"][0]["apps"],json!([format!("eth/{elf}")]));
    for method in [reqwest::Method::POST,reqwest::Method::DELETE] {
        assert_eq!(client.request(method,format!("{url}/admin/nodes/proof-0/drain")).bearer_auth("admin").send().await.unwrap().status(),StatusCode::NOT_FOUND);
    }
    assert_eq!(client.post(format!("{url}/admin/nodes/gpu/drain")).bearer_auth("admin").send().await.unwrap().status(),StatusCode::NO_CONTENT);
    assert!(c.capacity("gpu").is_none());
    let new=c.fleet.register(NodeInfo{name:"proof-1".into(),..replica}).unwrap();
    c.fleet.heartbeat(&new,true,vec![],false,"");
    assert!(c.fleet.status().nodes.iter().all(|n|n.draining==(n.machine=="machine")));
    assert_eq!(c.worker_status()[0]["busy"],"~proof-job","draining must preserve the active proof");
    assert_eq!(client.delete(format!("{url}/admin/nodes/gpu/drain")).bearer_auth("admin").send().await.unwrap().status(),StatusCode::NO_CONTENT);
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
    let path = format!("/batches/{}/logs?rank=1", job.id);
    let response = scale_call(c, "admin", "GET", &path, Value::Null).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn integrated_workers_reserve_resources_reject_scaling_and_restore_admission_guard() {
    let (_dir, c, ns) = setup().await;
    let elf = "a".repeat(64);
    let mut report = scale_inventory(&ns, true, false);
    report.services[0]["worker_command"] = json!(["/opt/eth-proofs"]);
    report.services[0]["machine"] = json!("machine");
    report.services[0]["replicas"] = json!(1);
    c.report("gpu", "session", report).await.unwrap();
    assert!(c.fleet.app_worker_owned(&ns, "eth", &elf));
    assert!(!c.fleet.app_worker_owned(&ns, "goat", &elf));
    assert_eq!(c.fleet.usage(&ns).gpus, 1.0);
    assert_eq!(c.fleet.usage(&ns).cpus, 2.0);
    assert!(c.request_scale(&ns, &format!("eth/{elf}"), request_resources(json!({"replicas":2}))).await.is_err());
    c.fleet.set_worker_owned_apps(HashSet::new());
    let _restarted = WorkloadController::new(c.fleet.clone(), "admin".into(), "cluster".into()).unwrap();
    assert!(c.fleet.app_worker_owned(&ns, "eth", &elf));
}

#[tokio::test]
async fn integrated_worker_owner_must_be_cleaned_before_moving() {
    let (_dir, c, ns) = setup().await;
    let mut report = scale_inventory(&ns, true, false);
    report.services[0]["worker_command"] = json!(["/opt/eth-proofs"]);
    report.services[0]["replicas"] = json!(1);
    let mut invalid = report.clone();
    invalid.services[0]["replicas"] = json!(2);
    assert!(c.report("gpu", "session", invalid).await.is_err());
    let mut duplicate = report.clone();
    let mut another = duplicate.services[0].clone();
    another["name"] = json!("second");
    duplicate.services.push(another);
    assert!(c.report("gpu", "session", duplicate).await.is_err());
    c.report("gpu", "session", report.clone()).await.unwrap();
    assert!(c.report("other", "session", report.clone()).await.is_err());
    let service = report.services[0].clone();
    assert!(c.command("other", NodeOperation::SetService,
        json!({"service":service,"disabled":false})).is_err());
    c.inner.lock().services.get_mut("gpu-service").unwrap().disabled = true;
    assert!(c.command("other", NodeOperation::SetService,
        json!({"service":service,"disabled":false})).is_err());
    c.fleet.release_job(&ns, "service:gpu-service").await.unwrap();
    assert!(c.command("other", NodeOperation::SetService,
        json!({"service":service,"disabled":false})).is_ok());
}
