use super::*;
use fleet_proto::fleet::v1::NodeInfo;
use fleet_proto::model::v1::{ProofJob, ProofJobState};

#[tokio::test]
async fn packaged_prover_cutover_drains_and_preserves_jobs_and_app_identity() {
    let (_dir, c, ns) = setup().await;
    register(&c, &ns, PROOF, &proof_elf()).await;
    let mut report = scale_inventory(&ns, true, false);
    c.report("gpu", "session", report.clone()).await.unwrap();
    let original = c.inner.lock().deployments["gpu-model"].spec.clone();
    let update = json!({"runtime":{"executor":"shim","driver":"docker"},
        "image":format!("registry.example/proof@sha256:{}", "c".repeat(64)),
        "port_base":0,"container_port":8080,"health_path":"/healthz"});
    assert!(c
        .request_update(&ns, PROOF, None, update.clone())
        .await
        .is_err());
    report.runtime_capabilities = vec!["shim-docker-v1".into(),"shim-prover-v1".into(),"shim-runtime-v2".into(),"shim-automatic-port-v1".into()];
    c.report("gpu", "session", report.clone()).await.unwrap();
    assert_eq!(c.inner.lock().deployments["gpu-model"].spec, original);

    let mut running = ProofJob {
        id: "running-proof".into(),
        client_id: "running-proof".into(),
        owner: "b".repeat(40),
        kind: fleet_proto::fleet::v1::ProofKind::Compressed as i32,
        namespace: ns.clone(),
        app: PROOF.into(),
        elf_id: proof_elf(),
        node: Some("model-0".into()),
        state: ProofJobState::Running as i32,
        ..Default::default()
    };
    let queued = ProofJob {
        id: "queued-proof".into(),
        client_id: "queued-proof".into(),
        node: None,
        state: ProofJobState::Queued as i32,
        ..running.clone()
    };
    c.fleet.store.upsert(&running).await.unwrap();
    c.fleet.store.upsert(&queued).await.unwrap();
    let node = c
        .fleet
        .register(NodeInfo {
            name: "model-0".into(),
            machine: "machine".into(),
            backend: "cuda".into(),
            gpus: 1,
            cpus: 2,
            control_version: 1,
            app_namespace: ns.clone(),
            app: PROOF.into(),
            app_elf_id: proof_elf(),
            ..Default::default()
        })
        .unwrap();
    c.fleet.heartbeat(&node, true, vec![], true, &running.id);
    c.request_update(&ns, PROOF, None, update.clone())
        .await
        .unwrap();
    c.dispatch().await.unwrap();
    assert!(
        !c.inner
            .lock()
            .commands
            .values()
            .any(|cmd| cmd.admitted && cmd.stops()),
        "an active proof must finish before its container is stopped"
    );
    running.state = ProofJobState::Succeeded as i32;
    running.finished_ms = Some(123);
    c.fleet.store.upsert(&running).await.unwrap();
    c.fleet.heartbeat(&node, true, vec![], false, "");
    c.dispatch().await.unwrap();
    let stop = command_where(&c, |cmd| cmd.parameters["scale_phase"] == "stop").unwrap();
    assert!(stop.admitted);
    assert!(stop.entry().0.get("runtime").is_none());
    report.apps[0]["disabled"] = json!(true);
    report.apps[0]["cleaned"] = json!(true);
    report.apps[0]["ready"] = json!(false);
    report.free_gpus = 2.0;
    report.results = vec![CommandResult {
        id: stop.id,
        result: json!({"cleaned":true}).into(),
        error: None,
    }];
    c.report("gpu", "session", report.clone()).await.unwrap();
    c.dispatch().await.unwrap();
    let write = command_where(&c, |cmd| cmd.is(NodeOperation::App)).unwrap();
    for (key, value) in update.as_object().unwrap() {
        report.apps[0][key] = value.clone();
    }
    report.results = vec![CommandResult {
        id: write.id,
        result: json!({"updated":["model"]}).into(),
        error: None,
    }];
    c.report("gpu", "session", report.clone()).await.unwrap();
    c.dispatch().await.unwrap();
    let start = command_where(&c, |cmd| cmd.parameters["scale_phase"] == "apply").unwrap();
    assert_eq!(start.entry().runtime_capability(), Some("shim-prover-v1"));
    for key in ["name", "app", "namespace", "elf_id"] {
        assert_eq!(start.entry().0[key], original[key]);
    }
    assert_eq!(
        c.fleet.store.get(&queued.id).await.unwrap().unwrap(),
        queued
    );
    assert_eq!(
        c.fleet.store.get(&running.id).await.unwrap().unwrap(),
        running
    );
    assert_eq!(c.fleet.app(&ns, PROOF).unwrap().elf_id, proof_elf());
    let restarted = WorkloadController::new(c.fleet.clone(), "cluster".into()).unwrap();
    assert_eq!(
        restarted.inner.lock().deployments["gpu-model"]
            .entry()
            .runtime_capability(),
        Some("shim-prover-v1")
    );
}
