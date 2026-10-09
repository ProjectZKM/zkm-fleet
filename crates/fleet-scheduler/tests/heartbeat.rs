//! Outbound-only control: durable leases, node-owned output and retry safety.
use fleet_proto::fleet::v1::{Assignment, Completion, HeartbeatRequest, NodeInfo, ProofKind};
use fleet_scheduler::{
    blobs::{self, Blobs},
    config::Config,
    fleet::Fleet,
    quota::Quota,
    store::{Job, JobState, MemoryStore, Store},
};
use std::{sync::Arc, time::Duration};

fn config(dir: &std::path::Path) -> Config {
    serde_yaml::from_str(&format!("public_url: http://unused\ndata_dir: {}\ncluster_token_file: unused\nadmin_token_file: unused\nheartbeat_secs: 1\ntenants:\n  - name: proof\n", dir.display())).unwrap()
}

async fn fresh(cfg: Config, store: Arc<MemoryStore>) -> Arc<Fleet> {
    let f = Fleet::new(
        cfg.clone(),
        store,
        Blobs::new(&cfg.data_dir).unwrap(),
        "a".repeat(48),
    );
    f.load_tokens().await.unwrap();
    f.load_apps().await.unwrap();
    f.recover().await.unwrap();
    f
}

fn info(name: &str, namespace: &str, elf_id: &str) -> NodeInfo {
    NodeInfo {
        name: name.into(),
        backend: "cuda".into(),
        gpus: 2,
        cpus: 8,
        machine: "gpu-host".into(),
        control_version: 1,
        app_namespace: namespace.into(),
        app_tenant: "proof".into(),
        app_elf_id: elf_id.into(),
        ..Default::default()
    }
}

async fn assigned(f: &Fleet, node: &str) -> Assignment {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let reply = f
            .exchange(HeartbeatRequest {
                node_id: node.into(),
                ready: true,
                ..Default::default()
            })
            .await
            .unwrap();
        if let Some(a) = reply.assignment {
            return a;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn restart_keeps_attempt_quota_and_accepts_retried_completion_once() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let store = Arc::new(MemoryStore::default());
    let f = fresh(cfg.clone(), store.clone()).await;
    let namespace = f
        .create_namespace(
            "team",
            Quota {
                gpus: Some(2.0),
                cpus: Some(8.0),
            },
        )
        .await
        .unwrap();
    let elf_id = f.blobs.put_elf(b"elf").unwrap();
    store
        .add_app(&namespace.id, "proof", &elf_id, 1, 0)
        .await
        .unwrap();
    f.load_apps().await.unwrap();
    for name in [blobs::STDIN, blobs::RECEIPTS] {
        f.blobs
            .put(
                "job",
                name,
                &bincode::serialize(&Vec::<Vec<u8>>::new()).unwrap(),
            )
            .unwrap();
    }
    f.submit(Job {
        id: "job".into(),
        client_id: "job".into(),
        namespace: namespace.id.clone(),
        owner: "ab".repeat(20),
        tag: String::new(),
        tenant: "proof".into(),
        elf_id: elf_id.clone(),
        priority: 0,
        kind: ProofKind::Compressed,
        state: JobState::Queued,
        error: String::new(),
        node: None,
        attempts: 0,
        failed_on: vec![],
        cycles: 0,
        prove_ms: 0,
        created_ms: 1,
        started_ms: None,
        finished_ms: None,
    })
    .await
    .unwrap();
    assert!(f
        .register(NodeInfo {
            control_version: 0,
            ..info("old-worker", &namespace.id, &elf_id)
        })
        .is_err());
    let original = f.register(info("worker", &namespace.id, &elf_id)).unwrap();
    let dispatcher = tokio::spawn(f.clone().run_dispatcher());
    let assignment = assigned(&f, &original).await;
    assert_eq!(f.usage(&namespace.id).gpus, 2.0);
    // Losing just the worker session must not strand a running assignment.
    f.deregister(&original);
    let original = f.register(info("worker", &namespace.id, &elf_id)).unwrap();
    assert_eq!(assigned(&f, &original).await, assignment);
    assert_eq!(f.job("job").await.unwrap().unwrap().attempts, 1);
    assert_eq!(f.usage(&namespace.id).gpus, 2.0);
    let other = f.register(info("other", &namespace.id, &elf_id)).unwrap();
    assert_eq!(
        f.fetch_inputs(&other, &assignment, true)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    let mut stale = assignment.clone();
    stale.lease.push('0');
    assert_eq!(
        f.fetch_inputs(&original, &stale, true)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    // The scheduler has no proof output and still tracks the active reservation.
    assert!(f.blobs.get("job", blobs::PROOF).is_err());
    assert_eq!(
        f.job("job").await.unwrap().unwrap().state,
        JobState::Running
    );
    dispatcher.abort();
    let recovered = fresh(cfg, store.clone()).await;
    assert_eq!(recovered.usage(&namespace.id).gpus, 2.0);
    assert!(
        !recovered
            .exchange(HeartbeatRequest {
                node_id: original,
                ready: true,
                ..Default::default()
            })
            .await
            .unwrap()
            .known
    );
    let session = recovered.register(info("worker", &namespace.id, &elf_id)).unwrap();
    let dispatcher = tokio::spawn(recovered.clone().run_dispatcher());
    let resumed = assigned(&recovered, &session).await;
    assert_eq!(
        resumed, assignment,
        "restart cannot create a second execution attempt"
    );
    let request = HeartbeatRequest {
        node_id: session.clone(),
        ready: true,
        busy: true,
        running_job: "job".into(),
        completion: Some(Completion {
            assignment: Some(assignment.clone()),
            cycles: 42,
            prove_ms: 9,
            ..Default::default()
        }),
        ..Default::default()
    };
    for _ in 0..2 {
        // Simulate losing the first completion acknowledgment.
        let reply = recovered.exchange(request.clone()).await.unwrap();
        assert_eq!(reply.completed_lease, assignment.lease);
        assert!(reply.assignment.is_none());
    }
    let saved = store.get("job").await.unwrap().unwrap();
    assert_eq!(saved.state, JobState::Succeeded);
    assert_eq!(saved.attempts, 1);
    assert_eq!(saved.cycles, 42);
    assert_eq!(recovered.usage(&namespace.id).gpus, 0.0);
    assert!(
        recovered.blobs.get("job", blobs::PROOF).is_err(),
        "completion must not require scheduler-held output"
    );
    let before = recovered.job("job").await.unwrap().unwrap();
    let reply = recovered
        .exchange(HeartbeatRequest {
            node_id: session,
            ready: true,
            completion: Some(Completion {
                assignment: Some(stale.clone()),
                error_code: 13,
                error: "delayed".into(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(reply.completed_lease, stale.lease);
    assert_eq!(
        recovered.job("job").await.unwrap().unwrap().finished_ms,
        before.finished_ms
    );
    let mut next = saved.clone();
    next.id = "next-job".into();
    next.client_id = "next-job".into();
    next.state = JobState::Queued;
    next.node = None;
    next.attempts = 0;
    next.started_ms = None;
    next.finished_ms = None;
    for name in [blobs::STDIN, blobs::RECEIPTS] {
        recovered
            .blobs
            .put(
                &next.id,
                name,
                &bincode::serialize(&Vec::<Vec<u8>>::new()).unwrap(),
            )
            .unwrap();
    }
    recovered.submit(next).await.unwrap();
    let next = assigned(&recovered, &request.node_id).await;
    let delayed = recovered.exchange(request).await.unwrap();
    assert_eq!(
        delayed.assignment,
        Some(next),
        "old completion must not release the next job's worker"
    );
    assert_eq!(recovered.usage(&namespace.id).gpus, 2.0);
    dispatcher.abort();
}
