//! Outbound-only control: durable leases, node-owned output and retry safety.
use fleet_proto::fleet::v1::{Assignment, Completion, HeartbeatRequest, NodeInfo, ProofKind};
use fleet_scheduler::{
    blobs::{self, BlobStore},
    config::SchedulerConfig,
    fleet::Scheduler,
    quota::Quota,
    store::{ProofJob, ProofJobState, MemoryStore, Store},
};
use std::{sync::Arc, time::Duration};

fn config(dir: &std::path::Path) -> SchedulerConfig {
    serde_yaml::from_str(&format!("public_url: http://unused\ndata_dir: {}\ncluster_token_file: unused\nheartbeat_secs: 1\n", dir.display())).unwrap()
}

async fn fresh(cfg: SchedulerConfig, store: Arc<MemoryStore>) -> Arc<Scheduler> {
    let f = Scheduler::new(
        cfg.clone(),
        store,
        BlobStore::new(&cfg.data_dir).unwrap().with_proof_output(
            cfg.proof_output_dir.clone().unwrap_or_else(|| cfg.data_dir.join("proofs")),
        ),
        "a".repeat(48),
    );
    f.load_tokens().await.unwrap();
    f.load_apps().await.unwrap();
    f.recover().await.unwrap();
    f
}

/// The proof app the tests' provers serve: program `elf_id` in the namespace.
fn app(namespace: &str, elf_id: &str) -> fleet_scheduler::store::App {
    fleet_scheduler::store::App {
        namespace: namespace.into(), name: "proof".into(), elf_id: elf_id.into(), created_ms: 1,
        class: "batch".into(), anchor: "none".into(), max_queued: 64, ..Default::default()
    }
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
        app: "proof".into(),
        app_elf_id: elf_id.into(),
        ..Default::default()
    }
}

async fn assigned(f: &Scheduler, node: &str) -> Assignment {
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
    store.add_app(&app(&namespace.id, &elf_id)).await.unwrap();
    f.load_apps().await.unwrap();
    let apps = f.apps().await.unwrap();
    assert_eq!((apps[0].0.name.as_str(), apps[0].0.elf_id.as_str()), ("proof", elf_id.as_str()));
    for name in [blobs::STDIN, blobs::RECEIPTS] {
        f.blobs
            .put(
                "job",
                name,
                &bincode::serialize(&Vec::<Vec<u8>>::new()).unwrap(),
            )
            .unwrap();
    }
    f.submit(ProofJob {
                inputs: None, outputs: None,
        id: "job".into(),
        client_id: "job".into(),
        namespace: namespace.id.clone(),
        owner: "ab".repeat(20),
        tag: String::new(),
        app: "proof".into(),
        elf_id: elf_id.clone(),
        priority: 0,
        kind: ProofKind::Compressed as i32,
        state: ProofJobState::Queued as i32,
        fail_status: None,
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
    let dispatcher = tokio::spawn(f.clone().dispatch_loop());
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
        f.job("job").await.unwrap().unwrap().state(),
        ProofJobState::Running
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
    let dispatcher = tokio::spawn(recovered.clone().dispatch_loop());
    let resumed = assigned(&recovered, &session).await;
    assert_eq!(
        resumed, assignment,
        "restart cannot create a second execution attempt"
    );
    let result = fleet_proto::fleet::v1::ProveResponse {
        proof_json: b"proof".to_vec(), public_values: vec![1,2], cycles:42, prove_ms:9,
        ..Default::default()
    };
    let foreign = recovered.register(info("other", &namespace.id, &elf_id)).unwrap();
    for (node,a) in [(&foreign,&assignment),(&session,&stale)] {
        assert_eq!(recovered.publish_output(node,a,result.clone()).await.unwrap_err().code(),tonic::Code::FailedPrecondition);
    }
    for _ in 0..2 { recovered.publish_output(&session,&assignment,result.clone()).await.unwrap(); }
    assert_eq!(recovered.usage(&namespace.id).gpus,2.0,"upload must not release an active execution");
    assert_eq!(recovered.blobs.sdk_output(&recovered.job("job").await.unwrap().unwrap(),blobs::PROOF).await.unwrap(),b"proof");
    let mut changed=result.clone(); changed.proof_json=b"different".to_vec();
    assert!(recovered.publish_output(&session,&assignment,changed).await.is_err());
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
    assert_eq!(saved.state(), ProofJobState::Succeeded);
    assert_eq!(saved.attempts, 1);
    assert_eq!(saved.cycles, 42);
    recovered.publish_output(&session,&assignment,result).await.unwrap();
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
    next.state = ProofJobState::Queued as i32;
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

#[tokio::test]
async fn metadata_completion_is_lease_fenced_and_survives_scheduler_restart() {
    use fleet_proto::fleet::v1::{ArtifactRef, ProofInputs, ProofOutputs, ProveResponse};
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let store = Arc::new(MemoryStore::default());
    let f = fresh(cfg.clone(), store.clone()).await;
    let namespace = f.create_namespace("app", Quota {gpus:Some(2.0),cpus:Some(8.0),
            },
        ).await.unwrap();
    let elf_id = fleet_proto::artifacts::hash(b"elf");
    store.add_app(&app(&namespace.id, &elf_id)).await.unwrap();
    f.load_apps().await.unwrap();
    let input = ArtifactRef {uri:"file:///app/inputs/hash".into(),sha256:"a".repeat(64),size:3,
    };
    f.submit(ProofJob {
        inputs:Some(ProofInputs {input:Some(input),elf:Some(ArtifactRef {uri:"file:///app/elf/hash".into(),sha256:elf_id.clone(),size:3,
            }),output_root:"file:///app/outputs".into(),
        }),
        outputs:None,id:"metadata-job".into(),client_id:"metadata-job".into(),namespace:namespace.id.clone(),owner:"ab".repeat(20),tag:String::new(),app:"proof".into(),elf_id:elf_id.clone(),priority:0,kind:ProofKind::Compressed as i32,
        state:ProofJobState::Queued as i32,
        fail_status: None,error:String::new(),node:None,attempts:0,failed_on:vec![],cycles:0,prove_ms:0,created_ms:1,started_ms:None,finished_ms:None,
    }).await.unwrap();
    let session = f.register(info("worker",&namespace.id,&elf_id)).unwrap();
    let dispatch = tokio::spawn(f.clone().dispatch_loop());
    let assignment = assigned(&f,&session).await;
    assert!(assignment.inputs.is_some());
    assert_eq!(assignment.kind,ProofKind::Compressed as i32);
    assert!(f.fetch_inputs(&session,&assignment,true).await.is_err());
    assert!(f.publish_output(&session,&assignment,ProveResponse {proof_json:b"proof".to_vec(),..Default::default()}).await.is_err());
    dispatch.abort();
    let recovered = fresh(cfg,store.clone()).await;
    let session = recovered.register(info("worker",&namespace.id,&elf_id)).unwrap();
    let dispatch = tokio::spawn(recovered.clone().dispatch_loop());
    assert_eq!(assigned(&recovered,&session).await,assignment);
    let outputs = ProofOutputs {
        machine: "gpu-host".into(),
        proof:Some(ArtifactRef {uri:format!("file:///app/outputs/metadata-job/{}/proof.json",assignment.lease),sha256:fleet_proto::artifacts::hash(b"proof"),size:5,
        }),
        public_values:Some(ArtifactRef {uri:format!("file:///app/outputs/metadata-job/{}/public_values.bin",assignment.lease),sha256:fleet_proto::artifacts::hash(b"values"),size:6,
        }),
    };
    let heartbeat = HeartbeatRequest {node_id:session,ready:true,busy:true,running_job:"metadata-job".into(),completion:Some(Completion {assignment:Some(assignment.clone()),cycles:42,prove_ms:9,outputs:Some(outputs.clone()),..Default::default()}),..Default::default()};
    let mut missing = heartbeat.clone();missing.completion.as_mut().unwrap().outputs=None;
    assert_eq!(recovered.exchange(missing).await.unwrap_err().code(),tonic::Code::InvalidArgument);
    let mut wrong = heartbeat.clone();wrong.completion.as_mut().unwrap().outputs.as_mut().unwrap().proof.as_mut().unwrap().uri="file:///app/outputs/other/proof.json".into();
    assert_eq!(recovered.exchange(wrong).await.unwrap_err().code(),tonic::Code::FailedPrecondition);
    assert_eq!(recovered.usage(&namespace.id).gpus,2.0);
    assert_eq!(store.get("metadata-job").await.unwrap().unwrap().state(),ProofJobState::Running);
    for _ in 0..2 {assert_eq!(recovered.exchange(heartbeat.clone()).await.unwrap().completed_lease,assignment.lease);}
    let saved = store.get("metadata-job").await.unwrap().unwrap();
    assert_eq!(saved.outputs,Some(outputs));assert_eq!((saved.cycles,saved.prove_ms,saved.attempts),(42,9,1));
    assert_eq!(recovered.usage(&namespace.id).gpus,0.0);
    assert!(!dir.path().join("proofs").exists(),"Scheduler must not read or write output bytes");
    dispatch.abort();
}

#[tokio::test]
async fn machine_drain_blocks_new_proofs_and_keeps_active_execution_and_completion() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(MemoryStore::default());
    let f = fresh(config(dir.path()), store.clone()).await;
    let ns = f.create_namespace("app", Quota::default()).await.unwrap();
    let elf = f.blobs.put_elf(b"elf").unwrap();
    f.add_app(&ns.id, &fleet_proto::model::v1::RegisterAppRequest { name: "proof".into(), elf_id: elf.clone(), ..Default::default() }).await.unwrap();
    let job = ProofJob {
        inputs: None, outputs: None, id: "active".into(), client_id: "active".into(),
        namespace: ns.id.clone(), owner: "ab".repeat(20), tag: String::new(),
        app: "proof".into(), elf_id: elf.clone(), priority: 0, kind: ProofKind::Compressed as i32,
        state: ProofJobState::Queued as i32,
        fail_status: None, error: String::new(), node: None, attempts: 0,
        failed_on: vec![], cycles: 0, prove_ms: 0, created_ms: 1, started_ms: None, finished_ms: None,
    };
    f.submit(job.clone()).await.unwrap();
    let session = f.register(info("proof-0", &ns.id, &elf)).unwrap();
    let dispatcher = tokio::spawn(f.clone().dispatch_loop());
    let assignment = assigned(&f, &session).await;
    f.set_machine_drain("gpu-host", true);
    f.submit(ProofJob {id: "next".into(), client_id: "next".into(), created_ms: 2, ..job}).await.unwrap();
    let late = f.register(info("proof-1", &ns.id, &elf)).unwrap();
    let running = f.exchange(HeartbeatRequest {node_id: session.clone(), ready: true,
        busy: true, running_job: "active".into(), ..Default::default()}).await.unwrap();
    assert_eq!(running.assignment, Some(assignment.clone()));
    let completed = f.exchange(HeartbeatRequest {node_id: session.clone(), ready: true,
        completion: Some(Completion {assignment: Some(assignment.clone()), cycles: 42, prove_ms: 9, ..Default::default()}),
        ..Default::default()}).await.unwrap();
    assert_eq!(completed.completed_lease, assignment.lease);
    assert_eq!(f.job("active").await.unwrap().unwrap().state(), ProofJobState::Succeeded);
    // Allow multiple dispatcher passes with both current and newly registered replicas ready.
    for _ in 0..10 {
        for session in [&session, &late] {
            assert!(f.exchange(HeartbeatRequest {node_id: session.clone(), ready: true, ..Default::default()}).await.unwrap().assignment.is_none());
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert_eq!(f.job("next").await.unwrap().unwrap().state(), ProofJobState::Queued);
    assert_eq!(f.usage(&ns.id).gpus, 0.0);
    f.set_machine_drain("gpu-host", false);
    assert_eq!(assigned(&f, &session).await.job_id, "next");
    dispatcher.abort();
}
