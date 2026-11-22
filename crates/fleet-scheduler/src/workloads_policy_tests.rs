//! Full HTTP administration + HTTP/2 node RPC on the same listener. The fake
//! receipt verifier isolates orchestration; production pins the Ziren verifier.
use super::*;
use trusted_shim::{
    contracts::{ExecutionIdentity, PolicyRef, ProofKind, Validity},
    ledger::SqliteLedger,
    ledger_transport::{ExecutionAuthority, RemoteLedger},
    proof::{ProgramRef, ProofReceipt, ReceiptVerifier},
    rail::{rego::*, AuthorizationLedger, EvaluationRequest, PolicyBundle, RuleEngine},
};

struct TestVerifier;
impl ReceiptVerifier for TestVerifier {
    fn verify(
        &self,
        _: &ProgramRef,
        receipt: &ProofReceipt,
        expected: &[u8],
    ) -> trusted_shim::Result<()> {
        if receipt.encoded_receipt == expected {
            Ok(())
        } else {
            Err(trusted_shim::Error::Untrusted)
        }
    }
}

async fn policy_setup_with_driver(
    driver: &str,
) -> (
    tempfile::TempDir,
    Arc<WorkloadController>,
    NodeReport,
    PolicyBundle,
    EvaluationRequest,
) {
    let (dir, c, ns) = setup().await;
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let program = ProgramRef {
        kind: ProofKind::PolicyEvaluation,
        program_sha256: "1".repeat(64),
        verifying_key_sha256: "2".repeat(64),
    };
    let ledger = Arc::new(
        SqliteLedger::open(
            &dir.path().join("ledger.sqlite"),
            program.clone(),
            Arc::new(TestVerifier),
        )
        .unwrap(),
    );
    let mut c = Arc::try_unwrap(c).ok().unwrap();
    c.policy = Some(crate::policy_gateway::PolicyGateway::test_ledger(
        ledger, program,
    ));
    let c = Arc::new(c);
    let mut report = scale_inventory(&ns, false, false);
    report.apps[0]["runtime"] = json!({"executor":"shim","driver":driver});
    report.apps[0]["services"] = if driver == "openshell" {
        json!([])
    } else {
        json!(["control"])
    };
    if driver == "openshell" {
        report.apps[0]["gpus"] = json!(0);
        report.apps[0]["replicas"] = json!(1);
        report.apps[0]["memory_gb"] = json!(0.125);
        report.apps[0]["ready"] = json!(false);
    }
    report.runtime_capabilities = vec![
        "shim-openshell-provisioning-v1".into(),
        "shim-docker-v1".into(),
        "shim-network-v1".into(),
        "shim-management-v1".into(),
    ];
    let execution = ExecutionIdentity {
        namespace: ns.clone(),
        workload: "model".into(),
        allocation_id: "model-0".into(),
        lease_id: "lease".into(),
        generation: 1,
        node_id: "machine".into(),
    };
    report.runtime_instances = serde_json::from_value(
        json!([{"entry_name":"model","ready":driver != "openshell","services":report.apps[0]["services"],"execution":execution}]),
    )
    .unwrap();
    c.report("gpu", "session", report.clone()).await.unwrap();
    assert!(c.guard(&execution).is_ok());
    let mut policy = PolicyBundle { policy:PolicyRef { issuer:"fleet".into(),policy_id:"read".into(),version:1,sha256:String::new() },engine:engine_profile(),modules:[("p.rego".into(),"package trusted\ndefault decision := {\"decision\":\"deny\",\"reason_codes\":[]}\ndecision := {\"decision\":\"allow\",\"reason_codes\":[]} if {input.operation == \"read\"}".into())].into(),data:json!({}) };
    policy.policy.sha256 = policy_digest(&policy).unwrap();
    let time = now() as u64;
    let request = EvaluationRequest {
        execution,
        principal: "alice".into(),
        action_id: "read-file".into(),
        operation: "read".into(),
        resource: "workspace".into(),
        parameters: json!({}),
        context: json!({"tenant":ns}),
        previous_state_root: "0".repeat(64),
        evaluated_at_unix: time,
        validity: Validity {
            not_before: time,
            expires_at: time + 120,
        },
    };
    (dir, c, report, policy, request)
}

async fn policy_setup() -> (
    tempfile::TempDir,
    Arc<WorkloadController>,
    NodeReport,
    PolicyBundle,
    EvaluationRequest,
) {
    policy_setup_with_driver("docker").await
}

async fn admin(c: &Arc<WorkloadController>, method: &str, path: &str, value: Value) -> Response {
    serve(
        c,
        Request::builder()
            .method(method)
            .uri(path)
            .header("authorization", format!("Bearer {ADMIN}"))
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&value).unwrap()))
            .unwrap(),
    )
    .await
}
async fn json_body(r: Response) -> Value {
    let status = r.status();
    let bytes = axum::body::to_bytes(r.into_body(), 2 * 1024 * 1024)
        .await
        .unwrap();
    assert!(
        status.is_success(),
        "{status}: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).unwrap()
}
async fn activate(c: &Arc<WorkloadController>, p: &PolicyBundle, r: &EvaluationRequest) {
    json_body(
        admin(
            c,
            "PUT",
            "/policy/budgets",
            json!({"name":"shared","allowance":2}),
        )
        .await,
    )
    .await;
    json_body(admin(c,"POST","/policy/allocations/activate",json!({"execution":r.execution,"policy":p,"context":r.context,"previous_state_root":r.previous_state_root,"budget":"shared"})).await).await;
    let staged = json_body(
        admin(
            c,
            "POST",
            "/policy/requests",
            json!({"job_id":"proof-job","request":r,"cost":1}),
        )
        .await,
    )
    .await;
    assert_eq!(staged["request"], json!(r));
}
async fn credential(c: &Arc<WorkloadController>, r: &EvaluationRequest, permission: &str) -> Value {
    let response = admin(
        c,
        "POST",
        "/policy/credentials",
        json!({"execution":r.execution,"permission":permission,"expires_at":r.validity.expires_at}),
    )
    .await;
    assert_eq!(response.headers()["cache-control"], "no-store");
    json_body(response).await
}

#[tokio::test]
async fn precreate_policy_requires_a_durable_admitted_command_and_survives_restart() {
    let (_dir, c, mut report, p, mut r) = policy_setup().await;
    report.runtime_instances.clear();
    c.report("gpu", "session", report.clone()).await.unwrap();
    assert!(c.guard(&r.execution).is_err());
    assert_eq!(
        admin(
            &c,
            "POST",
            "/policy/allocations/prepare",
            json!(r.execution)
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
    let command = c
        .command(
            "gpu",
            NodeOperation::SetApp,
            message::set_app(&report.apps[0], false),
        )
        .unwrap();
    r.execution.lease_id = command.clone();
    // A queued intent without resource admission is never authorization.
    assert_eq!(
        admin(
            &c,
            "POST",
            "/policy/allocations/prepare",
            json!(r.execution)
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
    c.dispatch().await.unwrap();
    assert!(c.inner.lock().commands.get(&command).unwrap().admitted);
    for field in 0..6 {
        let mut e = r.execution.clone();
        match field {
            0 => e.namespace.push('x'),
            1 => e.workload.push('x'),
            2 => e.allocation_id = "model-2".into(),
            3 => e.lease_id.push('x'),
            4 => e.generation = 0,
            _ => e.node_id.push('x'),
        }
        assert!(!admin(&c, "POST", "/policy/allocations/prepare", json!(e))
            .await
            .status()
            .is_success());
    }
    json_body(
        admin(
            &c,
            "POST",
            "/policy/allocations/prepare",
            json!(r.execution),
        )
        .await,
    )
    .await;
    json_body(
        admin(
            &c,
            "POST",
            "/policy/allocations/prepare",
            json!(r.execution),
        )
        .await,
    )
    .await;
    assert!(c.guard(&r.execution).is_ok());
    let mut different = r.execution.clone();
    different.generation += 1;
    assert_eq!(
        admin(&c, "POST", "/policy/allocations/prepare", json!(different))
            .await
            .status(),
        StatusCode::CONFLICT
    );
    assert!(c.guard(&different).is_err());
    // Activation/staging/credential issuance work before a runtime exists.
    activate(&c, &p, &r).await;
    let access = credential(&c, &r, "admission_and_execution").await;
    let mut recovered = WorkloadController::new(c.fleet.clone(), c.cluster.clone()).unwrap();
    recovered.policy = c.policy.clone();
    let recovered = Arc::new(recovered);
    assert!(recovered.guard(&r.execution).is_err()); // require a fresh node session
    recovered
        .report("gpu", "restarted", report.clone())
        .await
        .unwrap();
    assert!(recovered.guard(&r.execution).is_ok());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let router = crate::http::router(crate::http::HttpState {
        fleet: recovered.fleet.clone(),
        workloads: recovered.clone(),
    });
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let client = RemoteLedger::connect(
        &address,
        access["token"].as_str().unwrap(),
        r.execution.clone(),
    )
    .await
    .unwrap();
    let decision = RegorusEngine.evaluate(&p, &r).unwrap();
    let grant = client
        .reserve(&decision, Some(&encode_public_values(&decision).unwrap()))
        .await
        .unwrap();
    recovered.inner.lock().commands.update(&command, |c| {
        c.done = true;
    });
    assert!(recovered.guard(&r.execution).is_err()); // a finished command cannot authorize absent compute
    assert!(client
        .consume(&grant, &r.execution, &grant.request_sha256)
        .await
        .is_err());
    report.runtime_instances = serde_json::from_value(json!([{
        "entry_name":"model","ready":false,"services":["control"],"execution":r.execution
    }]))
    .unwrap();
    recovered.report("gpu", "restarted", report).await.unwrap();
    assert!(recovered.guard(&r.execution).is_ok()); // exact reported ownership takes over
    client
        .consume(&grant, &r.execution, &grant.request_sha256)
        .await
        .unwrap();
    assert!(client
        .consume(&grant, &r.execution, &grant.request_sha256)
        .await
        .is_err());
    recovered
        .fleet
        .release_allocation("deployment:gpu-model")
        .await
        .unwrap();
    assert!(recovered.guard(&r.execution).is_err());
    server.abort();
}

#[tokio::test]
async fn prepared_generations_reject_parallel_launches_and_fence_old_reports() {
    let (_dir, c, mut report, _, mut r) = policy_setup().await;
    report.runtime_instances.clear();
    c.report("gpu", "session", report.clone()).await.unwrap();
    let first = c
        .command(
            "gpu",
            NodeOperation::SetApp,
            message::set_app(&report.apps[0], false),
        )
        .unwrap();
    c.dispatch().await.unwrap();
    r.execution.lease_id = first.clone();
    c.prepare_policy_execution(&r.execution).unwrap();
    let old = r.execution.clone();
    // Persist a completed command, as an acknowledged node result does.
    let finished = c
        .inner
        .lock()
        .commands
        .update(&first, |c| {
            c.done = true;
            c.clone()
        })
        .unwrap();
    c.save("commands", &first, &finished).unwrap();
    r.execution.lease_id = c
        .command(
            "gpu",
            NodeOperation::SetApp,
            message::set_app(&report.apps[0], false),
        )
        .unwrap();
    c.dispatch().await.unwrap();
    assert!(c.prepare_policy_execution(&r.execution).is_err()); // generation cannot be reused
    r.execution.generation += 1;
    c.prepare_policy_execution(&r.execution).unwrap();
    assert!(c.guard(&r.execution).is_ok());
    report.runtime_instances = serde_json::from_value(json!([{
        "entry_name":"model","ready":true,"services":["control"],"execution":old
    }]))
    .unwrap();
    c.report("gpu", "session", report).await.unwrap();
    assert!(c.guard(&old).is_err()); // late old heartbeat cannot revive authorization
    assert!(c.guard(&r.execution).is_err()); // nor hide a conflicting live instance
}

#[tokio::test]
async fn precreate_scaling_authorizes_only_the_current_admitted_start() {
    let (_dir, c, mut report, _, mut r) = policy_setup().await;
    report.runtime_instances.clear();
    c.report("gpu", "session", report.clone()).await.unwrap();
    let mut scale = scaling::new_scale(report.apps[0].0.clone(), false);
    c.inner
        .lock()
        .deployments
        .get_mut("gpu-model")
        .unwrap()
        .scale = Some(scale.clone());
    let id = c
        .command(
            "gpu",
            NodeOperation::SetApp,
            message::scale_step(&report.apps[0], false, scale.id(), "start"),
        )
        .unwrap();
    scale.start = Some(id.clone());
    scale.phase = fleet_proto::model::v1::ScalePhase::Starting as i32;
    c.inner
        .lock()
        .deployments
        .get_mut("gpu-model")
        .unwrap()
        .scale = Some(scale.clone());
    c.dispatch().await.unwrap();
    r.execution.lease_id = id;
    c.prepare_policy_execution(&r.execution).unwrap();
    assert!(c.guard(&r.execution).is_ok());
    for changed in 0..4 {
        let mut wrong = scale.clone();
        match changed {
            0 => wrong.id.push('x'),
            1 => wrong.start = Some("another-command".into()),
            2 => wrong.phase = fleet_proto::model::v1::ScalePhase::Draining as i32,
            _ => wrong.phase = fleet_proto::model::v1::ScalePhase::Failed as i32,
        }
        c.inner
            .lock()
            .deployments
            .get_mut("gpu-model")
            .unwrap()
            .scale = Some(wrong);
        assert!(c.guard(&r.execution).is_err());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn policy_gateway_shares_listener_and_fences_consumption_with_live_fleet_state() {
    let (_dir, c, report, p, r) = policy_setup().await;
    activate(&c, &p, &r).await;
    let admission = credential(&c, &r, "admission").await;
    let execution = credential(&c, &r, "execution").await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let router = crate::http::router(crate::http::HttpState {
        fleet: c.fleet.clone(),
        workloads: c.clone(),
    });
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let a = RemoteLedger::connect(
        &address,
        admission["token"].as_str().unwrap(),
        r.execution.clone(),
    )
    .await
    .unwrap();
    let e = RemoteLedger::connect(
        &address,
        execution["token"].as_str().unwrap(),
        r.execution.clone(),
    )
    .await
    .unwrap();
    let decision = RegorusEngine.evaluate(&p, &r).unwrap();
    let proof = encode_public_values(&decision).unwrap();
    assert!(e.reserve(&decision, Some(&proof)).await.is_err());
    assert!(a.reserve(&decision, Some(b"forged")).await.is_err());
    let grant = a.reserve(&decision, Some(&proof)).await.unwrap();
    assert!(a
        .consume(&grant, &r.execution, &grant.request_sha256)
        .await
        .is_err());
    // Every live fence is checked again after successful proof verification.
    c.fleet
        .suspend_namespace(&r.execution.namespace, true)
        .await
        .unwrap();
    assert!(e
        .consume(&grant, &r.execution, &grant.request_sha256)
        .await
        .is_err());
    c.fleet
        .suspend_namespace(&r.execution.namespace, false)
        .await
        .unwrap();
    c.inner.lock().workers.get_mut("gpu").unwrap().seen = now() - 120.0;
    assert!(e
        .consume(&grant, &r.execution, &grant.request_sha256)
        .await
        .is_err());
    c.report("gpu", "session", report.clone()).await.unwrap();
    c.end_session("gpu", "session");
    assert!(e
        .consume(&grant, &r.execution, &grant.request_sha256)
        .await
        .is_err());
    c.report("gpu", "replacement", report.clone())
        .await
        .unwrap();
    // Rotating credentials must not reset the proof budget or consume the grant.
    json_body(
        admin(
            &c,
            "DELETE",
            &format!("/policy/credentials/{}", execution["id"].as_str().unwrap()),
            json!({}),
        )
        .await,
    )
    .await;
    assert!(e
        .consume(&grant, &r.execution, &grant.request_sha256)
        .await
        .is_err());
    let execution = credential(&c, &r, "execution").await;
    let e = RemoteLedger::connect(
        &address,
        execution["token"].as_str().unwrap(),
        r.execution.clone(),
    )
    .await
    .unwrap();
    e.consume(&grant, &r.execution, &grant.request_sha256)
        .await
        .unwrap();
    assert!(e
        .consume(&grant, &r.execution, &grant.request_sha256)
        .await
        .is_err());
    c.fleet
        .release_job(&r.execution.namespace, "deployment:gpu-model")
        .await
        .unwrap();
    assert!(c.guard(&r.execution).is_err());
    server.abort();
}

#[tokio::test]
async fn policy_administration_rejects_tenants_forged_allocations_and_retired_generations() {
    let (_dir, c, report, p, r) = policy_setup().await;
    let tenant = user_token(&c, &r.execution.namespace, 11, "scope=default", None, true).await;
    for path in [
        "/policy/program",
        "/policy/requests/job",
        "/policy/credentials",
    ] {
        let response = serve(
            &c,
            Request::builder()
                .uri(path)
                .header("authorization", format!("Bearer {tenant}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
    for field in 0..6 {
        let mut wrong = r.execution.clone();
        match field {
            0 => wrong.namespace.push('x'),
            1 => wrong.workload.push('x'),
            2 => wrong.allocation_id.push('x'),
            3 => wrong.lease_id.push('x'),
            4 => wrong.generation += 1,
            _ => wrong.node_id.push('x'),
        }
        assert!(c.guard(&wrong).is_err());
    }
    activate(&c, &p, &r).await;
    let mut next = report.clone();
    next.runtime_instances[0].execution.generation += 1;
    next.runtime_instances[0].execution.lease_id = "next".into();
    c.report("gpu", "session", next).await.unwrap();
    assert!(c.guard(&r.execution).is_err());
    let response = admin(&c,"POST","/policy/credentials",json!({"execution":r.execution,"permission":"execution","expires_at":r.validity.expires_at})).await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    // Historical revocation is still possible after replacement.
    json_body(admin(&c, "POST", "/policy/allocations/revoke", json!(r.execution)).await).await;
    c.report("gpu", "session", report).await.unwrap();
    let response = admin(&c,"POST","/policy/allocations/activate",json!({"execution":r.execution,"policy":p,"context":r.context,"previous_state_root":r.previous_state_root,"budget":"shared"})).await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

fn provisioning(request: &EvaluationRequest) -> trusted_shim::provisioning::ProvisionRequest {
    use prost_native::Message;
    use trusted_shim::openshell_gateway::{
        protocol::compute::v1::{
            DriverResourceRequirements, DriverSandbox, DriverSandboxSpec, DriverSandboxTemplate,
        },
        GatewayLaunch, SandboxPolicy,
    };
    let mut a = request.clone();
    a.operation = "runtime.create".into();
    let launch: trusted_shim::runtime::LaunchRequest=serde_json::from_value(json!({
        "execution":a.execution,"driver":"openshell","image":"runner:1","command":[],"environment":{},
        "limits":{"cpu_millicores":2000,"memory_bytes":134217728},
        "policy":{"issuer":"fleet","policy_id":"create","version":1,"sha256":"a".repeat(64)},
        "trust":{"attestation":{"mode":"skip_non_tee"},"policy_proof":"not_required","attestation_proof":"not_required"},"services":[]
    })).unwrap();
    let policy = SandboxPolicy::default();
    let binding = GatewayLaunch::new(launch.clone(), policy.clone()).unwrap();
    let sandbox = DriverSandbox {
        id: "sandbox-id".into(),
        name: binding.request().name,
        workspace: launch.execution.namespace.clone(),
        spec: Some(DriverSandboxSpec {
            command: launch.command.clone(),
            environment: launch.environment.clone().into_iter().collect(),
            policy: Some(policy),
            sandbox_token: "synthetic-native-token".into(),
            launch_authentication: b"synthetic-native-credentials".to_vec(),
            template: Some(DriverSandboxTemplate {
                image: launch.image.clone(),
                resources: Some(DriverResourceRequirements {
                    cpu_limit: "2000m".into(),
                    memory_limit: "134217728".into(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    let bytes = sandbox.encode_to_vec();
    let native = binding.bind(&bytes).unwrap();
    a.resource = native.authorization_resource();
    a.parameters = native.authorization_parameters();
    let g = trusted_shim::rail::ExecutionGrant {
        id: "grant".into(),
        credential: vec![1],
        execution: a.execution.clone(),
        request_sha256: request_digest(&a).unwrap(),
        validity: a.validity.clone(),
    };
    trusted_shim::provisioning::ProvisionRequest::new(
        launch,
        &bytes,
        binding.policy_sha256().into(),
        a,
        g,
        "b".repeat(64),
    )
    .unwrap()
}
async fn delivery_node(
    c: &Arc<WorkloadController>,
    ticket: &str,
    session: &str,
    operation: &str,
    body: Value,
) -> Response {
    serve(
        c,
        Request::builder()
            .method("POST")
            .uri(format!("/internal/runtime-deliveries/{ticket}/{operation}"))
            .header("authorization", "Bearer cluster")
            .header("x-fleet-worker", "gpu")
            .header("x-fleet-session", session)
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap(),
    )
    .await
}
#[tokio::test]
async fn private_delivery_checks_manifest_session_quota_and_durable_single_claim() {
    let (dir, c, report, _, r) = policy_setup_with_driver("openshell").await;
    let p = provisioning(&r);
    let payload = serde_json::to_value(&p).unwrap();
    let ticket = "d".repeat(64);
    let path = format!("/policy/runtime-deliveries/{ticket}");
    let denied = serve(
        &c,
        Request::builder()
            .method("PUT")
            .uri(&path)
            .body(Body::from(serde_json::to_vec(&payload).unwrap()))
            .unwrap(),
    )
    .await;
    assert_eq!(denied.status(), 401);
    for (field, value) in [
        ("image", json!("unapproved")),
        (
            "limits",
            json!({"cpu_millicores":100000,"memory_bytes":134217728}),
        ),
        ("environment", json!({"SECRET":"unexpected"})),
    ] {
        let mut bad = payload.clone();
        bad["request"][field] = value;
        assert_eq!(admin(&c, "PUT", &path, bad).await.status(), 403);
    }
    // Tampering with native policy/bytes must fail centrally, before any
    // one-shot delivery ticket can become visible to the node.
    for (field, value) in [
        ("native_policy_sha256", json!("f".repeat(64))),
        ("native_create_base64", json!("bm90LXByb3RvYnVm")),
    ] {
        let mut bad = payload.clone();
        bad[field] = value;
        assert_eq!(admin(&c, "PUT", &path, bad).await.status(), 403);
        assert!(c.deliveries.offers("gpu").is_empty());
        assert_eq!(admin(&c, "GET", &path, json!({})).await.status(), 403);
    }
    let staged = json_body(admin(&c, "PUT", &path, payload.clone()).await).await;
    assert_eq!(staged["state"], "queued");
    assert_eq!(c.deliveries.offers("gpu").len(), 1);
    assert_eq!(
        delivery_node(&c, &ticket, "wrong-session", "claim", json!({}))
            .await
            .status(),
        403
    );
    c.drain_worker("gpu", true);
    assert_eq!(
        delivery_node(&c, &ticket, "session", "claim", json!({}))
            .await
            .status(),
        403
    );
    c.drain_worker("gpu", false);
    let mut newer = report.clone();
    newer.runtime_instances[0].execution.generation += 1;
    c.report("gpu", "session", newer).await.unwrap();
    assert_eq!(
        delivery_node(&c, &ticket, "session", "claim", json!({}))
            .await
            .status(),
        403
    );
    c.report("gpu", "session", report.clone()).await.unwrap();
    c.fleet
        .suspend_namespace(&r.execution.namespace, true)
        .await
        .unwrap();
    assert_eq!(
        delivery_node(&c, &ticket, "session", "claim", json!({}))
            .await
            .status(),
        403
    );
    c.fleet
        .suspend_namespace(&r.execution.namespace, false)
        .await
        .unwrap();
    let claimed = delivery_node(&c, &ticket, "session", "claim", json!({})).await;
    assert_eq!(claimed.headers()["cache-control"], "no-store");
    let bytes = axum::body::to_bytes(claimed.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap(), payload);
    assert_eq!(
        hex::encode(sha2::Sha256::digest(&bytes)),
        staged["offer"]["payload_sha256"]
    );
    assert!(c.deliveries.offers("gpu").is_empty());
    assert!(!delivery_node(&c, &ticket, "session", "claim", json!({}))
        .await
        .status()
        .is_success());
    assert_eq!(
        json_body(admin(&c, "PUT", &path, payload).await).await["state"],
        "dispatched"
    );
    assert_eq!(
        delivery_node(&c, &ticket, "replacement", "ack", json!({"accepted":true}))
            .await
            .status(),
        403
    );
    assert_eq!(
        json_body(delivery_node(&c, &ticket, "session", "ack", json!({"accepted":true})).await)
            .await["state"],
        "acknowledged"
    );
    // The app remains unready and has no service route after transport acknowledgement.
    assert_eq!(
        c.app_resources(&r.execution.namespace, "model", None)["runtime_instances"],
        json!([])
    );
    for e in std::fs::read_dir(dir.path().join("workloads/runtime-deliveries")).unwrap() {
        let text = std::fs::read_to_string(e.unwrap().path()).unwrap();
        assert!(
            !text.contains(&p.native_create_base64)
                && !text.contains(&p.ledger_token)
                && !text.contains(&p.grant_base64)
        );
    }
}
