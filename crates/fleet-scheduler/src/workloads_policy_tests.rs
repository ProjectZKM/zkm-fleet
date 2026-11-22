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

async fn policy_setup() -> (
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
    report.apps[0]["runtime"] = json!({"executor":"shim","driver":"docker"});
    report.apps[0]["services"] = json!(["control"]);
    report.runtime_capabilities = vec![
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
        json!([{"entry_name":"model","ready":true,"services":["control"],"execution":execution}]),
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
