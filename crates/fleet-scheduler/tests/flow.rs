//! The scheduler end to end, with fake prover nodes: dispatch, long-poll, public
//! values, anchoring, exclusive reservation, retry on another node, and the tokens.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use sha2::Digest;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use fleet_proto::fleet::v1::{
    control_service_client::ControlServiceClient,
    Assignment, Completion, FetchInputsRequest, HeartbeatRequest, NodeInfo, ProveRequest, ProveResponse,
    RegisterRequest, PublishOutputRequest,
};
use fleet_proto::stage::v1::{
    stage_service_client::StageServiceClient, GenerateProofRequest, GetStatusRequest, Status as S,
    Step,
};
use fleet_scheduler::config::Config;

const TOKEN: &str = "0123456789abcdef0123456789abcdef-cluster";
const ADMIN: &str = "0123456789abcdef0123456789abcdef-admin!";

fn free_port() -> u16 {
    // A listener is dropped before serve() binds it. Keep parallel tests from
    // selecting a port already handed to another scheduler in this process.
    static PORTS: std::sync::OnceLock<std::sync::Mutex<std::collections::HashSet<u16>>> = std::sync::OnceLock::new();
    let mut used = PORTS.get_or_init(Default::default).lock().unwrap();
    loop {
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        if used.insert(port) { return port; }
    }
}

struct FakeNode {
    name: String,
    calls: Arc<AtomicUsize>,
    jobs: Arc<Mutex<Vec<String>>>,
    fail_first: bool,
    delay_ms: u64,
    failure_code: tonic::Code,
}

impl FakeNode {
    async fn prove(&self, req: Request<ProveRequest>) -> Result<Response<ProveResponse>, Status> {
        let auth = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        if auth != format!("Bearer {TOKEN}") {
            return Err(Status::unauthenticated("bad token"));
        }
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        let r = req.into_inner();
        self.jobs.lock().push(r.job_id.clone());
        if self.fail_first && n == 0 {
            return Err(Status::new(self.failure_code,"gpu fell over or response was lost"));
        }
        tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
        Ok(Response::new(ProveResponse {
            proof_json: format!(
                "{{\"node\":\"{}\",\"stdin\":{}}}",
                self.name,
                r.stdin_buffer.len()
            )
            .into_bytes(),
            public_values: vec![7, 7, 7],
            cycles: 1234,
            prove_ms: 100,
            wall_ms: 150,
            zkm_version: "v2.0.0".into(),
        }))
    }


}

struct Harness {
    stage: StageServiceClient<Channel>,
    http: String,
    control_port: u16,
    _dir: tempfile::TempDir,
}

async fn start() -> Harness {
    start_with("").await
}

/// A scheduler with extra top-level config lines.
async fn start_with(extra: &str) -> Harness {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("cluster.token"), TOKEN).unwrap();
    std::fs::write(dir.path().join("admin.token"), ADMIN).unwrap();
    let (sp, cp, hp) = (free_port(), free_port(), free_port());
    let yaml = format!(
        r#"
stage_addr: 127.0.0.1:{sp}
control_addr: 127.0.0.1:{cp}
http_addr: 127.0.0.1:{hp}
public_url: http://127.0.0.1:{hp}
data_dir: {d}/data
long_poll_ms: 3000
heartbeat_secs: 1
max_attempts: 2
default_tenant: eth
cluster_token_file: {d}/cluster.token
admin_token_file: {d}/admin.token
{extra}
tenants:
  - name: eth
    class: latency
    anchor: exclusive
    addresses: [{operator_address}]
  - name: goat
    class: batch
    anchor: shared
"#,
        d = dir.path().display(),
        operator_address = operator_address()
    );
    let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
    tokio::spawn(async move { fleet_scheduler::serve(cfg).await.unwrap() });
    let url = format!("http://127.0.0.1:{sp}");
    let stage = loop {
        if let Ok(channel) = tonic::transport::Endpoint::from_shared(url.clone()).unwrap()
            .timeout(Duration::from_secs(10)).connect().await {
            break StageServiceClient::new(channel);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let h = Harness {
        stage,
        http: format!("http://127.0.0.1:{hp}"),
        control_port: cp,
        _dir: dir,
    };
    // a job runs only for a registered app: the tests' programs, for the default tenant
    // (a program registered for another tenant too would keep its ELF when eth's app goes)
    for elf in TEST_ELFS {
        register(&h, "eth", elf);
    }
    h
}

/// The programs the tests submit (registered for tenant eth by `start_with`).
const TEST_ELFS: &[&[u8]] =
    &[b"elf-one", b"something else", b"elf", b"elf-r", b"eth-guest", b"elf-c", b"elf-m", b"elf-f", b"elf-d", b"elf-e"];

/// Registers the tenant's program as an app (the admin API, as fleetctl does).
fn register(h: &Harness, tenant: &str, elf: &[u8]) -> String {
    let id = fleet_scheduler::blobs::elf_id_of(elf);
    let url = format!("{}/admin/apps?namespace=system&tenant={tenant}&elf_id={id}", h.http);
    for _ in 0..100 {
        let code = admin_curl(h, &["-o", "/dev/null", "-w", "%{http_code}", "-X", "POST", &url]);
        if code != "000" {
            assert_eq!(code, "200", "registering {tenant}/{id}");
            return id;
        }
        std::thread::sleep(Duration::from_millis(50)); // the http port is not up yet
    }
    panic!("the admin API never answered");
}

async fn add_node(
    h: &Harness,
    name: &str,
    gpus: u32,
    fail_first: bool,
) -> (Arc<AtomicUsize>, Arc<Mutex<Vec<String>>>) {
    add_node_delayed(h, name, gpus, fail_first, 150).await
}

async fn add_node_for_app(h: &Harness, name: &str, gpus: u32, fail_first: bool,
    namespace: &str, tenant: &str, elf: &[u8])
    -> (Arc<AtomicUsize>, Arc<Mutex<Vec<String>>>) {
    add_node_app(h, name, gpus, fail_first, 150, tonic::Code::Internal, namespace, tenant, elf).await
}

async fn add_node_delayed(h: &Harness, name: &str, gpus: u32, fail_first: bool, delay_ms: u64)
    -> (Arc<AtomicUsize>, Arc<Mutex<Vec<String>>>) {
    add_node_error(h,name,gpus,fail_first,delay_ms,tonic::Code::Internal).await
}

async fn add_node_error(h: &Harness,name: &str,gpus: u32,fail_first: bool,delay_ms: u64,failure_code: tonic::Code)
    -> (Arc<AtomicUsize>, Arc<Mutex<Vec<String>>>) {
    add_node_app(h, name, gpus, fail_first, delay_ms, failure_code, "system", "eth", b"elf-one").await
}

async fn add_node_app(h: &Harness, name: &str, gpus: u32, fail_first: bool, delay_ms: u64,
    failure_code: tonic::Code, namespace: &str, tenant: &str, elf: &[u8])
    -> (Arc<AtomicUsize>, Arc<Mutex<Vec<String>>>) {
    let (calls, jobs, _) = add_node_app_session(h, name, gpus, fail_first, delay_ms, failure_code, namespace, tenant, elf).await;
    (calls, jobs)
}

async fn add_node_app_session(h: &Harness, name: &str, gpus: u32, fail_first: bool, delay_ms: u64,
    failure_code: tonic::Code, namespace: &str, tenant: &str, elf: &[u8])
    -> (Arc<AtomicUsize>, Arc<Mutex<Vec<String>>>, String) {
    let calls = Arc::new(AtomicUsize::new(0));
    let jobs = Arc::new(Mutex::new(vec![]));
    let svc = FakeNode {
        name: name.into(),
        calls: calls.clone(),
        jobs: jobs.clone(),
        fail_first,
        delay_ms,
        failure_code,
    };
    let channel = Channel::from_shared(format!("http://127.0.0.1:{}", h.control_port))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let auth = |mut r: Request<()>| {
        r.metadata_mut()
            .insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
        Ok(r)
    };
    let mut ctl = ControlServiceClient::with_interceptor(channel.clone(), auth);
    let namespace = if namespace == "system" {
        "00000000000000000000000000000000".to_string()
    } else {
        let body = admin_curl(h, &[&format!("{}/admin/namespaces/{namespace}", h.http)]);
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["id"].as_str().unwrap().to_string()
    };
    let elf_id = fleet_scheduler::blobs::elf_id_of(elf);
    let id = ctl
        .register(RegisterRequest {
            node: Some(NodeInfo {
                name: name.into(),
                backend: "cuda".into(),
                gpus,
                cpus: 8,
                zkm_version: "v2.0.0".into(),
                image: String::new(),
                machine: "test".into(),
                control_version: 1,
                app_namespace: namespace,
                app_tenant: tenant.into(),
                app_elf_id: elf_id,
            }),
        })
        .await
        .unwrap()
        .into_inner()
        .node_id;
    let session_id = id.clone();
    tokio::spawn(async move {
        let mut active: Option<(Assignment, tokio::task::JoinHandle<Result<Response<ProveResponse>, Status>>)> = None;
        let mut completed: Option<Completion> = None;
        let svc = Arc::new(svc);
        loop {
            if active.as_ref().is_some_and(|(_, t)| t.is_finished()) {
                let (assignment, task) = active.take().unwrap();
                let result = task.await.unwrap();
                completed = Some(match result {
                    Ok(result) => {
                        let result = result.into_inner();
                        let outputs = if let Some(inputs) = &assignment.inputs {
                            let root = std::path::Path::new(inputs.output_root.strip_prefix("file://").unwrap());
                            let output = fleet_ziren_node::output::Output::new(root.into()).unwrap();
                            output.publish(&assignment, &result).unwrap();
                            Some(output.references(&assignment).unwrap())
                        } else {
                            ctl.publish_output(PublishOutputRequest {
                                node_id:id.clone(), assignment:Some(assignment.clone()), result:Some(result.clone()),
                            }).await.unwrap(); None
                        };
                        Completion { assignment: Some(assignment), outputs, cycles: result.cycles, prove_ms: result.prove_ms, ..Default::default() }
                    }
                    Err(e) => Completion { assignment: Some(assignment), error_code: e.code() as i32, error: e.message().into(), ..Default::default() },
                });
            }
            let running_job = active.as_ref().map(|(a, _)| a.job_id.clone())
                .or_else(|| completed.as_ref().and_then(|c| c.assignment.as_ref().map(|a| a.job_id.clone()))).unwrap_or_default();
            if let Ok(response) = ctl.heartbeat(HeartbeatRequest { node_id: id.clone(), ready: true,
                busy: !running_job.is_empty(), running_job, completion: completed.clone(), ..Default::default() }).await {
                let response = response.into_inner();
                if completed.as_ref().is_some_and(|c| c.assignment.as_ref().unwrap().lease == response.completed_lease) { completed = None; }
                if active.is_none() && completed.is_none() {
                    if let Some(assignment) = response.assignment {
                        let inputs = if let Some(refs) = &assignment.inputs {
                            use prost::Message;
                            let root=std::path::Path::new(refs.output_root.strip_prefix("file://").unwrap()).parent().unwrap();
                            let bytes=fleet_proto::artifacts::read(refs.input.as_ref().unwrap(),root,fleet_proto::artifacts::MAX_INPUT).unwrap();
                            let mut request=ProveRequest::decode(bytes.as_slice()).unwrap();
                            request.job_id=assignment.job_id.clone();
                            request.elf=fleet_proto::artifacts::read(refs.elf.as_ref().unwrap(),root,fleet_proto::artifacts::MAX_INPUT).unwrap(); request
                        } else { ctl.fetch_inputs(FetchInputsRequest { node_id: id.clone(), assignment: Some(assignment.clone()), include_elf: true }).await.unwrap().into_inner() };
                        let worker = svc.clone();
                        let mut req = Request::new(inputs);
                        req.metadata_mut().insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
                        active = Some((assignment, tokio::spawn(async move { worker.prove(req).await })));
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
    tokio::time::sleep(Duration::from_millis(700)).await;
    (calls, jobs, session_id)
}

fn request(id: &str, elf: &[u8]) -> GenerateProofRequest {
    let mut req = GenerateProofRequest {
        proof_id: id.into(),
        elf_data: elf.to_vec(),
        private_input_stream: bincode::serialize(&vec![vec![1u8, 2, 3], vec![4]]).unwrap(),
        target_step: Some(Step::InAgg as i32),
        ..Default::default()
    };
    sign_request(&mut req, &operator_key());
    req
}

fn operator_key() -> k256::ecdsa::SigningKey {
    k256::ecdsa::SigningKey::from_slice(&[7; 32]).unwrap()
}

fn operator_address() -> String {
    format!("{:x}", alloy_primitives::Address::from_public_key(operator_key().verifying_key()))
}

async fn wait_done(h: &mut Harness, id: &str) -> fleet_proto::stage::v1::GetStatusResponse {
    for _ in 0..40 {
        let r = h
            .stage
            .get_status(GetStatusRequest {
                proof_id: id.into(),
            })
            .await
            .unwrap()
            .into_inner();
        if r.status != S::Computing as i32 {
            return r;
        }
    }
    panic!("job {id} never finished");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_job_runs_end_to_end_and_its_public_values_are_served() {
    let mut h = start().await;
    add_node(&h, "gpu-a", 2, false).await;
    let r = h
        .stage
        .generate_proof(request("job-1", b"elf-one"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.status, S::Computing as i32);
    let s = wait_done(&mut h, "job-1").await;
    assert_eq!(s.status, S::Success as i32);
    assert_eq!(
        String::from_utf8(s.proof_with_public_inputs).unwrap(),
        r#"{"node":"gpu-a","stdin":2}"#
    );
    assert_eq!(s.total_steps, 1234);
    assert_eq!(s.proving_time, 100);
    for path in ["/admin/jobs/job-1", "/admin/jobs"] {
        let view:serde_json::Value=serde_json::from_str(&admin_curl(&h,&[&format!("{}{path}",h.http)])).unwrap();
        let job=if view.is_array() {&view[0]} else {&view};
        assert_eq!(job["node"],"test","HTTP exposes the machine, not the proof replica");
        assert_eq!(job["diagnostics"]["execution_replica"],"gpu-a");
    }
    let pv = std::process::Command::new("curl")
        .args(["-sf", &s.public_values_url])
        .output()
        .unwrap();
    assert_eq!(pv.stdout, vec![7, 7, 7]);
    let (keyless, _) = s.public_values_url.split_once('?').unwrap();
    let wrong = format!("{keyless}?key={}", "0".repeat(32));
    for url in [keyless, wrong.as_str()] {
        let st = std::process::Command::new("curl").args(["-s", "-o", "/dev/null", "-w", "%{http_code}", url]).output().unwrap();
        assert_eq!(String::from_utf8(st.stdout).unwrap(), "404", "{url} opened without the proof's key");
    }
    let again = h
        .stage
        .generate_proof(request("job-1", b"something else"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        again.status,
        S::Computing as i32,
        "a known proof id is idempotent"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rejections_surface_through_get_status() {
    let mut h = start().await;
    let mut req = request("job-x", b"");
    req.elf_id = Some("ab".repeat(32));
    h.stage.generate_proof(req).await.unwrap();
    let s = wait_done(&mut h, "job-x").await;
    assert_eq!(s.status, S::InvalidParameter as i32);
    let mut bad = request("job-y", b"elf");
    bad.target_step = Some(Step::InSplit as i32);
    h.stage.generate_proof(bad).await.unwrap();
    assert_eq!(
        wait_done(&mut h, "job-y").await.status,
        S::InvalidParameter as i32
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_attempt_moves_to_another_node() {
    let mut h = start().await;
    let (a_calls, _) = add_node_for_app(&h, "gpu-a", 4, true, "system", "eth", b"elf-r").await;
    let (b_calls, _) = add_node_for_app(&h, "gpu-b", 1, false, "system", "eth", b"elf-r").await;
    h.stage
        .generate_proof(request("job-r", b"elf-r"))
        .await
        .unwrap();
    let s = wait_done(&mut h, "job-r").await;
    assert_eq!(s.status, S::Success as i32);
    assert!(String::from_utf8(s.proof_with_public_inputs)
        .unwrap()
        .contains("gpu-b"));
    assert_eq!(a_calls.load(Ordering::SeqCst), 1);
    assert_eq!(b_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_exclusive_anchor_keeps_the_program_and_keeps_others_off_the_node() {
    let mut h = start().await;
    let (_, a_jobs) = add_node_for_app(&h, "gpu-a", 4, false, "system", "eth", b"eth-guest").await;
    let (_, b_jobs) = add_node_for_app(&h, "gpu-b", 1, false, "system", "eth", b"eth-guest").await;
    for i in 0..3 {
        let id = format!("eth-{i}");
        h.stage
            .generate_proof(request(&id, b"eth-guest"))
            .await
            .unwrap();
        assert_eq!(wait_done(&mut h, &id).await.status, S::Success as i32);
    }
    assert_eq!(
        a_jobs.lock().len(),
        3,
        "latency tenant anchored on the larger node"
    );
    let st = std::process::Command::new("curl")
        .args([
            "-sf",
            "-H",
            &format!("authorization: Bearer {ADMIN}"),
            &format!("{}/admin/status", h.http),
        ])
        .output()
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&st.stdout).unwrap();
    assert!(v["nodes"].as_array().unwrap().is_empty(), "proof replicas do not create machine node records");
    assert!(v["anchors"].as_array().unwrap().iter().any(|a|
        a[1] == "eth" && a[2] == fleet_scheduler::blobs::elf_id_of(b"eth-guest") && a[3] == "gpu-a"));
    let unauth = std::process::Command::new("curl")
        .args([
            "-s",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            &format!("{}/admin/status", h.http),
        ])
        .output()
        .unwrap();
    assert_eq!(String::from_utf8(unauth.stdout).unwrap(), "401");
    let _ = b_jobs;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_control_api_refuses_a_node_without_the_cluster_token() {
    let h = start().await;
    let mut ctl = ControlServiceClient::connect(format!("http://127.0.0.1:{}", h.control_port))
        .await
        .unwrap();
    let err = ctl
        .register(RegisterRequest {
            node: Some(NodeInfo {
                name: "rogue".into(),
                ..Default::default()
            }),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    let err = ctl.fetch_inputs(FetchInputsRequest::default()).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    let err = ctl.heartbeat(HeartbeatRequest::default()).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}

const GOAT_ADDR: &str = "0x2222222222222222222222222222222222222222";

fn token(h: &Harness, tenant: &str, address: &str) {
    let id = address.trim_start_matches("0x");
    let id = &id[..32];
    let out = std::process::Command::new("curl")
        .args(["-sf", "-X", "POST", "-H", &format!("authorization: Bearer {ADMIN}"), "-H", "content-type: application/json"])
        .args(["-d", &format!(r#"{{"id":"{id}","tenant":"{tenant}","address":"{address}"}}"#), &format!("{}/admin/tokens", h.http)])
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
}

fn with_bearer<T>(msg: T, token: &str) -> Request<T> {
    let mut r = Request::new(msg);
    r.metadata_mut().insert("authorization", format!("Bearer {token}").parse().unwrap());
    r
}

#[tokio::test(flavor = "multi_thread")]
async fn a_forged_certificate_header_does_not_authenticate_a_proof_caller() {
    let mut h = start().await;
    let elf = fleet_scheduler::blobs::elf_id_of(b"elf-f");
    admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d",
        r#"{"name":"alice"}"#, &format!("{}/admin/namespaces", h.http)]);
    let (_, _, bearer) = namespace_token(&h, "alice", 91, &elf);
    add_node_for_app(&h, "gpu-a", 2, false, "alice", "eth", b"elf-f").await;
    let signed = signed_request("job-f", b"elf-f", &k256::ecdsa::SigningKey::from_slice(&[91; 32]).unwrap());
    let missing_bearer = h.stage.generate_proof(Request::new(signed.clone())).await.unwrap().into_inner();
    assert_eq!(missing_bearer.status, S::InvalidParameter as i32);
    assert!(missing_bearer.error_message.contains("bearer token is required"));
    h.stage.generate_proof(with_bearer(signed, &bearer)).await.unwrap();
    wait_done(&mut h, "job-f").await;
    let mut forged = Request::new(GetStatusRequest { proof_id: "job-f".into() });
    forged.metadata_mut().insert("x-forwarded-client-cert", format!(r#"Hash=ab12;Subject="CN={GOAT_ADDR},O=zkm-fleet""#).parse().unwrap());
    let s = h.stage.get_status(forged).await.unwrap().into_inner();
    assert_eq!(s.status, S::Unspecified as i32);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_app_is_deleted_with_its_jobs_once_none_is_running() {
    let mut h = start().await;
    add_node_for_app(&h, "gpu-a", 2, false, "system", "eth", b"elf-d").await;
    h.stage.generate_proof(request("job-d", b"elf-d")).await.unwrap();
    assert_eq!(wait_done(&mut h, "job-d").await.status, S::Success as i32);
    let app = proof_app(&h, "eth");
    let elf = app["elf_id"].as_str().unwrap();
    assert_eq!(app["jobs"], 1);
    let gone = admin_curl(&h, &["-X", "DELETE", &format!("{}/admin/apps?namespace=system&tenant=eth&elf_id={elf}", h.http)]);
    let gone: serde_json::Value = serde_json::from_str(&gone).unwrap();
    assert_eq!((gone["jobs_deleted"].as_u64(), gone["elf_removed"].as_bool()), (Some(1), Some(true)));
    let s = h.stage.get_status(GetStatusRequest { proof_id: "job-d".into() }).await.unwrap().into_inner();
    assert_eq!(s.status, S::Unspecified as i32, "the deleted app's job is gone");
    let again = admin_curl(&h, &["-o", "/dev/null", "-w", "%{http_code}", "-X", "DELETE", &format!("{}/admin/apps?namespace=system&tenant=eth&elf_id={elf}", h.http)]);
    assert_eq!(again, "404");
}

/// The admin API through curl (what fleetctl does), as text.
fn admin_curl(_h: &Harness, args: &[&str]) -> String {
    let out = std::process::Command::new("curl")
        .args(["-s", "--max-time", "10", "-H", &format!("authorization: Bearer {ADMIN}")])
        .args(args)
        .output()
        .unwrap();
    String::from_utf8(out.stdout).unwrap()
}

/// A tenant's (only) proof app, from the list.
fn proof_app(h: &Harness, tenant: &str) -> serde_json::Value {
    let apps: serde_json::Value = serde_json::from_str(&admin_curl(h, &[&format!("{}/admin/apps", h.http)])).unwrap();
    apps.as_array().unwrap().iter().find(|a| a["tenant"] == tenant).unwrap().clone()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_job_runs_only_for_a_registered_app() {
    let mut h = start().await;
    add_node_for_app(&h, "gpu-a", 2, false, "system", "eth", b"unregistered").await;
    let id = fleet_scheduler::blobs::elf_id_of(b"unregistered");
    h.stage.generate_proof(request("job-u1", b"unregistered")).await.unwrap();
    assert_eq!(wait_done(&mut h, "job-u1").await.status, S::InvalidParameter as i32, "refused: not an app");
    let apps = admin_curl(&h, &[&format!("{}/admin/apps", h.http)]);
    assert!(!apps.contains(&id), "the refusal registered nothing");
    assert_eq!(register(&h, "eth", b"unregistered"), id);
    let app: serde_json::Value = serde_json::from_str(&admin_curl(&h, &[&format!("{}/admin/apps", h.http)])).unwrap();
    let app = app.as_array().unwrap().iter().find(|a| a["elf_id"] == id.as_str()).unwrap().clone();
    assert_eq!((app["tenant"].as_str(), app["jobs"].as_u64(), app["disabled"].as_bool()), (Some("eth"), Some(0), Some(false)), "listed before its first job");
    h.stage.generate_proof(request("job-u2", b"unregistered")).await.unwrap();
    assert_eq!(wait_done(&mut h, "job-u2").await.status, S::Success as i32, "runs once registered");
    let code = |args: &[&str]| admin_curl(&h, &[&["-o", "/dev/null", "-w", "%{http_code}", "-X", "POST"][..], args].concat());
    assert_eq!(code(&[&format!("{}/admin/apps?namespace=system&tenant=eth&elf_id={id}", h.http)]), "409", "registered twice");
    assert_eq!(code(&[&format!("{}/admin/apps?namespace=system&tenant=nobody&elf_id={id}", h.http)]), "404", "no such tenant");
    assert_eq!(code(&[&format!("{}/admin/apps?namespace=system&tenant=eth&elf_id=abc", h.http)]), "400", "not a program id");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_disabled_app_refuses_new_jobs_until_it_is_enabled() {
    let mut h = start().await;
    add_node_for_app(&h, "gpu-a", 2, false, "system", "eth", b"elf-e").await;
    h.stage.generate_proof(request("job-e1", b"elf-e")).await.unwrap();
    assert_eq!(wait_done(&mut h, "job-e1").await.status, S::Success as i32);
    let elf = proof_app(&h, "eth")["elf_id"].as_str().unwrap().to_string();
    let key = format!("{}/admin/apps/disabled?namespace=system&tenant=eth&elf_id={elf}", h.http);
    let off: serde_json::Value = serde_json::from_str(&admin_curl(&h, &["-X", "PUT", &key])).unwrap();
    assert_eq!(off["disabled"], true);
    assert_eq!(proof_app(&h, "eth")["disabled"], true, "the list says so");
    h.stage.generate_proof(request("job-e2", b"elf-e")).await.unwrap();
    assert_eq!(wait_done(&mut h, "job-e2").await.status, S::InvalidParameter as i32, "refused while disabled");
    admin_curl(&h, &["-X", "DELETE", &key]);
    assert_eq!(proof_app(&h, "eth")["disabled"], false);
    h.stage.generate_proof(request("job-e3", b"elf-e")).await.unwrap();
    assert_eq!(wait_done(&mut h, "job-e3").await.status, S::Success as i32, "runs again once enabled");
    let unknown = admin_curl(&h, &["-o", "/dev/null", "-w", "%{http_code}", "-X", "PUT", &format!("{}/admin/apps/disabled?namespace=system&tenant=nobody&elf_id=ff", h.http)]);
    assert_eq!(unknown, "404");
}

fn signed_request(id: &str, elf: &[u8], key: &k256::ecdsa::SigningKey) -> GenerateProofRequest {
    let mut req = request(id, elf);
    sign_request(&mut req, key);
    req
}

fn sign_request(req: &mut GenerateProofRequest, key: &k256::ecdsa::SigningKey) {
    let msg = fleet_scheduler::auth::signed_message(&req.proof_id, req.block_no, req.seg_size);
    let hash = alloy_primitives::eip191_hash_message(msg.as_bytes());
    let (sig, recid) = key.sign_prehash_recoverable(hash.as_slice()).unwrap();
    let mut bytes = sig.to_bytes().to_vec();
    bytes.push(27 + recid.to_byte());
    req.signature = hex::encode(bytes);
}

#[tokio::test(flavor = "multi_thread")]
async fn jobs_require_authenticated_immutable_identity() {
    let mut h = start().await;
    let mut unsigned = request("unsigned", b"elf");
    unsigned.signature.clear();
    let refused = h.stage.generate_proof(unsigned).await.unwrap().into_inner();
    assert_eq!(refused.status, S::InvalidParameter as i32);
    assert_eq!(admin_curl(&h, &["-o", "/dev/null", "-w", "%{http_code}", &format!("{}/admin/jobs/unsigned", h.http)]), "404");

    let submitted = h.stage.generate_proof(request("identity", b"elf")).await.unwrap().into_inner();
    assert_eq!(submitted.status, S::Computing as i32);
    let saved: serde_json::Value = serde_json::from_str(&admin_curl(&h, &[&format!("{}/admin/jobs/identity", h.http)])).unwrap();
    assert_eq!(saved["owner"], operator_address());
    assert_eq!(saved["namespace"], fleet_scheduler::store::SYSTEM_NAMESPACE);
    assert_eq!(saved["tag"], "");
    assert_eq!(saved["client_id"], "identity");
    assert_eq!(saved["state"], "queued");
    for field in ["node", "started_ms", "finished_ms"] {
        assert!(saved[field].is_null(), "queued {field} is legitimately absent");
    }
    // Another token in the same tenant can poll/retry, but cannot rewrite audit identity.
    let key = k256::ecdsa::SigningKey::from_slice(&[8; 32]).unwrap();
    let address = format!("{:x}", alloy_primitives::Address::from_public_key(key.verifying_key()));
    token(&h, "eth", &address);
    h.stage.generate_proof(signed_request("identity", b"different input", &key)).await.unwrap();
    let unchanged: serde_json::Value = serde_json::from_str(&admin_curl(&h, &[&format!("{}/admin/jobs/identity", h.http)])).unwrap();
    assert_eq!(saved, unchanged);
}

fn user_http(h: &Harness, token: &str, method: &str, path: &str) -> (u16, String) {
    let out = std::process::Command::new("curl")
        .args(["-sS", "-X", method, "-H", &format!("authorization: Bearer {token}"),
            "-w", "\n%{http_code}", &format!("{}{path}", h.http)])
        .output().unwrap();
    let text = String::from_utf8(out.stdout).unwrap();
    let (body, code) = text.rsplit_once('\n').unwrap();
    (code.parse().unwrap(), body.into())
}

#[tokio::test(flavor = "multi_thread")]
async fn user_tokens_scope_apps_jobs_and_revocation_even_within_one_tenant() {
    use sha2::Digest;
    let mut h = start().await;
    // Namespace-specific nodes are registered after their namespaces are created below.
    let elf = fleet_scheduler::blobs::elf_id_of(b"elf");
    let keys: Vec<_> = [11u8, 12].iter().map(|v| k256::ecdsa::SigningKey::from_slice(&[*v; 32]).unwrap()).collect();
    let addresses: Vec<_> = keys.iter().map(|k| format!("{:x}", alloy_primitives::Address::from_public_key(k.verifying_key()))).collect();
    let tokens = ["a".repeat(96), "b".repeat(96)];
    for i in 0..2 {
        let ns = format!("user-{i}");
        let created_ns = admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d", &serde_json::json!({"name": ns}).to_string(),
            &format!("{}/admin/namespaces", h.http)]);
        assert!(serde_json::from_str::<serde_json::Value>(&created_ns).unwrap()["id"].is_string());
        let code = admin_curl(&h, &["-X", "POST", "-o", "/dev/null", "-w", "%{http_code}",
            &format!("{}/admin/apps?namespace={ns}&tenant=eth&elf_id={elf}", h.http)]);
        assert_eq!(code, "200");
        let token_id = format!("{:02x}", i + 1).repeat(16);
        let body = serde_json::json!({"id": token_id, "namespace": ns, "tenant": "eth", "address": addresses[i], "label": format!("user-{i}"),
            "apps": [format!("eth/{elf}"), "training"], "token_hash": hex::encode(sha2::Sha256::digest(tokens[i].as_bytes()))});
        let created = admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d", &body.to_string(),
            &format!("{}/admin/tokens", h.http)]);
        let c: serde_json::Value = serde_json::from_str(&created).unwrap();
        assert_eq!(c["apps"][1], "training");
        assert!(c["id"].as_str().is_some_and(|id| id.len() == 32));
        assert!(c.get("address").is_none(), "proof signer binding is internal to the token resource");
        assert!(c.get("token_hash").is_none());
        add_node_for_app(&h, &format!("gpu-user-{i}"), 2, false, &ns, "eth", b"elf").await;
        let id = format!("user-{i}");
        let response = h.stage.generate_proof(with_bearer(signed_request(&id, b"elf", &keys[i]), &tokens[i])).await.unwrap().into_inner();
        assert_eq!(response.status, S::Computing as i32, "{}", response.error_message);
    }
    let own = h.stage.get_status(with_bearer(GetStatusRequest { proof_id: "user-0".into() }, &tokens[0])).await.unwrap().into_inner();
    assert_eq!(own.status, S::Success as i32);
    let foreign = h.stage.get_status(with_bearer(GetStatusRequest { proof_id: "user-0".into() }, &tokens[1])).await.unwrap().into_inner();
    assert_eq!(foreign.status, S::Unspecified as i32);
    let collision = h.stage.generate_proof(with_bearer(signed_request("not-granted-b", b"elf-r", &keys[1]), &tokens[1])).await.unwrap().into_inner();
    assert_eq!(collision.status, S::InvalidParameter as i32); // This user's grant does not include elf-r.
    let wrong_signer = h.stage.generate_proof(with_bearer(signed_request("wrong-cert", b"elf", &keys[0]), &tokens[1])).await.unwrap().into_inner();
    assert_eq!(wrong_signer.status, S::InvalidParameter as i32);
    let unsigned = h.stage.generate_proof(with_bearer(request("unsigned-user", b"elf"), &tokens[0])).await.unwrap().into_inner();
    assert_eq!(unsigned.status, S::InvalidParameter as i32);
    let denied = h.stage.generate_proof(with_bearer(signed_request("not-granted", b"elf-r", &keys[0]), &tokens[0])).await.unwrap().into_inner();
    assert_eq!(denied.status, S::InvalidParameter as i32);
    assert!(denied.error_message.contains("not granted"));
    let rejection = h.stage.get_status(with_bearer(GetStatusRequest { proof_id: "not-granted".into() }, &tokens[0])).await.unwrap().into_inner();
    assert_eq!(rejection.status, S::InvalidParameter as i32);
    assert_eq!(user_http(&h, &tokens[0], "GET", "/admin/jobs/user-0").0, 200);
    assert_eq!(user_http(&h, &tokens[1], "GET", "/admin/jobs/user-0").0, 404);
    assert_eq!(user_http(&h, &tokens[0], "GET", "/admin/tokens").0, 403);
    assert_eq!(user_http(&h, &tokens[0], "GET", "/admin/status").0, 403);
    assert_eq!(user_http(&h, &tokens[0], "POST", "/admin/apps").0, 403);
    assert_eq!(user_http(&h, &tokens[0], "PUT", &format!("/admin/apps/priority?namespace=user-0&tenant=eth&elf_id={elf}&priority=100")).0, 403);
    let (_, body) = user_http(&h, &tokens[1], "GET", "/admin/jobs?limit=1");
    let jobs: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(jobs.as_array().unwrap().len(), 1);
    assert_eq!(jobs[0]["owner"], addresses[1]);
    let (_, body) = user_http(&h, &tokens[1], "GET", "/admin/apps");
    let apps: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(apps.as_array().unwrap().len(), 1);
    assert_eq!(apps[0]["jobs"], 1);
    assert_eq!(user_http(&h, &tokens[0], "GET", "/access").0, 200);
    admin_curl(&h, &["-X", "DELETE", &format!("{}/admin/tokens/{}", h.http, "01".repeat(16))]);
    assert_eq!(user_http(&h, &tokens[0], "GET", "/access").0, 401);
    assert_eq!(user_http(&h, &tokens[0], "GET", "/admin/jobs/user-0").0, 401);
    let revoked = h.stage.get_status(with_bearer(GetStatusRequest { proof_id: "user-0".into() }, &tokens[0])).await.unwrap_err();
    assert_eq!(revoked.code(), tonic::Code::Unauthenticated);
    // A previously issued public-values capability no longer works after revocation.
    let values = std::process::Command::new("curl").args(["-s", "-o", "/dev/null", "-w", "%{http_code}", &own.public_values_url]).output().unwrap();
    assert_eq!(String::from_utf8(values.stdout).unwrap(), "404");
}

#[tokio::test(flavor = "multi_thread")]
async fn bearer_tokens_authorize_proof_submission_and_status_without_client_certificates() {
    let mut h = start().await;
    // Alice's dedicated node is registered after creating the namespace below.
    let elf = fleet_scheduler::blobs::elf_id_of(b"elf");
    admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d",
        &serde_json::json!({"name": "alice"}).to_string(), &format!("{}/admin/namespaces", h.http)]);
    let (alice_key, _, alice_token) = tag_token(&h, "alice", Some("research"), 71, &elf);
    let (bob_key, _, bob_token) = tag_token(&h, "alice", Some("production"), 72, &elf);
    add_node_for_app(&h, "gpu-bearer", 2, false, "alice", "eth", b"elf").await;

    let accepted = h.stage.generate_proof(with_bearer(
        signed_request("bearer-proof", b"elf", &alice_key), &alice_token,
    )).await.unwrap().into_inner();
    assert_eq!(accepted.status, S::Computing as i32, "{}", accepted.error_message);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let own = loop {
        let response = h.stage.get_status(with_bearer(
            GetStatusRequest { proof_id: "bearer-proof".into() }, &alice_token,
        )).await.unwrap().into_inner();
        if response.status != S::Computing as i32 { break response; }
        assert!(tokio::time::Instant::now() < deadline, "bearer proof did not finish");
    };
    assert_eq!(own.status, S::Success as i32);
    assert!(own.public_values_url.contains("token_id=47474747474747474747474747474747"));

    let foreign = h.stage.get_status(with_bearer(
        GetStatusRequest { proof_id: "bearer-proof".into() }, &bob_token,
    )).await.unwrap().into_inner();
    assert_eq!(foreign.status, S::Unspecified as i32);

    let proof_path = "/admin/jobs/bearer-proof/output/proof.json";
    assert_eq!(user_http(&h,&alice_token,"GET",proof_path).0,200);
    assert_eq!(user_http(&h,&bob_token,"GET",proof_path).0,404);
    assert_eq!(user_http(&h,&alice_token,"GET","/admin/jobs/bearer-proof/output/stdin.bin").0,403);
    std::fs::remove_dir_all(h._dir.path().join("data/proofs")).unwrap();
    let missing = user_http(&h,&alice_token,"GET",proof_path);
    assert_eq!(missing.0,404);
    assert!(missing.1.contains("proof output is not available on the control plane"));

    let mismatched = h.stage.generate_proof(with_bearer(
        signed_request("bearer-mismatch", b"elf", &bob_key), &alice_token,
    )).await.unwrap().into_inner();
    assert_eq!(mismatched.status, S::InvalidParameter as i32);

    let no_token = h.stage.get_status(Request::new(GetStatusRequest { proof_id: "bearer-proof".into() }))
        .await.unwrap().into_inner();
    assert_eq!(no_token.status, S::Unspecified as i32);

    let revoked = admin_curl(&h, &["-X", "DELETE", "-H", &format!("authorization: Bearer {ADMIN}"),
        &format!("{}/admin/tokens/{}", h.http, "47".repeat(16))]);
    assert!(revoked.is_empty());
    let after_revoke = h.stage.get_status(with_bearer(
        GetStatusRequest { proof_id: "bearer-proof".into() }, &alice_token,
    )).await.unwrap_err();
    assert_eq!(after_revoke.code(), tonic::Code::Unauthenticated);
}

/// Machine fetches run without any GPU actor or inbound listener.
async fn start_fetch_agent(h: &Harness, root: std::path::PathBuf) -> tokio::task::JoinHandle<()> {
    use fleet_proto::fleet::v1::{worker_service_client::WorkerServiceClient, WorkerHeartbeatRequest};
    let mut worker = WorkerServiceClient::connect(format!("http://127.0.0.1:{}", h.control_port)).await.unwrap();
    let http = h.http.clone();
    let task = tokio::spawn(async move {
        let client = reqwest::Client::new();
        loop {
            let mut request = Request::new(WorkerHeartbeatRequest {
                worker_id: "storage-agent".into(), session: "storage-session".into(), proof_fetch_ready: true,
                report_json: serde_json::to_vec(&serde_json::json!({"version":1,"machine":"test","ready":true,
                    "total_gpus":8,"total_cpus":48,"free_gpus":8,"free_cpus":48})).unwrap(),
            });
            request.metadata_mut().insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
            let response = worker.heartbeat(request).await.unwrap().into_inner();
            for fetch in response.proof_fetches {
                let result = fleet_ziren_node::fetch::read(&fetch, std::slice::from_ref(&root));
                let mut post = client.post(format!("{http}/internal/proof-fetch/{}",fetch.ticket))
                    .bearer_auth(TOKEN).header("x-fleet-worker","storage-agent").header("x-fleet-session","storage-session");
                post = match result { Ok(bytes) => post.body(bytes), Err(_) => post.header("x-fleet-fetch-error","invalid") };
                assert_eq!(post.send().await.unwrap().status().as_u16(),204);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    task
}

#[tokio::test(flavor = "multi_thread")]
async fn app_endpoint_reads_local_outputs_without_machine_fetch_heartbeats() {
    use fleet_proof_app::{App, AppApi};
    use std::future::IntoFuture;
    use fleet_proto::stage::v1::stage_service_server::StageServiceServer;
    let h = start().await;
    admin_curl(&h,&["-X","POST","-H","content-type: application/json","-d","{\"name\":\"alice\"}",&format!("{}/admin/namespaces",h.http)]);
    let elf=fleet_scheduler::blobs::elf_id_of(b"elf");
    let (key,_,token)=tag_token(&h,"alice",Some("research"),81,&elf);
    let (_,_,foreign)=tag_token(&h,"alice",Some("production"),82,&elf);
    let (_, _, proof_node) = add_node_app_session(&h,"app-prover",2,false,150,tonic::Code::Internal,"alice","eth",b"elf").await;
    let storage=tempfile::tempdir().unwrap();
    let http_listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_url=format!("http://{}",http_listener.local_addr().unwrap());
    let namespace:serde_json::Value=serde_json::from_str(&admin_curl(&h,&[&format!("{}/admin/namespaces/alice",h.http)])).unwrap();
    let token_file = storage.path().join("user.token");
    std::fs::write(&token_file,&token).unwrap();
    let mut app = App::new(storage.path().into(),namespace["id"].as_str().unwrap().into(),"eth".into(),
        vec![elf, fleet_scheduler::blobs::elf_id_of(b"another-program")],h.http.clone(),http_url).unwrap();
    app.token_file = Some(token_file);
    let app=Arc::new(app);
    tokio::spawn(axum::serve(http_listener,fleet_proof_app::router(app.clone())).into_future());
    let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let channel=Channel::from_shared(format!("http://{}",listener.local_addr().unwrap())).unwrap();
    let task=tokio::spawn(tonic::transport::Server::builder().add_service(StageServiceServer::new(AppApi(app)))
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)));
    let mut client=StageServiceClient::new(channel.connect().await.unwrap());
    let mut request=signed_request("app-storage-proof",b"elf",&key);
    request.receipt_inputs = vec![vec![9,8,7]]; // Older SDK wire field.
    let accepted=client.generate_proof(request.clone()).await.unwrap().into_inner();
    assert_eq!(accepted.status,S::Computing as i32,"{}",accepted.error_message);
    let deadline=tokio::time::Instant::now()+Duration::from_secs(5);
    let status=loop {
        let response=client.get_status(with_bearer(GetStatusRequest{proof_id:"app-storage-proof".into()},&token)).await.unwrap().into_inner();
        if response.status==S::Success as i32 {break response;}
        assert!(tokio::time::Instant::now()<deadline,"app proof did not finish");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(!status.proof_with_public_inputs.is_empty());
    // The fake prover reports 100 ms despite slower execution. Neither admission nor input/output transfers change this value.
    assert_eq!(status.proving_time, 100);
    assert!(status.total_steps > 0);
    let mut changed=request.clone();
    changed.private_input_stream=bincode::serialize(&vec![b"different".to_vec()]).unwrap();
    assert_eq!(client.generate_proof(with_bearer(changed,&token)).await.unwrap().into_inner().status,S::InvalidParameter as i32);
    let duplicate=client.generate_proof(with_bearer(request,&token)).await.unwrap().into_inner();
    assert_eq!(duplicate.status,S::Computing as i32);
    let foreign=client.get_status(with_bearer(GetStatusRequest{proof_id:"app-storage-proof".into()},&foreign)).await.unwrap().into_inner();
    assert_eq!(foreign.status,S::Unspecified as i32);
    let values=reqwest::get(&status.public_values_url).await.unwrap();
    assert!(values.status().is_success());assert!(!values.bytes().await.unwrap().is_empty());
    for dir in ["elf","jobs"] {assert_eq!(std::fs::read_dir(h._dir.path().join("data").join(dir)).unwrap().count(),0);}
    assert!(!h._dir.path().join("data/proofs").exists());
    let metadata=user_http(&h,&token,"GET","/admin/jobs/app-storage-proof");
    let metadata:serde_json::Value=serde_json::from_str(&metadata.1).unwrap();
    assert!(metadata["inputs"]["input"]["uri"].as_str().unwrap().starts_with("file://"));
    use prost::Message;
    let input_path=metadata["inputs"]["input"]["uri"].as_str().unwrap().strip_prefix("file://").unwrap();
    let input=fleet_proto::fleet::v1::ProveRequest::decode(std::fs::read(input_path).unwrap().as_slice()).unwrap();
    assert_eq!(input.receipts,vec![vec![9,8,7]]);
    assert_eq!(client.get_status(with_bearer(GetStatusRequest{proof_id:"app-storage-proof".into()},"invalid-supplied-token")).await.unwrap_err().code(),tonic::Code::Unauthenticated);
    assert!(metadata["outputs"]["proof"]["sha256"].as_str().unwrap().len()==64);
    let (_,_,ungranted)=tag_token(&h,"alice",Some("research"),83,&fleet_scheduler::blobs::elf_id_of(b"another-program"));
    assert_eq!(user_http(&h,&ungranted,"GET","/admin/jobs/app-storage-proof/output/proof.json").0,404);
    let mut control_stage=h.stage.clone();
    assert_eq!(control_stage.get_status(with_bearer(GetStatusRequest{proof_id:"app-storage-proof".into()},&ungranted)).await.unwrap().into_inner().status,S::Unspecified as i32);
    // Local reads retain integrity checks and never fall back to the control host.
    let proof_path = metadata["outputs"]["proof"]["uri"].as_str().unwrap().strip_prefix("file://").unwrap();
    std::fs::write(proof_path, b"corrupt").unwrap();
    assert_eq!(client.get_status(with_bearer(GetStatusRequest {proof_id:"app-storage-proof".into()}, &token))
        .await.unwrap_err().code(), tonic::Code::DataLoss);
    std::fs::write(proof_path, &status.proof_with_public_inputs).unwrap();
    // App status and public values succeeded with no machine fetch agent.
    let fetch_agent = start_fetch_agent(&h, storage.path().into()).await;
    // Stopping the proof actor releases GPU execution, but machine storage still serves files.
    let channel = Channel::from_shared(format!("http://127.0.0.1:{}",h.control_port)).unwrap().connect().await.unwrap();
    let mut ctl = ControlServiceClient::with_interceptor(channel, fleet_scheduler::token::Attach::new(TOKEN).unwrap());
    ctl.deregister(fleet_proto::fleet::v1::DeregisterRequest {node_id:proof_node.into()}).await.unwrap();
    assert_eq!(metadata["outputs"]["machine"],"test");
    let elf = metadata["elf_id"].as_str().unwrap();
    admin_curl(&h,&["-X","PUT",&format!("{}/admin/apps/disabled?namespace=alice&tenant=eth&elf_id={elf}",h.http)]);
    let rejected = client.generate_proof(with_bearer(signed_request("app-disabled-proof", b"elf", &key), &token))
        .await.unwrap().into_inner();
    assert_eq!(rejected.status, S::InvalidParameter as i32);
    let rejected = client.get_status(with_bearer(GetStatusRequest {proof_id:"app-disabled-proof".into()}, &token))
        .await.unwrap().into_inner();
    assert_eq!(rejected.status, S::InvalidParameter as i32);
    assert!(rejected.proof_with_public_inputs.is_empty());
    let fetch = reqwest::Client::new().get(format!("{}/admin/jobs/app-storage-proof/output/proof.json",h.http))
        .bearer_auth(&token).send().await.unwrap();
    assert_eq!(fetch.status().as_u16(),200);assert_eq!(fetch.bytes().await.unwrap().as_ref(),status.proof_with_public_inputs);
    let path = metadata["outputs"]["proof"]["uri"].as_str().unwrap().strip_prefix("file://").unwrap();
    std::fs::write(path,b"corrupt").unwrap();
    let corrupt = reqwest::Client::new().get(format!("{}/admin/jobs/app-storage-proof/output/proof.json",h.http)).bearer_auth(&token).send().await.unwrap();
    assert_eq!(corrupt.status().as_u16(),502);
    std::fs::write(path,&status.proof_with_public_inputs).unwrap();
    admin_curl(&h,&["-X","DELETE",&format!("{}/admin/tokens/{}",h.http,"51".repeat(16))]);
    assert_eq!(reqwest::get(&status.public_values_url).await.unwrap().status().as_u16(),404);
    assert_eq!(client.get_status(with_bearer(GetStatusRequest{proof_id:"app-storage-proof".into()},&token)).await.unwrap_err().code(),tonic::Code::Unauthenticated);
    fetch_agent.abort();
    task.abort();
}

fn namespace_token(h: &Harness, namespace: &str, seed: u8, elf_id: &str) -> (k256::ecdsa::SigningKey, String, String) {
    tag_token(h,namespace,None,seed,elf_id)
}

#[tokio::test(flavor = "multi_thread")]
async fn apps_require_admin_created_namespaces_and_mutations_are_namespace_local() {
    let mut h = start().await;
    let elf = fleet_scheduler::blobs::elf_id_of(b"elf");
    for (selector, expected) in [("", "400"), ("&namespace=missing", "404")] {
        let code = admin_curl(&h, &["-X", "POST", "-o", "/dev/null", "-w", "%{http_code}",
            &format!("{}/admin/apps?tenant=eth&elf_id={elf}{selector}", h.http)]);
        assert_eq!(code, expected);
    }
    for name in ["alice", "bob"] {
        admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d",
            &serde_json::json!({"name":name}).to_string(), &format!("{}/admin/namespaces", h.http)]);
    }
    let (alice_key, _, alice_token) = namespace_token(&h, "alice", 61, &elf);
    let (bob_key, _, bob_token) = namespace_token(&h, "bob", 62, &elf);
    assert_eq!(user_http(&h, &alice_token, "POST", "/admin/namespaces").0, 403);
    assert_eq!(user_http(&h, &alice_token, "POST", &format!("/admin/apps?namespace=alice&tenant=eth&elf_id={elf}")).0, 403);
    let key = format!("{}/admin/apps/disabled?namespace=alice&tenant=eth&elf_id={elf}", h.http);
    admin_curl(&h, &["-X", "PUT", &key]);
    let (code, apps) = user_http(&h, &bob_token, "GET", "/admin/apps");
    assert_eq!(code, 200);
    let apps: serde_json::Value = serde_json::from_str(&apps).unwrap();
    assert_eq!(apps.as_array().unwrap().len(), 1);
    assert_eq!(apps[0]["disabled"], false);
    let denied = h.stage.generate_proof(with_bearer(signed_request("disabled", b"elf", &alice_key), &alice_token)).await.unwrap().into_inner();
    assert_eq!(denied.status, S::InvalidParameter as i32);
    add_node_for_app(&h, "gpu-namespaces", 2, false, "bob", "eth", b"elf").await;
    h.stage.generate_proof(with_bearer(signed_request("bob-job", b"elf", &bob_key), &bob_token)).await.unwrap();
    let result = h.stage.get_status(with_bearer(GetStatusRequest { proof_id:"bob-job".into() }, &bob_token)).await.unwrap().into_inner();
    assert_eq!(result.status, S::Success as i32);
    let deleted = admin_curl(&h, &["-X", "DELETE", "-o", "/dev/null", "-w", "%{http_code}",
        &format!("{}/admin/apps?namespace=alice&tenant=eth&elf_id={elf}", h.http)]);
    assert_eq!(deleted, "200");
    let result = h.stage.get_status(with_bearer(GetStatusRequest { proof_id:"bob-job".into() }, &bob_token)).await.unwrap().into_inner();
    assert_eq!(result.status, S::Success as i32, "deleting Alice's registration preserves Bob's job and shared ELF");
}

fn tag_token(h: &Harness,namespace: &str,tag: Option<&str>,seed: u8,elf_id: &str) -> (k256::ecdsa::SigningKey,String,String) {
    use sha2::Digest;
    let code = admin_curl(h, &["-X", "POST", "-o", "/dev/null", "-w", "%{http_code}",
        &format!("{}/admin/apps?namespace={namespace}&tenant=eth&elf_id={elf_id}", h.http)]);
    assert!(code == "200" || code == "409", "register namespace app: {code}");
    let key = k256::ecdsa::SigningKey::from_slice(&[seed; 32]).unwrap();
    let address = format!("{:x}", alloy_primitives::Address::from_public_key(key.verifying_key()));
    let token = format!("{seed:02x}").repeat(48);
    let body = serde_json::json!({"id": format!("{seed:02x}").repeat(16), "namespace": namespace, "tag":tag, "tenant": "eth", "address": address,
        "apps": [format!("eth/{elf_id}"), "training"], "label": namespace,
        "token_hash": hex::encode(sha2::Sha256::digest(token.as_bytes()))});
    let created = admin_curl(h, &["-X", "POST", "-H", "content-type: application/json", "-d", &body.to_string(),
        &format!("{}/admin/tokens", h.http)]);
    let c: serde_json::Value = serde_json::from_str(&created).unwrap();
    assert!(c["namespace"].is_string(), "{created}");
    (key, address, token)
}

#[tokio::test(flavor = "multi_thread")]
async fn namespace_rotation_preserves_jobs_and_suspension_blocks_every_key() {
    let mut h = start().await;
    let elf = fleet_scheduler::blobs::elf_id_of(b"elf");
    for name in ["alice", "bob"] {
        let body = serde_json::json!({"name": name});
        let created = admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d", &body.to_string(),
            &format!("{}/admin/namespaces", h.http)]);
        assert!(serde_json::from_str::<serde_json::Value>(&created).unwrap()["id"].is_string());
    }
    for invalid in ["admin", "system", "Alice", "../escape", "", "9alice", "alice"] {
        let body = serde_json::json!({"name": invalid}).to_string();
        let code = admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d", &body,
            "-o", "/dev/null", "-w", "%{http_code}", &format!("{}/admin/namespaces", h.http)]);
        assert_eq!(code, "400", "invalid, reserved or duplicate namespace {invalid:?}");
    }
    let (old_key, old_addr, old_token) = namespace_token(&h, "alice", 21, &elf);
    let (bob_key, _, bob_token) = namespace_token(&h, "bob", 22, &elf);
    let (_, alice_jobs) = add_node_for_app(&h, "gpu-ns-alice", 2, false, "alice", "eth", b"elf").await;
    let (_, bob_jobs) = add_node_for_app(&h, "gpu-ns-bob", 2, false, "bob", "eth", b"elf").await;
    for (key, token) in [(&old_key, &old_token), (&bob_key, &bob_token)] {
        let r = h.stage.generate_proof(with_bearer(signed_request("same-id", b"elf", key), token)).await.unwrap().into_inner();
        assert_eq!(r.proof_id, "same-id");
        assert_eq!(r.status, S::Computing as i32);
    }
    let alice = h.stage.get_status(with_bearer(GetStatusRequest { proof_id: "same-id".into() }, &old_token)).await.unwrap().into_inner();
    let bob = h.stage.get_status(with_bearer(GetStatusRequest { proof_id: "same-id".into() }, &bob_token)).await.unwrap().into_inner();
    assert_eq!(alice.status, S::Success as i32);
    assert_eq!(bob.status, S::Success as i32);
    assert_ne!(alice.public_values_url, bob.public_values_url);
    assert_eq!(alice_jobs.lock().len(), 1, "Alice's dedicated server runs only Alice's proof");
    assert_eq!(bob_jobs.lock().len(), 1, "Bob's dedicated server runs only Bob's proof");
    let jobs: Vec<_> = alice_jobs.lock().iter().chain(bob_jobs.lock().iter()).cloned().collect();
    assert_eq!(jobs.len(), 2);
    assert_ne!(jobs[0], jobs[1], "nodes use distinct internal ids");
    let (_, a_json) = user_http(&h, &old_token, "GET", "/admin/jobs/same-id");
    let (_, b_json) = user_http(&h, &bob_token, "GET", "/admin/jobs/same-id");
    let a: serde_json::Value = serde_json::from_str(&a_json).unwrap();
    let b: serde_json::Value = serde_json::from_str(&b_json).unwrap();
    assert_ne!(a["id"], b["id"]);
    assert_ne!(a["namespace"], b["namespace"]);
    assert_eq!(user_http(&h, &bob_token, "GET", &format!("/admin/jobs/{}", a["id"].as_str().unwrap())).0, 404);
    assert_eq!(user_http(&h, &bob_token, "GET", "/admin/jobs/same-id?namespace=alice").0, 403);
    let (new_key, _, new_token) = namespace_token(&h, "alice", 23, &elf);
    let repeated = h.stage.generate_proof(with_bearer(signed_request("same-id", b"elf", &new_key), &new_token)).await.unwrap().into_inner();
    assert_eq!(repeated.status, S::Computing as i32);
    assert_eq!(alice_jobs.lock().len() + bob_jobs.lock().len(), 2, "rotation does not queue a duplicate proof");
    admin_curl(&h, &["-X", "DELETE", &format!("{}/admin/tokens/{}", h.http, "15".repeat(16))]);
    assert_eq!(user_http(&h, &old_token, "GET", "/access").0, 401);
    let (_, rotated_json) = user_http(&h, &new_token, "GET", "/admin/jobs/same-id");
    let rotated: serde_json::Value = serde_json::from_str(&rotated_json).unwrap();
    assert_eq!(rotated["id"], a["id"]);
    assert_eq!(rotated["owner"], old_addr, "the original submitter remains in the audit record");
    let rotated = h.stage.get_status(with_bearer(GetStatusRequest { proof_id: "same-id".into() }, &new_token)).await.unwrap().into_inner();
    assert_eq!(rotated.status, S::Success as i32);
    for (url, expected) in [(&alice.public_values_url, "404"), (&rotated.public_values_url, "200")] {
        let out = std::process::Command::new("curl").args(["-s", "-o", "/dev/null", "-w", "%{http_code}", url]).output().unwrap();
        assert_eq!(String::from_utf8(out.stdout).unwrap(), expected);
    }
    let admin_a = admin_curl(&h, &[&format!("{}/admin/jobs/same-id?namespace=alice", h.http)]);
    assert_eq!(serde_json::from_str::<serde_json::Value>(&admin_a).unwrap()["id"], a["id"]);
    let (_, _, second_token) = namespace_token(&h, "alice", 24, &elf);
    let denied = user_http(&h, &new_token, "POST", "/admin/namespaces");
    assert_eq!(denied.0, 403);
    admin_curl(&h, &["-X", "PUT", &format!("{}/admin/namespaces/alice/suspended", h.http)]);
    assert_eq!(user_http(&h, &new_token, "GET", "/access").0, 401);
    assert_eq!(user_http(&h, &second_token, "GET", "/access").0, 401);
    assert_eq!(user_http(&h, &bob_token, "GET", "/access").0, 200);
    assert_eq!(h.stage.get_status(with_bearer(GetStatusRequest { proof_id: "same-id".into() }, &new_token)).await.unwrap_err().code(), tonic::Code::Unauthenticated);
    admin_curl(&h, &["-X", "DELETE", &format!("{}/admin/namespaces/alice/suspended", h.http)]);
    assert_eq!(user_http(&h, &new_token, "GET", "/access").0, 200);
    assert_eq!(user_http(&h, &old_token, "GET", "/access").0, 401);
}


#[tokio::test(flavor = "multi_thread")]
async fn namespace_resource_quota_is_shared_by_proofs_runs_and_rotated_keys() {
    let mut h = start().await;
    // The namespace-bound node is added after Alice's namespace and app exist.
    for name in ["alice", "bob"] {
        let body = serde_json::json!({"name":name,"quota":{"gpus":2,"cpus":8}}).to_string();
        admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d", &body,
            &format!("{}/admin/namespaces", h.http)]);
    }
    let elf = fleet_scheduler::blobs::elf_id_of(b"elf");
    let (key, _, token) = namespace_token(&h, "alice", 31, &elf);
    let (calls, _) = add_node_app(&h, "gpu-quota", 2, false, 1000, tonic::Code::Internal, "alice", "eth", b"elf").await;
    let (_, _, replacement_token) = namespace_token(&h, "alice", 32, &elf);
    let allocation_url = format!("{}/admin/namespaces/alice/allocations", h.http);
    let held = serde_json::json!({"id":"held-run","gpus":2,"cpus":8}).to_string();
    let held_reply = admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d", &held, &allocation_url]);
    assert!(serde_json::from_str::<serde_json::Value>(&held_reply).unwrap()["id"].is_string());
    let overflow = serde_json::json!({"id":"another-run","gpus":0,"cpus":1}).to_string();
    let code = admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d", &overflow,
        "-o", "/dev/null", "-w", "%{http_code}", &allocation_url]);
    assert_eq!(code, "429");
    let r = h.stage.generate_proof(with_bearer(signed_request("proof-quota", b"elf", &key), &token)).await.unwrap().into_inner();
    assert_eq!(r.status, S::Computing as i32);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0, "a run reservation leaves no quota for a proof");
    let (_, record) = user_http(&h, &token, "GET", "/admin/jobs/proof-quota");
    assert_eq!(serde_json::from_str::<serde_json::Value>(&record).unwrap()["state"], "queued");
    let (_, access) = user_http(&h, &replacement_token, "GET", "/access");
    let access: serde_json::Value = serde_json::from_str(&access).unwrap();
    assert_eq!(access["quota"]["gpus"], 2.0);
    assert_eq!(access["usage"]["gpus"], 2.0);
    assert_eq!(user_http(&h, &token, "POST", "/admin/namespaces/alice/allocations").0, 403);
    // Bob has an independent cap, even with the same tenant and app grants.
    let bob_claim = serde_json::json!({"id":"bob-run","gpus":0,"cpus":1}).to_string();
    let bob = admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d", &bob_claim,
        &format!("{}/admin/namespaces/bob/allocations", h.http)]);
    assert!(serde_json::from_str::<serde_json::Value>(&bob).unwrap()["id"].is_string());
    admin_curl(&h, &["-X", "DELETE", &format!("{allocation_url}/held-run")]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while calls.load(Ordering::SeqCst) == 0 {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let code = admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d", &overflow,
        "-o", "/dev/null", "-w", "%{http_code}", &allocation_url]);
    assert_eq!(code, "429", "a running proof occupies the same quota as runs");
    let done = h.stage.get_status(with_bearer(GetStatusRequest { proof_id:"proof-quota".into() }, &token)).await.unwrap().into_inner();
    assert_eq!(done.status, S::Success as i32);
    let (_, access) = user_http(&h, &token, "GET", "/access");
    assert_eq!(serde_json::from_str::<serde_json::Value>(&access).unwrap()["usage"]["cpus"], 0.0);
    // An individually oversized run is refused; zero/unlimited caps can be updated.
    let too_big = serde_json::json!({"id":"oversized","gpus":3,"cpus":1}).to_string();
    assert_eq!(admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d", &too_big,
        "-o", "/dev/null", "-w", "%{http_code}", &allocation_url]), "422");
    for quota in [serde_json::json!({"gpus":-1}), serde_json::json!({"gpu_hours":5})] {
        let code = admin_curl(&h, &["-X", "PUT", "-H", "content-type: application/json", "-d", &quota.to_string(),
            "-o", "/dev/null", "-w", "%{http_code}", &format!("{}/admin/namespaces/alice/quota", h.http)]);
        assert!(code == "400" || code == "422");
    }
    let mut concurrent = Vec::new();
    for id in ["concurrent-a", "concurrent-b"] {
        let url = allocation_url.clone();
        concurrent.push(std::thread::spawn(move || {
            let body = serde_json::json!({"id":id,"gpus":2,"cpus":8}).to_string();
            let out = std::process::Command::new("curl").args(["-s", "--max-time", "5", "-H", &format!("authorization: Bearer {ADMIN}"),
                "-H", "content-type: application/json", "-X", "POST", "-d", &body, "-o", "/dev/null", "-w", "%{http_code}", &url]).output().unwrap();
            String::from_utf8(out.stdout).unwrap()
        }));
    }
    let mut codes: Vec<_> = concurrent.into_iter().map(|t| t.join().unwrap()).collect();
    codes.sort();
    assert_eq!(codes, ["200", "429"], "concurrent admissions cannot overbook a namespace");

}

#[tokio::test(flavor = "multi_thread")]
async fn disconnected_namespaced_proof_keeps_quota_and_rejoins_original_node() {
    let mut h = start().await;
    // Both dedicated nodes are added after Alice's namespace app is registered below.
    let created = admin_curl(&h,&["-X","POST","-H","content-type: application/json","-d",
        r#"{"name":"alice","quota":{"gpus":2,"cpus":8}}"#,&format!("{}/admin/namespaces",h.http)]);
    assert!(created.contains("alice"));
    let elf=fleet_scheduler::blobs::elf_id_of(b"elf");
    let (key,_,token)=namespace_token(&h,"alice",41,&elf);
    let (original,_) = add_node_app(&h,"original",2,true,100,tonic::Code::Unavailable,"alice","eth",b"elf").await;
    let (other,_) = add_node_app(&h,"other",1,false,100,tonic::Code::Internal,"alice","eth",b"elf").await;
    h.stage.generate_proof(with_bearer(signed_request("disconnected",b"elf",&key),&token)).await.unwrap();
    let deadline=tokio::time::Instant::now()+Duration::from_secs(3);
    while original.load(Ordering::SeqCst)==0 {
        assert!(tokio::time::Instant::now()<deadline);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let (_,access)=user_http(&h,&token,"GET","/access");
    assert_eq!(serde_json::from_str::<serde_json::Value>(&access).unwrap()["usage"]["gpus"],2.0);
    let code=admin_curl(&h,&["-X","POST","-H","content-type: application/json","-d",
        r#"{"id":"blocked","gpus":1,"cpus":1}"#,"-o","/dev/null","-w","%{http_code}",&format!("{}/admin/namespaces/alice/allocations",h.http)]);
    assert_eq!(code,"429");
    assert_eq!(other.load(Ordering::SeqCst),0,"never fail over an execution of uncertain status");
    let done=h.stage.get_status(with_bearer(GetStatusRequest{proof_id:"disconnected".into()},&token)).await.unwrap().into_inner();
    assert_eq!(done.status,S::Success as i32);
    assert_eq!(original.load(Ordering::SeqCst),2);
    assert_eq!(other.load(Ordering::SeqCst),0);
}

#[tokio::test(flavor = "multi_thread")]
async fn tags_isolate_proofs_and_enforce_both_resource_caps() {
    let mut h=start().await;
    admin_curl(&h,&["-X","POST","-H","content-type: application/json","-d",r#"{"name":"billing","quota":{"gpus":2,"cpus":16}}"#,&format!("{}/admin/namespaces",h.http)]);
    let elf=fleet_scheduler::blobs::elf_id_of(b"elf");
    let (ka,_,ta)=tag_token(&h,"billing",Some("research"),51,&elf);
    let (kb,_,tb)=tag_token(&h,"billing",Some("production"),52,&elf);
    let (_,_,replacement)=tag_token(&h,"billing",Some("research"),53,&elf);
    add_node_for_app(&h,"tag-prover",1,false,"billing","eth",b"elf").await;
    let listing=admin_curl(&h,&[&format!("{}/admin/namespaces/billing",h.http)]);
    let listing:serde_json::Value=serde_json::from_str(&listing).unwrap();
    assert_eq!(listing["tags"].as_array().unwrap().len(),3);
    // Tags are optional attributes; no separate registry or creation step exists.
    let before=admin_curl(&h,&[&format!("{}/admin/namespaces/billing?tag=research",h.http)]);
    let before:serde_json::Value=serde_json::from_str(&before).unwrap();
    assert_eq!(before["name"],"billing");
    assert_eq!(before["quota"]["gpus"],2.0);
    assert!(before["tag_quota"]["gpus"].is_null());
    for tag in ["research","production","staging"] {
        let reply=admin_curl(&h,&["-X","PUT","-H","content-type: application/json","-d",r#"{"gpus":1,"cpus":8}"#,&format!("{}/admin/namespaces/billing/quota?tag={tag}",h.http)]);
        let result:serde_json::Value=serde_json::from_str(&reply).unwrap();
        assert_eq!(result["tag"],tag);
        assert_eq!(result["quota"]["gpus"],2.0,"tag caps never replace the namespace quota");
        assert_eq!(result["tag_quota"]["gpus"],1.0);
    }
    let (ku,_,tu)=namespace_token(&h,"billing",54,&elf);
    let (kd,_,td)=tag_token(&h,"billing",Some("default"),55,&elf);
    for (k,t) in [(&ka,&ta),(&kb,&tb),(&ku,&tu),(&kd,&td)] {
        h.stage.generate_proof(with_bearer(signed_request("same",b"elf",k),t)).await.unwrap();
        assert_eq!(h.stage.get_status(with_bearer(GetStatusRequest{proof_id:"same".into()},t)).await.unwrap().into_inner().status,S::Success as i32);
    }
    let (_,a)=user_http(&h,&ta,"GET","/admin/jobs/same");
    let a:serde_json::Value=serde_json::from_str(&a).unwrap();
    let (_,b)=user_http(&h,&tb,"GET","/admin/jobs/same");
    let b:serde_json::Value=serde_json::from_str(&b).unwrap();
    assert_ne!(a["id"],b["id"]);
    assert_eq!(a["tag"],"research");
    let (_,untagged)=user_http(&h,&tu,"GET","/admin/jobs/same");
    let untagged:serde_json::Value=serde_json::from_str(&untagged).unwrap();
    assert_eq!(untagged["tag"], "");
    assert!(untagged.get("group").is_none());
    let (_,named_default)=user_http(&h,&td,"GET","/admin/jobs/same");
    let named_default:serde_json::Value=serde_json::from_str(&named_default).unwrap();
    assert_eq!(named_default["tag"],"default");
    assert_ne!(untagged["id"],named_default["id"]);
    for foreign in [&a,&b,&named_default] {
        assert_eq!(user_http(&h,&tu,"GET",&format!("/admin/jobs/{}",foreign["id"].as_str().unwrap())).0,404);
    }
    let (_,own)=user_http(&h,&tu,"GET","/admin/jobs");
    assert_eq!(serde_json::from_str::<serde_json::Value>(&own).unwrap().as_array().unwrap().len(),1);
    assert_eq!(user_http(&h,&tb,"GET",&format!("/admin/jobs/{}",a["id"].as_str().unwrap())).0,404);
    assert_eq!(user_http(&h,&replacement,"GET","/admin/jobs/same").0,200);
    assert_eq!(user_http(&h,&ta,"GET","/admin/jobs?group=production").0,400);
    assert_eq!(user_http(&h,&ta,"GET","/admin/jobs?tag=production").0,403);
    assert_eq!(user_http(&h,&ta,"GET","/admin/jobs/same?tag=research").0,200);
    let (_,access)=user_http(&h,&replacement,"GET","/access");
    let access:serde_json::Value=serde_json::from_str(&access).unwrap();
    assert_eq!(access["namespace"], a["namespace"]);
    assert_eq!(access["tag"], "research");
    assert_eq!(access["tag_quota"]["gpus"],1.0);
    let url=format!("{}/admin/namespaces/billing/allocations",h.http);
    let claim=|id:&str,tag:&str,gpus:u32| {
        let body=serde_json::json!({"id":id,"tag":tag,"gpus":gpus,"cpus":1}).to_string();
        admin_curl(&h,&["-X","POST","-H","content-type: application/json","-d",&body,"-o","/dev/null","-w","%{http_code}",&url])
    };
    assert_eq!(claim("r1","research",1),"200");
    assert_eq!(claim("r2","research",1),"429","tag aggregate cap");
    assert_eq!(claim("r3","research",2),"422","tag per-job cap");
    assert_eq!(claim("p1","production",1),"200");
    admin_curl(&h,&["-X","PUT","-H","content-type: application/json","-d",r#"{"gpus":2,"cpus":16}"#,&format!("{}/admin/namespaces/billing/quota?tag=production",h.http)]);
    assert_eq!(claim("p2","production",1),"429","all tags share namespace cap");
    assert_eq!(claim("r1","production",1),"409","allocation cannot change tag");
    assert_eq!(claim("free","unregistered",0),"200","tags need no registration");
    assert_eq!(claim("bad","-invalid",0),"400");
    assert_eq!(user_http(&h,&ta,"POST","/admin/namespaces/billing/groups").0,404);
    let code=admin_curl(&h,&["-X","POST","-o","/dev/null","-w","%{http_code}",&format!("{}/admin/namespaces/billing/tags",h.http)]);
    assert_eq!(code,"404","tags have no independent creation endpoint");
}

#[tokio::test(flavor = "multi_thread")]
async fn jobs_inherit_app_priority_and_dispatch_highest_then_fifo() {
    let mut h = start().await;
    let elf = fleet_scheduler::blobs::elf_id_of(b"elf");
    let priority_url = format!("{}/admin/apps/priority?namespace=system&tenant=eth&elf_id={elf}", h.http);
    h.stage.generate_proof(request("low-priority", b"elf")).await.unwrap();
    assert_eq!(admin_curl(&h, &["-o", "/dev/null", "-w", "%{http_code}", "-X", "PUT", &format!("{priority_url}&priority=10")]), "200");
    h.stage.generate_proof(request("high-first", b"elf")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    h.stage.generate_proof(request("high-second", b"elf")).await.unwrap();
    // An app edit changes future submissions; pending jobs retain their snapshot.
    assert_eq!(admin_curl(&h, &["-o", "/dev/null", "-w", "%{http_code}", "-X", "PUT", &format!("{priority_url}&priority=-10")]), "200");
    let jobs: serde_json::Value = serde_json::from_str(&admin_curl(&h, &[&format!("{}/admin/jobs", h.http)])).unwrap();
    let find = |id: &str| jobs.as_array().unwrap().iter().find(|j| j["client_id"] == id).unwrap();
    assert_eq!(find("low-priority")["priority"], 0);
    assert_eq!(find("high-first")["priority"], 10);
    assert_eq!(find("high-second")["priority"], 10);
    let expected: Vec<String> = ["high-first", "high-second", "low-priority"].iter()
        .map(|id| find(id)["id"].as_str().unwrap().to_owned()).collect();
    let (_, calls) = add_node_for_app(&h, "priority-node", 1, false, "system", "eth", b"elf").await;
    wait_done(&mut h, "low-priority").await;
    assert_eq!(*calls.lock(), expected);
    assert_eq!(proof_app(&h, "eth")["priority"], -10);
}

#[tokio::test(flavor = "multi_thread")]
async fn app_management_token_can_delete_only_its_namespace_and_tenant_apps() {
    let h = start().await;
    let elf = fleet_scheduler::blobs::elf_id_of(b"owner-managed-app");
    for name in ["owner-scope", "other-scope"] {
        admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d",
            &serde_json::json!({"name": name}).to_string(), &format!("{}/admin/namespaces", h.http)]);
    }
    for (namespace, tenant) in [("owner-scope", "goat"), ("owner-scope", "eth"), ("other-scope", "goat")] {
        let code = admin_curl(&h, &["-X", "POST", "-o", "/dev/null", "-w", "%{http_code}",
            &format!("{}/admin/apps?namespace={namespace}&tenant={tenant}&elf_id={elf}", h.http)]);
        assert_eq!(code, "200");
    }
    let address = format!("{:x}", alloy_primitives::Address::from_public_key(
        k256::ecdsa::SigningKey::from_slice(&[81; 32]).unwrap().verifying_key()));
    let bearer = "81".repeat(48);
    let body = serde_json::json!({"id":"81".repeat(16), "namespace":"owner-scope", "tag":"default",
        "tenant":"goat", "address":address, "label":"namespace owner", "manage_apps":true,
        "token_hash":hex::encode(sha2::Sha256::digest(bearer.as_bytes()))});
    admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d", &body.to_string(),
        &format!("{}/admin/tokens", h.http)]);
    let own = format!("/admin/apps?namespace=owner-scope&tenant=goat&elf_id={elf}");
    let other_tenant = format!("/admin/apps?namespace=owner-scope&tenant=eth&elf_id={elf}");
    let other_namespace = format!("/admin/apps?namespace=other-scope&tenant=goat&elf_id={elf}");
    assert_eq!(user_http(&h, &bearer, "DELETE", &own).0, 200);
    assert_eq!(user_http(&h, &bearer, "DELETE", &other_tenant).0, 403);
    assert_eq!(user_http(&h, &bearer, "DELETE", &other_namespace).0, 403);
}

#[tokio::test(flavor = "multi_thread")]
async fn persistent_service_reservations_count_against_namespace_quota() {
    let h = start().await;
    let created = admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d",
        r#"{"name":"service-budget"}"#, &format!("{}/admin/namespaces", h.http)]);
    let ns: serde_json::Value = serde_json::from_str(&created).unwrap();
    let id = ns["id"].as_str().unwrap();
    let quota = serde_json::json!({"gpus": 2.0, "cpus": 8.0}).to_string();
    admin_curl(&h, &["-X", "PUT", "-H", "content-type: application/json", "-d", &quota,
        &format!("{}/admin/namespaces/{id}/quota", h.http)]);
    let service = serde_json::json!({"id":"service:ethproofs-gpu-0", "tag":"default", "gpus":1.0, "cpus":4.0});
    let status = admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d", &service.to_string(),
        "-o", "/dev/null", "-w", "%{http_code}", &format!("{}/admin/namespaces/{id}/allocations", h.http)]);
    assert_eq!(status, "200");
    let run = serde_json::json!({"id":"overflow", "tag":"default", "gpus":1.0, "cpus":5.0});
    let status = admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d", &run.to_string(),
        "-o", "/dev/null", "-w", "%{http_code}", &format!("{}/admin/namespaces/{id}/allocations", h.http)]);
    assert_eq!(status, "429", "persistent service capacity must reduce available quota");
    let status = admin_curl(&h, &["-X", "DELETE", "-o", "/dev/null", "-w", "%{http_code}",
        &format!("{}/admin/allocations/service:ethproofs-gpu-0", h.http)]);
    assert_eq!(status, "204");
    let status = admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d", &run.to_string(),
        "-o", "/dev/null", "-w", "%{http_code}", &format!("{}/admin/namespaces/{id}/allocations", h.http)]);
    assert_eq!(status, "200");
}

/// Machine execution commands are delivered on the authenticated control listener;
/// a client needs no route to the machine or Ray itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn machine_heartbeat_requires_cluster_token_and_populates_control_plane_nodes() {
    use fleet_proto::fleet::v1::{worker_service_client::WorkerServiceClient, WorkerHeartbeatRequest};
    let h = start().await;
    let mut worker = WorkerServiceClient::connect(format!("http://127.0.0.1:{}", h.control_port)).await.unwrap();
    let report = serde_json::to_vec(&serde_json::json!({
        "version": 1, "machine": "gpu-machine", "ready": true,
        "total_gpus": 8, "total_cpus": 48, "free_gpus": 8, "free_cpus": 48,
    })).unwrap();
    let heartbeat = || WorkerHeartbeatRequest { worker_id: "gpu-1".into(), session: "agent-session".into(), report_json: report.clone(), proof_fetch_ready: false };
    assert_eq!(worker.heartbeat(heartbeat()).await.unwrap_err().code(),tonic::Code::Unauthenticated);
    let mut wrong = Request::new(heartbeat());
    wrong.metadata_mut().insert("authorization", "Bearer wrong-token".parse().unwrap());
    assert_eq!(worker.heartbeat(wrong).await.unwrap_err().code(),tonic::Code::Unauthenticated);
    let mut request = Request::new(heartbeat());
    request.metadata_mut().insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
    assert_eq!(worker.heartbeat(request).await.unwrap().into_inner().heartbeat_secs,2);
    let status = admin_curl(&h,&[&format!("{}/admin/status",h.http)]);
    let status: serde_json::Value = serde_json::from_str(&status).unwrap();
    assert!(status["nodes"].as_array().unwrap().iter().any(|n| n["name"]=="gpu-1" && n["backend"]=="ray-local"));
}

#[tokio::test(flavor = "multi_thread")]
async fn integrated_app_worker_rejects_sdk_jobs_in_its_own_namespace() {
    use fleet_proto::fleet::v1::{worker_service_client::WorkerServiceClient, WorkerHeartbeatRequest};
    let mut h = start().await;
    admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d",
        r#"{"name":"alice","quota":{"gpus":2,"cpus":8}}"#, &format!("{}/admin/namespaces", h.http)]);
    let elf = fleet_scheduler::blobs::elf_id_of(b"elf");
    let (key, _, token) = namespace_token(&h, "alice", 94, &elf);
    let channel = Channel::from_shared(format!("http://127.0.0.1:{}", h.control_port)).unwrap().connect().await.unwrap();
    let mut agent = WorkerServiceClient::with_interceptor(channel, fleet_scheduler::token::Attach::new(TOKEN).unwrap());
    let report = serde_json::json!({"version":1,"machine":"test","ready":true,
        "total_gpus":2,"total_cpus":8,"free_gpus":1,"free_cpus":6,"profiles":{},
        "services":[{"name":"ethproofs","kind":"ziren-gpu","namespace":"alice",
            "proof_app":format!("eth/{elf}"),"worker_command":["/opt/eth-proofs"],
            "machine":"test","replicas":1,"gpus":1,"cpus":2,"ready":true,"disabled":false}]});
    agent.heartbeat(WorkerHeartbeatRequest {worker_id:"integrated-worker".into(),session:"session".into(),
        report_json:serde_json::to_vec(&report).unwrap(),proof_fetch_ready:false}).await.unwrap();
    let response = h.stage.generate_proof(with_bearer(signed_request("owned-stream", b"elf", &key), &token))
        .await.unwrap().into_inner();
    assert_eq!(response.status, S::InvalidParameter as i32);
    assert!(response.error_message.contains("owns its input stream"));
    let status = h.stage.get_status(with_bearer(GetStatusRequest {proof_id:"owned-stream".into()}, &token))
        .await.unwrap().into_inner();
    assert_eq!(status.status, S::InvalidParameter as i32);
    let (_, access) = user_http(&h, &token, "GET", "/access");
    assert_eq!(serde_json::from_str::<serde_json::Value>(&access).unwrap()["usage"]["gpus"], 1.0);
}
