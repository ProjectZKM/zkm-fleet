//! The scheduler end to end, with fake prover nodes: dispatch, long-poll, public
//! values, anchoring, exclusive reservation, retry on another node, and the tokens.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status};

use fleet_proto::fleet::v1::{
    control_service_client::ControlServiceClient,
    node_service_server::{NodeService, NodeServiceServer},
    HealthRequest, HealthResponse, HeartbeatRequest, NodeInfo, ProveRequest, ProveResponse,
    RegisterRequest,
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

#[tonic::async_trait]
impl NodeService for FakeNode {
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

    async fn health(&self, _: Request<HealthRequest>) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            ready: true,
            cached_elf_ids: vec![],
            busy: false,
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

/// A scheduler with extra top-level config lines (e.g. `client_cert_identity: true`).
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

async fn add_node_delayed(h: &Harness, name: &str, gpus: u32, fail_first: bool, delay_ms: u64)
    -> (Arc<AtomicUsize>, Arc<Mutex<Vec<String>>>) {
    add_node_error(h,name,gpus,fail_first,delay_ms,tonic::Code::Internal).await
}

async fn add_node_error(h: &Harness,name: &str,gpus: u32,fail_first: bool,delay_ms: u64,failure_code: tonic::Code)
    -> (Arc<AtomicUsize>, Arc<Mutex<Vec<String>>>) {
    // Bind before advertising: an outbound connection must not steal a dropped port.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
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
    tokio::spawn(async move {
        Server::builder().add_service(NodeServiceServer::new(svc))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)).await.unwrap();
    });
    let channel = Channel::from_shared(format!("http://127.0.0.1:{}", h.control_port))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut ctl = ControlServiceClient::with_interceptor(channel, |mut r: Request<()>| {
        r.metadata_mut()
            .insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
        Ok(r)
    });
    let id = ctl
        .register(RegisterRequest {
            node: Some(NodeInfo {
                name: name.into(),
                endpoint: format!("http://127.0.0.1:{port}"),
                backend: "cuda".into(),
                gpus,
                cpus: 8,
                zkm_version: "v2.0.0".into(),
                image: String::new(),
                machine: "test".into(),
            }),
        })
        .await
        .unwrap()
        .into_inner()
        .node_id;
    tokio::spawn(async move {
        loop {
            let _ = ctl
                .heartbeat(HeartbeatRequest {
                    node_id: id.clone(),
                    ready: true,
                    ..Default::default()
                })
                .await;
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });
    tokio::time::sleep(Duration::from_millis(700)).await;
    (calls, jobs)
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
    let (a_calls, _) = add_node(&h, "gpu-a", 4, true).await;
    let (b_calls, _) = add_node(&h, "gpu-b", 1, false).await;
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
    let (_, a_jobs) = add_node(&h, "gpu-a", 4, false).await;
    let (_, b_jobs) = add_node(&h, "gpu-b", 1, false).await;
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
    let a = v["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|n| n["name"] == "gpu-a")
        .unwrap()
        .clone();
    assert_eq!(a["reserved_for"], "eth");
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
}

const ETH_ADDR: &str = "0x1111111111111111111111111111111111111111";
const GOAT_ADDR: &str = "0x2222222222222222222222222222222222222222";

fn credential(h: &Harness, tenant: &str, address: &str) {
    let out = std::process::Command::new("curl")
        .args(["-sf", "-X", "POST", "-H", &format!("authorization: Bearer {ADMIN}"), "-H", "content-type: application/json"])
        .args(["-d", &format!(r#"{{"tenant":"{tenant}","address":"{address}"}}"#), &format!("{}/admin/credentials", h.http)])
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
}

/// A request as the gateway forwards it after verifying a client certificate.
fn with_cert<T>(msg: T, cn: Option<&str>) -> Request<T> {
    let mut r = Request::new(msg);
    if let Some(cn) = cn {
        let v = format!(r#"Hash=ab12;Subject="CN={cn},O=zkm-fleet""#);
        r.metadata_mut().insert("x-forwarded-client-cert", v.parse().unwrap());
    }
    r
}

#[tokio::test(flavor = "multi_thread")]
async fn with_client_cert_identity_only_the_submitting_tenant_sees_a_proof() {
    let mut h = start_with("client_cert_identity: true").await;
    credential(&h, "eth", ETH_ADDR);
    credential(&h, "goat", GOAT_ADDR);
    add_node(&h, "gpu-a", 2, false).await;
    // unsigned requests fall to the default tenant (eth): an eth certificate may send them
    let mut unsigned = request("job-c", b"elf-c");
    unsigned.signature.clear();
    let r = h.stage.generate_proof(with_cert(unsigned, Some(ETH_ADDR))).await.unwrap().into_inner();
    assert_eq!(r.status, S::Computing as i32);
    // a goat certificate on an eth request is refused
    let mismatch = h.stage.generate_proof(with_cert(request("job-m", b"elf-m"), Some(GOAT_ADDR))).await.unwrap().into_inner();
    assert_eq!(mismatch.status, S::InvalidParameter as i32);
    // the owner sees it finish; the other tenant sees nothing; no certificate or an unknown one is refused
    let mut done = None;
    for _ in 0..40 {
        let s = h.stage.get_status(with_cert(GetStatusRequest { proof_id: "job-c".into() }, Some(ETH_ADDR))).await.unwrap().into_inner();
        if s.status != S::Computing as i32 {
            done = Some(s);
            break;
        }
    }
    let done = done.expect("job-c never finished");
    assert_eq!(done.status, S::Success as i32);
    let other = h.stage.get_status(with_cert(GetStatusRequest { proof_id: "job-c".into() }, Some(GOAT_ADDR))).await.unwrap().into_inner();
    assert_eq!(other.status, S::Unspecified as i32, "another tenant sees an unknown proof");
    assert!(other.proof_with_public_inputs.is_empty() && other.public_values_url.is_empty());
    let none = h.stage.get_status(with_cert(GetStatusRequest { proof_id: "job-c".into() }, None)).await.unwrap_err();
    assert_eq!(none.code(), tonic::Code::PermissionDenied);
    let unknown = h
        .stage
        .get_status(with_cert(GetStatusRequest { proof_id: "job-c".into() }, Some("0x3333333333333333333333333333333333333333")))
        .await
        .unwrap_err();
    assert_eq!(unknown.code(), tonic::Code::PermissionDenied);
    // revoking the credential revokes its certificate
    let out = std::process::Command::new("curl")
        .args(["-sf", "-X", "DELETE", "-H", &format!("authorization: Bearer {ADMIN}")])
        .arg(format!("{}/admin/credentials/{}", h.http, ETH_ADDR.trim_start_matches("0x")))
        .output()
        .unwrap();
    assert!(out.status.success());
    let revoked = h.stage.get_status(with_cert(GetStatusRequest { proof_id: "job-c".into() }, Some(ETH_ADDR))).await.unwrap_err();
    assert_eq!(revoked.code(), tonic::Code::PermissionDenied);
}

#[tokio::test(flavor = "multi_thread")]
async fn without_client_cert_identity_a_forged_header_is_ignored() {
    let mut h = start().await;
    add_node(&h, "gpu-a", 2, false).await;
    h.stage.generate_proof(request("job-f", b"elf-f")).await.unwrap();
    wait_done(&mut h, "job-f").await;
    let s = h
        .stage
        .get_status(with_cert(GetStatusRequest { proof_id: "job-f".into() }, Some(GOAT_ADDR)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(s.status, S::Success as i32);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_app_is_deleted_with_its_jobs_once_none_is_running() {
    let mut h = start().await;
    add_node(&h, "gpu-a", 2, false).await;
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
    add_node(&h, "gpu-a", 2, false).await;
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
    add_node(&h, "gpu-a", 2, false).await;
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
    // Another credential in the same tenant can poll/retry, but cannot rewrite audit identity.
    let key = k256::ecdsa::SigningKey::from_slice(&[8; 32]).unwrap();
    let address = format!("{:x}", alloy_primitives::Address::from_public_key(key.verifying_key()));
    credential(&h, "eth", &address);
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
async fn user_credentials_scope_apps_jobs_and_revocation_even_within_one_tenant() {
    use sha2::Digest;
    let mut h = start_with("client_cert_identity: true").await;
    add_node(&h, "gpu-user", 2, false).await;
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
        let body = serde_json::json!({"namespace": ns, "tenant": "eth", "address": addresses[i], "label": format!("user-{i}"),
            "apps": [format!("eth/{elf}"), "training"], "token_hash": hex::encode(sha2::Sha256::digest(tokens[i].as_bytes()))});
        let created = admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d", &body.to_string(),
            &format!("{}/admin/credentials", h.http)]);
        let c: serde_json::Value = serde_json::from_str(&created).unwrap();
        assert_eq!(c["apps"][1], "training");
        assert!(c.get("token_hash").is_none());
        let id = format!("user-{i}");
        let response = h.stage.generate_proof(with_cert(signed_request(&id, b"elf", &keys[i]), Some(&addresses[i]))).await.unwrap().into_inner();
        assert_eq!(response.status, S::Computing as i32, "{}", response.error_message);
    }
    let own = h.stage.get_status(with_cert(GetStatusRequest { proof_id: "user-0".into() }, Some(&addresses[0]))).await.unwrap().into_inner();
    assert_eq!(own.status, S::Success as i32);
    let foreign = h.stage.get_status(with_cert(GetStatusRequest { proof_id: "user-0".into() }, Some(&addresses[1]))).await.unwrap().into_inner();
    assert_eq!(foreign.status, S::Unspecified as i32);
    let collision = h.stage.generate_proof(with_cert(signed_request("not-granted-b", b"elf-r", &keys[1]), Some(&addresses[1]))).await.unwrap().into_inner();
    assert_eq!(collision.status, S::InvalidParameter as i32); // This user's grant does not include elf-r.
    let wrong_cert = h.stage.generate_proof(with_cert(signed_request("wrong-cert", b"elf", &keys[0]), Some(&addresses[1]))).await.unwrap().into_inner();
    assert_eq!(wrong_cert.status, S::InvalidParameter as i32);
    let unsigned = h.stage.generate_proof(with_cert(request("unsigned-user", b"elf"), Some(&addresses[0]))).await.unwrap().into_inner();
    assert_eq!(unsigned.status, S::InvalidParameter as i32);
    let denied = h.stage.generate_proof(with_cert(signed_request("not-granted", b"elf-r", &keys[0]), Some(&addresses[0]))).await.unwrap().into_inner();
    assert_eq!(denied.status, S::InvalidParameter as i32);
    assert!(denied.error_message.contains("not granted"));
    let rejection = h.stage.get_status(with_cert(GetStatusRequest { proof_id: "not-granted".into() }, Some(&addresses[0]))).await.unwrap().into_inner();
    assert_eq!(rejection.status, S::InvalidParameter as i32);
    assert_eq!(user_http(&h, &tokens[0], "GET", "/admin/jobs/user-0").0, 200);
    assert_eq!(user_http(&h, &tokens[1], "GET", "/admin/jobs/user-0").0, 404);
    assert_eq!(user_http(&h, &tokens[0], "GET", "/admin/credentials").0, 403);
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
    admin_curl(&h, &["-X", "DELETE", &format!("{}/admin/credentials/{}", h.http, addresses[0])]);
    assert_eq!(user_http(&h, &tokens[0], "GET", "/access").0, 401);
    assert_eq!(user_http(&h, &tokens[0], "GET", "/admin/jobs/user-0").0, 401);
    let revoked = h.stage.get_status(with_cert(GetStatusRequest { proof_id: "user-0".into() }, Some(&addresses[0]))).await.unwrap_err();
    assert_eq!(revoked.code(), tonic::Code::PermissionDenied);
    // A previously issued public-values capability no longer works after revocation.
    let values = std::process::Command::new("curl").args(["-s", "-o", "/dev/null", "-w", "%{http_code}", &own.public_values_url]).output().unwrap();
    assert_eq!(String::from_utf8(values.stdout).unwrap(), "404");
}

fn namespace_credential(h: &Harness, namespace: &str, seed: u8, elf_id: &str) -> (k256::ecdsa::SigningKey, String, String) {
    tag_credential(h,namespace,None,seed,elf_id)
}

#[tokio::test(flavor = "multi_thread")]
async fn apps_require_admin_created_namespaces_and_mutations_are_namespace_local() {
    let mut h = start_with("client_cert_identity: true").await;
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
    let (alice_key, alice_addr, alice_token) = namespace_credential(&h, "alice", 61, &elf);
    let (bob_key, bob_addr, bob_token) = namespace_credential(&h, "bob", 62, &elf);
    assert_eq!(user_http(&h, &alice_token, "POST", "/admin/namespaces").0, 403);
    assert_eq!(user_http(&h, &alice_token, "POST", &format!("/admin/apps?namespace=alice&tenant=eth&elf_id={elf}")).0, 403);
    let key = format!("{}/admin/apps/disabled?namespace=alice&tenant=eth&elf_id={elf}", h.http);
    admin_curl(&h, &["-X", "PUT", &key]);
    let (code, apps) = user_http(&h, &bob_token, "GET", "/admin/apps");
    assert_eq!(code, 200);
    let apps: serde_json::Value = serde_json::from_str(&apps).unwrap();
    assert_eq!(apps.as_array().unwrap().len(), 1);
    assert_eq!(apps[0]["disabled"], false);
    let denied = h.stage.generate_proof(with_cert(signed_request("disabled", b"elf", &alice_key), Some(&alice_addr))).await.unwrap().into_inner();
    assert_eq!(denied.status, S::InvalidParameter as i32);
    add_node(&h, "gpu-namespaces", 2, false).await;
    h.stage.generate_proof(with_cert(signed_request("bob-job", b"elf", &bob_key), Some(&bob_addr))).await.unwrap();
    let result = h.stage.get_status(with_cert(GetStatusRequest { proof_id:"bob-job".into() }, Some(&bob_addr))).await.unwrap().into_inner();
    assert_eq!(result.status, S::Success as i32);
    let deleted = admin_curl(&h, &["-X", "DELETE", "-o", "/dev/null", "-w", "%{http_code}",
        &format!("{}/admin/apps?namespace=alice&tenant=eth&elf_id={elf}", h.http)]);
    assert_eq!(deleted, "200");
    let result = h.stage.get_status(with_cert(GetStatusRequest { proof_id:"bob-job".into() }, Some(&bob_addr))).await.unwrap().into_inner();
    assert_eq!(result.status, S::Success as i32, "deleting Alice's registration preserves Bob's job and shared ELF");
}

fn tag_credential(h: &Harness,namespace: &str,tag: Option<&str>,seed: u8,elf_id: &str) -> (k256::ecdsa::SigningKey,String,String) {
    use sha2::Digest;
    let code = admin_curl(h, &["-X", "POST", "-o", "/dev/null", "-w", "%{http_code}",
        &format!("{}/admin/apps?namespace={namespace}&tenant=eth&elf_id={elf_id}", h.http)]);
    assert!(code == "200" || code == "409", "register namespace app: {code}");
    let key = k256::ecdsa::SigningKey::from_slice(&[seed; 32]).unwrap();
    let address = format!("{:x}", alloy_primitives::Address::from_public_key(key.verifying_key()));
    let token = format!("{seed:02x}").repeat(48);
    let body = serde_json::json!({"namespace": namespace, "tag":tag, "tenant": "eth", "address": address,
        "apps": [format!("eth/{elf_id}"), "training"], "label": namespace,
        "token_hash": hex::encode(sha2::Sha256::digest(token.as_bytes()))});
    let created = admin_curl(h, &["-X", "POST", "-H", "content-type: application/json", "-d", &body.to_string(),
        &format!("{}/admin/credentials", h.http)]);
    let c: serde_json::Value = serde_json::from_str(&created).unwrap();
    assert!(c["namespace"].is_string(), "{created}");
    (key, address, token)
}

#[tokio::test(flavor = "multi_thread")]
async fn namespace_rotation_preserves_jobs_and_suspension_blocks_every_key() {
    let mut h = start_with("client_cert_identity: true").await;
    let (_, node_jobs) = add_node(&h, "gpu-ns", 2, false).await;
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
    let (old_key, old_addr, old_token) = namespace_credential(&h, "alice", 21, &elf);
    let (bob_key, bob_addr, bob_token) = namespace_credential(&h, "bob", 22, &elf);
    for (key, address) in [(&old_key, &old_addr), (&bob_key, &bob_addr)] {
        let r = h.stage.generate_proof(with_cert(signed_request("same-id", b"elf", key), Some(address))).await.unwrap().into_inner();
        assert_eq!(r.proof_id, "same-id");
        assert_eq!(r.status, S::Computing as i32);
    }
    let alice = h.stage.get_status(with_cert(GetStatusRequest { proof_id: "same-id".into() }, Some(&old_addr))).await.unwrap().into_inner();
    let bob = h.stage.get_status(with_cert(GetStatusRequest { proof_id: "same-id".into() }, Some(&bob_addr))).await.unwrap().into_inner();
    assert_eq!(alice.status, S::Success as i32);
    assert_eq!(bob.status, S::Success as i32);
    assert_ne!(alice.public_values_url, bob.public_values_url);
    let jobs = node_jobs.lock().clone();
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
    let (new_key, new_addr, new_token) = namespace_credential(&h, "alice", 23, &elf);
    let repeated = h.stage.generate_proof(with_cert(signed_request("same-id", b"elf", &new_key), Some(&new_addr))).await.unwrap().into_inner();
    assert_eq!(repeated.status, S::Computing as i32);
    assert_eq!(node_jobs.lock().len(), 2, "rotation does not queue a duplicate proof");
    admin_curl(&h, &["-X", "DELETE", &format!("{}/admin/credentials/{old_addr}", h.http)]);
    assert_eq!(user_http(&h, &old_token, "GET", "/access").0, 401);
    let (_, rotated_json) = user_http(&h, &new_token, "GET", "/admin/jobs/same-id");
    let rotated: serde_json::Value = serde_json::from_str(&rotated_json).unwrap();
    assert_eq!(rotated["id"], a["id"]);
    assert_eq!(rotated["owner"], old_addr, "the original submitter remains in the audit record");
    let rotated = h.stage.get_status(with_cert(GetStatusRequest { proof_id: "same-id".into() }, Some(&new_addr))).await.unwrap().into_inner();
    assert_eq!(rotated.status, S::Success as i32);
    for (url, expected) in [(&alice.public_values_url, "404"), (&rotated.public_values_url, "200")] {
        let out = std::process::Command::new("curl").args(["-s", "-o", "/dev/null", "-w", "%{http_code}", url]).output().unwrap();
        assert_eq!(String::from_utf8(out.stdout).unwrap(), expected);
    }
    let admin_a = admin_curl(&h, &[&format!("{}/admin/jobs/same-id?namespace=alice", h.http)]);
    assert_eq!(serde_json::from_str::<serde_json::Value>(&admin_a).unwrap()["id"], a["id"]);
    let (_, _, second_token) = namespace_credential(&h, "alice", 24, &elf);
    let operator_address = "cd".repeat(20);
    admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d",
        &serde_json::json!({"tenant":"eth", "address":operator_address}).to_string(),
        &format!("{}/admin/credentials", h.http)]);
    let operator_read = h.stage.get_status(with_cert(GetStatusRequest { proof_id: a["id"].as_str().unwrap().into() }, Some(&operator_address))).await.unwrap().into_inner();
    assert_eq!(operator_read.status, S::Unspecified as i32, "tenant-wide operator access cannot read a user namespace");
    let denied = user_http(&h, &new_token, "POST", "/admin/namespaces");
    assert_eq!(denied.0, 403);
    admin_curl(&h, &["-X", "PUT", &format!("{}/admin/namespaces/alice/suspended", h.http)]);
    assert_eq!(user_http(&h, &new_token, "GET", "/access").0, 401);
    assert_eq!(user_http(&h, &second_token, "GET", "/access").0, 401);
    assert_eq!(user_http(&h, &bob_token, "GET", "/access").0, 200);
    assert_eq!(h.stage.get_status(with_cert(GetStatusRequest { proof_id: "same-id".into() }, Some(&new_addr))).await.unwrap_err().code(), tonic::Code::PermissionDenied);
    admin_curl(&h, &["-X", "DELETE", &format!("{}/admin/namespaces/alice/suspended", h.http)]);
    assert_eq!(user_http(&h, &new_token, "GET", "/access").0, 200);
    assert_eq!(user_http(&h, &old_token, "GET", "/access").0, 401);
}


#[tokio::test(flavor = "multi_thread")]
async fn namespace_resource_quota_is_shared_by_proofs_runs_and_rotated_keys() {
    let mut h = start_with("client_cert_identity: true").await;
    let (calls, _) = add_node_delayed(&h, "gpu-quota", 2, false, 1000).await;
    for name in ["alice", "bob"] {
        let body = serde_json::json!({"name":name,"quota":{"gpus":2,"cpus":8}}).to_string();
        admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d", &body,
            &format!("{}/admin/namespaces", h.http)]);
    }
    let elf = fleet_scheduler::blobs::elf_id_of(b"elf");
    let (key, address, token) = namespace_credential(&h, "alice", 31, &elf);
    let (_, _, replacement_token) = namespace_credential(&h, "alice", 32, &elf);
    let allocation_url = format!("{}/admin/namespaces/alice/allocations", h.http);
    let held = serde_json::json!({"id":"held-run","gpus":2,"cpus":8}).to_string();
    let held_reply = admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d", &held, &allocation_url]);
    assert!(serde_json::from_str::<serde_json::Value>(&held_reply).unwrap()["id"].is_string());
    let overflow = serde_json::json!({"id":"another-run","gpus":0,"cpus":1}).to_string();
    let code = admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d", &overflow,
        "-o", "/dev/null", "-w", "%{http_code}", &allocation_url]);
    assert_eq!(code, "429");
    let r = h.stage.generate_proof(with_cert(signed_request("proof-quota", b"elf", &key), Some(&address))).await.unwrap().into_inner();
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
    let done = h.stage.get_status(with_cert(GetStatusRequest { proof_id:"proof-quota".into() }, Some(&address))).await.unwrap().into_inner();
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
    let mut h = start_with("client_cert_identity: true").await;
    let (original,_) = add_node_error(&h,"original",2,true,100,tonic::Code::Unavailable).await;
    let (other,_) = add_node(&h,"other",1,false).await;
    let created = admin_curl(&h,&["-X","POST","-H","content-type: application/json","-d",
        r#"{"name":"alice","quota":{"gpus":2,"cpus":8}}"#,&format!("{}/admin/namespaces",h.http)]);
    assert!(created.contains("alice"));
    let elf=fleet_scheduler::blobs::elf_id_of(b"elf");
    let (key,address,token)=namespace_credential(&h,"alice",41,&elf);
    h.stage.generate_proof(with_cert(signed_request("disconnected",b"elf",&key),Some(&address))).await.unwrap();
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
    let done=h.stage.get_status(with_cert(GetStatusRequest{proof_id:"disconnected".into()},Some(&address))).await.unwrap().into_inner();
    assert_eq!(done.status,S::Success as i32);
    assert_eq!(original.load(Ordering::SeqCst),2);
    assert_eq!(other.load(Ordering::SeqCst),0);
}

#[tokio::test(flavor = "multi_thread")]
async fn tags_isolate_proofs_and_enforce_both_resource_caps() {
    let mut h=start_with("client_cert_identity: true").await;
    add_node(&h,"tag-prover",1,false).await;
    admin_curl(&h,&["-X","POST","-H","content-type: application/json","-d",r#"{"name":"billing","quota":{"gpus":2,"cpus":16}}"#,&format!("{}/admin/namespaces",h.http)]);
    let elf=fleet_scheduler::blobs::elf_id_of(b"elf");
    let (ka,aa,ta)=tag_credential(&h,"billing",Some("research"),51,&elf);
    let (kb,ab,tb)=tag_credential(&h,"billing",Some("production"),52,&elf);
    let (_,_,replacement)=tag_credential(&h,"billing",Some("research"),53,&elf);
    let listing=admin_curl(&h,&[&format!("{}/admin/namespaces/billing",h.http)]);
    let listing:serde_json::Value=serde_json::from_str(&listing).unwrap();
    assert_eq!(listing["tags"].as_array().unwrap().len(),2);
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
    let (ku,au,tu)=namespace_credential(&h,"billing",54,&elf);
    let (kd,ad,td)=tag_credential(&h,"billing",Some("default"),55,&elf);
    for (k,a) in [(&ka,&aa),(&kb,&ab),(&ku,&au),(&kd,&ad)] {
        h.stage.generate_proof(with_cert(signed_request("same",b"elf",k),Some(a))).await.unwrap();
        assert_eq!(h.stage.get_status(with_cert(GetStatusRequest{proof_id:"same".into()},Some(a))).await.unwrap().into_inner().status,S::Success as i32);
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
    let (_, calls) = add_node(&h, "priority-node", 1, false).await;
    wait_done(&mut h, "low-priority").await;
    assert_eq!(*calls.lock(), expected);
    assert_eq!(proof_app(&h, "eth")["priority"], -10);
}
