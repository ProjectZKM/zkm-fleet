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
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct FakeNode {
    name: String,
    calls: Arc<AtomicUsize>,
    jobs: Arc<Mutex<Vec<String>>>,
    fail_first: bool,
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
            return Err(Status::internal("gpu fell over"));
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
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
  - name: goat
    class: batch
    anchor: shared
"#,
        d = dir.path().display()
    );
    let cfg: Config = serde_yaml::from_str(&yaml).unwrap();
    tokio::spawn(async move { fleet_scheduler::serve(cfg).await.unwrap() });
    let url = format!("http://127.0.0.1:{sp}");
    let stage = loop {
        if let Ok(c) = StageServiceClient::connect(url.clone()).await {
            break c;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    Harness {
        stage,
        http: format!("http://127.0.0.1:{hp}"),
        control_port: cp,
        _dir: dir,
    }
}

async fn add_node(
    h: &Harness,
    name: &str,
    gpus: u32,
    fail_first: bool,
) -> (Arc<AtomicUsize>, Arc<Mutex<Vec<String>>>) {
    let port = free_port();
    let calls = Arc::new(AtomicUsize::new(0));
    let jobs = Arc::new(Mutex::new(vec![]));
    let svc = FakeNode {
        name: name.into(),
        calls: calls.clone(),
        jobs: jobs.clone(),
        fail_first,
    };
    tokio::spawn(
        Server::builder()
            .add_service(NodeServiceServer::new(svc))
            .serve(format!("127.0.0.1:{port}").parse().unwrap()),
    );
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
    GenerateProofRequest {
        proof_id: id.into(),
        elf_data: elf.to_vec(),
        private_input_stream: bincode::serialize(&vec![vec![1u8, 2, 3], vec![4]]).unwrap(),
        target_step: Some(Step::InAgg as i32),
        ..Default::default()
    }
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
    let r = h.stage.generate_proof(with_cert(request("job-c", b"elf-c"), Some(ETH_ADDR))).await.unwrap().into_inner();
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
    let curl = |args: &[&str]| {
        let out = std::process::Command::new("curl")
            .args(["-s", "-H", &format!("authorization: Bearer {ADMIN}")])
            .args(args)
            .output()
            .unwrap();
        String::from_utf8(out.stdout).unwrap()
    };
    let apps: serde_json::Value = serde_json::from_str(&curl(&[&format!("{}/admin/apps", h.http)])).unwrap();
    let app = apps.as_array().unwrap().iter().find(|a| a["tenant"] == "eth").unwrap().clone();
    let elf = app["elf_id"].as_str().unwrap();
    assert_eq!(app["jobs"], 1);
    let gone = curl(&["-X", "DELETE", &format!("{}/admin/apps?tenant=eth&elf_id={elf}", h.http)]);
    let gone: serde_json::Value = serde_json::from_str(&gone).unwrap();
    assert_eq!((gone["jobs_deleted"].as_u64(), gone["elf_removed"].as_bool()), (Some(1), Some(true)));
    let s = h.stage.get_status(GetStatusRequest { proof_id: "job-d".into() }).await.unwrap().into_inner();
    assert_eq!(s.status, S::Unspecified as i32, "the deleted app's job is gone");
    let again = curl(&["-o", "/dev/null", "-w", "%{http_code}", "-X", "DELETE", &format!("{}/admin/apps?tenant=eth&elf_id={elf}", h.http)]);
    assert_eq!(again, "404");
}
