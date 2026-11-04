//! The scheduler end to end, with fake prover nodes: jobs submitted to an app over HTTP with
//! their inputs in app storage, dispatch, outputs served through the app's job route,
//! anchoring, exclusive reservation, retry on another node, and the tokens.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use prost::Message;
use sha2::Digest;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use fleet_proto::artifacts;
use fleet_proto::fleet::v1::{
    control_service_client::ControlServiceClient,
    ArtifactRef, Assignment, Completion, HeartbeatRequest, NodeInfo, ProofInputs, ProofKind, ProveRequest, ProveResponse, RegisterRequest,
};
use fleet_proto::sdk::SubmitProofRequest;
use fleet_proto::stage::v1::{Status as S, Step};
use fleet_scheduler::config::SchedulerConfig;

const TOKEN: &str = "0123456789abcdef0123456789abcdef-cluster";
/// The default namespace's token (read, submit, manage; tag `scope=default`). Its signer is
/// `operator_key`.
const USER: &str = "757575757575757575757575757575757575757575757575757575757575757575757575757575757575757575757575";
/// The namespace the tests' default apps and token are in.
const PROOFS: &str = "proofs";

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
            return Err(Status::new(self.failure_code,"gpu fell over or response was lost",
            ));
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
    http: String,
    control_port: u16,
    /// The administrator's token the in-memory scheduler issued itself.
    admin: String,
    /// App storage: the tests' submissions write their inputs here, and provers their outputs.
    storage: PathBuf,
    client: reqwest::Client,
    _dir: tempfile::TempDir,
}

async fn start() -> Harness {
    start_with("").await
}

/// A scheduler with extra top-level config lines.
async fn start_with(extra: &str) -> Harness {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("cluster.token"), TOKEN).unwrap();
    let (cp, hp) = (free_port(), free_port());
    let yaml = format!(
        r#"
control_addr: 127.0.0.1:{cp}
http_addr: 127.0.0.1:{hp}
data_dir: {d}/data
heartbeat_secs: 1
max_attempts: 2
cluster_token_file: {d}/cluster.token
{extra}
"#,
        d = dir.path().display(),
    );
    let cfg: SchedulerConfig = serde_yaml::from_str(&yaml).unwrap();
    tokio::spawn(async move { fleet_scheduler::serve(cfg).await.unwrap() });
    let admin_file = dir.path().join("data/admin.token");
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let admin = loop {
        match std::fs::read_to_string(&admin_file) {
            Ok(t) if !t.trim().is_empty() => break t.trim().to_string(),
            _ => {
                assert!(std::time::Instant::now() < deadline, "the scheduler issued no admin token");
                tokio::time::sleep(Duration::from_millis(20)).await
            }
        }
    };
    let storage = dir.path().canonicalize().unwrap().join("storage");
    std::fs::create_dir_all(&storage).unwrap();
    let h = Harness { http: format!("http://127.0.0.1:{hp}"), control_port: cp, admin, storage, client: reqwest::Client::new(), _dir: dir };
    while admin_curl(&h, &["-o", "/dev/null", "-w", "%{http_code}", &format!("{}/healthz", h.http)]) != "200" {
        assert!(std::time::Instant::now() < deadline, "the scheduler's HTTP API never came up");
        tokio::time::sleep(Duration::from_millis(50)).await; // the http port is not up yet
    }
    create_namespace(&h, PROOFS, None);
    let body = serde_json::json!({"id": "75".repeat(16), "namespace": PROOFS, "tag": "scope=default", "address": operator_address(),
        "label": "default", "permissions": ["read", "submit", "manage"], "token_hash": hex::encode(sha2::Sha256::digest(USER.as_bytes()))});
    let created = admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d", &body.to_string(), &format!("{}/tokens", h.http)]);
    assert!(created.contains(PROOFS) || created.contains("\"namespace\""), "default token: {created}");
    // a job runs only for a registered app: the tests' programs, in the default namespace
    for elf in TEST_ELFS {
        register(&h, PROOFS, elf);
    }
    h
}

/// The programs the tests submit (registered in the default namespace by `start_with`).
const TEST_ELFS: &[&[u8]] =
    &[b"elf-one", b"something else", b"elf", b"elf-r", b"eth-guest", b"elf-c", b"elf-m", b"elf-f", b"elf-d", b"elf-e",
];

/// The tests' name for the app of program `elf_id` in `namespace`: app names are unique across
/// the fleet.
fn app_in(namespace: &str, elf_id: &str) -> String {
    format!("{namespace}-{}", &elf_id[..8])
}

/// The tests' name for the app of program `elf_id` in the default namespace.
fn app_named(elf_id: &str) -> String {
    app_in(PROOFS, elf_id)
}

fn app_of(elf: &[u8]) -> String {
    app_named(&artifacts::hash(elf))
}

/// Creates a namespace (as an administrator), with a quota when given.
fn create_namespace(h: &Harness, name: &str, quota: Option<serde_json::Value>) -> serde_json::Value {
    let mut body = serde_json::json!({"name": name});
    if let Some(quota) = quota { body["quota"] = quota; }
    let created = admin_curl(h, &["-X", "POST", "-H", "content-type: application/json", "-d", &body.to_string(), &format!("{}/namespaces", h.http)]);
    serde_json::from_str(&created).unwrap_or(serde_json::Value::Null)
}

/// Registers the program as an app of the namespace (as fleetctl does), with the policy the
/// tests' provers expect: the biggest prover first, the program kept on its prover.
fn register(h: &Harness, namespace: &str, elf: &[u8]) -> String {
    let id = artifacts::hash(elf);
    assert_eq!(register_id(h, namespace, &id), "201", "registering {}", app_in(namespace, &id));
    id
}

/// Registers program `elf_id` as an app of the namespace; the HTTP status.
fn register_id(h: &Harness, namespace: &str, elf_id: &str) -> String {
    let body = serde_json::json!({"name": app_in(namespace, elf_id), "spec": {"image": "ziren-gpu:test"},
        "proof": {"elf_id": elf_id, "class": "latency", "anchor": "exclusive"}});
    admin_curl(h, &["-o", "/dev/null", "-w", "%{http_code}", "-X", "POST", "-H", "content-type: application/json",
        "-d", &body.to_string(), &format!("{}/apps?namespace={namespace}", h.http)])
}

async fn add_node(
    h: &Harness,
    name: &str,
    gpus: u32,
    fail_first: bool,
) -> (Arc<AtomicUsize>, Arc<Mutex<Vec<String>>>) {
    add_node_app(h, name, gpus, fail_first, 150, tonic::Code::Internal, PROOFS, b"elf-one").await
}

/// A prover of the namespace's app of program `elf`.
async fn add_node_for_app(h: &Harness, name: &str, gpus: u32, fail_first: bool,
    namespace: &str, elf: &[u8],
)
    -> (Arc<AtomicUsize>, Arc<Mutex<Vec<String>>>) {
    add_node_app(h, name, gpus, fail_first, 150, tonic::Code::Internal, namespace, elf,
    ).await
}

#[allow(clippy::too_many_arguments)]
async fn add_node_app(h: &Harness, name: &str, gpus: u32, fail_first: bool, delay_ms: u64,
    failure_code: tonic::Code, namespace: &str, elf: &[u8],
)
    -> (Arc<AtomicUsize>, Arc<Mutex<Vec<String>>>) {
    let (calls, jobs, _) = add_node_app_session(h, name, gpus, fail_first, delay_ms, failure_code, namespace, elf,
    ).await;
    (calls, jobs)
}

/// A fake prover: it reads each assignment's input and ELF from app storage (the parent of its
/// output root) and writes its outputs there, reporting them by reference.
#[allow(clippy::too_many_arguments)]
async fn add_node_app_session(h: &Harness, name: &str, gpus: u32, fail_first: bool, delay_ms: u64,
    failure_code: tonic::Code, namespace: &str, elf: &[u8],
)
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
    let body = admin_curl(h, &[&format!("{}/namespaces/{namespace}", h.http)]);
    let elf_id = artifacts::hash(elf);
    let app = app_in(namespace, &elf_id);
    let namespace = serde_json::from_str::<serde_json::Value>(&body).unwrap()["id"].as_str().unwrap().to_string();
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
                app,
                app_elf_id: elf_id,
            }),
        })
        .await
        .unwrap()
        .into_inner()
        .node_id;
    let session_id = id.clone();
    tokio::spawn(async move {
        let mut active: Option<(Assignment, tokio::task::JoinHandle<Result<Response<ProveResponse>, Status>>,
        )> = None;
        let mut completed: Option<Completion> = None;
        let svc = Arc::new(svc);
        loop {
            if active.as_ref().is_some_and(|(_, t)| t.is_finished()) {
                let (assignment, task) = active.take().unwrap();
                let result = task.await.unwrap();
                completed = Some(match result {
                    Ok(result) => {
                        let result = result.into_inner();
                        let inputs = assignment.inputs.as_ref().expect("every assignment has app-storage inputs");
                        let root = std::path::Path::new(inputs.output_root.strip_prefix("file://").unwrap());
                        let output = fleet_ziren_node::output::Output::new(root.into()).unwrap();
                        output.publish(&assignment, &result).unwrap();
                        let outputs = Some(output.references(&assignment).unwrap());
                        Completion { assignment: Some(assignment), outputs, cycles: result.cycles, prove_ms: result.prove_ms, ..Default::default() }
                    }
                    Err(e) => Completion { assignment: Some(assignment), error_code: e.code() as i32, error: e.message().into(), ..Default::default() },
                });
            }
            let running_job = active.as_ref().map(|(a, _)| a.job_id.clone())
                .or_else(|| {
                    completed.as_ref().and_then(|c| c.assignment.as_ref().map(|a| a.job_id.clone()))
                }).unwrap_or_default();
            if let Ok(response) = ctl.heartbeat(HeartbeatRequest { node_id: id.clone(), ready: true,
                busy: !running_job.is_empty(), running_job, completion: completed.clone(), ..Default::default() }).await {
                let response = response.into_inner();
                if completed.as_ref().is_some_and(|c| {
                    c.assignment.as_ref().unwrap().lease == response.completed_lease
                }) { completed = None; }
                if active.is_none() && completed.is_none() {
                    if let Some(assignment) = response.assignment {
                        let refs = assignment.inputs.as_ref().expect("every assignment has app-storage inputs");
                        let root = std::path::Path::new(refs.output_root.strip_prefix("file://").unwrap(),
                        ).parent().unwrap();
                        let bytes = artifacts::read(refs.input.as_ref().unwrap(), root, artifacts::MAX_INPUT).unwrap();
                        let mut inputs = ProveRequest::decode(bytes.as_slice()).unwrap();
                        inputs.job_id = assignment.job_id.clone();
                        inputs.elf = artifacts::read(refs.elf.as_ref().unwrap(), root, artifacts::MAX_INPUT).unwrap();
                        let worker = svc.clone();
                        let mut req = Request::new(inputs);
                        req.metadata_mut().insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
                        active = Some((assignment, tokio::spawn(async move { worker.prove(req).await }),
                        ));
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
    tokio::time::sleep(Duration::from_millis(700)).await;
    (calls, jobs, session_id)
}

/// The stdin of the tests' proofs: two guest reads.
fn stdin() -> Vec<Vec<u8>> {
    vec![vec![1, 2, 3], vec![4]]
}

/// A submission of program `elf` as proof `id` (to Compressed), signed by the default token's
/// signer.
fn request(h: &Harness, id: &str, elf: &[u8]) -> SubmitProofRequest {
    signed_request(h, id, elf, &operator_key())
}

fn signed_request(h: &Harness, id: &str, elf: &[u8], key: &k256::ecdsa::SigningKey) -> SubmitProofRequest {
    request_with(h, id, elf, stdin(), key)
}

/// As an app's endpoint does: the input (a `ProveRequest`) and the ELF are written to app
/// storage, and the submission names them, and the output root beside them.
fn request_with(h: &Harness, id: &str, elf: &[u8], stdin_buffer: Vec<Vec<u8>>, key: &k256::ecdsa::SigningKey,
) -> SubmitProofRequest {
    let elf_id = artifacts::hash(elf);
    let input = ProveRequest { job_id: id.into(), elf_id: elf_id.clone(), stdin_buffer, kind: ProofKind::Compressed as i32,
        ..Default::default() };
    let mut req = SubmitProofRequest {
        proof_id: id.into(),
        elf_id,
        target_step: Some(Step::InAgg as i32),
        inputs: ProofInputs { input: Some(store(h, "inputs", &input.encode_to_vec())), elf: Some(store(h, "elf", elf)),
            output_root: artifacts::file_uri(&h.storage.join("outputs")).unwrap() },
        ..Default::default()
    };
    req.signature = signature(&req.proof_id, req.block_no, req.seg_size, key);
    req
}

/// Content-addressed, as an app's endpoint stores its files: written once and whole (a
/// temporary file renamed into place), since a prover may be reading the same file for an
/// earlier submission of the same input.
fn store(h: &Harness, dir: &str, bytes: &[u8]) -> ArtifactRef {
    let path = h.storage.join(dir).join(artifacts::hash(bytes));
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    if !path.exists() {
        let temp = path.with_extension(uuid::Uuid::new_v4().to_string());
        std::fs::write(&temp, bytes).unwrap();
        std::fs::rename(temp, &path).unwrap();
    }
    artifacts::reference(&path, bytes).unwrap()
}

fn signature(proof_id: &str, block_no: Option<u64>, seg_size: u32, key: &k256::ecdsa::SigningKey) -> String {
    let msg = fleet_scheduler::token::signed_message(proof_id, block_no, seg_size);
    let hash = alloy_primitives::eip191_hash_message(msg.as_bytes());
    let (sig, recid) = key.sign_prehash_recoverable(hash.as_slice()).unwrap();
    let mut bytes = sig.to_bytes().to_vec();
    bytes.push(27 + recid.to_byte());
    hex::encode(bytes)
}

fn operator_key() -> k256::ecdsa::SigningKey {
    k256::ecdsa::SigningKey::from_slice(&[7; 32]).unwrap()
}

fn operator_address() -> String {
    format!("{:x}", alloy_primitives::Address::from_public_key(operator_key().verifying_key()))
}

/// `POST /apps/{app}/jobs` as `token`: the HTTP status and the body (JSON when it is).
async fn post_job(h: &Harness, token: &str, app: &str, req: &SubmitProofRequest) -> (u16, serde_json::Value) {
    let r = h.client.post(format!("{}/apps/{app}/jobs", h.http)).bearer_auth(token).json(req).send().await.unwrap();
    let code = r.status().as_u16();
    let text = r.text().await.unwrap();
    (code, serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text)))
}

/// Submits to `app` as `token`: the submission's stage status and error message.
async fn submit(h: &Harness, token: &str, app: &str, req: &SubmitProofRequest) -> (i32, String) {
    let (code, v) = post_job(h, token, app, req).await;
    assert_eq!(code, 200, "submitting {} to {app}: {v}", req.proof_id);
    assert_eq!(v["proof_id"], req.proof_id.as_str());
    (v["status"].as_i64().unwrap() as i32, v["error_message"].as_str().unwrap().into())
}

/// Submits as the default token, to its program's app in the default namespace.
async fn submit_default(h: &Harness, req: &SubmitProofRequest) -> (i32, String) {
    submit(h, USER, &app_named(&req.elf_id), req).await
}

/// `GET /apps/{app}/jobs/{id}` as `token`: the HTTP status and the job.
async fn get_job(h: &Harness, token: &str, app: &str, id: &str) -> (u16, serde_json::Value) {
    let r = h.client.get(format!("{}/apps/{app}/jobs/{id}", h.http)).bearer_auth(token).send().await.unwrap();
    let code = r.status().as_u16();
    let text = r.text().await.unwrap();
    (code, serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text)))
}

/// The job once it has succeeded or failed.
async fn wait_done(h: &Harness, token: &str, app: &str, id: &str) -> serde_json::Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let (code, job) = get_job(h, token, app, id).await;
        if code == 200 && matches!(job["state"].as_str(), Some("succeeded" | "failed")) {
            return job;
        }
        assert!(tokio::time::Instant::now() < deadline, "job {id} never finished: {code} {job}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The default token's job of program `elf`, once done.
async fn done(h: &Harness, id: &str, elf: &[u8]) -> serde_json::Value {
    wait_done(h, USER, &app_of(elf), id).await
}

/// The job's output (`proof` or `public_values`), read from app storage by its reference.
fn output(job: &serde_json::Value, name: &str) -> Vec<u8> {
    let uri = job["outputs"][name]["uri"].as_str().unwrap_or_else(|| panic!("no {name} output: {job}"));
    std::fs::read(uri.strip_prefix("file://").unwrap()).unwrap()
}

fn assert_refused(job: &serde_json::Value) {
    assert_eq!((job["state"].as_str(), job["fail_status"].as_str()), (Some("failed"), Some("invalid_parameter")), "{job}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_job_runs_end_to_end_and_its_outputs_are_served_through_its_app() {
    let h = start().await;
    add_node(&h, "gpu-a", 2, false).await;
    let app = app_of(b"elf-one");
    let req = request(&h, "job-1", b"elf-one");
    assert_eq!(submit(&h, USER, &app, &req).await, (S::Computing as i32, String::new()));
    let job = done(&h, "job-1", b"elf-one").await;
    assert_eq!(job["state"], "succeeded", "{job}");
    assert_eq!(output(&job, "proof"), br#"{"node":"gpu-a","stdin":2}"#);
    assert_eq!((job["cycles"].as_u64(), job["prove_ms"].as_u64()), (Some(1234), Some(100)));
    assert_eq!(job["outputs"]["machine"], "test");
    for path in [format!("/apps/{app}/jobs/job-1?namespace={PROOFS}"), format!("/apps/{app}/jobs?namespace={PROOFS}")] {
        let view:serde_json::Value=serde_json::from_str(&admin_curl(&h,&[&format!("{}{path}",h.http)])).unwrap();
        let job=if view.is_array() {&view[0]} else {&view};
        assert_eq!(job["node"],"test","HTTP exposes the machine, not the proof replica");
        assert_eq!(job["diagnostics"]["execution_replica"],"gpu-a");
    }
    // Relayed from the prover's machine, by its fetch agent.
    let fetch_agent = start_fetch_agent(&h, h.storage.clone()).await;
    for (name, expected) in [("proof.json", br#"{"node":"gpu-a","stdin":2}"#.to_vec()), ("public_values.bin", vec![7, 7, 7])] {
        let r = h.client.get(format!("{}/apps/{app}/jobs/job-1/output/{name}", h.http)).bearer_auth(USER).send().await.unwrap();
        assert_eq!(r.status().as_u16(), 200, "{name}");
        assert_eq!(r.bytes().await.unwrap().as_ref(), expected.as_slice(), "{name}");
    }
    assert_eq!(user_http(&h, USER, "GET", &format!("/apps/{app}/jobs/job-1/output/stdin.bin")).0, 404);
    assert_eq!(submit(&h, USER, &app, &req).await.0, S::Computing as i32, "an identical resubmission is idempotent");
    let (status, error) = submit_default(&h, &request(&h, "job-1", b"something else")).await;
    assert_eq!(status, S::InvalidParameter as i32, "a known proof id with other inputs");
    assert!(error.contains("different inputs"), "{error}");
    assert_eq!(done(&h, "job-1", b"elf-one").await["outputs"], job["outputs"], "the refusal leaves the job as it was");
    fetch_agent.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn rejections_are_recorded_as_failed_jobs() {
    let h = start().await;
    let app = app_of(b"elf");
    let mut req = request(&h, "job-x", b"");
    req.elf_id = "ab".repeat(32);
    req.inputs.elf.as_mut().unwrap().sha256 = req.elf_id.clone();
    let (status, error) = submit(&h, USER, &app, &req).await;
    assert_eq!(status, S::InvalidParameter as i32);
    assert!(error.contains("not an app of this namespace"), "{error}");
    assert_refused(&wait_done(&h, USER, &app, "job-x").await);
    let mut bad = request(&h, "job-y", b"elf");
    bad.target_step = Some(Step::InSplit as i32);
    assert_eq!(submit(&h, USER, &app, &bad).await.0, S::InvalidParameter as i32);
    assert_refused(&wait_done(&h, USER, &app, "job-y").await);
    // The ELF a submission names must be the program's.
    let mut other = request(&h, "job-z", b"elf");
    other.inputs.elf = Some(store(&h, "elf", b"elf-r"));
    let (status, error) = submit(&h, USER, &app, &other).await;
    assert_eq!(status, S::InvalidParameter as i32);
    assert!(error.contains("ELF hash does not match"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_attempt_moves_to_another_node() {
    let h = start().await;
    let (a_calls, _) = add_node_for_app(&h, "gpu-a", 4, true, PROOFS, b"elf-r").await;
    let (b_calls, _) = add_node_for_app(&h, "gpu-b", 1, false, PROOFS, b"elf-r").await;
    assert_eq!(submit_default(&h, &request(&h, "job-r", b"elf-r")).await.0, S::Computing as i32);
    let job = done(&h, "job-r", b"elf-r").await;
    assert_eq!(job["state"], "succeeded", "{job}");
    assert!(String::from_utf8(output(&job, "proof")).unwrap().contains("gpu-b"));
    assert_eq!(a_calls.load(Ordering::SeqCst), 1);
    assert_eq!(b_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_exclusive_anchor_keeps_the_program_and_keeps_others_off_the_node() {
    let h = start().await;
    let (_, a_jobs) = add_node_for_app(&h, "gpu-a", 4, false, PROOFS, b"eth-guest").await;
    let (_, b_jobs) = add_node_for_app(&h, "gpu-b", 1, false, PROOFS, b"eth-guest").await;
    for i in 0..3 {
        let id = format!("eth-{i}");
        submit_default(&h, &request(&h, &id, b"eth-guest")).await;
        assert_eq!(done(&h, &id, b"eth-guest").await["state"], "succeeded");
    }
    assert_eq!(
        a_jobs.lock().len(),
        3,
        "a latency app anchored on the larger node"
    );
    let st = std::process::Command::new("curl")
        .args([
            "-sf",
            "-H",
            &format!("authorization: Bearer {}", h.admin),
            &format!("{}/status", h.http),
        ])
        .output()
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&st.stdout).unwrap();
    assert!(v["nodes"].as_array().unwrap().is_empty(), "proof replicas do not create machine node records");
    assert!(v["anchors"].as_array().unwrap().iter().any(|a| a["app"] == app_of(b"eth-guest") && a["node"] == "gpu-a"));
    let unauth = std::process::Command::new("curl")
        .args([
            "-s",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            &format!("{}/status", h.http),
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
    let err = ctl.heartbeat(HeartbeatRequest::default()).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}

fn with_bearer<T>(msg: T, token: &str) -> Request<T> {
    let mut r = Request::new(msg);
    r.metadata_mut().insert("authorization", format!("Bearer {token}").parse().unwrap());
    r
}

#[tokio::test(flavor = "multi_thread")]
async fn an_app_is_deleted_with_its_jobs_once_none_is_running() {
    let h = start().await;
    add_node_for_app(&h, "gpu-a", 2, false, PROOFS, b"elf-d").await;
    submit_default(&h, &request(&h, "job-d", b"elf-d")).await;
    assert_eq!(done(&h, "job-d", b"elf-d").await["state"], "succeeded");
    let app = registered_app(&h, b"elf-d");
    assert_eq!((app["proof"].is_object(), app["jobs"]["total"].as_u64()), (true, Some(1)));
    let url = format!("{}/apps/{}?namespace={PROOFS}", h.http, app_of(b"elf-d"));
    let gone = admin_curl(&h, &["-X", "DELETE", &url]);
    let gone: serde_json::Value = serde_json::from_str(&gone).unwrap();
    assert_eq!((gone["app"].as_str(), gone["jobs_deleted"].as_u64()), (Some(app_of(b"elf-d").as_str()), Some(1)), "{gone}");
    assert_eq!(get_job(&h, USER, &app_of(b"elf-d"), "job-d").await.0, 404, "the deleted app's job is gone");
    let again = admin_curl(&h, &["-o", "/dev/null", "-w", "%{http_code}", "-X", "DELETE", &url]);
    assert_eq!(again, "404");
}

/// The API through curl as an administrator (what fleetctl does), as text.
fn admin_curl(h: &Harness, args: &[&str]) -> String {
    let out = std::process::Command::new("curl")
        .args(["-s", "--max-time", "10", "-H", &format!("authorization: Bearer {}", h.admin),
        ])
        .args(args)
        .output()
        .unwrap();
    String::from_utf8(out.stdout).unwrap()
}

/// The default namespace's app of program `elf`, as the API describes it.
fn registered_app(h: &Harness, elf: &[u8]) -> serde_json::Value {
    serde_json::from_str(&admin_curl(h, &[&format!("{}/apps/{}?namespace={PROOFS}", h.http, app_of(elf))])).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_job_runs_only_for_a_registered_app() {
    let h = start().await;
    add_node_for_app(&h, "gpu-a", 2, false, PROOFS, b"unregistered").await;
    let id = artifacts::hash(b"unregistered");
    let req = request(&h, "job-u1", b"unregistered");
    assert_eq!(post_job(&h, USER, &app_of(b"unregistered"), &req).await.0, 404, "no such app");
    let (status, error) = submit(&h, USER, &app_of(b"elf"), &req).await;
    assert_eq!(status, S::InvalidParameter as i32, "refused: not an app");
    assert!(error.contains("not an app of this namespace"), "{error}");
    assert_refused(&done(&h, "job-u1", b"elf").await);
    let apps = admin_curl(&h, &[&format!("{}/apps", h.http)]);
    assert!(!apps.contains(&id), "the refusal registered nothing");
    assert_eq!(register(&h, PROOFS, b"unregistered"), id);
    let app = registered_app(&h, b"unregistered");
    assert_eq!((app["proof"]["elf_id"].as_str(), app["jobs"]["total"].as_u64(), app["disabled"].as_bool(), app["state"].as_str()),
        (Some(id.as_str()), Some(0), Some(false), Some("registered")), "listed before its first job");
    assert_eq!((app["proof"]["class"].as_str(), app["proof"]["anchor"].as_str()), (Some("latency"), Some("exclusive")));
    submit_default(&h, &request(&h, "job-u2", b"unregistered")).await;
    assert_eq!(done(&h, "job-u2", b"unregistered").await["state"], "succeeded", "jobs once registered");
    let post = |body: serde_json::Value| {
        admin_curl(&h, &["-o", "/dev/null", "-w", "%{http_code}", "-X", "POST", "-H", "content-type: application/json",
            "-d", &body.to_string(), &format!("{}/apps?namespace={PROOFS}", h.http)])
    };
    let prover = |name: &str, proof: serde_json::Value| serde_json::json!({"name": name, "spec": {"image": "ziren-gpu:test"}, "proof": proof});
    assert_eq!(post(prover(&app_named(&id), serde_json::json!({"elf_id": "f".repeat(64)}))), "409", "a name is registered once");
    assert_eq!(post(prover("again", serde_json::json!({"elf_id": id}))), "409", "a program is one app of the namespace");
    assert_eq!(post(prover("slow", serde_json::json!({"elf_id": "f".repeat(64), "class": "slow"}))), "400", "no such class");
    assert_eq!(post(prover("short", serde_json::json!({"elf_id": "abc"}))), "400", "not a program id");
    assert_eq!(post(prover("a/b", serde_json::json!({"elf_id": "f".repeat(64)}))), "400", "not a name");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_disabled_app_refuses_new_jobs_until_it_is_enabled() {
    let h = start().await;
    add_node_for_app(&h, "gpu-a", 2, false, PROOFS, b"elf-e").await;
    submit_default(&h, &request(&h, "job-e1", b"elf-e")).await;
    assert_eq!(done(&h, "job-e1", b"elf-e").await["state"], "succeeded");
    let key = format!("{}/apps/{}/disabled?namespace={PROOFS}", h.http, app_of(b"elf-e"));
    let off: serde_json::Value = serde_json::from_str(&admin_curl(&h, &["-X", "PUT", &key])).unwrap();
    assert_eq!(off["disabled"], true);
    assert_eq!(registered_app(&h, b"elf-e")["disabled"], true, "the app says so");
    let (status, error) = submit_default(&h, &request(&h, "job-e2", b"elf-e")).await;
    assert_eq!(status, S::InvalidParameter as i32, "refused while disabled");
    assert!(error.contains("disabled"), "{error}");
    assert_refused(&done(&h, "job-e2", b"elf-e").await);
    admin_curl(&h, &["-X", "DELETE", &key]);
    assert_eq!(registered_app(&h, b"elf-e")["disabled"], false);
    submit_default(&h, &request(&h, "job-e3", b"elf-e")).await;
    assert_eq!(done(&h, "job-e3", b"elf-e").await["state"], "succeeded", "jobs again once enabled");
    let unknown = admin_curl(&h, &["-o", "/dev/null", "-w", "%{http_code}", "-X", "PUT", &format!("{}/apps/nobody/disabled?namespace={PROOFS}", h.http)]);
    assert_eq!(unknown, "404");
}

#[tokio::test(flavor = "multi_thread")]
async fn jobs_require_authenticated_immutable_identity() {
    let h = start().await;
    let jobs = format!("{}/apps/{}/jobs", h.http, app_of(b"elf"));
    let mut unsigned = request(&h, "unsigned", b"elf");
    unsigned.signature.clear();
    let (status, error) = submit_default(&h, &unsigned).await;
    assert_eq!(status, S::InvalidParameter as i32);
    assert!(error.contains("signature"), "{error}");
    assert_eq!(admin_curl(&h, &["-o", "/dev/null", "-w", "%{http_code}", &format!("{jobs}/unsigned?namespace={PROOFS}")]), "404");

    let submitted = request(&h, "identity", b"elf");
    assert_eq!(submit_default(&h, &submitted).await.0, S::Computing as i32);
    let saved: serde_json::Value = serde_json::from_str(&admin_curl(&h, &[&format!("{jobs}/identity?namespace={PROOFS}")])).unwrap();
    let namespace: serde_json::Value = serde_json::from_str(&admin_curl(&h, &[&format!("{}/namespaces/{PROOFS}", h.http)])).unwrap();
    assert_eq!(saved["owner"], operator_address());
    assert_eq!(saved["namespace"], namespace["id"]);
    assert_eq!(saved["app"], app_of(b"elf"));
    assert_eq!(saved["tag"], "scope=default");
    assert_eq!(saved["client_id"], "identity");
    assert_eq!(saved["state"], "queued");
    assert_eq!(saved["inputs"], serde_json::to_value(&submitted.inputs).unwrap());
    for field in ["node", "started_ms", "finished_ms"] {
        assert!(saved[field].is_null(), "queued {field} is legitimately absent");
    }
    // Another token of the namespace and tag can poll/retry, but cannot rewrite audit identity.
    let (key, _, bearer) = tag_token(&h, PROOFS, Some("scope=default"), 8, &artifacts::hash(b"elf"));
    let retried = signed_request(&h, "identity", b"elf", &key);
    assert_eq!(submit(&h, &bearer, &app_of(b"elf"), &retried).await.0, S::Computing as i32);
    let changed = request_with(&h, "identity", b"elf", vec![b"different input".to_vec()], &key);
    assert_eq!(submit(&h, &bearer, &app_of(b"elf"), &changed).await.0, S::InvalidParameter as i32);
    let unchanged: serde_json::Value = serde_json::from_str(&admin_curl(&h, &[&format!("{jobs}/identity?namespace={PROOFS}")])).unwrap();
    assert_eq!(saved, unchanged);
}

fn user_http(h: &Harness, token: &str, method: &str, path: &str) -> (u16, String) {
    let out = std::process::Command::new("curl")
        .args(["-sS", "-X", method, "-H", &format!("authorization: Bearer {token}"),
            "-w", "\n%{http_code}", &format!("{}{path}", h.http),
        ])
        .output().unwrap();
    let text = String::from_utf8(out.stdout).unwrap();
    let (body, code) = text.rsplit_once('\n').unwrap();
    (code.parse().unwrap(), body.into())
}

#[tokio::test(flavor = "multi_thread")]
async fn user_tokens_scope_apps_jobs_and_revocation() {
    use sha2::Digest;
    let h = start().await;
    // Namespace-specific nodes are registered after their namespaces are created below.
    let elf = artifacts::hash(b"elf");
    let keys: Vec<_> = [11u8, 12].iter().map(|v| k256::ecdsa::SigningKey::from_slice(&[*v; 32]).unwrap()).collect();
    let addresses: Vec<_> = keys.iter().map(|k| {
            format!("{:x}", alloy_primitives::Address::from_public_key(k.verifying_key()))
        }).collect();
    let tokens = ["a".repeat(96), "b".repeat(96)];
    for i in 0..2 {
        let ns = format!("user-{i}");
        assert!(create_namespace(&h, &ns, None)["id"].is_string());
        register(&h, &ns, b"elf");
        let token_id = format!("{:02x}", i + 1).repeat(16);
        let body = serde_json::json!({"id": token_id, "namespace": ns, "address": addresses[i], "label": format!("user-{i}"),
            "apps": [app_in(&ns, &elf), "training"], "token_hash": hex::encode(sha2::Sha256::digest(tokens[i].as_bytes()))});
        let created = admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d", &body.to_string(),
            &format!("{}/tokens", h.http),
            ],
        );
        let c: serde_json::Value = serde_json::from_str(&created).unwrap();
        assert_eq!(c["apps"][1], "training");
        assert_eq!(c["permissions"], serde_json::json!(["read", "submit"]));
        assert!(c["id"].as_str().is_some_and(|id| id.len() == 32));
        assert!(c.get("address").is_none(), "proof signer binding is internal to the token resource");
        assert!(c.get("token_hash").is_none());
        add_node_for_app(&h, &format!("gpu-user-{i}"), 2, false, &ns, b"elf").await;
        let id = format!("user-{i}");
        let (status, error) = submit(&h, &tokens[i], &app_in(&ns, &elf), &signed_request(&h, &id, b"elf", &keys[i])).await;
        assert_eq!(status, S::Computing as i32, "{error}");
    }
    // user-0 has another app its token is not granted.
    register(&h, "user-0", b"elf-r");
    let (own_app, other_app) = (app_in("user-0", &elf), app_in("user-1", &elf));
    assert_eq!(wait_done(&h, &tokens[0], &own_app, "user-0").await["state"], "succeeded");
    assert_eq!(wait_done(&h, &tokens[1], &other_app, "user-1").await["state"], "succeeded");
    assert_eq!(get_job(&h, &tokens[1], &other_app, "user-0").await.0, 404, "another namespace's job");
    // elf-r is no app of user-1's namespace.
    let (status, error) = submit(&h, &tokens[1], &other_app, &signed_request(&h, "not-an-app-b", b"elf-r", &keys[1])).await;
    assert_eq!(status, S::InvalidParameter as i32);
    assert!(error.contains("not an app of this namespace"), "{error}");
    let (status, error) = submit(&h, &tokens[1], &other_app, &signed_request(&h, "wrong-cert", b"elf", &keys[0])).await;
    assert_eq!(status, S::InvalidParameter as i32);
    assert!(error.contains("signature must match"), "{error}");
    let mut unsigned = signed_request(&h, "unsigned-user", b"elf", &keys[0]);
    unsigned.signature.clear();
    assert_eq!(submit(&h, &tokens[0], &own_app, &unsigned).await.0, S::InvalidParameter as i32);
    let not_granted = app_in("user-0", &artifacts::hash(b"elf-r"));
    assert_eq!(post_job(&h, &tokens[0], &not_granted, &signed_request(&h, "not-granted", b"elf-r", &keys[0])).await.0, 403);
    // Nor through a granted app: a program's jobs are its own app's.
    let (status, error) = submit(&h, &tokens[0], &own_app, &signed_request(&h, "wrong-app", b"elf-r", &keys[0])).await;
    assert_eq!(status, S::InvalidParameter as i32);
    assert!(error.contains(&format!("is app {not_granted}")), "{error}");
    assert_refused(&wait_done(&h, &tokens[0], &own_app, "wrong-app").await);
    let job = format!("/apps/{own_app}/jobs/user-0");
    assert_eq!(user_http(&h, &tokens[0], "GET", &job).0, 200);
    assert_eq!(user_http(&h, &tokens[1], "GET", &job).0, 404);
    assert_eq!(user_http(&h, &tokens[0], "GET", &format!("/apps/{not_granted}/jobs/user-0")).0, 403, "not granted");
    assert_eq!(user_http(&h, &tokens[0], "GET", "/tokens").0, 403);
    assert_eq!(user_http(&h, &tokens[0], "GET", "/status").0, 403);
    assert_eq!(user_http(&h, &tokens[0], "POST", "/apps").0, 403);
    assert_eq!(user_http(&h, &tokens[0], "PUT", &format!("/apps/{own_app}/priority")).0, 403);
    let (_, body) = user_http(&h, &tokens[1], "GET", &format!("/apps/{other_app}/jobs?limit=1"));
    let jobs: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(jobs.as_array().unwrap().len(), 1);
    assert_eq!(jobs[0]["owner"], addresses[1]);
    let (_, body) = user_http(&h, &tokens[1], "GET", "/apps");
    let apps: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(apps.as_array().unwrap().len(), 1);
    assert_eq!(apps[0]["jobs"]["total"], 2, "its job and the refusal recorded under it");
    let (_, body) = user_http(&h, &tokens[0], "GET", "/apps");
    let apps: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(apps.as_array().unwrap().len(), 1, "an app the token is not granted is not listed");
    assert_eq!(user_http(&h, &tokens[0], "GET", "/access").0, 200);
    admin_curl(&h, &["-X", "DELETE", &format!("{}/tokens/{}", h.http, "01".repeat(16)),
        ],
    );
    assert_eq!(user_http(&h, &tokens[0], "GET", "/access").0, 401);
    assert_eq!(user_http(&h, &tokens[0], "GET", &job).0, 401);
    assert_eq!(post_job(&h, &tokens[0], &own_app, &signed_request(&h, "revoked", b"elf", &keys[0])).await.0, 401);
}

#[tokio::test(flavor = "multi_thread")]
async fn bearer_tokens_authorize_proof_submission_and_status_without_client_certificates() {
    let h = start().await;
    // Alice's dedicated node is registered after creating the namespace below.
    let elf = artifacts::hash(b"elf");
    let app = app_in("alice", &elf);
    create_namespace(&h, "alice", None);
    let (alice_key, _, alice_token) = tag_token(&h, "alice", Some("scope=research"), 71, &elf);
    let (bob_key, _, bob_token) = tag_token(&h, "alice", Some("scope=production"), 72, &elf);
    add_node_for_app(&h, "gpu-bearer", 2, false, "alice", b"elf").await;
    let fetch_agent = start_fetch_agent(&h, h.storage.clone()).await;

    let (status, error) = submit(&h, &alice_token, &app, &signed_request(&h, "bearer-proof", b"elf", &alice_key)).await;
    assert_eq!(status, S::Computing as i32, "{error}");
    assert_eq!(wait_done(&h, &alice_token, &app, "bearer-proof").await["state"], "succeeded");
    assert_eq!(get_job(&h, &bob_token, &app, "bearer-proof").await.0, 404, "another tag's job");

    let proof_path = format!("/apps/{app}/jobs/bearer-proof/output/proof.json");
    let proof_path = proof_path.as_str();
    assert_eq!(user_http(&h,&alice_token,"GET",proof_path).0,200);
    assert_eq!(user_http(&h,&bob_token,"GET",proof_path).0,404);
    assert_eq!(user_http(&h,&alice_token,"GET",&format!("/apps/{app}/jobs/bearer-proof/output/stdin.bin")).0,404);

    let (status, _) = submit(&h, &alice_token, &app, &signed_request(&h, "bearer-mismatch", b"elf", &bob_key)).await;
    assert_eq!(status, S::InvalidParameter as i32);

    let no_token = h.client.get(format!("{}/apps/{app}/jobs/bearer-proof", h.http)).send().await.unwrap();
    assert_eq!(no_token.status().as_u16(), 401);
    let no_token = h.client.post(format!("{}/apps/{app}/jobs", h.http))
        .json(&signed_request(&h, "anonymous", b"elf", &alice_key)).send().await.unwrap();
    assert_eq!(no_token.status().as_u16(), 401);

    let revoked = admin_curl(&h, &["-X", "DELETE", &format!("{}/tokens/{}", h.http, "47".repeat(16))]);
    assert!(revoked.is_empty());
    assert_eq!(get_job(&h, &alice_token, &app, "bearer-proof").await.0, 401);
    fetch_agent.abort();
}

/// Machine fetches run without any GPU actor or inbound listener.
async fn start_fetch_agent(h: &Harness, root: std::path::PathBuf) -> tokio::task::JoinHandle<()> {
    use fleet_proto::fleet::v1::{worker_service_client::WorkerServiceClient, WorkerHeartbeatRequest,
    };
    let mut worker = WorkerServiceClient::connect(format!("http://127.0.0.1:{}", h.control_port)).await.unwrap();
    let http = h.http.clone();
    let task = tokio::spawn(async move {
        let client = reqwest::Client::new();
        loop {
            let mut request = Request::new(WorkerHeartbeatRequest {
                worker_id: "storage-agent".into(), session: "storage-session".into(), proof_fetch_ready: true,
                report: Some(
                    serde_json::from_value(
                        serde_json::json!({"version":3,"machine":"test","ready":true,
                    "total_gpus":8,"total_cpus":48,"free_gpus":8,"free_cpus":48}),
                    ).unwrap(),
                ),
            });
            request.metadata_mut().insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
            let response = worker.heartbeat(request).await.unwrap().into_inner();
            for fetch in response.proof_fetches {
                let result = fleet_ziren_node::fetch::read(&fetch, std::slice::from_ref(&root));
                let mut post = client.post(format!("{http}/internal/proof-fetch/{}",fetch.ticket))
                    .bearer_auth(TOKEN).header("x-fleet-worker","storage-agent").header("x-fleet-session","storage-session");
                post = match result { Ok(bytes) => post.body(bytes), Err(_) => post.header("x-fleet-fetch-error","invalid"),
                };
                assert_eq!(post.send().await.unwrap().status().as_u16(),204);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    task
}

/// A proof request to an app's own endpoint (the SDK's stage API it serves locally).
fn stage_request(id: &str, elf: &[u8], key: &k256::ecdsa::SigningKey) -> fleet_proto::stage::v1::GenerateProofRequest {
    let mut req = fleet_proto::stage::v1::GenerateProofRequest {
        proof_id: id.into(),
        elf_data: elf.to_vec(),
        private_input_stream: bincode::serialize(&stdin()).unwrap(),
        target_step: Some(Step::InAgg as i32),
        ..Default::default()
    };
    req.signature = signature(&req.proof_id, req.block_no, req.seg_size, key);
    req
}

#[tokio::test(flavor = "multi_thread")]
async fn app_endpoint_reads_local_outputs_without_machine_fetch_heartbeats() {
    use fleet_proof_app::{ProofAppServer, ProofAppStageService};
    use fleet_proto::stage::v1::{stage_service_client::StageServiceClient, stage_service_server::StageServiceServer, GetStatusRequest};
    use std::future::IntoFuture;
    let h = start().await;
    create_namespace(&h, "alice", None);
    let elf=artifacts::hash(b"elf");
    let app_name=app_in("alice",&elf);
    let (key,_,token)=tag_token(&h,"alice",Some("scope=research"),81,&elf);
    let (_,_,foreign)=tag_token(&h,"alice",Some("scope=production"),82,&elf);
    let (_, _, proof_node) = add_node_app_session(&h,"app-prover",2,false,150,tonic::Code::Internal,"alice", b"elf",
    ).await;
    let storage=tempfile::tempdir().unwrap();
    let http_listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_url=format!("http://{}",http_listener.local_addr().unwrap());
    let namespace:serde_json::Value=serde_json::from_str(&admin_curl(&h,&[&format!("{}/namespaces/alice",h.http)],
    )).unwrap();
    let token_file = storage.path().join("user.token");
    std::fs::write(&token_file,&token).unwrap();
    let mut app = ProofAppServer::new(storage.path().into(),namespace["id"].as_str().unwrap().into(),
        vec![elf, artifacts::hash(b"another-program")],h.http.clone(),http_url,
    ).unwrap();
    app.token_file = Some(token_file);
    let app=Arc::new(app);
    tokio::spawn(axum::serve(http_listener,fleet_proof_app::router(app.clone())).into_future());
    let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let channel=Channel::from_shared(format!("http://{}",listener.local_addr().unwrap())).unwrap();
    let task=tokio::spawn(tonic::transport::Server::builder().add_service(StageServiceServer::new(ProofAppStageService(app)))
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let mut client=StageServiceClient::new(channel.connect().await.unwrap());
    let mut request=stage_request("app-storage-proof",b"elf",&key);
    request.receipt_inputs = vec![vec![9,8,7]]; // Older SDK wire field.
    let accepted=client.generate_proof(request.clone()).await.unwrap().into_inner();
    assert_eq!(accepted.status,S::Computing as i32,"{}",accepted.error_message);
    let deadline=tokio::time::Instant::now()+Duration::from_secs(5);
    let status=loop {
        let response=client.get_status(with_bearer(GetStatusRequest{proof_id:"app-storage-proof".into(),
                },&token,
            )).await.unwrap().into_inner();
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
    let foreign=client.get_status(with_bearer(GetStatusRequest{proof_id:"app-storage-proof".into(),
            },&foreign,
        )).await.unwrap().into_inner();
    assert_eq!(foreign.status,S::Unspecified as i32);
    let values=reqwest::get(&status.public_values_url).await.unwrap();
    assert!(values.status().is_success());assert!(!values.bytes().await.unwrap().is_empty());
    // No proof bytes, inputs or programs are kept on the control plane.
    for dir in ["elf","jobs","proofs"] {assert!(!h._dir.path().join("data").join(dir).exists(), "data/{dir}");}
    let metadata=user_http(&h,&token,"GET",&format!("/apps/{app_name}/jobs/app-storage-proof"));
    let metadata:serde_json::Value=serde_json::from_str(&metadata.1).unwrap();
    assert!(metadata["inputs"]["input"]["uri"].as_str().unwrap().starts_with("file://"));
    let input_path=metadata["inputs"]["input"]["uri"].as_str().unwrap().strip_prefix("file://").unwrap();
    let input=ProveRequest::decode(std::fs::read(input_path).unwrap().as_slice()).unwrap();
    assert_eq!(input.receipts,vec![vec![9,8,7]]);
    assert_eq!(client.get_status(with_bearer(GetStatusRequest{proof_id:"app-storage-proof".into()},"invalid-supplied-token")).await.unwrap_err().code(),tonic::Code::Unauthenticated);
    assert!(metadata["outputs"]["proof"]["sha256"].as_str().unwrap().len()==64);
    let (_,_,ungranted)=tag_token(&h,"alice",Some("scope=research"),83,&artifacts::hash(b"another-program"),
    );
    assert_eq!(user_http(&h,&ungranted,"GET",&format!("/apps/{app_name}/jobs/app-storage-proof")).0,403, "the token is not granted the app");
    assert_eq!(user_http(&h,&ungranted,"GET",&format!("/apps/{app_name}/jobs/app-storage-proof/output/proof.json")).0,403, "the token is not granted the app");
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
    let mut ctl = ControlServiceClient::with_interceptor(channel, fleet_scheduler::token::BearerTokenInterceptor::new(TOKEN).unwrap(),
    );
    ctl.deregister(fleet_proto::fleet::v1::DeregisterRequest {node_id:proof_node.into(),
    }).await.unwrap();
    assert_eq!(metadata["outputs"]["machine"],"test");
    admin_curl(&h,&["-X","PUT",&format!("{}/apps/{app_name}/disabled?namespace=alice",h.http)]);
    let rejected = client.generate_proof(with_bearer(stage_request("app-disabled-proof", b"elf", &key), &token,
        ))
        .await.unwrap().into_inner();
    assert_eq!(rejected.status, S::InvalidParameter as i32);
    let rejected = client.get_status(with_bearer(GetStatusRequest {proof_id:"app-disabled-proof".into(),
            }, &token,
        ))
        .await.unwrap().into_inner();
    assert_eq!(rejected.status, S::InvalidParameter as i32);
    assert!(rejected.proof_with_public_inputs.is_empty());
    let fetch = reqwest::Client::new().get(format!("{}/apps/{app_name}/jobs/app-storage-proof/output/proof.json",h.http))
        .bearer_auth(&token).send().await.unwrap();
    assert_eq!(fetch.status().as_u16(),200);assert_eq!(fetch.bytes().await.unwrap().as_ref(),status.proof_with_public_inputs);
    let path = metadata["outputs"]["proof"]["uri"].as_str().unwrap().strip_prefix("file://").unwrap();
    std::fs::write(path,b"corrupt").unwrap();
    let corrupt = reqwest::Client::new().get(format!("{}/apps/{app_name}/jobs/app-storage-proof/output/proof.json",h.http)).bearer_auth(&token).send().await.unwrap();
    assert_eq!(corrupt.status().as_u16(),502);
    std::fs::write(path,&status.proof_with_public_inputs).unwrap();
    admin_curl(&h,&["-X","DELETE",&format!("{}/tokens/{}",h.http,"51".repeat(16)),
        ],
    );
    assert_eq!(reqwest::get(&status.public_values_url).await.unwrap().status().as_u16(),404);
    assert_eq!(client.get_status(with_bearer(GetStatusRequest{proof_id:"app-storage-proof".into()},&token)).await.unwrap_err().code(),tonic::Code::Unauthenticated);
    fetch_agent.abort();
    task.abort();
}

fn namespace_token(h: &Harness, namespace: &str, seed: u8, elf_id: &str,
) -> (k256::ecdsa::SigningKey, String, String) {
    tag_token(h,namespace,None,seed,elf_id)
}

#[tokio::test(flavor = "multi_thread")]
async fn apps_require_admin_created_namespaces_and_mutations_are_namespace_local() {
    let h = start().await;
    let elf = artifacts::hash(b"elf");
    let app = app_in("alice", &elf);
    for (selector, expected) in [("", "400"), ("?namespace=missing", "404")] {
        let code = admin_curl(&h, &["-X", "POST", "-o", "/dev/null", "-w", "%{http_code}", "-H", "content-type: application/json",
            "-d", &serde_json::json!({"name": app, "spec": {"image": "ziren-gpu:test"}, "proof": {"elf_id": elf}}).to_string(),
            &format!("{}/apps{selector}", h.http)]);
        assert_eq!(code, expected);
    }
    for name in ["alice", "bob"] {
        create_namespace(&h, name, None);
    }
    let (alice_key, _, alice_token) = namespace_token(&h, "alice", 61, &elf);
    let (bob_key, _, bob_token) = namespace_token(&h, "bob", 62, &elf);
    assert_eq!(user_http(&h, &alice_token, "POST", "/namespaces").0, 403);
    assert_eq!(user_http(&h, &alice_token, "POST", "/apps").0, 403);
    admin_curl(&h, &["-X", "PUT", &format!("{}/apps/{app}/disabled?namespace=alice", h.http)]);
    let (code, apps) = user_http(&h, &bob_token, "GET", "/apps");
    assert_eq!(code, 200);
    let apps: serde_json::Value = serde_json::from_str(&apps).unwrap();
    assert_eq!(apps.as_array().unwrap().len(), 1);
    assert_eq!(apps[0]["disabled"], false);
    let denied = submit(&h, &alice_token, &app, &signed_request(&h, "disabled", b"elf", &alice_key)).await;
    assert_eq!(denied.0, S::InvalidParameter as i32);
    add_node_for_app(&h, "gpu-namespaces", 2, false, "bob", b"elf").await;
    let bob_app = app_in("bob", &elf);
    submit(&h, &bob_token, &bob_app, &signed_request(&h, "bob-job", b"elf", &bob_key)).await;
    assert_eq!(wait_done(&h, &bob_token, &bob_app, "bob-job").await["state"], "succeeded");
    let deleted = admin_curl(&h, &["-X", "DELETE", "-o", "/dev/null", "-w", "%{http_code}",
        &format!("{}/apps/{app}?namespace=alice", h.http)]);
    assert_eq!(deleted, "200");
    assert_eq!(wait_done(&h, &bob_token, &bob_app, "bob-job").await["state"], "succeeded",
        "deleting Alice's registration preserves Bob's job and shared ELF");
}

/// A token of the namespace and tag that may read and submit the app of program `elf_id`
/// (registered here when it is not yet) and batch `training`: its signing key, signer
/// address and bearer secret.
fn tag_token(h: &Harness,namespace: &str,tag: Option<&str>,seed: u8,elf_id: &str,
) -> (k256::ecdsa::SigningKey,String,String) {
    use sha2::Digest;
    let code = register_id(h, namespace, elf_id);
    assert!(code == "201" || code == "409", "register namespace app: {code}");
    let key = k256::ecdsa::SigningKey::from_slice(&[seed; 32]).unwrap();
    let address = format!("{:x}", alloy_primitives::Address::from_public_key(key.verifying_key()));
    let token = format!("{seed:02x}").repeat(48);
    let body = serde_json::json!({"id": format!("{seed:02x}").repeat(16), "namespace": namespace, "tag":tag, "address": address,
        "apps": [app_in(namespace, elf_id), "training"], "label": namespace,
        "token_hash": hex::encode(sha2::Sha256::digest(token.as_bytes()))});
    let created = admin_curl(h, &["-X", "POST", "-H", "content-type: application/json", "-d", &body.to_string(),
        &format!("{}/tokens", h.http),
        ],
    );
    let c: serde_json::Value = serde_json::from_str(&created).unwrap_or_else(|_| panic!("{created}"));
    assert!(c["namespace"].is_string(), "{created}");
    (key, address, token)
}

#[tokio::test(flavor = "multi_thread")]
async fn namespace_rotation_preserves_jobs_and_suspension_blocks_every_key() {
    let h = start().await;
    let elf = artifacts::hash(b"elf");
    let (app, bob_app) = (app_in("alice", &elf), app_in("bob", &elf));
    for name in ["alice", "bob"] {
        assert!(create_namespace(&h, name, None)["id"].is_string());
    }
    for invalid in ["admin", "system", "Alice", "../escape", "", "9alice", "alice",
    ] {
        let body = serde_json::json!({"name": invalid}).to_string();
        let code = admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d", &body,
            "-o", "/dev/null", "-w", "%{http_code}", &format!("{}/namespaces", h.http),
            ],
        );
        assert_eq!(code, "400", "invalid, reserved or duplicate namespace {invalid:?}");
    }
    let (old_key, old_addr, old_token) = namespace_token(&h, "alice", 21, &elf);
    let (bob_key, _, bob_token) = namespace_token(&h, "bob", 22, &elf);
    let (_, alice_jobs) = add_node_for_app(&h, "gpu-ns-alice", 2, false, "alice", b"elf").await;
    let (_, bob_jobs) = add_node_for_app(&h, "gpu-ns-bob", 2, false, "bob", b"elf").await;
    for (key, token, app) in [(&old_key, &old_token, &app), (&bob_key, &bob_token, &bob_app)] {
        assert_eq!(submit(&h, token, app, &signed_request(&h, "same-id", b"elf", key)).await.0, S::Computing as i32);
    }
    assert_eq!(wait_done(&h, &old_token, &app, "same-id").await["state"], "succeeded");
    assert_eq!(wait_done(&h, &bob_token, &bob_app, "same-id").await["state"], "succeeded");
    assert_eq!(alice_jobs.lock().len(), 1, "Alice's dedicated server jobs only Alice's proof");
    assert_eq!(bob_jobs.lock().len(), 1, "Bob's dedicated server jobs only Bob's proof");
    let jobs: Vec<_> = alice_jobs.lock().iter().chain(bob_jobs.lock().iter()).cloned().collect();
    assert_eq!(jobs.len(), 2);
    assert_ne!(jobs[0], jobs[1], "nodes use distinct internal ids");
    let (_, a_json) = user_http(&h, &old_token, "GET", &format!("/apps/{app}/jobs/same-id"));
    let (_, b_json) = user_http(&h, &bob_token, "GET", &format!("/apps/{bob_app}/jobs/same-id"));
    let a: serde_json::Value = serde_json::from_str(&a_json).unwrap();
    let b: serde_json::Value = serde_json::from_str(&b_json).unwrap();
    assert_ne!(a["id"], b["id"]);
    assert_ne!(a["namespace"], b["namespace"]);
    assert_ne!(a["outputs"]["proof"]["uri"], b["outputs"]["proof"]["uri"], "each job's outputs are its own");
    assert_eq!(user_http(&h, &bob_token, "GET", &format!("/apps/{bob_app}/jobs/{}", a["id"].as_str().unwrap())).0, 404);
    assert_eq!(user_http(&h, &bob_token, "GET", &format!("/apps/{app}/jobs/same-id?namespace=alice")).0, 403);
    let (new_key, _, new_token) = namespace_token(&h, "alice", 23, &elf);
    let repeated = submit(&h, &new_token, &app, &signed_request(&h, "same-id", b"elf", &new_key)).await;
    assert_eq!(repeated.0, S::Computing as i32);
    assert_eq!(alice_jobs.lock().len() + bob_jobs.lock().len(), 2, "rotation does not queue a duplicate proof");
    admin_curl(&h, &["-X", "DELETE", &format!("{}/tokens/{}", h.http, "15".repeat(16)),
        ],
    );
    assert_eq!(user_http(&h, &old_token, "GET", "/access").0, 401);
    assert_eq!(get_job(&h, &old_token, &app, "same-id").await.0, 401);
    let (_, rotated_json) = user_http(&h, &new_token, "GET", &format!("/apps/{app}/jobs/same-id"));
    let rotated: serde_json::Value = serde_json::from_str(&rotated_json).unwrap();
    assert_eq!(rotated["id"], a["id"]);
    assert_eq!(rotated["owner"], old_addr, "the original submitter remains in the audit record");
    assert_eq!(rotated["state"], "succeeded");
    let admin_a = admin_curl(&h, &[&format!("{}/apps/{app}/jobs/same-id?namespace=alice", h.http)]);
    assert_eq!(serde_json::from_str::<serde_json::Value>(&admin_a).unwrap()["id"], a["id"]);
    let (_, _, second_token) = namespace_token(&h, "alice", 24, &elf);
    let denied = user_http(&h, &new_token, "POST", "/namespaces");
    assert_eq!(denied.0, 403);
    admin_curl(&h, &["-X", "PUT", &format!("{}/namespaces/alice/suspended", h.http),
        ],
    );
    assert_eq!(user_http(&h, &new_token, "GET", "/access").0, 401);
    assert_eq!(user_http(&h, &second_token, "GET", "/access").0, 401);
    assert_eq!(user_http(&h, &bob_token, "GET", "/access").0, 200);
    assert_eq!(get_job(&h, &new_token, &app, "same-id").await.0, 401);
    admin_curl(&h, &["-X", "DELETE", &format!("{}/namespaces/alice/suspended", h.http),
        ],
    );
    assert_eq!(user_http(&h, &new_token, "GET", "/access").0, 200);
    assert_eq!(user_http(&h, &old_token, "GET", "/access").0, 401);
}


#[tokio::test(flavor = "multi_thread")]
async fn namespace_resource_quota_is_shared_by_proofs_batches_and_rotated_keys() {
    let h = start().await;
    // The namespace-bound node is added after Alice's namespace and app exist.
    for name in ["alice", "bob"] {
        create_namespace(&h, name, Some(serde_json::json!({"gpus":2,"cpus":8})));
    }
    let elf = artifacts::hash(b"elf");
    let app = app_in("alice", &elf);
    let (key, _, token) = namespace_token(&h, "alice", 31, &elf);
    let (calls, _) = add_node_app(&h, "gpu-quota", 2, false, 1000, tonic::Code::Internal, "alice", b"elf",
    ).await;
    let (_, _, replacement_token) = namespace_token(&h, "alice", 32, &elf);
    let allocation_url = format!("{}/namespaces/alice/allocations", h.http);
    let held = serde_json::json!({"id":"held-run","gpus":2,"cpus":8}).to_string();
    let held_reply = admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d", &held, &allocation_url,
        ],
    );
    assert!(serde_json::from_str::<serde_json::Value>(&held_reply).unwrap()["id"].is_string());
    let overflow = serde_json::json!({"id":"another-run","gpus":0,"cpus":1}).to_string();
    let code = admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d", &overflow,
        "-o", "/dev/null", "-w", "%{http_code}", &allocation_url,
        ],
    );
    assert_eq!(code, "429");
    let r = submit(&h, &token, &app, &signed_request(&h, "proof-quota", b"elf", &key)).await;
    assert_eq!(r.0, S::Computing as i32);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0, "a run reservation leaves no quota for a proof");
    let (_, record) = user_http(&h, &token, "GET", &format!("/apps/{app}/jobs/proof-quota"));
    assert_eq!(serde_json::from_str::<serde_json::Value>(&record).unwrap()["state"], "queued");
    let (_, access) = user_http(&h, &replacement_token, "GET", "/access");
    let access: serde_json::Value = serde_json::from_str(&access).unwrap();
    assert_eq!(access["quota"]["gpus"], 2.0);
    assert_eq!(access["usage"]["gpus"], 2.0);
    assert_eq!(user_http(&h, &token, "POST", "/namespaces/alice/allocations").0, 403);
    // Bob has an independent cap, even with the same app grants.
    let bob_claim = serde_json::json!({"id":"bob-run","gpus":0,"cpus":1}).to_string();
    let bob = admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d", &bob_claim,
        &format!("{}/namespaces/bob/allocations", h.http),
        ],
    );
    assert!(serde_json::from_str::<serde_json::Value>(&bob).unwrap()["id"].is_string());
    admin_curl(&h, &["-X", "DELETE", &format!("{allocation_url}/held-run")]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while calls.load(Ordering::SeqCst) == 0 {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let code = admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d", &overflow,
        "-o", "/dev/null", "-w", "%{http_code}", &allocation_url,
        ],
    );
    assert_eq!(code, "429", "a running proof occupies the same quota as jobs");
    assert_eq!(wait_done(&h, &token, &app, "proof-quota").await["state"], "succeeded");
    let (_, access) = user_http(&h, &token, "GET", "/access");
    assert_eq!(serde_json::from_str::<serde_json::Value>(&access).unwrap()["usage"]["cpus"], 0.0);
    // An individually oversized run is refused; zero/unlimited caps can be updated.
    let too_big = serde_json::json!({"id":"oversized","gpus":3,"cpus":1}).to_string();
    assert_eq!(admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d", &too_big,
        "-o", "/dev/null", "-w", "%{http_code}", &allocation_url]), "422");
    for quota in [serde_json::json!({"gpus":-1}), serde_json::json!({"gpu_hours":5}),
    ] {
        let code = admin_curl(&h, &["-X", "PUT", "-H", "content-type: application/json", "-d", &quota.to_string(),
            "-o", "/dev/null", "-w", "%{http_code}", &format!("{}/namespaces/alice/quota", h.http),
            ],
        );
        assert!(code == "400" || code == "422");
    }
    let mut concurrent = Vec::new();
    for id in ["concurrent-a", "concurrent-b"] {
        let url = allocation_url.clone();
        let admin = h.admin.clone();
        concurrent.push(std::thread::spawn(move || {
            let body = serde_json::json!({"id":id,"gpus":2,"cpus":8}).to_string();
            let out = std::process::Command::new("curl").args(["-s", "--max-time", "5", "-H", &format!("authorization: Bearer {admin}"),
                "-H", "content-type: application/json", "-X", "POST", "-d", &body, "-o", "/dev/null", "-w", "%{http_code}", &url,
                ]).output().unwrap();
            String::from_utf8(out.stdout).unwrap()
        }));
    }
    let mut codes: Vec<_> = concurrent.into_iter().map(|t| t.join().unwrap()).collect();
    codes.sort();
    assert_eq!(codes, ["200", "429"], "concurrent admissions cannot overbook a namespace");

}

#[tokio::test(flavor = "multi_thread")]
async fn disconnected_namespaced_proof_keeps_quota_and_rejoins_original_node() {
    let h = start().await;
    // Both dedicated nodes are added after Alice's namespace app is registered below.
    let created = create_namespace(&h, "alice", Some(serde_json::json!({"gpus":2,"cpus":8})));
    assert_eq!(created["name"], "alice");
    let elf=artifacts::hash(b"elf");
    let app=app_in("alice",&elf);
    let (key,_,token)=namespace_token(&h,"alice",41,&elf);
    let (original,_) = add_node_app(&h,"original",2,true,100,tonic::Code::Unavailable,"alice", b"elf",
    ).await;
    let (other,_) = add_node_app(&h,"other",1,false,100,tonic::Code::Internal,"alice", b"elf",
    ).await;
    submit(&h,&token,&app,&signed_request(&h,"disconnected",b"elf",&key)).await;
    let deadline=tokio::time::Instant::now()+Duration::from_secs(3);
    while original.load(Ordering::SeqCst)==0 {
        assert!(tokio::time::Instant::now()<deadline);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let (_,access)=user_http(&h,&token,"GET","/access");
    assert_eq!(serde_json::from_str::<serde_json::Value>(&access).unwrap()["usage"]["gpus"],2.0);
    let code=admin_curl(&h,&["-X","POST","-H","content-type: application/json","-d",
        r#"{"id":"blocked","gpus":1,"cpus":1}"#,"-o","/dev/null","-w","%{http_code}",&format!("{}/namespaces/alice/allocations",h.http),
        ],
    );
    assert_eq!(code,"429");
    assert_eq!(other.load(Ordering::SeqCst),0,"never fail over an execution of uncertain status");
    assert_eq!(wait_done(&h,&token,&app,"disconnected").await["state"],"succeeded");
    assert_eq!(original.load(Ordering::SeqCst),2);
    assert_eq!(other.load(Ordering::SeqCst),0);
}

#[tokio::test(flavor = "multi_thread")]
async fn tags_isolate_proofs_and_enforce_both_resource_caps() {
    let h=start().await;
    create_namespace(&h, "billing", Some(serde_json::json!({"gpus":2,"cpus":16})));
    let elf=artifacts::hash(b"elf");
    let app=app_in("billing", &elf);
    let jobs=format!("/apps/{app}/jobs");
    let (ka,_,ta)=tag_token(&h,"billing",Some("scope=research"),51,&elf);
    let (kb,_,tb)=tag_token(&h,"billing",Some("scope=production"),52,&elf);
    let (_,_,replacement)=tag_token(&h,"billing",Some("scope=research"),53,&elf);
    add_node_for_app(&h,"tag-prover",1,false,"billing", b"elf").await;
    let listing=admin_curl(&h,&[&format!("{}/namespaces/billing",h.http)]);
    let listing:serde_json::Value=serde_json::from_str(&listing).unwrap();
    assert_eq!(listing["tags"].as_array().unwrap().len(),3);
    // Tags are optional attributes; no separate registry or creation step exists.
    let before=admin_curl(&h,&[&format!("{}/namespaces/billing?tag=scope=research",h.http)],
    );
    let before:serde_json::Value=serde_json::from_str(&before).unwrap();
    assert_eq!(before["name"],"billing");
    assert_eq!(before["quota"]["gpus"],2.0);
    assert!(before["tag_quota"]["gpus"].is_null());
    for tag in ["scope=research","scope=production","scope=staging"] {
        let reply=admin_curl(&h,&["-X","PUT","-H","content-type: application/json","-d",r#"{"gpus":1,"cpus":8}"#,&format!("{}/namespaces/billing/quota?tag={tag}",h.http),
            ],
        );
        let result:serde_json::Value=serde_json::from_str(&reply).unwrap();
        assert_eq!(result["tag"],tag);
        assert_eq!(result["quota"]["gpus"],2.0,"tag caps never replace the namespace quota");
        assert_eq!(result["tag_quota"]["gpus"],1.0);
    }
    let (ku,_,tu)=namespace_token(&h,"billing",54,&elf);
    let (kd,_,td)=tag_token(&h,"billing",Some("scope=default"),55,&elf);
    for (k,t) in [(&ka,&ta),(&kb,&tb),(&ku,&tu),(&kd,&td)] {
        assert_eq!(submit(&h,t,&app,&signed_request(&h,"same",b"elf",k)).await.0,S::Computing as i32);
        assert_eq!(wait_done(&h,t,&app,"same").await["state"],"succeeded");
    }
    let (_,a)=user_http(&h,&ta,"GET",&format!("{jobs}/same"));
    let a:serde_json::Value=serde_json::from_str(&a).unwrap();
    let (_,b)=user_http(&h,&tb,"GET",&format!("{jobs}/same"));
    let b:serde_json::Value=serde_json::from_str(&b).unwrap();
    assert_ne!(a["id"],b["id"]);
    assert_eq!(a["tag"],"scope=research");
    let (_,untagged)=user_http(&h,&tu,"GET",&format!("{jobs}/same"));
    let untagged:serde_json::Value=serde_json::from_str(&untagged).unwrap();
    assert_eq!(untagged["tag"], "");
    assert!(untagged.get("group").is_none());
    let (_,named_default)=user_http(&h,&td,"GET",&format!("{jobs}/same"));
    let named_default:serde_json::Value=serde_json::from_str(&named_default).unwrap();
    assert_eq!(named_default["tag"],"scope=default");
    assert_ne!(untagged["id"],named_default["id"]);
    for foreign in [&a,&b,&named_default] {
        assert_eq!(user_http(&h,&tu,"GET",&format!("{jobs}/{}",foreign["id"].as_str().unwrap())).0,404);
    }
    let (_,own)=user_http(&h,&tu,"GET",&jobs);
    assert_eq!(serde_json::from_str::<serde_json::Value>(&own).unwrap().as_array().unwrap().len(),1);
    assert_eq!(user_http(&h,&tb,"GET",&format!("{jobs}/{}",a["id"].as_str().unwrap())).0,404);
    assert_eq!(user_http(&h,&replacement,"GET",&format!("{jobs}/same")).0,200);
    assert_eq!(user_http(&h,&ta,"GET",&format!("{jobs}?group=production")).0,400);
    assert_eq!(user_http(&h,&ta,"GET",&format!("{jobs}?tag=scope=production")).0,403);
    assert_eq!(user_http(&h,&ta,"GET",&format!("{jobs}/same?tag=scope=research")).0,200);
    let (_,access)=user_http(&h,&replacement,"GET","/access");
    let access:serde_json::Value=serde_json::from_str(&access).unwrap();
    assert_eq!(access["namespace"], a["namespace"]);
    assert_eq!(access["tag"], "scope=research");
    assert_eq!(access["tag_quota"]["gpus"],1.0);
    let url=format!("{}/namespaces/billing/allocations",h.http);
    let claim=|id:&str,tag:&str,gpus:u32| {
        let body=serde_json::json!({"id":id,"tag":tag,"gpus":gpus,"cpus":1}).to_string();
        admin_curl(&h,&["-X","POST","-H","content-type: application/json","-d",&body,"-o","/dev/null","-w","%{http_code}",&url,
            ],
        )
    };
    assert_eq!(claim("r1","scope=research",1),"200");
    assert_eq!(claim("r2","scope=research",1),"429","tag aggregate cap");
    assert_eq!(claim("r3","scope=research",2),"422","tag per-job cap");
    assert_eq!(claim("p1","scope=production",1),"200");
    admin_curl(&h,&["-X","PUT","-H","content-type: application/json","-d",r#"{"gpus":2,"cpus":16}"#,&format!("{}/namespaces/billing/quota?tag=scope=production",h.http),
        ],
    );
    assert_eq!(claim("p2","scope=production",1),"429","all tags share namespace cap");
    assert_eq!(claim("r1","scope=production",1),"409","allocation cannot change tag");
    assert_eq!(claim("free","scope=unregistered",0),"200","tags need no registration");
    assert_eq!(claim("bare","unregistered",0),"400","new tags require key=value");
    assert_eq!(claim("bad","-invalid",0),"400");
    assert_eq!(user_http(&h,&ta,"POST","/namespaces/billing/groups").0,404);
    let code=admin_curl(&h,&["-X","POST","-o","/dev/null","-w","%{http_code}",&format!("{}/namespaces/billing/tags",h.http),
        ],
    );
    assert_eq!(code,"404","tags have no independent creation endpoint");
}

#[tokio::test(flavor = "multi_thread")]
async fn jobs_inherit_app_priority_and_dispatch_highest_then_fifo() {
    let h = start().await;
    let priority_url = format!("{}/apps/{}/priority?namespace={PROOFS}", h.http, app_of(b"elf"));
    let priority = |h: &Harness, p: i32| admin_curl(h, &["-o", "/dev/null", "-w", "%{http_code}", "-X", "PUT", "-H", "content-type: application/json",
        "-d", &serde_json::json!({"priority": p}).to_string(), &priority_url]);
    submit_default(&h, &request(&h, "low-priority", b"elf")).await;
    assert_eq!(priority(&h, 10), "200");
    submit_default(&h, &request(&h, "high-first", b"elf")).await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    submit_default(&h, &request(&h, "high-second", b"elf")).await;
    // An app edit changes future submissions; pending jobs retain their snapshot.
    assert_eq!(priority(&h, -10), "200");
    let jobs: serde_json::Value = serde_json::from_str(&admin_curl(&h, &[&format!("{}/apps/{}/jobs?namespace={PROOFS}", h.http, app_of(b"elf"))])).unwrap();
    let find = |id: &str| {
        jobs.as_array().unwrap().iter().find(|j| j["client_id"] == id).unwrap()
    };
    assert_eq!(find("low-priority")["priority"], 0);
    assert_eq!(find("high-first")["priority"], 10);
    assert_eq!(find("high-second")["priority"], 10);
    let expected: Vec<String> = ["high-first", "high-second", "low-priority"].iter()
        .map(|id| find(id)["id"].as_str().unwrap().to_owned()).collect();
    let (_, calls) = add_node_for_app(&h, "priority-node", 1, false, PROOFS, b"elf").await;
    done(&h, "low-priority", b"elf").await;
    assert_eq!(*calls.lock(), expected);
    assert_eq!(registered_app(&h, b"elf")["priority"], -10);
}

#[tokio::test(flavor = "multi_thread")]
async fn namespace_owner_can_manage_every_app_only_in_its_namespace() {
    let h = start().await;
    for name in ["owner-scope", "other-scope"] {
        create_namespace(&h, name, None);
    }
    for (namespace, elf) in [("owner-scope", &b"owner-managed-app"[..]), ("owner-scope", b"another-app"), ("other-scope", b"owner-managed-app")] {
        register(&h, namespace, elf);
    }
    let address = format!("{:x}", alloy_primitives::Address::from_public_key(
        k256::ecdsa::SigningKey::from_slice(&[81; 32]).unwrap().verifying_key()));
    let bearer = "81".repeat(48);
    let body = serde_json::json!({"id":"81".repeat(16), "namespace":"owner-scope", "tag":"scope=default",
        "address":address, "label":"namespace owner", "permissions":["read","submit","manage"],
        "token_hash":hex::encode(sha2::Sha256::digest(bearer.as_bytes()))});
    admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d", &body.to_string(),
        &format!("{}/tokens", h.http),
        ],
    );
    let app = |namespace: &str, elf: &[u8]| app_in(namespace, &artifacts::hash(elf));
    assert_eq!(user_http(&h, &bearer, "DELETE", &format!("/apps/{}", app("owner-scope", b"owner-managed-app"))).0, 200);
    assert_eq!(user_http(&h, &bearer, "DELETE", &format!("/apps/{}", app("owner-scope", b"another-app"))).0, 200);
    let other = app("other-scope", b"owner-managed-app");
    assert_eq!(user_http(&h, &bearer, "DELETE", &format!("/apps/{other}?namespace=other-scope")).0, 403);
    assert!(admin_curl(&h, &[&format!("{}/apps/{other}?namespace=other-scope", h.http)]).contains("\"proof\""));
}

#[tokio::test(flavor = "multi_thread")]
async fn app_reservations_count_against_namespace_quota() {
    let h = start().await;
    let ns = create_namespace(&h, "app-budget", None);
    let id = ns["id"].as_str().unwrap();
    let quota = serde_json::json!({"gpus": 2.0, "cpus": 8.0}).to_string();
    admin_curl(&h, &["-X", "PUT", "-H", "content-type: application/json", "-d", &quota,
        &format!("{}/namespaces/{id}/quota", h.http),
        ],
    );
    let deployment = serde_json::json!({"id":"deployment:ethproofs-gpu-0", "tag":"scope=default", "gpus":1.0, "cpus":4.0});
    let status = admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d", &deployment.to_string(),
        "-o", "/dev/null", "-w", "%{http_code}", &format!("{}/namespaces/{id}/allocations", h.http),
        ],
    );
    assert_eq!(status, "200");
    let run = serde_json::json!({"id":"overflow", "tag":"scope=default", "gpus":1.0, "cpus":5.0});
    let status = admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d", &run.to_string(),
        "-o", "/dev/null", "-w", "%{http_code}", &format!("{}/namespaces/{id}/allocations", h.http),
        ],
    );
    assert_eq!(status, "429", "an app's reserved capacity must reduce available quota");
    let status = admin_curl(&h, &["-X", "DELETE", "-o", "/dev/null", "-w", "%{http_code}",
        &format!("{}/allocations/deployment:ethproofs-gpu-0", h.http),
        ],
    );
    assert_eq!(status, "204");
    let status = admin_curl(&h, &["-X", "POST", "-H", "content-type: application/json", "-d", &run.to_string(),
        "-o", "/dev/null", "-w", "%{http_code}", &format!("{}/namespaces/{id}/allocations", h.http),
        ],
    );
    assert_eq!(status, "200");
}

/// Machine execution commands are delivered on the authenticated control listener;
/// a client needs no route to the machine or Ray itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn machine_heartbeat_requires_cluster_token_and_populates_control_plane_nodes() {
    use fleet_proto::fleet::v1::{worker_service_client::WorkerServiceClient, WorkerHeartbeatRequest,
    };
    let h = start().await;
    let mut worker = WorkerServiceClient::connect(format!("http://127.0.0.1:{}", h.control_port)).await.unwrap();
    let report = serde_json::from_value::<fleet_proto::model::v1::NodeReport>(serde_json::json!({
        "version": 3, "machine": "gpu-machine", "ready": true,
        "total_gpus": 8, "total_cpus": 48, "free_gpus": 8, "free_cpus": 48,
    })).unwrap();
    let heartbeat = || WorkerHeartbeatRequest { worker_id: "gpu-1".into(), session: "agent-session".into(),
        report: Some(report.clone()), proof_fetch_ready: false,
    };
    assert_eq!(worker.heartbeat(heartbeat()).await.unwrap_err().code(),tonic::Code::Unauthenticated);
    let mut wrong = Request::new(heartbeat());
    wrong.metadata_mut().insert("authorization", "Bearer wrong-token".parse().unwrap());
    assert_eq!(worker.heartbeat(wrong).await.unwrap_err().code(),tonic::Code::Unauthenticated);
    let mut request = Request::new(heartbeat());
    request.metadata_mut().insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
    assert_eq!(worker.heartbeat(request).await.unwrap().into_inner().heartbeat_secs,2);
    let status = admin_curl(&h,&[&format!("{}/status",h.http)]);
    let status: serde_json::Value = serde_json::from_str(&status).unwrap();
    assert!(status["nodes"].as_array().unwrap().iter().any(|n| n["name"]=="gpu-1" && n["backend"]=="ray-local"));
}

#[tokio::test(flavor = "multi_thread")]
async fn integrated_app_worker_rejects_sdk_jobs_in_its_own_namespace() {
    use fleet_proto::fleet::v1::{worker_service_client::WorkerServiceClient, WorkerHeartbeatRequest,
    };
    let h = start().await;
    create_namespace(&h, "alice", Some(serde_json::json!({"gpus":2,"cpus":8})));
    let elf = artifacts::hash(b"elf");
    let app = app_in("alice", &elf);
    let (key, _, token) = namespace_token(&h, "alice", 94, &elf);
    let channel = Channel::from_shared(format!("http://127.0.0.1:{}", h.control_port)).unwrap().connect().await.unwrap();
    let mut agent = WorkerServiceClient::with_interceptor(channel, fleet_scheduler::token::BearerTokenInterceptor::new(TOKEN).unwrap(),
    );
    let report = serde_json::json!({"version":3,"machine":"test","ready":true,
        "total_gpus":2,"total_cpus":8,"free_gpus":1,"free_cpus":6,"batches":{},
        "apps":[{"name":"ethproofs","kind":"ziren-gpu","namespace":"alice",
            "app":app,"elf_id":elf,"worker_command":["/opt/eth-proofs"],
            "machine":"test","replicas":1,"gpus":1,"cpus":2,"ready":true,"disabled":false}]});
    agent.heartbeat(WorkerHeartbeatRequest {worker_id:"integrated-worker".into(),session:"session".into(),
            report: Some(serde_json::from_value(report).unwrap()),proof_fetch_ready:false,
        }).await.unwrap();
    let (status, error) = submit(&h, &token, &app, &signed_request(&h, "owned-stream", b"elf", &key)).await;
    assert_eq!(status, S::InvalidParameter as i32);
    assert!(error.contains("owns its input stream"), "{error}");
    assert_refused(&wait_done(&h, &token, &app, "owned-stream").await);
    let (_, access) = user_http(&h, &token, "GET", "/access");
    assert_eq!(serde_json::from_str::<serde_json::Value>(&access).unwrap()["usage"]["gpus"], 1.0);
}
