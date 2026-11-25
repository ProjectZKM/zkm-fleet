//! Administrative exec is bound to a created, live allocation and the approved
//! native process identity, and is delivered whole through the stock gateway's
//! interactive exec. The gateway is a double: a real exec needs a supervisor.
use super::*;
use crate::openshell::{ExecGateway, ExecOutput};
use tokio::sync::Notify;

#[derive(Default)]
struct Gateway {
    starts: Mutex<Vec<protocol::ExecSandboxRequest>>,
    stdin: Mutex<Vec<Vec<u8>>>,
    hold: Option<Arc<Notify>>,
    fail: AtomicBool,
}
#[async_trait::async_trait]
impl ExecGateway for Gateway {
    async fn create(&self, _: &str, _: protocol::CreateSandboxRequest) -> trusted_shim::rail::Result<()> {
        Err(trusted_shim::rail::Error::Unavailable)
    }
    async fn exec(&self, start: protocol::ExecSandboxRequest, stdin: Vec<u8>) -> trusted_shim::rail::Result<ExecOutput> {
        self.starts.lock().unwrap().push(start);
        self.stdin.lock().unwrap().push(stdin.clone());
        if let Some(hold) = &self.hold {
            hold.notified().await;
        }
        if self.fail.load(Ordering::SeqCst) {
            return Err(trusted_shim::rail::Error::Unavailable);
        }
        Ok(ExecOutput { stdout: stdin, stderr: b"warn".to_vec(), exit_code: 7 })
    }
}

async fn created(gateway: Arc<Gateway>) -> (tempfile::TempDir, Arc<WorkloadController>, String, NodeReport) {
    let (dir, c, report, mut policy, mut request) = policy_setup_with_inventory("openshell", |_| {}).await;
    request.operation = "runtime.create".into();
    for module in policy.modules.values_mut() {
        *module = module.replace("input.operation == \"read\"", "input.operation == \"runtime.create\"");
    }
    policy.policy.sha256 = policy_digest(&policy).unwrap();
    activate(&c, &policy, &request).await;
    let mut c = Arc::try_unwrap(c).ok().unwrap();
    Arc::get_mut(c.policy.as_mut().unwrap()).unwrap().openshell = Some(Arc::new(
        crate::openshell::Launches::open(&dir.path().join("launches"), admission()).unwrap().with_control(gateway),
    ));
    let c = Arc::new(c);
    let mut launch = provisioning(&request).request;
    launch.policy = policy.policy.clone();
    let native = SandboxPolicy {
        version: 1,
        process: Some(protocol::sandbox::v1::ProcessPolicy { run_as_user: "1000".into(), run_as_group: "1000".into() }),
        ..Default::default()
    };
    let prepared = json_body(admin(&c, "POST", "/policy/openshell/launches", json!({"request":launch,
        "native_policy_base64":STANDARD.encode(native.encode_to_vec()),"principal":"alice",
        "expires_at":crate::workloads::now() as u64 + 300,"cost":1})).await).await;
    (dir, c, prepared["name"].as_str().unwrap().to_owned(), report)
}

fn phase(c: &WorkloadController, name: &str, phase: &str) {
    c.policy.as_ref().unwrap().openshell.as_ref().unwrap().test_phase(name, phase);
}

async fn exec(c: &Arc<WorkloadController>, name: &str, body: Value) -> Response {
    admin(c, "POST", &format!("/policy/openshell/launches/{name}/exec"), body).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_exec_runs_only_in_a_created_live_allocation_as_its_policy_user() {
    let gateway = Arc::new(Gateway::default());
    let (_dir, c, name, report) = created(gateway.clone()).await;
    let input = vec![0x5a_u8; 1024 * 1024 + 17];
    let body = json!({"command":["sh","-c","cat; echo \"$1\"","x","a b"],"stdin":STANDARD.encode(&input),"timeout_seconds":42});

    // Not before this allocation's own delivery created the sandbox.
    for early in ["prepared", "proving", "delivered"] {
        phase(&c, &name, early);
        assert_eq!(exec(&c, &name, body.clone()).await.status(), 403, "{early}");
    }
    assert!(gateway.starts.lock().unwrap().is_empty());
    phase(&c, &name, "created");

    let out = json_body(exec(&c, &name, body.clone()).await).await;
    assert_eq!(STANDARD.decode(out["stdout"].as_str().unwrap()).unwrap(), input);
    assert_eq!(STANDARD.decode(out["stderr"].as_str().unwrap()).unwrap(), b"warn");
    assert_eq!(out["exit_code"], 7);
    let start = gateway.starts.lock().unwrap()[0].clone();
    assert_eq!(start.sandbox, name);
    assert_eq!(start.command, ["sh", "-c", "cat; echo \"$1\"", "x", "a b"]);
    assert!(!start.tty && start.no_login_shell && start.environment.is_empty() && start.workdir.is_empty());
    assert_eq!(start.execution_timeout.unwrap().seconds, 42);
    assert!(start.workspace_scope.is_some());
    let first = uuid::Uuid::parse_str(&start.request_id).unwrap();
    assert_eq!(gateway.stdin.lock().unwrap()[0], input);

    // Each call is a distinct admission; a retry never replays an old one.
    json_body(exec(&c, &name, body.clone()).await).await;
    assert_ne!(gateway.starts.lock().unwrap()[1].request_id, first.to_string());

    // The approved native process identity, never a caller-chosen one.
    let mut own = body.clone(); own["user"] = json!("1000");
    json_body(exec(&c, &name, own).await).await;
    for user in ["root", "0", ""] {
        let mut other = body.clone(); other["user"] = json!(user);
        assert_eq!(exec(&c, &name, other).await.status(), 403, "{user}");
    }

    // Bounds match shim's Docker control listener.
    let calls = gateway.starts.lock().unwrap().len();
    for (field, value) in [("command", json!([])), ("command", json!(vec!["a"; 257])), ("command", json!(["a\u{0}b"])),
        ("command", json!(["a".repeat(256 * 1024)])), ("timeout_seconds", json!(0)), ("timeout_seconds", json!(601)),
        ("stdin", json!("not base64!")), ("env", json!({"A":"b"}))] {
        let mut bad = body.clone(); bad[field] = value;
        assert!(exec(&c, &name, bad).await.status().is_client_error(), "{field}");
    }
    assert_eq!(gateway.starts.lock().unwrap().len(), calls);

    // An unknown outcome is never reported as success.
    gateway.fail.store(true, Ordering::SeqCst);
    assert_eq!(exec(&c, &name, body.clone()).await.status(), 503);
    gateway.fail.store(false, Ordering::SeqCst);

    // Losing the owning node's live reservation closes control immediately.
    let mut gone = report.clone();
    gone.runtime_instances.clear();
    c.report("gpu", "session", gone).await.unwrap();
    assert_eq!(exec(&c, &name, body.clone()).await.status(), 403);
    c.report("gpu", "session", report).await.unwrap();
    json_body(exec(&c, &name, body).await).await;
    assert_eq!(exec(&c, "f0000000000000000", json!({"command":["true"],"timeout_seconds":1})).await.status(), 403);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_exec_is_bounded_in_concurrency_and_stdin_frames() {
    let hold = Arc::new(Notify::new());
    let gateway = Arc::new(Gateway { hold: Some(hold.clone()), ..Default::default() });
    let (_dir, c, name, _) = created(gateway.clone()).await;
    phase(&c, &name, "created");
    let body = json!({"command":["true"],"timeout_seconds":5});
    let mut held = Vec::new();
    for _ in 0..4 {
        let (c, name, body) = (c.clone(), name.clone(), body.clone());
        held.push(tokio::spawn(async move { exec(&c, &name, body).await.status() }));
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while gateway.starts.lock().unwrap().len() < 4 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(exec(&c, &name, body.clone()).await.status(), 503);
    for _ in 0..4 {
        hold.notify_one();
    }
    for task in held {
        assert_eq!(task.await.unwrap(), 200);
    }

    // Every stdin frame stays well under the stock gateway's 1 MiB message limit.
    let input = vec![1_u8; 5 * 1024 * 1024 + 3];
    let frames: Vec<_> = crate::openshell::stdin_frames(&input).collect();
    assert!(frames.iter().all(|f| f.encoded_len() < 1024 * 1024));
    let rebuilt: Vec<u8> = frames
        .into_iter()
        .flat_map(|f| match f.payload {
            Some(protocol::exec_sandbox_input::Payload::Stdin(data)) => data,
            _ => panic!("stdin frames only"),
        })
        .collect();
    assert_eq!(rebuilt, input);
}
