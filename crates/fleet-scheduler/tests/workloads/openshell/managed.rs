//! Native services beside the application route: single-use ingress that
//! forwards the caller's own credential, logs-only control, administrative exec
//! through the stock gateway and a durable volume. Real supervisor, router,
//! gateway and workload; proof and route-grant authorities are fixtures.
use super::*;
use crate::openshell::{ExecGateway, ExecOutput, StockGateway};

/// A world-writable directory in the workload image: a fresh named volume takes
/// its owner and mode, so the policy user can write without any chown.
pub(super) const VOLUME_TARGET: &str = "/var/tmp";
const CHAT: &str = "/v1/chat/completions";
const SERVICES: &[(&str, &str, &str)] = &[("chat", "http1", "ingress:chat"), ("control", "http1", "control")];

pub(super) fn rule() -> Value {
    json!({"paths":[CHAT],"methods":["POST"],"single_use":true})
}

pub(super) fn permissive() -> String {
    trusted_shim::openshell_gateway::resource_admission::DriverAdmissionConfig {
        allow_driver_config: true,
        ..Default::default()
    }
    .acknowledgement()
}

/// The stock gateway client, bound once the gateway's certificates exist.
#[derive(Default)]
pub(super) struct LateGateway(pub(super) std::sync::OnceLock<StockGateway>);
#[async_trait::async_trait]
impl ExecGateway for LateGateway {
    async fn exec(&self, start: protocol::ExecSandboxRequest, stdin: Vec<u8>) -> trusted_shim::rail::Result<ExecOutput> {
        self.0.get().ok_or(trusted_shim::rail::Error::Unavailable)?.exec(start, stdin).await
    }
}

/// The shim-provisioned volume named in the public request; removed on drop,
/// including after a failed assertion that left its sandbox running.
pub(super) struct Volume(String);
impl Drop for Volume {
    fn drop(&mut self) {
        let docker = |args: &[&str]| std::process::Command::new("docker").args(args).output();
        let filter = format!("volume={}", self.0);
        if let Ok(out) = docker(&["ps", "-aq", "--filter", &filter]) {
            for id in String::from_utf8_lossy(&out.stdout).split_whitespace() {
                let _ = docker(&["rm", "-f", id]);
            }
        }
        let _ = docker(&["volume", "rm", "-f", &self.0]);
    }
}

pub(super) fn volume(prepared: &Value) -> Volume {
    let wire = STANDARD.decode(prepared["create_request_base64"].as_str().unwrap()).unwrap();
    let public = protocol::CreateSandboxRequest::decode(wire.as_slice()).unwrap();
    let config = public.spec.unwrap().template.unwrap().driver_config.unwrap();
    let docker = match &config.fields["docker"].kind {
        Some(prost_types_native::value::Kind::StructValue(s)) => s.clone(),
        _ => panic!("docker driver config"),
    };
    let mounts = match &docker.fields["mounts"].kind {
        Some(prost_types_native::value::Kind::ListValue(l)) => l.values.clone(),
        _ => panic!("mounts"),
    };
    assert_eq!(mounts.len(), 1);
    let source = match &mounts[0].kind {
        Some(prost_types_native::value::Kind::StructValue(m)) => match &m.fields["source"].kind {
            Some(prost_types_native::value::Kind::StringValue(s)) => s.clone(),
            _ => panic!("volume source"),
        },
        _ => panic!("mount"),
    };
    Volume(source)
}

/// Data written inside the sandbox outlives the sandbox itself.
pub(super) fn durable(volume: &Volume) {
    let output = std::process::Command::new("docker")
        .args(["run", "--rm", "--network", "none", "-v"])
        .arg(format!("{}:/v:ro", volume.0))
        .args(["alpine:3.21", "cat", "/v/marker"])
        .output()
        .unwrap();
    assert!(output.status.success(), "durable volume readable after deletion");
    assert_eq!(String::from_utf8_lossy(&output.stdout), "durable\n");
}

async fn exec(c: &Arc<WorkloadController>, name: &str, body: Value) -> Response {
    admin(c, "POST", &format!("/policy/openshell/launches/{name}/exec"), body).await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn exercise(
    root: &Path,
    c: Arc<WorkloadController>,
    mut report: NodeReport,
    execution: &ExecutionIdentity,
    shim_socket: &Path,
    name: &str,
    observer: crate::openshell::Observer,
) {
    let route_root = root.join("services-router");
    std::fs::create_dir(&route_root).unwrap();
    let route = route::Route::managed_services(&route_root, execution, c.clone(), SERVICES).await;
    route.control_ready.store(true, Ordering::SeqCst);
    let heartbeat = {
        let c = c.clone();
        let socket = shim_socket.to_owned();
        let execution = execution.clone();
        tokio::spawn(async move {
            loop {
                if let Ok(status) = try_shim_call(
                    &socket,
                    json!({"operation":"inspect","execution":execution,"driver":"openshell"}),
                )
                .await
                {
                    let ready = status["application_ready"] == true;
                    report.apps[0]["ready"] = json!(ready);
                    report.runtime_instances[0].ready = ready;
                    report.runtime_instances[0].services =
                        if ready { SERVICES.iter().map(|s| s.0.to_owned()).collect() } else { vec![] };
                    report.runtime_instances[0].native_observation_base64 =
                        status["native_observation_base64"].as_str().map(str::to_owned);
                    c.report("gpu", "session", report.clone()).await.unwrap();
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
    };
    let observation = tokio::spawn(observer.run(c.clone()));
    shim_call(shim_socket, json!({"operation":"network","driver":"openshell","execution":execution,"config":route.config})).await;
    tokio::time::timeout(Duration::from_secs(30), async {
        while !(route.service_ready("chat").await && route.service_ready("control").await) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("both declared services register");

    // Ingress: the application's own credential, exactly one bound request.
    let body = br#"{"messages":[{"role":"user","content":"hi"}]}"#;
    let chat = |grant: String, method: reqwest::Method, path: &str, body: &[u8]| {
        let (route_id, _) = route.service("chat");
        route
            .client_for_tests()
            .request(method, format!("{}/v1/http/{route_id}{path}", route.base_url()))
            .header("x-trusted-access", grant)
            .bearer_auth("caller-app-key")
            .body(body.to_vec())
    };
    let grant = route.bound("chat", "POST", CHAT, body);
    let response = chat(grant.clone(), reqwest::Method::POST, CHAT, body).send().await.unwrap();
    assert_eq!(response.status(), 200);
    let mut expected = b"Bearer caller-app-key|".to_vec();
    expected.extend_from_slice(body);
    assert_eq!(response.bytes().await.unwrap().as_ref(), expected.as_slice());
    let replay = chat(grant, reqwest::Method::POST, CHAT, body).send().await.unwrap();
    assert!(!replay.status().is_success(), "a bound request is never served twice");
    // An unbound grant, another body, path or method never reaches the app.
    let (_, unbound) = route.service("chat");
    assert!(!chat(unbound, reqwest::Method::POST, CHAT, body).send().await.unwrap().status().is_success());
    let tampered = route.bound("chat", "POST", CHAT, body);
    assert!(!chat(tampered, reqwest::Method::POST, CHAT, b"{}").send().await.unwrap().status().is_success());
    for (method, path) in [("POST", "/admin"), ("GET", CHAT)] {
        let grant = route.bound("chat", method, path, b"");
        let method = reqwest::Method::from_bytes(method.as_bytes()).unwrap();
        assert!(!chat(grant, method, path, b"").send().await.unwrap().status().is_success(), "{path}");
    }

    // Control: application logs only; Docker-style control routes do not exist.
    let logs = route.request("control", reqwest::Method::GET, "logs?tail=200").send().await.unwrap();
    assert_eq!(logs.status(), 200);
    let logs: Value = logs.json().await.unwrap();
    // Base64, the same shape as shim's Docker control listener.
    let stdout = String::from_utf8(STANDARD.decode(logs["stdout"].as_str().unwrap()).unwrap()).unwrap();
    assert!(stdout.contains("native fixture listening"), "application stdout: {stdout:?}");
    assert!(!route.request("control", reqwest::Method::GET, "logs?tail=0").send().await.unwrap().status().is_success());
    for (method, path) in [("GET", "metadata"), ("POST", "exec"), ("POST", "restart")] {
        let method = reqwest::Method::from_bytes(method.as_bytes()).unwrap();
        assert!(!route.request("control", method, path).json(&json!({})).send().await.unwrap().status().is_success(), "{path}");
    }

    // Exec runs through the stock gateway as the policy user and can write the
    // durable volume. The caller cannot choose another identity.
    let script = format!("id -u; echo durable > {VOLUME_TARGET}/marker; cat {VOLUME_TARGET}/marker");
    let response = exec(&c, name, json!({"command":["sh","-c",script],"timeout_seconds":60})).await;
    assert_eq!(response.status(), 200);
    let out = json_body(response).await;
    assert_eq!(out["exit_code"], 0, "stderr: {}", String::from_utf8_lossy(&STANDARD.decode(out["stderr"].as_str().unwrap()).unwrap()));
    assert_eq!(String::from_utf8(STANDARD.decode(out["stdout"].as_str().unwrap()).unwrap()).unwrap(), "1000\ndurable\n");
    let stdin = vec![b'x'; 3 * 1024 * 1024 + 5];
    let out = json_body(exec(&c, name, json!({"command":["wc","-c"],"stdin":STANDARD.encode(&stdin),"timeout_seconds":60})).await).await;
    assert_eq!(String::from_utf8(STANDARD.decode(out["stdout"].as_str().unwrap()).unwrap()).unwrap().trim(), stdin.len().to_string());
    let out = json_body(exec(&c, name, json!({"command":["sh","-c","exit 3"],"timeout_seconds":10})).await).await;
    assert_eq!(out["exit_code"], 3);
    assert_eq!(exec(&c, name, json!({"command":["id"],"user":"root","timeout_seconds":10})).await.status(), 403);

    shim_call(shim_socket, json!({"operation":"delete","execution":execution,"driver":"openshell"})).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while route.service_ready("chat").await || route.service_ready("control").await {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("deletion withdraws every service");
    heartbeat.abort();
    observation.abort();
}
