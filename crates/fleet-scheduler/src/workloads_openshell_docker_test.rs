//! Opt-in stock gateway + stock Docker driver acceptance. Only proofs are doubled.
use super::*;
use std::{path::Path, process::Command};

// Every Docker operation is scoped to this test's unique operator namespace.
struct DockerScope(String);
impl DockerScope {
    fn ids(&self, kind: &str) -> Vec<String> {
        let output = Command::new("docker")
            .args([
                kind,
                "ls",
                if kind == "container" { "-aq" } else { "-q" },
                "--filter",
            ])
            .arg(format!("label=openshell.ai/sandbox-namespace={}", self.0))
            .output()
            .unwrap();
        assert!(output.status.success(), "Docker resource listing failed");
        String::from_utf8(output.stdout)
            .unwrap()
            .split_whitespace()
            .map(str::to_owned)
            .collect()
    }
}
impl Drop for DockerScope {
    fn drop(&mut self) {
        // Panic cleanup is independent of gateway/shim availability.
        for kind in ["container", "volume"] {
            for id in self.ids(kind) {
                let _ = Command::new("docker")
                    .args([kind, "rm", "-f", &id])
                    .output();
            }
        }
    }
}

async fn driver(
    root: &Path,
    socket: &Path,
    port: u16,
    scope: &DockerScope,
) -> tokio::process::Child {
    let binary =
        std::env::var_os("OPENSHELL_DOCKER_DRIVER_BINARY").expect("native Docker driver binary");
    let runtime =
        std::env::var("OPENSHELL_TEST_SANDBOX_IMAGE").expect("pinned sandbox runtime image");
    let supervisor =
        std::env::var("OPENSHELL_TEST_SUPERVISOR_IMAGE").expect("pinned supervisor image");
    std::fs::write(root.join("driver.toml"), format!(
        "sandbox_label={:?}\ngrpc_endpoint=\"https://127.0.0.1:{port}\"\nsupervisor_network_mode=\"host\"\nguest_tls_ca={:?}\nsandbox_runtime_image={runtime:?}\nsupervisor_image={supervisor:?}\n",
        scope.0, root.join("ca.pem")
    )).unwrap();
    let log = std::fs::File::create(root.join("driver.log")).unwrap();
    let mut child = tokio::process::Command::new(binary)
        .arg("--config")
        .arg(root.join("driver.toml"))
        .arg("--bind-socket")
        .arg(socket)
        .arg("--gateway-bind")
        .arg(format!("127.0.0.1:{port}"))
        .env("XDG_STATE_HOME", root.join("native-state"))
        .env("XDG_CONFIG_HOME", root.join("native-config"))
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(45), async {
        while !socket.exists() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "stock driver exited; inspect private acceptance logs"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("stock driver readiness");
    child
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires stock gateway/driver binaries, pinned runtime images, Docker and openssl"]
async fn stock_docker_startup_reaches_gateway_readiness_and_cleans_up_offline() {
    let image = std::env::var("OPENSHELL_TEST_WORKLOAD_IMAGE").expect("pinned workload image");
    let command = vec!["/bin/sleep".to_string(), "300".to_string()];
    let (dir, c, mut report, mut policy, mut request) =
        policy_setup_with_inventory("openshell", |report| {
            report.apps[0]["image"] = json!(image);
            report.apps[0]["command"] = json!(command);
        })
        .await;
    // Retain private diagnostics outside the repository, including on failure.
    let root = dir.keep();
    eprintln!("private Docker acceptance artifacts: {}", root.display());
    let scope = DockerScope(format!(
        "fleet-acceptance-{}",
        uuid::Uuid::new_v4().simple()
    ));
    request.operation = "runtime.create".into();
    request.validity.expires_at = now() as u64 + 600;
    for module in policy.modules.values_mut() {
        *module = module.replace(
            "input.operation == \"read\"",
            "input.operation == \"runtime.create\"",
        );
    }
    policy.policy.sha256 = policy_digest(&policy).unwrap();
    activate(&c, &policy, &request).await;
    let network = Arc::new(Network::default());
    let mut c = Arc::try_unwrap(c).ok().unwrap();
    let p = Arc::get_mut(c.policy.as_mut().unwrap()).unwrap();
    p.test_network(network.clone());
    p.openshell = Some(Arc::new(
        crate::openshell::Launches::open(&root.join("launches"), admission()).unwrap(),
    ));
    let c = Arc::new(c);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let router = crate::http::router(crate::http::HttpState {
        fleet: c.fleet.clone(),
        workloads: c.clone(),
    });
    let http = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let compute_socket = root.join("compute.sock");
    let cc = c.clone();
    let path = compute_socket.clone();
    let compute = tokio::spawn(async move { crate::openshell::serve(cc, path).await.unwrap() });
    socket_ready(&compute_socket).await;
    let api_port = gateway::port();
    let (mut gateway_process, mut gateway) =
        gateway::start_at(&root, &compute_socket, api_port).await;
    let native_socket = root.join("native.sock");
    let mut native = driver(&root, &native_socket, api_port, &scope).await;
    let state = root.join("shim");
    let shim_socket = state.join("control.sock");
    let stop = tokio_util::sync::CancellationToken::new();
    let shutdown = stop.clone();
    let ledger_endpoint = address.clone();
    let daemon = tokio::spawn(async move {
        trusted_shim::daemon::serve_configured(
            &state,
            "machine".into(),
            trusted_shim::daemon::DaemonConfig {
                openshell: Some(trusted_shim::openshell_daemon::OpenShellConfig {
                    socket: native_socket,
                    admission_policy: admission(),
                    ledger_endpoint,
                    ledger_ca_file: None,
                }),
            },
            shutdown,
        )
        .await
        .unwrap()
    });
    socket_ready(&shim_socket).await;
    shim_call(
        &shim_socket,
        json!({"operation":"allocate","execution":request.execution,"driver":"openshell"}),
    )
    .await;
    let mut launch = provisioning(&request).request;
    launch.policy = policy.policy.clone();
    launch.image = image;
    launch.command = command;
    let native_policy = SandboxPolicy {
        version: 1,
        process: Some(protocol::sandbox::v1::ProcessPolicy {
            run_as_user: "1000".into(),
            run_as_group: "1000".into(),
        }),
        filesystem: Some(protocol::sandbox::v1::FilesystemPolicy {
            read_only: ["/usr", "/lib", "/lib64", "/bin", "/etc"]
                .map(str::to_owned)
                .to_vec(),
            read_write: vec!["/tmp".into()],
            include_workdir: true,
        }),
        ..Default::default()
    };
    let response = admin(
        &c,
        "POST",
        "/policy/openshell/launches",
        json!({
            "request":launch,"native_policy_base64":STANDARD.encode(native_policy.encode_to_vec()),
            "principal":"alice","expires_at":request.validity.expires_at,"cost":1
        }),
    )
    .await;
    assert_eq!(response.status(), 200);
    let prepared = json_body(response).await;
    let bytes = STANDARD
        .decode(prepared["create_request_base64"].as_str().unwrap())
        .unwrap();
    let public = protocol::CreateSandboxRequest::decode(bytes.as_slice()).unwrap();
    let query = protocol::GetSandboxRequest {
        name: public.name.clone(),
        workspace_scope: public.workspace_scope.clone(),
    };
    gateway
        .create_workspace(protocol::CreateWorkspaceRequest {
            name: prepared["workspace"].as_str().unwrap().into(),
            ..Default::default()
        })
        .await
        .unwrap();
    let mut creator = gateway.clone();
    let mut creation =
        tokio::spawn(async move { creator.create_sandbox(public).await.map(|_| ()) });
    tokio::time::timeout(Duration::from_secs(15), async {
        while network.submitted.load(Ordering::SeqCst) == 0 {
            if creation.is_finished() {
                panic!(
                    "creation ended before proof: {:?}",
                    (&mut creation).await.unwrap()
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    assert!(scope.ids("container").is_empty());
    assert!(c.deliveries.offers("gpu").is_empty());
    network.done.store(true, Ordering::SeqCst);
    let offer = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(o) = c.deliveries.offers("gpu").first() {
                break o.clone();
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    let node = node_delivery::DeliveryClient {
        http: reqwest::Client::new(),
        scheduler: address,
        secret: "cluster".into(),
        worker: "gpu".into(),
        session: "session".into(),
        socket: shim_socket.clone(),
    };
    node.deliver(offer.clone()).await;
    assert_eq!(
        c.deliveries.record(&offer.ticket).unwrap().state,
        "acknowledged"
    );
    let mut compute_client = compute_client(compute_socket).await;
    let mut previous = None;
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            let observed = shim_call(
                &shim_socket,
                json!({"operation":"inspect","execution":request.execution,"driver":"openshell"}),
            )
            .await;
            report.runtime_instances[0].native_observation_base64 = observed
                ["native_observation_base64"]
                .as_str()
                .map(str::to_owned);
            c.report("gpu", "session", report.clone()).await.unwrap();
            assert!(c.native_observation(&request.execution).unwrap().is_some());
            let native = compute_client
                .get_sandbox(GetSandboxRequest {
                    sandbox_id: observed["sandbox_id"].as_str().unwrap().into(),
                    name: query.name.clone(),
                })
                .await
                .unwrap()
                .into_inner()
                .sandbox
                .unwrap();
            let native_instance = native.status.unwrap().instance_id;
            let sandbox = gateway
                .get_sandbox(query.clone())
                .await
                .unwrap()
                .into_inner()
                .sandbox
                .unwrap();
            let status = sandbox.status.unwrap();
            let progress = (
                status.phase,
                status
                    .conditions
                    .iter()
                    .map(|c| (c.r#type.clone(), c.status.clone(), c.reason.clone()))
                    .collect::<Vec<_>>(),
            );
            if previous.as_ref() != Some(&progress) {
                eprintln!("gateway progress: {progress:?}");
                previous = Some(progress);
            }
            if status.phase == protocol::SandboxPhase::Ready as i32 && creation.is_finished() {
                assert!(!native_instance.is_empty());
                assert!(status
                    .conditions
                    .iter()
                    .any(|c| c.r#type == "Ready" && c.status == "True"));
                assert_eq!(status.configuration_activated, Some(true));
                break;
            }
            assert_ne!(
                status.phase,
                protocol::SandboxPhase::Error as i32,
                "gateway conditions: {:?}",
                status.conditions
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("joined native runtime / supervisor readiness");
    tokio::time::timeout(Duration::from_secs(10), creation)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(network.submitted.load(Ordering::SeqCst), 1);
    assert!(!report.runtime_instances[0].ready && report.runtime_instances[0].services.is_empty());
    let containers = scope.ids("container");
    let output = Command::new("docker")
        .arg("inspect")
        .args(&containers)
        .output()
        .unwrap();
    assert!(output.status.success());
    let inspected: Vec<Value> = serde_json::from_slice(&output.stdout).unwrap();
    let workload = inspected
        .iter()
        .find(|v| v["Config"]["Labels"]["openshell.ai/isolation-role"] == "sandbox")
        .unwrap();
    assert_eq!(workload["HostConfig"]["NetworkMode"], "none");
    assert_eq!(workload["HostConfig"]["NanoCpus"], 2_000_000_000_u64);
    assert_eq!(workload["HostConfig"]["Memory"], 134_217_728_u64);
    assert_eq!(workload["HostConfig"]["Privileged"], false);
    // A still-connected supervisor must not preserve gateway readiness after
    // Fleet loses the owning node session.
    c.end_session("gpu", "session");
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let status = gateway
                .get_sandbox(query.clone())
                .await
                .unwrap()
                .into_inner()
                .sandbox
                .unwrap()
                .status
                .unwrap();
            if status.phase == protocol::SandboxPhase::Error as i32 {
                assert!(status.conditions.iter().any(|c| c.r#type == "Ready"
                    && c.status == "False"
                    && c.reason == "FleetObservationUnavailable"));
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("lost Fleet owner must revoke joined readiness");
    // Cleanup must not require the gateway or central ledger to be available.
    gateway_process.kill().await.unwrap();
    compute.abort();
    http.abort();
    shim_call(
        &shim_socket,
        json!({"operation":"delete","execution":request.execution,"driver":"openshell"}),
    )
    .await;
    assert!(scope.ids("container").is_empty());
    stop.cancel();
    daemon.await.unwrap();
    native.kill().await.unwrap();
}
