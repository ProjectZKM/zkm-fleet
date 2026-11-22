//! Real Fleet HTTP + private compute gRPC + node delivery + shim daemon. Native
//! compute and remote proofs are explicit doubles; no local prover is started.
use super::*;
use base64::{engine::general_purpose::STANDARD, Engine};
use prost_native::Message;
use std::{
    os::unix::fs::PermissionsExt,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Mutex,
    },
    time::Duration,
};
use tonic_native::{Request as NativeRequest, Response as NativeResponse, Status};
use trusted_shim::openshell_gateway::{
    extension_protocol::{extension_metadata, ExtensionFamily},
    protocol::{self, compute::v1::*},
    SandboxPolicy,
};

// Exercise the actual production node forwarder against this local cluster.
#[path = "../../fleet-node/src/runtime_delivery.rs"]
mod node_delivery;

fn admission() -> String {
    trusted_shim::openshell_gateway::resource_admission::DriverAdmissionConfig::default()
        .acknowledgement()
}

#[derive(Default)]
struct Network {
    submitted: AtomicUsize,
    done: AtomicBool,
    forged: AtomicBool,
    public: Mutex<BTreeMap<String, Vec<u8>>>,
}
#[async_trait::async_trait]
impl trusted_shim::proof::ProofNetwork for Network {
    async fn submit(&self, r: &trusted_shim::proof::ProofRequest) -> trusted_shim::Result<String> {
        self.submitted.fetch_add(1, Ordering::SeqCst);
        let public = evaluate_witness(&r.witness).unwrap();
        assert_eq!(public, r.expected_public_values);
        self.public
            .lock()
            .unwrap()
            .insert(r.request_id.clone(), public);
        // Deliberately lose the submission response. The same ID must be polled.
        Err(trusted_shim::Error::Unavailable)
    }
    async fn status(&self, id: &str) -> trusted_shim::Result<trusted_shim::proof::ProofStatus> {
        use trusted_shim::proof::ProofStatus;
        if !self.done.load(Ordering::SeqCst) {
            return Ok(ProofStatus::Pending);
        }
        Ok(ProofStatus::Complete(ProofReceipt {
            job_id: id.into(),
            encoded_receipt: if self.forged.load(Ordering::SeqCst) {
                b"forged".to_vec()
            } else {
                self.public.lock().unwrap()[id].clone()
            },
        }))
    }
}

#[derive(Clone, Default)]
struct Native {
    current: Arc<Mutex<Option<DriverSandbox>>>,
    creates: Arc<AtomicUsize>,
    input: Arc<Mutex<Option<DriverSandbox>>>,
}
#[tonic_native::async_trait]
impl compute_driver_server::ComputeDriver for Native {
    type WatchSandboxesStream =
        tokio_stream::wrappers::ReceiverStream<std::result::Result<WatchSandboxesEvent, Status>>;
    async fn get_capabilities(
        &self,
        _: NativeRequest<GetCapabilitiesRequest>,
    ) -> std::result::Result<NativeResponse<GetCapabilitiesResponse>, Status> {
        Ok(NativeResponse::new(GetCapabilitiesResponse {
            driver_name: "docker".into(),
            resource_admission_policy: admission(),
            extension: Some(extension_metadata(
                ExtensionFamily::Compute,
                "test/native",
                "test",
                [],
            )),
            resource_capabilities: Some(ResourceCapabilities {
                cpu: Some(CpuResourceCapabilities {
                    limit_supported: true,
                }),
                memory: Some(MemoryResourceCapabilities {
                    limit_supported: true,
                }),
                gpu: None,
            }),
            ..Default::default()
        }))
    }
    async fn validate_sandbox_create(
        &self,
        _: NativeRequest<ValidateSandboxCreateRequest>,
    ) -> std::result::Result<NativeResponse<ValidateSandboxCreateResponse>, Status> {
        Ok(NativeResponse::new(ValidateSandboxCreateResponse {}))
    }
    async fn create_sandbox(
        &self,
        r: NativeRequest<CreateSandboxRequest>,
    ) -> std::result::Result<NativeResponse<CreateSandboxResponse>, Status> {
        self.creates.fetch_add(1, Ordering::SeqCst);
        let mut s = r.into_inner().sandbox.unwrap();
        *self.input.lock().unwrap() = Some(s.clone());
        s.status = Some(DriverSandboxStatus {
            instance_id: "native-instance".into(),
            agent_fd: "private-host:1".into(),
            sandbox_fd: "private-host:2".into(),
            conditions: vec![DriverCondition {
                r#type: "Ready".into(),
                status: "True".into(),
                message: "private-diagnostic".into(),
                ..Default::default()
            }],
            fence_evidence: Some(DriverFenceEvidence {
                generation: "generation".into(),
                evidence_digest: "a".repeat(64),
                resource_claims: Default::default(),
            }),
            ..Default::default()
        });
        *self.current.lock().unwrap() = Some(s);
        Ok(NativeResponse::new(CreateSandboxResponse {
            runtime_identity: "native-instance".into(),
        }))
    }
    async fn get_sandbox(
        &self,
        _: NativeRequest<GetSandboxRequest>,
    ) -> std::result::Result<NativeResponse<GetSandboxResponse>, Status> {
        Ok(NativeResponse::new(GetSandboxResponse {
            sandbox: Some(
                self.current
                    .lock()
                    .unwrap()
                    .clone()
                    .ok_or_else(|| Status::not_found("absent"))?,
            ),
        }))
    }
    async fn list_sandboxes(
        &self,
        _: NativeRequest<ListSandboxesRequest>,
    ) -> std::result::Result<NativeResponse<ListSandboxesResponse>, Status> {
        Ok(NativeResponse::new(ListSandboxesResponse {
            sandboxes: self.current.lock().unwrap().clone().into_iter().collect(),
        }))
    }
    async fn stop_sandbox(
        &self,
        _: NativeRequest<StopSandboxRequest>,
    ) -> std::result::Result<NativeResponse<StopSandboxResponse>, Status> {
        Ok(NativeResponse::new(StopSandboxResponse {}))
    }
    async fn delete_sandbox(
        &self,
        _: NativeRequest<DeleteSandboxRequest>,
    ) -> std::result::Result<NativeResponse<DeleteSandboxResponse>, Status> {
        *self.current.lock().unwrap() = None;
        Ok(NativeResponse::new(DeleteSandboxResponse { deleted: true }))
    }
    async fn start_sandbox(
        &self,
        _: NativeRequest<StartSandboxRequest>,
    ) -> std::result::Result<NativeResponse<StartSandboxResponse>, Status> {
        Err(Status::unimplemented("test"))
    }
    async fn suspend_sandbox(
        &self,
        _: NativeRequest<SuspendSandboxRequest>,
    ) -> std::result::Result<NativeResponse<SuspendSandboxResponse>, Status> {
        Err(Status::unimplemented("test"))
    }
    async fn authenticate_sandbox(
        &self,
        _: NativeRequest<AuthenticateSandboxRequest>,
    ) -> std::result::Result<NativeResponse<AuthenticateSandboxResponse>, Status> {
        Err(Status::unimplemented("test"))
    }
    async fn watch_sandboxes(
        &self,
        _: NativeRequest<WatchSandboxesRequest>,
    ) -> std::result::Result<NativeResponse<Self::WatchSandboxesStream>, Status> {
        Err(Status::unimplemented("test"))
    }
    async fn ensure_workspace(
        &self,
        _: NativeRequest<EnsureWorkspaceRequest>,
    ) -> std::result::Result<NativeResponse<EnsureWorkspaceResponse>, Status> {
        Ok(NativeResponse::new(EnsureWorkspaceResponse {}))
    }
    async fn delete_workspace(
        &self,
        _: NativeRequest<DeleteWorkspaceRequest>,
    ) -> std::result::Result<NativeResponse<DeleteWorkspaceResponse>, Status> {
        Ok(NativeResponse::new(DeleteWorkspaceResponse {}))
    }
}
async fn socket_ready(path: &std::path::Path) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await
        }
    })
    .await
    .unwrap();
}
async fn shim_call(path: &std::path::Path, command: Value) -> Value {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::net::UnixStream::connect(path).await.unwrap();
    s.write_all(
        &serde_json::to_vec(&json!({"protocol":1,"node_id":"machine","command":command})).unwrap(),
    )
    .await
    .unwrap();
    s.shutdown().await.unwrap();
    let mut bytes = vec![];
    s.read_to_end(&mut bytes).await.unwrap();
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert!(v.get("error").is_none(), "shim response: {v}");
    v["result"].clone()
}
async fn compute_client(
    path: std::path::PathBuf,
) -> compute_driver_client::ComputeDriverClient<tonic_native::transport::Channel> {
    let channel = tonic_native::transport::Endpoint::from_static("http://[::]:50051")
        .connect_with_connector(tower::service_fn(move |_| {
            let p = path.clone();
            async move {
                tokio::net::UnixStream::connect(p)
                    .await
                    .map(hyper_util::rt::TokioIo::new)
            }
        }))
        .await
        .unwrap();
    compute_driver_client::ComputeDriverClient::new(channel)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn startup_waits_for_proof_delivers_once_and_returns_only_fresh_owned_observations() {
    for forged in [false, true] {
        startup(forged, false).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires pinned unmodified OPENSHELL_GATEWAY_BINARY and openssl"]
async fn stock_gateway_startup_through_fleet_and_persistent_shim() {
    startup(false, true).await;
}

#[path = "workloads_openshell_gateway_test.rs"]
mod gateway;

async fn startup(forged: bool, stock: bool) {
    let (dir, c, mut report, mut policy, mut request) = policy_setup_with_driver("openshell").await;
    request.operation = "runtime.create".into();
    for module in policy.modules.values_mut() {
        *module = module.replace(
            "input.operation == \"read\"",
            "input.operation == \"runtime.create\"",
        );
    }
    policy.policy.sha256 = policy_digest(&policy).unwrap();
    activate(&c, &policy, &request).await;
    let network = Arc::new(Network::default());
    network.forged.store(forged, Ordering::SeqCst);
    let mut c = Arc::try_unwrap(c).ok().unwrap();
    let p = Arc::get_mut(c.policy.as_mut().unwrap()).unwrap();
    p.test_network(network.clone());
    p.openshell = Some(Arc::new(
        crate::openshell::Launches::open(&dir.path().join("launches"), admission()).unwrap(),
    ));
    let c = Arc::new(c);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let router = crate::http::router(crate::http::HttpState {
        fleet: c.fleet.clone(),
        workloads: c.clone(),
    });
    let http = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let native = Native::default();
    let native_socket = dir.path().join("native.sock");
    let listener = tokio::net::UnixListener::bind(&native_socket).unwrap();
    std::fs::set_permissions(&native_socket, std::fs::Permissions::from_mode(0o600)).unwrap();
    let native_service = native.clone();
    let native_task = tokio::spawn(async move {
        tonic_native::transport::Server::builder()
            .add_service(compute_driver_server::ComputeDriverServer::new(
                native_service,
            ))
            .serve_with_incoming(tokio_stream::wrappers::UnixListenerStream::new(listener))
            .await
            .unwrap()
    });
    let state = dir.path().join("shim");
    let shim_socket = state.join("control.sock");
    let stop = tokio_util::sync::CancellationToken::new();
    let stop2 = stop.clone();
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
            stop2,
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
    let fixture = provisioning(&request);
    let mut launch = fixture.request.clone();
    launch.policy = policy.policy.clone();
    let native_bytes = STANDARD.decode(&fixture.native_create_base64).unwrap();
    let mut sandbox = DriverSandbox::decode(native_bytes.as_slice()).unwrap();
    let native_policy = SandboxPolicy {
        version: 1,
        process: Some(protocol::sandbox::v1::ProcessPolicy {
            run_as_user: "1000".into(),
            run_as_group: "1000".into(),
        }),
        ..Default::default()
    };
    sandbox.spec.as_mut().unwrap().policy = Some(native_policy.clone());
    sandbox.spec.as_mut().unwrap().workload_identity = Some(WorkloadIdentityRequest {
        user: "1000".into(),
        group: "1000".into(),
    });
    let input = json!({"request":launch,"native_policy_base64":STANDARD.encode(native_policy.encode_to_vec()),"principal":"alice","expires_at":request.validity.expires_at,"cost":1});
    assert_eq!(
        serve(
            &c,
            axum::http::Request::builder()
                .method("POST")
                .uri("/policy/openshell/launches")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&input).unwrap()))
                .unwrap()
        )
        .await
        .status(),
        401
    );
    let prepared =
        json_body(admin(&c, "POST", "/policy/openshell/launches", input.clone()).await).await;
    let wire = STANDARD
        .decode(prepared["create_request_base64"].as_str().unwrap())
        .unwrap();
    let public = protocol::CreateSandboxRequest::decode(wire.as_slice()).unwrap();
    assert_eq!(public.name, sandbox.name);
    assert_eq!(
        admin(&c, "POST", "/policy/openshell/launches", input.clone())
            .await
            .status(),
        200
    );
    let mut changed = input;
    changed["principal"] = "another-owner".into();
    assert_eq!(
        admin(&c, "POST", "/policy/openshell/launches", changed)
            .await
            .status(),
        409
    );
    let compute_socket = dir.path().join("compute.sock");
    let path = compute_socket.clone();
    let cc = c.clone();
    let server = tokio::spawn(async move { crate::openshell::serve(cc, path).await.unwrap() });
    socket_ready(&compute_socket).await;
    assert!(crate::openshell::serve(c.clone(), compute_socket.clone())
        .await
        .is_err());
    assert!(compute_socket.exists()); // A second listener must not unlink a live socket.
    let mut client = compute_client(compute_socket.clone()).await;
    let caps = client
        .get_capabilities(GetCapabilitiesRequest {
            gateway: Some(
                trusted_shim::openshell_gateway::extension_protocol::gateway_metadata(
                    ExtensionFamily::Compute,
                ),
            ),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!caps.driver_reports_runtime_readiness && !caps.gateway_manages_lifecycle);
    let mut wrong = sandbox.clone();
    wrong
        .spec
        .as_mut()
        .unwrap()
        .command
        .push("injection".into());
    assert!(client
        .create_sandbox(CreateSandboxRequest {
            sandbox: Some(wrong)
        })
        .await
        .is_err());
    assert_eq!(network.submitted.load(Ordering::SeqCst), 0);
    let mut gateway = if stock {
        Some(gateway::start(dir.path(), &compute_socket).await)
    } else {
        None
    };
    let mut creation = if let Some((_, public_client)) = gateway.as_mut() {
        public_client
            .create_workspace(protocol::CreateWorkspaceRequest {
                name: prepared["workspace"].as_str().unwrap().into(),
                ..Default::default()
            })
            .await
            .unwrap();
        let mut wrong = public.clone();
        wrong.spec.as_mut().unwrap().command.push("injected".into());
        assert!(public_client.create_sandbox(wrong).await.is_err());
        assert_eq!(network.submitted.load(Ordering::SeqCst), 0);
        let mut creator = public_client.clone();
        tokio::spawn(async move { creator.create_sandbox(public).await.map(|_| ()) })
    } else {
        let mut creator = client.clone();
        let native_request = sandbox.clone();
        tokio::spawn(async move {
            creator
                .create_sandbox(CreateSandboxRequest {
                    sandbox: Some(native_request),
                })
                .await
                .map(|_| ())
        })
    };
    tokio::time::timeout(Duration::from_secs(10), async {
        while network.submitted.load(Ordering::SeqCst) == 0 {
            if creation.is_finished() {
                panic!(
                    "creation ended before proof submission: {:?}",
                    (&mut creation).await.unwrap()
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await
        }
    })
    .await
    .unwrap();
    assert!(c.deliveries.offers("gpu").is_empty());
    assert_eq!(native.creates.load(Ordering::SeqCst), 0);
    assert!(!creation.is_finished());
    let pending = json_body(
        admin(
            &c,
            "GET",
            &format!("/policy/openshell/launches/{}", sandbox.name),
            json!({}),
        )
        .await,
    )
    .await;
    let bound_id = pending["sandbox_id"].as_str().unwrap().to_owned();
    let mut premature = DriverSandbox {
        id: bound_id.clone(),
        name: sandbox.name.clone(),
        workspace: trusted_shim::openshell_gateway::workspace_name(&request.execution.namespace),
        status: Some(DriverSandboxStatus {
            instance_id: "premature".into(),
            conditions: vec![DriverCondition {
                r#type: "Ready".into(),
                status: "True".into(),
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    };
    report.runtime_instances[0].native_observation_base64 =
        Some(STANDARD.encode(premature.encode_to_vec()));
    c.report("gpu", "session", report.clone()).await.unwrap();
    let pending = client
        .get_sandbox(GetSandboxRequest {
            sandbox_id: bound_id,
            name: sandbox.name.clone(),
        })
        .await
        .unwrap()
        .into_inner()
        .sandbox
        .unwrap();
    assert_eq!(pending.status.unwrap().conditions[0].status, "False");
    report.runtime_instances[0].native_observation_base64 = None;
    c.report("gpu", "session", report.clone()).await.unwrap();
    if !stock {
        assert!(client
            .create_sandbox(CreateSandboxRequest {
                sandbox: Some(sandbox.clone())
            })
            .await
            .is_err());
        assert_eq!(network.submitted.load(Ordering::SeqCst), 1);
    }
    network.done.store(true, Ordering::SeqCst);
    if forged {
        assert!(tokio::time::timeout(Duration::from_secs(10), creation)
            .await
            .unwrap()
            .unwrap()
            .is_err());
        assert!(c.deliveries.offers("gpu").is_empty());
        assert_eq!(native.creates.load(Ordering::SeqCst), 0);
        server.abort();
        let _ = server.await;
    } else {
        let offer = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(o) = c.deliveries.offers("gpu").first() {
                    break o.clone();
                }
                tokio::time::sleep(Duration::from_millis(10)).await
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
        assert_eq!(native.creates.load(Ordering::SeqCst), 1);
        assert_eq!(
            c.deliveries.record(&offer.ticket).unwrap().state,
            "acknowledged"
        );
        let observed = shim_call(
            &shim_socket,
            json!({"operation":"inspect","execution":request.execution,"driver":"openshell"}),
        )
        .await;
        report.runtime_instances[0].native_observation_base64 = Some(
            observed["native_observation_base64"]
                .as_str()
                .unwrap()
                .into(),
        );
        c.report("gpu", "session", report.clone()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(15), creation)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if stock {
            sandbox = native.input.lock().unwrap().clone().unwrap();
        }
        assert_eq!(
            native
                .current
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .status
                .as_ref()
                .unwrap()
                .instance_id,
            "native-instance"
        );
        let snapshot = client
            .get_sandbox(GetSandboxRequest {
                sandbox_id: sandbox.id.clone(),
                name: sandbox.name.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .sandbox
            .unwrap();
        assert!(snapshot.spec.is_none());
        assert!(snapshot.status.as_ref().unwrap().agent_fd.is_empty());
        assert!(snapshot.status.as_ref().unwrap().conditions[0]
            .message
            .is_empty());
        assert!(snapshot.status.as_ref().unwrap().fence_evidence.is_some());
        let valid_observation = report.runtime_instances[0]
            .native_observation_base64
            .clone();
        premature = snapshot.clone();
        for replacement in ["other-tenant", "replaced-instance"] {
            if replacement == "other-tenant" {
                premature.workspace = replacement.into();
            } else {
                premature = snapshot.clone();
                premature.status.as_mut().unwrap().instance_id = replacement.into();
            }
            report.runtime_instances[0].native_observation_base64 =
                Some(STANDARD.encode(premature.encode_to_vec()));
            c.report("gpu", "session", report.clone()).await.unwrap();
            let denied = client
                .get_sandbox(GetSandboxRequest {
                    sandbox_id: sandbox.id.clone(),
                    name: sandbox.name.clone(),
                })
                .await
                .unwrap()
                .into_inner()
                .sandbox
                .unwrap();
            assert_eq!(denied.status.unwrap().conditions[0].status, "False");
        }
        report.runtime_instances[0].native_observation_base64 = valid_observation;
        c.report("gpu", "session", report.clone()).await.unwrap();
        assert!(
            !report.runtime_instances[0].ready && report.runtime_instances[0].services.is_empty()
        );
        // Same native callback observes, without another proof or native mutation.
        client
            .create_sandbox(CreateSandboxRequest {
                sandbox: Some(sandbox.clone()),
            })
            .await
            .unwrap();
        node.deliver(offer).await;
        assert_eq!(native.creates.load(Ordering::SeqCst), 1);
        report.ready = false;
        c.report("gpu", "session", report.clone()).await.unwrap();
        let stale = client
            .get_sandbox(GetSandboxRequest {
                sandbox_id: sandbox.id.clone(),
                name: sandbox.name.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .sandbox
            .unwrap();
        assert_eq!(stale.status.unwrap().conditions[0].status, "False");
        assert!(client
            .create_sandbox(CreateSandboxRequest {
                sandbox: Some(sandbox.clone())
            })
            .await
            .is_err());
        report.ready = true;
        c.report("gpu", "session", report.clone()).await.unwrap();
        server.abort();
        let _ = server.await;
        assert!(!compute_socket.exists());
        // Reconstruct controller, ledger and launch journal from durable state;
        // the shim and native instance stay alive across the control restart.
        let mut recovered_controller =
            WorkloadController::new(c.fleet.clone(), "cluster".into()).unwrap();
        let program = c.policy.as_ref().unwrap().program.clone();
        let ledger = Arc::new(
            SqliteLedger::open(
                &dir.path().join("ledger.sqlite"),
                program.clone(),
                Arc::new(TestVerifier),
            )
            .unwrap(),
        );
        let mut recovered_policy =
            crate::policy_gateway::PolicyGateway::test_ledger(ledger, program);
        let p = Arc::get_mut(&mut recovered_policy).unwrap();
        p.test_network(network.clone());
        p.openshell = Some(Arc::new(
            crate::openshell::Launches::open(&dir.path().join("launches"), admission()).unwrap(),
        ));
        recovered_controller.policy = Some(recovered_policy);
        let recovered_controller = Arc::new(recovered_controller);
        recovered_controller
            .report("gpu", "session", report)
            .await
            .unwrap();
        let path = compute_socket.clone();
        let cc = recovered_controller;
        let restarted =
            tokio::spawn(async move { crate::openshell::serve(cc, path).await.unwrap() });
        socket_ready(&compute_socket).await;
        let mut recovered = compute_client(compute_socket).await;
        recovered
            .create_sandbox(CreateSandboxRequest {
                sandbox: Some(sandbox),
            })
            .await
            .unwrap();
        assert_eq!(native.creates.load(Ordering::SeqCst), 1);
        restarted.abort();
        let _ = restarted.await;
    }
    assert_eq!(network.submitted.load(Ordering::SeqCst), 1);
    for path in std::fs::read_dir(dir.path().join("launches")).unwrap() {
        let text = std::fs::read_to_string(path.unwrap().path()).unwrap();
        assert!(
            !text.contains("synthetic-native")
                && !text.contains("private-host")
                && !text.contains("private-diagnostic")
        );
    }
    if let Some((mut child, _)) = gateway {
        child.kill().await.unwrap();
        child.wait().await.unwrap();
    }
    stop.cancel();
    daemon.await.unwrap();
    native_task.abort();
    http.abort();
}
