//! A namespace's owner sets a native app's policy with its own token; the
//! scheduler then activates it and launches every replica generation itself.
//! The stock gateway is a double that hands each create to the compute
//! extension, as the gateway's compute driver call does.
use super::*;
use crate::openshell::{ExecGateway, ExecOutput};
use trusted_shim::rail::Error;

#[derive(Default)]
struct Stock {
    controller: Mutex<Option<Arc<WorkloadController>>>,
    creates: Mutex<Vec<(String, String)>>,
    execs: Mutex<Vec<(String, Vec<String>)>>,
}
#[async_trait::async_trait]
impl ExecGateway for Stock {
    async fn exec(&self, start: protocol::ExecSandboxRequest, stdin: Vec<u8>) -> trusted_shim::rail::Result<ExecOutput> {
        self.execs.lock().unwrap().push((start.sandbox, start.command));
        Ok(ExecOutput { stdout: stdin, stderr: vec![], exit_code: 0 })
    }
    async fn create(&self, workspace: &str, request: protocol::CreateSandboxRequest) -> trusted_shim::rail::Result<()> {
        use compute_driver_server::ComputeDriver;
        self.creates.lock().unwrap().push((workspace.into(), request.name.clone()));
        let c = self.controller.lock().unwrap().clone().unwrap();
        let sandbox = DriverSandbox {
            id: format!("id-{}", request.name),
            name: request.name,
            workspace: workspace.into(),
            spec: Some(DriverSandboxSpec {
                command: vec!["/bin/true".into()],
                policy: Some(SandboxPolicy::default()),
                sandbox_token: "synthetic-native-token".into(),
                launch_authentication: b"synthetic-native-credentials".to_vec(),
                template: Some(DriverSandboxTemplate {
                    image: "runner:1".into(),
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
        crate::openshell::Compute(c)
            .create_sandbox(NativeRequest::new(CreateSandboxRequest { sandbox: Some(sandbox) }))
            .await
            .map(|_| ())
            .map_err(|_| Error::Rejected)
    }
}

async fn call(c: &Arc<WorkloadController>, path: &str, token: &str, method: &str, body: Value) -> Response {
    scale_call(c.clone(), token, method, path, body).await
}

/// The owner's view of the app's policy, once `done` holds.
async fn until(c: &Arc<WorkloadController>, token: &str, path: &str, done: impl Fn(&Value) -> bool) -> Value {
    let mut last = Value::Null;
    let waited = tokio::time::timeout(Duration::from_secs(40), async {
        loop {
            last = json_body(scale_call(c.clone(), token, "GET", path, Value::Null).await).await;
            if done(&last) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await
        }
    })
    .await;
    assert!(waited.is_ok(), "{last}");
    last
}

async fn owner_launches(driver: &str) {
    let native = SandboxPolicy::default();
    let hash = trusted_shim::openshell_gateway::deterministic_policy_hash(&native);
    let (dir, c, mut report, mut policy, request) = policy_setup_with_inventory("openshell", |report| {
        if driver != "openshell" {
            report.apps[0]["runtime"]["driver"] = json!(driver);
            report.apps[0]["access_policy_sha256"] = json!(hash);
            report.apps[0]["services"] = json!(["http"]);
            report.apps[0]["container_port"] = json!(8090);
            report.apps[0]["health_path"] = json!("/health");
            report.runtime_capabilities.push("shim-openshell-access-v1".into());
        }
    })
    .await;
    for module in policy.modules.values_mut() {
        // It admits the launch, and of the owner's commands only `/bin/cat`.
        *module = module.replace(
            "{input.operation == \"read\"}",
            "{input.operation == \"runtime.create\"}\ndecision := {\"decision\":\"allow\",\"reason_codes\":[]} if {\n\tinput.operation == \"exec\"\n\tinput.resource == \"model-0\"\n\tinput.principal == \"alice\"\n\tinput.parameters.command[0] == \"/bin/cat\"\n}",
        );
    }
    policy.policy.sha256 = policy_digest(&policy).unwrap();
    let ns = request.execution.namespace.clone();
    let network = Arc::new(Network::default());
    network.done.store(true, Ordering::SeqCst);
    let stock = Arc::new(Stock::default());
    let mut c = Arc::try_unwrap(c).ok().unwrap();
    let p = Arc::get_mut(c.policy.as_mut().unwrap()).unwrap();
    p.test_network(network.clone());
    p.openshell = Some(Arc::new(
        crate::openshell::Launches::open(&dir.path().join("launches"), admission()).unwrap().with_control(stock.clone()),
    ));
    let c = Arc::new(c);
    *stock.controller.lock().unwrap() = Some(c.clone());

    let owner = user_token(&c, &ns, 7, "scope=default", None, true).await;
    let path = format!("/apps/model/policy?namespace={ns}");
    let body = json!({"policy": policy, "context": {"tenant": ns}, "principal": "alice", "cost": 1,
        "native_policy_base64": STANDARD.encode(native.encode_to_vec())});
    assert_eq!(call(&c, &path, &owner, "GET", Value::Null).await.status(), 404);
    // Only the app's owner sets it: not a node's credential, nor another tag's token, which sees no such app.
    assert_eq!(call(&c, &path, "cluster", "PUT", body.clone()).await.status(), 401);
    let other = user_token(&c, &ns, 8, "scope=other", None, true).await;
    assert_eq!(call(&c, &path, &other, "PUT", body.clone()).await.status(), 404);
    for (key, wrong) in [("cost", json!(0)), ("principal", json!("")), ("context", json!([])),
        ("native_policy_base64", json!("not base64")), ("state_root", json!("short"))] {
        let mut invalid = body.clone();
        invalid[key] = wrong;
        assert_eq!(call(&c, &path, &owner, "PUT", invalid).await.status(), 400, "{key}");
    }
    let mut undigested = body.clone();
    undigested["policy"]["policy"]["sha256"] = json!("0".repeat(64));
    assert_eq!(call(&c, &path, &owner, "PUT", undigested).await.status(), 400);
    if driver != "openshell" {
        // The entry admits exactly the access rules its `access_policy_sha256` names.
        let mut rules = body.clone();
        rules["native_policy_base64"] = json!(STANDARD.encode(SandboxPolicy { version: 1, ..Default::default() }.encode_to_vec()));
        assert_eq!(call(&c, &path, &owner, "PUT", rules).await.status(), 400);
    }
    let set = json_body(call(&c, &path, &owner, "PUT", body.clone()).await).await;
    assert_eq!(set["policy"], json!(policy.policy));
    assert_eq!((set["budget"].as_str(), set["native_policy_sha256"].as_str()), (Some(crate::openshell::budget(&ns).as_str()), Some(hash.as_str())));
    // Neither the policy's rules and data nor its context come back.
    assert!(set.get("context").is_none() && set["policy"].get("modules").is_none());

    let worker = tokio::spawn(crate::openshell::launch_loop(c.clone()));
    // Without a budget for the namespace the ledger takes no activation, and nothing is submitted.
    let refused = until(&c, &owner, &path, |v| v["launches"][0]["error"].is_string()).await;
    assert!(refused["launches"][0]["error"].as_str().unwrap().contains("policy budget"), "{refused}");
    assert_eq!(refused["launches"][0]["phase"], "pending");
    assert!(stock.creates.lock().unwrap().is_empty());
    json_body(admin(&c, "PUT", "/policy/budgets", json!({"name": crate::openshell::budget(&ns), "allowance": 3})).await).await;

    let delivered = until(&c, &owner, &path, |v| v["launches"][0]["phase"] == "delivered").await;
    assert_eq!((delivered["launches"][0]["generation"].as_u64(), delivered["launches"][0]["allocation_id"].as_str()), (Some(1), Some("model-0")));
    assert_eq!(network.submitted.load(Ordering::SeqCst), 1);
    let name = trusted_shim::openshell_gateway::GatewayLaunch::execution_name(&request.execution).unwrap();
    let workspace = trusted_shim::openshell_gateway::workspace_name(&ns);
    assert_eq!(*stock.creates.lock().unwrap(), vec![(workspace.clone(), name.clone())]);
    let (active, context, _) = c.policy.as_ref().unwrap().ledger.authority_snapshot(&request.execution).unwrap();
    assert_eq!((active.policy, context), (policy.policy.clone(), json!({"tenant": ns})));
    assert_eq!(c.deliveries.offers("gpu").len(), 1);

    // The node reports the sandbox it created: the gateway's create returns.
    let observed = DriverSandbox {
        id: format!("id-{name}"),
        name: name.clone(),
        workspace: workspace.clone(),
        status: Some(DriverSandboxStatus {
            instance_id: "native-instance".into(),
            conditions: vec![DriverCondition { r#type: "Ready".into(), status: "True".into(), ..Default::default() }],
            ..Default::default()
        }),
        ..Default::default()
    };
    report.runtime_instances[0].native_observation_base64 = Some(STANDARD.encode(observed.encode_to_vec()));
    c.report("gpu", "session", report.clone()).await.unwrap();
    until(&c, &owner, &path, |v| v["launches"][0]["phase"] == "created" && v["launches"][0]["error"].is_null()).await;

    // The owner's exec is the policy's decision on that command, and each allowed one draws on the budget.
    let exec = format!("/apps/model/exec?namespace={ns}");
    let command = |program: &str| json!({"command": [program, "-"], "stdin": STANDARD.encode("in"), "timeout_seconds": 5});
    assert_eq!(call(&c, &exec, "cluster", "POST", command("/bin/cat")).await.status(), 401);
    assert_eq!(call(&c, &exec, &other, "POST", command("/bin/cat")).await.status(), 404);
    assert_eq!(call(&c, &exec, &owner, "GET", Value::Null).await.status(), 405);
    assert_eq!(call(&c, &exec, &owner, "POST", json!({"command": [], "timeout_seconds": 5})).await.status(), 400);
    assert_eq!(call(&c, &format!("{exec}&replica=model-1"), &owner, "POST", command("/bin/cat")).await.status(), 409);
    let denied = call(&c, &exec, &owner, "POST", command("/bin/rm")).await;
    assert_eq!(denied.status(), 403);
    assert!(stock.execs.lock().unwrap().is_empty());
    let ran = json_body(call(&c, &format!("{exec}&replica=model-0"), &owner, "POST", command("/bin/cat")).await).await;
    assert_eq!((ran["stdout"].as_str(), ran["exit_code"].as_i64(), ran["decision"]["decision"].as_str()),
        (Some(STANDARD.encode("in").as_str()), Some(0), Some("allow")), "{ran}");
    assert_eq!(ran["decision"]["policy"], json!(policy.policy));
    assert_eq!(*stock.execs.lock().unwrap(), vec![(name.clone(), vec!["/bin/cat".to_owned(), "-".to_owned()])]);
    // Its grant is consumed, and its proof is queued under the decision's job.
    let job = ran["decision"]["job_id"].as_str().unwrap();
    assert_eq!(json_body(admin(&c, "GET", &format!("/policy/preflight/{job}"), json!({})).await).await["receipt_state"], "queued");

    // A restart is a new generation of the same replica: the same policy, a new activation and proof.
    report.runtime_instances[0].execution.generation = 2;
    report.runtime_instances[0].execution.lease_id = "lease-2".into();
    report.runtime_instances[0].native_observation_base64 = None;
    c.report("gpu", "session", report.clone()).await.unwrap();
    until(&c, &owner, &path, |v| v["launches"][0]["generation"] == 2 && v["launches"][0]["phase"] == "delivered").await;
    assert_eq!(network.submitted.load(Ordering::SeqCst), 2);
    let second = crate::policy_gateway::identity(report.runtime_instances[0].execution.clone());
    assert_eq!(c.policy.as_ref().unwrap().ledger.authority_snapshot(&second).unwrap().0.policy, policy.policy);
    let creates = stock.creates.lock().unwrap().clone();
    assert_eq!(creates.len(), 2);
    assert_ne!(creates[0].1, creates[1].1);
    // The budget's 3 are spent on two launches and one exec: the next allow is refused, and nothing runs.
    until(&c, &owner, &path, |v| v["launches"][0]["generation"] == 2 && v["launches"][0]["phase"] == "delivered").await;
    report.runtime_instances[0].native_observation_base64 = Some(STANDARD.encode(DriverSandbox {
        id: format!("id-{}", creates[1].1), name: creates[1].1.clone(), ..observed
    }.encode_to_vec()));
    c.report("gpu", "session", report.clone()).await.unwrap();
    until(&c, &owner, &path, |v| v["launches"][0]["phase"] == "created").await;
    assert_eq!(call(&c, &exec, &owner, "POST", command("/bin/cat")).await.status(), 409);
    assert_eq!(stock.execs.lock().unwrap().len(), 1);

    assert_eq!(call(&c, &path, &owner, "DELETE", Value::Null).await.status(), 200);
    assert_eq!(call(&c, &path, &owner, "GET", Value::Null).await.status(), 404);
    worker.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_owner_sets_a_policy_and_the_scheduler_launches_each_generation_with_it() {
    owner_launches("openshell").await;
    owner_launches("docker").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_owner_creates_a_native_app_with_its_policy_in_one_request() {
    let (dir, c, _, policy, request) = policy_setup_with_inventory("openshell", |_| {}).await;
    let ns = request.execution.namespace.clone();
    let native = SandboxPolicy::default();
    let hash = trusted_shim::openshell_gateway::deterministic_policy_hash(&native);
    let owner = user_token(&c, &ns, 7, "scope=default", None, true).await;
    let app = |name: &str| json!({"name": name, "spec": {"image": "runner:1", "command": ["/bin/true"], "cpus": 1, "gpus": 0, "memory_gb": 0.125},
        "policy": {"policy": policy, "context": {}, "principal": "alice", "cost": 1,
            "native_policy_base64": STANDARD.encode(native.encode_to_vec())}});
    // A scheduler without the compute extension has nothing to launch a policy with.
    assert_eq!(scale_call(c.clone(), &owner, "POST", "/apps", app("agent")).await.status(), 400);
    let mut c = Arc::try_unwrap(c).ok().unwrap();
    Arc::get_mut(c.policy.as_mut().unwrap()).unwrap().openshell = Some(Arc::new(
        crate::openshell::Launches::open(&dir.path().join("launches"), admission()).unwrap().with_control(Arc::new(Stock::default())),
    ));
    let c = Arc::new(c);
    let mut invalid = app("agent");
    invalid["policy"]["cost"] = json!(0);
    assert_eq!(scale_call(c.clone(), &owner, "POST", "/apps", invalid).await.status(), 400);
    let created = scale_call(c.clone(), &owner, "POST", "/apps", app("agent")).await;
    assert_eq!(created.status(), 201);
    // The policy selects the native path: the entry admits exactly its native rules.
    let view = json_body(scale_call(c.clone(), &owner, "GET", &format!("/apps/agent?namespace={ns}"), Value::Null).await).await;
    assert_eq!(view["spec"]["access_policy_sha256"], json!(hash), "{view}");
    let path = format!("/apps/agent/policy?namespace={ns}");
    let set = json_body(call(&c, &path, &owner, "GET", Value::Null).await).await;
    assert_eq!((set["policy"].clone(), set["launches"].clone()), (json!(policy.policy), json!([])));
    // It is stored: a scheduler that starts again reads it back.
    assert!(crate::openshell::Launches::open(&dir.path().join("launches"), admission()).unwrap().test_selected(&ns, "agent"));
    // The policy goes with its app.
    assert_eq!(scale_call(c.clone(), &owner, "DELETE", &format!("/apps/agent?namespace={ns}"), Value::Null).await.status(), 200);
    assert_eq!(std::fs::read_dir(dir.path().join("launches/selections")).unwrap().count(), 0);
}
