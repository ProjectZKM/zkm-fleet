//! Native GPUs and durable volumes: Fleet fills Ray's device grant into the
//! prepared launch, translates both into the stock public request, and binds
//! every later delivery to exactly that grant and the admitted manifest.
use super::*;
use trusted_shim::{
    openshell_gateway::resource_admission::DriverAdmissionConfig, runtime::DurableMount,
};

fn permissive() -> String {
    DriverAdmissionConfig {
        allow_driver_config: true,
        ..Default::default()
    }
    .acknowledgement()
}

async fn controller(
    admission: String,
    gpu_ids: &[&str],
) -> (
    tempfile::TempDir,
    Arc<WorkloadController>,
    trusted_shim::runtime::LaunchRequest,
    NodeReport,
) {
    let ids: Vec<String> = gpu_ids.iter().map(|s| (*s).to_owned()).collect();
    let (dir, c, report, mut policy, mut request) = policy_setup_with_inventory("openshell", |report| {
        report.apps[0]["gpus"] = json!(2);
        report.apps[0]["mounts"] = json!([{"name":"data","target":"/data"},{"name":"models","target":"/models","read_only":true}]);
        report.runtime_capabilities.push("shim-openshell-attachments-v1".into());
        report.runtime_instances[0].gpu_ids = ids;
    })
    .await;
    request.operation = "runtime.create".into();
    for module in policy.modules.values_mut() {
        *module = module.replace(
            "input.operation == \"read\"",
            "input.operation == \"runtime.create\"",
        );
    }
    policy.policy.sha256 = policy_digest(&policy).unwrap();
    activate(&c, &policy, &request).await;
    let mut c = Arc::try_unwrap(c).ok().unwrap();
    Arc::get_mut(c.policy.as_mut().unwrap()).unwrap().openshell = Some(Arc::new(
        crate::openshell::Launches::open(&dir.path().join("launches"), admission).unwrap(),
    ));
    let mut launch = provisioning(&request).request;
    launch.policy = policy.policy.clone();
    launch.mounts = vec![
        DurableMount {
            name: "data".into(),
            target: "/data".into(),
            read_only: false,
        },
        DurableMount {
            name: "models".into(),
            target: "/models".into(),
            read_only: true,
        },
    ];
    (dir, Arc::new(c), launch, report)
}

/// One expiry per process, so identical preparations are byte-identical.
fn preparation(launch: &trusted_shim::runtime::LaunchRequest) -> Value {
    static EXPIRES: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    let expires = *EXPIRES.get_or_init(|| crate::workloads::now() as u64 + 600);
    json!({"request":launch,"native_policy_base64":STANDARD.encode(SandboxPolicy::default().encode_to_vec()),
        "principal":"alice","expires_at":expires,"cost":1})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_launch_carries_exactly_the_ray_grant_and_admitted_volumes() {
    let (_dir, c, launch, _) = controller(permissive(), &["3", "5"]).await;
    assert_eq!(c.native_gpus(&launch.execution).unwrap(), ["3", "5"]);

    // The caller omits node-local ids; Fleet fills them before translation.
    let prepared = json_body(
        admin(
            &c,
            "POST",
            "/policy/openshell/launches",
            preparation(&launch),
        )
        .await,
    )
    .await;
    let wire = STANDARD
        .decode(prepared["create_request_base64"].as_str().unwrap())
        .unwrap();
    let public = protocol::CreateSandboxRequest::decode(wire.as_slice()).unwrap();
    let spec = public.spec.unwrap();
    assert_eq!(
        spec.resource_requirements.unwrap().gpu.unwrap().count,
        Some(2)
    );
    let config = serde_json::to_value(prost_struct_json(
        &spec.template.unwrap().driver_config.unwrap(),
    ))
    .unwrap();
    assert_eq!(
        config["docker"]["cdi_devices"],
        json!(["nvidia.com/gpu=3", "nvidia.com/gpu=5"])
    );
    let mounts = config["docker"]["mounts"].as_array().unwrap();
    assert_eq!(mounts.len(), 2);
    assert!(mounts
        .iter()
        .all(|m| m["type"] == "volume" && m["source"].as_str().unwrap().starts_with("fleet-")));
    assert_eq!(
        (mounts[0]["target"].clone(), mounts[1]["read_only"].clone()),
        (json!("/data"), json!(true))
    );

    // Identical re-preparation, explicit or omitted ids, is idempotent.
    let mut explicit = launch.clone();
    explicit.gpus = vec!["3".into(), "5".into()];
    for input in [preparation(&launch), preparation(&explicit)] {
        json_body(admin(&c, "POST", "/policy/openshell/launches", input).await).await;
    }
    c.with_runtime_delivery(&explicit.execution, None, Some(&explicit), |_| Ok(()))
        .unwrap();

    // Any other devices or storage are not this allocation's grant.
    let substitutions: Vec<Box<dyn Fn(&mut trusted_shim::runtime::LaunchRequest)>> = vec![
        Box::new(|l| l.gpus.reverse()),
        Box::new(|l| l.gpus.truncate(1)),
        Box::new(|l| l.gpus.clear()),
        Box::new(|l| l.gpus.push("7".into())),
        Box::new(|l| l.gpus[1] = "6".into()),
        Box::new(|l| l.mounts.clear()),
        Box::new(|l| l.mounts.reverse()),
        Box::new(|l| l.mounts[0].read_only = true),
        Box::new(|l| l.mounts[1].read_only = false),
        Box::new(|l| l.mounts[0].name = "other".into()),
        Box::new(|l| l.mounts[0].target = "/elsewhere".into()),
    ];
    for (index, change) in substitutions.iter().enumerate() {
        let mut bad = explicit.clone();
        change(&mut bad);
        assert!(
            c.with_runtime_delivery(&bad.execution, None, Some(&bad), |_| Ok(()))
                .is_err(),
            "{index}"
        );
        if bad.gpus.is_empty() {
            continue; // Omission means "fill from the grant", covered above.
        }
        // A fresh store, so rejection is the binding, not a name conflict.
        let (_dir, fresh, mut good, _) = controller(permissive(), &["3", "5"]).await;
        good.gpus = explicit.gpus.clone();
        let mut bad = good.clone();
        change(&mut bad);
        let response = admin(
            &fresh,
            "POST",
            "/policy/openshell/launches",
            preparation(&bad),
        )
        .await;
        assert!(!response.status().is_success(), "{index}");
        json_body(
            admin(
                &fresh,
                "POST",
                "/policy/openshell/launches",
                preparation(&good),
            )
            .await,
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attachments_require_a_complete_report_and_a_permissive_policy() {
    // A reservation whose ids are not exactly the admitted count of distinct
    // devices is dropped from the report, so nothing can be prepared for it.
    let (_dir, c, launch, report) = controller(permissive(), &["3", "5"]).await;
    for ids in [
        vec!["3"],
        vec!["3", "3"],
        vec!["3", "5", "7"],
        vec!["3", "gpu/5"],
        vec![],
    ] {
        let mut bad = report.clone();
        bad.runtime_instances[0].gpu_ids = ids.iter().map(|s| (*s).to_owned()).collect();
        c.report("gpu", "session", bad).await.unwrap();
        assert!(c.native_gpus(&launch.execution).is_err(), "{ids:?}");
        assert!(!admin(
            &c,
            "POST",
            "/policy/openshell/launches",
            preparation(&launch)
        )
        .await
        .status()
        .is_success());
    }
    c.report("gpu", "session", report).await.unwrap();
    assert_eq!(c.native_gpus(&launch.execution).unwrap(), ["3", "5"]);

    let (_dir, c, launch, _) = controller(admission(), &["3", "5"]).await;
    let response = admin(
        &c,
        "POST",
        "/policy/openshell/launches",
        preparation(&launch),
    )
    .await;
    assert!(!response.status().is_success());
}

/// Read a protobuf Struct back as JSON for assertions.
fn prost_struct_json(s: &prost_types_native::Struct) -> Value {
    fn value(v: &prost_types_native::Value) -> Value {
        use prost_types_native::value::Kind;
        match v.kind.as_ref() {
            Some(Kind::StringValue(s)) => json!(s),
            Some(Kind::BoolValue(b)) => json!(b),
            Some(Kind::NumberValue(n)) => json!(n),
            Some(Kind::ListValue(l)) => Value::Array(l.values.iter().map(value).collect()),
            Some(Kind::StructValue(s)) => prost_struct_json(s),
            Some(Kind::NullValue(_)) | None => Value::Null,
        }
    }
    Value::Object(
        s.fields
            .iter()
            .map(|(k, v)| (k.clone(), value(v)))
            .collect(),
    )
}
