//! Stock OpenShell compute extension inside Fleet. Allocation and private
//! delivery remain owned by Fleet; the native gateway still owns credentials,
//! supervisor sessions and joined readiness. No gateway process is launched here.
use crate::{
    runtime_delivery::stage_payload,
    workloads::{atomic, WorkloadController},
};
use axum::{
    extract::{Path as ApiPath, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response as HttpResponse},
    routing::post,
    Json, Router,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use fleet_proto::model::v1::OpenShellLaunchPreparation;
use parking_lot::Mutex;
use prost_native::Message;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::sync::Semaphore;
use tonic_native::{Request, Response, Status};
use trusted_shim::{
    contracts::Validity,
    openshell_gateway::{
        control_observation,
        extension_protocol::{self, ExtensionFamily},
        protocol::compute::v1::*,
        workspace_name, GatewayLaunch, SandboxPolicy,
    },
    proof::{ProofRequest, ProofStatus},
    provisioning::ProvisionRequest,
    rail::{
        rego::{commitment, encode_public_values, RegorusEngine},
        Error, EvaluationRequest, PolicyBundle, PolicyProofCodec, RuleEngine,
    },
    runtime::LaunchRequest,
};
type Result<T> = trusted_shim::rail::Result<T>;
const MAX_RECORD: usize = 4 * 1024 * 1024;
mod control;
mod owner;
mod readiness;
pub(crate) use control::{ExecGateway, StockGateway};
#[cfg(test)]
pub(crate) use control::{stdin_frames, ExecOutput};
#[cfg(test)]
pub(crate) use owner::budget;
pub(crate) use owner::{app_policy, launch_loop, native_rules_hash, remove_app_policy, set_app_policy, Reservation};
pub(crate) use readiness::{services_ready, Observer};
fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
fn status(_: Error) -> Status {
    Status::failed_precondition("Fleet launch rejected or requires reconciliation")
}
fn unavailable() -> Status {
    Status::unavailable("Fleet launch unavailable; inspect existing operation before retrying")
}
fn valid_name(s: &str) -> bool {
    s.len() == 19
        && s.starts_with('f')
        && s[1..]
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// No native credentials, proof grants or ledger tokens. Never serialize this
/// record to an administrative status response; the launch may contain env secrets.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    preparation: OpenShellLaunchPreparation,
    name: String,
    phase: String,
    sandbox_id: Option<String>,
    native_sha256: Option<String>,
    job_id: Option<String>,
    request: Option<EvaluationRequest>,
    policy: Option<PolicyBundle>,
    instance_id: Option<String>,
}
impl Record {
    fn launch(&self) -> Result<LaunchRequest> {
        serde_json::from_value(self.preparation.request.clone().into())
            .map_err(|_| Error::InvalidInput)
    }
    fn binding(&self) -> Result<GatewayLaunch> {
        let raw = STANDARD
            .decode(&self.preparation.native_policy_base64)
            .map_err(|_| Error::InvalidInput)?;
        let policy = SandboxPolicy::decode(raw.as_slice()).map_err(|_| Error::InvalidInput)?;
        GatewayLaunch::new(self.launch()?, policy).map_err(|_| Error::InvalidInput)
    }
    fn current(&self) -> Result<()> {
        if self.preparation.expires_at <= now() {
            Err(Error::Rejected)
        } else {
            Ok(())
        }
    }
}

pub(crate) struct Launches {
    dir: PathBuf,
    records: Mutex<BTreeMap<String, Record>>,
    running: Mutex<BTreeSet<String>>,
    permits: Arc<Semaphore>,
    admission_policy: String,
    /// The acknowledged node policy accepts GPU/volume `driver_config`.
    attachments: bool,
    readiness: readiness::Cache,
    /// Exec and create through the stock gateway, when it is configured.
    control: Option<Arc<dyn ExecGateway>>,
    control_slots: Semaphore,
    /// Each native app's policy, by namespace and app.
    selections: Mutex<BTreeMap<(String, String), owner::Selection>>,
    /// The scheduler's own launch attempts, by launch name.
    attempts: Mutex<BTreeMap<String, owner::Attempt>>,
}
impl Launches {
    pub(crate) fn open(dir: &Path, admission_policy: String) -> anyhow::Result<Self> {
        let admission: trusted_shim::openshell_gateway::resource_admission::DriverAdmissionConfig =
            serde_json::from_str(admission_policy.strip_prefix("v1:").ok_or_else(|| {
                anyhow::anyhow!("versioned native resource-admission acknowledgement required")
            })?)?;
        admission.validate().map_err(anyhow::Error::msg)?;
        match std::fs::DirBuilder::new().mode(0o700).create(dir) {
            Ok(()) => (),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => (),
            Err(e) => return Err(e.into()),
        }
        let meta = std::fs::symlink_metadata(dir)?;
        anyhow::ensure!(
            meta.is_dir()
                && meta.uid() == unsafe { libc::geteuid() }
                && meta.permissions().mode() & 0o077 == 0,
            "private launch directory required"
        );
        let mut records = BTreeMap::new();
        for entry in std::fs::read_dir(dir)? {
            let path = entry?.path();
            if path.extension().is_none_or(|s| s != "json") {
                continue;
            }
            let m = std::fs::symlink_metadata(&path)?;
            anyhow::ensure!(
                m.is_file()
                    && m.uid() == unsafe { libc::geteuid() }
                    && m.permissions().mode() & 0o077 == 0
                    && m.len() <= MAX_RECORD as u64,
                "invalid launch record"
            );
            let record: Record = serde_json::from_slice(&std::fs::read(&path)?)?;
            anyhow::ensure!(
                valid_name(&record.name)
                    && path.file_stem().and_then(|s| s.to_str()) == Some(&record.name)
                    && record.binding()?.request().name == record.name
                    && matches!(
                        record.phase.as_str(),
                        "prepared" | "bound" | "proving" | "dispatching" | "delivered" | "created"
                    ),
                "invalid launch binding"
            );
            records.insert(record.name.clone(), record);
        }
        Ok(Self {
            dir: dir.into(),
            records: Mutex::new(records),
            running: Mutex::new(BTreeSet::new()),
            permits: Arc::new(Semaphore::new(4)),
            admission_policy,
            attachments: admission.allow_driver_config,
            readiness: Default::default(),
            control: None,
            control_slots: Semaphore::new(control::CONTROL_SLOTS),
            selections: Mutex::new(owner::load(&dir.join("selections"))?),
            attempts: Default::default(),
        })
    }
    pub(crate) fn with_control(mut self, gateway: Arc<dyn ExecGateway>) -> Self {
        self.control = Some(gateway);
        self
    }
    #[cfg(test)]
    pub(crate) fn test_selected(&self, namespace: &str, app: &str) -> bool {
        self.selections.lock().contains_key(&(namespace.to_owned(), app.to_owned()))
    }
    #[cfg(test)]
    pub(crate) fn test_phase(&self, name: &str, phase: &str) {
        let mut record = self.get(name).unwrap();
        record.phase = phase.into();
        self.update(record).unwrap();
    }
    fn save(&self, record: &Record) -> Result<()> {
        let bytes = serde_json::to_vec(record).map_err(|_| Error::InvalidInput)?;
        if !valid_name(&record.name) || bytes.len() > MAX_RECORD {
            return Err(Error::InvalidInput);
        }
        atomic(&self.dir.join(format!("{}.json", record.name)), &bytes)
            .map_err(|_| Error::Unavailable)
    }
    fn get(&self, name: &str) -> Result<Record> {
        self.records
            .lock()
            .get(name)
            .cloned()
            .ok_or(Error::Rejected)
    }
    fn active_records(&self, c: &WorkloadController) -> Vec<Record> {
        let names = c.active_native_names();
        let records = self.records.lock();
        names
            .into_iter()
            .filter_map(|name| records.get(&name))
            .filter(|r| r.sandbox_id.is_some())
            .cloned()
            .collect()
    }
    fn update(&self, record: Record) -> Result<()> {
        let mut records = self.records.lock();
        if records
            .get(&record.name)
            .is_none_or(|old| old.preparation != record.preparation)
        {
            return Err(Error::Conflict);
        }
        // Fence memory before an ambiguous rename/fsync result as well.
        records.insert(record.name.clone(), record.clone());
        self.readiness.remove(&record.name);
        self.save(&record)
    }
}

fn launches(c: &WorkloadController) -> Result<Arc<Launches>> {
    c.policy
        .as_ref()
        .and_then(|p| p.openshell.clone())
        .ok_or(Error::Unavailable)
}

pub(crate) fn admin_router(c: Arc<WorkloadController>) -> Router {
    if launches(&c).is_err() {
        return Router::new();
    }
    Router::new()
        .route("/policy/openshell/launches", post(prepare))
        .route(
            "/policy/openshell/launches/:name",
            axum::routing::get(launch_status),
        )
        .route(
            "/policy/openshell/launches/:name/exec",
            post(control::exec).layer(axum::extract::DefaultBodyLimit::max(90 * 1024 * 1024)),
        )
        .with_state(c)
}
fn private(result: Result<serde_json::Value>) -> HttpResponse {
    let mut response = match result {
        Ok(v) => Json(v).into_response(),
        Err(e) => (
            match e {
                Error::InvalidInput => StatusCode::BAD_REQUEST,
                Error::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
                Error::Conflict => StatusCode::CONFLICT,
                _ => StatusCode::FORBIDDEN,
            },
            "OpenShell launch rejected or unavailable",
        )
            .into_response(),
    };
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    response
}
async fn prepare(
    State(c): State<Arc<WorkloadController>>,
    Json(input): Json<OpenShellLaunchPreparation>,
) -> HttpResponse {
    private(
        tokio::task::spawn_blocking(move || prepare_inner(&c, input))
            .await
            .unwrap_or(Err(Error::Unavailable)),
    )
}
fn prepare_inner(
    c: &WorkloadController,
    preparation: OpenShellLaunchPreparation,
) -> Result<serde_json::Value> {
    let (name, workspace, request) = prepare_record(c, preparation)?;
    Ok(json!({"name":name, "workspace":workspace,
        "create_request_base64":STANDARD.encode(request.encode_to_vec())}))
}
/// The launch's name, its workspace and the stock gateway's create request.
fn prepare_record(
    c: &WorkloadController,
    preparation: OpenShellLaunchPreparation,
) -> Result<(String, String, trusted_shim::openshell_gateway::protocol::CreateSandboxRequest)> {
    if preparation.native_policy_base64.len() > 1_398_104
        || preparation.principal.is_empty()
        || preparation.principal.len() > 256
        || preparation.principal.chars().any(char::is_control)
        || preparation.cost == 0
        || preparation.expires_at <= now()
        || preparation.expires_at > now().saturating_add(900)
    {
        return Err(Error::InvalidInput);
    }
    let store = launches(c)?;
    let mut record = Record {
        preparation,
        name: String::new(),
        phase: "prepared".into(),
        sandbox_id: None,
        native_sha256: None,
        job_id: None,
        request: None,
        policy: None,
        instance_id: None,
    };
    // Callers never learn node-local device ids: an omitted GPU list is
    // filled from the owning node's Ray grant, which delivery then requires.
    let mut launch = record.launch()?;
    if launch.gpus.is_empty() {
        launch.gpus = c.native_gpus(&launch.execution)?;
        if !launch.gpus.is_empty() {
            record.preparation.request = serde_json::to_value(&launch)
                .map_err(|_| Error::InvalidInput)?
                .into();
        }
    }
    if (!launch.gpus.is_empty() || !launch.mounts.is_empty()) && !store.attachments {
        return Err(Error::Rejected);
    }
    let binding = record.binding()?;
    let request = binding.request();
    record.name = request.name.clone();
    c.with_runtime_delivery(&launch.execution, None, Some(&launch), |_| {
        let (policy, _, _) = c
            .policy
            .as_ref()
            .ok_or(Error::Unavailable)?
            .ledger
            .authority_snapshot(&launch.execution)?;
        if policy.policy != launch.policy {
            return Err(Error::Rejected);
        }
        let mut records = store.records.lock();
        // A shortened native workspace is only a routing hint. Never allow a
        // collision to merge two canonical Fleet namespaces.
        for old in records.values() {
            let namespace = old.launch()?.execution.namespace;
            if namespace != launch.execution.namespace
                && workspace_name(&namespace) == workspace_name(&launch.execution.namespace)
            {
                return Err(Error::Conflict);
            }
        }
        if let Some(old) = records.get(&record.name) {
            if old.preparation != record.preparation {
                let mut renewed = old.preparation.clone();
                renewed.expires_at = record.preparation.expires_at;
                // Renew only expired pre-grant attempts with identical inputs.
                // Once delivery may have happened, a new allocation is required.
                if !matches!(old.phase.as_str(), "prepared" | "bound" | "proving")
                    || old.preparation.expires_at > now()
                    || renewed != record.preparation
                {
                    return Err(Error::Conflict);
                }
                if store.running.lock().contains(&record.name) {
                    return Err(Error::Conflict);
                }
                store.save(&record)?;
                records.insert(record.name.clone(), record.clone());
            }
        } else {
            store.save(&record)?;
            records.insert(record.name.clone(), record.clone());
        }
        Ok((record.name.clone(), workspace_name(&launch.execution.namespace), request))
    })
}
async fn launch_status(
    State(c): State<Arc<WorkloadController>>,
    ApiPath(name): ApiPath<String>,
) -> HttpResponse {
    private(launches(&c).and_then(|s| s.get(&name)).and_then(|r| {
        Ok(json!({"name":r.name,"phase":r.phase,
        "execution":r.launch()?.execution,"sandbox_id":r.sandbox_id,"proof_job_id":r.job_id,
        "expires_at":r.preparation.expires_at}))
    }))
}

#[derive(Clone)]
pub(crate) struct Compute(pub(crate) Arc<WorkloadController>);
impl Compute {
    fn record(&self, name: &str, id: Option<&str>) -> Result<Record> {
        let r = launches(&self.0)?.get(name)?;
        if id.is_some_and(|id| r.sandbox_id.as_deref() != Some(id)) {
            return Err(Error::Rejected);
        }
        Ok(r)
    }
    fn observe(&self, r: &Record) -> Result<DriverSandbox> {
        // A node observation cannot replace central authorization or delivery.
        // In particular, an ambiguous dispatch requires operator reconciliation.
        if !matches!(r.phase.as_str(), "delivered" | "created") {
            return Err(Error::Unavailable);
        }
        let launch = r.launch()?;
        let raw = self.0.native_observation(&launch.execution)?;
        let Some(raw) = raw else {
            return Err(Error::Unavailable);
        };
        if raw.len() > 21848 {
            return Err(Error::Rejected);
        }
        let bytes = STANDARD.decode(raw).map_err(|_| Error::Rejected)?;
        let s = DriverSandbox::decode(bytes.as_slice()).map_err(|_| Error::Rejected)?;
        if s.spec.is_some()
            || s.id != r.sandbox_id.as_deref().unwrap_or("")
            || s.name != r.name
            || s.workspace != workspace_name(&launch.execution.namespace)
            || control_observation(&s).map_err(|_| Error::Rejected)? != s
            || s.status.as_ref().is_none_or(|s| {
                s.instance_id.is_empty()
                    || r.instance_id
                        .as_ref()
                        .is_some_and(|id| id != &s.instance_id)
            })
        {
            return Err(Error::Rejected);
        }
        Ok(s)
    }
    fn snapshot(&self, r: &Record) -> DriverSandbox {
        self.observe(r).unwrap_or_else(|_| DriverSandbox {
            id: r.sandbox_id.clone().unwrap_or_default(),
            name: r.name.clone(),
            workspace: r
                .launch()
                .map(|l| workspace_name(&l.execution.namespace))
                .unwrap_or_default(),
            status: Some(DriverSandboxStatus {
                conditions: vec![DriverCondition {
                    r#type: "Ready".into(),
                    status: "False".into(),
                    // Stock gateways treat unrecognized Ready=False reasons
                    // as terminal failures. Before Create is acknowledged,
                    // waiting for a node report is a normal starting state.
                    // Once pinned, losing that observation must fail closed:
                    // a live supervisor session cannot revive a stale owner.
                    reason: if r.phase == "created" {
                        "FleetObservationUnavailable"
                    } else {
                        "Starting"
                    }
                    .into(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        })
    }
    async fn create(&self, sandbox: DriverSandbox) -> Result<String> {
        let store = launches(&self.0)?;
        let _permit = store
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Unavailable)?;
        {
            let mut running = store.running.lock();
            if !running.insert(sandbox.name.clone()) {
                return Err(Error::Conflict);
            }
        }
        struct Running(Arc<Launches>, String);
        impl Drop for Running {
            fn drop(&mut self) {
                self.0.running.lock().remove(&self.1);
            }
        }
        let _running = Running(store.clone(), sandbox.name.clone());
        let mut r = self.record(&sandbox.name, None)?;
        let binding = r.binding()?;
        let native = binding
            .bind(&sandbox.encode_to_vec())
            .map_err(|_| Error::Rejected)?;
        let launch = r.launch()?;
        self.0
            .with_runtime_delivery(&launch.execution, None, Some(&launch), |_| Ok(()))?;
        if r.native_sha256
            .as_ref()
            .is_some_and(|h| h != native.native_sha256())
        {
            return Err(Error::Conflict);
        }
        if r.phase == "created" {
            return Ok(self.observe(&r)?.status.unwrap().instance_id);
        }
        // After private dispatch, observation is safe even after grant expiry.
        // Never mint credentials here for an expired attempt.
        if matches!(r.phase.as_str(), "dispatching" | "delivered") {
            if r.phase == "dispatching"
                && r.job_id.as_ref().is_some_and(|id| {
                    self.0.deliveries.record(id).is_ok_and(|d| {
                        matches!(
                            d.state.as_str(),
                            "dispatched" | "acknowledged" | "uncertain"
                        ) && crate::policy_gateway::identity(d.offer.execution) == launch.execution
                    })
                })
            {
                r.phase = "delivered".into();
                store.update(r.clone())?;
            }
            if let Ok(s) = self.observe(&r) {
                let instance = s.status.unwrap().instance_id;
                r.instance_id = Some(instance.clone());
                r.phase = "created".into();
                store.update(r)?;
                return Ok(instance);
            }
        }
        r.current()?;
        let policy = self.0.policy.as_ref().ok_or(Error::Unavailable)?;
        let network = policy.network.clone().ok_or(Error::Unavailable)?;
        if r.phase == "prepared" {
            let (approved, context, root) = policy.ledger.authority_snapshot(&launch.execution)?;
            if approved.policy != launch.policy {
                return Err(Error::Rejected);
            }
            let id = commitment(
                "fleet.openshell.startup.v1",
                &(
                    &launch.execution,
                    native.native_sha256(),
                    r.preparation.expires_at,
                ),
            )?;
            let time = now();
            r.request = Some(EvaluationRequest {
                execution: launch.execution.clone(),
                principal: r.preparation.principal.clone(),
                action_id: id.clone(),
                operation: "runtime.create".into(),
                resource: native.authorization_resource(),
                parameters: native.authorization_parameters(),
                context,
                previous_state_root: root,
                evaluated_at_unix: time,
                validity: Validity {
                    not_before: time,
                    expires_at: r.preparation.expires_at,
                },
            });
            r.policy = Some(approved);
            r.job_id = Some(id);
            r.sandbox_id = Some(sandbox.id.clone());
            r.native_sha256 = Some(native.native_sha256().to_owned());
            r.phase = "bound".into();
            let mut records = store.records.lock();
            if records
                .values()
                .any(|old| old.name != r.name && old.sandbox_id == r.sandbox_id)
            {
                return Err(Error::Conflict);
            }
            records.insert(r.name.clone(), r.clone());
            store.save(&r)?;
        }
        let request = r.request.clone().ok_or(Error::Rejected)?;
        native
            .validate_authorization(&launch, &request)
            .map_err(|_| Error::Rejected)?;
        let id = r.job_id.clone().ok_or(Error::Rejected)?;
        let approved = r.policy.as_ref().ok_or(Error::Rejected)?;
        let decision = RegorusEngine.evaluate(approved, &request)?;
        if r.phase == "bound" {
            let c = self.0.clone();
            let staged_request = request.clone();
            let staged_id = id.clone();
            let cost = r.preparation.cost;
            tokio::task::spawn_blocking(move || {
                c.with_runtime_delivery(&staged_request.execution, None, None, |_| {
                    c.policy
                        .as_ref()
                        .unwrap()
                        .ledger
                        .stage(&staged_id, &staged_request, cost)
                })
            })
            .await
            .map_err(|_| Error::Unavailable)??;
            r.phase = "proving".into();
            store.update(r.clone())?;
        }
        if matches!(r.phase.as_str(), "proving" | "dispatching") {
            // Fleet proof submission is idempotent for the same ID and inputs.
            // Re-submit on Create retry, including a crash before the first send.
            let proof = ProofRequest {
                request_id: id.clone(),
                program: policy.program.clone(),
                witness: RegorusEngine.encode_witness(approved, &request)?,
                expected_public_values: encode_public_values(&decision)?,
            };
            match network.submit(&proof).await {
                Ok(returned) if returned != id => return Err(Error::Rejected),
                // A lost reply may already have queued the job. Poll once;
                // unknown/rejected submission returns retryable, never strands
                // this launch in a permanently unsubmitted state.
                Err(_)
                    if !matches!(
                        network.status(&id).await,
                        Ok(ProofStatus::Pending | ProofStatus::Complete(_))
                    ) =>
                {
                    return Err(Error::Unavailable)
                }
                _ => (),
            }
            let receipt = loop {
                r.current()?;
                self.0
                    .with_runtime_delivery(&launch.execution, None, Some(&launch), |_| Ok(()))?;
                match network.status(&id).await {
                    Ok(ProofStatus::Complete(receipt)) if receipt.job_id == id => break receipt,
                    Ok(ProofStatus::Complete(_)) | Ok(ProofStatus::Failed) => {
                        return Err(Error::Rejected)
                    }
                    _ => tokio::time::sleep(Duration::from_millis(250)).await,
                }
            };
            r.phase = "dispatching".into();
            store.update(r.clone())?;
            let c = self.0.clone();
            let launch = launch.clone();
            let request = request.clone();
            let sandbox = sandbox.clone();
            let native_policy = binding.policy_sha256().to_owned();
            let id = id.clone();
            tokio::task::spawn_blocking(move || {
                let p = c.policy.as_ref().ok_or(Error::Unavailable)?;
                // Cryptography runs outside Fleet's allocation mutex. Commit
                // rechecks the staged epoch under the live allocation fence.
                let verified =
                    p.ledger
                        .verify_reservation(&decision, &receipt.encoded_receipt, None)?;
                let (grant, token) =
                    c.with_runtime_delivery(&launch.execution, None, Some(&launch), |_| {
                        let grant = p.ledger.commit_reservation(verified, None)?;
                        let token = p.ledger.issue_delivery_access(
                            &id,
                            &launch.execution,
                            &request.validity,
                        )?;
                        Ok((grant, token))
                    })?;
                let payload = ProvisionRequest::new(
                    launch,
                    &sandbox.encode_to_vec(),
                    native_policy,
                    request,
                    grant,
                    token,
                )
                .map_err(|_| Error::Rejected)?;
                stage_payload(&c, id, payload)
            })
            .await
            .map_err(|_| Error::Unavailable)??;
            r.phase = "delivered".into();
            store.update(r.clone())?;
        }
        // Staging an identical payload cannot resend a dispatched ticket.
        if r.phase != "delivered" {
            return Err(Error::Conflict);
        }
        loop {
            if let Ok(s) = self.observe(&r) {
                let instance = s.status.unwrap().instance_id;
                r.instance_id = Some(instance.clone());
                r.phase = "created".into();
                store.update(r)?;
                return Ok(instance);
            }
            r.current()?;
            let delivery = self.0.deliveries.record(&id)?;
            if !matches!(
                delivery.state.as_str(),
                "queued" | "dispatched" | "acknowledged" | "uncertain"
            ) {
                return Err(Error::Rejected);
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

#[tonic_native::async_trait]
impl compute_driver_server::ComputeDriver for Compute {
    type WatchSandboxesStream =
        tokio_stream::wrappers::ReceiverStream<std::result::Result<WatchSandboxesEvent, Status>>;
    async fn get_capabilities(
        &self,
        r: Request<GetCapabilitiesRequest>,
    ) -> std::result::Result<Response<GetCapabilitiesResponse>, Status> {
        let mut extension = extension_protocol::extension_metadata(
            ExtensionFamily::Compute,
            "fleet/openshell",
            env!("CARGO_PKG_VERSION"),
            [],
        );
        extension
            .required_capabilities
            .push(extension_protocol::COMPUTE_LAUNCH_AUTHENTICATION.into());
        extension_protocol::validate_gateway_metadata(
            ExtensionFamily::Compute,
            "fleet",
            Some(&extension),
            r.into_inner().gateway,
        )
        .map_err(|_| Status::failed_precondition("incompatible gateway"))?;
        Ok(Response::new(GetCapabilitiesResponse {
            driver_name: "docker".into(),
            extension: Some(extension),
            resource_admission_policy: launches(&self.0).map_err(status)?.admission_policy.clone(),
            resource_capabilities: Some(ResourceCapabilities {
                cpu: Some(CpuResourceCapabilities {
                    limit_supported: true,
                }),
                memory: Some(MemoryResourceCapabilities {
                    limit_supported: true,
                }),
                // Only an exact count: Fleet binds it to the Ray-granted devices.
                gpu: Some(GpuResourceCapabilities {
                    default_selection_supported: false,
                    count_selection_supported: true,
                }),
            }),
            gateway_manages_lifecycle: false,
            driver_reports_runtime_readiness: false,
            ..Default::default()
        }))
    }
    async fn validate_sandbox_create(
        &self,
        r: Request<ValidateSandboxCreateRequest>,
    ) -> std::result::Result<Response<ValidateSandboxCreateResponse>, Status> {
        let s = r
            .into_inner()
            .sandbox
            .ok_or_else(|| Status::invalid_argument("missing sandbox"))?;
        let r = self.record(&s.name, None).map_err(status)?;
        r.current().map_err(status)?;
        let launch = r.launch().map_err(status)?;
        self.0
            .with_runtime_delivery(&launch.execution, None, Some(&launch), |_| {
                r.binding()?
                    .validate_proposal(&s)
                    .map_err(|_| Error::Rejected)
            })
            .map_err(status)?;
        Ok(Response::new(ValidateSandboxCreateResponse {}))
    }
    async fn create_sandbox(
        &self,
        r: Request<CreateSandboxRequest>,
    ) -> std::result::Result<Response<CreateSandboxResponse>, Status> {
        let s = r
            .into_inner()
            .sandbox
            .ok_or_else(|| Status::invalid_argument("missing sandbox"))?;
        if s.encoded_len() > 1024 * 1024 {
            return Err(Status::invalid_argument("oversize sandbox"));
        }
        let this = self.clone();
        // Accepted work survives a disconnected gateway RPC. A duplicate callback
        // can observe the journal but cannot submit or deliver a second mutation.
        let runtime_identity = tokio::spawn(async move { this.create(s).await })
            .await
            .map_err(|_| unavailable())?
            .map_err(status)?;
        Ok(Response::new(CreateSandboxResponse { runtime_identity }))
    }
    async fn get_sandbox(
        &self,
        r: Request<GetSandboxRequest>,
    ) -> std::result::Result<Response<GetSandboxResponse>, Status> {
        let q = r.into_inner();
        let r = self
            .record(&q.name, Some(&q.sandbox_id))
            .map_err(|_| Status::not_found("unknown Fleet sandbox"))?;
        Ok(Response::new(GetSandboxResponse {
            sandbox: Some(self.snapshot(&r)),
        }))
    }
    async fn list_sandboxes(
        &self,
        _: Request<ListSandboxesRequest>,
    ) -> std::result::Result<Response<ListSandboxesResponse>, Status> {
        let records = launches(&self.0).map_err(status)?.active_records(&self.0);
        Ok(Response::new(ListSandboxesResponse {
            sandboxes: records.iter().map(|r| self.snapshot(r)).collect(),
        }))
    }
    async fn watch_sandboxes(
        &self,
        _: Request<WatchSandboxesRequest>,
    ) -> std::result::Result<Response<Self::WatchSandboxesStream>, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let this = self.clone();
        tokio::spawn(async move {
            let mut previous = BTreeSet::new();
            while let Ok(store) = launches(&this.0) {
                let mut records = store.active_records(&this.0);
                let active: BTreeSet<_> = records.iter().map(|r| r.name.clone()).collect();
                // Deliver one final unavailable observation when a live owner
                // disappears, without rescanning its history on every tick.
                records.extend(
                    previous
                        .difference(&active)
                        .filter_map(|name| store.get(name).ok()),
                );
                previous = active;
                for r in records {
                    let s = this.snapshot(&r);
                    if tx
                        .send(Ok(WatchSandboxesEvent {
                            payload: Some(watch_sandboxes_event::Payload::Sandbox(
                                WatchSandboxesSandboxEvent { sandbox: Some(s) },
                            )),
                        }))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                tokio::select! {_=tx.closed()=>return,_=tokio::time::sleep(Duration::from_secs(1))=>()}
            }
        });
        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }
    async fn ensure_workspace(
        &self,
        r: Request<EnsureWorkspaceRequest>,
    ) -> std::result::Result<Response<EnsureWorkspaceResponse>, Status> {
        let workspace = r.into_inner().workspace;
        let store = launches(&self.0).map_err(status)?;
        if !store.records.lock().values().any(|r| {
            r.launch()
                .is_ok_and(|l| workspace_name(&l.execution.namespace) == workspace)
        }) {
            return Err(Status::permission_denied(
                "workspace has no prepared Fleet allocation",
            ));
        }
        Ok(Response::new(EnsureWorkspaceResponse {}))
    }
    async fn authenticate_sandbox(
        &self,
        _: Request<AuthenticateSandboxRequest>,
    ) -> std::result::Result<Response<AuthenticateSandboxResponse>, Status> {
        Err(Status::unimplemented(
            "use gateway-issued launch credentials",
        ))
    }
    async fn stop_sandbox(
        &self,
        _: Request<StopSandboxRequest>,
    ) -> std::result::Result<Response<StopSandboxResponse>, Status> {
        Err(Status::failed_precondition(
            "stop through Fleet allocation lifecycle",
        ))
    }
    async fn start_sandbox(
        &self,
        _: Request<StartSandboxRequest>,
    ) -> std::result::Result<Response<StartSandboxResponse>, Status> {
        Err(Status::failed_precondition(
            "start through a new Fleet allocation",
        ))
    }
    async fn suspend_sandbox(
        &self,
        _: Request<SuspendSandboxRequest>,
    ) -> std::result::Result<Response<SuspendSandboxResponse>, Status> {
        Err(Status::unimplemented("suspend unavailable"))
    }
    async fn delete_sandbox(
        &self,
        _: Request<DeleteSandboxRequest>,
    ) -> std::result::Result<Response<DeleteSandboxResponse>, Status> {
        Err(Status::failed_precondition(
            "delete through Fleet allocation lifecycle",
        ))
    }
    async fn delete_workspace(
        &self,
        _: Request<DeleteWorkspaceRequest>,
    ) -> std::result::Result<Response<DeleteWorkspaceResponse>, Status> {
        Err(Status::failed_precondition(
            "delete through Fleet namespace lifecycle",
        ))
    }
}

pub(crate) async fn serve(c: Arc<WorkloadController>, path: PathBuf) -> anyhow::Result<()> {
    use fs2::FileExt;
    use std::os::unix::fs::{FileTypeExt, OpenOptionsExt};
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("missing socket parent"))?;
    let m = std::fs::symlink_metadata(parent)?;
    anyhow::ensure!(
        m.is_dir() && m.uid() == unsafe { libc::geteuid() } && m.permissions().mode() & 0o077 == 0,
        "private same-user compute directory required"
    );
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path.with_extension("lock"))?;
    let m = lock.metadata()?;
    anyhow::ensure!(
        m.is_file() && m.uid() == unsafe { libc::geteuid() } && m.permissions().mode() & 0o077 == 0,
        "private socket lock required"
    );
    lock.try_lock_exclusive()?;
    if let Ok(m) = std::fs::symlink_metadata(&path) {
        anyhow::ensure!(
            m.file_type().is_socket()
                && m.uid() == unsafe { libc::geteuid() }
                && m.permissions().mode() & 0o077 == 0,
            "unsafe existing socket"
        );
        match tokio::time::timeout(
            Duration::from_secs(2),
            tokio::net::UnixStream::connect(&path),
        )
        .await
        {
            Ok(Err(e))
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
                ) =>
            {
                std::fs::remove_file(&path)?
            }
            _ => anyhow::bail!("compute socket is already in use"),
        }
    }
    let listener = tokio::net::UnixListener::bind(&path)?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    struct SocketGuard(PathBuf, u64, u64, std::fs::File);
    impl Drop for SocketGuard {
        fn drop(&mut self) {
            if std::fs::symlink_metadata(&self.0)
                .is_ok_and(|m| m.dev() == self.1 && m.ino() == self.2)
            {
                let _ = std::fs::remove_file(&self.0);
            }
            let _ = self.3.unlock();
        }
    }
    let meta = std::fs::symlink_metadata(&path)?;
    let _guard = SocketGuard(path.clone(), meta.dev(), meta.ino(), lock);
    let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);
    use tokio_stream::StreamExt;
    let incoming = incoming.filter_map(|s| match s {
        Ok(s)
            if s.peer_cred()
                .is_ok_and(|p| p.uid() == unsafe { libc::geteuid() }) =>
        {
            Some(Ok(s))
        }
        Ok(_) => None,
        Err(e) => Some(Err(e)),
    });
    tonic_native::transport::Server::builder()
        .add_service(
            compute_driver_server::ComputeDriverServer::new(Compute(c))
                .max_decoding_message_size(1024 * 1024)
                .max_encoding_message_size(4 * 1024 * 1024),
        )
        .serve_with_incoming(incoming)
        .await?;
    Ok(())
}
