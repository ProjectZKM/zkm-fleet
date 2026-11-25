//! A native app's policy, set by the app's owner, and the scheduler's own launch
//! of every replica generation with it: it activates the policy for the replica's
//! execution, prepares the launch and submits the create to the stock gateway.
//! The compute extension still proves and delivers each create.
use super::*;
use fleet_proto::model::v1::AppPolicy;
use sha2::{Digest, Sha256};
use trusted_shim::{
    contracts::ExecutionIdentity,
    ledger_transport::ExecutionAuthority,
    openshell_gateway::deterministic_policy_hash,
    rail::rego::{engine_profile, policy_digest},
};

const ZERO_ROOT: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const MAX_CONTEXT: usize = 256 * 1024;
/// Launch attempts in flight; each one waits for its create's proof and delivery.
const MAX_ATTEMPTS: usize = 16;
/// How long an approval request can be approved and then used.
const APPROVAL_SECS: u64 = 24 * 3600;
/// An app's approval requests that are pending or approved and not yet used.
const MAX_OPEN_APPROVALS: usize = 32;
const FIRST_RETRY: Duration = Duration::from_secs(2);
const LAST_RETRY: Duration = Duration::from_secs(60);

/// The policy budget every launch of a namespace's apps draws from. An
/// administrator sets its allowance with `PUT /policy/budgets`.
pub(crate) fn budget(namespace: &str) -> String {
    format!("namespace:{namespace}")
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Selection {
    namespace: String,
    app: String,
    policy: AppPolicy,
}
impl Selection {
    fn file(namespace: &str, app: &str) -> String {
        let mut hash = Sha256::new();
        hash.update(namespace.as_bytes());
        hash.update([0]);
        hash.update(app.as_bytes());
        format!("{}.json", hex::encode(hash.finalize()))
    }
    pub(super) fn record(&self) -> ((String, String), String) {
        ((self.namespace.clone(), self.app.clone()), Self::file(&self.namespace, &self.app))
    }
    fn bundle(&self) -> Result<PolicyBundle> {
        serde_json::from_value(self.policy.policy.0.clone()).map_err(|_| Error::InvalidInput)
    }
    fn root(&self) -> &str {
        self.policy.state_root.as_deref().unwrap_or(ZERO_ROOT)
    }
    /// What the ledger and the launch preparation would refuse later.
    fn check(&self, spec: &serde_json::Value) -> Result<()> {
        let p = &self.policy;
        let bundle = self.bundle()?;
        if policy_digest(&bundle)? != bundle.policy.sha256
            || bundle.engine != engine_profile()
            || !p.context.0.is_object()
            || serde_json::to_vec(&p.context.0).map_or(true, |c| c.len() > MAX_CONTEXT)
            || p.native_policy_base64.len() > 1_398_104
            || p.principal.is_empty()
            || p.principal.len() > 256
            || p.principal.chars().any(char::is_control)
            || p.cost == 0
            || self.root().len() != 64
            || !self.root().bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(Error::InvalidInput);
        }
        let native = native_rules_hash(p)?;
        // On a driver other than OpenShell's own, the entry names the access rules it admits.
        if !fleet_proto::workload::Entry(spec).native_access()
            || spec["access_policy_sha256"].as_str().is_some_and(|hash| hash != native)
        {
            return Err(Error::Rejected);
        }
        Ok(())
    }
}

/// A request to approve one exec that the replica's policy answered
/// `approval_required`. Approving it changes no decision: the same exec, sent
/// again with it, is evaluated afresh with the approval among its parameters,
/// and runs once if the policy then allows it.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Approval {
    /// The policy job of the decision that asked for it.
    pub(super) id: String,
    execution: ExecutionIdentity,
    /// The exec as the policy saw it: command, user, stdin hash and timeout.
    parameters: serde_json::Value,
    principal: String,
    reason_codes: Vec<String>,
    /// The token that sent the exec; none for an administrator's.
    requester: Option<String>,
    requested_at: u64,
    expires_at: u64,
    /// `pending`, `approved`, `refused` or `used`.
    state: String,
    approver: Option<String>,
    /// Approved with another credential than the one that sent the exec.
    independent: bool,
    decided_at: Option<u64>,
}
impl Approval {
    pub(super) fn file(&self) -> (String, String) {
        (self.id.clone(), format!("{}.json", self.id))
    }
    /// What the policy reads as `input.parameters.approval`.
    pub(super) fn fact(&self) -> serde_json::Value {
        json!({"id": self.id, "approver": self.approver, "approved_at": self.decided_at, "independent": self.independent})
    }
    fn view(&self) -> serde_json::Value {
        let state = if self.expires_at <= now() && self.state != "used" { "expired" } else { self.state.as_str() };
        json!({"id": self.id, "operation": "exec", "allocation_id": self.execution.allocation_id,
            "generation": self.execution.generation, "parameters": self.parameters, "principal": self.principal,
            "reason_codes": self.reason_codes, "state": state, "requested_at": self.requested_at,
            "expires_at": self.expires_at, "approver": self.approver, "independent": self.independent,
            "decided_at": self.decided_at})
    }
}

/// One execution's launch attempts: at most one runs, and a failed one waits longer each time.
pub(super) struct Attempt {
    running: bool,
    next: std::time::Instant,
    delay: Duration,
    error: Option<&'static str>,
}

/// A native reservation a fresh node session reports, with the entry it runs.
pub(crate) struct Reservation {
    pub execution: ExecutionIdentity,
    pub spec: serde_json::Value,
    pub gpus: Vec<String>,
}

/// The private records of `dir`, each under the key and file name `name` gives it.
pub(super) fn load<K: Ord, T: serde::de::DeserializeOwned>(
    dir: &Path,
    name: impl Fn(&T) -> (K, String),
) -> anyhow::Result<BTreeMap<K, T>> {
    let mut records = BTreeMap::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Ok(records);
    };
    for entry in entries {
        let path = entry?.path();
        if path.extension().is_none_or(|s| s != "json") {
            continue;
        }
        let m = std::fs::symlink_metadata(&path)?;
        anyhow::ensure!(
            m.is_file() && m.permissions().mode() & 0o077 == 0 && m.len() <= MAX_RECORD as u64,
            "invalid app policy or approval record"
        );
        let record: T = serde_json::from_slice(&std::fs::read(&path)?)?;
        let (key, file) = name(&record);
        anyhow::ensure!(
            path.file_name().and_then(|n| n.to_str()) == Some(&file),
            "invalid app policy or approval record name"
        );
        records.insert(key, record);
    }
    Ok(records)
}

impl Launches {
    fn selections_dir(&self) -> PathBuf {
        self.dir.join("selections")
    }
    fn selection(&self, namespace: &str, app: &str) -> Option<Selection> {
        self.selections
            .lock()
            .get(&(namespace.to_owned(), app.to_owned()))
            .cloned()
    }
    fn select(&self, selection: Selection) -> Result<()> {
        let bytes = serde_json::to_vec(&selection).map_err(|_| Error::InvalidInput)?;
        if bytes.len() > MAX_RECORD {
            return Err(Error::InvalidInput);
        }
        let mut selections = self.selections.lock();
        atomic(
            &self
                .selections_dir()
                .join(Selection::file(&selection.namespace, &selection.app)),
            &bytes,
        )
        .map_err(|_| Error::Unavailable)?;
        selections.insert((selection.namespace.clone(), selection.app.clone()), selection);
        Ok(())
    }
    fn deselect(&self, namespace: &str, app: &str) -> Result<()> {
        let mut selections = self.selections.lock();
        match std::fs::remove_file(self.selections_dir().join(Selection::file(namespace, app))) {
            Ok(()) => (),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            Err(_) => return Err(Error::Unavailable),
        }
        selections.remove(&(namespace.to_owned(), app.to_owned()));
        Ok(())
    }
    fn write_approval(&self, approvals: &mut BTreeMap<String, Approval>, approval: Approval) -> Result<()> {
        let bytes = serde_json::to_vec(&approval).map_err(|_| Error::InvalidInput)?;
        if bytes.len() > MAX_RECORD {
            return Err(Error::InvalidInput);
        }
        atomic(&self.dir.join("approvals").join(approval.file().1), &bytes).map_err(|_| Error::Unavailable)?;
        approvals.insert(approval.id.clone(), approval);
        Ok(())
    }
    /// Records that the exec `parameters` in `execution` waits for an approval,
    /// under the policy job `id` of the decision that asked for one.
    pub(super) fn request_approval(
        &self,
        id: &str,
        execution: &ExecutionIdentity,
        parameters: serde_json::Value,
        principal: &str,
        reason_codes: Vec<String>,
        requester: Option<String>,
    ) -> Result<serde_json::Value> {
        let mut approvals = self.approvals.lock();
        let open = approvals.values().filter(|a| {
            a.execution.namespace == execution.namespace
                && a.execution.workload == execution.workload
                && a.expires_at > now()
                && matches!(a.state.as_str(), "pending" | "approved")
        });
        if open.count() >= MAX_OPEN_APPROVALS {
            return Err(Error::Unavailable);
        }
        let approval = Approval {
            id: id.to_owned(),
            execution: execution.clone(),
            parameters,
            principal: principal.to_owned(),
            reason_codes,
            requester,
            requested_at: now(),
            expires_at: now() + APPROVAL_SECS,
            state: "pending".into(),
            approver: None,
            independent: false,
            decided_at: None,
        };
        let reply = json!({"id": approval.id, "expires_at": approval.expires_at});
        self.write_approval(&mut approvals, approval)?;
        Ok(reply)
    }
    /// The approval `id`, when it stands for exactly this exec in this execution.
    pub(super) fn approved(&self, id: &str, execution: &ExecutionIdentity, parameters: &serde_json::Value) -> Option<Approval> {
        self.approvals.lock().get(id).filter(|a| {
            a.state == "approved" && a.expires_at > now() && a.execution == *execution && a.parameters == *parameters
        }).cloned()
    }
    /// An approval runs one command: a second use finds it spent.
    pub(super) fn spend(&self, id: &str) -> Result<()> {
        let mut approvals = self.approvals.lock();
        let mut approval = approvals.get(id).filter(|a| a.state == "approved" && a.expires_at > now()).cloned().ok_or(Error::Conflict)?;
        approval.state = "used".into();
        self.write_approval(&mut approvals, approval)
    }
    /// Claims `name`'s next attempt when none runs and its wait is over.
    fn begin(&self, name: &str) -> bool {
        let mut attempts = self.attempts.lock();
        let now = std::time::Instant::now();
        if attempts.values().filter(|a| a.running).count() >= MAX_ATTEMPTS {
            return false;
        }
        let attempt = attempts.entry(name.to_owned()).or_insert(Attempt {
            running: false,
            next: now,
            delay: FIRST_RETRY,
            error: None,
        });
        if attempt.running || now < attempt.next {
            return false;
        }
        attempt.running = true;
        true
    }
    fn finish(&self, name: &str, error: Option<&'static str>) {
        if let Some(attempt) = self.attempts.lock().get_mut(name) {
            attempt.running = false;
            attempt.next = std::time::Instant::now() + attempt.delay;
            attempt.delay = if error.is_some() {
                (attempt.delay * 2).min(LAST_RETRY)
            } else {
                FIRST_RETRY
            };
            attempt.error = error;
        }
    }
}

/// The exact launch Fleet admitted for this reservation: its entry's image,
/// command, environment, limits, services and volumes, and the devices Ray
/// granted. `with_runtime_delivery` holds every later step to the same values.
fn launch_request(r: &Reservation, bundle: &PolicyBundle) -> Result<LaunchRequest> {
    let (spec, e) = (&r.spec, &r.execution);
    let driver = spec["runtime"]["driver"].as_str().ok_or(Error::InvalidInput)?;
    // A TEE launch names its node's attestation profile, which the entry does not hold.
    if !matches!(driver, "docker" | "openshell") {
        return Err(Error::Unsupported);
    }
    let or = |key: &str, default: serde_json::Value| spec.get(key).cloned().unwrap_or(default);
    let ingress = or("ingress", json!({}));
    let protocol = or("service_protocol", json!("http1"));
    let names = or("services", json!([]));
    let services: Vec<_> = names
        .as_array()
        .ok_or(Error::InvalidInput)?
        .iter()
        .filter_map(serde_json::Value::as_str)
        .map(|service| {
            let control = service == "control";
            json!({"id": {"namespace": e.namespace, "workload": e.workload, "service": service},
                "protocol": if control { json!("http1") } else { protocol.clone() },
                "endpoint_ref": if control { "control".to_owned() }
                    else if ingress.get(service).is_some() { format!("ingress:{service}") }
                    else { "app".to_owned() }})
        })
        .collect();
    let mut options = serde_json::Value::Null;
    if !services.is_empty() {
        options = json!({"container_port": spec["container_port"], "health_path": or("health_path", json!("/"))});
        if spec["health_tcp_banner"].is_string() {
            options["health_tcp_banner"] = spec["health_tcp_banner"].clone();
        }
        if ingress.as_object().is_some_and(|i| !i.is_empty()) {
            options["ingress"] = ingress;
        }
    }
    let scaled = |key: &str, unit: f64| spec[key].as_f64().map(|n| (n * unit).round() as u64);
    let mut launch = json!({
        "execution": e, "driver": driver, "image": spec["image"],
        "command": or("command", json!([])), "environment": or("env", json!({})),
        "limits": {"cpu_millicores": scaled("cpus", 1000.0), "memory_bytes": scaled("memory_gb", (1u64 << 30) as f64)},
        "policy": bundle.policy,
        "trust": {"attestation": {"mode": "skip_non_tee"}, "policy_proof": "not_required", "attestation_proof": "not_required"},
        "services": services, "driver_options": options, "gpus": r.gpus, "mounts": or("mounts", json!([])),
    });
    if spec["access_policy_sha256"].is_string() {
        launch["access_policy_sha256"] = spec["access_policy_sha256"].clone();
    }
    serde_json::from_value(launch).map_err(|_| Error::InvalidInput)
}

/// One attempt to get `r` created. Every step repeats safely: activation with
/// the same values changes nothing, an identical preparation is the same record,
/// and the compute extension resumes a create it already accepted.
async fn launch(
    c: &Arc<WorkloadController>,
    store: &Arc<Launches>,
    gateway: &dyn ExecGateway,
    r: Reservation,
    selection: Selection,
    name: &str,
) -> std::result::Result<(), &'static str> {
    let existing = store.get(name).ok();
    // A grant may be out for a create past its proof; only the same request can finish it.
    let (workspace, request) = match existing {
        Some(record)
            if matches!(record.phase.as_str(), "dispatching" | "delivered") || record.current().is_ok() =>
        {
            let request = record.binding().map_err(|_| "its launch record is invalid")?.request();
            (workspace_name(&r.execution.namespace), request)
        }
        expired => {
            let preparation = match expired {
                Some(record) => OpenShellLaunchPreparation {
                    expires_at: now() + 900,
                    ..record.preparation
                },
                None => {
                    let bundle = selection.bundle().map_err(|_| "its policy is invalid")?;
                    let launch = launch_request(&r, &bundle)
                        .map_err(|_| "its entry has no launch on this path (a TEE driver, or a setting the native profile lacks)")?;
                    let policy = c.policy.clone().ok_or("the policy ledger is not configured")?;
                    let (controller, e, context, root) =
                        (c.clone(), r.execution.clone(), selection.policy.context.0.clone(), selection.root().to_owned());
                    tokio::task::spawn_blocking(move || {
                        let _guard = controller.guard(&e)?;
                        policy.ledger.activate(&e, &bundle, &context, &root, &budget(&e.namespace))
                    })
                    .await
                    .unwrap_or(Err(Error::Unavailable))
                    .map_err(|_| "the ledger refused its policy: the namespace needs a policy budget, and the reservation must be current")?;
                    OpenShellLaunchPreparation {
                        request: serde_json::to_value(&launch).map_err(|_| "its launch is invalid")?.into(),
                        native_policy_base64: selection.policy.native_policy_base64.clone(),
                        principal: selection.policy.principal.clone(),
                        expires_at: now() + 900,
                        cost: selection.policy.cost,
                    }
                }
            };
            let controller = c.clone();
            let (_, workspace, request) = tokio::task::spawn_blocking(move || prepare_record(&controller, preparation))
                .await
                .unwrap_or(Err(Error::Unavailable))
                .map_err(|_| "its launch was refused: the entry, the policy and the node's native profile must agree")?;
            (workspace, request)
        }
    };
    gateway
        .create(&workspace, request)
        .await
        .map_err(|_| "the gateway's create failed, or its policy decision or proof did")
}

/// Launches every native reservation whose app has a policy, and again for each
/// new generation. Runs for the scheduler's life; without the stock gateway's
/// settings there is nothing to submit to.
pub(crate) async fn launch_loop(c: Arc<WorkloadController>) {
    let Some((store, gateway)) = launches(&c)
        .ok()
        .and_then(|store| store.control.clone().map(|gateway| (store, gateway)))
    else {
        return std::future::pending().await;
    };
    loop {
        let reservations = c.native_reservations();
        let mut names = BTreeSet::new();
        for r in reservations {
            let Ok(name) = GatewayLaunch::execution_name(&r.execution) else {
                continue;
            };
            names.insert(name.clone());
            let Some(selection) = store.selection(&r.execution.namespace, &r.execution.workload) else {
                continue;
            };
            if store.get(&name).is_ok_and(|record| record.phase == "created") {
                store.finish(&name, None);
                continue;
            }
            if !store.begin(&name) {
                continue;
            }
            let (c, store, gateway) = (c.clone(), store.clone(), gateway.clone());
            tokio::spawn(async move {
                let result = launch(&c, &store, gateway.as_ref(), r, selection, &name).await;
                if let Err(error) = result {
                    tracing::warn!(launch = %name, error, "native launch attempt failed");
                }
                store.finish(&name, result.err());
            });
        }
        store
            .attempts
            .lock()
            .retain(|name, attempt| attempt.running || names.contains(name));
        // A policy goes with its app (`delete`); this also covers an app its node no longer configures.
        let stale: Vec<_> = store
            .selections
            .lock()
            .keys()
            .filter(|(namespace, app)| !c.app_exists(namespace, app))
            .cloned()
            .collect();
        for (namespace, app) in stale {
            let _ = store.deselect(&namespace, &app);
        }
        // An approval request goes when it expires, and with its app.
        store.approvals.lock().retain(|_, a| {
            let keep = a.expires_at > now() && c.app_exists(&a.execution.namespace, &a.execution.workload);
            if !keep {
                let _ = std::fs::remove_file(store.dir.join("approvals").join(a.file().1));
            }
            keep
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// What an entry's `access_policy_sha256` must be for this policy's native rules.
pub(crate) fn native_rules_hash(policy: &AppPolicy) -> Result<String> {
    let raw = STANDARD
        .decode(&policy.native_policy_base64)
        .map_err(|_| Error::InvalidInput)?;
    let native = SandboxPolicy::decode(raw.as_slice()).map_err(|_| Error::InvalidInput)?;
    Ok(deterministic_policy_hash(&native))
}

/// `PUT /apps/NAME/policy` and the `policy` of `POST /apps`.
pub(crate) fn set_app_policy(
    c: &WorkloadController,
    namespace: &str,
    app: &str,
    spec: &serde_json::Value,
    policy: AppPolicy,
    store: bool,
) -> Result<()> {
    let selection = Selection {
        namespace: namespace.to_owned(),
        app: app.to_owned(),
        policy,
    };
    // Without the stock gateway's settings, nothing would launch the app with it.
    let launches = launches(c).ok().filter(|l| l.control.is_some()).ok_or(Error::Unavailable)?;
    selection.check(spec)?;
    if store {
        launches.select(selection)?;
    }
    Ok(())
}

pub(crate) fn remove_app_policy(c: &WorkloadController, namespace: &str, app: &str) -> Result<()> {
    launches(c)?.deselect(namespace, app)
}

/// `GET /apps/NAME/policy`: what is selected, never the policy's modules, data,
/// context or native rules, and where each replica's launch stands.
pub(crate) fn app_policy(c: &WorkloadController, namespace: &str, app: &str) -> Result<serde_json::Value> {
    let store = launches(c)?;
    let selection = store.selection(namespace, app).ok_or(Error::Rejected)?;
    let attempts = store.attempts.lock();
    let launches: Vec<_> = c
        .native_reservations()
        .into_iter()
        .filter(|r| r.execution.namespace == namespace && r.execution.workload == app)
        .filter_map(|r| {
            let name = GatewayLaunch::execution_name(&r.execution).ok()?;
            let phase = store.get(&name).map_or("pending".to_owned(), |record| record.phase);
            Some(json!({"allocation_id": r.execution.allocation_id, "generation": r.execution.generation,
                "node_id": r.execution.node_id, "phase": phase,
                "error": attempts.get(&name).and_then(|a| a.error)}))
        })
        .collect();
    Ok(json!({"policy": selection.bundle()?.policy,
        "native_policy_sha256": native_rules_hash(&selection.policy)?,
        "principal": selection.policy.principal, "cost": selection.policy.cost,
        "state_root": selection.root(), "budget": budget(namespace), "launches": launches}))
}

/// `GET /apps/NAME/approvals`: the app's approval requests, newest first.
pub(crate) fn app_approvals(c: &WorkloadController, namespace: &str, app: &str) -> Result<serde_json::Value> {
    let store = launches(c)?;
    let mut approvals: Vec<_> = store
        .approvals
        .lock()
        .values()
        .filter(|a| a.execution.namespace == namespace && a.execution.workload == app)
        .cloned()
        .collect();
    approvals.sort_by_key(|a| std::cmp::Reverse(a.requested_at));
    Ok(json!({"approvals": approvals.iter().map(Approval::view).collect::<Vec<_>>()}))
}

/// `POST /apps/NAME/approvals/ID`: approves or refuses one pending request, once.
/// `token` is the deciding credential's ID, none for an administrator's.
pub(crate) fn decide_approval(
    c: &WorkloadController,
    namespace: &str,
    app: &str,
    id: &str,
    approver: &str,
    approve: bool,
    token: Option<&str>,
) -> Result<serde_json::Value> {
    if approver.is_empty() || approver.len() > 256 || approver.chars().any(char::is_control) {
        return Err(Error::InvalidInput);
    }
    let store = launches(c)?;
    let mut approvals = store.approvals.lock();
    let mut approval = approvals
        .get(id)
        .filter(|a| a.execution.namespace == namespace && a.execution.workload == app)
        .cloned()
        .ok_or(Error::Rejected)?;
    if approval.state != "pending" || approval.expires_at <= now() {
        return Err(Error::Conflict);
    }
    approval.state = if approve { "approved" } else { "refused" }.into();
    approval.approver = Some(approver.to_owned());
    approval.independent = approval.requester.as_deref() != token;
    approval.decided_at = Some(now());
    let view = approval.view();
    store.write_approval(&mut approvals, approval)?;
    Ok(view)
}
