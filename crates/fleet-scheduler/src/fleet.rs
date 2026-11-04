//! Scheduler core: jobs in memory (persisted to the store), the node registry,
//! anchoring, and the dispatcher that assigns jobs on nodes.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use tokio::sync::{watch, Notify};

use fleet_proto::fleet::v1::{Assignment, Completion, HeartbeatRequest, HeartbeatResponse, NodeInfo, ProveRequest,
};
use fleet_proto::stage::v1::Status as StageStatus;

use crate::blobs::{self, BlobStore};
use crate::config::SchedulerConfig;
use crate::placement::{self, AnchorPolicy, AppPolicy, PlacementDecision, PlacementCandidate, SchedulingClass};
use crate::quota::{Allocation, Quota, Usage};
use crate::store::{normalize_address, App, ProofJob, ProofJobState, Namespace, Store, Token};
use fleet_proto::model::v1::RegisterAppRequest;


/// How an attempt failed, which decides whether it counts and what it says about the node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProofAttemptFailure {
    /// The node could not be reached: the job never started. Not counted as an attempt.
    NotStarted,
    /// The connection broke or timed out mid-proof: the node or its machine is suspect.
    Lost,
    /// The node answered with an error.
    Prover,
    /// The node is proving something this scheduler did not send it. Not counted.
    Busy,
}

/// `RegisteredProver::busy` for a proof the node reported itself rather than one dispatched here.
const REPORTED: char = '~';

fn reported(job_id: &str) -> String {
    format!("{REPORTED}{job_id}")
}

/// Nodes tried per job, whatever the reason, before it fails.
const MAX_NODES_TRIED: usize = 8;

fn classify(status: &tonic::Status) -> ProofAttemptFailure {
    let m = status.message();
    match status.code() {
        tonic::Code::Unavailable => ProofAttemptFailure::NotStarted,
        tonic::Code::ResourceExhausted => ProofAttemptFailure::Busy,
        // The node could not reach its GPU server (restarting, removed): nothing about the job.
        _ if m.contains("CUDA RPC")
            && (m.contains("error sending request") || m.contains("onnection refused")) =>
        {
            ProofAttemptFailure::NotStarted
        }
        tonic::Code::Unknown | tonic::Code::Cancelled | tonic::Code::Aborted
            if m.contains("transport") || m.contains("connection") || m.contains("broken pipe") =>
        {
            ProofAttemptFailure::Lost
        }
        _ => ProofAttemptFailure::Prover,
    }
}

/// Whether `name` can name an app or batch: letters, digits, `_`, `-` and `.`, at most 63.
pub fn valid_app_name(name: &str) -> bool {
    !name.is_empty() && name.len() <= 63 && name.bytes().all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
}

fn valid_elf_id(elf_id: &str) -> bool {
    elf_id.len() == 64 && elf_id.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// The node backends an app may name.
const BACKENDS: [&str; 3] = ["cuda", "cpu", "mock"];

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

struct ProofJobEntry {
    job: ProofJob,
    /// Bumped on every change; `GetStatus` long-polls on it.
    changed: watch::Sender<u64>,
}

pub struct RegisteredProver {
    pub id: String,
    pub info: NodeInfo,
    pub last_heartbeat: Instant,
    pub ready: bool,
    /// The job running there: dispatched by this scheduler, or `~job` as the node reported it.
    pub busy: Option<String>,
    /// The last job released here; a heartbeat sent before the release still names it.
    last_released: Option<String>,
    pub cached: HashSet<String>,
}

#[derive(Default)]
struct SchedulerState {
    jobs: HashMap<String, ProofJobEntry>,
    nodes: HashMap<String, RegisteredProver>,
    /// (namespace, app) -> dedicated node name.
    anchors: HashMap<(String, String), String>,
    /// node name -> app key (`namespace/name`), for exclusive anchors.
    reserved: HashMap<String, String>,
    /// Machine identities taking no new proofs, including replicas registered later.
    draining_machines: HashSet<String>,
    /// Assignments become visible only after their job and quota are durable.
    deliverable: HashSet<String>,
}

pub struct Scheduler {
    pub cfg: SchedulerConfig,
    pub stage_submissions: tokio::sync::Mutex<()>,
    pub store: Arc<dyn Store>,
    pub blobs: BlobStore,
    pub proof_fetch: crate::proof_fetch::ProofFetcher,
    pub log_relay: crate::log_relay::LogRelay,
    cluster_token: String,
    completion_ops: tokio::sync::Mutex<()>,
    inner: Mutex<SchedulerState>,
    /// address -> token, mirrored from the store.
    tokens: Mutex<HashMap<String, Token>>,
    /// Held while the token mirror changes, so a reload does not undo an issue or revoke.
    token_ops: tokio::sync::Mutex<()>,
    namespaces: Mutex<HashMap<String, Namespace>>,
    allocations: Mutex<HashMap<String, Allocation>>,
    quota_ops: tokio::sync::Mutex<()>,
    /// The registered proof apps, (namespace, name) -> record, mirrored from the store. A job
    /// executes only for one of them.
    apps: Mutex<HashMap<(String, String), App>>,
    worker_owned_apps: Mutex<HashSet<(String, String)>>,
    wake: Notify,
    started: Instant,
    stopping: AtomicBool,
    /// Submits hold it shared, an app's delete exclusively: no job of an app is accepted
    /// while the app (and its ELF) is being deleted.
    app_ops: tokio::sync::RwLock<()>,
}

/// A request refused for what it asks (the admin API answers with `status`).
#[derive(Debug)]
pub struct RequestRejection {
    pub status: u16,
    pub message: String,
}

impl std::fmt::Display for RequestRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for RequestRejection {}

fn refuse(status: u16, message: String) -> anyhow::Error {
    RequestRejection { status, message }.into()
}

use fleet_proto::model::v1::AppAnchor;
pub use fleet_proto::model::v1::{FleetStatus, NodeStatus};

impl Scheduler {
    pub fn cluster_authorized(&self, header: Option<&str>) -> bool {
        crate::token::bearer_matches(header, &self.cluster_token)
    }

    pub fn new(
        cfg: SchedulerConfig,
        store: Arc<dyn Store>,
        blobs: BlobStore,
        cluster_token: String,
    ) -> Arc<Self> {
        Arc::new(Self {
            stage_submissions: tokio::sync::Mutex::new(()),
            cfg,
            store,
            blobs,
            proof_fetch: crate::proof_fetch::ProofFetcher::default(),
            log_relay: crate::log_relay::LogRelay::default(),
            cluster_token,
            completion_ops: tokio::sync::Mutex::new(()),
            inner: Mutex::new(SchedulerState::default()),
            tokens: Mutex::new(HashMap::new()),
            token_ops: tokio::sync::Mutex::new(()),
            namespaces: Mutex::new(HashMap::new()),
            allocations: Mutex::new(HashMap::new()),
            quota_ops: tokio::sync::Mutex::new(()),
            apps: Mutex::new(HashMap::new()),
            worker_owned_apps: Mutex::new(HashSet::new()),
            wake: Notify::new(),
            started: Instant::now(),
            stopping: AtomicBool::new(false),
            app_ops: tokio::sync::RwLock::new(()),
        })
    }

    /// The registered apps, from the store; how many there are.
    pub async fn load_apps(&self) -> anyhow::Result<usize> {
        let map: HashMap<_, _> = self.store.apps().await?.into_iter().map(|a| ((a.namespace.clone(), a.name.clone()), a)).collect();
        let n = map.len();
        *self.apps.lock() = map;
        Ok(n)
    }

    /// The registered app `name` of the namespace.
    pub fn app(&self, namespace: &str, name: &str) -> Option<App> {
        self.apps.lock().get(&(namespace.to_string(), name.to_string())).cloned()
    }

    /// The namespace's registered app of this program.
    pub fn app_for_elf(&self, namespace: &str, elf_id: &str) -> Option<App> {
        self.apps.lock().values().find(|a| a.namespace == namespace && a.elf_id == elf_id).cloned()
    }

    pub fn app_known(&self, namespace: &str, name: &str) -> bool {
        self.apps.lock().contains_key(&(namespace.to_string(), name.to_string()))
    }

    /// Whether the app refuses new jobs.
    pub fn app_disabled(&self, namespace: &str, name: &str) -> bool {
        self.app(namespace, name).is_some_and(|a| a.disabled)
    }

    pub(crate) fn set_worker_owned_apps(&self, apps: HashSet<(String, String)>) {
        *self.worker_owned_apps.lock() = apps;
    }

    pub(crate) fn app_worker_owned(&self, namespace: &str, name: &str) -> bool {
        self.worker_owned_apps.lock().contains(&(namespace.into(), name.into()))
    }

    pub fn app_priority(&self, namespace: &str, name: &str) -> i32 {
        self.app(namespace, name).map_or(0, |a| a.priority)
    }

    pub async fn set_app_priority(&self, namespace: &str, name: &str, priority: i32,
    ) -> anyhow::Result<serde_json::Value> {
        let _ops = self.app_ops.write().await;
        if !self.app_known(namespace, name) { return Err(refuse(404, format!("no app {name}"))); }
        self.store.set_app_priority(namespace, name, priority).await?;
        if let Some(a) = self.apps.lock().get_mut(&(namespace.into(), name.into())) { a.priority = priority; }
        Ok(serde_json::json!({"namespace":namespace,"name":name,"priority":priority}))
    }

    /// Registers a program as an app of the namespace: from then on its jobs are accepted (the
    /// ELF itself arrives with the first one). Its policy says where they are placed.
    pub async fn add_app(&self, reference: &str, r: &RegisterAppRequest) -> anyhow::Result<App> {
        let _ops = self.app_ops.write().await;
        let namespace = self.namespace(reference).ok_or_else(|| {
            refuse(404, "unknown namespace; an administrator must create it first".into())
        })?;
        if namespace.suspended_ms.is_some() { return Err(refuse(403, "namespace is suspended".into())); }
        if !valid_app_name(&r.name) {
            return Err(refuse(400, "name: letters, digits, _ . -, at most 63".into()));
        }
        let elf_id = r.elf_id.as_deref().unwrap_or("").trim().trim_start_matches("0x").to_ascii_lowercase();
        if !valid_elf_id(&elf_id) {
            return Err(refuse(400, "elf_id: the program's id, 64 hex digits (the sha256 of the ELF)".into()));
        }
        let class = r.class.clone().unwrap_or_else(|| "batch".into());
        if SchedulingClass::from_name(&class).is_none() {
            return Err(refuse(400, format!("class: one of {}", SchedulingClass::NAMES.join(", "))));
        }
        let anchor = r.anchor.clone().unwrap_or_else(|| "none".into());
        if AnchorPolicy::from_name(&anchor).is_none() {
            return Err(refuse(400, format!("anchor: one of {}", AnchorPolicy::NAMES.join(", "))));
        }
        if let Some(b) = r.backends.iter().find(|b| !BACKENDS.contains(&b.as_str())) {
            return Err(refuse(400, format!("backend {b}: one of {}", BACKENDS.join(", "))));
        }
        let max_queued = r.max_queued.unwrap_or(64);
        if max_queued == 0 { return Err(refuse(400, "max_queued must be at least 1".into())); }
        if self.app_known(&namespace.id, &r.name) {
            return Err(refuse(409, format!("app {} exists", r.name)));
        }
        if let Some(other) = self.app_for_elf(&namespace.id, &elf_id) {
            return Err(refuse(409, format!("program {elf_id} is app {} of this namespace already", other.name)));
        }
        let app = App {
            namespace: namespace.id.clone(), name: r.name.clone(), elf_id, priority: r.priority.unwrap_or(0),
            disabled: false, created_ms: now_ms(), class, anchor, backends: r.backends.clone(),
            zkm_version: r.zkm_version.clone().filter(|v| !v.is_empty()), max_queued, ..Default::default()
        };
        if !self.store.add_app(&app).await? {
            return Err(refuse(409, format!("app {} or its program exists", r.name)));
        }
        self.apps.lock().insert((app.namespace.clone(), app.name.clone()), app.clone());
        tracing::info!(app = %app.name, elf = %app.elf_id, "app registered");
        Ok(app)
    }

    /// Disables an app (its new jobs are refused; queued and running ones finish) or
    /// enables it again.
    pub async fn set_app_disabled(&self, namespace: &str, name: &str, disabled: bool,
    ) -> anyhow::Result<serde_json::Value> {
        let _ops = self.app_ops.write().await;
        if !self.app_known(namespace, name) {
            return Err(refuse(404, format!("no app {name}")));
        }
        self.store.set_app_disabled(namespace, name, disabled, now_ms()).await?;
        if let Some(a) = self.apps.lock().get_mut(&(namespace.into(), name.into())) { a.disabled = disabled; }
        tracing::info!(app = %name, disabled, "app setting changed");
        Ok(serde_json::json!({ "app": name, "disabled": disabled }))
    }

    pub async fn load_tokens(&self) -> anyhow::Result<usize> {
        let mut allocations = self.store.allocations().await?;
        for a in &allocations {
            if let Some(id) = a.id.strip_prefix("proof:") {
                if self.store.get(id).await?.is_none_or(|j| j.state().is_terminal()) { self.store.release(&a.id).await?; }
            }
        }
        allocations = self.store.allocations().await?;
        *self.allocations.lock() = allocations.into_iter().map(|a| (a.id.clone(), a)).collect();
        *self.namespaces.lock() = self.store.namespaces().await?.into_iter().map(|n| (n.id.clone(), n)).collect();
        let all = self.store.tokens().await?;
        let n = all.len();
        *self.tokens.lock() = all.into_iter().map(|c| (c.address.clone(), c)).collect();
        Ok(n)
    }

    /// Tokens issued or revoked outside this process (`fleet-scheduler issue-admin`) take
    /// effect: the mirror is read again from the store.
    pub async fn reload_tokens(&self) {
        let _ops = self.token_ops.lock().await;
        match self.store.tokens().await {
            Ok(all) => *self.tokens.lock() = all.into_iter().map(|c| (c.address.clone(), c)).collect(),
            Err(e) => tracing::warn!("reloading tokens: {e:#}"),
        }
    }

    /// The key a public values URL carries: an HMAC of the proof id under a secret derived
    /// from the cluster token, so the URL works only for whoever GetStatus gave it to.
    pub fn public_values_key(&self, proof_id: &str) -> String {
        use hmac::Mac;
        let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(
            format!("zkm-fleet public values\0{}", self.cluster_token).as_bytes(),
        )
        .expect("HMAC takes any key length");
        mac.update(proof_id.as_bytes());
        hex::encode(&mac.finalize().into_bytes()[..16])
    }

    /// Whether `key` opens this proof's public values.
    pub fn public_values_open(&self, proof_id: &str, key: Option<&str>) -> bool {
        use subtle_eq::ConstantTimeEq;
        key.is_some_and(|k| {
            k.as_bytes().ct_eq(self.public_values_key(proof_id).as_bytes())
        })
    }

    pub fn namespace(&self, reference: &str) -> Option<Namespace> {
        self.namespaces.lock().values().find(|n| n.id == reference || n.name == reference).cloned()
    }

    pub fn namespaces(&self) -> Vec<Namespace> {
        let mut all: Vec<_> = self.namespaces.lock().values().cloned().collect();
        all.sort_by(|a, b| a.name.cmp(&b.name));
        all
    }

    pub async fn create_namespace(&self, name: &str, quota: Quota) -> anyhow::Result<Namespace> {
        quota.validate()?;
        anyhow::ensure!(!name.is_empty() && name.len() <= 63
            && name.as_bytes()[0].is_ascii_lowercase()
            && name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            && !matches!(name, "system" | "admin"), "invalid or reserved namespace name");
        let n = Namespace { quota, tag_quotas: [("scope=default".into(), Default::default())].into_iter().collect(), id: uuid::Uuid::new_v4().simple().to_string(), name: name.into(), created_ms: now_ms(), suspended_ms: None,
        };
        self.store.add_namespace(&n).await?;
        self.namespaces.lock().insert(n.id.clone(), n.clone());
        Ok(n)
    }

    pub fn tags(&self, namespace: &str) -> Vec<String> {
        let mut tags = std::collections::BTreeSet::new();
        if let Some(n) = self.namespace(namespace) { tags.extend(n.tag_quotas.keys().filter(|t|!t.is_empty()).cloned()); }
        tags.extend(self.tokens.lock().values().filter(|c|c.namespace.as_deref()==Some(namespace)).filter_map(|c|c.tag.clone()),
        );
        tags.extend(self.allocations.lock().values().filter(|a|a.namespace==namespace).filter_map(|a|a.tag.clone()),
        );
        tags.into_iter().collect()
    }
    pub fn validate_tag(tag: &str) -> anyhow::Result<()> {
        fleet_proto::models::validate_tag(tag)
    }
    pub async fn set_tag_quota(&self, namespace: &str, tag: &str, quota: Quota,
    ) -> anyhow::Result<Namespace> {
        let _ops = self.quota_ops.lock().await;
        quota.validate()?;
        if !tag.is_empty() { Self::validate_tag(tag)?; }
        let mut n = self.namespace(namespace).ok_or_else(|| refuse(404, "no such namespace".into()))?;
        self.store.set_tag_quota(&n.id,tag,&quota).await?;
        n.tag_quotas.insert(tag.into(),quota);
        self.namespaces.lock().insert(n.id.clone(),n.clone());
        self.wake.notify_one(); Ok(n)
    }
    pub fn tag_usage(&self, namespace: &str, tag: Option<&str>) -> Usage {
        self.allocations.lock().values().filter(|a| a.namespace == namespace && a.tag.as_deref() == tag)
            .fold(Usage::default(), |mut u,a| {u.gpus+=a.gpus; u.cpus+=a.cpus;u})
    }

    pub async fn suspend_namespace(&self, reference: &str, suspended: bool,
    ) -> anyhow::Result<Namespace> {
        let _ops = self.quota_ops.lock().await;
        let mut n = self.namespace(reference).ok_or_else(|| refuse(404, "no such namespace".into()))?;
        n.suspended_ms = suspended.then(now_ms);
        self.store.suspend_namespace(&n.id, n.suspended_ms).await?;
        self.namespaces.lock().insert(n.id.clone(), n.clone());
        Ok(n)
    }

    pub async fn set_quota(&self, reference: &str, quota: Quota) -> anyhow::Result<Namespace> {
        let _ops = self.quota_ops.lock().await;
        quota.validate()?;
        let mut n = self.namespace(reference).ok_or_else(|| refuse(404, "no such namespace".into()))?;
        self.store.set_quota(&n.id, &quota).await?;
        n.quota = quota;
        self.namespaces.lock().insert(n.id.clone(), n.clone());
        self.wake.notify_one();
        Ok(n)
    }

    pub fn usage(&self, namespace: &str) -> Usage {
        let mut usage = Usage::default();
        for a in self.allocations.lock().values().filter(|a| a.namespace == namespace) {
            usage.gpus += a.gpus;
            usage.cpus += a.cpus;
        }
        usage
    }

    pub fn allocations(&self, namespace: &str) -> Vec<Allocation> {
        self.allocations.lock().values().filter(|a| a.namespace == namespace).cloned().collect()
    }

    /// Decide which lower-priority jobs may be stopped to admit higher-priority work.
    /// Ray-derived capacity/candidate facts come from the adapter; namespace and quota
    /// state always come from this scheduler.
    pub fn plan_preemption(&self, request: crate::preemption::PreemptionRequest,
    ) -> anyhow::Result<crate::preemption::PreemptionPlan> {
        let namespace = self.namespace(&request.namespace)
            .ok_or_else(|| refuse(404, "no such namespace".into()))?;
        if namespace.suspended_ms.is_some() {
            return Ok(crate::preemption::PreemptionPlan::impossible());
        }
        if let Some(tag) = request.tag.as_deref().filter(|tag| !tag.is_empty()) {
            Self::validate_tag(tag).map_err(|e| refuse(400, e.to_string()))?;
        }
        let tag_quota = namespace.tag_quotas.get(request.tag.as_deref().unwrap_or(""))
            .cloned().unwrap_or_default();
        let allocations: Vec<_> = self.allocations.lock().values().cloned().collect();
        crate::preemption::plan(request, &namespace.quota, &tag_quota, &allocations)
            .map(|plan| plan.unwrap_or_else(crate::preemption::PreemptionPlan::impossible))
            .map_err(|e| refuse(400, e.to_string()))
    }

    fn fits_quota(&self, namespace: &str, tag: Option<&str>, id: &str, gpus: f64, cpus: f64,
    ) -> bool {
        let Some(n) = self.namespace(namespace) else { return false; };
        let tag_quota = n.tag_quotas.get(tag.unwrap_or("")).cloned().unwrap_or_default();
        let all = self.allocations.lock();
        if all.get(id).is_some_and(|a| {
            a.namespace == namespace && a.tag.as_deref() == tag && a.gpus == gpus && a.cpus == cpus
        }) { return true; }
        let (used_gpus, used_cpus) = all.values().filter(|a| a.namespace == namespace && a.id != id)
            .fold((0.0, 0.0), |(g,c), a| (g + a.gpus, c + a.cpus));
        let (tag_gpus, tag_cpus) = all.values().filter(|a| a.namespace == namespace && a.tag.as_deref() == tag && a.id != id)
            .fold((0.0,0.0), |(g,c),a| (g+a.gpus,c+a.cpus));
        n.quota.fits(used_gpus + gpus, used_cpus + cpus) && tag_quota.fits(tag_gpus+gpus,tag_cpus+cpus)
    }

    /// In-memory admission is atomic across proof dispatch and jobs API requests.
    fn allocate(&self, a: Allocation, restore: bool) -> anyhow::Result<()> {
        a.validate().map_err(|e| refuse(400, e.to_string()))?;
        let n = self.namespace(&a.namespace).ok_or_else(|| refuse(404, "no such namespace".into()))?;
        if let Some(tag) = &a.tag { Self::validate_tag(tag).map_err(|e|refuse(400,e.to_string()))?; }
        let tag_quota = n.tag_quotas.get(a.tag.as_deref().unwrap_or("")).cloned().unwrap_or_default();
        let mut all = self.allocations.lock();
        if let Some(old) = all.get(&a.id) {
            if old.namespace != a.namespace || old.tag != a.tag { return Err(refuse(409, "allocation belongs to another namespace or tag".into(),
                )); }
            if old.gpus == a.gpus && old.cpus == a.cpus { return Ok(()); }
        }
        if !restore {
            if !n.quota.fits(a.gpus, a.cpus) || !tag_quota.fits(a.gpus, a.cpus) { return Err(refuse(422, "job exceeds the namespace or tag GPU/CPU quota".into(),
                )); }
            let (used_gpus, used_cpus) = all.values().filter(|v| v.namespace == a.namespace && v.id != a.id)
                .fold((0.0, 0.0), |(g,c), v| (g + v.gpus, c + v.cpus));
            let (tag_gpus,tag_cpus) = all.values().filter(|v| v.namespace == a.namespace && v.tag == a.tag && v.id != a.id)
                .fold((0.0,0.0), |(g,c),v| (g+v.gpus,c+v.cpus));
            if !n.quota.fits(used_gpus + a.gpus, used_cpus + a.cpus) || !tag_quota.fits(tag_gpus+a.gpus,tag_cpus+a.cpus) {
                return Err(refuse(429, "namespace or tag GPU/CPU quota is occupied; waiting for resources".into(),
                ));
            }
        }
        all.insert(a.id.clone(), a);
        Ok(())
    }

    pub async fn reserve_job(&self, namespace: &str, tag: Option<&str>, id: &str, gpus: f64, cpus: f64, restore: bool,
    ) -> anyhow::Result<Allocation> {
        let _ops = self.quota_ops.lock().await;
        let service_id = id.strip_prefix("service:");
        anyhow::ensure!(service_id.map_or_else(|| blobs::is_safe_id(id), blobs::is_safe_id), "invalid job/app id");
        let n = self.namespace(namespace).ok_or_else(|| refuse(404, "no such namespace".into()))?;
        let prefix = if service_id.is_some() { "" } else { "job:" };
        let a = Allocation { id: format!("{prefix}{id}"), namespace: n.id, tag: tag.map(str::to_owned), gpus, cpus,
        };
        let previous = self.allocations.lock().get(&a.id).cloned();
        self.allocate(a.clone(), restore)?;
        if let Err(e) = self.store.reserve(&a).await {
            let mut all = self.allocations.lock();
            if let Some(old) = previous { all.insert(old.id.clone(), old); } else { all.remove(&a.id); }
            return Err(e);
        }
        Ok(a)
    }

    pub async fn release_job(&self, namespace: &str, id: &str) -> anyhow::Result<()> {
        let _ops = self.quota_ops.lock().await;
        let key = if id.starts_with("service:") { id.to_string() } else { format!("job:{id}") };
        anyhow::ensure!(self.allocations.lock().get(&key).is_none_or(|a| a.namespace == namespace), "allocation belongs to another namespace or tag");
        self.release_quota(&key).await
    }

    pub async fn release_allocation(&self, id: &str) -> anyhow::Result<()> {
        let _ops = self.quota_ops.lock().await;
        anyhow::ensure!(self.allocations.lock().contains_key(id), "no such allocation");
        self.release_quota(id).await
    }

    async fn release_quota(&self, id: &str) -> anyhow::Result<()> {
        self.store.release(id).await?;
        self.allocations.lock().remove(id);
        self.wake.notify_one();
        Ok(())
    }

    fn allocate_proof(&self, job: &ProofJob, node: &RegisteredProver, restore: bool) -> bool {
        self.allocate(Allocation { id: format!("proof:{}", job.id), namespace: job.namespace.clone(),
            tag: (!job.tag.is_empty()).then(|| job.tag.clone()),
            gpus: node.info.gpus as f64, cpus: node.info.cpus as f64,
            }, restore,
        ).is_ok()
    }

    fn token_active(&self, c: &Token) -> bool {
        c.revoked_ms.is_none() && match &c.namespace {
            Some(id) => self.namespace(id).is_some_and(|n| n.suspended_ms.is_none()),
            None => crate::token::allowed(Some(c), crate::token::Permission::Admin),
        }
    }
    pub fn access_record(&self, c: &Token) -> serde_json::Value {
        let mut v = serde_json::to_value(c).expect("token serialization");
        if c.apps.is_none() { v["apps"] = serde_json::json!([]); }
        if let Some(n) = c.namespace.as_ref().and_then(|id| self.namespace(id)) {
            v["namespace_name"] = serde_json::json!(n.name);
            v["quota"] = serde_json::json!(n.quota);
            v["usage"] = serde_json::json!(self.usage(&n.id));
            v["tag_quota"] = serde_json::json!(n.tag_quotas.get(c.tag.as_deref().unwrap_or("")).cloned().unwrap_or_default());
        }
        v
    }

    pub fn token_for_id(&self, id: &str) -> Option<Token> {
        self.tokens.lock().values().find(|t| t.id == id).cloned().filter(|t| self.token_active(t))
    }

    pub fn token_for_token(&self, header: Option<&str>) -> Option<Token> {
        use sha2::Digest;
        use subtle_eq::ConstantTimeEq;
        let token = header?.strip_prefix("Bearer ")?;
        if token.len() < 64 { return None; }
        let hash = hex::encode(sha2::Sha256::digest(token.as_bytes()));
        let c = self.tokens.lock().values()
            .find(|c| c.token_hash.as_ref().is_some_and(|h| h.as_bytes().ct_eq(hash.as_bytes())))
            .cloned()?;
        self.token_active(&c).then_some(c)
    }

    pub async fn namespace_job(&self, namespace: &str, tag: Option<&str>, client_id: &str,
    ) -> anyhow::Result<Option<ProofJob>> {
        self.store.namespace_job(namespace, tag, client_id).await
    }

    /// Namespaced proof ids never collide with another user's client ids.
    pub fn internal_proof_id(namespace: &str, tag: Option<&str>, client_id: &str) -> String {
        use sha2::Digest;
        let tag = tag.unwrap_or("");
        format!("p-{}", hex::encode(sha2::Sha256::digest(format!("{namespace}\0{tag}\0{client_id}"))))
    }

    pub fn public_values_key_for(&self, id: &str, issuer: &str) -> String {
        self.public_values_key(&format!("{id}\0{issuer}"))
    }

    /// Records a token. An administrator's holds `admin` alone and no namespace; any other
    /// holds some of read, submit and manage (read and submit by default) in a namespace.
    pub async fn add_user_token(&self, id: &str, address: &str, label: &str,
        apps: Option<Vec<String>>, token_hash: Option<String>, namespace: Option<String>, tag: Option<String>, permissions: Vec<String>,
    ) -> anyhow::Result<Token> {
        use crate::token::Permission;
        let permissions: Vec<String> = if permissions.is_empty() { vec!["read".into(), "submit".into()] } else { permissions };
        let admin = permissions.iter().any(|p| p == Permission::Admin.name());
        if admin {
            anyhow::ensure!(permissions.iter().all(|p| p == Permission::Admin.name()), "an admin token holds admin alone: it may do everything");
            anyhow::ensure!(namespace.is_none() && tag.is_none() && apps.is_none(), "an admin token has no namespace, tag or app grants");
            anyhow::ensure!(token_hash.is_some(), "an admin token requires an API token");
        }
        for p in &permissions {
            anyhow::ensure!(admin || Permission::ISSUABLE.iter().any(|q| q.name() == p), "unknown permission {p}; tokens may hold read, submit and manage, or admin");
        }
        let permissions: Vec<String> = if admin { vec![Permission::Admin.name().into()] } else {
            Permission::ISSUABLE.iter().map(|q| q.name()).filter(|q| permissions.iter().any(|p| p == q)).map(Into::into).collect()
        };
        let manage = permissions.iter().any(|p| p == "manage");
        let namespace = match namespace {
            Some(reference) => {
                let n = self.namespace(&reference).ok_or_else(|| anyhow::anyhow!("unknown namespace; create it first"))?;
                anyhow::ensure!(n.suspended_ms.is_none(), "namespace is suspended");
                Some(n.id)
            }
            None => None,
        };
        if let Some(tag) = &tag {
            anyhow::ensure!(namespace.is_some(), "tag requires namespace");
            Self::validate_tag(tag)?;
        }
        anyhow::ensure!(admin || namespace.is_some(), "a token needs a namespace, unless it is an administrator's");
        anyhow::ensure!(namespace.as_deref() != Some(crate::store::SYSTEM_NAMESPACE), "system namespace is reserved");
        anyhow::ensure!(admin || apps.is_some() || manage, "a token without the manage permission requires explicit app grants");
        anyhow::ensure!(!manage || (tag.is_some() && token_hash.is_some()), "manage tokens require a tag and API token");
        if let Some(hash) = &token_hash {
            anyhow::ensure!(hash.len() == 64 && hash.bytes().all(|b| b.is_ascii_hexdigit()), "invalid token hash");
        }
        for app in apps.iter().flatten() {
            anyhow::ensure!(valid_app_name(app), "invalid app grant {app:?}: an app or batch name");
        }
        let address = normalize_address(address);
        anyhow::ensure!(
            address.len() == 40 && address.bytes().all(|b| b.is_ascii_hexdigit()),
            "not an address"
        );
        let id = id.to_ascii_lowercase();
        anyhow::ensure!(id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit()), "token id must be 32 hex characters");
        let c = Token {
            id,
            address: address.clone(),
            label: label.into(),
            created_ms: now_ms(),
            revoked_ms: None,
            apps: apps.map(Into::into),
            namespace,
            tag,
            permissions,
            token_hash: token_hash.map(|s| s.to_ascii_lowercase()),
        };
        let _ops = self.token_ops.lock().await;
        self.store.add_token(&c).await?;
        self.tokens.lock().insert(address, c.clone());
        Ok(c)
    }

    pub async fn revoke_token(&self, id: &str) -> anyhow::Result<bool> {
        let _ops = self.token_ops.lock().await;
        let at = now_ms();
        let found = self.store.revoke_token(id, at).await?;
        if let Some(c) = self.tokens.lock().values_mut().find(|t| t.id == id) {
            c.revoked_ms = Some(at);
        }
        Ok(found)
    }

    pub fn tokens(&self) -> Vec<Token> {
        let mut v: Vec<Token> = self.tokens.lock().values().cloned().collect();
        v.sort_by_key(|c| c.created_ms);
        v
    }

    /// Stops new proof placements on a machine while active executions finish.
    pub fn set_machine_drain(&self, machine: &str, drain: bool) {
        let mut inner = self.inner.lock();
        if drain {
            inner.draining_machines.insert(machine.to_string());
        } else {
            inner.draining_machines.remove(machine);
        }
        drop(inner);
        self.wake.notify_one();
    }

    /// Reloads queued and running jobs after a restart; running ones go back to the queue.
    pub async fn recover(&self) -> anyhow::Result<usize> {
        let mut jobs = self.store.incomplete().await?;
        for j in &mut jobs {
            if j.state() == ProofJobState::Running {
                // `node` stays as a hint: that node may still be proving the job and
                // hands the result over when asked again (see `assign`).
                j.state = ProofJobState::Queued as i32;
                self.store.upsert(j).await?;
            }
        }
        let n = jobs.len();
        let mut inner = self.inner.lock();
        for j in jobs {
            let (tx, _) = watch::channel(0);
            inner.jobs.insert(
                j.id.clone(),
                ProofJobEntry {
                    job: j,
                    changed: tx,
                },
            );
        }
        drop(inner);
        self.wake.notify_one();
        Ok(n)
    }

    pub fn queued_for(&self, namespace: &str, app: &str) -> usize {
        self.inner
            .lock()
            .jobs
            .values()
            .filter(|e| e.job.namespace == namespace && e.job.app == app && !e.job.state().is_terminal())
            .count()
    }

    /// Records a new job. A known id is left as it is (the SDK may resubmit).
    pub async fn submit(&self, job: ProofJob) -> anyhow::Result<bool> {
        job.validate()?;
        anyhow::ensure!(self.namespace(&job.namespace).is_some(), "job namespace does not exist");
        let _ops = self.app_ops.read().await;
        if !job.state().is_terminal() && job.inputs.is_none() && !self.blobs.has_elf(&job.elf_id) {
            anyhow::bail!("program {} is not stored (its app was deleted): send the ELF itself again", job.elf_id);
        }
        {
            let inner = self.inner.lock();
            if inner.jobs.contains_key(&job.id) {
                return Ok(false);
            }
        }
        if self.store.get(&job.id).await?.is_some() {
            return Ok(false);
        }
        self.store.upsert(&job).await?;
        let (tx, _) = watch::channel(0);
        self.inner
            .lock()
            .jobs
            .insert(job.id.clone(), ProofJobEntry { job, changed: tx });
        self.wake.notify_one();
        Ok(true)
    }

    pub async fn job(&self, id: &str) -> anyhow::Result<Option<ProofJob>> {
        if let Some(e) = self.inner.lock().jobs.get(id) {
            return Ok(Some(e.job.clone()));
        }
        self.store.get(id).await
    }

    /// The registered apps with their job counts, and the node each is anchored on now.
    pub async fn apps(&self) -> anyhow::Result<Vec<(App, Option<String>)>> {
        let anchors = self.inner.lock().anchors.clone();
        Ok(self.store.apps().await?.into_iter().map(|a| {
            let node = anchors.get(&(a.namespace.clone(), a.name.clone())).cloned();
            (a, node)
        }).collect())
    }

    /// Deletes an app, its terminal job records and their inputs and outputs, and its
    /// anchor. Removes the ELF only when no other app or job uses it. Refused while any of
    /// its jobs is queued or running.
    pub async fn delete_app(&self, namespace: &str, name: &str,
    ) -> anyhow::Result<serde_json::Value> {
        let _ops = self.app_ops.write().await;
        let apps = self.store.apps().await?;
        let app = apps
            .iter()
            .find(|a| a.namespace == namespace && a.name == name)
            .ok_or_else(|| refuse(404, format!("no app {name}")))?;
        // Its dedicated worker would keep the node's GPUs with no app left to serve.
        if self.app_worker_owned(namespace, name) {
            return Err(refuse(409, format!("app {name} still has a dedicated worker; disable it first: fleetctl app disable {name}")));
        }
        let live = self
            .inner
            .lock()
            .jobs
            .values()
            .filter(|e| e.job.namespace == namespace && e.job.app == name && !e.job.state().is_terminal())
            .count() as u64;
        if app.active > 0 || live > 0 {
            return Err(refuse(409, format!(
                "{name} has {} job(s) queued or running: delete it when they are done",
                app.active.max(live)
            )));
        }
        let elf_id = app.elf_id.clone();
        let ids = self.store.delete_app(namespace, name).await?;
        self.apps.lock().remove(&(namespace.to_string(), name.to_string()));
        {
            let mut inner = self.inner.lock();
            for id in &ids {
                inner.jobs.remove(id);
            }
            if let Some(node) = inner.anchors.remove(&(namespace.to_string(), name.to_string())) {
                if inner.reserved.get(&node) == Some(&placement::app_key(namespace, name)) {
                    inner.reserved.remove(&node);
                }
            }
        }
        let mut files_failed = 0;
        for id in &ids {
            if self.blobs.remove_job(id).is_err() {
                files_failed += 1;
            }
        }
        let shared = apps.iter().any(|a| a.elf_id == elf_id && (a.name != name || a.namespace != namespace))
            || self.inner.lock().jobs.values().any(|e| e.job.elf_id == elf_id);
        let elf_removed = !shared && self.blobs.remove_elf(&elf_id).unwrap_or(false);
        tracing::info!(app = %name, jobs = ids.len(), elf_removed, "app deleted");
        Ok(serde_json::json!({
            "app": name, "jobs_deleted": ids.len(), "elf_removed": elf_removed,
            "files_not_removed": files_failed,
        }))
    }

    /// The newest jobs (the store has every one) that the filter selects.
    pub async fn recent_jobs(&self, f: &crate::store::ProofJobFilter, limit: usize,
    ) -> anyhow::Result<Vec<ProofJob>> {
        self.store.recent(f, limit).await
    }

    /// Waits up to `timeout` for the job to reach a terminal state.
    pub async fn wait_terminal(&self, id: &str, timeout: Duration) {
        let rx = {
            let inner = self.inner.lock();
            match inner.jobs.get(id) {
                Some(e) if !e.job.state().is_terminal() => e.changed.subscribe(),
                _ => return,
            }
        };
        let mut rx = rx;
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            match tokio::time::timeout_at(deadline, rx.changed()).await {
                Ok(Ok(())) => {
                    if self
                        .inner
                        .lock()
                        .jobs
                        .get(id)
                        .map_or(true, |e| e.job.state().is_terminal())
                    {
                        return;
                    }
                }
                _ => return,
            }
        }
    }

    fn update_job(&self, id: &str, f: impl FnOnce(&mut ProofJob)) -> Option<ProofJob> {
        let mut inner = self.inner.lock();
        let e = inner.jobs.get_mut(id)?;
        f(&mut e.job);
        e.changed.send_modify(|v| *v += 1);
        Some(e.job.clone())
    }

    // ---------------------------------------------------------------- nodes

    pub fn register(&self, mut info: NodeInfo) -> anyhow::Result<String> {
        anyhow::ensure!(!info.name.is_empty(), "node name is empty");
        anyhow::ensure!(info.control_version == 1, "worker must support heartbeat control version 1; upgrade the node binary");
        let app_fields = [!info.app_namespace.is_empty(), !info.app.is_empty(), !info.app_elf_id.is_empty(),
        ];
        anyhow::ensure!(app_fields.iter().all(|v| *v) || app_fields.iter().all(|v| !*v),
            "dedicated proof app identity requires namespace, app and elf_id together");
        if !info.app_namespace.is_empty() {
            let namespace = self.namespace(&info.app_namespace).ok_or_else(|| anyhow::anyhow!("proof node names an unknown namespace"))?;
            info.app_namespace = namespace.id;
            anyhow::ensure!(valid_app_name(&info.app), "proof node names an invalid app");
            info.app_elf_id.make_ascii_lowercase();
            anyhow::ensure!(valid_elf_id(&info.app_elf_id), "proof node app elf_id must be 64 hex digits");
            // An app not registered yet has no jobs; once it is, only its program is proven.
            if let Some(app) = self.app(&info.app_namespace, &info.app) {
                anyhow::ensure!(app.elf_id == info.app_elf_id,
                    "proof node proves program {}, but app {} is program {}", info.app_elf_id, app.name, app.elf_id);
            }
        }
        let id = uuid::Uuid::new_v4().to_string();
        let mut inner = self.inner.lock();
        if let Some(previous) = inner.nodes.values().find(|n| n.info.name == info.name) {
            let same_app = previous.info.app_namespace == info.app_namespace
                && previous.info.app == info.app
                && previous.info.app_elf_id == info.app_elf_id;
            anyhow::ensure!(
                same_app || previous.busy.is_none(),
                "cannot change the proof app binding of a busy node"
            );
            if !same_app {
                inner.anchors.retain(|_, node| node != &info.name);
                inner.reserved.remove(&info.name);
            }
        }
        inner.nodes.retain(|_, n| n.info.name != info.name);
        // A replacement session keeps the original execution and its reservation.
        // The worker can resume the same lease without waiting for a scheduler restart.
        let busy = inner.jobs.values()
            .find(|e| {
                e.job.state() == ProofJobState::Running && e.job.node.as_deref() == Some(info.name.as_str())
            })
            .map(|e| e.job.id.clone());
        tracing::info!(node = %info.name, backend = %info.backend, gpus = info.gpus, "node registered");
        inner.nodes.insert(
            id.clone(),
            RegisteredProver {
                id: id.clone(),
                info,
                last_heartbeat: Instant::now(),
                ready: false,
                busy,
                last_released: None,
                cached: HashSet::new(),
            },
        );
        drop(inner);
        self.wake.notify_one();
        Ok(id)
    }

    pub fn heartbeat(
        &self,
        id: &str,
        ready: bool,
        cached: Vec<String>,
        busy: bool,
        running_job: &str,
    ) -> bool {
        let mut inner = self.inner.lock();
        let Some(n) = inner.nodes.get_mut(id) else {
            return false;
        };
        let mut wake = ready && !n.ready;
        n.last_heartbeat = Instant::now();
        n.ready = ready;
        n.cached = cached.into_iter().collect();
        // The node's own word fills in proofs this scheduler did not dispatch: one from
        // before a restart, or the one behind a "node busy" answer.
        let ours = n.busy.as_ref().is_some_and(|b| !b.starts_with(REPORTED));
        if !ours {
            if busy && n.last_released.as_deref() != Some(running_job) {
                n.busy = Some(reported(running_job));
            } else if n.busy.take().is_some() {
                wake = true;
            }
        }
        drop(inner);
        if wake {
            self.wake.notify_one();
        }
        true
    }

    pub fn deregister(&self, id: &str) {
        if let Some(n) = self.inner.lock().nodes.remove(id) {
            tracing::info!(node = %n.info.name, "node deregistered");
        }
    }

    /// Drops expired sessions. Uncertain executions retain their resource reservations
    /// until the original worker reconnects and reports their outcome.
    pub fn sweep(&self) {
        let stale = Duration::from_secs(self.cfg.node_stale_secs);
        let mut inner = self.inner.lock();
        let before = inner.nodes.len();
        inner.nodes.retain(|_, n| {
            let keep = n.last_heartbeat.elapsed() < stale;
            if !keep {
                tracing::warn!(node = %n.info.name, "node stale, dropped");
            }
            keep
        });
        if inner.nodes.len() != before {
            drop(inner);
            self.wake.notify_one();
        }
    }

    pub fn status(&self) -> FleetStatus {
        let inner = self.inner.lock();
        let (mut queued, mut running) = (0, 0);
        for e in inner.jobs.values() {
            match e.job.state() {
                ProofJobState::Queued => queued += 1,
                ProofJobState::Running => running += 1,
                _ => {}
            }
        }
        let mut nodes: Vec<NodeStatus> = inner
            .nodes
            .values()
            .map(|n| NodeStatus {
                id: n.id.clone(),
                name: n.info.name.clone(),
                backend: n.info.backend.clone(),
                gpus: n.info.gpus,
                machine: n.info.machine.clone(),
                zkm_version: n.info.zkm_version.clone(),
                image: n.info.image.clone(),
                app: (!n.info.app_namespace.is_empty()).then(|| placement::app_key(&n.info.app_namespace, &n.info.app)),
                ready: n.ready,
                busy: n.busy.clone(),
                reserved_for: inner.reserved.get(&n.info.name).cloned(),
                draining: inner.draining_machines.contains(&n.info.machine),
                cached_elfs: n.cached.len() as u64,
                heartbeat_age_ms: n.last_heartbeat.elapsed().as_millis() as u64,
            })
            .collect();
        nodes.sort_by(|a, b| a.name.cmp(&b.name));
        let anchors = inner
            .anchors
            .iter()
            .map(|((namespace, app), n)| AppAnchor {
                namespace: namespace.clone(),
                app: app.clone(),
                node: n.clone(),
            })
            .collect();
        FleetStatus {
            queued,
            running,
            nodes,
            anchors,
        }
    }

    // ------------------------------------------------------------- dispatch

    pub async fn dispatch_loop(self: Arc<Self>) {
        let mut sweep =
            tokio::time::interval(Duration::from_secs(self.cfg.heartbeat_secs.max(1) as u64));
        loop {
            for (job_id, node_id) in self.assign() {
                let me = self.clone();
                tokio::spawn(async move { me.prepare(job_id, node_id).await });
            }
            tokio::select! {
                _ = self.wake.notified() => {}
                _ = sweep.tick() => {
                    self.sweep();
                    self.reload_tokens().await;
                }
            }
        }
    }

    /// No new placements: the process is stopping. Jobs left queued are recovered on restart.
    pub fn stop_dispatching(&self) {
        self.stopping.store(true, Ordering::SeqCst);
    }

    /// One placement pass: app priority, then oldest first.
    fn assign(&self) -> Vec<(String, String)> {
        if self.stopping.load(Ordering::SeqCst) {
            return Vec::new();
        }
        let policies: HashMap<(String, String), AppPolicy> = self.apps.lock().iter()
            .map(|(k, a)| (k.clone(), AppPolicy::of(a))).collect();
        let mut inner = self.inner.lock();
        let mut queued: Vec<(std::cmp::Reverse<i32>, u64, String)> = inner
            .jobs
            .values()
            .filter(|e| e.job.state() == ProofJobState::Queued)
            .map(|e| {
                (
                    std::cmp::Reverse(e.job.priority),
                    e.job.created_ms,
                    e.job.id.clone(),
                )
            })
            .collect();
        queued.sort();
        let mut out = Vec::new();
        for (_, _, job_id) in queued {
            let job = inner.jobs[&job_id].job.clone();
            let Some(policy) = policies.get(&(job.namespace.clone(), job.app.clone())) else {
                continue;
            };
            // A recovered job goes back to the node it was running on (or that says it is
            // proving it), which joins the proof if it is still going. Nodes re-register
            // within a heartbeat or two.
            let mine = reported(&job_id);
            let hint = job.node.clone().or_else(|| {
                inner
                    .nodes
                    .values()
                    .find(|n| n.busy.as_deref() == Some(mine.as_str()))
                    .map(|n| n.info.name.clone())
            });
            if let Some(hint) = hint.as_ref().filter(|h| {
                inner.nodes.values().any(|n| {
                    &n.info.name == *h && n.info.app_namespace == job.namespace
                        && n.info.app == job.app
                        && n.info.app_elf_id.eq_ignore_ascii_case(&job.elf_id)
                })
            }) {
                let at = inner
                    .nodes
                    .values()
                    .find(|n| &n.info.name == hint)
                    .map(|n| (n.id.clone(), n.ready, n.busy.clone()));
                let free_or_ours =
                    |b: &Option<String>| b.is_none() || b.as_deref() == Some(mine.as_str());
                match at {
                    Some((node_id, true, b)) if free_or_ours(&b) => {
                        if !self.allocate_proof(&job, &inner.nodes[&node_id], job.started_ms.is_some(),
                        ) { continue; }
                        tracing::info!(job = %job_id, node = %hint, "resuming on the node that was running it");
                        self.commit(&mut inner, policy, &job_id, &node_id);
                        out.push((job_id, node_id));
                        continue;
                    }
                    Some((_, true, _)) => {}
                    _ if self.started.elapsed() < Duration::from_secs(self.cfg.node_stale_secs) => {
                        continue
                    }
                    _ => {}
                }
                // A disconnected namespaced proof may still be running. Its reservation
                // belongs to that execution, so only the original prover may resolve it.
                if job.started_ms.is_some()
                    && self.allocations.lock().contains_key(&format!("proof:{}", job.id)) {
                    continue;
                }
                inner.jobs.get_mut(&job_id).unwrap().job.node = None;
            }
            let ids: Vec<String> = inner.nodes.keys().cloned().collect();
            let views: Vec<PlacementCandidate> = ids
                .iter()
                .map(|id| {
                    let n = &inner.nodes[id];
                    PlacementCandidate {
                        name: n.info.name.clone(),
                        backend: n.info.backend.clone(),
                        gpus: n.info.gpus,
                        zkm_version: n.info.zkm_version.clone(),
                        ready: n.ready && n.info.app_namespace == job.namespace
                            && n.info.app == job.app
                            && n.info.app_elf_id.eq_ignore_ascii_case(&job.elf_id)
                            && !inner.draining_machines.contains(&n.info.machine)
                            && self.fits_quota(&job.namespace, (!job.tag.is_empty()).then_some(job.tag.as_str()), &format!("proof:{}", job.id), n.info.gpus as f64, n.info.cpus as f64,
                            ),
                        busy: n.busy.is_some(),
                        reserved_for: inner.reserved.get(&n.info.name).cloned(),
                        has_elf: n.cached.contains(&job.elf_id),
                    }
                })
                .collect();
            let anchor = inner.anchors.get(&(job.namespace.clone(), job.app.clone())).cloned();
            match placement::choose(policy, &job.failed_on, anchor.as_deref(), &views) {
                PlacementDecision::Selected(i) => {
                    let node_id = ids[i].clone();
                    if !self.allocate_proof(&job, &inner.nodes[&node_id], false) { continue; }
                    self.commit(&mut inner, policy, &job_id, &node_id);
                    out.push((job_id, node_id));
                }
                PlacementDecision::WaitForAnchor | PlacementDecision::NoNode => {}
            }
        }
        out
    }

    /// Marks `job_id` running on `node_id`, moving the app's anchor there if it has one.
    fn commit(
        &self,
        inner: &mut SchedulerState,
        app: &AppPolicy,
        job_id: &str,
        node_id: &str,
    ) {
        let name = inner.nodes[node_id].info.name.clone();
        let job = &inner.jobs[job_id].job;
        let anchor_key = (job.namespace.clone(), job.app.clone());
        let anchor = inner.anchors.get(&anchor_key).cloned();
        if app.anchor != AnchorPolicy::None && anchor.as_deref() != Some(name.as_str()) {
            if let Some(old) = anchor {
                if inner.reserved.get(&old) == Some(&app.key) {
                    inner.reserved.remove(&old);
                }
            }
            inner.anchors.insert(anchor_key, name.clone());
            if app.anchor == AnchorPolicy::Exclusive {
                inner.reserved.insert(name.clone(), app.key.clone());
            }
            tracing::info!(app = %app.key, node = %name, "anchored");
        }
        inner.nodes.get_mut(node_id).unwrap().busy = Some(job_id.to_string());
        let e = inner.jobs.get_mut(job_id).unwrap();
        e.job.state = ProofJobState::Running as i32;
        e.job.node = Some(name);
        if e.job.started_ms.is_none() {
            e.job.attempts += 1;
            e.job.started_ms = Some(now_ms());
        }
        e.changed.send_modify(|v| *v += 1);
    }

    fn release(&self, node_id: &str, job_id: &str, elf_id: Option<&str>) {
        let mut inner = self.inner.lock();
        if let Some(n) = inner.nodes.get_mut(node_id) {
            if n.busy.as_deref() == Some(job_id) || n.busy.as_deref() == Some(reported(job_id).as_str()) {
                n.last_released = Some(job_id.to_string());
                n.busy = None;
            }
            if let Some(e) = elf_id {
                n.cached.insert(e.to_string());
            }
        }
        drop(inner);
        self.wake.notify_one();
    }

    /// Persist before offering work in a heartbeat response. No outbound node RPC.
    async fn prepare(self: Arc<Self>, job_id: String, _node_id: String) {
        loop {
            let _ops = self.completion_ops.lock().await;
            let Some(job) = self.inner.lock().jobs.get(&job_id).map(|e| e.job.clone()) else { return; };
            if job.state() != ProofJobState::Running { return; }
            let allocation = self.allocations.lock().get(&format!("proof:{job_id}")).cloned();
            let durable = async {
                if let Some(a) = allocation { self.store.reserve(&a).await?; }
                self.store.upsert(&job).await
            }.await;
            match durable {
                Ok(()) => { self.inner.lock().deliverable.insert(job_id); return; }
                Err(e) => tracing::error!(job = %job_id, "persisting assignment: {e:#}"),
            }
            if self.stopping.load(Ordering::SeqCst) { return; }
            drop(_ops);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    fn assignment(job: &ProofJob) -> Assignment {
        Assignment { inputs: job.inputs.clone(), kind: job.kind as i32, job_id: job.id.clone(), elf_id: job.elf_id.clone(),
            lease: format!("{}-{}", job.attempts, job.started_ms.unwrap_or(0)),
        }
    }

    /// Callers authorize namespace/tag/app access before fetching bytes.
    pub async fn proof_output(&self, job: &ProofJob, name: &'static str,
    ) -> Result<Vec<u8>, tonic::Status> {
        if job.state() != ProofJobState::Succeeded { return Err(tonic::Status::failed_precondition("proof has not succeeded",
            )); }
        if let Some(outputs) = &job.outputs {
            let reference = match name {
                blobs::PROOF => outputs.proof.clone(),
                blobs::PUBLIC_VALUES => outputs.public_values.clone(),
                _ => return Err(tonic::Status::not_found("unknown proof output")),
            }.ok_or_else(|| tonic::Status::internal("output reference missing"))?;
            let output_root = job.inputs.as_ref().ok_or_else(|| tonic::Status::internal("output root missing"))?.output_root.clone();
            self.proof_fetch.fetch(&outputs.machine, fleet_proto::fleet::v1::ProofFetch {
                job_id: job.id.clone(), lease: Self::assignment(job).lease, elf_id: job.elf_id.clone(),
                output_root, name: name.into(), reference: Some(reference), ..Default::default()
            },
                ).await
        } else {
            self.blobs.sdk_output(job, name).await.map_err(|e| {
                if e.downcast_ref::<std::io::Error>().is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) {
                    tonic::Status::not_found("proof output is not available on the control plane")
                } else { tonic::Status::internal(e.to_string()) }
            })
        }
    }

    /// Fence input access and completion reports by session, worker and execution attempt.
    pub async fn assigned_job(&self, node_id: &str, a: &Assignment) -> Result<ProofJob, tonic::Status> {
        let name = self.inner.lock().nodes.get(node_id).map(|n| n.info.name.clone())
            .ok_or_else(|| tonic::Status::not_found("node session expired; register again"))?;
        let j = self.job(&a.job_id).await.map_err(|e| tonic::Status::internal(e.to_string()))?
            .ok_or_else(|| tonic::Status::not_found("job missing"))?;
        if j.node.as_deref() != Some(&name) || j.started_ms.is_none()
            || Self::assignment(&j).job_id != a.job_id
            || Self::assignment(&j).elf_id != a.elf_id || Self::assignment(&j).lease != a.lease
            || (j.inputs.is_some() && Self::assignment(&j) != *a) {
            return Err(tonic::Status::failed_precondition("assignment superseded"));
        }
        Ok(j)
    }

    pub async fn fetch_inputs(&self, node_id: &str, a: &Assignment, with_elf: bool,
    ) -> Result<ProveRequest, tonic::Status> {
        let j = self.assigned_job(node_id, a).await?;
        if j.state().is_terminal() { return Err(tonic::Status::failed_precondition("job already finished")); }
        if j.inputs.is_some() { return Err(tonic::Status::failed_precondition("inputs are in app-owned storage; read the assignment references directly",
            )); }
        self.build_request(&j, with_elf).map_err(|e| tonic::Status::internal(e.to_string()))
    }

    /// Nodes initiate output publication; the scheduler never connects to a node.
    pub async fn publish_output(&self, node_id: &str, a: &Assignment, result: fleet_proto::fleet::v1::ProveResponse,
    ) -> Result<(), tonic::Status> {
        let _ops = self.completion_ops.lock().await;
        let j = self.assigned_job(node_id, a).await?;
        if j.inputs.is_some() { return Err(tonic::Status::failed_precondition("outputs stay in app-owned storage; report references in completion metadata",
            )); }
        if !matches!(j.state(), ProofJobState::Running | ProofJobState::Succeeded) {
            return Err(tonic::Status::failed_precondition("job cannot publish output",
            ));
        }
        if result.proof_json.is_empty() || result.proof_json.len() > 64 << 20 || result.public_values.len() > 64 << 20 {
            return Err(tonic::Status::invalid_argument("invalid proof output size"));
        }
        let blobs = self.blobs.clone();
        tokio::task::spawn_blocking(move || blobs.publish_output(&j, &result)).await
            .map_err(|e| tonic::Status::internal(e.to_string()))?
            .map_err(|e| tonic::Status::internal(e.to_string()))
    }

    pub async fn exchange(&self, r: HeartbeatRequest) -> Result<HeartbeatResponse, tonic::Status> {
        // Completion acknowledgments are sent only after durable state and quota cleanup.
        if !self.heartbeat(&r.node_id, r.ready, r.cached_elf_ids, r.busy, &r.running_job,
        ) {
            return Ok(HeartbeatResponse { known: false, ..Default::default() });
        }
        let mut completed_lease = String::new();
        if let Some(c) = r.completion {
            let _ops = self.completion_ops.lock().await;
            let a = c.assignment.as_ref().ok_or_else(|| tonic::Status::invalid_argument("completion assignment missing"))?;
            match self.assigned_job(&r.node_id, a).await {
                Ok(j) => self.complete(&r.node_id, j, c.clone()).await?,
                // A delayed result from an older attempt is acknowledged and discarded.
                Err(e) if e.code() == tonic::Code::FailedPrecondition || e.code() == tonic::Code::NotFound => {}
                Err(e) => return Err(e),
            }
            completed_lease = a.lease.clone();
        }
        let assignment = {
            let inner = self.inner.lock();
            inner.nodes.get(&r.node_id).filter(|n| n.ready).and_then(|n| {
                n.busy.as_ref().filter(|id| inner.deliverable.contains(*id))
                    .and_then(|id| inner.jobs.get(id))
                    .filter(|e| {
                            e.job.state() == ProofJobState::Running && e.job.node.as_ref() == Some(&n.info.name)
                        })
                    .map(|e| Self::assignment(&e.job))
            })
        };
        Ok(HeartbeatResponse { known: true, assignment, completed_lease,
        })
    }

    async fn complete(&self, node_id: &str, mut j: ProofJob, c: Completion,
    ) -> Result<(), tonic::Status> {
        let err = |e: anyhow::Error| tonic::Status::internal(e.to_string());
        if !j.state().is_terminal() {
            if c.error_code == 0 {
                // The authenticated worker reports success only after publishing its
                // output. Scheduling must not depend on a control-host storage mount.
                if let Some(inputs) = &j.inputs {
                    let mut outputs = c.outputs.clone().ok_or_else(|| {
                        tonic::Status::invalid_argument("completion output references missing")
                    })?;
                    fleet_proto::artifacts::validate_outputs(&outputs).map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
                    let prefix = format!("{}/{}/{}/", inputs.output_root.trim_end_matches('/'), j.id, Self::assignment(&j).lease);
                    if outputs.proof.as_ref().unwrap().uri != format!("{prefix}proof.json") || outputs.public_values.as_ref().unwrap().uri != format!("{prefix}public_values.bin") {
                        return Err(tonic::Status::failed_precondition("output does not belong to this execution lease",
                        ));
                    }
                    outputs.machine = self.inner.lock().nodes.get(node_id).map(|n| n.info.machine.clone())
                        .filter(|m| !m.is_empty()).ok_or_else(|| {
                            tonic::Status::failed_precondition("output machine identity missing")
                        })?;
                    j.outputs = Some(outputs);
                }
                j.state = ProofJobState::Succeeded as i32;
                j.error.clear();
                j.cycles = c.cycles;
                j.prove_ms = c.prove_ms;
                j.finished_ms = Some(now_ms());
            } else {
                let failure = classify(&tonic::Status::new(tonic::Code::from_i32(c.error_code), c.error.clone(),
                ));
                if matches!(failure, ProofAttemptFailure::NotStarted | ProofAttemptFailure::Lost | ProofAttemptFailure::Busy) {
                    // No transfer or timeout may release units for an uncertain execution.
                    // Retry the same lease on the original node after its next heartbeat.
                    return Ok(());
                }
                j.error = c.error;
                if let Some(name) = j.node.clone() { j.failed_on.push(name); }
                if j.attempts >= self.cfg.max_attempts || j.failed_on.len() >= MAX_NODES_TRIED {
                    j.state = ProofJobState::Failed as i32;
                    j.fail_status = Some(StageStatus::ProveError as i32);
                    j.finished_ms = Some(now_ms());
                } else {
                    j.state = ProofJobState::Queued as i32;
                    j.node = None;
                    j.started_ms = None;
                }
            }
            self.store.upsert(&j).await.map_err(err)?;
        }
        self.release_quota(&format!("proof:{}", j.id)).await.map_err(err)?;
        self.update_job(&j.id, |stored| *stored = j.clone());
        self.inner.lock().deliverable.remove(&j.id);
        self.release(node_id, &j.id, (j.state() == ProofJobState::Succeeded).then_some(j.elf_id.as_str()),
        );
        self.wake.notify_one();
        Ok(())
    }

    fn build_request(&self, job: &ProofJob, with_elf: bool) -> anyhow::Result<ProveRequest> {
        let stdin: Vec<Vec<u8>> = bincode::deserialize(&self.blobs.get(&job.id, blobs::STDIN)?)?;
        let receipts: Vec<Vec<u8>> =
            bincode::deserialize(&self.blobs.get(&job.id, blobs::RECEIPTS)?)?;
        Ok(ProveRequest {
            job_id: job.id.clone(),
            elf_id: job.elf_id.clone(),
            elf: if with_elf {
                self.blobs.elf(&job.elf_id)?
            } else {
                Vec::new()
            },
            stdin_buffer: stdin,
            receipts,
            kind: job.kind as i32,
        })
    }


}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_tags_require_a_single_key_value_pair() {
        for tag in ["scope=default", "team=research", "network=Ethereum-mainnet", "build=1.2_3",
        ] {
            assert!(Scheduler::validate_tag(tag).is_ok(), "{tag}");
        }
        for tag in ["default", "", "=default", "scope=", "scope=a=b", "scope=a,b", "team=two words", "1key=value",
        ] {
            assert!(Scheduler::validate_tag(tag).is_err(), "{tag}");
        }
        assert!(Scheduler::validate_tag(&format!("key={}", "a".repeat(64))).is_err());
    }

    #[test]
    fn failures_are_classified_by_what_they_say_about_the_node() {
        let c = |code, msg: &str| classify(&tonic::Status::new(code, msg));
        assert_eq!(
            c(tonic::Code::Unavailable, "tcp connect error"),
            ProofAttemptFailure::NotStarted
        );
        assert_eq!(c(tonic::Code::Unknown, "transport error"), ProofAttemptFailure::Lost);
        assert_eq!(c(tonic::Code::Cancelled, "connection reset"), ProofAttemptFailure::Lost);
        assert_eq!(
            c(tonic::Code::ResourceExhausted, "node busy"),
            ProofAttemptFailure::Busy
        );
        assert_eq!(c(tonic::Code::Internal, "gpu fell over"), ProofAttemptFailure::Prover);
        assert_eq!(c(tonic::Code::Unknown, "panicked"), ProofAttemptFailure::Prover);
        let gpu_gone = "Internal error: io error: CUDA RPC `prove_core_stateless` failed: error sending request for url (http://127.0.0.1:30101/twirp/api.ProverService/ProveCoreStateless)";
        assert_eq!(c(tonic::Code::Internal, gpu_gone), ProofAttemptFailure::NotStarted);
    }
}

/// Constant-time comparison for keys and tokens.
mod subtle_eq {
    pub trait ConstantTimeEq {
        fn ct_eq(&self, other: &Self) -> bool;
    }
    impl ConstantTimeEq for [u8] {
        fn ct_eq(&self, other: &[u8]) -> bool {
            self.len() == other.len() && self.iter().zip(other).fold(0u8, |acc, (a, b)| acc | (a ^ b)) == 0
        }
    }
}
