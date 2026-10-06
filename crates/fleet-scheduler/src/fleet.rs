//! Scheduler core: jobs in memory (persisted to the store), the node registry,
//! anchoring, and the dispatcher that runs jobs on nodes.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use tokio::sync::{watch, Notify};
use tonic::codegen::InterceptedService;
use tonic::transport::{Channel, Endpoint};

use fleet_proto::fleet::v1::{node_service_client::NodeServiceClient, NodeInfo, ProveRequest};
use fleet_proto::stage::v1::Status as StageStatus;

use crate::blobs::{self, Blobs};
use crate::config::{Anchor, Class, Config};
use crate::placement::{self, Choice, NodeView};
use crate::store::{normalize_address, Credential, Job, JobState, Store};
use crate::token;

pub type NodeClient = NodeServiceClient<InterceptedService<Channel, token::Attach>>;

/// How an attempt failed, which decides whether it counts and what it says about the node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Failure {
    /// The node could not be reached: the job never started. Not counted as an attempt.
    NotStarted,
    /// The connection broke or timed out mid-proof: the node or its machine is suspect.
    Lost,
    /// The node answered with an error.
    Prover,
    /// The node is proving something this scheduler did not send it. Not counted.
    Busy,
}

/// `Node::busy` for a proof the node reported itself rather than one dispatched here.
const REPORTED: char = '~';

fn reported(job_id: &str) -> String {
    format!("{REPORTED}{job_id}")
}

/// Nodes tried per job, whatever the reason, before it fails.
const MAX_NODES_TRIED: usize = 8;

fn classify(status: &tonic::Status) -> Failure {
    let m = status.message();
    match status.code() {
        tonic::Code::Unavailable => Failure::NotStarted,
        tonic::Code::ResourceExhausted => Failure::Busy,
        // The node could not reach its GPU server (restarting, removed): nothing about the job.
        _ if m.contains("CUDA RPC")
            && (m.contains("error sending request") || m.contains("onnection refused")) =>
        {
            Failure::NotStarted
        }
        tonic::Code::Unknown | tonic::Code::Cancelled | tonic::Code::Aborted
            if m.contains("transport") || m.contains("connection") || m.contains("broken pipe") =>
        {
            Failure::Lost
        }
        _ => Failure::Prover,
    }
}

/// A proof app's name: its tenant and the start of its program's id; `(rejected)` for
/// requests refused before either was known.
pub fn app_name(tenant: &str, elf_id: &str) -> String {
    if tenant.is_empty() && elf_id.is_empty() {
        return "(rejected)".into();
    }
    format!("{}/{}", if tenant.is_empty() { "?" } else { tenant }, &elf_id[..8.min(elf_id.len())])
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

struct JobEntry {
    job: Job,
    /// Bumped on every change; `GetStatus` long-polls on it.
    changed: watch::Sender<u64>,
}

pub struct Node {
    pub id: String,
    pub info: NodeInfo,
    client: NodeClient,
    pub last_heartbeat: Instant,
    pub ready: bool,
    /// The job running there: dispatched by this scheduler, or `~job` as the node reported it.
    pub busy: Option<String>,
    /// The last job released here; a heartbeat sent before the release still names it.
    last_released: Option<String>,
    pub cached: HashSet<String>,
}

#[derive(Default)]
struct Inner {
    jobs: HashMap<String, JobEntry>,
    nodes: HashMap<String, Node>,
    /// (tenant, elf_id) -> node name.
    anchors: HashMap<(String, String), String>,
    /// node name -> tenant, for exclusive anchors.
    reserved: HashMap<String, String>,
    /// Node names taking no new jobs.
    draining: HashSet<String>,
}

pub struct Fleet {
    pub cfg: Config,
    pub store: Arc<dyn Store>,
    pub blobs: Blobs,
    cluster_token: String,
    inner: Mutex<Inner>,
    /// address -> credential, mirrored from the store.
    creds: Mutex<HashMap<String, Credential>>,
    wake: Notify,
    started: Instant,
    stopping: AtomicBool,
}

#[derive(Debug, serde::Serialize)]
pub struct NodeStatus {
    pub id: String,
    pub name: String,
    pub backend: String,
    pub gpus: u32,
    pub machine: String,
    pub zkm_version: String,
    pub image: String,
    pub ready: bool,
    pub busy: Option<String>,
    pub reserved_for: Option<String>,
    pub draining: bool,
    pub cached_elfs: usize,
    pub heartbeat_age_ms: u64,
}

#[derive(Debug, serde::Serialize)]
pub struct FleetStatus {
    pub queued: usize,
    pub running: usize,
    pub nodes: Vec<NodeStatus>,
    pub anchors: Vec<(String, String, String)>,
}

impl Fleet {
    pub fn new(
        cfg: Config,
        store: Arc<dyn Store>,
        blobs: Blobs,
        cluster_token: String,
    ) -> Arc<Self> {
        Arc::new(Self {
            cfg,
            store,
            blobs,
            cluster_token,
            inner: Mutex::new(Inner::default()),
            creds: Mutex::new(HashMap::new()),
            wake: Notify::new(),
            started: Instant::now(),
            stopping: AtomicBool::new(false),
        })
    }

    pub async fn load_credentials(&self) -> anyhow::Result<usize> {
        let all = self.store.credentials().await?;
        let n = all.len();
        *self.creds.lock() = all.into_iter().map(|c| (c.address.clone(), c)).collect();
        Ok(n)
    }

    /// Tenant of a request signer: a live credential, else an address listed in the config.
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

    /// Whether `key` opens this proof's public values. Proofs finished before this
    /// scheduler started were handed out without a key and stay readable without one.
    pub async fn public_values_open(&self, proof_id: &str, key: Option<&str>) -> bool {
        if let Some(k) = key {
            use subtle_eq::ConstantTimeEq;
            return k.as_bytes().ct_eq(self.public_values_key(proof_id).as_bytes());
        }
        let started = now_ms().saturating_sub(self.started.elapsed().as_millis() as u64);
        matches!(self.job(proof_id).await, Ok(Some(j)) if j.finished_ms.is_some_and(|f| f < started))
    }

    pub fn tenant_for_signer(&self, address: &str) -> Option<String> {
        let a = normalize_address(address);
        if let Some(c) = self.creds.lock().get(&a) {
            return if c.revoked_ms.is_none() {
                Some(c.tenant.clone())
            } else {
                None
            };
        }
        self.cfg.tenant_for_address(&a).map(|t| t.name.clone())
    }

    pub async fn add_credential(
        &self,
        tenant: &str,
        address: &str,
        label: &str,
    ) -> anyhow::Result<Credential> {
        anyhow::ensure!(self.cfg.tenant(tenant).is_some(), "unknown tenant {tenant}");
        let address = normalize_address(address);
        anyhow::ensure!(
            address.len() == 40 && address.bytes().all(|b| b.is_ascii_hexdigit()),
            "not an address"
        );
        let c = Credential {
            address: address.clone(),
            tenant: tenant.into(),
            label: label.into(),
            created_ms: now_ms(),
            revoked_ms: None,
        };
        self.store.add_credential(&c).await?;
        self.creds.lock().insert(address, c.clone());
        Ok(c)
    }

    pub async fn revoke_credential(&self, address: &str) -> anyhow::Result<bool> {
        let at = now_ms();
        let found = self.store.revoke_credential(address, at).await?;
        if let Some(c) = self.creds.lock().get_mut(&normalize_address(address)) {
            c.revoked_ms = Some(at);
        }
        Ok(found)
    }

    pub fn credentials(&self) -> Vec<Credential> {
        let mut v: Vec<Credential> = self.creds.lock().values().cloned().collect();
        v.sort_by_key(|c| c.created_ms);
        v
    }

    /// Stops placing new jobs on a node (it finishes its current one); `false` undoes it.
    pub fn set_drain(&self, name: &str, drain: bool) -> bool {
        let mut inner = self.inner.lock();
        let found = inner.nodes.values().any(|n| n.info.name == name);
        if drain {
            inner.draining.insert(name.to_string());
        } else {
            inner.draining.remove(name);
        }
        drop(inner);
        self.wake.notify_one();
        found
    }

    /// Reloads queued and running jobs after a restart; running ones go back to the queue.
    pub async fn recover(&self) -> anyhow::Result<usize> {
        let mut jobs = self.store.incomplete().await?;
        for j in &mut jobs {
            if j.state == JobState::Running {
                // `node` stays as a hint: that node may still be proving the job and
                // hands the result over when asked again (see `assign`).
                j.state = JobState::Queued;
                j.attempts = j.attempts.saturating_sub(1);
                self.store.upsert(j).await?;
            }
        }
        let n = jobs.len();
        let mut inner = self.inner.lock();
        for j in jobs {
            let (tx, _) = watch::channel(0);
            inner.jobs.insert(
                j.id.clone(),
                JobEntry {
                    job: j,
                    changed: tx,
                },
            );
        }
        drop(inner);
        self.wake.notify_one();
        Ok(n)
    }

    pub fn queued_for(&self, tenant: &str) -> usize {
        self.inner
            .lock()
            .jobs
            .values()
            .filter(|e| e.job.tenant == tenant && !e.job.state.is_terminal())
            .count()
    }

    /// Records a new job. A known id is left as it is (the SDK may resubmit).
    pub async fn submit(&self, job: Job) -> anyhow::Result<bool> {
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
            .insert(job.id.clone(), JobEntry { job, changed: tx });
        self.wake.notify_one();
        Ok(true)
    }

    pub async fn job(&self, id: &str) -> anyhow::Result<Option<Job>> {
        if let Some(e) = self.inner.lock().jobs.get(id) {
            return Ok(Some(e.job.clone()));
        }
        self.store.get(id).await
    }

    /// The proof apps (a tenant's program) with their job counts, and the node each is
    /// anchored on now.
    pub async fn apps(&self) -> anyhow::Result<Vec<serde_json::Value>> {
        let anchors = self.inner.lock().anchors.clone();
        Ok(self
            .store
            .apps()
            .await?
            .into_iter()
            .map(|a| {
                let node = anchors.get(&(a.tenant.clone(), a.elf_id.clone())).cloned();
                let mut v = serde_json::to_value(&a).unwrap_or_default();
                v["name"] = serde_json::json!(app_name(&a.tenant, &a.elf_id));
                v["anchored_on"] = serde_json::json!(node);
                v
            })
            .collect())
    }

    /// The newest jobs (the store has every one), of one tenant or all.
    pub async fn recent_jobs(&self, tenant: Option<&str>, limit: usize) -> anyhow::Result<Vec<Job>> {
        self.store.recent(tenant, limit).await
    }

    /// Waits up to `timeout` for the job to reach a terminal state.
    pub async fn wait_terminal(&self, id: &str, timeout: Duration) {
        let rx = {
            let inner = self.inner.lock();
            match inner.jobs.get(id) {
                Some(e) if !e.job.state.is_terminal() => e.changed.subscribe(),
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
                        .map_or(true, |e| e.job.state.is_terminal())
                    {
                        return;
                    }
                }
                _ => return,
            }
        }
    }

    fn update_job(&self, id: &str, f: impl FnOnce(&mut Job)) -> Option<Job> {
        let mut inner = self.inner.lock();
        let e = inner.jobs.get_mut(id)?;
        f(&mut e.job);
        e.changed.send_modify(|v| *v += 1);
        Some(e.job.clone())
    }

    async fn persist(&self, job: Option<Job>) {
        if let Some(j) = job {
            if let Err(e) = self.store.upsert(&j).await {
                tracing::error!(job = %j.id, "persisting job failed: {e:#}");
            }
        }
    }

    // ---------------------------------------------------------------- nodes

    pub fn register(&self, info: NodeInfo) -> anyhow::Result<String> {
        anyhow::ensure!(!info.name.is_empty(), "node name is empty");
        let endpoint = Endpoint::from_shared(info.endpoint.clone())?
            .connect_timeout(Duration::from_secs(5))
            .tcp_keepalive(Some(Duration::from_secs(30)));
        let channel = endpoint.connect_lazy();
        let client =
            NodeServiceClient::with_interceptor(channel, token::Attach::new(&self.cluster_token)?)
                .max_decoding_message_size(1 << 30)
                .max_encoding_message_size(1 << 30);
        let id = uuid::Uuid::new_v4().to_string();
        let mut inner = self.inner.lock();
        let fresh = Duration::from_secs(2 * self.cfg.heartbeat_secs.max(1) as u64);
        if let Some(other) = inner.nodes.values().find(|n| {
            n.info.name == info.name
                && n.info.endpoint != info.endpoint
                && n.last_heartbeat.elapsed() < fresh
        }) {
            tracing::warn!(node = %info.name, live = %other.info.endpoint, new = %info.endpoint,
                "two live processes claim this node name; the newer registration replaces the older");
        }
        inner.nodes.retain(|_, n| n.info.name != info.name);
        tracing::info!(node = %info.name, endpoint = %info.endpoint, backend = %info.backend, gpus = info.gpus, "node registered");
        inner.nodes.insert(
            id.clone(),
            Node {
                id: id.clone(),
                info,
                client,
                last_heartbeat: Instant::now(),
                ready: false,
                busy: None,
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

    /// Drops nodes that stopped heartbeating; their running jobs fail over when the RPC errors.
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
            match e.job.state {
                JobState::Queued => queued += 1,
                JobState::Running => running += 1,
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
                ready: n.ready,
                busy: n.busy.clone(),
                reserved_for: inner.reserved.get(&n.info.name).cloned(),
                draining: inner.draining.contains(&n.info.name),
                cached_elfs: n.cached.len(),
                heartbeat_age_ms: n.last_heartbeat.elapsed().as_millis() as u64,
            })
            .collect();
        nodes.sort_by(|a, b| a.name.cmp(&b.name));
        let anchors = inner
            .anchors
            .iter()
            .map(|((t, e), n)| (t.clone(), e.clone(), n.clone()))
            .collect();
        FleetStatus {
            queued,
            running,
            nodes,
            anchors,
        }
    }

    // ------------------------------------------------------------- dispatch

    pub async fn run_dispatcher(self: Arc<Self>) {
        let mut sweep =
            tokio::time::interval(Duration::from_secs(self.cfg.heartbeat_secs.max(1) as u64));
        loop {
            for (job_id, node_id) in self.assign() {
                let me = self.clone();
                tokio::spawn(async move { me.run(job_id, node_id).await });
            }
            tokio::select! {
                _ = self.wake.notified() => {}
                _ = sweep.tick() => self.sweep(),
            }
        }
    }

    /// No new placements: the process is stopping. Jobs left queued are recovered on restart.
    pub fn stop_dispatching(&self) {
        self.stopping.store(true, Ordering::SeqCst);
    }

    /// One placement pass: latency jobs first, then oldest first.
    fn assign(&self) -> Vec<(String, String)> {
        if self.stopping.load(Ordering::SeqCst) {
            return Vec::new();
        }
        let mut inner = self.inner.lock();
        let mut queued: Vec<(u8, u64, String)> = inner
            .jobs
            .values()
            .filter(|e| e.job.state == JobState::Queued)
            .map(|e| {
                let class = self
                    .cfg
                    .tenant(&e.job.tenant)
                    .map(|t| t.class)
                    .unwrap_or_default();
                (
                    if class == Class::Latency { 0 } else { 1 },
                    e.job.created_ms,
                    e.job.id.clone(),
                )
            })
            .collect();
        queued.sort();
        let mut out = Vec::new();
        for (_, _, job_id) in queued {
            let job = inner.jobs[&job_id].job.clone();
            let Some(tenant) = self.cfg.tenant(&job.tenant) else {
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
            if let Some(hint) = hint.as_ref().filter(|h| placement::allowed(tenant, h)) {
                let at = inner
                    .nodes
                    .values()
                    .find(|n| &n.info.name == hint)
                    .map(|n| (n.id.clone(), n.ready, n.busy.clone()));
                let free_or_ours =
                    |b: &Option<String>| b.is_none() || b.as_deref() == Some(mine.as_str());
                match at {
                    Some((node_id, true, b)) if free_or_ours(&b) => {
                        tracing::info!(job = %job_id, node = %hint, "resuming on the node that was running it");
                        self.commit(&mut inner, tenant, &job_id, &node_id);
                        out.push((job_id, node_id));
                        continue;
                    }
                    Some((_, true, _)) => {}
                    _ if self.started.elapsed() < Duration::from_secs(self.cfg.node_stale_secs) => {
                        continue
                    }
                    _ => {}
                }
                inner.jobs.get_mut(&job_id).unwrap().job.node = None;
            }
            let ids: Vec<String> = inner.nodes.keys().cloned().collect();
            let views: Vec<NodeView> = ids
                .iter()
                .map(|id| {
                    let n = &inner.nodes[id];
                    NodeView {
                        name: n.info.name.clone(),
                        backend: n.info.backend.clone(),
                        gpus: n.info.gpus,
                        zkm_version: n.info.zkm_version.clone(),
                        ready: n.ready && !inner.draining.contains(&n.info.name),
                        busy: n.busy.is_some(),
                        reserved_for: inner.reserved.get(&n.info.name).cloned(),
                        has_elf: n.cached.contains(&job.elf_id),
                    }
                })
                .collect();
            let anchor_key = (job.tenant.clone(), job.elf_id.clone());
            let anchor = inner.anchors.get(&anchor_key).cloned();
            match placement::choose(tenant, &job.failed_on, anchor.as_deref(), &views) {
                Choice::Run(i) => {
                    let node_id = ids[i].clone();
                    self.commit(&mut inner, tenant, &job_id, &node_id);
                    out.push((job_id, node_id));
                }
                Choice::WaitForAnchor | Choice::NoNode => {}
            }
        }
        out
    }

    /// Marks `job_id` running on `node_id`, moving the tenant's anchor there if it has one.
    fn commit(
        &self,
        inner: &mut Inner,
        tenant: &crate::config::TenantConfig,
        job_id: &str,
        node_id: &str,
    ) {
        let name = inner.nodes[node_id].info.name.clone();
        let elf_id = inner.jobs[job_id].job.elf_id.clone();
        let anchor_key = (tenant.name.clone(), elf_id.clone());
        let anchor = inner.anchors.get(&anchor_key).cloned();
        if tenant.anchor != Anchor::None && anchor.as_deref() != Some(name.as_str()) {
            if let Some(old) = anchor {
                if inner.reserved.get(&old) == Some(&tenant.name) {
                    inner.reserved.remove(&old);
                }
            }
            inner.anchors.insert(anchor_key, name.clone());
            if tenant.anchor == Anchor::Exclusive {
                inner.reserved.insert(name.clone(), tenant.name.clone());
            }
            tracing::info!(tenant = %tenant.name, elf = %&elf_id[..12.min(elf_id.len())], node = %name, "anchored");
        }
        inner.nodes.get_mut(node_id).unwrap().busy = Some(job_id.to_string());
        let e = inner.jobs.get_mut(job_id).unwrap();
        e.job.state = JobState::Running;
        e.job.node = Some(name);
        e.job.attempts += 1;
        e.job.started_ms = Some(now_ms());
        e.changed.send_modify(|v| *v += 1);
    }

    fn release(&self, node_id: &str, elf_id: Option<&str>) {
        let mut inner = self.inner.lock();
        if let Some(n) = inner.nodes.get_mut(node_id) {
            n.last_released = n.busy.take();
            if let Some(e) = elf_id {
                n.cached.insert(e.to_string());
            }
        }
        drop(inner);
        self.wake.notify_one();
    }

    async fn run(self: Arc<Self>, job_id: String, node_id: String) {
        let Some(job) = self.inner.lock().jobs.get(&job_id).map(|e| e.job.clone()) else {
            return;
        };
        self.persist(Some(job.clone())).await;
        let found = self
            .inner
            .lock()
            .nodes
            .get(&node_id)
            .map(|n| (n.client.clone(), n.cached.contains(&job.elf_id)));
        let Some((client, has_elf)) = found else {
            self.fail_attempt(
                &job_id,
                &node_id,
                "node left before the job started",
                Failure::NotStarted,
            )
            .await;
            return;
        };
        let request = match self.build_request(&job, !has_elf) {
            Ok(r) => r,
            Err(e) => {
                self.release(&node_id, None);
                let j = self.update_job(&job_id, |j| {
                    j.state = JobState::Failed(StageStatus::InternalError);
                    j.error = format!("reading inputs: {e:#}");
                    j.finished_ms = Some(now_ms());
                });
                self.persist(j).await;
                return;
            }
        };
        let timeout = Duration::from_secs(self.cfg.job_timeout_secs);
        let mut result = tokio::time::timeout(timeout, client.clone().prove(request.clone())).await;
        if let Ok(Err(status)) = &result {
            if status.code() == tonic::Code::FailedPrecondition
                && status.message().contains("elf_missing")
            {
                let mut retry = request.clone();
                match self.blobs.elf(&job.elf_id) {
                    Ok(elf) => {
                        retry.elf = elf;
                        result = tokio::time::timeout(timeout, client.clone().prove(retry)).await;
                    }
                    Err(e) => tracing::error!(job = %job_id, "reading elf: {e:#}"),
                }
            }
        }
        match result {
            Ok(Ok(resp)) => {
                let resp = resp.into_inner();
                let written = self
                    .blobs
                    .put(&job_id, blobs::PROOF, &resp.proof_json)
                    .and_then(|_| {
                        self.blobs
                            .put(&job_id, blobs::PUBLIC_VALUES, &resp.public_values)
                    });
                self.release(&node_id, Some(&job.elf_id));
                let j = self.update_job(&job_id, |j| {
                    match &written {
                        Ok(()) => {
                            j.state = JobState::Succeeded;
                            j.cycles = resp.cycles;
                            j.prove_ms = resp.prove_ms;
                        }
                        Err(e) => {
                            j.state = JobState::Failed(StageStatus::InternalError);
                            j.error = format!("storing the proof: {e:#}");
                        }
                    }
                    j.finished_ms = Some(now_ms());
                });
                if let Some(j) = &j {
                    tracing::info!(job = %j.id, tenant = %j.tenant, node = ?j.node, cycles = j.cycles, prove_ms = j.prove_ms,
                        wall_ms = resp.wall_ms, queue_ms = j.started_ms.unwrap_or(0).saturating_sub(j.created_ms), "job done");
                }
                self.persist(j).await;
            }
            Ok(Err(status)) => {
                let msg = format!("{}: {}", status.code(), status.message());
                self.fail_attempt(&job_id, &node_id, &msg, classify(&status))
                    .await;
            }
            Err(_) => {
                self.fail_attempt(&job_id, &node_id, "timed out", Failure::Lost)
                    .await
            }
        }
    }

    fn build_request(&self, job: &Job, with_elf: bool) -> anyhow::Result<ProveRequest> {
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

    /// Requeues the job on another node, or fails it after `max_attempts` counted attempts
    /// (or `MAX_NODES_TRIED` nodes). A node that could not be reached, or dropped the
    /// connection, is marked not ready together with the other nodes on its machine (the
    /// machine is likely gone); the next heartbeat of a live node marks it ready again.
    async fn fail_attempt(&self, job_id: &str, node_id: &str, msg: &str, failure: Failure) {
        if failure == Failure::Busy {
            // Busy with a proof from before a restart: keep the node out until its heartbeat
            // says it is free, and place the job again as if it had not been tried.
            if let Some(n) = self.inner.lock().nodes.get_mut(node_id) {
                n.busy = Some(reported(""));
            }
            let j = self.update_job(job_id, |j| {
                j.attempts = j.attempts.saturating_sub(1);
                j.state = JobState::Queued;
                j.node = None;
            });
            tracing::info!(job = job_id, "node was busy; placing the job again");
            self.persist(j).await;
            self.wake.notify_one();
            return;
        }
        let node_name = {
            let mut inner = self.inner.lock();
            let info = inner
                .nodes
                .get(node_id)
                .map(|n| (n.info.name.clone(), n.info.machine.clone()));
            if failure != Failure::Prover {
                if let Some((_, machine)) = &info {
                    for n in inner.nodes.values_mut() {
                        if n.info.machine == *machine || n.id == node_id {
                            n.ready = false;
                        }
                    }
                }
            }
            info.map(|(name, _)| name)
        };
        self.release(node_id, None);
        let max = self.cfg.max_attempts;
        let j = self.update_job(job_id, |j| {
            // A node that never started the job says nothing about the job: placement
            // waits for its machine's heartbeats instead (it was marked not ready above).
            if failure == Failure::NotStarted {
                j.attempts = j.attempts.saturating_sub(1);
            } else if let Some(n) = node_name.clone().or_else(|| j.node.clone()) {
                j.failed_on.push(n);
            }
            j.error = msg.to_string();
            if j.attempts >= max || j.failed_on.len() >= MAX_NODES_TRIED {
                j.state = JobState::Failed(StageStatus::ProveError);
                j.finished_ms = Some(now_ms());
            } else {
                j.state = JobState::Queued;
                j.node = None;
            }
        });
        if let Some(j) = &j {
            tracing::warn!(job = %j.id, attempt = j.attempts, ?failure, error = %msg, retrying = !j.state.is_terminal(), "job attempt failed");
        }
        self.persist(j).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failures_are_classified_by_what_they_say_about_the_node() {
        let c = |code, msg: &str| classify(&tonic::Status::new(code, msg));
        assert_eq!(
            c(tonic::Code::Unavailable, "tcp connect error"),
            Failure::NotStarted
        );
        assert_eq!(c(tonic::Code::Unknown, "transport error"), Failure::Lost);
        assert_eq!(c(tonic::Code::Cancelled, "connection reset"), Failure::Lost);
        assert_eq!(
            c(tonic::Code::ResourceExhausted, "node busy"),
            Failure::Busy
        );
        assert_eq!(c(tonic::Code::Internal, "gpu fell over"), Failure::Prover);
        assert_eq!(c(tonic::Code::Unknown, "panicked"), Failure::Prover);
        let gpu_gone = "Internal error: io error: CUDA RPC `prove_core_stateless` failed: error sending request for url (http://127.0.0.1:30101/twirp/api.ProverService/ProveCoreStateless)";
        assert_eq!(c(tonic::Code::Internal, gpu_gone), Failure::NotStarted);
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
