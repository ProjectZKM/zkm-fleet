//! Batch policy and durable records live in the control plane. Workers execute typed
//! commands with their own Ray instance and initiate every control connection.
use crate::{
    blobs::is_safe_id,
    fleet::Scheduler,
    preemption::{PreemptionCandidate, NodeCapacity, PreemptionRequest},
    store::Token,
    token,
};
use axum::{
    body::to_bytes,
    extract::{Request, State},
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Json, Router,
};
use fleet_proto::fleet::v1::{
    worker_service_server::WorkerService, NodeCommand, NodeOperation, WorkerHeartbeatRequest,
    WorkerHeartbeatResponse,
};
use parking_lot::Mutex;
use serde::Serialize;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}
fn text<'a>(v: &'a Value, key: &str) -> &'a str {
    v[key].as_str().unwrap_or("")
}
fn terminal(state: &str) -> bool {
    matches!(state, "succeeded" | "failed" | "cancelled" | "lost")
}
fn fail(code: u16, message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(crate::fleet::RequestRejection {
        status: code,
        message: message.into(),
    })
}
fn response_error(e: anyhow::Error) -> Response {
    if e.downcast_ref::<serde_json::Error>().is_some() {
        return (StatusCode::BAD_REQUEST, "invalid JSON request").into_response();
    }
    if let Some(e) = e.downcast_ref::<crate::fleet::RequestRejection>() {
        (StatusCode::from_u16(e.status).unwrap(), e.message.clone()).into_response()
    } else {
        tracing::error!(error=%e, "batch control operation failed");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "batch control operation failed",
        )
            .into_response()
    }
}

use fleet_proto::model::v1::{WorkloadTemplate, Job, CommandRecord, NodeReport, ScaleStatus, AppDeployment};

struct NodeSession {
    session: String,
    seen: f64,
    report: NodeReport,
    draining: bool,
}
#[derive(Default)]
struct WorkloadState {
    apps: BTreeMap<String, WorkloadTemplate>,
    jobs: BTreeMap<String, Job>,
    commands: BTreeMap<String, CommandRecord>,
    workers: BTreeMap<String, NodeSession>,
    services: BTreeMap<String, AppDeployment>,
}

pub struct WorkloadController {
    fleet: Arc<Scheduler>,
    cluster: String,
    dir: PathBuf,
    inner: Mutex<WorkloadState>,
    ops: tokio::sync::Mutex<()>,
    notify: tokio::sync::Notify,
}
pub(crate) fn integrated_worker(service: &Value) -> bool {
    text(service, "kind") == "ziren-gpu"
        && service["worker_command"].as_array().is_some_and(|command| !command.is_empty())
}

/// The proof apps an enabled integrated worker feeds: (namespace, app).
fn worker_owned(inner: &WorkloadState) -> HashSet<(String, String)> {
    inner.services.values()
        .filter(|s| integrated_worker(&s.service) && !s.disabled)
        .map(|s| (text(&s.service, "namespace").to_owned(), text(&s.service, "app").to_owned()))
        .collect()
}

impl WorkloadController {
    pub fn new(fleet: Arc<Scheduler>, cluster: String) -> anyhow::Result<Self> {
        let dir = fleet.cfg.data_dir.join("workloads");
        for name in ["apps", "jobs", "commands", "artifacts", "services"] {
            fs::create_dir_all(dir.join(name))?;
        }
        let mut inner = WorkloadState::default();
        for v in load::<WorkloadTemplate>(&dir.join("apps"))? {
            inner.apps.insert(v.name.clone(), v);
        }
        for v in load::<Job>(&dir.join("jobs"))? {
            inner.jobs.insert(v.id.clone(), v);
        }
        for v in load::<CommandRecord>(&dir.join("commands"))? {
            inner.commands.insert(v.id.clone(), v);
        }
        for v in load::<AppDeployment>(&dir.join("services"))? {
            inner
                .services
                .insert(format!("{}-{}", v.worker, text(&v.service, "name")), v);
        }
        fleet.set_worker_owned_apps(worker_owned(&inner));
        Ok(Self {
            fleet,
            cluster,
            dir,
            inner: Mutex::new(inner),
            ops: tokio::sync::Mutex::new(()),
            notify: tokio::sync::Notify::new(),
        })
    }
    pub(crate) fn fleet(&self) -> &Arc<Scheduler> {
        &self.fleet
    }
    fn refresh_worker_owned_apps(&self) {
        self.update_worker_owned_apps(&self.inner.lock());
    }
    fn update_worker_owned_apps(&self, inner: &WorkloadState) {
        self.fleet.set_worker_owned_apps(worker_owned(inner));
    }

    /// Mirrors a prover's state onto its proof app. A prover whose app is not registered has
    /// nothing to mirror: that must not fail the heartbeat or scale pass carrying it, or every
    /// later node result is rejected with it.
    pub(super) async fn sync_app_record(&self, service: &Value, disabled: bool) -> anyhow::Result<()> {
        let (namespace, app) = (text(service, "namespace"), text(service, "app"));
        if !self.fleet.app_known(namespace, app) {
            if text(service, "kind").starts_with("ziren-") {
                tracing::warn!(service = text(service, "name"), app, "prover's proof app is not registered");
            }
            return Ok(());
        }
        self.fleet.set_app_disabled(namespace, app, disabled).await.map(drop)
    }

    /// Drops a service record, its allocation already released. Nothing else removes one, and a
    /// stale record can restart its service or claim its app's worker slot.
    fn forget_service(&self, key: &str) -> anyhow::Result<()> {
        self.inner.lock().services.remove(key);
        match std::fs::remove_file(self.dir.join("services").join(format!("{key}.json"))) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e.into()),
            _ => {}
        }
        tracing::info!(service = key, "service record removed: its node no longer configures it");
        Ok(())
    }

    /// The node reports every service its configuration has, so one it stopped reporting was
    /// removed from it. Its record goes once nothing about it is still open.
    fn forget_unreported_services(&self, worker: &str, reported: &[String]) -> anyhow::Result<()> {
        let candidates: Vec<(String, String)> = {
            let i = self.inner.lock();
            i.services.iter()
                .filter(|(_, intent)| {
                    let name = text(&intent.service, "name");
                    intent.worker == worker && intent.disabled
                        && !intent.scale.as_ref().is_some_and(ScaleStatus::pending)
                        && !reported.iter().any(|r| r == name)
                        && !i.commands.values().any(|c| !c.done && c.worker == worker
                            && c.operation == NodeOperation::SetService as i32
                            && text(&c.parameters["service"], "name") == name)
                })
                .map(|(key, intent)| (key.clone(), text(&intent.service, "namespace").to_owned()))
                .collect()
        };
        for (key, namespace) in candidates {
            if !self.fleet.allocations(&namespace).iter().any(|a| a.id == format!("service:{key}")) {
                self.forget_service(&key)?;
            }
        }
        Ok(())
    }

    /// An app-level node message's address for `app` in `namespace`. A node's configuration may name the namespace by id
    /// or by name, so both go along; the node finds the services that run the app.
    pub(super) fn app_message(&self, namespace: &str, app: &str) -> Value {
        let name = self.fleet.namespace(namespace).map(|n| n.name).unwrap_or_default();
        json!({"app":{"namespace":namespace,"namespace_name":name,"name":app}})
    }

    /// Sends an app-level change (`{"remove":true}` or `{"set":{..}}`) for `message`'s app or
    /// batch to `workers`. A command waits in the scheduler while its node is offline.
    pub(super) fn tell_nodes<'a>(&self, workers: impl IntoIterator<Item = &'a String>, mut message: Value, change: Value) -> anyhow::Result<Vec<String>> {
        for (key, value) in change.as_object().into_iter().flatten() {
            message[key] = value.clone();
        }
        workers.into_iter().map(|worker| self.command(worker, NodeOperation::App, message.clone())).collect()
    }

    /// Tells every node that `app` was deleted from `namespace`; each removes the services it
    /// configures for it.
    pub(crate) fn remove_app(&self, namespace: &str, app: &str) -> anyhow::Result<()> {
        let workers: Vec<String> = self.inner.lock().workers.keys().cloned().collect();
        self.tell_nodes(&workers, self.app_message(namespace, app), json!({"remove":true}))?;
        Ok(())
    }

    fn check_worker_owner(&self, inner: &WorkloadState, worker: &str, service: &Value,
    ) -> anyhow::Result<()> {
        if !integrated_worker(service) { return Ok(()); }
        let namespace = text(service, "namespace");
        let allocations = self.fleet.allocations(namespace);
        let key = format!("{}-{}", worker, text(service, "name"));
        let owner = inner.services.iter().find(|(other_key, intent)| {
            *other_key != &key && integrated_worker(&intent.service)
                && text(&intent.service, "namespace") == namespace
                && text(&intent.service, "app") == text(service, "app")
                && (!intent.disabled || allocations.iter().any(|a| a.id == format!("service:{other_key}")))
        });
        if owner.is_some() {
            return Err(fail(409, "this app already has an integrated worker; clean up its owner before moving it",
            ));
        }
        Ok(())
    }

    fn save(&self, kind: &str, id: &str, value: &impl Serialize) -> anyhow::Result<()> {
        atomic(
            &self.dir.join(kind).join(format!("{id}.json")),
            &serde_json::to_vec(value)?,
        )
    }
    fn command(
        &self,
        worker: &str,
        operation: NodeOperation,
        parameters: Value,
    ) -> anyhow::Result<String> {
        let mut inner = self.inner.lock();
        if let Some(c) = inner.commands.values().find(|c| {
            (!c.done || operation == NodeOperation::LaunchJob)
                && c.worker == worker
                && c.operation == operation as i32
                && c.parameters == parameters
        }) {
            return Ok(c.id.clone());
        }
        if operation == NodeOperation::SetService {
            if parameters["disabled"].as_bool() == Some(false) {
                self.check_worker_owner(&inner, worker, &parameters["service"])?;
            }
            if inner.services.get(&format!("{}-{}", worker, text(&parameters["service"], "name")))
                .and_then(|s| s.scale.as_ref()).is_some_and(|s|
                    s.pending() && parameters["scale_id"].as_str() != Some(s.id())) {
                return Err(fail(409, "app scaling is already pending"));
            }
            let same_service = |c: &CommandRecord| {
                !c.done
                    && c.worker == worker
                    && c.operation == operation as i32
                    && text(&c.parameters["service"], "name") == text(&parameters["service"], "name")
            };
            // An operation still waiting for capacity was never sent to the node, so a newer one
            // replaces it. Without this an app that cannot be placed could not be disabled or scaled.
            let waiting: Vec<_> = inner.commands.values().filter(|c| same_service(c) && !c.admitted).map(|c| c.id.clone()).collect();
            for id in waiting {
                if let Some(c) = inner.commands.get_mut(&id) {
                    c.done = true;
                    c.error = Some("replaced by a newer service operation before it was sent to the node".into());
                    self.save("commands", &id, &*c)?;
                }
            }
            if inner.commands.values().any(same_service) {
                return Err(fail(409, "a service operation is already pending"));
            }
            let intent = AppDeployment {
                worker: worker.into(),
                service: parameters["service"].clone().into(),
                disabled: parameters["disabled"].as_bool().unwrap_or(true),
                scale: inner.services.get(&format!("{}-{}", worker, text(&parameters["service"], "name")))
                    .and_then(|s| {
                        s.scale.as_ref().filter(|_| {
                                parameters["scale_id"].is_string() || parameters["scale_cleanup"].as_bool() == Some(true)
                            }).cloned()
                    }),
            };
            let key = format!("{}-{}", worker, text(&intent.service, "name"));
            self.save("services", &key, &intent)?;
            inner.services.insert(key, intent);
            self.update_worker_owned_apps(&inner);
        }
        let c = CommandRecord {
            id: uuid::Uuid::new_v4().to_string(),
            worker: worker.into(),
            operation: operation as i32,
            parameters: parameters.into(),
            done: false,
            result: Value::Null.into(),
            error: None,
            admitted: operation != NodeOperation::SetService,
        };
        self.save("commands", &c.id, &c)?;
        let id = c.id.clone();
        inner.commands.insert(id.clone(), c);
        self.notify.notify_waiters();
        Ok(id)
    }
    pub fn worker_status(&self) -> Vec<Value> {
        let proofs = self.fleet.status().nodes;
        let inner = self.inner.lock();
        inner.workers.iter().map(|(id, w)| {
            let busy = inner.jobs.values()
                .filter(|r| r.worker.as_deref() == Some(id) && !terminal(r.state()))
                .map(|r| r.id.clone())
                .chain(proofs.iter().filter(|n| n.machine == w.report.machine).filter_map(|n| n.busy.clone()),
                    )
                .collect::<Vec<_>>();
            let apps = w.report.services.iter().map(|s| {
                text(s, "app")
            }).collect::<Vec<_>>();
            json!({"id":id,"name":id,"backend":"ray-local",
                "gpus":w.report.total_gpus,"cpus":w.report.total_cpus,"machine":w.report.machine,
                "free_gpus":w.report.free_gpus,"free_cpus":w.report.free_cpus,
                "ready":w.report.ready && now()-w.seen<30.0 && !w.draining,"draining":w.draining,
                "busy":(!busy.is_empty()).then(||busy.join(", ")),"apps":apps,
                "batches":w.report.batches.keys().collect::<Vec<_>>(),
                "heartbeat_age_ms":((now()-w.seen)*1000.0) as u64})
        }).collect()
    }
    pub fn drain_worker(&self, id: &str, draining: bool) -> bool {
        let mut inner = self.inner.lock();
        let Some(worker) = inner.workers.get_mut(id) else { return false; };
        worker.draining = draining;
        self.fleet.set_machine_drain(&worker.report.machine, draining);
        true
    }
    fn visible(
        &self,
        namespace: &str,
        tag: Option<&str>,
        principal: Option<&Token>,
        q: &BTreeMap<String, String>,
    ) -> bool {
        if let Some(c) = principal {
            if c.namespace.as_deref() != Some(namespace) || c.tag.as_deref() != tag {
                return false;
            }
        }
        if let Some(n) = q.get("namespace") {
            if self.fleet.namespace(n).is_none_or(|n| n.id != namespace) {
                return false;
            }
        }
        if let Some(t) = q.get("tag") {
            if t.as_str() != tag.unwrap_or("") {
                return false;
            }
        }
        true
    }
    fn app_visible(&self, app: &WorkloadTemplate, principal: Option<&Token>, q: &BTreeMap<String, String>,
    ) -> bool {
        let selected = principal.and_then(|c| c.tag.as_deref())
            .or_else(|| q.get("tag").map(String::as_str));
        let fixed = app.tag.as_deref().or_else(|| app.spec["tag"].as_str());
        self.visible(&app.namespace, selected, principal, q)
            && fixed.is_none_or(|tag| {
                if principal.is_some() { selected == Some(tag) }
                else { selected.is_none_or(|selected| selected == tag)
                }
            })
    }
    fn scope(
        &self,
        principal: Option<&Token>,
        q: &BTreeMap<String, String>,
        reference: Option<&str>,
    ) -> anyhow::Result<(String, Option<String>)> {
        let reference = reference
            .or_else(|| q.get("namespace").map(String::as_str))
            .or_else(|| principal.and_then(|p| p.namespace.as_deref()))
            .ok_or_else(|| fail(400, "namespace: an existing namespace is required"))?;
        let n = self
            .fleet
            .namespace(reference)
            .ok_or_else(|| fail(404, "unknown namespace"))?;
        if n.suspended_ms.is_some() {
            return Err(fail(403, "namespace is suspended"));
        }
        let tag = principal
            .and_then(|p| p.tag.clone())
            .or_else(|| q.get("tag").filter(|t| !t.is_empty()).cloned());
        if !self.visible(&n.id, tag.as_deref(), principal, q) {
            return Err(fail(403, "another namespace or tag cannot be selected"));
        }
        if let Some(t) = &tag {
            crate::fleet::Scheduler::validate_tag(t).map_err(|e| fail(400, e.to_string()))?;
        }
        Ok((n.id, tag))
    }
    fn require_grant(c: Option<&Token>, name: &str) -> anyhow::Result<()> {
        if c.is_some_and(|c| {
            c.apps
                .as_ref()
                .is_some_and(|a| !a.iter().any(|a| a == name))
        }) {
            return Err(fail(403, "app is not granted"));
        }
        Ok(())
    }
    fn require_admin_settings(c: Option<&Token>, settings: &serde_json::Map<String, Value>, keys: &[&str]) -> anyhow::Result<()> {
        if c.is_some() {
            if let Some(key) = keys.iter().find(|key| settings.contains_key(**key)) {
                return Err(fail(403, format!("{key} requires administrator credentials")));
            }
        }
        Ok(())
    }
    fn require_service_scope(&self, c: Option<&Token>, namespace: &str, app: &str, node: Option<&str>,
    ) -> anyhow::Result<()> {
        if let Some(c) = c {
            let (_, deployment) = self.scale_service(namespace, app, node)?;
            if deployment.service["tag"].as_str().is_some_and(|tag| c.tag.as_deref() != Some(tag)) {
                return Err(fail(403, "another tag cannot be selected"));
            }
        }
        Ok(())
    }
    fn job(
        &self,
        id: &str,
        c: Option<&Token>,
        q: &BTreeMap<String, String>,
    ) -> anyhow::Result<Job> {
        let r = self
            .inner
            .lock()
            .jobs
            .get(id)
            .filter(|r| !r.purged)
            .cloned()
            .ok_or_else(|| fail(404, "no such batch job"))?;
        if !self.visible(&r.namespace, r.tag.as_deref(), c, q) {
            return Err(fail(404, "no such batch job"));
        }
        Ok(r)
    }
    /// The services among `keys` whose node is connected.
    fn online(&self, keys: &[String]) -> Vec<String> {
        let i = self.inner.lock();
        keys.iter().filter(|k| i.services.get(*k).and_then(|d| i.workers.get(&d.worker)).is_some_and(|w| now() - w.seen < 30.0)).cloned().collect()
    }

    /// Whether a batch still has job records (not purged).
    fn has_jobs(&self, batch: &str) -> bool {
        self.inner.lock().jobs.values().any(|r| r.batch == batch && !r.purged)
    }
    async fn submit(
        &self,
        batch: &str,
        mut request: Value,
        c: Option<&Token>,
        q: &BTreeMap<String, String>,
    ) -> anyhow::Result<Value> {
        let _ops = self.ops.lock().await;
        let body = request
            .as_object_mut()
            .ok_or_else(|| fail(400, "expected object"))?;
        let allowed = [
            "name",
            "command",
            "gpus",
            "cpus",
            "gpu_mem",
            "vram_wait",
            "machine",
            "nodes",
            "after",
            "retry",
            "restart_on_oom",
            "mount",
            "env",
            "log",
            "workdir",
        ];
        if body.keys().any(|k| !allowed.contains(&k.as_str())) {
            return Err(fail(400, "unknown or server-side submission fields"));
        }
        for key in [
            "cpus",
            "nodes",
            "gpu_mem",
            "vram_wait",
            "retry",
            "restart_on_oom",
        ] {
            if !request[key].is_null() && request[key].as_u64().is_none() {
                return Err(fail(400, format!("{key}: expected non-negative integer")));
            }
        }
        if !request["gpus"].is_null() && request["gpus"].as_f64().is_none() {
            return Err(fail(400, "gpus: expected number"));
        }
        for key in ["mount", "env", "machine"] {
            if !request[key].is_null()
                && !request[key]
                    .as_array()
                    .is_some_and(|a| a.iter().all(Value::is_string))
            {
                return Err(fail(400, format!("{key}: expected string array")));
            }
        }
        if request["machine"].as_array().is_some_and(|a| a.len() > 1) {
            return Err(fail(422, "one machine may be selected for a local job"));
        }
        for key in ["name", "after", "log", "workdir"] {
            if !request[key].is_null() && !request[key].is_string() {
                return Err(fail(400, format!("{key}: expected string")));
            }
        }
        let name = batch.to_string();
        Self::require_grant(c, &name)?;
        let app = self
            .inner
            .lock()
            .apps
            .get(&name)
            .filter(|a| !a.removed)
            .cloned()
            .ok_or_else(|| fail(404, "unknown batch"))?;
        if app.persistent || app.spec["disabled"].as_bool() == Some(true) {
            return Err(fail(409, "app does not accept batch jobs"));
        }
        let mut selectors = q.clone();
        selectors
            .entry("namespace".into())
            .or_insert(app.namespace.clone());
        let (namespace, tag) = self.scope(c, &selectors, Some(&app.namespace))?;
        if !request["command"]
            .as_array()
            .is_some_and(|cmd| !cmd.is_empty() && cmd.iter().all(Value::is_string))
        {
            return Err(fail(400, "command: a non-empty list of strings"));
        }
        let nodes = request["nodes"].as_u64().unwrap_or(1);
        if nodes != 1 {
            return Err(fail(
                422,
                "cross-machine jobs are not supported by local Ray; use one node",
            ));
        }
        let gpus = request["gpus"].as_f64().unwrap_or(1.0);
        let cpus = request["cpus"].as_u64().unwrap_or(8);
        if !gpus.is_finite()
            || gpus < 0.0
            || cpus < 1
            || (gpus > 1.0 && gpus.fract() != 0.0)
            || gpus > app.spec["max_gpus"].as_f64().unwrap_or(8.0)
        {
            return Err(fail(
                400,
                "invalid GPU/CPU request or batch GPU cap exceeded",
            ));
        }
        let n = self.fleet.namespace(&namespace).unwrap();
        let tq = n
            .tag_quotas
            .get(tag.as_deref().unwrap_or(""))
            .cloned()
            .unwrap_or_default();
        if !n.quota.fits(gpus, cpus as f64) || !tq.fits(gpus, cpus as f64) {
            return Err(fail(422, "job exceeds namespace/tag quota"));
        }
        if let Some(after) = request["after"].as_str() {
            self.job(after, c, &selectors)?;
        }
        let gpu_mem = request["gpu_mem"]
            .as_u64()
            .unwrap_or_else(|| app.spec["default_gpu_mem_gb"].as_u64().unwrap_or(0));
        if gpus > 0.0 && gpus < 1.0 && gpu_mem == 0 {
            return Err(fail(400, "fractional GPUs require a VRAM limit"));
        }
        let mounts = request["mount"].as_array().cloned().unwrap_or_default();
        let roots = app.spec["allow"].as_array().cloned().unwrap_or_default();
        if mounts.iter().any(|m| {
            m.as_str()
                .is_none_or(|m| !path_allowed(m.split(':').next().unwrap_or(""), &roots))
        }) {
            return Err(fail(403, "mount outside app allow roots"));
        }
        if c.is_some() && request["log"].is_string() {
            return Err(fail(403, "users must use job log storage"));
        }
        request["gpus"] = json!(gpus);
        request["cpus"] = json!(cpus);
        request["gpu_mem"] = json!(gpu_mem);
        request["nodes"] = json!(1);
        let base = text(&request, "name");
        let base = if base.is_empty() { name.as_str() } else { base };
        let base: String = base
            .chars()
            .take(40)
            .map(|c| {
                if c.is_ascii_alphanumeric() || "_-".contains(c) {
                    c
                } else {
                    '-'
                }
            })
            .collect();
        let id = format!("{base}-{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);
        let state = if request["after"].is_string() {
            "waiting_dependency"
        } else {
            "waiting_quota"
        };
        let r = Job {
            id: id.clone(),
            lease: uuid::Uuid::new_v4().to_string(),
            owner: c.map_or("admin", |c| c.id.as_str()).into(),
            namespace,
            tag,
            batch: name,
            policy: app.spec,
            request: request.into(),
            worker: None,
            status: json!({"id":id,"state":state,"submitted_at":now(),"gpus_requested":gpus,"cpus":cpus,"gpu_mem_gb":gpu_mem,"gpus":[],"nodes":1,"priority":0}).into(),
            quota_held: false,
            stop: None,
            logs_ready: false,
            purged: false,
        };
        self.save("jobs", &id, &r)?;
        let v = r.value();
        self.inner.lock().jobs.insert(id, r);
        self.notify.notify_waiters();
        Ok(v)
    }
    async fn report(
        &self,
        worker_id: &str,
        session: &str,
        report: NodeReport,
    ) -> anyhow::Result<Vec<String>> {
        if !is_safe_id(worker_id)
            || worker_id.len() > 63
            || session.len() > 80
            || session.is_empty()
            || report.version != 2
        {
            return Err(fail(400, "invalid worker contract"));
        }
        if [
            report.total_gpus,
            report.total_cpus,
            report.free_gpus,
            report.free_cpus,
        ]
        .into_iter()
        .any(|v| !v.is_finite() || v < 0.0)
            || report.free_gpus > report.total_gpus + 1e-6
            || report.free_cpus > report.total_cpus + 1e-6
        {
            return Err(fail(400, "invalid capacity"));
        }
        let mut report = report;
        for service in &mut report.services {
            let reference = service["namespace_ref"]
                .as_str()
                .or_else(|| service["namespace"].as_str())
                .unwrap_or(crate::store::SYSTEM_NAMESPACE)
                .to_owned();
            service["namespace_ref"] = json!(reference);
            if let Some(n) = self.fleet.namespace(&reference) {
                service["namespace"] = json!(n.id);
            }
        }
        for spec in report.batches.values().chain(report.services.iter()) {
            if let Some(tag) = spec["tag"].as_str() {
                crate::fleet::Scheduler::validate_tag(tag).map_err(|e| fail(400, e.to_string()))?;
            }
        }
        let mut worker_streams = HashSet::new();
        for service in &report.services {
            if integrated_worker(service) {
                if service["replicas"].as_u64().unwrap_or(1) != 1 {
                    return Err(fail(409, "integrated workers require exactly one replica"));
                }
                if service["disabled"].as_bool() != Some(true)
                    && !worker_streams.insert((text(service, "namespace"), text(service, "app"))) {
                    return Err(fail(409, "duplicate integrated worker stream in node report",
                    ));
                }
            }
        }
        let _ops = self.ops.lock().await;
        {
            let mut i = self.inner.lock();
            if i.workers
                .get(worker_id)
                .is_some_and(|w| w.session != session && now() - w.seen < 30.0)
            {
                return Err(fail(409, "another agent session is active for this worker"));
            }
            for service in &report.services {
                if service["disabled"].as_bool() != Some(true) && service["cleaned"].as_bool() != Some(true) {
                    self.check_worker_owner(&i, worker_id, service)?;
                }
            }
            let draining = i.workers.get(worker_id).is_some_and(|w| w.draining);
            i.workers.insert(
                worker_id.into(),
                NodeSession {
                    session: session.into(),
                    seen: now(),
                    report: report.clone(),
                    draining,
                },
            );
        }
        for (name, mut spec) in report.batches.clone().into_iter().chain(
            report
                .services
                .iter()
                .filter(|s| text(s, "kind") == "app")
                .map(|s| (text(s, "name").into(), s.clone())),
        ) {
            if !is_safe_id(&name) || !spec.is_object() {
                continue;
            }
            let reference = spec["namespace"]
                .as_str()
                .unwrap_or(crate::store::SYSTEM_NAMESPACE);
            let Some(namespace) = self.fleet.namespace(reference) else {
                continue;
            };
            spec["namespace"] = json!(namespace.id);
            if spec["priority"].is_null() {
                spec["priority"] = json!(0);
            }
            let persistent = text(&spec, "kind") == "app";
            let existing = self.inner.lock().apps.get(&name).cloned();
            // Apps and batches share one name space: a node's batch and serving app of the same
            // name would otherwise merge into one record. The one already known keeps the name.
            if existing.as_ref().is_some_and(|a| !a.removed && a.persistent != persistent) {
                tracing::warn!(app = %name, "node reports an app and a batch with the same name; ignoring the one that is not registered");
                continue;
            }
            let mut app = existing.unwrap_or(WorkloadTemplate {
                name: name.clone(),
                namespace: namespace.id.clone(),
                tag: None,
                spec,
                persistent,
                removed: false,
                workers: Vec::new(),
            });
            if app.namespace != namespace.id {
                continue;
            }
            if !app.workers.iter().any(|id| id == worker_id) {
                app.workers.push(worker_id.into());
            }
            self.save("apps", &name, &app)?;
            self.inner.lock().apps.insert(name, app);
        }
        for observed in &report.services {
            let name = text(observed, "name");
            if !is_safe_id(name)
                || !matches!(text(observed, "kind"), "app" | "ziren-gpu" | "ziren-cpu")
                || text(observed, "kind") == "app" && self.inner.lock().apps.get(name).is_some_and(|a| !a.removed && !a.persistent)
            {
                continue;
            }
            let Some(namespace) = self.fleet.namespace(text(observed, "namespace")) else {
                continue;
            };
            let key = format!("{worker_id}-{name}");
            let existing = { self.inner.lock().services.get(&key).cloned() };
            let intent = if let Some(mut intent) = existing {
                // The app a service runs, its program and its launch settings are the node's own
                // configuration, not resource-scale overrides.
                for field in ["app", "elf_id", "worker_command", "worker_env_file", "worker_workdir", "worker_stop_timeout_s", "machine", "proof_storage_root",
                ] {
                    if observed.get(field).is_some() || intent.service.get(field).is_some() {
                        intent.service[field] = observed[field].clone();
                    }
                }
                self.save("services", &key, &intent)?;
                self.inner.lock().services.insert(key.clone(), intent.clone());
                intent
            } else {
                let mut service = observed.clone();
                service["namespace_ref"] = observed["namespace_ref"].clone();
                service["namespace"] = json!(namespace.id);
                let intent = AppDeployment {
                    worker: worker_id.into(),
                    service,
                    disabled: observed["disabled"].as_bool().unwrap_or(false),
                    scale: None,
                };
                self.save("services", &key, &intent)?;
                self.inner
                    .lock()
                    .services
                    .insert(key.clone(), intent.clone());
                intent
            };
            if text(observed, "kind") == "app" || integrated_worker(observed) {
                let id = format!("service:{key}");
                let replicas = observed["replicas"].as_f64().unwrap_or(1.0);
                if observed["cleaned"].as_bool() == Some(true) && intent.disabled {
                    self.fleet.release_job(&namespace.id, &id).await?;
                } else if observed["ready"].as_bool() == Some(true) {
                    self.fleet
                        .reserve_job(
                            &namespace.id,
                            observed["tag"].as_str(),
                            &id,
                            observed["gpus"].as_f64().unwrap_or(0.0) * replicas,
                            observed["cpus"].as_f64().unwrap_or(1.0) * replicas,
                            true,
                        )
                        .await?;
                }
            }
            let mismatch = observed["disabled"].as_bool().unwrap_or(false) != intent.disabled
                || if intent.disabled {
                    observed["cleaned"].as_bool() != Some(true)
                } else {
                    observed["ready"].as_bool() != Some(true)
                };
            let pending = {
                self.inner.lock().commands.values().any(|c| {
                    !c.done
                        && c.worker == worker_id
                        && (c.operation == NodeOperation::SetService as i32 && text(&c.parameters["service"], "name") == name
                            || c.operation == NodeOperation::App as i32 && text(&c.parameters["service"], "name") == name)
                })
            };
            if !pending && !intent.scale.as_ref().is_some_and(ScaleStatus::pending)
                && !intent.scale.as_ref().is_some_and(ScaleStatus::failed)
                && !scaling::same_resources(observed, &intent.service)
            {
                // A node restart or local edit cannot replace the control plane's
                // desired allocation. Repair drift with the same drain/cleanup fences.
                let mut repaired = intent.clone();
                repaired.scale = Some(scaling::new_scale(self.with_priority(intent.service.clone()), intent.disabled));
                self.save("services", &key, &repaired)?;
                self.inner.lock().services.insert(key.clone(), repaired);
                continue;
            }
            if mismatch && !pending && !intent.scale.as_ref().is_some_and(ScaleStatus::pending) {
                if intent.scale.as_ref().is_some_and(ScaleStatus::failed) {
                    // After a failed change the service stays down, its failure still shown.
                    if observed["disabled"].as_bool() != Some(true) {
                        self.tell_nodes([&worker_id.to_owned()], json!({"service":{"name":name}}), json!({"set":{"disabled":true}}))?;
                    }
                    if observed["cleaned"].as_bool() != Some(true) {
                        self.command(worker_id, NodeOperation::SetService,
                            json!({"service":intent.service,"disabled":true,"scale_cleanup":true}))?;
                    }
                } else if observed["disabled"].as_bool().unwrap_or(false) != intent.disabled {
                    // The node's configuration drifted from the desired one: write it back like a scale.
                    let mut repaired = intent.clone();
                    repaired.scale = Some(scaling::new_scale(self.with_priority(intent.service.clone()), intent.disabled));
                    self.save("services", &key, &repaired)?;
                    self.inner.lock().services.insert(key.clone(), repaired);
                } else {
                    // Configured as desired but not running as desired: start or stop it.
                    self.command(worker_id, NodeOperation::SetService,
                        json!({"service":self.with_priority(intent.service.clone()),"disabled":intent.disabled}))?;
                }
            }
        }
        self.forget_unreported_services(worker_id, &report.services.iter().map(|s| text(s, "name").to_owned()).collect::<Vec<_>>())?;
        self.refresh_worker_owned_apps();
        for incoming in &report.jobs {
            if !is_safe_id(&incoming.id) || !incoming.status.is_object() {
                continue;
            }
            let existing = { self.inner.lock().jobs.get(&incoming.id).cloned() };
            let mut r = if let Some(r) = existing {
                r
            } else {
                let batch = text(&incoming.spec, "batch").to_string();
                let Some(app) = self.inner.lock().apps.get(&batch).filter(|a| !a.removed).cloned() else {
                    continue;
                };
                let mut request = json!({"command":incoming.spec["command"],"gpus":incoming.spec["gpus"],"cpus":incoming.spec["cpus"],"nodes":1});
                if request["gpus"].is_null() {
                    request["gpus"] = json!(0);
                }
                Job {
                    id: incoming.id.clone(),
                    lease: incoming.lease.clone(),
                    owner: incoming.spec["owner"].as_str().unwrap_or("admin").into(),
                    namespace: app.namespace,
                    tag: incoming.spec["tag"].as_str().map(str::to_owned),
                    batch,
                    policy: incoming.spec["policy"].clone().into(),
                    request: request.into(),
                    worker: Some(worker_id.into()),
                    status: incoming.status.clone(),
                    quota_held: false,
                    stop: None,
                    logs_ready: incoming.logs_ready,
                    purged: false,
                }
            };
            if r.purged || r.worker.as_deref() != Some(worker_id) || r.lease != incoming.lease {
                continue;
            }
            let accepted = [
                "queued",
                "waiting_quota",
                "waiting_vram",
                "running",
                "retrying",
                "succeeded",
                "failed",
                "cancelled",
                "preempted",
                "lost",
            ];
            if !accepted.contains(&text(&incoming.status, "state")) {
                continue;
            }
            // A reconnect cannot regress terminal state or cancel/preemption intent.
            if !terminal(r.state()) || r.stop.is_some() {
                r.status = incoming.status.clone();
            }
            r.logs_ready |= incoming.logs_ready;
            if r.stop.as_deref() == Some("preempt") && terminal(r.state()) {
                r.stop = None;
            }
            if (terminal(r.state()) || r.state() == "preempted") && incoming.cleaned {
                if r.quota_held {
                    self.fleet.release_job(&r.namespace, &r.id).await?;
                    r.quota_held = false;
                }
                if r.stop.as_deref() == Some("preempt") {
                    r.lease = uuid::Uuid::new_v4().to_string();
                    r.worker = None;
                    r.logs_ready = false;
                    r.status["state"] = json!("waiting_quota");
                    r.status["finished_at"] = Value::Null;
                    r.stop = None;
                } else if r.stop.as_deref() == Some("cancel") {
                    r.status["state"] = json!("cancelled");
                    r.stop = None;
                }
            } else if !r.quota_held
                && (!incoming.cleaned || !terminal(r.state()) && r.state() != "preempted")
            {
                self.fleet
                    .reserve_job(
                        &r.namespace,
                        r.tag.as_deref(),
                        &r.id,
                        r.gpus(),
                        r.cpus(),
                        true,
                    )
                    .await?;
                r.quota_held = true;
            }
            self.save("jobs", &r.id, &r)?;
            self.inner.lock().jobs.insert(r.id.clone(), r);
        }
        let mut acknowledged = vec![];
        for result in &report.results {
            let Some(mut c) = self.inner.lock().commands.get(&result.id).cloned() else {
                acknowledged.push(result.id.clone());
                continue;
            };
            if c.worker != worker_id {
                return Err(fail(403, "command belongs to another worker"));
            }
            if !c.done {
                c.done = true;
                c.result = result.result.clone();
                c.error = result.error.clone();
                if c.operation == NodeOperation::LaunchJob as i32 && c.error.is_some() {
                    let id = text(&c.parameters, "id");
                    let job = { self.inner.lock().jobs.get(id).cloned() };
                    if let Some(mut r) = job {
                        if r.lease == text(&c.parameters, "lease")
                            && r.worker.as_deref() == Some(worker_id)
                        {
                            r.status["state"] = json!("failed");
                            r.status["error"] = json!(c.error);
                            r.status["finished_at"] = json!(now());
                            if c.result["cleaned"].as_bool() == Some(true) && r.quota_held {
                                self.fleet.release_job(&r.namespace, &r.id).await?;
                                r.quota_held = false;
                                r.logs_ready = true;
                            }
                            self.save("jobs", &r.id, &r)?;
                            self.inner.lock().jobs.insert(r.id.clone(), r);
                        }
                    }
                }
                if matches!(
                    NodeOperation::try_from(c.operation),
                    Ok(NodeOperation::CancelJob | NodeOperation::PreemptJob)
                ) && c.error.is_none()
                    && c.result["cleaned"].as_bool() == Some(true)
                {
                    let existing = {
                        self.inner
                            .lock()
                            .jobs
                            .get(text(&c.parameters, "id"))
                            .cloned()
                    };
                    if let Some(mut r) = existing {
                        if r.lease == text(&c.parameters, "lease")
                            && r.worker.as_deref() == Some(worker_id)
                            && r.stop.is_some()
                        {
                            if r.quota_held {
                                self.fleet.release_job(&r.namespace, &r.id).await?;
                                r.quota_held = false;
                            }
                            if r.stop.as_deref() == Some("preempt") {
                                r.lease = uuid::Uuid::new_v4().to_string();
                                r.worker = None;
                                r.status["state"] = json!("waiting_quota");
                                r.status["finished_at"] = Value::Null;
                            } else {
                                r.status["state"] = json!("cancelled");
                                r.status["finished_at"] = json!(now());
                            }
                            r.stop = None;
                            self.save("jobs", &r.id, &r)?;
                            self.inner.lock().jobs.insert(r.id.clone(), r);
                        }
                    }
                }
                if c.operation == NodeOperation::SetService as i32 && c.error.is_none() {
                    let service = &c.parameters["service"];
                    if (text(service, "kind") == "app" || integrated_worker(service))
                        && c.parameters["disabled"].as_bool() == Some(true)
                        && c.result["cleaned"].as_bool() == Some(true)
                    {
                        self.fleet
                            .release_job(
                                text(service, "namespace"),
                                &format!("service:{}-{}", worker_id, text(service, "name")),
                            )
                            .await?;
                    }
                    if text(service, "kind") == "ziren-gpu"
                        && c.parameters["disabled"].as_bool() == Some(false)
                        && c.parameters["scale_id"].is_null()
                    {
                        self.sync_app_record(service, false).await?;
                    }
                }
                self.save("commands", &c.id, &c)?;
                self.inner.lock().commands.insert(c.id.clone(), c);
            }
            acknowledged.push(result.id.clone());
        }
        self.notify.notify_waiters();
        Ok(acknowledged)
    }
    fn capacity(&self, worker: &str) -> Option<(f64, f64)> {
        let i = self.inner.lock();
        let w = i.workers.get(worker)?;
        if !w.report.ready || w.draining || now() - w.seen > 30.0 {
            return None;
        }
        let pending = i
            .jobs
            .values()
            .filter(|r| {
                r.worker.as_deref() == Some(worker)
                    && r.quota_held
                    && !terminal(r.state())
                    && !w.report.jobs.iter().any(|s| {
                        s.id == r.id
                            && s.lease == r.lease
                            && !matches!(text(&s.status, "state"), "queued" | "waiting_quota")
                    })
            })
            .fold((0.0, 0.0), |(g, c), r| (g + r.gpus(), c + r.cpus()));
        let services = i
            .commands
            .values()
            .filter(|c| {
                c.worker == worker
                    && !c.done
                    && c.admitted
                    && c.operation == NodeOperation::SetService as i32
                    && c.parameters["disabled"].as_bool() == Some(false)
            })
            .fold((0.0, 0.0), |(g, c), cmd| {
                let s = &cmd.parameters["service"];
                let n = s["replicas"].as_f64().unwrap_or(1.0);
                (
                    g + s["gpus"].as_f64().unwrap_or(0.0) * n,
                    c + s["cpus"].as_f64().unwrap_or(1.0) * n,
                )
            });
        Some((
            (w.report.free_gpus - pending.0 - services.0).max(0.0),
            (w.report.free_cpus - pending.1 - services.1).max(0.0),
        ))
    }
    fn purge(&self, mut r: Job) -> anyhow::Result<Value> {
        if !terminal(r.state()) || r.quota_held {
            return Err(fail(409, "wait for confirmed cleanup before purging"));
        }
        r.purged = true;
        self.save("jobs", &r.id, &r)?;
        self.inner.lock().jobs.insert(r.id.clone(), r.clone());
        if let Some(worker) = &r.worker {
            self.command(
                worker,
                NodeOperation::PurgeJob,
                json!({"id":r.id,"lease":r.lease}),
            )?;
        }
        let path = self.dir.join("artifacts").join(&r.id);
        if path.exists() {
            fs::remove_dir_all(path)?;
        }
        Ok(json!({"id":r.id,"purged":true}))
    }
    async fn stop_job(&self, mut r: Job, preempt: bool) -> anyhow::Result<Job> {
        if terminal(r.state()) && (preempt || !r.quota_held) {
            return Err(fail(409, "job already finished"));
        }
        if let Some(worker) = &r.worker {
            r.stop = Some(if preempt { "preempt" } else { "cancel" }.into());
            self.save("jobs", &r.id, &r)?;
            self.inner.lock().jobs.insert(r.id.clone(), r.clone());
            self.command(
                worker,
                if preempt {
                    NodeOperation::PreemptJob
                } else {
                    NodeOperation::CancelJob
                },
                json!({"id":r.id,"lease":r.lease}),
            )?;
        } else if !preempt {
            r.status["state"] = json!("cancelled");
            r.status["finished_at"] = json!(now());
            r.logs_ready = true;
            self.save("jobs", &r.id, &r)?;
            self.inner.lock().jobs.insert(r.id.clone(), r.clone());
        }
        Ok(r)
    }
    async fn preempt_for(
        &self,
        namespace: &str,
        tag: Option<&str>,
        priority: i32,
        gpus: f64,
        cpus: f64,
        pins: Vec<Option<String>>,
        exclude: Option<String>,
    ) -> anyhow::Result<bool> {
        let (workers, candidates) = {
            let i = self.inner.lock();
            let workers = i.workers.keys().cloned().collect::<Vec<_>>();
            let candidates = i
                .jobs
                .values()
                .filter(|r| {
                    r.quota_held
                        && r.stop.is_none()
                        && matches!(r.state(), "running" | "waiting_vram" | "retrying")
                })
                .filter_map(|r| {
                    r.worker.as_ref().map(|w| PreemptionCandidate {
                        id: r.id.clone(),
                        namespace: r.namespace.clone(),
                        priority: r.policy["priority"].as_i64().unwrap_or(0) as i32,
                        node_ip: w.clone(),
                        gpus: r.gpus(),
                        cpus: r.cpus(),
                        started_at: r.status["started_at"].as_f64(),
                        preemptible: r.policy["preemptible"].as_bool().unwrap_or(true),
                        min_runtime_s: r.policy["min_runtime_s"].as_u64().unwrap_or(300),
                    })
                })
                .collect::<Vec<_>>();
            (workers, candidates)
        };
        let free = workers
            .into_iter()
            .filter_map(|w| {
                self.capacity(&w).map(|(gpus, cpus)| NodeCapacity {
                    node_ip: w,
                    gpus,
                    cpus,
                })
            })
            .collect();
        let plan = self.fleet.plan_preemption(PreemptionRequest {
            contract_version: 1,
            namespace: namespace.into(),
            tag: tag.map(str::to_owned),
            priority,
            gpus,
            cpus,
            nodes: 1,
            pins: pins.into_iter().map(Into::into).collect(),
            now: now(),
            free,
            candidates,
            quota_demand: Some(crate::quota::Usage { gpus, cpus }),
            exclude_allocation: exclude,
        })?;
        if !plan.possible || plan.jobs.is_empty() {
            return Ok(false);
        }
        for id in plan.jobs {
            let r = { self.inner.lock().jobs.get(&id).cloned() };
            if let Some(r) = r {
                self.stop_job(r, true).await?;
            }
        }
        Ok(true)
    }
    async fn dispatch(&self) -> anyhow::Result<()> {
        let _ops = self.ops.lock().await;
        self.advance_scales().await?;
        let held = {
            self.inner
                .lock()
                .jobs
                .values()
                .filter(|r| r.quota_held)
                .cloned()
                .collect::<Vec<_>>()
        };
        for r in held {
            self.fleet
                .reserve_job(
                    &r.namespace,
                    r.tag.as_deref(),
                    &r.id,
                    r.gpus(),
                    r.cpus(),
                    true,
                )
                .await?;
        }
        let stopping = {
            self.inner
                .lock()
                .jobs
                .values()
                .filter(|r| r.stop.is_some() && r.worker.is_some())
                .cloned()
                .collect::<Vec<_>>()
        };
        for r in stopping {
            let operation = if r.stop.as_deref() == Some("preempt") {
                NodeOperation::PreemptJob
            } else {
                NodeOperation::CancelJob
            };
            let parameters = json!({"id":r.id,"lease":r.lease});
            let recorded = {
                self.inner.lock().commands.values().any(|c| {
                    c.worker == *r.worker.as_ref().unwrap()
                        && c.operation == operation as i32
                        && c.parameters == parameters
                })
            };
            if !recorded {
                self.command(r.worker.as_deref().unwrap(), operation, parameters)?;
            }
        }
        let services = {
            self.inner
                .lock()
                .commands
                .values()
                .filter(|c| {
                    !c.done && !c.admitted && c.operation == NodeOperation::SetService as i32
                })
                .cloned()
                .collect::<Vec<_>>()
        };
        for mut c in services {
            let s = &c.parameters["service"];
            let disabled = c.parameters["disabled"].as_bool() == Some(true);
            if disabled && text(s, "kind") == "ziren-gpu" {
                let names = (0..s["replicas"].as_u64().unwrap_or(1))
                    .map(|n| format!("{}-{n}", text(s, "name")))
                    .collect::<HashSet<_>>();
                if self
                    .fleet
                    .status()
                    .nodes
                    .iter()
                    .any(|n| names.contains(&n.name) && n.busy.is_some())
                {
                    continue;
                }
            }
            if disabled && text(s, "kind") == "ziren-gpu" {
                let jobs = self
                    .fleet
                    .recent_jobs(
                        &crate::store::ProofJobFilter {
                            namespace: Some(text(s, "namespace").into()),
                            app: Some(text(s, "app").into()),
                            active: true,
                            ..Default::default()
                        },
                        usize::MAX,
                    )
                    .await?;
                if jobs
                    .iter()
                    .any(|j| j.node.is_some() && j.started_ms.is_some())
                {
                    continue;
                }
            }
            if !disabled {
                let already_ready = {
                    let i = self.inner.lock();
                    i.workers.get(&c.worker).is_some_and(|w| {
                        w.report.services.iter().any(|v| {
                            text(v, "name") == text(s, "name") && v["ready"].as_bool() == Some(true)
                                && v["disabled"].as_bool() != Some(true)
                                && scaling::same_resources(v, s)
                        })
                    })
                };
                let n = s["replicas"].as_f64().unwrap_or(1.0);
                let g = s["gpus"].as_f64().unwrap_or(0.0) * n;
                let cpu = s["cpus"].as_f64().unwrap_or(1.0) * n;
                let fits = already_ready
                    || self
                        .capacity(&c.worker)
                        .is_some_and(|(fg, fc)| fg + 1e-6 >= g && fc + 1e-6 >= cpu);
                if !fits {
                    self.preempt_for(
                        text(s, "namespace"),
                        s["tag"].as_str(),
                        s["priority"].as_i64().unwrap_or(0) as i32,
                        g,
                        cpu,
                        vec![Some(c.worker.clone())],
                        None,
                    )
                    .await?;
                    continue;
                }
                if (text(s, "kind") == "app" || integrated_worker(s))
                    && self
                        .fleet
                        .reserve_job(
                            text(s, "namespace"),
                            s["tag"].as_str(),
                            &format!("service:{}-{}", c.worker, text(s, "name")),
                            g,
                            cpu,
                            false,
                        )
                        .await
                        .is_err()
                {
                    continue;
                }
            }
            c.admitted = true;
            self.save("commands", &c.id, &c)?;
            self.inner.lock().commands.insert(c.id.clone(), c);
        }
        let mut jobs = {
            self.inner
                .lock()
                .jobs
                .values()
                .filter(|r| {
                    r.worker.is_none()
                        && matches!(r.state(), "waiting_quota" | "waiting_dependency")
                        && r.stop.is_none()
                })
                .cloned()
                .collect::<Vec<_>>()
        };
        jobs.sort_by(|a, b| {
            b.policy["priority"]
                .as_i64()
                .unwrap_or(0)
                .cmp(&a.policy["priority"].as_i64().unwrap_or(0))
                .then(
                    a.status["submitted_at"]
                        .as_f64()
                        .unwrap_or(0.0)
                        .total_cmp(&b.status["submitted_at"].as_f64().unwrap_or(0.0)),
                )
                .then(a.id.cmp(&b.id))
        });
        for mut r in jobs {
            let namespace = self
                .fleet
                .namespace(&r.namespace)
                .ok_or_else(|| fail(404, "job namespace disappeared"))?;
            let tag_quota = namespace
                .tag_quotas
                .get(r.tag.as_deref().unwrap_or(""))
                .cloned()
                .unwrap_or_default();
            if !namespace.quota.fits(r.gpus(), r.cpus()) || !tag_quota.fits(r.gpus(), r.cpus()) {
                r.status["state"] = json!("failed");
                r.status["error"] = json!("job exceeds current namespace/tag quota");
                r.status["finished_at"] = json!(now());
                self.save("jobs", &r.id, &r)?;
                self.inner.lock().jobs.insert(r.id.clone(), r);
                continue;
            }
            if let Some(after) = r.request["after"].as_str() {
                let state = self
                    .inner
                    .lock()
                    .jobs
                    .get(after)
                    .map(|r| r.state().to_owned());
                if state.as_deref() != Some("succeeded") {
                    if state.as_deref().is_some_and(terminal) {
                        r.status["state"] = json!("failed");
                        r.status["error"] = json!("dependency did not succeed");
                        r.status["finished_at"] = json!(now());
                        self.save("jobs", &r.id, &r)?;
                        self.inner.lock().jobs.insert(r.id.clone(), r);
                    }
                    continue;
                }
            }
            let allowed = {
                let i = self.inner.lock();
                let app = i.apps.get(&r.batch);
                i.workers
                    .iter()
                    .filter(|(id, w)| {
                        let pin = r.request["machine"]
                            .as_array()
                            .and_then(|m| m.first())
                            .and_then(Value::as_str);
                        let app_ok =
                            app.is_none_or(|a| a.workers.is_empty() || a.workers.contains(*id));
                        app_ok && pin.is_none_or(|p| p == id.as_str() || p == w.report.machine)
                    })
                    .map(|(id, _)| id.clone())
                    .collect::<Vec<_>>()
            };
            if allowed.is_empty() {
                continue;
            }
            let worker = allowed
                .iter()
                .find(|w| {
                    self.capacity(w)
                        .is_some_and(|(g, c)| g + 1e-6 >= r.gpus() && c + 1e-6 >= r.cpus())
                })
                .cloned();
            let Some(worker) = worker else {
                self.preempt_for(
                    &r.namespace,
                    r.tag.as_deref(),
                    r.policy["priority"].as_i64().unwrap_or(0) as i32,
                    r.gpus(),
                    r.cpus(),
                    allowed.into_iter().map(Some).take(1).collect(),
                    Some(format!("job:{}", r.id)),
                )
                .await?;
                continue;
            };
            r.quota_held = true;
            r.worker = Some(worker.clone());
            r.status["state"] = json!("queued");
            r.status["detail"] = json!("assigned; awaiting node acknowledgement");
            self.save("jobs", &r.id, &r)?;
            if self
                .fleet
                .reserve_job(
                    &r.namespace,
                    r.tag.as_deref(),
                    &r.id,
                    r.gpus(),
                    r.cpus(),
                    false,
                )
                .await
                .is_err()
            {
                r.quota_held = false;
                r.worker = None;
                r.status["state"] = json!("waiting_quota");
                self.save("jobs", &r.id, &r)?;
                self.inner.lock().jobs.insert(r.id.clone(), r.clone());
                self.preempt_for(
                    &r.namespace,
                    r.tag.as_deref(),
                    r.policy["priority"].as_i64().unwrap_or(0) as i32,
                    r.gpus(),
                    r.cpus(),
                    vec![Some(worker)],
                    Some(format!("job:{}", r.id)),
                )
                .await?;
                continue;
            }
            self.inner.lock().jobs.insert(r.id.clone(), r.clone());
            self.command(&worker,NodeOperation::LaunchJob,json!({"id":r.id,"lease":r.lease,"namespace":r.namespace,"tag":r.tag,"owner":r.owner,"batch":r.batch,"policy":r.policy,"request":r.request,"submitted_at":r.status["submitted_at"]}))?;
        }
        // A durable assigned job whose process died before command persistence still
        // receives the same lease. Uncertain execution is never moved to another node.
        let assigned = {
            self.inner
                .lock()
                .jobs
                .values()
                .filter(|r| r.worker.is_some() && r.state() == "queued" && r.stop.is_none())
                .cloned()
                .collect::<Vec<_>>()
        };
        for r in assigned {
            self.command(r.worker.as_deref().unwrap(),NodeOperation::LaunchJob,json!({"id":r.id,"lease":r.lease,"namespace":r.namespace,"tag":r.tag,"owner":r.owner,"batch":r.batch,"policy":r.policy,"request":r.request,"submitted_at":r.status["submitted_at"]}))?;
        }
        Ok(())
    }
    pub async fn dispatch_loop(self: Arc<Self>) -> anyhow::Result<()> {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if let Err(e) = self.dispatch().await {
                tracing::error!(error=%e,"job dispatch retained durable intents");
            }
        }
    }
}

pub struct NodeControlService(pub Arc<WorkloadController>);
#[tonic::async_trait]
impl WorkerService for NodeControlService {
    async fn heartbeat(
        &self,
        request: tonic::Request<WorkerHeartbeatRequest>,
    ) -> Result<tonic::Response<WorkerHeartbeatResponse>, tonic::Status> {
        let r = request.into_inner();
        let report = r.report
            .ok_or_else(|| tonic::Status::invalid_argument("missing worker report"))?;
        let machine = report.machine.clone();
        let reported = report
            .jobs
            .iter()
            .filter(|r| r.cleaned && r.logs_ready && terminal(text(&r.status, "state")))
            .map(|r| (r.id.clone(), r.lease.clone()))
            .collect::<Vec<_>>();
        let ack = self
            .0
            .report(&r.worker_id, &r.session, report)
            .await
            .map_err(|e| tonic::Status::failed_precondition(e.to_string()))?;
        let r_worker = r.worker_id.clone();
        let commands = self
            .0
            .inner
            .lock()
            .commands
            .values()
            .filter(|c| c.worker == r.worker_id && !c.done && c.admitted)
            .take(16)
            .map(|c| NodeCommand {
                id: c.id.clone(),
                operation: c.operation,
                parameters: Some(c.parameters.clone()),
            })
            .collect();
        Ok(tonic::Response::new(WorkerHeartbeatResponse {
            proof_fetches: self.0.fleet.proof_fetch.heartbeat(&machine, &r.worker_id, &r.session, r.proof_fetch_ready,
            ),
            log_requests: self.0.fleet.log_relay.heartbeat(&r.worker_id, &r.session),
            heartbeat_secs: 2,
            commands,
            acknowledged_results: ack,
            acknowledged_jobs: reported
                .into_iter()
                .filter(|(id, lease)| {
                    self.0.inner.lock().jobs.get(id).is_some_and(|r| {
                        !r.purged
                            && r.lease == *lease
                            && r.worker.as_deref() == Some(r_worker.as_str())
                            && terminal(r.state())
                            && r.logs_ready
                            && !r.quota_held
                    })
                })
                .map(|(id, lease)| format!("{id}/{lease}"))
                .collect(),
        }))
    }
}
fn load<T: serde::de::DeserializeOwned>(dir: &Path) -> anyhow::Result<Vec<T>> {
    fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|e| e == "json"))
        .map(|e| Ok(serde_json::from_slice(&fs::read(e.path())?)?))
        .collect()
}
fn atomic(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    fs::create_dir_all(path.parent().unwrap())?;
    let tmp = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
    let mut f = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    fs::rename(&tmp, path)?;
    fs::File::open(path.parent().unwrap())?.sync_all()?;
    Ok(())
}
fn path_allowed(path: &str, roots: &[Value]) -> bool {
    path.starts_with('/')
        && !path.split('/').any(|p| p == "..")
        && roots.iter().filter_map(Value::as_str).any(|r| {
            r != "/"
                && (path == r.trim_end_matches('/')
                    || path.starts_with(&format!("{}/", r.trim_end_matches('/'))))
        })
}

/// The checks every batch's settings pass, on create and on update.
fn check_batch_spec(spec: &Value) -> anyhow::Result<()> {
    if spec["priority"].as_i64().is_none_or(|p| i32::try_from(p).is_err())
        || spec["max_gpus"].as_u64().is_none()
        || !spec["allow"].is_array()
        || !spec["env"].is_object()
    {
        return Err(fail(400, "invalid batch"));
    }
    if ["disabled", "preemptible"].into_iter().any(|k| !spec[k].is_boolean())
        || ["default_gpu_mem_gb", "shm_gb", "min_runtime_s", "stop_timeout_s"]
            .into_iter()
            .any(|k| spec[k].as_u64().is_none())
        || spec["env"].as_object().is_none_or(|v| v.values().any(|v| !v.is_string()))
        || spec["allow"].as_array().is_none_or(|v| {
            v.iter().any(|v| {
                v.as_str().is_none_or(|p| !p.starts_with('/') || p == "/" || p.split('/').any(|s| s == ".."))
            })
        })
        || !spec["user"].is_null() && !spec["user"].is_string()
        || text(spec, "image").is_empty() || text(spec, "image").starts_with('-')
    {
        return Err(fail(400, "invalid batch settings"));
    }
    Ok(())
}

/// Settings an update may change on a batch (its name, namespace, priority and enabled state
/// have their own operations).
const BATCH_SETTINGS: [&str; 10] = ["image", "user", "max_gpus", "default_gpu_mem_gb", "shm_gb", "allow", "env",
    "preemptible", "min_runtime_s", "stop_timeout_s"];
const BATCH_ADMIN_SETTINGS: [&str; 2] = ["user", "allow"];

/// The apps and batches API. `crate::http::router` authorizes each request first.
pub fn router(control: Arc<WorkloadController>) -> Router {
    let mut router = Router::new();
    for root in ["apps", "batches"] {
        router = router
            .route(&format!("/{root}"), any(handle))
            .route(&format!("/{root}/*path"), any(handle));
    }
    router.with_state(control)
}

/// The nodes' batch log uploads, authenticated by the cluster token.
pub fn internal_router(control: Arc<WorkloadController>) -> Router {
    Router::new()
        .route("/internal/jobs/*path", any(internal_handle))
        .with_state(control)
}

async fn handle(State(s): State<Arc<WorkloadController>>, request: Request) -> Response {
    match handle_result(s, request).await {
        Ok(r) => r,
        Err(e) => response_error(e),
    }
}

async fn internal_handle(State(s): State<Arc<WorkloadController>>, request: Request) -> Response {
    let uri = request.uri().clone();
    let parts: Vec<&str> = uri.path().trim_matches('/').split('/').collect();
    let result = match query(&uri) {
        Ok(q) => internal_logs(s, request, &parts, &q).await,
        Err(e) => Err(e),
    };
    result.unwrap_or_else(response_error)
}

fn query(uri: &axum::http::Uri) -> anyhow::Result<BTreeMap<String, String>> {
    Ok(reqwest::Url::parse(&format!("http://localhost{uri}"))?
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect())
}

async fn handle_result(s: Arc<WorkloadController>, request: Request) -> anyhow::Result<Response> {
    let uri = request.uri().clone();
    let method = request.method().clone();
    let q = query(&uri)?;
    if q.contains_key("group") {
        return Err(fail(400, "use optional tag; groups are not a resource"));
    }
    let mut parts: Vec<_> = uri.path().trim_matches('/').split('/').collect();
    // The token `crate::http::router` admitted: None for an administrator's.
    let principal = request.extensions().get::<Option<Token>>().cloned()
        .ok_or_else(|| fail(401, "missing or wrong token"))?;
    let c = principal.as_ref();
    // Scope selectors never widen a token's namespace or tag.
    if let Some(c) = c {
        s.scope(Some(c), &q, None)?;
    }
    if parts.first() == Some(&"apps") {
        if parts.len() >= 2 {
            if let Some(app) = s.proof_app(c, &q, parts[1])? {
                return s.proof_app_request(c, &q, app, &method, &parts[2..], request).await;
            }
        }
        if parts.get(2) == Some(&"jobs") {
            return Err(fail(404, format!("no proof app {} (only proof apps have jobs; a batch's are under /batches)", parts[1])));
        }
    }
    // /batches/{name}/jobs[/{id}] are one batch's jobs; the other /batches routes are batch
    // definitions, which share the serving app template routes.
    let batch_jobs = parts.first() == Some(&"batches") && parts.get(2) == Some(&"jobs");
    let batch = parts.first() == Some(&"batches") && !batch_jobs;
    if batch { parts[0] = "apps"; }
    if batch && parts.len() == 3 && parts[2] == "scale" {
        return Err(fail(400, "batch resources are selected per job with batch submit --gpus and --cpus",
        ));
    }
    let action = match parts.as_slice() {
        ["apps", _, action] if matches!(*action, "scale" | "logs" | "settings") && !batch => Some(*action),
        _ => None,
    };
    if let Some(action) = action {
        let (namespace, _) = s.scope(c, &q, None)?;
        let app = s.inner.lock().apps.get(parts[1]).filter(|a| !a.removed && a.namespace == namespace && s.app_visible(a, c, &q))
            .cloned().ok_or_else(|| fail(404, "unknown app"))?;
        if !app.persistent { return Err(fail(400, "batch job resources are selected with batch submit --gpus and --cpus",
            )); }
        WorkloadController::require_grant(c, &app.name)?;
        return s.service_action(c, &q, &namespace, &app.name, action, &method, request).await;
    }
    if method == Method::GET && parts == ["apps"] {
        if q.contains_key("tag") { s.scope(c, &q, None)?; }
        let templates = {
            let i = s.inner.lock();
            let with_jobs: HashSet<&str> = i.jobs.values().filter(|r| !r.purged).map(|r| r.batch.as_str()).collect();
            i.apps
                .values()
                .filter(|a| {
                    (!a.removed || batch && with_jobs.contains(a.name.as_str()))
                        && a.persistent != batch
                        && s.app_visible(a, c, &q)
                        && WorkloadController::require_grant(c, &a.name).is_ok()
                })
                .cloned()
                .collect::<Vec<_>>()
        };
        let tag = c.map(|c| c.tag.as_deref().unwrap_or("")).or_else(|| q.get("tag").map(String::as_str));
        if batch {
            return Ok(Json(templates.iter().map(|a| s.batch_value(a, tag)).collect::<Vec<_>>()).into_response());
        }
        let mut apps: Vec<Value> = templates.iter().map(|a| s.serving_app_value(a, tag)).collect();
        apps.extend(s.proof_app_values(c, &q, tag).await?);
        let last = |a: &Value| a["jobs"]["last_at"].as_f64().unwrap_or(0.0);
        apps.sort_by(|a, b| last(b).total_cmp(&last(a)));
        return Ok(Json(apps).into_response());
    }
    if method == Method::POST && parts == ["apps"] {
        if !batch {
            let body: Value = serde_json::from_slice(&to_bytes(request.into_body(), 1 << 20).await?)?;
            // A serving app names the node that runs it and its image; a proof app names a program.
            if body.get("node").is_some() || body.get("image").is_some() {
                return s.create_serving_app(c, &q, body).await;
            }
            let body: fleet_proto::model::v1::RegisterAppRequest = serde_json::from_value(body)?;
            let (namespace, tag) = s.scope(c, &q, body.namespace.as_deref())?;
            WorkloadController::require_grant(c, &body.name)?;
            let _ops = s.ops.lock().await;
            if s.inner.lock().apps.get(&body.name).is_some_and(|a| !a.removed) {
                return Err(fail(409, format!("an app or batch named {} exists", body.name)));
            }
            let app = s.fleet.add_app(&namespace, &body).await?;
            drop(_ops);
            return Ok((StatusCode::CREATED, Json(s.proof_app_value(&app, None, tag.as_deref()).await?)).into_response());
        }
        let mut body: Value =
            serde_json::from_slice(&to_bytes(request.into_body(), 1 << 20).await?)?;
        let name = text(&body, "name").to_string();
        if !crate::fleet::valid_app_name(&name) {
            return Err(fail(400, "invalid batch name"));
        }
        let (namespace, tag) = s.scope(c, &q, body["namespace"].as_str())?;
        if !body["image"].is_string() {
            return Err(fail(400, "image is required"));
        }
        WorkloadController::require_grant(c, &name)?;
        let allowed = [
            "name",
            "namespace",
            "image",
            "user",
            "max_gpus",
            "default_gpu_mem_gb",
            "shm_gb",
            "allow",
            "env",
            "disabled",
            "preemptible",
            "min_runtime_s",
            "stop_timeout_s",
            "priority",
        ];
        if body
            .as_object()
            .is_none_or(|o| o.keys().any(|k| !allowed.contains(&k.as_str())))
        {
            return Err(fail(400, "unknown batch fields"));
        }
        for (key, default) in [
            ("max_gpus", json!(8)),
            ("default_gpu_mem_gb", json!(0)),
            ("shm_gb", json!(16)),
            ("priority", json!(0)),
            ("allow", json!([])),
            ("env", json!({})),
            ("disabled", json!(false)),
            ("preemptible", json!(true)),
            ("min_runtime_s", json!(300)),
            ("stop_timeout_s", json!(30)),
        ] {
            if body[key].is_null() {
                body[key] = default;
            }
        }
        check_batch_spec(&body)?;
        if c.is_some() && (!body["allow"].as_array().unwrap().is_empty() || !body["user"].is_null()) {
            return Err(fail(403, "batch host mount roots and runtime user require administrator credentials"));
        }
        body["namespace"] = json!(namespace);
        body.as_object_mut().unwrap().remove("name");
        let _ops = s.ops.lock().await;
        if s.inner.lock().apps.get(&name).is_some_and(|a| !a.removed) || s.fleet.app_known(&namespace, &name) {
            return Err(fail(409, format!("an app or batch named {name} exists")));
        }
        let app = WorkloadTemplate {
            name: name.clone(),
            namespace,
            tag,
            spec: body.into(),
            persistent: false,
            removed: false,
            workers: Vec::new(),
        };
        s.save("apps", &name, &app)?;
        s.inner.lock().apps.insert(name, app.clone());
        return Ok((StatusCode::CREATED, Json(s.batch_value(&app, app.tag.as_deref())),
        ).into_response());
    }
    if parts.first() == Some(&"apps") && parts.len() >= 2 {
        let name = parts[1];
        // A deleted batch stays readable while it has job records: they are its history.
        let readable_deleted = batch && method == Method::GET && parts.len() == 2 && s.has_jobs(name);
        let mut app = s
            .inner
            .lock()
            .apps
            .get(name)
            .filter(|a| (!a.removed || readable_deleted) && a.persistent != batch)
            .cloned()
            .ok_or_else(|| fail(404, if batch { "unknown batch" } else { "unknown app" }))?;
        WorkloadController::require_grant(c, name)?;
        if !s.app_visible(&app, c, &q) {
            return Err(fail(404, if batch { "unknown batch" } else { "unknown app" }));
        }
        let tag = c.map(|c| c.tag.as_deref().unwrap_or("")).or_else(|| q.get("tag").map(String::as_str));
        let view = |s: &WorkloadController, app: &WorkloadTemplate| if batch { s.batch_value(app, tag) } else { s.serving_app_value(app, tag) };
        if method == Method::GET && parts.len() == 2 {
            return Ok(Json(view(&s, &app)).into_response());
        }
        if method == Method::DELETE && parts.len() == 2 {
            let _ops = s.ops.lock().await;
            s.ensure_not_scaling(&app.namespace, name)?;
            let persistent_ready_to_remove = !app.persistent
                || (app.spec["disabled"].as_bool() == Some(true)
                    && app.workers.iter().all(|id| {
                        s.inner.lock().workers.get(id).is_some_and(|w| {
                            now() - w.seen < 30.0
                                && w.report.services.iter().any(|v| {
                                    text(v, "name") == name && v["cleaned"].as_bool() == Some(true)
                                })
                        })
                    }));
            if !persistent_ready_to_remove
                || s.inner
                    .lock()
                    .jobs
                    .values()
                    .any(|r| r.batch == name && (!terminal(r.state()) || r.quota_held))
            {
                return Err(fail(
                    409,
                    "disable persistent services or finish/cancel jobs before deleting",
                ));
            }
            app.removed = true;
            s.save("apps", name, &app)?;
            s.inner.lock().apps.insert(name.into(), app.clone());
            if app.persistent {
                s.remove_app(&app.namespace, name)?;
            } else {
                s.tell_nodes(&app.workers, json!({"batch":{"name":name}}), json!({"remove":true}))?;
            }
            return Ok(Json(json!({"name":name,"removed":true})).into_response());
        }
        if batch && parts.len() == 3 && parts[2] == "settings" && method == Method::PUT {
            let set: Value = serde_json::from_slice(&to_bytes(request.into_body(), 1 << 20).await?)?;
            let set = set.as_object().filter(|o| !o.is_empty()).cloned()
                .ok_or_else(|| fail(400, "set at least one setting"))?;
            if let Some(key) = set.keys().find(|k| !BATCH_SETTINGS.contains(&k.as_str())) {
                return Err(fail(400, format!("{key} cannot be updated; settings: {}", BATCH_SETTINGS.join(", "))));
            }
            WorkloadController::require_admin_settings(c, &set, &BATCH_ADMIN_SETTINGS)?;
            let _ops = s.ops.lock().await;
            app = s.inner.lock().apps.get(name).filter(|a| !a.removed && !a.persistent).cloned()
                .ok_or_else(|| fail(404, "unknown batch"))?;
            for (k, v) in &set {
                app.spec[k.as_str()] = v.clone();
            }
            check_batch_spec(&app.spec)?;
            s.save("apps", name, &app)?;
            s.inner.lock().apps.insert(name.into(), app.clone());
            // Jobs already accepted keep the settings they were submitted with.
            s.tell_nodes(&app.workers, json!({"batch":{"name":name}}), json!({"set": set}))?;
            return Ok(Json(s.batch_value(&app, tag)).into_response());
        }
        if parts.len() == 3 && parts[2] == "priority" && method == Method::PUT {
            let p = priority_of(request).await?;
            let _ops = s.ops.lock().await;
            app = s
                .inner
                .lock()
                .apps
                .get(name)
                .filter(|a| !a.removed && a.persistent != batch)
                .cloned()
                .ok_or_else(|| fail(404, "unknown app"))?;
            app.spec["priority"] = json!(p);
            s.save("apps", name, &app)?;
            s.inner.lock().apps.insert(name.into(), app.clone());
            return Ok(Json(view(&s, &app)).into_response());
        }
        if parts.len() == 3
            && parts[2] == "disabled"
            && matches!(method, Method::PUT | Method::DELETE)
        {
            let disabled = method == Method::PUT;
            let ids = {
                let _ops = s.ops.lock().await;
                app = s
                    .inner
                    .lock()
                    .apps
                    .get(name)
                    .filter(|a| !a.removed && a.persistent != batch)
                    .cloned()
                    .ok_or_else(|| fail(404, "unknown app"))?;
                app.spec["disabled"] = json!(disabled);
                s.save("apps", name, &app)?;
                s.inner.lock().apps.insert(name.into(), app.clone());
                if app.persistent {
                    // Every deployment changes; an offline node's waits until it reconnects.
                    let keys = s.app_services(&app.namespace, name);
                    s.begin_toggle(&keys, disabled)?;
                    s.online(&keys)
                } else {
                    vec![]
                }
            };
            s.wait_scales(&ids).await?;
            app = s.inner.lock().apps.get(name).cloned().unwrap_or(app);
            return Ok(Json(view(&s, &app)).into_response());
        }
    }
    if batch_jobs {
        let batch = parts[1];
        WorkloadController::require_grant(c, batch)?;
        if method == Method::GET && parts.len() == 3 {
            let limit = q
                .get("limit")
                .and_then(|n| n.parse::<usize>().ok())
                .unwrap_or(50)
                .min(10000);
            let active = q.get("active").is_some_and(|a| a == "1" || a == "true");
            let mut jobs = {
                s.inner
                    .lock()
                    .jobs
                    .values()
                    .filter(|r| {
                        !r.purged
                            && r.batch == batch
                            && s.visible(&r.namespace, r.tag.as_deref(), c, &q)
                            && (!active || !terminal(r.state()))
                    })
                    .cloned()
                    .collect::<Vec<_>>()
            };
            jobs.sort_by(|a, b| {
                b.status["submitted_at"]
                    .as_f64()
                    .unwrap_or(0.0)
                    .total_cmp(&a.status["submitted_at"].as_f64().unwrap_or(0.0))
            });
            return Ok(Json(
                jobs.into_iter()
                    .take(limit)
                    .map(|r| r.value())
                    .collect::<Vec<_>>(),
            )
            .into_response());
        }
        if method == Method::DELETE && parts.len() == 3 {
            let days = q
                .get("older_than")
                .and_then(|s| s.parse::<f64>().ok())
                .filter(|v| v.is_finite() && *v >= 0.0)
                .ok_or_else(|| fail(400, "older_than must be non-negative days"))?;
            let _ops = s.ops.lock().await;
            let jobs = {
                s.inner
                    .lock()
                    .jobs
                    .values()
                    .filter(|r| {
                        !r.purged
                            && r.batch == batch
                            && terminal(r.state())
                            && !r.quota_held
                            && r.status["finished_at"]
                                .as_f64()
                                .is_some_and(|v| v < now() - days * 86400.0)
                            && s.visible(&r.namespace, r.tag.as_deref(), c, &q)
                    })
                    .cloned()
                    .collect::<Vec<_>>()
            };
            return Ok(Json(
                jobs.into_iter()
                    .map(|r| s.purge(r))
                    .collect::<anyhow::Result<Vec<_>>>()?,
            )
            .into_response());
        }
        if method == Method::POST && parts.len() == 3 {
            let body = serde_json::from_slice(&to_bytes(request.into_body(), 1 << 20).await?)?;
            return Ok((StatusCode::CREATED, Json(s.submit(batch, body, c, &q).await?)).into_response());
        }
        if parts.len() >= 4 {
            let id = parts[3];
            let job = |s: &WorkloadController| {
                Some(s.job(id, c, &q)?).filter(|r| r.batch == batch).ok_or_else(|| fail(404, "unknown batch job"))
            };
            let r = job(&s)?;
            if method == Method::GET && parts.len() == 4 {
                // One job also shows the submission it was accepted with; listings stay status only.
                let mut v = r.value();
                v["request"] = r.request.0.clone();
                return Ok(Json(v).into_response());
            }
            if method == Method::POST && parts.len() == 5 && matches!(parts[4], "cancel" | "preempt") {
                let preempt = parts[4] == "preempt";
                let _ops = s.ops.lock().await;
                let r = job(&s)?;
                return Ok(Json(s.stop_job(r, preempt).await?.value()).into_response());
            }
            if method == Method::DELETE && parts.len() == 4 {
                let _ops = s.ops.lock().await;
                let r = job(&s)?;
                return Ok(Json(vec![s.purge(r)?]).into_response());
            }
            if method == Method::GET && parts.len() == 5 && parts[4] == "logs" {
                return log_response(&s, &r, &q).await;
            }
        }
    }
    Err(fail(404, "unknown workload API operation"))
}

/// A `{"priority": N}` body's priority.
async fn priority_of(request: Request) -> anyhow::Result<i32> {
    let body: Value = serde_json::from_slice(&to_bytes(request.into_body(), 1 << 20).await?)?;
    body["priority"]
        .as_i64()
        .and_then(|p| i32::try_from(p).ok())
        .ok_or_else(|| fail(400, "priority must be a signed 32-bit integer"))
}

impl WorkloadController {
    /// The keys of `app`'s service records in `namespace`.
    pub(crate) fn app_services(&self, namespace: &str, app: &str) -> Vec<String> {
        self.inner.lock().services.iter()
            .filter(|(_, d)| text(&d.service, "app") == app && text(&d.service, "namespace") == namespace)
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// The registered proof app `name` the caller selects: in its namespace (a token's, or the
    /// one `?namespace=` gives), or for an administrator who gives none, the one of that name.
    fn proof_app(&self, c: Option<&Token>, q: &BTreeMap<String, String>, name: &str) -> anyhow::Result<Option<crate::store::App>> {
        if c.is_some() || q.contains_key("namespace") {
            let (namespace, _) = self.scope(c, q, None)?;
            return Ok(self.fleet.app(&namespace, name));
        }
        let all: Vec<_> = self.fleet.namespaces().into_iter().filter_map(|n| self.fleet.app(&n.id, name)).collect();
        match all.len() {
            0 | 1 => Ok(all.into_iter().next()),
            n => Err(fail(400, format!("{n} namespaces have an app {name}: select one with --namespace"))),
        }
    }

    /// The registered app `name` of the namespace as it is now, with its job counts.
    async fn stored_proof_app(&self, namespace: &str, name: &str, tag: Option<&str>) -> anyhow::Result<Value> {
        let (app, anchored) = self.fleet.apps().await?.into_iter()
            .find(|(a, _)| a.namespace == namespace && a.name == name)
            .ok_or_else(|| fail(404, "unknown app"))?;
        self.proof_app_value(&app, anchored.as_deref(), tag).await
    }

    /// A scale, logs or settings request for `app`'s services.
    #[allow(clippy::too_many_arguments)]
    async fn service_action(&self, c: Option<&Token>, q: &BTreeMap<String, String>, namespace: &str, app: &str,
        action: &str, method: &Method, request: Request,
    ) -> anyhow::Result<Response> {
        if action == "logs" {
            if method != Method::GET {
                return Err(fail(405, "GET required"));
            }
            return self.app_logs(c, namespace, app, q).await;
        }
        if action == "settings" {
            if method != Method::PUT {
                return Err(fail(405, "PUT required"));
            }
            let node = q.get("node").map(String::as_str);
            self.require_service_scope(c, namespace, app, node)?;
            let set: Value = serde_json::from_slice(&to_bytes(request.into_body(), 1 << 20).await?)?;
            if let Some(settings) = set.as_object() {
                Self::require_admin_settings(c, settings, &Self::ADMIN_SERVICE_SETTINGS)?;
            }
            return Ok((StatusCode::ACCEPTED, Json(self.request_update(namespace, app, node, set).await?)).into_response());
        }
        if method == Method::GET {
            self.require_service_scope(c, namespace, app, q.get("node").map(String::as_str))?;
            return Ok(Json(self.scale_status(namespace, app, q.get("node").map(String::as_str))?).into_response());
        }
        if method == Method::PUT {
            let body: fleet_proto::model::v1::ScaleRequest = serde_json::from_slice(&to_bytes(request.into_body(), 4096).await?)?;
            self.require_service_scope(c, namespace, app, body.node.as_deref())?;
            return Ok((StatusCode::ACCEPTED, Json(self.request_scale(namespace, app, body).await?)).into_response());
        }
        Err(fail(405, "GET or PUT required"))
    }

    /// A request for a registered proof app: `rest` is the path after `/apps/NAME`.
    async fn proof_app_request(&self, c: Option<&Token>, q: &BTreeMap<String, String>, app: crate::store::App,
        method: &Method, rest: &[&str], request: Request,
    ) -> anyhow::Result<Response> {
        WorkloadController::require_grant(c, &app.name)?;
        let tag = c.map(|c| c.tag.clone().unwrap_or_default()).or_else(|| q.get("tag").cloned());
        if c.is_some_and(|c| app.namespace != c.namespace.clone().unwrap_or_default()) {
            return Err(fail(404, "unknown app"));
        }
        let (namespace, name) = (app.namespace.clone(), app.name.clone());
        // Changing the app changes every service that runs it: a token's tag must cover them.
        let changes = !matches!((method, rest), (&Method::GET, _) | (&Method::POST, ["jobs"]) | (_, ["scale" | "logs" | "settings"]));
        if changes {
            if let Some(c) = c {
                let i = self.inner.lock();
                if i.services.values().any(|d| text(&d.service, "namespace") == namespace && text(&d.service, "app") == name
                    && d.service["tag"].as_str().is_some_and(|tag| c.tag.as_deref() != Some(tag))) {
                    return Err(fail(403, "another tag's services run this app"));
                }
            }
        }
        match (method, rest) {
            (&Method::GET, []) => Ok(Json(self.stored_proof_app(&namespace, &name, tag.as_deref()).await?).into_response()),
            (&Method::DELETE, []) => {
                let _guard = self.app_lifecycle_guard(&namespace, &name).await?;
                let deleted = self.fleet.delete_app(&namespace, &name).await?;
                self.remove_app(&namespace, &name)?;
                Ok(Json(deleted).into_response())
            }
            (&Method::PUT, ["priority"]) => {
                let p = priority_of(request).await?;
                Ok(Json(self.fleet.set_app_priority(&namespace, &name, p).await?).into_response())
            }
            (&Method::PUT | &Method::DELETE, ["disabled"]) => {
                let disabled = method == Method::PUT;
                // Every deployment changes; an offline node's waits until it reconnects.
                let keys = self.app_services(&namespace, &name);
                {
                    let _ops = self.ops.lock().await;
                    // Refused while another change is under way, before anything changes.
                    self.begin_toggle(&keys, disabled)?;
                    // Its jobs are refused until its prover is up again, which re-enables it.
                    self.fleet.set_app_disabled(&namespace, &name, disabled || !keys.is_empty()).await?;
                }
                self.wait_scales(&self.online(&keys)).await?;
                Ok(Json(self.stored_proof_app(&namespace, &name, tag.as_deref()).await?).into_response())
            }
            (_, [action]) if matches!(*action, "scale" | "logs" | "settings") => {
                self.service_action(c, q, &namespace, &name, action, method, request).await
            }
            (_, ["jobs", jobs @ ..]) => {
                crate::proof_api::app_jobs(self, c, q, &app, tag.as_deref(), method, jobs, request).await
            }
            _ => Err(fail(404, "unknown app operation")),
        }
    }
}
async fn log_response(
    s: &WorkloadController,
    r: &Job,
    q: &BTreeMap<String, String>,
) -> anyhow::Result<Response> {
    if q.get("rank").is_some_and(|rank| rank != "0") {
        return Err(fail(400, "batch logs are local to one machine; only rank 0 exists",
        ));
    }
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    let path = s
        .dir
        .join("artifacts")
        .join(&r.id)
        .join(&r.lease)
        .join("logs");
    let mut file = match tokio::fs::File::open(path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(
                Json(json!({"data":"","offset":0,"done":terminal(r.state())&&r.logs_ready}))
                    .into_response(),
            )
        }
        Err(e) => return Err(e.into()),
    };
    let length = file.metadata().await?.len();
    let tail = q.get("offset").is_none();
    let offset = q
        .get("offset")
        .and_then(|o| o.parse::<u64>().ok())
        .unwrap_or(length.saturating_sub(256 << 10))
        .min(length);
    file.seek(std::io::SeekFrom::Start(offset)).await?;
    let mut bytes = vec![];
    file.take(1 << 20).read_to_end(&mut bytes).await?;
    let end = offset + bytes.len() as u64;
    let mut data = String::from_utf8_lossy(&bytes).into_owned();
    if tail {
        let lines = q
            .get("lines")
            .and_then(|n| n.parse::<usize>().ok())
            .unwrap_or(50)
            .min(10000);
        let chunks = data.lines().collect::<Vec<_>>();
        data = chunks[chunks.len().saturating_sub(lines)..].join("\n");
        if !data.is_empty() {
            data.push('\n');
        }
    }
    Ok(Json(
        json!({"data":data,"offset":end,"done":terminal(r.state())&&r.logs_ready&&end==length}),
    )
    .into_response())
}
async fn internal_logs(
    s: Arc<WorkloadController>,
    request: Request,
    parts: &[&str],
    q: &BTreeMap<String, String>,
) -> anyhow::Result<Response> {
    if !token::bearer_matches(
        request
            .headers()
            .get(token::HEADER)
            .and_then(|v| v.to_str().ok()),
        &s.cluster,
    ) {
        return Err(fail(401, "internal token required"));
    }
    if parts.len() != 4 || parts[1] != "jobs" {
        return Err(fail(404, "unknown artifact"));
    }
    let id = parts[2];
    let name = parts[3];
    let r = s
        .inner
        .lock()
        .jobs
        .get(id)
        .cloned()
        .ok_or_else(|| fail(404, "unknown job"))?;
    if request
        .headers()
        .get("x-fleet-worker")
        .and_then(|v| v.to_str().ok())
        != r.worker.as_deref()
        || request
            .headers()
            .get("x-fleet-lease")
            .and_then(|v| v.to_str().ok())
            != Some(r.lease.as_str())
    {
        return Err(fail(409, "stale or wrong execution lease"));
    }
    if !matches!(name, "logs" | "logs-offset") {
        return Err(fail(404, "unknown log operation"));
    }
    let path = s.dir.join("artifacts").join(id).join(&r.lease).join("logs");
    if request.method() == Method::GET && name == "logs-offset" {
        return Ok(Json(
            json!({"offset":tokio::fs::metadata(path).await.map(|m|m.len()).unwrap_or(0)}),
        )
        .into_response());
    }
    if request.method() != Method::PUT {
        return Err(fail(405, "PUT required"));
    }
    if name == "logs" {
        use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
        let _ops = s.ops.lock().await;
        tokio::fs::create_dir_all(path.parent().unwrap()).await?;
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .await?;
        let offset = q
            .get("offset")
            .and_then(|n| n.parse::<u64>().ok())
            .ok_or_else(|| fail(400, "offset required"))?;
        let bytes = to_bytes(request.into_body(), 1 << 20).await?;
        let length = file.metadata().await?.len();
        if offset > length {
            return Err(fail(409, "log offset mismatch"));
        }
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        if offset < length {
            let mut existing = vec![0; bytes.len()];
            file.read_exact(&mut existing).await?;
            if existing != bytes {
                return Err(fail(409, "log contents conflict"));
            }
        } else {
            file.write_all(&bytes).await?;
            file.sync_all().await?;
        }
        return Ok(Json(json!({"offset":offset+bytes.len() as u64})).into_response());
    }
    Err(fail(405, "unsupported log operation"))
}

#[cfg(test)]
#[path = "workloads_tests.rs"]
mod tests;

#[path = "workloads_scale.rs"]
mod scaling;

#[path = "workloads_views.rs"]
mod views;
