//! App-owner desired resource settings, reconciled through unary heartbeats.
use super::*;

use fleet_proto::model::v1::{ScalePhase as Phase, ScaleRequest, ScaleStatus};

/// A change to one deployment: drain, stop, have the node write the target configuration
/// (NODE_OPERATION_APP), start it again unless `disabled`.
pub(super) fn new_scale(target: impl Into<Value>, disabled: bool) -> ScaleStatus {
    ScaleStatus {
            id: uuid::Uuid::new_v4().to_string(),
            target: target.into().into(),
            disabled,
            phase: Phase::Draining as i32,
            stop: None,
            start: None,
            error: None,
            ready_since: None,
            set: None,
            update: None,
        }
}

/// A change whose start still waits for capacity has nothing on the node to protect (its old
/// server is stopped and the start was never sent): a newer change replaces it.
fn waiting_for_capacity(i: &WorkloadState, intent: &AppDeployment) -> bool {
    intent.scale.as_ref().is_some_and(|s| {
        s.phase() == Phase::Starting
            && s.start.as_ref().is_some_and(|id| i.commands.get(id).is_some_and(|c| !c.done && !c.admitted))
    })
}

/// Whether `intent` is leaving its node: disabled, and its node told to remove its app's
/// entries, or no longer configuring it. It is no longer one of its app's deployments, and its
/// record goes once nothing about it is open.
pub(super) fn retiring(i: &WorkloadState, intent: &AppDeployment) -> bool {
    intent.disabled
        && (i.commands.open().any(|c| c.worker == intent.worker && c.removes_app(intent.entry().app()))
            || i.workers.get(&intent.worker).is_some_and(|w| w.report.entry(intent.entry().name()).is_none()))
}

/// The entry `intent` has once the change under way, if any, is done.
fn planned(intent: &AppDeployment) -> Entry<'_> {
    intent.scale.as_ref().filter(|s| s.pending()).map(ScaleStatus::target_entry).unwrap_or(intent.entry())
}

/// Whether `intent`'s replicas are to run: its change under way starts them, or, without one,
/// its node was last told to run them.
fn wants_running(intent: &AppDeployment) -> bool {
    intent.scale.as_ref().filter(|s| s.pending()).map_or(!intent.disabled, |s| !s.disabled)
}

/// Whether `intent`'s replicas are up, on a connected node.
fn running_now(i: &WorkloadState, intent: &AppDeployment) -> bool {
    !intent.disabled && i.workers.get(&intent.worker).is_some_and(|w| {
        now() - w.seen < 30.0 && w.report.ready
            && w.report.entry(intent.entry().name()).is_some_and(|e| e.ready() && !e.disabled())
    })
}

/// Where deployment `key` comes in the order an app's replicas are kept in: those running now
/// first, then those that are to run, then the rest, by node within each.
fn rank(i: &WorkloadState, key: &str, intent: &AppDeployment) -> (bool, bool, String) {
    (!running_now(i, intent), !wants_running(intent), key.to_owned())
}

const REPLICA_RANGE: &str = "gpus must be nonnegative, cpus positive, and replicas between 1 and 64; use app disable to stop all replicas";

impl WorkloadController {
    pub(crate) async fn app_lifecycle_guard(
        &self,
        namespace: &str,
        app: &str,
    ) -> anyhow::Result<tokio::sync::MutexGuard<'_, ()>> {
        let guard = self.ops.lock().await;
        self.ensure_not_scaling(namespace, app)?;
        Ok(guard)
    }

    pub(super) fn ensure_not_scaling(&self, namespace: &str, app: &str) -> anyhow::Result<()> {
        if self.inner.lock().deployments.values().any(|s| {
            s.entry().namespace() == namespace && s.entry().app() == app && s.scale.as_ref().is_some_and(ScaleStatus::pending)
        }) {
            return Err(fail(409, "app scaling is already pending"));
        }
        Ok(())
    }

    /// `app`'s deployments in `namespace` (on `node`, when given) but those being retired, in
    /// the order its replicas are kept in (`rank`).
    fn app_deployment_list(&self, namespace: &str, app: &str, node: Option<&str>) -> Vec<(String, AppDeployment)> {
        let i = self.inner.lock();
        let mut list: Vec<(String, AppDeployment)> = i.deployments.iter()
            .filter(|(_, intent)| {
                let s = intent.entry();
                s.namespace() == namespace
                    && node.is_none_or(|n| n == intent.worker)
                    && matches!(s.kind(), "ziren-gpu" | "app")
                    && s.app() == app
                    && !retiring(&i, intent)
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        list.sort_by_cached_key(|(key, intent)| rank(&i, key, intent));
        list
    }

    pub(super) fn app_deployment(
        &self,
        namespace: &str,
        app: &str,
        node: Option<&str>,
    ) -> anyhow::Result<(String, AppDeployment)> {
        let mut deployments = self.app_deployment_list(namespace, app, node).into_iter();
        let first = deployments
            .next()
            .ok_or_else(|| fail(404, "the app runs on no node (enable it to place it), or not on that one"))?;
        if deployments.next().is_some() {
            return Err(fail(400, "the app runs on several nodes; select --node NODE"));
        }
        Ok(first)
    }

    fn scale_value(&self, intent: &AppDeployment) -> Value {
        let i = self.inner.lock();
        let observed = i.workers.get(&intent.worker).and_then(|w| w.report.entry(intent.entry().name())).map(|e| e.0);
        let target = intent.scale.as_ref().map(ScaleStatus::target_entry).unwrap_or(intent.entry());
        json!({"node":intent.worker,"entry":intent.entry().name(),
            "namespace":intent.entry().namespace(),
            "gpus":target.gpus().unwrap_or(1.0),"cpus":target.cpus(),
            "replicas":target.replicas(),
            "state":intent.scale.as_ref().map(|s| serde_json::to_value(&s.phase()).unwrap()).unwrap_or(json!("configured")),
            "disabled":intent.scale.as_ref().map(|s|s.disabled).unwrap_or(intent.disabled),
            "operation":intent.scale.as_ref().map(|s|s.id.clone()),
            "error":intent.scale.as_ref().and_then(|s|s.error.clone()),
            "observed":observed.map(|s|json!({"gpus":s["gpus"],"cpus":s["cpus"],"replicas":s["replicas"],"ready":s["ready"],"disabled":s["disabled"]}))})
    }

    /// Reads the last `lines` lines of the app's log from the node that runs it,
    /// through that node's heartbeat.
    pub(super) async fn app_logs(
        &self,
        c: Option<&Token>,
        namespace: &str,
        app: &str,
        q: &BTreeMap<String, String>,
    ) -> anyhow::Result<Response> {
        let node = q.get("node").map(String::as_str);
        self.require_deployment_scope(c, namespace, app, node)?;
        let (_, deployment) = self.app_deployment(namespace, app, node)?;
        let lines = match q.get("lines") {
            Some(v) => v.parse::<u32>().map_err(|_| fail(400, "lines must be a number"))?,
            None => 100,
        };
        let bytes = self
            .fleet
            .log_relay
            .request(&deployment.worker, deployment.entry().name(), lines)
            .await
            .map_err(|e| fail(fleet_proto::sdk::http_status_code(e.code()), e.message().to_owned()))?;
        Ok(bytes.into_response())
    }

    /// The app's replicas and their resources, with each node's part; one node's part when
    /// `node` is given.
    pub(super) fn scale_status(
        &self,
        namespace: &str,
        app: &str,
        node: Option<&str>,
    ) -> anyhow::Result<Value> {
        if node.is_some() {
            let (_, deployment) = self.app_deployment(namespace, app, node)?;
            return Ok(self.scale_value(&deployment));
        }
        let deployments = self.app_deployment_list(namespace, app, None);
        if deployments.is_empty() {
            return Err(fail(404, "the app runs on no node (enable it to place it)"));
        }
        Ok(self.app_scale_value(&deployments))
    }

    /// An app's scale: its replicas over all its deployments, their resources as the first
    /// one has them, and its most pressing state, a failure first, then a change under way.
    fn app_scale_value(&self, deployments: &[(String, AppDeployment)]) -> Value {
        let parts: Vec<Value> = deployments.iter().map(|(_, d)| self.scale_value(d)).collect();
        let state = parts.iter().map(|p| &p["state"]).find(|s| *s == "failed")
            .or_else(|| parts.iter().map(|p| &p["state"]).find(|s| !matches!(s.as_str(), Some("complete" | "configured"))))
            .unwrap_or(&parts[0]["state"]);
        json!({"namespace": parts[0]["namespace"], "replicas": parts.iter().filter_map(|p| p["replicas"].as_u64()).sum::<u64>(),
            "gpus": parts[0]["gpus"], "cpus": parts[0]["cpus"], "state": state,
            "disabled": parts.iter().all(|p| p["disabled"] == json!(true)),
            "error": parts.iter().find_map(|p| p["error"].as_str()),
            "nodes": parts.iter().map(|p| p["node"].clone()).collect::<Vec<_>>(), "deployments": parts})
    }

    /// The deployment of `app` (on `node`, when it has several) that a scale or settings update
    /// changes: the app still exists and no other operation on the deployment is under way.
    fn deployment_to_change(&self, namespace: &str, app: &str, node: Option<&str>) -> anyhow::Result<(String, AppDeployment)> {
        let (key, intent) = self.app_deployment(namespace, app, node)?;
        self.check_changeable(namespace, app, &[(key.clone(), intent.clone())])?;
        Ok((key, intent))
    }

    /// Every deployment of `app` a scale changes, in the order its replicas are kept in.
    fn deployments_to_change(&self, namespace: &str, app: &str) -> anyhow::Result<Vec<(String, AppDeployment)>> {
        let deployments = self.app_deployment_list(namespace, app, None);
        if deployments.is_empty() {
            return Err(fail(404, "the app runs on no node (enable it to place it)"));
        }
        self.check_changeable(namespace, app, &deployments)?;
        Ok(deployments)
    }

    /// The app still exists and no other operation on these deployments of it is under way.
    fn check_changeable(&self, namespace: &str, app: &str, deployments: &[(String, AppDeployment)]) -> anyhow::Result<()> {
        // Recheck existence under the operation lock: deletion may have finished
        // after the HTTP handler resolved the app but before this request acquired it.
        let exists = self.fleet.app_known(namespace, app)
            || self.inner
                .lock()
                .apps
                .get(app)
                .is_some_and(|a| !a.removed && a.persistent && a.namespace == namespace);
        if !exists {
            return Err(fail(404, "unknown app"));
        }
        let i = self.inner.lock();
        for (_, intent) in deployments {
            if intent.scale.as_ref().is_some_and(ScaleStatus::pending) && !waiting_for_capacity(&i, intent)
                // one still waiting for capacity is replaced by this request
                || i.commands.open().any(|c| c.admitted && c.changes_entry(&intent.worker, intent.entry().name()))
            {
                return Err(fail(409, "an app operation is already pending"));
            }
        }
        Ok(())
    }

    /// Settings an update may change on an app. The rest is identity, placement or the
    /// node's own configuration, or has its own operation (scale, enable/disable, priority).
    /// `machine` anchors a container app to the machine it runs on, and changes nothing else.
    pub(super) const APP_SETTINGS: [&'static str; 12] = ["image", "env", "command", "user", "workdir", "volumes",
        "shm_gb", "memory_gb", "health_path", "stop_timeout_s", "ready_timeout_s", "machine"];
    // Apps can access host networking, storage and prover credentials. Changing
    // their executable or environment is as privileged as changing their mounts.
    pub(super) const ADMIN_APP_SETTINGS: [&'static str; 6] = ["image", "env", "command", "user", "workdir", "volumes"];
    // A namespace's owner creates its own apps, as it does its batches: the host's
    // filesystem and user ids stay administrator settings.
    const ADMIN_CREATE_SETTINGS: [&'static str; 3] = ["user", "volumes", "proof_storage_root"];

    /// Starts enabling or disabling these deployments of `app` the way a scale changes them:
    /// drain, stop, the node writes the enabled state, start again unless disabled. The caller
    /// holds the operation lock.
    pub(super) fn begin_toggle(&self, keys: &[String], disabled: bool) -> anyhow::Result<()> {
        for key in keys {
            let mut intent = self.inner.lock().deployments.get(key).cloned()
                .ok_or_else(|| fail(404, "no deployment of this app"))?;
            let busy = {
                let i = self.inner.lock();
                intent.scale.as_ref().is_some_and(ScaleStatus::pending) && !waiting_for_capacity(&i, &intent)
                    || i.commands.open().any(|c| c.admitted && c.changes_entry(&intent.worker, intent.entry().name()))
            };
            if busy {
                return Err(fail(409, "an app operation is already pending"));
            }
            intent.scale = Some(new_scale(self.with_priority(intent.spec.clone()), disabled));
            self.save("deployments", key, &intent)?;
            self.inner.lock().deployments.insert(key.clone(), intent);
        }
        self.notify.notify_waiters();
        Ok(())
    }

    /// An app entry as a change targets it: with its app's current priority, which only the
    /// scheduler keeps (a proof app's record, or the app's template).
    pub(super) fn with_priority(&self, entry: impl Into<Value>) -> Value {
        let mut entry: Value = entry.into();
        let (namespace, app) = (Entry(&entry).namespace(), Entry(&entry).app());
        let priority = if self.fleet.app_known(namespace, app) {
            self.fleet.app_priority(namespace, app).into()
        } else {
            self.inner.lock().apps.get(app).filter(|a| a.persistent).map_or(0, |a| a.entry().priority())
        };
        EntryMut(&mut entry).set_priority(priority);
        entry
    }

    /// Waits until the changes of these deployments finished; a failed one is the error.
    pub(super) async fn wait_scales(&self, keys: &[String]) -> anyhow::Result<()> {
        tokio::time::timeout(Duration::from_secs(1800), async {
            loop {
                let notified = self.notify.notified();
                let scales: Vec<Option<ScaleStatus>> = {
                    let i = self.inner.lock();
                    keys.iter().map(|k| i.deployments.get(k).and_then(|d| d.scale.clone())).collect()
                };
                if let Some(error) = scales.iter().flatten().find(|s| s.failed()).map(|s| s.error.clone().unwrap_or_default()) {
                    return Err(fail(503, error));
                }
                if !scales.iter().flatten().any(ScaleStatus::pending) {
                    return Ok(());
                }
                notified.await;
            }
        })
        .await
        .map_err(|_| fail(503, "node has not confirmed the operation; desired state is retained"))?
    }

    pub(super) async fn request_update(&self, namespace: &str, app: &str, node: Option<&str>, set: Value) -> anyhow::Result<Value> {
        let _ops = self.ops.lock().await;
        let set = set.as_object().filter(|o| !o.is_empty()).cloned()
            .ok_or_else(|| fail(400, "set at least one setting"))?;
        if let Some(machine) = set.get("machine") {
            if set.len() > 1 {
                return Err(fail(400, "set machine on its own: anchoring an app changes nothing else and doesn't restart it"));
            }
            let machine = machine.as_str().ok_or_else(|| fail(400, "machine: invalid value"))?;
            return self.anchor(namespace, app, node, machine);
        }
        for (key, value) in &set {
            let valid = match key.as_str() {
                "image" => value.as_str().is_some_and(|v| !v.is_empty() && !v.starts_with('-')),
                "env" => value.as_object().is_some_and(|o| o.values().all(Value::is_string)),
                "command" | "volumes" => value.as_array().is_some_and(|a| a.iter().all(Value::is_string)),
                "user" | "workdir" | "health_path" => value.is_string(),
                "shm_gb" | "stop_timeout_s" => value.as_u64().is_some(),
                "memory_gb" | "ready_timeout_s" => value.as_f64().is_some_and(|v| v.is_finite() && v > 0.0),
                _ => return Err(fail(400, format!("{key} cannot be updated; settings: {}", Self::APP_SETTINGS.join(", ")))),
            };
            if !valid {
                return Err(fail(400, format!("{key}: invalid value")));
            }
        }
        let (key, mut intent) = self.deployment_to_change(namespace, app, node)?;
        if set.iter().all(|(k, v)| intent.spec.get(k) == Some(v)) {
            return Ok(self.scale_value(&intent));
        }
        let mut target = self.with_priority(intent.spec.clone());
        for (k, v) in &set {
            target[k.as_str()] = v.clone();
        }
        let disabled = intent.scale.as_ref().filter(|s| s.phase() == Phase::Failed).map(|s| s.disabled).unwrap_or(intent.disabled);
        let mut scale = new_scale(target, disabled);
        scale.set = Some(Value::Object(set).into());
        intent.scale = Some(scale);
        // Persist the transition before changing admission or sending any command.
        self.save("deployments", &key, &intent)?;
        self.inner.lock().deployments.insert(key, intent.clone());
        self.notify.notify_waiters();
        Ok(self.scale_value(&intent))
    }

    /// Anchors `app`'s deployment on `node` (its only one, without a node) to `machine`, the
    /// machine it runs on, or lifts the anchor with "". A start of an anchored app waits for
    /// room on its node and is never moved to another, so an app that keeps its data on its
    /// node's disks stays with it. Only the node's configuration changes: the app runs on.
    /// The caller holds the operation lock.
    fn anchor(&self, namespace: &str, app: &str, node: Option<&str>, machine: &str) -> anyhow::Result<Value> {
        let (key, mut intent) = self.deployment_to_change(namespace, app, node)?;
        if !intent.entry().is_container() {
            return Err(fail(400, "only a container app is anchored this way: a prover's node configures its own machine"));
        }
        let on = self.inner.lock().workers.get(&intent.worker).map(|w| w.report.machine.clone()).unwrap_or_default();
        if !machine.is_empty() && machine != on {
            return Err(fail(409, format!(
                "{app} runs on node {} (machine {on}): it can be anchored only to the machine it runs on", intent.worker)));
        }
        if intent.entry().machine() == machine {
            return Ok(self.scale_value(&intent));
        }
        let name = intent.entry().name().to_owned();
        self.tell_nodes([&intent.worker], message::entry(&name), message::set(json!({"machine": machine})))?;
        intent.spec["machine"] = json!(machine);
        self.save("deployments", &key, &intent)?;
        self.inner.lock().deployments.insert(key, intent.clone());
        // Placing the app anew follows the anchor too.
        let template = self.inner.lock().apps.get(app).filter(|a| a.persistent && !a.removed && a.namespace == namespace).cloned();
        if let Some(mut template) = template {
            template.spec["machine"] = json!(machine);
            self.save("apps", app, &template)?;
            self.inner.lock().apps.insert(app.into(), template);
        }
        tracing::info!(app, node = intent.worker, machine, "anchoring an app to the machine it runs on");
        Ok(self.scale_value(&intent))
    }

    /// Sets an app's replica count, and its replicas' GPUs and CPUs: the app's settings, not any
    /// node's. The count is split over the app's deployments in the order its replicas are kept
    /// in: each keeps the replicas it has, up to the count; new ones go to the first, whose start
    /// moves to a node with room for all of them when its own has none; one left with none is
    /// retired. Without a count, each deployment keeps its own.
    pub(super) async fn request_scale(
        &self,
        namespace: &str,
        app: &str,
        req: ScaleRequest,
    ) -> anyhow::Result<Value> {
        let _ops = self.ops.lock().await;
        if req.gpus.is_none() && req.cpus.is_none() && req.replicas.is_none() {
            return Err(fail(400, "set at least one of gpus, cpus or replicas"));
        }
        if req.node.is_some() {
            return Err(fail(400, "replicas, GPUs and CPUs are the app's settings: scale it without a node, and the scheduler places its replicas"));
        }
        if req.replicas.is_some_and(|r| r == 0 || r > 64) {
            return Err(fail(400, REPLICA_RANGE));
        }
        let deployments = self.deployments_to_change(namespace, app)?;
        let count = req.replicas.map_or_else(|| deployments.iter().map(|(_, d)| d.entry().replicas()).sum(), u64::from);
        let mut left = count;
        let mut plan: Vec<(String, AppDeployment, u64)> = deployments.into_iter().map(|(key, intent)| {
            let keep = intent.entry().replicas().min(left);
            left -= keep;
            (key, intent, keep)
        }).collect();
        plan[0].2 += left;
        let ns = self
            .fleet
            .namespace(namespace)
            .ok_or_else(|| fail(404, "unknown namespace"))?;
        if ns.suspended_ms.is_some() {
            return Err(fail(403, "namespace is suspended"));
        }
        let mut changes = Vec::new();
        let mut retired = Vec::new();
        let (mut quota_g, mut quota_cpu, mut tag) = (0.0, 0.0, None);
        for (key, intent, replicas) in plan {
            if replicas == 0 {
                retired.push(key);
                continue;
            }
            let mut target = self.with_priority(intent.spec.clone());
            EntryMut(&mut target).set_resources(req.gpus, req.cpus, Some(replicas as u32));
            let t = Entry(&target);
            let (g, cpu) = (t.gpus().unwrap_or(1.0), t.cpus());
            if t.integrated_worker() && replicas != 1 {
                return Err(fail(400, "integrated app workers require one machine-pinned replica"));
            }
            if !g.is_finite() || g < 0.0 || !cpu.is_finite() || cpu <= 0.0 {
                return Err(fail(400, REPLICA_RANGE));
            }
            if t.kind() == "ziren-gpu" && (g < 1.0 || g.fract() != 0.0) {
                return Err(fail(400, "a proof server requires a positive integer GPU count"));
            }
            // Container replicas reserve the whole app. Proof quota is charged per assigned
            // proof; extra replicas can remain idle while that quota is occupied.
            if t.holds_allocation() {
                (quota_g, quota_cpu) = (quota_g + g * replicas as f64, quota_cpu + cpu * replicas as f64);
            } else {
                (quota_g, quota_cpu) = (f64::max(quota_g, g), f64::max(quota_cpu, cpu));
            }
            tag.get_or_insert_with(|| t.tag().unwrap_or("").to_owned());
            if intent.entry().same_resources(t) && intent.scale.as_ref().is_none_or(|s| s.phase() != Phase::Failed) {
                continue;
            }
            self.check_node_room(&intent.worker, g * replicas as f64, cpu * replicas as f64, replicas)?;
            changes.push((key, intent, target));
        }
        let tag_quota = ns.tag_quotas.get(tag.as_deref().unwrap_or("")).cloned().unwrap_or_default();
        if !ns.quota.fits(quota_g, quota_cpu) || !tag_quota.fits(quota_g, quota_cpu) {
            return Err(fail(
                422,
                "requested allocation exceeds namespace or tag GPU/CPU quota",
            ));
        }
        for (key, mut intent, target) in changes {
            let disabled = intent
                .scale
                .as_ref()
                .filter(|s| s.phase() == Phase::Failed)
                .map(|s| s.disabled)
                .unwrap_or(intent.disabled);
            intent.scale = Some(new_scale(target, disabled));
            // Persist the transition before changing admission or sending any command.
            self.save("deployments", &key, &intent)?;
            self.inner.lock().deployments.insert(key, intent);
        }
        for key in &retired {
            self.retire(key)?;
        }
        if req.replicas.is_some() {
            self.set_app_replicas(namespace, app, count)?;
        }
        self.notify.notify_waiters();
        Ok(self.app_scale_value(&self.app_deployment_list(namespace, app, None)))
    }

    /// Refuses a change of `worker`'s deployment to `replicas` replicas of `gpus` GPUs and
    /// `cpus` CPUs in all when the node can't take it: it is offline, unready or draining, or
    /// smaller than that.
    fn check_node_room(&self, worker: &str, gpus: f64, cpus: f64, replicas: u64) -> anyhow::Result<()> {
        let i = self.inner.lock();
        let node = i
            .workers
            .get(worker)
            .filter(|w| now() - w.seen < 30.0 && w.report.ready && !w.draining)
            .ok_or_else(|| fail(503, format!("node {worker} is offline, unready or draining")))?;
        if gpus > node.report.total_gpus + 1e-9 || cpus > node.report.total_cpus + 1e-9 {
            return Err(fail(
                422,
                format!(
                    "requested allocation exceeds node {worker} capacity: requested {gpus} GPUs and {cpus} CPUs (replicas: {replicas}); node total {} GPUs and {} CPUs, currently free {} GPUs and {} CPUs",
                    node.report.total_gpus,
                    node.report.total_cpus,
                    node.report.free_gpus,
                    node.report.free_cpus,
                ),
            ));
        }
        Ok(())
    }

    /// Records `count` as the replica count of the app the scheduler keeps a record of.
    fn set_app_replicas(&self, namespace: &str, app: &str, count: u64) -> anyhow::Result<()> {
        let template = self.inner.lock().apps.get(app).filter(|a| a.persistent && !a.removed && a.namespace == namespace).cloned();
        if let Some(mut template) = template {
            template.spec["replicas"] = json!(count);
            self.save("apps", app, &template)?;
            self.inner.lock().apps.insert(app.into(), template);
        }
        Ok(())
    }

    /// The replicas `app` has over its deployments once their changes are done.
    fn app_replicas(&self, namespace: &str, app: &str) -> u64 {
        self.app_deployment_list(namespace, app, None).iter().map(|(_, d)| planned(d).replicas()).sum()
    }

    /// Whether starting `start`'s entry would run more replicas of a container app than its
    /// count: the app's other deployments, ahead of this one in the order its replicas are kept
    /// in, already run them all on connected nodes. Only an app the scheduler keeps a record of
    /// has a count; a prover's nodes configure their own replicas, and an entry its node anchors
    /// to the machine (`machine`) is the node's own, so it always runs there.
    pub(super) fn beyond_replica_count(&self, start: &CommandRecord) -> bool {
        let s = start.entry();
        if !s.is_container() || !s.machine().is_empty() {
            return false;
        }
        let key = format!("{}-{}", start.worker, s.name());
        let i = self.inner.lock();
        let Some(count) = i.apps.get(s.app())
            .filter(|a| a.persistent && !a.removed && a.namespace == s.namespace())
            .map(|a| a.entry().replicas()) else {
            return false;
        };
        let Some(this) = i.deployments.get(&key).filter(|d| d.entry().machine().is_empty()) else {
            return false;
        };
        let own = rank(&i, &key, this);
        let ahead: u64 = i.deployments.iter()
            .filter(|(k, d)| **k != key && d.entry().app() == s.app() && d.entry().namespace() == s.namespace())
            .filter(|(k, d)| running_now(&i, d) && !retiring(&i, d) && rank(&i, k, d) < own)
            .map(|(_, d)| planned(d).replicas())
            .sum();
        ahead >= count
    }

    /// Retires deployment `key`, which its app's replica count leaves no replicas: it is
    /// disabled at once, so it no longer counts and nothing starts it, and its node removes the
    /// app's entries, stopping what is left of their replicas (once it is back, when it is
    /// offline). That frees its room, and the record goes with the node's next report. The
    /// app's data stays on the node.
    pub(super) fn retire(&self, key: &str) -> anyhow::Result<()> {
        let Some(mut intent) = self.inner.lock().deployments.get(key).cloned() else {
            return Ok(());
        };
        let entry = intent.entry();
        let (namespace, app, name, worker) =
            (entry.namespace().to_owned(), entry.app().to_owned(), entry.name().to_owned(), intent.worker.clone());
        intent.disabled = true;
        if let Some(scale) = intent.scale.as_mut().filter(|s| s.pending()) {
            scale.phase = Phase::Complete as i32;
        }
        self.save("deployments", key, &intent)?;
        self.inner.lock().deployments.insert(key.into(), intent);
        // A start or stop never sent there has nothing left to do.
        let unsent: Vec<String> = self.inner.lock().commands.open()
            .filter(|c| !c.admitted && c.changes_entry(&worker, &name)).map(|c| c.id.clone()).collect();
        for id in unsent {
            let dropped = self.inner.lock().commands.update(&id, |c| {
                c.done = true;
                c.error = Some("its app's replica count leaves this node none".into());
                c.clone()
            });
            if let Some(c) = dropped {
                self.save("commands", &id, &c)?;
            }
        }
        self.tell_nodes([&worker], self.app_message(&namespace, &app), message::remove())?;
        tracing::info!(deployment = key, app, "retiring a deployment its app's replica count leaves no replicas");
        Ok(())
    }

    fn scale_command(
        &self,
        intent: &AppDeployment,
        spec: &Value,
        disabled: bool,
        phase: &str,
    ) -> anyhow::Result<String> {
        let parameters = message::scale_step(spec, disabled, &intent.scale.as_ref().unwrap().id, phase);
        // Completed commands are also reused after a crash between command and
        // phase persistence; the worker's command journal fences duplicate execution.
        if let Some(id) = self
            .inner
            .lock()
            .commands
            .values()
            .find(|c| {
                c.worker == intent.worker && c.is(NodeOperation::SetApp) && c.parameters == parameters
            })
            .map(|c| c.id.clone())
        {
            return Ok(id);
        }
        self.command(&intent.worker, NodeOperation::SetApp, parameters)
    }

    fn save_scale(&self, key: &str, scale: ScaleStatus) -> anyhow::Result<()> {
        let mut intent = self.inner.lock().deployments.get(key).cloned().unwrap();
        intent.scale = Some(scale);
        self.save("deployments", key, &intent)?;
        self.inner.lock().deployments.insert(key.into(), intent);
        self.notify.notify_waiters();
        Ok(())
    }

    pub(super) async fn advance_scales(&self) -> anyhow::Result<()> {
        let intents = self
            .inner
            .lock()
            .deployments
            .iter()
            .filter(|(_, s)| s.scale.as_ref().is_some_and(ScaleStatus::pending))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<_>>();
        for (key, intent) in intents {
            // One deployment's conflict (an old owner of an integrated worker, say) must not
            // stall the rest of the fleet: defer it to the next pass and keep dispatching.
            let name = key.clone();
            if let Err(error) = self.advance_scale(key, intent).await {
                tracing::warn!(deployment = %name, error = %error, "scale deferred to the next pass");
            }
        }
        Ok(())
    }
    async fn advance_scale(&self, key: String, intent: AppDeployment) -> anyhow::Result<()> {
        let mut scale = intent.scale.clone().unwrap();
            if scale.phase() == Phase::Draining {
                // A proof app refuses jobs while its prover is down; an app's template records it.
                self.sync_app_record(intent.entry(), true).await?;
                let template = self.inner.lock().apps.get(intent.entry().app()).filter(|a| a.persistent).cloned();
                if let Some(mut app) = template {
                    app.set_disabled(true);
                    self.save("apps", &app.name, &app)?;
                    self.inner.lock().apps.insert(app.name.clone(), app);
                }
                if scale.stop.is_none() {
                    scale.stop =
                        Some(self.scale_command(&intent, &intent.spec, true, "stop")?);
                    self.save_scale(&key, scale.clone())?;
                }
                let command = self
                    .inner
                    .lock()
                    .commands
                    .get(scale.stop.as_ref().unwrap())
                    .cloned()
                    .unwrap();
                if !command.done {
                    return Ok(());
                }
                if command.error.is_some() || !command.cleaned() {
                    scale.phase = Phase::Failed as i32;
                    scale.error = Some(
                        command
                            .error
                            .unwrap_or_else(|| "node did not confirm the app's cleanup".into()),
                    );
                    self.save_scale(&key, scale)?;
                    return Ok(());
                }
                // The node writes the target configuration while the app is stopped.
                {
                    if scale.update.is_none() {
                        let mut set = scale.set.as_ref().map(|s| s.0.clone()).unwrap_or_else(|| json!({}));
                        for k in Entry::RESOURCES {
                            if !scale.target[k].is_null() {
                                set[k] = scale.target[k].clone();
                            }
                        }
                        EntryMut(&mut set).set_disabled(scale.disabled);
                        let sent = self.tell_nodes([&intent.worker], message::entry(intent.entry().name()), message::set(set))?;
                        scale.update = sent.into_iter().next();
                        self.save_scale(&key, scale.clone())?;
                    }
                    let command = self.inner.lock().commands.get(scale.update.as_ref().unwrap()).cloned().unwrap();
                    if !command.done {
                        return Ok(());
                    }
                    if command.error.is_some() || !command.updated(intent.entry().name()) {
                        scale.phase = Phase::Failed as i32;
                        scale.error = Some(command.error.unwrap_or_else(|| "node did not write the new configuration".into()));
                        self.save_scale(&key, scale)?;
                        return Ok(());
                    }
                }
                scale.phase = Phase::Starting as i32;
                self.save_scale(&key, scale.clone())?;
            }
            if scale.start.is_none() {
                scale.start =
                    Some(self.scale_command(&intent, &scale.target, scale.disabled, "apply")?);
                self.save_scale(&key, scale.clone())?;
            }
            let command = self
                .inner
                .lock()
                .commands
                .get(scale.start.as_ref().unwrap())
                .cloned()
                .unwrap();
            if !command.done {
                return Ok(());
            }
            let first_ack = scale.ready_since.is_none();
            let ready_since = *scale.ready_since.get_or_insert_with(now);
            let timeout = scale.target_entry().ready_timeout_s().clamp(1.0, 3600.0) + 30.0;
            let (connected, applied) = {
                let i = self.inner.lock();
                let worker = i
                    .workers
                    .get(&intent.worker)
                    .filter(|w| now() - w.seen < 30.0);
                (
                    worker.is_some(),
                    worker.is_some_and(|w| {
                        let target = scale.target_entry();
                        w.report.entries().any(|s| {
                            s.name() == target.name()
                                && s.same_resources(target)
                                && s.disabled() == scale.disabled
                                && if scale.disabled { s.cleaned() } else { s.ready() }
                        })
                    }),
                )
            };
            if command.error.is_some() || (!applied && connected && now() - ready_since > timeout) {
                scale.phase = Phase::Failed as i32;
                scale.error=Some(command.error.clone().unwrap_or_else(||"node did not report the requested resources ready before the startup timeout".into()));
                let mut failed = self.inner.lock().deployments.get(&key).cloned().unwrap();
                failed.disabled = true;
                failed.scale = Some(scale);
                self.save("deployments", &key, &failed)?;
                self.inner.lock().deployments.insert(key, failed);
                return Ok(());
            }
            if !applied {
                if first_ack {
                    self.save_scale(&key, scale)?;
                }
                return Ok(());
            }
            if !scale.disabled {
                self.sync_app_record(scale.target_entry(), false).await?;
            }
            let app = self
                .inner
                .lock()
                .apps
                .get(intent.entry().app())
                .cloned();
            if let Some(mut app) = app.filter(|a| a.persistent) {
                for k in Entry::RESOURCES {
                    app.spec[k] = scale.target[k].clone();
                }
                // The count is the app's, over all its nodes.
                app.spec["replicas"] = json!(self.app_replicas(intent.entry().namespace(), intent.entry().app()));
                for (k, v) in scale.set.as_ref().and_then(|set| set.as_object()).into_iter().flatten() {
                    app.spec[k.as_str()] = v.clone();
                }
                app.set_disabled(scale.disabled);
                self.save("apps", &app.name, &app)?;
                self.inner.lock().apps.insert(app.name.clone(), app);
            }
            scale.phase = Phase::Complete as i32;
            self.save_scale(&key, scale)?;
        Ok(())
    }
}

impl WorkloadController {
    /// Creates an app from its container spec, disabled and on no node: `fleetctl app enable`
    /// places it (`deploy`), starts it and admits it against the namespace's quota. An app that
    /// proves a program also has its proof record, which takes its jobs from now on; they wait
    /// for its prover. The name is one of the
    /// namespace's apps and batches, so it is checked against both here and when a node reports.
    pub(super) async fn create_app(&self, c: Option<&Token>, q: &BTreeMap<String, String>,
        request: fleet_proto::model::v1::RegisterAppRequest,
    ) -> anyhow::Result<Response> {
        // `request` is validated.
        let name = request.name.clone();
        let spec = request.spec.as_object().cloned().unwrap_or_default();
        let (namespace, tag) = self.scope(c, q, request.namespace.as_deref())?;
        WorkloadController::require_grant(c, &name)?;
        WorkloadController::require_admin_settings(c, &spec, &Self::ADMIN_CREATE_SETTINGS)?;
        let _ops = self.ops.lock().await;
        self.ensure_name_free(&name)?;
        {
            let i = self.inner.lock();
            if i.apps.contains_key(&name)
                || i.deployments.values().any(|d| d.entry().name() == name)
                || self.fleet.app_known(&namespace, &name)
            {
                return Err(fail(409, format!("an app or batch named {name} exists")));
            }
        }
        let mut entry = json!({"name": name, "kind": request.kind(), "namespace": namespace, "disabled": true,
            "priority": request.priority.unwrap_or(0), "replicas": 1, "gpus": 1.0, "cpus": 1});
        if let Some(tag) = &tag {
            entry["tag"] = json!(tag);
        }
        for (key, value) in spec.into_iter().filter(|(_, v)| !v.is_null()) {
            entry[key.as_str()] = value;
        }
        let proof = match &request.proof {
            Some(proof) => Some(self.fleet.add_app(&namespace, &name, request.priority.unwrap_or(0), proof).await?),
            None => None,
        };
        if let Some(proof) = &proof {
            entry["elf_id"] = json!(proof.elf_id);
        }
        let app = WorkloadTemplate {
            name: name.clone(),
            namespace,
            tag: tag.clone(),
            spec: entry.into(),
            persistent: true,
            removed: false,
            workers: Vec::new(),
        };
        self.save("apps", &name, &app)?;
        self.inner.lock().apps.insert(name, app.clone());
        drop(_ops);
        let view = match &proof {
            Some(proof) => self.proof_app_value(proof, None, tag.as_deref()).await?,
            None => self.app_value(&app, tag.as_deref()),
        };
        Ok((StatusCode::CREATED, Json(view)).into_response())
    }

    /// Places `app`, if it runs on no node yet, on the node `place_app` picks: that node writes
    /// the app's entry disabled, and this returns once it reports the entry, so enabling it can
    /// start it there. An app without a template (one a node configures itself) has nothing to
    /// place. Takes the operation lock, and waits without it for the node's heartbeat.
    pub(super) async fn deploy(&self, namespace: &str, app: &str) -> anyhow::Result<()> {
        let (node, create) = {
            let _ops = self.ops.lock().await;
            // Already placed, or proved by provers that connected on their own.
            let key = crate::placement::app_key(namespace, app);
            if !self.app_deployments(namespace, app).is_empty()
                || self.fleet.status().nodes.iter().any(|n| n.app.as_deref() == Some(key.as_str())) {
                return Ok(());
            }
            let Some(mut template) = self.inner.lock().apps.get(app)
                .filter(|a| a.persistent && !a.removed && a.namespace == namespace).cloned() else {
                return Ok(());
            };
            let pending = self.inner.lock().commands.open()
                .find(|c| !c.done && c.is(NodeOperation::App) && c.created().is_some_and(|e| e.name() == app))
                .map(|c| (c.worker.clone(), c.id.clone()));
            match pending {
                Some(pending) => pending,
                None => {
                    // A node that refused to create it, as one without its volumes does, is
                    // picked again only when no other can take it.
                    let refused: Vec<String> = self.inner.lock().commands.values()
                        .filter(|c| c.is(NodeOperation::App) && c.error.is_some() && c.created().is_some_and(|e| e.name() == app))
                        .map(|c| c.worker.clone()).collect();
                    let (node, _) = self.place_app(template.entry(), &refused)
                        .or_else(|_| self.place_app(template.entry(), &[]))?;
                    let create = message::with(self.app_message(namespace, app), &message::create(&template.spec));
                    let create = self.command(&node, NodeOperation::App, create)?;
                    template.workers = vec![node.clone()];
                    self.save("apps", app, &template)?;
                    self.inner.lock().apps.insert(app.into(), template);
                    (node, create)
                }
            }
        };
        let key = format!("{node}-{app}");
        tokio::time::timeout(Duration::from_secs(120), async {
            loop {
                let notified = self.notify.notified();
                let (reported, failed) = {
                    let i = self.inner.lock();
                    let command = i.commands.get(&create);
                    (i.deployments.contains_key(&key),
                        command.and_then(|c| c.error.clone().or_else(|| c.done.then(String::new))))
                };
                if reported {
                    return Ok(());
                }
                if let Some(error) = failed.filter(|e| !e.is_empty()) {
                    // The next enable places it again.
                    let template = self.inner.lock().apps.get(app).cloned();
                    if let Some(mut template) = template {
                        template.workers.clear();
                        self.save("apps", app, &template)?;
                        self.inner.lock().apps.insert(app.into(), template);
                    }
                    return Err(fail(503, format!("{node} could not create the app: {error}")));
                }
                notified.await;
            }
        })
        .await
        .map_err(|_| fail(503, format!("{node} has not reported the app's entry yet; enable it again once it does")))?
    }

    /// The node for an app's `entry`: a live, ready node that is not draining and has the GPUs
    /// and CPUs of all its replicas. One that has them free now is preferred, the one with the
    /// fewest free GPUs among those, as a batch-class proof app is placed, so larger requests
    /// keep the larger gaps; otherwise the one with the most free GPUs, where the app waits for
    /// room. Whether it has the room now comes along. `except` are nodes not to pick. An entry
    /// anchored to a `machine` goes to that machine's node only.
    fn place_app(&self, entry: Entry<'_>, except: &[String]) -> anyhow::Result<(String, bool)> {
        let (gpus, cpus) = entry.total(1.0);
        let machine = entry.machine();
        let large: Vec<String> = self.inner.lock().workers.iter()
            .filter(|(id, w)| !except.contains(id) && (machine.is_empty() || w.report.machine == machine)
                && w.report.total_gpus + 1e-9 >= gpus && w.report.total_cpus + 1e-9 >= cpus)
            .map(|(id, _)| id.clone()).collect();
        let sizes: Vec<(String, f64, f64)> = large.into_iter()
            .filter_map(|id| self.capacity(&id).map(|(g, c)| (id, g, c)))
            .collect();
        let fits = |&(_, g, c): &&(String, f64, f64)| g + 1e-9 >= gpus && c + 1e-9 >= cpus;
        sizes.iter().filter(fits).min_by(|a, b| a.1.total_cmp(&b.1).then(a.2.total_cmp(&b.2)))
            .map(|(id, _, _)| (id.clone(), true))
            .or_else(|| sizes.iter().max_by(|a, b| a.1.total_cmp(&b.1).then(b.0.cmp(&a.0))).map(|(id, _, _)| (id.clone(), false)))
            .ok_or_else(|| fail(503, if machine.is_empty() {
                format!("no ready node has {gpus} GPUs and {cpus} CPUs for this app")
            } else {
                format!("no ready node on machine {machine} has {gpus} GPUs and {cpus} CPUs for this app")
            }))
    }

    /// Places a queued `start` of an app the scheduler placed, as `place_app` places a new
    /// app: on its own node when that has the room (`fits`), else on the node that fits it
    /// best now. That node is sent the app's entry, disabled, and the start runs there once it
    /// reports it (`start_on`); a node whose create fails is not picked again. With no room
    /// anywhere the caller preempts on the start's own node, and the start keeps the room that
    /// frees. Nothing of what the app keeps on a node's disks is copied: that is the app's own.
    /// An entry whose node configures its `machine` is anchored to that machine and stays.
    /// Returns whether the start runs on another node, so it is not sent to its own. The caller
    /// holds the operation lock.
    pub(super) fn place_start(&self, start: &CommandRecord, fits: bool) -> anyhow::Result<bool> {
        let s = start.entry();
        let (namespace, app, name) = (s.namespace(), s.app(), s.name());
        let placed = self.inner.lock().apps.get(app).is_some_and(|a| a.persistent && !a.removed && a.namespace == namespace);
        if !placed || !s.machine().is_empty() {
            return Ok(false);
        }
        let creates: Vec<CommandRecord> = self.inner.lock().commands.values()
            .filter(|c| c.worker != start.worker && c.is(NodeOperation::App) && c.created().is_some_and(|e| e.name() == name))
            .cloned().collect();
        if creates.iter().any(|c| !c.done) {
            return Ok(true);
        }
        // A node the app was created on before and has since moved off keeps a disabled record
        // until it reports again; a start sent there would find no entry. So the start follows
        // only a create whose node still reports the entry and is not told to remove the app.
        let created_on = {
            let i = self.inner.lock();
            creates.iter().filter(|c| c.error.is_none()).map(|c| c.worker.clone()).find(|w| {
                i.deployments.contains_key(&format!("{w}-{name}"))
                    && i.workers.get(w).is_some_and(|node| node.report.entry(name).is_some())
                    && !i.commands.open().any(|c| &c.worker == w && c.removes_app(app))
            })
        };
        if let Some(target) = created_on {
            self.start_on(start, &target)?;
            return Ok(true);
        }
        let preempting = self.inner.lock().jobs.open().any(|r| {
            r.worker.as_deref() == Some(start.worker.as_str()) && r.stop.as_deref() == Some("preempt") && !terminal(r.state())
        });
        if fits || preempting {
            return Ok(false);
        }
        // Not its own node, one that already configures an entry of this name, or one whose
        // create of it failed.
        let skip: Vec<String> = {
            let i = self.inner.lock();
            i.workers.iter()
                .filter(|(id, w)| **id == start.worker || w.report.entry(name).is_some())
                .map(|(id, _)| id.clone())
                .chain(creates.iter().filter(|c| c.error.is_some()).map(|c| c.worker.clone()))
                .collect()
        };
        let Ok((target, true)) = self.place_app(s, &skip) else {
            return Ok(false);
        };
        // What the old node reported about the entry goes, its configuration comes along.
        let mut entry = s.0.clone();
        EntryMut(&mut entry).forget_report();
        EntryMut(&mut entry).set_disabled(true);
        self.command(&target, NodeOperation::App, message::with(self.app_message(namespace, app), &message::create(&entry)))?;
        tracing::info!(app, node = target, "placing an app's start on a node with room for it");
        Ok(true)
    }

    /// Runs `start` on `target`, which reports the app's entry: the start is not sent to its
    /// own node, that node's deployment is done and the node removes its entry, and the app
    /// starts on `target`. Removing the entry stops what is left of the app there; its data
    /// stays where the app keeps it, which may be storage `target` shares.
    fn start_on(&self, start: &CommandRecord, target: &str) -> anyhow::Result<()> {
        let s = start.entry();
        let (namespace, app, name) = (s.namespace(), s.app(), s.name());
        let old_key = format!("{}-{name}", start.worker);
        let old = self.inner.lock().deployments.get(&old_key).cloned();
        if let Some(mut old) = old {
            old.disabled = true;
            if let Some(scale) = old.scale.as_mut().filter(|s| s.pending()) {
                scale.phase = Phase::Complete as i32;
            }
            self.save("deployments", &old_key, &old)?;
            self.inner.lock().deployments.insert(old_key, old);
        }
        let mut unsent = start.clone();
        unsent.done = true;
        unsent.error = Some(format!("the app starts on {target}, which has room for it"));
        self.save("commands", &unsent.id, &unsent)?;
        self.inner.lock().commands.insert(unsent.id.clone(), unsent);
        self.tell_nodes([&start.worker], self.app_message(namespace, app), message::remove())?;
        let template = self.inner.lock().apps.get(app).cloned();
        if let Some(mut template) = template {
            template.workers = vec![target.into()];
            self.save("apps", app, &template)?;
            self.inner.lock().apps.insert(app.into(), template);
        }
        tracing::info!(app, node = target, "starting the app on the node placed for it");
        self.begin_toggle(&[format!("{target}-{name}")], false)
    }
}
