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

pub(super) fn same_resources(a: &Value, b: &Value) -> bool {
    ["gpus", "cpus", "replicas"].iter().all(|key| {
        let default = if *key == "gpus" && text(b, "kind") == "ziren-cpu" {
            0.0
        } else {
            1.0
        };
        (a[*key].as_f64().unwrap_or(default) - b[*key].as_f64().unwrap_or(default)).abs() < 1e-9
    })
}

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
            text(&s.spec, "namespace") == namespace
                && text(&s.spec, "app") == app
                && s.scale.as_ref().is_some_and(ScaleStatus::pending)
        }) {
            return Err(fail(409, "app scaling is already pending"));
        }
        Ok(())
    }

    pub(super) fn app_deployment(
        &self,
        namespace: &str,
        app: &str,
        node: Option<&str>,
    ) -> anyhow::Result<(String, AppDeployment)> {
        let i = self.inner.lock();
        let mut deployments = i.deployments.iter().filter(|(_, intent)| {
            let s = &intent.spec;
            text(s, "namespace") == namespace
                && node.is_none_or(|n| n == intent.worker)
                && matches!(text(s, "kind"), "ziren-gpu" | "app")
                && text(s, "app") == app
        });
        let first = deployments
            .next()
            .map(|(k, v)| (k.clone(), v.clone()))
            .ok_or_else(|| fail(404, "the app runs on no node (enable it to place it), or not on that one"))?;
        if deployments.next().is_some() {
            return Err(fail(400, "the app runs on several nodes; select --node NODE"));
        }
        Ok(first)
    }

    fn scale_value(&self, intent: &AppDeployment) -> Value {
        let i = self.inner.lock();
        let observed = i.workers.get(&intent.worker).and_then(|w| {
            w.report
                .apps
                .iter()
                .find(|s| text(s, "name") == text(&intent.spec, "name"))
        });
        let target = intent
            .scale
            .as_ref()
            .map(|s| &s.target)
            .unwrap_or(&intent.spec);
        json!({"node":intent.worker,"entry":text(&intent.spec,"name"),
            "namespace":text(&intent.spec,"namespace"),
            "gpus":target["gpus"].as_f64().unwrap_or(1.0),"cpus":target["cpus"].as_f64().unwrap_or(1.0),
            "replicas":target["replicas"].as_u64().unwrap_or(1),
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
            .request(&deployment.worker, text(&deployment.spec, "name"), lines)
            .await
            .map_err(|e| fail(fleet_proto::sdk::http_status_code(e.code()), e.message().to_owned()))?;
        Ok(bytes.into_response())
    }

    pub(super) fn scale_status(
        &self,
        namespace: &str,
        app: &str,
        node: Option<&str>,
    ) -> anyhow::Result<Value> {
        let (_, deployment) = self.app_deployment(namespace, app, node)?;
        Ok(self.scale_value(&deployment))
    }

    /// The deployment of `app` (on `node`, when it has several) that a scale or settings update
    /// changes: the app still exists and no other operation on the deployment is under way.
    fn deployment_to_change(&self, namespace: &str, app: &str, node: Option<&str>) -> anyhow::Result<(String, AppDeployment)> {
        let (key, intent) = self.app_deployment(namespace, app, node)?;
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
        {
            let i = self.inner.lock();
            if intent.scale.as_ref().is_some_and(ScaleStatus::pending) && !waiting_for_capacity(&i, &intent)
                || i.commands.open().any(|c| {
                    !c.done
                        && c.admitted // one still waiting for capacity is replaced by this request
                        && c.worker == intent.worker
                        && c.operation == NodeOperation::SetApp as i32
                        && text(&c.parameters["app"], "name") == text(&intent.spec, "name")
                })
            {
                return Err(fail(409, "an app operation is already pending"));
            }
        }
        Ok((key, intent))
    }

    /// Settings an update may change on an app. The rest is identity, placement or the
    /// node's own configuration, or has its own operation (scale, enable/disable, priority).
    pub(super) const APP_SETTINGS: [&'static str; 11] = ["image", "env", "command", "user", "workdir", "volumes",
        "shm_gb", "memory_gb", "health_path", "stop_timeout_s", "ready_timeout_s"];
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
                    || i.commands.open().any(|c| {
                        !c.done && c.admitted && c.worker == intent.worker
                            && c.operation == NodeOperation::SetApp as i32
                            && text(&c.parameters["app"], "name") == text(&intent.spec, "name")
                    })
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
        let (namespace, app) = (text(&entry, "namespace"), text(&entry, "app"));
        let priority = if self.fleet.app_known(namespace, app) {
            self.fleet.app_priority(namespace, app).into()
        } else {
            self.inner.lock().apps.get(app).filter(|a| a.persistent).map_or(0, |a| a.spec["priority"].as_i64().unwrap_or(0))
        };
        entry["priority"] = json!(priority);
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
        let (key, mut intent) = self.deployment_to_change(namespace, app, req.node.as_deref())?;
        let mut target = self.with_priority(intent.spec.clone());
        if let Some(v) = req.gpus {
            target["gpus"] = json!(v);
        }
        if let Some(v) = req.cpus {
            target["cpus"] = json!(v);
        }
        if let Some(v) = req.replicas {
            target["replicas"] = json!(v);
        }
        let g = target["gpus"].as_f64().unwrap_or(1.0);
        let cpu = target["cpus"].as_f64().unwrap_or(1.0);
        let replicas = target["replicas"].as_u64().unwrap_or(1);
        if super::integrated_worker(&target) && replicas != 1 {
            return Err(fail(400, "integrated app workers require one machine-pinned replica",
            ));
        }
        if !g.is_finite()
            || g < 0.0
            || !cpu.is_finite()
            || cpu <= 0.0
            || replicas == 0
            || replicas > 64
        {
            return Err(fail(400,"gpus must be nonnegative, cpus positive, and replicas between 1 and 64; use app disable to stop all replicas"));
        }
        if text(&target, "kind") == "ziren-gpu" && (g < 1.0 || g.fract() != 0.0) {
            return Err(fail(
                400,
                "a proof server requires a positive integer GPU count",
            ));
        }
        let total_g = g * replicas as f64;
        let total_cpu = cpu * replicas as f64;
        {
            let i = self.inner.lock();
            let worker = i
                .workers
                .get(&intent.worker)
                .filter(|w| now() - w.seen < 30.0 && w.report.ready && !w.draining)
                .ok_or_else(|| fail(503, "node is offline, unready or draining"))?;
            if total_g > worker.report.total_gpus + 1e-9
                || total_cpu > worker.report.total_cpus + 1e-9
            {
                return Err(fail(
                    422,
                    format!(
                        "requested allocation exceeds node {} capacity: requested {total_g} GPUs and {total_cpu} CPUs (replicas: {replicas}); node total {} GPUs and {} CPUs, currently free {} GPUs and {} CPUs",
                        intent.worker,
                        worker.report.total_gpus,
                        worker.report.total_cpus,
                        worker.report.free_gpus,
                        worker.report.free_cpus,
                    ),
                ));
            }
        }
        let ns = self
            .fleet
            .namespace(namespace)
            .ok_or_else(|| fail(404, "unknown namespace"))?;
        if ns.suspended_ms.is_some() {
            return Err(fail(403, "namespace is suspended"));
        }
        // Container replicas reserve the whole app. Proof quota is charged
        // per assigned proof; extra replicas can remain idle while that quota is occupied.
        let (quota_g, quota_cpu) = if text(&target, "kind") == "app" || super::integrated_worker(&target) {
            (total_g, total_cpu)
        } else {
            (g, cpu)
        };
        let tag_quota = ns
            .tag_quotas
            .get(target["tag"].as_str().unwrap_or(""))
            .cloned()
            .unwrap_or_default();
        if !ns.quota.fits(quota_g, quota_cpu) || !tag_quota.fits(quota_g, quota_cpu) {
            return Err(fail(
                422,
                "requested allocation exceeds namespace or tag GPU/CPU quota",
            ));
        }
        if same_resources(&intent.spec, &target)
            && intent
                .scale
                .as_ref()
                .is_none_or(|s| s.phase() != Phase::Failed)
        {
            return Ok(self.scale_value(&intent));
        }
        let disabled = intent
            .scale
            .as_ref()
            .filter(|s| s.phase() == Phase::Failed)
            .map(|s| s.disabled)
            .unwrap_or(intent.disabled);
        intent.scale = Some(new_scale(target, disabled));
        // Persist the transition before changing admission or sending any command.
        self.save("deployments", &key, &intent)?;
        self.inner.lock().deployments.insert(key, intent.clone());
        self.notify.notify_waiters();
        Ok(self.scale_value(&intent))
    }

    fn scale_command(
        &self,
        intent: &AppDeployment,
        spec: &Value,
        disabled: bool,
        phase: &str,
    ) -> anyhow::Result<String> {
        let parameters = json!({"app":spec,"disabled":disabled,
            "scale_id":intent.scale.as_ref().unwrap().id,"scale_phase":phase});
        // Completed commands are also reused after a crash between command and
        // phase persistence; the worker's command journal fences duplicate execution.
        if let Some(id) = self
            .inner
            .lock()
            .commands
            .values()
            .find(|c| {
                c.worker == intent.worker
                    && c.operation == NodeOperation::SetApp as i32
                    && c.parameters == parameters
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
                self.sync_app_record(&intent.spec, true).await?;
                let template = self.inner.lock().apps.get(text(&intent.spec, "app")).filter(|a| a.persistent).cloned();
                if let Some(mut app) = template {
                    app.spec["disabled"] = json!(true);
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
                if command.error.is_some() || command.result["cleaned"].as_bool() != Some(true) {
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
                        for k in ["gpus", "cpus", "replicas"] {
                            if !scale.target[k].is_null() {
                                set[k] = scale.target[k].clone();
                            }
                        }
                        set["disabled"] = json!(scale.disabled);
                        let message = json!({"entry":{"name":text(&intent.spec, "name")}});
                        let sent = self.tell_nodes([&intent.worker], message, json!({"set": set}))?;
                        scale.update = sent.into_iter().next();
                        self.save_scale(&key, scale.clone())?;
                    }
                    let command = self.inner.lock().commands.get(scale.update.as_ref().unwrap()).cloned().unwrap();
                    if !command.done {
                        return Ok(());
                    }
                    let written = command.result["updated"].as_array()
                        .is_some_and(|names| names.iter().any(|n| n.as_str() == Some(text(&intent.spec, "name"))));
                    if command.error.is_some() || !written {
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
            let timeout = scale.target["ready_timeout_s"]
                .as_f64()
                .unwrap_or(600.0)
                .clamp(1.0, 3600.0)
                + 30.0;
            let (connected, applied) = {
                let i = self.inner.lock();
                let worker = i
                    .workers
                    .get(&intent.worker)
                    .filter(|w| now() - w.seen < 30.0);
                (
                    worker.is_some(),
                    worker.is_some_and(|w| {
                        w.report.apps.iter().any(|s| {
                            text(s, "name") == text(&scale.target, "name")
                                && same_resources(s, &scale.target)
                                && s["disabled"].as_bool().unwrap_or(false) == scale.disabled
                                && s[if scale.disabled { "cleaned" } else { "ready" }].as_bool()
                                    == Some(true)
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
                self.sync_app_record(&scale.target, false).await?;
            }
            let app = self
                .inner
                .lock()
                .apps
                .get(text(&intent.spec, "app"))
                .cloned();
            if let Some(mut app) = app.filter(|a| a.persistent) {
                for k in ["gpus", "cpus", "replicas"] {
                    app.spec[k] = scale.target[k].clone();
                }
                for (k, v) in scale.set.as_ref().and_then(|set| set.as_object()).into_iter().flatten() {
                    app.spec[k.as_str()] = v.clone();
                }
                app.spec["disabled"] = json!(scale.disabled);
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
        {
            let i = self.inner.lock();
            if i.apps.get(&name).is_some_and(|a| !a.removed)
                || i.deployments.values().any(|d| text(&d.spec, "name") == name)
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
                .find(|c| !c.done && c.operation == NodeOperation::App as i32 && text(&c.parameters["create"], "name") == app)
                .map(|c| (c.worker.clone(), c.id.clone()));
            match pending {
                Some(pending) => pending,
                None => {
                    let (node, _) = self.place_app(&template.spec, &[])?;
                    let mut message = self.app_message(namespace, app);
                    message["create"] = template.spec.0.clone();
                    let create = self.command(&node, NodeOperation::App, message)?;
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
    /// room. Whether it has the room now comes along. `except` are nodes not to pick.
    fn place_app(&self, entry: &Value, except: &[String]) -> anyhow::Result<(String, bool)> {
        let replicas = entry["replicas"].as_f64().unwrap_or(1.0);
        let gpus = entry["gpus"].as_f64().unwrap_or(1.0) * replicas;
        let cpus = entry["cpus"].as_f64().unwrap_or(1.0) * replicas;
        let large: Vec<String> = self.inner.lock().workers.iter()
            .filter(|(id, w)| !except.contains(id) && w.report.total_gpus + 1e-9 >= gpus && w.report.total_cpus + 1e-9 >= cpus)
            .map(|(id, _)| id.clone()).collect();
        let sizes: Vec<(String, f64, f64)> = large.into_iter()
            .filter_map(|id| self.capacity(&id).map(|(g, c)| (id, g, c)))
            .collect();
        let fits = |&(_, g, c): &&(String, f64, f64)| g + 1e-9 >= gpus && c + 1e-9 >= cpus;
        sizes.iter().filter(fits).min_by(|a, b| a.1.total_cmp(&b.1).then(a.2.total_cmp(&b.2)))
            .map(|(id, _, _)| (id.clone(), true))
            .or_else(|| sizes.iter().max_by(|a, b| a.1.total_cmp(&b.1).then(b.0.cmp(&a.0))).map(|(id, _, _)| (id.clone(), false)))
            .ok_or_else(|| fail(503, format!("no ready node has {gpus} GPUs and {cpus} CPUs for this app")))
    }

    /// Places a queued `start` of an app the scheduler placed, as `place_app` places a new
    /// app: on its own node when that has the room (`fits`), else on the node that fits it
    /// best now. That node is sent the app's entry, disabled, and the start runs there once it
    /// reports it (`start_on`); a node whose create fails is not picked again. With no room
    /// anywhere the caller preempts on the start's own node, and the start keeps the room that
    /// frees. Nothing of what the app keeps on a node's disks is copied: that is the app's own.
    /// Returns whether the start runs on another node, so it is not sent to its own. The caller
    /// holds the operation lock.
    pub(super) fn place_start(&self, start: &CommandRecord, fits: bool) -> anyhow::Result<bool> {
        let s = &start.parameters["app"];
        let (namespace, app, name) = (text(s, "namespace"), text(s, "app"), text(s, "name"));
        let placed = self.inner.lock().apps.get(app).is_some_and(|a| a.persistent && !a.removed && a.namespace == namespace);
        if !placed {
            return Ok(false);
        }
        let creates: Vec<CommandRecord> = self.inner.lock().commands.values()
            .filter(|c| c.worker != start.worker && c.operation == NodeOperation::App as i32
                && text(&c.parameters["create"], "name") == name)
            .cloned().collect();
        if creates.iter().any(|c| !c.done) {
            return Ok(true);
        }
        let created_on = creates.iter().filter(|c| c.error.is_none()).map(|c| c.worker.clone())
            .find(|w| self.inner.lock().deployments.contains_key(&format!("{w}-{name}")));
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
                .filter(|(id, w)| **id == start.worker || w.report.apps.iter().any(|a| text(a, "name") == name))
                .map(|(id, _)| id.clone())
                .chain(creates.iter().filter(|c| c.error.is_some()).map(|c| c.worker.clone()))
                .collect()
        };
        let Ok((target, true)) = self.place_app(s, &skip) else {
            return Ok(false);
        };
        let mut entry = s.clone();
        if let Some(fields) = entry.as_object_mut() {
            // What the old node reported about the entry, not its configuration.
            for key in ["ready", "logs", "cleaned", "namespace_ref"] {
                fields.remove(key);
            }
        }
        entry["disabled"] = json!(true);
        if !entry["machine"].is_null() {
            entry["machine"] = json!(self.inner.lock().workers[&target].report.machine);
        }
        let mut message = self.app_message(namespace, app);
        message["create"] = entry;
        self.command(&target, NodeOperation::App, message)?;
        tracing::info!(app, node = target, "placing an app's start on a node with room for it");
        Ok(true)
    }

    /// Runs `start` on `target`, which reports the app's entry: the start is not sent to its
    /// own node, that node's deployment is done and the node removes its entry, and the app
    /// starts on `target`. Removing the entry stops what is left of the app there; its data
    /// stays where the app keeps it, which may be storage `target` shares.
    fn start_on(&self, start: &CommandRecord, target: &str) -> anyhow::Result<()> {
        let s = &start.parameters["app"];
        let (namespace, app, name) = (text(s, "namespace"), text(s, "app"), text(s, "name"));
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
        self.tell_nodes([&start.worker], self.app_message(namespace, app), json!({"remove": true}))?;
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
