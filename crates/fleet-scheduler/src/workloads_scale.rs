//! App-owner desired resource settings, reconciled through unary heartbeats.
use super::*;

use fleet_proto::model::v1::{ScalePhase as Phase, ScaleRequest, ScaleStatus};

/// A change to one service: drain, stop, have the node write the target configuration
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
        if self.inner.lock().services.values().any(|s| {
            text(&s.service, "namespace") == namespace
                && text(&s.service, "app") == app
                && s.scale.as_ref().is_some_and(ScaleStatus::pending)
        }) {
            return Err(fail(409, "app scaling is already pending"));
        }
        Ok(())
    }

    pub(super) fn scale_service(
        &self,
        namespace: &str,
        app: &str,
        node: Option<&str>,
    ) -> anyhow::Result<(String, AppDeployment)> {
        let i = self.inner.lock();
        let mut services = i.services.iter().filter(|(_, intent)| {
            let s = &intent.service;
            text(s, "namespace") == namespace
                && node.is_none_or(|n| n == intent.worker)
                && matches!(text(s, "kind"), "ziren-gpu" | "app")
                && text(s, "app") == app
        });
        let first = services
            .next()
            .map(|(k, v)| (k.clone(), v.clone()))
            .ok_or_else(|| fail(404, "no configured service for this app and node"))?;
        if services.next().is_some() {
            return Err(fail(400, "app has multiple services; select --node NODE"));
        }
        Ok(first)
    }

    fn scale_value(&self, intent: &AppDeployment) -> Value {
        let i = self.inner.lock();
        let observed = i.workers.get(&intent.worker).and_then(|w| {
            w.report
                .services
                .iter()
                .find(|s| text(s, "name") == text(&intent.service, "name"))
        });
        let target = intent
            .scale
            .as_ref()
            .map(|s| &s.target)
            .unwrap_or(&intent.service);
        json!({"node":intent.worker,"service":text(&intent.service,"name"),
            "namespace":text(&intent.service,"namespace"),
            "gpus":target["gpus"].as_f64().unwrap_or(1.0),"cpus":target["cpus"].as_f64().unwrap_or(1.0),
            "replicas":target["replicas"].as_u64().unwrap_or(1),
            "state":intent.scale.as_ref().map(|s| serde_json::to_value(&s.phase()).unwrap()).unwrap_or(json!("configured")),
            "disabled":intent.scale.as_ref().map(|s|s.disabled).unwrap_or(intent.disabled),
            "operation":intent.scale.as_ref().map(|s|s.id.clone()),
            "error":intent.scale.as_ref().and_then(|s|s.error.clone()),
            "observed":observed.map(|s|json!({"gpus":s["gpus"],"cpus":s["cpus"],"replicas":s["replicas"],"ready":s["ready"],"disabled":s["disabled"]}))})
    }

    /// Reads the last `lines` lines of the app service's log from the node that runs it,
    /// through that node's heartbeat.
    pub(super) async fn app_logs(
        &self,
        c: Option<&Token>,
        namespace: &str,
        app: &str,
        q: &BTreeMap<String, String>,
    ) -> anyhow::Result<Response> {
        let node = q.get("node").map(String::as_str);
        self.require_service_scope(c, namespace, app, node)?;
        let (_, deployment) = self.scale_service(namespace, app, node)?;
        let lines = match q.get("lines") {
            Some(v) => v.parse::<u32>().map_err(|_| fail(400, "lines must be a number"))?,
            None => 100,
        };
        let bytes = self
            .fleet
            .log_relay
            .request(&deployment.worker, text(&deployment.service, "name"), lines)
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
        let (_, service) = self.scale_service(namespace, app, node)?;
        Ok(self.scale_value(&service))
    }

    /// The service of `app` (on `node`, when it has several) that a scale or settings update
    /// changes: the app still exists and no other operation on the service is under way.
    fn service_to_change(&self, namespace: &str, app: &str, node: Option<&str>) -> anyhow::Result<(String, AppDeployment)> {
        let (key, intent) = self.scale_service(namespace, app, node)?;
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
                || i.commands.values().any(|c| {
                    !c.done
                        && c.admitted // one still waiting for capacity is replaced by this request
                        && c.worker == intent.worker
                        && c.operation == NodeOperation::SetService as i32
                        && text(&c.parameters["service"], "name") == text(&intent.service, "name")
                })
            {
                return Err(fail(409, "a service operation is already pending"));
            }
        }
        Ok((key, intent))
    }

    /// Settings an update may change on a service. The rest is identity, placement or the
    /// node's own configuration, or has its own operation (scale, enable/disable, priority).
    pub(super) const SERVICE_SETTINGS: [&'static str; 11] = ["image", "env", "command", "user", "workdir", "volumes",
        "shm_gb", "memory_gb", "health_path", "stop_timeout_s", "ready_timeout_s"];
    // Services can access host networking, storage and prover credentials. Changing
    // their executable or environment is as privileged as changing their mounts.
    pub(super) const ADMIN_SERVICE_SETTINGS: [&'static str; 6] = ["image", "env", "command", "user", "workdir", "volumes"];

    /// Changes `app`'s service settings like a scale does: drain, stop, have the node write the
    /// settings (NODE_OPERATION_APP), start it again with them.
    /// Starts enabling or disabling these services of `app` the way a scale changes them:
    /// drain, stop, the node writes the enabled state, start again unless disabled. The caller
    /// holds the operation lock.
    pub(super) fn begin_toggle(&self, keys: &[String], disabled: bool) -> anyhow::Result<()> {
        for key in keys {
            let mut intent = self.inner.lock().services.get(key).cloned()
                .ok_or_else(|| fail(404, "no configured service for this app"))?;
            let busy = {
                let i = self.inner.lock();
                intent.scale.as_ref().is_some_and(ScaleStatus::pending) && !waiting_for_capacity(&i, &intent)
                    || i.commands.values().any(|c| {
                        !c.done && c.admitted && c.worker == intent.worker
                            && c.operation == NodeOperation::SetService as i32
                            && text(&c.parameters["service"], "name") == text(&intent.service, "name")
                    })
            };
            if busy {
                return Err(fail(409, "a service operation is already pending"));
            }
            intent.scale = Some(new_scale(self.with_priority(intent.service.clone()), disabled));
            self.save("services", key, &intent)?;
            self.inner.lock().services.insert(key.clone(), intent);
        }
        self.notify.notify_waiters();
        Ok(())
    }

    /// `service` as a change targets it: with its app's current priority, which only the
    /// scheduler keeps (a serving app's template, or the registered app's record).
    pub(super) fn with_priority(&self, service: impl Into<Value>) -> Value {
        let mut service: Value = service.into();
        let serving = self.inner.lock().apps.get(text(&service, "name"))
            .filter(|a| a.persistent).map(|a| a.spec["priority"].as_i64().unwrap_or(0));
        let priority = serving.unwrap_or_else(|| self.fleet.app_priority(text(&service, "namespace"), text(&service, "app")).into());
        service["priority"] = json!(priority);
        service
    }

    /// Waits until the changes of these services finished; a failed one is the error.
    pub(super) async fn wait_scales(&self, keys: &[String]) -> anyhow::Result<()> {
        tokio::time::timeout(Duration::from_secs(1800), async {
            loop {
                let notified = self.notify.notified();
                let scales: Vec<Option<ScaleStatus>> = {
                    let i = self.inner.lock();
                    keys.iter().map(|k| i.services.get(k).and_then(|d| d.scale.clone())).collect()
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
                _ => return Err(fail(400, format!("{key} cannot be updated; settings: {}", Self::SERVICE_SETTINGS.join(", ")))),
            };
            if !valid {
                return Err(fail(400, format!("{key}: invalid value")));
            }
        }
        let (key, mut intent) = self.service_to_change(namespace, app, node)?;
        if set.iter().all(|(k, v)| intent.service.get(k) == Some(v)) {
            return Ok(self.scale_value(&intent));
        }
        let mut target = self.with_priority(intent.service.clone());
        for (k, v) in &set {
            target[k.as_str()] = v.clone();
        }
        let disabled = intent.scale.as_ref().filter(|s| s.phase() == Phase::Failed).map(|s| s.disabled).unwrap_or(intent.disabled);
        let mut scale = new_scale(target, disabled);
        scale.set = Some(Value::Object(set).into());
        intent.scale = Some(scale);
        // Persist the transition before changing admission or sending any command.
        self.save("services", &key, &intent)?;
        self.inner.lock().services.insert(key, intent.clone());
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
        let (key, mut intent) = self.service_to_change(namespace, app, req.node.as_deref())?;
        let mut target = self.with_priority(intent.service.clone());
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
        // Persistent replicas reserve the whole service. Proof quota is charged
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
        if same_resources(&intent.service, &target)
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
        self.save("services", &key, &intent)?;
        self.inner.lock().services.insert(key, intent.clone());
        self.notify.notify_waiters();
        Ok(self.scale_value(&intent))
    }

    fn scale_command(
        &self,
        intent: &AppDeployment,
        service: &Value,
        disabled: bool,
        phase: &str,
    ) -> anyhow::Result<String> {
        let parameters = json!({"service":service,"disabled":disabled,
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
                    && c.operation == NodeOperation::SetService as i32
                    && c.parameters == parameters
            })
            .map(|c| c.id.clone())
        {
            return Ok(id);
        }
        self.command(&intent.worker, NodeOperation::SetService, parameters)
    }

    fn save_scale(&self, key: &str, scale: ScaleStatus) -> anyhow::Result<()> {
        let mut intent = self.inner.lock().services.get(key).cloned().unwrap();
        intent.scale = Some(scale);
        self.save("services", key, &intent)?;
        self.inner.lock().services.insert(key.into(), intent);
        self.notify.notify_waiters();
        Ok(())
    }

    pub(super) async fn advance_scales(&self) -> anyhow::Result<()> {
        let intents = self
            .inner
            .lock()
            .services
            .iter()
            .filter(|(_, s)| s.scale.as_ref().is_some_and(ScaleStatus::pending))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<_>>();
        for (key, intent) in intents {
            let mut scale = intent.scale.clone().unwrap();
            if scale.phase() == Phase::Draining {
                // A serving app has its template; any other service is a proof server.
                let serving = self.inner.lock().apps.get(text(&intent.service, "name"))
                    .filter(|a| a.persistent).cloned();
                match serving {
                    Some(mut app) => {
                        app.spec["disabled"] = json!(true);
                        self.save("apps", &app.name, &app)?;
                        self.inner.lock().apps.insert(app.name.clone(), app);
                    }
                    None => self.sync_app_record(&intent.service, true).await?,
                }
                if scale.stop.is_none() {
                    scale.stop =
                        Some(self.scale_command(&intent, &intent.service, true, "stop")?);
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
                    continue;
                }
                if command.error.is_some() || command.result["cleaned"].as_bool() != Some(true) {
                    scale.phase = Phase::Failed as i32;
                    scale.error = Some(
                        command
                            .error
                            .unwrap_or_else(|| "node did not confirm service cleanup".into()),
                    );
                    self.save_scale(&key, scale)?;
                    continue;
                }
                // The node writes the target configuration while the service is stopped.
                {
                    if scale.update.is_none() {
                        let mut set = scale.set.as_ref().map(|s| s.0.clone()).unwrap_or_else(|| json!({}));
                        for k in ["gpus", "cpus", "replicas"] {
                            if !scale.target[k].is_null() {
                                set[k] = scale.target[k].clone();
                            }
                        }
                        set["disabled"] = json!(scale.disabled);
                        let message = json!({"service":{"name":text(&intent.service, "name")}});
                        let sent = self.tell_nodes([&intent.worker], message, json!({"set": set}))?;
                        scale.update = sent.into_iter().next();
                        self.save_scale(&key, scale.clone())?;
                    }
                    let command = self.inner.lock().commands.get(scale.update.as_ref().unwrap()).cloned().unwrap();
                    if !command.done {
                        continue;
                    }
                    let written = command.result["updated"].as_array()
                        .is_some_and(|names| names.iter().any(|n| n.as_str() == Some(text(&intent.service, "name"))));
                    if command.error.is_some() || !written {
                        scale.phase = Phase::Failed as i32;
                        scale.error = Some(command.error.unwrap_or_else(|| "node did not write the new configuration".into()));
                        self.save_scale(&key, scale)?;
                        continue;
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
                continue;
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
                        w.report.services.iter().any(|s| {
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
                let mut failed = self.inner.lock().services.get(&key).cloned().unwrap();
                failed.disabled = true;
                failed.scale = Some(scale);
                self.save("services", &key, &failed)?;
                self.inner.lock().services.insert(key, failed);
                continue;
            }
            if !applied {
                if first_ack {
                    self.save_scale(&key, scale)?;
                }
                continue;
            }
            if !scale.disabled {
                self.sync_app_record(&scale.target, false).await?;
            }
            let app = self
                .inner
                .lock()
                .apps
                .get(text(&intent.service, "name"))
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
        }
        Ok(())
    }
}

impl WorkloadController {
    /// Registers a serving app: a `kind: app` service on `node`, which the node writes to its
    /// services.yaml disabled, so `fleetctl app enable` starts it. The name is one of the
    /// namespace's apps and batches, so it is checked against both here and when the node reports.
    pub(super) async fn create_serving_app(&self, c: Option<&Token>, q: &BTreeMap<String, String>, body: Value) -> anyhow::Result<Response> {
        let name = text(&body, "name").to_string();
        if !crate::fleet::valid_app_name(&name) {
            return Err(fail(400, "invalid app name"));
        }
        let node = text(&body, "node").to_string();
        if node.is_empty() {
            return Err(fail(400, "node is required: the node that runs the app"));
        }
        let allowed = ["name", "namespace", "node", "image", "user", "replicas", "gpus", "cpus", "shm_gb",
            "port_base", "health_path", "workdir", "volumes", "env", "command"];
        if body.as_object().is_none_or(|o| o.keys().any(|k| !allowed.contains(&k.as_str()))) {
            return Err(fail(400, "unknown app fields"));
        }
        if !body["image"].as_str().is_some_and(|i| !i.is_empty() && !i.starts_with('-')) {
            return Err(fail(400, "image is required"));
        }
        let (namespace, tag) = self.scope(c, q, body["namespace"].as_str())?;
        WorkloadController::require_grant(c, &name)?;
        WorkloadController::require_admin_settings(c, body.as_object().unwrap(), &Self::ADMIN_SERVICE_SETTINGS)?;
        let replicas = body["replicas"].as_u64().unwrap_or(1);
        if !(1..=64).contains(&replicas) {
            return Err(fail(400, "replicas must be between 1 and 64"));
        }
        let gpus = body["gpus"].as_f64().unwrap_or(1.0);
        if !(gpus >= 0.0 && gpus.is_finite()) || body["cpus"].as_u64().is_none() && !body["cpus"].is_null() {
            return Err(fail(400, "gpus must be a number of cards and cpus a whole number"));
        }
        if !body["env"].is_null() && body["env"].as_object().is_none_or(|e| e.values().any(|v| !v.is_string())) {
            return Err(fail(400, "env must map names to strings"));
        }
        if !body["command"].is_null() && body["command"].as_array().is_none_or(|a| a.iter().any(|v| !v.is_string())) {
            return Err(fail(400, "command must be a list of strings"));
        }
        if !body["volumes"].is_null() && body["volumes"].as_array().is_none_or(|a| a.iter().any(|v| !v.is_string())) {
            return Err(fail(400, "volumes must be a list of paths"));
        }
        let _ops = self.ops.lock().await;
        {
            let i = self.inner.lock();
            if !node.is_empty() && !i.workers.contains_key(&node) {
                return Err(fail(404, "unknown node"));
            }
            if i.apps.get(&name).is_some_and(|a| !a.removed)
                || i.services.values().any(|s| text(&s.service, "name") == name)
                || self.fleet.app_known(&namespace, &name)
            {
                return Err(fail(409, format!("an app or batch named {name} exists")));
            }
        }
        let mut entry = json!({"name": name, "kind": "app", "namespace": namespace, "disabled": true,
            "replicas": replicas, "gpus": gpus, "cpus": body["cpus"].as_u64().unwrap_or(1)});
        if let Some(tag) = &tag {
            entry["tag"] = json!(tag);
        }
        for key in ["image", "user", "shm_gb", "port_base", "health_path", "workdir", "volumes", "env", "command"] {
            if !body[key].is_null() {
                entry[key] = body[key].clone();
            }
        }
        let mut spec = entry.clone();
        spec["priority"] = json!(0);
        let app = WorkloadTemplate {
            name: name.clone(),
            namespace: namespace.clone(),
            tag: tag.clone(),
            spec: spec.into(),
            persistent: true,
            removed: false,
            workers: vec![node.clone()],
        };
        let mut message = self.app_message(&namespace, &name);
        message["create"] = entry;
        self.command(&node, NodeOperation::App, message)?;
        self.save("apps", &name, &app)?;
        self.inner.lock().apps.insert(name, app.clone());
        Ok((StatusCode::CREATED, Json(self.serving_app_value(&app, tag.as_deref()))).into_response())
    }
}
