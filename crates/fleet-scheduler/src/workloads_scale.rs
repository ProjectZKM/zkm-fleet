//! App-owner desired resource settings, reconciled through unary heartbeats.
use super::*;

use fleet_proto::model::v1::{ScalePhase as Phase, ScaleRequest, ScaleStatus};

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
        }
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
                && (text(&s.service, "proof_app") == app || text(&s.service, "name") == app)
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
                && ((text(s, "kind") == "ziren-gpu" && text(s, "proof_app") == app)
                    || (text(s, "kind") == "app" && text(s, "name") == app))
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
        let (key, mut intent) = self.scale_service(namespace, app, req.node.as_deref())?;
        // Recheck existence under the operation lock: deletion may have finished
        // after the HTTP handler resolved the app but before this request acquired it.
        let exists = if let Some((tenant, elf)) = app.split_once('/') {
            self.fleet.app_known(namespace, tenant, elf)
        } else {
            self.inner
                .lock()
                .apps
                .get(app)
                .is_some_and(|a| !a.removed && a.persistent && a.namespace == namespace)
        };
        if !exists {
            return Err(fail(404, "unknown app"));
        }
        {
            let i = self.inner.lock();
            // A scale whose start still waits for capacity has nothing on the node to protect
            // (its old server is stopped and the start was never sent): a new size replaces it.
            let waiting_start = intent.scale.as_ref().is_some_and(|s| {
                s.phase() == Phase::Starting
                    && s.start.as_ref().is_some_and(|id| i.commands.get(id).is_some_and(|c| !c.done && !c.admitted))
            });
            if intent.scale.as_ref().is_some_and(ScaleStatus::pending) && !waiting_start
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
        let mut target = intent.service.clone();
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
                if text(&intent.service, "proof_app").contains('/') {
                    self.sync_proof_app(&intent.service, true).await?;
                } else {
                    let app = self
                        .inner
                        .lock()
                        .apps
                        .get(text(&intent.service, "name"))
                        .cloned();
                    if let Some(mut app) = app {
                        app.profile["disabled"] = json!(true);
                        self.save("apps", &app.name, &app)?;
                        self.inner.lock().apps.insert(app.name.clone(), app);
                    }
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
                self.sync_proof_app(&scale.target, false).await?;
            }
            let app = self
                .inner
                .lock()
                .apps
                .get(text(&intent.service, "name"))
                .cloned();
            if let Some(mut app) = app.filter(|a| a.persistent) {
                for k in ["gpus", "cpus", "replicas"] {
                    app.profile[k] = scale.target[k].clone();
                }
                app.profile["disabled"] = json!(scale.disabled);
                self.save("apps", &app.name, &app)?;
                self.inner.lock().apps.insert(app.name.clone(), app);
            }
            scale.phase = Phase::Complete as i32;
            self.save_scale(&key, scale)?;
        }
        Ok(())
    }
}
