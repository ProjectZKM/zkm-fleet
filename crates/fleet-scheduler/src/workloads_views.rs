//! User-facing locations and resource settings. Execution replica IDs remain internal.
use super::*;

impl WorkloadController {
    /// Resolve a machine identity or proof replica to the machine-agent name.
    fn machine_name(&self, replica: Option<&str>, machine: Option<&str>) -> Option<String> {
        let proof_machine = replica.and_then(|id| {
            self.fleet
                .status()
                .nodes
                .into_iter()
                .find(|n| n.name == id)
                .map(|n| n.machine)
        });
        let machine = machine
            .filter(|m| !m.is_empty())
            .or(proof_machine.as_deref());
        let i = self.inner.lock();
        if let Some(machine) = machine {
            return Some(
                i.workers
                    .iter()
                    .find(|(id, w)| id.as_str() == machine || w.report.machine == machine)
                    .map(|(id, _)| id.clone())
                    .unwrap_or_else(|| machine.to_owned()),
            );
        }
        replica.and_then(|replica| {
            let workers: std::collections::BTreeSet<_> = i
                .deployments
                .values()
                .filter(|intent| {
                    let entry = &intent.spec;
                    text(entry, "kind").starts_with("ziren-")
                        && replica
                            .strip_prefix(&format!("{}-", text(entry, "name")))
                            .is_some_and(|index| index.parse::<u32>().is_ok())
                })
                .map(|intent| intent.worker.clone())
                .collect();
            (workers.len() == 1).then(|| workers.into_iter().next().unwrap())
        })
    }

    pub(crate) fn proof_job_value(&self, job: &crate::store::ProofJob) -> Value {
        let mut v = serde_json::to_value(job).unwrap_or_default();
        v["node"] = json!(self.machine_name(
            job.node.as_deref(),
            job.outputs.as_ref().map(|o| o.machine.as_str())
        ));
        v["failed_on"] = json!(job
            .failed_on
            .iter()
            .filter_map(|id| self.machine_name(Some(id), None))
            .collect::<std::collections::BTreeSet<_>>());
        v["diagnostics"] = json!({"execution_replica":job.node,"failed_replicas":job.failed_on});
        v
    }

    /// Configured totals across all deployments and replicas; disabled apps keep their settings.
    /// Quotas are shared namespace limits, not independent limits for each app.
    pub(crate) fn app_resources(&self, namespace: &str, app: &str, tag: Option<&str>) -> Value {
        let i = self.inner.lock();
        let deployments: Vec<_> = i
            .deployments
            .values()
            .filter(|intent| {
                let s = &intent.spec;
                text(s, "namespace") == namespace
                    && text(s, "app") == app
            })
            .collect();
        let mut gpus = 0.0;
        let mut cpus = 0.0;
        let mut replicas = 0u64;
        let mut nodes = std::collections::BTreeSet::new();
        let mut tags = std::collections::BTreeSet::new();
        let mut images = std::collections::BTreeSet::new();
        let mut states = std::collections::BTreeSet::new();
        let mut reasons = Vec::new();
        for intent in &deployments {
            let s = intent
                .scale
                .as_ref()
                .filter(|s| s.pending())
                .map(|s| s.target())
                .unwrap_or(&intent.spec);
            let count = s["replicas"].as_u64().unwrap_or(1);
            replicas += count;
            gpus += s["gpus"]
                .as_f64()
                .unwrap_or(if text(s, "kind") == "ziren-cpu" {
                    0.0
                } else {
                    1.0
                })
                * count as f64;
            cpus += s["cpus"].as_f64().unwrap_or(1.0) * count as f64;
            nodes.insert(intent.worker.clone());
            tags.insert(s["tag"].as_str().unwrap_or("").to_owned());
            if let Some(image) = s["image"].as_str().filter(|i| !i.is_empty()) {
                images.insert(image.to_owned());
            }
            let worker = i.workers.get(&intent.worker).filter(|w| now() - w.seen < 30.0);
            let observed = worker.and_then(|w| {
                w.report.apps.iter()
                .find(|s| text(s, "name") == text(&intent.spec, "name"))
            });
            let state = if intent.scale.as_ref().is_some_and(|s| s.failed()) {
                "failed"
            } else if worker.is_none() {
                "offline"
            } else if let Some(scale) = intent.scale.as_ref().filter(|s| s.pending()) {
                if scale.phase() == fleet_proto::model::v1::ScalePhase::Starting {
                    reasons.push(Self::starting_reason(&i.commands, intent, worker));
                }
                scale.phase().name()
            } else if intent.disabled {
                if observed.is_some_and(|s| s["cleaned"].as_bool() == Some(true)) { "disabled" } else { "draining" }
            } else if worker.is_some_and(|w| w.report.ready) && observed.is_some_and(|s| {
                    s["ready"].as_bool() == Some(true) && s["disabled"].as_bool() != Some(true)
                }) {
                "running"
            } else {
                reasons.push(Self::starting_reason(&i.commands, intent, worker));
                "starting"
            };
            states.insert(state);
        }
        let state = ["failed", "offline", "draining", "starting", "running", "disabled",
        ]
            .into_iter().find(|s| states.contains(s)).unwrap_or("configured");
        let n = self.fleet.namespace(namespace);
        json!({
            "resources":(!deployments.is_empty()).then(||json!({"gpus":gpus,"cpus":cpus,"replicas":replicas,"nodes":nodes})),
            "tags":tags, "images":images, "state":state,
            "detail":(state == "starting").then(|| reasons.join("; ")),
            "namespace_quota":n.as_ref().map(|n|&n.quota),
            "tag_quota":tag.and_then(|tag|n.as_ref()?.tag_quotas.get(tag)),
        })
    }

    /// Why an enabled app is not running yet, for `fleetctl app`'s DETAIL column.
    fn starting_reason(commands: &super::records::Records<CommandRecord>, intent: &AppDeployment, worker: Option<&NodeSession>) -> String {
        let name = text(&intent.spec, "name");
        let pending = commands.open().find(|c| {
            !c.done
                && c.worker == intent.worker
                && c.operation == NodeOperation::SetApp as i32
                && text(&c.parameters["app"], "name") == name
        });
        match pending {
            Some(c) if !c.admitted => {
                let s = &c.parameters["app"];
                let n = s["replicas"].as_f64().unwrap_or(1.0);
                let (g, cpu) = (s["gpus"].as_f64().unwrap_or(0.0) * n, s["cpus"].as_f64().unwrap_or(1.0) * n);
                match worker {
                    Some(w) => format!("waiting for capacity: needs {g} GPU and {cpu} CPU, {} has {} GPU and {} CPU free",
                        intent.worker, w.report.free_gpus, w.report.free_cpus),
                    None => format!("waiting for capacity: needs {g} GPU and {cpu} CPU"),
                }
            }
            Some(_) => format!("starting on {}", intent.worker),
            None => format!("waiting for {} to report it ready", intent.worker),
        }
    }

    /// An app or batch as every listing shows one: what it is, where it runs, its state and
    /// jobs. `sections` add an app's container settings under `spec` and, when it proves a
    /// program, that program and its placement policy under `proof`; a batch's policy under
    /// `batch`.
    #[allow(clippy::too_many_arguments)]
    fn app_view(&self, namespace: &str, name: &str, fixed_tag: Option<&str>, priority: Value, disabled: bool,
        jobs: Value, tag: Option<&str>, sections: Vec<(&str, Value)>,
    ) -> Value {
        let r = self.app_resources(namespace, name, tag);
        let proves = sections.iter().any(|(key, _)| *key == "proof");
        let state = if r["resources"].is_null() {
            if disabled { "disabled" } else if proves { "registered" } else { "configured" }
        } else {
            r["state"].as_str().unwrap_or("configured")
        };
        let mut v = json!({
            "type": "app", "name": name, "namespace": namespace,
            "tag": fixed_tag, "tags": r["tags"], "state": state, "detail": r["detail"],
            "priority": priority, "disabled": disabled, "resources": r["resources"],
            "namespace_quota": r["namespace_quota"], "tag_quota": r["tag_quota"],
            "jobs": jobs,
        });
        for (key, value) in sections {
            v[key] = value;
        }
        if proves {
            // A proof app runs in its provers' container: their image when they agree, each
            // distinct one while an update has moved some of them.
            v["proof"]["image"] = match r["images"].as_array().map(Vec::as_slice) {
                Some([one]) => one.clone(),
                Some([]) | None => Value::Null,
                Some(several) => Value::Array(several.to_vec()),
            };
        }
        v
    }

    /// A template's job records (not purged; the selected tag's when one is), counted the way
    /// every app's `jobs` are.
    fn template_jobs(&self, app: &WorkloadTemplate, tag: Option<&str>) -> Value {
        let i = self.inner.lock();
        let jobs: Vec<&Job> = i.jobs.values()
            .filter(|r| !r.purged && r.batch == app.name && r.namespace == app.namespace
                && tag.is_none_or(|tag| r.tag.as_deref().unwrap_or("") == tag))
            .collect();
        let count = |f: &dyn Fn(&Job) -> bool| jobs.iter().filter(|r| f(r)).count();
        json!({
            "total": jobs.len(),
            "active": count(&|r| !terminal(r.state())),
            "succeeded": count(&|r| r.state() == "succeeded"),
            "failed": count(&|r| matches!(r.state(), "failed" | "lost")),
            "last_at": jobs.iter().filter_map(|r| r.status["submitted_at"].as_f64()).max_by(f64::total_cmp),
        })
    }

    /// A batch in the app shape: `type: batch`, `state` enabled, disabled or deleted, its job
    /// counts under `jobs`, and its image, user, resource policy, mount roots, environment and
    /// workers under `batch`.
    pub(crate) fn batch_value(&self, app: &WorkloadTemplate, tag: Option<&str>) -> Value {
        let jobs = self.template_jobs(app, tag);
        let mut settings = app.spec.0.clone();
        if let Some(o) = settings.as_object_mut() {
            for key in ["name", "type", "kind", "namespace", "namespace_ref", "priority", "disabled", "tag"] {
                o.remove(key);
            }
            o.insert("workers".into(), json!(app.workers));
        }
        let disabled = app.spec["disabled"].as_bool() == Some(true);
        let fixed_tag = app.tag.as_deref().or_else(|| app.spec["tag"].as_str());
        let mut v = self.app_view(&app.namespace, &app.name, fixed_tag, app.spec["priority"].clone(), disabled,
            jobs, tag, vec![("batch", settings)]);
        // A batch runs no app of its own: its state is whether it takes jobs, and its tag
        // is the one it was registered with.
        v["type"] = json!("batch");
        v["state"] = json!(if app.removed { "deleted" } else if disabled { "disabled" } else { "enabled" });
        v["tags"] = json!([fixed_tag.unwrap_or("")]);
        v
    }

    /// An app's container settings as its record shows them, with the nodes it runs on.
    fn spec_value(app: &WorkloadTemplate) -> Value {
        let mut settings = app.spec.0.clone();
        if let Some(o) = settings.as_object_mut() {
            for key in ["name", "namespace", "namespace_ref", "priority", "disabled", "tag", "app", "elf_id"] {
                o.remove(key);
            }
            o.insert("workers".into(), json!(app.workers));
        }
        settings
    }

    /// An app that proves no program, in the app shape.
    pub(crate) fn app_value(&self, app: &WorkloadTemplate, tag: Option<&str>) -> Value {
        let jobs = self.template_jobs(app, tag);
        self.app_view(&app.namespace, &app.name, app.tag.as_deref().or_else(|| app.spec["tag"].as_str()),
            app.spec["priority"].clone(), app.spec["disabled"].as_bool() == Some(true), jobs, tag,
            vec![("spec", Self::spec_value(app))])
    }

    /// An app that proves a program, in the app shape: its proof record, and its container
    /// settings when the scheduler created it; its job counts are the selected tag's.
    pub(crate) async fn proof_app_value(&self, app: &crate::store::App, anchored: Option<&str>, tag: Option<&str>) -> anyhow::Result<Value> {
        let (mut total, mut active, mut succeeded, mut failed, mut last) = (app.jobs, app.active, app.succeeded, app.failed, app.last_created_ms);
        if let Some(tag) = tag {
            let jobs = self.fleet.recent_jobs(&crate::store::ProofJobFilter {
                namespace: Some(app.namespace.clone()), app: Some(app.name.clone()), tag: Some(tag.into()), ..Default::default()
            }, usize::MAX).await?;
            use crate::store::ProofJobState as S;
            total = jobs.len() as u64;
            active = jobs.iter().filter(|j| !j.state().is_terminal()).count() as u64;
            succeeded = jobs.iter().filter(|j| j.state() == S::Succeeded).count() as u64;
            failed = jobs.iter().filter(|j| j.state() == S::Failed).count() as u64;
            last = jobs.iter().map(|j| j.created_ms).max();
        }
        let jobs = json!({
            "total": total, "active": active, "succeeded": succeeded, "failed": failed,
            "last_at": last.map(|ms| ms as f64 / 1000.0),
        });
        let proof = json!({
            "elf_id": app.elf_id, "class": app.class, "anchor": app.anchor, "backends": app.backends,
            "zkm_version": app.zkm_version, "max_queued": app.max_queued, "mean_prove_ms": app.mean_prove_ms,
            "anchored_on": self.machine_name(anchored, None), "created_at": app.created_ms as f64 / 1000.0,
            "diagnostics": {"anchor_replica": anchored},
        });
        let mut sections = vec![("proof", proof)];
        let template = self.inner.lock().apps.get(&app.name).filter(|t| t.persistent && !t.removed && t.namespace == app.namespace).cloned();
        if let Some(template) = template {
            sections.push(("spec", Self::spec_value(&template)));
        }
        Ok(self.app_view(&app.namespace, &app.name, None, json!(app.priority), app.disabled, jobs, tag, sections))
    }

    /// The proof apps the caller sees: a token's namespace's that it is granted, or
    /// an administrator's selected namespace's (every namespace's without one).
    pub(crate) async fn proof_app_values(&self, c: Option<&Token>, q: &BTreeMap<String, String>, tag: Option<&str>) -> anyhow::Result<Vec<Value>> {
        let namespace = match (c, q.get("namespace")) {
            (None, None) => None,
            _ => Some(self.scope(c, q, None)?.0),
        };
        let mut out = Vec::new();
        for (app, anchored) in self.fleet.apps().await? {
            if namespace.as_ref().is_some_and(|n| n != &app.namespace) || Self::require_grant(c, &app.name).is_err() {
                continue;
            }
            out.push(self.proof_app_value(&app, anchored.as_deref(), tag).await?);
        }
        Ok(out)
    }
}
