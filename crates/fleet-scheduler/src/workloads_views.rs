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
                .services
                .values()
                .filter(|intent| {
                    let service = &intent.service;
                    text(service, "kind").starts_with("ziren-")
                        && replica
                            .strip_prefix(&format!("{}-", text(service, "name")))
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

    /// Configured totals across all services/replicas; disabled apps keep their settings.
    /// Quotas are shared namespace limits, not independent limits for each app.
    pub(crate) fn app_resources(&self, namespace: &str, app: &str, tag: Option<&str>) -> Value {
        let i = self.inner.lock();
        let services: Vec<_> = i
            .services
            .values()
            .filter(|intent| {
                let s = &intent.service;
                text(s, "namespace") == namespace
                    && (text(s, "proof_app") == app
                        || (text(s, "kind") == "app" && text(s, "name") == app))
            })
            .collect();
        let mut gpus = 0.0;
        let mut cpus = 0.0;
        let mut replicas = 0u64;
        let mut nodes = std::collections::BTreeSet::new();
        let mut tags = std::collections::BTreeSet::new();
        let mut states = std::collections::BTreeSet::new();
        let mut reasons = Vec::new();
        for intent in &services {
            let s = intent
                .scale
                .as_ref()
                .filter(|s| s.pending())
                .map(|s| s.target())
                .unwrap_or(&intent.service);
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
            let worker = i.workers.get(&intent.worker).filter(|w| now() - w.seen < 30.0);
            let observed = worker.and_then(|w| {
                w.report.services.iter()
                .find(|s| text(s, "name") == text(&intent.service, "name"))
            });
            let state = if intent.scale.as_ref().is_some_and(|s| s.failed()) {
                "failed"
            } else if worker.is_none() {
                "offline"
            } else if let Some(scale) = intent.scale.as_ref().filter(|s| s.pending()) {
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
            "resources":(!services.is_empty()).then(||json!({"gpus":gpus,"cpus":cpus,"replicas":replicas,"nodes":nodes})),
            "tags":tags, "state":state,
            "detail":(state == "starting").then(|| reasons.join("; ")),
            "namespace_quota":n.as_ref().map(|n|&n.quota),
            "tag_quota":tag.and_then(|tag|n.as_ref()?.tag_quotas.get(tag)),
        })
    }

    /// Why an enabled service is not running yet, for `fleetctl app`'s DETAIL column.
    fn starting_reason(commands: &BTreeMap<String, CommandRecord>, intent: &AppDeployment, worker: Option<&NodeSession>) -> String {
        let name = text(&intent.service, "name");
        let pending = commands.values().find(|c| {
            !c.done
                && c.worker == intent.worker
                && c.operation == NodeOperation::SetService as i32
                && text(&c.parameters["service"], "name") == name
        });
        match pending {
            Some(c) if !c.admitted => {
                let s = &c.parameters["service"];
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

    pub(crate) fn proof_app_value(&self, mut app: Value, tag: Option<&str>) -> Value {
        let namespace = text(&app, "namespace");
        let name = format!("{}/{}", text(&app, "tenant"), text(&app, "elf_id"));
        let resources = self.app_resources(namespace, &name, tag);
        app.as_object_mut()
            .unwrap()
            .extend(resources.as_object().unwrap().clone());
        if app["resources"].is_null() && app["disabled"].as_bool() == Some(true) {
            app["state"] = json!("disabled");
        }
        let anchor = app["anchored_on"].as_str();
        let machine = self.machine_name(anchor, None);
        app["diagnostics"] = json!({"anchor_replica":anchor});
        app["anchored_on"] = json!(machine);
        app
    }
}
