//! User-facing locations and resource settings. Execution replica IDs remain internal.
use super::*;

impl RunControl {
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

    pub(crate) fn proof_job_value(&self, job: &crate::store::Job) -> Value {
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
        }
        let n = self.fleet.namespace(namespace);
        json!({
            "resources":(!services.is_empty()).then(||json!({"gpus":gpus,"cpus":cpus,"replicas":replicas,"nodes":nodes})),
            "namespace_quota":n.as_ref().map(|n|&n.quota),
            "tag_quota":tag.and_then(|tag|n.as_ref()?.tag_quotas.get(tag)),
        })
    }

    pub(crate) fn proof_app_value(&self, mut app: Value, tag: Option<&str>) -> Value {
        let namespace = text(&app, "namespace");
        let name = format!("{}/{}", text(&app, "tenant"), text(&app, "elf_id"));
        let resources = self.app_resources(namespace, &name, tag);
        app.as_object_mut()
            .unwrap()
            .extend(resources.as_object().unwrap().clone());
        let anchor = app["anchored_on"].as_str();
        let machine = self.machine_name(anchor, None);
        app["diagnostics"] = json!({"anchor_replica":anchor});
        app["anchored_on"] = json!(machine);
        app
    }
}
