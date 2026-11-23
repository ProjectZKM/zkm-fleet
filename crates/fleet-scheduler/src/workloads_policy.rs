//! Live scheduler fence for policy authorization. A reported identity alone is
//! insufficient: deployment intent, session freshness and quota must still hold.
use super::*;
use trusted_shim::{
    contracts::ExecutionIdentity,
    ledger_transport::{ExecutionAuthority, ExecutionGuard},
    rail::{Error, Result},
};

pub(super) fn preparation_key(e: &ExecutionIdentity) -> Result<String> {
    if e.generation == 0
        || [
            &e.namespace,
            &e.workload,
            &e.allocation_id,
            &e.lease_id,
            &e.node_id,
        ]
        .iter()
        .any(|s| s.is_empty() || s.len() > 256 || s.contains('\0'))
    {
        return Err(Error::InvalidInput);
    }
    // One immutable identity per admitted command/replica, not one per caller
    // supplied generation. Retain history to fence later generation rollback.
    trusted_shim::rail::rego::commitment(
        "fleet.policy-preparation.v1",
        &(
            &e.namespace,
            &e.workload,
            &e.allocation_id,
            &e.lease_id,
            &e.node_id,
        ),
    )
}

fn same_slot(a: &ExecutionIdentity, b: &ExecutionIdentity) -> bool {
    a.namespace == b.namespace
        && a.workload == b.workload
        && a.allocation_id == b.allocation_id
        && a.node_id == b.node_id
}

impl WorkloadController {
    pub(crate) fn active_native_names(&self) -> Vec<String> {
        let i = self.inner.read();
        i.workers
            .values()
            .filter(|w| !w.ended && now() - w.seen < self.fleet.cfg.node_stale_secs as f64)
            .flat_map(|w| &w.report.runtime_instances)
            .filter_map(|r| {
                trusted_shim::openshell_gateway::GatewayLaunch::execution_name(
                    &crate::policy_gateway::identity(r.execution.clone()),
                )
                .ok()
            })
            .collect()
    }
}

fn owner<'a>(
    i: &'a WorkloadState,
    e: &ExecutionIdentity,
    stale: u64,
) -> Result<(&'a str, &'a NodeSession)> {
    let mut owners = i.workers.iter().filter(|(_, w)| {
        w.report.machine == e.node_id && !w.ended && now() - w.seen < stale as f64
    });
    let (id, w) = owners.next().ok_or(Error::Rejected)?;
    if owners.next().is_some() || w.ended || !w.report.ready || now() - w.seen >= stale as f64 {
        return Err(Error::Rejected);
    }
    Ok((id, w))
}

fn pending_entry<'a>(i: &'a WorkloadState, worker: &str, e: &ExecutionIdentity) -> Result<&'a str> {
    let c = i.commands.get(&e.lease_id).ok_or(Error::Rejected)?;
    let entry = c.entry();
    if c.worker != worker
        || c.done
        || !c.admitted
        || c.error.is_some()
        || !c.is(NodeOperation::SetApp)
        || !c.starts()
        || entry.runtime_capability().is_none()
        || entry.namespace() != e.namespace
        || entry.app() != e.workload
    {
        return Err(Error::Rejected);
    }
    let index = e
        .allocation_id
        .strip_prefix(&format!("{}-", entry.name()))
        .and_then(|n| n.parse::<u64>().ok())
        .ok_or(Error::Rejected)?;
    if index >= entry.replicas() || e.allocation_id != format!("{}-{index}", entry.name()) {
        return Err(Error::Rejected);
    }
    let intent = i
        .deployments
        .get(&format!("{worker}-{}", entry.name()))
        .ok_or(Error::Rejected)?;
    if intent.spec != c.parameters["app"] {
        return Err(Error::Rejected);
    }
    if intent.scale.as_ref().is_some_and(|s| {
        s.phase() != fleet_proto::model::v1::ScalePhase::Starting
            || s.start.as_deref() != Some(e.lease_id.as_str())
            || c.scale_id() != Some(s.id())
    }) {
        return Err(Error::Rejected);
    }
    Ok(entry.name())
}

fn intent(
    i: &WorkloadState,
    worker_id: &str,
    worker: &NodeSession,
    entry: &str,
    e: &ExecutionIdentity,
) -> Result<()> {
    let intent = i
        .deployments
        .get(&format!("{worker_id}-{entry}"))
        .ok_or(Error::Rejected)?;
    if intent.disabled
        || intent.entry().namespace() != e.namespace
        || intent.entry().app() != e.workload
        || intent.scale.as_ref().is_some_and(|s| {
            s.pending()
                && !(s.phase() == fleet_proto::model::v1::ScalePhase::Starting
                    && s.start.as_deref() == Some(e.lease_id.as_str())
                    && i.commands.get(&e.lease_id).is_some_and(|c| {
                        c.admitted
                            && c.error.is_none()
                            && c.starts()
                            && c.scale_id() == Some(s.id())
                    }))
        })
        || scaling::retiring(i, intent)
        || !worker.report.entry(entry).is_some_and(|e| !e.disabled())
    {
        return Err(Error::Rejected);
    }
    Ok(())
}

impl WorkloadController {
    pub(super) fn native_route_ready(
        &self,
        i: &WorkloadState,
        instance: &fleet_proto::model::v1::RuntimeInstance,
    ) -> bool {
        let execution = crate::policy_gateway::identity(instance.execution.clone());
        let Ok((worker_id, worker)) = owner(i, &execution, self.fleet.cfg.node_stale_secs) else {
            return false;
        };
        if worker.draining
            || i.policy_preparations
                .values()
                .any(|old| same_slot(old, &execution) && old.generation > execution.generation)
            || intent(i, worker_id, worker, &instance.entry_name, &execution).is_err()
        {
            return false;
        }
        if self
            .fleet
            .policy_allocation_admits(&execution.namespace, &format!("deployment:{worker_id}-{}", instance.entry_name))
            .is_err()
        {
            return false;
        }
        crate::openshell::services_ready(self, instance, &worker.session)
    }

    pub(crate) fn native_observation(&self, e: &ExecutionIdentity) -> Result<Option<String>> {
        self.with_runtime_delivery_info(e, None, None, |_, instance, _| {
            Ok(instance.native_observation_base64.clone())
        })
    }
    /// Ray's device grant for a native reservation, as its owning node reports it.
    pub(crate) fn native_gpus(&self, e: &ExecutionIdentity) -> Result<Vec<String>> {
        self.with_runtime_delivery_info(e, None, None, |_, instance, _| Ok(instance.gpu_ids.clone()))
    }
    pub(crate) fn native_readiness_basis(
        &self,
        launch: &trusted_shim::runtime::LaunchRequest,
    ) -> Result<(String, String)> {
        self.with_runtime_delivery_info(
            &launch.execution,
            None,
            Some(launch),
            |_, instance, session| {
                Ok((
                    instance
                        .native_observation_base64
                        .clone()
                        .ok_or(Error::Unavailable)?,
                    session.to_owned(),
                ))
            },
        )
    }
    /// Hold the execution fence through a private delivery transition: the
    /// deployment, session and quota checked here stay valid until `op` returns.
    /// Native reservations must be reported by the owning fresh node session.
    pub(crate) fn with_runtime_delivery<T>(
        &self,
        e: &ExecutionIdentity,
        caller: Option<(&str, &str)>,
        launch: Option<&trusted_shim::runtime::LaunchRequest>,
        op: impl FnOnce(&str) -> Result<T>,
    ) -> Result<T> {
        self.with_runtime_delivery_info(e, caller, launch, |worker, _, _| op(worker))
    }
    fn with_runtime_delivery_info<T>(
        &self,
        e: &ExecutionIdentity,
        caller: Option<(&str, &str)>,
        launch: Option<&trusted_shim::runtime::LaunchRequest>,
        op: impl FnOnce(&str, &fleet_proto::model::v1::RuntimeInstance, &str) -> Result<T>,
    ) -> Result<T> {
        let _fence = self.fleet.execution_fence.read();
        let (worker_id, instance, session) = self.delivery_basis(e, caller, launch)?;
        op(&worker_id, &instance, &session)
    }
    /// Validates under brief locks; the caller holds the execution fence.
    fn delivery_basis(
        &self,
        e: &ExecutionIdentity,
        caller: Option<(&str, &str)>,
        launch: Option<&trusted_shim::runtime::LaunchRequest>,
    ) -> Result<(String, fleet_proto::model::v1::RuntimeInstance, String)> {
        let i = self.inner.read();
        let (worker_id, worker) = owner(&i, e, self.fleet.cfg.node_stale_secs)?;
        if caller.is_some_and(|(id, session)| id != worker_id || session != worker.session)
            || worker.draining
            || i.policy_preparations
                .values()
                .any(|old| same_slot(old, e) && old.generation > e.generation)
        {
            return Err(Error::Rejected);
        }
        let instance = worker
            .report
            .runtime_instances
            .iter()
            .find(|r| crate::policy_gateway::identity(r.execution.clone()) == *e)
            .ok_or(Error::Rejected)?;
        intent(&i, worker_id, worker, &instance.entry_name, e)?;
        let deployment = i
            .deployments
            .get(&format!("{worker_id}-{}", instance.entry_name))
            .ok_or(Error::Rejected)?;
        let spec = &deployment.spec;
        if !fleet_proto::workload::Entry(spec).native_access() {
            return Err(Error::Rejected);
        }
        if let Some(r) = launch {
            // The native decoder separately binds gateway bytes to this launch.
            // These checks bind it to the resources and workload Fleet admitted.
            let env = spec
                .get("env")
                .cloned()
                .unwrap_or_else(|| serde_json::json!({}));
            let command = spec
                .get("command")
                .cloned()
                .unwrap_or_else(|| serde_json::json!([]));
            if r.execution != *e
                || spec["runtime"]["driver"] != r.driver
                || spec.get("access_policy_sha256").and_then(serde_json::Value::as_str) != r.access_policy_sha256.as_deref()
                || spec["image"] != r.image
                || command != serde_json::json!(r.command)
                || env != serde_json::json!(r.environment)
                || r.limits.cpu_millicores
                    != spec["cpus"].as_f64().map(|n| (n * 1000.0).round() as u64)
                || r.limits.memory_bytes
                    != spec["memory_gb"]
                        .as_f64()
                        .map(|n| (n * (1u64 << 30) as f64).round() as u64)
                || !match (&r.trust.attestation, &r.trust.attestation_proof, r.driver.as_str()) {
                    (trusted_shim::attestation::AttestationRequirement::SkipNonTee, trusted_shim::proof::ProofRequirement::NotRequired, "docker" | "openshell") => true,
                    (trusted_shim::attestation::AttestationRequirement::Required { profile }, trusted_shim::proof::ProofRequirement::Required, "tee") => matches!(profile.as_str(), "snp" | "tdx"),
                    (trusted_shim::attestation::AttestationRequirement::Required { profile }, trusted_shim::proof::ProofRequirement::Required, "tee-simulation") => profile == "sample",
                    _ => false,
                }
                || r.trust.policy_proof != trusted_shim::proof::ProofRequirement::NotRequired
                // Exactly the devices Ray granted this reservation on its node,
                // and exactly the admitted durable volumes.
                || r.gpus != instance.gpu_ids
                || r.gpus.len() as f64 != spec["gpus"].as_f64().unwrap_or(0.0)
                || spec.get("mounts").map_or(Some(Vec::new()), |m| {
                    serde_json::from_value::<Vec<trusted_shim::runtime::DurableMount>>(m.clone()).ok()
                }).as_ref() != Some(&r.mounts)
            {
                return Err(Error::Rejected);
            }
            let options = trusted_shim::openshell_services::ServiceOptions::from_request(r)
                .map_err(|_| Error::Rejected)?;
            let names: Vec<_> = r.services.iter().map(|s| s.id.service.as_str()).collect();
            let protocol = spec.get("service_protocol").and_then(serde_json::Value::as_str).unwrap_or("http1");
            if serde_json::json!(names)
                != spec
                    .get("services")
                    .cloned()
                    .unwrap_or(serde_json::json!([]))
                // The logs-only control service is always HTTP.
                || r.services.iter().any(|s| serde_json::json!(s.protocol) != if s.id.service == "control" { "http1" } else { protocol })
                || options.as_ref().is_some_and(|o| {
                    spec["container_port"] != o.container_port
                        // Exactly the admitted ingress rules; shim binds each
                        // service's endpoint to its rule's presence.
                        || spec.get("ingress").map_or(Some(Default::default()), |i| {
                            serde_json::from_value::<std::collections::BTreeMap<String, trusted_shim::management::Ingress>>(i.clone()).ok()
                        }).as_ref() != Some(&o.ingress)
                        || spec.get("health_tcp_banner").and_then(serde_json::Value::as_str) != o.health_tcp_banner.as_deref()
                        || spec
                            .get("health_path")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("/")
                            != o.health_path
                })
            {
                return Err(Error::Rejected);
            }
        }
        self.fleet.policy_allocation_admits(
            &e.namespace,
            &format!("deployment:{worker_id}-{}", instance.entry_name),
        )?;
        Ok((worker_id.to_owned(), instance.clone(), worker.session.clone()))
    }

    /// An operator may prepare exactly one identity for an already admitted
    /// command's replica. This neither schedules a workload nor changes legacy
    /// execution modes. Store before activation/remote proof/dispatch.
    pub(crate) fn prepare_policy_execution(&self, e: &ExecutionIdentity) -> Result<()> {
        let key = preparation_key(e)?;
        let mut i = self.inner.lock();
        let (worker_id, worker) = owner(&i, e, self.fleet.cfg.node_stale_secs)?;
        let entry = pending_entry(&i, worker_id, e)?;
        intent(&i, worker_id, worker, entry, e)?;
        if worker.report.runtime_instances.iter().any(|r| {
            let observed = crate::policy_gateway::identity(r.execution.clone());
            same_slot(&observed, e)
        }) {
            return Err(Error::Conflict);
        }
        // Exclusive: this lock holds the execution fence for the durable write.
        self.fleet
            .policy_allocation_admits(&e.namespace, &format!("deployment:{worker_id}-{entry}"))?;
        if let Some(previous) = i.policy_preparations.get(&key) {
            return if previous == e {
                Ok(())
            } else {
                Err(Error::Conflict)
            };
        }
        if i.policy_preparations.values().any(|old| {
            same_slot(old, e)
                && (old.generation >= e.generation
                    || i.commands.get(&old.lease_id).is_some_and(|c| !c.done))
        }) {
            return Err(Error::Conflict);
        }
        self.save("policy-preparations", &key, e)
            .map_err(|_| Error::Unavailable)?;
        i.policy_preparations.insert(key, e.clone());
        Ok(())
    }
}

impl ExecutionAuthority for WorkloadController {
    /// The fence, held shared, is the guard: every change that could invalidate
    /// these checks waits for it, while unrelated readers of the workload state
    /// and other guards proceed through the ledger transaction.
    fn guard(&self, execution: &ExecutionIdentity) -> Result<Box<dyn ExecutionGuard + '_>> {
        let fence = self.fleet.execution_fence.read();
        self.admit(execution)?;
        Ok(Box::new(fence))
    }
}

impl WorkloadController {
    fn admit(&self, execution: &ExecutionIdentity) -> Result<()> {
        let i = self.inner.read();
        let (worker_id, worker) = owner(&i, execution, self.fleet.cfg.node_stale_secs)?;
        if i.policy_preparations
            .values()
            .any(|e| same_slot(e, execution) && e.generation > execution.generation)
        {
            return Err(Error::Rejected);
        }
        let instance = worker.report.runtime_instances.iter().find(|r| {
            let e = &r.execution;
            e.namespace == execution.namespace
                && e.workload == execution.workload
                && e.allocation_id == execution.allocation_id
                && e.lease_id == execution.lease_id
                && e.generation == execution.generation
                && e.node_id == execution.node_id
        });
        let entry = if let Some(instance) = instance {
            instance.entry_name.as_str()
        } else {
            if i.policy_preparations.get(&preparation_key(execution)?) != Some(execution)
                || worker.report.runtime_instances.iter().any(|r| {
                    same_slot(
                        &crate::policy_gateway::identity(r.execution.clone()),
                        execution,
                    )
                })
            {
                return Err(Error::Rejected);
            }
            pending_entry(&i, worker_id, execution)?
        };
        intent(&i, worker_id, worker, entry, execution)?;
        self.fleet.policy_allocation_admits(
            &execution.namespace,
            &format!("deployment:{worker_id}-{entry}"),
        )
    }
}
