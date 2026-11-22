//! Live scheduler fence for policy authorization. A reported identity alone is
//! insufficient: deployment intent, session freshness and quota must still hold.
use super::*;
use trusted_shim::{
    contracts::ExecutionIdentity,
    ledger_transport::{ExecutionAuthority, ExecutionGuard},
    rail::{Error, Result},
};

impl ExecutionAuthority for WorkloadController {
    fn guard(&self, execution: &ExecutionIdentity) -> Result<Box<dyn ExecutionGuard + '_>> {
        let i = self.inner.lock();
        let mut owners = i
            .workers
            .iter()
            .filter(|(_, w)| w.report.machine == execution.node_id);
        let (worker_id, worker) = owners.next().ok_or(Error::Rejected)?;
        // A duplicate machine identity is ambiguous even if one session is stale.
        if owners.next().is_some()
            || worker.ended
            || !worker.report.ready
            || now() - worker.seen >= self.fleet.cfg.node_stale_secs as f64
        {
            return Err(Error::Rejected);
        }
        let instance = worker
            .report
            .runtime_instances
            .iter()
            .find(|r| {
                let e = &r.execution;
                e.namespace == execution.namespace
                    && e.workload == execution.workload
                    && e.allocation_id == execution.allocation_id
                    && e.lease_id == execution.lease_id
                    && e.generation == execution.generation
                    && e.node_id == execution.node_id
            })
            .ok_or(Error::Rejected)?;
        let intent = i
            .deployments
            .get(&format!("{worker_id}-{}", instance.entry_name))
            .ok_or(Error::Rejected)?;
        if intent.disabled
            || intent.entry().namespace() != execution.namespace
            || intent.entry().app() != execution.workload
            || intent.scale.as_ref().is_some_and(|s| s.pending())
            || scaling::retiring(&i, intent)
            || !worker
                .report
                .entry(&instance.entry_name)
                .is_some_and(|e| !e.disabled())
        {
            return Err(Error::Rejected);
        }
        let quota = self.fleet.policy_allocation_guard(
            &execution.namespace,
            &format!("deployment:{worker_id}-{}", instance.entry_name),
        )?;
        Ok(Box::new((i, quota)))
    }
}
