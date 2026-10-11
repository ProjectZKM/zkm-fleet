//! Which node jobs a job. Pure, so the rules are tested without a cluster.

use crate::config::{AnchorPolicy, SchedulingClass, TenantConfig};

#[derive(Debug, Clone)]
pub struct PlacementCandidate {
    pub name: String,
    pub backend: String,
    pub gpus: u32,
    pub zkm_version: String,
    /// Heartbeating and its prover answers `Ready`.
    pub ready: bool,
    pub busy: bool,
    /// Tenant this node is reserved for (an exclusive anchor), if any.
    pub reserved_for: Option<String>,
    pub has_elf: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PlacementDecision {
    /// Index into the node slice.
    Selected(usize),
    /// The job's anchor node is healthy but busy: keep the job for it.
    WaitForAnchor,
    /// No eligible node is free.
    NoNode,
}

/// Whether the tenant's node list admits a node of this name.
pub fn allowed(tenant: &TenantConfig, name: &str) -> bool {
    tenant.nodes.is_empty() || tenant.nodes.iter().any(|p| name.starts_with(p.as_str()))
}

pub fn eligible(node: &PlacementCandidate, tenant: &TenantConfig, failed_on: &[String]) -> bool {
    node.ready
        && allowed(tenant, &node.name)
        && !failed_on.iter().any(|f| f == &node.name)
        && tenant
            .zkm_version
            .as_ref()
            .map_or(true, |v| v == &node.zkm_version)
        && (tenant.backends.is_empty() || tenant.backends.iter().any(|b| b == &node.backend))
        && node
            .reserved_for
            .as_ref()
            .map_or(true, |r| r == &tenant.name)
}

/// `anchor` is the name of the node this (tenant, program) is anchored to. Nodes the
/// job failed on are avoided while another node could take it; when none could (a
/// tenant with one node), they are tried again: the attempt limit ends the retries.
pub fn choose(
    tenant: &TenantConfig,
    failed_on: &[String],
    anchor: Option<&str>,
    nodes: &[PlacementCandidate],
) -> PlacementDecision {
    let elsewhere = nodes
        .iter()
        .any(|n| eligible(n, tenant, &[]) && !failed_on.contains(&n.name));
    let failed_on: &[String] = if elsewhere { failed_on } else { &[] };
    if tenant.anchor != AnchorPolicy::None {
        if let Some(name) = anchor {
            if let Some((i, n)) = nodes.iter().enumerate().find(|(_, n)| n.name == name) {
                if eligible(n, tenant, failed_on) {
                    if !n.busy {
                        return PlacementDecision::Selected(i);
                    }
                    // Exclusive: the reserved node is the tenant's; wait for it.
                    // Shared: prefer the anchor, but a free node beats waiting.
                    if tenant.anchor == AnchorPolicy::Exclusive {
                        return PlacementDecision::WaitForAnchor;
                    }
                }
            }
        }
    }
    let mut best: Option<(usize, (bool, i64, &str))> = None;
    for (i, n) in nodes.iter().enumerate() {
        if n.busy || !eligible(n, tenant, failed_on) {
            continue;
        }
        let size = match tenant.class {
            SchedulingClass::Latency => -(n.gpus as i64),
            SchedulingClass::Batch => n.gpus as i64,
        };
        let key = (!n.has_elf, size, n.name.as_str());
        if best.as_ref().map_or(true, |(_, b)| key < *b) {
            best = Some((i, key));
        }
    }
    best.map_or(PlacementDecision::NoNode, |(i, _)| PlacementDecision::Selected(i))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tenant(class: SchedulingClass, anchor: AnchorPolicy) -> TenantConfig {
        TenantConfig {
            name: "t".into(),
            addresses: vec![],
            class,
            anchor,
            zkm_version: Some("v2".into()),
            backends: vec![],
            nodes: vec![],
            max_queued: 8,
        }
    }

    fn node(name: &str, gpus: u32) -> PlacementCandidate {
        PlacementCandidate {
            name: name.into(),
            backend: "cuda".into(),
            gpus,
            zkm_version: "v2".into(),
            ready: true,
            busy: false,
            reserved_for: None,
            has_elf: false,
        }
    }

    #[test]
    fn latency_prefers_the_largest_free_node_and_batch_the_smallest() {
        let nodes = vec![node("a", 1), node("b", 4), node("c", 2)];
        assert_eq!(
            choose(&tenant(SchedulingClass::Latency, AnchorPolicy::None), &[], None, &nodes),
            PlacementDecision::Selected(1)
        );
        assert_eq!(
            choose(&tenant(SchedulingClass::Batch, AnchorPolicy::None), &[], None, &nodes),
            PlacementDecision::Selected(0)
        );
    }

    #[test]
    fn a_node_with_the_program_hot_wins() {
        let mut nodes = vec![node("a", 4), node("b", 1)];
        nodes[1].has_elf = true;
        assert_eq!(
            choose(&tenant(SchedulingClass::Latency, AnchorPolicy::None), &[], None, &nodes),
            PlacementDecision::Selected(1)
        );
    }

    #[test]
    fn version_backend_reservation_and_readiness_exclude() {
        let t = tenant(SchedulingClass::Batch, AnchorPolicy::None);
        let mut nodes = vec![node("a", 1), node("b", 1), node("c", 1), node("d", 1)];
        nodes[0].zkm_version = "v1".into();
        nodes[1].reserved_for = Some("other".into());
        nodes[2].ready = false;
        assert_eq!(choose(&t, &[], None, &nodes), PlacementDecision::Selected(3));
        nodes[3].busy = true;
        assert_eq!(choose(&t, &[], None, &nodes), PlacementDecision::NoNode);
    }

    #[test]
    fn a_failed_node_is_avoided_while_another_could_run_the_job() {
        let t = tenant(SchedulingClass::Batch, AnchorPolicy::None);
        let mut nodes = vec![node("a", 1), node("b", 1)];
        assert_eq!(choose(&t, &["a".into()], None, &nodes), PlacementDecision::Selected(1));
        // The other node is busy, not gone: wait for it rather than retry on `a`.
        nodes[1].busy = true;
        assert_eq!(choose(&t, &["a".into()], None, &nodes), PlacementDecision::NoNode);
        // Only the failed node is left (a tenant with one node): try it again.
        nodes[1].ready = false;
        assert_eq!(choose(&t, &["a".into()], None, &nodes), PlacementDecision::Selected(0));
    }

    #[test]
    fn a_tenant_runs_only_on_the_nodes_it_names() {
        let mut t = tenant(SchedulingClass::Latency, AnchorPolicy::Exclusive);
        t.nodes = vec!["eth-gpu".into()];
        let mut nodes = vec![node("goat-gpu-0", 1), node("eth-gpu-0", 4)];
        nodes[1].ready = false;
        // The only ready node is someone else's: wait rather than run there.
        assert_eq!(choose(&t, &[], None, &nodes), PlacementDecision::NoNode);
        assert_eq!(choose(&t, &[], Some("goat-gpu-0"), &nodes), PlacementDecision::NoNode);
        nodes[1].ready = true;
        assert_eq!(choose(&t, &[], Some("goat-gpu-0"), &nodes), PlacementDecision::Selected(1));
    }

    #[test]
    fn a_tenant_with_one_node_retries_on_it_even_when_anchored() {
        let t = tenant(SchedulingClass::Latency, AnchorPolicy::Exclusive);
        let nodes = vec![node("a", 4)];
        assert_eq!(choose(&t, &["a".into()], Some("a"), &nodes), PlacementDecision::Selected(0));
    }

    #[test]
    fn a_shared_anchor_spills_to_a_free_node_when_busy() {
        let t = tenant(SchedulingClass::Latency, AnchorPolicy::Shared);
        let mut nodes = vec![node("a", 4), node("b", 4)];
        nodes[0].busy = true;
        assert_eq!(choose(&t, &[], Some("a"), &nodes), PlacementDecision::Selected(1));
        nodes[1].busy = true;
        assert_eq!(choose(&t, &[], Some("a"), &nodes), PlacementDecision::NoNode);
    }

    #[test]
    fn an_exclusive_anchor_waits_for_its_busy_node_and_moves_when_it_is_gone() {
        let t = tenant(SchedulingClass::Latency, AnchorPolicy::Exclusive);
        let mut nodes = vec![node("a", 4), node("b", 4)];
        nodes[0].busy = true;
        assert_eq!(choose(&t, &[], Some("a"), &nodes), PlacementDecision::WaitForAnchor);
        nodes[0].busy = false;
        assert_eq!(choose(&t, &[], Some("a"), &nodes), PlacementDecision::Selected(0));
        nodes[0].ready = false;
        assert_eq!(choose(&t, &[], Some("a"), &nodes), PlacementDecision::Selected(1));
        assert_eq!(choose(&t, &[], Some("gone"), &nodes), PlacementDecision::Selected(1));
    }

    #[test]
    fn an_anchor_that_failed_the_job_is_not_reused() {
        let t = tenant(SchedulingClass::Latency, AnchorPolicy::Exclusive);
        let nodes = vec![node("a", 4), node("b", 4)];
        assert_eq!(choose(&t, &["a".into()], Some("a"), &nodes), PlacementDecision::Selected(1));
    }
}
