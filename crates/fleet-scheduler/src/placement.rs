//! Which node runs a job. Pure, so the rules are tested without a cluster.

use crate::config::{Anchor, Class, TenantConfig};

#[derive(Debug, Clone)]
pub struct NodeView {
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
pub enum Choice {
    /// Index into the node slice.
    Run(usize),
    /// The job's anchor node is healthy but busy: keep the job for it.
    WaitForAnchor,
    /// No eligible node is free.
    NoNode,
}

pub fn eligible(node: &NodeView, tenant: &TenantConfig, failed_on: &[String]) -> bool {
    node.ready
        && !failed_on.iter().any(|f| f == &node.name)
        && tenant.zkm_version.as_ref().map_or(true, |v| v == &node.zkm_version)
        && (tenant.backends.is_empty() || tenant.backends.iter().any(|b| b == &node.backend))
        && node.reserved_for.as_ref().map_or(true, |r| r == &tenant.name)
}

/// `anchor` is the name of the node this (tenant, program) is anchored to.
pub fn choose(
    tenant: &TenantConfig,
    failed_on: &[String],
    anchor: Option<&str>,
    nodes: &[NodeView],
) -> Choice {
    if tenant.anchor != Anchor::None {
        if let Some(name) = anchor {
            if let Some((i, n)) = nodes.iter().enumerate().find(|(_, n)| n.name == name) {
                if eligible(n, tenant, failed_on) {
                    if !n.busy {
                        return Choice::Run(i);
                    }
                    // Exclusive: the reserved node is the tenant's; wait for it.
                    // Shared: prefer the anchor, but a free node beats waiting.
                    if tenant.anchor == Anchor::Exclusive {
                        return Choice::WaitForAnchor;
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
            Class::Latency => -(n.gpus as i64),
            Class::Batch => n.gpus as i64,
        };
        let key = (!n.has_elf, size, n.name.as_str());
        if best.as_ref().map_or(true, |(_, b)| key < *b) {
            best = Some((i, key));
        }
    }
    best.map_or(Choice::NoNode, |(i, _)| Choice::Run(i))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tenant(class: Class, anchor: Anchor) -> TenantConfig {
        TenantConfig {
            name: "t".into(),
            addresses: vec![],
            class,
            anchor,
            zkm_version: Some("v2".into()),
            backends: vec![],
            max_queued: 8,
        }
    }

    fn node(name: &str, gpus: u32) -> NodeView {
        NodeView {
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
        assert_eq!(choose(&tenant(Class::Latency, Anchor::None), &[], None, &nodes), Choice::Run(1));
        assert_eq!(choose(&tenant(Class::Batch, Anchor::None), &[], None, &nodes), Choice::Run(0));
    }

    #[test]
    fn a_node_with_the_program_hot_wins() {
        let mut nodes = vec![node("a", 4), node("b", 1)];
        nodes[1].has_elf = true;
        assert_eq!(choose(&tenant(Class::Latency, Anchor::None), &[], None, &nodes), Choice::Run(1));
    }

    #[test]
    fn version_backend_reservation_and_failures_exclude() {
        let t = tenant(Class::Batch, Anchor::None);
        let mut nodes = vec![node("a", 1), node("b", 1), node("c", 1), node("d", 1)];
        nodes[0].zkm_version = "v1".into();
        nodes[1].reserved_for = Some("other".into());
        nodes[2].ready = false;
        assert_eq!(choose(&t, &["d".into()], None, &nodes), Choice::NoNode);
        assert_eq!(choose(&t, &[], None, &nodes), Choice::Run(3));
    }

    #[test]
    fn a_shared_anchor_spills_to_a_free_node_when_busy() {
        let t = tenant(Class::Latency, Anchor::Shared);
        let mut nodes = vec![node("a", 4), node("b", 4)];
        nodes[0].busy = true;
        assert_eq!(choose(&t, &[], Some("a"), &nodes), Choice::Run(1));
        nodes[1].busy = true;
        assert_eq!(choose(&t, &[], Some("a"), &nodes), Choice::NoNode);
    }

    #[test]
    fn an_exclusive_anchor_waits_for_its_busy_node_and_moves_when_it_is_gone() {
        let t = tenant(Class::Latency, Anchor::Exclusive);
        let mut nodes = vec![node("a", 4), node("b", 4)];
        nodes[0].busy = true;
        assert_eq!(choose(&t, &[], Some("a"), &nodes), Choice::WaitForAnchor);
        nodes[0].busy = false;
        assert_eq!(choose(&t, &[], Some("a"), &nodes), Choice::Run(0));
        nodes[0].ready = false;
        assert_eq!(choose(&t, &[], Some("a"), &nodes), Choice::Run(1));
        assert_eq!(choose(&t, &[], Some("gone"), &nodes), Choice::Run(1));
    }

    #[test]
    fn an_anchor_that_failed_the_job_is_not_reused() {
        let t = tenant(Class::Latency, Anchor::Exclusive);
        let nodes = vec![node("a", 4), node("b", 4)];
        assert_eq!(choose(&t, &["a".into()], Some("a"), &nodes), Choice::Run(1));
    }
}
