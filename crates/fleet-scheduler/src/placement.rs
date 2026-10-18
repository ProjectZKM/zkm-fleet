//! Which node runs a job. Pure, so the rules are tested without a cluster.

/// How an app's jobs are placed: its registration's class, anchor, backends and version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppPolicy {
    /// The app, `namespace/name`: what an exclusive anchor reserves its node for.
    pub key: String,
    pub class: SchedulingClass,
    pub anchor: AnchorPolicy,
    /// Only nodes proving with this circuit version serve the app.
    pub zkm_version: Option<String>,
    /// Allowed node backends ("cuda", "cpu", "mock"); empty means any.
    pub backends: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SchedulingClass {
    /// The biggest free prover first.
    Latency,
    /// The smallest prover that is free.
    #[default]
    Batch,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum AnchorPolicy {
    /// Any eligible node.
    #[default]
    None,
    /// Keep a program on the node it first ran on while that node is healthy.
    Shared,
    /// As `shared`, and the anchor node serves this app only.
    Exclusive,
}

impl SchedulingClass {
    pub const NAMES: [&'static str; 2] = ["latency", "batch"];

    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "latency" => Some(Self::Latency),
            "batch" => Some(Self::Batch),
            _ => None,
        }
    }
}

impl AnchorPolicy {
    pub const NAMES: [&'static str; 3] = ["none", "shared", "exclusive"];

    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "none" => Some(Self::None),
            "shared" => Some(Self::Shared),
            "exclusive" => Some(Self::Exclusive),
            _ => None,
        }
    }
}

impl AppPolicy {
    /// A registered app's policy; its names were checked when it was registered.
    pub fn of(app: &fleet_proto::model::v1::App) -> Self {
        Self {
            key: app_key(&app.namespace, &app.name),
            class: SchedulingClass::from_name(&app.class).unwrap_or_default(),
            anchor: AnchorPolicy::from_name(&app.anchor).unwrap_or_default(),
            zkm_version: app.zkm_version.clone(),
            backends: app.backends.clone(),
        }
    }
}

/// An app's key across namespaces: `namespace/name`.
pub fn app_key(namespace: &str, name: &str) -> String {
    format!("{namespace}/{name}")
}

#[derive(Debug, Clone)]
pub struct PlacementCandidate {
    pub name: String,
    pub backend: String,
    pub gpus: u32,
    pub zkm_version: String,
    /// Heartbeating and its prover answers `Ready`.
    pub ready: bool,
    pub busy: bool,
    /// The app this node is reserved for (an exclusive anchor), if any.
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

pub fn eligible(node: &PlacementCandidate, app: &AppPolicy, failed_on: &[String]) -> bool {
    node.ready
        && !failed_on.iter().any(|f| f == &node.name)
        && app.zkm_version.as_ref().map_or(true, |v| v == &node.zkm_version)
        && (app.backends.is_empty() || app.backends.iter().any(|b| b == &node.backend))
        && node.reserved_for.as_ref().map_or(true, |r| r == &app.key)
}

/// `anchor` is the name of the node this app is anchored to. Nodes the job failed on are
/// avoided while another node could take it; when none could (an app with one node), they
/// are tried again: the attempt limit ends the retries.
pub fn choose(
    app: &AppPolicy,
    failed_on: &[String],
    anchor: Option<&str>,
    nodes: &[PlacementCandidate],
) -> PlacementDecision {
    let elsewhere = nodes
        .iter()
        .any(|n| eligible(n, app, &[]) && !failed_on.contains(&n.name));
    let failed_on: &[String] = if elsewhere { failed_on } else { &[] };
    if app.anchor != AnchorPolicy::None {
        if let Some(name) = anchor {
            if let Some((i, n)) = nodes.iter().enumerate().find(|(_, n)| n.name == name) {
                if eligible(n, app, failed_on) {
                    if !n.busy {
                        return PlacementDecision::Selected(i);
                    }
                    // Exclusive: the reserved node is the app's; wait for it.
                    // Shared: prefer the anchor, but a free node beats waiting.
                    if app.anchor == AnchorPolicy::Exclusive {
                        return PlacementDecision::WaitForAnchor;
                    }
                }
            }
        }
    }
    let mut best: Option<(usize, (bool, i64, &str))> = None;
    for (i, n) in nodes.iter().enumerate() {
        if n.busy || !eligible(n, app, failed_on) {
            continue;
        }
        let size = match app.class {
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

    fn app(class: SchedulingClass, anchor: AnchorPolicy) -> AppPolicy {
        AppPolicy { key: "n/t".into(), class, anchor, zkm_version: Some("v2".into()), backends: vec![] }
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
            choose(&app(SchedulingClass::Latency, AnchorPolicy::None), &[], None, &nodes),
            PlacementDecision::Selected(1)
        );
        assert_eq!(
            choose(&app(SchedulingClass::Batch, AnchorPolicy::None), &[], None, &nodes),
            PlacementDecision::Selected(0)
        );
    }

    #[test]
    fn a_node_with_the_program_hot_wins() {
        let mut nodes = vec![node("a", 4), node("b", 1)];
        nodes[1].has_elf = true;
        assert_eq!(
            choose(&app(SchedulingClass::Latency, AnchorPolicy::None), &[], None, &nodes),
            PlacementDecision::Selected(1)
        );
    }

    #[test]
    fn version_backend_reservation_and_readiness_exclude() {
        let t = app(SchedulingClass::Batch, AnchorPolicy::None);
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
        let t = app(SchedulingClass::Batch, AnchorPolicy::None);
        let mut nodes = vec![node("a", 1), node("b", 1)];
        assert_eq!(choose(&t, &["a".into()], None, &nodes), PlacementDecision::Selected(1));
        // The other node is busy, not gone: wait for it rather than retry on `a`.
        nodes[1].busy = true;
        assert_eq!(choose(&t, &["a".into()], None, &nodes), PlacementDecision::NoNode);
        // Only the failed node is left (an app with one node): try it again.
        nodes[1].ready = false;
        assert_eq!(choose(&t, &["a".into()], None, &nodes), PlacementDecision::Selected(0));
    }

    #[test]
    fn an_app_with_one_node_retries_on_it_even_when_anchored() {
        let t = app(SchedulingClass::Latency, AnchorPolicy::Exclusive);
        let nodes = vec![node("a", 4)];
        assert_eq!(choose(&t, &["a".into()], Some("a"), &nodes), PlacementDecision::Selected(0));
    }

    #[test]
    fn a_shared_anchor_spills_to_a_free_node_when_busy() {
        let t = app(SchedulingClass::Latency, AnchorPolicy::Shared);
        let mut nodes = vec![node("a", 4), node("b", 4)];
        nodes[0].busy = true;
        assert_eq!(choose(&t, &[], Some("a"), &nodes), PlacementDecision::Selected(1));
        nodes[1].busy = true;
        assert_eq!(choose(&t, &[], Some("a"), &nodes), PlacementDecision::NoNode);
    }

    #[test]
    fn an_exclusive_anchor_waits_for_its_busy_node_and_moves_when_it_is_gone() {
        let t = app(SchedulingClass::Latency, AnchorPolicy::Exclusive);
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
        let t = app(SchedulingClass::Latency, AnchorPolicy::Exclusive);
        let nodes = vec![node("a", 4), node("b", 4)];
        assert_eq!(choose(&t, &["a".into()], Some("a"), &nodes), PlacementDecision::Selected(1));
    }
}
