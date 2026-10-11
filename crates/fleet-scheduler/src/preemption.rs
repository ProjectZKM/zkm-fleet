//! Preemption planning for job and persistent-app work.
//!
//! Ray remains responsible for reporting free capacity and actually stopping actors.
//! Scheduler owns the namespace, priority, and quota decision so those rules have one source.

use crate::quota::{Allocation, Quota};
#[cfg(test)]
use fleet_proto::model::v1::Usage;
pub use fleet_proto::model::v1::{
    NodeCapacity, PreemptionCandidate, PreemptionPlan,
    PreemptionRequest,
};
use std::collections::{HashMap, HashSet};

pub fn plan(
    request: PreemptionRequest,
    namespace_quota: &Quota,
    tag_quota: &Quota,
    allocations: &[Allocation],
) -> anyhow::Result<Option<PreemptionPlan>> {
    validate(&request)?;
    let eligible: Vec<_> = request
        .candidates
        .iter()
        .filter(|c| {
            c.namespace == request.namespace
                && c.priority < request.priority
                && takeable(c, request.now)
        })
        .cloned()
        .collect();

    let quota_fits = |victims: &HashSet<String>| {
        let (demand_gpus, demand_cpus) = request.quota_demand
            .as_ref()
            .map(|d| (d.gpus, d.cpus))
            .unwrap_or((0.0, 0.0));
        let mut namespace_gpus = demand_gpus;
        let mut namespace_cpus = demand_cpus;
        let mut tag_gpus = demand_gpus;
        let mut tag_cpus = demand_cpus;
        for allocation in allocations.iter().filter(|a| {
            a.namespace == request.namespace
                && request.exclude_allocation.as_deref() != Some(a.id.as_str())
                && !allocation_is_selected_job(&a.id, victims)
        }) {
            namespace_gpus += allocation.gpus;
            namespace_cpus += allocation.cpus;
            if allocation.tag == request.tag {
                tag_gpus += allocation.gpus;
                tag_cpus += allocation.cpus;
            }
        }
        namespace_quota.fits(namespace_gpus, namespace_cpus) && tag_quota.fits(tag_gpus, tag_cpus)
    };

    let all_eligible: HashSet<_> = eligible.iter().map(|c| c.id.clone()).collect();
    if !quota_fits(&all_eligible) {
        return Ok(None);
    }

    let mut available: HashMap<String, (f64, f64)> = request
        .free
        .iter()
        .map(|n| (n.node_ip.clone(), (n.gpus, n.cpus)))
        .collect();
    let free_initial = available.clone();
    let mut selected = HashSet::new();
    let mut order = Vec::new();
    let mut placed = Vec::new();

    for rank in 0..request.nodes {
        let pin = request.pins.get(rank as usize).and_then(|p| p.as_deref());
        let choices = choose_victims(
            request.gpus,
            request.cpus.max(1.0),
            &available,
            &eligible,
            &selected,
            &request.namespace,
            request.priority,
            request.now,
            pin,
        );
        let Some((node, victims)) = choices else {
            return Ok(None);
        };
        for id in victims {
            select_job(&id, &eligible, &mut selected, &mut order, &mut available);
        }
        placed.push(node.clone());
        available.remove(&node);
    }

    let mut quota_order = eligible.clone();
    quota_order.sort_by(candidate_order);
    for candidate in quota_order {
        if quota_fits(&selected) {
            break;
        }
        select_job(
            &candidate.id,
            &eligible,
            &mut selected,
            &mut order,
            &mut available,
        );
    }
    if !quota_fits(&selected) {
        return Ok(None);
    }

    // Drop redundant victims without breaking any node placement or quota requirement.
    for id in order.iter().rev() {
        let mut remaining = selected.clone();
        remaining.remove(id);
        let physically_fits = placed.iter().all(|ip| {
            let (base_gpus, base_cpus) = free_initial.get(ip).copied().unwrap_or_default();
            let (gpus, cpus) = eligible
                .iter()
                .filter(|c| c.id != *id && remaining.contains(&c.id) && c.node_ip == *ip)
                .fold((base_gpus, base_cpus), |(g, cpu), c| {
                    (g + c.gpus, cpu + c.cpus)
                });
            gpus >= request.gpus && cpus >= request.cpus.max(1.0)
        });
        if physically_fits && quota_fits(&remaining) {
            selected = remaining;
        }
    }

    Ok(Some(PreemptionPlan {
        possible: true,
        jobs: order
            .into_iter()
            .filter(|id| selected.contains(id))
            .collect(),
    }))
}

fn validate(r: &PreemptionRequest) -> anyhow::Result<()> {
    anyhow::ensure!(
        r.contract_version == 1,
        "unsupported preemption contract version"
    );
    anyhow::ensure!(!r.namespace.is_empty(), "namespace is required");
    anyhow::ensure!(
        r.nodes > 0 && r.nodes <= 256,
        "nodes must be between 1 and 256"
    );
    anyhow::ensure!(
        r.pins.is_empty() || r.pins.len() == r.nodes as usize,
        "pins must be empty or have one entry per node"
    );
    anyhow::ensure!(
        r.now.is_finite()
            && r.gpus.is_finite()
            && r.cpus.is_finite()
            && r.gpus >= 0.0
            && r.cpus >= 0.0,
        "resource request and time must be finite and non-negative"
    );
    if let Some(d) = &r.quota_demand {
        let (gpus, cpus) = (d.gpus, d.cpus);
        anyhow::ensure!(
            gpus.is_finite() && cpus.is_finite() && gpus >= 0.0 && cpus >= 0.0,
            "quota demand must be finite and non-negative"
        );
    }
    for n in &r.free {
        anyhow::ensure!(
            !n.node_ip.is_empty()
                && n.gpus.is_finite()
                && n.cpus.is_finite()
                && n.gpus >= 0.0
                && n.cpus >= 0.0,
            "invalid Ray capacity"
        );
    }
    for c in &r.candidates {
        anyhow::ensure!(
            !c.id.is_empty()
                && !c.namespace.is_empty()
                && !c.node_ip.is_empty()
                && c.gpus.is_finite()
                && c.cpus.is_finite()
                && c.gpus >= 0.0
                && c.cpus >= 0.0,
            "invalid preemption candidate"
        );
    }
    Ok(())
}

fn allocation_is_selected_job(id: &str, victims: &HashSet<String>) -> bool {
    id.strip_prefix("job:")
        .is_some_and(|job_id| victims.contains(job_id))
}

fn takeable(c: &PreemptionCandidate, now: f64) -> bool {
    c.preemptible
        && c.started_at
            .is_none_or(|started| now - started >= c.min_runtime_s as f64)
}

fn candidate_order(a: &PreemptionCandidate, b: &PreemptionCandidate) -> std::cmp::Ordering {
    a.priority
        .cmp(&b.priority)
        .then_with(|| a.started_at.is_some().cmp(&b.started_at.is_some()))
        .then_with(|| {
            b.started_at
                .unwrap_or(0.0)
                .total_cmp(&a.started_at.unwrap_or(0.0))
        })
        .then_with(|| a.id.cmp(&b.id))
}

fn select_job(
    id: &str,
    eligible: &[PreemptionCandidate],
    selected: &mut HashSet<String>,
    order: &mut Vec<String>,
    available: &mut HashMap<String, (f64, f64)>,
) {
    if !selected.insert(id.to_owned()) {
        return;
    }
    order.push(id.to_owned());
    for c in eligible.iter().filter(|c| c.id == id) {
        if let Some((gpus, cpus)) = available.get_mut(&c.node_ip) {
            *gpus += c.gpus;
            *cpus += c.cpus;
        }
    }
}

fn choose_victims(
    need_gpus: f64,
    need_cpus: f64,
    free: &HashMap<String, (f64, f64)>,
    candidates: &[PreemptionCandidate],
    selected: &HashSet<String>,
    namespace: &str,
    priority: i32,
    now: f64,
    pin: Option<&str>,
) -> Option<(String, Vec<String>)> {
    let mut nodes: Vec<_> = free.iter().collect();
    nodes.sort_by(|a, b| a.0.cmp(b.0));
    let mut best: Option<(String, Vec<String>, Vec<i32>)> = None;
    for (ip, &(mut gpus, mut cpus)) in nodes {
        if pin.is_some_and(|p| p != ip.as_str()) {
            continue;
        }
        if gpus >= need_gpus && cpus >= need_cpus {
            return Some((ip.clone(), Vec::new()));
        }
        let mut pool: Vec<_> = candidates
            .iter()
            .filter(|c| {
                c.node_ip == *ip
                    && c.namespace == namespace
                    && c.priority < priority
                    && !selected.contains(&c.id)
                    && takeable(c, now)
            })
            .collect();
        pool.sort_by(|a, b| candidate_order(a, b));
        let mut picked = Vec::new();
        let mut score = Vec::new();
        for c in pool {
            if gpus >= need_gpus && cpus >= need_cpus {
                break;
            }
            picked.push(c.id.clone());
            score.push(c.priority);
            gpus += c.gpus;
            cpus += c.cpus;
        }
        score.sort_by(|a, b| b.cmp(a));
        if gpus >= need_gpus
            && cpus >= need_cpus
            && best
                .as_ref()
                .is_none_or(|(_, _, old_score)| score < *old_score)
        {
            best = Some((ip.clone(), picked, score));
        }
    }
    best.map(|(ip, picked, _)| (ip, picked))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(
        id: &str,
        node: &str,
        priority: i32,
        gpus: f64,
        cpus: f64,
        started_at: Option<f64>,
    ) -> PreemptionCandidate {
        PreemptionCandidate {
            id: id.into(),
            namespace: "ns".into(),
            priority,
            node_ip: node.into(),
            gpus,
            cpus,
            started_at,
            preemptible: true,
            min_runtime_s: 300,
        }
    }

    fn request() -> PreemptionRequest {
        PreemptionRequest {
            contract_version: 1,
            namespace: "ns".into(),
            tag: Some("scope=default".into()),
            priority: 10,
            gpus: 1.0,
            cpus: 4.0,
            nodes: 1,
            pins: vec![],
            now: 10_000.0,
            free: vec![NodeCapacity {
                node_ip: "a".into(),
                gpus: 0.0,
                cpus: 40.0,
            }],
            candidates: vec![],
            quota_demand: None,
            exclude_allocation: None,
        }
    }

    #[test]
    fn selects_lowest_priority_then_newest_and_only_what_is_needed() {
        let mut r = request();
        r.gpus = 2.0;
        r.candidates = vec![
            candidate("old", "a", 1, 1.0, 8.0, Some(1.0)),
            candidate("new", "a", 1, 1.0, 8.0, Some(9_600.0)),
            candidate("medium", "a", 1, 1.0, 8.0, Some(9_000.0)),
        ];
        let p = plan(r, &Quota::default(), &Quota::default(), &[])
            .unwrap()
            .unwrap();
        assert_eq!(p.jobs, vec!["new", "medium"]);
    }

    #[test]
    fn excludes_other_namespaces_equal_or_higher_priority_and_unpreemptible_jobs() {
        let mut r = request();
        r.candidates = vec![
            candidate("other", "a", -1, 1.0, 8.0, None),
            candidate("equal", "a", 10, 1.0, 8.0, None),
        ];
        r.candidates[0].namespace = "other".into();
        r.candidates[1].priority = 10;
        r.candidates
            .push(candidate("protected", "a", 0, 1.0, 8.0, None));
        r.candidates[2].preemptible = false;
        assert!(plan(r, &Quota::default(), &Quota::default(), &[])
            .unwrap()
            .is_none());
    }

    #[test]
    fn rejects_unknown_adapter_contract_versions() {
        let mut r = request();
        r.contract_version = 2;
        assert!(plan(r, &Quota::default(), &Quota::default(), &[]).is_err());
    }

    #[test]
    fn uses_free_capacity_without_preempting_and_counts_cpu_shortfalls() {
        let mut r = request();
        r.free.push(NodeCapacity {
            node_ip: "b".into(),
            gpus: 2.0,
            cpus: 40.0,
        });
        r.candidates
            .push(candidate("unused", "a", 0, 1.0, 8.0, None));
        let p = plan(r, &Quota::default(), &Quota::default(), &[])
            .unwrap()
            .unwrap();
        assert!(p.jobs.is_empty());

        let mut r = request();
        r.gpus = 1.0;
        r.cpus = 32.0;
        r.free[0].cpus = 4.0;
        r.candidates = vec![
            candidate("gpu-job", "a", 0, 1.0, 2.0, None),
            candidate("cpu-job", "a", 0, 0.0, 30.0, Some(9_000.0)),
        ];
        let p = plan(r, &Quota::default(), &Quota::default(), &[])
            .unwrap()
            .unwrap();
        assert_eq!(p.jobs.len(), 2);
        assert!(p.jobs.contains(&"gpu-job".to_string()));
        assert!(p.jobs.contains(&"cpu-job".to_string()));
    }

    #[test]
    fn waits_for_min_runtime_but_can_take_unstarted_jobs() {
        let mut r = request();
        r.candidates = vec![
            candidate("young", "a", 1, 1.0, 8.0, Some(9_990.0)),
            candidate("waiting", "a", 1, 1.0, 8.0, None),
        ];
        let p = plan(r, &Quota::default(), &Quota::default(), &[])
            .unwrap()
            .unwrap();
        assert_eq!(p.jobs, vec!["waiting"]);
    }

    #[test]
    fn quota_victims_are_selected_and_redundant_other_tag_jobs_are_not_taken() {
        let mut r = request();
        r.free[0].gpus = 1.0;
        r.quota_demand = Some(Usage {
            gpus: 1.0,
            cpus: 8.0,
        });
        r.candidates = vec![candidate("quota", "a", 1, 0.0, 8.0, Some(1.0))];
        let allocations = vec![Allocation {
            id: "job:quota".into(),
            namespace: "ns".into(),
            tag: Some("scope=other".into()),
            gpus: 0.0,
            cpus: 8.0,
        }];
        let p = plan(
            r,
            &Quota {
                gpus: Some(1.0),
                cpus: Some(8.0),
            },
            &Quota::default(),
            &allocations,
        )
        .unwrap()
        .unwrap();
        assert_eq!(p.jobs, vec!["quota"]);
    }

    #[test]
    fn tag_quota_requires_a_victim_from_that_tag() {
        let mut r = request();
        r.free[0].gpus = 1.0;
        r.quota_demand = Some(Usage {
            gpus: 1.0,
            cpus: 0.0,
        });
        r.candidates = vec![
            candidate("wrong-tag", "a", -2, 0.0, 0.0, None),
            candidate("right-tag", "a", 0, 0.0, 0.0, None),
        ];
        let allocations = vec![
            Allocation {
                id: "job:wrong-tag".into(),
                namespace: "ns".into(),
                tag: Some("scope=other".into()),
                gpus: 0.0,
                cpus: 0.0,
            },
            Allocation {
                id: "job:right-tag".into(),
                namespace: "ns".into(),
                tag: Some("scope=default".into()),
                gpus: 1.0,
                cpus: 0.0,
            },
        ];
        let tag_cap = Quota {
            gpus: Some(1.0),
            cpus: None,
        };
        let p = plan(r.clone(), &Quota::default(), &tag_cap, &allocations)
            .unwrap()
            .unwrap();
        assert_eq!(p.jobs, vec!["right-tag"]);
        r.candidates.pop();
        assert!(plan(r, &Quota::default(), &tag_cap, &allocations)
            .unwrap()
            .is_none());
    }

    #[test]
    fn prefers_a_node_that_needs_fewer_victims_and_obeys_a_pin() {
        let mut r = request();
        r.gpus = 2.0;
        r.free = vec![
            NodeCapacity {
                node_ip: "a".into(),
                gpus: 0.0,
                cpus: 40.0,
            },
            NodeCapacity {
                node_ip: "b".into(),
                gpus: 0.0,
                cpus: 40.0,
            },
        ];
        r.candidates = vec![
            candidate("a1", "a", 0, 1.0, 8.0, None),
            candidate("a2", "a", 0, 1.0, 8.0, None),
            candidate("b1", "b", 0, 2.0, 8.0, None),
        ];
        let p = plan(r.clone(), &Quota::default(), &Quota::default(), &[])
            .unwrap()
            .unwrap();
        assert_eq!(p.jobs, vec!["b1"]);
        r.pins = vec![Some("a".into()).into()];
        let p = plan(r, &Quota::default(), &Quota::default(), &[])
            .unwrap()
            .unwrap();
        assert_eq!(p.jobs, vec!["a1", "a2"]);
    }

    #[test]
    fn respects_machine_pins_and_multi_node_distinctness() {
        let mut r = request();
        r.nodes = 2;
        r.pins = vec![Some("a".into()).into(), Some("b".into()).into()];
        r.free.push(NodeCapacity {
            node_ip: "b".into(),
            gpus: 0.0,
            cpus: 40.0,
        });
        r.candidates = vec![
            candidate("a-job", "a", 1, 1.0, 8.0, None),
            candidate("b-job", "b", 1, 1.0, 8.0, None),
        ];
        let p = plan(r, &Quota::default(), &Quota::default(), &[])
            .unwrap()
            .unwrap();
        assert_eq!(p.jobs, vec!["a-job", "b-job"]);
    }
}
