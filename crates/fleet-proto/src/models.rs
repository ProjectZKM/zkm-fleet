//! Behavior on generated records; field definitions live in model.proto.
use crate::fleet::v1::ProofKind;
use crate::model::v1::*;
use crate::stage::v1::Status as StageStatus;
use serde_json::Value;

pub const MAX_ROUTER_REVOCATIONS: usize = 4096;
pub const ROUTER_CAPABILITIES_HEADER: &str = "x-fleet-router-capabilities";
pub const ROUTER_WORKLOAD_DEFAULTS: &str = "workload-defaults-v1";
pub const ROUTER_SCOPED_REVOCATIONS: &str = "scoped-revocations-v1";
pub const ROUTER_REVOCATION_COMPACTION: &str = "revocation-compaction-v1";
pub fn valid_router_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 63 && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}
pub fn valid_route_id(id: &str) -> bool {
    id.len() == 64 && id.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
impl RouterControl {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.capabilities.len() <= 16
            && self.capabilities.iter().all(|c| router_name(c))
            && self.capabilities.iter().collect::<std::collections::HashSet<_>>().len() == self.capabilities.len(),
            "invalid router capabilities");
        if let Some(auth) = &self.authorization { auth.validate()?; }
        anyhow::ensure!(self.delivered_authorization_revision.is_none_or(|r| self.authorization.as_ref().is_some_and(|a| r <= a.revision))
            && self.applied_authorization_revision.is_none_or(|r| self.delivered_authorization_revision.is_some_and(|d| r <= d)), "invalid authorization acknowledgment");
        anyhow::ensure!(valid_router_id(&self.router_id) && !self.issuer.is_empty() && self.issuer.len() <= 256
            && !self.issuer.chars().any(char::is_control) && self.epoch.len() == 32
            && self.epoch.bytes().all(|b| b.is_ascii_hexdigit()) && !self.token_id.is_empty()
            && !self.namespace.is_empty(), "invalid router enrollment");
        anyhow::ensure!(self.revoked_routes.len() <= MAX_ROUTER_REVOCATIONS
            && self.revoked_routes.iter().all(|r| valid_route_id(r))
            && self.revoked_routes.iter().collect::<std::collections::HashSet<_>>().len() == self.revoked_routes.len()
            && self.delivered_revision <= self.revoked_routes.len() as u64
            && self.applied_revision.is_none_or(|r| r <= self.delivered_revision)
            && self.applied_revision.is_some() == self.applied_ms.is_some(), "invalid router control state");
        Ok(())
    }
}

pub const MAX_ROUTER_AUTHORIZATION_RECORDS: usize = 1024;
pub const ROUTER_NAMESPACE_RECORDS: usize = 64;
/// A relay accepting delegate denials that expire once no grant signed before
/// them can still be valid.
pub const ROUTER_REVOCATION_EXPIRY: &str = "revocation-expiry-v1";
/// A relay accepting the removal of an enabled policy that a retirement floor covers.
pub const ROUTER_POLICY_RETIREMENT: &str = "policy-retirement-v1";
/// The relay's longest grant lifetime plus clock margin between Fleet and relays.
pub const DELEGATE_REVOCATION_RETENTION_SECS: u64 = 3600 + 900;
/// Fleet drops an expired denial only this long after it expired, so a relay
/// whose clock lags Fleet's never sees an early removal.
pub const REVOCATION_EXPIRY_GRACE_SECS: u64 = 300;
/// Live expiring delegate denials, beside the permanent records above.
pub const MAX_ROUTER_EXPIRING_REVOCATIONS: usize = 4096;
pub const ROUTER_NAMESPACE_EXPIRING_REVOCATIONS: usize = 256;
fn router_name(s: &str) -> bool {
    !s.is_empty() && s.len() <= 63 && s.bytes().all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
}
fn router_namespace(s: &str) -> bool { s.len() == 32 && s.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) }
impl RouterAccessPolicy {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(router_namespace(&self.namespace) && (self.workload == "*" || router_name(&self.workload)) && router_name(&self.service)
            && self.version > 0 && valid_route_id(&self.sha256), "invalid router access policy");
        Ok(())
    }
}
impl RouterSubjectRevocation {
    /// Whether retaining this denial makes the old record redundant, including
    /// its operator/application scope. This never expires a permanent ban.
    pub fn covers(&self, old: &Self) -> bool {
        self.namespace == old.namespace && self.principal == old.principal
            && (self.session.is_none() || self.session == old.session)
            && (self.application_only != Some(true) || old.application_only == Some(true))
            // At least as long: an expiring denial never replaces a permanent one.
            && match (self.expires_at, old.expires_at) {
                (None, _) => true,
                (Some(new), Some(old)) => new >= old,
                (Some(_), None) => false,
            }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(router_namespace(&self.namespace) && valid_route_id(&self.principal)
            && self.session.as_ref().is_none_or(|s| valid_route_id(s))
            // Only a namespace delegate's denial may expire.
            && self.expires_at.is_none_or(|e| e > 0 && self.application_only == Some(true)), "invalid subject revocation");
        Ok(())
    }
}
impl RouterAuthorizeRequest {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(router_namespace(&self.namespace) && router_name(&self.workload) && router_name(&self.service)
            && valid_route_id(&self.principal) && valid_route_id(&self.session), "invalid access identity");
        Ok(())
    }
}
impl RouterAuthorization {
    /// A suspension permanently withdraws the current bindings. Resuming the
    /// namespace does not enable policies or resurrect previously issued grants.
    pub fn suspend_namespace(&mut self, namespace: &str) -> anyhow::Result<bool> {
        if !self.policies.iter().any(|p| p.namespace == namespace && p.enabled) {
            return Ok(false);
        }
        let revision = self.revision.checked_add(1).ok_or_else(|| anyhow::anyhow!("authorization revision exhausted"))?;
        anyhow::ensure!(self.policies.iter().filter(|p| p.namespace == namespace && p.enabled)
            .all(|p| p.version < u64::MAX), "policy version exhausted");
        for policy in self.policies.iter_mut().filter(|p| p.namespace == namespace && p.enabled) {
            policy.enabled = false;
            policy.version += 1;
        }
        self.revision = revision;
        Ok(true)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        let expiring = self.revoked_subjects.iter().filter(|r| r.expires_at.is_some()).count();
        anyhow::ensure!(self.revision > 0 && self.policies.len() + self.revoked_subjects.len() - expiring <= MAX_ROUTER_AUTHORIZATION_RECORDS
            && expiring <= MAX_ROUTER_EXPIRING_REVOCATIONS, "authorization state capacity exceeded");
        anyhow::ensure!(self.retired_policy_version != Some(0), "invalid retirement floor");
        let mut policies = std::collections::HashSet::new();
        for p in &self.policies {
            p.validate()?;
            anyhow::ensure!(policies.insert((&p.namespace, &p.workload, &p.service)), "duplicate access policy");
        }
        let mut subjects = std::collections::HashSet::new();
        for r in &self.revoked_subjects {
            r.validate()?;
            anyhow::ensure!(subjects.insert((&r.namespace, &r.principal, &r.session, r.application_only == Some(true))), "duplicate subject revocation");
        }
        Ok(())
    }
    pub fn authorize(&self, request: &RouterAuthorizeRequest) -> anyhow::Result<RouterAccessBinding> {
        request.validate()?;
        anyhow::ensure!(!self.revoked_subjects.iter().any(|r| r.namespace == request.namespace && r.principal == request.principal
            && (request.service != "control" || r.application_only != Some(true))
            && r.session.as_ref().is_none_or(|s| s == &request.session)), "subject revoked");
        let scoped = |p: &&RouterAccessPolicy| p.namespace == request.namespace && p.service == request.service;
        let policy = self.policies.iter().filter(scoped).find(|p| p.workload == request.workload)
            .or_else(|| self.policies.iter().filter(scoped).find(|p| p.workload == "*" && request.service != "control"))
            .filter(|p| p.enabled).ok_or_else(|| anyhow::anyhow!("service not authorized"))?;
        Ok(RouterAccessBinding {principal: request.principal.clone(), session: request.session.clone(),
            policy_version: policy.version, policy_sha256: policy.sha256.clone()})
    }

    /// Reserve capacity per namespace at policy admission. Existing oversized
    /// namespaces keep their records; new writes cannot consume another's share.
    pub fn validate_namespace_capacity(&self, changed: &str) -> anyhow::Result<()> {
        let mut counts = std::collections::BTreeMap::<&str, usize>::new();
        let permanent = self.revoked_subjects.iter().filter(|r| r.expires_at.is_none());
        for ns in self.policies.iter().map(|p|p.namespace.as_str()).chain(permanent.map(|r|r.namespace.as_str())) {
            *counts.entry(ns).or_default() += 1;
        }
        anyhow::ensure!(counts.get(changed).copied().unwrap_or(0) <= ROUTER_NAMESPACE_RECORDS
            && counts.values().map(|n| (*n).max(ROUTER_NAMESPACE_RECORDS)).sum::<usize>() <= MAX_ROUTER_AUTHORIZATION_RECORDS,
            "namespace authorization capacity exceeded");
        Ok(())
    }

    /// Live expiring denials: one namespace cannot hold more than its share.
    pub fn validate_expiring_capacity(&self, changed: &str) -> anyhow::Result<()> {
        let expiring = || self.revoked_subjects.iter().filter(|r| r.expires_at.is_some());
        anyhow::ensure!(expiring().filter(|r| r.namespace == changed).count() <= ROUTER_NAMESPACE_EXPIRING_REVOCATIONS
            && expiring().count() <= MAX_ROUTER_EXPIRING_REVOCATIONS, "namespace revocation capacity exceeded");
        Ok(())
    }

    /// Removes the enabled exact policies `gone` selects and raises the retirement floor
    /// over their versions. The caller commits the result as a new revision. A disabled
    /// policy stays: it is what keeps its namespace's default from applying to a workload.
    pub fn retire(&mut self, gone: impl Fn(&RouterAccessPolicy) -> bool) -> bool {
        let before = self.policies.len();
        let mut floor = self.retired_policy_version;
        self.policies.retain(|p| {
            let retired = p.enabled && p.workload != "*" && gone(p);
            if retired {
                floor = floor.max(Some(p.version));
            }
            !retired
        });
        self.retired_policy_version = floor;
        self.policies.len() != before
    }

    /// The version a policy added now gets: the requested one, raised above every retired
    /// policy's. None when no version is left.
    pub fn new_policy_version(&self, requested: u64) -> Option<u64> {
        match self.retired_policy_version {
            Some(floor) => floor.checked_add(1).map(|above| above.max(requested)),
            None => Some(requested),
        }
    }

    /// Drop delegate denials that expired at least the grace period ago. The
    /// caller commits the result as a new revision. Permanent denials stay.
    pub fn drop_expired(&mut self, now_secs: u64) -> bool {
        let before = self.revoked_subjects.len();
        self.revoked_subjects.retain(|r| r.expires_at.is_none_or(|e| now_secs < e.saturating_add(REVOCATION_EXPIRY_GRACE_SECS)));
        self.revoked_subjects.len() != before
    }
}

impl NodeReport {
    pub fn validate_runtime_instances(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.runtime_instances.len() <= 1024, "too many runtime instances");
        let mut seen = std::collections::HashSet::new();
        for instance in &self.runtime_instances {
            anyhow::ensure!(instance.native_observation_base64.as_ref().is_none_or(|v| !v.is_empty() && v.len() <= 21848), "invalid native observation size");
            let e = &instance.execution;
            let entry = self.entry(&instance.entry_name).ok_or_else(|| anyhow::anyhow!("runtime instance has no app"))?;
            anyhow::ensure!(instance.native_observation_base64.is_none() || entry.native_access(), "native observation requires an access-policy allocation");
            // A native reservation reports exactly its Ray-granted devices.
            let mut ids = std::collections::HashSet::new();
            anyhow::ensure!(instance.gpu_ids.len() == if entry.native_access() { entry.gpus().unwrap_or(0.0) as usize } else { 0 }
                && instance.gpu_ids.iter().all(|id| !id.is_empty() && id.len() <= 64
                    && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') && ids.insert(id)),
                "runtime GPU ids differ from the app's grant");
            anyhow::ensure!(entry.0["runtime"]["executor"] == "shim" && (entry.is_container() || entry.is_prover())
                && e.node_id == self.machine && e.generation > 0
                && e.workload == entry.app() && (e.namespace == entry.namespace() || Some(e.namespace.as_str()) == entry.namespace_reference())
                && [&e.namespace,&e.workload,&e.allocation_id,&e.lease_id,&e.node_id].iter().all(|v| !v.is_empty() && v.len() <= 256 && !v.chars().any(char::is_control))
                && seen.insert((&e.namespace,&e.allocation_id,&e.node_id)), "invalid runtime execution identity");
            let prefix = format!("{}-",instance.entry_name);
            anyhow::ensure!(e.allocation_id.strip_prefix(&prefix).and_then(|s|s.parse::<u64>().ok()).is_some_and(|index| index < entry.replicas()), "invalid runtime replica");
            let expected: Vec<String> = entry.0["services"].as_array().into_iter().flatten().filter_map(Value::as_str).map(str::to_owned).collect();
            // An unhealthy workload may retain only its declared management
            // endpoint so an administrator can repair this exact allocation.
            anyhow::ensure!((entry.native_access() && !instance.ready && instance.services.is_empty())
                || instance.services == expected || (instance.services == ["control"]
                && expected.iter().any(|s| s == "control")), "runtime services differ from app manifest");
        }
        Ok(())
    }
}

pub fn validate_tag(tag: &str) -> anyhow::Result<()> {
    let (key, value) = tag
        .split_once('=')
        .ok_or_else(|| anyhow::anyhow!("tag must be KEY=VALUE"))?;
    let valid = |s: &str| {
        !s.is_empty()
            && s.len() <= 63
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    };
    anyhow::ensure!(valid(key) && key.as_bytes()[0].is_ascii_alphabetic() && valid(value),
        "tag must be KEY=VALUE: key starts with a letter; key and value use letters, digits, '.', '_' or '-' and have at most 63 characters each");
    Ok(())
}

pub fn status_name(s: StageStatus) -> String {
    s.as_str_name().to_ascii_lowercase()
}
pub fn kind_name(k: ProofKind) -> String {
    k.as_str_name()
        .trim_start_matches("PROOF_KIND_")
        .to_ascii_lowercase()
}

impl ProofJobState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed)
    }
    pub fn name(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }
    pub fn from_name(name: &str) -> anyhow::Result<Self> {
        Ok(match name {
            "queued" => Self::Queued,
            "running" => Self::Running,
            "succeeded" => Self::Succeeded,
            "failed" => Self::Failed,
            other => anyhow::bail!("unknown job state {other:?}"),
        })
    }
}
impl ProofJob {
    pub fn failure_name(&self) -> Option<String> {
        self.fail_status
            .and_then(|s| StageStatus::try_from(s).ok())
            .map(status_name)
    }
}
impl std::ops::Deref for StringList {
    type Target = Vec<String>;
    fn deref(&self) -> &Self::Target {
        &self.values
    }
}
impl From<Vec<String>> for StringList {
    fn from(values: Vec<String>) -> Self {
        Self { values }
    }
}
impl<'a> IntoIterator for &'a StringList {
    type Item = &'a String;
    type IntoIter = std::slice::Iter<'a, String>;
    fn into_iter(self) -> Self::IntoIter {
        self.values.iter()
    }
}
impl ScalePhase {
    pub fn name(self) -> &'static str {
        match self {
            Self::Draining => "draining",
            Self::Starting => "starting",
            Self::Complete => "complete",
            Self::Failed => "failed",
        }
    }
}
impl ScaleStatus {
    pub fn pending(&self) -> bool {
        matches!(self.phase(), ScalePhase::Draining | ScalePhase::Starting)
    }
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn failed(&self) -> bool {
        self.phase() == ScalePhase::Failed
    }
}
pub mod scale_phase {
    use super::*;
    pub fn serialize<S: serde::Serializer>(phase: &i32, s: S) -> Result<S::Ok, S::Error> {
        let phase = ScalePhase::try_from(*phase).map_err(serde::ser::Error::custom)?;
        serde::Serialize::serialize(&phase, s)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(d: D) -> Result<i32, D::Error> {
        <ScalePhase as serde::Deserialize>::deserialize(d).map(|phase| phase as i32)
    }
}

impl Quota {
    pub fn validate(&self) -> anyhow::Result<()> {
        for cap in [self.gpus, self.cpus].into_iter().flatten() {
            anyhow::ensure!(
                cap.is_finite() && cap >= 0.0,
                "resource caps must be finite and non-negative"
            );
        }
        Ok(())
    }

    pub fn fits(&self, gpus: f64, cpus: f64) -> bool {
        self.gpus.is_none_or(|cap| gpus <= cap + 1e-9)
            && self.cpus.is_none_or(|cap| cpus <= cap + 1e-9)
    }
}

impl Allocation {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.id.is_empty() && self.id.len() <= 160,
            "invalid allocation id"
        );
        anyhow::ensure!(
            self.gpus.is_finite() && self.cpus.is_finite() && self.gpus >= 0.0 && self.cpus >= 0.0,
            "allocated resources must be finite and non-negative"
        );
        Ok(())
    }
}

impl ProofJob {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            ProofJobState::try_from(self.state).is_ok() && ProofKind::try_from(self.kind).is_ok(),
            "unknown job state or proof kind"
        );
        anyhow::ensure!(
            (self.state() == ProofJobState::Failed) == self.fail_status.is_some(),
            "only failed jobs have a failure status"
        );
        anyhow::ensure!(self.fail_status.is_none_or(|s| StageStatus::try_from(s).is_ok()), "unknown proof failure status");
        anyhow::ensure!(
            crate::artifacts::is_safe_id(&self.id) && crate::artifacts::is_safe_id(&self.client_id),
            "job id and client_id are required safe identifiers"
        );
        anyhow::ensure!(
            self.owner.len() == 40
                && self
                    .owner
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "job owner must be an authenticated token address"
        );
        anyhow::ensure!(
            self.namespace.len() == 32
                && self
                    .namespace
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "job namespace must be an immutable namespace ID"
        );
        if !self.tag.is_empty() {
            validate_tag(&self.tag)?;
        }
        anyhow::ensure!(
            self.state().is_terminal() == self.finished_ms.is_some(),
            "only terminal jobs have finished_ms"
        );
        if let Some(inputs) = &self.inputs {
            crate::artifacts::validate_inputs(inputs, &self.elf_id)?;
            if self.state() == ProofJobState::Succeeded {
                anyhow::ensure!(
                    self.outputs.is_some(),
                    "successful proof requires output references"
                );
            }
        }
        if let Some(outputs) = &self.outputs {
            crate::artifacts::validate_outputs(outputs)?;
        }
        Ok(())
    }
}

impl serde::Serialize for ProofJob {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut m = s.serialize_struct("ProofJob", 22)?;
        m.serialize_field("id", &self.id)?;
        m.serialize_field("inputs", &self.inputs)?;
        m.serialize_field("outputs", &self.outputs)?;
        m.serialize_field("app", &self.app)?;
        m.serialize_field("owner", &self.owner)?;
        m.serialize_field("namespace", &self.namespace)?;
        m.serialize_field("tag", &self.tag)?;
        m.serialize_field("client_id", &self.client_id)?;
        m.serialize_field("elf_id", &self.elf_id)?;
        m.serialize_field("priority", &self.priority)?;
        m.serialize_field("kind", &kind_name(self.kind()))?;
        m.serialize_field("state", self.state().name())?;
        m.serialize_field("fail_status", &self.failure_name())?;
        m.serialize_field("error", &self.error)?;
        m.serialize_field("node", &self.node)?;
        m.serialize_field("attempts", &self.attempts)?;
        m.serialize_field("failed_on", &self.failed_on)?;
        m.serialize_field("cycles", &self.cycles)?;
        m.serialize_field("prove_ms", &self.prove_ms)?;
        m.serialize_field("created_ms", &self.created_ms)?;
        m.serialize_field("started_ms", &self.started_ms)?;
        m.serialize_field("finished_ms", &self.finished_ms)?;
        m.end()
    }
}

impl ProofJobFilter {
    pub fn matches(&self, j: &ProofJob) -> bool {
        self.namespace.as_ref().is_none_or(|n| &j.namespace == n)
            && self.tag.as_ref().is_none_or(|g| &j.tag == g)
            && self.owner.as_ref().is_none_or(|o| &j.owner == o)
            && self.app.as_ref().is_none_or(|a| &j.app == a)
            && (!self.active || !j.state().is_terminal())
    }
}

impl NodePin {
    pub fn as_deref(&self) -> Option<&str> {
        self.node_ip.as_deref()
    }
}
impl From<Option<String>> for NodePin {
    fn from(node_ip: Option<String>) -> Self {
        Self { node_ip }
    }
}
impl PreemptionPlan {
    pub fn impossible() -> Self {
        Self {
            possible: false,
            jobs: Vec::new(),
        }
    }
}

impl RegisterAppRequest {
    /// Settings every app's spec may give; the scheduler adds its name, namespace, tag,
    /// priority and program, and creates it disabled.
    pub const SPEC_SETTINGS: [&'static str; 13] = ["kind", "image", "replicas", "gpus", "cpus", "shm_gb",
        "memory_gb", "env", "volumes", "ready_timeout_s", "stop_timeout_s", "cpu_limit", "memory_limit_gb"];
    /// Settings only a container app's spec (`kind: app`) may give.
    pub const CONTAINER_SETTINGS: [&'static str; 14] = ["command", "port_base", "health_path", "workdir", "user", "runtime", "services", "container_port", "entrypoint", "ingress", "service_protocol", "access_policy_sha256", "health_tcp_banner", "mounts"];
    /// At most this many GPUs or durable mounts per native replica, as in shim.
    pub const MAX_NATIVE_ATTACHMENTS: usize = 16;

    /// Durable named volumes of a native replica: `{name, target, read_only?}`.
    /// Shapes match shim's own checks so an admitted app never fails on a node.
    pub fn valid_durable_mounts(mounts: &Value) -> bool {
        let Some(mounts) = mounts.as_array() else { return false };
        let (mut names, mut targets) = (std::collections::HashSet::new(), std::collections::HashSet::new());
        mounts.len() <= Self::MAX_NATIVE_ATTACHMENTS && mounts.iter().all(|m| {
            let Some(m) = m.as_object() else { return false };
            let name = m.get("name").and_then(Value::as_str).unwrap_or("");
            let target = m.get("target").and_then(Value::as_str).unwrap_or("");
            m.keys().all(|k| ["name", "target", "read_only"].contains(&k.as_str()))
                && m.get("read_only").is_none_or(Value::is_boolean)
                && !name.is_empty() && name.len() <= 63 && name.as_bytes()[0].is_ascii_alphanumeric()
                && name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                && target.len() <= 1024 && target.starts_with('/')
                && target.bytes().all(|b| b.is_ascii_graphic() && !b"\\,:".contains(&b))
                && target[1..].split('/').all(|p| !p.is_empty() && p != "." && p != "..")
                && names.insert(name) && targets.insert(target)
        })
    }

    pub fn local_container_runtime(runtime: Option<&Value>) -> bool {
        runtime.is_some_and(|v| ["docker", "tee", "tee-simulation"].iter().any(|driver|
            v == &serde_json::json!({"executor":"shim", "driver": driver})))
    }

    pub fn validate_transport(spec: &serde_json::Map<String, Value>) -> anyhow::Result<()> {
        if spec.get("kind").and_then(Value::as_str).is_some_and(|k| k.starts_with("ziren-")) && spec.contains_key("runtime") {
            anyhow::ensure!(spec.get("runtime") == Some(&serde_json::json!({"executor":"shim","driver":"docker"}))
                && spec.get("port_base").is_some_and(|p| p.as_u64().is_some_and(|p| p <= 65535))
                && spec.contains_key("container_port")
                && spec.get("services").is_none_or(|v| v.as_array().is_some_and(Vec::is_empty))
                && spec.get("ingress").is_none_or(|v| v.as_object().is_some_and(serde_json::Map::is_empty))
                && spec.get("service_protocol").is_none_or(|v| v == "http1"),
                "packaged provers require shim Docker and a private HTTP health port");
        }
        if let Some(hash) = spec.get("access_policy_sha256") {
            anyhow::ensure!(hash.as_str().is_some_and(|h| h.len() == 64 && h.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)))
                && Self::local_container_runtime(spec.get("runtime")), "access policy requires SHA256 and a runtime driver");
        }
        let native = crate::workload::Entry(&Value::Object(spec.clone())).native_access();
        let tcp = spec.get("service_protocol").is_some_and(|v| v == "tcp");
        let banner = spec.get("health_tcp_banner").filter(|v| !v.is_null());
        anyhow::ensure!(banner.is_none() || native && tcp, "TCP banner health requires native OpenShell TCP services");
        if native {
            let services = spec.get("services").and_then(Value::as_array).is_some_and(|v| !v.is_empty());
            let health = spec.get("health_path").map_or(Some("/"), Value::as_str);
            // Whole GPUs only: shim attaches exactly the device ids Ray granted.
            let gpus = spec.get("gpus").and_then(Value::as_f64);
            let attached = gpus.is_some_and(|g| g > 0.0) || spec.get("mounts").is_some();
            anyhow::ensure!(!attached || matches!(spec["runtime"]["driver"].as_str(), Some("docker" | "openshell")),
                "native GPUs and durable mounts require a non-TEE driver");
            anyhow::ensure!(spec.get("mounts").is_none_or(Self::valid_durable_mounts), "invalid durable mounts");
            anyhow::ensure!(gpus.is_some_and(|g| g >= 0.0 && g <= Self::MAX_NATIVE_ATTACHMENTS as f64 && g.fract() == 0.0)
                && spec.get("cpus").and_then(Value::as_u64).is_some_and(|v| v > 0 && v <= u32::MAX as u64 / 1000)
                && spec.get("memory_gb").and_then(Value::as_f64).is_some_and(|v| v.is_finite() && v * (1u64 << 30) as f64 >= (6 * 1024 * 1024) as f64 && v * ((1u64 << 30) as f64) < u64::MAX as f64)
                && ["port_base", "entrypoint", "user", "workdir", "shm_gb"].iter().all(|k| !spec.contains_key(*k))
                && spec.get("volumes").is_none_or(|v| v.as_array().is_some_and(|a| a.is_empty()))
                // A native node holds no application credential: the caller sends its own.
                && spec.get("ingress").and_then(Value::as_object).is_none_or(|i| i.values().all(|r| r.get("bearer_env").is_none_or(Value::is_null)))
                && spec.get("service_protocol").is_none_or(|v| v == "http1" || v == "tcp")
                && (!tcp || health == Some("/") && banner.and_then(Value::as_str).is_some_and(|s|
                    !s.is_empty() && s.len() <= 128 && s.bytes().all(|b| b.is_ascii_graphic() || b == b' ')))
                // `control` is the logs-only service beside an application service.
                && spec.get("services").and_then(Value::as_array).is_none_or(|s| s.is_empty() || s.iter().any(|s| s != "control"))
                && services == spec.contains_key("container_port")
                && health.is_some_and(|p| p.starts_with('/') && !p.starts_with("//") && p.len() <= 2048
                    && !p.bytes().any(|b| b.is_ascii_control() || b == b'\\' || b == b'#') && (services || p == "/")),
                "native OpenShell requires CPU/memory limits, private HTTP services (ingress without bearer_env, control beside an application service) or TCP services with banner health");
        }
        anyhow::ensure!(native || !spec.contains_key("mounts"), "durable mounts require native OpenShell");
        if ["services", "container_port", "entrypoint", "ingress", "service_protocol"].iter().any(|k| spec.contains_key(*k)) {
            anyhow::ensure!(native || Self::local_container_runtime(spec.get("runtime")),
                "transport settings require explicit shim execution");
        }
        if let Some(protocol) = spec.get("service_protocol") {
            anyhow::ensure!(matches!(protocol.as_str(), Some("http1" | "tcp")), "invalid service_protocol");
            if protocol == "tcp" {
                anyhow::ensure!(spec.get("services").and_then(Value::as_array).is_some_and(|s| s.iter().any(|s| s.as_str().is_some_and(|s| s != "control")))
                    && spec.get("ingress").is_none_or(|v| v.as_object().is_some_and(|i| i.is_empty())), "TCP requires an application service without HTTP ingress");
            }
        }
        if let Some(services) = spec.get("services") {
            let services = services.as_array().ok_or_else(|| anyhow::anyhow!("services must be an array"))?;
            let mut names = std::collections::HashSet::new();
            anyhow::ensure!(services.len() <= 8 && services.iter().all(|s| s.as_str().is_some_and(|s|
                !s.is_empty() && s.len() <= 64 && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_') && names.insert(s))), "invalid services");
        }
        if let Some(ingress) = spec.get("ingress") {
            let rules = ingress.as_object().ok_or_else(|| anyhow::anyhow!("ingress must be an object"))?;
            anyhow::ensure!(rules.len() <= 8, "too many ingress rules");
            for (name, rule) in rules {
                anyhow::ensure!(name != "control" && spec.get("services").and_then(Value::as_array).is_some_and(|s| s.iter().any(|v| v == name)), "ingress must reference an application service");
                let rule = rule.as_object().ok_or_else(|| anyhow::anyhow!("ingress rule must be an object"))?;
                anyhow::ensure!(rule.keys().all(|k| ["paths", "methods", "bearer_env", "single_use"].contains(&k.as_str())), "unknown ingress field");
                anyhow::ensure!(rule.get("single_use").is_none_or(Value::is_boolean), "single_use must be boolean");
                anyhow::ensure!(rule.get("paths").and_then(Value::as_array).is_some_and(|paths| !paths.is_empty() && paths.len() <= 32 && paths.iter().all(|p| p.as_str().is_some_and(|p| p.starts_with('/') && p.len()<=256 && p.is_ascii() && !p.contains(['%', '?', '#', '\\']) && !p.chars().any(char::is_control) && !p.split('/').any(|p| p == "." || p == "..")))), "invalid ingress paths");
                anyhow::ensure!(rule.get("methods").and_then(Value::as_array).is_some_and(|methods| !methods.is_empty() && methods.len()<=7 && methods.iter().all(|m| m.as_str().is_some_and(|m| ["GET","HEAD","POST","PUT","PATCH","DELETE","OPTIONS"].contains(&m)))), "invalid ingress methods");
                if let Some(env) = rule.get("bearer_env").filter(|v| !v.is_null()) {
                    let env = env.as_str().ok_or_else(|| anyhow::anyhow!("invalid bearer environment"))?;
                    anyhow::ensure!(!env.is_empty() && env.len()<=128 && env.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') && spec.get("env").and_then(|e|e.get(env)).and_then(Value::as_str).is_some_and(|s| !s.is_empty()), "missing ingress bearer environment");
                }
            }
        }
        if spec.get("port_base").and_then(Value::as_u64) == Some(0) {
            anyhow::ensure!(Self::local_container_runtime(spec.get("runtime")) && spec.get("container_port").is_some(), "automatic host ports require shim and container_port");
        }
        if let Some(port) = spec.get("container_port") {
            anyhow::ensure!(port.as_u64().is_some_and(|p| (1..=65535).contains(&p)), "invalid container_port");
        }
        if let Some(entrypoint) = spec.get("entrypoint") {
            anyhow::ensure!(entrypoint.as_str().is_some_and(|s| !s.is_empty() && !s.contains('\0')), "invalid entrypoint");
        }
        Ok(())
    }
    /// Settings only a prover's spec (`kind: ziren-gpu` or `ziren-cpu`) may give.
    pub const PROVER_SETTINGS: [&'static str; 3] = ["proof_storage_root", "backend", "rust_log"];
    /// The most CPUs a reservation or a limit gives: shim counts them in 32-bit millicores.
    pub const MAX_CPUS: f64 = (u32::MAX / 1000) as f64;
    /// The smallest hard CPU limit Docker accepts.
    pub const MIN_CPU_LIMIT: f64 = 0.01;
    /// The smallest hard memory limit shim accepts, in bytes.
    pub const MIN_MEMORY_LIMIT_BYTES: f64 = (6 * 1024 * 1024) as f64;

    /// That an app's CPU and memory settings fit its runtime. `cpus` and `memory_gb` are what
    /// Ray reserves; `cpu_limit` and `memory_limit_gb` are hard limits, which only a shim
    /// container (driver docker, tee or tee-simulation) gets, and only when it sets them. A
    /// reservation is a positive whole number of CPUs, except that a shim `kind: app` without
    /// GPUs may reserve 0 or a fraction when it sets `cpu_limit`, `memory_gb` and
    /// `memory_limit_gb`: a replica that reserves less than a CPU is always capped. `spec`
    /// carries its `kind`; a missing `cpus` is 1 and missing `gpus` the kind's default.
    pub fn validate_resources(spec: &serde_json::Map<String, Value>) -> anyhow::Result<()> {
        let value = Value::Object(spec.clone());
        let entry = crate::workload::Entry(&value);
        let set = |key: &str| spec.get(key).filter(|v| !v.is_null());
        let limited = set("cpu_limit").is_some() || set("memory_limit_gb").is_some();
        let shim = Self::local_container_runtime(spec.get("runtime"));
        if limited {
            anyhow::ensure!(!entry.native_access(),
                "cpu_limit and memory_limit_gb don't apply to a native OpenShell app: its cpus and memory_gb are its limits");
            anyhow::ensure!(shim,
                "cpu_limit and memory_limit_gb need runtime {{executor: shim}} with driver docker, tee or tee-simulation");
        }
        let cpus = entry.cpus();
        anyhow::ensure!(cpus.is_finite() && (0.0..=Self::MAX_CPUS).contains(&cpus), "cpus must be a number from 0 to {}", Self::MAX_CPUS);
        if let Some(limit) = set("cpu_limit") {
            let limit = limit.as_f64().unwrap_or(f64::NAN);
            anyhow::ensure!(limit.is_finite() && (Self::MIN_CPU_LIMIT..=Self::MAX_CPUS).contains(&limit),
                "cpu_limit must be at least {} CPUs", Self::MIN_CPU_LIMIT);
            anyhow::ensure!(limit + 1e-9 >= cpus, "cpu_limit ({limit}) must be at least cpus ({cpus}), which Ray reserves");
        }
        if let Some(limit) = set("memory_limit_gb") {
            let limit = limit.as_f64().unwrap_or(f64::NAN);
            let bytes = limit * (1u64 << 30) as f64;
            anyhow::ensure!(bytes.is_finite() && bytes >= Self::MIN_MEMORY_LIMIT_BYTES && bytes < i64::MAX as f64,
                "memory_limit_gb must be at least 6 MiB (0.005859375 GiB)");
            if let Some(memory) = entry.memory_gb() {
                anyhow::ensure!(limit + 1e-12 >= memory, "memory_limit_gb ({limit}) must be at least memory_gb ({memory}), which Ray reserves");
            }
        }
        if cpus.fract() != 0.0 || cpus == 0.0 {
            anyhow::ensure!(shim && !entry.native_access() && entry.is_container() && entry.gpus_or_default() == 0.0,
                "cpus must be a positive whole number: 0 or a fraction is only for a kind: app without GPUs on runtime {{executor: shim}} with driver docker, tee or tee-simulation, not native OpenShell");
            anyhow::ensure!(set("cpu_limit").is_some() && entry.memory_gb().is_some_and(|m| m > 0.0) && set("memory_limit_gb").is_some(),
                "cpus {cpus} reserves less than a whole CPU: also set cpu_limit, memory_gb and memory_limit_gb, so its container is always capped");
        }
        Ok(())
    }

    pub fn validate_runtime(runtime: &Value) -> anyhow::Result<()> {
        anyhow::ensure!(Self::local_container_runtime(Some(runtime))
            || runtime == &serde_json::json!({"executor":"shim", "driver":"openshell"}),
            "runtime must select shim with driver docker, openshell, tee or tee-simulation; trust parameters are node-owned");
        Ok(())
    }

    /// The runtime of the app's container: `app` unless it proves a program, which a Ziren GPU
    /// prover does unless the spec asks for a CPU one.
    pub fn kind(&self) -> &str {
        self.spec.get("kind").and_then(Value::as_str)
            .unwrap_or(if self.proof.is_some() { "ziren-gpu" } else { "app" })
    }

    /// What a spec of this kind leaves unset: the shim's Docker driver, and for a prover its
    /// packaged worker's health port on a host port the shim picks.
    pub fn defaults(kind: &str) -> serde_json::Map<String, Value> {
        let mut defaults = serde_json::json!({"runtime":{"executor":"shim","driver":"docker"}});
        if kind != "app" {
            (defaults["port_base"], defaults["container_port"]) = (0.into(), 8080.into());
        }
        defaults.as_object().cloned().unwrap_or_default()
    }

    /// That the spec is an object of known, well-typed settings for its kind, and that a
    /// prover, and only a prover, has a program. The program and its policy are checked where
    /// the app is registered; the settings here, before the app is recorded, since a setting the
    /// node refuses later would leave an app record with nothing behind it.
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.name.is_empty() && self.name.len() <= 63
                && self.name.bytes().all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b)),
            "invalid app name: letters, digits, _ . -, at most 63"
        );
        let spec = self.spec.as_object().ok_or_else(|| anyhow::anyhow!("spec must be a JSON object of settings"))?;
        let kind = self.kind();
        // A prover is a container too, with its storage and proving settings.
        let own = match kind {
            "app" => Self::CONTAINER_SETTINGS.to_vec(),
            "ziren-gpu" | "ziren-cpu" => [&Self::PROVER_SETTINGS[..], &Self::CONTAINER_SETTINGS[..]].concat(),
            _ => anyhow::bail!("kind must be app, ziren-gpu or ziren-cpu"),
        };
        if let Some(key) = spec.keys().find(|k| !Self::SPEC_SETTINGS.contains(&k.as_str()) && !own.contains(&k.as_str())) {
            anyhow::bail!("unknown setting {key} for a {kind} app; it takes {}, {}", Self::SPEC_SETTINGS.join(", "), own.join(", "));
        }
        anyhow::ensure!(
            (kind != "app") == self.proof.is_some(),
            "a prover (kind ziren-gpu or ziren-cpu) proves a program: give proof with its elf_id, and only for a prover"
        );
        if let Some(proof) = &self.proof {
            anyhow::ensure!(!proof.elf_id.trim().is_empty(), "proof.elf_id is required: the program's id");
        }
        let set = |key: &str| spec.get(key).filter(|v| !v.is_null());
        anyhow::ensure!(
            set("image").and_then(Value::as_str).is_some_and(|i| !i.is_empty() && !i.starts_with('-')),
            "image is required"
        );
        if kind == "app" && !crate::workload::Entry(&self.spec).native_access() {
            anyhow::ensure!(set("port_base").is_some(), "port_base is required: replica i serves on port_base + i");
        }
        let mut execution = Self::defaults(kind);
        execution.extend(spec.clone());
        execution.insert("kind".into(), Value::String(kind.into()));
        Self::validate_runtime(&execution["runtime"])?;
        Self::validate_transport(&execution)?;
        let strings = |v: &Value| v.as_array().is_none_or(|a| a.iter().any(|v| !v.is_string()));
        let positive = |v: &Value| v.as_f64().is_none_or(|v| v <= 0.0 || !v.is_finite());
        let invalid = ["user", "health_path", "workdir", "proof_storage_root", "backend", "rust_log"].into_iter()
                .find(|k| set(k).is_some_and(|v| !v.is_string()))
            .or_else(|| ["shm_gb", "port_base", "stop_timeout_s"].into_iter().find(|k| set(k).is_some_and(|v| v.as_u64().is_none())))
            .or_else(|| set("cpus").is_some_and(|v| v.as_f64().is_none_or(|v| v < 0.0 || !v.is_finite())).then_some("cpus"))
            .or_else(|| ["memory_gb", "ready_timeout_s", "cpu_limit", "memory_limit_gb"].into_iter().find(|k| set(k).is_some_and(positive)))
            .or_else(|| ["command", "volumes"].into_iter().find(|k| set(k).is_some_and(strings)))
            .or_else(|| set("replicas").is_some_and(|r| r.as_u64().is_none_or(|r| !(1..=64).contains(&r))).then_some("replicas"))
            .or_else(|| set("gpus").is_some_and(|g| g.as_f64().is_none_or(|g| g < 0.0 || !g.is_finite())).then_some("gpus"))
            .or_else(|| set("env").is_some_and(|e| e.as_object().is_none_or(|e| e.values().any(|v| !v.is_string()))).then_some("env"));
        if let Some(key) = invalid {
            anyhow::bail!("invalid app setting {key}");
        }
        Self::validate_resources(&execution)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use serde_json::json;

    #[test]
    fn packaged_provers_reuse_container_transport_but_require_new_adapter_capability() {
        let source = json!({"name":"policy", "spec":{"kind":"ziren-gpu", "image":"proof:2",
            "runtime":{"executor":"shim","driver":"docker"}, "port_base":0, "container_port":8080,
            "command":[], "health_path":"/healthz", "proof_storage_root":"/srv/proofs"},
            "proof":{"elf_id":"ab".repeat(32)}});
        let request: RegisterAppRequest = serde_json::from_value(source.clone()).unwrap();
        request.validate().unwrap();
        let entry = crate::workload::Entry(&request.spec);
        assert!(entry.is_prover());
        assert!(!entry.is_container(), "SDK proof jobs retain their existing per-job quota accounting");
        assert_eq!(entry.runtime_capability(), Some("shim-prover-v1"));
        let mut report = NodeReport::default();
        report.runtime_capabilities = vec!["shim-docker-v1".into(),"shim-runtime-v2".into(),"shim-automatic-port-v1".into()];
        assert!(!report.supports_runtime(entry));
        report.runtime_capabilities.push("shim-prover-v1".into());
        assert!(report.supports_runtime(entry));
        for key in ["port_base", "container_port"] {
            let mut bad = source.clone();
            bad["spec"][key] = Value::Null;
            assert!(serde_json::from_value::<RegisterAppRequest>(bad).unwrap().validate().is_err());
        }
        for explicit_kind in [true, false] {
            let mut bad = source.clone();
            if !explicit_kind { bad["spec"].as_object_mut().unwrap().remove("kind"); }
            bad["spec"]["services"] = json!(["public"]);
            assert!(serde_json::from_value::<RegisterAppRequest>(bad).unwrap().validate().is_err());
        }
    }

    #[test]
    fn namespace_capacity_and_scoped_revocations_preserve_operator_control() {
        let ns = "ab".repeat(16);
        let mut auth = RouterAuthorization { revision:1, ..Default::default() };
        for n in 0..ROUTER_NAMESPACE_RECORDS {
            auth.policies.push(RouterAccessPolicy {namespace:ns.clone(),workload:format!("app-{n}"),
                service:"control".into(),version:1,sha256:"12".repeat(32),enabled:true});
        }
        auth.validate_namespace_capacity(&ns).unwrap();
        let mut other = auth.policies[0].clone(); other.namespace = "cd".repeat(16);
        auth.policies.push(other.clone());
        auth.validate_namespace_capacity(&other.namespace).unwrap();
        let request = RouterAuthorizeRequest {namespace:other.namespace.clone(),workload:other.workload.clone(),service:"control".into(),
            principal:"34".repeat(32),session:"56".repeat(32)};
        auth.revoked_subjects.push(RouterSubjectRevocation {namespace:other.namespace.clone(),principal:request.principal.clone(),
            session:None,application_only:Some(true),expires_at:None});
        assert!(auth.authorize(&request).is_ok());
        auth.revoked_subjects[0].application_only = None;
        assert!(auth.authorize(&request).is_err());
        other.namespace = ns.clone(); other.workload = "overflow".into(); auth.policies.push(other);
        assert!(auth.validate_namespace_capacity(&ns).is_err());
    }

    #[test]
    fn retiring_removes_enabled_exact_policies_and_later_ones_start_above_them() {
        let policy = |workload: &str, version, enabled| RouterAccessPolicy {namespace:"ab".repeat(16),workload:workload.into(),
            service:"chat".into(),version,sha256:"cd".repeat(32),enabled};
        let mut auth = RouterAuthorization {revision:1,
            policies:vec![policy("gone",3,true),policy("denied",7,false),policy("*",9,true),policy("kept",1,true)],..Default::default()};
        auth.validate().unwrap();
        assert!(serde_json::to_value(&auth).unwrap().get("retired_policy_version").is_none());
        assert_eq!(auth.new_policy_version(1), Some(1));
        assert!(!auth.retire(|p| p.workload == "nothing") && auth.retired_policy_version.is_none());
        // A denial and a namespace default stay, whatever selects them.
        assert!(auth.retire(|p| p.workload != "kept"));
        assert_eq!(auth.policies.iter().map(|p| p.workload.as_str()).collect::<Vec<_>>(), ["denied","*","kept"]);
        assert_eq!(auth.retired_policy_version, Some(3));
        assert_eq!(serde_json::to_value(&auth).unwrap()["retired_policy_version"], 3);
        assert_eq!((auth.new_policy_version(1), auth.new_policy_version(9)), (Some(4), Some(9)));
        // The floor only rises.
        auth.policies.push(policy("low",2,true));
        assert!(auth.retire(|p| p.workload == "low"));
        assert_eq!(auth.retired_policy_version, Some(3));
        auth.validate().unwrap();
        auth.retired_policy_version = Some(u64::MAX);
        assert_eq!(auth.new_policy_version(1), None);
    }

    #[test]
    fn automatic_port_services_compose_requirements_and_cleanup_is_independent() {
        let raw = json!({"runtime":{"executor":"shim","driver":"docker"},"port_base":0,
            "container_port":8080,"services":["http"]});
        let entry = crate::workload::Entry(&raw);
        let mut report = NodeReport::default();
        report.runtime_capabilities = vec!["shim-docker-v1".into(),"shim-automatic-port-v1".into(),"shim-runtime-v2".into()];
        assert!(!report.supports_runtime(entry));
        report.runtime_capabilities.push("shim-network-v1".into());
        assert!(report.supports_runtime(entry));
        report.runtime_capabilities = vec!["shim-cleanup-v1".into()];
        assert!(!report.supports_runtime(entry));
        assert!(report.supports_cleanup(entry));
    }

    #[test]
    fn namespace_service_defaults_never_override_exact_denials_or_cross_namespaces() {
        let policy = RouterAccessPolicy { namespace:"ab".repeat(16),workload:"*".into(),service:"chat".into(),
            version:1,sha256:"cd".repeat(32),enabled:true };
        let mut auth=RouterAuthorization {revision:1,policies:vec![policy.clone()],..Default::default()};
        auth.validate().unwrap();
        let mut request=RouterAuthorizeRequest {namespace:policy.namespace.clone(),workload:"new-agent".into(),service:"chat".into(),
            principal:"12".repeat(32),session:"34".repeat(32)};
        assert_eq!(auth.authorize(&request).unwrap().policy_version,1);
        auth.policies.push(RouterAccessPolicy {workload:request.workload.clone(),enabled:false,..policy.clone()});
        assert!(auth.authorize(&request).is_err());
        request.workload="another-agent".into(); assert!(auth.authorize(&request).is_ok());
        request.namespace="ef".repeat(16); assert!(auth.authorize(&request).is_err());
        request.namespace=policy.namespace; request.service="control".into(); assert!(auth.authorize(&request).is_err());
        request.service="chat".into();request.workload="*".into();assert!(auth.authorize(&request).is_err());
    }

    #[test]
    fn tcp_services_require_shim_and_capable_nodes_without_http_ingress() {
        let good = json!({"runtime":{"executor":"shim","driver":"docker"},"services":["ssh","control"],"service_protocol":"tcp"});
        RegisterAppRequest::validate_transport(good.as_object().unwrap()).unwrap();
        assert_eq!(crate::workload::Entry(&good).runtime_capability(), Some("shim-tcp-v1"));
        for (field,value) in [("service_protocol",json!("udp")),("runtime",Value::Null),
            ("services",json!(["control"])),("ingress",json!({"ssh":{}}))] {
            let mut bad=good.clone(); bad[field]=value;
            assert!(RegisterAppRequest::validate_transport(bad.as_object().unwrap()).is_err());
        }
    }

    #[test]
    fn runtime_ingress_and_automatic_ports_are_validated_before_placement() {
        let good = json!({"runtime":{"executor":"shim","driver":"docker"},"port_base":0,"container_port":18789,
            "services":["chat","control"],"env":{"TOKEN":"private"},
            "ingress":{"chat":{"paths":["/v1/chat/completions"],"methods":["POST"],"bearer_env":"TOKEN"}}});
        RegisterAppRequest::validate_transport(good.as_object().unwrap()).unwrap();
        for (field, value) in [("paths",json!(["/../exec"])),("methods",json!(["CONNECT"])),("bearer_env",json!("MISSING")),("single_use",json!("true"))] {
            let mut bad=good.clone(); bad["ingress"]["chat"][field]=value;
            assert!(RegisterAppRequest::validate_transport(bad.as_object().unwrap()).is_err());
        }
        let mut bad=good.clone(); bad["ingress"]["control"]=bad["ingress"]["chat"].clone();
        assert!(RegisterAppRequest::validate_transport(bad.as_object().unwrap()).is_err());
        let mut bad=good; bad.as_object_mut().unwrap().remove("container_port");
        assert!(RegisterAppRequest::validate_transport(bad.as_object().unwrap()).is_err());
    }

    #[test]
    fn native_provisioning_profile_rejects_unmapped_workload_settings() {
        let spec = json!({"runtime":{"executor":"shim","driver":"openshell"},"image":"approved", "cpus":1,"gpus":0,"memory_gb":0.125});
        let request = |spec:Value| RegisterAppRequest {name:"native".into(),spec:spec.into(),..Default::default()};
        request(spec.clone()).validate().unwrap();
        for (field,value) in [("port_base",json!(8080)),("services",json!(["http"])),("gpus",json!(0.5)),("gpus",json!(17)),
            ("gpus",Value::Null),("cpus",json!(0)),("cpus",json!(0.5)),("memory_gb",json!(0.001)),("volumes",json!(["/host"])),("entrypoint",json!("/bin/sh"))] {
            let mut bad = spec.clone(); bad[field] = value;
            assert!(request(bad).validate().is_err(),"{field}");
        }
        let mut http=spec.clone();http["services"]=json!(["http"]);http["container_port"]=json!(8090);http["health_path"]=json!("/health");
        request(http.clone()).validate().unwrap();
        assert_eq!(crate::workload::Entry(&http).runtime_capability(),Some("shim-openshell-services-v1"));
        let mut tcp = http.clone();
        tcp["service_protocol"] = json!("tcp"); tcp["health_path"] = json!("/");
        tcp["health_tcp_banner"] = json!("SSH-2.0-");
        request(tcp.clone()).validate().unwrap();
        let entry = crate::workload::Entry(&tcp);
        assert_eq!(entry.runtime_capability(), Some("shim-openshell-tcp-services-v1"));
        let mut node = NodeReport::default();
        node.runtime_capabilities = vec!["shim-openshell-services-v1".into()];
        assert!(!node.supports_runtime(entry));
        node.runtime_capabilities.push("shim-openshell-tcp-services-v1".into());
        assert!(node.supports_runtime(entry));
        for banner in [Value::Null, json!(""), json!("a".repeat(129)), json!("SSH\n"), json!("非ASCII")] {
            let mut bad = tcp.clone(); bad["health_tcp_banner"] = banner;
            assert!(request(bad).validate().is_err());
        }
        let mut bad = http.clone(); bad["health_tcp_banner"] = json!("SSH-2.0-");
        assert!(request(bad).validate().is_err());
        for (field,value) in [("services",json!(["control"])),("services",json!([])),("container_port",json!(0)),
            ("service_protocol",json!("tcp")),("health_path",json!("//other/health")),("health_path",json!("/a#b")),
            ("health_path",json!("/a\\b")),("health_path",json!("/a\n")),("health_path",Value::Null)] {
            let mut bad=http.clone();bad[field]=value;assert!(request(bad).validate().is_err(),"{field}");
        }
    }

    #[test]
    fn native_gpus_and_durable_mounts_are_validated_and_require_attachment_capability() {
        let request = |spec:&Value| RegisterAppRequest {name:"native".into(),spec:spec.clone().into(),..Default::default()};
        let spec = json!({"runtime":{"executor":"shim","driver":"openshell"},"image":"approved","cpus":1,"gpus":2,"memory_gb":0.125,
            "mounts":[{"name":"data","target":"/data"},{"name":"models-1","target":"/srv/models","read_only":true}]});
        request(&spec).validate().unwrap();
        let entry = crate::workload::Entry(&spec);
        let mut node = NodeReport::default();
        node.runtime_capabilities = vec!["shim-openshell-provisioning-v1".into()];
        assert!(!node.supports_runtime(entry));
        node.runtime_capabilities.push("shim-openshell-attachments-v1".into());
        assert!(node.supports_runtime(entry));
        let mut plain = spec.clone(); plain["gpus"] = json!(0); plain.as_object_mut().unwrap().remove("mounts");
        node.runtime_capabilities.pop();
        assert!(node.supports_runtime(crate::workload::Entry(&plain)));
        for mounts in [json!({}), json!([{"name":"Data","target":"/d"}]), json!([{"name":"-d","target":"/d"}]),
            json!([{"name":"d","target":"d"}]), json!([{"name":"d","target":"/"}]), json!([{"name":"d","target":"/a/../b"}]),
            json!([{"name":"d","target":"/a:ro"}]), json!([{"name":"d","target":"/a,b"}]), json!([{"name":"d","target":"/a b"}]),
            json!([{"name":"d","target":"/d","read_only":"yes"}]), json!([{"name":"d","target":"/d","source":"/host"}]),
            json!([{"name":"d","target":"/a"},{"name":"d","target":"/b"}]), json!([{"name":"a","target":"/d"},{"name":"b","target":"/d"}]),
            Value::Array((0..17).map(|i| json!({"name":format!("v{i}"),"target":format!("/v{i}")})).collect())] {
            let mut bad = spec.clone(); bad["mounts"] = mounts.clone();
            assert!(request(&bad).validate().is_err(), "{mounts}");
        }
        // Durable mounts are native-only; attachments stay out of TEE drivers.
        let docker = json!({"runtime":{"executor":"shim","driver":"docker"},"image":"x","port_base":8000,"mounts":[{"name":"d","target":"/d"}]});
        assert!(request(&docker).validate().is_err());
        for driver in ["tee", "tee-simulation"] {
            let mut tee = spec.clone();
            tee["runtime"]["driver"] = json!(driver); tee["access_policy_sha256"] = json!("ab".repeat(32));
            assert!(request(&tee).validate().is_err(), "{driver}");
            tee["gpus"] = json!(0); tee.as_object_mut().unwrap().remove("mounts");
            request(&tee).validate().unwrap();
        }
        let mut rules = spec.clone();
        rules["runtime"]["driver"] = json!("docker"); rules["access_policy_sha256"] = json!("ab".repeat(32));
        request(&rules).validate().unwrap();
    }

    #[test]
    fn delegate_denials_expire_only_after_their_retention_and_keep_their_own_budget() {
        let ns = "ab".repeat(16);
        let denial = |session: usize, expires_at: Option<u64>| RouterSubjectRevocation {namespace:ns.clone(),
            principal:"12".repeat(32),session:Some(format!("{session:064x}")),application_only:Some(true),expires_at};
        // Longer or permanent covers; shorter, or expiring over permanent, does not.
        assert!(denial(1, Some(20)).covers(&denial(1, Some(10))) && denial(1, None).covers(&denial(1, Some(10))));
        assert!(!denial(1, Some(5)).covers(&denial(1, Some(10))) && !denial(1, Some(20)).covers(&denial(1, None)));
        // Only a namespace delegate's denial may expire.
        let mut operator = denial(1, Some(10)); operator.application_only = None;
        assert!(operator.validate().is_err() && denial(1, Some(0)).validate().is_err());
        denial(1, Some(10)).validate().unwrap();
        // Dropped only a grace period after expiry; permanent denials stay.
        let mut auth = RouterAuthorization {revision:1,policies:vec![],revoked_subjects:vec![denial(1, Some(100)), denial(2, None)],..Default::default()};
        assert!(!auth.drop_expired(100 + REVOCATION_EXPIRY_GRACE_SECS - 1));
        assert!(auth.drop_expired(100 + REVOCATION_EXPIRY_GRACE_SECS));
        assert_eq!(auth.revoked_subjects, [denial(2, None)]);
        // Expiring denials do not consume the permanent namespace budget.
        auth.revoked_subjects = (0..ROUTER_NAMESPACE_EXPIRING_REVOCATIONS).map(|i| denial(i, Some(100))).collect();
        auth.validate().unwrap();
        auth.validate_namespace_capacity(&ns).unwrap();
        auth.validate_expiring_capacity(&ns).unwrap();
        auth.revoked_subjects.push(denial(ROUTER_NAMESPACE_EXPIRING_REVOCATIONS, Some(100)));
        assert!(auth.validate_expiring_capacity(&ns).is_err());
        auth.revoked_subjects = (0..=MAX_ROUTER_EXPIRING_REVOCATIONS).map(|i| denial(i, Some(100))).collect();
        assert!(auth.validate().is_err());
        auth.revoked_subjects.pop();
        auth.validate().unwrap();
        // A present denial still refuses access, expired or not.
        let request = RouterAuthorizeRequest {namespace:ns.clone(),workload:"agent".into(),service:"chat".into(),
            principal:"12".repeat(32),session:format!("{:064x}", 1)};
        assert!(auth.authorize(&request).is_err());
    }

    #[test]
    fn native_ingress_and_logs_control_forward_caller_credentials_and_require_capability() {
        let request = |spec:&Value| RegisterAppRequest {name:"native".into(),spec:spec.clone().into(),..Default::default()};
        let spec = json!({"runtime":{"executor":"shim","driver":"openshell"},"image":"approved","cpus":1,"gpus":0,"memory_gb":0.125,
            "services":["chat","control"],"container_port":18789,"health_path":"/health",
            "ingress":{"chat":{"paths":["/v1/chat/completions"],"methods":["POST"],"single_use":true}}});
        request(&spec).validate().unwrap();
        let mut node = NodeReport::default();
        node.runtime_capabilities = vec!["shim-openshell-services-v1".into()];
        let mut control = spec.clone(); control.as_object_mut().unwrap().remove("ingress");
        let mut plain = control.clone(); plain["services"] = json!(["chat"]);
        for entry in [&spec, &control] {
            assert!(!node.supports_runtime(crate::workload::Entry(entry)));
        }
        assert!(node.supports_runtime(crate::workload::Entry(&plain)));
        node.runtime_capabilities.push("shim-openshell-ingress-v1".into());
        for entry in [&spec, &control, &plain] {
            request(entry).validate().unwrap();
            assert!(node.supports_runtime(crate::workload::Entry(entry)));
        }
        // A native node holds no application credential; control needs an application.
        let mut bad = Vec::new();
        let mut v = spec.clone(); v["ingress"]["chat"]["bearer_env"] = json!("TOKEN"); v["env"] = json!({"TOKEN":"x"}); bad.push(v);
        let mut v = control.clone(); v["services"] = json!(["control"]); bad.push(v);
        let mut v = spec.clone(); v["ingress"] = json!({"control":{"paths":["/"],"methods":["GET"]}}); bad.push(v);
        let mut v = spec.clone(); v["service_protocol"] = json!("tcp"); v["health_path"] = json!("/"); v["health_tcp_banner"] = json!("SSH-2.0-"); bad.push(v);
        for (index, v) in bad.iter().enumerate() {
            assert!(request(v).validate().is_err(), "{index}");
        }
        // TCP applications may still carry the logs-only control service.
        let mut tcp = control.clone();
        tcp["services"] = json!(["ssh","control"]); tcp["service_protocol"] = json!("tcp");
        tcp["health_path"] = json!("/"); tcp["health_tcp_banner"] = json!("SSH-2.0-");
        request(&tcp).validate().unwrap();
    }

    #[test]
    fn native_reservations_report_exactly_their_granted_gpu_ids() {
        let report = |gpus: u64, ids: &[&str], native: bool| {
            let mut app = json!({"name":"model","app":"model","namespace":"ns","kind":"app","replicas":1,"gpus":gpus,
                "runtime":{"executor":"shim","driver":if native {"openshell"} else {"docker"}},"services":[]});
            if !native { app["services"] = json!(["http"]); }
            let mut r = NodeReport { machine: "m".into(), apps: vec![app.into()], ..Default::default() };
            r.runtime_instances = serde_json::from_value(json!([{"entry_name":"model","ready":!native,
                "services":if native { json!([]) } else { json!(["http"]) },"gpu_ids":ids,
                "execution":{"namespace":"ns","workload":"model","allocation_id":"model-0","lease_id":"l","generation":1,"node_id":"m"}}])).unwrap();
            r
        };
        report(2, &["3", "5"], true).validate_runtime_instances().unwrap();
        report(0, &[], true).validate_runtime_instances().unwrap();
        report(1, &[], false).validate_runtime_instances().unwrap();
        for (gpus, ids, native) in [(2, &["3"][..], true), (2, &["3", "3"], true), (1, &["3", "5"], true),
            (1, &["gpu/3"], true), (1, &[""], true), (0, &["3"], true), (1, &["3"], false)] {
            assert!(report(gpus, ids, native).validate_runtime_instances().is_err(), "{gpus} {ids:?} {native}");
        }
        // Absent ids stay off the wire, so older schedulers read new reports unchanged.
        let encoded = serde_json::to_value(&report(0, &[], true).runtime_instances[0]).unwrap();
        assert!(encoded.get("gpu_ids").is_none());
    }

    #[test]
    fn access_rules_compose_with_each_driver_and_require_native_capabilities() {
        for driver in ["docker", "tee", "tee-simulation"] {
            let spec = json!({"runtime":{"executor":"shim","driver":driver}, "access_policy_sha256":"ab".repeat(32),
                "image":"test", "cpus":1,"gpus":0,"memory_gb":0.125});
            RegisterAppRequest { name:"rules".into(),spec:spec.clone().into(),..Default::default() }.validate().unwrap();
            let entry = crate::workload::Entry(&spec);
            let mut report = NodeReport::default();
            report.runtime_capabilities = vec![format!("shim-{driver}-v1"), "shim-openshell-provisioning-v1".into()];
            assert!(!report.supports_runtime(entry));
            report.runtime_capabilities.push("shim-openshell-access-v1".into());
            assert!(report.supports_runtime(entry));
            report.runtime_capabilities.remove(0);
            assert!(!report.supports_runtime(entry));
        }
    }

    #[test]
    fn tee_modes_and_transport_capabilities_are_not_interchangeable() {
        for (driver, capability) in [("tee", "shim-tee-v1"), ("tee-simulation", "shim-tee-simulation-v1")] {
            let spec = json!({"runtime":{"executor":"shim","driver":driver},"image":"test", "cpus":1,"gpus":0,
                "port_base":0,"container_port":8080,"services":["http"],"memory_gb":0.125});
            RegisterAppRequest { name:"tee".into(),spec:spec.clone().into(),..Default::default() }.validate().unwrap();
            let mut report = NodeReport::default();
            report.runtime_capabilities = vec!["shim-network-v1".into(),"shim-runtime-v2".into(),"shim-automatic-port-v1".into(),
                (if driver == "tee" {"shim-tee-simulation-v1"} else {"shim-tee-v1"}).into()];
            let entry = crate::workload::Entry(&spec);
            assert!(!report.supports_runtime(entry));
            report.runtime_capabilities.push(capability.into());
            assert!(report.supports_runtime(entry));
            let mut docker = spec.clone(); docker["runtime"]["driver"] = json!("docker");
            assert!(!report.supports_runtime(crate::workload::Entry(&docker)));
            report.runtime_capabilities.retain(|c| c != "shim-network-v1");
            assert!(!report.supports_runtime(entry));
        }
    }

    #[test]
    fn runtime_selection_is_explicit_and_rejects_unsupported_trust_options() {
        for runtime in [json!({"executor":"shim","driver":"docker"}),json!({"executor":"shim","driver":"openshell"})] {
            RegisterAppRequest::validate_runtime(&runtime).unwrap();
        }
        for runtime in [Value::Null,json!("shim"),json!({"executor":"legacy"}),json!({"executor":"shim","driver":"unknown"}),
            json!({"executor":"shim","driver":"docker","policy_proof":"required"})] {
            assert!(RegisterAppRequest::validate_runtime(&runtime).is_err());
        }
        let bare: NodeReport=serde_json::from_value(json!({"version":3,"machine":"m","ready":true,
            "total_gpus":0,"total_cpus":1,"free_gpus":0,"free_cpus":1})).unwrap();
        assert!(bare.supports_runtime(crate::workload::Entry(&json!({}))));
        assert!(!bare.supports_runtime(crate::workload::Entry(&json!({"runtime":{"executor":"shim","driver":"docker"}}))));
    }

    #[test]
    fn an_app_is_its_container_and_a_prover_also_its_program() {
        let app = |body: Value| -> anyhow::Result<()> {
            serde_json::from_value::<RegisterAppRequest>(body)?.validate()
        };
        let runtime = json!({"executor":"shim","driver":"docker"});
        let container = json!({"image":"serve:1","port_base":50100,"gpus":2,"replicas":1,"command":["serve","{port}"],"env":{"A":"1"},"runtime":runtime});
        app(json!({"name":"model","spec":container})).unwrap();
        for (key, value) in [("gpus", json!("2")), ("replicas", json!(0)), ("port_base", json!(-1)), ("port_base", Value::Null),
            ("image", Value::Null), ("workdir", json!(1)), ("env", json!({"A":1})), ("kind", json!("vm")),
            ("proof_storage_root", json!("/proofs")), ("name", json!("x")), ("runtime", json!({"executor":"legacy"}))] {
            let mut spec = container.clone();
            spec[key] = value;
            assert!(app(json!({"name":"model","spec":spec})).is_err(), "{key}");
        }
        let proof = json!({"elf_id":"ab".repeat(32),"class":"latency"});
        let prover = json!({"image":"ziren-gpu:1","gpus":1,"proof_storage_root":"/proofs"});
        app(json!({"name":"eth","spec":prover,"proof":proof})).unwrap();
        app(json!({"name":"eth","spec":{"kind":"ziren-cpu","image":"ziren-cpu:1","gpus":0},"proof":proof})).unwrap();
        assert!(app(json!({"name":"eth","spec":prover})).is_err(), "a prover proves a program");
        let mut plain = container.clone();
        plain["kind"] = json!("app");
        assert!(app(json!({"name":"model","spec":plain,"proof":proof})).is_err(), "a container app proves none");
        for (key, value) in [("services", json!(["http"])), ("runtime", json!({"executor":"shim","driver":"tee"})), ("container_port", Value::Null)] {
            let mut spec = prover.clone();
            spec[key] = value;
            assert!(app(json!({"name":"eth","spec":spec,"proof":proof})).is_err(), "a prover has only its health port, on shim Docker: {key}");
        }
        assert!(app(json!({"name":"eth","elf_id":"ab".repeat(32)})).is_err(), "an app is created from its spec");
        assert!(app(json!({"name":"model","node":"gpu","spec":container})).is_err(), "the scheduler picks the node");
    }

    #[test]
    fn node_reports_share_the_json_and_protobuf_contract() {
        let source = json!({"version":3,"machine":"gpu","ready":true,
            "total_gpus":8,"total_cpus":64,"free_gpus":6,"free_cpus":32,
            "batches":{"training":{"image":"app","counter":u64::MAX}},
            "jobs":[{"id":"job","lease":"attempt","status":{"state":"running"},
                "spec":{"namespace":"alice"},"cleaned":false,"logs_ready":false}]});
        let report: NodeReport = serde_json::from_value(source).unwrap();
        let decoded = NodeReport::decode(report.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded, report);
        assert_eq!(decoded.batches["training"]["counter"], json!(u64::MAX));
        assert!(serde_json::from_value::<NodeReport>(json!({"version":3,"unknown":true})).is_err());
    }

    #[test]
    fn token_grant_presence_and_private_fields_survive_codegen() {
        let mut token = Token {
            id: "id".into(),
            address: "private signer".into(),
            token_hash: Some("private hash".into()),
            ..Default::default()
        };
        for apps in [
            None,
            Some(StringList::default()),
            Some(vec!["training".into()].into()),
        ] {
            token.apps = apps;
            let decoded = Token::decode(token.encode_to_vec().as_slice()).unwrap();
            assert_eq!(decoded.apps, token.apps);
            let public = serde_json::to_value(&decoded).unwrap();
            assert!(public.get("address").is_none());
            assert!(public.get("token_hash").is_none());
            assert_eq!(public["apps"], serde_json::to_value(&token.apps).unwrap());
        }
    }

    #[test]
    fn hard_limits_and_partial_reservations_are_only_for_capped_shim_containers() {
        let validate = |spec: Value| RegisterAppRequest { name:"agent".into(), spec:spec.into(), ..Default::default() }.validate();
        let capped = json!({"image":"agent:1","port_base":50100,"gpus":0,"cpus":0,"cpu_limit":2,"memory_gb":1,"memory_limit_gb":1.5,
            "runtime":{"executor":"shim","driver":"docker"}});
        for driver in ["docker", "tee", "tee-simulation"] {
            let mut spec = capped.clone(); spec["runtime"]["driver"] = json!(driver);
            validate(spec.clone()).unwrap();
            spec["cpus"] = json!(0.25);
            validate(spec).unwrap();
        }
        // Without one, nothing changes: a whole reservation, no limit.
        validate(json!({"image":"web:1","port_base":50100,"cpus":2,"runtime":{"executor":"shim","driver":"docker"}})).unwrap();
        validate(json!({"image":"web:1","port_base":50100,"cpus":2,"cpu_limit":2.5,"runtime":{"executor":"shim","driver":"docker"}})).unwrap();
        let refused = |change: Value| {
            let mut spec = capped.clone();
            for (k, v) in change.as_object().unwrap() {
                if v.is_null() { spec.as_object_mut().unwrap().remove(k); } else { spec[k] = v.clone(); }
            }
            validate(spec).unwrap_err().to_string()
        };
        for (change, message) in [
            (json!({"runtime":{"executor":"shim","driver":"openshell"},"cpus":1,"port_base":null}), "don't apply to a native OpenShell app"),
            (json!({"cpu_limit":null}), "also set cpu_limit, memory_gb and memory_limit_gb"),
            (json!({"memory_gb":null}), "also set cpu_limit, memory_gb and memory_limit_gb"),
            (json!({"memory_limit_gb":null}), "also set cpu_limit, memory_gb and memory_limit_gb"),
            (json!({"gpus":1}), "only for a kind: app without GPUs"),
            (json!({"cpus":3}), "must be at least cpus (3)"),
            (json!({"memory_gb":2}), "must be at least memory_gb (2)"),
            (json!({"memory_limit_gb":0.005}), "at least 6 MiB"),
            (json!({"memory_limit_gb":1e12}), "at least 6 MiB"),
            (json!({"cpu_limit":0.005}), "at least 0.01 CPUs"),
            (json!({"cpu_limit":5e6}), "at least 0.01 CPUs"),
            (json!({"cpu_limit":0}), "invalid app setting cpu_limit"),
            (json!({"memory_limit_gb":-1}), "invalid app setting memory_limit_gb"),
            (json!({"cpus":-1}), "invalid app setting cpus"),
            (json!({"cpus":"1"}), "invalid app setting cpus"),
        ] {
            let error = refused(change.clone());
            assert!(error.contains(message), "{change}: {error}");
        }
        let mut prover = capped.as_object().unwrap().clone();
        prover.insert("kind".into(), json!("ziren-cpu"));
        let error = RegisterAppRequest::validate_resources(&prover).unwrap_err().to_string();
        assert!(error.contains("only for a kind: app without GPUs"), "{error}");
        // An app without the limits keeps whole CPUs.
        for cpus in [0.5, 0.0] {
            assert!(validate(json!({"image":"web:1","port_base":50100,"cpus":cpus,"runtime":{"executor":"shim","driver":"docker"}})).is_err());
        }
        // The smallest hard limits shim accepts are accepted.
        let mut spec = capped.clone(); (spec["cpu_limit"], spec["memory_gb"], spec["memory_limit_gb"]) = (json!(0.01), json!(0.005859375), json!(0.005859375));
        validate(spec).unwrap();
    }

    #[test]
    fn limits_and_unreserved_cpus_need_node_capabilities_and_a_tee_limit_fits_its_vcpus() {
        use crate::workload::{Entry, CPU_SHARES_CAPABILITY as SHARES, LIMITS_CAPABILITY as LIMITS, TEE_CPU_OVERCOMMIT_CAPABILITY as OVERCOMMIT};
        let report = |caps: &[&str]| NodeReport { total_cpus: 8.0, runtime_capabilities: caps.iter().map(|c| c.to_string()).collect(), ..Default::default() };
        let plain = json!({"kind":"app","cpus":2,"runtime":{"executor":"shim","driver":"docker"}});
        assert!(report(&["shim-docker-v1"]).supports_runtime(Entry(&plain)), "an app without limits needs nothing new");
        let mut limited = plain.clone(); limited["memory_limit_gb"] = json!(4);
        assert!(!report(&["shim-docker-v1"]).supports_runtime(Entry(&limited)));
        assert!(report(&["shim-docker-v1", LIMITS]).supports_runtime(Entry(&limited)));
        let mut unreserved = limited.clone(); (unreserved["cpus"], unreserved["cpu_limit"]) = (json!(0), json!(1));
        assert!(!report(&["shim-docker-v1", LIMITS]).supports_runtime(Entry(&unreserved)));
        assert!(report(&["shim-docker-v1", LIMITS, SHARES]).supports_runtime(Entry(&unreserved)));
        let mut half = unreserved.clone(); half["cpus"] = json!(0.5);
        assert!(report(&["shim-docker-v1", LIMITS]).supports_runtime(Entry(&half)), "a fraction is a reservation: normal weight");
        let mut tee = unreserved.clone(); tee["runtime"]["driver"] = json!("tee");
        assert!(!report(&["shim-tee-v1", LIMITS, SHARES]).supports_runtime(Entry(&tee)));
        assert!(report(&["shim-tee-v1", LIMITS, SHARES, OVERCOMMIT]).supports_runtime(Entry(&tee)));
        assert!(report(&[]).fits_cpu_limit(Entry(&tee)));
        tee["cpu_limit"] = json!(8.5);
        assert!(!report(&[]).fits_cpu_limit(Entry(&tee)));
        assert!(report(&[]).fits_cpu_limit(Entry(&json!({"cpu_limit":8.5,"runtime":{"executor":"shim","driver":"docker"}}))));
    }
}
