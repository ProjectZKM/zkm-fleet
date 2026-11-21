//! Behavior on generated records; field definitions live in model.proto.
use crate::fleet::v1::ProofKind;
use crate::model::v1::*;
use crate::stage::v1::Status as StageStatus;
use serde_json::Value;

pub const MAX_ROUTER_REVOCATIONS: usize = 4096;
pub fn valid_router_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 63 && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}
pub fn valid_route_id(id: &str) -> bool {
    id.len() == 64 && id.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
impl RouterControl {
    pub fn validate(&self) -> anyhow::Result<()> {
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
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(router_namespace(&self.namespace) && valid_route_id(&self.principal)
            && self.session.as_ref().is_none_or(|s| valid_route_id(s)), "invalid subject revocation");
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
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.revision > 0 && self.policies.len() + self.revoked_subjects.len() <= MAX_ROUTER_AUTHORIZATION_RECORDS,
            "authorization state capacity exceeded");
        let mut policies = std::collections::HashSet::new();
        for p in &self.policies {
            p.validate()?;
            anyhow::ensure!(policies.insert((&p.namespace, &p.workload, &p.service)), "duplicate access policy");
        }
        let mut subjects = std::collections::HashSet::new();
        for r in &self.revoked_subjects {
            r.validate()?;
            anyhow::ensure!(subjects.insert((&r.namespace, &r.principal, &r.session)), "duplicate subject revocation");
        }
        Ok(())
    }
    pub fn authorize(&self, request: &RouterAuthorizeRequest) -> anyhow::Result<RouterAccessBinding> {
        request.validate()?;
        anyhow::ensure!(!self.revoked_subjects.iter().any(|r| r.namespace == request.namespace && r.principal == request.principal
            && r.session.as_ref().is_none_or(|s| s == &request.session)), "subject revoked");
        let scoped = |p: &&RouterAccessPolicy| p.namespace == request.namespace && p.service == request.service;
        let policy = self.policies.iter().filter(scoped).find(|p| p.workload == request.workload)
            .or_else(|| self.policies.iter().filter(scoped).find(|p| p.workload == "*"))
            .filter(|p| p.enabled).ok_or_else(|| anyhow::anyhow!("service not authorized"))?;
        Ok(RouterAccessBinding {principal: request.principal.clone(), session: request.session.clone(),
            policy_version: policy.version, policy_sha256: policy.sha256.clone()})
    }
}

impl NodeReport {
    pub fn validate_native_provers(&self) -> anyhow::Result<()> {
        for entry in self.entries() {
            if let Some(binary) = entry.0.get("prover_binary").filter(|v| !v.is_null()) {
                anyhow::ensure!(matches!(entry.kind(), "ziren-gpu" | "ziren-cpu")
                    && !entry.integrated_worker() && !entry.machine().is_empty()
                    && entry.machine() == self.machine
                    && binary.as_str().is_some_and(|p| p.starts_with('/') && p.len() <= 4096
                        && !p.chars().any(char::is_control)),
                    "prover_binary requires an absolute path and a native prover pinned to the reporting machine");
            }
        }
        anyhow::ensure!(self.native_provers.len() <= 1024, "too many native prover processes");
        let mut seen = std::collections::HashSet::new();
        for process in &self.native_provers {
            let entry = self.entry(&process.entry_name)
                .ok_or_else(|| anyhow::anyhow!("native prover process has no app"))?;
            let prefix = format!("{}-", entry.name());
            anyhow::ensure!(matches!(entry.kind(), "ziren-gpu" | "ziren-cpu") && !entry.integrated_worker()
                && process.replica_name.strip_prefix(&prefix).and_then(|s| s.parse::<u64>().ok())
                    .is_some_and(|index| index < entry.replicas() && process.replica_name == format!("{prefix}{index}"))
                && process.executable.starts_with('/') && process.executable.len() <= 4096
                && !process.executable.chars().any(char::is_control)
                && seen.insert(&process.replica_name), "invalid native prover process");
        }
        Ok(())
    }

    pub fn validate_runtime_instances(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.runtime_instances.len() <= 1024, "too many runtime instances");
        let mut seen = std::collections::HashSet::new();
        for instance in &self.runtime_instances {
            let e = &instance.execution;
            let entry = self.entry(&instance.entry_name).ok_or_else(|| anyhow::anyhow!("runtime instance has no app"))?;
            anyhow::ensure!(entry.0["runtime"]["executor"] == "shim" && entry.is_container()
                && e.node_id == self.machine && e.generation > 0
                && e.workload == entry.app() && (e.namespace == entry.namespace() || Some(e.namespace.as_str()) == entry.namespace_reference())
                && [&e.namespace,&e.workload,&e.allocation_id,&e.lease_id,&e.node_id].iter().all(|v| !v.is_empty() && v.len() <= 256 && !v.chars().any(char::is_control))
                && seen.insert((&e.namespace,&e.allocation_id,&e.node_id)), "invalid runtime execution identity");
            let prefix = format!("{}-",instance.entry_name);
            anyhow::ensure!(e.allocation_id.strip_prefix(&prefix).and_then(|s|s.parse::<u64>().ok()).is_some_and(|index| index < entry.replicas()), "invalid runtime replica");
            let expected: Vec<String> = entry.0["services"].as_array().into_iter().flatten().filter_map(Value::as_str).map(str::to_owned).collect();
            // An unhealthy workload may retain only its declared management
            // endpoint so an administrator can repair this exact allocation.
            anyhow::ensure!(instance.services == expected || (instance.services == ["control"]
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
pub fn default_jobs_limit() -> u64 {
    20
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
    pub fn target(&self) -> &Value {
        &self.target
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
    pub const SPEC_SETTINGS: [&'static str; 11] = ["kind", "image", "replicas", "gpus", "cpus", "shm_gb",
        "memory_gb", "env", "volumes", "ready_timeout_s", "stop_timeout_s"];
    /// Settings only a container app's spec (`kind: app`) may give.
    pub const CONTAINER_SETTINGS: [&'static str; 11] = ["command", "port_base", "health_path", "workdir", "user", "runtime", "services", "container_port", "entrypoint", "ingress", "service_protocol"];

    pub fn validate_transport(spec: &serde_json::Map<String, Value>) -> anyhow::Result<()> {
        if ["services", "container_port", "entrypoint", "ingress", "service_protocol"].iter().any(|k| spec.contains_key(*k)) {
            anyhow::ensure!(spec.get("runtime") == Some(&serde_json::json!({"executor":"shim","driver":"docker"})),
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
            anyhow::ensure!(spec.get("runtime") == Some(&serde_json::json!({"executor":"shim","driver":"docker"})) && spec.get("container_port").is_some(), "automatic host ports require shim and container_port");
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

    pub fn validate_runtime(runtime: &Value) -> anyhow::Result<()> {
        anyhow::ensure!(runtime == &serde_json::json!({"executor":"legacy"})
            || runtime == &serde_json::json!({"executor":"shim", "driver":"docker"}),
            "runtime must select legacy or shim with driver docker; trust options are not supported here");
        Ok(())
    }

    /// The runtime of the app's container: `app` unless it proves a program, which a Ziren GPU
    /// prover does unless the spec asks for a CPU one.
    pub fn kind(&self) -> &str {
        self.spec.get("kind").and_then(Value::as_str)
            .unwrap_or(if self.proof.is_some() { "ziren-gpu" } else { "app" })
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
        let own: &[&str] = match kind {
            "app" => &Self::CONTAINER_SETTINGS,
            "ziren-gpu" | "ziren-cpu" => &Self::PROVER_SETTINGS,
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
        if kind == "app" {
            anyhow::ensure!(set("port_base").is_some(), "port_base is required: replica i serves on port_base + i");
        }
        if let Some(runtime) = spec.get("runtime") {
            Self::validate_runtime(runtime)?;
        }
        Self::validate_transport(spec)?;
        let strings = |v: &Value| v.as_array().is_none_or(|a| a.iter().any(|v| !v.is_string()));
        let positive = |v: &Value| v.as_f64().is_none_or(|v| v <= 0.0 || !v.is_finite());
        let invalid = ["user", "health_path", "workdir", "proof_storage_root", "backend", "rust_log"].into_iter()
                .find(|k| set(k).is_some_and(|v| !v.is_string()))
            .or_else(|| ["cpus", "shm_gb", "port_base", "stop_timeout_s"].into_iter().find(|k| set(k).is_some_and(|v| v.as_u64().is_none())))
            .or_else(|| ["memory_gb", "ready_timeout_s"].into_iter().find(|k| set(k).is_some_and(positive)))
            .or_else(|| ["command", "volumes"].into_iter().find(|k| set(k).is_some_and(strings)))
            .or_else(|| set("replicas").is_some_and(|r| r.as_u64().is_none_or(|r| !(1..=64).contains(&r))).then_some("replicas"))
            .or_else(|| set("gpus").is_some_and(|g| g.as_f64().is_none_or(|g| g < 0.0 || !g.is_finite())).then_some("gpus"))
            .or_else(|| set("env").is_some_and(|e| e.as_object().is_none_or(|e| e.values().any(|v| !v.is_string()))).then_some("env"));
        if let Some(key) = invalid {
            anyhow::bail!("invalid app setting {key}");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use serde_json::json;

    #[test]
    fn namespace_service_defaults_never_override_exact_denials_or_cross_namespaces() {
        let policy = RouterAccessPolicy { namespace:"ab".repeat(16),workload:"*".into(),service:"chat".into(),
            version:1,sha256:"cd".repeat(32),enabled:true };
        let mut auth=RouterAuthorization {revision:1,policies:vec![policy.clone()],revoked_subjects:vec![]};
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
        for (field,value) in [("service_protocol",json!("udp")),("runtime",json!({"executor":"legacy"})),
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
    fn runtime_selection_is_explicit_and_rejects_unsupported_trust_options() {
        for runtime in [json!({"executor":"legacy"}),json!({"executor":"shim","driver":"docker"})] {
            RegisterAppRequest::validate_runtime(&runtime).unwrap();
        }
        for runtime in [Value::Null,json!("shim"),json!({"executor":"shim","driver":"openshell"}),
            json!({"executor":"shim","driver":"docker","policy_proof":"required"})] {
            assert!(RegisterAppRequest::validate_runtime(&runtime).is_err());
        }
        let legacy: NodeReport=serde_json::from_value(json!({"version":3,"machine":"m","ready":true,
            "total_gpus":0,"total_cpus":1,"free_gpus":0,"free_cpus":1})).unwrap();
        assert!(legacy.supports_runtime(crate::workload::Entry(&json!({}))));
        assert!(!legacy.supports_runtime(crate::workload::Entry(&json!({"runtime":{"executor":"shim","driver":"docker"}}))));
    }

    #[test]
    fn an_app_is_its_container_and_a_prover_also_its_program() {
        let app = |body: Value| -> anyhow::Result<()> {
            serde_json::from_value::<RegisterAppRequest>(body)?.validate()
        };
        let container = json!({"image":"serve:1","port_base":50100,"gpus":2,"replicas":1,"command":["serve","{port}"],"env":{"A":"1"}});
        app(json!({"name":"model","spec":container})).unwrap();
        for (key, value) in [("gpus", json!("2")), ("replicas", json!(0)), ("port_base", json!(-1)), ("port_base", Value::Null),
            ("image", Value::Null), ("workdir", json!(1)), ("env", json!({"A":1})), ("kind", json!("vm")),
            ("proof_storage_root", json!("/proofs")), ("name", json!("x"))] {
            let mut spec = container.clone();
            spec[key] = value;
            assert!(app(json!({"name":"model","spec":spec})).is_err(), "{key}");
        }
        let proof = json!({"elf_id":"ab".repeat(32),"class":"latency"});
        let prover = json!({"image":"ziren-gpu:1","gpus":1,"proof_storage_root":"/proofs"});
        app(json!({"name":"eth","spec":prover,"proof":proof})).unwrap();
        app(json!({"name":"eth","spec":{"kind":"ziren-cpu","image":"ziren-cpu:1","gpus":0},"proof":proof})).unwrap();
        assert!(app(json!({"name":"eth","spec":prover})).is_err(), "a prover proves a program");
        assert!(app(json!({"name":"model","spec":container,"proof":proof})).is_err(), "a container app proves none");
        assert!(app(json!({"name":"eth","spec":{"image":"ziren-gpu:1","port_base":1},"proof":proof})).is_err(),
            "a prover takes no container settings");
        assert!(app(json!({"name":"eth","spec":{"image":"ziren-gpu:1","prover_binary":"/tmp/agent-supplied"},"proof":proof})).is_err(),
            "prover binary selection belongs to administrator-owned node configuration");
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
}
