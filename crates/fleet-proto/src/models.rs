//! Behavior on generated records; field definitions live in model.proto.
use crate::fleet::v1::ProofKind;
use crate::model::v1::*;
use crate::stage::v1::Status as StageStatus;
use serde_json::{json, Value};

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

impl Job {
    pub fn value(&self) -> Value {
        let mut v = self.status.0.clone();
        for (k, x) in [
            ("type", json!("batch")),
            ("id", json!(self.id)),
            ("batch", json!(self.batch)),
            ("namespace", json!(self.namespace)),
            ("tag", json!(self.tag)),
            ("owner", json!(self.owner)),
            ("priority", self.policy["priority"].clone()),
            ("worker", json!(self.worker)),
            ("node", json!(self.worker)),
        ] {
            v[k] = x;
        }
        v
    }
    pub fn state(&self) -> &str {
        self.status["state"].as_str().unwrap_or("")
    }
    pub fn gpus(&self) -> f64 {
        self.request["gpus"].as_f64().unwrap_or(0.0)
    }
    pub fn cpus(&self) -> f64 {
        self.request["cpus"].as_f64().unwrap_or(1.0)
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

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn node_reports_share_the_json_and_protobuf_contract() {
        let source = json!({"version":2,"machine":"gpu","ready":true,
            "total_gpus":8,"total_cpus":64,"free_gpus":6,"free_cpus":32,
            "batches":{"training":{"image":"app","counter":u64::MAX}},
            "jobs":[{"id":"job","lease":"attempt","status":{"state":"running"},
                "spec":{"namespace":"alice"},"cleaned":false,"logs_ready":false}]});
        let report: NodeReport = serde_json::from_value(source).unwrap();
        let decoded = NodeReport::decode(report.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded, report);
        assert_eq!(decoded.batches["training"]["counter"], json!(u64::MAX));
        assert!(serde_json::from_value::<NodeReport>(json!({"version":2,"unknown":true})).is_err());
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
