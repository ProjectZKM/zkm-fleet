//! Read-only mTLS observation of the unmodified gateway. No business traffic or
//! gateway credentials go to a node. Readiness is deliberately not persisted.
use super::*;
use crate::config::OpenShellGatewayConfig;
use std::{io::Read, os::unix::fs::OpenOptionsExt, time::Instant};
use trusted_shim::openshell_gateway::protocol::{self, open_shell_client::OpenShellClient};

const FRESHNESS: Duration = Duration::from_secs(5);
const RPC_TIMEOUT: Duration = Duration::from_secs(2);

struct Observation {
    execution: trusted_shim::contracts::ExecutionIdentity,
    session: String,
    instance: String,
    policy: trusted_shim::contracts::PolicyRef,
    started: Instant,
}

#[derive(Default)]
pub(super) struct Cache(Mutex<BTreeMap<String, Observation>>);
impl Cache {
    pub(super) fn remove(&self, name: &str) {
        self.0.lock().remove(name);
    }
}

fn certificate(path: &Path, secret: bool) -> anyhow::Result<Vec<u8>> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let m = file.metadata()?;
    anyhow::ensure!(
        m.is_file()
            && m.len() <= 64 * 1024
            && m.permissions().mode() & (if secret { 0o077 } else { 0o022 }) == 0
            && (!secret || m.uid() == unsafe { libc::geteuid() }),
        "OpenShell mTLS files must be bounded regular files with protected permissions"
    );
    let mut bytes = Vec::new();
    file.take(64 * 1024 + 1).read_to_end(&mut bytes)?;
    anyhow::ensure!(
        !bytes.is_empty() && bytes.len() <= 64 * 1024,
        "invalid OpenShell mTLS file"
    );
    Ok(bytes)
}

/// The operator's mTLS client for the stock gateway. `rpc_timeout` bounds each
/// call; long-running streams (administrative exec) bound themselves instead.
pub(super) fn gateway_client(
    config: &OpenShellGatewayConfig,
    rpc_timeout: Option<Duration>,
) -> anyhow::Result<OpenShellClient<tonic_native::transport::Channel>> {
    config.validate()?;
    fleet_proto::initialize_tls();
    use tonic_native::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(certificate(&config.ca_file, false)?))
        .identity(Identity::from_pem(
            certificate(&config.cert_file, false)?,
            certificate(&config.key_file, true)?,
        ));
    let mut endpoint = Endpoint::from_shared(config.endpoint.clone())?.connect_timeout(RPC_TIMEOUT);
    if let Some(timeout) = rpc_timeout {
        endpoint = endpoint.timeout(timeout);
    }
    Ok(OpenShellClient::new(endpoint.tls_config(tls)?.connect_lazy())
        .max_decoding_message_size(MAX_RECORD))
}

#[derive(Clone)]
pub(crate) struct Observer(OpenShellClient<tonic_native::transport::Channel>);
impl Observer {
    pub(crate) fn new(config: &OpenShellGatewayConfig) -> anyhow::Result<Self> {
        Ok(Self(gateway_client(config, Some(RPC_TIMEOUT))?))
    }

    async fn check(&self, record: &Record) -> Result<()> {
        let request = record.binding()?.request();
        let query = protocol::GetSandboxRequest {
            name: record.name.clone(),
            workspace_scope: request.workspace_scope.clone(),
        };
        let mut client = self.0.clone();
        let desired = client
            .get_sandbox_config(protocol::sandbox::v1::GetSandboxConfigRequest {
                name: record.name.clone(),
                workspace_scope: request.workspace_scope.clone(),
            })
            .await
            .map_err(|_| Error::Unavailable)?
            .into_inner();
        // Use the current desired revision as well as the loaded admission.
        // A failed or pending update must not leave an old ready policy routable.
        let revision = client
            .get_sandbox_policy_status(protocol::GetSandboxPolicyStatusRequest {
                sandbox: record.name.clone(),
                workspace_scope: request.workspace_scope,
                version: 0,
                global: false,
            })
            .await
            .map_err(|_| Error::Unavailable)?
            .into_inner();
        let sandbox = client
            .get_sandbox(query)
            .await
            .map_err(|_| Error::Unavailable)?
            .into_inner()
            .sandbox
            .ok_or(Error::Rejected)?;
        validate(record, &sandbox, &revision)
            .and_then(|_| validate_configuration(&sandbox, &desired))
    }

    // One caller per scheduler; bounded parallelism prevents a slow sandbox
    // delaying the entire fleet. Timeout includes channel reconnect/queueing.
    pub(crate) async fn refresh(&self, c: &Arc<WorkloadController>) {
        let Ok(store) = launches(c) else {
            return;
        };
        store
            .readiness
            .0
            .lock()
            .retain(|_, v| v.started.elapsed() < FRESHNESS);
        let records = store.active_records(c);
        let mut tasks = tokio::task::JoinSet::new();
        for record in records {
            if tasks.len() >= 16 {
                let _ = tasks.join_next().await;
            }
            let observer = self.clone();
            let c = c.clone();
            tasks.spawn(async move {
                observer.refresh_one(&c, record).await;
            });
        }
        while tasks.join_next().await.is_some() {}
    }

    async fn refresh_one(&self, c: &Arc<WorkloadController>, record: Record) {
        let Ok(store) = launches(c) else {
            return;
        };
        let result = async {
            let launch = record.launch()?;
            if record.phase != "created" || launch.services.is_empty() {
                return Err(Error::Rejected);
            }
            let started = Instant::now();
            let before = c.native_readiness_basis(&launch)?;
            // Public compute details may be absent. The Fleet Compute extension
            // pins them in its Create journal; join a fresh owned observation
            // with this exact public sandbox ID.
            Compute(c.clone()).observe(&record)?;
            tokio::time::timeout(RPC_TIMEOUT, self.check(&record))
                .await
                .map_err(|_| Error::Unavailable)??;
            if c.native_readiness_basis(&launch)? != before {
                return Err(Error::Rejected);
            }
            let p = c.policy.as_ref().ok_or(Error::Unavailable)?;
            if p.ledger.authority_snapshot(&launch.execution)?.0.policy != launch.policy {
                return Err(Error::Rejected);
            }
            Ok(Observation {
                execution: launch.execution,
                session: before.1,
                instance: record.instance_id.clone().ok_or(Error::Rejected)?,
                policy: launch.policy,
                started,
            })
        }
        .await;
        let mut cache = store.readiness.0.lock();
        match result {
            Ok(o) if o.started.elapsed() < FRESHNESS => {
                cache.insert(record.name, o);
            }
            _ => {
                cache.remove(&record.name);
            }
        }
    }

    pub(crate) async fn run(self, c: Arc<WorkloadController>) {
        loop {
            self.refresh(&c).await;
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

fn validate_configuration(
    sandbox: &protocol::Sandbox,
    desired: &protocol::sandbox::v1::GetSandboxConfigResponse,
) -> Result<()> {
    let meta = sandbox.metadata.as_ref().ok_or(Error::Rejected)?;
    let admission = sandbox
        .status
        .as_ref()
        .and_then(|s| s.configuration_admission.as_ref())
        .ok_or(Error::Rejected)?;
    if !desired.configuration_admitted
        || !desired.configuration_error.is_empty()
        || desired.workspace != meta.workspace
        || desired.configuration_instance_id != admission.instance_id
        || desired.version != admission.policy_version
        || desired.policy_hash != admission.policy_hash
        || desired.config_revision != admission.config_revision
        || desired.provider_env_revision != admission.provider_env_revision
    {
        return Err(Error::Rejected);
    }
    Ok(())
}

fn validate(
    record: &Record,
    sandbox: &protocol::Sandbox,
    revision: &protocol::GetSandboxPolicyStatusResponse,
) -> Result<()> {
    let binding = record.binding()?;
    let meta = sandbox.metadata.as_ref().ok_or(Error::Rejected)?;
    let status = sandbox.status.as_ref().ok_or(Error::Rejected)?;
    let admission = status
        .configuration_admission
        .as_ref()
        .ok_or(Error::Rejected)?;
    let latest = revision.revision.as_ref().ok_or(Error::Rejected)?;
    let ready = |name: &str| {
        let mut conditions = status.conditions.iter().filter(|c| c.r#type == name);
        conditions.next().is_some_and(|c| c.status == "True") && conditions.next().is_none()
    };
    if record.sandbox_id.as_deref() != Some(meta.id.as_str())
        || meta.name != record.name
        || meta.workspace != workspace_name(&record.launch()?.execution.namespace)
        || meta.deletion_time.is_some()
        || record.instance_id.as_ref().is_none_or(|id| id.is_empty())
        || (!status.agent_pod.is_empty()
            && record.instance_id.as_deref() != Some(status.agent_pod.as_str()))
        || status.phase != protocol::SandboxPhase::Ready as i32
        || !ready("Ready")
        || !ready("ConfigurationReady")
        || status
            .conditions
            .iter()
            .any(|c| c.r#type == "DesiredConfigurationReady" && c.status != "True")
        || status.configuration_activated != Some(true)
        || admission.state != protocol::ConfigurationAdmissionState::Accepted as i32
        || admission.instance_id.is_empty()
        || status.main_process_instance_id.is_empty()
        || !admission.error.is_empty()
        || admission.policy_hash != binding.policy_sha256()
        || admission.policy_version == 0
        || admission.policy_version != status.current_policy_version
        || latest.version != admission.policy_version
        || latest.version != revision.active_version
        || latest.policy_hash != admission.policy_hash
        || latest.status != protocol::PolicyStatus::Loaded as i32
        || !latest.load_error.is_empty()
    {
        return Err(Error::Rejected);
    }
    Ok(())
}

/// Caller holds the live deployment/session fence. Recheck policy revocation,
/// exact execution and physical instance synchronously at every publication.
pub(crate) fn services_ready(
    c: &WorkloadController,
    instance: &fleet_proto::model::v1::RuntimeInstance,
    session: &str,
) -> bool {
    let Some(p) = c.policy.as_ref() else {
        return false;
    };
    let Some(store) = p.openshell.as_ref() else {
        return false;
    };
    let execution = crate::policy_gateway::identity(instance.execution.clone());
    let native = instance
        .native_observation_base64
        .as_ref()
        .filter(|raw| raw.len() <= 21848)
        .and_then(|raw| STANDARD.decode(raw).ok())
        .and_then(|raw| DriverSandbox::decode(raw.as_slice()).ok());
    let Some(native) = native else {
        return false;
    };
    let record = store.records.lock().get(&native.name).cloned();
    let Some(record) = record else {
        return false;
    };
    let cache = store.readiness.0.lock();
    let Some(observed) = cache.get(&native.name) else {
        return false;
    };
    if observed.execution != execution
        || observed.started.elapsed() >= FRESHNESS
        || observed.session != session
    {
        return false;
    }
    record.phase == "created"
        && record.sandbox_id.as_deref() == Some(native.id.as_str())
        && record.launch().is_ok_and(|l| l.execution == execution)
        && native.workspace == workspace_name(&execution.namespace)
        && native.status.as_ref().is_some_and(|s| {
            s.instance_id == observed.instance
                && s.conditions
                    .iter()
                    .any(|c| c.r#type == "Ready" && c.status == "True")
        })
        && p.ledger
            .authority_snapshot(&execution)
            .is_ok_and(|s| s.0.policy == observed.policy)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (
        Record,
        protocol::Sandbox,
        protocol::GetSandboxPolicyStatusResponse,
    ) {
        let launch: LaunchRequest = serde_json::from_value(json!({
            "execution":{"namespace":"test","workload":"agent","allocation_id":"agent-0","lease_id":"one","generation":1,"node_id":"node"},
            "driver":"openshell","image":"agent@sha256:approved","command":["agent","serve"],"environment":{},
            "limits":{"cpu_millicores":1000,"memory_bytes":67108864},
            "policy":{"issuer":"fleet","policy_id":"create","version":1,"sha256":"a".repeat(64)},
            "trust":{"attestation":{"mode":"skip_non_tee"},"policy_proof":"not_required","attestation_proof":"not_required"},"services":[]
        })).unwrap();
        let policy = SandboxPolicy::default();
        let binding = GatewayLaunch::new(launch.clone(), policy.clone()).unwrap();
        let name = binding.request().name;
        let record = Record {
            preparation: serde_json::from_value(json!({"request":launch,"native_policy_base64":STANDARD.encode(policy.encode_to_vec()),
                "principal":"alice","expires_at":now()+600,"cost":1})).unwrap(),
            name: name.clone(), phase:"created".into(), sandbox_id:Some("id".into()),
            native_sha256:None, job_id:None, request:None, policy:None, instance_id:Some("physical".into()),
        };
        let sandbox = protocol::Sandbox {
            metadata: Some(protocol::datamodel::v1::ObjectMeta {
                id: "id".into(),
                name,
                workspace: "test".into(),
                ..Default::default()
            }),
            status: Some(protocol::SandboxStatus {
                agent_pod: "physical".into(),
                phase: protocol::SandboxPhase::Ready as i32,
                current_policy_version: 1,
                main_process_instance_id: "supervisor".into(),
                configuration_activated: Some(true),
                configuration_admission: Some(protocol::SandboxConfigurationAdmission {
                    instance_id: "supervisor".into(),
                    state: protocol::ConfigurationAdmissionState::Accepted as i32,
                    policy_version: 1,
                    policy_hash: binding.policy_sha256().into(),
                    ..Default::default()
                }),
                conditions: ["Ready", "ConfigurationReady"]
                    .into_iter()
                    .map(|name| protocol::SandboxCondition {
                        r#type: name.into(),
                        status: "True".into(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let revision = protocol::GetSandboxPolicyStatusResponse {
            active_version: 1,
            revision: Some(protocol::SandboxPolicyRevision {
                version: 1,
                policy_hash: binding.policy_sha256().into(),
                status: protocol::PolicyStatus::Loaded as i32,
                ..Default::default()
            }),
        };
        (record, sandbox, revision)
    }

    #[test]
    fn joined_readiness_rejects_stale_instances_policies_and_partial_admission() {
        let (record, good, revision) = fixture();
        validate(&record, &good, &revision).unwrap();
        let admission = good
            .status
            .as_ref()
            .unwrap()
            .configuration_admission
            .as_ref()
            .unwrap();
        let desired = protocol::sandbox::v1::GetSandboxConfigResponse {
            configuration_admitted: true,
            workspace: "test".into(),
            configuration_instance_id: admission.instance_id.clone(),
            version: admission.policy_version,
            policy_hash: admission.policy_hash.clone(),
            ..Default::default()
        };
        validate_configuration(&good, &desired).unwrap();
        for field in 0..8 {
            let mut changed = desired.clone();
            match field {
                0 => changed.configuration_admitted = false,
                1 => changed.configuration_error = "rejected".into(),
                2 => changed.workspace.push('x'),
                3 => changed.configuration_instance_id.push('x'),
                4 => changed.version += 1,
                5 => changed.policy_hash.push('x'),
                6 => changed.config_revision += 1,
                _ => changed.provider_env_revision += 1,
            }
            assert!(
                validate_configuration(&good, &changed).is_err(),
                "configuration case {field}"
            );
        }
        for field in 0..17 {
            let mut sandbox = good.clone();
            let status = sandbox.status.as_mut().unwrap();
            let admission = status.configuration_admission.as_mut().unwrap();
            match field {
                0 => sandbox.metadata.as_mut().unwrap().id.push('x'),
                1 => sandbox.metadata.as_mut().unwrap().name.push('x'),
                2 => sandbox.metadata.as_mut().unwrap().workspace.push('x'),
                3 => sandbox.metadata.as_mut().unwrap().deletion_time = Some(Default::default()),
                4 => status.agent_pod.push('x'),
                5 => status.phase = protocol::SandboxPhase::Provisioning as i32,
                6 => status.conditions[0].status = "False".into(),
                7 => status.conditions[1].status = "False".into(),
                8 => status.configuration_activated = None,
                9 => admission.state = protocol::ConfigurationAdmissionState::Pending as i32,
                10 => admission.instance_id.clear(),
                11 => admission.error = "rejected desired configuration".into(),
                12 => admission.policy_hash.push('x'),
                13 => admission.policy_version += 1,
                14 => status.current_policy_version += 1,
                15 => status.conditions.push(status.conditions[0].clone()),
                _ => status.conditions.push(protocol::SandboxCondition {
                    r#type: "DesiredConfigurationReady".into(),
                    status: "False".into(),
                    ..Default::default()
                }),
            }
            assert!(
                validate(&record, &sandbox, &revision).is_err(),
                "case {field}"
            );
        }
        let mut no_projection = good.clone();
        no_projection.status.as_mut().unwrap().agent_pod.clear();
        no_projection
            .status
            .as_mut()
            .unwrap()
            .main_process_instance_id = "different-session-id".into();
        validate(&record, &no_projection, &revision).unwrap();
        for field in 0..5 {
            let mut changed = revision.clone();
            let latest = changed.revision.as_mut().unwrap();
            match field {
                0 => latest.version += 1,
                1 => latest.policy_hash.push('x'),
                2 => latest.status = protocol::PolicyStatus::Pending as i32,
                3 => latest.load_error = "failed".into(),
                _ => changed.active_version += 1,
            }
            assert!(
                validate(&record, &good, &changed).is_err(),
                "revision case {field}"
            );
        }
    }

    #[test]
    fn gateway_credentials_and_origin_are_operator_bound() {
        let config = OpenShellGatewayConfig {
            endpoint: "https://localhost:8080".into(),
            ca_file: "/tmp/ca".into(),
            cert_file: "/tmp/cert".into(),
            key_file: "/tmp/key".into(),
        };
        config.validate().unwrap();
        for url in [
            "http://localhost:8080",
            "https://user@localhost",
            "https://localhost/path",
            "https://localhost?token=secret",
            "https://localhost/#fragment",
        ] {
            let mut bad = config.clone();
            bad.endpoint = url.into();
            assert!(bad.validate().is_err());
        }
        let dir = tempfile::tempdir().unwrap();
        let key = dir.path().join("key");
        std::fs::write(&key, b"test key bytes").unwrap();
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(certificate(&key, true).is_ok());
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(certificate(&key, true).is_err());
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&key, &link).unwrap();
        assert!(certificate(&link, false).is_err());
    }
}
