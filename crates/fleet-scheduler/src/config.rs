use std::path::{Path, PathBuf};

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct SchedulerConfig {
    /// Cluster-internal control API (node registration and heartbeats).
    #[serde(default = "default_control_addr")]
    pub control_addr: String,
    /// HTTP: health, the API and the nodes' relays.
    #[serde(default = "default_http_addr")]
    pub http_addr: String,
    /// Postgres; the in-memory store is used when absent (tests, local jobs).
    #[serde(default)]
    pub database_url: Option<String>,
    /// File holding the Postgres URL (keeps the password out of this file); wins over `database_url`.
    #[serde(default)]
    pub database_url_file: Option<PathBuf>,
    /// The apps', batches' and their jobs' records (`workloads/`).
    pub data_dir: PathBuf,
    #[serde(default = "default_heartbeat_secs")]
    pub heartbeat_secs: u32,
    /// A node that has not sent a heartbeat for this long is dropped.
    #[serde(default = "default_node_stale_secs")]
    pub node_stale_secs: u64,
    /// How long a router access policy outlives its app or batch before it is retired. It
    /// leaves time to set a policy before creating the app it is for.
    #[serde(default = "default_router_policy_grace_secs")]
    pub router_policy_grace_secs: u64,
    /// Attempts per job, the first one included; a retry avoids the nodes that failed it.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    /// TLS for node-initiated control gRPC, independent of gateway TLS termination.
    #[serde(default)]
    pub control_tls: Option<TlsConfig>,
    /// Shared secret authenticating node-initiated control requests. At least 32 characters.
    /// Every other caller has a token from the database (`fleet-scheduler issue-admin` makes
    /// the first administrator's).
    pub cluster_token_file: PathBuf,
    /// Optional central policy admission, hosted on this scheduler's HTTP/2
    /// listener. Reuses data_dir for its private durable authorization ledger.
    #[serde(default)]
    pub policy_gateway: Option<PolicyGatewayConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyGatewayConfig {
    /// Approved guest ELF and vk.bytes32() from the same build manifest.
    pub program_elf: PathBuf,
    pub program_sha256: String,
    pub program_vkey: String,
    /// Remote policy-receipt worker. When absent, preflight admission is disabled.
    #[serde(default)]
    pub preflight: Option<PreflightConfig>,
    /// Stock gateway compute extension, embedded in this scheduler process.
    #[serde(default)]
    pub openshell: Option<OpenShellConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenShellConfig {
    /// Same-host private Unix socket selected in the stock gateway config.
    pub compute_socket: PathBuf,
    /// Exact versioned resource-admission acknowledgement of the native driver.
    pub admission_policy: String,
    /// Authenticated stock gateway API used to observe policy readiness.
    /// Omission keeps native service publication disabled.
    #[serde(default)]
    pub gateway: Option<OpenShellGatewayConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenShellGatewayConfig {
    pub endpoint: String,
    pub ca_file: PathBuf,
    pub cert_file: PathBuf,
    pub key_file: PathBuf,
}

impl OpenShellGatewayConfig {
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        let url = reqwest::Url::parse(&self.endpoint)?;
        anyhow::ensure!(url.scheme() == "https" && url.host_str().is_some()
            && url.username().is_empty() && url.password().is_none()
            && url.path() == "/" && url.query().is_none() && url.fragment().is_none()
            && [&self.ca_file, &self.cert_file, &self.key_file].iter().all(|p| p.is_absolute()),
            "OpenShell gateway requires an HTTPS origin and absolute mTLS file paths");
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreflightConfig {
    /// The policy proof app's StageService endpoint, not the Fleet control URL.
    pub endpoint: String,
    pub token_file: PathBuf,
    #[serde(default)]
    pub signer_file: Option<PathBuf>,
}

/// Settings this configuration no longer has, with what replaced them: starting with one is
/// refused rather than silently ignoring it.
const REMOVED: [(&str, &str); 9] = [
    ("tenants", "a proof app's placement policy is set when it is registered (fleetctl app add NAME --elf-id ID --class --anchor --backend)"),
    ("default_tenant", "every proof job comes from a namespace token"),
    ("require_signature", "every proof job is signed by its namespace token's key"),
    ("admin_token_file", "administrator tokens are in the database: fleet-scheduler issue-admin --label NAME --out FILE"),
    ("stage_addr", "the stage API is retired: SDK clients use their app's endpoint (fleet-proof-app)"),
    ("tls", "the stage API is retired; control_tls secures the control API"),
    ("public_url", "the stage API's public values are retired: an app's endpoint serves them"),
    ("proof_output_dir", "proofs stay in app storage"),
    ("long_poll_ms", "the stage API is retired: an app's endpoint holds GetStatus open"),
];

#[derive(Debug, Clone, Deserialize)]
pub struct TlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
}

fn default_control_addr() -> String {
    "0.0.0.0:51001".into()
}
fn default_http_addr() -> String {
    "0.0.0.0:9191".into()
}
fn default_heartbeat_secs() -> u32 {
    5
}
fn default_router_policy_grace_secs() -> u64 {
    600
}
fn default_node_stale_secs() -> u64 {
    20
}
fn default_max_attempts() -> u32 {
    2
}

impl SchedulerConfig {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)?;
        let raw: serde_yaml::Value = serde_yaml::from_str(&text)?;
        for (key, instead) in REMOVED {
            anyhow::ensure!(raw.get(key).is_none(), "{}: {key} was removed: {instead}", path.display());
        }
        let mut cfg: SchedulerConfig = serde_yaml::from_value(raw)?;
        if let Some(f) = &cfg.database_url_file {
            cfg.database_url = Some(std::fs::read_to_string(f)?.trim().to_string());
        }
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        let control: std::net::SocketAddr = self.control_addr.parse()
            .map_err(|e| anyhow::anyhow!("control_addr must be a socket address: {e}"))?;
        anyhow::ensure!(
            self.control_tls.is_some() || (control.ip().is_loopback() && self.database_url.is_none()),
            "control gRPC requires TLS whenever it is network-reachable or backed by production Postgres"
        );
        anyhow::ensure!(self.max_attempts >= 1, "max_attempts must be at least 1");
        if let Some(policy) = &self.policy_gateway {
            anyhow::ensure!(policy.program_elf.is_absolute()
                && policy.program_sha256.len() == 64
                && policy.program_sha256.bytes().all(|b| b.is_ascii_hexdigit())
                && policy.program_vkey.strip_prefix("0x").unwrap_or(&policy.program_vkey).len() == 64
                && hex::decode(policy.program_vkey.strip_prefix("0x").unwrap_or(&policy.program_vkey)).is_ok(),
                "invalid approved policy guest manifest");
            if let Some(p) = &policy.preflight {
                anyhow::ensure!(p.token_file.is_absolute()
                    && p.signer_file.as_ref().is_none_or(|f| f.is_absolute())
                    && trusted_shim::ledger_transport::RemoteLedger::endpoint(&p.endpoint).is_ok(),
                    "invalid preflight proof-network configuration");
            }
            if let Some(o) = &policy.openshell {
                anyhow::ensure!(o.compute_socket.is_absolute() && !o.admission_policy.is_empty()
                    && policy.preflight.is_some(), "OpenShell requires a private socket, admission policy and remote proof network");
                if let Some(g) = &o.gateway { g.validate()?; }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn cfg(control: &str, tls: &str) -> SchedulerConfig {
        serde_yaml::from_str(&format!("data_dir: /tmp/fleet\ncluster_token_file: /tmp/cluster\ncontrol_addr: {control}\n{tls}")).unwrap()
    }
    #[test]
    fn control_listener_requires_tls_outside_loopback() {
        assert!(cfg("127.0.0.1:51001", "").validate().is_ok());
        assert!(cfg("0.0.0.0:51001", "").validate().is_err());
        assert!(cfg("0.0.0.0:51001", "control_tls:\n  cert_path: /tmp/cert\n  key_path: /tmp/key\n").validate().is_ok());
        let mut production = cfg("127.0.0.1:51001", "");
        production.database_url = Some("postgres://localhost/fleet".into());
        assert!(production.validate().is_err());
    }

    #[test]
    fn a_removed_setting_is_refused_with_what_replaced_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fleet.yaml");
        let base = "data_dir: /tmp/fleet\ncluster_token_file: /tmp/cluster\ncontrol_addr: 127.0.0.1:51001\n";
        std::fs::write(&path, base).unwrap();
        assert!(SchedulerConfig::load(&path).is_ok());
        std::fs::write(&path, format!("{base}admin_token_file: /tmp/admin\n")).unwrap();
        let e = SchedulerConfig::load(&path).unwrap_err().to_string();
        assert!(e.contains("admin_token_file was removed") && e.contains("fleet-scheduler issue-admin"), "{e}");
    }

    #[test]
    fn the_example_configuration_loads() {
        let raw: serde_yaml::Value = serde_yaml::from_str(include_str!("../../../deploy/fleet.example.yaml")).unwrap();
        for (key, _) in REMOVED {
            assert!(raw.get(key).is_none(), "the example sets removed {key}");
        }
        serde_yaml::from_value::<SchedulerConfig>(raw).unwrap().validate().unwrap();
    }
}
