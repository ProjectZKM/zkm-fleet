use std::path::{Path, PathBuf};

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct SchedulerConfig {
    /// External stage API (SDK network prover), deprecated for `fleet-proof-app`. TLS when
    /// `tls` is set.
    #[serde(default = "default_stage_addr")]
    pub stage_addr: String,
    /// Cluster-internal control API (node registration and heartbeats).
    #[serde(default = "default_control_addr")]
    pub control_addr: String,
    /// HTTP: public values for the SDK, health, operator status.
    #[serde(default = "default_http_addr")]
    pub http_addr: String,
    /// Base URL clients reach `http_addr` at; public values are served under it.
    pub public_url: String,
    /// Postgres; the in-memory store is used when absent (tests, local jobs).
    #[serde(default)]
    pub database_url: Option<String>,
    /// File holding the Postgres URL (keeps the password out of this file); wins over `database_url`.
    #[serde(default)]
    pub database_url_file: Option<PathBuf>,
    /// ELFs and job inputs; previously stored SDK results remain readable.
    pub data_dir: PathBuf,
    /// Control-host proof output received from nodes; the SDK adapter reads it,
    /// and app deletion removes completed jobs' output directories. Defaults to
    /// data_dir/proofs.
    #[serde(default)]
    pub proof_output_dir: Option<PathBuf>,
    /// How long `GetStatus` holds a request open waiting for the job to finish.
    #[serde(default = "default_long_poll_ms")]
    pub long_poll_ms: u64,
    #[serde(default = "default_heartbeat_secs")]
    pub heartbeat_secs: u32,
    /// A node that has not sent a heartbeat for this long is dropped.
    #[serde(default = "default_node_stale_secs")]
    pub node_stale_secs: u64,
    /// Attempts per job, the first one included; a retry avoids the nodes that failed it.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default)]
    pub tls: Option<TlsConfig>,
    /// TLS for node-initiated control gRPC, independent of gateway TLS termination.
    #[serde(default)]
    pub control_tls: Option<TlsConfig>,
    /// Shared secret authenticating node-initiated control requests. At least 32 characters.
    /// Every other caller has a token from the database (`fleet-scheduler issue-admin` makes
    /// the first administrator's).
    pub cluster_token_file: PathBuf,
}

/// Settings this configuration no longer has, with what replaced them: starting with one is
/// refused rather than silently ignoring it.
const REMOVED: [(&str, &str); 4] = [
    ("tenants", "a proof app's placement policy is set when it is registered (fleetctl app add NAME --elf-id ID --class --anchor --backend)"),
    ("default_tenant", "every proof job comes from a namespace token"),
    ("require_signature", "every proof job is signed by its namespace token's key"),
    ("admin_token_file", "administrator tokens are in the database: fleet-scheduler issue-admin --label NAME --out FILE"),
];

#[derive(Debug, Clone, Deserialize)]
pub struct TlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
}

fn default_stage_addr() -> String {
    "0.0.0.0:51000".into()
}
fn default_control_addr() -> String {
    "0.0.0.0:51001".into()
}
fn default_http_addr() -> String {
    "0.0.0.0:9191".into()
}
fn default_long_poll_ms() -> u64 {
    2_000
}
fn default_heartbeat_secs() -> u32 {
    5
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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn cfg(control: &str, tls: &str) -> SchedulerConfig {
        serde_yaml::from_str(&format!("public_url: http://localhost:9191\ndata_dir: /tmp/fleet\ncluster_token_file: /tmp/cluster\ncontrol_addr: {control}\n{tls}")).unwrap()
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
        let base = "public_url: http://localhost:9191\ndata_dir: /tmp/fleet\ncluster_token_file: /tmp/cluster\ncontrol_addr: 127.0.0.1:51001\n";
        std::fs::write(&path, base).unwrap();
        assert!(SchedulerConfig::load(&path).is_ok());
        std::fs::write(&path, format!("{base}admin_token_file: /tmp/admin\n")).unwrap();
        let e = SchedulerConfig::load(&path).unwrap_err().to_string();
        assert!(e.contains("admin_token_file was removed") && e.contains("fleet-scheduler issue-admin"), "{e}");
    }
}
