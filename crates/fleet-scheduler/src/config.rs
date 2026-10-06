use std::path::{Path, PathBuf};

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// External stage API (SDK network prover). TLS when `tls` is set.
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
    /// Postgres; the in-memory store is used when absent (tests, local runs).
    #[serde(default)]
    pub database_url: Option<String>,
    /// File holding the Postgres URL (keeps the password out of this file); wins over `database_url`.
    #[serde(default)]
    pub database_url_file: Option<PathBuf>,
    /// ELFs, job inputs and results.
    pub data_dir: PathBuf,
    /// How long `GetStatus` holds a request open waiting for the job to finish.
    #[serde(default = "default_long_poll_ms")]
    pub long_poll_ms: u64,
    #[serde(default = "default_heartbeat_secs")]
    pub heartbeat_secs: u32,
    /// A node that has not sent a heartbeat for this long is dropped.
    #[serde(default = "default_node_stale_secs")]
    pub node_stale_secs: u64,
    /// Upper bound on one proof on a node.
    #[serde(default = "default_job_timeout_secs")]
    pub job_timeout_secs: u64,
    /// Attempts per job, the first one included; a retry avoids the nodes that failed it.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    /// Reject requests whose signature does not map to a tenant.
    #[serde(default)]
    pub require_signature: bool,
    /// Tenant for unsigned or unknown requests when `require_signature` is false.
    #[serde(default)]
    pub default_tenant: Option<String>,
    /// Take the caller's identity from the client certificate the gateway verified and
    /// forwarded (`x-forwarded-client-cert`, its CN a credential's address): GetStatus
    /// answers only the tenant that submitted the proof, and a request whose certificate
    /// and signature name different tenants is refused. Only with the gateway's mutual
    /// TLS in front (it strips the header from clients), never with the stage port open.
    #[serde(default)]
    pub client_cert_identity: bool,
    pub tenants: Vec<TenantConfig>,
    #[serde(default)]
    pub tls: Option<TlsConfig>,
    /// Shared secret between the scheduler and its prover nodes, both directions
    /// (node -> control API, scheduler -> node API). At least 32 characters.
    pub cluster_token_file: PathBuf,
    /// Bearer token for the operator HTTP endpoints (`/status`, ELF upload).
    pub admin_token_file: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TenantConfig {
    pub name: String,
    /// Ethereum addresses whose request signatures map to this tenant.
    #[serde(default)]
    pub addresses: Vec<String>,
    #[serde(default)]
    pub class: Class,
    #[serde(default)]
    pub anchor: Anchor,
    /// Only nodes proving with this circuit version serve the tenant.
    #[serde(default)]
    pub zkm_version: Option<String>,
    /// Allowed node backends ("cuda", "cpu", "mock"); empty means any.
    #[serde(default)]
    pub backends: Vec<String>,
    /// Node name prefixes that may run this tenant's jobs, e.g. `[ethproofs-gpu]` for
    /// the replicas of that service; empty means any. A tenant whose proofs must come
    /// from a particular prover configuration (keys, verification mode) pins it here.
    #[serde(default)]
    pub nodes: Vec<String>,
    #[serde(default = "default_max_queued")]
    pub max_queued: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Class {
    /// Dispatched before every batch job.
    Latency,
    #[default]
    Batch,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Anchor {
    /// Any eligible node.
    #[default]
    None,
    /// Keep a program on the node it first ran on while that node is healthy.
    Shared,
    /// As `shared`, and the anchor node serves this tenant only.
    Exclusive,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    /// Require client certificates signed by this CA.
    #[serde(default)]
    pub client_ca_path: Option<PathBuf>,
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
fn default_job_timeout_secs() -> u64 {
    3_600
}
fn default_max_attempts() -> u32 {
    2
}
fn default_max_queued() -> usize {
    64
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)?;
        let mut cfg: Config = serde_yaml::from_str(&text)?;
        if let Some(f) = &cfg.database_url_file {
            cfg.database_url = Some(std::fs::read_to_string(f)?.trim().to_string());
        }
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        let mut seen = std::collections::HashSet::new();
        for t in &self.tenants {
            anyhow::ensure!(
                seen.insert(t.name.as_str()),
                "tenant {} declared twice",
                t.name
            );
        }
        if let Some(d) = &self.default_tenant {
            anyhow::ensure!(
                self.tenant(d).is_some(),
                "default_tenant {d} is not declared"
            );
        }
        anyhow::ensure!(self.max_attempts >= 1, "max_attempts must be at least 1");
        Ok(())
    }

    pub fn tenant(&self, name: &str) -> Option<&TenantConfig> {
        self.tenants.iter().find(|t| t.name == name)
    }

    /// Tenant for a recovered signer address (case-insensitive, with or without 0x).
    pub fn tenant_for_address(&self, address: &str) -> Option<&TenantConfig> {
        let want = normalize_address(address);
        self.tenants
            .iter()
            .find(|t| t.addresses.iter().any(|a| normalize_address(a) == want))
    }
}

fn normalize_address(a: &str) -> String {
    a.trim()
        .trim_start_matches("0x")
        .trim_start_matches("0X")
        .to_ascii_lowercase()
}
