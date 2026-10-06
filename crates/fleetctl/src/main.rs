//! fleetctl: operate a zkm-fleet cluster and issue its credentials.
//!
//! Credentials never leave the operator's machine except where they must:
//! tenant signing keys are generated here and only their address is registered,
//! tokens and TLS keys are written to 0600 files.

use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use clap::{Parser, Subcommand};
use serde_json::Value;

#[derive(Parser)]
#[command(about = "Operate a zkm-fleet cluster and issue its credentials")]
struct Cli {
    /// Scheduler HTTP base URL (admin API under /admin).
    #[arg(
        long,
        env = "FLEET_URL",
        default_value = "http://127.0.0.1:9191",
        global = true
    )]
    url: String,
    /// File holding the admin token (default: admin.token in FLEET_ETC, ~/.config/zkm-fleet or /etc/zkm-fleet).
    #[arg(long, env = "FLEET_ADMIN_TOKEN_FILE", global = true)]
    admin_token_file: Option<PathBuf>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Queue, nodes, anchors.
    Status {
        #[arg(long)]
        json: bool,
    },
    /// Tenant signing keys (the SDK's ZKM_PRIVATE_KEY).
    #[command(subcommand)]
    Credential(CredentialCmd),
    /// Random tokens: the cluster token (scheduler <-> nodes) and the admin token.
    #[command(subcommand)]
    Token(TokenCmd),
    /// Certificates for the stage API (server) and its clients (mutual TLS).
    #[command(subcommand)]
    Tls(TlsCmd),
    /// Guest programs.
    #[command(subcommand)]
    Elf(ElfCmd),
    /// One job's record.
    Job { id: String },
    /// Prover nodes.
    #[command(subcommand)]
    Node(NodeCmd),
    /// Proof sequences driven from a cursor (see the scheduler's `streams:`).
    #[command(subcommand)]
    Stream(StreamCmd),
}

#[derive(Subcommand)]
enum StreamCmd {
    /// Every stream: head, cursor, lag, open steps, workers.
    List {
        #[arg(long)]
        json: bool,
    },
    /// Recent steps of a stream, newest first.
    Steps {
        name: String,
        #[arg(long, default_value_t = 30)]
        limit: usize,
        /// From the store (every step ever) rather than the scheduler's memory.
        #[arg(long)]
        all: bool,
    },
    /// Hand out no more work (leases in flight finish).
    Pause { name: String },
    Resume { name: String },
    /// Move the cursor: planning continues from this sequence number.
    Seek { name: String, seq: i64 },
}

#[derive(Subcommand)]
enum CredentialCmd {
    /// Generate a key for a tenant, register its address, write the client env file.
    Issue {
        #[arg(long)]
        tenant: String,
        #[arg(long, default_value = "")]
        label: String,
        /// Env file for the client (0600): ZKM_PROVER, ENDPOINT, ZKM_PRIVATE_KEY, TLS paths.
        #[arg(long)]
        out: PathBuf,
        /// Stage API endpoint clients use, e.g. https://fleet.example:51000.
        #[arg(long)]
        endpoint: String,
        /// Directory of `fleetctl tls init`: issue a client certificate for this credential too.
        /// The SDK only enables TLS when it has a client certificate (mutual TLS), so a
        /// stage API reachable from outside needs this.
        #[arg(long)]
        tls_dir: Option<PathBuf>,
        /// Server name in the stage certificate, when it is not the SDK's default "stage".
        #[arg(long)]
        domain_name: Option<String>,
    },
    List,
    Revoke {
        address: String,
    },
}

#[derive(Subcommand)]
enum TokenCmd {
    /// Write a new random token (48 bytes, hex) to a 0600 file.
    New {
        #[arg(long)]
        out: PathBuf,
    },
}

#[derive(Subcommand)]
enum TlsCmd {
    /// Create a CA and the stage server certificate.
    Init {
        #[arg(long)]
        dir: PathBuf,
        /// Names the server certificate covers. The SDK verifies DOMAIN_NAME, "stage" by default.
        #[arg(long, value_delimiter = ',', default_value = "stage")]
        names: Vec<String>,
    },
    /// Issue a client certificate signed by the CA in `dir`.
    Client {
        #[arg(long)]
        dir: PathBuf,
        #[arg(long)]
        name: String,
    },
}

#[derive(Subcommand)]
enum ElfCmd {
    /// Upload an ELF; requests may then carry only its elf_id.
    Put { file: PathBuf },
}

#[derive(Subcommand)]
enum NodeCmd {
    /// Stop placing new jobs on a node (it finishes the current one).
    Drain {
        name: String,
    },
    Undrain {
        name: String,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match &cli.cmd {
        Cmd::Token(TokenCmd::New { out }) => {
            let mut bytes = [0u8; 48];
            rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut bytes);
            write_secret(out, format!("{}\n", hex::encode(bytes)).as_bytes())?;
            println!("wrote {}", out.display());
        }
        Cmd::Tls(TlsCmd::Init { dir, names }) => tls_init(dir, names)?,
        Cmd::Tls(TlsCmd::Client { dir, name }) => {
            let (c, k) = tls_client(dir, name)?;
            println!(
                "client certificate: SSL_CERT_PATH={} SSL_KEY_PATH={}",
                c.display(),
                k.display()
            );
        }
        _ => admin(&cli)?,
    }
    Ok(())
}

struct Admin {
    url: String,
    token: String,
    http: reqwest::blocking::Client,
}

impl Admin {
    fn new(cli: &Cli) -> anyhow::Result<Self> {
        let path = match &cli.admin_token_file {
            Some(p) => p.clone(),
            None => default_admin_token()
                .context("no admin token: pass --admin-token-file or set FLEET_ADMIN_TOKEN_FILE")?,
        };
        let token = std::fs::read_to_string(&path)
            .with_context(|| format!("reading {}", path.display()))?
            .trim()
            .to_string();
        Ok(Self {
            url: cli.url.trim_end_matches('/').to_string(),
            token,
            http: reqwest::blocking::Client::new(),
        })
    }

    fn req(&self, method: reqwest::Method, path: &str) -> reqwest::blocking::RequestBuilder {
        self.http
            .request(method, format!("{}/admin{path}", self.url))
            .bearer_auth(&self.token)
    }

    fn send(&self, rb: reqwest::blocking::RequestBuilder) -> anyhow::Result<Option<Value>> {
        let r = rb.send()?;
        let status = r.status();
        let body = r.text()?;
        if !status.is_success() {
            bail!("{status}: {body}");
        }
        Ok(if body.is_empty() {
            None
        } else {
            Some(serde_json::from_str(&body).unwrap_or(Value::String(body)))
        })
    }
}

fn admin(cli: &Cli) -> anyhow::Result<()> {
    let a = Admin::new(cli)?;
    use reqwest::Method as M;
    match &cli.cmd {
        Cmd::Status { json } => {
            let v = a.send(a.req(M::GET, "/status"))?.unwrap_or_default();
            if *json {
                println!("{}", serde_json::to_string_pretty(&v)?);
            } else {
                print_status(&v);
            }
        }
        Cmd::Credential(CredentialCmd::Issue {
            tenant,
            label,
            out,
            endpoint,
            tls_dir,
            domain_name,
        }) => {
            if out.exists() {
                bail!(
                    "{} exists; refusing to overwrite a credential",
                    out.display()
                );
            }
            let key = k256::ecdsa::SigningKey::random(&mut rand::rngs::OsRng);
            let address = alloy_primitives::Address::from_private_key(&key);
            let v = a.send(a.req(M::POST, "/credentials").json(&serde_json::json!({
                "tenant": tenant, "address": format!("{address:#x}"), "label": label,
            })))?;
            let mut env = String::new();
            env += "ZKM_PROVER=network\n";
            env += &format!("ENDPOINT={endpoint}\n");
            env += &format!("ZKM_PRIVATE_KEY={}\n", hex::encode(key.to_bytes()));
            env += "ZKM_PROOF_POLL_INTERVAL=100\n";
            if let Some(dir) = tls_dir {
                let name = format!("{tenant}-{}", &format!("{address:x}")[..8]);
                let (cert, key_path) = tls_client(dir, &name)?;
                env += &format!("CA_CERT_PATH={}\n", abs(&dir.join("ca.pem"))?);
                env += &format!(
                    "SSL_CERT_PATH={}\nSSL_KEY_PATH={}\n",
                    abs(&cert)?,
                    abs(&key_path)?
                );
                if let Some(d) = domain_name {
                    env += &format!("DOMAIN_NAME={d}\n");
                }
            } else if endpoint.starts_with("https") {
                bail!("an https endpoint needs --tls-dir: the SDK only speaks TLS with a client certificate");
            }
            write_secret(out, env.as_bytes())?;
            println!(
                "issued {address:#x} for tenant {tenant}; client env in {} (0600)",
                out.display()
            );
            if let Some(v) = v {
                println!("{}", serde_json::to_string_pretty(&v)?);
            }
        }
        Cmd::Credential(CredentialCmd::List) => {
            let v = a.send(a.req(M::GET, "/credentials"))?.unwrap_or_default();
            println!(
                "{:<44} {:<14} {:<20} {}",
                "ADDRESS", "TENANT", "LABEL", "STATE"
            );
            for c in v.as_array().into_iter().flatten() {
                let state = if c["revoked_ms"].is_null() {
                    "active"
                } else {
                    "revoked"
                };
                println!(
                    "0x{:<42} {:<14} {:<20} {}",
                    c["address"].as_str().unwrap_or(""),
                    c["tenant"].as_str().unwrap_or(""),
                    c["label"].as_str().unwrap_or(""),
                    state
                );
            }
        }
        Cmd::Credential(CredentialCmd::Revoke { address }) => {
            a.send(a.req(
                M::DELETE,
                &format!("/credentials/{}", address.trim_start_matches("0x")),
            ))?;
            println!("revoked {address}");
        }
        Cmd::Elf(ElfCmd::Put { file }) => {
            let bytes = std::fs::read(file)?;
            let v = a.send(a.req(M::POST, "/elfs").body(bytes))?;
            println!("{}", serde_json::to_string_pretty(&v.unwrap_or_default())?);
        }
        Cmd::Job { id } => {
            let v = a.send(a.req(M::GET, &format!("/jobs/{id}")))?;
            println!("{}", serde_json::to_string_pretty(&v.unwrap_or_default())?);
        }
        Cmd::Node(NodeCmd::Drain { name }) => {
            a.send(a.req(M::POST, &format!("/nodes/{name}/drain")))?;
            println!("draining {name}");
        }
        Cmd::Node(NodeCmd::Undrain { name }) => {
            a.send(a.req(M::DELETE, &format!("/nodes/{name}/drain")))?;
            println!("{name} takes jobs again");
        }
        Cmd::Stream(StreamCmd::List { json }) => {
            let v = a.send(a.req(M::GET, "/streams"))?.unwrap_or_default();
            if *json {
                println!("{}", serde_json::to_string_pretty(&v)?);
            } else {
                print_streams(&v);
            }
        }
        Cmd::Stream(StreamCmd::Steps { name, limit, all }) => {
            let v = a
                .send(a.req(M::GET, &format!("/streams/{name}/steps?limit={limit}&all={all}")))?
                .unwrap_or_default();
            println!(
                "{:>12} {:<9} {:>3} {:>8} {:<22} {:<40}",
                "SEQ", "STATE", "TRY", "WALL S", "WORKER", "ERROR / ARTIFACT"
            );
            for s in v.as_array().into_iter().flatten() {
                let wall = s["wall_ms"].as_u64().map(|w| format!("{:.1}", w as f64 / 1000.0)).unwrap_or_default();
                let note = if s["error"].as_str().unwrap_or("").is_empty() {
                    s["artifact"].as_str().unwrap_or("").to_string()
                } else {
                    s["error"].as_str().unwrap_or("").chars().take(40).collect()
                };
                println!(
                    "{:>12} {:<9} {:>3} {:>8} {:<22} {:<40}",
                    s["seq"],
                    s["state"].as_str().unwrap_or(""),
                    s["attempts"],
                    wall,
                    s["worker"].as_str().unwrap_or("-"),
                    note,
                );
            }
        }
        Cmd::Stream(StreamCmd::Pause { name }) => {
            a.send(a.req(M::POST, &format!("/streams/{name}/pause")))?;
            println!("{name} paused: no new work is handed out");
        }
        Cmd::Stream(StreamCmd::Resume { name }) => {
            a.send(a.req(M::DELETE, &format!("/streams/{name}/pause")))?;
            println!("{name} resumed");
        }
        Cmd::Stream(StreamCmd::Seek { name, seq }) => {
            a.send(a.req(M::POST, &format!("/streams/{name}/seek")).json(&serde_json::json!({ "seq": seq })))?;
            println!("{name} continues from {seq}");
        }
        Cmd::Token(_) | Cmd::Tls(_) => unreachable!(),
    }
    Ok(())
}

/// admin.token in $FLEET_ETC, then ~/.config/zkm-fleet (written by install.sh without root), then /etc/zkm-fleet.
fn default_admin_token() -> Option<PathBuf> {
    let mut dirs: Vec<PathBuf> = Vec::new();
    if let Ok(e) = std::env::var("FLEET_ETC") {
        dirs.push(e.into());
    }
    if let Ok(h) = std::env::var("HOME") {
        dirs.push(PathBuf::from(h).join(".config/zkm-fleet"));
    }
    dirs.push("/etc/zkm-fleet".into());
    dirs.into_iter()
        .map(|d| d.join("admin.token"))
        .find(|p| p.exists())
}

fn print_streams(v: &Value) {
    println!(
        "{:<16} {:<7} {:>12} {:>6} {:>12} {:>12} {:>6} {:>7} {:>7} {:>7} {:<24}",
        "STREAM", "MODE", "HEAD", "AGE S", "NEXT", "LAST DONE", "LAG", "DONE", "FAILED", "SKIPPED", "OPEN"
    );
    for s in v.as_array().into_iter().flatten() {
        let open = s["open"]
            .as_object()
            .map(|o| o.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join(" "))
            .unwrap_or_default();
        let mode = if s["paused"].as_bool() == Some(true) {
            "paused"
        } else if s["chained"].as_bool() == Some(true) {
            "chained"
        } else {
            "indep"
        };
        println!(
            "{:<16} {:<7} {:>12} {:>6} {:>12} {:>12} {:>6} {:>7} {:>7} {:>7} {:<24}",
            s["name"].as_str().unwrap_or(""),
            mode,
            s["head"],
            s["head_age_s"],
            s["next_seq"],
            s["last_done"],
            s["lag"],
            s["done"],
            s["failed"],
            s["skipped"],
            open,
        );
        for w in s["workers"].as_array().into_iter().flatten() {
            println!("    worker {} seen {} s ago", w["name"].as_str().unwrap_or(""), w["seen_age_s"]);
        }
    }
}

fn print_status(v: &Value) {
    println!("queued {}  running {}", v["queued"], v["running"]);
    println!(
        "{:<24} {:<6} {:>4} {:<16} {:<6} {:<10} {:<36}",
        "NODE", "BACKEND", "GPUS", "MACHINE", "READY", "RESERVED", "BUSY WITH"
    );
    for n in v["nodes"].as_array().into_iter().flatten() {
        let ready = if n["draining"].as_bool() == Some(true) {
            "drain"
        } else if n["ready"].as_bool() == Some(true) {
            "yes"
        } else {
            "no"
        };
        println!(
            "{:<24} {:<6} {:>4} {:<16} {:<6} {:<10} {:<36}",
            n["name"].as_str().unwrap_or(""),
            n["backend"].as_str().unwrap_or(""),
            n["gpus"],
            n["machine"].as_str().unwrap_or(""),
            ready,
            n["reserved_for"].as_str().unwrap_or("-"),
            n["busy"].as_str().unwrap_or("-"),
        );
    }
    let anchors = v["anchors"].as_array().cloned().unwrap_or_default();
    if !anchors.is_empty() {
        println!("anchors:");
        for a in anchors {
            let elf = a[1].as_str().unwrap_or("");
            println!(
                "  {} / {}… -> {}",
                a[0].as_str().unwrap_or(""),
                &elf[..12.min(elf.len())],
                a[2].as_str().unwrap_or("")
            );
        }
    }
}

fn abs(p: &Path) -> anyhow::Result<String> {
    Ok(std::fs::canonicalize(p)
        .with_context(|| format!("{} not found", p.display()))?
        .display()
        .to_string())
}

fn write_secret(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("creating {}", path.display()))?;
    f.write_all(bytes)?;
    Ok(())
}

fn write_public(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    if path.exists() {
        bail!("{} exists; refusing to overwrite", path.display());
    }
    std::fs::write(path, bytes)?;
    Ok(())
}

fn validity(params: &mut rcgen::CertificateParams, days: i64) {
    let now = time::OffsetDateTime::now_utc();
    params.not_before = now - time::Duration::days(1);
    params.not_after = now + time::Duration::days(days);
}

fn tls_init(dir: &Path, names: &[String]) -> anyhow::Result<()> {
    std::fs::create_dir_all(dir)?;
    let ca_key = rcgen::KeyPair::generate()?;
    let mut ca = rcgen::CertificateParams::new(Vec::<String>::new())?;
    ca.distinguished_name
        .push(rcgen::DnType::CommonName, "zkm-fleet CA");
    ca.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca.key_usages = vec![
        rcgen::KeyUsagePurpose::KeyCertSign,
        rcgen::KeyUsagePurpose::CrlSign,
    ];
    validity(&mut ca, 3650);
    let ca_cert = ca.self_signed(&ca_key)?;

    let srv_key = rcgen::KeyPair::generate()?;
    let mut srv = rcgen::CertificateParams::new(names.to_vec())?;
    srv.distinguished_name.push(
        rcgen::DnType::CommonName,
        names.first().cloned().unwrap_or_else(|| "stage".into()),
    );
    srv.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
    validity(&mut srv, 825);
    let srv_cert = srv.signed_by(&srv_key, &ca_cert, &ca_key)?;

    write_public(&dir.join("ca.pem"), ca_cert.pem().as_bytes())?;
    write_secret(&dir.join("ca.key"), ca_key.serialize_pem().as_bytes())?;
    write_public(&dir.join("server.pem"), srv_cert.pem().as_bytes())?;
    write_secret(&dir.join("server.key"), srv_key.serialize_pem().as_bytes())?;
    println!(
        "CA and server certificate for {:?} in {}",
        names,
        dir.display()
    );
    println!("scheduler: tls.cert_path={0}/server.pem tls.key_path={0}/server.key tls.client_ca_path={0}/ca.pem", dir.display());
    println!(
        "clients:   CA_CERT_PATH={}/ca.pem (and DOMAIN_NAME={} if not \"stage\")",
        dir.display(),
        names[0]
    );
    Ok(())
}

fn tls_client(dir: &Path, name: &str) -> anyhow::Result<(PathBuf, PathBuf)> {
    let ca_key = rcgen::KeyPair::from_pem(&std::fs::read_to_string(dir.join("ca.key"))?)?;
    let ca_params =
        rcgen::CertificateParams::from_ca_cert_pem(&std::fs::read_to_string(dir.join("ca.pem"))?)?;
    let ca_cert = ca_params.self_signed(&ca_key)?;
    let key = rcgen::KeyPair::generate()?;
    let mut p = rcgen::CertificateParams::new(Vec::<String>::new())?;
    p.distinguished_name.push(rcgen::DnType::CommonName, name);
    p.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
    validity(&mut p, 825);
    let cert = p.signed_by(&key, &ca_cert, &ca_key)?;
    let (c, k) = (
        dir.join(format!("client-{name}.pem")),
        dir.join(format!("client-{name}.key")),
    );
    write_public(&c, cert.pem().as_bytes())?;
    write_secret(&k, key.serialize_pem().as_bytes())?;
    Ok((c, k))
}
