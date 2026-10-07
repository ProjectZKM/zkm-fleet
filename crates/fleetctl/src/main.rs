//! fleetctl: operate a zkm-fleet cluster and issue its credentials.
//!
//! Credentials never leave the operator's machine except where they must:
//! tenant signing keys are generated here and only their address is registered,
//! tokens and TLS keys are written to 0600 files.
//!
//! Every command prints a formatted view, or with `--json` the data as JSON.

mod out;

use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context};
use clap::{Args, Parser, Subcommand};
use serde::Serialize;
use serde_json::{json, Value};

use out::{col, Out};

#[derive(Parser)]
#[command(about = "Operate a zkm-fleet cluster and issue its credentials")]
struct Cli {
    /// Print the result as JSON instead of the formatted view.
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    cmd: Cmd,
}

/// Where the scheduler's admin API and the runs API are, and the token both take: every
/// command that talks to the cluster has these (the `init` ones work offline and do not).
#[derive(Args, Clone)]
struct Conn {
    /// Scheduler HTTP base URL (admin API under /admin).
    #[arg(long, env = "FLEET_URL", default_value = "http://127.0.0.1:9191", global = true)]
    url: String,
    /// Runs API base URL (the bridge's `kind: runs-api` service on the control machine).
    #[arg(long, env = "FLEET_RUNS_URL", default_value = "http://127.0.0.1:9192", global = true)]
    runs_url: String,
    /// File holding the admin token (default: admin.token in FLEET_ETC, ~/.config/zkm-fleet or /etc/zkm-fleet).
    #[arg(long, env = "FLEET_ADMIN_TOKEN_FILE", global = true)]
    admin_token_file: Option<PathBuf>,
}

#[derive(Subcommand)]
enum Cmd {
    /// Apps: proof programs (a tenant's ELF) and run profiles (runs.yaml), with their jobs.
    App(AppArgs),
    /// Jobs, the instances of apps (proof jobs and runs): the list (by default), one by
    /// its id, or submit / logs / cancel / purge / fetch a run.
    Job(JobArgs),
    /// Prover nodes: the list (by default), or drain / undrain one.
    Node(NodeArgs),
    /// Tenant signing keys (the SDK's ZKM_PRIVATE_KEY): the list (by default), one by its
    /// address, or issue / revoke one.
    Credential(CredentialArgs),
    /// Set up a cluster's secrets, offline: the cluster and admin tokens, the stage API's
    /// certificates (install.sh runs these; no scheduler is needed).
    #[command(subcommand)]
    Init(InitCmd),
}

#[derive(Args)]
#[command(args_conflicts_with_subcommands = true)]
struct AppArgs {
    #[command(subcommand)]
    cmd: Option<AppCmd>,
    /// An app's name: its record and newest jobs. Without one, the list.
    name: Option<String>,
    /// Only proof apps, or only run apps.
    #[arg(long = "type", value_enum)]
    kind: Option<Kind>,
    #[command(flatten)]
    conn: Conn,
}

#[derive(Subcommand)]
enum AppCmd {
    /// Add an app. A proof app: `tenant/<elf id>` (or `tenant/ --elf FILE`, hashed here);
    /// its jobs are accepted from then on, the ELF itself arrives with the first one. A
    /// run app: a name with --image and --allow, a profile in the server's runs.yaml.
    Add {
        #[command(flatten)]
        add: AddApp,
    },
    /// The app takes no new jobs; the queued and running ones finish. Its history stays.
    Disable { name: String },
    /// The app takes jobs again.
    Enable { name: String },
    /// Delete an app, refused while any of its jobs is queued or running. A proof app
    /// (tenant/<elf>) goes with its jobs' records, inputs and proofs, its anchor, and its
    /// ELF when no other tenant uses it; a run app's profile leaves runs.yaml and its past
    /// runs stay. Without --yes it only says what it would delete.
    Delete {
        /// The app, as `fleetctl app` names it (a proof app's full elf id works too).
        name: String,
        #[arg(long)]
        yes: bool,
    },
}

/// Which jobs a list shows: every one still queued or running, and the newest finished.
#[derive(Args)]
struct ListArgs {
    /// How many of the newest of each type (proof jobs, runs); the active ones are always shown.
    #[arg(short = 'n', long, default_value_t = 10)]
    limit: usize,
    /// Only proof jobs, or only runs.
    #[arg(long = "type", value_enum)]
    kind: Option<Kind>,
    /// Only what is queued or running.
    #[arg(short, long)]
    active: bool,
    /// Only this tenant's proof jobs (runs belong to no tenant).
    #[arg(long)]
    tenant: Option<String>,
    /// Only the jobs of this app (as `fleetctl app` names it: tenant/<elf> or a run profile).
    #[arg(long)]
    app: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum Kind {
    Proof,
    Run,
}

#[derive(Args)]
#[command(args_conflicts_with_subcommands = true)]
struct JobArgs {
    #[command(subcommand)]
    cmd: Option<JobCmd>,
    /// A proof job's or a run's id: its record. Without one, the list.
    id: Option<String>,
    #[command(flatten)]
    list: ListArgs,
    #[command(flatten)]
    conn: Conn,
}

#[derive(Args)]
#[command(args_conflicts_with_subcommands = true)]
struct NodeArgs {
    #[command(subcommand)]
    cmd: Option<NodeCmd>,
    /// A node's name: its record and the apps anchored on it. Without one, the list.
    name: Option<String>,
    #[command(flatten)]
    conn: Conn,
}

#[derive(Args)]
#[command(args_conflicts_with_subcommands = true)]
struct CredentialArgs {
    #[command(subcommand)]
    cmd: Option<CredentialCmd>,
    /// A credential's address: its record. Without one, the list.
    address: Option<String>,
    #[command(flatten)]
    conn: Conn,
}

#[derive(Subcommand)]
enum NodeCmd {
    /// Every prover node: backend, cards, machine, ready, reserved for, busy with (the default).
    List,
    /// Stop placing new jobs on a node (it finishes the current one).
    Drain { name: String },
    /// Let a drained node take jobs again.
    Undrain { name: String },
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
        /// Directory of `fleetctl init tls`: issue a client certificate for this credential too.
        /// The SDK only enables TLS when it has a client certificate (mutual TLS), so a
        /// stage API reachable from outside needs this.
        #[arg(long)]
        tls_dir: Option<PathBuf>,
        /// Server name in the stage certificate, when it is not the SDK's default "stage".
        #[arg(long)]
        domain_name: Option<String>,
    },
    /// Every registered key: address, tenant, label, active or revoked (the default).
    List,
    /// Refuse a key's requests from now on.
    Revoke { address: String },
}

/// A submit: the command line and the request body are this one struct (its field
/// names are the API's), so nothing is assigned by hand. Unset flags are left out and
/// take the server's defaults.
#[derive(Args, Serialize)]
struct Submit {
    /// The run app: a profile in the server's runs.yaml (image, user, GPU cap, allowed mount roots).
    #[arg(long = "app", alias = "profile")]
    profile: String,
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    /// Cards; a fraction (0.5) shares one card and needs --gpu-mem.
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    gpus: Option<f64>,
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    cpus: Option<u32>,
    /// GiB free each granted card must have before the job starts.
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    gpu_mem: Option<u32>,
    /// Seconds to wait for that memory before failing.
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    vram_wait: Option<u32>,
    /// Spread the run over N machines (one container per machine).
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    nodes: Option<u32>,
    /// The machine the run must land on; with --nodes N, repeat for each.
    #[arg(long)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    machine: Vec<String>,
    /// Run only after this run succeeded.
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    after: Option<String>,
    /// Try a failed run again up to N times.
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    retry: Option<u32>,
    /// Try again up to N times when an attempt ran out of memory.
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    restart_on_oom: Option<u32>,
    /// git:/repo@ref (a repo on the server's machine) or a tar.gz: a local file is uploaded first.
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<String>,
    /// PATH[:ro|:rw] on the server's machine, mounted at its own path; repeat.
    #[arg(long)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    mount: Vec<String>,
    /// K=V for the job ({out} / {code} expand); repeat.
    #[arg(long)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    env: Vec<String>,
    /// File the job's output is appended to (default: the run's own log).
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    log: Option<String>,
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    workdir: Option<String>,
    /// The command, after `--`.
    #[arg(last = true, required = true)]
    command: Vec<String>,
}

/// An app to add. For a run app the command line and the request body are this one
/// struct (its field names are the profile's); unset flags take the server's defaults.
#[derive(Args, Serialize)]
struct AddApp {
    /// A proof app: TENANT/<elf id> (64 hex), or TENANT/ with --elf. A run app: its name
    /// (letters, digits, _ . -; it heads its runs' ids).
    name: String,
    /// A proof app: the ELF file, hashed here (sha256) to its id.
    #[arg(long)]
    #[serde(skip)]
    elf: Option<PathBuf>,
    /// A run app: the container image its runs get.
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    image: Option<String>,
    /// UID[:GID] the container runs as (default: the image's).
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    user: Option<String>,
    /// Cards a run may ask for (default 8).
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    max_gpus: Option<u32>,
    /// GiB a run needs free on its card unless it says --gpu-mem (default: the whole card).
    #[arg(long = "gpu-mem")]
    #[serde(rename = "default_gpu_mem_gb", skip_serializing_if = "Option::is_none")]
    gpu_mem: Option<u32>,
    /// /dev/shm size in GiB (default 16).
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    shm_gb: Option<u32>,
    /// A run app: a root (an absolute path on the server) a run may mount under; repeat. Nothing else can be mounted.
    #[arg(long)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    allow: Vec<String>,
    /// K=V every run gets; repeat.
    #[arg(long)]
    #[serde(skip)]
    env: Vec<String>,
}

/// What can be done to a run (a proof job is the scheduler's and its client's).
#[derive(Subcommand)]
enum JobCmd {
    /// Queue a run: fleetctl job submit --app film-gpu --gpus 1 --mount /data:ro -- python3 train.py
    Submit {
        #[command(flatten)]
        submit: Submit,
    },
    /// A run's output: the last lines, then with -f everything new until it ends (for a
    /// proof job, which has no output of its own, its record).
    Logs {
        id: String,
        #[arg(short, long)]
        follow: bool,
        #[arg(short = 'n', long, default_value_t = 50)]
        lines: usize,
        /// A --nodes run's rank k.
        #[arg(long, default_value_t = 0)]
        rank: usize,
    },
    /// Stop a run (SIGTERM, SIGKILL its app's stop_timeout_s later; its cards freed).
    Cancel { id: String },
    /// Stop a run as a service needing its cards would: it queues again (same id and
    /// out/, FLEET_ATTEMPT counting on) and spends no --retry.
    Preempt { id: String },
    /// Delete a finished run's record (out/ included), or every finished run older than --older-than days.
    Purge {
        id: Option<String>,
        #[arg(long)]
        older_than: Option<f64>,
    },
    /// The run's out/ as a tar.gz (to --out, else stdout).
    Fetch {
        id: String,
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum InitCmd {
    /// Write a new random token (48 bytes, hex) to a 0600 file: the cluster token
    /// (scheduler <-> nodes) or the admin token.
    Token {
        #[arg(long)]
        out: PathBuf,
    },
    /// Create a CA and the stage API's server certificate.
    Tls {
        #[arg(long)]
        dir: PathBuf,
        /// Names the server certificate covers. The SDK verifies DOMAIN_NAME, "stage" by default.
        #[arg(long, value_delimiter = ',', default_value = "stage")]
        names: Vec<String>,
    },
    /// Issue a client certificate signed by the CA in `dir`. The stage API takes a client's
    /// identity from its certificate's CN, so a certificate for a client is issued with its
    /// credential (`credential issue --tls-dir`); this one is for other uses.
    TlsClient {
        #[arg(long)]
        dir: PathBuf,
        #[arg(long)]
        name: String,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let out = Out { json: cli.json };
    match &cli.cmd {
        Cmd::Init(i) => init(i, &out),
        Cmd::Job(j) => job(j, &out),
        Cmd::App(a) => app(a, &out),
        other => admin(other, &out),
    }
}

impl Cmd {
    /// The connection of a command that talks to the scheduler.
    fn conn(&self) -> &Conn {
        match self {
            Cmd::Node(n) => &n.conn,
            Cmd::Credential(c) => &c.conn,
            Cmd::Init(_) | Cmd::Job(_) | Cmd::App(_) => unreachable!("not the scheduler's admin API alone"),
        }
    }
}

struct Admin {
    /// Base URL every path is appended to (the scheduler's `/admin`, or the runs API's root).
    base: String,
    token: String,
    http: reqwest::blocking::Client,
}

impl Admin {
    fn new(conn: &Conn) -> anyhow::Result<Self> {
        Self::at(&format!("{}/admin", conn.url.trim_end_matches('/')), &conn.admin_token_file)
    }

    fn runs(conn: &Conn) -> anyhow::Result<Self> {
        Self::at(conn.runs_url.trim_end_matches('/'), &conn.admin_token_file)
    }

    fn at(base: &str, token_file: &Option<PathBuf>) -> anyhow::Result<Self> {
        let path = match token_file {
            Some(p) => p.clone(),
            None => default_admin_token()
                .context("no admin token: pass --admin-token-file or set FLEET_ADMIN_TOKEN_FILE")?,
        };
        let token = std::fs::read_to_string(&path)
            .with_context(|| format!("reading {}", path.display()))?
            .trim()
            .to_string();
        Ok(Self {
            base: base.to_string(),
            token,
            http: reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(60))
                .build()?,
        })
    }

    fn req(&self, method: reqwest::Method, path: &str) -> reqwest::blocking::RequestBuilder {
        self.http
            .request(method, format!("{}{path}", self.base))
            .bearer_auth(&self.token)
    }

    /// The answer as JSON (Null for an empty one); a failure status is an error with its body.
    fn send(&self, rb: reqwest::blocking::RequestBuilder) -> anyhow::Result<Value> {
        let r = rb.send()?;
        let status = r.status();
        let body = r.text()?;
        if !status.is_success() {
            bail!("{status}: {body}");
        }
        Ok(if body.is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&body).unwrap_or(Value::String(body))
        })
    }

    fn get(&self, path: &str) -> anyhow::Result<Value> {
        self.send(self.req(reqwest::Method::GET, path))
    }

    fn get_q(&self, path: &str, query: &[(&str, String)]) -> anyhow::Result<Value> {
        self.send(self.req(reqwest::Method::GET, path).query(query))
    }
}

fn admin(cmd: &Cmd, out: &Out) -> anyhow::Result<()> {
    let a = Admin::new(cmd.conn())?;
    use reqwest::Method as M;
    match cmd {
        Cmd::Node(NodeArgs { cmd: None, name: Some(name), .. }) => {
            let v = a.get("/status")?;
            let Some(mut n) = node_rows(&v).into_iter().find(|n| n["name"].as_str() == Some(name.as_str())) else {
                bail!("no node {name} (fleetctl node lists them)");
            };
            let anchored: Vec<Value> = v["anchors"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|x| x[2].as_str() == Some(name.as_str()))
                .map(|x| proof_app(&x[0], &x[1]))
                .collect();
            n["anchored_apps"] = Value::Array(anchored);
            n.as_object_mut().map(|o| o.remove("state")); // the list's READY column; the record has ready / draining
            out.record(&n);
        }
        Cmd::Node(NodeArgs { cmd: None | Some(NodeCmd::List), .. }) => {
            let v = a.get("/status")?;
            out.table(&node_rows(&v), NODE_COLS, Some(&v["nodes"]));
        }
        Cmd::Node(NodeArgs { cmd: Some(NodeCmd::Drain { name }), .. }) => {
            a.send(a.req(M::POST, &format!("/nodes/{name}/drain")))?;
            out.done(&format!("draining {name}: it takes no new job and finishes the one it has"),
                     json!({ "node": name, "draining": true }));
        }
        Cmd::Node(NodeArgs { cmd: Some(NodeCmd::Undrain { name }), .. }) => {
            a.send(a.req(M::DELETE, &format!("/nodes/{name}/drain")))?;
            out.done(&format!("{name} takes jobs again"), json!({ "node": name, "draining": false }));
        }
        Cmd::Credential(CredentialArgs { cmd: Some(CredentialCmd::Issue { tenant, label, out: env_file, endpoint, tls_dir, domain_name }), .. }) => {
            if env_file.exists() {
                bail!("{} exists; refusing to overwrite a credential", env_file.display());
            }
            if tls_dir.is_none() && endpoint.starts_with("https") {
                bail!("an https endpoint needs --tls-dir: the SDK only speaks TLS with a client certificate");
            }
            let key = k256::ecdsa::SigningKey::random(&mut rand::rngs::OsRng);
            let address = alloy_primitives::Address::from_private_key(&key);
            let registered = a.send(a.req(M::POST, "/credentials").json(&json!({
                "tenant": tenant, "address": format!("{address:#x}"), "label": label,
            })))?;
            let mut env = vec![
                ("ZKM_PROVER", "network".to_string()),
                ("ENDPOINT", endpoint.clone()),
                ("ZKM_PRIVATE_KEY", hex::encode(key.to_bytes())),
                ("ZKM_PROOF_POLL_INTERVAL", "100".to_string()),
            ];
            if let Some(dir) = tls_dir {
                // its CN is the credential: the scheduler maps it to the tenant (and a
                // revoked credential's certificate is refused with it)
                let (cert, key_path) = tls_client(dir, &format!("{address:#x}"))?;
                env.push(("CA_CERT_PATH", abs(&dir.join("ca.pem"))?));
                env.push(("SSL_CERT_PATH", abs(&cert)?));
                env.push(("SSL_KEY_PATH", abs(&key_path)?));
                if let Some(d) = domain_name {
                    env.push(("DOMAIN_NAME", d.clone()));
                }
            }
            let text: String = env.iter().map(|(k, v)| format!("{k}={v}\n")).collect();
            write_secret(env_file, text.as_bytes())?;
            let result = json!({
                "address": format!("{address:#x}"), "tenant": tenant, "label": label,
                "env_file": env_file.display().to_string(), "endpoint": endpoint,
                "registered": registered,
            });
            if out.json {
                out.record(&result);
            } else {
                println!("issued {address:#x} for tenant {tenant}; client env in {} (0600)", env_file.display());
            }
        }
        Cmd::Credential(CredentialArgs { cmd: None, address: Some(address), .. }) => {
            let want = format!("0x{}", address.trim_start_matches("0x").to_ascii_lowercase());
            let v = a.get("/credentials")?;
            let Some(c) = credential_rows(&v).into_iter().find(|c| c["address"].as_str() == Some(want.as_str())) else {
                bail!("no credential {address} (fleetctl credential lists them)");
            };
            out.record(&c);
        }
        Cmd::Credential(CredentialArgs { cmd: None | Some(CredentialCmd::List), .. }) => {
            let v = a.get("/credentials")?;
            out.table(&credential_rows(&v), CREDENTIAL_COLS, Some(&v));
        }
        Cmd::Credential(CredentialArgs { cmd: Some(CredentialCmd::Revoke { address }), .. }) => {
            a.send(a.req(M::DELETE, &format!("/credentials/{}", address.trim_start_matches("0x"))))?;
            out.done(&format!("revoked {address}"), json!({ "revoked": address }));
        }
        Cmd::Init(_) | Cmd::Job(_) | Cmd::App(_) => unreachable!("not the scheduler's admin API alone"),
    }
    Ok(())
}

const NODE_COLS: &[out::Col] = &[
    col("NODE", "name"),
    col("BACKEND", "backend"),
    col("GPUS", "gpus"),
    col("MACHINE", "machine"),
    col("READY", "state"),
    col("RESERVED", "reserved_for"),
    col("BUSY WITH", "busy"),
    col("HEARTBEAT", "heartbeat_age_ms"),
];
const CREDENTIAL_COLS: &[out::Col] =
    &[col("ADDRESS", "address"), col("TENANT", "tenant"), col("LABEL", "label"), col("STATE", "state"), col("CREATED", "created_ms")];
const PURGE_COLS: &[out::Col] =
    &[col("RUN", "id"), col("PURGED", "purged"), col("STATE", "state"), col("SIZE", "bytes"), col("REASON", "reason")];

/// Credentials as rows: the address with 0x, a STATE column (active / revoked).
fn credential_rows(v: &Value) -> Vec<Value> {
    v.as_array()
        .into_iter()
        .flatten()
        .map(|c| {
            let mut r = c.clone();
            r["address"] = json!(format!("0x{}", c["address"].as_str().unwrap_or("")));
            r["state"] = json!(if c["revoked_ms"].is_null() { "active" } else { "revoked" });
            r
        })
        .collect()
}

/// The status's nodes, with one READY column (drain / yes / no).
fn node_rows(status: &Value) -> Vec<Value> {
    status["nodes"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|n| {
            let mut r = n.clone();
            r["state"] = json!(if n["draining"].as_bool() == Some(true) {
                "drain"
            } else if n["ready"].as_bool() == Some(true) {
                "yes"
            } else {
                "no"
            });
            r
        })
        .collect()
}

const PS_COLS: &[out::Col] = &[
    col("TYPE", "type"),
    col("APP", "app"),
    col("ID", "id"),
    col("STATE", "state"),
    col("WHERE", "where"),
    col("CREATED", "created_at"),
    col("TOOK", "took_ms"),
];

/// States that are not over yet, in both vocabularies.
const ACTIVE: &[&str] = &["queued", "running", "waiting_vram", "waiting_dependency", "retrying"];

fn is_active(row: &Value) -> bool {
    ACTIVE.contains(&row["state"].as_str().unwrap_or(""))
}

fn now_s() -> f64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs_f64()).unwrap_or(0.0)
}

/// How long it has taken: until it finished, or until now while it goes on.
fn took_ms(started_s: Option<f64>, finished_s: Option<f64>) -> Value {
    match started_s {
        Some(s) => json!(((finished_s.unwrap_or_else(now_s) - s) * 1000.0).max(0.0).round()),
        None => Value::Null,
    }
}

/// A proof app's name: its tenant and the start of its program's id (as the scheduler names it).
fn proof_app(tenant: &Value, elf_id: &Value) -> Value {
    let (tenant, elf) = (tenant.as_str().unwrap_or(""), elf_id.as_str().unwrap_or(""));
    if tenant.is_empty() && elf.is_empty() {
        return json!("(rejected)");
    }
    json!(format!("{}/{}", if tenant.is_empty() { "?" } else { tenant }, &elf[..8.min(elf.len())]))
}

/// A proof job as a list row (times from milliseconds to seconds).
fn proof_row(j: &Value) -> Value {
    let secs = |k: &str| j[k].as_f64().map(|ms| ms / 1000.0);
    json!({
        "type": "proof", "app": proof_app(&j["tenant"], &j["elf_id"]), "id": j["id"], "owner": j["tenant"],
        "state": j["state"], "where": j["node"],
        "created_at": secs("created_ms"), "started_at": secs("started_ms"), "finished_at": secs("finished_ms"),
        "took_ms": took_ms(secs("started_ms"), secs("finished_ms")), "record": j,
    })
}

/// A run as a `ps` row.
fn run_row(r: &Value) -> Value {
    let cards = r["gpus"].as_array().map(|g| g.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(","));
    json!({
        "type": "run", "app": r["profile"], "id": r["id"], "owner": Value::Null, "state": r["state"],
        "where": cards.filter(|c| !c.is_empty()).map(|c| format!("cards {c}")),
        "created_at": r["submitted_at"], "started_at": r["started_at"], "finished_at": r["finished_at"],
        "took_ms": took_ms(r["started_at"].as_f64(), r["finished_at"].as_f64()), "record": r,
    })
}

/// An app by the name `fleetctl app` shows: a proof app is `tenant/<elf id prefix>`
/// (`?` for no tenant) or `(rejected)`; any other name is a run app.
enum AppRef {
    Proof { tenant: String, elf: String },
    Run(String),
}

impl AppRef {
    fn parse(name: &str) -> Self {
        if name == "(rejected)" {
            return AppRef::Proof { tenant: String::new(), elf: String::new() };
        }
        match name.split_once('/') {
            Some((t, e)) => AppRef::Proof { tenant: if t == "?" { String::new() } else { t.into() }, elf: e.into() },
            None => AppRef::Run(name.into()),
        }
    }
}

/// Which jobs to fetch from one side.
struct Want<'a> {
    limit: usize,
    active: bool,
    tenant: Option<&'a str>,
    app: Option<&'a str>,
}

/// The newest proof jobs or runs that `want` asks for, as list rows (none when the
/// filter can only match the other type).
fn fetch_rows(kind: Kind, conn: &Conn, want: &Want) -> anyhow::Result<Vec<Value>> {
    let app = want.app.map(AppRef::parse);
    let mut q: Vec<(&str, String)> = vec![("limit", want.limit.to_string()), ("n", want.limit.to_string())];
    if want.active {
        q.push(("active", "true".into()));
    }
    let (v, row): (Value, fn(&Value) -> Value) = match (kind, app) {
        (Kind::Proof, Some(AppRef::Run(_))) | (Kind::Run, Some(AppRef::Proof { .. })) => return Ok(vec![]),
        (Kind::Run, _) if want.tenant.is_some() => return Ok(vec![]), // runs belong to no tenant
        (Kind::Proof, app) => {
            let tenant = match (&app, want.tenant) {
                (Some(AppRef::Proof { tenant, .. }), Some(t)) if tenant != t => return Ok(vec![]),
                (Some(AppRef::Proof { tenant, .. }), _) => Some(tenant.as_str()),
                (_, t) => t,
            };
            if let Some(t) = tenant {
                q.push(("tenant", t.into()));
            }
            if let Some(AppRef::Proof { elf, .. }) = &app {
                q.push(("elf", elf.clone()));
            }
            (Admin::new(conn)?.get_q("/jobs", &q)?, proof_row)
        }
        (Kind::Run, app) => {
            if let Some(AppRef::Run(name)) = &app {
                q.push(("profile", name.clone()));
            }
            (Admin::runs(conn)?.get_q("/runs", &q)?, run_row)
        }
    };
    let rows = v.as_array().into_iter().flatten().map(row);
    // an app name is exact (a proof app's elf part is a prefix on the server)
    Ok(rows
        .filter(|r| want.app.is_none_or(|a| r["app"].as_str() == Some(a)) && (!want.active || is_active(r)))
        .collect())
}

/// Every job of both types still queued or running, then the newest `-n` of each type.
fn list(p: &ListArgs, conn: &Conn, out: &Out) -> anyhow::Result<()> {
    let kinds: Vec<Kind> = match p.kind {
        Some(k) => vec![k],
        None => vec![Kind::Proof, Kind::Run],
    };
    let mut rows: Vec<Value> = Vec::new();
    let mut reached = 0;
    for k in &kinds {
        let want = |limit, active| Want { limit, active, tenant: p.tenant.as_deref(), app: p.app.as_deref() };
        // the active ones however old (a long run outlives many short proofs), then the newest
        let fetched = fetch_rows(*k, conn, &want(1000, true)).and_then(|mut r| {
            if !p.active {
                r.extend(fetch_rows(*k, conn, &want(p.limit, false))?);
            }
            Ok(r)
        });
        match fetched {
            Ok(r) => {
                reached += 1;
                for row in r {
                    if !rows.iter().any(|x| x["id"] == row["id"]) {
                        rows.push(row);
                    }
                }
            }
            Err(e) => eprintln!(
                "{}: not reachable ({e:#}); listing the rest",
                if *k == Kind::Proof { "the scheduler" } else { "the runs API" }
            ),
        }
    }
    if reached == 0 {
        bail!("neither the scheduler nor the runs API answered");
    }
    let created = |r: &Value| r["created_at"].as_f64().unwrap_or(0.0);
    rows.sort_by(|a, b| is_active(b).cmp(&is_active(a)).then(created(b).total_cmp(&created(a))));
    out.table(&rows, PS_COLS, None);
    Ok(())
}

const APP_COLS: &[out::Col] = &[
    col("TYPE", "type"),
    col("APP", "name"),
    col("ENABLED", "enabled"),
    col("ACTIVE", "active"),
    col("JOBS", "jobs"),
    col("OK", "succeeded"),
    col("FAILED", "failed"),
    col("LAST JOB", "last_at"),
    col("DETAIL", "detail"),
];

/// The one proof app a name means: as the list names it, or `tenant/<full elf id>`.
fn proof_app_named(a: &Admin, name: &str, tenant: &str) -> anyhow::Result<Value> {
    let apps = a.get("/apps")?;
    let found: Vec<&Value> = apps
        .as_array()
        .into_iter()
        .flatten()
        .filter(|x| {
            x["name"].as_str() == Some(name)
                || (x["tenant"].as_str() == Some(tenant) && name.split_once('/').map(|(_, e)| e) == x["elf_id"].as_str())
        })
        .collect();
    match found.as_slice() {
        [] => bail!("no proof app {name} (fleetctl app --type proof lists them)"),
        [one] => Ok((*one).clone()),
        many => bail!(
            "{name} names {} programs; give the full elf id: {}",
            many.len(),
            many.iter().map(|x| format!("{}/{}", tenant, x["elf_id"].as_str().unwrap_or(""))).collect::<Vec<_>>().join(", ")
        ),
    }
}

/// The run app of that name, from the list.
fn run_app_named(a: &Admin, name: &str) -> anyhow::Result<Value> {
    let apps = a.get("/apps")?;
    apps.as_array()
        .into_iter()
        .flatten()
        .find(|x| x["name"].as_str() == Some(name) && x["removed"].as_bool() != Some(true))
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("no run app {name} (fleetctl app --type run lists them)"))
}

/// Disables or enables an app.
fn set_app_disabled(name: &str, disabled: bool, conn: &Conn, out: &Out) -> anyhow::Result<()> {
    let method = if disabled { reqwest::Method::PUT } else { reqwest::Method::DELETE };
    let v = match AppRef::parse(name) {
        AppRef::Proof { tenant, .. } => {
            let a = Admin::new(conn)?;
            let app = proof_app_named(&a, name, &tenant)?;
            let key = [("tenant", app["tenant"].as_str().unwrap_or("")), ("elf_id", app["elf_id"].as_str().unwrap_or(""))];
            a.send(a.req(method, "/apps/disabled").query(&key))?
        }
        AppRef::Run(_) => {
            let a = Admin::runs(conn)?;
            run_app_named(&a, name)?;
            a.send(a.req(method, &format!("/apps/{name}/disabled")))?
        }
    };
    let text = if disabled {
        format!("{name} disabled: it takes no new jobs; the queued and running ones finish")
    } else {
        format!("{name} enabled: it takes jobs again")
    };
    out.done(&text, v);
    Ok(())
}

/// Adds an app: a tenant's program (registered with the scheduler) or a run profile.
fn add_app(add: &AddApp, conn: &Conn, out: &Out) -> anyhow::Result<()> {
    if let AppRef::Proof { tenant, elf } = AppRef::parse(&add.name) {
        if add.image.is_some() || !add.allow.is_empty() || !add.env.is_empty() || add.user.is_some() || add.max_gpus.is_some() {
            bail!("{}: a proof app takes --elf at most; --image, --allow, --user, --max-gpus, --env are a run app's", add.name);
        }
        if tenant.is_empty() || add.name == "(rejected)" {
            bail!("a proof app is TENANT/<elf id> (or TENANT/ with --elf FILE)");
        }
        let elf_id = match (&add.elf, elf.as_str()) {
            (Some(file), "") => {
                use sha2::Digest;
                let bytes = std::fs::read(file).with_context(|| format!("reading {}", file.display()))?;
                hex::encode(sha2::Sha256::digest(&bytes))
            }
            (None, id) if !id.is_empty() => id.trim_start_matches("0x").to_ascii_lowercase(),
            (Some(_), _) => bail!("give the elf id in the name or --elf FILE, not both"),
            (None, _) => bail!("give TENANT/<elf id>, or TENANT/ with --elf FILE"),
        };
        let a = Admin::new(conn)?;
        let v = a.send(a.req(reqwest::Method::POST, "/apps").query(&[("tenant", tenant.as_str()), ("elf_id", elf_id.as_str())]))?;
        out.done(
            &format!("added proof app {}: tenant {tenant}, program {elf_id} (its jobs are accepted from now on{})",
                     v["app"].as_str().unwrap_or(&add.name),
                     if v["elf_stored"].as_bool() == Some(true) { "" } else { "; the ELF itself arrives with the first one" }),
            v,
        );
        return Ok(());
    }
    if add.image.is_none() || add.allow.is_empty() {
        bail!("a run app needs --image and at least one --allow root");
    }
    let mut body = serde_json::to_value(add)?;
    let mut env = serde_json::Map::new();
    for kv in &add.env {
        let Some((k, v)) = kv.split_once('=') else { bail!("--env {kv}: K=V") };
        env.insert(k.into(), json!(v));
    }
    if !env.is_empty() {
        body["env"] = Value::Object(env);
    }
    let a = Admin::runs(conn)?;
    let v = a.send(a.req(reqwest::Method::POST, "/apps").json(&body))?;
    let gpus = v["max_gpus"].as_u64().unwrap_or(0);
    out.done(
        &format!("added run app {}: image {}, up to {gpus} GPU{}, mounts under {}", add.name,
                 v["image"].as_str().unwrap_or("?"), if gpus == 1 { "" } else { "s" },
                 v["allow"].as_array().map(|r| r.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(", ")).unwrap_or_default()),
        v,
    );
    Ok(())
}

/// One app's record, and its newest jobs.
fn describe_app(name: &str, conn: &Conn, out: &Out) -> anyhow::Result<()> {
    let (kind, mut rec) = match AppRef::parse(name) {
        AppRef::Proof { tenant, .. } => (Kind::Proof, with_type("proof", &proof_app_named(&Admin::new(conn)?, name, &tenant)?)),
        AppRef::Run(_) => (Kind::Run, with_type("run", &run_app_named(&Admin::runs(conn)?, name)?)),
    };
    let full = rec["name"].as_str().unwrap_or(name).to_string();
    let jobs = fetch_rows(kind, conn, &Want { limit: 5, active: false, tenant: None, app: Some(&full) })?;
    if out.json {
        rec["newest_jobs"] = Value::Array(jobs);
        out.record(&rec);
    } else {
        out.record(&rec);
        if !jobs.is_empty() {
            println!();
            out.table(&jobs, PS_COLS, None);
        }
    }
    Ok(())
}

/// Deletes an app (after showing what goes with it, until --yes).
fn delete_app(name: &str, yes: bool, conn: &Conn, out: &Out) -> anyhow::Result<()> {
    use reqwest::Method as M;
    match AppRef::parse(name) {
        AppRef::Proof { tenant, .. } => {
            let a = Admin::new(conn)?;
            let app = proof_app_named(&a, name, &tenant)?;
            let elf = app["elf_id"].as_str().unwrap_or("");
            if !yes {
                let rest = if elf.is_empty() {
                    String::new() // requests refused before a program was known: no ELF, no anchor
                } else {
                    format!(", its anchor, and its ELF {elf} unless another tenant uses it")
                };
                bail!(
                    "would delete proof app {name}: {} job record(s) with their inputs and proofs{rest} \
                     ({} queued or running: refused while any is). Pass --yes to delete",
                    app["jobs"], app["active"]
                );
            }
            let v = a.send(a.req(M::DELETE, "/apps").query(&[("tenant", app["tenant"].as_str().unwrap_or("")), ("elf_id", elf)]))?;
            out.done(
                &format!("deleted proof app {name}: {} job(s), ELF {}", v["jobs_deleted"],
                         if v["elf_removed"].as_bool() == Some(true) { "removed" } else { "kept (in use or absent)" }),
                v,
            );
        }
        AppRef::Run(_) => {
            let a = Admin::runs(conn)?;
            let app = run_app_named(&a, name)?;
            if !yes {
                bail!(
                    "would remove run app {name} (its profile) from runs.yaml; its {} past run(s) \
                     stay ({} queued or running: refused while any is). Pass --yes to delete",
                    app["jobs"], app["active"]
                );
            }
            let v = a.send(a.req(M::DELETE, &format!("/apps/{name}")))?;
            out.done(&format!("deleted run app {name}; its {} past run(s) stay", v["runs_kept"]), v);
        }
    }
    Ok(())
}

/// Proof apps from the scheduler and run apps from the runs API, most recently used first.
fn app(p: &AppArgs, out: &Out) -> anyhow::Result<()> {
    match &p.cmd {
        Some(AppCmd::Delete { name, yes }) => return delete_app(name, *yes, &p.conn, out),
        Some(AppCmd::Disable { name }) => return set_app_disabled(name, true, &p.conn, out),
        Some(AppCmd::Enable { name }) => return set_app_disabled(name, false, &p.conn, out),
        Some(AppCmd::Add { add }) => return add_app(add, &p.conn, out),
        None => {}
    }
    if let Some(name) = &p.name {
        return describe_app(name, &p.conn, out);
    }
    let mut rows: Vec<Value> = Vec::new();
    let mut raw = serde_json::Map::new();
    let mut reached = 0;
    if p.kind != Some(Kind::Run) {
        match Admin::new(&p.conn).and_then(|a| a.get("/apps")) {
            Ok(v) => {
                reached += 1;
                for a in v.as_array().into_iter().flatten() {
                    let mean = a["mean_prove_ms"].as_f64().map(|m| format!("mean prove {:.1} s", m / 1000.0));
                    let anchored = a["anchored_on"].as_str().map(|n| format!("anchored on {n}"));
                    let detail: Vec<String> = [anchored, mean].into_iter().flatten().collect();
                    rows.push(json!({
                        "type": "proof", "name": a["name"], "enabled": a["disabled"].as_bool() != Some(true),
                        "active": a["active"], "jobs": a["jobs"],
                        "succeeded": a["succeeded"], "failed": a["failed"],
                        "last_at": a["last_created_ms"].as_f64().map(|ms| ms / 1000.0), "detail": detail.join(", "),
                    }));
                }
                raw.insert("proof".into(), v);
            }
            Err(e) => eprintln!("the scheduler: not reachable ({e:#}); listing the rest"),
        }
    }
    if p.kind != Some(Kind::Proof) {
        match Admin::runs(&p.conn).and_then(|a| a.get("/apps")) {
            Ok(v) => {
                reached += 1;
                for a in v.as_array().into_iter().flatten() {
                    let removed = a["removed"].as_bool() == Some(true);
                    rows.push(json!({
                        "type": "run", "name": a["name"], "enabled": !removed && a["disabled"].as_bool() != Some(true),
                        "active": a["active"], "jobs": a["jobs"],
                        "succeeded": a["succeeded"], "failed": a["failed"], "last_at": a["last_submitted_at"],
                        "detail": if removed {
                            "removed from runs.yaml: only its past runs are left".to_string()
                        } else {
                            let gpus = a["max_gpus"].as_u64().unwrap_or(0);
                            format!("image {}, up to {gpus} GPU{}", a["image"].as_str().unwrap_or("?"), if gpus == 1 { "" } else { "s" })
                        },
                    }));
                }
                raw.insert("run".into(), v);
            }
            Err(e) => eprintln!("the runs API: not reachable ({e:#}); listing the rest"),
        }
    }
    if reached == 0 {
        bail!("neither the scheduler nor the runs API answered");
    }
    rows.sort_by(|a, b| b["last_at"].as_f64().unwrap_or(0.0).total_cmp(&a["last_at"].as_f64().unwrap_or(0.0)));
    out.table(&rows, APP_COLS, Some(&Value::Object(raw)));
    Ok(())
}

/// `record` with a leading `type` field (proof or run; a proof's own `kind` is its proof kind).
fn with_type(t: &str, record: &Value) -> Value {
    let mut m = serde_json::Map::new();
    m.insert("type".into(), json!(t));
    if let Some(o) = record.as_object() {
        m.extend(o.clone());
    }
    Value::Object(m)
}

/// A proof job or a run, by id: the scheduler is asked first, then the runs API.
fn record(id: &str, conn: &Conn, out: &Out) -> anyhow::Result<()> {
    let proof = Admin::new(conn).and_then(|a| a.get(&format!("/jobs/{id}")));
    if let Ok(v) = &proof {
        out.record(&with_type("proof", v));
        return Ok(());
    }
    match Admin::runs(conn).and_then(|a| a.get(&format!("/runs/{id}"))) {
        Ok(v) => {
            out.record(&with_type("run", &v));
            Ok(())
        }
        Err(run_err) => bail!(
            "no proof job or run {id} (scheduler: {:#}; runs API: {run_err:#})",
            proof.err().unwrap_or_else(|| anyhow::anyhow!("-"))
        ),
    }
}

fn job(j: &JobArgs, out: &Out) -> anyhow::Result<()> {
    match (&j.cmd, &j.id) {
        (None, None) => list(&j.list, &j.conn, out),
        (None, Some(id)) => record(id, &j.conn, out),
        (Some(cmd), _) => run_action(cmd, &j.conn, out),
    }
}

/// submit / logs / cancel / purge / fetch, through the runs API.
fn run_action(cmd: &JobCmd, conn: &Conn, out: &Out) -> anyhow::Result<()> {
    use reqwest::Method as M;
    let a = Admin::runs(conn)?;
    match cmd {
        JobCmd::Submit { submit } => {
            let mut body = serde_json::to_value(submit)?;
            let mut uploaded = Value::Null;
            if let Some(code) = &submit.code {
                let local = Path::new(code);
                if !code.starts_with("git:") && local.is_file() {
                    let bytes = std::fs::read(local).with_context(|| format!("reading {code}"))?;
                    uploaded = a.send(a.req(M::PUT, "/uploads").timeout(Duration::from_secs(3600)).body(bytes))?;
                    body["code"] = uploaded["path"].clone();
                }
            }
            let mut v = a.send(a.req(M::POST, "/runs").json(&body))?;
            if !uploaded.is_null() {
                v["uploaded"] = uploaded;
            }
            out.record(&v);
        }
        JobCmd::Cancel { id } => out.record(&a.send(a.req(M::POST, &format!("/runs/{id}/cancel")))?),
        JobCmd::Preempt { id } => out.record(&a.send(a.req(M::POST, &format!("/runs/{id}/preempt")))?),
        JobCmd::Purge { id, older_than } => {
            let path = match (id, older_than) {
                (Some(id), _) => format!("/runs/{id}"),
                (None, Some(days)) => format!("/runs?older_than={days}"),
                (None, None) => bail!("purge ID, or purge --older-than DAYS"),
            };
            let v = a.send(a.req(M::DELETE, &path))?;
            out.table(v.as_array().map(Vec::as_slice).unwrap_or_default(), PURGE_COLS, Some(&v));
        }
        JobCmd::Logs { id, follow, lines, rank } => {
            let first = a.get(&format!("/runs/{id}/logs?rank={rank}&lines={lines}"));
            let mut chunk = match first {
                Ok(c) => c,
                Err(run_err) => {
                    // not a run: a proof job has no output of its own, its record is what there is
                    let Ok(proof) = Admin::new(conn).and_then(|s| s.get(&format!("/jobs/{id}"))) else {
                        return Err(run_err);
                    };
                    if !out.json {
                        eprintln!(
                            "{id} is a proof job: it has no output of its own (logs are a run's container \
                             output). Its record (a failed attempt's error is in `error`):"
                        );
                    }
                    out.record(&with_type("proof", &proof));
                    return Ok(());
                }
            };
            let mut stdout = std::io::stdout();
            loop {
                if out.json {
                    out.line(&chunk);
                } else {
                    stdout.write_all(chunk["data"].as_str().unwrap_or("").as_bytes())?;
                    stdout.flush()?;
                }
                if !*follow || chunk["done"].as_bool() == Some(true) {
                    break;
                }
                if chunk["data"].as_str().is_none_or(str::is_empty) {
                    std::thread::sleep(Duration::from_secs(1));
                }
                let offset = chunk["offset"].as_u64().unwrap_or(0);
                chunk = a.get(&format!("/runs/{id}/logs?rank={rank}&offset={offset}"))?;
            }
        }
        JobCmd::Fetch { id, out: file } => {
            if file.is_none() && out.json {
                bail!("fetch writes the tar.gz itself to stdout; with --json give --out FILE");
            }
            let mut r = a.req(M::GET, &format!("/runs/{id}/out.tar.gz")).timeout(Duration::from_secs(3600)).send()?;
            if !r.status().is_success() {
                bail!("{}: {}", r.status(), r.text()?);
            }
            match file {
                Some(path) => {
                    let mut f = std::fs::File::create(path).with_context(|| format!("creating {}", path.display()))?;
                    let n = r.copy_to(&mut f)?;
                    out.done(&format!("wrote {} ({n} bytes)", path.display()),
                             json!({ "run": id, "file": path.display().to_string(), "bytes": n }));
                }
                None => {
                    r.copy_to(&mut std::io::stdout())?;
                }
            }
        }
    }
    Ok(())
}

fn init(cmd: &InitCmd, out: &Out) -> anyhow::Result<()> {
    match cmd {
        InitCmd::Token { out: path } => {
            let mut bytes = [0u8; 48];
            rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut bytes);
            write_secret(path, format!("{}\n", hex::encode(bytes)).as_bytes())?;
            out.done(&format!("wrote {}", path.display()), json!({ "token_file": path.display().to_string() }));
        }
        InitCmd::Tls { dir, names } => {
            let v = tls_init(dir, names)?;
            let d = dir.display();
            out.done(
                &format!(
                    "CA and server certificate for {names:?} in {d}\n\
                     scheduler: tls.cert_path={d}/server.pem tls.key_path={d}/server.key tls.client_ca_path={d}/ca.pem\n\
                     clients:   CA_CERT_PATH={d}/ca.pem (and DOMAIN_NAME={} if not \"stage\")",
                    names[0]
                ),
                v,
            );
        }
        InitCmd::TlsClient { dir, name } => {
            let (c, k) = tls_client(dir, name)?;
            out.done(
                &format!("client certificate: SSL_CERT_PATH={} SSL_KEY_PATH={}", c.display(), k.display()),
                json!({ "cert": c.display().to_string(), "key": k.display().to_string() }),
            );
        }
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

/// Writes ca.pem/ca.key and server.pem/server.key into `dir`; says what it wrote.
fn tls_init(dir: &Path, names: &[String]) -> anyhow::Result<Value> {
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
    let file = |f: &str| dir.join(f).display().to_string();
    Ok(json!({
        "dir": dir.display().to_string(), "names": names,
        "ca_cert": file("ca.pem"), "ca_key": file("ca.key"), "server_cert": file("server.pem"), "server_key": file("server.key"),
    }))
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
