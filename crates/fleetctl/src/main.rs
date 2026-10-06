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
    /// Queue, nodes, anchors, and the runs going on.
    Status {
        #[command(flatten)]
        conn: Conn,
    },
    /// Proof jobs and one-shot runs: the list (by default), one by its id, or submit /
    /// logs / cancel / purge / fetch a run.
    Job(JobArgs),
    /// Prover nodes: the list (by default), or drain / undrain one.
    Node(NodeArgs),
    /// Tenant signing keys (the SDK's ZKM_PRIVATE_KEY).
    #[command(subcommand)]
    Credential(CredentialCmd),
    /// Set up a cluster's secrets, offline: the cluster and admin tokens, the stage API's
    /// certificates (install.sh runs these; no scheduler is needed).
    #[command(subcommand)]
    Init(InitCmd),
}

/// Which proof jobs and runs a list shows.
#[derive(Args)]
struct ListArgs {
    /// How many of the newest.
    #[arg(short = 'n', long, default_value_t = 20)]
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
struct NodeArgs {
    #[command(subcommand)]
    cmd: Option<NodeCmd>,
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
        #[command(flatten)]
        conn: Conn,
    },
    /// Every registered key: address, tenant, label, active or revoked.
    List {
        #[command(flatten)]
        conn: Conn,
    },
    /// Refuse a key's requests from now on.
    Revoke {
        address: String,
        #[command(flatten)]
        conn: Conn,
    },
}

/// A submit: the command line and the request body are this one struct (its field
/// names are the API's), so nothing is assigned by hand. Unset flags are left out and
/// take the server's defaults.
#[derive(Args, Serialize)]
struct Submit {
    /// Profile in the server's runs.yaml (image, user, GPU cap, allowed mount roots).
    #[arg(long)]
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

/// What can be done to a run (a proof job is the scheduler's and its client's).
#[derive(Subcommand)]
enum JobCmd {
    /// Queue a run: fleetctl job submit --profile P --gpus 1 --mount /data:ro -- python3 train.py
    Submit {
        #[command(flatten)]
        submit: Submit,
    },
    /// A run's output: the last lines, then with -f everything new until it ends.
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
    /// Stop a run (its containers are removed, its cards freed).
    Cancel { id: String },
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
        other => admin(other, &out),
    }
}

impl Cmd {
    /// The connection of a command that talks to the scheduler.
    fn conn(&self) -> &Conn {
        match self {
            Cmd::Status { conn } => conn,
            Cmd::Node(n) => &n.conn,
            Cmd::Credential(
                CredentialCmd::Issue { conn, .. } | CredentialCmd::List { conn } | CredentialCmd::Revoke { conn, .. },
            ) => conn,
            Cmd::Init(_) | Cmd::Job(_) => unreachable!("not the scheduler's admin API alone"),
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
}

fn admin(cmd: &Cmd, out: &Out) -> anyhow::Result<()> {
    let a = Admin::new(cmd.conn())?;
    use reqwest::Method as M;
    match cmd {
        Cmd::Status { conn } => {
            let mut v = a.get("/status")?;
            let runs = fetch_rows(Kind::Run, conn, 200, None).map(|rows| rows.into_iter().filter(is_active).collect::<Vec<_>>());
            if out.json {
                if let Ok(r) = &runs {
                    v["runs"] = Value::Array(r.clone());
                }
                out.record(&v);
            } else {
                println!("proof jobs: queued {}  running {}\n", v["queued"], v["running"]);
                out.table(&node_rows(&v), NODE_COLS, None);
                let anchors = anchor_rows(&v);
                if !anchors.is_empty() {
                    println!();
                    out.table(&anchors, ANCHOR_COLS, None);
                }
                println!();
                match runs {
                    Ok(r) if r.is_empty() => println!("runs: none going on"),
                    Ok(r) => out.table(&r, PS_COLS, None),
                    Err(e) => println!("runs: the runs API is not reachable ({e:#})"),
                }
            }
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
        Cmd::Credential(CredentialCmd::Issue { tenant, label, out: env_file, endpoint, tls_dir, domain_name, .. }) => {
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
        Cmd::Credential(CredentialCmd::List { .. }) => {
            let v = a.get("/credentials")?;
            let rows: Vec<Value> = v
                .as_array()
                .into_iter()
                .flatten()
                .map(|c| {
                    let mut r = c.clone();
                    r["address"] = json!(format!("0x{}", c["address"].as_str().unwrap_or("")));
                    r["state"] = json!(if c["revoked_ms"].is_null() { "active" } else { "revoked" });
                    r
                })
                .collect();
            out.table(&rows, CREDENTIAL_COLS, Some(&v));
        }
        Cmd::Credential(CredentialCmd::Revoke { address, .. }) => {
            a.send(a.req(M::DELETE, &format!("/credentials/{}", address.trim_start_matches("0x"))))?;
            out.done(&format!("revoked {address}"), json!({ "revoked": address }));
        }
        Cmd::Init(_) | Cmd::Job(_) => unreachable!("not the scheduler's admin API alone"),
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
const ANCHOR_COLS: &[out::Col] = &[col("TENANT", "tenant"), col("PROGRAM", "program"), col("ANCHORED ON", "node"), col("NODE GPUS", "gpus")];
const CREDENTIAL_COLS: &[out::Col] =
    &[col("ADDRESS", "address"), col("TENANT", "tenant"), col("LABEL", "label"), col("STATE", "state"), col("CREATED", "created_ms")];
const PURGE_COLS: &[out::Col] =
    &[col("RUN", "id"), col("PURGED", "purged"), col("STATE", "state"), col("SIZE", "bytes"), col("REASON", "reason")];

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

/// Anchors as rows: a tenant's program pinned to one prover node (all that node's cards).
fn anchor_rows(status: &Value) -> Vec<Value> {
    let gpus = |node: &str| {
        status["nodes"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|n| n["name"].as_str() == Some(node))
            .map(|n| n["gpus"].clone())
            .unwrap_or(Value::Null)
    };
    status["anchors"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|a| {
            let elf = a[1].as_str().unwrap_or("");
            let node = a[2].as_str().unwrap_or("");
            json!({ "tenant": a[0], "program": format!("{}…", &elf[..12.min(elf.len())]), "node": node, "gpus": gpus(node) })
        })
        .collect()
}

const PS_COLS: &[out::Col] = &[
    col("TYPE", "type"),
    col("ID", "id"),
    col("OWNER", "owner"),
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

/// A proof job as a `ps` row (times from milliseconds to seconds, the state in lower case).
fn proof_row(j: &Value) -> Value {
    let secs = |k: &str| j[k].as_f64().map(|ms| ms / 1000.0);
    let state = j["state"].as_str().unwrap_or("").split('(').next().unwrap_or("").to_ascii_lowercase();
    json!({
        "type": "proof", "id": j["id"], "owner": j["tenant"], "state": state, "where": j["node"],
        "created_at": secs("created_ms"), "started_at": secs("started_ms"), "finished_at": secs("finished_ms"),
        "took_ms": took_ms(secs("started_ms"), secs("finished_ms")), "record": j,
    })
}

/// A run as a `ps` row.
fn run_row(r: &Value) -> Value {
    let cards = r["gpus"].as_array().map(|g| g.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(","));
    json!({
        "type": "run", "id": r["id"], "owner": Value::Null, "state": r["state"],
        "where": cards.filter(|c| !c.is_empty()).map(|c| format!("cards {c}")),
        "created_at": r["submitted_at"], "started_at": r["started_at"], "finished_at": r["finished_at"],
        "took_ms": took_ms(r["started_at"].as_f64(), r["finished_at"].as_f64()), "record": r,
    })
}

/// The newest proof jobs or runs, as `ps` rows.
fn fetch_rows(kind: Kind, conn: &Conn, limit: usize, tenant: Option<&str>) -> anyhow::Result<Vec<Value>> {
    let (v, row): (Value, fn(&Value) -> Value) = match kind {
        Kind::Proof => {
            let a = Admin::new(conn)?;
            let only = tenant.map(|t| format!("&tenant={t}")).unwrap_or_default();
            (a.get(&format!("/jobs?limit={limit}{only}"))?, proof_row)
        }
        Kind::Run => {
            let a = Admin::runs(conn)?;
            (a.get(&format!("/runs?n={limit}"))?, run_row)
        }
    };
    Ok(v.as_array().into_iter().flatten().map(row).collect())
}

/// Proof jobs and runs, newest first.
fn list(p: &ListArgs, conn: &Conn, out: &Out) -> anyhow::Result<()> {
    let kinds: Vec<Kind> = match (p.kind, &p.tenant) {
        (Some(k), _) => vec![k],
        (None, Some(_)) => vec![Kind::Proof],
        (None, None) => vec![Kind::Proof, Kind::Run],
    };
    // --active looks further back, so a long run is not pushed out by many short proofs
    let fetch = if p.active { 1000 } else { p.limit };
    let mut rows = Vec::new();
    let mut reached = 0;
    for k in &kinds {
        match fetch_rows(*k, conn, fetch, p.tenant.as_deref()) {
            Ok(r) => {
                rows.extend(r);
                reached += 1;
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
    rows.retain(|r| !p.active || is_active(r));
    rows.sort_by(|a, b| b["created_at"].as_f64().unwrap_or(0.0).total_cmp(&a["created_at"].as_f64().unwrap_or(0.0)));
    rows.truncate(p.limit);
    out.table(&rows, PS_COLS, None);
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
            let mut chunk = a.get(&format!("/runs/{id}/logs?rank={rank}&lines={lines}"))?;
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
