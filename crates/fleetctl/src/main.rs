//! fleetctl: operate a zkm-fleet cluster and issue its tokens.
//!
//! User tokens are issued by administrators and written to 0600 files. The proof SDK still requires an internal signing key; its address is bound to that token and is not a separate access resource.
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

use out::{col, OutputWriter};

#[derive(Parser)]
#[command(about = "Operate a zkm-fleet cluster and issue its tokens")]
struct Cli {
    /// Print the result as JSON instead of the formatted view.
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    cmd: Cmd,
    #[command(flatten)]
    _conn: ConnectionOptions,
}

/// The single control-plane endpoint and its access token: every
/// command that talks to the cluster has these (the `init` ones work offline and do not).
#[derive(Args, Clone)]
struct ConnectionOptions {
    /// Control-plane HTTP base URL for every command (admin API under /admin).
    #[arg(long, env = "FLEET_URL", default_value = "http://127.0.0.1:9191", global = true)]
    url: String,
    /// File holding the admin token (default: admin.token in FLEET_ETC, ~/.config/zkm-fleet or /etc/zkm-fleet).
    #[arg(long, env = "FLEET_ADMIN_TOKEN_FILE", global = true)]
    admin_token_file: Option<PathBuf>,
    /// User API token file. Takes precedence over the operator admin token.
    #[arg(long, env = "FLEET_TOKEN_FILE", global = true)]
    token_file: Option<PathBuf>,
    /// User namespace selector (inferred from user tokens; operators may select one).
    #[arg(long, env = "FLEET_NAMESPACE", global = true)]
    namespace: Option<String>,
    /// Optional KEY=VALUE tag within the namespace; inferred from user tokens.
    #[arg(long = "tag", value_name = "KEY=VALUE", env = "FLEET_TAG", global = true)]
    tag: Option<String>,
}

#[derive(Subcommand)]
enum Cmd {
    /// Long-running workloads.
    App(AppArgs),
    /// Finite batch workloads and their jobs.
    Batch(BatchArgs),
    /// Execution records: list or inspect app and batch jobs; fetch proof outputs.
    Job(JobArgs),
    /// Machine nodes: list their capacity, or drain / undrain a machine.
    Node(NodeArgs),
    /// Administrator-managed user access tokens: list, inspect, issue, refresh or revoke.
    Token(TokenArgs),
    /// Inspect your namespace; administrators manage shared quotas and access.
    Namespace(NamespaceArgs),
    /// Set up a cluster's secrets, offline: the cluster and admin tokens, the stage API's
    /// certificates (install.sh jobs these; no scheduler is needed).
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
    #[command(flatten)]
    conn: ConnectionOptions,
}

#[derive(Subcommand)]
enum AppCmd {
    /// Register a proof program: tenant/<ELF id>, or tenant/ --elf FILE.
    /// Serving deployments are initially configured in the node's services.yaml.
    Add {
        #[command(flatten)]
        add: AddProofApp,
    },
    /// Disable an app: proof apps drain and stop their dedicated GPU servers; persistent apps stop and release GPUs.
    Disable { name: String },
    /// Set the priority inherited by new jobs (higher values run first).
    Priority { name: String, #[arg(allow_hyphen_values = true)] priority: i32 },
    /// The app takes jobs again.
    Enable { name: String },
    /// App owners: change a proof or persistent app's resources within namespace quotas.
    /// Without resource flags, inspect its desired resources and scaling status.
    Scale {
        name: String,
        /// GPU units per replica (proof servers require whole cards).
        #[arg(long)] gpus: Option<f64>,
        /// CPU units per replica.
        #[arg(long)] cpus: Option<u32>,
        /// Replicas on the selected node (positive; use app disable to stop all).
        #[arg(long)] replicas: Option<u32>,
        /// Select a machine agent when the app has services on multiple nodes.
        #[arg(long)] node: Option<String>,
    },
    /// Show the last lines of an app's log, read from the node that runs it.
    Logs {
        name: String,
        /// How many of the last lines to show (at most 2000).
        #[arg(short = 'n', long, default_value_t = 100)] lines: u32,
        /// Select a machine agent when the app has services on multiple nodes.
        #[arg(long)] node: Option<String>,
    },
    /// Delete an app, refused while any of its jobs is queued or running. A proof app
    /// (tenant/<elf>) goes with its jobs' records, inputs and proofs, its anchor, and its
    /// ELF when no other tenant uses it; serving deployments must be disabled first.
    /// Without --yes it only says what it would delete.
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
    /// How many of the newest of each type (app and batch jobs); the active ones are always shown.
    #[arg(short = 'n', long, default_value_t = 10)]
    limit: usize,
    /// Only app jobs, or only batch jobs.
    #[arg(long = "type", value_enum)]
    kind: Option<JobType>,
    /// Only what is queued or running.
    #[arg(short, long)]
    active: bool,
    /// Only this tenant's app jobs (batch jobs belong to no tenant).
    #[arg(long)]
    tenant: Option<String>,
    /// Only the jobs of this app (as `fleetctl app` names it: tenant/<elf> or a batch profile).
    #[arg(long)]
    app: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum JobType {
    /// Jobs of apps: the proofs a proof app runs.
    App,
    Batch,
}

#[derive(Args)]
#[command(args_conflicts_with_subcommands = true)]
struct JobArgs {
    #[command(subcommand)]
    cmd: Option<JobCmd>,
    /// An app or batch job's id: its record. Without one, the list.
    id: Option<String>,
    #[command(flatten)]
    list: ListArgs,
    #[command(flatten)]
    conn: ConnectionOptions,
}

#[derive(Args)]
#[command(args_conflicts_with_subcommands = true)]
struct NodeArgs {
    #[command(subcommand)]
    cmd: Option<NodeCmd>,
    /// A machine node's name: its capacity, apps and active jobs. Without one, the list.
    name: Option<String>,
    #[command(flatten)]
    conn: ConnectionOptions,
}

#[derive(Args)]
#[command(args_conflicts_with_subcommands = true)]
struct TokenArgs {
    #[command(subcommand)]
    cmd: Option<TokenCmd>,
    /// A token's opaque ID: its record. Without one, the list.
    id: Option<String>,
    #[command(flatten)]
    conn: ConnectionOptions,
}

#[derive(Args)]
struct NamespaceArgs {
    #[command(subcommand)]
    cmd: Option<NamespaceCmd>,
    #[command(flatten)]
    conn: ConnectionOptions,
}

#[derive(Subcommand)]
enum NamespaceCmd {
    /// Administrator-only: create a namespace and its owner token bundle for scope=default.
    Create {
        /// Unique namespace name: lowercase letters, digits and hyphens.
        name: String,
        /// Tenant identity that will sign proof jobs for this namespace.
        #[arg(long)]
        tenant: String,
        #[command(flatten)] quota: QuotaArgs,
        #[command(flatten)] bundle: TokenBundle,
    },
    /// Administrator-only: replace shared GPU/CPU caps; --tag KEY=VALUE changes that tag's caps.
    /// Omitted caps are unlimited; no cap flags clears both. This does not allocate hardware.
    Quota { name: String, #[command(flatten)] quota: QuotaArgs },
    /// Inspect your namespace's quota and reserved resources; administrators can inspect any namespace.
    /// Scoped tokens see their own tag's usage and caps; --tag must match that tag.
    Get { name: String },
    /// List namespaces and configured quotas (the default); scoped tokens see only their own.
    List,
    /// Administrator-only: block namespace tokens and new submissions; accepted jobs keep running.
    Suspend { name: String },
    /// Administrator-only: restore namespace access; individually revoked tokens remain revoked.
    Resume { name: String },
}

#[derive(Args)]
struct QuotaArgs {
    /// Shared concurrent GPU cap for apps and batches; fractions are allowed (omitted: unlimited).
    #[arg(long)]
    gpus: Option<f64>,
    /// Shared concurrent CPU cap for apps and batches (omitted: unlimited).
    #[arg(long)]
    cpus: Option<f64>,
}

impl QuotaArgs {
    fn value(&self) -> anyhow::Result<Value> {
        if [self.gpus, self.cpus].into_iter().flatten().any(|v| !v.is_finite() || v < 0.0) {
            bail!("GPU/CPU caps must be finite and non-negative");
        }
        Ok(json!({"gpus": self.gpus, "cpus": self.cpus}))
    }
}

#[derive(Subcommand)]
enum NodeCmd {
    /// Every machine node: capacity, readiness and active jobs (the default).
    List,
    /// Stop placing new proof and batch jobs on a machine; active jobs finish.
    Drain { name: String },
    /// Let a drained node take jobs again.
    Undrain { name: String },
}

#[derive(Subcommand)]
enum TokenCmd {
    /// Issue an admin-managed token scoped to approved apps and batch profiles and write the client bundle.
    Issue {
        #[arg(long)]
        tenant: String,
        /// Approved app (repeat): serving/batch profile name or tenant/full-ELF-id. Required for user access.
        #[arg(long = "app", required = true)]
        apps: Vec<String>,
        #[arg(long, default_value = "")]
        label: String,
        #[command(flatten)]
        bundle: TokenBundle,
    },
    /// Issue a replacement in the same namespace with the same grants; revoke the old key after verification.
    Refresh {
        id: String,
        #[command(flatten)]
        bundle: TokenBundle,
    },
    /// Every registered token: proof binding, tenant, label, active or revoked (the default).
    List,
    /// Revoke a token immediately.
    Revoke { id: String },
}

#[derive(Args)]
struct TokenBundle {
        /// Env file for the client (0600): ZKM_PROVER, ENDPOINT, token and signing key.
        #[arg(long)]
        out: PathBuf,
        /// Stage API endpoint clients use, e.g. https://fleet.example:51000.
        #[arg(long)]
        endpoint: String,
        /// Server name in the stage certificate, when it is not the SDK's default "stage".
        #[arg(long)]
        domain_name: Option<String>,
        /// CA bundle used to verify the stage server certificate; omit when the SDK can use system trust roots.
        #[arg(long)]
        server_ca: Option<PathBuf>,
 }

/// A submit: the command line and the request body are this one struct (its field
/// names are the API's), so nothing is assigned by hand. Unset flags are left out and
/// take the server's defaults.
#[derive(Args, Serialize)]
struct BatchSubmission {
    /// Batch workload name (image, user, GPU cap and approved mount roots).
    #[arg(long = "app")]
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
    /// Spread the batch job over N machines (one container per machine).
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    nodes: Option<u32>,
    /// The machine the batch job must land on; with --nodes N, repeat for each.
    #[arg(long)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    machine: Vec<String>,
    /// Run only after this batch job succeeded.
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    after: Option<String>,
    /// Try a failed batch job again up to N times.
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    retry: Option<u32>,
    /// Try again up to N times when an attempt ran out of memory.
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    restart_on_oom: Option<u32>,
    /// PATH[:ro|:rw] on the execution machine under an app's allowed roots; repeat.
    #[arg(long)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    mount: Vec<String>,
    /// K=V for the job ({out} expands); repeat. Apps obtain inputs from storage.
    #[arg(long)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    env: Vec<String>,
    /// File the job's output is appended to (default: the batch job's own log).
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

/// A batch profile to add. The command line and the request body are this one
/// struct (its field names are the profile's); unset flags take the server's defaults.
#[derive(Args, Serialize)]
struct AddBatch {
    /// Priority inherited by new jobs (default 0; higher values run first).
    #[arg(long, allow_hyphen_values = true)]
    #[serde(skip_serializing_if = "Option::is_none")]
    priority: Option<i32>,
    /// Profile name (letters, digits, _ . -); used with batch submit --app.
    name: String,
    /// Container image used by this batch profile.
    #[arg(long)]
    image: String,
    /// UID[:GID] the container jobs as (default: the image's).
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    user: Option<String>,
    /// Cards a batch job may ask for (default 8).
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    max_gpus: Option<u32>,
    /// GiB a batch job needs free on its card unless it says --gpu-mem (default: the whole card).
    #[arg(long = "gpu-mem")]
    #[serde(rename = "default_gpu_mem_gb", skip_serializing_if = "Option::is_none")]
    gpu_mem: Option<u32>,
    /// /dev/shm size in GiB (default 16).
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    shm_gb: Option<u32>,
    /// Absolute mount root allowed by the profile owner.
    #[arg(long)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    allow: Vec<String>,
    /// K=V every batch job gets; repeat.
    #[arg(long)]
    #[serde(skip)]
    env: Vec<String>,
}

/// What can be done to a batch job (a proof job is the scheduler's and its client's).
#[derive(Subcommand)]
enum BatchJobCmd {
    /// Queue a batch job: fleetctl batch submit --app film-gpu --gpus 1 --mount /data:ro -- python3 train.py
    Submit {
        #[command(flatten)]
        submit: BatchSubmission,
    },
    /// Batch container logs: the last lines, then with -f everything new until it ends.
    Logs {
        id: String,
        #[arg(short, long)]
        follow: bool,
        #[arg(short = 'n', long, default_value_t = 50)]
        lines: usize,
    },
    /// Stop a batch job (SIGTERM, SIGKILL its profile's stop_timeout_s later; its cards freed).
    Cancel { id: String },
    /// Stop a batch job as a service needing its cards would: it queues again (same id and
    /// out/, FLEET_ATTEMPT counting on) and spends no --retry.
    Preempt { id: String },
    /// Delete a finished batch job's record (out/ included), or every finished batch job older than --older-than days.
    Purge {
        id: Option<String>,
        #[arg(long)]
        older_than: Option<f64>,
    },
}

#[derive(Subcommand)]
enum JobCmd {
    /// Fetch a proof job's proof.json and public_values.bin into --out DIR.
    Fetch {
        id: String,
        #[arg(long)]
        out: PathBuf,
    },
}

#[derive(Args)]
struct AddProofApp {
    /// TENANT/<full ELF SHA-256>, or TENANT/ with --elf FILE.
    name: String,
    #[arg(long)]
    elf: Option<PathBuf>,
    #[arg(long, allow_hyphen_values = true)]
    priority: Option<i32>,
}

#[derive(Args)]
#[command(args_conflicts_with_subcommands = true)]
struct BatchArgs {
    #[command(subcommand)]
    cmd: Option<BatchCmd>,
    /// A batch profile: its settings and newest jobs. Without one, every profile;
    /// the jobs themselves are `fleetctl job --type batch`.
    name: Option<String>,
    #[command(flatten)]
    conn: ConnectionOptions,
}

#[derive(Subcommand)]
enum BatchCmd {
    /// Register a reusable batch execution profile in an existing namespace.
    Add { #[command(flatten)] add: AddBatch },
    /// The profiles, as plain `fleetctl batch` lists them.
    #[command(name = "list", hide = true)]
    Profiles,
    /// A profile and its newest jobs, as `fleetctl batch NAME` shows them.
    #[command(name = "show", hide = true)]
    Profile { name: String },
    /// Stop accepting new jobs for this profile; accepted jobs continue.
    Disable { name: String },
    /// Accept new jobs for this profile again.
    Enable { name: String },
    /// App owners: set the priority inherited by new jobs.
    Priority { name: String, #[arg(allow_hyphen_values = true)] priority: i32 },
    /// Delete a profile after all accepted jobs finish; keep job history.
    Delete { name: String, #[arg(long)] yes: bool },
    #[command(flatten)]
    Job(BatchJobCmd),
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
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()))
        .with_target(false)
        .without_time()
        .init();
    let cli = Cli::parse();
    let out = OutputWriter { json: cli.json };
    match &cli.cmd {
        Cmd::Init(i) => init(i, &out),
        Cmd::Job(j) => job(j, &out),
        Cmd::App(a) => app(a, &out),
        Cmd::Batch(b) => batch(b, &out),
        other => admin(other, &out),
    }
}

impl Cmd {
    /// The connection of a command that talks to the scheduler.
    fn conn(&self) -> &ConnectionOptions {
        match self {
            Cmd::Node(n) => &n.conn,
            Cmd::Token(c) => &c.conn,
            Cmd::Namespace(n) => &n.conn,
            Cmd::Init(_) | Cmd::Job(_) | Cmd::App(_) | Cmd::Batch(_) => unreachable!("not the scheduler's admin API alone"),
        }
    }
}

struct FleetClient {
    /// Authenticated HTTP client shared by administrator and scoped tenant tokens.
    /// The base selects the control-plane root or proof API prefix.
    base: String,
    token: String,
    http: reqwest::blocking::Client,
    namespace: Option<String>,
    tag: Option<String>,
}

impl FleetClient {
    fn new(conn: &ConnectionOptions) -> anyhow::Result<Self> {
        let mut client = Self::control(conn)?;
        client.base.push_str("/admin");
        Ok(client)
    }

    fn control(conn: &ConnectionOptions) -> anyhow::Result<Self> {
        let mut client = Self::at(conn.url.trim_end_matches('/'),
            &conn.token_file.clone().or_else(|| conn.admin_token_file.clone()))?;
        client.namespace = conn.namespace.clone();
        client.tag = conn.tag.clone().filter(|t| !t.is_empty());
        Ok(client)
    }

    fn at(base: &str, token_file: &Option<PathBuf>) -> anyhow::Result<Self> {
        let path = match token_file {
            Some(p) => p.clone(),
            None => default_admin_token()
                .context("no token: pass --token-file, --admin-token-file or set FLEET_TOKEN_FILE/FLEET_ADMIN_TOKEN_FILE")?,
        };
        let token = std::fs::read_to_string(&path)
            .with_context(|| format!("reading {}", path.display()))?
            .trim()
            .to_string();
        Ok(Self {
            base: base.to_string(),
            namespace: None,
            tag: None,
            token,
            http: reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(60))
                .build()?,
        })
    }

    fn req(&self, method: reqwest::Method, path: &str) -> reqwest::blocking::RequestBuilder {
        let request = self.http.request(method, format!("{}{path}", self.base)).bearer_auth(&self.token);
        let request = match &self.namespace { Some(n) => request.query(&[("namespace", n)]), None => request };
        match &self.tag { Some(g) => request.query(&[("tag", g)]), None => request }
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

fn admin(cmd: &Cmd, out: &OutputWriter) -> anyhow::Result<()> {
    let a = FleetClient::new(cmd.conn())?;
    use reqwest::Method as M;
    match cmd {
        Cmd::Node(NodeArgs { cmd: None, name: Some(name), .. }) => {
            let v = a.get("/status")?;
            let Some(mut n) = node_rows(&v).into_iter().find(|n| n["name"].as_str() == Some(name.as_str())) else {
                bail!("no node {name} (fleetctl node lists them)");
            };
            n.as_object_mut().map(|o| o.remove("state")); // the list's READY column; the record has ready / draining
            out.record(&n);
        }
        Cmd::Node(NodeArgs { cmd: None | Some(NodeCmd::List), .. }) => {
            let v = a.get("/status")?;
            out.table(&node_rows(&v), NODE_COLS, Some(&v["nodes"]));
        }
        Cmd::Node(NodeArgs { cmd: Some(NodeCmd::Drain { name }), .. }) => {
            a.send(a.req(M::POST, &format!("/nodes/{name}/drain")))?;
            out.done(&format!("draining {name}: it takes no new proof or batch jobs; active jobs finish"),
                     json!({ "node": name, "draining": true }));
        }
        Cmd::Node(NodeArgs { cmd: Some(NodeCmd::Undrain { name }), .. }) => {
            a.send(a.req(M::DELETE, &format!("/nodes/{name}/drain")))?;
            out.done(&format!("{name} takes jobs again"), json!({ "node": name, "draining": false }));
        }
        Cmd::Namespace(n) => match &n.cmd {
            None | Some(NamespaceCmd::List) => {
                let all = a.get("/namespaces")?;
                out.table(all.as_array().unwrap_or(&vec![]), &[col("NAME", "name"), col("ID", "id"), col("SUSPENDED", "suspended_ms")], Some(&all));
            }
            Some(NamespaceCmd::Create { name, tenant, quota, bundle }) => {
                let namespace = a.send(a.req(M::POST, "/namespaces").json(&json!({"name": name, "quota": quota.value()?})))?;
                issue_token(&a, cmd.conn(), out, name, Some("scope=default"), tenant, None, true, "owner", bundle)?;
                if !out.json { writeln!(std::io::stdout().lock(), "namespace {} is ready with scope=default and owner access", namespace["name"].as_str().unwrap_or(name))?; }
            }
            Some(NamespaceCmd::Quota { name, quota }) => out.record(&a.send(a.req(M::PUT, &format!("/namespaces/{name}/quota")).json(&quota.value()?))?),
            Some(NamespaceCmd::Get { name }) => out.record(&a.get(&format!("/namespaces/{name}"))?),
            Some(NamespaceCmd::Suspend { name }) => out.record(&a.send(a.req(M::PUT, &format!("/namespaces/{name}/suspended")))?),
            Some(NamespaceCmd::Resume { name }) => out.record(&a.send(a.req(M::DELETE, &format!("/namespaces/{name}/suspended")))?),
        },
        Cmd::Token(TokenArgs { cmd: Some(TokenCmd::Issue { tenant, apps, label, bundle }), .. }) => {
            let namespace = cmd.conn().namespace.as_deref().context("token issue requires --namespace (create it with fleetctl namespace create NAME)")?;
            issue_token(&a, cmd.conn(), out, namespace, a.tag.as_deref(), tenant, Some(apps), false, label, bundle)?;
        }
        Cmd::Token(TokenArgs { cmd: Some(TokenCmd::Refresh { id, bundle }), .. }) => {
            let all = a.get("/tokens")?;
            let old = all.as_array().into_iter().flatten().find(|c| c["id"].as_str() == Some(&id))
                .context("no such token")?;
            let namespace = old["namespace"].as_str().context("token refresh requires an explicit namespace")?;
            let apps: Option<Vec<String>> = if old["apps"].is_null() { None } else { Some(serde_json::from_value(old["apps"].clone())?) };
            issue_token(&a, cmd.conn(), out, namespace, old["tag"].as_str(), old["tenant"].as_str().context("token has no tenant")?, apps.as_deref(),
                old["manage_apps"].as_bool().unwrap_or(false), old["label"].as_str().unwrap_or_default(), bundle)?;
            if !out.json { writeln!(std::io::stdout().lock(), "old token {id} remains active; revoke it after verifying the replacement")?; }
        }
        Cmd::Token(TokenArgs { cmd: None, id: Some(id), .. }) => {
            let v = a.get("/tokens")?;
            let Some(c) = token_rows(&v).into_iter().find(|c| c["id"].as_str() == Some(id.as_str())) else {
                bail!("no token {id} (fleetctl token lists them)");
            };
            out.record(&c);
        }
        Cmd::Token(TokenArgs { cmd: None | Some(TokenCmd::List), .. }) => {
            let v = a.get("/tokens")?;
            out.table(&token_rows(&v), TOKEN_COLS, Some(&v));
        }
        Cmd::Token(TokenArgs { cmd: Some(TokenCmd::Revoke { id }), .. }) => {
            a.send(a.req(M::DELETE, &format!("/tokens/{id}")))?;
            out.done(&format!("revoked token {id}"), json!({ "revoked": id }));
        }
        Cmd::Init(_) | Cmd::Job(_) | Cmd::App(_) | Cmd::Batch(_) => unreachable!("not the scheduler's admin API alone"),
    }
    Ok(())
}

fn issue_token(a: &FleetClient, conn: &ConnectionOptions, out: &OutputWriter, namespace: &str, tag: Option<&str>, tenant: &str, apps: Option<&[String]>, manage_apps: bool, label: &str, bundle: &TokenBundle) -> anyhow::Result<()> {
    let TokenBundle { out: env_file, endpoint, domain_name, server_ca } = bundle;
    let token_file = env_file.with_extension("token");
    let ca_file = env_file.with_extension("ca.pem");
    let mut bundle = vec![env_file.clone(), token_file.clone()];
    let trust_file = server_ca.clone();
    if trust_file.is_some() { bundle.push(ca_file.clone()); }
    for (i, path) in bundle.iter().enumerate() {
        if bundle[..i].contains(path) { bail!("--out must use a distinct env filename, such as user.env"); }
        if path.exists() { bail!("{} exists; refusing to overwrite a token", path.display()); }
    }
    use sha2::Digest;
    let mut random = [0u8; 48];
    rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut random);
    let user_token = hex::encode(random);
    let mut id_bytes = [0u8; 16];
    rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut id_bytes);
    let id = hex::encode(id_bytes);
    let token_hash = hex::encode(sha2::Sha256::digest(user_token.as_bytes()));
    let key = k256::ecdsa::SigningKey::random(&mut rand::rngs::OsRng);
    let address = alloy_primitives::Address::from_private_key(&key);
    let registered = a.send(a.req(reqwest::Method::POST, "/tokens").json(&json!({
        "id": id, "tenant": tenant, "address": format!("{address:#x}"), "label": label,
        "apps": apps, "manage_apps": manage_apps, "token_hash": token_hash, "namespace": namespace, "tag":tag,
    })))?;
    write_secret(&token_file, user_token.as_bytes())?;
    let mut env = vec![
        ("FLEET_TOKEN_FILE", abs(&token_file)?),
        ("FLEET_NAMESPACE", namespace.to_string()),
        ("FLEET_TAG", tag.unwrap_or_default().to_string()),
        ("FLEET_URL", conn.url.clone()),
        ("ZKM_PROVER", "network".to_string()),
        ("ENDPOINT", endpoint.clone()),
        ("ZKM_PRIVATE_KEY", hex::encode(key.to_bytes())),
        ("ZKM_PROOF_POLL_INTERVAL", "100".to_string()),
    ];
    if let Some(trust) = trust_file {
        let trust_bytes = std::fs::read(&trust)
            .with_context(|| format!("reading server CA {}", trust.display()))?;
        write_secret(&ca_file, &trust_bytes)?;
        env.push(("CA_CERT_PATH", abs(&ca_file)?));
    }
    if let Some(d) = domain_name {
        env.push(("DOMAIN_NAME", d.clone()));
    }
    let text: String = env.iter().map(|(k, v)| format!("{k}='{}'\n", v.replace('\'', "'\\''"))).collect();
    write_secret(env_file, text.as_bytes())?;
    let result = json!({
        "id": id, "tenant": tenant, "label": label,
        "env_file": env_file.display().to_string(), "token_file": token_file.display().to_string(), "endpoint": endpoint,
        "registered": registered,
    });
    if out.json {
        out.record(&result);
    } else {
        writeln!(std::io::stdout().lock(), "issued token {id} for tenant {tenant}; client env in {} (0600)", env_file.display())?;
    }
    Ok(())
}

const NODE_COLS: &[out::TableColumn] = &[
    col("NODE", "name"),
    col("BACKEND", "backend"),
    col("GPUS", "gpus"),
    col("CPUS", "cpus"),
    col("FREE GPUS", "free_gpus"),
    col("FREE CPUS", "free_cpus"),
    col("MACHINE", "machine"),
    col("READY", "state"),
    col("BUSY WITH", "busy"),
    col("HEARTBEAT", "heartbeat_age_ms"),
];
const TOKEN_COLS: &[out::TableColumn] =
    &[col("TOKEN ID", "id"), col("NAMESPACE","namespace"), col("TAG","tag"), col("TENANT", "tenant"), col("LABEL", "label"), col("STATE", "state"), col("CREATED", "created_ms")];
const PURGE_COLS: &[out::TableColumn] =
    &[col("RUN", "id"), col("PURGED", "purged"), col("STATE", "state"), col("SIZE", "bytes"), col("REASON", "reason")];

/// Tokens as rows: a STATE column (active / revoked); proof identity stays internal.
fn token_rows(v: &Value) -> Vec<Value> {
    v.as_array()
        .into_iter()
        .flatten()
        .map(|c| {
            let mut r = c.clone();
            r["tag"] = c["tag"].clone();
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

const PS_COLS: &[out::TableColumn] = &[
    col("TYPE", "type"),
    col("NAME", "app"),
    col("NAMESPACE", "namespace"),
    col("PRIORITY", "priority"),
    col("TAG", "tag"),
    col("ID", "id"),
    col("STATE", "state"),
    col("NODE", "where"),
    col("CREATED", "created_at"),
    col("TOOK", "took_ms"),
];

/// States that are not over yet, in both vocabularies.
const ACTIVE: &[&str] = &["queued", "running", "waiting_vram", "waiting_dependency", "waiting_quota", "retrying"];

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
        "type": "app", "app": proof_app(&j["tenant"], &j["elf_id"]), "id": j["id"], "client_id": j["client_id"], "namespace": j["namespace"], "tag": j["tag"], "owner": j["tenant"],
        "state": j["state"], "priority": j["priority"], "where": j["node"],
        "created_at": secs("created_ms"), "started_at": secs("started_ms"), "finished_at": secs("finished_ms"),
        "took_ms": took_ms(secs("started_ms"), secs("finished_ms")), "record": j,
    })
}

/// A batch job as a `ps` row.
fn batch_row(r: &Value) -> Value {
    json!({
        "type": "batch", "app": r["profile"], "id": r["id"], "namespace": r["namespace"], "tag": r["tag"], "owner": Value::Null, "state": r["state"], "priority": r["priority"],
        "where": r["node"],
        "created_at": r["submitted_at"], "started_at": r["started_at"], "finished_at": r["finished_at"],
        "took_ms": took_ms(r["started_at"].as_f64(), r["finished_at"].as_f64()), "record": with_type("batch", r),
    })
}

/// An app by the name `fleetctl app` shows: a proof app is `tenant/<elf id prefix>`
/// (`?` for no tenant) or `(rejected)`; any other name is a container workload.
enum AppRef {
    Proof { tenant: String, elf: String },
    Container(String),
}

impl AppRef {
    fn parse(name: &str) -> Self {
        if name == "(rejected)" {
            return AppRef::Proof { tenant: String::new(), elf: String::new() };
        }
        match name.split_once('/') {
            Some((t, e)) => AppRef::Proof { tenant: if t == "?" { String::new() } else { t.into() }, elf: e.into() },
            None => AppRef::Container(name.into()),
        }
    }
}

/// Which jobs to fetch from one side.
struct JobListFilter<'a> {
    limit: usize,
    active: bool,
    tenant: Option<&'a str>,
    app: Option<&'a str>,
}

/// The newest proof and batch jobs that `want` asks for, as list rows (none when the
/// filter can only match the other type).
fn fetch_rows(kind: JobType, conn: &ConnectionOptions, want: &JobListFilter) -> anyhow::Result<Vec<Value>> {
    let app = want.app.map(AppRef::parse);
    let mut q: Vec<(&str, String)> = vec![("limit", want.limit.to_string())];
    if want.active {
        q.push(("active", "true".into()));
    }
    let (v, row): (Value, fn(&Value) -> Value) = match (kind, app) {
        (JobType::App, Some(AppRef::Container(_))) | (JobType::Batch, Some(AppRef::Proof { .. })) => return Ok(vec![]),
        (JobType::Batch, _) if want.tenant.is_some() => return Ok(vec![]), // batch jobs belong to no tenant
        (JobType::App, app) => {
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
            (FleetClient::new(conn)?.get_q("/jobs", &q)?, proof_row)
        }
        (JobType::Batch, app) => {
            if let Some(AppRef::Container(name)) = &app {
                q.push(("profile", name.clone()));
            }
            (FleetClient::control(conn)?.get_q("/batches", &q)?, batch_row)
        }
    };
    let rows = v.as_array().into_iter().flatten().map(row);
    // an app name is exact (a proof app's elf part is a prefix on the server)
    Ok(rows
        .filter(|r| want.app.is_none_or(|a| r["app"].as_str() == Some(a)) && (!want.active || is_active(r)))
        .collect())
}

/// Every job of both types still queued or running, then the newest `-n` of each type.
fn list(p: &ListArgs, conn: &ConnectionOptions, out: &OutputWriter) -> anyhow::Result<()> {
    let kinds: Vec<JobType> = match p.kind {
        Some(k) => vec![k],
        None => vec![JobType::App, JobType::Batch],
    };
    let mut rows: Vec<Value> = Vec::new();
    let mut reached = 0;
    for k in &kinds {
        let want = |limit, active| JobListFilter { limit, active, tenant: p.tenant.as_deref(), app: p.app.as_deref() };
        // the active ones however old (a long batch job outlives many short proofs), then the newest
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
            Err(e) => tracing::warn!(error=%format!("{e:#}"), api=if *k == JobType::App { "scheduler" } else { "batch" }, "API not reachable; listing the rest"),
        }
    }
    if reached == 0 {
        bail!("neither the scheduler nor the batch API answered");
    }
    let created = |r: &Value| r["created_at"].as_f64().unwrap_or(0.0);
    rows.sort_by(|a, b| is_active(b).cmp(&is_active(a)).then(created(b).total_cmp(&created(a))));
    out.table(&rows, PS_COLS, None);
    Ok(())
}

const APP_COLS: &[out::TableColumn] = &[
    col("TYPE", "type"),
    col("NAME", "name"),
    col("NAMESPACE", "namespace"),
    col("PRIORITY", "priority"),
    col("TAG", "tag"),
    col("STATE", "state"),
    col("GPUS", "/resources/gpus"),
    col("CPUS", "/resources/cpus"),
    col("NODE", "/resources/nodes"),
    col("LAST JOB", "last_at"),
    col("DETAIL", "detail"),
];

/// The one proof app a name means: as the list names it, or `tenant/<full elf id>`.
fn proof_app_named(a: &FleetClient, name: &str, tenant: &str) -> anyhow::Result<Value> {
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
        [] => bail!("no app {name} (fleetctl app lists them)"),
        [one] => Ok((*one).clone()),
        many => bail!(
            "{name} matches {} registrations; select --namespace NAME and give the full elf id: {}",
            many.len(),
            many.iter().map(|x| format!("{}/{}", tenant, x["elf_id"].as_str().unwrap_or(""))).collect::<Vec<_>>().join(", ")
        ),
    }
}

/// A long-running serving deployment from the control plane.
fn serving_app_named(a: &FleetClient, name: &str) -> anyhow::Result<Value> {
    let apps = a.get("/apps")?;
    apps.as_array()
        .into_iter()
        .flatten()
        .find(|x| x["name"].as_str() == Some(name) && x["persistent"].as_bool() == Some(true) && x["removed"].as_bool() != Some(true))
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("no serving app {name}; use fleetctl batch profile for a finite workload"))
}

/// Disables or enables an app.
fn set_app_disabled(name: &str, disabled: bool, conn: &ConnectionOptions, out: &OutputWriter) -> anyhow::Result<()> {
    let method = if disabled { reqwest::Method::PUT } else { reqwest::Method::DELETE };
    let v = match AppRef::parse(name) {
        AppRef::Proof { tenant, .. } => {
            let a = FleetClient::new(conn)?;
            let app = proof_app_named(&a, name, &tenant)?;
            let mut manager = FleetClient::control(conn)?;
            // The app's canonical namespace is sent explicitly below. Do not append
            // the caller's selector a second time to this lifecycle request.
            manager.namespace = None;
            manager.tag = None;
            let key = [
                ("namespace", app["namespace"].as_str().context("proof app has no namespace")?),
                ("tenant", app["tenant"].as_str().unwrap_or("")),
                ("elf_id", app["elf_id"].as_str().unwrap_or("")),
            ];
            manager.send(manager.req(method, "/proof-apps/disabled")
                .query(&key).timeout(Duration::from_secs(1800)))?
        }
        AppRef::Container(_) => {
            let a = FleetClient::control(conn)?;
            serving_app_named(&a, name)?;
            a.send(a.req(method, &format!("/apps/{name}/disabled")).timeout(Duration::from_secs(1800)))?
        }
    };
    let text = if v["persistent"].as_bool() == Some(true) {
        if disabled { format!("{name} disabled: stopped; GPU reservations released") }
        else { format!("{name} enabled: service started") }
    } else if v["servers"].is_array() {
        if disabled { format!("{name} disabled: active proofs drained, dedicated GPU server stopped") }
        else { format!("{name} enabled: dedicated GPU server started") }
    } else if disabled {
        format!("{name} disabled: it takes no new jobs; the queued and running ones finish")
    } else {
        format!("{name} enabled: it takes jobs again")
    };
    out.done(&text, v);
    Ok(())
}

/// Changes app policy; accepted jobs retain their inherited priority.
fn set_app_priority(name: &str, priority: i32, conn: &ConnectionOptions, out: &OutputWriter) -> anyhow::Result<()> {
    let v = match AppRef::parse(name) {
        AppRef::Proof { tenant, .. } => {
            let mut a = FleetClient::new(conn)?;
            let app = proof_app_named(&a, name, &tenant)?;
            a.namespace = app["namespace"].as_str().map(str::to_owned);
            a.send(a.req(reqwest::Method::PUT, "/apps/priority").query(&[
                ("tenant", app["tenant"].as_str().unwrap_or("")),
                ("elf_id", app["elf_id"].as_str().unwrap_or("")),
                ("priority", &priority.to_string()),
            ]))?
        }
        AppRef::Container(_) => {
            let a = FleetClient::control(conn)?;
            serving_app_named(&a, name)?;
            a.send(a.req(reqwest::Method::PUT, &format!("/apps/{name}/priority")).json(&json!({"priority":priority})))?
        }
    };
    out.done(&format!("{name}: priority {priority}; new jobs inherit this priority"), v);
    Ok(())
}

/// The control client, URL path and query for `leaf` ("scale", "logs") of a proof or serving app.
type AppEndpoint = (FleetClient, String, Vec<(&'static str, String)>);
fn app_endpoint(name: &str, leaf: &str, conn: &ConnectionOptions) -> anyhow::Result<AppEndpoint> {
    let mut a = FleetClient::control(conn)?;
    Ok(match AppRef::parse(name) {
        AppRef::Proof { tenant, .. } => {
            let proof = proof_app_named(&FleetClient::new(conn)?, name, &tenant)?;
            a.namespace = proof["namespace"].as_str().map(str::to_owned);
            let query = vec![("tenant", tenant), ("elf_id", proof["elf_id"].as_str().unwrap_or("").to_string())];
            (a, format!("/proof-apps/{leaf}"), query)
        }
        AppRef::Container(_) => {
            let app = serving_app_named(&a, name)?;
            a.namespace = app["namespace"].as_str().map(str::to_owned);
            (a, format!("/apps/{name}/{leaf}"), vec![])
        }
    })
}

fn app_logs(name: &str, lines: u32, node: Option<&str>, conn: &ConnectionOptions, out: &OutputWriter) -> anyhow::Result<()> {
    let (a, path, mut query) = app_endpoint(name, "logs", conn)?;
    query.push(("lines", lines.to_string()));
    if let Some(node) = node { query.push(("node", node.to_string())); }
    let response = a.req(reqwest::Method::GET, &path).query(&query).send()?;
    let status = response.status();
    let body = response.bytes()?;
    if !status.is_success() {
        bail!("{status}: {}", String::from_utf8_lossy(&body));
    }
    if out.json {
        out.line(&json!({"data": String::from_utf8_lossy(&body)}));
    } else {
        std::io::stdout().write_all(&body)?;
    }
    Ok(())
}

fn scale_app(name: &str, gpus: Option<f64>, cpus: Option<u32>, replicas: Option<u32>, node: Option<&str>, conn: &ConnectionOptions, out: &OutputWriter) -> anyhow::Result<()> {
    let mut body = json!({});
    if let Some(v) = gpus {
        if !v.is_finite() || v < 0.0 { bail!("--gpus must be finite and nonnegative"); }
        body["gpus"] = json!(v);
    }
    if let Some(v) = cpus { body["cpus"] = json!(v); }
    if let Some(v) = replicas { body["replicas"] = json!(v); }
    let write = !body.as_object().unwrap().is_empty();
    if let Some(node) = node { body["node"] = json!(node); }
    let method = if write { reqwest::Method::PUT } else { reqwest::Method::GET };
    let (a, path, mut query) = app_endpoint(name, "scale", conn)?;
    if !write { if let Some(node)=node { query.push(("node",node.to_string())); } }
    let mut request=a.req(method,&path).query(&query);
    if write { request=request.json(&body); }
    let v=a.send(request)?;
    out.done(&format!("{name}: {} — {} replicas, {} GPUs and {} CPUs per replica on {}",
        v["state"].as_str().unwrap_or("requested"), v["replicas"], v["gpus"], v["cpus"], v["node"].as_str().unwrap_or("?")),v);
    Ok(())
}

/// Register a proof program with its long-running dedicated prover.
fn add_app(add: &AddProofApp, conn: &ConnectionOptions, out: &OutputWriter) -> anyhow::Result<()> {
    if conn.namespace.as_deref().is_none_or(str::is_empty) {
        bail!("app creation requires --namespace NAME; an administrator must create the namespace first");
    }
    let AppRef::Proof { tenant, elf } = AppRef::parse(&add.name) else {
        bail!("proof registration requires TENANT/<elf id>; configure serving deployments in services.yaml and finite profiles with fleetctl batch add");
    };
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
    let namespace = conn.namespace.as_deref().context("proof app creation requires --namespace")?;
    let mut manager = FleetClient::control(conn)?;
    manager.namespace = None;
    manager.tag = None;
    let _server = manager.get_q("/proof-apps/available", &[
        ("namespace", namespace.to_string()), ("tenant", tenant.clone()), ("elf_id", elf_id.clone()),
    ])?;
    let a = FleetClient::new(conn)?;
    let v = a.send(a.req(reqwest::Method::POST, "/apps").query(&[("tenant", tenant.as_str()), ("elf_id", elf_id.as_str()), ("priority", &add.priority.unwrap_or(0).to_string())]))?;
    out.done(
        &format!("added proof app {}: tenant {tenant}, program {elf_id} (its jobs are accepted from now on{})",
                 v["app"].as_str().unwrap_or(&add.name),
                 if v["elf_stored"].as_bool() == Some(true) { "" } else { "; the ELF itself arrives with the first one" }),
        v,
    );
    Ok(())
}

fn add_batch(add: &AddBatch, conn: &ConnectionOptions, out: &OutputWriter) -> anyhow::Result<()> {
    if conn.namespace.as_deref().is_none_or(str::is_empty) && conn.token_file.is_none() {
        bail!("batch profile creation requires --namespace NAME; an administrator must create the namespace first");
    }
    if add.name.contains('/') { bail!("a batch profile name cannot contain /"); }
    let mut body = serde_json::to_value(add)?;
    let mut env = serde_json::Map::new();
    for kv in &add.env {
        let Some((k, v)) = kv.split_once('=') else { bail!("--env {kv}: K=V") };
        env.insert(k.into(), json!(v));
    }
    if !env.is_empty() {
        body["env"] = Value::Object(env);
    }
    let a = FleetClient::control(conn)?;
    let v = a.send(a.req(reqwest::Method::POST, "/batch-profiles").json(&body))?;
    let gpus = v["max_gpus"].as_u64().unwrap_or(0);
    out.done(
        &format!("added batch profile {}: image {}, up to {gpus} GPU{}, mounts under {}", add.name,
                 v["image"].as_str().unwrap_or("?"), if gpus == 1 { "" } else { "s" },
                 v["allow"].as_array().map(|r| r.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(", ")).unwrap_or_default()),
        v,
    );
    Ok(())
}

/// One app's record, and its newest jobs.
fn describe_app(name: &str, conn: &ConnectionOptions, out: &OutputWriter) -> anyhow::Result<()> {
    let (kind, mut rec) = match AppRef::parse(name) {
        AppRef::Proof { tenant, .. } => (JobType::App, with_type("app", &proof_app_named(&FleetClient::new(conn)?, name, &tenant)?)),
        AppRef::Container(_) => {
            let app = serving_app_named(&FleetClient::control(conn)?, name)?;
            (JobType::Batch, with_type("app", &app))
        },
    };
    let full = rec["name"].as_str().unwrap_or(name).to_string();
    let jobs = if kind == JobType::App { fetch_rows(kind, conn, &JobListFilter { limit: 5, active: false, tenant: None, app: Some(&full) })? } else { vec![] };
    if out.json {
        rec["newest_jobs"] = Value::Array(jobs);
        out.record(&rec);
    } else {
        out.record(&rec);
        if !jobs.is_empty() {
            writeln!(std::io::stdout().lock())?;
            out.table(&jobs, PS_COLS, None);
        }
    }
    Ok(())
}

/// Deletes an app (after showing what goes with it, until --yes).
fn delete_app(name: &str, yes: bool, conn: &ConnectionOptions, out: &OutputWriter) -> anyhow::Result<()> {
    use reqwest::Method as M;
    match AppRef::parse(name) {
        AppRef::Proof { tenant, .. } => {
            let mut a = FleetClient::new(conn)?;
            let app = proof_app_named(&a, name, &tenant)?;
            a.namespace = app["namespace"].as_str().map(str::to_string);
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
        AppRef::Container(_) => {
            let a = FleetClient::control(conn)?;
            let app = serving_app_named(&a, name)?;
            if !yes {
                bail!(
                    "would remove serving app {name}; its {} past execution record(s) \
                     stay ({} queued or running: refused while any is). Pass --yes to delete",
                    app["jobs"], app["active"]
                );
            }
            let v = a.send(a.req(M::DELETE, &format!("/apps/{name}")))?;
            out.done(&format!("deleted serving app {name}; its {} past execution record(s) stay", v["jobs_kept"]), v);
        }
    }
    Ok(())
}

/// Long-running apps, most recently used first.
fn app(p: &AppArgs, out: &OutputWriter) -> anyhow::Result<()> {
    match &p.cmd {
        Some(AppCmd::Delete { name, yes }) => return delete_app(name, *yes, &p.conn, out),
        Some(AppCmd::Disable { name }) => return set_app_disabled(name, true, &p.conn, out),
        Some(AppCmd::Enable { name }) => return set_app_disabled(name, false, &p.conn, out),
        Some(AppCmd::Add { add }) => return add_app(add, &p.conn, out),
        Some(AppCmd::Priority { name, priority }) => return set_app_priority(name, *priority, &p.conn, out),
        Some(AppCmd::Logs { name, lines, node }) => return app_logs(name, *lines, node.as_deref(), &p.conn, out),
        Some(AppCmd::Scale { name, gpus, cpus, replicas, node }) => return scale_app(name, *gpus, *cpus, *replicas, node.as_deref(), &p.conn, out),
        None => {}
    }
    if let Some(name) = &p.name {
        return describe_app(name, &p.conn, out);
    }
    let mut apps: Vec<Value> = Vec::new();
    let mut reached = 0;
    match FleetClient::new(&p.conn).and_then(|a| a.get("/apps")) {
        Ok(v) => {
            reached += 1;
            apps.extend(v.as_array().into_iter().flatten().map(|a| with_type("app", a)));
        }
        Err(e) => tracing::warn!(error=%format!("{e:#}"), "scheduler not reachable; listing the rest"),
    }
    match FleetClient::control(&p.conn).and_then(|a| a.get("/apps")) {
        Ok(v) => {
            reached += 1;
            apps.extend(v.as_array().into_iter().flatten()
                .filter(|a| a["persistent"].as_bool() == Some(true)).map(|a| with_type("app", a)));
        }
        Err(e) => tracing::warn!(error=%format!("{e:#}"), "control API not reachable; listing the rest"),
    }
    if reached == 0 {
        bail!("neither proof nor serving app API answered");
    }
    let last_at = |a: &Value| a["last_created_ms"].as_f64().map(|ms| ms / 1000.0)
        .or_else(|| a["last_submitted_at"].as_f64());
    apps.sort_by(|a, b| last_at(b).unwrap_or(0.0).total_cmp(&last_at(a).unwrap_or(0.0)));
    let rows: Vec<Value> = apps.iter().map(|a| {
        let mean = a["mean_prove_ms"].as_f64().map(|m| format!("mean prove {:.1} s", m / 1000.0));
        let anchored = a["anchored_on"].as_str().map(|n| format!("anchored on {n}"));
        let image = a["image"].as_str().map(|i| format!("image {i}"));
        // Why a starting app is not running yet comes first.
        let why = a["detail"].as_str().filter(|d| !d.is_empty()).map(str::to_owned);
        let detail: Vec<String> = [why, anchored, mean, image].into_iter().flatten().collect();
        json!({
            "type": "app", "name": a["name"], "namespace": a["namespace"],
            "tag": a.get("tags").filter(|v| v.as_array().is_some_and(|tags| !tags.is_empty())).unwrap_or(&a["tag"]),
            "state": a["state"], "priority": a["priority"], "resources": a["resources"],
            "last_at": last_at(a), "detail": detail.join(", "),
        })
    }).collect();
    out.table(&rows, APP_COLS, Some(&Value::Array(apps)));
    Ok(())
}

fn batch_profile_named(a: &FleetClient, name: &str) -> anyhow::Result<Value> {
    let profile = a.get(&format!("/batch-profiles/{name}"))?;
    if profile["persistent"].as_bool() == Some(true) {
        bail!("{name} is a long-running app; use fleetctl app");
    }
    Ok(with_type("batch", &profile))
}

fn batch(p: &BatchArgs, out: &OutputWriter) -> anyhow::Result<()> {
    match &p.cmd {
        Some(BatchCmd::Job(cmd)) => return batch_action(cmd, &p.conn, out),
        Some(BatchCmd::Add { add }) => return add_batch(add, &p.conn, out),
        Some(BatchCmd::Profiles) => return batch_profiles(&p.conn, out),
        Some(BatchCmd::Profile { name }) => return batch_profile(name, &p.conn, out),
        Some(cmd) => {
            let a = FleetClient::control(&p.conn)?;
            let name = match cmd {
                BatchCmd::Disable { name } | BatchCmd::Enable { name }
                | BatchCmd::Priority { name, .. } | BatchCmd::Delete { name, .. } => name,
                _ => unreachable!(),
            };
            batch_profile_named(&a, name)?;
            let path = format!("/batch-profiles/{name}");
            let request = match cmd {
                BatchCmd::Disable { .. } => a.req(reqwest::Method::PUT, &format!("{path}/disabled")),
                BatchCmd::Enable { .. } => a.req(reqwest::Method::DELETE, &format!("{path}/disabled")),
                BatchCmd::Priority { priority, .. } => a.req(reqwest::Method::PUT, &format!("{path}/priority"))
                    .json(&json!({"priority": priority})),
                BatchCmd::Delete { yes, .. } => {
                    if !yes { bail!("would remove batch profile {name}; accepted jobs must finish first. Pass --yes to delete"); }
                    a.req(reqwest::Method::DELETE, &path)
                }
                _ => unreachable!(),
            };
            let value = a.send(request)?;
            out.done(&format!("updated batch profile {name}"), value);
            return Ok(());
        }
        None => {}
    }
    match &p.name {
        Some(name) => batch_profile(name, &p.conn, out),
        None => batch_profiles(&p.conn, out),
    }
}

/// The profiles, like `fleetctl app`: settings plus how many jobs are active and when the last began.
fn batch_profiles(conn: &ConnectionOptions, out: &OutputWriter) -> anyhow::Result<()> {
    let profiles = FleetClient::control(conn)?.get("/batch-profiles")?;
    // Every active job, however old, and enough recent ones to date each profile's last job.
    let jobs = fetch_rows(JobType::Batch, conn, &JobListFilter { limit: 1000, active: true, tenant: None, app: None })
        .and_then(|mut jobs| {
            jobs.extend(fetch_rows(JobType::Batch, conn, &JobListFilter { limit: 200, active: false, tenant: None, app: None })?);
            Ok(jobs)
        })
        .unwrap_or_else(|e| {
            tracing::warn!(error=%format!("{e:#}"), "batch jobs not reachable; listing profiles only");
            vec![]
        });
    let rows: Vec<Value> = profiles.as_array().into_iter().flatten().map(|profile| {
        let mut row = profile.clone();
        if row["state"].is_null() {
            row["state"] = json!(if profile["disabled"].as_bool() == Some(true) { "disabled" } else { "enabled" });
        }
        let mut ids = std::collections::HashSet::new();
        let own: Vec<&Value> = jobs.iter().filter(|j| j["app"] == profile["name"] && ids.insert(j["id"].to_string())).collect();
        row["active_jobs"] = json!(own.iter().filter(|j| is_active(j)).count());
        row["last_at"] = json!(own.iter().filter_map(|j| j["created_at"].as_f64()).reduce(f64::max));
        row
    }).collect();
    out.table(&rows, &[
        col("NAME", "name"), col("NAMESPACE", "namespace"),
        col("TAG", "tag"), col("STATE", "state"),
        col("PRIORITY", "priority"), col("MAX GPUS", "max_gpus"),
        col("ACTIVE", "active_jobs"), col("LAST JOB", "last_at"),
        col("IMAGE", "image"),
    ], None);
    Ok(())
}

fn batch_profile(name: &str, conn: &ConnectionOptions, out: &OutputWriter) -> anyhow::Result<()> {
    let mut profile = batch_profile_named(&FleetClient::control(conn)?, name)?;
    profile["newest_jobs"] = json!(fetch_rows(JobType::Batch, conn,
        &JobListFilter { limit: 5, active: false, tenant: None, app: Some(name) })?);
    out.record(&profile);
    Ok(())
}



/// `record` with its resource `type` field (a proof's own `kind` is its proof kind).
fn with_type(t: &str, record: &Value) -> Value {
    let mut m = serde_json::Map::new();
    if let Some(o) = record.as_object() {
        m.extend(o.clone());
    }
    m.insert("type".into(), json!(t));
    Value::Object(m)
}

/// An app or batch job, by id: the scheduler is asked first, then the batch API.
fn record(id: &str, conn: &ConnectionOptions, out: &OutputWriter) -> anyhow::Result<()> {
    let proof = FleetClient::new(conn).and_then(|a| a.get(&format!("/jobs/{id}")));
    if let Ok(v) = &proof {
        out.record(&with_type("app", v));
        return Ok(());
    }
    match FleetClient::control(conn).and_then(|a| a.get(&format!("/batches/{id}"))) {
        Ok(v) => {
            out.record(&with_type("batch", &v));
            Ok(())
        }
        Err(batch_error) => bail!(
            "no app or batch job {id} (scheduler: {:#}; batch API: {batch_error:#})",
            proof.err().unwrap_or_else(|| anyhow::anyhow!("-"))
        ),
    }
}

fn job(j: &JobArgs, out: &OutputWriter) -> anyhow::Result<()> {
    match (&j.cmd, &j.id) {
        (None, None) => list(&j.list, &j.conn, out),
        (None, Some(id)) => record(id, &j.conn, out),
        (Some(JobCmd::Fetch { id, out: dir }), _) => fetch_proof(id, dir, &j.conn, out),
    }
}

/// Download a retained proof through the control-plane relay.
fn fetch_proof(id: &str, dir: &Path, conn: &ConnectionOptions, out: &OutputWriter) -> anyhow::Result<()> {
    if dir.try_exists()? {
        bail!("output directory {} already exists; choose a new directory",dir.display());
    }
    let proof = FleetClient::new(conn)?;
    let mut files = Vec::new();
    for name in ["proof.json", "public_values.bin"] {
        let response = proof.req(reqwest::Method::GET,&format!("/jobs/{id}/output/{name}")).timeout(Duration::from_secs(75)).send()?;
        if !response.status().is_success() { bail!("{}: {}",response.status(),response.text()?); }
        files.push((name,response.bytes()?));
    }
    std::fs::create_dir(dir).with_context(||format!("creating {} (must not exist)",dir.display()))?;
    for (name,bytes) in files { std::fs::write(dir.join(name),bytes)?; }
    out.done(&format!("fetched proof output to {}",dir.display()),json!({"id":id,"out":dir}));
    Ok(())
}

fn batch_action(cmd: &BatchJobCmd, conn: &ConnectionOptions, out: &OutputWriter) -> anyhow::Result<()> {
    use reqwest::Method as M;
    let a = FleetClient::control(conn)?;
    match cmd {
        BatchJobCmd::Submit { submit } => {
            let body = serde_json::to_value(submit)?;
            let v = a.send(a.req(M::POST, "/batches").json(&body))?;
            out.record(&with_type("batch", &v));
        }
        BatchJobCmd::Cancel { id } => out.record(&a.send(a.req(M::POST, &format!("/batches/{id}/cancel")))?),
        BatchJobCmd::Preempt { id } => out.record(&a.send(a.req(M::POST, &format!("/batches/{id}/preempt")))?),
        BatchJobCmd::Purge { id, older_than } => {
            let path = match (id, older_than) {
                (Some(id), _) => format!("/batches/{id}"),
                (None, Some(days)) => format!("/batches?older_than={days}"),
                (None, None) => bail!("purge ID, or purge --older-than DAYS"),
            };
            let v = a.send(a.req(M::DELETE, &path))?;
            out.table(v.as_array().map(Vec::as_slice).unwrap_or_default(), PURGE_COLS, Some(&v));
        }
        BatchJobCmd::Logs { id, follow, lines } => {
            let mut chunk = a.get(&format!("/batches/{id}/logs?lines={lines}"))?;
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
                chunk = a.get(&format!("/batches/{id}/logs?offset={offset}"))?;
            }
        }
    }
    Ok(())
}

fn init(cmd: &InitCmd, out: &OutputWriter) -> anyhow::Result<()> {
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
                     scheduler: tls.cert_path={d}/server.pem tls.key_path={d}/server.key\n\
                     clients:   CA_CERT_PATH={d}/ca.pem (and DOMAIN_NAME={} if not \"stage\")",
                    names[0]
                ),
                v,
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
