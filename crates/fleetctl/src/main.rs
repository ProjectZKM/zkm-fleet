//! fleetctl: operate a zkm-fleet cluster and issue its tokens.
//!
//! Every command authenticates with one token: an administrator's (from `fleet-scheduler admin
//! issue` or `fleetctl token admin`) or a namespace's, which an administrator issues with the
//! permissions it needs. Tokens are written to 0600 files. The proof SDK still requires an
//! internal signing key; its address is bound to that token and is not a separate access resource.
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
    /// Control-plane HTTP base URL for every command.
    #[arg(long, env = "FLEET_URL", default_value = "http://127.0.0.1:9191", global = true)]
    url: String,
    /// API token file: an administrator's or a namespace's (default: admin.token in FLEET_ETC,
    /// ~/.config/zkm-fleet or /etc/zkm-fleet).
    #[arg(long, env = "FLEET_TOKEN_FILE", global = true)]
    token_file: Option<PathBuf>,
    /// Namespace selector (inferred from a namespace's token; administrators select one).
    #[arg(long, env = "FLEET_NAMESPACE", global = true)]
    namespace: Option<String>,
    /// Optional KEY=VALUE tag within the namespace; inferred from user tokens.
    #[arg(long = "tag", value_name = "KEY=VALUE", env = "FLEET_TAG", global = true)]
    tag: Option<String>,
}

#[derive(Subcommand)]
enum Cmd {
    /// Apps: registered proof programs and serving deployments.
    App(AppArgs),
    /// Finite batch workloads and their jobs.
    Batch(BatchArgs),
    /// Execution records: list or inspect app and batch jobs; fetch proof outputs.
    Job(JobArgs),
    /// Machine nodes: list their capacity, or drain / undrain a machine.
    Node(NodeArgs),
    /// Access tokens (administrators): list, inspect, issue, refresh or revoke.
    Token(TokenArgs),
    /// Inspect your namespace; administrators manage shared quotas and access.
    Namespace(NamespaceArgs),
    /// Set up a cluster's secrets, offline: the cluster token, the stage API's certificates
    /// (install.sh runs these; no scheduler is needed).
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
    /// Register a proof program as an app of the namespace: NAME --elf-id ID, or NAME --elf FILE.
    /// Serving deployments are initially configured in the node's services.yaml.
    Add {
        #[command(flatten)]
        add: AddApp,
    },
    /// Disable an app: its services drain and stop, releasing their GPUs.
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
    /// Change a proof or persistent app's settings. Image and environment changes require
    /// administrator credentials. Its service drains, stops,
    /// takes the new settings on its node and starts again, like a scale.
    Update {
        name: String,
        /// Container image.
        #[arg(long)] image: Option<String>,
        /// K=V; repeat. Replaces the app's whole environment.
        #[arg(long)] env: Vec<String>,
        /// /dev/shm size in GiB.
        #[arg(long)] shm_gb: Option<u32>,
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
    /// Delete an app, refused while any of its jobs is queued or running. A proof app goes
    /// with its jobs' records, inputs and proofs, its anchor, and its ELF when no other app
    /// uses it; serving deployments must be disabled first.
    /// Without --yes it only says what it would delete.
    Delete {
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
    /// Only the jobs of this app or batch.
    #[arg(long)]
    app: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum JobType {
    /// Jobs of apps: the proofs an app runs.
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
    /// Issue a token of the selected namespace (--namespace), scoped to approved apps and
    /// batches, and write the client bundle.
    Issue {
        /// Approved app or batch (repeat), by name. Required unless the token may manage its
        /// namespace.
        #[arg(long = "app")]
        apps: Vec<String>,
        /// What the token may do (repeat or comma-separate): read, submit, manage.
        #[arg(long = "permission", value_delimiter = ',', default_values_t = ["read".to_string(), "submit".to_string()])]
        permissions: Vec<String>,
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
    /// Issue another administrator's token, which may do everything, and write it to --out (0600).
    Admin {
        #[arg(long)]
        label: String,
        #[arg(long)]
        out: PathBuf,
    },
    /// Every registered token: namespace, permissions, label, active or revoked (the default).
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
    /// The batch to run under (its image, user, GPU cap and approved mount roots).
    #[serde(skip)]
    batch: String,
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

/// A batch to add. The command line and the request body are this one struct (its
/// field names are the batch's); unset flags take the server's defaults.
#[derive(Args, Serialize)]
struct AddBatch {
    /// Priority inherited by new jobs (default 0; higher values run first).
    #[arg(long, allow_hyphen_values = true)]
    #[serde(skip_serializing_if = "Option::is_none")]
    priority: Option<i32>,
    /// Batch name (letters, digits, _ . -); jobs run under it with fleetctl batch submit NAME.
    name: String,
    /// Container image the batch's jobs run in.
    #[arg(long)]
    image: String,
    /// UID[:GID] the container runs as (default: the image's); requires administrator credentials.
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
    /// Administrator-approved absolute mount root; repeat. Requires administrator credentials.
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
    /// Queue a batch job: fleetctl batch submit film-gpu --gpus 1 --mount /data:ro -- python3 train.py
    Submit {
        #[command(flatten)]
        submit: BatchSubmission,
    },
    /// Batch container logs: the last lines, then with -f everything new until it ends.
    Logs {
        id: String,
        /// The job's batch (found from the id when omitted).
        #[arg(long)]
        batch: Option<String>,
        #[arg(short, long)]
        follow: bool,
        #[arg(short = 'n', long, default_value_t = 50)]
        lines: usize,
    },
    /// Stop a batch job (SIGTERM, SIGKILL its batch's stop_timeout_s later; its cards freed).
    Cancel { id: String, #[arg(long)] batch: Option<String> },
    /// Stop a batch job as a service needing its cards would: it queues again (same id and
    /// out/, FLEET_ATTEMPT counting on) and spends no --retry.
    Preempt { id: String, #[arg(long)] batch: Option<String> },
    /// Delete a finished batch job's record (out/ included), or every finished batch job older than --older-than days.
    Purge {
        id: Option<String>,
        /// Only this batch's jobs (with --older-than), or the job's batch (found from the id when omitted).
        #[arg(long)]
        batch: Option<String>,
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

/// A proof app to register. The command line and the request body are this one struct (its
/// field names are the API's); unset flags take the server's defaults.
#[derive(Args, Serialize)]
struct AddApp {
    /// The app's name in the namespace: letters, digits, _ . -.
    name: String,
    /// The program's full ELF SHA-256.
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    elf_id: Option<String>,
    /// The ELF itself, to take its id from.
    #[arg(long)]
    #[serde(skip)]
    elf: Option<PathBuf>,
    /// latency: the biggest free prover first; batch (default): the smallest.
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    class: Option<String>,
    /// none (default); shared: keep the program on the prover it first ran on while healthy;
    /// exclusive: as shared, and that prover serves this app only.
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    anchor: Option<String>,
    /// A node backend that may prove it (repeat): cuda, cpu, mock; default any.
    #[arg(long = "backend")]
    #[serde(rename = "backends", skip_serializing_if = "Vec::is_empty")]
    backends: Vec<String>,
    /// Only provers of this circuit version prove it.
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    zkm_version: Option<String>,
    /// Jobs queued or running at most (default 64).
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    max_queued: Option<u32>,
    /// Priority inherited by new jobs (default 0; higher values run first).
    #[arg(long, allow_hyphen_values = true)]
    #[serde(skip_serializing_if = "Option::is_none")]
    priority: Option<i32>,
}

#[derive(Args)]
#[command(args_conflicts_with_subcommands = true)]
struct BatchArgs {
    #[command(subcommand)]
    cmd: Option<BatchCmd>,
    /// A batch: its settings and newest jobs. Without one, every batch (a deleted one while
    /// it still has jobs); the jobs themselves are `fleetctl job --type batch`.
    name: Option<String>,
    #[command(flatten)]
    conn: ConnectionOptions,
}

#[derive(Subcommand)]
enum BatchCmd {
    /// Register a reusable batch in an existing namespace.
    Add { #[command(flatten)] add: AddBatch },
    /// The batches, as plain `fleetctl batch` lists them.
    #[command(hide = true)]
    List,
    /// A batch and its newest jobs, as `fleetctl batch NAME` shows them.
    #[command(hide = true)]
    Show { name: String },
    /// Stop accepting new jobs for this batch; accepted jobs continue.
    Disable { name: String },
    /// Accept new jobs for this batch again.
    Enable { name: String },
    /// App owners: set the priority inherited by new jobs.
    Priority { name: String, #[arg(allow_hyphen_values = true)] priority: i32 },
    /// Change a batch's settings; accepted jobs keep the ones they were submitted with.
    Update {
        name: String,
        #[arg(long)] image: Option<String>,
        /// UID[:GID] the container runs as; requires administrator credentials.
        #[arg(long)] user: Option<String>,
        /// Cards a batch job may ask for.
        #[arg(long)] max_gpus: Option<u32>,
        /// GiB a batch job needs free on its card unless it says --gpu-mem.
        #[arg(long = "gpu-mem")] gpu_mem: Option<u32>,
        /// /dev/shm size in GiB.
        #[arg(long)] shm_gb: Option<u32>,
        /// Absolute mount root; repeat. Replaces the roots; requires administrator credentials.
        #[arg(long)] allow: Vec<String>,
        /// K=V every batch job gets; repeat. Replaces the batch's environment.
        #[arg(long)] env: Vec<String>,
    },
    /// Delete a batch after all accepted jobs finish; its job history stays until purged.
    Delete { name: String, #[arg(long)] yes: bool },
    #[command(flatten)]
    Job(BatchJobCmd),
}

#[derive(Subcommand)]
enum InitCmd {
    /// Write a new random token (48 bytes, hex) to a 0600 file: the cluster token
    /// (scheduler <-> nodes).
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
    /// The control plane's API, with the command's token and its namespace and tag selectors.
    base: String,
    token: String,
    http: reqwest::blocking::Client,
    namespace: Option<String>,
    tag: Option<String>,
}

impl FleetClient {
    fn new(conn: &ConnectionOptions) -> anyhow::Result<Self> {
        let path = match &conn.token_file {
            Some(p) => p.clone(),
            None => default_token()
                .context("no token: pass --token-file or set FLEET_TOKEN_FILE")?,
        };
        let token = std::fs::read_to_string(&path)
            .with_context(|| format!("reading {}", path.display()))?
            .trim()
            .to_string();
        Ok(Self {
            base: conn.url.trim_end_matches('/').to_string(),
            namespace: conn.namespace.clone(),
            tag: conn.tag.clone().filter(|t| !t.is_empty()),
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
            Some(NamespaceCmd::Create { name, quota, bundle }) => {
                let namespace = a.send(a.req(M::POST, "/namespaces").json(&json!({"name": name, "quota": quota.value()?})))?;
                let owner = ["read", "submit", "manage"].map(String::from);
                issue_token(&a, cmd.conn(), out, name, Some("scope=default"), None, &owner, "owner", bundle)?;
                if !out.json { writeln!(std::io::stdout().lock(), "namespace {} is ready with scope=default and owner access", namespace["name"].as_str().unwrap_or(name))?; }
            }
            Some(NamespaceCmd::Quota { name, quota }) => out.record(&a.send(a.req(M::PUT, &format!("/namespaces/{name}/quota")).json(&quota.value()?))?),
            Some(NamespaceCmd::Get { name }) => out.record(&a.get(&format!("/namespaces/{name}"))?),
            Some(NamespaceCmd::Suspend { name }) => out.record(&a.send(a.req(M::PUT, &format!("/namespaces/{name}/suspended")))?),
            Some(NamespaceCmd::Resume { name }) => out.record(&a.send(a.req(M::DELETE, &format!("/namespaces/{name}/suspended")))?),
        },
        Cmd::Token(TokenArgs { cmd: Some(TokenCmd::Issue { apps, permissions, label, bundle }), .. }) => {
            let namespace = cmd.conn().namespace.as_deref().context("token issue requires --namespace (create it with fleetctl namespace create NAME)")?;
            let apps = (!apps.is_empty()).then_some(apps.as_slice());
            issue_token(&a, cmd.conn(), out, namespace, a.tag.as_deref(), apps, permissions, label, bundle)?;
        }
        Cmd::Token(TokenArgs { cmd: Some(TokenCmd::Admin { label, out: path }), .. }) => {
            if path.exists() { bail!("{} exists; refusing to overwrite a token", path.display()); }
            let (id, secret, token_hash) = new_secret();
            let mut address = [0u8; 20];
            rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut address);
            let registered = a.send(a.req(M::POST, "/tokens").json(&json!({
                "id": id, "address": hex::encode(address), "label": label, "permissions": ["admin"], "token_hash": token_hash,
            })))?;
            write_secret(path, format!("{secret}\n").as_bytes())?;
            out.done(&format!("issued administrator token {id}; its secret is in {} (0600)", path.display()),
                json!({"id": id, "label": label, "token_file": path.display().to_string(), "registered": registered}));
        }
        Cmd::Token(TokenArgs { cmd: Some(TokenCmd::Refresh { id, bundle }), .. }) => {
            let all = a.get("/tokens")?;
            let old = all.as_array().into_iter().flatten().find(|c| c["id"].as_str() == Some(id))
                .context("no such token")?;
            let namespace = old["namespace"].as_str().context("an administrator's token is not refreshed: issue another with fleetctl token admin")?;
            let apps: Option<Vec<String>> = if old["apps"].is_null() { None } else { Some(serde_json::from_value(old["apps"].clone())?) };
            let permissions: Vec<String> = serde_json::from_value(old["permissions"].clone()).unwrap_or_default();
            issue_token(&a, cmd.conn(), out, namespace, old["tag"].as_str(), apps.as_deref(),
                &permissions, old["label"].as_str().unwrap_or_default(), bundle)?;
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

/// A new token: its id, its secret and the secret's digest (what the scheduler keeps).
fn new_secret() -> (String, String, String) {
    use sha2::Digest;
    let mut random = [0u8; 48];
    rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut random);
    let secret = hex::encode(random);
    let mut id_bytes = [0u8; 16];
    rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut id_bytes);
    let token_hash = hex::encode(sha2::Sha256::digest(secret.as_bytes()));
    (hex::encode(id_bytes), secret, token_hash)
}

#[allow(clippy::too_many_arguments)]
fn issue_token(a: &FleetClient, conn: &ConnectionOptions, out: &OutputWriter, namespace: &str, tag: Option<&str>, apps: Option<&[String]>, permissions: &[String], label: &str, bundle: &TokenBundle) -> anyhow::Result<()> {
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
    let (id, user_token, token_hash) = new_secret();
    let key = k256::ecdsa::SigningKey::random(&mut rand::rngs::OsRng);
    let address = alloy_primitives::Address::from_private_key(&key);
    let registered = a.send(a.req(reqwest::Method::POST, "/tokens").json(&json!({
        "id": id, "address": format!("{address:#x}"), "label": label,
        "apps": apps, "permissions": permissions, "token_hash": token_hash, "namespace": namespace, "tag":tag,
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
        "id": id, "namespace": namespace, "label": label,
        "env_file": env_file.display().to_string(), "token_file": token_file.display().to_string(), "endpoint": endpoint,
        "registered": registered,
    });
    if out.json {
        out.record(&result);
    } else {
        writeln!(std::io::stdout().lock(), "issued token {id} in namespace {namespace}; client env in {} (0600)", env_file.display())?;
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
    &[col("TOKEN ID", "id"), col("NAMESPACE","namespace"), col("TAG","tag"), col("PERMISSIONS", "permissions"), col("LABEL", "label"), col("STATE", "state"), col("CREATED", "created_ms")];
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

/// A proof job as a list row (times from milliseconds to seconds).
fn proof_row(j: &Value) -> Value {
    let secs = |k: &str| j[k].as_f64().map(|ms| ms / 1000.0);
    json!({
        "type": "app", "app": j["app"], "id": j["id"], "client_id": j["client_id"], "namespace": j["namespace"], "tag": j["tag"], "owner": j["owner"],
        "state": j["state"], "priority": j["priority"], "where": j["node"],
        "created_at": secs("created_ms"), "started_at": secs("started_ms"), "finished_at": secs("finished_ms"),
        "took_ms": took_ms(secs("started_ms"), secs("finished_ms")), "record": j,
    })
}

/// A batch job as a `ps` row.
fn batch_row(r: &Value) -> Value {
    json!({
        "type": "batch", "app": r["batch"], "id": r["id"], "namespace": r["namespace"], "tag": r["tag"], "owner": Value::Null, "state": r["state"], "priority": r["priority"],
        "where": r["node"],
        "created_at": r["submitted_at"], "started_at": r["started_at"], "finished_at": r["finished_at"],
        "took_ms": took_ms(r["started_at"].as_f64(), r["finished_at"].as_f64()), "record": with_type("batch", r),
    })
}

/// Which jobs to fetch from one side.
struct JobListFilter<'a> {
    limit: usize,
    active: bool,
    app: Option<&'a str>,
}

/// The proof apps the caller sees (only `app`, when given), by name.
fn proof_app_names(a: &FleetClient, app: Option<&str>) -> anyhow::Result<Vec<String>> {
    Ok(a.get("/apps")?.as_array().into_iter().flatten()
        .filter(|x| x["kind"] == "proof" && app.is_none_or(|name| x["name"].as_str() == Some(name)))
        .filter_map(|x| x["name"].as_str().map(str::to_owned))
        .collect())
}

/// The newest proof and batch jobs that `want` asks for, as list rows.
fn fetch_rows(kind: JobType, conn: &ConnectionOptions, want: &JobListFilter) -> anyhow::Result<Vec<Value>> {
    let a = FleetClient::new(conn)?;
    let mut q: Vec<(&str, String)> = vec![("limit", want.limit.to_string())];
    if want.active {
        q.push(("active", "true".into()));
    }
    let (mut jobs, row, at): (Vec<Value>, fn(&Value) -> Value, &str) = match kind {
        JobType::App => {
            let mut jobs = Vec::new();
            for app in proof_app_names(&a, want.app)? {
                jobs.extend(a.get_q(&format!("/apps/{app}/jobs"), &q)?.as_array().cloned().unwrap_or_default());
            }
            (jobs, proof_row, "created_ms")
        }
        JobType::Batch => {
            let batches = match want.app {
                Some(name) => batch_names(&a)?.into_iter().filter(|b| b == name).collect(),
                None => batch_names(&a)?,
            };
            let mut jobs = Vec::new();
            for batch in batches {
                jobs.extend(a.get_q(&format!("/batches/{batch}/jobs"), &q)?.as_array().cloned().unwrap_or_default());
            }
            (jobs, batch_row, "submitted_at")
        }
    };
    jobs.sort_by(|x, y| y[at].as_f64().unwrap_or(0.0).total_cmp(&x[at].as_f64().unwrap_or(0.0)));
    jobs.truncate(want.limit);
    Ok(jobs.iter().map(row).filter(|r| !want.active || is_active(r)).collect())
}

/// Every job of both types still queued or running, then the newest `-n` of each type.
fn list(p: &ListArgs, conn: &ConnectionOptions, out: &OutputWriter) -> anyhow::Result<()> {
    let kinds: Vec<JobType> = match p.kind {
        Some(k) => vec![k],
        None => vec![JobType::App, JobType::Batch],
    };
    let mut rows: Vec<Value> = Vec::new();
    for k in &kinds {
        let want = |limit, active| JobListFilter { limit, active, app: p.app.as_deref() };
        // the active ones however old (a long batch job outlives many short proofs), then the newest
        let mut fetched = fetch_rows(*k, conn, &want(1000, true))?;
        if !p.active {
            fetched.extend(fetch_rows(*k, conn, &want(p.limit, false))?);
        }
        for row in fetched {
            if !rows.iter().any(|x| x["id"] == row["id"]) {
                rows.push(row);
            }
        }
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
    col("IMAGE", "image"),
];

/// An app by its name, as the control plane describes it.
fn app_named(a: &FleetClient, name: &str) -> anyhow::Result<Value> {
    a.get(&format!("/apps/{name}")).with_context(|| format!("no app {name} (fleetctl app lists them)"))
}

/// Disables or enables an app.
fn set_app_disabled(name: &str, disabled: bool, conn: &ConnectionOptions, out: &OutputWriter) -> anyhow::Result<()> {
    let method = if disabled { reqwest::Method::PUT } else { reqwest::Method::DELETE };
    let a = FleetClient::new(conn)?;
    let v = a.send(a.req(method, &format!("/apps/{name}/disabled")).timeout(Duration::from_secs(1800)))?;
    let served = v["resources"].is_object();
    let text = match (v["kind"].as_str(), disabled, served) {
        (Some("proof"), true, true) => format!("{name} disabled: active proofs drained, its provers stopped"),
        (Some("proof"), false, true) => format!("{name} enabled: its provers started"),
        (Some("proof"), true, false) => format!("{name} disabled: it takes no new jobs; the queued and running ones finish"),
        (Some("proof"), false, false) => format!("{name} enabled: it takes jobs again"),
        (_, true, _) => format!("{name} disabled: stopped; GPU reservations released"),
        (_, false, _) => format!("{name} enabled: service started"),
    };
    out.done(&text, v);
    Ok(())
}

/// Changes app policy; accepted jobs retain their inherited priority.
fn set_app_priority(name: &str, priority: i32, conn: &ConnectionOptions, out: &OutputWriter) -> anyhow::Result<()> {
    let a = FleetClient::new(conn)?;
    let v = a.send(a.req(reqwest::Method::PUT, &format!("/apps/{name}/priority")).json(&json!({"priority":priority})))?;
    out.done(&format!("{name}: priority {priority}; new jobs inherit this priority"), v);
    Ok(())
}

fn app_logs(name: &str, lines: u32, node: Option<&str>, conn: &ConnectionOptions, out: &OutputWriter) -> anyhow::Result<()> {
    let a = FleetClient::new(conn)?;
    let mut query = vec![("lines", lines.to_string())];
    if let Some(node) = node { query.push(("node", node.to_string())); }
    let response = a.req(reqwest::Method::GET, &format!("/apps/{name}/logs")).query(&query).send()?;
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
    let a = FleetClient::new(conn)?;
    let mut request = a.req(method, &format!("/apps/{name}/scale"));
    if write { request = request.json(&body); } else if let Some(node) = node { request = request.query(&[("node", node)]); }
    let v = a.send(request)?;
    out.done(&format!("{name}: {} — {} replicas, {} GPUs and {} CPUs per replica on {}",
        v["state"].as_str().unwrap_or("requested"), v["replicas"], v["gpus"], v["cpus"], v["node"].as_str().unwrap_or("?")),v);
    Ok(())
}

/// Changes an app's settings; the scheduler drains and restarts its service with them.
fn update_app(name: &str, set: Value, node: Option<&str>, conn: &ConnectionOptions, out: &OutputWriter) -> anyhow::Result<()> {
    if set.as_object().is_none_or(|o| o.is_empty()) { bail!("give at least one setting to change"); }
    let a = FleetClient::new(conn)?;
    let mut request = a.req(reqwest::Method::PUT, &format!("/apps/{name}/settings"));
    if let Some(node) = node { request = request.query(&[("node", node)]); }
    let v = a.send(request.json(&set))?;
    out.done(&format!("{name}: {} — its service drains and restarts with the new settings on {}",
        v["state"].as_str().unwrap_or("requested"), v["node"].as_str().unwrap_or("?")), v);
    Ok(())
}

/// `K=V` arguments as an environment map.
fn env_map(pairs: &[String]) -> anyhow::Result<Value> {
    let mut env = serde_json::Map::new();
    for kv in pairs {
        let Some((k, v)) = kv.split_once('=') else { bail!("--env {kv}: K=V") };
        env.insert(k.into(), json!(v));
    }
    Ok(Value::Object(env))
}

/// Registers a proof program as an app of the namespace.
fn add_app(add: &AddApp, conn: &ConnectionOptions, out: &OutputWriter) -> anyhow::Result<()> {
    if conn.namespace.as_deref().is_none_or(str::is_empty) && conn.token_file.is_none() {
        bail!("app registration requires --namespace NAME; an administrator must create the namespace first");
    }
    let mut body = serde_json::to_value(add)?;
    body["elf_id"] = json!(match (&add.elf, &add.elf_id) {
        (Some(file), None) => {
            use sha2::Digest;
            let bytes = std::fs::read(file).with_context(|| format!("reading {}", file.display()))?;
            hex::encode(sha2::Sha256::digest(&bytes))
        }
        (None, Some(id)) => id.trim_start_matches("0x").to_ascii_lowercase(),
        (Some(_), Some(_)) => bail!("give --elf-id or --elf FILE, not both"),
        (None, None) => bail!("give the program: --elf-id ID or --elf FILE"),
    });
    let a = FleetClient::new(conn)?;
    let v = a.send(a.req(reqwest::Method::POST, "/apps").json(&body))?;
    out.done(
        &format!("added app {}: program {} (its jobs are accepted from now on)", add.name, body["elf_id"].as_str().unwrap_or("")),
        v,
    );
    Ok(())
}

fn add_batch(add: &AddBatch, conn: &ConnectionOptions, out: &OutputWriter) -> anyhow::Result<()> {
    if conn.namespace.as_deref().is_none_or(str::is_empty) && conn.token_file.is_none() {
        bail!("batch creation requires --namespace NAME; an administrator must create the namespace first");
    }
    if add.name.contains('/') { bail!("a batch name cannot contain /"); }
    let mut body = serde_json::to_value(add)?;
    let mut env = serde_json::Map::new();
    for kv in &add.env {
        let Some((k, v)) = kv.split_once('=') else { bail!("--env {kv}: K=V") };
        env.insert(k.into(), json!(v));
    }
    if !env.is_empty() {
        body["env"] = Value::Object(env);
    }
    let a = FleetClient::new(conn)?;
    let v = a.send(a.req(reqwest::Method::POST, "/batches").json(&body))?;
    let gpus = v["batch"]["max_gpus"].as_u64().unwrap_or(0);
    out.done(
        &format!("added batch {}: image {}, up to {gpus} GPU{}, mounts under {}", add.name,
                 v["batch"]["image"].as_str().unwrap_or("?"), if gpus == 1 { "" } else { "s" },
                 v["batch"]["allow"].as_array().map(|r| r.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(", ")).unwrap_or_default()),
        v,
    );
    Ok(())
}

/// One app's record, and a proof app's newest jobs.
fn describe_app(name: &str, conn: &ConnectionOptions, out: &OutputWriter) -> anyhow::Result<()> {
    let mut rec = app_named(&FleetClient::new(conn)?, name)?;
    let jobs = if rec["kind"] == "proof" {
        fetch_rows(JobType::App, conn, &JobListFilter { limit: 5, active: false, app: Some(name) })?
    } else {
        vec![]
    };
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
    let a = FleetClient::new(conn)?;
    let app = app_named(&a, name)?;
    let (total, active) = (&app["jobs"]["total"], &app["jobs"]["active"]);
    let proof = app["kind"] == "proof";
    if !yes {
        if proof {
            bail!(
                "would delete app {name}: {total} job record(s) with their inputs and proofs, its anchor, and its ELF {} \
                 unless another app uses it ({active} queued or running: refused while any is). Pass --yes to delete",
                app["proof"]["elf_id"].as_str().unwrap_or("")
            );
        }
        bail!(
            "would remove serving app {name}; its {total} past execution record(s) \
             stay ({active} queued or running: refused while any is). Pass --yes to delete"
        );
    }
    let v = a.send(a.req(reqwest::Method::DELETE, &format!("/apps/{name}")))?;
    let text = if proof {
        format!("deleted app {name}: {} job(s), ELF {}", v["jobs_deleted"],
            if v["elf_removed"].as_bool() == Some(true) { "removed" } else { "kept (in use or absent)" })
    } else {
        format!("deleted serving app {name}; its {total} past execution record(s) stay")
    };
    out.done(&text, v);
    Ok(())
}

/// Every app, proof and serving alike, most recently used first.
fn app(p: &AppArgs, out: &OutputWriter) -> anyhow::Result<()> {
    match &p.cmd {
        Some(AppCmd::Delete { name, yes }) => return delete_app(name, *yes, &p.conn, out),
        Some(AppCmd::Disable { name }) => return set_app_disabled(name, true, &p.conn, out),
        Some(AppCmd::Enable { name }) => return set_app_disabled(name, false, &p.conn, out),
        Some(AppCmd::Add { add }) => return add_app(add, &p.conn, out),
        Some(AppCmd::Priority { name, priority }) => return set_app_priority(name, *priority, &p.conn, out),
        Some(AppCmd::Logs { name, lines, node }) => return app_logs(name, *lines, node.as_deref(), &p.conn, out),
        Some(AppCmd::Scale { name, gpus, cpus, replicas, node }) => return scale_app(name, *gpus, *cpus, *replicas, node.as_deref(), &p.conn, out),
        Some(AppCmd::Update { name, image, env, shm_gb, node }) => {
            let mut set = json!({});
            if let Some(v) = image { set["image"] = json!(v); }
            if !env.is_empty() { set["env"] = env_map(env)?; }
            if let Some(v) = shm_gb { set["shm_gb"] = json!(v); }
            return update_app(name, set, node.as_deref(), &p.conn, out);
        }
        None => {}
    }
    if let Some(name) = &p.name {
        return describe_app(name, &p.conn, out);
    }
    let apps = FleetClient::new(&p.conn)?.get("/apps")?;
    let rows: Vec<Value> = apps.as_array().into_iter().flatten().map(|a| {
        let mean = a["proof"]["mean_prove_ms"].as_f64().map(|m| format!("mean prove {:.1} s", m / 1000.0));
        let anchored = a["proof"]["anchored_on"].as_str().map(|n| format!("anchored on {n}"));
        // Why a starting app is not running yet comes first.
        let why = a["detail"].as_str().filter(|d| !d.is_empty()).map(str::to_owned);
        let detail: Vec<String> = [why, anchored, mean].into_iter().flatten().collect();
        // A serving app's image is among its settings, a proof app's is its provers'.
        let image = [&a["service"]["image"], &a["proof"]["image"]].into_iter().find(|i| !i.is_null());
        json!({
            "type": a["kind"], "name": a["name"], "namespace": a["namespace"],
            "tag": a.get("tags").filter(|v| v.as_array().is_some_and(|tags| !tags.is_empty())).unwrap_or(&a["tag"]),
            "state": a["state"], "priority": a["priority"], "resources": a["resources"],
            "last_at": a["jobs"]["last_at"], "detail": detail.join(", "), "image": image,
        })
    }).collect();
    out.table(&rows, APP_COLS, Some(&apps));
    Ok(())
}

fn batch_named(a: &FleetClient, name: &str) -> anyhow::Result<Value> {
    let batch = a.get(&format!("/batches/{name}"))?;
    if batch["persistent"].as_bool() == Some(true) {
        bail!("{name} is a long-running app; use fleetctl app");
    }
    Ok(with_type("batch", &batch))
}

/// Every batch the caller can see, deleted ones that still have jobs included.
fn batch_names(a: &FleetClient) -> anyhow::Result<Vec<String>> {
    Ok(a.get("/batches")?.as_array().into_iter().flatten()
        .filter_map(|b| b["name"].as_str().map(str::to_owned)).collect())
}

/// A batch job's path: under `batch` when given, else under the batch that has the id.
fn batch_job_path(a: &FleetClient, batch: Option<&str>, id: &str) -> anyhow::Result<String> {
    if let Some(batch) = batch {
        return Ok(format!("/batches/{batch}/jobs/{id}"));
    }
    for batch in batch_names(a)? {
        let path = format!("/batches/{batch}/jobs/{id}");
        if a.get(&path).is_ok() {
            return Ok(path);
        }
    }
    bail!("no batch job {id}")
}

fn batch(p: &BatchArgs, out: &OutputWriter) -> anyhow::Result<()> {
    match &p.cmd {
        Some(BatchCmd::Job(cmd)) => return batch_action(cmd, &p.conn, out),
        Some(BatchCmd::Add { add }) => return add_batch(add, &p.conn, out),
        Some(BatchCmd::List) => return batches(&p.conn, out),
        Some(BatchCmd::Update { name, image, user, max_gpus, gpu_mem, shm_gb, allow, env }) => {
            let mut set = json!({});
            if let Some(v) = image { set["image"] = json!(v); }
            if let Some(v) = user { set["user"] = json!(v); }
            if let Some(v) = max_gpus { set["max_gpus"] = json!(v); }
            if let Some(v) = gpu_mem { set["default_gpu_mem_gb"] = json!(v); }
            if let Some(v) = shm_gb { set["shm_gb"] = json!(v); }
            if !allow.is_empty() { set["allow"] = json!(allow); }
            if !env.is_empty() { set["env"] = env_map(env)?; }
            if set.as_object().is_none_or(|o| o.is_empty()) { bail!("give at least one setting to change"); }
            let a = FleetClient::new(&p.conn)?;
            batch_named(&a, name)?;
            let v = a.send(a.req(reqwest::Method::PUT, &format!("/batches/{name}/settings")).json(&set))?;
            out.done(&format!("updated batch {name}; new jobs use the new settings"), v);
            return Ok(());
        }
        Some(BatchCmd::Show { name }) => return show_batch(name, &p.conn, out),
        Some(cmd) => {
            let a = FleetClient::new(&p.conn)?;
            let name = match cmd {
                BatchCmd::Disable { name } | BatchCmd::Enable { name }
                | BatchCmd::Priority { name, .. } | BatchCmd::Delete { name, .. } => name,
                _ => unreachable!(),
            };
            batch_named(&a, name)?;
            let path = format!("/batches/{name}");
            let request = match cmd {
                BatchCmd::Disable { .. } => a.req(reqwest::Method::PUT, &format!("{path}/disabled")),
                BatchCmd::Enable { .. } => a.req(reqwest::Method::DELETE, &format!("{path}/disabled")),
                BatchCmd::Priority { priority, .. } => a.req(reqwest::Method::PUT, &format!("{path}/priority"))
                    .json(&json!({"priority": priority})),
                BatchCmd::Delete { yes, .. } => {
                    if !yes { bail!("would remove batch {name}; accepted jobs must finish first. Pass --yes to delete"); }
                    a.req(reqwest::Method::DELETE, &path)
                }
                _ => unreachable!(),
            };
            let value = a.send(request)?;
            out.done(&format!("updated batch {name}"), value);
            return Ok(());
        }
        None => {}
    }
    match &p.name {
        Some(name) => show_batch(name, &p.conn, out),
        None => batches(&p.conn, out),
    }
}

/// The batches, like `fleetctl app`: each record carries its job counts, last job and image.
fn batches(conn: &ConnectionOptions, out: &OutputWriter) -> anyhow::Result<()> {
    let batches = FleetClient::new(conn)?.get("/batches")?;
    let rows = batches.as_array().cloned().unwrap_or_default();
    out.table(&rows, BATCH_COLS, Some(&batches));
    Ok(())
}

const BATCH_COLS: &[out::TableColumn] = &[
    col("NAME", "name"),
    col("NAMESPACE", "namespace"),
    col("PRIORITY", "priority"),
    col("TAG", "tag"),
    col("STATE", "state"),
    col("MAX GPUS", "/batch/max_gpus"),
    col("ACTIVE", "/jobs/active"),
    col("LAST JOB", "/jobs/last_at"),
    col("IMAGE", "/batch/image"),
];

fn show_batch(name: &str, conn: &ConnectionOptions, out: &OutputWriter) -> anyhow::Result<()> {
    let mut batch = batch_named(&FleetClient::new(conn)?, name)?;
    batch["newest_jobs"] = json!(fetch_rows(JobType::Batch, conn,
        &JobListFilter { limit: 5, active: false, app: Some(name) })?);
    out.record(&batch);
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

/// A proof job's path: under the proof app (`app`, when given) that has the id.
fn proof_job_path(a: &FleetClient, app: Option<&str>, id: &str) -> anyhow::Result<String> {
    for app in proof_app_names(a, app)? {
        let path = format!("/apps/{app}/jobs/{id}");
        if a.get(&path).is_ok() {
            return Ok(path);
        }
    }
    bail!("no app job {id}")
}

/// An app or batch job, by id (of `app`, an app or a batch, when given).
fn record(id: &str, app: Option<&str>, conn: &ConnectionOptions, out: &OutputWriter) -> anyhow::Result<()> {
    let a = FleetClient::new(conn)?;
    if let Ok(path) = proof_job_path(&a, app, id) {
        out.record(&with_type("app", &a.get(&path)?));
        return Ok(());
    }
    let batch = app.filter(|name| batch_names(&a).is_ok_and(|all| all.iter().any(|b| b == name)));
    match batch_job_path(&a, batch, id) {
        Ok(path) => {
            out.record(&with_type("batch", &a.get(&path)?));
            Ok(())
        }
        Err(_) => bail!("no app or batch job {id}"),
    }
}

fn job(j: &JobArgs, out: &OutputWriter) -> anyhow::Result<()> {
    match (&j.cmd, &j.id) {
        (None, None) => list(&j.list, &j.conn, out),
        (None, Some(id)) => record(id, j.list.app.as_deref(), &j.conn, out),
        (Some(JobCmd::Fetch { id, out: dir }), _) => fetch_proof(id, j.list.app.as_deref(), dir, &j.conn, out),
    }
}

/// Download a retained proof through the control-plane relay.
fn fetch_proof(id: &str, app: Option<&str>, dir: &Path, conn: &ConnectionOptions, out: &OutputWriter) -> anyhow::Result<()> {
    if dir.try_exists()? {
        bail!("output directory {} already exists; choose a new directory",dir.display());
    }
    let proof = FleetClient::new(conn)?;
    let path = proof_job_path(&proof, app, id)?;
    let mut files = Vec::new();
    for name in ["proof.json", "public_values.bin"] {
        let response = proof.req(reqwest::Method::GET,&format!("{path}/output/{name}")).timeout(Duration::from_secs(75)).send()?;
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
    let a = FleetClient::new(conn)?;
    match cmd {
        BatchJobCmd::Submit { submit } => {
            let body = serde_json::to_value(submit)?;
            let v = a.send(a.req(M::POST, &format!("/batches/{}/jobs", submit.batch)).json(&body))?;
            out.record(&with_type("batch", &v));
        }
        BatchJobCmd::Cancel { id, batch } => {
            let path = batch_job_path(&a, batch.as_deref(), id)?;
            out.record(&a.send(a.req(M::POST, &format!("{path}/cancel")))?)
        }
        BatchJobCmd::Preempt { id, batch } => {
            let path = batch_job_path(&a, batch.as_deref(), id)?;
            out.record(&a.send(a.req(M::POST, &format!("{path}/preempt")))?)
        }
        BatchJobCmd::Purge { id, batch, older_than } => {
            let purged = match (id, older_than) {
                (Some(id), _) => a.send(a.req(M::DELETE, &batch_job_path(&a, batch.as_deref(), id)?))?,
                (None, Some(days)) => {
                    let batches = match batch { Some(batch) => vec![batch.clone()], None => batch_names(&a)? };
                    let mut purged = Vec::new();
                    for batch in batches {
                        let v = a.send(a.req(M::DELETE, &format!("/batches/{batch}/jobs?older_than={days}")))?;
                        purged.extend(v.as_array().cloned().unwrap_or_default());
                    }
                    Value::Array(purged)
                }
                (None, None) => bail!("purge ID, or purge --older-than DAYS"),
            };
            out.table(purged.as_array().map(Vec::as_slice).unwrap_or_default(), PURGE_COLS, Some(&purged));
        }
        BatchJobCmd::Logs { id, batch, follow, lines } => {
            let path = batch_job_path(&a, batch.as_deref(), id)?;
            let mut chunk = a.get(&format!("{path}/logs?lines={lines}"))?;
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
                chunk = a.get(&format!("{path}/logs?offset={offset}"))?;
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
fn default_token() -> Option<PathBuf> {
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
