use std::{env, io::Write, path::PathBuf, process::Stdio, sync::Arc, time::Duration};

use anyhow::{bail, Context, Result};
use axum::Router;
use http::request::Parts;
use rmcp::schemars;
use rmcp::{
    handler::server::{tool::Extension, wrapper::Parameters},
    model::{ServerCapabilities, ServerConfig},
    tool, tool_handler, tool_router,
    transport::streamable_http_server::{
        session::local::LocalSessionManager, StreamableHttpServerConfig, StreamableHttpService,
    },
    ErrorData, Json, ServerHandler,
};
use serde::Deserialize;
use serde_json::Value;
use tokio::{io::AsyncReadExt, process::Command, time::timeout};

const COMMAND_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Clone)]
struct FleetctlClient {
    fleetctl: PathBuf,
    fleet_url: Option<String>,
}

impl FleetctlClient {
    fn from_env() -> Self {
        let fleetctl = env::var_os("FLEETCTL_BIN")
            .map(PathBuf::from)
            .or_else(|| {
                env::current_exe().ok().and_then(|p| {
                    let sibling = p.parent()?.join("fleetctl");
                    sibling.is_file().then_some(sibling)
                })
            })
            .unwrap_or_else(|| PathBuf::from("fleetctl"));

        Self {
            fleetctl,
            fleet_url: env::var("FLEET_URL").ok(),
        }
    }

    async fn run(&self, user_token: &str, args: &[String]) -> Result<Value> {
        let mut token_file = tempfile::Builder::new()
            .prefix("fleet-mcp-token-")
            .tempfile()
            .context("creating private temporary token file")?;
        token_file
            .write_all(user_token.as_bytes())
            .context("writing temporary user token")?;
        token_file
            .flush()
            .context("flushing temporary user token")?;

        let mut command = Command::new(&self.fleetctl);
        command
            .env_clear()
            .env("PATH", env::var_os("PATH").unwrap_or_default())
            .arg("--json")
            .args(args)
            .kill_on_drop(true)
            .env("FLEET_TOKEN_FILE", token_file.path())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(value) = &self.fleet_url {
            command.env("FLEET_URL", value);
        }

        let mut child = command
            .spawn()
            .with_context(|| format!("starting {}", self.fleetctl.display()))?;
        let stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        let output = timeout(COMMAND_TIMEOUT, async move {
            let stdout_task = tokio::spawn(async move {
                let mut bytes = Vec::new();
                let mut reader = stdout;
                reader.read_to_end(&mut bytes).await.map(|_| bytes)
            });
            let stderr_task = tokio::spawn(async move {
                let mut bytes = Vec::new();
                let mut reader = stderr;
                reader.read_to_end(&mut bytes).await.map(|_| bytes)
            });
            let status = child.wait().await?;
            let stdout = stdout_task.await??;
            let stderr = stderr_task.await??;
            Ok::<_, std::io::Error>((status, stdout, stderr))
        })
        .await
        .context("fleetctl command timed out")??;
        if !output.0.success() {
            let details = String::from_utf8_lossy(&output.2);
            bail!("fleetctl failed ({}): {}", output.0, details.trim());
        }
        if output.1.is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_slice(&output.1).context("fleetctl returned invalid JSON")
    }
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
enum JobType {
    /// Jobs of apps (proofs).
    App,
    Batch,
}

impl JobType {
    fn as_str(&self) -> &'static str {
        match self {
            Self::App => "app",
            Self::Batch => "batch",
        }
    }
}

#[derive(Deserialize, schemars::JsonSchema)]
struct JobsFilter {
    #[serde(default)]
    app: Option<String>,
    #[serde(default)]
    kind: Option<JobType>,
    #[serde(default)]
    active: bool,
    #[serde(default = "default_limit")]
    limit: usize,
}
fn default_limit() -> usize {
    20
}

#[derive(Deserialize, schemars::JsonSchema)]
struct JobId {
    id: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct AppName {
    name: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct CreateBatch {
    name: String,
    image: String,
    #[serde(default)]
    allow: Vec<String>,
    #[serde(default)]
    max_gpus: Option<u32>,
    #[serde(default)]
    priority: Option<i32>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct SubmitBatchJob {
    batch: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    gpus: Option<f64>,
    #[serde(default)]
    cpus: Option<u32>,
    #[serde(default)]
    mounts: Vec<String>,
    command: Vec<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct BatchJobsFilter {
    #[serde(default)]
    batch: Option<String>,
    #[serde(default)]
    active: bool,
    #[serde(default = "default_limit")]
    limit: usize,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct BatchJobLogs {
    id: String,
    /// The job's batch (found from the id when omitted).
    #[serde(default)]
    batch: Option<String>,
    #[serde(default = "default_log_lines")]
    lines: usize,
}
fn default_log_lines() -> usize { 50 }

#[derive(Deserialize, schemars::JsonSchema)]
struct BatchJobId {
    id: String,
    /// The job's batch (found from the id when omitted).
    #[serde(default)]
    batch: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct AppLogs {
    name: String,
    #[serde(default = "default_log_lines")]
    lines: usize,
    /// The machine, when the app runs on more than one.
    node: Option<String>,
}

/// An app's `RegisterAppRequest` (model.proto): its name, container spec and, for a prover,
/// its program and proof policy. Its namespace is the token's; the scheduler picks its node.
#[derive(Deserialize, schemars::JsonSchema)]
struct CreateApp {
    name: String,
    /// The app's container settings, the keys of its apps.yaml entry: kind (app, the default,
    /// or ziren-gpu/ziren-cpu for a prover), image, replicas, gpus, cpus, shm_gb, memory_gb, env,
    /// volumes, ready_timeout_s, stop_timeout_s; for an app also command, port_base, health_path,
    /// workdir, user; for a prover proof_storage_root, backend, rust_log. image is required, and
    /// port_base for an app; user, volumes and proof_storage_root require administrator credentials.
    spec: serde_json::Map<String, Value>,
    /// Priority inherited by the app's replicas and jobs (default 0; higher values run first).
    priority: Option<i32>,
    /// A prover's program: its full ELF SHA-256.
    elf_id: Option<String>,
    /// latency: the biggest free prover first; batch (default): the smallest.
    class: Option<String>,
    /// none (default), shared or exclusive: keep the program on the prover it first ran on.
    anchor: Option<String>,
    /// Node backends that may prove it: cuda, cpu, mock; default any.
    #[serde(default)]
    backends: Vec<String>,
    /// Only provers of this circuit version prove it.
    zkm_version: Option<String>,
    /// Jobs queued or running at most (default 64).
    max_queued: Option<u32>,
}

#[derive(Clone)]
struct FleetMcp {
    fleet: FleetctlClient,
}

#[tool_router]
impl FleetMcp {
    #[tool(
        name = "fleet_apps_list",
        description = "List long-running apps available to the authenticated namespace and token.",
        annotations(read_only_hint = true, destructive_hint = false)
    )]
    async fn apps_list(
        &self,
        Extension(parts): Extension<Parts>,
    ) -> Result<Json<Value>, ErrorData> {
        self.call(parts, vec!["app".into()]).await
    }

    #[tool(
        name = "fleet_app_get",
        description = "Get a long-running app visible to the authenticated token.",
        annotations(read_only_hint = true, destructive_hint = false)
    )]
    async fn app_get(
        &self,
        Parameters(input): Parameters<AppName>,
        Extension(parts): Extension<Parts>,
    ) -> Result<Json<Value>, ErrorData> {
        self.call(parts, vec!["app".into(), input.name]).await
    }

    #[tool(
        name = "fleet_app_logs",
        description = "Read the last lines of a visible long-running app's log from the node that runs it.",
        annotations(read_only_hint = true, destructive_hint = false)
    )]
    async fn app_logs(
        &self,
        Parameters(input): Parameters<AppLogs>,
        Extension(parts): Extension<Parts>,
    ) -> Result<Json<Value>, ErrorData> {
        if !(1..=2000).contains(&input.lines) {
            return Err(tool_error("lines must be between 1 and 2000"));
        }
        let mut args = vec!["app".into(), "logs".into(), input.name, "--lines".into(), input.lines.to_string()];
        if let Some(node) = input.node {
            args.extend(["--node".into(), node]);
        }
        self.call(parts, args).await
    }

    #[tool(
        name = "fleet_batch_create",
        description = "Create a batch in the authenticated namespace with workload-management access. Nonempty host mount roots require administrator credentials. The scheduler validates the batch and enforces namespace quotas.",
        annotations(read_only_hint = false, destructive_hint = false)
    )]
    async fn batch_create(
        &self,
        Parameters(input): Parameters<CreateBatch>,
        Extension(parts): Extension<Parts>,
    ) -> Result<Json<Value>, ErrorData> {
        let mut args = vec![
            "batch".into(),
            "add".into(),
            input.name,
            "--image".into(),
            input.image,
        ];
        for root in input.allow {
            args.extend(["--allow".into(), root]);
        }
        if let Some(max_gpus) = input.max_gpus {
            args.extend(["--max-gpus".into(), max_gpus.to_string()]);
        }
        if let Some(priority) = input.priority {
            args.extend(["--priority".into(), priority.to_string()]);
        }
        self.call(parts, args).await
    }

    #[tool(
        name = "fleet_app_create",
        description = "Create an app (a long-running container, such as a model server, or with elf_id a prover of that program) in the authenticated namespace; the scheduler validates its settings. It starts disabled; enabling it places it on a node and admits it against the namespace's quota. A namespace token with manage permission may create one; user, volumes and proof_storage_root require administrator credentials.",
        annotations(read_only_hint = false, destructive_hint = false)
    )]
    async fn app_create(
        &self,
        Parameters(input): Parameters<CreateApp>,
        Extension(parts): Extension<Parts>,
    ) -> Result<Json<Value>, ErrorData> {
        // fleetctl reads the settings from a file, kept private for this one call.
        let mut spec = tempfile::Builder::new()
            .prefix("fleet-mcp-spec-")
            .suffix(".json")
            .tempfile()
            .map_err(|e| tool_error(&format!("creating temporary settings file: {e}")))?;
        spec.write_all(Value::Object(input.spec).to_string().as_bytes())
            .map_err(|e| tool_error(&format!("writing temporary settings file: {e}")))?;
        let mut args = vec![
            "app".into(),
            "add".into(),
            input.name,
            "--spec".into(),
            spec.path().display().to_string(),
        ];
        let optional = [
            ("--priority", input.priority.map(|v| v.to_string())),
            ("--elf-id", input.elf_id),
            ("--class", input.class),
            ("--anchor", input.anchor),
            ("--zkm-version", input.zkm_version),
            ("--max-queued", input.max_queued.map(|v| v.to_string())),
        ];
        for (flag, value) in optional {
            if let Some(value) = value {
                args.extend([flag.into(), value]);
            }
        }
        for backend in input.backends {
            args.extend(["--backend".into(), backend]);
        }
        self.call(parts, args).await
    }

    #[tool(name = "fleet_batches_list", description = "List reusable finite batches in the authenticated namespace.",
        annotations(read_only_hint = true, destructive_hint = false))]
    async fn batches_list(&self, Extension(parts): Extension<Parts>) -> Result<Json<Value>, ErrorData> {
        self.call(parts, vec!["batch".into()]).await
    }

    #[tool(name = "fleet_batch_get", description = "Inspect a batch and its newest jobs.",
        annotations(read_only_hint = true, destructive_hint = false))]
    async fn batch_get(&self, Parameters(input): Parameters<AppName>, Extension(parts): Extension<Parts>) -> Result<Json<Value>, ErrorData> {
        self.call(parts, vec!["batch".into(), input.name]).await
    }

    #[tool(name = "fleet_batch_jobs_list", description = "List finite batch jobs visible to the authenticated token, optionally of one batch.",
        annotations(read_only_hint = true, destructive_hint = false))]
    async fn batch_jobs_list(&self, Parameters(input): Parameters<BatchJobsFilter>, Extension(parts): Extension<Parts>) -> Result<Json<Value>, ErrorData> {
        if !(1..=100).contains(&input.limit) { return Err(tool_error("limit must be between 1 and 100")); }
        let mut args = vec!["job".into(), "--type".into(), "batch".into(), "--limit".into(), input.limit.to_string()];
        if let Some(batch) = input.batch { args.extend(["--app".into(), batch]); }
        if input.active { args.push("--active".into()); }
        self.call(parts, args).await
    }

    #[tool(name = "fleet_batch_job_get", description = "Inspect a finite batch job visible to the authenticated token.",
        annotations(read_only_hint = true, destructive_hint = false))]
    async fn batch_job_get(&self, Parameters(input): Parameters<JobId>, Extension(parts): Extension<Parts>) -> Result<Json<Value>, ErrorData> {
        self.call(parts, vec!["job".into(), input.id]).await
    }

    #[tool(name = "fleet_batch_job_logs", description = "Read recent container logs for a visible batch job.",
        annotations(read_only_hint = true, destructive_hint = false))]
    async fn batch_job_logs(&self, Parameters(input): Parameters<BatchJobLogs>, Extension(parts): Extension<Parts>) -> Result<Json<Value>, ErrorData> {
        if !(1..=10000).contains(&input.lines) { return Err(tool_error("lines must be between 1 and 10000")); }
        let mut args = vec!["batch".into(), "logs".into(), input.id, "--lines".into(), input.lines.to_string()];
        if let Some(batch) = input.batch { args.extend(["--batch".into(), batch]); }
        self.call(parts, args).await
    }

    #[tool(
        name = "fleet_jobs_list",
        description = "List jobs visible to the authenticated token. Results are scoped by the token, namespace, and tag.",
        annotations(read_only_hint = true, destructive_hint = false)
    )]
    async fn jobs_list(
        &self,
        Parameters(filter): Parameters<JobsFilter>,
        Extension(parts): Extension<Parts>,
    ) -> Result<Json<Value>, ErrorData> {
        if !(1..=100).contains(&filter.limit) {
            return Err(tool_error("limit must be between 1 and 100"));
        }
        let mut args = vec![
            "job".to_string(),
            "--limit".into(),
            filter.limit.to_string(),
        ];
        if let Some(app) = filter.app {
            args.extend(["--app".into(), app]);
        }
        if let Some(kind) = filter.kind {
            args.extend(["--type".into(), kind.as_str().into()]);
        }
        if filter.active {
            args.push("--active".into());
        }
        self.call(parts, args).await
    }

    #[tool(
        name = "fleet_job_get",
        description = "Get one job visible to the authenticated token.",
        annotations(read_only_hint = true, destructive_hint = false)
    )]
    async fn job_get(
        &self,
        Parameters(input): Parameters<JobId>,
        Extension(parts): Extension<Parts>,
    ) -> Result<Json<Value>, ErrorData> {
        self.call(parts, vec!["job".into(), input.id]).await
    }

    #[tool(
        name = "fleet_batch_job_submit",
        description = "Submit a finite job under a batch available to the authenticated token. Commands read inputs and write outputs directly in owner-managed storage. FleetctlClient provides job status and logs. The command is passed directly without a shell.",
        annotations(read_only_hint = false, destructive_hint = false)
    )]
    async fn batch_job_submit(
        &self,
        Parameters(input): Parameters<SubmitBatchJob>,
        Extension(parts): Extension<Parts>,
    ) -> Result<Json<Value>, ErrorData> {
        if input.command.is_empty() {
            return Err(tool_error("command must contain at least one argument"));
        }
        if input.command.iter().any(|part| part.contains('\0')) {
            return Err(tool_error("command arguments cannot contain NUL bytes"));
        }
        let mut args = vec!["batch".into(), "submit".into(), input.batch];
        if let Some(name) = input.name {
            args.extend(["--name".into(), name]);
        }
        if let Some(gpus) = input.gpus {
            if !gpus.is_finite() || gpus < 0.0 {
                return Err(tool_error("gpus must be a finite non-negative number"));
            }
            args.extend(["--gpus".into(), gpus.to_string()]);
        }
        if let Some(cpus) = input.cpus {
            args.extend(["--cpus".into(), cpus.to_string()]);
        }
        for mount in input.mounts {
            args.extend(["--mount".into(), mount]);
        }
        args.push("--".into());
        args.extend(input.command);
        self.call(parts, args).await
    }

    #[tool(
        name = "fleet_batch_job_cancel",
        description = "Cancel a visible batch job and release its resources after cleanup. This stops the job.",
        annotations(read_only_hint = false, destructive_hint = true)
    )]
    async fn batch_job_cancel(
        &self,
        Parameters(input): Parameters<BatchJobId>,
        Extension(parts): Extension<Parts>,
    ) -> Result<Json<Value>, ErrorData> {
        let mut args = vec!["batch".into(), "cancel".into(), input.id];
        if let Some(batch) = input.batch {
            args.extend(["--batch".into(), batch]);
        }
        self.call(parts, args).await
    }
}

impl FleetMcp {
    async fn call(&self, parts: Parts, args: Vec<String>) -> Result<Json<Value>, ErrorData> {
        let token = bearer_token(&parts).map_err(|e| tool_error(&e))?;
        self.fleet
            .run(&token, &args)
            .await
            .map(Json)
            .map_err(|e| tool_error(&format!("{e:#}")))
    }
}

fn tool_error(message: &str) -> ErrorData {
    ErrorData::internal_error(message.to_owned(), None)
}

fn bearer_token(parts: &Parts) -> std::result::Result<String, String> {
    let value = parts
        .headers
        .get(http::header::AUTHORIZATION)
        .ok_or_else(|| "missing Authorization: Bearer header".to_string())?
        .to_str()
        .map_err(|_| "invalid Authorization header".to_string())?;
    let (scheme, token) = value
        .split_once(' ')
        .ok_or_else(|| "expected Authorization: Bearer <user-token>".to_string())?;
    if !scheme.eq_ignore_ascii_case("Bearer") || token.is_empty() || token.contains(' ') {
        return Err("expected Authorization: Bearer <user-token>".to_string());
    }
    Ok(token.to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    let fleet = FleetctlClient::from_env();
    let bind = env::var("FLEET_MCP_BIND").unwrap_or_else(|_| "127.0.0.1:8906".into());
    let allowed_hosts = env::var("FLEET_MCP_ALLOWED_HOSTS")
        .unwrap_or_else(|_| "127.0.0.1:8906,localhost:8906,34.215.238.232".into());
    let allowed_hosts = allowed_hosts
        .split(',')
        .map(str::trim)
        .filter(|host| !host.is_empty());
    let config = StreamableHttpServerConfig::default()
        .with_allowed_hosts(allowed_hosts)
        .enforce_origin_validation();
    let mcp = StreamableHttpService::new(
        move || {
            Ok(FleetMcp {
                fleet: fleet.clone(),
            })
        },
        Arc::new(LocalSessionManager::default()),
        config,
    );
    let app = Router::new().route_service("/mcp", mcp);
    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .with_context(|| format!("binding MCP server to {bind}"))?;
    axum::serve(listener, app)
        .await
        .context("MCP HTTP server stopped")?;
    Ok(())
}

#[tool_handler]
impl ServerHandler for FleetMcp {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(rmcp::model::Implementation::new("zkm-fleet", env!("CARGO_PKG_VERSION")))
            .with_instructions("Initialization and tool discovery are public. To execute a tool, send an Authorization: Bearer user-token header. Tool access is limited by that token's namespace, tag, and app grants.")
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn job_type_is_app_or_batch() {
        for (input, want) in [("\"app\"", "app"), ("\"batch\"", "batch")] {
            assert_eq!(serde_json::from_str::<super::JobType>(input).unwrap().as_str(), want);
        }
        for old in ["\"proof\"", "\"run\""] {
            assert!(serde_json::from_str::<super::JobType>(old).is_err(), "{old}");
        }
    }
}
