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
struct Fleet {
    fleetctl: PathBuf,
    fleet_url: Option<String>,
    runs_url: Option<String>,
}

impl Fleet {
    fn from_env() -> Result<Self> {
        let fleetctl = env::var_os("FLEETCTL_BIN")
            .map(PathBuf::from)
            .or_else(|| {
                env::current_exe().ok().and_then(|p| {
                    let sibling = p.parent()?.join("fleetctl");
                    sibling.is_file().then_some(sibling)
                })
            })
            .unwrap_or_else(|| PathBuf::from("fleetctl"));

        Ok(Self {
            fleetctl,
            fleet_url: env::var("FLEET_URL").ok(),
            runs_url: env::var("FLEET_RUNS_URL").ok(),
        })
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
        if let Some(value) = &self.runs_url {
            command.env("FLEET_RUNS_URL", value);
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
struct JobsFilter {
    #[serde(default)]
    app: Option<String>,
    #[serde(default)]
    kind: Option<String>,
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
struct CreateRunApp {
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
struct SubmitRun {
    app: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    gpus: Option<f64>,
    #[serde(default)]
    cpus: Option<u32>,
    command: Vec<String>,
}

#[derive(Clone)]
struct FleetMcp {
    fleet: Fleet,
}

#[tool_router]
impl FleetMcp {
    #[tool(
        name = "fleet_apps_list",
        description = "List apps available to the authenticated namespace and token.",
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
        description = "Get an app visible to the authenticated token.",
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
        name = "fleet_run_app_create",
        description = "Create a run app in the authenticated namespace. The token must have app-management access; mount roots are checked by the scheduler.",
        annotations(read_only_hint = false, destructive_hint = false)
    )]
    async fn run_app_create(
        &self,
        Parameters(input): Parameters<CreateRunApp>,
        Extension(parts): Extension<Parts>,
    ) -> Result<Json<Value>, ErrorData> {
        let mut args = vec![
            "app".into(),
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
            if !matches!(kind.as_str(), "proof" | "run") {
                return Err(tool_error("kind must be proof or run"));
            }
            args.extend(["--type".into(), kind]);
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
        name = "fleet_run_submit",
        description = "Submit a one-shot run job to an app available to the authenticated token. The command is passed directly without a shell.",
        annotations(read_only_hint = false, destructive_hint = false)
    )]
    async fn run_submit(
        &self,
        Parameters(input): Parameters<SubmitRun>,
        Extension(parts): Extension<Parts>,
    ) -> Result<Json<Value>, ErrorData> {
        if input.command.is_empty() {
            return Err(tool_error("command must contain at least one argument"));
        }
        if input.command.iter().any(|part| part.contains('\0')) {
            return Err(tool_error("command arguments cannot contain NUL bytes"));
        }
        let mut args = vec!["job".into(), "submit".into(), "--app".into(), input.app];
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
        args.push("--".into());
        args.extend(input.command);
        self.call(parts, args).await
    }

    #[tool(
        name = "fleet_job_cancel",
        description = "Cancel a visible run job and release its resources after cleanup. This stops the job.",
        annotations(read_only_hint = false, destructive_hint = true)
    )]
    async fn job_cancel(
        &self,
        Parameters(input): Parameters<JobId>,
        Extension(parts): Extension<Parts>,
    ) -> Result<Json<Value>, ErrorData> {
        self.call(parts, vec!["job".into(), "cancel".into(), input.id])
            .await
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
    let fleet = Fleet::from_env()?;
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
