//! Administrative exec for an exact native launch, through the stock gateway's
//! interactive exec. The sandbox supervisor and its OpenShell process policy run
//! every command; Fleet binds the request to a live, owned allocation, bounds it
//! and never executes anything on a node itself. Lifecycle changes remain Fleet
//! allocation operations; application output stays on the node.
use super::*;
use crate::config::OpenShellGatewayConfig;
use trusted_shim::openshell_gateway::protocol::{
    self, exec_sandbox_event, exec_sandbox_input, open_shell_client::OpenShellClient,
};

/// The same bounds as shim's Docker control listener, so callers keep one shape.
const MAX_ARGS: usize = 256;
/// The stock gateway's assembled command limit.
const MAX_COMMAND: usize = 256 * 1024;
const MAX_STREAM: usize = 64 * 1024 * 1024;
const MAX_TIMEOUT: u64 = 600;
/// Below the stock gateway's 1 MiB gRPC message limit.
const STDIN_CHUNK: usize = 512 * 1024;
/// Concurrent administrative execs per scheduler.
pub(super) const CONTROL_SLOTS: usize = 4;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExecInput {
    command: Vec<String>,
    #[serde(default)]
    user: Option<String>,
    /// Base64, at most 64 MiB decoded.
    #[serde(default)]
    stdin: String,
    timeout_seconds: u64,
}

pub(crate) struct ExecOutput {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_code: i32,
}

/// One bounded non-TTY exec. Implementations must deliver all of `stdin` then
/// end it, and fail rather than truncate output beyond `MAX_STREAM`.
#[async_trait::async_trait]
pub(crate) trait ExecGateway: Send + Sync {
    async fn exec(&self, start: protocol::ExecSandboxRequest, stdin: Vec<u8>) -> Result<ExecOutput>;
}

pub(crate) struct StockGateway(OpenShellClient<tonic_native::transport::Channel>);
impl StockGateway {
    pub(crate) fn new(config: &OpenShellGatewayConfig) -> anyhow::Result<Self> {
        Ok(Self(readiness::gateway_client(config, None)?))
    }
}

#[async_trait::async_trait]
impl ExecGateway for StockGateway {
    async fn exec(&self, start: protocol::ExecSandboxRequest, stdin: Vec<u8>) -> Result<ExecOutput> {
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let first = protocol::ExecSandboxInput {
            payload: Some(exec_sandbox_input::Payload::Start(start)),
        };
        tx.send(first).await.map_err(|_| Error::Unavailable)?;
        // Feed input concurrently with output so neither side can stall the other.
        let feeder = tokio::spawn(async move {
            for frame in stdin_frames(&stdin) {
                if tx.send(frame).await.is_err() {
                    return;
                }
            }
            // Dropping the sender ends the request stream: remote stdin EOF.
        });
        let _feeder = AbortOnDrop(feeder);
        let mut events = self
            .0
            .clone()
            .exec_sandbox_interactive(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await
            .map_err(|_| Error::Unavailable)?
            .into_inner();
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        while let Some(event) = events.message().await.map_err(|_| Error::Unavailable)? {
            match event.payload {
                Some(exec_sandbox_event::Payload::Stdout(o)) => append(&mut stdout, &o.data)?,
                Some(exec_sandbox_event::Payload::Stderr(e)) => append(&mut stderr, &e.data)?,
                Some(exec_sandbox_event::Payload::Exit(exit)) => {
                    return Ok(ExecOutput { stdout, stderr, exit_code: exit.exit_code })
                }
                None => return Err(Error::Unavailable),
            }
        }
        // A stream without an exit status is an unknown outcome, not success.
        Err(Error::Unavailable)
    }
}

/// Stdin as gateway frames, each well below the stock gRPC message limit.
pub(crate) fn stdin_frames(stdin: &[u8]) -> impl Iterator<Item = protocol::ExecSandboxInput> + '_ {
    stdin.chunks(STDIN_CHUNK).map(|chunk| protocol::ExecSandboxInput {
        payload: Some(exec_sandbox_input::Payload::Stdin(chunk.to_vec())),
    })
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn append(buffer: &mut Vec<u8>, data: &[u8]) -> Result<()> {
    if buffer.len() + data.len() > MAX_STREAM {
        return Err(Error::Unavailable);
    }
    buffer.extend_from_slice(data);
    Ok(())
}

fn native_user(record: &Record) -> Result<String> {
    let raw = STANDARD
        .decode(&record.preparation.native_policy_base64)
        .map_err(|_| Error::InvalidInput)?;
    let policy = SandboxPolicy::decode(raw.as_slice()).map_err(|_| Error::InvalidInput)?;
    Ok(policy.process.map(|p| p.run_as_user).unwrap_or_default())
}

pub(super) async fn exec(
    State(c): State<Arc<WorkloadController>>,
    ApiPath(name): ApiPath<String>,
    Json(input): Json<ExecInput>,
) -> HttpResponse {
    private(exec_inner(&c, &name, input).await)
}

async fn exec_inner(c: &Arc<WorkloadController>, name: &str, input: ExecInput) -> Result<serde_json::Value> {
    if input.command.is_empty()
        || input.command.len() > MAX_ARGS
        || input.command.iter().any(|a| a.contains('\0'))
        || input.command.iter().map(|a| a.len() + 3).sum::<usize>() > MAX_COMMAND
        || !(1..=MAX_TIMEOUT).contains(&input.timeout_seconds)
    {
        return Err(Error::InvalidInput);
    }
    let stdin = STANDARD.decode(&input.stdin).map_err(|_| Error::InvalidInput)?;
    if stdin.len() > MAX_STREAM {
        return Err(Error::InvalidInput);
    }
    let store = launches(c)?;
    let gateway = store.control.clone().ok_or(Error::Unavailable)?;
    let record = store.get(name)?;
    // Only a sandbox this allocation created; never before delivery completes.
    if record.phase != "created" {
        return Err(Error::Rejected);
    }
    // The process identity is the approved native policy's, not the caller's.
    if let Some(user) = &input.user {
        if user.is_empty() || native_user(&record)? != *user {
            return Err(Error::Rejected);
        }
    }
    let launch = record.launch()?;
    let workspace_scope = record.binding()?.request().workspace_scope;
    // Current owning node session, generation, deployment intent and quota.
    c.with_runtime_delivery(&launch.execution, None, Some(&launch), |_| Ok(()))?;
    let _slot = store.control_slots.try_acquire().map_err(|_| Error::Unavailable)?;
    let mut start = protocol::ExecSandboxRequest {
        workspace_scope,
        sandbox: record.name.clone(),
        command: input.command,
        tty: false,
        no_login_shell: true,
        // A retried transport never relaunches the same command.
        request_id: uuid::Uuid::new_v4().to_string(),
        execution_timeout: Some(Default::default()),
        ..Default::default()
    };
    if let Some(timeout) = start.execution_timeout.as_mut() {
        timeout.seconds = input.timeout_seconds as i64;
    }
    let output = tokio::time::timeout(
        Duration::from_secs(input.timeout_seconds + 30),
        gateway.exec(start, stdin),
    )
    .await
    .map_err(|_| Error::Unavailable)??;
    Ok(json!({"stdout":STANDARD.encode(output.stdout),"stderr":STANDARD.encode(output.stderr),
        "exit_code":output.exit_code}))
}
