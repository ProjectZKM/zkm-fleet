//! Exec for an exact native launch, through the stock gateway's interactive
//! exec: an administrator's by launch name, and an app owner's by replica, which
//! the launch's activated policy decides first. The sandbox supervisor and its
//! OpenShell process policy run every command; Fleet binds the request to a live,
//! owned allocation, bounds it and never executes anything on a node itself.
//! Lifecycle changes remain Fleet allocation operations; application output stays
//! on the node.
use super::*;
use crate::config::OpenShellGatewayConfig;
use sha2::{Digest, Sha256};
use trusted_shim::{contracts::ExecutionIdentity, ledger_transport::ExecutionAuthority, rail::Decision, AuthorizationLedger};
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
/// Concurrent execs per scheduler.
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

/// The stock gateway's public API, as the scheduler uses it.
#[async_trait::async_trait]
pub(crate) trait ExecGateway: Send + Sync {
    /// One bounded non-TTY exec. Implementations must deliver all of `stdin` then
    /// end it, and fail rather than truncate output beyond `MAX_STREAM`.
    async fn exec(&self, start: protocol::ExecSandboxRequest, stdin: Vec<u8>) -> Result<ExecOutput>;
    /// Creates `workspace` if it is missing, then the sandbox, returning once the
    /// gateway's create has: its compute callback proves and delivers the launch
    /// meanwhile. A sandbox the gateway already holds is that same create.
    async fn create(&self, workspace: &str, request: protocol::CreateSandboxRequest) -> Result<()>;
}

pub(crate) struct StockGateway(OpenShellClient<tonic_native::transport::Channel>);
impl StockGateway {
    pub(crate) fn new(config: &OpenShellGatewayConfig) -> anyhow::Result<Self> {
        Ok(Self(readiness::gateway_client(config, None)?))
    }
}

#[async_trait::async_trait]
impl ExecGateway for StockGateway {
    async fn create(&self, workspace: &str, request: protocol::CreateSandboxRequest) -> Result<()> {
        use tonic_native::Code::AlreadyExists;
        let mut client = self.0.clone();
        let made = client
            .create_workspace(protocol::CreateWorkspaceRequest { name: workspace.into(), ..Default::default() })
            .await;
        if made.is_err_and(|status| status.code() != AlreadyExists) {
            return Err(Error::Unavailable);
        }
        match client.create_sandbox(request).await {
            Ok(_) => Ok(()),
            Err(status) if status.code() == AlreadyExists => Ok(()),
            Err(_) => Err(Error::Unavailable),
        }
    }
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
    private(exec_inner(&c, &name, input, None).await.map_err(|refusal| match refusal {
        Refusal::Rail(e) => e,
        _ => Error::Rejected,
    }))
}

/// What refused an exec.
pub(crate) enum Refusal {
    Rail(Error),
    /// The policy's own answer, with the approval request it opened, if any.
    Policy(serde_json::Value),
    /// The app has no such replica, or several and none was named.
    Replica,
    /// The named approval is not approved, is spent or expired, or is another exec's.
    Approval,
}
impl From<Error> for Refusal {
    fn from(e: Error) -> Self {
        Self::Rail(e)
    }
}

/// Who sends an owner's exec, and the approval it names.
pub(crate) struct Caller {
    /// The token's ID; none for an administrator's.
    pub token: Option<String>,
    pub approval: Option<String>,
}

/// `POST /apps/NAME/exec`: the owner's exec in one replica of its native app.
pub(crate) async fn app_exec(
    c: &Arc<WorkloadController>,
    namespace: &str,
    app: &str,
    replica: Option<&str>,
    caller: Caller,
    input: ExecInput,
) -> std::result::Result<serde_json::Value, Refusal> {
    let mut replicas = c.native_reservations().into_iter().filter(|r| {
        r.execution.namespace == namespace
            && r.execution.workload == app
            && replica.is_none_or(|id| r.execution.allocation_id == id)
    });
    // One replica runs the command; an app with several names which.
    let (Some(r), None) = (replicas.next(), replicas.next()) else {
        return Err(Refusal::Replica);
    };
    let name = GatewayLaunch::execution_name(&r.execution).map_err(|_| Error::InvalidInput)?;
    exec_inner(c, &name, input, Some(caller)).await
}

/// Rail's decision on one exec under the launch's activated policy, as operation
/// `exec` on the replica. The ledger evaluates it, draws 1 from the launch's
/// budget for an allow and queues the decision's proof; the grant is consumed
/// here, so a decision runs one command.
///
/// `approval_required` opens an approval request for this exec. Sent again
/// with that request approved, the exec is evaluated afresh with the approval
/// as `parameters.approval`; the policy decides what an approval is worth, and
/// an allow spends it.
async fn decide(
    c: &Arc<WorkloadController>,
    record: &Record,
    e: &ExecutionIdentity,
    input: &ExecInput,
    stdin: &[u8],
    caller: Caller,
) -> std::result::Result<serde_json::Value, Refusal> {
    let policy = c.policy.clone().filter(|p| p.preflight.is_some()).ok_or(Error::Unavailable)?;
    let store = launches(c)?;
    let job_id = format!("exec-{}", uuid::Uuid::new_v4().simple());
    let asked = json!({"command": input.command, "user": input.user,
        "stdin_sha256": hex::encode(Sha256::digest(stdin)), "timeout_seconds": input.timeout_seconds});
    let approved = match &caller.approval {
        Some(id) => Some(store.approved(id, e, &asked).ok_or(Refusal::Approval)?),
        None => None,
    };
    let mut parameters = asked.clone();
    if let Some(approval) = &approved {
        parameters["approval"] = approval.fact();
    }
    let (controller, ledger, e, principal, id) =
        (c.clone(), policy.ledger.clone(), e.clone(), record.preparation.principal.clone(), job_id.clone());
    let (e, result) = tokio::task::spawn_blocking(move || {
        let _guard = controller.guard(&e)?;
        let (_, context, previous_state_root) = ledger.authority_snapshot(&e)?;
        let request = EvaluationRequest {
            execution: e.clone(),
            principal,
            action_id: id.clone(),
            operation: "exec".into(),
            resource: e.allocation_id.clone(),
            parameters,
            context,
            previous_state_root,
            evaluated_at_unix: now(),
            validity: Validity { not_before: now(), expires_at: now() + 60 },
        };
        Ok::<_, Error>((e, ledger.preflight(&id, &request, 1)?))
    })
    .await
    .unwrap_or(Err(Error::Unavailable))?;
    let decision = json!({"job_id": job_id, "decision": result.decision.decision,
        "reason_codes": result.decision.reason_codes, "policy": result.decision.policy});
    let Some(grant) = result.grant else {
        let mut refusal = json!({"decision": decision});
        if result.decision.decision == Decision::ApprovalRequired && approved.is_none() {
            // Without a free request slot the answer stands, with nothing to approve.
            refusal["approval"] = store
                .request_approval(&job_id, &e, asked, &record.preparation.principal, result.decision.reason_codes, caller.token)
                .unwrap_or_default();
        }
        return Err(Refusal::Policy(refusal));
    };
    if let Some(approval) = &approved {
        store.spend(&approval.id).map_err(|_| Refusal::Approval)?;
    }
    policy.ledger.consume(&grant, &e, &grant.request_sha256).await?;
    Ok(decision)
}

async fn exec_inner(
    c: &Arc<WorkloadController>,
    name: &str,
    input: ExecInput,
    owner: Option<Caller>,
) -> std::result::Result<serde_json::Value, Refusal> {
    if input.command.is_empty()
        || input.command.len() > MAX_ARGS
        || input.command.iter().any(|a| a.contains('\0'))
        || input.command.iter().map(|a| a.len() + 3).sum::<usize>() > MAX_COMMAND
        || !(1..=MAX_TIMEOUT).contains(&input.timeout_seconds)
    {
        return Err(Error::InvalidInput.into());
    }
    let stdin = STANDARD.decode(&input.stdin).map_err(|_| Error::InvalidInput)?;
    if stdin.len() > MAX_STREAM {
        return Err(Error::InvalidInput.into());
    }
    let store = launches(c)?;
    let gateway = store.control.clone().ok_or(Error::Unavailable)?;
    let record = store.get(name)?;
    // Only a sandbox this allocation created; never before delivery completes.
    if record.phase != "created" {
        return Err(Error::Rejected.into());
    }
    // The process identity is the approved native policy's, not the caller's.
    if let Some(user) = &input.user {
        if user.is_empty() || native_user(&record)? != *user {
            return Err(Error::Rejected.into());
        }
    }
    let launch = record.launch()?;
    let workspace_scope = record.binding()?.request().workspace_scope;
    // Current owning node session, generation, deployment intent and quota.
    c.with_runtime_delivery(&launch.execution, None, Some(&launch), |_| Ok(()))?;
    let _slot = store.control_slots.try_acquire().map_err(|_| Error::Unavailable)?;
    let decision = match owner {
        Some(caller) => Some(decide(c, &record, &launch.execution, &input, &stdin, caller).await?),
        None => None,
    };
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
    let mut result = json!({"stdout":STANDARD.encode(output.stdout),"stderr":STANDARD.encode(output.stderr),
        "exit_code":output.exit_code});
    if let Some(decision) = decision {
        result["decision"] = decision;
    }
    Ok(result)
}
