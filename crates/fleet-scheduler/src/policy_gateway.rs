//! Central policy administration and scoped node authorization on Fleet's
//! existing gateway. The guest is pinned; this service only verifies proofs.
use crate::{config::SchedulerConfig, workloads::WorkloadController};
use axum::{
    extract::{Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
    Json, Router,
};
use fleet_proto::model::v1::{
    PolicyActivationRequest, PolicyBudgetRequest, PolicyCredentialRequest, PolicyStageRequest,
    RuntimeExecutionIdentity,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    io::Read,
    os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt},
    sync::Arc,
};
use tokio::sync::Semaphore;
use trusted_shim::{
    contracts::{ExecutionIdentity, ProofKind, Validity},
    ledger::{LedgerPermission, SqliteLedger},
    ledger_transport::{ExecutionAuthority, LedgerService},
    proof::ProgramRef,
    rail::{Error, EvaluationRequest, PolicyBundle, Result},
    ziren::ZirenGroth16Verifier,
};

pub struct PolicyGateway {
    ledger: Arc<SqliteLedger>,
    program: ProgramRef,
    program_vkey: String,
    elf: Vec<u8>,
    permits: Arc<Semaphore>,
}

impl PolicyGateway {
    #[cfg(test)]
    pub(crate) fn test_ledger(ledger: Arc<SqliteLedger>, program: ProgramRef) -> Arc<Self> {
        Arc::new(Self {
            ledger,
            program,
            program_vkey: String::new(),
            elf: vec![],
            permits: Arc::new(Semaphore::new(16)),
        })
    }
    pub(crate) fn open(cfg: &SchedulerConfig) -> anyhow::Result<Option<Arc<Self>>> {
        let Some(p) = &cfg.policy_gateway else {
            return Ok(None);
        };
        const MAX_ELF: u64 = 64 * 1024 * 1024;
        // A bounded, no-follow read prevents symlink surprises and accidental
        // unbounded artifacts; the digest authenticates the actual file bytes.
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&p.program_elf)?;
        anyhow::ensure!(
            file.metadata()?.is_file(),
            "policy ELF must be a regular file"
        );
        let mut elf = Vec::new();
        file.take(MAX_ELF + 1).read_to_end(&mut elf)?;
        anyhow::ensure!(
            !elf.is_empty()
                && elf.len() as u64 <= MAX_ELF
                && hex::encode(Sha256::digest(&elf)) == p.program_sha256,
            "policy ELF does not match the approved manifest"
        );
        let vkey = hex::decode(p.program_vkey.strip_prefix("0x").unwrap_or(&p.program_vkey))?;
        let program = ProgramRef {
            kind: ProofKind::PolicyEvaluation,
            program_sha256: p.program_sha256.clone(),
            verifying_key_sha256: hex::encode(Sha256::digest(vkey)),
        };
        let verifier = Arc::new(ZirenGroth16Verifier::new(program.clone(), &p.program_vkey)?);
        let dir = cfg.data_dir.join("policy");
        match std::fs::DirBuilder::new().mode(0o700).create(&dir) {
            Ok(()) => (),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => (),
            Err(e) => return Err(e.into()),
        }
        let meta = std::fs::symlink_metadata(&dir)?;
        anyhow::ensure!(
            meta.is_dir() && meta.permissions().mode() & 0o077 == 0,
            "policy ledger directory must be private and not a symlink"
        );
        let ledger = Arc::new(SqliteLedger::open(
            &dir.join("ledger.sqlite"),
            program.clone(),
            verifier,
        )?);
        Ok(Some(Arc::new(Self {
            ledger,
            program,
            program_vkey: p.program_vkey.clone(),
            elf,
            permits: Arc::new(Semaphore::new(16)),
        })))
    }
}

/// These routes are merged inside the existing administrative auth middleware.
pub(crate) fn admin_router(c: Arc<WorkloadController>) -> Router {
    if c.policy.is_none() {
        return Router::new();
    }
    Router::new()
        .route("/policy/program", get(program))
        .route("/policy/program/elf", get(elf))
        .route("/policy/budgets", put(budget))
        .route("/policy/allocations/activate", post(activate))
        .route("/policy/allocations/revoke", post(revoke))
        .route("/policy/requests", post(stage))
        .route("/policy/requests/:id", get(request))
        .route("/policy/credentials", post(credential))
        .route("/policy/credentials/:id", delete(revoke_credential))
        .with_state(c)
}

/// The gRPC methods use their own allocation-scoped ledger credentials, never
/// an admin/namespace token. Keep them outside the general Fleet middleware.
pub(crate) fn node_router(c: Arc<WorkloadController>) -> Router {
    let Some(p) = &c.policy else {
        return Router::new();
    };
    let service = LedgerService::new(p.ledger.clone())
        .with_authority(c.clone())
        .into_service();
    Router::new()
        .route_service(
            "/trusted.ledger.v1.AuthorizationLedger/Reserve",
            service.clone(),
        )
        .route_service("/trusted.ledger.v1.AuthorizationLedger/Consume", service)
}

fn identity(e: RuntimeExecutionIdentity) -> ExecutionIdentity {
    ExecutionIdentity {
        namespace: e.namespace,
        workload: e.workload,
        allocation_id: e.allocation_id,
        lease_id: e.lease_id,
        generation: e.generation,
        node_id: e.node_id,
    }
}
fn failure(e: Error) -> Response {
    let status = match e {
        Error::InvalidInput | Error::Unsupported | Error::EvaluationFailed => {
            StatusCode::BAD_REQUEST
        }
        Error::Rejected => StatusCode::FORBIDDEN,
        Error::Conflict => StatusCode::CONFLICT,
        Error::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
    };
    (status, "policy operation rejected or unavailable").into_response()
}
async fn run(
    c: Arc<WorkloadController>,
    execution: Option<ExecutionIdentity>,
    op: impl FnOnce(&SqliteLedger) -> Result<Value> + Send + 'static,
) -> Response {
    let Some(p) = &c.policy else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Ok(permit) = p.permits.clone().try_acquire_owned() else {
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    };
    let p = p.clone();
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let _guard = execution.as_ref().map(|e| c.guard(e)).transpose()?;
        op(&p.ledger)
    })
    .await
    .unwrap_or(Err(Error::Unavailable));
    match result {
        Ok(v) => ([(header::CACHE_CONTROL, "no-store")], Json(v)).into_response(),
        Err(e) => failure(e),
    }
}
async fn program(State(c): State<Arc<WorkloadController>>) -> Response {
    let p = c.policy.as_ref().unwrap();
    Json(
        json!({"kind":p.program.kind,"program_sha256":p.program.program_sha256,
        "verifying_key_sha256":p.program.verifying_key_sha256,"program_vkey":p.program_vkey}),
    )
    .into_response()
}
async fn elf(State(c): State<Arc<WorkloadController>>) -> Response {
    (
        [(header::CONTENT_TYPE, "application/octet-stream")],
        c.policy.as_ref().unwrap().elf.clone(),
    )
        .into_response()
}
async fn budget(
    State(c): State<Arc<WorkloadController>>,
    Json(r): Json<PolicyBudgetRequest>,
) -> Response {
    run(c, None, move |l| {
        l.set_budget(&r.name, r.allowance)?;
        Ok(json!({}))
    })
    .await
}
async fn activate(
    State(c): State<Arc<WorkloadController>>,
    Json(r): Json<PolicyActivationRequest>,
) -> Response {
    let policy: PolicyBundle = match serde_json::from_value(r.policy.0) {
        Ok(v) => v,
        Err(_) => return failure(Error::InvalidInput),
    };
    let e = identity(r.execution);
    run(c, Some(e.clone()), move |l| {
        l.activate(&e, &policy, &r.context.0, &r.previous_state_root, &r.budget)?;
        Ok(json!({}))
    })
    .await
}
async fn stage(
    State(c): State<Arc<WorkloadController>>,
    Json(r): Json<PolicyStageRequest>,
) -> Response {
    let request: EvaluationRequest = match serde_json::from_value(r.request.0) {
        Ok(v) => v,
        Err(_) => return failure(Error::InvalidInput),
    };
    run(c, Some(request.execution.clone()), move |l| {
        let policy = l.stage(&r.job_id, &request, r.cost)?;
        Ok(json!({"job_id":r.job_id,"policy":policy,"request":request}))
    })
    .await
}
async fn request(State(c): State<Arc<WorkloadController>>, Path(id): Path<String>) -> Response {
    run(c, None, move |l| Ok(json!(l.request(&id)?))).await
}
async fn revoke(
    State(c): State<Arc<WorkloadController>>,
    Json(e): Json<RuntimeExecutionIdentity>,
) -> Response {
    // Revocation must work after the node or allocation has already gone away.
    run(c, None, move |l| {
        l.revoke(&identity(e))?;
        Ok(json!({}))
    })
    .await
}
async fn credential(
    State(c): State<Arc<WorkloadController>>,
    Json(r): Json<PolicyCredentialRequest>,
) -> Response {
    let permission = match r.permission.as_str() {
        "admission" => LedgerPermission::Admission,
        "execution" => LedgerPermission::Execution,
        "admission_and_execution" => LedgerPermission::AdmissionAndExecution,
        _ => return failure(Error::InvalidInput),
    };
    let e = identity(r.execution);
    run(c, Some(e.clone()), move |l| {
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Error::Unavailable)?.as_secs();
        let token = l.issue_access(&e, permission, &Validity { not_before: now, expires_at: r.expires_at })?;
        Ok(json!({"id":hex::encode(Sha256::digest(token.as_bytes())),"token":token,"expires_at":r.expires_at}))
    }).await
}
async fn revoke_credential(
    State(c): State<Arc<WorkloadController>>,
    Path(id): Path<String>,
) -> Response {
    run(c, None, move |l| {
        l.revoke_access_id(&id)?;
        Ok(json!({}))
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn approved_guest_and_private_durable_ledger_are_required() {
        let dir = tempfile::tempdir().unwrap();
        let elf = dir.path().join("guest.elf");
        std::fs::write(&elf, b"\x7fELF-policy-fixture").unwrap();
        let mut cfg: SchedulerConfig = serde_yaml::from_str(&format!(
            "data_dir: {}\ncluster_token_file: cluster\n",
            dir.path().display()
        ))
        .unwrap();
        assert!(PolicyGateway::open(&cfg).unwrap().is_none());
        cfg.policy_gateway = Some(crate::config::PolicyGatewayConfig {
            program_elf: elf.clone(),
            program_sha256: hex::encode(Sha256::digest(std::fs::read(&elf).unwrap())),
            program_vkey: "01".repeat(32),
        });
        let p = PolicyGateway::open(&cfg).unwrap().unwrap();
        p.ledger.set_budget("durable", 1).unwrap();
        drop(p);
        assert!(PolicyGateway::open(&cfg).is_ok());
        // A database may not silently move to a different approved program key.
        cfg.policy_gateway.as_mut().unwrap().program_vkey = "02".repeat(32);
        assert!(PolicyGateway::open(&cfg).is_err());
        cfg.policy_gateway.as_mut().unwrap().program_vkey = "01".repeat(32);
        std::fs::write(&elf, b"substituted ELF").unwrap();
        assert!(PolicyGateway::open(&cfg).is_err());
        cfg.policy_gateway.as_mut().unwrap().program_sha256 =
            hex::encode(Sha256::digest(b"substituted ELF"));
        let link = dir.path().join("linked.elf");
        std::os::unix::fs::symlink(&elf, &link).unwrap();
        cfg.policy_gateway.as_mut().unwrap().program_elf = link;
        assert!(PolicyGateway::open(&cfg).is_err());
    }
}
