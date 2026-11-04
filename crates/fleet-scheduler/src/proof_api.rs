//! A proof app's jobs: `/apps/NAME/jobs[/ID[/output/FILE]]`, as a batch's are under
//! `/batches/NAME/jobs`. A submission carries app-owned storage references, not payloads.
use std::collections::BTreeMap;

use axum::{
    body::to_bytes,
    extract::Request,
    http::Method,
    response::{IntoResponse, Response},
    Json,
};
use fleet_proto::artifacts::{self, is_safe_id};
use fleet_proto::fleet::v1::ProofKind;
pub use fleet_proto::sdk::{kind_for, SubmitProofRequest};
use fleet_proto::stage::v1::Status as StageStatus;

use crate::fleet::{now_ms, Scheduler};
use crate::store::{normalize_address, App, ProofJob, ProofJobFilter, ProofJobState, Token};
use crate::{token, workloads::WorkloadController};

fn fail(status: u16, message: impl Into<String>) -> anyhow::Error {
    crate::fleet::RequestRejection { status, message: message.into() }.into()
}

/// `rest` is the path after `/apps/NAME/jobs`; the caller is granted `app`, in its namespace.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn app_jobs(w: &WorkloadController, c: Option<&Token>, q: &BTreeMap<String, String>, app: &App,
    tag: Option<&str>, method: &Method, rest: &[&str], request: Request,
) -> anyhow::Result<Response> {
    let fleet = w.fleet();
    match (method, rest) {
        (&Method::GET, []) => {
            let limit = q.get("limit").and_then(|n| n.parse::<usize>().ok()).unwrap_or(50).min(1000);
            let active = q.get("active").is_some_and(|a| a == "1" || a == "true");
            let jobs = fleet.recent_jobs(&ProofJobFilter {
                namespace: Some(app.namespace.clone()), tag: tag.map(str::to_owned), app: Some(app.name.clone()), active, owner: None,
            }, limit).await?;
            Ok(Json(jobs.iter().map(|j| w.proof_job_value(j)).collect::<Vec<_>>()).into_response())
        }
        (&Method::POST, []) => {
            let Some(token) = c else {
                return Err(fail(403, "an administrator's token submits no proofs: use a namespace token"));
            };
            let body: SubmitProofRequest = serde_json::from_slice(&to_bytes(request.into_body(), 1 << 20).await?)?;
            let proof_id = body.proof_id.clone();
            let (status, error) = match submit(fleet, body, token, &app.name).await {
                Ok(()) => (StageStatus::Computing, String::new()),
                Err((status, error)) => {
                    tracing::warn!(proof_id, "rejected: {error}");
                    (status, error)
                }
            };
            Ok(Json(serde_json::json!({"proof_id":proof_id,"status":status as i32,"error_message":error})).into_response())
        }
        (&Method::GET, [id]) => Ok(Json(w.proof_job_value(&job(w, app, tag, id).await?)).into_response()),
        (&Method::GET, [id, "output", name]) => {
            if !matches!(*name, artifacts::PROOF | artifacts::PUBLIC_VALUES) {
                return Err(fail(404, "unknown proof output"));
            }
            let j = job(w, app, tag, id).await?;
            if j.state() != ProofJobState::Succeeded {
                return Err(fail(409, "proof has not succeeded"));
            }
            match fleet.proof_output(&j, if *name == artifacts::PROOF { artifacts::PROOF } else { artifacts::PUBLIC_VALUES }).await {
                Ok(bytes) => Ok(([(axum::http::header::CONTENT_TYPE, "application/octet-stream")], bytes).into_response()),
                Err(e) => Ok(crate::proof_fetch::response_error(e)),
            }
        }
        _ => Err(fail(404, "unknown proof job operation")),
    }
}

/// The app's job `id`: the client's proof id (in the selected tag; in any, for an
/// administrator who selects none), or the internal one.
async fn job(w: &WorkloadController, app: &App, tag: Option<&str>, id: &str) -> anyhow::Result<ProofJob> {
    let fleet = w.fleet();
    let found = match tag {
        Some(tag) => fleet.namespace_job(&app.namespace, Some(tag).filter(|t| !t.is_empty()), id).await?,
        None => {
            let all = fleet.recent_jobs(&ProofJobFilter {
                namespace: Some(app.namespace.clone()), app: Some(app.name.clone()), ..Default::default()
            }, usize::MAX).await?;
            let mut named = all.into_iter().filter(|j| j.client_id == id);
            match (named.next(), named.next()) {
                (Some(_), Some(_)) => return Err(fail(409, format!("{id} names a job in several tags: select one with --tag"))),
                (one, _) => one,
            }
        }
    };
    let found = match found {
        Some(j) => Some(j),
        None => fleet.job(id).await?,
    };
    found.filter(|j| j.namespace == app.namespace && j.app == app.name && tag.is_none_or(|t| j.tag == t))
        .ok_or_else(|| fail(404, format!("app {} has no job {id}", app.name)))
}

/// Submits a proof job of `app` for `caller`, its inputs in app storage. A refusal is also
/// recorded as a failed job: the SDK, through the app's endpoint, learns a submission's
/// outcome by polling its proof id.
async fn submit(fleet: &Scheduler, req: SubmitProofRequest, caller: &Token, app: &str) -> Result<(), (StageStatus, String)> {
    let id = req.proof_id.clone();
    let refuse = |status, msg: &str| Err((status, msg.to_string()));
    if !is_safe_id(&id) {
        return refuse(StageStatus::InvalidParameter, "proof_id must be 1-128 of [A-Za-z0-9_-]");
    }
    if let Err(error) = artifacts::validate_inputs(&req.inputs, &req.elf_id) {
        return refuse(StageStatus::InvalidParameter, &error.to_string());
    }
    let Some(namespace) = caller.namespace.clone() else {
        return refuse(StageStatus::InvalidParameter, "an administrator's token submits no proofs: use a namespace token");
    };
    if !token::allowed(Some(caller), token::Permission::Submit) {
        return refuse(StageStatus::InvalidParameter, "permission denied: the token may not submit jobs");
    }
    // Serialize id reservation: another token cannot race a submission with the same id.
    let _submission = fleet.submissions.lock().await;
    let signed = token::recover_signer(&req.signature, &id, req.block_no, req.seg_size).ok()
        .is_some_and(|a| normalize_address(&a.to_string()) == caller.address);
    if !signed {
        return refuse(StageStatus::InvalidParameter, "the proof signature must match the bearer token");
    }
    let tag = caller.tag.clone();
    let job_id = match fleet.namespace_job(&namespace, tag.as_deref(), &id).await {
        Ok(Some(j)) => j.id,
        Ok(None) => Scheduler::internal_proof_id(&namespace, tag.as_deref(), &id),
        Err(e) => return refuse(StageStatus::InternalError, &format!("{e:#}")),
    };
    if let Ok(Some(job)) = fleet.job(&job_id).await {
        if caller.apps.as_ref().is_some_and(|apps| !apps.contains(&job.app)) {
            return refuse(StageStatus::InvalidParameter, "app is not granted to this token");
        }
        if job.inputs.as_ref() != Some(&req.inputs) || job.elf_id != req.elf_id
            || kind_for(req.from_step, req.target_step).ok() != Some(job.kind()) {
            return refuse(StageStatus::InvalidParameter, "proof_id already exists with different inputs or parameters");
        }
        if job.namespace != namespace || job.tag != tag.as_deref().unwrap_or("") {
            return refuse(StageStatus::InvalidParameter, "permission denied");
        }
        return Ok(());
    }
    // The app the job is for, once known (or the registered one it was submitted to): a
    // refusal is recorded under it, where its client polls.
    let mut named = Some(app).filter(|a| fleet.app_known(&namespace, a)).unwrap_or_default().to_string();
    let outcome: Result<ProofJob, (StageStatus, String)> = (|| {
        let kind = kind_for(req.from_step, req.target_step).map_err(|m| (StageStatus::InvalidParameter, m.to_string()))?;
        let elf_id = req.elf_id.trim_start_matches("0x").to_ascii_lowercase();
        let registered = fleet.app_for_elf(&namespace, &elf_id).ok_or_else(|| (StageStatus::InvalidParameter,
            format!("program {elf_id} is not an app of this namespace: create it first (fleetctl app add NAME --spec FILE --elf-id {elf_id})")))?;
        if registered.name != app {
            return Err((StageStatus::InvalidParameter, format!("program {elf_id} is app {}, not {app}", registered.name)));
        }
        named = registered.name.clone();
        if caller.apps.as_ref().is_some_and(|g| !g.contains(&registered.name)) {
            return Err((StageStatus::InvalidParameter, "app is not granted to this token".into()));
        }
        if fleet.queued_for(&namespace, &registered.name) >= registered.max_queued as usize {
            return Err((StageStatus::InternalError, format!("app {} has {} jobs queued or running, its most", registered.name, registered.max_queued)));
        }
        if fleet.app_worker_owned(&namespace, &registered.name) {
            return Err((StageStatus::InvalidParameter, "this app owns its input stream; SDK proof submission is unavailable".into()));
        }
        if registered.disabled {
            return Err((StageStatus::InvalidParameter, format!("app {} is disabled: it takes no new jobs", registered.name)));
        }
        Ok(ProofJob {
            inputs: Some(req.inputs.clone()), outputs: None,
            id: job_id.clone(),
            namespace: namespace.clone(),
            tag: tag.clone().unwrap_or_default(),
            client_id: id.clone(),
            app: registered.name.clone(),
            owner: caller.address.clone(),
            elf_id,
            priority: registered.priority,
            kind: kind as i32,
            state: ProofJobState::Queued as i32, fail_status: None,
            error: String::new(),
            node: None,
            attempts: 0,
            failed_on: vec![],
            cycles: 0,
            prove_ms: 0,
            created_ms: now_ms(),
            started_ms: None,
            finished_ms: None,
        })
    })();
    let refused = match outcome {
        Ok(job) => {
            tracing::info!(proof_id = %id, app = %job.app, kind = ?job.kind, "accepted");
            match fleet.submit(job).await {
                Ok(_) => return Ok(()),
                Err(e) => return refuse(StageStatus::InternalError, &format!("{e:#}")),
            }
        }
        Err(refused) => refused,
    };
    record_refusal(fleet, &job_id, &id, &refused, caller, &named).await;
    Err(refused)
}

async fn record_refusal(fleet: &Scheduler, job_id: &str, client_id: &str, (status, msg): &(StageStatus, String),
    caller: &Token, app: &str,
) {
    if !is_safe_id(job_id) {
        return;
    }
    let job = ProofJob {
        inputs: None, outputs: None,
        id: job_id.to_string(),
        client_id: client_id.to_string(),
        namespace: caller.namespace.clone().unwrap_or_default(),
        tag: caller.tag.clone().unwrap_or_default(),
        app: app.to_string(),
        owner: caller.address.clone(),
        elf_id: String::new(),
        priority: 0,
        kind: ProofKind::Unspecified as i32,
        state: ProofJobState::Failed as i32,
        fail_status: Some(*status as i32),
        error: msg.clone(),
        node: None,
        attempts: 0,
        failed_on: vec![],
        cycles: 0,
        prove_ms: 0,
        created_ms: now_ms(),
        started_ms: None,
        finished_ms: Some(now_ms()),
    };
    let _ = fleet.submit(job).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use fleet_proto::stage::v1::Step;

    #[test]
    fn steps_map_to_kinds_like_the_sdk_sets_them() {
        let s = |x: Step| Some(x as i32);
        assert_eq!(kind_for(None, s(Step::InAgg)), Ok(ProofKind::Compressed));
        assert_eq!(kind_for(None, s(Step::InSnark)), Ok(ProofKind::Groth16));
        assert_eq!(kind_for(None, None), Ok(ProofKind::Groth16));
        assert_eq!(kind_for(s(Step::InAgg), s(Step::InSnark)), Ok(ProofKind::CompressToGroth16));
        assert!(kind_for(None, s(Step::InSplit)).is_err());
    }
}
