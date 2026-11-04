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
use fleet_proto::stage::v1::GenerateProofRequest;
pub use fleet_proto::sdk::SubmitProofRequest;

use crate::{blobs, stage::ProofStageService, store::{App, ProofJob, ProofJobFilter, ProofJobState, Token}, workloads::WorkloadController};

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
            let api = ProofStageService { fleet: fleet.clone() };
            let response = api.generate(
                GenerateProofRequest {
                    proof_id: body.proof_id,
                    elf_id: Some(body.elf_id),
                    signature: body.signature,
                    block_no: body.block_no,
                    seg_size: body.seg_size,
                    from_step: body.from_step,
                    target_step: body.target_step,
                    ..Default::default()
                },
                Some(token.clone()),
                Some(body.inputs),
                Some(&app.name),
            ).await;
            Ok(Json(serde_json::json!({"proof_id":response.proof_id,"status":response.status,"error_message":response.error_message})).into_response())
        }
        (&Method::GET, [id]) => Ok(Json(w.proof_job_value(&job(w, app, tag, id).await?)).into_response()),
        (&Method::GET, [id, "output", name]) => {
            if !matches!(*name, blobs::PROOF | blobs::PUBLIC_VALUES) {
                return Err(fail(404, "unknown proof output"));
            }
            let j = job(w, app, tag, id).await?;
            if j.state() != ProofJobState::Succeeded {
                return Err(fail(409, "proof has not succeeded"));
            }
            match fleet.proof_output(&j, if *name == blobs::PROOF { blobs::PROOF } else { blobs::PUBLIC_VALUES }).await {
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
