//! The SDK-compatible stage API: `ZKM_PROVER=network` clients talk to this unchanged.

use std::sync::Arc;
use std::time::Duration;

use tonic::{Request, Response, Status};

use fleet_proto::fleet::v1::ProofKind;
use fleet_proto::stage::v1::{
    stage_service_server::StageService, GenerateProofRequest, GenerateProofResponse,
    GetStatusRequest, GetStatusResponse, Status as StageStatus, Step,
};

use crate::auth;
use crate::blobs::{self, is_safe_id};
use crate::fleet::{now_ms, Fleet};
use crate::store::{normalize_address, Job, JobState, Token};

pub struct StageApi {
    pub fleet: Arc<Fleet>,
}

#[derive(Clone)]
struct ProofCaller {
    tenant: String,
    address: String,
    token: Option<Token>,
}

/// Maps the request's steps to a proof kind, as the SDK's network prover sets them.
pub fn kind_for(
    from_step: Option<i32>,
    target_step: Option<i32>,
) -> Result<ProofKind, &'static str> {
    let from = from_step.unwrap_or(Step::Init as i32);
    let target = target_step.unwrap_or(Step::InSnark as i32);
    match (Step::try_from(from), Step::try_from(target)) {
        (Ok(Step::Init), Ok(Step::InAgg)) => Ok(ProofKind::Compressed),
        (Ok(Step::Init), Ok(Step::InSnark)) => Ok(ProofKind::Groth16),
        (Ok(Step::InAgg), Ok(Step::InSnark)) => Ok(ProofKind::CompressToGroth16),
        _ => Err("only Init->InAgg (compressed), Init->InSnark (groth16) and InAgg->InSnark are supported"),
    }
}

impl StageApi {
    fn reject(&self, proof_id: &str, status: StageStatus, msg: &str) -> GenerateProofResponse {
        tracing::warn!(proof_id, "rejected: {msg}");
        GenerateProofResponse {
            proof_id: proof_id.to_string(),
            status: status as i32,
            error_message: msg.to_string(),
            ..Default::default()
        }
    }

    /// Records a rejected request as a failed job, because the SDK only looks at the
    /// proof id of `GenerateProof` and learns the outcome from `GetStatus`.
    async fn record_rejection(&self, proof_id: &str, client_id: &str, status: StageStatus, msg: &str, identity: (String, String)) {
        if !is_safe_id(proof_id) {
            return;
        }
        let token = self.fleet.token_for_signer(&identity.1);
        let tag = token.as_ref().and_then(|c| c.tag.clone());
        let namespace = token.and_then(|c| c.namespace);
        let job = Job {
            id: proof_id.to_string(),
            client_id: client_id.to_string(),
            namespace: namespace.unwrap_or_else(|| crate::store::SYSTEM_NAMESPACE.into()),
            tag: tag.unwrap_or_default(),
            tenant: identity.0,
            owner: identity.1,
            elf_id: String::new(),
            priority: 0,
            kind: ProofKind::Unspecified,
            state: JobState::Failed(status),
            error: msg.to_string(),
            node: None,
            attempts: 0,
            failed_on: vec![],
            cycles: 0,
            prove_ms: 0,
            created_ms: now_ms(),
            started_ms: None,
            finished_ms: Some(now_ms()),
        };
        let _ = self.fleet.submit(job).await;
    }

    /// Bearer tokens identify proof API callers. The proof signature independently binds
    /// each submitted proof to its owner.
    fn caller(&self, md: &tonic::metadata::MetadataMap) -> Result<Option<ProofCaller>, Status> {
        let bearer = md.get(crate::token::HEADER).and_then(|v| v.to_str().ok());
        match bearer {
            Some(value) => {
                let token = self.fleet.token_for_token(Some(value)).ok_or_else(|| {
                    Status::unauthenticated("missing, invalid or revoked user token")
                })?;
                Ok(Some(ProofCaller {
                    tenant: token.tenant.clone(),
                    address: token.address.clone(),
                    token: Some(token),
                }))
            }
            None => Ok(None),
        }
    }

    async fn generate(&self, req: GenerateProofRequest, caller: Option<ProofCaller>) -> GenerateProofResponse {
        let id = req.proof_id.clone();
        if !is_safe_id(&id) {
            return self.reject(
                &id,
                StageStatus::InvalidParameter,
                "proof_id must be 1-128 of [A-Za-z0-9_-]",
            );
        }
        // Serialize id reservation and blob writes: another token cannot race a
        // submission with the same id and replace its input or ownership.
        let _submission = self.fleet.stage_submissions.lock().await;
        let recovered = auth::recover_signer(&req.signature, &id, req.block_no, req.seg_size).ok();
        let owner = recovered.as_ref().map(|a| normalize_address(&a.to_string())).or_else(|| caller.as_ref().map(|c| c.address.clone()));
        if owner.is_none() {
            return self.reject(&id, StageStatus::InvalidParameter, "an authenticated job owner is required");
        }
        if caller.as_ref().is_some_and(|c| recovered.is_none() || owner.as_ref() != Some(&c.address)) {
            return self.reject(&id, StageStatus::InvalidParameter, "the proof signature must match the bearer token");
        }
        let token = caller.as_ref().and_then(|c| c.token.clone()).or_else(|| owner.as_ref().and_then(|a| self.fleet.token_for_signer(a)));
        if token.as_ref().is_some_and(|t| t.namespace.is_some()) && caller.is_none() {
            return self.reject(&id, StageStatus::InvalidParameter, "a bearer token is required for namespace proof access");
        }
        let scoped = token.as_ref().is_some_and(|c| c.apps.is_some());
        if scoped && (recovered.is_none() || token.is_none() || caller.as_ref().is_none_or(|c| owner.as_ref() != Some(&c.address))) {
            return self.reject(&id, StageStatus::InvalidParameter, "a matching user token and proof signature are required");
        }
        if !req.signature.is_empty() && owner.as_ref().is_none_or(|a| self.fleet.tenant_for_signer(a).is_none()) {
            return self.reject(&id, StageStatus::InvalidParameter, "permission denied");
        }
        let tag = token.as_ref().and_then(|c| c.tag.clone());
        let namespace = token.as_ref().and_then(|c| c.namespace.clone());
        let job_id = if let Some(n) = &namespace {
            match self.fleet.namespace_job(n, tag.as_deref(), &id).await {
                Ok(Some(j)) => j.id,
                Ok(None) => Fleet::internal_proof_id(n, tag.as_deref(), &id),
                Err(e) => return self.reject(&id, StageStatus::InternalError, &format!("{e:#}")),
            }
        } else { id.clone() };
        if let Ok(Some(job)) = self.fleet.job(&job_id).await {
            let signed_tenant = owner.as_ref().and_then(|a| self.fleet.tenant_for_signer(a));
            let allowed = if scoped {
                namespace.is_some() && namespace.as_deref() == Some(job.namespace.as_str()) && job.tag == tag.as_deref().unwrap_or("")
            } else {
                signed_tenant.as_ref().is_some_and(|t| t == &job.tenant)
                    && caller.as_ref().is_none_or(|c| c.tenant == job.tenant)
            };
            if !allowed && (self.fleet.cfg.require_signature || token.is_some() || caller.is_some()) {
                return self.reject(&id, StageStatus::InvalidParameter, "permission denied");
            }
            return GenerateProofResponse { proof_id: id, status: StageStatus::Computing as i32, ..Default::default() };
        }
        let fail = |status, msg: &str| (status, msg.to_string());
        let outcome: Result<Job, (StageStatus, String)> = async {
            let signer = if req.signature.is_empty() {
                None
            } else {
                Some(
                    auth::recover_signer(&req.signature, &id, req.block_no, req.seg_size).map_err(
                        |e| {
                            fail(
                                StageStatus::InvalidParameter,
                                &format!("invalid signature: {e}"),
                            )
                        },
                    )?,
                )
            };
            let tenant = match signer.and_then(|a| self.fleet.tenant_for_signer(&a.to_string())) {
                Some(t) => t,
                None if !self.fleet.cfg.require_signature => self
                    .fleet
                    .cfg
                    .default_tenant
                    .clone()
                    .ok_or_else(|| fail(StageStatus::InvalidParameter, "permission denied"))?,
                None => return Err(fail(StageStatus::InvalidParameter, "permission denied")),
            };
            if caller.as_ref().is_some_and(|c| c.tenant != tenant) {
                return Err(fail(
                    StageStatus::InvalidParameter,
                    "the proof caller and signature belong to different tenants",
                ));
            }
            let tcfg = self
                .fleet
                .cfg
                .tenant(&tenant)
                .cloned()
                .ok_or_else(|| fail(StageStatus::InternalError, "tenant vanished"))?;
            if self.fleet.queued_for(&tenant) >= tcfg.max_queued {
                return Err(fail(StageStatus::InternalError, "tenant queue is full"));
            }
            let kind = kind_for(req.from_step, req.target_step)
                .map_err(|m| fail(StageStatus::InvalidParameter, m))?;
            // a job runs only for a registered app: the program is checked before it is stored
            let elf_id = if !req.elf_data.is_empty() {
                blobs::elf_id_of(&req.elf_data)
            } else {
                req.elf_id.clone().unwrap_or_default().trim_start_matches("0x").to_ascii_lowercase()
            };
            if token.as_ref().is_some_and(|c| c.apps.as_ref().is_some_and(|g| !g.contains(&format!("{tenant}/{elf_id}")))) {
                return Err(fail(StageStatus::InvalidParameter, "app is not granted to this token"));
            }
            if !self.fleet.app_known(namespace.as_deref().unwrap_or(crate::store::SYSTEM_NAMESPACE), &tenant, &elf_id) {
                return Err(fail(
                    StageStatus::InvalidParameter,
                    &format!("program {elf_id} is not an app of tenant {tenant}: register it in the submitting namespace first (fleetctl --namespace NAME app add {tenant}/{elf_id})"),
                ));
            }
            let elf_id = if !req.elf_data.is_empty() {
                self.fleet
                    .blobs
                    .put_elf(&req.elf_data)
                    .map_err(|e| fail(StageStatus::InternalError, &format!("{e:#}")))?
            } else {
                let given = elf_id;
                if !self.fleet.blobs.has_elf(&given) {
                    return Err(fail(
                        StageStatus::InvalidParameter,
                        "elf_id not found: send the ELF itself once; later requests may carry only its elf_id",
                    ));
                }
                given
            };
            if self.fleet.app_disabled(namespace.as_deref().unwrap_or(crate::store::SYSTEM_NAMESPACE), &tenant, &elf_id) {
                return Err(fail(
                    StageStatus::InvalidParameter,
                    &format!("app {} is disabled: it takes no new jobs", crate::fleet::app_name(&tenant, &elf_id)),
                ));
            }
            let stdin: Vec<Vec<u8>> = if req.private_input_stream.is_empty() {
                Vec::new()
            } else {
                bincode::deserialize(&req.private_input_stream).map_err(|e| {
                    fail(
                        StageStatus::InvalidParameter,
                        &format!("private_input_stream: {e}"),
                    )
                })?
            };
            let receipts: Vec<Vec<u8>> = req.receipts.clone();
            let put = |name, bytes: Vec<u8>| self.fleet.blobs.put(&job_id, name, &bytes);
            put(blobs::STDIN, bincode::serialize(&stdin).unwrap())
                .and_then(|_| put(blobs::RECEIPTS, bincode::serialize(&receipts).unwrap()))
                .map_err(|e| fail(StageStatus::InternalError, &format!("{e:#}")))?;
            Ok(Job {
                id: job_id.clone(),
                namespace: namespace.clone().unwrap_or_else(|| crate::store::SYSTEM_NAMESPACE.into()),
                tag: tag.clone().unwrap_or_default(),
                client_id: id.clone(),
                tenant: tenant.clone(),
                owner: owner.clone().expect("authenticated owner checked above"),
                elf_id: elf_id.clone(),
                priority: self.fleet.app_priority(namespace.as_deref().unwrap_or(crate::store::SYSTEM_NAMESPACE), &tenant, &elf_id),
                kind,
                state: JobState::Queued,
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
        }
        .await;
        match outcome {
            Ok(job) => {
                tracing::info!(proof_id = %id, tenant = %job.tenant, kind = ?job.kind, "accepted");
                if let Err(e) = self.fleet.submit(job).await {
                    return self.reject(&id, StageStatus::InternalError, &format!("{e:#}"));
                }
                GenerateProofResponse {
                    proof_id: id,
                    status: StageStatus::Computing as i32,
                    ..Default::default()
                }
            }
            Err((status, msg)) => {
                if let Some(identity) = owner.as_ref().and_then(|a| self.fleet.tenant_for_signer(a).map(|t| (t, a.clone()))) {
                    self.record_rejection(&job_id, &id, status, &msg, identity).await;
                }
                self.reject(&id, status, &msg)
            }
        }
    }

    async fn status(&self, id: &str, caller: Option<ProofCaller>) -> GetStatusResponse {
        let mut resp = GetStatusResponse {
            proof_id: id.to_string(),
            ..Default::default()
        };
        let caller_token = caller.as_ref().and_then(|c| c.token.clone());
        let internal = if let Some(c) = caller_token.as_ref().filter(|c| c.namespace.is_some()) {
            match self.fleet.namespace_job(c.namespace.as_deref().unwrap(), c.tag.as_deref(), id).await {
                Ok(Some(j)) => j.id,
                _ => { resp.status = StageStatus::Unspecified as i32; return resp; }
            }
        } else { id.to_string() };
        let id = internal.as_str();
        let allowed = match self.fleet.job(id).await {
            Ok(Some(j)) => match j.namespace.as_str() {
                n if n != crate::store::SYSTEM_NAMESPACE => caller_token.as_ref()
                    .is_some_and(|c| c.namespace.as_deref() == Some(n) && c.tag.as_deref().unwrap_or("") == j.tag),
                _ => caller.as_ref().is_none_or(|c| j.tenant == c.tenant && c.token.as_ref().is_none_or(|t| t.apps.is_none())),
            },
            Ok(None) => false,
            Err(e) => {
                tracing::error!(proof_id = id, "status lookup: {e:#}");
                resp.status = StageStatus::InternalError as i32;
                return resp;
            }
        };
        if !allowed { resp.status = StageStatus::Unspecified as i32; return resp; }
        self.fleet
            .wait_terminal(id, Duration::from_millis(self.fleet.cfg.long_poll_ms))
            .await;
        if caller.as_ref().is_some_and(|c| self.fleet.tenant_for_signer(&c.address).is_none()) {
            resp.status = StageStatus::Unspecified as i32;
            return resp;
        }
        let job = match self.fleet.job(id).await {
            Ok(Some(j)) => j,
            Ok(None) => {
                resp.status = StageStatus::Unspecified as i32;
                return resp;
            }
            Err(e) => {
                tracing::error!(proof_id = id, "status lookup: {e:#}");
                resp.status = StageStatus::InternalError as i32;
                return resp;
            }
        };
        match job.state {
            JobState::Queued => {
                resp.status = StageStatus::Computing as i32;
                resp.step = Step::Init as i32;
            }
            JobState::Running => {
                resp.status = StageStatus::Computing as i32;
                resp.step = Step::InProve as i32;
            }
            JobState::Failed(s) => {
                resp.status = s as i32;
                resp.step = Step::End as i32;
            }
            JobState::Succeeded => match self.fleet.blobs.sdk_output(&job, blobs::PROOF).await {
                Ok(proof) => {
                    resp.status = StageStatus::Success as i32;
                    resp.step = Step::End as i32;
                    resp.proof_with_public_inputs = proof;
                    resp.public_values_url = if job.namespace != crate::store::SYSTEM_NAMESPACE {
                        let token = caller_token.as_ref().expect("namespaced status requires user token");
                        format!("{}/{}/wrap/public_values.bin?token_id={}&key={}",
                            self.fleet.cfg.public_url.trim_end_matches('/'), id, token.id,
                            self.fleet.public_values_key_for(id, &token.id))
                    } else {
                        format!("{}/{}/wrap/public_values.bin?key={}",
                            self.fleet.cfg.public_url.trim_end_matches('/'), id, self.fleet.public_values_key(id))
                    };
                    resp.total_steps = job.cycles;
                    resp.proving_time = job.prove_ms;
                    resp.elf_id = job.elf_id.into_bytes();
                }
                Err(e) => {
                    tracing::error!(proof_id = id, "reading proof: {e:#}");
                    resp.status = StageStatus::InternalError as i32;
                }
            },
        }
        resp
    }
}

#[tonic::async_trait]
impl StageService for StageApi {
    async fn generate_proof(
        &self,
        request: Request<GenerateProofRequest>,
    ) -> Result<Response<GenerateProofResponse>, Status> {
        let caller = self.caller(request.metadata())?;
        Ok(Response::new(self.generate(request.into_inner(), caller).await))
    }

    async fn get_status(
        &self,
        request: Request<GetStatusRequest>,
    ) -> Result<Response<GetStatusResponse>, Status> {
        let caller = self.caller(request.metadata())?;
        Ok(Response::new(
            self.status(&request.into_inner().proof_id, caller).await,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn steps_map_to_kinds_like_the_sdk_sets_them() {
        let s = |x: Step| Some(x as i32);
        assert_eq!(kind_for(None, s(Step::InAgg)), Ok(ProofKind::Compressed));
        assert_eq!(kind_for(None, s(Step::InSnark)), Ok(ProofKind::Groth16));
        assert_eq!(kind_for(None, None), Ok(ProofKind::Groth16));
        assert_eq!(
            kind_for(s(Step::InAgg), s(Step::InSnark)),
            Ok(ProofKind::CompressToGroth16)
        );
        assert!(kind_for(None, s(Step::InSplit)).is_err());
    }
}
