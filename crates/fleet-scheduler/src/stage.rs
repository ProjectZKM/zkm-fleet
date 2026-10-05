//! The SDK-compatible stage API: `ZKM_PROVER=network` clients talk to this unchanged.

use std::sync::Arc;
use std::time::Duration;

use tonic::{Request, Response, Status};

use fleet_proto::fleet::v1::ProofKind;
use fleet_proto::stage::v1::{
    stage_service_server::StageService, GenerateProofRequest, GenerateProofResponse, GetStatusRequest,
    GetStatusResponse, Status as StageStatus, Step,
};

use crate::auth;
use crate::blobs::{self, is_safe_id};
use crate::fleet::{now_ms, Fleet};
use crate::store::{Job, JobState};

pub struct StageApi {
    pub fleet: Arc<Fleet>,
}

/// Maps the request's steps to a proof kind, as the SDK's network prover sets them.
pub fn kind_for(from_step: Option<i32>, target_step: Option<i32>) -> Result<ProofKind, &'static str> {
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
    async fn record_rejection(&self, proof_id: &str, status: StageStatus, msg: &str) {
        if !is_safe_id(proof_id) {
            return;
        }
        let job = Job {
            id: proof_id.to_string(),
            tenant: String::new(),
            elf_id: String::new(),
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

    async fn generate(&self, req: GenerateProofRequest) -> GenerateProofResponse {
        let id = req.proof_id.clone();
        if !is_safe_id(&id) {
            return self.reject(&id, StageStatus::InvalidParameter, "proof_id must be 1-128 of [A-Za-z0-9_-]");
        }
        if let Ok(Some(_)) = self.fleet.job(&id).await {
            return GenerateProofResponse { proof_id: id, status: StageStatus::Computing as i32, ..Default::default() };
        }
        let fail = |status, msg: &str| (status, msg.to_string());
        let outcome: Result<Job, (StageStatus, String)> = async {
            let signer = if req.signature.is_empty() {
                None
            } else {
                Some(
                    auth::recover_signer(&req.signature, &id, req.block_no, req.seg_size)
                        .map_err(|e| fail(StageStatus::InvalidParameter, &format!("invalid signature: {e}")))?,
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
            let tcfg = self.fleet.cfg.tenant(&tenant).cloned().ok_or_else(|| fail(StageStatus::InternalError, "tenant vanished"))?;
            if self.fleet.queued_for(&tenant) >= tcfg.max_queued {
                return Err(fail(StageStatus::InternalError, "tenant queue is full"));
            }
            let kind = kind_for(req.from_step, req.target_step).map_err(|m| fail(StageStatus::InvalidParameter, m))?;
            let elf_id = if !req.elf_data.is_empty() {
                self.fleet.blobs.put_elf(&req.elf_data).map_err(|e| fail(StageStatus::InternalError, &format!("{e:#}")))?
            } else {
                let given = req.elf_id.clone().unwrap_or_default();
                let given = given.trim_start_matches("0x").to_ascii_lowercase();
                if !self.fleet.blobs.has_elf(&given) {
                    return Err(fail(StageStatus::InvalidParameter, "elf_id not found: upload the ELF once (fleetctl elf put)"));
                }
                given
            };
            let stdin: Vec<Vec<u8>> = if req.private_input_stream.is_empty() {
                Vec::new()
            } else {
                bincode::deserialize(&req.private_input_stream)
                    .map_err(|e| fail(StageStatus::InvalidParameter, &format!("private_input_stream: {e}")))?
            };
            let receipts: Vec<Vec<u8>> = req.receipts.clone();
            let put = |name, bytes: Vec<u8>| self.fleet.blobs.put(&id, name, &bytes);
            put(blobs::STDIN, bincode::serialize(&stdin).unwrap())
                .and_then(|_| put(blobs::RECEIPTS, bincode::serialize(&receipts).unwrap()))
                .map_err(|e| fail(StageStatus::InternalError, &format!("{e:#}")))?;
            Ok(Job {
                id: id.clone(),
                tenant,
                elf_id,
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
                GenerateProofResponse { proof_id: id, status: StageStatus::Computing as i32, ..Default::default() }
            }
            Err((status, msg)) => {
                self.record_rejection(&id, status, &msg).await;
                self.reject(&id, status, &msg)
            }
        }
    }

    async fn status(&self, id: &str) -> GetStatusResponse {
        self.fleet.wait_terminal(id, Duration::from_millis(self.fleet.cfg.long_poll_ms)).await;
        let mut resp = GetStatusResponse { proof_id: id.to_string(), ..Default::default() };
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
            JobState::Succeeded => match self.fleet.blobs.get(id, blobs::PROOF) {
                Ok(proof) => {
                    resp.status = StageStatus::Success as i32;
                    resp.step = Step::End as i32;
                    resp.proof_with_public_inputs = proof;
                    resp.public_values_url = format!(
                        "{}/{}/wrap/public_values.bin",
                        self.fleet.cfg.public_url.trim_end_matches('/'),
                        id
                    );
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
        Ok(Response::new(self.generate(request.into_inner()).await))
    }

    async fn get_status(&self, request: Request<GetStatusRequest>) -> Result<Response<GetStatusResponse>, Status> {
        Ok(Response::new(self.status(&request.into_inner().proof_id).await))
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
        assert_eq!(kind_for(s(Step::InAgg), s(Step::InSnark)), Ok(ProofKind::CompressToGroth16));
        assert!(kind_for(None, s(Step::InSplit)).is_err());
    }
}
