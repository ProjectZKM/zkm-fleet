//! The SDK-compatible stage API: `ZKM_PROVER=network` clients talk to this unchanged.

use std::sync::Arc;
use std::time::Duration;

use tonic::{Request, Response, Status};

use fleet_proto::fleet::v1::ProofKind;
use fleet_proto::stage::v1::{
    stage_service_server::StageService, GenerateProofRequest, GenerateProofResponse,
    GetStatusRequest, GetStatusResponse, Status as StageStatus, Step,
};

use crate::blobs::{self, is_safe_id};
use crate::fleet::{now_ms, Scheduler};
use crate::store::{normalize_address, ProofJob, ProofJobState, Token};

use crate::token;

pub struct ProofStageService {
    pub fleet: Arc<Scheduler>,
}

/// Maps the request's steps to a proof kind, as the SDK's network prover sets them.
pub use fleet_proto::sdk::kind_for;

impl ProofStageService {
    fn may_read_job(caller: &Token, job: &ProofJob) -> bool {
        caller.apps.as_ref().is_none_or(|apps| apps.contains(&job.app))
            // The SDK polls its own rejected submissions, including attempts for an
            // ungranted or unknown app. These records contain no accepted proof inputs.
            || (job.state() == ProofJobState::Failed && job.owner == caller.address
                && job.elf_id.is_empty() && job.inputs.is_none())
    }

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
    async fn record_rejection(&self, job_id: &str, client_id: &str, status: StageStatus, msg: &str, caller: &Token, app: &str) {
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
            fail_status: Some(status as i32),
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

    /// The namespace token a proof API call carries (`Authorization: Bearer`). The proof
    /// signature independently binds each submitted proof to that token's signer.
    pub(crate) fn caller(&self, md: &tonic::metadata::MetadataMap) -> Result<Option<Token>, Status> {
        match md.get(crate::token::HEADER).and_then(|v| v.to_str().ok()) {
            Some(value) => self.fleet.token_for_token(Some(value))
                .map(Some)
                .ok_or_else(|| Status::unauthenticated("missing, invalid or revoked user token")),
            None => Ok(None),
        }
    }

    /// Submits a proof job of `app` (the one its program is, when not given) for `caller`.
    /// Inputs are app-owned storage references; without them the request carries the payload.
    pub(crate) async fn generate(&self, req: GenerateProofRequest, caller: Option<Token>,
        inputs: Option<fleet_proto::fleet::v1::ProofInputs>, app: Option<&str>,
    ) -> GenerateProofResponse {
        let id = req.proof_id.clone();
        if !is_safe_id(&id) {
            return self.reject(&id, StageStatus::InvalidParameter, "proof_id must be 1-128 of [A-Za-z0-9_-]");
        }
        if let Some(inputs) = &inputs {
            if let Err(error) = fleet_proto::artifacts::validate_inputs(inputs, req.elf_id.as_deref().unwrap_or("")) {
                return self.reject(&id, StageStatus::InvalidParameter, &error.to_string());
            }
        }
        let Some(caller) = caller else {
            return self.reject(&id, StageStatus::InvalidParameter, "a namespace token is required (Authorization: Bearer)");
        };
        let Some(namespace) = caller.namespace.clone() else {
            return self.reject(&id, StageStatus::InvalidParameter, "an administrator's token submits no proofs: use a namespace token");
        };
        if !token::allowed(Some(&caller), token::Permission::Submit) {
            return self.reject(&id, StageStatus::InvalidParameter, "permission denied: the token may not submit jobs");
        }
        // Serialize id reservation and blob writes: another token cannot race a
        // submission with the same id and replace its input or ownership.
        let _submission = self.fleet.stage_submissions.lock().await;
        let signed = token::recover_signer(&req.signature, &id, req.block_no, req.seg_size).ok()
            .is_some_and(|a| normalize_address(&a.to_string()) == caller.address);
        if !signed {
            return self.reject(&id, StageStatus::InvalidParameter, "the proof signature must match the bearer token");
        }
        let tag = caller.tag.clone();
        let job_id = match self.fleet.namespace_job(&namespace, tag.as_deref(), &id).await {
            Ok(Some(j)) => j.id,
            Ok(None) => Scheduler::internal_proof_id(&namespace, tag.as_deref(), &id),
            Err(e) => return self.reject(&id, StageStatus::InternalError, &format!("{e:#}")),
        };
        if let Ok(Some(job)) = self.fleet.job(&job_id).await {
            if !Self::may_read_job(&caller, &job) {
                return self.reject(&id, StageStatus::InvalidParameter, "app is not granted to this token");
            }
            if inputs.is_some() && (job.inputs != inputs || job.elf_id != req.elf_id.clone().unwrap_or_default()
                || kind_for(req.from_step, req.target_step).ok() != Some(job.kind())) {
                return self.reject(&id, StageStatus::InvalidParameter, "proof_id already exists with different inputs or parameters");
            }
            if job.namespace != namespace || job.tag != tag.as_deref().unwrap_or("") {
                return self.reject(&id, StageStatus::InvalidParameter, "permission denied");
            }
            return GenerateProofResponse { proof_id: id, status: StageStatus::Computing as i32, ..Default::default() };
        }
        // The app the job is for, once known (or the registered one it was submitted to): a
        // rejection is recorded under it, where its client polls.
        let mut named = app.filter(|a| self.fleet.app_known(&namespace, a)).unwrap_or_default().to_string();
        let fail = |status, msg: &str| (status, msg.to_string());
        let outcome: Result<ProofJob, (StageStatus, String)> = async {
            let kind = kind_for(req.from_step, req.target_step)
                .map_err(|m| fail(StageStatus::InvalidParameter, m))?;
            // a job runs only for a registered app: the program is checked before it is stored
            let elf_id = if !req.elf_data.is_empty() {
                blobs::elf_id_of(&req.elf_data)
            } else {
                req.elf_id.clone().unwrap_or_default().trim_start_matches("0x").to_ascii_lowercase()
            };
            let registered = self.fleet.app_for_elf(&namespace, &elf_id).ok_or_else(|| fail(
                StageStatus::InvalidParameter,
                &format!("program {elf_id} is not an app of this namespace: register it first (fleetctl app add NAME --elf-id {elf_id})"),
            ))?;
            if app.is_some_and(|a| a != registered.name) {
                return Err(fail(StageStatus::InvalidParameter, &format!("program {elf_id} is app {}, not {}", registered.name, app.unwrap_or_default())));
            }
            named = registered.name.clone();
            if caller.apps.as_ref().is_some_and(|g| !g.contains(&registered.name)) {
                return Err(fail(StageStatus::InvalidParameter, "app is not granted to this token"));
            }
            if self.fleet.queued_for(&namespace, &registered.name) >= registered.max_queued as usize {
                return Err(fail(StageStatus::InternalError, &format!("app {} has {} jobs queued or running, its most", registered.name, registered.max_queued)));
            }
            if self.fleet.app_worker_owned(&namespace, &registered.name) {
                return Err(fail(StageStatus::InvalidParameter,
                    "this app owns its input stream; SDK proof submission is unavailable"));
            }
            if registered.disabled {
                return Err(fail(StageStatus::InvalidParameter, &format!("app {} is disabled: it takes no new jobs", registered.name)));
            }
            if !req.elf_data.is_empty() {
                self.fleet.blobs.put_elf(&req.elf_data).map_err(|e| fail(StageStatus::InternalError, &format!("{e:#}")))?;
            } else if inputs.is_none() && !self.fleet.blobs.has_elf(&elf_id) {
                return Err(fail(
                    StageStatus::InvalidParameter,
                    "elf_id not found: send the ELF itself once; later requests may carry only its elf_id",
                ));
            }
            let stdin: Vec<Vec<u8>> = if req.private_input_stream.is_empty() {
                Vec::new()
            } else {
                bincode::deserialize(&req.private_input_stream).map_err(|e| {
                    fail(StageStatus::InvalidParameter, &format!("private_input_stream: {e}"))
                })?
            };
            let receipts: Vec<Vec<u8>> = req.receipts.clone();
            let put = |name, bytes: Vec<u8>| self.fleet.blobs.put(&job_id, name, &bytes);
            if inputs.is_none() { put(blobs::STDIN, bincode::serialize(&stdin).unwrap())
                .and_then(|_| put(blobs::RECEIPTS, bincode::serialize(&receipts).unwrap()))
                .map_err(|e| fail(StageStatus::InternalError, &format!("{e:#}")))?; }
            Ok(ProofJob {
                inputs: inputs.clone(), outputs: None,
                id: job_id.clone(),
                namespace: namespace.clone(),
                tag: tag.clone().unwrap_or_default(),
                client_id: id.clone(),
                app: registered.name.clone(),
                owner: caller.address.clone(),
                elf_id: elf_id.clone(),
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
        }
        .await;
        match outcome {
            Ok(job) => {
                tracing::info!(proof_id = %id, app = %job.app, kind = ?job.kind, "accepted");
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
                self.record_rejection(&job_id, &id, status, &msg, &caller, &named).await;
                self.reject(&id, status, &msg)
            }
        }
    }

    async fn status(&self, id: &str, caller: Option<Token>) -> GetStatusResponse {
        let mut resp = GetStatusResponse {
            proof_id: id.to_string(),
            status: StageStatus::Unspecified as i32,
            ..Default::default()
        };
        // A token that submits follows its proofs, as one that reads does.
        let Some(c) = caller.filter(|c| c.namespace.is_some()
            && (token::allowed(Some(c), token::Permission::Read) || token::allowed(Some(c), token::Permission::Submit))) else {
            return resp;
        };
        let namespace = c.namespace.clone().unwrap_or_default();
        let internal = match self.fleet.namespace_job(&namespace, c.tag.as_deref(), id).await {
            Ok(Some(j)) if Self::may_read_job(&c, &j) => j.id,
            Ok(Some(_)) => return resp,
            Ok(None) => return resp,
            Err(e) => {
                tracing::error!(proof_id = id, "status lookup: {e:#}");
                resp.status = StageStatus::InternalError as i32;
                return resp;
            }
        };
        let id = internal.as_str();
        self.fleet
            .wait_terminal(id, Duration::from_millis(self.fleet.cfg.long_poll_ms))
            .await;
        // Revoked or suspended while it waited.
        if self.fleet.token_for_id(&c.id).is_none() {
            return resp;
        }
        let job = match self.fleet.job(id).await {
            Ok(Some(j)) if j.namespace == namespace && j.tag == c.tag.as_deref().unwrap_or("")
                && Self::may_read_job(&c, &j) => j,
            Ok(_) => return resp,
            Err(e) => {
                tracing::error!(proof_id = id, "status lookup: {e:#}");
                resp.status = StageStatus::InternalError as i32;
                return resp;
            }
        };
        match job.state() {
            ProofJobState::Queued => {
                resp.status = StageStatus::Computing as i32;
                resp.step = Step::Init as i32;
            }
            ProofJobState::Running => {
                resp.status = StageStatus::Computing as i32;
                resp.step = Step::InProve as i32;
            }
            ProofJobState::Failed => {
                resp.status = job.fail_status.unwrap_or(StageStatus::InternalError as i32);
                resp.step = Step::End as i32;
            }
            ProofJobState::Succeeded => match self.fleet.proof_output(&job, blobs::PROOF).await {
                Ok(proof) => {
                    resp.status = StageStatus::Success as i32;
                    resp.step = Step::End as i32;
                    resp.proof_with_public_inputs = proof;
                    resp.public_values_url = format!("{}/{}/wrap/public_values.bin?token_id={}&key={}",
                        self.fleet.cfg.public_url.trim_end_matches('/'), id, c.id,
                        self.fleet.public_values_key_for(id, &c.id));
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
impl StageService for ProofStageService {
    async fn generate_proof(
        &self,
        request: Request<GenerateProofRequest>,
    ) -> Result<Response<GenerateProofResponse>, Status> {
        let caller = self.caller(request.metadata())?;
        Ok(Response::new(self.generate(request.into_inner(), caller, None, None).await))
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

    #[tokio::test]
    async fn status_checks_app_grants_before_waiting_or_reporting_any_state() {
        use crate::{blobs::BlobStore, config::SchedulerConfig, quota::Quota, store::MemoryStore};
        use sha2::Digest;
        let dir = tempfile::tempdir().unwrap();
        let cfg: SchedulerConfig = serde_yaml::from_str(&format!(
            "public_url: http://localhost\ndata_dir: {}\ncluster_token_file: unused\nlong_poll_ms: 2000\n", dir.path().display()
        )).unwrap();
        let fleet = Scheduler::new(cfg, Arc::new(MemoryStore::default()), BlobStore::new(dir.path()).unwrap(), "cluster".into());
        fleet.load_tokens().await.unwrap();
        let namespace = fleet.create_namespace("test", Quota::default()).await.unwrap().id;
        let elf_id = fleet.blobs.put_elf(b"test program").unwrap();
        let api = ProofStageService { fleet: fleet.clone() };
        for (seed, permission) in [(1, "read"), (2, "submit")] {
            let secret = format!("{seed:02x}").repeat(48);
            let caller = fleet.add_user_token(&format!("{seed:02x}").repeat(16), &format!("{seed:02x}").repeat(20), "test",
                Some(vec!["allowed".into()]), Some(hex::encode(sha2::Sha256::digest(secret.as_bytes()))),
                Some(namespace.clone()), Some("scope=default".into()), vec![permission.into()]).await.unwrap();
            for (index, state) in [ProofJobState::Queued, ProofJobState::Running, ProofJobState::Failed, ProofJobState::Succeeded].into_iter().enumerate() {
                // Even an accepted job submitted by this signer still needs an app grant.
                let client_id = format!("hidden-{seed}-{index}");
                fleet.submit(ProofJob {
                    id: Scheduler::internal_proof_id(&namespace, caller.tag.as_deref(), &client_id), client_id: client_id.clone(),
                    namespace: namespace.clone(), tag: "scope=default".into(), app: "denied".into(), owner: caller.address.clone(),
                    elf_id: elf_id.clone(), state: state as i32, fail_status: (state == ProofJobState::Failed).then_some(StageStatus::InvalidParameter as i32),
                    finished_ms: state.is_terminal().then(now_ms), ..Default::default()
                }).await.unwrap();
                let mut request = Request::new(GetStatusRequest { proof_id: client_id.clone() });
                request.metadata_mut().insert("authorization", format!("Bearer {secret}").parse().unwrap());
                let response = tokio::time::timeout(Duration::from_millis(200), api.get_status(request)).await
                    .expect("ungranted jobs must not enter the long poll").unwrap().into_inner();
                assert_eq!(response.status, StageStatus::Unspecified as i32, "{permission}: {state:?}");
                assert!(response.proof_with_public_inputs.is_empty() && response.public_values_url.is_empty());
            }
            for app in ["denied", ""] {
                let client_id = format!("rejected-{seed}-{app}");
                let id = Scheduler::internal_proof_id(&namespace, caller.tag.as_deref(), &client_id);
                api.record_rejection(&id, &client_id, StageStatus::InvalidParameter, "rejected", &caller, app).await;
                assert_eq!(api.status(&client_id, Some(caller.clone())).await.status, StageStatus::InvalidParameter as i32);
                let mut other = caller.clone();
                other.address = "ff".repeat(20);
                assert_eq!(api.status(&client_id, Some(other)).await.status, StageStatus::Unspecified as i32);
            }
        }
    }

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
