//! The Ziren SDK, used as it is: one `ProverClient` per node.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use lru::LruCache;
use parking_lot::Mutex;
use tokio::sync::Semaphore;
use tonic::{metadata::MetadataValue, Request, Response, Status};

use fleet_proto::fleet::v1::{
    node_service_server::NodeService, HealthRequest, HealthResponse, ProofKind, ProveRequest, ProveResponse,
};
use zkm_cuda::ZKMGpuServer;
use zkm_prover::ZKMProver;
use zkm_sdk::provers::CudaProver;
use zkm_sdk::{ProverClient, ZKMProofKind, ZKMProvingKey, ZKMStdin, ZKMVerifyingKey};

use crate::Args;

type Keys = Arc<(ZKMProvingKey, ZKMVerifyingKey)>;

pub struct Node {
    client: Arc<ProverClient>,
    backend: String,
    gpu_endpoint: String,
    keys: Mutex<LruCache<String, Keys>>,
    /// One proof at a time: the GPU server serialises anyway, and the CPU prover takes every core.
    slot: Arc<Semaphore>,
    http: reqwest::Client,
}

impl Node {
    pub async fn start(args: &Args) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder().timeout(Duration::from_secs(5)).build()?;
        let client = match args.backend.as_str() {
            "cpu" => tokio::task::spawn_blocking(ProverClient::cpu).await?,
            "mock" => ProverClient::mock(),
            "cuda" => {
                let deadline = Instant::now() + Duration::from_secs(args.ready_timeout);
                while !gpu_ready(&http, &args.gpu_endpoint).await {
                    anyhow::ensure!(Instant::now() < deadline, "GPU server at {} never became ready", args.gpu_endpoint);
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
                let endpoint = args.gpu_endpoint.clone();
                tokio::task::spawn_blocking(move || ProverClient {
                    prover: Box::new(CudaProver::new(ZKMProver::new(), ZKMGpuServer::External { endpoint })),
                })
                .await?
            }
            other => anyhow::bail!("unknown backend {other}"),
        };
        Ok(Self {
            client: Arc::new(client),
            backend: args.backend.clone(),
            gpu_endpoint: args.gpu_endpoint.clone(),
            keys: Mutex::new(LruCache::new(NonZeroUsize::new(args.pk_cache.max(1)).unwrap())),
            slot: Arc::new(Semaphore::new(1)),
            http,
        })
    }

    pub fn zkm_version(&self) -> &str {
        zkm_sdk::ZKM_CIRCUIT_VERSION
    }

    pub async fn ready(&self) -> bool {
        self.backend != "cuda" || gpu_ready(&self.http, &self.gpu_endpoint).await
    }

    pub fn cached(&self) -> Vec<String> {
        self.keys.lock().iter().map(|(k, _)| k.clone()).collect()
    }

    async fn keys_for(&self, elf_id: &str, elf: Vec<u8>) -> Result<Keys, Status> {
        if let Some(k) = self.keys.lock().get(elf_id) {
            return Ok(k.clone());
        }
        if elf.is_empty() {
            return Err(Status::failed_precondition("elf_missing"));
        }
        let got = zkm_elf_id(&elf);
        if got != elf_id {
            return Err(Status::invalid_argument(format!("elf does not hash to {elf_id} (got {got})")));
        }
        let client = self.client.clone();
        let t = Instant::now();
        let keys = tokio::task::spawn_blocking(move || client.setup(&elf))
            .await
            .map_err(|e| Status::internal(format!("setup panicked: {e}")))?;
        tracing::info!(elf_id, setup_ms = t.elapsed().as_millis() as u64, "program set up");
        let keys = Arc::new(keys);
        self.keys.lock().put(elf_id.to_string(), keys.clone());
        Ok(keys)
    }

    pub async fn prove(&self, req: ProveRequest) -> Result<ProveResponse, Status> {
        let _permit = self.slot.clone().try_acquire_owned().map_err(|_| Status::resource_exhausted("node busy"))?;
        let t0 = Instant::now();
        let keys = self.keys_for(&req.elf_id, req.elf).await?;
        let kind = match ProofKind::try_from(req.kind).unwrap_or(ProofKind::Unspecified) {
            ProofKind::Core => ZKMProofKind::Core,
            ProofKind::Compressed => ZKMProofKind::Compressed,
            ProofKind::Groth16 => ZKMProofKind::Groth16,
            ProofKind::Plonk => ZKMProofKind::Plonk,
            ProofKind::CompressToGroth16 => ZKMProofKind::CompressToGroth16,
            ProofKind::Unspecified => return Err(Status::invalid_argument("proof kind unspecified")),
        };
        let proofs = req
            .receipts
            .iter()
            .map(|r| bincode::deserialize(r))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| Status::invalid_argument(format!("receipt: {e}")))?;
        let stdin = ZKMStdin { buffer: req.stdin_buffer, ptr: 0, proofs };
        let client = self.client.clone();
        let elf_id = req.elf_id.clone();
        let job_id = req.job_id.clone();
        let t1 = Instant::now();
        let (proof, cycles, server_ms) = tokio::task::spawn_blocking(move || {
            let r = client.prover.prove_with_cycles(&keys.0, &stdin, kind, Some(elf_id));
            let server_ms = client.prover.take_prove_ms();
            r.map(|(p, c)| (p, c, server_ms))
        })
        .await
        .map_err(|e| Status::internal(format!("prover panicked: {e}")))?
        .map_err(|e| Status::internal(format!("{e:#}")))?;
        let prove_ms = server_ms.unwrap_or(t1.elapsed().as_millis() as u64);
        let proof_json = serde_json::to_vec(&proof.proof).map_err(|e| Status::internal(format!("encoding proof: {e}")))?;
        let wall_ms = t0.elapsed().as_millis() as u64;
        tracing::info!(job = %job_id, cycles, prove_ms, wall_ms, proof_bytes = proof_json.len(), "proved");
        Ok(ProveResponse {
            proof_json,
            public_values: proof.public_values.to_vec(),
            cycles,
            prove_ms,
            wall_ms,
            zkm_version: proof.zkm_version,
        })
    }
}

fn zkm_elf_id(elf: &[u8]) -> String {
    use sha2_digest::digest;
    digest(elf)
}

mod sha2_digest {
    use std::fmt::Write;

    pub fn digest(bytes: &[u8]) -> String {
        let h = <sha2::Sha256 as sha2::Digest>::digest(bytes);
        h.iter().fold(String::with_capacity(64), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
    }
}

/// `POST …/twirp/api.ProverService/Ready` in protobuf: an empty request, and
/// `{ready: true}` answers as the two bytes `08 01`.
async fn gpu_ready(http: &reqwest::Client, endpoint: &str) -> bool {
    let url = format!("{}/api.ProverService/Ready", endpoint.trim_end_matches('/'));
    match http.post(&url).header("content-type", "application/protobuf").body(Vec::<u8>::new()).send().await {
        Ok(r) if r.status().is_success() => r.bytes().await.map(|b| b.as_ref() == [0x08, 0x01]).unwrap_or(false),
        _ => false,
    }
}

pub struct NodeApi {
    pub node: Arc<Node>,
}

#[tonic::async_trait]
impl NodeService for NodeApi {
    async fn prove(&self, request: Request<ProveRequest>) -> Result<Response<ProveResponse>, Status> {
        self.node.prove(request.into_inner()).await.map(Response::new)
    }

    async fn health(&self, _: Request<HealthRequest>) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            ready: self.node.ready().await,
            cached_elf_ids: self.node.cached(),
            busy: self.node.slot.available_permits() == 0,
        }))
    }
}

fn bearer_ok(req: &Request<()>, token: &str) -> bool {
    let Some(got) = req.metadata().get("authorization").and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer "))
    else {
        return false;
    };
    got.len() == token.len() && got.bytes().zip(token.bytes()).fold(0u8, |a, (x, y)| a | (x ^ y)) == 0
}

/// Only the scheduler (holding the cluster token) may send work: the node receives witnesses.
#[derive(Clone)]
pub struct RequireToken(pub Arc<String>);

impl tonic::service::Interceptor for RequireToken {
    fn call(&mut self, req: Request<()>) -> Result<Request<()>, Status> {
        if bearer_ok(&req, &self.0) {
            Ok(req)
        } else {
            Err(Status::unauthenticated("missing or wrong cluster token"))
        }
    }
}

#[derive(Clone)]
pub struct AttachToken(MetadataValue<tonic::metadata::Ascii>);

impl AttachToken {
    pub fn new(token: &str) -> anyhow::Result<Self> {
        Ok(Self(format!("Bearer {token}").parse()?))
    }
}

impl tonic::service::Interceptor for AttachToken {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        req.metadata_mut().insert("authorization", self.0.clone());
        Ok(req)
    }
}
