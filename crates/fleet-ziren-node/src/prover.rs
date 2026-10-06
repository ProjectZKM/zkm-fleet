//! The Ziren SDK, used as it is: one `ProverClient` per node.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use lru::LruCache;
use parking_lot::Mutex;
use tokio::sync::watch;
use tonic::{metadata::MetadataValue, Request, Response, Status};

use fleet_proto::fleet::v1::{
    node_service_server::NodeService, HealthRequest, HealthResponse, ProofKind, ProveRequest,
    ProveResponse,
};
use zkm_cuda::ZKMGpuServer;
use zkm_prover::ZKMProver;
use zkm_sdk::provers::CudaProver;
use zkm_sdk::{ProverClient, ZKMProofKind, ZKMProvingKey, ZKMStdin, ZKMVerifyingKey};

use crate::Args;

type Keys = Arc<(ZKMProvingKey, ZKMVerifyingKey)>;
type Outcome = Result<ProveResponse, (tonic::Code, String)>;

/// How long a node stays ready after its GPU server last answered Ready.
const READY_GRACE: Duration = Duration::from_secs(15);

/// Results kept for jobs whose caller went away (a scheduler restart): it asks again.
const FINISHED_KEPT: usize = 4;

struct Running {
    job_id: String,
    done: watch::Receiver<Option<Outcome>>,
}

pub struct Node {
    client: Arc<ProverClient>,
    backend: String,
    gpu_endpoint: String,
    keys: Mutex<LruCache<String, Keys>>,
    /// One proof at a time: the GPU server serialises anyway, and the CPU prover takes every core.
    /// Lock order: `running` before `finished`.
    running: Mutex<Option<Running>>,
    finished: Mutex<LruCache<String, Outcome>>,
    http: reqwest::Client,
    /// Last time the GPU server answered Ready.
    last_ready: Mutex<Option<Instant>>,
}

impl Node {
    pub async fn start(args: &Args) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()?;
        let client = match args.backend.as_str() {
            "cpu" => tokio::task::spawn_blocking(ProverClient::cpu).await?,
            "mock" => ProverClient::mock(),
            "cuda" => {
                let deadline = Instant::now() + Duration::from_secs(args.ready_timeout);
                while !gpu_ready(&http, &args.gpu_endpoint).await {
                    anyhow::ensure!(
                        Instant::now() < deadline,
                        "GPU server at {} never became ready",
                        args.gpu_endpoint
                    );
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
                let endpoint = args.gpu_endpoint.clone();
                tokio::task::spawn_blocking(move || ProverClient {
                    prover: Box::new(CudaProver::new(
                        ZKMProver::new(),
                        ZKMGpuServer::External { endpoint },
                    )),
                })
                .await?
            }
            other => anyhow::bail!("unknown backend {other}"),
        };
        Ok(Self {
            client: Arc::new(client),
            backend: args.backend.clone(),
            gpu_endpoint: args.gpu_endpoint.clone(),
            keys: Mutex::new(LruCache::new(
                NonZeroUsize::new(args.pk_cache.max(1)).unwrap(),
            )),
            running: Mutex::new(None),
            finished: Mutex::new(LruCache::new(NonZeroUsize::new(FINISHED_KEPT).unwrap())),
            http,
            last_ready: Mutex::new(None),
        })
    }

    pub fn zkm_version(&self) -> &str {
        zkm_sdk::ZKM_CIRCUIT_VERSION
    }

    /// A GPU server busy with a proof can answer Ready late; reporting the node not
    /// ready then keeps the scheduler from sending the next job for a heartbeat. So a
    /// node that is proving is ready, and one failed probe soon after a good one is a
    /// slow answer, not an outage (an unreachable server fails jobs as unavailable).
    pub async fn ready(&self) -> bool {
        if self.backend != "cuda" || self.running_job().is_some() {
            return true;
        }
        if gpu_ready(&self.http, &self.gpu_endpoint).await {
            *self.last_ready.lock() = Some(Instant::now());
            return true;
        }
        self.last_ready
            .lock()
            .is_some_and(|t| t.elapsed() < READY_GRACE)
    }

    pub fn cached(&self) -> Vec<String> {
        self.keys.lock().iter().map(|(k, _)| k.clone()).collect()
    }

    /// The job being proved, if any.
    pub fn running_job(&self) -> Option<String> {
        self.running.lock().as_ref().map(|r| r.job_id.clone())
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
            return Err(Status::invalid_argument(format!(
                "elf does not hash to {elf_id} (got {got})"
            )));
        }
        let client = self.client.clone();
        let t = Instant::now();
        let keys = tokio::task::spawn_blocking(move || client.setup(&elf))
            .await
            .map_err(|e| Status::internal(format!("setup panicked: {e}")))?;
        tracing::info!(
            elf_id,
            setup_ms = t.elapsed().as_millis() as u64,
            "program set up"
        );
        let keys = Arc::new(keys);
        self.keys.lock().put(elf_id.to_string(), keys.clone());
        Ok(keys)
    }

    /// Starts the proof in its own task, so it finishes even when the caller goes away, or
    /// joins the running or a finished proof of the same job.
    pub async fn prove(self: &Arc<Self>, req: ProveRequest) -> Result<ProveResponse, Status> {
        let mut done = {
            let mut running = self.running.lock();
            if let Some(out) = self.finished.lock().get(&req.job_id) {
                tracing::info!(job = %req.job_id, "returning the result of a finished proof");
                return out.clone().map_err(|(code, msg)| Status::new(code, msg));
            }
            match &*running {
                Some(r) if r.job_id == req.job_id => {
                    tracing::info!(job = %req.job_id, "joining the running proof");
                    r.done.clone()
                }
                Some(_) => return Err(Status::resource_exhausted("node busy")),
                None => {
                    let (tx, rx) = watch::channel(None);
                    *running = Some(Running {
                        job_id: req.job_id.clone(),
                        done: rx.clone(),
                    });
                    let me = self.clone();
                    tokio::spawn(async move {
                        let job_id = req.job_id.clone();
                        let out = me
                            .run(req)
                            .await
                            .map_err(|s| (s.code(), s.message().to_string()));
                        {
                            let mut running = me.running.lock();
                            me.finished.lock().put(job_id, out.clone());
                            *running = None;
                        }
                        let _ = tx.send(Some(out));
                    });
                    rx
                }
            }
        };
        let out = done
            .wait_for(Option::is_some)
            .await
            .map_err(|_| Status::internal("the proof task ended"))?
            .clone();
        out.expect("waited for Some")
            .map_err(|(code, msg)| Status::new(code, msg))
    }

    async fn run(&self, req: ProveRequest) -> Result<ProveResponse, Status> {
        let t0 = Instant::now();
        // A direct probe: ready() is true while this very job counts as running.
        if self.backend == "cuda" && !gpu_ready(&self.http, &self.gpu_endpoint).await {
            return Err(Status::unavailable("GPU server not ready"));
        }
        let keys = self.keys_for(&req.elf_id, req.elf).await?;
        let kind = match ProofKind::try_from(req.kind).unwrap_or(ProofKind::Unspecified) {
            ProofKind::Core => ZKMProofKind::Core,
            ProofKind::Compressed => ZKMProofKind::Compressed,
            ProofKind::Groth16 => ZKMProofKind::Groth16,
            ProofKind::Plonk => ZKMProofKind::Plonk,
            ProofKind::CompressToGroth16 => ZKMProofKind::CompressToGroth16,
            ProofKind::Unspecified => {
                return Err(Status::invalid_argument("proof kind unspecified"))
            }
        };
        let proofs = req
            .receipts
            .iter()
            .map(|r| bincode::deserialize(r))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| Status::invalid_argument(format!("receipt: {e}")))?;
        let stdin = ZKMStdin {
            buffer: req.stdin_buffer,
            ptr: 0,
            proofs,
        };
        let client = self.client.clone();
        let elf_id = req.elf_id.clone();
        let job_id = req.job_id.clone();
        let t1 = Instant::now();
        let (proof, cycles, server_ms) = tokio::task::spawn_blocking(move || {
            let r = client
                .prover
                .prove_with_cycles(&keys.0, &stdin, kind, Some(elf_id));
            let server_ms = client.prover.take_prove_ms();
            r.map(|(p, c)| (p, c, server_ms))
        })
        .await
        .map_err(|e| Status::internal(format!("prover panicked: {e}")))?
        .map_err(|e| {
            // The GPU server went away mid-proof: not the job's fault, the scheduler retries it.
            let msg = format!("{e:#}");
            if msg.contains("error sending request") || msg.contains("onnection refused") {
                Status::unavailable(msg)
            } else {
                Status::internal(msg)
            }
        })?;
        if self.backend == "mock" {
            if let Some(ms) = std::env::var("FLEET_MOCK_DELAY_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
            {
                tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
            }
        }
        let prove_ms = server_ms.unwrap_or(t1.elapsed().as_millis() as u64);
        let proof_json = serde_json::to_vec(&proof.proof)
            .map_err(|e| Status::internal(format!("encoding proof: {e}")))?;
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
    match http
        .post(&url)
        .header("content-type", "application/protobuf")
        .body(Vec::<u8>::new())
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => r
            .bytes()
            .await
            .map(|b| b.as_ref() == [0x08, 0x01])
            .unwrap_or(false),
        _ => false,
    }
}

pub struct NodeApi {
    pub node: Arc<Node>,
}

#[tonic::async_trait]
impl NodeService for NodeApi {
    async fn prove(
        &self,
        request: Request<ProveRequest>,
    ) -> Result<Response<ProveResponse>, Status> {
        self.node
            .prove(request.into_inner())
            .await
            .map(Response::new)
    }

    async fn health(&self, _: Request<HealthRequest>) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            ready: self.node.ready().await,
            cached_elf_ids: self.node.cached(),
            busy: self.node.running_job().is_some(),
        }))
    }
}

fn bearer_ok(req: &Request<()>, token: &str) -> bool {
    let Some(got) = req
        .metadata()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
    else {
        return false;
    };
    got.len() == token.len()
        && got
            .bytes()
            .zip(token.bytes())
            .fold(0u8, |a, (x, y)| a | (x ^ y))
            == 0
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
