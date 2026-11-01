//! SDK protocol and payload storage belong to the app, outside Fleet's control plane.
use axum::{
    extract::{Path as HttpPath, State},
    http::StatusCode,
    response::{IntoResponse, Response as HttpResponse},
    routing::get,
};
use clap::Parser;
use fleet_proto::sdk::SubmitProofRequest;
use fleet_proto::{
    artifacts,
    fleet::v1::{ProofInputs, ProveRequest},
    stage::v1::{
        self as stage,
        stage_service_server::{StageService, StageServiceServer},
    },
};
use prost::Message;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use tonic::{transport::Server, Request, Response, Status};

#[derive(Parser)]
pub struct ProofAppArgs {
    #[arg(long, default_value = "127.0.0.1:51010")]
    pub stage_addr: std::net::SocketAddr,
    #[arg(long, default_value = "127.0.0.1:51011")]
    pub http_addr: std::net::SocketAddr,
    #[arg(long)]
    pub scheduler: String,
    /// Public CA bundle when the scheduler uses an operator-issued certificate.
    #[arg(long)]
    pub scheduler_ca_file: Option<PathBuf>,
    #[arg(long)]
    pub storage_root: PathBuf,
    /// Immutable Fleet namespace ID for this app endpoint.
    #[arg(long)]
    pub namespace: String,
    /// The programs this endpoint proves (repeat); each is an app of the namespace.
    #[arg(long, required = true)]
    pub elf_id: Vec<String>,
    #[arg(long)]
    pub elf_file: Vec<PathBuf>,
    /// Namespace user token for older SDKs on this loopback-only endpoint.
    #[arg(long)]
    pub token_file: Option<PathBuf>,
}

#[derive(Clone)]
struct PublicValuesCredential {
    bearer: String,
    app: String,
    job_id: String,
    expires: Instant,
}

pub struct ProofAppServer {
    pub storage_root: PathBuf,
    pub elf_ids: HashSet<String>,
    pub token_file: Option<PathBuf>,
    pub namespace: String,
    pub scheduler: String,
    pub public_values_url: String,
    client: reqwest::Client,
    capabilities: Mutex<HashMap<String, PublicValuesCredential>>,
    /// Program -> the namespace's app of it, as the scheduler named it.
    apps: Mutex<HashMap<String, String>>,
}

impl ProofAppServer {
    pub fn new(
        storage_root: PathBuf,
        namespace: String,
        elf_ids: Vec<String>,
        scheduler: String,
        public_values_url: String,
    ) -> anyhow::Result<Self> {
        let url = reqwest::Url::parse(&scheduler)?;
        anyhow::ensure!(
            url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none()
                && url.path() == "/",
            "scheduler must be an endpoint URL"
        );
        anyhow::ensure!(
            url.scheme() == "https"
                || (url.scheme() == "http"
                    && matches!(url.host_str(), Some("127.0.0.1" | "localhost"))),
            "scheduler requires HTTPS outside loopback"
        );
        anyhow::ensure!(
            namespace.len() == 32
                && namespace
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "namespace must be an immutable namespace ID"
        );
        anyhow::ensure!(
            !elf_ids.is_empty()
                && elf_ids.iter().all(|id| id.len() == 64
                    && id
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))),
            "ELF ids must be SHA-256 hashes"
        );
        std::fs::create_dir_all(storage_root.join("elf"))?;
        std::fs::create_dir_all(storage_root.join("inputs"))?;
        std::fs::create_dir_all(storage_root.join("outputs"))?;
        Ok(Self {
            storage_root: storage_root.canonicalize()?,
            namespace,
            elf_ids: elf_ids.into_iter().collect(),
            token_file: None,
            scheduler: scheduler.trim_end_matches('/').into(),
            public_values_url,
            client: reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(75))
                .build()?,
            capabilities: Mutex::new(HashMap::new()),
            apps: Mutex::new(HashMap::new()),
        })
    }

    fn bearer<T>(&self, request: &Request<T>) -> Result<String, Status> {
        if let Some(header) = request.metadata().get("authorization") {
            return header
                .to_str()
                .ok()
                .filter(|s| s.starts_with("Bearer ") && s.len() > 7)
                .map(String::from)
                .ok_or_else(|| Status::unauthenticated("invalid user authorization"));
        }
        let path = self
            .token_file
            .as_ref()
            .ok_or_else(|| Status::unauthenticated("user token required"))?;
        let token = fleet_proto::load_token(path)
            .map_err(|_| Status::unauthenticated("configured user token is unavailable"))?;
        Ok(format!("Bearer {token}"))
    }

    async fn call(
        &self,
        bearer: &str,
        method: reqwest::Method,
        path: &str,
        body: Option<&SubmitProofRequest>,
    ) -> Result<serde_json::Value, Status> {
        let mut request = self
            .client
            .request(method, format!("{}{path}", self.scheduler))
            .header("authorization", bearer);
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request
            .send()
            .await
            .map_err(|e| Status::unavailable(e.to_string()))?;
        let code = response.status();
        if !code.is_success() {
            let text = response.text().await.unwrap_or_default();
            return Err(match code.as_u16() {
                401 => Status::unauthenticated(text),
                403 => Status::permission_denied(text),
                404 => Status::not_found(text),
                _ => Status::unavailable(text),
            });
        }
        response
            .json()
            .await
            .map_err(|e| Status::internal(e.to_string()))
    }

    /// The token belongs to this endpoint's namespace; with a program, the token may use
    /// the namespace's app of it, whose name it returns.
    async fn authorize(&self, bearer: &str, elf_id: Option<&str>) -> Result<Option<String>, Status> {
        let access = self
            .call(bearer, reqwest::Method::GET, "/access", None)
            .await?;
        if access["namespace"].as_str() != Some(self.namespace.as_str()) {
            return Err(Status::permission_denied(
                "token does not belong to this app's namespace",
            ));
        }
        match elf_id {
            Some(elf_id) => self.app_for(bearer, elf_id).await.map(Some),
            None => Ok(None),
        }
    }

    /// The namespace's app of program `elf_id`, among the apps the token may use.
    async fn app_for(&self, bearer: &str, elf_id: &str) -> Result<String, Status> {
        if let Some(app) = self.apps.lock().await.get(elf_id) {
            return Ok(app.clone());
        }
        let apps = self.call(bearer, reqwest::Method::GET, "/apps", None).await?;
        let app = apps.as_array().into_iter().flatten()
            .find(|a| a["kind"] == "app" && a["namespace"].as_str() == Some(self.namespace.as_str())
                && a["proof"]["elf_id"].as_str() == Some(elf_id))
            .and_then(|a| a["name"].as_str())
            .ok_or_else(|| Status::permission_denied("the program is no app this token may use"))?
            .to_string();
        self.apps.lock().await.insert(elf_id.into(), app.clone());
        Ok(app)
    }

    fn store(
        &self,
        directory: &str,
        bytes: &[u8],
    ) -> anyhow::Result<fleet_proto::fleet::v1::ArtifactRef> {
        anyhow::ensure!(
            bytes.len() as u64 <= artifacts::MAX_INPUT,
            "app input exceeds size limit"
        );
        let path = self
            .storage_root
            .join(directory)
            .join(artifacts::hash(bytes));
        let reference = artifacts::reference(&path, bytes)?;
        if path.exists() {
            artifacts::read(&reference, &self.storage_root, artifacts::MAX_INPUT)?;
            return Ok(reference);
        }
        let temp = path.with_extension(uuid::Uuid::new_v4().to_string());
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        std::fs::rename(temp, &path)?;
        std::fs::File::open(path.parent().unwrap())?.sync_all()?;
        Ok(reference)
    }

    fn select_elf(&self, request: &stage::GenerateProofRequest) -> anyhow::Result<String> {
        let supplied = request
            .elf_id
            .as_ref()
            .map(|id| id.trim_start_matches("0x").to_ascii_lowercase());
        let id = if !request.elf_data.is_empty() {
            let hash = artifacts::hash(&request.elf_data);
            anyhow::ensure!(
                supplied.as_ref().is_none_or(|id| id == &hash),
                "ELF hash mismatch"
            );
            hash
        } else {
            supplied
                .or_else(|| {
                    (self.elf_ids.len() == 1).then(|| self.elf_ids.iter().next().unwrap().clone())
                })
                .ok_or_else(|| anyhow::anyhow!("ELF id required for a multi-program endpoint"))?
        };
        anyhow::ensure!(
            self.elf_ids.contains(&id),
            "ELF does not belong to this app endpoint"
        );
        Ok(id)
    }

    /// Read only references from an authenticated job record, under this app's root.
    async fn read_output(&self, job: &serde_json::Value, name: &str) -> Result<Vec<u8>, Status> {
        let field = match name {
            "proof.json" => "proof",
            "public_values.bin" => "public_values",
            _ => return Err(Status::invalid_argument("invalid output name")),
        };
        if job["state"] != "succeeded"
            || job["namespace"].as_str() != Some(&self.namespace)
            || !job["elf_id"]
                .as_str()
                .is_some_and(|id| self.elf_ids.contains(id))
        {
            return Err(Status::permission_denied(
                "output belongs to another app or is not complete",
            ));
        }
        let outputs: fleet_proto::fleet::v1::ProofOutputs =
            serde_json::from_value(job["outputs"].clone())
                .map_err(|_| Status::failed_precondition("local output references unavailable"))?;
        artifacts::validate_outputs(&outputs).map_err(|e| Status::data_loss(e.to_string()))?;
        let reference = if field == "proof" {
            outputs.proof.unwrap()
        } else {
            outputs.public_values.unwrap()
        };
        let root = self.storage_root.join("outputs");
        tokio::task::spawn_blocking(move || {
            artifacts::read(&reference, &root, artifacts::MAX_OUTPUT)
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .map_err(|e| Status::data_loss(e.to_string()))
    }

    fn prepare(&self, request: &stage::GenerateProofRequest) -> anyhow::Result<SubmitProofRequest> {
        anyhow::ensure!(artifacts::is_safe_id(&request.proof_id), "invalid proof_id");
        let elf_id = self.select_elf(request)?;
        let kind = fleet_proto::sdk::kind_for(request.from_step, request.target_step)
            .map_err(anyhow::Error::msg)?;
        let elf = if request.elf_data.is_empty() {
            let path = self.storage_root.join("elf").join(&elf_id);
            let bytes = std::fs::read(&path)?;
            let reference = artifacts::reference(&path, &bytes)?;
            anyhow::ensure!(reference.sha256 == elf_id, "cached ELF hash mismatch");
            reference
        } else {
            anyhow::ensure!(
                artifacts::hash(&request.elf_data) == elf_id,
                "ELF does not belong to this app"
            );
            self.store("elf", &request.elf_data)?
        };
        let stdin_buffer = if request.private_input_stream.is_empty() {
            vec![]
        } else {
            bincode::deserialize(&request.private_input_stream)?
        };
        let input = ProveRequest {
            job_id: request.proof_id.clone(),
            elf_id: elf_id.clone(),
            elf: vec![],
            stdin_buffer,
            receipts: if request.receipts.is_empty() {
                request.receipt_inputs.clone()
            } else {
                anyhow::ensure!(
                    request.receipt_inputs.is_empty() || request.receipt.inputs == request.receipts,
                    "conflicting receipts fields"
                );
                request.receipts.clone()
            },
            kind: kind as i32,
        };
        let reference = self.store("inputs", &input.encode_to_vec())?;
        Ok(SubmitProofRequest {
            proof_id: request.proof_id.clone(),
            elf_id: elf_id.clone(),
            signature: request.signature.clone(),
            block_no: request.block_no,
            seg_size: request.seg_size,
            from_step: request.from_step,
            target_step: request.target_step,
            inputs: ProofInputs {
                input: Some(reference),
                elf: Some(elf),
                output_root: artifacts::file_uri(&self.storage_root.join("outputs"))?,
            },
        })
    }
}

#[derive(Clone)]
pub struct ProofAppStageService(pub Arc<ProofAppServer>);
impl std::ops::Deref for ProofAppStageService {
    type Target = Arc<ProofAppServer>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[tonic::async_trait]
impl StageService for ProofAppStageService {
    async fn generate_proof(
        &self,
        request: Request<stage::GenerateProofRequest>,
    ) -> Result<Response<stage::GenerateProofResponse>, Status> {
        let bearer = self.bearer(&request)?;
        let elf_id = self
            .select_elf(request.get_ref())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let app_name = self.authorize(&bearer, Some(&elf_id)).await?.expect("a program's app");
        let start = Instant::now();
        let app = self.clone();
        let request = request.into_inner();
        let metadata = tokio::task::spawn_blocking(move || app.prepare(&request))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let stored_ms = start.elapsed().as_millis();
        let admission = Instant::now();
        let response = self
            .call(
                &bearer,
                reqwest::Method::POST,
                &format!("/apps/{app_name}/jobs"),
                Some(&metadata),
            )
            .await?;
        tracing::info!(proof_id=%metadata.proof_id,stored_ms,admission_ms=admission.elapsed().as_millis(),"proof submitted using app storage references");
        Ok(Response::new(stage::GenerateProofResponse {
            proof_id: metadata.proof_id,
            status: response["status"]
                .as_i64()
                .unwrap_or(stage::Status::InternalError as i64) as i32,
            error_message: response["error_message"].as_str().unwrap_or("").into(),
            ..Default::default()
        }))
    }

    async fn get_status(
        &self,
        request: Request<stage::GetStatusRequest>,
    ) -> Result<Response<stage::GetStatusResponse>, Status> {
        let bearer = self.bearer(&request)?;
        self.authorize(&bearer, None).await?;
        let id = request.into_inner().proof_id;
        if !artifacts::is_safe_id(&id) {
            return Err(Status::invalid_argument("invalid proof_id"));
        }
        // The job is one of this endpoint's apps'.
        let mut found = None;
        let mut elf_ids: Vec<&String> = self.elf_ids.iter().collect();
        elf_ids.sort();
        for elf_id in elf_ids {
            let Ok(app) = self.app_for(&bearer, elf_id).await else { continue };
            match self.call(&bearer, reqwest::Method::GET, &format!("/apps/{app}/jobs/{id}"), None).await {
                Ok(job) => {
                    found = Some((app, job));
                    break;
                }
                Err(e) if e.code() == tonic::Code::NotFound || e.code() == tonic::Code::PermissionDenied => {}
                Err(e) => return Err(e),
            }
        }
        let Some((app, job)) = found else {
            return Ok(Response::new(stage::GetStatusResponse {
                proof_id: id,
                status: stage::Status::Unspecified as i32,
                ..Default::default()
            }));
        };
        let elf_id = job["elf_id"]
            .as_str()
            .ok_or_else(|| Status::internal("job ELF missing"))?;
        // Fleet records admission failures without an ELF; SDKs still poll their IDs.
        let rejected = job["state"] == "failed" && elf_id.is_empty();
        if job["app"].as_str() != Some(app.as_str()) || (!rejected && !self.elf_ids.contains(elf_id)) {
            return Err(Status::permission_denied(
                "job belongs to another app endpoint",
            ));
        }
        let mut response = stage::GetStatusResponse {
            proof_id: id,
            ..Default::default()
        };
        match job["state"].as_str().unwrap_or("") {
            "queued" | "running" => {
                response.status = stage::Status::Computing as i32;
                response.step = if job["state"] == "running" {
                    stage::Step::InProve
                } else {
                    stage::Step::Init
                } as i32;
            }
            "failed" => {
                response.status = stage::Status::from_str_name(
                    &job["fail_status"]
                        .as_str()
                        .unwrap_or("internal_error")
                        .to_ascii_uppercase(),
                )
                .unwrap_or(stage::Status::InternalError) as i32;
                response.step = stage::Step::End as i32;
            }
            "succeeded" => {
                response.proof_with_public_inputs = self.read_output(&job, "proof.json").await?;
                response.status = stage::Status::Success as i32;
                response.step = stage::Step::End as i32;
                response.total_steps = job["cycles"].as_u64().unwrap_or(0);
                response.proving_time = job["prove_ms"].as_u64().unwrap_or(0);
                response.elf_id = elf_id.as_bytes().to_vec();
                let key = uuid::Uuid::new_v4().simple().to_string();
                let mut capabilities = self.capabilities.lock().await;
                capabilities.retain(|_, c| c.expires > Instant::now());
                capabilities.insert(
                    key.clone(),
                    PublicValuesCredential {
                        bearer,
                        app,
                        job_id: job["id"].as_str().unwrap().into(),
                        expires: Instant::now() + Duration::from_secs(300),
                    },
                );
                response.public_values_url = format!(
                    "{}/public-values/{key}",
                    self.public_values_url.trim_end_matches('/')
                );
            }
            _ => response.status = stage::Status::InternalError as i32,
        }
        Ok(Response::new(response))
    }
}

async fn values(State(app): State<Arc<ProofAppServer>>, HttpPath(key): HttpPath<String>) -> HttpResponse {
    let capability = app
        .capabilities
        .lock()
        .await
        .get(&key)
        .filter(|c| c.expires > Instant::now())
        .cloned();
    let Some(capability) = capability else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let job = match app
        .call(
            &capability.bearer,
            reqwest::Method::GET,
            &format!("/apps/{}/jobs/{}", capability.app, capability.job_id),
            None,
        )
        .await
    {
        Ok(job) => job,
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };
    let Some(elf_id) = job["elf_id"].as_str() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if app
        .authorize(&capability.bearer, Some(elf_id))
        .await
        .is_err()
    {
        return StatusCode::NOT_FOUND.into_response();
    }
    match app.read_output(&job, "public_values.bin").await {
        Ok(bytes) => bytes.into_response(),
        Err(e) => (
            StatusCode::from_u16(fleet_proto::sdk::http_status_code(e.code())).unwrap(),
            e.message().to_owned(),
        )
            .into_response(),
    }
}

pub fn router(app: Arc<ProofAppServer>) -> axum::Router {
    axum::Router::new()
        .route("/public-values/:key", get(values))
        .with_state(app)
}

pub async fn serve(args: ProofAppArgs) -> anyhow::Result<()> {
    anyhow::ensure!(
        args.stage_addr.ip().is_loopback() && args.http_addr.ip().is_loopback(),
        "app endpoint must bind loopback; use an authenticated HTTPS gateway for external access"
    );
    let mut app = ProofAppServer::new(
        args.storage_root,
        args.namespace,
        args.elf_id,
        args.scheduler,
        format!("http://{}", args.http_addr),
    )?;
    app.token_file = args.token_file;
    if let Some(ca) = args.scheduler_ca_file {
        app.client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(75))
            .add_root_certificate(reqwest::Certificate::from_pem(&std::fs::read(ca)?)?)
            .build()?;
    }
    for elf in args.elf_file {
        let bytes = std::fs::read(elf)?;
        anyhow::ensure!(
            app.elf_ids.contains(&artifacts::hash(&bytes)),
            "ELF is not configured for this endpoint"
        );
        app.store("elf", &bytes)?;
    }
    let app = Arc::new(app);
    let listener = tokio::net::TcpListener::bind(args.http_addr).await?;
    let http = axum::serve(listener, router(app.clone()));
    let stage = Server::builder()
        .add_service(
            StageServiceServer::new(ProofAppStageService(app))
                .max_decoding_message_size(1 << 30)
                .max_encoding_message_size(1 << 30),
        )
        .serve(args.stage_addr);
    tokio::select! {r=http=>r?,r=stage=>r?};
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn multiple_programs_are_allowlisted_and_legacy_receipts_are_preserved() {
        let storage = tempfile::tempdir().unwrap();
        let first = artifacts::hash(b"first");
        let second = artifacts::hash(b"second");
        let app = ProofAppServer::new(
            storage.path().into(),
            "a".repeat(32),
            vec![first.clone(), second.clone()],
            "http://127.0.0.1:9191".into(),
            "http://127.0.0.1:51011".into(),
        )
        .unwrap();
        for (bytes, id) in [(b"first".as_slice(), first), (b"second".as_slice(), second)] {
            let mut request = stage::GenerateProofRequest {
                proof_id: "job".into(),
                elf_data: bytes.into(),
                target_step: Some(stage::Step::InAgg as i32),
                receipt_inputs: vec![vec![1, 2, 3]],
                ..Default::default()
            };
            let metadata = app.prepare(&request).unwrap();
            assert_eq!(metadata.elf_id, id);
            let input = artifacts::read(
                metadata.inputs.input.as_ref().unwrap(),
                storage.path(),
                artifacts::MAX_INPUT,
            )
            .unwrap();
            assert_eq!(
                ProveRequest::decode(input.as_slice()).unwrap().receipts,
                vec![vec![1, 2, 3]]
            );
            request.elf_data.clear();
            request.elf_id = Some(id);
            assert!(app.prepare(&request).is_ok());
            request.receipts = vec![vec![4, 5]];
            assert!(app.prepare(&request).is_err());
        }
        assert!(app
            .select_elf(&stage::GenerateProofRequest::default())
            .is_err());
        assert!(app
            .select_elf(&stage::GenerateProofRequest {
                elf_data: b"other".to_vec(),
                ..Default::default()
            })
            .is_err());
    }
}
