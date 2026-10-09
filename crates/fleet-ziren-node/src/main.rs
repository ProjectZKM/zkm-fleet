//! A prover node: one Ziren SDK `ProverClient` (CPU, a GPU server, or mock) behind
//! outbound control requests. It registers with the scheduler, heartbeats,
//! and proves one job at a time.

mod prover;

use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};

use fleet_proto::fleet::v1::{
    control_service_client::ControlServiceClient, Assignment, Completion, DeregisterRequest,
    FetchInputsRequest, HeartbeatRequest, NodeInfo, RegisterRequest,
};
use fleet_ziren_node::output::Output;

#[derive(Parser, Clone)]
#[command(about = "zkm-fleet prover node")]
pub struct Args {
    /// Unique, stable name (e.g. the Ray actor name).
    #[arg(long, env = "FLEET_NODE_NAME")]
    name: String,
    /// "cuda", "cpu" or "mock".
    #[arg(long, env = "FLEET_BACKEND", default_value = "cuda")]
    backend: String,
    /// GPU server twirp endpoint, for the cuda backend.
    #[arg(
        long,
        env = "FLEET_GPU_ENDPOINT",
        default_value = "http://127.0.0.1:3000/twirp/"
    )]
    gpu_endpoint: String,
    /// Scheduler control API, e.g. https://scheduler.internal:51001.
    #[arg(long, env = "FLEET_SCHEDULER")]
    scheduler: String,
    /// Optional CA bundle for a private scheduler certificate authority.
    #[arg(long, env = "FLEET_SCHEDULER_CA_FILE")]
    scheduler_ca_file: Option<std::path::PathBuf>,
    /// File holding the cluster token shared with the scheduler.
    #[arg(long, env = "FLEET_CLUSTER_TOKEN_FILE")]
    cluster_token_file: std::path::PathBuf,
    #[arg(long, env = "FLEET_GPUS", default_value_t = 0)]
    gpus: u32,
    #[arg(long, env = "FLEET_CPUS", default_value_t = 0)]
    cpus: u32,
    /// GPU server image digest, reported to the scheduler.
    #[arg(long, env = "FLEET_IMAGE", default_value = "")]
    image: String,
    #[arg(long, env = "FLEET_MACHINE", default_value = "")]
    machine: String,
    /// Namespace/tenant/ELF identity for this dedicated proof-app server.
    #[arg(long, env = "FLEET_APP_NAMESPACE", default_value = "")]
    app_namespace: String,
    #[arg(long, env = "FLEET_APP_TENANT", default_value = "")]
    app_tenant: String,
    #[arg(long, env = "FLEET_APP_ELF_ID", default_value = "")]
    app_elf_id: String,
    /// Proof-node output directory; optionally exposed read-only to the SDK adapter.
    #[arg(long, env = "FLEET_OUTPUT_DIR", default_value = "proof-output")]
    output_dir: std::path::PathBuf,
    /// Program keys kept per node.
    #[arg(long, env = "FLEET_PK_CACHE", default_value_t = 8)]
    pk_cache: usize,
    /// Seconds to wait for the GPU server's Ready at start.
    #[arg(long, env = "FLEET_READY_TIMEOUT", default_value_t = 600)]
    ready_timeout: u64,
    /// Exit when the process that started this node goes away (a Ray actor): an orphan
    /// would keep its name registered and point it at a GPU server port another node reuses.
    #[arg(long, env = "FLEET_EXIT_WITH_PARENT", default_value_t = false)]
    exit_with_parent: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();
    let token = load_token(&args.cluster_token_file)?;

    let node = Arc::new(prover::Node::start(&args).await?);
    tracing::info!(backend = %args.backend, "worker ready; contacting scheduler through heartbeats");
    let control = tokio::spawn(control_loop(args.clone(), token, node.clone()));
    tokio::select! {
        r = control => r??,
        _ = tokio::signal::ctrl_c() => tracing::info!("shutting down"),
        _ = parent_gone(args.exit_with_parent) => tracing::warn!("the process that started this node is gone; exiting"),
    }
    Ok(())
}

/// Resolves when the parent process changes (it died and the node was re-parented);
/// never, unless asked to watch.
async fn parent_gone(watch: bool) {
    if !watch {
        return std::future::pending().await;
    }
    let parent = std::os::unix::process::parent_id();
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        if std::os::unix::process::parent_id() != parent {
            return;
        }
    }
}

fn load_token(path: &std::path::Path) -> anyhow::Result<String> {
    let t = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("reading the cluster token {}: {e}", path.display()))?
        .trim()
        .to_string();
    anyhow::ensure!(
        t.len() >= 32,
        "cluster token in {} is shorter than 32 characters",
        path.display()
    );
    Ok(t)
}

type Control =
    ControlServiceClient<tonic::codegen::InterceptedService<Channel, prover::AttachToken>>;

type Outcome = Result<(u64, u64), tonic::Status>;

/// All scheduler connections originate here. Proof work and output persistence
/// run separately so neither can block health and ownership heartbeats.
async fn control_loop(args: Args, token: String, node: Arc<prover::Node>) -> anyhow::Result<()> {
    let mut endpoint =
        Endpoint::from_shared(args.scheduler.clone())?.connect_timeout(Duration::from_secs(5));
    if endpoint.uri().scheme_str() == Some("https") {
        let host = endpoint
            .uri()
            .host()
            .ok_or_else(|| anyhow::anyhow!("scheduler HTTPS URL has no host"))?
            .to_owned();
        let mut tls = ClientTlsConfig::new().with_webpki_roots().domain_name(host);
        if let Some(path) = &args.scheduler_ca_file {
            tls = tls.ca_certificate(Certificate::from_pem(std::fs::read(path)?));
        }
        endpoint = endpoint.tls_config(tls)?;
    } else {
        anyhow::ensure!(endpoint.uri().host().is_some_and(|h| h == "localhost" || h == "127.0.0.1"),
            "scheduler control must use HTTPS outside loopback");
    }
    let mut client: Control = ControlServiceClient::with_interceptor(
        endpoint.clone().connect_lazy(),
        prover::AttachToken::new(&token)?,
    )
    .max_decoding_message_size(1 << 30)
    .max_encoding_message_size(1 << 30);
    let inputs: Control = ControlServiceClient::with_interceptor(
        endpoint.connect_lazy(),
        prover::AttachToken::new(&token)?,
    )
    .max_decoding_message_size(1 << 30)
    .max_encoding_message_size(1 << 30);
    let output = Output::new(args.output_dir.clone())?;
    let info = NodeInfo {
        name: args.name.clone(),
        backend: args.backend.clone(),
        gpus: args.gpus,
        cpus: args.cpus,
        zkm_version: node.zkm_version().into(),
        image: args.image.clone(),
        machine: args.machine.clone(),
        control_version: 1,
        app_namespace: args.app_namespace.clone(),
        app_tenant: args.app_tenant.clone(),
        app_elf_id: args.app_elf_id.to_ascii_lowercase(),
    };
    let mut node_id: Option<String> = None;
    let mut period = Duration::from_secs(5);
    let mut working: Option<(Assignment, tokio::task::JoinHandle<Outcome>)> = None;
    let mut pending: Option<(Assignment, Outcome)> = None;
    loop {
        if node_id.is_none() {
            let mut request = tonic::Request::new(RegisterRequest {
                node: Some(info.clone()),
            });
            request.set_timeout(Duration::from_secs(10));
            match client.register(request).await {
                Ok(r) => {
                    let r = r.into_inner();
                    tracing::info!(node_id = %r.node_id, "registered");
                    period = Duration::from_secs(r.heartbeat_secs.max(1) as u64);
                    node_id = Some(r.node_id);
                }
                Err(e) => tracing::warn!("register failed: {e}"),
            }
        }
        if let Some(id) = node_id.clone() {
            if working.as_ref().is_some_and(|(_, task)| task.is_finished()) {
                let (assignment, task) = working.take().unwrap();
                let result = task
                    .await
                    .unwrap_or_else(|e| Err(tonic::Status::internal(e.to_string())));
                pending = Some((assignment, result));
            }
            let completion = pending.as_ref().map(|(assignment, result)| {
                let mut completion = Completion {
                    assignment: Some(assignment.clone()),
                    ..Default::default()
                };
                match result {
                    Ok((cycles, prove_ms)) => {
                        completion.cycles = *cycles;
                        completion.prove_ms = *prove_ms;
                    }
                    Err(e) => {
                        completion.error_code = e.code() as i32;
                        completion.error = e.message().into();
                    }
                }
                completion
            });
            let running_job = working
                .as_ref()
                .map(|(a, _)| a.job_id.clone())
                .or_else(|| pending.as_ref().map(|(a, _)| a.job_id.clone()))
                .unwrap_or_default();
            let mut request = tonic::Request::new(HeartbeatRequest {
                node_id: id.clone(),
                ready: node.ready().await,
                cached_elf_ids: node.cached(),
                busy: !running_job.is_empty(),
                running_job,
                completion,
            });
            request.set_timeout(Duration::from_secs(10));
            match client.heartbeat(request).await {
                Ok(r) => {
                    let r = r.into_inner();
                    if !r.known {
                        node_id = None;
                        continue;
                    }
                    if pending
                        .as_ref()
                        .is_some_and(|(a, _)| a.lease == r.completed_lease)
                    {
                        pending = None;
                    }
                    if working.is_none() && pending.is_none() {
                        if let Some(assignment) = r.assignment {
                            let mut store = inputs.clone();
                            let n = node.clone();
                            let req = FetchInputsRequest {
                                node_id: id.clone(),
                                include_elf: !n.cached().contains(&assignment.elf_id),
                                assignment: Some(assignment.clone()),
                            };
                            tracing::info!(job = %assignment.job_id, lease = %assignment.lease, "accepted assignment");
                            let out = output.clone();
                            let execution = assignment.clone();
                            working = Some((
                                assignment,
                                tokio::spawn(async move {
                                    let saved = tokio::task::spawn_blocking({
                                        let out = out.clone();
                                        let execution = execution.clone();
                                        move || out.completed(&execution)
                                    })
                                    .await
                                    .map_err(|e| tonic::Status::unavailable(e.to_string()))?
                                    .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
                                    if let Some(metadata) = saved {
                                        return Ok(metadata);
                                    }
                                    let inputs = store
                                        .fetch_inputs(req)
                                        .await
                                        .map_err(|e| {
                                            if e.code() == tonic::Code::NotFound {
                                                tonic::Status::unavailable(e.message().to_owned())
                                            } else {
                                                e
                                            }
                                        })?
                                        .into_inner();
                                    let result = Arc::new(n.prove(inputs).await?);
                                    loop {
                                        let published = tokio::task::spawn_blocking({
                                            let out = out.clone();
                                            let execution = execution.clone();
                                            let result = result.clone();
                                            move || out.publish(&execution, &result)
                                        })
                                        .await;
                                        match published {
                                            Ok(Ok(())) => {
                                                return Ok((result.cycles, result.prove_ms))
                                            }
                                            other => {
                                                tracing::error!(job = %execution.job_id, error = ?other, "proof output write failed; retaining proof and reservation")
                                            }
                                        }
                                        tokio::time::sleep(Duration::from_secs(5)).await;
                                    }
                                }),
                            ));
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("heartbeat failed; retaining execution and pending result: {e}")
                }
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(period) => {},
            _ = tokio::signal::ctrl_c() => {
                if let Some(id) = node_id { let _ = client.deregister(DeregisterRequest { node_id: id }).await; }
                return Ok(());
            }
        }
    }
}
