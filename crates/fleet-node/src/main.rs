//! A prover node: one Ziren SDK `ProverClient` (CPU, a GPU server, or mock) behind
//! the fleet's NodeService. It registers itself with the scheduler, heartbeats,
//! and proves one job at a time.

mod prover;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tonic::transport::{Channel, Endpoint, Server};

use fleet_proto::fleet::v1::{
    control_service_client::ControlServiceClient, node_service_server::NodeServiceServer, DeregisterRequest,
    HeartbeatRequest, NodeInfo, RegisterRequest,
};

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
    #[arg(long, env = "FLEET_GPU_ENDPOINT", default_value = "http://127.0.0.1:3000/twirp/")]
    gpu_endpoint: String,
    /// Where NodeService listens.
    #[arg(long, env = "FLEET_LISTEN", default_value = "0.0.0.0:31000")]
    listen: SocketAddr,
    /// How the scheduler reaches this node, e.g. http://10.0.0.5:31000.
    #[arg(long, env = "FLEET_ADVERTISE")]
    advertise: String,
    /// Scheduler control API, e.g. http://10.0.0.1:51001.
    #[arg(long, env = "FLEET_SCHEDULER")]
    scheduler: String,
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
    /// Program keys kept per node.
    #[arg(long, env = "FLEET_PK_CACHE", default_value_t = 8)]
    pk_cache: usize,
    /// Seconds to wait for the GPU server's Ready at start.
    #[arg(long, env = "FLEET_READY_TIMEOUT", default_value_t = 600)]
    ready_timeout: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let args = Args::parse();
    let token = load_token(&args.cluster_token_file)?;

    let node = Arc::new(prover::Node::start(&args).await?);
    let service = NodeServiceServer::new(prover::NodeApi { node: node.clone() })
        .max_decoding_message_size(1 << 30)
        .max_encoding_message_size(1 << 30);
    let service = tonic::service::interceptor::InterceptedService::new(service, prover::RequireToken(Arc::new(token.clone())));
    let server = Server::builder().add_service(service).serve(args.listen);
    tracing::info!(listen = %args.listen, backend = %args.backend, "node service up");

    let control = tokio::spawn(control_loop(args.clone(), token, node.clone()));
    tokio::select! {
        r = server => r?,
        r = control => r??,
        _ = tokio::signal::ctrl_c() => tracing::info!("shutting down"),
    }
    Ok(())
}

fn load_token(path: &std::path::Path) -> anyhow::Result<String> {
    let t = std::fs::read_to_string(path)?.trim().to_string();
    anyhow::ensure!(t.len() >= 32, "cluster token in {} is shorter than 32 characters", path.display());
    Ok(t)
}

type Control = ControlServiceClient<tonic::codegen::InterceptedService<Channel, prover::AttachToken>>;

async fn control_client(args: &Args, token: &str) -> anyhow::Result<Control> {
    let channel = Endpoint::from_shared(args.scheduler.clone())?.connect_timeout(Duration::from_secs(5)).connect_lazy();
    Ok(ControlServiceClient::with_interceptor(channel, prover::AttachToken::new(token)?))
}

/// Registers, heartbeats, re-registers when the scheduler forgets us, deregisters on exit.
async fn control_loop(args: Args, token: String, node: Arc<prover::Node>) -> anyhow::Result<()> {
    let mut client = control_client(&args, &token).await?;
    let info = NodeInfo {
        name: args.name.clone(),
        endpoint: args.advertise.clone(),
        backend: args.backend.clone(),
        gpus: args.gpus,
        cpus: args.cpus,
        zkm_version: node.zkm_version().to_string(),
        image: args.image.clone(),
        machine: args.machine.clone(),
    };
    let mut node_id: Option<String> = None;
    let mut period = Duration::from_secs(5);
    loop {
        match &node_id {
            None => match client.register(RegisterRequest { node: Some(info.clone()) }).await {
                Ok(r) => {
                    let r = r.into_inner();
                    tracing::info!(node_id = %r.node_id, "registered");
                    period = Duration::from_secs(r.heartbeat_secs.max(1) as u64);
                    node_id = Some(r.node_id);
                    continue;
                }
                Err(e) => tracing::warn!("register failed: {e}"),
            },
            Some(id) => {
                let hb = HeartbeatRequest { node_id: id.clone(), ready: node.ready().await, cached_elf_ids: node.cached() };
                match client.heartbeat(hb).await {
                    Ok(r) => {
                        if !r.into_inner().known {
                            tracing::warn!("scheduler forgot this node; registering again");
                            node_id = None;
                            continue;
                        }
                    }
                    Err(e) => tracing::warn!("heartbeat failed: {e}"),
                }
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(period) => {}
            _ = tokio::signal::ctrl_c() => {
                if let Some(id) = node_id {
                    let _ = client.deregister(DeregisterRequest { node_id: id }).await;
                }
                return Ok(());
            }
        }
    }
}
