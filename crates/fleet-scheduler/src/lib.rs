pub mod blobs;
pub mod config;
pub mod control;
pub mod fleet;
pub mod http;
pub mod placement;
pub mod preemption;
pub mod proof_api;
pub mod log_relay;
pub mod proof_fetch;
pub mod quota;
pub mod workloads;
pub mod stage;
pub mod store;
pub mod token;

use std::future::IntoFuture;
use std::sync::Arc;

use tokio::sync::watch;
use tonic::transport::{Identity, Server, ServerTlsConfig};

use fleet_proto::fleet::v1::control_service_server::ControlServiceServer;
use fleet_proto::stage::v1::stage_service_server::StageServiceServer;

/// Resolves on SIGTERM or Ctrl-C.
async fn terminated() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("SIGTERM handler");
    tokio::select! {
        _ = term.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
}

/// Starts every listener and the dispatcher; returns when one of them stops, or after a
/// graceful stop on SIGTERM: listeners close, calls in flight (long polls included) finish.
pub async fn serve(cfg: config::SchedulerConfig) -> anyhow::Result<()> {
    let cluster_token = token::load(&cfg.cluster_token_file)?;
    let (store, _leader_lock): (Arc<dyn store::Store>, Option<store::PgLeaderLock>) =
        match &cfg.database_url {
            Some(url) => {
                let pg = store::PgStore::connect(url).await?;
                let lock = pg.acquire_leader_lock().await?;
                (Arc::new(pg), Some(lock))
            }
            None => {
                tracing::warn!("no database_url: jobs and tokens are kept in memory only");
                (Arc::new(store::MemoryStore::default()), None)
            }
        };
    let blobs = blobs::BlobStore::new(&cfg.data_dir)?.with_proof_output(
        cfg.proof_output_dir
            .clone()
            .unwrap_or_else(|| cfg.data_dir.join("proofs")),
    );
    let fleet = fleet::Scheduler::new(cfg.clone(), store, blobs, cluster_token.clone());
    let mut tokens = fleet.load_tokens().await?;
    if cfg.database_url.is_none() {
        // Nothing outside this process can issue one into memory: it issues its own.
        let path = cfg.data_dir.join("admin.token");
        let (admin, secret) = token::new_admin_token("in-memory scheduler");
        fleet.add_user_token(&admin.id, &admin.address, &admin.label, None, admin.token_hash.clone(), None, None, admin.permissions.clone()).await?;
        let _ = std::fs::remove_file(&path);
        write_secret(&path, &secret)?;
        tracing::warn!(path = %path.display(), "administrator token for this in-memory scheduler");
        tokens += 1;
    }
    let apps = fleet.load_apps().await?;
    let recovered = fleet.recover().await?;
    tracing::info!(
        tokens,
        apps,
        recovered_jobs = recovered,
        "scheduler state loaded"
    );

    let mut stage_server = Server::builder();
    if let Some(tls) = &cfg.tls {
        let identity = Identity::from_pem(
            std::fs::read(&tls.cert_path)?,
            std::fs::read(&tls.key_path)?,
        );
        stage_server = stage_server.tls_config(ServerTlsConfig::new().identity(identity))?;
    } else {
        tracing::warn!("stage API without TLS: keep it off untrusted networks");
    }
    let stage = StageServiceServer::new(stage::ProofStageService::new(fleet.clone()))
    .max_decoding_message_size(1 << 30)
    .max_encoding_message_size(1 << 30);
    let stage_addr = cfg.stage_addr.parse()?;
    tracing::info!(%stage_addr, "serving the deprecated gRPC stage API for SDK network clients; new clients use fleet-proof-app");
    let (stop_tx, stop_rx) = watch::channel(false);
    let stopping = move || {
        let mut rx = stop_rx.clone();
        async move {
            let _ = rx.wait_for(|stop| *stop).await;
        }
    };
    let stage_task = stage_server
        .add_service(stage)
        .serve_with_shutdown(stage_addr, stopping());

    let workloads = Arc::new(workloads::WorkloadController::new(
        fleet.clone(),
        cluster_token.clone(),
    )?);
    let worker_service = tonic::codegen::InterceptedService::new(
        fleet_proto::fleet::v1::worker_service_server::WorkerServiceServer::new(
            workloads::NodeControlService(workloads.clone()),
        )
        .max_decoding_message_size(16 << 20)
        .max_encoding_message_size(16 << 20),
        token::ClusterTokenVerifier(Arc::new(cluster_token.clone())),
    );
    let control = tonic::codegen::InterceptedService::new(
        ControlServiceServer::new(control::ProverControlService {
            fleet: fleet.clone(),
        })
        .max_decoding_message_size(1 << 30)
        .max_encoding_message_size(1 << 30),
        token::ClusterTokenVerifier(Arc::new(cluster_token)),
    );
    let control_addr = cfg.control_addr.parse()?;
    let mut control_server = Server::builder();
    if let Some(tls) = &cfg.control_tls {
        let identity = Identity::from_pem(
            std::fs::read(&tls.cert_path)?,
            std::fs::read(&tls.key_path)?,
        );
        control_server = control_server.tls_config(ServerTlsConfig::new().identity(identity))?;
    } else {
        tracing::warn!("control API without TLS: keep it on a trusted private network");
    }
    let control_task = control_server
        .add_service(control)
        .add_service(worker_service)
        .serve_with_shutdown(control_addr, stopping());

    let http_state = http::HttpState {
        fleet: fleet.clone(),
        workloads: workloads.clone(),
    };
    let http = http::router(http_state);
    let listener = tokio::net::TcpListener::bind(&cfg.http_addr).await?;
    let http_task = axum::serve(listener, http).with_graceful_shutdown(stopping());

    tracing::info!(stage = %cfg.stage_addr, control = %cfg.control_addr, http = %cfg.http_addr, "listening");
    let dispatcher = tokio::spawn(fleet.clone().dispatch_loop());
    let dispatch_loop = tokio::spawn(workloads.dispatch_loop());
    let f = fleet.clone();
    tokio::spawn(async move {
        terminated().await;
        f.stop_dispatching();
        tracing::info!("stopping: finishing calls in flight");
        let _ = stop_tx.send(true);
    });
    let servers = async {
        tokio::try_join!(
            anyhow_of(stage_task),
            anyhow_of(control_task),
            anyhow_of(http_task)
        )?;
        tracing::info!("stopped");
        anyhow::Ok(())
    };
    tokio::select! {
        r = servers => r?,
        r = dispatcher => r?,
        r = dispatch_loop => r??,
    }
    Ok(())
}

/// Issues an administrator's token into the configured database: its secret goes to `out`
/// (0600, never overwritten), its record to the database, where a running scheduler finds it
/// within a heartbeat.
pub async fn issue_admin_token(cfg: &config::SchedulerConfig, label: &str, out: &std::path::Path) -> anyhow::Result<store::Token> {
    use store::Store;
    let url = cfg.database_url.as_deref()
        .ok_or_else(|| anyhow::anyhow!("no database_url: an in-memory scheduler writes its own admin token to data_dir/admin.token"))?;
    let pg = store::PgStore::connect(url).await?;
    let (admin, secret) = token::new_admin_token(label);
    write_secret(out, &secret)?;
    if let Err(e) = pg.add_token(&admin).await {
        let _ = std::fs::remove_file(out);
        return Err(e);
    }
    Ok(admin)
}

fn write_secret(path: &std::path::Path, secret: &str) -> anyhow::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new().write(true).create_new(true).mode(0o600).open(path)
        .map_err(|e| anyhow::anyhow!("creating {}: {e}", path.display()))?;
    f.write_all(format!("{secret}\n").as_bytes())?;
    Ok(())
}

async fn anyhow_of<E: Into<anyhow::Error>>(
    f: impl IntoFuture<Output = Result<(), E>>,
) -> anyhow::Result<()> {
    f.await.map_err(Into::into)
}
