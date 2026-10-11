pub mod auth;
pub mod blobs;
pub mod config;
pub mod control;
pub mod fleet;
pub mod http;
pub mod placement;
pub mod proof_api;
pub mod proof_fetch;
pub mod preemption;
pub mod quota;
pub mod runs_api;
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
pub async fn serve(cfg: config::Config) -> anyhow::Result<()> {
    let cluster_token = token::load(&cfg.cluster_token_file)?;
    let admin_token = token::load(&cfg.admin_token_file)?;
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
    let blobs = blobs::Blobs::new(&cfg.data_dir)?.with_proof_output(
        cfg.proof_output_dir
            .clone()
            .unwrap_or_else(|| cfg.data_dir.join("proofs")),
    );
    let fleet = fleet::Fleet::new(cfg.clone(), store, blobs, cluster_token.clone());
    let tokens = fleet.load_tokens().await?;
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
    let stage = StageServiceServer::new(stage::StageApi {
        fleet: fleet.clone(),
    })
    .max_decoding_message_size(1 << 30)
    .max_encoding_message_size(1 << 30);
    let stage_addr = cfg.stage_addr.parse()?;
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

    let runs = Arc::new(runs_api::RunControl::new(
        fleet.clone(),
        admin_token.clone(),
        cluster_token.clone(),
    )?);
    let worker_service = tonic::codegen::InterceptedService::new(
        fleet_proto::fleet::v1::worker_service_server::WorkerServiceServer::new(
            runs_api::WorkerApi(runs.clone()),
        )
        .max_decoding_message_size(16 << 20)
        .max_encoding_message_size(16 << 20),
        token::Require(Arc::new(cluster_token.clone())),
    );
    let control = tonic::codegen::InterceptedService::new(
        ControlServiceServer::new(control::ControlApi {
            fleet: fleet.clone(),
        })
        .max_decoding_message_size(1 << 30)
        .max_encoding_message_size(1 << 30),
        token::Require(Arc::new(cluster_token)),
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
        admin_token: Arc::new(admin_token),
        runs: runs.clone(),
    };
    let http = http::router(http_state).merge(runs_api::router(runs.clone()));
    let listener = tokio::net::TcpListener::bind(&cfg.http_addr).await?;
    let http_task = axum::serve(listener, http).with_graceful_shutdown(stopping());

    tracing::info!(stage = %cfg.stage_addr, control = %cfg.control_addr, http = %cfg.http_addr, "listening");
    let dispatcher = tokio::spawn(fleet.clone().run_dispatcher());
    let run_dispatcher = tokio::spawn(runs.run_dispatcher());
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
        r = run_dispatcher => r??,
    }
    Ok(())
}

async fn anyhow_of<E: Into<anyhow::Error>>(
    f: impl IntoFuture<Output = Result<(), E>>,
) -> anyhow::Result<()> {
    f.await.map_err(Into::into)
}
