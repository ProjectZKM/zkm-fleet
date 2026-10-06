pub mod auth;
pub mod blobs;
pub mod config;
pub mod control;
pub mod fleet;
pub mod http;
pub mod placement;
pub mod stage;
pub mod store;
pub mod token;

use std::future::IntoFuture;
use std::sync::Arc;

use tokio::sync::watch;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};

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
    let store: Arc<dyn store::Store> = match &cfg.database_url {
        Some(url) => Arc::new(store::PgStore::connect(url).await?),
        None => {
            tracing::warn!("no database_url: jobs and credentials are kept in memory only");
            Arc::new(store::MemoryStore::default())
        }
    };
    let blobs = blobs::Blobs::new(&cfg.data_dir)?;
    let fleet = fleet::Fleet::new(cfg.clone(), store, blobs, cluster_token.clone());
    let creds = fleet.load_credentials().await?;
    let disabled = fleet.load_apps().await?;
    let recovered = fleet.recover().await?;
    tracing::info!(
        credentials = creds,
        disabled_apps = disabled,
        recovered_jobs = recovered,
        "scheduler state loaded"
    );

    let mut stage_server = Server::builder();
    if let Some(tls) = &cfg.tls {
        let identity = Identity::from_pem(
            std::fs::read(&tls.cert_path)?,
            std::fs::read(&tls.key_path)?,
        );
        let mut t = ServerTlsConfig::new().identity(identity);
        if let Some(ca) = &tls.client_ca_path {
            t = t.client_ca_root(Certificate::from_pem(std::fs::read(ca)?));
        }
        stage_server = stage_server.tls_config(t)?;
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

    let control = ControlServiceServer::with_interceptor(
        control::ControlApi {
            fleet: fleet.clone(),
        },
        token::Require(Arc::new(cluster_token)),
    );
    let control_addr = cfg.control_addr.parse()?;
    let control_task = Server::builder()
        .add_service(control)
        .serve_with_shutdown(control_addr, stopping());

    let http = http::router(http::HttpState {
        fleet: fleet.clone(),
        admin_token: Arc::new(admin_token),
    });
    let listener = tokio::net::TcpListener::bind(&cfg.http_addr).await?;
    let http_task = axum::serve(listener, http).with_graceful_shutdown(stopping());

    tracing::info!(stage = %cfg.stage_addr, control = %cfg.control_addr, http = %cfg.http_addr, "listening");
    let dispatcher = tokio::spawn(fleet.clone().run_dispatcher());
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
    }
    Ok(())
}

async fn anyhow_of<E: Into<anyhow::Error>>(
    f: impl IntoFuture<Output = Result<(), E>>,
) -> anyhow::Result<()> {
    f.await.map_err(Into::into)
}
