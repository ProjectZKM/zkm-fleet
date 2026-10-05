use std::path::PathBuf;

use clap::Parser;

#[derive(Parser)]
#[command(about = "zkm-fleet scheduler: stage API, node control, dispatch")]
struct Args {
    /// Scheduler configuration (YAML).
    #[arg(short, long, env = "FLEET_CONFIG", default_value = "fleet.yaml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();
    let cfg = fleet_scheduler::config::Config::load(&args.config)?;
    fleet_scheduler::serve(cfg).await
}
