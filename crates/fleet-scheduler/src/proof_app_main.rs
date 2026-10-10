use clap::Parser;
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    fleet_scheduler::proof_app::serve(fleet_scheduler::proof_app::Args::parse()).await
}
