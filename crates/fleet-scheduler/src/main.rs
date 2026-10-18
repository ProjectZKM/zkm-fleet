use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(about = "zkm-fleet scheduler: stage API, node control, dispatch")]
struct SchedulerArgs {
    /// Scheduler configuration (YAML).
    #[arg(short, long, env = "FLEET_CONFIG", default_value = "fleet.yaml", global = true)]
    config: PathBuf,
    /// Without one, the scheduler runs.
    #[command(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(Subcommand)]
enum Cmd {
    /// Issue an administrator's token: its secret goes to --out (0600), its record to the
    /// database. More can be issued later with `fleetctl token admin`.
    IssueAdmin {
        /// What the token is for, shown by `fleetctl token`.
        #[arg(long)]
        label: String,
        /// The file for the token's secret; it must not exist.
        #[arg(long)]
        out: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = SchedulerArgs::parse();
    let cfg = fleet_scheduler::config::SchedulerConfig::load(&args.config)?;
    match args.cmd {
        None => fleet_scheduler::serve(cfg).await,
        Some(Cmd::IssueAdmin { label, out }) => {
            let token = fleet_scheduler::issue_admin_token(&cfg, &label, &out).await?;
            println!("issued administrator token {} ({label}); its secret is in {}", token.id, out.display());
            Ok(())
        }
    }
}
