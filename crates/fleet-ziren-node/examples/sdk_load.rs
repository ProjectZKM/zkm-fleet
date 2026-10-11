//! Concurrent SDK clients: `sdk_load <elf> <jobs> [compressed|groth16]`, env from a fleetctl env file.
//! Logs each job's outcome and elapsed time through tracing.

use std::sync::Arc;
use std::time::{Duration, Instant};

use zkm_sdk::{NetworkProver, ZKMProofKind, ZKMStdin};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into())).init();
    let a: Vec<String> = std::env::args().collect();
    anyhow::ensure!(
        a.len() >= 3,
        "usage: sdk_load <elf> <jobs> [compressed|groth16]"
    );
    let elf = Arc::new(std::fs::read(&a[1])?);
    let jobs: u32 = a[2].parse()?;
    let kind = if a.get(3).map(String::as_str) == Some("groth16") {
        ZKMProofKind::Groth16
    } else {
        ZKMProofKind::Compressed
    };
    let prover = Arc::new(NetworkProver::from_env()?);
    let t0 = Instant::now();
    let mut handles = Vec::new();
    for i in 0..jobs {
        let (prover, elf) = (prover.clone(), elf.clone());
        handles.push(tokio::spawn(async move {
            let t = Instant::now();
            let mut stdin = ZKMStdin::new();
            stdin.write(&(10 + i));
            let r = prover
                .prove_with_cycles(&elf, stdin, kind, None, Some(Duration::from_secs(900)))
                .await;
            (
                i,
                r.map(|_| ()).map_err(|e| format!("{e:#}")),
                t.elapsed().as_secs_f64(),
            )
        }));
    }
    let (mut ok, mut failed) = (0, 0);
    for h in handles {
        let (i, r, secs) = h.await?;
        match r {
            Ok(()) => {
                ok += 1;
                tracing::info!(job=i, elapsed_secs=secs, "proof completed");
            }
            Err(e) => {
                failed += 1;
                tracing::error!(job=i, elapsed_secs=secs, error=%e, "proof failed");
            }
        }
    }
    tracing::info!(jobs, ok, failed, elapsed_secs=t0.elapsed().as_secs_f64(), "load completed");
    Ok(())
}
