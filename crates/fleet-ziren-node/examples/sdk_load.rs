//! Concurrent SDK clients: `sdk_load <elf> <jobs> [compressed|groth16]`, env from a fleetctl env file.
//! Prints one line per job (index, outcome, seconds) and a summary.

use std::sync::Arc;
use std::time::{Duration, Instant};

use zkm_sdk::{NetworkProver, ZKMProofKind, ZKMStdin};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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
                println!("job {i:>3} ok   {secs:>7.2}s");
            }
            Err(e) => {
                failed += 1;
                println!(
                    "job {i:>3} FAIL {secs:>7.2}s {}",
                    e.chars().take(120).collect::<String>()
                );
            }
        }
    }
    println!(
        "LOAD {jobs} jobs: {ok} ok, {failed} failed, {:.1}s total",
        t0.elapsed().as_secs_f64()
    );
    Ok(())
}
