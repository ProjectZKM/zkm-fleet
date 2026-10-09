//! Submits a guest through the Ziren SDK's network prover, exactly as EthProofs and
//! GOAT do: `cargo run --example sdk_submit -- <elf> <u32 input> [compressed|groth16] [--elf-id]`.
//! Configure it with the env file `fleetctl token issue` writes.

use std::time::{Duration, Instant};

use zkm_sdk::{NetworkProver, ZKMProofKind, ZKMStdin};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    anyhow::ensure!(
        args.len() >= 3,
        "usage: sdk_submit <elf> <u32 input> [compressed|groth16] [--elf-id]"
    );
    let elf = std::fs::read(&args[1])?;
    let n: u32 = args[2].parse()?;
    let kind = match args.get(3).map(String::as_str) {
        Some("groth16") => ZKMProofKind::Groth16,
        _ => ZKMProofKind::Compressed,
    };
    let by_id = args.iter().any(|a| a == "--elf-id");
    let elf_id = by_id.then(|| {
        use sha2::Digest;
        hex::encode(sha2::Sha256::digest(&elf))
    });
    let mut stdin = ZKMStdin::new();
    stdin.write(&n);
    let prover = NetworkProver::from_env()?;
    let t = Instant::now();
    let (proof, cycles) = prover
        .prove_with_cycles(&elf, stdin, kind, elf_id, Some(Duration::from_secs(3600)))
        .await?;
    println!(
        "PROOF_RECEIVED kind={kind:?} cycles={cycles} public_values={} bytes, elapsed={:.2}s, variant={:?}",
        proof.public_values.as_slice().len(),
        t.elapsed().as_secs_f64(),
        std::mem::discriminant(&proof.proof)
    );
    Ok(())
}
