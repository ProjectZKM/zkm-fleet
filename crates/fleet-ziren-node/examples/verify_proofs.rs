//! Verifies proofs the scheduler stored: `verify_proofs <elf> <job-dir>...`, each job
//! directory holding the proof.json and public_values.bin a client was served.
//! One line per proof, then a summary. The verifier's recursion key map is chosen as
//! in the prover: VERIFY_VK (default true, the production map).

use std::path::Path;

use zkm_sdk::{ProverClient, ZKMProof, ZKMProofWithPublicValues, ZKMPublicValues};

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into())).init();
    let a: Vec<String> = std::env::args().collect();
    anyhow::ensure!(a.len() >= 3, "usage: verify_proofs <elf> <job-dir>...");
    let elf = std::fs::read(&a[1])?;
    let client = ProverClient::cpu();
    let (_pk, vk) = client.setup(&elf);
    let (mut ok, mut failed) = (0, 0);
    for dir in &a[2..] {
        let p = Path::new(dir);
        let proof: ZKMProof = serde_json::from_slice(&std::fs::read(p.join("proof.json"))?)?;
        let public_values = ZKMPublicValues::from(&std::fs::read(p.join("public_values.bin"))?);
        let proof = ZKMProofWithPublicValues {
            proof,
            public_values,
            zkm_version: client.version(),
        };
        match client.verify(&proof, &vk) {
            Ok(()) => {
                ok += 1;
                tracing::info!(dir, "proof verified");
            }
            Err(e) => {
                failed += 1;
                tracing::error!(dir, error=%format!("{e:#}"), "proof verification failed");
            }
        }
    }
    tracing::info!(ok, failed, "verification completed");
    Ok(())
}
