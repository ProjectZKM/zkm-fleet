//! Storage-only proof fetching. This path never initializes the prover or a GPU.
use crate::output::Output;
use clap::Parser;
use fleet_proto::{
    artifacts,
    fleet::v1::{Assignment, ProofFetch},
};
use std::{
    io::{Read, Write},
    path::PathBuf,
};

#[derive(Parser)]
struct StorageFetchArgs {
    /// Administrator-approved app storage roots, retained independently of actors.
    #[arg(long, required = true)]
    storage_root: Vec<PathBuf>,
}

pub fn run() -> anyhow::Result<()> {
    let args = StorageFetchArgs::parse_from(std::env::args().skip(1));
    let mut input = Vec::new();
    std::io::stdin().take(16 << 10).read_to_end(&mut input)?;
    let request: ProofFetch = serde_json::from_slice(&input)?;
    match read(&request, &args.storage_root) {
        Ok(bytes) => std::io::stdout().write_all(&bytes).map_err(Into::into),
        Err(e) => {
            // Machine agent forwards a bounded, fixed error class, never local paths.
            let missing = e
                .downcast_ref::<std::io::Error>()
                .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound);
            tracing::warn!(reason=if missing { "missing" } else { "invalid" }, "artifact fetch rejected");
            std::process::exit(if missing { 4 } else { 5 });
        }
    }
}

pub fn read(request: &ProofFetch, roots: &[PathBuf]) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(
        matches!(request.name.as_str(), "proof.json" | "public_values.bin"),
        "unknown proof artifact"
    );
    let reference = request
        .reference
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("output reference missing"))?;
    artifacts::validate(reference, artifacts::MAX_OUTPUT)?;
    let mut selected = None;
    for root in roots {
        match artifacts::path_in_root(&request.output_root, root) {
            Ok(output) => {
                selected = Some(output);
                break;
            }
            Err(e)
                if e.downcast_ref::<std::io::Error>()
                    .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
            {
                return Err(e)
            }
            Err(_) => {}
        }
    }
    let root = selected.ok_or_else(|| anyhow::anyhow!("output outside configured storage"))?;
    anyhow::ensure!(
        artifacts::file_uri(&root)? == request.output_root.trim_end_matches('/'),
        "output root must be canonical"
    );
    let assignment = Assignment {
        job_id: request.job_id.clone(),
        lease: request.lease.clone(),
        elf_id: request.elf_id.clone(),
        ..Default::default()
    };
    // Missing retained output is distinct from a corrupt manifest or a rejected path.
    artifacts::path_in_root(&reference.uri, &root)?;
    let output = Output::new(root.clone())?;
    let refs = output.references(&assignment)?;
    let committed = if request.name == "proof.json" {
        refs.proof.unwrap()
    } else {
        refs.public_values.unwrap()
    };
    anyhow::ensure!(
        *reference == committed,
        "artifact does not match the committed job and lease"
    );
    artifacts::read(reference, &root, artifacts::MAX_OUTPUT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fleet_proto::fleet::v1::ProveResponse;

    #[test]
    fn fetch_requires_a_committed_lease_hash_and_approved_storage_root() {
        let storage = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let output = Output::new(storage.path().join("outputs")).unwrap();
        let assignment = Assignment {
            job_id: "job".into(),
            lease: "1-23".into(),
            elf_id: "a".repeat(64),
            ..Default::default()
        };
        output
            .publish(
                &assignment,
                &ProveResponse {
                    proof_json: b"proof".to_vec(),
                    public_values: b"values".to_vec(),
                    ..Default::default()
                },
            )
            .unwrap();
        let refs = output.references(&assignment).unwrap();
        let request = ProofFetch {
            job_id: assignment.job_id.clone(),
            lease: assignment.lease.clone(),
            elf_id: assignment.elf_id.clone(),
            output_root: artifacts::file_uri(&storage.path().join("outputs")).unwrap(),
            name: "proof.json".into(),
            reference: refs.proof,
            ..Default::default()
        };
        let roots = vec![storage.path().into()];
        assert_eq!(read(&request, &roots).unwrap(), b"proof");
        assert!(read(&request, &[other.path().into()]).is_err());
        assert!(read(
            &ProofFetch {
                lease: "old-lease".into(),
                ..request.clone()
            },
            &roots
        )
        .is_err());
        assert!(read(
            &ProofFetch {
                job_id: "../escape".into(),
                ..request.clone()
            },
            &roots
        )
        .is_err());
        assert!(read(
            &ProofFetch {
                name: "manifest.json".into(),
                ..request.clone()
            },
            &roots
        )
        .is_err());
        let mut wrong = request.clone();
        wrong.reference.as_mut().unwrap().sha256 = "0".repeat(64);
        assert!(read(&wrong, &roots).is_err());
        let path = storage.path().join("outputs/job/1-23/proof.json");
        std::fs::write(&path, b"other").unwrap();
        assert!(read(&request, &roots).is_err());
        std::fs::remove_file(&path).unwrap();
        assert_eq!(
            read(&request, &roots)
                .unwrap_err()
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .kind(),
            std::io::ErrorKind::NotFound
        );
    }
}
