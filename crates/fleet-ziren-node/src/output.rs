//! Durable proof output under a job's app-storage output root, reported by reference.
use std::io::Write;
use std::path::{Path, PathBuf};

use fleet_proto::fleet::v1::{Assignment, ProveResponse};
use sha2::{Digest, Sha256};

#[derive(Clone)]
pub struct Output(PathBuf);

impl Output {
    pub fn new(root: PathBuf) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&root)?;
        Ok(Self(root))
    }

    fn dir(&self, assignment: &Assignment) -> anyhow::Result<PathBuf> {
        for id in [&assignment.job_id, &assignment.lease] {
            anyhow::ensure!(
                !id.is_empty()
                    && id.len() <= 128
                    && id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
                "invalid output identifier"
            );
        }
        Ok(self.0.join(&assignment.job_id).join(&assignment.lease))
    }

    /// The manifest is committed last. Reconnecting or restarting a worker can
    /// report this execution's completion without proving the same job again.
    pub fn completed(&self, assignment: &Assignment) -> anyhow::Result<Option<(u64, u64)>> {
        let dir = self.dir(assignment)?;
        let bytes = match std::fs::read(dir.join("manifest.json")) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let manifest: serde_json::Value = serde_json::from_slice(&bytes)?;
        anyhow::ensure!(
            dir.join("proof.json").is_file() && dir.join("public_values.bin").is_file(),
            "incomplete proof output"
        );
        Ok(Some((
            manifest["cycles"]
                .as_u64()
                .ok_or_else(|| anyhow::anyhow!("missing cycles"))?,
            manifest["prove_ms"]
                .as_u64()
                .ok_or_else(|| anyhow::anyhow!("missing prove_ms"))?,
        )))
    }

    pub fn publish(&self, assignment: &Assignment, result: &ProveResponse) -> anyhow::Result<()> {
        anyhow::ensure!(!result.proof_json.is_empty() && result.proof_json.len() as u64 <= fleet_proto::artifacts::MAX_OUTPUT
            && result.public_values.len() as u64 <= fleet_proto::artifacts::MAX_OUTPUT, "invalid proof output size");
        let dir = self.dir(assignment)?;
        let manifest = serde_json::json!({ "cycles": result.cycles, "prove_ms": result.prove_ms,
            "wall_ms": result.wall_ms, "zkm_version": result.zkm_version,
            "proof_hash": format!("{:x}", Sha256::digest(&result.proof_json)),
            "public_values_hash": format!("{:x}", Sha256::digest(&result.public_values)) });
        if self.completed(assignment)?.is_some() {
            let existing: serde_json::Value =
                serde_json::from_slice(&std::fs::read(dir.join("manifest.json"))?)?;
            anyhow::ensure!(existing == manifest, "proof output is immutable");
            return Ok(());
        }
        std::fs::create_dir_all(&dir)?;
        atomic_write(&dir.join("proof.json"), &result.proof_json)?;
        atomic_write(&dir.join("public_values.bin"), &result.public_values)?;
        atomic_write(&dir.join("manifest.json"), &serde_json::to_vec(&manifest)?)?;
        // Persist directory entries as well as file contents before reporting success.
        std::fs::File::open(dir.parent().unwrap())?.sync_all()?;
        std::fs::File::open(&self.0)?.sync_all()?;
        Ok(())
    }

    /// Completion metadata names committed files; no proof bytes go to Fleet.
    pub fn references(&self, assignment: &Assignment) -> anyhow::Result<fleet_proto::fleet::v1::ProofOutputs> {
        anyhow::ensure!(self.completed(assignment)?.is_some(), "proof output is not committed");
        let dir = self.dir(assignment)?;
        let manifest: serde_json::Value = serde_json::from_slice(&std::fs::read(dir.join("manifest.json"))?)?;
        let reference = |name: &str, key: &str| -> anyhow::Result<_> {
            let path = dir.join(name);
            Ok(fleet_proto::fleet::v1::ArtifactRef {
                uri: fleet_proto::artifacts::file_uri(&path)?,
                sha256: manifest[key].as_str().ok_or_else(||anyhow::anyhow!("missing output hash"))?.into(),
                size: std::fs::metadata(path)?.len(),
            })
        };
        let outputs = fleet_proto::fleet::v1::ProofOutputs {
            machine: String::new(), // The scheduler fills this from authenticated registration.
            proof: Some(reference("proof.json","proof_hash")?),
            public_values: Some(reference("public_values.bin","public_values_hash")?),
        };
        fleet_proto::artifacts::validate_outputs(&outputs)?;
        for reference in [outputs.proof.as_ref().unwrap(), outputs.public_values.as_ref().unwrap()] {
            fleet_proto::artifacts::read(reference, &self.0, fleet_proto::artifacts::MAX_OUTPUT)?;
        }
        Ok(outputs)
    }
}

fn atomic_write(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let tmp = path.with_extension("pending");
    let mut file = std::fs::File::create(&tmp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    std::fs::rename(&tmp, path)?;
    std::fs::File::open(path.parent().unwrap())?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_output_is_fenced_and_survives_worker_restart() {
        let temp = tempfile::tempdir().unwrap();
        let a = Assignment {
            job_id: "job".into(),
            lease: "1-42".into(),
            ..Default::default()
        };
        let out = Output::new(temp.path().into()).unwrap();
        assert_eq!(out.completed(&a).unwrap(), None);
        let result = ProveResponse {
            proof_json: b"proof".to_vec(),
            public_values: vec![1, 2],
            cycles: 8,
            prove_ms: 3,
            ..Default::default()
        };
        out.publish(&a, &result).unwrap();
        out.publish(&a, &result).unwrap();
        let refs = out.references(&a).unwrap();
        assert_eq!(refs.proof.as_ref().unwrap().sha256, fleet_proto::artifacts::hash(b"proof"));
        assert_eq!(refs.proof.as_ref().unwrap().size,5);
        let restarted = Output::new(temp.path().into()).unwrap();
        assert_eq!(restarted.completed(&a).unwrap(), Some((8, 3)));
        assert!(restarted
            .publish(
                &a,
                &ProveResponse {
                    proof_json: vec![0],
                    ..result
                }
            )
            .is_err());
        assert_eq!(
            restarted
                .completed(&Assignment {
                    lease: "2-43".into(),
                    ..a.clone()
                })
                .unwrap(),
            None
        );
        assert!(out
            .completed(&Assignment {
                job_id: "../escape".into(),
                ..a.clone()
            })
            .is_err());
        std::fs::write(temp.path().join("job/1-42/proof.json"),b"corrupted").unwrap();
        assert!(out.references(&a).is_err(),"corrupt stored bytes cannot be reported as metadata");
    }
}
