//! Proof output belongs to the worker. There is no scheduler output RPC.
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
                ..a
            })
            .is_err());
    }
}
