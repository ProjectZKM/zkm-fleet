//! Payloads on the local filesystem: `elf/<elf_id>` and `jobs/<proof_id>/<name>`.

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

#[derive(Clone)]
pub struct BlobStore {
    root: PathBuf,
    proof_output: Option<PathBuf>,
}

pub const STDIN: &str = "stdin.bin";
pub const RECEIPTS: &str = "receipts.bin";
pub const PROOF: &str = "proof.json";
pub const PUBLIC_VALUES: &str = "public_values.bin";

/// Ids are used as path components; accept only what the SDK produces
/// (uuids, hex digests) so a request cannot escape the data directory.
pub use fleet_proto::artifacts::is_safe_id;

pub fn elf_id_of(elf: &[u8]) -> String {
    hex::encode(Sha256::digest(elf))
}

impl BlobStore {
    pub fn new(root: &Path) -> anyhow::Result<Self> {
        std::fs::create_dir_all(root.join("elf"))?;
        std::fs::create_dir_all(root.join("jobs"))?;
        Ok(Self {
            root: root.to_path_buf(),
            proof_output: None,
        })
    }

    /// Choose control-host proof storage for node uploads and SDK downloads.
    /// Deleting an app removes its terminal jobs' output directories.
    pub fn with_proof_output(mut self, root: PathBuf) -> Self {
        self.proof_output = Some(root);
        self
    }

    pub async fn sdk_output(&self, job: &crate::store::ProofJob, name: &'static str) -> anyhow::Result<Vec<u8>> {
        let blobs = self.clone();
        let job = job.clone();
        // A remote mount must not block scheduling or health heartbeats.
        let bytes = tokio::time::timeout(std::time::Duration::from_secs(10),
            tokio::task::spawn_blocking(move || blobs.read_output(&job, name))).await???;
        Ok(bytes)
    }

    pub fn publish_output(&self, job: &crate::store::ProofJob, result: &fleet_proto::fleet::v1::ProveResponse) -> anyhow::Result<()> {
        anyhow::ensure!(is_safe_id(&job.id), "bad job id");
        let started = job.started_ms.ok_or_else(|| anyhow::anyhow!("missing execution lease"))?;
        let root = self.proof_output.clone().unwrap_or_else(|| self.root.join("proofs"));
        let dir = root.join(&job.id).join(format!("{}-{started}", job.attempts));
        std::fs::create_dir_all(&dir)?;
        let manifest = serde_json::json!({"cycles":result.cycles,"prove_ms":result.prove_ms,
            "wall_ms":result.wall_ms,"zkm_version":result.zkm_version,
            "proof_hash":hex::encode(Sha256::digest(&result.proof_json)),
            "public_values_hash":hex::encode(Sha256::digest(&result.public_values))});
        if dir.join("manifest.json").exists() {
            let saved: serde_json::Value = serde_json::from_slice(&std::fs::read(dir.join("manifest.json"))?)?;
            anyhow::ensure!(saved == manifest && std::fs::read(dir.join(PROOF))? == result.proof_json
                && std::fs::read(dir.join(PUBLIC_VALUES))? == result.public_values, "proof output is immutable");
            return Ok(());
        }
        write_atomic(&dir.join(PROOF), &result.proof_json)?;
        write_atomic(&dir.join(PUBLIC_VALUES), &result.public_values)?;
        write_atomic(&dir.join("manifest.json"), &serde_json::to_vec(&manifest)?)?;
        std::fs::File::open(dir.parent().unwrap())?.sync_all()?;
        std::fs::File::open(root)?.sync_all()?;
        Ok(())
    }

    pub fn read_output(&self, job: &crate::store::ProofJob, name: &str) -> anyhow::Result<Vec<u8>> {
        anyhow::ensure!(matches!(name, PROOF | PUBLIC_VALUES), "unknown proof output");
        anyhow::ensure!(is_safe_id(&job.id), "bad job id");
        if let Some(started) = job.started_ms {
            let root = self.proof_output.clone().unwrap_or_else(|| self.root.join("proofs"));
            let path = root.join(&job.id).join(format!("{}-{started}", job.attempts)).join(name);
            match std::fs::read(path) {
                Ok(bytes) => return Ok(bytes),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {},
                Err(e) => return Err(e.into()),
            }
        }
        // Preserve access to proofs completed before node-owned output was enabled.
        self.get(&job.id, name)
    }

    pub fn put_elf(&self, elf: &[u8]) -> anyhow::Result<String> {
        let id = elf_id_of(elf);
        let path = self.root.join("elf").join(&id);
        if !path.exists() {
            write_atomic(&path, elf)?;
        }
        Ok(id)
    }

    pub fn has_elf(&self, elf_id: &str) -> bool {
        is_safe_id(elf_id) && self.root.join("elf").join(elf_id).exists()
    }

    pub fn elf(&self, elf_id: &str) -> anyhow::Result<Vec<u8>> {
        anyhow::ensure!(is_safe_id(elf_id), "bad elf id");
        Ok(std::fs::read(self.root.join("elf").join(elf_id))?)
    }

    fn job_dir(&self, job_id: &str) -> anyhow::Result<PathBuf> {
        anyhow::ensure!(is_safe_id(job_id), "bad job id");
        Ok(self.root.join("jobs").join(job_id))
    }

    pub fn put(&self, job_id: &str, name: &str, bytes: &[u8]) -> anyhow::Result<()> {
        let dir = self.job_dir(job_id)?;
        std::fs::create_dir_all(&dir)?;
        write_atomic(&dir.join(name), bytes)
    }

    pub fn get(&self, job_id: &str, name: &str) -> anyhow::Result<Vec<u8>> {
        Ok(std::fs::read(self.job_dir(job_id)?.join(name))?)
    }

    pub fn path(&self, job_id: &str, name: &str) -> anyhow::Result<PathBuf> {
        Ok(self.job_dir(job_id)?.join(name))
    }

    /// Removes all job artifacts: scheduler-side inputs, node-written outputs and
    /// any legacy SDK results still kept beside the inputs.
    pub fn remove_job(&self, job_id: &str) -> anyhow::Result<()> {
        let dir = self.job_dir(job_id)?;
        if dir.exists() {
            std::fs::remove_dir_all(dir)?;
        }
        if let Some(root) = &self.proof_output {
            let output = root.join(job_id);
            if output.exists() {
                std::fs::remove_dir_all(output)?;
            }
        }
        Ok(())
    }

    /// Removes a stored ELF; false when there was none.
    pub fn remove_elf(&self, elf_id: &str) -> anyhow::Result<bool> {
        anyhow::ensure!(is_safe_id(elf_id), "bad elf id");
        let path = self.root.join("elf").join(elf_id);
        if !path.exists() {
            return Ok(false);
        }
        std::fs::remove_file(path)?;
        Ok(true)
    }
}

fn write_atomic(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let tmp = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
    use std::io::Write;
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
    fn ids_cannot_escape_the_data_dir() {
        assert!(is_safe_id("3fa85f64-5717-4562-b3fc-2c963f66afa6"));
        assert!(is_safe_id(&"ab".repeat(32)));
        for bad in ["", "..", "../x", "a/b", "a b", "x.bin"] {
            assert!(!is_safe_id(bad), "{bad}");
        }
    }

    #[test]
    fn elf_round_trip_is_content_addressed() {
        let dir = tempfile::tempdir().unwrap();
        let b = BlobStore::new(dir.path()).unwrap();
        let id = b.put_elf(b"elf bytes").unwrap();
        assert_eq!(id, elf_id_of(b"elf bytes"));
        assert!(b.has_elf(&id));
        assert_eq!(b.elf(&id).unwrap(), b"elf bytes");
        assert!(!b.has_elf("../elf"));
    }
}
