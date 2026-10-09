//! Payloads on the local filesystem: `elf/<elf_id>` and `jobs/<proof_id>/<name>`.

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

#[derive(Clone)]
pub struct Blobs {
    root: PathBuf,
    proof_output: Option<PathBuf>,
}

pub const STDIN: &str = "stdin.bin";
pub const RECEIPTS: &str = "receipts.bin";
pub const PROOF: &str = "proof.json";
pub const PUBLIC_VALUES: &str = "public_values.bin";

/// Ids are used as path components; accept only what the SDK produces
/// (uuids, hex digests) so a request cannot escape the data directory.
pub fn is_safe_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

pub fn elf_id_of(elf: &[u8]) -> String {
    hex::encode(Sha256::digest(elf))
}

impl Blobs {
    pub fn new(root: &Path) -> anyhow::Result<Self> {
        std::fs::create_dir_all(root.join("elf"))?;
        std::fs::create_dir_all(root.join("jobs"))?;
        Ok(Self {
            root: root.to_path_buf(),
            proof_output: None,
        })
    }

    /// Attach shared job-artifact storage. Nodes write proof output; the SDK adapter
    /// reads it, and deleting an app removes its terminal jobs' output directories.
    pub fn with_proof_output(mut self, root: PathBuf) -> Self {
        self.proof_output = Some(root);
        self
    }

    pub async fn sdk_output(&self, job: &crate::store::Job, name: &'static str) -> anyhow::Result<Vec<u8>> {
        let blobs = self.clone();
        let job = job.clone();
        // A remote mount must not block scheduling or health heartbeats.
        let bytes = tokio::time::timeout(std::time::Duration::from_secs(10),
            tokio::task::spawn_blocking(move || blobs.read_output(&job, name))).await???;
        Ok(bytes)
    }

    /// A successful worker completion is accepted only when the shared output is
    /// visible and the committed manifest matches the result metadata and bytes.
    pub async fn verify_published_output(&self, job: &crate::store::Job, cycles: u64, prove_ms: u64) -> anyhow::Result<()> {
        if self.proof_output.is_none() { return Ok(()); }
        let blobs = self.clone();
        let job = job.clone();
        tokio::time::timeout(std::time::Duration::from_secs(10), tokio::task::spawn_blocking(move || {
            let proof = blobs.read_output(&job, PROOF)?;
            let public_values = blobs.read_output(&job, PUBLIC_VALUES)?;
            let root = blobs.proof_output.as_ref().expect("checked above");
            let started = job.started_ms.ok_or_else(|| anyhow::anyhow!("successful job has no execution lease"))?;
            let path = root.join(&job.id).join(format!("{}-{started}", job.attempts)).join("manifest.json");
            let manifest: serde_json::Value = serde_json::from_slice(&std::fs::read(path)?)?;
            anyhow::ensure!(manifest["cycles"].as_u64() == Some(cycles) && manifest["prove_ms"].as_u64() == Some(prove_ms), "proof output manifest does not match completion metadata");
            let proof_hash = hex::encode(Sha256::digest(&proof));
            let public_values_hash = hex::encode(Sha256::digest(&public_values));
            anyhow::ensure!(manifest["proof_hash"].as_str() == Some(proof_hash.as_str()), "proof output hash does not match manifest");
            anyhow::ensure!(manifest["public_values_hash"].as_str() == Some(public_values_hash.as_str()), "public values hash does not match manifest");
            Ok(())
        })).await??
    }

    pub fn read_output(&self, job: &crate::store::Job, name: &str) -> anyhow::Result<Vec<u8>> {
        anyhow::ensure!(matches!(name, PROOF | PUBLIC_VALUES), "unknown proof output");
        anyhow::ensure!(is_safe_id(&job.id), "bad job id");
        if let (Some(root), Some(started)) = (&self.proof_output, job.started_ms) {
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
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)?;
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
        let b = Blobs::new(dir.path()).unwrap();
        let id = b.put_elf(b"elf bytes").unwrap();
        assert_eq!(id, elf_id_of(b"elf bytes"));
        assert!(b.has_elf(&id));
        assert_eq!(b.elf(&id).unwrap(), b"elf bytes");
        assert!(!b.has_elf("../elf"));
    }
}
