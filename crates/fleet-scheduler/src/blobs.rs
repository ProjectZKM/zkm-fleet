//! Payloads on the local filesystem: `elf/<elf_id>` and `jobs/<proof_id>/<name>`.

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

#[derive(Clone)]
pub struct Blobs {
    root: PathBuf,
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
        })
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
