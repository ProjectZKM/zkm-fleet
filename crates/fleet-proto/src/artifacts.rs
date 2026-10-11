//! App-owned filesystem artifacts. The scheduler validates metadata only.
use crate::fleet::v1::{ArtifactRef, ProofInputs, ProofOutputs};
use sha2::{Digest, Sha256};
use std::io::Read;
use std::path::{Path, PathBuf};

pub const MAX_INPUT: u64 = 1 << 30;
pub const MAX_OUTPUT: u64 = 64 << 20;

/// Execution identifiers are also filesystem components on the control host and nodes.
pub fn is_safe_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

pub fn validate(reference: &ArtifactRef, limit: u64) -> anyhow::Result<()> {
    anyhow::ensure!(
        reference.uri.len() <= 4096 && reference.uri.starts_with("file:///"),
        "artifact must reference app-owned filesystem storage"
    );
    anyhow::ensure!(
        reference.sha256.len() == 64
            && reference
                .sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "invalid artifact hash"
    );
    anyhow::ensure!(reference.size <= limit, "artifact exceeds size limit");
    Ok(())
}

pub fn validate_inputs(inputs: &ProofInputs, elf_id: &str) -> anyhow::Result<()> {
    let input = inputs
        .input
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("input reference missing"))?;
    let elf = inputs
        .elf
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("ELF reference missing"))?;
    validate(input, MAX_INPUT)?;
    validate(elf, MAX_INPUT)?;
    anyhow::ensure!(elf.sha256 == elf_id, "ELF hash does not match app");
    anyhow::ensure!(
        inputs.output_root.len() <= 4096 && inputs.output_root.starts_with("file:///"),
        "output storage reference missing"
    );
    Ok(())
}

pub fn validate_outputs(outputs: &ProofOutputs) -> anyhow::Result<()> {
    anyhow::ensure!(
        outputs.proof.as_ref().is_some_and(|p| p.size > 0),
        "proof output is empty"
    );
    validate(
        outputs
            .proof
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("proof reference missing"))?,
        MAX_OUTPUT,
    )?;
    validate(
        outputs
            .public_values
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("public values reference missing"))?,
        MAX_OUTPUT,
    )
}

pub fn file_uri(path: &Path) -> anyhow::Result<String> {
    anyhow::ensure!(path.is_absolute(), "storage path must be absolute");
    Ok(format!(
        "file://{}",
        path.to_str()
            .ok_or_else(|| anyhow::anyhow!("storage path is not UTF-8"))?
    ))
}

pub fn path_in_root(uri: &str, root: &Path) -> anyhow::Result<PathBuf> {
    let path = Path::new(
        uri.strip_prefix("file://")
            .ok_or_else(|| anyhow::anyhow!("unsupported storage URI"))?,
    );
    anyhow::ensure!(path.is_absolute(), "storage path must be absolute");
    let path = path.canonicalize()?;
    anyhow::ensure!(
        path.starts_with(root.canonicalize()?),
        "artifact outside configured app storage"
    );
    Ok(path)
}

pub fn read(reference: &ArtifactRef, root: &Path, limit: u64) -> anyhow::Result<Vec<u8>> {
    validate(reference, limit)?;
    let path = path_in_root(&reference.uri, root)?;
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(limit + 1)
        .read_to_end(&mut bytes)?;
    anyhow::ensure!(
        bytes.len() as u64 == reference.size,
        "artifact size mismatch"
    );
    anyhow::ensure!(hash(&bytes) == reference.sha256, "artifact hash mismatch");
    Ok(bytes)
}

pub fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub fn reference(path: &Path, bytes: &[u8]) -> anyhow::Result<ArtifactRef> {
    Ok(ArtifactRef {
        uri: file_uri(path)?,
        sha256: hash(bytes),
        size: bytes.len() as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_are_bounded_hashed_and_confined_to_app_storage() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let path = root.path().join("input");
        std::fs::write(&path, b"input").unwrap();
        let original = reference(&path, b"input").unwrap();
        assert_eq!(read(&original, root.path(), 5).unwrap(), b"input");
        assert!(read(&original, root.path(), 4).is_err());
        let mut changed = original.clone();
        changed.size = 4;
        assert!(read(&changed, root.path(), 5).is_err());
        std::fs::write(&path, b"other").unwrap();
        assert!(read(&original, root.path(), 5).is_err());
        let outside_path = outside.path().join("secret");
        std::fs::write(&outside_path, b"secret").unwrap();
        std::os::unix::fs::symlink(&outside_path, root.path().join("link")).unwrap();
        let reference = reference(&root.path().join("link"), b"secret").unwrap();
        assert!(read(&reference, root.path(), MAX_INPUT).is_err());
        assert!(path_in_root("https://example.invalid/input", root.path()).is_err());
    }
}
