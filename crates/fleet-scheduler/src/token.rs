//! Shared-secret bearer tokens for the cluster-internal APIs and operator endpoints.

use tonic::{metadata::MetadataValue, Request, Status};

pub const HEADER: &str = "authorization";

/// Constant-time comparison, so a wrong token leaks nothing through timing.
pub fn equal(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

pub fn bearer_matches(header: Option<&str>, token: &str) -> bool {
    header
        .and_then(|h| h.strip_prefix("Bearer "))
        .map_or(false, |got| equal(got.as_bytes(), token.as_bytes()))
}

/// A tonic interceptor that admits only requests carrying `Bearer <token>`.
#[derive(Clone)]
pub struct Require(pub std::sync::Arc<String>);

impl tonic::service::Interceptor for Require {
    fn call(&mut self, req: Request<()>) -> Result<Request<()>, Status> {
        let header = req.metadata().get(HEADER).and_then(|v| v.to_str().ok());
        if bearer_matches(header, &self.0) {
            Ok(req)
        } else {
            Err(Status::unauthenticated("missing or wrong cluster token"))
        }
    }
}

/// A tonic interceptor that adds `Bearer <token>` to outgoing requests.
#[derive(Clone)]
pub struct Attach(pub MetadataValue<tonic::metadata::Ascii>);

impl Attach {
    pub fn new(token: &str) -> anyhow::Result<Self> {
        Ok(Self(format!("Bearer {token}").parse()?))
    }
}

impl tonic::service::Interceptor for Attach {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        req.metadata_mut().insert(HEADER, self.0.clone());
        Ok(req)
    }
}

/// Reads a token from a file (trimmed); refuses an empty one.
pub fn load(path: &std::path::Path) -> anyhow::Result<String> {
    let t = std::fs::read_to_string(path)?.trim().to_string();
    anyhow::ensure!(
        t.len() >= 32,
        "token in {} is shorter than 32 characters",
        path.display()
    );
    Ok(t)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bearer_check() {
        assert!(bearer_matches(Some("Bearer s3cret"), "s3cret"));
        assert!(!bearer_matches(Some("Bearer s3cre"), "s3cret"));
        assert!(!bearer_matches(Some("s3cret"), "s3cret"));
        assert!(!bearer_matches(None, "s3cret"));
    }
}
