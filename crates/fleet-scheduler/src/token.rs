//! Bearer authentication and SDK request signer recovery.

use std::str::FromStr;

use alloy_primitives::{Address, Signature};
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
pub struct ClusterTokenVerifier(pub std::sync::Arc<String>);

impl tonic::service::Interceptor for ClusterTokenVerifier {
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
pub struct BearerTokenInterceptor(pub MetadataValue<tonic::metadata::Ascii>);

impl BearerTokenInterceptor {
    pub fn new(token: &str) -> anyhow::Result<Self> {
        Ok(Self(format!("Bearer {token}").parse()?))
    }
}

impl tonic::service::Interceptor for BearerTokenInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        req.metadata_mut().insert(HEADER, self.0.clone());
        Ok(req)
    }
}

/// Reads a token from a file (trimmed); refuses an empty one.
pub use fleet_proto::load_token as load;

/// The message the SDK's network prover signs (EIP-191 personal message):
/// `proof_id&block_no&seg_size`, or `proof_id&seg_size` without a block number.
pub fn signed_message(proof_id: &str, block_no: Option<u64>, seg_size: u32) -> String {
    match block_no {
        Some(b) => format!("{proof_id}&{b}&{seg_size}"),
        None => format!("{proof_id}&{seg_size}"),
    }
}

/// Recovers the signer of a request signature as produced by the SDK.
pub fn recover_signer(
    signature: &str,
    proof_id: &str,
    block_no: Option<u64>,
    seg_size: u32,
) -> anyhow::Result<Address> {
    let sig = Signature::from_str(signature.trim())
        .map_err(|e| anyhow::anyhow!("malformed signature: {e}"))?;
    let msg = signed_message(proof_id, block_no, seg_size);
    sig.recover_address_from_msg(msg.as_bytes())
        .map_err(|e| anyhow::anyhow!("signature does not recover: {e}"))
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

    use alloy_primitives::eip191_hash_message;
    use k256::ecdsa::SigningKey;

    fn sign(key: &SigningKey, msg: &str) -> String {
        let hash = eip191_hash_message(msg.as_bytes());
        let (sig, recid) = key.sign_prehash_recoverable(hash.as_slice()).unwrap();
        let mut bytes = sig.to_bytes().to_vec();
        bytes.push(27 + recid.to_byte());
        hex::encode(bytes)
    }

    #[test]
    fn recovers_the_signer_of_an_sdk_style_signature() {
        let key = SigningKey::from_slice(&[7u8; 32]).unwrap();
        let expected = Address::from_public_key(key.verifying_key());
        for (block_no, msg) in [(None, "p-1&0"), (Some(42), "p-1&42&0")] {
            let sig = sign(&key, msg);
            assert_eq!(recover_signer(&sig, "p-1", block_no, 0).unwrap(), expected);
            assert_eq!(
                recover_signer(&format!("0x{sig}"), "p-1", block_no, 0).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn a_signature_over_another_request_recovers_someone_else() {
        let key = SigningKey::from_slice(&[7u8; 32]).unwrap();
        let expected = Address::from_public_key(key.verifying_key());
        let sig = sign(&key, "p-1&0");
        let got = recover_signer(&sig, "p-2", None, 0).unwrap();
        assert_ne!(got, expected);
    }

}
