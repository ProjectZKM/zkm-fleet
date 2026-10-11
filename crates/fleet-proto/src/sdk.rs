//! Metadata and step mapping shared by the SDK endpoint and admission adapter.
pub use crate::model::v1::SubmitProofRequest;
use crate::{
    fleet::v1::ProofKind,
    stage::v1::Step};

pub fn kind_for(
    from_step: Option<i32>,
    target_step: Option<i32>,
) -> Result<ProofKind, &'static str> {
    let from = from_step.unwrap_or(Step::Init as i32);
    let target = target_step.unwrap_or(Step::InSnark as i32);
    match (Step::try_from(from), Step::try_from(target)) {
        (Ok(Step::Init), Ok(Step::InAgg)) => Ok(ProofKind::Compressed),
        (Ok(Step::Init), Ok(Step::InSnark)) => Ok(ProofKind::Groth16),
        (Ok(Step::InAgg), Ok(Step::InSnark)) => Ok(ProofKind::CompressToGroth16),
        _ => Err("only Init->InAgg (compressed), Init->InSnark (groth16) and InAgg->InSnark are supported"),
    }
}

/// Both HTTP adapters expose the same retry and storage failure classes.
pub fn http_status_code(code: tonic::Code) -> u16 {
    match code {
        tonic::Code::NotFound => 404,
        tonic::Code::Unavailable => 503,
        tonic::Code::ResourceExhausted => 429,
        tonic::Code::DeadlineExceeded => 504,
        tonic::Code::DataLoss => 502,
        tonic::Code::FailedPrecondition => 409,
        _ => 500,
    }
}
