//! Metadata-only admission for an app's SDK endpoint.
use crate::{http::HttpState, stage::ProofStageService, store::Token};
use axum::{
    extract::{Extension, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use fleet_proto::stage::v1::GenerateProofRequest;
pub use fleet_proto::sdk::SubmitProofRequest;

pub async fn submit(
    State(state): State<HttpState>,
    Extension(token): Extension<Option<Token>>,
    Json(body): Json<SubmitProofRequest>,
) -> Response {
    if token.is_none() {
        return (StatusCode::FORBIDDEN, "namespace token required").into_response();
    }
    let api = ProofStageService {
        fleet: state.fleet.clone(),
    };
    // Reuse the proof caller checks, including SDK signer binding, after HTTP auth.
    let token = token.unwrap();
    let caller = crate::stage::ProofCaller::from_token(token);
    let response = api
        .generate_metadata(
            GenerateProofRequest {
                proof_id: body.proof_id,
                elf_id: Some(body.elf_id),
                signature: body.signature,
                block_no: body.block_no,
                seg_size: body.seg_size,
                from_step: body.from_step,
                target_step: body.target_step,
                ..Default::default()
            },
            Some(caller),
            body.inputs,
        )
        .await;
    Json(serde_json::json!({"proof_id":response.proof_id,"status":response.status,"error_message":response.error_message})).into_response()
}
