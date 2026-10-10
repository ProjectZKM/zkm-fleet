//! Metadata-only admission for an app's SDK endpoint.
use crate::{http::HttpState, stage::StageApi, store::Token};
use axum::{
    extract::{Extension, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use fleet_proto::fleet::v1::ProofInputs;
use fleet_proto::stage::v1::GenerateProofRequest;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Submit {
    pub proof_id: String,
    pub elf_id: String,
    pub signature: String,
    pub block_no: Option<u64>,
    pub seg_size: u32,
    pub from_step: Option<i32>,
    pub target_step: Option<i32>,
    pub inputs: ProofInputs,
}

pub async fn submit(
    State(state): State<HttpState>,
    Extension(token): Extension<Option<Token>>,
    Json(body): Json<Submit>,
) -> Response {
    if token.is_none() {
        return (StatusCode::FORBIDDEN, "namespace token required").into_response();
    }
    let api = StageApi {
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
