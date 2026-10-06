//! HTTP: public values for the SDK (`/{proof_id}/wrap/public_values.bin`), health,
//! and the admin API behind the admin token (used by `fleetctl`).

use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;

use crate::blobs::{self, is_safe_id};
use crate::fleet::Fleet;
use crate::streams::{LeaseRequest, Report, Streams};
use crate::token;

#[derive(Clone)]
pub struct HttpState {
    pub fleet: Arc<Fleet>,
    pub streams: Arc<Streams>,
    pub admin_token: Arc<String>,
}

pub fn router(state: HttpState) -> Router {
    let admin = Router::new()
        .route("/status", get(status))
        .route("/credentials", get(list_credentials).post(add_credential))
        .route(
            "/credentials/:address",
            axum::routing::delete(revoke_credential),
        )
        .route("/elfs", post(put_elf))
        .route("/jobs/:id", get(job))
        .route("/nodes/:name/drain", post(drain).delete(undrain))
        .route("/streams", get(streams_status))
        .route("/streams/:name/steps", get(stream_steps))
        .route("/streams/:name/lease", post(stream_lease))
        .route("/streams/:name/report", post(stream_report))
        .route("/streams/:name/pause", post(stream_pause).delete(stream_resume))
        .route("/streams/:name/seek", post(stream_seek))
        .layer(DefaultBodyLimit::max(512 << 20))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            require_admin,
        ));
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/:proof_id/wrap/public_values.bin", get(public_values))
        .nest("/admin", admin)
        .with_state(state)
}

async fn require_admin(
    State(s): State<HttpState>,
    headers: HeaderMap,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let header = headers.get(token::HEADER).and_then(|v| v.to_str().ok());
    if token::bearer_matches(header, &s.admin_token) {
        next.run(req).await
    } else {
        (StatusCode::UNAUTHORIZED, "missing or wrong admin token").into_response()
    }
}

fn internal(e: anyhow::Error) -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")).into_response()
}

/// Unauthenticated, as the SDK fetches it with a plain GET; the proof id is an unguessable uuid.
async fn public_values(State(s): State<HttpState>, Path(proof_id): Path<String>) -> Response {
    if !is_safe_id(&proof_id) {
        return StatusCode::NOT_FOUND.into_response();
    }
    match s.fleet.blobs.get(&proof_id, blobs::PUBLIC_VALUES) {
        Ok(bytes) => ([("content-type", "application/octet-stream")], bytes).into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn status(State(s): State<HttpState>) -> Response {
    Json(s.fleet.status()).into_response()
}

async fn list_credentials(State(s): State<HttpState>) -> Response {
    Json(s.fleet.credentials()).into_response()
}

#[derive(Deserialize)]
struct NewCredential {
    tenant: String,
    address: String,
    #[serde(default)]
    label: String,
}

async fn add_credential(State(s): State<HttpState>, Json(c): Json<NewCredential>) -> Response {
    match s
        .fleet
        .add_credential(&c.tenant, &c.address, &c.label)
        .await
    {
        Ok(cred) => {
            tracing::info!(tenant = %cred.tenant, address = %cred.address, label = %cred.label, "credential added");
            (StatusCode::CREATED, Json(cred)).into_response()
        }
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e:#}")).into_response(),
    }
}

async fn revoke_credential(State(s): State<HttpState>, Path(address): Path<String>) -> Response {
    match s.fleet.revoke_credential(&address).await {
        Ok(true) => {
            tracing::info!(%address, "credential revoked");
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => internal(e),
    }
}

async fn put_elf(State(s): State<HttpState>, body: Bytes) -> Response {
    if body.is_empty() {
        return (StatusCode::BAD_REQUEST, "empty body").into_response();
    }
    match s.fleet.blobs.put_elf(&body) {
        Ok(id) => Json(serde_json::json!({ "elf_id": id, "bytes": body.len() })).into_response(),
        Err(e) => internal(e),
    }
}

async fn job(State(s): State<HttpState>, Path(id): Path<String>) -> Response {
    match s.fleet.job(&id).await {
        Ok(Some(j)) => Json(serde_json::json!({
            "id": j.id, "tenant": j.tenant, "elf_id": j.elf_id, "kind": format!("{:?}", j.kind),
            "state": format!("{:?}", j.state), "error": j.error, "node": j.node, "attempts": j.attempts,
            "failed_on": j.failed_on, "cycles": j.cycles, "prove_ms": j.prove_ms,
            "created_ms": j.created_ms, "started_ms": j.started_ms, "finished_ms": j.finished_ms,
        }))
        .into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => internal(e),
    }
}

async fn drain(State(s): State<HttpState>, Path(name): Path<String>) -> Response {
    if s.fleet.set_drain(&name, true) {
        StatusCode::NO_CONTENT.into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            "no such node (the drain is kept for when it registers)",
        )
            .into_response()
    }
}

async fn undrain(State(s): State<HttpState>, Path(name): Path<String>) -> Response {
    s.fleet.set_drain(&name, false);
    StatusCode::NO_CONTENT.into_response()
}

// ------------------------------------------------------------------ streams

async fn streams_status(State(s): State<HttpState>) -> Response {
    Json(s.streams.status()).into_response()
}

#[derive(Deserialize)]
struct StepsQuery {
    #[serde(default = "default_steps_limit")]
    limit: usize,
    /// From the store rather than memory (every step ever, newest first).
    #[serde(default)]
    all: bool,
}

fn default_steps_limit() -> usize {
    50
}

async fn stream_steps(
    State(s): State<HttpState>,
    Path(name): Path<String>,
    axum::extract::Query(q): axum::extract::Query<StepsQuery>,
) -> Response {
    if q.all {
        return match s.streams.stored_steps(&name, q.limit).await {
            Ok(v) => Json(v).into_response(),
            Err(e) => internal(e),
        };
    }
    match s.streams.steps(&name, q.limit) {
        Some(v) => Json(v).into_response(),
        None => (StatusCode::NOT_FOUND, "no such stream").into_response(),
    }
}

async fn stream_lease(State(s): State<HttpState>, Path(name): Path<String>, Json(req): Json<LeaseRequest>) -> Response {
    match s.streams.lease(&name, &req).await {
        Ok(lease) => Json(lease).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, format!("{e:#}")).into_response(),
    }
}

async fn stream_report(State(s): State<HttpState>, Path(name): Path<String>, Json(r): Json<Report>) -> Response {
    match s.streams.report(&name, &r).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::CONFLICT, format!("{e:#}")).into_response(),
    }
}

async fn stream_pause(State(s): State<HttpState>, Path(name): Path<String>) -> Response {
    match s.streams.set_paused(&name, true).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::NOT_FOUND, format!("{e:#}")).into_response(),
    }
}

async fn stream_resume(State(s): State<HttpState>, Path(name): Path<String>) -> Response {
    match s.streams.set_paused(&name, false).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::NOT_FOUND, format!("{e:#}")).into_response(),
    }
}

#[derive(Deserialize)]
struct Seek {
    seq: i64,
}

async fn stream_seek(State(s): State<HttpState>, Path(name): Path<String>, Json(q): Json<Seek>) -> Response {
    match s.streams.seek(&name, q.seq).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::NOT_FOUND, format!("{e:#}")).into_response(),
    }
}
