//! Durable control stream for independently deployed data routers. Fleet never
//! proxies application bytes. An enrolled router can only read and acknowledge
//! its own stream; mutations require the existing administrator permission.
use crate::{fleet::now_ms, http::HttpState, store::Token};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Extension, Json, Router,
};
use fleet_proto::{
    model::v1::{RouterControl, RouterEnrollment, RouterSyncRequest, RouterSyncResponse},
    models::{valid_route_id, valid_router_id, MAX_ROUTER_REVOCATIONS},
};

pub fn router(state: HttpState) -> Router {
    Router::new()
        .route("/routers/:id", get(status).put(enroll))
        .route("/routers/:id/sync", post(sync))
        .route("/routers/:id/routes/:route", delete(revoke))
        .with_state(state)
}

type Result<T> = std::result::Result<T, StatusCode>;
fn unavailable(_: anyhow::Error) -> StatusCode {
    StatusCode::SERVICE_UNAVAILABLE
}

async fn status(State(s): State<HttpState>, Path(id): Path<String>) -> Result<Json<RouterControl>> {
    Ok(Json(
        s.fleet
            .store
            .router_control(&id)
            .await
            .map_err(unavailable)?
            .ok_or(StatusCode::NOT_FOUND)?,
    ))
}

async fn enroll(
    State(s): State<HttpState>,
    Path(id): Path<String>,
    Json(request): Json<RouterEnrollment>,
) -> Result<Response> {
    if !valid_router_id(&id) {
        return Err(StatusCode::BAD_REQUEST);
    }
    let token = s
        .fleet
        .tokens()
        .into_iter()
        .find(|t| {
            t.id == request.token_id
                && t.revoked_ms.is_none()
                && t.permissions == ["router_sync"]
                && t.token_hash.is_some()
        })
        .ok_or(StatusCode::BAD_REQUEST)?;
    let namespace = token.namespace.ok_or(StatusCode::BAD_REQUEST)?;
    for _ in 0..16 {
        let previous = s
            .fleet
            .store
            .router_control(&id)
            .await
            .map_err(unavailable)?;
        let mut next = previous.clone().unwrap_or_else(|| RouterControl {
            router_id: id.clone(),
            issuer: request.issuer.clone(),
            token_id: request.token_id.clone(),
            namespace: namespace.clone(),
            epoch: uuid::Uuid::new_v4().simple().to_string(),
            ..Default::default()
        });
        // Credential rotation preserves the epoch and all tombstones. Authority
        // changes require a distinct router identity, never a silent reset.
        if next.issuer != request.issuer || next.namespace != namespace {
            return Err(StatusCode::CONFLICT);
        }
        next.token_id = request.token_id.clone();
        next.validate().map_err(|_| StatusCode::BAD_REQUEST)?;
        if s.fleet
            .store
            .save_router_control(previous.as_ref(), &next)
            .await
            .map_err(unavailable)?
        {
            return Ok((
                if previous.is_some() {
                    StatusCode::OK
                } else {
                    StatusCode::CREATED
                },
                Json(next),
            )
                .into_response());
        }
    }
    Err(StatusCode::SERVICE_UNAVAILABLE)
}

async fn revoke(
    State(s): State<HttpState>,
    Path((id, route)): Path<(String, String)>,
) -> Result<Json<RouterControl>> {
    if !valid_route_id(&route) {
        return Err(StatusCode::BAD_REQUEST);
    }
    for _ in 0..16 {
        let previous = s
            .fleet
            .store
            .router_control(&id)
            .await
            .map_err(unavailable)?
            .ok_or(StatusCode::NOT_FOUND)?;
        if previous.revoked_routes.contains(&route) {
            return Ok(Json(previous));
        }
        if previous.revoked_routes.len() >= MAX_ROUTER_REVOCATIONS {
            return Err(StatusCode::INSUFFICIENT_STORAGE);
        }
        let mut next = previous.clone();
        next.revoked_routes.push(route.clone());
        if s.fleet
            .store
            .save_router_control(Some(&previous), &next)
            .await
            .map_err(unavailable)?
        {
            return Ok(Json(next));
        }
    }
    Err(StatusCode::SERVICE_UNAVAILABLE)
}

async fn sync(
    State(s): State<HttpState>,
    Path(id): Path<String>,
    Extension(caller): Extension<Option<Token>>,
    Json(request): Json<RouterSyncRequest>,
) -> Result<Json<RouterSyncResponse>> {
    // Admin credentials are deliberately not accepted as a router identity.
    let caller = caller.ok_or(StatusCode::FORBIDDEN)?;
    for _ in 0..16 {
        let previous = s
            .fleet
            .store
            .router_control(&id)
            .await
            .map_err(unavailable)?
            .ok_or(StatusCode::NOT_FOUND)?;
        if caller.id != previous.token_id
            || caller.namespace.as_deref() != Some(&previous.namespace)
            || caller.permissions != ["router_sync"]
            || !s
                .fleet
                .namespace(&previous.namespace)
                .is_some_and(|n| n.suspended_ms.is_none())
        {
            return Err(StatusCode::FORBIDDEN);
        }
        let revision = previous.revoked_routes.len() as u64;
        if request.epoch.as_ref().is_some_and(|e| e != &previous.epoch)
            || request.epoch.is_none()
                && (request.after_revision != 0 || request.applied_revision.is_some())
            || request.after_revision > revision
            || request
                .applied_revision
                .is_some_and(|a| a > request.after_revision || a > previous.delivered_revision)
        {
            return Err(StatusCode::CONFLICT);
        }
        let mut next = previous.clone();
        next.delivered_revision = revision;
        if let Some(applied) = request.applied_revision {
            if previous.applied_revision.is_some_and(|r| applied < r) {
                return Err(StatusCode::CONFLICT);
            }
            next.applied_revision = Some(applied);
            next.applied_ms = Some(now_ms());
        }
        if s.fleet
            .store
            .save_router_control(Some(&previous), &next)
            .await
            .map_err(unavailable)?
        {
            return Ok(Json(RouterSyncResponse {
                router_id: id,
                issuer: next.issuer,
                epoch: next.epoch,
                from_revision: request.after_revision,
                revision,
                revoked_routes: next.revoked_routes[request.after_revision as usize..].to_vec(),
                lease_seconds: 30,
            }));
        }
    }
    Err(StatusCode::SERVICE_UNAVAILABLE)
}
