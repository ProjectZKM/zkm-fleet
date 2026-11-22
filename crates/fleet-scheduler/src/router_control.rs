//! Durable control stream for independently deployed data routers. Fleet never
//! proxies application bytes. An enrolled router can only read and acknowledge
//! its own stream. Admins own policies; namespace delegates may revoke subjects.
use crate::{fleet::now_ms, http::HttpState, store::Token};
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Extension, Json, Router,
};
use fleet_proto::{
    model::v1::{
        RouterAccessBinding, RouterAccessPolicy, RouterAuthorization, RouterAuthorizeRequest,
        RouterControl, RouterEnrollment, RouterSubjectRevocation, RouterSyncRequest,
        RouterSyncResponse,
    },
    models::{
        valid_route_id, valid_router_id, MAX_ROUTER_REVOCATIONS, ROUTER_CAPABILITIES_HEADER,
        ROUTER_WORKLOAD_DEFAULTS,
    },
};

pub fn router(state: HttpState) -> Router {
    Router::new()
        .route("/routers/:id", get(status).put(enroll))
        .route("/routers/:id/sync", post(sync))
        .route("/routers/:id/routes/:route", delete(revoke))
        .route("/routers/:id/access-policy", axum::routing::put(set_policy))
        .route("/routers/:id/authorize", post(authorize_access))
        .route("/routers/:id/revoke-subject", post(revoke_subject))
        .with_state(state)
}

type Result<T> = std::result::Result<T, StatusCode>;
fn unavailable(_: anyhow::Error) -> StatusCode {
    StatusCode::SERVICE_UNAVAILABLE
}

async fn status(State(s): State<HttpState>, Path(id): Path<String>) -> Result<Json<RouterControl>> {
    Ok(Json(load_router(&s, &id).await?))
}

async fn load_router(s: &HttpState, id: &str) -> Result<RouterControl> {
    s.fleet
        .store
        .router_control(id)
        .await
        .map_err(unavailable)?
        .ok_or(StatusCode::NOT_FOUND)
}

/// Retry edits against the latest durable record without losing concurrent
/// revocations or acknowledgments. The edit must have no external side effects.
/// Idempotent edits return without a write; sync renewals use an explicit CAS.
async fn update_router(
    s: &HttpState,
    id: &str,
    edit: impl Fn(&mut RouterControl) -> Result<()>,
) -> Result<RouterControl> {
    for _ in 0..16 {
        let previous = load_router(s, id).await?;
        let mut next = previous.clone();
        edit(&mut next)?;
        if next == previous
            || s.fleet
                .store
                .save_router_control(Some(&previous), &next)
                .await
                .map_err(unavailable)?
        {
            return Ok(next);
        }
    }
    Err(StatusCode::SERVICE_UNAVAILABLE)
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
        if next.token_id != request.token_id {
            next.capabilities.clear();
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
    let next = update_router(&s, &id, |next| {
        if next.revoked_routes.contains(&route) {
            return Ok(());
        }
        if next.revoked_routes.len() >= MAX_ROUTER_REVOCATIONS {
            return Err(StatusCode::INSUFFICIENT_STORAGE);
        }
        next.revoked_routes.push(route.clone());
        Ok(())
    })
    .await?;
    Ok(Json(next))
}

async fn sync(
    State(s): State<HttpState>,
    Path(id): Path<String>,
    Extension(caller): Extension<Option<Token>>,
    headers: HeaderMap,
    Json(request): Json<RouterSyncRequest>,
) -> Result<Json<RouterSyncResponse>> {
    // Admin credentials are deliberately not accepted as a router identity.
    let caller = caller.ok_or(StatusCode::FORBIDDEN)?;
    // A header keeps the JSON request compatible with older schedulers, which
    // reject unknown fields. Missing capability advertisements mean no support.
    let mut capabilities = Vec::new();
    for value in headers.get_all(ROUTER_CAPABILITIES_HEADER) {
        let value = value.to_str().map_err(|_| StatusCode::BAD_REQUEST)?;
        capabilities.extend(value.split(',').map(str::trim).map(str::to_owned));
    }
    capabilities.sort();
    capabilities.dedup();
    for _ in 0..16 {
        let previous = load_router(&s, &id).await?;
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
                && (request.after_revision != 0
                    || request.applied_revision.is_some()
                    || request.applied_authorization_revision.is_some())
            || request.after_revision > revision
            || request
                .applied_revision
                .is_some_and(|a| a > request.after_revision || a > previous.delivered_revision)
        {
            return Err(StatusCode::CONFLICT);
        }
        let mut next = previous.clone();
        next.capabilities = capabilities.clone();
        next.validate().map_err(|_| StatusCode::BAD_REQUEST)?;
        if next
            .authorization
            .as_ref()
            .is_some_and(|a| a.policies.iter().any(|p| p.workload == "*"))
            && !next
                .capabilities
                .iter()
                .any(|c| c == ROUTER_WORKLOAD_DEFAULTS)
        {
            return Err(StatusCode::CONFLICT);
        }
        // Persist disabled policies with increasing versions, so an active
        // relay withdraws existing streams and a later resume cannot revive old
        // grants. An administrator must explicitly publish a new enabled policy.
        if let Some(auth) = &mut next.authorization {
            let suspended: std::collections::HashSet<_> = auth
                .policies
                .iter()
                .filter(|p| p.enabled && !namespace_active(&s, &p.namespace))
                .map(|p| p.namespace.clone())
                .collect();
            for namespace in suspended {
                auth.suspend_namespace(&namespace)
                    .map_err(|_| StatusCode::INSUFFICIENT_STORAGE)?;
            }
        }
        next.delivered_revision = revision;
        if let Some(applied) = request.applied_authorization_revision {
            if previous
                .delivered_authorization_revision
                .is_none_or(|d| applied > d)
                || previous
                    .applied_authorization_revision
                    .is_some_and(|r| applied < r)
            {
                return Err(StatusCode::CONFLICT);
            }
            next.applied_authorization_revision = Some(applied);
        }
        next.delivered_authorization_revision = next.authorization.as_ref().map(|a| a.revision);
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
                authorization: next.authorization,
            }));
        }
    }
    Err(StatusCode::SERVICE_UNAVAILABLE)
}

fn namespace_active(s: &HttpState, namespace: &str) -> bool {
    s.fleet
        .namespace(namespace)
        .is_some_and(|n| n.id == namespace && n.suspended_ms.is_none())
}

fn namespace_scope(
    s: &HttpState,
    caller: Option<&Token>,
    namespace: &str,
    allow_suspended: bool,
) -> Result<()> {
    if caller.is_some_and(|c| c.namespace.as_deref() != Some(namespace))
        || !s
            .fleet
            .namespace(namespace)
            .is_some_and(|n| n.id == namespace && (allow_suspended || n.suspended_ms.is_none()))
    {
        return Err(StatusCode::FORBIDDEN);
    }
    Ok(())
}

async fn authorize_access(
    State(s): State<HttpState>,
    Path(id): Path<String>,
    Extension(caller): Extension<Option<Token>>,
    Json(request): Json<RouterAuthorizeRequest>,
) -> Result<Json<RouterAccessBinding>> {
    // Business identities require a namespace delegate. The reserved control
    // service retains the separately authenticated administrator path.
    if (request.service == "control") != caller.is_none() {
        return Err(StatusCode::FORBIDDEN);
    }
    namespace_scope(&s, caller.as_ref(), &request.namespace, false)?;
    let state = load_router(&s, &id).await?;
    let auth = state.authorization.ok_or(StatusCode::FORBIDDEN)?;
    Ok(Json(
        auth.authorize(&request)
            .map_err(|_| StatusCode::FORBIDDEN)?,
    ))
}

async fn set_policy(
    State(s): State<HttpState>,
    Path(id): Path<String>,
    Json(policy): Json<RouterAccessPolicy>,
) -> Result<Json<RouterAuthorization>> {
    policy.validate().map_err(|_| StatusCode::BAD_REQUEST)?;
    let _policies = s.fleet.router_policy_ops.lock().await;
    namespace_scope(&s, None, &policy.namespace, !policy.enabled)?;
    let next = update_router(&s, &id, |next| {
        let auth = next
            .authorization
            .get_or_insert_with(RouterAuthorization::default);
        let old = auth.policies.iter_mut().find(|p| {
            p.namespace == policy.namespace
                && p.workload == policy.workload
                && p.service == policy.service
        });
        let disabling_existing = !policy.enabled && old.is_some();
        if policy.workload == "*"
            && !disabling_existing
            && !next
                .capabilities
                .iter()
                .any(|c| c == ROUTER_WORKLOAD_DEFAULTS)
        {
            return Err(StatusCode::CONFLICT);
        }
        if let Some(old) = old {
            if *old == policy {
                return Ok(());
            }
            if policy.version <= old.version {
                return Err(StatusCode::CONFLICT);
            }
            *old = policy.clone();
        } else {
            auth.policies.push(policy.clone());
        }
        auth.revision = auth
            .revision
            .checked_add(1)
            .ok_or(StatusCode::INSUFFICIENT_STORAGE)?;
        auth.validate()
            .map_err(|_| StatusCode::INSUFFICIENT_STORAGE)
    })
    .await?;
    Ok(Json(next.authorization.unwrap()))
}

async fn revoke_subject(
    State(s): State<HttpState>,
    Path(id): Path<String>,
    Extension(caller): Extension<Option<Token>>,
    Json(revocation): Json<RouterSubjectRevocation>,
) -> Result<Json<RouterAuthorization>> {
    revocation.validate().map_err(|_| StatusCode::BAD_REQUEST)?;
    namespace_scope(&s, caller.as_ref(), &revocation.namespace, caller.is_none())?;
    let next = update_router(&s, &id, |next| {
        let auth = next.authorization.as_mut().ok_or(StatusCode::FORBIDDEN)?;
        if !auth
            .policies
            .iter()
            .any(|p| p.namespace == revocation.namespace)
        {
            return Err(StatusCode::FORBIDDEN);
        }
        if auth.revoked_subjects.contains(&revocation) {
            return Ok(());
        }
        auth.revoked_subjects.push(revocation.clone());
        auth.revision = auth
            .revision
            .checked_add(1)
            .ok_or(StatusCode::INSUFFICIENT_STORAGE)?;
        auth.validate()
            .map_err(|_| StatusCode::INSUFFICIENT_STORAGE)
    })
    .await?;
    Ok(Json(scoped_authorization(
        next.authorization.unwrap(),
        caller.as_ref(),
    )))
}

fn scoped_authorization(
    mut auth: RouterAuthorization,
    caller: Option<&Token>,
) -> RouterAuthorization {
    if let Some(caller) = caller {
        auth.policies
            .retain(|p| Some(&p.namespace) == caller.namespace.as_ref());
        auth.revoked_subjects
            .retain(|r| Some(&r.namespace) == caller.namespace.as_ref());
    }
    auth
}
