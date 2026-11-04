//! HTTP: health, the nodes' internal relays, and the API fleetctl uses. Every API route needs a token holding the
//! permission `required` names for it; its handler scopes it to the token's namespace, tag and
//! app grants. An administrator's token reaches every namespace.

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Extension, Json, Router,
};
use fleet_proto::model::v1::{
    NamespaceSelector, CreateNamespaceRequest, CreateTokenRequest,
    AllocationRequest,
};

use crate::fleet::Scheduler;
use crate::store::Token;
use crate::token::{self, Permission};

#[derive(Clone)]
pub struct HttpState {
    pub fleet: Arc<Scheduler>,
    pub workloads: Arc<crate::workloads::WorkloadController>,
}

pub fn router(state: HttpState) -> Router {
    let api = Router::new()
        .route("/status", get(status))
        .route("/namespaces", get(namespaces).post(create_namespace))
        .route("/namespaces/:name", get(namespace))
        .route("/namespaces/:name/quota", put(set_quota))
        .route("/namespaces/:name/allocations", get(allocations).post(reserve_job))
        .route("/namespaces/:name/allocations/:id", axum::routing::delete(release_job))
        .route("/allocations/:id", axum::routing::delete(release_allocation))
        .route("/preemption/plan", post(preemption_plan))
        .route("/namespaces/:name/suspended", put(suspend_namespace).delete(resume_namespace))
        .route("/tokens", get(list_tokens).post(add_token))
        .route("/tokens/:id", axum::routing::delete(revoke_token))
        .route("/nodes/:name/drain", post(drain).delete(undrain))
        .with_state(state.clone())
        .merge(crate::workloads::router(state.workloads.clone()))
        .route_layer(axum::middleware::from_fn_with_state(state.clone(), authorize));
    Router::new()
        .route("/access", get(access))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(state.clone())
        .merge(api)
        .merge(crate::proof_fetch::router(state.clone()))
        .merge(crate::log_relay::router(state.clone()))
        .merge(crate::workloads::internal_router(state.workloads))
}

/// The permission an API route needs. Reading apps, batches, their jobs and the caller's own
/// namespace needs read; submitting a job (or cancelling a batch job) submit; changing an app
/// or batch, or purging its jobs, manage; everything else (namespaces and their quotas,
/// tokens, allocations, preemption, nodes, the fleet's status) admin.
pub fn required(method: &Method, path: &str) -> Permission {
    let parts: Vec<&str> = path.trim_matches('/').split('/').collect();
    match (method, parts.as_slice()) {
        (&Method::GET, ["namespaces"] | ["namespaces", _]) => Permission::Read,
        (&Method::GET, ["apps" | "batches", ..]) => Permission::Read,
        (&Method::POST, ["apps" | "batches", _, "jobs"]) => Permission::Submit,
        (&Method::POST, ["batches", _, "jobs", _, "cancel"]) => Permission::Submit,
        (_, ["batches", _, "jobs", _, "preempt"]) => Permission::Admin,
        (_, ["apps" | "batches", ..]) => Permission::Manage,
        _ => Permission::Admin,
    }
}

/// Admits a request whose token holds the route's permission. Handlers get the token as
/// `Option<Token>`: `None` for an administrator's, which no namespace or grant limits.
async fn authorize(
    State(s): State<HttpState>,
    headers: HeaderMap,
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let header = headers.get(token::HEADER).and_then(|v| v.to_str().ok());
    let Some(c) = s.fleet.token_for_token(header) else {
        return (StatusCode::UNAUTHORIZED, "missing or wrong token").into_response();
    };
    let need = required(req.method(), req.uri().path());
    if !token::allowed(Some(&c), need) {
        return (StatusCode::FORBIDDEN, format!("this token lacks the {} permission", need.name())).into_response();
    }
    let principal = (!token::allowed(Some(&c), Permission::Admin)).then_some(c);
    req.extensions_mut().insert(principal);
    next.run(req).await
}

async fn access(State(s): State<HttpState>, headers: HeaderMap) -> Response {
    match s.fleet.token_for_token(headers.get(token::HEADER).and_then(|v| v.to_str().ok())) {
        Some(c) => Json(s.fleet.access_record(&c)).into_response(),
        None => StatusCode::UNAUTHORIZED.into_response(),
    }
}

fn internal(e: anyhow::Error) -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")).into_response()
}

async fn status(State(s): State<HttpState>) -> Response {
    let mut status = serde_json::to_value(s.fleet.status()).unwrap();
    status["nodes"] = serde_json::json!(s.workloads.worker_status());
    Json(status).into_response()
}

async fn namespaces(State(s): State<HttpState>, Extension(c): Extension<Option<Token>>, axum::extract::Query(q): axum::extract::Query<NamespaceSelector>,
) -> Response {
    let mut namespaces = s.fleet.namespaces();
    if let Some(c) = c {
        let namespace = match selected_namespace(&s, Some(&c), q.namespace.as_deref()) { Ok(n) => n, Err(r) => return r };
        if let Err(r) = selected_tag(&s, Some(&c), namespace.as_deref(), q.tag.as_deref()) { return r; }
        namespaces.retain(|n| Some(n.id.as_str()) == namespace.as_deref());
        for n in &mut namespaces {
            n.tag_quotas.retain(|tag, _| c.tag.as_deref() == Some(tag.as_str()));
        }
    }
    Json(namespaces).into_response()
}

async fn create_namespace(State(s): State<HttpState>, Json(n): Json<CreateNamespaceRequest>) -> Response {
    if let Err(e) = n.quota.validate() { return (StatusCode::BAD_REQUEST, format!("{e:#}")).into_response(); }
    match s.fleet.create_namespace(&n.name, n.quota).await {
        Ok(n) => (StatusCode::CREATED, Json(n)).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e:#}")).into_response(),
    }
}

fn namespace_record(s: &HttpState, name: &str, tag: Option<&str>, c: Option<&Token>) -> Response {
    match s.fleet.namespace(name) {
        Some(mut n) => {
            if let Some(c) = c {
                n.tag_quotas.retain(|tag, _| c.tag.as_deref() == Some(tag.as_str()));
            }
            let mut v = serde_json::to_value(&n).unwrap();
            v["usage"] = serde_json::json!(s.fleet.usage(&n.id));
            if let Some(tag) = tag {
                let tag = (!tag.is_empty()).then_some(tag);
                if let Some(tag) = tag {
                    if let Err(e) = crate::fleet::Scheduler::validate_tag(tag) { return (StatusCode::BAD_REQUEST,e.to_string()).into_response(); }
                }
                v["tag"] = serde_json::json!(tag);
                v["tag_quota"] = serde_json::json!(n.tag_quotas.get(tag.unwrap_or("")).cloned().unwrap_or_default());
                v["tag_usage"] = serde_json::json!(s.fleet.tag_usage(&n.id,tag));
            } else {
                v["tags"] = serde_json::json!(s.fleet.tags(&n.id));
            }
            Json(v).into_response()
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn namespace(State(s): State<HttpState>, Extension(c): Extension<Option<Token>>, Path(name): Path<String>, axum::extract::Query(query): axum::extract::Query<NamespaceSelector>,
) -> Response {
    if let Some(c) = c.as_ref() {
        let namespace = match selected_namespace(&s, Some(c), Some(&name)) { Ok(n) => n, Err(r) => return r };
        if let Err(r) = selected_namespace(&s, Some(c), query.namespace.as_deref()) { return r; }
        let tag = match selected_tag(&s, Some(c), namespace.as_deref(), query.tag.as_deref()) { Ok(tag) => tag, Err(r) => return r };
        return namespace_record(&s, &name, tag.as_deref(), Some(c));
    }
    namespace_record(&s,&name,query.tag.as_deref(), None)
}

async fn set_quota(State(s): State<HttpState>, Path(name): Path<String>, axum::extract::Query(query): axum::extract::Query<NamespaceSelector>, Json(q): Json<crate::quota::Quota>,
) -> Response {
    if let Err(e) = q.validate() { return (StatusCode::BAD_REQUEST, format!("{e:#}")).into_response(); }
    if let Some(tag) = query.tag {
        return match s.fleet.set_tag_quota(&name,&tag,q).await {
            Ok(_) => namespace_record(&s,&name,Some(&tag), None), Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
        };
    }
    match s.fleet.set_quota(&name, q).await {
        Ok(n) => Json(n).into_response(), Err(e) => app_outcome(Err(e)),
    }
}

async fn allocations(State(s): State<HttpState>, Path(name): Path<String>) -> Response {
    match s.fleet.namespace(&name) {
        Some(n) => Json(s.fleet.allocations(&n.id)).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn reserve_job(State(s): State<HttpState>, Path(name): Path<String>, Json(r): Json<AllocationRequest>,
) -> Response {
    if let Some(tag) = &r.tag {
        if let Err(e) = crate::fleet::Scheduler::validate_tag(tag) { return (StatusCode::BAD_REQUEST, e.to_string()).into_response(); }
    }
    match s.fleet.reserve_job(&name, r.tag.as_deref(), &r.id, r.gpus, r.cpus, r.restore).await {
        Ok(a) => Json(a).into_response(), Err(e) => app_outcome(Err(e)),
    }
}

async fn release_job(State(s): State<HttpState>, Path((name, id)): Path<(String, String)>,
) -> Response {
    let Some(n) = s.fleet.namespace(&name) else { return StatusCode::NOT_FOUND.into_response(); };
    match s.fleet.release_job(&n.id, &id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(), Err(e) => app_outcome(Err(e)),
    }
}

async fn release_allocation(State(s): State<HttpState>, Path(id): Path<String>) -> Response {
    match s.fleet.release_allocation(&id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => app_outcome(Err(e)),
    }
}

async fn preemption_plan(State(s): State<HttpState>, Json(request): Json<crate::preemption::PreemptionRequest>,
) -> Response {
    match s.fleet.plan_preemption(request) {
        Ok(plan) => Json(plan).into_response(),
        Err(e) => match e.downcast_ref::<crate::fleet::RequestRejection>() {
            Some(r) => (StatusCode::from_u16(r.status).unwrap_or(StatusCode::BAD_REQUEST), r.message.clone(),
            ).into_response(),
            None => internal(e),
        },
    }
}

async fn suspend_namespace(State(s): State<HttpState>, Path(name): Path<String>) -> Response {
    match s.fleet.suspend_namespace(&name, true).await {
        Ok(n) => Json(n).into_response(), Err(e) => app_outcome(Err(e)),
    }
}

async fn resume_namespace(State(s): State<HttpState>, Path(name): Path<String>) -> Response {
    match s.fleet.suspend_namespace(&name, false).await {
        Ok(n) => Json(n).into_response(), Err(e) => app_outcome(Err(e)),
    }
}

fn selected_namespace(s: &HttpState, c: Option<&Token>, selected: Option<&str>,
) -> Result<Option<String>, Response> {
    if let Some(c) = c {
        let n = c.namespace.as_ref().and_then(|id| s.fleet.namespace(id)).ok_or_else(|| StatusCode::UNAUTHORIZED.into_response())?;
        if selected.is_some_and(|v| v != n.id && v != n.name) { return Err(StatusCode::FORBIDDEN.into_response()); }
        Ok(Some(n.id))
    } else if let Some(reference) = selected {
        s.fleet.namespace(reference).map(|n| Some(n.id)).ok_or_else(|| StatusCode::NOT_FOUND.into_response())
    } else { Ok(None) }
}

fn selected_tag(s: &HttpState, c: Option<&Token>, namespace: Option<&str>, selected: Option<&str>,
) -> Result<Option<String>,Response> {
    if let Some(c)=c {
        if selected.is_some_and(|g| c.tag.as_deref()!=Some(g)) { return Err(StatusCode::FORBIDDEN.into_response()); }
        Ok(Some(c.tag.clone().unwrap_or_default()))
    } else if let Some(tag)=selected {
        let n=namespace.ok_or_else(|| (StatusCode::BAD_REQUEST,"tag requires namespace").into_response())?;
        if s.fleet.namespace(n).is_none() { return Err(StatusCode::NOT_FOUND.into_response()); }
        crate::fleet::Scheduler::validate_tag(tag).map_err(|e|(StatusCode::BAD_REQUEST,e.to_string()).into_response())?;
        Ok(Some(tag.into()))
    } else { Ok(None) }
}

async fn list_tokens(State(s): State<HttpState>) -> Response {
    Json(s.fleet.tokens()).into_response()
}

async fn add_token(State(s): State<HttpState>, Json(c): Json<CreateTokenRequest>) -> Response {
    match s
        .fleet
        .add_user_token(&c.id, &c.address, &c.label, c.apps.map(|apps| apps.values), c.token_hash, c.namespace, c.tag, c.permissions,
        )
        .await
    {
        Ok(cred) => {
            tracing::info!(token_id = %cred.id, label = %cred.label, permissions = ?cred.permissions, "token added");
            (StatusCode::CREATED, Json(cred)).into_response()
        }
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e:#}")).into_response(),
    }
}

async fn revoke_token(State(s): State<HttpState>, Path(id): Path<String>) -> Response {
    match s.fleet.revoke_token(&id).await {
        Ok(true) => {
            tracing::info!(%id, "token revoked");
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => internal(e),
    }
}

/// An app action's outcome: its JSON, a refusal with its status, or 500.
fn app_outcome(r: anyhow::Result<serde_json::Value>) -> Response {
    match r {
        Ok(v) => Json(v).into_response(),
        Err(e) => match e.downcast_ref::<crate::fleet::RequestRejection>() {
            Some(r) => (StatusCode::from_u16(r.status).unwrap_or(StatusCode::BAD_REQUEST), r.message.clone(),
            ).into_response(),
            None => internal(e),
        },
    }
}

async fn drain(State(s): State<HttpState>, Path(name): Path<String>) -> Response {
    if s.workloads.drain_worker(&name, true) {
        StatusCode::NO_CONTENT.into_response()
    } else {
        (StatusCode::NOT_FOUND, "no such machine node").into_response()
    }
}

async fn undrain(State(s): State<HttpState>, Path(name): Path<String>) -> Response {
    if s.workloads.drain_worker(&name, false) {
        StatusCode::NO_CONTENT.into_response()
    } else {
        (StatusCode::NOT_FOUND, "no such machine node").into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_route_needs_the_permission_its_effect_does() {
        use Permission::*;
        for (method, path, need) in [
            (Method::GET, "/namespaces", Read),
            (Method::GET, "/namespaces/zkm", Read),
            (Method::GET, "/apps", Read),
            (Method::GET, "/apps/goat/jobs/p-1/output/proof.json", Read),
            (Method::GET, "/batches/training/jobs/j/logs", Read),
            (Method::POST, "/apps/goat/jobs", Submit),
            (Method::POST, "/batches/training/jobs", Submit),
            (Method::POST, "/batches/training/jobs/j/cancel", Submit),
            (Method::POST, "/batches/training/jobs/j/preempt", Admin),
            (Method::POST, "/apps", Manage),
            (Method::DELETE, "/apps/goat", Manage),
            (Method::PUT, "/apps/goat/disabled", Manage),
            (Method::PUT, "/apps/goat/scale", Manage),
            (Method::DELETE, "/batches/training/jobs", Manage),
            (Method::GET, "/status", Admin),
            (Method::POST, "/namespaces", Admin),
            (Method::GET, "/namespaces/zkm/allocations", Admin),
            (Method::PUT, "/namespaces/zkm/quota", Admin),
            (Method::GET, "/tokens", Admin),
            (Method::POST, "/nodes/gpu/drain", Admin),
            (Method::POST, "/preemption/plan", Admin),
        ] {
            assert_eq!(required(&method, path), need, "{method} {path}");
        }
    }

    #[test]
    fn admin_holds_every_permission_and_others_only_theirs() {
        let token = |p: &[&str]| fleet_proto::model::v1::Token { permissions: p.iter().map(|s| s.to_string()).collect(), ..Default::default() };
        for p in [Permission::Read, Permission::Submit, Permission::Manage, Permission::Admin] {
            assert!(token::allowed(Some(&token(&["admin"])), p));
            assert!(token::allowed(None, p));
        }
        let reader = token(&["read", "submit"]);
        assert!(token::allowed(Some(&reader), Permission::Submit));
        assert!(!token::allowed(Some(&reader), Permission::Manage));
        assert!(!token::allowed(Some(&token(&["manage"])), Permission::Admin));
    }
}
