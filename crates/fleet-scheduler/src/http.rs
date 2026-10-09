//! HTTP: public values for the SDK (`/{proof_id}/wrap/public_values.bin`), health,
//! and the fleetctl API: operators administer, scoped users read their own jobs.

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Extension, Json, Router,
};
use serde::Deserialize;

use crate::blobs::{self, is_safe_id};
use crate::fleet::Fleet;
use crate::store::{Token, JobFilter};
use crate::token;

#[derive(Clone)]
pub struct HttpState {
    pub fleet: Arc<Fleet>,
    pub admin_token: Arc<String>,
}

pub fn router(state: HttpState) -> Router {
    let admin = Router::new()
        .route("/status", get(status))
        .route("/namespaces", get(namespaces).post(create_namespace))
        .route("/namespaces/:name", get(namespace))
        .route("/namespaces/:name/quota", put(set_quota))
        .route("/namespaces/:name/allocations", get(allocations).post(reserve_run))
        .route("/namespaces/:name/allocations/:id", axum::routing::delete(release_run))
        .route("/allocations/:id", axum::routing::delete(release_allocation))
        .route("/namespaces/:name/suspended", put(suspend_namespace).delete(resume_namespace))
        .route("/tokens", get(list_tokens).post(add_token))
        .route(
            "/tokens/:id",
            axum::routing::delete(revoke_token),
        )
        .route("/apps", get(apps).post(add_app).delete(delete_app))
        .route("/apps/disabled", put(disable_app).delete(enable_app))
        .route("/apps/priority", put(set_app_priority))
        .route("/jobs", get(jobs))
        .route("/jobs/:id", get(job))
        .route("/nodes/:name/drain", post(drain).delete(undrain))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            authorize_http,
        ));
    Router::new()
        .route("/access", get(access))
        .route("/healthz", get(|| async { "ok" }))
        .route("/:proof_id/wrap/public_values.bin", get(public_values))
        .nest("/admin", admin)
        .with_state(state)
}

async fn authorize_http(
    State(s): State<HttpState>,
    headers: HeaderMap,
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let header = headers.get(token::HEADER).and_then(|v| v.to_str().ok());
    let principal = if token::bearer_matches(header, &s.admin_token) {
        None
    } else if let Some(c) = s.fleet.token_for_token(header) {
        let path = req.uri().path().strip_prefix("/admin").unwrap_or(req.uri().path());
        let read_scoped = req.method() == axum::http::Method::GET
            && (path == "/apps" || path == "/jobs"
                || path.strip_prefix("/jobs/").is_some_and(|id| !id.is_empty() && !id.contains('/')));
        let create_app = req.method() == axum::http::Method::POST && path == "/apps" && c.manage_apps;
        let delete_app = req.method() == axum::http::Method::DELETE && path == "/apps" && c.manage_apps;
        if !read_scoped && !create_app && !delete_app {
            return StatusCode::FORBIDDEN.into_response();
        }
        Some(c)
    } else {
        return (StatusCode::UNAUTHORIZED, "missing or wrong token").into_response();
    };
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

#[derive(Deserialize)]
struct PublicValuesQuery {
    key: Option<String>,
    token_id: Option<String>,
}

/// The SDK fetches this with a plain GET of the URL GetStatus gave it, which carries the
/// proof's key: only the caller GetStatus answered can read it.
async fn public_values(
    State(s): State<HttpState>,
    Path(proof_id): Path<String>,
    axum::extract::Query(q): axum::extract::Query<PublicValuesQuery>,
) -> Response {
    if !is_safe_id(&proof_id) { return StatusCode::NOT_FOUND.into_response(); }
    let job = match s.fleet.job(&proof_id).await {
        Ok(Some(j)) => j,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => return internal(e),
    };
    let allowed = if job.namespace != crate::store::SYSTEM_NAMESPACE {
        q.token_id.as_ref().and_then(|id| s.fleet.token_for_id(id)).is_some_and(|c| {
            c.namespace.as_deref() == Some(job.namespace.as_str()) && c.tag.as_deref().unwrap_or("") == job.tag && s.fleet.public_values_open(
                &format!("{proof_id}\0{}", c.id), q.key.as_deref())
        })
    } else {
        s.fleet.public_values_open(&proof_id, q.key.as_deref())
            && s.fleet.tenant_for_signer(&job.owner).is_some()
    };
    if !allowed { return StatusCode::NOT_FOUND.into_response(); }
    match s.fleet.blobs.sdk_output(&job, blobs::PUBLIC_VALUES).await {
        Ok(bytes) => ([("content-type", "application/octet-stream")], bytes).into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn status(State(s): State<HttpState>) -> Response {
    Json(s.fleet.status()).into_response()
}

async fn namespaces(State(s): State<HttpState>) -> Response {
    Json(s.fleet.namespaces()).into_response()
}

#[derive(Deserialize)]
struct NewNamespace { name: String, #[serde(default)] quota: crate::quota::Quota }

async fn create_namespace(State(s): State<HttpState>, Json(n): Json<NewNamespace>) -> Response {
    if let Err(e) = n.quota.validate() { return (StatusCode::BAD_REQUEST, format!("{e:#}")).into_response(); }
    match s.fleet.create_namespace(&n.name, n.quota).await {
        Ok(n) => (StatusCode::CREATED, Json(n)).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e:#}")).into_response(),
    }
}

fn namespace_record(s: &HttpState, name: &str, tag: Option<&str>) -> Response {
    match s.fleet.namespace(name) {
        Some(n) => {
            let mut v = serde_json::to_value(&n).unwrap();
            v["usage"] = serde_json::json!(s.fleet.usage(&n.id));
            if let Some(tag) = tag {
                let tag = (!tag.is_empty()).then_some(tag);
                if let Some(tag) = tag {
                    if let Err(e) = crate::fleet::Fleet::validate_tag(tag) { return (StatusCode::BAD_REQUEST,e.to_string()).into_response(); }
                }
                v["tag"] = serde_json::json!(tag);
                v["tag_quota"] = serde_json::json!(n.tag_quotas.get(tag.unwrap_or("")).cloned().unwrap_or_default());
                v["tag_usage"] = serde_json::json!(s.fleet.tag_usage(&n.id,tag));
            } else {
                v["tags"] = serde_json::json!(s.fleet.tags(&n.id));
            }
            Json(v).into_response()
        },
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn namespace(State(s): State<HttpState>, Path(name): Path<String>, axum::extract::Query(query): axum::extract::Query<NamespaceQuery>) -> Response {
    namespace_record(&s,&name,query.tag.as_deref())
}

async fn set_quota(State(s): State<HttpState>, Path(name): Path<String>, axum::extract::Query(query): axum::extract::Query<NamespaceQuery>, Json(q): Json<crate::quota::Quota>) -> Response {
    if let Err(e) = q.validate() { return (StatusCode::BAD_REQUEST, format!("{e:#}")).into_response(); }
    if let Some(tag) = query.tag {
        return match s.fleet.set_tag_quota(&name,&tag,q).await {
            Ok(_) => namespace_record(&s,&name,Some(&tag)), Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReserveRun { id: String, #[serde(default)] tag: Option<String>, gpus: f64, cpus: f64, #[serde(default)] restore: bool }

async fn reserve_run(State(s): State<HttpState>, Path(name): Path<String>, Json(r): Json<ReserveRun>) -> Response {
    match s.fleet.reserve_run(&name, r.tag.as_deref(), &r.id, r.gpus, r.cpus, r.restore).await {
        Ok(a) => Json(a).into_response(), Err(e) => app_outcome(Err(e)),
    }
}

async fn release_run(State(s): State<HttpState>, Path((name, id)): Path<(String, String)>) -> Response {
    let Some(n) = s.fleet.namespace(&name) else { return StatusCode::NOT_FOUND.into_response(); };
    match s.fleet.release_run(&n.id, &id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(), Err(e) => app_outcome(Err(e)),
    }
}

async fn release_allocation(State(s): State<HttpState>, Path(id): Path<String>) -> Response {
    match s.fleet.release_allocation(&id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => app_outcome(Err(e)),
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

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct NamespaceQuery { namespace: Option<String>, tag: Option<String> }

fn selected_namespace(s: &HttpState, c: Option<&Token>, selected: Option<&str>) -> Result<Option<String>, Response> {
    if let Some(c) = c {
        let n = c.namespace.as_ref().and_then(|id| s.fleet.namespace(id)).ok_or_else(|| StatusCode::UNAUTHORIZED.into_response())?;
        if selected.is_some_and(|v| v != n.id && v != n.name) { return Err(StatusCode::FORBIDDEN.into_response()); }
        Ok(Some(n.id))
    } else if let Some(reference) = selected {
        s.fleet.namespace(reference).map(|n| Some(n.id)).ok_or_else(|| StatusCode::NOT_FOUND.into_response())
    } else { Ok(None) }
}

fn selected_tag(s: &HttpState, c: Option<&Token>, namespace: Option<&str>, selected: Option<&str>) -> Result<Option<String>,Response> {
    if let Some(c)=c {
        if selected.is_some_and(|g| c.tag.as_deref()!=Some(g)) { return Err(StatusCode::FORBIDDEN.into_response()); }
        Ok(Some(c.tag.clone().unwrap_or_default()))
    } else if let Some(tag)=selected {
        let n=namespace.ok_or_else(|| (StatusCode::BAD_REQUEST,"tag requires namespace").into_response())?;
        if s.fleet.namespace(n).is_none() { return Err(StatusCode::NOT_FOUND.into_response()); }
        crate::fleet::Fleet::validate_tag(tag).map_err(|e|(StatusCode::BAD_REQUEST,e.to_string()).into_response())?;
        Ok(Some(tag.into()))
    } else { Ok(None) }
}

async fn list_tokens(State(s): State<HttpState>) -> Response {
    Json(s.fleet.tokens()).into_response()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NewToken {
    id: String,
    tenant: String,
    address: String,
    #[serde(default)]
    label: String,
    apps: Option<Vec<String>>,
    token_hash: Option<String>,
    namespace: Option<String>,
    tag: Option<String>,
    #[serde(default)]
    manage_apps: bool,
}

async fn add_token(State(s): State<HttpState>, Json(c): Json<NewToken>) -> Response {
    match s
        .fleet
        .add_user_token(&c.tenant, &c.id, &c.address, &c.label, c.apps, c.token_hash, c.namespace, c.tag, c.manage_apps)
        .await
    {
        Ok(cred) => {
            tracing::info!(tenant = %cred.tenant, token_id = %cred.id, label = %cred.label, "token added");
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JobsQuery {
    namespace: Option<String>,
    tag: Option<String>,
    tenant: Option<String>,
    elf: Option<String>,
    #[serde(default)]
    active: bool,
    #[serde(default = "default_jobs_limit")]
    limit: usize,
}

fn default_jobs_limit() -> usize {
    20
}

async fn apps(State(s): State<HttpState>, Extension(c): Extension<Option<Token>>, axum::extract::Query(q): axum::extract::Query<NamespaceQuery>) -> Response {
    let namespace = match selected_namespace(&s, c.as_ref(), q.namespace.as_deref()) { Ok(n) => n, Err(r) => return r };
    let tag = match selected_tag(&s,c.as_ref(),namespace.as_deref(),q.tag.as_deref()) { Ok(g)=>g,Err(r)=>return r };
    match s.fleet.apps().await {
        Ok(mut v) => {
            if let Some(c) = &c {
                if let Some(grants) = &c.apps {
                    v.retain(|a| grants.contains(&format!("{}/{}", a["tenant"].as_str().unwrap_or_default(), a["elf_id"].as_str().unwrap_or_default())));
                }
            }
            if namespace.is_some() {
                v.retain(|a| a["namespace"].as_str() == namespace.as_deref());
                // Counts and ordering are namespace-local even after rotating tokens.
                let jobs = match s.fleet.recent_jobs(&JobFilter { namespace, tag, ..Default::default() }, i64::MAX as usize).await {
                    Ok(j) => j, Err(e) => return internal(e),
                };
                for a in &mut v {
                    let own: Vec<_> = jobs.iter().filter(|j| Some(j.tenant.as_str()) == a["tenant"].as_str() && Some(j.elf_id.as_str()) == a["elf_id"].as_str()).collect();
                    a["jobs"] = serde_json::json!(own.len());
                    a["active"] = serde_json::json!(own.iter().filter(|j| !j.state.is_terminal()).count());
                    a["succeeded"] = serde_json::json!(own.iter().filter(|j| matches!(j.state, crate::store::JobState::Succeeded)).count());
                    a["failed"] = serde_json::json!(own.iter().filter(|j| matches!(j.state, crate::store::JobState::Failed(_))).count());
                    a["last_created_ms"] = serde_json::json!(own.iter().map(|j| j.created_ms).max());
                    a["mean_prove_ms"] = serde_json::Value::Null;
                    a["anchored_on"] = serde_json::Value::Null;
                }
                v.sort_by_key(|a| std::cmp::Reverse(a["last_created_ms"].as_u64().unwrap_or(0)));
            }
            Json(v).into_response()
        }
        Err(e) => internal(e),
    }
}

/// An app by its exact key (both given; empty for the rejected requests' app).
#[derive(Deserialize)]
struct AppKey {
    namespace: Option<String>,
    tag: Option<String>,
    tenant: Option<String>,
    elf_id: Option<String>,
    priority: Option<i32>,
}

impl AppKey {
    fn parts(&self, fleet: &Fleet) -> Result<(String, String, String), Response> {
        match (&self.namespace, &self.tenant, &self.elf_id) {
            (Some(reference), Some(t), Some(e)) => match fleet.namespace(reference) {
                Some(n) => Ok((n.id, t.clone(), e.clone())),
                None => Err((StatusCode::NOT_FOUND, "unknown namespace; an administrator must create it first").into_response()),
            },
            _ => Err((StatusCode::BAD_REQUEST, "give namespace, tenant and elf_id (the full id, as GET /admin/apps lists it)").into_response()),
        }
    }
}

/// An app action's outcome: its JSON, a refusal with its status, or 500.
fn app_outcome(r: anyhow::Result<serde_json::Value>) -> Response {
    match r {
        Ok(v) => Json(v).into_response(),
        Err(e) => match e.downcast_ref::<crate::fleet::Refusal>() {
            Some(r) => (StatusCode::from_u16(r.status).unwrap_or(StatusCode::BAD_REQUEST), r.message.clone()).into_response(),
            None => internal(e),
        },
    }
}

async fn delete_app(State(s): State<HttpState>, Extension(c): Extension<Option<Token>>, axum::extract::Query(k): axum::extract::Query<AppKey>) -> Response {
    match k.parts(&s.fleet) {
        Ok((n, t, e)) => {
            if let Some(c) = c {
                if c.namespace.as_deref() != Some(n.as_str()) || c.tenant != t
                    || k.tag.as_deref().is_some_and(|tag| c.tag.as_deref() != Some(tag)) {
                    return StatusCode::FORBIDDEN.into_response();
                }
            }
            app_outcome(s.fleet.delete_app(&n, &t, &e).await)
        }
        Err(r) => r,
    }
}

async fn add_app(State(s): State<HttpState>, Extension(c): Extension<Option<Token>>, axum::extract::Query(k): axum::extract::Query<AppKey>) -> Response {
    let priority = k.priority.unwrap_or(0);
    if let Some(c) = c {
        if !c.manage_apps { return StatusCode::FORBIDDEN.into_response(); }
        match k.parts(&s.fleet) {
            Ok((namespace, tenant, _)) if c.namespace.as_deref() == Some(namespace.as_str())
                && k.tag.as_deref() == c.tag.as_deref() && tenant == c.tenant => {},
            _ => return StatusCode::FORBIDDEN.into_response(),
        }
    }
    match k.parts(&s.fleet) {
        Ok((n, t, e)) => app_outcome(s.fleet.add_app(&n, &t, &e, priority).await),
        Err(r) => r,
    }
}

async fn set_app_priority(State(s): State<HttpState>, axum::extract::Query(k): axum::extract::Query<AppKey>) -> Response {
    let Some(priority) = k.priority else { return (StatusCode::BAD_REQUEST, "priority is required").into_response(); };
    match k.parts(&s.fleet) {
        Ok((n,t,e)) if s.fleet.app_known(&n,&t,&e) => app_outcome(s.fleet.set_app_priority(&n,&t,&e,priority).await),
        Ok(_) => StatusCode::NOT_FOUND.into_response(), Err(r) => r,
    }
}

async fn disable_app(State(s): State<HttpState>, axum::extract::Query(k): axum::extract::Query<AppKey>) -> Response {
    match k.parts(&s.fleet) {
        Ok((n, t, e)) => app_outcome(s.fleet.set_app_disabled(&n, &t, &e, true).await),
        Err(r) => r,
    }
}

async fn enable_app(State(s): State<HttpState>, axum::extract::Query(k): axum::extract::Query<AppKey>) -> Response {
    match k.parts(&s.fleet) {
        Ok((n, t, e)) => app_outcome(s.fleet.set_app_disabled(&n, &t, &e, false).await),
        Err(r) => r,
    }
}

async fn jobs(State(s): State<HttpState>, Extension(c): Extension<Option<Token>>, axum::extract::Query(q): axum::extract::Query<JobsQuery>) -> Response {
    let namespace = match selected_namespace(&s, c.as_ref(), q.namespace.as_deref()) { Ok(n) => n, Err(r) => return r };
    let tag = match selected_tag(&s,c.as_ref(),namespace.as_deref(),q.tag.as_deref()) { Ok(g)=>g,Err(r)=>return r };
    match s.fleet.recent_jobs(&JobFilter { namespace, tag, owner: None, tenant: q.tenant, elf: q.elf, active: q.active }, q.limit.min(1000)).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => internal(e),
    }
}

async fn job(State(s): State<HttpState>, Extension(c): Extension<Option<Token>>, Path(id): Path<String>, axum::extract::Query(q): axum::extract::Query<NamespaceQuery>) -> Response {
    let namespace = match selected_namespace(&s, c.as_ref(), q.namespace.as_deref()) { Ok(n) => n, Err(r) => return r };
    let tag = match selected_tag(&s,c.as_ref(),namespace.as_deref(),q.tag.as_deref()) { Ok(g)=>g,Err(r)=>return r };
    let resolved = if let Some(n) = &namespace {
        match s.fleet.namespace_job(n, tag.as_deref().filter(|t|!t.is_empty()), &id).await {
            Ok(Some(j)) => Ok(Some(j)),
            Ok(None) => s.fleet.job(&id).await,
            Err(e) => Err(e),
        }
    } else { s.fleet.job(&id).await };
    match resolved {
        Ok(Some(j)) if namespace.as_ref().is_none_or(|n| &j.namespace == n) && tag.as_ref().is_none_or(|g|&j.tag==g) => Json(j).into_response(),
        Ok(_) => StatusCode::NOT_FOUND.into_response(),
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
