//! HTTP: public values for the SDK (`/{proof_id}/wrap/public_values.bin`), health,
//! and the fleetctl API: operators manage quotas, owners manage scoped app policy.

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Extension, Json, Router,
};
use fleet_proto::model::v1::{
    ProofAppRequest, ProofJobsQuery, NamespaceSelector, CreateNamespaceRequest, CreateTokenRequest, PublicValuesQuery,
    AllocationRequest,
};

use crate::blobs::{self, is_safe_id};
use crate::fleet::Scheduler;
use crate::store::{ProofJobFilter, Token};
use crate::token;

#[derive(Clone)]
pub struct HttpState {
    pub fleet: Arc<Scheduler>,
    pub admin_token: Arc<String>,
    pub workloads: Arc<crate::workloads::WorkloadController>,
}

pub fn router(state: HttpState) -> Router {
    let admin = Router::new()
        .route("/status", get(status))
        .route("/namespaces", get(namespaces).post(create_namespace))
        .route("/namespaces/:name", get(namespace))
        .route("/namespaces/:name/quota", put(set_quota))
        .route("/namespaces/:name/allocations", get(allocations).post(reserve_job),
        )
        .route("/namespaces/:name/allocations/:id", axum::routing::delete(release_job),
        )
        .route("/allocations/:id", axum::routing::delete(release_allocation),
        )
        .route("/preemption/plan", post(preemption_plan))
        .route("/namespaces/:name/suspended", put(suspend_namespace).delete(resume_namespace),
        )
        .route("/tokens", get(list_tokens).post(add_token))
        .route(
            "/tokens/:id",
            axum::routing::delete(revoke_token))
        .route("/apps", get(apps).post(add_app).delete(delete_app))
        .route("/apps/disabled", put(disable_app).delete(enable_app))
        .route("/apps/priority", put(set_app_priority))
        .route("/jobs", get(jobs))
        .route("/jobs/:id", get(job))
        .route("/jobs/:id/output/:name", get(proof_output))
        .route("/proof-jobs", axum::routing::post(crate::proof_api::submit))

        .route("/nodes/:name/drain", post(drain).delete(undrain))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            authorize_http,
        ));
    let fetch = crate::proof_fetch::router(state.clone()).merge(crate::log_relay::router(state.clone()));
    Router::new()
        .route("/access", get(access))
        .route("/healthz", get(|| async { "ok" }))
        .route("/:proof_id/wrap/public_values.bin", get(public_values))
        .nest("/admin", admin)
        .with_state(state)
        .merge(fetch)
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
            && (path == "/apps" || path == "/jobs" || path == "/namespaces"
                || path.strip_prefix("/namespaces/").is_some_and(|name| !name.is_empty() && !name.contains('/'))
                || path.strip_prefix("/jobs/").is_some_and(|id| {
                    let parts = id.split('/').collect::<Vec<_>>();
                    !parts[0].is_empty() && (parts.len() == 1 ||
                        (parts.len() == 3 && parts[1] == "output" && matches!(parts[2], blobs::PROOF | blobs::PUBLIC_VALUES)))
                }));
        let manage_app = c.manage_apps && matches!((req.method().as_str(), path),
            ("POST" | "DELETE", "/apps") | ("PUT" | "DELETE", "/apps/disabled") | ("PUT", "/apps/priority"));
        if !read_scoped && !manage_app && !(path == "/proof-jobs" && req.method() == axum::http::Method::POST) {
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
            c.namespace.as_deref() == Some(job.namespace.as_str()) && c.tag.as_deref().unwrap_or("") == job.tag
                && c.apps.as_ref().is_none_or(|apps| apps.contains(&format!("{}/{}", job.tenant, job.elf_id))) && s.fleet.public_values_open(
                &format!("{proof_id}\0{}", c.id), q.key.as_deref())
        })
    } else {
        s.fleet.public_values_open(&proof_id, q.key.as_deref())
            && s.fleet.tenant_for_signer(&job.owner).is_some()
    };
    if !allowed { return StatusCode::NOT_FOUND.into_response(); }
    match s.fleet.proof_output(&job, blobs::PUBLIC_VALUES).await {
        Ok(bytes) => ([("content-type", "application/octet-stream")], bytes).into_response(),
        Err(e) => crate::proof_fetch::response_error(e),
    }
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
        .add_user_token(&c.tenant, &c.id, &c.address, &c.label, c.apps.map(|apps| apps.values), c.token_hash, c.namespace, c.tag, c.manage_apps,
        )
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

async fn apps(State(s): State<HttpState>, Extension(c): Extension<Option<Token>>, axum::extract::Query(q): axum::extract::Query<NamespaceSelector>,
) -> Response {
    let namespace = match selected_namespace(&s, c.as_ref(), q.namespace.as_deref()) { Ok(n) => n, Err(r) => return r,
    };
    let tag = match selected_tag(&s,c.as_ref(),namespace.as_deref(),q.tag.as_deref()) { Ok(g)=>g,Err(r)=>return r,
    };
    match s.fleet.apps().await {
        Ok(mut v) => {
            if let Some(c) = &c {
                if let Some(grants) = &c.apps {
                    v.retain(|a| {
                        grants.contains(&format!("{}/{}", a["tenant"].as_str().unwrap_or_default(), a["elf_id"].as_str().unwrap_or_default()))
                    });
                }
            }
            if namespace.is_some() {
                v.retain(|a| a["namespace"].as_str() == namespace.as_deref());
                // Counts and ordering are namespace-local even after rotating tokens.
                let jobs = match s.fleet.recent_jobs(&ProofJobFilter { namespace, tag: tag.clone(), ..Default::default() }, i64::MAX as usize,
                    ).await {
                    Ok(j) => j, Err(e) => return internal(e),
                };
                for a in &mut v {
                    let own: Vec<_> = jobs.iter().filter(|j| {
                            Some(j.tenant.as_str()) == a["tenant"].as_str() && Some(j.elf_id.as_str()) == a["elf_id"].as_str()
                        }).collect();
                    a["jobs"] = serde_json::json!(own.len());
                    a["active"] = serde_json::json!(own.iter().filter(|j| !j.state().is_terminal()).count());
                    a["succeeded"] = serde_json::json!(own.iter().filter(|j| matches!(j.state(), crate::store::ProofJobState::Succeeded)).count());
                    a["failed"] = serde_json::json!(own.iter().filter(|j| matches!(j.state(), crate::store::ProofJobState::Failed)).count());
                    a["last_created_ms"] = serde_json::json!(own.iter().map(|j| j.created_ms).max());
                    a["mean_prove_ms"] = serde_json::Value::Null;
                    a["anchored_on"] = serde_json::Value::Null;
                }
                v.sort_by_key(|a| std::cmp::Reverse(a["last_created_ms"].as_u64().unwrap_or(0)));
            }
            v = v.into_iter().map(|app| s.workloads.proof_app_value(app, tag.as_deref())).collect();
            Json(v).into_response()
        }
        Err(e) => internal(e),
    }
}

/// An app by its exact key (both given; empty for the rejected requests' app).

fn app_parts(key: &ProofAppRequest, fleet: &Scheduler) -> Result<(String, String, String), Response> {
        match (&key.namespace, &key.tenant, &key.elf_id) {
            (Some(reference), Some(t), Some(e)) => match fleet.namespace(reference) {
                Some(n) => Ok((n.id, t.clone(), e.clone())),
                None => Err((StatusCode::NOT_FOUND, "unknown namespace; an administrator must create it first",
            ).into_response()),
            },
            _ => Err((StatusCode::BAD_REQUEST, "give namespace, tenant and elf_id (the full id, as GET /admin/apps lists it)",
        ).into_response()),
    }
}

fn authorize_app_management(c: Option<&Token>, key: &ProofAppRequest, namespace: &str, tenant: &str, elf: &str,
) -> Result<(), Response> {
    if let Some(c) = c {
        if !c.manage_apps || c.namespace.as_deref() != Some(namespace)
            || key.tag.as_deref().is_some_and(|tag| c.tag.as_deref() != Some(tag))
            || c.apps.as_ref().is_some_and(|apps| !apps.contains(&format!("{tenant}/{elf}"))) {
            return Err(StatusCode::FORBIDDEN.into_response());
        }
    }
    Ok(())
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

async fn delete_app(State(s): State<HttpState>, Extension(c): Extension<Option<Token>>, axum::extract::Query(k): axum::extract::Query<ProofAppRequest>,
) -> Response {
    match app_parts(&k, &s.fleet) {
        Ok((n, t, e)) => {
            if let Err(r) = authorize_app_management(c.as_ref(), &k, &n, &t, &e) { return r; }
            let _guard = match s.workloads.app_lifecycle_guard(&n, &format!("{t}/{e}")).await {
                Ok(guard) => guard, Err(error) => return app_outcome(Err(error)),
            };
            app_outcome(s.fleet.delete_app(&n, &t, &e).await)
        }
        Err(r) => r,
    }
}

async fn add_app(State(s): State<HttpState>, Extension(c): Extension<Option<Token>>, axum::extract::Query(k): axum::extract::Query<ProofAppRequest>,
) -> Response {
    let priority = k.priority.unwrap_or(0);
    if let Some(c) = c {
        if !c.manage_apps { return StatusCode::FORBIDDEN.into_response(); }
        match app_parts(&k, &s.fleet) {
            Ok((namespace, tenant, elf)) if k.tag.as_deref() == c.tag.as_deref()
                && authorize_app_management(Some(&c), &k, &namespace, &tenant, &elf).is_ok() => {}
            _ => return StatusCode::FORBIDDEN.into_response(),
        }
    }
    match app_parts(&k, &s.fleet) {
        Ok((n, t, e)) => app_outcome(s.fleet.add_app(&n, &t, &e, priority).await),
        Err(r) => r,
    }
}

async fn set_app_priority(State(s): State<HttpState>, Extension(c): Extension<Option<Token>>, axum::extract::Query(k): axum::extract::Query<ProofAppRequest>,
) -> Response {
    let Some(priority) = k.priority else { return (StatusCode::BAD_REQUEST, "priority is required").into_response(); };
    match app_parts(&k, &s.fleet) {
        Ok((n,t,e)) => {
            if let Err(r) = authorize_app_management(c.as_ref(), &k, &n, &t, &e) { return r; }
            if !s.fleet.app_known(&n, &t, &e) { return StatusCode::NOT_FOUND.into_response(); }
            app_outcome(s.fleet.set_app_priority(&n,&t,&e,priority).await)
        }
        Err(r) => r,
    }
}

async fn disable_app(State(s): State<HttpState>, Extension(c): Extension<Option<Token>>, axum::extract::Query(k): axum::extract::Query<ProofAppRequest>,
) -> Response {
    match app_parts(&k, &s.fleet) {
        Ok((n, t, e)) => {
            if let Err(r) = authorize_app_management(c.as_ref(), &k, &n, &t, &e) { return r; }
            let _guard = match s.workloads.app_lifecycle_guard(&n, &format!("{t}/{e}")).await {
                Ok(guard) => guard, Err(error) => return app_outcome(Err(error)),
            };
            app_outcome(s.fleet.set_app_disabled(&n, &t, &e, true).await)
        }
        Err(r) => r,
    }
}

async fn enable_app(State(s): State<HttpState>, Extension(c): Extension<Option<Token>>, axum::extract::Query(k): axum::extract::Query<ProofAppRequest>,
) -> Response {
    match app_parts(&k, &s.fleet) {
        Ok((n, t, e)) => {
            if let Err(r) = authorize_app_management(c.as_ref(), &k, &n, &t, &e) { return r; }
            let _guard = match s.workloads.app_lifecycle_guard(&n, &format!("{t}/{e}")).await {
                Ok(guard) => guard, Err(error) => return app_outcome(Err(error)),
            };
            app_outcome(s.fleet.set_app_disabled(&n, &t, &e, false).await)
        }
        Err(r) => r,
    }
}

async fn jobs(State(s): State<HttpState>, Extension(c): Extension<Option<Token>>, axum::extract::Query(q): axum::extract::Query<ProofJobsQuery>,
) -> Response {
    let namespace = match selected_namespace(&s, c.as_ref(), q.namespace.as_deref()) { Ok(n) => n, Err(r) => return r,
    };
    let tag = match selected_tag(&s,c.as_ref(),namespace.as_deref(),q.tag.as_deref()) { Ok(g)=>g,Err(r)=>return r,
    };
    match s.fleet.recent_jobs(&ProofJobFilter { namespace, tag, owner: None, tenant: q.tenant, elf: q.elf, active: q.active,
            }, q.limit.min(1000) as usize,
        ).await {
        Ok(v) => Json(v.iter().map(|j| s.workloads.proof_job_value(j)).collect::<Vec<_>>(),
        ).into_response(),
        Err(e) => internal(e),
    }
}

async fn job(State(s): State<HttpState>, Extension(c): Extension<Option<Token>>, Path(id): Path<String>, axum::extract::Query(q): axum::extract::Query<NamespaceSelector>,
) -> Response {
    match scoped_job(&s, c.as_ref(), &id, &q).await {
        Ok(j) => Json(s.workloads.proof_job_value(&j)).into_response(),
        Err(r) => r,
    }
}

async fn scoped_job(s: &HttpState, c: Option<&Token>, id: &str, q: &NamespaceSelector,
) -> Result<crate::store::ProofJob, Response> {
    let namespace = selected_namespace(s, c, q.namespace.as_deref())?;
    let tag = selected_tag(s,c,namespace.as_deref(),q.tag.as_deref())?;
    let resolved = if let Some(n) = &namespace {
        match s.fleet.namespace_job(n, tag.as_deref().filter(|t|!t.is_empty()), &id).await {
            Ok(Some(j)) => Ok(Some(j)),
            Ok(None) => s.fleet.job(&id).await,
            Err(e) => Err(e),
        }
    } else { s.fleet.job(&id).await };
    match resolved {
        Ok(Some(j)) if namespace.as_ref().is_none_or(|n| &j.namespace == n) && tag.as_ref().is_none_or(|g|&j.tag==g) =>
        {
            Ok(j)
        }
        Ok(_) => Err(StatusCode::NOT_FOUND.into_response()),
        Err(e) => Err(internal(e)),
    }
}

async fn proof_output(State(s): State<HttpState>, Extension(c): Extension<Option<Token>>, Path((id,name)): Path<(String,String)>, axum::extract::Query(q): axum::extract::Query<NamespaceSelector>,
) -> Response {
    if !matches!(name.as_str(), blobs::PROOF | blobs::PUBLIC_VALUES) { return StatusCode::NOT_FOUND.into_response(); }
    let j = match scoped_job(&s,c.as_ref(),&id,&q).await { Ok(j)=>j, Err(r)=>return r,
    };
    if c.as_ref().is_some_and(|c| {
        c.apps.as_ref().is_some_and(|apps| !apps.contains(&format!("{}/{}", j.tenant, j.elf_id)))
    }) {
        return StatusCode::NOT_FOUND.into_response();
    }
    if j.state() != crate::store::ProofJobState::Succeeded { return (StatusCode::CONFLICT,"proof has not succeeded").into_response(); }
    match s.fleet.proof_output(&j, if name == blobs::PROOF {blobs::PROOF} else {blobs::PUBLIC_VALUES},
        ).await {
        Ok(bytes) => ([(axum::http::header::CONTENT_TYPE,"application/octet-stream")],bytes,
        ).into_response(),
        Err(e) => crate::proof_fetch::response_error(e),
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
