//! One-shot private delivery to outbound node agents. Disk and heartbeats hold
//! metadata only. A dispatched payload is never automatically sent a second time.
use crate::{
    policy_gateway::identity,
    workloads::{atomic, WorkloadController},
};
use axum::{
    body::to_bytes,
    extract::{Path, Request, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{post, put},
    Json, Router,
};
use fleet_proto::model::v1::{RuntimeDeliveryAck, RuntimeDeliveryOffer, RuntimeDeliveryRecord};
use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt},
    path::PathBuf,
    sync::Arc,
};
use trusted_shim::{
    provisioning::{ProvisionRequest, MAX_PROVISION_BYTES},
    rail::{Error, Result},
};

#[derive(Default)]
struct QueueState {
    records: BTreeMap<String, RuntimeDeliveryRecord>,
    payloads: BTreeMap<String, Vec<u8>>,
}
pub(crate) struct DeliveryQueue {
    dir: PathBuf,
    inner: Mutex<QueueState>,
    permits: Arc<tokio::sync::Semaphore>,
}
fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
fn valid_ticket(id: &str) -> bool {
    id.len() == 64
        && id
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}
impl DeliveryQueue {
    pub(crate) fn open(dir: &std::path::Path) -> anyhow::Result<Self> {
        match std::fs::DirBuilder::new().mode(0o700).create(dir) {
            Ok(()) => (),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => (),
            Err(e) => return Err(e.into()),
        }
        let m = std::fs::symlink_metadata(dir)?;
        anyhow::ensure!(
            m.is_dir()
                && m.uid() == unsafe { libc::geteuid() }
                && m.permissions().mode() & 0o077 == 0,
            "private delivery directory required"
        );
        let mut inner = QueueState::default();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            if entry.path().extension().is_none_or(|e| e != "json") {
                continue;
            }
            let m = std::fs::symlink_metadata(entry.path())?;
            anyhow::ensure!(
                m.is_file()
                    && m.len() <= 8192
                    && m.uid() == unsafe { libc::geteuid() }
                    && m.permissions().mode() & 0o077 == 0,
                "invalid delivery record"
            );
            let r: RuntimeDeliveryRecord = serde_json::from_slice(&std::fs::read(entry.path())?)?;
            anyhow::ensure!(
                valid_ticket(&r.offer.ticket)
                    && valid_ticket(&r.offer.payload_sha256)
                    && entry.path().file_stem().and_then(|s| s.to_str()) == Some(&r.offer.ticket)
                    && matches!(
                        r.state.as_str(),
                        "queued"
                            | "dispatched"
                            | "acknowledged"
                            | "uncertain"
                            | "cancelled"
                            | "expired"
                    ),
                "invalid delivery record"
            );
            inner.records.insert(r.offer.ticket.clone(), r);
        }
        Ok(Self {
            dir: dir.into(),
            inner: Mutex::new(inner),
            permits: Arc::new(tokio::sync::Semaphore::new(16)),
        })
    }
    fn save(&self, r: &RuntimeDeliveryRecord) -> Result<()> {
        atomic(
            &self.dir.join(format!("{}.json", r.offer.ticket)),
            &serde_json::to_vec(r).map_err(|_| Error::InvalidInput)?,
        )
        .map_err(|_| Error::Unavailable)
    }
    fn expire(&self, i: &mut QueueState) -> Result<()> {
        for r in i
            .records
            .values_mut()
            .filter(|r| r.state == "queued" && r.expires_at <= now())
        {
            let mut expired = r.clone();
            expired.state = "expired".into();
            self.save(&expired)?;
            *r = expired;
            i.payloads.remove(&r.offer.ticket);
        }
        Ok(())
    }
    fn stage(&self, mut r: RuntimeDeliveryRecord, bytes: Vec<u8>) -> Result<RuntimeDeliveryRecord> {
        let mut i = self.inner.lock();
        self.expire(&mut i)?;
        if let Some(old) = i.records.get(&r.offer.ticket) {
            if old.offer != r.offer || old.worker != r.worker || old.expires_at != r.expires_at {
                return Err(Error::Conflict);
            }
            if old.state != "queued" {
                return Ok(old.clone());
            }
            r = old.clone();
        } else if i.records.values().any(|old| {
            old.offer.execution == r.offer.execution
                && matches!(old.state.as_str(), "queued" | "dispatched" | "uncertain")
        }) {
            return Err(Error::Conflict);
        }
        if i.payloads.len() >= 16 && !i.payloads.contains_key(&r.offer.ticket) {
            return Err(Error::Unavailable);
        }
        self.save(&r)?; // Crash here leaves a queued record awaiting the exact payload.
        i.records.insert(r.offer.ticket.clone(), r.clone());
        i.payloads.insert(r.offer.ticket.clone(), bytes);
        Ok(r)
    }
    pub(crate) fn record(&self, ticket: &str) -> Result<RuntimeDeliveryRecord> {
        let mut i = self.inner.lock();
        self.expire(&mut i)?;
        i.records.get(ticket).cloned().ok_or(Error::Rejected)
    }
    pub(crate) fn offers(&self, worker: &str) -> Vec<RuntimeDeliveryOffer> {
        let mut i = self.inner.lock();
        if self.expire(&mut i).is_err() {
            return vec![];
        }
        i.records
            .values()
            .filter(|r| {
                r.worker == worker
                    && r.state == "queued"
                    && i.payloads.contains_key(&r.offer.ticket)
            })
            .take(2)
            .map(|r| r.offer.clone())
            .collect()
    }
    fn payload(&self, ticket: &str) -> Result<Vec<u8>> {
        self.inner
            .lock()
            .payloads
            .get(ticket)
            .cloned()
            .ok_or(Error::Unavailable)
    }
    fn claim(&self, ticket: &str, worker: &str, session: &str) -> Result<Vec<u8>> {
        let mut i = self.inner.lock();
        self.expire(&mut i)?;
        let mut r = i.records.get(ticket).cloned().ok_or(Error::Rejected)?;
        if r.worker != worker || session.is_empty() {
            return Err(Error::Rejected);
        }
        if r.state != "queued" {
            return Err(Error::Conflict);
        }
        if !i.payloads.contains_key(ticket) {
            return Err(Error::Unavailable);
        }
        r.state = "dispatched".into();
        r.claimed_session = Some(session.into());
        // Fence memory too if rename succeeded but fsync reports an error.
        // No later request in this process may dispatch an ambiguous write.
        i.records.insert(ticket.into(), r.clone());
        let bytes = i.payloads.remove(ticket).unwrap();
        self.save(&r)?; // Persist uncertainty BEFORE any credential leaves the process.
        Ok(bytes)
    }
    fn ack(
        &self,
        ticket: &str,
        worker: &str,
        session: &str,
        accepted: bool,
    ) -> Result<RuntimeDeliveryRecord> {
        let mut i = self.inner.lock();
        let mut r = i.records.get(ticket).cloned().ok_or(Error::Rejected)?;
        if r.worker != worker || r.claimed_session.as_deref() != Some(session) {
            return Err(Error::Rejected);
        }
        let state = if accepted {
            "acknowledged"
        } else {
            "uncertain"
        };
        if r.state == state {
            return Ok(r);
        }
        if r.state != "dispatched" {
            return Err(Error::Conflict);
        }
        r.state = state.into();
        self.save(&r)?;
        i.records.insert(ticket.into(), r.clone());
        Ok(r)
    }
    fn cancel(&self, ticket: &str) -> Result<RuntimeDeliveryRecord> {
        let mut i = self.inner.lock();
        self.expire(&mut i)?;
        let mut r = i.records.get(ticket).cloned().ok_or(Error::Rejected)?;
        if r.state != "queued" && r.state != "cancelled" {
            return Err(Error::Conflict);
        }
        r.state = "cancelled".into();
        self.save(&r)?;
        i.records.insert(ticket.into(), r.clone());
        i.payloads.remove(ticket);
        Ok(r)
    }
}
fn failure(e: Error) -> Response {
    let code = match e {
        Error::InvalidInput | Error::Unsupported | Error::EvaluationFailed => {
            StatusCode::BAD_REQUEST
        }
        Error::Rejected => StatusCode::FORBIDDEN,
        Error::Conflict => StatusCode::CONFLICT,
        Error::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
    };
    (code, "runtime delivery rejected or unavailable").into_response()
}
fn private(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    response
}
pub(crate) fn admin_router(c: Arc<WorkloadController>) -> Router {
    if c.policy.is_none() {
        return Router::new();
    }
    Router::new()
        .route(
            "/policy/runtime-deliveries/:ticket",
            put(stage).get(status).delete(cancel),
        )
        .with_state(c)
}
pub(crate) fn node_router(c: Arc<WorkloadController>) -> Router {
    if c.policy.is_none() {
        return Router::new();
    }
    Router::new()
        .route("/internal/runtime-deliveries/:ticket/claim", post(claim))
        .route("/internal/runtime-deliveries/:ticket/ack", post(ack))
        .with_state(c)
}
pub(crate) fn stage_payload(
    c: &WorkloadController,
    ticket: String,
    p: ProvisionRequest,
) -> Result<RuntimeDeliveryRecord> {
    if !valid_ticket(&ticket) {
        return Err(Error::InvalidInput);
    }
    p.validate().map_err(|_| Error::InvalidInput)?;
    // Bind gateway-native bytes to the exact proved request before any
    // credentials enter delivery. Shim repeats this before central Consume.
    p.validate_native().map_err(|_| Error::Rejected)?;
    let bytes = trusted_shim::rail::rego::canonical_bytes(
        &serde_json::to_value(&p).map_err(|_| Error::InvalidInput)?,
    )?;
    let e = &p.request.execution;
    c.with_runtime_delivery(e, None, Some(&p.request), |worker| {
        let offer = RuntimeDeliveryOffer {
            ticket,
            execution: serde_json::from_value(
                serde_json::to_value(e).map_err(|_| Error::InvalidInput)?,
            )
            .map_err(|_| Error::InvalidInput)?,
            payload_sha256: hex::encode(Sha256::digest(&bytes)),
        };
        c.deliveries.stage(
            RuntimeDeliveryRecord {
                offer,
                worker: worker.into(),
                expires_at: p.authorization.validity.expires_at,
                state: "queued".into(),
                claimed_session: None,
            },
            bytes,
        )
    })
}


async fn stage(
    State(c): State<Arc<WorkloadController>>,
    Path(ticket): Path<String>,
    request: Request,
) -> Response {
    if !valid_ticket(&ticket) {
        return failure(Error::InvalidInput);
    }
    let Ok(permit) = c.deliveries.permits.clone().try_acquire_owned() else {
        return failure(Error::Unavailable);
    };
    let Ok(bytes) = to_bytes(request.into_body(), MAX_PROVISION_BYTES).await else {
        return failure(Error::InvalidInput);
    };
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let p: ProvisionRequest =
            serde_json::from_slice(&bytes).map_err(|_| Error::InvalidInput)?;
        stage_payload(&c, ticket, p)
    })
    .await;
    private(match result {
        Ok(Ok(r)) => Json(r).into_response(),
        Ok(Err(e)) => failure(e),
        Err(_) => failure(Error::Unavailable),
    })
}
async fn status(State(c): State<Arc<WorkloadController>>, Path(ticket): Path<String>) -> Response {
    private(match c.deliveries.record(&ticket) {
        Ok(r) => {
            let awaiting_payload = r.state == "queued" && c.deliveries.payload(&ticket).is_err();
            Json(serde_json::json!({"record":r,"awaiting_payload":awaiting_payload}))
                .into_response()
        }
        Err(e) => failure(e),
    })
}
async fn cancel(State(c): State<Arc<WorkloadController>>, Path(ticket): Path<String>) -> Response {
    private(match c.deliveries.cancel(&ticket) {
        Ok(r) => Json(r).into_response(),
        Err(e) => failure(e),
    })
}
fn node(c: &WorkloadController, request: &Request) -> Result<(String, String)> {
    let h = |name| {
        request
            .headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
    };
    if !c.fleet().cluster_authorized(Some(h(crate::token::HEADER))) {
        return Err(Error::Rejected);
    }
    let worker = h("x-fleet-worker");
    let session = h("x-fleet-session");
    if worker.is_empty() || session.is_empty() || worker.len() > 256 || session.len() > 256 {
        return Err(Error::Rejected);
    }
    Ok((worker.into(), session.into()))
}
async fn claim(
    State(c): State<Arc<WorkloadController>>,
    Path(ticket): Path<String>,
    request: Request,
) -> Response {
    let (worker, session) = match node(&c, &request) {
        Ok(v) => v,
        Err(e) => return failure(e),
    };
    let Ok(permit) = c.deliveries.permits.clone().try_acquire_owned() else {
        return failure(Error::Unavailable);
    };
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let r = c.deliveries.record(&ticket)?;
        if r.worker != worker {
            return Err(Error::Rejected);
        }
        let bytes = c.deliveries.payload(&ticket)?;
        let p: ProvisionRequest =
            serde_json::from_slice(&bytes).map_err(|_| Error::InvalidInput)?;
        p.validate_native().map_err(|_| Error::Rejected)?;
        c.with_runtime_delivery(
            &identity(r.offer.execution),
            Some((&worker, &session)),
            Some(&p.request),
            |_| c.deliveries.claim(&ticket, &worker, &session),
        )
    })
    .await;
    private(match result {
        Ok(Ok(bytes)) => ([(header::CONTENT_TYPE, "application/json")], bytes).into_response(),
        Ok(Err(e)) => failure(e),
        Err(_) => failure(Error::Unavailable),
    })
}
async fn ack(
    State(c): State<Arc<WorkloadController>>,
    Path(ticket): Path<String>,
    request: Request,
) -> Response {
    let (worker, session) = match node(&c, &request) {
        Ok(v) => v,
        Err(e) => return failure(e),
    };
    let Ok(bytes) = to_bytes(request.into_body(), 1024).await else {
        return failure(Error::InvalidInput);
    };
    let Ok(ack) = serde_json::from_slice::<RuntimeDeliveryAck>(&bytes) else {
        return failure(Error::InvalidInput);
    };
    let Ok(permit) = c.deliveries.permits.clone().try_acquire_owned() else {
        return failure(Error::Unavailable);
    };
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let r = c.deliveries.record(&ticket)?;
        c.with_runtime_delivery(
            &identity(r.offer.execution),
            Some((&worker, &session)),
            None,
            |_| c.deliveries.ack(&ticket, &worker, &session, ack.accepted),
        )
    })
    .await;
    private(match result {
        Ok(Ok(r)) => Json(r).into_response(),
        Ok(Err(e)) => failure(e),
        Err(_) => failure(Error::Unavailable),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn record(ticket: &str) -> RuntimeDeliveryRecord {
        RuntimeDeliveryRecord {
            offer: RuntimeDeliveryOffer { ticket: ticket.repeat(64), payload_sha256:"c".repeat(64),
                execution: serde_json::from_value(serde_json::json!({"namespace":"tenant","workload":"agent","allocation_id":"agent-0","node_id":"machine","lease_id":"lease","generation":1})).unwrap() },
            worker:"worker".into(), expires_at:now()+60, state:"queued".into(), claimed_session:None,
        }
    }
    #[test]
    fn restart_restores_only_metadata_and_claims_are_never_redelivered() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("deliveries");
        let r = record("a");
        let ticket = &r.offer.ticket;
        let payload = b"synthetic-private-launch-credential".to_vec();
        let q = DeliveryQueue::open(&path).unwrap();
        q.stage(r.clone(), payload.clone()).unwrap();
        for e in std::fs::read_dir(&path).unwrap() {
            assert!(!std::fs::read_to_string(e.unwrap().path())
                .unwrap()
                .contains("synthetic-private"));
        }
        drop(q);
        let q = DeliveryQueue::open(&path).unwrap();
        assert!(q.offers("worker").is_empty());
        assert!(q.claim(ticket, "worker", "session").is_err());
        let mut changed = r.clone();
        changed.offer.payload_sha256 = "d".repeat(64);
        assert!(q.stage(changed, payload.clone()).is_err());
        q.stage(r.clone(), payload.clone()).unwrap();
        assert_eq!(q.offers("worker").len(), 1);
        assert!(q.claim(ticket, "other-worker", "session").is_err());
        assert_eq!(q.claim(ticket, "worker", "session").unwrap(), payload);
        assert!(q.claim(ticket, "worker", "session").is_err());
        drop(q);
        let q = DeliveryQueue::open(&path).unwrap();
        assert_eq!(q.record(ticket).unwrap().state, "dispatched");
        assert_eq!(q.stage(r, payload).unwrap().state, "dispatched");
        assert!(q.offers("worker").is_empty());
        assert!(q
            .ack(&"a".repeat(64), "worker", "replacement", true)
            .is_err());
        q.ack(&"a".repeat(64), "worker", "session", true).unwrap();
        q.ack(&"a".repeat(64), "worker", "session", true).unwrap();
        assert!(q.ack(&"a".repeat(64), "worker", "session", false).is_err());
    }
    #[test]
    fn cancellation_expiry_and_concurrent_claim_preserve_tombstones() {
        let dir = tempfile::tempdir().unwrap();
        let q = Arc::new(DeliveryQueue::open(&dir.path().join("deliveries")).unwrap());
        let r = record("a");
        let ticket = r.offer.ticket.clone();
        q.stage(r.clone(), vec![1]).unwrap();
        q.cancel(&ticket).unwrap();
        assert_eq!(q.stage(r, vec![1]).unwrap().state, "cancelled");
        assert!(q.claim(&ticket, "worker", "session").is_err());
        let r = record("b");
        let ticket = r.offer.ticket.clone();
        q.stage(r, vec![1]).unwrap();
        let tasks: Vec<_> = (0..8)
            .map(|_| {
                let q = q.clone();
                let t = ticket.clone();
                std::thread::spawn(move || q.claim(&t, "worker", "session").is_ok())
            })
            .collect();
        assert_eq!(
            tasks
                .into_iter()
                .map(|h| h.join().unwrap() as usize)
                .sum::<usize>(),
            1
        );
        let mut expired = record("e");
        expired.offer.execution.generation = 2;
        expired.expires_at = now() - 1;
        q.stage(expired, vec![1]).unwrap();
        assert!(q.claim(&"e".repeat(64), "worker", "session").is_err());
        assert_eq!(q.record(&"e".repeat(64)).unwrap().state, "expired");
    }
    #[test]
    fn failed_durable_claim_drops_payload_and_cannot_be_retried_in_memory() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("deliveries");
        let q = DeliveryQueue::open(&path).unwrap();
        let r = record("a");
        let ticket = r.offer.ticket.clone();
        q.stage(r, vec![1]).unwrap();
        std::fs::rename(&path, dir.path().join("saved")).unwrap();
        std::fs::write(&path, "not a directory").unwrap();
        assert!(q.claim(&ticket, "worker", "session").is_err());
        assert_eq!(q.record(&ticket).unwrap().state, "dispatched");
        assert!(q.payload(&ticket).is_err());
        assert!(q.offers("worker").is_empty());
    }
}
