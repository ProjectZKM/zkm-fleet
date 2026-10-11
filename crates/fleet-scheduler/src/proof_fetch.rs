//! Client-triggered proof fetches, delivered through ordinary machine heartbeats.
//! Bytes are relayed in bounded memory and are never written to control-host storage.
use axum::{
    body::to_bytes,
    extract::{Path, Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Router,
};
use fleet_proto::{artifacts, fleet::v1::ProofFetch};
use parking_lot::Mutex;
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use tokio::sync::{oneshot, Semaphore};
use tonic::Status;

struct Worker {
    id: String,
    session: String,
    seen: Instant,
}
struct Pending {
    machine: String,
    worker: String,
    session: String,
    request: ProofFetch,
    claimed: bool,
    reply: Option<oneshot::Sender<Result<Vec<u8>, Status>>>,
}
#[derive(Default)]
struct Inner {
    workers: HashMap<String, Worker>,
    pending: HashMap<String, Pending>,
}
pub struct Relay {
    inner: Mutex<Inner>,
    slots: Semaphore,
}
impl Default for Relay {
    fn default() -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            slots: Semaphore::new(4),
        }
    }
}

/// Pending requests live only as long as the client's request, including cancellation.
struct FetchGuard<'a> {
    relay: &'a Relay,
    ticket: String,
}
impl Drop for FetchGuard<'_> {
    fn drop(&mut self) {
        self.relay.inner.lock().pending.remove(&self.ticket);
    }
}

impl Relay {
    pub fn heartbeat(
        &self,
        machine: &str,
        worker: &str,
        session: &str,
        ready: bool,
    ) -> Vec<ProofFetch> {
        let mut inner = self.inner.lock();
        // Fence tickets on process restart or a change of the handler's capability.
        let invalid: Vec<_> = inner
            .pending
            .iter()
            .filter(|(_, p)| {
                p.machine == machine && (!ready || p.worker != worker || p.session != session)
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in invalid {
            if let Some(mut p) = inner.pending.remove(&id) {
                if let Some(reply) = p.reply.take() {
                    let _ = reply.send(Err(Status::unavailable(
                        "proof fetch worker restarted; retry",
                    )));
                }
            }
        }
        if !ready {
            inner.workers.remove(machine);
            return vec![];
        }
        inner.workers.insert(
            machine.into(),
            Worker {
                id: worker.into(),
                session: session.into(),
                seen: Instant::now(),
            },
        );
        inner
            .pending
            .values()
            .filter(|p| p.worker == worker && p.session == session && !p.claimed)
            .map(|p| p.request.clone())
            .collect()
    }

    pub async fn fetch(&self, machine: &str, mut request: ProofFetch) -> Result<Vec<u8>, Status> {
        let _slot = self
            .slots
            .try_acquire()
            .map_err(|_| Status::resource_exhausted("proof fetch capacity reached; retry"))?;
        let reference = request
            .reference
            .as_ref()
            .ok_or_else(|| Status::internal("output reference missing"))?;
        artifacts::validate(reference, artifacts::MAX_OUTPUT)
            .map_err(|e| Status::internal(e.to_string()))?;
        let (tx, rx) = oneshot::channel();
        request.ticket = format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let guard = FetchGuard {
            relay: self,
            ticket: request.ticket.clone(),
        };
        {
            let mut inner = self.inner.lock();
            let worker = inner
                .workers
                .get(machine)
                .filter(|w| w.seen.elapsed() < Duration::from_secs(30))
                .ok_or_else(|| {
                    Status::unavailable(
                        "proof storage machine is offline or its fetch handler is not configured",
                    )
                })?;
            let worker_id = worker.id.clone();
            let session = worker.session.clone();
            inner.pending.insert(
                guard.ticket.clone(),
                Pending {
                    machine: machine.into(),
                    worker: worker_id,
                    session,
                    request,
                    claimed: false,
                    reply: Some(tx),
                },
            );
        }
        tokio::time::timeout(Duration::from_secs(60), rx)
            .await
            .map_err(|_| Status::deadline_exceeded("proof fetch timed out"))?
            .map_err(|_| Status::unavailable("proof fetch ended"))?
    }

    fn claim(
        &self,
        ticket: &str,
        worker: &str,
        session: &str,
    ) -> Result<(ProofFetch, oneshot::Sender<Result<Vec<u8>, Status>>), StatusCode> {
        let mut inner = self.inner.lock();
        let pending = inner.pending.get_mut(ticket).ok_or(StatusCode::NOT_FOUND)?;
        if pending.worker != worker || pending.session != session {
            return Err(StatusCode::FORBIDDEN);
        }
        if pending.claimed {
            return Err(StatusCode::CONFLICT);
        }
        pending.claimed = true;
        Ok((pending.request.clone(), pending.reply.take().unwrap()))
    }

    fn finish(
        &self,
        ticket: &str,
        reply: oneshot::Sender<Result<Vec<u8>, Status>>,
        bytes: Vec<u8>,
    ) -> StatusCode {
        let inner = self.inner.lock();
        let current = inner.pending.get(ticket).is_some_and(|p| {
            inner
                .workers
                .get(&p.machine)
                .is_some_and(|w| w.id == p.worker && w.session == p.session)
        });
        // Check fencing and deliver while holding the same lock as heartbeat updates.
        if !current {
            let _ = reply.send(Err(Status::unavailable(
                "proof fetch worker restarted or client disconnected",
            )));
            return StatusCode::GONE;
        }
        match reply.send(Ok(bytes)) {
            Ok(()) => StatusCode::NO_CONTENT,
            Err(_) => StatusCode::GONE,
        }
    }
}

pub fn router(state: crate::http::HttpState) -> Router {
    Router::new()
        .route("/internal/proof-fetch/:ticket", post(receive))
        .with_state(state)
}

async fn receive(
    State(s): State<crate::http::HttpState>,
    Path(ticket): Path<String>,
    request: Request,
) -> Response {
    if !s.fleet.cluster_authorized(
        request
            .headers()
            .get(crate::token::HEADER)
            .and_then(|h| h.to_str().ok()),
    ) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let (worker, session, error) = {
        let header = |name| {
            request
                .headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_owned()
        };
        (
            header("x-fleet-worker"),
            header("x-fleet-session"),
            header("x-fleet-fetch-error"),
        )
    };
    let (fetch, mut reply) = match s.fleet.proof_fetch.claim(&ticket, &worker, &session) {
        Ok(p) => p,
        Err(code) => return code.into_response(),
    };
    if !error.is_empty() {
        let code = match error.as_str() {
            "missing" => tonic::Code::NotFound,
            "invalid" => tonic::Code::DataLoss,
            _ => tonic::Code::Unavailable,
        };
        let _ = reply.send(Err(Status::new(
            code,
            "proof storage could not serve the committed artifact",
        )));
        return StatusCode::NO_CONTENT.into_response();
    }
    let reference = fetch.reference.unwrap();
    let result = tokio::select! {
        _ = reply.closed() => return StatusCode::GONE.into_response(),
        result = tokio::time::timeout(Duration::from_secs(30), to_bytes(request.into_body(), reference.size as usize)) => result,
    };
    let bytes = match result {
        Ok(Ok(bytes))
            if bytes.len() as u64 == reference.size
                && artifacts::hash(&bytes) == reference.sha256 =>
        {
            bytes.to_vec()
        }
        _ => {
            let _ = reply.send(Err(Status::data_loss(
                "proof fetch size or hash mismatch, or incomplete transfer",
            )));
            return StatusCode::BAD_GATEWAY.into_response();
        }
    };
    s.fleet
        .proof_fetch
        .finish(&ticket, reply, bytes)
        .into_response()
}

pub fn response_error(error: Status) -> Response {
    let code = StatusCode::from_u16(fleet_proto::sdk::http_status_code(error.code())).unwrap();
    (code, error.message().to_owned()).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        blobs::Blobs, config::Config, fleet::Fleet, http::HttpState, runs_api::RunControl,
        store::MemoryStore,
    };
    use std::{future::IntoFuture, sync::Arc};
    const SECRET: &str = "0123456789abcdef0123456789abcdef";

    fn request(bytes: &[u8]) -> ProofFetch {
        ProofFetch {
            job_id: "job".into(),
            lease: "1-2".into(),
            name: "proof.json".into(),
            reference: Some(fleet_proto::fleet::v1::ArtifactRef {
                uri: "file:///owner/outputs/job/1-2/proof.json".into(),
                sha256: artifacts::hash(bytes),
                size: bytes.len() as u64,
            }),
            ..Default::default()
        }
    }

    async fn queued(relay: &Relay) -> ProofFetch {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let commands = relay.heartbeat("gpu-machine", "worker", "session", true);
            if let Some(c) = commands.first() {
                return c.clone();
            }
            assert!(Instant::now() < deadline);
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn tickets_are_session_bound_replayed_until_claimed_and_cancelled_with_the_client() {
        let relay = Arc::new(Relay::default());
        assert_eq!(
            relay
                .fetch("gpu-machine", request(b"proof"))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unavailable
        );
        relay.heartbeat("gpu-machine", "worker", "session", true);
        let r = relay.clone();
        let task = tokio::spawn(async move { r.fetch("gpu-machine", request(b"proof")).await });
        let fetch = queued(&relay).await;
        assert_eq!(queued(&relay).await, fetch);
        assert_eq!(
            relay.claim(&fetch.ticket, "other", "session").unwrap_err(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            relay.claim(&fetch.ticket, "worker", "stale").unwrap_err(),
            StatusCode::FORBIDDEN
        );
        let (_, reply) = relay.claim(&fetch.ticket, "worker", "session").unwrap();
        assert!(relay
            .heartbeat("gpu-machine", "worker", "session", true)
            .is_empty());
        assert_eq!(
            relay.claim(&fetch.ticket, "worker", "session").unwrap_err(),
            StatusCode::CONFLICT
        );
        reply.send(Ok(b"proof".to_vec())).unwrap();
        assert_eq!(task.await.unwrap().unwrap(), b"proof");
        assert_eq!(
            relay.claim(&fetch.ticket, "worker", "session").unwrap_err(),
            StatusCode::NOT_FOUND
        );
        let r = relay.clone();
        let task = tokio::spawn(async move { r.fetch("gpu-machine", request(b"proof")).await });
        let fetch = queued(&relay).await;
        task.abort();
        let _ = task.await;
        assert_eq!(
            relay.claim(&fetch.ticket, "worker", "session").unwrap_err(),
            StatusCode::NOT_FOUND
        );
        assert!(relay.inner.lock().pending.is_empty());
        let r = relay.clone();
        let task = tokio::spawn(async move { r.fetch("gpu-machine", request(b"proof")).await });
        queued(&relay).await;
        relay.heartbeat("gpu-machine", "worker", "new-session", true);
        assert_eq!(
            task.await.unwrap().unwrap_err().code(),
            tonic::Code::Unavailable
        );
        assert!(relay.inner.lock().pending.is_empty());
    }

    #[tokio::test]
    async fn relay_checks_cluster_auth_fencing_size_hash_and_single_use_without_files() {
        let dir = tempfile::tempdir().unwrap();
        let cfg: Config = serde_yaml::from_str(&format!("public_url: http://unused\ndata_dir: {}\ncluster_token_file: unused\nadmin_token_file: unused\ntenants:\n - name: proof\n",dir.path().display())).unwrap();
        let fleet = Fleet::new(
            cfg,
            Arc::new(MemoryStore::default()),
            Blobs::new(dir.path()).unwrap(),
            SECRET.into(),
        );
        let state = HttpState {
            fleet: fleet.clone(),
            admin_token: Arc::new(SECRET.into()),
            runs: Arc::new(RunControl::new(fleet.clone(), SECRET.into(), SECRET.into()).unwrap()),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(axum::serve(listener, router(state)).into_future());
        let client = reqwest::Client::new();
        let post = |ticket: &str| {
            client
                .post(format!("{url}/internal/proof-fetch/{ticket}"))
                .bearer_auth(SECRET)
                .header("x-fleet-worker", "worker")
                .header("x-fleet-session", "session")
        };
        assert_eq!(
            client
                .post(format!("{url}/internal/proof-fetch/unknown"))
                .send()
                .await
                .unwrap()
                .status(),
            401
        );
        fleet
            .proof_fetch
            .heartbeat("gpu-machine", "worker", "session", true);
        // Also exercise an output larger than Axum's default JSON body limit.
        for (expected, sent, code) in [
            (b"proof".to_vec(), b"proof".to_vec(), 204),
            (b"proof".to_vec(), b"wrong".to_vec(), 502),
            (b"proof".to_vec(), b"shorter-too-long".to_vec(), 502),
            (vec![7; 3 << 20], vec![7; 3 << 20], 204),
        ] {
            let f = fleet.clone();
            let expected_copy = expected.clone();
            let task = tokio::spawn(async move {
                f.proof_fetch
                    .fetch("gpu-machine", request(&expected_copy))
                    .await
            });
            let fetch = queued(&fleet.proof_fetch).await;
            assert_eq!(
                client
                    .post(format!("{url}/internal/proof-fetch/{}", fetch.ticket))
                    .bearer_auth(SECRET)
                    .header("x-fleet-worker", "worker")
                    .header("x-fleet-session", "wrong")
                    .body(sent.clone())
                    .send()
                    .await
                    .unwrap()
                    .status(),
                403
            );
            assert_eq!(
                post(&fetch.ticket)
                    .body(sent)
                    .send()
                    .await
                    .unwrap()
                    .status(),
                code
            );
            let result = task.await.unwrap();
            if code == 204 {
                assert_eq!(result.unwrap(), expected);
            } else {
                assert_eq!(result.unwrap_err().code(), tonic::Code::DataLoss);
            }
            assert_eq!(
                post(&fetch.ticket)
                    .body(b"proof".to_vec())
                    .send()
                    .await
                    .unwrap()
                    .status(),
                404
            );
        }
        assert!(!dir.path().join("proofs").exists());
        server.abort();
    }
}
