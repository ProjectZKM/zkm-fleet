//! Client-triggered reads of an app service's log, delivered through ordinary machine
//! heartbeats. A request lives only as long as the client's call; nothing is stored here.
use axum::{
    body::to_bytes,
    extract::{Path, Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Router,
};
use fleet_proto::fleet::v1::LogRequest;
use parking_lot::Mutex;
use std::{collections::HashMap, time::Duration};
use tokio::sync::{oneshot, Semaphore};
use tonic::Status;

/// The most log bytes one reply carries.
pub const MAX_LOG_BYTES: usize = 256 * 1024;
pub const MAX_LOG_LINES: u32 = 2000;

struct Pending {
    worker: String,
    session: String,
    request: LogRequest,
    delivered: bool,
    reply: Option<oneshot::Sender<Result<Vec<u8>, Status>>>,
}
#[derive(Default)]
struct State_ {
    sessions: HashMap<String, String>,
    pending: HashMap<String, Pending>,
}
pub struct LogRelay {
    inner: Mutex<State_>,
    slots: Semaphore,
}
impl Default for LogRelay {
    fn default() -> Self {
        Self {
            inner: Mutex::new(State_::default()),
            slots: Semaphore::new(8),
        }
    }
}

/// Drops the request when the client's call ends or is cancelled.
struct Guard<'a> {
    relay: &'a LogRelay,
    ticket: String,
}
impl Drop for Guard<'_> {
    fn drop(&mut self) {
        self.relay.inner.lock().pending.remove(&self.ticket);
    }
}

impl LogRelay {
    /// Records the worker's current session and returns the requests it has not yet seen.
    /// A request made to an earlier session of the same worker fails: that agent restarted.
    pub fn heartbeat(&self, worker: &str, session: &str) -> Vec<LogRequest> {
        let mut inner = self.inner.lock();
        inner.sessions.insert(worker.into(), session.into());
        let stale: Vec<_> = inner
            .pending
            .iter()
            .filter(|(_, p)| p.worker == worker && p.session != session)
            .map(|(id, _)| id.clone())
            .collect();
        for id in stale {
            if let Some(mut p) = inner.pending.remove(&id) {
                if let Some(reply) = p.reply.take() {
                    let _ = reply.send(Err(Status::unavailable("node restarted; retry")));
                }
            }
        }
        inner
            .pending
            .values_mut()
            .filter(|p| p.worker == worker && !p.delivered)
            .map(|p| {
                p.delivered = true;
                p.request.clone()
            })
            .collect()
    }

    pub async fn request(
        &self,
        worker: &str,
        service: &str,
        lines: u32,
    ) -> Result<Vec<u8>, Status> {
        let _slot = self
            .slots
            .try_acquire()
            .map_err(|_| Status::resource_exhausted("too many log requests; retry"))?;
        let (tx, rx) = oneshot::channel();
        let ticket = format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let guard = Guard {
            relay: self,
            ticket: ticket.clone(),
        };
        {
            let mut inner = self.inner.lock();
            let session = inner
                .sessions
                .get(worker)
                .cloned()
                .ok_or_else(|| Status::unavailable("node is offline"))?;
            inner.pending.insert(
                ticket.clone(),
                Pending {
                    worker: worker.into(),
                    session,
                    request: LogRequest {
                        ticket,
                        service: service.into(),
                        lines: lines.clamp(1, MAX_LOG_LINES),
                    },
                    delivered: false,
                    reply: Some(tx),
                },
            );
        }
        let result = tokio::time::timeout(Duration::from_secs(15), rx)
            .await
            .map_err(|_| Status::deadline_exceeded("the node did not answer; it may be busy or offline"))?
            .map_err(|_| Status::unavailable("log request ended"))?;
        drop(guard);
        result
    }

    pub(crate) fn claim(
        &self,
        ticket: &str,
        worker: &str,
        session: &str,
    ) -> Result<oneshot::Sender<Result<Vec<u8>, Status>>, StatusCode> {
        let mut inner = self.inner.lock();
        let pending = inner.pending.get_mut(ticket).ok_or(StatusCode::NOT_FOUND)?;
        if pending.worker != worker || pending.session != session {
            return Err(StatusCode::FORBIDDEN);
        }
        pending.reply.take().ok_or(StatusCode::CONFLICT)
    }
}

pub fn router(state: crate::http::HttpState) -> Router {
    Router::new()
        .route("/internal/app-logs/:ticket", post(receive))
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
    // Scoped so the borrow of `request` ends before the body is awaited.
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
            header("x-fleet-log-error"),
        )
    };
    let reply = match s.fleet.log_relay.claim(&ticket, &worker, &session) {
        Ok(reply) => reply,
        Err(code) => return code.into_response(),
    };
    if !error.is_empty() {
        let status = match error.as_str() {
            "missing" => Status::not_found("this service has no log yet"),
            "unconfigured" => Status::not_found("this service reports no log file"),
            _ => Status::unavailable("the node could not read the log"),
        };
        let _ = reply.send(Err(status));
        return StatusCode::NO_CONTENT.into_response();
    }
    match to_bytes(request.into_body(), MAX_LOG_BYTES).await {
        Ok(bytes) => {
            let _ = reply.send(Ok(bytes.to_vec()));
            StatusCode::NO_CONTENT.into_response()
        }
        Err(_) => {
            let _ = reply.send(Err(Status::data_loss("log reply was too large or incomplete")));
            StatusCode::BAD_REQUEST.into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn requests_reach_only_their_workers_current_session_and_are_delivered_once() {
        let relay = Arc::new(LogRelay::default());
        assert_eq!(
            relay.request("gpu", "svc", 10).await.unwrap_err().code(),
            tonic::Code::Unavailable,
            "an unknown node is offline"
        );
        relay.heartbeat("gpu", "s1");
        let asking = {
            let relay = relay.clone();
            tokio::spawn(async move { relay.request("gpu", "svc", 5000).await })
        };
        let request = loop {
            if let Some(r) = relay.heartbeat("gpu", "s1").into_iter().next() {
                break r;
            }
            tokio::task::yield_now().await;
        };
        assert_eq!((request.service.as_str(), request.lines), ("svc", MAX_LOG_LINES));
        assert!(relay.heartbeat("gpu", "s1").is_empty(), "delivered once");
        assert_eq!(relay.claim(&request.ticket, "other", "s1").unwrap_err(), StatusCode::FORBIDDEN);
        assert_eq!(relay.claim(&request.ticket, "gpu", "old").unwrap_err(), StatusCode::FORBIDDEN);
        let reply = relay.claim(&request.ticket, "gpu", "s1").unwrap();
        assert_eq!(relay.claim(&request.ticket, "gpu", "s1").unwrap_err(), StatusCode::CONFLICT);
        reply.send(Ok(b"line\n".to_vec())).unwrap();
        assert_eq!(asking.await.unwrap().unwrap(), b"line\n".to_vec());
        assert!(relay.inner.lock().pending.is_empty(), "nothing outlives the call");
    }

    #[tokio::test]
    async fn a_restarted_node_session_fails_waiting_requests() {
        let relay = Arc::new(LogRelay::default());
        relay.heartbeat("gpu", "s1");
        let asking = {
            let relay = relay.clone();
            tokio::spawn(async move { relay.request("gpu", "svc", 10).await })
        };
        while relay.inner.lock().pending.is_empty() {
            tokio::task::yield_now().await;
        }
        relay.heartbeat("gpu", "s2");
        assert_eq!(asking.await.unwrap().unwrap_err().code(), tonic::Code::Unavailable);
    }
}
