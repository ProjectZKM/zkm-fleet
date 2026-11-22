//! Credential delivery never passes through the Ray bridge, argv or disk.
use anyhow::{ensure, Result};
use fleet_proto::model::v1::{RuntimeDeliveryAck, RuntimeDeliveryOffer};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
const MAX_PAYLOAD: usize = 4 * 1024 * 1024;

pub fn validate_socket(path: &Path) -> Result<()> {
    ensure!(path.is_absolute(), "private shim socket required");
    let uid = unsafe { libc::geteuid() };
    let dir = std::fs::symlink_metadata(
        path.parent()
            .ok_or_else(|| anyhow::anyhow!("missing socket directory"))?,
    )?;
    let m = std::fs::symlink_metadata(path)?;
    ensure!(
        dir.is_dir()
            && dir.uid() == uid
            && dir.permissions().mode() & 0o077 == 0
            && m.file_type().is_socket()
            && m.uid() == uid
            && m.permissions().mode() & 0o077 == 0,
        "private same-user shim socket required"
    );
    Ok(())
}
async fn provision(path: &Path, offer: &RuntimeDeliveryOffer, payload: &[u8]) -> Result<()> {
    ensure!(
        payload.len() <= MAX_PAYLOAD
            && hex::encode(Sha256::digest(payload)) == offer.payload_sha256,
        "delivery digest mismatch"
    );
    let payload: Value = serde_json::from_slice(payload)?;
    ensure!(
        payload["request"]["execution"] == serde_json::to_value(&offer.execution)?
            && payload["request"]["driver"] == "openshell",
        "delivery identity mismatch"
    );
    validate_socket(path)?;
    let mut socket = tokio::net::UnixStream::connect(path).await?;
    ensure!(
        socket.peer_cred()?.uid() == unsafe { libc::geteuid() },
        "untrusted shim peer"
    );
    let command = json!({"protocol":1,"node_id":offer.execution.node_id,"command":{"operation":"provision","payload":payload}});
    socket.write_all(&serde_json::to_vec(&command)?).await?;
    socket.shutdown().await?;
    let mut bytes = vec![];
    socket.take(65537).read_to_end(&mut bytes).await?;
    ensure!(bytes.len() <= 65536, "oversize shim response");
    let response: Value = serde_json::from_slice(&bytes)?;
    ensure!(
        response["protocol"] == 1
            && response.get("error").is_none()
            && response["result"]["execution"] == serde_json::to_value(&offer.execution)?
            && response["result"]["phase"] == "created",
        "shim did not acknowledge provisioning"
    );
    Ok(())
}
#[derive(Clone)]
pub struct DeliveryClient {
    pub http: reqwest::Client,
    pub scheduler: String,
    pub secret: String,
    pub worker: String,
    pub session: String,
    pub socket: PathBuf,
}
impl DeliveryClient {
    fn post(&self, offer: &RuntimeDeliveryOffer, operation: &str) -> reqwest::RequestBuilder {
        self.http
            .post(format!(
                "{}/internal/runtime-deliveries/{}/{operation}",
                self.scheduler.trim_end_matches('/'),
                offer.ticket
            ))
            .bearer_auth(&self.secret)
            .header("x-fleet-worker", &self.worker)
            .header("x-fleet-session", &self.session)
    }
    pub async fn deliver(&self, offer: RuntimeDeliveryOffer) {
        if offer.ticket.len() != 64
            || !offer
                .ticket
                .bytes()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        {
            return;
        }
        let fetched = async {
            let mut response = self
                .post(&offer, "claim")
                .timeout(Duration::from_secs(15))
                .send()
                .await?
                .error_for_status()?;
            ensure!(
                response
                    .content_length()
                    .is_none_or(|n| n <= MAX_PAYLOAD as u64),
                "oversize delivery"
            );
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await? {
                ensure!(
                    bytes.len() + chunk.len() <= MAX_PAYLOAD,
                    "oversize delivery"
                );
                bytes.extend_from_slice(&chunk);
            }
            anyhow::Ok(bytes)
        }
        .await;
        let Ok(bytes) = fetched else {
            // Claim may already be durable. Never retry the mutation or print a
            // response body; operator reconciliation handles uncertain delivery.
            tracing::warn!(ticket=%offer.ticket,"runtime claim unavailable; no automatic redelivery");
            return;
        };
        let accepted = matches!(
            tokio::time::timeout(
                Duration::from_secs(150),
                provision(&self.socket, &offer, &bytes)
            )
            .await,
            Ok(Ok(()))
        );
        let response = self
            .post(&offer, "ack")
            .timeout(Duration::from_secs(10))
            .json(&RuntimeDeliveryAck { accepted })
            .send()
            .await;
        if !response.is_ok_and(|r| r.status().is_success()) {
            tracing::warn!(ticket=%offer.ticket,"runtime result unacknowledged; inspect the owning shim");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::Request, routing::post, Router};
    use std::sync::{Arc, Mutex};
    #[tokio::test]
    async fn private_http_to_unix_delivery_sanitizes_results_and_fences_identity() {
        for mode in ["success", "error", "wrong-identity", "oversize"] {
            let dir = tempfile::tempdir().unwrap();
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
            let path = dir.path().join("shim.sock");
            let listener = tokio::net::UnixListener::bind(&path).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
            let execution = json!({"namespace":"tenant","workload":"agent","allocation_id":"agent-0","lease_id":"one","generation":1,"node_id":"machine"});
            let payload=serde_json::to_vec(&json!({"request":{"execution":execution,"driver":"openshell"},"ledger_token":"synthetic-private-token"})).unwrap();
            let offer = RuntimeDeliveryOffer {
                ticket: "a".repeat(64),
                execution: serde_json::from_value(execution.clone()).unwrap(),
                payload_sha256: hex::encode(Sha256::digest(&payload)),
            };
            let expected = payload.clone();
            let expected_execution = execution.clone();
            let shim = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = vec![];
                socket.read_to_end(&mut bytes).await.unwrap();
                let request: Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(request["node_id"], "machine");
                assert_eq!(request["command"]["operation"], "provision");
                assert_eq!(
                    request["command"]["payload"],
                    serde_json::from_slice::<Value>(&expected).unwrap()
                );
                let mut response = json!({"protocol":1,"result":{"execution":expected_execution,"phase":"created"}});
                if mode == "error" {
                    response = json!({"protocol":1,"error":"synthetic-private-token"});
                }
                if mode == "wrong-identity" {
                    response["result"]["execution"]["namespace"] = json!("other");
                }
                if mode == "oversize" {
                    response["extra"] = json!("x".repeat(70000));
                }
                let _ = socket
                    .write_all(&serde_json::to_vec(&response).unwrap())
                    .await;
            });
            let ack = Arc::new(Mutex::new(None));
            let received = ack.clone();
            let route = format!("/internal/runtime-deliveries/{}", offer.ticket);
            let app = Router::new()
                .route(
                    &format!("{route}/claim"),
                    post(move |r: Request| async move {
                        assert_eq!(r.headers()["authorization"], "Bearer cluster-secret");
                        assert_eq!(r.headers()["x-fleet-worker"], "worker");
                        assert_eq!(r.headers()["x-fleet-session"], "session");
                        payload
                    }),
                )
                .route(
                    &format!("{route}/ack"),
                    post(move |r: Request| async move {
                        *received.lock().unwrap() =
                            Some(axum::body::to_bytes(r.into_body(), 1024).await.unwrap());
                    }),
                );
            let server = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = server.local_addr().unwrap();
            let server = tokio::spawn(async move { axum::serve(server, app).await.unwrap() });
            let client = DeliveryClient {
                http: reqwest::Client::builder()
                    .no_proxy()
                    .redirect(reqwest::redirect::Policy::none())
                    .build()
                    .unwrap(),
                scheduler: format!("http://{address}"),
                secret: "cluster-secret".into(),
                worker: "worker".into(),
                session: "session".into(),
                socket: path,
            };
            client.deliver(offer).await;
            shim.await.unwrap();
            server.abort();
            let ack = ack.lock().unwrap();
            let value: Value = serde_json::from_slice(ack.as_ref().unwrap()).unwrap();
            assert_eq!(value, json!({"accepted":mode=="success"}));
        }
    }
    #[tokio::test]
    async fn digest_and_private_socket_checks_precede_transmission() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shim.sock");
        let _listener = tokio::net::UnixListener::bind(&path).unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o666)).unwrap();
        assert!(validate_socket(&path).is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        validate_socket(&path).unwrap();
        std::os::unix::fs::symlink(&path, dir.path().join("link")).unwrap();
        assert!(validate_socket(&dir.path().join("link")).is_err());
        let offer = RuntimeDeliveryOffer {
            ticket: "a".repeat(64),
            execution: Default::default(),
            payload_sha256: "b".repeat(64),
        };
        assert!(provision(&path, &offer, b"{}").await.is_err());
    }
}
