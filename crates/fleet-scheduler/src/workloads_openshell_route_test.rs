//! Real local relay/connector fixture. Only route grants are issued by the test;
//! production publication remains gated on app readiness and Fleet control sync.
use std::{
    net::SocketAddr,
    path::Path,
    process::Command,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use trusted_router_transport::{
    edge,
    grants::{self, Grant, Purpose, Verifier, AUDIENCE},
    relay::Relay,
};
use trusted_shim::contracts::{ExecutionIdentity, PolicyRef, ServiceId};

pub(super) struct Route {
    base: String,
    access: String,
    revoke: String,
    route: String,
    client: reqwest::Client,
    connector: tokio::task::JoinHandle<()>,
    relay: tokio::task::JoinHandle<()>,
    registration: tokio::task::JoinHandle<()>,
    pub(super) config: trusted_shim::data_plane::NetworkConfig,
    pub(super) control_ready: Arc<AtomicBool>,
    pub(super) registrations: Arc<AtomicUsize>,
}
impl Drop for Route {
    fn drop(&mut self) {
        self.connector.abort();
        self.relay.abort();
        self.registration.abort();
    }
}
impl Route {
    pub(super) async fn start(root: &Path, execution: &ExecutionIdentity, app: SocketAddr) -> Self {
        Self::prepare(root, execution, Some(app)).await
    }

    pub(super) async fn managed(root: &Path, execution: &ExecutionIdentity) -> Self {
        Self::prepare(root, execution, None).await
    }

    async fn prepare(root: &Path, execution: &ExecutionIdentity, app: Option<SocketAddr>) -> Self {
        let private = root.join("router-key.pem");
        let public = root.join("router-public.pem");
        for args in [
            vec![
                "genpkey",
                "-algorithm",
                "ED25519",
                "-out",
                private.to_str().unwrap(),
            ],
            vec![
                "pkey",
                "-in",
                private.to_str().unwrap(),
                "-pubout",
                "-out",
                public.to_str().unwrap(),
            ],
        ] {
            assert!(Command::new("openssl")
                .args(args)
                .output()
                .unwrap()
                .status
                .success());
        }
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&public, std::fs::Permissions::from_mode(0o644)).unwrap();
        let key = std::fs::read(private).unwrap();
        let relay = Relay::new(
            Verifier::new(&std::fs::read(&public).unwrap(), "native-test".into()).unwrap(),
            &root.join("router.sqlite"),
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let relay = tokio::spawn(async move {
            relay.serve(listener).await.unwrap();
        });
        let grant = Grant {
            iss: "native-test".into(),
            aud: AUDIENCE.into(),
            sub: execution.node_id.clone(),
            nbf: grants::now(),
            exp: grants::now() + 600,
            purpose: Purpose::Register,
            http_request: None,
            access: None,
            execution: execution.clone(),
            service: ServiceId {
                namespace: execution.namespace.clone(),
                workload: execution.workload.clone(),
                service: "native-http".into(),
            },
            policy: PolicyRef {
                issuer: "native-test".into(),
                policy_id: "test-route".into(),
                version: 1,
                sha256: "ab".repeat(32),
            },
        };
        let route = grant.route_id();
        let token = grants::sign(&key, &grant).unwrap();
        let url = format!("ws://{address}/v1/connect/{route}");
        let connector = tokio::spawn(async move {
            if let Some(app) = app {
                edge::connect(&url, &token, app, CancellationToken::new())
                    .await
                    .unwrap();
            }
        });
        // Only this acceptance fixture grants registration. Production Fleet
        // must join authenticated native policy readiness before issuing it.
        let control_ready = Arc::new(AtomicBool::new(app.is_some()));
        let registrations = Arc::new(AtomicUsize::new(0));
        let approved = control_ready.clone();
        let accepted = registrations.clone();
        let registration_grant = grant.clone();
        let registration_key = key.clone();
        let router = axum::Router::new().route("/v1/runtime/registration", axum::routing::post(
            move |headers: axum::http::HeaderMap, axum::Json(body): axum::Json<serde_json::Value>| {
                let approved = approved.clone(); let accepted = accepted.clone();
                let grant = registration_grant.clone(); let key = registration_key.clone();
                async move {
                    if !approved.load(Ordering::SeqCst)
                        || headers.get("authorization").and_then(|s|s.to_str().ok()) != Some("Bearer fixture-enrollment")
                        || body["execution"] != serde_json::json!(grant.execution)
                        || body["service"] != serde_json::json!({"id":grant.service,"protocol":"http1","endpoint_ref":"app"}) {
                        return Err(axum::http::StatusCode::FORBIDDEN);
                    }
                    accepted.fetch_add(1, Ordering::SeqCst);
                    Ok(axum::Json(serde_json::json!({"token":grants::sign(&key, &grant).unwrap()})))
                }
            }));
        let registration_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let registration_address = registration_listener.local_addr().unwrap();
        let registration = tokio::spawn(async move {
            axum::serve(registration_listener, router).await.unwrap();
        });
        let token_file = root.join("router-enrollment");
        std::fs::write(&token_file, "fixture-enrollment").unwrap();
        std::fs::set_permissions(&token_file, std::fs::Permissions::from_mode(0o600)).unwrap();
        let config = trusted_shim::data_plane::NetworkConfig {
            gateway_url: format!("http://{registration_address}"),
            relay_url: format!("ws://{address}"),
            token_file,
            public_key_file: public,
            issuer: "native-test".into(),
        };
        let access = grants::sign(
            &key,
            &Grant {
                purpose: Purpose::Access,
                sub: "app-user".into(),
                ..grant.clone()
            },
        )
        .unwrap();
        let revoke = grants::sign(
            &key,
            &Grant {
                purpose: Purpose::Revoke,
                ..grant.clone()
            },
        )
        .unwrap();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap();
        let base = format!("http://{address}");
        if app.is_some() {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if client
                        .get(format!("{base}/v1/ready/{route}"))
                        .bearer_auth(&access)
                        .send()
                        .await
                        .unwrap()
                        .status()
                        == 204
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("outbound connector readiness");
        }
        let mut other = Grant {
            purpose: Purpose::Access,
            ..grant
        };
        other.execution.namespace = "other-tenant".into();
        other.service.namespace = "other-tenant".into();
        let forbidden = client
            .get(format!("{base}/v1/http/{route}/echo"))
            .header("x-trusted-access", grants::sign(&key, &other).unwrap())
            .send()
            .await
            .unwrap();
        assert!(matches!(forbidden.status().as_u16(), 401 | 403));
        Self {
            base,
            access,
            revoke,
            route,
            client,
            connector,
            relay,
            registration,
            config,
            control_ready,
            registrations,
        }
    }

    pub(super) async fn ready(&self) -> bool {
        self.client
            .get(format!("{}/v1/ready/{}", self.base, self.route))
            .bearer_auth(&self.access)
            .send()
            .await
            .unwrap()
            .status()
            == 204
    }
    pub(super) async fn wait_ready(&self) {
        tokio::time::timeout(Duration::from_secs(15), async {
            while !self.ready().await {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("managed outbound connector readiness");
    }

    pub(super) fn get(&self, path: &str) -> reqwest::RequestBuilder {
        self.client
            .get(format!("{}/v1/http/{}/{}", self.base, self.route, path))
            .header("x-trusted-access", &self.access)
    }

    pub(super) async fn websocket(&self) {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};
        let mut request = format!(
            "{}/v1/http/{}/ws",
            self.base.replace("http://", "ws://"),
            self.route
        )
        .into_client_request()
        .unwrap();
        request
            .headers_mut()
            .insert("x-trusted-access", self.access.parse().unwrap());
        let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        for message in [
            Message::Text("native-chat".into()),
            Message::Binary(vec![0, 1, 255].into()),
        ] {
            socket.send(message.clone()).await.unwrap();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(3), socket.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                message
            );
        }
        socket.close(None).await.unwrap();
    }

    pub(super) async fn revoke(&self) {
        assert_eq!(
            self.client
                .delete(format!("{}/v1/revoke/{}", self.base, self.route))
                .bearer_auth(&self.revoke)
                .send()
                .await
                .unwrap()
                .status(),
            204
        );
    }
}
