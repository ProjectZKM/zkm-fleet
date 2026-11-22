//! Native OpenShell links AWS-LC alongside Fleet's Ring backend. Exercise the
//! actual scheduler process so a provider installed by this test cannot mask a
//! missing initialization at the production TLS listener.
use fleet_proto::fleet::v1::{worker_service_client::WorkerServiceClient, WorkerHeartbeatRequest};
use std::{os::unix::fs::PermissionsExt, time::Duration};
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};

#[tokio::test]
async fn native_protocol_dependency_preserves_tls_and_cluster_authentication() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let pem = cert.cert.pem();
    std::fs::write(path.join("server.pem"), &pem).unwrap();
    std::fs::write(path.join("server.key"), cert.signing_key.serialize_pem()).unwrap();
    std::fs::write(path.join("cluster.token"), "a".repeat(48)).unwrap();
    for name in ["server.key", "cluster.token"] {
        std::fs::set_permissions(path.join(name), std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let config = serde_json::json!({
        "control_addr":address.to_string(), "http_addr":"127.0.0.1:0",
        "data_dir":path.join("state"), "cluster_token_file":path.join("cluster.token"),
        "control_tls":{"cert_path":path.join("server.pem"),"key_path":path.join("server.key")},
    });
    std::fs::write(
        path.join("fleet.yaml"),
        serde_yaml::to_string(&config).unwrap(),
    )
    .unwrap();
    let log = std::fs::File::create(path.join("scheduler.log")).unwrap();
    let mut scheduler = tokio::process::Command::new(env!("CARGO_BIN_EXE_fleet-scheduler"))
        .arg("--config")
        .arg(path.join("fleet.yaml"))
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    fleet_proto::initialize_tls();
    let endpoint = Endpoint::from_shared(format!("https://{address}"))
        .unwrap()
        .connect_timeout(Duration::from_secs(1));
    let trusted = endpoint
        .clone()
        .tls_config(
            ClientTlsConfig::new()
                .domain_name("localhost")
                .ca_certificate(Certificate::from_pem(pem)),
        )
        .unwrap();
    let channel = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            assert!(
                scheduler.try_wait().unwrap().is_none(),
                "TLS scheduler exited during startup"
            );
            if let Ok(channel) = trusted.connect().await {
                break channel;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("TLS scheduler startup");
    let error = WorkerServiceClient::new(channel)
        .heartbeat(WorkerHeartbeatRequest::default())
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unauthenticated);
    let stranger = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    assert!(endpoint
        .tls_config(
            ClientTlsConfig::new()
                .domain_name("localhost")
                .ca_certificate(Certificate::from_pem(stranger.cert.pem()))
        )
        .unwrap()
        .connect()
        .await
        .is_err());
    scheduler.kill().await.unwrap();
    scheduler.wait().await.unwrap();
}
