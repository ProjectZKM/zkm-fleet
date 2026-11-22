//! Opt-in fixture for the pinned, unmodified gateway. Certificates are ephemeral.
use super::*;
use std::{path::Path, process::Command};
fn openssl(root: &Path, args: &[&str]) {
    let output = Command::new("openssl")
        .current_dir(root)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "test certificate generation failed"
    );
}
pub(super) fn port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

pub(super) async fn start(
    root: &Path,
    socket: &Path,
) -> (
    tokio::process::Child,
    protocol::open_shell_client::OpenShellClient<tonic_native::transport::Channel>,
) {
    start_at(root, socket, port()).await
}

pub(super) async fn start_at(
    root: &Path,
    socket: &Path,
    api_port: u16,
) -> (
    tokio::process::Child,
    protocol::open_shell_client::OpenShellClient<tonic_native::transport::Channel>,
) {
    let binary = std::env::var_os("OPENSHELL_GATEWAY_BINARY").expect("native gateway binary");
    std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700)).unwrap();
    openssl(
        root,
        &[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "ca.key",
            "-out",
            "ca.pem",
            "-days",
            "1",
            "-subj",
            "/CN=shim-test-ca",
        ],
    );
    for (name, subject, extension) in [
        (
            "server",
            "/CN=localhost",
            "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n",
        ),
        (
            "client",
            "/CN=shim-test/OU=operator",
            "extendedKeyUsage=clientAuth\n",
        ),
    ] {
        openssl(
            root,
            &[
                "req",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-keyout",
                &format!("{name}.key"),
                "-out",
                &format!("{name}.csr"),
                "-subj",
                subject,
            ],
        );
        std::fs::write(root.join(format!("{name}.ext")), extension).unwrap();
        openssl(
            root,
            &[
                "x509",
                "-req",
                "-in",
                &format!("{name}.csr"),
                "-CA",
                "ca.pem",
                "-CAkey",
                "ca.key",
                "-CAcreateserial",
                "-days",
                "1",
                "-out",
                &format!("{name}.pem"),
                "-extfile",
                &format!("{name}.ext"),
            ],
        );
    }
    openssl(
        root,
        &["genpkey", "-algorithm", "ED25519", "-out", "jwt.key"],
    );
    openssl(
        root,
        &["pkey", "-in", "jwt.key", "-pubout", "-out", "jwt.pub"],
    );
    std::fs::write(root.join("jwt.kid"), "shim-acceptance").unwrap();
    let config = format!(
        "[openshell]\nversion=2\n[openshell.gateway.gateway_jwt]\nsigning_key_path={:?}\npublic_key_path={:?}\nkid_path={:?}\ngateway_id=\"shim-acceptance\"\n",
        root.join("jwt.key"), root.join("jwt.pub"), root.join("jwt.kid")
    );
    std::fs::write(root.join("gateway.toml"), config).unwrap();
    let log = std::fs::File::create(root.join("gateway.log")).unwrap();
    let mut gateway = tokio::process::Command::new(binary)
        .args([
            "--bind-address",
            "127.0.0.1",
            "--port",
            &api_port.to_string(),
            "--health-port",
            &port().to_string(),
            "--metrics-port",
            &port().to_string(),
            "--enable-mtls-auth",
            "true",
            "--enable-operator-auth",
            "true",
            "--compute-driver",
            "docker",
        ])
        .arg("--compute-driver-socket")
        .arg(socket)
        .arg("--config")
        .arg(root.join("gateway.toml"))
        .arg("--db-url")
        .arg(format!(
            "sqlite://{}?mode=rwc",
            root.join("gateway.db").display()
        ))
        .arg("--tls-cert")
        .arg(root.join("server.pem"))
        .arg("--tls-key")
        .arg(root.join("server.key"))
        .arg("--tls-client-ca")
        .arg(root.join("ca.pem"))
        .env("XDG_STATE_HOME", root.join("state"))
        .env("XDG_CONFIG_HOME", root.join("config"))
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let tls = tonic_native::transport::ClientTlsConfig::new()
        .ca_certificate(tonic_native::transport::Certificate::from_pem(
            std::fs::read(root.join("ca.pem")).unwrap(),
        ))
        .identity(tonic_native::transport::Identity::from_pem(
            std::fs::read(root.join("client.pem")).unwrap(),
            std::fs::read(root.join("client.key")).unwrap(),
        ));
    let endpoint =
        tonic_native::transport::Endpoint::from_shared(format!("https://127.0.0.1:{api_port}"))
            .unwrap()
            .tls_config(tls)
            .unwrap();
    let connected = tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            if let Ok(c) = endpoint.connect().await {
                break c;
            }
            assert!(
                gateway.try_wait().unwrap().is_none(),
                "native gateway exited: {}",
                std::fs::read_to_string(root.join("gateway.log")).unwrap()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("native gateway readiness");
    let client = protocol::open_shell_client::OpenShellClient::new(connected);
    (gateway, client)
}
