//! Token issuance uses the real CLI against a private HTTP test endpoint.
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

use sha2::Digest;

struct Temp(PathBuf);
impl Drop for Temp {
    fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.0); }
}

fn cli() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_fleetctl"));
    cmd.env_remove("FLEET_TOKEN_FILE").env_remove("FLEET_ADMIN_TOKEN_FILE").env_remove("FLEET_NAMESPACE").env_remove("FLEET_TAG").env_remove("FLEET_GROUP");
    cmd
}

#[test]
fn issues_private_scoped_bundle_and_refuses_overwrite() {
    use std::os::unix::fs::PermissionsExt;
    let dir = Temp(std::env::temp_dir().join(format!("fleet user 'bundle-{:x}", rand::random::<u128>())));
    std::fs::create_dir(&dir.0).unwrap();
    let tls = dir.0.join("tls");
    let init = cli().args(["init", "tls", "--names", "stage", "--dir"]).arg(&tls).output().unwrap();
    assert!(init.status.success(), "{}", String::from_utf8_lossy(&init.stderr));
    let admin = dir.0.join("admin.token");
    std::fs::write(&admin, "operator-token").unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    listener.set_nonblocking(true).unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        let mut original = serde_json::Value::Null;
        for step in 0..4 {
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut stream = loop {
                if let Ok((stream, _)) = listener.accept() { break stream; }
                assert!(Instant::now() < deadline, "CLI did not contact the token endpoint");
                std::thread::sleep(Duration::from_millis(10));
            };
            stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            let mut bytes = vec![];
            let mut buf = [0u8; 4096];
            let (header_end, size) = loop {
                let n = stream.read(&mut buf).unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buf[..n]);
                if let Some(pos) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&bytes[..pos]).to_ascii_lowercase();
                    assert!(headers.contains("authorization: bearer operator-token\r\n"));
                    if step == 0 { assert!(headers.starts_with("post /admin/tokens?namespace=alice&tag=research ")); }
                    if step == 1 { assert!(headers.starts_with("get /admin/tokens ")); }
                    if step == 2 { assert!(headers.starts_with("post /admin/tokens ")); }
                    if step == 3 { assert!(headers.starts_with("post /admin/tokens?namespace=alice ")); }
                    let size = headers.lines().find_map(|l| l.strip_prefix("content-length: "))
                        .unwrap_or("0").parse::<usize>().unwrap();
                    break (pos + 4, size);
                }
            };
            while bytes.len() < header_end + size {
                let n = stream.read(&mut buf).unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buf[..n]);
            }
            let response = if step == 1 {
                let mut row = original.clone();
                row.as_object_mut().unwrap().remove("address");
                row["namespace"] = "1".repeat(32).into();
                serde_json::json!([row]).to_string()
            } else {
                let body: serde_json::Value = serde_json::from_slice(&bytes[header_end..header_end + size]).unwrap();
                if step == 0 { original = body.clone(); }
                tx.send(body).unwrap();
                "{}".to_string()
            };
            write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", response.len(), response).unwrap();
        }
    });
    let env_file = dir.0.join("alice.env");
    let mut issue = cli();
    issue.args(["--json", "token", "issue", "--tenant", "eth", "--label", "alice",
        "--namespace", "alice", "--tag", "research", "--app", "training", "--endpoint", "https://127.0.0.1:51000", "--domain-name", "stage", "--url", &url,
        "--admin-token-file"]).arg(&admin).arg("--server-ca").arg(tls.join("ca.pem")).arg("--out").arg(&env_file);
    let output = issue.output().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let registered = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let token_file = env_file.with_extension("token");
    let token = std::fs::read_to_string(&token_file).unwrap();
    assert_eq!(registered["namespace"], "alice");
    assert_eq!(registered["tag"], "research");
    assert_eq!(registered["apps"], serde_json::json!(["training"]));
    assert_eq!(registered["token_hash"], hex::encode(sha2::Sha256::digest(token.as_bytes())));
    assert!(!String::from_utf8_lossy(&output.stdout).contains(&token));
    assert!(!String::from_utf8_lossy(&output.stderr).contains(&token));
    for extension in ["env", "token", "ca.pem"] {
        let path = env_file.with_extension(extension);
        assert_eq!(std::fs::metadata(path).unwrap().permissions().mode() & 0o777, 0o600);
    }
    // Issued env files must source correctly even for paths with shell metacharacters.
    let loaded = Command::new("bash").args(["-c", "set -a; . \"$1\"; printf '%s' \"$FLEET_TOKEN_FILE\"", "token-test"])
        .arg(&env_file).output().unwrap();
    assert!(loaded.status.success());
    assert_eq!(String::from_utf8(loaded.stdout).unwrap(), token_file.to_str().unwrap());
    let replacement_env = dir.0.join("replacement.env");
    let refreshed = cli().args(["--json", "token", "refresh", registered["id"].as_str().unwrap(),
        "--endpoint", "https://127.0.0.1:51000", "--domain-name", "stage", "--url", &url,
        "--admin-token-file"]).arg(&admin).arg("--server-ca").arg(tls.join("ca.pem")).arg("--out").arg(&replacement_env).output().unwrap();
    assert!(refreshed.status.success(), "{}", String::from_utf8_lossy(&refreshed.stderr));
    let replacement = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(replacement["namespace"], "1".repeat(32));
    for field in ["apps", "tenant", "label", "tag"] { assert_eq!(replacement[field], registered[field]); }
    assert_ne!(replacement["id"], registered["id"]);
    assert!(!String::from_utf8_lossy(&refreshed.stdout).contains(registered["address"].as_str().unwrap()));
    assert_ne!(replacement["token_hash"], registered["token_hash"]);
    let replacement_token = std::fs::read_to_string(replacement_env.with_extension("token")).unwrap();
    assert_ne!(replacement_token, token);
    assert!(!String::from_utf8_lossy(&refreshed.stdout).contains(&replacement_token));
    for extension in ["env", "token", "ca.pem"] {
        assert_eq!(std::fs::metadata(replacement_env.with_extension(extension)).unwrap().permissions().mode() & 0o777, 0o600);
    }
    let bundle=std::fs::read_to_string(replacement_env).unwrap();
    assert!(bundle.contains(&"1".repeat(32)));
    let tag = Command::new("bash").args(["-c", "set -a; . \"$1\"; printf '%s' \"$FLEET_TAG\"", "tag-test"])
        .arg(dir.0.join("replacement.env")).output().unwrap();
    assert!(tag.status.success());
    assert_eq!(String::from_utf8(tag.stdout).unwrap(),"research");
    let untagged_env = dir.0.join("untagged.env");
    let untagged = cli().args(["token","issue","--namespace","alice","--tenant","eth","--app","training",
        "--endpoint","http://127.0.0.1:51000","--url",&url,"--admin-token-file"])
        .arg(&admin).arg("--out").arg(&untagged_env).output().unwrap();
    assert!(untagged.status.success(),"{}",String::from_utf8_lossy(&untagged.stderr));
    assert!(rx.recv_timeout(Duration::from_secs(5)).unwrap()["tag"].is_null());
    assert!(std::fs::read_to_string(&untagged_env).unwrap().contains("FLEET_TAG=''"));
    server.join().unwrap(); // Issue + lookup + replacement + untagged issue; no automatic revocation.
    let duplicate = issue.output().unwrap();
    assert!(!duplicate.status.success());
    assert!(String::from_utf8_lossy(&duplicate.stderr).contains("refusing to overwrite"));
}

#[test]
fn proof_token_bundle_uses_server_tls_without_a_client_certificate() {
    use std::os::unix::fs::PermissionsExt;
    let dir = Temp(std::env::temp_dir().join(format!("fleet-server-tls-{:x}", rand::random::<u128>())));
    std::fs::create_dir(&dir.0).unwrap();
    let admin = dir.0.join("admin.token");
    std::fs::write(&admin, "operator-token").unwrap();
    let server_ca = dir.0.join("gateway-ca.pem");
    std::fs::write(&server_ca, b"gateway trust bundle").unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut request = Vec::new();
        let mut buffer = [0; 2048];
        let header_end = loop {
            let n = stream.read(&mut buffer).unwrap();
            assert!(n > 0);
            request.extend_from_slice(&buffer[..n]);
            if let Some(pos) = request.windows(4).position(|w| w == b"\r\n\r\n") { break pos + 4; }
        };
        let headers = String::from_utf8_lossy(&request[..header_end]);
        assert!(headers.starts_with("POST /admin/tokens?namespace=alice "), "{headers}");
        let length: usize = headers.lines().find_map(|line| {
            line.to_ascii_lowercase().strip_prefix("content-length: ").map(str::to_owned)
        }).unwrap().parse().unwrap();
        while request.len() < header_end + length {
            let n = stream.read(&mut buffer).unwrap();
            assert!(n > 0);
            request.extend_from_slice(&buffer[..n]);
        }
        let body: serde_json::Value = serde_json::from_slice(&request[header_end..header_end + length]).unwrap();
        assert_eq!(body["apps"][0], format!("eth/{}", "ab".repeat(32)));
        let response = r#"{"id":"abababababababababababababababab","namespace":"alice"}"#;
        write!(stream, "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).unwrap();
    });
    let env_file = dir.0.join("alice.env");
    let output = cli().args([
        "--json", "token", "issue", "--namespace", "alice", "--tenant", "eth", "--app",
        &format!("eth/{}", "ab".repeat(32)), "--endpoint", "https://fleet.example:51000",
        "--domain-name", "fleet.example", "--server-ca",
    ]).arg(&server_ca).args(["--url", &url, "--admin-token-file"]).arg(&admin)
        .arg("--out").arg(&env_file).output().unwrap();
    server.join().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let env = std::fs::read_to_string(&env_file).unwrap();
    assert!(env.contains("FLEET_TOKEN_FILE="));
    assert!(env.contains("CA_CERT_PATH="));
    assert!(env.contains("DOMAIN_NAME='fleet.example'"));
    assert!(!env.contains("SSL_CERT_PATH="));
    assert!(!env.contains("SSL_KEY_PATH="));
    assert_eq!(std::fs::read(env_file.with_extension("ca.pem")).unwrap(), b"gateway trust bundle");
    for file in [env_file, dir.0.join("alice.token"), dir.0.join("alice.ca.pem")] {
        assert_eq!(std::fs::metadata(file).unwrap().permissions().mode() & 0o777, 0o600);
    }
}

#[test]
fn namespace_quota_cli_attaches_unit_caps_and_can_clear_them() {
    let dir = Temp(std::env::temp_dir().join(format!("fleet-quota-{:x}", rand::random::<u128>())));
    std::fs::create_dir(&dir.0).unwrap();
    let admin = dir.0.join("admin.token");
    std::fs::write(&admin, "operator-token").unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    listener.set_nonblocking(true).unwrap();
    let server = std::thread::spawn(move || {
        let mut requests = Vec::new();
        for step in 0..7 {
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut stream = loop {
                if let Ok((s, _)) = listener.accept() { break s; }
                assert!(Instant::now() < deadline);
                std::thread::sleep(Duration::from_millis(10));
            };
            stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            let mut bytes = Vec::new();
            let mut buf = [0u8; 4096];
            let (end, size, headers) = loop {
                let n = stream.read(&mut buf).unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buf[..n]);
                if let Some(pos) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&bytes[..pos]).to_ascii_lowercase();
                    let size = headers.lines().find_map(|l| l.strip_prefix("content-length: ")).unwrap().parse::<usize>().unwrap();
                    break (pos + 4, size, headers);
                }
            };
            while bytes.len() < end + size {
                let n = stream.read(&mut buf).unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buf[..n]);
            }
            let body: serde_json::Value = serde_json::from_slice(&bytes[end..end + size]).unwrap();
            let request = headers.lines().next().unwrap().to_string();
            if step == 0 { assert!(request.starts_with("post /admin/namespaces ")); }
            if step == 1 { assert!(request.starts_with("post /admin/tokens ")); }
            requests.push((request, body));
            stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").unwrap();
        }
        requests
    });
    let server_ca = dir.0.join("gateway-ca.pem");
    std::fs::write(&server_ca, b"gateway trust bundle").unwrap();
    let owner_env = dir.0.join("alice.env");
    let create = cli().args(["namespace", "create", "alice", "--tenant", "eth", "--gpus", "0.5", "--cpus", "4",
        "--endpoint", "https://127.0.0.1:51000", "--domain-name", "stage", "--url", &url, "--admin-token-file"])
        .arg(&admin).arg("--server-ca").arg(&server_ca)
        .arg("--out").arg(&owner_env).output().unwrap();
    assert!(create.status.success(), "{}", String::from_utf8_lossy(&create.stderr));
    assert_eq!(std::fs::read(owner_env.with_extension("ca.pem")).unwrap(), b"gateway trust bundle");
    for args in [vec!["namespace", "quota", "alice", "--gpus", "2", "--cpus", "8"],
                 vec!["namespace", "quota", "alice"],
                 vec!["namespace","quota","alice","--tag","research","--gpus","1","--cpus","4"],
                 vec!["namespace","quota","alice","--tag","research","--gpus","2"],
                 vec!["namespace","quota","alice","--tag","research"]] {
        let result = cli().args(args).args(["--url", &url, "--admin-token-file"]).arg(&admin).output().unwrap();
        assert!(result.status.success(), "{}", String::from_utf8_lossy(&result.stderr));
    }
    let requests = server.join().unwrap();
    assert_eq!(requests[0].1["quota"], serde_json::json!({"gpus":0.5,"cpus":4.0}));
    assert_eq!(requests[1].1["manage_apps"], true);
    assert_eq!(requests[1].1["tag"], "default");
    assert!(requests[2].0.starts_with("put /admin/namespaces/alice/quota "));
    assert_eq!(requests[2].1, serde_json::json!({"gpus":2.0,"cpus":8.0}));
    assert_eq!(requests[3].1, serde_json::json!({"gpus":null,"cpus":null}));
    assert!(requests[4].0.starts_with("put /admin/namespaces/alice/quota?tag=research "));
    assert_eq!(requests[4].1,serde_json::json!({"gpus":1.0,"cpus":4.0}));
    assert!(requests[5].0.starts_with("put /admin/namespaces/alice/quota?tag=research "));
    assert_eq!(requests[5].1,serde_json::json!({"gpus":2.0,"cpus":null}));
    assert_eq!(requests[6].1,serde_json::json!({"gpus":null,"cpus":null}));
    let invalid = cli().args(["namespace", "quota", "alice", "--gpus", "NaN", "--url", &url, "--admin-token-file"]).arg(admin).output().unwrap();
    assert!(!invalid.status.success());
    assert!(String::from_utf8_lossy(&invalid.stderr).contains("finite and non-negative"));
}
