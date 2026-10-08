//! App registration carries an explicit namespace to the private API.
use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::Command;

fn cli() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_fleetctl"));
    for name in ["FLEET_NAMESPACE", "FLEET_TAG", "FLEET_TOKEN_FILE", "FLEET_ADMIN_TOKEN_FILE"] {
        cmd.env_remove(name);
    }
    cmd
}

#[test]
fn app_creation_requires_namespace_before_contacting_api() {
    let output = cli().args(["app", "add", "training", "--image", "python:3", "--allow", "/tmp"]).output().unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("requires --namespace"));
}

#[test]
fn app_creation_sends_namespace_selector() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let token = std::env::temp_dir().join(format!("fleet-app-test-{}.token", std::process::id()));
    std::fs::write(&token, "test-admin").unwrap();
    let server = std::thread::spawn(move || {
        listener.set_nonblocking(true).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let mut stream = loop {
            if let Ok((s, _)) = listener.accept() { break s; }
            assert!(std::time::Instant::now() < deadline, "CLI never contacted the API");
            std::thread::sleep(std::time::Duration::from_millis(10));
        };
        stream.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();
        let mut request = Vec::new();
        let mut buffer = [0; 2048];
        loop {
            let count = stream.read(&mut buffer).unwrap();
            assert!(count > 0);
            request.extend_from_slice(&buffer[..count]);
            if request.windows(4).any(|w| w == b"\r\n\r\n") { break; }
        }
        let headers = String::from_utf8_lossy(&request);
        assert!(headers.starts_with("POST /apps?namespace=alice HTTP/1.1\r\n"), "{headers}");
        assert!(headers.to_ascii_lowercase().contains("authorization: bearer test-admin\r\n"));
        let header_end = request.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
        let length: usize = headers.lines().find_map(|l| l.to_lowercase().strip_prefix("content-length: ").map(str::to_owned)).unwrap().parse().unwrap();
        let mut body_bytes = request[header_end..].to_vec();
        let mut rest = vec![0; length - body_bytes.len()];
        stream.read_exact(&mut rest).unwrap();
        body_bytes.extend(rest);
        assert_eq!(serde_json::from_slice::<serde_json::Value>(&body_bytes).unwrap()["priority"], 77);
        let body = r#"{"priority":77,"name":"training","namespace":"11111111111111111111111111111111","image":"python:3","max_gpus":8,"allow":["/tmp"]}"#;
        write!(stream, "HTTP/1.1 201 Created\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
    });
    let output = cli().args(["--json", "app", "add", "training", "--namespace", "alice", "--runs-url", &url,
        "--image", "python:3", "--allow", "/tmp", "--priority", "77", "--admin-token-file"]).arg(&token).output().unwrap();
    std::fs::remove_file(token).unwrap();
    server.join().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let app: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(app["namespace"], "1".repeat(32));
    assert_eq!(app["priority"], 77);
}

#[test]
fn app_priority_uses_admin_policy_routes_for_both_app_types() {
    for proof in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let token = std::env::temp_dir().join(format!("fleet-priority-test-{}-{proof}.token", std::process::id()));
        std::fs::write(&token, "test-admin").unwrap();
        let elf = "a".repeat(64);
        let name = if proof { format!("eth/{elf}") } else { "training".into() };
        let app = serde_json::json!({"name":name,"tenant":"eth","elf_id":elf,"namespace":"1".repeat(32),"priority":0});
        let server = std::thread::spawn(move || {
            listener.set_nonblocking(true).unwrap();
            for index in 0..2 {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
                let mut stream = loop {
                    if let Ok((s, _)) = listener.accept() { break s; }
                    assert!(std::time::Instant::now() < deadline);
                    std::thread::sleep(std::time::Duration::from_millis(10));
                };
                stream.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();
                let mut bytes = Vec::new();
                let mut byte = [0];
                while !bytes.ends_with(b"\r\n\r\n") {
                    stream.read_exact(&mut byte).unwrap();
                    bytes.push(byte[0]);
                }
                let headers = String::from_utf8(bytes).unwrap();
                assert!(headers.to_lowercase().contains("authorization: bearer test-admin"));
                if index == 0 {
                    assert!(headers.starts_with(if proof { "GET /admin/apps?namespace=alice " } else { "GET /apps?namespace=alice " }), "{headers}");
                } else if proof {
                    assert!(headers.starts_with("PUT /admin/apps/priority?namespace=11111111111111111111111111111111&tenant=eth&elf_id="), "{headers}");
                    assert!(headers.lines().next().unwrap().ends_with("&priority=-5 HTTP/1.1"));
                } else {
                    assert!(headers.starts_with("PUT /apps/training/priority?namespace=alice "), "{headers}");
                    let length: usize = headers.lines().find_map(|l| l.to_lowercase().strip_prefix("content-length: ").map(str::to_owned)).unwrap().parse().unwrap();
                    let mut body = vec![0; length];
                    stream.read_exact(&mut body).unwrap();
                    assert_eq!(serde_json::from_slice::<serde_json::Value>(&body).unwrap(), serde_json::json!({"priority":-5}));
                }
                let body = if index == 0 { serde_json::json!([app]) } else { serde_json::json!({"priority":-5}) }.to_string();
                write!(stream, "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
            }
        });
        let output = cli().args(["--json", "--namespace", "alice", if proof { "--url" } else { "--runs-url" }, &url,
            "--admin-token-file"]).arg(&token).args(["app", "priority", &name, "-5"]).output().unwrap();
        server.join().unwrap();
        std::fs::remove_file(&token).unwrap();
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        assert_eq!(serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["priority"], -5);
    }
}
