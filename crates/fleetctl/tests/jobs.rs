//! Job listing sends selectors and uses each API's supported limit parameter.
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

fn list_query_is_scoped(kind: &str, endpoint: &str, limit_param: &str, other_param: &str) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let token = std::env::temp_dir().join(format!("fleet-jobs-test-{}.token", std::process::id()));
    std::fs::write(&token, "test-admin").unwrap();
    let endpoint = endpoint.to_string();
    let limit_param = limit_param.to_string();
    let other_param = other_param.to_string();
    let server = std::thread::spawn(move || {
        for expected_limit in ["1000", "10"] {
            let (mut stream, _) = listener.accept().unwrap();
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
            let request_line = headers.lines().next().unwrap();
            assert!(request_line.starts_with(&format!("GET {endpoint}?")), "{request_line}");
            assert!(request_line.contains("namespace=alice"), "{request_line}");
            assert!(request_line.contains(&format!("{limit_param}={expected_limit}")), "{request_line}");
            assert!(!request_line.contains(&format!("{other_param}=")), "unsupported query was sent: {request_line}");
            let body = "[]";
            write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
        }
    });

    let output = cli().args([
        "--json", "job", "--namespace", "alice", "--type", kind, "--url", &url,
        "--runs-url", &url, "--admin-token-file",
    ]).arg(&token).output().unwrap();
    std::fs::remove_file(token).unwrap();
    server.join().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(), serde_json::json!([]));
}

#[test]
fn job_lists_send_namespace_and_api_specific_limit_query() {
    list_query_is_scoped("proof", "/admin/jobs", "limit", "n");
    list_query_is_scoped("run", "/runs", "n", "limit");
}
