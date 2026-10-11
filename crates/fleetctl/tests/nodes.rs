use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::Command;

#[test]
fn node_views_show_total_and_free_cpu_gpu_resources() {
    for detail in [false, true] {
        for json in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let token = std::env::temp_dir().join(format!(
                "fleet-node-resources-{}-{detail}-{json}.token",
                std::process::id()
            ));
            std::fs::write(&token, "test-admin").unwrap();
            let node = serde_json::json!({
                "id": "gpu-box", "name": "gpu-box", "backend": "ray-local",
                "gpus": 8, "cpus": 48, "free_gpus": 2, "free_cpus": 0,
                "machine": "gpu-machine", "ready": true, "draining": false,
                "busy": null, "heartbeat_age_ms": 1000
            });
            let body = serde_json::json!({"nodes": [node.clone()]}).to_string();
            let server = std::thread::spawn(move || {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
                let mut stream = loop {
                    if let Ok((stream, _)) = listener.accept() {
                        break stream;
                    }
                    assert!(
                        std::time::Instant::now() < deadline,
                        "node CLI did not contact API"
                    );
                    std::thread::sleep(std::time::Duration::from_millis(10));
                };
                stream
                    .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                let mut headers = Vec::new();
                let mut byte = [0];
                while !headers.ends_with(b"\r\n\r\n") {
                    stream.read_exact(&mut byte).unwrap();
                    headers.push(byte[0]);
                }
                let headers = String::from_utf8(headers).unwrap();
                assert!(
                    headers.starts_with("GET /admin/status HTTP/1.1"),
                    "{headers}"
                );
                assert!(headers
                    .to_lowercase()
                    .contains("authorization: bearer test-admin"));
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            });

            let mut cmd = Command::new(env!("CARGO_BIN_EXE_fleetctl"));
            for name in [
                "FLEET_URL",
                "FLEET_NAMESPACE",
                "FLEET_TAG",
                "FLEET_TOKEN_FILE",
                "FLEET_ADMIN_TOKEN_FILE",
            ] {
                cmd.env_remove(name);
            }
            cmd.args(["--url", &url, "--admin-token-file"]).arg(&token);
            if json {
                cmd.arg("--json");
            }
            cmd.arg("node");
            if detail {
                cmd.arg("gpu-box");
            }
            let output = cmd.output().unwrap();
            server.join().unwrap();
            std::fs::remove_file(token).unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            if json {
                let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
                assert_eq!(if detail { value } else { value[0].clone() }, node);
            } else {
                let text = String::from_utf8(output.stdout).unwrap();
                if detail {
                    for expected in ["gpus 8", "cpus 48", "free_gpus 2", "free_cpus 0"] {
                        assert!(
                            text.lines().any(|line| line
                                .split_whitespace()
                                .collect::<Vec<_>>()
                                .join(" ")
                                == expected),
                            "{text}"
                        );
                    }
                } else {
                    let header = text.lines().next().unwrap();
                    assert!(
                        header.contains("CPUS")
                            && header.contains("FREE GPUS")
                            && header.contains("FREE CPUS"),
                        "{text}"
                    );
                    let cells: Vec<_> = text
                        .lines()
                        .nth(1)
                        .unwrap()
                        .split_whitespace()
                        .take(6)
                        .collect();
                    assert_eq!(
                        cells,
                        ["gpu-box", "ray-local", "8", "48", "2", "0"],
                        "{text}"
                    );
                }
            }
        }
    }
}
