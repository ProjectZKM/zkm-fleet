//! App registration carries an explicit namespace to the private API.
use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::Command;

fn cli() -> Command {
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
    cmd
}

#[test]
fn batch_creation_requires_namespace_before_contacting_api() {
    let output = cli()
        .args([
            "batch", "add", "training", "--image", "python:3", "--allow", "/tmp",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("requires --namespace"));
}

#[test]
fn batch_creation_with_user_token_leaves_namespace_to_verified_api_scope() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let token = std::env::temp_dir().join(format!("fleet-batch-owner-test-{}.token", std::process::id()));
    std::fs::write(&token, "owner-token").unwrap();
    let server = std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut stream = loop {
            if let Ok((stream, _)) = listener.accept() { break stream; }
            assert!(std::time::Instant::now() < deadline, "CLI rejected token-inferred namespace");
            std::thread::sleep(std::time::Duration::from_millis(10));
        };
        stream.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();
        let mut request = Vec::new();
        let mut byte = [0];
        while !request.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).unwrap(); request.push(byte[0]);
        }
        let headers = String::from_utf8(request).unwrap();
        // Drain the request body before closing the socket; unread bytes can
        // turn an otherwise successful response into a TCP connection reset.
        let length = headers.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().unwrap())
        }).unwrap();
        let mut request_body = vec![0; length];
        stream.read_exact(&mut request_body).unwrap();
        assert!(headers.starts_with("POST /batches HTTP/1.1"));
        assert!(headers.to_ascii_lowercase().contains("authorization: bearer owner-token"));
        let body = r#"{"name":"training","namespace":"alice","type":"batch","persistent":false}"#;
        write!(stream, "HTTP/1.1 201 Created\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
    });
    let output = cli().args(["--json", "--url", &url, "--token-file"])
        .arg(&token).args(["batch", "add", "training", "--image", "python:3"])
        .output().unwrap();
    server.join().unwrap(); std::fs::remove_file(token).unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let batch: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(batch["namespace"], "alice");
    assert_eq!(batch["type"], "batch");
}

fn app_list_server(listener: TcpListener) -> std::thread::JoinHandle<()> {
    listener.set_nonblocking(true).unwrap();
    std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        for endpoint in ["/admin/apps", "/apps"] {
            let mut stream = loop {
                if let Ok((stream, _)) = listener.accept() { break stream; }
                assert!(std::time::Instant::now() < deadline, "app list did not contact {endpoint}");
                std::thread::sleep(std::time::Duration::from_millis(10));
            };
            stream.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();
            let mut headers = Vec::new();
            let mut byte = [0];
            while !headers.ends_with(b"\r\n\r\n") {
                stream.read_exact(&mut byte).unwrap();
                headers.push(byte[0]);
            }
            assert!(String::from_utf8(headers).unwrap()
                .starts_with(&format!("GET {endpoint}?namespace=alice&tag=scope%3Ddefault HTTP/1.1")));
            let body = if endpoint == "/admin/apps" {
                serde_json::json!([{
                    "name":"eth/aaaaaaaa", "type":"app", "namespace":"alice",
                    "disabled":true, "last_created_ms":2000, "mean_prove_ms":1200, "elf_id":"a".repeat(64),
                    "tags":["scope=default"], "state":"disabled",
                    "resources":{"gpus":6,"cpus":48,"replicas":1,"nodes":["gpu-machine"]},
                    "namespace_quota":{"gpus":0,"cpus":null}
                }])
            } else {
                serde_json::json!([
                    {"name":"training","type":"run","namespace":"alice","persistent":false},
                    {"name":"comfyui","type":"app","namespace":"alice",
                     "persistent":true,"last_submitted_at":3,"image":"comfyui:latest",
                     "tags":["scope=default"], "state":"starting",
                     "detail":"waiting for capacity: needs 1 GPU and 2 CPU, gpu-machine has 0 GPU and 8 CPU free",
                     "resources":{"gpus":1,"cpus":2,"replicas":1,"nodes":["gpu-machine"]}}
                ])
            }.to_string();
            write!(stream, "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
        }
    })
}

#[test]
fn app_list_unifies_apps_and_excludes_batches() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let token = std::env::temp_dir().join(format!("fleet-batch-app-test-{}.token", std::process::id()));
    std::fs::write(&token, "test-admin").unwrap();
    let server = app_list_server(listener);
    let output = cli().args([
        "--json", "--namespace", "alice", "--tag", "scope=default", "--url", &url, "--admin-token-file",
    ]).arg(&token).arg("app").output().unwrap();
    server.join().unwrap();
    std::fs::remove_file(token).unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let apps = result.as_array().unwrap();
    assert_eq!(apps.len(), 2);
    assert_eq!(apps[0]["name"], "comfyui");
    assert_eq!(apps[1]["name"], "eth/aaaaaaaa");
    assert_eq!(apps[1]["elf_id"], "a".repeat(64));
    assert_eq!(apps[1]["mean_prove_ms"], 1200);
    for app in apps {
        assert_eq!(app["type"], "app");
        assert!(app.get("kind").is_none());
        assert_eq!(app["tags"], serde_json::json!(["scope=default"]));
    }
}

#[test]
fn app_list_shows_why_a_starting_app_is_not_running() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let token = std::env::temp_dir().join(format!("fleet-app-detail-test-{}.token", std::process::id()));
    std::fs::write(&token, "test-admin").unwrap();
    let server = app_list_server(listener);
    let output = cli().args(["--namespace", "alice", "--tag", "scope=default", "--url", &url, "--admin-token-file"])
        .arg(&token).arg("app").output().unwrap();
    server.join().unwrap();
    std::fs::remove_file(token).unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let table = String::from_utf8_lossy(&output.stdout);
    let row = table.lines().find(|l| l.contains("comfyui")).unwrap();
    assert!(row.contains("starting") && row.contains("waiting for capacity: needs 1 GPU and 2 CPU"), "{row}");
    assert!(row.contains("image comfyui:latest"), "the reason comes first, other details stay: {row}");
}

#[test]
fn batch_creation_sends_namespace_selector() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let token = std::env::temp_dir().join(format!("fleet-app-test-{}.token", std::process::id()));
    std::fs::write(&token, "test-admin").unwrap();
    let server = std::thread::spawn(move || {
        listener.set_nonblocking(true).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let mut stream = loop {
            if let Ok((s, _)) = listener.accept() {
                break s;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "CLI never contacted the API"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        };
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        let mut request = Vec::new();
        let mut buffer = [0; 2048];
        loop {
            let count = stream.read(&mut buffer).unwrap();
            assert!(count > 0);
            request.extend_from_slice(&buffer[..count]);
            if request.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        let headers = String::from_utf8_lossy(&request);
        assert!(
            headers.starts_with("POST /batches?namespace=alice HTTP/1.1\r\n"),
            "{headers}"
        );
        assert!(headers
            .to_ascii_lowercase()
            .contains("authorization: bearer test-admin\r\n"));
        let header_end = request.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
        let length: usize = headers
            .lines()
            .find_map(|l| {
                l.to_lowercase()
                    .strip_prefix("content-length: ")
                    .map(str::to_owned)
            })
            .unwrap()
            .parse()
            .unwrap();
        let mut body_bytes = request[header_end..].to_vec();
        let mut rest = vec![0; length - body_bytes.len()];
        stream.read_exact(&mut rest).unwrap();
        body_bytes.extend(rest);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body_bytes).unwrap()["priority"],
            77
        );
        let body = r#"{"priority":77,"name":"training","namespace":"11111111111111111111111111111111","image":"python:3","max_gpus":8,"allow":["/tmp"]}"#;
        write!(
            stream,
            "HTTP/1.1 201 Created\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
    });
    let output = cli()
        .args([
            "--json",
            "batch",
            "add",
            "training",
            "--namespace",
            "alice",
            "--url",
            &url,
            "--image",
            "python:3",
            "--allow",
            "/tmp",
            "--priority",
            "77",
            "--admin-token-file",
        ])
        .arg(&token)
        .output()
        .unwrap();
    std::fs::remove_file(token).unwrap();
    server.join().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let app: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(app["namespace"], "1".repeat(32));
    assert_eq!(app["priority"], 77);
}

#[test]
fn app_priority_uses_admin_policy_routes_for_both_app_types() {
    for proof in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let token = std::env::temp_dir().join(format!(
            "fleet-priority-test-{}-{proof}.token",
            std::process::id()
        ));
        std::fs::write(&token, "test-admin").unwrap();
        let elf = "a".repeat(64);
        let name = if proof {
            format!("eth/{elf}")
        } else {
            "comfyui".into()
        };
        let app = serde_json::json!({"name":name,"tenant":"eth","elf_id":elf,"namespace":"1".repeat(32),"priority":0,"persistent":true});
        let server = std::thread::spawn(move || {
            listener.set_nonblocking(true).unwrap();
            for index in 0..2 {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
                let mut stream = loop {
                    if let Ok((s, _)) = listener.accept() {
                        break s;
                    }
                    assert!(std::time::Instant::now() < deadline);
                    std::thread::sleep(std::time::Duration::from_millis(10));
                };
                stream
                    .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                let mut bytes = Vec::new();
                let mut byte = [0];
                while !bytes.ends_with(b"\r\n\r\n") {
                    stream.read_exact(&mut byte).unwrap();
                    bytes.push(byte[0]);
                }
                let headers = String::from_utf8(bytes).unwrap();
                assert!(headers
                    .to_lowercase()
                    .contains("authorization: bearer test-admin"));
                if index == 0 {
                    assert!(
                        headers.starts_with(if proof {
                            "GET /admin/apps?namespace=alice "
                        } else {
                            "GET /apps?namespace=alice "
                        }),
                        "{headers}"
                    );
                } else if proof {
                    assert!(headers.starts_with("PUT /admin/apps/priority?namespace=11111111111111111111111111111111&tenant=eth&elf_id="), "{headers}");
                    assert!(headers
                        .lines()
                        .next()
                        .unwrap()
                        .ends_with("&priority=-5 HTTP/1.1"));
                } else {
                    assert!(
                        headers.starts_with("PUT /apps/comfyui/priority?namespace=alice "),
                        "{headers}"
                    );
                    let length: usize = headers
                        .lines()
                        .find_map(|l| {
                            l.to_lowercase()
                                .strip_prefix("content-length: ")
                                .map(str::to_owned)
                        })
                        .unwrap()
                        .parse()
                        .unwrap();
                    let mut body = vec![0; length];
                    stream.read_exact(&mut body).unwrap();
                    assert_eq!(
                        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
                        serde_json::json!({"priority":-5})
                    );
                }
                let body = if index == 0 {
                    serde_json::json!([app])
                } else {
                    serde_json::json!({"priority":-5})
                }
                .to_string();
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
        });
        let output = cli()
            .args([
                "--json",
                "--namespace",
                "alice",
                "--url",
                &url,
                "--admin-token-file",
            ])
            .arg(&token)
            .args(["app", "priority", &name, "-5"])
            .output()
            .unwrap();
        server.join().unwrap();
        std::fs::remove_file(&token).unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["priority"],
            -5
        );
    }
}

#[test]
fn proof_app_disable_uses_control_plane_for_lifecycle_and_canonical_namespace() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let token =
        std::env::temp_dir().join(format!("fleet-disable-test-{}.token", std::process::id()));
    std::fs::write(&token, "test-admin").unwrap();
    let elf = "a".repeat(64);
    let hash = "1".repeat(32);
    let app_elf = elf.clone();
    let server = std::thread::spawn(move || {
        for index in 0..2 {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
            let mut bytes = Vec::new();
            let mut byte = [0];
            while !bytes.ends_with(b"\r\n\r\n") {
                stream.read_exact(&mut byte).unwrap();
                bytes.push(byte[0]);
            }
            let headers = String::from_utf8(bytes).unwrap();
            let request = headers.lines().next().unwrap();
            let body = if index == 0 {
                assert!(
                    request.starts_with("GET /admin/apps?namespace=alice HTTP/1.1"),
                    "{request}"
                );
                serde_json::json!([{"name":format!("eth/{}", &app_elf[..8]), "namespace":hash,
                    "tenant":"eth", "elf_id":app_elf, "disabled":false}])
                .to_string()
            } else {
                assert!(request.starts_with(&format!("PUT /proof-apps/disabled?namespace={hash}&tenant=eth&elf_id={app_elf} HTTP/1.1")), "{request}");
                assert!(
                    !request.contains("namespace=alice"),
                    "duplicate namespace: {request}"
                );
                serde_json::json!({"namespace":hash,"tenant":"eth","elf_id":app_elf,"disabled":true,"servers":["eth-a"]}).to_string()
            };
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        }
    });
    let output = cli()
        .args(["--namespace", "alice", "--url", &url, "--admin-token-file"])
        .arg(&token)
        .args(["app", "disable", &format!("eth/{elf}")])
        .output()
        .unwrap();
    std::fs::remove_file(token).unwrap();
    server.join().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("dedicated GPU server stopped"));
}

#[test]
fn app_scale_and_status_use_only_control_plane_routes_and_preserve_omitted_settings() {
    for proof in [false,true] { for inspect in [false,true] {
        let listener=TcpListener::bind("127.0.0.1:0").unwrap();listener.set_nonblocking(true).unwrap();
        let url=format!("http://{}",listener.local_addr().unwrap());
        let token=std::env::temp_dir().join(format!("fleet-scale-{}-{proof}-{inspect}.token",std::process::id()));
        std::fs::write(&token,"test-admin").unwrap();
        let elf="a".repeat(64);let ns="1".repeat(32);
        let name=if proof {format!("eth/{elf}")} else {"comfyui".into()};
        let app=serde_json::json!({"name":name,"tenant":"eth","elf_id":elf,"namespace":ns,"persistent":true});
        let server=std::thread::spawn(move || {
            for index in 0..2 {
                let deadline=std::time::Instant::now()+std::time::Duration::from_secs(10);
                let mut stream=loop {
                    if let Ok((s,_))=listener.accept() {break s;}
                    assert!(std::time::Instant::now()<deadline,"scale CLI did not contact control plane");
                    std::thread::sleep(std::time::Duration::from_millis(10));
                };
                stream.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();
                let mut headers=Vec::new();let mut byte=[0];
                while !headers.ends_with(b"\r\n\r\n") {stream.read_exact(&mut byte).unwrap();headers.push(byte[0]);}
                let headers=String::from_utf8(headers).unwrap();
                assert!(headers.to_lowercase().contains("authorization: bearer test-admin"));
                if index==0 {assert!(headers.starts_with(if proof {"GET /admin/apps?namespace=alice "} else {"GET /apps?namespace=alice "}),"{headers}");}
                else {
                    let line=headers.lines().next().unwrap();
                    assert!(line.starts_with(if inspect {"GET "}else{"PUT "}));
                    assert!(line.contains(if proof {"/proof-apps/scale?"}else{"/apps/comfyui/scale?"}),"{line}");
                    assert!(line.contains(&format!("namespace={ns}")),"{line}");
                    if proof {assert!(line.contains(&format!("elf_id={elf}")));}
                    if inspect {assert!(line.contains("node=gpu-box"));}
                    else {
                        let size:usize=headers.lines().find_map(|l|l.to_lowercase().strip_prefix("content-length: ").map(str::to_owned)).unwrap().parse().unwrap();
                        let mut bytes=vec![0;size];stream.read_exact(&mut bytes).unwrap();
                        assert_eq!(serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(),serde_json::json!({"replicas":2,"node":"gpu-box"}));
                    }
                }
                let body=if index==0 {serde_json::json!([app])}else{serde_json::json!({"state":"draining","replicas":2,"gpus":2,"cpus":16,"node":"gpu-box"})}.to_string();
                write!(stream,"HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).unwrap();
            }
        });
        let mut cmd=cli();cmd.args(["--json","--namespace","alice","--url",&url,"--admin-token-file"]).arg(&token)
            .args(["app","scale",&name,"--node","gpu-box"]);
        if !inspect {cmd.args(["--replicas","2"]);}
        let output=cmd.output().unwrap();server.join().unwrap();std::fs::remove_file(token).unwrap();
        assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));
        assert_eq!(serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["replicas"],2);
    }}
}

#[test]
fn app_logs_read_the_control_plane_route_and_print_the_raw_log_or_the_error() {
    for proof in [false, true] { for status in [200u16, 503] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap(); listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let token = std::env::temp_dir().join(format!("fleet-logs-{}-{proof}-{status}.token", std::process::id()));
        std::fs::write(&token, "test-admin").unwrap();
        let elf = "a".repeat(64); let ns = "1".repeat(32);
        let name = if proof { format!("eth/{elf}") } else { "comfyui".into() };
        let app = serde_json::json!({"name":name,"tenant":"eth","elf_id":elf,"namespace":ns,"persistent":true});
        let server = std::thread::spawn(move || {
            for index in 0..2 {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
                let mut stream = loop {
                    if let Ok((s, _)) = listener.accept() { break s; }
                    assert!(std::time::Instant::now() < deadline, "logs CLI did not contact control plane");
                    std::thread::sleep(std::time::Duration::from_millis(10));
                };
                stream.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();
                let mut headers = Vec::new(); let mut byte = [0];
                while !headers.ends_with(b"\r\n\r\n") { stream.read_exact(&mut byte).unwrap(); headers.push(byte[0]); }
                let headers = String::from_utf8(headers).unwrap();
                assert!(headers.to_lowercase().contains("authorization: bearer test-admin"));
                if index == 0 {
                    let body = serde_json::json!([app]).to_string();
                    write!(stream, "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
                    continue;
                }
                let line = headers.lines().next().unwrap();
                assert!(line.starts_with("GET "), "{line}");
                assert!(line.contains(if proof { "/proof-apps/logs?" } else { "/apps/comfyui/logs?" }), "{line}");
                assert!(line.contains(&format!("namespace={ns}")) && line.contains("lines=7") && line.contains("node=gpu-box"), "{line}");
                if proof { assert!(line.contains(&format!("elf_id={elf}")), "{line}"); }
                let body = if status == 200 { "alpha\nbeta\n" } else { "node is offline" };
                write!(stream, "HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
            }
        });
        let output = cli().args(["--namespace", "alice", "--url", &url, "--admin-token-file"]).arg(&token)
            .args(["app", "logs", &name, "-n", "7", "--node", "gpu-box"]).output().unwrap();
        server.join().unwrap(); std::fs::remove_file(token).unwrap();
        if status == 200 {
            assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
            assert_eq!(String::from_utf8_lossy(&output.stdout), "alpha\nbeta\n");
        } else {
            assert!(!output.status.success());
            assert!(String::from_utf8_lossy(&output.stderr).contains("503") && String::from_utf8_lossy(&output.stderr).contains("node is offline"));
        }
    }}
}

/// A stub control plane that answers each GET by path and records what was asked.
fn batch_server(listener: TcpListener, requests: usize) -> std::thread::JoinHandle<Vec<String>> {
    listener.set_nonblocking(true).unwrap();
    std::thread::spawn(move || {
        let mut seen = Vec::new();
        for _ in 0..requests {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            let mut stream = loop {
                if let Ok((s, _)) = listener.accept() { break s; }
                assert!(std::time::Instant::now() < deadline, "CLI stopped after {seen:?}");
                std::thread::sleep(std::time::Duration::from_millis(10));
            };
            stream.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();
            let mut headers = Vec::new(); let mut byte = [0];
            while !headers.ends_with(b"\r\n\r\n") { stream.read_exact(&mut byte).unwrap(); headers.push(byte[0]); }
            let line = String::from_utf8(headers).unwrap().lines().next().unwrap().to_string();
            let path = line.split_whitespace().nth(1).unwrap().to_string();
            let (status, body) = if path.starts_with("/batches?") {
                (200, serde_json::json!([
                    {"name":"training","namespace":"alice","priority":3,"max_gpus":2,"image":"python:3"},
                    {"name":"idle","namespace":"alice","disabled":true,"image":"python:3"}]).to_string())
            } else if path.starts_with("/batches/training/jobs?") {
                (200, serde_json::json!([
                    {"id":"a","batch":"training","state":"running","submitted_at":1_800_000_100.0},
                    {"id":"b","batch":"training","state":"succeeded","submitted_at":1_800_000_000.0}]).to_string())
            } else if path.starts_with("/batches/idle/jobs?") {
                (200, "[]".to_string())
            } else {
                panic!("unexpected request {line}");
            };
            write!(stream, "HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
            seen.push(path);
        }
        seen
    })
}

#[test]
fn plain_batch_lists_every_batch_like_app() {
    let token = std::env::temp_dir().join(format!("fleet-batches-{}.token", std::process::id()));
    std::fs::write(&token, "test-admin").unwrap();
    // The batches, then each batch's active jobs and its recent ones to count and date them.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = batch_server(listener, 7);
    let output = cli().args(["--json", "batch", "--namespace", "alice", "--url", &url, "--admin-token-file"])
        .arg(&token).output().unwrap();
    let seen = server.join().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert!(seen[0].starts_with("/batches?"), "{seen:?}");
    assert_eq!(seen.iter().filter(|p| p.starts_with("/batches/training/jobs?")).count(), 2, "{seen:?}");
    let rows: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(rows[0]["name"], "training");
    assert_eq!(rows[0]["active_jobs"], 1, "the running job, counted once although both lists return it");
    assert_eq!(rows[0]["last_at"], 1_800_000_100.0);
    assert_eq!((rows[1]["state"].as_str(), rows[1]["active_jobs"].as_u64()), (Some("disabled"), Some(0)));
    assert!(rows[1]["last_at"].is_null());
    std::fs::remove_file(token).unwrap();
}
