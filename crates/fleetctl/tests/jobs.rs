//! Job listing sends selectors and uses each API's supported limit parameter.
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

fn list_query_is_scoped(command: &str, kind: &str, endpoint: &str, limit_param: &str, other_param: &str) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let token = std::env::temp_dir().join(format!("fleet-jobs-test-{}.token", std::process::id()));
    std::fs::write(&token, "test-admin").unwrap();
    let endpoint = endpoint.to_string();
    let limit_param = limit_param.to_string();
    let other_param = other_param.to_string();
    let batch = kind == "batch";
    let server = std::thread::spawn(move || {
        for expected_limit in ["1000", "10"] {
            let (mut stream, _) = listener.accept().unwrap();
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
            let request_line = headers.lines().next().unwrap();
            assert!(
                request_line.starts_with(&format!("GET {endpoint}?")),
                "{request_line}"
            );
            assert!(request_line.contains("namespace=alice"), "{request_line}");
            assert!(
                request_line.contains(&format!("{limit_param}={expected_limit}")),
                "{request_line}"
            );
            assert!(
                !request_line.contains(&format!("{other_param}=")),
                "unsupported query was sent: {request_line}"
            );
            let body = if batch {
                r#"[{"id":"training-job","type":"run","profile":"training","namespace":"alice","state":"succeeded","node":"gpu-machine","gpus":["0","1"]}]"#
            } else {
                "[]"
            };
            write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
        }
    });

    let mut command_line = cli();
    command_line
        .args([
            "--json",
            command,
            "--namespace",
            "alice",
            "--url",
            &url,
            "--admin-token-file",
        ])
        .arg(&token);
    if command == "job" { command_line.args(["--type", kind]); }
    let output = command_line.output().unwrap();
    std::fs::remove_file(token).unwrap();
    server.join().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let rows: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    if batch {
        assert_eq!(rows.as_array().unwrap().len(), 1);
        assert_eq!(rows[0]["type"], "batch");
        assert_eq!(rows[0]["record"]["type"], "batch");
        assert_eq!(rows[0]["namespace"], "alice");
        assert_eq!(rows[0]["app"], "training");
        assert_eq!(rows[0]["where"], "gpu-machine", "GPU card indices are execution details, not a node");
    } else {
        assert_eq!(rows, serde_json::json!([]));
    }
}

#[test]
fn job_lists_send_namespace_and_api_specific_limit_query() {
    list_query_is_scoped("job", "proof", "/admin/jobs", "limit", "n");
    list_query_is_scoped("job", "batch", "/batches", "limit", "n");
    list_query_is_scoped("batch", "batch", "/batches", "limit", "n");
}

#[test]
fn batch_submission_refuses_the_removed_code_upload_flag() {
    let output = cli()
        .args([
            "batch",
            "submit",
            "--profile",
            "training",
            "--code",
            "inputs.tar.gz",
            "--",
            "true",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("unexpected argument '--code'"));
}

#[test]
fn proof_fetch_requires_an_explicit_destination() {
    let output = cli()
        .args(["job", "fetch", "job-1234abcd"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
}

#[test]
fn batch_submission_passes_storage_references_without_uploading_files() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let token =
        std::env::temp_dir().join(format!("fleet-submit-test-{}.token", std::process::id()));
    std::fs::write(&token, "test-admin").unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        let mut bytes = Vec::new();
        let mut buffer = [0; 2048];
        let (header_end, body_length) = loop {
            let count = stream.read(&mut buffer).unwrap();
            assert!(count > 0);
            bytes.extend_from_slice(&buffer[..count]);
            if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&bytes[..end]);
                assert!(headers.starts_with("POST /batches HTTP/1.1"));
                let length = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length: ")
                            .map(|v| v.parse::<usize>().unwrap())
                    })
                    .unwrap();
                if bytes.len() >= end + 4 + length {
                    break (end + 4, length);
                }
            }
        };
        let body: serde_json::Value =
            serde_json::from_slice(&bytes[header_end..header_end + body_length]).unwrap();
        assert_eq!(
            body["env"],
            serde_json::json!(["INPUT_URI=s3://owner-bucket/dataset"])
        );
        assert_eq!(
            body["command"],
            serde_json::json!(["train", "--input", "s3://owner-bucket/dataset"])
        );
        assert!(body.get("code").is_none());
        let response = r#"{"id":"storage-job","state":"queued"}"#;
        write!(stream, "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).unwrap();
    });
    let output = cli()
        .args(["--json", "--url", &url, "--admin-token-file"])
        .arg(&token)
        .args([
            "batch",
            "submit",
            "--profile",
            "training",
            "--env",
            "INPUT_URI=s3://owner-bucket/dataset",
            "--",
            "train",
            "--input",
            "s3://owner-bucket/dataset",
        ])
        .output()
        .unwrap();
    std::fs::remove_file(token).unwrap();
    server.join().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn proof_fetch_downloads_both_files_through_the_scoped_control_plane() {
    let listener=TcpListener::bind("127.0.0.1:0").unwrap();
    let url=format!("http://{}",listener.local_addr().unwrap());
    let root=std::env::temp_dir().join(format!("fleet-fetch-{}-{}",std::process::id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    std::fs::create_dir(&root).unwrap();
    let token=root.join("token");std::fs::write(&token,"test-admin").unwrap();
    let server=std::thread::spawn(move || {
        for (name,body) in [("proof.json",b"{\"proof\":1}".as_slice()),("public_values.bin",b"\x00\x01\xff".as_slice())] {
            let (mut stream,_)=listener.accept().unwrap();
            stream.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();
            let mut request=Vec::new();let mut buf=[0;2048];
            while !request.windows(4).any(|w|w==b"\r\n\r\n") {let n=stream.read(&mut buf).unwrap();assert!(n>0);request.extend_from_slice(&buf[..n]);}
            let request=String::from_utf8_lossy(&request);
            assert!(request.starts_with(&format!("GET /admin/jobs/proof-job/output/{name}?namespace=alice&tag=production ")));
            assert!(request.to_ascii_lowercase().contains("authorization: bearer test-admin"));
            write!(stream,"HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len()).unwrap();stream.write_all(body).unwrap();
        }
    });
    let dest=root.join("output");
    let result=cli().args(["job","fetch","proof-job","--namespace","alice","--tag","production","--url",&url,"--admin-token-file"]).arg(&token).arg("--out").arg(&dest).output().unwrap();
    server.join().unwrap();
    assert!(result.status.success(),"{}",String::from_utf8_lossy(&result.stderr));
    assert_eq!(std::fs::read(dest.join("proof.json")).unwrap(),b"{\"proof\":1}");
    assert_eq!(std::fs::read(dest.join("public_values.bin")).unwrap(),b"\x00\x01\xff");
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn proof_fetch_rejects_existing_destination_before_requesting_output() {
    let root = std::env::temp_dir().join(format!("fleet-existing-output-{}-{}",std::process::id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    std::fs::create_dir(&root).unwrap();
    let token = root.join("token");
    std::fs::write(&token,"test-admin").unwrap();
    let marker = root.join("existing.txt");
    std::fs::write(&marker,"keep").unwrap();
    let result = cli().args(["job","fetch","proof-job","--url","http://127.0.0.1:1","--admin-token-file"])
        .arg(&token).arg("--out").arg(&root).output().unwrap();
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("already exists; choose a new directory"));
    assert_eq!(std::fs::read_to_string(marker).unwrap(),"keep");
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn finite_job_commands_are_only_available_under_batch() {
    for arguments in [
        vec!["job", "submit", "--profile", "training", "--", "true"],
        vec!["job", "logs", "job-id"],
        vec!["job", "cancel", "job-id"],
        vec!["job", "preempt", "job-id"],
        vec!["job", "purge", "job-id"],
        vec!["batch", "submit", "--app", "training", "--", "true"],
        vec!["batch", "logs", "job-id", "--rank", "1"],
    ] {
        let output = cli().args(&arguments).output().unwrap();
        assert_eq!(output.status.code(), Some(2), "{arguments:?}");
        assert!(!output.stderr.is_empty());
    }
}
