//! Job listing, lookup and proof fetching address each app's or batch's jobs, with the selectors.
mod common;
use common::{cli, stub, token_file};
use std::net::TcpListener;

/// `fleetctl job --type KIND` lists the active jobs, then the newest, of every app or batch.
fn list_query_is_scoped(kind: &str) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let token = token_file(&format!("jobs-{kind}"));
    let batch = kind == "batch";
    // The apps (or batches), then their jobs: for the active ones and for the newest.
    let server = stub(listener, 4, move |line, _| {
        let path = line.split_whitespace().nth(1).unwrap();
        (200, if path.starts_with("/apps?") {
            r#"[{"kind":"app","name":"ethproofs"}]"#.into()
        } else if path.starts_with("/batches?") {
            r#"[{"name":"training"}]"#.into()
        } else if batch {
            r#"[{"id":"training-job","type":"batch","batch":"training","namespace":"alice","state":"succeeded","node":"gpu-machine","gpus":["0","1"]}]"#.into()
        } else {
            r#"[{"id":"p-1","client_id":"job-1","app":"ethproofs","namespace":"alice","state":"succeeded","created_ms":2000,"node":"gpu-machine"}]"#.into()
        })
    });
    let output = cli().args(["--json", "job", "--type", kind, "--namespace", "alice", "--url", &url, "--token-file"])
        .arg(&token).output().unwrap();
    let seen = server.join().unwrap();
    std::fs::remove_file(token).unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let (list, jobs) = if batch { ("/batches?", "/batches/training/jobs?") } else { ("/apps?", "/apps/ethproofs/jobs?") };
    for (index, limit) in [(1, "1000"), (3, "10")] {
        assert!(seen[index - 1].line.contains(list), "{}", seen[index - 1].line);
        let line = &seen[index].line;
        assert!(line.starts_with(&format!("GET {jobs}")) && line.contains("namespace=alice") && line.contains(&format!("limit={limit}")), "{line}");
        assert!(!line.contains("n="), "unsupported query was sent: {line}");
    }
    let rows: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 1, "{rows}");
    assert_eq!(rows[0]["type"], if batch { "batch" } else { "app" });
    assert_eq!(rows[0]["namespace"], "alice");
    assert_eq!(rows[0]["app"], if batch { "training" } else { "ethproofs" });
    assert_eq!(rows[0]["where"], "gpu-machine", "GPU card indices are execution details, not a node");
}

#[test]
fn job_lists_send_namespace_and_api_specific_limit_query() {
    list_query_is_scoped("app");
    list_query_is_scoped("batch");
}

#[test]
fn batch_submission_passes_storage_references_without_uploading_files() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let token = token_file("submit");
    let server = stub(listener, 1, |_, _| (201, r#"{"id":"storage-job","state":"queued"}"#.into()));
    let output = cli()
        .args(["--json", "--url", &url, "--token-file"])
        .arg(&token)
        .args(["batch", "submit", "training", "--env", "INPUT_URI=s3://owner-bucket/dataset", "--",
            "train", "--input", "s3://owner-bucket/dataset"])
        .output()
        .unwrap();
    let seen = server.join().unwrap();
    std::fs::remove_file(token).unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert!(seen[0].line.starts_with("POST /batches/training/jobs HTTP/1.1"), "{}", seen[0].line);
    let body = &seen[0].body;
    assert_eq!(body["env"], serde_json::json!(["INPUT_URI=s3://owner-bucket/dataset"]));
    assert_eq!(body["command"], serde_json::json!(["train", "--input", "s3://owner-bucket/dataset"]));
    assert!(body.get("code").is_none());
    assert!(body.get("batch").is_none(), "the batch is in the path");
}

#[test]
fn proof_fetch_downloads_both_files_through_the_scoped_control_plane() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let root = std::env::temp_dir().join(format!("fleet-fetch-{}-{}", std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    std::fs::create_dir(&root).unwrap();
    let token = root.join("token");
    std::fs::write(&token, "test-admin").unwrap();
    // The proof apps, the job (it is found under one), then its two files.
    let server = stub(listener, 4, |line, _| {
        let path = line.split_whitespace().nth(1).unwrap();
        (200, if path.starts_with("/apps?") {
            r#"[{"kind":"app","name":"ethproofs"}]"#.into()
        } else if path.starts_with("/apps/ethproofs/jobs/proof-job/output/proof.json?") {
            "{\"proof\":1}".into()
        } else if path.starts_with("/apps/ethproofs/jobs/proof-job/output/public_values.bin?") {
            "values".into()
        } else if path.starts_with("/apps/ethproofs/jobs/proof-job?") {
            r#"{"id":"p-1","client_id":"proof-job"}"#.into()
        } else {
            panic!("unexpected request {line}");
        })
    });
    let dest = root.join("output");
    let result = cli().args(["job", "fetch", "proof-job", "--namespace", "alice", "--tag", "scope=production", "--url", &url, "--token-file"])
        .arg(&token).arg("--out").arg(&dest).output().unwrap();
    let seen = server.join().unwrap();
    assert!(result.status.success(), "{}", String::from_utf8_lossy(&result.stderr));
    for s in &seen {
        assert!(s.line.contains("namespace=alice&tag=scope%3Dproduction"), "{}", s.line);
        assert!(s.headers.contains("authorization: bearer test-admin"));
    }
    assert_eq!(std::fs::read(dest.join("proof.json")).unwrap(), b"{\"proof\":1}");
    assert_eq!(std::fs::read(dest.join("public_values.bin")).unwrap(), b"values");
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn proof_fetch_rejects_existing_destination_before_requesting_output() {
    let root = std::env::temp_dir().join(format!("fleet-existing-output-{}-{}", std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    std::fs::create_dir(&root).unwrap();
    let token = root.join("token");
    std::fs::write(&token, "test-admin").unwrap();
    let marker = root.join("existing.txt");
    std::fs::write(&marker, "keep").unwrap();
    let result = cli().args(["job", "fetch", "proof-job", "--url", "http://127.0.0.1:1", "--token-file"])
        .arg(&token).arg("--out").arg(&root).output().unwrap();
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("already exists; choose a new directory"));
    assert_eq!(std::fs::read_to_string(marker).unwrap(), "keep");
    std::fs::remove_dir_all(root).unwrap();
}
