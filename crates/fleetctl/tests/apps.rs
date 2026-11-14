//! fleetctl's app and batch commands against a stub control plane.
mod common;
use common::{cli, stub, token_file};
use std::io::{Read, Write};
use std::net::TcpListener;

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
        let body = r#"{"type":"batch","name":"training","namespace":"alice","batch":{"image":"python:3"}}"#;
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

/// The app list, as `GET /apps` returns it: a model server and a proof app.
fn app_list() -> String {
    serde_json::json!([
        {"type":"app", "name":"comfyui", "namespace":"alice",
         "tags":["scope=default"], "state":"starting", "priority":0, "disabled":false,
         "detail":"waiting for capacity: needs 1 GPU and 2 CPU, gpu-machine has 0 GPU and 8 CPU free",
         "resources":{"gpus":1,"cpus":2,"replicas":1,"nodes":["gpu-machine"]},
         "jobs":{"total":0,"active":0,"succeeded":0,"failed":0,"last_at":3.0},
         "spec":{"image":"comfyui:latest"}},
        {"type":"app", "name":"ethproofs", "namespace":"alice",
         "tags":["scope=default"], "state":"disabled", "priority":0, "disabled":true,
         "resources":{"gpus":6,"cpus":48,"replicas":1,"nodes":["gpu-machine"]},
         "namespace_quota":{"gpus":0,"cpus":null},
         "jobs":{"total":4,"active":0,"succeeded":4,"failed":0,"last_at":2.0},
         "proof":{"elf_id":"a".repeat(64),"class":"latency","anchor":"exclusive","mean_prove_ms":1200,
                  "image":"ghcr.io/projectzkm/ziren-gpu@sha256:d4b8"}},
    ]).to_string()
}

#[test]
fn app_list_shows_every_app_from_one_request() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let token = token_file("app-list");
    let server = stub(listener, 1, |_, _| (200, app_list()));
    let output = cli().args(["--json", "--namespace", "alice", "--tag", "scope=default", "--url", &url, "--token-file"])
        .arg(&token).arg("app").output().unwrap();
    let seen = server.join().unwrap();
    std::fs::remove_file(token).unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert!(seen[0].line.starts_with("GET /apps?namespace=alice&tag=scope%3Ddefault HTTP/1.1"), "{}", seen[0].line);
    assert!(seen[0].headers.contains("authorization: bearer test-admin"));
    let apps: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let apps = apps.as_array().unwrap();
    assert_eq!((apps[0]["name"].as_str(), apps[0]["spec"]["image"].as_str()), (Some("comfyui"), Some("comfyui:latest")));
    assert_eq!(apps[1]["name"], "ethproofs");
    assert_eq!(apps[1]["proof"]["mean_prove_ms"], 1200);
    for app in apps {
        assert_eq!(app["type"], "app");
        assert_eq!(app["tags"], serde_json::json!(["scope=default"]));
    }
}

#[test]
fn app_list_shows_why_a_starting_app_is_not_running() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let token = token_file("app-detail");
    let server = stub(listener, 1, |_, _| (200, app_list()));
    let output = cli().args(["--namespace", "alice", "--tag", "scope=default", "--url", &url, "--token-file"])
        .arg(&token).arg("app").output().unwrap();
    server.join().unwrap();
    std::fs::remove_file(token).unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let table = String::from_utf8_lossy(&output.stdout);
    let head = table.lines().next().unwrap();
    assert!(head.ends_with("IMAGE") && head.contains("DETAIL"), "the image has its own last column, as in fleetctl batch: {table}");
    let row = table.lines().find(|l| l.contains("comfyui")).unwrap();
    assert!(row.contains("starting") && row.contains("waiting for capacity: needs 1 GPU and 2 CPU"), "{row}");
    assert!(row.ends_with("comfyui:latest") && !row.contains("image comfyui"), "the image is a column, not a detail: {row}");
    let row = table.lines().find(|l| l.contains("ethproofs")).unwrap();
    assert!(row.starts_with("ethproofs") && row.contains("mean prove 1.2 s"), "{row}");
    assert!(row.ends_with("ghcr.io/projectzkm/ziren-gpu@sha256:d4b8"), "a proof app shows its prover image: {row}");
}

#[test]
fn batch_creation_sends_namespace_selector() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let token = token_file("batch-add");
    let server = stub(listener, 1, |_, _| (201,
        r#"{"type":"batch","name":"training","namespace":"11111111111111111111111111111111","priority":77,"batch":{"image":"python:3","max_gpus":8,"allow":["/tmp"]}}"#.into()));
    let output = cli().args(["--json", "batch", "add", "training", "--namespace", "alice", "--url", &url,
            "--image", "python:3", "--allow", "/tmp", "--priority", "77", "--token-file"])
        .arg(&token).output().unwrap();
    let seen = server.join().unwrap();
    std::fs::remove_file(token).unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert!(seen[0].line.starts_with("POST /batches?namespace=alice HTTP/1.1"), "{}", seen[0].line);
    assert_eq!(seen[0].body["priority"], 77);
    let batch: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(batch["namespace"], "1".repeat(32));
    assert_eq!(batch["priority"], 77);
}

#[test]
fn app_creation_sends_the_spec_and_a_provers_program_and_policy() {
    let elf = std::env::temp_dir().join(format!("fleet-app-add-{}.elf", std::process::id()));
    std::fs::write(&elf, b"program").unwrap();
    let spec = std::env::temp_dir().join(format!("fleet-app-add-{}.json", std::process::id()));
    std::fs::write(&spec, r#"{"kind":"ziren-gpu","image":"ziren-gpu:1","gpus":4}"#).unwrap();
    let elf_id = { use sha2::Digest; hex::encode(sha2::Sha256::digest(b"program")) };
    for by_file in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let token = token_file(&format!("app-add-{by_file}"));
        let server = stub(listener, 1, |_, body| (201, body.to_string()));
        let mut cmd = cli();
        cmd.args(["--json", "--namespace", "zkm", "--url", &url, "--token-file"]).arg(&token)
            .args(["app", "add", "ethproofs-z4", "--spec"]).arg(&spec).args(["--class", "latency", "--anchor", "exclusive", "--backend", "cuda", "--priority", "-2"]);
        if by_file { cmd.arg("--elf").arg(&elf); } else { cmd.args(["--elf-id", &format!("0x{}", elf_id.to_uppercase())]); }
        let output = cmd.output().unwrap();
        let seen = server.join().unwrap();
        std::fs::remove_file(token).unwrap();
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        assert!(seen[0].line.starts_with("POST /apps?namespace=zkm HTTP/1.1"), "{}", seen[0].line);
        assert_eq!(seen[0].body, serde_json::json!({"name":"ethproofs-z4","priority":-2,
            "spec":{"kind":"ziren-gpu","image":"ziren-gpu:1","gpus":4},
            "proof":{"elf_id":elf_id,"class":"latency","anchor":"exclusive","backends":["cuda"]}}));
    }
    std::fs::remove_file(elf).unwrap();
    let refused = |args: &[&str]| {
        let output = cli().args(["--namespace", "zkm", "--url", "http://127.0.0.1:1", "--token-file", "/dev/null", "app", "add", "x"])
            .args(args).output().unwrap();
        assert!(!output.status.success());
        String::from_utf8_lossy(&output.stderr).into_owned()
    };
    assert!(refused(&["--elf-id", "aa"]).contains("--spec"));
    let spec_arg = spec.to_str().unwrap();
    assert!(refused(&["--spec", spec_arg, "--class", "latency"]).contains("--elf-id ID or --elf FILE"));
    std::fs::remove_file(spec).unwrap();
}

#[test]
fn app_commands_address_every_app_by_its_name() {
    for (args, method, leaf, body, reply) in [
        (vec!["priority", "ethproofs", "-5"], "PUT", "/priority", serde_json::json!({"priority":-5}), r#"{"priority":-5}"#),
        (vec!["disable", "ethproofs"], "PUT", "/disabled", serde_json::Value::Null,
            r#"{"name":"ethproofs","disabled":true,"resources":{"gpus":1},"proof":{"elf_id":"aaaa"}}"#),
        (vec!["enable", "comfyui"], "DELETE", "/disabled", serde_json::Value::Null, r#"{"name":"comfyui","disabled":false,"spec":{"image":"comfyui:1"}}"#),
        (vec!["scale", "ethproofs", "--replicas", "2"], "PUT", "/scale", serde_json::json!({"replicas":2}),
            r#"{"state":"draining","replicas":2,"gpus":2,"cpus":16,"nodes":["gpu-box"]}"#),
        (vec!["update", "comfyui", "--image", "comfyui:2"], "PUT", "/settings", serde_json::json!({"image":"comfyui:2"}),
            r#"{"state":"draining","node":"gpu-box"}"#),
        (vec!["update", "sam3", "--machine", "10-60-85-215"], "PUT", "/settings", serde_json::json!({"machine":"10-60-85-215"}),
            r#"{"state":"running","node":"gpu-box"}"#),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let token = token_file(&format!("app-{}-{}", args[0], args[1]));
        let server = stub(listener, 1, move |_, _| (200, reply.into()));
        let output = cli().args(["--json", "--namespace", "alice", "--url", &url, "--token-file"]).arg(&token)
            .arg("app").args(&args).output().unwrap();
        let seen = server.join().unwrap();
        std::fs::remove_file(token).unwrap();
        assert!(output.status.success(), "{args:?}: {}", String::from_utf8_lossy(&output.stderr));
        assert!(seen[0].line.starts_with(&format!("{method} /apps/{}{leaf}?namespace=alice", args[1])), "{args:?}: {}", seen[0].line);
        assert_eq!(seen[0].body, body, "{args:?}");
    }
}

/// An app's replicas and their resources are the app's: scaling names no node, and a node
/// only narrows what is inspected.
#[test]
fn app_scale_takes_no_node_and_inspects_one_nodes_part() {
    let token = token_file("app-scale-node");
    let output = cli().args(["--namespace", "alice", "--url", "http://127.0.0.1:9", "--token-file"]).arg(&token)
        .args(["app", "scale", "comfyui", "--replicas", "1", "--node", "gpu-box"]).output().unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("scale it without --node"), "{}", String::from_utf8_lossy(&output.stderr));

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = stub(listener, 1, move |_, _| (200, r#"{"state":"complete","replicas":1,"gpus":1,"cpus":4,"node":"gpu-box"}"#.into()));
    let output = cli().args(["--namespace", "alice", "--url", &url, "--token-file"]).arg(&token)
        .args(["app", "scale", "comfyui", "--node", "gpu-box"]).output().unwrap();
    let seen = server.join().unwrap();
    std::fs::remove_file(token).unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert!(seen[0].line.starts_with("GET /apps/comfyui/scale?namespace=alice&node=gpu-box"), "{}", seen[0].line);
    assert!(String::from_utf8_lossy(&output.stdout).contains("on gpu-box"), "{}", String::from_utf8_lossy(&output.stdout));
}

#[test]
fn app_logs_read_the_control_plane_route_and_print_the_raw_log_or_the_error() {
    for status in [200u16, 503] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let token = token_file(&format!("app-logs-{status}"));
        let server = stub(listener, 1, move |_, _| (status, if status == 200 { "alpha\nbeta\n" } else { "node is offline" }.into()));
        let output = cli().args(["--namespace", "alice", "--url", &url, "--token-file"]).arg(&token)
            .args(["app", "logs", "ethproofs", "-n", "7", "--node", "gpu-box"]).output().unwrap();
        let seen = server.join().unwrap();
        std::fs::remove_file(token).unwrap();
        let line = &seen[0].line;
        assert!(line.starts_with("GET /apps/ethproofs/logs?") && line.contains("namespace=alice")
            && line.contains("lines=7") && line.contains("node=gpu-box"), "{line}");
        if status == 200 {
            assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
            assert_eq!(String::from_utf8_lossy(&output.stdout), "alpha\nbeta\n");
        } else {
            assert!(!output.status.success());
            let error = String::from_utf8_lossy(&output.stderr);
            assert!(error.contains("503") && error.contains("node is offline"), "{error}");
        }
    }
}

#[test]
fn app_delete_says_what_goes_until_confirmed() {
    let app = r#"{"name":"ethproofs","jobs":{"total":4,"active":0},"proof":{"elf_id":"aaaa"}}"#;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let token = token_file("app-delete");
    let server = stub(listener, 3, move |line, _| (200, if line.starts_with("GET") { app.into() } else { r#"{"app":"ethproofs","jobs_deleted":4,"nodes":["gpu-a"]}"#.into() }));
    let refused = cli().args(["--namespace", "alice", "--url", &url, "--token-file"]).arg(&token)
        .args(["app", "delete", "ethproofs"]).output().unwrap();
    assert!(!refused.status.success());
    let preview = String::from_utf8_lossy(&refused.stderr);
    assert!(preview.contains("would delete app ethproofs: 4 job record(s) with their inputs and proofs, its configuration and \
        replicas on its nodes, their logs and its workers' directories, its anchor, and its ELF aaaa unless another app uses it. \
        Its volumes, env files, storage roots and image stay"), "{preview}");
    let deleted = cli().args(["--namespace", "alice", "--url", &url, "--token-file"]).arg(&token)
        .args(["app", "delete", "ethproofs", "--yes"]).output().unwrap();
    let seen = server.join().unwrap();
    std::fs::remove_file(token).unwrap();
    assert!(deleted.status.success(), "{}", String::from_utf8_lossy(&deleted.stderr));
    let said = String::from_utf8_lossy(&deleted.stdout);
    assert!(said.contains("deleted app ethproofs and its 4 job(s); its nodes remove what they hold of it: gpu-a"), "{said}");
    assert!(seen[2].line.starts_with("DELETE /apps/ethproofs?namespace=alice"), "{}", seen[2].line);
}

#[test]
fn batch_delete_says_what_goes_until_confirmed_like_app_delete() {
    let batch = r#"{"type":"batch","name":"training","persistent":false,"jobs":{"total":5,"active":1}}"#;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let token = token_file("batch-delete");
    let server = stub(listener, 3, move |line, _| (200, if line.starts_with("GET") { batch.into() } else {
        r#"{"name":"training","removed":true,"jobs_deleted":5,"nodes":["gpu-a","gpu-b"]}"#.into() }));
    let refused = cli().args(["--namespace", "alice", "--url", &url, "--token-file"]).arg(&token)
        .args(["batch", "delete", "training"]).output().unwrap();
    assert!(!refused.status.success());
    let preview = String::from_utf8_lossy(&refused.stderr);
    assert!(preview.contains("would delete batch training with its 5 job(s): their records, logs and outputs, on the control host \
        and its nodes. Its mounts stay."), "{preview}");
    assert!(preview.contains("(1 now). Pass --yes to delete"), "{preview}");
    let deleted = cli().args(["--namespace", "alice", "--url", &url, "--token-file"]).arg(&token)
        .args(["batch", "delete", "training", "--yes"]).output().unwrap();
    let seen = server.join().unwrap();
    std::fs::remove_file(token).unwrap();
    assert!(deleted.status.success(), "{}", String::from_utf8_lossy(&deleted.stderr));
    let said = String::from_utf8_lossy(&deleted.stdout);
    assert!(said.contains("deleted batch training and its 5 job(s); its nodes remove what they hold of it: gpu-a, gpu-b"), "{said}");
    assert!(seen[2].line.starts_with("DELETE /batches/training?namespace=alice"), "{}", seen[2].line);
}

/// Two batches as `GET /batches` returns them: the app record's shape, with the policy under `batch`.
fn batch_list() -> String {
    serde_json::json!([
        {"type":"batch",  "name":"training", "namespace":"alice", "tag":"scope=default", "tags":["scope=default"],
         "state":"enabled", "priority":3, "disabled":false,
         "jobs":{"total":2,"active":1,"succeeded":1,"failed":0,"last_at":1_800_000_100.0},
         "batch":{"image":"python:3","max_gpus":2,"allow":["/tmp"],"workers":[]}},
        {"type":"batch",  "name":"idle", "namespace":"alice", "tag":null, "tags":[""],
         "state":"disabled", "priority":0, "disabled":true,
         "jobs":{"total":0,"active":0,"succeeded":0,"failed":0,"last_at":null},
         "batch":{"image":"python:3","max_gpus":1,"workers":[]}},
    ]).to_string()
}

#[test]
fn plain_batch_lists_every_batch_like_app() {
    let token = token_file("batches");
    // One request: a batch record carries its own job counts and last job.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = stub(listener, 1, |_, _| (200, batch_list()));
    let output = cli().args(["--json", "batch", "--namespace", "alice", "--url", &url, "--token-file"])
        .arg(&token).output().unwrap();
    let seen = server.join().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert!(seen[0].line.starts_with("GET /batches?namespace=alice"), "{}", seen[0].line);
    let rows: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(rows[0]["name"], "training");
    assert_eq!((rows[0]["jobs"]["active"].as_u64(), rows[0]["batch"]["image"].as_str()), (Some(1), Some("python:3")));
    assert_eq!((rows[1]["state"].as_str(), rows[1]["jobs"]["last_at"].is_null()), (Some("disabled"), true));

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = stub(listener, 1, |_, _| (200, batch_list()));
    let output = cli().args(["batch", "--namespace", "alice", "--url", &url, "--token-file"])
        .arg(&token).output().unwrap();
    server.join().unwrap();
    std::fs::remove_file(token).unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let table = String::from_utf8_lossy(&output.stdout);
    let head = table.lines().next().unwrap();
    assert!(head.starts_with("NAME") && head.contains("MAX GPUS") && head.contains("ACTIVE") && head.ends_with("IMAGE"),
        "the columns fleetctl app has, plus the batch's policy: {table}");
    let row = table.lines().find(|l| l.starts_with("training")).unwrap();
    assert!(row.contains("enabled") && row.contains(" 2 ") && row.contains(" UTC ") && row.ends_with("python:3"), "{row}");
    let row = table.lines().find(|l| l.starts_with("idle")).unwrap();
    assert!(row.contains("disabled") && row.ends_with("python:3"), "{row}");
}
