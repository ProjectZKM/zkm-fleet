#![cfg(unix)]

use std::{
    os::unix::fs::PermissionsExt,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use serde_json::{json, Value};

struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn message(response: &str) -> Value {
    if let Ok(value) = serde_json::from_str(response) {
        return value;
    }
    response
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .find_map(|line| serde_json::from_str(line).ok())
        .unwrap_or_else(|| panic!("response contains no JSON-RPC message: {response}"))
}

#[tokio::test]
async fn allows_anonymous_discovery_and_authenticates_tool_calls() {
    let temp = tempfile::tempdir().unwrap();
    let fake_fleetctl = temp.path().join("fleetctl");
    let args_log = temp.path().join("args.txt");
    std::fs::write(
        &fake_fleetctl,
        format!(
            "#!/bin/sh\n test -z \"${{FLEET_ADMIN_TOKEN_FILE}}\" || exit 41\n test \"$(cat \"$FLEET_TOKEN_FILE\")\" = scoped-token || exit 42\n printf '%s\\n' \"$*\" >> '{}'\n printf '{{\"apps\":[{{\"name\":\"demo\"}}]}}\\n'\n",
            args_log.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&fake_fleetctl, std::fs::Permissions::from_mode(0o700)).unwrap();

    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reservation.local_addr().unwrap();
    drop(reservation);
    let origin = format!("http://{address}");
    let child = Command::new(env!("CARGO_BIN_EXE_fleet-mcp"))
        .env("FLEETCTL_BIN", &fake_fleetctl)
        .env("FLEET_MCP_BIND", address.to_string())
        .env("FLEET_MCP_ALLOWED_HOSTS", address.to_string())
        .env("FLEET_ADMIN_TOKEN_FILE", "/must/not/be/inherited")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut server = Server(child);

    let client = reqwest::Client::new();
    let startup_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if client.get(format!("{origin}/unknown")).send().await.is_ok() {
            break;
        }
        assert!(
            Instant::now() < startup_deadline,
            "MCP server did not start"
        );
        thread::sleep(Duration::from_millis(25));
    }

    let response = client
        .post(format!("{origin}/mcp"))
        .header("accept", "application/json, text/event-stream")
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}))
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "{}",
        response.text().await.unwrap()
    );
    let session = response
        .headers()
        .get("mcp-session-id")
        .expect("initialize returns a session id")
        .to_str()
        .unwrap()
        .to_string();
    let init = message(&response.text().await.unwrap());
    assert_eq!(init["result"]["protocolVersion"], "2025-11-25");

    let tools_response = client
        .post(format!("{origin}/mcp"))
        .header("accept", "application/json, text/event-stream")
        .header("mcp-session-id", &session)
        .header("mcp-protocol-version", "2025-11-25")
        .json(&json!({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}))
        .send()
        .await
        .unwrap();
    assert!(tools_response.status().is_success());
    let tools = message(&tools_response.text().await.unwrap())["result"]["tools"].clone();
    assert!(tools
        .as_array()
        .unwrap()
        .iter()
        .any(|tool| tool["name"] == "fleet_batch_submit"));
    for name in ["fleet_apps_list", "fleet_batch_profiles_list", "fleet_batch_profile_get",
        "fleet_batches_list", "fleet_batch_get", "fleet_batch_logs", "fleet_batch_cancel"] {
        assert!(tools.as_array().unwrap().iter().any(|tool| tool["name"] == name), "missing {name}");
    }
    let submit_schema = &tools.as_array().unwrap().iter()
        .find(|tool| tool["name"] == "fleet_batch_submit").unwrap()["inputSchema"];
    assert!(submit_schema["properties"].get("profile").is_some());
    assert!(submit_schema["properties"].get("app").is_none());
    assert!(!tools.as_array().unwrap().iter().any(|tool|
        tool["name"].as_str().unwrap().starts_with("fleet_run_")));
    let jobs_schema = &tools.as_array().unwrap().iter()
        .find(|tool| tool["name"] == "fleet_jobs_list").unwrap()["inputSchema"];
    assert!(jobs_schema.to_string().contains("batch"));
    assert!(!jobs_schema.to_string().contains("\"run\""));

    let anonymous_call = client
        .post(format!("{origin}/mcp"))
        .header("accept", "application/json, text/event-stream")
        .header("mcp-session-id", &session)
        .header("mcp-protocol-version", "2025-11-25")
        .json(&json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"fleet_batch_profile_create","arguments":{"name":"demo","image":"image:tag"}}}))
        .send()
        .await
        .unwrap();
    assert!(anonymous_call.status().is_success());
    let anonymous_error = message(&anonymous_call.text().await.unwrap());
    assert!(anonymous_error["error"].is_object() || anonymous_error["result"]["isError"] == true);
    assert!(anonymous_error
        .to_string()
        .contains("Authorization: Bearer"));
    assert!(!args_log.exists(), "unauthenticated tool call ran fleetctl");

    let call_response = client
        .post(format!("{origin}/mcp"))
        .bearer_auth("scoped-token")
        .header("accept", "application/json, text/event-stream")
        .header("mcp-session-id", &session)
        .header("mcp-protocol-version", "2025-11-25")
        .json(&json!({"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"fleet_batch_profile_create","arguments":{"name":"demo","image":"image:tag","allow":["/srv"],"max_gpus":2,"priority":7}}}))
        .send()
        .await
        .unwrap();
    assert!(call_response.status().is_success());
    assert_eq!(
        message(&call_response.text().await.unwrap())["result"]["structuredContent"]["apps"][0]
            ["name"],
        "demo"
    );
    assert_eq!(
        std::fs::read_to_string(&args_log).unwrap().trim(),
        "--json batch add demo --image image:tag --allow /srv --max-gpus 2 --priority 7"
    );

    for (id, kind, accepted) in [(5, "batch", true), (6, "run", false)] {
        let response = client.post(format!("{origin}/mcp"))
            .bearer_auth("scoped-token")
            .header("accept", "application/json, text/event-stream")
            .header("mcp-session-id", &session)
            .header("mcp-protocol-version", "2025-11-25")
            .json(&json!({"jsonrpc":"2.0","id":id,"method":"tools/call",
                "params":{"name":"fleet_jobs_list","arguments":{"kind":kind}}}))
            .send().await.unwrap();
        assert!(response.status().is_success());
        let result = message(&response.text().await.unwrap());
        assert_eq!(result["error"].is_object() || result["result"]["isError"] == true, !accepted);
        assert_eq!(std::fs::read_to_string(&args_log).unwrap().lines().count(), 2);
    }
    assert!(std::fs::read_to_string(&args_log).unwrap()
        .contains("--json job --limit 20 --type batch"));

    for (id, name, arguments, expected) in [
        (10, "fleet_batch_profiles_list", json!({}), "--json batch list"),
        (11, "fleet_batch_profile_get", json!({"name":"training"}), "--json batch show training"),
        (7, "fleet_batches_list", json!({"profile":"training","active":true,"limit":3}),
            "--json batch --limit 3 --app training --active"),
        (8, "fleet_batch_submit", json!({"profile":"training","gpus":1,"cpus":4,"command":["python","train.py"]}),
            "--json batch submit --app training --gpus 1 --cpus 4 -- python train.py"),
        (12, "fleet_batch_submit", json!({"profile":"training","mounts":["/approved:ro"],"command":["python","train.py"]}),
            "--json batch submit --app training --mount /approved:ro -- python train.py"),
        (9, "fleet_batch_logs", json!({"id":"training-job","lines":25}),
            "--json batch logs training-job --lines 25"),
    ] {
        let response = client.post(format!("{origin}/mcp"))
            .bearer_auth("scoped-token")
            .header("accept", "application/json, text/event-stream")
            .header("mcp-session-id", &session)
            .header("mcp-protocol-version", "2025-11-25")
            .json(&json!({"jsonrpc":"2.0","id":id,"method":"tools/call",
                "params":{"name":name,"arguments":arguments}}))
            .send().await.unwrap();
        let result = message(&response.text().await.unwrap());
        assert!(!result["error"].is_object() && result["result"]["isError"] != true, "{result}");
        assert_eq!(std::fs::read_to_string(&args_log).unwrap().lines().last().unwrap(), expected);
    }

    server.0.kill().unwrap();
}
