//! Outbound machine heartbeats. A slim Python child performs local Ray operations.
mod logs;
use anyhow::{Context, Result};
use clap::Parser;
use fleet_proto::fleet::v1::{
    worker_service_client::WorkerServiceClient, LogRequest, ProofFetch, WorkerHeartbeatRequest,
};
use serde_json::{json, Value};
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader},
    sync::Mutex,
};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};

#[derive(Parser)]
struct NodeAgentArgs {
    #[arg(long, env = "FLEET_SCHEDULER")]
    scheduler: String,
    #[arg(long, env = "FLEET_CLUSTER_TOKEN_FILE")]
    cluster_token_file: PathBuf,
    #[arg(long)]
    scheduler_ca_file: Option<PathBuf>,
    #[arg(long)]
    name: String,
    #[arg(long)]
    services: PathBuf,
    #[arg(long)]
    batches: PathBuf,
    #[arg(long)]
    jobs_dir: PathBuf,
    #[arg(long)]
    bridge: PathBuf,
    #[arg(long)]
    python: PathBuf,
    #[arg(long, default_value = "127.0.0.1:6379")]
    ray_address: String,
    /// Storage-only handler; it does not start a GPU server.
    #[arg(long)]
    proof_node_binary: Option<PathBuf>,
    /// Approved app storage, independent of enabled proof actors.
    #[arg(long, requires = "proof_node_binary")]
    proof_storage_root: Vec<PathBuf>,
}
struct BridgeSnapshot {
    report: Value,
    seen: Instant,
}
type Input = Arc<Mutex<tokio::process::ChildStdin>>;
async fn send(input: &Input, value: Value) -> Result<()> {
    let mut bytes = serde_json::to_vec(&value)?;
    bytes.push(b'\n');
    input.lock().await.write_all(&bytes).await?;
    Ok(())
}
async fn stopped() {
    let mut term =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
    tokio::select! {_=term.recv()=>{},_=tokio::signal::ctrl_c()=>{}}
}
async fn connect(endpoint: Endpoint) -> Result<WorkerServiceClient<Channel>> {
    Ok(WorkerServiceClient::new(endpoint.connect().await?)
        .max_decoding_message_size(16 << 20)
        .max_encoding_message_size(16 << 20))
}
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let a = NodeAgentArgs::parse();
    anyhow::ensure!(
        fleet_proto::artifacts::is_safe_id(&a.name),
        "invalid worker name"
    );
    let url = reqwest::Url::parse(&a.scheduler)?;
    anyhow::ensure!(
        url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
            && url.path() == "/",
        "scheduler must be an endpoint URL"
    );
    anyhow::ensure!(
        url.scheme() == "https"
            || (url.scheme() == "http"
                && matches!(url.host_str(), Some("127.0.0.1" | "localhost"))),
        "node connections require HTTPS outside loopback"
    );
    anyhow::ensure!(
        a.ray_address.starts_with("127.0.0.1:") || a.ray_address.starts_with("localhost:"),
        "Ray must run locally"
    );
    let secret = tokio::fs::read_to_string(&a.cluster_token_file).await?;
    let secret = secret.trim().to_owned();
    anyhow::ensure!(secret.len() >= 32, "cluster token too short");
    let auth: tonic::metadata::MetadataValue<_> = format!("Bearer {secret}").parse()?;
    let mut endpoint =
        Endpoint::from_shared(a.scheduler.clone())?.connect_timeout(Duration::from_secs(5));
    let mut http = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(5));
    if url.scheme() == "https" {
        let mut tls = ClientTlsConfig::new().with_webpki_roots();
        if let Some(ca) = &a.scheduler_ca_file {
            let bytes = tokio::fs::read(ca).await?;
            tls = tls.ca_certificate(Certificate::from_pem(&bytes));
            http = http.add_root_certificate(reqwest::Certificate::from_pem(&bytes)?);
        }
        endpoint = endpoint.tls_config(tls)?;
    }
    let mut command = tokio::process::Command::new(&a.python);
    command
        .current_dir(&a.bridge)
        .arg("-m")
        .arg("fleet_bridge.node")
        .arg("--services")
        .arg(&a.services)
        .arg("--batches")
        .arg(&a.batches)
        .arg("--jobs-dir")
        .arg(&a.jobs_dir)
        .arg("--address")
        .arg(&a.ray_address);
    let mut child = command
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .context("starting local Ray bridge")?;
    let input = Arc::new(Mutex::new(child.stdin.take().unwrap()));
    let snapshot = Arc::new(Mutex::new(None::<BridgeSnapshot>));
    let reader_snapshot = snapshot.clone();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let reader = tokio::spawn(async move {
        while let Ok(Some(line)) = lines.next_line().await {
            if let Ok(v) = serde_json::from_str::<Value>(&line) {
                if v["report"].is_object() {
                    *reader_snapshot.lock().await = Some(BridgeSnapshot {
                        report: v["report"].clone(),
                        seen: Instant::now(),
                    });
                }
            }
        }
    });
    let http = http.build()?;
    let log_task = tokio::spawn(sync_logs(
        http.clone(),
        a.scheduler.clone(),
        secret.clone(),
        a.name.clone(),
        a.jobs_dir.clone(),
        snapshot.clone(),
        input.clone(),
    ));
    let session = uuid::Uuid::new_v4().to_string();
    let heartbeat = async {
        let mut client = None;
        let mut fetches = tokio::task::JoinSet::new();
        let mut fetching = std::collections::HashSet::new();
        let mut log_reads = tokio::task::JoinSet::new();
        loop {
            while let Some(result) = fetches.try_join_next() {
                if let Ok(ticket) = result {
                    fetching.remove(&ticket);
                }
            }
            let report = {
                let s = snapshot.lock().await;
                s.as_ref().map(|s| {
                    let mut report = s.report.clone();
                    if s.seen.elapsed() > Duration::from_secs(20) {
                        report["ready"] = json!(false);
                    }
                    report
                })
            };
            let Some(report) = report else {
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            };
            if client.is_none() {
                match connect(endpoint.clone()).await {
                    Ok(c) => {
                        client = Some(c);
                        tracing::info!(worker=%a.name,"node connected to control plane");
                    }
                    Err(e) => {
                        tracing::warn!(error=%e,"node heartbeat connection unavailable");
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        continue;
                    }
                }
            }
            let mut request = tonic::Request::new(WorkerHeartbeatRequest {
                worker_id: a.name.clone(),
                session: session.clone(),
                report: Some(serde_json::from_value(report)?),
                proof_fetch_ready: a.proof_node_binary.is_some()
                    && !a.proof_storage_root.is_empty(),
            });
            request.metadata_mut().insert("authorization", auth.clone());
            request.set_timeout(Duration::from_secs(10));
            match client.as_mut().unwrap().heartbeat(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    for fetch in response.proof_fetches {
                        if fetching.len() >= 4 || !fetching.insert(fetch.ticket.clone()) {
                            continue;
                        }
                        let http = http.clone();
                        let scheduler = a.scheduler.clone();
                        let secret = secret.clone();
                        let worker = a.name.clone();
                        let session = session.clone();
                        let binary = a.proof_node_binary.clone();
                        let roots = a.proof_storage_root.clone();
                        fetches.spawn(async move {
                            let ticket = fetch.ticket.clone();
                            if let Some(binary) = binary {
                                if let Err(error) = fetch_proof(http, &scheduler, &secret, &worker, &session, binary, roots, fetch).await {
                                    tracing::debug!(error=%error,"proof fetch relay failed; client may retry");
                                }
                            }
                            ticket
                        });
                    }
                    while log_reads.try_join_next().is_some() {}
                    for request in response.log_requests {
                        if log_reads.len() >= 4 {
                            continue;
                        }
                        let path = {
                            let s = snapshot.lock().await;
                            s.as_ref()
                                .and_then(|s| log_path(&s.report, &request.service))
                        };
                        log_reads.spawn(serve_log(
                            http.clone(),
                            a.scheduler.clone(),
                            secret.clone(),
                            a.name.clone(),
                            session.clone(),
                            path,
                            request,
                        ));
                    }
                    for execution in response.acknowledged_jobs {
                        if let Some((id, lease)) = execution.split_once('/') {
                            send(&input, json!({"reported":id,"lease":lease})).await?;
                        }
                    }
                    for id in response.acknowledged_results {
                        send(&input, json!({"ack":id})).await?;
                    }
                    for command in response.commands {
                        let parameters =
                            command.parameters.context("missing command parameters")?;
                        send(&input,json!({"command":{"id":command.id,"operation":command.operation,"parameters":parameters}})).await?;
                    }
                    tokio::time::sleep(Duration::from_secs(
                        response.heartbeat_secs.clamp(1, 10) as u64
                    ))
                    .await;
                }
                Err(e) => {
                    tracing::warn!(error=%e,"node heartbeat rejected; local execution continues");
                    client = None;
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
        #[allow(unreachable_code)]
        Ok::<_, anyhow::Error>(())
    };
    tokio::select! {result=heartbeat=>{result?;},_=stopped()=>{},status=child.wait()=>{anyhow::bail!("local Ray bridge exited: {:?}",status?);}}
    log_task.abort();
    reader.abort();
    let _ = child.kill().await;
    Ok(())
}

/// The log file this machine reported for a service. Only a path the bridge itself
/// reported, and only a node log, is ever read.
fn log_path(report: &Value, service: &str) -> Option<PathBuf> {
    let path = report["services"]
        .as_array()?
        .iter()
        .find(|s| s["name"].as_str() == Some(service))?["logs"]
        .get(0)?
        .as_str()?;
    let path = PathBuf::from(path);
    (path.is_absolute()
        && path.to_string_lossy().ends_with(".node.log")
        && !path
            .components()
            .any(|c| c == std::path::Component::ParentDir))
    .then_some(path)
}

async fn serve_log(
    http: reqwest::Client,
    scheduler: String,
    secret: String,
    worker: String,
    session: String,
    path: Option<PathBuf>,
    request: LogRequest,
) {
    if request.ticket.len() != 64 || !request.ticket.bytes().all(|b| b.is_ascii_hexdigit()) {
        return;
    }
    let read = match path {
        Some(path) => {
            let lines = request.lines;
            tokio::task::spawn_blocking(move || logs::tail(&path, lines))
                .await
                .map_err(|_| "unavailable")
                .and_then(|r| {
                    r.map_err(|e| {
                        if e.kind() == std::io::ErrorKind::NotFound {
                            "missing"
                        } else {
                            "unavailable"
                        }
                    })
                })
        }
        None => Err("unconfigured"),
    };
    let mut reply = http
        .post(format!(
            "{}/internal/app-logs/{}",
            scheduler.trim_end_matches('/'),
            request.ticket
        ))
        .bearer_auth(secret)
        .header("x-fleet-worker", worker)
        .header("x-fleet-session", session)
        .timeout(Duration::from_secs(10));
    reply = match read {
        Ok(bytes) => reply.body(bytes),
        Err(code) => reply.header("x-fleet-log-error", code),
    };
    if let Err(error) = reply.send().await.and_then(|r| r.error_for_status()) {
        tracing::debug!(%error, "log reply failed; the client may retry");
    }
}

async fn fetch_proof(
    http: reqwest::Client,
    scheduler: &str,
    secret: &str,
    worker: &str,
    session: &str,
    binary: PathBuf,
    roots: Vec<PathBuf>,
    fetch: ProofFetch,
) -> Result<()> {
    anyhow::ensure!(
        fetch.ticket.len() == 64 && fetch.ticket.bytes().all(|b| b.is_ascii_hexdigit()),
        "invalid fetch ticket"
    );
    let mut command = tokio::process::Command::new(binary);
    command.arg("fetch-artifact");
    for root in roots {
        command.arg("--storage-root").arg(root);
    }
    command
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
    let result = async {
        let mut child = command.spawn()?;
        let mut stdin = child.stdin.take().unwrap();
        stdin.write_all(&serde_json::to_vec(&fetch)?).await?;
        drop(stdin);
        let mut bytes = Vec::new();
        child
            .stdout
            .take()
            .unwrap()
            .take(fleet_proto::artifacts::MAX_OUTPUT + 1)
            .read_to_end(&mut bytes)
            .await?;
        anyhow::ensure!(
            bytes.len() as u64 <= fleet_proto::artifacts::MAX_OUTPUT,
            "artifact exceeds size limit"
        );
        let status = child.wait().await?;
        Ok::<_, anyhow::Error>((status, bytes))
    };
    let result = tokio::time::timeout(Duration::from_secs(20), result).await;
    let mut request = http
        .post(format!(
            "{}/internal/proof-fetch/{}",
            scheduler.trim_end_matches('/'),
            fetch.ticket
        ))
        .bearer_auth(secret)
        .header("x-fleet-worker", worker)
        .header("x-fleet-session", session)
        .timeout(Duration::from_secs(30));
    request = match result {
        Ok(Ok((status, bytes))) if status.success() => request.body(bytes),
        Ok(Ok((status, _))) => request.header(
            "x-fleet-fetch-error",
            if status.code() == Some(4) {
                "missing"
            } else {
                "invalid"
            },
        ),
        _ => request.header("x-fleet-fetch-error", "unavailable"),
    };
    request.send().await?.error_for_status()?;
    Ok(())
}
async fn sync_logs(
    client: reqwest::Client,
    url: String,
    secret: String,
    worker: String,
    jobs_dir: PathBuf,
    snapshot: Arc<Mutex<Option<BridgeSnapshot>>>,
    input: Input,
) {
    loop {
        let jobs = {
            snapshot
                .lock()
                .await
                .as_ref()
                .and_then(|s| s.report["jobs"].as_array().cloned())
                .unwrap_or_default()
        };
        for job in jobs {
            let outcome = async {
                let Some(id) = job["id"].as_str() else {
                    return Ok(());
                };
                let Some(lease) = job["lease"].as_str() else {
                    return Ok(());
                };
                if !fleet_proto::artifacts::is_safe_id(id)
                    || !fleet_proto::artifacts::is_safe_id(lease)
                {
                    return Ok(());
                }
                let base = format!("{}/internal/jobs/{id}", url.trim_end_matches('/'));
                let request = |method, path: &str| {
                    client
                        .request(method, format!("{base}/{path}"))
                        .bearer_auth(&secret)
                        .header("x-fleet-worker", &worker)
                        .header("x-fleet-lease", lease)
                };
                let response = request(reqwest::Method::GET, "logs-offset")
                    .timeout(Duration::from_secs(10))
                    .send()
                    .await?
                    .error_for_status()?;
                let offset = response.json::<Value>().await?["offset"]
                    .as_u64()
                    .unwrap_or(0);
                let log = job["spec"]["log"]
                    .as_str()
                    .map(PathBuf::from)
                    .unwrap_or_else(|| jobs_dir.join(id).join("log"));
                let mut logs_done = true;
                match tokio::fs::File::open(log).await {
                    Ok(mut file) => {
                        let len = file.metadata().await?.len();
                        anyhow::ensure!(offset <= len, "local log is shorter than stored log");
                        if offset < len {
                            file.seek(std::io::SeekFrom::Start(offset)).await?;
                            let mut bytes = Vec::new();
                            file.take(1 << 20).read_to_end(&mut bytes).await?;
                            logs_done = offset + bytes.len() as u64 == len;
                            request(reqwest::Method::PUT, &format!("logs?offset={offset}"))
                                .body(bytes)
                                .timeout(Duration::from_secs(30))
                                .send()
                                .await?
                                .error_for_status()?;
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
                if job["logs_ready"].as_bool() != Some(true)
                    && logs_done
                    && job["cleaned"].as_bool() == Some(true)
                    && matches!(
                        job["status"]["state"].as_str(),
                        Some("succeeded" | "failed" | "cancelled" | "lost")
                    )
                {
                    send(&input, json!({"logs_ready":id,"lease":lease})).await?;
                }
                Ok::<_, anyhow::Error>(())
            }
            .await;
            if let Err(e) = outcome {
                tracing::debug!(error=%e,"job logs will retry");
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
