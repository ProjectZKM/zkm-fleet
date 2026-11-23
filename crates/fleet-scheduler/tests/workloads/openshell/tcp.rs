//! Native TCP data path: real supervisor, router and workload; fixture grants.
use super::*;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn receive(socket: &mut Socket, expected: &[u8]) {
    let mut bytes = Vec::new();
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        while bytes.len() < expected.len() {
            match socket.next().await.unwrap().unwrap() {
                Message::Binary(data) => bytes.extend(data),
                Message::Ping(_) | Message::Pong(_) => {}
                message => panic!("unexpected raw tunnel message: {message:?}"),
            }
        }
        assert_eq!(bytes, expected);
    })
    .await;
    assert!(
        result.is_ok(),
        "TCP boundary: received {} of {} bytes",
        bytes.len(),
        expected.len()
    );
}

async fn drained(socket: &mut Socket) {
    tokio::time::timeout(Duration::from_secs(8), async {
        while let Some(Ok(message)) = socket.next().await {
            if matches!(message, Message::Close(_)) {
                break;
            }
            assert!(!matches!(message, Message::Binary(_) | Message::Text(_)));
        }
    })
    .await
    .expect("revocation must close active TCP streams");
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn exercise(
    root: &Path,
    c: Arc<WorkloadController>,
    mut report: NodeReport,
    execution: &ExecutionIdentity,
    shim_socket: &Path,
    native: &DriverSandbox,
    host_key: &str,
    observer: crate::openshell::Observer,
) {
    let image = std::env::var("OPENSHELL_TEST_SUPERVISOR_IMAGE").unwrap();
    let stop = tokio_util::sync::CancellationToken::new();
    // A separate HTTP fixture toggles health; the admitted TCP service receives
    // no HTTP probes. All data still crosses the stock supervisor boundary.
    let health =
        trusted_shim::openshell_network::bind(native.clone(), &image, host_key, 8090, stop.clone())
            .await
            .unwrap();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    let unready = format!("http://{}/health/unready", health.address());
    let ready = format!("http://{}/health/ready", health.address());
    client.get(&unready).send().await.unwrap();
    let route_root = root.join("tcp-router");
    std::fs::create_dir(&route_root).unwrap();
    let route = route::Route::managed_tcp(&route_root, execution, c.clone()).await;
    route.control_ready.store(true, Ordering::SeqCst);
    let heartbeat = {
        let c = c.clone();
        let socket = shim_socket.to_owned();
        let execution = execution.clone();
        tokio::spawn(async move {
            loop {
                if let Ok(status) = try_shim_call(
                    &socket,
                    json!({"operation":"inspect","execution":execution,"driver":"openshell"}),
                )
                .await
                {
                    let ready = status["application_ready"] == true;
                    report.apps[0]["ready"] = json!(ready);
                    report.runtime_instances[0].ready = ready;
                    report.runtime_instances[0].services = if ready {
                        vec!["native-http".into()]
                    } else {
                        vec![]
                    };
                    report.runtime_instances[0].native_observation_base64 = status
                        ["native_observation_base64"]
                        .as_str()
                        .map(str::to_owned);
                    c.report("gpu", "session", report.clone()).await.unwrap();
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
    };
    let observation = tokio::spawn(observer.run(c));
    shim_call(shim_socket, json!({"operation":"network","driver":"openshell","execution":execution,"config":route.config})).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(!route.ready().await);
    assert_eq!(route.registrations.load(Ordering::SeqCst), 0);
    client.get(&ready).send().await.unwrap();
    route.wait_ready().await;
    let mut socket = route.open_tcp().await;
    receive(&mut socket, b"SSH-2.0-native-fixture\r\n").await;
    let payload: Vec<u8> = (0..=255).cycle().take(512 * 1024).collect();
    for chunk in payload.chunks(16 * 1024) {
        socket
            .send(Message::Binary(chunk.to_vec().into()))
            .await
            .unwrap();
        receive(&mut socket, chunk).await;
    }
    client.get(&unready).send().await.unwrap();
    drained(&mut socket).await;
    assert!(!route.ready().await);
    client.get(&ready).send().await.unwrap();
    route.wait_ready().await;
    let mut socket = route.open_tcp().await;
    receive(&mut socket, b"SSH-2.0-native-fixture\r\n").await;
    route.revoke().await;
    drained(&mut socket).await;
    assert!(!route.ready().await);
    shim_call(
        shim_socket,
        json!({"operation":"delete","execution":execution,"driver":"openshell"}),
    )
    .await;
    heartbeat.abort();
    observation.abort();
    stop.cancel();
    health.close().await;
}
