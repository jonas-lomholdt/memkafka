use std::{
    collections::BTreeSet,
    net::SocketAddr,
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use serde::Deserialize;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    process::{Child, Command},
    task::JoinHandle,
    time::{sleep, timeout},
};

static NEXT_OUTPUT: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "camelCase")]
struct Observation {
    scenario: String,
    api_key: i16,
    api_version: i16,
    client_id: Option<String>,
}

fn request(api_key: i16, api_version: i16, client_id: Option<&str>, body: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&api_key.to_be_bytes());
    payload.extend_from_slice(&api_version.to_be_bytes());
    payload.extend_from_slice(&42_i32.to_be_bytes());
    match client_id {
        Some(client_id) => {
            payload.extend_from_slice(&(client_id.len() as i16).to_be_bytes());
            payload.extend_from_slice(client_id.as_bytes());
        }
        None => payload.extend_from_slice(&(-1_i16).to_be_bytes()),
    }
    payload.extend_from_slice(body);

    let mut frame = Vec::with_capacity(payload.len() + 4);
    frame.extend_from_slice(&(payload.len() as i32).to_be_bytes());
    frame.extend_from_slice(&payload);
    frame
}

fn output_path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "memkafka-api-version-proxy-test-{}-{}.jsonl",
        std::process::id(),
        NEXT_OUTPUT.fetch_add(1, Ordering::Relaxed)
    ))
}

async fn read_frame(stream: &mut TcpStream) -> Vec<u8> {
    let mut prefix = [0_u8; 4];
    stream.read_exact(&mut prefix).await.unwrap();
    let size = i32::from_be_bytes(prefix);
    assert!(size >= 0, "test fixture only sends non-negative frames");
    let mut payload = vec![0; size as usize];
    stream.read_exact(&mut payload).await.unwrap();
    [prefix.as_slice(), payload.as_slice()].concat()
}

async fn start_proxy(
    upstream: SocketAddr,
    output: &PathBuf,
    listen: Option<SocketAddr>,
) -> (Child, SocketAddr) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_kafka-api-version-proxy"));
    command
        .arg("--listen")
        .arg(
            listen
                .unwrap_or_else(|| "127.0.0.1:0".parse().unwrap())
                .to_string(),
        )
        .arg("--upstream")
        .arg(upstream.to_string())
        .arg("--scenario")
        .arg("test-scenario")
        .arg("--output")
        .arg(output)
        .stdout(std::process::Stdio::piped());
    let mut child = command.spawn().unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut lines = tokio::io::BufReader::new(stdout).lines();
    let ready = timeout(Duration::from_secs(3), lines.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let address = ready
        .strip_prefix("READY listen=")
        .expect("recorder must print its exact readiness line")
        .parse()
        .unwrap();
    (child, address)
}

async fn wait_for_observations(path: &PathBuf, expected: usize) -> Vec<Observation> {
    for _ in 0..50 {
        if let Ok(content) = tokio::fs::read_to_string(path).await {
            let observations: Vec<Observation> = content
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            if observations.len() == expected {
                return observations;
            }
        }
        sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {expected} observation(s)");
}

async fn stop_proxy(mut child: Child) {
    child.kill().await.unwrap();
    child.wait().await.unwrap();
}

async fn upstream_once(listener: TcpListener, response: Vec<u8>) -> JoinHandle<Vec<u8>> {
    tokio::spawn(async move {
        let (mut upstream, _) = listener.accept().await.unwrap();
        let received = read_frame(&mut upstream).await;
        upstream.write_all(&response).await.unwrap();
        upstream.shutdown().await.unwrap();
        received
    })
}

#[tokio::test]
async fn fragmented_request_is_forwarded_exactly_and_recorded() {
    // Break caught: a frame reader that assumes one TCP read contains a complete Kafka request.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let output = output_path();
    let (child, proxy) = start_proxy(listener.local_addr().unwrap(), &output, None).await;
    let expected = request(18, 3, Some("rdkafka"), &[9, 8, 7]);
    let upstream = upstream_once(listener, vec![0, 0, 0, 0]).await;

    let mut client = TcpStream::connect(proxy).await.unwrap();
    client.write_all(&expected[..1]).await.unwrap();
    client.write_all(&expected[1..6]).await.unwrap();
    client.write_all(&expected[6..]).await.unwrap();
    let mut response = [0; 4];
    client.read_exact(&mut response).await.unwrap();

    assert_eq!(upstream.await.unwrap(), expected);
    assert_eq!(
        wait_for_observations(&output, 1).await,
        vec![Observation {
            scenario: "test-scenario".into(),
            api_key: 18,
            api_version: 3,
            client_id: Some("rdkafka".into()),
        }]
    );
    stop_proxy(child).await;
    let _ = tokio::fs::remove_file(output).await;
}

#[tokio::test]
async fn forwards_the_upstream_response_without_rewriting() {
    // Break caught: a response path that drops, buffers incorrectly, or mutates broker bytes.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let output = output_path();
    let (child, proxy) = start_proxy(listener.local_addr().unwrap(), &output, None).await;
    let expected_request = request(3, 9, Some("client"), &[1]);
    let expected_response = vec![0, 0, 0, 4, 0xde, 0xad, 0xbe, 0xef];
    let upstream = upstream_once(listener, expected_response.clone()).await;

    let mut client = TcpStream::connect(proxy).await.unwrap();
    client.write_all(&expected_request).await.unwrap();
    let mut response = vec![0; expected_response.len()];
    client.read_exact(&mut response).await.unwrap();

    assert_eq!(response, expected_response);
    assert_eq!(upstream.await.unwrap(), expected_request);
    stop_proxy(child).await;
    let _ = tokio::fs::remove_file(output).await;
}

#[tokio::test]
async fn records_null_client_id_as_json_null() {
    // Break caught: treating Kafka's nullable client ID as an empty string or rejecting it.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let output = output_path();
    let (child, proxy) = start_proxy(listener.local_addr().unwrap(), &output, None).await;
    let expected = request(18, 3, None, &[]);
    let upstream = upstream_once(listener, vec![0, 0, 0, 0]).await;

    let mut client = TcpStream::connect(proxy).await.unwrap();
    client.write_all(&expected).await.unwrap();
    let mut response = [0; 4];
    client.read_exact(&mut response).await.unwrap();

    assert_eq!(upstream.await.unwrap(), expected);
    assert_eq!(
        wait_for_observations(&output, 1).await,
        vec![Observation {
            scenario: "test-scenario".into(),
            api_key: 18,
            api_version: 3,
            client_id: None,
        }]
    );
    stop_proxy(child).await;
    let _ = tokio::fs::remove_file(output).await;
}

#[tokio::test]
async fn concurrent_connections_write_complete_observation_lines() {
    // Break caught: concurrent record writes interleave, producing invalid JSON Lines.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let output = output_path();
    let (child, proxy) = start_proxy(listener.local_addr().unwrap(), &output, None).await;
    let upstream = tokio::spawn(async move {
        let mut received = BTreeSet::new();
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().await.unwrap();
            received.insert(read_frame(&mut stream).await);
            stream.write_all(&[0, 0, 0, 0]).await.unwrap();
            stream.shutdown().await.unwrap();
        }
        received
    });
    let first = request(1, 2, Some("first"), &[]);
    let second = request(2, 4, Some("second"), &[]);
    let first_client = async {
        let mut client = TcpStream::connect(proxy).await.unwrap();
        client.write_all(&first).await.unwrap();
        let mut response = [0; 4];
        client.read_exact(&mut response).await.unwrap();
    };
    let second_client = async {
        let mut client = TcpStream::connect(proxy).await.unwrap();
        client.write_all(&second).await.unwrap();
        let mut response = [0; 4];
        client.read_exact(&mut response).await.unwrap();
    };
    tokio::join!(first_client, second_client);

    assert_eq!(upstream.await.unwrap(), BTreeSet::from([first, second]));
    assert_eq!(
        wait_for_observations(&output, 2)
            .await
            .into_iter()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            Observation {
                scenario: "test-scenario".into(),
                api_key: 1,
                api_version: 2,
                client_id: Some("first".into()),
            },
            Observation {
                scenario: "test-scenario".into(),
                api_key: 2,
                api_version: 4,
                client_id: Some("second".into()),
            },
        ])
    );
    stop_proxy(child).await;
    let _ = tokio::fs::remove_file(output).await;
}

#[tokio::test]
async fn short_header_is_forwarded_without_an_observation() {
    // Break caught: malformed request headers are logged as invented API observations or dropped.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let output = output_path();
    let (child, proxy) = start_proxy(listener.local_addr().unwrap(), &output, None).await;
    let expected = vec![0, 0, 0, 3, 18, 0, 3];
    let upstream = upstream_once(listener, vec![0, 0, 0, 0]).await;

    let mut client = TcpStream::connect(proxy).await.unwrap();
    client.write_all(&expected).await.unwrap();
    let mut response = [0; 4];
    client.read_exact(&mut response).await.unwrap();

    assert_eq!(upstream.await.unwrap(), expected);
    sleep(Duration::from_millis(30)).await;
    let content = std::fs::read_to_string(&output).unwrap_or_default();
    assert!(
        content.is_empty(),
        "invalid header must not create a JSON line"
    );
    stop_proxy(child).await;
}

#[tokio::test]
async fn recorder_can_rebind_immediately_after_closing() {
    // Break caught: listener setup leaves an address unusable after recorder shutdown.
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let output = output_path();
    let requested: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (first, address) =
        start_proxy(upstream.local_addr().unwrap(), &output, Some(requested)).await;
    stop_proxy(first).await;

    let (second, rebound) =
        start_proxy(upstream.local_addr().unwrap(), &output, Some(address)).await;
    assert_eq!(rebound, address);
    stop_proxy(second).await;
    let _ = tokio::fs::remove_file(output).await;
}
