use std::time::Duration;

use clap::Parser;
use memkafka::{
    config::{AdvertisedAddress, Cli, Config},
    server::{BoundEndpoints, readiness_message, serve},
};
use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    sync::oneshot,
    time::{sleep, timeout},
};

fn ephemeral_config() -> Config {
    Config::try_from(
        Cli::try_parse_from([
            "memkafka",
            "--kafka-listen",
            "127.0.0.1:0",
            "--schema-registry-listen",
            "127.0.0.1:0",
        ])
        .unwrap(),
    )
    .unwrap()
}

#[test]
fn readiness_message_names_both_resolved_endpoints() {
    let endpoints = BoundEndpoints::new(
        "127.0.0.1:19092".parse().unwrap(),
        "127.0.0.1:18081".parse().unwrap(),
        AdvertisedAddress::new("broker", 19092).unwrap(),
    );

    assert_eq!(
        readiness_message(&endpoints),
        "MemKafka ready kafka=127.0.0.1:19092 schema_registry=http://127.0.0.1:18081 advertised_kafka=broker:19092"
    );
}

#[tokio::test]
async fn both_endpoints_accept_connections_until_shutdown() {
    let (ready_tx, ready_rx) = oneshot::channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let mut server = tokio::spawn(serve(ephemeral_config(), ready_tx, async {
        let _ = shutdown_rx.await;
    }));

    let endpoints = match timeout(Duration::from_secs(1), ready_rx).await {
        Ok(Ok(endpoints)) => endpoints,
        ready_result => {
            let server_result = timeout(Duration::from_secs(1), &mut server).await;
            panic!("server did not become ready: ready={ready_result:?}, server={server_result:?}");
        }
    };

    TcpStream::connect(endpoints.kafka).await.unwrap();
    TcpStream::connect(endpoints.schema_registry).await.unwrap();
    assert_eq!(endpoints.advertised_kafka.port(), endpoints.kafka.port());

    shutdown_tx.send(()).unwrap();
    timeout(Duration::from_secs(1), server)
        .await
        .expect("server did not shut down")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn kafka_bind_failure_is_reported_before_readiness() {
    let reserved_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let reserved_address = reserved_listener.local_addr().unwrap();
    let config = Config::try_from(
        Cli::try_parse_from([
            "memkafka",
            "--kafka-listen",
            &reserved_address.to_string(),
            "--schema-registry-listen",
            "127.0.0.1:0",
        ])
        .unwrap(),
    )
    .unwrap();
    let (ready_tx, ready_rx) = oneshot::channel();

    let error = serve(config, ready_tx, std::future::pending())
        .await
        .unwrap_err();

    assert!(error.to_string().contains("failed to bind Kafka listener"));
    assert!(ready_rx.await.is_err());
}

#[tokio::test]
async fn shutdown_bounds_an_incomplete_schema_registry_request() {
    let (ready_tx, ready_rx) = oneshot::channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(serve(ephemeral_config(), ready_tx, async {
        let _ = shutdown_rx.await;
    }));
    let endpoints = timeout(Duration::from_secs(1), ready_rx)
        .await
        .unwrap()
        .unwrap();
    let mut client = TcpStream::connect(endpoints.schema_registry).await.unwrap();
    client
        .write_all(
            b"POST /subjects/slow/versions HTTP/1.1\r\n\
              Host: localhost\r\n\
              Content-Type: application/json\r\n\
              Content-Length: 1000\r\n\r\n\
              {\"schema\":\"",
        )
        .await
        .unwrap();
    sleep(Duration::from_millis(25)).await;

    shutdown_tx.send(()).unwrap();
    timeout(Duration::from_secs(2), server)
        .await
        .expect("server exceeded its HTTP shutdown grace period")
        .unwrap()
        .unwrap();
}
