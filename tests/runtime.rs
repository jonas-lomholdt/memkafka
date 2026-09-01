use std::time::Duration;

use bytes::{Bytes, BytesMut};
use clap::Parser;
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, RequestHeader, RequestKind, ResponseHeader, ResponseKind,
};
use kafka_protocol::protocol::{Decodable, StrBytes, encode_request_header_into_buffer};
use memkafka::{
    config::{AdvertisedAddress, Cli, Config},
    kafka::frame::{read_frame, write_frame},
    server::{BoundEndpoints, BoundKafkaListener, readiness_message, serve},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::oneshot,
    time::{sleep, timeout},
};

fn ephemeral_config() -> Config {
    Config::from(
        Cli::try_parse_from([
            "memkafka",
            "--kafka-listen",
            "127.0.0.1:0",
            "--schema-registry-listen",
            "127.0.0.1:0",
        ])
        .unwrap(),
    )
}

#[test]
fn readiness_message_names_both_resolved_endpoints() {
    let endpoints = BoundEndpoints::new(
        vec![BoundKafkaListener::new(
            "127.0.0.1:19092".parse().unwrap(),
            AdvertisedAddress::new("broker", 19092).unwrap(),
        )],
        "127.0.0.1:18081".parse().unwrap(),
    );

    assert_eq!(
        readiness_message(&endpoints),
        "MemKafka ready kafka=127.0.0.1:19092 schema_registry=http://127.0.0.1:18081 advertised_kafka=broker:19092"
    );
}

#[test]
fn readiness_message_names_every_kafka_listener() {
    let endpoints = BoundEndpoints::new(
        vec![
            BoundKafkaListener::new(
                "0.0.0.0:9092".parse().unwrap(),
                AdvertisedAddress::new("localhost", 9092).unwrap(),
            ),
            BoundKafkaListener::new(
                "0.0.0.0:9093".parse().unwrap(),
                AdvertisedAddress::new("kafka", 9093).unwrap(),
            ),
        ],
        "127.0.0.1:18081".parse().unwrap(),
    );

    assert_eq!(
        readiness_message(&endpoints),
        "MemKafka ready kafka=0.0.0.0:9092,0.0.0.0:9093 schema_registry=http://127.0.0.1:18081 advertised_kafka=localhost:9092,kafka:9093"
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

    TcpStream::connect(endpoints.kafka()).await.unwrap();
    TcpStream::connect(endpoints.schema_registry())
        .await
        .unwrap();
    assert_eq!(
        endpoints.advertised_kafka().port(),
        endpoints.kafka().port()
    );

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
    let config = Config::from(
        Cli::try_parse_from([
            "memkafka",
            "--kafka-listen",
            &reserved_address.to_string(),
            "--schema-registry-listen",
            "127.0.0.1:0",
        ])
        .unwrap(),
    );
    let (ready_tx, ready_rx) = oneshot::channel();

    let error = serve(config, ready_tx, std::future::pending())
        .await
        .unwrap_err();

    assert!(error.to_string().contains("failed to bind Kafka listener"));
    assert!(ready_rx.await.is_err());
}

#[tokio::test]
async fn empty_kafka_listener_configuration_is_rejected_before_readiness() {
    let mut config = ephemeral_config();
    config.kafka_listeners.clear();
    let (ready_tx, mut ready_rx) = oneshot::channel();
    let server = serve(config, ready_tx, std::future::pending());
    tokio::pin!(server);

    let result = timeout(Duration::from_secs(1), async {
        tokio::select! {
            biased;
            result = &mut server => result,
            ready = &mut ready_rx => panic!("server reported readiness without a Kafka listener: {ready:?}"),
        }
    })
    .await
    .expect("empty Kafka listener configuration did not fail");

    let error = result.expect_err("empty Kafka listener configuration was accepted");
    assert!(error.to_string().contains("at least one Kafka listener"));
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
    let mut client = TcpStream::connect(endpoints.schema_registry())
        .await
        .unwrap();
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

#[tokio::test]
async fn connection_failure_does_not_stop_the_kafka_listener() {
    let (ready_tx, ready_rx) = oneshot::channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(serve(ephemeral_config(), ready_tx, async {
        let _ = shutdown_rx.await;
    }));
    let endpoints = timeout(Duration::from_secs(1), ready_rx)
        .await
        .expect("server readiness timed out")
        .expect("server readiness channel closed");
    let mut offender = TcpStream::connect(endpoints.kafka())
        .await
        .expect("connect offending Kafka socket");
    let mut survivor = TcpStream::connect(endpoints.kafka())
        .await
        .expect("connect surviving Kafka socket");

    write_frame(
        &mut offender,
        &Bytes::from_static(&[0x7f, 0xff, 0x00, 0x00, 0x00, 0x00, 0x20, 0x01]),
    )
    .await
    .expect("write unknown API request");
    let mut byte = [0_u8; 1];
    assert_eq!(
        timeout(Duration::from_secs(1), offender.read(&mut byte))
            .await
            .expect("offending connection stayed open")
            .expect("read offending connection"),
        0
    );

    write_frame(&mut survivor, &api_versions_request(8_194))
        .await
        .expect("write supported request on surviving socket");
    let mut response = timeout(Duration::from_secs(1), read_frame(&mut survivor))
        .await
        .expect("survivor response timed out")
        .expect("read survivor response")
        .expect("surviving socket was closed");
    let header = ResponseHeader::decode(
        &mut response,
        ApiKey::ApiVersions.response_header_version(4),
    )
    .expect("decode survivor response header");
    let body = ResponseKind::decode(ApiKey::ApiVersions, &mut response, 4)
        .expect("decode survivor response body");
    assert_eq!(header.correlation_id, 8_194);
    let ResponseKind::ApiVersions(body) = body else {
        panic!("expected ApiVersions response");
    };
    assert_eq!(body.error_code, 0);
    assert!(!body.api_keys.is_empty());
    assert!(response.is_empty());

    shutdown_tx.send(()).expect("request server shutdown");
    timeout(Duration::from_secs(1), server)
        .await
        .expect("server did not shut down")
        .expect("server task panicked")
        .expect("server returned an error");
}

fn api_versions_request(correlation_id: i32) -> Bytes {
    let version = 4;
    let header = RequestHeader::default()
        .with_request_api_key(ApiKey::ApiVersions as i16)
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_static_str("runtime-test")));
    let request = ApiVersionsRequest::default()
        .with_client_software_name(StrBytes::from_static_str("runtime-test"))
        .with_client_software_version(StrBytes::from_static_str("1"));
    let mut encoded = BytesMut::new();
    encode_request_header_into_buffer(&mut encoded, &header).expect("encode request header");
    RequestKind::ApiVersions(request)
        .encode(&mut encoded, version)
        .expect("encode request body");
    encoded.freeze()
}
