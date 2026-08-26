use std::time::Duration;

use bytes::{Bytes, BytesMut};
use clap::Parser;
use kafka_protocol::{
    messages::{
        ApiKey, ApiVersionsRequest, RequestHeader, RequestKind, ResponseHeader, ResponseKind,
    },
    protocol::{Decodable, StrBytes, encode_request_header_into_buffer},
};
use memkafka::kafka::{
    codec::{decode_request, encode_response},
    dispatcher::Dispatcher,
    frame::{read_frame, write_frame},
};
use memkafka::{
    config::{Cli, Config},
    server::serve,
};
use tokio::{net::TcpStream, sync::oneshot, time::timeout};

const API_VERSIONS_VERSION: i16 = 3;

#[tokio::test]
async fn api_versions_v3_round_trips_with_correlation_id() {
    let request = encode_api_versions_request(42);

    let decoded = decode_request(request).expect("decode ApiVersions request");
    let response = Dispatcher
        .dispatch(&decoded)
        .await
        .expect("dispatch ApiVersions request");
    let encoded = encode_response(
        decoded.api_key,
        decoded.header.request_api_version,
        decoded.header.correlation_id,
        &response,
    )
    .expect("encode ApiVersions response");
    let (header, response) = decode_api_versions_response(encoded);

    assert_eq!(header.correlation_id, 42);
    assert_eq!(response.error_code, 0);
    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.api_keys.len(), 1);
    assert_eq!(response.api_keys[0].api_key, ApiKey::ApiVersions as i16);
    assert_eq!(response.api_keys[0].min_version, 0);
    assert_eq!(response.api_keys[0].max_version, 4);
}

#[tokio::test]
async fn tcp_api_versions_keeps_connection_open_for_multiple_requests() {
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
    let mut connection = TcpStream::connect(endpoints.kafka)
        .await
        .expect("connect to Kafka endpoint");

    for correlation_id in [73, 74] {
        write_frame(
            &mut connection,
            &encode_api_versions_request(correlation_id),
        )
        .await
        .expect("write ApiVersions frame");
        let response = timeout(Duration::from_secs(1), read_frame(&mut connection))
            .await
            .expect("Kafka response timed out")
            .expect("read Kafka response")
            .expect("server closed the persistent connection");
        let (header, body) = decode_api_versions_response(response);

        assert_eq!(header.correlation_id, correlation_id);
        assert_eq!(body.api_keys.len(), 1);
        assert_eq!(body.api_keys[0].api_key, ApiKey::ApiVersions as i16);
        assert_eq!(body.api_keys[0].min_version, 0);
        assert_eq!(body.api_keys[0].max_version, 4);
    }

    shutdown_tx.send(()).expect("request server shutdown");
    timeout(Duration::from_secs(1), server)
        .await
        .expect("server shutdown timed out")
        .expect("server task panicked")
        .expect("server returned an error");
}

fn ephemeral_config() -> Config {
    Config::try_from(
        Cli::try_parse_from([
            "memkafka",
            "--kafka-listen",
            "127.0.0.1:0",
            "--schema-registry-listen",
            "127.0.0.1:0",
        ])
        .expect("parse test configuration"),
    )
    .expect("build test configuration")
}

fn encode_api_versions_request(correlation_id: i32) -> Bytes {
    let header = RequestHeader::default()
        .with_request_api_key(ApiKey::ApiVersions as i16)
        .with_request_api_version(API_VERSIONS_VERSION)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_static_str("memkafka-wire-test")));
    let request = ApiVersionsRequest::default()
        .with_client_software_name(StrBytes::from_static_str("memkafka-test"))
        .with_client_software_version(StrBytes::from_static_str("1.0"));
    let mut encoded = BytesMut::new();

    encode_request_header_into_buffer(&mut encoded, &header).expect("encode request header");
    RequestKind::ApiVersions(request)
        .encode(&mut encoded, API_VERSIONS_VERSION)
        .expect("encode request body");

    encoded.freeze()
}

fn decode_api_versions_response(
    mut encoded: Bytes,
) -> (
    ResponseHeader,
    kafka_protocol::messages::ApiVersionsResponse,
) {
    let header = ResponseHeader::decode(
        &mut encoded,
        ApiKey::ApiVersions.response_header_version(API_VERSIONS_VERSION),
    )
    .expect("decode response header");
    let response = ResponseKind::decode(ApiKey::ApiVersions, &mut encoded, API_VERSIONS_VERSION)
        .expect("decode response body");

    let ResponseKind::ApiVersions(response) = response else {
        panic!("expected ApiVersions response");
    };
    assert!(encoded.is_empty(), "response has trailing bytes");
    (header, response)
}
