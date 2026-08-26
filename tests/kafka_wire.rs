use std::{num::NonZeroU32, time::Duration};

use bytes::{Bytes, BytesMut};
use clap::Parser;
use kafka_protocol::{
    messages::{
        ApiKey, ApiVersionsRequest, BrokerId, CreateTopicsRequest, MetadataRequest, RequestHeader,
        RequestKind, ResponseHeader, ResponseKind, TopicName,
        create_topics_request::{CreatableReplicaAssignment, CreatableTopic, CreatableTopicConfig},
        create_topics_response::CreateTopicsResponse,
        metadata_request::MetadataRequestTopic,
        metadata_response::MetadataResponse,
    },
    protocol::{Decodable, StrBytes, encode_request_header_into_buffer},
};
use memkafka::kafka::{
    codec::{decode_request, encode_response},
    dispatcher::Dispatcher,
    frame::{read_frame, write_frame},
};
use memkafka::{
    broker::BrokerState,
    config::{AdvertisedAddress, Cli, Config},
    server::serve,
};
use tokio::{net::TcpStream, sync::oneshot, time::timeout};

const API_VERSIONS_VERSION: i16 = 3;

#[tokio::test]
async fn api_versions_v3_round_trips_with_correlation_id() {
    let request = encode_api_versions_request(42);

    let decoded = decode_request(request).expect("decode ApiVersions request");
    let response = test_dispatcher()
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
    assert_eq!(response.api_keys.len(), 3);
    assert_api_range(&response, ApiKey::Metadata, 0, 9);
    assert_api_range(&response, ApiKey::ApiVersions, 0, 4);
    assert_api_range(&response, ApiKey::CreateTopics, 2, 6);
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
        assert_eq!(body.api_keys.len(), 3);
        assert_api_range(&body, ApiKey::Metadata, 0, 9);
        assert_api_range(&body, ApiKey::ApiVersions, 0, 4);
        assert_api_range(&body, ApiKey::CreateTopics, 2, 6);
    }

    shutdown_tx.send(()).expect("request server shutdown");
    timeout(Duration::from_secs(1), server)
        .await
        .expect("server shutdown timed out")
        .expect("server task panicked")
        .expect("server returned an error");
}

#[tokio::test]
async fn metadata_v9_auto_creates_two_partitions() {
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

    write_frame(
        &mut connection,
        &encode_metadata_request(91, "events", true),
    )
    .await
    .expect("write Metadata frame");
    let response = timeout(Duration::from_secs(1), read_frame(&mut connection))
        .await
        .expect("Kafka response timed out")
        .expect("read Kafka response")
        .expect("server closed before Metadata response");
    let (header, response) = decode_metadata_response(response);

    assert_eq!(header.correlation_id, 91);
    assert_eq!(response.brokers.len(), 1);
    assert_eq!(response.brokers[0].node_id, 1);
    assert_eq!(
        response.brokers[0].host.as_str(),
        endpoints.advertised_kafka.host()
    );
    assert_eq!(
        response.brokers[0].port,
        i32::from(endpoints.advertised_kafka.port())
    );
    assert_eq!(
        response.cluster_id.as_ref().map(StrBytes::as_str),
        Some("memkafka")
    );
    assert_eq!(response.controller_id, 1);
    assert_eq!(response.topics.len(), 1);
    assert_eq!(response.topics[0].error_code, 0);
    assert_eq!(
        response.topics[0].name.as_ref().map(|name| name.as_str()),
        Some("events")
    );
    assert!(!response.topics[0].is_internal);
    assert_eq!(response.topics[0].partitions.len(), 2);
    for (partition_index, partition) in response.topics[0].partitions.iter().enumerate() {
        assert_eq!(partition.error_code, 0);
        assert_eq!(partition.partition_index, partition_index as i32);
        assert_eq!(partition.leader_id, 1);
        assert_eq!(partition.replica_nodes, vec![BrokerId::from(1)]);
        assert_eq!(partition.isr_nodes, vec![BrokerId::from(1)]);
        assert!(partition.offline_replicas.is_empty());
    }

    shutdown_tx.send(()).expect("request server shutdown");
    timeout(Duration::from_secs(1), server)
        .await
        .expect("server shutdown timed out")
        .expect("server task panicked")
        .expect("server returned an error");
}

#[tokio::test]
async fn metadata_requires_server_and_request_auto_creation_flags() {
    let server_disabled = test_broker_state(false);
    let response = dispatch_metadata_request(
        &Dispatcher::new(server_disabled.clone()),
        101,
        Some(vec!["server-disabled"]),
        true,
    )
    .await;
    assert_eq!(response.topics[0].error_code, 3);
    assert!(server_disabled.topics().list().await.is_empty());

    let request_disabled = test_broker_state(true);
    let response = dispatch_metadata_request(
        &Dispatcher::new(request_disabled.clone()),
        102,
        Some(vec!["request-disabled"]),
        false,
    )
    .await;
    assert_eq!(response.topics[0].error_code, 3);
    assert!(request_disabled.topics().list().await.is_empty());
}

#[tokio::test]
async fn metadata_null_topics_lists_catalog_in_name_order_without_mutating() {
    let broker = test_broker_state(true);
    broker
        .topics()
        .create_explicit("zebra", 1, 1)
        .await
        .expect("create zebra");
    broker
        .topics()
        .create_explicit("alpha", 3, 1)
        .await
        .expect("create alpha");

    let response =
        dispatch_metadata_request(&Dispatcher::new(broker.clone()), 103, None, false).await;
    let names = response
        .topics
        .iter()
        .map(|topic| topic.name.as_ref().expect("topic name").as_str())
        .collect::<Vec<_>>();

    assert_eq!(names, vec!["alpha", "zebra"]);
    assert_eq!(response.topics[0].partitions.len(), 3);
    assert_eq!(response.topics[1].partitions.len(), 1);
    assert_eq!(broker.topics().list().await.len(), 2);
}

#[tokio::test]
async fn metadata_maps_invalid_names_to_code_17_without_mutating() {
    let broker = test_broker_state(true);

    let response = dispatch_metadata_request(
        &Dispatcher::new(broker.clone()),
        104,
        Some(vec!["bad/name"]),
        true,
    )
    .await;

    assert_eq!(response.topics[0].error_code, 17);
    assert!(response.topics[0].partitions.is_empty());
    assert!(broker.topics().list().await.is_empty());
}

#[tokio::test]
async fn create_topics_v6_creates_six_partitions_over_tcp() {
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

    write_frame(
        &mut connection,
        &encode_create_topics_request(121, vec![creatable_topic("orders", 6, 1)], false),
    )
    .await
    .expect("write CreateTopics frame");
    let response = timeout(Duration::from_secs(1), read_frame(&mut connection))
        .await
        .expect("CreateTopics response timed out")
        .expect("read CreateTopics response")
        .expect("server closed before CreateTopics response");
    let (header, response) = decode_create_topics_response(response);

    assert_eq!(header.correlation_id, 121);
    assert_eq!(response.topics.len(), 1);
    assert_eq!(response.topics[0].name.as_str(), "orders");
    assert_eq!(response.topics[0].error_code, 0);
    assert_eq!(response.topics[0].error_message, None);
    assert_eq!(response.topics[0].num_partitions, 6);
    assert_eq!(response.topics[0].replication_factor, 1);
    assert_eq!(response.topics[0].configs, Some(Vec::new()));

    write_frame(
        &mut connection,
        &encode_metadata_request(122, "orders", false),
    )
    .await
    .expect("write Metadata frame");
    let metadata = timeout(Duration::from_secs(1), read_frame(&mut connection))
        .await
        .expect("Metadata response timed out")
        .expect("read Metadata response")
        .expect("server closed before Metadata response");
    let (_, metadata) = decode_metadata_response(metadata);
    assert_eq!(metadata.topics[0].error_code, 0);
    assert_eq!(metadata.topics[0].partitions.len(), 6);

    shutdown_tx.send(()).expect("request server shutdown");
    timeout(Duration::from_secs(1), server)
        .await
        .expect("server shutdown timed out")
        .expect("server task panicked")
        .expect("server returned an error");
}

#[tokio::test]
async fn create_topics_reports_duplicate_without_replacing_metadata() {
    let broker = test_broker_state(true);
    let dispatcher = Dispatcher::new(broker.clone());

    let first = dispatch_create_topics_request(
        &dispatcher,
        123,
        vec![creatable_topic("orders", 6, 1)],
        false,
    )
    .await;
    let duplicate = dispatch_create_topics_request(
        &dispatcher,
        124,
        vec![creatable_topic("orders", 3, 1)],
        false,
    )
    .await;

    assert_eq!(first.topics[0].error_code, 0);
    assert_eq!(duplicate.topics[0].error_code, 36);
    assert!(
        duplicate.topics[0]
            .error_message
            .as_ref()
            .is_some_and(|message| !message.is_empty())
    );
    assert_eq!(broker.topics().list().await[0].partition_count, 6);
}

#[tokio::test]
async fn create_topics_maps_unsupported_and_invalid_options_without_mutating() {
    let broker = test_broker_state(true);
    let assignment = CreatableReplicaAssignment::default()
        .with_partition_index(0)
        .with_broker_ids(vec![BrokerId::from(1)]);
    let config = CreatableTopicConfig::default()
        .with_name(StrBytes::from_static_str("cleanup.policy"))
        .with_value(Some(StrBytes::from_static_str("compact")));
    let topics = vec![
        creatable_topic("replicated", 3, 2),
        creatable_topic("partitionless", 0, 1),
        creatable_topic("assigned", -1, -1).with_assignments(vec![assignment]),
        creatable_topic("configured", 2, 1).with_configs(vec![config]),
        creatable_topic("bad/name", 2, 1),
    ];

    let response =
        dispatch_create_topics_request(&Dispatcher::new(broker.clone()), 125, topics, false).await;
    let codes = response
        .topics
        .iter()
        .map(|topic| topic.error_code)
        .collect::<Vec<_>>();

    assert_eq!(codes, vec![38, 37, 39, 40, 17]);
    assert!(response.topics.iter().all(|topic| {
        topic
            .error_message
            .as_ref()
            .is_some_and(|message| !message.is_empty())
    }));
    assert!(broker.topics().list().await.is_empty());
}

#[tokio::test]
async fn create_topics_validate_only_reports_success_without_mutating() {
    let broker = test_broker_state(true);

    let response = dispatch_create_topics_request(
        &Dispatcher::new(broker.clone()),
        126,
        vec![creatable_topic("preview", 4, 1)],
        true,
    )
    .await;

    assert_eq!(response.topics[0].error_code, 0);
    assert_eq!(response.topics[0].num_partitions, 4);
    assert_eq!(response.topics[0].replication_factor, 1);
    assert!(broker.topics().list().await.is_empty());
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

fn test_dispatcher() -> Dispatcher {
    Dispatcher::new(test_broker_state(true))
}

fn test_broker_state(auto_create_topics: bool) -> BrokerState {
    BrokerState::new(
        1,
        AdvertisedAddress::new("127.0.0.1", 9092).expect("valid test address"),
        auto_create_topics,
        NonZeroU32::new(2).expect("nonzero literal"),
    )
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

fn encode_metadata_request(
    correlation_id: i32,
    topic: &'static str,
    allow_auto_topic_creation: bool,
) -> Bytes {
    encode_metadata_topics(correlation_id, Some(vec![topic]), allow_auto_topic_creation)
}

fn encode_metadata_topics(
    correlation_id: i32,
    topics: Option<Vec<&'static str>>,
    allow_auto_topic_creation: bool,
) -> Bytes {
    const VERSION: i16 = 9;
    let header = RequestHeader::default()
        .with_request_api_key(ApiKey::Metadata as i16)
        .with_request_api_version(VERSION)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_static_str("memkafka-wire-test")));
    let topics = topics.map(|topics| {
        topics
            .into_iter()
            .map(|topic| {
                MetadataRequestTopic::default()
                    .with_name(Some(TopicName::from(StrBytes::from_static_str(topic))))
            })
            .collect()
    });
    let request = MetadataRequest::default()
        .with_topics(topics)
        .with_allow_auto_topic_creation(allow_auto_topic_creation);
    let mut encoded = BytesMut::new();

    encode_request_header_into_buffer(&mut encoded, &header).expect("encode request header");
    RequestKind::Metadata(request)
        .encode(&mut encoded, VERSION)
        .expect("encode Metadata body");
    encoded.freeze()
}

async fn dispatch_metadata_request(
    dispatcher: &Dispatcher,
    correlation_id: i32,
    topics: Option<Vec<&'static str>>,
    allow_auto_topic_creation: bool,
) -> MetadataResponse {
    let decoded = decode_request(encode_metadata_topics(
        correlation_id,
        topics,
        allow_auto_topic_creation,
    ))
    .expect("decode Metadata request");
    let response = dispatcher
        .dispatch(&decoded)
        .await
        .expect("dispatch Metadata request");
    let encoded = encode_response(
        decoded.api_key,
        decoded.header.request_api_version,
        decoded.header.correlation_id,
        &response,
    )
    .expect("encode Metadata response");
    let (header, response) = decode_metadata_response(encoded);
    assert_eq!(header.correlation_id, correlation_id);
    response
}

fn creatable_topic(name: &'static str, partitions: i32, replication_factor: i16) -> CreatableTopic {
    CreatableTopic::default()
        .with_name(TopicName::from(StrBytes::from_static_str(name)))
        .with_num_partitions(partitions)
        .with_replication_factor(replication_factor)
}

fn encode_create_topics_request(
    correlation_id: i32,
    topics: Vec<CreatableTopic>,
    validate_only: bool,
) -> Bytes {
    const VERSION: i16 = 6;
    let header = RequestHeader::default()
        .with_request_api_key(ApiKey::CreateTopics as i16)
        .with_request_api_version(VERSION)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_static_str("memkafka-wire-test")));
    let request = CreateTopicsRequest::default()
        .with_topics(topics)
        .with_timeout_ms(1_000)
        .with_validate_only(validate_only);
    let mut encoded = BytesMut::new();

    encode_request_header_into_buffer(&mut encoded, &header).expect("encode request header");
    RequestKind::CreateTopics(request)
        .encode(&mut encoded, VERSION)
        .expect("encode CreateTopics body");
    encoded.freeze()
}

async fn dispatch_create_topics_request(
    dispatcher: &Dispatcher,
    correlation_id: i32,
    topics: Vec<CreatableTopic>,
    validate_only: bool,
) -> CreateTopicsResponse {
    let decoded = decode_request(encode_create_topics_request(
        correlation_id,
        topics,
        validate_only,
    ))
    .expect("decode CreateTopics request");
    let response = dispatcher
        .dispatch(&decoded)
        .await
        .expect("dispatch CreateTopics request");
    let encoded = encode_response(
        decoded.api_key,
        decoded.header.request_api_version,
        decoded.header.correlation_id,
        &response,
    )
    .expect("encode CreateTopics response");
    let (header, response) = decode_create_topics_response(encoded);
    assert_eq!(header.correlation_id, correlation_id);
    response
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

fn decode_metadata_response(mut encoded: Bytes) -> (ResponseHeader, MetadataResponse) {
    const VERSION: i16 = 9;
    let header = ResponseHeader::decode(
        &mut encoded,
        ApiKey::Metadata.response_header_version(VERSION),
    )
    .expect("decode response header");
    let response = ResponseKind::decode(ApiKey::Metadata, &mut encoded, VERSION)
        .expect("decode Metadata response body");
    let ResponseKind::Metadata(response) = response else {
        panic!("expected Metadata response");
    };
    assert!(encoded.is_empty(), "response has trailing bytes");
    (header, response)
}

fn decode_create_topics_response(mut encoded: Bytes) -> (ResponseHeader, CreateTopicsResponse) {
    const VERSION: i16 = 6;
    let header = ResponseHeader::decode(
        &mut encoded,
        ApiKey::CreateTopics.response_header_version(VERSION),
    )
    .expect("decode response header");
    let response = ResponseKind::decode(ApiKey::CreateTopics, &mut encoded, VERSION)
        .expect("decode CreateTopics response body");
    let ResponseKind::CreateTopics(response) = response else {
        panic!("expected CreateTopics response");
    };
    assert!(encoded.is_empty(), "response has trailing bytes");
    (header, response)
}

fn assert_api_range(
    response: &kafka_protocol::messages::ApiVersionsResponse,
    api_key: ApiKey,
    min_version: i16,
    max_version: i16,
) {
    let range = response
        .api_keys
        .iter()
        .find(|range| range.api_key == api_key as i16)
        .unwrap_or_else(|| panic!("missing {api_key:?} API range"));
    assert_eq!(range.min_version, min_version);
    assert_eq!(range.max_version, max_version);
}
