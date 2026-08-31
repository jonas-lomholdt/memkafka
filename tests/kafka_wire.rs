use std::{num::NonZeroU32, time::Duration};

use bytes::{BufMut, Bytes, BytesMut};
use clap::Parser;
use kafka_protocol::{
    ResponseError,
    messages::{
        ApiKey, ApiVersionsRequest, BrokerId, CreateTopicsRequest, DescribeConfigsRequest,
        DescribeGroupsRequest, FetchRequest, FindCoordinatorRequest, GroupId, HeartbeatRequest,
        InitProducerIdRequest, JoinGroupRequest, LeaveGroupRequest, ListGroupsRequest,
        ListOffsetsRequest, MetadataRequest, OffsetCommitRequest, OffsetFetchRequest,
        ProduceRequest, RequestHeader, RequestKind, ResponseHeader, ResponseKind, SyncGroupRequest,
        TopicName, TransactionalId,
        create_topics_request::{CreatableReplicaAssignment, CreatableTopic, CreatableTopicConfig},
        create_topics_response::CreateTopicsResponse,
        describe_configs_request::DescribeConfigsResource,
        fetch_request::{FetchPartition, FetchTopic},
        fetch_response::FetchResponse,
        join_group_request::JoinGroupRequestProtocol,
        leave_group_request::MemberIdentity,
        list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
        list_offsets_response::ListOffsetsResponse,
        metadata_request::MetadataRequestTopic,
        metadata_response::MetadataResponse,
        offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
        offset_fetch_request::OffsetFetchRequestTopic,
        produce_request::{PartitionProduceData, TopicProduceData},
        produce_response::ProduceResponse,
        sync_group_request::SyncGroupRequestAssignment,
    },
    protocol::{Decodable, StrBytes, encode_request_header_into_buffer},
    records::{
        Compression, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, Record,
        RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    },
};
use memkafka::kafka::{
    codec::{DecodeStage, DecodedFrame, RequestDecodeError, RequestPrefix, decode_frame},
    connection,
    dispatcher::Dispatcher,
    frame::{read_frame, write_frame},
};
use memkafka::{
    broker::BrokerState,
    config::{AdvertisedAddress, Cli, Config},
    server::serve,
};
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    sync::{oneshot, watch},
    task::JoinHandle,
    time::timeout,
};

const API_VERSIONS_VERSION: i16 = 3;

#[test]
fn codec_decodes_generated_advertised_request_and_flexible_header_tags() {
    // This raw ApiVersions v3 frame has a flexible request header with one unknown tagged field.
    // It catches decoding the header with the wrong version or discarding its tagged fields.
    let frame = Bytes::from_static(&[
        0x00, 0x12, 0x00, 0x03, 0x01, 0x02, 0x03, 0x04, 0x00, 0x01, b'x', 0x01, 0x07, 0x03, 0xaa,
        0xbb, 0xcc, 0x07, b'c', b'l', b'i', b'e', b'n', b't', 0x02, b'1', 0x00,
    ]);

    let DecodedFrame::Request(request) = decode_frame(frame).expect("decode ApiVersions v3") else {
        panic!("expected a decoded request");
    };

    assert_eq!(request.api_key, ApiKey::ApiVersions);
    assert_eq!(request.header.request_api_version, 3);
    assert_eq!(request.header.correlation_id, 0x0102_0304);
    assert_eq!(request.header.client_id.as_deref(), Some("x"));
    assert_eq!(
        request.header.unknown_tagged_fields.get(&7),
        Some(&Bytes::from_static(&[0xaa, 0xbb, 0xcc]))
    );
    let RequestKind::ApiVersions(body) = request.body else {
        panic!("expected ApiVersions body");
    };
    assert_eq!(body.client_software_name.as_str(), "client");
    assert_eq!(body.client_software_version.as_str(), "1");
}

#[test]
fn codec_routes_unsupported_api_versions_after_decoding_only_its_header() {
    // The final byte is an invalid body. This catches accidentally decoding an unsupported
    // ApiVersions body instead of returning the special routeable outcome after its header.
    let cases = [
        (
            Bytes::from_static(&[
                0x00, 0x12, 0x00, 0x05, 0x11, 0x22, 0x33, 0x44, 0xff, 0xff, 0x00, 0xff,
            ]),
            5,
            0x1122_3344,
        ),
        (
            Bytes::from_static(&[
                0x00, 0x12, 0x7f, 0xff, 0x55, 0x66, 0x77, 0x00, 0xff, 0xff, 0x00, 0xff,
            ]),
            i16::MAX,
            0x5566_7700,
        ),
    ];

    for (frame, requested_version, expected_correlation_id) in cases {
        let DecodedFrame::UnsupportedApiVersions {
            header,
            requested_version: actual_version,
        } = decode_frame(frame).expect("route unsupported ApiVersions")
        else {
            panic!("expected unsupported ApiVersions outcome");
        };

        assert_eq!(actual_version, requested_version);
        assert_eq!(header.request_api_key, ApiKey::ApiVersions as i16);
        assert_eq!(header.request_api_version, requested_version);
        assert_eq!(header.correlation_id, expected_correlation_id);
    }
}

#[test]
fn codec_reports_unknown_raw_api_key_with_fixed_prefix_context() {
    // This catches converting the API key only after a header decode has already failed.
    let error = decode_frame(Bytes::from_static(&[
        0x7f, 0xff, 0x00, 0x09, 0x01, 0x02, 0x03, 0x04,
    ]))
    .expect_err("unknown API key must be routeable");

    let RequestDecodeError::UnknownApiKey { prefix } = error else {
        panic!("expected unknown API key error");
    };
    assert_eq!(
        prefix,
        RequestPrefix {
            raw_api_key: i16::MAX,
            api_version: 9,
            correlation_id: 0x0102_0304,
        }
    );
}

#[test]
fn codec_reports_non_api_versions_schema_range_errors_before_headers() {
    // These frames contain no header. They catch treating the generated schema range as support
    // policy or attempting to decode an impossible header/body before returning a close outcome.
    let cases = [
        (
            Bytes::from_static(&[0x00, 0x03, 0xff, 0xff, 0x01, 0x02, 0x03, 0x04]),
            -1,
            0x0102_0304,
        ),
        (
            Bytes::from_static(&[0x00, 0x03, 0x00, 0x0e, 0x05, 0x06, 0x07, 0x08]),
            14,
            0x0506_0708,
        ),
    ];

    for (frame, api_version, expected_correlation_id) in cases {
        let error = decode_frame(frame).expect_err("out-of-schema version must be routeable");
        let RequestDecodeError::VersionOutOfSchema { prefix, api_key } = error else {
            panic!("expected schema-range error");
        };
        assert_eq!(api_key, ApiKey::Metadata);
        assert_eq!(
            prefix,
            RequestPrefix {
                raw_api_key: ApiKey::Metadata as i16,
                api_version,
                correlation_id: expected_correlation_id,
            }
        );
    }
}

#[test]
fn codec_reports_malformed_stages_with_available_context() {
    // Each fixture exercises a separate branch: too little fixed prefix, truncated header,
    // truncated body, and extra body bytes after a successful decode.
    let cases = [
        (
            Bytes::from_static(&[0x00, 0x12, 0x00, 0x03, 0x01, 0x02, 0x03]),
            None,
            None,
            None,
        ),
        (
            Bytes::from_static(&[0x00, 0x03, 0x00, 0x09, 0x01, 0x02, 0x03, 0x04]),
            Some(RequestPrefix {
                raw_api_key: ApiKey::Metadata as i16,
                api_version: 9,
                correlation_id: 0x0102_0304,
            }),
            Some(ApiKey::Metadata),
            Some(DecodeStage::Header),
        ),
        (
            Bytes::from_static(&[0x00, 0x03, 0x00, 0x04, 0x11, 0x22, 0x33, 0x44, 0xff, 0xff]),
            Some(RequestPrefix {
                raw_api_key: ApiKey::Metadata as i16,
                api_version: 4,
                correlation_id: 0x1122_3344,
            }),
            Some(ApiKey::Metadata),
            Some(DecodeStage::Body),
        ),
        (
            Bytes::from_static(&[
                0x00, 0x12, 0x00, 0x03, 0x55, 0x66, 0x77, 0x00, 0xff, 0xff, 0x00, 0x01, 0x01, 0x00,
                0x99,
            ]),
            Some(RequestPrefix {
                raw_api_key: ApiKey::ApiVersions as i16,
                api_version: 3,
                correlation_id: 0x5566_7700,
            }),
            Some(ApiKey::ApiVersions),
            Some(DecodeStage::TrailingBytes),
        ),
    ];

    for (frame, expected_prefix, expected_api_key, expected_stage) in cases {
        let error = decode_frame(frame).expect_err("malformed frame must fail");
        match (error, expected_stage) {
            (RequestDecodeError::TruncatedPrefix, None) => {}
            (
                RequestDecodeError::Malformed {
                    prefix,
                    api_key,
                    stage,
                    ..
                },
                Some(expected_stage),
            ) => {
                assert_eq!(prefix, expected_prefix);
                assert_eq!(api_key, expected_api_key);
                assert_eq!(stage, expected_stage);
            }
            (error, _) => panic!("unexpected decode error: {error}"),
        }
    }
}

#[tokio::test]
async fn api_versions_v3_round_trips_with_correlation_id() {
    let response = dispatch_kind(
        &test_dispatcher(),
        ApiKey::ApiVersions,
        3,
        RequestKind::ApiVersions(
            ApiVersionsRequest::default()
                .with_client_software_name(StrBytes::from_static_str("memkafka-test"))
                .with_client_software_version(StrBytes::from_static_str("1.0")),
        ),
    )
    .await;
    let ResponseKind::ApiVersions(response) = response else {
        panic!("expected ApiVersions response");
    };

    assert_eq!(response.error_code, 0);
    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.api_keys.len(), 17);
    assert_api_range(&response, ApiKey::Metadata, 4, 9);
    assert_api_range(&response, ApiKey::ApiVersions, 3, 4);
    assert_api_range(&response, ApiKey::CreateTopics, 4, 6);
    assert_api_range(&response, ApiKey::Produce, 7, 7);
    assert_api_range(&response, ApiKey::ListOffsets, 3, 3);
    assert_api_range(&response, ApiKey::Fetch, 4, 4);
    assert_api_range(&response, ApiKey::FindCoordinator, 2, 2);
    assert_api_range(&response, ApiKey::JoinGroup, 5, 5);
    assert_api_range(&response, ApiKey::SyncGroup, 3, 3);
    assert_api_range(&response, ApiKey::Heartbeat, 3, 3);
    assert_api_range(&response, ApiKey::LeaveGroup, 1, 3);
    assert_api_range(&response, ApiKey::OffsetCommit, 7, 7);
    assert_api_range(&response, ApiKey::OffsetFetch, 5, 5);
    assert_api_range(&response, ApiKey::ListGroups, 0, 0);
    assert_api_range(&response, ApiKey::DescribeGroups, 0, 0);
    assert_api_range(&response, ApiKey::InitProducerId, 0, 0);
    assert_api_range(&response, ApiKey::DescribeConfigs, 1, 1);
}

#[tokio::test]
async fn join_group_v5_member_id_required_uses_non_nullable_protocol_name() {
    let response = dispatch_kind(
        &test_dispatcher(),
        ApiKey::JoinGroup,
        5,
        RequestKind::JoinGroup(
            JoinGroupRequest::default()
                .with_group_id(GroupId::from(StrBytes::from_static_str("join-group-error")))
                .with_session_timeout_ms(10_000)
                .with_rebalance_timeout_ms(30_000)
                .with_member_id(StrBytes::default())
                .with_protocol_type(StrBytes::from_static_str("consumer"))
                .with_protocols(vec![
                    JoinGroupRequestProtocol::default()
                        .with_name(StrBytes::from_static_str("cooperative-sticky"))
                        .with_metadata(Bytes::from_static(b"subscription")),
                ]),
        ),
    )
    .await;
    let ResponseKind::JoinGroup(response) = response else {
        panic!("expected JoinGroup response")
    };

    assert_eq!(response.error_code, ResponseError::MemberIdRequired.code());
    assert!(!response.member_id.is_empty());
    assert_eq!(response.protocol_name, Some(StrBytes::default()));
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
        assert_eq!(body.api_keys.len(), 17);
        assert_api_range(&body, ApiKey::Metadata, 4, 9);
        assert_api_range(&body, ApiKey::ApiVersions, 3, 4);
        assert_api_range(&body, ApiKey::CreateTopics, 4, 6);
        assert_api_range(&body, ApiKey::Produce, 7, 7);
        assert_api_range(&body, ApiKey::ListOffsets, 3, 3);
        assert_api_range(&body, ApiKey::Fetch, 4, 4);
        assert_api_range(&body, ApiKey::FindCoordinator, 2, 2);
        assert_api_range(&body, ApiKey::JoinGroup, 5, 5);
        assert_api_range(&body, ApiKey::SyncGroup, 3, 3);
        assert_api_range(&body, ApiKey::Heartbeat, 3, 3);
        assert_api_range(&body, ApiKey::LeaveGroup, 1, 3);
        assert_api_range(&body, ApiKey::OffsetCommit, 7, 7);
        assert_api_range(&body, ApiKey::OffsetFetch, 5, 5);
        assert_api_range(&body, ApiKey::ListGroups, 0, 0);
        assert_api_range(&body, ApiKey::DescribeGroups, 0, 0);
        assert_api_range(&body, ApiKey::InitProducerId, 0, 0);
        assert_api_range(&body, ApiKey::DescribeConfigs, 1, 1);
    }

    shutdown_tx.send(()).expect("request server shutdown");
    timeout(Duration::from_secs(1), server)
        .await
        .expect("server shutdown timed out")
        .expect("server task panicked")
        .expect("server returned an error");
}

#[tokio::test]
async fn same_connection_survives_a_typed_unsupported_version_response() {
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

    let unsupported = encode_request_kind(
        ApiKey::Metadata,
        3,
        7_301,
        RequestKind::Metadata(MetadataRequest::default().with_topics(Some(vec![
            MetadataRequestTopic::default().with_name(Some(topic_name("same-connection-topic"))),
        ]))),
    );
    write_frame(&mut connection, &unsupported)
        .await
        .expect("write unsupported Metadata request");
    let encoded = timeout(Duration::from_secs(1), read_frame(&mut connection))
        .await
        .expect("unsupported response timed out")
        .expect("read unsupported response")
        .expect("typed rejection closed the connection");
    let (header, response) = decode_response_kind(ApiKey::Metadata, 3, encoded);
    assert_eq!(header.correlation_id, 7_301);
    let ResponseKind::Metadata(response) = response else {
        panic!("expected Metadata response");
    };
    assert_eq!(response.topics.len(), 1);
    assert_eq!(
        response.topics[0].error_code,
        ResponseError::UnsupportedVersion.code()
    );
    assert_eq!(
        response.topics[0].name.as_ref().map(|name| name.0.as_str()),
        Some("same-connection-topic")
    );

    write_frame(&mut connection, &encode_api_versions_request_for(7_302, 4))
        .await
        .expect("write supported ApiVersions request");
    let encoded = timeout(Duration::from_secs(1), read_frame(&mut connection))
        .await
        .expect("ApiVersions response timed out")
        .expect("read ApiVersions response")
        .expect("server closed the surviving connection");
    let (header, response) = decode_response_kind(ApiKey::ApiVersions, 4, encoded);
    assert_eq!(header.correlation_id, 7_302);
    let ResponseKind::ApiVersions(response) = response else {
        panic!("expected ApiVersions response");
    };
    assert_eq!(response.error_code, 0);
    assert_eq!(response.api_keys.len(), 17);

    shutdown_tx.send(()).expect("request server shutdown");
    timeout(Duration::from_secs(1), server)
        .await
        .expect("server shutdown timed out")
        .expect("server task panicked")
        .expect("server returned an error");
}

#[tokio::test]
async fn fatal_connection_inputs_close_only_the_offending_socket() {
    let (ready_tx, ready_rx) = oneshot::channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(serve(ephemeral_config(), ready_tx, async {
        let _ = shutdown_rx.await;
    }));
    let endpoints = timeout(Duration::from_secs(1), ready_rx)
        .await
        .expect("server readiness timed out")
        .expect("server readiness channel closed");

    let mut trailing = BytesMut::from(&encode_api_versions_request_for(7_405, 4)[..]);
    trailing.put_u8(0xff);
    let fatal_frames = [
        (
            "unknown raw API key",
            Bytes::from_static(&[0x7f, 0xff, 0x00, 0x00, 0x00, 0x00, 0x1c, 0xe9]),
        ),
        (
            "generated but unadvertised API",
            encode_request_kind(
                ApiKey::DeleteTopics,
                ApiKey::DeleteTopics.valid_versions().min,
                7_402,
                RequestKind::DeleteTopics(Default::default()),
            ),
        ),
        (
            "out-of-generated-schema version",
            raw_request_prefix(
                ApiKey::Metadata as i16,
                ApiKey::Metadata.valid_versions().max + 1,
                7_403,
            ),
        ),
        (
            "malformed body",
            encode_request_header_only(ApiKey::Metadata, 4, 7_404),
        ),
        ("trailing bytes", trailing.freeze()),
    ];

    for (case, frame) in fatal_frames {
        let mut offender = TcpStream::connect(endpoints.kafka)
            .await
            .unwrap_or_else(|error| panic!("connect offender for {case}: {error}"));
        let mut survivor = TcpStream::connect(endpoints.kafka)
            .await
            .unwrap_or_else(|error| panic!("connect survivor for {case}: {error}"));

        write_frame(&mut offender, &frame)
            .await
            .unwrap_or_else(|error| panic!("write fatal frame for {case}: {error}"));
        assert_socket_closed(&mut offender, case).await;

        let correlation_id = 7_500 + i32::from(frame[0]);
        write_frame(
            &mut survivor,
            &encode_api_versions_request_for(correlation_id, 4),
        )
        .await
        .unwrap_or_else(|error| panic!("write survivor request after {case}: {error}"));
        let encoded = timeout(Duration::from_secs(1), read_frame(&mut survivor))
            .await
            .unwrap_or_else(|_| panic!("survivor timed out after {case}"))
            .unwrap_or_else(|error| panic!("read survivor response after {case}: {error}"))
            .unwrap_or_else(|| panic!("survivor closed after {case}"));
        let (header, response) = decode_response_kind(ApiKey::ApiVersions, 4, encoded);
        assert_eq!(header.correlation_id, correlation_id);
        let ResponseKind::ApiVersions(response) = response else {
            panic!("expected ApiVersions response after {case}");
        };
        assert_eq!(response.error_code, 0, "survivor failed after {case}");
    }

    shutdown_tx.send(()).expect("request server shutdown");
    timeout(Duration::from_secs(1), server)
        .await
        .expect("server shutdown timed out")
        .expect("server task panicked")
        .expect("server returned an error");
}

#[tokio::test]
async fn rejected_produce_versions_do_not_append_and_acks_zero_writes_nothing() {
    let server = SpawnedServer::start(ephemeral_config()).await;
    let mut connection = server.connect().await;

    write_frame(
        &mut connection,
        &encode_request_kind(
            ApiKey::CreateTopics,
            6,
            7_601,
            RequestKind::CreateTopics(
                CreateTopicsRequest::default()
                    .with_topics(vec![creatable_topic("rejected-produce", 1, 1)])
                    .with_timeout_ms(1_000),
            ),
        ),
    )
    .await
    .expect("create observation topic");
    let (_, response) = decode_response_kind(
        ApiKey::CreateTopics,
        6,
        read_response(&mut connection, "CreateTopics").await,
    );
    let ResponseKind::CreateTopics(response) = response else {
        panic!("expected CreateTopics response");
    };
    assert_eq!(response.topics[0].error_code, 0);

    for (correlation_id, acks) in [(7_602, 1), (7_604, 0)] {
        write_frame(
            &mut connection,
            &encode_request_kind(
                ApiKey::Produce,
                6,
                correlation_id,
                RequestKind::Produce(
                    ProduceRequest::default()
                        .with_acks(acks)
                        .with_timeout_ms(1_000)
                        .with_topic_data(vec![produce_topic(
                            "rejected-produce",
                            vec![produce_partition(0, record_batch(&["must-not-append"]))],
                        )]),
                ),
            ),
        )
        .await
        .expect("write unsupported Produce");

        if acks == 0 {
            assert!(
                timeout(Duration::from_millis(50), read_frame(&mut connection))
                    .await
                    .is_err(),
                "unsupported acks=0 Produce wrote response bytes"
            );
            write_frame(
                &mut connection,
                &encode_api_versions_request_for(correlation_id + 1, 4),
            )
            .await
            .expect("reuse connection after rejected acks=0 Produce");
            let (header, response) = decode_response_kind(
                ApiKey::ApiVersions,
                4,
                read_response(&mut connection, "ApiVersions after acks=0").await,
            );
            assert_eq!(header.correlation_id, correlation_id + 1);
            let ResponseKind::ApiVersions(response) = response else {
                panic!("expected ApiVersions response");
            };
            assert_eq!(response.error_code, 0);
        } else {
            let (header, response) = decode_response_kind(
                ApiKey::Produce,
                6,
                read_response(&mut connection, "unsupported Produce").await,
            );
            assert_eq!(header.correlation_id, correlation_id);
            let ResponseKind::Produce(response) = response else {
                panic!("expected Produce response");
            };
            assert_eq!(response.responses.len(), 1);
            assert_eq!(response.responses[0].name.0.as_str(), "rejected-produce");
            assert_eq!(response.responses[0].partition_responses.len(), 1);
            let partition = &response.responses[0].partition_responses[0];
            assert_eq!(partition.index, 0);
            assert_eq!(
                partition.error_code,
                ResponseError::UnsupportedVersion.code()
            );
            assert_eq!(partition.base_offset, -1);
        }

        write_frame(
            &mut connection,
            &encode_request_kind(
                ApiKey::ListOffsets,
                3,
                correlation_id + 2,
                RequestKind::ListOffsets(
                    ListOffsetsRequest::default()
                        .with_replica_id(BrokerId::from(-1))
                        .with_isolation_level(0)
                        .with_topics(vec![list_offsets_topic("rejected-produce", vec![(0, -1)])]),
                ),
            ),
        )
        .await
        .expect("write ListOffsets observation");
        let (_, response) = decode_response_kind(
            ApiKey::ListOffsets,
            3,
            read_response(&mut connection, "ListOffsets after rejected Produce").await,
        );
        let ResponseKind::ListOffsets(response) = response else {
            panic!("expected ListOffsets response");
        };
        let partition = &response.topics[0].partitions[0];
        assert_eq!(partition.partition_index, 0);
        assert_eq!(partition.error_code, 0);
        assert_eq!(partition.offset, 0, "rejected Produce appended a record");
    }

    server.shutdown().await;
}

#[tokio::test]
async fn rejected_create_topics_does_not_create_the_named_topic() {
    let mut config = ephemeral_config();
    config.auto_create_topics = false;
    let server = SpawnedServer::start(config).await;
    let mut connection = server.connect().await;

    write_frame(
        &mut connection,
        &encode_request_kind(
            ApiKey::CreateTopics,
            3,
            7_701,
            RequestKind::CreateTopics(
                CreateTopicsRequest::default()
                    .with_topics(vec![creatable_topic("must-stay-absent", 1, 1)])
                    .with_timeout_ms(1_000),
            ),
        ),
    )
    .await
    .expect("write unsupported CreateTopics");
    let (header, response) = decode_response_kind(
        ApiKey::CreateTopics,
        3,
        read_response(&mut connection, "unsupported CreateTopics").await,
    );
    assert_eq!(header.correlation_id, 7_701);
    let ResponseKind::CreateTopics(response) = response else {
        panic!("expected CreateTopics response");
    };
    assert_eq!(response.topics.len(), 1);
    assert_eq!(response.topics[0].name.0.as_str(), "must-stay-absent");
    assert_eq!(
        response.topics[0].error_code,
        ResponseError::UnsupportedVersion.code()
    );

    write_frame(
        &mut connection,
        &encode_metadata_request(7_702, "must-stay-absent", false),
    )
    .await
    .expect("write Metadata observation");
    let (_, response) = decode_response_kind(
        ApiKey::Metadata,
        9,
        read_response(&mut connection, "Metadata after rejected CreateTopics").await,
    );
    let ResponseKind::Metadata(response) = response else {
        panic!("expected Metadata response");
    };
    assert_eq!(response.topics.len(), 1);
    assert_eq!(
        response.topics[0].name.as_ref().map(|name| name.0.as_str()),
        Some("must-stay-absent")
    );
    assert_eq!(
        response.topics[0].error_code,
        ResponseError::UnknownTopicOrPartition.code()
    );
    assert!(response.topics[0].partitions.is_empty());

    server.shutdown().await;
}

#[tokio::test]
async fn rejected_offset_commit_does_not_store_the_requested_offset() {
    let server = SpawnedServer::start(ephemeral_config()).await;
    let mut connection = server.connect().await;
    let group_id = GroupId::from(StrBytes::from_static_str("rejected-offset-group"));

    write_frame(
        &mut connection,
        &encode_request_kind(
            ApiKey::OffsetCommit,
            6,
            7_801,
            RequestKind::OffsetCommit(
                OffsetCommitRequest::default()
                    .with_group_id(group_id.clone())
                    .with_generation_id_or_member_epoch(1)
                    .with_member_id(StrBytes::from_static_str("member-1"))
                    .with_topics(vec![
                        OffsetCommitRequestTopic::default()
                            .with_name(topic_name("rejected-offset-topic"))
                            .with_partitions(vec![
                                OffsetCommitRequestPartition::default()
                                    .with_partition_index(0)
                                    .with_committed_offset(99),
                            ]),
                    ]),
            ),
        ),
    )
    .await
    .expect("write unsupported OffsetCommit");
    let (header, response) = decode_response_kind(
        ApiKey::OffsetCommit,
        6,
        read_response(&mut connection, "unsupported OffsetCommit").await,
    );
    assert_eq!(header.correlation_id, 7_801);
    let ResponseKind::OffsetCommit(response) = response else {
        panic!("expected OffsetCommit response");
    };
    assert_eq!(response.topics[0].name.0.as_str(), "rejected-offset-topic");
    assert_eq!(response.topics[0].partitions[0].partition_index, 0);
    assert_eq!(
        response.topics[0].partitions[0].error_code,
        ResponseError::UnsupportedVersion.code()
    );

    write_frame(
        &mut connection,
        &encode_request_kind(
            ApiKey::OffsetFetch,
            5,
            7_802,
            RequestKind::OffsetFetch(
                OffsetFetchRequest::default()
                    .with_group_id(group_id)
                    .with_topics(Some(vec![
                        OffsetFetchRequestTopic::default()
                            .with_name(topic_name("rejected-offset-topic"))
                            .with_partition_indexes(vec![0]),
                    ])),
            ),
        ),
    )
    .await
    .expect("write OffsetFetch observation");
    let (_, response) = decode_response_kind(
        ApiKey::OffsetFetch,
        5,
        read_response(&mut connection, "OffsetFetch after rejected commit").await,
    );
    let ResponseKind::OffsetFetch(response) = response else {
        panic!("expected OffsetFetch response");
    };
    assert_eq!(response.error_code, 0);
    assert_eq!(response.topics[0].name.0.as_str(), "rejected-offset-topic");
    let partition = &response.topics[0].partitions[0];
    assert_eq!(partition.partition_index, 0);
    assert_eq!(partition.error_code, 0);
    assert_eq!(partition.committed_offset, -1);

    server.shutdown().await;
}

#[tokio::test]
async fn rejected_group_mutations_leave_the_group_catalog_unchanged() {
    let server = SpawnedServer::start(ephemeral_config()).await;
    let mut connection = server.connect().await;
    let group_ids = ["rejected-join", "rejected-sync", "rejected-leave"];
    let requests = [
        (
            ApiKey::JoinGroup,
            4,
            RequestKind::JoinGroup(
                JoinGroupRequest::default()
                    .with_group_id(GroupId::from(StrBytes::from_static_str(group_ids[0])))
                    .with_session_timeout_ms(10_000)
                    .with_rebalance_timeout_ms(30_000)
                    .with_member_id(StrBytes::default())
                    .with_protocol_type(StrBytes::from_static_str("consumer"))
                    .with_protocols(vec![
                        JoinGroupRequestProtocol::default()
                            .with_name(StrBytes::from_static_str("cooperative-sticky"))
                            .with_metadata(Bytes::from_static(b"subscription")),
                    ]),
            ),
        ),
        (
            ApiKey::SyncGroup,
            2,
            RequestKind::SyncGroup(
                SyncGroupRequest::default()
                    .with_group_id(GroupId::from(StrBytes::from_static_str(group_ids[1])))
                    .with_generation_id(1)
                    .with_member_id(StrBytes::from_static_str("member-1")),
            ),
        ),
        (
            ApiKey::LeaveGroup,
            0,
            RequestKind::LeaveGroup(
                LeaveGroupRequest::default()
                    .with_group_id(GroupId::from(StrBytes::from_static_str(group_ids[2])))
                    .with_member_id(StrBytes::from_static_str("member-1")),
            ),
        ),
    ];

    for (index, (api_key, version, request)) in requests.into_iter().enumerate() {
        let correlation_id = 7_900 + i32::try_from(index).expect("small index");
        write_frame(
            &mut connection,
            &encode_request_kind(api_key, version, correlation_id, request),
        )
        .await
        .unwrap_or_else(|error| panic!("write unsupported {api_key:?}: {error}"));
        let (header, response) = decode_response_kind(
            api_key,
            version,
            read_response(&mut connection, "unsupported group mutation").await,
        );
        assert_eq!(header.correlation_id, correlation_id);
        let error_code = match response {
            ResponseKind::JoinGroup(response) => response.error_code,
            ResponseKind::SyncGroup(response) => response.error_code,
            ResponseKind::LeaveGroup(response) => response.error_code,
            _ => panic!("unexpected response for {api_key:?}"),
        };
        assert_eq!(error_code, ResponseError::UnsupportedVersion.code());
    }

    write_frame(
        &mut connection,
        &encode_request_kind(
            ApiKey::ListGroups,
            0,
            7_904,
            RequestKind::ListGroups(ListGroupsRequest::default()),
        ),
    )
    .await
    .expect("write ListGroups observation");
    let (_, response) = decode_response_kind(
        ApiKey::ListGroups,
        0,
        read_response(&mut connection, "ListGroups after rejected mutations").await,
    );
    let ResponseKind::ListGroups(response) = response else {
        panic!("expected ListGroups response");
    };
    assert_eq!(response.error_code, 0);
    assert!(response.groups.is_empty());

    write_frame(
        &mut connection,
        &encode_request_kind(
            ApiKey::DescribeGroups,
            0,
            7_905,
            RequestKind::DescribeGroups(
                DescribeGroupsRequest::default().with_groups(
                    group_ids
                        .into_iter()
                        .map(|group| GroupId::from(StrBytes::from_static_str(group)))
                        .collect(),
                ),
            ),
        ),
    )
    .await
    .expect("write DescribeGroups observation");
    let (_, response) = decode_response_kind(
        ApiKey::DescribeGroups,
        0,
        read_response(&mut connection, "DescribeGroups after rejected mutations").await,
    );
    let ResponseKind::DescribeGroups(response) = response else {
        panic!("expected DescribeGroups response");
    };
    assert_eq!(response.groups.len(), 3);
    for (group, expected_id) in response.groups.iter().zip(group_ids) {
        assert_eq!(group.group_id.0.as_str(), expected_id);
        assert_eq!(group.error_code, ResponseError::GroupIdNotFound.code());
        assert!(group.members.is_empty());
    }

    server.shutdown().await;
}

#[tokio::test]
async fn init_producer_id_v0_allocates_distinct_epoch_zero_identities() {
    let dispatcher = test_dispatcher();
    let request = || {
        RequestKind::InitProducerId(InitProducerIdRequest::default().with_transactional_id(None))
    };

    let first = dispatch_kind(&dispatcher, ApiKey::InitProducerId, 0, request()).await;
    let ResponseKind::InitProducerId(first) = first else {
        panic!("expected InitProducerId response")
    };
    assert_eq!(first.error_code, 0);
    assert_eq!(i64::from(first.producer_id), 1);
    assert_eq!(first.producer_epoch, 0);

    let second = dispatch_kind(&dispatcher, ApiKey::InitProducerId, 0, request()).await;
    let ResponseKind::InitProducerId(second) = second else {
        panic!("expected InitProducerId response")
    };
    assert_eq!(second.error_code, 0);
    assert_eq!(i64::from(second.producer_id), 2);
    assert_eq!(second.producer_epoch, 0);
}

#[tokio::test]
async fn init_producer_id_v0_rejects_transactional_ids_without_allocating() {
    let dispatcher = test_dispatcher();
    let transactional = dispatch_kind(
        &dispatcher,
        ApiKey::InitProducerId,
        0,
        RequestKind::InitProducerId(InitProducerIdRequest::default().with_transactional_id(Some(
            TransactionalId::from(StrBytes::from_static_str("transactional")),
        ))),
    )
    .await;
    let ResponseKind::InitProducerId(transactional) = transactional else {
        panic!("expected InitProducerId response")
    };
    assert_eq!(
        transactional.error_code,
        ResponseError::UnsupportedForMessageFormat.code()
    );
    assert_eq!(i64::from(transactional.producer_id), -1);
    assert_eq!(transactional.producer_epoch, -1);

    let next = dispatch_kind(
        &dispatcher,
        ApiKey::InitProducerId,
        0,
        RequestKind::InitProducerId(InitProducerIdRequest::default().with_transactional_id(None)),
    )
    .await;
    let ResponseKind::InitProducerId(next) = next else {
        panic!("expected InitProducerId response")
    };
    assert_eq!(i64::from(next.producer_id), 1);
}

#[tokio::test]
async fn list_groups_v0_returns_an_empty_successful_listing() {
    let response = dispatch_kind(
        &test_dispatcher(),
        ApiKey::ListGroups,
        0,
        RequestKind::ListGroups(ListGroupsRequest::default()),
    )
    .await;
    let ResponseKind::ListGroups(response) = response else {
        panic!("expected ListGroups response")
    };

    assert_eq!(response.error_code, 0);
    assert!(response.groups.is_empty());
}

#[tokio::test]
async fn list_groups_v0_returns_existing_group_ids_and_protocol_types() {
    let dispatcher = test_dispatcher();
    let group_id = GroupId::from(StrBytes::from_static_str("listed-group"));
    let join = |member_id: StrBytes| {
        JoinGroupRequest::default()
            .with_group_id(group_id.clone())
            .with_session_timeout_ms(10_000)
            .with_rebalance_timeout_ms(30_000)
            .with_member_id(member_id)
            .with_protocol_type(StrBytes::from_static_str("consumer"))
            .with_protocols(vec![
                JoinGroupRequestProtocol::default()
                    .with_name(StrBytes::from_static_str("cooperative-sticky"))
                    .with_metadata(Bytes::from_static(b"subscription")),
            ])
    };
    let response = dispatch_kind(
        &dispatcher,
        ApiKey::JoinGroup,
        5,
        RequestKind::JoinGroup(join(StrBytes::default())),
    )
    .await;
    let ResponseKind::JoinGroup(response) = response else {
        panic!("expected JoinGroup response")
    };
    let response = dispatch_kind(
        &dispatcher,
        ApiKey::JoinGroup,
        5,
        RequestKind::JoinGroup(join(response.member_id)),
    )
    .await;
    let ResponseKind::JoinGroup(response) = response else {
        panic!("expected JoinGroup response")
    };
    assert_eq!(response.error_code, 0);

    let response = dispatch_kind(
        &dispatcher,
        ApiKey::ListGroups,
        0,
        RequestKind::ListGroups(ListGroupsRequest::default()),
    )
    .await;
    let ResponseKind::ListGroups(response) = response else {
        panic!("expected ListGroups response")
    };

    assert_eq!(response.error_code, 0);
    assert_eq!(response.groups.len(), 1);
    assert_eq!(response.groups[0].group_id.as_str(), "listed-group");
    assert_eq!(response.groups[0].protocol_type.as_str(), "consumer");
}

#[tokio::test]
async fn describe_groups_v0_sorts_members_and_preserves_missing_then_known_request_order() {
    let dispatcher = test_dispatcher();
    let group_id = GroupId::from(StrBytes::from_static_str("described-group"));
    let join = |member_id: StrBytes, metadata: &'static [u8]| {
        JoinGroupRequest::default()
            .with_group_id(group_id.clone())
            .with_session_timeout_ms(10_000)
            .with_rebalance_timeout_ms(30_000)
            .with_member_id(member_id)
            .with_protocol_type(StrBytes::from_static_str("consumer"))
            .with_protocols(vec![
                JoinGroupRequestProtocol::default()
                    .with_name(StrBytes::from_static_str("cooperative-sticky"))
                    .with_metadata(Bytes::from_static(metadata)),
            ])
    };
    let response = dispatch_kind(
        &dispatcher,
        ApiKey::JoinGroup,
        5,
        RequestKind::JoinGroup(join(StrBytes::default(), b"subscription-a")),
    )
    .await;
    let ResponseKind::JoinGroup(response) = response else {
        panic!("expected JoinGroup response")
    };
    let first_member_id = response.member_id;
    assert_eq!(first_member_id.as_str(), "memkafka-wire-test-1");

    let response = dispatch_kind(
        &dispatcher,
        ApiKey::JoinGroup,
        5,
        RequestKind::JoinGroup(join(first_member_id.clone(), b"subscription-a")),
    )
    .await;
    let ResponseKind::JoinGroup(response) = response else {
        panic!("expected JoinGroup response")
    };
    assert_eq!(response.error_code, 0);

    let response = dispatch_kind(
        &dispatcher,
        ApiKey::JoinGroup,
        5,
        RequestKind::JoinGroup(join(StrBytes::default(), b"subscription-b")),
    )
    .await;
    let ResponseKind::JoinGroup(response) = response else {
        panic!("expected JoinGroup response")
    };
    let second_member_id = response.member_id;
    assert_eq!(second_member_id.as_str(), "memkafka-wire-test-2");

    let second_request = join(second_member_id.clone(), b"subscription-b");
    let first_request = join(first_member_id.clone(), b"subscription-a");
    let (second_join, response) = tokio::join!(
        biased;
        dispatch_kind(
            &dispatcher,
            ApiKey::JoinGroup,
            5,
            RequestKind::JoinGroup(second_request),
        ),
        dispatch_kind(
            &dispatcher,
            ApiKey::JoinGroup,
            5,
            RequestKind::JoinGroup(first_request),
        ),
    );
    let ResponseKind::JoinGroup(response) = response else {
        panic!("expected JoinGroup response")
    };
    assert_eq!(response.error_code, 0);
    let ResponseKind::JoinGroup(second_join) = second_join else {
        panic!("expected JoinGroup response")
    };
    assert_eq!(second_join.error_code, 0);

    let response = dispatch_kind(
        &dispatcher,
        ApiKey::SyncGroup,
        3,
        RequestKind::SyncGroup(
            SyncGroupRequest::default()
                .with_group_id(group_id.clone())
                .with_generation_id(response.generation_id)
                .with_member_id(first_member_id.clone())
                .with_assignments(vec![
                    SyncGroupRequestAssignment::default()
                        .with_member_id(first_member_id)
                        .with_assignment(Bytes::from_static(b"assignment-a")),
                    SyncGroupRequestAssignment::default()
                        .with_member_id(second_member_id)
                        .with_assignment(Bytes::from_static(b"assignment-b")),
                ]),
        ),
    )
    .await;
    let ResponseKind::SyncGroup(response) = response else {
        panic!("expected SyncGroup response")
    };
    assert_eq!(response.error_code, 0);

    let response = dispatch_kind(
        &dispatcher,
        ApiKey::DescribeGroups,
        0,
        RequestKind::DescribeGroups(DescribeGroupsRequest::default().with_groups(vec![
            GroupId::from(StrBytes::from_static_str("missing-group")),
            group_id,
        ])),
    )
    .await;
    let ResponseKind::DescribeGroups(response) = response else {
        panic!("expected DescribeGroups response")
    };

    assert_eq!(response.groups.len(), 2);
    assert_eq!(response.groups[0].protocol_data.as_str(), "");
    assert_eq!(
        response.groups[0].error_code,
        ResponseError::GroupIdNotFound.code()
    );
    assert_eq!(response.groups[0].group_id.as_str(), "missing-group");
    assert!(response.groups[0].members.is_empty());

    assert_eq!(response.groups[1].error_code, 0);
    assert_eq!(response.groups[1].group_id.as_str(), "described-group");
    assert_eq!(response.groups[1].group_state.as_str(), "Stable");
    assert_eq!(response.groups[1].protocol_type.as_str(), "consumer");
    assert_eq!(
        response.groups[1].protocol_data.as_str(),
        "cooperative-sticky"
    );
    assert_eq!(response.groups[1].members.len(), 2);
    assert_eq!(
        response.groups[1].members[0].member_id.as_str(),
        "memkafka-wire-test-1"
    );
    assert_eq!(
        response.groups[1].members[0].client_id.as_str(),
        "memkafka-wire-test"
    );
    assert_eq!(
        response.groups[1].members[0].member_metadata,
        Bytes::from_static(b"subscription-a")
    );
    assert_eq!(
        response.groups[1].members[0].member_assignment,
        Bytes::from_static(b"assignment-a")
    );
    assert!(response.groups[1].members[0].client_host.is_empty());
    assert_eq!(
        response.groups[1].members[1].member_id.as_str(),
        "memkafka-wire-test-2"
    );
    assert_eq!(
        response.groups[1].members[1].client_id.as_str(),
        "memkafka-wire-test"
    );
    assert_eq!(
        response.groups[1].members[1].member_metadata,
        Bytes::from_static(b"subscription-b")
    );
    assert_eq!(
        response.groups[1].members[1].member_assignment,
        Bytes::from_static(b"assignment-b")
    );
    assert!(response.groups[1].members[1].client_host.is_empty());
}

#[tokio::test]
async fn describe_configs_v1_returns_read_only_results_for_every_requested_resource() {
    let broker = test_broker_state(false);
    broker
        .topics()
        .create_explicit("events", 1, 1)
        .await
        .expect("create topic");
    let response = dispatch_kind(
        &Dispatcher::new(broker),
        ApiKey::DescribeConfigs,
        1,
        RequestKind::DescribeConfigs(DescribeConfigsRequest::default().with_resources(vec![
                DescribeConfigsResource::default()
                    .with_resource_type(4)
                    .with_resource_name(StrBytes::from_static_str("1")),
                DescribeConfigsResource::default()
                    .with_resource_type(2)
                    .with_resource_name(StrBytes::from_static_str("events")),
            ])),
    )
    .await;
    let ResponseKind::DescribeConfigs(response) = response else {
        panic!("expected DescribeConfigs response")
    };

    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.results.len(), 2);
    assert_eq!(response.results[0].error_code, 0);
    assert_eq!(response.results[0].resource_type, 4);
    assert_eq!(response.results[0].resource_name.as_str(), "1");
    assert!(response.results[0].configs.is_empty());
    assert_eq!(response.results[1].error_code, 0);
    assert_eq!(response.results[1].resource_type, 2);
    assert_eq!(response.results[1].resource_name.as_str(), "events");
    assert!(response.results[1].configs.is_empty());
}

#[tokio::test]
async fn describe_configs_v1_rejects_unknown_or_unsupported_resources() {
    let response = dispatch_kind(
        &test_dispatcher(),
        ApiKey::DescribeConfigs,
        1,
        RequestKind::DescribeConfigs(DescribeConfigsRequest::default().with_resources(vec![
                DescribeConfigsResource::default()
                    .with_resource_type(2)
                    .with_resource_name(StrBytes::from_static_str("missing-topic")),
                DescribeConfigsResource::default()
                    .with_resource_type(4)
                    .with_resource_name(StrBytes::from_static_str("2")),
                DescribeConfigsResource::default()
                    .with_resource_type(8)
                    .with_resource_name(StrBytes::from_static_str("unsupported")),
            ])),
    )
    .await;
    let ResponseKind::DescribeConfigs(response) = response else {
        panic!("expected DescribeConfigs response")
    };

    assert_eq!(
        response.results[0].error_code,
        ResponseError::UnknownTopicOrPartition.code()
    );
    assert_eq!(
        response.results[1].error_code,
        ResponseError::BrokerNotAvailable.code()
    );
    assert_eq!(
        response.results[2].error_code,
        ResponseError::InvalidRequest.code()
    );
}

#[tokio::test]
async fn classic_group_requests_complete_membership_and_offset_lifecycle() {
    let dispatcher = test_dispatcher();
    let group_id = GroupId::from(StrBytes::from_static_str("orders-group"));

    let response = dispatch_kind(
        &dispatcher,
        ApiKey::FindCoordinator,
        2,
        RequestKind::FindCoordinator(
            FindCoordinatorRequest::default()
                .with_key(StrBytes::from_static_str("orders-group"))
                .with_key_type(0),
        ),
    )
    .await;
    let ResponseKind::FindCoordinator(response) = response else {
        panic!("expected FindCoordinator response")
    };
    assert_eq!(response.error_code, 0);
    assert_eq!(response.node_id, 1);
    assert_eq!(response.host.as_str(), "127.0.0.1");
    assert_eq!(response.port, 9092);

    let join = |member_id: StrBytes| {
        JoinGroupRequest::default()
            .with_group_id(group_id.clone())
            .with_session_timeout_ms(10_000)
            .with_rebalance_timeout_ms(30_000)
            .with_member_id(member_id)
            .with_protocol_type(StrBytes::from_static_str("consumer"))
            .with_protocols(vec![
                JoinGroupRequestProtocol::default()
                    .with_name(StrBytes::from_static_str("cooperative-sticky"))
                    .with_metadata(Bytes::from_static(b"subscription")),
            ])
    };
    let response = dispatch_kind(
        &dispatcher,
        ApiKey::JoinGroup,
        5,
        RequestKind::JoinGroup(join(StrBytes::default())),
    )
    .await;
    let ResponseKind::JoinGroup(response) = response else {
        panic!("expected JoinGroup response")
    };
    assert_eq!(response.error_code, 79);
    assert!(!response.member_id.is_empty());
    let member_id = response.member_id;

    let response = dispatch_kind(
        &dispatcher,
        ApiKey::JoinGroup,
        5,
        RequestKind::JoinGroup(join(member_id.clone())),
    )
    .await;
    let ResponseKind::JoinGroup(response) = response else {
        panic!("expected JoinGroup response")
    };
    assert_eq!(response.error_code, 0);
    assert_eq!(response.generation_id, 1);
    assert_eq!(response.leader, member_id);
    assert_eq!(response.members.len(), 1);
    let generation_id = response.generation_id;

    let response = dispatch_kind(
        &dispatcher,
        ApiKey::Heartbeat,
        3,
        RequestKind::Heartbeat(
            HeartbeatRequest::default()
                .with_group_id(group_id.clone())
                .with_generation_id(generation_id)
                .with_member_id(member_id.clone()),
        ),
    )
    .await;
    let ResponseKind::Heartbeat(response) = response else {
        panic!("expected Heartbeat response")
    };
    assert_eq!(response.error_code, 0);

    let response = dispatch_kind(
        &dispatcher,
        ApiKey::SyncGroup,
        3,
        RequestKind::SyncGroup(
            SyncGroupRequest::default()
                .with_group_id(group_id.clone())
                .with_generation_id(generation_id)
                .with_member_id(member_id.clone()),
        ),
    )
    .await;
    let ResponseKind::SyncGroup(response) = response else {
        panic!("expected SyncGroup response")
    };
    assert_eq!(response.error_code, ResponseError::InvalidRequest.code());

    let assignment = Bytes::from_static(b"assignment");
    let response = dispatch_kind(
        &dispatcher,
        ApiKey::SyncGroup,
        3,
        RequestKind::SyncGroup(
            SyncGroupRequest::default()
                .with_group_id(group_id.clone())
                .with_generation_id(generation_id)
                .with_member_id(member_id.clone())
                .with_assignments(vec![
                    SyncGroupRequestAssignment::default()
                        .with_member_id(member_id.clone())
                        .with_assignment(assignment.clone()),
                ]),
        ),
    )
    .await;
    let ResponseKind::SyncGroup(response) = response else {
        panic!("expected SyncGroup response")
    };
    assert_eq!(response.error_code, 0);
    assert_eq!(response.assignment, assignment);

    let response = dispatch_kind(
        &dispatcher,
        ApiKey::Heartbeat,
        3,
        RequestKind::Heartbeat(
            HeartbeatRequest::default()
                .with_group_id(group_id.clone())
                .with_generation_id(generation_id)
                .with_member_id(member_id.clone()),
        ),
    )
    .await;
    let ResponseKind::Heartbeat(response) = response else {
        panic!("expected Heartbeat response")
    };
    assert_eq!(response.error_code, 0);

    for (heartbeat_member, heartbeat_generation, expected_error) in [
        (
            member_id.clone(),
            generation_id + 1,
            ResponseError::IllegalGeneration,
        ),
        (
            StrBytes::from_static_str("missing-member"),
            generation_id,
            ResponseError::UnknownMemberId,
        ),
    ] {
        let response = dispatch_kind(
            &dispatcher,
            ApiKey::Heartbeat,
            3,
            RequestKind::Heartbeat(
                HeartbeatRequest::default()
                    .with_group_id(group_id.clone())
                    .with_generation_id(heartbeat_generation)
                    .with_member_id(heartbeat_member),
            ),
        )
        .await;
        let ResponseKind::Heartbeat(response) = response else {
            panic!("expected Heartbeat response")
        };
        assert_eq!(response.error_code, expected_error.code());
    }

    let response = dispatch_kind(
        &dispatcher,
        ApiKey::OffsetCommit,
        7,
        RequestKind::OffsetCommit(
            OffsetCommitRequest::default()
                .with_group_id(group_id.clone())
                .with_generation_id_or_member_epoch(generation_id)
                .with_member_id(member_id.clone())
                .with_topics(vec![
                    OffsetCommitRequestTopic::default()
                        .with_name(TopicName::from(StrBytes::from_static_str("events")))
                        .with_partitions(vec![
                            OffsetCommitRequestPartition::default()
                                .with_partition_index(0)
                                .with_committed_offset(4),
                        ]),
                ]),
        ),
    )
    .await;
    let ResponseKind::OffsetCommit(response) = response else {
        panic!("expected OffsetCommit response")
    };
    assert_eq!(response.topics[0].partitions[0].error_code, 0);

    let offset_fetch = || {
        OffsetFetchRequest::default()
            .with_group_id(group_id.clone())
            .with_topics(Some(vec![
                OffsetFetchRequestTopic::default()
                    .with_name(TopicName::from(StrBytes::from_static_str("events")))
                    .with_partition_indexes(vec![0]),
            ]))
    };
    let response = dispatch_kind(
        &dispatcher,
        ApiKey::OffsetFetch,
        5,
        RequestKind::OffsetFetch(offset_fetch()),
    )
    .await;
    let ResponseKind::OffsetFetch(response) = response else {
        panic!("expected OffsetFetch response")
    };
    assert_eq!(response.error_code, 0);
    assert_eq!(response.topics[0].partitions[0].committed_offset, 4);

    let response = dispatch_kind(
        &dispatcher,
        ApiKey::LeaveGroup,
        3,
        RequestKind::LeaveGroup(
            LeaveGroupRequest::default()
                .with_group_id(group_id.clone())
                .with_members(vec![
                    MemberIdentity::default().with_member_id(member_id.clone()),
                ]),
        ),
    )
    .await;
    let ResponseKind::LeaveGroup(response) = response else {
        panic!("expected LeaveGroup response")
    };
    assert_eq!(response.error_code, 0);
    assert_eq!(response.members[0].error_code, 0);

    let response = dispatch_kind(
        &dispatcher,
        ApiKey::OffsetFetch,
        5,
        RequestKind::OffsetFetch(offset_fetch()),
    )
    .await;
    let ResponseKind::OffsetFetch(response) = response else {
        panic!("expected OffsetFetch response")
    };
    assert_eq!(response.topics[0].partitions[0].committed_offset, 4);
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
async fn metadata_force_overrides_only_the_named_request_opt_out() {
    let forced = test_broker_state_with_force(true, true);
    let response = dispatch_metadata_request(
        &Dispatcher::new(forced.clone()),
        104,
        Some(vec!["forced-topic"]),
        false,
    )
    .await;
    assert_eq!(response.topics[0].error_code, 0);
    assert_eq!(response.topics[0].partitions.len(), 2);

    let server_disabled = test_broker_state_with_force(false, true);
    let response = dispatch_metadata_request(
        &Dispatcher::new(server_disabled.clone()),
        105,
        Some(vec!["still-disabled"]),
        false,
    )
    .await;
    assert_eq!(response.topics[0].error_code, 3);
    assert!(server_disabled.topics().list().await.is_empty());

    let before = forced.topics().list().await.len();
    let response =
        dispatch_metadata_request(&Dispatcher::new(forced.clone()), 106, None, false).await;
    assert_eq!(response.topics.len(), before);
    assert_eq!(forced.topics().list().await.len(), before);
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

#[tokio::test]
async fn produce_v7_appends_batches_and_returns_contiguous_base_offsets() {
    let broker = test_broker_state(true);
    let dispatcher = Dispatcher::new(broker.clone());

    let first = dispatch_produce_request(
        &dispatcher,
        131,
        -1,
        None,
        vec![produce_topic(
            "events",
            vec![produce_partition(0, record_batch(&["first", "second"]))],
        )],
    )
    .await;
    let second = dispatch_produce_request(
        &dispatcher,
        132,
        1,
        None,
        vec![produce_topic(
            "events",
            vec![produce_partition(0, record_batch(&["third"]))],
        )],
    )
    .await;

    assert_eq!(first.responses[0].partition_responses[0].error_code, 0);
    assert_eq!(first.responses[0].partition_responses[0].base_offset, 0);
    assert_eq!(second.responses[0].partition_responses[0].error_code, 0);
    assert_eq!(second.responses[0].partition_responses[0].base_offset, 2);
    assert_eq!(broker.topics().list().await[0].partition_count, 2);
}

#[tokio::test]
async fn idempotent_produce_validates_identity_and_preserves_log_on_rejections() {
    let broker = test_broker_state(false);
    broker
        .topics()
        .create_explicit("events", 1, 1)
        .await
        .expect("create topic");
    let dispatcher = Dispatcher::new(broker.clone());
    let ResponseKind::InitProducerId(identity) = dispatch_kind(
        &dispatcher,
        ApiKey::InitProducerId,
        0,
        RequestKind::InitProducerId(InitProducerIdRequest::default().with_transactional_id(None)),
    )
    .await
    else {
        panic!("expected InitProducerId response")
    };
    assert_eq!(identity.error_code, 0);
    assert_eq!(i64::from(identity.producer_id), 1);
    assert_eq!(identity.producer_epoch, 0);

    let initial = idempotent_record_batch(1, 0, 0, &["first"]);
    let first = dispatch_produce_request(
        &dispatcher,
        132,
        -1,
        None,
        vec![produce_topic(
            "events",
            vec![produce_partition(0, initial.clone())],
        )],
    )
    .await;
    assert_eq!(first.responses[0].partition_responses[0].error_code, 0);
    assert_eq!(first.responses[0].partition_responses[0].base_offset, 0);

    let retry = dispatch_produce_request(
        &dispatcher,
        133,
        -1,
        None,
        vec![produce_topic("events", vec![produce_partition(0, initial)])],
    )
    .await;
    assert_eq!(retry.responses[0].partition_responses[0].error_code, 0);
    assert_eq!(retry.responses[0].partition_responses[0].base_offset, 0);
    assert_eq!(partition_state(&dispatcher, 1330).await.0, 1);

    assert_partition_state_unchanged_after_rejection(
        &dispatcher,
        134,
        idempotent_record_batch(99, 0, 0, &["unknown-id"]),
        ResponseError::UnknownProducerId,
    )
    .await;
    assert_partition_state_unchanged_after_rejection(
        &dispatcher,
        135,
        idempotent_record_batch(1, 1, 1, &["wrong-epoch"]),
        ResponseError::InvalidProducerEpoch,
    )
    .await;
    assert_partition_state_unchanged_after_rejection(
        &dispatcher,
        136,
        idempotent_record_batch(1, 0, 2, &["gap"]),
        ResponseError::OutOfOrderSequenceNumber,
    )
    .await;

    for sequence in 1..=5 {
        let response = dispatch_produce_request(
            &dispatcher,
            140 + sequence,
            -1,
            None,
            vec![produce_topic(
                "events",
                vec![produce_partition(
                    0,
                    idempotent_record_batch(1, 0, sequence, &["later"]),
                )],
            )],
        )
        .await;
        assert_eq!(response.responses[0].partition_responses[0].error_code, 0);
    }
    assert_partition_state_unchanged_after_rejection(
        &dispatcher,
        146,
        idempotent_record_batch(1, 0, 0, &["first"]),
        ResponseError::DuplicateSequenceNumber,
    )
    .await;
}

#[tokio::test]
async fn produce_v7_maps_request_and_partition_errors_without_partial_corruption() {
    let broker = test_broker_state(false);
    broker
        .topics()
        .create_explicit("events", 1, 1)
        .await
        .expect("create topic");
    let dispatcher = Dispatcher::new(broker);

    let invalid_acks = dispatch_produce_request(
        &dispatcher,
        133,
        2,
        None,
        vec![produce_topic(
            "events",
            vec![produce_partition(0, record_batch(&["not-appended"]))],
        )],
    )
    .await;
    assert_eq!(
        invalid_acks.responses[0].partition_responses[0].error_code,
        21
    );

    let transactional = dispatch_produce_request(
        &dispatcher,
        134,
        -1,
        Some(TransactionalId::from(StrBytes::from_static_str("tx"))),
        vec![produce_topic(
            "events",
            vec![produce_partition(0, record_batch(&["not-appended"]))],
        )],
    )
    .await;
    assert_eq!(
        transactional.responses[0].partition_responses[0].error_code,
        43
    );

    let mixed = dispatch_produce_request(
        &dispatcher,
        135,
        -1,
        None,
        vec![
            produce_topic(
                "events",
                vec![
                    produce_partition(0, record_batch(&["appended"])),
                    produce_partition(9, record_batch(&["unknown-partition"])),
                ],
            ),
            produce_topic(
                "missing",
                vec![produce_partition(0, record_batch(&["unknown-topic"]))],
            ),
            produce_topic(
                "events",
                vec![produce_partition(0, Bytes::from_static(b"malformed"))],
            ),
        ],
    )
    .await;

    assert_eq!(mixed.responses[0].partition_responses[0].error_code, 0);
    assert_eq!(mixed.responses[0].partition_responses[0].base_offset, 0);
    assert_eq!(mixed.responses[0].partition_responses[1].error_code, 3);
    assert_eq!(mixed.responses[1].partition_responses[0].error_code, 3);
    assert_eq!(mixed.responses[2].partition_responses[0].error_code, 2);

    let after_errors = dispatch_produce_request(
        &dispatcher,
        136,
        1,
        None,
        vec![produce_topic(
            "events",
            vec![produce_partition(0, record_batch(&["after-errors"]))],
        )],
    )
    .await;
    assert_eq!(
        after_errors.responses[0].partition_responses[0].base_offset,
        1
    );
}

#[tokio::test]
async fn acks_zero_appends_without_writing_a_produce_response() {
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
        &encode_produce_request(
            140,
            0,
            None,
            vec![produce_topic(
                "fire-and-forget",
                vec![produce_partition(0, record_batch(&["stored"]))],
            )],
        ),
    )
    .await
    .expect("write acks=0 Produce frame");
    write_frame(&mut connection, &encode_api_versions_request(141))
        .await
        .expect("write ApiVersions frame");

    let response = timeout(Duration::from_secs(1), read_frame(&mut connection))
        .await
        .expect("ApiVersions response timed out")
        .expect("read Kafka response")
        .expect("server closed before ApiVersions response");
    let (header, _) = decode_api_versions_response(response);
    assert_eq!(header.correlation_id, 141);

    write_frame(
        &mut connection,
        &encode_fetch_request(
            142,
            0,
            1,
            i32::MAX,
            vec![fetch_topic("fire-and-forget", vec![(0, 0, i32::MAX)])],
        ),
    )
    .await
    .expect("write Fetch frame");
    let response = timeout(Duration::from_secs(1), read_frame(&mut connection))
        .await
        .expect("Fetch response timed out")
        .expect("read Fetch response")
        .expect("server closed before Fetch response");
    let (header, response) = decode_fetch_response(response);
    assert_eq!(header.correlation_id, 142);
    let records = decode_records(
        response.responses[0].partitions[0]
            .records
            .clone()
            .expect("record bytes"),
    );
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].value, Some(Bytes::from_static(b"stored")));

    shutdown_tx.send(()).expect("request server shutdown");
    timeout(Duration::from_secs(1), server)
        .await
        .expect("server shutdown timed out")
        .expect("server task panicked")
        .expect("server returned an error");
}

#[tokio::test]
async fn list_offsets_v3_reports_earliest_latest_and_partition_errors() {
    let broker = test_broker_state(true);
    let dispatcher = Dispatcher::new(broker);
    let produced = dispatch_produce_request(
        &dispatcher,
        150,
        -1,
        None,
        vec![produce_topic(
            "events",
            vec![produce_partition(0, record_batch(&["zero", "one", "two"]))],
        )],
    )
    .await;
    assert_eq!(produced.responses[0].partition_responses[0].error_code, 0);

    let response = dispatch_list_offsets_request(
        &dispatcher,
        151,
        vec![
            list_offsets_topic("events", vec![(0, -2), (0, -1), (0, 1234), (9, -1)]),
            list_offsets_topic("missing", vec![(0, -1)]),
        ],
    )
    .await;
    let events = &response.topics[0].partitions;

    assert_eq!((events[0].error_code, events[0].offset), (0, 0));
    assert_eq!((events[1].error_code, events[1].offset), (0, 3));
    assert_eq!((events[2].error_code, events[2].offset), (43, -1));
    assert_eq!((events[3].error_code, events[3].offset), (3, -1));
    assert_eq!(response.topics[1].partitions[0].error_code, 3);
    assert_eq!(response.topics[1].partitions[0].offset, -1);
}

#[tokio::test]
async fn fetch_v4_returns_ordered_records_watermarks_and_partition_errors() {
    let broker = test_broker_state(true);
    let dispatcher = Dispatcher::new(broker);
    dispatch_produce_request(
        &dispatcher,
        160,
        -1,
        None,
        vec![produce_topic(
            "events",
            vec![produce_partition(0, record_batch(&["zero", "one", "two"]))],
        )],
    )
    .await;

    let response = dispatch_fetch_request(
        &dispatcher,
        161,
        0,
        1,
        i32::MAX,
        vec![
            fetch_topic("events", vec![(0, 0, i32::MAX), (1, -1, i32::MAX)]),
            fetch_topic("missing", vec![(0, 0, i32::MAX)]),
        ],
    )
    .await;
    let events = &response.responses[0].partitions;
    assert_eq!(events[0].error_code, 0);
    assert_eq!(events[0].high_watermark, 3);
    assert_eq!(events[0].last_stable_offset, 3);
    assert_eq!(events[0].aborted_transactions, Some(Vec::new()));

    let records = decode_records(events[0].records.clone().expect("record bytes"));
    assert_eq!(
        records
            .iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(
        records
            .iter()
            .map(
                |record| String::from_utf8_lossy(record.value.as_ref().expect("value"))
                    .into_owned()
            )
            .collect::<Vec<_>>(),
        vec!["zero", "one", "two"]
    );
    assert_eq!(records[0].key, Some(Bytes::from_static(b"key-0")));
    assert_eq!(records[0].timestamp, 1_700_000_000_000);
    assert_eq!(
        records[0].headers.get(&StrBytes::from_static_str("source")),
        Some(&Some(Bytes::from_static(b"wire-test")))
    );
    assert_eq!(events[1].error_code, 1);
    assert_eq!(response.responses[1].partitions[0].error_code, 3);

    let at_end = dispatch_fetch_request(
        &dispatcher,
        162,
        0,
        1,
        i32::MAX,
        vec![fetch_topic("events", vec![(0, 3, i32::MAX)])],
    )
    .await;
    let at_end = &at_end.responses[0].partitions[0];
    assert_eq!(at_end.error_code, 0);
    assert_eq!(at_end.high_watermark, 3);
    assert!(at_end.records.as_ref().is_none_or(Bytes::is_empty));

    let past_end = dispatch_fetch_request(
        &dispatcher,
        163,
        0,
        1,
        i32::MAX,
        vec![fetch_topic("events", vec![(0, 4, i32::MAX)])],
    )
    .await;
    assert_eq!(past_end.responses[0].partitions[0].error_code, 1);
}

#[tokio::test]
async fn fetch_v4_honors_byte_limits_but_returns_the_first_oversized_batch() {
    let broker = test_broker_state(false);
    broker
        .topics()
        .create_explicit("events", 2, 1)
        .await
        .expect("create topic");
    let dispatcher = Dispatcher::new(broker);
    let first = record_batch(&["first"]);
    let first_len = i32::try_from(first.len()).expect("small batch");
    dispatch_produce_request(
        &dispatcher,
        168,
        -1,
        None,
        vec![produce_topic(
            "events",
            vec![
                produce_partition(0, first),
                produce_partition(1, record_batch(&["other-partition"])),
            ],
        )],
    )
    .await;
    dispatch_produce_request(
        &dispatcher,
        169,
        -1,
        None,
        vec![produce_topic(
            "events",
            vec![produce_partition(0, record_batch(&["second"]))],
        )],
    )
    .await;

    let partition_limited = dispatch_fetch_request(
        &dispatcher,
        170,
        0,
        1,
        i32::MAX,
        vec![fetch_topic("events", vec![(0, 0, first_len)])],
    )
    .await;
    assert_eq!(
        decode_records(
            partition_limited.responses[0].partitions[0]
                .records
                .clone()
                .expect("record bytes")
        )
        .len(),
        1
    );

    let request_limited = dispatch_fetch_request(
        &dispatcher,
        171,
        0,
        1,
        1,
        vec![fetch_topic(
            "events",
            vec![(0, 0, i32::MAX), (1, 0, i32::MAX)],
        )],
    )
    .await;
    assert_eq!(
        decode_records(
            request_limited.responses[0].partitions[0]
                .records
                .clone()
                .expect("first oversized batch")
        )
        .len(),
        1
    );
    assert!(
        request_limited.responses[0].partitions[1]
            .records
            .as_ref()
            .is_none_or(Bytes::is_empty)
    );
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

struct SpawnedServer {
    kafka: std::net::SocketAddr,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<anyhow::Result<()>>,
}

impl SpawnedServer {
    async fn start(config: Config) -> Self {
        let (ready_tx, ready_rx) = oneshot::channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let mut task = tokio::spawn(serve(config, ready_tx, async {
            let _ = shutdown_rx.await;
        }));
        let endpoints = match timeout(Duration::from_secs(1), ready_rx).await {
            Ok(Ok(endpoints)) => endpoints,
            ready_result => {
                let server_result = timeout(Duration::from_secs(1), &mut task).await;
                panic!(
                    "server did not become ready: ready={ready_result:?}, server={server_result:?}"
                );
            }
        };
        Self {
            kafka: endpoints.kafka,
            shutdown: shutdown_tx,
            task,
        }
    }

    async fn connect(&self) -> TcpStream {
        TcpStream::connect(self.kafka)
            .await
            .expect("connect to Kafka endpoint")
    }

    async fn shutdown(self) {
        self.shutdown.send(()).expect("request server shutdown");
        timeout(Duration::from_secs(1), self.task)
            .await
            .expect("server shutdown timed out")
            .expect("server task panicked")
            .expect("server returned an error");
    }
}

async fn read_response(connection: &mut TcpStream, context: &str) -> Bytes {
    timeout(Duration::from_secs(1), read_frame(connection))
        .await
        .unwrap_or_else(|_| panic!("{context} response timed out"))
        .unwrap_or_else(|error| panic!("read {context} response: {error}"))
        .unwrap_or_else(|| panic!("connection closed before {context} response"))
}

fn test_dispatcher() -> Dispatcher {
    Dispatcher::new(test_broker_state(true))
}

async fn dispatch_kind(
    dispatcher: &Dispatcher,
    api_key: ApiKey,
    version: i16,
    body: RequestKind,
) -> ResponseKind {
    let header = RequestHeader::default()
        .with_request_api_key(api_key as i16)
        .with_request_api_version(version)
        .with_correlation_id(1)
        .with_client_id(Some(StrBytes::from_static_str("memkafka-wire-test")));
    let mut encoded_request = BytesMut::new();
    encode_request_header_into_buffer(&mut encoded_request, &header)
        .expect("encode request header");
    body.encode(&mut encoded_request, version)
        .expect("encode request body");
    let mut encoded_response = exchange_with_dispatcher(dispatcher, encoded_request.freeze()).await;
    let response_header = ResponseHeader::decode(
        &mut encoded_response,
        api_key.response_header_version(version),
    )
    .expect("decode response header");
    assert_eq!(response_header.correlation_id, 1);
    ResponseKind::decode(api_key, &mut encoded_response, version).expect("decode response body")
}

async fn exchange_with_dispatcher(dispatcher: &Dispatcher, request: Bytes) -> Bytes {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback Kafka test listener");
    let address = listener
        .local_addr()
        .expect("read loopback listener address");
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let dispatcher = dispatcher.clone();
    let server = tokio::spawn(async move {
        let (socket, _) = listener
            .accept()
            .await
            .expect("accept loopback Kafka client");
        connection::serve(socket, dispatcher, shutdown_rx).await
    });
    let mut client = TcpStream::connect(address)
        .await
        .expect("connect loopback Kafka test client");

    write_frame(&mut client, &request)
        .await
        .expect("write loopback Kafka request");
    let response = read_frame(&mut client)
        .await
        .expect("read loopback Kafka response")
        .expect("loopback Kafka connection closed before its response");

    shutdown_tx
        .send(true)
        .expect("stop loopback Kafka connection");
    timeout(Duration::from_secs(1), server)
        .await
        .expect("loopback Kafka task did not stop")
        .expect("loopback Kafka task panicked")
        .expect("loopback Kafka connection failed");
    response
}

fn test_broker_state(auto_create_topics: bool) -> BrokerState {
    test_broker_state_with_force(auto_create_topics, false)
}

fn test_broker_state_with_force(
    auto_create_topics: bool,
    force_auto_create_topics: bool,
) -> BrokerState {
    BrokerState::new(
        1,
        AdvertisedAddress::new("127.0.0.1", 9092).expect("valid test address"),
        auto_create_topics,
        force_auto_create_topics,
        NonZeroU32::new(2).expect("nonzero literal"),
    )
}

fn encode_api_versions_request(correlation_id: i32) -> Bytes {
    encode_api_versions_request_for(correlation_id, API_VERSIONS_VERSION)
}

fn encode_api_versions_request_for(correlation_id: i32, version: i16) -> Bytes {
    let header = RequestHeader::default()
        .with_request_api_key(ApiKey::ApiVersions as i16)
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_static_str("memkafka-wire-test")));
    let request = ApiVersionsRequest::default()
        .with_client_software_name(StrBytes::from_static_str("memkafka-test"))
        .with_client_software_version(StrBytes::from_static_str("1.0"));
    let mut encoded = BytesMut::new();

    encode_request_header_into_buffer(&mut encoded, &header).expect("encode request header");
    RequestKind::ApiVersions(request)
        .encode(&mut encoded, version)
        .expect("encode request body");

    encoded.freeze()
}

fn encode_request_kind(
    api_key: ApiKey,
    version: i16,
    correlation_id: i32,
    request: RequestKind,
) -> Bytes {
    let header = RequestHeader::default()
        .with_request_api_key(api_key as i16)
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_static_str("memkafka-wire-test")));
    let mut encoded = BytesMut::new();
    encode_request_header_into_buffer(&mut encoded, &header).expect("encode request header");
    request
        .encode(&mut encoded, version)
        .unwrap_or_else(|error| panic!("encode {api_key:?} v{version} request: {error}"));
    encoded.freeze()
}

fn encode_request_header_only(api_key: ApiKey, version: i16, correlation_id: i32) -> Bytes {
    let header = RequestHeader::default()
        .with_request_api_key(api_key as i16)
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_static_str("memkafka-wire-test")));
    let mut encoded = BytesMut::new();
    encode_request_header_into_buffer(&mut encoded, &header).expect("encode request header");
    encoded.freeze()
}

fn raw_request_prefix(raw_api_key: i16, version: i16, correlation_id: i32) -> Bytes {
    let mut encoded = BytesMut::with_capacity(8);
    encoded.put_i16(raw_api_key);
    encoded.put_i16(version);
    encoded.put_i32(correlation_id);
    encoded.freeze()
}

fn decode_response_kind(
    api_key: ApiKey,
    version: i16,
    mut encoded: Bytes,
) -> (ResponseHeader, ResponseKind) {
    let header = ResponseHeader::decode(&mut encoded, api_key.response_header_version(version))
        .unwrap_or_else(|error| panic!("decode {api_key:?} v{version} response header: {error}"));
    let response = ResponseKind::decode(api_key, &mut encoded, version)
        .unwrap_or_else(|error| panic!("decode {api_key:?} v{version} response body: {error}"));
    assert!(
        encoded.is_empty(),
        "{api_key:?} v{version} response has trailing bytes"
    );
    (header, response)
}

async fn assert_socket_closed(connection: &mut TcpStream, case: &str) {
    let mut byte = [0_u8; 1];
    let read = timeout(Duration::from_secs(1), connection.read(&mut byte))
        .await
        .unwrap_or_else(|_| panic!("offending connection stayed open for {case}"))
        .unwrap_or_else(|error| panic!("read offending connection after {case}: {error}"));
    assert_eq!(read, 0, "offending connection returned bytes for {case}");
}

fn topic_name(value: &'static str) -> TopicName {
    TopicName::from(StrBytes::from_static_str(value))
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
    let encoded = exchange_with_dispatcher(
        dispatcher,
        encode_metadata_topics(correlation_id, topics, allow_auto_topic_creation),
    )
    .await;
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

fn encode_produce_request(
    correlation_id: i32,
    acks: i16,
    transactional_id: Option<TransactionalId>,
    topics: Vec<TopicProduceData>,
) -> Bytes {
    const VERSION: i16 = 7;
    let header = RequestHeader::default()
        .with_request_api_key(ApiKey::Produce as i16)
        .with_request_api_version(VERSION)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_static_str("memkafka-wire-test")));
    let request = ProduceRequest::default()
        .with_transactional_id(transactional_id)
        .with_acks(acks)
        .with_timeout_ms(1_000)
        .with_topic_data(topics);
    let mut encoded = BytesMut::new();

    encode_request_header_into_buffer(&mut encoded, &header).expect("encode request header");
    RequestKind::Produce(request)
        .encode(&mut encoded, VERSION)
        .expect("encode Produce body");
    encoded.freeze()
}

async fn dispatch_produce_request(
    dispatcher: &Dispatcher,
    correlation_id: i32,
    acks: i16,
    transactional_id: Option<TransactionalId>,
    topics: Vec<TopicProduceData>,
) -> ProduceResponse {
    let encoded = exchange_with_dispatcher(
        dispatcher,
        encode_produce_request(correlation_id, acks, transactional_id, topics),
    )
    .await;
    let (header, response) = decode_produce_response(encoded);
    assert_eq!(header.correlation_id, correlation_id);
    response
}

async fn assert_partition_state_unchanged_after_rejection(
    dispatcher: &Dispatcher,
    correlation_id: i32,
    records: Bytes,
    expected_error: ResponseError,
) {
    let before = partition_state(dispatcher, correlation_id * 10).await;
    let response = dispatch_produce_request(
        dispatcher,
        correlation_id,
        -1,
        None,
        vec![produce_topic("events", vec![produce_partition(0, records)])],
    )
    .await;

    assert_partition_error(response, expected_error);
    assert_eq!(
        partition_state(dispatcher, correlation_id * 10 + 2).await,
        before,
        "rejected Produce must not change the latest offset or fetched bytes"
    );
}

async fn partition_state(dispatcher: &Dispatcher, correlation_id: i32) -> (i64, Option<Bytes>) {
    let latest = dispatch_list_offsets_request(
        dispatcher,
        correlation_id,
        vec![list_offsets_topic("events", vec![(0, -1)])],
    )
    .await;
    let latest = latest.topics[0].partitions[0].offset;
    let fetched = dispatch_fetch_request(
        dispatcher,
        correlation_id + 1,
        0,
        1,
        i32::MAX,
        vec![fetch_topic("events", vec![(0, 0, i32::MAX)])],
    )
    .await;

    (latest, fetched.responses[0].partitions[0].records.clone())
}

fn assert_partition_error(response: ProduceResponse, expected_error: ResponseError) {
    let partition = &response.responses[0].partition_responses[0];
    assert_eq!(partition.error_code, expected_error.code());
    assert_eq!(partition.base_offset, -1);
}

fn encode_list_offsets_request(correlation_id: i32, topics: Vec<ListOffsetsTopic>) -> Bytes {
    const VERSION: i16 = 3;
    let header = RequestHeader::default()
        .with_request_api_key(ApiKey::ListOffsets as i16)
        .with_request_api_version(VERSION)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_static_str("memkafka-wire-test")));
    let request = ListOffsetsRequest::default()
        .with_replica_id(BrokerId::from(-1))
        .with_isolation_level(0)
        .with_topics(topics);
    let mut encoded = BytesMut::new();

    encode_request_header_into_buffer(&mut encoded, &header).expect("encode request header");
    RequestKind::ListOffsets(request)
        .encode(&mut encoded, VERSION)
        .expect("encode ListOffsets body");
    encoded.freeze()
}

async fn dispatch_list_offsets_request(
    dispatcher: &Dispatcher,
    correlation_id: i32,
    topics: Vec<ListOffsetsTopic>,
) -> ListOffsetsResponse {
    let encoded = exchange_with_dispatcher(
        dispatcher,
        encode_list_offsets_request(correlation_id, topics),
    )
    .await;
    let (header, response) = decode_list_offsets_response(encoded);
    assert_eq!(header.correlation_id, correlation_id);
    response
}

fn encode_fetch_request(
    correlation_id: i32,
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    topics: Vec<FetchTopic>,
) -> Bytes {
    const VERSION: i16 = 4;
    let header = RequestHeader::default()
        .with_request_api_key(ApiKey::Fetch as i16)
        .with_request_api_version(VERSION)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_static_str("memkafka-wire-test")));
    let request = FetchRequest::default()
        .with_replica_id(BrokerId::from(-1))
        .with_max_wait_ms(max_wait_ms)
        .with_min_bytes(min_bytes)
        .with_max_bytes(max_bytes)
        .with_isolation_level(0)
        .with_topics(topics);
    let mut encoded = BytesMut::new();

    encode_request_header_into_buffer(&mut encoded, &header).expect("encode request header");
    RequestKind::Fetch(request)
        .encode(&mut encoded, VERSION)
        .expect("encode Fetch body");
    encoded.freeze()
}

async fn dispatch_fetch_request(
    dispatcher: &Dispatcher,
    correlation_id: i32,
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    topics: Vec<FetchTopic>,
) -> FetchResponse {
    let encoded = exchange_with_dispatcher(
        dispatcher,
        encode_fetch_request(correlation_id, max_wait_ms, min_bytes, max_bytes, topics),
    )
    .await;
    let (header, response) = decode_fetch_response(encoded);
    assert_eq!(header.correlation_id, correlation_id);
    response
}

fn produce_topic(name: &'static str, partitions: Vec<PartitionProduceData>) -> TopicProduceData {
    TopicProduceData::default()
        .with_name(TopicName::from(StrBytes::from_static_str(name)))
        .with_partition_data(partitions)
}

fn produce_partition(index: i32, records: Bytes) -> PartitionProduceData {
    PartitionProduceData::default()
        .with_index(index)
        .with_records(Some(records))
}

fn list_offsets_topic(name: &'static str, partitions: Vec<(i32, i64)>) -> ListOffsetsTopic {
    ListOffsetsTopic::default()
        .with_name(TopicName::from(StrBytes::from_static_str(name)))
        .with_partitions(
            partitions
                .into_iter()
                .map(|(partition_index, timestamp)| {
                    ListOffsetsPartition::default()
                        .with_partition_index(partition_index)
                        .with_timestamp(timestamp)
                })
                .collect(),
        )
}

fn fetch_topic(name: &'static str, partitions: Vec<(i32, i64, i32)>) -> FetchTopic {
    FetchTopic::default()
        .with_topic(TopicName::from(StrBytes::from_static_str(name)))
        .with_partitions(
            partitions
                .into_iter()
                .map(|(partition, fetch_offset, partition_max_bytes)| {
                    FetchPartition::default()
                        .with_partition(partition)
                        .with_fetch_offset(fetch_offset)
                        .with_partition_max_bytes(partition_max_bytes)
                })
                .collect(),
        )
}

fn record_batch(values: &[&str]) -> Bytes {
    record_batch_with_producer(values, NO_PRODUCER_ID, NO_PRODUCER_EPOCH, 0)
}

fn idempotent_record_batch(
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    values: &[&str],
) -> Bytes {
    record_batch_with_producer(values, producer_id, producer_epoch, base_sequence)
}

fn record_batch_with_producer(
    values: &[&str],
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
) -> Bytes {
    let records = values
        .iter()
        .enumerate()
        .map(|(offset, value)| Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id,
            producer_epoch,
            timestamp_type: TimestampType::Creation,
            offset: offset as i64,
            sequence: base_sequence + offset as i32,
            timestamp: 1_700_000_000_000 + offset as i64,
            key: Some(Bytes::from(format!("key-{offset}"))),
            value: Some(Bytes::copy_from_slice(value.as_bytes())),
            headers: [(
                StrBytes::from_static_str("source"),
                Some(Bytes::from_static(b"wire-test")),
            )]
            .into_iter()
            .collect(),
        })
        .collect::<Vec<_>>();
    let mut encoded = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut encoded,
        &records,
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .expect("encode RecordBatch");
    encoded.freeze()
}

async fn dispatch_create_topics_request(
    dispatcher: &Dispatcher,
    correlation_id: i32,
    topics: Vec<CreatableTopic>,
    validate_only: bool,
) -> CreateTopicsResponse {
    let encoded = exchange_with_dispatcher(
        dispatcher,
        encode_create_topics_request(correlation_id, topics, validate_only),
    )
    .await;
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

fn decode_produce_response(mut encoded: Bytes) -> (ResponseHeader, ProduceResponse) {
    const VERSION: i16 = 7;
    let header = ResponseHeader::decode(
        &mut encoded,
        ApiKey::Produce.response_header_version(VERSION),
    )
    .expect("decode response header");
    let response = ResponseKind::decode(ApiKey::Produce, &mut encoded, VERSION)
        .expect("decode Produce response body");
    let ResponseKind::Produce(response) = response else {
        panic!("expected Produce response");
    };
    assert!(encoded.is_empty(), "response has trailing bytes");
    (header, response)
}

fn decode_list_offsets_response(mut encoded: Bytes) -> (ResponseHeader, ListOffsetsResponse) {
    const VERSION: i16 = 3;
    let header = ResponseHeader::decode(
        &mut encoded,
        ApiKey::ListOffsets.response_header_version(VERSION),
    )
    .expect("decode response header");
    let response = ResponseKind::decode(ApiKey::ListOffsets, &mut encoded, VERSION)
        .expect("decode ListOffsets response body");
    let ResponseKind::ListOffsets(response) = response else {
        panic!("expected ListOffsets response");
    };
    assert!(encoded.is_empty(), "response has trailing bytes");
    (header, response)
}

fn decode_fetch_response(mut encoded: Bytes) -> (ResponseHeader, FetchResponse) {
    const VERSION: i16 = 4;
    let header =
        ResponseHeader::decode(&mut encoded, ApiKey::Fetch.response_header_version(VERSION))
            .expect("decode response header");
    let response = ResponseKind::decode(ApiKey::Fetch, &mut encoded, VERSION)
        .expect("decode Fetch response body");
    let ResponseKind::Fetch(response) = response else {
        panic!("expected Fetch response");
    };
    assert!(encoded.is_empty(), "response has trailing bytes");
    (header, response)
}

fn decode_records(mut encoded: Bytes) -> Vec<Record> {
    RecordBatchDecoder::decode_all(&mut encoded)
        .expect("decode RecordBatches")
        .into_iter()
        .flat_map(|batch| batch.records)
        .collect()
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
