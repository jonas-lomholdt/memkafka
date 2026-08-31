use std::{num::NonZeroU32, panic, time::Duration};

use bytes::{BufMut, Bytes, BytesMut};
use clap::Parser;
use kafka_protocol::{
    ResponseError,
    messages::{
        ApiKey, ApiVersionsRequest, BrokerId, CreateTopicsRequest, DescribeConfigsRequest,
        DescribeGroupsRequest, FetchRequest, FindCoordinatorRequest, GroupId, HeartbeatRequest,
        InitProducerIdRequest, JoinGroupRequest, LeaveGroupRequest, ListGroupsRequest,
        ListOffsetsRequest, MetadataRequest, OffsetCommitRequest, OffsetFetchRequest,
        ProduceRequest, ProducerId, RequestHeader, RequestKind, ResponseHeader, ResponseKind,
        SyncGroupRequest, TopicName, TransactionalId,
        api_versions_response::{ApiVersion, ApiVersionsResponse},
        create_topics_request::{CreatableReplicaAssignment, CreatableTopic, CreatableTopicConfig},
        create_topics_response::CreateTopicsResponse,
        describe_configs_request::DescribeConfigsResource,
        fetch_request::{FetchPartition, FetchTopic},
        fetch_response::FetchResponse,
        join_group_request::JoinGroupRequestProtocol,
        leave_group_request::MemberIdentity,
        leave_group_response::{LeaveGroupResponse, MemberResponse},
        list_groups_response::{ListGroupsResponse, ListedGroup},
        list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
        list_offsets_response::ListOffsetsResponse,
        metadata_request::MetadataRequestTopic,
        metadata_response::MetadataResponse,
        offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
        offset_fetch_request::{
            OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchRequestTopics,
        },
        produce_request::{PartitionProduceData, TopicProduceData},
        produce_response::ProduceResponse,
        sync_group_request::SyncGroupRequestAssignment,
    },
    protocol::{Decodable, Encodable, StrBytes, encode_request_header_into_buffer},
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
use serde::Deserialize;
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    sync::{oneshot, watch},
    task::JoinHandle,
    time::timeout,
};

const API_VERSIONS_VERSION: i16 = 3;

struct BoundaryCase {
    api_key: ApiKey,
    request_for: fn(i16) -> RequestKind,
    assert_supported: fn(&RequestKind, &ResponseKind, i16),
    assert_error: fn(&RequestKind, &ResponseKind, i16),
}

struct TaggedRequestExpectation {
    header: (i32, Bytes),
    body: (i32, Bytes),
}

#[derive(Deserialize)]
struct BoundaryManifest {
    apis: Vec<BoundaryCapability>,
}

#[derive(Deserialize)]
struct BoundaryCapability {
    #[serde(rename = "apiKey")]
    api_key: i16,
    supported: BoundaryVersion,
    #[serde(rename = "kafka43")]
    kafka_4_3: BoundaryVersion,
}

#[derive(Deserialize)]
struct BoundaryVersion {
    min: i16,
    max: i16,
}

#[test]
#[should_panic(expected = "exact capability advertisement mismatch")]
fn supported_api_versions_callback_rejects_a_default_response_stub() {
    // Mutation evidence: a dispatcher that returns the generated default response does not
    // satisfy the precise supported-response contract below.
    assert_api_versions_supported(
        &RequestKind::ApiVersions(ApiVersionsRequest::default()),
        &ResponseKind::ApiVersions(Default::default()),
        4,
    );
}

#[test]
#[should_panic(expected = "supported LeaveGroup v3 results")]
fn supported_leave_group_callback_rejects_truncated_member_results() {
    let request = RequestKind::LeaveGroup(LeaveGroupRequest::default().with_members(vec![
        MemberIdentity::default()
            .with_member_id(StrBytes::from_static_str("boundary-supported-member")),
        MemberIdentity::default()
            .with_member_id(StrBytes::from_static_str("boundary-supported-member-b")),
    ]));
    let response = ResponseKind::LeaveGroup(LeaveGroupResponse::default().with_members(vec![
            MemberResponse::default()
                .with_member_id(StrBytes::from_static_str("boundary-supported-member"))
                .with_error_code(ResponseError::UnknownMemberId.code()),
        ]));

    assert_leave_group_supported(&request, &response, 3);
}

#[test]
#[should_panic(expected = "supported ListGroups results")]
fn supported_list_groups_callback_rejects_truncated_group_results() {
    let request = RequestKind::ListGroups(ListGroupsRequest::default());
    let response = ResponseKind::ListGroups(ListGroupsResponse::default().with_groups(vec![
            ListedGroup::default()
                .with_group_id(GroupId::from(StrBytes::from_static_str("boundary-list-group")))
                .with_protocol_type(StrBytes::from_static_str("consumer")),
        ]));

    assert_list_groups_supported(&request, &response, 0);
}

#[test]
fn flexible_api_versions_body_tags_are_rejected_with_classic_headers() {
    for version in [3, 4] {
        assert_eq!(
            ApiKey::ApiVersions.response_header_version(version),
            0,
            "ApiVersions v{version} response header is classic"
        );
        let mut response =
            ApiVersionsResponse::default().with_api_keys(vec![ApiVersion::default()]);
        response.api_keys[0]
            .unknown_tagged_fields
            .insert(701, Bytes::from_static(b"api-version-body-tag"));
        let mut encoded = BytesMut::new();
        ResponseHeader::default()
            .with_correlation_id(91_000 + i32::from(version))
            .encode(&mut encoded, 0)
            .expect("encode classic ApiVersions response header");
        response
            .encode(&mut encoded, version)
            .expect("encode flexible ApiVersions response body");

        let panic = match panic::catch_unwind(|| {
            decode_response_kind(ApiKey::ApiVersions, version, encoded.freeze())
        }) {
            Ok(_) => panic!("ApiVersions v{version} body tags must not be silently skipped"),
            Err(panic) => panic,
        };
        let message = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .expect("unknown-tag assertion must panic with a message");
        let expected = format!(
            "ApiVersions v{version} api_version unexpectedly contains unknown tagged fields"
        );
        assert!(
            message.contains(&expected),
            "unexpected ApiVersions v{version} body-tag panic: {message}"
        );
    }
}

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
async fn advertised_api_boundary_matrix() {
    let cases = boundary_cases();
    let manifest = boundary_manifest();
    assert_boundary_coverage(&cases, &manifest);

    let server = SpawnedServer::start(ephemeral_config()).await;
    let mut connection = server.connect().await;
    let mut correlation_id = 80_000;

    for case in cases {
        let capability = boundary_capability(&manifest, case.api_key);

        for version in supported_boundary_versions(capability) {
            let request = supported_request_for(case.api_key, version);
            if case.api_key == ApiKey::ListGroups && version == capability.supported.min {
                correlation_id += 1;
                seed_list_groups_fixture(&mut connection, correlation_id).await;
                correlation_id += 3;
            }
            correlation_id += 1;
            write_frame(
                &mut connection,
                &encode_boundary_request(case.api_key, version, correlation_id, &request, None),
            )
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "write supported {:?} v{version} request: {error}",
                    case.api_key
                )
            });
            let encoded = read_response(
                &mut connection,
                &format!("supported {:?} v{version}", case.api_key),
            )
            .await;
            let (header, response) = decode_response_kind(case.api_key, version, encoded);
            assert_eq!(header.correlation_id, correlation_id);
            (case.assert_supported)(&request, &response, version);
        }

        for version in adjacent_unsupported_versions(capability) {
            let request = (case.request_for)(version);
            correlation_id += 1;
            write_frame(
                &mut connection,
                &encode_boundary_request(case.api_key, version, correlation_id, &request, None),
            )
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "write unsupported {:?} v{version} request: {error}",
                    case.api_key
                )
            });
            let encoded = read_response(
                &mut connection,
                &format!("unsupported {:?} v{version}", case.api_key),
            )
            .await;
            let (header, response) = decode_response_kind(case.api_key, version, encoded);
            assert_eq!(header.correlation_id, correlation_id);
            (case.assert_error)(&request, &response, version);
        }
    }

    server.shutdown().await;
}

#[tokio::test]
async fn advertised_api_same_connection_matrix() {
    let cases = boundary_cases();
    let manifest = boundary_manifest();
    assert_boundary_coverage(&cases, &manifest);

    let server = SpawnedServer::start(ephemeral_config()).await;
    let mut connection = server.connect().await;
    let mut correlation_id = 81_000;
    let mut exercised_lower_neighbor = false;
    let mut exercised_upper_neighbor = false;
    let mut one_neighbor_exceptions = Vec::new();

    for case in &cases {
        let capability = boundary_capability(&manifest, case.api_key);
        let requested_direction = scheduled_boundary_direction(case.api_key);
        let (version, actual_direction) =
            scheduled_same_connection_version(capability, requested_direction);
        if requested_direction != actual_direction {
            one_neighbor_exceptions.push((case.api_key, requested_direction, actual_direction));
        }
        exercised_lower_neighbor |= version < capability.supported.min;
        exercised_upper_neighbor |= version > capability.supported.max;
        let request = (case.request_for)(version);
        correlation_id += 1;
        write_frame(
            &mut connection,
            &encode_boundary_request(case.api_key, version, correlation_id, &request, None),
        )
        .await
        .unwrap_or_else(|error| {
            panic!(
                "write same-connection unsupported {:?} v{version} request: {error}",
                case.api_key
            )
        });
        let encoded = read_response(
            &mut connection,
            &format!("same-connection unsupported {:?} v{version}", case.api_key),
        )
        .await;
        let (header, response) = decode_response_kind(case.api_key, version, encoded);
        assert_eq!(header.correlation_id, correlation_id);
        (case.assert_error)(&request, &response, version);

        correlation_id += 1;
        assert_api_versions_v4_success(&mut connection, correlation_id).await;
    }

    assert!(
        exercised_lower_neighbor && exercised_upper_neighbor,
        "same-connection matrix must exercise lower and upper neighbors"
    );
    assert_eq!(
        one_neighbor_exceptions,
        vec![
            (
                ApiKey::ListGroups,
                BoundaryDirection::Lower,
                BoundaryDirection::Upper
            ),
            (
                ApiKey::ApiVersions,
                BoundaryDirection::Upper,
                BoundaryDirection::Lower
            ),
            (
                ApiKey::DescribeConfigs,
                BoundaryDirection::Lower,
                BoundaryDirection::Upper
            ),
        ],
        "only schema edges may override the explicit lower/upper schedule"
    );

    assert!(
        cases.iter().any(|case| case.api_key == ApiKey::Produce),
        "Produce boundary case"
    );
    let produce_version =
        adjacent_unsupported_versions(boundary_capability(&manifest, ApiKey::Produce))
            .into_iter()
            .last()
            .expect("Produce has an unsupported neighbor");
    let request = produce_request(0);
    correlation_id += 1;
    write_frame(
        &mut connection,
        &encode_boundary_request(
            ApiKey::Produce,
            produce_version,
            correlation_id,
            &RequestKind::Produce(request),
            None,
        ),
    )
    .await
    .expect("write unsupported Produce acks=0 request");

    let mut unexpected = [0_u8; 1];
    match timeout(Duration::from_millis(100), connection.read(&mut unexpected)).await {
        Err(_) => {}
        Ok(Ok(0)) => panic!("unsupported Produce acks=0 closed the reusable connection"),
        Ok(Ok(_)) => panic!("unsupported Produce acks=0 wrote a response"),
        Ok(Err(error)) => panic!("read after unsupported Produce acks=0: {error}"),
    }

    correlation_id += 1;
    assert_api_versions_v4_success(&mut connection, correlation_id).await;
    server.shutdown().await;
}

#[tokio::test]
async fn tagged_field_boundary_rejections_remain_decodable() {
    let cases = boundary_cases();
    let manifest = boundary_manifest();
    assert_boundary_coverage(&cases, &manifest);
    let server = SpawnedServer::start(ephemeral_config()).await;
    let mut connection = server.connect().await;

    for (correlation_id, (api_key, version, header_tag, body_tag)) in [
        (ApiKey::Produce, 9, 101, 201),
        (ApiKey::Fetch, 12, 102, 202),
        (ApiKey::OffsetFetch, 6, 103, 203),
        (ApiKey::CreateTopics, 7, 104, 204),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, (api_key, version, header_tag, body_tag))| {
        (
            82_000 + index as i32,
            (api_key, version, header_tag, body_tag),
        )
    }) {
        let case = cases
            .iter()
            .find(|case| case.api_key == api_key)
            .expect("tagged-field boundary case");
        let capability = boundary_capability(&manifest, api_key);
        assert!(
            capability.kafka_4_3.min <= version
                && version <= capability.kafka_4_3.max
                && !capability_supports(capability, version),
            "tagged-field version must be schema-known and unsupported"
        );
        assert_eq!(api_key.request_header_version(version), 2);

        let mut request = (case.request_for)(version);
        add_unknown_body_tag(
            &mut request,
            api_key,
            body_tag,
            Bytes::from(vec![body_tag as u8]),
        );
        let header_value = Bytes::from(vec![header_tag as u8]);
        let encoded_request = encode_boundary_request(
            api_key,
            version,
            correlation_id,
            &request,
            Some((header_tag, header_value.clone())),
        );
        assert_encoded_request_tags(
            encoded_request.clone(),
            api_key,
            version,
            correlation_id,
            TaggedRequestExpectation {
                header: (header_tag, header_value),
                body: (body_tag, Bytes::from(vec![body_tag as u8])),
            },
        );
        write_frame(&mut connection, &encoded_request)
            .await
            .unwrap_or_else(|error| {
                panic!("write tagged {:?} v{version} request: {error}", api_key)
            });
        let encoded =
            read_response(&mut connection, &format!("tagged {:?} v{version}", api_key)).await;
        let (header, response) = decode_response_kind(api_key, version, encoded);
        assert_eq!(header.correlation_id, correlation_id);
        (case.assert_error)(&request, &response, version);
    }

    server.shutdown().await;
}

#[tokio::test]
async fn classic_header_boundary_rejections_remain_decodable() {
    let cases = boundary_cases();
    let server = SpawnedServer::start(ephemeral_config()).await;
    let mut connection = server.connect().await;

    for (correlation_id, api_key, version) in [
        (83_001, ApiKey::Metadata, 3),
        (83_002, ApiKey::DescribeGroups, 1),
    ] {
        let case = cases
            .iter()
            .find(|case| case.api_key == api_key)
            .expect("classic boundary case");
        assert_eq!(api_key.request_header_version(version), 1);
        assert_eq!(api_key.response_header_version(version), 0);
        let request = (case.request_for)(version);
        write_frame(
            &mut connection,
            &encode_boundary_request(api_key, version, correlation_id, &request, None),
        )
        .await
        .unwrap_or_else(|error| panic!("write classic {:?} v{version} request: {error}", api_key));
        let encoded = read_response(
            &mut connection,
            &format!("classic {:?} v{version}", api_key),
        )
        .await;
        let (header, response) = decode_response_kind(api_key, version, encoded);
        assert_eq!(header.correlation_id, correlation_id);
        (case.assert_error)(&request, &response, version);
    }

    server.shutdown().await;
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

fn boundary_cases() -> [BoundaryCase; 17] {
    [
        BoundaryCase {
            api_key: ApiKey::Produce,
            request_for: |_: i16| RequestKind::Produce(produce_request(1)),
            assert_supported: assert_produce_supported,
            assert_error: assert_produce_unsupported,
        },
        BoundaryCase {
            api_key: ApiKey::Fetch,
            request_for: fetch_boundary_request,
            assert_supported: assert_fetch_supported,
            assert_error: assert_fetch_unsupported,
        },
        BoundaryCase {
            api_key: ApiKey::ListOffsets,
            request_for: list_offsets_boundary_request,
            assert_supported: assert_list_offsets_supported,
            assert_error: assert_list_offsets_unsupported,
        },
        BoundaryCase {
            api_key: ApiKey::Metadata,
            request_for: metadata_boundary_request,
            assert_supported: assert_metadata_supported,
            assert_error: assert_metadata_unsupported,
        },
        BoundaryCase {
            api_key: ApiKey::OffsetCommit,
            request_for: offset_commit_boundary_request,
            assert_supported: assert_offset_commit_supported,
            assert_error: assert_offset_commit_unsupported,
        },
        BoundaryCase {
            api_key: ApiKey::OffsetFetch,
            request_for: offset_fetch_boundary_request,
            assert_supported: assert_offset_fetch_supported,
            assert_error: assert_offset_fetch_unsupported,
        },
        BoundaryCase {
            api_key: ApiKey::FindCoordinator,
            request_for: find_coordinator_boundary_request,
            assert_supported: assert_find_coordinator_supported,
            assert_error: assert_find_coordinator_unsupported,
        },
        BoundaryCase {
            api_key: ApiKey::JoinGroup,
            request_for: join_group_boundary_request,
            assert_supported: assert_join_group_supported,
            assert_error: assert_join_group_unsupported,
        },
        BoundaryCase {
            api_key: ApiKey::Heartbeat,
            request_for: heartbeat_boundary_request,
            assert_supported: assert_heartbeat_supported,
            assert_error: assert_heartbeat_unsupported,
        },
        BoundaryCase {
            api_key: ApiKey::LeaveGroup,
            request_for: leave_group_boundary_request,
            assert_supported: assert_leave_group_supported,
            assert_error: assert_leave_group_unsupported,
        },
        BoundaryCase {
            api_key: ApiKey::SyncGroup,
            request_for: sync_group_boundary_request,
            assert_supported: assert_sync_group_supported,
            assert_error: assert_sync_group_unsupported,
        },
        BoundaryCase {
            api_key: ApiKey::DescribeGroups,
            request_for: describe_groups_boundary_request,
            assert_supported: assert_describe_groups_supported,
            assert_error: assert_describe_groups_unsupported,
        },
        BoundaryCase {
            api_key: ApiKey::ListGroups,
            request_for: |_: i16| RequestKind::ListGroups(ListGroupsRequest::default()),
            assert_supported: assert_list_groups_supported,
            assert_error: assert_list_groups_unsupported,
        },
        BoundaryCase {
            api_key: ApiKey::ApiVersions,
            request_for: |_: i16| RequestKind::ApiVersions(ApiVersionsRequest::default()),
            assert_supported: assert_api_versions_supported,
            assert_error: assert_api_versions_unsupported,
        },
        BoundaryCase {
            api_key: ApiKey::CreateTopics,
            request_for: create_topics_boundary_request,
            assert_supported: assert_create_topics_supported,
            assert_error: assert_create_topics_unsupported,
        },
        BoundaryCase {
            api_key: ApiKey::InitProducerId,
            request_for: |_: i16| RequestKind::InitProducerId(InitProducerIdRequest::default()),
            assert_supported: assert_init_producer_id_supported,
            assert_error: assert_init_producer_id_unsupported,
        },
        BoundaryCase {
            api_key: ApiKey::DescribeConfigs,
            request_for: describe_configs_boundary_request,
            assert_supported: assert_describe_configs_supported,
            assert_error: assert_describe_configs_unsupported,
        },
    ]
}

fn boundary_manifest() -> BoundaryManifest {
    serde_json::from_str(
        &memkafka::kafka::capabilities::manifest_json().expect("render capability manifest"),
    )
    .expect("decode executable capability manifest")
}

fn assert_boundary_coverage(cases: &[BoundaryCase], manifest: &BoundaryManifest) {
    assert_eq!(cases.len(), 17, "boundary table must cover 17 APIs exactly");
    assert_eq!(manifest.apis.len(), 17, "manifest must advertise 17 APIs");
    assert!(
        cases
            .windows(2)
            .all(|pair| (pair[0].api_key as i16) < (pair[1].api_key as i16)),
        "boundary cases must be in API-key order"
    );
    assert_eq!(
        cases
            .iter()
            .map(|case| case.api_key as i16)
            .collect::<Vec<_>>(),
        manifest
            .apis
            .iter()
            .map(|capability| capability.api_key)
            .collect::<Vec<_>>(),
        "boundary table and executable manifest diverged"
    );
}

fn boundary_capability(manifest: &BoundaryManifest, api_key: ApiKey) -> &BoundaryCapability {
    manifest
        .apis
        .iter()
        .find(|capability| capability.api_key == api_key as i16)
        .unwrap_or_else(|| panic!("{api_key:?} is absent from the capability manifest"))
}

fn capability_supports(capability: &BoundaryCapability, version: i16) -> bool {
    capability.supported.min <= version && version <= capability.supported.max
}

fn supported_boundary_versions(capability: &BoundaryCapability) -> Vec<i16> {
    let mut versions = vec![capability.supported.min];
    if capability.supported.max != capability.supported.min {
        versions.push(capability.supported.max);
    }
    versions
}

fn adjacent_unsupported_versions(capability: &BoundaryCapability) -> Vec<i16> {
    [capability.supported.min - 1, capability.supported.max + 1]
        .into_iter()
        .filter(|version| {
            capability.kafka_4_3.min <= *version
                && *version <= capability.kafka_4_3.max
                && !capability_supports(capability, *version)
        })
        .collect()
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum BoundaryDirection {
    Lower,
    Upper,
}

fn scheduled_boundary_direction(api_key: ApiKey) -> BoundaryDirection {
    match api_key {
        ApiKey::Produce
        | ApiKey::ListOffsets
        | ApiKey::OffsetCommit
        | ApiKey::FindCoordinator
        | ApiKey::Heartbeat
        | ApiKey::SyncGroup
        | ApiKey::ListGroups
        | ApiKey::CreateTopics
        | ApiKey::DescribeConfigs => BoundaryDirection::Lower,
        ApiKey::Fetch
        | ApiKey::Metadata
        | ApiKey::OffsetFetch
        | ApiKey::JoinGroup
        | ApiKey::LeaveGroup
        | ApiKey::DescribeGroups
        | ApiKey::ApiVersions
        | ApiKey::InitProducerId => BoundaryDirection::Upper,
        _ => panic!("same-connection schedule is missing {api_key:?}"),
    }
}

fn scheduled_same_connection_version(
    capability: &BoundaryCapability,
    requested_direction: BoundaryDirection,
) -> (i16, BoundaryDirection) {
    let adjacent = adjacent_unsupported_versions(capability);
    let matches_requested_direction = |version: &i16| match requested_direction {
        BoundaryDirection::Lower => *version < capability.supported.min,
        BoundaryDirection::Upper => *version > capability.supported.max,
    };
    if let Some(version) = adjacent.iter().copied().find(matches_requested_direction) {
        return (version, requested_direction);
    }

    assert_eq!(
        adjacent.len(),
        1,
        "{:#?} must have a requested neighbor or exactly one schema-known exception",
        capability.api_key
    );
    let version = adjacent[0];
    let actual_direction = if version < capability.supported.min {
        BoundaryDirection::Lower
    } else {
        BoundaryDirection::Upper
    };
    (version, actual_direction)
}

fn produce_request(acks: i16) -> ProduceRequest {
    ProduceRequest::default()
        .with_acks(acks)
        .with_timeout_ms(1_234)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(topic_name("boundary-produce-a"))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(7)
                        .with_records(Some(Bytes::from_static(b"boundary-a-one"))),
                    PartitionProduceData::default()
                        .with_index(17)
                        .with_records(Some(Bytes::from_static(b"boundary-a-two"))),
                ]),
            TopicProduceData::default()
                .with_name(topic_name("boundary-produce-b"))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(27)
                        .with_records(Some(Bytes::from_static(b"boundary-b-one"))),
                    PartitionProduceData::default()
                        .with_index(37)
                        .with_records(Some(Bytes::from_static(b"boundary-b-two"))),
                ]),
        ])
}

fn fetch_boundary_request(_: i16) -> RequestKind {
    RequestKind::Fetch(
        FetchRequest::default()
            .with_replica_id(BrokerId::from(-1))
            .with_max_wait_ms(0)
            .with_min_bytes(0)
            .with_max_bytes(4_096)
            .with_session_id(99)
            .with_session_epoch(4)
            .with_topics(vec![
                FetchTopic::default()
                    .with_topic(topic_name("boundary-fetch-a"))
                    .with_partitions(vec![
                        FetchPartition::default()
                            .with_partition(8)
                            .with_fetch_offset(456)
                            .with_partition_max_bytes(2_048),
                        FetchPartition::default()
                            .with_partition(18)
                            .with_fetch_offset(556)
                            .with_partition_max_bytes(2_048),
                    ]),
                FetchTopic::default()
                    .with_topic(topic_name("boundary-fetch-b"))
                    .with_partitions(vec![
                        FetchPartition::default()
                            .with_partition(28)
                            .with_fetch_offset(656)
                            .with_partition_max_bytes(2_048),
                        FetchPartition::default()
                            .with_partition(38)
                            .with_fetch_offset(756)
                            .with_partition_max_bytes(2_048),
                    ]),
            ]),
    )
}

fn list_offsets_boundary_request(_: i16) -> RequestKind {
    RequestKind::ListOffsets(
        ListOffsetsRequest::default()
            .with_replica_id(BrokerId::from(-1))
            .with_topics(vec![
                ListOffsetsTopic::default()
                    .with_name(topic_name("boundary-offsets-a"))
                    .with_partitions(vec![
                        ListOffsetsPartition::default()
                            .with_partition_index(9)
                            .with_timestamp(1_725_000_000_000),
                        ListOffsetsPartition::default()
                            .with_partition_index(19)
                            .with_timestamp(1_725_000_000_001),
                    ]),
                ListOffsetsTopic::default()
                    .with_name(topic_name("boundary-offsets-b"))
                    .with_partitions(vec![
                        ListOffsetsPartition::default()
                            .with_partition_index(29)
                            .with_timestamp(1_725_000_000_002),
                        ListOffsetsPartition::default()
                            .with_partition_index(39)
                            .with_timestamp(1_725_000_000_003),
                    ]),
            ]),
    )
}

fn metadata_boundary_request(_: i16) -> RequestKind {
    RequestKind::Metadata(MetadataRequest::default().with_topics(Some(vec![
        MetadataRequestTopic::default().with_name(Some(topic_name("boundary-metadata-a"))),
        MetadataRequestTopic::default().with_name(Some(topic_name("boundary-metadata-b"))),
    ])))
}

fn offset_commit_boundary_request(_: i16) -> RequestKind {
    RequestKind::OffsetCommit(
        OffsetCommitRequest::default()
            .with_group_id(GroupId::from(StrBytes::from_static_str(
                "boundary-commit-group",
            )))
            .with_member_id(StrBytes::from_static_str("boundary-commit-member"))
            .with_topics(vec![
                OffsetCommitRequestTopic::default()
                    .with_name(topic_name("boundary-commit-a"))
                    .with_partitions(vec![
                        OffsetCommitRequestPartition::default()
                            .with_partition_index(10)
                            .with_committed_offset(987),
                        OffsetCommitRequestPartition::default()
                            .with_partition_index(20)
                            .with_committed_offset(988),
                    ]),
                OffsetCommitRequestTopic::default()
                    .with_name(topic_name("boundary-commit-b"))
                    .with_partitions(vec![
                        OffsetCommitRequestPartition::default()
                            .with_partition_index(30)
                            .with_committed_offset(989),
                        OffsetCommitRequestPartition::default()
                            .with_partition_index(40)
                            .with_committed_offset(990),
                    ]),
            ]),
    )
}

fn offset_fetch_boundary_request(version: i16) -> RequestKind {
    if version <= 7 {
        return RequestKind::OffsetFetch(
            OffsetFetchRequest::default()
                .with_group_id(GroupId::from(StrBytes::from_static_str(
                    "boundary-offset-fetch-group",
                )))
                .with_topics(Some(vec![
                    OffsetFetchRequestTopic::default()
                        .with_name(topic_name("boundary-offset-fetch-a"))
                        .with_partition_indexes(vec![11, 21]),
                    OffsetFetchRequestTopic::default()
                        .with_name(topic_name("boundary-offset-fetch-b"))
                        .with_partition_indexes(vec![31, 41]),
                ])),
        );
    }

    RequestKind::OffsetFetch(OffsetFetchRequest::default().with_groups(vec![
        OffsetFetchRequestGroup::default()
            .with_group_id(GroupId::from(StrBytes::from_static_str(
                "boundary-offset-fetch-group-a",
            )))
            .with_topics(Some(vec![
                OffsetFetchRequestTopics::default()
                    .with_name(topic_name("boundary-offset-fetch-v8-a"))
                    .with_partition_indexes(vec![12, 22]),
                OffsetFetchRequestTopics::default()
                    .with_name(topic_name("boundary-offset-fetch-v8-b"))
                    .with_partition_indexes(vec![32, 42]),
            ])),
        OffsetFetchRequestGroup::default()
            .with_group_id(GroupId::from(StrBytes::from_static_str(
                "boundary-offset-fetch-group-b",
            )))
            .with_topics(Some(vec![
                OffsetFetchRequestTopics::default()
                    .with_name(topic_name("boundary-offset-fetch-v8-c"))
                    .with_partition_indexes(vec![52, 62]),
                OffsetFetchRequestTopics::default()
                    .with_name(topic_name("boundary-offset-fetch-v8-d"))
                    .with_partition_indexes(vec![72, 82]),
            ])),
    ]))
}

fn find_coordinator_boundary_request(version: i16) -> RequestKind {
    if version <= 3 {
        RequestKind::FindCoordinator(
            FindCoordinatorRequest::default()
                .with_key(StrBytes::from_static_str("boundary-coordinator")),
        )
    } else {
        RequestKind::FindCoordinator(
            FindCoordinatorRequest::default().with_coordinator_keys(vec![
                StrBytes::from_static_str("boundary-coordinator-a"),
                StrBytes::from_static_str("boundary-coordinator-b"),
            ]),
        )
    }
}

fn join_group_boundary_request(_: i16) -> RequestKind {
    RequestKind::JoinGroup(
        JoinGroupRequest::default()
            .with_group_id(GroupId::from(StrBytes::from_static_str(
                "boundary-join-group",
            )))
            .with_member_id(StrBytes::from_static_str("boundary-join-member"))
            .with_protocol_type(StrBytes::from_static_str("consumer")),
    )
}

fn heartbeat_boundary_request(_: i16) -> RequestKind {
    RequestKind::Heartbeat(
        HeartbeatRequest::default()
            .with_group_id(GroupId::from(StrBytes::from_static_str(
                "boundary-heartbeat-group",
            )))
            .with_member_id(StrBytes::from_static_str("boundary-heartbeat-member")),
    )
}

fn leave_group_boundary_request(version: i16) -> RequestKind {
    let request = LeaveGroupRequest::default().with_group_id(GroupId::from(
        StrBytes::from_static_str("boundary-leave-group"),
    ));
    if version <= 2 {
        RequestKind::LeaveGroup(
            request.with_member_id(StrBytes::from_static_str("boundary-leave-member")),
        )
    } else {
        RequestKind::LeaveGroup(request.with_members(vec![
            MemberIdentity::default()
                .with_member_id(StrBytes::from_static_str("boundary-leave-member-a"))
                .with_group_instance_id(Some(StrBytes::from_static_str("boundary-instance-a"))),
            MemberIdentity::default()
                .with_member_id(StrBytes::from_static_str("boundary-leave-member-b"))
                .with_group_instance_id(Some(StrBytes::from_static_str("boundary-instance-b"))),
        ]))
    }
}

fn sync_group_boundary_request(_: i16) -> RequestKind {
    RequestKind::SyncGroup(
        SyncGroupRequest::default()
            .with_group_id(GroupId::from(StrBytes::from_static_str(
                "boundary-sync-group",
            )))
            .with_member_id(StrBytes::from_static_str("boundary-sync-member")),
    )
}

fn describe_groups_boundary_request(_: i16) -> RequestKind {
    RequestKind::DescribeGroups(DescribeGroupsRequest::default().with_groups(vec![
        GroupId::from(StrBytes::from_static_str("boundary-describe-group-a")),
        GroupId::from(StrBytes::from_static_str("boundary-describe-group-b")),
    ]))
}

fn create_topics_boundary_request(_: i16) -> RequestKind {
    RequestKind::CreateTopics(CreateTopicsRequest::default().with_topics(vec![
        CreatableTopic::default()
            .with_name(topic_name("boundary-create-a"))
            .with_num_partitions(5)
            .with_replication_factor(1),
        CreatableTopic::default()
            .with_name(topic_name("boundary-create-b"))
            .with_num_partitions(7)
            .with_replication_factor(1),
    ]))
}

fn describe_configs_boundary_request(_: i16) -> RequestKind {
    RequestKind::DescribeConfigs(DescribeConfigsRequest::default().with_resources(vec![
        DescribeConfigsResource::default()
            .with_resource_type(2)
            .with_resource_name(StrBytes::from_static_str("boundary-config-topic")),
        DescribeConfigsResource::default()
            .with_resource_type(4)
            .with_resource_name(StrBytes::from_static_str("boundary-config-broker")),
    ]))
}

fn supported_request_for(api_key: ApiKey, version: i16) -> RequestKind {
    match api_key {
        ApiKey::Produce => RequestKind::Produce(produce_request(2)),
        ApiKey::Fetch => fetch_boundary_request(version),
        ApiKey::ListOffsets => list_offsets_boundary_request(version),
        ApiKey::Metadata => RequestKind::Metadata(
            MetadataRequest::default()
                .with_topics(Some(vec![
                    MetadataRequestTopic::default()
                        .with_name(Some(topic_name("boundary-supported-metadata-a"))),
                    MetadataRequestTopic::default()
                        .with_name(Some(topic_name("boundary-supported-metadata-b"))),
                ]))
                .with_allow_auto_topic_creation(false),
        ),
        ApiKey::OffsetCommit => offset_commit_boundary_request(version),
        ApiKey::OffsetFetch => offset_fetch_boundary_request(version),
        ApiKey::FindCoordinator => RequestKind::FindCoordinator(
            FindCoordinatorRequest::default()
                .with_key(StrBytes::from_static_str("boundary-supported-coordinator")),
        ),
        ApiKey::JoinGroup => RequestKind::JoinGroup(
            JoinGroupRequest::default()
                .with_group_id(GroupId::from(StrBytes::from_static_str(
                    "boundary-supported-join",
                )))
                .with_session_timeout_ms(10_000)
                .with_rebalance_timeout_ms(10_000)
                .with_member_id(StrBytes::default())
                .with_protocol_type(StrBytes::from_static_str("consumer")),
        ),
        ApiKey::Heartbeat => RequestKind::Heartbeat(
            HeartbeatRequest::default()
                .with_group_id(GroupId::from(StrBytes::from_static_str(
                    "boundary-supported-heartbeat",
                )))
                .with_member_id(StrBytes::from_static_str("boundary-supported-member")),
        ),
        ApiKey::LeaveGroup => {
            let request = LeaveGroupRequest::default().with_group_id(GroupId::from(
                StrBytes::from_static_str("boundary-supported-leave"),
            ));
            if version <= 2 {
                RequestKind::LeaveGroup(
                    request.with_member_id(StrBytes::from_static_str("boundary-supported-member")),
                )
            } else {
                RequestKind::LeaveGroup(request.with_members(vec![
                    MemberIdentity::default().with_member_id(
                            StrBytes::from_static_str("boundary-supported-member"),
                        ),
                    MemberIdentity::default().with_member_id(
                        StrBytes::from_static_str("boundary-supported-member-b"),
                    ),
                ]))
            }
        }
        ApiKey::SyncGroup => RequestKind::SyncGroup(
            SyncGroupRequest::default()
                .with_group_id(GroupId::from(StrBytes::from_static_str(
                    "boundary-supported-sync",
                )))
                .with_member_id(StrBytes::from_static_str("boundary-supported-member")),
        ),
        ApiKey::DescribeGroups => describe_groups_boundary_request(version),
        ApiKey::ListGroups => RequestKind::ListGroups(ListGroupsRequest::default()),
        ApiKey::ApiVersions => RequestKind::ApiVersions(
            ApiVersionsRequest::default()
                .with_client_software_name(StrBytes::from_static_str("boundary"))
                .with_client_software_version(StrBytes::from_static_str("1")),
        ),
        ApiKey::CreateTopics => RequestKind::CreateTopics(
            CreateTopicsRequest::default()
                .with_validate_only(true)
                .with_topics(vec![
                    CreatableTopic::default()
                        .with_name(topic_name("boundary-supported-create-a"))
                        .with_num_partitions(1)
                        .with_replication_factor(1),
                    CreatableTopic::default()
                        .with_name(topic_name("boundary-supported-create-b"))
                        .with_num_partitions(2)
                        .with_replication_factor(1),
                ]),
        ),
        ApiKey::InitProducerId => RequestKind::InitProducerId(
            InitProducerIdRequest::default().with_transactional_id(None),
        ),
        ApiKey::DescribeConfigs => describe_configs_boundary_request(version),
        _ => panic!("supported boundary fixture is missing {api_key:?}"),
    }
}

fn encode_boundary_request(
    api_key: ApiKey,
    version: i16,
    correlation_id: i32,
    request: &RequestKind,
    header_tag: Option<(i32, Bytes)>,
) -> Bytes {
    let mut header = RequestHeader::default()
        .with_request_api_key(api_key as i16)
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_static_str("boundary-matrix")));
    if let Some((tag, value)) = header_tag {
        header.unknown_tagged_fields.insert(tag, value);
    }
    let mut encoded = BytesMut::new();
    encode_request_header_into_buffer(&mut encoded, &header)
        .unwrap_or_else(|error| panic!("encode {api_key:?} v{version} request header: {error}"));
    request
        .encode(&mut encoded, version)
        .unwrap_or_else(|error| panic!("encode {api_key:?} v{version} request body: {error}"));
    encoded.freeze()
}

fn add_unknown_body_tag(request: &mut RequestKind, api_key: ApiKey, tag: i32, value: Bytes) {
    match request {
        RequestKind::Produce(body) if api_key == ApiKey::Produce => {
            body.unknown_tagged_fields.insert(tag, value);
        }
        RequestKind::Fetch(body) if api_key == ApiKey::Fetch => {
            body.unknown_tagged_fields.insert(tag, value);
        }
        RequestKind::OffsetFetch(body) if api_key == ApiKey::OffsetFetch => {
            body.unknown_tagged_fields.insert(tag, value);
        }
        RequestKind::CreateTopics(body) if api_key == ApiKey::CreateTopics => {
            body.unknown_tagged_fields.insert(tag, value);
        }
        _ => panic!("{api_key:?} does not have the requested flexible body fixture"),
    }
}

fn assert_encoded_request_tags(
    encoded: Bytes,
    api_key: ApiKey,
    version: i16,
    correlation_id: i32,
    tags: TaggedRequestExpectation,
) {
    let DecodedFrame::Request(decoded) = decode_frame(encoded).expect("decode tagged request")
    else {
        panic!("expected decoded tagged request");
    };
    assert_eq!(decoded.api_key, api_key);
    assert_eq!(decoded.header.request_api_version, version);
    assert_eq!(decoded.header.correlation_id, correlation_id);
    assert_eq!(decoded.header.unknown_tagged_fields.len(), 1);
    assert_eq!(
        decoded.header.unknown_tagged_fields.get(&tags.header.0),
        Some(&tags.header.1)
    );
    let unknown_tagged_fields = match decoded.body {
        RequestKind::Produce(body) if api_key == ApiKey::Produce => body.unknown_tagged_fields,
        RequestKind::Fetch(body) if api_key == ApiKey::Fetch => body.unknown_tagged_fields,
        RequestKind::OffsetFetch(body) if api_key == ApiKey::OffsetFetch => {
            body.unknown_tagged_fields
        }
        RequestKind::CreateTopics(body) if api_key == ApiKey::CreateTopics => {
            body.unknown_tagged_fields
        }
        _ => panic!("decoded tagged request has the wrong body for {api_key:?}"),
    };
    assert_eq!(unknown_tagged_fields.len(), 1);
    assert_eq!(unknown_tagged_fields.get(&tags.body.0), Some(&tags.body.1));
}

/// ApiVersions v3/v4 has a classic response header with a flexible tagged body.  Other task
/// APIs use a flexible body exactly when their response header is flexible.
fn response_body_is_flexible(api_key: ApiKey, version: i16) -> bool {
    (api_key == ApiKey::ApiVersions && version >= 3)
        || api_key.response_header_version(version) == 1
}

/// Flexible response bodies are decoded by the shared matrix decoder. The protocol carries no
/// response body tags for these fixtures, so audit every encoded generated tagged-field map.
fn assert_flexible_response_tags_empty(api_key: ApiKey, version: i16, response: &ResponseKind) {
    assert!(
        response_body_is_flexible(api_key, version),
        "{api_key:?} v{version} does not have a flexible response body"
    );

    macro_rules! assert_no_tags {
        ($value:expr) => {
            assert!(
                $value.unknown_tagged_fields.is_empty(),
                "{api_key:?} v{version} {} unexpectedly contains unknown tagged fields",
                stringify!($value)
            )
        };
    }

    match response {
        ResponseKind::Produce(response) if api_key == ApiKey::Produce => {
            assert_no_tags!(response);
            for topic in &response.responses {
                assert_no_tags!(topic);
                for partition in &topic.partition_responses {
                    assert_no_tags!(partition);
                    if version >= 10 {
                        assert_no_tags!(partition.current_leader);
                    }
                    for record_error in &partition.record_errors {
                        assert_no_tags!(record_error);
                    }
                }
            }
            if version >= 10 {
                for endpoint in &response.node_endpoints {
                    assert_no_tags!(endpoint);
                }
            }
        }
        ResponseKind::Fetch(response) if api_key == ApiKey::Fetch => {
            assert_no_tags!(response);
            for topic in &response.responses {
                assert_no_tags!(topic);
                for partition in &topic.partitions {
                    assert_no_tags!(partition);
                    if partition.diverging_epoch != Default::default() {
                        assert_no_tags!(partition.diverging_epoch);
                    }
                    if partition.current_leader != Default::default() {
                        assert_no_tags!(partition.current_leader);
                    }
                    if partition.snapshot_id != Default::default() {
                        assert_no_tags!(partition.snapshot_id);
                    }
                    for transaction in partition
                        .aborted_transactions
                        .as_deref()
                        .unwrap_or_default()
                    {
                        assert_no_tags!(transaction);
                    }
                }
            }
            if version >= 16 {
                for endpoint in &response.node_endpoints {
                    assert_no_tags!(endpoint);
                }
            }
        }
        ResponseKind::ListOffsets(response) if api_key == ApiKey::ListOffsets => {
            assert_no_tags!(response);
            for topic in &response.topics {
                assert_no_tags!(topic);
                for partition in &topic.partitions {
                    assert_no_tags!(partition);
                }
            }
        }
        ResponseKind::Metadata(response) if api_key == ApiKey::Metadata => {
            assert_no_tags!(response);
            for broker in &response.brokers {
                assert_no_tags!(broker);
            }
            for topic in &response.topics {
                assert_no_tags!(topic);
                for partition in &topic.partitions {
                    assert_no_tags!(partition);
                }
            }
        }
        ResponseKind::OffsetCommit(response) if api_key == ApiKey::OffsetCommit => {
            assert_no_tags!(response);
            for topic in &response.topics {
                assert_no_tags!(topic);
                for partition in &topic.partitions {
                    assert_no_tags!(partition);
                }
            }
        }
        ResponseKind::OffsetFetch(response) if api_key == ApiKey::OffsetFetch => {
            assert_no_tags!(response);
            if version <= 7 {
                for topic in &response.topics {
                    assert_no_tags!(topic);
                    for partition in &topic.partitions {
                        assert_no_tags!(partition);
                    }
                }
            }
            if version >= 8 {
                for group in &response.groups {
                    assert_no_tags!(group);
                    for topic in &group.topics {
                        assert_no_tags!(topic);
                        for partition in &topic.partitions {
                            assert_no_tags!(partition);
                        }
                    }
                }
            }
        }
        ResponseKind::FindCoordinator(response) if api_key == ApiKey::FindCoordinator => {
            assert_no_tags!(response);
            for coordinator in &response.coordinators {
                assert_no_tags!(coordinator);
            }
        }
        ResponseKind::JoinGroup(response) if api_key == ApiKey::JoinGroup => {
            assert_no_tags!(response);
            for member in &response.members {
                assert_no_tags!(member);
            }
        }
        ResponseKind::Heartbeat(response) if api_key == ApiKey::Heartbeat => {
            assert_no_tags!(response);
        }
        ResponseKind::LeaveGroup(response) if api_key == ApiKey::LeaveGroup => {
            assert_no_tags!(response);
            for member in &response.members {
                assert_no_tags!(member);
            }
        }
        ResponseKind::SyncGroup(response) if api_key == ApiKey::SyncGroup => {
            assert_no_tags!(response);
        }
        ResponseKind::DescribeGroups(response) if api_key == ApiKey::DescribeGroups => {
            assert_no_tags!(response);
            for group in &response.groups {
                assert_no_tags!(group);
                for member in &group.members {
                    assert_no_tags!(member);
                }
            }
        }
        ResponseKind::ListGroups(response) if api_key == ApiKey::ListGroups => {
            assert_no_tags!(response);
            for group in &response.groups {
                assert_no_tags!(group);
            }
        }
        ResponseKind::ApiVersions(response) if api_key == ApiKey::ApiVersions => {
            assert_no_tags!(response);
            for api_version in &response.api_keys {
                assert_no_tags!(api_version);
            }
            for feature in &response.supported_features {
                assert_no_tags!(feature);
            }
            for feature in &response.finalized_features {
                assert_no_tags!(feature);
            }
        }
        ResponseKind::CreateTopics(response) if api_key == ApiKey::CreateTopics => {
            assert_no_tags!(response);
            for topic in &response.topics {
                assert_no_tags!(topic);
                for config in topic.configs.as_deref().unwrap_or_default() {
                    assert_no_tags!(config);
                }
            }
        }
        ResponseKind::InitProducerId(response) if api_key == ApiKey::InitProducerId => {
            assert_no_tags!(response);
        }
        ResponseKind::DescribeConfigs(response) if api_key == ApiKey::DescribeConfigs => {
            assert_no_tags!(response);
            for result in &response.results {
                assert_no_tags!(result);
                for config in &result.configs {
                    assert_no_tags!(config);
                    for synonym in &config.synonyms {
                        assert_no_tags!(synonym);
                    }
                }
            }
        }
        _ => panic!("missing flexible response tag audit for {api_key:?} v{version}"),
    }
}

async fn seed_list_groups_fixture(connection: &mut TcpStream, correlation_id: i32) {
    seed_list_group_fixture(
        connection,
        correlation_id,
        "boundary-list-group",
        "consumer",
        "range",
        "boundary-matrix-1",
    )
    .await;
    seed_list_group_fixture(
        connection,
        correlation_id + 2,
        "boundary-list-group-b",
        "connect",
        "roundrobin",
        "boundary-matrix-2",
    )
    .await;
}

async fn seed_list_group_fixture(
    connection: &mut TcpStream,
    correlation_id: i32,
    group_id: &'static str,
    protocol_type: &'static str,
    protocol_name: &'static str,
    member_id: &'static str,
) {
    let initial = RequestKind::JoinGroup(
        JoinGroupRequest::default()
            .with_group_id(GroupId::from(StrBytes::from_static_str(group_id)))
            .with_session_timeout_ms(10_000)
            .with_rebalance_timeout_ms(10_000)
            .with_member_id(StrBytes::new())
            .with_protocol_type(StrBytes::from_static_str(protocol_type))
            .with_protocols(vec![
                JoinGroupRequestProtocol::default()
                    .with_name(StrBytes::from_static_str(protocol_name))
                    .with_metadata(Bytes::new()),
            ]),
    );
    write_frame(
        connection,
        &encode_boundary_request(ApiKey::JoinGroup, 5, correlation_id, &initial, None),
    )
    .await
    .expect("write ListGroups fixture member-id JoinGroup");
    let (header, response) = decode_response_kind(
        ApiKey::JoinGroup,
        5,
        read_response(connection, "ListGroups fixture member-id JoinGroup").await,
    );
    assert_eq!(header.correlation_id, correlation_id);
    let ResponseKind::JoinGroup(response) = response else {
        panic!("expected ListGroups fixture JoinGroup response");
    };
    assert_eq!(response.error_code, ResponseError::MemberIdRequired.code());
    assert_eq!(response.member_id.as_str(), member_id);

    let joined = RequestKind::JoinGroup(
        JoinGroupRequest::default()
            .with_group_id(GroupId::from(StrBytes::from_static_str(group_id)))
            .with_session_timeout_ms(10_000)
            .with_rebalance_timeout_ms(10_000)
            .with_member_id(response.member_id.clone())
            .with_protocol_type(StrBytes::from_static_str(protocol_type))
            .with_protocols(vec![
                JoinGroupRequestProtocol::default()
                    .with_name(StrBytes::from_static_str(protocol_name))
                    .with_metadata(Bytes::new()),
            ]),
    );
    write_frame(
        connection,
        &encode_boundary_request(ApiKey::JoinGroup, 5, correlation_id + 1, &joined, None),
    )
    .await
    .expect("write ListGroups fixture JoinGroup");
    let (header, response) = decode_response_kind(
        ApiKey::JoinGroup,
        5,
        read_response(connection, "ListGroups fixture JoinGroup").await,
    );
    assert_eq!(header.correlation_id, correlation_id + 1);
    let ResponseKind::JoinGroup(response) = response else {
        panic!("expected completed ListGroups fixture JoinGroup response");
    };
    assert_eq!(response.error_code, 0);
    assert_eq!(response.generation_id, 1);
    assert_eq!(
        response.protocol_name,
        Some(StrBytes::from_static_str(protocol_name))
    );
    assert_eq!(response.leader.as_str(), member_id);
    assert_eq!(response.member_id.as_str(), member_id);
    assert_eq!(response.members.len(), 1);
    assert_eq!(response.members[0].member_id.as_str(), member_id);
    assert_eq!(response.members[0].metadata, Bytes::new());
}

async fn assert_api_versions_v4_success(connection: &mut TcpStream, correlation_id: i32) {
    write_frame(
        connection,
        &encode_api_versions_request_for(correlation_id, 4),
    )
    .await
    .expect("write supported ApiVersions v4 after boundary rejection");
    let encoded = read_response(
        connection,
        "supported ApiVersions v4 after boundary rejection",
    )
    .await;
    let (header, response) = decode_response_kind(ApiKey::ApiVersions, 4, encoded);
    assert_eq!(header.correlation_id, correlation_id);
    let ResponseKind::ApiVersions(response) = response else {
        panic!("expected ApiVersions success response");
    };
    assert_eq!(response.error_code, 0);
    assert_eq!(response.api_keys.len(), 17);
}

fn assert_produce_supported(request: &RequestKind, response: &ResponseKind, _: i16) {
    let RequestKind::Produce(request) = request else {
        panic!("expected supported Produce fixture");
    };
    let ResponseKind::Produce(response) = response else {
        panic!("expected supported Produce response");
    };
    assert_eq!(
        request.acks, 2,
        "fixture must exercise validation, not a stub"
    );
    assert_eq!(response.throttle_time_ms, 0);
    assert!(response.node_endpoints.is_empty());
    assert_eq!(response.responses.len(), request.topic_data.len());
    for (actual_topic, expected_topic) in response.responses.iter().zip(&request.topic_data) {
        assert_eq!(actual_topic.name, expected_topic.name);
        assert_eq!(
            actual_topic.partition_responses.len(),
            expected_topic.partition_data.len()
        );
        for (actual_partition, expected_partition) in actual_topic
            .partition_responses
            .iter()
            .zip(&expected_topic.partition_data)
        {
            assert_eq!(actual_partition.index, expected_partition.index);
            assert_eq!(
                actual_partition.error_code,
                ResponseError::InvalidRequiredAcks.code()
            );
            assert_eq!(actual_partition.base_offset, -1);
            assert_eq!(actual_partition.log_append_time_ms, -1);
            assert_eq!(actual_partition.log_start_offset, 0);
            assert!(actual_partition.record_errors.is_empty());
            assert_eq!(actual_partition.error_message, None);
        }
    }
}

fn assert_fetch_supported(request: &RequestKind, response: &ResponseKind, _: i16) {
    let RequestKind::Fetch(request) = request else {
        panic!("expected supported Fetch fixture");
    };
    let ResponseKind::Fetch(response) = response else {
        panic!("expected supported Fetch response");
    };
    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.error_code, 0);
    assert_eq!(response.session_id, 0);
    assert!(response.node_endpoints.is_empty());
    assert_eq!(response.responses.len(), request.topics.len());
    for (actual_topic, expected_topic) in response.responses.iter().zip(&request.topics) {
        assert_eq!(actual_topic.topic, expected_topic.topic);
        assert_eq!(
            actual_topic.partitions.len(),
            expected_topic.partitions.len()
        );
        for (actual_partition, expected_partition) in actual_topic
            .partitions
            .iter()
            .zip(&expected_topic.partitions)
        {
            assert_eq!(
                actual_partition.partition_index,
                expected_partition.partition
            );
            assert_eq!(
                actual_partition.error_code,
                ResponseError::UnknownTopicOrPartition.code()
            );
            assert_eq!(actual_partition.high_watermark, -1);
            assert_eq!(actual_partition.last_stable_offset, -1);
            assert_eq!(actual_partition.log_start_offset, -1);
            assert_eq!(actual_partition.diverging_epoch.epoch, -1);
            assert_eq!(actual_partition.diverging_epoch.end_offset, -1);
            assert_eq!(
                actual_partition.current_leader.leader_id,
                BrokerId::from(-1)
            );
            assert_eq!(actual_partition.current_leader.leader_epoch, -1);
            assert_eq!(actual_partition.snapshot_id.end_offset, -1);
            assert_eq!(actual_partition.snapshot_id.epoch, -1);
            assert_eq!(actual_partition.aborted_transactions, Some(Vec::new()));
            assert_eq!(actual_partition.preferred_read_replica, BrokerId::from(-1));
            assert_eq!(actual_partition.records, Some(Bytes::new()));
        }
    }
}

fn assert_list_offsets_supported(request: &RequestKind, response: &ResponseKind, _: i16) {
    let RequestKind::ListOffsets(request) = request else {
        panic!("expected supported ListOffsets fixture");
    };
    let ResponseKind::ListOffsets(response) = response else {
        panic!("expected supported ListOffsets response");
    };
    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.topics.len(), request.topics.len());
    for (actual_topic, expected_topic) in response.topics.iter().zip(&request.topics) {
        assert_eq!(actual_topic.name, expected_topic.name);
        assert_eq!(
            actual_topic.partitions.len(),
            expected_topic.partitions.len()
        );
        for (actual_partition, expected_partition) in actual_topic
            .partitions
            .iter()
            .zip(&expected_topic.partitions)
        {
            assert_eq!(
                actual_partition.partition_index,
                expected_partition.partition_index
            );
            assert_eq!(
                actual_partition.error_code,
                ResponseError::UnknownTopicOrPartition.code()
            );
            assert_eq!(actual_partition.timestamp, -1);
            assert_eq!(actual_partition.offset, -1);
            assert_eq!(actual_partition.leader_epoch, -1);
        }
    }
}

fn assert_metadata_supported(request: &RequestKind, response: &ResponseKind, version: i16) {
    let RequestKind::Metadata(request) = request else {
        panic!("expected supported Metadata fixture");
    };
    let ResponseKind::Metadata(response) = response else {
        panic!("expected supported Metadata response");
    };
    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.error_code, 0);
    assert_eq!(response.brokers.len(), 1);
    assert_eq!(response.brokers[0].node_id, BrokerId::from(1));
    assert_eq!(response.brokers[0].host.as_str(), "127.0.0.1");
    assert_eq!(response.brokers[0].port, 19_092);
    assert_eq!(
        response.cluster_id,
        Some(StrBytes::from_static_str("memkafka"))
    );
    assert_eq!(response.controller_id, BrokerId::from(1));
    if version >= 8 {
        assert_eq!(response.cluster_authorized_operations, i32::MIN);
    }
    let expected_topics = request.topics.as_deref().expect("explicit Metadata topics");
    assert_eq!(response.topics.len(), expected_topics.len());
    for (actual_topic, expected_topic) in response.topics.iter().zip(expected_topics) {
        assert_eq!(
            actual_topic.error_code,
            ResponseError::UnknownTopicOrPartition.code()
        );
        assert_eq!(actual_topic.name, expected_topic.name);
        assert!(actual_topic.topic_id.is_nil());
        assert!(!actual_topic.is_internal);
        assert!(actual_topic.partitions.is_empty());
        if version >= 8 {
            assert_eq!(actual_topic.topic_authorized_operations, i32::MIN);
        }
    }
}

fn assert_offset_commit_supported(request: &RequestKind, response: &ResponseKind, _: i16) {
    let RequestKind::OffsetCommit(request) = request else {
        panic!("expected supported OffsetCommit fixture");
    };
    let ResponseKind::OffsetCommit(response) = response else {
        panic!("expected supported OffsetCommit response");
    };
    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.topics.len(), request.topics.len());
    for (actual_topic, expected_topic) in response.topics.iter().zip(&request.topics) {
        assert_eq!(actual_topic.name, expected_topic.name);
        assert_eq!(
            actual_topic.partitions.len(),
            expected_topic.partitions.len()
        );
        for (actual_partition, expected_partition) in actual_topic
            .partitions
            .iter()
            .zip(&expected_topic.partitions)
        {
            assert_eq!(
                actual_partition.partition_index,
                expected_partition.partition_index
            );
            assert_eq!(
                actual_partition.error_code,
                ResponseError::UnknownMemberId.code()
            );
        }
    }
}

fn assert_offset_fetch_supported(request: &RequestKind, response: &ResponseKind, version: i16) {
    let RequestKind::OffsetFetch(request) = request else {
        panic!("expected supported OffsetFetch fixture");
    };
    let ResponseKind::OffsetFetch(response) = response else {
        panic!("expected supported OffsetFetch response");
    };
    assert_eq!(version, 5);
    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.error_code, 0);
    assert!(response.groups.is_empty());
    let expected_topics = request.topics.as_deref().expect("OffsetFetch v5 topics");
    assert_eq!(response.topics.len(), expected_topics.len());
    for (actual_topic, expected_topic) in response.topics.iter().zip(expected_topics) {
        assert_eq!(actual_topic.name, expected_topic.name);
        assert_eq!(
            actual_topic.partitions.len(),
            expected_topic.partition_indexes.len()
        );
        for (actual_partition, expected_partition) in actual_topic
            .partitions
            .iter()
            .zip(&expected_topic.partition_indexes)
        {
            assert_eq!(actual_partition.partition_index, *expected_partition);
            assert_eq!(actual_partition.committed_offset, -1);
            assert_eq!(actual_partition.committed_leader_epoch, -1);
            assert_eq!(actual_partition.metadata, None);
            assert_eq!(actual_partition.error_code, 0);
        }
    }
}

fn assert_find_coordinator_supported(request: &RequestKind, response: &ResponseKind, _: i16) {
    let RequestKind::FindCoordinator(request) = request else {
        panic!("expected supported FindCoordinator fixture");
    };
    let ResponseKind::FindCoordinator(response) = response else {
        panic!("expected supported FindCoordinator response");
    };
    assert_eq!(request.key.as_str(), "boundary-supported-coordinator");
    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.error_code, 0);
    assert_eq!(response.error_message, None);
    assert_eq!(response.node_id, BrokerId::from(1));
    assert_eq!(response.host.as_str(), "127.0.0.1");
    assert_eq!(response.port, 19_092);
    assert!(response.coordinators.is_empty());
}

fn assert_join_group_supported(request: &RequestKind, response: &ResponseKind, _: i16) {
    let RequestKind::JoinGroup(request) = request else {
        panic!("expected supported JoinGroup fixture");
    };
    let ResponseKind::JoinGroup(response) = response else {
        panic!("expected supported JoinGroup response");
    };
    assert!(
        request.protocols.is_empty(),
        "fixture must take validation path"
    );
    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(
        response.error_code,
        ResponseError::InconsistentGroupProtocol.code()
    );
    assert_eq!(response.generation_id, -1);
    assert_eq!(response.protocol_type, None);
    assert_eq!(response.protocol_name, Some(StrBytes::new()));
    assert!(response.leader.is_empty());
    assert!(!response.skip_assignment);
    assert!(response.member_id.is_empty());
    assert!(response.members.is_empty());
}

fn assert_heartbeat_supported(request: &RequestKind, response: &ResponseKind, _: i16) {
    let RequestKind::Heartbeat(request) = request else {
        panic!("expected supported Heartbeat fixture");
    };
    let ResponseKind::Heartbeat(response) = response else {
        panic!("expected supported Heartbeat response");
    };
    assert_eq!(request.group_id.0.as_str(), "boundary-supported-heartbeat");
    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.error_code, ResponseError::UnknownMemberId.code());
}

fn assert_leave_group_supported(request: &RequestKind, response: &ResponseKind, version: i16) {
    let RequestKind::LeaveGroup(request) = request else {
        panic!("expected supported LeaveGroup fixture");
    };
    let ResponseKind::LeaveGroup(response) = response else {
        panic!("expected supported LeaveGroup response");
    };
    assert_eq!(response.throttle_time_ms, 0);
    if version <= 2 {
        assert_eq!(response.error_code, ResponseError::UnknownMemberId.code());
        assert!(response.members.is_empty());
    } else {
        assert_eq!(response.error_code, 0);
        assert_eq!(request.members.len(), 2, "supported LeaveGroup v3 fixture");
        assert_eq!(response.members.len(), 2, "supported LeaveGroup v3 results");
        assert_eq!(response.members.len(), request.members.len());
        for (actual_member, expected_member) in response.members.iter().zip(&request.members) {
            assert_eq!(actual_member.member_id, expected_member.member_id);
            assert_eq!(actual_member.group_instance_id, None);
            assert_eq!(
                actual_member.error_code,
                ResponseError::UnknownMemberId.code()
            );
        }
    }
}

fn assert_sync_group_supported(request: &RequestKind, response: &ResponseKind, _: i16) {
    let RequestKind::SyncGroup(request) = request else {
        panic!("expected supported SyncGroup fixture");
    };
    let ResponseKind::SyncGroup(response) = response else {
        panic!("expected supported SyncGroup response");
    };
    assert_eq!(request.group_id.0.as_str(), "boundary-supported-sync");
    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.error_code, ResponseError::UnknownMemberId.code());
    assert_eq!(response.protocol_type, None);
    assert_eq!(response.protocol_name, None);
    assert!(response.assignment.is_empty());
}

fn assert_describe_groups_supported(request: &RequestKind, response: &ResponseKind, _: i16) {
    let RequestKind::DescribeGroups(request) = request else {
        panic!("expected supported DescribeGroups fixture");
    };
    let ResponseKind::DescribeGroups(response) = response else {
        panic!("expected supported DescribeGroups response");
    };
    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.groups.len(), request.groups.len());
    for (actual_group, expected_group) in response.groups.iter().zip(&request.groups) {
        assert_eq!(
            actual_group.error_code,
            ResponseError::GroupIdNotFound.code()
        );
        assert_eq!(actual_group.error_message, None);
        assert_eq!(actual_group.group_id, *expected_group);
        assert!(actual_group.group_state.is_empty());
        assert!(actual_group.protocol_type.is_empty());
        assert!(actual_group.protocol_data.is_empty());
        assert!(actual_group.members.is_empty());
    }
}

fn assert_list_groups_supported(request: &RequestKind, response: &ResponseKind, _: i16) {
    let RequestKind::ListGroups(_) = request else {
        panic!("expected supported ListGroups fixture");
    };
    let ResponseKind::ListGroups(response) = response else {
        panic!("expected supported ListGroups response");
    };
    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.error_code, 0);
    assert_eq!(response.groups.len(), 2, "supported ListGroups results");
    assert_eq!(
        response.groups[0].group_id.0.as_str(),
        "boundary-list-group"
    );
    assert_eq!(response.groups[0].protocol_type.as_str(), "consumer");
    assert_eq!(
        response.groups[1].group_id.0.as_str(),
        "boundary-list-group-b"
    );
    assert_eq!(response.groups[1].protocol_type.as_str(), "connect");
}

fn assert_create_topics_supported(request: &RequestKind, response: &ResponseKind, version: i16) {
    let RequestKind::CreateTopics(request) = request else {
        panic!("expected supported CreateTopics fixture");
    };
    let ResponseKind::CreateTopics(response) = response else {
        panic!("expected supported CreateTopics response");
    };
    assert!(
        request.validate_only,
        "fixture must leave topic state unchanged"
    );
    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.topics.len(), request.topics.len());
    for (actual_topic, expected_topic) in response.topics.iter().zip(&request.topics) {
        assert_eq!(actual_topic.name, expected_topic.name);
        assert!(actual_topic.topic_id.is_nil());
        assert_eq!(actual_topic.error_code, 0);
        assert_eq!(actual_topic.error_message, None);
        assert_eq!(actual_topic.topic_config_error_code, 0);
        assert_eq!(
            actual_topic.num_partitions,
            if version >= 5 {
                expected_topic.num_partitions
            } else {
                -1
            }
        );
        assert_eq!(
            actual_topic.replication_factor,
            if version >= 5 {
                expected_topic.replication_factor
            } else {
                -1
            }
        );
        assert_eq!(actual_topic.configs, Some(Vec::new()));
    }
}

fn assert_init_producer_id_supported(request: &RequestKind, response: &ResponseKind, _: i16) {
    let RequestKind::InitProducerId(request) = request else {
        panic!("expected supported InitProducerId fixture");
    };
    let ResponseKind::InitProducerId(response) = response else {
        panic!("expected supported InitProducerId response");
    };
    assert_eq!(request.transactional_id, None);
    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.error_code, 0);
    assert_eq!(response.producer_id, ProducerId::from(1));
    assert_eq!(response.producer_epoch, 0);
    assert_eq!(response.ongoing_txn_producer_id, ProducerId::from(-1));
    assert_eq!(response.ongoing_txn_producer_epoch, -1);
}

fn assert_describe_configs_supported(request: &RequestKind, response: &ResponseKind, _: i16) {
    let RequestKind::DescribeConfigs(request) = request else {
        panic!("expected supported DescribeConfigs fixture");
    };
    let ResponseKind::DescribeConfigs(response) = response else {
        panic!("expected supported DescribeConfigs response");
    };
    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.results.len(), request.resources.len());
    let expected_errors = [
        ResponseError::UnknownTopicOrPartition.code(),
        ResponseError::BrokerNotAvailable.code(),
    ];
    for ((actual_result, expected_resource), expected_error) in response
        .results
        .iter()
        .zip(&request.resources)
        .zip(expected_errors)
    {
        assert_eq!(actual_result.error_code, expected_error);
        assert_eq!(actual_result.error_message, None);
        assert_eq!(actual_result.resource_type, expected_resource.resource_type);
        assert_eq!(actual_result.resource_name, expected_resource.resource_name);
        assert!(actual_result.configs.is_empty());
    }
}

fn assert_unsupported(error_code: i16) {
    assert_eq!(error_code, ResponseError::UnsupportedVersion.code());
}

fn assert_produce_unsupported(request: &RequestKind, response: &ResponseKind, version: i16) {
    let RequestKind::Produce(request) = request else {
        panic!("expected Produce request fixture");
    };
    let ResponseKind::Produce(response) = response else {
        panic!("expected Produce response");
    };
    assert_eq!(response.throttle_time_ms, 0);
    assert!(response.node_endpoints.is_empty());
    assert_eq!(response.responses.len(), request.topic_data.len());
    for (response_topic, request_topic) in response.responses.iter().zip(&request.topic_data) {
        if version <= 12 {
            assert_eq!(response_topic.name, request_topic.name);
        } else {
            assert_eq!(response_topic.topic_id, request_topic.topic_id);
        }
        assert_eq!(
            response_topic.partition_responses.len(),
            request_topic.partition_data.len()
        );
        for (response_partition, request_partition) in response_topic
            .partition_responses
            .iter()
            .zip(&request_topic.partition_data)
        {
            assert_eq!(response_partition.index, request_partition.index);
            assert_unsupported(response_partition.error_code);
            assert_eq!(response_partition.base_offset, -1);
            assert_eq!(response_partition.log_append_time_ms, -1);
            assert_eq!(response_partition.log_start_offset, -1);
            assert!(response_partition.record_errors.is_empty());
            if version >= 8 {
                assert_eq!(
                    response_partition
                        .error_message
                        .as_ref()
                        .map(StrBytes::as_str),
                    Some("The version of API is not supported.")
                );
            } else {
                assert_eq!(response_partition.error_message, None);
            }
            assert_eq!(
                response_partition.current_leader.leader_id,
                BrokerId::from(-1)
            );
            assert_eq!(response_partition.current_leader.leader_epoch, -1);
        }
    }
}

fn assert_fetch_unsupported(request: &RequestKind, response: &ResponseKind, version: i16) {
    let RequestKind::Fetch(request) = request else {
        panic!("expected Fetch request fixture");
    };
    let ResponseKind::Fetch(response) = response else {
        panic!("expected Fetch response");
    };
    assert_eq!(response.throttle_time_ms, 0);
    if version >= 7 {
        assert_unsupported(response.error_code);
    }
    assert_eq!(response.session_id, 0);
    assert!(response.node_endpoints.is_empty());
    assert_eq!(response.responses.len(), request.topics.len());
    for (response_topic, request_topic) in response.responses.iter().zip(&request.topics) {
        if version <= 12 {
            assert_eq!(response_topic.topic, request_topic.topic);
        } else {
            assert_eq!(response_topic.topic_id, request_topic.topic_id);
        }
        assert_eq!(
            response_topic.partitions.len(),
            request_topic.partitions.len()
        );
        for (response_partition, request_partition) in response_topic
            .partitions
            .iter()
            .zip(&request_topic.partitions)
        {
            assert_eq!(
                response_partition.partition_index,
                request_partition.partition
            );
            assert_unsupported(response_partition.error_code);
            assert_eq!(response_partition.high_watermark, -1);
            assert_eq!(response_partition.last_stable_offset, -1);
            assert_eq!(response_partition.log_start_offset, -1);
            assert_eq!(response_partition.diverging_epoch.epoch, -1);
            assert_eq!(response_partition.diverging_epoch.end_offset, -1);
            assert_eq!(
                response_partition.current_leader.leader_id,
                BrokerId::from(-1)
            );
            assert_eq!(response_partition.current_leader.leader_epoch, -1);
            assert_eq!(response_partition.snapshot_id.end_offset, -1);
            assert_eq!(response_partition.snapshot_id.epoch, -1);
            assert_eq!(response_partition.aborted_transactions, Some(Vec::new()));
            assert_eq!(
                response_partition.preferred_read_replica,
                BrokerId::from(-1)
            );
            assert_eq!(response_partition.records, Some(Bytes::new()));
        }
    }
}

fn assert_list_offsets_unsupported(request: &RequestKind, response: &ResponseKind, _: i16) {
    let RequestKind::ListOffsets(request) = request else {
        panic!("expected ListOffsets request fixture");
    };
    let ResponseKind::ListOffsets(response) = response else {
        panic!("expected ListOffsets response");
    };
    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.topics.len(), request.topics.len());
    for (response_topic, request_topic) in response.topics.iter().zip(&request.topics) {
        assert_eq!(response_topic.name, request_topic.name);
        assert_eq!(
            response_topic.partitions.len(),
            request_topic.partitions.len()
        );
        for (response_partition, request_partition) in response_topic
            .partitions
            .iter()
            .zip(&request_topic.partitions)
        {
            assert_eq!(
                response_partition.partition_index,
                request_partition.partition_index
            );
            assert_unsupported(response_partition.error_code);
            assert_eq!(response_partition.timestamp, -1);
            assert_eq!(response_partition.offset, -1);
            assert_eq!(response_partition.leader_epoch, -1);
        }
    }
}

fn assert_metadata_unsupported(request: &RequestKind, response: &ResponseKind, version: i16) {
    let RequestKind::Metadata(request) = request else {
        panic!("expected Metadata request fixture");
    };
    let ResponseKind::Metadata(response) = response else {
        panic!("expected Metadata response");
    };
    assert_eq!(response.throttle_time_ms, 0);
    assert!(response.brokers.is_empty());
    assert_eq!(response.cluster_id, None);
    assert_eq!(response.controller_id, BrokerId::from(-1));
    if version >= 8 {
        assert_eq!(response.cluster_authorized_operations, i32::MIN);
    }
    if version >= 13 {
        assert_unsupported(response.error_code);
    }
    let requested_topics = request.topics.as_deref().unwrap_or_default();
    assert_eq!(response.topics.len(), requested_topics.len());
    for (response_topic, request_topic) in response.topics.iter().zip(requested_topics) {
        assert_unsupported(response_topic.error_code);
        assert_eq!(response_topic.name, request_topic.name);
        if version >= 10 {
            assert_eq!(response_topic.topic_id, request_topic.topic_id);
        }
        if version >= 8 {
            assert_eq!(response_topic.topic_authorized_operations, i32::MIN);
        }
        assert!(!response_topic.is_internal);
        assert!(response_topic.partitions.is_empty());
    }
}

fn assert_offset_commit_unsupported(request: &RequestKind, response: &ResponseKind, version: i16) {
    let RequestKind::OffsetCommit(request) = request else {
        panic!("expected OffsetCommit request fixture");
    };
    let ResponseKind::OffsetCommit(response) = response else {
        panic!("expected OffsetCommit response");
    };
    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.topics.len(), request.topics.len());
    for (response_topic, request_topic) in response.topics.iter().zip(&request.topics) {
        if version <= 9 {
            assert_eq!(response_topic.name, request_topic.name);
        } else {
            assert_eq!(response_topic.topic_id, request_topic.topic_id);
        }
        assert_eq!(
            response_topic.partitions.len(),
            request_topic.partitions.len()
        );
        for (response_partition, request_partition) in response_topic
            .partitions
            .iter()
            .zip(&request_topic.partitions)
        {
            assert_eq!(
                response_partition.partition_index,
                request_partition.partition_index
            );
            assert_unsupported(response_partition.error_code);
        }
    }
}

fn assert_offset_fetch_unsupported(request: &RequestKind, response: &ResponseKind, version: i16) {
    let RequestKind::OffsetFetch(request) = request else {
        panic!("expected OffsetFetch request fixture");
    };
    let ResponseKind::OffsetFetch(response) = response else {
        panic!("expected OffsetFetch response");
    };
    assert_eq!(response.throttle_time_ms, 0);
    if version <= 7 {
        if version >= 2 {
            assert_unsupported(response.error_code);
            assert!(response.topics.is_empty());
            assert!(response.groups.is_empty());
            return;
        }
        assert!(response.groups.is_empty());
        let requested_topics = request.topics.as_deref().unwrap_or_default();
        assert_eq!(response.topics.len(), requested_topics.len());
        for (response_topic, request_topic) in response.topics.iter().zip(requested_topics) {
            assert_eq!(response_topic.name, request_topic.name);
            assert_eq!(
                response_topic.partitions.len(),
                request_topic.partition_indexes.len()
            );
            for (response_partition, request_partition) in response_topic
                .partitions
                .iter()
                .zip(&request_topic.partition_indexes)
            {
                assert_eq!(response_partition.partition_index, *request_partition);
                assert_eq!(response_partition.committed_offset, -1);
                assert_eq!(response_partition.committed_leader_epoch, -1);
                assert_eq!(response_partition.metadata, None);
                assert_unsupported(response_partition.error_code);
            }
        }
        return;
    }

    assert!(response.topics.is_empty());
    assert_eq!(response.groups.len(), request.groups.len());
    for (response_group, request_group) in response.groups.iter().zip(&request.groups) {
        assert_eq!(response_group.group_id, request_group.group_id);
        assert_unsupported(response_group.error_code);
        let requested_topics = request_group.topics.as_deref().unwrap_or_default();
        assert_eq!(response_group.topics.len(), requested_topics.len());
        for (response_topic, request_topic) in response_group.topics.iter().zip(requested_topics) {
            if version <= 9 {
                assert_eq!(response_topic.name, request_topic.name);
            } else {
                assert_eq!(response_topic.topic_id, request_topic.topic_id);
            }
            assert_eq!(
                response_topic.partitions.len(),
                request_topic.partition_indexes.len()
            );
            for (response_partition, request_partition) in response_topic
                .partitions
                .iter()
                .zip(&request_topic.partition_indexes)
            {
                assert_eq!(response_partition.partition_index, *request_partition);
                assert_eq!(response_partition.committed_offset, -1);
                assert_eq!(response_partition.committed_leader_epoch, -1);
                assert_eq!(response_partition.metadata, None);
                assert_unsupported(response_partition.error_code);
            }
        }
    }
}

fn assert_find_coordinator_unsupported(
    request: &RequestKind,
    response: &ResponseKind,
    version: i16,
) {
    let RequestKind::FindCoordinator(request) = request else {
        panic!("expected FindCoordinator request fixture");
    };
    let ResponseKind::FindCoordinator(response) = response else {
        panic!("expected FindCoordinator response");
    };
    assert_eq!(response.throttle_time_ms, 0);
    if version <= 3 {
        assert_unsupported(response.error_code);
        if version == 0 {
            assert_eq!(response.error_message, Some(StrBytes::new()));
        } else {
            assert_eq!(
                response.error_message.as_ref().map(StrBytes::as_str),
                Some("The version of API is not supported.")
            );
        }
        assert_eq!(response.node_id, BrokerId::from(-1));
        assert!(response.host.is_empty());
        assert_eq!(response.port, -1);
        assert!(response.coordinators.is_empty());
    } else {
        assert_eq!(response.coordinators.len(), request.coordinator_keys.len());
        for (response_coordinator, request_key) in
            response.coordinators.iter().zip(&request.coordinator_keys)
        {
            assert_eq!(response_coordinator.key, *request_key);
            assert_eq!(response_coordinator.node_id, BrokerId::from(-1));
            assert!(response_coordinator.host.is_empty());
            assert_eq!(response_coordinator.port, -1);
            assert_unsupported(response_coordinator.error_code);
            assert_eq!(
                response_coordinator
                    .error_message
                    .as_ref()
                    .map(StrBytes::as_str),
                Some("The version of API is not supported.")
            );
        }
    }
}

fn assert_join_group_unsupported(request: &RequestKind, response: &ResponseKind, _: i16) {
    let RequestKind::JoinGroup(_) = request else {
        panic!("expected JoinGroup request fixture");
    };
    let ResponseKind::JoinGroup(response) = response else {
        panic!("expected JoinGroup response");
    };
    assert_eq!(response.throttle_time_ms, 0);
    assert_unsupported(response.error_code);
    assert_eq!(response.generation_id, -1);
    assert_eq!(response.protocol_type, None);
    assert_eq!(response.protocol_name, Some(StrBytes::new()));
    assert!(response.leader.is_empty());
    assert!(!response.skip_assignment);
    assert!(response.member_id.is_empty());
    assert!(response.members.is_empty());
}

fn assert_heartbeat_unsupported(request: &RequestKind, response: &ResponseKind, _: i16) {
    let RequestKind::Heartbeat(_) = request else {
        panic!("expected Heartbeat request fixture");
    };
    let ResponseKind::Heartbeat(response) = response else {
        panic!("expected Heartbeat response");
    };
    assert_eq!(response.throttle_time_ms, 0);
    assert_unsupported(response.error_code);
}

fn assert_leave_group_unsupported(request: &RequestKind, response: &ResponseKind, _: i16) {
    let RequestKind::LeaveGroup(_) = request else {
        panic!("expected LeaveGroup request fixture");
    };
    let ResponseKind::LeaveGroup(response) = response else {
        panic!("expected LeaveGroup response");
    };
    assert_eq!(response.throttle_time_ms, 0);
    assert_unsupported(response.error_code);
    assert!(response.members.is_empty());
}

fn assert_sync_group_unsupported(request: &RequestKind, response: &ResponseKind, _: i16) {
    let RequestKind::SyncGroup(_) = request else {
        panic!("expected SyncGroup request fixture");
    };
    let ResponseKind::SyncGroup(response) = response else {
        panic!("expected SyncGroup response");
    };
    assert_eq!(response.throttle_time_ms, 0);
    assert_unsupported(response.error_code);
    assert_eq!(response.protocol_type, None);
    assert_eq!(response.protocol_name, None);
    assert!(response.assignment.is_empty());
}

fn assert_describe_groups_unsupported(request: &RequestKind, response: &ResponseKind, _: i16) {
    let RequestKind::DescribeGroups(request) = request else {
        panic!("expected DescribeGroups request fixture");
    };
    let ResponseKind::DescribeGroups(response) = response else {
        panic!("expected DescribeGroups response");
    };
    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.groups.len(), request.groups.len());
    for (response_group, request_group) in response.groups.iter().zip(&request.groups) {
        assert_unsupported(response_group.error_code);
        assert_eq!(response_group.error_message, None);
        assert_eq!(response_group.group_id, *request_group);
        assert!(response_group.group_state.is_empty());
        assert!(response_group.protocol_type.is_empty());
        assert!(response_group.protocol_data.is_empty());
        assert!(response_group.members.is_empty());
    }
}

fn assert_list_groups_unsupported(request: &RequestKind, response: &ResponseKind, _: i16) {
    let RequestKind::ListGroups(_) = request else {
        panic!("expected ListGroups request fixture");
    };
    let ResponseKind::ListGroups(response) = response else {
        panic!("expected ListGroups response");
    };
    assert_eq!(response.throttle_time_ms, 0);
    assert_unsupported(response.error_code);
    assert!(response.groups.is_empty());
}

fn assert_api_versions_supported(request: &RequestKind, response: &ResponseKind, _: i16) {
    let RequestKind::ApiVersions(_) = request else {
        panic!("expected ApiVersions request fixture");
    };
    let ResponseKind::ApiVersions(response) = response else {
        panic!("expected ApiVersions response");
    };
    assert_eq!(response.error_code, 0);
    assert_eq!(response.throttle_time_ms, 0);
    assert!(response.unknown_tagged_fields.is_empty());
    assert!(response.supported_features.is_empty());
    assert_eq!(response.finalized_features_epoch, -1);
    assert!(response.finalized_features.is_empty());
    assert!(!response.zk_migration_ready);
    assert_eq!(
        response
            .api_keys
            .iter()
            .map(|api| (api.api_key, api.min_version, api.max_version))
            .collect::<Vec<_>>(),
        vec![
            (ApiKey::Produce as i16, 7, 7),
            (ApiKey::Fetch as i16, 4, 4),
            (ApiKey::ListOffsets as i16, 3, 3),
            (ApiKey::Metadata as i16, 4, 9),
            (ApiKey::OffsetCommit as i16, 7, 7),
            (ApiKey::OffsetFetch as i16, 5, 5),
            (ApiKey::FindCoordinator as i16, 2, 2),
            (ApiKey::JoinGroup as i16, 5, 5),
            (ApiKey::Heartbeat as i16, 3, 3),
            (ApiKey::LeaveGroup as i16, 1, 3),
            (ApiKey::SyncGroup as i16, 3, 3),
            (ApiKey::DescribeGroups as i16, 0, 0),
            (ApiKey::ListGroups as i16, 0, 0),
            (ApiKey::ApiVersions as i16, 3, 4),
            (ApiKey::CreateTopics as i16, 4, 6),
            (ApiKey::InitProducerId as i16, 0, 0),
            (ApiKey::DescribeConfigs as i16, 1, 1),
        ],
        "exact capability advertisement mismatch"
    );
}

fn assert_api_versions_unsupported(request: &RequestKind, response: &ResponseKind, _: i16) {
    let RequestKind::ApiVersions(_) = request else {
        panic!("expected ApiVersions request fixture");
    };
    let ResponseKind::ApiVersions(response) = response else {
        panic!("expected ApiVersions response");
    };
    assert_unsupported(response.error_code);
    assert_eq!(response.api_keys.len(), 1);
    assert_eq!(response.api_keys[0].api_key, ApiKey::ApiVersions as i16);
    assert_eq!(response.api_keys[0].min_version, 0);
    assert_eq!(response.api_keys[0].max_version, 4);
    assert_eq!(response.throttle_time_ms, 0);
    assert!(response.supported_features.is_empty());
    assert_eq!(response.finalized_features_epoch, -1);
    assert!(response.finalized_features.is_empty());
    assert!(!response.zk_migration_ready);
}

fn assert_create_topics_unsupported(request: &RequestKind, response: &ResponseKind, version: i16) {
    let RequestKind::CreateTopics(request) = request else {
        panic!("expected CreateTopics request fixture");
    };
    let ResponseKind::CreateTopics(response) = response else {
        panic!("expected CreateTopics response");
    };
    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.topics.len(), request.topics.len());
    for (response_topic, request_topic) in response.topics.iter().zip(&request.topics) {
        assert_eq!(response_topic.name, request_topic.name);
        assert!(response_topic.topic_id.is_nil());
        assert_unsupported(response_topic.error_code);
        if version >= 1 {
            assert_eq!(
                response_topic.error_message.as_ref().map(StrBytes::as_str),
                Some("The version of API is not supported.")
            );
        } else {
            assert_eq!(response_topic.error_message, None);
        }
        assert_eq!(response_topic.topic_config_error_code, 0);
        assert_eq!(response_topic.num_partitions, -1);
        assert_eq!(response_topic.replication_factor, -1);
        assert_eq!(response_topic.configs, Some(Vec::new()));
    }
}

fn assert_init_producer_id_unsupported(request: &RequestKind, response: &ResponseKind, _: i16) {
    let RequestKind::InitProducerId(_) = request else {
        panic!("expected InitProducerId request fixture");
    };
    let ResponseKind::InitProducerId(response) = response else {
        panic!("expected InitProducerId response");
    };
    assert_eq!(response.throttle_time_ms, 0);
    assert_unsupported(response.error_code);
    assert_eq!(response.producer_id, ProducerId::from(-1));
    assert_eq!(response.producer_epoch, -1);
    assert_eq!(response.ongoing_txn_producer_id, ProducerId::from(-1));
    assert_eq!(response.ongoing_txn_producer_epoch, -1);
}

fn assert_describe_configs_unsupported(request: &RequestKind, response: &ResponseKind, _: i16) {
    let RequestKind::DescribeConfigs(request) = request else {
        panic!("expected DescribeConfigs request fixture");
    };
    let ResponseKind::DescribeConfigs(response) = response else {
        panic!("expected DescribeConfigs response");
    };
    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.results.len(), request.resources.len());
    for (response_result, request_resource) in response.results.iter().zip(&request.resources) {
        assert_unsupported(response_result.error_code);
        assert_eq!(
            response_result.error_message.as_ref().map(StrBytes::as_str),
            Some("The version of API is not supported.")
        );
        assert_eq!(
            response_result.resource_type,
            request_resource.resource_type
        );
        assert_eq!(
            response_result.resource_name,
            request_resource.resource_name
        );
        assert!(response_result.configs.is_empty());
    }
}

fn ephemeral_config() -> Config {
    Config::try_from(
        Cli::try_parse_from([
            "memkafka",
            "--kafka-listen",
            "127.0.0.1:0",
            "--kafka-advertised-address",
            "127.0.0.1:19092",
            "--schema-registry-listen",
            "127.0.0.1:0",
        ])
        .expect("parse test configuration"),
    )
    .expect("build test configuration")
}

struct SpawnedServer {
    kafka: std::net::SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<anyhow::Result<()>>>,
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
                task.abort();
                let server_result = timeout(Duration::from_secs(1), &mut task).await;
                panic!(
                    "server did not become ready: ready={ready_result:?}, server={server_result:?}"
                );
            }
        };
        Self {
            kafka: endpoints.kafka,
            shutdown: Some(shutdown_tx),
            task: Some(task),
        }
    }

    async fn connect(&self) -> TcpStream {
        timeout(Duration::from_secs(1), TcpStream::connect(self.kafka))
            .await
            .expect("Kafka connection timed out")
            .expect("connect to Kafka endpoint")
    }

    async fn shutdown(mut self) {
        self.shutdown
            .take()
            .expect("server shutdown sender is available")
            .send(())
            .expect("request server shutdown");
        timeout(
            Duration::from_secs(1),
            self.task.take().expect("server task is available"),
        )
        .await
        .expect("server shutdown timed out")
        .expect("server task panicked")
        .expect("server returned an error");
    }
}

impl Drop for SpawnedServer {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
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
    let response_header_version = api_key.response_header_version(version);
    let header = ResponseHeader::decode(&mut encoded, response_header_version)
        .unwrap_or_else(|error| panic!("decode {api_key:?} v{version} response header: {error}"));
    if response_header_version == 1 {
        assert!(
            header.unknown_tagged_fields.is_empty(),
            "{api_key:?} v{version} response header unexpectedly contains unknown tags"
        );
    }
    let response = ResponseKind::decode(api_key, &mut encoded, version)
        .unwrap_or_else(|error| panic!("decode {api_key:?} v{version} response body: {error}"));
    assert!(
        encoded.is_empty(),
        "{api_key:?} v{version} response has trailing bytes"
    );
    if response_body_is_flexible(api_key, version) {
        assert_flexible_response_tags_empty(api_key, version, &response);
    }
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
