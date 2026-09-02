use std::{error::Error, fmt};

use bytes::Bytes;
use kafka_protocol::{
    ResponseError,
    messages::{
        ApiKey, ApiVersionsResponse, BrokerId, CreateTopicsResponse, DescribeClusterResponse,
        DescribeConfigsResponse, DescribeGroupsResponse, DescribeTopicPartitionsRequest,
        DescribeTopicPartitionsResponse, FetchResponse, FindCoordinatorResponse, HeartbeatResponse,
        InitProducerIdResponse, JoinGroupResponse, LeaveGroupResponse, ListGroupsResponse,
        ListOffsetsResponse, MetadataResponse, OffsetCommitResponse, OffsetFetchResponse,
        ProduceResponse, ProducerId, RequestKind, ResponseKind, SyncGroupResponse,
        api_versions_response::ApiVersion,
        create_topics_response::CreatableTopicResult,
        describe_configs_response::DescribeConfigsResult,
        describe_groups_response::DescribedGroup,
        describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
        fetch_response::{FetchableTopicResponse, PartitionData},
        find_coordinator_response::Coordinator,
        list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse},
        metadata_response::MetadataResponseTopic,
        offset_commit_response::{OffsetCommitResponsePartition, OffsetCommitResponseTopic},
        offset_fetch_response::{
            OffsetFetchResponseGroup, OffsetFetchResponsePartition, OffsetFetchResponsePartitions,
            OffsetFetchResponseTopic, OffsetFetchResponseTopics,
        },
        produce_response::{PartitionProduceResponse, TopicProduceResponse},
    },
    protocol::StrBytes,
};

use super::codec::DecodedRequest;
use uuid::Uuid;

pub(crate) const ERROR_RESPONSE_API_KEYS: &[ApiKey] = &[
    ApiKey::Produce,
    ApiKey::Fetch,
    ApiKey::ListOffsets,
    ApiKey::Metadata,
    ApiKey::OffsetCommit,
    ApiKey::OffsetFetch,
    ApiKey::FindCoordinator,
    ApiKey::JoinGroup,
    ApiKey::Heartbeat,
    ApiKey::LeaveGroup,
    ApiKey::SyncGroup,
    ApiKey::DescribeGroups,
    ApiKey::ListGroups,
    ApiKey::ApiVersions,
    ApiKey::CreateTopics,
    ApiKey::InitProducerId,
    ApiKey::DescribeConfigs,
    ApiKey::DescribeCluster,
    ApiKey::DescribeTopicPartitions,
];

const UNSUPPORTED_VERSION_MESSAGE: &str = "The version of API is not supported.";

pub(crate) fn unsupported_version(
    request: &DecodedRequest,
) -> Result<ResponseKind, ErrorResponseError> {
    let version = request.header.request_api_version;
    match (request.api_key, &request.body) {
        (ApiKey::Produce, RequestKind::Produce(body)) => {
            Ok(unsupported_produce(body, version).into())
        }
        (ApiKey::Fetch, RequestKind::Fetch(body)) => Ok(unsupported_fetch(body).into()),
        (ApiKey::ListOffsets, RequestKind::ListOffsets(body)) => {
            Ok(unsupported_list_offsets(body).into())
        }
        (ApiKey::Metadata, RequestKind::Metadata(body)) => Ok(unsupported_metadata(body).into()),
        (ApiKey::OffsetCommit, RequestKind::OffsetCommit(body)) => {
            Ok(unsupported_offset_commit(body).into())
        }
        (ApiKey::OffsetFetch, RequestKind::OffsetFetch(body)) => {
            Ok(unsupported_offset_fetch(body, version).into())
        }
        (ApiKey::FindCoordinator, RequestKind::FindCoordinator(body)) => {
            Ok(unsupported_find_coordinator(body, version).into())
        }
        (ApiKey::JoinGroup, RequestKind::JoinGroup(_)) => Ok(unsupported_join_group().into()),
        (ApiKey::Heartbeat, RequestKind::Heartbeat(_)) => Ok(unsupported_heartbeat().into()),
        (ApiKey::LeaveGroup, RequestKind::LeaveGroup(_)) => Ok(unsupported_leave_group().into()),
        (ApiKey::SyncGroup, RequestKind::SyncGroup(_)) => Ok(unsupported_sync_group().into()),
        (ApiKey::DescribeGroups, RequestKind::DescribeGroups(body)) => {
            Ok(unsupported_describe_groups(body).into())
        }
        (ApiKey::ListGroups, RequestKind::ListGroups(_)) => Ok(unsupported_list_groups().into()),
        (ApiKey::ApiVersions, RequestKind::ApiVersions(_)) => Ok(unsupported_api_versions()),
        (ApiKey::CreateTopics, RequestKind::CreateTopics(body)) => {
            Ok(unsupported_create_topics(body).into())
        }
        (ApiKey::InitProducerId, RequestKind::InitProducerId(_)) => {
            Ok(unsupported_init_producer_id().into())
        }
        (ApiKey::DescribeConfigs, RequestKind::DescribeConfigs(body)) => {
            Ok(unsupported_describe_configs(body).into())
        }
        (ApiKey::DescribeCluster, RequestKind::DescribeCluster(_)) => {
            Ok(unsupported_describe_cluster().into())
        }
        (ApiKey::DescribeTopicPartitions, RequestKind::DescribeTopicPartitions(body)) => {
            Ok(unsupported_describe_topic_partitions(body).into())
        }
        _ => Err(ErrorResponseError::BodyMismatch(request.api_key)),
    }
}

pub(crate) fn unsupported_api_versions() -> ResponseKind {
    ApiVersionsResponse::default()
        .with_error_code(unsupported_code())
        .with_api_keys(vec![
            ApiVersion::default()
                .with_api_key(ApiKey::ApiVersions as i16)
                .with_min_version(0)
                .with_max_version(4),
        ])
        .with_throttle_time_ms(0)
        .with_supported_features(Vec::new())
        .with_finalized_features_epoch(-1)
        .with_finalized_features(Vec::new())
        .with_zk_migration_ready(false)
        .into()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ErrorResponseError {
    BodyMismatch(ApiKey),
}

impl fmt::Display for ErrorResponseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BodyMismatch(api_key) => write!(
                formatter,
                "decoded body does not match Kafka API {api_key:?} for an error response"
            ),
        }
    }
}

impl Error for ErrorResponseError {}

fn unsupported_code() -> i16 {
    ResponseError::UnsupportedVersion.code()
}

fn unsupported_describe_cluster() -> DescribeClusterResponse {
    DescribeClusterResponse::default().with_error_code(unsupported_code())
}

fn unsupported_describe_topic_partitions(
    request: &DescribeTopicPartitionsRequest,
) -> DescribeTopicPartitionsResponse {
    DescribeTopicPartitionsResponse::default().with_topics(
        request
            .topics
            .iter()
            .map(|topic| {
                DescribeTopicPartitionsResponseTopic::default()
                    .with_error_code(unsupported_code())
                    .with_name(Some(topic.name.clone()))
                    .with_topic_id(Uuid::nil())
                    .with_is_internal(false)
                    .with_partitions(Vec::new())
            })
            .collect(),
    )
}

fn unsupported_produce(
    request: &kafka_protocol::messages::ProduceRequest,
    version: i16,
) -> ProduceResponse {
    ProduceResponse::default()
        .with_responses(
            request
                .topic_data
                .iter()
                .map(|topic| {
                    TopicProduceResponse::default()
                        .with_name(topic.name.clone())
                        .with_topic_id(topic.topic_id)
                        .with_partition_responses(
                            topic
                                .partition_data
                                .iter()
                                .map(|partition| {
                                    PartitionProduceResponse::default()
                                        .with_index(partition.index)
                                        .with_error_code(unsupported_code())
                                        .with_base_offset(-1)
                                        .with_log_append_time_ms(-1)
                                        .with_log_start_offset(-1)
                                        .with_record_errors(Vec::new())
                                        .with_error_message((version >= 8).then(|| {
                                            StrBytes::from_static_str(UNSUPPORTED_VERSION_MESSAGE)
                                        }))
                                })
                                .collect(),
                        )
                })
                .collect(),
        )
        .with_throttle_time_ms(0)
        .with_node_endpoints(Vec::new())
}

fn unsupported_fetch(request: &kafka_protocol::messages::FetchRequest) -> FetchResponse {
    FetchResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(unsupported_code())
        .with_session_id(0)
        .with_responses(
            request
                .topics
                .iter()
                .map(|topic| {
                    FetchableTopicResponse::default()
                        .with_topic(topic.topic.clone())
                        .with_topic_id(topic.topic_id)
                        .with_partitions(
                            topic
                                .partitions
                                .iter()
                                .map(|partition| {
                                    PartitionData::default()
                                        .with_partition_index(partition.partition)
                                        .with_error_code(unsupported_code())
                                        .with_high_watermark(-1)
                                        .with_last_stable_offset(-1)
                                        .with_log_start_offset(-1)
                                        .with_aborted_transactions(Some(Vec::new()))
                                        .with_records(Some(Bytes::new()))
                                })
                                .collect(),
                        )
                })
                .collect(),
        )
        .with_node_endpoints(Vec::new())
}

fn unsupported_list_offsets(
    request: &kafka_protocol::messages::ListOffsetsRequest,
) -> ListOffsetsResponse {
    ListOffsetsResponse::default()
        .with_throttle_time_ms(0)
        .with_topics(
            request
                .topics
                .iter()
                .map(|topic| {
                    ListOffsetsTopicResponse::default()
                        .with_name(topic.name.clone())
                        .with_partitions(
                            topic
                                .partitions
                                .iter()
                                .map(|partition| {
                                    ListOffsetsPartitionResponse::default()
                                        .with_partition_index(partition.partition_index)
                                        .with_error_code(unsupported_code())
                                        .with_timestamp(-1)
                                        .with_offset(-1)
                                        .with_leader_epoch(-1)
                                })
                                .collect(),
                        )
                })
                .collect(),
        )
}

fn unsupported_metadata(request: &kafka_protocol::messages::MetadataRequest) -> MetadataResponse {
    MetadataResponse::default()
        .with_throttle_time_ms(0)
        .with_brokers(Vec::new())
        .with_cluster_id(None)
        .with_controller_id(BrokerId::from(-1))
        .with_topics(
            request
                .topics
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|topic| {
                    MetadataResponseTopic::default()
                        .with_error_code(unsupported_code())
                        .with_name(topic.name.clone())
                        .with_topic_id(topic.topic_id)
                        .with_is_internal(false)
                        .with_partitions(Vec::new())
                })
                .collect(),
        )
        .with_error_code(unsupported_code())
}

fn unsupported_offset_commit(
    request: &kafka_protocol::messages::OffsetCommitRequest,
) -> OffsetCommitResponse {
    OffsetCommitResponse::default()
        .with_throttle_time_ms(0)
        .with_topics(
            request
                .topics
                .iter()
                .map(|topic| {
                    OffsetCommitResponseTopic::default()
                        .with_name(topic.name.clone())
                        .with_topic_id(topic.topic_id)
                        .with_partitions(
                            topic
                                .partitions
                                .iter()
                                .map(|partition| {
                                    OffsetCommitResponsePartition::default()
                                        .with_partition_index(partition.partition_index)
                                        .with_error_code(unsupported_code())
                                })
                                .collect(),
                        )
                })
                .collect(),
        )
}

fn unsupported_offset_fetch(
    request: &kafka_protocol::messages::OffsetFetchRequest,
    version: i16,
) -> OffsetFetchResponse {
    if version < 2 {
        return OffsetFetchResponse::default()
            .with_throttle_time_ms(0)
            .with_topics(
                request
                    .topics
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .map(|topic| {
                        OffsetFetchResponseTopic::default()
                            .with_name(topic.name.clone())
                            .with_partitions(
                                topic
                                    .partition_indexes
                                    .iter()
                                    .map(|partition_index| {
                                        OffsetFetchResponsePartition::default()
                                            .with_partition_index(*partition_index)
                                            .with_committed_offset(-1)
                                            .with_committed_leader_epoch(-1)
                                            .with_metadata(None)
                                            .with_error_code(unsupported_code())
                                    })
                                    .collect(),
                            )
                    })
                    .collect(),
            )
            .with_error_code(0)
            .with_groups(Vec::new());
    }

    if version <= 7 {
        return OffsetFetchResponse::default()
            .with_throttle_time_ms(0)
            .with_topics(Vec::new())
            .with_error_code(unsupported_code())
            .with_groups(Vec::new());
    }

    OffsetFetchResponse::default()
        .with_throttle_time_ms(0)
        .with_topics(Vec::new())
        .with_groups(
            request
                .groups
                .iter()
                .map(|group| {
                    OffsetFetchResponseGroup::default()
                        .with_group_id(group.group_id.clone())
                        .with_topics(
                            group
                                .topics
                                .as_deref()
                                .unwrap_or_default()
                                .iter()
                                .map(|topic| {
                                    OffsetFetchResponseTopics::default()
                                        .with_name(topic.name.clone())
                                        .with_topic_id(topic.topic_id)
                                        .with_partitions(
                                            topic
                                                .partition_indexes
                                                .iter()
                                                .map(|partition_index| {
                                                    OffsetFetchResponsePartitions::default()
                                                        .with_partition_index(*partition_index)
                                                        .with_committed_offset(-1)
                                                        .with_committed_leader_epoch(-1)
                                                        .with_metadata(None)
                                                        .with_error_code(unsupported_code())
                                                })
                                                .collect(),
                                        )
                                })
                                .collect(),
                        )
                        .with_error_code(unsupported_code())
                })
                .collect(),
        )
}

fn unsupported_find_coordinator(
    request: &kafka_protocol::messages::FindCoordinatorRequest,
    version: i16,
) -> FindCoordinatorResponse {
    if version <= 3 {
        return FindCoordinatorResponse::default()
            .with_throttle_time_ms(0)
            .with_error_code(unsupported_code())
            .with_error_message(Some(if version == 0 {
                StrBytes::new()
            } else {
                StrBytes::from_static_str(UNSUPPORTED_VERSION_MESSAGE)
            }))
            .with_node_id(BrokerId::from(-1))
            .with_host(StrBytes::new())
            .with_port(-1)
            .with_coordinators(Vec::new());
    }

    FindCoordinatorResponse::default()
        .with_throttle_time_ms(0)
        .with_coordinators(
            request
                .coordinator_keys
                .iter()
                .map(|key| {
                    Coordinator::default()
                        .with_key(key.clone())
                        .with_node_id(BrokerId::from(-1))
                        .with_host(StrBytes::new())
                        .with_port(-1)
                        .with_error_code(unsupported_code())
                        .with_error_message(Some(StrBytes::from_static_str(
                            UNSUPPORTED_VERSION_MESSAGE,
                        )))
                })
                .collect(),
        )
}

fn unsupported_join_group() -> JoinGroupResponse {
    JoinGroupResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(unsupported_code())
        .with_generation_id(-1)
        .with_protocol_type(None)
        .with_protocol_name(Some(StrBytes::new()))
        .with_leader(StrBytes::new())
        .with_skip_assignment(false)
        .with_member_id(StrBytes::new())
        .with_members(Vec::new())
}

fn unsupported_heartbeat() -> HeartbeatResponse {
    HeartbeatResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(unsupported_code())
}

fn unsupported_leave_group() -> LeaveGroupResponse {
    LeaveGroupResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(unsupported_code())
        .with_members(Vec::new())
}

fn unsupported_sync_group() -> SyncGroupResponse {
    SyncGroupResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(unsupported_code())
        .with_protocol_type(None)
        .with_protocol_name(None)
        .with_assignment(Bytes::new())
}

fn unsupported_describe_groups(
    request: &kafka_protocol::messages::DescribeGroupsRequest,
) -> DescribeGroupsResponse {
    DescribeGroupsResponse::default()
        .with_throttle_time_ms(0)
        .with_groups(
            request
                .groups
                .iter()
                .map(|group_id| {
                    DescribedGroup::default()
                        .with_error_code(unsupported_code())
                        .with_error_message(None)
                        .with_group_id(group_id.clone())
                        .with_group_state(StrBytes::new())
                        .with_protocol_type(StrBytes::new())
                        .with_protocol_data(StrBytes::new())
                        .with_members(Vec::new())
                })
                .collect(),
        )
}

fn unsupported_list_groups() -> ListGroupsResponse {
    ListGroupsResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(unsupported_code())
        .with_groups(Vec::new())
}

fn unsupported_create_topics(
    request: &kafka_protocol::messages::CreateTopicsRequest,
) -> CreateTopicsResponse {
    CreateTopicsResponse::default()
        .with_throttle_time_ms(0)
        .with_topics(
            request
                .topics
                .iter()
                .map(|topic| {
                    CreatableTopicResult::default()
                        .with_name(topic.name.clone())
                        .with_error_code(unsupported_code())
                        .with_error_message(Some(StrBytes::from_static_str(
                            UNSUPPORTED_VERSION_MESSAGE,
                        )))
                        .with_topic_config_error_code(0)
                        .with_num_partitions(-1)
                        .with_replication_factor(-1)
                        .with_configs(Some(Vec::new()))
                })
                .collect(),
        )
}

fn unsupported_init_producer_id() -> InitProducerIdResponse {
    InitProducerIdResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(unsupported_code())
        .with_producer_id(ProducerId::from(-1))
        .with_producer_epoch(-1)
        .with_ongoing_txn_producer_id(ProducerId::from(-1))
        .with_ongoing_txn_producer_epoch(-1)
}

fn unsupported_describe_configs(
    request: &kafka_protocol::messages::DescribeConfigsRequest,
) -> DescribeConfigsResponse {
    DescribeConfigsResponse::default()
        .with_throttle_time_ms(0)
        .with_results(
            request
                .resources
                .iter()
                .map(|resource| {
                    DescribeConfigsResult::default()
                        .with_error_code(unsupported_code())
                        .with_error_message(Some(StrBytes::from_static_str(
                            UNSUPPORTED_VERSION_MESSAGE,
                        )))
                        .with_resource_type(resource.resource_type)
                        .with_resource_name(resource.resource_name.clone())
                        .with_configs(Vec::new())
                })
                .collect(),
        )
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};
    use kafka_protocol::{
        ResponseError,
        messages::{
            ApiKey, ApiVersionsRequest, BrokerId, CreateTopicsRequest, DescribeClusterRequest,
            DescribeConfigsRequest, DescribeGroupsRequest, DescribeTopicPartitionsRequest,
            FetchRequest, FindCoordinatorRequest, GroupId, HeartbeatRequest, InitProducerIdRequest,
            JoinGroupRequest, LeaveGroupRequest, ListGroupsRequest, ListOffsetsRequest,
            MetadataRequest, OffsetCommitRequest, OffsetFetchRequest, ProduceRequest, ProducerId,
            RequestHeader, RequestKind, ResponseHeader, ResponseKind, SyncGroupRequest, TopicName,
            create_topics_request::CreatableTopic,
            describe_configs_request::DescribeConfigsResource,
            describe_topic_partitions_request::TopicRequest,
            fetch_request::{FetchPartition, FetchTopic},
            leave_group_request::MemberIdentity,
            list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
            metadata_request::MetadataRequestTopic,
            offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
            offset_fetch_request::{
                OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchRequestTopics,
            },
            produce_request::{PartitionProduceData, TopicProduceData},
        },
        protocol::{Decodable, StrBytes},
    };

    use super::{
        ERROR_RESPONSE_API_KEYS, ErrorResponseError, UNSUPPORTED_VERSION_MESSAGE,
        unsupported_api_versions, unsupported_version,
    };
    use crate::kafka::{
        capabilities::{ApiCapability, CAPABILITIES, capability},
        codec::{DecodedRequest, encode_response},
        dispatcher::DISPATCHED_API_KEYS,
    };

    const CORRELATION_ID: i32 = 0x1020_3040;
    const TOPIC_ID: &str = "12345678-1234-5678-9abc-def012345678";
    const TOPIC_ID_2: &str = "22345678-1234-5678-9abc-def012345678";
    const TOPIC_ID_3: &str = "32345678-1234-5678-9abc-def012345678";
    const TOPIC_ID_4: &str = "42345678-1234-5678-9abc-def012345678";

    struct ErrorResponseCase {
        api_key: ApiKey,
        request_for: fn(i16) -> RequestKind,
        extra_versions: &'static [i16],
        assert_shape: fn(&ResponseKind, i16),
    }

    #[test]
    fn error_response_shapes_round_trip_for_all_advertised_apis() {
        for case in cases() {
            let capability = capability(case.api_key).expect("advertised capability");
            let versions = tested_unsupported_versions(capability, case.extra_versions);
            assert!(
                !versions.is_empty(),
                "{:?} needs an unsupported schema-known test version",
                case.api_key
            );

            for version in versions {
                let body = round_trip_request(case.api_key, version, (case.request_for)(version));
                let request = DecodedRequest {
                    header: RequestHeader::default()
                        .with_request_api_key(case.api_key as i16)
                        .with_request_api_version(version)
                        .with_correlation_id(CORRELATION_ID),
                    api_key: case.api_key,
                    body,
                };

                let response = unsupported_version(&request).unwrap_or_else(|error| {
                    panic!(
                        "build {:?} v{version} unsupported response: {error}",
                        case.api_key
                    )
                });
                let decoded = round_trip_response(case.api_key, version, &response);
                (case.assert_shape)(&decoded, version);
            }
        }
    }

    #[test]
    fn unsupported_api_versions_reports_kafka_4_3_api_versions_range() {
        let response = unsupported_api_versions();
        assert_api_versions(&response, 0);
        let decoded = round_trip_response(ApiKey::ApiVersions, 0, &response);
        assert_api_versions(&decoded, 0);
    }

    #[test]
    fn constructed_describe_topic_partitions_error_preserves_raw_names_and_duplicates() {
        let request = DecodedRequest {
            header: RequestHeader::default()
                .with_request_api_key(ApiKey::DescribeTopicPartitions as i16)
                .with_request_api_version(0),
            api_key: ApiKey::DescribeTopicPartitions,
            body: RequestKind::DescribeTopicPartitions(
                DescribeTopicPartitionsRequest::default().with_topics(vec![
                    TopicRequest::default().with_name(topic_name("bravo")),
                    TopicRequest::default().with_name(topic_name("alpha")),
                    TopicRequest::default().with_name(topic_name("bravo")),
                ]),
            ),
        };

        let response = unsupported_version(&request).expect("build constructed error response");
        let ResponseKind::DescribeTopicPartitions(response) = response else {
            panic!("expected DescribeTopicPartitions response");
        };
        assert_eq!(response.topics.len(), 3);
        assert_eq!(response.next_cursor, None);
        assert_eq!(
            response
                .topics
                .iter()
                .map(|topic| topic.name.as_ref().expect("raw name").as_str())
                .collect::<Vec<_>>(),
            vec!["bravo", "alpha", "bravo"]
        );
        for topic in response.topics {
            assert_eq!(topic.error_code, ResponseError::UnsupportedVersion.code());
            assert!(topic.topic_id.is_nil());
            assert!(!topic.is_internal);
            assert!(topic.partitions.is_empty());
        }
    }

    #[test]
    fn error_response_reports_api_and_body_mismatches() {
        let mismatch = DecodedRequest {
            header: RequestHeader::default().with_request_api_version(3),
            api_key: ApiKey::Metadata,
            body: RequestKind::ApiVersions(ApiVersionsRequest::default()),
        };
        assert_eq!(
            unsupported_version(&mismatch),
            Err(ErrorResponseError::BodyMismatch(ApiKey::Metadata))
        );

        let unadvertised = DecodedRequest {
            header: RequestHeader::default().with_request_api_version(5),
            api_key: ApiKey::DeleteTopics,
            body: RequestKind::DeleteTopics(Default::default()),
        };
        assert_eq!(
            unsupported_version(&unadvertised),
            Err(ErrorResponseError::BodyMismatch(ApiKey::DeleteTopics))
        );
    }

    #[test]
    fn advertised_dispatch_and_error_response_coverage_sets_are_equal() {
        let mut capability_keys = CAPABILITIES
            .iter()
            .map(|capability| capability.api_key as i16)
            .collect::<Vec<_>>();
        let mut dispatch_keys = DISPATCHED_API_KEYS
            .iter()
            .map(|api_key| *api_key as i16)
            .collect::<Vec<_>>();
        let mut error_response_keys = ERROR_RESPONSE_API_KEYS
            .iter()
            .map(|api_key| *api_key as i16)
            .collect::<Vec<_>>();
        capability_keys.sort_unstable();
        dispatch_keys.sort_unstable();
        error_response_keys.sort_unstable();

        assert_eq!(
            dispatch_keys, capability_keys,
            "dispatcher coverage drifted"
        );
        assert_eq!(
            error_response_keys, capability_keys,
            "unsupported response coverage drifted"
        );
    }

    #[test]
    fn unsupported_acks_zero_produce_does_not_expect_a_response() {
        let request = DecodedRequest {
            header: RequestHeader::default()
                .with_request_api_key(ApiKey::Produce as i16)
                .with_request_api_version(8),
            api_key: ApiKey::Produce,
            body: RequestKind::Produce(produce_request(0)),
        };

        assert!(!request.expects_response());
    }

    fn cases() -> [ErrorResponseCase; 18] {
        [
            ErrorResponseCase {
                api_key: ApiKey::Produce,
                request_for: |_: i16| RequestKind::Produce(produce_request(1)),
                extra_versions: &[3, 13],
                assert_shape: assert_produce,
            },
            ErrorResponseCase {
                api_key: ApiKey::Fetch,
                request_for: fetch_request,
                extra_versions: &[6, 18],
                assert_shape: assert_fetch,
            },
            ErrorResponseCase {
                api_key: ApiKey::ListOffsets,
                request_for: list_offsets_request,
                extra_versions: &[1, 11],
                assert_shape: assert_list_offsets,
            },
            ErrorResponseCase {
                api_key: ApiKey::Metadata,
                request_for: metadata_request,
                extra_versions: &[0, 13],
                assert_shape: assert_metadata,
            },
            ErrorResponseCase {
                api_key: ApiKey::OffsetCommit,
                request_for: offset_commit_request,
                extra_versions: &[2, 10],
                assert_shape: assert_offset_commit,
            },
            ErrorResponseCase {
                api_key: ApiKey::OffsetFetch,
                request_for: offset_fetch_request,
                extra_versions: &[1, 8, 10],
                assert_shape: assert_offset_fetch,
            },
            ErrorResponseCase {
                api_key: ApiKey::FindCoordinator,
                request_for: find_coordinator_request,
                extra_versions: &[0, 4, 6],
                assert_shape: assert_find_coordinator,
            },
            ErrorResponseCase {
                api_key: ApiKey::JoinGroup,
                request_for: join_group_request,
                extra_versions: &[0, 9],
                assert_shape: assert_join_group,
            },
            ErrorResponseCase {
                api_key: ApiKey::Heartbeat,
                request_for: heartbeat_request,
                extra_versions: &[0],
                assert_shape: assert_heartbeat,
            },
            ErrorResponseCase {
                api_key: ApiKey::LeaveGroup,
                request_for: leave_group_request,
                extra_versions: &[0, 5],
                assert_shape: assert_leave_group,
            },
            ErrorResponseCase {
                api_key: ApiKey::SyncGroup,
                request_for: sync_group_request,
                extra_versions: &[0, 5],
                assert_shape: assert_sync_group,
            },
            ErrorResponseCase {
                api_key: ApiKey::DescribeGroups,
                request_for: describe_groups_request,
                extra_versions: &[6],
                assert_shape: assert_describe_groups,
            },
            ErrorResponseCase {
                api_key: ApiKey::ListGroups,
                request_for: list_groups_request,
                extra_versions: &[5],
                assert_shape: assert_list_groups,
            },
            ErrorResponseCase {
                api_key: ApiKey::ApiVersions,
                request_for: api_versions_request,
                extra_versions: &[0],
                assert_shape: assert_api_versions,
            },
            ErrorResponseCase {
                api_key: ApiKey::CreateTopics,
                request_for: create_topics_request,
                extra_versions: &[2],
                assert_shape: assert_create_topics,
            },
            ErrorResponseCase {
                api_key: ApiKey::InitProducerId,
                request_for: init_producer_id_request,
                extra_versions: &[6],
                assert_shape: assert_init_producer_id,
            },
            ErrorResponseCase {
                api_key: ApiKey::DescribeConfigs,
                request_for: describe_configs_request,
                extra_versions: &[4],
                assert_shape: assert_describe_configs,
            },
            ErrorResponseCase {
                api_key: ApiKey::DescribeCluster,
                request_for: describe_cluster_request,
                extra_versions: &[0],
                assert_shape: assert_describe_cluster,
            },
        ]
    }

    fn tested_unsupported_versions(capability: &ApiCapability, extra_versions: &[i16]) -> Vec<i16> {
        let mut versions = adjacent_unsupported(capability);
        versions.extend(extra_versions.iter().copied().filter(|version| {
            capability.kafka_4_3.min <= *version
                && *version <= capability.kafka_4_3.max
                && !capability.supports(*version)
        }));
        versions.sort_unstable();
        versions.dedup();
        versions
    }

    fn adjacent_unsupported(capability: &ApiCapability) -> Vec<i16> {
        [capability.supported.min - 1, capability.supported.max + 1]
            .into_iter()
            .filter(|version| {
                capability.kafka_4_3.min <= *version
                    && *version <= capability.kafka_4_3.max
                    && !capability.supports(*version)
            })
            .collect()
    }

    fn round_trip_request(api_key: ApiKey, version: i16, request: RequestKind) -> RequestKind {
        let mut encoded = BytesMut::new();
        request
            .encode(&mut encoded, version)
            .unwrap_or_else(|error| panic!("encode {api_key:?} v{version} request: {error}"));
        let mut encoded = encoded.freeze();
        let decoded = RequestKind::decode(api_key, &mut encoded, version)
            .unwrap_or_else(|error| panic!("decode {api_key:?} v{version} request: {error}"));
        assert!(encoded.is_empty(), "request body has trailing bytes");
        decoded
    }

    fn round_trip_response(api_key: ApiKey, version: i16, response: &ResponseKind) -> ResponseKind {
        let mut encoded = encode_response(api_key, version, CORRELATION_ID, response)
            .unwrap_or_else(|error| panic!("encode {api_key:?} v{version} response: {error}"));
        let header = ResponseHeader::decode(&mut encoded, api_key.response_header_version(version))
            .unwrap_or_else(|error| {
                panic!("decode {api_key:?} v{version} response header: {error}")
            });
        assert_eq!(header.correlation_id, CORRELATION_ID);
        let decoded = ResponseKind::decode(api_key, &mut encoded, version)
            .unwrap_or_else(|error| panic!("decode {api_key:?} v{version} response: {error}"));
        assert!(encoded.is_empty(), "response body has trailing bytes");
        decoded
    }

    fn topic_name(value: &'static str) -> TopicName {
        TopicName::from(StrBytes::from_static_str(value))
    }

    fn group_id(value: &'static str) -> GroupId {
        GroupId::from(StrBytes::from_static_str(value))
    }

    fn produce_request(acks: i16) -> ProduceRequest {
        ProduceRequest::default()
            .with_acks(acks)
            .with_timeout_ms(1_234)
            .with_topic_data(vec![
                TopicProduceData::default()
                    .with_name(topic_name("produce-topic"))
                    .with_topic_id(TOPIC_ID.parse().expect("valid topic UUID"))
                    .with_partition_data(vec![
                        PartitionProduceData::default()
                            .with_index(7)
                            .with_records(Some(Bytes::from_static(b"records-must-not-echo"))),
                        PartitionProduceData::default()
                            .with_index(17)
                            .with_records(Some(Bytes::from_static(
                                b"second-records-must-not-echo",
                            ))),
                    ]),
                TopicProduceData::default()
                    .with_name(topic_name("produce-topic-2"))
                    .with_topic_id(TOPIC_ID_2.parse().expect("valid topic UUID"))
                    .with_partition_data(vec![
                        PartitionProduceData::default()
                            .with_index(27)
                            .with_records(Some(Bytes::from_static(b"third-records-must-not-echo"))),
                        PartitionProduceData::default()
                            .with_index(37)
                            .with_records(Some(Bytes::from_static(
                                b"fourth-records-must-not-echo",
                            ))),
                    ]),
            ])
    }

    fn fetch_request(_: i16) -> RequestKind {
        RequestKind::Fetch(
            FetchRequest::default()
                .with_replica_id(BrokerId::from(-1))
                .with_max_wait_ms(1_234)
                .with_min_bytes(1)
                .with_max_bytes(4_096)
                .with_session_id(99)
                .with_session_epoch(4)
                .with_topics(vec![
                    FetchTopic::default()
                        .with_topic(topic_name("fetch-topic"))
                        .with_topic_id(TOPIC_ID.parse().expect("valid topic UUID"))
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
                        .with_topic(topic_name("fetch-topic-2"))
                        .with_topic_id(TOPIC_ID_2.parse().expect("valid topic UUID"))
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

    fn list_offsets_request(_: i16) -> RequestKind {
        RequestKind::ListOffsets(
            ListOffsetsRequest::default()
                .with_replica_id(BrokerId::from(-1))
                .with_topics(vec![
                    ListOffsetsTopic::default()
                        .with_name(topic_name("offsets-topic"))
                        .with_partitions(vec![
                            ListOffsetsPartition::default()
                                .with_partition_index(9)
                                .with_timestamp(1_725_000_000_000),
                            ListOffsetsPartition::default()
                                .with_partition_index(19)
                                .with_timestamp(1_725_000_000_001),
                        ]),
                    ListOffsetsTopic::default()
                        .with_name(topic_name("offsets-topic-2"))
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

    fn metadata_request(_: i16) -> RequestKind {
        RequestKind::Metadata(MetadataRequest::default().with_topics(Some(vec![
            MetadataRequestTopic::default()
                .with_name(Some(topic_name("metadata-topic")))
                .with_topic_id(TOPIC_ID.parse().expect("valid topic UUID")),
            MetadataRequestTopic::default()
                .with_name(Some(topic_name("metadata-topic-2")))
                .with_topic_id(TOPIC_ID_2.parse().expect("valid topic UUID")),
        ])))
    }

    fn offset_commit_request(_: i16) -> RequestKind {
        RequestKind::OffsetCommit(
            OffsetCommitRequest::default()
                .with_group_id(group_id("commit-group"))
                .with_member_id(StrBytes::from_static_str("commit-member"))
                .with_topics(vec![
                    OffsetCommitRequestTopic::default()
                        .with_name(topic_name("commit-topic"))
                        .with_topic_id(TOPIC_ID.parse().expect("valid topic UUID"))
                        .with_partitions(vec![
                            OffsetCommitRequestPartition::default()
                                .with_partition_index(10)
                                .with_committed_offset(987)
                                .with_committed_metadata(Some(StrBytes::from_static_str(
                                    "metadata-must-not-echo",
                                ))),
                            OffsetCommitRequestPartition::default()
                                .with_partition_index(20)
                                .with_committed_offset(988)
                                .with_committed_metadata(Some(StrBytes::from_static_str(
                                    "second-metadata-must-not-echo",
                                ))),
                        ]),
                    OffsetCommitRequestTopic::default()
                        .with_name(topic_name("commit-topic-2"))
                        .with_topic_id(TOPIC_ID_2.parse().expect("valid topic UUID"))
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

    fn offset_fetch_request(version: i16) -> RequestKind {
        let mut request = OffsetFetchRequest::default();
        if version <= 7 {
            request = request
                .with_group_id(group_id("fetch-offset-group"))
                .with_topics(Some(vec![
                    OffsetFetchRequestTopic::default()
                        .with_name(topic_name("fetch-offset-topic"))
                        .with_partition_indexes(vec![11, 21]),
                    OffsetFetchRequestTopic::default()
                        .with_name(topic_name("fetch-offset-topic-2"))
                        .with_partition_indexes(vec![31, 41]),
                ]));
        } else {
            request = request.with_groups(vec![
                OffsetFetchRequestGroup::default()
                    .with_group_id(group_id("fetch-offset-group-v8"))
                    .with_topics(Some(vec![
                        OffsetFetchRequestTopics::default()
                            .with_name(topic_name("fetch-offset-topic-v8"))
                            .with_topic_id(TOPIC_ID.parse().expect("valid topic UUID"))
                            .with_partition_indexes(vec![12, 22]),
                        OffsetFetchRequestTopics::default()
                            .with_name(topic_name("fetch-offset-topic-v8-2"))
                            .with_topic_id(TOPIC_ID_2.parse().expect("valid topic UUID"))
                            .with_partition_indexes(vec![32, 42]),
                    ])),
                OffsetFetchRequestGroup::default()
                    .with_group_id(group_id("fetch-offset-group-v8-2"))
                    .with_topics(Some(vec![
                        OffsetFetchRequestTopics::default()
                            .with_name(topic_name("fetch-offset-topic-v8-3"))
                            .with_topic_id(TOPIC_ID_3.parse().expect("valid topic UUID"))
                            .with_partition_indexes(vec![52, 62]),
                        OffsetFetchRequestTopics::default()
                            .with_name(topic_name("fetch-offset-topic-v8-4"))
                            .with_topic_id(TOPIC_ID_4.parse().expect("valid topic UUID"))
                            .with_partition_indexes(vec![72, 82]),
                    ])),
            ]);
        }
        RequestKind::OffsetFetch(request)
    }

    fn find_coordinator_request(version: i16) -> RequestKind {
        let mut request = FindCoordinatorRequest::default();
        if version <= 3 {
            request = request.with_key(StrBytes::from_static_str("coordinator-key"));
        } else {
            request = request.with_coordinator_keys(vec![
                StrBytes::from_static_str("coordinator-key-v4"),
                StrBytes::from_static_str("coordinator-key-v4-2"),
            ]);
        }
        RequestKind::FindCoordinator(request)
    }

    fn join_group_request(_: i16) -> RequestKind {
        RequestKind::JoinGroup(
            JoinGroupRequest::default()
                .with_group_id(group_id("join-group"))
                .with_member_id(StrBytes::from_static_str("join-member"))
                .with_protocol_type(StrBytes::from_static_str("consumer")),
        )
    }

    fn heartbeat_request(_: i16) -> RequestKind {
        RequestKind::Heartbeat(
            HeartbeatRequest::default()
                .with_group_id(group_id("heartbeat-group"))
                .with_member_id(StrBytes::from_static_str("heartbeat-member")),
        )
    }

    fn leave_group_request(version: i16) -> RequestKind {
        let mut request = LeaveGroupRequest::default().with_group_id(group_id("leave-group"));
        if version <= 2 {
            request = request.with_member_id(StrBytes::from_static_str("leave-member"));
        } else {
            request = request.with_members(vec![
                MemberIdentity::default()
                    .with_member_id(StrBytes::from_static_str("leave-member-v3"))
                    .with_group_instance_id(Some(StrBytes::from_static_str("leave-instance-v3"))),
                MemberIdentity::default()
                    .with_member_id(StrBytes::from_static_str("leave-member-v3-2"))
                    .with_group_instance_id(Some(StrBytes::from_static_str("leave-instance-v3-2"))),
            ]);
        }
        RequestKind::LeaveGroup(request)
    }

    fn sync_group_request(_: i16) -> RequestKind {
        RequestKind::SyncGroup(
            SyncGroupRequest::default()
                .with_group_id(group_id("sync-group"))
                .with_member_id(StrBytes::from_static_str("sync-member")),
        )
    }

    fn describe_groups_request(_: i16) -> RequestKind {
        RequestKind::DescribeGroups(DescribeGroupsRequest::default().with_groups(vec![
            group_id("described-group"),
            group_id("described-group-2"),
        ]))
    }

    fn list_groups_request(_: i16) -> RequestKind {
        RequestKind::ListGroups(ListGroupsRequest::default())
    }

    fn api_versions_request(_: i16) -> RequestKind {
        RequestKind::ApiVersions(ApiVersionsRequest::default())
    }

    fn create_topics_request(_: i16) -> RequestKind {
        RequestKind::CreateTopics(CreateTopicsRequest::default().with_topics(vec![
            CreatableTopic::default()
                .with_name(topic_name("created-topic"))
                .with_num_partitions(5)
                .with_replication_factor(1),
            CreatableTopic::default()
                .with_name(topic_name("created-topic-2"))
                .with_num_partitions(7)
                .with_replication_factor(1),
        ]))
    }

    fn init_producer_id_request(_: i16) -> RequestKind {
        RequestKind::InitProducerId(InitProducerIdRequest::default())
    }

    fn describe_configs_request(_: i16) -> RequestKind {
        RequestKind::DescribeConfigs(DescribeConfigsRequest::default().with_resources(vec![
            DescribeConfigsResource::default()
                .with_resource_type(2)
                .with_resource_name(StrBytes::from_static_str("configured-topic")),
            DescribeConfigsResource::default()
                .with_resource_type(4)
                .with_resource_name(StrBytes::from_static_str("configured-broker")),
        ]))
    }

    fn describe_cluster_request(_: i16) -> RequestKind {
        RequestKind::DescribeCluster(DescribeClusterRequest::default().with_endpoint_type(1))
    }

    fn assert_produce(response: &ResponseKind, version: i16) {
        let ResponseKind::Produce(response) = response else {
            panic!("expected Produce response, got {response:?}");
        };
        assert_eq!(response.throttle_time_ms, 0);
        assert!(response.node_endpoints.is_empty());
        assert_eq!(response.responses.len(), 2);
        for (topic, (expected_name, expected_id, expected_partitions)) in
            response.responses.iter().zip([
                ("produce-topic", TOPIC_ID, [7, 17]),
                ("produce-topic-2", TOPIC_ID_2, [27, 37]),
            ])
        {
            if version <= 12 {
                assert_eq!(topic.name.as_str(), expected_name);
            } else {
                assert_eq!(topic.topic_id.to_string(), expected_id);
            }
            assert_eq!(topic.partition_responses.len(), 2);
            for (partition, expected_index) in
                topic.partition_responses.iter().zip(expected_partitions)
            {
                assert_eq!(partition.index, expected_index);
                assert_unsupported(partition.error_code);
                assert_eq!(partition.base_offset, -1);
                assert_eq!(partition.log_append_time_ms, -1);
                assert_eq!(partition.log_start_offset, -1);
                assert!(partition.record_errors.is_empty());
                if version >= 8 {
                    assert_eq!(
                        partition.error_message.as_ref().map(StrBytes::as_str),
                        Some(UNSUPPORTED_VERSION_MESSAGE)
                    );
                } else {
                    assert_eq!(partition.error_message, None);
                }
                assert_eq!(partition.current_leader.leader_id, BrokerId::from(-1));
                assert_eq!(partition.current_leader.leader_epoch, -1);
            }
        }
    }

    fn assert_fetch(response: &ResponseKind, version: i16) {
        let ResponseKind::Fetch(response) = response else {
            panic!("expected Fetch response, got {response:?}");
        };
        assert_eq!(response.throttle_time_ms, 0);
        if version >= 7 {
            assert_unsupported(response.error_code);
        }
        assert_eq!(response.session_id, 0);
        assert!(response.node_endpoints.is_empty());
        assert_eq!(response.responses.len(), 2);
        for (topic, (expected_name, expected_id, expected_partitions)) in
            response.responses.iter().zip([
                ("fetch-topic", TOPIC_ID, [8, 18]),
                ("fetch-topic-2", TOPIC_ID_2, [28, 38]),
            ])
        {
            if version <= 12 {
                assert_eq!(topic.topic.as_str(), expected_name);
            } else {
                assert_eq!(topic.topic_id.to_string(), expected_id);
            }
            assert_eq!(topic.partitions.len(), 2);
            for (partition, expected_index) in topic.partitions.iter().zip(expected_partitions) {
                assert_eq!(partition.partition_index, expected_index);
                assert_unsupported(partition.error_code);
                assert_eq!(partition.high_watermark, -1);
                assert_eq!(partition.last_stable_offset, -1);
                assert_eq!(partition.log_start_offset, -1);
                assert_eq!(partition.diverging_epoch.epoch, -1);
                assert_eq!(partition.diverging_epoch.end_offset, -1);
                assert_eq!(partition.current_leader.leader_id, BrokerId::from(-1));
                assert_eq!(partition.current_leader.leader_epoch, -1);
                assert_eq!(partition.snapshot_id.end_offset, -1);
                assert_eq!(partition.snapshot_id.epoch, -1);
                assert_eq!(partition.aborted_transactions, Some(Vec::new()));
                assert_eq!(partition.preferred_read_replica, BrokerId::from(-1));
                assert_eq!(partition.records, Some(Bytes::new()));
            }
        }
    }

    fn assert_list_offsets(response: &ResponseKind, _: i16) {
        let ResponseKind::ListOffsets(response) = response else {
            panic!("expected ListOffsets response, got {response:?}");
        };
        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.topics.len(), 2);
        for (topic, (expected_name, expected_partitions)) in response
            .topics
            .iter()
            .zip([("offsets-topic", [9, 19]), ("offsets-topic-2", [29, 39])])
        {
            assert_eq!(topic.name.as_str(), expected_name);
            assert_eq!(topic.partitions.len(), 2);
            for (partition, expected_index) in topic.partitions.iter().zip(expected_partitions) {
                assert_eq!(partition.partition_index, expected_index);
                assert_unsupported(partition.error_code);
                assert_eq!(partition.timestamp, -1);
                assert_eq!(partition.offset, -1);
                assert_eq!(partition.leader_epoch, -1);
            }
        }
    }

    fn assert_metadata(response: &ResponseKind, version: i16) {
        let ResponseKind::Metadata(response) = response else {
            panic!("expected Metadata response, got {response:?}");
        };
        assert_eq!(response.throttle_time_ms, 0);
        assert!(response.brokers.is_empty());
        assert_eq!(response.cluster_id, None);
        assert_eq!(response.controller_id, BrokerId::from(-1));
        if version >= 13 {
            assert_unsupported(response.error_code);
        }
        assert_eq!(response.topics.len(), 2);
        for (topic, (expected_name, expected_id)) in response.topics.iter().zip([
            ("metadata-topic", TOPIC_ID),
            ("metadata-topic-2", TOPIC_ID_2),
        ]) {
            assert_unsupported(topic.error_code);
            assert_eq!(
                topic.name.as_ref().map(|name| name.as_str()),
                Some(expected_name)
            );
            if version >= 10 {
                assert_eq!(topic.topic_id.to_string(), expected_id);
            }
            assert!(!topic.is_internal);
            assert!(topic.partitions.is_empty());
        }
    }

    fn assert_offset_commit(response: &ResponseKind, version: i16) {
        let ResponseKind::OffsetCommit(response) = response else {
            panic!("expected OffsetCommit response, got {response:?}");
        };
        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.topics.len(), 2);
        for (topic, (expected_name, expected_id, expected_partitions)) in
            response.topics.iter().zip([
                ("commit-topic", TOPIC_ID, [10, 20]),
                ("commit-topic-2", TOPIC_ID_2, [30, 40]),
            ])
        {
            if version <= 9 {
                assert_eq!(topic.name.as_str(), expected_name);
            } else {
                assert_eq!(topic.topic_id.to_string(), expected_id);
            }
            assert_eq!(topic.partitions.len(), 2);
            for (partition, expected_index) in topic.partitions.iter().zip(expected_partitions) {
                assert_eq!(partition.partition_index, expected_index);
                assert_unsupported(partition.error_code);
            }
        }
    }

    fn assert_offset_fetch(response: &ResponseKind, version: i16) {
        let ResponseKind::OffsetFetch(response) = response else {
            panic!("expected OffsetFetch response, got {response:?}");
        };
        assert_eq!(response.throttle_time_ms, 0);
        if version < 2 {
            assert!(response.groups.is_empty());
            assert_eq!(response.topics.len(), 2);
            for (topic, (expected_name, expected_partitions)) in response.topics.iter().zip([
                ("fetch-offset-topic", [11, 21]),
                ("fetch-offset-topic-2", [31, 41]),
            ]) {
                assert_eq!(topic.name.as_str(), expected_name);
                assert_eq!(topic.partitions.len(), 2);
                for (partition, expected_index) in topic.partitions.iter().zip(expected_partitions)
                {
                    assert_eq!(partition.partition_index, expected_index);
                    assert_eq!(partition.committed_offset, -1);
                    assert_eq!(partition.committed_leader_epoch, -1);
                    assert_eq!(partition.metadata, None);
                    assert_unsupported(partition.error_code);
                }
            }
        } else if version <= 7 {
            assert_unsupported(response.error_code);
            assert!(response.topics.is_empty());
            assert!(response.groups.is_empty());
        } else {
            assert!(response.topics.is_empty());
            assert_eq!(response.groups.len(), 2);
            for (group, (expected_group_id, expected_topics)) in response.groups.iter().zip([
                (
                    "fetch-offset-group-v8",
                    [
                        ("fetch-offset-topic-v8", TOPIC_ID, [12, 22]),
                        ("fetch-offset-topic-v8-2", TOPIC_ID_2, [32, 42]),
                    ],
                ),
                (
                    "fetch-offset-group-v8-2",
                    [
                        ("fetch-offset-topic-v8-3", TOPIC_ID_3, [52, 62]),
                        ("fetch-offset-topic-v8-4", TOPIC_ID_4, [72, 82]),
                    ],
                ),
            ]) {
                assert_eq!(group.group_id.as_str(), expected_group_id);
                assert_unsupported(group.error_code);
                assert_eq!(group.topics.len(), 2);
                for (topic, (expected_name, expected_id, expected_partitions)) in
                    group.topics.iter().zip(expected_topics)
                {
                    if version <= 9 {
                        assert_eq!(topic.name.as_str(), expected_name);
                    } else {
                        assert_eq!(topic.topic_id.to_string(), expected_id);
                    }
                    assert_eq!(topic.partitions.len(), 2);
                    for (partition, expected_index) in
                        topic.partitions.iter().zip(expected_partitions)
                    {
                        assert_eq!(partition.partition_index, expected_index);
                        assert_eq!(partition.committed_offset, -1);
                        assert_eq!(partition.committed_leader_epoch, -1);
                        assert_eq!(partition.metadata, None);
                        assert_unsupported(partition.error_code);
                    }
                }
            }
        }
    }

    fn assert_find_coordinator(response: &ResponseKind, version: i16) {
        let ResponseKind::FindCoordinator(response) = response else {
            panic!("expected FindCoordinator response, got {response:?}");
        };
        assert_eq!(response.throttle_time_ms, 0);
        if version <= 3 {
            assert_unsupported(response.error_code);
            if version == 0 {
                assert_eq!(response.error_message, Some(StrBytes::new()));
            } else {
                assert_eq!(
                    response.error_message.as_ref().map(StrBytes::as_str),
                    Some(UNSUPPORTED_VERSION_MESSAGE)
                );
            }
            assert_eq!(response.node_id, BrokerId::from(-1));
            assert!(response.host.is_empty());
            assert_eq!(response.port, -1);
            assert!(response.coordinators.is_empty());
        } else {
            assert_eq!(response.coordinators.len(), 2);
            for (coordinator, expected_key) in response
                .coordinators
                .iter()
                .zip(["coordinator-key-v4", "coordinator-key-v4-2"])
            {
                assert_eq!(coordinator.key.as_str(), expected_key);
                assert_eq!(coordinator.node_id, BrokerId::from(-1));
                assert!(coordinator.host.is_empty());
                assert_eq!(coordinator.port, -1);
                assert_unsupported(coordinator.error_code);
                assert_eq!(
                    coordinator.error_message.as_ref().map(StrBytes::as_str),
                    Some(UNSUPPORTED_VERSION_MESSAGE)
                );
            }
        }
    }

    fn assert_join_group(response: &ResponseKind, _: i16) {
        let ResponseKind::JoinGroup(response) = response else {
            panic!("expected JoinGroup response, got {response:?}");
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

    fn assert_heartbeat(response: &ResponseKind, _: i16) {
        let ResponseKind::Heartbeat(response) = response else {
            panic!("expected Heartbeat response, got {response:?}");
        };
        assert_eq!(response.throttle_time_ms, 0);
        assert_unsupported(response.error_code);
    }

    fn assert_leave_group(response: &ResponseKind, _: i16) {
        let ResponseKind::LeaveGroup(response) = response else {
            panic!("expected LeaveGroup response, got {response:?}");
        };
        assert_eq!(response.throttle_time_ms, 0);
        assert_unsupported(response.error_code);
        assert!(response.members.is_empty());
    }

    fn assert_sync_group(response: &ResponseKind, _: i16) {
        let ResponseKind::SyncGroup(response) = response else {
            panic!("expected SyncGroup response, got {response:?}");
        };
        assert_eq!(response.throttle_time_ms, 0);
        assert_unsupported(response.error_code);
        assert_eq!(response.protocol_type, None);
        assert_eq!(response.protocol_name, None);
        assert!(response.assignment.is_empty());
    }

    fn assert_describe_groups(response: &ResponseKind, _: i16) {
        let ResponseKind::DescribeGroups(response) = response else {
            panic!("expected DescribeGroups response, got {response:?}");
        };
        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.groups.len(), 2);
        for (group, expected_group_id) in response
            .groups
            .iter()
            .zip(["described-group", "described-group-2"])
        {
            assert_unsupported(group.error_code);
            assert_eq!(group.group_id.as_str(), expected_group_id);
            assert!(group.group_state.is_empty());
            assert!(group.protocol_type.is_empty());
            assert!(group.protocol_data.is_empty());
            assert!(group.members.is_empty());
        }
    }

    fn assert_list_groups(response: &ResponseKind, _: i16) {
        let ResponseKind::ListGroups(response) = response else {
            panic!("expected ListGroups response, got {response:?}");
        };
        assert_eq!(response.throttle_time_ms, 0);
        assert_unsupported(response.error_code);
        assert!(response.groups.is_empty());
    }

    fn assert_api_versions(response: &ResponseKind, _: i16) {
        let ResponseKind::ApiVersions(response) = response else {
            panic!("expected ApiVersions response, got {response:?}");
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

    fn assert_create_topics(response: &ResponseKind, version: i16) {
        let ResponseKind::CreateTopics(response) = response else {
            panic!("expected CreateTopics response, got {response:?}");
        };
        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.topics.len(), 2);
        for (topic, expected_name) in response
            .topics
            .iter()
            .zip(["created-topic", "created-topic-2"])
        {
            assert_eq!(topic.name.as_str(), expected_name);
            assert!(topic.topic_id.is_nil());
            assert_unsupported(topic.error_code);
            if version >= 1 {
                assert_eq!(
                    topic.error_message.as_ref().map(StrBytes::as_str),
                    Some(UNSUPPORTED_VERSION_MESSAGE)
                );
            } else {
                assert_eq!(topic.error_message, None);
            }
            assert_eq!(topic.topic_config_error_code, 0);
            assert_eq!(topic.num_partitions, -1);
            assert_eq!(topic.replication_factor, -1);
            assert_eq!(topic.configs, Some(Vec::new()));
        }
    }

    fn assert_init_producer_id(response: &ResponseKind, _: i16) {
        let ResponseKind::InitProducerId(response) = response else {
            panic!("expected InitProducerId response, got {response:?}");
        };
        assert_eq!(response.throttle_time_ms, 0);
        assert_unsupported(response.error_code);
        assert_eq!(response.producer_id, ProducerId::from(-1));
        assert_eq!(response.producer_epoch, -1);
        assert_eq!(response.ongoing_txn_producer_id, ProducerId::from(-1));
        assert_eq!(response.ongoing_txn_producer_epoch, -1);
    }

    fn assert_describe_configs(response: &ResponseKind, _: i16) {
        let ResponseKind::DescribeConfigs(response) = response else {
            panic!("expected DescribeConfigs response, got {response:?}");
        };
        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.results.len(), 2);
        for (result, (expected_type, expected_name)) in response
            .results
            .iter()
            .zip([(2, "configured-topic"), (4, "configured-broker")])
        {
            assert_unsupported(result.error_code);
            assert_eq!(
                result.error_message.as_ref().map(StrBytes::as_str),
                Some(UNSUPPORTED_VERSION_MESSAGE)
            );
            assert_eq!(result.resource_type, expected_type);
            assert_eq!(result.resource_name.as_str(), expected_name);
            assert!(result.configs.is_empty());
        }
    }

    fn assert_describe_cluster(response: &ResponseKind, _: i16) {
        let ResponseKind::DescribeCluster(response) = response else {
            panic!("expected DescribeCluster response, got {response:?}");
        };
        assert_eq!(response.throttle_time_ms, 0);
        assert_unsupported(response.error_code);
        assert_eq!(response.error_message, None);
        assert_eq!(response.endpoint_type, 1);
        assert!(response.cluster_id.is_empty());
        assert_eq!(response.controller_id, BrokerId::from(-1));
        assert!(response.brokers.is_empty());
        assert_eq!(response.cluster_authorized_operations, i32::MIN);
    }

    fn assert_unsupported(error_code: i16) {
        assert_eq!(error_code, ResponseError::UnsupportedVersion.code());
    }
}
