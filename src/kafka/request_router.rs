use std::{error::Error, fmt};

use kafka_protocol::messages::{ApiKey, RequestHeader, RequestKind, ResponseKind};

use super::{
    capabilities,
    codec::{DecodedFrame, DecodedRequest},
    error_response::{self, ErrorResponseError},
};

pub(crate) struct SupportedRequest<'a>(&'a DecodedRequest);

impl<'a> SupportedRequest<'a> {
    pub(crate) fn header(&self) -> &'a RequestHeader {
        &self.0.header
    }

    pub(crate) fn api_key(&self) -> ApiKey {
        self.0.api_key
    }

    pub(crate) fn body(&self) -> &'a RequestKind {
        &self.0.body
    }

    pub(crate) fn expects_response(&self) -> bool {
        self.0.expects_response()
    }
}

pub(crate) struct ResponseEnvelope {
    pub(crate) api_key: ApiKey,
    pub(crate) encoding_version: i16,
    pub(crate) correlation_id: i32,
    pub(crate) body: ResponseKind,
}

#[allow(clippy::large_enum_variant)] // Preserve the plan's explicit, unboxed routing boundary.
pub(crate) enum Route<'a> {
    Dispatch(SupportedRequest<'a>),
    Respond(ResponseEnvelope),
    NoResponse,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RouteError {
    UnsupportedApi {
        api_key: ApiKey,
        version: i16,
        correlation_id: i32,
    },
    ErrorResponse(ErrorResponseError),
}

impl fmt::Display for RouteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedApi {
                api_key,
                version,
                correlation_id,
            } => write!(
                formatter,
                "Kafka API {api_key:?} v{version} is not advertised (correlation ID {correlation_id})"
            ),
            Self::ErrorResponse(error) => write!(formatter, "failed to route Kafka error: {error}"),
        }
    }
}

impl Error for RouteError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::UnsupportedApi { .. } => None,
            Self::ErrorResponse(error) => Some(error),
        }
    }
}

pub(crate) fn route(frame: &DecodedFrame) -> Result<Route<'_>, RouteError> {
    match frame {
        DecodedFrame::Request(request) => {
            let version = request.header.request_api_version;
            let correlation_id = request.header.correlation_id;
            let Some(capability) = capabilities::capability(request.api_key) else {
                return Err(RouteError::UnsupportedApi {
                    api_key: request.api_key,
                    version,
                    correlation_id,
                });
            };
            if capability.supports(version) {
                return Ok(Route::Dispatch(SupportedRequest(request)));
            }
            if request.api_key == ApiKey::Produce && !request.expects_response() {
                return Ok(Route::NoResponse);
            }

            let body =
                error_response::unsupported_version(request).map_err(RouteError::ErrorResponse)?;
            Ok(Route::Respond(ResponseEnvelope {
                api_key: request.api_key,
                encoding_version: version,
                correlation_id,
                body,
            }))
        }
        DecodedFrame::UnsupportedApiVersions { header, .. } => {
            Ok(Route::Respond(ResponseEnvelope {
                api_key: ApiKey::ApiVersions,
                encoding_version: 0,
                correlation_id: header.correlation_id,
                body: error_response::unsupported_api_versions(),
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use kafka_protocol::{
        ResponseError,
        messages::{
            ApiKey, ApiVersionsRequest, CreateTopicsRequest, DescribeClusterRequest,
            DescribeConfigsRequest, DescribeGroupsRequest, DescribeTopicPartitionsRequest,
            FetchRequest, FindCoordinatorRequest, HeartbeatRequest, InitProducerIdRequest,
            JoinGroupRequest, LeaveGroupRequest, ListGroupsRequest, ListOffsetsRequest,
            MetadataRequest, OffsetCommitRequest, OffsetFetchRequest, ProduceRequest,
            RequestHeader, RequestKind, ResponseKind, SyncGroupRequest, TopicName,
            metadata_request::MetadataRequestTopic,
            produce_request::{PartitionProduceData, TopicProduceData},
        },
        protocol::StrBytes,
    };

    use super::{Route, RouteError, route};
    use crate::kafka::{
        capabilities::{ApiCapability, CAPABILITIES},
        codec::{DecodedFrame, DecodedRequest},
    };

    const CORRELATION_ID: i32 = 0x1020_3040;

    #[test]
    fn request_router_dispatches_supported_versions_through_an_opaque_token() {
        let frame = request_frame(
            ApiKey::ApiVersions,
            4,
            RequestKind::ApiVersions(
                ApiVersionsRequest::default()
                    .with_client_software_name(StrBytes::from_static_str("router-test"))
                    .with_client_software_version(StrBytes::from_static_str("1")),
            ),
        );
        let DecodedFrame::Request(original) = &frame else {
            unreachable!("fixture is a decoded request");
        };

        let Route::Dispatch(request) = route(&frame).expect("route supported request") else {
            panic!("expected dispatch route");
        };

        assert_eq!(request.api_key(), ApiKey::ApiVersions);
        assert_eq!(request.header().correlation_id, CORRELATION_ID);
        assert_eq!(request.header().request_api_version, 4);
        assert!(matches!(request.body(), RequestKind::ApiVersions(_)));
        assert!(request.expects_response());
        assert!(std::ptr::eq(request.header(), &original.header));
        assert!(std::ptr::eq(request.body(), &original.body));
    }

    #[test]
    fn request_router_builds_a_typed_response_for_schema_known_unadvertised_versions() {
        let frame = request_frame(
            ApiKey::Metadata,
            3,
            RequestKind::Metadata(MetadataRequest::default().with_topics(Some(vec![
                MetadataRequestTopic::default().with_name(Some(topic_name("router-topic"))),
            ]))),
        );

        let Route::Respond(response) = route(&frame).expect("route unsupported Metadata") else {
            panic!("expected response route");
        };

        assert_eq!(response.api_key, ApiKey::Metadata);
        assert_eq!(response.encoding_version, 3);
        assert_eq!(response.correlation_id, CORRELATION_ID);
        let ResponseKind::Metadata(body) = response.body else {
            panic!("expected Metadata response");
        };
        assert_eq!(body.topics.len(), 1);
        assert_eq!(
            body.topics[0].error_code,
            ResponseError::UnsupportedVersion.code()
        );
        assert_eq!(
            body.topics[0].name.as_ref().map(|name| name.0.as_str()),
            Some("router-topic")
        );
    }

    #[test]
    fn request_router_suppresses_unsupported_acks_zero_produce_without_dispatch() {
        let frame = request_frame(
            ApiKey::Produce,
            6,
            RequestKind::Produce(ProduceRequest::default().with_acks(0).with_topic_data(vec![
                        TopicProduceData::default()
                            .with_name(topic_name("router-topic"))
                            .with_partition_data(vec![
                                PartitionProduceData::default()
                                    .with_index(0)
                                    .with_records(Some(Bytes::from_static(b"must-not-append"))),
                            ]),
                    ])),
        );

        assert!(matches!(route(&frame), Ok(Route::NoResponse)));
    }

    #[test]
    fn every_advertised_boundary_routes_to_its_explicit_outcome() {
        assert_eq!(
            CAPABILITIES.len(),
            19,
            "the advertised boundary matrix changed"
        );

        for capability in CAPABILITIES {
            for version in supported_boundaries(capability) {
                let frame = request_frame(
                    capability.api_key,
                    version,
                    router_request(capability.api_key),
                );

                let Route::Dispatch(request) = route(&frame).unwrap_or_else(|error| {
                    panic!(
                        "route supported {:?} v{version} through dispatcher: {error}",
                        capability.api_key
                    )
                }) else {
                    panic!(
                        "supported {:?} v{version} did not route to dispatch",
                        capability.api_key
                    );
                };
                assert_eq!(request.api_key(), capability.api_key);
                assert_eq!(request.header().request_api_version, version);
                assert_eq!(request.header().correlation_id, CORRELATION_ID);
            }

            for version in adjacent_unsupported(capability) {
                let frame = request_frame(
                    capability.api_key,
                    version,
                    router_request(capability.api_key),
                );

                let Route::Respond(response) = route(&frame).unwrap_or_else(|error| {
                    panic!(
                        "route unsupported {:?} v{version}: {error}",
                        capability.api_key
                    )
                }) else {
                    panic!(
                        "unsupported {:?} v{version} did not route to a response",
                        capability.api_key
                    );
                };
                assert_eq!(response.api_key, capability.api_key);
                assert_eq!(response.encoding_version, version);
                assert_eq!(response.correlation_id, CORRELATION_ID);
                assert_response_variant(capability.api_key, &response.body);
            }
        }

        let capability = CAPABILITIES
            .iter()
            .find(|capability| capability.api_key == ApiKey::Produce)
            .expect("Produce is advertised");
        let version = adjacent_unsupported(capability)
            .into_iter()
            .next()
            .expect("Produce has a schema-known unsupported neighbor");
        let frame = request_frame(
            ApiKey::Produce,
            version,
            RequestKind::Produce(ProduceRequest::default().with_acks(0)),
        );
        assert!(matches!(route(&frame), Ok(Route::NoResponse)));
    }

    #[test]
    fn request_router_uses_version_zero_for_unsupported_api_versions() {
        let frame = DecodedFrame::UnsupportedApiVersions {
            header: RequestHeader::default()
                .with_request_api_key(ApiKey::ApiVersions as i16)
                .with_request_api_version(i16::MAX)
                .with_correlation_id(CORRELATION_ID),
            requested_version: i16::MAX,
        };

        let Route::Respond(response) = route(&frame).expect("route unsupported ApiVersions") else {
            panic!("expected response route");
        };

        assert_eq!(response.api_key, ApiKey::ApiVersions);
        assert_eq!(response.encoding_version, 0);
        assert_eq!(response.correlation_id, CORRELATION_ID);
        let ResponseKind::ApiVersions(body) = response.body else {
            panic!("expected ApiVersions response");
        };
        assert_eq!(body.error_code, ResponseError::UnsupportedVersion.code());
        assert_eq!(body.api_keys.len(), 1);
        assert_eq!(body.api_keys[0].api_key, ApiKey::ApiVersions as i16);
        assert_eq!(body.api_keys[0].min_version, 0);
        assert_eq!(body.api_keys[0].max_version, 4);
    }

    #[test]
    fn request_router_rejects_generated_but_unadvertised_apis() {
        let frame = request_frame(
            ApiKey::DeleteTopics,
            ApiKey::DeleteTopics.valid_versions().min,
            RequestKind::DeleteTopics(Default::default()),
        );

        assert!(matches!(
            route(&frame),
            Err(RouteError::UnsupportedApi {
                api_key: ApiKey::DeleteTopics,
                version,
                correlation_id: CORRELATION_ID,
            }) if version == ApiKey::DeleteTopics.valid_versions().min
        ));
    }

    fn request_frame(api_key: ApiKey, version: i16, body: RequestKind) -> DecodedFrame {
        DecodedFrame::Request(DecodedRequest {
            header: RequestHeader::default()
                .with_request_api_key(api_key as i16)
                .with_request_api_version(version)
                .with_correlation_id(CORRELATION_ID)
                .with_client_id(Some(StrBytes::from_static_str("request-router-test"))),
            api_key,
            body,
        })
    }

    fn topic_name(value: &'static str) -> TopicName {
        TopicName::from(StrBytes::from_static_str(value))
    }

    fn supported_boundaries(capability: &ApiCapability) -> Vec<i16> {
        let mut versions = vec![capability.supported.min];
        if capability.supported.max != capability.supported.min {
            versions.push(capability.supported.max);
        }
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

    fn router_request(api_key: ApiKey) -> RequestKind {
        match api_key {
            ApiKey::Produce => RequestKind::Produce(ProduceRequest::default().with_acks(1)),
            ApiKey::Fetch => RequestKind::Fetch(FetchRequest::default()),
            ApiKey::ListOffsets => RequestKind::ListOffsets(ListOffsetsRequest::default()),
            ApiKey::Metadata => RequestKind::Metadata(MetadataRequest::default()),
            ApiKey::OffsetCommit => RequestKind::OffsetCommit(OffsetCommitRequest::default()),
            ApiKey::OffsetFetch => RequestKind::OffsetFetch(OffsetFetchRequest::default()),
            ApiKey::FindCoordinator => {
                RequestKind::FindCoordinator(FindCoordinatorRequest::default())
            }
            ApiKey::JoinGroup => RequestKind::JoinGroup(JoinGroupRequest::default()),
            ApiKey::Heartbeat => RequestKind::Heartbeat(HeartbeatRequest::default()),
            ApiKey::LeaveGroup => RequestKind::LeaveGroup(LeaveGroupRequest::default()),
            ApiKey::SyncGroup => RequestKind::SyncGroup(SyncGroupRequest::default()),
            ApiKey::DescribeGroups => RequestKind::DescribeGroups(DescribeGroupsRequest::default()),
            ApiKey::ListGroups => RequestKind::ListGroups(ListGroupsRequest::default()),
            ApiKey::ApiVersions => RequestKind::ApiVersions(ApiVersionsRequest::default()),
            ApiKey::CreateTopics => RequestKind::CreateTopics(CreateTopicsRequest::default()),
            ApiKey::InitProducerId => RequestKind::InitProducerId(InitProducerIdRequest::default()),
            ApiKey::DescribeConfigs => {
                RequestKind::DescribeConfigs(DescribeConfigsRequest::default())
            }
            ApiKey::DescribeCluster => {
                RequestKind::DescribeCluster(DescribeClusterRequest::default())
            }
            ApiKey::DescribeTopicPartitions => {
                RequestKind::DescribeTopicPartitions(DescribeTopicPartitionsRequest::default())
            }
            _ => panic!("router boundary fixture is missing {api_key:?}"),
        }
    }

    fn assert_response_variant(api_key: ApiKey, body: &ResponseKind) {
        assert!(
            matches!(
                (api_key, body),
                (ApiKey::Produce, ResponseKind::Produce(_))
                    | (ApiKey::Fetch, ResponseKind::Fetch(_))
                    | (ApiKey::ListOffsets, ResponseKind::ListOffsets(_))
                    | (ApiKey::Metadata, ResponseKind::Metadata(_))
                    | (ApiKey::OffsetCommit, ResponseKind::OffsetCommit(_))
                    | (ApiKey::OffsetFetch, ResponseKind::OffsetFetch(_))
                    | (ApiKey::FindCoordinator, ResponseKind::FindCoordinator(_))
                    | (ApiKey::JoinGroup, ResponseKind::JoinGroup(_))
                    | (ApiKey::Heartbeat, ResponseKind::Heartbeat(_))
                    | (ApiKey::LeaveGroup, ResponseKind::LeaveGroup(_))
                    | (ApiKey::SyncGroup, ResponseKind::SyncGroup(_))
                    | (ApiKey::DescribeGroups, ResponseKind::DescribeGroups(_))
                    | (ApiKey::ListGroups, ResponseKind::ListGroups(_))
                    | (ApiKey::ApiVersions, ResponseKind::ApiVersions(_))
                    | (ApiKey::CreateTopics, ResponseKind::CreateTopics(_))
                    | (ApiKey::InitProducerId, ResponseKind::InitProducerId(_))
                    | (ApiKey::DescribeConfigs, ResponseKind::DescribeConfigs(_))
                    | (ApiKey::DescribeCluster, ResponseKind::DescribeCluster(_))
                    | (
                        ApiKey::DescribeTopicPartitions,
                        ResponseKind::DescribeTopicPartitions(_)
                    )
            ),
            "expected {api_key:?} response variant, got {body:?}"
        );
    }
}
