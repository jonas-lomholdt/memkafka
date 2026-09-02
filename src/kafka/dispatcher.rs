use std::{error::Error, fmt};

use kafka_protocol::messages::{ApiKey, RequestKind, ResponseKind};

use crate::{broker::BrokerState, config::AdvertisedAddress};

use super::{
    api_versions, create_topics, describe_cluster, describe_configs, describe_groups,
    describe_topic_partitions, fetch, find_coordinator, heartbeat, init_producer_id, join_group,
    leave_group, list_groups, list_offsets, metadata, offset_commit, offset_fetch, produce,
    request_router::SupportedRequest, sync_group,
};

#[allow(dead_code)] // Kept as the exact capability/dispatcher/rejection coverage declaration.
pub(crate) const DISPATCHED_API_KEYS: &[ApiKey] = &[
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

#[derive(Clone, Debug)]
pub struct Dispatcher {
    broker: BrokerState,
    advertised_kafka: AdvertisedAddress,
}

impl Dispatcher {
    pub fn new(broker: BrokerState, advertised_kafka: AdvertisedAddress) -> Self {
        Self {
            broker,
            advertised_kafka,
        }
    }

    pub(crate) async fn dispatch(
        &self,
        request: SupportedRequest<'_>,
    ) -> Result<ResponseKind, DispatchError> {
        let version = request.header().request_api_version;

        match request.api_key() {
            ApiKey::ApiVersions => match request.body() {
                RequestKind::ApiVersions(_) => Ok(api_versions::response().into()),
                _ => Err(DispatchError::BodyMismatch(request.api_key())),
            },
            ApiKey::Metadata => match request.body() {
                RequestKind::Metadata(body) => {
                    Ok(
                        metadata::response(body, version, &self.broker, &self.advertised_kafka)
                            .await
                            .into(),
                    )
                }
                _ => Err(DispatchError::BodyMismatch(request.api_key())),
            },
            ApiKey::CreateTopics => match request.body() {
                RequestKind::CreateTopics(body) => {
                    Ok(create_topics::response(body, &self.broker).await.into())
                }
                _ => Err(DispatchError::BodyMismatch(request.api_key())),
            },
            ApiKey::Produce => match request.body() {
                RequestKind::Produce(body) => {
                    Ok(produce::response(body, &self.broker).await.into())
                }
                _ => Err(DispatchError::BodyMismatch(request.api_key())),
            },
            ApiKey::ListOffsets => match request.body() {
                RequestKind::ListOffsets(body) => {
                    Ok(list_offsets::response(body, &self.broker).await.into())
                }
                _ => Err(DispatchError::BodyMismatch(request.api_key())),
            },
            ApiKey::Fetch => match request.body() {
                RequestKind::Fetch(body) => Ok(fetch::response(body, &self.broker).await.into()),
                _ => Err(DispatchError::BodyMismatch(request.api_key())),
            },
            ApiKey::FindCoordinator => match request.body() {
                RequestKind::FindCoordinator(body) => {
                    Ok(
                        find_coordinator::response(body, &self.broker, &self.advertised_kafka)
                            .into(),
                    )
                }
                _ => Err(DispatchError::BodyMismatch(request.api_key())),
            },
            ApiKey::JoinGroup => match request.body() {
                RequestKind::JoinGroup(body) => Ok(join_group::response(
                    body,
                    version,
                    request
                        .header()
                        .client_id
                        .as_ref()
                        .map(|value| value.as_str()),
                    &self.broker,
                )
                .await
                .into()),
                _ => Err(DispatchError::BodyMismatch(request.api_key())),
            },
            ApiKey::SyncGroup => match request.body() {
                RequestKind::SyncGroup(body) => {
                    Ok(sync_group::response(body, &self.broker).await.into())
                }
                _ => Err(DispatchError::BodyMismatch(request.api_key())),
            },
            ApiKey::Heartbeat => match request.body() {
                RequestKind::Heartbeat(body) => {
                    Ok(heartbeat::response(body, &self.broker).await.into())
                }
                _ => Err(DispatchError::BodyMismatch(request.api_key())),
            },
            ApiKey::LeaveGroup => match request.body() {
                RequestKind::LeaveGroup(body) => {
                    Ok(leave_group::response(body, version, &self.broker)
                        .await
                        .into())
                }
                _ => Err(DispatchError::BodyMismatch(request.api_key())),
            },
            ApiKey::OffsetCommit => match request.body() {
                RequestKind::OffsetCommit(body) => {
                    Ok(offset_commit::response(body, &self.broker).await.into())
                }
                _ => Err(DispatchError::BodyMismatch(request.api_key())),
            },
            ApiKey::OffsetFetch => match request.body() {
                RequestKind::OffsetFetch(body) => {
                    Ok(offset_fetch::response(body, &self.broker).await.into())
                }
                _ => Err(DispatchError::BodyMismatch(request.api_key())),
            },
            ApiKey::ListGroups => match request.body() {
                RequestKind::ListGroups(_) => Ok(list_groups::response(&self.broker).await.into()),
                _ => Err(DispatchError::BodyMismatch(request.api_key())),
            },
            ApiKey::DescribeGroups => match request.body() {
                RequestKind::DescribeGroups(body) => {
                    Ok(describe_groups::response(body, &self.broker).await.into())
                }
                _ => Err(DispatchError::BodyMismatch(request.api_key())),
            },
            ApiKey::InitProducerId => match request.body() {
                RequestKind::InitProducerId(body) => {
                    Ok(init_producer_id::response(body, &self.broker).await.into())
                }
                _ => Err(DispatchError::BodyMismatch(request.api_key())),
            },
            ApiKey::DescribeConfigs => match request.body() {
                RequestKind::DescribeConfigs(body) => {
                    Ok(describe_configs::response(body, &self.broker).await.into())
                }
                _ => Err(DispatchError::BodyMismatch(request.api_key())),
            },
            ApiKey::DescribeCluster => match request.body() {
                RequestKind::DescribeCluster(body) => {
                    Ok(
                        describe_cluster::response(body, &self.broker, &self.advertised_kafka)
                            .into(),
                    )
                }
                _ => Err(DispatchError::BodyMismatch(request.api_key())),
            },
            ApiKey::DescribeTopicPartitions => match request.body() {
                RequestKind::DescribeTopicPartitions(body) => {
                    Ok(describe_topic_partitions::response(body, &self.broker)
                        .await
                        .into())
                }
                _ => Err(DispatchError::BodyMismatch(request.api_key())),
            },
            _ => Err(DispatchError::BodyMismatch(request.api_key())),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DispatchError {
    BodyMismatch(ApiKey),
}

impl fmt::Display for DispatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BodyMismatch(api_key) => {
                write!(
                    formatter,
                    "decoded body does not match Kafka API {api_key:?}"
                )
            }
        }
    }
}

impl Error for DispatchError {}
