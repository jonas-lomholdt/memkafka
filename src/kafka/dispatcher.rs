use std::{error::Error, fmt};

use kafka_protocol::messages::{ApiKey, RequestKind, ResponseKind};

use crate::broker::BrokerState;

use super::{
    api_versions, capabilities, codec::DecodedRequest, create_topics, describe_configs,
    describe_groups, fetch, find_coordinator, heartbeat, init_producer_id, join_group, leave_group,
    list_groups, list_offsets, metadata, offset_commit, offset_fetch, produce, sync_group,
};

#[derive(Clone, Debug)]
pub struct Dispatcher {
    broker: BrokerState,
}

impl Dispatcher {
    pub fn new(broker: BrokerState) -> Self {
        Self { broker }
    }

    pub async fn dispatch(&self, request: &DecodedRequest) -> Result<ResponseKind, DispatchError> {
        let version = request.header.request_api_version;
        let Some(capability) = capabilities::capability(request.api_key) else {
            return Err(DispatchError::UnsupportedApi(request.api_key));
        };
        if !capability.supports(version) {
            return Err(DispatchError::UnsupportedVersion {
                api_key: request.api_key,
                version,
            });
        }

        match request.api_key {
            ApiKey::ApiVersions => match &request.body {
                RequestKind::ApiVersions(_) => Ok(api_versions::response().into()),
                _ => Err(DispatchError::BodyMismatch(request.api_key)),
            },
            ApiKey::Metadata => match &request.body {
                RequestKind::Metadata(body) => {
                    Ok(metadata::response(body, &self.broker).await.into())
                }
                _ => Err(DispatchError::BodyMismatch(request.api_key)),
            },
            ApiKey::CreateTopics => match &request.body {
                RequestKind::CreateTopics(body) => {
                    Ok(create_topics::response(body, &self.broker).await.into())
                }
                _ => Err(DispatchError::BodyMismatch(request.api_key)),
            },
            ApiKey::Produce => match &request.body {
                RequestKind::Produce(body) => {
                    Ok(produce::response(body, &self.broker).await.into())
                }
                _ => Err(DispatchError::BodyMismatch(request.api_key)),
            },
            ApiKey::ListOffsets => match &request.body {
                RequestKind::ListOffsets(body) => {
                    Ok(list_offsets::response(body, &self.broker).await.into())
                }
                _ => Err(DispatchError::BodyMismatch(request.api_key)),
            },
            ApiKey::Fetch => match &request.body {
                RequestKind::Fetch(body) => Ok(fetch::response(body, &self.broker).await.into()),
                _ => Err(DispatchError::BodyMismatch(request.api_key)),
            },
            ApiKey::FindCoordinator => match &request.body {
                RequestKind::FindCoordinator(body) => {
                    Ok(find_coordinator::response(body, &self.broker).into())
                }
                _ => Err(DispatchError::BodyMismatch(request.api_key)),
            },
            ApiKey::JoinGroup => match &request.body {
                RequestKind::JoinGroup(body) => Ok(join_group::response(
                    body,
                    version,
                    request
                        .header
                        .client_id
                        .as_ref()
                        .map(|value| value.as_str()),
                    &self.broker,
                )
                .await
                .into()),
                _ => Err(DispatchError::BodyMismatch(request.api_key)),
            },
            ApiKey::SyncGroup => match &request.body {
                RequestKind::SyncGroup(body) => {
                    Ok(sync_group::response(body, &self.broker).await.into())
                }
                _ => Err(DispatchError::BodyMismatch(request.api_key)),
            },
            ApiKey::Heartbeat => match &request.body {
                RequestKind::Heartbeat(body) => {
                    Ok(heartbeat::response(body, &self.broker).await.into())
                }
                _ => Err(DispatchError::BodyMismatch(request.api_key)),
            },
            ApiKey::LeaveGroup => match &request.body {
                RequestKind::LeaveGroup(body) => {
                    Ok(leave_group::response(body, version, &self.broker)
                        .await
                        .into())
                }
                _ => Err(DispatchError::BodyMismatch(request.api_key)),
            },
            ApiKey::OffsetCommit => match &request.body {
                RequestKind::OffsetCommit(body) => {
                    Ok(offset_commit::response(body, &self.broker).await.into())
                }
                _ => Err(DispatchError::BodyMismatch(request.api_key)),
            },
            ApiKey::OffsetFetch => match &request.body {
                RequestKind::OffsetFetch(body) => {
                    Ok(offset_fetch::response(body, &self.broker).await.into())
                }
                _ => Err(DispatchError::BodyMismatch(request.api_key)),
            },
            ApiKey::ListGroups => match &request.body {
                RequestKind::ListGroups(_) => Ok(list_groups::response(&self.broker).await.into()),
                _ => Err(DispatchError::BodyMismatch(request.api_key)),
            },
            ApiKey::DescribeGroups => match &request.body {
                RequestKind::DescribeGroups(body) => {
                    Ok(describe_groups::response(body, &self.broker).await.into())
                }
                _ => Err(DispatchError::BodyMismatch(request.api_key)),
            },
            ApiKey::InitProducerId => match &request.body {
                RequestKind::InitProducerId(body) => {
                    Ok(init_producer_id::response(body, &self.broker).await.into())
                }
                _ => Err(DispatchError::BodyMismatch(request.api_key)),
            },
            ApiKey::DescribeConfigs => match &request.body {
                RequestKind::DescribeConfigs(body) => {
                    Ok(describe_configs::response(body, &self.broker).await.into())
                }
                _ => Err(DispatchError::BodyMismatch(request.api_key)),
            },
            _ => Err(DispatchError::UnsupportedApi(request.api_key)),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DispatchError {
    UnsupportedApi(ApiKey),
    UnsupportedVersion { api_key: ApiKey, version: i16 },
    BodyMismatch(ApiKey),
}

impl fmt::Display for DispatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedApi(api_key) => {
                write!(formatter, "Kafka API {api_key:?} is not implemented")
            }
            Self::UnsupportedVersion { api_key, version } => {
                write!(
                    formatter,
                    "Kafka API {api_key:?} v{version} is not supported"
                )
            }
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
