use std::{error::Error, fmt};

use kafka_protocol::messages::{ApiKey, RequestKind, ResponseKind};

use crate::broker::BrokerState;

use super::{
    api_versions, codec::DecodedRequest, create_topics, describe_configs, describe_groups, fetch,
    find_coordinator, heartbeat, join_group, leave_group, list_groups, list_offsets, metadata,
    offset_commit, offset_fetch, produce, sync_group,
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
        match request.api_key {
            ApiKey::ApiVersions => {
                require_version(request.api_key, version, &api_versions::VERSION_RANGE)?;
                match &request.body {
                    RequestKind::ApiVersions(_) => Ok(api_versions::response().into()),
                    _ => Err(DispatchError::BodyMismatch(request.api_key)),
                }
            }
            ApiKey::Metadata => {
                require_version(request.api_key, version, &metadata::VERSION_RANGE)?;
                match &request.body {
                    RequestKind::Metadata(body) => {
                        Ok(metadata::response(body, &self.broker).await.into())
                    }
                    _ => Err(DispatchError::BodyMismatch(request.api_key)),
                }
            }
            ApiKey::CreateTopics => {
                require_version(request.api_key, version, &create_topics::VERSION_RANGE)?;
                match &request.body {
                    RequestKind::CreateTopics(body) => {
                        Ok(create_topics::response(body, &self.broker).await.into())
                    }
                    _ => Err(DispatchError::BodyMismatch(request.api_key)),
                }
            }
            ApiKey::Produce => {
                require_version(request.api_key, version, &produce::VERSION_RANGE)?;
                match &request.body {
                    RequestKind::Produce(body) => {
                        Ok(produce::response(body, &self.broker).await.into())
                    }
                    _ => Err(DispatchError::BodyMismatch(request.api_key)),
                }
            }
            ApiKey::ListOffsets => {
                require_version(request.api_key, version, &list_offsets::VERSION_RANGE)?;
                match &request.body {
                    RequestKind::ListOffsets(body) => {
                        Ok(list_offsets::response(body, &self.broker).await.into())
                    }
                    _ => Err(DispatchError::BodyMismatch(request.api_key)),
                }
            }
            ApiKey::Fetch => {
                require_version(request.api_key, version, &fetch::VERSION_RANGE)?;
                match &request.body {
                    RequestKind::Fetch(body) => {
                        Ok(fetch::response(body, &self.broker).await.into())
                    }
                    _ => Err(DispatchError::BodyMismatch(request.api_key)),
                }
            }
            ApiKey::FindCoordinator => {
                require_version(request.api_key, version, &find_coordinator::VERSION_RANGE)?;
                match &request.body {
                    RequestKind::FindCoordinator(body) => {
                        Ok(find_coordinator::response(body, &self.broker).into())
                    }
                    _ => Err(DispatchError::BodyMismatch(request.api_key)),
                }
            }
            ApiKey::JoinGroup => {
                require_version(request.api_key, version, &join_group::VERSION_RANGE)?;
                match &request.body {
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
                }
            }
            ApiKey::SyncGroup => {
                require_version(request.api_key, version, &sync_group::VERSION_RANGE)?;
                match &request.body {
                    RequestKind::SyncGroup(body) => {
                        Ok(sync_group::response(body, &self.broker).await.into())
                    }
                    _ => Err(DispatchError::BodyMismatch(request.api_key)),
                }
            }
            ApiKey::Heartbeat => {
                require_version(request.api_key, version, &heartbeat::VERSION_RANGE)?;
                match &request.body {
                    RequestKind::Heartbeat(body) => {
                        Ok(heartbeat::response(body, &self.broker).await.into())
                    }
                    _ => Err(DispatchError::BodyMismatch(request.api_key)),
                }
            }
            ApiKey::LeaveGroup => {
                require_version(request.api_key, version, &leave_group::VERSION_RANGE)?;
                match &request.body {
                    RequestKind::LeaveGroup(body) => {
                        Ok(leave_group::response(body, version, &self.broker)
                            .await
                            .into())
                    }
                    _ => Err(DispatchError::BodyMismatch(request.api_key)),
                }
            }
            ApiKey::OffsetCommit => {
                require_version(request.api_key, version, &offset_commit::VERSION_RANGE)?;
                match &request.body {
                    RequestKind::OffsetCommit(body) => {
                        Ok(offset_commit::response(body, &self.broker).await.into())
                    }
                    _ => Err(DispatchError::BodyMismatch(request.api_key)),
                }
            }
            ApiKey::OffsetFetch => {
                require_version(request.api_key, version, &offset_fetch::VERSION_RANGE)?;
                match &request.body {
                    RequestKind::OffsetFetch(body) => {
                        Ok(offset_fetch::response(body, &self.broker).await.into())
                    }
                    _ => Err(DispatchError::BodyMismatch(request.api_key)),
                }
            }
            ApiKey::ListGroups => {
                require_version(request.api_key, version, &list_groups::VERSION_RANGE)?;
                match &request.body {
                    RequestKind::ListGroups(_) => {
                        Ok(list_groups::response(&self.broker).await.into())
                    }
                    _ => Err(DispatchError::BodyMismatch(request.api_key)),
                }
            }
            ApiKey::DescribeGroups => {
                require_version(request.api_key, version, &describe_groups::VERSION_RANGE)?;
                match &request.body {
                    RequestKind::DescribeGroups(body) => {
                        Ok(describe_groups::response(body, &self.broker).await.into())
                    }
                    _ => Err(DispatchError::BodyMismatch(request.api_key)),
                }
            }
            ApiKey::DescribeConfigs => {
                require_version(request.api_key, version, &describe_configs::VERSION_RANGE)?;
                match &request.body {
                    RequestKind::DescribeConfigs(body) => {
                        Ok(describe_configs::response(body, &self.broker).await.into())
                    }
                    _ => Err(DispatchError::BodyMismatch(request.api_key)),
                }
            }
            _ => Err(DispatchError::UnsupportedApi(request.api_key)),
        }
    }
}

fn require_version(
    api_key: ApiKey,
    version: i16,
    supported: &std::ops::RangeInclusive<i16>,
) -> Result<(), DispatchError> {
    if supported.contains(&version) {
        Ok(())
    } else {
        Err(DispatchError::UnsupportedVersion { api_key, version })
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
