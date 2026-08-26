use std::{error::Error, fmt};

use kafka_protocol::messages::{ApiKey, RequestKind, ResponseKind};

use crate::broker::BrokerState;

use super::{api_versions, codec::DecodedRequest, metadata};

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
