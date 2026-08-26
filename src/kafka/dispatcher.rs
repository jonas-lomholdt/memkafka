use std::{error::Error, fmt};

use kafka_protocol::messages::{ApiKey, RequestKind, ResponseKind};

use super::{api_versions, codec::DecodedRequest};

#[derive(Clone, Debug, Default)]
pub struct Dispatcher;

impl Dispatcher {
    pub async fn dispatch(&self, request: &DecodedRequest) -> Result<ResponseKind, DispatchError> {
        if request.api_key != ApiKey::ApiVersions {
            return Err(DispatchError::UnsupportedApi(request.api_key));
        }
        if !api_versions::VERSION_RANGE.contains(&request.header.request_api_version) {
            return Err(DispatchError::UnsupportedVersion {
                api_key: request.api_key,
                version: request.header.request_api_version,
            });
        }

        match &request.body {
            RequestKind::ApiVersions(_) => Ok(api_versions::response().into()),
            _ => Err(DispatchError::BodyMismatch(request.api_key)),
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
