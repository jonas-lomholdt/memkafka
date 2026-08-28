use kafka_protocol::messages::{ApiVersionsResponse, api_versions_response::ApiVersion};

use super::capabilities::CAPABILITIES;

pub(crate) fn response() -> ApiVersionsResponse {
    ApiVersionsResponse::default().with_api_keys(
        CAPABILITIES
            .iter()
            .map(|capability| {
                ApiVersion::default()
                    .with_api_key(capability.api_key as i16)
                    .with_min_version(capability.supported.min)
                    .with_max_version(capability.supported.max)
            })
            .collect(),
    )
}
