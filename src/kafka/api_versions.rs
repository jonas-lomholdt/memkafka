use kafka_protocol::messages::{ApiKey, ApiVersionsResponse, api_versions_response::ApiVersion};

pub(crate) const VERSION_RANGE: std::ops::RangeInclusive<i16> = 0..=4;

pub(crate) fn response() -> ApiVersionsResponse {
    ApiVersionsResponse::default().with_api_keys(vec![
        ApiVersion::default()
            .with_api_key(ApiKey::ApiVersions as i16)
            .with_min_version(*VERSION_RANGE.start())
            .with_max_version(*VERSION_RANGE.end()),
    ])
}
