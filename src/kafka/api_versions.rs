use kafka_protocol::messages::{ApiKey, ApiVersionsResponse, api_versions_response::ApiVersion};

use super::{create_topics, fetch, list_offsets, metadata, produce};

pub(crate) const VERSION_RANGE: std::ops::RangeInclusive<i16> = 0..=4;

pub(crate) fn response() -> ApiVersionsResponse {
    ApiVersionsResponse::default().with_api_keys(vec![
        api_range(ApiKey::Metadata, &metadata::VERSION_RANGE),
        api_range(ApiKey::ApiVersions, &VERSION_RANGE),
        api_range(ApiKey::CreateTopics, &create_topics::VERSION_RANGE),
        api_range(ApiKey::Produce, &produce::VERSION_RANGE),
        api_range(ApiKey::ListOffsets, &list_offsets::VERSION_RANGE),
        api_range(ApiKey::Fetch, &fetch::VERSION_RANGE),
    ])
}

fn api_range(api_key: ApiKey, versions: &std::ops::RangeInclusive<i16>) -> ApiVersion {
    ApiVersion::default()
        .with_api_key(api_key as i16)
        .with_min_version(*versions.start())
        .with_max_version(*versions.end())
}
