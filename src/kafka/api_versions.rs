use kafka_protocol::messages::{ApiKey, ApiVersionsResponse, api_versions_response::ApiVersion};

use super::{
    create_topics, fetch, find_coordinator, heartbeat, join_group, leave_group, list_offsets,
    metadata, offset_commit, offset_fetch, produce, sync_group,
};

pub(crate) const VERSION_RANGE: std::ops::RangeInclusive<i16> = 0..=4;

pub(crate) fn response() -> ApiVersionsResponse {
    ApiVersionsResponse::default().with_api_keys(vec![
        api_range(ApiKey::Metadata, &metadata::VERSION_RANGE),
        api_range(ApiKey::ApiVersions, &VERSION_RANGE),
        api_range(ApiKey::CreateTopics, &create_topics::VERSION_RANGE),
        api_range(ApiKey::Produce, &produce::VERSION_RANGE),
        api_range(ApiKey::ListOffsets, &list_offsets::VERSION_RANGE),
        api_range(ApiKey::Fetch, &fetch::VERSION_RANGE),
        api_range(ApiKey::FindCoordinator, &find_coordinator::VERSION_RANGE),
        api_range(ApiKey::JoinGroup, &join_group::VERSION_RANGE),
        api_range(ApiKey::SyncGroup, &sync_group::VERSION_RANGE),
        api_range(ApiKey::Heartbeat, &heartbeat::VERSION_RANGE),
        api_range(ApiKey::LeaveGroup, &leave_group::VERSION_RANGE),
        api_range(ApiKey::OffsetCommit, &offset_commit::VERSION_RANGE),
        api_range(ApiKey::OffsetFetch, &offset_fetch::VERSION_RANGE),
    ])
}

fn api_range(api_key: ApiKey, versions: &std::ops::RangeInclusive<i16>) -> ApiVersion {
    ApiVersion::default()
        .with_api_key(api_key as i16)
        .with_min_version(*versions.start())
        .with_max_version(*versions.end())
}
