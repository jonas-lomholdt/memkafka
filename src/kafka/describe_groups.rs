use kafka_protocol::{
    ResponseError,
    messages::{
        DescribeGroupsRequest, DescribeGroupsResponse, GroupId,
        describe_groups_response::{DescribedGroup, DescribedGroupMember},
    },
    protocol::StrBytes,
};

use crate::broker::{BrokerState, groups::GroupDescription};

pub(crate) const VERSION_RANGE: std::ops::RangeInclusive<i16> = 0..=0;

pub(crate) async fn response(
    request: &DescribeGroupsRequest,
    broker: &BrokerState,
) -> DescribeGroupsResponse {
    let mut groups = Vec::with_capacity(request.groups.len());
    for group_id in &request.groups {
        groups.push(match broker.groups().describe(group_id.as_str()).await {
            Some(description) => described_group(description),
            None => missing_group(group_id.clone()),
        });
    }

    DescribeGroupsResponse::default().with_groups(groups)
}

fn described_group(description: GroupDescription) -> DescribedGroup {
    DescribedGroup::default()
        .with_error_code(0)
        .with_group_id(GroupId::from(StrBytes::from_string(description.group_id)))
        .with_group_state(StrBytes::from_static_str(description.state))
        .with_protocol_type(StrBytes::from_string(description.protocol_type))
        .with_protocol_data(StrBytes::from_string(description.protocol_name))
        .with_members(
            description
                .members
                .into_iter()
                .map(|member| {
                    DescribedGroupMember::default()
                        .with_member_id(StrBytes::from_string(member.member_id))
                        .with_client_id(StrBytes::from_string(member.client_id))
                        .with_client_host(StrBytes::default())
                        .with_member_metadata(member.metadata)
                        .with_member_assignment(member.assignment)
                })
                .collect(),
        )
}

fn missing_group(group_id: GroupId) -> DescribedGroup {
    DescribedGroup::default()
        .with_error_code(ResponseError::GroupIdNotFound.code())
        .with_group_id(group_id)
        .with_group_state(StrBytes::default())
        .with_protocol_type(StrBytes::default())
        .with_protocol_data(StrBytes::default())
        .with_members(Vec::new())
}
