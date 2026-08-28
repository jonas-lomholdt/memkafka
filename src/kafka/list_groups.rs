use kafka_protocol::{
    messages::{GroupId, ListGroupsResponse, list_groups_response::ListedGroup},
    protocol::StrBytes,
};

use crate::broker::BrokerState;

pub(crate) async fn response(broker: &BrokerState) -> ListGroupsResponse {
    let groups = broker
        .groups()
        .list()
        .await
        .into_iter()
        .map(|group| {
            ListedGroup::default()
                .with_group_id(GroupId::from(StrBytes::from_string(group.group_id)))
                .with_protocol_type(StrBytes::from_string(group.protocol_type))
        })
        .collect();

    ListGroupsResponse::default()
        .with_error_code(0)
        .with_groups(groups)
}
