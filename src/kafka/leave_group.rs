use kafka_protocol::{
    messages::{LeaveGroupRequest, LeaveGroupResponse, leave_group_response::MemberResponse},
    protocol::StrBytes,
};

use crate::broker::BrokerState;

use super::group_error::response_error;

pub(crate) const VERSION_RANGE: std::ops::RangeInclusive<i16> = 0..=3;

pub(crate) async fn response(
    request: &LeaveGroupRequest,
    version: i16,
    broker: &BrokerState,
) -> LeaveGroupResponse {
    if version <= 2 {
        let error_code = broker
            .groups()
            .leave(request.group_id.as_str(), request.member_id.as_str())
            .await
            .err()
            .map_or(0, |error| response_error(error).code());
        return LeaveGroupResponse::default()
            .with_throttle_time_ms(0)
            .with_error_code(error_code);
    }

    let mut members = Vec::with_capacity(request.members.len());
    for member in &request.members {
        let error_code = if member.group_instance_id.is_some() {
            kafka_protocol::ResponseError::UnsupportedVersion.code()
        } else {
            broker
                .groups()
                .leave(request.group_id.as_str(), member.member_id.as_str())
                .await
                .err()
                .map_or(0, |error| response_error(error).code())
        };
        members.push(
            MemberResponse::default()
                .with_member_id(member.member_id.clone())
                .with_group_instance_id(None::<StrBytes>)
                .with_error_code(error_code),
        );
    }

    LeaveGroupResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(0)
        .with_members(members)
}
