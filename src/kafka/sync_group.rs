use kafka_protocol::{
    messages::{SyncGroupRequest, SyncGroupResponse},
    protocol::StrBytes,
};

use crate::broker::{BrokerState, groups::SyncAssignment};

use super::group_error::response_error;

pub(crate) const VERSION_RANGE: std::ops::RangeInclusive<i16> = 0..=3;

pub(crate) async fn response(
    request: &SyncGroupRequest,
    broker: &BrokerState,
) -> SyncGroupResponse {
    if request.group_instance_id.is_some() {
        return SyncGroupResponse::default()
            .with_error_code(kafka_protocol::ResponseError::UnsupportedVersion.code());
    }
    let assignments = request
        .assignments
        .iter()
        .map(|assignment| SyncAssignment {
            member_id: assignment.member_id.as_str().to_owned(),
            assignment: assignment.assignment.clone(),
        })
        .collect();

    match broker
        .groups()
        .sync(
            request.group_id.as_str(),
            request.generation_id,
            request.member_id.as_str(),
            assignments,
        )
        .await
    {
        Ok(assignment) => SyncGroupResponse::default()
            .with_throttle_time_ms(0)
            .with_error_code(0)
            .with_assignment(assignment),
        Err(error) => SyncGroupResponse::default()
            .with_throttle_time_ms(0)
            .with_error_code(response_error(error).code())
            .with_assignment(bytes::Bytes::new())
            .with_protocol_type(None::<StrBytes>)
            .with_protocol_name(None::<StrBytes>),
    }
}
