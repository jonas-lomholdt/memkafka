use kafka_protocol::{
    ResponseError,
    messages::{HeartbeatRequest, HeartbeatResponse},
};

use crate::broker::BrokerState;

use super::group_error::response_error;

pub(crate) const VERSION_RANGE: std::ops::RangeInclusive<i16> = 0..=3;

pub(crate) async fn response(
    request: &HeartbeatRequest,
    broker: &BrokerState,
) -> HeartbeatResponse {
    let error_code = if request.group_instance_id.is_some() {
        ResponseError::UnsupportedVersion.code()
    } else {
        broker
            .groups()
            .heartbeat(
                request.group_id.as_str(),
                request.generation_id,
                request.member_id.as_str(),
            )
            .await
            .err()
            .map_or(0, |error| response_error(error).code())
    };
    HeartbeatResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(error_code)
}
