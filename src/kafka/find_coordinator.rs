use kafka_protocol::{
    ResponseError,
    messages::{BrokerId, FindCoordinatorRequest, FindCoordinatorResponse},
    protocol::StrBytes,
};

use crate::broker::BrokerState;

pub(crate) fn response(
    request: &FindCoordinatorRequest,
    broker: &BrokerState,
) -> FindCoordinatorResponse {
    if request.key.is_empty() {
        return error_response(ResponseError::InvalidGroupId);
    }
    if request.key_type != 0 {
        return error_response(ResponseError::UnsupportedVersion);
    }

    FindCoordinatorResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(0)
        .with_error_message(None)
        .with_node_id(BrokerId::from(broker.broker_id()))
        .with_host(StrBytes::from_string(
            broker.advertised_kafka().host().to_owned(),
        ))
        .with_port(i32::from(broker.advertised_kafka().port()))
}

fn error_response(error: ResponseError) -> FindCoordinatorResponse {
    FindCoordinatorResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(error.code())
        .with_error_message(Some(StrBytes::from_string(error.to_string())))
        .with_node_id(BrokerId::from(-1))
        .with_host(StrBytes::default())
        .with_port(-1)
}
