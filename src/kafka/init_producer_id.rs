use kafka_protocol::{
    ResponseError,
    messages::{InitProducerIdRequest, InitProducerIdResponse, ProducerId},
};

use crate::broker::BrokerState;

pub(crate) async fn response(
    request: &InitProducerIdRequest,
    broker: &BrokerState,
) -> InitProducerIdResponse {
    if request.transactional_id.is_some() {
        return error_response(ResponseError::UnsupportedForMessageFormat);
    }

    match broker.producers().allocate().await {
        Ok(identity) => InitProducerIdResponse::default()
            .with_throttle_time_ms(0)
            .with_error_code(0)
            .with_producer_id(ProducerId::from(identity.producer_id))
            .with_producer_epoch(identity.producer_epoch),
        Err(_) => error_response(ResponseError::UnknownServerError),
    }
}

fn error_response(error: ResponseError) -> InitProducerIdResponse {
    InitProducerIdResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(error.code())
        .with_producer_id(ProducerId::from(-1))
        .with_producer_epoch(-1)
}
