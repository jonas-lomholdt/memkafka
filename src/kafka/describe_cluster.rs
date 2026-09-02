use kafka_protocol::{
    ResponseError,
    messages::{
        BrokerId, DescribeClusterRequest, DescribeClusterResponse,
        describe_cluster_response::DescribeClusterBroker,
    },
    protocol::StrBytes,
};

use crate::{broker::BrokerState, config::AdvertisedAddress};

use super::discovery::{CLUSTER_AUTHORIZED_OPERATIONS, CLUSTER_ID, optional_authorized_operations};

pub(crate) fn response(
    request: &DescribeClusterRequest,
    broker: &BrokerState,
    advertised_kafka: &AdvertisedAddress,
) -> DescribeClusterResponse {
    match request.endpoint_type {
        1 => broker_response(request, broker, advertised_kafka),
        2 => error_response(2, ResponseError::MismatchedEndpointType),
        endpoint_type => error_response(endpoint_type, ResponseError::UnsupportedEndpointType),
    }
}

fn broker_response(
    request: &DescribeClusterRequest,
    broker: &BrokerState,
    advertised_kafka: &AdvertisedAddress,
) -> DescribeClusterResponse {
    let broker_id = BrokerId::from(broker.broker_id());

    DescribeClusterResponse::default()
        .with_endpoint_type(1)
        .with_cluster_id(StrBytes::from_static_str(CLUSTER_ID))
        .with_controller_id(broker_id)
        .with_brokers(vec![
            DescribeClusterBroker::default()
                .with_broker_id(broker_id)
                .with_host(StrBytes::from_string(advertised_kafka.host().to_owned()))
                .with_port(i32::from(advertised_kafka.port()))
                .with_rack(None)
                .with_is_fenced(false),
        ])
        .with_cluster_authorized_operations(optional_authorized_operations(
            request.include_cluster_authorized_operations,
            CLUSTER_AUTHORIZED_OPERATIONS,
        ))
}

fn error_response(endpoint_type: i8, error: ResponseError) -> DescribeClusterResponse {
    DescribeClusterResponse::default()
        .with_endpoint_type(endpoint_type)
        .with_error_code(error.code())
}
