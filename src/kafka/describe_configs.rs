use std::collections::HashSet;

use kafka_protocol::{
    ResponseError,
    messages::{
        DescribeConfigsRequest, DescribeConfigsResponse,
        describe_configs_response::DescribeConfigsResult,
    },
};

use crate::broker::BrokerState;

pub(crate) const VERSION_RANGE: std::ops::RangeInclusive<i16> = 1..=1;

pub(crate) async fn response(
    request: &DescribeConfigsRequest,
    broker: &BrokerState,
) -> DescribeConfigsResponse {
    let topic_names = broker
        .topics()
        .list()
        .await
        .into_iter()
        .map(|topic| topic.name)
        .collect::<HashSet<_>>();
    let broker_name = broker.broker_id().to_string();
    DescribeConfigsResponse::default()
        .with_throttle_time_ms(0)
        .with_results(
            request
                .resources
                .iter()
                .map(|resource| {
                    let error = match resource.resource_type {
                        2 if topic_names.contains(resource.resource_name.as_str()) => None,
                        2 => Some(ResponseError::UnknownTopicOrPartition),
                        4 if resource.resource_name.as_str() == broker_name => None,
                        4 => Some(ResponseError::BrokerNotAvailable),
                        _ => Some(ResponseError::InvalidRequest),
                    };
                    DescribeConfigsResult::default()
                        .with_error_code(error.map_or(0, |error| error.code()))
                        .with_error_message(None)
                        .with_resource_type(resource.resource_type)
                        .with_resource_name(resource.resource_name.clone())
                        .with_configs(Vec::new())
                })
                .collect(),
        )
}
