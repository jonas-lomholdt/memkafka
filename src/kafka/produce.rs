use kafka_protocol::{
    ResponseError,
    messages::{
        ProduceRequest, ProduceResponse,
        produce_request::{PartitionProduceData, TopicProduceData},
        produce_response::{PartitionProduceResponse, TopicProduceResponse},
    },
};

use crate::broker::{BrokerState, partition::AppendError, topics::TopicError};

pub(crate) const VERSION_RANGE: std::ops::RangeInclusive<i16> = 3..=7;

pub(crate) async fn response(request: &ProduceRequest, broker: &BrokerState) -> ProduceResponse {
    let request_error = if !matches!(request.acks, -1..=1) {
        Some(ResponseError::InvalidRequiredAcks)
    } else if request.transactional_id.is_some() {
        Some(ResponseError::UnsupportedForMessageFormat)
    } else {
        None
    };

    let mut responses = Vec::with_capacity(request.topic_data.len());
    for topic in &request.topic_data {
        responses.push(produce_topic(topic, request_error, broker).await);
    }

    ProduceResponse::default()
        .with_responses(responses)
        .with_throttle_time_ms(0)
}

async fn produce_topic(
    topic: &TopicProduceData,
    request_error: Option<ResponseError>,
    broker: &BrokerState,
) -> TopicProduceResponse {
    let topic_error = if let Some(error) = request_error {
        Some(error)
    } else {
        match broker
            .topics()
            .get_or_auto_create(topic.name.as_str(), broker.auto_create_topics())
            .await
        {
            Ok(Some(_)) => None,
            Ok(None) => Some(ResponseError::UnknownTopicOrPartition),
            Err(TopicError::InvalidName) => Some(ResponseError::InvalidTopicException),
            Err(error) => unreachable!("topic lookup returned creation-only error: {error}"),
        }
    };

    let mut partition_responses = Vec::with_capacity(topic.partition_data.len());
    for partition in &topic.partition_data {
        let response = if let Some(error) = topic_error {
            error_partition(partition.index, error)
        } else {
            produce_partition(topic.name.as_str(), partition, broker).await
        };
        partition_responses.push(response);
    }

    TopicProduceResponse::default()
        .with_name(topic.name.clone())
        .with_partition_responses(partition_responses)
}

async fn produce_partition(
    topic: &str,
    request: &PartitionProduceData,
    broker: &BrokerState,
) -> PartitionProduceResponse {
    let Some(log) = broker.topics().partition(topic, request.index).await else {
        return error_partition(request.index, ResponseError::UnknownTopicOrPartition);
    };
    let Some(records) = request.records.clone() else {
        return error_partition(request.index, ResponseError::CorruptMessage);
    };

    match log.append(records).await {
        Ok(result) => {
            broker.notify_append();
            PartitionProduceResponse::default()
                .with_index(request.index)
                .with_base_offset(result.base_offset)
                .with_log_append_time_ms(-1)
                .with_log_start_offset(0)
        }
        Err(AppendError::UnsupportedBatch) => {
            error_partition(request.index, ResponseError::UnsupportedForMessageFormat)
        }
        Err(AppendError::Malformed | AppendError::OffsetOverflow) => {
            error_partition(request.index, ResponseError::CorruptMessage)
        }
    }
}

fn error_partition(index: i32, error: ResponseError) -> PartitionProduceResponse {
    PartitionProduceResponse::default()
        .with_index(index)
        .with_error_code(error.code())
        .with_base_offset(-1)
        .with_log_append_time_ms(-1)
        .with_log_start_offset(0)
}
