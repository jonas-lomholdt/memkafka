use kafka_protocol::{
    ResponseError,
    messages::{
        ListOffsetsRequest, ListOffsetsResponse,
        list_offsets_request::ListOffsetsPartition,
        list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse},
    },
};

use crate::broker::BrokerState;

pub(crate) async fn response(
    request: &ListOffsetsRequest,
    broker: &BrokerState,
) -> ListOffsetsResponse {
    let mut topics = Vec::with_capacity(request.topics.len());
    for topic in &request.topics {
        let mut partitions = Vec::with_capacity(topic.partitions.len());
        for partition in &topic.partitions {
            partitions.push(partition_response(topic.name.as_str(), partition, broker).await);
        }
        topics.push(
            ListOffsetsTopicResponse::default()
                .with_name(topic.name.clone())
                .with_partitions(partitions),
        );
    }

    ListOffsetsResponse::default()
        .with_throttle_time_ms(0)
        .with_topics(topics)
}

async fn partition_response(
    topic: &str,
    partition: &ListOffsetsPartition,
    broker: &BrokerState,
) -> ListOffsetsPartitionResponse {
    let Some(log) = broker
        .topics()
        .partition(topic, partition.partition_index)
        .await
    else {
        return error_partition(
            partition.partition_index,
            ResponseError::UnknownTopicOrPartition,
        );
    };

    let offset = match partition.timestamp {
        -2 => 0,
        -1 => log.next_offset().await,
        _ => {
            return error_partition(
                partition.partition_index,
                ResponseError::UnsupportedForMessageFormat,
            );
        }
    };

    ListOffsetsPartitionResponse::default()
        .with_partition_index(partition.partition_index)
        .with_timestamp(-1)
        .with_offset(offset)
        .with_leader_epoch(-1)
}

fn error_partition(index: i32, error: ResponseError) -> ListOffsetsPartitionResponse {
    ListOffsetsPartitionResponse::default()
        .with_partition_index(index)
        .with_error_code(error.code())
        .with_timestamp(-1)
        .with_offset(-1)
        .with_leader_epoch(-1)
}
