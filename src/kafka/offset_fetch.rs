use std::collections::BTreeMap;

use kafka_protocol::{
    messages::{
        OffsetFetchRequest, OffsetFetchResponse, TopicName,
        offset_fetch_response::{OffsetFetchResponsePartition, OffsetFetchResponseTopic},
    },
    protocol::StrBytes,
};

use crate::broker::{
    BrokerState,
    groups::{FetchedOffset, TopicPartition},
};

pub(crate) async fn response(
    request: &OffsetFetchRequest,
    broker: &BrokerState,
) -> OffsetFetchResponse {
    let requested = request.topics.as_ref().map(|topics| {
        topics
            .iter()
            .flat_map(|topic| {
                topic.partition_indexes.iter().map(|partition| {
                    TopicPartition::new(topic.name.as_str().to_owned(), *partition)
                })
            })
            .collect::<Vec<_>>()
    });
    let fetched = broker
        .groups()
        .fetch_offsets(request.group_id.as_str(), requested.as_deref())
        .await;

    OffsetFetchResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(0)
        .with_topics(group_by_topic(fetched))
}

fn group_by_topic(offsets: Vec<FetchedOffset>) -> Vec<OffsetFetchResponseTopic> {
    let mut topics = BTreeMap::<String, Vec<OffsetFetchResponsePartition>>::new();
    for offset in offsets {
        topics.entry(offset.topic).or_default().push(
            OffsetFetchResponsePartition::default()
                .with_partition_index(offset.partition)
                .with_committed_offset(offset.offset.unwrap_or(-1))
                .with_committed_leader_epoch(-1)
                .with_metadata(offset.metadata.map(StrBytes::from_string))
                .with_error_code(0),
        );
    }
    topics
        .into_iter()
        .map(|(name, partitions)| {
            OffsetFetchResponseTopic::default()
                .with_name(TopicName::from(StrBytes::from_string(name)))
                .with_partitions(partitions)
        })
        .collect()
}
