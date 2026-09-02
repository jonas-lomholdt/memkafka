use std::collections::BTreeSet;

use kafka_protocol::{
    ResponseError,
    messages::{
        BrokerId, DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse, TopicName,
        describe_topic_partitions_request::TopicRequest,
        describe_topic_partitions_response::{
            Cursor, DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic,
        },
    },
    protocol::StrBytes,
};
use uuid::Uuid;

use crate::broker::{
    BrokerState,
    topics::{TopicError, TopicMetadata},
};

use super::discovery::TOPIC_AUTHORIZED_OPERATIONS;

fn selected_names(
    request: &DescribeTopicPartitionsRequest,
    catalog_topics: &[TopicMetadata],
) -> Result<Vec<String>, ResponseError> {
    if request
        .cursor
        .as_ref()
        .is_some_and(|cursor| cursor.partition_index < 0)
    {
        return Err(ResponseError::InvalidRequest);
    }

    let explicit = !request.topics.is_empty();
    let names = if explicit {
        request
            .topics
            .iter()
            .map(|topic| topic.name.as_str().to_owned())
            .collect::<BTreeSet<_>>()
    } else {
        catalog_topics
            .iter()
            .map(|topic| topic.name.clone())
            .collect::<BTreeSet<_>>()
    };

    if explicit
        && request
            .cursor
            .as_ref()
            .is_some_and(|cursor| !names.contains(cursor.topic_name.as_str()))
    {
        return Err(ResponseError::InvalidRequest);
    }

    let cursor_name = request
        .cursor
        .as_ref()
        .map(|cursor| cursor.topic_name.as_str());
    Ok(names
        .into_iter()
        .filter(|name| cursor_name.is_none_or(|cursor| name.as_str() >= cursor))
        .collect())
}

pub(crate) async fn response(
    request: &DescribeTopicPartitionsRequest,
    broker: &BrokerState,
) -> DescribeTopicPartitionsResponse {
    let catalog_topics = broker.topics().list().await;
    let names = match selected_names(request, &catalog_topics) {
        Ok(names) => names,
        Err(error) => {
            return DescribeTopicPartitionsResponse::default()
                .with_topics(request_error_topics(&request.topics, error));
        }
    };

    let mut remaining = usize::try_from(request.response_partition_limit.max(1))
        .expect("a positive i32 partition limit fits in usize");
    let mut topics = Vec::new();
    let mut next_cursor = None;

    for (index, name) in names.iter().enumerate() {
        let metadata = match broker.topics().get(name).await {
            Ok(Some(metadata)) => metadata,
            Ok(None) => {
                topics.push(error_topic(name, ResponseError::UnknownTopicOrPartition));
                continue;
            }
            Err(TopicError::InvalidName) => {
                topics.push(error_topic(name, ResponseError::InvalidTopicException));
                continue;
            }
            Err(error) => unreachable!("topic lookup returned creation-only error: {error}"),
        };

        let start = request
            .cursor
            .as_ref()
            .filter(|cursor| cursor.topic_name.as_str() == metadata.name)
            .map(|cursor| {
                usize::try_from(cursor.partition_index)
                    .expect("selected_names validated the non-negative cursor")
            })
            .unwrap_or(0);
        let end = metadata.partition_count as usize;
        let take = end.saturating_sub(start).min(remaining);
        topics.push(success_topic(
            metadata,
            start..start + take,
            broker.broker_id(),
        ));
        remaining -= take;

        if start + take < end {
            next_cursor = Some(cursor(name, start + take));
            break;
        }
        if remaining == 0 && index + 1 < names.len() {
            next_cursor = Some(cursor(&names[index + 1], 0));
            break;
        }
    }

    DescribeTopicPartitionsResponse::default()
        .with_topics(topics)
        .with_next_cursor(next_cursor)
}

fn success_topic(
    metadata: TopicMetadata,
    partitions: std::ops::Range<usize>,
    broker_id: i32,
) -> DescribeTopicPartitionsResponseTopic {
    DescribeTopicPartitionsResponseTopic::default()
        .with_name(Some(TopicName::from(StrBytes::from_string(metadata.name))))
        .with_topic_id(metadata.id)
        .with_is_internal(false)
        .with_partitions(
            partitions
                .map(|partition_index| {
                    DescribeTopicPartitionsResponsePartition::default()
                        .with_partition_index(
                            i32::try_from(partition_index)
                                .expect("Kafka partition index fits in a signed integer"),
                        )
                        .with_leader_id(BrokerId::from(broker_id))
                        .with_leader_epoch(0)
                        .with_replica_nodes(vec![BrokerId::from(broker_id)])
                        .with_isr_nodes(vec![BrokerId::from(broker_id)])
                        .with_eligible_leader_replicas(None)
                        .with_last_known_elr(None)
                        .with_offline_replicas(Vec::new())
                })
                .collect(),
        )
        .with_topic_authorized_operations(TOPIC_AUTHORIZED_OPERATIONS)
}

fn request_error_topics(
    requested: &[TopicRequest],
    error: ResponseError,
) -> Vec<DescribeTopicPartitionsResponseTopic> {
    requested
        .iter()
        .map(|topic| error_topic(topic.name.as_str(), error))
        .collect()
}

fn error_topic(name: &str, error: ResponseError) -> DescribeTopicPartitionsResponseTopic {
    DescribeTopicPartitionsResponseTopic::default()
        .with_error_code(error.code())
        .with_name(Some(TopicName::from(StrBytes::from_string(
            name.to_owned(),
        ))))
        .with_topic_id(Uuid::nil())
        .with_is_internal(false)
        .with_partitions(Vec::new())
}

fn cursor(name: &str, partition_index: usize) -> Cursor {
    Cursor::default()
        .with_topic_name(TopicName::from(StrBytes::from_string(name.to_owned())))
        .with_partition_index(
            i32::try_from(partition_index)
                .expect("Kafka continuation partition fits in a signed integer"),
        )
}

#[cfg(test)]
mod tests {
    use kafka_protocol::{
        ResponseError,
        messages::{
            DescribeTopicPartitionsRequest, TopicName,
            describe_topic_partitions_request::{Cursor, TopicRequest},
        },
        protocol::StrBytes,
    };
    use uuid::Uuid;

    use super::selected_names;
    use crate::broker::topics::TopicMetadata;

    #[test]
    fn selection_sorts_deduplicates_and_clamps_to_the_cursor_topic() {
        let catalog_topics = vec![metadata("charlie", 1), metadata("alpha", 3)];

        assert_eq!(
            selected_names(&request(&[], None), &catalog_topics),
            Ok(vec!["alpha".to_owned(), "charlie".to_owned()])
        );
        assert_eq!(
            selected_names(
                &request(&["charlie", "alpha", "bravo", "alpha"], Some(("bravo", 0)),),
                &catalog_topics,
            ),
            Ok(vec!["bravo".to_owned(), "charlie".to_owned()])
        );
    }

    #[test]
    fn selection_rejects_negative_and_absent_explicit_cursors() {
        let catalog_topics = vec![metadata("alpha", 3)];

        assert_eq!(
            selected_names(
                &request(&["alpha", "alpha"], Some(("alpha", -1))),
                &catalog_topics,
            ),
            Err(ResponseError::InvalidRequest)
        );
        assert_eq!(
            selected_names(
                &request(&["alpha", "alpha"], Some(("missing", 0))),
                &catalog_topics,
            ),
            Err(ResponseError::InvalidRequest)
        );
        assert_eq!(
            selected_names(&request(&[], Some(("alpha", -1))), &catalog_topics),
            Err(ResponseError::InvalidRequest)
        );
    }

    fn metadata(name: &str, partition_count: u32) -> TopicMetadata {
        TopicMetadata {
            id: Uuid::new_v4(),
            name: name.to_owned(),
            partition_count,
        }
    }

    fn request(topics: &[&str], cursor: Option<(&str, i32)>) -> DescribeTopicPartitionsRequest {
        DescribeTopicPartitionsRequest::default()
            .with_topics(
                topics
                    .iter()
                    .map(|name| {
                        TopicRequest::default()
                            .with_name(TopicName::from(StrBytes::from_string((*name).to_owned())))
                    })
                    .collect(),
            )
            .with_cursor(cursor.map(|(name, partition_index)| {
                Cursor::default()
                    .with_topic_name(TopicName::from(StrBytes::from_string(name.to_owned())))
                    .with_partition_index(partition_index)
            }))
    }
}
