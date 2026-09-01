use kafka_protocol::{
    ResponseError,
    messages::{
        BrokerId, MetadataRequest, MetadataResponse, TopicName,
        metadata_response::{
            MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
        },
    },
    protocol::StrBytes,
};

use crate::{
    broker::{
        BrokerState,
        topics::{TopicError, TopicMetadata},
    },
    config::AdvertisedAddress,
};

pub(crate) async fn response(
    request: &MetadataRequest,
    broker: &BrokerState,
    advertised_kafka: &AdvertisedAddress,
) -> MetadataResponse {
    let topics = match &request.topics {
        None => broker
            .topics()
            .list()
            .await
            .into_iter()
            .map(|topic| success_topic(topic, broker.broker_id()))
            .collect(),
        Some(requested_topics) => {
            let mut topics = Vec::with_capacity(requested_topics.len());
            let allow_auto_create = broker.auto_create_topics()
                && (request.allow_auto_topic_creation || broker.force_auto_create_topics());
            for requested in requested_topics {
                let Some(name) = requested.name.as_ref() else {
                    topics.push(error_topic(None, ResponseError::InvalidTopicException));
                    continue;
                };
                let topic = match broker
                    .topics()
                    .get_or_auto_create(name.as_str(), allow_auto_create)
                    .await
                {
                    Ok(Some(metadata)) => success_topic(metadata, broker.broker_id()),
                    Ok(None) => {
                        error_topic(Some(name.clone()), ResponseError::UnknownTopicOrPartition)
                    }
                    Err(TopicError::InvalidName) => {
                        error_topic(Some(name.clone()), ResponseError::InvalidTopicException)
                    }
                    Err(error) => {
                        unreachable!("topic lookup returned creation-only error: {error}")
                    }
                };
                topics.push(topic);
            }
            topics
        }
    };

    MetadataResponse::default()
        .with_brokers(vec![
            MetadataResponseBroker::default()
                .with_node_id(BrokerId::from(broker.broker_id()))
                .with_host(StrBytes::from_string(advertised_kafka.host().to_owned()))
                .with_port(i32::from(advertised_kafka.port())),
        ])
        .with_cluster_id(Some(StrBytes::from_static_str("memkafka")))
        .with_controller_id(BrokerId::from(broker.broker_id()))
        .with_topics(topics)
}

fn success_topic(topic: TopicMetadata, broker_id: i32) -> MetadataResponseTopic {
    let partitions = (0..topic.partition_count)
        .map(|partition_index| {
            MetadataResponsePartition::default()
                .with_partition_index(
                    i32::try_from(partition_index)
                        .expect("Kafka partition count must fit in a signed integer"),
                )
                .with_leader_id(BrokerId::from(broker_id))
                .with_leader_epoch(0)
                .with_replica_nodes(vec![BrokerId::from(broker_id)])
                .with_isr_nodes(vec![BrokerId::from(broker_id)])
                .with_offline_replicas(Vec::new())
        })
        .collect();

    MetadataResponseTopic::default()
        .with_name(Some(TopicName::from(StrBytes::from_string(topic.name))))
        .with_partitions(partitions)
}

fn error_topic(name: Option<TopicName>, error: ResponseError) -> MetadataResponseTopic {
    MetadataResponseTopic::default()
        .with_error_code(error.code())
        .with_name(name)
        .with_partitions(Vec::new())
}
