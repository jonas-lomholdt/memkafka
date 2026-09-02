use kafka_protocol::{
    ResponseError,
    messages::{
        BrokerId, MetadataRequest, MetadataResponse, TopicName,
        metadata_request::MetadataRequestTopic,
        metadata_response::{
            MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
        },
    },
    protocol::StrBytes,
};
use uuid::Uuid;

use crate::{
    broker::{
        BrokerState,
        topics::{TopicError, TopicMetadata},
    },
    config::AdvertisedAddress,
};

use super::discovery::{
    CLUSTER_AUTHORIZED_OPERATIONS, CLUSTER_ID, TOPIC_AUTHORIZED_OPERATIONS,
    optional_authorized_operations,
};

pub(crate) async fn response(
    request: &MetadataRequest,
    version: i16,
    broker: &BrokerState,
    advertised_kafka: &AdvertisedAddress,
) -> MetadataResponse {
    let (error_code, mut topics) = topics_for_request(request, version, broker).await;
    let topic_authorized_operations = optional_authorized_operations(
        request.include_topic_authorized_operations,
        TOPIC_AUTHORIZED_OPERATIONS,
    );
    for topic in &mut topics {
        topic.topic_authorized_operations = topic_authorized_operations;
    }

    MetadataResponse::default()
        .with_brokers(vec![
            MetadataResponseBroker::default()
                .with_node_id(BrokerId::from(broker.broker_id()))
                .with_host(StrBytes::from_string(advertised_kafka.host().to_owned()))
                .with_port(i32::from(advertised_kafka.port())),
        ])
        .with_cluster_id(Some(StrBytes::from_static_str(CLUSTER_ID)))
        .with_controller_id(BrokerId::from(broker.broker_id()))
        .with_topics(topics)
        .with_cluster_authorized_operations(optional_authorized_operations(
            request.include_cluster_authorized_operations,
            CLUSTER_AUTHORIZED_OPERATIONS,
        ))
        .with_error_code(error_code)
}

async fn topics_for_request(
    request: &MetadataRequest,
    version: i16,
    broker: &BrokerState,
) -> (i16, Vec<MetadataResponseTopic>) {
    let Some(requested) = request.topics.as_deref() else {
        let topics = broker
            .topics()
            .list()
            .await
            .into_iter()
            .map(|topic| success_topic(topic, broker.broker_id()))
            .collect();
        return (0, topics);
    };
    if requested.is_empty() {
        return (0, Vec::new());
    }

    let invalid_legacy_shape = (10..=11).contains(&version)
        && requested
            .iter()
            .any(|topic| topic.name.is_none() || !topic.topic_id.is_nil());
    let uuid_mode = version >= 12 && requested.iter().any(|topic| !topic.topic_id.is_nil());
    let invalid_name_mode =
        version >= 10 && !uuid_mode && requested.iter().any(|topic| topic.name.is_none());
    if invalid_legacy_shape || invalid_name_mode {
        return (
            ResponseError::InvalidRequest.code(),
            requested
                .iter()
                .map(|topic| {
                    error_topic(
                        topic.name.clone(),
                        Uuid::nil(),
                        ResponseError::InvalidRequest,
                    )
                })
                .collect(),
        );
    }

    if uuid_mode {
        return (0, topics_by_id(requested, broker).await);
    }

    let allow_auto_create = broker.auto_create_topics()
        && (request.allow_auto_topic_creation || broker.force_auto_create_topics());
    if allow_auto_create {
        for topic in requested {
            let Some(name) = topic.name.as_ref() else {
                continue;
            };
            match broker
                .topics()
                .get_or_auto_create(name.as_str(), true)
                .await
            {
                Ok(_) | Err(TopicError::InvalidName) => {}
                Err(error) => {
                    unreachable!(
                        "automatic topic creation returned an explicit-only error: {error}"
                    )
                }
            }
        }
    }

    (0, topics_by_name(requested, version, broker).await)
}

async fn topics_by_name(
    requested: &[MetadataRequestTopic],
    _version: i16,
    broker: &BrokerState,
) -> Vec<MetadataResponseTopic> {
    let mut topics = Vec::with_capacity(requested.len());
    for requested in requested {
        let Some(name) = requested.name.as_ref() else {
            topics.push(error_topic(
                None,
                Uuid::nil(),
                ResponseError::InvalidTopicException,
            ));
            continue;
        };
        let topic = match broker.topics().get(name.as_str()).await {
            Ok(Some(metadata)) => success_topic(metadata, broker.broker_id()),
            Ok(None) => error_topic(
                Some(name.clone()),
                Uuid::nil(),
                ResponseError::UnknownTopicOrPartition,
            ),
            Err(TopicError::InvalidName) => error_topic(
                Some(name.clone()),
                Uuid::nil(),
                ResponseError::InvalidTopicException,
            ),
            Err(error) => unreachable!("topic lookup returned creation-only error: {error}"),
        };
        topics.push(topic);
    }
    topics
}

async fn topics_by_id(
    requested: &[MetadataRequestTopic],
    broker: &BrokerState,
) -> Vec<MetadataResponseTopic> {
    let mut topics = Vec::with_capacity(requested.len());
    for requested in requested {
        let topic = match broker.topics().get_by_id(requested.topic_id).await {
            Some(metadata) => success_topic(metadata, broker.broker_id()),
            None => error_topic(None, requested.topic_id, ResponseError::UnknownTopicId),
        };
        topics.push(topic);
    }
    topics
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
        .with_topic_id(topic.id)
        .with_partitions(partitions)
}

fn error_topic(
    name: Option<TopicName>,
    topic_id: Uuid,
    error: ResponseError,
) -> MetadataResponseTopic {
    MetadataResponseTopic::default()
        .with_error_code(error.code())
        .with_name(name)
        .with_topic_id(topic_id)
        .with_partitions(Vec::new())
}
