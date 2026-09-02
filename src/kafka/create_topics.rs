use kafka_protocol::{
    ResponseError,
    messages::{
        CreateTopicsRequest, CreateTopicsResponse, TopicName,
        create_topics_request::CreatableTopic, create_topics_response::CreatableTopicResult,
    },
    protocol::StrBytes,
};

use crate::broker::{
    BrokerState,
    topics::{TopicError, TopicMetadata},
};

pub(crate) async fn response(
    request: &CreateTopicsRequest,
    broker: &BrokerState,
) -> CreateTopicsResponse {
    let mut results = Vec::with_capacity(request.topics.len());
    for topic in &request.topics {
        results.push(create_one(topic, request.validate_only, broker).await);
    }
    CreateTopicsResponse::default().with_topics(results)
}

async fn create_one(
    topic: &CreatableTopic,
    validate_only: bool,
    broker: &BrokerState,
) -> CreatableTopicResult {
    if !topic.assignments.is_empty() {
        return error_result(
            topic.name.clone(),
            ResponseError::InvalidReplicaAssignment,
            "manual replica assignments are not supported",
        );
    }
    if !topic.configs.is_empty() {
        return error_result(
            topic.name.clone(),
            ResponseError::InvalidConfig,
            "custom topic configurations are not supported",
        );
    }

    if validate_only {
        return match broker
            .topics()
            .validate_explicit(
                topic.name.as_str(),
                topic.num_partitions,
                topic.replication_factor,
            )
            .await
        {
            Ok(()) => validation_success_result(topic.name.clone(), topic.num_partitions),
            Err(error) => topic_error_result(topic.name.clone(), error),
        };
    }

    match broker
        .topics()
        .create_explicit(
            topic.name.as_str(),
            topic.num_partitions,
            topic.replication_factor,
        )
        .await
    {
        Ok(metadata) => success_result(topic.name.clone(), metadata),
        Err(error) => topic_error_result(topic.name.clone(), error),
    }
}

fn success_result(name: TopicName, metadata: TopicMetadata) -> CreatableTopicResult {
    success_result_with_partition_count(
        name,
        i32::try_from(metadata.partition_count)
            .expect("explicit Kafka partition count fits in a signed integer"),
    )
}

fn validation_success_result(name: TopicName, partition_count: i32) -> CreatableTopicResult {
    success_result_with_partition_count(name, partition_count)
}

fn success_result_with_partition_count(
    name: TopicName,
    partition_count: i32,
) -> CreatableTopicResult {
    CreatableTopicResult::default()
        .with_name(name)
        .with_error_message(None)
        .with_num_partitions(partition_count)
        .with_replication_factor(1)
        .with_configs(Some(Vec::new()))
}

fn topic_error_result(name: TopicName, error: TopicError) -> CreatableTopicResult {
    let response_error = match error {
        TopicError::AlreadyExists => ResponseError::TopicAlreadyExists,
        TopicError::InvalidName => ResponseError::InvalidTopicException,
        TopicError::InvalidPartitions => ResponseError::InvalidPartitions,
        TopicError::InvalidReplicationFactor => ResponseError::InvalidReplicationFactor,
    };
    error_result(name, response_error, &error.to_string())
}

fn error_result(name: TopicName, error: ResponseError, message: &str) -> CreatableTopicResult {
    CreatableTopicResult::default()
        .with_name(name)
        .with_error_code(error.code())
        .with_error_message(Some(StrBytes::from_string(message.to_owned())))
}
