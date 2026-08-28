use kafka_protocol::{
    ResponseError,
    messages::{
        OffsetCommitRequest, OffsetCommitResponse,
        offset_commit_response::{OffsetCommitResponsePartition, OffsetCommitResponseTopic},
    },
};

use crate::broker::{BrokerState, groups::OffsetCommit};

use super::group_error::response_error;

pub(crate) async fn response(
    request: &OffsetCommitRequest,
    broker: &BrokerState,
) -> OffsetCommitResponse {
    let commits = request
        .topics
        .iter()
        .flat_map(|topic| {
            topic.partitions.iter().map(|partition| OffsetCommit {
                topic: topic.name.as_str().to_owned(),
                partition: partition.partition_index,
                offset: partition.committed_offset,
                metadata: partition
                    .committed_metadata
                    .as_ref()
                    .map(|metadata| metadata.as_str().to_owned()),
            })
        })
        .collect::<Vec<_>>();
    let error_code = if request.group_instance_id.is_some() {
        ResponseError::UnsupportedVersion.code()
    } else {
        broker
            .groups()
            .commit_offsets(
                request.group_id.as_str(),
                request.generation_id_or_member_epoch,
                request.member_id.as_str(),
                commits.clone(),
            )
            .await
            .err()
            .map_or(0, |error| response_error(error).code())
    };

    if error_code == 0 {
        for commit in commits {
            tracing::info!(
                group = request.group_id.as_str(),
                topic = commit.topic,
                partition = commit.partition,
                offset = commit.offset,
                "committed consumer offset"
            );
        }
    }

    OffsetCommitResponse::default()
        .with_throttle_time_ms(0)
        .with_topics(
            request
                .topics
                .iter()
                .map(|topic| {
                    OffsetCommitResponseTopic::default()
                        .with_name(topic.name.clone())
                        .with_partitions(
                            topic
                                .partitions
                                .iter()
                                .map(|partition| {
                                    OffsetCommitResponsePartition::default()
                                        .with_partition_index(partition.partition_index)
                                        .with_error_code(error_code)
                                })
                                .collect(),
                        )
                })
                .collect(),
        )
}
