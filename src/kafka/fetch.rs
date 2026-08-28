use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::{
    ResponseError,
    messages::{
        FetchRequest, FetchResponse,
        fetch_request::FetchPartition,
        fetch_response::{FetchableTopicResponse, PartitionData},
    },
};
use tokio::time::{Instant, sleep_until};

use crate::broker::{BrokerState, partition::FetchError};

pub(crate) async fn response(request: &FetchRequest, broker: &BrokerState) -> FetchResponse {
    let max_wait = u64::try_from(request.max_wait_ms).unwrap_or(0);
    let min_bytes = usize::try_from(request.min_bytes).unwrap_or(0);
    let deadline = Instant::now() + Duration::from_millis(max_wait);

    loop {
        let notified = broker.append_notification().notified_owned();
        tokio::pin!(notified);
        notified.as_mut().enable();

        let snapshot = snapshot(request, broker).await;
        if snapshot.record_bytes >= min_bytes
            || min_bytes == 0
            || max_wait == 0
            || snapshot.valid_partitions == 0
            || Instant::now() >= deadline
        {
            return snapshot.response;
        }

        tokio::select! {
            () = notified.as_mut() => {}
            () = sleep_until(deadline) => return snapshot.response,
        }
    }
}

struct FetchSnapshot {
    response: FetchResponse,
    record_bytes: usize,
    valid_partitions: usize,
}

async fn snapshot(request: &FetchRequest, broker: &BrokerState) -> FetchSnapshot {
    let max_bytes = usize::try_from(request.max_bytes).unwrap_or(0);
    let mut record_bytes = 0_usize;
    let mut valid_partitions = 0_usize;
    let mut responses = Vec::with_capacity(request.topics.len());

    for topic in &request.topics {
        let mut partitions = Vec::with_capacity(topic.partitions.len());
        for partition in &topic.partitions {
            let remaining = max_bytes.saturating_sub(record_bytes);
            let partition_max = usize::try_from(partition.partition_max_bytes).unwrap_or(0);
            let mut response = partition_response(
                topic.topic.as_str(),
                partition,
                partition_max.min(remaining),
                broker,
            )
            .await;

            if response.error_code == 0 {
                valid_partitions += 1;
            }
            if let Some(records) = response.records.as_mut() {
                let exceeds_request_limit = record_bytes > 0
                    && record_bytes
                        .checked_add(records.len())
                        .is_none_or(|size| size > max_bytes);
                if exceeds_request_limit {
                    *records = Bytes::new();
                } else {
                    record_bytes = record_bytes.saturating_add(records.len());
                }
            }
            partitions.push(response);
        }
        responses.push(
            FetchableTopicResponse::default()
                .with_topic(topic.topic.clone())
                .with_partitions(partitions),
        );
    }

    FetchSnapshot {
        response: FetchResponse::default()
            .with_throttle_time_ms(0)
            .with_responses(responses),
        record_bytes,
        valid_partitions,
    }
}

async fn partition_response(
    topic: &str,
    request: &FetchPartition,
    max_bytes: usize,
    broker: &BrokerState,
) -> PartitionData {
    let Some(log) = broker.topics().partition(topic, request.partition).await else {
        return error_partition(request.partition, ResponseError::UnknownTopicOrPartition);
    };

    match log.fetch(request.fetch_offset, max_bytes).await {
        Ok(snapshot) => PartitionData::default()
            .with_partition_index(request.partition)
            .with_high_watermark(snapshot.high_watermark)
            .with_last_stable_offset(snapshot.high_watermark)
            .with_aborted_transactions(Some(Vec::new()))
            .with_records(Some(snapshot.records)),
        Err(FetchError::OutOfRange) => {
            error_partition(request.partition, ResponseError::OffsetOutOfRange)
        }
    }
}

fn error_partition(index: i32, error: ResponseError) -> PartitionData {
    PartitionData::default()
        .with_partition_index(index)
        .with_error_code(error.code())
        .with_high_watermark(-1)
        .with_last_stable_offset(-1)
        .with_aborted_transactions(Some(Vec::new()))
        .with_records(Some(Bytes::new()))
}
