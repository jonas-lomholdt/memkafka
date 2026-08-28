use std::{ops::Range, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use rskafka::{
    BackoffConfig,
    client::{
        ClientBuilder,
        partition::{Compression, PartitionClient, UnknownTopicHandling},
    },
};
use serde::Serialize;
use tokio::{
    sync::watch,
    task::JoinSet,
    time::{Instant, timeout, timeout_at},
};

use crate::{config::WorkloadConfig, event};

const BYTES_PER_GIB: f64 = 1024.0 * 1024.0 * 1024.0;
const CONNECTION_DEADLINE: Duration = Duration::from_secs(10);
const REQUEST_DEADLINE: Duration = Duration::from_secs(30);
const WORKLOAD_DEADLINE: Duration = Duration::from_secs(10 * 60);
const FETCH_MAX_BYTES: i32 = 8 * 1024 * 1024;
const FETCH_MAX_WAIT_MS: i32 = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunMetrics {
    pub producer_seconds: f64,
    pub end_to_end_seconds: f64,
    pub messages: u64,
    pub value_bytes: u64,
}

impl RunMetrics {
    pub fn new(
        messages: u64,
        value_bytes: u64,
        producer_seconds: f64,
        end_to_end_seconds: f64,
    ) -> Self {
        Self {
            producer_seconds,
            end_to_end_seconds,
            messages,
            value_bytes,
        }
    }

    pub fn producer_records_per_second(&self) -> f64 {
        self.messages as f64 / self.producer_seconds
    }

    pub fn end_to_end_records_per_second(&self) -> f64 {
        self.messages as f64 / self.end_to_end_seconds
    }

    pub fn producer_gib_per_second(&self) -> f64 {
        self.value_bytes as f64 / BYTES_PER_GIB / self.producer_seconds
    }

    pub fn end_to_end_gib_per_second(&self) -> f64 {
        self.value_bytes as f64 / BYTES_PER_GIB / self.end_to_end_seconds
    }
}

fn partition_ranges(messages: u64, partitions: i32) -> Vec<Range<u64>> {
    let partitions = partitions as u64;
    let base = messages / partitions;
    let remainder = messages % partitions;

    (0..partitions)
        .map(|partition| 0..base + u64::from(partition < remainder))
        .collect()
}

pub async fn run(
    bootstrap_server: &str,
    topic: &str,
    config: &WorkloadConfig,
) -> Result<RunMetrics> {
    config
        .validate()
        .context("validate workload configuration")?;

    let value_bytes = config
        .messages
        .checked_mul(u64::try_from(config.payload_bytes).context("payload bytes exceed u64")?)
        .context("total value bytes overflow u64")?;
    let backoff = BackoffConfig {
        init_backoff: Duration::from_millis(50),
        max_backoff: Duration::from_secs(1),
        base: 2.0,
        deadline: Some(CONNECTION_DEADLINE),
    };
    let client = timeout(
        CONNECTION_DEADLINE,
        ClientBuilder::new(vec![bootstrap_server.to_owned()])
            .client_id("memkafka-throughput-benchmark")
            .backoff_config(backoff)
            .build(),
    )
    .await
    .with_context(|| format!("run topic {topic}: connect to broker {bootstrap_server} timed out"))?
    .with_context(|| format!("run topic {topic}: connect to broker {bootstrap_server}"))?;

    let controller = client
        .controller_client()
        .with_context(|| format!("run topic {topic}: create controller client"))?;
    timeout(
        CONNECTION_DEADLINE,
        controller.create_topic(topic, config.partitions, 1, 10_000),
    )
    .await
    .with_context(|| format!("run topic {topic}: topic creation timed out"))?
    .with_context(|| format!("run topic {topic}: create fresh topic"))?;

    let mut clients = Vec::with_capacity(config.partitions as usize);
    for partition in 0..config.partitions {
        let client = timeout(
            CONNECTION_DEADLINE,
            client.partition_client(topic, partition, UnknownTopicHandling::Retry),
        )
        .await
        .with_context(|| {
            format!("run topic {topic}, partition {partition}: client creation timed out")
        })?
        .with_context(|| format!("run topic {topic}, partition {partition}: create client"))?;
        clients.push(Arc::new(client));
    }

    let (start_tx, start_rx) = watch::channel(None);
    let mut producers = JoinSet::new();
    let mut consumers = JoinSet::new();
    for (partition, (client, range)) in clients
        .into_iter()
        .zip(partition_ranges(config.messages, config.partitions))
        .enumerate()
    {
        let partition = i32::try_from(partition).context("partition index exceeds i32")?;
        producers.spawn(produce_partition(
            Arc::clone(&client),
            partition,
            range.clone(),
            config.payload_bytes,
            config.batch_records,
            start_rx.clone(),
        ));
        consumers.spawn(consume_partition(
            client,
            partition,
            range.end,
            config.payload_bytes,
            start_rx.clone(),
        ));
    }

    let start = Instant::now();
    start_tx
        .send(Some(start))
        .context("publish common workload start instant")?;
    let deadline = start + WORKLOAD_DEADLINE;

    join_tasks(&mut producers, deadline, topic, "producer").await?;
    let producer_seconds = start.elapsed().as_secs_f64();
    join_tasks(&mut consumers, deadline, topic, "consumer").await?;
    let end_to_end_seconds = start.elapsed().as_secs_f64();

    Ok(RunMetrics::new(
        config.messages,
        value_bytes,
        producer_seconds,
        end_to_end_seconds,
    ))
}

async fn produce_partition(
    client: Arc<PartitionClient>,
    partition: i32,
    range: Range<u64>,
    payload_bytes: usize,
    batch_records: usize,
    mut start_rx: watch::Receiver<Option<Instant>>,
) -> Result<()> {
    wait_for_start(&mut start_rx)
        .await
        .with_context(|| format!("partition {partition}: wait for producer start"))?;

    let mut expected_offset = range.start;
    while expected_offset < range.end {
        let batch_end = range
            .end
            .min(expected_offset.saturating_add(batch_records as u64));
        let records = (expected_offset..batch_end)
            .map(|sequence| {
                event::record(partition, sequence, payload_bytes).with_context(|| {
                    format!(
                        "partition {partition}, expected offset {sequence}: build producer record"
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let offsets = timeout(
            REQUEST_DEADLINE,
            client.produce(records, Compression::NoCompression),
        )
        .await
        .with_context(|| {
            format!(
                "partition {partition}, expected offset {expected_offset}: produce request timed out"
            )
        })?
        .with_context(|| {
            format!("partition {partition}, expected offset {expected_offset}: produce batch")
        })?;

        let expected_count = usize::try_from(batch_end - expected_offset)
            .context("producer batch length exceeds usize")?;
        if offsets.len() != expected_count {
            bail!(
                "partition {partition}, expected offset {expected_offset}: expected {expected_count} acknowledged offsets, got {}",
                offsets.len()
            );
        }
        for (index, actual_offset) in offsets.into_iter().enumerate() {
            let offset = expected_offset + index as u64;
            let expected = i64::try_from(offset).context("producer offset exceeds i64")?;
            if actual_offset != expected {
                bail!(
                    "partition {partition}, expected offset {expected}: broker acknowledged offset {actual_offset}"
                );
            }
        }
        expected_offset = batch_end;
    }

    Ok(())
}

async fn consume_partition(
    client: Arc<PartitionClient>,
    partition: i32,
    expected_records: u64,
    payload_bytes: usize,
    mut start_rx: watch::Receiver<Option<Instant>>,
) -> Result<()> {
    wait_for_start(&mut start_rx)
        .await
        .with_context(|| format!("partition {partition}: wait for consumer start"))?;

    let mut expected_offset = 0_u64;
    while expected_offset < expected_records {
        let fetch_offset = i64::try_from(expected_offset).context("consumer offset exceeds i64")?;
        let (records, _high_watermark) = timeout(
            REQUEST_DEADLINE,
            client.fetch_records(fetch_offset, 1..FETCH_MAX_BYTES, FETCH_MAX_WAIT_MS),
        )
        .await
        .with_context(|| {
            format!(
                "partition {partition}, expected offset {expected_offset}: fetch request timed out"
            )
        })?
        .with_context(|| {
            format!("partition {partition}, expected offset {expected_offset}: fetch records")
        })?;

        for record in records {
            if expected_offset >= expected_records {
                bail!(
                    "partition {partition}, expected {expected_records} records: received unexpected offset {}",
                    record.offset
                );
            }
            event::validate(&record, partition, expected_offset, payload_bytes).with_context(
                || {
                    format!(
                        "partition {partition}, expected offset {expected_offset}: validate record"
                    )
                },
            )?;
            expected_offset += 1;
        }
    }

    Ok(())
}

async fn wait_for_start(start_rx: &mut watch::Receiver<Option<Instant>>) -> Result<Instant> {
    loop {
        if let Some(start) = *start_rx.borrow() {
            return Ok(start);
        }
        start_rx.changed().await.context("start sender dropped")?;
    }
}

async fn join_tasks(
    tasks: &mut JoinSet<Result<()>>,
    deadline: Instant,
    topic: &str,
    role: &str,
) -> Result<()> {
    timeout_at(deadline, async {
        while let Some(result) = tasks.join_next().await {
            result
                .with_context(|| format!("run topic {topic}: {role} task panicked"))?
                .with_context(|| format!("run topic {topic}: {role} task failed"))?;
        }
        Ok(())
    })
    .await
    .with_context(|| format!("run topic {topic}: {role} tasks exceeded workload deadline"))?
}

#[cfg(test)]
mod tests {
    use crate::config::WorkloadConfig;

    use super::{RunMetrics, partition_ranges, run};

    #[test]
    fn computes_producer_and_end_to_end_rates_from_exact_totals() {
        let metrics = RunMetrics::new(1_000, 4_096_000, 0.5, 0.8);

        assert_eq!(metrics.producer_records_per_second(), 2_000.0);
        assert_eq!(metrics.end_to_end_records_per_second(), 1_250.0);
        assert!((metrics.producer_gib_per_second() - 0.0076293945).abs() < 1e-9);
    }

    #[test]
    fn distributes_the_remainder_to_the_lowest_partition_numbers() {
        let ranges = partition_ranges(10, 3);

        assert_eq!(ranges, vec![0..4, 0..3, 0..3]);
    }

    #[tokio::test]
    async fn rejects_an_invalid_workload_before_connecting() {
        let config = WorkloadConfig {
            messages: 0,
            ..WorkloadConfig::default()
        };

        let error = run("127.0.0.1:1", "unused", &config).await.unwrap_err();

        assert!(
            error
                .to_string()
                .contains("validate workload configuration")
        );
    }
}
