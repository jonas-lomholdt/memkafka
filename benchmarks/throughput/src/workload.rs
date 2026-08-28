use std::{collections::HashMap, fmt, future::Future, ops::Range, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use rskafka::{
    BackoffConfig,
    client::{
        ClientBuilder,
        partition::{Compression, OffsetAt, PartitionClient, UnknownTopicHandling},
    },
};
use serde::Serialize;
use tokio::{
    sync::watch,
    task::{Id, JoinSet},
    time::{Instant, timeout, timeout_at},
};

use crate::{config::WorkloadConfig, event};

const BYTES_PER_GIB: f64 = 1024.0 * 1024.0 * 1024.0;
const CONNECTION_DEADLINE: Duration = Duration::from_secs(10);
const REQUEST_DEADLINE: Duration = Duration::from_secs(30);
const WORKLOAD_DEADLINE: Duration = Duration::from_secs(10 * 60);
const FETCH_MAX_BYTES: i32 = 8 * 1024 * 1024;
const FETCH_MAX_WAIT_MS: i32 = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskRole {
    Producer,
    Consumer,
}

impl fmt::Display for TaskRole {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Producer => formatter.write_str("producer"),
            Self::Consumer => formatter.write_str("consumer"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TaskContext {
    role: TaskRole,
    partition: i32,
    expected_final_offset: u64,
}

impl TaskContext {
    fn new(role: TaskRole, partition: i32, expected_final_offset: u64) -> Self {
        Self {
            role,
            partition,
            expected_final_offset,
        }
    }
}

impl fmt::Display for TaskContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} partition {}, expected final offset {}",
            self.role, self.partition, self.expected_final_offset
        )
    }
}

#[derive(Default)]
struct TaskGroup {
    tasks: JoinSet<Result<()>>,
    contexts: HashMap<Id, TaskContext>,
}

impl TaskGroup {
    fn spawn<F>(&mut self, context: TaskContext, task: F)
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        let handle = self.tasks.spawn(task);
        self.contexts.insert(handle.id(), context);
    }

    async fn abort_and_drain(&mut self) {
        self.tasks.abort_all();
        while self.tasks.join_next().await.is_some() {}
        self.contexts.clear();
    }

    fn active_contexts(&self) -> String {
        let mut contexts = self
            .contexts
            .values()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        contexts.sort();
        contexts.join("; ")
    }
}

#[derive(Default)]
struct TaskProgress {
    producers: usize,
    consumers: usize,
    producer_seconds: Option<f64>,
    end_to_end_seconds: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct TaskTimings {
    producer_seconds: f64,
    end_to_end_seconds: f64,
}

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
    let mut tasks = TaskGroup::default();
    for (partition, (client, range)) in clients
        .iter()
        .cloned()
        .zip(partition_ranges(config.messages, config.partitions))
        .enumerate()
    {
        let partition = i32::try_from(partition)
            .with_context(|| format!("run topic {topic}: partition index exceeds i32"))?;
        let expected_final_offset = range.end;
        tasks.spawn(
            TaskContext::new(TaskRole::Producer, partition, expected_final_offset),
            produce_partition(
                Arc::clone(&client),
                partition,
                range,
                config.payload_bytes,
                config.batch_records,
                start_rx.clone(),
            ),
        );
        tasks.spawn(
            TaskContext::new(TaskRole::Consumer, partition, expected_final_offset),
            consume_partition(
                client,
                partition,
                expected_final_offset,
                config.payload_bytes,
                start_rx.clone(),
            ),
        );
    }

    let start = Instant::now();
    start_tx
        .send(Some(start))
        .context("publish common workload start instant")?;
    let deadline = start + WORKLOAD_DEADLINE;

    let timings = coordinate_tasks(
        tasks,
        config.partitions as usize,
        config.partitions as usize,
        start,
        deadline,
        topic,
    )
    .await?;
    validate_final_partition_lengths(&clients, config, deadline, topic).await?;

    Ok(RunMetrics::new(
        config.messages,
        value_bytes,
        timings.producer_seconds,
        timings.end_to_end_seconds,
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

async fn coordinate_tasks(
    mut tasks: TaskGroup,
    expected_producers: usize,
    expected_consumers: usize,
    start: Instant,
    deadline: Instant,
    topic: &str,
) -> Result<TaskTimings> {
    let mut progress = TaskProgress::default();
    let outcome = timeout_at(deadline, async {
        while let Some(result) = tasks.tasks.join_next_with_id().await {
            let (id, result) = match result {
                Ok(result) => result,
                Err(error) => {
                    let context = tasks.contexts.remove(&error.id()).with_context(|| {
                        format!("run topic {topic}: find context for panicked task")
                    })?;
                    bail!("run topic {topic}: {context} panicked: {error}");
                }
            };
            let context = tasks
                .contexts
                .remove(&id)
                .with_context(|| format!("run topic {topic}: find context for completed task"))?;
            result.with_context(|| format!("run topic {topic}: {context} failed"))?;

            match context.role {
                TaskRole::Producer => {
                    progress.producers += 1;
                    if progress.producers == expected_producers {
                        progress.producer_seconds = Some(start.elapsed().as_secs_f64());
                    }
                }
                TaskRole::Consumer => {
                    progress.consumers += 1;
                    if progress.consumers == expected_consumers {
                        progress.end_to_end_seconds = Some(start.elapsed().as_secs_f64());
                    }
                }
            }
        }

        if progress.producers != expected_producers || progress.consumers != expected_consumers {
            bail!(
                "run topic {topic}: task set ended after {}/{} producers and {}/{} consumers",
                progress.producers,
                expected_producers,
                progress.consumers,
                expected_consumers
            );
        }
        let producer_seconds = progress
            .producer_seconds
            .context("all producer tasks completed without a producer completion time")?;
        let end_to_end_seconds = progress
            .end_to_end_seconds
            .context("all consumer tasks completed without an end-to-end completion time")?;
        Ok(TaskTimings {
            producer_seconds,
            end_to_end_seconds,
        })
    })
    .await;

    match outcome {
        Ok(Ok(timings)) => Ok(timings),
        Ok(Err(error)) => {
            tasks.abort_and_drain().await;
            Err(error)
        }
        Err(_) => {
            let active = tasks.active_contexts();
            tasks.abort_and_drain().await;
            bail!(
                "run topic {topic}: workload deadline exceeded after {}/{} producers and {}/{} consumers; active tasks: {active}",
                progress.producers,
                expected_producers,
                progress.consumers,
                expected_consumers
            )
        }
    }
}

async fn validate_final_partition_lengths(
    clients: &[Arc<PartitionClient>],
    config: &WorkloadConfig,
    deadline: Instant,
    topic: &str,
) -> Result<()> {
    for (partition, client) in clients.iter().enumerate() {
        let partition = i32::try_from(partition)
            .with_context(|| format!("run topic {topic}: partition index exceeds i32"))?;
        let expected_records = config.records_in_partition(partition);
        let latest_offset = timeout_at(
            deadline,
            timeout(REQUEST_DEADLINE, client.get_offset(OffsetAt::Latest)),
        )
        .await
        .with_context(|| {
            format!(
                "run topic {topic}, partition {partition}, expected final offset {expected_records}: workload deadline exceeded during final offset check"
            )
        })?
        .with_context(|| {
            format!(
                "run topic {topic}, partition {partition}, expected final offset {expected_records}: latest-offset request timed out"
            )
        })?
        .with_context(|| {
            format!(
                "run topic {topic}, partition {partition}, expected final offset {expected_records}: fetch latest offset"
            )
        })?;
        validate_partition_length(partition, expected_records, latest_offset)
            .with_context(|| format!("run topic {topic}: validate final partition length"))?;
    }

    Ok(())
}

fn validate_partition_length(
    partition: i32,
    expected_records: u64,
    latest_offset: i64,
) -> Result<()> {
    let expected_offset = i64::try_from(expected_records).with_context(|| {
        format!("partition {partition}: expected final offset exceeds i64 ({expected_records})")
    })?;
    if latest_offset != expected_offset {
        bail!(
            "partition {partition}: expected final offset {expected_offset}, got {latest_offset}"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use anyhow::bail;
    use tokio::time::{Instant, sleep};

    use crate::config::WorkloadConfig;

    use super::{
        RunMetrics, TaskContext, TaskGroup, TaskRole, coordinate_tasks, partition_ranges, run,
        validate_partition_length,
    };

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

    #[tokio::test]
    async fn reports_a_consumer_failure_without_waiting_for_a_slow_producer() {
        let mut tasks = TaskGroup::default();
        tasks.spawn(TaskContext::new(TaskRole::Producer, 0, 10), async move {
            sleep(Duration::from_secs(5)).await;
            bail!("producer request timed out")
        });
        tasks.spawn(TaskContext::new(TaskRole::Consumer, 0, 10), async move {
            bail!("record validation failed")
        });
        let started = Instant::now();

        let error = coordinate_tasks(
            tasks,
            1,
            1,
            started,
            started + Duration::from_secs(1),
            "test-topic",
        )
        .await
        .unwrap_err();

        assert!(started.elapsed() < Duration::from_millis(500));
        let error = format!("{error:#}");
        assert!(error.contains("consumer partition 0"));
        assert!(error.contains("record validation failed"));
        assert!(!error.contains("producer request timed out"));
    }

    #[tokio::test]
    async fn captures_end_to_end_time_before_post_consumer_validation_delay() {
        let mut tasks = TaskGroup::default();
        tasks.spawn(TaskContext::new(TaskRole::Producer, 0, 10), async {
            Ok(())
        });
        tasks.spawn(TaskContext::new(TaskRole::Consumer, 0, 10), async {
            Ok(())
        });
        let started = Instant::now();

        let timings = coordinate_tasks(
            tasks,
            1,
            1,
            started,
            started + Duration::from_secs(1),
            "test-topic",
        )
        .await
        .unwrap();
        sleep(Duration::from_millis(20)).await;

        assert!(
            timings.end_to_end_seconds < started.elapsed().as_secs_f64(),
            "post-consumer validation delay must not change captured end-to-end time"
        );
    }

    #[test]
    fn rejects_a_final_partition_offset_beyond_the_expected_record_count() {
        let error = validate_partition_length(2, 3, 4).unwrap_err();

        let error = error.to_string();
        assert!(error.contains("partition 2"));
        assert!(error.contains("expected final offset 3"));
        assert!(error.contains("got 4"));
    }
}
