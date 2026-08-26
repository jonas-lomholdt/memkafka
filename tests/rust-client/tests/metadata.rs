use std::{
    collections::BTreeSet,
    env, process,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use chrono::{DateTime, Utc};
use rskafka::{
    BackoffConfig,
    client::{
        Client, ClientBuilder,
        error::Error,
        partition::{Compression, OffsetAt, UnknownTopicHandling},
    },
    record::{Record, RecordAndOffset},
};

#[tokio::test]
async fn metadata_auto_creates_two_partitions() {
    let client = client().await;
    let topic = unique_topic("rust-auto");

    client
        .partition_client(topic.clone(), 0, UnknownTopicHandling::Retry)
        .await
        .expect("topic-specific metadata should auto-create the topic");

    assert_eq!(
        topic_partitions(&client, &topic).await,
        BTreeSet::from([0, 1])
    );
}

#[tokio::test]
async fn admin_creates_six_partition_topic() {
    let client = client().await;
    let topic = unique_topic("rust-explicit");

    client
        .controller_client()
        .expect("controller client")
        .create_topic(topic.clone(), 6, 1, 5_000)
        .await
        .expect("six-partition topic should be created");

    assert_eq!(
        topic_partitions(&client, &topic).await,
        BTreeSet::from([0, 1, 2, 3, 4, 5])
    );
}

#[tokio::test]
async fn admin_rejects_replication_factor_two() {
    let client = client().await;
    let topic = unique_topic("rust-invalid-rf");

    let failure = client
        .controller_client()
        .expect("controller client")
        .create_topic(topic, 2, 2, 5_000)
        .await
        .expect_err("replication factor 2 must fail");

    match failure {
        Error::ServerError { protocol_error, .. } => {
            assert_eq!(protocol_error.to_string(), "InvalidReplicationFactor");
        }
        other => panic!("expected InvalidReplicationFactor, got {other:?}"),
    }
}

#[tokio::test]
async fn publishes_and_fetches_in_order_then_reads_uncommitted_records_again() {
    let client = client().await;
    let topic = unique_topic("rust-delivery");
    client
        .controller_client()
        .expect("controller client")
        .create_topic(topic.clone(), 1, 1, 5_000)
        .await
        .expect("single-partition topic should be created");
    let partition = client
        .partition_client(topic, 0, UnknownTopicHandling::Error)
        .await
        .expect("partition client");

    for index in 0..10_i64 {
        let offsets = partition
            .produce(
                vec![Record {
                    key: Some(format!("key-{index}").into_bytes()),
                    value: Some(format!("message-{index}").into_bytes()),
                    headers: [("source".to_owned(), b"rust-test".to_vec())]
                        .into_iter()
                        .collect(),
                    timestamp: DateTime::<Utc>::from_timestamp_millis(index)
                        .expect("small timestamp"),
                }],
                Compression::NoCompression,
            )
            .await
            .expect("produce acknowledged record");
        assert_eq!(offsets, vec![index]);
    }

    assert_eq!(
        partition
            .get_offset(OffsetAt::Earliest)
            .await
            .expect("earliest offset"),
        0
    );
    assert_eq!(
        partition
            .get_offset(OffsetAt::Latest)
            .await
            .expect("latest offset"),
        10
    );

    let first = partition
        .fetch_records(0, 1..1_000_000, 1_000)
        .await
        .expect("first fetch")
        .0;
    assert_sequence(&first);

    let repeated = partition
        .fetch_records(0, 1..1_000_000, 1_000)
        .await
        .expect("repeated fetch")
        .0;
    assert_sequence(&repeated);
}

fn assert_sequence(records: &[RecordAndOffset]) {
    assert_eq!(records.len(), 10);
    for (index, record) in records.iter().enumerate() {
        assert_eq!(record.offset, index as i64);
        assert_eq!(
            record.record.key.as_deref(),
            Some(format!("key-{index}").as_bytes())
        );
        assert_eq!(
            record.record.value.as_deref(),
            Some(format!("message-{index}").as_bytes())
        );
        assert_eq!(
            record.record.headers.get("source").map(Vec::as_slice),
            Some(b"rust-test".as_slice())
        );
    }
}

async fn client() -> Client {
    ClientBuilder::new(vec![bootstrap_servers()])
        .backoff_config(BackoffConfig {
            deadline: Some(Duration::from_secs(5)),
            ..BackoffConfig::default()
        })
        .build()
        .await
        .expect("connect to MemKafka")
}

async fn topic_partitions(client: &Client, topic: &str) -> BTreeSet<i32> {
    client
        .list_topics()
        .await
        .expect("list topics")
        .into_iter()
        .find(|candidate| candidate.name == topic)
        .unwrap_or_else(|| panic!("metadata omitted topic {topic}"))
        .partitions
}

fn bootstrap_servers() -> String {
    env::var("MEMKAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "127.0.0.1:9092".to_owned())
}

fn unique_topic(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after Unix epoch")
        .as_nanos();
    format!("{prefix}-{}-{nanos}", process::id())
}
