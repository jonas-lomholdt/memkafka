use std::{
    collections::BTreeSet,
    env, process,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rskafka::{
    BackoffConfig,
    client::{Client, ClientBuilder, error::Error, partition::UnknownTopicHandling},
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
