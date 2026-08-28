use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use clap::Parser;
use memkafka_throughput_benchmark::{config::WorkloadConfig, workload};

#[derive(Debug, Parser)]
#[command(about = "Measure validated end-to-end Kafka throughput")]
struct Args {
    #[arg(long)]
    bootstrap_server: Option<String>,

    #[arg(long, default_value_t = 1_000_000)]
    messages: u64,

    #[arg(long, default_value_t = 4_096)]
    payload_bytes: usize,

    #[arg(long, default_value_t = 8)]
    partitions: i32,

    #[arg(long, default_value_t = 256)]
    batch_records: usize,

    #[arg(long, default_value = "memkafka-throughput")]
    topic_prefix: String,

    #[arg(long)]
    output_json: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let bootstrap_server = args.bootstrap_server.as_deref().context(
        "--bootstrap-server is required until local broker orchestration is added in Task 3",
    )?;
    let config = WorkloadConfig {
        messages: args.messages,
        payload_bytes: args.payload_bytes,
        partitions: args.partitions,
        batch_records: args.batch_records,
    };
    config
        .validate()
        .context("validate command-line workload")?;

    let topic = unique_topic(&args.topic_prefix)?;
    let metrics = workload::run(bootstrap_server, &topic, &config)
        .await
        .with_context(|| format!("external broker run for topic {topic}"))?;
    let json = serde_json::to_vec_pretty(&metrics).context("serialize run metrics")?;
    std::fs::write(&args.output_json, json)
        .with_context(|| format!("write run metrics to {}", args.output_json.display()))?;

    Ok(())
}

fn unique_topic(prefix: &str) -> Result<String> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time is before Unix epoch")?
        .as_nanos();
    Ok(format!("{prefix}-{}-{timestamp}", std::process::id()))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use clap::Parser;

    use super::Args;

    #[test]
    fn parses_the_external_broker_workload_flags() {
        let args = Args::try_parse_from([
            "benchmark",
            "--bootstrap-server",
            "127.0.0.1:19092",
            "--messages",
            "1000",
            "--payload-bytes",
            "4096",
            "--partitions",
            "8",
            "--batch-records",
            "256",
            "--topic-prefix",
            "smoke",
            "--output-json",
            "/tmp/smoke.json",
        ])
        .unwrap();

        assert_eq!(args.bootstrap_server.as_deref(), Some("127.0.0.1:19092"));
        assert_eq!(args.messages, 1_000);
        assert_eq!(args.payload_bytes, 4_096);
        assert_eq!(args.partitions, 8);
        assert_eq!(args.batch_records, 256);
        assert_eq!(args.topic_prefix, "smoke");
        assert_eq!(args.output_json, PathBuf::from("/tmp/smoke.json"));
    }
}
