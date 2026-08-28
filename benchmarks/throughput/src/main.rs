use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use clap::Parser;
use memkafka_throughput_benchmark::{
    broker::BrokerGuard,
    config::WorkloadConfig,
    report::{self, BenchmarkReport, BenchmarkRun},
    workload,
};

#[derive(Debug, Parser)]
#[command(
    about = "Measure validated end-to-end Kafka throughput",
    args_override_self = true
)]
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

    #[arg(long, default_value = "target/release/memkafka")]
    broker_binary: PathBuf,

    #[arg(long, default_value_t = 1)]
    runs: usize,

    #[arg(long)]
    skip_memory_check: bool,

    #[arg(long)]
    output_json: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = WorkloadConfig {
        messages: args.messages,
        payload_bytes: args.payload_bytes,
        partitions: args.partitions,
        batch_records: args.batch_records,
    };
    config
        .validate()
        .context("validate command-line workload")?;
    anyhow::ensure!(args.runs > 0, "--runs must be greater than zero");

    if let Some(bootstrap_server) = args.bootstrap_server.as_deref() {
        return run_external(&args, bootstrap_server, &config).await;
    }
    run_local(&args, config).await
}

async fn run_external(args: &Args, bootstrap_server: &str, config: &WorkloadConfig) -> Result<()> {
    anyhow::ensure!(
        args.runs == 1,
        "--runs is local-broker only; external-broker mode performs exactly one run"
    );
    let topic = unique_topic(&args.topic_prefix)?;
    let metrics = workload::run(bootstrap_server, &topic, config)
        .await
        .with_context(|| format!("external broker run for topic {topic}"))?;
    let json = serde_json::to_vec_pretty(&metrics).context("serialize run metrics")?;
    std::fs::write(&args.output_json, json)
        .with_context(|| format!("write run metrics to {}", args.output_json.display()))?;

    Ok(())
}

async fn run_local(args: &Args, config: WorkloadConfig) -> Result<()> {
    if !args.skip_memory_check {
        report::ensure_available_memory(&config, report::available_memory())
            .context("benchmark memory preflight")?;
    }

    let mut runs = Vec::with_capacity(args.runs);
    for run_number in 1..=args.runs {
        let mut broker = BrokerGuard::start(&args.broker_binary).with_context(|| {
            format!(
                "run {run_number}/{}: start fresh broker {}",
                args.runs,
                args.broker_binary.display()
            )
        })?;
        let topic = unique_topic(&format!("{}-run-{run_number}", args.topic_prefix))?;
        let pid = broker.pid();
        let log_path = broker.log_path().to_path_buf();
        eprintln!(
            "run {run_number}/{}: broker PID {pid}, topic {topic}, log {}",
            args.runs,
            log_path.display()
        );

        let metrics =
            match workload::run(broker.bootstrap_server(), &topic, &config).await {
                Ok(metrics) => metrics,
                Err(error) => {
                    let shutdown_result = broker.stop();
                    let error = error.context(format!(
                    "run {run_number}/{} failed for topic {topic} with broker PID {pid} (log: {})",
                    args.runs,
                    log_path.display()
                ));
                    return match shutdown_result {
                        Ok(()) => Err(error),
                        Err(shutdown_error) => Err(error
                            .context(format!("broker cleanup also failed: {shutdown_error:#}"))),
                    };
                }
            };

        let peak_result = broker.peak_rss_bytes().with_context(|| {
            format!(
                "run {run_number}/{}: capture peak RSS for broker PID {pid} (log: {})",
                args.runs,
                log_path.display()
            )
        });
        let shutdown_result = broker.stop().with_context(|| {
            format!(
                "run {run_number}/{}: stop broker PID {pid} (log: {})",
                args.runs,
                log_path.display()
            )
        });
        let peak_rss_bytes = peak_result?;
        shutdown_result?;

        let run = BenchmarkRun::new(run_number, topic, pid, metrics, peak_rss_bytes);
        eprintln!(
            "run {run_number}/{}: producer {:.0} records/s, end-to-end {:.0} records/s, peak RSS {:.2} MiB",
            args.runs,
            run.producer_records_per_second,
            run.end_to_end_records_per_second,
            run.peak_rss_bytes as f64 / (1024.0 * 1024.0),
        );
        runs.push(run);
    }

    let report = BenchmarkReport::capture(config, runs).context("aggregate benchmark report")?;
    report::write_json_atomic(&args.output_json, &report)
        .with_context(|| format!("write benchmark report to {}", args.output_json.display()))?;
    eprintln!(
        "median: producer {:.0} records/s, end-to-end {:.0} records/s; report {}",
        report.median.producer_records_per_second,
        report.median.end_to_end_records_per_second,
        args.output_json.display(),
    );
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
        assert_eq!(args.runs, 1);
        assert!(!args.skip_memory_check);
    }

    #[test]
    fn parses_local_broker_orchestration_flags() {
        let args = Args::try_parse_from([
            "benchmark",
            "--broker-binary",
            "/tmp/memkafka",
            "--runs",
            "3",
            "--skip-memory-check",
            "--output-json",
            "/tmp/local.json",
        ])
        .unwrap();

        assert_eq!(args.bootstrap_server, None);
        assert_eq!(args.broker_binary, PathBuf::from("/tmp/memkafka"));
        assert_eq!(args.runs, 3);
        assert!(args.skip_memory_check);
        assert_eq!(args.output_json, PathBuf::from("/tmp/local.json"));
    }
}
