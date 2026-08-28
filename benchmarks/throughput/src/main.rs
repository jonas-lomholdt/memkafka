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

#[cfg(unix)]
struct LocalSignals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl LocalSignals {
    fn new() -> Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};

        Ok(Self {
            interrupt: signal(SignalKind::interrupt()).context("listen for SIGINT")?,
            terminate: signal(SignalKind::terminate()).context("listen for SIGTERM")?,
        })
    }

    async fn receive(&mut self) -> Result<&'static str> {
        tokio::select! {
            signal = self.interrupt.recv() => signal.map(|()| "SIGINT").context("SIGINT stream closed"),
            signal = self.terminate.recv() => signal.map(|()| "SIGTERM").context("SIGTERM stream closed"),
        }
    }
}

#[cfg(windows)]
struct LocalSignals {
    ctrl_c: tokio::signal::windows::CtrlC,
}

#[cfg(windows)]
impl LocalSignals {
    fn new() -> Result<Self> {
        Ok(Self {
            ctrl_c: tokio::signal::windows::ctrl_c().context("listen for Ctrl-C")?,
        })
    }

    async fn receive(&mut self) -> Result<&'static str> {
        self.ctrl_c
            .recv()
            .await
            .map(|()| "Ctrl-C")
            .context("Ctrl-C stream closed")
    }
}

#[cfg(not(any(unix, windows)))]
struct LocalSignals;

#[cfg(not(any(unix, windows)))]
impl LocalSignals {
    fn new() -> Result<Self> {
        Ok(Self)
    }

    async fn receive(&mut self) -> Result<&'static str> {
        tokio::signal::ctrl_c().await.context("listen for Ctrl-C")?;
        Ok("Ctrl-C")
    }
}

#[derive(Clone)]
struct InterruptionLatch {
    sender: tokio::sync::watch::Sender<Option<&'static str>>,
}

impl InterruptionLatch {
    fn new() -> (Self, tokio::sync::watch::Receiver<Option<&'static str>>) {
        let (sender, receiver) = tokio::sync::watch::channel(None);
        (Self { sender }, receiver)
    }

    fn record(&self, signal: &'static str) {
        self.sender.send_if_modified(|latched| {
            if latched.is_some() {
                false
            } else {
                *latched = Some(signal);
                true
            }
        });
    }
}

struct SignalMonitor {
    receiver: tokio::sync::watch::Receiver<Option<&'static str>>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<Result<()>>>,
}

impl SignalMonitor {
    fn start() -> Result<Self> {
        let mut signals = LocalSignals::new().context("install local benchmark signal handlers")?;
        let (latch, receiver) = InterruptionLatch::new();
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            tokio::select! {
                biased;
                interruption = signals.receive() => {
                    latch.record(interruption?);
                    let _ = (&mut stopped).await;
                    Ok(())
                }
                _ = &mut stopped => Ok(()),
            }
        });
        Ok(Self {
            receiver,
            stop: Some(stop),
            task: Some(task),
        })
    }

    async fn wait_for_interruption(&mut self) -> Result<&'static str> {
        loop {
            if let Some(signal) = *self.receiver.borrow() {
                return Ok(signal);
            }
            self.receiver
                .changed()
                .await
                .context("local signal monitor stopped before interruption")?;
        }
    }

    async fn latched_after_yield(&self) -> Option<&'static str> {
        tokio::task::yield_now().await;
        *self.receiver.borrow()
    }

    async fn checkpoint(&self, checkpoint: &str) -> Result<()> {
        tokio::task::yield_now().await;
        ensure_not_interrupted(&self.receiver, checkpoint)
    }

    async fn stop(mut self, require_success_gate: bool) -> Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.await.context("join local signal monitor")??;
        }
        if require_success_gate {
            ensure_not_interrupted(&self.receiver, "final local lifecycle gate")?;
        }
        Ok(())
    }
}

fn ensure_not_interrupted(
    receiver: &tokio::sync::watch::Receiver<Option<&'static str>>,
    checkpoint: &str,
) -> Result<()> {
    if let Some(signal) = *receiver.borrow() {
        anyhow::bail!("benchmark interrupted by {signal} at {checkpoint}");
    }
    Ok(())
}

enum ActiveRunOutcome {
    Workload(Result<workload::RunMetrics>),
    Interrupted(Result<&'static str>),
}

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
    let mut monitor = SignalMonitor::start()?;
    let result = run_local_inner(args, config, &mut monitor).await;
    let require_success_gate = result.is_ok();
    let monitor_result = monitor.stop(require_success_gate).await;
    combine_local_and_monitor(result, monitor_result)
}

async fn run_local_inner(
    args: &Args,
    config: WorkloadConfig,
    monitor: &mut SignalMonitor,
) -> Result<()> {
    if !args.skip_memory_check {
        report::ensure_available_memory(&config, report::available_memory())
            .context("benchmark memory preflight")?;
    }

    let mut runs = Vec::with_capacity(args.runs);
    for run_number in 1..=args.runs {
        monitor.checkpoint("before fresh broker spawn").await?;
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
        if let Some(signal) = monitor.latched_after_yield().await {
            let shutdown_result = broker.stop().with_context(|| {
                format!(
                    "clean up interrupted run {run_number}/{} broker PID {pid} after startup (log: {})",
                    args.runs,
                    log_path.display()
                )
            });
            return interruption_result(Ok(signal), shutdown_result, pid, &log_path);
        }

        let outcome = tokio::select! {
            biased;
            interruption = monitor.wait_for_interruption() => {
                ActiveRunOutcome::Interrupted(interruption)
            }
            result = workload::run(broker.bootstrap_server(), &topic, &config) => {
                ActiveRunOutcome::Workload(result)
            }
        };
        let post_selection_interruption = monitor.latched_after_yield().await;
        let metrics = match (post_selection_interruption, outcome) {
            (Some(signal), _) => {
                let shutdown_result = broker.stop().with_context(|| {
                        format!(
                            "clean up interrupted run {run_number}/{} broker PID {pid} after workload selection (log: {})",
                            args.runs,
                            log_path.display()
                        )
                    });
                return interruption_result(Ok(signal), shutdown_result, pid, &log_path);
            }
            (None, ActiveRunOutcome::Workload(Ok(metrics))) => metrics,
            (None, ActiveRunOutcome::Workload(Err(error))) => {
                let shutdown_result = broker.stop();
                let error = error.context(format!(
                    "run {run_number}/{} failed for topic {topic} with broker PID {pid} (log: {})",
                    args.runs,
                    log_path.display()
                ));
                return match shutdown_result {
                    Ok(()) => Err(error),
                    Err(shutdown_error) => {
                        Err(error
                            .context(format!("broker cleanup also failed: {shutdown_error:#}")))
                    }
                };
            }
            (None, ActiveRunOutcome::Interrupted(interruption)) => {
                let shutdown_result = broker.stop().with_context(|| {
                    format!(
                        "clean up interrupted run {run_number}/{} broker PID {pid} (log: {})",
                        args.runs,
                        log_path.display()
                    )
                });
                return interruption_result(interruption, shutdown_result, pid, &log_path);
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
        if let Some(signal) = monitor.latched_after_yield().await {
            let cleanup_result = combine_peak_and_shutdown(peak_result, shutdown_result).map(drop);
            return interruption_result(Ok(signal), cleanup_result, pid, &log_path);
        }
        let peak_rss_bytes = combine_peak_and_shutdown(peak_result, shutdown_result)?;

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

    monitor.checkpoint("before report aggregation").await?;
    let report = BenchmarkReport::capture(config, runs).context("aggregate benchmark report")?;
    monitor.checkpoint("before report writing").await?;
    report::write_json_atomic(&args.output_json, &report)
        .with_context(|| format!("write benchmark report to {}", args.output_json.display()))?;
    monitor.checkpoint("before local benchmark success").await?;
    eprintln!(
        "median: producer {:.0} records/s, end-to-end {:.0} records/s; report {}",
        report.median.producer_records_per_second,
        report.median.end_to_end_records_per_second,
        args.output_json.display(),
    );
    Ok(())
}

fn combine_local_and_monitor(result: Result<()>, monitor_result: Result<()>) -> Result<()> {
    match (result, monitor_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(monitor_error)) => Err(error.context(format!(
            "signal monitor shutdown also failed: {monitor_error:#}"
        ))),
    }
}

fn interruption_result(
    interruption: Result<&'static str>,
    shutdown_result: Result<()>,
    pid: u32,
    log_path: &std::path::Path,
) -> Result<()> {
    match (interruption, shutdown_result) {
        (Ok(signal), Ok(())) => anyhow::bail!(
            "benchmark interrupted by {signal}; broker PID {pid} was stopped (log: {})",
            log_path.display()
        ),
        (Ok(signal), Err(shutdown_error)) => anyhow::bail!(
            "benchmark interrupted by {signal}, and cleanup failed for broker PID {pid}: {shutdown_error:#}"
        ),
        (Err(signal_error), Ok(())) => {
            Err(signal_error.context(format!("local signal listener failed; broker PID {pid} was stopped")))
        }
        (Err(signal_error), Err(shutdown_error)) => Err(signal_error.context(format!(
            "local signal listener failed, and cleanup failed for broker PID {pid}: {shutdown_error:#}"
        ))),
    }
}

fn combine_peak_and_shutdown(peak_result: Result<u64>, shutdown_result: Result<()>) -> Result<u64> {
    match (peak_result, shutdown_result) {
        (Ok(peak), Ok(())) => Ok(peak),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Err(peak_error), Err(shutdown_error)) => {
            Err(peak_error.context(format!("broker shutdown also failed: {shutdown_error:#}")))
        }
    }
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

    use super::{Args, InterruptionLatch, combine_peak_and_shutdown, ensure_not_interrupted};

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

    #[test]
    fn preserves_peak_rss_and_shutdown_errors_together() {
        let error = combine_peak_and_shutdown(
            Err(anyhow::anyhow!("RSS sampler failed")),
            Err(anyhow::anyhow!("broker shutdown failed")),
        )
        .unwrap_err();
        let message = format!("{error:#}");

        assert!(message.contains("RSS sampler failed"), "{message}");
        assert!(message.contains("broker shutdown failed"), "{message}");
    }

    #[test]
    fn latches_only_the_first_local_interruption() {
        let (latch, receiver) = InterruptionLatch::new();

        latch.record("SIGINT");
        latch.record("SIGTERM");

        assert_eq!(*receiver.borrow(), Some("SIGINT"));
    }

    #[test]
    fn pre_success_gate_rejects_a_latched_interruption() {
        let (latch, receiver) = InterruptionLatch::new();
        latch.record("SIGTERM");

        let error = ensure_not_interrupted(&receiver, "before success").unwrap_err();
        let message = error.to_string();

        assert!(message.contains("SIGTERM"), "{message}");
        assert!(message.contains("before success"), "{message}");
    }
}
