use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::Path,
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde::Serialize;
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};

use crate::{config::WorkloadConfig, workload::RunMetrics};

const BYTES_PER_GIB: f64 = 1024.0 * 1024.0 * 1024.0;
const CLIENT_VERSION: &str = "rskafka 0.6.0";
static NEXT_TEMPORARY_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadMetadata {
    pub messages: u64,
    pub payload_bytes: usize,
    pub partitions: i32,
    pub batch_records: usize,
}

impl From<WorkloadConfig> for WorkloadMetadata {
    fn from(config: WorkloadConfig) -> Self {
        Self {
            messages: config.messages,
            payload_bytes: config.payload_bytes,
            partitions: config.partitions,
            batch_records: config.batch_records,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MachineMetadata {
    pub operating_system: String,
    pub operating_system_version: String,
    pub architecture: String,
    pub cpu: String,
    pub logical_cores: usize,
    pub total_memory_bytes: u64,
    pub available_memory_bytes: u64,
    pub rustc_version: String,
    pub client_version: String,
}

impl MachineMetadata {
    pub fn capture() -> Result<Self> {
        let system = System::new_with_specifics(
            RefreshKind::nothing()
                .with_memory(MemoryRefreshKind::nothing().with_ram())
                .with_cpu(CpuRefreshKind::everything()),
        );

        Ok(Self {
            operating_system: System::name().unwrap_or_else(|| std::env::consts::OS.to_owned()),
            operating_system_version: System::os_version().unwrap_or_else(|| "unknown".to_owned()),
            architecture: std::env::consts::ARCH.to_owned(),
            cpu: system
                .cpus()
                .first()
                .map(|cpu| cpu.brand().trim().to_owned())
                .filter(|brand| !brand.is_empty())
                .unwrap_or_else(|| "unknown".to_owned()),
            logical_cores: system.cpus().len(),
            total_memory_bytes: system.total_memory(),
            available_memory_bytes: system.available_memory(),
            rustc_version: command_output("rustc", &["--version"])
                .context("capture rustc version")?,
            client_version: CLIENT_VERSION.to_owned(),
        })
    }

    #[cfg(test)]
    fn fixture() -> Self {
        Self {
            operating_system: "TestOS".to_owned(),
            operating_system_version: "1.0".to_owned(),
            architecture: "test-arch".to_owned(),
            cpu: "Test CPU".to_owned(),
            logical_cores: 8,
            total_memory_bytes: 16 * 1024 * 1024 * 1024,
            available_memory_bytes: 8 * 1024 * 1024 * 1024,
            rustc_version: "rustc 1.98.0".to_owned(),
            client_version: CLIENT_VERSION.to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BenchmarkRun {
    pub run: usize,
    pub topic: String,
    pub broker_pid: u32,
    pub messages: u64,
    pub value_bytes: u64,
    pub producer_seconds: f64,
    pub producer_records_per_second: f64,
    pub producer_gib_per_second: f64,
    pub end_to_end_seconds: f64,
    pub end_to_end_records_per_second: f64,
    pub end_to_end_gib_per_second: f64,
    pub peak_rss_bytes: u64,
}

impl BenchmarkRun {
    pub fn new(
        run: usize,
        topic: String,
        broker_pid: u32,
        metrics: RunMetrics,
        peak_rss_bytes: u64,
    ) -> Self {
        Self {
            run,
            topic,
            broker_pid,
            messages: metrics.messages,
            value_bytes: metrics.value_bytes,
            producer_seconds: metrics.producer_seconds,
            producer_records_per_second: metrics.producer_records_per_second(),
            producer_gib_per_second: metrics.producer_gib_per_second(),
            end_to_end_seconds: metrics.end_to_end_seconds,
            end_to_end_records_per_second: metrics.end_to_end_records_per_second(),
            end_to_end_gib_per_second: metrics.end_to_end_gib_per_second(),
            peak_rss_bytes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MedianMetrics {
    pub producer_seconds: f64,
    pub producer_records_per_second: f64,
    pub producer_gib_per_second: f64,
    pub end_to_end_seconds: f64,
    pub end_to_end_records_per_second: f64,
    pub end_to_end_gib_per_second: f64,
    pub peak_rss_bytes: u64,
}

impl MedianMetrics {
    fn from_runs(runs: &[BenchmarkRun]) -> Result<Self> {
        if runs.is_empty() {
            bail!("cannot aggregate an empty benchmark run list");
        }

        Ok(Self {
            producer_seconds: median_f64(runs.iter().map(|run| run.producer_seconds))?,
            producer_records_per_second: median_f64(
                runs.iter().map(|run| run.producer_records_per_second),
            )?,
            producer_gib_per_second: median_f64(
                runs.iter().map(|run| run.producer_gib_per_second),
            )?,
            end_to_end_seconds: median_f64(runs.iter().map(|run| run.end_to_end_seconds))?,
            end_to_end_records_per_second: median_f64(
                runs.iter().map(|run| run.end_to_end_records_per_second),
            )?,
            end_to_end_gib_per_second: median_f64(
                runs.iter().map(|run| run.end_to_end_gib_per_second),
            )?,
            peak_rss_bytes: median_u64(runs.iter().map(|run| run.peak_rss_bytes)),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BenchmarkReport {
    pub schema_version: u32,
    pub generated_at: DateTime<Utc>,
    pub commit: String,
    pub workload: WorkloadMetadata,
    pub machine: MachineMetadata,
    pub runs: Vec<BenchmarkRun>,
    pub median: MedianMetrics,
}

impl BenchmarkReport {
    pub fn new(
        generated_at: DateTime<Utc>,
        commit: String,
        workload: WorkloadConfig,
        machine: MachineMetadata,
        runs: Vec<BenchmarkRun>,
    ) -> Result<Self> {
        let median = MedianMetrics::from_runs(&runs)?;
        Ok(Self {
            schema_version: 1,
            generated_at,
            commit,
            workload: workload.into(),
            machine,
            runs,
            median,
        })
    }

    pub fn capture(workload: WorkloadConfig, runs: Vec<BenchmarkRun>) -> Result<Self> {
        Self::new(
            Utc::now(),
            command_output("git", &["rev-parse", "HEAD"]).context("capture broker commit")?,
            workload,
            MachineMetadata::capture().context("capture benchmark machine metadata")?,
            runs,
        )
    }
}

pub fn available_memory() -> u64 {
    let system = System::new_with_specifics(
        RefreshKind::nothing().with_memory(MemoryRefreshKind::nothing().with_ram()),
    );
    system.available_memory()
}

pub fn required_available_bytes(config: &WorkloadConfig) -> u64 {
    let payload_bytes = u64::try_from(config.payload_bytes).unwrap_or(u64::MAX);
    config
        .messages
        .saturating_mul(payload_bytes)
        .saturating_mul(2)
}

pub fn ensure_available_memory(config: &WorkloadConfig, available_bytes: u64) -> Result<()> {
    let required_bytes = required_available_bytes(config);
    if available_bytes < required_bytes {
        bail!(
            "benchmark requires {:.2} GiB available memory (2x retained value bytes), but only {:.2} GiB is available; reduce --messages or --payload-bytes, or pass --skip-memory-check",
            required_bytes as f64 / BYTES_PER_GIB,
            available_bytes as f64 / BYTES_PER_GIB,
        );
    }
    Ok(())
}

pub fn write_json_atomic(path: &Path, report: &BenchmarkReport) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
        .with_context(|| format!("create report directory {}", parent.display()))?;

    let file_name = path
        .file_name()
        .context("output JSON path must name a file")?
        .to_string_lossy();
    let json = serde_json::to_vec_pretty(report).context("serialize benchmark report")?;
    let (temporary, mut temporary_file) = create_unique_temporary(parent, &file_name)?;

    let write_result = (|| -> Result<()> {
        temporary_file
            .write_all(&json)
            .with_context(|| format!("write temporary report {}", temporary.display()))?;
        temporary_file
            .sync_all()
            .with_context(|| format!("sync temporary report {}", temporary.display()))?;
        drop(temporary_file);
        fs::rename(&temporary, path).with_context(|| {
            format!(
                "atomically replace report {} from {}",
                path.display(),
                temporary.display()
            )
        })?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    write_result
}

fn create_unique_temporary(parent: &Path, file_name: &str) -> Result<(std::path::PathBuf, File)> {
    loop {
        let temporary_id = NEXT_TEMPORARY_ID.fetch_add(1, Ordering::Relaxed);
        let temporary = parent.join(format!(
            ".{file_name}.{}.{temporary_id}.tmp",
            std::process::id()
        ));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
        {
            Ok(file) => return Ok((temporary, file)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("create temporary report {}", temporary.display()));
            }
        }
    }
}

fn command_output(program: &str, arguments: &[&str]) -> Result<String> {
    let output = Command::new(program)
        .args(arguments)
        .output()
        .with_context(|| format!("run {program}"))?;
    if !output.status.success() {
        bail!("{program} exited with {}", output.status);
    }
    String::from_utf8(output.stdout)
        .with_context(|| format!("decode {program} output as UTF-8"))
        .map(|output| output.trim().to_owned())
}

fn median_f64(values: impl Iterator<Item = f64>) -> Result<f64> {
    let mut values = values.collect::<Vec<_>>();
    if values.is_empty() {
        bail!("cannot compute the median of an empty list");
    }
    if values.iter().any(|value| !value.is_finite()) {
        bail!("cannot compute the median of non-finite values");
    }
    values.sort_by(f64::total_cmp);
    let upper = values.len() / 2;
    if values.len() % 2 == 0 {
        Ok(values[upper - 1] / 2.0 + values[upper] / 2.0)
    } else {
        Ok(values[upper])
    }
}

fn median_u64(values: impl Iterator<Item = u64>) -> u64 {
    let mut values = values.collect::<Vec<_>>();
    values.sort_unstable();
    let upper = values.len() / 2;
    if values.len() % 2 == 0 {
        values[upper - 1] / 2 + values[upper] / 2 + (values[upper - 1] % 2 + values[upper] % 2) / 2
    } else {
        values[upper]
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use chrono::{TimeZone, Utc};

    use crate::{config::WorkloadConfig, workload::RunMetrics};

    use super::{
        BenchmarkReport, BenchmarkRun, MachineMetadata, ensure_available_memory,
        required_available_bytes,
    };

    fn run(index: usize, producer_records_per_second: f64) -> BenchmarkRun {
        let messages = 600;
        BenchmarkRun::new(
            index,
            format!("topic-{index}"),
            10_000 + index as u32,
            RunMetrics::new(
                messages,
                messages * 4096,
                messages as f64 / producer_records_per_second,
                messages as f64 / (producer_records_per_second / 2.0),
            ),
            128 * 1024 * 1024,
        )
    }

    #[test]
    fn chooses_the_middle_producer_rate_after_total_order_sorting() {
        let report = BenchmarkReport::new(
            Utc.with_ymd_and_hms(2026, 8, 28, 12, 0, 0)
                .single()
                .unwrap(),
            "0123456789abcdef".to_owned(),
            WorkloadConfig::default(),
            MachineMetadata::fixture(),
            vec![run(1, 100.0), run(2, 300.0), run(3, 200.0)],
        )
        .unwrap();

        assert_eq!(report.median.producer_records_per_second, 200.0);
    }

    #[test]
    fn serializes_the_versioned_report_contract_in_camel_case() {
        let report = BenchmarkReport::new(
            Utc.with_ymd_and_hms(2026, 8, 28, 12, 0, 0)
                .single()
                .unwrap(),
            "0123456789abcdef".to_owned(),
            WorkloadConfig::default(),
            MachineMetadata::fixture(),
            vec![run(1, 100.0)],
        )
        .unwrap();

        let json = serde_json::to_value(report).unwrap();

        assert_eq!(json["schemaVersion"], 1);
        assert_eq!(json["runs"][0]["peakRssBytes"], 128 * 1024 * 1024);
        assert_eq!(json["runs"][0]["messages"], 600);
        assert_eq!(json["runs"][0]["valueBytes"], 600 * 4096);
        assert_eq!(json["workload"]["payloadBytes"], 4096);
    }

    #[test]
    fn estimates_twice_the_retained_values_with_saturating_arithmetic() {
        let ordinary = WorkloadConfig {
            messages: 1_000,
            payload_bytes: 4_096,
            ..WorkloadConfig::default()
        };
        let overflowing = WorkloadConfig {
            messages: u64::MAX,
            payload_bytes: usize::MAX,
            ..WorkloadConfig::default()
        };

        assert_eq!(required_available_bytes(&ordinary), 8_192_000);
        assert_eq!(required_available_bytes(&overflowing), u64::MAX);
    }

    #[test]
    fn memory_preflight_names_required_and_available_gib() {
        let config = WorkloadConfig {
            messages: 2 * 1024 * 1024 * 1024,
            payload_bytes: 2,
            ..WorkloadConfig::default()
        };

        let error = ensure_available_memory(&config, 4 * 1024 * 1024 * 1024).unwrap_err();
        let message = error.to_string();

        assert!(message.contains("8.00 GiB"), "{message}");
        assert!(message.contains("4.00 GiB"), "{message}");
    }

    #[cfg(unix)]
    #[test]
    fn atomic_writer_does_not_follow_a_stale_pid_scoped_symlink() {
        use std::os::unix::fs::symlink;

        let directory = std::env::temp_dir().join(format!(
            "memkafka-report-symlink-test-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir(&directory).unwrap();
        let sentinel = directory.join("sentinel.txt");
        fs::write(&sentinel, "untouched").unwrap();
        let output = directory.join("report.json");
        let stale_temporary = directory.join(format!(".report.json.{}.0.tmp", std::process::id()));
        symlink(&sentinel, &stale_temporary).unwrap();
        let report = BenchmarkReport::new(
            Utc.with_ymd_and_hms(2026, 8, 28, 12, 0, 0)
                .single()
                .unwrap(),
            "0123456789abcdef".to_owned(),
            WorkloadConfig::default(),
            MachineMetadata::fixture(),
            vec![run(1, 100.0)],
        )
        .unwrap();

        super::write_json_atomic(&output, &report).unwrap();

        assert_eq!(fs::read_to_string(&sentinel).unwrap(), "untouched");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&fs::read(&output).unwrap()).unwrap()["schemaVersion"],
            1
        );
        fs::remove_dir_all(directory).unwrap();
    }
}
