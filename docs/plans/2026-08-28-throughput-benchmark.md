# Throughput Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an isolated Rust harness that produces, consumes, validates, measures, and graphs one million deterministic 4 KiB Kafka records without adding benchmark code or dependencies to MemKafka.

**Architecture:** A standalone Cargo workspace talks to MemKafka only through `rskafka` and the public CLI. Focused modules own configuration/event encoding, Kafka traffic, broker process metrics, report aggregation, and dependency-free SVG rendering; a thin shell entry point builds release binaries and invokes the harness.

**Tech Stack:** Rust 1.98, Tokio, rskafka 0.6.0, Clap, Serde JSON, sysinfo 0.39.6, Bash, GitHub Actions.

**Spec:** `docs/2026-08-28-throughput-benchmark-and-ghcr-design.md`

## Global Constraints

- Keep the benchmark in `benchmarks/throughput/` as a standalone workspace with its own `Cargo.lock`.
- Do not modify the root `Cargo.toml`, import the `memkafka` crate, or add broker instrumentation.
- Default to 1,000,000 records, exactly 4,096 value bytes, 8 partitions, 256 records per batch, no compression, and 3 fresh-process runs.
- Produce and consume concurrently; validate count, contiguous offsets, payload identity, and partition order.
- Report producer and end-to-end elapsed time, records/s, GiB/s, peak broker RSS, workload metadata, machine metadata, and commit.
- CI runs 10,000 records once with no timing threshold; the full benchmark remains manual.
- Generated results live only in `docs/benchmarks/`; no hidden workspace or planning directory is allowed.

---

### Task 1: Standalone crate, validated configuration, and exact event format

**Files:**
- Create: `benchmarks/throughput/Cargo.toml`
- Create: `benchmarks/throughput/Cargo.lock`
- Create: `benchmarks/throughput/src/main.rs`
- Create: `benchmarks/throughput/src/config.rs`
- Create: `benchmarks/throughput/src/event.rs`

**Interfaces:**
- Produces: `WorkloadConfig::validate() -> anyhow::Result<()>`
- Produces: `WorkloadConfig::records_in_partition(i32) -> u64`
- Produces: `event::record(partition: i32, sequence: u64, payload_bytes: usize) -> anyhow::Result<rskafka::record::Record>`
- Produces: `event::validate(record: &RecordAndOffset, partition: i32, sequence: u64, payload_bytes: usize) -> anyhow::Result<()>`

- [ ] **Step 1: Scaffold the independent workspace and write failing configuration tests**

Use this manifest so dependency resolution is isolated and reproducible:

```toml
[package]
name = "memkafka-throughput-benchmark"
version = "0.1.0"
edition = "2024"
rust-version = "1.98"
publish = false

[dependencies]
anyhow = "1"
chrono = { version = "0.4", default-features = false, features = ["clock", "serde", "std"] }
clap = { version = "4.5", features = ["derive"] }
futures = "0.3"
rskafka = { version = "=0.6.0", default-features = false }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
sysinfo = { version = "=0.39.6", default-features = false, features = ["system"] }
tokio = { version = "1", features = ["macros", "process", "rt-multi-thread", "signal", "sync", "time"] }

[lints.rust]
unsafe_code = "forbid"

[workspace]
```

Define `WorkloadConfig` in `config.rs` with `messages: u64`, `payload_bytes: usize`, `partitions: i32`, and `batch_records: usize`. Add tests proving defaults equal `1_000_000/4096/8/256`, zero values are rejected, messages are divided across partitions including a remainder, and payloads too small for the fixed JSON envelope are rejected.

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
cargo test --manifest-path benchmarks/throughput/Cargo.toml config::tests
```

Expected: compile failure because `validate` and `records_in_partition` are not implemented.

- [ ] **Step 3: Implement configuration validation and distribution**

Use deterministic remainder allocation:

```rust
pub fn records_in_partition(&self, partition: i32) -> u64 {
    let base = self.messages / self.partitions as u64;
    let remainder = self.messages % self.partitions as u64;
    base + u64::from((partition as u64) < remainder)
}
```

Reject non-positive partitions, zero messages/batches, payloads below `MIN_PAYLOAD_BYTES`, and configurations where `messages < partitions`.

- [ ] **Step 4: Write failing event-format tests**

Tests must create records for partition `3`, sequence `42`, and payload size `4096`, then assert:

```rust
assert_eq!(record.value.as_ref().unwrap().len(), 4096);
assert_eq!(record.key.as_deref(), Some(b"p03-s00000000000000000042"));
serde_json::from_slice::<serde_json::Value>(record.value.as_ref().unwrap()).unwrap();
```

Construct a `RecordAndOffset` at offset `42` and assert `validate` succeeds. Mutate the key, truncate the value, and change the offset in separate tests; each mutation must return an error naming the mismatched field.

- [ ] **Step 5: Run the event tests and verify RED**

Run:

```bash
cargo test --manifest-path benchmarks/throughput/Cargo.toml event::tests
```

Expected: compile failure because `record` and `validate` are not implemented.

- [ ] **Step 6: Implement deterministic 4 KiB JSON records**

Build a valid JSON envelope with stable fields and compute padding before serializing. The key and JSON identity must use the same fixed-width partition and sequence. Include headers `content-type=application/json` and `event-type=EquipmentMoved`, and a fixed UTC timestamp so record generation is reproducible. `validate` checks offset, key, value length, JSON opening/closing bytes, and the embedded fixed-width identity without fully parsing 4 GiB of JSON during a measured run.

- [ ] **Step 7: Run tests, formatting, and strict Clippy**

Run:

```bash
cargo fmt --manifest-path benchmarks/throughput/Cargo.toml -- --check
cargo clippy --locked --manifest-path benchmarks/throughput/Cargo.toml --all-targets -- -D warnings
cargo test --locked --manifest-path benchmarks/throughput/Cargo.toml
```

Expected: all commands exit `0`.

- [ ] **Step 8: Commit the isolated event foundation**

```bash
git add benchmarks/throughput
git commit -m "feat: add isolated throughput workload format"
```

---

### Task 2: Concurrent Kafka producer, consumer, and validation flow

**Files:**
- Create: `benchmarks/throughput/src/workload.rs`
- Modify: `benchmarks/throughput/src/main.rs`

**Interfaces:**
- Consumes: `WorkloadConfig`, `event::record`, and `event::validate`
- Produces: `RunMetrics { producer_seconds: f64, end_to_end_seconds: f64, messages: u64, value_bytes: u64 }`
- Produces: `workload::run(bootstrap_server: &str, topic: &str, config: &WorkloadConfig) -> anyhow::Result<RunMetrics>`

- [ ] **Step 1: Write failing distribution and rate tests**

Add tests for:

```rust
let metrics = RunMetrics::new(1_000, 4_096_000, 0.5, 0.8);
assert_eq!(metrics.producer_records_per_second(), 2_000.0);
assert_eq!(metrics.end_to_end_records_per_second(), 1_250.0);
assert!((metrics.producer_gib_per_second() - 0.0076293945).abs() < 1e-9);
```

Also test `partition_ranges(10, 3)` returns `0..4`, `0..3`, and `0..3` for partitions `0`, `1`, and `2`.

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
cargo test --manifest-path benchmarks/throughput/Cargo.toml workload::tests
```

Expected: compile failure because `RunMetrics` and `partition_ranges` do not exist.

- [ ] **Step 3: Implement metrics and partition ranges**

Store durations as seconds in the serializable result and compute rates from the exact message/value totals. Keep floating-point calculations out of Kafka loops.
Annotate result/report structs with `#[serde(rename_all = "camelCase")]` so the CLI and CI assertions use one stable JSON contract.

- [ ] **Step 4: Implement the real Kafka workload**

The implementation must:

1. build an `rskafka::Client` with a bounded backoff deadline;
2. create a fresh topic with `config.partitions` and replication factor `1`;
3. create one `PartitionClient` per partition;
4. spawn one consumer and one producer task per partition behind a shared start signal;
5. produce chunks of at most `batch_records` with `Compression::NoCompression`;
6. verify every returned base/record offset is contiguous;
7. fetch from offset `0` with a bounded wait and an 8 MiB maximum response;
8. validate every fetched record using its partition-local sequence;
9. record producer completion after every acknowledged producer task finishes; and
10. record end-to-end completion after every consumer validates its expected count.

Use a Tokio `watch::channel<Option<Instant>>` so the outer task publishes one common start instant after all tasks are spawned. Wrap every error with run, partition, and expected-offset context.

- [ ] **Step 5: Add the external-broker CLI path**

Expose these flags in `main.rs`:

```text
--bootstrap-server <HOST:PORT>
--messages <COUNT>
--payload-bytes <BYTES>
--partitions <COUNT>
--batch-records <COUNT>
--topic-prefix <PREFIX>
--output-json <PATH>
```

When `--bootstrap-server` is present, execute exactly one run and write its `RunMetrics` as JSON. Do not start or inspect a process in this mode.

- [ ] **Step 6: Verify against a live release broker with 1,000 records**

Start `target/release/memkafka` on loopback in one terminal, then run:

```bash
cargo run --release --locked --manifest-path benchmarks/throughput/Cargo.toml -- \
  --bootstrap-server 127.0.0.1:9092 \
  --messages 1000 \
  --payload-bytes 4096 \
  --partitions 8 \
  --batch-records 256 \
  --output-json /tmp/memkafka-throughput-smoke.json
```

Expected: exit `0`; JSON reports exactly `1000` messages and `4_096_000` value bytes; broker logs contain no request failure.

- [ ] **Step 7: Run all standalone checks**

Run:

```bash
cargo fmt --manifest-path benchmarks/throughput/Cargo.toml -- --check
cargo clippy --locked --manifest-path benchmarks/throughput/Cargo.toml --all-targets -- -D warnings
cargo test --locked --manifest-path benchmarks/throughput/Cargo.toml
```

Expected: all commands exit `0`.

- [ ] **Step 8: Commit the real-client workload**

```bash
git add benchmarks/throughput
git commit -m "feat: benchmark end-to-end Kafka throughput"
```

---

### Task 3: Fresh broker orchestration, memory safety, and multi-run report

**Files:**
- Create: `benchmarks/throughput/src/broker.rs`
- Create: `benchmarks/throughput/src/report.rs`
- Create: `benchmarks/throughput/run.sh`
- Modify: `benchmarks/throughput/src/main.rs`

**Interfaces:**
- Consumes: `workload::run` and `RunMetrics`
- Produces: `BrokerGuard::start(binary: &Path) -> anyhow::Result<BrokerGuard>`
- Produces: `BrokerGuard::bootstrap_server(&self) -> &str`
- Produces: `BrokerGuard::peak_rss_bytes(&mut self) -> anyhow::Result<u64>`
- Produces: `BenchmarkReport { schema_version, generated_at, commit, workload, machine, runs, median }`
- Produces: `report::write_json_atomic(path: &Path, report: &BenchmarkReport) -> anyhow::Result<()>`

- [ ] **Step 1: Write failing report and memory-safety tests**

Test that the median of producer rates `[100.0, 300.0, 200.0]` is `200.0`; JSON includes `schemaVersion: 1`; and `required_available_bytes` returns twice the configured value bytes using saturating arithmetic. Test that a configuration needing more memory than `available_memory()` yields an error containing both required and available GiB.

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
cargo test --manifest-path benchmarks/throughput/Cargo.toml report::tests
```

Expected: compile failure because aggregation and memory checks are absent.

- [ ] **Step 3: Implement report aggregation and host metadata**

Use `sysinfo::System` to capture total/available bytes, CPU brand, logical cores, operating-system name/version, and architecture. Capture `rustc --version`, `rskafka 0.6.0`, and `git rev-parse HEAD`. Use `f64::total_cmp` for deterministic medians and write JSON to a sibling temporary file before renaming it over the destination.

- [ ] **Step 4: Implement broker lifecycle and RSS sampling**

Reserve two loopback ports, start the binary with explicit Kafka listen/advertised and Schema Registry addresses, and poll the Kafka port for at most 10 seconds while also checking `Child::try_wait`. Refresh only the child process with `sysinfo` every 100 ms and retain the maximum `Process::memory()` value. `BrokerGuard::stop` terminates and awaits the child; `Drop` performs best-effort termination for panic/error paths.
Start MemKafka with `--quiet`, redirect stdout/stderr to a per-run temporary log, and include the log path when startup or workload execution fails so child output can neither fill a pipe nor disappear.

- [ ] **Step 5: Implement local multi-run mode**

When no external bootstrap server is provided:

1. check `available_memory >= messages * payload_bytes * 2` unless `--skip-memory-check` is set;
2. start one fresh broker per run;
3. call `workload::run` with a unique topic;
4. stop the RSS sampler and broker;
5. attach peak RSS to that run;
6. aggregate all runs; and
7. write one report only after every run succeeds.

Add `--broker-binary`, `--runs`, `--skip-memory-check`, and `--output-json`. Defaults point at the root release binary and `docs/benchmarks/latest.json` when invoked through `run.sh`.

- [ ] **Step 6: Add the thin root-relative wrapper**

`run.sh` must use `set -euo pipefail`, resolve the repository root from its own path, build root and benchmark release binaries with `--locked`, and execute local mode with any caller arguments appended. It must not create or retain a hidden workspace.

- [ ] **Step 7: Run a two-run local lifecycle smoke**

Run:

```bash
benchmarks/throughput/run.sh --messages 1000 --runs 2 --skip-memory-check --output-json /tmp/memkafka-throughput-local.json
```

Expected: two successful entries, distinct fresh topics/processes, positive peak RSS, a median summary, and no MemKafka child left running.

- [ ] **Step 8: Run all standalone checks and shell syntax**

Run:

```bash
bash -n benchmarks/throughput/run.sh
cargo fmt --manifest-path benchmarks/throughput/Cargo.toml -- --check
cargo clippy --locked --manifest-path benchmarks/throughput/Cargo.toml --all-targets -- -D warnings
cargo test --locked --manifest-path benchmarks/throughput/Cargo.toml
```

Expected: all commands exit `0`.

- [ ] **Step 9: Commit orchestration and reporting**

```bash
git add benchmarks/throughput
git commit -m "feat: orchestrate repeatable throughput runs"
```

---

### Task 4: Dependency-free SVG graph and checked-in benchmark result

**Files:**
- Create: `benchmarks/throughput/src/svg.rs`
- Create: `docs/benchmarks/latest.json`
- Create: `docs/benchmarks/throughput.svg`
- Modify: `benchmarks/throughput/src/main.rs`
- Modify: `benchmarks/throughput/run.sh`

**Interfaces:**
- Consumes: `BenchmarkReport`
- Produces: `svg::render(report: &BenchmarkReport) -> anyhow::Result<String>`
- Produces: atomic JSON and SVG output from one successful report

- [ ] **Step 1: Write failing SVG tests**

Create a three-run fixture and assert the rendered document:

```rust
assert!(svg.starts_with("<svg"));
assert!(svg.contains("Producer throughput"));
assert!(svg.contains("End-to-end throughput"));
assert!(svg.contains("Median"));
assert_eq!(svg.matches("class=\"producer-bar\"").count(), 3);
assert_eq!(svg.matches("class=\"end-to-end-bar\"").count(), 3);
```

Also test that CPU text containing `&<>\"` is XML-escaped and that equal/zero rate inputs never produce `NaN` or `inf`.

- [ ] **Step 2: Run the SVG tests and verify RED**

Run:

```bash
cargo test --manifest-path benchmarks/throughput/Cargo.toml svg::tests
```

Expected: compile failure because `svg::render` is absent.

- [ ] **Step 3: Implement deterministic SVG rendering**

Render a responsive `viewBox` SVG with two bars per run, median guide lines, labeled records/s and GiB/s, and a compact footer containing payload, partitions, peak RSS, CPU/OS, commit, and generation time. Scale bars against the maximum finite rate, XML-escape all metadata, use no scripts/fonts/external assets, and end the file with one newline.

- [ ] **Step 4: Write JSON and SVG as one successful operation**

Generate both strings before replacing either destination. Write sibling temporary files, rename JSON, then rename SVG. On rendering or serialization failure, leave existing published artifacts untouched.
Add `--output-svg <PATH>` to local mode, default it to `docs/benchmarks/throughput.svg` through `run.sh`, and require it whenever local mode publishes a report. External CI-smoke mode continues to allow JSON-only output.

- [ ] **Step 5: Run the full benchmark**

Run from an otherwise quiet machine:

```bash
benchmarks/throughput/run.sh
```

Expected: three successful fresh-process runs totaling one million validated 4 KiB events each; `docs/benchmarks/latest.json` and `docs/benchmarks/throughput.svg` are created; no broker remains; output prints producer/end-to-end medians and peak RSS. If the memory preflight rejects the machine, report the exact required/available values instead of bypassing it for a publishable result.

- [ ] **Step 6: Verify the artifacts match the workload**

Run:

```bash
jq -e '.schemaVersion == 1 and .workload.messages == 1000000 and .workload.payloadBytes == 4096 and .workload.partitions == 8 and (.runs | length) == 3' docs/benchmarks/latest.json
rg -n "Producer throughput|End-to-end throughput|Median" docs/benchmarks/throughput.svg
```

Expected: both commands exit `0`.

- [ ] **Step 7: Commit the graph and raw evidence**

```bash
git add benchmarks/throughput docs/benchmarks
git commit -m "docs: publish reproducible throughput benchmark"
```

---

### Task 5: CI correctness smoke and README presentation

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `README.md`

**Interfaces:**
- Consumes: standalone benchmark CLI and checked-in SVG/JSON
- Produces: hosted 10,000-record correctness coverage with no performance threshold

- [ ] **Step 1: Add standalone crate checks to CI**

After installing Rust, add formatting, strict Clippy, and locked unit-test steps using `--manifest-path benchmarks/throughput/Cargo.toml`. Do not add the crate to the root workspace.

- [ ] **Step 2: Add the external-broker benchmark smoke**

After `memkafka-acceptance` is running, execute:

```bash
cargo run --release --locked --manifest-path benchmarks/throughput/Cargo.toml -- \
  --bootstrap-server 127.0.0.1:9092 \
  --messages 10000 \
  --payload-bytes 4096 \
  --partitions 8 \
  --batch-records 256 \
  --output-json /tmp/memkafka-throughput-smoke.json
jq -e '.messages == 10000 and .valueBytes == 40960000' /tmp/memkafka-throughput-smoke.json
```

No assertion may inspect elapsed time or throughput.

- [ ] **Step 3: Add the README benchmark section at the bottom**

Embed `docs/benchmarks/throughput.svg`, summarize the median producer/end-to-end rates and peak RSS from `latest.json`, name the CPU/OS/commit, link the raw JSON, and show `benchmarks/throughput/run.sh`. State plainly that this is one machine-specific sample and not a universal guarantee.

- [ ] **Step 4: Run all local verification**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
cargo fmt --manifest-path benchmarks/throughput/Cargo.toml -- --check
cargo clippy --locked --manifest-path benchmarks/throughput/Cargo.toml --all-targets -- -D warnings
cargo test --locked --manifest-path benchmarks/throughput/Cargo.toml
bash -n benchmarks/throughput/run.sh
git diff --check
```

Expected: every command exits `0` and the repository contains no hidden benchmark/planning directory.

- [ ] **Step 5: Run the 10,000-record smoke against a local container**

Build/start the current image, run the exact CI command, verify JSON, then remove only that test container. Expected: all 10,000 records are acknowledged, fetched, and validated in order.

- [ ] **Step 6: Commit CI and README integration**

```bash
git add .github/workflows/ci.yml README.md
git commit -m "test: smoke throughput benchmark in CI"
```

- [ ] **Step 7: Push and monitor hosted CI**

```bash
git push origin main
gh run list --branch main --limit 3
gh run watch <RUN_ID> --exit-status
```

Expected: the complete existing client matrix and new benchmark smoke are green.
