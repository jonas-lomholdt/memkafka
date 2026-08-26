# Produce, Fetch, and In-Process Delivery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Append Kafka RecordBatch bytes to ordered in-memory partition logs, expose them through Produce, Fetch, and ListOffsets, and prove the publish/consume delivery contract through the pinned .NET, Java, Rust, and Go clients.

**Architecture:** Each topic owns a fixed vector of independently locked append-only partition logs. Produce validates complete magic-2 RecordBatches, assigns offsets under the partition lock, rewrites only each outer base offset, and wakes a shared broker append notification. Fetch snapshots complete stored batches without retaining locks, long-polls on that notification, and reports the in-memory log end as its high watermark. The wire dispatcher continues to advertise only handlers and versions that are implemented and covered.

**Tech Stack:** Rust 1.98.0, Tokio, Bytes, `kafka-protocol` 0.18.0, Confluent.Kafka 2.15.0, Apache Kafka Java client 4.3.1, `rskafka` 0.6.0, `franz-go` 1.21.6, Docker, and GitHub Actions.

**Spec:** `docs/2026-08-26-memkafka-design.md`, especially sections 7, 7.1, 7.2, and 7.3.

## Global Constraints

- Keep implementation plans in `docs/plans/`.
- Implement behavior test-first and keep each red/green cycle focused on one contract.
- Store the producer's complete modern RecordBatch bytes; never deserialize records, decompress payloads, or interpret application serialization.
- Accept only `magic = 2`, non-transactional, non-idempotent batches in this slice.
- Assign offsets and append atomically under one partition lock. Never hold that lock while writing a socket or waiting for data.
- A successful `acks=1` or `acks=all` response means the complete batch is already fetchable. `acks=0` appends but emits no response.
- Preserve order by ascending broker-assigned offsets within each partition.
- Advertise only Produce `3-7`, Fetch `4`, and ListOffsets `1-3` in addition to the existing API matrix.
- Disable producer idempotence in all four clients because producer IDs and epochs are outside v0.1.
- Use direct partition assignment for this slice. Group coordination and committed-offset restart behavior remain a later plan.
- Direct assignment may start or seek to any valid offset in this slice. Both client auto-commit and explicit manual commit are required in the later group-coordination slice.
- Keep the current CI timeout and run all four real-client suites against the built Docker image.

## File Structure

- `src/broker/partition.rs`: validated RecordBatch storage, offset assignment, and bounded fetch snapshots.
- `src/broker/topics.rs`: topic entries containing metadata plus fixed partition-log vectors.
- `src/broker/mod.rs`: broker-wide append notification and partition lookup.
- `src/kafka/produce.rs`: Produce validation, append, acknowledgement, and error mapping.
- `src/kafka/list_offsets.rs`: earliest/latest offset lookup.
- `src/kafka/fetch.rs`: immediate and long-poll Fetch responses.
- `src/kafka/dispatcher.rs`: new request routing and live API registry.
- `src/kafka/api_versions.rs`: advertised API ranges.
- `src/kafka/connection.rs`: response suppression for `acks=0`.
- `tests/kafka_wire.rs`: protocol-level Produce/ListOffsets/Fetch and connection behavior.
- `tests/confluent/Program.cs`: .NET publish/consume/order/re-fetch acceptance.
- `tests/java/src/test/java/io/memkafka/acceptance/KafkaJavaClientBlackBoxTest.java`: Java delivery acceptance.
- `tests/rust-client/tests/metadata.rs`: Rust delivery acceptance.
- `tests/go-client/metadata_test.go`: Go delivery acceptance.
- `.github/workflows/ci.yml`: unchanged four-client execution path, now covering delivery.
- `README.md`: exact compatibility status and delivery boundary.

---

### Task 1: Ordered raw RecordBatch partition log

**Files:**
- Create: `src/broker/partition.rs`
- Modify: `src/broker/mod.rs`

**Interfaces:**

```rust
pub(crate) struct PartitionLog { /* locked inner state */ }

pub(crate) struct AppendResult {
    pub(crate) base_offset: i64,
    pub(crate) last_offset: i64,
    pub(crate) record_count: i32,
}

pub(crate) struct FetchSnapshot {
    pub(crate) records: Bytes,
    pub(crate) high_watermark: i64,
}

impl PartitionLog {
    pub(crate) async fn append(&self, records: Bytes) -> Result<AppendResult, AppendError>;
    pub(crate) async fn fetch(
        &self,
        offset: i64,
        partition_max_bytes: usize,
    ) -> Result<FetchSnapshot, FetchError>;
    pub(crate) async fn next_offset(&self) -> i64;
}
```

- [ ] **Step 1: Write failing batch-validation and append tests**

Build real uncompressed magic-2 RecordBatches with `kafka_protocol::records::RecordBatchEncoder`. Assert:

- the first two-record append returns offsets `0..=1`;
- the second one-record append returns offset `2`;
- stored batches contain rewritten outer base offsets `0` and `2`;
- Fetch from `0` returns both batches in the same byte order;
- Fetch from an offset inside the first batch returns that complete batch;
- concurrent single-record appends receive every unique offset in `0..64`;
- truncated, bad-CRC, magic-1, transactional, and producer-ID-bearing batches fail without advancing `next_offset`.

The break caught is duplicate offsets, partially mutated logs, unsafe record parsing, or loss of opaque Kafka payload bytes.

- [ ] **Step 2: Verify RED**

Run: `cargo test broker::partition::tests --lib`

Expected: compilation fails because the partition module and interfaces do not exist.

- [ ] **Step 3: Implement structural validation and atomic append**

Split the incoming record set using each batch's signed length at byte `8`; require at least the 61-byte fixed header and exact complete batch boundaries. Validate each isolated batch with `RecordBatchDecoder::decode_batch_info` so CRC and magic checks use the pinned protocol library. Read the last-offset delta at byte `23` and record count at byte `57`, require a positive record count and `last_offset_delta == record_count - 1`, reject transactional/control attribute bits and producer IDs other than `-1`, and reject offset overflow.

Clone each validated batch to mutable bytes, overwrite only bytes `0..8` with the assigned base offset, freeze it, and store:

```rust
struct StoredBatch {
    base_offset: i64,
    last_offset: i64,
    record_count: i32,
    bytes: Bytes,
}

struct PartitionLogInner {
    next_offset: i64,
    batches: Vec<StoredBatch>,
}
```

Validate the whole request before taking the append lock. Under one lock, assign every batch contiguously, build the new stored entries, extend the vector, then advance `next_offset`. A validation or overflow error leaves state unchanged.

- [ ] **Step 4: Implement bounded complete-batch fetch**

Reject offsets below `0` or above `next_offset`. Start at the first batch with `last_offset >= requested_offset`. Return complete batches up to `partition_max_bytes`, except that the first eligible batch is always returned so consumers can make progress. Offset `next_offset` returns empty bytes with the current high watermark.

- [ ] **Step 5: Verify GREEN and commit**

Run:

```bash
cargo test broker::partition::tests --lib
cargo clippy --all-targets --all-features -- -D warnings
```

Commit:

```bash
git add src/broker
git commit -m "feat: add ordered in-memory partition logs"
```

---

### Task 2: Attach logs to topics and publish append notifications

**Files:**
- Modify: `src/broker/topics.rs`
- Modify: `src/broker/mod.rs`

**Interfaces:**

```rust
pub(crate) async fn partition(
    &self,
    topic: &str,
    partition: i32,
) -> Option<Arc<PartitionLog>>;

pub(crate) fn append_notification(&self) -> Arc<Notify>;
pub(crate) fn notify_append(&self);
```

- [ ] **Step 1: Write failing catalog storage tests**

Create an explicit three-partition topic and assert partition indexes `0`, `1`, and `2` resolve to distinct empty logs while `-1` and `3` do not. Re-run the existing metadata, validation, deterministic listing, and 32-task auto-creation tests unchanged.

- [ ] **Step 2: Verify RED**

Run: `cargo test broker::topics::tests --lib`

Expected: compilation fails because topics contain metadata only.

- [ ] **Step 3: Replace catalog values with internal topic entries**

Use an internal `TopicEntry { metadata, partitions: Vec<Arc<PartitionLog>> }`. Keep `get`, `get_or_auto_create`, `create_explicit`, and `list` returning the same public `TopicMetadata` values so metadata semantics do not change. Allocate all logs atomically with topic creation.

Add one broker-wide `Arc<Notify>`. Produce invokes `notify_waiters()` after a successful append. Fetch obtains an owned notification future before checking all requested partitions, then waits on it only if the snapshot is still below `min_bytes`; this ordering avoids missed wakeups.

- [ ] **Step 4: Verify GREEN and commit**

Run:

```bash
cargo test broker --lib
cargo test --test kafka_wire metadata
cargo clippy --all-targets --all-features -- -D warnings
```

Commit:

```bash
git add src/broker
git commit -m "feat: attach logs to kafka topics"
```

---

### Task 3: Produce with correct acknowledgement behavior

**Files:**
- Create: `src/kafka/produce.rs`
- Modify: `src/kafka/mod.rs`
- Modify: `src/kafka/dispatcher.rs`
- Modify: `src/kafka/api_versions.rs`
- Modify: `src/kafka/connection.rs`
- Modify: `tests/kafka_wire.rs`

**Interfaces:**
- Adds live Produce versions `3-7`.
- Maps `acks=1` and `acks=-1` to append-then-response.
- Maps `acks=0` to append-without-response.
- Auto-creates a missing topic only when broker configuration permits it.

- [ ] **Step 1: Write failing handler tests**

Submit a Produce v7 request containing one valid batch with two literal records to topic `events`, partition `0`, `acks=-1`. Assert response error `0`, base offset `0`, and log end `2`. Submit another batch with `acks=1` and assert base offset `2`.

Also assert:

- missing topics auto-create with the configured partition count;
- disabled auto-creation returns `UnknownTopicOrPartition` (`3`);
- a nonexistent partition returns `UnknownTopicOrPartition` (`3`);
- `acks=2` returns `InvalidRequiredAcks` (`21`) without appending;
- invalid batches return `CorruptMessage` (`2`) without appending;
- a non-null transactional ID returns `UnsupportedForMessageFormat` (`43`);
- one invalid partition does not prevent valid partitions in the same request from appending.

- [ ] **Step 2: Verify RED**

Run: `cargo test --test kafka_wire produce`

Expected: request dispatch fails because Produce is not advertised or routed.

- [ ] **Step 3: Implement Produce routing and response mapping**

Validate acknowledgement mode and transactional ID before mutation. Resolve or auto-create each topic, append partition record sets independently, notify after every successful partition append, and return one response entry per requested topic/partition. Use base offset `-1` for failures and zero throttling.

Do not advertise idempotence-related APIs and do not accept batches carrying producer identity in this slice.

- [ ] **Step 4: Write the failing `acks=0` TCP test**

On one real TCP connection, send a valid `acks=0` Produce followed by ApiVersions with correlation ID `91`. Assert the first response read has correlation ID `91`, then Fetch the partition and prove the produced record exists.

The break caught is emitting an illegal Produce response that desynchronizes the connection.

- [ ] **Step 5: Suppress only `acks=0` responses**

Have dispatch return an explicit response disposition, or have `DecodedRequest::expects_response()` identify only Produce with `acks=0`. The connection must still await dispatch completion, log failures, and continue reading before skipping the socket write. All other requests always receive a response.

- [ ] **Step 6: Verify GREEN and commit**

Run:

```bash
cargo test --test kafka_wire produce
cargo test --test kafka_wire acks_zero
cargo clippy --all-targets --all-features -- -D warnings
```

Commit:

```bash
git add src/kafka tests/kafka_wire.rs
git commit -m "feat: append kafka produce batches"
```

---

### Task 4: Earliest and latest offsets

**Files:**
- Create: `src/kafka/list_offsets.rs`
- Modify: `src/kafka/mod.rs`
- Modify: `src/kafka/dispatcher.rs`
- Modify: `src/kafka/api_versions.rs`
- Modify: `tests/kafka_wire.rs`

**Interfaces:**
- Adds live ListOffsets versions `1-3`.
- Timestamp `-2` returns earliest offset `0`.
- Timestamp `-1` returns latest offset `next_offset`.

- [ ] **Step 1: Write failing ListOffsets tests**

Append three records, request earliest and latest for the partition, and assert literal offsets `0` and `3`. Assert unknown topic/partition returns error `3`. Assert timestamps other than `-1` and `-2` return `UnsupportedForMessageFormat` (`43`) and offset `-1`.

- [ ] **Step 2: Verify RED**

Run: `cargo test --test kafka_wire list_offsets`

Expected: request dispatch fails because ListOffsets is not advertised or routed.

- [ ] **Step 3: Implement the handler and advertise it**

Read `next_offset` under the partition lock and release it before response encoding. Preserve request topic/partition order in the response. Use leader epoch `-1`, timestamp `-1`, and zero throttle where those fields exist in the negotiated versions.

- [ ] **Step 4: Verify GREEN and commit**

Run:

```bash
cargo test --test kafka_wire list_offsets
cargo clippy --all-targets --all-features -- -D warnings
```

Commit:

```bash
git add src/kafka tests/kafka_wire.rs
git commit -m "feat: report kafka partition offsets"
```

---

### Task 5: Immediate and long-poll Fetch

**Files:**
- Create: `src/kafka/fetch.rs`
- Modify: `src/kafka/mod.rs`
- Modify: `src/kafka/dispatcher.rs`
- Modify: `src/kafka/api_versions.rs`
- Modify: `tests/kafka_wire.rs`

**Interfaces:**
- Adds only Fetch version `4`.
- Returns raw complete RecordBatches with `high_watermark = next_offset` and `last_stable_offset = next_offset`.
- Respects request `max_bytes`, per-partition `partition_max_bytes`, `min_bytes`, and `max_wait_ms` with first-batch progress semantics.

- [ ] **Step 1: Write failing immediate Fetch tests**

Produce records with literal keys, values, headers, and timestamps. Fetch from offset `0`, decode with `RecordBatchDecoder`, and assert exact content and offsets in producer order. Assert:

- fetching at the log end returns empty records and the correct high watermark;
- a negative or past-log-end offset returns `OffsetOutOfRange` (`1`);
- unknown topic/partition returns error `3`;
- partition and request byte limits stop before later batches;
- the first eligible batch is returned even when it exceeds either limit.

- [ ] **Step 2: Verify RED**

Run: `cargo test --test kafka_wire fetch`

Expected: request dispatch fails because Fetch is not advertised or routed.

- [ ] **Step 3: Implement one Fetch snapshot pass**

For every requested partition, snapshot eligible complete batches and current high watermark without retaining its lock. Apply both byte budgets in request order. Populate v4 response fields with high watermark and last stable offset equal to `next_offset`, an empty aborted-transaction list, and raw record bytes.

- [ ] **Step 4: Write failing long-poll tests with paused Tokio time**

Assert an empty Fetch:

- remains pending before `max_wait_ms` when no data exists;
- completes at the deadline with empty records;
- wakes after a relevant append and returns the appended batch;
- reevaluates all partitions after any shared append notification;
- waits until total response record bytes reach `min_bytes`, unless the deadline wins.

- [ ] **Step 5: Implement race-free shared-notification waiting**

Before each snapshot pass, create `broker.append_notification().notified_owned()`. If available record bytes meet `min_bytes`, or `max_wait_ms` is zero, return immediately. Otherwise `tokio::select!` between that notification and one pinned deadline, then reevaluate all partitions. Do not spawn timer tasks and do not retain a partition lock across the wait.

- [ ] **Step 6: Verify GREEN and commit**

Run:

```bash
cargo test --test kafka_wire fetch
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

Commit:

```bash
git add src/kafka tests/kafka_wire.rs
git commit -m "feat: fetch ordered kafka record batches"
```

---

### Task 6: Four-client publish/consume delivery acceptance

**Files:**
- Modify: `tests/confluent/Program.cs`
- Modify: `tests/java/src/test/java/io/memkafka/acceptance/KafkaJavaClientBlackBoxTest.java`
- Modify: `tests/rust-client/tests/metadata.rs`
- Modify: `tests/go-client/metadata_test.go`
- Modify: `README.md`
- Modify: `docs/2026-08-26-memkafka-design.md`

**Acceptance contract shared by every client:**

1. Explicitly create a single-partition topic with a unique name.
2. Disable producer idempotence, require all acknowledgements, and limit in-flight Produce requests to one where the client exposes that control.
3. Sequentially publish ten records to partition `0`, awaiting each acknowledgement.
4. Directly assign partition `0` at offset `0`; do not use a consumer group or auto-commit.
5. Consume and assert offsets exactly `0..9`, values exactly `message-0..message-9`, and keys in the same order.
6. Seek or create a fresh direct partition reader at offset `0` without committing.
7. Consume the same ten records again and assert the same offsets and order.

This proves publish/consume interoperability, per-partition ordering, and repeat delivery when processing has not been committed. It does not claim group restart recovery yet.

- [ ] **Step 1: Add the failing .NET delivery scenario**

Use `EnableIdempotence=false`, `Acks=All`, `MaxInFlight=1`, explicit `TopicPartition(0)`, `EnableAutoCommit=false`, and `AutoOffsetReset=Earliest`. Keep every poll bounded by the existing suite deadline.

Run: `dotnet run --project tests/confluent/MemKafka.Acceptance.csproj -- 127.0.0.1:9092`

Expected before this plan's broker APIs: failure during Produce negotiation or append.

- [ ] **Step 2: Add the failing Java delivery scenario**

Use `enable.idempotence=false`, `acks=all`, `max.in.flight.requests.per.connection=1`, manual `assign`, and `seekToBeginning`. Await each `send(...).get()`.

Run: `mvn -q -f tests/java/pom.xml test -Dmemkafka.bootstrap=127.0.0.1:9092`

- [ ] **Step 3: Add the failing Rust delivery scenario**

Use `rskafka`'s direct partition client, sequential `produce` calls, and `fetch_records` from offset `0`. Decode and compare all returned records twice.

Run: `cargo test --manifest-path tests/rust-client/Cargo.toml --locked`

- [ ] **Step 4: Add the failing Go delivery scenario**

Configure franz-go with `DisableIdempotentWrite`, `RequiredAcks(kgo.AllISRAcks())`, `MaxProduceRequestsInflightPerBroker(1)`, and direct `ConsumePartitions` from `kgo.NewOffset().AtStart()`. Await every `ProduceSync` result and reassign at start for the second read.

Run: `go test ./...` from `tests/go-client`.

- [ ] **Step 5: Verify all four suites GREEN against native and Docker execution**

Run the repository's native acceptance harness, build `memkafka:ci`, then execute the same four suite commands against the container exactly as `.github/workflows/ci.yml` does. No workflow matrix change is needed because every existing client job now includes its delivery test.

- [ ] **Step 6: Update compatibility documentation**

Mark Produce, Fetch, ListOffsets, ordered per-partition delivery, and in-process at-least-once behavior as implemented. State immediately beside that claim:

- state disappears at process exit;
- duplicates are possible after an unknown Produce outcome;
- `acks=0` is outside the acknowledgement guarantee;
- group commits and restart recovery remain pending.

- [ ] **Step 7: Commit**

```bash
git add tests README.md docs/2026-08-26-memkafka-design.md
git commit -m "test: verify delivery across four kafka clients"
```

---

### Task 7: Full verification and delivery checkpoint

**Files:**
- Verify: all tracked project files
- Modify only if verification reveals a defect

- [ ] **Step 1: Run formatting and locked dependency checks**

```bash
cargo fmt --all -- --check
cargo fmt --manifest-path tests/rust-client/Cargo.toml --all -- --check
dotnet restore tests/confluent/MemKafka.Acceptance.csproj --locked-mode
go mod verify
```

Run `gofmt -l` over `tests/go-client` and require no output.

- [ ] **Step 2: Run all build and unit/integration checks**

```bash
cargo test --all-targets --all-features
cargo clippy --all-targets --all-features -- -D warnings
cargo clippy --manifest-path tests/rust-client/Cargo.toml --all-targets --all-features -- -D warnings
cargo build --locked
```

- [ ] **Step 3: Run the exact four-client container acceptance path**

Build the Docker image, start it on an isolated Docker network, wait for the Kafka endpoint, and run the .NET, Java, Rust, and Go suites. Require all metadata, topic-creation, publish, consume, ordering, and repeat-fetch scenarios to pass before claiming completion.

- [ ] **Step 4: Inspect final changes**

```bash
git status --short
git diff --check
git log --oneline --decorate -10
```

Confirm there are no generated build outputs, temporary containers, placeholder tests, or unrelated changes.

- [ ] **Step 5: Commit any verification-only corrections**

If verification required a correction, rerun the affected command plus the full suite and commit only the correction. Do not push; the configured `origin` remains ready for the user-approved push later.
