# Non-Transactional Idempotent Production Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Support real non-transactional Kafka idempotent producers with producer allocation, epoch validation, sequence enforcement, and retry deduplication.

**Architecture:** Add a small process-local producer coordinator to allocate IDs and validate epochs, expose it through `InitProducerId v0`, and extend each partition log with per-producer sequence state plus a five-request retry cache. The Produce handler validates allocated identities before atomically classifying a record set as a new append, exact retry, stale retry, or sequence gap.

**Tech Stack:** Rust 1.98.0, Tokio, Bytes, `kafka-protocol` 0.18.0 generated messages and RecordBatch metadata, and the existing Rust wire tests.

**Spec:** [`../2026-08-26-memkafka-design.md`](../2026-08-26-memkafka-design.md), Sections 2, 5, 6, 7.1.1, 7.3, 12.6, and 13.

## Global Constraints

- Support non-transactional idempotence only; transactional IDs, transactional batches, control batches, and transaction-coordinator APIs remain unsupported.
- Advertise only `InitProducerId v0`.
- Producer IDs are positive, unique, process-local, and start at `1`; epochs start at `0` and are not recovered or bumped in v0.1.
- Sequence state is independent per producer ID and partition.
- Exact retries within the last five accepted Produce requests return the original offset range without appending or notifying fetch waiters.
- Sequence gaps, unknown producer IDs, invalid epochs, and expired duplicates must not mutate partition bytes, offsets, sequence state, or retry history.
- Preserve all current non-idempotent, compression, ordering, and `acks=0|1|all` behavior.
- Avoid new dependencies; the RecordBatch CRC and length provide the retry fingerprint.

---

### Task 1: Process-local producer coordinator

**Files:**
- Create: `src/broker/producers.rs`
- Modify: `src/broker/mod.rs`

**Interfaces:**
- Consumes: no Kafka wire types; this is broker-domain state.
- Produces: `ProducerCoordinator`, `ProducerIdentity { producer_id: i64, producer_epoch: i16 }`, `ProducerError`, `allocate()`, and `validate()`.

- [ ] **Step 1: Write failing coordinator unit tests**

Create tests proving literal allocation and validation behavior:

```rust
let coordinator = ProducerCoordinator::new();
let first = coordinator.allocate().await.expect("first producer");
let second = coordinator.allocate().await.expect("second producer");
assert_eq!(first, ProducerIdentity { producer_id: 1, producer_epoch: 0 });
assert_eq!(second, ProducerIdentity { producer_id: 2, producer_epoch: 0 });
assert_eq!(coordinator.validate(1, 0).await, Ok(()));
assert_eq!(coordinator.validate(99, 0).await, Err(ProducerError::UnknownProducerId));
assert_eq!(coordinator.validate(1, 1).await, Err(ProducerError::InvalidProducerEpoch));
```

- [ ] **Step 2: Run and verify RED**

Run: `cargo test broker::producers::tests --lib`

Expected: compilation fails because the producer module does not exist.

- [ ] **Step 3: Implement coordinator state**

Use one focused lock:

```rust
#[derive(Clone, Debug)]
pub(crate) struct ProducerCoordinator {
    inner: Arc<Mutex<ProducerCoordinatorInner>>,
}

#[derive(Debug)]
struct ProducerCoordinatorInner {
    next_id: i64,
    epochs: HashMap<i64, i16>,
}
```

`allocate` returns `ProducerError::IdExhausted` if `checked_add(1)` fails; otherwise it inserts epoch `0`. `validate` distinguishes an absent ID from a mismatched epoch. Implement `Default` by calling `new`.

- [ ] **Step 4: Add coordinator ownership to `BrokerState`**

Add `producers: ProducerCoordinator`, initialize it in `BrokerState::new`, and expose:

```rust
pub(crate) fn producers(&self) -> &ProducerCoordinator {
    &self.producers
}
```

- [ ] **Step 5: Run unit and broker regression tests GREEN**

Run:

```bash
cargo test broker::producers::tests --lib
cargo test --lib
```

Expected: all tests pass.

- [ ] **Step 6: Commit the coordinator**

```bash
git add src/broker/producers.rs src/broker/mod.rs
git commit -m "feat: allocate idempotent producer identities"
```

### Task 2: `InitProducerId v0` API

**Files:**
- Create: `src/kafka/init_producer_id.rs`
- Modify: `src/kafka/mod.rs`
- Modify: `src/kafka/api_versions.rs`
- Modify: `src/kafka/dispatcher.rs`
- Modify: `tests/kafka_wire.rs`

**Interfaces:**
- Consumes: `BrokerState::producers().allocate()` from Task 1.
- Produces: `init_producer_id::VERSION_RANGE = 0..=0` and `response(&InitProducerIdRequest, &BrokerState) -> InitProducerIdResponse`.

- [ ] **Step 1: Write failing API and allocation tests**

After the DescribeGroups plan, update API counts from `16` to `17` and assert:

```rust
assert_api_range(&response, ApiKey::InitProducerId, 0, 0);
```

Dispatch two v0 requests with `transactional_id=None` and assert IDs `1`, `2`, epoch `0`, and error `0`. Dispatch one request with `transactional_id=Some("transactional")` and assert:

```rust
assert_eq!(response.error_code, ResponseError::UnsupportedForMessageFormat.code());
assert_eq!(i64::from(response.producer_id), -1);
assert_eq!(response.producer_epoch, -1);
```

- [ ] **Step 2: Run and verify RED**

Run: `cargo test --test kafka_wire init_producer_id`

Expected: FAIL because the API is unsupported.

- [ ] **Step 3: Implement the v0 handler**

For a null transactional ID, allocate and build:

```rust
InitProducerIdResponse::default()
    .with_throttle_time_ms(0)
    .with_error_code(0)
    .with_producer_id(ProducerId::from(identity.producer_id))
    .with_producer_epoch(identity.producer_epoch)
```

For a transactional ID, return `UnsupportedForMessageFormat`, producer ID `-1`, and epoch `-1`. Map allocator exhaustion to `UnknownServerError` without panicking.

- [ ] **Step 4: Advertise and dispatch only v0**

Add the module, API range, and a dispatcher arm matching `RequestKind::InitProducerId(body)`. Keep `require_version` so v1+ remains rejected.

- [ ] **Step 5: Run API tests GREEN**

Run:

```bash
cargo test --test kafka_wire init_producer_id
cargo test --test kafka_wire api_versions
```

Expected: allocation, transactional rejection, and API matrix pass.

- [ ] **Step 6: Commit the handshake**

```bash
git add src/kafka/init_producer_id.rs src/kafka/mod.rs src/kafka/api_versions.rs src/kafka/dispatcher.rs tests/kafka_wire.rs
git commit -m "feat: initialize idempotent producers"
```

### Task 3: Partition sequence state and retry classification

**Files:**
- Modify: `src/broker/partition.rs`

**Interfaces:**
- Consumes: magic-2 `BatchDecodeInfo::{producer_id, producer_epoch, base_sequence, record_count}`.
- Produces: `RecordSetProducer`, `record_set_producer(&Bytes)`, expanded `AppendError`, and `AppendResult::appended`.

- [ ] **Step 1: Add failing unit tests for new idempotent appends**

Use the existing `RecordBatchEncoder` helpers to create batches with explicit producer ID, epoch, and sequences. Add separate tests for:

```rust
// New append.
assert_eq!(first.base_offset, 0);
assert!(first.appended);

// Exact retry.
assert_eq!(retry.base_offset, first.base_offset);
assert_eq!(retry.last_offset, first.last_offset);
assert_eq!(retry.record_count, first.record_count);
assert!(!retry.appended);
assert_eq!(log.next_offset().await, first.last_offset + 1);

// Next sequence.
assert_eq!(second.base_offset, first.last_offset + 1);

// Gap and stale duplicate.
assert_eq!(log.append(batch_at_sequence(7)).await, Err(AppendError::OutOfOrderSequence));
assert_eq!(log.append(changed_batch_at_sequence(0)).await, Err(AppendError::DuplicateSequence));
```

Also prove two producer IDs and two partitions have independent sequences, a six-request history evicts the first retry, sequence wraps from `i32::MAX` to `0`, and every rejected call leaves `next_offset` unchanged.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `cargo test broker::partition::tests::idempotent --lib`

Expected: compilation fails because producer-bearing batches are still rejected.

- [ ] **Step 3: Extend validated batch metadata**

Add header data and a CRC-based fingerprint:

```rust
struct ValidatedBatch {
    record_count: i32,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    crc: u32,
    bytes: Bytes,
}
```

Continue rejecting transactional and control batches. Accept either all non-idempotent batches (`producer_id == -1`, epoch and sequence `-1`) or all batches with one producer ID and epoch; reject mixed identities as `Malformed`.

- [ ] **Step 4: Add record-set inspection**

Expose:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RecordSetProducer {
    pub(crate) producer_id: i64,
    pub(crate) producer_epoch: i16,
}

pub(crate) fn record_set_producer(
    records: &Bytes,
) -> Result<Option<RecordSetProducer>, AppendError>
```

This uses the same structural validator as `append` and returns `None` for legacy non-idempotent RecordBatch input.

- [ ] **Step 5: Implement atomic per-producer state**

Add to `PartitionLogInner`:

```rust
producer_states: HashMap<i64, ProducerPartitionState>,

struct ProducerPartitionState {
    producer_epoch: i16,
    next_sequence: i32,
    recent: VecDeque<RecentAppend>,
}

struct RecentAppend {
    base_sequence: i32,
    fingerprints: Vec<(i32, u32)>,
    result: AppendResult,
}
```

The fingerprint tuple is `(record_count, crc)`. Keep exactly five `RecentAppend` entries per producer-partition. Cache the append result's offsets/count, but return a copy with `appended=false` for a retry. Classify under the existing partition mutex:

- all fingerprints match a cached `base_sequence`: return cached offsets with `appended=false`;
- first base sequence equals `next_sequence` and every later batch is contiguous: append and cache with `appended=true`;
- a lower sequence not in cache: `DuplicateSequence`;
- a higher sequence: `OutOfOrderSequence`.

Calculate wrapped next sequence with modulus `i32::MAX as i64 + 1`; never use debug-overflow-prone signed addition.

- [ ] **Step 6: Run partition tests GREEN**

Run: `cargo test broker::partition::tests --lib`

Expected: existing non-idempotent, malformed, compression, concurrency, and fetch tests plus all idempotence tests pass.

- [ ] **Step 7: Commit partition semantics**

```bash
git add src/broker/partition.rs
git commit -m "feat: deduplicate idempotent partition appends"
```

### Task 4: Produce identity validation and Kafka error mapping

**Files:**
- Modify: `src/kafka/produce.rs`
- Modify: `tests/kafka_wire.rs`

**Interfaces:**
- Consumes: `record_set_producer`, `ProducerCoordinator::validate`, and new `AppendError` variants.
- Produces: Kafka errors `UnknownProducerId`, `InvalidProducerEpoch`, `OutOfOrderSequenceNumber`, and `DuplicateSequenceNumber`; fetch notification only for new bytes.

- [ ] **Step 1: Write failing handler tests**

Allocate a producer through `InitProducerId`, then Produce a record set with ID `1`, epoch `0`, and sequence `0`. Assert success at offset `0`. Send the identical request and assert offset `0` again while latest remains `1`. Add literal rejection cases:

```rust
assert_partition_error(unknown_id_response, ResponseError::UnknownProducerId);
assert_partition_error(wrong_epoch_response, ResponseError::InvalidProducerEpoch);
assert_partition_error(gap_response, ResponseError::OutOfOrderSequenceNumber);
assert_partition_error(expired_retry_response, ResponseError::DuplicateSequenceNumber);
```

After each rejection, assert latest offset and fetched bytes are unchanged.

- [ ] **Step 2: Run and verify RED**

Run: `cargo test --test kafka_wire idempotent_produce`

Expected: FAIL because Produce still maps producer-bearing batches to `UnsupportedForMessageFormat`.

- [ ] **Step 3: Validate producer identity before append**

In `produce_partition`, inspect the record set. When it contains a producer identity, call:

```rust
broker
    .producers()
    .validate(identity.producer_id, identity.producer_epoch)
    .await
```

Map coordinator errors before calling `PartitionLog::append`. Structural inspection errors retain their existing malformed/unsupported mapping.

- [ ] **Step 4: Map sequence errors and suppress duplicate notifications**

Extend the append match:

```rust
Ok(result) => {
    if result.appended {
        broker.notify_append();
    }
    success_partition(request.index, result.base_offset)
}
Err(AppendError::OutOfOrderSequence) => {
    error_partition(request.index, ResponseError::OutOfOrderSequenceNumber)
}
Err(AppendError::DuplicateSequence) => {
    error_partition(request.index, ResponseError::DuplicateSequenceNumber)
}
```

Map unknown ID and invalid epoch from the coordinator to their same-named Kafka errors.

- [ ] **Step 5: Run wire and full Rust tests GREEN**

Run:

```bash
cargo test --test kafka_wire idempotent_produce
cargo test --test kafka_wire produce_
cargo test --all-targets --all-features
```

Expected: all tests pass and exact retry leaves latest offset unchanged.

- [ ] **Step 6: Commit Produce integration**

```bash
git add src/kafka/produce.rs tests/kafka_wire.rs
git commit -m "feat: accept idempotent produce requests"
```

### Task 5: Idempotence verification and documentation boundary

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: passing producer handshake and sequence tests.
- Produces: an accurate non-transactional idempotence claim before the real-client integration plan.

- [ ] **Step 1: Document the technical support boundary**

Add `InitProducerId 0` to the advertised API list. Remove idempotent producers from exclusions and explicitly retain transactional IDs, transactional/control batches, transactions, exactly-once semantics, and producer epoch recovery as exclusions.

- [ ] **Step 2: Run required verification**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
git diff --check
```

Expected: every command exits `0`.

- [ ] **Step 3: Commit documentation**

```bash
git add README.md
git commit -m "docs: define idempotent producer support"
```
