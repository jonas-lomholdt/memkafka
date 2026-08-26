# Classic Groups and Offset Commits Implementation Plan

> **Status:** Approved for implementation on 2026-08-26.

**Goal:** Add a real classic consumer-group coordinator foundation and prove subscribe/consume, automatic commits, explicit commits, restart resume, and uncommitted redelivery through Confluent.Kafka.

**Architecture:** `BrokerState` owns a `GroupCoordinator`. The coordinator keeps a concurrent catalog of groups, while every individual `Group` is protected by its own Tokio mutex so membership transitions and offset changes are serialized per group. Protocol handlers translate Kafka request types into coordinator operations; subscription metadata and assignments remain opaque bytes owned by the client assignor.

**Version boundary:** Advertise the newest non-flexible versions required by the pinned librdkafka client: FindCoordinator v0–2, JoinGroup v0–5, SyncGroup v0–3, Heartbeat v0–3, LeaveGroup v0–3, OffsetCommit v2–7, and OffsetFetch v1–5.

**Not deferred:** This checkpoint uses the real group state model, generations, leader election, protocol negotiation, fencing, and opaque assignments. It is not a direct-assignment or commits-only shortcut.

**Next checkpoint:** Extend this same coordinator to simultaneous multi-member joins, deterministic session expiry, successive cooperative rebalances, and the full §12.2–12.3 acceptance matrix.

---

## Task 1: Add the per-group state machine

**Files:**

- Create: `src/broker/groups.rs`
- Modify: `src/broker/mod.rs`

1. Write failing unit tests for:
   - an empty member ID receiving a generated member ID without mutating the generation;
   - the assigned member joining generation `1` as leader;
   - protocol selection from the member's advertised protocols;
   - leader-only member metadata visibility;
   - leader assignments moving `CompletingRebalance → Stable`;
   - heartbeat success and stale generation/member fencing;
   - graceful leave returning the group to `Empty` while retaining offsets.
2. Run `cargo test broker::groups` and confirm the tests fail for missing behavior.
3. Implement `GroupCoordinator`, `Group`, `GroupState`, `Member`, and typed coordinator results/errors.
4. Keep member IDs monotonic and opaque, preserve protocol metadata and assignments as `Bytes`, and never hold a catalog lock while awaiting a group lock.
5. Run the focused tests until green.

## Task 2: Add isolated in-memory committed offsets

**Files:**

- Modify: `src/broker/groups.rs`

1. Write failing tests proving:
   - commits are isolated by group/topic/partition;
   - an offset overwrites only the same group/topic/partition key;
   - missing offsets return Kafka's unset offset (`-1`);
   - a stable member with the current generation can commit;
   - unknown members and stale generations are rejected;
   - offsets remain after the last member leaves.
2. Run `cargo test broker::groups` and observe RED.
3. Add the committed-offset map and validated commit/fetch operations.
4. Run the focused tests until green.

## Task 3: Expose the coordinator APIs

**Files:**

- Create: `src/kafka/find_coordinator.rs`
- Create: `src/kafka/join_group.rs`
- Create: `src/kafka/sync_group.rs`
- Create: `src/kafka/heartbeat.rs`
- Create: `src/kafka/leave_group.rs`
- Create: `src/kafka/offset_commit.rs`
- Create: `src/kafka/offset_fetch.rs`
- Modify: `src/kafka/mod.rs`
- Modify: `src/kafka/dispatcher.rs`
- Modify: `src/kafka/api_versions.rs`
- Modify: `tests/kafka_wire.rs`

1. Add failing wire/dispatcher tests for every advertised API and maximum version.
2. Add handler unit tests for success plus `MemberIdRequired`, `UnknownMemberId`, `IllegalGeneration`, and `RebalanceInProgress` mappings.
3. Run the focused tests and observe RED.
4. Implement handlers and dispatcher routing. `FindCoordinator` always returns broker `1` and the configured advertised host/port.
5. Encode only fields valid for the request version; do not advertise flexible versions in this checkpoint.
6. Run focused Kafka tests until green.

## Task 4: Prove group delivery and commit semantics with Confluent.Kafka

**Files:**

- Modify: `tests/confluent/Program.cs`
- Modify: `.github/workflows/test.yml` only if the existing Confluent job does not already execute the acceptance program

1. Add black-box scenarios using `Subscribe()` and `Consume()`:
   - consume records with auto commit, wait for the commit interval, close, restart with the same group, and resume at the next offset;
   - disable auto commit, explicitly commit after processing, restart, and resume at the next offset;
   - disable auto commit, process without committing, restart, and receive the record again.
2. Run the suite against the current binary and confirm RED because the group APIs are absent.
3. Complete any protocol corrections exposed by the real client, adding focused regression tests before fixes.
4. Run the Confluent suite natively and against the Docker image until green.

## Task 5: Update product truth and verify the checkpoint

**Files:**

- Modify: `README.md`
- Modify: `docs/2026-08-26-memkafka-design.md`

1. Update the implementation status and API matrix with only behavior proven by tests.
2. Keep multi-member rebalance/session-expiry/cooperative-sticky work clearly marked as pending.
3. Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
cargo clippy --manifest-path tests/rust-client/Cargo.toml --all-targets --all-features -- -D warnings
cargo test --manifest-path tests/rust-client/Cargo.toml --locked
dotnet restore tests/confluent/MemKafka.Acceptance.csproj --locked-mode
dotnet run --project tests/confluent/MemKafka.Acceptance.csproj --no-restore
docker build -t memkafka:ci .
```

4. Run all four existing client suites against `memkafka:ci`; the group scenarios are mandatory through Confluent.Kafka, while the Java, Rust, and Go suites guard baseline compatibility.
5. Commit the plan separately, then commit implementation and black-box coverage in coherent checkpoints. Do not push unless requested.
