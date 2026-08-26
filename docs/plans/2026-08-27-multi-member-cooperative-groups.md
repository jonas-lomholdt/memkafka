# Multi-member cooperative groups implementation plan

> **Status:** Completed 2026-08-27.

**Goal:** Complete the classic-group behavior in §§8–9 and the mandatory multi-member acceptance scenarios in §§12.2–12.3 of the design specification.

**Architecture:** Keep one Tokio mutex per group, but turn JoinGroup and SyncGroup into real asynchronous barriers. A rebalance epoch tracks which live members have rejoined; one generation is published only after the full cohort arrives. Join and Sync waiters use a per-group notification and always re-check state after waking. A per-group deadline driver independently enforces generation Join/Sync deadlines using the largest member rebalance timeout and stable-member expiry using the session timeout; all deadlines derive from Tokio `Instant` for deterministic paused-time tests. Subscription metadata and assignments remain opaque to the coordinator; decoding owned partitions is best-effort and logging-only.

**Acceptance boundary:** The real Confluent.Kafka 2.15.0 cooperative-sticky assignor owns partition assignment. MemKafka coordinates generations, forwards subscription metadata, installs leader-provided assignments, fences invalid requests, and never assigns a partition itself.

## Task 1: Add a red real-client multi-member scenario

**Files:**

- Modify: `tests/confluent/Program.cs`

1. Create one six-partition topic and enough records to make every assigned partition observable.
2. Start consumers A and B concurrently with `PartitionAssignmentStrategy.CooperativeSticky` and record assignment/revocation callbacks.
3. Assert that their stable assignments are disjoint and together cover all six partitions.
4. Add consumer C, wait for successive cooperative rounds to settle, and repeat the disjoint/full-coverage assertion.
5. Gracefully close one member and assert the survivors redistribute all partitions without duplicates.
6. Run the native suite and confirm RED against the current immediate-response coordinator.

## Task 2: Implement a generation-safe JoinGroup barrier

**Files:**

- Modify: `src/broker/groups.rs`
- Modify: `src/kafka/join_group.rs`

1. Add failing paused-time unit tests proving two simultaneous members receive the same generation, only the leader sees all subscription blobs, and a late join starts exactly one new generation.
2. Track the active rebalance epoch, each member's joined epoch, and a completed immutable join result per member.
3. Start a rebalance on membership or subscription change, clear prior assignments, and wake stale SyncGroup waiters.
4. Complete the join barrier only when every live member has joined the active epoch; negotiate one common protocol and increment the generation once.
5. Return the same generation/protocol/leader snapshot to every waiter, with the full member list only for the elected leader.

## Task 3: Implement the SyncGroup barrier and fencing

**Files:**

- Modify: `src/broker/groups.rs`
- Modify: `src/kafka/sync_group.rs`
- Modify: `tests/kafka_wire.rs`

1. Add failing tests where a follower SyncGroup waits until the leader submits complete assignments.
2. Atomically validate and install exactly one assignment for every current member.
3. Wake followers only after the generation becomes Stable; return RebalanceInProgress if a newer rebalance supersedes their wait.
4. Keep UnknownMemberId, IllegalGeneration, incomplete/duplicate assignment, and commit-during-rebalance behavior explicitly covered.

## Task 4: Expire silent members deterministically

**Files:**

- Modify: `src/broker/groups.rs`

1. Add paused-time tests proving heartbeats extend membership and a silent member is retained before, then removed at, its session deadline.
2. When a join or sync barrier waits, sleep only until the generation's rebalance deadline or a state notification, then re-check under the group lock.
3. Also prune expired members at coordinator entry points so a survivor heartbeat or later join observes expiry promptly.
4. Expiry logs the member, triggers one rebalance for survivors, elects a replacement leader when required, and moves the last-member group to Empty without deleting committed offsets.

## Task 5: Prove cooperative rounds and diagnostics

**Files:**

- Modify: `src/broker/groups.rs`
- Modify: `src/kafka/join_group.rs`
- Modify: `tests/confluent/Program.cs`

1. Decode `ConsumerProtocolSubscription` only for structured debug logging of currently owned partitions; malformed metadata remains opaque and never breaks coordination.
2. Emit the mandatory info event once per completed cooperative generation with the real group, generation, protocol, rebalance type, and member count.
3. Extend the real-client scenario to verify that a partition is not transferred while its prior owner still reports it as owned.
4. Add an ungraceful member stop, wait past its short session timeout, and assert complete non-overlapping redistribution among survivors.
5. Assert separate groups still consume and commit independently.

## Task 6: Update product truth, verify, review, and commit

**Files:**

- Modify: `README.md`
- Modify: `docs/2026-08-26-memkafka-design.md`

1. Update status only for behavior proven through the real client.
2. Run root formatting, strict Clippy, and all Rust tests.
3. Run the Confluent suite natively and against a freshly built Docker image.
4. Run the Java, Rust, Go, and Kafbat regression suites against that image.
5. Request an independent correctness review, fix material findings, and commit without pushing.

## Result

The coordinator now provides generation-safe asynchronous Join/Sync barriers, cached retry responses, classic heartbeat fencing, independently driven generation-level rebalance deadlines and session expiry, graceful leave, retained offsets, and cooperative diagnostics. The Confluent.Kafka acceptance suite proves exact disjoint coverage, minimal A/B-to-A/B/C movement, no transfer before revocation, continuous pre-expiry ownership, and eventual redistribution after both graceful and ungraceful departure.
