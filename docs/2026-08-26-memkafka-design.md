# MemKafka v0.1 Design Specification

**Date:** 2026-08-26  
**Status:** In progress
**Implementation:** Metadata and topic creation complete; Produce/Fetch is the next slice

## 1. Summary

MemKafka is a fast, single-binary, in-memory Kafka-compatible broker for local development and integration tests. The same process also exposes a Confluent-compatible Schema Registry HTTP API.

The project is deliberately not a Kafka replacement. It implements the externally visible Kafka behaviors needed by real clients while rejecting the production concerns that make Kafka slow and tedious to run in tests. The first compatibility target is `Confluent.Kafka` and its Confluent Avro Schema Registry integration.

The central product rule is:

> A feature is supported only when an unmodified real client passes a black-box test against the `memkafka` binary.

v0.1 provides real topics, partitions, offsets, ordered at-least-once Produce/Fetch behavior within one process lifetime, classic consumer groups, cooperative-sticky rebalancing, offset commits, and an Avro-first Schema Registry subset. All state lives in memory and disappears when the process exits.

## 2. Goals

MemKafka v0.1 must:

- start as one native Rust executable with no Java, Docker, KRaft, or external service dependency;
- include a maintained Dockerfile so the same executable can also run as a small container image when desired;
- become ready quickly enough to be started per integration-test suite;
- expose a Kafka TCP endpoint and Schema Registry HTTP endpoint from the same process;
- allow normal application code to use its real `Confluent.Kafka` configuration unchanged except for endpoint addresses and unsupported production features;
- auto-create unknown topics by default with exactly two partitions;
- support explicit topic creation with a requested partition count and replication factor `1`;
- preserve Kafka keys, values, headers, timestamps, compression, partition ordering, and offsets;
- provide at-least-once delivery for acknowledged records within one MemKafka process lifetime when producers retry unknown outcomes and consumers commit only after processing;
- implement real classic consumer-group coordination, including multiple members, generations, heartbeats, session expiry, rebalances, and committed offsets;
- interoperate with the real `cooperative-sticky` assignor in librdkafka and make the selected protocol visible in logs;
- provide the Schema Registry operations required by Confluent's Avro serializer and deserializer;
- fail clearly when a client requests a feature outside the supported subset.

## 3. Non-goals and positioning

MemKafka is test infrastructure, not production infrastructure. It makes no durability, availability, fault-tolerance, throughput, or fidelity claims beyond its explicit compatibility tests.

It does not try to reproduce Kafka's internal architecture. There is one virtual broker, no replicated log, no controller quorum, and no internal topics. Kafka's wire protocol is an adapter over small in-memory state machines.

The project should remain useful because it is narrow. Broad protocol compatibility can grow after v0.1 through additional real-client test suites, but v0.1 does not claim compatibility with every Kafka client.

## 4. User experience and defaults

The default invocation is:

```bash
memkafka
```

Default behavior:

```text
Kafka endpoint          127.0.0.1:9092
Schema Registry         http://127.0.0.1:8081
Broker ID               1
Auto-create topics      true
Default partitions      2
Storage                 memory only
```

The following settings must be configurable through stable command-line options:

```text
--kafka-listen <host:port>
--kafka-advertised-address <host:port>
--schema-registry-listen <host:port>
--auto-create-topics <true|false>
--default-partitions <positive integer>
--log-level <error|warn|info|debug|trace>
--quiet
```

Configuration errors and address-binding failures are fatal and must be reported before readiness. Once both listeners are accepting connections, MemKafka emits one unambiguous readiness log line containing both resolved endpoints. `--quiet` suppresses the banner and ordinary informational logs, but not fatal startup errors.

Auto-creation may be triggered by a topic-specific metadata lookup or a Produce request. With auto-creation disabled, the broker returns Kafka's unknown-topic-or-partition error instead. Explicit topic creation always overrides the default partition count.

### 4.1 Docker image requirement

v0.1 must include a root-level `Dockerfile`, even though running the native binary remains the primary local-development path. The image must:

- use a multi-stage build with the same pinned latest-stable Rust version as the project;
- copy only the compiled `memkafka` executable into a small maintained runtime image;
- run as a non-root user;
- expose Kafka on port `9092` and Schema Registry on port `8081`;
- bind both servers to `0.0.0.0` inside the container;
- default the Kafka advertised address to `localhost:9092` for host-to-container use;
- allow the advertised address to be overridden for Docker Compose or other container networks;
- use an exec-form entrypoint so shutdown signals reach MemKafka directly;
- contain no Java runtime, Kafka distribution, build toolchain, or source code in the final runtime layer.

The repository should also include a small `.dockerignore` that excludes build output, version-control data, editor files, and other unnecessary build-context content.

The expected basic workflow is:

```bash
docker build -t memkafka .
docker run --rm -p 9092:9092 -p 8081:8081 memkafka
```

Container support is a packaging requirement, not a separate runtime implementation. The image runs the exact same binary and acceptance suite as native execution.

## 5. Architecture

MemKafka runs both servers on one asynchronous Rust runtime:

```text
memkafka process
├── Kafka TCP server
│   ├── frame reader/writer
│   ├── protocol decoder/encoder
│   ├── API version negotiation
│   └── request dispatcher
├── Broker state
│   ├── topic catalog
│   ├── partition logs
│   ├── fetch notifications
│   └── classic group coordinator
└── Schema Registry HTTP server
    ├── subjects and versions
    ├── global schema IDs
    └── Confluent-compatible responses
```

### 5.1 Modern Rust baseline

MemKafka starts on **Rust 1.98.0 stable**, the current stable release when this specification was written, and uses the **Rust 2024 edition**. The repository pins that toolchain in `rust-toolchain.toml` for reproducible builds and declares `edition = "2024"` plus `rust-version = "1.98"` in `Cargo.toml`.

This is a latest-stable policy, not a permanent Rust 1.98 ceiling:

- before every MemKafka release, update the pinned toolchain to the newest stable Rust release and run the full test suite;
- use stable Rust features and modern 2024-edition idioms; do not depend on nightly feature gates unless a concrete blocker is documented and deliberately accepted;
- v0.1 supports its pinned current-stable toolchain only and carries no older-compiler compatibility burden;
- start with the latest compatible, non-prerelease versions of maintained dependencies and commit `Cargo.lock`;
- keep dependencies intentionally small, remove unused crates, and review upgrades rather than allowing unbounded version ranges;
- use current async and observability conventions: structured concurrency, explicit cancellation and shutdown, `tracing`-based structured logs, and no locks held across `.await` points;
- set `unsafe_code = "forbid"` at the workspace level unless a narrowly documented exception becomes unavoidable;
- enforce formatting, warnings, and Clippy in CI across all targets and features.

The initial dependency direction is current stable releases of Tokio for the runtime, Bytes for owned wire buffers, Axum/Tower for HTTP, `tracing` for diagnostics, and a maintained generated Kafka protocol crate. Dependency choices remain replaceable implementation details: behavior is defined by the real-client tests, not by a particular framework.

References: [Rust 1.98.0 release announcement](https://blog.rust-lang.org/2026/08/20/Rust-1.98.0/) and [Rust 2024 Edition Guide](https://doc.rust-lang.org/edition-guide/rust-2024/).

The implementation should use generated Kafka protocol types from a maintained Rust protocol crate rather than hand-writing every request and response codec. The exact crate version and advertised Kafka API-version matrix are pinned with the v0.1 test dependencies. MemKafka must advertise only API versions it actually implements; unsupported API keys or versions return the appropriate protocol error instead of being accepted optimistically.

The protocol layer owns framing, correlation IDs, version-aware encoding, and Kafka error mapping. Domain components own broker behavior and remain independent of connection state. The HTTP layer owns Confluent REST shapes and delegates schema identity and versioning to the registry store.

## 6. Single-broker metadata and topic lifecycle

Broker ID `1` is the controller, leader, and sole replica for every partition. All metadata responses use the externally reachable address configured for the Kafka listener.

A topic contains a fixed vector of partitions. v0.1 supports creating topics but not increasing their partition count later.

Topic creation rules:

- auto-created topics contain exactly `default_partitions`, which defaults to `2`;
- explicitly created topics use the requested positive partition count;
- replication factor `1` is accepted;
- any other replication factor is rejected with a clear Kafka error because replication is not simulated;
- repeating an equivalent create request is handled consistently with Kafka's topic-already-exists semantics;
- topic names and partition indexes are validated before state is mutated.

The initial Kafka API surface includes the narrow version set needed for these behaviors:

- `ApiVersions`
- `Metadata`
- `CreateTopics`
- `Produce`
- `Fetch`
- `ListOffsets`
- `FindCoordinator`
- `JoinGroup`
- `SyncGroup`
- `Heartbeat`
- `LeaveGroup`
- `OffsetCommit`
- `OffsetFetch`

The precise numeric versions are an interoperability decision: tests pin a `Confluent.Kafka` minor line, record what librdkafka negotiates, and lock the smallest working version set. Adding an API version requires corresponding black-box coverage and must never change existing semantics silently.

The producer client chooses a partition using the metadata MemKafka advertises. MemKafka does not reimplement librdkafka's Murmur2 or sticky producer partitioners; it appends to the partition named in the Produce request.

## 7. Partition storage: raw RecordBatch plus metadata index

Each partition is an independent append-only in-memory log:

```text
Partition
├── next_offset: i64
├── batches: ordered collection of StoredBatch
└── append notification

StoredBatch
├── base_offset: i64
├── last_offset: i64
├── record_count: i32
├── encoded_size: usize
└── bytes: immutable Kafka RecordBatch bytes
```

MemKafka stores the client's Kafka RecordBatch representation almost unchanged. It parses only enough batch metadata to validate structural bounds, determine the offset span, maintain the index, and rewrite the broker-assigned outer base offset when necessary. The record payload remains opaque.

This preserves compression, keys, values, headers, timestamps, and client serialization without translating records into a MemKafka-specific model. It also keeps the broker serializer-agnostic and compression-agnostic. Avro knowledge belongs to Schema Registry, not to the Kafka storage layer.

v0.1 accepts modern RecordBatch format (`magic = 2`). Legacy message-set formats are outside scope.

### 7.1 Produce

Appending a valid batch is serialized per partition:

1. Read the partition's `next_offset` as the batch base offset.
2. Calculate the batch's last offset from its encoded offset span.
3. Store the immutable batch bytes and metadata index entry.
4. Advance `next_offset` atomically with the append.
5. Wake pending Fetch requests.
6. Return the assigned base offset when a response is required.

Concurrent producers may append to the same partition, but no two records may receive the same offset and each partition's stored order must match its assigned-offset order.

Acknowledgement modes:

- `acks=0`: append without sending a Produce response;
- `acks=1`: append, then report success;
- `acks=all` (`-1`): equivalent to `acks=1` because the in-sync replica set contains only the single virtual broker.

These acknowledgements do not imply persistence. Idempotent production and producer epochs are not supported in v0.1.

### 7.2 Fetch

For a requested offset, MemKafka locates the first stored batch whose `last_offset` is greater than or equal to the requested offset. It returns complete Kafka batches from that point. Returning a complete batch when the requested offset falls inside it is required so compressed batches never need to be decompressed and rebuilt by the broker.

Fetch respects `partition_max_bytes`, `max_bytes`, `min_bytes`, and `max_wait_ms` sufficiently for real-client behavior. As in Kafka, the first eligible oversized batch may be returned despite a byte limit so the consumer can make progress.

An empty Fetch long-polls until enough data is available, a relevant append occurs, or `max_wait_ms` expires. A multi-partition Fetch waits on shared append notifications and reevaluates availability; it does not create one timer task per partition. No state lock is held while waiting.

With no retention or transactions:

```text
log_start_offset   = 0
high_watermark     = next_offset
last_stable_offset = next_offset
```

`ListOffsets` returns offset `0` for earliest and `next_offset` for latest.

### 7.3 Delivery and ordering contract

Within one running MemKafka process, a successful `acks=1` or `acks=all` Produce response means the complete batch was atomically appended before acknowledgement. Every acknowledged record remains fetchable until process shutdown. If the connection or response is lost after append, a retry may append a duplicate; MemKafka guarantees at-least-once delivery, not exactly-once delivery or deduplication.

At-least-once delivery is an end-to-end contract with the real client: the producer must retry requests whose outcome is unknown, and the consumer must commit an offset only after processing the corresponding record. The guarantee does not apply to `acks=0`, a producer that abandons an unknown result, a consumer that commits before processing, or any state after MemKafka exits.

Each partition's assigned offsets define its canonical total order. Appends to one partition are serialized, and Fetch returns stored records in ascending offset order without broker-side reordering. For concurrent producers, this guarantees assigned-offset order rather than wall-clock invocation order. Sequential sends that wait for acknowledgement on the same partition are observed in the same sequence.

## 8. Classic consumer groups

v0.1 implements the classic Kafka consumer-group protocol as real broker behavior. A single-consumer shortcut is explicitly insufficient.

Each group maintains:

```text
Group
├── group_id
├── state
├── generation_id
├── protocol_type
├── selected_protocol
├── leader_member_id
├── members
│   ├── member_id
│   ├── client_id
│   ├── session_timeout
│   ├── last_heartbeat
│   ├── advertised protocols
│   └── opaque subscription metadata
├── opaque assignments by member
└── committed offsets by topic/partition
```

The required state machine is:

```text
Empty
  → PreparingRebalance
  → CompletingRebalance
  → Stable
  → PreparingRebalance  (membership or subscription change)
  → Empty               (last member leaves or expires)
```

`Dead` may be used internally for a coordinator being removed or shut down, but normal v0.1 groups remain in memory until process exit.

Required semantics:

- `FindCoordinator` always resolves the single virtual broker;
- member IDs and group leaders are assigned consistently;
- the generation increments for every rebalance;
- all members receive the negotiated common protocol and current generation;
- only the elected client leader receives all member subscription blobs and submits assignments through `SyncGroup`;
- assignments remain opaque to MemKafka and are returned to their target members;
- heartbeats keep a stable member alive;
- joining, graceful leaving, changed subscriptions, and session expiry trigger rebalances;
- stale generations and unknown members are fenced with Kafka-compatible errors;
- OffsetCommit and OffsetFetch persist offsets in memory independently for each group;
- clients may use automatic commits or disable them and commit offsets explicitly;
- committed offsets survive consumer restarts within the same MemKafka process;
- different groups consume and commit independently.

Coordination transitions for one group are serialized. Network connections do not own group membership: disconnecting a socket alone does not immediately remove a member, because a crashed client must follow the session-timeout path unless it sent `LeaveGroup`.

## 9. Cooperative-sticky behavior and logging

`cooperative-sticky` is a mandatory v0.1 compatibility scenario, not an approximation implemented by MemKafka.

In the classic protocol, consumers advertise assignment protocols and opaque subscription metadata in `JoinGroup`. MemKafka negotiates a common protocol, elects a leader, and forwards member metadata to that leader. The real librdkafka cooperative-sticky assignor computes the assignment, including currently owned partitions, and returns opaque member assignments through `SyncGroup`.

MemKafka therefore implements coordination while the client implements assignment. It must support successive cooperative rebalance generations so partitions can be revoked in one round and transferred in a later round without simultaneous ownership.

When cooperative-sticky is selected, an info-level structured event is required:

```text
INFO group=equipment-events generation=7 protocol=cooperative-sticky \
     rebalance=cooperative members=3 "Using cooperative incremental rebalancing"
```

Debug logging should expose negotiation and transitions without logging message values or schema bodies:

```text
DEBUG group=equipment-events member=consumer-a \
      advertised_protocols=[cooperative-sticky] owned_partitions=[events[0],events[2]]
```

Other required info-level group events are member join/leave/expiry, rebalance start, selected protocol, stable generation, and committed offsets. Unsupported or non-common assignment protocols produce a warning and the appropriate JoinGroup error.

## 10. Schema Registry v0.1

The HTTP server implements the Confluent response shapes and error envelope needed by the real Confluent Avro serializer and deserializer.

Registry state is independent of Kafka topics:

```text
Registry
├── next_global_schema_id
├── schemas_by_id
└── subjects
    └── ordered versions
        ├── version
        └── schema_id
```

Required endpoints:

```text
GET  /subjects
POST /subjects/{subject}
POST /subjects/{subject}/versions
GET  /subjects/{subject}/versions
GET  /subjects/{subject}/versions/{version}
GET  /subjects/{subject}/versions/latest
GET  /schemas/ids/{id}
GET  /config
GET  /config/{subject}
```

Registration behavior:

- absent `schemaType` and `schemaType: AVRO` are accepted;
- Protobuf, JSON Schema, and schema references are rejected as unsupported in v0.1;
- IDs are global, positive, and monotonically allocated;
- versions are positive and monotonically allocated per subject;
- registering the exact same schema string again for a subject returns the existing ID and version instead of creating a duplicate;
- the exact same schema string across subjects reuses the global schema ID while receiving a subject-local version;
- semantically equivalent schemas with different text are not normalized in v0.1;
- compatibility mode is always `NONE`; compatibility enforcement and mutation are outside scope;
- missing subjects, versions, and IDs use Confluent-compatible HTTP status codes and `{ "error_code", "message" }` response bodies.

MemKafka never serializes or deserializes application records. The real Confluent serializer registers the schema and writes the Confluent wire-format schema ID into the Kafka record; the real deserializer reads that ID and obtains the schema through the registry endpoint.

## 11. Concurrency, lifecycle, and error handling

The shared state must preserve these invariants:

- topic names are unique;
- partition indexes are fixed after topic creation;
- offsets are unique, monotonically increasing, and independent per partition;
- batches are immutable after append;
- an acknowledged batch remains fetchable in assigned-offset order until shutdown;
- group generation changes and member transitions are atomic per group;
- committed offsets are isolated by group and topic/partition;
- schema IDs are globally unique and subject versions are ordered.

Locks must remain local to a topic, partition, group, or registry operation where practical. Socket writes, Fetch waits, and HTTP response streaming must not hold shared-state locks.

Malformed TCP frames, invalid lengths, and decode failures terminate only the offending connection and are logged with connection context. A well-formed but invalid request receives the closest Kafka error supported by that response version. Unsupported API keys or versions do not crash the process.

HTTP validation failures return Confluent-compatible JSON errors. Unexpected handler failures return a generic server error without exposing internal details. One client failure must not corrupt shared broker or registry state.

On normal process shutdown, listeners stop accepting work and in-flight responses receive a short opportunity to finish. State is then discarded; there is no shutdown persistence step.

## 12. Test strategy and acceptance contract

Testing has three layers:

1. **Protocol and unit tests** validate framing, versioned encoding, error mapping, batch indexing, offsets, schema identity, and group state transitions.
2. **State-machine and concurrency tests** validate interleavings, timeouts, rebalance generations, unique offsets, and wake-up behavior with deterministic time where possible.
3. **Black-box compatibility tests** start the real compiled `memkafka` process and communicate only through its Kafka and HTTP endpoints using real clients.

Internal Rust tests are necessary but never sufficient to claim client compatibility. The v0.1 acceptance suite pins a supported `Confluent.Kafka` minor line and the matching Confluent Schema Registry Avro packages. Independent metadata, topic-creation, and baseline delivery checks also pin the Apache Kafka Java client, a pure-Rust client, and a pure-Go client. The release notes record those exact versions.

### 12.1 Broker and producer acceptance

The first four metadata and topic-creation scenarios and the baseline publish/consume, ordering, and repeated-fetch scenarios run through all four pinned clients: Confluent.Kafka, Apache Kafka Java, pure Rust, and pure Go. The remaining advanced scenarios are mandatory through the primary Confluent.Kafka suite.

- connect and negotiate API versions;
- auto-create an unknown topic and observe exactly two partitions through the real metadata API;
- create a topic explicitly with six partitions and replication factor `1`;
- reject unsupported replication factors;
- produce and consume one record and multiple batches;
- publish ten numbered records sequentially to one explicit partition and consume the identical sequence at contiguous offsets;
- fetch again from the same uncommitted offset and receive the records again, demonstrating at-least-once redelivery;
- preserve keys, values, headers, timestamps, and per-partition ordering;
- allocate monotonically increasing offsets across Produce requests;
- keep offsets independent across partitions;
- let librdkafka perform keyed and sticky partition selection across multiple partitions;
- handle concurrent producers without duplicate offsets;
- return correct earliest/latest offsets and watermarks;
- round-trip uncompressed, gzip, snappy, lz4, and zstd batches produced by the real client;
- long-poll an empty Fetch, wake it on Produce, and time it out correctly.

### 12.2 Consumer-group acceptance

- `Subscribe()` and `Consume()` work through the classic group protocol;
- manual `Assign()` and seeking to an explicit partition offset work without group coordination;
- earliest/latest reset behavior works;
- automatic commits are returned after restarting a consumer within the same process;
- with automatic commits disabled, an explicit manual commit is returned after restarting a consumer within the same process;
- restart a consumer without committing and redeliver the previously processed record;
- commit only after processing, restart the consumer, and resume after the committed record;
- separate groups consume and commit independently;
- two or more consumers receive non-overlapping assignments covering every partition;
- joining and graceful leaving trigger rebalances;
- an ungraceful consumer failure triggers rebalance only after session expiry;
- heartbeats preserve membership;
- generations increment and stale generations are rejected;
- offset commits during invalid group states receive the correct error.

### 12.3 Cooperative-sticky acceptance

Configure the real client with:

```csharp
PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
```

Then verify:

- librdkafka advertises `cooperative-sticky` and MemKafka selects it;
- the mandatory info log records the selection;
- currently owned partitions pass through subscription metadata;
- adding consumers minimizes movement according to the real client assignor;
- a partition is never assigned to a new member while the prior owner still reports it as owned;
- successive rebalance rounds complete a transfer and reach Stable;
- removing or timing out a member redistributes all partitions without duplicates.

The primary scenario uses a six-partition topic and consumers A, B, and C. After every stable generation, the intersection of assignments must be empty and their union must equal all six partitions.

### 12.4 Schema Registry acceptance

Using the real `CachedSchemaRegistryClient`, `AvroSerializer`, and `AvroDeserializer`:

- automatically register an Avro schema;
- assign and return a global schema ID;
- avoid duplicate IDs and versions for an identical registration;
- increment versions for distinct schemas under one subject;
- list subjects and versions;
- fetch schemas by ID and subject version;
- produce a Kafka record containing the Confluent wire-format schema ID;
- fetch the record and deserialize it successfully through the real Avro deserializer;
- return Confluent-compatible errors for missing resources and unsupported schema types.

## 13. Explicit v0.1 exclusions

The following are not implemented or simulated in v0.1:

- persistence, recovery, snapshots, or durability;
- multiple brokers, replication, real ISR behavior, leader election, rack awareness, or failover;
- KRaft, ZooKeeper, controller protocols, or internal Kafka topics;
- transactions, exactly-once semantics, idempotent producers, producer IDs, or epochs;
- KIP-848's newer consumer group protocol, `ConsumerGroupHeartbeat`, or broker-side assignors;
- retention policies, log compaction, segment files, tiered storage, or DeleteRecords;
- partition-count increases, topic deletion, ACLs, quotas, or most administrative/configuration APIs;
- TLS, SASL, authentication, authorization, or multi-tenant isolation;
- realistic latency, network faults, disk faults, broker restarts, or performance benchmarking against Kafka;
- legacy message-set formats predating RecordBatch magic `2`;
- Protobuf or JSON Schema registry support;
- schema references, compatibility enforcement, compatibility configuration changes, deletion, or semantic schema normalization;
- Java, Rust, and Go client compatibility beyond their explicitly tested slices, or guaranteed compatibility with Python and other Kafka clients until each has its own black-box suite.

If an excluded Kafka feature is requested, MemKafka must return a clear protocol error or omit the capability from `ApiVersions`. It must not silently claim production semantics it does not provide.

## 14. v0.1 completion criteria

v0.1 is complete when:

1. a clean build on the pinned latest-stable Rust toolchain produces one distributable `memkafka` executable;
2. the root Dockerfile produces a non-root runtime image containing that binary, and the documented `docker run` command exposes both endpoints;
3. the binary starts both endpoints with the documented defaults and reports readiness;
4. every mandatory black-box test in Section 12 passes against its pinned real clients;
5. cooperative-sticky selection and rebalance lifecycle are visible in logs;
6. unsupported features fail clearly without crashing or corrupting state;
7. formatting and strict Clippy checks pass across all targets and features;
8. the README states the compatibility target, native and container startup paths, ephemeral data model, Rust baseline, and exclusions without implying production suitability;
9. acknowledged records remain fetchable in assigned-offset order for the lifetime of the process, and the real-client tests demonstrate at-least-once redelivery.

Implementation must not expand v0.1 merely to imitate Kafka internals. New behavior enters scope only when required by the pinned real-client acceptance suite or added explicitly to this specification.

## 15. Optional future benchmarks

It could be fun to add a small set of useful benchmarks at the bottom of the README after v0.1. This is not a completion requirement.

One useful comparison would measure container startup-to-readiness time for Kafka plus Schema Registry versus MemKafka. Any published result should name the images and versions, define readiness consistently, describe the machine and cold/warm-cache conditions, and include enough repeated runs to avoid presenting a one-off timing as representative.
