# Kafka modern discovery design

**Date:** 2026-09-01  
**Roadmap slice:** Cut 3 in [`kafka-api-parity-roadmap.md`](kafka-api-parity-roadmap.md)  
**Kafka baseline:** Apache Kafka 4.3.1

## Goal

Give every in-memory topic one stable Kafka UUID for its lifetime and expose that identity consistently through current Kafka discovery APIs. Add `DescribeCluster` and paginated `DescribeTopicPartitions`, modernize `Metadata`, and return topic IDs from `CreateTopics` v7. Prove the complete slice through focused Rust tests and the unmodified Apache Kafka Java 4.3.1 Admin client.

## User-visible result

After this cut:

- a topic receives one non-zero random UUID when it is created;
- every successful name-based or ID-based discovery path returns that UUID;
- restarting MemKafka creates a fresh in-memory catalog and therefore fresh topic UUIDs;
- Java `Admin.describeCluster()` reports one coherent MemKafka broker/controller;
- Java `Admin.describeTopics()` uses `DescribeTopicPartitions` and transparently follows pagination cursors;
- existing producer, consumer, group, Schema Registry, Kafbat, and multi-listener behavior remains unchanged.

## Non-goals

This cut does not add:

- persistence or UUID stability across process restarts;
- topic deletion or recreation within one process;
- partition growth or truncation;
- Fetch, ListOffsets, or offset APIs that address topics by UUID;
- a generic storage trait or pluggable durable backend;
- replication, KRaft, controller listeners, or multi-broker behavior;
- authentication or configurable authorization.

The catalog module remains the replacement seam for a future durable implementation. A generic storage abstraction is deferred until a second implementation creates a concrete need for one.

## Version policy

MemKafka continues to target the pinned current-client floor rather than historical Kafka releases.

| API | Before | After | Reason |
| --- | --- | --- | --- |
| Metadata | 4-9 | 4-13 | Preserve every current client while adding flexible topic-ID behavior through Kafka 4.3's latest v13. |
| CreateTopics | 4-6 | 4-7 | Preserve the current floor and return the created topic UUID in the latest response. |
| DescribeCluster | missing | 2 | Advertise only Kafka 4.3's latest version. |
| DescribeTopicPartitions | missing | 0 | Kafka 4.3 defines only v0. |

Versions below existing floors remain unsupported. `DescribeCluster` v0-v1 receive typed `UNSUPPORTED_VERSION` responses through the existing request-router path. `DescribeTopicPartitions` has no schema-known adjacent version because v0 is its complete Kafka 4.3 range.

## Topic identity and catalog state

Add a direct `uuid` dependency with v4 generation enabled. `TopicMetadata` gains `id: Uuid` alongside its name and partition count.

`TopicCatalog` owns one `RwLock<CatalogState>`. `CatalogState` contains:

- an ordered `BTreeMap<String, TopicEntry>` for deterministic name iteration;
- a `HashMap<Uuid, String>` reverse index for first-class UUID lookup.

Both indexes are private. Protocol handlers use catalog methods and never access maps directly.

Explicit and automatic creation follow the same atomic sequence while holding the catalog write lock:

1. validate the topic definition;
2. recheck that the name is vacant;
3. generate random v4 UUIDs until one is absent from the reverse index;
4. create the partition logs;
5. insert the topic entry and reverse index before releasing the lock;
6. return the stored metadata.

Concurrent auto-creation of one name therefore returns one topic incarnation and one UUID to every caller. A UUID is never allocated by validation-only `CreateTopics`; its successful v7 response uses the nil UUID because no topic was created.

Catalog reads return cloned metadata or partition-log handles. Response encoding does not hold the catalog lock. The catalog exposes name lookup, UUID lookup, and deterministic listing as explicit operations.

## Shared discovery invariants

Every discovery handler uses these fixed single-broker facts:

- cluster ID: `memkafka`;
- broker ID: `1` by default, or the configured `BrokerState` ID;
- controller ID: the same broker ID;
- leader ID and only replica/ISR: the same broker ID;
- leader epoch: `0`;
- rack: null;
- broker fenced: false;
- internal topic: false;
- offline replicas: empty.

Network identity remains per connection. `Metadata` and `DescribeCluster` receive the `AdvertisedAddress` owned by that connection's `Dispatcher`, so each listener advertises the correct host and port.

MemKafka has no authorizer, matching Kafka's allow-all behavior when no authorizer is configured. Authorization bitfields use Kafka 4.3.1 `AclOperation` codes:

- topic operations `READ`, `WRITE`, `CREATE`, `DELETE`, `ALTER`, `DESCRIBE`, `DESCRIBE_CONFIGS`, and `ALTER_CONFIGS`: `3576`;
- cluster operations `CREATE`, `ALTER`, `DESCRIBE`, `CLUSTER_ACTION`, `DESCRIBE_CONFIGS`, `ALTER_CONFIGS`, and `IDEMPOTENT_WRITE`: `8096`.

An optional authorized-operations field remains the schema default `i32::MIN` unless the request asks for it. `DescribeTopicPartitions` has no include flag, so successful topic entries always carry the topic bitfield, matching Kafka 4.3.1.

## Metadata v4-v13

Existing name-based behavior remains unchanged for v4-v9. Successful Metadata responses at v10-v13 include the catalog UUID.

Kafka's version-specific request rules apply:

- v10-v11 do not support UUID requests. A null name or non-nil request UUID produces `INVALID_REQUEST` for the requested entries.
- v12-v13 support UUID requests.
- if any v12-v13 request entry contains a non-nil UUID, the complete request uses UUID mode, matching Kafka. Name-only entries in that mixed request are not independently resolved.
- otherwise the request uses name mode.
- a null topic collection lists all topics; an empty collection requests no topics for every supported version.

Name mode:

- valid existing names return full metadata and the stored UUID where the response version supports it;
- missing names auto-create only when broker configuration and the request both allow it;
- missing names with creation disabled return `UNKNOWN_TOPIC_OR_PARTITION` and a nil UUID;
- invalid names return `INVALID_TOPIC_EXCEPTION` and a nil UUID.

UUID mode:

- known UUIDs resolve through the reverse index and return the canonical name and stored UUID;
- unknown UUIDs return `UNKNOWN_TOPIC_ID`, preserve the requested UUID, use a null name in v12-v13, and return no partitions;
- UUID requests never auto-create topics.

Topic and cluster authorized-operation fields use the shared constants only when the applicable request flag is true. Metadata v13's top-level error is zero for a normally processed request. Semantic request failures use the Kafka error-response shape: each requested entry receives `INVALID_REQUEST`, and v13 also carries the top-level error.

## CreateTopics v4-v7

Existing validation, replication-factor, manual-assignment, custom-config, duplicate, and `validate_only` behavior remains unchanged.

For v7:

- successful real creation returns the newly stored UUID;
- `validate_only` returns a nil UUID and does not allocate or reserve identity;
- every failed result returns a nil UUID.

The handler continues to process topics independently and preserves per-topic errors.

## DescribeCluster v2

`DescribeCluster` v2 is a flexible request/response and is handled on the Kafka broker listener.

For endpoint type `1` (brokers), return:

- no error and no error message;
- response endpoint type `1`;
- cluster ID `memkafka`;
- controller ID equal to the broker ID;
- exactly one broker with the connection-specific advertised address, null rack, and `is_fenced=false`;
- cluster authorized operations `8096` when requested, otherwise `i32::MIN`.

Endpoint type `2` (controllers) returns `MISMATCHED_ENDPOINT_TYPE`, because MemKafka exposes a broker endpoint and does not pretend to expose a KRaft controller listener. Any other endpoint type returns `UNSUPPORTED_ENDPOINT_TYPE`. Error responses do not fabricate brokers.

## DescribeTopicPartitions v0

The handler matches Apache Kafka 4.3.1's ordering, limiting, and cursor rules.

### Topic selection

- An empty request topic list means all catalog topics.
- An explicit list is de-duplicated.
- Selected names are sorted lexicographically before processing.
- With a cursor, names lexicographically before the cursor topic are skipped.
- For an explicit topic list, the cursor topic must appear in that list; otherwise every requested topic receives `INVALID_REQUEST`.
- A negative cursor partition index is invalid and produces the same error response.

### Partition limit and cursor

MemKafka has no separate server-side maximum, so the effective limit is `max(response_partition_limit, 1)`.

The limit counts successful partition entries, not topic envelopes or unknown-topic errors. For each sorted topic:

1. start at the cursor partition for the cursor topic, otherwise partition `0`;
2. append at most the remaining number of partitions;
3. if that topic has more partitions, set `next_cursor` to its first omitted partition and stop;
4. if the topic was completed exactly at the limit and another topic remains, set `next_cursor` to partition `0` of that next topic and stop;
5. otherwise continue until every selected topic is complete and return no cursor.

A start partition at or beyond a real topic's partition count yields a successful topic with no partitions and continues. This mirrors Kafka 4.3.1.

### Topic results

Successful topics return the stored UUID, false internal status, the topic authorization bitfield, and partitions with broker `1` as leader/replica/ISR, epoch `0`, null eligible-leader and last-known-ELR lists, and no offline replicas.

For an explicit request:

- a valid missing name returns `UNKNOWN_TOPIC_OR_PARTITION`, nil UUID, and no partitions;
- an invalid name returns `INVALID_TOPIC_EXCEPTION`, nil UUID, and no partitions.

An all-topics request omits missing topics by construction. DescribeTopicPartitions never auto-creates topics.

## Routing and error responses

Add both new API keys to the capability registry, dispatcher coverage, and typed error-response coverage declarations. The dispatcher passes the request version to version-sensitive handlers and the connection's advertised address to `DescribeCluster`.

The central capability registry remains the source for `ApiVersions`, request routing, supported-boundary tests, and the checked-in capability manifest. The existing response-aware unsupported-version path remains responsible for adjacent schema-known versions; handlers do not duplicate version gates.

Well-formed invalid resources and cursors receive Kafka responses. Malformed frames, unknown API keys, and versions outside the generated Kafka 4.3 schema retain the existing connection-local failure policy.

## Verification

### Catalog tests

Prove:

- created UUIDs are non-zero, stable, unique, and available through both indexes;
- validation-only requests allocate no identity and mutate no state;
- concurrent auto-creation returns the same UUID to every caller;
- failed and duplicate creation preserve both indexes and existing metadata.

### Focused handler and wire tests

Prove:

- Metadata v10-v13 response IDs and v12-v13 UUID lookup/error semantics;
- v10-v11 reject UUID requests without mutation;
- CreateTopics v7 returns the exact catalog UUID and validate-only returns nil;
- DescribeCluster v2 success, endpoint errors, authorization flag, and per-listener address selection;
- DescribeTopicPartitions exact within-topic and between-topic cursors, all-topic ordering, limit clamping, invalid cursors, missing names, invalid names, and stable UUIDs;
- capability ranges, routing variants, typed unsupported responses, and generated manifest stay synchronized.

### Java 4.3.1 black-box acceptance

Extend the existing Java suite to:

1. call `Admin.describeCluster()` and assert cluster `memkafka`, broker/controller identity, and reachable endpoint;
2. create several topics and capture the v7 create result UUID;
3. list and describe them and assert every API reports the same non-zero UUID;
4. use `DescribeTopicsOptions.partitionSizeLimitPerResponse(2)` with enough partitions and topics to force multiple `DescribeTopicPartitions` requests;
5. assert the final descriptions contain every partition exactly once and in order.

The pinned request-version capture against Kafka 4.3.1 must then record `DescribeCluster` v2 and `DescribeTopicPartitions` v0 for the Java scenario. The checked-in capability manifest must advertise all 19 implemented keys with the new version windows.

### Full regression gate

Before handoff, run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

Then run the complete hosted workflow, which also covers the native and container .NET suites, Java, Go, Rust, protocol compatibility, benchmark smoke, and Kafbat UI.

## Documentation

Update the README compatibility claims, current advertised API windows, and Java coverage. Update the Kafka API parity roadmap inventory and mark Cut 3 delivered only after its real-client acceptance and capability evidence pass.

## References

- Vendored Kafka 4.3.1 schemas under `crates/kafka-protocol/protocol_codegen/schema/kafka-4.3.1/message/`
- [Kafka 4.3.1 Metadata request handling](https://github.com/apache/kafka/blob/4.3.1/core/src/main/scala/kafka/server/KafkaApis.scala)
- [Kafka 4.3.1 DescribeTopicPartitions handler](https://github.com/apache/kafka/blob/4.3.1/core/src/main/java/kafka/server/handlers/DescribeTopicPartitionsRequestHandler.java)
- [Kafka 4.3.1 metadata pagination](https://github.com/apache/kafka/blob/4.3.1/metadata/src/main/java/org/apache/kafka/metadata/KRaftMetadataCache.java)
- [Kafka 4.3.1 authorization operation sets](https://github.com/apache/kafka/blob/4.3.1/server/src/main/java/org/apache/kafka/security/authorizer/AclEntry.java)
