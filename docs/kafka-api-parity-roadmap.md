# Kafka API parity roadmap

> **Snapshot:** 2026-08-29, targeting Apache Kafka 4.3. This is a living compatibility map. Kafka will evolve, and reaching 77/77 API keys would still not prove behavioral parity.

## Executive recommendation

Pursue near-1:1 **external Kafka API and client-behavior parity** while preserving MemKafka's one-process, one-virtual-broker, in-memory design.

The practical order is:

1. make current APIs version-aware and behaviorally exact;
2. unlock daily `Admin`/AdminClient and inspection workflows;
3. add transactions and exactly-once behavior;
4. add modern consumer groups;
5. add security, quotas, and telemetry;
6. add share and Streams groups;
7. emulate the remaining operator/controller surface deterministically.

Do not build a KRaft cluster, replica manager, disk log, or multi-process architecture merely to resemble Kafka internally. When one deterministic in-memory answer is externally equivalent for a client, that is the right implementation.

### Snapshot scorecard

| Measure | 2026-08-28 result |
| --- | ---: |
| Stable API keys in the [Kafka 4.3 protocol table](https://kafka.apache.org/43/design/protocol#api-keys) | 77 |
| API keys advertised by MemKafka | 17 |
| Advertised keys with a material version or semantic gap | 17 |
| Stable keys not advertised | 60 |

The raw 17/77 count is useful as an inventory check, but not as a compatibility score:

- one key can contain many independently negotiated versions and behaviors;
- Produce, Fetch, Metadata, and group coordination matter to ordinary clients far more than most controller APIs;
- several missing keys exist primarily for brokers, controllers, or operators;
- a response that decodes and says “success” can still be observably wrong;
- cross-API invariants—such as topic IDs, transaction visibility, epochs, and committed offsets—matter more than isolated happy paths.

The target is therefore **client-visible behavior**, measured with unmodified clients and Apache Kafka 4.3 as a differential oracle, not the fastest route to 77 green rows.

### Quick navigation

- [What parity means](#what-parity-means)
- [Protocol version support policy](#protocol-version-support-policy)
- [Current implementation inventory](#current-implementation-inventory)
- [Complete Kafka 4.3 stable API-key matrix](#complete-kafka-43-stable-api-key-matrix)
- [Priority roadmap](#priority-roadmap)
- [Recommended first implementation sequence](#recommended-first-implementation-sequence)
- [Testing and compatibility strategy](#testing-and-compatibility-strategy)
- [Success criteria and guardrails](#success-criteria-and-guardrails)
- [Related Schema Registry gap](#related-schema-registry-gap)

## What parity means

Parity is an externally observable contract. For every version MemKafka advertises, it includes:

- the correct request and response shape, header version, flexible encoding, tagged-field behavior, and nullability;
- Kafka-compatible errors for invalid state, invalid resources, unsupported versions, and fencing;
- correct state transitions, including timeouts and concurrent requests;
- cross-API invariants and side effects—for example, one topic UUID everywhere, deletion invalidating old IDs, transaction markers changing `read_committed` visibility, and group deletion removing offsets;
- black-box coverage through an unmodified real client;
- normalized differential agreement with a pinned Apache Kafka 4.3 broker.

Differential comparisons should normalize only intentional single-broker differences: addresses, broker/controller IDs, generated cluster/topic/member IDs, throttle timing, wall-clock timestamps, and explicitly documented synthetic operator fields. Error codes, state changes, returned resources, offsets, epochs, assignments, and subsequent observable effects are not normalization candidates.

Parity explicitly does **not** include:

- Kafka configuration-file parity;
- a multi-process broker architecture;
- a replication or ISR implementation;
- KRaft internals or internal topics;
- Kafka's disk layout, segments, or log directories;
- production durability, availability, or fault-tolerance claims.

A plausible no-op is not parity. If `DeleteTopics` returns success but Metadata still exposes the topic, if `EndTxn` succeeds but `read_committed` sees aborted records, or if an alter operation reports success without an observable state change, a client can detect the mismatch. MemKafka must implement the semantic effect or return a clear Kafka error.

The [Kafka 4.3 protocol](https://kafka.apache.org/43/design/protocol) defines wire shapes and version ranges. The [Kafka 4.3 Admin API](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/admin/Admin.html) is a useful client-visible map of management and inspection workflows.

## Protocol version support policy

MemKafka targets one current Kafka baseline, presently Apache Kafka 4.3. It does not preserve older wire versions merely for legacy Kafka releases or clients.

Each API has two distinct ranges:

- the **current supported window** is the contiguous range implemented and advertised by MemKafka today;
- the **Kafka 4.3 target window** starts at the lowest version observed from the pinned current-client matrix and extends through that API's latest stable Kafka 4.3 request version;
- MemKafka advertises only implemented versions, with no gaps;
- versions below the evidence-backed floor are rejected and are not compatibility targets;
- a floor may move upward when all pinned clients move forward, but it does not move downward to admit an older client.

The current client floor is Confluent.Kafka 2.15.0, the separate Confluent.Kafka 2.13.2 flow profile, Apache Kafka Java 4.3.1, rskafka 0.6.0, franz-go 1.21.6, and Kafbat UI 1.5.0. These are scenario-specific pins, not promises that every feature in each client is supported. Adding or replacing a client requires recording the API versions it negotiates in its black-box scenarios. A client update that asks for a lower version than the recorded floor fails compatibility review instead of silently expanding the support window downward.

Before a new API is advertised, its named current-client or tool scenario runs against the pinned Kafka broker to establish the floor. An API without such evidence remains unadvertised. Existing ranges are implementation inventory, not permanent compatibility promises. The delivered capability-registry cut records actual negotiation and narrows unnecessarily low floors without advertising the still-unimplemented Kafka 4.3 target ceilings.

Kafka 4.3's complete version ranges remain in the matrix as protocol reference data. Versions below MemKafka's evidence-backed floor do not prevent an API from reaching `implemented`; missing the Kafka 4.3 target ceiling or any version between the evidence floor and that ceiling does.

## Current implementation inventory

The source of truth for advertised versions is the central runtime capability registry, published as the generated [`kafka-api-capabilities.json`](compatibility/kafka-api-capabilities.json) manifest. The dispatcher, handlers, broker state, focused tests, and black-box suites establish what those versions do.

“Proven” below distinguishes real-client evidence from focused wire tests. A wire test is valuable, but it does not independently justify a public compatibility claim.

| Key | API | MemKafka | Kafka 4.3 | Current proven behavior | Important gaps |
| ---: | --- | --- | --- | --- | --- |
| 0 | Produce | 7 | 3-13 | Real Java, .NET, Go, and Rust clients append acknowledged ordered records; the flow-profile .NET client proves non-transactional idempotent production. | No transactions/control batches, broker epoch validation, newer flexible versions, or complete modern error surface. |
| 1 | Fetch | 4 | 4-18 | Real clients fetch ordered batches, seek, repeat uncommitted reads, and use earliest/latest behavior; wire tests cover long-polling and byte limits. | No fetch sessions, topic IDs, current/last-fetched leader epochs, diverging epochs, preferred replicas, or isolation semantics. |
| 2 | ListOffsets | 3 | 1-11 | Real clients prove earliest/latest and seeking; wire tests prove unknown-partition errors. | Only timestamps `-2` and `-1`; timestamp lookup is rejected, leader epoch is always `-1`, and newer flexible/topic-ID schemas are absent. |
| 3 | Metadata | 4-9 | 0-13 | Real clients prove discovery, advertised address, auto-creation, explicit partition counts, and all-topic listing paths. | No topic IDs, newer flexible versions, rack data, listener/security semantics, or realistic cluster changes. |
| 8 | OffsetCommit | 7 | 2-10 | Real .NET consumers prove automatic/manual commit resume, redelivery without commit, and independent group offsets. | Static membership is rejected; retention timestamp, leader epoch, topic IDs, and newer versions are not implemented. |
| 9 | OffsetFetch | 5 | 1-10 | Real .NET consumers prove committed-offset recovery and group isolation. | No multi-group request, topic IDs, member epoch, unstable transactional offsets, or full unknown-group behavior. |
| 10 | FindCoordinator | 2 | 0-6 | Real group consumers discover the single coordinator. | Group coordinators only; transaction/share coordinator types and batched coordinator responses are absent. |
| 11 | JoinGroup | 5 | 0-9 | Real .NET clients prove member-ID handshake, cooperative-sticky negotiation, A/B/C joins, and rebalance rounds. | Static membership is rejected; no reason field, skip-assignment behavior, newer schemas, or modern group protocol. |
| 12 | Heartbeat | 3 | 0-4 | Real multi-member lifecycle proves heartbeats preserve active membership and silent members expire. | Static membership is rejected and v4 is absent. |
| 13 | LeaveGroup | 1-3 | 0-5 | Real clients prove graceful leave and redistribution. | Static-member leave is rejected; reason fields and newer response semantics are absent. |
| 14 | SyncGroup | 3 | 0-5 | Real cooperative-sticky clients prove leader-supplied opaque assignments and successive stable generations. | Static membership is rejected; protocol type/name response fields and newer schemas are absent. |
| 15 | DescribeGroups | 0 | 0-6 | Kafbat black-box coverage proves an active group is discoverable; wire tests assert state, metadata, assignments, ordering, and unknown groups. | No authorized operations, newer group fields/types, or version coverage beyond v0. |
| 16 | ListGroups | 0 | 0-5 | Kafbat black-box coverage proves group listing; wire tests assert group IDs and protocol types. | No state/type filters, group states, or newer versions. |
| 18 | ApiVersions | 3-4 | 0-4 | All pinned clients negotiate successfully; wire tests cover v3 and connection reuse. | Unsupported-new-version fallback remains incomplete; advertised ranges and dispatch gates now share the central registry. |
| 19 | CreateTopics | 4-6 | 2-7 | Real Admin clients create topics, observe partition counts, and receive `INVALID_REPLICATION_FACTOR`; wire tests cover validation-only and errors. | Custom configs and manual replica assignments are rejected; v7 and topic-ID lifecycle are absent. |
| 22 | InitProducerId | 0 | 0-6 | A real idempotent .NET producer obtains an ID and publishes; wire tests prove distinct IDs and transactional-ID rejection. | Non-transactional allocation only, epoch always `0`; no transactional IDs, fencing/recovery, or newer versions. |
| 32 | DescribeConfigs | 1 | 1-4 | The Kafbat black-box path negotiates the read-only API; wire tests cover known and unknown topic/broker resources. | Successful resources return empty config lists; no synonyms, documentation, config source/sensitivity, or non-empty values. |

### Current architectural strengths to preserve

- immutable raw RecordBatch storage preserves client serialization, compression, keys, values, headers, and timestamps;
- partition-local locking gives deterministic offsets and retry-safe idempotent sequence tracking;
- classic group coordination is a real state machine, not a single-consumer shortcut;
- the single broker is consistently broker, controller, leader, and replica;
- unsupported transactions, static membership, custom topic configs, and manual assignments fail instead of silently succeeding.

### Current structural risks

- the registry now aligns `ApiVersions` and dispatch version gates, but handler semantics remain uneven across each advertised window;
- handlers are generally written to one shared generated request type rather than making version-specific semantics explicit;
- decode happens before dispatch version rejection, so unsupported schemas can become connection failures rather than the closest Kafka response;
- topic identity is name-only, which blocks modern Metadata, Fetch, offsets, deletion/recreation safety, and KIP-848;
- a pinned Kafka 4.3.1 request-capture oracle and machine-readable manifests now exist, but normalized semantic differentials remain future work.

## Complete Kafka 4.3 stable API-key matrix

This matrix contains all 77 entries in the stable Api Keys table from the [official Kafka 4.3 protocol snapshot](https://kafka.apache.org/43/design/protocol#api-keys).

Status means:

- `implemented`: the evidence-backed client floor through the Kafka 4.3 ceiling and the target behavior are covered;
- `partial`: advertised today, but version or semantic parity is incomplete;
- `missing`: not advertised or dispatched.

No API currently meets the strict `implemented` definition; the 17 advertised keys are `partial` and the other 60 are `missing`.

`kafka-protocol = 0.18.0` is confirmed in `Cargo.toml`. Its installed generated sources omit Streams keys 88 and 89 entirely (`†`). Concrete top-level request/response `Message::VERSIONS` do not reach Kafka 4.3's latest stable version for ListOffsets, OffsetCommit, OffsetFetch, InitProducerId, WriteTxnMarkers, DescribeLogDirs, ShareFetch, ShareAcknowledge, AddRaftVoter, WriteShareGroupState, ReadShareGroupStateSummary, and DescribeShareGroupOffsets (`‡`). For OffsetCommit, OffsetFetch, and InitProducerId, only the request codec lags: it stops at v9, v9, and v5 respectively while each response codec reaches the official v10, v10, and v6 maximum. Those rows require a generated-protocol dependency update, a maintained fork, or an upstream contribution before the Kafka 4.3 ceiling can be decoded safely.

### Core data plane and discovery

| Key | Name | Kafka 4.3 request range | Status | Priority | MemKafka target semantics / rationale |
| ---: | --- | --- | --- | --- | --- |
| 0 | Produce | 3-13 | partial | P0 | Preserve raw batches; add versioned errors, epochs, flexible forms, transactions, and exact side effects. |
| 1 | Fetch | 4-18 | partial | P0 | Add sessions, topic IDs, leader epochs, tier/isolation fields, and correct incremental behavior. |
| 2 | ListOffsets | 1-11 | partial | P0 | Resolve earliest/latest and record timestamps with correct leader epochs and errors. Codec max gap. ‡ |
| 3 | Metadata | 0-13 | partial | P0 | Add stable topic UUIDs, flexible versions, authorized operations, and consistent single-broker metadata. |
| 18 | ApiVersions | 0-4 | partial | P0 | Generate from one capability registry and implement Kafka's unsupported-version negotiation path. |
| 23 | OffsetForLeaderEpoch | 2-4 | missing | P0 | Return deterministic partition end offsets for validated current/previous in-memory leader epochs. |
| 75 | DescribeTopicPartitions | 0 | missing | P1 | Paginated, topic-ID-aware discovery for modern Admin clients. |

### Classic groups and offsets

| Key | Name | Kafka 4.3 request range | Status | Priority | MemKafka target semantics / rationale |
| ---: | --- | --- | --- | --- | --- |
| 8 | OffsetCommit | 2-10 | partial | P0 | Complete current versions, static membership, leader epochs, topic IDs, and transactional fencing. Request codec stops at v9; v10 requires a generated-codec update. ‡ |
| 9 | OffsetFetch | 1-10 | partial | P0 | Add multi-group/topic-ID forms and return correct group/transaction state. Request codec stops at v9; v10 requires a generated-codec update. ‡ |
| 10 | FindCoordinator | 0-6 | partial | P0 | Resolve group, transaction, share, and Streams coordinator types to broker 1, including batched forms. |
| 11 | JoinGroup | 0-9 | partial | P0 | Finish classic versions, static membership, reasons, skip-assignment semantics, and fencing. |
| 12 | Heartbeat | 0-4 | partial | P0 | Finish static-member fencing and v4 behavior. |
| 13 | LeaveGroup | 0-5 | partial | P0 | Finish batched/static member removal, reasons, and per-member errors. |
| 14 | SyncGroup | 0-5 | partial | P0 | Finish static membership and protocol type/name semantics across versions. |
| 15 | DescribeGroups | 0-6 | partial | P0 | Return complete current group state, authorized operations, and modern fields. |
| 16 | ListGroups | 0-5 | partial | P0 | Add state/type filters and complete listed group metadata. |
| 42 | DeleteGroups | 0-2 | missing | P1 | Delete only empty/inactive groups and their in-memory metadata with Kafka-compatible per-group errors. |
| 47 | OffsetDelete | 0 | missing | P1 | Delete selected committed offsets while enforcing active-subscription constraints. |

### Topic, config, and admin lifecycle

| Key | Name | Kafka 4.3 request range | Status | Priority | MemKafka target semantics / rationale |
| ---: | --- | --- | --- | --- | --- |
| 19 | CreateTopics | 2-7 | partial | P0 | Finish v7/topic IDs; keep replication factor 1 and reject unsupported assignments/configs precisely until modeled. |
| 20 | DeleteTopics | 1-6 | missing | P1 | Delete by name or ID atomically; invalidate old IDs, logs, and metadata while handling group references. |
| 21 | DeleteRecords | 0-2 | missing | P1 | Advance an in-memory log-start offset and make Fetch/ListOffsets enforce the new boundary. |
| 32 | DescribeConfigs | 1-4 | partial | P1 | Return truthful broker/topic effective configs, sources, synonyms, sensitivity, and requested-key filtering. |
| 33 | AlterConfigs | 0-2 | missing | P1 | Compatibility wrapper for supported mutable settings; reject unsupported keys, never silently accept them. |
| 37 | CreatePartitions | 0-3 | missing | P1 | Grow partition vectors atomically, preserving all existing logs and rejecting manual assignments. |
| 44 | IncrementalAlterConfigs | 0-1 | missing | P1 | Mutate only the small supported in-memory config set with validate-only and atomic error semantics. |
| 60 | DescribeCluster | 0-2 | missing | P1 | Report cluster `memkafka`, broker/controller 1, endpoint, and authorized operations consistently. |
| 74 | ListConfigResources | 0-1 | missing | P1 | Enumerate only resources whose MemKafka config state can be described truthfully. |

### Transactions and producer introspection

| Key | Name | Kafka 4.3 request range | Status | Priority | MemKafka target semantics / rationale |
| ---: | --- | --- | --- | --- | --- |
| 22 | InitProducerId | 0-6 | partial | P2 | Add transactional-ID ownership, producer epochs, fencing, timeout, and recovery. Request codec stops at v5; v6 requires a generated-codec update. ‡ |
| 24 | AddPartitionsToTxn | 0-5 | missing | P2 | Enlist partitions atomically in the active transaction and validate producer identity/epoch. |
| 25 | AddOffsetsToTxn | 0-4 | missing | P2 | Enlist the target consumer group in a transaction. |
| 26 | EndTxn | 0-5 | missing | P2 | Commit or abort one active transaction, fence stale producers, and emit observable control batches. |
| 27 | WriteTxnMarkers | 1-2 | missing | P2 | Apply commit/abort markers deterministically to local partitions; keep the broker-facing API coherent. Codec max gap. ‡ |
| 28 | TxnOffsetCommit | 0-5 | missing | P2 | Stage offsets and publish them only on transaction commit with group/member fencing. |
| 61 | DescribeProducers | 0 | missing | P2 | Expose active producer IDs, epochs, sequences, and last timestamps per partition. |
| 65 | DescribeTransactions | 0 | missing | P2 | Return transactional state, producer identity, timeout, and enlisted partitions/groups. |
| 66 | ListTransactions | 0-2 | missing | P2 | Filter and list in-memory transactions by producer ID/state. |

### Security, identity, and quotas

| Key | Name | Kafka 4.3 request range | Status | Priority | MemKafka target semantics / rationale |
| ---: | --- | --- | --- | --- | --- |
| 17 | SaslHandshake | 0-1 | missing | P4 | Negotiate an explicitly enabled small mechanism set and enforce the connection authentication sequence. |
| 29 | DescribeAcls | 1-3 | missing | P4 | Filter an in-memory ACL store using Kafka resource/pattern semantics. |
| 30 | CreateAcls | 1-3 | missing | P4 | Atomically create validated ACL bindings and report per-binding errors. |
| 31 | DeleteAcls | 1-3 | missing | P4 | Match/delete ACL bindings and return the bindings actually removed. |
| 36 | SaslAuthenticate | 0-2 | missing | P4 | Authenticate configured test identities and bind a principal to the connection. |
| 38 | CreateDelegationToken | 1-3 | missing | P4 | Issue process-local, expiring test tokens only when the native security surface enables them. |
| 39 | RenewDelegationToken | 1-2 | missing | P4 | Extend a valid token with owner/renewer authorization checks. |
| 40 | ExpireDelegationToken | 1-2 | missing | P4 | Expire a valid token immediately or at the requested time. |
| 41 | DescribeDelegationToken | 1-3 | missing | P4 | Filter and report active process-local tokens without leaking secrets. |
| 48 | DescribeClientQuotas | 0-1 | missing | P4 | Query a small in-memory entity quota store with exact filter semantics. |
| 49 | AlterClientQuotas | 0-1 | missing | P4 | Validate-only or atomically mutate supported quota keys; reject unsupported dimensions. |
| 50 | DescribeUserScramCredentials | 0 | missing | P4 | Report configured test users and SCRAM mechanism metadata, never stored credentials. |
| 51 | AlterUserScramCredentials | 0 | missing | P4 | Upsert/delete process-local SCRAM credentials with iteration and duplicate validation. |

### Modern consumer groups

| Key | Name | Kafka 4.3 request range | Status | Priority | MemKafka target semantics / rationale |
| ---: | --- | --- | --- | --- | --- |
| 68 | ConsumerGroupHeartbeat | 0-1 | missing | P3 | Implement the KIP-848 member/group epoch state machine, topic-ID subscriptions, and server-side assignors. |
| 69 | ConsumerGroupDescribe | 0-1 | missing | P3 | Expose KIP-848 group/member epochs, subscriptions, target/current assignments, and states. |

The [KIP-848 protocol](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/217387038/KIP-848%2BThe%2BNext%2BGeneration%2Bof%2Bthe%2BConsumer%2BRebalance%2BProtocol) moves assignment and incremental reconciliation into the coordinator. It is not a thin alias for JoinGroup/SyncGroup.

### Telemetry

| Key | Name | Kafka 4.3 request range | Status | Priority | MemKafka target semantics / rationale |
| ---: | --- | --- | --- | --- | --- |
| 71 | GetTelemetrySubscriptions | 0 | missing | P4 | Allocate stable process-local client instance IDs and return native subscription rules. |
| 72 | PushTelemetry | 0 | missing | P4 | Validate subscription IDs, intervals, size, compression, and accept or expose payloads to a test hook. |

The target should follow [KIP-714](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/173085915/KIP-714%2BClient%2Bmetrics%2Band%2Bobservability) without reproducing Kafka's plugin/configuration system.

### Share and Streams groups

| Key | Name | Kafka 4.3 request range | Status | Priority | MemKafka target semantics / rationale |
| ---: | --- | --- | --- | --- | --- |
| 76 | ShareGroupHeartbeat | 1 | missing | P5 | Maintain share membership, subscriptions, member epochs, and broker-side assignments. |
| 77 | ShareGroupDescribe | 1 | missing | P5 | Report share group state, members, subscriptions, and assignments. |
| 78 | ShareFetch | 1-2 | missing | P5 | Acquire records with per-group locks, delivery counts, sessions, and isolation behavior. Codec max gap. ‡ |
| 79 | ShareAcknowledge | 1-2 | missing | P5 | Accept/release/reject acquired records exactly once per acquisition epoch. Codec max gap. ‡ |
| 83 | InitializeShareGroupState | 0 | missing | P5 | Initialize partition share-state epochs and start offsets idempotently. |
| 84 | ReadShareGroupState | 0 | missing | P5 | Return acquisition state and delivery counts for requested partitions. |
| 85 | WriteShareGroupState | 0-1 | missing | P5 | Apply epoch-fenced share state updates atomically. Codec max gap. ‡ |
| 86 | DeleteShareGroupState | 0 | missing | P5 | Delete requested share partition state with per-partition errors. |
| 87 | ReadShareGroupStateSummary | 0-1 | missing | P5 | Return state epoch/start-offset summaries without acquisition payloads. Codec max gap. ‡ |
| 88 | StreamsGroupHeartbeat | 0 | missing | P5 | Implement Streams topology/member epochs and active/standby/warm-up task assignment. Generated types unavailable. † |
| 89 | StreamsGroupDescribe | 0 | missing | P5 | Report topology, group state, members, and task assignments. Generated types unavailable. † |
| 90 | DescribeShareGroupOffsets | 0-1 | missing | P5 | Return per-topic share start offsets with topic-ID validation. Codec max gap. ‡ |
| 91 | AlterShareGroupOffsets | 0 | missing | P5 | Reset share start offsets only when group state permits it. |
| 92 | DeleteShareGroupOffsets | 0 | missing | P5 | Remove share offsets/state with active-group fencing. |

[KIP-932](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/255070434/KIP-932%2BQueues%2Bfor%2BKafka) defines record acquisition and acknowledgement, not ordinary consumer commits. [KIP-1071](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/311627992/KIP-1071%2BStreams%2BRebalance%2BProtocol) makes topology and task assignment broker-visible. Both need dedicated state machines and real client scenarios.

### Cluster, controller, and operator surface

| Key | Name | Kafka 4.3 request range | Status | Priority | MemKafka target semantics / rationale |
| ---: | --- | --- | --- | --- | --- |
| 34 | AlterReplicaLogDirs | 1-2 | missing | P6 | Accept only the broker's synthetic in-memory directory; reject nonexistent brokers/dirs precisely. |
| 35 | DescribeLogDirs | 1-5 | missing | P6 | Report one stable synthetic `mem://` directory, its partitions, offsets, and in-memory sizes. Codec max gap. ‡ |
| 43 | ElectLeaders | 0-2 | missing | P6 | Validate topics/partitions and report election-not-needed for the sole eligible leader without fake topology changes. |
| 45 | AlterPartitionReassignments | 0-1 | missing | P6 | Treat replica set `[1]` as an idempotent completed assignment; reject every impossible replica set. |
| 46 | ListPartitionReassignments | 0 | missing | P6 | Return no active reassignments after validated single-broker operations. |
| 55 | DescribeQuorum | 0-2 | missing | P6 | Return a deterministic one-node quorum view with coherent leader/voter IDs and epochs. |
| 57 | UpdateFeatures | 0-2 | missing | P6 | Validate and expose a small feature-level store used by emulated APIs, including validate-only behavior. |
| 64 | UnregisterBroker | 0 | missing | P6 | Reject removal of broker 1 and identify unknown brokers; never report destructive success with no effect. |
| 80 | AddRaftVoter | 0-1 | missing | P6 | Validate voter identity/endpoints against the fixed one-node quorum and return meaningful duplicate/invalid errors. Codec max gap. ‡ |
| 81 | RemoveRaftVoter | 0 | missing | P6 | Reject removal of the sole voter and report voter-not-found for unknown identities. |

These APIs do not require a real replica manager or KRaft implementation. They require coherent answers. A synthetic directory, fixed leader, completed `[1]` reassignment, and one-voter quorum are useful to inspection tools because every related API agrees. Unsupported mutations must return errors instead of ceremonial success.

## Priority roadmap

### P0 — Parity foundation and modernize existing APIs

**Ecosystem value:** makes current producer, consumer, and discovery support dependable across newer clients; prevents `ApiVersions`, decoding, dispatch, and docs from drifting.

**Foundation status:** cut 1 is delivered. CI now captures pinned-client request versions against Kafka 4.3.1, checks a generated compatibility manifest, and rejects drift from the central runtime capability registry. This is version evidence and consistency enforcement, not completion of the 17 advertised APIs; Kafka 4.3 ceilings and semantic gaps remain below.

**Delivered foundation:**

- capture the API key/version pairs used by every pinned black-box client scenario and check them into a machine-readable client-floor artifact;
- keep key, evidence-backed floor, current ceiling, Kafka 4.3 range, handler, maturity, and proof lanes in one runtime capability registry;
- generate or validate `ApiVersions`, dispatcher coverage, per-version test cases, and the checked-in compatibility artifact from it.

**Remaining state and semantic work:**

- introduce version-aware handler patterns and response/error builders;
- build the pinned Kafka 4.3 differential harness;
- update generated protocol support for every P0 schema before advertising it;
- add stable topic IDs and propagate them through Metadata, Fetch, ListOffsets, commits, and topic recreation;
- implement flexible versions/tagged fields as specified by [KIP-482](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/120722234/KIP-482%2BThe%2BKafka%2BProtocol%2Bshould%2BSupport%2BOptional%2BTagged%2BFields);
- implement Fetch sessions from [KIP-227](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=74687799), leader epochs, timestamp offsets, and complete current error paths;
- finish every classic-group version inside the supported client-floor-to-Kafka-4.3 window, including static membership.

**Acceptance:** floor/ceiling and rejected-below/rejected-above probes for every advertised key; CI proves each advertised floor exactly matches current-client evidence; normalized Kafka 4.3 differentials for success and error paths; existing Java/.NET/Go/Rust/Kafbat lanes stay green; static-member restart and incremental Fetch scenarios pass through real clients.

**Dependencies:** generated protocol upgrade/fork; Kafka 4.3 test image; capability artifact schema; stable topic-ID store.

### P1 — Daily AdminClient and tooling

**Ecosystem value:** unlocks routine setup, teardown, inspection, cleanup, and test isolation through the Kafka 4.3 `Admin` interface and tools such as Kafbat.

**State and semantic work:**

- DescribeCluster and paginated DescribeTopicPartitions;
- DeleteTopics with topic-ID invalidation and deterministic references cleanup;
- CreatePartitions with immutable existing partitions;
- DeleteRecords backed by a real in-memory log-start offset;
- truthful full DescribeConfigs plus IncrementalAlterConfigs and a narrow AlterConfigs wrapper;
- DeleteGroups and OffsetDelete with active-group fencing;
- meaningful ListConfigResources and single-broker inspection fields.

**Acceptance:** a real Java Admin client creates, grows, describes, configures, truncates, deletes, and recreates a topic; the recreated topic has a new ID; group offsets can be listed/deleted and an empty group can be deleted; Kafbat performs those supported operations without fallback or stale data.

**Dependencies:** P0 topic IDs/capability registry; log-start offsets; small typed config store; group-to-topic reference queries.

### P2 — Transactions and exactly-once

**Ecosystem value:** supports transactional producers, atomic consume-transform-produce tests, Kafka Streams' exactly-once mode, and transaction inspection.

**State and semantic work:**

- transactional ID → producer ID/epoch ownership, timeout, fencing, and recovery;
- transaction coordinator states and enlisted partitions/groups;
- AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, TxnOffsetCommit, and coherent WriteTxnMarkers;
- transactional/control batch acceptance and indexing;
- last stable offset, aborted transaction ranges, `read_committed` vs `read_uncommitted` Fetch;
- atomic publication of staged group offsets;
- DescribeProducers, DescribeTransactions, and ListTransactions.

**Acceptance:** real Java and librdkafka transactional producers commit and abort; an unmodified read-committed consumer hides aborted/uncommitted records; `sendOffsetsToTransaction` advances offsets only on commit; stale epochs are fenced; Admin transaction inspection agrees with observed records.

**Dependencies:** P0 leader/topic identity; control-batch codec; partition transaction index; transaction/group lock-order rules.

### P3 — Modern groups

**Ecosystem value:** supports clients using Kafka's current consumer protocol and reduces dependence on classic client-side rebalance barriers.

**State and semantic work:**

- first finish classic version coverage and static membership in P0;
- implement KIP-848 consumer group/member epochs, subscriptions by topic ID, target/current assignments, reconciliation, and session expiry;
- add deterministic broker-side range and uniform assignors before optional assignors;
- bridge offset commit/fetch semantics without conflating classic generations and modern member epochs;
- implement ConsumerGroupDescribe as a real view of the same state.

**Acceptance:** Java KafkaConsumer with `group.protocol=consumer` scales from one to three members and back without overlapping ownership; regex/topic recreation behavior uses IDs correctly; stale member epochs are fenced; classic and modern groups coexist and are both inspectable.

**Dependencies:** P0 topic IDs, current offset APIs, capability registry, and deterministic metadata change notifications.

### P4 — Security, quotas, and telemetry

**Ecosystem value:** lets integration tests exercise authentication/authorization failures, credential rotation, quota administration, and client metrics negotiation without external security infrastructure.

**State and semantic work:**

- connection authentication state and SASL handshake/authenticate sequencing;
- a small MemKafka-native setup surface for test users, SCRAM secrets, ACLs, tokens, quotas, and telemetry subscriptions;
- authorization checks shared by data, group, and admin handlers;
- delegation token lifetime/ownership and safe secret handling;
- entity quota filter/mutation semantics;
- KIP-714 client instance IDs, subscription epochs, push intervals, size limits, and an observable in-memory/test receiver.

Configuration-file parity remains out of scope. The setup surface should be intentionally small and native to MemKafka; the Kafka APIs remain externally compatible.

**Acceptance:** unmodified Java and librdkafka clients authenticate, receive authorization errors for denied operations, succeed after an ACL change, exercise SCRAM credential rotation, query/change quotas, and negotiate/push telemetry with valid and stale subscriptions.

**Dependencies:** principal on connection context; authorization hooks in handlers; secret-zeroization review; P1 config resource support for client metrics.

### P5 — Share groups and Streams groups

**Ecosystem value:** supports KafkaShareConsumer queue-like workflows and Kafka Streams' broker-driven rebalance protocol.

**State and semantic work:**

- share membership/member epochs and broker-side assignment;
- record acquisition locks, expiry, delivery counts, acknowledgement outcomes, and share sessions;
- share partition state epochs plus offset describe/alter/delete;
- Streams topology identity/epochs, subtopologies, internal topic requirements, and active/standby/warm-up task assignment;
- Streams group inspection;
- upgrade generated protocol support for keys 88/89 and newer share versions.

**Acceptance:** a real KafkaShareConsumer distributes records across members, redelivers released/expired records, never redelivers accepted records, and exposes and resets offsets; a real Kafka Streams topology using the Streams protocol scales members while preserving disjoint active tasks and coherent standby/warm-up roles.

**Dependencies:** P2 isolation/transactions for realistic Streams; P3 broker-side assignment patterns; generated types for 88/89; deterministic timers.

### P6 — Operator/controller surface

**Ecosystem value:** keeps broad Admin clients, diagnostics, and cluster explorers functional even when they inspect operations that are trivial in a one-broker system.

**State and semantic work:**

- one synthetic log directory with real in-memory sizes and offsets;
- idempotent single-replica reassignment and no-op election reporting with correct per-partition results;
- coherent one-leader/one-voter quorum and feature views;
- explicit errors for impossible directory, broker, voter, and replica mutations;
- UnregisterBroker protections and feature validation.

**Acceptance:** Java Admin inspection sees the same broker, leader, directory, replicas, quorum, and feature levels across calls; valid idempotent single-broker requests complete; impossible mutations return Kafka-compatible errors and leave every related view unchanged.

**Dependencies:** P0 identity/capability registry; P1 topic lifecycle and size accounting; a documented synthetic operator model.

## Recommended first implementation sequence

Each cut should be specified and reviewed independently. No dates or effort estimates are implied.

| Cut | Vertical slice | Real-client or tool acceptance scenario unlocked |
| ---: | --- | --- |
| 1 (delivered) | Capture negotiated versions from every pinned current-client scenario against Kafka 4.3.1, and create one central runtime capability registry that validates `ApiVersions`, dispatch, and the generated compatibility manifest. | CI records the client-backed floor and fails on advertisement, manifest, or request-evidence drift. This proves version demand, not semantic parity. |
| 2 | Upgrade/fork generated protocol schemas and make unsupported key/version handling response-aware, including flexible headers and ApiVersions fallback. | Pinned clients negotiate within the floor-to-ceiling window; requests immediately below and above it receive Kafka-compatible rejection rather than a dropped connection. |
| 3 | Introduce stable topic IDs and implement modern Metadata, DescribeCluster, and DescribeTopicPartitions together. | Java `Admin.describeCluster()` and paginated `describeTopics()` agree on one broker; delete/recreate preparation can distinguish topic incarnations. |
| 4 | Add Fetch session state, incremental updates/forgotten topics, and session errors. | A Java or librdkafka consumer sustains an incremental Fetch session across partition additions and recovers from an invalid session epoch. |
| 5 | Add partition leader epochs, OffsetForLeaderEpoch, and timestamp ListOffsets. | Java `offsetsForTimes()` returns the first eligible record and a consumer validates/truncates against a deterministic leader epoch. |
| 6 | Implement DeleteTopics by name/ID and topic recreation with a new UUID. | Java Admin deletes and recreates a topic; old-ID Fetch/Metadata requests fail and the new topic begins empty. |
| 7 | Implement CreatePartitions with atomic validation and preserved logs. | Java Admin grows a two-partition topic to six; existing offsets remain readable and clients discover four new empty partitions. |
| 8 | Implement DeleteRecords and a real log-start offset. | Java Admin truncates to an offset; Fetch below it fails, earliest advances, latest is unchanged, and Kafbat shows only remaining records. |
| 9 | Make DescribeConfigs truthful for broker/topic resources, filtering, sources, synonyms, and sensitivity. | Java Admin and Kafbat display the same effective MemKafka values and unknown keys/resources return exact errors. |
| 10 | Add IncrementalAlterConfigs plus the narrow AlterConfigs compatibility wrapper for supported mutable settings. | Java Admin changes a supported topic setting with validate-only and real mutation paths; unsupported keys fail without partial changes. |
| 11 | Add DeleteGroups and OffsetDelete, then complete ListGroups/DescribeGroups filters and versions. | Java Admin lists/describes an active group, deletes selected offsets when legal, and deletes the group after all members leave. |
| 12 | Complete classic group schemas and static membership, including instance fencing and restart behavior. | A real consumer with `group.instance.id` restarts without unnecessary reassignment and a duplicate live instance is fenced. |
| 13 | Add transactional-ID allocation, epoch bump/fencing, AddPartitionsToTxn, and transaction inspection in the Ongoing state. | A real transactional producer initializes and begins producing; a second producer with the same ID fences the first; Admin sees the transaction. |
| 14 | Add EndTxn/control batches, TxnOffsetCommit, aborted ranges, last-stable-offset, and Fetch isolation. | A consume-transform-produce test atomically commits output and offsets; abort hides output from `read_committed` while `read_uncommitted` can observe it. |
| 15 | Implement ConsumerGroupHeartbeat with one broker-side assignor and ConsumerGroupDescribe. | Three unmodified Java consumers using `group.protocol=consumer` converge, scale down, and expose consistent member/assignment epochs. |

After cut 15, take P4, P5, and P6 as scenario-driven slices. Do not start a whole family merely to reserve API keys.

## Testing and compatibility strategy

### One capability source

The central runtime capability registry is the source for advertised API keys and versions. It currently drives or validates:

- the runtime `ApiVersions` response;
- dispatcher/handler coverage;
- the evidence-backed current-client floor and Kafka-baseline ceiling;
- supported floor/ceiling and rejected-below/rejected-above test vectors;
- the checked-in compatibility artifact consumed by docs;
- maturity and real-client proof metadata.

The generated [`kafka-api-capabilities.json`](compatibility/kafka-api-capabilities.json) manifest publishes current supported windows, Kafka 4.3 ranges, and proof scenarios. The separate [`kafka-4.3-client-requests.json`](compatibility/kafka-4.3-client-requests.json) artifact records request versions observed from pinned clients against Kafka 4.3.1. CI fails if code, `ApiVersions`, captured current-client evidence, or the generated artifact disagrees. A client evidence change requires explicit compatibility review.

### Protocol and version tests

For each advertised API:

- decode and encode the evidence-backed floor and Kafka-baseline ceiling;
- exercise each version boundary where fields or headers change;
- reject one version below the floor and above the ceiling with the closest Kafka behavior;
- assert flexible compact fields, tagged fields, nullability, defaults, and response header version;
- cover per-resource partial success, not only whole-request success/failure;
- compare all documented error paths with Kafka 4.3.

### State-machine and concurrency tests

Use deterministic time and controlled interleavings for:

- group generations/member epochs, joins, leaves, fencing, and expiry;
- Fetch/share sessions and wakeups;
- producer/transaction epochs, timeouts, commits, and aborts;
- concurrent topic create/delete/grow and stale topic IDs;
- log-start/high-watermark/last-stable-offset invariants;
- ACL/quota/config mutation atomicity.

Every rejected operation must prove that state is unchanged.

### Differential black-box tests

Run identical probes against MemKafka and a digest-pinned Apache Kafka 4.3 image. Capture request inputs and client-observed results, then normalize only the intentional fields listed in [What parity means](#what-parity-means).

Prefer public client APIs for probes. Use a raw protocol probe only for versions, malformed inputs, or broker/controller APIs that ordinary clients cannot force deterministically.

Store compact normalized fixtures so a semantic change is reviewed as data rather than buried in logs.

### Real-client lanes

Keep the current lanes and expand deliberately:

| Lane | Primary role |
| --- | --- |
| Apache Kafka Java client/Admin | Reference producer, consumer, transactions, groups, and full Admin surface |
| librdkafka via Confluent.Kafka (.NET) | Independent negotiation, idempotence, transactions, classic groups, and rebalance behavior |
| franz-go | Independent Go codecs, producer/consumer behavior, and admin operations |
| rskafka | Independent Rust metadata/data-plane behavior; add broader Rust coverage if its API surface permits |
| Python | Pin an unmodified client and cover discovery, produce/fetch, groups/offsets, transactions where supported |
| Node | Pin an unmodified client such as a maintained KafkaJS-compatible lane and cover discovery, data, groups, and admin |

Do not infer one client's coverage from another. Pin versions when a lane is introduced and record the exact scenario it proves.

### Ecosystem gates

Add gates when their dependencies are behaviorally mature:

- **Kafbat:** expand from discovery/browsing to topic/group/config lifecycle after P1;
- **transactional clients:** gate P2 on commit, abort, fencing, `sendOffsetsToTransaction`, and both isolation levels;
- **Kafka Streams:** gate exactly-once mode after P2, modern consumer protocol after P3, and StreamsGroup RPCs after P5;
- **Kafka Connect:** add only after its actual Admin, group, offset, config, and transaction calls are mapped; startup alone is not success.

### Fuzzing and property tests

- fuzz length prefixes, request headers, flexible/tagged fields, record batches, and malformed/truncated payloads;
- assert one malformed connection cannot corrupt broker state;
- property-test monotonic unique offsets, topic-ID uniqueness across recreation, epoch fencing, assignment disjointness, commit visibility, and atomic mutation;
- seed the corpus with every supported floor/ceiling schema and differential failure.

## Success criteria and guardrails

### Per-API maturity ladder

| Level | Meaning | May be described as supported? |
| --- | --- | --- |
| `absent` | No generated codec or handler. | No |
| `codec` | Request/response types encode and decode for named versions. No semantic claim. | No |
| `protocol` | Dispatcher and focused wire/error tests pass. State may still be shallow. | No |
| `behavioral` | State transitions and cross-API invariants pass, and at least one unmodified real client proves the intended user scenario. | Yes, with the exact scenario/version stated |
| `ecosystem-proven` | Differential Kafka results plus multiple clients or a real tool/framework pass the documented scenario. | Yes |

The public rule remains: a feature is supported only when an unmodified real client passes a black-box test against the `memkafka` binary. The ladder prevents “codec exists” and “handler returned success” from diluting that rule.

### API-level exit criteria

An API advances to `behavioral` only when:

- its advertised floor-to-ceiling window has boundary/rejected codec and error tests;
- each pinned client scenario negotiates at or above the recorded floor;
- all state mutations are atomic and failure paths prove no mutation;
- its cross-API effects are tested;
- a real client scenario exercises the API without patches or test hooks;
- unsupported versions/features remain clearly omitted or rejected.

It advances to `ecosystem-proven` when a Kafka 4.3 differential passes after documented normalization and either a second independent client or an ecosystem tool passes the same capability.

### Simplicity guardrails

- No feature without a named consumer, Admin, tool, or framework scenario.
- No replication/controller machinery when deterministic single-broker semantics suffice.
- No silent no-ops.
- No Kafka configuration-file parity; expose only a small MemKafka-native setup surface where a scenario needs state.
- No legacy wire-version work below the evidence-backed floor; client pins may raise floors but do not lower them.
- No API advertisement before the handler, state semantics, errors, and real-client test exist.
- No production, durability, availability, security-hardening, or performance-equivalence claims.
- Keep unsupported-version and unsupported-feature errors explicit throughout migration.
- Never use 77/77 as shorthand for behavioral parity.

## Related Schema Registry gap

Schema Registry is a separate HTTP compatibility surface, not a Kafka broker API. It must not be included in the 77-key score.

The repository currently provides an Avro-first subset: subjects, exact-text registration/deduplication, IDs, versions, lookups, and read-only compatibility mode `NONE`. Its declared major gaps are:

- Protobuf;
- JSON Schema;
- schema references;
- compatibility enforcement and compatibility config mutation;
- subject/version/schema deletion;
- semantic normalization.

Track those in a separate Schema Registry roadmap after the broker API matrix is established. Combining the two would hide progress and make “Kafka parity” ambiguous.

## Evidence and maintenance notes

Repository evidence reviewed for this snapshot:

- `src/kafka/api_versions.rs`, `src/kafka/dispatcher.rs`, and every handler under `src/kafka/`;
- `src/broker/` topic, partition, producer, and group state;
- `src/schema_registry.rs`;
- `README.md` and `docs/2026-08-26-memkafka-design.md`;
- focused wire tests plus .NET, Java, Go, Rust, flow-profile, and Kafbat black-box tests under `tests/`;
- the generated capability manifest and pinned-client Kafka 4.3.1 request evidence under `docs/compatibility/`;
- `Cargo.toml` and the installed `kafka-protocol 0.18.0` generated sources;
- the [Apache Kafka 4.3 protocol](https://kafka.apache.org/43/design/protocol), Kafka 4.3 [Admin API](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/admin/Admin.html), and linked Apache KIPs.

When Kafka or the generated protocol crate changes, regenerate the stable table, diff names/keys/version ranges, rerun all capability checks, and date a new snapshot. Preserve older snapshots in version control rather than silently rewriting what the v0.1 implementation once supported.
