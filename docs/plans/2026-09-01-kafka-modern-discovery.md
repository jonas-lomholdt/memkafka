# Kafka Modern Discovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give each in-memory topic a stable UUID and expose one coherent broker/topic view through Kafka 4.3.1's current discovery APIs.

**Architecture:** Keep topic names, UUIDs, partition logs, and reverse lookup in one atomically updated `TopicCatalog` state. Add small Kafka handlers for `DescribeCluster` v2 and `DescribeTopicPartitions` v0, then extend `Metadata` through v13 and `CreateTopics` through v7. Put shared single-broker discovery facts in one private module while leaving request routing, capability declaration, and connection-specific advertised addresses in their existing layers.

**Tech Stack:** Rust 1.98 / edition 2024, Tokio, `kafka-protocol` 0.18.0-memkafka.1 generated from Kafka 4.3.1, `uuid` 1.x, Java 25, Apache Kafka Java client 4.3.1, Maven, Docker, and GitHub Actions.

**Spec:** [`../2026-09-01-kafka-modern-discovery-design.md`](../2026-09-01-kafka-modern-discovery-design.md)

## Global Constraints

- Work on `jlo/kafka-modern-discovery` and preserve unrelated user changes.
- Read [`../2026-08-26-memkafka-design.md`](../2026-08-26-memkafka-design.md) before the first behavior change.
- Add one focused failing test before each behavior change and confirm it fails for the intended reason.
- Advertise only the approved current-client windows: Metadata v4-v13, CreateTopics v4-v7, DescribeCluster v2, and DescribeTopicPartitions v0.
- Keep topic UUIDs in memory only. Do not add persistence, deletion, replication, a storage trait, or UUID-addressed data-plane APIs.
- Keep catalog name and UUID indexes private and update both under the same write lock.
- Build responses from cloned catalog metadata so no lock is held during protocol encoding.
- Keep every listener's advertised address connection-specific through its existing `Dispatcher`.
- Update `DISPATCHED_API_KEYS`, `ERROR_RESPONSE_API_KEYS`, `CAPABILITIES`, router fixtures, wire boundary tests, and the checked-in manifest together whenever an API window changes.
- Keep `unsafe_code = "forbid"`, strict Clippy, and the pinned Rust toolchain.
- Make one small commit after each task is green.

---

## Task 1: Add atomic topic identity to the catalog

**Files:**

- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `src/broker/topics.rs`
- Modify: `src/kafka/create_topics.rs`

**Interfaces:**

- `TopicMetadata` gains `pub id: Uuid`.
- `TopicCatalog` stores `Arc<RwLock<CatalogState>>`.
- `CatalogState` owns `topics_by_name: BTreeMap<String, TopicEntry>` and `names_by_id: HashMap<Uuid, String>`.
- Add `TopicCatalog::get(&self, name: &str) -> Result<Option<TopicMetadata>, TopicError>` so read-only handlers can distinguish invalid names from valid missing names.
- Add `TopicCatalog::get_by_id(&self, id: Uuid) -> Option<TopicMetadata>`.
- Change `validate_explicit` to `Result<(), TopicError>` so successful validation cannot manufacture identity.

- [ ] **Step 1: Add focused UUID lifecycle tests**

Extend the `src/broker/topics.rs` test module with these behaviors:

```rust
#[tokio::test]
async fn created_topic_has_one_stable_non_nil_id_in_both_indexes() {
    let catalog = catalog_with_two_defaults();
    let created = catalog
        .create_explicit("events", 3, 1)
        .await
        .expect("create topic");

    assert!(!created.id.is_nil());
    assert_eq!(catalog.get("events").await, Ok(Some(created.clone())));
    assert_eq!(catalog.get_by_id(created.id).await, Some(created.clone()));
    assert_eq!(catalog.list().await, vec![created]);
}

#[tokio::test]
async fn separate_topics_receive_distinct_ids() {
    let catalog = catalog_with_two_defaults();
    let first = catalog.create_explicit("a", 1, 1).await.expect("first");
    let second = catalog.create_explicit("b", 1, 1).await.expect("second");

    assert_ne!(first.id, second.id);
}

#[tokio::test]
async fn validation_and_failed_creation_do_not_mutate_identity_indexes() {
    let catalog = catalog_with_two_defaults();

    assert_eq!(catalog.validate_explicit("valid", 2, 1).await, Ok(()));
    assert_eq!(
        catalog.create_explicit("invalid", 0, 1).await,
        Err(TopicError::InvalidPartitions)
    );
    assert!(catalog.list().await.is_empty());
    assert_eq!(catalog.get("valid").await, Ok(None));
}
```

Change the existing concurrent auto-create test to collect every returned UUID and assert that all 32 calls returned the same non-nil UUID. Extend the duplicate-create test to assert that name lookup and reverse lookup still return the original metadata after the duplicate fails.

- [ ] **Step 2: Run the catalog tests and confirm RED**

Run:

```bash
cargo test broker::topics::tests -- --nocapture
```

Expected: compilation fails because `TopicMetadata::id`, `get`, and `get_by_id` do not exist and `validate_explicit` still returns metadata.

- [ ] **Step 3: Add UUID generation and the dual-index state**

Add the direct dependency:

```toml
uuid = { version = "1", features = ["v4"] }
```

Use this private state shape in `src/broker/topics.rs`:

```rust
#[derive(Debug, Default)]
struct CatalogState {
    topics_by_name: BTreeMap<String, TopicEntry>,
    names_by_id: HashMap<Uuid, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicMetadata {
    pub id: Uuid,
    pub name: String,
    pub partition_count: u32,
}
```

Factor name/partition/replication validation so it returns only the validated partition count. For creation, acquire the write lock, validate, check that the name is vacant, and then generate a UUID with a collision guard:

```rust
fn next_topic_id(state: &CatalogState) -> Uuid {
    loop {
        let candidate = Uuid::new_v4();
        if !candidate.is_nil() && !state.names_by_id.contains_key(&candidate) {
            return candidate;
        }
    }
}
```

Insert the `TopicEntry` and reverse-index entry before releasing the lock. Use the same helper from explicit and automatic creation. On the automatic-create slow path, recheck the name after acquiring the write lock and return the stored metadata if another task won the race.

- [ ] **Step 4: Add read-only lookup methods without leaking locks**

Implement validated name lookup, UUID lookup, deterministic `list`, and `partition` against `CatalogState`. `get` validates before reading and returns `Err(TopicError::InvalidName)` for invalid input, `Ok(None)` for a valid miss, and cloned metadata for a hit. Clone metadata or `Arc<PartitionLog>` while the read guard is held, then return the clone.

- [ ] **Step 5: Adapt validation-only CreateTopics to the identity-free result**

Split the existing `create_one` validation branch from real creation. A successful `validate_explicit` call builds the current success response from the requested partition count and leaves the default nil topic ID. A real creation still passes stored `TopicMetadata` into `success_result`. This step changes no advertised API window; it only keeps current CreateTopics v4-v6 behavior compiling while ensuring validation does not allocate identity.

- [ ] **Step 6: Confirm GREEN and commit**

Run:

```bash
cargo fmt --all
cargo test broker::topics::tests -- --nocapture
cargo test --all-targets --all-features
git diff --check
```

Expected: all commands exit `0`.

Commit:

```bash
git add Cargo.toml Cargo.lock src/broker/topics.rs src/kafka/create_topics.rs
git commit -m "feat: assign stable topic ids"
```

---

## Task 2: Modernize Metadata and CreateTopics around catalog identity

**Files:**

- Create: `src/kafka/discovery.rs`
- Modify: `src/kafka/mod.rs`
- Modify: `src/kafka/metadata.rs`
- Modify: `src/kafka/create_topics.rs`
- Modify: `src/kafka/dispatcher.rs`
- Modify: `src/kafka/capabilities.rs`
- Modify: `src/kafka/error_response.rs`
- Modify: `src/kafka/request_router.rs`
- Modify: `tests/kafka_wire.rs`

**Interfaces:**

- `metadata::response` receives the request version before `BrokerState`.
- `discovery` owns cluster ID and authorization constants.
- Metadata supports v4-v13 and CreateTopics supports v4-v7.

- [ ] **Step 1: Add failing wire tests for the new version windows**

In `tests/kafka_wire.rs`, add tests that create one topic and prove:

```text
CreateTopics v7 returns a non-nil topic ID.
Metadata v10, v11, v12, and v13 return that exact ID for a name request.
CreateTopics v7 validate_only returns a nil ID and Metadata cannot find the name.
CreateTopics v7 errors return a nil ID.
Metadata v4-v9 preserve their existing name-based behavior.
```

Add focused Metadata v12/v13 cases for known UUID, unknown UUID, mixed UUID/name mode, null topic list, empty topic list, disabled auto-create, invalid names, and authorization flags. Add v10/v11 cases proving a non-nil request UUID or null name yields `INVALID_REQUEST` without catalog mutation. For v13, also assert the top-level error; for earlier response versions, assert the per-topic errors only.

Use the generated request fields directly:

```rust
MetadataRequestTopic::default()
    .with_topic_id(created_id)
    .with_name(None)
```

- [ ] **Step 2: Raise only the approved capability windows and confirm RED**

Change the capability declarations and their exact expected-window tests to:

```rust
ApiKey::Metadata => VersionWindow { min: 4, max: 13 }
ApiKey::CreateTopics => VersionWindow { min: 4, max: 7 }
```

Update the supported/adjacent boundary fixtures in `src/kafka/request_router.rs`, `src/kafka/error_response.rs`, and `tests/kafka_wire.rs`, then run:

```bash
cargo test --test kafka_wire metadata_v13 -- --nocapture
cargo test --test kafka_wire create_topics_v7 -- --nocapture
```

Expected: the requests now route, but assertions fail because responses still omit or mishandle topic IDs.

- [ ] **Step 3: Centralize the shared discovery constants**

Create `src/kafka/discovery.rs` with no mutable state:

```rust
pub(crate) const CLUSTER_ID: &str = "memkafka";
pub(crate) const TOPIC_AUTHORIZED_OPERATIONS: i32 = 3576;
pub(crate) const CLUSTER_AUTHORIZED_OPERATIONS: i32 = 8096;

pub(crate) const fn optional_authorized_operations(include: bool, value: i32) -> i32 {
    if include { value } else { i32::MIN }
}
```

Register it privately in `src/kafka/mod.rs`. Reuse `CLUSTER_ID` and both bitfields from every discovery handler introduced by this plan.

- [ ] **Step 4: Implement version-aware Metadata resolution**

Pass `version` from `Dispatcher::dispatch`:

```rust
metadata::response(
    body,
    version,
    &self.broker,
    &self.advertised_kafka,
).await
```

Split Metadata handling into explicit helpers:

```rust
async fn topics_for_request(
    request: &MetadataRequest,
    version: i16,
    broker: &BrokerState,
) -> (i16, Vec<MetadataResponseTopic>);

async fn topics_by_name(
    requested: &[MetadataRequestTopic],
    version: i16,
    broker: &BrokerState,
) -> Vec<MetadataResponseTopic>;

async fn topics_by_id(
    requested: &[MetadataRequestTopic],
    broker: &BrokerState,
) -> Vec<MetadataResponseTopic>;
```

Required branch order:

1. `None` lists the catalog; `Some([])` returns no topics.
2. In v10-v11, reject the requested entries if any name is null or any UUID is non-nil.
3. In v12-v13, choose UUID mode for the complete request when any entry has a non-nil UUID.
4. Name mode keeps the existing auto-create rules.
5. UUID mode uses `TopicCatalog::get_by_id`, never creates, returns canonical names for hits, and preserves unknown requested IDs with `ResponseError::UnknownTopicId` plus a null name.

Every successful response topic carries `.with_topic_id(topic.id)`. Error topics carry `Uuid::nil()` except unknown-ID results, which preserve the requested UUID. Set topic and cluster authorization bitfields only when the request asks for them. Set the v13 top-level error to `INVALID_REQUEST` for semantic request rejection and zero otherwise.

- [ ] **Step 5: Populate CreateTopics v7 IDs only when creation is real**

Keep the validation and creation branches introduced in Task 1 separate. The validation branch remains identity-free:

```rust
if validate_only {
    return match broker.topics().validate_explicit(
        topic.name.as_str(),
        topic.num_partitions,
        topic.replication_factor,
    ).await {
        Ok(()) => validated_result(topic),
        Err(error) => topic_error_result(topic.name.clone(), error),
    };
}
```

For a real success, set `.with_topic_id(metadata.id)`. Leave `CreatableTopicResult::default()`'s nil UUID intact for validation and every error. Older response encoders omit the field automatically.

- [ ] **Step 6: Run focused and regression tests, then commit**

Run:

```bash
cargo fmt --all
cargo test broker::topics::tests -- --nocapture
cargo test --test kafka_wire metadata -- --nocapture
cargo test --test kafka_wire create_topics -- --nocapture
cargo test --all-targets --all-features
git diff --check
```

Expected: all commands exit `0`.

Commit:

```bash
git add src/kafka/discovery.rs src/kafka/mod.rs src/kafka/metadata.rs \
  src/kafka/create_topics.rs src/kafka/dispatcher.rs src/kafka/capabilities.rs \
  src/kafka/error_response.rs src/kafka/request_router.rs tests/kafka_wire.rs
git commit -m "feat: expose topic ids through metadata"
```

---

## Task 3: Add DescribeCluster v2 as a complete routed API

**Files:**

- Create: `src/kafka/describe_cluster.rs`
- Modify: `src/kafka/mod.rs`
- Modify: `src/kafka/dispatcher.rs`
- Modify: `src/kafka/capabilities.rs`
- Modify: `src/kafka/error_response.rs`
- Modify: `src/kafka/request_router.rs`
- Modify: `tests/kafka_wire.rs`

**Interfaces:**

- `describe_cluster::response(request, broker, advertised_kafka) -> DescribeClusterResponse`.
- DescribeCluster advertises only v2.

- [ ] **Step 1: Add failing v2 wire tests**

Add `tests/kafka_wire.rs` cases for:

```text
endpoint_type=1 returns cluster memkafka, controller=broker, and one unfenced broker.
include_cluster_authorized_operations=false returns i32::MIN.
include_cluster_authorized_operations=true returns 8096.
endpoint_type=2 returns MISMATCHED_ENDPOINT_TYPE and no brokers.
an unrecognized endpoint type returns UNSUPPORTED_ENDPOINT_TYPE and no brokers.
two listeners each return their own advertised host/port.
v1 receives a typed UNSUPPORTED_VERSION response without dispatch.
```

Use `DescribeClusterRequest::default().with_endpoint_type(1)` and the existing multi-listener wire harness.

- [ ] **Step 2: Run the new wire tests and confirm RED**

Run:

```bash
cargo test --test kafka_wire describe_cluster -- --nocapture
```

Expected: v2 receives the existing typed unsupported-version path because DescribeCluster is not advertised yet.

- [ ] **Step 3: Advertise and route v2 with a minimal empty response**

Add API key 60 in numeric order to the capability, dispatch, error-response, and router declarations:

```rust
ApiCapability {
    api_key: ApiKey::DescribeCluster,
    name: "DescribeCluster",
    supported: VersionWindow { min: 2, max: 2 },
    kafka_4_3: VersionWindow { min: 0, max: 2 },
    proof_scenarios: &["apache-kafka-java-4.3.1"],
}
```

Increase exact registry/coverage counts from 17 to 18 and extend `router_request`. Create `src/kafka/describe_cluster.rs` with the final `response(request, broker, advertised_kafka)` signature and temporarily return `DescribeClusterResponse::default()`. Run:

```bash
cargo test --test kafka_wire describe_cluster -- --nocapture
```

Expected: the request reaches the new dispatch arm, but the assertions stay RED because the response has no cluster or broker data.

- [ ] **Step 4: Implement the single-broker DescribeCluster response**

For broker endpoint type `1`, construct:

```rust
DescribeClusterResponse::default()
    .with_endpoint_type(1)
    .with_cluster_id(StrBytes::from_static_str(CLUSTER_ID))
    .with_controller_id(BrokerId::from(broker.broker_id()))
    .with_brokers(vec![
        DescribeClusterBroker::default()
            .with_broker_id(BrokerId::from(broker.broker_id()))
            .with_host(StrBytes::from_string(advertised_kafka.host().to_owned()))
            .with_port(i32::from(advertised_kafka.port()))
            .with_rack(None)
            .with_is_fenced(false),
    ])
    .with_cluster_authorized_operations(optional_authorized_operations(
        request.include_cluster_authorized_operations,
        CLUSTER_AUTHORIZED_OPERATIONS,
    ))
```

For endpoint type `2`, return `ResponseError::MismatchedEndpointType`; for every other value, return `ResponseError::UnsupportedEndpointType`. Error results keep an empty broker list and do not invent a controller/cluster payload.

- [ ] **Step 5: Complete the typed unsupported-version response**

In `error_response::unsupported_version`, map `RequestKind::DescribeCluster` to a default `DescribeClusterResponse` with `UNSUPPORTED_VERSION`. Add it to `ERROR_RESPONSE_API_KEYS` and the exact variant tests. The router remains the only version gate.

- [ ] **Step 6: Confirm GREEN and commit**

Run:

```bash
cargo fmt --all
cargo test --test kafka_wire describe_cluster -- --nocapture
cargo test kafka::request_router::tests -- --nocapture
cargo test kafka::error_response::tests -- --nocapture
cargo test --all-targets --all-features
git diff --check
```

Expected: all commands exit `0`.

Commit:

```bash
git add src/kafka/describe_cluster.rs src/kafka/mod.rs src/kafka/dispatcher.rs \
  src/kafka/capabilities.rs src/kafka/error_response.rs \
  src/kafka/request_router.rs tests/kafka_wire.rs
git commit -m "feat: implement describe cluster"
```

---

## Task 4: Implement exact DescribeTopicPartitions pagination

**Files:**

- Create: `src/kafka/describe_topic_partitions.rs`
- Modify: `src/kafka/mod.rs`
- Modify: `src/kafka/dispatcher.rs`
- Modify: `src/kafka/capabilities.rs`
- Modify: `src/kafka/error_response.rs`
- Modify: `src/kafka/request_router.rs`
- Modify: `tests/kafka_wire.rs`

**Interfaces:**

- `describe_topic_partitions::response(request, broker) -> DescribeTopicPartitionsResponse`.
- DescribeTopicPartitions advertises only v0.

- [ ] **Step 1: Write table-driven wire tests first**

Put the pagination matrix in `tests/kafka_wire.rs` using the existing real TCP harness. Create catalog topics `alpha` with three partitions, `bravo` with two, and `charlie` with one. Cover this exact table:

| Request | Expected topic partitions | Expected next cursor |
| --- | --- | --- |
| all topics, limit 2 | `alpha:[0,1]` | `alpha:2` |
| all topics, cursor `alpha:2`, limit 2 | `alpha:[2]`, `bravo:[0]` | `bravo:1` |
| all topics, cursor `bravo:2`, limit 1 | `bravo:[]`, `charlie:[0]` | none |
| explicit duplicates in reverse order, limit 3 | `alpha:[0,1,2]` once | `bravo:0` |
| limit 0 | one successful partition | matching continuation cursor |
| missing explicit topic before a real topic, limit 1 | missing error plus one real partition | continuation based only on real partitions |

Also test invalid cursor topic, negative partition, invalid topic name, and valid missing topic. Invalid cursor requests must preserve the raw request entries and return one `INVALID_REQUEST` result per entry, including duplicates, with no cursor. Valid pagination still de-duplicates names before sorting.

For every successful topic in the matrix, assert its non-nil stored UUID, `is_internal=false`, topic authorization bitfield `3576`, and exact leader/epoch/replica/ISR/offline-replica fields. For missing and invalid topics, assert nil UUIDs and empty partition lists.

- [ ] **Step 2: Run the new wire tests and confirm RED**

Run:

```bash
cargo test --test kafka_wire describe_topic_partitions -- --nocapture
```

Expected: v0 is not advertised, so the connection cannot return the expected discovery response.

- [ ] **Step 3: Add the v0 capability/routing surface with an empty response**

Add API key 75 after all lower API keys:

```rust
ApiCapability {
    api_key: ApiKey::DescribeTopicPartitions,
    name: "DescribeTopicPartitions",
    supported: VersionWindow { min: 0, max: 0 },
    kafka_4_3: VersionWindow { min: 0, max: 0 },
    proof_scenarios: &["apache-kafka-java-4.3.1"],
}
```

Increase exact coverage counts from 18 to 19. Extend `router_request`, dispatcher matching, error-response declarations, and the wire codec helpers. Create `src/kafka/describe_topic_partitions.rs` with the final `response(request, broker)` signature and temporarily return `DescribeTopicPartitionsResponse::default()`. Run:

```bash
cargo test --test kafka_wire describe_topic_partitions -- --nocapture
```

Expected: routing succeeds, but the pagination assertions stay RED because the response is empty.

- [ ] **Step 4: Normalize selection and validate the cursor**

Implement a pure selection helper that returns a sorted, de-duplicated `Vec<String>`:

```rust
fn selected_names(
    request: &DescribeTopicPartitionsRequest,
    catalog_topics: &[TopicMetadata],
) -> Result<Vec<String>, ResponseError>;
```

Rules:

1. Empty `request.topics` selects every catalog name.
2. Explicit names are inserted into a `BTreeSet`.
3. A negative cursor partition is invalid.
4. For explicit selection, the cursor topic must be present.
5. Names before the cursor topic are removed.

Resolve each selected name with the validated `TopicCatalog::get` method. Do not call `get_or_auto_create`; this API is read-only. Assert that missing and invalid requests leave the catalog unchanged.

- [ ] **Step 5: Implement partition-counted pagination**

Use `usize::try_from(request.response_partition_limit.max(1))` for the effective limit. Maintain `remaining`, response topics, and `next_cursor`. Unknown/invalid topic envelopes do not decrement `remaining`.

For each successful topic:

```rust
let start = if cursor.topic_name == metadata.name {
    usize::try_from(cursor.partition_index).expect("validated non-negative cursor")
} else {
    0
};
let end = metadata.partition_count as usize;
let take = end.saturating_sub(start).min(remaining);
```

Append partitions in `[start, start + take)`. If the topic still has partitions, set the cursor to that topic's first omitted partition. If the topic completed exactly when `remaining` reached zero and another selected topic remains, set the cursor to that next topic at partition zero. A start at or above the real count returns a successful empty topic and continues.

- [ ] **Step 6: Build Kafka-shaped success and error topics**

Successful topics include the stored UUID, canonical name, `is_internal=false`, `TOPIC_AUTHORIZED_OPERATIONS`, and partition entries with:

```text
error=NONE
leader=broker ID
leader epoch=0
replicas=[broker ID]
ISR=[broker ID]
eligible leader replicas=null
last known ELR=null
offline replicas=[]
```

Valid missing names return `UNKNOWN_TOPIC_OR_PARTITION`; invalid names return `INVALID_TOPIC_EXCEPTION`. Both use nil UUIDs and empty partitions.

- [ ] **Step 7: Add focused helper tests and typed error construction**

The wire matrix already sends v0 with a two-partition limit, decodes the first response cursor, sends continuation requests, and proves the union has every partition exactly once and in order. Add focused unit tests beside any pure selection or cursor helper whose edge cases are clearer without TCP setup.

Although v0 has no schema-known adjacent version, add DescribeTopicPartitions to the exhaustive typed error constructor and coverage declaration. For a constructed unsupported response, preserve each requested name, set `UNSUPPORTED_VERSION`, nil UUID, false internal status, and no partitions. This keeps the exhaustive request-kind match future-safe.

- [ ] **Step 8: Confirm GREEN and commit**

Run:

```bash
cargo fmt --all
cargo test kafka::describe_topic_partitions::tests -- --nocapture
cargo test --test kafka_wire describe_topic_partitions -- --nocapture
cargo test kafka::request_router::tests -- --nocapture
cargo test kafka::error_response::tests -- --nocapture
cargo test --all-targets --all-features
git diff --check
```

Expected: all commands exit `0`.

Commit:

```bash
git add src/kafka/describe_topic_partitions.rs src/kafka/mod.rs \
  src/kafka/dispatcher.rs src/kafka/capabilities.rs src/kafka/error_response.rs \
  src/kafka/request_router.rs tests/kafka_wire.rs
git commit -m "feat: describe topic partitions"
```

---

## Task 5: Prove cross-API identity and listener coherence over TCP

**Files:**

- Modify: `tests/kafka_wire.rs`

- [ ] **Step 1: Add one identity contract test**

Create `modern_discovery_uses_one_topic_identity_across_apis`. Over one real TCP connection:

1. create `identity-topic` with CreateTopics v7 and capture its non-nil UUID;
2. request Metadata v13 by name and assert the same UUID;
3. request Metadata v13 by UUID and assert the canonical name and same UUID;
4. request DescribeTopicPartitions v0 and assert the same UUID plus every partition;
5. request Metadata v13 by a random unknown UUID and assert `UNKNOWN_TOPIC_ID`, null name, the requested UUID, and no catalog mutation.

- [ ] **Step 2: Extend the multi-listener contract**

In the existing two-listener test, send both Metadata v13 and DescribeCluster v2 over each connection. Assert each pair reports its listener's advertised host/port while cluster ID, broker ID, controller ID, topic UUID, and partitions remain identical.

- [ ] **Step 3: Prove identity survives unrelated requests and duplicate failures**

Create a topic, record its UUID, exercise Produce, Fetch, Metadata, a duplicate CreateTopics failure, and DescribeTopicPartitions, then assert every discovery response still carries the original UUID.

- [ ] **Step 4: Run the complete wire suite and commit**

Run:

```bash
cargo fmt --all
cargo test --test kafka_wire -- --nocapture
cargo test --all-targets --all-features
git diff --check
```

Expected: all commands exit `0`.

Commit:

```bash
git add tests/kafka_wire.rs
git commit -m "test: prove modern discovery contract"
```

---

## Task 6: Extend the Java 4.3.1 black-box and request evidence

**Files:**

- Modify: `tests/java/src/test/java/io/memkafka/acceptance/KafkaJavaClientBlackBoxTest.java`
- Modify: `docs/compatibility/kafka-4.3-client-requests.json`

- [ ] **Step 1: Add the Java cluster and topic-identity acceptance test**

Add one Java test that uses only public Kafka 4.3.1 Admin APIs:

```java
@Test
void adminDiscoversClusterAndStableTopicIdsThroughPagination() throws Exception {
    var first = uniqueTopic("java-discovery-a");
    var second = uniqueTopic("java-discovery-b");

    try (var admin = Admin.create(adminConfiguration())) {
        var cluster = admin.describeCluster();
        assertEquals("memkafka", cluster.clusterId().get(5, SECONDS));
        var controller = cluster.controller().get(5, SECONDS);
        var nodes = cluster.nodes().get(5, SECONDS);
        assertEquals(1, nodes.size());
        assertEquals(controller.id(), nodes.iterator().next().id());

        var create = admin.createTopics(List.of(
                new NewTopic(first, 3, (short) 1),
                new NewTopic(second, 2, (short) 1)));
        create.all().get(5, SECONDS);
        var createdFirstId = create.topicId(first).get(5, SECONDS);
        var createdSecondId = create.topicId(second).get(5, SECONDS);

        var options = new DescribeTopicsOptions().partitionSizeLimitPerResponse(2);
        var described = admin.describeTopics(List.of(second, first), options)
                .allTopicNames()
                .get(5, SECONDS);
        var listings = admin.listTopics().listings().get(5, SECONDS).stream()
                .collect(Collectors.toMap(TopicListing::name, TopicListing::topicId));

        assertEquals(createdFirstId, described.get(first).topicId());
        assertEquals(createdSecondId, described.get(second).topicId());
        assertEquals(createdFirstId, listings.get(first));
        assertEquals(createdSecondId, listings.get(second));
        assertEquals(List.of(0, 1, 2), described.get(first).partitions().stream()
                .map(TopicPartitionInfo::partition)
                .toList());
        assertEquals(List.of(0, 1), described.get(second).partitions().stream()
                .map(TopicPartitionInfo::partition)
                .toList());
    }
}
```

Add imports for `DescribeTopicsOptions`, `TopicListing`, `TopicPartitionInfo`, and `Collectors`. Assert both IDs are unequal to `org.apache.kafka.common.Uuid.ZERO_UUID` before comparing them across APIs.

- [ ] **Step 2: Run Java against MemKafka and confirm the real client path**

Use the same native/container setup as the `java-blackbox` job in `.github/workflows/verify.yml`. Build and start MemKafka, wait for readiness, then run:

```bash
mvn --batch-mode --file tests/java/pom.xml test
```

Expected: all Java tests pass. If request capture shows the Admin client did not paginate, first confirm both topics and the partition limit are present; do not add sleeps or a protocol-only substitute.

- [ ] **Step 3: Refresh the pinned Kafka 4.3.1 request-version evidence**

Run:

```bash
tests/api-versions/run.sh --update
tests/api-versions/run.sh --check
```

Expected: the Java scenario records DescribeCluster v2 and DescribeTopicPartitions v0, the evidence check passes, and existing client scenarios remain intact.

- [ ] **Step 4: Commit black-box coverage and evidence**

```bash
git add tests/java/src/test/java/io/memkafka/acceptance/KafkaJavaClientBlackBoxTest.java \
  docs/compatibility/kafka-4.3-client-requests.json
git commit -m "test: cover modern discovery with java"
```

---

## Task 7: Synchronize capability artifacts, docs, and the full release gate

**Files:**

- Modify: `docs/compatibility/kafka-api-capabilities.json`
- Modify: `README.md`
- Modify: `docs/kafka-api-parity-roadmap.md`

- [ ] **Step 1: Regenerate and check the 19-API manifest**

Run:

```bash
cargo run --quiet --example kafka_api_capabilities -- \
  --update docs/compatibility/kafka-api-capabilities.json
cargo run --quiet --example kafka_api_capabilities -- \
  --check docs/compatibility/kafka-api-capabilities.json
```

Expected: the manifest has exactly 19 sorted APIs, Metadata v4-v13, CreateTopics v4-v7, DescribeCluster v2, and DescribeTopicPartitions v0.

- [ ] **Step 2: Update public compatibility documentation**

In `README.md`:

- change the implemented API count from 17 to 19;
- list the four modern discovery windows exactly;
- state that topic UUIDs are stable for one process lifetime and intentionally change after restart;
- add Java 4.3.1 discovery/pagination to the black-box coverage summary;
- keep claims scoped to API compatibility, not Kafka configuration or distributed durability.

In `docs/kafka-api-parity-roadmap.md`:

- update the API inventory and capability count;
- mark Cut 3 delivered only now that Rust, wire, Java, and capability evidence pass;
- retain persistence, topic deletion, partition mutation, multi-broker behavior, and UUID data-plane APIs as later work.

- [ ] **Step 3: Run the local quality gate**

Run exactly:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
cargo run --quiet --example kafka_api_capabilities -- \
  --check docs/compatibility/kafka-api-capabilities.json
tests/api-versions/run.sh --check
git diff --check
```

Expected: every command exits `0`.

- [ ] **Step 4: Run the same black-box surfaces as hosted CI**

Follow `.github/workflows/verify.yml` exactly for the native/container .NET, Java, Go, Rust, Kafbat, protocol compatibility, and benchmark-smoke jobs. Do not invent a separate local orchestration path.

Expected: every suite passes and the Java test proves real DescribeCluster and paginated DescribeTopicPartitions use.

- [ ] **Step 5: Review the final diff for accidental scope**

Run:

```bash
git status --short
git diff --stat main...HEAD
git diff --check main...HEAD
```

Confirm there is no persistence layer, deletion behavior, replication claim, configuration expansion, generated protocol edit, or unrelated formatting churn.

- [ ] **Step 6: Commit docs and generated capability data**

```bash
git add README.md docs/kafka-api-parity-roadmap.md \
  docs/compatibility/kafka-api-capabilities.json
git commit -m "docs: publish modern discovery support"
```

## Completion Checklist

- [ ] Every real topic has one stable non-nil UUID for the process lifetime.
- [ ] Explicit and automatic creation update name and UUID indexes atomically.
- [ ] Metadata v4-v13 preserves older behavior and implements modern UUID semantics.
- [ ] CreateTopics v7 returns IDs only for real successful creation.
- [ ] DescribeCluster v2 reports the correct per-listener broker endpoint.
- [ ] DescribeTopicPartitions v0 matches Kafka 4.3.1 ordering and cursor behavior.
- [ ] All discovery APIs agree on topic, broker, controller, partition, and cluster identity.
- [ ] Java 4.3.1 follows the real multi-page path and reports the same UUIDs.
- [ ] Capability declarations, checked-in manifests, docs, and request evidence agree.
- [ ] Format, strict Clippy, full Rust tests, and hosted-equivalent black boxes are green.
