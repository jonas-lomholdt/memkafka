# Kafka Metadata and Topics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the real MemKafka TCP endpoint negotiate Kafka API versions, auto-create and describe topics, and create topics explicitly through an unmodified Confluent.Kafka client.

**Architecture:** The Kafka listener delegates length-delimited frames to a version-aware codec backed by generated `kafka-protocol` types. A dispatcher owns the advertised API matrix and calls small handlers over a concurrency-safe in-memory topic catalog. Each connection is isolated; malformed frames close only that connection, while valid requests return version-correct Kafka responses.

**Tech Stack:** Rust 1.98.0, Tokio, Bytes, `kafka-protocol` 0.18.0, Confluent.Kafka 2.15.0, and .NET 10.

**Spec:** `docs/2026-08-26-memkafka-design.md`

## Global Constraints

- Keep plans in `docs/plans/`.
- Use Rust `1.98.0`, edition `2024`, and forbid unsafe code in MemKafka source.
- Use `kafka-protocol = 0.18.0` generated types; do not hand-write message-body codecs.
- Enable only `broker`, `client`, and `messages_enums`; do not enable compression codecs in this phase.
- Advertise only live handlers: `ApiVersions 0-4`, `Metadata 0-9`, and `CreateTopics 2-6` by the end of this plan.
- Broker ID `1` is controller, leader, sole replica, and sole ISR member.
- Auto-created topics use `default_partitions`, which defaults to exactly `2`.
- Explicit topics require a positive partition count and replication factor `1`.
- No socket write or connection wait may hold a broker-state lock.
- Malformed frames and decode failures close only the offending connection.
- A feature is compatible only after the Confluent.Kafka 2.15.0 black-box runner passes.

## File Structure

- `src/kafka/frame.rs`: bounded Kafka frame reading and response writes.
- `src/kafka/codec.rs`: generated request-header/body decoding and response encoding.
- `src/kafka/dispatcher.rs`: live API registry, version validation, and handler routing.
- `src/kafka/connection.rs`: one connection request loop with connection-local failures.
- `src/kafka/mod.rs`: focused Kafka module exports.
- `src/broker/topics.rs`: topic validation and atomic catalog mutations.
- `src/broker/mod.rs`: broker identity, advertised endpoint, configuration, and shared catalog.
- `src/kafka/api_versions.rs`: API negotiation response.
- `src/kafka/metadata.rs`: metadata lookup and auto-creation mapping.
- `src/kafka/create_topics.rs`: explicit topic creation and Kafka error mapping.
- `src/server.rs`: inject broker state into the Kafka listener and own connection tasks.
- `tests/kafka_wire.rs`: real TCP frame/request/response tests.
- `tests/confluent/MemKafka.Acceptance.csproj`: pinned real-client runner project.
- `tests/confluent/Program.cs`: black-box Confluent.Kafka acceptance scenarios.
- `.github/workflows/ci.yml`: run the pinned real-client acceptance runner.
- `README.md`: report the exact implemented compatibility slice.

---

### Task 1: Bounded Kafka frames

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Create: `src/kafka/mod.rs`
- Create: `src/kafka/frame.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: an async byte stream containing Kafka's signed big-endian `int32` length prefix.
- Produces: `read_frame<R>(&mut R) -> Result<Option<Bytes>, FrameError>` and `write_frame<W>(&mut W, &[u8]) -> Result<(), FrameError>`.

- [ ] **Step 1: Add codec dependencies**

Add direct dependencies:

```toml
bytes = "1"
kafka-protocol = { version = "0.18.0", default-features = false, features = ["broker", "client", "messages_enums"] }
```

Also add Tokio's `io-util` feature for bounded async reads and writes. Keep the lockfile committed.

- [ ] **Step 2: Write failing frame tests**

Add tests in `src/kafka/frame.rs` that use `tokio::io::duplex` and hand-derived bytes:

```rust
#[tokio::test]
async fn reads_one_complete_frame() {
    let (mut client, mut server) = tokio::io::duplex(32);
    client.write_all(&[0, 0, 0, 3, 1, 2, 3]).await.unwrap();
    drop(client);

    assert_eq!(read_frame(&mut server).await.unwrap(), Some(Bytes::from_static(&[1, 2, 3])));
    assert_eq!(read_frame(&mut server).await.unwrap(), None);
}

#[test]
fn rejects_negative_and_oversized_lengths_before_allocating() {
    assert_eq!(decode_frame_length(-1).unwrap_err(), FrameError::InvalidLength(-1));
    assert_eq!(
        decode_frame_length((MAX_FRAME_SIZE + 1) as i32).unwrap_err(),
        FrameError::TooLarge(MAX_FRAME_SIZE + 1)
    );
}
```

The break caught is accepting invalid lengths, allocating attacker-controlled buffers, or confusing clean EOF with a truncated frame.

- [ ] **Step 3: Verify RED**

Run: `cargo test kafka::frame::tests --lib`

Expected: compilation fails because the frame functions and errors do not exist.

- [ ] **Step 4: Implement the minimal frame codec**

Set `MAX_FRAME_SIZE` to `100 * 1024 * 1024`. A clean EOF before any prefix byte returns `Ok(None)`; EOF inside a prefix/body returns `FrameError::Io` with `UnexpectedEof`. Validate length before allocating. `write_frame` enforces the same limit, writes prefix then body, and flushes.

- [ ] **Step 5: Verify GREEN and commit**

Run:

```bash
cargo test kafka::frame::tests --lib
cargo clippy --all-targets --all-features -- -D warnings
```

Commit:

```bash
git add Cargo.toml Cargo.lock src/lib.rs src/kafka
git commit -m "feat: add bounded kafka frame codec"
```

---

### Task 2: Version-aware envelopes and ApiVersions

**Files:**
- Create: `src/kafka/codec.rs`
- Create: `src/kafka/dispatcher.rs`
- Create: `src/kafka/api_versions.rs`
- Create: `src/kafka/connection.rs`
- Modify: `src/kafka/mod.rs`
- Modify: `src/server.rs`
- Create: `tests/kafka_wire.rs`

**Interfaces:**
- Produces: `DecodedRequest { header: RequestHeader, api_key: ApiKey, body: RequestKind }`.
- Produces: `encode_response(api_key: ApiKey, api_version: i16, correlation_id: i32, body: &ResponseKind) -> anyhow::Result<Bytes>`.
- Produces: `Dispatcher::dispatch(DecodedRequest) -> Result<ResponseKind, DispatchError>`.
- Initially advertises only `ApiVersions 0-4`.

- [ ] **Step 1: Write the failing ApiVersions codec test**

Encode an `ApiVersionsRequest` v3 with correlation ID `42` using the crate's client codec, pass it to `decode_request`, dispatch it, encode the response, and decode it using the crate's client response codec. Assert the response header correlation ID is `42` and its sole entry is literal `(api_key=18, min=0, max=4)`.

The break caught is wrong flexible-header selection, lost correlation IDs, or optimistic advertisement of unimplemented APIs.

- [ ] **Step 2: Verify RED**

Run: `cargo test --test kafka_wire api_versions_v3_round_trips_with_correlation_id`

Expected: compilation fails because the codec and dispatcher are missing.

- [ ] **Step 3: Implement envelope decoding and encoding**

Decode headers with `decode_request_header_from_buffer`, convert the key with `ApiKey::try_from`, decode via `RequestKind::decode`, and reject trailing bytes. Encode a `ResponseHeader` using `api_key.response_header_version(api_version)`, then `ResponseKind::encode`; the outer frame module adds the length prefix.

Implement `ApiVersionsResponse` with error code `0`, zero throttle, and one `ApiVersion` entry for the live handler. The dispatcher rejects an API/version pair not in its registry before decoding its handler semantics.

- [ ] **Step 4: Verify GREEN**

Run: `cargo test --test kafka_wire api_versions_v3_round_trips_with_correlation_id`

Expected: PASS.

- [ ] **Step 5: Write the failing TCP negotiation test**

Start `serve` on ephemeral ports, connect a real `TcpStream`, send the encoded v3 request frame, read one response frame, and assert correlation ID `73` plus the single advertised API range. Send a second request on the same socket to prove the connection loop persists.

- [ ] **Step 6: Implement connection isolation and verify**

Replace the current close-on-accept behavior with one spawned connection loop per socket. The loop reads one frame, decodes, dispatches, writes the response, and repeats. Connection errors log peer/api context and end only that task. The Kafka listener keeps accepting connections and drains its connection `JoinSet` during shutdown.

Run: `cargo test --test kafka_wire`

Expected: both envelope and TCP negotiation tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/kafka src/server.rs tests/kafka_wire.rs
git commit -m "feat: negotiate kafka api versions"
```

---

### Task 3: Atomic topic catalog

**Files:**
- Create: `src/broker/mod.rs`
- Create: `src/broker/topics.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- Produces: `TopicMetadata { name: String, partition_count: u32 }`.
- Produces: `TopicCatalog::create_explicit(name, partitions, replication_factor)`.
- Produces: `TopicCatalog::get_or_auto_create(name, allow_auto_create)` and `TopicCatalog::list()`.

- [ ] **Step 1: Write failing catalog behavior tests**

Test these literal outcomes:

- `events` with six partitions and replication factor `1` is created.
- repeating it returns `TopicError::AlreadyExists` without replacing metadata;
- zero partitions returns `InvalidPartitions`;
- replication factor `2` returns `InvalidReplicationFactor`;
- empty, `.`, `..`, names longer than 249 bytes, and names containing `/` return `InvalidName`;
- auto-creating the same name concurrently from 32 tasks yields one catalog entry with exactly the configured default partition count.

The break caught is non-atomic creation, invalid Kafka names entering state, or wrong explicit/default partition semantics.

- [ ] **Step 2: Verify RED**

Run: `cargo test broker::topics::tests --lib`

Expected: compilation fails because the broker catalog does not exist.

- [ ] **Step 3: Implement the catalog**

Store a `BTreeMap<String, TopicMetadata>` behind a Tokio `RwLock`. Validate before taking the write lock. Perform check-and-insert under one write guard and release it before returning. Topic names allow only ASCII alphanumerics, `.`, `_`, and `-`, except the two reserved dot names. `list()` returns deterministic name order.

- [ ] **Step 4: Verify GREEN and commit**

Run:

```bash
cargo test broker::topics::tests --lib
cargo clippy --all-targets --all-features -- -D warnings
```

Commit:

```bash
git add src/lib.rs src/broker
git commit -m "feat: add atomic topic catalog"
```

---

### Task 4: Metadata and automatic topic creation

**Files:**
- Create: `src/kafka/metadata.rs`
- Modify: `src/kafka/dispatcher.rs`
- Modify: `src/kafka/api_versions.rs`
- Modify: `src/kafka/connection.rs`
- Modify: `src/server.rs`
- Modify: `tests/kafka_wire.rs`

**Interfaces:**
- Adds live range `Metadata 0-9`.
- Consumes: `MetadataRequest`, `BrokerState`, broker ID `1`, and resolved advertised Kafka address.
- Produces: version-correct `MetadataResponse` values with topic/partition errors.

- [ ] **Step 1: Write the failing metadata tests**

At the handler level, request unknown topic `events` with auto-creation enabled and assert exactly two partitions numbered `0` and `1`, each with leader/replica/ISR `[1]`. Request an unknown topic with server auto-creation disabled and assert `UnknownTopicOrPartition` (`3`) with no catalog mutation. Request `topics=None` after creating two topics and assert deterministic complete listing.

At TCP level, send Metadata v9 and assert broker host/port equal the resolved advertised address, controller ID `1`, cluster ID `memkafka`, and two partition entries.

The break caught is incorrect auto-creation gating, metadata pointing clients at a bind-only address, or wrong single-broker topology.

- [ ] **Step 2: Verify RED**

Run: `cargo test --test kafka_wire metadata_v9_auto_creates_two_partitions`

Expected: the dispatcher rejects Metadata because it is not live.

- [ ] **Step 3: Implement metadata mapping**

Add `BrokerState` containing broker ID, advertised address, server auto-create flag, and the shared catalog. For named requests, auto-create only when both the server and request allow it. For `topics=None`, list without mutation. Build one broker entry, controller `1`, partition indexes `0..partition_count`, leader `1`, replica `[1]`, ISR `[1]`, no offline replicas, and non-internal topics. Map invalid requested names to `InvalidTopicException` (`17`).

- [ ] **Step 4: Verify GREEN and commit**

Run:

```bash
cargo test --test kafka_wire
cargo test --all-targets --all-features
```

Commit:

```bash
git add src/broker src/kafka src/server.rs tests/kafka_wire.rs
git commit -m "feat: serve kafka topic metadata"
```

---

### Task 5: Explicit topic creation

**Files:**
- Create: `src/kafka/create_topics.rs`
- Modify: `src/kafka/dispatcher.rs`
- Modify: `src/kafka/api_versions.rs`
- Modify: `tests/kafka_wire.rs`

**Interfaces:**
- Adds live range `CreateTopics 2-6`.
- Consumes: `CreateTopicsRequest` entries.
- Produces: per-topic `CreatableTopicResult` with Kafka error codes and clear messages.

- [ ] **Step 1: Write failing CreateTopics tests**

Through the real TCP codec, assert:

- creating `orders` with six partitions and replication factor `1` succeeds and later Metadata reports six partitions;
- repeating it returns `TopicAlreadyExists` (`36`);
- replication factor `2` returns `InvalidReplicationFactor` (`38`) and does not create the topic;
- partition count `0` returns `InvalidPartitions` (`37`);
- manual assignments return `InvalidReplicaAssignment` (`39`);
- custom configs return `InvalidConfig` (`40`);
- `validate_only=true` validates successfully without catalog mutation.

The break caught is partial mutation on invalid requests, unsupported options being silently accepted, or mismatch between CreateTopics and Metadata.

- [ ] **Step 2: Verify RED**

Run: `cargo test --test kafka_wire create_topics_v6_creates_six_partitions`

Expected: the dispatcher rejects CreateTopics because it is not live.

- [ ] **Step 3: Implement per-topic creation results**

Validate every entry before mutation. Use catalog errors to populate exact response codes and messages. For v5-v6 success, set `num_partitions`, replication factor `1`, and an empty config list. For `validate_only`, run the same validation and existing-name check without insertion. Process batched topics independently in request order.

- [ ] **Step 4: Verify GREEN and commit**

Run:

```bash
cargo test --test kafka_wire
cargo test --all-targets --all-features
cargo clippy --all-targets --all-features -- -D warnings
```

Commit:

```bash
git add src/kafka tests/kafka_wire.rs
git commit -m "feat: create kafka topics explicitly"
```

---

### Task 6: Four-client black-box acceptance

**Files:**
- Create: `tests/confluent/MemKafka.Acceptance.csproj`
- Create: `tests/confluent/Program.cs`
- Create: `tests/java/pom.xml`
- Create: `tests/java/src/test/java/io/memkafka/acceptance/KafkaJavaClientBlackBoxTest.java`
- Create: `tests/rust-client/Cargo.toml`
- Create: `tests/rust-client/tests/metadata.rs`
- Create: `tests/go-client/go.mod`
- Create: `tests/go-client/metadata_test.go`
- Modify: `.github/workflows/ci.yml`
- Modify: `README.md`

**Interfaces:**
- Consumes: only the compiled `memkafka` process endpoints through Confluent.Kafka 2.15.0, Apache Kafka Java 4.3.1, rskafka 0.6.0, and franz-go 1.21.6.
- Produces: a separate passing result from each client for negotiation, auto-creation, explicit creation, and rejection semantics.

- [ ] **Step 1: Add the pinned acceptance runner**

Create a `net10.0` console project with:

```xml
<PackageReference Include="Confluent.Kafka" Version="2.15.0" />
```

`Program.cs` starts `target/debug/memkafka` with ephemeral Kafka and Registry ports, reads the readiness line, and uses a real `AdminClient` configured only with `BootstrapServers` plus short test timeouts. It must:

1. fetch metadata for a new topic and assert exactly two partitions;
2. create a six-partition topic with replication factor `1` and assert six through metadata;
3. request replication factor `2` and assert `ErrorCode.InvalidReplicationFactor`;
4. print concise progress and return nonzero on any failed assertion;
5. terminate the child process in `finally`.

- [ ] **Step 2: Add the independent Java, Rust, and Go runners**

Pin Java 25 with Apache Kafka Java client 4.3.1, pure-Rust rskafka 0.6.0 on the repository Rust toolchain, and pure-Go franz-go 1.21.6 on Go 1.27. Each runner uses unique topic names and repeats the same three observable scenarios as the Confluent.Kafka runner without calling MemKafka internals.

- [ ] **Step 3: Verify the black-box runners fail before final wiring fixes**

Run:

```bash
cargo build
dotnet run --project tests/confluent/MemKafka.Acceptance.csproj
```

If a runner passes immediately, confirm it exercises the real binary by temporarily removing a required API from the advertised list and observing failure, then restore it. The Rust and Go suites must both fail when `CreateTopics` is hidden.

- [ ] **Step 4: Fix only evidence-backed interop gaps**

For every observed librdkafka mismatch, add a focused Rust wire regression test first, verify it fails for the same protocol reason, then make the smallest handler/version change. Do not advertise extra APIs to suppress client warnings.

- [ ] **Step 5: Add CI and truthful docs**

Install .NET 10, Java 25, and Go 1.27 in CI. Build MemKafka, run Confluent.Kafka against the native binary, build the Docker image, then run all four suites against the image. Update README status to list the four pinned clients and working APIs while stating Produce/Fetch/groups/Schema Registry remain unavailable.

- [ ] **Step 6: Final verification and commit**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
dotnet run --project tests/confluent/MemKafka.Acceptance.csproj
cargo test --locked --manifest-path tests/rust-client/Cargo.toml
(cd tests/go-client && go test -mod=readonly ./...)
docker build -t memkafka:local .
```

Commit:

```bash
git add tests .github/workflows/ci.yml README.md
git commit -m "test: verify metadata across four clients"
```

## Plan Self-Review

- Coverage: frame bounds, versioned headers, correlation IDs, truthful API negotiation, topic validation, atomic creation, metadata auto-creation, explicit creation, advertised endpoints, Kafka errors, four independent real-client suites, CI, and README are included.
- Deferred by phase boundary: Produce, Fetch, ListOffsets, partition logs, group coordination, and Schema Registry.
- Type consistency: `BrokerState`, `TopicCatalog`, `DecodedRequest`, `Dispatcher`, and the three live API ranges are named consistently across tasks.
- Placeholder scan: every behavior has a literal expected result and an executable verification command.
