# Kafka API Capability Registry and Version Evidence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace MemKafka's scattered API-version declarations with one capability registry, prove the current-client version floor with repeatable black-box evidence, and reject wire versions below that floor without retaining legacy-only ranges.

**Architecture:** A standalone Rust TCP recorder under `tests/api-versions/` observes Kafka request headers without modifying either broker. It runs each pinned client scenario through a digest-pinned Apache Kafka 4.3.1 broker and stores deterministic evidence. MemKafka's runtime uses one static Rust capability registry for both `ApiVersions` and dispatch gating; a generated JSON snapshot makes the same data reviewable from the docs.

**Tech Stack:** Rust 1.98.0, Tokio, `kafka-protocol` 0.18.0, Clap, Serde JSON, Bash, jq, Docker, Apache Kafka 4.3.1, Confluent.Kafka 2.15.0 and 2.13.2, Apache Kafka Java 4.3.1, rskafka 0.6.0, franz-go 1.21.6, and Kafbat UI 1.5.0.

**Spec:** [`../2026-08-26-memkafka-design.md`](../2026-08-26-memkafka-design.md), Section 12.7; [`../kafka-api-parity-roadmap.md`](../kafka-api-parity-roadmap.md), “Protocol version support policy,” “Recommended first implementation sequence” cut 1, and “Testing and compatibility strategy.”

## Global Constraints

- Keep this plan under `docs/plans/` and implementation files in ordinary repository paths.
- Pin the Kafka oracle to `apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`.
- Treat Kafka 4.3 as the target ceiling. This cut records the ceiling but does not advertise versions that handlers do not implement.
- Keep one contiguous advertised window per API. The floor is the lowest version needed by the pinned current-client scenarios; versions below it are outside the compatibility contract.
- Preserve the explicit Confluent.Kafka 2.13.2 flow-profile lane. Do not add older clients to lower a floor.
- Do not add a recorder flag, observation file, proxy state, or new dependency to the shipped MemKafka server.
- Keep the recorder as an independent test crate so it cannot enter the MemKafka binary or Docker image.
- Store deterministic evidence: no timestamps, container IDs, random ports, correlation IDs, or request counts in checked-in JSON.
- Do not raise an implemented ceiling or add a Kafka API in this cut. Those remain separate behavior-first vertical slices.
- Keep existing black-box behavior green: delivery, ordering, redelivery, groups, commits, idempotence, Schema Registry, Avro, and Kafbat browsing.

---

### Task 1: Build the transparent Kafka request-version recorder

**Files:**
- Create: `tests/api-versions/proxy/Cargo.toml`
- Create: `tests/api-versions/proxy/Cargo.lock`
- Create: `tests/api-versions/proxy/src/main.rs`
- Create: `tests/api-versions/proxy/tests/forwarding.rs`
- Create: `tests/api-versions/proxy/Dockerfile`

**Interfaces:**
- Consumes client TCP connections plus an upstream Kafka address.
- Forwards Kafka bytes unchanged in both directions.
- Writes one JSON Lines record per valid client request.

```text
kafka-api-version-proxy \
  --listen 127.0.0.1:0 \
  --upstream 127.0.0.1:19092 \
  --scenario confluent-kafka-2.15.0 \
  --output /tmp/confluent-kafka-2.15.0.jsonl
```

```json
{"scenario":"confluent-kafka-2.15.0","apiKey":18,"apiVersion":3,"clientId":"rdkafka"}
```

- [ ] **Step 1: Write failing forwarding and parsing tests**

Use an ephemeral fake upstream listener. Cover fragmented requests, exact request forwarding, exact response forwarding, null client IDs, concurrent connections, and short/invalid request headers. Invalid headers must still be forwarded but must not create observations.

Construct request frames from the common Kafka header:

```text
INT32 frame_length
INT16 api_key
INT16 api_version
INT32 correlation_id
NULLABLE_STRING client_id
request body bytes
```

Split the length prefix and body across several writes in the fragmentation test. Assert exact upstream bytes and parse the single JSON line into a typed `Observation`.

- [ ] **Step 2: Run and verify RED**

```bash
cargo test --manifest-path tests/api-versions/proxy/Cargo.toml
```

Expected: compilation fails because the recorder crate and types do not exist.

- [ ] **Step 3: Implement bounded frame forwarding**

Use one task for client-to-broker framed forwarding and one for broker-to-client `tokio::io::copy`. The request path reads the four-byte length, rejects negative lengths, caps allocation at 100 MiB, parses only the common header, appends one JSON line, and forwards the original prefix and payload without rewriting either.

```rust
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct Observation<'a> {
    scenario: &'a str,
    api_key: i16,
    api_version: i16,
    client_id: Option<&'a str>,
}

fn parse_request_header(frame: &[u8]) -> Option<(i16, i16, Option<&str>)>;
```

Serialize writes behind one `tokio::sync::Mutex` so concurrent connections cannot interleave JSON lines. Flush after each line so failed scenarios still leave diagnostics.

Bind the listener before connecting upstream, accept port `0`, set `SO_REUSEADDR`, and print exactly `READY listen=<ip:port>` after binding. The orchestrator uses that address as Kafka's advertised listener; upstream connections open lazily when clients connect. Add a test that closes the recorder and immediately binds a new recorder to the same address.

- [ ] **Step 4: Add the isolated container image**

Add an empty `[workspace]` table to the recorder manifest so Cargo treats it as an independent crate beneath the root workspace. Build only that crate and copy only its executable into the runtime stage. Pin the builder to the repository toolchain. The runtime image does not contain MemKafka.

- [ ] **Step 5: Run recorder quality gates GREEN**

```bash
cargo fmt --manifest-path tests/api-versions/proxy/Cargo.toml -- --check
cargo clippy --locked --manifest-path tests/api-versions/proxy/Cargo.toml --all-targets -- -D warnings
cargo test --locked --manifest-path tests/api-versions/proxy/Cargo.toml
docker build -f tests/api-versions/proxy/Dockerfile -t memkafka-api-version-proxy:test .
```

Expected: all tests pass and the image contains only the recorder executable.

- [ ] **Step 6: Commit the recorder**

```bash
git add tests/api-versions/proxy
git commit -m "test: add Kafka API version recorder"
```

### Task 2: Make every pinned scenario runnable against the Kafka oracle

**Files:**
- Modify: `tests/confluent/Program.cs`
- Modify: `tests/flow-compat/Program.cs`
- Modify: `tests/go-client/cmd/kafbat-seed/main.go`
- Create: `tests/go-client/cmd/kafbat-seed/main_test.go`
- Create: `tests/api-versions/kafbat.sh`

**Interfaces:**
- `MEMKAFKA_KAFKA_ONLY=true`: the Confluent.Kafka 2.15.0 runner executes Kafka scenarios but skips Schema Registry/Avro.
- `MEMKAFKA_API_VERSION_PROBE=true`: the 2.13.2 runner uses `MEMKAFKA_BOOTSTRAP_SERVERS` and pre-creates subscription topics before subscribing.
- `MEMKAFKA_KAFBAT_STRING_ONLY=true`: the Kafbat seed runs without Schema Registry variables.
- Java, Go, and Rust suites run unchanged against Kafka configured with two default partitions, auto-creation, and one broker.

- [ ] **Step 1: Demonstrate the current external-oracle failures**

With Kafka available through `127.0.0.1:19093`, run:

```bash
MEMKAFKA_BOOTSTRAP_SERVERS=127.0.0.1:19093 MEMKAFKA_KAFKA_ONLY=true \
  dotnet run --no-restore --project tests/confluent/MemKafka.Acceptance.csproj

MEMKAFKA_BOOTSTRAP_SERVERS=127.0.0.1:19093 MEMKAFKA_API_VERSION_PROBE=true \
  dotnet run --no-restore --project tests/flow-compat/MemKafka.FlowCompatibility.csproj
```

Expected: the primary runner reaches unavailable Schema Registry work, and the flow runner starts its own MemKafka.

- [ ] **Step 2: Add the primary .NET Kafka-only branch**

Keep every Kafka scenario unchanged. Gate only Schema Registry/Avro:

```csharp
var kafkaOnly = Environment.GetEnvironmentVariable("MEMKAFKA_KAFKA_ONLY")
    ?.Equals("true", StringComparison.OrdinalIgnoreCase) == true;

if (!kafkaOnly)
{
    await AssertSchemaRegistryAndAvro(admin, bootstrapServers, schemaRegistryUrl);
}
```

The normal MemKafka lane must still execute Avro.

- [ ] **Step 3: Add the flow-profile external-broker branch**

When probe mode is true, require `MEMKAFKA_BOOTSTRAP_SERVERS`, do not launch or terminate MemKafka, and create the four subscription topics before `Subscribe`. Keep group assignment, idempotent production, ordered consumption, and exact offset assertions unchanged. The default branch must still start MemKafka with forced auto-creation.

- [ ] **Step 4: Add string-only Kafbat seeding**

Require Schema Registry and Avro variables only when string-only mode is false. String-only mode creates and produces the string topic and keeps the same consumer group alive. Extract parsing behind a function that accepts a lookup closure, then unit-test both modes without mutating process-global environment variables.

- [ ] **Step 5: Add the Kafka-oracle Kafbat scenario**

`tests/api-versions/kafbat.sh` creates one Docker network and starts:

1. Kafka 4.3.1 at `kafka:19092`, advertising `api-version-proxy:9092`;
2. the recorder at `api-version-proxy:9092`, forwarding to Kafka with scenario `kafbat-1.5.0`;
3. Kafbat 1.5.0 pointing at the recorder without Schema Registry;
4. the seed image in string-only mode.

Assert Kafbat reports `ONLINE`, exposes the active group, lists the topic, and returns the exact string message. Preserve broker, proxy, UI, and seed logs on failure.

- [ ] **Step 6: Verify oracle and normal branches GREEN**

Run the two .NET commands from Step 1, plus:

```bash
MEMKAFKA_BOOTSTRAP_SERVERS=127.0.0.1:19093 mvn --batch-mode --file tests/java/pom.xml test
MEMKAFKA_BOOTSTRAP_SERVERS=127.0.0.1:19093 cargo test --locked --manifest-path tests/rust-client/Cargo.toml
(cd tests/go-client && MEMKAFKA_BOOTSTRAP_SERVERS=127.0.0.1:19093 go test -count=1 -mod=readonly ./...)
```

Run `tests/api-versions/kafbat.sh` with locally built recorder and seed images. Then rerun the existing MemKafka black-box commands unchanged.

- [ ] **Step 7: Commit oracle-compatible scenarios**

```bash
git add tests/confluent/Program.cs tests/flow-compat/Program.cs \
  tests/go-client/cmd/kafbat-seed tests/api-versions/kafbat.sh
git commit -m "test: run pinned clients against Kafka 4.3"
```

### Task 3: Capture and normalize pinned-client request evidence

**Files:**
- Create: `tests/api-versions/run.sh`
- Create: `docs/compatibility/kafka-4.3-client-requests.json`
- Modify: `.gitignore`

**Interfaces:**
- `tests/api-versions/run.sh --check` captures to a temporary directory and compares with checked-in evidence.
- `tests/api-versions/run.sh --update` deliberately replaces checked-in evidence.
- Raw diagnostics default to `artifacts/api-versions/` and remain gitignored.

- [ ] **Step 1: Write a failing evidence-schema check**

Require these stable top-level fields:

```json
{
  "schemaVersion": 1,
  "kafkaBaseline": {
    "version": "4.3.1",
    "image": "apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837"
  },
  "scenarios": []
}
```

Each scenario contains `id`, `client`, `version`, and sorted `requests`. Each request contains numeric `apiKey` and sorted unique `versions`. Normalized evidence excludes counts and client IDs.

- [ ] **Step 2: Run and verify RED**

```bash
tests/api-versions/run.sh --check
```

Expected: FAIL because orchestration and evidence do not exist.

- [ ] **Step 3: Implement host-client orchestration**

Start one combined-mode Kafka container with:

```text
process.roles=broker,controller
node.id=1
num.partitions=2
auto.create.topics.enable=true
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
```

Publish Kafka's real listener to a validated unused loopback port. Start the first recorder before Kafka with `--listen 127.0.0.1:0`, parse its `READY` line, and configure Kafka to advertise that bound recorder address. The recorder connects to Kafka lazily after the broker becomes ready. For each later host scenario, restart the recorder on the same advertised port, run the client, stop it, and append its JSON Lines file. Use exactly these IDs:

```text
confluent-kafka-2.15.0
confluent-kafka-flow-2.13.2
apache-kafka-java-4.3.1
rskafka-0.6.0
franz-go-1.21.6
kafbat-1.5.0
```

Use traps with exact PIDs and container names. Do not kill by an unscoped process pattern.

- [ ] **Step 4: Normalize deterministically**

Use jq to group by scenario/API key, sort scenarios and keys, sort/deduplicate versions, inject pinned metadata, and render two-space-indented JSON with a trailing newline. `--check` uses a temporary file plus `cmp`; `--update` replaces only the evidence file.

- [ ] **Step 5: Capture the initial evidence**

```bash
tests/api-versions/run.sh --update
tests/api-versions/run.sh --check
```

Expected: the second run is byte-identical. Review unexpected keys/versions; never delete evidence merely because MemKafka lacks the API.

- [ ] **Step 6: Commit the evidence lane**

```bash
git add .gitignore tests/api-versions/run.sh docs/compatibility/kafka-4.3-client-requests.json
git commit -m "test: capture current-client Kafka API versions"
```

### Task 4: Centralize runtime API capabilities

**Files:**
- Create: `src/kafka/capabilities.rs`
- Create: `examples/kafka_api_capabilities.rs`
- Create: `docs/compatibility/kafka-api-capabilities.json`
- Modify: `src/kafka/mod.rs`
- Modify: `src/kafka/api_versions.rs`
- Modify: `src/kafka/dispatcher.rs`
- Modify: every handler under `src/kafka/` that declares `VERSION_RANGE`
- Modify: `tests/kafka_wire.rs`

**Interfaces:**
- Consumes one static `CAPABILITIES` slice.
- Produces the runtime `ApiVersions` list, dispatch version gating, and a checked-in JSON snapshot.

- [ ] **Step 1: Write failing registry tests**

Add tests for ordering, uniqueness, non-empty windows, and containment inside Kafka 4.3:

```rust
assert!(CAPABILITIES.windows(2).all(|pair| {
    pair[0].api_key as i16 < pair[1].api_key as i16
}));
assert!(CAPABILITIES.iter().all(|api| api.supported.min <= api.supported.max));
assert!(CAPABILITIES.iter().all(|api| {
    api.kafka_4_3.min <= api.supported.min
        && api.supported.max <= api.kafka_4_3.max
}));
assert_eq!(api_versions_response_ranges(), registry_supported_ranges());
```

Exercise both accepted boundaries and the immediately adjacent rejected versions:

```rust
for capability in CAPABILITIES {
    assert!(capability.supports(capability.supported.min));
    assert!(capability.supports(capability.supported.max));
    assert!(!capability.supports(capability.supported.min - 1));
    assert!(!capability.supports(capability.supported.max + 1));
}
```

- [ ] **Step 2: Run and verify RED**

```bash
cargo test kafka::capabilities --lib
```

Expected: compilation fails because the registry does not exist.

- [ ] **Step 3: Implement the registry types**

Use compact copyable values rather than `RangeInclusive`:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct VersionWindow {
    pub(crate) min: i16,
    pub(crate) max: i16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ApiCapability {
    pub(crate) api_key: ApiKey,
    pub(crate) name: &'static str,
    pub(crate) supported: VersionWindow,
    pub(crate) kafka_4_3: VersionWindow,
    pub(crate) proof_scenarios: &'static [&'static str],
}

pub(crate) fn capability(api_key: ApiKey) -> Option<&'static ApiCapability>;
pub fn manifest_json() -> Result<String, serde_json::Error>;
```

Define `CAPABILITIES` with the 17 currently advertised ranges from `src/kafka/api_versions.rs`, sorted by numeric key, plus the Kafka 4.3 targets and proof scenarios listed in Task 5. Keep it as the sole source of supported MemKafka versions. Handler modules no longer declare `VERSION_RANGE`. Expose the module as `#[doc(hidden)] pub mod capabilities` so the example can call `manifest_json()` without exposing the internal table or structs. Task 5 narrows only the current floors after this refactor is green.

- [ ] **Step 4: Generate `ApiVersions` from the registry**

Replace the manual vector with:

```rust
ApiVersionsResponse::default().with_api_keys(
    CAPABILITIES
        .iter()
        .map(|capability| {
            ApiVersion::default()
                .with_api_key(capability.api_key as i16)
                .with_min_version(capability.supported.min)
                .with_max_version(capability.supported.max)
        })
        .collect(),
)
```

- [ ] **Step 5: Gate dispatch once from the registry**

At the start of `Dispatcher::dispatch`, resolve the capability and reject missing keys or out-of-window versions. Remove all 17 duplicated `require_version` calls and the local helper. Preserve handler matching and `BodyMismatch` checks.

```rust
let version = request.header.request_api_version;
let Some(capability) = capabilities::capability(request.api_key) else {
    return Err(DispatchError::UnsupportedApi(request.api_key));
};
if !capability.supports(version) {
    return Err(DispatchError::UnsupportedVersion {
        api_key: request.api_key,
        version,
    });
}
```

- [ ] **Step 6: Add deterministic capability rendering**

The example accepts exactly one mode and path:

```text
cargo run --example kafka_api_capabilities -- --check docs/compatibility/kafka-api-capabilities.json
cargo run --example kafka_api_capabilities -- --update docs/compatibility/kafka-api-capabilities.json
```

The JSON contains baseline `4.3`, all 17 advertised APIs, numeric key, name, supported window, Kafka target window, and sorted proof scenario IDs. `--check` performs a byte comparison. `--update` writes atomically through a sibling temporary file.

- [ ] **Step 7: Run focused tests GREEN**

```bash
cargo test kafka::capabilities --lib
cargo test --test kafka_wire api_versions
cargo run --quiet --example kafka_api_capabilities -- \
  --update docs/compatibility/kafka-api-capabilities.json
cargo run --quiet --example kafka_api_capabilities -- \
  --check docs/compatibility/kafka-api-capabilities.json
```

Expected: one registry generates the advertised response and stable snapshot.

- [ ] **Step 8: Commit the registry**

```bash
git add src/kafka/api_versions.rs src/kafka/capabilities.rs \
  src/kafka/create_topics.rs src/kafka/describe_configs.rs \
  src/kafka/describe_groups.rs src/kafka/dispatcher.rs src/kafka/fetch.rs \
  src/kafka/find_coordinator.rs src/kafka/heartbeat.rs \
  src/kafka/init_producer_id.rs src/kafka/join_group.rs \
  src/kafka/leave_group.rs src/kafka/list_groups.rs \
  src/kafka/list_offsets.rs src/kafka/metadata.rs src/kafka/mod.rs \
  src/kafka/offset_commit.rs src/kafka/offset_fetch.rs src/kafka/produce.rs \
  src/kafka/sync_group.rs examples/kafka_api_capabilities.rs \
  docs/compatibility/kafka-api-capabilities.json tests/kafka_wire.rs
git commit -m "refactor: centralize Kafka API capabilities"
```

### Task 5: Enforce the current-client floor

**Files:**
- Modify: `src/kafka/capabilities.rs`
- Modify: `tests/kafka_wire.rs`
- Regenerate: `docs/compatibility/kafka-api-capabilities.json`

**Supported windows for this cut:**

| API | Supported | Kafka 4.3 target | Floor evidence |
| --- | ---: | ---: | --- |
| Produce | 7 | 3-13 | rskafka and higher-level clients negotiate v7 at the present ceiling. |
| Fetch | 4 | 4-18 | Present implementation and rskafka use v4. |
| ListOffsets | 3 | 1-11 | rskafka 0.6.0 supports through v3. |
| Metadata | 4-9 | 0-13 | rskafka 0.6.0 supports through v4. |
| OffsetCommit | 7 | 2-10 | Current .NET group scenarios negotiate v7. |
| OffsetFetch | 5 | 1-10 | Current .NET group scenarios negotiate v5. |
| FindCoordinator | 2 | 0-6 | Current group scenarios negotiate v2. |
| JoinGroup | 5 | 0-9 | Current cooperative scenarios negotiate v5. |
| Heartbeat | 3 | 0-4 | Current group scenarios negotiate v3. |
| LeaveGroup | 3 | 0-5 | Current group scenarios negotiate v3. |
| SyncGroup | 3 | 0-5 | Current cooperative scenarios negotiate v3. |
| DescribeGroups | 0 | 0-6 | Kafbat's current proven implementation is v0. |
| ListGroups | 0 | 0-5 | Kafbat's current proven implementation is v0. |
| ApiVersions | 3-4 | 0-4 | rskafka/librdkafka use v3; Kafka Java can use v4. |
| CreateTopics | 5-6 | 2-7 | rskafka 0.6.0 supports through v5. |
| InitProducerId | 0 | 0-6 | The proven non-transactional allocation path is v0. |
| DescribeConfigs | 1 | 1-4 | The proven Kafbat path is v1. |

- [ ] **Step 1: Change expected ranges and verify RED**

Update both `ApiVersions` tests to assert this table before changing the registry. Add direct dispatcher cases for one version immediately below every raised floor:

```rust
assert_eq!(
    dispatcher.dispatch(&request_at(ApiKey::Metadata, 3)).await,
    Err(DispatchError::UnsupportedVersion {
        api_key: ApiKey::Metadata,
        version: 3,
    }),
);
```

Use a table of `(ApiKey, rejected_version)` cases and one deliberately mismatched default `RequestKind`: the version gate must run before body matching. Keep the test below the TCP layer because response-aware unsupported-version handling belongs to roadmap cut 2.

- [ ] **Step 2: Run and verify RED**

```bash
cargo test --test kafka_wire api_versions
cargo test --test kafka_wire rejects_versions_below_current_client_floor
```

Expected: the response assertions fail because the registry still has old floors.

- [ ] **Step 3: Apply the exact supported windows**

Change only `supported.min` values. Keep every current `supported.max` and every Kafka 4.3 target range unchanged.

- [ ] **Step 4: Prove focused tests GREEN**

```bash
cargo test --test kafka_wire api_versions
cargo test --test kafka_wire rejects_versions_below_current_client_floor
cargo test --test kafka_wire
```

- [ ] **Step 5: Prove every pinned MemKafka client remains GREEN**

Run the normal acceptance commands from `.github/workflows/verify.yml`:

```bash
dotnet run --no-restore --project tests/confluent/MemKafka.Acceptance.csproj
dotnet run --no-restore --project tests/flow-compat/MemKafka.FlowCompatibility.csproj
mvn --batch-mode --file tests/java/pom.xml test
cargo test --locked --manifest-path tests/rust-client/Cargo.toml
(cd tests/go-client && go test -count=1 -mod=readonly ./...)
tests/kafbat/run.sh
```

Expected: every pinned client negotiates successfully. A failure means that API floor is not proven; inspect evidence and correct only that floor.

- [ ] **Step 6: Regenerate and check the artifact**

```bash
cargo run --quiet --example kafka_api_capabilities -- \
  --update docs/compatibility/kafka-api-capabilities.json
cargo run --quiet --example kafka_api_capabilities -- \
  --check docs/compatibility/kafka-api-capabilities.json
```

- [ ] **Step 7: Commit the enforced floor**

```bash
git add src/kafka/capabilities.rs tests/kafka_wire.rs \
  docs/compatibility/kafka-api-capabilities.json
git commit -m "refactor: enforce current-client Kafka API floors"
```

### Task 6: Add capability and version-drift gates to CI

**Files:**
- Modify: `.github/workflows/verify.yml`

- [ ] **Step 1: Add independent recorder crate gates**

Before the main crate formatting step, add:

```yaml
- name: Check API version recorder formatting
  run: cargo fmt --manifest-path tests/api-versions/proxy/Cargo.toml -- --check

- name: Run Clippy on API version recorder
  run: >-
    cargo clippy --locked
    --manifest-path tests/api-versions/proxy/Cargo.toml
    --all-targets
    -- -D warnings

- name: Test API version recorder
  run: cargo test --locked --manifest-path tests/api-versions/proxy/Cargo.toml
```

- [ ] **Step 2: Check the generated capability snapshot**

After main Rust tests, add:

```yaml
- name: Check Kafka API capability snapshot
  run: >-
    cargo run --quiet --example kafka_api_capabilities --
    --check docs/compatibility/kafka-api-capabilities.json
```

- [ ] **Step 3: Run the Kafka 4.3 request-evidence lane**

After .NET, Java, Go, and Rust dependencies are ready but before the MemKafka container starts, build the recorder/seed images and run:

```yaml
- name: Verify pinned-client Kafka API versions
  env:
    MEMKAFKA_API_VERSION_LOG_DIR: artifacts/api-versions
  run: tests/api-versions/run.sh --check
```

Reuse installed runtimes and restored caches. Do not add a second workflow that can drift from full verification.

- [ ] **Step 4: Upload evidence diagnostics**

Add an `if: always()` artifact upload for `artifacts/api-versions`. Retain raw JSON Lines and container/client logs, but never commit them.

- [ ] **Step 5: Run workflow-equivalent checks locally**

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
cargo fmt --manifest-path tests/api-versions/proxy/Cargo.toml -- --check
cargo clippy --locked --manifest-path tests/api-versions/proxy/Cargo.toml --all-targets -- -D warnings
cargo test --locked --manifest-path tests/api-versions/proxy/Cargo.toml
tests/api-versions/run.sh --check
```

Expected: generated capability data and live request evidence match checked-in files.

- [ ] **Step 6: Commit CI enforcement**

```bash
git add .github/workflows/verify.yml
git commit -m "ci: guard Kafka API capability drift"
```

### Task 7: Publish the policy and verify the shipped artifact

**Files:**
- Modify: `README.md`
- Modify: `docs/kafka-api-parity-roadmap.md`
- Modify: `docs/2026-08-26-memkafka-design.md`

- [ ] **Step 1: Update public compatibility ranges**

Replace old advertised ranges in README and the roadmap inventory with Task 5's windows. Label the separate Kafka 4.3 target so a narrow current implementation is not mistaken for parity.

- [ ] **Step 2: Link machine-readable evidence**

From README's compatibility section, link:

- `docs/compatibility/kafka-api-capabilities.json` for advertised/target windows and proof scenarios;
- `docs/compatibility/kafka-4.3-client-requests.json` for requests observed from pinned clients against Kafka 4.3.1.

Explain briefly that client upgrades changing the evidence fail CI and require an explicit compatibility review.

- [ ] **Step 3: Mark roadmap cut 1 accurately**

State that cut 1 provides request-version evidence and a central registry. Do not call the 17 APIs complete: Kafka 4.3 ceilings and semantic gaps remain future slices.

- [ ] **Step 4: Run final repository verification**

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
cargo run --quiet --example kafka_api_capabilities -- \
  --check docs/compatibility/kafka-api-capabilities.json
tests/api-versions/run.sh --check
```

Then run the full native/container/Kafbat black-box sequence from `.github/workflows/verify.yml`.

- [ ] **Step 5: Verify the production binary stays lean**

```bash
docker build -t memkafka:capability-registry .
docker run --rm --entrypoint /bin/sh memkafka:capability-registry \
  -c 'wc -c /usr/local/bin/memkafka'
```

Expected: the recorder is absent, production dependencies are unchanged, and the binary remains in the existing approximate 7.7-8.3 MiB architecture-dependent range.

- [ ] **Step 6: Scan structure and text**

```bash
find . -type d \( -name superpowers -o -name .superpowers \) -print
rg -n "TO[D]O|TB[D]|PLACEHOLDE[R]" \
  README.md docs src tests .github examples
git status --short
```

Expected: no placeholders, prohibited directory references, or unintended changes.

- [ ] **Step 7: Commit documentation**

```bash
git add README.md docs/2026-08-26-memkafka-design.md \
  docs/kafka-api-parity-roadmap.md
git commit -m "docs: publish Kafka API version evidence"
```

- [ ] **Step 8: Push and verify GitHub Actions when authorized**

```bash
git push origin main
gh run list --branch main --limit 5
gh run watch --exit-status
```

Expected: verification and edge-image publication are green. This cut does not create a release tag.
