# Forced Consumer Topic Creation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an explicit opt-in mode that lets MemKafka auto-create named subscription topics even when a consumer sends `allow_auto_topic_creation=false`.

**Architecture:** Thread a `force_auto_create_topics` boolean from Clap configuration into `BrokerState`. The Metadata handler keeps its current Kafka-compatible gate by default, but in force mode it permits named auto-creation whenever server auto-creation is enabled; all-topics metadata requests remain read-only.

**Tech Stack:** Rust 1.98.0, Clap 4, Tokio, `kafka-protocol` 0.18.0, and the existing Rust wire-test harness.

**Spec:** [`../2026-08-26-memkafka-design.md`](../2026-08-26-memkafka-design.md), Sections 2, 4, 6, and 12.6.

## Global Constraints

- Preserve the default Kafka-compatible behavior: consumers that opt out do not create topics unless force mode is enabled.
- Force mode is effective only when `--auto-create-topics true`; it never overrides a server-level disable.
- A Metadata request with `topics=None` lists the catalog without mutation in every mode.
- Auto-created topics retain the configured default partition count, which defaults to exactly `2`.
- Do not change Produce-triggered auto-creation or explicit CreateTopics semantics.
- Keep `unsafe_code = "forbid"`, Rust 2024 edition, strict Clippy, and the pinned Rust 1.98.0 toolchain.

---

### Task 1: Configuration and broker-state plumbing

**Files:**
- Modify: `src/config.rs`
- Modify: `src/broker/mod.rs`
- Modify: `src/server.rs`
- Modify: `tests/kafka_wire.rs`

**Interfaces:**
- Consumes: existing `Cli`, `Config`, `BrokerState::new`, and server construction.
- Produces: `Config::force_auto_create_topics: bool`, `BrokerState::force_auto_create_topics() -> bool`, and a five-argument `BrokerState::new` carrying the new flag.

- [ ] **Step 1: Write failing configuration tests**

Extend `defaults_match_the_public_contract` and add an explicit parse test:

```rust
assert!(!config.force_auto_create_topics);

let config = Config::try_from(
    Cli::try_parse_from(["memkafka", "--force-auto-create-topics", "true"]).unwrap(),
)
.unwrap();
assert!(config.force_auto_create_topics);
```

- [ ] **Step 2: Run the configuration tests and verify RED**

Run: `cargo test config::tests --lib`

Expected: compilation fails because `Config` and `Cli` do not expose `force_auto_create_topics`.

- [ ] **Step 3: Add the CLI and `Config` field**

Add this field beside `auto_create_topics` in `Cli`:

```rust
#[arg(
    long,
    default_value_t = false,
    action = ArgAction::Set,
    value_parser = BoolishValueParser::new()
)]
force_auto_create_topics: bool,
```

Add `pub force_auto_create_topics: bool` to `Config` and copy the parsed value in `TryFrom<Cli>`.

- [ ] **Step 4: Thread the value through `BrokerState`**

Change the constructor and getter to:

```rust
pub fn new(
    broker_id: i32,
    advertised_kafka: AdvertisedAddress,
    auto_create_topics: bool,
    force_auto_create_topics: bool,
    default_partitions: NonZeroU32,
) -> Self

pub fn force_auto_create_topics(&self) -> bool {
    self.force_auto_create_topics
}
```

Pass `config.force_auto_create_topics` from `server::serve`. Update every `BrokerState::new` call in tests with `false` so existing behavior stays unchanged. Add a helper for later wire tests:

```rust
fn test_broker_state_with_force(
    auto_create_topics: bool,
    force_auto_create_topics: bool,
) -> BrokerState
```

- [ ] **Step 5: Verify configuration and existing wire tests GREEN**

Run:

```bash
cargo test config::tests --lib
cargo test --test kafka_wire metadata_
```

Expected: all selected tests pass; existing request opt-out behavior remains unchanged because helpers default force mode to `false`.

- [ ] **Step 6: Commit the plumbing**

```bash
git add src/config.rs src/broker/mod.rs src/server.rs tests/kafka_wire.rs
git commit -m "feat: configure forced topic creation"
```

### Task 2: Metadata force-mode behavior

**Files:**
- Modify: `src/kafka/metadata.rs`
- Modify: `tests/kafka_wire.rs`

**Interfaces:**
- Consumes: `BrokerState::auto_create_topics()` and `BrokerState::force_auto_create_topics()` from Task 1.
- Produces: named Metadata requests use `server_enabled && (request_enabled || force_enabled)` as their creation gate.

- [ ] **Step 1: Write the failing force-mode wire test**

Add `metadata_force_overrides_only_the_named_request_opt_out` with three literal cases:

```rust
let forced = test_broker_state_with_force(true, true);
let response = dispatch_metadata_request(
    &Dispatcher::new(forced.clone()),
    104,
    Some(vec!["forced-topic"]),
    false,
)
.await;
assert_eq!(response.topics[0].error_code, 0);
assert_eq!(response.topics[0].partitions.len(), 2);

let server_disabled = test_broker_state_with_force(false, true);
let response = dispatch_metadata_request(
    &Dispatcher::new(server_disabled.clone()),
    105,
    Some(vec!["still-disabled"]),
    false,
)
.await;
assert_eq!(response.topics[0].error_code, 3);
assert!(server_disabled.topics().list().await.is_empty());

let before = forced.topics().list().await.len();
let response = dispatch_metadata_request(
    &Dispatcher::new(forced.clone()),
    106,
    None,
    false,
)
.await;
assert_eq!(response.topics.len(), before);
assert_eq!(forced.topics().list().await.len(), before);
```

- [ ] **Step 2: Run the new test and verify RED**

Run: `cargo test --test kafka_wire metadata_force_overrides_only_the_named_request_opt_out`

Expected: FAIL because `forced-topic` returns `UnknownTopicOrPartition`.

- [ ] **Step 3: Implement the minimal Metadata gate**

In the named-topics branch of `metadata::response`, calculate:

```rust
let allow_auto_create = broker.auto_create_topics()
    && (request.allow_auto_topic_creation || broker.force_auto_create_topics());
```

Do not change the `topics=None` branch or the topic catalog API.

- [ ] **Step 4: Run focused and full Rust tests**

Run:

```bash
cargo test --test kafka_wire metadata_
cargo test --all-targets --all-features
```

Expected: all tests pass, including the pre-existing test that requires both flags when force mode is off.

- [ ] **Step 5: Commit the behavior**

```bash
git add src/kafka/metadata.rs tests/kafka_wire.rs
git commit -m "feat: force consumer topic creation"
```

### Task 3: Public CLI documentation

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: the shipped `--force-auto-create-topics <true|false>` option.
- Produces: accurate startup and compatibility documentation without implying it is Kafka's default behavior.

- [ ] **Step 1: Add the default and CLI option**

Add this Defaults row:

```markdown
| Force consumer topic creation | `false` |
```

Add `--force-auto-create-topics <true|false>` directly after `--auto-create-topics` in the CLI block.

- [ ] **Step 2: Document the semantic boundary**

Add this paragraph below the CLI block:

```markdown
`--force-auto-create-topics true` is an explicit integration-test convenience. When server auto-creation is enabled, it lets named consumer subscriptions create missing topics even if the client sends `allow_auto_topic_creation=false`. The default remains Kafka-compatible and honors the client opt-out.
```

- [ ] **Step 3: Check the documentation diff**

Run: `git diff --check && rg -n "force-auto-create-topics|Force consumer" README.md`

Expected: exit `0` and exactly the new option/default/explanation are present.

- [ ] **Step 4: Commit the documentation**

```bash
git add README.md
git commit -m "docs: explain forced topic creation"
```

### Task 4: Plan-level verification

**Files:**
- Verify only; no planned source changes.

**Interfaces:**
- Consumes: Tasks 1-3.
- Produces: an independently releasable forced-topic-creation slice ready for the pinned real-client acceptance plan.

- [ ] **Step 1: Run all repository checks required by `AGENTS.md`**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

Expected: every command exits `0`; TCP tests may require permission to bind ephemeral localhost ports.

- [ ] **Step 2: Confirm a clean diff for this slice**

Run: `git status --short && git log -3 --oneline`

Expected: no uncommitted files; the recent commits contain configuration, behavior, and documentation separately.
