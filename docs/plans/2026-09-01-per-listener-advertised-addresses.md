# Per-Listener Advertised Addresses Implementation Plan

**Goal:** Serve clients that arrive over different networks by binding one Kafka listener per network and advertising the address that belongs to that listener.

**Architecture:** The advertised Kafka address becomes a property of the listener instead of the broker. `Cli` accepts repeatable `--kafka-listen` and `--kafka-advertised-address` options that pair by position and become `Config::kafka_listeners`. `server::serve` binds each configured listener, resolves its advertised address, and gives each listener its own `Dispatcher`. `BrokerState` no longer carries an advertised address; `Dispatcher` carries it and passes it to the two handlers that report broker coordinates, `metadata` and `find_coordinator`. `connection::serve` is unchanged because the dispatcher it already receives is per listener.

**Tech Stack:** Rust 1.98.0, Clap 4, Tokio, `kafka-protocol` 0.18.0, and the existing Rust wire-test harness.

**Spec:** [`../2026-08-26-memkafka-design.md`](../2026-08-26-memkafka-design.md), Sections 4 and 4.1.

## Global Constraints

- One `--kafka-listen` and at most one `--kafka-advertised-address` behave exactly as before, including the readiness log line.
- The advertised-address count must be zero or exactly the listener count; any other count is a fatal configuration error reported before readiness.
- With no advertised addresses, every listener advertises its own bound address, as a single listener does today.
- A client is answered with the advertised address of the listener its connection arrived on, in both Metadata and FindCoordinator responses.
- There is exactly one source of truth for the bound listeners; no primary field duplicating a collection entry.
- Keep `unsafe_code = "forbid"`, Rust 2024 edition, strict Clippy, and the pinned Rust 1.98.0 toolchain.

---

### Task 1: Repeatable listener configuration

**Files:**
- Modify: `src/config.rs`

**Interfaces:**
- Consumes: existing `Cli`, `Config`, `AdvertisedAddress`, and `ConfigError`.
- Produces: `config::KafkaListener { listen, advertised }` and `Config::kafka_listeners: Vec<KafkaListener>`, replacing `Config::kafka_listen` and `Config::kafka_advertised_address`.

- [x] **Step 1: Write the failing configuration tests**

Add positional-pairing, derive-every-address, and both count-mismatch cases, and restate the defaults test in terms of `kafka_listeners`.

- [x] **Step 2: Make both options repeatable**

Change `Cli::kafka_listen` to `Vec<SocketAddr>` keeping `default_value = "127.0.0.1:9092"`, and `Cli::kafka_advertised_address` to `Vec<String>`.

- [x] **Step 3: Pair by position in `TryFrom<Cli>`**

Reject a non-zero advertised count that differs from the listener count with `ConfigError::unpaired_advertised_addresses`, then zip the two lists into `Vec<KafkaListener>`.

### Task 2: Listener-owned advertised addresses

**Files:**
- Modify: `src/broker/mod.rs`, `src/kafka/dispatcher.rs`, `src/kafka/metadata.rs`, `src/kafka/find_coordinator.rs`, `src/kafka/fetch.rs`, `tests/kafka_wire.rs`

**Interfaces:**
- Consumes: `Config::kafka_listeners` from Task 1.
- Produces: a four-argument `BrokerState::new` without an advertised address, `Dispatcher::new(broker, advertised_kafka)`, and `metadata::response` / `find_coordinator::response` taking `&AdvertisedAddress`.

- [x] **Step 1: Move the field**

Drop `advertised_kafka` and its accessor from `BrokerState` and store it on `Dispatcher` instead.

- [x] **Step 2: Thread it into the two response handlers**

Pass `&self.advertised_kafka` from `Dispatcher::dispatch` into `metadata::response` and `find_coordinator::response`; both previously read it from `BrokerState`.

- [x] **Step 3: Update the test constructors**

Route the test `Dispatcher::new` call sites through one helper per test module so the advertised test address is declared once.

### Task 3: Bind every configured listener

**Files:**
- Modify: `src/server.rs`, `tests/runtime.rs`, `tests/kafka_wire.rs`

**Interfaces:**
- Consumes: Tasks 1 and 2.
- Produces: `server::BoundKafkaListener`, an encapsulated `BoundEndpoints` holding `Vec<BoundKafkaListener>` with `kafka()`, `advertised_kafka()`, `kafka_listeners()`, `primary_kafka()`, and `schema_registry()` accessors, and one Kafka listener task per configured listener.

- [x] **Step 1: Bind and resolve per listener**

Extract `bind_kafka_listener`, which binds one address and either uses its configured advertised address or derives one from the bound address.

- [x] **Step 2: Spawn one dispatcher per listener**

Build the broker once, then spawn `run_kafka_listener` per bound listener with `Dispatcher::new(broker.clone(), bound.advertised().clone())`.

- [x] **Step 3: Report every listener at readiness**

Emit the Kafka listen addresses and advertised addresses as comma-separated lists in listener order, which leaves the single-listener message byte-identical.

- [x] **Step 4: Prove the behavior over the wire**

Add `every_kafka_listener_advertises_its_own_address`, which binds two ephemeral listeners with distinct advertised names and asserts each connection's Metadata response returns its own listener's advertised host and port, plus `find_coordinator_v2_uses_the_listener_advertised_address` and `readiness_message_names_every_kafka_listener`.

### Task 4: Documentation and verification

**Files:**
- Modify: `README.md`, `docs/2026-08-26-memkafka-design.md`

- [x] **Step 1: Document the option pairing**

Mark both options repeatable in the CLI blocks, explain the pairing and the count rule, and lead the Aspire section with the two-listener topology while keeping the single shared-address pattern as an alternative.

- [x] **Step 2: Run all repository checks required by `AGENTS.md`**

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test
```

Expected: every command exits `0`.
