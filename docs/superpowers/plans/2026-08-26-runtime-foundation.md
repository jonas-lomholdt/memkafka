# MemKafka Runtime Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first runnable MemKafka executable with validated CLI configuration, both network listeners, one readiness event, and graceful shutdown.

**Architecture:** A small library owns configuration and server lifecycle so behavior can be tested without shelling out. The binary only parses CLI arguments, installs logging, supplies the OS shutdown signal, and reports fatal errors. Kafka and Schema Registry handlers remain intentionally skeletal in this phase; later vertical plans replace their connection/router boundaries without changing startup semantics.

**Tech Stack:** Rust 1.98.0 stable, Rust 2024 edition, Tokio, Axum, Clap, `tracing`, `tracing-subscriber`, and `anyhow`.

**Spec:** `docs/2026-08-26-memkafka-design.md`

## Global Constraints

- Pin Rust `1.98.0` in `rust-toolchain.toml`; declare `edition = "2024"` and `rust-version = "1.98"`.
- Forbid unsafe code with workspace lint `unsafe_code = "forbid"`.
- Default Kafka listen address: `127.0.0.1:9092`.
- Default Schema Registry listen address: `127.0.0.1:8081`.
- Default broker ID: `1`; auto-create topics: `true`; default partitions: `2`.
- The stable options are `--kafka-listen`, `--kafka-advertised-address`, `--schema-registry-listen`, `--auto-create-topics`, `--default-partitions`, `--log-level`, and `--quiet`.
- Bind both listeners before emitting readiness. Bind failure is fatal and must happen before readiness.
- `--quiet` suppresses informational output but not fatal startup errors.
- Never hold a shared-state lock over an `.await` point.
- This phase must not advertise Kafka APIs or Schema Registry routes that are not implemented yet.

## File Structure

- `Cargo.toml`: package metadata, dependencies, and strict workspace lints.
- `rust-toolchain.toml`: exact stable compiler pin and formatting/Clippy components.
- `src/lib.rs`: public module boundary used by tests and the executable.
- `src/config.rs`: CLI shape, advertised-address parsing, defaults, and validation.
- `src/server.rs`: dual-listener binding, readiness data, listener task ownership, and shutdown.
- `src/logging.rs`: one-time `tracing` subscriber initialization.
- `src/main.rs`: process boundary only: parse, log initialization, Ctrl-C, fatal exit.
- `tests/runtime.rs`: black-box network lifecycle tests against the real library server.
- `Dockerfile` and `.dockerignore`: native binary container packaging.
- `.github/workflows/ci.yml`: formatting, Clippy, tests, and image build.
- `README.md`: scope warning, startup commands, defaults, and current compatibility status.

---

### Task 1: Rust project and validated configuration

**Files:**
- Create: `Cargo.toml`
- Create: `rust-toolchain.toml`
- Create: `src/lib.rs`
- Create: `src/config.rs`

**Interfaces:**
- Consumes: command-line strings accepted by `clap::Parser`.
- Produces: `Cli`, `Config`, `LogLevel`, and `AdvertisedAddress`; `Config::try_from(Cli) -> Result<Config, ConfigError>`.

- [x] **Step 1: Create only the package manifest and compiler pin**

Use package name `memkafka`, version `0.1.0`, edition `2024`, and rust-version `1.98`. Add current compatible non-prerelease releases of `anyhow`, `axum`, `clap` with `derive`, `tokio` with `macros`, `net`, `rt-multi-thread`, `signal`, `sync`, and `time`, `tracing`, and `tracing-subscriber` with `env-filter`. Add `[workspace.lints.rust] unsafe_code = "forbid"`, then generate and commit `Cargo.lock`.

- [x] **Step 2: Write failing configuration tests**

In `src/config.rs`, add tests first:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn defaults_match_the_public_contract() {
        let config = Config::try_from(Cli::try_parse_from(["memkafka"]).unwrap()).unwrap();

        assert_eq!(config.kafka_listen, "127.0.0.1:9092".parse().unwrap());
        assert_eq!(
            config.schema_registry_listen,
            "127.0.0.1:8081".parse().unwrap()
        );
        assert_eq!(config.kafka_advertised_address, None);
        assert!(config.auto_create_topics);
        assert_eq!(config.default_partitions.get(), 2);
        assert_eq!(config.log_level, LogLevel::Info);
        assert!(!config.quiet);
    }

    #[test]
    fn zero_default_partitions_is_rejected() {
        let error = Cli::try_parse_from(["memkafka", "--default-partitions", "0"])
            .unwrap_err();

        assert!(error.to_string().contains("--default-partitions"));
    }

    #[test]
    fn advertised_address_accepts_a_dns_name() {
        let config = Config::try_from(
            Cli::try_parse_from([
                "memkafka",
                "--kafka-advertised-address",
                "broker:19092",
            ])
            .unwrap(),
        )
        .unwrap();

        assert_eq!(
            config.kafka_advertised_address,
            Some(AdvertisedAddress::new("broker", 19092).unwrap())
        );
    }
}
```

The production change caught by these tests is a broken public default, acceptance of an unusable zero partition count, or restricting advertised Kafka addresses to numeric IPs.

- [x] **Step 3: Run the configuration tests and verify RED**

Run: `cargo test config::tests --lib`

Expected: compilation fails because `Cli`, `Config`, `LogLevel`, and `AdvertisedAddress` do not exist.

- [x] **Step 4: Implement the minimal configuration types**

Implement `Cli` with Clap derives and private raw fields. Parse listen addresses as `SocketAddr`, partition count as `NonZeroU32`, and the advertised endpoint as a dedicated `AdvertisedAddress { host: String, port: u16 }` with `FromStr` support for DNS names, IPv4, and bracketed IPv6. Convert `Cli` into an immutable public `Config`. Keep an absent advertised endpoint as `None`; the runtime resolves that default after binding so port `0` tests work correctly.

Export modules from `src/lib.rs`:

```rust
#![forbid(unsafe_code)]

pub mod config;
pub mod logging;
pub mod server;
```

- [x] **Step 5: Run the configuration tests and verify GREEN**

Run: `cargo test config::tests --lib`

Expected: all configuration tests pass with no warnings.

- [x] **Step 6: Commit the configuration slice**

```bash
git add Cargo.toml Cargo.lock rust-toolchain.toml src/lib.rs src/config.rs
git commit -m "feat: add memkafka runtime configuration"
```

---

### Task 2: Dual-listener lifecycle

**Files:**
- Create: `src/server.rs`
- Create: `tests/runtime.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: `Config` and a shutdown future with `Output = ()`.
- Produces: `BoundEndpoints { kafka: SocketAddr, schema_registry: SocketAddr, advertised_kafka: AdvertisedAddress }`; `serve<F>(Config, oneshot::Sender<BoundEndpoints>, F) -> anyhow::Result<()>`.

- [ ] **Step 1: Write the failing dual-listener test**

Create `tests/runtime.rs`:

```rust
use std::time::Duration;

use memkafka::config::{Cli, Config};
use memkafka::server::serve;
use tokio::{net::TcpStream, sync::oneshot, time::timeout};

fn ephemeral_config() -> Config {
    use clap::Parser;

    Config::try_from(
        Cli::try_parse_from([
            "memkafka",
            "--kafka-listen",
            "127.0.0.1:0",
            "--schema-registry-listen",
            "127.0.0.1:0",
        ])
        .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn both_endpoints_accept_connections_until_shutdown() {
    let (ready_tx, ready_rx) = oneshot::channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(serve(ephemeral_config(), ready_tx, async {
        let _ = shutdown_rx.await;
    }));

    let endpoints = timeout(Duration::from_secs(1), ready_rx)
        .await
        .expect("server did not become ready")
        .expect("server stopped before readiness");

    TcpStream::connect(endpoints.kafka).await.unwrap();
    TcpStream::connect(endpoints.schema_registry).await.unwrap();
    assert_eq!(endpoints.advertised_kafka.port(), endpoints.kafka.port());

    shutdown_tx.send(()).unwrap();
    timeout(Duration::from_secs(1), server)
        .await
        .expect("server did not shut down")
        .unwrap()
        .unwrap();
}
```

The production change caught is failing to bind or drive either listener, resolving an omitted advertised port before an ephemeral bind, or leaking listener tasks on shutdown.

- [ ] **Step 2: Run the lifecycle test and verify RED**

Run: `cargo test --test runtime both_endpoints_accept_connections_until_shutdown`

Expected: compilation fails because `memkafka::server::serve` does not exist.

- [ ] **Step 3: Implement listener binding and structured shutdown**

In `src/server.rs`:

- bind the Kafka listener first and add context `failed to bind Kafka listener at {address}`;
- bind the Schema Registry listener second and add equivalent context;
- calculate `BoundEndpoints` from each listener's `local_addr()`;
- if no advertised address was supplied, derive it from the bound Kafka IP and actual port;
- create one `tokio::sync::watch` shutdown channel;
- run a Kafka accept loop and `axum::serve(TcpListener, Router::new())` in a `JoinSet`;
- send readiness only after both listeners are bound and both tasks have been created;
- select between the caller's shutdown future and the first unexpected server-task exit;
- broadcast shutdown and drain the `JoinSet` before returning;
- accept and close Kafka sockets for now, with a debug event clearly saying protocol dispatch is not installed yet.

Do not add a fake health route or claim any Kafka API support.

- [ ] **Step 4: Run the lifecycle test and verify GREEN**

Run: `cargo test --test runtime both_endpoints_accept_connections_until_shutdown`

Expected: PASS with no leaked-task warning.

- [ ] **Step 5: Write and verify a bind-failure test**

Add a test that reserves a local TCP address using `std::net::TcpListener`, configures Kafka to that address, calls `serve`, and asserts:

```rust
assert!(error.to_string().contains("failed to bind Kafka listener"));
assert!(ready_rx.await.is_err());
```

Run it before implementation changes to ensure it fails for the expected missing-context reason, then add the precise context and rerun it to PASS.

- [ ] **Step 6: Run all lifecycle tests**

Run: `cargo test --test runtime`

Expected: all runtime tests pass.

- [ ] **Step 7: Commit the lifecycle slice**

```bash
git add src/server.rs src/lib.rs tests/runtime.rs
git commit -m "feat: run kafka and registry listeners"
```

---

### Task 3: Process logging, readiness, and fatal exit behavior

**Files:**
- Create: `src/logging.rs`
- Create: `src/main.rs`
- Modify: `src/server.rs`
- Modify: `tests/runtime.rs`

**Interfaces:**
- Consumes: `LogLevel`, `quiet`, and `BoundEndpoints`.
- Produces: `logging::init(LogLevel, bool) -> anyhow::Result<()>`; `readiness_message(&BoundEndpoints) -> String`; executable exit status `0` after normal shutdown and `1` after runtime failure.

- [ ] **Step 1: Write the failing readiness-message test**

Add a literal assertion to `tests/runtime.rs` using fixed socket addresses:

```rust
#[test]
fn readiness_message_names_both_resolved_endpoints() {
    let endpoints = BoundEndpoints::for_test(
        "127.0.0.1:19092".parse().unwrap(),
        "127.0.0.1:18081".parse().unwrap(),
        AdvertisedAddress::new("broker", 19092).unwrap(),
    );

    assert_eq!(
        readiness_message(&endpoints),
        "MemKafka ready kafka=127.0.0.1:19092 schema_registry=http://127.0.0.1:18081 advertised_kafka=broker:19092"
    );
}
```

Prefer a normal public constructor over a test-only production method when implementing; the shown `for_test` name is shorthand in the test plan, not permission to put test-only lifecycle methods in production.

The production change caught is omitting or confusing the bound and advertised endpoints in the one readiness event.

- [ ] **Step 2: Run the readiness test and verify RED**

Run: `cargo test --test runtime readiness_message_names_both_resolved_endpoints`

Expected: compilation fails because `readiness_message` is missing.

- [ ] **Step 3: Implement the process boundary**

- Give `BoundEndpoints` a regular public constructor used by the test and the server.
- Add `readiness_message` with the exact literal format above.
- Initialize a compact `tracing_subscriber` using the configured level; force the maximum ordinary level to `WARN` when `quiet` is true.
- Emit readiness once at info level after both listener tasks exist.
- In `main`, parse with `Cli::parse()`, convert to `Config`, install logging, and call `serve` with `tokio::signal::ctrl_c()` mapped to `()`.
- On failure, print `memkafka: {error:#}` to stderr even under `--quiet` and return `ExitCode::FAILURE`.
- On normal shutdown, return `ExitCode::SUCCESS`.

- [ ] **Step 4: Run tests and verify GREEN**

Run: `cargo test --all-targets --all-features`

Expected: all tests pass with no warnings.

- [ ] **Step 5: Manually verify startup and shutdown at process level**

Run the binary with ephemeral ports and debug logging, observe exactly one readiness line, connect to both reported ports, then send Ctrl-C. Expected: exit status `0`, no panic, and no fatal output.

- [ ] **Step 6: Commit the process slice**

```bash
git add src/main.rs src/logging.rs src/server.rs tests/runtime.rs
git commit -m "feat: report readiness and graceful shutdown"
```

---

### Task 4: Container, CI, and truthful project documentation

**Files:**
- Create: `Dockerfile`
- Create: `.dockerignore`
- Create: `.github/workflows/ci.yml`
- Create: `README.md`

**Interfaces:**
- Consumes: the release-mode `memkafka` binary and its stable CLI.
- Produces: a non-root image with ports `9092` and `8081`, plus reproducible validation commands.

- [ ] **Step 1: Add the Docker packaging**

Use a multi-stage `rust:1.98.0-bookworm` builder, build with `cargo build --locked --release`, and copy only `/build/target/release/memkafka` from the builder into a maintained small runtime image. Create and select a non-root `memkafka` user. Use:

```dockerfile
EXPOSE 9092 8081
ENTRYPOINT ["/usr/local/bin/memkafka"]
CMD ["--kafka-listen", "0.0.0.0:9092", "--schema-registry-listen", "0.0.0.0:8081", "--kafka-advertised-address", "localhost:9092"]
```

Exclude `.git`, `target`, editor metadata, logs, and local environment files in `.dockerignore`.

- [ ] **Step 2: Add CI**

Create one workflow that runs on pushes and pull requests and executes:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
docker build -t memkafka:ci .
```

- [ ] **Step 3: Add the README**

Document native and Docker startup, all defaults and CLI options, memory-only state, the pinned Rust baseline, and the explicit warning that protocol compatibility is not implemented by this foundation phase yet. Link the design spec for the v0.1 target. Do not describe empty listener boundaries as a working Kafka broker or Schema Registry.

- [ ] **Step 4: Verify the complete foundation**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
docker build -t memkafka:local .
```

Expected: every command exits `0`; the runtime image reports a non-root user and contains no Rust toolchain or source tree.

- [ ] **Step 5: Commit packaging and documentation**

```bash
git add Dockerfile .dockerignore .github/workflows/ci.yml README.md
git commit -m "build: package memkafka runtime foundation"
```

## Plan Self-Review

- Spec coverage for this phase: compiler baseline, CLI defaults and validation, dual listeners, readiness, quiet/fatal behavior, graceful shutdown, Docker packaging, CI, and truthful README are covered.
- Deferred by explicit phase boundary: Kafka framing and APIs, broker state, Schema Registry routes/state, consumer groups, and real-client acceptance suites.
- Type consistency: `Config`, `AdvertisedAddress`, `BoundEndpoints`, `serve`, and `readiness_message` have one spelling and ownership boundary throughout.
- Placeholder scan: no unresolved markers or unspecified error/test step remains.
