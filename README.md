# MemKafka

[![CI](https://github.com/jonas-lomholdt/memkafka/actions/workflows/ci.yml/badge.svg)](https://github.com/jonas-lomholdt/memkafka/actions/workflows/ci.yml)

MemKafka is becoming a fast, single-binary, in-memory Kafka-compatible broker for local development and integration tests. The same process will also expose a Confluent-compatible Schema Registry API.

> **Current status:** topic discovery, topic creation, Produce, Fetch, ListOffsets, partition ordering, and repeat delivery from an uncommitted offset work through four independent clients: Confluent.Kafka 2.15.0, Apache Kafka Java 4.3.1, rskafka 0.6.0, and franz-go 1.21.6. Confluent.Kafka also passes single-member `Subscribe()`/`Consume()`, automatic and explicit offset commits, restart resume, and no-commit redelivery. Multi-member rebalancing and Schema Registry routes are next.

MemKafka is test infrastructure. It is not intended for production, and all state disappears when the process exits.

## Run locally

You need the Rust version pinned in `rust-toolchain.toml`.

```bash
cargo run
```

When both listeners are ready, MemKafka prints one line containing their resolved addresses.

## Run in Docker

```bash
docker build -t memkafka .
docker run --rm -p 9092:9092 -p 8081:8081 memkafka
```

The container runs as a non-root user and advertises Kafka at `localhost:9092` by default. Override the advertised address for Docker Compose or another container network:

```bash
docker run --rm \
  -p 9092:9092 \
  -p 8081:8081 \
  memkafka \
  --kafka-listen 0.0.0.0:9092 \
  --schema-registry-listen 0.0.0.0:8081 \
  --kafka-advertised-address memkafka:9092
```

## Defaults

| Setting | Default |
| --- | --- |
| Kafka endpoint | `127.0.0.1:9092` |
| Schema Registry | `http://127.0.0.1:8081` |
| Broker ID | `1` |
| Auto-create topics | `true` |
| Default partitions | `2` |
| Storage | Memory only |

## CLI

```text
--kafka-listen <host:port>
--kafka-advertised-address <host:port>
--schema-registry-listen <host:port>
--auto-create-topics <true|false>
--default-partitions <positive integer>
--log-level <error|warn|info|debug|trace>
--quiet
```

`--quiet` suppresses informational logs. Fatal startup errors still go to stderr.

## Compatibility target

The v0.1 target is an unmodified real `Confluent.Kafka` client plus Confluent's Avro Schema Registry integration. Compatibility will only be claimed after black-box tests pass against pinned real client versions.

The current black-box suite passes independently with Confluent.Kafka 2.15.0, Apache Kafka Java 4.3.1, pure-Rust rskafka 0.6.0, and pure-Go franz-go 1.21.6 for:

- `ApiVersions` negotiation;
- Metadata lookup and automatic topic creation with two default partitions;
- explicit topic creation with a caller-selected partition count;
- clear rejection of unsupported replication factors;
- acknowledged Produce to an explicit partition;
- Fetch from an earliest or manually selected offset;
- ten sequential records observed at contiguous offsets in partition order;
- a second read from offset `0` without a commit, demonstrating in-process at-least-once redelivery.

The Confluent.Kafka suite additionally proves a real classic-group Join/Sync/Heartbeat lifecycle with `cooperative-sticky`, automatic commit restart recovery, explicit commit restart recovery, and redelivery after restarting without a commit.

The broker advertises `Produce 3-7`, `Fetch 4`, `ListOffsets 1-3`, `Metadata 0-9`, `ApiVersions 0-4`, `CreateTopics 2-6`, `FindCoordinator 0-2`, `JoinGroup 0-5`, `SyncGroup 0-3`, `Heartbeat 0-3`, `LeaveGroup 0-3`, `OffsetCommit 2-7`, and `OffsetFetch 1-5`.

CI runs the Confluent.Kafka suite against both the native binary and its Docker image, then runs separate Java 25, Rust, and Go 1.27 suites against the image.

The remaining planned subset includes:

- multi-member classic consumer groups with cooperative-sticky rebalancing and session expiry;
- the Schema Registry endpoints needed by Confluent's Avro serializer and deserializer.
- a pinned Kafbat UI black-box test that discovers a topic and returns a produced record through Kafbat's message API.

It excludes persistence, replication, transactions, authentication, TLS, retention, topic deletion, Protobuf, and JSON Schema.

The at-least-once guarantee applies only to acknowledged records while the MemKafka process remains running. It intentionally permits duplicate delivery after an unknown Produce outcome and does not imply durability across shutdown. `acks=0` is supported but is outside that guarantee. Automatic and explicit commits retain consumer progress only until MemKafka exits.

See the [v0.1 design specification](docs/2026-08-26-memkafka-design.md) for the exact acceptance contract and exclusions.

## License

MemKafka is open source under the [MIT License](LICENSE). Copyright © 2026 Jonas Lomholdt.
