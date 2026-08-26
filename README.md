# MemKafka

MemKafka is becoming a fast, single-binary, in-memory Kafka-compatible broker for local development and integration tests. The same process will also expose a Confluent-compatible Schema Registry API.

> **Current status:** runtime foundation only. Both ports open, configuration and shutdown work, but Kafka protocol handling and Schema Registry routes are not implemented yet.

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

The planned subset includes:

- topic creation, metadata, Produce, Fetch, offsets, and modern RecordBatch storage;
- classic consumer groups with cooperative-sticky rebalancing;
- in-memory offset commits;
- the Schema Registry endpoints needed by Confluent's Avro serializer and deserializer.

It excludes persistence, replication, transactions, authentication, TLS, retention, topic deletion, Protobuf, and JSON Schema.

See the [v0.1 design specification](docs/2026-08-26-memkafka-design.md) for the exact acceptance contract and exclusions.
