# Schema Registry and Avro Acceptance Plan

**Goal:** Finish the v0.1 Confluent-compatible Avro Schema Registry slice and prove it with the real .NET client and serializer.

**Source of truth:** `docs/2026-08-26-memkafka-design.md`, sections 10 and 12.4.

## 1. Registry state (test first)

- Add an isolated, clonable in-memory registry guarded by one async read/write lock.
- Allocate global positive schema IDs and per-subject positive versions atomically.
- Deduplicate exact schema text within a subject, and reuse its global ID across subjects.
- Keep subjects deterministic and schemas independent from Kafka topics.
- Unit-test allocation, deduplication, version lookup, and concurrent registration.

## 2. Confluent REST contract (test first)

- Add the required `/subjects`, `/schemas/ids/{id}`, and `/config` routes.
- Accept absent `schemaType` or `AVRO`; reject other types and non-empty references.
- Return Confluent JSON shapes and error envelopes, including 40401/40402/40403 and validation errors.
- Test registration, lookup, latest/version reads, sorted lists, compatibility `NONE`, invalid versions, missing resources, and unsupported types through the Axum router.

## 3. Server integration

- Give the Schema Registry listener its own registry state and router.
- Preserve coordinated readiness and graceful shutdown for both listeners.
- Run the Rust unit, HTTP, and runtime suites.

## 4. Real Confluent Avro black box

- Pin `Confluent.SchemaRegistry` and `Confluent.SchemaRegistry.Serdes.Avro` to the existing 2.15.0 client line.
- Capture both native readiness endpoints and accept both external endpoint environment variables.
- Use `CachedSchemaRegistryClient` to register, deduplicate, list, and fetch schemas.
- Use `AvroSerializer<GenericRecord>` to publish through Kafka, inspect the Confluent wire ID, and use `AvroDeserializer<GenericRecord>` to recover the record.
- Assert missing-resource and unsupported-type errors through the HTTP boundary.

## 5. CI, docs, and completion

- Pass the container Schema Registry URL into the GitHub black-box job.
- Update README/spec implementation status and pinned acceptance coverage.
- Run formatting, strict Clippy, every Rust test, native and container Confluent acceptance, Java/Rust/Go clients, and Kafbat UI.
- Review the complete diff, address findings, and commit without pushing.
