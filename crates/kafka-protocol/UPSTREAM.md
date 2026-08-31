# Vendored `kafka-protocol` provenance

## Pins

Upstream: https://github.com/kafka-protocol-rs/kafka-protocol-rs
Base commit: f0abe7eb99d3d54f48120bf1918623c71ba67cce
Local version: `0.18.0-memkafka.1`
Kafka release: 4.3.1
Kafka commit: 26b251a451ce941d3d7a55e6487bcb7f16b5ad48

Local patches add offline schema input/output arguments and deterministic ordering and formatting.
Generated code is third-party Apache-2.0 input transformed by the MIT/Apache-2.0 generator.

## Regeneration and upgrades

Official Kafka inputs live in `protocol_codegen/schema/kafka-4.3.1/`. Check generated output without changing it:

```bash
scripts/protocol/regenerate.sh --check
```

For an intentional regeneration, run `scripts/protocol/regenerate.sh`, review the generated diff, then run the checks below. For a Kafka upgrade, replace the schema directory only from one exact Apache Kafka tag and commit, preserve its `NOTICE`, update the pins here and in the schema `SOURCE.md`, then regenerate.

```bash
tests/protocol-vendor.sh
scripts/protocol/regenerate.sh --check
cargo fmt --manifest-path crates/kafka-protocol/Cargo.toml --all -- --check
cargo test --locked --manifest-path crates/kafka-protocol/Cargo.toml \
  --features broker,client,messages_enums
```

Return to crates.io only when an upstream release contains the required Kafka schemas and allocation fix and passes MemKafka's full suite.
