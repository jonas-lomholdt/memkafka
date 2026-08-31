# Vendored `kafka-protocol` provenance

Upstream: https://github.com/kafka-protocol-rs/kafka-protocol-rs
Base commit: f0abe7eb99d3d54f48120bf1918623c71ba67cce
Kafka release: 4.3.1
Kafka commit: 26b251a451ce941d3d7a55e6487bcb7f16b5ad48

Local patches: offline schema input/output arguments, deterministic ordering/formatting,
version suffix 0.18.0-memkafka.1

Return condition: an upstream release contains Kafka 4.3 schemas and the allocation fix
and passes MemKafka's full suite.

Generated code is third-party Apache-2.0 input transformed by the MIT/Apache-2.0
generator.
