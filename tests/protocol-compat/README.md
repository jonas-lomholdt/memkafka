# Kafka protocol error compatibility

This oracle compares MemKafka with Apache Kafka's own `kafka-clients` error
responses for 27 adjacent unsupported versions across 18 APIs. Metadata v10 and
CreateTopics v7 left this matrix when they became supported; DescribeCluster v1
entered as the schema-known version below its v2 floor. When a supported window
already touches Kafka's schema minimum or maximum, its lower or upper neighbor
is outside the generated schema and cannot be encoded as a typed request.
DescribeTopicPartitions v0 is both its schema minimum and maximum, so neither
adjacent neighbor is schema-known. The harness also sends raw
unsupported ApiVersions v5 and v32767 requests to MemKafka and a live Kafka
broker, then compares the uniquely decoded response encodings. Ordinary clients
negotiate supported versions and therefore cannot exercise these paths.

The environment is pinned to Kafka clients 4.3.1, Java 25 in
`maven:3.9.11-eclipse-temurin-25`, and Kafka 4.3.1 at the digest in `run.sh`.
Build the locally owned MemKafka image, then run the oracle:

```bash
docker build -t memkafka:ci .
tests/protocol-compat/run.sh
```

Results default to `artifacts/protocol-compat`. Override that location with
`MEMKAFKA_PROTOCOL_ARTIFACT_DIR`. Each bounded phase has a positive-integer
override: `MEMKAFKA_PROTOCOL_IMAGE_PULL_TIMEOUT_SECONDS`,
`MEMKAFKA_PROTOCOL_INFRASTRUCTURE_TIMEOUT_SECONDS`,
`MEMKAFKA_PROTOCOL_READINESS_TIMEOUT_SECONDS`,
`MEMKAFKA_PROTOCOL_READINESS_PROBE_TIMEOUT_SECONDS`,
`MEMKAFKA_PROTOCOL_MAVEN_TIMEOUT_SECONDS`,
`MEMKAFKA_PROTOCOL_PROBE_TIMEOUT_SECONDS`,
`MEMKAFKA_PROTOCOL_DIFF_TIMEOUT_SECONDS`,
`MEMKAFKA_PROTOCOL_CLEANUP_TIMEOUT_SECONDS`, and
`MEMKAFKA_PROTOCOL_TERMINATION_GRACE_SECONDS`.

Diagnostics contain broker/Maven logs, normalized response evidence, and the
last command context. They contain no credentials, request payload bytes, record
values, or raw response bodies. The Maven log records any byte-equivalent
ApiVersions decoder candidates; normalized output uses their lowest canonical
body version. Flexible typed responses retain every generated top-level and
nested tagged-field map, including known tagged defaults, in deterministic
evidence. On failure, inspect the artifact path printed by the runner. Cleanup
targets only the runner's exact PID-suffixed containers, network, and validated
temporary directory; every removal command is bounded. The local MemKafka image
is inspected but never pulled or overwritten.
