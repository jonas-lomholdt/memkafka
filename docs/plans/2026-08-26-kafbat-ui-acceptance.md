# Kafbat UI black-box acceptance plan

**Goal:** Prove that a pinned released Kafbat UI discovers MemKafka and returns an exact record through Kafbat's public message-browsing API.

**Contract:** Follow §12.5 of [`../2026-08-26-memkafka-design.md`](../2026-08-26-memkafka-design.md). Kafbat's API response is the assertion; logs are diagnostics only.

## Steps

1. Pin the released `ghcr.io/kafbat/kafka-ui:v1.5.0@sha256:7cda86a33344160309fdb65146332e4da65db81a945614f2fe32e210803f6fd1` image and record the minimal environment configuration.
2. Run Kafbat against the current broker to discover the exact Kafka API gaps required for cluster/topic/message browsing.
3. For each required read-only API, add a failing protocol round-trip test, implement the smallest honest response, and advertise only the tested versions.
4. Add a deterministic black-box script that creates an isolated Docker network, starts MemKafka and Kafbat, waits for health, produces a unique key/value through a real client, and polls Kafbat's HTTP API until the exact record appears.
5. Add that script to GitHub Actions with always-on log capture and cleanup.
6. Run formatting, strict Clippy, the full Rust suite, and the Kafbat black-box test before committing.

## Guardrails

- Do not add broad admin compatibility just to make unrelated Kafbat screens work.
- Do not inspect MemKafka state directly from the test.
- Do not accept a healthy process or connection log as proof of message browsing.
- Keep all temporary container and network names unique and clean them on success or failure.

## Result

The live compatibility probe required only `ListGroups v0` and read-only `DescribeConfigs v1`. The packaged test uses an independent franz-go producer and asserts the exact key/value in Kafbat's SSE `MESSAGE` event.
