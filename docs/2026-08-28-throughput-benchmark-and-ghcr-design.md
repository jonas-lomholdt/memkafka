# Throughput Benchmark and GHCR Publishing Design

## Purpose

Add two independent developer-facing capabilities without coupling either one to MemKafka's broker implementation:

1. a repeatable end-to-end throughput benchmark with a checked-in graph; and
2. release-time publication of the MemKafka container image to GitHub Container Registry.

Both additions must remain clean vertical cuts. The benchmark observes MemKafka only through its public Kafka protocol and command-line interface. Container publishing uses the existing `Dockerfile` and adds no release logic to the Rust binary.

## Non-goals

- Add benchmark-only code, dependencies, metrics, or hooks to the broker.
- Make performance a required CI threshold.
- Claim that results from one machine generalize to other machines.
- Compare MemKafka with Apache Kafka or another broker in this first benchmark.
- Benchmark compression, multiple payload profiles, startup time, or Schema Registry throughput.
- Use Criterion for this end-to-end workload.
- Publish an `edge` image for every commit to `main`.

## Repository boundaries

The benchmark is a standalone Rust workspace:

```text
benchmarks/throughput/
├── Cargo.toml
├── Cargo.lock
├── run.sh
└── src/
    └── main.rs
```

It has its own locked dependencies and does not join the root Cargo workspace. The root `Cargo.toml` gains no benchmark dependency. The benchmark may use the pinned `rskafka` client and ordinary operating-system process inspection, but it may not import the `memkafka` crate or access broker internals.

Published benchmark artifacts live separately:

```text
docs/benchmarks/
├── latest.json
└── throughput.svg
```

The benchmark binary writes machine-readable results. The wrapper owns local broker startup, shutdown, and peak-RSS sampling. SVG rendering is implemented inside the standalone benchmark crate so running it does not require Python, gnuplot, or another plotting tool.

Container publishing is isolated in `.github/workflows/publish.yml`. The existing broker build remains defined by the root `Dockerfile`.

## Benchmark workload

The default measured workload is:

- 1,000,000 records;
- an exactly 4 KiB JSON value per record, plus a key and small headers;
- 8 partitions with records distributed evenly and deterministically;
- explicit batches of 256 records, approximately 1 MiB of values per Produce request;
- no compression;
- one concurrent producer flow and one concurrent consumer flow per partition;
- acknowledged Produce requests;
- concurrent consumption starting before measured production;
- three independent measured runs, each using a fresh MemKafka process and topic.

Each JSON event contains deterministic identifiers, a partition-local sequence, a timestamp, representative business fields, and padding. The key selects its partition. Consumers validate the exact record count, contiguous offsets, payload size, embedded partition and sequence, and ordered delivery within every partition.

The release binary is built before timing starts. Connection setup and topic creation are also excluded. The producer timer begins immediately before the first measured Produce call. Producer completion is recorded when all Produce requests are acknowledged. End-to-end completion is recorded when all consumers have fetched and validated their assigned records.

Because MemKafka retains acknowledged records for the lifetime of the process, the default workload intentionally retains roughly 4 GiB of values plus protocol and allocation overhead. The wrapper must fail early with a clear message when the host does not have enough available memory rather than risking an unexplained out-of-memory failure.

## Metrics and results

Each measured run records:

- acknowledged producer elapsed time;
- producer records per second and GiB per second;
- end-to-end elapsed time;
- end-to-end records per second and GiB per second;
- peak broker resident-set size;
- total messages and value bytes;
- partition and batch configuration;
- broker commit;
- UTC timestamp;
- operating system and architecture;
- CPU model and logical core count;
- total host memory;
- Rust and benchmark-client versions.

`latest.json` contains every individual run and the median summary. The generated SVG shows producer and end-to-end throughput for all three runs plus their medians, and includes a compact annotation for payload size, partitions, peak RSS, machine, and commit.

The README embeds this SVG at the bottom and states that the numbers are a reproducible sample from one named machine, not a universal performance promise. It links to the raw result and the exact rerun command.

## Commands and failure behavior

The local wrapper provides one obvious command from the repository root:

```bash
benchmarks/throughput/run.sh
```

The wrapper:

1. builds MemKafka and the benchmark in release mode;
2. checks available memory;
3. starts a fresh broker on loopback ports;
4. waits for readiness with a bounded timeout;
5. runs the three measurements;
6. samples broker RSS while each measurement runs;
7. writes `latest.json` and `throughput.svg`; and
8. terminates the broker on success, failure, or interruption.

Configuration flags allow message count, run count, partitions, batch size, payload size, broker address, and output path to be overridden. Defaults remain the documented workload. Errors include enough context to identify the failed run, partition, offset, or process step. Partial results are never presented as a successful benchmark.

## CI smoke coverage

CI treats the benchmark as correctness tooling, not as a performance gate. It:

- checks formatting and strict Clippy for the standalone crate;
- runs its focused unit tests;
- starts MemKafka through the public container interface;
- executes one 10,000-record run with the same 4 KiB payload, partitioning, batching, concurrent consumption, and validation;
- requires a valid machine-readable result; and
- applies no throughput or timing threshold.

The million-record, three-run benchmark remains manual because shared GitHub runners are noisy and the workload retains more than 4 GiB in memory.

## GHCR publishing

`.github/workflows/publish.yml` publishes every push to `main` and canonical stable tags matching `vMAJOR.MINOR.PATCH`. It rejects every other ref before registry login.

For `refs/heads/main`, the workflow publishes a multi-platform OCI image for Linux AMD64 and ARM64 with:

- `ghcr.io/jonas-lomholdt/memkafka:edge`; and
- `ghcr.io/jonas-lomholdt/memkafka:sha-<short-commit>`.

For a canonical stable tag such as `v0.1.0`, the workflow publishes a multi-platform OCI image for Linux AMD64 and ARM64 with:

- `ghcr.io/jonas-lomholdt/memkafka:0.1.0`;
- `ghcr.io/jonas-lomholdt/memkafka:0.1`;
- `ghcr.io/jonas-lomholdt/memkafka:0`; and
- `ghcr.io/jonas-lomholdt/memkafka:latest`.

The publishing job runs only after its verification job succeeds. It uses the repository `GITHUB_TOKEN` with workflow-level `contents: read` and job-level `packages: write`, and does not require a personal access token. The image carries OCI source, description, revision, version, and MIT license metadata. `latest` remains release-only: `main` pushes move `edge` and the immutable `sha-<short-commit>` tag, but never `latest`.

The README documents anonymous usage:

```bash
docker pull ghcr.io/jonas-lomholdt/memkafka:latest
```

GitHub creates the first container package as private. After the first successful publication, the maintainer must perform the documented one-time package-settings change to make it public. Public visibility permits anonymous pulls and cannot be reverted to private, so the workflow must not attempt to hide that manual decision.

## Acceptance criteria

The work is complete when:

1. the benchmark remains a standalone workspace with no broker dependency or instrumentation;
2. the default command processes and validates 1,000,000 deterministic 4 KiB events over 8 partitions in three fresh-process runs;
3. output reports producer and end-to-end throughput plus peak broker RSS and full machine/workload metadata;
4. `latest.json` and `throughput.svg` are generated deterministically from successful runs and the README embeds the graph with honest caveats;
5. the 10,000-record smoke benchmark passes in CI without a performance threshold;
6. a separate release workflow verifies and publishes Linux AMD64 and ARM64 images from `main` as `edge` plus `sha-<short-commit>`, and from canonical stable tags as exact `major.minor.patch`, `major.minor`, `major`, and `latest` tags;
7. the image is linked to the repository and carries source, description, revision, version, and license metadata;
8. the README documents the GHCR pull command and one-time public-visibility step; and
9. formatting, strict Clippy, broker tests, benchmark tests, existing black-box suites, and hosted CI all pass.
