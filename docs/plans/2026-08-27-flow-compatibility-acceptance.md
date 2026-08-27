# Self-Contained Flow Compatibility Acceptance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Permanently reproduce and verify the three application compatibility patterns with pinned Confluent.Kafka 2.13.2 and group-aware Kafbat tests, without depending on flow-v2 or Aspire.

**Architecture:** Add a dedicated black-box .NET runner that starts the real MemKafka binary in forced-topic mode, subscribes to absent topics with the consumer's default opt-out intact, and publishes through a genuinely idempotent producer. Run the acceptance once against the current baseline for RED, execute the three implementation plans, then wire the passing runner into CI and update public compatibility claims.

**Tech Stack:** .NET 10, Confluent.Kafka 2.13.2/librdkafka, Rust 1.98.0, Docker, GitHub Actions, and the existing Kafbat/Java/Rust/Go acceptance suites.

**Spec:** [`../2026-08-26-memkafka-design.md`](../2026-08-26-memkafka-design.md), Sections 12.5, 12.6, 13, and 14.

## Global Constraints

- This suite is self-contained: no flow-v2 source, Aspire process, external schemas, credentials, or application-specific configuration.
- Keep the existing Confluent.Kafka 2.15.0 + Avro suite unchanged and passing.
- Pin Confluent.Kafka exactly to `2.13.2` and commit its NuGet lock file.
- Leave consumer `AllowAutoCreateTopics` unset so librdkafka uses its consumer default `false`.
- Set producer `EnableIdempotence=true`; do not disable idempotence or replace the producer with raw protocol code.
- Require observable topic creation, group assignment, delivery reports, contiguous offsets, and ordered consume results.
- Keep all deadlines bounded and terminate the child broker in `finally`.
- Execute Task 1 before any production implementation to record the outer black-box RED result.

---

### Task 1: Add the pinned 2.13.2 black-box runner and record RED

**Files:**
- Create: `tests/flow-compat/MemKafka.FlowCompatibility.csproj`
- Create: `tests/flow-compat/Program.cs`
- Create: `tests/flow-compat/packages.lock.json` via restore

**Interfaces:**
- Consumes: only the compiled MemKafka binary and its readiness line.
- Produces: a process exit code that covers forced subscription-topic creation, classic group assignment, idempotent Produce, and ordered Fetch.

- [ ] **Step 1: Create the locked project**

Use:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net10.0</TargetFramework>
    <ImplicitUsings>enable</ImplicitUsings>
    <Nullable>enable</Nullable>
    <TreatWarningsAsErrors>true</TreatWarningsAsErrors>
    <RestorePackagesWithLockFile>true</RestorePackagesWithLockFile>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Confluent.Kafka" Version="2.13.2" />
  </ItemGroup>
</Project>
```

Run: `dotnet restore tests/flow-compat/MemKafka.FlowCompatibility.csproj`

Expected: restore succeeds and writes `packages.lock.json` pinning Confluent.Kafka 2.13.2 and its librdkafka runtime assets.

- [ ] **Step 2: Start the real broker with force mode**

Follow the existing Confluent runner's repository-root and process-output pattern. Start:

```csharp
startInfo.ArgumentList.Add("--kafka-listen");
startInfo.ArgumentList.Add("127.0.0.1:0");
startInfo.ArgumentList.Add("--schema-registry-listen");
startInfo.ArgumentList.Add("127.0.0.1:0");
startInfo.ArgumentList.Add("--auto-create-topics");
startInfo.ArgumentList.Add("true");
startInfo.ArgumentList.Add("--force-auto-create-topics");
startInfo.ArgumentList.Add("true");
```

Strip ANSI sequences before parsing `MemKafka ready kafka=...`, wait at most 10 seconds, and include captured stdout/stderr in timeout diagnostics.

- [ ] **Step 3: Exercise consumer subscription auto-creation**

Use four unique names carrying these recognizable prefixes:

```csharp
var topics = new[]
{
    $"edi-moves-inbound-{suffix}",
    $"rkem-moves-inbound-{suffix}",
    $"comet-itinerary-update-{suffix}",
    $"comet-moves-outbound-{suffix}",
};
```

Build the real consumer without assigning `AllowAutoCreateTopics`:

```csharp
var assigned = new TaskCompletionSource<IReadOnlyList<TopicPartition>>(
    TaskCreationOptions.RunContinuationsAsynchronously);
using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
{
    BootstrapServers = bootstrapServers,
    GroupId = $"flow-compat-{suffix}",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false,
    SessionTimeoutMs = 10_000,
    SocketTimeoutMs = 5_000,
})
    .SetPartitionsAssignedHandler((_, partitions) =>
    {
        assigned.TrySetResult(partitions);
    })
    .Build();
consumer.Subscribe(topics);
```

Call bounded `Consume` polls until `assigned.Task` completes. Through a real `AdminClient`, request metadata for every name and assert exactly two partitions per topic. Assert the assignment's distinct topic set equals the four literal names.

- [ ] **Step 4: Exercise an idempotent producer and ordered consumption**

Create one explicit single-partition topic. Build:

```csharp
using var producer = new ProducerBuilder<string, string>(new ProducerConfig
{
    BootstrapServers = bootstrapServers,
    EnableIdempotence = true,
    MessageTimeoutMs = 5_000,
    SocketTimeoutMs = 5_000,
})
    .Build();
```

Produce `value-0` through `value-9` sequentially to partition `0`, await every delivery, and assert delivery offsets `0..9`. Create a separate manually assigned consumer at offset `0`, consume ten records, and assert exact values and offsets in order.

- [ ] **Step 5: Add bounded cleanup and clear output**

Keep the subscription consumer alive until all scenarios complete so its group remains active. Close consumers in `finally`, kill the child process tree, await both output pumps, and print:

```text
PASS   Confluent.Kafka 2.13.2 forced subscriptions and idempotent produce/consume
```

- [ ] **Step 6: Build the runner**

Run:

```bash
dotnet restore --locked-mode tests/flow-compat/MemKafka.FlowCompatibility.csproj
dotnet build --no-restore tests/flow-compat/MemKafka.FlowCompatibility.csproj
```

Expected: build succeeds with `0 Warning(s)` and `0 Error(s)`.

- [ ] **Step 7: Run against the current baseline and verify RED**

Run:

```bash
cargo build
dotnet run --no-restore --project tests/flow-compat/MemKafka.FlowCompatibility.csproj
```

Expected before production work: failure because the force CLI flag is unknown, consumer topics remain unknown, or librdkafka reports that no broker supports idempotent producers. Record the first real failure in a short note at the bottom of this plan under `## Execution evidence`; do not weaken the test.

- [ ] **Step 8: Commit only the failing acceptance runner**

```bash
git add tests/flow-compat
git commit -m "test: reproduce flow compatibility gaps"
```

### Task 2: Execute the three implementation plans and make acceptance GREEN

**Files:**
- Follow the exact files in the three referenced plans.

**Interfaces:**
- Consumes: the RED runner from Task 1.
- Produces: forced named-topic creation, `DescribeGroups v0`, group-aware Kafbat, `InitProducerId v0`, and idempotent partition sequencing.

- [ ] **Step 1: Execute forced consumer-topic creation**

Execute every checkbox in [`2026-08-27-forced-consumer-topic-creation.md`](2026-08-27-forced-consumer-topic-creation.md) in order.

- [ ] **Step 2: Re-run the 2.13.2 runner**

Run: `dotnet run --no-restore --project tests/flow-compat/MemKafka.FlowCompatibility.csproj`

Expected at this intermediate point: the subscription/topic/group section passes, then the idempotent producer reports missing `InitProducerId` support. This proves the force-mode fix without weakening the remaining RED assertion.

- [ ] **Step 3: Execute DescribeGroups + Kafbat**

Execute every checkbox in [`2026-08-27-describe-groups-kafbat.md`](2026-08-27-describe-groups-kafbat.md) in order, including its active-group RED/GREEN Kafbat cycle.

- [ ] **Step 4: Execute idempotent production**

Execute every checkbox in [`2026-08-27-idempotent-production.md`](2026-08-27-idempotent-production.md) in order.

- [ ] **Step 5: Re-run the exact outer acceptance GREEN**

Run:

```bash
cargo build
env -u NO_COLOR CI=true GITHUB_ACTIONS=true \
  dotnet run --no-restore --project tests/flow-compat/MemKafka.FlowCompatibility.csproj
```

Expected: the exact PASS line appears and the process exits `0`, including under ANSI-colored GitHub-style logging.

### Task 3: Wire the flow profile into GitHub Actions

**Files:**
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: locked .NET runner from Task 1 and compiled MemKafka from the existing workflow.
- Produces: a required CI step that prevents regression of all three flow-profile behaviors.

- [ ] **Step 1: Add locked restore**

Immediately after the existing Confluent restore step, add:

```yaml
- name: Restore flow compatibility acceptance runner
  working-directory: tests/flow-compat
  run: dotnet restore --locked-mode
```

- [ ] **Step 2: Add native black-box execution**

Immediately after the existing native Confluent test, add:

```yaml
- name: Run Confluent.Kafka 2.13.2 flow compatibility tests
  run: >-
    dotnet run --no-restore
    --project tests/flow-compat/MemKafka.FlowCompatibility.csproj
```

The runner starts its own broker with force mode; do not point it at the later shared container, which is intentionally started with default topic semantics.

- [ ] **Step 3: Validate workflow syntax and local equivalent**

Run:

```bash
dotnet restore --locked-mode tests/flow-compat/MemKafka.FlowCompatibility.csproj
cargo build
env -u NO_COLOR CI=true GITHUB_ACTIONS=true \
  dotnet run --no-restore --project tests/flow-compat/MemKafka.FlowCompatibility.csproj
```

Expected: locked restore and acceptance both exit `0`.

- [ ] **Step 4: Commit CI wiring**

```bash
git add .github/workflows/ci.yml tests/flow-compat/packages.lock.json
git commit -m "ci: test the Confluent 2.13 flow profile"
```

### Task 4: Final public compatibility status

**Files:**
- Modify: `README.md`
- Modify: `docs/2026-08-26-memkafka-design.md`

**Interfaces:**
- Consumes: passing real-client, protocol, and Kafbat suites.
- Produces: public claims that exactly match shipped black-box coverage.

- [ ] **Step 1: Add the pinned flow-profile matrix row**

Add:

```markdown
| Confluent.Kafka flow profile (.NET) | 2.13.2 | ✅ forced subscriptions | ✅ idempotent | ✅ | — | — |
```

Keep the 2.15.0 row for Avro and full group coverage. In the legend, explain that `forced subscriptions` requires `--force-auto-create-topics true`.

- [ ] **Step 2: Update status and CI prose**

State that pinned 2.13.2 proves consumer subscription auto-creation in force mode and idempotent publish/consume. Update the CI paragraph to include this runner. Keep transactions and exactly-once processing explicitly excluded.

- [ ] **Step 3: Mark the approved spec extension implemented**

Change the spec status to `Implemented` and its implementation line to include forced consumer-topic creation, group-aware Kafbat, and non-transactional idempotent production. Do this only after every verification in Task 5 passes.

- [ ] **Step 4: Commit final documentation**

```bash
git add README.md docs/2026-08-26-memkafka-design.md
git commit -m "docs: report flow profile compatibility"
```

### Task 5: Full repository and black-box verification

**Files:**
- Verify only; edit only if a test exposes a real defect, and use a fresh failing regression before its fix.

**Interfaces:**
- Consumes: every prior task and plan.
- Produces: completion evidence matching local checks and the GitHub workflow.

- [ ] **Step 1: Run formatting, lint, and Rust tests**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

Expected: all commands exit `0`.

- [ ] **Step 2: Run both native .NET suites**

Run:

```bash
dotnet restore --locked-mode tests/confluent/MemKafka.Acceptance.csproj
dotnet restore --locked-mode tests/flow-compat/MemKafka.FlowCompatibility.csproj
dotnet build --no-restore tests/confluent/MemKafka.Acceptance.csproj
dotnet build --no-restore tests/flow-compat/MemKafka.FlowCompatibility.csproj
env -u NO_COLOR CI=true GITHUB_ACTIONS=true \
  dotnet run --no-restore --project tests/confluent/MemKafka.Acceptance.csproj
env -u NO_COLOR CI=true GITHUB_ACTIONS=true \
  dotnet run --no-restore --project tests/flow-compat/MemKafka.FlowCompatibility.csproj
```

Expected: both PASS lines appear with zero warnings/errors.

- [ ] **Step 3: Run container and cross-client suites**

Run the same order as CI:

```bash
docker build --tag memkafka:ci .
docker run --detach --rm --name memkafka-final \
  --publish 127.0.0.1:19092:9092 \
  --publish 127.0.0.1:18081:8081 \
  memkafka:ci \
  --kafka-listen 0.0.0.0:9092 \
  --schema-registry-listen 0.0.0.0:8081 \
  --kafka-advertised-address 127.0.0.1:19092
MEMKAFKA_BOOTSTRAP_SERVERS=127.0.0.1:19092 \
MEMKAFKA_SCHEMA_REGISTRY_URL=http://127.0.0.1:18081 \
  dotnet run --no-restore --project tests/confluent/MemKafka.Acceptance.csproj
MEMKAFKA_BOOTSTRAP_SERVERS=127.0.0.1:19092 \
  mvn --batch-mode --no-transfer-progress --file tests/java/pom.xml test
MEMKAFKA_BOOTSTRAP_SERVERS=127.0.0.1:19092 \
  cargo test --locked --manifest-path tests/rust-client/Cargo.toml
(cd tests/go-client && MEMKAFKA_BOOTSTRAP_SERVERS=127.0.0.1:19092 go test -count=1 -mod=readonly ./...)
docker rm --force memkafka-final
docker build --file tests/kafbat/Dockerfile.seed --tag memkafka-kafbat-seed:ci .
tests/kafbat/run.sh
```

Expected: all four clients, Avro, and active-group Kafbat pass. If `memkafka-final` already exists, remove only that exact container before retrying.

- [ ] **Step 4: Check repository state**

Run:

```bash
git diff --check
git status --short --branch
```

Expected: no uncommitted changes and branch `main` is ahead only by the reviewed implementation commits.

## Execution evidence

Record the initial real-client RED command and its first protocol failure here during Task 1, then append the final GREEN command and PASS line during Task 2. Do not paste transient secrets, full broker logs, or application data.
