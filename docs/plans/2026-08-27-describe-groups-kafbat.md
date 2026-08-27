# DescribeGroups and Group-Aware Kafbat Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the smallest truthful `DescribeGroups` API and prove Kafbat UI remains online while an active classic consumer group exists.

**Architecture:** Add a read-only group-description snapshot to the existing coordinator and adapt it into Kafka `DescribeGroups v0` responses. Strengthen the Kafbat Docker test by keeping a franz-go consumer group active while Kafbat refreshes cluster state and browses the seeded message.

**Tech Stack:** Rust 1.98.0, Tokio, `kafka-protocol` 0.18.0, Go 1.27 with franz-go 1.21.6, Docker, Kafbat UI v1.5.0, Bash, curl, and jq.

**Spec:** [`../2026-08-26-memkafka-design.md`](../2026-08-26-memkafka-design.md), Sections 6, 8, 12.5, and 12.6.

## Global Constraints

- Advertise only `DescribeGroups v0`; later versions remain unsupported until a real client requires them.
- Descriptions are read-only point-in-time snapshots and preserve request order.
- Report actual group state, selected protocol, member subscription metadata, and member assignment bytes.
- Unknown IDs return `GroupIdNotFound` (`69`) and do not create groups.
- Sort members by member ID so wire tests and diagnostics are deterministic.
- `client_host` is the empty string in v0 because connection ownership is intentionally not part of group membership; do not refactor the connection layer solely for this display field.
- Keep Kafbat pinned to its existing digest and keep its API response—not logs—as the black-box assertion.

---

### Task 1: Read-only coordinator descriptions

**Files:**
- Modify: `src/broker/groups.rs`

**Interfaces:**
- Consumes: existing `Group`, `Member`, `GroupState`, opaque `JoinProtocol::metadata`, and `Group::assignments`.
- Produces: `GroupCoordinator::describe(&self, group_id: &str) -> Option<GroupDescription>` plus `GroupDescription` and `GroupMemberDescription` value types.

- [ ] **Step 1: Write failing coordinator tests**

Add a test that creates and stabilizes one group through the real `join` and `sync` methods, then asserts these literal fields:

```rust
let description = coordinator
    .describe("described-group")
    .await
    .expect("group exists");
assert_eq!(description.group_id, "described-group");
assert_eq!(description.state, "Stable");
assert_eq!(description.protocol_type, "consumer");
assert_eq!(description.protocol_name, "cooperative-sticky");
assert_eq!(description.members.len(), 1);
assert_eq!(description.members[0].client_id, "describe-client");
assert_eq!(description.members[0].metadata, Bytes::from_static(b"subscription"));
assert_eq!(description.members[0].assignment, Bytes::from_static(b"assignment"));
assert!(coordinator.describe("missing-group").await.is_none());
```

The setup calls `join` until it returns `JoinResult::Joined`, then calls `sync` as leader with `SyncAssignment { member_id, assignment: Bytes::from_static(b"assignment") }`; it does not construct private `Group` state directly.

- [ ] **Step 2: Run the coordinator test and verify RED**

Run: `cargo test broker::groups::tests::describes_stable_group_without_mutating_it --lib`

Expected: compilation fails because `describe` and description types do not exist.

- [ ] **Step 3: Add description value types**

Add these types next to `GroupSummary`:

```rust
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GroupDescription {
    pub(crate) group_id: String,
    pub(crate) state: &'static str,
    pub(crate) protocol_type: String,
    pub(crate) protocol_name: String,
    pub(crate) members: Vec<GroupMemberDescription>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GroupMemberDescription {
    pub(crate) member_id: String,
    pub(crate) client_id: String,
    pub(crate) metadata: Bytes,
    pub(crate) assignment: Bytes,
}
```

Map states exactly as `Empty`, `PreparingRebalance`, `CompletingRebalance`, and `Stable`.

- [ ] **Step 4: Implement the point-in-time snapshot**

Start with:

```rust
let group = self.group(group_id).await?;
let mut group = group.lock().await;
expire_stale_members(&mut group, Instant::now(), group_id, None);
```

For each member, select metadata from the protocol whose name equals `group.selected_protocol`; use empty bytes when no protocol is selected. Read the assignment from `group.assignments`, also defaulting to empty bytes, then sort by `member_id`. Do not call `group_or_insert`, `begin_rebalance`, or mutate assignments.

- [ ] **Step 5: Run group tests GREEN**

Run: `cargo test broker::groups::tests --lib`

Expected: all group tests pass, including stable and missing-group assertions.

- [ ] **Step 6: Commit the coordinator slice**

```bash
git add src/broker/groups.rs
git commit -m "feat: expose consumer group descriptions"
```

### Task 2: `DescribeGroups v0` protocol handler

**Files:**
- Create: `src/kafka/describe_groups.rs`
- Modify: `src/kafka/mod.rs`
- Modify: `src/kafka/api_versions.rs`
- Modify: `src/kafka/dispatcher.rs`
- Modify: `tests/kafka_wire.rs`

**Interfaces:**
- Consumes: `GroupCoordinator::describe` from Task 1 and generated `DescribeGroupsRequest`/`DescribeGroupsResponse` types.
- Produces: `describe_groups::VERSION_RANGE = 0..=0` and `describe_groups::response(&DescribeGroupsRequest, &BrokerState) -> DescribeGroupsResponse`.

- [ ] **Step 1: Write failing API-matrix and response tests**

Update both API-version tests from `15` to `16` entries and add:

```rust
assert_api_range(&response, ApiKey::DescribeGroups, 0, 0);
```

Add a wire test that stabilizes `described-group`, requests it followed by `missing-group`, and asserts:

```rust
assert_eq!(response.groups.len(), 2);
assert_eq!(response.groups[0].error_code, 0);
assert_eq!(response.groups[0].group_id.as_str(), "described-group");
assert_eq!(response.groups[0].group_state.as_str(), "Stable");
assert_eq!(response.groups[0].protocol_type.as_str(), "consumer");
assert_eq!(response.groups[0].protocol_data.as_str(), "cooperative-sticky");
assert_eq!(response.groups[0].members[0].member_metadata, Bytes::from_static(b"subscription"));
assert_eq!(response.groups[0].members[0].member_assignment, Bytes::from_static(b"assignment"));
assert_eq!(response.groups[1].error_code, ResponseError::GroupIdNotFound.code());
assert_eq!(response.groups[1].group_id.as_str(), "missing-group");
```

- [ ] **Step 2: Run the tests and verify RED**

Run: `cargo test --test kafka_wire describe_groups`

Expected: FAIL because `DescribeGroups` is not advertised or dispatched.

- [ ] **Step 3: Implement the handler module**

Use generated response builders. Successful members map as:

```rust
DescribedGroupMember::default()
    .with_member_id(StrBytes::from_string(member.member_id))
    .with_client_id(StrBytes::from_string(member.client_id))
    .with_client_host(StrBytes::default())
    .with_member_metadata(member.metadata)
    .with_member_assignment(member.assignment)
```

Successful groups set error `0`, real state, protocol type, selected protocol in `protocol_data`, and mapped members. Missing groups set `GroupIdNotFound.code()`, the requested ID, empty strings, and no members. Preserve request ordering.

- [ ] **Step 4: Advertise and dispatch v0**

Add `mod describe_groups;`, add its range to `api_versions::response`, and add:

```rust
ApiKey::DescribeGroups => {
    require_version(request.api_key, version, &describe_groups::VERSION_RANGE)?;
    match &request.body {
        RequestKind::DescribeGroups(body) => {
            Ok(describe_groups::response(body, &self.broker).await.into())
        }
        _ => Err(DispatchError::BodyMismatch(request.api_key)),
    }
}
```

- [ ] **Step 5: Run protocol tests GREEN**

Run:

```bash
cargo test --test kafka_wire describe_groups
cargo test --test kafka_wire api_versions
```

Expected: response content, unknown-group error, and API matrix all pass.

- [ ] **Step 6: Commit the protocol slice**

```bash
git add src/kafka/describe_groups.rs src/kafka/mod.rs src/kafka/api_versions.rs src/kafka/dispatcher.rs tests/kafka_wire.rs
git commit -m "feat: describe classic consumer groups"
```

### Task 3: Active-group Kafbat black-box regression

**Files:**
- Modify: `tests/go-client/cmd/kafbat-seed/main.go`
- Modify: `tests/kafbat/run.sh`

**Interfaces:**
- Consumes: existing Kafbat seed image, Docker network, and Kafbat cluster refresh endpoint.
- Produces: a long-running seed container that produces the exact probe record and keeps `MEMKAFKA_KAFBAT_GROUP` active until cleanup.

- [ ] **Step 1: Make the Go seed hold a real group**

Require `MEMKAFKA_KAFBAT_GROUP`, add these options to the existing client, and retain `kgo.DisableIdempotentWrite()`:

```go
kgo.ConsumerGroup(groupID),
kgo.ConsumeTopics(topic),
kgo.SessionTimeout(60*time.Second),
```

After the offset-0 delivery assertion, poll until the exact record is consumed, print `group active <groupID>`, and keep polling so heartbeats continue:

```go
for {
	fetches := client.PollRecords(context.Background(), 10)
	if errs := fetches.Errors(); len(errs) != 0 {
		panic(fmt.Errorf("group poll: %v", errs))
	}
	for _, consumed := range fetches.Records() {
		if consumed.Topic == topic && string(consumed.Key) == key && string(consumed.Value) == value {
			fmt.Printf("group active %s\n", groupID)
			for {
				if errs := client.PollRecords(context.Background(), 10).Errors(); len(errs) != 0 {
					panic(fmt.Errorf("group heartbeat poll: %v", errs))
				}
			}
		}
	}
}
```

- [ ] **Step 2: Run the seed as a managed background container**

Add:

```bash
readonly SEED_CONTAINER="memkafka-kafbat-seed-${SUFFIX}"
readonly GROUP_ID="kafbat-group-${SUFFIX}"
```

Include the seed container in diagnostics and cleanup. Replace blocking seed execution with:

```bash
docker run --detach \
  --name "${SEED_CONTAINER}" \
  --network "${NETWORK}" \
  --env "MEMKAFKA_BOOTSTRAP_SERVERS=${BROKER_CONTAINER}:9092" \
  --env "MEMKAFKA_KAFBAT_TOPIC=${TOPIC}" \
  --env "MEMKAFKA_KAFBAT_KEY=${KEY}" \
  --env "MEMKAFKA_KAFBAT_VALUE=${VALUE}" \
  --env "MEMKAFKA_KAFBAT_GROUP=${GROUP_ID}" \
  "${SEED_IMAGE}" >/dev/null

group_active=false
for _ in {1..30}; do
  if docker logs "${SEED_CONTAINER}" 2>&1 | grep -F "group active ${GROUP_ID}" >/dev/null; then
    group_active=true
    break
  fi
  sleep 1
done
if [[ "${group_active}" != true ]]; then
  echo "Kafbat seed consumer group did not become active" >&2
  exit 1
fi
```

- [ ] **Step 3: Run Kafbat against the new handler**

Run:

```bash
docker build --tag memkafka:ci .
docker build --file tests/kafbat/Dockerfile.seed --tag memkafka-kafbat-seed:ci .
tests/kafbat/run.sh
```

Expected: `PASS Kafbat UI discovered ...` while the seed group remains active.

- [ ] **Step 4: Format and test Go**

Run:

```bash
test -z "$(gofmt -l tests/go-client)"
(cd tests/go-client && go test -count=1 -mod=readonly ./...)
```

Expected: both commands exit `0`.

- [ ] **Step 5: Commit the regression**

```bash
git add tests/go-client/cmd/kafbat-seed/main.go tests/kafbat/run.sh
git commit -m "test: keep Kafbat online with consumer groups"
```

### Task 4: Compatibility documentation and verification

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: passing wire and Kafbat tests from Tasks 1-3.
- Produces: a truthful Kafbat groups claim and advertised API list.

- [ ] **Step 1: Update README claims**

Change the Kafbat row's Groups and commits cell from `—` to `✅ read-only`. Update the Kafbat paragraph to state that its cluster remains online with an active consumer group. Add `DescribeGroups 0` immediately after `ListGroups 0` in the advertised API list.

- [ ] **Step 2: Run repository verification**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
docker build --tag memkafka:ci .
docker build --file tests/kafbat/Dockerfile.seed --tag memkafka-kafbat-seed:ci .
tests/kafbat/run.sh
git diff --check
```

Expected: every command exits `0`; Kafbat reports the group-aware PASS line.

- [ ] **Step 3: Commit documentation**

```bash
git add README.md
git commit -m "docs: report group-aware Kafbat support"
```
