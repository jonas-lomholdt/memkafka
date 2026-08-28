# GHCR Publishing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish verified Linux AMD64 and ARM64 MemKafka images to `ghcr.io/jonas-lomholdt/memkafka` from green `main` and canonical stable tags, with anonymous pull documentation.

**Architecture:** OCI metadata remains in the existing `Dockerfile`; image verification includes focused black-box publication-policy scripts; ordinary CI and release publication share one full reusable verification workflow. Successful same-repository `CI` runs publish the exact verified `main` SHA, while canonical stable tag pushes run the same full gate. Docker Buildx publishes one primary multi-platform tag, captures its digest, and promotes only fresh, monotonic mutable aliases. Package visibility remains an explicit one-time maintainer action after the first push.

**Tech Stack:** Docker/OCI, Docker Buildx, GitHub Actions, GitHub Container Registry, Bash.

**Spec:** `docs/2026-08-28-throughput-benchmark-and-ghcr-design.md`

## Global Constraints

- Keep publishing isolated in `.github/workflows/publish.yml` and shell helpers under `scripts/ci/`; do not add release code to the Rust binary.
- Trigger main publication only from a successful same-repository `CI` push run whose verified head branch is still `main`. Trigger tag publication from pushed tags matching `v*.*.*`, but accept only canonical stable tags `vMAJOR.MINOR.PATCH`.
- Run the complete reusable hosted suite before publishing and grant only `contents: read` plus `packages: write`.
- Publish Linux AMD64 and ARM64 manifests.
- Publish one primary tag first: `sha-<full-40-character-commit>` for `main`, or exact `major.minor.patch` for a stable release. Promote `edge`, `major.minor`, `major`, and `latest` from the captured manifest digest only after their freshness rules pass.
- Treat `sha-*` as a commit-addressed mutable registry tag and the published OCI digest as the immutable image identity.
- Advance stable aliases monotonically against every canonical stable tag on the remote; never let an older or out-of-order release move an alias backward.
- Pin every action in a `packages: write` job to a reviewed full commit SHA.
- Link the package to `https://github.com/jonas-lomholdt/memkafka` and include source, description, revision, version, and MIT license metadata.
- Use the repository `GITHUB_TOKEN`; do not create or require a personal access token.
- Keep `latest` release-only and keep the first public-visibility change explicit because GitHub does not allow a public package to be made private again.

---

### Task 1: OCI labels and local container metadata test

**Files:**
- Modify: `Dockerfile`
- Create: `tests/container-image.sh`
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: an image tag as `$1`, defaulting to `memkafka:ci`
- Produces: an executable black-box assertion for image labels, non-root user, and CLI startup

- [ ] **Step 1: Write the failing image metadata test**

Create `tests/container-image.sh` with `set -euo pipefail`. Inspect the supplied image with `docker image inspect` and require:

```text
org.opencontainers.image.source=https://github.com/jonas-lomholdt/memkafka
org.opencontainers.image.description=Fast, single-binary, in-memory Kafka-compatible broker for development and integration tests
org.opencontainers.image.licenses=MIT
```

Also assert `.Config.User == "memkafka"`. Verify `--help` by creating and starting a temporary container, polling its state for at most 10 seconds, asserting exit status `0`, and removing it on success, failure, or interruption. Print one concise `PASS` line.

- [ ] **Step 2: Run the test against the current image and verify RED**

Run:

```bash
docker build --tag memkafka:oci-red .
tests/container-image.sh memkafka:oci-red
```

Expected: failure naming the first missing OCI label.

- [ ] **Step 3: Add static OCI labels to the runtime image**

Add one Dockerfile label block:

```dockerfile
LABEL org.opencontainers.image.source="https://github.com/jonas-lomholdt/memkafka" \
      org.opencontainers.image.description="Fast, single-binary, in-memory Kafka-compatible broker for development and integration tests" \
      org.opencontainers.image.licenses="MIT"
```

Keep revision and version dynamic; the publishing workflow supplies them from Git metadata.

- [ ] **Step 4: Rebuild and verify GREEN**

Run:

```bash
docker build --tag memkafka:oci-green .
tests/container-image.sh memkafka:oci-green
bash -n tests/container-image.sh
```

Expected: the script prints `PASS` and every command exits `0`.

- [ ] **Step 5: Add the image assertion to ordinary CI**

Immediately after CI builds `memkafka:ci`, run `tests/container-image.sh memkafka:ci`. This catches label/user/entrypoint regressions without coupling the release workflow to benchmark code.

- [ ] **Step 6: Run root verification and commit**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
git diff --check
```

Then commit:

```bash
git add Dockerfile tests/container-image.sh .github/workflows/ci.yml
git commit -m "test: verify distributable container metadata"
```

---

### Task 2: Mainline and release multi-platform publication workflow and README usage

**Files:**
- Modify: `.github/workflows/ci.yml`
- Create: `.github/workflows/verify.yml`
- Modify: `.github/workflows/publish.yml`
- Modify: `README.md`
- Create: `scripts/ci/resolve-publish-context.sh`
- Create: `scripts/ci/select-release-aliases.sh`
- Modify: `tests/container-image.sh`
- Create: `tests/publish-ref-behavior.sh`

**Interfaces:**
- Consumes: `refs/heads/main` or a canonical stable tag such as `v0.1.0` pointing at a reviewed commit
- Produces: a primary AMD64/ARM64 manifest at `sha-<full-40-character-commit>` for `main` or `0.1.0` for release `v0.1.0`, then promotes only eligible mutable aliases to that captured digest

- [ ] **Step 1: Extract the complete hosted suite and create verification-first workflow entry points**

Move the complete current `CI` job to `.github/workflows/verify.yml` with `on: workflow_call` and `contents: read`. Keep `.github/workflows/ci.yml` as the public `CI` wrapper for all branch pushes and pull requests, explicitly excluding tag pushes, and call the reusable workflow exactly once.

For `main`, trigger `publish.yml` from completed `CI` runs and require a push event, success, `head_branch == main`, and same-repository provenance. For stable tags, call the same reusable verification workflow. A failed or skipped gate must not reach registry login.

- [ ] **Step 2: Write the failing publish-ref behavior test**

Create `tests/publish-ref-behavior.sh` to exercise the shared publish-context resolver and release-alias helper with literal cases. Require:

- explicit `refs/heads/main` plus a full verified SHA => primary `sha-<full-commit>` and candidate alias `edge`;
- canonical `refs/tags/v1.2.3` => primary `1.2.3` and candidate aliases `1.2`, `1`, and `latest`;
- fallback to GitHub's direct-event variables for stable tag pushes;
- rejection of leading zero, prerelease, non-main, mismatched ref/ref-name, invalid SHA, and any input producing a Docker tag outside the 128-character grammar;
- monotonic alias selection for a lone release, newer patches, newer majors, unordered inputs, and numeric components too large for shell arithmetic; and
- workflow integration: reusable full gates, successful same-repository main CI, action SHA pins, digest promotion, concurrency, and policy-test execution from the reusable gate.

This must prove no cross-channel tags leak between the `main` and release publications.

Run the expanded test against the current implementation and confirm it fails for the missing full-SHA context outputs, alias selector, and reusable-gate wiring.

- [ ] **Step 3: Add a shared publish-context resolver and wire it into the workflow**

Create `scripts/ci/resolve-publish-context.sh` and run it before registry login. It accepts MemKafka-specific target ref, ref name, and SHA variables for `workflow_run`, with GitHub's direct-event variables as fallback. It must accept exactly:

- `refs/heads/main`; or
- `refs/tags/vMAJOR.MINOR.PATCH`, where each numeric component is `0` or a non-zero digit followed by digits.

Reject every other ref and any mismatch or invalid full SHA. Emit the channel, primary tag, alias candidates, version, and verified target SHA. Validate every emitted Docker tag before output. For `main`, the primary is `sha-<full-commit>` and the only candidate alias is `edge`; for stable tags, the primary is exact `major.minor.patch` and the candidates are `major.minor`, `major`, and `latest`.

In `.github/workflows/publish.yml`, replace the existing release-tag validation step with:

```bash
scripts/ci/resolve-publish-context.sh
```

- [ ] **Step 4: Add digest-first authenticated multi-platform publication**

Pin every action in the `packages: write` job to its reviewed full SHA. Generate metadata for exactly the primary tag, explicitly set OCI revision to the verified target SHA, and retain source, description, license, version, and created labels. Build and push AMD64/ARM64 with maximum provenance and an SBOM, then capture `docker/build-push-action`'s manifest digest.

```yaml
- uses: docker/setup-qemu-action@96fe6ef7f33517b61c61be40b68a1882f3264fb8 # v4
  with:
    platforms: arm64
- uses: docker/setup-buildx-action@37fe631027851001ddb9b187196cc803df7f5f0e # v4
- uses: docker/login-action@dbcb813823bdd20940b903addbd779551569679f # v4
  with:
    registry: ${{ env.REGISTRY }}
    username: ${{ github.actor }}
    password: ${{ secrets.GITHUB_TOKEN }}
- id: meta
  uses: docker/metadata-action@dc802804100637a589fabce1cb79ff13a1411302 # v6
  with:
    images: ${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}
    flavor: |
      latest=false
    tags: |
      type=raw,value=${{ steps.publish_context.outputs.primary_tag }}
- id: build
  uses: docker/build-push-action@53b7df96c91f9c12dcc8a07bcb9ccacbed38856a # v7
  with:
    context: .
    platforms: linux/amd64,linux/arm64
    push: true
    tags: ${{ steps.meta.outputs.tags }}
    labels: ${{ steps.meta.outputs.labels }}
    provenance: mode=max
    sbom: true
```

After the primary push, promote aliases only with `docker buildx imagetools create` against `${{ steps.build.outputs.digest }}`. Re-read `refs/heads/main` immediately before moving `edge`. For releases, use a focused pure shell helper to compare all canonical remote stable tags as decimal strings: move the minor alias only for the highest patch in that minor, the major alias only for the highest version in that major, and `latest` only for the highest stable version overall. Use one cancelling mainline concurrency group and one non-cancelling group per exact release tag.

- [ ] **Step 5: Add anonymous-pull documentation**

Near the main Docker quick start, prefer:

```bash
docker run --rm -p 9092:9092 -p 8081:8081 \
  ghcr.io/jonas-lomholdt/memkafka:latest
```

Keep the local `docker build` path for contributors. Explain that `edge` follows only the latest fully green, still-current `main` commit; stable aliases advance monotonically; `sha-<full-commit>` is commit-addressed but still a mutable registry tag; and an OCI digest is the immutable pin. Keep the one-time package-settings path: package page → Package settings → Change visibility → Public. State that public visibility cannot be reverted.

- [ ] **Step 6: Validate workflow syntax and publish-ref behavior**

Run:

```bash
ruby -e 'require "yaml"; Dir[".github/workflows/*.yml"].each { |path| YAML.parse_file(path) }'
tests/publish-ref-behavior.sh
bash -n scripts/ci/*.sh tests/container-image.sh tests/publish-ref-behavior.sh
git diff --check
```

Expected: every workflow parses; the policy test proves exact primary tags, rejection behavior, monotonic release aliases, and workflow gating; and all shell scripts parse cleanly. Do not push any image from this step.

- [ ] **Step 7: Commit publication automation**

```bash
git add .github/workflows README.md scripts/ci tests/container-image.sh tests/publish-ref-behavior.sh docs
git commit -m "ci: harden verified image publication"
```

---

### Task 3: Hosted verification, first mainline publication, release publication, and public handoff

**Files:**
- Verify only: `.github/workflows/ci.yml`
- Verify only: `.github/workflows/publish.yml`
- Verify only: `README.md`

**Interfaces:**
- Consumes: green `main` and a canonical stable tag at version `0.1.0`
- Produces: a commit-addressed `sha-<full-commit>` primary plus fresh `edge`, an exact stable release plus monotonic aliases, immutable OCI digests, and an explicit visibility handoff

- [ ] **Step 1: Run complete local verification**

Run workflow YAML parsing, all shell policy tests and syntax checks, root formatting, strict Clippy, the full Rust suite, and every standalone benchmark check. The hosted reusable gate runs the container metadata/CLI assertion and all native/container client suites. Do not rerun the known-broken local AMD64 QEMU build.

- [ ] **Step 2: Push `main` and require green ordinary CI**

```bash
git push origin main
gh run list --branch main --workflow CI --limit 3
gh run watch <CI_RUN_ID> --exit-status
```

Expected: ordinary `CI` calls the reusable full gate exactly once, including every native/container client, the publication-policy test, and the metadata assertion. Its successful push completion is the only event that starts main publication.

- [ ] **Step 3: Monitor the mainline publish workflow and inspect `edge` plus SHA output**

The publication run is a `workflow_run` consumer of the successful `CI` run and must build `workflow_run.head_sha`, not the publication wrapper's own `github.sha`. Run:

```bash
MAIN_SHA="$(git rev-parse origin/main)"
gh run list --event workflow_run --branch main --workflow "Publish container" --limit 3
gh run watch <MAIN_PUBLISH_RUN_ID> --exit-status
docker buildx imagetools inspect ghcr.io/jonas-lomholdt/memkafka:edge
docker buildx imagetools inspect "ghcr.io/jonas-lomholdt/memkafka:sha-$MAIN_SHA"
```

Expected: both tags resolve to the same AMD64/ARM64 manifest digest; `edge` moved only after the remote-main freshness check; and the `sha-<full-commit>` tag is commit-addressed. Record the `sha256` manifest digest as the immutable image identity.

- [ ] **Step 4: Confirm the release tag is new and points at green `main`**

Run:

```bash
git fetch --tags origin
git tag --list v0.1.0
git rev-parse HEAD
git rev-parse origin/main
```

Expected: `v0.1.0` is absent and `HEAD` equals `origin/main`. If the tag already exists, stop instead of moving or replacing it.

- [ ] **Step 5: Create and push the first release tag**

```bash
git tag -a v0.1.0 -m "MemKafka v0.1.0"
git push origin v0.1.0
```

This external publication step is authorized by the approved design; never force-update the tag.

- [ ] **Step 6: Monitor the release publish workflow and inspect the manifest**

```bash
gh run list --event push --workflow "Publish container" --limit 3
gh run watch <PUBLISH_RUN_ID> --exit-status
docker buildx imagetools inspect ghcr.io/jonas-lomholdt/memkafka:0.1.0
docker buildx imagetools inspect ghcr.io/jonas-lomholdt/memkafka:0.1
docker buildx imagetools inspect ghcr.io/jonas-lomholdt/memkafka:0
docker buildx imagetools inspect ghcr.io/jonas-lomholdt/memkafka:latest
```

Expected: the tag push first passes the same full reusable gate. The exact `0.1.0` manifest contains `linux/amd64` and `linux/arm64`; because this is the first canonical release, `0.1`, `0`, and `latest` resolve to its same digest. Later lower or out-of-order releases still publish their exact version but move only aliases for which they are highest in the complete remote canonical-tag set. Release publication never emits `edge` or `sha-*`.

- [ ] **Step 7: Complete the explicit public-visibility handoff**

Open the package settings for `jonas-lomholdt/memkafka`, confirm the package is linked to this repository, and ask the maintainer to choose **Change visibility → Public**. Do not represent the image as anonymously available until that irreversible one-time action is confirmed.

- [ ] **Step 8: Verify a truly anonymous pull**

After public visibility is confirmed, log Docker out of `ghcr.io` in an isolated Docker config directory and run:

```bash
mkdir -p /tmp/memkafka-anonymous-docker
docker --config /tmp/memkafka-anonymous-docker pull ghcr.io/jonas-lomholdt/memkafka:latest
```

Expected: pull succeeds without credentials. Report the immutable digest and the public pull command in the final handoff.
