# GHCR Publishing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish verified Linux AMD64 and ARM64 MemKafka images to `ghcr.io/jonas-lomholdt/memkafka` from green `main` and canonical stable tags, with anonymous pull documentation.

**Architecture:** OCI metadata remains in the existing `Dockerfile`; image verification includes a small black-box ref-contract script; one GitHub Actions workflow verifies the source commit and then uses Docker Buildx to publish one multi-platform manifest per accepted `main` or release ref. Package visibility remains an explicit one-time maintainer action after the first push.

**Tech Stack:** Docker/OCI, Docker Buildx, GitHub Actions, GitHub Container Registry, Bash.

**Spec:** `docs/2026-08-28-throughput-benchmark-and-ghcr-design.md`

## Global Constraints

- Keep publishing isolated in `.github/workflows/publish.yml`; do not add release code to the Rust binary.
- Trigger on pushes to `main` and pushed tags matching `v*.*.*`, but accept only `refs/heads/main` or canonical stable tags `vMAJOR.MINOR.PATCH`.
- Verify before publishing and grant only `contents: read` plus `packages: write`.
- Publish Linux AMD64 and ARM64 manifests.
- Generate exactly `edge` plus `sha-<short-commit>` for `main`, and exactly `major.minor.patch`, `major.minor`, `major`, and `latest` for stable releases.
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

Also assert `.Config.User == "memkafka"` and run `docker run --rm "$IMAGE" --help` successfully. Print one concise `PASS` line.

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
- Modify: `.github/workflows/publish.yml`
- Modify: `README.md`
- Create: `scripts/ci/resolve-publish-context.sh`
- Create: `tests/publish-ref-behavior.sh`

**Interfaces:**
- Consumes: `refs/heads/main` or a canonical stable tag such as `v0.1.0` pointing at a reviewed commit
- Produces: `ghcr.io/jonas-lomholdt/memkafka:{edge,sha-<short-commit>}` for `main`, or `ghcr.io/jonas-lomholdt/memkafka:{0.1.0,0.1,0,latest}` for a release, each as one AMD64/ARM64 manifest

- [ ] **Step 1: Create a verification-first workflow skeleton**

Use this event/permission/job shape:

```yaml
name: Publish container

on:
  push:
    branches:
      - main
    tags:
      - "v*.*.*"

permissions:
  contents: read

env:
  REGISTRY: ghcr.io
  IMAGE_NAME: jonas-lomholdt/memkafka

jobs:
  verify:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7
      - run: rustup show
      - run: cargo fmt --all -- --check
      - run: cargo clippy --all-targets --all-features -- -D warnings
      - run: cargo test --all-targets --all-features

  publish:
    needs: verify
    runs-on: ubuntu-latest
    permissions:
      contents: read
      packages: write
```

The publish job must not run when verification fails.

- [ ] **Step 2: Write the failing publish-ref behavior test**

Create `tests/publish-ref-behavior.sh` to exercise the shared publish-context resolver with literal cases. Require:

- `refs/heads/main` => `channel=edge`;
- `refs/tags/v1.2.3` => `channel=release`, `version=1.2.3`, `major_minor=1.2`, `major=1`;
- rejection for `refs/tags/v01.2.3`, `refs/tags/v1.2.3-rc1`, and `refs/heads/release`.

Run it before the helper exists and expect a failure about the missing resolver script.

- [ ] **Step 3: Add a shared publish-context resolver and wire it into the workflow**

Create `scripts/ci/resolve-publish-context.sh` and run it before registry login. It must accept exactly:

- `refs/heads/main`; or
- `refs/tags/vMAJOR.MINOR.PATCH`, where each numeric component is `0` or a non-zero digit followed by digits.

Reject every other ref. For `main`, emit `channel=edge`. For stable tags, emit `channel=release`, `version`, `major_minor`, and `major`. This prevents `latest` from being moved by `alpha`, `beta`, `rc`, or leading-zero tags even though the workflow glob is broad.

In `.github/workflows/publish.yml`, replace the existing release-tag validation step with:

```bash
scripts/ci/resolve-publish-context.sh
```

- [ ] **Step 4: Add authenticated multi-platform Buildx publication**

Use current major releases from the verified Docker publisher:

```yaml
- uses: docker/setup-qemu-action@v4
  with:
    platforms: arm64
- uses: docker/setup-buildx-action@v4
- uses: docker/login-action@v4
  with:
    registry: ${{ env.REGISTRY }}
    username: ${{ github.actor }}
    password: ${{ secrets.GITHUB_TOKEN }}
- id: meta
  uses: docker/metadata-action@v6
  with:
    images: ${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}
    flavor: |
      latest=false
    tags: |
      type=raw,value=edge,enable=${{ steps.publish_context.outputs.channel == 'edge' }}
      type=sha,prefix=sha-,format=short,enable=${{ steps.publish_context.outputs.channel == 'edge' }}
      type=raw,value=${{ steps.publish_context.outputs.version }},enable=${{ steps.publish_context.outputs.channel == 'release' }}
      type=raw,value=${{ steps.publish_context.outputs.major_minor }},enable=${{ steps.publish_context.outputs.channel == 'release' }}
      type=raw,value=${{ steps.publish_context.outputs.major }},enable=${{ steps.publish_context.outputs.channel == 'release' }}
      type=raw,value=latest,enable=${{ steps.publish_context.outputs.channel == 'release' }}
- uses: docker/build-push-action@v7
  with:
    context: .
    platforms: linux/amd64,linux/arm64
    push: true
    tags: ${{ steps.meta.outputs.tags }}
    labels: ${{ steps.meta.outputs.labels }}
    provenance: mode=max
    sbom: true
```

The metadata action supplies the dynamic revision/version/created labels. The static Dockerfile labels preserve source/description/license. `main` must produce exactly `edge` and `sha-<short-commit>`, without `latest`. A release must produce exactly patch, minor, major, and `latest`, without `edge` or SHA tags.

- [ ] **Step 5: Add anonymous-pull documentation**

Near the main Docker quick start, prefer:

```bash
docker run --rm -p 9092:9092 -p 8081:8081 \
  ghcr.io/jonas-lomholdt/memkafka:latest
```

Keep the local `docker build` path for contributors. Add a compact release note explaining that `latest` and version tags are stable releases, `edge` follows the latest green `main` publication, and `sha-<short-commit>` pins an immutable development snapshot. Keep the one-time package-settings path: package page → Package settings → Change visibility → Public. State that public visibility cannot be reverted.

- [ ] **Step 6: Validate workflow syntax and publish-ref behavior**

Run:

```bash
ruby -e 'require "yaml"; YAML.parse_file(".github/workflows/publish.yml")'
tests/publish-ref-behavior.sh
bash -n scripts/ci/resolve-publish-context.sh
bash -n tests/container-image.sh
bash -n tests/publish-ref-behavior.sh
git diff --check
```

Expected: YAML parses; the publish-ref test proves `main`, stable release, and rejection behavior; and all shell scripts parse cleanly. Do not push any image from this step.

- [ ] **Step 7: Commit publication automation**

```bash
git add .github/workflows/publish.yml README.md scripts/ci/resolve-publish-context.sh tests/publish-ref-behavior.sh
git commit -m "ci: publish edge images from main"
```

---

### Task 3: Hosted verification, first mainline publication, release publication, and public handoff

**Files:**
- Verify only: `.github/workflows/ci.yml`
- Verify only: `.github/workflows/publish.yml`
- Verify only: `README.md`

**Interfaces:**
- Consumes: green `main` and a canonical stable tag at version `0.1.0`
- Produces: verified `edge`/`sha-<short-commit>` plus stable release GHCR packages and an explicit visibility handoff

- [ ] **Step 1: Run complete local verification**

Run the root formatting, strict Clippy, full Rust suite, container metadata test, and all changed standalone benchmark checks. Remove only temporary image tags created by this plan.

- [ ] **Step 2: Push `main` and require green ordinary CI**

```bash
git push origin main
gh run list --branch main --workflow CI --limit 3
gh run watch <CI_RUN_ID> --exit-status
```

Expected: ordinary CI, including every existing native/container client and the new metadata assertion, completes successfully.

- [ ] **Step 3: Monitor the mainline publish workflow and inspect `edge` plus SHA output**

Run:

```bash
gh run list --branch main --workflow "Publish container" --limit 3
gh run watch <MAIN_PUBLISH_RUN_ID> --exit-status
docker buildx imagetools inspect ghcr.io/jonas-lomholdt/memkafka:edge
docker buildx imagetools inspect ghcr.io/jonas-lomholdt/memkafka:sha-<short-commit>
```

Expected: both manifests contain `linux/amd64` and `linux/arm64`; `edge` resolves to the latest green `main` commit; and the immutable `sha-<short-commit>` tag resolves to the same publication.

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
gh run list --workflow "Publish container" --limit 3
gh run watch <PUBLISH_RUN_ID> --exit-status
docker buildx imagetools inspect ghcr.io/jonas-lomholdt/memkafka:0.1.0
```

Expected: the manifest contains `linux/amd64` and `linux/arm64`; tags `0.1.0`, `0.1`, `0`, and `latest` resolve to the release; and the release publication does not emit `edge` or `sha-*`.

- [ ] **Step 7: Complete the explicit public-visibility handoff**

Open the package settings for `jonas-lomholdt/memkafka`, confirm the package is linked to this repository, and ask the maintainer to choose **Change visibility → Public**. Do not represent the image as anonymously available until that irreversible one-time action is confirmed.

- [ ] **Step 8: Verify a truly anonymous pull**

After public visibility is confirmed, log Docker out of `ghcr.io` in an isolated Docker config directory and run:

```bash
mkdir -p /tmp/memkafka-anonymous-docker
docker --config /tmp/memkafka-anonymous-docker pull ghcr.io/jonas-lomholdt/memkafka:latest
```

Expected: pull succeeds without credentials. Report the immutable digest and the public pull command in the final handoff.
