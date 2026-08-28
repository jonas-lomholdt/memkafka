#!/usr/bin/env bash
set -euo pipefail

IMAGE="${1:-memkafka:ci}"

assert_inspect_value() {
    local description="$1"
    local format="$2"
    local expected="$3"
    local actual

    actual="$(docker image inspect --format "$format" "$IMAGE")"
    if [[ "$actual" != "$expected" ]]; then
        printf 'FAIL: %s: expected %q, got %q\n' "$description" "$expected" "$actual" >&2
        exit 1
    fi
}

assert_inspect_value \
    'org.opencontainers.image.source label' \
    '{{ index .Config.Labels "org.opencontainers.image.source" }}' \
    'https://github.com/jonas-lomholdt/memkafka'
assert_inspect_value \
    'org.opencontainers.image.description label' \
    '{{ index .Config.Labels "org.opencontainers.image.description" }}' \
    'Fast, single-binary, in-memory Kafka-compatible broker for development and integration tests'
assert_inspect_value \
    'org.opencontainers.image.licenses label' \
    '{{ index .Config.Labels "org.opencontainers.image.licenses" }}' \
    'MIT'
assert_inspect_value 'container user' '{{ .Config.User }}' 'memkafka'

docker run --rm "$IMAGE" --help >/dev/null

printf 'PASS: container image metadata and CLI verified for %s\n' "$IMAGE"
