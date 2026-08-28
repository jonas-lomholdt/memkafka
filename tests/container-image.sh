#!/usr/bin/env bash
set -euo pipefail

IMAGE="${1:-memkafka:ci}"
CLI_TIMEOUT_SECONDS=10
cli_container_id=""

cleanup_cli_container() {
    if [[ -n "$cli_container_id" ]]; then
        docker container rm --force "$cli_container_id" >/dev/null 2>&1 || true
    fi
}

trap cleanup_cli_container EXIT
trap 'exit 130' HUP INT TERM

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

cli_container_id="$(docker container create "$IMAGE" --help)"
docker container start "$cli_container_id" >/dev/null

deadline=$((SECONDS + CLI_TIMEOUT_SECONDS))
while [[ "$(docker container inspect --format '{{ .State.Running }}' "$cli_container_id")" == "true" ]]; do
    if (( SECONDS >= deadline )); then
        printf 'FAIL: container CLI --help did not exit within %s seconds\n' \
            "$CLI_TIMEOUT_SECONDS" >&2
        docker container logs "$cli_container_id" >&2 || true
        exit 1
    fi
    sleep 1
done

cli_exit_code="$(docker container inspect --format '{{ .State.ExitCode }}' "$cli_container_id")"
if [[ "$cli_exit_code" != "0" ]]; then
    printf 'FAIL: container CLI --help exited with status %s\n' "$cli_exit_code" >&2
    docker container logs "$cli_container_id" >&2 || true
    exit 1
fi

cleanup_cli_container
cli_container_id=""

printf 'PASS: container image metadata and CLI verified for %s\n' "$IMAGE"
