#!/usr/bin/env bash
set -euo pipefail

SCRIPT="scripts/ci/resolve-publish-context.sh"
TEST_SHA="0123456789abcdef0123456789abcdef01234567"

assert_output_line() {
    local output_file="$1"
    local expected="$2"

    if ! grep -Fx -- "$expected" "$output_file" >/dev/null; then
        printf 'FAIL: expected output line %q in %s\n' "$expected" "$output_file" >&2
        printf 'Actual output:\n' >&2
        cat "$output_file" >&2
        exit 1
    fi
}

run_success_case() {
    local description="$1"
    local ref="$2"
    local ref_name="$3"
    shift 3

    local output_file
    output_file="$(mktemp)"

    GITHUB_REF="$ref" \
    GITHUB_REF_NAME="$ref_name" \
    GITHUB_SHA="$TEST_SHA" \
    GITHUB_OUTPUT="$output_file" \
    "$SCRIPT"

    for expected_line in "$@"; do
        assert_output_line "$output_file" "$expected_line"
    done

    rm -f "$output_file"
    printf 'PASS: %s\n' "$description"
}

run_failure_case() {
    local description="$1"
    local ref="$2"
    local ref_name="$3"

    local output_file
    output_file="$(mktemp)"

    if GITHUB_REF="$ref" \
        GITHUB_REF_NAME="$ref_name" \
        GITHUB_SHA="$TEST_SHA" \
        GITHUB_OUTPUT="$output_file" \
        "$SCRIPT" >/dev/null 2>&1; then
        printf 'FAIL: expected rejection for %s\n' "$description" >&2
        rm -f "$output_file"
        exit 1
    fi

    rm -f "$output_file"
    printf 'PASS: %s\n' "$description"
}

run_success_case \
    'main push resolves to edge publication' \
    'refs/heads/main' \
    'main' \
    'channel=edge'

run_success_case \
    'stable release tag resolves semantic publication outputs' \
    'refs/tags/v1.2.3' \
    'v1.2.3' \
    'channel=release' \
    'version=1.2.3' \
    'major_minor=1.2' \
    'major=1'

run_failure_case 'noncanonical release tag with leading zero' 'refs/tags/v01.2.3' 'v01.2.3'
run_failure_case 'noncanonical prerelease tag' 'refs/tags/v1.2.3-rc1' 'v1.2.3-rc1'
run_failure_case 'non-main branch ref' 'refs/heads/release' 'release'

printf 'PASS: publish ref behavior verified\n'
