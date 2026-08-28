#!/usr/bin/env bash
set -euo pipefail

SCRIPT="scripts/ci/resolve-publish-context.sh"
TEST_SHA="0123456789abcdef0123456789abcdef01234567"

read_multiline_output() {
    local output_file="$1"
    local key="$2"
    local header="${key}<<"
    local delimiter=""
    local line=""
    local value=""

    while IFS= read -r line; do
        if [[ -z "$delimiter" ]]; then
            if [[ "$line" == "$header"* ]]; then
                delimiter="${line#"$header"}"
                continue
            fi
        else
            if [[ "$line" == "$delimiter" ]]; then
                printf '%s' "$value"
                return 0
            fi

            if [[ -n "$value" ]]; then
                value+=$'\n'
            fi
            value+="$line"
        fi
    done < "$output_file"

    printf 'FAIL: missing completed multiline output %q in %s\n' "$key" "$output_file" >&2
    printf 'Actual output:\n' >&2
    cat "$output_file" >&2
    exit 1
}

resolve_metadata_tags() {
    local rules="$1"
    local rule=""
    local result=""
    local type=""
    local value=""
    local prefix=""
    local format=""
    local field=""

    while IFS= read -r rule; do
        [[ -n "$rule" ]] || continue
        type=""
        value=""
        prefix=""
        format=""

        IFS=',' read -r -a fields <<< "$rule"
        for field in "${fields[@]}"; do
            case "$field" in
                type=*)
                    type="${field#type=}"
                    ;;
                value=*)
                    value="${field#value=}"
                    ;;
                prefix=*)
                    prefix="${field#prefix=}"
                    ;;
                format=*)
                    format="${field#format=}"
                    ;;
            esac
        done

        case "$type" in
            raw)
                if [[ -n "$result" ]]; then
                    result+=$'\n'
                fi
                result+="$value"
                ;;
            sha)
                if [[ "$format" != "short" ]]; then
                    printf 'FAIL: unsupported sha format %q in rule %q\n' "$format" "$rule" >&2
                    exit 1
                fi
                if [[ -n "$result" ]]; then
                    result+=$'\n'
                fi
                result+="${prefix}${TEST_SHA:0:7}"
                ;;
            *)
                printf 'FAIL: unsupported metadata-action rule %q\n' "$rule" >&2
                exit 1
                ;;
        esac
    done <<< "$rules"

    printf '%s' "$result"
}

assert_exact_value() {
    local description="$1"
    local actual="$2"
    local expected="$3"

    if [[ "$actual" != "$expected" ]]; then
        printf 'FAIL: %s\nExpected:\n%s\nActual:\n%s\n' "$description" "$expected" "$actual" >&2
        exit 1
    fi
}

run_success_case() {
    local description="$1"
    local ref="$2"
    local ref_name="$3"
    local expected_tags="$4"

    local output_file
    output_file="$(mktemp)"

    GITHUB_REF="$ref" \
    GITHUB_REF_NAME="$ref_name" \
    GITHUB_SHA="$TEST_SHA" \
    GITHUB_OUTPUT="$output_file" \
    "$SCRIPT"

    local actual_rules
    actual_rules="$(read_multiline_output "$output_file" tags)"

    local actual_tags
    actual_tags="$(resolve_metadata_tags "$actual_rules")"

    assert_exact_value "${description} tag contract" "$actual_tags" "$expected_tags"

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
    $'edge\nsha-0123456'

run_success_case \
    'stable release tag resolves semantic publication outputs' \
    'refs/tags/v1.2.3' \
    'v1.2.3' \
    $'1.2.3\n1.2\n1\nlatest'

run_failure_case 'noncanonical release tag with leading zero' 'refs/tags/v01.2.3' 'v01.2.3'
run_failure_case 'noncanonical prerelease tag' 'refs/tags/v1.2.3-rc1' 'v1.2.3-rc1'
run_failure_case 'non-main branch ref' 'refs/heads/release' 'release'

printf 'PASS: publish ref behavior verified\n'
