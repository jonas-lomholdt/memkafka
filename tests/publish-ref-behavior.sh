#!/usr/bin/env bash
set -euo pipefail

CONTEXT_SCRIPT="scripts/ci/resolve-publish-context.sh"
ALIAS_SCRIPT="scripts/ci/select-release-aliases.sh"
TEST_SHA="0123456789abcdef0123456789abcdef01234567"
WRAPPER_SHA="89abcdef0123456789abcdef0123456789abcdef"

fail() {
    printf 'FAIL: %s\n' "$1" >&2
    exit 1
}

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

    fail "missing completed multiline output $key in $output_file"
}

read_single_output() {
    local output_file="$1"
    local key="$2"
    local line=""

    while IFS= read -r line; do
        if [[ "$line" == "$key="* ]]; then
            printf '%s' "${line#*=}"
            return 0
        fi
    done < "$output_file"

    fail "missing single-line output $key in $output_file"
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

assert_file_contains() {
    local description="$1"
    local source_file="$2"
    local expected="$3"

    if ! grep -F -- "$expected" "$source_file" >/dev/null; then
        fail "$description: $source_file does not contain $expected"
    fi
}

run_context_success_case() {
    local description="$1"
    local ref="$2"
    local ref_name="$3"
    local expected_channel="$4"
    local expected_primary_tag="$5"
    local expected_aliases="$6"
    local expected_version="$7"

    local output_file
    output_file="$(mktemp)"

    MEMKAFKA_PUBLISH_REF="$ref" \
    MEMKAFKA_PUBLISH_REF_NAME="$ref_name" \
    MEMKAFKA_PUBLISH_SHA="$TEST_SHA" \
    GITHUB_REF='refs/heads/wrapper' \
    GITHUB_REF_NAME='wrapper' \
    GITHUB_SHA="$WRAPPER_SHA" \
    GITHUB_OUTPUT="$output_file" \
    "$CONTEXT_SCRIPT"

    assert_exact_value \
        "$description channel" \
        "$(read_single_output "$output_file" channel)" \
        "$expected_channel"
    assert_exact_value \
        "$description primary tag" \
        "$(read_single_output "$output_file" primary_tag)" \
        "$expected_primary_tag"
    assert_exact_value \
        "$description alias candidates" \
        "$(read_multiline_output "$output_file" alias_candidates)" \
        "$expected_aliases"
    assert_exact_value \
        "$description version" \
        "$(read_single_output "$output_file" version)" \
        "$expected_version"
    assert_exact_value \
        "$description target SHA" \
        "$(read_single_output "$output_file" target_sha)" \
        "$TEST_SHA"

    rm -f "$output_file"
    printf 'PASS: %s\n' "$description"
}

run_fallback_success_case() {
    local output_file
    output_file="$(mktemp)"

    GITHUB_REF='refs/tags/v1.2.3' \
    GITHUB_REF_NAME='v1.2.3' \
    GITHUB_SHA="$TEST_SHA" \
    GITHUB_OUTPUT="$output_file" \
    "$CONTEXT_SCRIPT"

    assert_exact_value \
        'direct release event primary tag' \
        "$(read_single_output "$output_file" primary_tag)" \
        '1.2.3'
    assert_exact_value \
        'direct release event alias candidates' \
        "$(read_multiline_output "$output_file" alias_candidates)" \
        $'1.2\n1\nlatest'

    rm -f "$output_file"
    printf 'PASS: direct release event uses GitHub fallback inputs\n'
}

run_context_failure_case() {
    local description="$1"
    local ref="$2"
    local ref_name="$3"
    local sha="${4:-$TEST_SHA}"

    local output_file
    output_file="$(mktemp)"

    if MEMKAFKA_PUBLISH_REF="$ref" \
        MEMKAFKA_PUBLISH_REF_NAME="$ref_name" \
        MEMKAFKA_PUBLISH_SHA="$sha" \
        GITHUB_OUTPUT="$output_file" \
        "$CONTEXT_SCRIPT" >/dev/null 2>&1; then
        rm -f "$output_file"
        fail "expected rejection for $description"
    fi

    rm -f "$output_file"
    printf 'PASS: %s rejected\n' "$description"
}

run_alias_case() {
    local description="$1"
    local current_version="$2"
    local remote_tags="$3"
    local expected_aliases="$4"
    local actual_aliases

    actual_aliases="$(printf '%s\n' "$remote_tags" | "$ALIAS_SCRIPT" "$current_version")"
    assert_exact_value "$description" "$actual_aliases" "$expected_aliases"
    printf 'PASS: %s\n' "$description"
}

assert_workflow_contract() {
    local publish_workflow='.github/workflows/publish.yml'
    local verify_workflow='.github/workflows/verify.yml'
    local ci_workflow='.github/workflows/ci.yml'
    local reusable_count

    assert_file_contains 'reusable verification trigger' "$verify_workflow" 'workflow_call:'
    assert_file_contains 'policy test belongs to full verification' "$verify_workflow" 'tests/publish-ref-behavior.sh'
    assert_file_contains 'CI delegates to reusable verification' "$ci_workflow" 'uses: ./.github/workflows/verify.yml'
    reusable_count="$(grep -F -c 'uses: ./.github/workflows/verify.yml' "$ci_workflow")"
    assert_exact_value 'CI calls reusable verification exactly once' "$reusable_count" '1'

    assert_file_contains 'main publication follows CI completion' "$publish_workflow" 'workflow_run:'
    assert_file_contains 'main publication requires CI push event' "$publish_workflow" "github.event.workflow_run.event == 'push'"
    assert_file_contains 'main publication requires CI success' "$publish_workflow" "github.event.workflow_run.conclusion == 'success'"
    assert_file_contains 'main publication requires main branch' "$publish_workflow" "github.event.workflow_run.head_branch == 'main'"
    assert_file_contains 'main publication requires same repository' "$publish_workflow" 'github.event.workflow_run.head_repository.full_name == github.repository'
    assert_file_contains 'release publication delegates to full verification' "$publish_workflow" 'uses: ./.github/workflows/verify.yml'
    assert_file_contains 'main publication uses verified SHA' "$publish_workflow" 'github.event.workflow_run.head_sha'
    assert_file_contains 'publication resolves checked-out commit' "$publish_workflow" 'git rev-parse --verify HEAD^{commit}'
    assert_file_contains 'publication context uses checked-out commit' "$publish_workflow" 'MEMKAFKA_PUBLISH_SHA: ${{ steps.source.outputs.target_sha }}'
    assert_file_contains 'primary image build has an id' "$publish_workflow" 'id: build'
    assert_file_contains 'primary image build captures its digest' "$publish_workflow" 'steps.build.outputs.digest'
    assert_file_contains 'aliases are promoted from the digest' "$publish_workflow" 'docker buildx imagetools create'
    assert_file_contains 'main freshness reads the remote head' "$publish_workflow" 'refs/heads/main'
    assert_file_contains 'release aliases read remote tags' "$publish_workflow" 'git ls-remote --tags --refs'
    assert_file_contains 'mainline publication cancels obsolete runs' "$publish_workflow" 'cancel-in-progress: ${{ github.event_name == '\''workflow_run'\'' }}'

    assert_file_contains 'checkout action is pinned' "$publish_workflow" 'actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7'
    assert_file_contains 'QEMU action is pinned' "$publish_workflow" 'docker/setup-qemu-action@96fe6ef7f33517b61c61be40b68a1882f3264fb8 # v4'
    assert_file_contains 'Buildx action is pinned' "$publish_workflow" 'docker/setup-buildx-action@37fe631027851001ddb9b187196cc803df7f5f0e # v4'
    assert_file_contains 'registry login action is pinned' "$publish_workflow" 'docker/login-action@dbcb813823bdd20940b903addbd779551569679f # v4'
    assert_file_contains 'metadata action is pinned' "$publish_workflow" 'docker/metadata-action@dc802804100637a589fabce1cb79ff13a1411302 # v6'
    assert_file_contains 'image build action is pinned' "$publish_workflow" 'docker/build-push-action@53b7df96c91f9c12dcc8a07bcb9ccacbed38856a # v7'

    if grep -E 'uses: (actions|docker)/[^@]+@v[0-9]+' "$publish_workflow" >/dev/null; then
        fail 'publish workflow contains an action pinned only to a movable major tag'
    fi

    printf 'PASS: workflow publication contract verified\n'
}

run_context_success_case \
    'workflow-run main resolves verified full-SHA publication context' \
    'refs/heads/main' \
    'main' \
    'main' \
    "sha-$TEST_SHA" \
    'edge' \
    "sha-$TEST_SHA"

run_context_success_case \
    'stable release resolves exact primary and candidate aliases' \
    'refs/tags/v1.2.3' \
    'v1.2.3' \
    'release' \
    '1.2.3' \
    $'1.2\n1\nlatest' \
    '1.2.3'

run_fallback_success_case

run_context_failure_case 'noncanonical release tag with leading zero' 'refs/tags/v01.2.3' 'v01.2.3'
run_context_failure_case 'noncanonical prerelease tag' 'refs/tags/v1.2.3-rc1' 'v1.2.3-rc1'
run_context_failure_case 'non-main branch ref' 'refs/heads/release' 'release'
run_context_failure_case 'mismatched main ref name' 'refs/heads/main' 'release'
run_context_failure_case 'mismatched release ref name' 'refs/tags/v1.2.3' 'v1.2.4'
run_context_failure_case 'invalid target SHA' 'refs/heads/main' 'main' 'not-a-full-git-sha'

overlong_component="$(printf '1%.0s' {1..126})"
run_context_failure_case \
    'release producing an overlong Docker tag' \
    "refs/tags/v${overlong_component}.1.1" \
    "v${overlong_component}.1.1"

run_alias_case \
    'only stable version moves every release alias' \
    '1.2.3' \
    'v1.2.3' \
    $'1.2\n1\nlatest'

run_alias_case \
    'newer patch blocks older patch from every alias' \
    '1.2.3' \
    $'v1.2.4\nv1.2.3' \
    ''

run_alias_case \
    'newer major blocks only the global latest alias' \
    '1.2.3' \
    $'v2.0.0\nv1.2.3\nv0.99.99' \
    $'1.2\n1'

run_alias_case \
    'newer minor preserves only the current minor alias' \
    '1.2.3' \
    $'v1.3.0\nv1.2.3' \
    '1.2'

run_alias_case \
    'unordered canonical tags are compared by numeric components' \
    '10.20.30' \
    $'v10.20.29\nv9.999.999\nv10.20.30\nv10.19.999\nvnot-a-release\nv01.2.3' \
    $'10.20\n10\nlatest'

huge_major="$(printf '9%.0s' {1..40})"
huge_minor="$(printf '8%.0s' {1..40})"
huge_patch="$(printf '7%.0s' {1..39})"
larger_huge_patch="1$(printf '0%.0s' {1..39})"
run_alias_case \
    'huge numeric components compare without arithmetic overflow' \
    "$huge_major.$huge_minor.$huge_patch" \
    "v$huge_major.$huge_minor.$larger_huge_patch" \
    ''

assert_workflow_contract

printf 'PASS: publish ref and alias behavior verified\n'
