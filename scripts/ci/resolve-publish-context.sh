#!/usr/bin/env bash
set -euo pipefail

ref="${MEMKAFKA_PUBLISH_REF:-${GITHUB_REF:-}}"
ref_name="${MEMKAFKA_PUBLISH_REF_NAME:-${GITHUB_REF_NAME:-}}"
target_sha="${MEMKAFKA_PUBLISH_SHA:-${GITHUB_SHA:-}}"
output_file="${GITHUB_OUTPUT:-}"

emit_output() {
    local key="$1"
    local value="$2"

    if [[ -n "$output_file" ]]; then
        printf '%s=%s\n' "$key" "$value" >> "$output_file"
    else
        printf '%s=%s\n' "$key" "$value"
    fi
}

emit_multiline_output() {
    local key="$1"
    local value="$2"
    local delimiter="MEMKAFKA_${key}_EOF"

    if printf '%s\n' "$value" | grep -Fx -- "$delimiter" >/dev/null; then
        printf 'Refusing to emit GitHub output %s because delimiter %s appears in the value\n' "$key" "$delimiter" >&2
        exit 1
    fi

    if [[ -n "$output_file" ]]; then
        {
            printf '%s<<%s\n' "$key" "$delimiter"
            printf '%s\n' "$value"
            printf '%s\n' "$delimiter"
        } >> "$output_file"
    else
        printf '%s\n' "$value"
    fi
}

validate_docker_tag() {
    local tag="$1"

    if (( ${#tag} > 128 )); then
        printf 'Refusing Docker tag longer than 128 characters: %s\n' "$tag" >&2
        exit 1
    fi

    if [[ ! "$tag" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]*$ ]]; then
        printf 'Refusing invalid Docker tag: %s\n' "$tag" >&2
        exit 1
    fi
}

if [[ ! "$target_sha" =~ ^[0-9a-f]{40}$ ]]; then
    printf 'Expected a full 40-character lowercase Git SHA, got %s\n' "$target_sha" >&2
    exit 1
fi

channel=""
primary_tag=""
alias_candidates=""
version=""

case "$ref" in
    refs/heads/main)
        if [[ "$ref_name" != "main" ]]; then
            printf 'Expected ref name main for %s, got %s\n' "$ref" "$ref_name" >&2
            exit 1
        fi

        channel="main"
        primary_tag="sha-$target_sha"
        alias_candidates="edge"
        version="$primary_tag"
        ;;
    refs/tags/*)
        if [[ "$ref" != "refs/tags/$ref_name" ]]; then
            printf 'Expected publish ref (%s) to match publish ref name (%s)\n' "$ref" "$ref_name" >&2
            exit 1
        fi

        if [[ "$ref_name" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
            major="${BASH_REMATCH[1]}"
            minor="${BASH_REMATCH[2]}"
            patch="${BASH_REMATCH[3]}"
            channel="release"
            primary_tag="$major.$minor.$patch"
            alias_candidates=$major.$minor$'\n'$major$'\nlatest'
            version="$primary_tag"
        else
            printf 'Expected refs/heads/main or canonical stable tag vMAJOR.MINOR.PATCH, got %s\n' "$ref" >&2
            exit 1
        fi
        ;;
    *)
        printf 'Expected refs/heads/main or canonical stable tag vMAJOR.MINOR.PATCH, got %s\n' "$ref" >&2
        exit 1
        ;;
esac

validate_docker_tag "$primary_tag"
while IFS= read -r alias; do
    [[ -n "$alias" ]] || continue
    validate_docker_tag "$alias"
done <<< "$alias_candidates"

emit_output channel "$channel"
emit_output primary_tag "$primary_tag"
emit_multiline_output alias_candidates "$alias_candidates"
emit_output version "$version"
emit_output target_sha "$target_sha"
