#!/usr/bin/env bash
set -euo pipefail

ref="${GITHUB_REF:-}"
ref_name="${GITHUB_REF_NAME:-}"
output_file="${GITHUB_OUTPUT:-}"

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

tag_rules=""

case "$ref" in
    refs/heads/main)
        tag_rules=$'type=raw,value=edge\ntype=sha,prefix=sha-,format=short'
        ;;
    refs/tags/*)
        if [[ "$ref" != "refs/tags/$ref_name" ]]; then
            printf 'Expected GITHUB_REF (%s) to match GITHUB_REF_NAME (%s)\n' "$ref" "$ref_name" >&2
            exit 1
        fi

        if [[ "$ref_name" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
            tag_rules=$'type=raw,value='"${BASH_REMATCH[1]}.${BASH_REMATCH[2]}.${BASH_REMATCH[3]}"$'\n''type=raw,value='"${BASH_REMATCH[1]}.${BASH_REMATCH[2]}"$'\n''type=raw,value='"${BASH_REMATCH[1]}"$'\n''type=raw,value=latest'
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

emit_multiline_output tags "$tag_rules"
