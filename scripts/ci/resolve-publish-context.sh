#!/usr/bin/env bash
set -euo pipefail

ref="${GITHUB_REF:-}"
ref_name="${GITHUB_REF_NAME:-}"
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

case "$ref" in
    refs/heads/main)
        emit_output channel edge
        ;;
    refs/tags/*)
        if [[ "$ref" != "refs/tags/$ref_name" ]]; then
            printf 'Expected GITHUB_REF (%s) to match GITHUB_REF_NAME (%s)\n' "$ref" "$ref_name" >&2
            exit 1
        fi

        if [[ "$ref_name" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
            emit_output channel release
            emit_output version "${BASH_REMATCH[1]}.${BASH_REMATCH[2]}.${BASH_REMATCH[3]}"
            emit_output major_minor "${BASH_REMATCH[1]}.${BASH_REMATCH[2]}"
            emit_output major "${BASH_REMATCH[1]}"
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
