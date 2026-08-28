#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C

current_version="${1:-}"

if [[ ! "$current_version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
    printf 'Expected canonical stable version MAJOR.MINOR.PATCH, got %s\n' "$current_version" >&2
    exit 1
fi

current_major="${BASH_REMATCH[1]}"
current_minor="${BASH_REMATCH[2]}"
current_patch="${BASH_REMATCH[3]}"

decimal_greater() {
    local left="$1"
    local right="$2"

    if (( ${#left} != ${#right} )); then
        (( ${#left} > ${#right} ))
        return
    fi

    [[ "$left" > "$right" ]]
}

version_greater() {
    local left_major="$1"
    local left_minor="$2"
    local left_patch="$3"
    local right_major="$4"
    local right_minor="$5"
    local right_patch="$6"

    if [[ "$left_major" != "$right_major" ]]; then
        decimal_greater "$left_major" "$right_major"
        return
    fi

    if [[ "$left_minor" != "$right_minor" ]]; then
        decimal_greater "$left_minor" "$right_minor"
        return
    fi

    if [[ "$left_patch" != "$right_patch" ]]; then
        decimal_greater "$left_patch" "$right_patch"
        return
    fi

    return 1
}

move_minor="true"
move_major="true"
move_latest="true"

while IFS= read -r remote_tag; do
    if [[ ! "$remote_tag" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
        continue
    fi

    remote_major="${BASH_REMATCH[1]}"
    remote_minor="${BASH_REMATCH[2]}"
    remote_patch="${BASH_REMATCH[3]}"

    if [[ "$remote_major" == "$current_major" && "$remote_minor" == "$current_minor" ]] \
        && decimal_greater "$remote_patch" "$current_patch"; then
        move_minor="false"
    fi

    if [[ "$remote_major" == "$current_major" ]] \
        && version_greater \
            "$remote_major" "$remote_minor" "$remote_patch" \
            "$current_major" "$current_minor" "$current_patch"; then
        move_major="false"
    fi

    if version_greater \
        "$remote_major" "$remote_minor" "$remote_patch" \
        "$current_major" "$current_minor" "$current_patch"; then
        move_latest="false"
    fi
done

if [[ "$move_minor" == "true" ]]; then
    printf '%s.%s\n' "$current_major" "$current_minor"
fi

if [[ "$move_major" == "true" ]]; then
    printf '%s\n' "$current_major"
fi

if [[ "$move_latest" == "true" ]]; then
    printf 'latest\n'
fi
