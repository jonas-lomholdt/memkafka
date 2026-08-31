#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
schema_dir="$repo_root/crates/kafka-protocol/protocol_codegen/schema/kafka-4.3.1/message"
manifest_path="$repo_root/crates/kafka-protocol/protocol_codegen/Cargo.toml"
messages_dir="$repo_root/crates/kafka-protocol/src/messages"

case "$#" in
    0)
        cd "$repo_root"
        cargo run --locked --manifest-path "$manifest_path" -- \
            --schema-dir "$schema_dir" \
            --output-dir "$messages_dir"
        ;;
    1)
        if [[ "$1" != "--check" ]]; then
            echo "usage: ${0##*/} [--check]" >&2
            exit 2
        fi

        temporary_dir="$(mktemp -d)"
        trap 'rm -rf "$temporary_dir"' EXIT
        mkdir -p "$temporary_dir/src/messages"

        cd "$repo_root"
        cargo run --locked --manifest-path "$manifest_path" -- \
            --schema-dir "$schema_dir" \
            --output-dir "$temporary_dir/src/messages"
        diff -ru "$repo_root/crates/kafka-protocol/src/messages.rs" \
            "$temporary_dir/src/messages.rs"
        diff -ru "$repo_root/crates/kafka-protocol/src/messages" \
            "$temporary_dir/src/messages"
        ;;
    *)
        echo "usage: ${0##*/} [--check]" >&2
        exit 2
        ;;
esac
