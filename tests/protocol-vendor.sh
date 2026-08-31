#!/usr/bin/env bash

set -euo pipefail

fail() {
    printf 'protocol vendor boundary check failed: %s\n' "$*" >&2
    exit 1
}

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

test -d crates/kafka-protocol || fail 'crates/kafka-protocol is missing'
test -f crates/kafka-protocol/Cargo.toml || fail 'vendored kafka-protocol Cargo.toml is missing'
test -f crates/kafka-protocol/LICENSE-APACHE || fail 'vendored Apache license is missing'
test -f crates/kafka-protocol/LICENSE-MIT || fail 'vendored MIT license is missing'
test -f crates/kafka-protocol/UPSTREAM.md || fail 'vendored provenance record is missing'
test ! -d crates/kafka-protocol/.git || fail 'vendored subtree must not contain a .git directory'

metadata=$(cargo metadata --format-version 1 --no-deps) || fail 'cargo metadata could not read the workspace'
if ! printf '%s' "$metadata" | python3 -c '
import json
import pathlib
import sys

metadata = json.load(sys.stdin)
root = pathlib.Path.cwd()
vendor = root / "crates" / "kafka-protocol"

root_package = next(package for package in metadata["packages"]
                    if pathlib.Path(package["manifest_path"]) == root / "Cargo.toml")
dependency = next((dependency for dependency in root_package["dependencies"]
                   if dependency["name"] == "kafka-protocol"), None)
assert dependency is not None
assert pathlib.Path(dependency["path"]) == vendor
assert dependency["req"] == "^0.18.0-memkafka.1"
assert dependency["uses_default_features"] is False
assert dependency["features"] == ["broker", "client", "messages_enums"]

vendor_metadata = json.loads(__import__("subprocess").check_output([
    "cargo", "metadata", "--format-version", "1", "--no-deps", "--manifest-path",
    str(vendor / "Cargo.toml"),
], text=True))
vendor_package = next(package for package in vendor_metadata["packages"]
                      if pathlib.Path(package["manifest_path"]) == vendor / "Cargo.toml")
assert vendor_package["name"] == "kafka-protocol"
assert vendor_package["version"] == "0.18.0-memkafka.1"
assert pathlib.Path(vendor_metadata["workspace_root"]) == vendor
assert vendor_package["id"] not in metadata["workspace_members"]
'; then
    fail 'cargo metadata does not describe the required path dependency and workspace boundary'
fi

grep -Fqx 'Upstream: https://github.com/kafka-protocol-rs/kafka-protocol-rs' crates/kafka-protocol/UPSTREAM.md || fail 'UPSTREAM.md does not name the required upstream'
grep -Fqx 'Base commit: f0abe7eb99d3d54f48120bf1918623c71ba67cce' crates/kafka-protocol/UPSTREAM.md || fail 'UPSTREAM.md does not name the required base commit'
grep -Fqx 'Kafka release: 4.3.1' crates/kafka-protocol/UPSTREAM.md || fail 'UPSTREAM.md does not name Kafka 4.3.1'
grep -Fqx 'Kafka commit: 26b251a451ce941d3d7a55e6487bcb7f16b5ad48' crates/kafka-protocol/UPSTREAM.md || fail 'UPSTREAM.md does not name the required Kafka commit'

printf 'protocol vendor boundary: OK\n'
