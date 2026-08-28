#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repository_root="$(cd -- "$script_dir/../.." && pwd)"

cargo build --locked --release --manifest-path "$repository_root/Cargo.toml"
cargo build --locked --release --manifest-path "$script_dir/Cargo.toml"

cd -- "$repository_root"
exec "$script_dir/target/release/memkafka-throughput-benchmark" \
  --broker-binary "$repository_root/target/release/memkafka" \
  --runs 3 \
  --output-json "$repository_root/docs/benchmarks/latest.json" \
  --output-svg "$repository_root/docs/benchmarks/throughput.svg" \
  "$@"
