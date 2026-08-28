#!/usr/bin/env bash
set -euo pipefail

readonly SCRIPT_DIRECTORY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly PREDICATE="${SCRIPT_DIRECTORY}/is-retryable-auto-create-failure.sh"
readonly FIXTURES="${SCRIPT_DIRECTORY}/fixtures/auto-create-retry"

assert_retryable() {
  local scenario=$1
  local log=$2

  if ! "${PREDICATE}" "${scenario}" "${FIXTURES}/${log}"; then
    printf 'expected retryable failure: %s %s\n' "${scenario}" "${log}" >&2
    return 1
  fi
}

assert_not_retryable() {
  local scenario=$1
  local log=$2

  if "${PREDICATE}" "${scenario}" "${FIXTURES}/${log}"; then
    printf 'unexpected retryable failure: %s %s\n' "${scenario}" "${log}" >&2
    return 1
  fi
}

assert_retryable confluent-kafka-2.15.0 confluent-exact.txt
assert_retryable rskafka-0.6.0 rust-exact.txt
assert_not_retryable rskafka-0.6.0 rust-timeout.txt
assert_not_retryable rskafka-0.6.0 rust-wrong-topic.txt
assert_not_retryable rskafka-0.6.0 rust-second-failure.txt
assert_not_retryable franz-go-1.21.6 rust-exact.txt
assert_not_retryable apache-kafka-java-4.3.1 generic-unknown-topic.txt
assert_not_retryable confluent-kafka-flow-2.13.2 generic-unknown-topic.txt
assert_not_retryable kafbat-1.5.0 generic-unknown-topic.txt

printf 'PASS   Kafka API version auto-create retry predicate\n'
