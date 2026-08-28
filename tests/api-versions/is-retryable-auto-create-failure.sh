#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 || ! -f "$2" ]]; then
  printf 'usage: %s SCENARIO FAILURE_LOG\n' "$0" >&2
  exit 2
fi

readonly SCENARIO=$1
readonly FAILURE_LOG=$2

if grep -E '^timed out after [0-9]+s: ' "${FAILURE_LOG}" >/dev/null; then
  exit 1
fi

case "${SCENARIO}" in
  confluent-kafka-2.15.0)
    grep -F "metadata for 'auto-" "${FAILURE_LOG}" >/dev/null \
      && grep -F 'failed: UnknownTopicOrPart' "${FAILURE_LOG}" >/dev/null
    ;;
  rskafka-0.6.0)
    grep -F 'partition client: ServerError { protocol_error: UnknownTopicOrPartition' \
      "${FAILURE_LOG}" >/dev/null \
      && grep -F 'request: Topic("rust-delivery-' "${FAILURE_LOG}" >/dev/null
    ;;
  *)
    exit 1
    ;;
esac
