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
    failed_test_count="$(grep -Ec '^test .* \.\.\. FAILED$' "${FAILURE_LOG}" || true)"
    failed_summary_count="$(grep -Ec '^test result: FAILED\.' "${FAILURE_LOG}" || true)"
    [[ "${failed_test_count}" == 1 ]] \
      && [[ "${failed_summary_count}" == 1 ]] \
      && grep -Fx \
        'test publishes_and_fetches_in_order_then_reads_uncommitted_records_again ... FAILED' \
        "${FAILURE_LOG}" >/dev/null \
      && grep -E \
        '^test result: FAILED\. 3 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in .+$' \
        "${FAILURE_LOG}" >/dev/null \
      && grep -E \
        '^partition client: ServerError \{ protocol_error: UnknownTopicOrPartition, .*request: Topic\("rust-delivery-[^"]+"\),' \
        "${FAILURE_LOG}" >/dev/null
    ;;
  *)
    exit 1
    ;;
esac
