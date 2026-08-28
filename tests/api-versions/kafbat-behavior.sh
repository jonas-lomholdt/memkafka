#!/usr/bin/env bash
set -euo pipefail

readonly SCRIPT_DIRECTORY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly KAFBAT_SCRIPT="${SCRIPT_DIRECTORY}/kafbat.sh"
readonly FAKE_COMMAND="${SCRIPT_DIRECTORY}/fixtures/kafbat-fake-command.sh"
readonly TEST_ROOT="$(mktemp -d)"
readonly FAKE_BIN="${TEST_ROOT}/bin"

cleanup() {
  rm -rf "${TEST_ROOT}"
}
trap cleanup EXIT

mkdir -p "${FAKE_BIN}"
ln -s "${FAKE_COMMAND}" "${FAKE_BIN}/curl"
ln -s "${FAKE_COMMAND}" "${FAKE_BIN}/docker"
ln -s "${FAKE_COMMAND}" "${FAKE_BIN}/sleep"

run_kafbat() {
  local name=$1
  local recorder_writes=$2
  local require_max_time=$3
  local log_dir="${TEST_ROOT}/${name}-logs"
  local output="${TEST_ROOT}/${name}.out"
  mkdir -p "${log_dir}"

  set +e
  env \
    PATH="${FAKE_BIN}:${PATH}" \
    FAKE_CURL_LOG="${TEST_ROOT}/${name}-curl.log" \
    FAKE_DOCKER_LOG="${TEST_ROOT}/${name}-docker.log" \
    FAKE_LOG_DIR="${log_dir}" \
    FAKE_RECORDER_WRITES="${recorder_writes}" \
    FAKE_REQUIRE_MAX_TIME="${require_max_time}" \
    MEMKAFKA_KAFBAT_LOG_DIR="${log_dir}" \
    "${KAFBAT_SCRIPT}" >"${output}" 2>&1
  RUN_STATUS=$?
  set -e
  RUN_LOG_DIR="${log_dir}"
  RUN_OUTPUT="${output}"
}

test_only_kafbat_uses_the_recorder_listener() {
  local docker_log

  run_kafbat topology true false
  if ((RUN_STATUS != 0)); then
    cat "${RUN_OUTPUT}" >&2
    printf 'expected Kafbat scenario to succeed while checking listener topology\n' >&2
    return 1
  fi
  docker_log="${TEST_ROOT}/topology-docker.log"
  if ! grep -F \
      'KAFKA_ADVERTISED_LISTENERS=PROXY://api-version-proxy:9092,DIRECT://kafka:19094' \
      "${docker_log}" >/dev/null; then
    cat "${docker_log}" >&2
    printf 'Kafka did not advertise distinct proxy and direct listeners\n' >&2
    return 1
  fi
  if ! grep -F 'KAFKA_INTER_BROKER_LISTENER_NAME=DIRECT' \
      "${docker_log}" >/dev/null; then
    cat "${docker_log}" >&2
    printf 'Kafka did not declare the direct inter-broker listener\n' >&2
    return 1
  fi
  if ! grep -F 'KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=api-version-proxy:9092' \
      "${docker_log}" >/dev/null; then
    cat "${docker_log}" >&2
    printf 'Kafbat did not bootstrap through the recorder listener\n' >&2
    return 1
  fi
  if ! grep -F 'MEMKAFKA_BOOTSTRAP_SERVERS=kafka:19094' \
      "${docker_log}" >/dev/null; then
    cat "${docker_log}" >&2
    printf 'setup seed did not bootstrap through the direct Kafka listener\n' >&2
    return 1
  fi
}

test_stale_observations_are_replaced() {
  local log_dir="${TEST_ROOT}/stale-logs"
  mkdir -p "${log_dir}"
  printf 'stale observation\n' >"${log_dir}/kafbat-1.5.0.jsonl"

  run_kafbat stale true false
  if ((RUN_STATUS != 0)); then
    cat "${RUN_OUTPUT}" >&2
    printf 'expected Kafbat scenario to succeed while testing observation replacement\n' >&2
    return 1
  fi
  if [[ "$(<"${RUN_LOG_DIR}/kafbat-1.5.0.jsonl")" == *"stale observation"* ]]; then
    printf 'Kafbat scenario retained stale recorder observations\n' >&2
    return 1
  fi
}

test_every_http_request_has_a_maximum_time() {
  run_kafbat max-time true true
  if ((RUN_STATUS != 0)); then
    cat "${RUN_OUTPUT}" >&2
    cat "${TEST_ROOT}/max-time-curl.log" >&2
    printf 'Kafbat scenario issued an HTTP request without --max-time\n' >&2
    return 1
  fi
}

test_empty_recorder_evidence_fails_the_scenario() {
  run_kafbat empty false false
  if ((RUN_STATUS == 0)); then
    printf 'Kafbat scenario succeeded without recorder observations\n' >&2
    return 1
  fi
  if [[ "$(<"${RUN_OUTPUT}")" != *"recorder did not observe any Kafka requests"* ]]; then
    cat "${RUN_OUTPUT}" >&2
    printf 'Kafbat scenario failed without the expected empty-recorder diagnostic\n' >&2
    return 1
  fi
}

case "${1:-all}" in
  stale)
    test_stale_observations_are_replaced
    ;;
  max-time)
    test_every_http_request_has_a_maximum_time
    ;;
  nonempty)
    test_empty_recorder_evidence_fails_the_scenario
    ;;
  topology)
    test_only_kafbat_uses_the_recorder_listener
    ;;
  all)
    test_only_kafbat_uses_the_recorder_listener
    test_stale_observations_are_replaced
    test_every_http_request_has_a_maximum_time
    test_empty_recorder_evidence_fails_the_scenario
    ;;
  *)
    printf 'usage: %s [stale|max-time|nonempty|topology|all]\n' "$0" >&2
    exit 2
    ;;
esac

printf 'PASS   Kafka-oracle Kafbat script behavior\n'
