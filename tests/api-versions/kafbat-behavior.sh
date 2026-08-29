#!/usr/bin/env bash
set -euo pipefail

readonly SCRIPT_DIRECTORY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly KAFBAT_SCRIPT="${SCRIPT_DIRECTORY}/kafbat.sh"
readonly FAKE_COMMAND="${SCRIPT_DIRECTORY}/fixtures/kafbat-fake-command.sh"
readonly TEST_ROOT="$(mktemp -d)"
readonly FAKE_BIN="${TEST_ROOT}/bin"
readonly KAFKA_IMAGE="apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837"
readonly KAFBAT_IMAGE="ghcr.io/kafbat/kafka-ui:v1.5.0@sha256:7cda86a33344160309fdb65146332e4da65db81a945614f2fe32e210803f6fd1"
readonly PROXY_IMAGE="memkafka-api-version-proxy:test"
readonly SEED_IMAGE="memkafka-kafbat-seed:ci"

RUN_PULL_PID_FILE=""

terminate_exact_pid() {
  local process_id=$1

  if kill -0 "${process_id}" >/dev/null 2>&1; then
    kill -TERM "${process_id}" >/dev/null 2>&1 || true
  fi
}

cleanup() {
  if [[ -n "${RUN_PULL_PID_FILE}" && -s "${RUN_PULL_PID_FILE}" ]]; then
    terminate_exact_pid "$(<"${RUN_PULL_PID_FILE}")"
  fi
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
  local missing_images=${4:-}
  local pull_mode=${5:-success}
  local pull_timeout=${6:-300}
  local log_dir="${TEST_ROOT}/${name}-logs"
  local output="${TEST_ROOT}/${name}.out"
  mkdir -p "${log_dir}"
  RUN_PULL_PID_FILE="${TEST_ROOT}/${name}-pull.pid"

  set +e
  env \
    PATH="${FAKE_BIN}:${PATH}" \
    FAKE_CURL_LOG="${TEST_ROOT}/${name}-curl.log" \
    FAKE_DOCKER_LOG="${TEST_ROOT}/${name}-docker.log" \
    FAKE_IMAGE_STATE_FILE="${TEST_ROOT}/${name}-images.txt" \
    FAKE_MISSING_IMAGES="${missing_images}" \
    FAKE_PULL_MODE="${pull_mode}" \
    FAKE_PULL_PID_FILE="${RUN_PULL_PID_FILE}" \
    FAKE_LOG_DIR="${log_dir}" \
    FAKE_RECORDER_WRITES="${recorder_writes}" \
    FAKE_REQUIRE_MAX_TIME="${require_max_time}" \
    MEMKAFKA_API_VERSION_IMAGE_PULL_TIMEOUT_SECONDS="${pull_timeout}" \
    MEMKAFKA_API_VERSION_TERMINATION_GRACE_SECONDS=1 \
    MEMKAFKA_KAFBAT_LOG_DIR="${log_dir}" \
    "${KAFBAT_SCRIPT}" >"${output}" 2>&1
  RUN_STATUS=$?
  set -e
  RUN_LOG_DIR="${log_dir}"
  RUN_OUTPUT="${output}"
}

assert_pull_log_contains() {
  local log_file=$1
  local expected=$2

  if [[ ! -f "${RUN_LOG_DIR}/${log_file}" ]]; then
    printf 'missing retained pull log: %s\n' "${RUN_LOG_DIR}/${log_file}" >&2
    return 1
  fi
  grep -F -- "${expected}" "${RUN_LOG_DIR}/${log_file}" >/dev/null
}

test_cold_remote_images_are_pulled_before_containers_start() {
  local first_run_line
  local kafbat_pull_line

  run_kafbat cold-images true false "${KAFKA_IMAGE} ${KAFBAT_IMAGE}" success 7
  if ((RUN_STATUS != 0)); then
    cat "${RUN_OUTPUT}" >&2
    printf 'cold Kafbat scenario did not prepare missing pinned images\n' >&2
    return 1
  fi
  grep -Fx "pull ${KAFKA_IMAGE}" "${TEST_ROOT}/cold-images-docker.log" >/dev/null
  grep -Fx "pull ${KAFBAT_IMAGE}" "${TEST_ROOT}/cold-images-docker.log" >/dev/null
  if grep -E "^pull (${PROXY_IMAGE}|${SEED_IMAGE})$" \
      "${TEST_ROOT}/cold-images-docker.log" >/dev/null; then
    cat "${TEST_ROOT}/cold-images-docker.log" >&2
    printf 'standalone Kafbat tried to pull a locally owned image\n' >&2
    return 1
  fi
  kafbat_pull_line="$(grep -nF "pull ${KAFBAT_IMAGE}" \
    "${TEST_ROOT}/cold-images-docker.log" | cut -d: -f1)"
  first_run_line="$(grep -n '^run ' "${TEST_ROOT}/cold-images-docker.log" | head -1 | cut -d: -f1)"
  if ((kafbat_pull_line >= first_run_line)); then
    cat "${TEST_ROOT}/cold-images-docker.log" >&2
    printf 'remote pulls did not finish before Kafbat containers started\n' >&2
    return 1
  fi
  assert_pull_log_contains kafka-image-pull.log \
    "simulated pulled image: ${KAFKA_IMAGE}"
  assert_pull_log_contains kafbat-image-pull.log \
    "simulated pulled image: ${KAFBAT_IMAGE}"
}

test_warm_remote_images_are_not_pulled() {
  run_kafbat warm-images true false
  if ((RUN_STATUS != 0)); then
    cat "${RUN_OUTPUT}" >&2
    printf 'warm-cache Kafbat scenario failed\n' >&2
    return 1
  fi
  if grep -E '^pull ' "${TEST_ROOT}/warm-images-docker.log" >/dev/null; then
    cat "${TEST_ROOT}/warm-images-docker.log" >&2
    printf 'warm-cache Kafbat scenario performed an unnecessary pull\n' >&2
    return 1
  fi
  assert_pull_log_contains kafka-image-pull.log \
    "using cached pinned Kafka image: ${KAFKA_IMAGE}"
  assert_pull_log_contains kafbat-image-pull.log \
    "using cached pinned Kafbat image: ${KAFBAT_IMAGE}"
}

test_pull_failure_stops_and_retains_actionable_log() {
  run_kafbat pull-failure true false "${KAFKA_IMAGE}" fail 7
  if ((RUN_STATUS != 93)); then
    cat "${RUN_OUTPUT}" >&2
    printf 'failed Kafbat image pull exited %d, expected 93\n' "${RUN_STATUS}" >&2
    return 1
  fi
  if [[ "$(<"${RUN_OUTPUT}")" != *"failed to pull pinned Kafka image within 7s; retained log: ${RUN_LOG_DIR}/kafka-image-pull.log"* ]]; then
    cat "${RUN_OUTPUT}" >&2
    printf 'failed image pull did not identify its deadline and retained log\n' >&2
    return 1
  fi
  if grep -E '^run ' "${TEST_ROOT}/pull-failure-docker.log" >/dev/null; then
    cat "${TEST_ROOT}/pull-failure-docker.log" >&2
    printf 'Kafbat started containers after a failed image pull\n' >&2
    return 1
  fi
  assert_pull_log_contains kafka-image-pull.log \
    "simulated pull failure: ${KAFKA_IMAGE}"
}

test_pull_timeout_stops_reaps_and_retains_actionable_log() {
  run_kafbat pull-timeout true false "${KAFKA_IMAGE}" hang 1
  if ((RUN_STATUS != 124)); then
    cat "${RUN_OUTPUT}" >&2
    printf 'timed-out Kafbat image pull exited %d, expected 124\n' "${RUN_STATUS}" >&2
    return 1
  fi
  assert_pull_log_contains kafka-image-pull.log \
    'timed out after 1s: pull pinned Kafka image'
  if [[ ! -s "${RUN_PULL_PID_FILE}" ]]; then
    printf 'timed-out Kafbat image pull never recorded its PID\n' >&2
    return 1
  fi
  if kill -0 "$(<"${RUN_PULL_PID_FILE}")" >/dev/null 2>&1; then
    printf 'timed-out Kafbat image pull is still running\n' >&2
    return 1
  fi
}

test_missing_local_image_fails_without_registry_pull() {
  run_kafbat local-image true false "${PROXY_IMAGE}"
  if ((RUN_STATUS == 0)); then
    printf 'Kafbat scenario accepted a missing locally owned proxy image\n' >&2
    return 1
  fi
  if grep -Fx "pull ${PROXY_IMAGE}" "${TEST_ROOT}/local-image-docker.log" >/dev/null; then
    cat "${TEST_ROOT}/local-image-docker.log" >&2
    printf 'Kafbat tried to registry-pull the locally owned proxy image\n' >&2
    return 1
  fi
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
  images)
    test_cold_remote_images_are_pulled_before_containers_start
    test_warm_remote_images_are_not_pulled
    test_pull_failure_stops_and_retains_actionable_log
    test_pull_timeout_stops_reaps_and_retains_actionable_log
    test_missing_local_image_fails_without_registry_pull
    ;;
  all)
    test_cold_remote_images_are_pulled_before_containers_start
    test_warm_remote_images_are_not_pulled
    test_pull_failure_stops_and_retains_actionable_log
    test_pull_timeout_stops_reaps_and_retains_actionable_log
    test_missing_local_image_fails_without_registry_pull
    test_only_kafbat_uses_the_recorder_listener
    test_stale_observations_are_replaced
    test_every_http_request_has_a_maximum_time
    test_empty_recorder_evidence_fails_the_scenario
    ;;
  *)
    printf 'usage: %s [stale|max-time|nonempty|topology|images|all]\n' "$0" >&2
    exit 2
    ;;
esac

printf 'PASS   Kafka-oracle Kafbat script behavior\n'
