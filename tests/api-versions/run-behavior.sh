#!/usr/bin/env bash
set -euo pipefail
set -m

readonly SCRIPT_DIRECTORY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPOSITORY_ROOT="$(cd "${SCRIPT_DIRECTORY}/../.." && pwd)"
readonly RUN_SCRIPT="${SCRIPT_DIRECTORY}/run.sh"
readonly FAKE_COMMAND="${SCRIPT_DIRECTORY}/fixtures/run-fake-command.sh"
readonly SPAWN_SIGNAL_ENV="${SCRIPT_DIRECTORY}/fixtures/signal-during-spawn.bash"
readonly BOUNDED_COMMAND="${SCRIPT_DIRECTORY}/bounded-command.py"
readonly BOUNDED_COMMAND_BEHAVIOR="${SCRIPT_DIRECTORY}/bounded-command-behavior.py"
readonly REAL_PYTHON3="$(command -v python3)"
readonly TEST_ROOT="$(mktemp -d)"
readonly FAKE_BIN="${TEST_ROOT}/bin"
readonly KAFKA_IMAGE="apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837"
readonly KAFBAT_IMAGE="ghcr.io/kafbat/kafka-ui:v1.5.0@sha256:7cda86a33344160309fdb65146332e4da65db81a945614f2fe32e210803f6fd1"

RUNNER_PID=""
RUN_OUTPUT=""
FAKE_CHILD_PID_FILE=""
FAKE_DESCENDANT_PID_FILE=""
FAKE_SUPERVISOR_PID_FILE=""
FAKE_PULL_PID_FILE=""
FAKE_DOCKER_LOG=""
FAKE_EVENT_LOG=""
FAKE_SUPERVISOR_LOG=""
FAKE_IMAGE_STATE_FILE=""
SPAWN_SIGNAL_MARKER=""

terminate_exact_pid() {
  local process_id=$1

  if kill -0 "${process_id}" >/dev/null 2>&1; then
    kill -TERM "${process_id}" >/dev/null 2>&1 || true
    for _ in {1..20}; do
      if ! kill -0 "${process_id}" >/dev/null 2>&1; then
        return
      fi
      sleep 0.05
    done
    kill -KILL "${process_id}" >/dev/null 2>&1 || true
  fi
}

cleanup() {
  local exit_code=$?

  if [[ -n "${RUNNER_PID}" ]]; then
    terminate_exact_pid "${RUNNER_PID}"
    wait "${RUNNER_PID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${FAKE_CHILD_PID_FILE}" && -s "${FAKE_CHILD_PID_FILE}" ]]; then
    terminate_exact_pid "$(<"${FAKE_CHILD_PID_FILE}")"
  fi
  if [[ -n "${FAKE_DESCENDANT_PID_FILE}" && -s "${FAKE_DESCENDANT_PID_FILE}" ]]; then
    terminate_exact_pid "$(<"${FAKE_DESCENDANT_PID_FILE}")"
  fi
  if [[ -n "${FAKE_SUPERVISOR_PID_FILE}" && -s "${FAKE_SUPERVISOR_PID_FILE}" ]]; then
    terminate_exact_pid "$(<"${FAKE_SUPERVISOR_PID_FILE}")"
  fi
  if [[ -n "${FAKE_PULL_PID_FILE}" && -s "${FAKE_PULL_PID_FILE}" ]]; then
    terminate_exact_pid "$(<"${FAKE_PULL_PID_FILE}")"
  fi
  rm -rf "${TEST_ROOT}"
  return "${exit_code}"
}
trap cleanup EXIT

mkdir -p "${FAKE_BIN}"
for command_name in cargo docker dotnet go python3; do
  ln -s "${FAKE_COMMAND}" "${FAKE_BIN}/${command_name}"
done

start_runner() {
  local name=$1
  local fake_mode=$2
  local working_directory=$3
  local build_timeout=${4:-1}
  local bash_environment=${5:-}
  local missing_images=${6:-}
  local pull_mode=${7:-success}
  local pull_timeout=${8:-300}

  RUN_OUTPUT="${TEST_ROOT}/${name}.out"
  FAKE_CHILD_PID_FILE="${TEST_ROOT}/${name}-child.pid"
  FAKE_DESCENDANT_PID_FILE="${TEST_ROOT}/${name}-descendant.pid"
  FAKE_SUPERVISOR_PID_FILE="${TEST_ROOT}/${name}-supervisor.pid"
  FAKE_PULL_PID_FILE="${TEST_ROOT}/${name}-pull.pid"
  FAKE_DOCKER_LOG="${TEST_ROOT}/${name}-docker.log"
  FAKE_EVENT_LOG="${TEST_ROOT}/${name}-events.log"
  FAKE_SUPERVISOR_LOG="${TEST_ROOT}/${name}-supervisors.log"
  FAKE_IMAGE_STATE_FILE="${TEST_ROOT}/${name}-images.txt"
  SPAWN_SIGNAL_MARKER="${TEST_ROOT}/${name}-spawn-signal"
  (
    cd "${working_directory}"
    exec env \
      PATH="${FAKE_BIN}:${PATH}" \
      FAKE_RUN_MODE="${fake_mode}" \
      FAKE_RUN_CARGO_CWD="${TEST_ROOT}/${name}-cargo-cwd" \
      FAKE_RUN_CHILD_PID_FILE="${FAKE_CHILD_PID_FILE}" \
      FAKE_RUN_DESCENDANT_PID_FILE="${FAKE_DESCENDANT_PID_FILE}" \
      FAKE_RUN_SUPERVISOR_PID_FILE="${FAKE_SUPERVISOR_PID_FILE}" \
      FAKE_RUN_PULL_PID_FILE="${FAKE_PULL_PID_FILE}" \
      FAKE_RUN_DOCKER_LOG="${FAKE_DOCKER_LOG}" \
      FAKE_RUN_EVENT_LOG="${FAKE_EVENT_LOG}" \
      FAKE_RUN_SUPERVISOR_LOG="${FAKE_SUPERVISOR_LOG}" \
      FAKE_RUN_IMAGE_STATE_FILE="${FAKE_IMAGE_STATE_FILE}" \
      FAKE_RUN_MISSING_IMAGES="${missing_images}" \
      FAKE_RUN_PULL_MODE="${pull_mode}" \
      FAKE_RUN_REAL_PYTHON3="${REAL_PYTHON3}" \
      FAKE_RUN_SPAWN_SIGNAL_MARKER="${SPAWN_SIGNAL_MARKER}" \
      BASH_ENV="${bash_environment}" \
      MEMKAFKA_API_VERSION_ARTIFACT_DIR="${TEST_ROOT}/${name}-artifacts" \
      MEMKAFKA_API_VERSION_RECORDER_BUILD_TIMEOUT_SECONDS="${build_timeout}" \
      MEMKAFKA_API_VERSION_IMAGE_PULL_TIMEOUT_SECONDS="${pull_timeout}" \
      MEMKAFKA_API_VERSION_TERMINATION_GRACE_SECONDS=1 \
      "${RUN_SCRIPT}" --check
  ) >"${RUN_OUTPUT}" 2>&1 &
  RUNNER_PID=$!

  for _ in {1..50}; do
    if [[ -s "${FAKE_CHILD_PID_FILE}" ]]; then
      return
    fi
    if ! kill -0 "${RUNNER_PID}" >/dev/null 2>&1; then
      return
    fi
    sleep 0.05
  done
}

image_log_path() {
  local name=$1
  local image_name=$2

  find "${TEST_ROOT}/${name}-artifacts" \
    -type f -name "${image_name}-image-pull.log" -print -quit
}

test_cold_remote_images_are_pulled_before_build_and_startup() {
  local cargo_line
  local kafka_log
  local kafka_pull_line
  local kafbat_log

  start_runner cold-images fail "${REPOSITORY_ROOT}" 30 "" \
    "${KAFKA_IMAGE} ${KAFBAT_IMAGE}" success 7
  if ! wait_for_runner_with_deadline 8; then
    printf 'cold-image runner did not reach the recorder build\n' >&2
    return 1
  fi
  if ((RUN_EXIT != 79)); then
    cat "${RUN_OUTPUT}" >&2
    printf 'cold-image runner exited %d, expected fake build exit 79\n' "${RUN_EXIT}" >&2
    return 1
  fi
  if ! grep -Fx "pull ${KAFKA_IMAGE}" "${FAKE_DOCKER_LOG}" >/dev/null \
    || ! grep -Fx "pull ${KAFBAT_IMAGE}" "${FAKE_DOCKER_LOG}" >/dev/null; then
    cat "${FAKE_DOCKER_LOG}" >&2
    printf 'runner did not explicitly pull both missing pinned images\n' >&2
    return 1
  fi
  if grep -E '^pull (memkafka-api-version-proxy:test|memkafka-kafbat-seed:ci)$' \
      "${FAKE_DOCKER_LOG}" >/dev/null; then
    cat "${FAKE_DOCKER_LOG}" >&2
    printf 'runner tried to pull a locally owned task image\n' >&2
    return 1
  fi
  grep -F -- \
    "--timeout 7 --termination-grace 1 --label pull pinned Kafka image -- docker pull ${KAFKA_IMAGE}" \
    "${FAKE_SUPERVISOR_LOG}" >/dev/null
  kafka_pull_line="$(grep -nF "docker pull ${KAFKA_IMAGE}" "${FAKE_EVENT_LOG}" | cut -d: -f1)"
  cargo_line="$(grep -nF 'cargo build --locked --manifest-path tests/api-versions/proxy/Cargo.toml' \
    "${FAKE_EVENT_LOG}" | cut -d: -f1)"
  if ((kafka_pull_line >= cargo_line)); then
    cat "${FAKE_EVENT_LOG}" >&2
    printf 'Kafka pull did not complete before build/startup work\n' >&2
    return 1
  fi
  kafka_log="$(image_log_path cold-images kafka)"
  kafbat_log="$(image_log_path cold-images kafbat)"
  grep -Fx "simulated pulled image: ${KAFKA_IMAGE}" "${kafka_log}" >/dev/null
  grep -Fx "simulated pulled image: ${KAFBAT_IMAGE}" "${kafbat_log}" >/dev/null
}

test_warm_remote_images_are_inspected_without_pull() {
  local kafka_log
  local kafbat_log

  start_runner warm-images fail "${REPOSITORY_ROOT}" 30
  if ! wait_for_runner_with_deadline 8; then
    printf 'warm-image runner did not reach the recorder build\n' >&2
    return 1
  fi
  if ((RUN_EXIT != 79)); then
    cat "${RUN_OUTPUT}" >&2
    printf 'warm-image runner exited %d, expected fake build exit 79\n' "${RUN_EXIT}" >&2
    return 1
  fi
  if grep -E '^pull ' "${FAKE_DOCKER_LOG}" >/dev/null; then
    cat "${FAKE_DOCKER_LOG}" >&2
    printf 'warm-image runner performed an unnecessary pull\n' >&2
    return 1
  fi
  grep -Fx "image inspect ${KAFKA_IMAGE}" "${FAKE_DOCKER_LOG}" >/dev/null
  grep -Fx "image inspect ${KAFBAT_IMAGE}" "${FAKE_DOCKER_LOG}" >/dev/null
  kafka_log="$(image_log_path warm-images kafka)"
  kafbat_log="$(image_log_path warm-images kafbat)"
  grep -Fx "using cached pinned Kafka image: ${KAFKA_IMAGE}" "${kafka_log}" >/dev/null
  grep -Fx "using cached pinned Kafbat image: ${KAFBAT_IMAGE}" "${kafbat_log}" >/dev/null
}

test_image_pull_failure_stops_before_build_and_preserves_log() {
  local kafka_log

  start_runner pull-failure fail "${REPOSITORY_ROOT}" 30 "" \
    "${KAFKA_IMAGE}" fail 7
  if ! wait_for_runner_with_deadline 8; then
    printf 'failed image pull did not stop the runner\n' >&2
    return 1
  fi
  if ((RUN_EXIT != 93)); then
    cat "${RUN_OUTPUT}" >&2
    printf 'failed image pull exited %d, expected 93\n' "${RUN_EXIT}" >&2
    return 1
  fi
  if grep -F 'cargo build ' "${FAKE_EVENT_LOG}" >/dev/null; then
    cat "${FAKE_EVENT_LOG}" >&2
    printf 'runner continued to build after failed image pull\n' >&2
    return 1
  fi
  kafka_log="$(image_log_path pull-failure kafka)"
  grep -Fx "simulated pull failure: ${KAFKA_IMAGE}" "${kafka_log}" >/dev/null
}

test_image_pull_timeout_stops_and_reaps_the_pull() {
  local kafka_log

  start_runner pull-timeout fail "${REPOSITORY_ROOT}" 30 "" \
    "${KAFKA_IMAGE}" hang 1
  if ! wait_for_runner_with_deadline 8; then
    printf 'timed-out image pull did not stop the runner\n' >&2
    return 1
  fi
  if ((RUN_EXIT != 124)); then
    cat "${RUN_OUTPUT}" >&2
    printf 'timed-out image pull exited %d, expected 124\n' "${RUN_EXIT}" >&2
    return 1
  fi
  kafka_log="$(image_log_path pull-timeout kafka)"
  grep -F 'timed out after 1s: pull pinned Kafka image' "${kafka_log}" >/dev/null
  assert_pid_file_stopped "${FAKE_PULL_PID_FILE}" "timed-out image pull"
  if grep -F 'cargo build ' "${FAKE_EVENT_LOG}" >/dev/null; then
    cat "${FAKE_EVENT_LOG}" >&2
    printf 'runner continued to build after image pull timeout\n' >&2
    return 1
  fi
}

wait_for_runner_with_deadline() {
  local timeout_seconds=$1
  local watchdog_file="${TEST_ROOT}/watchdog-fired"
  local watchdog_pid

  rm -f "${watchdog_file}"
  (
    sleep "${timeout_seconds}"
    : >"${watchdog_file}"
    kill -KILL "${RUNNER_PID}" >/dev/null 2>&1 || true
  ) &
  watchdog_pid=$!
  set +e
  wait "${RUNNER_PID}"
  RUN_EXIT=$?
  set -e
  RUNNER_PID=""
  if kill -0 "${watchdog_pid}" >/dev/null 2>&1; then
    kill -TERM "${watchdog_pid}" >/dev/null 2>&1 || true
  fi
  wait "${watchdog_pid}" >/dev/null 2>&1 || true
  [[ ! -f "${watchdog_file}" ]]
}

assert_child_stopped() {
  local child_pid

  child_pid="$(<"${FAKE_CHILD_PID_FILE}")"
  if kill -0 "${child_pid}" >/dev/null 2>&1; then
    printf 'bounded command left child PID %s running\n' "${child_pid}" >&2
    return 1
  fi
}

assert_pid_file_stopped() {
  local pid_file=$1
  local label=$2
  local process_id

  if [[ ! -s "${pid_file}" ]]; then
    printf '%s never recorded its PID\n' "${label}" >&2
    return 1
  fi
  process_id="$(<"${pid_file}")"
  for _ in {1..40}; do
    if ! kill -0 "${process_id}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.05
  done
  printf '%s left PID %s running\n' "${label}" "${process_id}" >&2
  return 1
}

assert_cleanup_and_no_update() {
  local signal_name=$1
  local cleanup_count

  cleanup_count="$(grep -c '^network rm ' "${FAKE_DOCKER_LOG}" || true)"
  if [[ "${cleanup_count}" != 1 ]]; then
    cat "${FAKE_DOCKER_LOG}" >&2
    printf '%s performed cleanup %s times, expected once\n' \
      "${signal_name}" "${cleanup_count}" >&2
    return 1
  fi
  if [[ "$(<"${RUN_OUTPUT}")" == *'Updated Kafka API version evidence'* ]]; then
    printf 'runner continued into evidence update after %s\n' "${signal_name}" >&2
    return 1
  fi
}

test_build_timeout_is_bounded_and_reaps_child() {
  start_runner timeout hang "${REPOSITORY_ROOT}"
  if ! wait_for_runner_with_deadline 8; then
    cat "${RUN_OUTPUT}" >&2
    printf 'runner did not enforce the one-second recorder-build deadline\n' >&2
    return 1
  fi
  if ((RUN_EXIT != 124)); then
    cat "${RUN_OUTPUT}" >&2
    printf 'recorder-build timeout exited %d, expected 124\n' "${RUN_EXIT}" >&2
    return 1
  fi
  if [[ "$(<"${RUN_OUTPUT}")" != *"timed out after 1s: build standalone recorder"* ]]; then
    cat "${RUN_OUTPUT}" >&2
    printf 'timeout did not report an actionable recorder-build diagnostic\n' >&2
    return 1
  fi
  assert_child_stopped
}

test_signal_exits_and_cleans_once() {
  local signal_name=$1
  local expected_exit=$2
  local name="signal-${signal_name}"

  start_runner "${name}" hang "${REPOSITORY_ROOT}"
  if [[ ! -s "${FAKE_CHILD_PID_FILE}" ]]; then
    cat "${RUN_OUTPUT}" >&2
    printf 'runner never reached the supervised recorder build\n' >&2
    return 1
  fi
  kill "-${signal_name}" "${RUNNER_PID}"
  if ! wait_for_runner_with_deadline 8; then
    cat "${RUN_OUTPUT}" >&2
    printf 'runner continued after %s\n' "${signal_name}" >&2
    return 1
  fi
  if ((RUN_EXIT != expected_exit)); then
    cat "${RUN_OUTPUT}" >&2
    printf '%s exited %d, expected %d\n' "${signal_name}" "${RUN_EXIT}" "${expected_exit}" >&2
    return 1
  fi
  assert_cleanup_and_no_update "${signal_name}"
  assert_child_stopped
}

test_signal_during_supervisor_spawn_is_deferred_until_owned() {
  start_runner immediate-signal hang "${REPOSITORY_ROOT}" 30 "${SPAWN_SIGNAL_ENV}"
  if ! wait_for_runner_with_deadline 8; then
    cat "${RUN_OUTPUT}" >&2
    printf 'runner did not exit after TERM during supervisor spawn\n' >&2
    return 1
  fi
  if ((RUN_EXIT != 143)); then
    cat "${RUN_OUTPUT}" >&2
    printf 'spawn-time TERM exited %d, expected 143\n' "${RUN_EXIT}" >&2
    return 1
  fi
  if [[ ! -f "${SPAWN_SIGNAL_MARKER}" ]]; then
    printf 'spawn-time TERM fixture did not reach the ownership gap\n' >&2
    return 1
  fi
  for _ in {1..50}; do
    [[ -s "${FAKE_SUPERVISOR_PID_FILE}" ]] && break
    sleep 0.05
  done
  assert_pid_file_stopped "${FAKE_SUPERVISOR_PID_FILE}" "spawn-time supervisor"
  if [[ -s "${FAKE_CHILD_PID_FILE}" ]]; then
    assert_pid_file_stopped "${FAKE_CHILD_PID_FILE}" "spawn-time command"
  fi
  assert_cleanup_and_no_update TERM
}

test_signal_during_bounded_command_spawn_is_deferred_until_owned() {
  "${REAL_PYTHON3}" "${BOUNDED_COMMAND_BEHAVIOR}" "${BOUNDED_COMMAND}"
}

test_timeout_kills_owned_descendants() {
  start_runner descendant descendant "${REPOSITORY_ROOT}"
  for _ in {1..50}; do
    [[ -s "${FAKE_DESCENDANT_PID_FILE}" ]] && break
    sleep 0.05
  done
  if ! wait_for_runner_with_deadline 8; then
    cat "${RUN_OUTPUT}" >&2
    printf 'runner did not enforce the descendant timeout\n' >&2
    return 1
  fi
  if ((RUN_EXIT != 124)); then
    cat "${RUN_OUTPUT}" >&2
    printf 'descendant timeout exited %d, expected 124\n' "${RUN_EXIT}" >&2
    return 1
  fi
  assert_pid_file_stopped "${FAKE_CHILD_PID_FILE}" "timed-out command"
  assert_pid_file_stopped "${FAKE_DESCENDANT_PID_FILE}" "timed-out descendant"
  assert_cleanup_and_no_update timeout
}

test_invocation_outside_repository_uses_repository_root() {
  local outside_directory="${TEST_ROOT}/outside"
  local recorded_cwd

  mkdir -p "${outside_directory}"
  start_runner outside fail "${outside_directory}"
  if ! wait_for_runner_with_deadline 3; then
    printf 'outside-root fixture did not exit\n' >&2
    return 1
  fi
  recorded_cwd="$(<"${TEST_ROOT}/outside-cargo-cwd")"
  if [[ "${recorded_cwd}" != "${REPOSITORY_ROOT}" ]]; then
    printf 'cargo ran from %s, expected repository root %s\n' \
      "${recorded_cwd}" "${REPOSITORY_ROOT}" >&2
    return 1
  fi
}

case "${1:-all}" in
  timeout)
    test_build_timeout_is_bounded_and_reaps_child
    ;;
  term)
    test_signal_exits_and_cleans_once TERM 143
    ;;
  int)
    test_signal_exits_and_cleans_once INT 130
    ;;
  immediate)
    test_signal_during_bounded_command_spawn_is_deferred_until_owned
    test_signal_during_supervisor_spawn_is_deferred_until_owned
    ;;
  descendant)
    test_timeout_kills_owned_descendants
    ;;
  outside)
    test_invocation_outside_repository_uses_repository_root
    ;;
  images)
    test_cold_remote_images_are_pulled_before_build_and_startup
    test_warm_remote_images_are_inspected_without_pull
    test_image_pull_failure_stops_before_build_and_preserves_log
    test_image_pull_timeout_stops_and_reaps_the_pull
    ;;
  all)
    test_cold_remote_images_are_pulled_before_build_and_startup
    test_warm_remote_images_are_inspected_without_pull
    test_image_pull_failure_stops_before_build_and_preserves_log
    test_image_pull_timeout_stops_and_reaps_the_pull
    test_build_timeout_is_bounded_and_reaps_child
    test_signal_exits_and_cleans_once TERM 143
    test_signal_exits_and_cleans_once INT 130
    test_signal_during_bounded_command_spawn_is_deferred_until_owned
    test_signal_during_supervisor_spawn_is_deferred_until_owned
    test_timeout_kills_owned_descendants
    test_invocation_outside_repository_uses_repository_root
    ;;
  *)
    printf 'usage: %s [timeout|term|int|immediate|descendant|outside|images|all]\n' "$0" >&2
    exit 2
    ;;
esac

printf 'PASS   Kafka API version runner behavior\n'
