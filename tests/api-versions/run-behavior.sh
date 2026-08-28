#!/usr/bin/env bash
set -euo pipefail
set -m

readonly SCRIPT_DIRECTORY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPOSITORY_ROOT="$(cd "${SCRIPT_DIRECTORY}/../.." && pwd)"
readonly RUN_SCRIPT="${SCRIPT_DIRECTORY}/run.sh"
readonly FAKE_COMMAND="${SCRIPT_DIRECTORY}/fixtures/run-fake-command.sh"
readonly REAL_PYTHON3="$(command -v python3)"
readonly TEST_ROOT="$(mktemp -d)"
readonly FAKE_BIN="${TEST_ROOT}/bin"

RUNNER_PID=""
RUN_OUTPUT=""
FAKE_CHILD_PID_FILE=""
FAKE_DOCKER_LOG=""

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

  RUN_OUTPUT="${TEST_ROOT}/${name}.out"
  FAKE_CHILD_PID_FILE="${TEST_ROOT}/${name}-child.pid"
  FAKE_DOCKER_LOG="${TEST_ROOT}/${name}-docker.log"
  (
    cd "${working_directory}"
    exec env \
      PATH="${FAKE_BIN}:${PATH}" \
      FAKE_RUN_MODE="${fake_mode}" \
      FAKE_RUN_CARGO_CWD="${TEST_ROOT}/${name}-cargo-cwd" \
      FAKE_RUN_CHILD_PID_FILE="${FAKE_CHILD_PID_FILE}" \
      FAKE_RUN_DOCKER_LOG="${FAKE_DOCKER_LOG}" \
      FAKE_RUN_REAL_PYTHON3="${REAL_PYTHON3}" \
      MEMKAFKA_API_VERSION_ARTIFACT_DIR="${TEST_ROOT}/${name}-artifacts" \
      MEMKAFKA_API_VERSION_RECORDER_BUILD_TIMEOUT_SECONDS=1 \
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
  local cleanup_count

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
  assert_child_stopped
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
  outside)
    test_invocation_outside_repository_uses_repository_root
    ;;
  all)
    test_build_timeout_is_bounded_and_reaps_child
    test_signal_exits_and_cleans_once TERM 143
    test_signal_exits_and_cleans_once INT 130
    test_invocation_outside_repository_uses_repository_root
    ;;
  *)
    printf 'usage: %s [timeout|term|int|outside|all]\n' "$0" >&2
    exit 2
    ;;
esac

printf 'PASS   Kafka API version runner behavior\n'
