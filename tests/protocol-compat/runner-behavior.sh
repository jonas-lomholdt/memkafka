#!/usr/bin/env bash
set -euo pipefail

readonly SCRIPT_DIRECTORY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPOSITORY_ROOT="$(cd "${SCRIPT_DIRECTORY}/../.." && pwd)"
readonly RUNNER="${SCRIPT_DIRECTORY}/run.sh"
readonly FAKE_DOCKER="${SCRIPT_DIRECTORY}/fixtures/fake-docker.sh"
readonly FAKE_RM="${SCRIPT_DIRECTORY}/fixtures/fake-rm.sh"
readonly BOUNDED_COMMAND="${REPOSITORY_ROOT}/tests/api-versions/bounded-command.py"
readonly TEST_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/memkafka-protocol-runner-test.XXXXXX")"
readonly FAKE_BIN="${TEST_ROOT}/bin"
readonly REAL_PYTHON3="$(command -v python3)"
readonly REAL_DIFF="$(command -v diff)"

cleanup() {
  local exit_code=$?
  if [[ -d "${TEST_ROOT}" && "${TEST_ROOT}" == */memkafka-protocol-runner-test.* ]]; then
    rm -rf "${TEST_ROOT}"
  fi
  return "${exit_code}"
}
trap cleanup EXIT

mkdir -p "${FAKE_BIN}"
ln -s "${FAKE_DOCKER}" "${FAKE_BIN}/docker"
ln -s "${FAKE_RM}" "${FAKE_BIN}/rm"
ln -s "${REAL_PYTHON3}" "${FAKE_BIN}/python3"
ln -s "${REAL_DIFF}" "${FAKE_BIN}/diff"

run_case() {
  local name=$1
  local typed_mode=$2
  local missing_local=$3
  local probe_timeout=${4:-5}
  local cleanup_mode=${5:-pass}
  local artifacts="${TEST_ROOT}/${name}-artifacts"
  local output="${TEST_ROOT}/${name}.out"
  local docker_log="${TEST_ROOT}/${name}-docker.log"
  local pid_file="${TEST_ROOT}/${name}-hang.pid"
  local cleanup_pid_file="${TEST_ROOT}/${name}-cleanup-hang.pid"
  local cleanup_target_file="${TEST_ROOT}/${name}-cleanup-target"
  mkdir -p "${artifacts}"
  : >"${docker_log}"

  set +e
  "${REAL_PYTHON3}" "${BOUNDED_COMMAND}" \
    --timeout 12 \
    --termination-grace 1 \
    --label "runner behavior case ${name}" \
    -- env \
    PATH="${FAKE_BIN}:/usr/bin:/bin" \
    MEMKAFKA_PROTOCOL_IMAGE=owned-memkafka:test \
    MEMKAFKA_PROTOCOL_ARTIFACT_DIR="${artifacts}" \
    MEMKAFKA_PROTOCOL_IMAGE_PULL_TIMEOUT_SECONDS=5 \
    MEMKAFKA_PROTOCOL_INFRASTRUCTURE_TIMEOUT_SECONDS=5 \
    MEMKAFKA_PROTOCOL_READINESS_TIMEOUT_SECONDS=5 \
    MEMKAFKA_PROTOCOL_READINESS_PROBE_TIMEOUT_SECONDS=2 \
    MEMKAFKA_PROTOCOL_MAVEN_TIMEOUT_SECONDS=5 \
    MEMKAFKA_PROTOCOL_PROBE_TIMEOUT_SECONDS="${probe_timeout}" \
    MEMKAFKA_PROTOCOL_CLEANUP_TIMEOUT_SECONDS=1 \
    MEMKAFKA_PROTOCOL_TERMINATION_GRACE_SECONDS=1 \
    FAKE_PROTOCOL_DOCKER_LOG="${docker_log}" \
    FAKE_PROTOCOL_TYPED_MODE="${typed_mode}" \
    FAKE_PROTOCOL_MISSING_LOCAL="${missing_local}" \
    FAKE_PROTOCOL_HANG_PID_FILE="${pid_file}" \
    FAKE_PROTOCOL_RM_MODE="${cleanup_mode}" \
    FAKE_PROTOCOL_CLEANUP_PID_FILE="${cleanup_pid_file}" \
    FAKE_PROTOCOL_CLEANUP_TARGET_FILE="${cleanup_target_file}" \
    "${RUNNER}" >"${output}" 2>&1
  RUN_EXIT=$?
  set -e
  RUN_ARTIFACTS="${artifacts}"
  RUN_OUTPUT="${output}"
  RUN_DOCKER_LOG="${docker_log}"
  RUN_PID_FILE="${pid_file}"
  RUN_CLEANUP_PID_FILE="${cleanup_pid_file}"
  RUN_CLEANUP_TARGET_FILE="${cleanup_target_file}"
}

test_missing_local_image_is_never_pulled() {
  run_case missing-local success true
  if ((RUN_EXIT == 0)); then
    printf 'runner accepted a missing locally owned MemKafka image\n' >&2
    return 1
  fi
  if grep -F 'pull owned-memkafka:test' "${RUN_DOCKER_LOG}" >/dev/null; then
    cat "${RUN_DOCKER_LOG}" >&2
    printf 'runner tried to pull the locally owned MemKafka image\n' >&2
    return 1
  fi
  if grep -F 'network create ' "${RUN_DOCKER_LOG}" >/dev/null; then
    cat "${RUN_DOCKER_LOG}" >&2
    printf 'runner created infrastructure after local image inspection failed\n' >&2
    return 1
  fi
}

test_failure_captures_diagnostics_and_cleans_exact_owned_targets() {
  local start_names
  local cleanup_names
  local started_networks
  local cleanup_networks

  run_case failure fail false
  if ((RUN_EXIT != 88)); then
    cat "${RUN_OUTPUT}" >&2
    printf 'typed probe failure exited %d, expected 88\n' "${RUN_EXIT}" >&2
    return 1
  fi
  start_names=$(sed -n 's/.*--name \([^ ]*\).*/\1/p' "${RUN_DOCKER_LOG}" | sort -u)
  cleanup_names=$(sed -n 's/^rm --force //p' "${RUN_DOCKER_LOG}" | tr ' ' '\n' | sort -u)
  if [[ -z "${start_names}" || -z "${cleanup_names}" ]]; then
    printf 'owned container cleanup sets must both be non-empty\n' >&2
    return 1
  fi
  if [[ "${start_names}" != "${cleanup_names}" ]]; then
    printf 'owned container cleanup set mismatch\nstarted/probe:\n%s\ncleanup:\n%s\n' \
      "${start_names}" "${cleanup_names}" >&2
    return 1
  fi
  started_networks=$(sed -n 's/^network create //p' "${RUN_DOCKER_LOG}" | sort -u)
  cleanup_networks=$(sed -n 's/^network rm //p' "${RUN_DOCKER_LOG}" | sort -u)
  if [[ -z "${started_networks}" || -z "${cleanup_networks}" ]]; then
    printf 'owned network cleanup sets must both be non-empty\n' >&2
    return 1
  fi
  if [[ "${started_networks}" != "${cleanup_networks}" ]]; then
    printf 'owned network cleanup set mismatch\ncreated:\n%s\ncleanup:\n%s\n' \
      "${started_networks}" "${cleanup_networks}" >&2
    return 1
  fi
  find "${RUN_ARTIFACTS}" -type f -name memkafka.log -print -quit | grep -q .
  find "${RUN_ARTIFACTS}" -type f -name kafka.log -print -quit | grep -q .
  find "${RUN_ARTIFACTS}" -type f -name maven.log -print -quit | grep -q .
  find "${RUN_ARTIFACTS}" -type f -name summary.txt -print -quit | grep -q .
  grep -F 'last command: typed-errors against MemKafka' "${RUN_OUTPUT}" >/dev/null
}

test_temp_cleanup_timeout_preserves_failure_and_reaps_cleanup() {
  run_case cleanup-timeout fail false 5 hang-temp
  if ((RUN_EXIT != 88)); then
    cat "${RUN_OUTPUT}" >&2
    printf 'cleanup timeout changed probe exit %d, expected 88\n' "${RUN_EXIT}" >&2
    return 1
  fi
  grep -F 'timed out after 1s: remove protocol compatibility temp directory' \
    "${RUN_OUTPUT}" >/dev/null
  if [[ ! -s "${RUN_CLEANUP_PID_FILE}" ]]; then
    printf 'hanging fake temp cleanup never recorded its PID\n' >&2
    return 1
  fi
  if kill -0 "$(<"${RUN_CLEANUP_PID_FILE}")" >/dev/null 2>&1; then
    printf 'bounded runner left the hung temp cleanup alive\n' >&2
    return 1
  fi
  if [[ ! -s "${RUN_CLEANUP_TARGET_FILE}" ]]; then
    printf 'hanging fake temp cleanup never recorded its exact target\n' >&2
    return 1
  fi
  if [[ -e "$(<"${RUN_CLEANUP_TARGET_FILE}")" ]]; then
    printf 'terminated fake temp cleanup left its target behind\n' >&2
    return 1
  fi
}

test_probe_timeout_is_bounded_and_reaps_the_command() {
  run_case timeout hang false 1
  if ((RUN_EXIT != 124)); then
    cat "${RUN_OUTPUT}" >&2
    printf 'hung probe exited %d, expected timeout 124\n' "${RUN_EXIT}" >&2
    return 1
  fi
  grep -F 'timed out after 1s: typed-errors against MemKafka' "${RUN_OUTPUT}" >/dev/null
  if [[ ! -s "${RUN_PID_FILE}" ]]; then
    printf 'hung fake probe never recorded its PID\n' >&2
    return 1
  fi
  if kill -0 "$(<"${RUN_PID_FILE}")" >/dev/null 2>&1; then
    printf 'bounded runner left the hung probe alive\n' >&2
    return 1
  fi
}

test_missing_local_image_is_never_pulled
test_failure_captures_diagnostics_and_cleans_exact_owned_targets
test_probe_timeout_is_bounded_and_reaps_the_command
test_temp_cleanup_timeout_preserves_failure_and_reaps_cleanup

printf 'PASS   protocol compatibility runner behavior\n'
