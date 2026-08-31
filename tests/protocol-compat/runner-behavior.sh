#!/usr/bin/env bash
set -euo pipefail

readonly SCRIPT_DIRECTORY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPOSITORY_ROOT="$(cd "${SCRIPT_DIRECTORY}/../.." && pwd)"
readonly RUNNER="${SCRIPT_DIRECTORY}/run.sh"
readonly FAKE_DOCKER="${SCRIPT_DIRECTORY}/fixtures/fake-docker.sh"
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
ln -s "${REAL_PYTHON3}" "${FAKE_BIN}/python3"
ln -s "${REAL_DIFF}" "${FAKE_BIN}/diff"

run_case() {
  local name=$1
  local typed_mode=$2
  local missing_local=$3
  local probe_timeout=${4:-5}
  local artifacts="${TEST_ROOT}/${name}-artifacts"
  local output="${TEST_ROOT}/${name}.out"
  local docker_log="${TEST_ROOT}/${name}-docker.log"
  local pid_file="${TEST_ROOT}/${name}-hang.pid"
  mkdir -p "${artifacts}"
  : >"${docker_log}"

  set +e
  env \
    PATH="${FAKE_BIN}:/usr/bin:/bin" \
    MEMKAFKA_PROTOCOL_IMAGE=owned-memkafka:test \
    MEMKAFKA_PROTOCOL_ARTIFACT_DIR="${artifacts}" \
    MEMKAFKA_PROTOCOL_IMAGE_PULL_TIMEOUT_SECONDS=5 \
    MEMKAFKA_PROTOCOL_INFRASTRUCTURE_TIMEOUT_SECONDS=5 \
    MEMKAFKA_PROTOCOL_READINESS_TIMEOUT_SECONDS=5 \
    MEMKAFKA_PROTOCOL_READINESS_PROBE_TIMEOUT_SECONDS=2 \
    MEMKAFKA_PROTOCOL_MAVEN_TIMEOUT_SECONDS=5 \
    MEMKAFKA_PROTOCOL_PROBE_TIMEOUT_SECONDS="${probe_timeout}" \
    MEMKAFKA_PROTOCOL_CLEANUP_TIMEOUT_SECONDS=2 \
    MEMKAFKA_PROTOCOL_TERMINATION_GRACE_SECONDS=1 \
    FAKE_PROTOCOL_DOCKER_LOG="${docker_log}" \
    FAKE_PROTOCOL_TYPED_MODE="${typed_mode}" \
    FAKE_PROTOCOL_MISSING_LOCAL="${missing_local}" \
    FAKE_PROTOCOL_HANG_PID_FILE="${pid_file}" \
    "${RUNNER}" >"${output}" 2>&1
  RUN_EXIT=$?
  set -e
  RUN_ARTIFACTS="${artifacts}"
  RUN_OUTPUT="${output}"
  RUN_DOCKER_LOG="${docker_log}"
  RUN_PID_FILE="${pid_file}"
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
  local network

  run_case failure fail false
  if ((RUN_EXIT != 88)); then
    cat "${RUN_OUTPUT}" >&2
    printf 'typed probe failure exited %d, expected 88\n' "${RUN_EXIT}" >&2
    return 1
  fi
  start_names=$(sed -n 's/.*--name \([^ ]*\).*/\1/p' "${RUN_DOCKER_LOG}" | sort -u)
  cleanup_names=$(sed -n 's/^rm --force //p' "${RUN_DOCKER_LOG}" | tr ' ' '\n' | sort -u)
  while IFS= read -r name; do
    [[ -z "${name}" ]] && continue
    if ! grep -Fx "${name}" <<<"${cleanup_names}" >/dev/null; then
      printf 'started container was not cleaned exactly: %s\n' "${name}" >&2
      return 1
    fi
  done <<<"${start_names}"
  network=$(sed -n 's/^network create //p' "${RUN_DOCKER_LOG}")
  grep -Fx "network rm ${network}" "${RUN_DOCKER_LOG}" >/dev/null
  find "${RUN_ARTIFACTS}" -type f -name memkafka.log -print -quit | grep -q .
  find "${RUN_ARTIFACTS}" -type f -name kafka.log -print -quit | grep -q .
  find "${RUN_ARTIFACTS}" -type f -name maven.log -print -quit | grep -q .
  find "${RUN_ARTIFACTS}" -type f -name summary.txt -print -quit | grep -q .
  grep -F 'last command: typed-errors against MemKafka' "${RUN_OUTPUT}" >/dev/null
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

printf 'PASS   protocol compatibility runner behavior\n'
