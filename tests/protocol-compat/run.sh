#!/usr/bin/env bash
set -euo pipefail
exec 3>&2

readonly SCRIPT_DIRECTORY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPOSITORY_ROOT="$(cd "${SCRIPT_DIRECTORY}/../.." && pwd)"
readonly BOUNDED_COMMAND="${REPOSITORY_ROOT}/tests/api-versions/bounded-command.py"
readonly MEMKAFKA_IMAGE="${MEMKAFKA_PROTOCOL_IMAGE:-memkafka:ci}"
readonly MAVEN_IMAGE="maven:3.9.11-eclipse-temurin-25"
readonly KAFKA_IMAGE="apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837"
readonly ARTIFACT_ROOT="${MEMKAFKA_PROTOCOL_ARTIFACT_DIR:-${REPOSITORY_ROOT}/artifacts/protocol-compat}"
readonly SUFFIX="$$"
readonly MEMKAFKA_CONTAINER="memkafka-protocol-memkafka-${SUFFIX}"
readonly KAFKA_CONTAINER="memkafka-protocol-kafka-${SUFFIX}"
readonly MAVEN_CONTAINER="memkafka-protocol-maven-${SUFFIX}"
readonly NETWORK="memkafka-protocol-${SUFFIX}"
readonly IMAGE_PULL_TIMEOUT_SECONDS="${MEMKAFKA_PROTOCOL_IMAGE_PULL_TIMEOUT_SECONDS:-300}"
readonly INFRASTRUCTURE_TIMEOUT_SECONDS="${MEMKAFKA_PROTOCOL_INFRASTRUCTURE_TIMEOUT_SECONDS:-60}"
readonly READINESS_TIMEOUT_SECONDS="${MEMKAFKA_PROTOCOL_READINESS_TIMEOUT_SECONDS:-90}"
readonly READINESS_PROBE_TIMEOUT_SECONDS="${MEMKAFKA_PROTOCOL_READINESS_PROBE_TIMEOUT_SECONDS:-10}"
readonly MAVEN_TIMEOUT_SECONDS="${MEMKAFKA_PROTOCOL_MAVEN_TIMEOUT_SECONDS:-600}"
readonly PROBE_TIMEOUT_SECONDS="${MEMKAFKA_PROTOCOL_PROBE_TIMEOUT_SECONDS:-300}"
readonly DIFF_TIMEOUT_SECONDS="${MEMKAFKA_PROTOCOL_DIFF_TIMEOUT_SECONDS:-30}"
readonly CLEANUP_TIMEOUT_SECONDS="${MEMKAFKA_PROTOCOL_CLEANUP_TIMEOUT_SECONDS:-20}"
readonly TERMINATION_GRACE_SECONDS="${MEMKAFKA_PROTOCOL_TERMINATION_GRACE_SECONDS:-5}"

TEMP_DIRECTORY=""
RESULT_DIRECTORY=""
RUN_ARTIFACT_DIRECTORY=""
MAVEN_LOG=""
LAST_COMMAND="preflight"
ACTIVE_COMMAND_PID=""
ACTIVE_COMMAND_SIGNAL=TERM
DEFERRED_SIGNAL=""
NETWORK_CREATED=false

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'required command is unavailable: %s\n' "$1" >&2
    exit 1
  fi
}

require_positive_integer() {
  local name=$1
  local value=$2
  if [[ ! "${value}" =~ ^[1-9][0-9]*$ ]]; then
    printf '%s must be a positive integer, got: %s\n' "${name}" "${value}" >&2
    exit 2
  fi
}

stop_supervised_command() {
  local process_id=$1
  local initial_signal=$2
  local running_jobs_file="${TEMP_DIRECTORY}/running-jobs-${process_id}"
  local running_pid=""

  jobs -pr >"${running_jobs_file}"
  while IFS= read -r running_pid; do
    if [[ "${running_pid}" == "${process_id}" ]]; then
      kill "-${initial_signal}" "${process_id}" >/dev/null 2>&1 || true
      break
    fi
  done <"${running_jobs_file}"
  wait "${process_id}" >/dev/null 2>&1 || true
}

defer_interrupt() {
  [[ -n "${DEFERRED_SIGNAL}" ]] || DEFERRED_SIGNAL=INT
}

defer_termination() {
  [[ -n "${DEFERRED_SIGNAL}" ]] || DEFERRED_SIGNAL=TERM
}

begin_supervisor_spawn() {
  DEFERRED_SIGNAL=""
  trap defer_interrupt INT
  trap defer_termination TERM
}

finish_supervisor_spawn() {
  local pending_signal="${DEFERRED_SIGNAL}"
  DEFERRED_SIGNAL=""
  trap handle_interrupt INT
  trap handle_termination TERM
  case "${pending_signal}" in
    INT) handle_interrupt ;;
    TERM) handle_termination ;;
  esac
}

run_bounded() {
  local timeout_seconds=$1
  local label=$2
  local exit_code=0
  shift 2

  LAST_COMMAND="${label}"
  begin_supervisor_spawn
  python3 "${BOUNDED_COMMAND}" \
    --timeout "${timeout_seconds}" \
    --termination-grace "${TERMINATION_GRACE_SECONDS}" \
    --label "${label}" \
    "$@" &
  ACTIVE_COMMAND_PID=$!
  finish_supervisor_spawn
  if wait "${ACTIVE_COMMAND_PID}"; then
    exit_code=0
  else
    exit_code=$?
  fi
  ACTIVE_COMMAND_PID=""
  return "${exit_code}"
}

run_cleanup_command() {
  local label=$1
  shift
  python3 "${BOUNDED_COMMAND}" \
    --timeout "${CLEANUP_TIMEOUT_SECONDS}" \
    --termination-grace "${TERMINATION_GRACE_SECONDS}" \
    --label "${label}" \
    -- "$@"
}

prepare_remote_image() {
  local label=$1
  local image=$2
  local log=$3
  local inspect_exit=0

  : >"${log}"
  if run_bounded "${INFRASTRUCTURE_TIMEOUT_SECONDS}" \
    "inspect cached pinned ${label} image" -- \
    docker image inspect "${image}" >>"${log}" 2>&1; then
    return
  else
    inspect_exit=$?
  fi
  if ((inspect_exit == 124 || inspect_exit == 130 || inspect_exit == 143)); then
    return "${inspect_exit}"
  fi
  run_bounded "${IMAGE_PULL_TIMEOUT_SECONDS}" "pull pinned ${label} image" -- \
    docker pull "${image}" >>"${log}" 2>&1
}

container_is_running() {
  local container=$1
  local state_file="${TEMP_DIRECTORY}/${container}.state"
  run_bounded "${READINESS_PROBE_TIMEOUT_SECONDS}" "inspect ${container} state" -- \
    docker inspect --format '{{.State.Running}}' "${container}" >"${state_file}" 2>&1 \
    && grep -Fx true "${state_file}" >/dev/null
}

wait_for_memkafka() {
  local deadline=$((SECONDS + READINESS_TIMEOUT_SECONDS))
  local probe_timeout=0
  local readiness_log="${TEMP_DIRECTORY}/memkafka-readiness.log"

  : >"${readiness_log}"
  while ((SECONDS < deadline)); do
    container_is_running "${MEMKAFKA_CONTAINER}"
    probe_timeout=$((deadline - SECONDS))
    ((probe_timeout > READINESS_PROBE_TIMEOUT_SECONDS)) \
      && probe_timeout=${READINESS_PROBE_TIMEOUT_SECONDS}
    if run_bounded "${probe_timeout}" "probe MemKafka readiness" -- \
      docker logs "${MEMKAFKA_CONTAINER}" >"${readiness_log}" 2>&1 \
      && grep -F 'MemKafka ready kafka=' \
        "${readiness_log}" >/dev/null; then
      return
    fi
    sleep 1
  done
  printf 'MemKafka did not become ready within %ss\n' "${READINESS_TIMEOUT_SECONDS}" >&2
  return 1
}

wait_for_kafka() {
  local deadline=$((SECONDS + READINESS_TIMEOUT_SECONDS))
  local probe_timeout=0
  local readiness_log="${TEMP_DIRECTORY}/kafka-readiness.log"

  : >"${readiness_log}"
  while ((SECONDS < deadline)); do
    container_is_running "${KAFKA_CONTAINER}"
    probe_timeout=$((deadline - SECONDS))
    ((probe_timeout > READINESS_PROBE_TIMEOUT_SECONDS)) \
      && probe_timeout=${READINESS_PROBE_TIMEOUT_SECONDS}
    if run_bounded "${probe_timeout}" "probe Kafka readiness" -- \
      docker exec "${KAFKA_CONTAINER}" \
      /opt/kafka/bin/kafka-broker-api-versions.sh \
      --bootstrap-server localhost:19092 >>"${readiness_log}" 2>&1; then
      return
    fi
    sleep 1
  done
  printf 'Kafka 4.3.1 did not become ready within %ss\n' "${READINESS_TIMEOUT_SECONDS}" >&2
  return 1
}

run_maven() {
  local timeout_seconds=$1
  local label=$2
  shift 2
  run_bounded "${timeout_seconds}" "${label}" -- \
    docker run --rm \
    --name "${MAVEN_CONTAINER}" \
    --network "${NETWORK}" \
    --volume "${REPOSITORY_ROOT}:/workspace:ro" \
    --volume "${TEMP_DIRECTORY}/maven-repository:/maven-repository" \
    --volume "${TEMP_DIRECTORY}/target:/build/target" \
    --volume "${RESULT_DIRECTORY}:/results" \
    --env MAVEN_CONFIG=/tmp/maven-config \
    "${MAVEN_IMAGE}" \
    mvn --batch-mode --no-transfer-progress \
    --file /workspace/tests/protocol-compat/pom.xml \
    -Dmaven.repo.local=/maven-repository \
    -Dprotocol.compat.build.directory=/build/target \
    "$@" >>"${MAVEN_LOG}" 2>&1
}

run_probe() {
  local label=$1
  local arguments=$2
  run_maven "${PROBE_TIMEOUT_SECONDS}" "${label}" \
    -DskipTests exec:java \
    -Dexec.mainClass=io.memkafka.protocol.ProtocolCompatibilityProbe \
    "-Dexec.args=${arguments}"
}

capture_diagnostics() {
  [[ -n "${RUN_ARTIFACT_DIRECTORY}" ]] || return 0
  mkdir -p "${RUN_ARTIFACT_DIRECTORY}"
  if [[ -n "${TEMP_DIRECTORY}" ]]; then
    run_cleanup_command "capture MemKafka diagnostics" \
      docker logs "${MEMKAFKA_CONTAINER}" >"${RUN_ARTIFACT_DIRECTORY}/memkafka.log" 2>&1 || true
    run_cleanup_command "capture Kafka diagnostics" \
      docker logs "${KAFKA_CONTAINER}" >"${RUN_ARTIFACT_DIRECTORY}/kafka.log" 2>&1 || true
  fi
  if [[ -n "${MAVEN_LOG}" && -f "${MAVEN_LOG}" ]]; then
    cp "${MAVEN_LOG}" "${RUN_ARTIFACT_DIRECTORY}/maven.log"
  else
    : >"${RUN_ARTIFACT_DIRECTORY}/maven.log"
  fi
  if [[ -n "${RESULT_DIRECTORY}" && -d "${RESULT_DIRECTORY}" ]]; then
    find "${RESULT_DIRECTORY}" -maxdepth 1 -type f -exec cp {} "${RUN_ARTIFACT_DIRECTORY}/" \;
  fi
  printf 'Kafka clients: 4.3.1\nJava: 25\nMaven image: %s\nKafka image: %s\nlast command: %s\n' \
    "${MAVEN_IMAGE}" "${KAFKA_IMAGE}" "${LAST_COMMAND}" \
    >"${RUN_ARTIFACT_DIRECTORY}/summary.txt"
}

cleanup() {
  local exit_code=$?

  if [[ -n "${ACTIVE_COMMAND_PID}" ]]; then
    stop_supervised_command "${ACTIVE_COMMAND_PID}" "${ACTIVE_COMMAND_SIGNAL}"
    ACTIVE_COMMAND_PID=""
  fi
  capture_diagnostics
  if command -v docker >/dev/null 2>&1 && command -v python3 >/dev/null 2>&1; then
    run_cleanup_command "remove exact protocol compatibility containers" \
      docker rm --force \
      "${MAVEN_CONTAINER}" "${MEMKAFKA_CONTAINER}" "${KAFKA_CONTAINER}" \
      >/dev/null 2>&1 || true
    if [[ "${NETWORK_CREATED}" == true ]]; then
      run_cleanup_command "remove exact protocol compatibility network" \
        docker network rm "${NETWORK}" >/dev/null 2>&1 || true
    fi
  fi
  if ((exit_code != 0)); then
    if [[ -n "${MAVEN_LOG}" && -f "${MAVEN_LOG}" ]]; then
      grep -F 'timed out after ' "${MAVEN_LOG}" >&3 || true
    fi
    printf 'protocol compatibility runner failed; last command: %s\n' "${LAST_COMMAND}" >&3
    if [[ -n "${RUN_ARTIFACT_DIRECTORY}" ]]; then
      printf 'diagnostics: %s\n' "${RUN_ARTIFACT_DIRECTORY}" >&3
    fi
  fi
  if [[ -n "${TEMP_DIRECTORY}" \
      && "${TEMP_DIRECTORY}" == "${TMPDIR:-/tmp}/memkafka-protocol-compat."* \
      && -d "${TEMP_DIRECTORY}" ]]; then
    run_cleanup_command "remove protocol compatibility temp directory" \
      rm -rf "${TEMP_DIRECTORY}" >&3 2>&3 || true
  fi
  return "${exit_code}"
}

handle_interrupt() {
  printf 'received INT; cancelling protocol compatibility runner\n' >&3
  ACTIVE_COMMAND_SIGNAL=INT
  exit 130
}

handle_termination() {
  printf 'received TERM; cancelling protocol compatibility runner\n' >&3
  ACTIVE_COMMAND_SIGNAL=TERM
  exit 143
}

trap cleanup EXIT
trap handle_interrupt INT
trap handle_termination TERM

require_command docker
require_command python3
require_command diff
for timeout_setting in \
  IMAGE_PULL_TIMEOUT_SECONDS INFRASTRUCTURE_TIMEOUT_SECONDS READINESS_TIMEOUT_SECONDS \
  READINESS_PROBE_TIMEOUT_SECONDS MAVEN_TIMEOUT_SECONDS PROBE_TIMEOUT_SECONDS \
  DIFF_TIMEOUT_SECONDS CLEANUP_TIMEOUT_SECONDS TERMINATION_GRACE_SECONDS; do
  require_positive_integer "${timeout_setting}" "${!timeout_setting}"
done

mkdir -p "${ARTIFACT_ROOT}"
RUN_ARTIFACT_DIRECTORY="${ARTIFACT_ROOT}/run-${SUFFIX}"
mkdir -p "${RUN_ARTIFACT_DIRECTORY}"
TEMP_DIRECTORY="$(mktemp -d "${TMPDIR:-/tmp}/memkafka-protocol-compat.XXXXXX")"
RESULT_DIRECTORY="${TEMP_DIRECTORY}/results"
MAVEN_LOG="${TEMP_DIRECTORY}/maven.log"
mkdir -p \
  "${RESULT_DIRECTORY}" "${TEMP_DIRECTORY}/maven-repository" "${TEMP_DIRECTORY}/target"
: >"${MAVEN_LOG}"

if ! run_bounded "${INFRASTRUCTURE_TIMEOUT_SECONDS}" "inspect local MemKafka image" -- \
  docker image inspect "${MEMKAFKA_IMAGE}" >/dev/null 2>&1; then
  printf 'required local MemKafka image is unavailable: %s\n' "${MEMKAFKA_IMAGE}" >&2
  exit 1
fi
prepare_remote_image Kafka "${KAFKA_IMAGE}" "${TEMP_DIRECTORY}/kafka-image.log"
prepare_remote_image Maven "${MAVEN_IMAGE}" "${TEMP_DIRECTORY}/maven-image.log"

run_bounded "${INFRASTRUCTURE_TIMEOUT_SECONDS}" "create protocol compatibility network" -- \
  docker network create "${NETWORK}" >/dev/null
NETWORK_CREATED=true

run_bounded "${INFRASTRUCTURE_TIMEOUT_SECONDS}" "start MemKafka container" -- \
  docker run --detach \
  --name "${MEMKAFKA_CONTAINER}" \
  --network "${NETWORK}" \
  --network-alias memkafka \
  "${MEMKAFKA_IMAGE}" >/dev/null

run_bounded "${INFRASTRUCTURE_TIMEOUT_SECONDS}" "start Kafka 4.3.1 container" -- \
  docker run --detach \
  --name "${KAFKA_CONTAINER}" \
  --network "${NETWORK}" \
  --network-alias kafka \
  --env KAFKA_NODE_ID=1 \
  --env KAFKA_PROCESS_ROLES=broker,controller \
  --env KAFKA_LISTENERS=PLAINTEXT://:19092,CONTROLLER://:19093 \
  --env KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:19092 \
  --env KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  --env KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  --env KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:19093 \
  --env KAFKA_NUM_PARTITIONS=2 \
  --env KAFKA_AUTO_CREATE_TOPICS_ENABLE=true \
  --env KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  --env KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS=1000 \
  --env KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  --env KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  --env KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  "${KAFKA_IMAGE}" >/dev/null

wait_for_memkafka
wait_for_kafka
run_maven "${MAVEN_TIMEOUT_SECONDS}" "compile and test Java protocol probe" test
run_probe "typed-errors against MemKafka" \
  'typed-errors --bootstrap-server memkafka:9092 --output /results/typed-errors.json'
run_probe "supported semantics against MemKafka" \
  'supported-semantics --bootstrap-server memkafka:9092 --output /results/supported-semantics-memkafka.json'
run_probe "supported semantics against Kafka" \
  'supported-semantics --bootstrap-server kafka:19092 --output /results/supported-semantics-kafka.json'
run_bounded "${DIFF_TIMEOUT_SECONDS}" "diff supported semantic outputs" -- \
  diff -u \
  "${RESULT_DIRECTORY}/supported-semantics-kafka.json" \
  "${RESULT_DIRECTORY}/supported-semantics-memkafka.json"

for version in 5 32767; do
  run_probe "ApiVersions v${version} against MemKafka" \
    "api-versions --bootstrap-server memkafka:9092 --version ${version} --output /results/api-versions-v${version}-memkafka.json"
  run_probe "ApiVersions v${version} against Kafka" \
    "api-versions --bootstrap-server kafka:19092 --version ${version} --output /results/api-versions-v${version}-kafka.json"
  run_bounded "${DIFF_TIMEOUT_SECONDS}" "diff ApiVersions v${version} normalized outputs" -- \
    diff -u \
    "${RESULT_DIRECTORY}/api-versions-v${version}-kafka.json" \
    "${RESULT_DIRECTORY}/api-versions-v${version}-memkafka.json"
done

LAST_COMMAND="completed all protocol compatibility comparisons"
printf 'PASS   27 Kafka 4.3.1 typed-error cases across 18 APIs\n'
printf 'PASS   8 supported-response semantic cases match Kafka 4.3.1\n'
printf 'PASS   unsupported ApiVersions v5 and v32767 match Kafka 4.3.1\n'
printf 'Protocol compatibility artifacts: %s\n' "${RUN_ARTIFACT_DIRECTORY}"
