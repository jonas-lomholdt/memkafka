#!/usr/bin/env bash
set -euo pipefail
exec 3>&2

readonly SCRIPT_DIRECTORY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPOSITORY_ROOT="$(cd "${SCRIPT_DIRECTORY}/../.." && pwd)"
cd "${REPOSITORY_ROOT}"

readonly EVIDENCE_FILE="${REPOSITORY_ROOT}/docs/compatibility/kafka-4.3-client-requests.json"
readonly BOUNDED_COMMAND="${SCRIPT_DIRECTORY}/bounded-command.py"
readonly KAFKA_IMAGE="apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837"
readonly MAVEN_IMAGE="maven:3.9.11-eclipse-temurin-25"
readonly PROXY_IMAGE="${MEMKAFKA_API_VERSION_PROXY_IMAGE:-memkafka-api-version-proxy:test}"
readonly SEED_IMAGE="${MEMKAFKA_KAFBAT_SEED_IMAGE:-memkafka-kafbat-seed:ci}"
readonly SCENARIO_METADATA='[
  {"id":"confluent-kafka-2.15.0","client":"Confluent.Kafka","version":"2.15.0"},
  {"id":"confluent-kafka-flow-2.13.2","client":"Confluent.Kafka","version":"2.13.2"},
  {"id":"apache-kafka-java-4.3.1","client":"Apache Kafka Java","version":"4.3.1"},
  {"id":"rskafka-0.6.0","client":"rskafka","version":"0.6.0"},
  {"id":"franz-go-1.21.6","client":"franz-go","version":"1.21.6"},
  {"id":"kafbat-1.5.0","client":"Kafbat UI","version":"1.5.0"}
]'
readonly SUFFIX="$$"
readonly KAFKA_CONTAINER="memkafka-api-versions-kafka-${SUFFIX}"
readonly MAVEN_CONTAINER="memkafka-api-versions-maven-${SUFFIX}"
readonly JAVA_PROXY_CONTAINER="memkafka-api-versions-java-proxy-${SUFFIX}"
readonly NETWORK="memkafka-api-versions-host-${SUFFIX}"
readonly MAX_AUTO_CREATE_ATTEMPTS=5
readonly RECORDER_BUILD_TIMEOUT_SECONDS="${MEMKAFKA_API_VERSION_RECORDER_BUILD_TIMEOUT_SECONDS:-300}"
readonly IMAGE_BUILD_TIMEOUT_SECONDS="${MEMKAFKA_API_VERSION_IMAGE_BUILD_TIMEOUT_SECONDS:-900}"
readonly DOTNET_SETUP_TIMEOUT_SECONDS="${MEMKAFKA_API_VERSION_DOTNET_SETUP_TIMEOUT_SECONDS:-60}"
readonly DOTNET_SCENARIO_TIMEOUT_SECONDS="${MEMKAFKA_API_VERSION_DOTNET_SCENARIO_TIMEOUT_SECONDS:-600}"
readonly JAVA_MAVEN_SCENARIO_TIMEOUT_SECONDS="${MEMKAFKA_API_VERSION_JAVA_MAVEN_SCENARIO_TIMEOUT_SECONDS:-900}"
readonly RUST_SCENARIO_TIMEOUT_SECONDS="${MEMKAFKA_API_VERSION_RUST_SCENARIO_TIMEOUT_SECONDS:-600}"
readonly GO_SCENARIO_TIMEOUT_SECONDS="${MEMKAFKA_API_VERSION_GO_SCENARIO_TIMEOUT_SECONDS:-600}"
readonly KAFBAT_SCENARIO_TIMEOUT_SECONDS="${MEMKAFKA_API_VERSION_KAFBAT_SCENARIO_TIMEOUT_SECONDS:-600}"
readonly INFRASTRUCTURE_TIMEOUT_SECONDS="${MEMKAFKA_API_VERSION_INFRASTRUCTURE_TIMEOUT_SECONDS:-60}"
readonly READINESS_PROBE_TIMEOUT_SECONDS="${MEMKAFKA_API_VERSION_READINESS_PROBE_TIMEOUT_SECONDS:-10}"
readonly KAFKA_READINESS_TIMEOUT_SECONDS="${MEMKAFKA_API_VERSION_KAFKA_READINESS_TIMEOUT_SECONDS:-90}"
readonly RECORDER_READINESS_TIMEOUT_SECONDS="${MEMKAFKA_API_VERSION_RECORDER_READINESS_TIMEOUT_SECONDS:-15}"
readonly RECORDER_LIFETIME_TIMEOUT_SECONDS="${MEMKAFKA_API_VERSION_RECORDER_LIFETIME_TIMEOUT_SECONDS:-3600}"
readonly JAVA_RECORDER_READINESS_TIMEOUT_SECONDS="${MEMKAFKA_API_VERSION_JAVA_RECORDER_READINESS_TIMEOUT_SECONDS:-30}"
readonly OBSERVATION_TIMEOUT_SECONDS="${MEMKAFKA_API_VERSION_OBSERVATION_TIMEOUT_SECONDS:-10}"
readonly CLEANUP_COMMAND_TIMEOUT_SECONDS="${MEMKAFKA_API_VERSION_CLEANUP_COMMAND_TIMEOUT_SECONDS:-20}"
readonly TERMINATION_GRACE_SECONDS="${MEMKAFKA_API_VERSION_TERMINATION_GRACE_SECONDS:-5}"

MODE=""
TEMP_DIRECTORY=""
RAW_DIRECTORY=""
PROXY_PID=""
RECORDER_ADDRESS=""
RECORDER_SCENARIO=""
KAFKA_RUNNING=false
LAST_LOG=""
DOTNET_WORK_DIRECTORY=""
ACTIVE_COMMAND_PID=""
ACTIVE_COMMAND_LABEL=""
ACTIVE_COMMAND_SIGNAL=TERM
DEFERRED_SIGNAL=""

usage() {
  printf 'usage: %s --check|--update\n' "$0" >&2
  exit 2
}

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
  if [[ -z "${DEFERRED_SIGNAL}" ]]; then
    DEFERRED_SIGNAL=INT
  fi
}

defer_termination() {
  if [[ -z "${DEFERRED_SIGNAL}" ]]; then
    DEFERRED_SIGNAL=TERM
  fi
}

begin_supervisor_spawn() {
  DEFERRED_SIGNAL=""
  trap defer_interrupt INT
  trap defer_termination TERM
}

finish_supervisor_spawn() {
  local pending_signal=""

  trap handle_interrupt INT
  trap handle_termination TERM
  pending_signal="${DEFERRED_SIGNAL}"
  DEFERRED_SIGNAL=""
  case "${pending_signal}" in
    INT)
      handle_interrupt
      ;;
    TERM)
      handle_termination
      ;;
  esac
}

run_bounded() {
  local timeout_seconds=$1
  local label=$2
  local exit_code=0
  shift 2

  ACTIVE_COMMAND_LABEL="${label}"
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
  ACTIVE_COMMAND_LABEL=""
  return "${exit_code}"
}

run_cleanup_command() {
  local label=$1
  shift

  python3 "${BOUNDED_COMMAND}" \
    --timeout "${CLEANUP_COMMAND_TIMEOUT_SECONDS}" \
    --termination-grace "${TERMINATION_GRACE_SECONDS}" \
    --label "${label}" \
    -- "$@"
}

stop_recorder() {
  if [[ -n "${PROXY_PID}" ]]; then
    stop_supervised_command "${PROXY_PID}" TERM
    PROXY_PID=""
    RECORDER_ADDRESS=""
    RECORDER_SCENARIO=""
  fi
}

stop_kafka() {
  if [[ "${KAFKA_RUNNING}" == true ]]; then
    run_cleanup_command "capture Kafka diagnostics" \
      docker logs "${KAFKA_CONTAINER}" >"${RAW_DIRECTORY}/kafka.log" 2>&1 || true
    run_cleanup_command "remove Kafka container" \
      docker rm --force "${KAFKA_CONTAINER}" >/dev/null 2>&1 || true
    KAFKA_RUNNING=false
  fi
}

cleanup() {
  local exit_code=$?

  if [[ -n "${ACTIVE_COMMAND_PID}" ]]; then
    stop_supervised_command "${ACTIVE_COMMAND_PID}" "${ACTIVE_COMMAND_SIGNAL}"
    ACTIVE_COMMAND_PID=""
    ACTIVE_COMMAND_LABEL=""
  fi
  stop_recorder
  stop_kafka
  run_cleanup_command "remove task helper containers" \
    docker rm --force "${MAVEN_CONTAINER}" "${JAVA_PROXY_CONTAINER}" >/dev/null 2>&1 || true
  run_cleanup_command "remove task network" \
    docker network rm "${NETWORK}" >/dev/null 2>&1 || true
  if [[ -n "${TEMP_DIRECTORY}" && -d "${TEMP_DIRECTORY}" ]]; then
    rm -rf "${TEMP_DIRECTORY}"
  fi
  if ((exit_code != 0)) && [[ -n "${LAST_LOG}" && -f "${LAST_LOG}" ]]; then
    printf 'Last scenario diagnostics (%s):\n' "${LAST_LOG}" >&3
    cat "${LAST_LOG}" >&3
  fi
  if [[ -n "${RAW_DIRECTORY}" ]]; then
    printf 'Kafka API version diagnostics: %s\n' "${RAW_DIRECTORY}" >&3
  fi
  return "${exit_code}"
}

handle_interrupt() {
  printf 'received INT; cancelling Kafka API version capture\n' >&3
  ACTIVE_COMMAND_SIGNAL=INT
  exit 130
}

handle_termination() {
  printf 'received TERM; cancelling Kafka API version capture\n' >&3
  ACTIVE_COMMAND_SIGNAL=TERM
  exit 143
}

trap cleanup EXIT
trap handle_interrupt INT
trap handle_termination TERM

validate_evidence() {
  local evidence=$1

  jq --exit-status \
    --arg image "${KAFKA_IMAGE}" \
    --argjson metadata "${SCENARIO_METADATA}" \
    '
      (keys == ["kafkaBaseline", "scenarios", "schemaVersion"])
      and (.schemaVersion == 1)
      and (.kafkaBaseline | keys == ["image", "version"])
      and (.kafkaBaseline == {version: "4.3.1", image: $image})
      and (.scenarios | type == "array")
      and (.scenarios | map(.id) == (map(.id) | sort))
      and (
        .scenarios | map({id, client, version})
        == ($metadata | sort_by(.id))
      )
      and all(.scenarios[];
        keys == ["client", "id", "requests", "version"]
        and (.id | type == "string")
        and (.client | type == "string")
        and (.version | type == "string")
        and (.requests | type == "array" and length > 0)
        and (.requests | map(.apiKey) == (map(.apiKey) | sort | unique))
        and all(.requests[];
          keys == ["apiKey", "versions"]
          and (.apiKey | type == "number")
          and (.apiKey == (.apiKey | floor))
          and (.versions | type == "array" and length > 0)
          and (.versions == (.versions | sort | unique))
          and all(.versions[];
            type == "number" and . == floor
          )
        )
      )
    ' "${evidence}" >/dev/null
}

normalize_observations() {
  local destination=$1
  shift

  jq --slurp \
    --arg image "${KAFKA_IMAGE}" \
    --argjson metadata "${SCENARIO_METADATA}" \
    '
      . as $observations
      | if all($observations[];
          keys == ["apiKey", "apiVersion", "clientId", "scenario"]
          and (.scenario | type == "string")
          and (.apiKey | type == "number")
          and (.apiVersion | type == "number")
          and (.clientId == null or (.clientId | type == "string"))
        ) then . else error("invalid recorder observation schema") end
      | ($observations | map(.scenario) | sort | unique) as $actual_scenarios
      | ($metadata | map(.id) | sort) as $expected_scenarios
      | if $actual_scenarios == $expected_scenarios
        then .
        else error(
          "unexpected or missing scenarios: actual="
          + ($actual_scenarios | tojson)
          + " expected="
          + ($expected_scenarios | tojson)
        )
        end
      | {
          schemaVersion: 1,
          kafkaBaseline: {
            version: "4.3.1",
            image: $image
          },
          scenarios: (
            $metadata
            | map(
                . as $scenario
                | {
                    id: $scenario.id,
                    client: $scenario.client,
                    version: $scenario.version,
                    requests: (
                      $observations
                      | map(select(.scenario == $scenario.id))
                      | group_by(.apiKey)
                      | map({
                          apiKey: .[0].apiKey,
                          versions: (map(.apiVersion) | sort | unique)
                        })
                      | sort_by(.apiKey)
                    )
                  }
              )
            | sort_by(.id)
          )
        }
    ' "$@" >"${destination}"
}

available_loopback_port() {
  python3 -c 'import socket; sock = socket.socket(); sock.bind(("127.0.0.1", 0)); print(sock.getsockname()[1]); sock.close()'
}

start_recorder() {
  local scenario=$1
  local listen_address=$2
  local observation_file="${RAW_DIRECTORY}/${scenario}.jsonl"
  local recorder_log="${RAW_DIRECTORY}/${scenario}-proxy.log"
  local ready_address=""
  local deadline=$((SECONDS + RECORDER_READINESS_TIMEOUT_SECONDS))

  : >"${observation_file}"
  : >"${recorder_log}"
  RECORDER_SCENARIO="${scenario}"
  begin_supervisor_spawn
  python3 "${BOUNDED_COMMAND}" \
    --timeout "${RECORDER_LIFETIME_TIMEOUT_SECONDS}" \
    --termination-grace "${TERMINATION_GRACE_SECONDS}" \
    --label "recorder ${scenario}" \
    -- "${SCRIPT_DIRECTORY}/proxy/target/debug/kafka-api-version-proxy" \
    --listen "${listen_address}" \
    --upstream "127.0.0.1:${KAFKA_PORT}" \
    --scenario "${scenario}" \
    --output "${observation_file}" >"${recorder_log}" 2>&1 &
  PROXY_PID=$!
  finish_supervisor_spawn

  while ((SECONDS < deadline)); do
    ready_address="$(awk -F= '/^READY listen=/{print $2; exit}' "${recorder_log}")"
    if [[ -n "${ready_address}" ]]; then
      break
    fi
    if ! kill -0 "${PROXY_PID}" >/dev/null 2>&1; then
      cat "${recorder_log}" >&2
      printf 'recorder exited before readiness for %s\n' "${scenario}" >&2
      exit 1
    fi
    sleep 0.1
  done
  if [[ -z "${ready_address}" ]]; then
    printf 'recorder did not become ready for %s within %ss\n' \
      "${scenario}" "${RECORDER_READINESS_TIMEOUT_SECONDS}" >&2
    exit 1
  fi
  if [[ "${listen_address}" != "127.0.0.1:0" && "${ready_address}" != "${listen_address}" ]]; then
    printf 'recorder rebound to %s, expected %s\n' "${ready_address}" "${listen_address}" >&2
    exit 1
  fi
  case "${ready_address}" in
    127.0.0.1:[0-9]*)
      ;;
    *)
      printf 'recorder reported an invalid loopback address: %s\n' "${ready_address}" >&2
      exit 1
      ;;
  esac
  RECORDER_ADDRESS="${ready_address}"
}

wait_for_kafka() {
  local ready=false
  local deadline=$((SECONDS + KAFKA_READINESS_TIMEOUT_SECONDS))
  local probe_timeout=0
  local readiness_log="${RAW_DIRECTORY}/kafka-readiness.log"

  : >"${readiness_log}"
  while ((SECONDS < deadline)); do
    probe_timeout=$((deadline - SECONDS))
    if ((probe_timeout > READINESS_PROBE_TIMEOUT_SECONDS)); then
      probe_timeout=${READINESS_PROBE_TIMEOUT_SECONDS}
    fi
    if run_bounded "${probe_timeout}" "probe Kafka readiness" -- \
      docker exec "${KAFKA_CONTAINER}" \
      /opt/kafka/bin/kafka-broker-api-versions.sh \
      --bootstrap-server localhost:19092 >>"${readiness_log}" 2>&1; then
      ready=true
      break
    fi
    sleep 1
  done
  if [[ "${ready}" != true ]]; then
    run_bounded "${READINESS_PROBE_TIMEOUT_SECONDS}" "capture Kafka readiness diagnostics" -- \
      docker logs "${KAFKA_CONTAINER}" >&2 || true
    printf 'Kafka 4.3.1 did not become ready within %s seconds; probe log: %s\n' \
      "${KAFKA_READINESS_TIMEOUT_SECONDS}" "${readiness_log}" >&2
    exit 1
  fi
}

ensure_observations() {
  local scenario=$1
  local observations="${RAW_DIRECTORY}/${scenario}.jsonl"
  local deadline=$((SECONDS + OBSERVATION_TIMEOUT_SECONDS))

  while ((SECONDS < deadline)); do
    if [[ -s "${observations}" ]]; then
      return
    fi
    sleep 0.1
  done
  printf 'recorder captured no requests for %s within %ss\n' \
    "${scenario}" "${OBSERVATION_TIMEOUT_SECONDS}" >&2
  exit 1
}

run_host_scenario() {
  local scenario=$1
  local attempt=1
  shift

  if [[ "${RECORDER_SCENARIO}" != "${scenario}" ]]; then
    stop_recorder
    start_recorder "${scenario}" "${ADVERTISED_ADDRESS}"
  fi
  LAST_LOG="${RAW_DIRECTORY}/${scenario}.log"
  while ! "$@" >"${LAST_LOG}" 2>&1; do
    if ((attempt < MAX_AUTO_CREATE_ATTEMPTS)) \
      && "${SCRIPT_DIRECTORY}/is-retryable-auto-create-failure.sh" \
        "${scenario}" "${LAST_LOG}"; then
      mv "${LAST_LOG}" "${LAST_LOG%.log}-attempt-${attempt}.log"
      printf 'retrying %s after exact fresh-topic visibility race (attempt %d/%d)\n' \
        "${scenario}" "$((attempt + 1))" "${MAX_AUTO_CREATE_ATTEMPTS}" >&3
      attempt=$((attempt + 1))
      continue
    fi
    printf 'client scenario failed: %s\n' "${scenario}" >&2
    exit 1
  done
  ensure_observations "${scenario}"
  stop_recorder
}

run_java_scenario() {
  local advertised_port="${ADVERTISED_ADDRESS##*:}"
  local java_exit=0
  local ready=false
  local deadline=0
  local probe_timeout=0
  local readiness_log="${RAW_DIRECTORY}/apache-kafka-java-4.3.1-readiness.log"

  if command -v mvn >/dev/null 2>&1; then
    run_bounded \
      "${JAVA_MAVEN_SCENARIO_TIMEOUT_SECONDS}" \
      "Apache Kafka Java 4.3.1 Maven scenario" \
      -- env MEMKAFKA_BOOTSTRAP_SERVERS="${ADVERTISED_ADDRESS}" \
      mvn --batch-mode --file tests/java/pom.xml test
    return
  fi

  stop_recorder
  : >"${RAW_DIRECTORY}/apache-kafka-java-4.3.1.jsonl"
  run_bounded "${INFRASTRUCTURE_TIMEOUT_SECONDS}" "start containerized Java recorder" -- \
    docker run --detach \
    --name "${JAVA_PROXY_CONTAINER}" \
    --network "${NETWORK}" \
    --volume "${RAW_DIRECTORY}:/artifacts" \
    "${PROXY_IMAGE}" \
    --listen "0.0.0.0:${advertised_port}" \
    --upstream "${KAFKA_CONTAINER_ADDRESS}:19092" \
    --scenario apache-kafka-java-4.3.1 \
    --output /artifacts/apache-kafka-java-4.3.1.jsonl >/dev/null
  deadline=$((SECONDS + JAVA_RECORDER_READINESS_TIMEOUT_SECONDS))
  : >"${readiness_log}"
  while ((SECONDS < deadline)); do
    probe_timeout=$((deadline - SECONDS))
    if ((probe_timeout > READINESS_PROBE_TIMEOUT_SECONDS)); then
      probe_timeout=${READINESS_PROBE_TIMEOUT_SECONDS}
    fi
    if run_bounded "${probe_timeout}" "probe containerized Java recorder readiness" -- \
      docker logs "${JAVA_PROXY_CONTAINER}" >"${readiness_log}" 2>&1 \
      && grep -F "READY listen=0.0.0.0:${advertised_port}" \
        "${readiness_log}" >/dev/null; then
      ready=true
      break
    fi
    sleep 0.1
  done
  if [[ "${ready}" != true ]]; then
    cat "${readiness_log}" >&2
    printf 'containerized Java recorder did not become ready within %ss\n' \
      "${JAVA_RECORDER_READINESS_TIMEOUT_SECONDS}" >&2
    return 1
  fi

  mkdir -p "${RAW_DIRECTORY}/maven-repository"
  run_bounded \
    "${JAVA_MAVEN_SCENARIO_TIMEOUT_SECONDS}" \
    "Apache Kafka Java 4.3.1 containerized Maven scenario" \
    -- docker run --rm \
    --name "${MAVEN_CONTAINER}" \
    --network "container:${JAVA_PROXY_CONTAINER}" \
    --user "$(id -u):$(id -g)" \
    --volume "${REPOSITORY_ROOT}:/workspace" \
    --volume "${RAW_DIRECTORY}/maven-repository:/maven-repository" \
    --workdir /workspace \
    --env MAVEN_CONFIG=/tmp/maven-config \
    --env "MEMKAFKA_BOOTSTRAP_SERVERS=${ADVERTISED_ADDRESS}" \
    "${MAVEN_IMAGE}" \
    mvn -Dmaven.repo.local=/maven-repository \
    --batch-mode \
    --file tests/java/pom.xml \
    test || java_exit=$?
  run_bounded "${READINESS_PROBE_TIMEOUT_SECONDS}" "capture Java recorder diagnostics" -- \
    docker logs "${JAVA_PROXY_CONTAINER}" \
    >"${RAW_DIRECTORY}/apache-kafka-java-4.3.1-proxy.log" 2>&1 || true
  run_bounded "${CLEANUP_COMMAND_TIMEOUT_SECONDS}" "remove Java recorder container" -- \
    docker rm --force "${JAVA_PROXY_CONTAINER}" >/dev/null 2>&1 || true
  return "${java_exit}"
}

run_confluent_scenario() {
  run_bounded \
    "${DOTNET_SCENARIO_TIMEOUT_SECONDS}" \
    "Confluent.Kafka 2.15.0 .NET scenario" \
    --chdir "${DOTNET_WORK_DIRECTORY}" \
    -- env \
    MEMKAFKA_BOOTSTRAP_SERVERS="${ADVERTISED_ADDRESS}" \
    MEMKAFKA_KAFKA_ONLY=true \
    dotnet run --no-restore \
    --project "${REPOSITORY_ROOT}/tests/confluent/MemKafka.Acceptance.csproj"
}

run_flow_scenario() {
  run_bounded \
    "${DOTNET_SCENARIO_TIMEOUT_SECONDS}" \
    "Confluent.Kafka 2.13.2 .NET scenario" \
    --chdir "${DOTNET_WORK_DIRECTORY}" \
    -- env \
    MEMKAFKA_BOOTSTRAP_SERVERS="${ADVERTISED_ADDRESS}" \
    MEMKAFKA_API_VERSION_PROBE=true \
    dotnet run --no-restore \
    --project "${REPOSITORY_ROOT}/tests/flow-compat/MemKafka.FlowCompatibility.csproj"
}

run_rust_scenario() {
  run_bounded \
    "${RUST_SCENARIO_TIMEOUT_SECONDS}" \
    "rskafka 0.6.0 Rust scenario" \
    -- env MEMKAFKA_BOOTSTRAP_SERVERS="${ADVERTISED_ADDRESS}" \
    cargo test --locked --manifest-path tests/rust-client/Cargo.toml
}

run_go_scenario() {
  run_bounded \
    "${GO_SCENARIO_TIMEOUT_SECONDS}" \
    "franz-go 1.21.6 Go scenario" \
    --chdir "${REPOSITORY_ROOT}/tests/go-client" \
    -- env \
    MEMKAFKA_BOOTSTRAP_SERVERS="${ADVERTISED_ADDRESS}" \
    MEMKAFKA_API_VERSION_PROBE=true \
    go test -count=1 -mod=readonly ./...
}

if [[ $# -ne 1 ]]; then
  usage
fi
case "$1" in
  --check)
    MODE=check
    ;;
  --update)
    MODE=update
    ;;
  *)
    usage
    ;;
esac

require_command cargo
require_command docker
require_command dotnet
require_command go
require_command jq
require_command python3

require_positive_integer MEMKAFKA_API_VERSION_RECORDER_BUILD_TIMEOUT_SECONDS \
  "${RECORDER_BUILD_TIMEOUT_SECONDS}"
require_positive_integer MEMKAFKA_API_VERSION_IMAGE_BUILD_TIMEOUT_SECONDS \
  "${IMAGE_BUILD_TIMEOUT_SECONDS}"
require_positive_integer MEMKAFKA_API_VERSION_DOTNET_SETUP_TIMEOUT_SECONDS \
  "${DOTNET_SETUP_TIMEOUT_SECONDS}"
require_positive_integer MEMKAFKA_API_VERSION_DOTNET_SCENARIO_TIMEOUT_SECONDS \
  "${DOTNET_SCENARIO_TIMEOUT_SECONDS}"
require_positive_integer MEMKAFKA_API_VERSION_JAVA_MAVEN_SCENARIO_TIMEOUT_SECONDS \
  "${JAVA_MAVEN_SCENARIO_TIMEOUT_SECONDS}"
require_positive_integer MEMKAFKA_API_VERSION_RUST_SCENARIO_TIMEOUT_SECONDS \
  "${RUST_SCENARIO_TIMEOUT_SECONDS}"
require_positive_integer MEMKAFKA_API_VERSION_GO_SCENARIO_TIMEOUT_SECONDS \
  "${GO_SCENARIO_TIMEOUT_SECONDS}"
require_positive_integer MEMKAFKA_API_VERSION_KAFBAT_SCENARIO_TIMEOUT_SECONDS \
  "${KAFBAT_SCENARIO_TIMEOUT_SECONDS}"
require_positive_integer MEMKAFKA_API_VERSION_INFRASTRUCTURE_TIMEOUT_SECONDS \
  "${INFRASTRUCTURE_TIMEOUT_SECONDS}"
require_positive_integer MEMKAFKA_API_VERSION_READINESS_PROBE_TIMEOUT_SECONDS \
  "${READINESS_PROBE_TIMEOUT_SECONDS}"
require_positive_integer MEMKAFKA_API_VERSION_KAFKA_READINESS_TIMEOUT_SECONDS \
  "${KAFKA_READINESS_TIMEOUT_SECONDS}"
require_positive_integer MEMKAFKA_API_VERSION_RECORDER_READINESS_TIMEOUT_SECONDS \
  "${RECORDER_READINESS_TIMEOUT_SECONDS}"
require_positive_integer MEMKAFKA_API_VERSION_RECORDER_LIFETIME_TIMEOUT_SECONDS \
  "${RECORDER_LIFETIME_TIMEOUT_SECONDS}"
require_positive_integer MEMKAFKA_API_VERSION_JAVA_RECORDER_READINESS_TIMEOUT_SECONDS \
  "${JAVA_RECORDER_READINESS_TIMEOUT_SECONDS}"
require_positive_integer MEMKAFKA_API_VERSION_OBSERVATION_TIMEOUT_SECONDS \
  "${OBSERVATION_TIMEOUT_SECONDS}"
require_positive_integer MEMKAFKA_API_VERSION_CLEANUP_COMMAND_TIMEOUT_SECONDS \
  "${CLEANUP_COMMAND_TIMEOUT_SECONDS}"
require_positive_integer MEMKAFKA_API_VERSION_TERMINATION_GRACE_SECONDS \
  "${TERMINATION_GRACE_SECONDS}"

if [[ "${MODE}" == check ]]; then
  if [[ ! -f "${EVIDENCE_FILE}" ]]; then
    printf 'Kafka API version evidence is missing: %s\n' "${EVIDENCE_FILE}" >&2
    exit 1
  fi
  if ! validate_evidence "${EVIDENCE_FILE}"; then
    printf 'Kafka API version evidence does not match the required deterministic schema: %s\n' \
      "${EVIDENCE_FILE}" >&2
    exit 1
  fi
fi

requested_raw_directory="${MEMKAFKA_API_VERSION_ARTIFACT_DIR:-${REPOSITORY_ROOT}/artifacts/api-versions}"
mkdir -p "${requested_raw_directory}"
readonly RAW_DIRECTORY="$(cd "${requested_raw_directory}" && pwd)/run-${SUFFIX}"
mkdir -p "${RAW_DIRECTORY}"
readonly TEMP_DIRECTORY="$(mktemp -d "${TMPDIR:-/tmp}/memkafka-api-versions.XXXXXX")"
readonly GENERATED_EVIDENCE="${TEMP_DIRECTORY}/kafka-4.3-client-requests.json"
readonly KAFKA_PORT="$(available_loopback_port)"

LAST_LOG="${RAW_DIRECTORY}/dotnet-sdks.log"
run_bounded "${DOTNET_SETUP_TIMEOUT_SECONDS}" "list installed .NET SDKs" -- \
  dotnet --list-sdks >"${LAST_LOG}" 2>&1
dotnet_sdk="$(awk '$1 ~ /^10\.[0-9]+\.[0-9]+$/ {sdk=$1} END {print sdk}' "${LAST_LOG}")"
if [[ -z "${dotnet_sdk}" ]]; then
  printf 'a stable .NET 10 SDK is required for the pinned Confluent.Kafka scenarios\n' >&2
  exit 1
fi
readonly DOTNET_WORK_DIRECTORY="${RAW_DIRECTORY}/dotnet-sdk"
LAST_LOG="${RAW_DIRECTORY}/dotnet-global-json.log"
run_bounded "${DOTNET_SETUP_TIMEOUT_SECONDS}" "pin stable .NET 10 SDK" -- \
  dotnet new globaljson \
  --sdk-version "${dotnet_sdk}" \
  --output "${DOTNET_WORK_DIRECTORY}" \
  --force >"${LAST_LOG}" 2>&1

printf 'Building the standalone recorder...\n'
LAST_LOG="${RAW_DIRECTORY}/proxy-build.log"
run_bounded "${RECORDER_BUILD_TIMEOUT_SECONDS}" "build standalone recorder" -- \
  cargo build --locked --manifest-path tests/api-versions/proxy/Cargo.toml \
  >"${LAST_LOG}" 2>&1
LAST_LOG="${RAW_DIRECTORY}/proxy-image-build.log"
run_bounded "${IMAGE_BUILD_TIMEOUT_SECONDS}" "build recorder image" -- \
  docker build \
  --file tests/api-versions/proxy/Dockerfile \
  --tag "${PROXY_IMAGE}" \
  "${REPOSITORY_ROOT}" >"${LAST_LOG}" 2>&1

start_recorder confluent-kafka-2.15.0 127.0.0.1:0
readonly ADVERTISED_ADDRESS="${RECORDER_ADDRESS}"

run_bounded "${INFRASTRUCTURE_TIMEOUT_SECONDS}" "create Kafka task network" -- \
  docker network create "${NETWORK}" >/dev/null
KAFKA_RUNNING=true
run_bounded "${INFRASTRUCTURE_TIMEOUT_SECONDS}" "start Kafka 4.3.1 container" -- \
  docker run --detach \
  --name "${KAFKA_CONTAINER}" \
  --network "${NETWORK}" \
  --network-alias kafka \
  --publish "127.0.0.1:${KAFKA_PORT}:19092" \
  --env KAFKA_NODE_ID=1 \
  --env KAFKA_PROCESS_ROLES=broker,controller \
  --env KAFKA_LISTENERS=PLAINTEXT://:19092,CONTROLLER://:19093 \
  --env "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://${ADVERTISED_ADDRESS}" \
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
wait_for_kafka
LAST_LOG="${RAW_DIRECTORY}/kafka-inspect.log"
run_bounded "${INFRASTRUCTURE_TIMEOUT_SECONDS}" "inspect Kafka task container" -- \
  docker inspect \
  --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' \
  "${KAFKA_CONTAINER}" >"${LAST_LOG}" 2>&1
readonly KAFKA_CONTAINER_ADDRESS="$(<"${LAST_LOG}")"
if [[ -z "${KAFKA_CONTAINER_ADDRESS}" ]]; then
  printf 'Kafka container has no address on %s\n' "${NETWORK}" >&2
  exit 1
fi

run_host_scenario confluent-kafka-2.15.0 run_confluent_scenario
run_host_scenario confluent-kafka-flow-2.13.2 run_flow_scenario

run_host_scenario apache-kafka-java-4.3.1 run_java_scenario
run_host_scenario rskafka-0.6.0 run_rust_scenario
run_host_scenario franz-go-1.21.6 run_go_scenario
stop_kafka

printf 'Building Kafbat oracle images...\n'
LAST_LOG="${RAW_DIRECTORY}/seed-image-build.log"
run_bounded "${IMAGE_BUILD_TIMEOUT_SECONDS}" "build Kafbat seed image" -- \
  docker build \
  --file tests/kafbat/Dockerfile.seed \
  --tag "${SEED_IMAGE}" \
  "${REPOSITORY_ROOT}" >"${LAST_LOG}" 2>&1

readonly KAFBAT_DIRECTORY="${RAW_DIRECTORY}/kafbat"
mkdir -p "${KAFBAT_DIRECTORY}"
LAST_LOG="${RAW_DIRECTORY}/kafbat-1.5.0.log"
if ! run_bounded \
  "${KAFBAT_SCENARIO_TIMEOUT_SECONDS}" \
  "Kafbat 1.5.0 oracle scenario" \
  -- env \
  MEMKAFKA_API_VERSION_PROXY_IMAGE="${PROXY_IMAGE}" \
  MEMKAFKA_KAFBAT_SEED_IMAGE="${SEED_IMAGE}" \
  MEMKAFKA_KAFBAT_LOG_DIR="${KAFBAT_DIRECTORY}" \
  "${SCRIPT_DIRECTORY}/kafbat.sh" >"${LAST_LOG}" 2>&1; then
  printf 'client scenario failed: kafbat-1.5.0\n' >&2
  exit 1
fi
if [[ ! -s "${KAFBAT_DIRECTORY}/kafbat-1.5.0.jsonl" ]]; then
  printf 'recorder captured no requests for kafbat-1.5.0\n' >&2
  exit 1
fi

normalize_observations \
  "${GENERATED_EVIDENCE}" \
  "${RAW_DIRECTORY}/confluent-kafka-2.15.0.jsonl" \
  "${RAW_DIRECTORY}/confluent-kafka-flow-2.13.2.jsonl" \
  "${RAW_DIRECTORY}/apache-kafka-java-4.3.1.jsonl" \
  "${RAW_DIRECTORY}/rskafka-0.6.0.jsonl" \
  "${RAW_DIRECTORY}/franz-go-1.21.6.jsonl" \
  "${KAFBAT_DIRECTORY}/kafbat-1.5.0.jsonl"

if ! validate_evidence "${GENERATED_EVIDENCE}"; then
  printf 'normalized Kafka API version evidence failed schema validation\n' >&2
  exit 1
fi

if [[ "${MODE}" == update ]]; then
  mkdir -p "$(dirname "${EVIDENCE_FILE}")"
  cp "${GENERATED_EVIDENCE}" "${EVIDENCE_FILE}"
  printf 'Updated Kafka API version evidence: %s\n' "${EVIDENCE_FILE}"
elif ! cmp --silent "${EVIDENCE_FILE}" "${GENERATED_EVIDENCE}"; then
  printf 'Kafka API version evidence differs; inspect and run %s --update deliberately:\n' "$0" >&2
  diff --unified "${EVIDENCE_FILE}" "${GENERATED_EVIDENCE}" >&2 || true
  exit 1
else
  printf 'PASS   Kafka API version evidence is byte-identical\n'
fi
