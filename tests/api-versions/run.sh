#!/usr/bin/env bash
set -euo pipefail

readonly SCRIPT_DIRECTORY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPOSITORY_ROOT="$(cd "${SCRIPT_DIRECTORY}/../.." && pwd)"
readonly EVIDENCE_FILE="${REPOSITORY_ROOT}/docs/compatibility/kafka-4.3-client-requests.json"
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

MODE=""
TEMP_DIRECTORY=""
RAW_DIRECTORY=""
PROXY_PID=""
RECORDER_ADDRESS=""
RECORDER_SCENARIO=""
KAFKA_RUNNING=false
LAST_LOG=""
DOTNET_WORK_DIRECTORY=""

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

stop_recorder() {
  if [[ -n "${PROXY_PID}" ]]; then
    if kill -0 "${PROXY_PID}" >/dev/null 2>&1; then
      kill "${PROXY_PID}" >/dev/null 2>&1 || true
    fi
    wait "${PROXY_PID}" >/dev/null 2>&1 || true
    PROXY_PID=""
    RECORDER_ADDRESS=""
    RECORDER_SCENARIO=""
  fi
}

stop_kafka() {
  if [[ "${KAFKA_RUNNING}" == true ]]; then
    docker logs "${KAFKA_CONTAINER}" >"${RAW_DIRECTORY}/kafka.log" 2>&1 || true
    docker rm --force "${KAFKA_CONTAINER}" >/dev/null 2>&1 || true
    KAFKA_RUNNING=false
  fi
}

cleanup() {
  local exit_code=$?

  stop_recorder
  stop_kafka
  docker rm --force "${MAVEN_CONTAINER}" "${JAVA_PROXY_CONTAINER}" >/dev/null 2>&1 || true
  docker network rm "${NETWORK}" >/dev/null 2>&1 || true
  if [[ -n "${TEMP_DIRECTORY}" && -d "${TEMP_DIRECTORY}" ]]; then
    rm -rf "${TEMP_DIRECTORY}"
  fi
  if ((exit_code != 0)) && [[ -n "${LAST_LOG}" && -f "${LAST_LOG}" ]]; then
    printf 'Last scenario diagnostics (%s):\n' "${LAST_LOG}" >&2
    cat "${LAST_LOG}" >&2
  fi
  if [[ -n "${RAW_DIRECTORY}" ]]; then
    printf 'Kafka API version diagnostics: %s\n' "${RAW_DIRECTORY}" >&2
  fi
  return "${exit_code}"
}
trap cleanup EXIT INT TERM

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

  : >"${observation_file}"
  : >"${recorder_log}"
  "${SCRIPT_DIRECTORY}/proxy/target/debug/kafka-api-version-proxy" \
    --listen "${listen_address}" \
    --upstream "127.0.0.1:${KAFKA_PORT}" \
    --scenario "${scenario}" \
    --output "${observation_file}" >"${recorder_log}" 2>&1 &
  PROXY_PID=$!
  RECORDER_SCENARIO="${scenario}"

  for _ in {1..100}; do
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
    printf 'recorder did not become ready for %s\n' "${scenario}" >&2
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

  for _ in {1..90}; do
    if docker exec "${KAFKA_CONTAINER}" \
      /opt/kafka/bin/kafka-broker-api-versions.sh \
      --bootstrap-server localhost:19092 >/dev/null 2>&1; then
      ready=true
      break
    fi
    sleep 1
  done
  if [[ "${ready}" != true ]]; then
    docker logs "${KAFKA_CONTAINER}" >&2 || true
    printf 'Kafka 4.3.1 did not become ready within 90 seconds\n' >&2
    exit 1
  fi
}

ensure_observations() {
  local scenario=$1
  local observations="${RAW_DIRECTORY}/${scenario}.jsonl"

  for _ in {1..50}; do
    if [[ -s "${observations}" ]]; then
      return
    fi
    sleep 0.1
  done
  printf 'recorder captured no requests for %s\n' "${scenario}" >&2
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
    if [[ "${scenario}" == confluent-kafka-2.15.0 \
      && ${attempt} -lt 5 ]] \
      && grep -F "metadata for 'auto-" "${LAST_LOG}" >/dev/null \
      && grep -F 'UnknownTopicOrPart' "${LAST_LOG}" >/dev/null; then
      mv "${LAST_LOG}" "${LAST_LOG%.log}-attempt-${attempt}.log"
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

  if command -v mvn >/dev/null 2>&1; then
    env MEMKAFKA_BOOTSTRAP_SERVERS="${ADVERTISED_ADDRESS}" \
      mvn --batch-mode --file tests/java/pom.xml test
    return
  fi

  stop_recorder
  : >"${RAW_DIRECTORY}/apache-kafka-java-4.3.1.jsonl"
  docker run --detach \
    --name "${JAVA_PROXY_CONTAINER}" \
    --network "${NETWORK}" \
    --volume "${RAW_DIRECTORY}:/artifacts" \
    "${PROXY_IMAGE}" \
    --listen "0.0.0.0:${advertised_port}" \
    --upstream "${KAFKA_CONTAINER_ADDRESS}:19092" \
    --scenario apache-kafka-java-4.3.1 \
    --output /artifacts/apache-kafka-java-4.3.1.jsonl >/dev/null
  for _ in {1..100}; do
    if docker logs "${JAVA_PROXY_CONTAINER}" 2>&1 \
      | grep -F "READY listen=0.0.0.0:${advertised_port}" >/dev/null; then
      ready=true
      break
    fi
    sleep 0.1
  done
  if [[ "${ready}" != true ]]; then
    docker logs "${JAVA_PROXY_CONTAINER}" >&2 || true
    printf 'containerized Java recorder did not become ready\n' >&2
    return 1
  fi

  mkdir -p "${RAW_DIRECTORY}/maven-repository"
  docker run --rm \
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
  docker logs "${JAVA_PROXY_CONTAINER}" \
    >"${RAW_DIRECTORY}/apache-kafka-java-4.3.1-proxy.log" 2>&1 || true
  docker rm --force "${JAVA_PROXY_CONTAINER}" >/dev/null 2>&1 || true
  return "${java_exit}"
}

run_confluent_scenario() {
  (
    cd "${DOTNET_WORK_DIRECTORY}"
    env \
      MEMKAFKA_BOOTSTRAP_SERVERS="${ADVERTISED_ADDRESS}" \
      MEMKAFKA_KAFKA_ONLY=true \
      dotnet run --no-restore \
      --project "${REPOSITORY_ROOT}/tests/confluent/MemKafka.Acceptance.csproj"
  )
}

run_flow_scenario() {
  (
    cd "${DOTNET_WORK_DIRECTORY}"
    env \
      MEMKAFKA_BOOTSTRAP_SERVERS="${ADVERTISED_ADDRESS}" \
      MEMKAFKA_API_VERSION_PROBE=true \
      dotnet run --no-restore \
      --project "${REPOSITORY_ROOT}/tests/flow-compat/MemKafka.FlowCompatibility.csproj"
  )
}

run_rust_scenario() {
  env MEMKAFKA_BOOTSTRAP_SERVERS="${ADVERTISED_ADDRESS}" \
    cargo test --locked --manifest-path tests/rust-client/Cargo.toml
}

run_go_scenario() {
  (
    cd "${REPOSITORY_ROOT}/tests/go-client"
    env \
      MEMKAFKA_BOOTSTRAP_SERVERS="${ADVERTISED_ADDRESS}" \
      MEMKAFKA_API_VERSION_PROBE=true \
      go test -count=1 -mod=readonly ./...
  )
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

dotnet_sdk="$(dotnet --list-sdks \
  | awk '$1 ~ /^10\.[0-9]+\.[0-9]+$/ {sdk=$1} END {print sdk}')"
if [[ -z "${dotnet_sdk}" ]]; then
  printf 'a stable .NET 10 SDK is required for the pinned Confluent.Kafka scenarios\n' >&2
  exit 1
fi
readonly DOTNET_WORK_DIRECTORY="${RAW_DIRECTORY}/dotnet-sdk"
dotnet new globaljson \
  --sdk-version "${dotnet_sdk}" \
  --output "${DOTNET_WORK_DIRECTORY}" \
  --force >"${RAW_DIRECTORY}/dotnet-global-json.log" 2>&1

printf 'Building the standalone recorder...\n'
cargo build --locked --manifest-path tests/api-versions/proxy/Cargo.toml \
  >"${RAW_DIRECTORY}/proxy-build.log" 2>&1
LAST_LOG="${RAW_DIRECTORY}/proxy-image-build.log"
docker build \
  --file tests/api-versions/proxy/Dockerfile \
  --tag "${PROXY_IMAGE}" \
  "${REPOSITORY_ROOT}" >"${LAST_LOG}" 2>&1

start_recorder confluent-kafka-2.15.0 127.0.0.1:0
readonly ADVERTISED_ADDRESS="${RECORDER_ADDRESS}"

docker network create "${NETWORK}" >/dev/null
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
KAFKA_RUNNING=true
wait_for_kafka
readonly KAFKA_CONTAINER_ADDRESS="$(docker inspect \
  --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' \
  "${KAFKA_CONTAINER}")"
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
docker build \
  --file tests/kafbat/Dockerfile.seed \
  --tag "${SEED_IMAGE}" \
  "${REPOSITORY_ROOT}" >"${LAST_LOG}" 2>&1

readonly KAFBAT_DIRECTORY="${RAW_DIRECTORY}/kafbat"
mkdir -p "${KAFBAT_DIRECTORY}"
LAST_LOG="${RAW_DIRECTORY}/kafbat-1.5.0.log"
if ! env \
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
