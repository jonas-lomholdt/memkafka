#!/usr/bin/env bash
set -euo pipefail

readonly KAFBAT_IMAGE="ghcr.io/kafbat/kafka-ui:v1.5.0@sha256:7cda86a33344160309fdb65146332e4da65db81a945614f2fe32e210803f6fd1"
readonly MEMKAFKA_IMAGE="${MEMKAFKA_IMAGE:-memkafka:ci}"
readonly SEED_IMAGE="${MEMKAFKA_KAFBAT_SEED_IMAGE:-memkafka-kafbat-seed:ci}"
readonly SUFFIX="$$"
readonly NETWORK="memkafka-kafbat-${SUFFIX}"
readonly BROKER_CONTAINER="memkafka-kafbat-broker-${SUFFIX}"
readonly UI_CONTAINER="memkafka-kafbat-ui-${SUFFIX}"
readonly TOPIC="kafbat-probe-${SUFFIX}"
readonly KEY="kafbat-key-${SUFFIX}"
readonly VALUE="kafbat-value-${SUFFIX}"
readonly CLUSTER_RESPONSE="$(mktemp)"
readonly TOPICS_RESPONSE="$(mktemp)"
readonly MESSAGES_RESPONSE="$(mktemp)"
readonly LOG_DIR="${MEMKAFKA_KAFBAT_LOG_DIR:-${TMPDIR:-/tmp}/memkafka-kafbat-${SUFFIX}}"

mkdir -p "${LOG_DIR}"

cleanup() {
  local exit_code=$?

  docker logs "${BROKER_CONTAINER}" >"${LOG_DIR}/memkafka.log" 2>&1 || true
  docker logs "${UI_CONTAINER}" >"${LOG_DIR}/kafbat.log" 2>&1 || true
  cp "${CLUSTER_RESPONSE}" "${LOG_DIR}/cluster-response.json" || true
  cp "${TOPICS_RESPONSE}" "${LOG_DIR}/topics-response.json" || true
  cp "${MESSAGES_RESPONSE}" "${LOG_DIR}/messages-response.txt" || true

  if (( exit_code != 0 )); then
    cat "${LOG_DIR}/memkafka.log" >&2 || true
    cat "${LOG_DIR}/kafbat.log" >&2 || true
  fi
  docker rm --force "${BROKER_CONTAINER}" "${UI_CONTAINER}" >/dev/null 2>&1 || true
  docker network rm "${NETWORK}" >/dev/null 2>&1 || true
  rm -f "${CLUSTER_RESPONSE}" "${TOPICS_RESPONSE}" "${MESSAGES_RESPONSE}"
  echo "Kafbat diagnostics: ${LOG_DIR}"
  return "${exit_code}"
}
trap cleanup EXIT

docker image inspect "${MEMKAFKA_IMAGE}" >/dev/null
docker image inspect "${SEED_IMAGE}" >/dev/null
docker network create "${NETWORK}" >/dev/null

docker run --detach \
  --name "${BROKER_CONTAINER}" \
  --network "${NETWORK}" \
  "${MEMKAFKA_IMAGE}" \
  --kafka-listen 0.0.0.0:9092 \
  --schema-registry-listen 0.0.0.0:8081 \
  --kafka-advertised-address "${BROKER_CONTAINER}:9092" >/dev/null

docker run --detach \
  --name "${UI_CONTAINER}" \
  --network "${NETWORK}" \
  --publish 127.0.0.1::8080 \
  --env KAFKA_CLUSTERS_0_NAME=memkafka \
  --env "KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=${BROKER_CONTAINER}:9092" \
  --env "KAFKA_CLUSTERS_0_SCHEMAREGISTRY=http://${BROKER_CONTAINER}:8081" \
  "${KAFBAT_IMAGE}" >/dev/null

readonly MAPPED_ADDRESS="$(docker port "${UI_CONTAINER}" 8080/tcp)"
readonly KAFBAT_PORT="${MAPPED_ADDRESS##*:}"
readonly KAFBAT_URL="http://127.0.0.1:${KAFBAT_PORT}"

ready=false
for _ in {1..60}; do
  if curl --fail --silent --show-error "${KAFBAT_URL}/actuator/health" >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "${ready}" != true ]]; then
  echo "Kafbat did not become healthy within 60 seconds" >&2
  exit 1
fi

docker run --rm \
  --network "${NETWORK}" \
  --env "MEMKAFKA_BOOTSTRAP_SERVERS=${BROKER_CONTAINER}:9092" \
  --env "MEMKAFKA_KAFBAT_TOPIC=${TOPIC}" \
  --env "MEMKAFKA_KAFBAT_KEY=${KEY}" \
  --env "MEMKAFKA_KAFBAT_VALUE=${VALUE}" \
  "${SEED_IMAGE}" >/dev/null

online=false
for _ in {1..30}; do
  if curl --fail --silent --show-error --request POST \
      "${KAFBAT_URL}/api/clusters/memkafka/cache" >"${CLUSTER_RESPONSE}" \
      && jq --exit-status '.status == "ONLINE"' "${CLUSTER_RESPONSE}" >/dev/null; then
    online=true
    break
  fi
  sleep 1
done
if [[ "${online}" != true ]]; then
  echo "Kafbat did not report the MemKafka cluster online" >&2
  exit 1
fi

curl --fail --silent --show-error \
  "${KAFBAT_URL}/api/clusters/memkafka/topics?perPage=100" >"${TOPICS_RESPONSE}"
jq --exit-status --arg topic "${TOPIC}" \
  'any(.topics[]; .name == $topic and .partitionCount == 1)' \
  "${TOPICS_RESPONSE}" >/dev/null

curl --fail --silent --show-error --max-time 15 \
  "${KAFBAT_URL}/api/clusters/memkafka/topics/${TOPIC}/messages/v2?mode=EARLIEST&limit=10" \
  >"${MESSAGES_RESPONSE}"
sed -n 's/^data://p' "${MESSAGES_RESPONSE}" \
  | jq --slurp --exit-status --arg key "${KEY}" --arg value "${VALUE}" \
    'any(.[]; .type == "MESSAGE" and .message.key == $key and .message.value == $value)' \
    >/dev/null

echo "PASS   Kafbat UI discovered ${TOPIC} and returned its exact key/value"
