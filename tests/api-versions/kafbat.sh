#!/usr/bin/env bash
set -euo pipefail

readonly KAFKA_IMAGE="apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837"
readonly PROXY_IMAGE="${MEMKAFKA_API_VERSION_PROXY_IMAGE:-memkafka-api-version-proxy:test}"
readonly KAFBAT_IMAGE="ghcr.io/kafbat/kafka-ui:v1.5.0@sha256:7cda86a33344160309fdb65146332e4da65db81a945614f2fe32e210803f6fd1"
readonly SEED_IMAGE="${MEMKAFKA_KAFBAT_SEED_IMAGE:-memkafka-kafbat-seed:ci}"
readonly SUFFIX="$$"
readonly NETWORK="memkafka-api-versions-kafbat-${SUFFIX}"
readonly KAFKA_CONTAINER="memkafka-api-versions-kafka-${SUFFIX}"
readonly PROXY_CONTAINER="memkafka-api-versions-proxy-${SUFFIX}"
readonly UI_CONTAINER="memkafka-api-versions-kafbat-${SUFFIX}"
readonly SEED_CONTAINER="memkafka-api-versions-seed-${SUFFIX}"
readonly CLUSTER_NAME="kafka-oracle"
readonly GROUP_ID="kafbat-group-${SUFFIX}"
readonly TOPIC="kafbat-probe-${SUFFIX}"
readonly KEY="kafbat-key-${SUFFIX}"
readonly VALUE="kafbat-value-${SUFFIX}"
readonly CLUSTER_RESPONSE="$(mktemp)"
readonly GROUP_RESPONSE="$(mktemp)"
readonly TOPICS_RESPONSE="$(mktemp)"
readonly MESSAGES_RESPONSE="$(mktemp)"

requested_log_dir="${MEMKAFKA_KAFBAT_LOG_DIR:-${TMPDIR:-/tmp}/memkafka-api-versions-kafbat-${SUFFIX}}"
mkdir -p "${requested_log_dir}"
readonly LOG_DIR="$(cd "${requested_log_dir}" && pwd)"
readonly OBSERVATIONS_FILE="${LOG_DIR}/kafbat-1.5.0.jsonl"

cleanup() {
  local exit_code=$?

  docker logs "${KAFKA_CONTAINER}" >"${LOG_DIR}/kafka.log" 2>&1 || true
  docker logs "${PROXY_CONTAINER}" >"${LOG_DIR}/proxy.log" 2>&1 || true
  docker logs "${UI_CONTAINER}" >"${LOG_DIR}/kafbat.log" 2>&1 || true
  docker logs "${SEED_CONTAINER}" >"${LOG_DIR}/seed.log" 2>&1 || true
  cp "${CLUSTER_RESPONSE}" "${LOG_DIR}/cluster-response.json" || true
  cp "${GROUP_RESPONSE}" "${LOG_DIR}/group-response.json" || true
  cp "${TOPICS_RESPONSE}" "${LOG_DIR}/topics-response.json" || true
  cp "${MESSAGES_RESPONSE}" "${LOG_DIR}/messages-response.txt" || true

  if ((exit_code != 0)); then
    echo "Kafka oracle Kafbat scenario failed; retained diagnostics follow" >&2
    cat "${LOG_DIR}/kafka.log" >&2 || true
    cat "${LOG_DIR}/proxy.log" >&2 || true
    cat "${LOG_DIR}/kafbat.log" >&2 || true
    cat "${LOG_DIR}/seed.log" >&2 || true
  fi
  docker rm --force \
    "${KAFKA_CONTAINER}" \
    "${PROXY_CONTAINER}" \
    "${UI_CONTAINER}" \
    "${SEED_CONTAINER}" >/dev/null 2>&1 || true
  docker network rm "${NETWORK}" >/dev/null 2>&1 || true
  rm -f "${CLUSTER_RESPONSE}" "${GROUP_RESPONSE}" "${TOPICS_RESPONSE}" "${MESSAGES_RESPONSE}"
  echo "Kafka oracle Kafbat diagnostics: ${LOG_DIR}"
  return "${exit_code}"
}
trap cleanup EXIT

assert_seed_running() {
  local checkpoint=$1

  if [[ "$(docker inspect --format '{{.State.Running}}' "${SEED_CONTAINER}" 2>/dev/null)" != true ]]; then
    echo "Kafbat seed container stopped ${checkpoint}" >&2
    exit 1
  fi
}

docker image inspect "${KAFKA_IMAGE}" >/dev/null
docker image inspect "${PROXY_IMAGE}" >/dev/null
docker image inspect "${KAFBAT_IMAGE}" >/dev/null
docker image inspect "${SEED_IMAGE}" >/dev/null
docker network create "${NETWORK}" >/dev/null

docker run --detach \
  --name "${KAFKA_CONTAINER}" \
  --network "${NETWORK}" \
  --network-alias kafka \
  --env KAFKA_NODE_ID=1 \
  --env KAFKA_PROCESS_ROLES=broker,controller \
  --env KAFKA_LISTENERS=PLAINTEXT://:19092,CONTROLLER://:19093 \
  --env KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://api-version-proxy:9092 \
  --env KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  --env KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  --env KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:19093 \
  --env KAFKA_NUM_PARTITIONS=2 \
  --env KAFKA_AUTO_CREATE_TOPICS_ENABLE=true \
  --env KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  --env KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  --env KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  --env KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  "${KAFKA_IMAGE}" >/dev/null

readonly KAFKA_ADDRESS="$(docker inspect \
  --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}:19092' \
  "${KAFKA_CONTAINER}")"
if [[ "${KAFKA_ADDRESS}" == ":19092" ]]; then
  echo "Kafka container has no address on ${NETWORK}" >&2
  exit 1
fi

: >"${OBSERVATIONS_FILE}"
docker run --detach \
  --name "${PROXY_CONTAINER}" \
  --network "${NETWORK}" \
  --network-alias api-version-proxy \
  --volume "${LOG_DIR}:/artifacts" \
  "${PROXY_IMAGE}" \
  --listen 0.0.0.0:9092 \
  --upstream "${KAFKA_ADDRESS}" \
  --scenario kafbat-1.5.0 \
  --output /artifacts/kafbat-1.5.0.jsonl >/dev/null

docker run --detach \
  --name "${UI_CONTAINER}" \
  --network "${NETWORK}" \
  --publish 127.0.0.1::8080 \
  --env "KAFKA_CLUSTERS_0_NAME=${CLUSTER_NAME}" \
  --env KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=api-version-proxy:9092 \
  --env KAFKA_CLUSTERS_0_DEFAULTKEYSERDE=String \
  --env KAFKA_CLUSTERS_0_DEFAULTVALUESERDE=String \
  "${KAFBAT_IMAGE}" >/dev/null

readonly MAPPED_ADDRESS="$(docker port "${UI_CONTAINER}" 8080/tcp)"
readonly KAFBAT_PORT="${MAPPED_ADDRESS##*:}"
readonly KAFBAT_URL="http://127.0.0.1:${KAFBAT_PORT}"

ready=false
for _ in {1..60}; do
  if curl --fail --silent --show-error --max-time 5 \
      "${KAFBAT_URL}/actuator/health" >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "${ready}" != true ]]; then
  echo "Kafbat did not become healthy within 60 seconds" >&2
  exit 1
fi

docker run --detach \
  --name "${SEED_CONTAINER}" \
  --network "${NETWORK}" \
  --env MEMKAFKA_BOOTSTRAP_SERVERS=api-version-proxy:9092 \
  --env "MEMKAFKA_KAFBAT_TOPIC=${TOPIC}" \
  --env "MEMKAFKA_KAFBAT_KEY=${KEY}" \
  --env "MEMKAFKA_KAFBAT_VALUE=${VALUE}" \
  --env "MEMKAFKA_KAFBAT_GROUP=${GROUP_ID}" \
  --env MEMKAFKA_KAFBAT_STRING_ONLY=true \
  "${SEED_IMAGE}" >/dev/null

group_active=false
for _ in {1..30}; do
  if docker logs "${SEED_CONTAINER}" 2>&1 | grep -F "group active ${GROUP_ID}" >/dev/null; then
    group_active=true
    break
  fi
  sleep 1
done
if [[ "${group_active}" != true ]]; then
  echo "Kafbat seed consumer group did not become active" >&2
  exit 1
fi

online=false
for _ in {1..30}; do
  if curl --fail --silent --show-error --max-time 5 --request POST \
      "${KAFBAT_URL}/api/clusters/${CLUSTER_NAME}/cache" >"${CLUSTER_RESPONSE}" \
      && jq --exit-status '.status == "ONLINE"' "${CLUSTER_RESPONSE}" >/dev/null; then
    online=true
    break
  fi
  sleep 1
done
if [[ "${online}" != true ]]; then
  echo "Kafbat did not report the Kafka oracle cluster online" >&2
  exit 1
fi
assert_seed_running "after Kafbat reported the cluster online"

group_visible=false
for _ in {1..30}; do
  assert_seed_running "while waiting for consumer-group visibility"
  if curl --fail --silent --show-error --max-time 5 \
      "${KAFBAT_URL}/api/clusters/${CLUSTER_NAME}/consumer-groups/${GROUP_ID}" \
      >"${GROUP_RESPONSE}" \
      && jq --exit-status --arg group_id "${GROUP_ID}" \
        '.groupId == $group_id and .members >= 1' \
        "${GROUP_RESPONSE}" >/dev/null; then
    group_visible=true
    break
  fi
  sleep 1
done
if [[ "${group_visible}" != true ]]; then
  echo "Kafbat did not expose active consumer group ${GROUP_ID}" >&2
  exit 1
fi

curl --fail --silent --show-error --max-time 5 \
  "${KAFBAT_URL}/api/clusters/${CLUSTER_NAME}/topics?perPage=100" >"${TOPICS_RESPONSE}"
jq --exit-status --arg topic "${TOPIC}" \
  'any(.topics[]; .name == $topic and .partitionCount == 1)' \
  "${TOPICS_RESPONSE}" >/dev/null

curl --fail --silent --show-error --max-time 15 \
  "${KAFBAT_URL}/api/clusters/${CLUSTER_NAME}/topics/${TOPIC}/messages/v2?mode=EARLIEST&limit=10" \
  >"${MESSAGES_RESPONSE}"
sed -n 's/^data://p' "${MESSAGES_RESPONSE}" \
  | jq --slurp --exit-status --arg key "${KEY}" --arg value "${VALUE}" \
    'any(.[]; .type == "MESSAGE" and .message.key == $key and .message.value == $value)' \
    >/dev/null
assert_seed_running "after exact message browsing"
if [[ ! -s "${OBSERVATIONS_FILE}" ]]; then
  echo "Kafka API version recorder did not observe any Kafka requests" >&2
  exit 1
fi

echo "PASS   Kafbat UI returned the Kafka oracle group, topic, and exact string message"
