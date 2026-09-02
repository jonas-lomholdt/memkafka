#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' "$*" >>"${FAKE_PROTOCOL_DOCKER_LOG:?}"

contains() {
  local needle=$1
  shift
  local argument
  for argument in "$@"; do
    if [[ "${argument}" == *"${needle}"* ]]; then
      return 0
    fi
  done
  return 1
}

result_directory() {
  local previous=""
  local argument
  for argument in "$@"; do
    if [[ "${previous}" == --volume && "${argument}" == *:/results ]]; then
      printf '%s\n' "${argument%:/results}"
      return
    fi
    previous="${argument}"
  done
  return 1
}

probe_output_name() {
  local arguments="$*"
  arguments="${arguments##*--output /results/}"
  printf '%s\n' "${arguments%% *}"
}

case "${1:-} ${2:-}" in
  "image inspect")
    image=${3:?}
    if [[ "${FAKE_PROTOCOL_MISSING_LOCAL:-false}" == true \
        && "${image}" == "${MEMKAFKA_PROTOCOL_IMAGE:-memkafka:ci}" ]]; then
      exit 44
    fi
    ;;
  "network create"|"network rm")
    ;;
  "logs "*)
    printf 'MemKafka ready kafka=0.0.0.0:9092 schema_registry=http://0.0.0.0:8081 advertised_kafka=127.0.0.1:9092\n'
    ;;
  "inspect --format")
    printf 'true\n'
    ;;
  "exec "*)
    ;;
  "rm --force")
    ;;
  "pull "*)
    printf 'fake pull should be explicit\n'
    ;;
  "run "*)
    if contains --detach "$@"; then
      printf 'fake-container-id\n'
      exit 0
    fi
    if ! contains exec:java "$@"; then
      exit 0
    fi
    if contains typed-errors "$@"; then
      case "${FAKE_PROTOCOL_TYPED_MODE:-success}" in
        fail)
          printf 'simulated typed probe failure\n' >&2
          exit 88
          ;;
        hang)
          printf '%s\n' "$$" >"${FAKE_PROTOCOL_HANG_PID_FILE:?}"
          sleep 30
          ;;
      esac
    fi
    results=$(result_directory "$@")
    output=$(probe_output_name "$@")
    mkdir -p "${results}"
    if contains typed-errors "$@"; then
      printf '{"schemaVersion":1,"caseCount":27,"cases":[]}\n' >"${results}/${output}"
    elif contains supported-semantics "$@"; then
      printf '{"schemaVersion":1,"caseCount":8,"cases":[]}\n' >"${results}/${output}"
    else
      version=5
      if [[ "${output}" == *32767* ]]; then
        version=32767
      fi
      printf '{"requestedVersion":%s,"responseHeaderVersion":0,"decodedBodyVersion":0,"correlationId":1234,"error":"UNSUPPORTED_VERSION"}\n' \
        "${version}" >"${results}/${output}"
    fi
    ;;
  *)
    printf 'unsupported fake docker invocation: %s\n' "$*" >&2
    exit 91
    ;;
esac
