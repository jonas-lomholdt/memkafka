#!/usr/bin/env bash
set -euo pipefail

image_is_missing() {
  local image=$1
  local missing_image

  if grep -Fx "${image}" "${FAKE_IMAGE_STATE_FILE}" >/dev/null 2>&1; then
    return 1
  fi
  for missing_image in ${FAKE_MISSING_IMAGES:-}; do
    if [[ "${missing_image}" == "${image}" ]]; then
      return 0
    fi
  done
  return 1
}

case "${0##*/}" in
  curl)
    printf '%s\n' "$*" >>"${FAKE_CURL_LOG}"
    if [[ "${FAKE_REQUIRE_MAX_TIME:-false}" == true ]]; then
      has_max_time=false
      for argument in "$@"; do
        if [[ "${argument}" == --max-time ]]; then
          has_max_time=true
          break
        fi
      done
      if [[ "${has_max_time}" != true ]]; then
        printf 'missing --max-time: %s\n' "$*" >>"${FAKE_CURL_LOG}"
        exit 97
      fi
    fi

    url=""
    for argument in "$@"; do
      if [[ "${argument}" == http://* || "${argument}" == https://* ]]; then
        url="${argument}"
      fi
    done
    case "${url}" in
      */actuator/health)
        printf '{"status":"UP"}\n'
        ;;
      */cache)
        printf '{"status":"ONLINE"}\n'
        ;;
      */consumer-groups/*)
        printf '{"groupId":"%s","members":1}\n' "${url##*/}"
        ;;
      */messages/v2*)
        printf 'data:{"type":"MESSAGE","message":{"key":"kafbat-key-%s","value":"kafbat-value-%s"}}\n' \
          "${PPID}" "${PPID}"
        ;;
      */topics\?*)
        printf '{"topics":[{"name":"kafbat-probe-%s","partitionCount":1}]}\n' "${PPID}"
        ;;
      *)
        printf 'unexpected fake curl URL: %s\n' "${url}" >&2
        exit 98
        ;;
    esac
    ;;
  docker)
    operation=${1:-}
    shift || true
    printf '%s %s\n' "${operation}" "$*" >>"${FAKE_DOCKER_LOG}"
    case "${operation}" in
      image)
        if [[ "${1:-}" == inspect ]] && image_is_missing "${2:-}"; then
          printf 'No such image: %s\n' "${2:-}" >&2
          exit 1
        fi
        exit 0
        ;;
      pull)
        case "${FAKE_PULL_MODE:-success}" in
          fail)
            printf 'simulated pull failure: %s\n' "${1:-}" >&2
            exit 93
            ;;
          hang)
            printf '%s\n' "$$" >"${FAKE_PULL_PID_FILE}"
            trap 'exit 143' TERM
            trap 'exit 130' INT
            while true; do
              /bin/sleep 1
            done
            ;;
          success)
            printf '%s\n' "${1:-}" >>"${FAKE_IMAGE_STATE_FILE}"
            printf 'simulated pulled image: %s\n' "${1:-}"
            exit 0
            ;;
          *)
            printf 'unexpected fake pull mode: %s\n' "${FAKE_PULL_MODE}" >&2
            exit 94
            ;;
        esac
        ;;
      network)
        exit 0
        ;;
      run)
        if [[ " $* " == *" --scenario kafbat-1.5.0 "* \
          && "${FAKE_RECORDER_WRITES:-true}" == true ]]; then
          printf '{"scenario":"kafbat-1.5.0","apiKey":18,"apiVersion":3,"clientId":"fake"}\n' \
            >>"${FAKE_LOG_DIR}/kafbat-1.5.0.jsonl"
        fi
        printf 'fake-container-id\n'
        ;;
      inspect)
        if [[ " $* " == *'.NetworkSettings.Networks'* ]]; then
          printf '172.18.0.2:19092\n'
        else
          printf 'true\n'
        fi
        ;;
      port)
        printf '127.0.0.1:18080\n'
        ;;
      logs)
        container=${1:-}
        if [[ "${container}" == *-seed-* ]]; then
          printf 'group active kafbat-group-%s\n' "${container##*-}"
        fi
        ;;
      rm)
        exit 0
        ;;
      *)
        printf 'unexpected fake docker operation: %s\n' "${operation}" >&2
        exit 99
        ;;
    esac
    ;;
  sleep)
    exit 0
    ;;
  *)
    printf 'unexpected fake command: %s\n' "${0##*/}" >&2
    exit 100
    ;;
esac
