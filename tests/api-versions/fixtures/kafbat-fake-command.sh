#!/usr/bin/env bash
set -euo pipefail

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
    case "${operation}" in
      image)
        exit 0
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
