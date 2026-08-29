#!/usr/bin/env bash
set -euo pipefail

image_is_missing() {
  local image=$1
  local missing_image

  if grep -Fx "${image}" "${FAKE_RUN_IMAGE_STATE_FILE}" >/dev/null 2>&1; then
    return 1
  fi
  for missing_image in ${FAKE_RUN_MISSING_IMAGES:-}; do
    if [[ "${missing_image}" == "${image}" ]]; then
      return 0
    fi
  done
  return 1
}

case "${0##*/}" in
  cargo)
    printf 'cargo %s\n' "$*" >>"${FAKE_RUN_EVENT_LOG}"
    printf '%s\n' "${PWD}" >"${FAKE_RUN_CARGO_CWD}"
    printf '%s\n' "$$" >"${FAKE_RUN_CHILD_PID_FILE}"
    if [[ "${FAKE_RUN_MODE}" == descendant ]]; then
      (
        trap '' INT TERM
        while true; do
          sleep 1
        done
      ) &
      printf '%s\n' "$!" >"${FAKE_RUN_DESCENDANT_PID_FILE}"
    fi
    if [[ "${FAKE_RUN_MODE}" == hang || "${FAKE_RUN_MODE}" == descendant ]]; then
      trap 'exit 143' TERM
      trap 'exit 130' INT
      while true; do
        sleep 1
      done
    fi
    exit 79
    ;;
  docker)
    printf '%s\n' "$*" >>"${FAKE_RUN_DOCKER_LOG}"
    printf 'docker %s\n' "$*" >>"${FAKE_RUN_EVENT_LOG}"
    if [[ "${1:-}" == image && "${2:-}" == inspect ]]; then
      if image_is_missing "${3:-}"; then
        printf 'No such image: %s\n' "${3:-}" >&2
        exit 1
      fi
      exit 0
    fi
    if [[ "${1:-}" == pull ]]; then
      case "${FAKE_RUN_PULL_MODE:-success}" in
        fail)
          printf 'simulated pull failure: %s\n' "${2:-}" >&2
          exit 93
          ;;
        hang)
          printf '%s\n' "$$" >"${FAKE_RUN_PULL_PID_FILE}"
          trap 'exit 143' TERM
          trap 'exit 130' INT
          while true; do
            sleep 1
          done
          ;;
        success)
          printf '%s\n' "${2:-}" >>"${FAKE_RUN_IMAGE_STATE_FILE}"
          printf 'simulated pulled image: %s\n' "${2:-}"
          exit 0
          ;;
        *)
          printf 'unexpected fake pull mode: %s\n' "${FAKE_RUN_PULL_MODE}" >&2
          exit 94
          ;;
      esac
    fi
    exit 0
    ;;
  dotnet)
    case "${1:-}" in
      --list-sdks)
        printf '10.0.300 [/fake/dotnet/sdk]\n'
        ;;
      new)
        output_directory=""
        while (($#)); do
          if [[ "$1" == --output ]]; then
            output_directory=$2
            break
          fi
          shift
        done
        mkdir -p "${output_directory}"
        printf '{"sdk":{"version":"10.0.300"}}\n' >"${output_directory}/global.json"
        ;;
      *)
        exit 91
        ;;
    esac
    ;;
  go)
    exit 92
    ;;
  python3)
    if [[ "${1:-}" == *bounded-command.py ]]; then
      printf '%s\n' "$*" >>"${FAKE_RUN_SUPERVISOR_LOG}"
      printf '%s\n' "$$" >"${FAKE_RUN_SUPERVISOR_PID_FILE}"
      if [[ "$*" == *'--label build standalone recorder'* \
        && -n "${BASH_ENV:-}" ]]; then
        kill -TERM "${PPID}"
      fi
      exec "${FAKE_RUN_REAL_PYTHON3}" "$@"
    fi
    printf '29092\n'
    ;;
  *)
    printf 'unexpected fake command: %s\n' "${0##*/}" >&2
    exit 99
    ;;
esac
