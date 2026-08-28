#!/usr/bin/env bash
set -euo pipefail

case "${0##*/}" in
  cargo)
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
