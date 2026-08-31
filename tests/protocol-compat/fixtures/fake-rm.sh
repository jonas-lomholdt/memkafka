#!/usr/bin/env bash
set -euo pipefail

if [[ "${FAKE_PROTOCOL_RM_MODE:-pass}" == hang-temp \
    && "${*: -1}" == "${TMPDIR:-/tmp}/memkafka-protocol-compat."* ]]; then
  target="${*: -1}"
  printf '%s\n' "$$" >"${FAKE_PROTOCOL_CLEANUP_PID_FILE:?}"
  printf '%s\n' "${target}" >"${FAKE_PROTOCOL_CLEANUP_TARGET_FILE:?}"
  trap '/bin/rm -rf "${target}"; exit 0' TERM
  sleep 30
fi

exec /bin/rm "$@"
