set -T

signal_during_spawn() {
  if [[ "${BASH_COMMAND}" == 'ACTIVE_COMMAND_PID=$!' \
    && "${LAST_LOG:-}" == */proxy-build.log \
    && ! -f "${FAKE_RUN_SPAWN_SIGNAL_MARKER}" ]]; then
    : >"${FAKE_RUN_SPAWN_SIGNAL_MARKER}"
    sleep 0.5
  fi
}

trap signal_during_spawn DEBUG
