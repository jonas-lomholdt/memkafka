#!/usr/bin/env python3
"""Run one command with a portable hard deadline and scoped cleanup."""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from collections.abc import Sequence
from typing import Optional


def parse_args(arguments: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", required=True, type=float)
    parser.add_argument("--termination-grace", required=True, type=float)
    parser.add_argument("--label", required=True)
    parser.add_argument("--chdir")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    parsed = parser.parse_args(arguments)
    if parsed.command[:1] == ["--"]:
        parsed.command = parsed.command[1:]
    if not parsed.command:
        parser.error("a command is required after --")
    if parsed.timeout <= 0:
        parser.error("--timeout must be positive")
    if parsed.termination_grace <= 0:
        parser.error("--termination-grace must be positive")
    return parsed


def main(arguments: Sequence[str]) -> int:
    parsed = parse_args(arguments)
    child: Optional[subprocess.Popen] = None
    owned_process_group: Optional[int] = None
    supervised_signals = {signal.SIGINT, signal.SIGTERM}
    previous_signal_mask = signal.pthread_sigmask(
        signal.SIG_BLOCK, supervised_signals
    )

    def stop_owned_group(initial_signal: signal.Signals) -> None:
        if (
            child is None
            or owned_process_group is None
            or child.returncode is not None
        ):
            return

        try:
            os.killpg(owned_process_group, initial_signal)
        except (PermissionError, ProcessLookupError):
            child.wait()
            return

        time.sleep(parsed.termination_grace)
        try:
            os.killpg(owned_process_group, signal.SIGKILL)
        except (PermissionError, ProcessLookupError):
            pass
        else:
            print(
                f"owned process group did not exit after {initial_signal.name} "
                f"for {parsed.termination_grace:g}s; killed: {parsed.label}",
                file=sys.stderr,
                flush=True,
            )
        child.wait()

    def forward_signal(signal_number: int, _frame: object) -> None:
        received_signal = signal.Signals(signal_number)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        stop_owned_group(received_signal)
        raise SystemExit(128 + signal_number)

    signal.signal(signal.SIGINT, forward_signal)
    signal.signal(signal.SIGTERM, forward_signal)
    try:
        child = subprocess.Popen(
            parsed.command,
            cwd=parsed.chdir,
            start_new_session=True,
            preexec_fn=lambda: signal.pthread_sigmask(
                signal.SIG_SETMASK, previous_signal_mask
            ),
        )
        owned_process_group = child.pid
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, previous_signal_mask)

    try:
        return child.wait(timeout=parsed.timeout)
    except subprocess.TimeoutExpired:
        print(
            f"timed out after {parsed.timeout:g}s: {parsed.label}",
            file=sys.stderr,
            flush=True,
        )
        stop_owned_group(signal.SIGTERM)
        return 124


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except FileNotFoundError as error:
        print(f"failed to start bounded command: {error}", file=sys.stderr)
        raise SystemExit(127) from error
    except PermissionError as error:
        print(f"failed to start bounded command: {error}", file=sys.stderr)
        raise SystemExit(126) from error
