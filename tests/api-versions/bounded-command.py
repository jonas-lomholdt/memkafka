#!/usr/bin/env python3
"""Run one command with a portable hard deadline and scoped cleanup."""

from __future__ import annotations

import argparse
import signal
import subprocess
import sys
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

    def stop_child(initial_signal: signal.Signals) -> None:
        if child is None or child.poll() is not None:
            return
        child.send_signal(initial_signal)
        try:
            child.wait(timeout=parsed.termination_grace)
        except subprocess.TimeoutExpired:
            print(
                f"command ignored {initial_signal.name} for "
                f"{parsed.termination_grace:g}s; killing: {parsed.label}",
                file=sys.stderr,
                flush=True,
            )
            child.kill()
            child.wait()

    def forward_signal(signal_number: int, _frame: object) -> None:
        received_signal = signal.Signals(signal_number)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        stop_child(received_signal)
        raise SystemExit(128 + signal_number)

    signal.signal(signal.SIGINT, forward_signal)
    signal.signal(signal.SIGTERM, forward_signal)
    child = subprocess.Popen(parsed.command, cwd=parsed.chdir)

    try:
        return child.wait(timeout=parsed.timeout)
    except subprocess.TimeoutExpired:
        print(
            f"timed out after {parsed.timeout:g}s: {parsed.label}",
            file=sys.stderr,
            flush=True,
        )
        stop_child(signal.SIGTERM)
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
