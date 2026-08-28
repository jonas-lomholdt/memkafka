#!/usr/bin/env python3
"""Exercise signal ownership at the bounded-command Popen boundary."""

from __future__ import annotations

import importlib.util
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from types import ModuleType

sys.dont_write_bytecode = True


def load_helper(helper_path: Path) -> ModuleType:
    specification = importlib.util.spec_from_file_location(
        "bounded_command_under_test", helper_path
    )
    if specification is None or specification.loader is None:
        raise RuntimeError(f"could not load {helper_path}")
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def run_signal_during_spawn(helper_path: Path, pid_file: Path) -> int:
    module = load_helper(helper_path)
    real_popen = module.subprocess.Popen

    def signal_before_returning_child(*args: object, **kwargs: object) -> object:
        child = real_popen(*args, **kwargs)
        pid_file.write_text(f"{child.pid}\n", encoding="utf-8")
        os.kill(os.getpid(), signal.SIGTERM)
        return child

    module.subprocess.Popen = signal_before_returning_child
    return module.main(
        [
            "--timeout",
            "30",
            "--termination-grace",
            "0.1",
            "--label",
            "immediate spawn signal test",
            "--",
            sys.executable,
            "-c",
            "import time; time.sleep(30)",
        ]
    )


def pid_exists(process_id: int) -> bool:
    try:
        os.kill(process_id, 0)
    except ProcessLookupError:
        return False
    return True


def assert_signal_during_spawn_is_owned(helper_path: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="bounded-command-behavior-") as directory:
        pid_file = Path(directory) / "child.pid"
        driver = subprocess.run(
            [sys.executable, __file__, "--driver", str(helper_path), str(pid_file)],
            check=False,
            capture_output=True,
            text=True,
        )
        if driver.returncode != 143:
            raise AssertionError(
                f"spawn-time TERM exited {driver.returncode}, expected 143:\n"
                f"{driver.stdout}{driver.stderr}"
            )
        if not pid_file.is_file():
            raise AssertionError("spawn-time child did not record its PID")

        child_pid = int(pid_file.read_text(encoding="utf-8"))
        for _ in range(40):
            if not pid_exists(child_pid):
                break
            time.sleep(0.05)
        else:
            try:
                os.killpg(child_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            raise AssertionError(
                f"spawn-time TERM left owned process group {child_pid} running"
            )


def main(arguments: list[str]) -> int:
    if arguments[:1] == ["--driver"]:
        if len(arguments) != 3:
            raise SystemExit("driver requires helper and PID file paths")
        return run_signal_during_spawn(Path(arguments[1]), Path(arguments[2]))
    if len(arguments) != 1:
        raise SystemExit(f"usage: {sys.argv[0]} BOUNDED_COMMAND")
    assert_signal_during_spawn_is_owned(Path(arguments[0]))
    print("PASS   bounded command spawn ownership")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
