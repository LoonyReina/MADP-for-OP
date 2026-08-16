#!/usr/bin/env python3
"""Run one resident tester-daemon command without a console-host parent."""

from __future__ import annotations

import argparse
import json
import os
import runpy
import subprocess
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def process_creation_flags() -> int:
    if os.name != "nt":
        return 0
    return getattr(subprocess, "CREATE_NO_WINDOW", 0)


def process_startupinfo() -> subprocess.STARTUPINFO | None:
    if os.name != "nt":
        return None
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startupinfo.wShowWindow = 0
    return startupinfo


def read_payload(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("resident payload must be a JSON object")
    return data


def is_python_script_command(command: list[str]) -> bool:
    if len(command) < 2:
        return False
    executable = Path(command[0]).name.lower()
    return executable in {"python.exe", "pythonw.exe", "python", "pythonw"} and command[1].lower().endswith(".py")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_state(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--payload", required=True)
    args = parser.parse_args(argv)
    payload_path = Path(args.payload).resolve()
    payload = read_payload(payload_path)
    state_path = Path(
        str(
            payload.get("state")
            or payload_path.with_suffix(".state.json")
        )
    ).resolve()
    command = [str(item) for item in payload.get("command", [])]
    if not command:
        raise ValueError("resident payload command is empty")
    cwd = Path(str(payload.get("cwd", "."))).resolve()
    stdout_path = Path(str(payload.get("stdout", "resident.out.log"))).resolve()
    stderr_path = Path(str(payload.get("stderr", "resident.err.log"))).resolve()
    environment = payload.get("environment", {})
    if isinstance(environment, dict):
        for key, value in environment.items():
            os.environ[str(key)] = str(value)

    state = {
        "schema": "ascendop.resident-command-state.v1",
        "payload_path": str(payload_path),
        "state": "running",
        "pid": os.getpid(),
        "started_at": utc_now_iso(),
        "command": command,
    }
    write_state(state_path, state)
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    stderr_path.parent.mkdir(parents=True, exist_ok=True)
    os.chdir(cwd)
    original_stdio = (sys.stdin, sys.stdout, sys.stderr)
    exit_code = 1
    error = ""
    with open(os.devnull, "r", encoding="utf-8") as stdin, stdout_path.open(
        "a", encoding="utf-8", buffering=1
    ) as stdout, stderr_path.open("a", encoding="utf-8", buffering=1) as stderr:
        try:
            sys.stdin = stdin
            sys.stdout = stdout
            sys.stderr = stderr
            if is_python_script_command(command):
                sys.argv = command[1:]
                try:
                    runpy.run_path(command[1], run_name="__main__")
                except SystemExit as exc:
                    exit_code = (
                        int(exc.code or 0)
                        if isinstance(exc.code, (int, type(None)))
                        else 1
                    )
                else:
                    exit_code = 0
            else:
                completed = subprocess.run(
                    command,
                    cwd=str(cwd),
                    stdin=subprocess.DEVNULL,
                    stdout=stdout,
                    stderr=stderr,
                    env=os.environ.copy(),
                    creationflags=process_creation_flags(),
                    startupinfo=process_startupinfo(),
                    check=False,
                )
                exit_code = int(completed.returncode)
        except BaseException as exc:
            error = f"{type(exc).__name__}: {exc}"
            traceback.print_exc(file=stderr)
            exit_code = 1
        finally:
            sys.stdin, sys.stdout, sys.stderr = original_stdio
    write_state(
        state_path,
        {
            **state,
            "state": "completed" if exit_code == 0 else "failed",
            "finished_at": utc_now_iso(),
            "exit_code": exit_code,
            "error": error,
        },
    )
    return exit_code


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except BaseException:
        payload = ""
        if "--payload" in sys.argv:
            try:
                payload = sys.argv[sys.argv.index("--payload") + 1]
            except IndexError:
                payload = ""
        error_path = (
            Path(payload).resolve().with_suffix(".boot-error.log")
            if payload
            else Path.cwd() / "resident_process.boot-error.log"
        )
        error_path.parent.mkdir(parents=True, exist_ok=True)
        error_path.write_text(traceback.format_exc(), encoding="utf-8")
        raise
