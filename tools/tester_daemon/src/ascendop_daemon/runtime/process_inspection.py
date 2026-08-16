from __future__ import annotations

import os
import subprocess
from typing import Any


def windows_process_table() -> list[dict[str, Any]]:
    """Return the Windows process table without spawning a console host."""
    if os.name != "nt":
        return []
    try:
        import psutil
    except ImportError:
        return []

    records: list[dict[str, Any]] = []
    for process in psutil.process_iter(["pid", "ppid", "cmdline", "name"]):
        try:
            info = process.info
            argv = [str(value) for value in (info.get("cmdline") or [])]
            command_line = subprocess.list2cmdline(argv) if argv else ""
            records.append(
                {
                    "ProcessId": int(info.get("pid", 0) or 0),
                    "ParentProcessId": int(info.get("ppid", 0) or 0),
                    "CommandLine": command_line,
                    "Name": str(info.get("name", "") or ""),
                }
            )
        except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess, OSError):
            continue
    return records


def windows_process_command_line(pid: int) -> str:
    if os.name != "nt" or pid <= 0:
        return ""
    try:
        import psutil
    except ImportError:
        return ""
    try:
        argv = psutil.Process(pid).cmdline()
    except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess, OSError):
        return ""
    return subprocess.list2cmdline([str(value) for value in argv]) if argv else ""
