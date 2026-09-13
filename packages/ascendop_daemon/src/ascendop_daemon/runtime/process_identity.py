from __future__ import annotations

import ctypes
from functools import lru_cache
import os
from pathlib import Path
import subprocess
import time


def process_start_token(pid: int) -> str:
    """Return a process-creation identity that changes when a PID is reused."""
    if pid <= 0:
        return ""
    if os.name == "nt":
        try:
            return _windows_process_start_token(pid)
        except (OSError, ValueError):
            return ""
    try:
        raw = (Path("/proc") / str(pid) / "stat").read_text(encoding="utf-8")
    except (FileNotFoundError, PermissionError, ProcessLookupError, OSError):
        return ""
    _prefix, separator, suffix = raw.rpartition(")")
    if not separator:
        return ""
    fields = suffix.strip().split()
    return fields[19] if len(fields) > 19 else ""


def process_identity_matches(pid: int, expected_start_token: str) -> bool:
    expected = str(expected_start_token or "")
    return bool(expected) and process_start_token(pid) == expected


def observe_process_identity(pid: int, expected_start_token: str) -> str:
    """Distinguish known exit/PID reuse from an inaccessible identity probe."""
    if pid <= 0:
        return "unknown"
    if os.name == "nt":
        try:
            state, token = _windows_process_identity(pid)
        except (OSError, ValueError):
            state, token = "unknown", ""
        if state == "unknown" and str(expected_start_token).startswith("win-"):
            try:
                expected_birth = int(expected_start_token[4:], 16)
            except ValueError:
                return "unknown"
            observation = _cached_creation_probe(pid, int(time.monotonic() // 30))
            # WMI truncates sub-microsecond precision. Only a clearly different
            # creation time proves PID reuse; same/missing observations remain
            # unknown. Never kill or adopt the unrelated protected process.
            if observation and abs(observation["creation_filetime_100ns"] - expected_birth) > 10_000_000:
                return "exited"
    else:
        try:
            raw = (Path("/proc") / str(pid) / "stat").read_text(encoding="utf-8")
        except (FileNotFoundError, ProcessLookupError):
            return "exited"
        except OSError:
            return "unknown"
        _, separator, suffix = raw.rpartition(")")
        fields = suffix.strip().split()
        if not separator or len(fields) <= 19:
            return "unknown"
        state, token = ("exited" if fields[0] in {"Z", "X"} else "alive"), fields[19]
    if state != "alive":
        return state
    # A definitely absent PID proves exit even for a legacy record without a
    # birth token. A present PID without that token proves neither ownership
    # nor reuse, so do not manufacture an exit from the missing binding.
    if not expected_start_token:
        return "unknown"
    return "alive" if token == expected_start_token else "exited"


def _windows_process_start_token(pid: int) -> str:
    return _windows_process_identity(pid)[1]


@lru_cache(maxsize=128)
def _cached_creation_probe(pid: int, observation_window: int):
    """Bound repeated denied-handle probes without caching unknown forever."""
    return windows_creation_probe(pid)


def windows_creation_probe(pid: int) -> dict | None:
    """Read-only fallback shared by migration and live identity observation."""
    if os.name != "nt" or type(pid) is not int or pid <= 0:
        return None
    command = ("$ErrorActionPreference='Stop'; "
        f"$observedNativeProcess=Get-CimInstance -ClassName Win32_Process -Filter 'ProcessId={pid}'; "
        "if ($null -ne $observedNativeProcess.CreationDate) { "
        "$observedNativeProcess.CreationDate.ToUniversalTime().ToFileTimeUtc().ToString() }")
    try:
        result = subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", command],
            capture_output=True, text=True, timeout=8, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
        birth = int(result.stdout.strip()) if result.returncode == 0 else 0
        if birth > 0:
            return {"provider": "Win32_Process.CreationDate", "pid": pid, "creation_filetime_100ns": birth}
    except (OSError, ValueError, subprocess.TimeoutExpired):
        pass
    return None


def _windows_process_identity(pid: int) -> tuple[str, str]:
    class FileTime(ctypes.Structure):
        _fields_ = [("low", ctypes.c_uint32), ("high", ctypes.c_uint32)]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    open_process = kernel32.OpenProcess
    open_process.argtypes = [ctypes.c_uint32, ctypes.c_int, ctypes.c_uint32]
    open_process.restype = ctypes.c_void_p
    get_process_times = kernel32.GetProcessTimes
    get_process_times.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(FileTime),
        ctypes.POINTER(FileTime),
        ctypes.POINTER(FileTime),
        ctypes.POINTER(FileTime),
    ]
    get_process_times.restype = ctypes.c_int
    get_exit_code_process = kernel32.GetExitCodeProcess
    get_exit_code_process.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint32)]
    get_exit_code_process.restype = ctypes.c_int
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [ctypes.c_void_p]
    close_handle.restype = ctypes.c_int

    handle = open_process(0x1000, 0, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
    if not handle:
        # ERROR_INVALID_PARAMETER is the documented absent-PID result;
        # access denied and other failures do not establish process death.
        return ("exited" if ctypes.get_last_error() == 87 else "unknown"), ""
    creation = FileTime()
    exit_time = FileTime()
    kernel = FileTime()
    user = FileTime()
    exit_code = ctypes.c_uint32()
    try:
        if not get_exit_code_process(handle, ctypes.byref(exit_code)):
            return "unknown", ""
        if exit_code.value != 259:  # STILL_ACTIVE
            return "exited", ""
        if not get_process_times(
            handle,
            ctypes.byref(creation),
            ctypes.byref(exit_time),
            ctypes.byref(kernel),
            ctypes.byref(user),
        ):
            return "unknown", ""
    finally:
        close_handle(handle)
    return "alive", f"win-{creation.high:08x}{creation.low:08x}"
