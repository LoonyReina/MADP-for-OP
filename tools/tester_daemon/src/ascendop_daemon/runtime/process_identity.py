from __future__ import annotations

import ctypes
import os
from pathlib import Path


def process_start_token(pid: int) -> str:
    """Return a process-creation identity that changes when a PID is reused."""
    if pid <= 0:
        return ""
    if os.name == "nt":
        return _windows_process_start_token(pid)
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


def _windows_process_start_token(pid: int) -> str:
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
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [ctypes.c_void_p]
    close_handle.restype = ctypes.c_int

    handle = open_process(0x1000, 0, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
    if not handle:
        return ""
    creation = FileTime()
    exit_time = FileTime()
    kernel = FileTime()
    user = FileTime()
    try:
        if not get_process_times(
            handle,
            ctypes.byref(creation),
            ctypes.byref(exit_time),
            ctypes.byref(kernel),
            ctypes.byref(user),
        ):
            return ""
    finally:
        close_handle(handle)
    return f"win-{creation.high:08x}{creation.low:08x}"
