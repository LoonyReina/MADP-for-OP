"""Observe an owned Windows Job, including orphaned tool grandchildren.

One sampling operation, no polling scheduler or business-state writer. A quiet
provider turn is a separate prerequisite. Never infer quiescence from parent
PID traversal, a missing named Job, a process name or a failed query.
"""
from __future__ import annotations

import ctypes as C
from ctypes import wintypes as W
import os
from pathlib import Path

from .windows_gated_process import _api, _birth


class _Entry(C.Structure):
    _fields_ = [("size", W.DWORD), ("usage", W.DWORD), ("pid", W.DWORD),
        ("heap", C.c_size_t), ("module", W.DWORD), ("threads", W.DWORD),
        ("parent", W.DWORD), ("priority", W.LONG), ("flags", W.DWORD), ("exe", W.WCHAR * 260)]


def _job_pids(k, handle):
    capacity = 32
    while capacity <= 65536:
        class Pids(C.Structure):
            _fields_ = [("assigned", W.DWORD), ("count", W.DWORD), ("pids", C.c_size_t * capacity)]
        value = Pids()
        okay = k.QueryInformationJobObject(handle, 3, C.byref(value), C.sizeof(value), None)
        if okay and value.count == value.assigned and value.count <= capacity:
            return set(value.pids[:value.count])
        if not okay and C.get_last_error() != 234:
            raise C.WinError(C.get_last_error())
        capacity = max(capacity * 2, value.assigned)
    raise OSError("resident Job process inventory exceeds bound")


def _process_api():
    k = _api()
    signatures = {
        "CreateToolhelp32Snapshot": (W.HANDLE, [W.DWORD, W.DWORD]),
        "Process32FirstW": (W.BOOL, [W.HANDLE, C.POINTER(_Entry)]),
        "Process32NextW": (W.BOOL, [W.HANDLE, C.POINTER(_Entry)]),
        "IsProcessInJob": (W.BOOL, [W.HANDLE, W.HANDLE, C.POINTER(W.BOOL)]),
        "QueryFullProcessImageNameW": (W.BOOL, [W.HANDLE, W.DWORD, W.LPWSTR, C.POINTER(W.DWORD)]),
    }
    for name, (result, arguments) in signatures.items():
        function = getattr(k, name)
        function.restype, function.argtypes = result, arguments
    return k


def _parents(k):
    snapshot = k.CreateToolhelp32Snapshot(2, 0)
    if snapshot == C.c_void_p(-1).value:
        raise C.WinError(C.get_last_error())
    try:
        entry = _Entry()
        entry.size = C.sizeof(entry)
        values = {}
        okay = k.Process32FirstW(snapshot, C.byref(entry))
        while okay:
            values[entry.pid] = entry.parent
            okay = k.Process32NextW(snapshot, C.byref(entry))
        if C.get_last_error() != 18:
            raise C.WinError(C.get_last_error())
        return values
    finally:
        k.CloseHandle(snapshot)


def job_processes(handle):
    """Return stable live member identities, or raise instead of returning empty."""
    k = _process_api()
    pids = _job_pids(k, handle)
    parents = _parents(k)
    members = []
    for pid in pids:
        process = k.OpenProcess(0x1000 | 0x100000, False, pid)
        if not process:
            raise C.WinError(C.get_last_error())
        try:
            belongs = W.BOOL()
            if not k.IsProcessInJob(process, handle, C.byref(belongs)) or not belongs.value:
                raise OSError("resident Job membership changed during observation")
            path, size = C.create_unicode_buffer(32768), W.DWORD(32768)
            if not k.QueryFullProcessImageNameW(process, 0, path, C.byref(size)):
                raise C.WinError(C.get_last_error())
            members.append({"pid": pid, "start_token": _birth(k, process),
                "parent_pid": parents.get(pid), "image": path.value})
        finally:
            k.CloseHandle(process)
    if _job_pids(k, handle) != pids:
        raise OSError("resident Job membership changed during observation")
    return members


def observe_resident_writers(handle, *, runtime, executable, turn_terminal):
    if not turn_terminal:
        return {"state": "busy", "reason": "original turn is not terminal", "writers": []}
    try:
        members = job_processes(handle)
    except OSError as exc:
        return {"state": "unknown", "reason": str(exc), "writers": []}
    root = next((m for m in members if m["pid"] == runtime["pid"]
        and m["start_token"] == runtime["start_token"]), None)
    canonical = lambda value: os.path.normcase(os.path.abspath(value))
    if root is None or canonical(root["image"]) != canonical(executable):
        return {"state": "unknown", "reason": "original resident identity is not present", "writers": []}
    # Only direct, unique runtime helpers from the selected immutable install
    # can remain idle. Tools named similarly elsewhere are ordinary writers.
    helpers = {canonical(Path(executable).with_name("codex-code-mode-host.exe")),
        canonical(Path(os.environ["SystemRoot"]) / "System32/conhost.exe")}
    accepted_helpers, writers = set(), []
    for member in members:
        if member == root:
            continue
        image = canonical(member["image"])
        if (member["parent_pid"] == root["pid"] and member["start_token"] >= root["start_token"]
                and image in helpers and image not in accepted_helpers):
            accepted_helpers.add(image)
        else:
            writers.append(member)
    return {"state": "busy" if writers else "quiescent", "writers": writers,
        "runtime": runtime, "members": members}
