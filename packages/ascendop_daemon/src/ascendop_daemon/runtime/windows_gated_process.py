"""Windows carrier primitive: no application code before durable admission.

The initial thread is suspended and atomically assigned to a kill-on-close job.
Until detach() the parent's death kills even an unrecorded child. The caller
must commit PID/birth/thread identity and permission BEFORE detach()/resume().
After detach, recovery resumes that same thread, never creates another process.
No queue, Agent policy, content hashing, or subprocess supervisor lives here.
"""
from __future__ import annotations

import ctypes as C
from contextlib import contextmanager
from ctypes import wintypes as W
import os
from pathlib import Path
import subprocess
import uuid


class _Startup(C.Structure):
    _fields_ = [("cb", W.DWORD), ("reserved", W.LPWSTR), ("desktop", W.LPWSTR),
        ("title", W.LPWSTR), ("x", W.DWORD), ("y", W.DWORD), ("xs", W.DWORD),
        ("ys", W.DWORD), ("xc", W.DWORD), ("yc", W.DWORD), ("fill", W.DWORD),
        ("flags", W.DWORD), ("show", W.WORD), ("reserved_size", W.WORD),
        ("reserved_bytes", C.c_void_p), ("stdin", W.HANDLE), ("stdout", W.HANDLE),
        ("stderr", W.HANDLE)]


class _StartupEx(C.Structure):
    _fields_ = [("startup", _Startup), ("attributes", C.c_void_p)]


class _ProcessInfo(C.Structure):
    _fields_ = [("process", W.HANDLE), ("thread", W.HANDLE), ("pid", W.DWORD), ("tid", W.DWORD)]


class _JobBasic(C.Structure):
    _fields_ = [("process_time", C.c_int64), ("job_time", C.c_int64), ("flags", W.DWORD),
        ("min_ws", C.c_size_t), ("max_ws", C.c_size_t), ("process_limit", W.DWORD),
        ("affinity", C.c_size_t), ("priority", W.DWORD), ("scheduling", W.DWORD)]


class _JobLimits(C.Structure):
    _fields_ = [("basic", _JobBasic), ("io", C.c_uint64 * 6),
        ("process_memory", C.c_size_t), ("job_memory", C.c_size_t),
        ("peak_process", C.c_size_t), ("peak_job", C.c_size_t)]


class _JobAccounting(C.Structure):
    _fields_ = [("user_time", C.c_int64), ("kernel_time", C.c_int64),
        ("period_user_time", C.c_int64), ("period_kernel_time", C.c_int64),
        ("page_faults", W.DWORD), ("total_processes", W.DWORD),
        ("active_processes", W.DWORD), ("terminated_processes", W.DWORD)]


def _api():
    if os.name != "nt":
        raise NotImplementedError("gated native launch requires the Windows carrier")
    k = C.WinDLL("kernel32", use_last_error=True)
    signatures = {
        "CreateJobObjectW": (W.HANDLE, [C.c_void_p, W.LPCWSTR]),
        "OpenJobObjectW": (W.HANDLE, [W.DWORD, W.BOOL, W.LPCWSTR]),
        "QueryInformationJobObject": (W.BOOL, [W.HANDLE, C.c_int, C.c_void_p, W.DWORD, C.c_void_p]),
        "SetInformationJobObject": (W.BOOL, [W.HANDLE, C.c_int, C.c_void_p, W.DWORD]),
        "InitializeProcThreadAttributeList": (W.BOOL, [C.c_void_p, W.DWORD, W.DWORD, C.POINTER(C.c_size_t)]),
        "UpdateProcThreadAttribute": (W.BOOL, [C.c_void_p, W.DWORD, C.c_size_t, C.c_void_p, C.c_size_t, C.c_void_p, C.c_void_p]),
        "DeleteProcThreadAttributeList": (None, [C.c_void_p]),
        "CreateProcessW": (W.BOOL, [W.LPCWSTR, W.LPWSTR, C.c_void_p, C.c_void_p,
            W.BOOL, W.DWORD, C.c_void_p, W.LPCWSTR, C.POINTER(_StartupEx), C.POINTER(_ProcessInfo)]),
        "GetCurrentProcess": (W.HANDLE, []),
        "DuplicateHandle": (W.BOOL, [W.HANDLE, W.HANDLE, W.HANDLE, C.POINTER(W.HANDLE), W.DWORD, W.BOOL, W.DWORD]),
        "CloseHandle": (W.BOOL, [W.HANDLE]),
        "ResumeThread": (W.DWORD, [W.HANDLE]),
        "OpenThread": (W.HANDLE, [W.DWORD, W.BOOL, W.DWORD]),
        "OpenProcess": (W.HANDLE, [W.DWORD, W.BOOL, W.DWORD]),
        "GetProcessIdOfThread": (W.DWORD, [W.HANDLE]),
        "GetProcessTimes": (W.BOOL, [W.HANDLE] + [C.POINTER(W.FILETIME)] * 4),
        "WaitForSingleObject": (W.DWORD, [W.HANDLE, W.DWORD]),
        "GetExitCodeProcess": (W.BOOL, [W.HANDLE, C.POINTER(W.DWORD)]),
        "TerminateProcess": (W.BOOL, [W.HANDLE, W.UINT]),
    }
    for name, (result, args) in signatures.items():
        fn = getattr(k, name)
        fn.restype, fn.argtypes = result, args
    return k


def _check(value):
    if not value:
        raise C.WinError(C.get_last_error())
    return value


def _birth(k, handle):
    times = [W.FILETIME() for _ in range(4)]
    _check(k.GetProcessTimes(handle, *(C.byref(t) for t in times)))
    return f"win-{times[0].dwHighDateTime:08x}{times[0].dwLowDateTime:08x}"


def _resume(k, handle):
    previous = k.ResumeThread(handle)
    if previous == 0xFFFFFFFF:
        raise C.WinError(C.get_last_error())
    if previous > 1:
        raise RuntimeError("native initial thread has an unexpected external suspension")


class GatedProcess:
    def __init__(self, k, info, job, job_name):
        token = _birth(k, info.process)
        self._k, self._info, self._job = k, info, job
        self.pid, self.thread_id = info.pid, info.tid
        self.start_token = token
        self.job_name = job_name
        self._detached = False

    def detach(self, *, keep_kill_on_close=False):
        """Only after durable permission: survive the launching relay's death."""
        if self._job and not self._detached:
            if not keep_kill_on_close:
                limits = _JobLimits()
                _check(self._k.SetInformationJobObject(self._job, 9, C.byref(limits), C.sizeof(limits)))
            # Keep a queryable name while this carrier is alive. Closing the
            # last handle removes the name even when a child is still active.
            self._detached = True

    def resume(self):
        if not self._detached:
            raise ValueError("detach only after durable admission before native resume")
        _resume(self._k, self._info.thread)

    @contextmanager
    def inherited_query_handle(self):
        """Give only the trusted keeper QUERY access, never job mutation rights."""
        handle = W.HANDLE()
        current = self._k.GetCurrentProcess()
        _check(self._k.DuplicateHandle(current, self._job, current, C.byref(handle), 0x4, True, 0))
        try:
            yield handle.value
        finally:
            self._k.CloseHandle(handle)

    def poll(self):
        state = self._k.WaitForSingleObject(self._info.process, 0)
        if state == 258:  # WAIT_TIMEOUT: exit code 259 is also a legal final code.
            return None
        if state != 0:
            raise C.WinError(C.get_last_error())
        result = W.DWORD()
        _check(self._k.GetExitCodeProcess(self._info.process, C.byref(result)))
        return result.value

    def terminate(self):
        _check(self._k.TerminateProcess(self._info.process, 1))

    def close(self):
        if self._job:
            self._k.CloseHandle(self._job)
            self._job = None
        for field in ("thread", "process"):
            handle = getattr(self._info, field)
            if handle:
                self._k.CloseHandle(handle)
                setattr(self._info, field, None)

    def __del__(self):
        if hasattr(self, "_k"):
            self.close()


def start_suspended(command: list[str], *, cwd: Path, environment: dict[str, str],
        stdin_path: Path | None = None, stdout_path: Path | None = None, stderr_path: Path | None = None,
        inherited_handles: tuple[int, ...] = (), stdio_handles: tuple[int, int, int] | None = None) -> GatedProcess:
    if stdio_handles is not None:
        if len(stdio_handles) != 3 or any(path is not None for path in (stdin_path, stdout_path, stderr_path)):
            raise ValueError("provide exactly one native stdio source")
    elif any(path is None for path in (stdin_path, stdout_path, stderr_path)):
        raise ValueError("native stdio paths are required")
    k = _api()
    import msvcrt
    # The name is an execution identity, not a content digest. Default
    # job limits forbid child breakaway; detach removes ONLY kill-on-close.
    job_name = "Local\\AscendOP-native-" + uuid.uuid4().hex
    C.set_last_error(0)
    job = _check(k.CreateJobObjectW(None, job_name))
    if C.get_last_error() == 183:  # Never adopt an unrelated named object.
        k.CloseHandle(job)
        raise ValueError("native job identity already exists")
    attrs = None
    handles = []
    info = _ProcessInfo()
    try:
        limits = _JobLimits()
        limits.basic.flags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        _check(k.SetInformationJobObject(job, 9, C.byref(limits), C.sizeof(limits)))
        size = C.c_size_t()
        k.InitializeProcThreadAttributeList(None, 2, 0, C.byref(size))
        buffer = C.create_string_buffer(size.value)
        _check(k.InitializeProcThreadAttributeList(buffer, 2, 0, C.byref(size)))
        attrs = buffer
        current = k.GetCurrentProcess()
        if stdio_handles is not None:
            for original_handle in stdio_handles:
                handle = W.HANDLE()
                _check(k.DuplicateHandle(current, original_handle, current,
                    C.byref(handle), 0, True, 2))  # DUPLICATE_SAME_ACCESS
                handles.append(handle.value)
        else:
            for path, mode in ((stdin_path, "rb"), (stdout_path, "ab"), (stderr_path, "ab")):
                with path.open(mode) as stream:
                    handle = W.HANDLE()
                    _check(k.DuplicateHandle(current, msvcrt.get_osfhandle(stream.fileno()), current,
                        C.byref(handle), 0, True, 2))
                    handles.append(handle.value)
        inherited = (W.HANDLE * (3 + len(inherited_handles)))(*handles, *inherited_handles)
        jobs = (W.HANDLE * 1)(job)
        _check(k.UpdateProcThreadAttribute(attrs, 0, 0x20002, inherited, C.sizeof(inherited), None, None))
        _check(k.UpdateProcThreadAttribute(attrs, 0, 0x2000D, jobs, C.sizeof(jobs), None, None))
        startup = _StartupEx()
        startup.startup.cb = C.sizeof(startup)
        startup.startup.flags = 0x101  # USESTDHANDLES | USESHOWWINDOW (hidden)
        startup.startup.stdin, startup.startup.stdout, startup.startup.stderr = handles
        startup.attributes = C.cast(attrs, C.c_void_p)
        env = C.create_unicode_buffer("\0".join(f"{key}={value}" for key, value in sorted(environment.items(), key=lambda pair: pair[0].upper())) + "\0\0")
        command_line = C.create_unicode_buffer(subprocess.list2cmdline(command))
        flags = 0x4 | 0x400 | 0x80000 | 0x08000000  # SUSPENDED | UNICODE | EXTENDED | NO_WINDOW
        _check(k.CreateProcessW(command[0], command_line, None, None, True, flags,
            env, str(cwd), C.byref(startup), C.byref(info)))
        result = GatedProcess(k, info, job, job_name)
        job = None
        return result
    except BaseException:
        for handle in (info.thread, info.process):
            if handle:
                k.CloseHandle(handle)
        raise
    finally:
        if attrs is not None:
            k.DeleteProcThreadAttributeList(attrs)
        for handle in handles:
            k.CloseHandle(handle)
        if job:
            k.CloseHandle(job)


def observe_job(job_name: str) -> str:
    """Observe this admitted tree only: alive/exited/unknown, without a scan.

    Object lifetime is not namespace lifetime: after the final handle closes,
    a live orphan child may still exist even though OpenJobObject says missing.
    Missing/access-denied therefore remain unknown, never proof of quiescence.
    Callers must not accept a model-supplied name.
    """
    prefix = "Local\\AscendOP-native-"
    if (not isinstance(job_name, str) or not job_name.startswith(prefix)
            or len(job_name) != len(prefix) + 32
            or any(char not in "0123456789abcdef" for char in job_name[len(prefix):])):
        raise ValueError("invalid admitted native job name")
    k = _api()
    handle = k.OpenJobObjectW(0x4, False, job_name)  # JOB_OBJECT_QUERY
    if not handle:
        return "unknown"
    try:
        return observe_job_handle(handle)
    finally:
        k.CloseHandle(handle)


def observe_job_handle(handle: int) -> str:
    """For a caller-owned original handle, not a model-supplied object name."""
    accounting = _JobAccounting()
    if not _api().QueryInformationJobObject(handle, 1, C.byref(accounting), C.sizeof(accounting), None):
        return "unknown"
    return "alive" if accounting.active_processes else "exited"


def resume_identified(*, pid: int, start_token: str, thread_id: int):
    """Idempotent release of the original initial thread, never PID-only."""
    k = _api()
    process = _check(k.OpenProcess(0x1000 | 0x100000, False, pid))
    thread = None
    try:
        if _birth(k, process) != start_token or k.WaitForSingleObject(process, 0) != 258:
            raise ValueError("native launch process identity has exited or changed")
        thread = _check(k.OpenThread(0x2 | 0x800, False, thread_id))
        if k.GetProcessIdOfThread(thread) != pid:
            raise ValueError("native initial thread belongs to another process")
        _resume(k, thread)
    finally:
        if thread:
            k.CloseHandle(thread)
        k.CloseHandle(process)
