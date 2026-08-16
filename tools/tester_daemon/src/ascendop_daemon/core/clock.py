from __future__ import annotations

import hashlib
import os
import time
from pathlib import Path


def host_id() -> str:
    return os.environ.get("COMPUTERNAME") or os.environ.get("HOSTNAME") or "unknown-host"


def boot_id() -> str:
    linux_boot_id = Path("/proc/sys/kernel/random/boot_id")
    if linux_boot_id.exists():
        value = linux_boot_id.read_text(encoding="ascii", errors="ignore").strip()
        if value:
            return value
    uptime_seconds = _windows_uptime_seconds()
    boot_epoch_bucket = int((time.time() - uptime_seconds) // 10)
    value = f"{host_id()}:{boot_epoch_bucket}"
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]


def _windows_uptime_seconds() -> float:
    if os.name != "nt":
        return time.monotonic()
    import ctypes

    return float(ctypes.windll.kernel32.GetTickCount64()) / 1000.0
