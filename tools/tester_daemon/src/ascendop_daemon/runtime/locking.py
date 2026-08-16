from __future__ import annotations

import ctypes
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Callable

from ascendop_daemon.core.atomic_io import write_json_atomic
from ascendop_daemon.core.models import DaemonConfig, config_seasons, utc_now_iso


class DaemonLock:
    def __init__(self, root: Path, stale_after_seconds: int = 0) -> None:
        self.state_dir = root / "TestUtils" / "tester_daemon"
        self.lock_path = self.state_dir / "daemon.lock"
        self.heartbeat_path = self.state_dir / "daemon_heartbeat.json"
        self.stale_after_seconds = stale_after_seconds
        self.fd: int | None = None

    def __enter__(self) -> "DaemonLock":
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.acquire()
        return self

    def acquire(self) -> None:
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        try:
            self.fd = os.open(str(self.lock_path), flags)
        except FileExistsError as exc:
            if self.is_stale():
                try:
                    self.lock_path.unlink(missing_ok=True)
                except PermissionError as perm_exc:
                    raise RuntimeError(f"daemon lock exists and cannot be replaced: {self.lock_path}") from perm_exc
                self.fd = os.open(str(self.lock_path), flags)
            else:
                raise RuntimeError(f"daemon lock exists: {self.lock_path}") from exc
        os.write(self.fd, f"pid={os.getpid()} acquired_at={utc_now_iso()}\n".encode("utf-8"))

    def is_stale(self) -> bool:
        if not self.lock_path.exists():
            return False
        metadata = read_lock_metadata(self.lock_path)
        if metadata.get("released_at"):
            return True
        pid = read_lock_pid(self.lock_path)
        if pid > 0 and lock_owner_alive(self.lock_path):
            return False
        if pid > 0:
            return True
        if self.stale_after_seconds <= 0:
            return False
        try:
            age_seconds = time.time() - self.lock_path.stat().st_mtime
        except OSError:
            return False
        return age_seconds >= self.stale_after_seconds

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
        try:
            self.lock_path.unlink()
        except FileNotFoundError:
            pass

    def write_heartbeat(self, config: DaemonConfig, mode: str) -> None:
        payload = _read_json_object(self.heartbeat_path)
        payload.update({
            "time": utc_now_iso(),
            "pid": os.getpid(),
            "mode": mode,
            "season": config.season,
            "seasons": list(config_seasons(config)),
            "transport": config.transport,
            "operators": list(config.operators),
        })
        write_json_atomic(self.heartbeat_path, payload)


class NamedProcessLock:
    def __init__(
        self,
        root: Path,
        name: str,
        stale_after_seconds: int = 0,
        wait_timeout_seconds: float = 0,
        poll_interval_seconds: float = 0.1,
        on_wait: Callable[[float], None] | None = None,
        wait_notification_interval_seconds: float = 1.0,
    ) -> None:
        self.state_dir = root / "TestUtils" / "tester_daemon"
        self.lock_path = self.state_dir / f"{name}.lock"
        self.waiters_dir = self.state_dir / f"{name}.waiters"
        self.stale_after_seconds = stale_after_seconds
        self.wait_timeout_seconds = max(0.0, float(wait_timeout_seconds))
        self.poll_interval_seconds = max(0.01, float(poll_interval_seconds))
        self.on_wait = on_wait
        self.wait_notification_interval_seconds = max(
            self.poll_interval_seconds,
            float(wait_notification_interval_seconds),
        )
        self.fd: int | None = None
        self.ticket_path: Path | None = None

    def __enter__(self) -> "NamedProcessLock":
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.acquire()
        return self

    def acquire(self) -> None:
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        started_at = time.monotonic()
        deadline = started_at + self.wait_timeout_seconds
        last_wait_notification = float("-inf")
        last_lock_error: FileExistsError | None = None
        self._cleanup_stale_waiters()

        # Preserve the uncontended fast path. Once any waiter exists, every
        # later contender joins behind it so a fast releasing process cannot
        # repeatedly reacquire and starve an engine transport request.
        if not self._ordered_waiters():
            while True:
                try:
                    self.fd = os.open(str(self.lock_path), flags)
                    self._write_owner_metadata()
                    return
                except FileExistsError as exc:
                    last_lock_error = exc
                    if not self.is_stale():
                        break
                    try:
                        self.lock_path.unlink(missing_ok=True)
                    except PermissionError as perm_exc:
                        raise RuntimeError(
                            f"process lock exists and cannot be replaced: {self.lock_path}"
                        ) from perm_exc
                    self._cleanup_stale_waiters()
                    if self._ordered_waiters():
                        break

        if self.wait_timeout_seconds <= 0:
            raise RuntimeError(f"process lock exists: {self.lock_path}") from last_lock_error

        self._register_waiter(deadline)
        if self.on_wait is not None:
            self.on_wait(0.0)
            last_wait_notification = 0.0
        try:
            while True:
                now = time.monotonic()
                if now >= deadline:
                    elapsed = max(0.0, now - started_at)
                    if self.on_wait is not None and elapsed > last_wait_notification:
                        self.on_wait(elapsed)
                    raise RuntimeError(
                        f"process lock exists: {self.lock_path}"
                    ) from last_lock_error

                self._cleanup_stale_waiters()
                if self._is_head_waiter():
                    try:
                        self.fd = os.open(str(self.lock_path), flags)
                    except FileExistsError as exc:
                        last_lock_error = exc
                        if self.is_stale():
                            try:
                                self.lock_path.unlink(missing_ok=True)
                            except PermissionError as perm_exc:
                                raise RuntimeError(
                                    "process lock exists and cannot be replaced: "
                                    f"{self.lock_path}"
                                ) from perm_exc
                            continue
                    else:
                        self._remove_waiter_ticket()
                        self._write_owner_metadata()
                        return

                elapsed = max(0.0, now - started_at)
                if self.on_wait is not None and (
                    elapsed - last_wait_notification
                    >= self.wait_notification_interval_seconds
                ):
                    self.on_wait(elapsed)
                    last_wait_notification = elapsed
                time.sleep(self.poll_interval_seconds)
        finally:
            if self.fd is None:
                self._remove_waiter_ticket()

    def _write_owner_metadata(self) -> None:
        if self.fd is None:
            raise RuntimeError("process lock file descriptor is unavailable")
        try:
            started_at_epoch = process_started_at_epoch(os.getpid())
            started_at_field = (
                f" process_started_at_epoch={started_at_epoch:.6f}"
                if started_at_epoch is not None
                else ""
            )
            os.write(
                self.fd,
                (
                    f"pid={os.getpid()} acquired_at={utc_now_iso()}"
                    f"{started_at_field}\n"
                ).encode("utf-8"),
            )
        except BaseException:
            os.close(self.fd)
            self.fd = None
            self.lock_path.unlink(missing_ok=True)
            raise

    def _register_waiter(self, deadline: float) -> None:
        self.waiters_dir.mkdir(parents=True, exist_ok=True)
        remaining_seconds = max(0.0, deadline - time.monotonic())
        expires_at_epoch = time.time() + remaining_seconds + max(
            5.0,
            self.poll_interval_seconds * 5.0,
        )
        for sequence in range(100):
            ticket_name = (
                f"{time.monotonic_ns():020d}_{os.getpid():010d}_"
                f"{id(self):x}_{sequence:02d}.waiter"
            )
            ticket_path = self.waiters_dir / ticket_name
            try:
                fd = os.open(
                    str(ticket_path),
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                )
            except FileExistsError:
                continue
            try:
                payload = {
                    "pid": os.getpid(),
                    "created_at": utc_now_iso(),
                    "expires_at_epoch": expires_at_epoch,
                }
                os.write(
                    fd,
                    (json.dumps(payload, sort_keys=True) + "\n").encode("utf-8"),
                )
            finally:
                os.close(fd)
            self.ticket_path = ticket_path
            return
        raise RuntimeError(f"cannot register process lock waiter: {self.lock_path}")

    def _ordered_waiters(self) -> list[Path]:
        try:
            return sorted(self.waiters_dir.glob("*.waiter"), key=lambda path: path.name)
        except OSError:
            return []

    def _is_head_waiter(self) -> bool:
        if self.ticket_path is None or not self.ticket_path.exists():
            return False
        waiters = self._ordered_waiters()
        return bool(waiters) and waiters[0] == self.ticket_path

    def _cleanup_stale_waiters(self) -> None:
        now_epoch = time.time()
        for ticket_path in self._ordered_waiters():
            payload = _read_json_object(ticket_path)
            try:
                pid = int(payload.get("pid", 0))
            except (TypeError, ValueError):
                pid = 0
            try:
                expires_at_epoch = float(payload.get("expires_at_epoch", 0.0))
            except (TypeError, ValueError):
                expires_at_epoch = 0.0
            remove = pid > 0 and not process_alive(pid)
            if (
                not remove
                and pid > 0
                and process_started_after_timestamp(
                    pid,
                    str(payload.get("created_at") or ""),
                )
            ):
                remove = True
            remove = remove or (
                expires_at_epoch > 0.0 and now_epoch >= expires_at_epoch
            )
            if pid <= 0 or expires_at_epoch <= 0.0:
                try:
                    age_seconds = now_epoch - ticket_path.stat().st_mtime
                except OSError:
                    continue
                remove = remove or age_seconds >= 5.0
            if remove:
                ticket_path.unlink(missing_ok=True)

    def _remove_waiter_ticket(self) -> None:
        if self.ticket_path is None:
            return
        self.ticket_path.unlink(missing_ok=True)
        self.ticket_path = None

    def is_stale(self) -> bool:
        if not self.lock_path.exists():
            return False
        metadata = read_lock_metadata(self.lock_path)
        if metadata.get("released_at"):
            return True
        pid = read_lock_pid(self.lock_path)
        if pid > 0 and lock_owner_alive(self.lock_path):
            return False
        if pid > 0:
            return True
        if self.stale_after_seconds <= 0:
            return False
        try:
            age_seconds = time.time() - self.lock_path.stat().st_mtime
        except OSError:
            return False
        return age_seconds >= self.stale_after_seconds

    def __exit__(self, exc_type, exc, tb) -> None:
        self._remove_waiter_ticket()
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
        self._release_lock_path()

    def _release_lock_path(self) -> None:
        for attempt in range(40):
            try:
                self.lock_path.unlink()
                return
            except FileNotFoundError:
                return
            except PermissionError:
                if attempt == 39:
                    break
                time.sleep(0.025)

        # Windows scanners can briefly deny deletion after the descriptor
        # closes. A release marker prevents a live PID from pinning an orphan
        # sentinel forever.
        try:
            self.lock_path.write_text(
                f"pid=0 released_at={utc_now_iso()}\n",
                encoding="utf-8",
            )
        except OSError as exc:
            raise RuntimeError(
                f"process lock cannot be released: {self.lock_path}"
            ) from exc


def _read_json_object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def read_lock_pid(path: Path) -> int:
    metadata = read_lock_metadata(path)
    try:
        return int(metadata.get("pid", "0"))
    except ValueError:
        return 0


def read_lock_metadata(path: Path) -> dict[str, str]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return {}
    metadata: dict[str, str] = {}
    for token in text.replace("\n", " ").split():
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        metadata[key.strip()] = value.strip()
    return metadata


def lock_owner_alive(path: Path) -> bool:
    metadata = read_lock_metadata(path)
    try:
        pid = int(metadata.get("pid", "0"))
    except ValueError:
        return False
    if pid <= 0 or not process_alive(pid):
        return False
    current_started_at = process_started_at_epoch(pid)
    if current_started_at is None:
        return True
    try:
        recorded_started_at = float(
            metadata.get("process_started_at_epoch", "")
        )
    except ValueError:
        recorded_started_at = 0.0
    if (
        recorded_started_at > 0.0
        and abs(current_started_at - recorded_started_at) > 2.0
    ):
        return False
    return not process_started_after_timestamp(
        pid,
        metadata.get("acquired_at", ""),
        process_start_epoch=current_started_at,
    )


def process_started_after_timestamp(
    pid: int,
    timestamp: str,
    *,
    process_start_epoch: float | None = None,
) -> bool:
    if not timestamp:
        return False
    started_at = (
        process_start_epoch
        if process_start_epoch is not None
        else process_started_at_epoch(pid)
    )
    if started_at is None:
        return False
    try:
        observed_at = datetime.fromisoformat(
            timestamp.replace("Z", "+00:00")
        ).timestamp()
    except ValueError:
        return False
    return started_at > observed_at + 2.0


def process_started_at_epoch(pid: int) -> float | None:
    if pid <= 0:
        return None
    if os.name == "nt":
        return _windows_process_started_at_epoch(pid)
    stat_path = Path(f"/proc/{pid}/stat")
    try:
        stat_text = stat_path.read_text(encoding="utf-8")
        closing_paren = stat_text.rfind(")")
        fields = stat_text[closing_paren + 2 :].split()
        start_ticks = int(fields[19])
        clock_ticks = int(os.sysconf("SC_CLK_TCK"))
        boot_time = 0
        for line in Path("/proc/stat").read_text(
            encoding="utf-8"
        ).splitlines():
            if line.startswith("btime "):
                boot_time = int(line.split()[1])
                break
        if boot_time <= 0 or clock_ticks <= 0:
            return None
        return float(boot_time) + (float(start_ticks) / float(clock_ticks))
    except (OSError, ValueError, IndexError):
        return None


def _windows_process_started_at_epoch(pid: int) -> float | None:
    class FileTime(ctypes.Structure):
        _fields_ = [
            ("low", ctypes.c_ulong),
            ("high", ctypes.c_ulong),
        ]

    try:
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        handle = kernel32.OpenProcess(0x1000, False, pid)
        if not handle:
            return None
        try:
            creation = FileTime()
            exit_time = FileTime()
            kernel_time = FileTime()
            user_time = FileTime()
            if not kernel32.GetProcessTimes(
                handle,
                ctypes.byref(creation),
                ctypes.byref(exit_time),
                ctypes.byref(kernel_time),
                ctypes.byref(user_time),
            ):
                return None
            ticks = (int(creation.high) << 32) | int(creation.low)
            return (float(ticks) / 10_000_000.0) - 11_644_473_600.0
        finally:
            kernel32.CloseHandle(handle)
    except (AttributeError, OSError, TypeError, ValueError):
        return None


def process_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        if os.name == "nt":
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            handle = kernel32.OpenProcess(0x1000, False, pid)
            if not handle:
                # Access denied still means the process exists; treating it as
                # dead lets supervisors remove live locks and start duplicates.
                return ctypes.get_last_error() == 5
            try:
                exit_code = ctypes.c_ulong()
                if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                    return False
                return exit_code.value == 259
            finally:
                kernel32.CloseHandle(handle)
        os.kill(pid, 0)
        return True
    except OSError:
        return False
