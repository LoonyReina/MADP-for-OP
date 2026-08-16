from __future__ import annotations

import gzip
import json
import os
import shutil
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping


ARTIFACT_DIR_NAMES = (
    "logs",
    "observability_payloads",
    "execute_worker_payloads",
    "resident_launch_payloads",
)

ROTATABLE_JSONL_NAMES = (
    "events.jsonl",
    "timeline.jsonl",
    "daemon_runtime_events.jsonl",
    "engine_admission_events.jsonl",
    "engine_pump_events.jsonl",
    "operator_plugin_events.jsonl",
    "session_recovery_events.jsonl",
    "supervisor_events.jsonl",
    "solver_trigger_bridge_events.jsonl",
    "solver_trigger_ack.jsonl",
    "tester_trigger_ack.jsonl",
    "solver_wakeups.jsonl",
    "tester_wakeups.jsonl",
    "execute_worker_events.jsonl",
    "execute_history.jsonl",
    "execute_failures.jsonl",
    "gitpartner_worktree_recoveries.jsonl",
    "solver_thread_replacements.jsonl",
    "tester_thread_replacements.jsonl",
    "native_relay_claim_events.jsonl",
    "native_relay_app_side_events.jsonl",
    "gp_diagnostic_events.jsonl",
    "runtime_maintenance_events.jsonl",
)


def run_runtime_maintenance(
    root: Path,
    policy: Mapping[str, object] | None = None,
    *,
    actor: str,
    force: bool = False,
    now: float | None = None,
) -> dict[str, object]:
    """Bound daemon-owned runtime storage without touching workflow archives."""
    policy = policy or {}
    state_dir = root / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    current_time = time.time() if now is None else float(now)
    interval_seconds = _positive_float(policy.get("runtime_maintenance_interval_seconds"), 300.0)
    retention_hours = _positive_float(policy.get("runtime_artifact_retention_hours"), 2.0)
    max_jsonl_bytes = _positive_int(policy.get("runtime_jsonl_max_bytes"), 32 * 1024 * 1024)
    keep_jsonl_lines = _positive_int(policy.get("runtime_jsonl_keep_lines"), 10_000)
    max_archives = _positive_int(policy.get("runtime_archive_max_files"), 8)
    low_disk_mb = _positive_int(policy.get("runtime_low_disk_threshold_mb"), 256)
    free_before = _disk_free_bytes(state_dir)
    state_path = state_dir / "runtime_maintenance.json"
    previous = _read_json(state_path)
    previous_time = _timestamp_seconds(previous.get("completed_at"))
    due = previous_time is None or current_time - previous_time >= interval_seconds
    low_disk = free_before < low_disk_mb * 1024 * 1024

    if not force and not due and not low_disk:
        return {
            "status": "skipped",
            "reason": "interval_not_due",
            "actor": actor,
            "free_bytes": free_before,
            "next_due_in_seconds": max(0.0, interval_seconds - (current_time - previous_time)),
        }

    lock_path = state_dir / "runtime_maintenance.lock"
    lock_fd = _acquire_lock(lock_path, current_time=current_time)
    if lock_fd is None:
        return {
            "status": "skipped",
            "reason": "maintenance_lock_busy",
            "actor": actor,
            "free_bytes": free_before,
        }

    started_at = _iso_from_seconds(current_time)
    deleted_files = 0
    deleted_bytes = 0
    rotations: list[dict[str, object]] = []
    errors: list[str] = []
    try:
        cutoff = current_time - retention_hours * 3600.0
        for directory_name in ARTIFACT_DIR_NAMES:
            directory = state_dir / directory_name
            count, size, prune_errors = _prune_old_files(directory, cutoff=cutoff)
            deleted_files += count
            deleted_bytes += size
            errors.extend(prune_errors)

        archive_dir = state_dir / "runtime_archives"
        for filename in ROTATABLE_JSONL_NAMES:
            path = state_dir / filename
            try:
                rotation = rotate_jsonl(
                    path,
                    archive_dir=archive_dir,
                    max_bytes=max_jsonl_bytes,
                    keep_lines=keep_jsonl_lines,
                    timestamp=current_time,
                )
            except OSError as exc:
                errors.append(f"rotate {filename}: {exc}")
                continue
            if rotation is not None:
                rotations.append(rotation)
                _prune_archives(archive_dir, stem=path.stem, max_files=max_archives)

        free_after = _disk_free_bytes(state_dir)
        record: dict[str, object] = {
            "status": "completed_with_errors" if errors else "completed",
            "actor": actor,
            "started_at": started_at,
            "completed_at": _iso_from_seconds(time.time()),
            "low_disk_triggered": low_disk,
            "low_disk_threshold_mb": low_disk_mb,
            "artifact_retention_hours": retention_hours,
            "deleted_files": deleted_files,
            "deleted_bytes": deleted_bytes,
            "rotations": rotations,
            "free_bytes_before": free_before,
            "free_bytes_after": free_after,
            "errors": errors,
        }
        _atomic_write_json(state_path, record)
        _append_bounded_history(
            state_dir / "runtime_maintenance_events.jsonl",
            record,
            max_bytes=max(1024 * 1024, max_jsonl_bytes // 4),
        )
        return record
    except BaseException as exc:
        failure = {
            "status": "failed",
            "actor": actor,
            "started_at": started_at,
            "completed_at": _iso_from_seconds(time.time()),
            "free_bytes_before": free_before,
            "error": repr(exc),
        }
        try:
            _atomic_write_json(state_path, failure)
        except OSError:
            pass
        return failure
    finally:
        try:
            os.close(lock_fd)
        finally:
            lock_path.unlink(missing_ok=True)


def rotate_jsonl(
    path: Path,
    *,
    archive_dir: Path,
    max_bytes: int,
    keep_lines: int,
    timestamp: float | None = None,
) -> dict[str, object] | None:
    if not path.exists():
        return None
    original_stat = path.stat()
    if original_stat.st_size <= max_bytes:
        return None

    target_tail_bytes = max(1, max_bytes // 2)
    tail: deque[tuple[int, int]] = deque()
    tail_bytes = 0
    offset = 0
    with path.open("rb") as source:
        for line in source:
            length = len(line)
            tail.append((offset, length))
            tail_bytes += length
            offset += length
            while len(tail) > 1 and (len(tail) > keep_lines or tail_bytes > target_tail_bytes):
                _, removed_length = tail.popleft()
                tail_bytes -= removed_length
    if not tail:
        return None
    tail_start = tail[0][0]
    if tail_start <= 0:
        return None

    archive_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.fromtimestamp(timestamp or time.time(), tz=timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    archive_path = archive_dir / f"{path.stem}_{stamp}.jsonl.gz"
    archive_temp = archive_path.with_suffix(archive_path.suffix + ".tmp")
    tail_temp = path.with_suffix(path.suffix + ".rotate.tmp")
    try:
        with path.open("rb") as source, gzip.open(archive_temp, "wb", compresslevel=6) as archive:
            _copy_limited(source, archive, tail_start)
        with path.open("rb") as source, tail_temp.open("wb") as target:
            source.seek(tail_start)
            shutil.copyfileobj(source, target, length=1024 * 1024)
            target.flush()
            os.fsync(target.fileno())

        current_stat = path.stat()
        if (
            current_stat.st_size != original_stat.st_size
            or current_stat.st_mtime_ns != original_stat.st_mtime_ns
        ):
            raise OSError("source changed during rotation; retry later")

        os.replace(archive_temp, archive_path)
        os.replace(tail_temp, path)
    finally:
        archive_temp.unlink(missing_ok=True)
        tail_temp.unlink(missing_ok=True)

    return {
        "file": path.name,
        "archive": archive_path.name,
        "bytes_before": original_stat.st_size,
        "bytes_archived": tail_start,
        "bytes_after": path.stat().st_size,
        "lines_kept": len(tail),
    }


def _prune_old_files(directory: Path, *, cutoff: float) -> tuple[int, int, list[str]]:
    if not directory.exists():
        return 0, 0, []
    count = 0
    size = 0
    errors: list[str] = []
    for path in directory.rglob("*"):
        if not path.is_file():
            continue
        try:
            stat = path.stat()
            if stat.st_mtime >= cutoff:
                continue
            path.unlink()
            count += 1
            size += stat.st_size
        except OSError as exc:
            errors.append(f"prune {path.name}: {exc}")
    for path in sorted(directory.rglob("*"), key=lambda item: len(item.parts), reverse=True):
        if not path.is_dir():
            continue
        try:
            path.rmdir()
        except OSError:
            pass
    return count, size, errors


def _prune_archives(archive_dir: Path, *, stem: str, max_files: int) -> None:
    matches = sorted(
        archive_dir.glob(f"{stem}_*.jsonl.gz"),
        key=lambda path: path.stat().st_mtime_ns,
        reverse=True,
    )
    for path in matches[max_files:]:
        path.unlink(missing_ok=True)


def _copy_limited(source: object, target: object, limit: int) -> None:
    remaining = limit
    while remaining > 0:
        chunk = source.read(min(1024 * 1024, remaining))
        if not chunk:
            break
        target.write(chunk)
        remaining -= len(chunk)


def _append_bounded_history(path: Path, record: Mapping[str, object], *, max_bytes: int) -> None:
    encoded = (json.dumps(dict(record), ensure_ascii=False) + "\n").encode("utf-8")
    if path.exists() and path.stat().st_size + len(encoded) > max_bytes:
        existing = path.read_bytes()
        tail = existing[-max(0, max_bytes // 2) :]
        newline = tail.find(b"\n")
        if newline >= 0:
            tail = tail[newline + 1 :]
        temp = path.with_suffix(path.suffix + ".tmp")
        temp.write_bytes(tail)
        os.replace(temp, path)
    with path.open("ab") as target:
        target.write(encoded)


def _acquire_lock(path: Path, *, current_time: float, stale_seconds: float = 600.0) -> int | None:
    try:
        return os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        try:
            if current_time - path.stat().st_mtime <= stale_seconds:
                return None
            path.unlink()
        except OSError:
            return None
        try:
            return os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            return None


def _atomic_write_json(path: Path, payload: Mapping[str, object]) -> None:
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temp, path)


def _read_json(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _timestamp_seconds(value: object) -> float | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def _iso_from_seconds(value: float) -> str:
    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()


def _disk_free_bytes(path: Path) -> int:
    try:
        return int(shutil.disk_usage(path).free)
    except OSError:
        return 0


def _positive_int(value: object, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _positive_float(value: object, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default
