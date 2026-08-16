from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ascendop_daemon.runtime.locking import process_alive
from ascendop_daemon.core.models import utc_now_iso

CONFIRMED_IDE_VISIBILITIES = {
    "confirmed_by_native_relay",
    "live_proxy",
    "live_ws",
}
CONFIRMED_IDE_DELIVERIES = {"ide-native-relay", "codex-app-send-message-to-thread"}
FAILED_STATUSES = {"interrupted", "failed", "cancelled"}
OWNER_LOSS_FAILURE_KINDS = {
    "codex_cli_process_dead_no_thread_progress",
    "local_app_server_process_exit",
    "local_delivery_worker_exit",
}

FAILED_OR_PENDING_DELIVERY_STATUSES = {
    "needs-native-delivery",
    "interrupted",
    "failed",
    "cancelled",
}
STALE_FAILURE_FIELDS = {
    "error",
    "delivery_required",
    "delivery_retry_after",
    "delivery_retry_reason",
    "failure_kind",
    "last_delivery_error",
    "latest_has_agent_output",
    "latest_user_only_turn",
    "remote_control_status",
    "remote_control_error",
    "cli_returncode",
    "cli_stdout_tail",
    "cli_stderr_tail",
}
STALE_TURN_OBSERVATION_FIELDS = {
    "turn_status",
    "wait_status",
    "method",
    "native_id",
    "native_status",
    "native_startedAt",
    "native_completedAt",
    "native_durationMs",
    "storage_visible",
    "native_visible",
    "latest_has_agent_output",
    "latest_user_only_turn",
    "last_observed_at",
    "last_observed_turn_id",
    "last_observed_turn_status",
    "last_observed_has_agent_output",
    "last_observed_user_only_turn",
    "watch_retry_after",
    "watch_retry_reason",
    "last_watch_error",
}
STALE_NATIVE_DELIVERY_PROOF_FIELDS = STALE_TURN_OBSERVATION_FIELDS | {
    "turn_id",
    "delivery_reconciliation",
    "delivery_reconciled_at",
    "ide_panel_visible",
    "ide_panel_visibility",
}
NO_AGENT_OUTPUT_RETRY_COUNT_FIELD = "native_no_agent_output_count"
NO_AGENT_OUTPUT_TURN_IDS_FIELD = "native_no_agent_output_turn_ids"


def is_transient_native_poll_failure(record: dict[str, Any]) -> bool:
    """Return True for app-server poll glitches that conflict with native IDE delivery."""
    if str(record.get("delivery_reconciliation", "") or "") == "exact-native-turn-terminal":
        return False
    if str(record.get("failure_kind", "") or "") == "codex_app_restart_interrupted":
        return False
    if codex_cli_resume_process_active(record):
        return True
    if str(record.get("delivery_retry_reason", "") or "") == "remote_control_not_ready":
        return False
    status = str(record.get("status", "") or record.get("native_status", "") or "")
    if status not in FAILED_STATUSES:
        return False
    app_server_mode = str(record.get("app_server_mode", "") or "")
    error_text = " ".join(
        str(record.get(field, "") or "")
        for field in ("error", "last_delivery_error", "last_watch_error")
    ).lower()
    local_owner = (
        str(record.get("delivery", "") or "").startswith("app-server-")
        or str(record.get("remote_control_status", "") or "") == "storage-visible-local-owner"
    ) and (
        app_server_mode not in {"proxy", "ws", "thread-observation", "codex-cli"}
        or "app-server exited" in error_text
        or str(record.get("failure_kind", "") or "") == "local_app_server_process_exit"
    )
    if local_owner:
        return False
    delivery = str(record.get("delivery", "") or "")
    visibility = str(record.get("ide_panel_visibility", "") or "")
    confirmed = (
        record.get("ide_panel_visible") is True
        or visibility in CONFIRMED_IDE_VISIBILITIES
    ) and (
        delivery in CONFIRMED_IDE_DELIVERIES or visibility in CONFIRMED_IDE_VISIBILITIES
    )
    if not confirmed:
        return False
    completed_at = record.get("native_completedAt", record.get("latest_completed_at"))
    duration_ms = record.get("native_durationMs", record.get("latest_duration_ms"))
    proxy_fallback = (
        str(record.get("watch_retry_reason", "") or "") == "proxy_unavailable_observation_fallback"
        or "app-server watch returned interrupted" in str(record.get("error", "") or "")
        or "app-server watch returned interrupted" in str(record.get("last_watch_error", "") or "")
    )
    if proxy_fallback and completed_at in (None, "") and duration_ms in (None, ""):
        return True
    if str(record.get("thread_status_type", "") or "") == "idle":
        age_seconds = observed_failure_age_seconds(record)
        if age_seconds is not None and age_seconds >= 10:
            return False
    if completed_at not in (None, "") or duration_ms not in (None, ""):
        return False
    error = str(record.get("error", "") or "")
    if "Not initialized" in error:
        return True
    return str(record.get("app_server_mode", "") or "") == "thread-observation"


def codex_cli_resume_process_active(record: dict[str, Any]) -> bool:
    """Return True while a CLI resume fallback still owns the visible turn."""
    if str(record.get("delivery", "") or "") != "codex-cli-exec-resume":
        return False
    if "cli_returncode" in record:
        return False
    try:
        pid = int(record.get("cli_pid", 0) or 0)
    except (TypeError, ValueError):
        return False
    return pid > 0 and process_alive(pid)


def codex_cli_resume_process_dead(record: dict[str, Any]) -> bool:
    """Return True when a CLI resume fallback can no longer make progress."""
    if str(record.get("delivery", "") or "") != "codex-cli-exec-resume":
        return False
    try:
        pid = int(record.get("cli_pid", 0) or 0)
    except (TypeError, ValueError):
        return False
    if pid <= 0:
        return False
    if "cli_returncode" in record:
        return True
    return not process_alive(pid)


def delivery_reconciliation_pending(record: object) -> bool:
    """Return True while an ownerless native turn has not been reconciled.

    A retry timer alone cannot prove whether the original prompt reached the
    configured Codex task. Keep the gate fenced until an exact native turn or
    board outcome resolves that ambiguity.
    """
    if not isinstance(record, dict) or record.get("completion_unconfirmed") is not True:
        return False
    failure_kind = str(record.get("failure_kind", "") or "")
    return (
        failure_kind in OWNER_LOSS_FAILURE_KINDS
        or record.get("orphaned_delivery_owner") is True
        or record.get("control_plane_unavailable") is True
    )


def observed_failure_age_seconds(record: dict[str, Any]) -> int | None:
    observed_time = parse_record_time(str(record.get("last_observed_at", "") or ""))
    if observed_time is None:
        observed_time = parse_record_time(str(record.get("updated_at", "") or ""))
    if observed_time is None:
        return None
    return max(0, int((datetime.now(timezone.utc) - observed_time).total_seconds()))


def parse_record_time(text: str) -> datetime | None:
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def trigger_record_age_seconds(record: dict[str, Any], now: str = "") -> int | None:
    updated_at = parse_record_time(str(record.get("updated_at", "") or ""))
    if updated_at is None:
        return None
    current = parse_record_time(now) if now else datetime.now(timezone.utc)
    if current is None:
        current = datetime.now(timezone.utc)
    return max(0, int((current - updated_at).total_seconds()))


def completed_trigger_stale(
    record: dict[str, Any],
    stale_seconds: int,
    now: str = "",
) -> bool:
    """A completed turn is stale only while its exact board key is still actionable."""
    if normalized_trigger_status(record, str(record.get("status", "") or "")) != "completed":
        return False
    if stale_seconds <= 0:
        return False
    age_seconds = completed_trigger_age_seconds(record, now)
    return age_seconds is not None and age_seconds >= stale_seconds


def completed_trigger_age_seconds(record: dict[str, Any], now: str = "") -> int | None:
    completed_time: datetime | None = None
    for field in ("native_completedAt", "latest_completed_at", "completed_at"):
        value = record.get(field)
        if value in (None, ""):
            continue
        try:
            completed_time = datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (TypeError, ValueError, OSError):
            completed_time = parse_record_time(str(value))
        if completed_time is not None:
            break
    if completed_time is None:
        return trigger_record_age_seconds(record, now)
    current = parse_record_time(now) if now else datetime.now(timezone.utc)
    if current is None:
        current = datetime.now(timezone.utc)
    return max(0, int((current - completed_time).total_seconds()))


def normalized_trigger_status(record: dict[str, Any], default: str = "") -> str:
    status = str(record.get("status", "") or default)
    if is_transient_native_poll_failure(record):
        return "active"
    return status


def ack_solver_trigger(
    root: Path,
    key: str,
    thread_id: str,
    status: str = "sent",
    metadata: dict[str, Any] | None = None,
) -> Path:
    return ack_trigger_state(
        root,
        key,
        thread_id,
        status,
        metadata,
        event_name="solver_trigger_ack",
        jsonl_name="solver_trigger_ack.jsonl",
        state_name="solver_trigger_ack_state.json",
    )


def ack_tester_trigger(
    root: Path,
    key: str,
    thread_id: str,
    status: str = "sent",
    metadata: dict[str, Any] | None = None,
) -> Path:
    return ack_trigger_state(
        root,
        key,
        thread_id,
        status,
        metadata,
        event_name="tester_trigger_ack",
        jsonl_name="tester_trigger_ack.jsonl",
        state_name="tester_trigger_ack_state.json",
    )


def ack_trigger_state(
    root: Path,
    key: str,
    thread_id: str,
    status: str,
    metadata: dict[str, Any] | None,
    *,
    event_name: str,
    jsonl_name: str,
    state_name: str,
) -> Path:
    state_dir = root / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    now = utc_now_iso()
    record = {
        "time": now,
        "event": event_name,
        "key": key,
        "thread_id": thread_id,
        "status": status,
    }
    if metadata:
        record.update(metadata)
    path = state_dir / state_name
    with trigger_state_lock(state_dir, state_name):
        with (state_dir / jsonl_name).open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")

        state = read_named_trigger_ack_state(root, state_name)
        sent = dict(state.get("sent", {})) if isinstance(state.get("sent"), dict) else {}
        previous = sent.get(key)
        previous_record = dict(previous) if isinstance(previous, dict) else {}
        reset_native_turn_proof = bool(
            (metadata or {}).get("reset_native_turn_proof")
        )
        if reset_native_turn_proof:
            for field in STALE_NATIVE_DELIVERY_PROOF_FIELDS:
                previous_record.pop(field, None)
        incoming_turn_id = str((metadata or {}).get("turn_id") or (metadata or {}).get("native_id") or "")
        previous_turn_id = str(previous_record.get("turn_id") or previous_record.get("native_id") or "")
        if incoming_turn_id and previous_turn_id and incoming_turn_id != previous_turn_id:
            previous_record["previous_turn_id"] = previous_turn_id
            for field in STALE_TURN_OBSERVATION_FIELDS:
                previous_record.pop(field, None)
        if status == "needs-native-delivery" and is_app_side_visible_delivery_record(
            previous_record, key, thread_id
        ):
            next_record = {
                **previous_record,
                "key": key,
                "thread_id": thread_id,
                "updated_at": now,
                "last_native_delivery_required_at": now,
            }
            if metadata and metadata.get("error"):
                next_record["last_native_delivery_error"] = metadata.get("error")
            sent[key] = next_record
            write_json_atomic(path, {"updated_at": now, "sent": sent})
            return path
        if should_preserve_app_side_visible_ack(previous_record, key, thread_id, status, metadata):
            next_record = {
                **previous_record,
                "key": key,
                "thread_id": thread_id,
                "updated_at": now,
                "last_ignored_failure_status": status,
                "last_ignored_failure_at": now,
                "last_ignored_failure_reason": (
                    str(metadata.get("delivery_retry_reason", "") or "")
                    if metadata
                    else ""
                ),
            }
            if metadata:
                for source, target in (
                    ("error", "last_ignored_failure_error"),
                    ("last_delivery_error", "last_ignored_delivery_error"),
                    ("turn_status", "last_ignored_turn_status"),
                    ("native_status", "last_ignored_native_status"),
                    ("wait_status", "last_ignored_wait_status"),
                ):
                    if metadata.get(source) not in (None, ""):
                        next_record[target] = metadata.get(source)
                touch = {
                    key: metadata[key]
                    for key in ("last_observed_at", "last_observed_turn_id", "last_observed_turn_status")
                    if metadata.get(key) not in (None, "")
                }
                next_record.update(touch)
            sent[key] = next_record
            write_json_atomic(path, {"updated_at": now, "sent": sent})
            return path
        next_record = {
            **previous_record,
            "key": key,
            "thread_id": thread_id,
            "status": status,
            "updated_at": now,
        }
        if metadata:
            next_record.update(metadata)
        next_record.pop("reset_native_turn_proof", None)
        if (
            status in FAILED_STATUSES
            and metadata
            and metadata.get("failure_kind") == "native_turn_no_agent_output"
        ):
            raw_turn_ids = previous_record.get(NO_AGENT_OUTPUT_TURN_IDS_FIELD, [])
            turn_ids = [str(value) for value in raw_turn_ids if str(value)] if isinstance(raw_turn_ids, list) else []
            if not turn_ids and previous_record.get("failure_kind") == "native_turn_no_agent_output":
                previous_failed_turn = str(
                    previous_record.get("turn_id") or previous_record.get("native_id") or ""
                )
                if previous_failed_turn:
                    # Older records counted every observation poll. Seed from the
                    # one known failed turn so they self-repair instead of becoming
                    # permanently unrecoverable.
                    turn_ids.append(previous_failed_turn)
            if incoming_turn_id and incoming_turn_id not in turn_ids:
                turn_ids.append(incoming_turn_id)
            if turn_ids:
                next_record[NO_AGENT_OUTPUT_TURN_IDS_FIELD] = turn_ids
                next_record[NO_AGENT_OUTPUT_RETRY_COUNT_FIELD] = len(turn_ids)
            else:
                previous_count = int(previous_record.get(NO_AGENT_OUTPUT_RETRY_COUNT_FIELD, 0) or 0)
                next_record[NO_AGENT_OUTPUT_RETRY_COUNT_FIELD] = max(1, previous_count)
        if status == "needs-native-delivery":
            for field in (
                "delivery",
                "ide_panel_visible",
                "ide_panel_visibility",
                "turn_id",
                "turn_status",
                "wait_status",
                "method",
                "native_id",
                "native_status",
                "native_startedAt",
                "native_completedAt",
                "native_durationMs",
                "storage_visible",
                "native_visible",
                "failure_kind",
                "latest_has_agent_output",
                "latest_user_only_turn",
                NO_AGENT_OUTPUT_RETRY_COUNT_FIELD,
            ):
                next_record.pop(field, None)
            next_record["ide_panel_visible"] = bool(metadata.get("ide_panel_visible", False)) if metadata else False
            next_record["ide_panel_visibility"] = (
                str(metadata.get("ide_panel_visibility", "") or "native_delivery_required")
                if metadata
                else "native_delivery_required"
            )
        if status not in FAILED_OR_PENDING_DELIVERY_STATUSES:
            for field in STALE_FAILURE_FIELDS:
                if not metadata or field not in metadata:
                    next_record.pop(field, None)
            next_record.pop(NO_AGENT_OUTPUT_RETRY_COUNT_FIELD, None)
            if status not in {"sent", "delivered", "acked", "active"}:
                next_record.pop(NO_AGENT_OUTPUT_TURN_IDS_FIELD, None)
            if metadata:
                for field in ("latest_has_agent_output", "latest_user_only_turn"):
                    if field in metadata:
                        next_record[field] = metadata[field]
            visibility = str(next_record.get("ide_panel_visibility", "") or "")
            if (
                visibility in CONFIRMED_IDE_VISIBILITIES
                and str(next_record.get("turn_id") or next_record.get("native_id") or "")
                and bool(next_record.get("storage_visible") or next_record.get("native_visible"))
            ):
                next_record["ide_panel_visible"] = True
        previous_visibility = str(previous_record.get("ide_panel_visibility", "") or "")
        if (
            status not in FAILED_OR_PENDING_DELIVERY_STATUSES
            and previous_record.get("ide_panel_visible") is True
            and previous_visibility in CONFIRMED_IDE_VISIBILITIES
            and next_record.get("ide_panel_visible") is False
            and not (metadata and metadata.get("control_plane_unavailable") is True)
        ):
            next_record["ide_panel_visible"] = True
            next_record["ide_panel_visibility"] = previous_visibility
        sent[key] = next_record
        write_json_atomic(path, {"updated_at": now, "sent": sent})
        return path


def should_preserve_app_side_visible_ack(
    previous_record: dict[str, Any],
    key: str,
    thread_id: str,
    status: str,
    metadata: dict[str, Any] | None,
) -> bool:
    """Keep a live IDE-visible app-side turn from being downgraded by proxy glitches."""
    if status not in FAILED_STATUSES:
        return False
    if not metadata:
        return False
    if not is_app_side_visible_delivery_record(previous_record, key, thread_id):
        return False
    if str(previous_record.get("delivery", "") or "") != "codex-app-send-message-to-thread":
        return False
    previous_status = normalized_trigger_status(previous_record, str(previous_record.get("status", "") or ""))
    if previous_status not in {"sent", "delivered", "acked", "active", "completed"}:
        return False
    previous_turn_id = str(previous_record.get("turn_id") or previous_record.get("native_id") or "")
    incoming_turn_id = str(metadata.get("turn_id") or metadata.get("native_id") or "")
    if previous_turn_id and incoming_turn_id and previous_turn_id != incoming_turn_id:
        return False
    if metadata.get("native_completedAt") not in (None, ""):
        return False
    if metadata.get("native_durationMs") not in (None, ""):
        return False
    if metadata.get("latest_user_only_turn") is True:
        return False
    if str(metadata.get("failure_kind", "") or "") in {
        "native_turn_no_agent_output",
        "native_turn_stalled_no_activity",
        "codex_app_outage_stalled",
    }:
        return False
    retry_reason = str(metadata.get("delivery_retry_reason", "") or "")
    error_text = " ".join(
        str(metadata.get(field, "") or "")
        for field in ("error", "last_delivery_error", "last_watch_error")
    )
    method = str(metadata.get("method", "") or "")
    if retry_reason == "remote_control_not_ready":
        return True
    if method == "thread-observation-sync" and "app-server watch returned interrupted" in error_text:
        return True
    if method == "thread-observation-sync" and metadata.get("latest_has_agent_output") is True:
        return True
    return False


def is_app_side_visible_delivery_record(record: dict[str, Any], key: str, thread_id: str) -> bool:
    if not isinstance(record, dict):
        return False
    if str(record.get("key", "") or "") != key:
        return False
    if str(record.get("thread_id", "") or "") != thread_id:
        return False
    visibility = str(record.get("ide_panel_visibility", "") or "")
    if not bool(record.get("ide_panel_visible")) and visibility not in CONFIRMED_IDE_VISIBILITIES:
        return False
    if not str(record.get("turn_id") or record.get("native_id") or ""):
        return False
    status = normalized_trigger_status(record)
    if status not in {"sent", "delivered", "acked", "active", "completed"}:
        return False
    if status in FAILED_OR_PENDING_DELIVERY_STATUSES:
        return False
    delivery = str(record.get("delivery", "") or "")
    return delivery in CONFIRMED_IDE_DELIVERIES or visibility in CONFIRMED_IDE_VISIBILITIES


def read_trigger_ack_state(root: Path) -> dict[str, Any]:
    return read_named_trigger_ack_state(root, "solver_trigger_ack_state.json")


def read_tester_trigger_ack_state(root: Path) -> dict[str, Any]:
    return read_named_trigger_ack_state(root, "tester_trigger_ack_state.json")


def read_named_trigger_ack_state(root: Path, name: str) -> dict[str, Any]:
    path = root / "TestUtils" / "tester_daemon" / name
    if not path.exists():
        return {}
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return {}
    malformed = False
    try:
        exact = json.loads(text)
        data = exact if isinstance(exact, dict) else {}
    except json.JSONDecodeError:
        malformed = True
        data = read_json_object_or_recover(text)
    if malformed:
        rebuilt = rebuild_trigger_ack_state_from_jsonl(path.parent, name)
        if rebuilt:
            data = merge_trigger_state_objects(data, rebuilt)
    elif not data:
        data = rebuild_trigger_ack_state_from_jsonl(path.parent, name)
    return data if isinstance(data, dict) else {}


@contextmanager
def trigger_state_lock(state_dir: Path, state_name: str, timeout_seconds: float = 30.0):
    lock_path = state_dir / f"{state_name}.lock"
    deadline = time.time() + timeout_seconds
    fd: int | None = None
    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, f"pid={os.getpid()} acquired_at={utc_now_iso()}\n".encode("utf-8"))
            break
        except FileExistsError:
            if trigger_lock_owner_dead(lock_path) or stale_lock(
                lock_path,
                stale_seconds=max(10.0, timeout_seconds),
            ):
                try:
                    lock_path.unlink()
                    continue
                except OSError:
                    pass
            if time.time() >= deadline:
                raise RuntimeError(f"trigger ack state lock busy: {lock_path}")
            time.sleep(0.05)
    try:
        yield
    finally:
        if fd is not None:
            os.close(fd)
        try:
            lock_path.unlink()
        except FileNotFoundError:
            pass


def trigger_lock_owner_dead(path: Path) -> bool:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False
    pid = 0
    for token in text.replace("\n", " ").split():
        if not token.startswith("pid="):
            continue
        try:
            pid = int(token.split("=", 1)[1])
        except (TypeError, ValueError):
            pid = 0
        break
    return pid > 0 and not process_alive(pid)


def stale_lock(path: Path, stale_seconds: float) -> bool:
    try:
        age = time.time() - path.stat().st_mtime
    except OSError:
        return False
    return age >= stale_seconds


def write_json_atomic(path: Path, payload: dict[str, Any], timeout_seconds: float = 2.0) -> None:
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    try:
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        deadline = time.monotonic() + max(0.0, timeout_seconds)
        delay = 0.02
        while True:
            try:
                os.replace(str(tmp), str(path))
                return
            except OSError as exc:
                retryable = isinstance(exc, PermissionError) or getattr(exc, "winerror", None) in {
                    5,
                    32,
                    33,
                }
                if not retryable or time.monotonic() >= deadline:
                    raise
                time.sleep(delay)
                delay = min(delay * 1.5, 0.2)
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            pass


def read_json_object_or_recover(text: str) -> dict[str, Any]:
    try:
        data = json.loads(text)
        return data if isinstance(data, dict) else {}
    except json.JSONDecodeError:
        pass
    decoder = json.JSONDecoder()
    idx = 0
    merged: dict[str, Any] = {}
    while idx < len(text):
        while idx < len(text) and text[idx].isspace():
            idx += 1
        if idx >= len(text):
            break
        try:
            data, end = decoder.raw_decode(text, idx)
        except json.JSONDecodeError:
            break
        if isinstance(data, dict):
            merged = merge_trigger_state_objects(merged, data)
        idx = max(end, idx + 1)
    return merged


def merge_trigger_state_objects(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    if not left:
        return right
    if not right:
        return left
    sent = dict(left.get("sent", {})) if isinstance(left.get("sent"), dict) else {}
    right_sent = right.get("sent", {}) if isinstance(right.get("sent"), dict) else {}
    for key, record in right_sent.items():
        sent[key] = record
    return {
        "updated_at": right.get("updated_at") or left.get("updated_at") or "",
        "sent": sent,
    }


def rebuild_trigger_ack_state_from_jsonl(state_dir: Path, state_name: str) -> dict[str, Any]:
    jsonl_name = {
        "solver_trigger_ack_state.json": "solver_trigger_ack.jsonl",
        "tester_trigger_ack_state.json": "tester_trigger_ack.jsonl",
    }.get(state_name, "")
    if not jsonl_name:
        return {}
    path = state_dir / jsonl_name
    if not path.exists():
        return {}
    sent: dict[str, Any] = {}
    updated_at = ""
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return {}
    for line in lines:
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(record, dict):
            continue
        key = str(record.get("key", "") or "")
        if not key:
            continue
        updated_at = str(record.get("time") or record.get("updated_at") or updated_at)
        sent[key] = {
            **record,
            "updated_at": str(record.get("updated_at") or record.get("time") or ""),
        }
    return {"updated_at": updated_at, "sent": sent} if sent else {}
