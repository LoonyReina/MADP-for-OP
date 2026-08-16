from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ascendop_daemon.automation.trigger_state import (
    codex_cli_resume_process_active,
    codex_cli_resume_process_dead,
    normalized_trigger_status,
)


ACTIVE_ACK_COVER_SECONDS = 300


def ack_has_live_delivery_fence(record: Any) -> bool:
    if not isinstance(record, dict):
        return False
    turn_id = str(record.get("turn_id") or record.get("native_id") or "")
    if not turn_id:
        return False
    status = normalized_trigger_status(record, str(record.get("status", "") or ""))
    native_status = str(record.get("native_status") or record.get("turn_status") or "")
    if status != "active" or native_status not in {"active", "inProgress", "running"}:
        return False
    observed_at = parse_timestamp(
        str(record.get("last_observed_at") or record.get("updated_at") or "")
    )
    if observed_at is None:
        return False
    now = datetime.now(timezone.utc).replace(microsecond=0)
    if (now - observed_at).total_seconds() > ACTIVE_ACK_COVER_SECONDS:
        return False
    if str(record.get("delivery", "") or "") == "codex-cli-exec-resume":
        return codex_cli_resume_process_active(record)
    return True


def tester_casegen_active_covering_record(
    sent: Any,
    op: str,
    current_key: str,
) -> dict[str, Any] | None:
    if not isinstance(sent, dict) or not op:
        return None
    current_case_version = extract_case_version(current_key)
    prefix = f"{op}|"
    for key, record in sent.items():
        trigger_key = str(key)
        if trigger_key == current_key or not trigger_key.startswith(prefix):
            continue
        if (
            "|needs-case-version|" not in trigger_key
            and "|casegen-evidence-incomplete|" not in trigger_key
        ):
            continue
        if current_case_version and extract_case_version(trigger_key) != current_case_version:
            continue
        if not isinstance(record, dict):
            continue
        status = normalized_trigger_status(record, str(record.get("status", "") or ""))
        if status not in {"sent", "delivered", "acked", "active"}:
            continue
        if codex_cli_resume_process_dead(record):
            continue
        if status == "active":
            if not ack_has_live_delivery_fence(record):
                continue
        elif not ack_is_ide_confirmed(record):
            continue
        if not str(record.get("turn_id") or record.get("native_id") or ""):
            continue
        return record
    return None


def ack_is_ide_confirmed(record: Any) -> bool:
    if not isinstance(record, dict):
        return False
    status = normalized_trigger_status(record, str(record.get("status", "") or ""))
    if status in {"failed", "interrupted", "cancelled", "needs-native-delivery"}:
        return False
    if str(record.get("delivery_retry_reason", "") or "") == "remote_control_not_ready":
        return False
    if str(record.get("failure_kind", "") or "") == "native_turn_no_agent_output":
        return False
    if status != "active" and (
        record.get("latest_user_only_turn") is True
        or record.get("last_observed_user_only_turn") is True
    ):
        return False
    native_status = str(record.get("native_status") or record.get("turn_status") or "")
    if status != "active" and native_status in {"failed", "interrupted", "cancelled"}:
        return False
    delivery = str(record.get("delivery", "") or "")
    visibility = str(record.get("ide_panel_visibility", "") or "")
    return record.get("ide_panel_visible") is True and (
        delivery in {"ide-native-relay", "codex-app-send-message-to-thread"}
        or visibility in {"confirmed_by_native_relay", "live_proxy", "live_ws"}
    )


def parse_timestamp(text: str) -> datetime | None:
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(microsecond=0)


def extract_case_version(key: str) -> str:
    case_version = ""
    for token in key.replace("\\", " ").replace("/", " ").replace(";", " ").split():
        if token.startswith("case_v"):
            case_version = token.strip("`'\".,:;)")
    return case_version
