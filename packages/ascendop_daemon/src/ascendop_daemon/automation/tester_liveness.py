from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ascendop_daemon.core.models import ActionKind, DaemonConfig, GateDecision, casegen_session_ops, tester_thread_for
from ascendop_daemon.automation.delivery_fence import (
    tester_casegen_active_covering_record,
)
from ascendop_daemon.automation.trigger_state import normalized_trigger_status, read_tester_trigger_ack_state


ACTIVE_STATUSES = {"sent", "delivered", "acked", "active", "completed"}
FAILED_STATUSES = {"interrupted", "failed", "cancelled"}


def build_tester_liveness(
    root: Path,
    config: DaemonConfig,
    decisions: tuple[GateDecision, ...],
    captured_at: str,
) -> dict[str, Any]:
    stale_seconds = int(
        config.policy.get("tester_board_stale_seconds")
        or config.policy.get("tester_trigger_retry_seconds")
        or 300
    )
    now = parse_timestamp(captured_at) or datetime.now(timezone.utc)
    ack_state = read_tester_trigger_ack_state(root)
    sent = ack_state.get("sent", {}) if isinstance(ack_state.get("sent"), dict) else {}
    observation_payload = read_json(
        root / "TestUtils" / "tester_daemon" / "tester_thread_observations.json"
    )
    observations = (
        observation_payload.get("threads", [])
        if isinstance(observation_payload.get("threads"), list)
        else []
    )
    latest_by_thread = {
        str(item.get("thread_id", "") or ""): item
        for item in observations
        if isinstance(item, dict) and item.get("thread_id")
    }
    required_reads = [
        {"op": op, "thread_id": tester_thread_for(config, op)}
        for op in casegen_session_ops(config)
    ]
    active_gates: list[dict[str, Any]] = []
    stale_gates: list[dict[str, Any]] = []
    missing_threads = [item for item in required_reads if not item.get("thread_id")]

    for decision in decisions:
        if not is_tester_casegen_decision(decision, sent):
            continue
        key = "|".join([decision.row.op, decision.row.gate_stage, decision.row.next_command])
        record = sent.get(key) if isinstance(sent, dict) else None
        covered_by_casegen_ack = False
        if not isinstance(record, dict):
            record = tester_casegen_active_covering_record(sent, decision.row.op, key)
            covered_by_casegen_ack = isinstance(record, dict)
        raw_status = str(record.get("status", "") or "") if isinstance(record, dict) else "not-sent"
        status = normalized_trigger_status(record, raw_status) if isinstance(record, dict) else "not-sent"
        updated_at = str(record.get("updated_at", "") or "") if isinstance(record, dict) else ""
        updated_ts = parse_timestamp(updated_at)
        age_seconds = max(0, int((now - updated_ts).total_seconds())) if updated_ts else None
        thread_id = (
            str(record.get("thread_id", "") or "")
            if isinstance(record, dict)
            else tester_thread_for(config, decision.row.op)
        )
        observed = latest_by_thread.get(thread_id, {})
        observed_turn_id = (
            str(observed.get("latest_turn_id", "") or "") if isinstance(observed, dict) else ""
        )
        observed_status = (
            str(observed.get("latest_turn_status", "") or "")
            if isinstance(observed, dict)
            else ""
        )
        observed_idle = observed.get("idle_seconds") if isinstance(observed, dict) else None
        native_thread_active = (
            observed_status in {"inProgress", "active", "running"}
            and isinstance(observed_idle, (int, float))
            and (stale_seconds <= 0 or float(observed_idle) < stale_seconds)
        )
        item = {
            "op": decision.row.op,
            "gate_stage": decision.row.gate_stage,
            "key": key,
            "action": decision.action.value,
            "thread_id": thread_id,
            "status": status,
            "raw_status": raw_status if raw_status != status else "",
            "updated_at": updated_at,
            "age_seconds": age_seconds,
            "requires_native_read": bool(thread_id),
            "next_command": decision.row.next_command,
            "missing_thread": not bool(thread_id),
            "observed_turn_id": observed_turn_id,
            "observed_turn_status": observed_status,
            "observed_turn_idle_seconds": observed_idle,
            "native_thread_active": native_thread_active,
        }
        if covered_by_casegen_ack:
            item["covered_by_casegen_ack"] = True
        item["stale"] = (
            status in FAILED_STATUSES
            or (
                status in ACTIVE_STATUSES
                and age_seconds is not None
                and stale_seconds > 0
                and age_seconds >= stale_seconds
                and not native_thread_active
            )
        )
        if item["stale"]:
            stale_gates.append(item)
        active_gates.append(item)

    return {
        "updated_at": captured_at,
        "stale_threshold_seconds": stale_seconds,
        "required_thread_reads": required_reads,
        "missing_tester_threads": missing_threads,
        "active_tester_gates": active_gates,
        "stale_tester_gates": stale_gates,
    }


def is_tester_casegen_decision(decision: GateDecision, sent: dict[str, Any]) -> bool:
    if decision.action == ActionKind.NOTIFY_TESTER_CASEGEN:
        return True
    if decision.row.next_owner != "tester":
        return False
    gate_text = f"{decision.row.gate_stage} {decision.row.wakeups} {decision.reason}".lower()
    command_text = decision.row.next_command.lower()
    if not (
        "casegen" in gate_text
        or "case-version" in gate_text
        or "needs-case" in gate_text
        or "generate-case-version" in command_text
        or "tester-authored casegen" in command_text
    ):
        return False
    key = "|".join([decision.row.op, decision.row.gate_stage, decision.row.next_command])
    record = sent.get(key) if isinstance(sent, dict) else None
    if not isinstance(record, dict):
        record = tester_casegen_active_covering_record(sent, decision.row.op, key)
    if not isinstance(record, dict):
        return False
    status = normalized_trigger_status(record, str(record.get("status", "") or ""))
    return status in ACTIVE_STATUSES or status in FAILED_STATUSES


def write_tester_liveness_files(root: Path, snapshot: dict[str, Any]) -> None:
    state_dir = root / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "tester_session_status.json").write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    (state_dir / "TESTER_SESSION_STATUS.md").write_text(
        render_tester_liveness(snapshot),
        encoding="utf-8",
    )


def render_tester_liveness(snapshot: dict[str, Any]) -> str:
    lines = [
        "# Tester Session Status",
        "",
        f"- updated_at: {snapshot.get('updated_at', '')}",
        f"- stale_threshold_seconds: {snapshot.get('stale_threshold_seconds', '')}",
        "",
        "## Required Thread Reads",
        "",
    ]
    reads = snapshot.get("required_thread_reads", [])
    if isinstance(reads, list) and reads:
        for item in reads:
            if isinstance(item, dict):
                lines.append(f"- {item.get('op', '-')}: {item.get('thread_id') or 'MISSING'}")
    else:
        lines.append("- none")
    lines.extend(["", "## Active Tester Gates", ""])
    active = snapshot.get("active_tester_gates", [])
    if isinstance(active, list) and active:
        for item in active:
            if isinstance(item, dict):
                lines.append(
                    f"- {item.get('op', '-')}: {item.get('gate_stage', '-')} "
                    f"status={item.get('status', '-')} age_seconds={item.get('age_seconds', '-')}"
                )
    else:
        lines.append("- none")
    lines.append("")
    return "\n".join(lines)


def parse_timestamp(text: str) -> datetime | None:
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}
