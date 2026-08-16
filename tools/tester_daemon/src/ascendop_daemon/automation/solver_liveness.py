from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ascendop_daemon.core.models import ActionKind, DaemonConfig, GateDecision, observed_operators, solver_thread_for
from ascendop_daemon.automation.trigger_state import normalized_trigger_status, read_trigger_ack_state


ACTIVE_STATUSES = {"sent", "delivered", "acked", "active", "completed"}
FAILED_STATUSES = {"interrupted", "failed", "cancelled"}


def build_solver_liveness(
    root: Path,
    config: DaemonConfig,
    decisions: tuple[GateDecision, ...],
    captured_at: str,
) -> dict[str, Any]:
    stale_seconds = int(
        config.policy.get("solver_board_stale_seconds")
        or config.policy.get("solver_trigger_retry_seconds")
        or 480
    )
    now = parse_timestamp(captured_at) or datetime.now(timezone.utc)
    ack_state = read_trigger_ack_state(root)
    sent = ack_state.get("sent", {}) if isinstance(ack_state.get("sent"), dict) else {}
    observation_payload = read_json(root / "TestUtils" / "tester_daemon" / "solver_thread_observations.json")
    observations = observation_payload.get("threads", []) if isinstance(observation_payload.get("threads"), list) else []
    latest_by_thread = {
        str(item.get("thread_id", "") or ""): item
        for item in observations
        if isinstance(item, dict) and item.get("thread_id")
    }

    required_reads = [
        {"op": op, "thread_id": solver_thread_for(config, op)}
        for op in sorted(observed_operators(config))
        if solver_thread_for(config, op)
    ]
    active_gates: list[dict[str, Any]] = []
    stale_gates: list[dict[str, Any]] = []

    for decision in decisions:
        if decision.row.next_owner != "solver":
            continue
        if (
            "gitpartner-run-msopgen" in decision.row.next_command
            and decision.action != ActionKind.NOTIFY_SOLVER
        ):
            # Workspace generation is harness-owned. A historical solver ack
            # for this gate must not remain an active/stale solver obligation.
            continue
        key = "|".join([decision.row.op, decision.row.gate_stage, decision.row.next_command])
        record = sent.get(key) if isinstance(sent, dict) else None
        raw_status = str(record.get("status", "") or "") if isinstance(record, dict) else "not-sent"
        status = normalized_trigger_status(record, raw_status) if isinstance(record, dict) else "not-sent"
        updated_at = str(record.get("updated_at", "") or "") if isinstance(record, dict) else ""
        updated_ts = parse_timestamp(updated_at)
        age_seconds = max(0, int((now - updated_ts).total_seconds())) if updated_ts else None
        last_observed_at = str(record.get("last_observed_at", "") or "") if isinstance(record, dict) else ""
        observed_ts = parse_timestamp(last_observed_at)
        observation_age_seconds = (
            max(0, int((now - observed_ts).total_seconds())) if observed_ts else None
        )
        thread_id = (
            str(record.get("thread_id", "") or "")
            if isinstance(record, dict)
            else solver_thread_for(config, decision.row.op)
        )
        native_status = str(record.get("native_status", "") or "") if isinstance(record, dict) else ""
        observed = latest_by_thread.get(thread_id, {})
        observed_turn_id = str(observed.get("latest_turn_id", "") or "") if isinstance(observed, dict) else ""
        observed_status = str(observed.get("latest_turn_status", "") or "") if isinstance(observed, dict) else ""
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
            "last_observed_at": last_observed_at,
            "observation_age_seconds": observation_age_seconds,
            "native_status": native_status,
            "observed_turn_id": observed_turn_id,
            "observed_turn_status": observed_status,
            "observed_turn_idle_seconds": observed_idle,
            "observed_turn_mismatch": bool(
                observed_turn_id
                and isinstance(record, dict)
                and str(record.get("turn_id") or record.get("native_id") or "")
                and observed_turn_id
                != str(record.get("turn_id") or record.get("native_id") or "")
            ),
            "native_thread_active": native_thread_active,
            "failure_kind": str(record.get("failure_kind", "") or "") if isinstance(record, dict) else "",
            "thread_status_type": str(record.get("thread_status_type", "") or "") if isinstance(record, dict) else "",
            "native_no_agent_output_count": (
                record.get("native_no_agent_output_count", 0) if isinstance(record, dict) else 0
            ),
            "ide_panel_visible": bool(record.get("ide_panel_visible")) if isinstance(record, dict) else False,
            "requires_native_read": bool(thread_id),
            "next_command": decision.row.next_command,
        }
        if status in FAILED_STATUSES:
            item["stale"] = (
                stale_seconds > 0
                and (
                    observation_age_seconds is None
                    or observation_age_seconds >= stale_seconds
                )
            )
        elif (
            status in ACTIVE_STATUSES
            and age_seconds is not None
            and stale_seconds > 0
            and age_seconds >= stale_seconds
            and not native_thread_active
        ):
            item["stale"] = True
        else:
            item["stale"] = False
        if item["stale"]:
            stale_gates.append(item)
        active_gates.append(item)

    return {
        "updated_at": captured_at,
        "stale_threshold_seconds": stale_seconds,
        "required_thread_reads": required_reads,
        "active_solver_gates": active_gates,
        "stale_solver_gates": stale_gates,
    }


def write_solver_liveness_files(root: Path, snapshot: dict[str, Any]) -> None:
    state_dir = root / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "solver_session_status.json").write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    (state_dir / "SOLVER_SESSION_STATUS.md").write_text(
        render_solver_liveness(snapshot),
        encoding="utf-8",
    )


def render_solver_liveness(snapshot: dict[str, Any]) -> str:
    lines = [
        "# Solver Session Status",
        "",
        f"- updated_at: {snapshot.get('updated_at', '')}",
        f"- stale_threshold_seconds: {snapshot.get('stale_threshold_seconds', '')}",
        "",
        "## Required Native Reads",
        "",
    ]
    reads = snapshot.get("required_thread_reads", [])
    if isinstance(reads, list) and reads:
        lines.extend(["| op | thread_id |", "|---|---|"])
        for item in reads:
            if not isinstance(item, dict):
                continue
            lines.append(f"| {item.get('op', '-')} | {item.get('thread_id', '-')} |")
    else:
        lines.append("- none")
    lines.extend(["", "## Active Solver Gates", ""])
    gates = snapshot.get("active_solver_gates", [])
    if isinstance(gates, list) and gates:
        lines.extend(
            [
                "| op | gate | status | raw_status | age_seconds | observation_age_seconds | native | ide_visible | stale |",
                "|---|---|---|---|---:|---:|---|---:|---:|",
            ]
        )
        for item in gates:
            if not isinstance(item, dict):
                continue
            lines.append(
                f"| {item.get('op', '-')} | {item.get('gate_stage', '-')} | "
                f"{item.get('status', '-')} | {item.get('raw_status', '-') or '-'} | "
                f"{item.get('age_seconds', '-')} | {item.get('observation_age_seconds', '-') or '-'} | "
                f"{item.get('native_status', '-') or '-'} | {bool(item.get('ide_panel_visible'))} | "
                f"{bool(item.get('stale'))} |"
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
