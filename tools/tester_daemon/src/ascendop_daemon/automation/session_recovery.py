from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ascendop_daemon.core.models import DaemonConfig, solver_thread_for, tester_thread_for, utc_now_iso
from ascendop_daemon.legacy.native_relay_outbox import release_native_relay_claims
from ascendop_daemon.automation.trigger_state import (
    ack_solver_trigger,
    ack_tester_trigger,
    normalized_trigger_status,
    read_tester_trigger_ack_state,
    read_trigger_ack_state,
)


ACTIVE_DELIVERY_STATUSES = {"sent", "delivered", "acked", "active"}


def recover_codex_sessions(
    root: Path,
    config: DaemonConfig,
    *,
    reason: str,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Requeue only current native session gates after an explicit app restart.

    The data plane is deliberately outside this recovery boundary: queue,
    result, resource lease, and execute-worker state are never modified.
    """
    state_dir = root / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    now = utc_now_iso()
    recovered: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    current_gate_keys: set[str] = set()

    for role, plan_name, ack_reader, ack_writer, configured_thread in (
        (
            "solver",
            "solver_trigger_plan.json",
            read_trigger_ack_state,
            ack_solver_trigger,
            solver_thread_for,
        ),
        (
            "tester",
            "tester_trigger_plan.json",
            read_tester_trigger_ack_state,
            ack_tester_trigger,
            tester_thread_for,
        ),
    ):
        plan = _read_json(state_dir / plan_name)
        ack_state = ack_reader(root)
        sent = ack_state.get("sent", {}) if isinstance(ack_state.get("sent"), dict) else {}
        triggers = plan.get("triggers", []) if isinstance(plan.get("triggers"), list) else []
        for trigger in triggers:
            if not isinstance(trigger, dict):
                continue
            key = str(trigger.get("key", "") or "")
            op = str(trigger.get("op", "") or "")
            plan_thread_id = str(trigger.get("thread_id", "") or "")
            expected_thread_id = configured_thread(config, op)
            if not key or not op:
                continue
            if op not in config.operators:
                skipped.append(_skip(role, op, key, "operator-not-active"))
                continue
            if not expected_thread_id:
                skipped.append(_skip(role, op, key, "configured-thread-missing"))
                continue
            if plan_thread_id != expected_thread_id:
                skipped.append(_skip(role, op, key, "plan-thread-mismatch"))
                continue
            current_gate_keys.add(key)
            ack = sent.get(key)
            ack_record = dict(ack) if isinstance(ack, dict) else {}
            ack_status = normalized_trigger_status(ack_record)
            plan_status = str(trigger.get("status", "") or "")
            recovery_status = ack_status if ack_record else plan_status
            if recovery_status not in ACTIVE_DELIVERY_STATUSES:
                skipped.append(_skip(role, op, key, f"not-active:{plan_status or '-'}:{ack_status or '-'}"))
                continue

            previous_turn_id = str(ack_record.get("turn_id") or ack_record.get("native_id") or "")
            item = {
                "role": role,
                "op": op,
                "key": key,
                "thread_id": expected_thread_id,
                "plan_status": plan_status,
                "previous_ack_status": ack_status,
                "previous_turn_id": previous_turn_id,
                "action": "mark-interrupted-for-immediate-native-retry",
            }
            recovered.append(item)
            if dry_run:
                continue
            metadata: dict[str, Any] = {
                "failure_kind": "codex_app_restart_interrupted",
                "error": reason,
                "method": "explicit-app-restart-recovery",
                "native_status": "interrupted",
                "turn_status": "interrupted",
                "wait_status": "interrupted",
                "ide_panel_visible": False,
                "ide_panel_visibility": "app_restart_recovery_pending",
                "app_restart_recovered_at": now,
                "completion_unconfirmed": False,
                "control_plane_unavailable": False,
                "orphaned_delivery_owner": False,
                "delivery_reconciliation": "explicit-app-restart",
            }
            if previous_turn_id:
                metadata["turn_id"] = previous_turn_id
            ack_writer(root, key, expected_thread_id, "interrupted", metadata)

    released_claims = release_native_relay_claims(
        root,
        keys=current_gate_keys,
        reason=reason,
        dry_run=dry_run,
    )
    payload = {
        "schema_version": 1,
        "updated_at": now,
        "status": "dry-run" if dry_run else "recovered",
        "reason": reason,
        "dry_run": dry_run,
        "active_operators": list(config.operators),
        "recovered_count": len(recovered),
        "released_claim_count": int(released_claims.get("released_count", 0) or 0),
        "recovered": recovered,
        "released_claims": released_claims.get("released", []),
        "skipped": skipped,
        "preserved": {
            "session_ids": True,
            "operator_plugin_set": True,
            "queue_and_results": True,
            "resource_leases": True,
            "execute_workers": True,
            "resident_daemon_when_healthy": True,
        },
    }
    if not dry_run:
        _write_recovery_report(state_dir, payload)
    return payload


def _skip(role: str, op: str, key: str, reason: str) -> dict[str, str]:
    return {"role": role, "op": op, "key": key, "reason": reason}


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _write_recovery_report(state_dir: Path, payload: dict[str, Any]) -> None:
    json_path = state_dir / "SESSION_RECOVERY.json"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    lines = [
        "# Session Recovery",
        "",
        f"- updated_at: {payload.get('updated_at', '')}",
        f"- status: {payload.get('status', '')}",
        f"- reason: {payload.get('reason', '')}",
        f"- recovered_count: {payload.get('recovered_count', 0)}",
        f"- released_claim_count: {payload.get('released_claim_count', 0)}",
        "",
        "| role | op | previous status | previous turn | action |",
        "|---|---|---|---|---|",
    ]
    for item in payload.get("recovered", []):
        if not isinstance(item, dict):
            continue
        lines.append(
            f"| {item.get('role', '-')} | {item.get('op', '-')} | "
            f"{item.get('previous_ack_status', '-')} | {item.get('previous_turn_id', '-')} | "
            f"{item.get('action', '-')} |"
        )
    if not payload.get("recovered"):
        lines.append("| - | - | - | - | no active native turn required recovery |")
    lines.append("")
    (state_dir / "SESSION_RECOVERY.md").write_text("\n".join(lines), encoding="utf-8")
    with (state_dir / "session_recovery_events.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
