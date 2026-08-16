from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from hashlib import sha1
from pathlib import Path
from typing import Any

from ascendop_daemon.core.models import DaemonConfig, utc_now_iso


STATE_FILE = "operator_plugin_state.json"
EVENTS_FILE = "operator_plugin_events.jsonl"
TERMINAL_QUEUE_STATUSES = {
    "done",
    "failed",
    "cancelled",
    "canceled",
    "success",
    "error",
}
ACTIVE_RELAY_STATUSES = {
    "active",
    "claimed",
    "reconcile-wait",
    "same-session-recovery-required",
    "sent",
}
ENGINE_OUTBOX_TERMINAL_STATES = {
    "cancelled-before-admission",
    "standby-cancelled",
    "return-lost",
    "workflow-archived",
    "superseded-by-workflow-result",
    "superseded-by-logical-attempt",
    "canary-complete",
}


def operator_set_signature(operators: list[str] | tuple[str, ...]) -> str:
    normalized = sorted({str(op) for op in operators if str(op)})
    return sha1("\n".join(normalized).encode("utf-8")).hexdigest()[:12]


def reconcile_operator_plugin_state(
    root: Path,
    config: DaemonConfig,
    *,
    now: str | None = None,
) -> dict[str, Any]:
    """Persist the active plugin generation used to scope scheduling metrics.

    The first observation adopts legacy history.  A later membership change
    starts a fresh metric epoch so tests from an earlier activation cannot pay
    debt for a newly inserted operator.
    """
    now = now or utc_now_iso()
    state_dir = root / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    path = state_dir / STATE_FILE
    previous = read_json(path)
    active = [str(op) for op in getattr(config, "operators", ()) if str(op)]
    sessions = getattr(config, "operator_sessions", {})
    sessions = sessions if isinstance(sessions, dict) else {}
    signature = operator_set_signature(active)
    previous_signature = str(previous.get("signature", "") or "")
    previous_active = [
        str(op) for op in previous.get("active_operators", []) if str(op)
    ]

    if not previous_signature:
        payload = {
            "schema_version": 1,
            "generation": 1,
            "signature": signature,
            "active_operators": active,
            "active_operator_count": len(active),
            "draining_operators": list(getattr(config, "draining_operators", ())),
            "initialized_at": now,
            "changed_at": now,
            "metrics_epoch_at": "",
            "metrics_epoch_reason": "adopt-existing-history",
            "previous_active_operators": [],
            "added": active,
            "removed": [],
            "last_seen_at": now,
        }
        write_json_atomic(path, payload)
        append_event(state_dir, "operator_set_initialized", payload)
        return payload

    membership_changed = previous_signature != signature
    if membership_changed:
        generation = int(previous.get("generation", 1) or 1) + 1
        payload = {
            "schema_version": 1,
            "generation": generation,
            "signature": signature,
            "active_operators": active,
            "active_operator_count": len(active),
            "draining_operators": list(getattr(config, "draining_operators", ())),
            "initialized_at": str(previous.get("initialized_at", "") or now),
            "changed_at": now,
            "metrics_epoch_at": now,
            "metrics_epoch_reason": "active-operator-membership-changed",
            "previous_active_operators": previous_active,
            "added": [op for op in active if op not in previous_active],
            "removed": [op for op in previous_active if op not in active],
            "last_seen_at": now,
        }
        write_json_atomic(path, payload)
        append_event(state_dir, "operator_set_changed", payload)
        return payload

    payload = dict(previous)
    payload.update(
        {
            "active_operators": active,
            "active_operator_count": len(active),
            "draining_operators": list(getattr(config, "draining_operators", ())),
            "last_seen_at": now,
        }
    )
    write_json_atomic(path, payload)
    return payload


def read_operator_plugin_state(root: Path) -> dict[str, Any]:
    return read_json(root / "TestUtils" / "tester_daemon" / STATE_FILE)


def build_operator_plugin_status(
    root: Path,
    config_path: Path,
) -> dict[str, Any]:
    data = read_json(config_path)
    config = _load_config_after_write(config_path)
    state = reconcile_operator_plugin_state(root, config)
    sessions = data.get("operator_sessions", {})
    sessions = sessions if isinstance(sessions, dict) else {}
    configured = data.get("operators", [])
    configured = configured if isinstance(configured, list) else []
    registered = [str(op) for op in configured if str(op)]
    registered.extend(str(op) for op in sessions if str(op) not in registered)
    active = set(config.operators)
    draining = set(config.draining_operators)
    plugins: list[dict[str, Any]] = []
    for op in registered:
        session = sessions.get(op, {})
        session = session if isinstance(session, dict) else {}
        lifecycle = (
            "active" if op in active else "draining" if op in draining else "disabled"
        )
        plugins.append(
            {
                "op": op,
                "lifecycle": lifecycle,
                "season": str(
                    session.get("season", "") or data.get("season", "") or ""
                ),
                "solver_thread_id": str(session.get("solver_thread_id", "") or ""),
                "tester_thread_id": str(session.get("tester_thread_id", "") or ""),
                "roles": (
                    session.get("roles", {})
                    if isinstance(session.get("roles"), dict)
                    else {}
                ),
            }
        )
    return {
        "active_operator_count": len(active),
        "active_operators": list(config.operators),
        "draining_operators": list(config.draining_operators),
        "generation": state.get("generation", 0),
        "metrics_epoch_at": state.get("metrics_epoch_at", ""),
        "plugins": plugins,
    }


def metric_epoch_for_config(root: Path, config: DaemonConfig) -> datetime | None:
    state = read_operator_plugin_state(root)
    epochs: list[datetime] = []
    signature_matches = str(state.get("signature", "") or "") == operator_set_signature(
        config.operators
    )
    if signature_matches:
        parsed = parse_metric_epoch(state.get("metrics_epoch_at"))
        if parsed is not None:
            epochs.append(parsed)

        # The service epoch belongs to the active plugin generation. Ignore it
        # for a different or uninitialized config so dry-run snapshots and a
        # newly inserted operator set cannot inherit an unrelated live epoch.
        service = read_json(
            root / "TestUtils" / "tester_daemon" / "workflow_service_state.json"
        )
        if str(service.get("action", "") or "") == "started":
            parsed = parse_metric_epoch(service.get("metrics_epoch_at"))
            if parsed is not None:
                epochs.append(parsed)
    return max(epochs) if epochs else None


def parse_metric_epoch(value: object) -> datetime | None:
    raw = str(value or "")
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def update_operator_enabled(
    root: Path,
    config_path: Path,
    op: str,
    enabled: bool,
    *,
    require_quiescent: bool = True,
) -> dict[str, Any]:
    data = read_json(config_path)
    sessions = data.get("operator_sessions", {})
    if not isinstance(sessions, dict) or not isinstance(sessions.get(op), dict):
        raise ValueError(f"operator plugin is not registered: {op}")
    session = sessions[op]
    current = bool(session.get("enabled", True))
    if current == enabled and enabled and bool(session.get("drain_requested", False)):
        session["drain_requested"] = False
        write_json_atomic(config_path, data)
        config = _load_config_after_write(config_path)
        state = reconcile_operator_plugin_state(root, config)
        return {
            "changed": True,
            "op": op,
            "enabled": True,
            "drain_cancelled": True,
            "active_operators": list(config.operators),
            "generation": state.get("generation", 0),
            "metrics_epoch_at": state.get("metrics_epoch_at", ""),
        }
    if current == enabled:
        config = _load_config_after_write(config_path)
        state = reconcile_operator_plugin_state(root, config)
        return {
            "changed": False,
            "op": op,
            "enabled": enabled,
            "active_operators": list(config.operators),
            "generation": state.get("generation", 0),
        }

    if enabled:
        validate_session_for_enable(op, session)
        operators = data.get("operators", [])
        if not isinstance(operators, list):
            raise ValueError("config operators must be a list")
        if op not in operators:
            operators.append(op)
    elif require_quiescent:
        blockers = operator_disable_blockers(root, op)
        if blockers:
            raise RuntimeError(
                f"operator plugin is not quiescent: {op}: " + "; ".join(blockers)
            )

    session["enabled"] = enabled
    if not enabled:
        session["drain_requested"] = False
    write_json_atomic(config_path, data)
    config = _load_config_after_write(config_path)
    state = reconcile_operator_plugin_state(root, config)
    append_event(
        root / "TestUtils" / "tester_daemon",
        "operator_plugin_enabled" if enabled else "operator_plugin_disabled",
        {
            "op": op,
            "enabled": enabled,
            "active_operators": list(config.operators),
            "generation": state.get("generation", 0),
            "metrics_epoch_at": state.get("metrics_epoch_at", ""),
        },
    )
    return {
        "changed": True,
        "op": op,
        "enabled": enabled,
        "active_operators": list(config.operators),
        "generation": state.get("generation", 0),
        "metrics_epoch_at": state.get("metrics_epoch_at", ""),
    }


def request_operator_drain(root: Path, config_path: Path, op: str) -> dict[str, Any]:
    data = read_json(config_path)
    sessions = data.get("operator_sessions", {})
    if not isinstance(sessions, dict) or not isinstance(sessions.get(op), dict):
        raise ValueError(f"operator plugin is not registered: {op}")
    session = sessions[op]
    if not bool(session.get("enabled", True)):
        return {"changed": False, "op": op, "enabled": False, "drain_requested": False}
    blockers = operator_disable_blockers(root, op)
    if not blockers:
        return update_operator_enabled(root, config_path, op, False)
    session["drain_requested"] = True
    write_json_atomic(config_path, data)
    config = _load_config_after_write(config_path)
    state = reconcile_operator_plugin_state(root, config)
    append_event(
        root / "TestUtils" / "tester_daemon",
        "operator_plugin_drain_requested",
        {"op": op, "blockers": blockers, "generation": state.get("generation", 0)},
    )
    return {
        "changed": True,
        "op": op,
        "enabled": True,
        "drain_requested": True,
        "blockers": blockers,
        "active_operators": list(config.operators),
        "generation": state.get("generation", 0),
    }


def reconcile_draining_operator_plugins(
    root: Path,
    config_path: Path,
    config: DaemonConfig,
) -> tuple[DaemonConfig, list[dict[str, Any]]]:
    actions: list[dict[str, Any]] = []
    current = config
    for op in list(current.draining_operators):
        session = current.operator_sessions.get(op)
        if session is None or not session.drain_requested:
            continue
        blockers = operator_disable_blockers(root, op)
        if blockers:
            actions.append({"op": op, "action": "drain-wait", "blockers": blockers})
            continue
        result = update_operator_enabled(root, config_path, op, False)
        actions.append({"op": op, "action": "drain-complete", **result})
        current = _load_config_after_write(config_path)
    return current, actions


def validate_session_for_enable(op: str, session: dict[str, Any]) -> None:
    if not str(session.get("solver_thread_id", "") or ""):
        raise ValueError(f"operator plugin missing solver_thread_id: {op}")
    roles = session.get("roles", {})
    roles = roles if isinstance(roles, dict) else {}
    if bool(roles.get("casegen", False)) and not str(
        session.get("tester_thread_id", "") or ""
    ):
        raise ValueError(
            f"operator plugin missing tester_thread_id for casegen role: {op}"
        )
    if not str(session.get("season", "") or ""):
        raise ValueError(f"operator plugin missing season: {op}")


def operator_disable_blockers(root: Path, op: str) -> list[str]:
    blockers: list[str] = []
    leases = read_json(root / "TestUtils" / "tester_daemon" / "leases.json").get(
        "leases", []
    )
    if isinstance(leases, list) and any(
        isinstance(item, dict) and str(item.get("op", "") or "") == op
        for item in leases
    ):
        blockers.append("active resource lease")

    queue_rows = read_queue_rows(root / "TestUtils" / "submit" / "queue.md")
    active_rows = [
        row
        for row in queue_rows
        if row.get("op") == op
        and row.get("status", "").lower() not in TERMINAL_QUEUE_STATUSES
    ]
    if active_rows:
        versions = ",".join(str(row.get("test_version", "")) for row in active_rows[:3])
        blockers.append(f"nonterminal queue rows={versions}")

    engine_entries = read_json(
        root / "TestUtils" / "tester_daemon" / "engine_pump_state.json"
    ).get("entries", {})
    if isinstance(engine_entries, dict):
        active_engine = [
            item
            for item in engine_entries.values()
            if isinstance(item, dict)
            and str(item.get("operator", "") or "") == op
            and str(item.get("state", "") or "") not in ENGINE_OUTBOX_TERMINAL_STATES
        ]
        if active_engine:
            states = ",".join(
                str(item.get("state", "") or "unknown") for item in active_engine[:3]
            )
            blockers.append(f"nonterminal engine outbox states={states}")

    for filename in ("solver_trigger_plan.json", "tester_trigger_plan.json"):
        triggers = read_json(root / "TestUtils" / "tester_daemon" / filename).get(
            "triggers", []
        )
        if not isinstance(triggers, list):
            continue
        active = [
            item
            for item in triggers
            if isinstance(item, dict)
            and str(item.get("op", "") or "") == op
            and str(item.get("status", "") or "") in ACTIVE_RELAY_STATUSES
        ]
        if active:
            blockers.append(f"active relay turn in {filename}")
    # A turn may write board-consumable files before the final native thread
    # observation lands, which clears the plan.  Keep polling the enabled
    # plugin until its ack is terminal so the last visible turn is not orphaned.
    from ascendop_daemon.automation.delivery_fence import ack_has_live_delivery_fence

    for filename in ("solver_trigger_ack_state.json", "tester_trigger_ack_state.json"):
        sent = read_json(root / "TestUtils" / "tester_daemon" / filename).get(
            "sent", {}
        )
        if not isinstance(sent, dict):
            continue
        if any(
            str(key).startswith(f"{op}|")
            and isinstance(record, dict)
            and ack_has_live_delivery_fence(record)
            for key, record in sent.items()
        ):
            blockers.append(f"native turn not terminal in {filename}")
    return blockers


def read_queue_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    rows: list[dict[str, str]] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        stripped = line.strip()
        if (
            not stripped.startswith("|")
            or stripped.startswith("|---")
            or stripped.startswith("| status ")
        ):
            continue
        cells = [cell.strip() for cell in stripped.strip("|").split("|")]
        if len(cells) < 3:
            continue
        rows.append({"status": cells[0], "op": cells[1], "test_version": cells[2]})
    return rows


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def append_event(state_dir: Path, event: str, payload: dict[str, Any]) -> None:
    state_dir.mkdir(parents=True, exist_ok=True)
    record = {"time": utc_now_iso(), "event": event, **payload}
    with (state_dir / EVENTS_FILE).open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def _load_config_after_write(path: Path) -> DaemonConfig:
    # Local import avoids a module cycle: config_loader does not depend on the
    # plugin state, while this command needs the canonical active-set filter.
    from ascendop_daemon.runtime.config_loader import load_config

    return load_config(path, apply_completion_markers=True)
from ascendop_daemon.core.atomic_io import write_json_atomic
