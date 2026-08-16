from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ascendop_daemon.core.models import (
    ActionKind,
    BoardSnapshot,
    DaemonConfig,
    DaemonPlan,
    GateDecision,
    casegen_session_ops,
    observed_operators,
    solver_thread_for,
)
from ascendop_daemon.registry.operator_plugins import metric_epoch_for_config


DRAIN_ALLOWED_ACTIONS = {
    ActionKind.HOLD,
    ActionKind.DISPATCH_SUBMIT,
    ActionKind.RESTORE_SUBMIT,
    ActionKind.REQUEUE_SUBMIT,
    ActionKind.REPAIR_QUEUE,
    ActionKind.RECOVER_BLOCKED,
    ActionKind.RECOVER_GITPARTNER_WORKTREE,
    ActionKind.HEARTBEAT_ACTIVE_REQUEST,
    ActionKind.CANCEL_STALLED_REQUEST,
}


def build_operator_runtime_audit(
    root: Path,
    config: DaemonConfig,
    snapshot: BoardSnapshot,
    decisions: tuple[GateDecision, ...],
    plan: DaemonPlan,
    solver_liveness: dict[str, object],
    tester_liveness: dict[str, object],
    efficiency: dict[str, object],
) -> dict[str, object]:
    state_dir = root / "TestUtils" / "tester_daemon"
    plugin_state = read_json(state_dir / "operator_plugin_state.json")
    active = list(config.operators)
    draining = list(config.draining_operators)
    observed = list(observed_operators(config))
    active_set = set(active)
    draining_set = set(draining)
    observed_set = set(observed)
    issues: list[str] = []
    warnings: list[str] = []

    if active_set & draining_set:
        issues.append("active and draining operator sets overlap")
    if plugin_state:
        if set(plugin_state.get("active_operators", [])) != active_set:
            issues.append("operator plugin state active set differs from live config")
        if set(plugin_state.get("draining_operators", [])) != draining_set:
            issues.append("operator plugin state draining set differs from live config")
    else:
        warnings.append(
            "operator plugin state is unavailable; generation consistency was not audited"
        )

    board_ops = {row.op for row in snapshot.rows}
    if board_ops and board_ops != observed_set:
        missing = sorted(observed_set - board_ops)
        extra = sorted(board_ops - observed_set)
        issues.append(
            f"board/operator plugin set mismatch missing={missing} extra={extra}"
        )
    elif not board_ops and observed_set:
        warnings.append(
            "board snapshot is empty; membership coverage was not audited on this tick"
        )

    expected_solver_reads = {op for op in observed if solver_thread_for(config, op)}
    solver_reads = read_ops(solver_liveness.get("required_thread_reads", []))
    if solver_reads != expected_solver_reads:
        issues.append(
            "solver required reads differ from observable plugins "
            f"expected={sorted(expected_solver_reads)} actual={sorted(solver_reads)}"
        )
    missing_solver_threads = sorted(
        op for op in observed if not solver_thread_for(config, op)
    )
    if missing_solver_threads:
        issues.append(
            f"observable plugins missing solver threads: {missing_solver_threads}"
        )

    expected_tester_reads = set(casegen_session_ops(config))
    tester_reads = read_ops(tester_liveness.get("required_thread_reads", []))
    if tester_reads != expected_tester_reads:
        issues.append(
            "Tester required reads differ from casegen-capable observable plugins "
            f"expected={sorted(expected_tester_reads)} actual={sorted(tester_reads)}"
        )

    balance = as_dict(efficiency.get("traffic_balance"))
    min_per_operator = int(
        config.policy.get("traffic_balance_min_tests_per_operator", 3) or 3
    )
    expected_balance_applicable = len(active) >= 2
    expected_window = (
        len(active) * min_per_operator + 1 if expected_balance_applicable else 0
    )
    if bool(balance.get("applicable", False)) != expected_balance_applicable:
        issues.append("traffic balance applicability does not match active k")
    if int(balance.get("active_operator_count", -1) or 0) != len(active):
        issues.append("traffic balance active operator count is stale")
    if int(balance.get("window_size", -1) or 0) != expected_window:
        issues.append(
            f"traffic balance window is not dynamic k*min+1: expected={expected_window} "
            f"actual={balance.get('window_size')}"
        )
    for field in ("counts", "debt"):
        keys = set(as_dict(balance.get(field)).keys())
        if keys != active_set:
            issues.append(f"traffic balance {field} keys differ from active plugins")

    submit_gap = as_dict(efficiency.get("completion_to_next_submit"))
    if bool(submit_gap.get("applicable", False)) != expected_balance_applicable:
        issues.append("completion-to-next-submit applicability does not match active k")
    if int(submit_gap.get("active_operator_count", -1) or 0) != len(active):
        issues.append("completion-to-next-submit active operator count is stale")

    generation = int(plugin_state.get("generation", 0) or 0)
    scheduler_balance = as_dict(plan.scheduler_state.get("traffic_balance"))
    if (
        plugin_state
        and scheduler_balance
        and int(scheduler_balance.get("operator_set_generation", 0) or 0) != generation
    ):
        issues.append(
            "scheduler balance generation differs from operator plugin generation"
        )
    balance_generation = int(balance.get("operator_set_generation", 0) or 0)
    if (
        plugin_state
        and expected_balance_applicable
        and balance_generation != generation
    ):
        issues.append(
            "efficiency balance generation differs from operator plugin generation"
        )
    combined_metric_epoch = metric_epoch_for_config(root, config)
    metric_epoch = (
        combined_metric_epoch.isoformat(timespec="seconds")
        if combined_metric_epoch is not None
        else str(plugin_state.get("metrics_epoch_at", "") or "")
    )
    if (
        plugin_state
        and expected_balance_applicable
        and str(balance.get("metric_epoch_at", "") or "") != metric_epoch
    ):
        issues.append(
            "efficiency balance metric epoch differs from unified workflow epoch"
        )
    if (
        plugin_state
        and expected_balance_applicable
        and str(submit_gap.get("metric_epoch_at", "") or "") != metric_epoch
    ):
        issues.append("submit-gap metric epoch differs from unified workflow epoch")

    illegal_drain_actions = [
        f"{decision.row.op}:{decision.action.value}"
        for decision in decisions
        if decision.row.op in draining_set
        and decision.action not in DRAIN_ALLOWED_ACTIONS
    ]
    if illegal_drain_actions:
        issues.append(
            f"detached plugins expose expansion actions: {illegal_drain_actions}"
        )
    if plan.selected and plan.selected.row.op not in observed_set:
        issues.append(
            f"scheduler selected non-observable plugin: {plan.selected.row.op}"
        )

    if len(active) < 2:
        warnings.append(
            "cross-operator balance and submit-gap acceptance are intentionally disabled for k<2"
        )

    return {
        "schema_version": 1,
        "captured_at": snapshot.captured_at,
        "ok": not issues,
        "active_operators": active,
        "draining_operators": draining,
        "observed_operators": observed,
        "active_operator_count": len(active),
        "operator_set_generation": generation,
        "metric_epoch_at": metric_epoch,
        "expected_balance_window_size": expected_window,
        "board_operators": sorted(board_ops),
        "solver_required_read_operators": sorted(solver_reads),
        "tester_required_read_operators": sorted(tester_reads),
        "issues": issues,
        "warnings": warnings,
    }


def write_operator_runtime_audit(root: Path, audit: dict[str, object]) -> None:
    state_dir = root / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "operator_runtime_audit.json").write_text(
        json.dumps(audit, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    lines = [
        "# Operator Runtime Audit",
        "",
        f"- captured_at: {audit.get('captured_at', '-')}",
        f"- ok: {audit.get('ok', False)}",
        f"- generation: {audit.get('operator_set_generation', '-')}",
        f"- active: {', '.join(audit.get('active_operators', [])) or '-'}",
        f"- draining: {', '.join(audit.get('draining_operators', [])) or '-'}",
        f"- observed: {', '.join(audit.get('observed_operators', [])) or '-'}",
        f"- expected_balance_window_size: {audit.get('expected_balance_window_size', '-')}",
    ]
    for heading, key in (("Issues", "issues"), ("Warnings", "warnings")):
        values = audit.get(key, [])
        if isinstance(values, list) and values:
            lines.extend(["", f"## {heading}", ""])
            lines.extend(f"- {value}" for value in values)
    (state_dir / "OPERATOR_RUNTIME_AUDIT.md").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )


def read_ops(raw: object) -> set[str]:
    if not isinstance(raw, list):
        return set()
    return {
        str(item.get("op", "") or "")
        for item in raw
        if isinstance(item, dict) and str(item.get("op", "") or "")
    }


def as_dict(raw: object) -> dict[str, Any]:
    return raw if isinstance(raw, dict) else {}


def read_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}
