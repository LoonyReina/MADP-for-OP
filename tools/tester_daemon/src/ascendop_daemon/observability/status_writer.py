from __future__ import annotations

import json
import os
import time
from pathlib import Path

from ascendop_daemon.core.atomic_io import write_json_atomic
from ascendop_daemon.legacy.efficiency import build_efficiency_snapshot, write_efficiency_files
from ascendop_daemon.observability.iteration_quality import build_iteration_quality, write_iteration_quality_files
from ascendop_daemon.core.models import ActionKind, BoardSnapshot, DaemonConfig, DaemonPlan, GateDecision, TransportObservation, config_seasons
from ascendop_daemon.registry.operator_runtime_audit import build_operator_runtime_audit, write_operator_runtime_audit
from ascendop_daemon.observability.performance_history import build_performance_history, write_performance_history_files
from ascendop_daemon.automation.solver_liveness import build_solver_liveness, write_solver_liveness_files
from ascendop_daemon.automation.tester_liveness import build_tester_liveness, write_tester_liveness_files
from ascendop_daemon.legacy.timeline import update_timeline_files


_PERFORMANCE_CACHE: dict[tuple[str, str, tuple[str, ...], int], tuple[float, dict[str, object]]] = {}
_ITERATION_QUALITY_CACHE: dict[tuple[str, tuple[str, ...]], tuple[float, dict[str, object]]] = {}


def cached_performance_history(root: Path, config: DaemonConfig) -> dict[str, object]:
    limit = int(config.policy.get("performance_history_limit", 40) or 40)
    refresh_seconds = max(
        0.0,
        float(config.policy.get("performance_history_refresh_seconds", 10.0) or 10.0),
    )
    key = (str(root.resolve()), ",".join(config_seasons(config)), tuple(config.operators), limit)
    now = time.monotonic()
    cached = _PERFORMANCE_CACHE.get(key)
    if cached and refresh_seconds > 0 and now - cached[0] < refresh_seconds:
        return cached[1]
    performance = build_performance_history(root, config, limit)
    _PERFORMANCE_CACHE[key] = (now, performance)
    return performance


def cached_iteration_quality(root: Path, config: DaemonConfig) -> dict[str, object]:
    refresh_seconds = max(
        0.0,
        float(config.policy.get("iteration_quality_refresh_seconds", 10.0) or 10.0),
    )
    key = (str(root.resolve()), tuple(config.operators))
    now = time.monotonic()
    cached = _ITERATION_QUALITY_CACHE.get(key)
    if cached and refresh_seconds > 0 and now - cached[0] < refresh_seconds:
        return cached[1]
    quality = build_iteration_quality(root, config)
    _ITERATION_QUALITY_CACHE[key] = (now, quality)
    return quality


class StatusWriter:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.state_dir = root / "TestUtils" / "tester_daemon"
        self.last_timings: dict[str, float] = {}

    def write(
        self,
        snapshot: BoardSnapshot,
        decisions: tuple[GateDecision, ...],
        plan: DaemonPlan,
        mode: str,
        config: DaemonConfig,
        resource_leases: tuple[dict[str, object], ...] = (),
        action_liveness: dict[str, object] | None = None,
    ) -> None:
        timings: dict[str, float] = {}
        self.state_dir.mkdir(parents=True, exist_ok=True)
        stage_started = time.perf_counter()
        solver_liveness = build_solver_liveness(self.root, config, decisions, snapshot.captured_at)
        timings["solver_liveness_seconds"] = time.perf_counter() - stage_started
        stage_started = time.perf_counter()
        tester_liveness = build_tester_liveness(self.root, config, decisions, snapshot.captured_at)
        timings["tester_liveness_seconds"] = time.perf_counter() - stage_started
        stage_started = time.perf_counter()
        timeline = update_timeline_files(
            self.root,
            config,
            snapshot,
            decisions,
            plan,
            resource_leases,
            action_liveness,
        )
        timings["timeline_seconds"] = time.perf_counter() - stage_started
        stage_started = time.perf_counter()
        efficiency = build_efficiency_snapshot(
            self.root,
            config,
            decisions,
            snapshot.transport,
            resource_leases,
            snapshot.captured_at,
            timeline=timeline,
        )
        timings["efficiency_seconds"] = time.perf_counter() - stage_started
        operator_runtime_audit = build_operator_runtime_audit(
            self.root,
            config,
            snapshot,
            decisions,
            plan,
            solver_liveness,
            tester_liveness,
            efficiency,
        )
        write_operator_runtime_audit(self.root, operator_runtime_audit)
        stage_started = time.perf_counter()
        performance = cached_performance_history(self.root, config)
        timings["performance_history_seconds"] = time.perf_counter() - stage_started
        stage_started = time.perf_counter()
        iteration_quality = cached_iteration_quality(self.root, config)
        timings["iteration_quality_seconds"] = time.perf_counter() - stage_started
        stage_started = time.perf_counter()
        thread_observations = read_json_dict(self.state_dir / "solver_thread_observations.json")
        tester_thread_observations = read_json_dict(self.state_dir / "tester_thread_observations.json")
        if not tester_thread_observations:
            tester_thread_observations = thread_observations
        (self.state_dir / "DAEMON_STATUS.md").write_text(
            self.render(
                snapshot,
                decisions,
                plan,
                mode,
                resource_leases,
                action_liveness,
                solver_liveness=solver_liveness,
                efficiency=efficiency,
                performance=performance,
                thread_observations=thread_observations,
                tester_liveness=tester_liveness,
                timeline=timeline,
            ),
            encoding="utf-8",
        )
        state = {
            "captured_at": snapshot.captured_at,
            "mode": mode,
            "selected": serialize_decision(plan.selected),
            "decisions": [serialize_decision(d) for d in decisions],
            "transport": [serialize_transport(obs) for obs in snapshot.transport],
            "scheduler": plan.scheduler_state,
            "resource_leases": list(resource_leases),
            "action_liveness": action_liveness or {},
            "solver_liveness": solver_liveness,
            "tester_liveness": tester_liveness,
            "solver_thread_observations": thread_observations,
            "efficiency": efficiency,
            "operator_runtime_audit": operator_runtime_audit,
            "performance": performance,
            "iteration_quality": iteration_quality,
            "timeline": timeline,
        }
        (self.state_dir / "state.json").write_text(
            json.dumps(state, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        (self.state_dir / "ACTION_PLAN.md").write_text(
            self.render_action_plan(plan, mode, action_liveness),
            encoding="utf-8",
        )
        heartbeat = {
            "time": snapshot.captured_at,
            "pid": os.getpid(),
            "mode": mode,
            "season": config.season,
            "seasons": list(config_seasons(config)),
            "transport": config.transport,
            "operators": list(config.operators),
            "selected_action_id": plan.selected.action_id if plan.selected else "",
            "transport_observation_count": len(snapshot.transport),
            "terminal_transport_observation_count": sum(1 for obs in snapshot.transport if obs.terminal),
            "resource_lease_count": len(resource_leases),
            "action_liveness": action_liveness or {},
            "solver_liveness": {
                "required_thread_read_count": len(solver_liveness.get("required_thread_reads", [])),
                "active_solver_gate_count": len(solver_liveness.get("active_solver_gates", [])),
                "stale_solver_gate_count": len(solver_liveness.get("stale_solver_gates", [])),
            },
            "tester_liveness": {
                "required_thread_read_count": len(tester_liveness.get("required_thread_reads", [])),
                "active_tester_gate_count": len(tester_liveness.get("active_tester_gates", [])),
                "stale_tester_gate_count": len(tester_liveness.get("stale_tester_gates", [])),
                "missing_tester_thread_count": len(tester_liveness.get("missing_tester_threads", [])),
            },
            "solver_thread_observations": summarize_thread_observations(
                thread_observations,
                role="solver",
            ),
            "tester_thread_observations": summarize_thread_observations(
                tester_thread_observations,
                role="tester",
            ),
            "efficiency": {
                "resource_idle": efficiency.get("resource_idle", False),
                "tester_owned_work": efficiency.get("tester_owned_work", []),
                "recent_selected_balance": efficiency.get("recent_selected_balance", {}),
                "recent_dispatch_balance": efficiency.get("recent_dispatch_balance", {}),
                "traffic_balance": efficiency.get("traffic_balance", {}),
                "balance_recovery": efficiency.get("balance_recovery", {}),
                "completion_to_next_submit": efficiency.get("completion_to_next_submit", {}),
            },
            "operator_runtime_audit": operator_runtime_audit,
            "performance": summarize_performance_heartbeat(performance),
            "timeline": {
                "new_event_count": timeline.get("new_event_count", 0),
                "total_event_count": timeline.get("total_event_count", 0),
                "test_idle_window_seconds": timeline.get("test_idle_window_seconds"),
                "max_result_to_solver_ack_seconds": timeline.get("max_result_to_solver_ack_seconds"),
                "max_result_to_next_dispatch_seconds": timeline.get("max_result_to_next_dispatch_seconds"),
                "completion_to_next_submit": timeline.get("completion_to_next_submit", {}),
            },
        }
        write_json_atomic(self.state_dir / "daemon_heartbeat.json", heartbeat)
        write_solver_liveness_files(self.root, solver_liveness)
        write_tester_liveness_files(self.root, tester_liveness)
        write_efficiency_files(self.root, efficiency)
        write_performance_history_files(self.root, performance)
        write_iteration_quality_files(self.root, iteration_quality)
        timings["render_and_write_seconds"] = time.perf_counter() - stage_started
        self.last_timings = timings
    def render(
        self,
        snapshot: BoardSnapshot,
        decisions: tuple[GateDecision, ...],
        plan: DaemonPlan,
        mode: str,
        resource_leases: tuple[dict[str, object], ...] = (),
        action_liveness: dict[str, object] | None = None,
        solver_liveness: dict[str, object] | None = None,
        efficiency: dict[str, object] | None = None,
        performance: dict[str, object] | None = None,
        thread_observations: dict[str, object] | None = None,
        tester_liveness: dict[str, object] | None = None,
        timeline: dict[str, object] | None = None,
    ) -> str:
        lines = [
            "# AscendOP Tester Daemon Status",
            "",
            f"- captured_at: {snapshot.captured_at}",
            f"- mode: {mode}",
            f"- board_command: `{' '.join(snapshot.command)}`",
            "",
            "| op | gate | owner | action | priority | reason |",
            "|---|---|---|---|---:|---|",
        ]
        for decision in decisions:
            row = decision.row
            lines.append(
                f"| {row.op} | {row.gate_stage} | {row.next_owner} | "
                f"{decision.action.value} | {decision.priority} | {escape_cell(decision.reason)} |"
            )
        lines.extend(["", "## Transport Observations", ""])
        if snapshot.transport:
            lines.extend(
                [
                    "| op | version | request | state | client | terminal | stalled | remote | status | heartbeat |",
                    "|---|---|---|---|---|---:|---:|---|---|---|",
                ]
            )
            for obs in snapshot.transport:
                lines.append(
                    f"| {obs.op} | {obs.test_version} | {obs.request_id or '-'} | "
                    f"{obs.state or '-'} | {obs.client_state or '-'} | "
                    f"{display_bool(obs.terminal)} | {display_bool(obs.stalled)} | "
                    f"{obs.remote_feedback_status or '-'} | {escape_cell(obs.output_status_path or '-')} | "
                    f"{escape_cell(obs.heartbeat_path or '-')} |"
                )
        else:
            lines.append("- none")
        lines.extend(["", "## Resource Leases", ""])
        if resource_leases:
            lines.extend(
                [
                    "| resource | op | action_id | acquired | expires |",
                    "|---|---|---|---|---|",
                ]
            )
            for lease in resource_leases:
                lines.append(
                    f"| {escape_cell(str(lease.get('resource_id', '-')))} | "
                    f"{escape_cell(str(lease.get('op', '-')))} | "
                    f"{escape_cell(str(lease.get('action_id', '-')))} | "
                    f"{escape_cell(str(lease.get('acquired_at', '-')))} | "
                    f"{escape_cell(str(lease.get('expires_at', '-')))} |"
                )
        else:
            lines.append("- none")
        lines.extend(["", "## Solver Liveness", ""])
        if solver_liveness:
            stale = solver_liveness.get("stale_solver_gates", [])
            active = solver_liveness.get("active_solver_gates", [])
            reads = solver_liveness.get("required_thread_reads", [])
            lines.append(f"- required_thread_reads: {len(reads) if isinstance(reads, list) else 0}")
            lines.append(f"- active_solver_gates: {len(active) if isinstance(active, list) else 0}")
            lines.append(f"- stale_solver_gates: {len(stale) if isinstance(stale, list) else 0}")
            if isinstance(active, list):
                for item in active:
                    if isinstance(item, dict):
                        lines.append(
                            f"- {item.get('op', '-')}: status={item.get('status', '-')} "
                            f"age_seconds={item.get('age_seconds', '-')} "
                            f"native={item.get('native_status', '-') or '-'} "
                            f"ide_visible={item.get('ide_panel_visible', False)}"
                        )
            thread_data = thread_observations or {}
            threads = thread_data.get("threads", []) if isinstance(thread_data.get("threads"), list) else []
            lines.extend(["", "## Solver Thread Observations", ""])
            if threads:
                lines.extend(
                    [
                        "| op | latest_turn | status | ide_visible | idle_seconds | latest_activity |",
                        "|---|---|---|---:|---:|---|",
                    ]
                )
                for item in threads:
                    if not isinstance(item, dict):
                        continue
                    lines.append(
                        f"| {item.get('op', '-')} | {item.get('latest_turn_id', '-') or '-'} | "
                        f"{item.get('latest_turn_status', '-') or '-'} | "
                        f"{bool(item.get('ide_panel_visible'))} | "
                        f"{item.get('idle_seconds', '-') if 'idle_seconds' in item else '-'} | "
                        f"{item.get('latest_activity_at', '-') or '-'} |"
                    )
            else:
                lines.append("- none")
        else:
            lines.append("- none")
        lines.extend(["", "## Tester Liveness", ""])
        if tester_liveness:
            stale_tester = tester_liveness.get("stale_tester_gates", [])
            active_tester = tester_liveness.get("active_tester_gates", [])
            tester_reads = tester_liveness.get("required_thread_reads", [])
            missing = tester_liveness.get("missing_tester_threads", [])
            lines.append(f"- required_thread_reads: {len(tester_reads) if isinstance(tester_reads, list) else 0}")
            lines.append(f"- missing_tester_threads: {len(missing) if isinstance(missing, list) else 0}")
            lines.append(f"- active_tester_gates: {len(active_tester) if isinstance(active_tester, list) else 0}")
            lines.append(f"- stale_tester_gates: {len(stale_tester) if isinstance(stale_tester, list) else 0}")
            if isinstance(active_tester, list):
                for item in active_tester:
                    if isinstance(item, dict):
                        lines.append(
                            f"- {item.get('op', '-')}: status={item.get('status', '-')} "
                            f"age_seconds={item.get('age_seconds', '-')} "
                            f"thread={item.get('thread_id') or 'MISSING'}"
                        )
        else:
            lines.append("- none")
        lines.extend(["", "## Test Efficiency", ""])
        if efficiency:
            lines.append(f"- resource_idle: {efficiency.get('resource_idle', False)}")
            balance = efficiency.get("traffic_balance", {})
            if isinstance(balance, dict) and balance:
                lines.append(
                    "- traffic_balance: "
                    f"window={balance.get('window_size', '-')} "
                    f"sample_count={balance.get('sample_count', '-')} "
                    f"ok={balance.get('ok', False)} "
                    f"debt={balance.get('debt', {})}"
                )
            recovery = efficiency.get("balance_recovery", {})
            if isinstance(recovery, dict) and recovery.get("plans"):
                labels = []
                for item in recovery.get("plans", []):
                    if not isinstance(item, dict):
                        continue
                    labels.append(
                        f"{item.get('op', '-')}:debt={item.get('debt', '-')} "
                        f"state={item.get('state', '-')} "
                        f"compensated={item.get('compensated', False)}"
                    )
                if labels:
                    lines.append("- balance_recovery: " + "; ".join(labels))
                uncompensated = recovery.get("uncompensated_ops", [])
                lines.append(f"- balance_uncompensated_ops: {uncompensated}")
            operators = efficiency.get("operators", {})
            if isinstance(operators, dict):
                for op, data in operators.items():
                    if not isinstance(data, dict):
                        continue
                    result = data.get("latest_result", {}) if isinstance(data.get("latest_result"), dict) else {}
                    case = data.get("latest_case", {}) if isinstance(data.get("latest_case"), dict) else {}
                    lines.append(
                        f"- {op}: latest_result={result.get('test_version', '-')} "
                        f"weighted_us={result.get('weighted_time_us', '-')} "
                        f"case={case.get('case_version', '-')} "
                        f"usage={case.get('usage_count', '-')}/{case.get('max_usage', '-')}"
                    )
        else:
            lines.append("- none")
        lines.extend(["", "## Performance History", ""])
        if performance:
            operators = performance.get("operators", {})
            if isinstance(operators, dict):
                for op, data in operators.items():
                    if not isinstance(data, dict):
                        continue
                    latest = data.get("latest_pass", {}) if isinstance(data.get("latest_pass"), dict) else {}
                    active = data.get("active_release", {}) if isinstance(data.get("active_release"), dict) else {}
                    lines.append(
                        f"- {op}: latest_pass={latest.get('test_version', '-') if latest else '-'} "
                        f"weighted_us={latest.get('weighted_time_us', '-') if latest else '-'} "
                        f"active_release={data.get('active_release_name', '-') or '-'} "
                        f"release_weighted_us={active.get('weighted_time_us', '-') if active else '-'} "
                        f"failure_streak={data.get('failure_streak', 0)}"
                    )
        else:
            lines.append("- none")
        lines.extend(["", "## Scheduler Gaps", ""])
        if timeline:
            lines.append(f"- new_event_count: {timeline.get('new_event_count', 0)}")
            lines.append(f"- total_event_count: {timeline.get('total_event_count', 0)}")
            lines.append(f"- test_idle_window_seconds: {timeline.get('test_idle_window_seconds', '-')}")
            lines.append(
                f"- max_result_to_solver_ack_seconds: {timeline.get('max_result_to_solver_ack_seconds', '-')}"
            )
            lines.append(
                f"- max_result_to_next_dispatch_seconds: {timeline.get('max_result_to_next_dispatch_seconds', '-')}"
            )
            submit_gap = timeline.get("completion_to_next_submit", {})
            if isinstance(submit_gap, dict) and submit_gap:
                lines.append(
                    "- completion_to_next_submit: "
                    f"window={submit_gap.get('window_size', '-')} "
                    f"sample_count={submit_gap.get('sample_count', '-')} "
                    f"ok={submit_gap.get('ok', False)} "
                    f"max_gap_seconds={submit_gap.get('max_gap_seconds', '-')} "
                    f"violations={submit_gap.get('violation_count', 0)}"
                )
        else:
            lines.append("- none")
        lines.extend(["", "## Action Liveness", ""])
        if action_liveness:
            for key, value in action_liveness.items():
                lines.append(f"- {key}: {value}")
        else:
            lines.append("- none")
        lines.extend(["", "## Scheduler", ""])
        if plan.scheduler_state:
            for key, value in plan.scheduler_state.items():
                lines.append(f"- {key}: {value}")
        else:
            lines.append("- none")
        lines.extend(["", "## Selected Action", ""])
        if plan.selected:
            lines.append(f"- op: {plan.selected.row.op}")
            lines.append(f"- action: {plan.selected.action.value}")
            lines.append(f"- reason: {plan.selected.reason}")
            if plan.selected.command:
                lines.append(f"- command: `{plan.selected.command}`")
        else:
            lines.append("- none")
        lines.append("")
        return "\n".join(lines)

    def render_action_plan(
        self,
        plan: DaemonPlan,
        mode: str,
        action_liveness: dict[str, object] | None = None,
    ) -> str:
        lines = [
            "# Tester Daemon Action Plan",
            "",
            "This file is generated by shadow/advisory mode. It is not proof that execute should run.",
            "",
        ]
        if not plan.selected:
            lines.append("- selected: none")
            lines.append("")
            return "\n".join(lines)
        selected = plan.selected
        lines.extend(
            [
                f"- op: {selected.row.op}",
                f"- gate_stage: {selected.row.gate_stage}",
                f"- action: {selected.action.value}",
                f"- priority: {selected.priority}",
                f"- reason: {selected.reason}",
                f"- action_id: `{display_inline(selected.action_id)}`",
                f"- board_next_command: `{display_inline(selected.row.next_command)}`",
            ]
        )
        if selected.command:
            lines.append("- executable: yes")
            if mode in {"shadow", "advisory"}:
                lines.append(
                    "- why_not_executed: daemon is in shadow/advisory mode; use controlled execute only after checking action_id drift"
                )
        else:
            lines.append("- executable: no")
            if selected.action == ActionKind.NOTIFY_SOLVER:
                lines.append(
                    "- why_not_executed: board owner is solver; daemon records wakeup state instead of consuming tester hardware"
                )
            elif selected.action == ActionKind.NOTIFY_TESTER_CASEGEN:
                lines.append(
                    "- why_not_executed: board owner is Tester casegen; daemon records tester trigger state instead of generating cases"
                )
            else:
                lines.append("- why_not_executed: selected action has no harness command")
        if action_liveness:
            lines.append(f"- liveness_repeat_count: {action_liveness.get('repeat_count', '-')}")
            lines.append(f"- liveness_age_seconds: {action_liveness.get('age_seconds', '-')}")
            lines.append(f"- liveness_stagnant: {action_liveness.get('stagnant', '-')}")
        if selected.command:
            lines.append(f"- command: `{selected.command}`")
            lines.append("")
            lines.append("Dry-run execute:")
            lines.append("")
            lines.append("```powershell")
            lines.append(
                "python tools\\tester_daemon\\daemon.py tick --config "
                "tools\\tester_daemon\\config\\s5_910b_gitpartner_glugrad_bitwise.json "
                "--mode execute --dry-run-execute"
            )
            lines.append("```")
            lines.append("")
            lines.append("Controlled execute with drift check:")
            lines.append("")
            lines.append("```powershell")
            lines.append(
                "python tools\\tester_daemon\\daemon.py tick --config "
                "tools\\tester_daemon\\config\\s5_910b_gitpartner_glugrad_bitwise.json "
                f"--mode execute --expected-action-id {powershell_quote(selected.action_id)}"
            )
            lines.append("```")
        lines.append("")
        return "\n".join(lines)


def summarize_performance_heartbeat(performance: dict[str, object]) -> dict[str, object]:
    operators = performance.get("operators", {})
    summary: dict[str, object] = {}
    if not isinstance(operators, dict):
        return summary
    for op, data in operators.items():
        if not isinstance(data, dict):
            continue
        latest = data.get("latest_pass", {}) if isinstance(data.get("latest_pass"), dict) else {}
        active = data.get("active_release", {}) if isinstance(data.get("active_release"), dict) else {}
        summary[str(op)] = {
            "latest_pass": latest.get("test_version", "") if latest else "",
            "latest_weighted_us": latest.get("weighted_time_us") if latest else None,
            "active_release": data.get("active_release_name", ""),
            "active_release_weighted_us": active.get("weighted_time_us") if active else None,
            "failure_streak": data.get("failure_streak", 0),
            "latest_vs_best_recent_same_case_pct": data.get("latest_vs_best_recent_same_case_pct"),
        }
    return summary


def summarize_thread_observations(
    thread_observations: dict[str, object],
    *,
    role: str | None = None,
) -> dict[str, object]:
    threads = thread_observations.get("threads", [])
    summary: dict[str, object] = {}
    if not isinstance(threads, list):
        return summary
    for item in threads:
        if not isinstance(item, dict):
            continue
        item_role = str(item.get("role", "") or "")
        if role and item_role and item_role != role:
            continue
        op = str(item.get("op", "") or "")
        if not op:
            continue
        summary[op] = {
            "thread_id": item.get("thread_id", ""),
            "latest_turn_id": item.get("latest_turn_id", ""),
            "latest_turn_status": item.get("latest_turn_status", ""),
            "ide_panel_visible": bool(item.get("ide_panel_visible")),
            "idle_seconds": item.get("idle_seconds"),
            "latest_activity_at": item.get("latest_activity_at", ""),
        }
    return summary


def read_json_dict(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def serialize_decision(decision: GateDecision | None) -> dict[str, object] | None:
    if decision is None:
        return None
    return {
        "op": decision.row.op,
        "gate_stage": decision.row.gate_stage,
        "next_owner": decision.row.next_owner,
        "action": decision.action.value,
        "reason": decision.reason,
        "command": decision.command,
        "priority": decision.priority,
        "blocks_operator": decision.blocks_operator,
        "action_id": decision.action_id,
    }


def serialize_transport(observation: TransportObservation) -> dict[str, object]:
    return {
        "op": observation.op,
        "test_version": observation.test_version,
        "request_id": observation.request_id,
        "heartbeat_path": observation.heartbeat_path,
        "output_status_path": observation.output_status_path,
        "state": observation.state,
        "client_state": observation.client_state,
        "client_updated_at": observation.client_updated_at,
        "client_progress_observed_at": observation.client_progress_observed_at,
        "terminal": observation.terminal,
        "stalled": observation.stalled,
        "remote_feedback_status": observation.remote_feedback_status,
        "stall_reason": observation.stall_reason,
        "first_observed_at_utc": observation.first_observed_at_utc,
        "observed_at_utc": observation.observed_at_utc,
        "last_feedback_at_utc": observation.last_feedback_at_utc,
        "elapsed_without_feedback_seconds": observation.elapsed_without_feedback_seconds,
        "elapsed_without_remote_feedback_seconds": observation.elapsed_without_remote_feedback_seconds,
        "relay_publish_verify": observation.relay_publish_verify,
        "client_ssh": observation.client_ssh,
        "summary": observation.summary,
    }


def escape_cell(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


def display_bool(value: bool | None) -> str:
    if value is None:
        return "-"
    return "yes" if value else "no"


def display_inline(text: str) -> str:
    return text.replace("\\", "\\\\").replace("`", "\\`")


def powershell_quote(text: str) -> str:
    escaped = text.replace("'", "''")
    return f"'{escaped}'"
