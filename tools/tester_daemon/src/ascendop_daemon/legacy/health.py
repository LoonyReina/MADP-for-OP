from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ascendop_daemon.runtime.config_loader import load_config
from ascendop_daemon.runtime.control import read_stop_request
from ascendop_daemon.runtime.locking import process_alive, read_lock_pid
from ascendop_daemon.legacy.native_relay_outbox import (
    NATIVE_RELAY_CONSUMER_STATUS,
    account_usage_limit_cooldown_summary,
    active_claims_by_entry,
    read_native_relay_claims,
    tester_casegen_active_covering_ack,
)
from ascendop_daemon.storage.state_reader import StateReader
from ascendop_daemon.legacy.timeline import TIMELINE_FILE, build_gap_snapshot, read_timeline_events
from ascendop_daemon.automation.trigger_state import normalized_trigger_status, read_tester_trigger_ack_state, read_trigger_ack_state
from ascendop_daemon.automation.worker_liveness import worker_process_status

SOLVER_OBSERVATION_STALE_SECONDS = 300
NATIVE_RELAY_CONSUMER_STALE_SECONDS = 90
CRITICAL_THREAD_OBSERVER_MIN_STALE_SECONDS = 15
SOLVER_ACK_ACTIVE_STATUSES = {"sent", "delivered", "acked", "active", "completed"}


@dataclass(frozen=True)
class HealthReport:
    ok: bool
    issues: tuple[str, ...]
    observations: tuple[str, ...]

    def render(self) -> str:
        lines = ["HEALTH_OK" if self.ok else "HEALTH_FAIL"]
        if self.issues:
            lines.append("")
            lines.append("Issues:")
            lines.extend(f"- {issue}" for issue in self.issues)
        if self.observations:
            lines.append("")
            lines.append("Observations:")
            lines.extend(f"- {observation}" for observation in self.observations)
        return "\n".join(lines) + "\n"


def check_health(root: Path, max_heartbeat_age_seconds: int = 300) -> HealthReport:
    state_dir = root / "TestUtils" / "tester_daemon"
    heartbeat_path = state_dir / "daemon_heartbeat.json"
    issues: list[str] = []
    observations: list[str] = []
    try:
        free_bytes = int(shutil.disk_usage(state_dir if state_dir.exists() else root).free)
    except OSError:
        free_bytes = 0
    free_mb = free_bytes // (1024 * 1024)
    maintenance = read_json(state_dir / "runtime_maintenance.json")
    low_disk_threshold_mb = int(maintenance.get("low_disk_threshold_mb", 64) or 64)
    observations.append(f"runtime_disk_free_mb={free_mb}")
    if free_mb < low_disk_threshold_mb:
        issues.append(
            "runtime disk space low: "
            f"free_mb={free_mb} threshold_mb={low_disk_threshold_mb} "
            f"maintenance_status={maintenance.get('status', 'missing')}"
        )
    heartbeat_stale_issue = ""
    leases = read_json(state_dir / "leases.json")
    lease_list = leases.get("leases", []) if isinstance(leases.get("leases"), list) else []
    stale_lease_pids: list[int] = []
    active_lease_pids: list[int] = []
    for lease in lease_list:
        if not isinstance(lease, dict):
            continue
        pid = int(lease.get("pid", 0) or 0)
        action_id = str(lease.get("action_id", "") or "")
        status = worker_process_status(root, action_id, {"pid": pid}) if action_id else None
        if status and status.active:
            active_lease_pids.append(status.effective_pid)
            if status.adopted_child:
                observations.append(
                    f"resource_lease_adopted_process={status.effective_pid} old_pid={pid} reason={status.reason}"
                )
        elif not action_id and pid > 0 and process_alive(pid):
            active_lease_pids.append(pid)
        elif pid > 0 and not process_alive(pid):
            stale_lease_pids.append(pid)
    active_execute = bool(active_lease_pids)

    heartbeat = read_json(heartbeat_path)
    if not heartbeat:
        if active_execute:
            observations.append(f"missing heartbeat but active execute pid alive: {active_lease_pids[0]}")
        else:
            issues.append(f"missing daemon heartbeat: {relpath(heartbeat_path, root)}")
    else:
        heartbeat_time = parse_timestamp(str(heartbeat.get("time", "")))
        if heartbeat_time is None:
            issues.append("daemon heartbeat has no parseable time")
        else:
            age = max(0, int((datetime.now(timezone.utc) - heartbeat_time).total_seconds()))
            observations.append(f"heartbeat_age_seconds={age}")
            if max_heartbeat_age_seconds > 0 and age > max_heartbeat_age_seconds:
                if active_execute:
                    observations.append(
                        f"heartbeat stale but active execute pid alive: age_seconds={age} pid={active_lease_pids[0]}"
                    )
                else:
                    heartbeat_stale_issue = (
                        f"daemon heartbeat stale: age_seconds={age} threshold={max_heartbeat_age_seconds}"
                    )
    if heartbeat:
        observations.append(f"mode={heartbeat.get('mode', '-')}")
        observations.append(f"selected_action_id={heartbeat.get('selected_action_id', '-')}")
        liveness = heartbeat.get("action_liveness")
        if isinstance(liveness, dict):
            observations.append(f"action_repeat_count={liveness.get('repeat_count', '-')}")
            observations.append(f"action_age_seconds={liveness.get('age_seconds', '-')}")
            if bool(liveness.get("stagnant")):
                message = (
                    "selected action stagnant: "
                    f"action_id={liveness.get('selected_action_id', '')} "
                    f"age_seconds={liveness.get('age_seconds', '-')}"
                )
                if active_execute:
                    observations.append(message + f" but active execute pid alive: {active_lease_pids[0]}")
                else:
                    issues.append(message)

    critical_observer = read_json(state_dir / "critical_thread_observer.json")
    if critical_observer:
        observer_time = parse_timestamp(str(critical_observer.get("updated_at", "")))
        try:
            observer_interval = max(
                0.1,
                float(critical_observer.get("interval_seconds", 0) or 0),
            )
        except (TypeError, ValueError):
            observer_interval = 0.1
        observer_threshold = max(
            CRITICAL_THREAD_OBSERVER_MIN_STALE_SECONDS,
            int(observer_interval * 10),
        )
        observer_age = (
            max(
                0,
                int((datetime.now(timezone.utc) - observer_time).total_seconds()),
            )
            if observer_time is not None
            else None
        )
        observations.append(
            "critical_thread_observer="
            f"status={critical_observer.get('status', '-')} "
            f"age_seconds={observer_age if observer_age is not None else 'missing'} "
            f"thread_count={critical_observer.get('thread_count', '-')}"
        )
        if str(critical_observer.get("status") or "") != "ok":
            issues.append(
                "critical thread observer failed: "
                f"status={critical_observer.get('status', '-')} "
                f"error={critical_observer.get('error', '-')}"
            )
        elif observer_age is None or observer_age > observer_threshold:
            issues.append(
                "critical thread observer stale: "
                f"age_seconds={observer_age if observer_age is not None else 'missing'} "
                f"threshold={observer_threshold}"
            )

    operator_runtime_audit = read_json(state_dir / "operator_runtime_audit.json")
    if not operator_runtime_audit:
        if isinstance(heartbeat.get("operator_runtime_audit"), dict):
            issues.append("missing operator runtime audit")
        else:
            observations.append("operator_runtime_audit=legacy-heartbeat-no-audit")
    else:
        observations.append(
            "operator_runtime_audit="
            f"ok={operator_runtime_audit.get('ok', False)} "
            f"generation={operator_runtime_audit.get('operator_set_generation', '-')} "
            f"active_count={operator_runtime_audit.get('active_operator_count', '-')}"
        )
        for issue in operator_runtime_audit.get("issues", []):
            issues.append(f"operator runtime audit: {issue}")

    if leases:
        lease_count = len(lease_list)
        observations.append(f"resource_lease_count={lease_count}")
        if active_lease_pids:
            observations.append(f"active_execute_pids={','.join(str(pid) for pid in active_lease_pids)}")
        if stale_lease_pids:
            issues.append(
                "stale resource lease pid not alive: "
                + ",".join(str(pid) for pid in sorted(set(stale_lease_pids)))
            )

    stale_workers = read_stale_execute_workers(root, state_dir)
    if stale_workers:
        issues.append(
            "stale execute worker pid not alive: "
            + ",".join(f"{item.get('action_id', '-')}:pid={item.get('pid', '-')}" for item in stale_workers)
        )

    state = read_json(state_dir / "state.json")
    decisions = state.get("decisions", []) if isinstance(state.get("decisions"), list) else []
    current_tester_casegen_ops = {
        str(decision.get("op", "") or "")
        for decision in decisions
        if isinstance(decision, dict) and decision_dict_is_tester_casegen(decision)
    }
    for decision in decisions:
        if not isinstance(decision, dict):
            continue
        reason = str(decision.get("reason", "") or "")
        if "repeated invalid-case/harness-contract rollover" in reason:
            issues.append(
                "workflow blocked by repeated invalid-case/harness-contract rollover: "
                f"{decision.get('op', '-')} {reason}"
            )

    pending_triggers, acked_stale_triggers, active_bridge_triggers = read_pending_solver_triggers(state_dir)
    relay_consumer_fresh = bool(read_app_side_relay_status(state_dir).get("fresh"))
    active_bridge_solver_ops = {
        str(trigger.get("op", "") or "")
        for trigger in active_bridge_triggers
        if isinstance(trigger, dict) and trigger.get("op")
    }
    if pending_triggers:
        target = observations if relay_consumer_fresh else issues
        target.append(
            "solver trigger pending delivery: "
            + ", ".join(
                f"{trigger.get('op', '-')}/{trigger.get('gate_stage', '-')} status={trigger.get('status', '-')}"
                for trigger in pending_triggers
            )
        )
    solver_reconcile_wait = [
        trigger
        for trigger in read_json(state_dir / "solver_trigger_plan.json").get("triggers", [])
        if isinstance(trigger, dict) and trigger.get("status") == "reconcile-wait"
    ]
    if solver_reconcile_wait:
        issues.append(
            "solver delivery awaiting Codex restart reconciliation: "
            + ", ".join(
                f"{trigger.get('op', '-')}/{trigger.get('gate_stage', '-')}"
                for trigger in solver_reconcile_wait
            )
        )
    if acked_stale_triggers:
        observations.append(
            "solver_trigger_plan_stale_but_acked="
            + ",".join(str(trigger.get("op", "-")) for trigger in acked_stale_triggers)
        )
    if active_bridge_triggers:
        observations.append(
            "solver_trigger_bridge_active="
            + ",".join(str(trigger.get("op", "-")) for trigger in active_bridge_triggers)
        )
    pending_tester_triggers, missing_tester_triggers = read_pending_tester_triggers(state_dir)
    if pending_tester_triggers:
        target = observations if relay_consumer_fresh else issues
        target.append(
            "tester casegen trigger pending delivery: "
            + ", ".join(
                f"{trigger.get('op', '-')}/{trigger.get('gate_stage', '-')} status={trigger.get('status', '-')}"
                for trigger in pending_tester_triggers
            )
        )
    tester_reconcile_wait = [
        trigger
        for trigger in read_json(state_dir / "tester_trigger_plan.json").get("triggers", [])
        if isinstance(trigger, dict) and trigger.get("status") == "reconcile-wait"
    ]
    if tester_reconcile_wait:
        issues.append(
            "tester delivery awaiting Codex restart reconciliation: "
            + ", ".join(
                f"{trigger.get('op', '-')}/{trigger.get('gate_stage', '-')}"
                for trigger in tester_reconcile_wait
            )
        )
    if missing_tester_triggers:
        issues.append(
            "tester casegen trigger missing tester_thread_id: "
            + ", ".join(str(trigger.get("op", "-")) for trigger in missing_tester_triggers)
        )
    tester_remote_control_blockers = read_tester_remote_control_blockers(state_dir)
    for blocker in tester_remote_control_blockers:
        issues.append(
            remote_control_blocker_issue(
                str(blocker.get("op", "-") or "-"),
                blocker,
                kind="tester casegen",
            )
        )
    if heartbeat_stale_issue:
        if issues or pending_triggers:
            issues.append(heartbeat_stale_issue)
        else:
            observations.append(
                "daemon heartbeat self-check due: "
                + heartbeat_stale_issue.removeprefix("daemon ")
            )
    add_bridge_health(
        root,
        state_dir,
        issues,
        observations,
        has_pending_triggers=bool(pending_triggers),
        max_heartbeat_age_seconds=max_heartbeat_age_seconds,
    )
    add_relay_capability_health(state_dir, issues, observations)
    solver_session = read_json(state_dir / "solver_session_status.json")
    active_gates: list[dict[str, Any]] = []
    active_gate_by_op: dict[str, dict[str, Any]] = {}
    required_reads: list[dict[str, Any]] = []
    if solver_session:
        ack_state = read_trigger_ack_state(root)
        sent_records = ack_state.get("sent", {}) if isinstance(ack_state.get("sent"), dict) else {}
        required_reads = (
            solver_session.get("required_thread_reads", [])
            if isinstance(solver_session.get("required_thread_reads"), list)
            else []
        )
        raw_active_gates = (
            solver_session.get("active_solver_gates", [])
            if isinstance(solver_session.get("active_solver_gates"), list)
            else []
        )
        active_gates = [
            overlay_solver_gate_with_ack(gate, sent_records)
            for gate in raw_active_gates
            if isinstance(gate, dict)
        ]
        active_gate_by_op = {
            str(gate.get("op", "") or ""): gate
            for gate in active_gates
            if isinstance(gate, dict) and gate.get("op")
        }
        raw_stale_gates = (
            solver_session.get("stale_solver_gates", [])
            if isinstance(solver_session.get("stale_solver_gates"), list)
            else []
        )
        stale_gates = [
            overlay_solver_gate_with_ack(gate, sent_records)
            for gate in raw_stale_gates
            if isinstance(gate, dict)
        ]
        observations.append(f"solver_required_thread_reads={len(required_reads)}")
        if active_gates:
            observations.append(
                "solver_active_gates="
                + ",".join(
                    f"{gate.get('op', '-')}/{gate.get('gate_stage', '-')}:{gate.get('status', '-')}"
                    for gate in active_gates
                    if isinstance(gate, dict)
                )
            )
            for gate in active_gates:
                if not isinstance(gate, dict):
                    continue
                op = str(gate.get("op", "") or "")
                usage_limit = account_usage_limit_blocker(gate)
                if op and str(gate.get("thread_status_type", "") or "") == "systemError" and not usage_limit:
                    issues.append(
                        "solver same-session recovery required: "
                        f"{op} thread_status_type=systemError"
                    )
                active_gate_status = active_solver_gate_status(gate)
                if op and active_gate_status in {"interrupted", "failed", "cancelled"}:
                    failure_kind = str(gate.get("failure_kind", "") or "")
                    if usage_limit:
                        issues.append(account_usage_limit_issue(op, gate))
                    elif remote_control_blocker(gate):
                        issues.append(remote_control_blocker_issue(op, gate))
                    elif str(gate.get("thread_status_type", "") or "") == "systemError":
                        issues.append(
                            "solver same-session recovery required: "
                            f"{op} status={active_gate_status} thread_status_type=systemError"
                        )
                    elif failure_kind == "native_turn_no_agent_output":
                        no_agent_count = int(gate.get("native_no_agent_output_count", 1) or 1)
                        if no_agent_count >= 2:
                            issues.append(
                                "solver same-session recovery required: "
                                f"{op} status={active_gate_status} failure_kind={failure_kind}"
                            )
                        else:
                            issues.append(
                                "solver trigger completed without agent output; "
                                f"one IDE-visible retry is allowed: {op} status={active_gate_status}"
                            )
                    else:
                        issues.append(
                            "solver-owned workflow has terminal failed active trigger: "
                            f"{op} status={active_gate_status}"
                        )
        if stale_gates:
            reportable_stale_gates = [
                gate
                for gate in stale_gates
                if isinstance(gate, dict)
                and str(gate.get("failure_kind", "") or "") != "native_turn_no_agent_output"
                and not account_usage_limit_blocker(gate)
                and not active_solver_gate_fresh(gate, SOLVER_OBSERVATION_STALE_SECONDS)
            ]
            if reportable_stale_gates:
                issues.append(
                    "solver trigger stale after delivery: "
                    + ", ".join(
                        f"{gate.get('op', '-')}/{gate.get('gate_stage', '-')} "
                        f"status={gate.get('status', '-')} age_seconds={gate.get('age_seconds', '-')}"
                        for gate in reportable_stale_gates
                    )
                )
    thread_observations = read_json(state_dir / "solver_thread_observations.json")
    thread_observation_age_seconds: int | None = None
    if thread_observations:
        observation_time = parse_timestamp(str(thread_observations.get("updated_at", "") or ""))
        if observation_time is None:
            issues.append("solver thread observations have no parseable time")
        else:
            age = max(0, int((datetime.now(timezone.utc) - observation_time).total_seconds()))
            thread_observation_age_seconds = age
            observations.append(f"solver_thread_observation_age_seconds={age}")
            solver_owned_ops_for_stale_check = {
                str(decision.get("op", "") or "")
                for decision in decisions
                if isinstance(decision, dict) and str(decision.get("next_owner", "") or "") == "solver"
            }
            solver_owned_ops_for_stale_check.update(
                str(gate.get("op", "") or "")
                for gate in active_gates
                if isinstance(gate, dict) and gate.get("op")
            )
            if age > SOLVER_OBSERVATION_STALE_SECONDS:
                detail = (
                    "solver thread observations stale: "
                    f"age_seconds={age} threshold={SOLVER_OBSERVATION_STALE_SECONDS}"
                )
                if solver_owned_ops_have_fresh_visible_active_gates(
                    solver_owned_ops_for_stale_check,
                    active_gate_by_op,
                    SOLVER_OBSERVATION_STALE_SECONDS,
                ) and not pending_triggers:
                    observations.append(detail + " (covered by fresh IDE relay ack)")
                elif solver_owned_ops_for_stale_check or pending_triggers:
                    issues.append(detail)
                else:
                    observations.append(detail + " (observation-only; no active solver-owned pilot gate)")
        threads = thread_observations.get("threads", [])
        configured_solver_threads = {
            str(item.get("op", "") or ""): str(item.get("thread_id", "") or "")
            for item in required_reads
            if isinstance(item, dict) and item.get("op") and item.get("thread_id")
        }
        thread_by_op: dict[str, dict[str, Any]] = {}
        if isinstance(threads, list):
            for item in threads:
                if not isinstance(item, dict) or not item.get("op"):
                    continue
                if str(item.get("role", "solver") or "solver") != "solver":
                    continue
                op = str(item.get("op", "") or "")
                configured_thread_id = configured_solver_threads.get(op, "")
                observed_thread_id = str(item.get("thread_id", "") or "")
                if configured_thread_id and observed_thread_id and observed_thread_id != configured_thread_id:
                    continue
                thread_by_op[op] = item
        if thread_by_op:
            observations.append(
                "solver_thread_idle="
                + ",".join(
                    f"{op}:{item.get('latest_turn_status', '-')}/{item.get('idle_seconds', '-')}"
                    for op, item in sorted(thread_by_op.items())
                )
            )
        solver_owned_ops = {
            str(decision.get("op", "") or "")
            for decision in decisions
            if isinstance(decision, dict) and str(decision.get("next_owner", "") or "") == "solver"
        }
        solver_owned_ops.update(
            str(gate.get("op", "") or "")
            for gate in active_gates
            if isinstance(gate, dict) and gate.get("op")
        )
        idle_threshold = 300
        for op in sorted(solver_owned_ops):
            active_gate = active_gate_by_op.get(op)
            active_gate_status = active_solver_gate_status(active_gate)
            if op in active_bridge_solver_ops:
                observations.append(f"solver_bridge_delivery_active={op}")
                continue
            if active_gate_status in {"not-sent", "ready", "retry-ready", "pending"}:
                observations.append(f"solver_gate_awaiting_native_delivery={op}:{active_gate_status}")
                continue
            if active_gate_status in {"interrupted", "failed", "cancelled"}:
                continue
            if active_solver_gate_fresh(active_gate, idle_threshold):
                gate_age = active_solver_gate_age_seconds(active_gate)
                observations.append(
                    "solver_active_trigger_visible="
                    f"{op}:{active_solver_gate_status(active_gate) or '-'}"
                    f"/{gate_age if gate_age is not None else '-'}"
                )
                continue
            item = thread_by_op.get(op)
            status = str(item.get("latest_turn_status", "") or "") if isinstance(item, dict) else ""
            if status in {"interrupted", "failed", "cancelled"}:
                if remote_control_blocker(active_gate):
                    observations.append(
                        "solver native turn interrupted by relay remote-control blocker: "
                        f"{op}"
                    )
                elif not transient_failed_thread_observation(active_gate, item):
                    issues.append(
                        "solver-owned workflow has terminal failed native turn: "
                        f"{op} status={status}"
                    )
            idle_seconds = optional_int(item.get("idle_seconds")) if isinstance(item, dict) else None
            if idle_seconds is not None and idle_seconds >= idle_threshold:
                issues.append(
                    "solver-owned workflow idle without native progress: "
                    f"{op} idle_seconds={idle_seconds} threshold={idle_threshold}"
                )
        for op, item in sorted(thread_by_op.items()):
            if op not in solver_owned_ops:
                continue
            active_gate = active_gate_by_op.get(op)
            usage_limit = account_usage_limit_blocker(active_gate)
            visibility_error = str(
                item.get("resume_error") or item.get("error") or item.get("read_error") or ""
            )
            if visibility_error:
                issues.append(
                    "solver IDE session unreadable by daemon: "
                    f"{op} error={visibility_error[:180]}"
                )
            status_type = str(item.get("thread_status_type", "") or "")
            if status_type == "systemError" and not usage_limit:
                issues.append(f"solver IDE session is in systemError: {op}")
            if bool(item.get("thread_header_stale")) and not usage_limit:
                issues.append(
                    "solver IDE session header is not advancing: "
                    f"{op} lag_seconds={item.get('thread_header_lag_seconds', '-')}"
                )
            if op in active_bridge_solver_ops:
                continue
            if item.get("latest_user_only_turn") is True:
                if usage_limit:
                    continue
                active_gate_status = active_solver_gate_status(active_gate)
                if active_gate_status in {"not-sent", "ready", "retry-ready", "pending"}:
                    continue
                idle_seconds = optional_int(item.get("idle_seconds"))
                if remote_control_blocker(active_gate):
                    continue
                if (
                    active_solver_gate_status(active_gate) not in {"interrupted", "failed", "cancelled"}
                    and active_solver_gate_fresh(active_gate, idle_threshold)
                ):
                    continue
                issues.append(f"solver IDE latest turn has no agent output: {op}")
    tester_session = read_json(state_dir / "tester_session_status.json")
    if tester_session:
        tester_ack_state = read_tester_trigger_ack_state(root)
        tester_sent_records = (
            tester_ack_state.get("sent", {}) if isinstance(tester_ack_state.get("sent"), dict) else {}
        )
        tester_required_reads = (
            tester_session.get("required_thread_reads", [])
            if isinstance(tester_session.get("required_thread_reads"), list)
            else []
        )
        tester_missing = (
            tester_session.get("missing_tester_threads", [])
            if isinstance(tester_session.get("missing_tester_threads"), list)
            else []
        )
        raw_tester_active = (
            tester_session.get("active_tester_gates", [])
            if isinstance(tester_session.get("active_tester_gates"), list)
            else []
        )
        tester_active = [
            overlay_solver_gate_with_ack(gate, tester_sent_records)
            for gate in raw_tester_active
            if isinstance(gate, dict) and tester_gate_is_current_casegen(gate, current_tester_casegen_ops)
        ]
        raw_tester_stale = (
            tester_session.get("stale_tester_gates", [])
            if isinstance(tester_session.get("stale_tester_gates"), list)
            else []
        )
        tester_stale = [
            overlay_solver_gate_with_ack(gate, tester_sent_records)
            for gate in raw_tester_stale
            if isinstance(gate, dict) and tester_gate_is_current_casegen(gate, current_tester_casegen_ops)
        ]
        observations.append(f"tester_required_thread_reads={len(tester_required_reads)}")
        if tester_active:
            observations.append(
                "tester_active_gates="
                + ",".join(
                    f"{gate.get('op', '-')}/{gate.get('gate_stage', '-')}:{gate.get('status', '-')}"
                    for gate in tester_active
                    if isinstance(gate, dict)
                )
            )
        if tester_missing:
            issues.append(
                "tester casegen session missing thread id: "
                + ", ".join(
                    str(item.get("op", "-"))
                    for item in tester_missing
                    if isinstance(item, dict)
                )
            )
        if tester_stale:
            fresh_tester_stale = [
                gate
                for gate in tester_stale
                if isinstance(gate, dict)
                and active_solver_gate_fresh(gate, SOLVER_OBSERVATION_STALE_SECONDS)
            ]
            if fresh_tester_stale:
                observations.append(
                    "tester_active_trigger_visible="
                    + ",".join(
                        f"{gate.get('op', '-')}:{active_solver_gate_status(gate) or '-'}/"
                        f"{active_solver_gate_age_seconds(gate) if active_solver_gate_age_seconds(gate) is not None else '-'}"
                        for gate in fresh_tester_stale
                    )
                )
            reportable_tester_stale = [
                gate
                for gate in tester_stale
                if isinstance(gate, dict)
                and not active_solver_gate_fresh(gate, SOLVER_OBSERVATION_STALE_SECONDS)
            ]
        else:
            reportable_tester_stale = []
        if reportable_tester_stale:
            issues.append(
                "tester casegen trigger stale after delivery: "
                + ", ".join(
                    f"{gate.get('op', '-')}/{gate.get('gate_stage', '-')} "
                    f"status={gate.get('status', '-')} age_seconds={gate.get('age_seconds', '-')}"
                    for gate in reportable_tester_stale
                    if isinstance(gate, dict)
                )
            )
    efficiency = read_json(state_dir / "test_efficiency.json")
    current_resource_idle = bool(efficiency.get("resource_idle", True)) if isinstance(efficiency, dict) else True
    if lease_list:
        current_resource_idle = False
    if efficiency:
        observations.append(f"resource_idle={current_resource_idle}")
        recent_dispatch = efficiency.get("recent_dispatch_balance", {})
        if isinstance(recent_dispatch, dict) and recent_dispatch:
            observations.append(
                "recent_dispatch_balance="
                + ",".join(f"{op}:{count}" for op, count in sorted(recent_dispatch.items()))
            )
        traffic_balance = efficiency.get("traffic_balance", {})
        if isinstance(traffic_balance, dict) and traffic_balance:
            debt = traffic_balance.get("debt", {})
            observations.append(
                "traffic_balance="
                f"window={traffic_balance.get('window_size', '-')} "
                f"samples={traffic_balance.get('sample_count', '-')} "
                f"ok={traffic_balance.get('ok', '-')}"
            )
            if isinstance(debt, dict) and debt:
                observations.append(
                    "traffic_balance_debt="
                    + ",".join(f"{op}:{value}" for op, value in sorted(debt.items()))
                )
            balance_recovery = efficiency.get("balance_recovery", {}) if isinstance(efficiency, dict) else {}
            if isinstance(balance_recovery, dict) and balance_recovery.get("plans"):
                plan_labels = []
                for item in balance_recovery.get("plans", []):
                    if not isinstance(item, dict):
                        continue
                    plan_labels.append(
                        f"{item.get('op', '-')}:{item.get('state', '-')}:"
                        f"compensated={item.get('compensated', False)}"
                    )
                if plan_labels:
                    observations.append("traffic_balance_recovery=" + ",".join(plan_labels))
            uncompensated = []
            if isinstance(balance_recovery, dict) and isinstance(balance_recovery.get("uncompensated_ops"), list):
                uncompensated = [str(op) for op in balance_recovery.get("uncompensated_ops", []) if str(op)]
            if traffic_balance.get("enough_samples") and not traffic_balance.get("ok") and uncompensated:
                issues.append(
                    "traffic balance debt in sliding test window: "
                    f"window={traffic_balance.get('window_size', '-')} "
                    f"min_per_operator={traffic_balance.get('min_per_operator', '-')} "
                    f"debt={debt} uncompensated_ops={uncompensated}"
                )
    gaps = read_or_rebuild_gap_snapshot(root, state_dir)
    if gaps:
        idle_window = optional_int(gaps.get("test_idle_window_seconds"))
        idle_threshold = optional_int(gaps.get("test_idle_stale_seconds")) or 480
        observations.append(f"timeline_events={gaps.get('total_event_count', '-')}")
        observations.append(f"test_idle_window_seconds={idle_window if idle_window is not None else '-'}")
        if gaps.get("max_result_to_solver_ide_active_seconds") is not None:
            observations.append(
                "max_result_to_solver_ide_active_seconds="
                f"{gaps.get('max_result_to_solver_ide_active_seconds')}"
            )
        if gaps.get("max_result_to_next_dispatch_seconds") is not None:
            observations.append(
                f"max_result_to_next_dispatch_seconds={gaps.get('max_result_to_next_dispatch_seconds')}"
            )
        if gaps.get("recent_window_result_count") is not None:
            observations.append(f"recent_window_result_count={gaps.get('recent_window_result_count')}")
        recent_result_to_ide = optional_int(
            gaps.get("recent_window_max_result_to_solver_ide_active_seconds")
        )
        result_to_ide_threshold = (
            optional_int(gaps.get("result_to_solver_ide_active_threshold_seconds"))
            or 10
        )
        if recent_result_to_ide is not None:
            observations.append(
                "recent_window_max_result_to_solver_ide_active_seconds="
                f"{recent_result_to_ide}"
            )
            if recent_result_to_ide >= result_to_ide_threshold:
                issues.append(
                    "RESULT-to-IDE active SLA violated: "
                    f"threshold=<{result_to_ide_threshold}s "
                    f"recent_window_max_seconds={recent_result_to_ide}; "
                    "solver active-to-completed duration is explicitly excluded"
                )
        if gaps.get("recent_window_max_result_to_next_dispatch_seconds") is not None:
            observations.append(
                "recent_window_max_result_to_next_dispatch_seconds="
                f"{gaps.get('recent_window_max_result_to_next_dispatch_seconds')}"
            )
        if gaps.get("max_remote_clock_ahead_seconds") is not None:
            observations.append(f"max_remote_clock_ahead_seconds={gaps.get('max_remote_clock_ahead_seconds')}")
        submit_gap = gaps.get("completion_to_next_submit", {})
        if isinstance(submit_gap, dict) and submit_gap:
            observations.append(
                "completion_to_next_submit="
                f"window={submit_gap.get('window_size', '-')} "
                f"samples={submit_gap.get('sample_count', '-')} "
                f"max_gap_seconds={submit_gap.get('max_gap_seconds', '-')} "
                f"violations={submit_gap.get('violation_count', 0)}"
            )
            violations = submit_gap.get("violations", [])
            if isinstance(violations, list) and violations:
                first = violations[0] if isinstance(violations[0], dict) else {}
                issues.append(
                    "submit gap SLA violated: "
                    f"threshold={submit_gap.get('threshold_seconds', '-')}s "
                    f"max_gap_seconds={submit_gap.get('max_gap_seconds', '-')} "
                    f"violation_count={submit_gap.get('violation_count', '-')} "
                    f"root_cause={first.get('root_cause', '-') if isinstance(first, dict) else '-'} "
                    f"recovery_action={first.get('recovery_action', '-') if isinstance(first, dict) else '-'}"
                )
        full_flow = gaps.get("full_flow_latency", {})
        if isinstance(full_flow, dict) and full_flow:
            gate = full_flow.get("gate", {})
            aggregate = full_flow.get("aggregate", {})
            gate = gate if isinstance(gate, dict) else {}
            aggregate = aggregate if isinstance(aggregate, dict) else {}
            observations.append(
                "full_flow_latency="
                f"status={gate.get('status', '-')} "
                f"strict={gate.get('strict_complete_count', 0)}/"
                f"{gate.get('eligible_sample_count', 0)} "
                f"violations={gate.get('violation_count', 0)}"
            )
            bottlenecks = aggregate.get("controllable_bottleneck_counts", {})
            if isinstance(bottlenecks, dict) and bottlenecks:
                observations.append(
                    "full_flow_bottlenecks="
                    + ",".join(f"{name}:{count}" for name, count in bottlenecks.items())
                )
            if gate.get("status") == "failed":
                violation_rows = gate.get("violations", [])
                first = (
                    violation_rows[0]
                    if isinstance(violation_rows, list)
                    and violation_rows
                    and isinstance(violation_rows[0], dict)
                    else {}
                )
                issues.append(
                    "full-flow latency contract failed: "
                    f"violations={gate.get('violation_count', 0)} "
                    f"op={first.get('op', '-')} "
                    f"test_version={first.get('test_version', '-')} "
                    f"missing={first.get('missing_required_timestamps', [])} "
                    f"order={first.get('timestamp_order_violations', [])} "
                    f"thresholds={first.get('threshold_violations', [])}"
                )
        tester_work = efficiency.get("tester_owned_work", []) if isinstance(efficiency, dict) else []
        if (
            bool(gaps.get("resource_idle"))
            and current_resource_idle
            and idle_window is not None
            and idle_window >= idle_threshold
            and isinstance(tester_work, list)
            and tester_work
        ):
            observations.append(
                "test idle self-check due: "
                f"idle_seconds={idle_window} threshold={idle_threshold} tester_work={','.join(map(str, tester_work))}"
            )

    prewarm = read_json(state_dir / "case_cache_prewarm_status.json")
    prewarm_rows = prewarm.get("operators", [])
    if isinstance(prewarm_rows, list):
        for raw in prewarm_rows:
            if not isinstance(raw, dict):
                continue
            state = str(raw.get("state") or "")
            if state == "ready":
                continue
            op = str(raw.get("op") or "unknown-op")
            case_version = str(raw.get("case_version") or "unknown-case")
            observations.append(
                "case_cache_prewarm="
                f"{op}/{case_version}:{state or 'unknown'} "
                f"attempts={raw.get('attempt_count', 0)}"
            )
            if state == "blocked":
                issues.append(
                    "case-cache prewarm blocked: "
                    f"op={op} case_version={case_version} "
                    f"attempts={raw.get('attempt_count', 0)}/"
                    f"{raw.get('max_attempts', '-')} "
                    f"engine_job_id={raw.get('engine_job_id', '-')} "
                    f"terminal_state={raw.get('terminal_state', '-')} "
                    f"engine_state={raw.get('engine_state', '-')} "
                    f"error={raw.get('error', '-') or '-'}"
                )

    stop_request = read_stop_request(root)
    if stop_request:
        issues.append(
            "daemon stop requested: "
            f"requested_at={stop_request.get('requested_at', '')} "
            f"reason={stop_request.get('reason', '')}"
        )
        observations.append(
            f"stop_requested_at={stop_request.get('requested_at', '')} reason={stop_request.get('reason', '')}"
        )

    lock_path = state_dir / "daemon.lock"
    if lock_path.exists():
        observations.append(f"lock_exists={relpath(lock_path, root)}")
        lock_pid = read_lock_pid(lock_path)
        if lock_pid:
            observations.append(f"lock_pid={lock_pid}")
            if not process_alive(lock_pid):
                issues.append(f"daemon lock pid not alive: pid={lock_pid} path={relpath(lock_path, root)}")

    return HealthReport(ok=not issues, issues=tuple(issues), observations=tuple(observations))


def add_bridge_health(
    root: Path,
    state_dir: Path,
    issues: list[str],
    observations: list[str],
    *,
    has_pending_triggers: bool,
    max_heartbeat_age_seconds: int,
) -> None:
    heartbeat_path = state_dir / "solver_trigger_bridge_heartbeat.json"
    heartbeat = read_json(heartbeat_path)
    bridge_alive = False
    bridge_disabled = False
    bridge_age: int | None = None
    bridge_pid = 0
    if heartbeat:
        bridge_mode = str(heartbeat.get("mode", "") or "")
        bridge_disabled = bridge_mode.startswith("disabled")
        if bridge_mode:
            observations.append(f"solver_bridge_heartbeat_mode={bridge_mode}")
        bridge_time = parse_timestamp(str(heartbeat.get("time", "")))
        if bridge_time is not None:
            bridge_age = max(0, int((datetime.now(timezone.utc) - bridge_time).total_seconds()))
            observations.append(f"solver_bridge_heartbeat_age_seconds={bridge_age}")
        bridge_pid = int(heartbeat.get("pid", 0) or 0)
        if bridge_pid:
            if bridge_mode == "run":
                observations.append(f"solver_bridge_pid={bridge_pid}")
                bridge_alive = process_alive(bridge_pid)
            else:
                observations.append(f"solver_bridge_last_probe_pid={bridge_pid}")
    else:
        observations.append(f"solver_bridge_heartbeat_missing={relpath(heartbeat_path, root)}")

    lock_path = state_dir / "solver_trigger_bridge.lock"
    if lock_path.exists():
        lock_pid = read_lock_pid(lock_path)
        observations.append(f"solver_bridge_lock_exists={relpath(lock_path, root)}")
        if lock_pid:
            observations.append(f"solver_bridge_lock_pid={lock_pid}")
            bridge_alive = bridge_alive or process_alive(lock_pid)
            if not process_alive(lock_pid) and has_pending_triggers:
                issues.append(f"solver trigger bridge lock pid not alive: pid={lock_pid}")

    bridge_stale = (
        max_heartbeat_age_seconds > 0
        and (bridge_age is None or bridge_age > max_heartbeat_age_seconds)
    )
    if has_pending_triggers and not bridge_disabled and (not bridge_alive or bridge_stale):
        issue_bits = []
        if not bridge_alive:
            issue_bits.append("process_not_alive")
        if bridge_stale:
            issue_bits.append(f"heartbeat_stale age_seconds={bridge_age if bridge_age is not None else 'missing'}")
        issues.append("solver trigger bridge unavailable for pending trigger: " + "; ".join(issue_bits))

    poll_status = read_json(state_dir / "solver_thread_poll_status.json")
    if poll_status:
        status = str(poll_status.get("status", "") or "")
        updated_at = parse_timestamp(str(poll_status.get("updated_at", "") or ""))
        age = None
        if updated_at is not None:
            age = max(0, int((datetime.now(timezone.utc) - updated_at).total_seconds()))
            observations.append(f"solver_thread_poll_status_age_seconds={age}")
        observations.append(f"solver_thread_poll_status={status or '-'}")
        if status == "failed":
            error = str(poll_status.get("error", "") or "")
            detail = f" age_seconds={age if age is not None else 'unknown'}"
            if error:
                detail += f" error={error[:180]}"
            thread_observations = read_json(state_dir / "solver_thread_observations.json")
            observation_time = parse_timestamp(str(thread_observations.get("updated_at", "") or ""))
            observation_age = (
                max(0, int((datetime.now(timezone.utc) - observation_time).total_seconds()))
                if observation_time is not None
                else None
            )
            if observation_age is not None and observation_age <= SOLVER_OBSERVATION_STALE_SECONDS:
                observations.append("solver thread polling failed in bridge but native observations are fresh:" + detail)
            else:
                issues.append("solver thread polling failed in bridge:" + detail)


def add_relay_capability_health(
    state_dir: Path,
    issues: list[str],
    observations: list[str],
) -> None:
    poll_status = read_json(state_dir / "solver_thread_poll_status.json")
    repair = read_json(state_dir / "app_server_proxy_repair.json")
    relay_state = read_json(state_dir / "relay_capability_state.json")
    status = str(poll_status.get("status", "") or "")
    error = str(poll_status.get("error", "") or "")
    fallback_reason = str(poll_status.get("fallback_reason", "") or "")
    returncode = repair.get("returncode")
    socket_exists = repair.get("socket_exists_after")
    if repair:
        observations.append(
            "app_server_proxy_repair="
            f"returncode={returncode} socket_exists={socket_exists} "
            f"platform={repair.get('platform', '-')}"
        )
    if status in {"failed", "degraded"}:
        observations.append(f"solver_thread_poll_autonomy_status={status}")
    unavailable = status in {"failed", "degraded"} and (
        "proxy control socket is not available" in error
        or "proxy socket unavailable" in fallback_reason
        or returncode not in (None, 0)
        or socket_exists is False
    )
    remote_control_blocked = (
        str(relay_state.get("status", "") or "") == "blocked"
        and str(relay_state.get("reason", "") or "") == "remote_control_not_ready"
    )
    if remote_control_blocked:
        observations.append(
            "ide_remote_control_delivery="
            f"blocked status={relay_state.get('remote_control_status', '-')} "
            f"retry_after={relay_state.get('retry_after', '-')}"
        )
        unavailable = True
    if not unavailable:
        return
    bridge_heartbeat = read_json(state_dir / "solver_trigger_bridge_heartbeat.json")
    if bool(bridge_heartbeat.get("cli_resume_fallback_enabled")):
        observations.append(
            "IDE remoteControl unavailable; daemon-owned codex exec resume fallback is enabled"
        )
        return
    app_side = read_app_side_relay_status(state_dir)
    outbox = read_json(state_dir / "native_relay_outbox.json")
    outbox_entries = outbox.get("entries", []) if isinstance(outbox.get("entries"), list) else []
    outbox_count = int(outbox.get("entry_count", len(outbox_entries)) or 0) if isinstance(outbox, dict) else 0
    active_claims = active_claims_by_entry(read_native_relay_claims(state_dir.parent.parent))
    unclaimed_outbox_count = sum(
        1
        for item in outbox_entries
        if isinstance(item, dict) and str(item.get("id", "") or "") not in active_claims
    )
    app_side_fresh = bool(app_side.get("fresh"))
    if app_side:
        observations.append(
            "app_side_native_relay="
            f"fresh={app_side_fresh} age_seconds={app_side.get('age_seconds', '-')} "
            f"kind={app_side.get('kind', '-')} status={app_side.get('status', '-')}"
        )
    if outbox_count == 0:
        cooldown = account_usage_limit_cooldown_summary(state_dir.parent.parent)
        if cooldown.get("all_active_triggers_cooling"):
            observations.append(
                "app_side_native_relay=intentionally-dormant "
                "reason=account_usage_limit "
                f"retry_after={cooldown.get('retry_after', '-')} "
                f"ops={','.join(cooldown.get('ops', [])) or '-'}"
            )
            return
        if app_side_fresh:
            observations.append(
                "IDE-native relay platform proxy unavailable but app-side relay is fresh and outbox is empty"
            )
        else:
            issues.append(
                "IDE-native relay unavailable for autonomous delivery: "
                "daemon cannot call Codex App native send on this platform; "
                "app-side native relay heartbeat is stale or missing. "
                "No native relay outbox is pending now, but the next solver/tester gate would require manual relay."
            )
        return
    if outbox_count and app_side_fresh and unclaimed_outbox_count:
        observations.append(
            "IDE-native relay platform proxy unavailable but app-side relay is fresh and outbox is pending: "
            f"available_entry_count={unclaimed_outbox_count}"
        )
        return
    if outbox_count and unclaimed_outbox_count == 0:
        observations.append(
            "IDE-native relay platform proxy unavailable but all pending outbox items are claimed by app-side relay: "
            f"active_claim_count={len(active_claims)}"
        )
        return
    detail = str(repair.get("stderr_tail", "") or error or fallback_reason or "proxy socket unavailable")
    if unclaimed_outbox_count:
        detail = f"native relay outbox has {unclaimed_outbox_count} unclaimed pending item(s); " + detail
    elif not app_side_fresh:
        detail = "app-side native relay heartbeat is stale or missing; " + detail
    issues.append(
        "IDE-native relay unavailable for autonomous delivery: "
        "daemon cannot call Codex App native send on this platform; "
        "provide an app-side relay or a working app-server control socket. "
        + detail.strip().replace("\n", " ")[:220]
    )


def read_app_side_relay_status(state_dir: Path) -> dict[str, Any]:
    record = read_json(state_dir / NATIVE_RELAY_CONSUMER_STATUS)
    source = NATIVE_RELAY_CONSUMER_STATUS
    if not record:
        legacy = read_json(state_dir / "native_relay_app_side_status.json")
        # Before the dedicated file existed, claim/poll records were written
        # here.  Delivery records in the same file are not consumer liveness.
        if str(legacy.get("kind", "") or "") == "native-relay-claim":
            record = legacy
            source = "native_relay_app_side_status.json:legacy-claim"
        elif legacy:
            updated_at = parse_timestamp(str(legacy.get("updated_at", "") or ""))
            age = (
                max(0, int((datetime.now(timezone.utc) - updated_at).total_seconds()))
                if updated_at is not None
                else None
            )
            return {
                **legacy,
                "source": "native_relay_app_side_status.json:delivery-only",
                "fresh": False,
                "age_seconds": age,
                "stale_threshold_seconds": NATIVE_RELAY_CONSUMER_STALE_SECONDS,
                "consumer_liveness": False,
                "reason": "delivery-is-not-consumer-liveness",
            }
    if not record:
        return {}
    updated_at = parse_timestamp(str(record.get("updated_at", "") or ""))
    if updated_at is None:
        return {**record, "source": source, "fresh": False, "age_seconds": None}
    age = max(0, int((datetime.now(timezone.utc) - updated_at).total_seconds()))
    return {
        **record,
        "source": source,
        "fresh": age <= NATIVE_RELAY_CONSUMER_STALE_SECONDS,
        "age_seconds": age,
        "stale_threshold_seconds": NATIVE_RELAY_CONSUMER_STALE_SECONDS,
    }


def read_stale_execute_workers(root: Path, state_dir: Path) -> list[dict[str, Any]]:
    workers = read_json(state_dir / "execute_workers.json").get("workers", {})
    if not isinstance(workers, dict):
        return []
    stale: list[dict[str, Any]] = []
    for action_id, worker in workers.items():
        if not isinstance(worker, dict):
            continue
        pid = int(worker.get("pid", 0) or 0)
        status = worker_process_status(root, str(action_id), worker)
        if not status.active and pid > 0:
            stale.append({"action_id": action_id, "pid": pid, "reason": status.reason})
    return stale


def read_pending_solver_triggers(
    state_dir: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    plan = read_json(state_dir / "solver_trigger_plan.json")
    ack_state = read_trigger_ack_state(state_dir.parent.parent)
    sent = ack_state.get("sent", {}) if isinstance(ack_state.get("sent"), dict) else {}
    bridge_workers = read_json(state_dir / "solver_trigger_bridge_workers.json")
    active_workers = bridge_workers.get("workers", {}) if isinstance(bridge_workers.get("workers"), dict) else {}
    observations = read_json(state_dir / "solver_thread_observations.json")
    observed_threads = observations.get("threads", []) if isinstance(observations.get("threads"), list) else []
    in_progress_ops = {
        str(item.get("op", "") or "")
        for item in observed_threads
        if isinstance(item, dict)
        and str(item.get("role", "solver") or "solver") == "solver"
        and str(item.get("latest_turn_status", "") or "").lower() in {"inprogress", "in_progress", "running", "active"}
    }
    triggers = plan.get("triggers", []) if isinstance(plan.get("triggers"), list) else []
    pending: list[dict[str, Any]] = []
    acked_stale: list[dict[str, Any]] = []
    active_bridge: list[dict[str, Any]] = []
    for trigger in triggers:
        if not isinstance(trigger, dict):
            continue
        status = str(trigger.get("status", "") or "")
        if status not in {"ready", "retry-ready"}:
            continue
        key = str(trigger.get("key", "") or "")
        record = sent.get(key) if isinstance(sent, dict) else None
        record_status = ""
        if isinstance(record, dict):
            record_status = normalized_trigger_status(record, str(record.get("status", "") or ""))
        if str(trigger.get("op", "") or "") in in_progress_ops:
            active_bridge.append(trigger)
        elif bridge_worker_active(active_workers.get(key)):
            active_bridge.append(trigger)
        elif record_status in {"sent", "delivered", "acked", "active", "completed"} and not (
            status == "retry-ready" and record_status == "completed"
        ):
            acked_stale.append(trigger)
        else:
            pending.append(trigger)
    return pending, acked_stale, active_bridge


def read_pending_tester_triggers(
    state_dir: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    plan = read_json(state_dir / "tester_trigger_plan.json")
    ack_state = read_tester_trigger_ack_state(state_dir.parent.parent)
    sent = ack_state.get("sent", {}) if isinstance(ack_state.get("sent"), dict) else {}
    triggers = plan.get("triggers", []) if isinstance(plan.get("triggers"), list) else []
    pending: list[dict[str, Any]] = []
    missing: list[dict[str, Any]] = []
    for trigger in triggers:
        if not isinstance(trigger, dict):
            continue
        status = str(trigger.get("status", "") or "")
        if status == "missing-tester-thread":
            missing.append(trigger)
            continue
        if status not in {"ready", "retry-ready", "needs-native-delivery"}:
            continue
        key = str(trigger.get("key", "") or "")
        record = sent.get(key) if isinstance(sent, dict) else None
        record_status = (
            normalized_trigger_status(record, str(record.get("status", "") or ""))
            if isinstance(record, dict)
            else ""
        )
        if record_status in {"sent", "delivered", "acked", "active", "completed"} and not (
            status == "retry-ready" and record_status == "completed"
        ):
            continue
        if status != "retry-ready" and tester_casegen_active_covering_ack(
            sent,
            str(trigger.get("op", "") or ""),
            key,
        ):
            continue
        pending.append(trigger)
    return pending, missing


def read_tester_remote_control_blockers(state_dir: Path) -> list[dict[str, Any]]:
    plan = read_json(state_dir / "tester_trigger_plan.json")
    ack_state = read_tester_trigger_ack_state(state_dir.parent.parent)
    sent = ack_state.get("sent", {}) if isinstance(ack_state.get("sent"), dict) else {}
    triggers = plan.get("triggers", []) if isinstance(plan.get("triggers"), list) else []
    blockers: list[dict[str, Any]] = []
    for trigger in triggers:
        if not isinstance(trigger, dict):
            continue
        key = str(trigger.get("key", "") or "")
        record = sent.get(key) if isinstance(sent, dict) else None
        if not isinstance(record, dict) or not remote_control_blocker(record):
            continue
        status = str(trigger.get("status", "") or "")
        if status != "retry-ready" and tester_casegen_active_covering_ack(
            sent,
            str(trigger.get("op", "") or ""),
            key,
        ):
            continue
        blockers.append({**trigger, **record, "key": key})
    return blockers


def bridge_worker_active(worker: object) -> bool:
    if not isinstance(worker, dict):
        return False
    return process_alive(int(worker.get("pid", 0) or 0))


def decision_dict_is_tester_casegen(decision: dict[str, Any]) -> bool:
    if str(decision.get("action", "") or "") == "notify_tester_casegen":
        return True
    if str(decision.get("next_owner", "") or "") != "tester":
        return False
    gate_text = (
        f"{decision.get('gate_stage', '')} "
        f"{decision.get('wakeups', '')} "
        f"{decision.get('reason', '')}"
    ).lower()
    command_text = str(decision.get("command", "") or "").lower()
    return (
        "casegen" in gate_text
        or "case-version" in gate_text
        or "needs-case" in gate_text
        or "generate-case-version" in command_text
        or "tester-authored casegen" in command_text
    )


def tester_gate_is_current_casegen(gate: dict[str, Any], current_casegen_ops: set[str]) -> bool:
    op = str(gate.get("op", "") or "")
    if not op:
        return False
    if current_casegen_ops and op not in current_casegen_ops:
        return False
    gate_text = (
        f"{gate.get('gate_stage', '')} "
        f"{gate.get('action', '')} "
        f"{gate.get('reason', '')}"
    ).lower()
    command_text = str(gate.get("next_command", "") or "").lower()
    return (
        "notify_tester_casegen" in gate_text
        or "casegen" in gate_text
        or "case-version" in gate_text
        or "needs-case" in gate_text
        or "generate-case-version" in command_text
        or "tester-authored casegen" in command_text
    )


def overlay_solver_gate_with_ack(gate: dict[str, Any], sent_records: dict[str, Any]) -> dict[str, Any]:
    key = str(gate.get("key", "") or "")
    record = sent_records.get(key) if isinstance(sent_records, dict) else None
    if not isinstance(record, dict):
        return dict(gate)
    merged = dict(gate)
    record_status = str(record.get("status", "") or "")
    if record_status:
        merged["status"] = record_status
        raw_status = str(gate.get("status", "") or "")
        merged["raw_status"] = raw_status if raw_status and raw_status != record_status else ""
    for name in (
        "thread_id",
        "turn_id",
        "updated_at",
        "last_observed_at",
        "native_status",
        "delivery_retry_reason",
        "delivery_retry_after",
        "last_delivery_error",
        "remote_control_status",
        "failure_kind",
        "thread_status_type",
        "native_no_agent_output_count",
        "ide_panel_visible",
        "ide_panel_visibility",
    ):
        if name in record and record.get(name) not in (None, ""):
            merged[name] = record.get(name)
    if "turn_status" in record and record.get("turn_status") not in (None, ""):
        merged.setdefault("native_status", record.get("turn_status"))
    if record_status in SOLVER_ACK_ACTIVE_STATUSES:
        updated_at = parse_timestamp(str(merged.get("updated_at", "") or ""))
        if updated_at is not None:
            merged["age_seconds"] = max(0, int((datetime.now(timezone.utc) - updated_at).total_seconds()))
        observed_at = parse_timestamp(str(merged.get("last_observed_at", "") or ""))
        if observed_at is not None:
            merged["observation_age_seconds"] = max(
                0,
                int((datetime.now(timezone.utc) - observed_at).total_seconds()),
            )
    return merged


def remote_control_blocker(record: object) -> bool:
    return (
        isinstance(record, dict)
        and str(record.get("delivery_retry_reason", "") or "") == "remote_control_not_ready"
    )


def account_usage_limit_blocker(record: object) -> bool:
    if not isinstance(record, dict):
        return False
    if str(record.get("delivery_retry_reason", "") or "") != "account_usage_limit":
        return False
    retry_after = parse_timestamp(str(record.get("delivery_retry_after", "") or ""))
    return retry_after is not None and datetime.now(timezone.utc) < retry_after


def account_usage_limit_issue(op: str, record: dict[str, Any]) -> str:
    retry_after = str(record.get("delivery_retry_after", "") or "-")
    return f"Codex account usage limit blocks solver relay: {op} retry_after={retry_after}"


def remote_control_blocker_issue(op: str, record: dict[str, Any], *, kind: str = "solver") -> str:
    retry_after = str(record.get("delivery_retry_after", "") or "-")
    status = str(record.get("remote_control_status", "") or "-")
    error = str(record.get("last_delivery_error", "") or "")
    detail = f" status={status} retry_after={retry_after}"
    if error:
        detail += f" error={error[:180]}"
    return f"IDE relay remote-control not ready for {kind} trigger: {op}{detail}"


def active_solver_gate_status(gate: object) -> str:
    if not isinstance(gate, dict):
        return ""
    status_value = str(gate.get("status", "") or "").strip()
    status = str(gate.get("native_status", "") or "").strip()
    if (
        status_value in {"interrupted", "failed", "cancelled"}
        and str(gate.get("failure_kind", "") or "") == "native_turn_no_agent_output"
    ):
        return status_value
    if (
        status_value in {"sent", "delivered", "acked", "active"}
        and status in {"interrupted", "failed", "cancelled"}
    ):
        return status_value
    if status:
        return status
    return status_value


def active_solver_gate_age_seconds(gate: object) -> int | None:
    if not isinstance(gate, dict):
        return None
    age_seconds = optional_int(gate.get("age_seconds"))
    if age_seconds is not None:
        return age_seconds
    updated_at = parse_timestamp(str(gate.get("updated_at", "") or ""))
    if updated_at is None:
        return None
    return max(0, int((datetime.now(timezone.utc) - updated_at).total_seconds()))


def active_solver_gate_fresh(gate: object, threshold_seconds: int) -> bool:
    status = active_solver_gate_status(gate)
    if status not in {"sent", "delivered", "acked", "active", "inProgress", "running", "queued"}:
        return False
    age_seconds = active_solver_gate_age_seconds(gate)
    return age_seconds is not None and age_seconds < threshold_seconds


def solver_owned_ops_have_fresh_visible_active_gates(
    solver_owned_ops: set[str],
    active_gate_by_op: dict[str, dict[str, Any]],
    threshold_seconds: int,
) -> bool:
    if not solver_owned_ops:
        return False
    for op in solver_owned_ops:
        gate = active_gate_by_op.get(op)
        if not active_solver_gate_fresh(gate, threshold_seconds):
            return False
        if not isinstance(gate, dict) or not bool(gate.get("ide_panel_visible")):
            return False
    return True


def transient_failed_thread_observation(active_gate: object, item: object) -> bool:
    if not isinstance(active_gate, dict) or not isinstance(item, dict):
        return False
    if active_solver_gate_status(active_gate) not in {"sent", "delivered", "acked", "active", "inProgress", "running", "queued"}:
        return False
    if not bool(active_gate.get("ide_panel_visible")) and not bool(item.get("ide_panel_visible")):
        return False
    completed_at = item.get("latest_completed_at")
    duration_ms = item.get("latest_duration_ms")
    return completed_at in (None, "") and duration_ms in (None, "")


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def read_or_rebuild_gap_snapshot(root: Path, state_dir: Path) -> dict[str, Any]:
    fallback = read_json(state_dir / "scheduler_gaps.json")
    config_path = runtime_config_path(root, state_dir)
    if config_path is None:
        return fallback
    timeline_path = state_dir / TIMELINE_FILE
    events = read_timeline_events(timeline_path)
    if not events:
        return fallback
    try:
        config = load_config(config_path, apply_completion_markers=True)
        snapshot = StateReader(root, config).read()
        return build_gap_snapshot(root, config, snapshot, events, [])
    except Exception:
        return fallback


def runtime_config_path(root: Path, state_dir: Path) -> Path | None:
    process = read_json(state_dir / "daemon_process.json")
    command = process.get("command", [])
    if not isinstance(command, list):
        return None
    for index, value in enumerate(command):
        if str(value) != "--config" or index + 1 >= len(command):
            continue
        raw_path = Path(str(command[index + 1]))
        return raw_path if raw_path.is_absolute() else root / raw_path
    return None


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


def optional_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def relpath(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)
