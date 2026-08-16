from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ascendop_daemon.legacy.health import (
    check_health,
    overlay_solver_gate_with_ack,
    parse_timestamp,
    read_app_side_relay_status,
    remote_control_blocker,
    remote_control_blocker_issue,
)
from ascendop_daemon.workflow.casegen_evidence import latest_casegen_evidence_issue
from ascendop_daemon.runtime.control import read_stop_request
from ascendop_daemon.legacy.efficiency import build_knowledge_usage
from ascendop_daemon.legacy.engine_admission import EngineAdmissionError, EngineAdmissionStore
from ascendop_daemon.legacy.engine_pump import EnginePump, EnginePumpError
from ascendop_daemon.legacy.engine_promotion import promotion_gate_status
from ascendop_daemon.core.models import (
    DaemonConfig,
    TransportObservation,
    solver_session_replacement_allowed,
    utc_now_iso,
)
from ascendop_daemon.core.models import casegen_role_enabled
from ascendop_daemon.legacy.native_relay_outbox import (
    account_usage_limit_cooldown_summary,
    active_claims_by_entry,
    read_native_relay_claims,
)
from ascendop_daemon.runtime.locking import process_alive, read_lock_pid
from ascendop_daemon.storage.state_reader import StateReader
from ascendop_daemon.legacy.timeline import TIMELINE_FILE, build_gap_snapshot, read_timeline_events
from ascendop_daemon.automation.trigger_state import (
    is_transient_native_poll_failure,
    normalized_trigger_status,
    read_tester_trigger_ack_state,
    read_trigger_ack_state,
)


CONFIRMED_IDE_VISIBILITIES = {
    "confirmed_by_native_relay",
    "live_proxy",
    "live_ws",
}
CONFIRMED_IDE_DELIVERIES = {"ide-native-relay", "codex-app-send-message-to-thread"}
ACTIVE_ACK_STATUSES = {"sent", "delivered", "acked", "active"}
FAILED_TURN_STATUSES = {"interrupted", "failed", "cancelled"}
STORAGE_ONLY_VISIBILITIES = {
    "",
    "unverified_standalone_app_server",
    "not_visible",
    "storage_visible_assumed_for_monitoring",
    "storage_visible_not_ide_visible",
    "thread_read_visible_for_monitoring",
    "codex_cli_resume_session",
}


def filter_health_issues_with_live_board(
    health_issues: tuple[str, ...] | list[str],
    board_rows: list[dict[str, Any]],
) -> tuple[str, ...]:
    """Suppress health issues that came from stale state when live board moved on."""
    live_solver_ops = {
        str(row.get("op", "") or "")
        for row in board_rows
        if isinstance(row, dict)
        and str(row.get("next_owner", "") or "") == "solver"
        and str(row.get("op", "") or "")
    }
    live_owner_by_op = {
        str(row.get("op", "") or ""): str(row.get("next_owner", "") or "")
        for row in board_rows
        if isinstance(row, dict) and str(row.get("op", "") or "")
    }
    filtered: list[str] = []
    for issue in health_issues:
        text = str(issue)
        if text.startswith("solver thread observations stale:") and not live_solver_ops:
            continue
        if text.startswith("solver-owned workflow idle without native progress: "):
            rest = text.split(": ", 1)[1]
            op = rest.split(" ", 1)[0]
            if live_owner_by_op.get(op) != "solver":
                continue
        filtered.append(text)
    return tuple(filtered)


def build_status_query(
    root: Path,
    config: DaemonConfig,
    *,
    config_path: str = "",
    max_heartbeat_age_seconds: int = 120,
    refresh_board: bool = True,
) -> dict[str, Any]:
    state_dir = root / "TestUtils" / "tester_daemon"
    captured_at = utc_now_iso()
    issues: list[str] = []
    warnings: list[str] = []
    recommended_actions: list[dict[str, Any]] = []
    state = read_json(state_dir / "state.json")
    live_transport: tuple[TransportObservation, ...] = ()
    live_snapshot: Any = None

    board_rows: list[dict[str, Any]] = []
    board_error = ""
    if refresh_board:
        try:
            snapshot = StateReader(root, config).read()
            live_snapshot = snapshot
            captured_at = snapshot.captured_at
            live_transport = snapshot.transport
            board_rows = [
                {
                    "season": row.season,
                    "op": row.op,
                    "gate_stage": row.gate_stage,
                    "next_owner": row.next_owner,
                    "wakeups": row.wakeups,
                    "next_command": row.next_command,
                }
                for row in snapshot.rows
            ]
        except Exception as exc:
            board_error = str(exc)
            issues.append(f"live session-board failed: {exc}")
    if not board_rows:
        board_rows = [
            {
                "op": str(item.get("op", "") or ""),
                "gate_stage": str(item.get("gate_stage", "") or ""),
                "next_owner": str(item.get("next_owner", "") or ""),
                "action": str(item.get("action", "") or ""),
                "reason": str(item.get("reason", "") or ""),
                "next_command": str(item.get("command", "") or ""),
            }
            for item in state.get("decisions", [])
            if isinstance(item, dict)
        ]
    board_rows = apply_casegen_evidence_overlay(root, config, board_rows)

    health_report = check_health(
        root, max_heartbeat_age_seconds=max_heartbeat_age_seconds
    )
    health_issues = filter_health_issues_with_live_board(
        tuple(health_report.issues), board_rows
    )

    heartbeat = read_json(state_dir / "daemon_heartbeat.json")
    runtime_residency = build_runtime_residency_summary(
        state_dir,
        daemon_max_heartbeat_age_seconds=max_heartbeat_age_seconds,
        bridge_max_heartbeat_age_seconds=max_heartbeat_age_seconds,
    )
    leases = read_json(state_dir / "leases.json")
    supervisor = read_json(state_dir / "supervisor_state.json")
    plan = read_json(state_dir / "solver_trigger_plan.json")
    ack_state = read_trigger_ack_state(root)
    solver_session = read_json(state_dir / "solver_session_status.json")
    thread_observations = read_json(state_dir / "solver_thread_observations.json")
    thread_poll_status = read_json(state_dir / "solver_thread_poll_status.json")
    app_server_proxy_repair = read_json(state_dir / "app_server_proxy_repair.json")
    relay_capability_state = read_json(state_dir / "relay_capability_state.json")
    native_relay_outbox = read_json(state_dir / "native_relay_outbox.json")
    native_relay_claims = read_native_relay_claims(root)
    native_relay_claim_summary = build_native_relay_claim_summary(native_relay_claims)
    native_relay_outbox_summary = build_native_relay_outbox_summary(
        native_relay_outbox, native_relay_claims
    )
    app_side_relay = read_app_side_relay_status(state_dir)
    efficiency = read_json(state_dir / "test_efficiency.json")
    knowledge_usage = build_knowledge_usage(root, config)
    gaps = read_or_rebuild_gap_snapshot(root, config, live_snapshot)
    performance = read_json(state_dir / "perf_history.json")
    iteration_quality = read_json(state_dir / "iteration_quality.json")
    replacement_plan = read_json(state_dir / "solver_replacement_plan.json")
    tester_plan = read_json(state_dir / "tester_trigger_plan.json")
    tester_ack_state = read_tester_trigger_ack_state(root)
    tester_session = read_json(state_dir / "tester_session_status.json")
    operator_plugins = read_json(state_dir / "operator_plugin_state.json")
    operator_runtime_audit = read_json(state_dir / "operator_runtime_audit.json")
    prompt_metrics = read_json(state_dir / "session_prompt_metrics.json")
    engine_ab_latest = read_json(state_dir / "engine_ab_latest.json")
    engine_promotion = promotion_gate_status(root, config.policy)
    if engine_promotion.get("required") and not engine_promotion.get("allowed"):
        issues.append(
            "engine-v1 production promotion blocked: "
            + "; ".join(str(item) for item in engine_promotion.get("blockers", []))
        )
    try:
        engine_admission = EngineAdmissionStore(root).snapshot()
    except (EngineAdmissionError, OSError, ValueError, json.JSONDecodeError) as exc:
        engine_admission = {"enabled": False, "error": str(exc)}
        warnings.append(f"engine admission state unreadable: {exc}")
    try:
        engine_pump = EnginePump(root).status()
    except (EnginePumpError, OSError, ValueError, json.JSONDecodeError) as exc:
        engine_pump = {"error": str(exc)}
        warnings.append(f"engine pump state unreadable: {exc}")
    stop_request = read_stop_request(root)
    if stop_request:
        command_hint = supervisor_launch_command_hint(config_path)
        recommended_actions.append(
            {
                "kind": "clear_stop_and_launch_supervisor_loop",
                "reason": (
                    "daemon stop file is present; launch the hidden persistent watchdog with --clear-stop "
                    "so it can keep daemon and relay bridge resident"
                ),
                "requested_at": stop_request.get("requested_at", ""),
                "stop_reason": stop_request.get("reason", ""),
                "command_hint": command_hint,
            }
        )

    active_solver_keys = {
        trigger_key(row)
        for row in board_rows
        if row.get("next_owner") == "solver" and trigger_key(row)
    }
    active_solver_ops = {
        str(row.get("op", "") or "")
        for row in board_rows
        if row.get("next_owner") == "solver"
    }

    solver_visibility = build_solver_visibility(
        config,
        plan,
        ack_state,
        solver_session,
        thread_observations,
        board_rows=board_rows,
        active_solver_keys=active_solver_keys,
        active_solver_ops=active_solver_ops,
    )
    tester_visibility = build_tester_visibility(
        tester_plan, tester_ack_state, tester_session
    )
    native_relay_events = read_timeline_events(
        state_dir / "native_relay_app_side_events.jsonl"
    )
    solver_tester_rules = build_solver_tester_rule_summary(
        config=config,
        captured_at=captured_at,
        board_rows=board_rows,
        solver_ack_state=ack_state,
        tester_ack_state=tester_ack_state,
        solver_visibility=solver_visibility,
        tester_visibility=tester_visibility,
        native_relay_outbox=native_relay_outbox_summary,
        native_relay_claims=native_relay_claim_summary,
        native_relay_events=native_relay_events,
        knowledge_usage=knowledge_usage,
    )
    replacement_allowed = solver_session_replacement_allowed(config)
    solver_replacement = (
        build_solver_replacement_summary(replacement_plan)
        if replacement_allowed
        else {
            "updated_at": replacement_plan.get("updated_at", ""),
            "replacement_allowed": False,
            "replacement_required_count": 0,
            "replacements": [],
        }
    )
    tester_remote_control_blocked = False
    for item in tester_visibility.get("records", []):
        if isinstance(item, dict) and item.get("trigger_status") == "reconcile-wait":
            issues.append(
                "tester delivery awaiting Codex restart reconciliation: "
                f"{item.get('op', '-')} {item.get('key', '-')}"
            )
            recommended_actions.append(
                {
                    "kind": "reconcile_native_turn",
                    "op": item.get("op", ""),
                    "gate": item.get("gate_stage", ""),
                    "reason": "wait for the exact configured tester turn or authoritative board advance; do not replay",
                }
            )
        if not isinstance(item, dict) or not remote_control_blocker(item):
            continue
        tester_remote_control_blocked = True
        issues.append(
            remote_control_blocker_issue(
                str(item.get("op", "-") or "-"),
                item,
                kind="tester casegen",
            )
        )
        recommended_actions.append(
            {
                "kind": "repair_ide_remote_control",
                "op": item.get("op", ""),
                "gate": item.get("gate_stage", ""),
                "retry_after": item.get("delivery_retry_after", ""),
                "reason": (
                    "Codex app-server cannot run tester casegen turn because "
                    "remoteControl/enable does not reach connected"
                ),
            }
        )
    for item in solver_visibility["records"]:
        if item.get("trigger_status") == "reconcile-wait":
            issues.append(
                "solver delivery awaiting Codex restart reconciliation: "
                f"{item.get('op', '-')} {item.get('key', '-')}"
            )
            recommended_actions.append(
                {
                    "kind": "reconcile_native_turn",
                    "op": item.get("op", ""),
                    "gate": item.get("gate_stage", ""),
                    "reason": "wait for the exact configured solver turn or authoritative board advance; do not replay",
                }
            )
        if item.get("active_solver_gate") and not item.get("ide_confirmed"):
            issues.append(
                "active solver trigger is not IDE-confirmed: "
                f"{item.get('op', '-')} {item.get('key', '-')}"
            )
        no_agent_count = int(item.get("native_no_agent_output_count", 1) or 1)
        if item.get("active_solver_gate") and remote_control_blocker(item):
            issues.append(
                remote_control_blocker_issue(str(item.get("op", "-") or "-"), item)
            )
            recommended_actions.append(
                {
                    "kind": "repair_ide_remote_control",
                    "op": item.get("op", ""),
                    "retry_after": item.get("delivery_retry_after", ""),
                    "reason": (
                        "Codex app-server created IDE-visible turns but cannot run them because "
                        "remoteControl/enable does not reach connected"
                    ),
                }
            )
        elif item.get("active_solver_gate") and (
            item.get("thread_status_type") == "systemError"
            or (
                item.get("failure_kind") == "native_turn_no_agent_output"
                and no_agent_count >= 2
            )
        ):
            issues.append(
                (
                    "solver session replacement required: "
                    if replacement_allowed
                    else "solver same-session recovery required: "
                )
                + f"{item.get('op', '-')} {item.get('key', '-')}"
            )
        elif (
            item.get("active_solver_gate")
            and item.get("failure_kind") == "native_turn_no_agent_output"
        ):
            issues.append(
                "solver trigger completed without agent output; one IDE-visible retry is allowed: "
                f"{item.get('op', '-')} {item.get('key', '-')}"
            )
        if item.get("storage_only_ack"):
            warnings.append(
                "solver ack is storage-only, not IDE-confirmed: "
                f"{item.get('op', '-')} {item.get('key', '-')}"
            )
        if item.get("skipped_synthetic_turn_id"):
            warnings.append(
                "ignored synthetic solver turn for liveness: "
                f"{item.get('op', '-')} {item.get('skipped_synthetic_turn_id')}"
            )
    fresh_active_visible_ops = {
        str(item.get("op", "") or "")
        for item in solver_visibility["records"]
        if item.get("active_solver_gate")
        and item.get("ide_confirmed")
        and item.get("ack_status") in ACTIVE_ACK_STATUSES
    }
    remote_control_blocked_ops = {
        str(item.get("op", "") or "")
        for item in solver_visibility["records"]
        if item.get("active_solver_gate") and remote_control_blocker(item)
    }
    gate_delivery_blocker_active = tester_remote_control_blocked or bool(
        remote_control_blocked_ops
    )
    for item in solver_visibility.get("thread_records", []):
        if not isinstance(item, dict) or not item.get("active_solver_thread"):
            continue
        if item.get("visibility_error"):
            issues.append(
                "solver IDE session unreadable by daemon: "
                f"{item.get('op', '-')} error={str(item.get('visibility_error', ''))[:180]}"
            )
        if item.get("thread_status_type") == "systemError":
            issues.append(
                f"solver IDE session is in systemError: {item.get('op', '-')}"
            )
            recommended_actions.append(
                {
                    "kind": (
                        "replace_solver_session"
                        if replacement_allowed
                        else "recover_current_solver_session"
                    ),
                    "op": item.get("op", ""),
                    "thread_id": item.get("thread_id", ""),
                    "reason": (
                        "active solver thread is in systemError"
                        if replacement_allowed
                        else "active solver thread is in systemError; keep current session and retry only when solver-owned"
                    ),
                }
            )
        if item.get("thread_header_stale"):
            issues.append(
                "solver IDE session header is not advancing: "
                f"{item.get('op', '-')} lag_seconds={item.get('thread_header_lag_seconds', '-')}"
            )
        op = str(item.get("op", "") or "")
        if op in remote_control_blocked_ops:
            continue
        if item.get("latest_user_only_turn") and op not in fresh_active_visible_ops:
            issues.append(
                f"solver IDE latest turn has no agent output: {item.get('op', '-')}"
            )
    recommended_actions.extend(solver_visibility["recommended_actions"])
    solver_thread_poll = build_solver_thread_poll_summary(thread_poll_status)
    relay_capability = build_relay_capability_summary(
        solver_thread_poll,
        app_server_proxy_repair,
        relay_capability_state,
    )
    outbox_entry_count = int(native_relay_outbox_summary.get("entry_count", 0) or 0)
    outbox_available_count = int(
        native_relay_outbox_summary.get("available_entry_count", outbox_entry_count)
        or 0
    )
    account_cooldown = account_usage_limit_cooldown_summary(root)
    delivery_deferred_for_account_limit = outbox_entry_count == 0 and bool(
        account_cooldown.get("all_active_triggers_cooling")
    )
    delivery_currently_not_needed = (
        outbox_entry_count == 0 and not gate_delivery_blocker_active
    )
    app_side_relay_fresh = bool(app_side_relay.get("fresh"))
    app_side_relay_covers_delivery = (
        app_side_relay_fresh and (outbox_entry_count == 0 or outbox_available_count > 0)
    ) or (
        outbox_entry_count > 0
        and outbox_available_count == 0
        and int(native_relay_claim_summary.get("active_claim_count", 0) or 0) > 0
    )
    cli_resume_covers_delivery = bool(
        config.policy.get("solver_bridge_cli_resume_fallback", False)
    )
    app_side_relay_required = (
        relay_capability.get("native_delivery_available") is False
        and not cli_resume_covers_delivery
        and not delivery_deferred_for_account_limit
    )
    relay_capability["app_side_relay_required"] = app_side_relay_required
    relay_capability["app_side_relay_fresh"] = app_side_relay_fresh
    if solver_thread_poll.get("status") in {"failed", "degraded"}:
        error = str(solver_thread_poll.get("error", "") or "")
        if app_side_relay_covers_delivery:
            if outbox_entry_count == 0:
                relay_coverage_reason = "app-side relay is fresh and outbox is empty"
            elif outbox_available_count > 0:
                relay_coverage_reason = (
                    "app-side relay is fresh and outbox has "
                    f"{outbox_available_count} available pending item(s)"
                )
            else:
                relay_coverage_reason = (
                    "pending outbox items are actively claimed by app-side relay"
                )
            warnings.append(
                "IDE-native relay proxy is unavailable, but " + relay_coverage_reason
            )
        elif delivery_currently_not_needed:
            if delivery_deferred_for_account_limit:
                warnings.append(
                    "IDE-native relay is intentionally dormant during account usage cooldown: "
                    f"retry_after={account_cooldown.get('retry_after', '-')}"
                )
            else:
                warnings.append(
                    "IDE-native relay proxy is unavailable, but no native relay outbox is pending"
                )
        else:
            issues.append(
                "solver thread polling is not autonomous in bridge: "
                + (
                    error[:220]
                    if error
                    else str(
                        solver_thread_poll.get("fallback_reason", "") or "unknown error"
                    )[:220]
                )
            )
            recommended_actions.append(
                {
                    "kind": "repair_solver_thread_polling",
                    "reason": (
                        "daemon cannot use IDE-native relay from this platform; restore app-server "
                        "proxy control socket or provide an explicit Codex App-side relay"
                    ),
                    "error": error[:240],
                    "relay_capability": relay_capability,
                }
            )
    if relay_capability.get("native_delivery_available") is False:
        if app_side_relay_covers_delivery:
            relay_capability["effective_delivery_available"] = True
            relay_capability["effective_delivery"] = "app-side-native-relay"
            relay_capability["app_side_relay"] = app_side_relay
        elif delivery_currently_not_needed:
            if delivery_deferred_for_account_limit:
                relay_capability["effective_delivery_available"] = True
                relay_capability["effective_delivery"] = "deferred-account-usage-limit"
                relay_capability["delivery_deferred"] = True
                relay_capability["delivery_deferred_until"] = account_cooldown.get(
                    "retry_after", ""
                )
                relay_capability["delivery_deferred_ops"] = account_cooldown.get(
                    "ops", []
                )
                relay_capability["app_side_relay"] = app_side_relay
                cooldown_warning = (
                    "IDE-native relay is intentionally dormant during account usage cooldown: "
                    f"retry_after={account_cooldown.get('retry_after', '-')}"
                )
                if cooldown_warning not in warnings:
                    warnings.append(cooldown_warning)
            elif app_side_relay_required and not app_side_relay_fresh:
                relay_capability["effective_delivery_available"] = False
                relay_capability["effective_delivery"] = "app-side-native-relay-stale"
                relay_capability["app_side_relay_required"] = True
                relay_capability["app_side_relay"] = app_side_relay
                issues.append(
                    "app-side native relay heartbeat is stale; autonomous solver/tester delivery "
                    "is not ready for the next gate even though no native relay outbox is pending now"
                )
                recommended_actions.append(
                    {
                        "kind": "restore_app_side_native_relay",
                        "reason": (
                            "daemon cannot call Codex App native send from the background process; "
                            "keep an App-side native relay consumer fresh so the next real gate is delivered"
                        ),
                    }
                )
            else:
                relay_capability["effective_delivery_available"] = True
                relay_capability["effective_delivery"] = (
                    "no-pending-native-relay-outbox"
                )
                relay_capability["app_side_relay"] = app_side_relay
                if app_side_relay and not app_side_relay.get("fresh"):
                    warnings.append(
                        "app-side native relay heartbeat is stale, but no native relay outbox is pending"
                    )
        elif cli_resume_covers_delivery:
            relay_capability["effective_delivery_available"] = True
            relay_capability["effective_delivery"] = "codex-cli-exec-resume"
            warnings.append(
                "IDE remoteControl is unavailable; hidden codex exec resume fallback is enabled for gate-owned delivery"
            )
        else:
            issues.append(
                "IDE-native relay unavailable for autonomous solver/tester delivery: "
                "daemon cannot call Codex App native send on this platform; "
                + str(relay_capability.get("reason", "unknown"))[:220]
            )
    effective_delivery_blocked = bool(
        (
            relay_capability.get(
                "delivery_blocked_raw", relay_capability.get("delivery_blocked")
            )
            and not app_side_relay_covers_delivery
            and not delivery_currently_not_needed
            and not cli_resume_covers_delivery
        )
        or (app_side_relay_required and not app_side_relay_fresh)
    )
    relay_capability["effective_delivery_blocked"] = effective_delivery_blocked
    relay_capability["delivery_blocked"] = effective_delivery_blocked
    if (
        relay_capability.get("delivery_blocked")
        and not app_side_relay_covers_delivery
        and not cli_resume_covers_delivery
    ):
        recommended_actions = [
            action
            for action in recommended_actions
            if not (
                isinstance(action, dict)
                and str(action.get("kind", "") or "")
                in {"deliver_solver_trigger", "deliver_tester_trigger"}
            )
        ]
        recommended_actions.append(
            {
                "kind": "wait_or_repair_ide_remote_control",
                "reason": (
                    "IDE relay delivery is globally blocked; do not start solver/tester delivery "
                    "workers until remoteControl reaches connected"
                ),
                "retry_after": relay_capability.get("delivery_blocked_until", ""),
                "remote_control_status": relay_capability.get(
                    "remote_control_status", ""
                ),
            }
        )

    lease_list = (
        leases.get("leases", []) if isinstance(leases.get("leases"), list) else []
    )
    idle = build_idle_summary(config, efficiency, gaps, resource_leases=lease_list)
    if idle.get("stale"):
        warnings.append(
            "tester-owned work self-check due: "
            f"idle_seconds={idle.get('test_idle_window_seconds')} "
            f"threshold={idle.get('threshold_seconds')}"
        )
        recommended_actions.append(
            {
                "kind": "daemon_execute_tick",
                "reason": "resource idle with tester-owned work beyond idle threshold",
            }
        )
    for gap in idle.get("gap_warnings", []):
        warnings.append(str(gap))
    for observation in health_report.observations:
        if str(observation).startswith("daemon heartbeat self-check due:"):
            warnings.append(str(observation))
            recommended_actions.append(
                {
                    "kind": "daemon_execute_tick",
                    "reason": "daemon heartbeat is stale; run a one-shot self-check tick",
                }
            )
    for issue in health_issues:
        if issue.startswith("daemon heartbeat stale:"):
            warnings.append(
                "daemon heartbeat self-check due: " + issue.removeprefix("daemon ")
            )
            recommended_actions.append(
                {
                    "kind": "daemon_execute_tick",
                    "reason": "daemon heartbeat is stale; run a one-shot self-check tick",
                }
            )
        else:
            issues.append(issue)

    if stop_request:
        recommended_actions = [
            action
            for action in recommended_actions
            if not (
                isinstance(action, dict)
                and str(action.get("kind", "") or "") == "daemon_execute_tick"
            )
        ]
        warnings.append(
            "daemon stop file is present; suppressing direct execute-tick recommendation"
        )

    issues = dedupe_preserve_order(issues)
    quality_summary = build_iteration_quality_summary(iteration_quality)
    suggestion_board_quality = (
        iteration_quality.get("suggestion_board", {})
        if isinstance(iteration_quality.get("suggestion_board"), dict)
        else {}
    )
    quality_regression_threshold = float(
        config.policy.get("skill_quality_regression_warning_pct", 20.0) or 20.0
    )
    for op, item in quality_summary.items():
        regression = item.get("regression_vs_best_pct")
        if (
            regression is not None
            and float(regression) >= quality_regression_threshold
            and item.get("review_status") != "complete"
        ):
            warnings.append(
                f"same-case regression awaits solver skill review: {op} "
                f"latest={item.get('latest', '-')} best={item.get('best_same_case', '-')} "
                f"delta={regression}%"
            )
        if (
            item.get("route_reasoning_applicable")
            and item.get("route_reasoning_status") != "complete"
        ):
            warnings.append(
                f"evidence-first route model incomplete: {op} latest={item.get('latest', '-')} "
                f"score={item.get('route_reasoning_score', '-')}/"
                f"{item.get('route_reasoning_max_score', '-')}"
            )
    if suggestion_board_quality.get("malformed_open_count", 0):
        warnings.append(
            "skill suggestion board has malformed open entries: "
            f"count={suggestion_board_quality.get('malformed_open_count')}"
        )
    warnings = dedupe_preserve_order(warnings)
    recommended_actions = dedupe_recommended_actions(recommended_actions)
    balance = build_balance_summary(root, efficiency, board_rows)
    resume_readiness = build_resume_readiness_summary(
        config=config,
        config_path=config_path,
        stop_request=stop_request,
        heartbeat=heartbeat,
        board_rows=board_rows,
        idle=idle,
        runtime_residency=runtime_residency,
        relay_capability=relay_capability,
        app_side_relay=app_side_relay,
        native_relay_outbox=native_relay_outbox_summary,
        native_relay_claims=native_relay_claim_summary,
        balance=balance,
        recommended_actions=recommended_actions,
        issues=issues,
        warnings=warnings,
    )
    hard_acceptance = build_hard_acceptance_summary(
        resume_readiness, solver_tester_rules
    )
    status = "ALERT" if issues else "WARN" if warnings or recommended_actions else "OK"
    payload: dict[str, Any] = {
        "captured_at": captured_at,
        "status": status,
        "resume_readiness": resume_readiness,
        "hard_acceptance": hard_acceptance,
        "board_error": board_error,
        "board": board_rows,
        "health": {
            "ok": health_report.ok,
            "issues": list(health_report.issues),
            "observations": list(health_report.observations),
        },
        "daemon": {
            "heartbeat_time": heartbeat.get("time", ""),
            "mode": heartbeat.get("mode", ""),
            "resource_lease_count": (
                len(leases.get("leases", []))
                if isinstance(leases.get("leases"), list)
                else 0
            ),
            "supervisor_actions": supervisor.get("actions", []),
        },
        "operator_plugins": operator_plugins,
        "operator_runtime_audit": operator_runtime_audit,
        "session_prompt_metrics": prompt_metrics,
        "engine_admission": engine_admission,
        "engine_pump": engine_pump,
        "engine_ab_latest": engine_ab_latest,
        "engine_promotion": engine_promotion,
        "test_executor": str(config.policy.get("test_executor", "legacy") or "legacy"),
        "runtime_residency": runtime_residency,
        "resources": build_resource_summary(leases),
        "transport": (
            build_live_transport_summary(live_transport)
            if live_transport
            else build_transport_summary(state, efficiency)
        ),
        "solver_visibility": solver_visibility,
        "tester_visibility": tester_visibility,
        "solver_tester_rules": solver_tester_rules,
        "solver_thread_poll": solver_thread_poll,
        "relay_capability": relay_capability,
        "relay_capability_state": relay_capability_state,
        "app_side_relay": app_side_relay,
        "native_relay_outbox": native_relay_outbox_summary,
        "native_relay_claims": native_relay_claim_summary,
        "solver_replacement": solver_replacement,
        "idle_and_gaps": idle,
        "balance": balance,
        "performance": build_performance_summary(performance),
        "iteration_quality": quality_summary,
        "suggestion_board_quality": suggestion_board_quality,
        "issues": issues,
        "warnings": warnings,
        "recommended_actions": recommended_actions,
    }
    return payload


def apply_casegen_evidence_overlay(
    root: Path,
    config: DaemonConfig,
    board_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return daemon-effective rows after casegen evidence policy.

    The live session-board is intentionally raw workflow state. The daemon,
    however, must not let a solver pending/submit gate consume work when the
    newest case version lacks Tester-authored evidence. Status-query should
    therefore display the same effective gate the daemon will act on.
    """
    overlaid: list[dict[str, Any]] = []
    for row in board_rows:
        op = str(row.get("op", "") or "")
        gate = str(row.get("gate_stage", "") or "")
        owner = str(row.get("next_owner", "") or "")
        if not op or not casegen_role_enabled(config, op):
            overlaid.append(row)
            continue
        should_check = (
            owner == "solver" and gate == "needs-pending-candidate"
        ) or gate in {"submit-ready", "submit-running", "submit-waiting"}
        if not should_check:
            overlaid.append(row)
            continue
        issue = latest_casegen_evidence_issue(root, op)
        if issue is None:
            overlaid.append(row)
            continue
        missing = ", ".join(issue.missing)
        next_row = dict(row)
        next_row["raw_gate_stage"] = gate
        next_row["raw_next_owner"] = owner
        next_row["gate_stage"] = "casegen-evidence-incomplete"
        next_row["next_owner"] = "tester"
        next_row["wakeups"] = f"CASEGEN_EVIDENCE_INCOMPLETE missing={missing}"
        try:
            rel_case_dir = str(issue.case_dir.relative_to(root))
        except ValueError:
            rel_case_dir = str(issue.case_dir)
        next_row["next_command"] = (
            f"complete Tester-authored casegen evidence for {op} {issue.case_version}; "
            f"case_dir={rel_case_dir}; missing={missing}; "
            "do not create pending/submit until CASEGEN_PLAN.md and MODEL_AUDIT.md explain weak points, "
            "history basis, current source/tiling linkage, and generated cases"
        )
        overlaid.append(next_row)
    return overlaid


def build_solver_visibility(
    config: DaemonConfig,
    plan: dict[str, Any],
    ack_state: dict[str, Any],
    solver_session: dict[str, Any],
    thread_observations: dict[str, Any],
    *,
    board_rows: list[dict[str, Any]],
    active_solver_keys: set[str],
    active_solver_ops: set[str],
) -> dict[str, Any]:
    sent = ack_state.get("sent", {}) if isinstance(ack_state.get("sent"), dict) else {}
    triggers = (
        plan.get("triggers", []) if isinstance(plan.get("triggers"), list) else []
    )
    raw_active_gates = (
        solver_session.get("active_solver_gates", [])
        if isinstance(solver_session.get("active_solver_gates"), list)
        else []
    )
    active_gates = [
        overlay_solver_gate_with_ack(gate, sent)
        for gate in raw_active_gates
        if isinstance(gate, dict)
    ]
    threads = (
        thread_observations.get("threads", [])
        if isinstance(thread_observations.get("threads"), list)
        else []
    )
    solver_thread_ids = {
        str(op): str(thread_id)
        for op, thread_id in config.solver_threads.items()
        if op and thread_id
    }
    thread_by_op: dict[str, dict[str, Any]] = {}
    for item in threads:
        if not isinstance(item, dict) or not item.get("op"):
            continue
        if str(item.get("role", "solver") or "solver") != "solver":
            continue
        op = str(item.get("op", "") or "")
        configured_thread_id = solver_thread_ids.get(op, "")
        observed_thread_id = str(item.get("thread_id", "") or "")
        if (
            configured_thread_id
            and observed_thread_id
            and observed_thread_id != configured_thread_id
        ):
            continue
        thread_by_op[op] = item
    board_by_op = {
        str(item.get("op", "") or ""): item
        for item in board_rows
        if isinstance(item, dict) and item.get("op")
    }
    records: list[dict[str, Any]] = []
    actions: list[dict[str, Any]] = []

    for trigger in triggers:
        if not isinstance(trigger, dict):
            continue
        key = str(trigger.get("key", "") or "")
        op = str(trigger.get("op", "") or "")
        record = sent.get(key) if isinstance(sent, dict) else None
        visibility = visibility_state(record if isinstance(record, dict) else {})
        # A solver-owned operator may have hundreds of historical trigger
        # records.  Only the exact live board key is active; treating every
        # trigger for the same operator as active resurrects stale failures
        # after a restart or a later gate transition.
        active = key in active_solver_keys
        trigger_status = str(trigger.get("status", "") or "")
        ack_status = (
            normalized_trigger_status(record, "not-sent")
            if isinstance(record, dict)
            else "not-sent"
        )
        records.append(
            {
                "op": op,
                "gate_stage": trigger.get("gate_stage", ""),
                "key": key,
                "trigger_status": trigger_status,
                "ack_status": ack_status,
                "raw_ack_status": (
                    record.get("status", "") if isinstance(record, dict) else ""
                ),
                "turn_id": record.get("turn_id") if isinstance(record, dict) else "",
                "delivery": record.get("delivery") if isinstance(record, dict) else "",
                "visibility": visibility["label"],
                "ide_confirmed": visibility["confirmed"],
                "storage_only_ack": visibility["storage_only"],
                "active_solver_gate": active,
                "delivery_retry_reason": (
                    record.get("delivery_retry_reason", "")
                    if isinstance(record, dict)
                    else ""
                ),
                "delivery_retry_after": (
                    record.get("delivery_retry_after", "")
                    if isinstance(record, dict)
                    else ""
                ),
                "last_delivery_error": (
                    record.get("last_delivery_error", "")
                    if isinstance(record, dict)
                    else ""
                ),
                "remote_control_status": (
                    record.get("remote_control_status", "")
                    if isinstance(record, dict)
                    else ""
                ),
                "failure_kind": (
                    record.get("failure_kind", "") if isinstance(record, dict) else ""
                ),
                "thread_status_type": (
                    record.get("thread_status_type", "")
                    if isinstance(record, dict)
                    else ""
                ),
                "completion_unconfirmed": (
                    record.get("completion_unconfirmed", False)
                    if isinstance(record, dict)
                    else False
                ),
                "orphaned_delivery_owner": (
                    record.get("orphaned_delivery_owner", False)
                    if isinstance(record, dict)
                    else False
                ),
                "control_plane_unavailable": (
                    record.get("control_plane_unavailable", False)
                    if isinstance(record, dict)
                    else False
                ),
                "native_no_agent_output_count": (
                    record.get("native_no_agent_output_count", 0)
                    if isinstance(record, dict)
                    else 0
                ),
            }
        )
        no_agent_count = (
            int(record.get("native_no_agent_output_count", 1) or 1)
            if isinstance(record, dict)
            else 0
        )
        if (
            active
            and isinstance(record, dict)
            and (
                record.get("thread_status_type") == "systemError"
                or (
                    record.get("failure_kind") == "native_turn_no_agent_output"
                    and no_agent_count >= 2
                )
            )
        ):
            replacement_allowed = solver_session_replacement_allowed(config)
            actions.append(
                {
                    "kind": (
                        "replace_solver_session"
                        if replacement_allowed
                        else "recover_current_solver_session"
                    ),
                    "op": op,
                    "gate_stage": trigger.get("gate_stage", ""),
                    "key": key,
                    "prompt_path": trigger.get("prompt_path", ""),
                    "reason": (
                        "current solver thread completed user-only turns without agent output"
                        if replacement_allowed
                        else "current solver thread completed user-only turns without agent output; keep current session"
                    ),
                }
            )
        elif active and (
            ack_status in {"not-sent", "needs-native-delivery"}
            or (
                trigger_status == "retry-ready"
                and ack_status in {"failed", "interrupted", "cancelled"}
            )
        ):
            actions.append(
                {
                    "kind": "deliver_solver_trigger",
                    "op": op,
                    "gate_stage": trigger.get("gate_stage", ""),
                    "key": key,
                    "prompt_path": trigger.get("prompt_path", ""),
                    "reason": "live board is solver-owned and trigger needs IDE-native delivery",
                }
            )

    for gate in active_gates:
        if not isinstance(gate, dict):
            continue
        key = str(gate.get("key", "") or "")
        if any(item.get("key") == key for item in records):
            continue
        record = sent.get(key) if isinstance(sent, dict) else None
        visibility = visibility_state(record if isinstance(record, dict) else {})
        records.append(
            {
                "op": gate.get("op", ""),
                "gate_stage": gate.get("gate_stage", ""),
                "key": key,
                "trigger_status": gate.get("status", ""),
                "ack_status": (
                    record.get("status", "not-sent")
                    if isinstance(record, dict)
                    else "not-sent"
                ),
                "turn_id": record.get("turn_id") if isinstance(record, dict) else "",
                "delivery": record.get("delivery") if isinstance(record, dict) else "",
                "visibility": visibility["label"],
                "ide_confirmed": visibility["confirmed"],
                "storage_only_ack": visibility["storage_only"],
                "active_solver_gate": True,
                "age_seconds": gate.get("age_seconds"),
                "stale": bool(gate.get("stale")),
                "delivery_retry_reason": (
                    record.get("delivery_retry_reason", "")
                    if isinstance(record, dict)
                    else ""
                ),
                "delivery_retry_after": (
                    record.get("delivery_retry_after", "")
                    if isinstance(record, dict)
                    else ""
                ),
                "last_delivery_error": (
                    record.get("last_delivery_error", "")
                    if isinstance(record, dict)
                    else ""
                ),
                "remote_control_status": (
                    record.get("remote_control_status", "")
                    if isinstance(record, dict)
                    else ""
                ),
                "failure_kind": (
                    record.get("failure_kind", "") if isinstance(record, dict) else ""
                ),
            }
        )

    thread_records: list[dict[str, Any]] = []
    active_ack_by_op = build_active_ack_by_op(
        sent, active_solver_keys=active_solver_keys
    )
    active_gate_by_op = build_active_gate_by_op(active_gates)
    for op, thread_id in sorted(config.solver_threads.items()):
        observed = thread_by_op.get(op, {})
        board = board_by_op.get(op, {})
        latest_turn_id = observed.get("latest_turn_id", "")
        latest_turn_status = observed.get("latest_turn_status", "")
        raw_latest_turn_status = ""
        active_ack = active_ack_by_op.get(op, {})
        if is_transient_terminal_observation(active_ack, observed):
            raw_latest_turn_status = str(latest_turn_status or "")
            latest_turn_status = active_ack.get("status", latest_turn_status)
        active_gate = active_gate_by_op.get(op, {})
        active_gate_status = str(
            active_gate.get("native_status") or active_gate.get("status") or ""
        )
        if op in active_solver_ops and active_gate_status:
            if latest_turn_status != active_gate_status:
                raw_latest_turn_status = raw_latest_turn_status or str(
                    latest_turn_status or ""
                )
            latest_turn_status = active_gate_status
            latest_turn_id = (
                active_gate.get("turn_id")
                or active_gate.get("native_id")
                or latest_turn_id
            )
        visibility_error = str(
            observed.get("resume_error")
            or observed.get("error")
            or observed.get("read_error")
            or ""
        )
        latest_activity_at = str(observed.get("latest_activity_at", "") or "")
        latest_activity_age_seconds = observed.get("idle_seconds")
        if latest_activity_age_seconds is None:
            latest_activity_epoch = timestamp_number(
                observed.get("latest_completed_at", observed.get("latest_started_at"))
            )
            if latest_activity_epoch is not None:
                latest_activity_age_seconds = max(
                    0,
                    int(datetime.now(timezone.utc).timestamp() - latest_activity_epoch),
                )
                if not latest_activity_at:
                    latest_activity_at = datetime.fromtimestamp(
                        latest_activity_epoch,
                        tz=timezone.utc,
                    ).isoformat(timespec="seconds")
        if op in active_solver_ops and active_gate:
            gate_age = active_gate.get("age_seconds")
            if gate_age is not None:
                latest_activity_age_seconds = gate_age
            gate_updated_at = str(active_gate.get("updated_at", "") or "")
            if gate_updated_at:
                latest_activity_at = gate_updated_at
        current_owner = str(board.get("next_owner", "") or "")
        current_gate = str(board.get("gate_stage", "") or "")
        if op in active_solver_ops:
            trigger_policy = "solver-owned: deliver only when trigger_plan is ready/retry-ready and ack is not current"
        elif current_owner:
            trigger_policy = f"observation-only: current owner is {current_owner}"
        else:
            trigger_policy = "observation-only: no live board row"
        thread_records.append(
            {
                "op": op,
                "thread_id": thread_id,
                "current_gate_stage": current_gate,
                "current_owner": current_owner,
                "latest_turn_id": latest_turn_id,
                "latest_turn_status": latest_turn_status,
                "raw_latest_turn_status": raw_latest_turn_status,
                "latest_has_agent_output": observed.get("latest_has_agent_output"),
                "latest_user_only_turn": observed.get("latest_user_only_turn"),
                "idle_seconds": latest_activity_age_seconds,
                "latest_activity_at": latest_activity_at,
                "trigger_policy": trigger_policy,
                "ide_panel_visible": bool(observed.get("ide_panel_visible")),
                "ide_panel_visibility": observed.get("ide_panel_visibility", ""),
                "skipped_synthetic_turn_id": observed.get(
                    "skipped_synthetic_turn_id", ""
                ),
                "thread_status_type": observed.get("thread_status_type", ""),
                "thread_updated_at": observed.get(
                    "thread_updatedAt", observed.get("thread_updated_at", "")
                ),
                "thread_header_stale": bool(observed.get("thread_header_stale")),
                "thread_header_lag_seconds": observed.get("thread_header_lag_seconds"),
                "active_trigger_status": active_gate_status,
                "active_trigger_age_seconds": (
                    active_gate.get("age_seconds") if active_gate else None
                ),
                "resume_error": observed.get("resume_error", ""),
                "read_error": observed.get("read_error", ""),
                "visibility_error": visibility_error,
                "active_solver_thread": op in active_solver_ops,
            }
        )
        if op in active_solver_ops and visibility_error:
            actions.append(
                {
                    "kind": "repair_solver_ide_session",
                    "op": op,
                    "thread_id": thread_id,
                    "reason": "configured solver thread cannot be resumed/read by daemon",
                }
            )
        if observed.get("skipped_synthetic_turn_id"):
            records.append(
                {
                    "op": op,
                    "key": "",
                    "visibility": "synthetic-turn-skipped",
                    "ide_confirmed": bool(observed.get("ide_panel_visible")),
                    "storage_only_ack": False,
                    "active_solver_gate": op in active_solver_ops,
                    "skipped_synthetic_turn_id": observed.get(
                        "skipped_synthetic_turn_id", ""
                    ),
                }
            )

    return {
        "updated_at": thread_observations.get("updated_at", ""),
        "required_thread_reads": solver_session.get("required_thread_reads", []),
        "active_solver_gates": active_gates,
        "records": records,
        "thread_records": thread_records,
        "recommended_actions": actions,
    }


def build_tester_visibility(
    tester_plan: dict[str, Any],
    tester_ack_state: dict[str, Any],
    tester_session: dict[str, Any],
) -> dict[str, Any]:
    sent = (
        tester_ack_state.get("sent", {})
        if isinstance(tester_ack_state.get("sent"), dict)
        else {}
    )
    triggers = (
        tester_plan.get("triggers", [])
        if isinstance(tester_plan.get("triggers"), list)
        else []
    )
    records: list[dict[str, Any]] = []
    for trigger in triggers:
        if not isinstance(trigger, dict):
            continue
        key = str(trigger.get("key", "") or "")
        record = sent.get(key) if isinstance(sent, dict) else None
        visibility = visibility_state(record if isinstance(record, dict) else {})
        raw_status = (
            str(record.get("status", "") or "") if isinstance(record, dict) else ""
        )
        ack_status = (
            normalized_trigger_status(record, "not-sent")
            if isinstance(record, dict)
            else "not-sent"
        )
        records.append(
            {
                "op": trigger.get("op", ""),
                "gate_stage": trigger.get("gate_stage", ""),
                "key": key,
                "trigger_status": trigger.get("status", ""),
                "ack_status": ack_status,
                "raw_ack_status": raw_status if raw_status != ack_status else "",
                "thread_id": trigger.get("thread_id", "")
                or (record.get("thread_id", "") if isinstance(record, dict) else ""),
                "turn_id": (
                    record.get("turn_id", "") if isinstance(record, dict) else ""
                ),
                "delivery": (
                    record.get("delivery", "") if isinstance(record, dict) else ""
                ),
                "updated_at": (
                    record.get("updated_at", "") if isinstance(record, dict) else ""
                ),
                "delivery_retry_reason": (
                    record.get("delivery_retry_reason", "")
                    if isinstance(record, dict)
                    else ""
                ),
                "delivery_retry_after": (
                    record.get("delivery_retry_after", "")
                    if isinstance(record, dict)
                    else ""
                ),
                "last_delivery_error": (
                    record.get("last_delivery_error", "")
                    if isinstance(record, dict)
                    else ""
                ),
                "remote_control_status": (
                    record.get("remote_control_status", "")
                    if isinstance(record, dict)
                    else ""
                ),
                "visibility": visibility["label"],
                "ide_confirmed": visibility["confirmed"],
                "storage_only_ack": visibility["storage_only"],
                "prompt_path": trigger.get("prompt_path", ""),
                "roles": trigger.get("roles", {}),
                "completion_unconfirmed": (
                    record.get("completion_unconfirmed", False)
                    if isinstance(record, dict)
                    else False
                ),
                "orphaned_delivery_owner": (
                    record.get("orphaned_delivery_owner", False)
                    if isinstance(record, dict)
                    else False
                ),
                "control_plane_unavailable": (
                    record.get("control_plane_unavailable", False)
                    if isinstance(record, dict)
                    else False
                ),
            }
        )

    return {
        "updated_at": tester_plan.get("updated_at", "")
        or tester_session.get("updated_at", ""),
        "required_thread_reads": tester_session.get("required_thread_reads", []),
        "missing_tester_threads": tester_session.get("missing_tester_threads", []),
        "active_tester_gates": tester_session.get("active_tester_gates", []),
        "stale_tester_gates": tester_session.get("stale_tester_gates", []),
        "records": records,
    }


def timestamp_number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_solver_thread_poll_summary(poll_status: dict[str, Any]) -> dict[str, Any]:
    if not poll_status:
        return {}
    return {
        "updated_at": poll_status.get("updated_at", ""),
        "status": poll_status.get("status", ""),
        "mode": poll_status.get("mode", ""),
        "thread_count": poll_status.get("thread_count"),
        "ops": poll_status.get("ops", []),
        "poll_fallback": poll_status.get("poll_fallback", ""),
        "fallback_reason": poll_status.get("fallback_reason", ""),
        "error": poll_status.get("error", ""),
    }


def build_relay_capability_summary(
    poll_summary: dict[str, Any],
    app_server_proxy_repair: dict[str, Any],
    relay_capability_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    relay_capability_state = relay_capability_state or {}
    status = str(poll_summary.get("status", "") or "")
    error = str(poll_summary.get("error", "") or "")
    fallback_reason = str(poll_summary.get("fallback_reason", "") or "")
    repair_returncode = app_server_proxy_repair.get("returncode")
    socket_exists = app_server_proxy_repair.get("socket_exists_after")
    stderr_tail = str(app_server_proxy_repair.get("stderr_tail", "") or "")
    platform = str(app_server_proxy_repair.get("platform", "") or "")
    remote_control_repair_supported = not (
        platform.startswith("win")
        and "daemon lifecycle is only supported on Unix platforms" in stderr_tail
    )
    unavailable = status in {"failed", "degraded"} and (
        "proxy control socket is not available" in error
        or "proxy socket unavailable" in fallback_reason
        or repair_returncode not in (None, 0)
        or socket_exists is False
    )
    reason = ""
    if unavailable:
        if stderr_tail:
            reason = stderr_tail.strip().replace("\n", " ")[:300]
        elif error:
            reason = error[:300]
        elif fallback_reason:
            reason = fallback_reason[:300]
        else:
            reason = "app-server proxy socket unavailable"
    state_status = str(relay_capability_state.get("status", "") or "")
    state_reason = str(relay_capability_state.get("reason", "") or "")
    delivery_blocked = state_status == "blocked"
    if delivery_blocked:
        unavailable = True
        if state_reason == "remote_control_not_ready":
            reason = (
                "remoteControl/enable did not reach connected; "
                + (
                    "Windows codex app-server daemon remote-control lifecycle is unsupported; "
                    if not remote_control_repair_supported
                    else ""
                )
                + "external daemon delivery is paused until retry_after"
            )
        elif relay_capability_state.get("last_error"):
            reason = str(relay_capability_state.get("last_error", ""))[:300]
        elif state_reason:
            reason = state_reason[:300]
    return {
        "native_delivery_available": False if unavailable else None,
        "poll_status": status,
        "poll_error": error,
        "fallback_reason": fallback_reason,
        "proxy_repair_time": app_server_proxy_repair.get("time", ""),
        "proxy_repair_returncode": repair_returncode,
        "proxy_socket": app_server_proxy_repair.get("socket", ""),
        "proxy_socket_exists_after": socket_exists,
        "proxy_repair_platform": platform,
        "remote_control_repair_supported": remote_control_repair_supported,
        "delivery_blocked": delivery_blocked,
        "effective_delivery_blocked": delivery_blocked,
        "remote_control_delivery_blocked": delivery_blocked,
        "delivery_blocked_raw": delivery_blocked,
        "delivery_blocker_reason": state_reason,
        "delivery_blocked_until": relay_capability_state.get("retry_after", ""),
        "remote_control_status": relay_capability_state.get(
            "remote_control_status", ""
        ),
        "last_delivery_error": relay_capability_state.get("last_error", ""),
        "reason": reason,
    }


def build_native_relay_outbox_summary(
    outbox: dict[str, Any],
    claims_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    entries = (
        outbox.get("entries", []) if isinstance(outbox.get("entries"), list) else []
    )
    active_claims = active_claims_by_entry(claims_payload or {})
    by_type: dict[str, int] = {}
    by_op: dict[str, int] = {}
    slim: list[dict[str, Any]] = []
    claimed_count = 0
    for item in entries:
        if not isinstance(item, dict):
            continue
        entry_id = str(item.get("id", "") or "")
        claim = active_claims.get(entry_id)
        claim_status = (
            "claimed" if claim else str(item.get("claim_status", "") or "available")
        )
        if claim_status == "claimed":
            claimed_count += 1
        trigger_type = str(item.get("type", "") or "unknown")
        op = str(item.get("op", "") or "")
        by_type[trigger_type] = by_type.get(trigger_type, 0) + 1
        if op:
            by_op[op] = by_op.get(op, 0) + 1
        slim.append(
            {
                "id": item.get("id", ""),
                "type": trigger_type,
                "op": op,
                "gate_stage": item.get("gate_stage", ""),
                "status": item.get("status", ""),
                "thread_id": item.get("thread_id", ""),
                "prompt_path": item.get("prompt_path", ""),
                "claim_status": claim_status,
                "claimed_by": (
                    claim.get("claimed_by", "") if claim else item.get("claimed_by", "")
                ),
                "claim_expires_at": (
                    claim.get("expires_at", "")
                    if claim
                    else item.get("claim_expires_at", "")
                ),
            }
        )
    return {
        "updated_at": outbox.get("updated_at", ""),
        "relay_contract": outbox.get("relay_contract", ""),
        "entry_count": len(slim),
        "claimed_entry_count": claimed_count,
        "available_entry_count": len(slim) - claimed_count,
        "by_type": by_type,
        "by_op": by_op,
        "entries": slim,
    }


def build_native_relay_claim_summary(claims_payload: dict[str, Any]) -> dict[str, Any]:
    active = active_claims_by_entry(claims_payload)
    records: list[dict[str, Any]] = []
    for entry_id, claim in active.items():
        records.append(
            {
                "entry_id": entry_id,
                "claim_id": claim.get("claim_id", ""),
                "type": claim.get("type", ""),
                "op": claim.get("op", ""),
                "gate_stage": claim.get("gate_stage", ""),
                "thread_id": claim.get("thread_id", ""),
                "claimed_by": claim.get("claimed_by", ""),
                "claimed_at": claim.get("claimed_at", ""),
                "expires_at": claim.get("expires_at", ""),
            }
        )
    return {
        "updated_at": claims_payload.get("updated_at", ""),
        "active_claim_count": len(records),
        "claims": records,
    }


def build_solver_replacement_summary(
    replacement_plan: dict[str, Any]
) -> dict[str, Any]:
    replacements = (
        replacement_plan.get("replacements", [])
        if isinstance(replacement_plan.get("replacements"), list)
        else []
    )
    records: list[dict[str, Any]] = []
    for item in replacements:
        if not isinstance(item, dict):
            continue
        records.append(
            {
                "op": item.get("op", ""),
                "gate_stage": item.get("gate_stage", ""),
                "old_thread_id": item.get("old_thread_id", ""),
                "reason": item.get("reason", ""),
                "replacement_prompt_path": item.get("replacement_prompt_path", ""),
                "register_command": item.get("register_command_template", ""),
            }
        )
    return {
        "updated_at": replacement_plan.get("updated_at", ""),
        "replacement_required_count": replacement_plan.get(
            "replacement_required_count", len(records)
        ),
        "replacements": records,
    }


def build_active_ack_by_op(
    sent: dict[str, Any],
    *,
    active_solver_keys: set[str] | None = None,
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for key, record in sent.items():
        key_text = str(key)
        if active_solver_keys is not None and key_text not in active_solver_keys:
            continue
        if not isinstance(record, dict):
            continue
        raw_status = str(record.get("status", "") or "")
        status = normalized_trigger_status(record, raw_status)
        if status not in ACTIVE_ACK_STATUSES:
            continue
        delivery = str(record.get("delivery", "") or "")
        visibility = str(record.get("ide_panel_visibility", "") or "")
        if (
            delivery not in CONFIRMED_IDE_DELIVERIES
            and visibility not in CONFIRMED_IDE_VISIBILITIES
        ):
            continue
        op = key_text.split("|", 1)[0]
        if op:
            normalized = dict(record)
            if raw_status != status:
                normalized["raw_status"] = raw_status
                normalized["status"] = status
            result[op] = normalized
    return result


def build_active_gate_by_op(
    active_gates: list[dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for gate in active_gates:
        if not isinstance(gate, dict):
            continue
        op = str(gate.get("op", "") or "")
        status = str(gate.get("native_status") or gate.get("status") or "")
        if not op or not status:
            continue
        result[op] = gate
    return result


def is_transient_terminal_observation(
    active_ack: dict[str, Any], observed: dict[str, Any]
) -> bool:
    if not active_ack or not observed:
        return False
    observed_status = str(observed.get("latest_turn_status", "") or "")
    if observed_status not in FAILED_TURN_STATUSES:
        return False
    if is_transient_native_poll_failure(active_ack):
        return True
    ack_turn_id = str(active_ack.get("turn_id") or active_ack.get("native_id") or "")
    observed_turn_id = str(observed.get("latest_turn_id", "") or "")
    if not ack_turn_id or observed_turn_id != ack_turn_id:
        return False
    completed_at = observed.get("latest_completed_at")
    duration_ms = observed.get("latest_duration_ms")
    return completed_at in (None, "") and duration_ms in (None, "")


def build_idle_summary(
    config: DaemonConfig,
    efficiency: dict[str, Any],
    gaps: dict[str, Any],
    *,
    resource_leases: list[Any] | None = None,
) -> dict[str, Any]:
    threshold = int(config.policy.get("test_idle_stale_seconds", 480) or 480)
    ack_threshold = int(config.policy.get("solver_trigger_retry_seconds", 300) or 300)
    dispatch_threshold = max(threshold, 600)
    idle_window = optional_int(gaps.get("test_idle_window_seconds"))
    tester_work = (
        efficiency.get("tester_owned_work", [])
        if isinstance(efficiency.get("tester_owned_work"), list)
        else []
    )
    active_resource_leases = [
        lease for lease in (resource_leases or []) if isinstance(lease, dict)
    ]
    resource_idle = bool(efficiency.get("resource_idle"))
    if active_resource_leases:
        resource_idle = False
    gap_warnings: list[str] = []
    recent_ack = optional_int(
        gaps.get("recent_window_max_result_to_solver_ack_seconds")
    )
    recent_dispatch = optional_int(
        gaps.get("recent_window_max_result_to_next_dispatch_seconds")
    )
    submit_gap = (
        gaps.get("completion_to_next_submit", {})
        if isinstance(gaps.get("completion_to_next_submit"), dict)
        else {}
    )
    if recent_ack is not None and recent_ack > ack_threshold:
        gap_warnings.append(
            f"recent result->solver ack gap high: {recent_ack}s threshold={ack_threshold}s"
        )
    if recent_dispatch is not None and recent_dispatch > dispatch_threshold:
        gap_warnings.append(
            f"recent result->next dispatch gap high: {recent_dispatch}s threshold={dispatch_threshold}s"
        )
    if submit_gap:
        violation_count = optional_int(submit_gap.get("violation_count")) or 0
        threshold_seconds = optional_int(submit_gap.get("threshold_seconds"))
        max_gap = optional_int(submit_gap.get("max_gap_seconds"))
        if violation_count > 0:
            gap_warnings.append(
                f"completion->next submit gap violated: max={max_gap}s "
                f"threshold={threshold_seconds}s violations={violation_count}"
            )
    return {
        "resource_idle": resource_idle,
        "active_resource_lease_count": len(active_resource_leases),
        "tester_owned_work": tester_work,
        "test_idle_window_seconds": idle_window,
        "threshold_seconds": threshold,
        "stale": bool(
            resource_idle
            and tester_work
            and idle_window is not None
            and idle_window >= threshold
        ),
        "recent_window_result_count": gaps.get("recent_window_result_count"),
        "recent_window_max_result_to_solver_ack_seconds": recent_ack,
        "recent_window_max_result_to_next_dispatch_seconds": recent_dispatch,
        "completion_to_next_submit": submit_gap,
        "gap_warnings": gap_warnings,
    }


def build_resource_summary(leases: dict[str, Any]) -> list[dict[str, Any]]:
    lease_list = (
        leases.get("leases", []) if isinstance(leases.get("leases"), list) else []
    )
    result: list[dict[str, Any]] = []
    for lease in lease_list:
        if not isinstance(lease, dict):
            continue
        result.append(
            {
                "resource_id": lease.get("resource_id", ""),
                "resource_type": lease.get("resource_type", ""),
                "op": lease.get("op", ""),
                "gate_stage": lease.get("gate_stage", ""),
                "pid": lease.get("pid", ""),
                "acquired_at": lease.get("acquired_at", ""),
                "expires_at": lease.get("expires_at", ""),
                "action_id": lease.get("action_id", ""),
            }
        )
    return result


def build_runtime_residency_summary(
    state_dir: Path,
    *,
    daemon_max_heartbeat_age_seconds: int,
    bridge_max_heartbeat_age_seconds: int,
) -> dict[str, Any]:
    return {
        "supervisor_loop": build_runtime_target_summary(
            state_dir,
            target="supervisor_loop",
            lock_name="supervisor_loop.lock",
            heartbeat_name="supervisor_loop_heartbeat.json",
            process_name="supervisor_loop_process.json",
            max_heartbeat_age_seconds=daemon_max_heartbeat_age_seconds,
        ),
        "daemon": build_runtime_target_summary(
            state_dir,
            target="daemon",
            lock_name="daemon.lock",
            heartbeat_name="daemon_heartbeat.json",
            process_name="daemon_process.json",
            max_heartbeat_age_seconds=daemon_max_heartbeat_age_seconds,
        ),
        "solver_trigger_bridge": build_runtime_target_summary(
            state_dir,
            target="solver_trigger_bridge",
            lock_name="solver_trigger_bridge.lock",
            heartbeat_name="solver_trigger_bridge_heartbeat.json",
            process_name="solver_trigger_bridge_process.json",
            max_heartbeat_age_seconds=bridge_max_heartbeat_age_seconds,
        ),
    }


def build_runtime_target_summary(
    state_dir: Path,
    *,
    target: str,
    lock_name: str,
    heartbeat_name: str,
    process_name: str,
    max_heartbeat_age_seconds: int,
) -> dict[str, Any]:
    lock_pid = read_lock_pid(state_dir / lock_name)
    heartbeat = read_json(state_dir / heartbeat_name)
    process_record = read_json(state_dir / process_name)
    heartbeat_pid = optional_int(heartbeat.get("pid")) or 0
    process_file_pid = optional_int(process_record.get("pid")) or 0
    runtime_pid_candidates = [pid for pid in (lock_pid, process_file_pid) if pid > 0]
    pid_candidates = [
        pid for pid in (lock_pid, process_file_pid, heartbeat_pid) if pid > 0
    ]
    alive_pids = [pid for pid in runtime_pid_candidates if process_alive(pid)]
    heartbeat_time = str(heartbeat.get("time", "") or "")
    heartbeat_age = timestamp_age_seconds(heartbeat_time)
    heartbeat_fresh = heartbeat_age is not None and (
        max_heartbeat_age_seconds <= 0 or heartbeat_age <= max_heartbeat_age_seconds
    )
    process_started_at = str(process_record.get("started_at", "") or "")
    process_started_age = timestamp_age_seconds(process_started_at)
    return {
        "target": target,
        "lock_pid": lock_pid,
        "heartbeat_pid": heartbeat_pid,
        "process_file_pid": process_file_pid,
        "effective_pid": (
            alive_pids[0]
            if alive_pids
            else (pid_candidates[0] if pid_candidates else 0)
        ),
        "alive_pids": alive_pids,
        "process_alive": bool(alive_pids),
        "heartbeat_time": heartbeat_time,
        "heartbeat_age_seconds": heartbeat_age,
        "heartbeat_fresh": bool(heartbeat_fresh),
        "process_started_at": process_started_at,
        "process_started_age_seconds": process_started_age,
        "process_action": process_record.get("action", ""),
        "resident_ok": bool(alive_pids and heartbeat_fresh),
        "max_heartbeat_age_seconds": max_heartbeat_age_seconds,
    }


def timestamp_age_seconds(text: str) -> int | None:
    timestamp = parse_timestamp(text)
    if timestamp is None:
        return None
    return max(0, int((datetime.now(timezone.utc) - timestamp).total_seconds()))


def supervisor_launch_command_hint(config_path: str = "") -> str:
    config_arg = config_path or "<config>"
    return (
        f"python tools\\tester_daemon\\daemon.py supervise-launch --config {config_arg} "
        "--mode execute --write-state --allow-live-execute --clear-stop "
        "--max-heartbeat-age-seconds 120 --bridge-max-heartbeat-age-seconds 120 "
        "--replace-stale-lock-after-seconds 120"
    )


def configured_supervisor_loop_interval_seconds(config: DaemonConfig) -> float:
    configured = config.policy.get(
        "supervise_loop_interval_seconds",
        config.policy.get("run_interval_seconds", 60.0),
    )
    return max(1.0, float(configured or 60.0))


def build_resume_readiness_summary(
    *,
    config: DaemonConfig,
    config_path: str = "",
    stop_request: dict[str, Any],
    heartbeat: dict[str, Any],
    board_rows: list[dict[str, Any]],
    idle: dict[str, Any],
    runtime_residency: dict[str, Any],
    relay_capability: dict[str, Any],
    app_side_relay: dict[str, Any],
    native_relay_outbox: dict[str, Any],
    native_relay_claims: dict[str, Any],
    balance: dict[str, Any],
    recommended_actions: list[dict[str, Any]],
    issues: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    action_kinds = [
        str(action.get("kind", "") or "")
        for action in recommended_actions
        if isinstance(action, dict) and action.get("kind")
    ]
    outbox_count = optional_int(native_relay_outbox.get("entry_count")) or 0
    outbox_available = (
        optional_int(native_relay_outbox.get("available_entry_count")) or 0
    )
    active_claims = optional_int(native_relay_claims.get("active_claim_count")) or 0
    delivery_blocked = bool(
        relay_capability.get("delivery_blocked")
        or relay_capability.get("effective_delivery_blocked")
    )
    effective_delivery = str(relay_capability.get("effective_delivery", "") or "")
    app_side_relay_required = bool(relay_capability.get("app_side_relay_required"))
    app_side_relay_fresh = (
        bool(app_side_relay.get("fresh")) if app_side_relay else False
    )
    app_side_relay_resident_ok = (not app_side_relay_required) or app_side_relay_fresh
    resource_idle = bool(idle.get("resource_idle"))
    tester_owned_work = [
        str(item)
        for item in (
            idle.get("tester_owned_work", [])
            if isinstance(idle.get("tester_owned_work"), list)
            else []
        )
    ]
    submit_gap = idle.get("completion_to_next_submit", {})
    submit_gap = submit_gap if isinstance(submit_gap, dict) else {}

    runnable_harness_ops: list[str] = []
    blocked_harness_ops: list[str] = []
    solver_owned_ops: list[str] = []
    tester_casegen_ops: list[str] = []
    daemon_action_markers = (
        "prepare-submit",
        "gitpartner-run-submit",
        "restore-submit",
        "promote-release",
        "create-v1-regression-sentinel",
        "record-v1-regression-sentinel",
        "create-case-regression-sentinel",
        "record-case-regression-sentinel",
    )
    for row in board_rows:
        if not isinstance(row, dict):
            continue
        op = str(row.get("op", "") or "")
        gate = str(row.get("gate_stage", "") or "")
        owner = str(row.get("next_owner", "") or "")
        command = str(row.get("next_command", "") or "")
        gate_lower = gate.lower()
        command_lower = command.lower()
        if owner == "solver" and op and "gitpartner-run-msopgen" not in command_lower:
            solver_owned_ops.append(op)
        if op and "gitpartner-run-msopgen" in command_lower:
            runnable_harness_ops.append(op)
        if (
            owner in {"daemon", "harness"}
            and op
            and command_assigns_daemon_action(command_lower, daemon_action_markers)
        ):
            runnable_harness_ops.append(op)
        if (
            owner == "tester"
            and op
            and ("case" in gate_lower or "casegen" in gate_lower)
        ):
            tester_casegen_ops.append(op)
        if owner != "tester" or not op:
            continue
        if (
            gate_lower in {"pending-ready", "submit-ready"}
            or "prepare-submit" in command_lower
        ):
            runnable_harness_ops.append(op)
        elif (
            gate_lower in {"submit-blocked", "transport-blocked"}
            or "recover-blocked" in command_lower
        ):
            blocked_harness_ops.append(op)

    harness_can_progress = bool(resource_idle and runnable_harness_ops)
    first_action = action_kinds[0] if action_kinds else ""
    if stop_request:
        first_action = "clear_stop_and_launch_supervisor_loop"
    elif harness_can_progress:
        first_action = "daemon_execute_tick"

    loop_state = "monitoring"
    if stop_request:
        loop_state = "stopped"
    elif delivery_blocked and outbox_count > 0:
        loop_state = "relay-blocked"
    elif outbox_count > 0 and active_claims > 0:
        loop_state = "relay-claimed"
    elif outbox_count > 0:
        loop_state = "relay-pending"
    elif resource_idle and runnable_harness_ops:
        loop_state = "runnable-harness-work"
    elif bool(idle.get("stale")):
        loop_state = "idle-self-check-due"
    elif issues:
        loop_state = "attention"

    traffic_balance = (
        balance.get("traffic_balance", {})
        if isinstance(balance.get("traffic_balance"), dict)
        else {}
    )
    balance_recovery = (
        balance.get("balance_recovery", {})
        if isinstance(balance.get("balance_recovery"), dict)
        else {}
    )
    daemon_runtime = (
        runtime_residency.get("daemon", {})
        if isinstance(runtime_residency.get("daemon"), dict)
        else {}
    )
    supervisor_runtime = (
        runtime_residency.get("supervisor_loop", {})
        if isinstance(runtime_residency.get("supervisor_loop"), dict)
        else {}
    )
    bridge_runtime = (
        runtime_residency.get("solver_trigger_bridge", {})
        if isinstance(runtime_residency.get("solver_trigger_bridge"), dict)
        else {}
    )
    # IDE relay health only gates solver/tester message delivery.  A stale
    # app-side relay must still be visible for hard acceptance, but it should
    # not stop resource-free harness work such as prepare/submit dispatch.
    can_run_now = not bool(stop_request) and (
        not delivery_blocked or harness_can_progress
    )
    return {
        "loop_state": loop_state,
        "can_run_now": can_run_now,
        "requires_clear_stop": bool(stop_request),
        "stop_requested_at": (
            stop_request.get("requested_at", "") if stop_request else ""
        ),
        "stop_reason": stop_request.get("reason", "") if stop_request else "",
        "heartbeat_time": heartbeat.get("time", ""),
        "daemon_mode": heartbeat.get("mode", ""),
        "supervisor_loop_resident_ok": bool(supervisor_runtime.get("resident_ok")),
        "supervisor_loop_alive": bool(supervisor_runtime.get("process_alive")),
        "supervisor_loop_heartbeat_fresh": bool(
            supervisor_runtime.get("heartbeat_fresh")
        ),
        "supervisor_loop_pid": supervisor_runtime.get("effective_pid", 0),
        "daemon_resident_ok": bool(daemon_runtime.get("resident_ok")),
        "daemon_alive": bool(daemon_runtime.get("process_alive")),
        "daemon_heartbeat_fresh": bool(daemon_runtime.get("heartbeat_fresh")),
        "daemon_pid": daemon_runtime.get("effective_pid", 0),
        "bridge_resident_ok": bool(bridge_runtime.get("resident_ok")),
        "bridge_alive": bool(bridge_runtime.get("process_alive")),
        "bridge_heartbeat_fresh": bool(bridge_runtime.get("heartbeat_fresh")),
        "bridge_pid": bridge_runtime.get("effective_pid", 0),
        "supervisor_loop_interval_seconds": configured_supervisor_loop_interval_seconds(
            config
        ),
        "first_action_kind": first_action,
        "action_kinds": action_kinds,
        "safe_recovery_action": (
            "clear_stop_and_launch_supervisor_loop" if stop_request else first_action
        ),
        "resume_command_hint": (
            supervisor_launch_command_hint(config_path) if stop_request else ""
        ),
        "resource_idle": resource_idle,
        "test_idle_window_seconds": idle.get("test_idle_window_seconds"),
        "tester_owned_work": tester_owned_work,
        "runnable_harness_ops": sorted(set(runnable_harness_ops)),
        "blocked_harness_ops": sorted(set(blocked_harness_ops)),
        "solver_owned_ops": sorted(set(solver_owned_ops)),
        "tester_casegen_ops": sorted(set(tester_casegen_ops)),
        "relay_effective_delivery": effective_delivery,
        "relay_delivery_blocked": delivery_blocked,
        "app_side_relay_required": app_side_relay_required,
        "app_side_relay_fresh": app_side_relay_fresh,
        "app_side_relay_resident_ok": app_side_relay_resident_ok,
        "native_relay_outbox_count": outbox_count,
        "native_relay_available_count": outbox_available,
        "native_relay_active_claim_count": active_claims,
        "submit_gap_ok": submit_gap.get("ok") if submit_gap else None,
        "submit_gap_max_seconds": (
            submit_gap.get("max_gap_seconds") if submit_gap else None
        ),
        "submit_gap_violation_count": (
            submit_gap.get("violation_count") if submit_gap else None
        ),
        "traffic_balance_ok": traffic_balance.get("ok") if traffic_balance else None,
        "traffic_balance_debt": (
            traffic_balance.get("debt", {}) if traffic_balance else {}
        ),
        "next_debt_target": (
            balance_recovery.get("next_debt_target", "") if balance_recovery else ""
        ),
        "issue_count": len(issues),
        "warning_count": len(warnings),
    }


def build_solver_tester_rule_summary(
    *,
    config: DaemonConfig,
    captured_at: str,
    board_rows: list[dict[str, Any]],
    solver_ack_state: dict[str, Any],
    tester_ack_state: dict[str, Any],
    solver_visibility: dict[str, Any],
    tester_visibility: dict[str, Any],
    native_relay_outbox: dict[str, Any],
    native_relay_claims: dict[str, Any],
    native_relay_events: list[dict[str, Any]] | None = None,
    knowledge_usage: dict[str, Any] | None = None,
) -> dict[str, Any]:
    active_ops = {str(op) for op in config.operators if str(op)}
    recent_seconds = int(
        config.policy.get("hard_acceptance_recent_trigger_seconds", 3600) or 3600
    )
    now = parse_timestamp(captured_at) or datetime.now(timezone.utc)
    solver_reads = [
        item
        for item in solver_visibility.get("required_thread_reads", [])
        if isinstance(item, dict) and str(item.get("op", "") or "") in active_ops
    ]
    tester_reads = [
        item
        for item in tester_visibility.get("required_thread_reads", [])
        if isinstance(item, dict) and str(item.get("op", "") or "") in active_ops
    ]
    solver_recent = recent_ide_visible_ack_records(
        solver_ack_state,
        now=now,
        recent_seconds=recent_seconds,
        active_ops=active_ops,
        trigger_kind="solver",
    )
    tester_recent = recent_ide_visible_ack_records(
        tester_ack_state,
        now=now,
        recent_seconds=recent_seconds,
        active_ops=active_ops,
        trigger_kind="tester",
    )
    solver_recent = merge_recent_trigger_records(
        solver_recent,
        recent_ide_visible_relay_events(
            native_relay_events or [],
            now=now,
            recent_seconds=recent_seconds,
            active_ops=active_ops,
            trigger_kind="solver",
        ),
    )
    tester_recent = merge_recent_trigger_records(
        tester_recent,
        recent_ide_visible_relay_events(
            native_relay_events or [],
            now=now,
            recent_seconds=recent_seconds,
            active_ops=active_ops,
            trigger_kind="tester",
        ),
    )
    current_solver_owned = [
        row
        for row in board_rows
        if str(row.get("op", "") or "") in active_ops
        and str(row.get("next_owner", "") or "") == "solver"
        and "gitpartner-run-msopgen" not in str(row.get("next_command", "") or "")
    ]
    current_casegen_gates = [
        row
        for row in board_rows
        if str(row.get("op", "") or "") in active_ops
        and row_looks_like_casegen_gate(row)
    ]
    outbox_entries = (
        native_relay_outbox.get("entries", [])
        if isinstance(native_relay_outbox.get("entries"), list)
        else []
    )
    solver_outbox_count = count_relay_entries(outbox_entries, "solver")
    tester_outbox_count = count_relay_entries(outbox_entries, "tester")
    solver_outbox_ops = ops_with_relay_entries(outbox_entries, "solver")
    tester_outbox_ops = ops_with_relay_entries(outbox_entries, "tester")
    active_solver_gate_ops = ops_from_records(
        solver_visibility.get("active_solver_gates", [])
    )
    active_tester_gate_ops = ops_from_records(
        tester_visibility.get("active_tester_gates", [])
    )
    current_solver_ops = {str(row.get("op", "") or "") for row in current_solver_owned}
    current_casegen_ops = {
        str(row.get("op", "") or "") for row in current_casegen_gates
    }
    solver_missing_coverage_ops = sorted(
        current_solver_ops - (solver_outbox_ops | active_solver_gate_ops)
    )
    casegen_missing_coverage_ops = sorted(
        current_casegen_ops - (tester_outbox_ops | active_tester_gate_ops)
    )
    tester_missing_threads = [
        item
        for item in tester_visibility.get("missing_tester_threads", [])
        if isinstance(item, dict) and str(item.get("op", "") or "") in active_ops
    ]
    board_contract_issues = board_role_contract_issues(
        board_rows, active_ops=active_ops
    )
    working_set_issues: list[str] = []
    if knowledge_usage is not None:
        knowledge_operators = knowledge_usage.get("operators", {})
        if not isinstance(knowledge_operators, dict):
            knowledge_operators = {}
        for op in sorted(active_ops):
            operator_usage = knowledge_operators.get(op, {})
            shared = (
                operator_usage.get("shared_knowledge", {})
                if isinstance(operator_usage, dict)
                else {}
            )
            if not isinstance(shared, dict):
                shared = {}
            if not shared.get("current_focus_exists"):
                working_set_issues.append(
                    f"operator knowledge working set missing: {op}/current_focus.md"
                )
            elif not shared.get("current_focus_sections_complete"):
                working_set_issues.append(
                    f"operator knowledge working set sections incomplete: {op}/current_focus.md"
                )
            elif not shared.get("current_focus_bounded"):
                working_set_issues.append(
                    "operator knowledge working set exceeds 80 lines: "
                    f"{op}/current_focus.md lines={shared.get('current_focus_line_count', 0)}"
                )
    checks = {
        "solver_threads_configured": len(solver_reads) >= len(active_ops)
        and all(item.get("thread_id") for item in solver_reads),
        "tester_threads_configured": len(tester_reads)
        >= len([op for op in active_ops if casegen_role_enabled(config, op)])
        and not tester_missing_threads,
        "recent_solver_ide_trigger": bool(solver_recent),
        "recent_tester_casegen_ide_trigger": not current_casegen_ops
        or bool(tester_recent),
        "current_solver_gate_has_relay_coverage": not solver_missing_coverage_ops,
        "current_casegen_gate_has_relay_coverage": not casegen_missing_coverage_ops,
        "board_role_contract_consistent": not board_contract_issues,
        "operator_knowledge_working_sets_ok": not working_set_issues,
    }
    blockers: list[str] = []
    if not checks["solver_threads_configured"]:
        blockers.append("configured active solver sessions are incomplete")
    if not checks["tester_threads_configured"]:
        blockers.append("configured active tester casegen sessions are incomplete")
    if not checks["recent_solver_ide_trigger"]:
        blockers.append(
            f"no IDE-visible solver trigger observed in the last {recent_seconds}s"
        )
    if not checks["recent_tester_casegen_ide_trigger"]:
        blockers.append(
            f"no IDE-visible tester casegen trigger observed in the last {recent_seconds}s"
        )
    if not checks["current_solver_gate_has_relay_coverage"]:
        blockers.append(
            "current solver-owned gate has no active relay/outbox coverage for "
            + ", ".join(solver_missing_coverage_ops)
        )
    if not checks["current_casegen_gate_has_relay_coverage"]:
        blockers.append(
            "current tester casegen gate has no active relay/outbox coverage for "
            + ", ".join(casegen_missing_coverage_ops)
        )
    if not checks["board_role_contract_consistent"]:
        blockers.extend(board_contract_issues)
    if not checks["operator_knowledge_working_sets_ok"]:
        blockers.extend(working_set_issues)
    return {
        "ok": all(checks.values()),
        "checks": checks,
        "blockers": blockers,
        "recent_window_seconds": recent_seconds,
        "recent_solver_ide_trigger_count": len(solver_recent),
        "recent_tester_casegen_ide_trigger_count": len(tester_recent),
        "recent_solver_ide_triggers": solver_recent[:5],
        "recent_tester_casegen_ide_triggers": tester_recent[:5],
        "current_solver_owned_ops": sorted(current_solver_ops),
        "current_casegen_ops": sorted(current_casegen_ops),
        "solver_missing_relay_coverage_ops": solver_missing_coverage_ops,
        "casegen_missing_relay_coverage_ops": casegen_missing_coverage_ops,
        "solver_outbox_count": solver_outbox_count,
        "tester_outbox_count": tester_outbox_count,
        "solver_outbox_ops": sorted(solver_outbox_ops),
        "tester_outbox_ops": sorted(tester_outbox_ops),
        "active_solver_gate_ops": sorted(active_solver_gate_ops),
        "active_tester_gate_ops": sorted(active_tester_gate_ops),
        "board_role_contract_issues": board_contract_issues,
        "operator_knowledge_working_set_issues": working_set_issues,
        "active_claim_count": int(
            native_relay_claims.get("active_claim_count", 0) or 0
        ),
    }


def board_role_contract_issues(
    board_rows: list[dict[str, Any]],
    *,
    active_ops: set[str],
) -> list[str]:
    issues: list[str] = []
    daemon_markers = (
        "prepare-submit",
        "gitpartner-run-submit",
        "restore-submit",
        "promote-release",
        "create-v1-regression-sentinel",
        "record-v1-regression-sentinel",
        "create-case-regression-sentinel",
        "record-case-regression-sentinel",
    )
    for row in board_rows:
        op = str(row.get("op", "") or "")
        if op not in active_ops:
            continue
        owner = str(row.get("next_owner", "") or "").lower()
        command = str(row.get("next_command", "") or "").lower()
        if owner == "tester" and not row_looks_like_casegen_gate(row):
            issues.append(f"board role drift: {op} assigns non-casegen work to Tester")
        if owner == "solver" and command_assigns_daemon_action(command, daemon_markers):
            issues.append(f"board role drift: {op} assigns daemon command to Solver")
        if owner == "daemon" and command_assigns_casegen_action(command):
            issues.append(
                f"board role drift: {op} assigns semantic casegen work to daemon"
            )
    return issues


def command_assigns_daemon_action(command: str, markers: tuple[str, ...]) -> bool:
    """Recognize executable daemon actions without matching negated guardrail prose."""
    normalized = " ".join(str(command or "").lower().split())
    for marker in markers:
        escaped = re.escape(marker)
        if re.search(rf"^(?:python\s+\S+\s+)?{escaped}(?:\s|$)", normalized):
            return True
        if re.search(
            rf"\b(?:next_workflow\.py|daemon\.py)\s+{escaped}(?:\s|$)", normalized
        ):
            return True
    return False


def command_assigns_casegen_action(command: str) -> bool:
    """Match an executable casegen action, not a durable casegen evidence path."""
    normalized = " ".join(str(command or "").lower().split())
    markers = ("generate-case-version",)
    return command_assigns_daemon_action(normalized, markers)


def recent_ide_visible_ack_records(
    ack_state: dict[str, Any],
    *,
    now: datetime,
    recent_seconds: int,
    active_ops: set[str],
    trigger_kind: str,
) -> list[dict[str, Any]]:
    sent = ack_state.get("sent", {}) if isinstance(ack_state.get("sent"), dict) else {}
    records: list[dict[str, Any]] = []
    for key, record in sent.items():
        if not isinstance(record, dict):
            continue
        op = str(record.get("op", "") or "") or str(key).split("|", 1)[0]
        if op not in active_ops:
            continue
        if trigger_kind == "tester" and not key_looks_like_casegen(str(key)):
            continue
        visibility = visibility_state(record)
        if not visibility["confirmed"]:
            continue
        delivery = str(record.get("delivery", "") or "")
        timestamp_text = str(
            record.get("last_observed_at")
            or record.get("updated_at")
            or record.get("delivered_at")
            or ""
        )
        ts = parse_timestamp(timestamp_text)
        if ts is None:
            continue
        age = max(0, int((now - ts).total_seconds()))
        if recent_seconds > 0 and age > recent_seconds:
            continue
        records.append(
            {
                "op": op,
                "key": str(key),
                "status": normalized_trigger_status(
                    record, str(record.get("status", "") or "")
                ),
                "thread_id": record.get("thread_id", ""),
                "turn_id": record.get("turn_id", record.get("native_id", "")),
                "delivery": delivery,
                "ide_panel_visibility": visibility["label"],
                "updated_at": record.get("updated_at", ""),
                "last_observed_at": record.get("last_observed_at", ""),
                "age_seconds": age,
            }
        )
    records.sort(key=lambda item: int(item.get("age_seconds", 0) or 0))
    return records


def recent_ide_visible_relay_events(
    events: list[dict[str, Any]],
    *,
    now: datetime,
    recent_seconds: int,
    active_ops: set[str],
    trigger_kind: str,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for event in events:
        if (
            not isinstance(event, dict)
            or str(event.get("kind", "") or "") != trigger_kind
        ):
            continue
        op = (
            str(event.get("op", "") or "")
            or str(event.get("key", "") or "").split("|", 1)[0]
        )
        key = str(event.get("key", "") or "")
        if op not in active_ops or (
            trigger_kind == "tester" and not key_looks_like_casegen(key)
        ):
            continue
        if event.get("ide_panel_visible") is not True:
            continue
        if str(event.get("delivery", "") or "") not in CONFIRMED_IDE_DELIVERIES:
            continue
        ts = parse_timestamp(str(event.get("updated_at") or event.get("time") or ""))
        if ts is None:
            continue
        age = max(0, int((now - ts).total_seconds()))
        if recent_seconds > 0 and age > recent_seconds:
            continue
        records.append(
            {
                "op": op,
                "key": key,
                "status": str(event.get("status", "") or "sent"),
                "thread_id": event.get("thread_id", ""),
                "turn_id": event.get("turn_id", ""),
                "delivery": event.get("delivery", ""),
                "ide_panel_visibility": str(
                    event.get("ide_panel_visibility", "") or "confirmed_by_native_relay"
                ),
                "updated_at": event.get("updated_at", event.get("time", "")),
                "last_observed_at": "",
                "age_seconds": age,
                "evidence_source": "native_relay_app_side_events",
            }
        )
    records.sort(key=lambda item: int(item.get("age_seconds", 0) or 0))
    return records


def merge_recent_trigger_records(
    ack_records: list[dict[str, Any]],
    relay_records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str, str], dict[str, Any]] = {}
    for record in (*relay_records, *ack_records):
        identity = (
            str(record.get("key", "") or ""),
            str(record.get("thread_id", "") or ""),
            str(record.get("turn_id", "") or ""),
        )
        previous = merged.get(identity)
        if previous is None or int(record.get("age_seconds", 0) or 0) < int(
            previous.get("age_seconds", 0) or 0
        ):
            merged[identity] = record
    return sorted(
        merged.values(), key=lambda item: int(item.get("age_seconds", 0) or 0)
    )


def row_looks_like_casegen_gate(row: dict[str, Any]) -> bool:
    if str(row.get("next_owner", "") or "").lower() != "tester":
        return False
    text = " ".join(
        str(row.get(name, "") or "")
        for name in ("gate_stage", "next_command", "wakeups", "reason")
    )
    return key_looks_like_casegen(text)


def key_looks_like_casegen(text: str) -> bool:
    lowered = text.lower()
    return any(
        marker in lowered
        for marker in (
            "casegen",
            "needs_casegen",
            "needs-case-version",
            "generate-case-version",
            "casegen-evidence",
        )
    )


def count_relay_entries(entries: list[Any], trigger_type: str) -> int:
    return sum(
        1
        for item in entries
        if isinstance(item, dict) and str(item.get("type", "") or "") == trigger_type
    )


def ops_with_relay_entries(entries: list[Any], trigger_type: str) -> set[str]:
    return {
        str(item.get("op", "") or "")
        for item in entries
        if isinstance(item, dict)
        and str(item.get("type", "") or "") == trigger_type
        and str(item.get("op", "") or "")
    }


def ops_from_records(records: Any) -> set[str]:
    if not isinstance(records, list):
        return set()
    return {
        str(item.get("op", "") or "")
        for item in records
        if isinstance(item, dict) and str(item.get("op", "") or "")
    }


def build_hard_acceptance_summary(
    readiness: dict[str, Any],
    solver_tester_rules: dict[str, Any] | None = None,
) -> dict[str, Any]:
    def as_bool(value: Any) -> bool:
        return bool(value)

    solver_tester_rules = solver_tester_rules or {}
    relay_resident = as_bool(readiness.get("bridge_resident_ok")) or (
        as_bool(readiness.get("app_side_relay_required"))
        and as_bool(readiness.get("app_side_relay_resident_ok"))
    )
    checks = {
        "no_stop_request": not as_bool(readiness.get("requires_clear_stop")),
        "supervisor_loop_resident": as_bool(
            readiness.get("supervisor_loop_resident_ok")
        ),
        "daemon_resident": as_bool(readiness.get("daemon_resident_ok")),
        "relay_resident": relay_resident,
        "relay_not_blocked": not as_bool(readiness.get("relay_delivery_blocked")),
        "app_side_relay_ready": as_bool(readiness.get("app_side_relay_resident_ok")),
        "solver_tester_rules_ok": solver_tester_rules.get("ok") is True,
        "traffic_balance_ok": readiness.get("traffic_balance_ok") is True,
        "submit_gap_ok": readiness.get("submit_gap_ok") is True,
        "no_idle_runnable_work": not (
            as_bool(readiness.get("resource_idle"))
            and bool(readiness.get("runnable_harness_ops") or [])
            and not as_bool(readiness.get("requires_clear_stop"))
        ),
    }
    blockers: list[str] = []
    if not checks["no_stop_request"]:
        blockers.append("daemon stop request is present")
    if not checks["supervisor_loop_resident"]:
        blockers.append("supervisor loop is not resident")
    if not checks["daemon_resident"]:
        blockers.append("daemon process is not resident")
    if not checks["relay_resident"]:
        blockers.append("no native relay delivery process is resident")
    if not checks["relay_not_blocked"]:
        blockers.append("native relay delivery is blocked")
    if not checks["app_side_relay_ready"]:
        blockers.append("app-side native relay is required but stale")
    if not checks["solver_tester_rules_ok"]:
        blockers.extend(
            str(item) for item in solver_tester_rules.get("blockers", []) if item
        )
    if not checks["traffic_balance_ok"]:
        blockers.append("traffic balance window is not passing")
    if not checks["submit_gap_ok"]:
        blockers.append("completion-to-next-submit gap window is not passing")
    if not checks["no_idle_runnable_work"]:
        blockers.append("resource is idle while runnable harness work exists")
    return {
        "ok": all(checks.values()),
        "checks": checks,
        "blockers": blockers,
        "submit_gap_max_seconds": readiness.get("submit_gap_max_seconds"),
        "submit_gap_violation_count": readiness.get("submit_gap_violation_count"),
        "traffic_balance_debt": readiness.get("traffic_balance_debt", {}),
        "next_debt_target": readiness.get("next_debt_target", ""),
        "solver_tester_rules": solver_tester_rules,
    }


def build_transport_summary(
    state: dict[str, Any], efficiency: dict[str, Any]
) -> list[dict[str, Any]]:
    raw_items = (
        state.get("transport", []) if isinstance(state.get("transport"), list) else []
    )
    if not raw_items:
        operators = (
            efficiency.get("operators", {})
            if isinstance(efficiency.get("operators"), dict)
            else {}
        )
        for data in operators.values():
            if isinstance(data, dict) and isinstance(data.get("transport"), list):
                raw_items.extend(
                    item for item in data["transport"] if isinstance(item, dict)
                )
    result: list[dict[str, Any]] = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        result.append(
            {
                "op": item.get("op", ""),
                "test_version": item.get("test_version", ""),
                "state": item.get("state", ""),
                "client_state": item.get("client_state", ""),
                "client_updated_at": item.get("client_updated_at", ""),
                "terminal": bool(item.get("terminal")),
                "stalled": bool(item.get("stalled")),
                "remote_feedback_status": item.get("remote_feedback_status", ""),
                "elapsed_without_remote_feedback_seconds": item.get(
                    "elapsed_without_remote_feedback_seconds",
                    item.get("elapsed_without_feedback_seconds", ""),
                ),
                "first_observed_at_utc": item.get("first_observed_at_utc", ""),
                "last_feedback_at_utc": item.get("last_feedback_at_utc", ""),
                "summary": item.get("summary", ""),
            }
        )
    return result


def build_live_transport_summary(
    observations: tuple[TransportObservation, ...]
) -> list[dict[str, Any]]:
    return [
        {
            "op": item.op,
            "test_version": item.test_version,
            "state": item.state,
            "client_state": item.client_state,
            "client_updated_at": item.client_updated_at,
            "terminal": bool(item.terminal),
            "stalled": bool(item.stalled),
            "remote_feedback_status": item.remote_feedback_status,
            "elapsed_without_remote_feedback_seconds": item.elapsed_without_remote_feedback_seconds,
            "first_observed_at_utc": item.first_observed_at_utc,
            "last_feedback_at_utc": item.last_feedback_at_utc,
            "summary": item.summary,
        }
        for item in observations
    ]


def build_balance_summary(
    root: Path, efficiency: dict[str, Any], board_rows: list[dict[str, Any]]
) -> dict[str, Any]:
    operators = (
        efficiency.get("operators", {})
        if isinstance(efficiency.get("operators"), dict)
        else {}
    )
    board_by_op = {
        str(row.get("op", "") or ""): row for row in board_rows if row.get("op")
    }
    op_names = sorted(set(operators) | set(board_by_op))
    operator_summary = {
        op: build_balance_operator(
            root, op, operators.get(op, {}), board_by_op.get(op, {})
        )
        for op in op_names
    }
    traffic_balance = efficiency.get("traffic_balance", {})
    return {
        "recent_selected_balance": efficiency.get("recent_selected_balance", {}),
        "recent_dispatch_balance": efficiency.get("recent_dispatch_balance", {}),
        "traffic_balance": traffic_balance,
        "balance_recovery": build_balance_recovery_plan(
            traffic_balance, operator_summary
        ),
        "operators": operator_summary,
    }


def build_balance_operator(
    root: Path,
    op: str,
    efficiency_data: Any,
    board_row: dict[str, Any],
) -> dict[str, Any]:
    data = efficiency_data if isinstance(efficiency_data, dict) else {}
    latest_result = (
        data.get("latest_result") if isinstance(data.get("latest_result"), dict) else {}
    )
    latest_case = (
        data.get("latest_case") if isinstance(data.get("latest_case"), dict) else {}
    )
    board_gate = str(board_row.get("gate_stage", "") or "")
    board_owner = str(board_row.get("next_owner", "") or "")
    board_result = current_board_result(root, op, board_row)
    result = board_result or latest_result
    return {
        "gate_stage": board_gate or data.get("gate_stage", ""),
        "next_owner": board_owner or data.get("next_owner", ""),
        "action": data.get("action", ""),
        "reason": data.get("reason", ""),
        "latest_result": result.get("test_version", ""),
        "latest_verdict": result.get("verdict", ""),
        "case": result.get("case_version", "") or latest_case.get("case_version", ""),
        "usage": latest_case.get("usage_count", ""),
    }


def build_balance_recovery_plan(
    traffic_balance: Any,
    operators: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    balance = traffic_balance if isinstance(traffic_balance, dict) else {}
    debt_raw = balance.get("debt", {}) if isinstance(balance.get("debt"), dict) else {}
    debt: dict[str, int] = {}
    for op, value in debt_raw.items():
        try:
            debt[str(op)] = max(0, int(value))
        except (TypeError, ValueError):
            continue
    debt_ops = [op for op, value in debt.items() if value > 0]
    plans = [
        build_operator_debt_plan(op, debt.get(op, 0), operators.get(op, {}))
        for op in sorted(debt_ops, key=lambda name: (-debt.get(name, 0), name))
    ]
    uncompensated = [
        str(plan.get("op", "") or "")
        for plan in plans
        if not bool(plan.get("compensated"))
    ]
    next_target = plans[0]["op"] if plans else ""
    return {
        "ok": not plans,
        "next_debt_target": next_target,
        "uncompensated_ops": uncompensated,
        "plans": plans,
    }


def build_operator_debt_plan(
    op: str, debt: int, data: dict[str, Any]
) -> dict[str, Any]:
    gate = str(data.get("gate_stage", "") or "")
    owner = str(data.get("next_owner", "") or "")
    selected_action = str(data.get("action", "") or "")
    reason = str(data.get("reason", "") or "")
    gate_lower = gate.lower()
    owner_lower = owner.lower()
    state = "blocked_or_manual"
    action = "inspect live board and blocker before spending another test slot"
    compensated = False
    if owner_lower in {"daemon", "tester"} and (
        gate_lower in {"pending-ready", "submit-ready"}
        or "prepare-submit" in gate_lower
        or "submit-ready" in gate_lower
    ):
        state = "runnable_submit"
        action = "dispatch this debt operator before non-debt submits when the resource is idle"
        compensated = True
    elif owner_lower in {"daemon", "tester"} and (
        gate_lower == "submit-waiting"
        or "waiting" in gate_lower
        or "queued" in gate_lower
    ):
        state = "queued_behind_resource"
        action = "dispatch this debt operator first after the active GitPartner lease completes"
        compensated = True
    elif owner_lower in {"daemon", "tester"} and gate_lower == "submit-running":
        state = "active_or_running"
        action = "debt operator already owns a running submit slot"
        compensated = True
    elif owner_lower == "solver":
        state = "awaiting_solver_output"
        action = "observe solver turn; deliver only a real ready/retry-ready trigger, then dispatch after pending appears"
        compensated = (
            selected_action == "notify_solver"
            or "solver trigger already active" in reason.lower()
        )
    elif owner_lower == "tester" and ("case" in gate_lower or "casegen" in gate_lower):
        state = "awaiting_tester_casegen"
        action = "observe or deliver the real casegen trigger; submit remains blocked until required evidence exists"
        compensated = (
            selected_action == "notify_tester"
            or "tester trigger already active" in reason.lower()
        )
    return {
        "op": op,
        "debt": debt,
        "gate_stage": gate,
        "next_owner": owner,
        "state": state,
        "compensated": compensated,
        "recovery_action": action,
    }


def current_board_result(
    root: Path, op: str, board_row: dict[str, Any]
) -> dict[str, Any]:
    if str(board_row.get("gate_stage", "") or "") != "result-exists":
        return {}
    result_path = result_path_from_board_command(
        root, op, str(board_row.get("next_command", "") or "")
    )
    if result_path is None or not result_path.exists():
        return {}
    return parse_result_summary(result_path, op)


def result_path_from_board_command(root: Path, op: str, command: str) -> Path | None:
    marker = f"operators_testresult\\{op}\\"
    normalized = command.replace("/", "\\")
    start = normalized.find(marker)
    if start < 0:
        return None
    rest = normalized[start + len(marker) :]
    test_version = rest.split("\\", 1)[0].strip()
    if not test_version:
        return None
    return root / "operators_testresult" / op / test_version / "RESULT.md"


def parse_result_summary(path: Path, op: str) -> dict[str, Any]:
    result: dict[str, Any] = {"test_version": path.parent.name}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return result
    prefix_map = {
        "Verdict:": "verdict",
        "Case version:": "case_version",
    }
    for line in lines:
        for prefix, key in prefix_map.items():
            if line.startswith(prefix):
                result[key] = line[len(prefix) :].strip()
        if line.startswith("# Result "):
            parts = line.split()
            if len(parts) >= 4 and parts[2] == op:
                result["test_version"] = parts[3]
    return result


def build_performance_summary(performance: dict[str, Any]) -> dict[str, Any]:
    operators = (
        performance.get("operators", {})
        if isinstance(performance.get("operators"), dict)
        else {}
    )
    return {
        op: {
            "latest_pass": (data.get("latest_pass") or {}).get("test_version", ""),
            "latest_weighted_us": (data.get("latest_pass") or {}).get(
                "weighted_time_us"
            ),
            "active_release": (data.get("active_release") or {}).get(
                "release_version", ""
            ),
            "active_release_weighted_us": (data.get("active_release") or {}).get(
                "weighted_time_us"
            ),
            "failure_streak": data.get("failure_streak", 0),
            "latest_vs_best_recent_same_case_pct": data.get(
                "latest_vs_best_recent_same_case_pct"
            ),
        }
        for op, data in operators.items()
        if isinstance(data, dict)
    }


def build_iteration_quality_summary(
    iteration_quality: dict[str, Any]
) -> dict[str, Any]:
    operators = (
        iteration_quality.get("operators", {})
        if isinstance(iteration_quality.get("operators"), dict)
        else {}
    )
    summary: dict[str, Any] = {}
    for op, data in operators.items():
        if not isinstance(data, dict):
            continue
        latest = data.get("latest", {}) if isinstance(data.get("latest"), dict) else {}
        review = (
            latest.get("review", {}) if isinstance(latest.get("review"), dict) else {}
        )
        route = (
            latest.get("route_reasoning", {})
            if isinstance(latest.get("route_reasoning"), dict)
            else {}
        )
        summary[op] = {
            "latest": latest.get("test_version", ""),
            "case_version": latest.get("case_version", ""),
            "weighted_time_us": latest.get("weighted_time_us"),
            "best_same_case": latest.get("best_same_case_version", ""),
            "best_same_case_weighted_us": latest.get("best_same_case_weighted_us"),
            "regression_vs_best_pct": latest.get("regression_vs_best_pct"),
            "review_status": latest.get("review_status", "missing"),
            "self_verdict": review.get("self_verdict", ""),
            "next_action": review.get("next_action", ""),
            "lineage_resolved": latest.get("lineage_resolved", False),
            "route_reasoning_applicable": bool(route.get("applicable")),
            "route_reasoning_status": route.get("status", "legacy"),
            "route_reasoning_score": route.get("score", 0),
            "route_reasoning_max_score": route.get("max_score", 0),
            "router_gap": (
                route.get("fields", {}).get("Router gap", "")
                if isinstance(route.get("fields"), dict)
                else ""
            ),
        }
    return summary


def visibility_state(record: dict[str, Any]) -> dict[str, Any]:
    visibility = str(record.get("ide_panel_visibility", "") or "")
    delivery = str(record.get("delivery", "") or "")
    confirmed = (
        record.get("ide_panel_visible") is True
        or (
            visibility in CONFIRMED_IDE_VISIBILITIES
            and bool(
                record.get("turn_id")
                or record.get("native_id")
                or record.get("storage_visible")
                or record.get("native_visible")
            )
        )
    ) and (
        visibility in CONFIRMED_IDE_VISIBILITIES or delivery in CONFIRMED_IDE_DELIVERIES
    )
    storage_only = (
        bool(record.get("storage_visible") or record.get("native_visible"))
        and not confirmed
    )
    if confirmed:
        label = visibility or "confirmed_by_native_relay"
    elif storage_only:
        label = "storage_visible_not_ide_visible"
    elif visibility in STORAGE_ONLY_VISIBILITIES:
        label = visibility or "not_visible"
    else:
        label = visibility
    return {"confirmed": confirmed, "storage_only": storage_only, "label": label}


def trigger_key(row: dict[str, Any]) -> str:
    op = str(row.get("op", "") or "")
    gate = str(row.get("gate_stage", "") or "")
    command = str(row.get("next_command", "") or "")
    if not op or not gate or not command:
        return ""
    return "|".join([op, gate, command])


def write_status_query_files(root: Path, payload: dict[str, Any]) -> None:
    state_dir = root / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "STATE_QUERY.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    (state_dir / "STATE_QUERY.md").write_text(
        render_status_query(payload), encoding="utf-8"
    )


def render_status_query(payload: dict[str, Any]) -> str:
    lines = [
        "# Tester Daemon State Query",
        "",
        f"- captured_at: {payload.get('captured_at', '')}",
        f"- status: {payload.get('status', '')}",
        "",
        "## Resume Readiness",
        "",
    ]
    readiness = payload.get("resume_readiness", {})
    if isinstance(readiness, dict) and readiness:
        lines.extend(
            [
                f"- loop_state: {readiness.get('loop_state', '-') or '-'}",
                f"- can_run_now: {readiness.get('can_run_now', '-')}",
                f"- requires_clear_stop: {readiness.get('requires_clear_stop', '-')}",
                f"- first_action_kind: {readiness.get('first_action_kind', '-') or '-'}",
                f"- safe_recovery_action: {readiness.get('safe_recovery_action', '-') or '-'}",
                f"- supervisor_loop_resident_ok: {readiness.get('supervisor_loop_resident_ok', '-')}",
                f"- supervisor_loop_alive: {readiness.get('supervisor_loop_alive', '-')}",
                f"- supervisor_loop_heartbeat_fresh: {readiness.get('supervisor_loop_heartbeat_fresh', '-')}",
                f"- supervisor_loop_pid: {readiness.get('supervisor_loop_pid', '-')}",
                f"- daemon_resident_ok: {readiness.get('daemon_resident_ok', '-')}",
                f"- daemon_alive: {readiness.get('daemon_alive', '-')}",
                f"- daemon_heartbeat_fresh: {readiness.get('daemon_heartbeat_fresh', '-')}",
                f"- daemon_pid: {readiness.get('daemon_pid', '-')}",
                f"- bridge_resident_ok: {readiness.get('bridge_resident_ok', '-')}",
                f"- bridge_alive: {readiness.get('bridge_alive', '-')}",
                f"- bridge_heartbeat_fresh: {readiness.get('bridge_heartbeat_fresh', '-')}",
                f"- bridge_pid: {readiness.get('bridge_pid', '-')}",
                f"- supervisor_loop_interval_seconds: {readiness.get('supervisor_loop_interval_seconds', '-')}",
                f"- resource_idle: {readiness.get('resource_idle', '-')}",
                f"- test_idle_window_seconds: {readiness.get('test_idle_window_seconds', '-')}",
                f"- runnable_harness_ops: {format_list(readiness.get('runnable_harness_ops', []))}",
                f"- blocked_harness_ops: {format_list(readiness.get('blocked_harness_ops', []))}",
                f"- solver_owned_ops: {format_list(readiness.get('solver_owned_ops', []))}",
                f"- tester_casegen_ops: {format_list(readiness.get('tester_casegen_ops', []))}",
                f"- relay_effective_delivery: {readiness.get('relay_effective_delivery', '-') or '-'}",
                f"- relay_delivery_blocked: {readiness.get('relay_delivery_blocked', '-')}",
                f"- app_side_relay_required: {readiness.get('app_side_relay_required', '-')}",
                f"- app_side_relay_fresh: {readiness.get('app_side_relay_fresh', '-')}",
                f"- app_side_relay_resident_ok: {readiness.get('app_side_relay_resident_ok', '-')}",
                f"- native_relay_outbox_count: {readiness.get('native_relay_outbox_count', '-')}",
                f"- native_relay_active_claim_count: {readiness.get('native_relay_active_claim_count', '-')}",
                f"- submit_gap_ok: {readiness.get('submit_gap_ok', '-')}",
                f"- submit_gap_max_seconds: {readiness.get('submit_gap_max_seconds', '-')}",
                f"- submit_gap_violation_count: {readiness.get('submit_gap_violation_count', '-')}",
                f"- traffic_balance_ok: {readiness.get('traffic_balance_ok', '-')}",
                f"- traffic_balance_debt: {readiness.get('traffic_balance_debt', {})}",
                f"- next_debt_target: {readiness.get('next_debt_target', '-') or '-'}",
            ]
        )
        if readiness.get("stop_requested_at"):
            lines.append(
                f"- stop_requested_at: {readiness.get('stop_requested_at', '')}"
            )
        if readiness.get("stop_reason"):
            lines.append(f"- stop_reason: {readiness.get('stop_reason', '')}")
        if readiness.get("resume_command_hint"):
            lines.append(
                f"- resume_command_hint: `{readiness.get('resume_command_hint', '')}`"
            )
    else:
        lines.append("- no resume readiness summary recorded")
    engine = payload.get("engine_admission", {})
    lines.extend(["", "## Test Engine Admission", ""])
    lines.append(f"- executor_mode: {payload.get('test_executor', 'legacy')}")
    if isinstance(engine, dict) and engine:
        lines.extend(
            [
                f"- enabled: {engine.get('enabled', False)}",
                f"- draining: {engine.get('draining', False)}",
                f"- protocol_version: {engine.get('protocol_version', '-')}",
                f"- accepted_queue_target: {engine.get('target_inflight', '-')}",
                f"- local_credit: {engine.get('local_credit', '-')}",
                f"- effective_credit: {engine.get('effective_credit', '-')}",
                f"- state_counts: {engine.get('state_counts', {})}",
                f"- execution_state_counts: {engine.get('execution_state_counts', {})}",
                f"- active_slot_job_count: {engine.get('active_slot_job_count', '-')}",
                f"- preactivation_only_job_count: {engine.get('preactivation_only_job_count', '-')}",
                f"- running_stage_count: {engine.get('running_stage_count', '-')}",
                f"- running_stage_counts_by_resource: {engine.get('running_stage_counts_by_resource', {})}",
                f"- error: {engine.get('error', '') or '-'}",
            ]
        )
        remote = engine.get("last_engine_snapshot", {})
        if isinstance(remote, dict) and remote:
            lines.extend(
                [
                    f"- engine_generation: {remote.get('engine_generation', '-') or '-'}",
                    f"- remote_capacity: {remote.get('capacity', {})}",
                    f"- remote_accepted_nonterminal: {remote.get('accepted_nonterminal', '-')}",
                    f"- remote_standby_count: {remote.get('standby_count', '-')}",
                    f"- remote_standby_credit: {remote.get('standby_credit', '-')}",
                    f"- remote_active_nonterminal: {remote.get('active_nonterminal', '-')}",
                    f"- remote_queued_nonterminal: {remote.get('queued_nonterminal', '-')}",
                    f"- remote_active_job_slots_free: {remote.get('active_job_slots_free', '-')}",
                    f"- remote_admission_credit: {remote.get('admission_credit', '-')}",
                    f"- remote_credit_before_backpressure: {remote.get('admission_credit_before_backpressure', '-')}",
                    f"- remote_return_ready_count: {remote.get('return_ready_count', '-')}",
                    f"- remote_return_backlog_bytes: {remote.get('return_backlog_bytes', '-')}",
                    f"- remote_required_return_backlog_bytes: {remote.get('required_return_backlog_bytes', '-')}",
                    f"- remote_return_backlog_pressure_ratio: {remote.get('return_backlog_pressure_ratio', '-')}",
                    f"- remote_return_backpressure_active: {remote.get('return_backpressure_active', False)}",
                    f"- remote_return_backpressure_reason: {remote.get('return_backpressure_reason', '') or '-'}",
                    f"- remote_return_backlog_limits: {remote.get('return_backlog_limits', {})}",
                    f"- remote_running_by_resource: {remote.get('running_by_resource', {})}",
                    f"- remote_running_locks: {remote.get('running_locks', [])}",
                    f"- remote_shared_resource_leases: {remote.get('shared_resource_leases', [])}",
                ]
            )
        jobs = engine.get("jobs", {})
        if isinstance(jobs, dict) and jobs:
            lines.extend(
                [
                    "",
                    "| job | op | state | execution | stage | resource | locks | started_at |",
                    "|---|---|---|---|---|---|---|---|",
                ]
            )
            for job_id, item in sorted(jobs.items()):
                if not isinstance(item, dict):
                    continue
                lines.append(
                    f"| {job_id} | {item.get('operator', '-') or '-'} | "
                    f"{item.get('state', '-') or '-'} | "
                    f"{item.get('execution_state', '-') or '-'} | "
                    f"{item.get('stage_name', '-') or '-'} | "
                    f"{item.get('stage_resource', '-') or '-'} | "
                    f"{','.join(str(value) for value in item.get('stage_locks', [])) or '-'} | "
                    f"{item.get('stage_started_at', '-') or '-'} |"
                )
    else:
        lines.append("- no engine admission state")
    pump = payload.get("engine_pump", {})
    lines.extend(["", "## Test Engine Pump", ""])
    if isinstance(pump, dict) and pump:
        lines.extend(
            [
                f"- outcome: {pump.get('outcome', '-')}",
                f"- state_counts: {pump.get('state_counts', {})}",
                f"- execution_profiles: {pump.get('execution_profiles', {})}",
                f"- updated_at: {pump.get('updated_at', '') or '-'}",
                f"- error: {pump.get('error', '') or '-'}",
                f"- recent_replenishment_count: {len(pump.get('recent_replenishments', []))}",
            ]
        )
        replenishments = pump.get("recent_replenishments", [])
        if isinstance(replenishments, list) and replenishments:
            lines.extend(
                [
                    "",
                    "| released job | replacement job | terminal -> replacement (s) | returned -> replacement (s) |",
                    "|---|---|---:|---:|",
                ]
            )
            for item in replenishments:
                if not isinstance(item, dict):
                    continue
                lines.append(
                    f"| {item.get('released_engine_job_id', '-') or '-'} | "
                    f"{item.get('replacement_engine_job_id', '-') or '-'} | "
                    f"{item.get('terminal_to_replenish_seconds', '-') if item.get('terminal_to_replenish_seconds') is not None else '-'} | "
                    f"{item.get('return_to_replenish_seconds', '-') if item.get('return_to_replenish_seconds') is not None else '-'} |"
                )
    else:
        lines.append("- no engine pump state")
    engine_ab = payload.get("engine_ab_latest", {})
    lines.extend(["", "## Test Engine A/B Gate", ""])
    if isinstance(engine_ab, dict) and engine_ab:
        lines.extend(
            [
                f"- comparison_id: {engine_ab.get('comparison_id', '-') or '-'}",
                f"- verdict: {engine_ab.get('verdict', '-')}",
                f"- generated_at: {engine_ab.get('generated_at', '-') or '-'}",
                f"- thresholds_percent: {engine_ab.get('thresholds_percent', {})}",
                f"- blockers: {format_list(engine_ab.get('blockers', []))}",
                f"- json_path: {engine_ab.get('json_path', '-') or '-'}",
                f"- markdown_path: {engine_ab.get('markdown_path', '-') or '-'}",
            ]
        )
        performance_ab = engine_ab.get("performance", {})
        if isinstance(performance_ab, dict):
            lines.append(
                f"- weighted_shift_percent: {performance_ab.get('weighted_shift_percent', '-') }"
            )
        if engine_ab.get("protocol_version") in {
            "engine-ab-series-v1",
            "engine-ab-series-v2",
            "engine-ab-series-v3",
        }:
            lines.extend(
                [
                    f"- pair_count: {engine_ab.get('pair_count', 0)}",
                    f"- execution_orders: {format_list(engine_ab.get('execution_orders', []))}",
                    f"- weighted_shift_series: {engine_ab.get('weighted_shift_percent', {})}",
                    "- remote_engine_code_generation: "
                    f"{engine_ab.get('remote_engine_code_generation', '-') or '-'}",
                ]
            )
    else:
        lines.append("- no engine A/B report")
    promotion = payload.get("engine_promotion", {})
    lines.extend(["", "## Test Engine Production Promotion", ""])
    if isinstance(promotion, dict) and promotion:
        report = promotion.get("report", {})
        report = report if isinstance(report, dict) else {}
        lines.extend(
            [
                f"- requested_mode: {promotion.get('requested_mode', 'legacy')}",
                f"- required: {promotion.get('required', False)}",
                f"- allowed: {promotion.get('allowed', False)}",
                f"- promotion_id: {report.get('promotion_id', '-') or '-'}",
                f"- verdict: {report.get('verdict', '-') or '-'}",
                f"- engine_code_generation: {report.get('engine_code_generation', '-') or '-'}",
                f"- blockers: {format_list(promotion.get('blockers', []))}",
                f"- path: {promotion.get('path', '-') or '-'}",
            ]
        )
    else:
        lines.append("- no engine promotion state")
    acceptance = payload.get("hard_acceptance", {})
    lines.extend(["", "## Hard Acceptance", ""])
    if isinstance(acceptance, dict) and acceptance:
        checks = acceptance.get("checks", {})
        lines.extend(
            [
                f"- ok: {acceptance.get('ok', '-')}",
                f"- submit_gap_max_seconds: {acceptance.get('submit_gap_max_seconds', '-')}",
                f"- submit_gap_violation_count: {acceptance.get('submit_gap_violation_count', '-')}",
                f"- traffic_balance_debt: {acceptance.get('traffic_balance_debt', {})}",
                f"- next_debt_target: {acceptance.get('next_debt_target', '') or '-'}",
                f"- blockers: {format_list(acceptance.get('blockers', []))}",
            ]
        )
        if isinstance(checks, dict) and checks:
            for name, value in checks.items():
                lines.append(f"- check.{name}: {value}")
    else:
        lines.append("- no hard acceptance summary recorded")
    rules = payload.get("solver_tester_rules", {})
    lines.extend(["", "## Solver/Tester Rule Coverage", ""])
    if isinstance(rules, dict) and rules:
        lines.extend(
            [
                f"- ok: {rules.get('ok', '-')}",
                f"- recent_window_seconds: {rules.get('recent_window_seconds', '-')}",
                f"- recent_solver_ide_trigger_count: {rules.get('recent_solver_ide_trigger_count', '-')}",
                f"- recent_tester_casegen_ide_trigger_count: {rules.get('recent_tester_casegen_ide_trigger_count', '-')}",
                f"- current_solver_owned_ops: {format_list(rules.get('current_solver_owned_ops', []))}",
                f"- current_casegen_ops: {format_list(rules.get('current_casegen_ops', []))}",
                f"- solver_missing_relay_coverage_ops: {format_list(rules.get('solver_missing_relay_coverage_ops', []))}",
                f"- casegen_missing_relay_coverage_ops: {format_list(rules.get('casegen_missing_relay_coverage_ops', []))}",
                f"- solver_outbox_ops: {format_list(rules.get('solver_outbox_ops', []))}",
                f"- tester_outbox_ops: {format_list(rules.get('tester_outbox_ops', []))}",
                f"- active_solver_gate_ops: {format_list(rules.get('active_solver_gate_ops', []))}",
                f"- active_tester_gate_ops: {format_list(rules.get('active_tester_gate_ops', []))}",
                f"- blockers: {format_list(rules.get('blockers', []))}",
            ]
        )
        checks = rules.get("checks", {})
        if isinstance(checks, dict) and checks:
            for name, value in checks.items():
                lines.append(f"- check.{name}: {value}")
    else:
        lines.append("- no solver/tester rule summary recorded")
    runtime = payload.get("runtime_residency", {})
    lines.extend(["", "## Runtime Residency", ""])
    if isinstance(runtime, dict) and runtime:
        lines.extend(
            [
                "| target | resident_ok | process_alive | effective_pid | heartbeat_fresh | heartbeat_age_s | process_action |",
                "|---|---:|---:|---:|---:|---:|---|",
            ]
        )
        for target in ("supervisor_loop", "daemon", "solver_trigger_bridge"):
            item = runtime.get(target, {})
            if not isinstance(item, dict):
                continue
            lines.append(
                f"| {target} | {bool(item.get('resident_ok'))} | {bool(item.get('process_alive'))} | "
                f"{item.get('effective_pid', 0) or 0} | {bool(item.get('heartbeat_fresh'))} | "
                f"{item.get('heartbeat_age_seconds', '-') if item.get('heartbeat_age_seconds') is not None else '-'} | "
                f"{item.get('process_action', '-') or '-'} |"
            )
    else:
        lines.append("- no runtime residency summary recorded")
    lines.extend(
        [
            "",
            "## Board",
            "",
            "| op | gate | owner | next |",
            "|---|---|---|---|",
        ]
    )
    for row in payload.get("board", []):
        if not isinstance(row, dict):
            continue
        next_command = str(row.get("next_command", "") or "").replace("|", "&#124;")
        lines.append(
            f"| {row.get('op', '-')} | {row.get('gate_stage', '-')} | "
            f"{row.get('next_owner', '-')} | {next_command[:160]} |"
        )
    lines.extend(
        [
            "",
            "## Solver Visibility",
            "",
            "| op | owner/gate | latest_turn | status | thread_status | visibility | age_s | user_only | trigger_policy | header_lag_s | read_error | synthetic_skipped |",
            "|---|---|---|---|---|---|---:|---:|---|---:|---|---|",
        ]
    )
    visibility = payload.get("solver_visibility", {})
    for item in (
        visibility.get("thread_records", []) if isinstance(visibility, dict) else []
    ):
        if not isinstance(item, dict):
            continue
        status = str(item.get("latest_turn_status", "") or "-")
        raw_status = str(item.get("raw_latest_turn_status", "") or "")
        if raw_status:
            status = f"{status} (raw {raw_status})"
        lines.append(
            f"| {item.get('op', '-')} | "
            f"{item.get('current_owner', '-') or '-'}/{item.get('current_gate_stage', '-') or '-'} | "
            f"{item.get('latest_turn_id', '-') or '-'} | "
            f"{status} | {item.get('thread_status_type', '-') or '-'} | "
            f"{item.get('ide_panel_visibility', '-') or '-'} | "
            f"{item.get('idle_seconds', '-') if item.get('idle_seconds') is not None else '-'} | "
            f"{bool(item.get('latest_user_only_turn'))} | "
            f"{str(item.get('trigger_policy', '-') or '-')[:120].replace('|', '&#124;')} | "
            f"{item.get('thread_header_lag_seconds', '-') if item.get('thread_header_lag_seconds') is not None else '-'} | "
            f"{str(item.get('visibility_error', '-') or '-')[:80].replace('|', '&#124;')} | "
            f"{item.get('skipped_synthetic_turn_id', '-') or '-'} |"
        )
    tester_visibility = payload.get("tester_visibility", {})
    lines.extend(
        [
            "",
            "## Tester Visibility",
            "",
            "| op | gate | trigger_status | ack_status | thread | turn | delivery | visibility | prompt |",
            "|---|---|---|---|---|---|---|---|---|",
        ]
    )
    tester_records = (
        tester_visibility.get("records", [])
        if isinstance(tester_visibility, dict)
        else []
    )
    if tester_records:
        for item in tester_records:
            if not isinstance(item, dict):
                continue
            ack_status = str(item.get("ack_status", "") or "-")
            raw_ack_status = str(item.get("raw_ack_status", "") or "")
            if raw_ack_status:
                ack_status = f"{ack_status} (raw {raw_ack_status})"
            lines.append(
                f"| {item.get('op', '-') or '-'} | {item.get('gate_stage', '-') or '-'} | "
                f"{item.get('trigger_status', '-') or '-'} | {ack_status} | "
                f"{item.get('thread_id', '-') or '-'} | {item.get('turn_id', '-') or '-'} | "
                f"{item.get('delivery', '-') or '-'} | {item.get('visibility', '-') or '-'} | "
                f"`{item.get('prompt_path', '-') or '-'}` |"
            )
    else:
        lines.append("| - | - | - | - | - | - | - | - | - |")
    missing_testers = (
        tester_visibility.get("missing_tester_threads", [])
        if isinstance(tester_visibility, dict)
        and isinstance(tester_visibility.get("missing_tester_threads"), list)
        else []
    )
    if missing_testers:
        lines.append("")
        lines.append(
            "- missing_tester_threads: "
            + ", ".join(
                f"{item.get('op', '-')}:MISSING"
                for item in missing_testers
                if isinstance(item, dict)
            )
        )
    poll = payload.get("solver_thread_poll", {})
    lines.extend(["", "## Solver Poller", ""])
    if isinstance(poll, dict) and poll:
        lines.extend(
            [
                f"- updated_at: {poll.get('updated_at', '')}",
                f"- status: {poll.get('status', '')}",
                f"- mode: {poll.get('mode', '') or '-'}",
                f"- thread_count: {poll.get('thread_count', '-') if poll.get('thread_count') is not None else '-'}",
            ]
        )
        if poll.get("ops"):
            lines.append(f"- ops: {', '.join(map(str, poll.get('ops', [])))}")
        if poll.get("poll_fallback"):
            lines.append(f"- poll_fallback: {poll.get('poll_fallback', '')}")
        if poll.get("fallback_reason"):
            lines.append(
                f"- fallback_reason: {str(poll.get('fallback_reason', ''))[:220]}"
            )
        if poll.get("error"):
            lines.append(f"- error: {str(poll.get('error', ''))[:220]}")
    else:
        lines.append("- no bridge poll status recorded")
    relay = payload.get("relay_capability", {})
    lines.extend(["", "## IDE Relay Capability", ""])
    if isinstance(relay, dict) and relay:
        lines.extend(
            [
                f"- native_delivery_available: {relay.get('native_delivery_available', '-')}",
                f"- effective_delivery_available: {relay.get('effective_delivery_available', '-')}",
                f"- effective_delivery: {relay.get('effective_delivery', '-') or '-'}",
                f"- delivery_blocked: {relay.get('delivery_blocked', '-')}",
                f"- effective_delivery_blocked: {relay.get('effective_delivery_blocked', '-')}",
                f"- remote_control_delivery_blocked: {relay.get('remote_control_delivery_blocked', '-')}",
                f"- delivery_blocked_raw: {relay.get('delivery_blocked_raw', '-')}",
                f"- delivery_blocker_reason: {relay.get('delivery_blocker_reason', '-') or '-'}",
                f"- delivery_blocked_until: {relay.get('delivery_blocked_until', '-') or '-'}",
                f"- remote_control_status: {relay.get('remote_control_status', '-') or '-'}",
                f"- poll_status: {relay.get('poll_status', '-') or '-'}",
                f"- proxy_socket: {relay.get('proxy_socket', '-') or '-'}",
                f"- proxy_socket_exists_after: {relay.get('proxy_socket_exists_after', '-')}",
                f"- proxy_repair_returncode: {relay.get('proxy_repair_returncode', '-')}",
                f"- remote_control_repair_supported: {relay.get('remote_control_repair_supported', '-')}",
            ]
        )
        if relay.get("reason"):
            lines.append(f"- reason: {str(relay.get('reason', ''))[:300]}")
    else:
        lines.append("- no relay capability record")
    app_side = payload.get("app_side_relay", {})
    lines.extend(["", "## App-Side Native Relay Consumer", ""])
    if isinstance(app_side, dict) and app_side:
        lines.extend(
            [
                f"- updated_at: {app_side.get('updated_at', '') or '-'}",
                f"- fresh: {app_side.get('fresh', '-')}",
                f"- age_seconds: {app_side.get('age_seconds', '-')}",
                f"- stale_threshold_seconds: {app_side.get('stale_threshold_seconds', '-')}",
                f"- source: {app_side.get('source', '-') or '-'}",
                f"- kind: {app_side.get('kind', '-') or '-'}",
                f"- status: {app_side.get('status', '-') or '-'}",
                f"- consumer: {app_side.get('consumer', '-') or '-'}",
                f"- relay_contract: {app_side.get('relay_contract', '-') or '-'}",
                f"- claimed_count: {app_side.get('claimed_count', '-')}",
                f"- active_claim_count: {app_side.get('active_claim_count', '-')}",
                f"- available_entry_count: {app_side.get('available_entry_count', '-')}",
            ]
        )
        if app_side.get("thread_id"):
            lines.append(f"- thread_id: {app_side.get('thread_id', '-')}")
        if app_side.get("turn_id"):
            lines.append(f"- turn_id: {app_side.get('turn_id', '-')}")
        if app_side.get("reason"):
            lines.append(f"- reason: {app_side.get('reason', '-')}")
    else:
        lines.append("- no app-side native relay consumer poll recorded")
    outbox = payload.get("native_relay_outbox", {})
    lines.extend(["", "## Native Relay Outbox", ""])
    if isinstance(outbox, dict) and outbox:
        lines.extend(
            [
                f"- updated_at: {outbox.get('updated_at', '')}",
                f"- relay_contract: {outbox.get('relay_contract', '') or '-'}",
                f"- entry_count: {outbox.get('entry_count', 0)}",
                f"- claimed_entry_count: {outbox.get('claimed_entry_count', 0)}",
                f"- available_entry_count: {outbox.get('available_entry_count', 0)}",
                f"- by_type: {format_counts(outbox.get('by_type', {}))}",
                f"- by_op: {format_counts(outbox.get('by_op', {}))}",
            ]
        )
        entries = (
            outbox.get("entries", []) if isinstance(outbox.get("entries"), list) else []
        )
        if entries:
            lines.extend(
                [
                    "",
                    "| id | type | op | gate | status | claim | thread | prompt |",
                    "|---|---|---|---|---|---|---|---|",
                ]
            )
            for item in entries:
                if not isinstance(item, dict):
                    continue
                lines.append(
                    f"| {item.get('id', '-') or '-'} | {item.get('type', '-') or '-'} | "
                    f"{item.get('op', '-') or '-'} | {item.get('gate_stage', '-') or '-'} | "
                    f"{item.get('status', '-') or '-'} | {item.get('claim_status', '-') or '-'} | "
                    f"{item.get('thread_id', '-') or '-'} | "
                    f"`{item.get('prompt_path', '-') or '-'}` |"
                )
    else:
        lines.append("- no native relay outbox recorded")
    claims = payload.get("native_relay_claims", {})
    lines.extend(["", "## Native Relay Claims", ""])
    if isinstance(claims, dict) and claims:
        lines.extend(
            [
                f"- updated_at: {claims.get('updated_at', '') or '-'}",
                f"- active_claim_count: {claims.get('active_claim_count', 0)}",
            ]
        )
        claim_rows = (
            claims.get("claims", []) if isinstance(claims.get("claims"), list) else []
        )
        if claim_rows:
            lines.extend(
                [
                    "",
                    "| entry | type | op | gate | claimed_by | expires_at |",
                    "|---|---|---|---|---|---|",
                ]
            )
            for item in claim_rows:
                if not isinstance(item, dict):
                    continue
                lines.append(
                    f"| {item.get('entry_id', '-') or '-'} | {item.get('type', '-') or '-'} | "
                    f"{item.get('op', '-') or '-'} | {item.get('gate_stage', '-') or '-'} | "
                    f"{item.get('claimed_by', '-') or '-'} | {item.get('expires_at', '-') or '-'} |"
                )
    else:
        lines.append("- no native relay claims recorded")
    replacement = payload.get("solver_replacement", {})
    lines.extend(["", "## Solver Current-Session Recovery", ""])
    replacements = (
        replacement.get("replacements", []) if isinstance(replacement, dict) else []
    )
    recovery_actions = [
        action
        for action in payload.get("recommended_actions", [])
        if isinstance(action, dict)
        and action.get("kind") == "recover_current_solver_session"
    ]
    if (
        isinstance(replacement, dict)
        and replacement.get("replacement_allowed") is False
    ):
        lines.extend(
            [
                "- replacement_allowed: false",
                "- current_solver_sessions_preserved: true",
            ]
        )
    if recovery_actions:
        lines.extend(
            [
                "",
                "| op | gate | thread | reason | prompt |",
                "|---|---|---|---|---|",
            ]
        )
        for item in recovery_actions:
            reason = str(item.get("reason", "") or "").replace("|", "&#124;")
            lines.append(
                f"| {item.get('op', '-') or '-'} | {item.get('gate_stage', '-') or '-'} | "
                f"{item.get('thread_id', '-') or '-'} | {reason[:120]} | "
                f"`{item.get('prompt_path', '-') or '-'}` |"
            )
    elif isinstance(replacements, list) and replacements:
        lines.extend(
            [
                f"- updated_at: {replacement.get('updated_at', '') if isinstance(replacement, dict) else ''}",
                f"- replacement_required_count: {replacement.get('replacement_required_count', len(replacements)) if isinstance(replacement, dict) else len(replacements)}",
                "",
                "| op | gate | old_thread | reason | prompt | register_command |",
                "|---|---|---|---|---|---|",
            ]
        )
        for item in replacements:
            if not isinstance(item, dict):
                continue
            reason = str(item.get("reason", "") or "").replace("|", "&#124;")
            command = str(item.get("register_command", "") or "").replace("|", "&#124;")
            lines.append(
                f"| {item.get('op', '-') or '-'} | {item.get('gate_stage', '-') or '-'} | "
                f"{item.get('old_thread_id', '-') or '-'} | {reason[:120]} | "
                f"`{item.get('replacement_prompt_path', '-') or '-'}` | `{command}` |"
            )
    else:
        lines.append("- none")
    lines.extend(["", "## Gaps", ""])
    idle = payload.get("idle_and_gaps", {})
    if isinstance(idle, dict):
        lines.extend(
            [
                f"- resource_idle: {idle.get('resource_idle', '')}",
                f"- test_idle_window_seconds: {idle.get('test_idle_window_seconds', '-')}",
                f"- recent_window_max_result_to_solver_ack_seconds: {idle.get('recent_window_max_result_to_solver_ack_seconds', '-')}",
                f"- recent_window_max_result_to_next_dispatch_seconds: {idle.get('recent_window_max_result_to_next_dispatch_seconds', '-')}",
            ]
        )
        submit_gap = idle.get("completion_to_next_submit", {})
        if isinstance(submit_gap, dict) and submit_gap:
            lines.append(
                "- completion_to_next_submit: "
                f"window={submit_gap.get('window_size', '-')} "
                f"samples={submit_gap.get('sample_count', '-')} "
                f"ok={submit_gap.get('ok', False)} "
                f"max_gap_seconds={submit_gap.get('max_gap_seconds', '-')} "
                f"violations={submit_gap.get('violation_count', 0)}"
            )
    lines.extend(["", "## Resource Leases", ""])
    resources = payload.get("resources", [])
    if isinstance(resources, list) and resources:
        lines.extend(
            [
                "| resource | type | op | gate | pid | acquired_at | expires_at |",
                "|---|---|---|---|---:|---|---|",
            ]
        )
        for lease in resources:
            if not isinstance(lease, dict):
                continue
            lines.append(
                f"| {lease.get('resource_id', '-') or '-'} | {lease.get('resource_type', '-') or '-'} | "
                f"{lease.get('op', '-') or '-'} | {lease.get('gate_stage', '-') or '-'} | "
                f"{lease.get('pid', '-') or '-'} | {lease.get('acquired_at', '-') or '-'} | "
                f"{lease.get('expires_at', '-') or '-'} |"
            )
    else:
        lines.append("- none")
    lines.extend(["", "## Transport", ""])
    transport = payload.get("transport", [])
    if isinstance(transport, list) and transport:
        lines.extend(
            [
                "| op | test_version | state | terminal | remote | no_feedback_s | summary |",
                "|---|---|---|---:|---|---:|---|",
            ]
        )
        for item in transport:
            if not isinstance(item, dict):
                continue
            summary = str(item.get("summary", "") or "").replace("|", "&#124;")
            lines.append(
                f"| {item.get('op', '-') or '-'} | {item.get('test_version', '-') or '-'} | "
                f"{item.get('state', '-') or '-'} | {bool(item.get('terminal'))} | "
                f"{item.get('remote_feedback_status', '-') or '-'} | "
                f"{item.get('elapsed_without_remote_feedback_seconds', '-') or '-'} | {summary[:120]} |"
            )
    else:
        lines.append("- none")
    lines.extend(["", "## Balance", ""])
    balance = payload.get("balance", {})
    if isinstance(balance, dict):
        lines.append(
            f"- recent_selected_balance: {balance.get('recent_selected_balance', {})}"
        )
        lines.append(
            f"- recent_dispatch_balance: {balance.get('recent_dispatch_balance', {})}"
        )
        traffic_balance = balance.get("traffic_balance", {})
        if isinstance(traffic_balance, dict) and traffic_balance:
            lines.append(
                "- traffic_balance: "
                f"window={traffic_balance.get('window_size', '-')} "
                f"samples={traffic_balance.get('sample_count', '-')} "
                f"ok={traffic_balance.get('ok', False)} "
                f"debt={traffic_balance.get('debt', {})}"
            )
        recovery = balance.get("balance_recovery", {})
        if isinstance(recovery, dict) and recovery:
            lines.append(
                "- balance_recovery: "
                f"ok={recovery.get('ok', False)} "
                f"next_debt_target={recovery.get('next_debt_target', '-') or '-'}"
            )
            plans = recovery.get("plans", [])
            if isinstance(plans, list) and plans:
                for item in plans:
                    if not isinstance(item, dict):
                        continue
                    lines.append(
                        f"  - {item.get('op', '-')}: debt={item.get('debt', '-')} "
                        f"state={item.get('state', '-')} "
                        f"action={item.get('recovery_action', '-')}"
                    )
        operators = balance.get("operators", {})
        if isinstance(operators, dict) and operators:
            lines.extend(
                [
                    "",
                    "| op | gate | owner | latest_result | verdict | case | usage |",
                    "|---|---|---|---|---|---|---:|",
                ]
            )
            for op, data in sorted(operators.items()):
                if not isinstance(data, dict):
                    continue
                lines.append(
                    f"| {op} | {data.get('gate_stage', '-') or '-'} | {data.get('next_owner', '-') or '-'} | "
                    f"{data.get('latest_result', '-') or '-'} | {data.get('latest_verdict', '-') or '-'} | "
                    f"{data.get('case', '-') or '-'} | {data.get('usage', '-') or '-'} |"
                )
    lines.extend(["", "## Performance", ""])
    performance = payload.get("performance", {})
    if isinstance(performance, dict) and performance:
        lines.extend(
            [
                "| op | latest_pass | latest_us | active_release | release_us | failure_streak | latest_vs_best_pct |",
                "|---|---|---:|---|---:|---:|---:|",
            ]
        )
        for op, data in sorted(performance.items()):
            if not isinstance(data, dict):
                continue
            lines.append(
                f"| {op} | {data.get('latest_pass', '-') or '-'} | "
                f"{data.get('latest_weighted_us', '-') if data.get('latest_weighted_us') is not None else '-'} | "
                f"{data.get('active_release', '-') or '-'} | "
                f"{data.get('active_release_weighted_us', '-') if data.get('active_release_weighted_us') is not None else '-'} | "
                f"{data.get('failure_streak', '-') if data.get('failure_streak') is not None else '-'} | "
                f"{data.get('latest_vs_best_recent_same_case_pct', '-') if data.get('latest_vs_best_recent_same_case_pct') is not None else '-'} |"
            )
    else:
        lines.append("- none")
    lines.extend(["", "## Iteration Quality", ""])
    iteration_quality = payload.get("iteration_quality", {})
    if isinstance(iteration_quality, dict) and iteration_quality:
        lines.extend(
            [
                "| op | latest | case | best_same_case | delta_pct | route model | router gap | review | self_verdict | next_action | lineage |",
                "|---|---|---|---|---:|---|---|---|---|---|---|",
            ]
        )
        for op, data in sorted(iteration_quality.items()):
            if not isinstance(data, dict):
                continue
            lines.append(
                f"| {op} | {data.get('latest', '-') or '-'} | {data.get('case_version', '-') or '-'} | "
                f"{data.get('best_same_case', '-') or '-'} | "
                f"{data.get('regression_vs_best_pct', '-') if data.get('regression_vs_best_pct') is not None else '-'} | "
                f"{data.get('route_reasoning_status', '-') or '-'}"
                f"/{data.get('route_reasoning_score', '-') if data.get('route_reasoning_score') is not None else '-'} | "
                f"{data.get('router_gap', '-') or '-'} | "
                f"{data.get('review_status', '-') or '-'} | {data.get('self_verdict', '-') or '-'} | "
                f"{data.get('next_action', '-') or '-'} | "
                f"{'resolved' if data.get('lineage_resolved') else 'unresolved'} |"
            )
    else:
        lines.append("- none")
    board_quality = payload.get("suggestion_board_quality", {})
    if isinstance(board_quality, dict) and board_quality:
        lines.extend(
            [
                "",
                "## Suggestion Board Quality",
                "",
                f"- status: {board_quality.get('status', '-')}",
                f"- open: {board_quality.get('open_count', '-')}",
                f"- structured: {board_quality.get('structured_count', '-')}",
                f"- requests: {board_quality.get('request_count', '-')}",
                f"- contributions: {board_quality.get('contribution_count', '-')}",
                f"- researched: {board_quality.get('researched_count', '-')}",
                f"- malformed_open: {board_quality.get('malformed_open_count', '-')}",
                f"- recent_review_linked: {board_quality.get('recent_review_linked_count', '-')}",
                f"- recent_review_none: {board_quality.get('recent_review_none_count', '-')}",
                f"- recent_review_missing: {board_quality.get('recent_review_missing_count', '-')}",
                f"- recent_review_incomplete: {board_quality.get('recent_review_incomplete_count', '-')}",
            ]
        )
    lines.extend(["", "## Actions", ""])
    actions = payload.get("recommended_actions", [])
    if isinstance(actions, list) and actions:
        for action in actions:
            if isinstance(action, dict):
                lines.append(
                    f"- {action.get('kind', '-')} {action.get('op', '')} {action.get('reason', '')}".rstrip()
                )
    else:
        lines.append("- none")
    for title, key in (("Issues", "issues"), ("Warnings", "warnings")):
        lines.extend(["", f"## {title}", ""])
        values = payload.get(key, [])
        if isinstance(values, list) and values:
            lines.extend(f"- {value}" for value in values)
        else:
            lines.append("- none")
    lines.append("")
    return "\n".join(lines)


def format_counts(counts: Any) -> str:
    if not isinstance(counts, dict) or not counts:
        return "-"
    return ",".join(f"{key}:{value}" for key, value in sorted(counts.items()))


def format_list(values: Any) -> str:
    if not isinstance(values, list) or not values:
        return "-"
    return ", ".join(str(value) for value in values)


def read_or_rebuild_gap_snapshot(
    root: Path, config: DaemonConfig, live_snapshot: Any
) -> dict[str, Any]:
    state_dir = root / "TestUtils" / "tester_daemon"
    fallback = read_json(state_dir / "scheduler_gaps.json")
    if live_snapshot is None:
        return fallback
    events = read_timeline_events(state_dir / TIMELINE_FILE)
    if not events:
        return fallback
    try:
        return build_gap_snapshot(root, config, live_snapshot, events, [])
    except Exception:
        return fallback


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def dedupe_recommended_actions(actions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_key: dict[tuple[str, str, str, str, str, str], dict[str, Any]] = {}
    result: list[dict[str, Any]] = []
    for action in actions:
        if not isinstance(action, dict):
            continue
        kind = str(action.get("kind", "") or "")
        op = str(action.get("op", "") or "")
        if kind in {"recover_current_solver_session", "replace_solver_session"} and op:
            key = (kind, op, "", "", "", "")
        else:
            key = (
                kind,
                op,
                str(action.get("gate_stage", "") or ""),
                str(action.get("key", "") or ""),
                str(action.get("thread_id", "") or ""),
                str(action.get("prompt_path", "") or ""),
            )
        existing = by_key.get(key)
        if existing is not None:
            for field, value in action.items():
                if field not in existing or existing.get(field) in ("", None):
                    existing[field] = value
            continue
        next_action = dict(action)
        by_key[key] = next_action
        result.append(next_action)
    return result


def optional_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
