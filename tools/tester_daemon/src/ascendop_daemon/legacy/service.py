from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import subprocess
import sys
import time
import traceback
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[5]
for package_root in (
    ROOT / "tools" / "tester_daemon" / "src",
    ROOT / "packages" / "ascendop_protocol" / "src",
    ROOT,
):
    if str(package_root) not in sys.path:
        sys.path.insert(0, str(package_root))

MIN_GITPARTNER_ORIGIN_VISIBILITY_RETRY_SECONDS = 30
DAEMON_PROCESS_STARTED_AT = datetime.now(timezone.utc)
LAST_NODE_RECONCILE_MONOTONIC = 0.0
LAST_REMOTE_NODE_REFRESH_MONOTONIC = 0.0

from ascendop_daemon.observability.audit_log import AuditLog
from ascendop_daemon.workflow.case_blocker import repeated_invalid_case_rollover_blocker
from ascendop_daemon.workflow.casegen_evidence import (
    enforce_casegen_evidence,
    latest_case_dir,
    latest_casegen_evidence_issue,
)
from ascendop_daemon.workflow.casegen_prewarm import (
    materialize_casegen_cache_snapshot,
)
from ascendop_daemon.runtime.config_loader import load_config
from ascendop_daemon.runtime.control import clear_stop_request, read_stop_request, write_stop_request
from ascendop_daemon.control_plane.control_database import (
    ControlDatabase,
    ControlDatabaseError,
)
from ascendop_daemon.legacy.distributed_experiment import (
    DistributedExperimentRunner,
    ExperimentTask,
    resolve_sustained_transport_capacity,
)
from ascendop_daemon.control_plane.endpoint_dispatcher import (
    build_dispatcher_pool,
)
from ascendop_daemon.legacy.executor import (
    Executor,
    action_requires_resource,
    consecutive_failed_execute_count,
    failed_execute_backoff,
    latest_failed_execute,
    process_creation_flags,
    process_startupinfo,
    prune_execute_workers,
    run_execute_worker,
    was_successfully_executed,
)
from ascendop_daemon.legacy.efficiency import build_traffic_balance
from ascendop_daemon.legacy.engine_admission import EngineAdmissionStore
from ascendop_daemon.workflow.engine_candidates import (
    EngineCandidateError,
    discover_engine_submit_candidates,
    evaluate_same_failure_retry_gate,
    workflow_attempt_is_managed,
)
from ascendop_daemon.legacy.engine_job_builder import (
    BATCHED_PROFILE,
    CASE_CACHE_PREWARM_PROFILE,
    CONSERVATIVE_PROFILE,
    CORRECTNESS_BATCHED_PROFILE,
    EngineJobBuildError,
    FUSED_SCALABLE_PROFILE,
    PERFORMANCE_FIRST_CORRECTNESS_BATCHED_PROFILE,
    PERFORMANCE_FIRST_SPLIT_PROFILE,
    PERFORMANCE_SESSION_CORRECTNESS_BATCHED_PROFILE,
    SCALABLE_PROFILE,
    SPLIT_PROFILE,
    build_compatibility_job,
    build_submit_case_cache_requirement,
    parse_submit_command,
)
from ascendop_daemon.workflow.engine_prewarm import (
    case_cache_prewarm_suffix,
    evaluate_case_cache_prewarm,
)
from ascendop_daemon.legacy.engine_pump import (
    EnginePump,
    EnginePumpError,
    retryable_remote_admission_rejection,
    runtime_generation_admission_rejection,
)
from ascendop_daemon.legacy.engine_promotion import (
    build_identity_evidence,
    build_throughput_evidence,
    engine_code_generation as local_engine_code_generation,
    evaluate_engine_promotion,
    expected_remote_engine_code_generation,
    promotion_gate_status,
    read_json_lines as read_engine_promotion_json_lines,
    read_object as read_engine_promotion_object,
    write_json as write_engine_promotion_json,
    write_promotion_report,
)
from ascendop_daemon.legacy.profiler_evidence import (
    ProfilerEvidenceError,
    enqueue_next_profiler_job,
    mark_flow_v3_profiler_enqueued,
    next_flow_v3_profiler_request,
    profiler_request_refresh_needed,
)
from ascendop_daemon.exchange.engine_result_ingestor import EngineResultIngestor
from ascendop_daemon.registry.engine_route import (
    EngineRouteError,
    EngineTransportRoute,
    legacy_engine_route,
    resolve_registered_engine_route,
)
from ascendop_daemon.legacy.engine_transport import EngineTransportAdapter
from ascendop_daemon.exchange.flow_v3_request_builder import (
    FlowV3RequestBuildError,
    build_candidate_request,
    build_diagnostic_request,
)
from ascendop_daemon.runtime.flow_v3_runtime import (
    FlowV3Runtime,
    flow_v3_local_generation,
    runtime_config as flow_v3_runtime_config,
    write_release_manifest as write_flow_v3_release_manifest,
)
from ascendop_daemon.legacy.flow_v3_store import FlowV3StoreError
from ascendop_daemon.workflow.gate_engine import GateEngine
from ascendop_daemon.exchange.gp_diagnostic import (
    GpDiagnosticError,
    GpDiagnosticRunner,
    direct_target_from_descriptor,
    endpoint_target,
)
from ascendop_daemon.legacy.health import check_health, read_app_side_relay_status
from ascendop_daemon.runtime.locking import DaemonLock, NamedProcessLock, process_alive, read_lock_pid
from ascendop_daemon.runtime.process_identity import process_start_token
from ascendop_daemon.runtime.process_inspection import (
    windows_process_command_line,
)
from ascendop_daemon.legacy.notifier import (
    SolverNotifier,
    completed_gate_retry_seconds,
    write_session_prompt_metrics,
)
from ascendop_daemon.registry.operator_plugins import (
    build_operator_plugin_status,
    reconcile_operator_plugin_state,
    reconcile_draining_operator_plugins,
    request_operator_drain,
    update_operator_enabled,
)
from ascendop_daemon.legacy.native_relay_outbox import (
    build_native_relay_outbox,
    claim_native_relay_entries,
    complete_native_relay_claim,
    release_native_relay_claims,
    tester_casegen_active_covering_record as relay_tester_casegen_active_covering_record,
    wait_for_native_relay_availability,
    write_native_relay_outbox,
)
from ascendop_daemon.registry.node_reconciler import (
    GitPartnerNodeAdmissionReconciler,
    NodeReconciler,
)
from ascendop_daemon.control_plane.resource_manager import ResourceManager
from ascendop_daemon.automation.session_recovery import recover_codex_sessions
from ascendop_daemon.runtime.runtime_maintenance import run_runtime_maintenance
from ascendop_daemon.control_plane.scheduler import Scheduler, traffic_debt
from ascendop_daemon.legacy.supervisor import (
    stop_process_force,
    taskkill_tree,
)
from ascendop_daemon.automation.solver_replacement import (
    render_solver_replacement_plan,
    write_solver_replacement_files,
)
from ascendop_daemon.storage.state_reader import StateReader
from ascendop_daemon.observability.status_writer import StatusWriter
from ascendop_daemon.legacy.status_query import build_status_query, render_status_query, write_status_query_files
from ascendop_daemon.workflow.workflow_profiles import (
    WorkflowProfileError,
    WorkflowProfileRegistry,
)
from ascendop_daemon.registry.system_registry import SystemRegistry
from ascendop_daemon.legacy.supervisor import (
    SupervisorOptions,
    start_supervisor_loop,
    stop_runtime_process,
    supervise_runtime,
)
from ascendop_daemon.core.models import (
    ActionKind,
    BoardRow,
    BoardSnapshot,
    DaemonConfig,
    DaemonPlan,
    GateDecision,
    TransportObservation,
    extract_row_test_version,
    extract_test_version,
    observed_operators,
    operator_season,
    operator_session,
    solver_session_replacement_allowed,
    utc_now_iso,
)
from ascendop_daemon.core.atomic_io import write_json_atomic
from ascendop_daemon.automation.trigger_state import (
    OWNER_LOSS_FAILURE_KINDS,
    ack_solver_trigger,
    ack_tester_trigger,
    codex_cli_resume_process_dead,
    completed_trigger_stale,
    delivery_reconciliation_pending,
    is_transient_native_poll_failure,
    normalized_trigger_status,
    read_tester_trigger_ack_state,
    read_trigger_ack_state,
    trigger_state_lock,
)
from ascendop_daemon.control_plane.test_requests import generate_test_requests
from ascendop_daemon.control_plane.workflow_priority import (
    load_operator_flow_priorities,
)


FLOW_V3_PROCESS_SOURCE_GENERATION = flow_v3_local_generation(ROOT)


def flow_v3_process_generation_status(
    root: Path = ROOT,
) -> dict[str, object]:
    current = flow_v3_local_generation(root)
    return {
        "matches": current == FLOW_V3_PROCESS_SOURCE_GENERATION,
        "process_source_generation": FLOW_V3_PROCESS_SOURCE_GENERATION,
        "disk_source_generation": current,
        "pid": os.getpid(),
    }


def record_flow_v3_process_generation_drift(role: str) -> dict[str, object]:
    status = flow_v3_process_generation_status(ROOT)
    append_daemon_runtime_event(
        ROOT,
        "flow_v3_process_generation_drift",
        {"role": role, **status},
    )
    return status


def tick(args: argparse.Namespace) -> int:
    clear_dead_daemon_lock_for_one_shot(ROOT)
    return run_tick(
        config_path=Path(args.config),
        mode=args.mode,
        write_state=args.write_state,
        dry_run_execute=args.dry_run_execute,
        expected_action_id=args.expected_action_id,
        allow_live_execute=args.allow_live_execute,
    )


def clear_dead_daemon_lock_for_one_shot(root: Path) -> bool:
    """Remove a dead long-running daemon lock before a one-shot tick."""
    lock_path = root / "TestUtils" / "tester_daemon" / "daemon.lock"
    pid = read_lock_pid(lock_path)
    if pid <= 0 or process_alive(pid):
        return False
    try:
        lock_path.unlink(missing_ok=True)
    except OSError as exc:
        append_daemon_runtime_event(
            root,
            "dead_daemon_lock_remove_failed",
            {"pid": pid, "path": str(lock_path), "error": str(exc)},
        )
        return False
    append_daemon_runtime_event(
        root,
        "dead_daemon_lock_removed",
        {"pid": pid, "path": str(lock_path)},
    )
    return True


def run_tick(
    config_path: Path,
    mode: str,
    write_state: bool,
    dry_run_execute: bool,
    expected_action_id: str = "",
    allow_live_execute: bool = False,
    observability_refresher: "ObservabilityRefresher | None" = None,
    thread_observation_refresher: "CriticalThreadObservationRefresher | None" = None,
    engine_candidate_refresher: "EngineCandidateRefresher | None" = None,
    maintenance_cadence: "RuntimeMaintenanceCadence | None" = None,
) -> int:
    tick_started = time.perf_counter()
    timings: dict[str, float] = {}
    stage_started = time.perf_counter()
    config_full_path = ROOT / config_path if not config_path.is_absolute() else config_path
    config = load_config(config_full_path, apply_completion_markers=True)
    timings["config_load_seconds"] = time.perf_counter() - stage_started
    stage_started = time.perf_counter()
    if write_state and thread_observation_refresher is not None:
        thread_observation_refresher.maybe_refresh(config)
    timings["thread_observation_seconds"] = time.perf_counter() - stage_started
    stage_started = time.perf_counter()
    # Remote lifecycle probes use GP and can take tens of seconds.  Keep only
    # local report ingestion on the workflow scheduling path; the async
    # observability worker owns remote refreshes.
    maybe_reconcile_control_nodes(config, root=ROOT, include_remote=False)
    timings["control_node_local_seconds"] = time.perf_counter() - stage_started
    stage_started = time.perf_counter()
    if maintenance_cadence is None or maintenance_cadence.due(
        "engine_executor",
        float(
            config.policy.get(
                "engine_executor_reconcile_interval_seconds",
                5.0,
            )
            or 5.0
        ),
    ):
        reconcile_engine_executor_config(config)
    config, _ = reconcile_draining_operator_plugins(ROOT, config_full_path, config)
    reconcile_operator_plugin_state(ROOT, config)
    reconcile_engine_operator_membership(config)
    resource_manager = ResourceManager(ROOT, config)
    timings["runtime_reconcile_seconds"] = time.perf_counter() - stage_started
    stage_started = time.perf_counter()
    snapshot, policy_decisions, decisions, leases, plan, action_liveness = build_tick_plan(
        config,
        resource_manager,
        timing_sink=timings,
    )
    timings["build_plan_seconds"] = time.perf_counter() - stage_started
    notifier = SolverNotifier(ROOT, config)
    audit = AuditLog(ROOT)
    stage_started = time.perf_counter()
    if write_state:
        audit.append(
            "tick",
            snapshot,
            plan.decisions,
            plan,
            mode,
            resource_leases=leases,
            action_liveness=action_liveness,
        )
    timings["audit_seconds"] = time.perf_counter() - stage_started

    execute_returncode = 0
    stage_started = time.perf_counter()
    if mode == "execute":
        executor = Executor(
            ROOT,
            dry_run=dry_run_execute,
            expected_action_id=expected_action_id,
            allow_live_execute=allow_live_execute,
            resource_manager=resource_manager,
            async_execute=bool(config.policy.get("async_execute_workers", False)),
            config_path=str(config_full_path),
            failed_retry_seconds=execute_failed_retry_seconds(config),
        )
        max_drain_actions = max(1, int(config.policy.get("max_tick_drain_actions", 3) or 1))
        for drain_index in range(max_drain_actions):
            if drain_index:
                snapshot, policy_decisions, decisions, leases, plan, action_liveness = build_tick_plan(
                    config,
                    resource_manager,
                )
                if write_state:
                    audit.append(
                        "tick_drain",
                        snapshot,
                        plan.decisions,
                        plan,
                        mode,
                        resource_leases=leases,
                        action_liveness=action_liveness,
                    )
            selected = plan.selected
            if engine_compatibility_dispatch_enabled(config, selected):
                if allow_live_execute and not dry_run_execute:
                    execute_returncode = enqueue_or_schedule_engine_candidates(
                        config,
                        engine_candidate_refresher,
                        config_path=config_full_path,
                    )
                else:
                    execute_returncode = 0
                    print("engine dispatch is dry-run/observe-only; no candidate was enqueued")
            elif engine_dispatch_is_unpromoted(config, selected):
                execute_returncode = 2
                append_daemon_runtime_event(
                    ROOT,
                    "engine_dispatch_fail_closed",
                    {
                        "op": selected.row.op if selected else "",
                        "gate": selected.row.gate_stage if selected else "",
                        "reason": "engine-v1 promotion gate is not currently allowed",
                    },
                )
                print(
                    "engine-v1 dispatch held because the promotion gate is not allowed; "
                    "legacy submit fallback is forbidden"
                )
            elif (
                selected is not None
                and selected.action == ActionKind.DISPATCH_SUBMIT
                and (
                    flow_v3_drain_work_pending(config)
                    if flow_v3_executor_mode(config)
                    else engine_drain_work_pending()
                )
            ):
                execute_returncode = 0
                if flow_v3_executor_mode(config):
                    maybe_start_flow_v3_worker(
                        config,
                        config_path=config_full_path,
                        allow_stop_drain=True,
                    )
                    print("dispatch held while accepted Flow V3 work drains")
                else:
                    maybe_start_engine_pump_worker(config)
                    print("legacy dispatch held while accepted engine work drains")
            else:
                execute_returncode = executor.run(plan)
            leases = tuple(resource_manager.prune_expired())
            if not should_drain_next_action(ROOT, selected, execute_returncode):
                break
        # The scheduler-facing decisions intentionally turn an already managed
        # or retry-held queue head into HOLD so another local action can run.
        # Candidate discovery must still see the unsuppressed queue signal:
        # it scans every queued row and can fill free remote credits with later
        # operators even while the board head is owned or circuit-held.
        runnable_engine_dispatch = any(
            engine_compatibility_dispatch_enabled(config, decision)
            for decision in policy_decisions
        )
        profiler_refresh = profiler_request_refresh_needed(ROOT)
        if (
            (runnable_engine_dispatch or profiler_refresh)
            and allow_live_execute
            and not dry_run_execute
        ):
            candidate_returncode = enqueue_or_schedule_engine_candidates(
                config,
                engine_candidate_refresher,
                config_path=config_full_path,
            )
            execute_returncode = max(execute_returncode, candidate_returncode)
        if flow_v3_executor_mode(config):
            if (
                not flow_v3_worker_is_alive(ROOT)
                and flow_v3_work_pending(config)
            ):
                maybe_start_flow_v3_worker(
                    config,
                    config_path=config_full_path,
                )
        elif (
            not engine_pump_worker_is_alive(ROOT)
            and engine_pump_work_pending()
        ):
            maybe_start_engine_pump_worker(config)
        if (
            not flow_v3_executor_mode(config)
            and (
                maintenance_cadence is None
                or maintenance_cadence.due(
            "engine_canary_recovery",
            float(
                config.policy.get(
                    "engine_canary_recovery_interval_seconds",
                    5.0,
                )
                or 5.0
            ),
                )
            )
        ):
            reconcile_engine_canary_recovery(config)
    timings["execute_seconds"] = time.perf_counter() - stage_started

    stage_started = time.perf_counter()
    if write_state:
        notifier.write(policy_decisions)
        timings["notifier_seconds"] = time.perf_counter() - stage_started
        stage_started = time.perf_counter()
    if write_state and observability_refresher is not None:
        refresh_started = observability_refresher.maybe_start(
            snapshot,
            plan.decisions,
            plan,
            mode,
            config,
            leases,
            action_liveness,
        )
        timings["observability_refresh_started"] = 1.0 if refresh_started else 0.0
        timings["observability_refresh_schedule_seconds"] = time.perf_counter() - stage_started
        rendered_status = ""
    elif write_state:
        status_writer = StatusWriter(ROOT)
        status_writer.write(
            snapshot,
            plan.decisions,
            plan,
            mode,
            config,
            resource_leases=leases,
            action_liveness=action_liveness,
        )
        rendered_status = (ROOT / "TestUtils" / "tester_daemon" / "DAEMON_STATUS.md").read_text(
            encoding="utf-8",
            errors="replace",
        )
        timings["status_write_seconds"] = time.perf_counter() - stage_started
        timings.update(
            {
                f"status_{key}": value
                for key, value in status_writer.last_timings.items()
            }
        )
        stage_started = time.perf_counter()
        write_solver_replacement_files(ROOT, config)
        timings["replacement_status_seconds"] = time.perf_counter() - stage_started
    else:
        status_writer = StatusWriter(ROOT)
        rendered_status = status_writer.render(snapshot, plan.decisions, plan, mode, action_liveness=action_liveness)
        timings["status_render_seconds"] = time.perf_counter() - stage_started

    total_seconds = time.perf_counter() - tick_started
    timings["total_seconds"] = total_seconds
    timing_threshold = float(config.policy.get("tick_timing_log_threshold_seconds", 2.0) or 2.0)
    if total_seconds >= timing_threshold:
        append_daemon_runtime_event(
            ROOT,
            "tick_timing",
            {
                "pid": os.getpid(),
                "mode": mode,
                "selected_action": plan.selected.action.value if plan.selected else "",
                "selected_op": plan.selected.row.op if plan.selected else "",
                **{key: round(value, 3) for key, value in timings.items()},
            },
        )

    if rendered_status:
        print(rendered_status)
    return execute_returncode


def maybe_reconcile_control_nodes(
    config: DaemonConfig,
    *,
    root: Path = ROOT,
    include_remote: bool = True,
) -> dict[str, object] | None:
    global LAST_NODE_RECONCILE_MONOTONIC, LAST_REMOTE_NODE_REFRESH_MONOTONIC
    if not bool(config.policy.get("control_plane_node_reconciler_enabled", False)):
        return None
    interval = max(
        0.5,
        float(
            config.policy.get(
                "control_plane_node_reconcile_interval_seconds",
                2,
            )
            or 2
        ),
    )
    current = time.monotonic()
    if current - LAST_NODE_RECONCILE_MONOTONIC < interval:
        return None
    LAST_NODE_RECONCILE_MONOTONIC = current
    database = ControlDatabase(
        rooted_path(
            root,
            str(
                config.policy.get("control_plane_database")
                or "TestUtils/tester_daemon/control.sqlite3"
            ),
        )
    )
    roots_value = config.policy.get("control_plane_node_report_roots")
    roots = (
        [str(value) for value in roots_value]
        if isinstance(roots_value, list)
        else ["GitPartner/output/_control/nodes"]
    )
    try:
        result = NodeReconciler(
            database,
            report_roots=[rooted_path(root, value) for value in roots],
            ack_root=rooted_path(
                root,
                str(
                    config.policy.get("control_plane_node_ack_root")
                    or "TestUtils/tester_daemon/node_acks"
                )
            ),
            pattern=str(
                config.policy.get("control_plane_node_report_pattern")
                or "*/report.json"
            ),
        ).run_once()
    except Exception as exc:
        append_daemon_runtime_event(
            root,
            "control_node_reconcile_failed",
            {"error": f"{type(exc).__name__}: {exc}"},
        )
        return {
            "failure_count": 1,
            "error": f"{type(exc).__name__}: {exc}",
        }
    remote_interval = max(
        interval,
        float(
            config.policy.get(
                "control_plane_remote_node_refresh_interval_seconds",
                60,
            )
            or 60
        ),
    )
    remote_state_path = (
        root
        / "TestUtils"
        / "tester_daemon"
        / "control_node_remote_refresh_state.json"
    )
    try:
        durable_remote_age = max(
            0.0,
            time.time() - remote_state_path.stat().st_mtime,
        )
    except OSError:
        durable_remote_age = float("inf")
    remote_refresh_due = (
        include_remote
        and current - LAST_REMOTE_NODE_REFRESH_MONOTONIC >= remote_interval
        and durable_remote_age >= remote_interval
    )
    if remote_refresh_due:
        LAST_REMOTE_NODE_REFRESH_MONOTONIC = current
        write_json_atomic(
            remote_state_path,
            {
                "updated_at": utc_now_iso(),
                "status": "running",
                "pid": os.getpid(),
                "interval_seconds": remote_interval,
            },
            ensure_ascii=True,
        )
        try:
            registry = SystemRegistry.load(
                rooted_path(
                    root,
                    str(
                        config.policy.get("control_plane_registry")
                        or "Develop/registry/system_registry.json"
                    )
                )
            )
            configured_endpoint = str(
                config.policy.get("test_engine_endpoint_id") or ""
            ).strip()
            endpoint_scope = (
                {configured_endpoint}
                if configured_endpoint
                and bool(
                    config.policy.get(
                        "control_plane_scope_remote_refresh_to_engine_endpoint",
                        True,
                    )
                )
                else None
            )
            result["remote_refresh"] = GitPartnerNodeAdmissionReconciler(
                root,
                database,
                registry,
                ack_root=rooted_path(
                    root,
                    str(
                        config.policy.get("control_plane_node_ack_root")
                        or "TestUtils/tester_daemon/node_acks"
                    )
                ),
            ).run_once(endpoint_ids=endpoint_scope)
            result["remote_refresh"]["endpoint_scope"] = (
                sorted(endpoint_scope) if endpoint_scope is not None else []
            )
            write_json_atomic(
                remote_state_path,
                {
                    "updated_at": utc_now_iso(),
                    "status": "completed",
                    "pid": os.getpid(),
                    "interval_seconds": remote_interval,
                    "endpoint_scope": result["remote_refresh"]["endpoint_scope"],
                    "refreshed_count": int(
                        result["remote_refresh"].get("refreshed_count", 0) or 0
                    ),
                    "failure_count": int(
                        result["remote_refresh"].get("failure_count", 0) or 0
                    ),
                },
                ensure_ascii=True,
            )
        except Exception as exc:
            result["remote_refresh"] = {
                "refreshed_count": 0,
                "failure_count": 1,
                "failures": [
                    {"error": f"{type(exc).__name__}: {exc}"},
                ],
            }
            append_daemon_runtime_event(
                root,
                "control_remote_node_refresh_failed",
                {"error": f"{type(exc).__name__}: {exc}"},
            )
            write_json_atomic(
                remote_state_path,
                {
                    "updated_at": utc_now_iso(),
                    "status": "failed",
                    "pid": os.getpid(),
                    "interval_seconds": remote_interval,
                    "error": f"{type(exc).__name__}: {exc}",
                },
                ensure_ascii=True,
            )
    if int(result.get("failure_count", 0) or 0):
        append_daemon_runtime_event(
            root,
            "control_node_reconcile_partial",
            {
                "failure_count": result["failure_count"],
                "failures": result["failures"],
            },
        )
    return result


def rooted_path(root: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else root / path


def engine_compatibility_dispatch_enabled(
    config: DaemonConfig,
    selected: GateDecision | None,
) -> bool:
    return bool(
        selected is not None
        and selected.action == ActionKind.DISPATCH_SUBMIT
        and selected.row.gate_stage == "submit-ready"
        and "--attach-existing" not in selected.command
        and engine_executor_enabled(config)
    )


def engine_compatibility_enabled(config: DaemonConfig) -> bool:
    return engine_executor_enabled(config)


def engine_dispatch_is_unpromoted(
    config: DaemonConfig,
    selected: GateDecision | None,
) -> bool:
    return bool(
        selected is not None
        and selected.action == ActionKind.DISPATCH_SUBMIT
        and str(config.policy.get("test_executor", "legacy") or "legacy")
        == "engine-v1"
        and not engine_executor_enabled(config)
    )


def engine_executor_enabled(config: DaemonConfig) -> bool:
    mode = str(config.policy.get("test_executor", "legacy") or "legacy")
    if mode == "flow-v3":
        return True
    if mode == "engine-v1-compatibility":
        return True
    if mode != "engine-v1":
        return False
    return bool(promotion_gate_status(ROOT, config.policy).get("allowed"))


def flow_v3_executor_mode(config: DaemonConfig) -> bool:
    return (
        str(config.policy.get("test_executor", "legacy") or "legacy")
        == "flow-v3"
    )


ENGINE_BACKPRESSURE_OPTIONS = {
    "standby_slots": "test_engine_standby_slots",
    "active_job_slots": "test_engine_active_job_slots",
    "return_backlog_soft_limit_bytes": "test_engine_return_backlog_soft_limit_bytes",
    "return_backlog_hard_limit_bytes": "test_engine_return_backlog_hard_limit_bytes",
    "return_backlog_soft_limit_jobs": "test_engine_return_backlog_soft_limit_jobs",
    "return_backlog_hard_limit_jobs": "test_engine_return_backlog_hard_limit_jobs",
}


def engine_capacity_overrides(config: DaemonConfig) -> dict[str, int]:
    return {
        engine_key: int(config.policy[policy_key])
        for engine_key, policy_key in ENGINE_BACKPRESSURE_OPTIONS.items()
        if config.policy.get(policy_key) is not None
    }


ENGINE_PUMP_DRAIN_STATES = (
    "admitting",
    "staging-standby",
    "accepted",
    "running",
    "return-ready",
    "returned-awaiting-ingest",
    "standby-cancel-requested",
)

ENGINE_CANARY_RECOVERY_OWNER = "daemon-canary-recovery"
ENGINE_CANARY_RECOVERY_TOKEN = "explicit-canary-outbox"
ENGINE_CANARY_RECOVERY_STATES = ("pending", *ENGINE_PUMP_DRAIN_STATES)
ENGINE_PUMP_REPLACEMENT_REQUEST_FILE = "engine_pump_replacement_request.json"


def engine_pump_status_has_drain_work(status: dict[str, object]) -> bool:
    counts = status.get("state_counts", {})
    if isinstance(counts, dict) and any(
        int(counts.get(name, 0) or 0) > 0 for name in ENGINE_PUMP_DRAIN_STATES
    ):
        return True
    return any(
        int(status.get(name, 0) or 0) > 0
        for name in ("pending_remote_ack_count", "pending_required_ack_count")
    )


def engine_drain_work_pending() -> bool:
    status = EnginePump(ROOT).status()
    admission = status.get("admission", {})
    if not isinstance(admission, dict) or not admission.get("draining"):
        return False
    return engine_pump_status_has_drain_work(status)


def flow_v3_drain_work_pending(config: DaemonConfig) -> bool:
    if not flow_v3_executor_mode(config):
        return False
    try:
        return FlowV3Runtime(
            flow_v3_runtime_config(ROOT, config.policy)
        ).has_drain_work()
    except (EngineRouteError, FlowV3StoreError, OSError, ValueError):
        return False


def flow_v3_work_pending(config: DaemonConfig) -> bool:
    if not flow_v3_executor_mode(config):
        return False
    try:
        status = FlowV3Runtime(
            flow_v3_runtime_config(ROOT, config.policy)
        ).store.status()
    except (EngineRouteError, FlowV3StoreError, OSError, ValueError):
        return False
    request_states = status.get("request_states", {})
    outbox_states = status.get("outbox_states", {})
    return bool(
        int(status.get("active_attempts", 0) or 0) > 0
        or (
            isinstance(request_states, dict)
            and any(
                int(request_states.get(state, 0) or 0) > 0
                for state in (
                    "created",
                    "validated",
                    "queued",
                    "admitted",
                    "dispatched",
                    "accepted",
                    "running",
                    "return-ready",
                    "ingested",
                    "acknowledged",
                )
            )
        )
        or (
            isinstance(outbox_states, dict)
            and any(
                int(outbox_states.get(state, 0) or 0) > 0
                for state in ("pending", "claimed", "retry")
            )
        )
    )


def engine_pump_work_pending() -> bool:
    status = EnginePump(ROOT).status()
    counts = status.get("state_counts", {})
    pending = (
        isinstance(counts, dict)
        and int(counts.get("pending", 0) or 0) > 0
    )
    return (
        pending
        or int(status.get("staged_enqueue_count", 0) or 0) > 0
        or engine_pump_status_has_retryable_admission_work(status)
        or engine_pump_status_has_drain_work(status)
    )


def engine_pump_status_has_retryable_admission_work(
    status: dict[str, object],
) -> bool:
    entries = status.get("entries", {})
    return any(
        isinstance(record, dict)
        and str(record.get("state") or "") == "admission-failed"
        and retryable_remote_admission_rejection(
            str(record.get("last_error") or "")
        )
        or (
            isinstance(record, dict)
            and str(record.get("state") or "") == "admission-failed"
            and (
                bool(record.get("retry_after_runtime_sync"))
                or runtime_generation_admission_rejection(
                    str(record.get("last_error") or "")
                )
            )
        )
        for record in (entries.values() if isinstance(entries, dict) else [])
    )


def engine_pump_worker_is_alive(
    root: Path,
    *,
    max_heartbeat_age_seconds: float = 300.0,
) -> bool:
    worker = read_json_dict(
        root / "TestUtils" / "tester_daemon" / "engine_pump_worker.json"
    )
    pid = int(worker.get("pid", 0) or 0)
    expected_start_token = str(worker.get("start_token") or "")
    if pid <= 0 or not expected_start_token or not process_alive(pid):
        return False
    if process_start_token(pid) != expected_start_token:
        return False
    heartbeat = parse_timestamp(str(worker.get("heartbeat_at") or ""))
    if heartbeat is None:
        return True
    heartbeat_age_seconds = max(
        0.0,
        (datetime.now(timezone.utc) - heartbeat).total_seconds(),
    )
    return heartbeat_age_seconds <= max(1.0, float(max_heartbeat_age_seconds))


def flow_v3_observed_service_states() -> dict[str, bool]:
    state_dir = ROOT / "TestUtils" / "tester_daemon"
    relay = read_app_side_relay_status(state_dir)
    watchdog = read_json_dict(state_dir / "watchdog_launcher_state.json")
    supervisor = read_json_dict(
        state_dir / "supervisor_loop_process.json"
    )
    if not supervisor:
        supervisor = watchdog
    watchdog_ready = recorded_process_ready(watchdog)
    supervisor_ready = recorded_process_ready(supervisor)
    return {
        "flow-v3-native-relay": bool(relay.get("fresh"))
        and not bool(relay.get("paused")),
        "flow-v3-watchdog": watchdog_ready,
        "flow-v3-supervisor": supervisor_ready,
    }


def recorded_process_ready(record: dict[str, object]) -> bool:
    pid = int(record.get("pid", 0) or 0)
    expected = str(record.get("start_token") or "")
    if pid <= 0 or not expected or not process_alive(pid):
        return False
    return process_start_token(pid) == expected


def engine_canary_recovery_work(
    pump_state: dict[str, object],
) -> list[str]:
    entries = pump_state.get("entries", {})
    if not isinstance(entries, dict):
        return []
    work: list[str] = []
    for job_id, raw in entries.items():
        if not isinstance(raw, dict) or bool(raw.get("workflow_ingest", True)):
            continue
        if str(raw.get("state") or "") in ENGINE_CANARY_RECOVERY_STATES:
            work.append(str(job_id))
            continue
        if str(raw.get("remote_ack_state") or "") == "pending":
            work.append(str(job_id))
            continue
        if str(raw.get("required_ack_state") or "") in {"pending", "piggybacked"}:
            work.append(str(job_id))
    return sorted(set(work))


def reconcile_engine_canary_recovery(config: DaemonConfig) -> dict[str, object]:
    store = EngineAdmissionStore(ROOT)
    admission = store.read()
    controller = store.active_controller_lease(admission)
    pump = EnginePump(ROOT)
    jobs = engine_canary_recovery_work(pump.read())

    if controller and controller.get("owner") not in {
        "workflow",
        ENGINE_CANARY_RECOVERY_OWNER,
    }:
        return {
            "started": False,
            "reason": "external_controller_active",
            "controller_owner": controller.get("owner"),
            "canary_job_count": len(jobs),
        }

    if not jobs:
        if controller and controller.get("owner") == ENGINE_CANARY_RECOVERY_OWNER:
            store.release_controller_lease(
                ENGINE_CANARY_RECOVERY_OWNER,
                ENGINE_CANARY_RECOVERY_TOKEN,
            )
            append_daemon_runtime_event(
                ROOT,
                "engine_canary_recovery_released",
                {"reason": "canary_outbox_drained"},
            )
            return {"started": False, "reason": "canary_outbox_drained", "released": True}
        return {"started": False, "reason": "no_explicit_canary_work"}

    if read_stop_request(ROOT):
        return {
            "started": False,
            "reason": "stop_requested",
            "canary_job_count": len(jobs),
        }

    if (
        not controller
        and admission.get("enabled")
        and not admission.get("draining")
        and engine_executor_enabled(config)
    ):
        result = maybe_start_engine_pump_worker(config)
        return {
            **result,
            "reason": "existing_admission_reused",
            "canary_job_count": len(jobs),
            "engine_job_ids": jobs,
        }

    target = max(
        1,
        int(config.policy.get("test_engine_target_inflight", 2) or 2),
    )
    if (
        not controller
        or controller.get("owner") != ENGINE_CANARY_RECOVERY_OWNER
        or not admission.get("enabled")
        or admission.get("draining")
        or int(admission.get("target_inflight", 1) or 1) != target
    ):
        store.configure(
            enabled=True,
            target_inflight=target,
            draining=False,
            controller_owner=ENGINE_CANARY_RECOVERY_OWNER,
            controller_token=ENGINE_CANARY_RECOVERY_TOKEN,
            lease_seconds=900,
        )
        append_daemon_runtime_event(
            ROOT,
            "engine_canary_recovery_acquired",
            {
                "canary_job_count": len(jobs),
                "engine_job_ids": jobs,
                "target_inflight": target,
            },
        )
    result = maybe_start_engine_pump_worker(config)
    return {**result, "canary_job_count": len(jobs), "engine_job_ids": jobs}


def reconcile_engine_executor_config(config: DaemonConfig) -> None:
    if (
        str(config.policy.get("test_executor", "legacy") or "legacy")
        == "flow-v3"
    ):
        runtime = FlowV3Runtime(flow_v3_runtime_config(ROOT, config.policy))
        runtime.register_local()
        runtime.register_observed_services(
            flow_v3_observed_service_states()
        )
        write_flow_v3_release_manifest(runtime.config)
        return
    target = max(1, int(config.policy.get("test_engine_target_inflight", 2) or 2))
    store = EngineAdmissionStore(ROOT)
    current = store.read()
    controller = store.active_controller_lease(current)
    if controller and controller.get("owner") != "workflow":
        return
    enabled = engine_executor_enabled(config)
    pump_status = EnginePump(ROOT).status()
    drain_needed = engine_pump_status_has_drain_work(pump_status)
    configured_enabled = enabled or drain_needed
    configured_draining = not enabled and drain_needed
    if (
        bool(current.get("enabled")) != configured_enabled
        or bool(current.get("draining")) != configured_draining
        or int(current.get("target_inflight", 1) or 1) != target
    ):
        store.configure(
            enabled=configured_enabled,
            target_inflight=target,
            draining=configured_draining,
        )


def reconcile_engine_operator_membership(config: DaemonConfig) -> None:
    if flow_v3_executor_mode(config):
        return
    try:
        result = EnginePump(ROOT).reconcile_allowed_operators(
            set(observed_operators(config))
        )
    except EnginePumpError as exc:
        append_daemon_runtime_event(
            ROOT,
            "engine_operator_membership_reconcile_deferred",
            {"error": str(exc)},
        )
        return
    if (
        result.get("cancelled")
        or result.get("standby_cancellation_requested")
        or result.get("blockers")
    ):
        append_daemon_runtime_event(
            ROOT,
            "engine_operator_membership_reconciled",
            result,
        )


def engine_pump_scheduling_state(pump: EnginePump) -> dict[str, object]:
    scheduling_state = getattr(pump, "scheduling_state", None)
    if callable(scheduling_state):
        return scheduling_state()
    return pump.read()


def enqueue_engine_compatibility_candidates(config: DaemonConfig) -> int:
    if read_stop_request(ROOT):
        return 0
    pump_generation_gate = local_engine_pump_generation_gate()
    write_json_atomic(
        ROOT
        / "TestUtils"
        / "tester_daemon"
        / "engine_pump_generation_gate.json",
        pump_generation_gate,
        ensure_ascii=True,
        sort_keys=True,
    )
    if pump_generation_gate["state"] != "ready":
        return 0
    target = max(1, int(config.policy.get("test_engine_target_inflight", 2) or 2))
    store = EngineAdmissionStore(ROOT)
    admission = store.read()
    controller = store.active_controller_lease(admission)
    if controller and controller.get("owner") != "workflow":
        return 0
    if not admission.get("enabled") or int(admission.get("target_inflight", 1) or 1) != target:
        store.configure(enabled=True, target_inflight=target, draining=False)
    pump = EnginePump(ROOT)
    pump_state = engine_pump_scheduling_state(pump)
    runtime_gate = engine_runtime_generation_gate(config)
    if runtime_gate["state"] != "ready":
        write_engine_runtime_gate_status(runtime_gate)
        return 0
    write_engine_runtime_gate_status(runtime_gate)
    execution_profile = str(
        config.policy.get("test_engine_execution_profile") or CONSERVATIVE_PROFILE
    )
    prewarm_enabled = execution_profile == FUSED_SCALABLE_PROFILE and bool(
        config.policy.get("test_engine_case_cache_prewarm", True)
    )
    prewarm_max_attempts = max(
        1,
        int(config.policy.get("test_engine_case_cache_prewarm_max_attempts", 3) or 3),
    )
    failures: list[str] = []
    if prewarm_enabled:
        pump_state, casegen_failures = enqueue_casegen_cache_prewarm_candidates(
            config,
            pump,
            pump_state,
            max_attempts=prewarm_max_attempts,
        )
        failures.extend(casegen_failures)
    held_candidates: list[dict[str, object]] = []
    try:
        candidates = discover_engine_submit_candidates(
            ROOT,
            config,
            pump_state=pump_state,
            traffic_debt=traffic_debt(build_traffic_balance(ROOT, config)),
            operator_priorities=load_operator_flow_priorities(ROOT, config),
            remote_engine_generation=str(
                runtime_gate.get("remote_engine_generation") or ""
            ),
            held_candidates=held_candidates,
        )
    except (EngineCandidateError, EngineJobBuildError, OSError, ValueError) as exc:
        append_daemon_runtime_event(ROOT, "engine_candidate_scan_failed", {"error": str(exc)})
        print(f"engine candidate scan failed: {exc}")
        return 2
    write_engine_retry_gate_status(held_candidates)
    enqueued: list[str] = []
    for candidate in candidates:
        try:
            if prewarm_enabled:
                submit_root = (
                    ROOT
                    / "TestUtils"
                    / "submit"
                    / candidate["op"]
                    / candidate["test_version"]
                )
                requirement = build_submit_case_cache_requirement(
                    ROOT,
                    submit_root,
                    op=candidate["op"],
                )
                prewarm = evaluate_case_cache_prewarm(
                    pump_state,
                    str(requirement["sha256"]),
                    max_attempts=prewarm_max_attempts,
                )
                if prewarm["state"] == "enqueue":
                    prewarm_attempt = int(prewarm["next_attempt"])
                    prewarm_spec, prewarm_payload = build_compatibility_job(
                        ROOT,
                        candidate["command"],
                        remote_root=str(
                            config.policy.get("test_engine_remote_root")
                            or config.remote_root
                        ),
                        execution_profile=CASE_CACHE_PREWARM_PROFILE,
                        job_id_suffix=case_cache_prewarm_suffix(
                            str(requirement["sha256"]), prewarm_attempt
                        ),
                        attempt_index=prewarm_attempt,
                        workflow_ingest=False,
                    )
                    entry = pump.enqueue(prewarm_spec, prewarm_payload)
                    observed_requirement = str(
                        entry.get("case_cache_requirement_sha256") or ""
                    )
                    if observed_requirement != requirement["sha256"]:
                        raise EngineJobBuildError(
                            "case-cache prewarm requirement changed while building: "
                            f"expected={requirement['sha256']} "
                            f"observed={observed_requirement}"
                        )
                    pump_state = engine_pump_scheduling_state(pump)
                    enqueued.append(str(entry.get("engine_job_id") or ""))
                    append_daemon_runtime_event(
                        ROOT,
                        "engine_case_cache_prewarm_enqueued",
                        {
                            "op": candidate["op"],
                            "test_version": candidate["test_version"],
                            "engine_job_id": entry.get("engine_job_id", ""),
                            "state": entry.get("state", ""),
                            "requirement_sha256": requirement["sha256"],
                            "prewarm_attempt": prewarm_attempt,
                        },
                    )
                    continue
                if prewarm["state"] == "waiting":
                    continue
                if prewarm["state"] == "blocked":
                    raise EngineJobBuildError(
                        "case-cache prewarm exhausted retries: "
                        f"requirement={requirement['sha256']} "
                        f"attempts={prewarm['attempt_count']} "
                        f"terminal={prewarm.get('terminal_state') or prewarm.get('engine_state')}"
                    )
            spec_path, payload_root = build_compatibility_job(
                ROOT,
                candidate["command"],
                remote_root=str(
                    config.policy.get("test_engine_remote_root") or config.remote_root
                ),
                execution_profile=execution_profile,
                job_id_suffix=str(candidate.get("job_id_suffix") or ""),
                attempt_index=int(candidate.get("attempt_index", "1") or 1),
                require_case_cache_hit=prewarm_enabled,
            )
            entry = pump.enqueue(spec_path, payload_root)
            enqueued.append(str(entry.get("engine_job_id") or ""))
            append_daemon_runtime_event(
                ROOT,
                "engine_dispatch_enqueued",
                {
                    "op": candidate["op"],
                    "test_version": candidate["test_version"],
                    "engine_job_id": entry.get("engine_job_id", ""),
                    "state": entry.get("state", ""),
                    "balance_debt": int(candidate.get("debt", "0") or 0),
                    "flow_priority": int(
                        candidate.get("flow_priority", "0") or 0
                    ),
                },
            )
        except (
            EngineJobBuildError,
            EnginePumpError,
            OSError,
            ValueError,
            json.JSONDecodeError,
        ) as exc:
            failures.append(f"{candidate['op']}/{candidate['test_version']}: {exc}")
            append_daemon_runtime_event(
                ROOT,
                "engine_dispatch_enqueue_failed",
                {
                    "op": candidate["op"],
                    "test_version": candidate["test_version"],
                    "error": str(exc),
                },
            )
    try:
        profiler_result = enqueue_next_profiler_job(
            ROOT,
            config,
            pump,
            engine_pump_scheduling_state(pump),
        )
        if profiler_result.get("outcome") == "enqueued":
            engine_job_id = str(profiler_result.get("engine_job_id") or "")
            if engine_job_id:
                enqueued.append(engine_job_id)
            append_daemon_runtime_event(
                ROOT,
                "engine_profiler_evidence_enqueued",
                profiler_result,
            )
    except (
        ProfilerEvidenceError,
        EngineJobBuildError,
        EnginePumpError,
        OSError,
        ValueError,
        json.JSONDecodeError,
    ) as exc:
        failures.append(f"profiler-evidence: {exc}")
        append_daemon_runtime_event(
            ROOT,
            "engine_profiler_evidence_enqueue_failed",
            {"error": str(exc)},
        )
    if enqueued:
        print(f"engine dispatch outbox enqueued: {', '.join(enqueued)}")
    if failures:
        print("engine dispatch enqueue failures: " + "; ".join(failures))
        return 2
    return 0


def local_engine_pump_generation_gate() -> dict[str, object]:
    worker = read_json_dict(
        ROOT / "TestUtils" / "tester_daemon" / "engine_pump_worker.json"
    )
    pid = int(worker.get("pid", 0) or 0)
    alive = bool(pid and process_alive(pid))
    expected = local_engine_code_generation(ROOT)
    actual = str(worker.get("code_generation") or "")
    state = "ready"
    reason = ""
    if alive and actual != expected:
        state = "hold"
        reason = (
            "active-engine-pump-code-generation-mismatch"
            if actual
            else "active-engine-pump-code-generation-unobserved"
        )
    return {
        "schema": "ascendop.local-engine-pump-generation-gate.v1",
        "state": state,
        "reason": reason,
        "pid": pid,
        "alive": alive,
        "worker_code_generation": actual,
        "expected_code_generation": expected,
        "checked_at": utc_now_iso(),
    }


def enqueue_or_schedule_engine_candidates(
    config: DaemonConfig,
    refresher: "EngineCandidateRefresher | None",
    *,
    config_path: Path | None = None,
) -> int:
    """Keep resident workflow ticks free of Engine admission I/O."""
    if flow_v3_executor_mode(config):
        maybe_start_flow_v3_worker(
            config,
            config_path=config_path,
        )
        return enqueue_flow_v3_candidates(config)
    if refresher is None:
        result = enqueue_engine_compatibility_candidates(config)
        maybe_start_engine_pump_worker(config)
        return result
    refresher.maybe_start(config)
    return 0


def enqueue_flow_v3_candidates(config: DaemonConfig) -> int:
    if read_stop_request(ROOT):
        return 0
    if not bool(flow_v3_process_generation_status(ROOT)["matches"]):
        record_flow_v3_process_generation_drift("candidate-producer")
        return 75
    try:
        runtime = FlowV3Runtime(flow_v3_runtime_config(ROOT, config.policy))
        runtime.register_local()
        runtime.register_observed_services(
            flow_v3_observed_service_states()
        )
        write_flow_v3_release_manifest(runtime.config)
        readiness = runtime.readiness()
        if not readiness["ready"]:
            append_daemon_runtime_event(
                ROOT,
                "flow_v3_dispatch_held",
                {
                    "blockers": readiness["blockers"],
                    "release_generation": runtime.config.release.release_generation,
                },
            )
            return 0
        held_candidates: list[dict[str, object]] = []
        candidates = discover_engine_submit_candidates(
            ROOT,
            config,
            pump_state={},
            traffic_debt=traffic_debt(build_traffic_balance(ROOT, config)),
            operator_priorities=load_operator_flow_priorities(ROOT, config),
            remote_engine_generation=(
                runtime.config.release.endpoint_code_generation
            ),
            held_candidates=held_candidates,
        )
        target = max(
            1,
            int(config.policy.get("test_engine_target_inflight", 4) or 4),
        )
        enqueued: list[str] = []
        active_identities = flow_v3_active_operator_test_identities(runtime)
        suppressed_identities = flow_v3_suppressed_operator_test_identities(runtime)
        available_slots = max(0, target - len(active_identities))
        for candidate in candidates:
            if len(enqueued) >= available_slots:
                break
            parsed = parse_submit_command(str(candidate.get("command") or ""))
            workflow_identity = (
                str(parsed["op"]),
                str(parsed["test_version"]),
                str(parsed["case_version"]),
                "operator-test",
            )
            if workflow_identity in suppressed_identities:
                continue
            envelope, _ = build_candidate_request(
                ROOT,
                candidate,
                endpoint_id=runtime.config.release.endpoint_id,
                endpoint_generation=(
                    runtime.config.release.endpoint_generation
                ),
                registration_generation=(
                    runtime.config.release.registration_generation
                ),
                code_generation=runtime.config.release.local_generation,
                remote_root=runtime.config.route.remote_root,
                package_root=runtime.config.package_root,
                profiler_mode="primary-all-cases",
            )
            record = runtime.dispatcher.enqueue(
                envelope,
                destination=runtime.config.release.endpoint_id,
            )
            enqueued.append(
                f"{record['request_id']}/{record['attempt_id']}"
            )
            active_identities.add(workflow_identity)
            suppressed_identities.add(workflow_identity)
        if not enqueued and not flow_v3_operator_test_active(runtime):
            profiler = next_flow_v3_profiler_request(ROOT, config)
            if profiler is not None:
                envelope, _ = build_diagnostic_request(
                    ROOT,
                    profiler["candidate"],
                    profiler_plan=profiler["profiler_plan"],
                    profiler_mode=str(profiler["profiler_mode"]),
                    endpoint_id=runtime.config.release.endpoint_id,
                    endpoint_generation=(
                        runtime.config.release.endpoint_generation
                    ),
                    registration_generation=(
                        runtime.config.release.registration_generation
                    ),
                    code_generation=(
                        runtime.config.release.local_generation
                    ),
                    remote_root=runtime.config.route.remote_root,
                    package_root=runtime.config.package_root,
                )
                record = runtime.dispatcher.enqueue(
                    envelope,
                    destination=runtime.config.release.endpoint_id,
                )
                profiler_record = mark_flow_v3_profiler_enqueued(
                    profiler,
                    request_id=str(record["request_id"]),
                    attempt_id=str(record["attempt_id"]),
                )
                enqueued.append(
                    f"{record['request_id']}/{record['attempt_id']}"
                )
                append_daemon_runtime_event(
                    ROOT,
                    "flow_v3_profiler_enqueued",
                    profiler_record,
                )
        if enqueued:
            append_daemon_runtime_event(
                ROOT,
                "flow_v3_candidates_enqueued",
                {
                    "requests": enqueued,
                    "release_generation": runtime.config.release.release_generation,
                },
            )
        return 0
    except (
        EngineCandidateError,
        ProfilerEvidenceError,
        FlowV3RequestBuildError,
        FlowV3StoreError,
        EngineRouteError,
        OSError,
        ValueError,
    ) as exc:
        append_daemon_runtime_event(
            ROOT,
            "flow_v3_candidate_scan_failed",
            {"error": f"{type(exc).__name__}: {exc}"},
        )
        return 2


def flow_v3_operator_test_active(runtime: FlowV3Runtime) -> bool:
    return bool(flow_v3_active_operator_test_identities(runtime))


def flow_v3_active_operator_test_identities(
    runtime: FlowV3Runtime,
) -> set[tuple[str, str, str, str]]:
    active = runtime.store.attempts_in_states(
        {
            "created",
            "validated",
            "queued",
            "admitted",
            "dispatched",
            "accepted",
            "running",
            "return-ready",
            "ingested",
            "acknowledged",
        },
        limit=100,
    )
    identities: set[tuple[str, str, str, str]] = set()
    for record in active:
        if not isinstance(record, dict):
            continue
        workflow = record.get("envelope", {}).get("workflow", {})
        if not isinstance(workflow, dict):
            continue
        operation_kind = str(workflow.get("operation_kind") or "")
        if operation_kind != "operator-test":
            continue
        identities.add(
            (
                str(workflow.get("operator") or ""),
                str(workflow.get("test_version") or ""),
                str(workflow.get("case_version") or ""),
                operation_kind,
            )
        )
    return identities


def flow_v3_suppressed_operator_test_identities(
    runtime: FlowV3Runtime,
) -> set[tuple[str, str, str, str]]:
    """Suppress duplicate gates while still permitting a fixed infra generation."""

    current_generation = str(runtime.config.release.local_generation)
    identities: set[tuple[str, str, str, str]] = set()
    for record in runtime.store.workflow_attempts(operation_kind="operator-test"):
        if not isinstance(record, dict):
            continue
        envelope = record.get("envelope", {})
        workflow = envelope.get("workflow", {}) if isinstance(envelope, dict) else {}
        if not isinstance(workflow, dict):
            continue
        identity = (
            str(workflow.get("operator") or ""),
            str(workflow.get("test_version") or ""),
            str(workflow.get("case_version") or ""),
            str(workflow.get("operation_kind") or ""),
        )
        state = str(record.get("state") or "")
        code_generation = str(
            envelope.get("meta", {}).get("code_generation") or ""
        )
        if (
            state in {
                "terminal-success",
                "terminal-business-failure",
            }
            or state in {
                "created",
                "validated",
                "queued",
                "admitted",
                "dispatched",
                "accepted",
                "running",
                "return-ready",
                "ingested",
                "acknowledged",
                "quarantined",
            }
            or (
                state == "terminal-infrastructure-failure"
                and code_generation == current_generation
            )
        ):
            identities.add(identity)
    return identities


def engine_runtime_generation_gate(config: DaemonConfig) -> dict[str, object]:
    repo = Path(
        str(config.policy.get("test_engine_gitpartner_repo") or "GitPartner")
    )
    if not repo.is_absolute():
        repo = ROOT / repo
    expected = expected_remote_engine_code_generation(
        ROOT,
        gitpartner_repo=repo,
    )
    admission = EngineAdmissionStore(ROOT).read()
    snapshot = admission.get("last_engine_snapshot")
    remote = (
        str(snapshot.get("engine_code_generation") or "")
        if isinstance(snapshot, dict)
        else ""
    )
    state = "ready" if expected and remote == expected else "hold"
    reason = ""
    if not expected:
        reason = "local-engine-runtime-generation-unavailable"
    elif not remote:
        reason = "remote-engine-runtime-generation-unobserved"
    elif remote != expected:
        reason = "remote-engine-runtime-generation-mismatch"
    return {
        "schema": "ascendop.engine-runtime-gate.v1",
        "state": state,
        "reason": reason,
        "endpoint_id": str(config.policy.get("test_engine_endpoint_id") or ""),
        "gitpartner_repo": str(repo.relative_to(ROOT) if ROOT in repo.parents else repo),
        "expected_remote_engine_generation": expected,
        "remote_engine_generation": remote,
        "observed_at": utc_now_iso(),
    }


def write_engine_runtime_gate_status(payload: dict[str, object]) -> None:
    path = ROOT / "TestUtils" / "tester_daemon" / "ENGINE_RUNTIME_GATE.json"
    previous = read_engine_promotion_object(path)
    write_json_atomic(path, payload, ensure_ascii=True)
    previous_key = (
        str(previous.get("state") or ""),
        str(previous.get("reason") or ""),
        str(previous.get("expected_remote_engine_generation") or ""),
        str(previous.get("remote_engine_generation") or ""),
    )
    current_key = (
        str(payload.get("state") or ""),
        str(payload.get("reason") or ""),
        str(payload.get("expected_remote_engine_generation") or ""),
        str(payload.get("remote_engine_generation") or ""),
    )
    if current_key != previous_key:
        append_daemon_runtime_event(ROOT, "engine_runtime_gate_changed", payload)


def write_engine_retry_gate_status(held: list[dict[str, object]]) -> None:
    path = ROOT / "TestUtils" / "tester_daemon" / "ENGINE_RETRY_GATE.json"
    payload: dict[str, object] = {
        "schema": "ascendop.engine-retry-gate.v1",
        "state": "hold" if held else "ready",
        "held_count": len(held),
        "held": held,
        "observed_at": utc_now_iso(),
    }
    previous = read_engine_promotion_object(path)
    write_json_atomic(path, payload, ensure_ascii=True)
    previous_key = [
        (
            str(item.get("op") or ""),
            str(item.get("test_version") or ""),
            str(item.get("failure_signature") or ""),
            str(item.get("failure_engine_generation") or ""),
        )
        for item in previous.get("held", [])
        if isinstance(item, dict)
    ]
    current_key = [
        (
            str(item.get("op") or ""),
            str(item.get("test_version") or ""),
            str(item.get("failure_signature") or ""),
            str(item.get("failure_engine_generation") or ""),
        )
        for item in held
    ]
    if current_key != previous_key:
        append_daemon_runtime_event(ROOT, "engine_retry_gate_changed", payload)


def enqueue_casegen_cache_prewarm_candidates(
    config: DaemonConfig,
    pump: EnginePump,
    pump_state: dict[str, object],
    *,
    max_attempts: int,
) -> tuple[dict[str, object], list[str]]:
    """Prewarm complete Tester case versions before a Solver candidate exists."""

    failures: list[str] = []
    status_rows: list[dict[str, object]] = []
    remote_root = str(
        config.policy.get("test_engine_remote_root") or config.remote_root
    )
    hardware = str(config.policy.get("test_engine_hardware") or "910B4")
    for op in observed_operators(config):
        issue = latest_casegen_evidence_issue(ROOT, op, config)
        if issue is not None:
            status_rows.append(
                {
                    "op": op,
                    "case_version": issue.case_version,
                    "state": "evidence-incomplete",
                    "missing": list(issue.missing),
                }
            )
            continue
        case_dir = latest_case_dir(ROOT, op)
        if case_dir is None:
            continue
        try:
            season = operator_season(config, op)
            snapshot = materialize_casegen_cache_snapshot(
                ROOT,
                op=op,
                season=season,
                case_dir=case_dir,
            )
            snapshot_root = Path(str(snapshot["snapshot_root"]))
            requirement = build_submit_case_cache_requirement(
                ROOT,
                snapshot_root,
                op=op,
            )
            prewarm = evaluate_case_cache_prewarm(
                pump_state,
                str(requirement["sha256"]),
                max_attempts=max_attempts,
            )
            row: dict[str, object] = {
                "op": op,
                "season": season,
                "case_version": case_dir.name,
                "snapshot_fingerprint": snapshot.get("fingerprint", ""),
                "snapshot_cache_hit": bool(snapshot.get("cache_hit")),
                "requirement_sha256": requirement["sha256"],
                **prewarm,
            }
            if prewarm["state"] == "enqueue":
                attempt = int(prewarm["next_attempt"])
                test_version = f"{op}_casegen_{case_dir.name}"
                vendor = re.sub(r"[^a-z0-9_]+", "_", op.lower()).strip("_")
                command = (
                    f"python scripts\\next_workflow.py gitpartner-run-submit "
                    f"{op} {test_version} --season {season} --mode correct "
                    f"--vendor {vendor}_case_cache --hardware {hardware} "
                    f"--case-version {case_dir.name} --remote-root {remote_root}"
                )
                spec_path, payload_root = build_compatibility_job(
                    ROOT,
                    command,
                    remote_root=remote_root,
                    submit_root_override=snapshot_root,
                    execution_profile=CASE_CACHE_PREWARM_PROFILE,
                    job_id_suffix=case_cache_prewarm_suffix(
                        str(requirement["sha256"]), attempt
                    ),
                    attempt_index=attempt,
                    workflow_ingest=False,
                )
                entry = pump.enqueue(spec_path, payload_root)
                observed_requirement = str(
                    entry.get("case_cache_requirement_sha256") or ""
                )
                if observed_requirement != requirement["sha256"]:
                    raise EngineJobBuildError(
                        "casegen prewarm requirement changed while building: "
                        f"expected={requirement['sha256']} "
                        f"observed={observed_requirement}"
                    )
                pump_state = engine_pump_scheduling_state(pump)
                row.update(
                    {
                        "state": "waiting",
                        "engine_state": entry.get("state", ""),
                        "engine_job_id": entry.get("engine_job_id", ""),
                    }
                )
                append_daemon_runtime_event(
                    ROOT,
                    "engine_casegen_cache_prewarm_enqueued",
                    row,
                )
            status_rows.append(row)
        except (
            EngineJobBuildError,
            EnginePumpError,
            OSError,
            ValueError,
            json.JSONDecodeError,
        ) as exc:
            failures.append(f"{op}/{case_dir.name} casegen-prewarm: {exc}")
            status_rows.append(
                {
                    "op": op,
                    "case_version": case_dir.name,
                    "state": "failed",
                    "error": str(exc),
                }
            )
            append_daemon_runtime_event(
                ROOT,
                "engine_casegen_cache_prewarm_failed",
                status_rows[-1],
            )
    write_casegen_cache_prewarm_status(status_rows)
    return pump_state, failures


def write_casegen_cache_prewarm_status(rows: list[dict[str, object]]) -> None:
    path = ROOT / "TestUtils" / "tester_daemon" / "case_cache_prewarm_status.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "protocol_version": "casegen-cache-prewarm-status-v1",
        "updated_at": utc_now_iso(),
        "operators": rows,
    }
    write_json_atomic(
        path,
        payload,
        ensure_ascii=True,
        sort_keys=True,
    )


FLOW_V3_WORKER_FILE = "flow_v3_worker.json"


def flow_v3_worker_path(root: Path = ROOT) -> Path:
    return root / "TestUtils" / "tester_daemon" / FLOW_V3_WORKER_FILE


def flow_v3_worker_is_alive(
    root: Path,
    *,
    max_heartbeat_age_seconds: float = 300.0,
) -> bool:
    worker = read_json_dict(flow_v3_worker_path(root))
    pid = int(worker.get("pid", 0) or 0)
    expected_start_token = str(worker.get("start_token") or "")
    if pid <= 0 or not expected_start_token or not process_alive(pid):
        return False
    if process_start_token(pid) != expected_start_token:
        return False
    heartbeat = parse_timestamp(str(worker.get("heartbeat_at") or ""))
    if heartbeat is None:
        return True
    age = max(
        0.0,
        (datetime.now(timezone.utc) - heartbeat).total_seconds(),
    )
    return age <= max(1.0, float(max_heartbeat_age_seconds))


def resolve_flow_v3_config_path(
    config: DaemonConfig,
    explicit: Path | None = None,
) -> Path:
    if explicit is not None:
        return explicit.resolve() if explicit.is_absolute() else (ROOT / explicit).resolve()
    configured = str(config.policy.get("flow_v3_config_path") or "").strip()
    if configured:
        path = Path(configured)
        return path.resolve() if path.is_absolute() else (ROOT / path).resolve()
    matches: list[Path] = []
    config_root = ROOT / "tools" / "tester_daemon" / "config"
    for path in sorted(config_root.glob("*.json")):
        try:
            candidate = load_config(path, apply_completion_markers=False)
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        if (
            flow_v3_executor_mode(candidate)
            and candidate.season == config.season
            and tuple(candidate.operators) == tuple(config.operators)
            and str(candidate.policy.get("test_engine_endpoint_id") or "")
            == str(config.policy.get("test_engine_endpoint_id") or "")
        ):
            matches.append(path.resolve())
    if len(matches) != 1:
        raise ValueError(
            "Flow V3 worker requires one exact config path; "
            f"matching_config_count={len(matches)}"
        )
    return matches[0]


def flow_v3_worker_identity(
    runtime: FlowV3Runtime,
    *,
    config_path: Path,
    interval_seconds: float,
) -> dict[str, object]:
    return {
        "release_generation": runtime.config.release.release_generation,
        "local_generation": runtime.config.release.local_generation,
        "endpoint_code_generation": (
            runtime.config.release.endpoint_code_generation
        ),
        "endpoint_id": runtime.config.release.endpoint_id,
        "endpoint_generation": runtime.config.release.endpoint_generation,
        "registration_generation": (
            runtime.config.release.registration_generation
        ),
        "config_path": str(config_path),
        "database_path": str(runtime.config.database_path),
        "interval_seconds": float(interval_seconds),
    }


def stop_mismatched_flow_v3_worker(
    worker: dict[str, object],
    *,
    desired_identity: dict[str, object],
) -> dict[str, object]:
    pid = int(worker.get("pid", 0) or 0)
    expected_start_token = str(worker.get("start_token") or "")
    current_start_token = process_start_token(pid)
    report: dict[str, object] = {
        "pid": pid,
        "expected_start_token": expected_start_token,
        "current_start_token": current_start_token,
        "previous_identity": worker.get("worker_identity"),
        "desired_identity": desired_identity,
        "stopped": False,
    }
    if pid <= 0 or not process_alive(pid):
        report.update({"stopped": True, "action": "already-exited"})
        return report
    if not expected_start_token or current_start_token != expected_start_token:
        report["action"] = "refused-process-identity-mismatch"
        return report
    command_line = windows_process_command_line(pid).lower().replace("/", "\\")
    if "daemon.py flow-v3" not in command_line or " run" not in command_line:
        report["action"] = "refused-command-marker-mismatch"
        report["command_line"] = command_line
        return report
    stop_result = taskkill_tree(pid) if os.name == "nt" else stop_process_force(pid)
    deadline = time.monotonic() + 5.0
    while process_alive(pid) and time.monotonic() < deadline:
        time.sleep(0.05)
    report.update(
        {
            "action": "stopped-mismatched-worker",
            "stop_result": stop_result,
            "stopped": not process_alive(pid),
        }
    )
    return report


def maybe_start_flow_v3_worker(
    config: DaemonConfig,
    *,
    config_path: Path | None = None,
    allow_stop_drain: bool = False,
) -> dict[str, object]:
    if not flow_v3_executor_mode(config):
        return {"started": False, "reason": "not-flow-v3"}
    generation = flow_v3_process_generation_status(ROOT)
    if not bool(generation["matches"]):
        record_flow_v3_process_generation_drift("worker-supervisor")
        return {
            "started": False,
            "reason": "process-generation-drift",
            **generation,
        }
    stopping = bool(read_stop_request(ROOT))
    try:
        runtime = FlowV3Runtime(flow_v3_runtime_config(ROOT, config.policy))
        resolved_config = resolve_flow_v3_config_path(config, config_path)
    except (EngineRouteError, FlowV3StoreError, OSError, ValueError) as exc:
        failure = {
            "started": False,
            "reason": "flow-v3-runtime-unavailable",
            "error": f"{type(exc).__name__}: {exc}",
        }
        append_daemon_runtime_event(ROOT, "flow_v3_worker_start_failed", failure)
        return failure
    if stopping and (
        not allow_stop_drain or not runtime.has_drain_work()
    ):
        return {"started": False, "reason": "stop_requested"}
    interval = max(
        0.25,
        float(config.policy.get("flow_v3_worker_interval_seconds", 1.0) or 1.0),
    )
    identity = flow_v3_worker_identity(
        runtime,
        config_path=resolved_config,
        interval_seconds=interval,
    )
    worker_path = flow_v3_worker_path(ROOT)
    worker = read_json_dict(worker_path)
    wait_timeout = max(30, int(runtime.config.wait_timeout_seconds))
    alive = flow_v3_worker_is_alive(
        ROOT,
        max_heartbeat_age_seconds=float(wait_timeout + 120),
    )
    if alive and worker.get("worker_identity") == identity:
        return {
            "started": False,
            "reason": "worker_active",
            "pid": int(worker.get("pid", 0) or 0),
            "worker_identity": identity,
        }
    if alive or (
        int(worker.get("pid", 0) or 0) > 0
        and process_alive(int(worker.get("pid", 0) or 0))
    ):
        stopped = stop_mismatched_flow_v3_worker(
            worker,
            desired_identity=identity,
        )
        append_daemon_runtime_event(
            ROOT,
            "flow_v3_worker_identity_mismatch",
            stopped,
        )
        if not bool(stopped.get("stopped")):
            return {
                "started": False,
                "reason": "worker_identity_mismatch",
                **stopped,
            }
    command = [
        sys.executable,
        str(ROOT / "tools" / "tester_daemon" / "daemon.py"),
        "flow-v3",
        "--config",
        str(resolved_config),
        "--interval-seconds",
        str(interval),
        "run",
    ]
    logs = ROOT / "TestUtils" / "tester_daemon" / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    with (logs / "flow_v3_worker.out.log").open(
        "a", encoding="utf-8"
    ) as out, (logs / "flow_v3_worker.err.log").open(
        "a", encoding="utf-8"
    ) as err:
        process = subprocess.Popen(
            command,
            cwd=ROOT,
            stdin=subprocess.DEVNULL,
            stdout=out,
            stderr=err,
            creationflags=process_creation_flags(),
            startupinfo=process_startupinfo(),
        )
    record = {
        "schema": "ascendop.flow.worker.v3",
        "pid": process.pid,
        "start_token": process_start_token(process.pid),
        "started_at": utc_now_iso(),
        "heartbeat_at": utc_now_iso(),
        "command": command,
        "worker_identity": identity,
        "state": "starting",
    }
    write_json_atomic(worker_path, record, ensure_ascii=True)
    append_daemon_runtime_event(ROOT, "flow_v3_worker_started", record)
    return {"started": True, **record}


def maybe_start_engine_pump_worker(
    config: DaemonConfig,
    *,
    allow_stop_drain: bool = False,
    force_capacity_sync: bool = False,
) -> dict[str, object]:
    if read_stop_request(ROOT) and not allow_stop_drain:
        return {"started": False, "reason": "stop_requested"}
    operator_scope = {
        str(op)
        for op in (*config.operators, *config.draining_operators)
        if str(op)
    }
    pump = EnginePump(ROOT, operator_scope=operator_scope)
    status = pump.status()
    counts = status.get("state_counts", {})
    runtime_gate = engine_runtime_generation_gate(config)
    runtime_sync_required = bool(
        runtime_gate.get("expected_remote_engine_generation")
    ) and str(runtime_gate.get("state") or "") != "ready"
    has_work = (
        engine_pump_status_has_drain_work(status)
        or engine_pump_status_has_retryable_admission_work(status)
        or (isinstance(counts, dict) and int(counts.get("pending", 0) or 0) > 0)
        or int(status.get("staged_enqueue_count", 0) or 0) > 0
        or runtime_sync_required
    )
    if not has_work and not force_capacity_sync:
        return {"started": False, "reason": "no_engine_work"}
    state_dir = ROOT / "TestUtils" / "tester_daemon"
    worker_path = state_dir / "engine_pump_worker.json"
    worker = read_json_dict(worker_path)
    interval = max(0.25, float(config.policy.get("test_engine_pump_interval_seconds", 1) or 1))
    engine_endpoint_id = str(
        config.policy.get("test_engine_endpoint_id") or ""
    ).strip()
    registry_path = str(
        config.policy.get("control_plane_registry")
        or "Develop/registry/system_registry.json"
    )
    try:
        route = (
            resolve_registered_engine_route(
                ROOT,
                registry_path=registry_path,
                endpoint_id=engine_endpoint_id,
            )
            if engine_endpoint_id
            else legacy_engine_route(config.policy, config.remote_root)
        )
    except (EngineRouteError, OSError, ValueError) as exc:
        failure = {
            "started": False,
            "reason": "engine_route_unavailable",
            "endpoint_id": engine_endpoint_id,
            "error": f"{type(exc).__name__}: {exc}",
        }
        append_daemon_runtime_event(ROOT, "engine_pump_route_unavailable", failure)
        return failure
    wait_timeout_seconds = max(
        30,
        int(config.policy.get("test_engine_wait_timeout_seconds", 180) or 180),
    )
    initial_grace_seconds = max(
        0,
        int(config.policy.get("test_engine_wait_initial_grace_seconds", 0) or 0),
    )
    exchange_wait_ready_seconds = max(
        0,
        int(
            config.policy.get(
                "test_engine_exchange_wait_ready_seconds",
                45,
            )
            or 0
        ),
    )
    capacity_overrides = engine_capacity_overrides(config)
    identity_capacity_overrides = dict(capacity_overrides)
    if route.device_inventory:
        identity_capacity_overrides["device_inventory"] = [
            dict(item) for item in route.device_inventory
        ]
    route_gp_repo = Path(route.gitpartner_repo)
    if not route_gp_repo.is_absolute():
        route_gp_repo = ROOT / route_gp_repo
    expected_remote_generation = expected_remote_engine_code_generation(
        ROOT,
        gitpartner_repo=route_gp_repo,
    )
    worker_identity = build_engine_pump_worker_identity(
        route,
        operator_scope=operator_scope,
        capacity_overrides=identity_capacity_overrides,
        wait_timeout_seconds=wait_timeout_seconds,
        initial_grace_seconds=initial_grace_seconds,
        interval_seconds=interval,
        exchange_wait_ready_seconds=exchange_wait_ready_seconds,
        expected_remote_generation=expected_remote_generation,
    )
    local_worker_generation = local_engine_code_generation(ROOT)
    worker_identity["local_worker_code_generation"] = local_worker_generation
    pid = int(worker.get("pid", 0) or 0)
    heartbeat_timeout_seconds = max(
        60.0,
        float(wait_timeout_seconds) + max(30.0, interval * 5.0),
    )
    worker_active = engine_pump_worker_is_alive(
        ROOT,
        max_heartbeat_age_seconds=heartbeat_timeout_seconds,
    )
    if not worker_active and pid > 0 and process_alive(pid):
        command_line = windows_process_command_line(pid).lower().replace("/", "\\")
        stale_worker_age = engine_pump_worker_age_seconds(worker)
        stale_event = {
            "pid": pid,
            "start_token": str(worker.get("start_token") or ""),
            "heartbeat_age_seconds": stale_worker_age,
            "heartbeat_timeout_seconds": heartbeat_timeout_seconds,
            "command_line": command_line,
            "worker_identity": worker.get("worker_identity"),
        }
        if "daemon.py engine-pump" in command_line and " run" in command_line:
            stopped = stop_mismatched_engine_pump_worker(
                worker,
                desired_identity=worker_identity,
            )
            stale_event["stop_result"] = stopped
            if not bool(stopped.get("stopped")):
                append_daemon_runtime_event(
                    ROOT,
                    "engine_pump_worker_stale_stop_failed",
                    stale_event,
                )
                return {
                    "started": False,
                    "reason": "stale_worker_stop_failed",
                    **stale_event,
                }
        append_daemon_runtime_event(
            ROOT,
            "engine_pump_worker_stale_record_discarded",
            stale_event,
        )
        worker = {}
        pid = 0
    if worker_active:
        if worker.get("worker_identity") == worker_identity:
            cancel_engine_pump_worker_replacement(worker)
            return {
                "started": False,
                "reason": "worker_active",
                "pid": pid,
                "worker_identity": worker_identity,
            }
        replacement = request_engine_pump_worker_replacement(
            worker,
            desired_identity=worker_identity,
        )
        handoff_grace_seconds = max(
            1.0,
            float(
                config.policy.get(
                    "test_engine_pump_worker_handoff_grace_seconds",
                    15,
                )
                or 15
            ),
        )
        replacement_age_seconds = engine_pump_replacement_request_age_seconds(
            replacement.get("request", {})
        )
        replacement["handoff_grace_seconds"] = handoff_grace_seconds
        replacement["handoff_age_seconds"] = replacement_age_seconds
        if (
            replacement.get("action") == "graceful-replacement-requested"
            and replacement_age_seconds >= handoff_grace_seconds
        ):
            escalation = stop_mismatched_engine_pump_worker(
                worker,
                desired_identity=worker_identity,
            )
            replacement["escalation"] = escalation
            replacement["stopped"] = bool(escalation.get("stopped"))
            append_daemon_runtime_event(
                ROOT,
                "engine_pump_worker_replacement_escalated",
                {
                    **escalation,
                    "handoff_age_seconds": replacement_age_seconds,
                    "handoff_grace_seconds": handoff_grace_seconds,
                },
            )
        append_daemon_runtime_event(
            ROOT,
            "engine_pump_worker_identity_mismatch",
            replacement,
        )
        if not bool(replacement.get("stopped")):
            reason = (
                "worker_identity_handoff_requested"
                if replacement.get("action") == "graceful-replacement-requested"
                else "worker_identity_handoff_failed"
            )
            return {
                "started": False,
                "reason": reason,
                "pid": pid,
                "worker_identity": worker_identity,
                "replacement": replacement,
            }

    last_started = parse_timestamp(str(worker.get("started_at", "") or ""))
    if last_started is not None and worker.get("worker_identity") == worker_identity:
        age = (datetime.now(timezone.utc) - last_started).total_seconds()
        if age < interval:
            return {"started": False, "reason": "interval", "age_seconds": age}

    command = [
        sys.executable,
        str(ROOT / "tools" / "tester_daemon" / "daemon.py"),
        "engine-pump",
        "--gitpartner-repo",
        route.gitpartner_repo,
        "--engine-root",
        route.engine_root,
        "--remote-root",
        route.remote_root,
        "--transport",
        route.transport,
        "--wait-timeout-seconds",
        str(wait_timeout_seconds),
        "--initial-grace-seconds",
        str(initial_grace_seconds),
        "--interval-seconds",
        str(interval),
        "--exchange-wait-ready-seconds",
        str(exchange_wait_ready_seconds),
    ]
    if engine_endpoint_id:
        command.extend(
            [
                "--registry",
                registry_path,
                "--registered-endpoint",
                engine_endpoint_id,
            ]
        )
    for engine_key, value in capacity_overrides.items():
        command.extend(["--" + engine_key.replace("_", "-"), str(value)])
    for op in sorted(operator_scope):
        command.extend(["--operator-scope", op])
    command.append("run")
    logs = state_dir / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    engine_pump_replacement_request_path().unlink(missing_ok=True)
    with (logs / "engine_pump.out.log").open("a", encoding="utf-8") as out, (
        logs / "engine_pump.err.log"
    ).open("a", encoding="utf-8") as err:
        process = subprocess.Popen(
            command,
            cwd=ROOT,
            stdin=subprocess.DEVNULL,
            stdout=out,
            stderr=err,
            creationflags=process_creation_flags(),
            startupinfo=process_startupinfo(),
        )
    record = {
        "pid": process.pid,
        "start_token": process_start_token(process.pid),
        "started_at": utc_now_iso(),
        "code_generation": local_worker_generation,
        "command": command,
        "worker_identity": worker_identity,
        "state_counts": counts,
    }
    write_json_atomic(worker_path, record, ensure_ascii=True)
    append_daemon_runtime_event(ROOT, "engine_pump_worker_started", record)
    return {"started": True, **record}


def engine_pump_worker_age_seconds(worker: dict[str, object]) -> float | None:
    heartbeat = parse_timestamp(str(worker.get("heartbeat_at") or ""))
    if heartbeat is None:
        return None
    return max(
        0.0,
        (datetime.now(timezone.utc) - heartbeat).total_seconds(),
    )


def build_engine_pump_worker_identity(
    route: EngineTransportRoute,
    *,
    operator_scope: set[str],
    capacity_overrides: dict[str, object],
    wait_timeout_seconds: int,
    initial_grace_seconds: int,
    interval_seconds: float,
    exchange_wait_ready_seconds: int,
    expected_remote_generation: str,
) -> dict[str, object]:
    return {
        "endpoint_id": route.endpoint_id,
        "node_id": route.node_id,
        "registration_generation": route.registration_generation,
        "gitpartner_repo": route.gitpartner_repo,
        "remote_root": route.remote_root,
        "engine_root": route.engine_root,
        "transport": route.transport,
        "transport_mode": route.transport_mode,
        "control_channel": route.control_channel,
        "result_channel": route.result_channel,
        "remote_gitpartner_repo": route.remote_gitpartner_repo,
        "operator_scope": sorted(operator_scope),
        "capacity_overrides": {
            str(key): json.loads(json.dumps(value, sort_keys=True))
            for key, value in sorted(capacity_overrides.items())
        },
        "wait_timeout_seconds": int(wait_timeout_seconds),
        "initial_grace_seconds": int(initial_grace_seconds),
        "interval_seconds": float(interval_seconds),
        "exchange_wait_ready_seconds": int(exchange_wait_ready_seconds),
        "expected_remote_generation": str(expected_remote_generation or ""),
    }


def stop_mismatched_engine_pump_worker(
    worker: dict[str, object],
    *,
    desired_identity: dict[str, object],
) -> dict[str, object]:
    pid = int(worker.get("pid", 0) or 0)
    expected_start_token = str(worker.get("start_token") or "")
    current_start_token = process_start_token(pid)
    report: dict[str, object] = {
        "pid": pid,
        "expected_start_token": expected_start_token,
        "current_start_token": current_start_token,
        "previous_identity": worker.get("worker_identity"),
        "desired_identity": desired_identity,
        "stopped": False,
    }
    if pid <= 0 or not process_alive(pid):
        report.update({"stopped": True, "action": "already-exited"})
        return report
    if not expected_start_token or current_start_token != expected_start_token:
        report["action"] = "refused-process-identity-mismatch"
        return report
    stop_result = (
        taskkill_tree(pid)
        if os.name == "nt"
        else stop_process_force(pid)
    )
    deadline = time.monotonic() + 5.0
    while process_alive(pid) and time.monotonic() < deadline:
        time.sleep(0.05)
    report.update(
        {
            "action": "stopped-mismatched-worker",
            "stop_result": stop_result,
            "stopped": not process_alive(pid),
        }
    )
    return report


def engine_pump_replacement_request_path(root: Path | None = None) -> Path:
    base = (root or ROOT).resolve()
    return (
        base
        / "TestUtils"
        / "tester_daemon"
        / ENGINE_PUMP_REPLACEMENT_REQUEST_FILE
    )


def request_engine_pump_worker_replacement(
    worker: dict[str, object],
    *,
    desired_identity: dict[str, object],
) -> dict[str, object]:
    pid = int(worker.get("pid", 0) or 0)
    expected_start_token = str(worker.get("start_token") or "")
    current_start_token = process_start_token(pid)
    report: dict[str, object] = {
        "pid": pid,
        "expected_start_token": expected_start_token,
        "current_start_token": current_start_token,
        "previous_identity": worker.get("worker_identity"),
        "desired_identity": desired_identity,
        "stopped": False,
    }
    if pid <= 0 or not process_alive(pid):
        report.update({"stopped": True, "action": "already-exited"})
        return report
    if not expected_start_token or current_start_token != expected_start_token:
        report["action"] = "refused-process-identity-mismatch"
        return report
    path = engine_pump_replacement_request_path()
    current = read_json_dict(path)
    requested_at = (
        str(current.get("requested_at") or "")
        if int(current.get("pid", 0) or 0) == pid
        and str(current.get("start_token") or "") == expected_start_token
        else ""
    )
    request = {
        "pid": pid,
        "start_token": expected_start_token,
        "requested_at": requested_at or utc_now_iso(),
        "updated_at": utc_now_iso(),
        "previous_identity": worker.get("worker_identity"),
        "desired_identity": desired_identity,
    }
    write_json_atomic(path, request, ensure_ascii=True)
    report.update(
        {
            "action": "graceful-replacement-requested",
            "request_path": str(path.relative_to(ROOT)),
            "request": request,
        }
    )
    return report


def engine_pump_replacement_request_age_seconds(
    request: object,
) -> float:
    if not isinstance(request, dict):
        return 0.0
    requested_at = parse_timestamp(str(request.get("requested_at") or ""))
    if requested_at is None:
        return 0.0
    return max(
        0.0,
        (datetime.now(timezone.utc) - requested_at).total_seconds(),
    )


def cancel_engine_pump_worker_replacement(worker: dict[str, object]) -> bool:
    path = engine_pump_replacement_request_path()
    request = read_json_dict(path)
    if not request:
        return False
    if (
        int(request.get("pid", 0) or 0) != int(worker.get("pid", 0) or 0)
        or str(request.get("start_token") or "")
        != str(worker.get("start_token") or "")
    ):
        return False
    path.unlink(missing_ok=True)
    append_daemon_runtime_event(
        ROOT,
        "engine_pump_worker_replacement_cancelled",
        {
            "pid": int(worker.get("pid", 0) or 0),
            "start_token": str(worker.get("start_token") or ""),
            "reason": "desired identity matches active worker again",
        },
    )
    return True


def engine_pump_worker_replacement_requested(
    *,
    pid: int,
    start_token: str,
) -> dict[str, object]:
    request = read_json_dict(engine_pump_replacement_request_path())
    if (
        int(request.get("pid", 0) or 0) != int(pid)
        or str(request.get("start_token") or "") != str(start_token)
    ):
        return {}
    return request


def prepare_engine_stop_drain(config: DaemonConfig) -> dict[str, object]:
    if flow_v3_executor_mode(config):
        try:
            runtime = FlowV3Runtime(
                flow_v3_runtime_config(ROOT, config.policy)
            )
            drain_work = runtime.has_drain_work()
        except (EngineRouteError, FlowV3StoreError, OSError, ValueError) as exc:
            return {
                "started": False,
                "reason": "flow-v3-stop-drain-unavailable",
                "error": f"{type(exc).__name__}: {exc}",
            }
        if not drain_work:
            return {
                "started": False,
                "reason": "no-flow-v3-drain-work",
                "status": runtime.store.status(),
            }
        result = maybe_start_flow_v3_worker(
            config,
            allow_stop_drain=True,
        )
        append_daemon_runtime_event(
            ROOT,
            "flow_v3_stop_drain_started",
            result,
        )
        return result
    operator_scope = {
        str(op)
        for op in (
            *tuple(getattr(config, "operators", ()) or ()),
            *tuple(getattr(config, "draining_operators", ()) or ()),
        )
        if str(op)
    }
    pump = EnginePump(
        ROOT,
        operator_scope=operator_scope if operator_scope else None,
    )
    status = pump.status()
    counts = status.get("state_counts", {})
    counts = counts if isinstance(counts, dict) else {}
    drain_work = engine_pump_status_has_drain_work(status)
    admission = EngineAdmissionStore(ROOT)
    current = admission.read()
    remote_known = bool(current.get("last_engine_snapshot"))
    if not drain_work:
        return {
            "started": False,
            "reason": (
                "no_scoped_engine_state" if operator_scope else "no_engine_state"
            ),
            "state_counts": counts,
            "operator_scope": sorted(operator_scope),
        }
    target = max(
        1,
        int(
            current.get("target_inflight")
            or config.policy.get("test_engine_target_inflight", 2)
            or 2
        ),
    )
    admission.configure(enabled=True, target_inflight=target, draining=True)
    result = maybe_start_engine_pump_worker(
        config,
        allow_stop_drain=True,
        force_capacity_sync=True,
    )
    append_daemon_runtime_event(
        ROOT,
        "engine_stop_drain_handoff",
        {
            "state_counts": counts,
            "remote_known": remote_known,
            "target_inflight": target,
            **result,
        },
    )
    return {"state_counts": counts, "remote_known": remote_known, **result}


def build_tick_plan(
    config: DaemonConfig,
    resource_manager: ResourceManager,
    *,
    timing_sink: dict[str, float] | None = None,
):
    # Reap finished action workers before taking the board/resource snapshot so
    # the same tick can hand the newly free device to the next queued submit.
    plan_stage_started = time.perf_counter()
    prune_execute_workers(ROOT, resource_manager)
    if timing_sink is not None:
        timing_sink["plan_prune_workers_seconds"] = time.perf_counter() - plan_stage_started

    plan_stage_started = time.perf_counter()
    state_reader = StateReader(ROOT, config)
    snapshot = state_reader.read()
    if timing_sink is not None:
        timing_sink["plan_board_seconds"] = time.perf_counter() - plan_stage_started

    plan_stage_started = time.perf_counter()
    gate_engine = GateEngine(config)
    raw_decisions = gate_engine.evaluate(snapshot.rows, snapshot.transport)
    # Schema repair must run before rollover suppression. Otherwise a malformed
    # latest case can be mistaken for permission to generate yet another case,
    # producing an invalid-case rollover loop instead of repairing in place.
    policy_decisions = enforce_casegen_evidence(ROOT, raw_decisions, config)
    policy_decisions = suppress_repeated_invalid_case_rollovers(ROOT, policy_decisions, config)
    if timing_sink is not None:
        timing_sink["plan_gate_policy_seconds"] = time.perf_counter() - plan_stage_started

    plan_stage_started = time.perf_counter()
    reconcile_native_relay_claim_observations(ROOT)
    reconcile_native_relay_completion_acks(ROOT)
    normalize_solver_ack_visibility(ROOT)
    sync_solver_ack_from_thread_observations(ROOT, config=config)
    sync_tester_ack_from_thread_observations(ROOT, config=config)
    reconcile_dead_trigger_owners(
        ROOT,
        retry_seconds=codex_cli_dead_trigger_retry_seconds(config),
    )
    policy_decisions = suppress_downstream_during_active_tester_casegen(
        ROOT,
        policy_decisions,
    )
    policy_decisions = fence_detached_operator_actions(config, policy_decisions)
    decisions = suppress_sent_solver_notifications(
        ROOT,
        policy_decisions,
        config=config,
        captured_at=snapshot.captured_at,
        retry_seconds=solver_failed_trigger_retry_seconds(config),
        codex_cli_dead_retry_seconds=codex_cli_dead_trigger_retry_seconds(config),
    )
    decisions = suppress_sent_tester_notifications(
        ROOT,
        decisions,
        config=config,
        captured_at=snapshot.captured_at,
        retry_seconds=tester_failed_trigger_retry_seconds(config),
        codex_cli_dead_retry_seconds=codex_cli_dead_trigger_retry_seconds(config),
    )
    decisions = reroute_legacy_payload_limit_to_engine(ROOT, decisions, config)
    decisions = maybe_allow_unmatched_gitpartner_recovery(ROOT, decisions, config)
    if timing_sink is not None:
        timing_sink["plan_trigger_state_seconds"] = time.perf_counter() - plan_stage_started

    plan_stage_started = time.perf_counter()
    traffic_balance = build_traffic_balance(ROOT, config)
    operator_priorities = load_operator_flow_priorities(ROOT, config)
    traffic_balance = {
        **traffic_balance,
        "operator_priorities": operator_priorities,
    }

    decisions = recover_active_gitpartner_worktree_blockers(
        ROOT,
        decisions,
        stale_lock_seconds=gitpartner_worktree_lock_stale_seconds(config),
    )

    decisions = suppress_relay_stall_recoveries(
        ROOT,
        decisions,
        transport=snapshot.transport,
        season=config.season,
        remote_root=config.remote_root,
        retry_seconds=relay_stall_failure_retry_seconds(config),
        probe_retry_seconds=relay_stall_probe_retry_seconds(config),
        probe_escalate_after_failures=relay_stall_probe_escalate_after_failures(config),
        probe_escalated_retry_seconds=relay_stall_probe_escalated_retry_seconds(config),
        recovery_escalate_after_failures=relay_stall_recovery_escalate_after_failures(config),
        recovery_escalated_retry_seconds=relay_stall_recovery_escalated_retry_seconds(config),
        cancel_after_failures=relay_stall_cancel_after_failures(config),
        cancel_wait_timeout_seconds=relay_stall_cancel_wait_timeout_seconds(config),
        head_blocker_retry_seconds=relay_stall_head_blocker_retry_seconds(config),
        heartbeat_timeout_seconds=gitpartner_heartbeat_timeout_seconds(config),
        traffic_balance=traffic_balance,
        debt_retry_seconds=traffic_balance_debt_recovery_retry_seconds(config),
        non_debt_head_blocker_cancel_after_failures=(
            traffic_balance_non_debt_head_blocker_cancel_after_failures(config)
        ),
    )
    decisions = recover_gitpartner_worktree_conflicts(
        ROOT,
        decisions,
        stale_lock_seconds=gitpartner_worktree_lock_stale_seconds(config),
    )
    decisions = suppress_failed_execute_backoff(
        ROOT,
        decisions,
        retry_seconds=execute_failed_retry_seconds(config),
        max_retry_seconds=execute_failed_max_retry_seconds(config),
        circuit_breaker_failures=execute_failed_circuit_breaker_failures(config),
        origin_visibility_retry_seconds=gitpartner_origin_visibility_retry_seconds(config),
        retry_failures_before=DAEMON_PROCESS_STARTED_AT,
    )
    decisions = suppress_infra_restore_backoff(
        ROOT,
        decisions,
        retry_seconds=infra_fail_restore_retry_seconds(config),
    )
    if timing_sink is not None:
        timing_sink["plan_recovery_balance_seconds"] = time.perf_counter() - plan_stage_started

    plan_stage_started = time.perf_counter()
    leases = tuple(resource_manager.prune_expired())
    decisions = suppress_resource_busy_actions(config, decisions, leases)
    decisions = suppress_successfully_executed_actions(ROOT, decisions)
    decisions = suppress_unpromoted_engine_dispatch_actions(ROOT, config, decisions)
    decisions = suppress_engine_managed_dispatch_actions(ROOT, config, decisions)

    scheduler = Scheduler(config)
    previous_scheduler_state = read_previous_scheduler_state(ROOT)
    plan = scheduler.plan(
        decisions,
        previous_scheduler_state,
        traffic_balance=traffic_balance,
        operator_priorities=operator_priorities,
    )
    action_liveness = read_selected_action_liveness(
        ROOT,
        plan.selected,
        snapshot.captured_at,
        int(config.policy.get("shadow_action_stagnant_seconds", 480) or 480),
    )
    if timing_sink is not None:
        timing_sink["plan_scheduler_seconds"] = time.perf_counter() - plan_stage_started
    return snapshot, policy_decisions, decisions, leases, plan, action_liveness


def suppress_successfully_executed_actions(
    root: Path,
    decisions: tuple[GateDecision, ...],
) -> tuple[GateDecision, ...]:
    """Prevent a converging board action from starving unrelated runnable work."""
    suppressed: list[GateDecision] = []
    non_executable = {
        ActionKind.HOLD,
        ActionKind.REVIEW_MANUAL,
        ActionKind.NOTIFY_SOLVER,
        ActionKind.NOTIFY_TESTER_CASEGEN,
    }
    for decision in decisions:
        if decision.action in non_executable or not decision.command:
            suppressed.append(decision)
            continue
        if not was_successfully_executed(root, decision.action_id):
            suppressed.append(decision)
            continue
        suppressed.append(
            GateDecision(
                row=decision.row,
                action=ActionKind.HOLD,
                reason=(
                    f"{decision.action.value} already completed successfully and its effect remains valid; "
                    "wait for this operator board to converge without starving other operators"
                ),
                command="",
                priority=0,
                blocks_operator=decision.blocks_operator,
            )
        )
    return tuple(suppressed)


def suppress_engine_managed_dispatch_actions(
    root: Path,
    config: DaemonConfig,
    decisions: tuple[GateDecision, ...],
    *,
    pump_state: dict[str, object] | None = None,
) -> tuple[GateDecision, ...]:
    """Let other operators advance while an engine-owned submit converges."""

    state = pump_state if pump_state is not None else EnginePump(root).read()
    runtime_gate = read_json_dict(
        root / "TestUtils" / "tester_daemon" / "ENGINE_RUNTIME_GATE.json"
    )
    remote_engine_generation = str(
        runtime_gate.get("remote_engine_generation") or ""
    )
    prewarm_rows: dict[tuple[str, str], dict[str, object]] = {}
    execution_profile = str(
        config.policy.get("test_engine_execution_profile") or CONSERVATIVE_PROFILE
    )
    if (
        engine_compatibility_enabled(config)
        and execution_profile == FUSED_SCALABLE_PROFILE
        and bool(config.policy.get("test_engine_case_cache_prewarm", True))
    ):
        prewarm_status = read_json_dict(
            root / "TestUtils" / "tester_daemon" / "case_cache_prewarm_status.json"
        )
        raw_rows = prewarm_status.get("operators", [])
        if isinstance(raw_rows, list):
            for raw in raw_rows:
                if not isinstance(raw, dict):
                    continue
                op = str(raw.get("op") or "")
                case_version = str(raw.get("case_version") or "")
                if op and case_version:
                    prewarm_rows[(op, case_version)] = raw
    suppressed: list[GateDecision] = []
    for decision in decisions:
        if decision.action != ActionKind.DISPATCH_SUBMIT:
            suppressed.append(decision)
            continue
        test_version = extract_test_version(decision.command)
        managed = bool(test_version) and workflow_attempt_is_managed(
            root,
            state,
            op=decision.row.op,
            test_version=test_version,
        )
        if managed:
            suppressed.append(
                GateDecision(
                    row=decision.row,
                    action=ActionKind.HOLD,
                    reason=(
                        "current queue attempt is already owned by the test engine; "
                        "wait for exactly-once result ingestion while other runnable operators advance"
                    ),
                    command="",
                    priority=0,
                    blocks_operator=decision.blocks_operator,
                )
            )
            continue
        retry_gate = evaluate_same_failure_retry_gate(
            config,
            state,
            op=decision.row.op,
            test_version=test_version,
            remote_engine_generation=remote_engine_generation,
        )
        if retry_gate["state"] == "hold":
            suppressed.append(
                GateDecision(
                    row=decision.row,
                    action=ActionKind.HOLD,
                    reason=(
                        "test engine deterministic-failure retry circuit is open "
                        f"for {decision.row.op}/{test_version}; "
                        f"retry_after_seconds={retry_gate.get('retry_after_seconds', 0)} "
                        "while other runnable operators advance"
                    ),
                    command="",
                    priority=0,
                    blocks_operator=decision.blocks_operator,
                )
            )
            continue
        case_match = re.search(
            r"--case-version\s+([A-Za-z0-9_.-]+)", decision.command
        )
        case_version = case_match.group(1) if case_match else ""
        prewarm = prewarm_rows.get((decision.row.op, case_version))
        prewarm_state = str((prewarm or {}).get("state") or "")
        if prewarm_state and prewarm_state != "ready":
            suppressed.append(
                GateDecision(
                    row=decision.row,
                    action=ActionKind.HOLD,
                    reason=(
                        f"strict case-cache prewarm is {prewarm_state} for "
                        f"{decision.row.op}/{case_version}; isolate this submit until "
                        "the hot cache is ready while other runnable operators advance"
                    ),
                    command="",
                    priority=0,
                    blocks_operator=decision.blocks_operator,
                )
            )
            continue
        suppressed.append(decision)
    return tuple(suppressed)


def suppress_unpromoted_engine_dispatch_actions(
    root: Path,
    config: DaemonConfig,
    decisions: tuple[GateDecision, ...],
) -> tuple[GateDecision, ...]:
    """Fail closed instead of silently falling back to a legacy test submit."""

    mode = str(config.policy.get("test_executor", "legacy") or "legacy")
    if mode != "engine-v1":
        return decisions
    promotion = promotion_gate_status(root, config.policy)
    if promotion.get("allowed"):
        return decisions
    raw_blockers = promotion.get("blockers", [])
    blockers = (
        ", ".join(str(item) for item in raw_blockers if str(item))
        if isinstance(raw_blockers, list)
        else str(raw_blockers or "")
    )
    detail = blockers or str(promotion.get("reason") or "promotion gate unavailable")
    suppressed: list[GateDecision] = []
    for decision in decisions:
        if decision.action != ActionKind.DISPATCH_SUBMIT:
            suppressed.append(decision)
            continue
        suppressed.append(
            GateDecision(
                row=decision.row,
                action=ActionKind.HOLD,
                reason=(
                    "engine-v1 promotion gate is not allowed; fail closed without "
                    f"legacy submit fallback while other runnable operators advance: {detail}"
                ),
                command="",
                priority=0,
                blocks_operator=decision.blocks_operator,
            )
        )
    return tuple(suppressed)


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


def fence_detached_operator_actions(
    config: DaemonConfig,
    decisions: tuple[GateDecision, ...],
) -> tuple[GateDecision, ...]:
    """Drain accepted work while preventing a detached plugin from expanding."""
    draining = set(config.draining_operators)
    if not draining:
        return decisions
    fenced: list[GateDecision] = []
    for decision in decisions:
        if decision.row.op not in draining or decision.action in DRAIN_ALLOWED_ACTIONS:
            fenced.append(decision)
            continue
        fenced.append(
            GateDecision(
                row=decision.row,
                action=ActionKind.HOLD,
                reason=(
                    "operator plugin detached from scheduling; observe the accepted queue/native turn "
                    "to terminal state, but do not create a new solver/tester/pending gate"
                ),
                command="",
                priority=0,
                blocks_operator=decision.blocks_operator,
            )
        )
    return tuple(fenced)


def should_drain_next_action(root: Path, selected: GateDecision | None, returncode: int) -> bool:
    if returncode != 0 or selected is None:
        return False
    result = read_json_dict(root / "TestUtils" / "tester_daemon" / "last_execute_result.json")
    outcome = str(result.get("outcome", "") or "")
    if outcome != "completed" or result.get("returncode") != 0:
        return False
    if bool(result.get("resource_bound")):
        return False
    if selected.action not in {
        ActionKind.PREPARE_SUBMIT,
        ActionKind.RESTORE_SUBMIT,
        ActionKind.REQUEUE_SUBMIT,
        ActionKind.REPAIR_QUEUE,
    }:
        return False
    return True


def suppress_repeated_invalid_case_rollovers(
    root: Path,
    decisions: tuple[GateDecision, ...],
    config,
) -> tuple[GateDecision, ...]:
    threshold = int(config.policy.get("max_consecutive_invalid_case_rollovers", 3) or 3)
    if threshold <= 0:
        return decisions
    suppressed: list[GateDecision] = []
    blocker_cache: dict[str, object] = {}
    for decision in decisions:
        if decision.action not in {ActionKind.NOTIFY_TESTER_CASEGEN, ActionKind.GENERATE_CASE_VERSION}:
            suppressed.append(decision)
            continue
        if decision.row.gate_stage == "casegen-evidence-incomplete":
            # This repairs the current case in place; it cannot extend the
            # repeated-rollover streak and is exactly how the streak is broken.
            suppressed.append(decision)
            continue
        blocker = blocker_cache.get(decision.row.op)
        if blocker is None:
            blocker = repeated_invalid_case_rollover_blocker(root, decision.row.op, threshold)
            blocker_cache[decision.row.op] = blocker
        if getattr(blocker, "blocked", False):
            suppressed.append(
                GateDecision(
                    row=decision.row,
                    action=ActionKind.HOLD,
                    reason=getattr(blocker, "reason", "repeated invalid-case/harness-contract rollover"),
                    command="",
                    priority=0,
                    blocks_operator=decision.blocks_operator,
                )
            )
        else:
            suppressed.append(decision)
    return tuple(suppressed)


def suppress_resource_busy_actions(
    config,
    decisions: tuple[GateDecision, ...],
    leases: tuple[dict[str, object], ...],
) -> tuple[GateDecision, ...]:
    if resource_capacity_available(config, leases):
        return decisions
    suppressed: list[GateDecision] = []
    for decision in decisions:
        if action_requires_resource(decision.action):
            suppressed.append(
                GateDecision(
                    row=decision.row,
                    action=ActionKind.HOLD,
                    reason="daemon resource busy; waiting for active action worker before running resource-bound action",
                    command="",
                    priority=0,
                    blocks_operator=decision.blocks_operator,
                )
            )
        else:
            suppressed.append(decision)
    return tuple(suppressed)


def resource_capacity_available(config, leases: tuple[dict[str, object], ...]) -> bool:
    resources = [resource for resource in config.resources if resource.get("enabled", True)]
    if not resources:
        resources = [{"id": config.transport or "default", "type": config.transport or "generic", "capacity": 1}]
    for resource in resources:
        resource_id = str(resource.get("id") or resource.get("type") or "resource")
        capacity = int(resource.get("capacity", 1) or 1)
        held = [lease for lease in leases if lease.get("resource_id") == resource_id]
        if len(held) < capacity:
            return True
    return False


def recover_gitpartner_worktree_conflicts(
    root: Path,
    decisions: tuple[GateDecision, ...],
    *,
    stale_lock_seconds: int,
) -> tuple[GateDecision, ...]:
    if gitpartner_transport_lock_active(root):
        return decisions
    rewritten: list[GateDecision] = []
    for decision in decisions:
        if decision.action not in {
            ActionKind.DISPATCH_SUBMIT,
            ActionKind.RECOVER_BLOCKED,
            ActionKind.HEARTBEAT_ACTIVE_REQUEST,
            ActionKind.CANCEL_STALLED_REQUEST,
        }:
            rewritten.append(decision)
            continue
        record = latest_failed_execute(root, decision.action_id)
        if not record or not failed_execute_mentions_gitpartner_worktree_conflict(record):
            rewritten.append(decision)
            continue
        if gitpartner_worktree_recovered_after(root, str(record.get("time", "") or "")):
            rewritten.append(decision)
            continue
        op = subprocess_safe_token(decision.row.op)
        action = subprocess_safe_token(decision.action.value)
        test_version = subprocess_safe_token(
            extract_row_test_version(decision.row)
            or extract_test_version(decision.command)
            or extract_test_version(decision.row.next_command)
            or "unknown-version"
        )
        command = (
            "python scripts\\next_workflow.py gitpartner-recover-worktree "
            f"--reason tester-daemon-gitpartner-worktree-conflict-{op}-{test_version}-{action} "
            f"--replace-stale-lock-after-seconds {max(0, int(stale_lock_seconds or 0))}"
        )
        rewritten.append(
            GateDecision(
                row=decision.row,
                action=ActionKind.RECOVER_GITPARTNER_WORKTREE,
                reason=(
                    "GitPartner local worktree is blocking trusted transport commands "
                    "(unmerged/autostash/index.lock); run harness-owned worktree recovery before "
                    "retrying submit/heartbeat/recover"
                ),
                command=command,
                priority=max(decision.priority, 98),
                blocks_operator=decision.blocks_operator,
            )
        )
    return tuple(rewritten)


def recover_active_gitpartner_worktree_blockers(
    root: Path,
    decisions: tuple[GateDecision, ...],
    *,
    stale_lock_seconds: int,
) -> tuple[GateDecision, ...]:
    if gitpartner_transport_lock_active(root):
        return decisions
    if any(decision.action == ActionKind.RECOVER_GITPARTNER_WORKTREE for decision in decisions):
        return decisions
    progress_actions = {
        ActionKind.DISPATCH_SUBMIT,
        ActionKind.RECOVER_BLOCKED,
        ActionKind.HEARTBEAT_ACTIVE_REQUEST,
        ActionKind.CANCEL_STALLED_REQUEST,
    }
    if not any(decision.action in progress_actions for decision in decisions):
        return decisions
    blockers = gitpartner_worktree_blocker_details(root)
    blockers = [
        blocker
        for blocker in blockers
        if str(blocker.get("kind", "") or "")
        in {"index_lock", "git_state", "unmerged_path", "status_failed"}
    ]
    if not blockers:
        return decisions
    detail = ",".join(str(item.get("kind", "")) for item in blockers[:4])
    rewritten: list[GateDecision] = []
    for decision in decisions:
        if decision.action not in progress_actions:
            rewritten.append(decision)
            continue
        op = subprocess_safe_token(decision.row.op)
        action = subprocess_safe_token(decision.action.value)
        test_version = subprocess_safe_token(
            extract_row_test_version(decision.row)
            or extract_test_version(decision.command)
            or extract_test_version(decision.row.next_command)
            or "unknown-version"
        )
        command = (
            "python scripts\\next_workflow.py gitpartner-recover-worktree "
            f"--reason tester-daemon-active-gitpartner-worktree-blocker-{op}-{test_version}-{action} "
            f"--replace-stale-lock-after-seconds {max(0, int(stale_lock_seconds or 0))}"
        )
        rewritten.append(
            GateDecision(
                row=decision.row,
                action=ActionKind.RECOVER_GITPARTNER_WORKTREE,
                reason=(
                    "GitPartner local worktree has active blockers before transport action "
                    f"({detail}); run harness-owned worktree recovery first"
                ),
                command=command,
                priority=max(decision.priority, 99),
                blocks_operator=decision.blocks_operator,
            )
        )
    return tuple(rewritten)


def gitpartner_transport_lock_active(root: Path) -> bool:
    lock_path = root / "TestUtils" / "tester_daemon" / "gitpartner_client_transport.lock"
    pid = read_lock_pid(lock_path)
    return pid > 0 and process_alive(pid)


def gitpartner_worktree_blocker_details(root: Path) -> list[dict[str, object]]:
    repo = root / "GitPartner"
    git_dir = repo / ".git"
    if not repo.exists() or not git_dir.exists():
        return []
    blockers: list[dict[str, object]] = []
    index_lock = git_dir / "index.lock"
    if index_lock.exists():
        try:
            age_seconds = max(0.0, time.time() - index_lock.stat().st_mtime)
        except OSError:
            age_seconds = 0.0
        blockers.append({"kind": "index_lock", "path": "GitPartner/.git/index.lock", "age_seconds": int(age_seconds)})
    for marker_name in ("rebase-merge", "rebase-apply", "MERGE_HEAD", "CHERRY_PICK_HEAD"):
        marker = git_dir / marker_name
        if marker.exists():
            blockers.append({"kind": "git_state", "path": f"GitPartner/.git/{marker_name}"})
    # Keep the resident fast path filesystem-only. Git for Windows allocates a
    # conhost for every invocation, including read-only status checks, so a
    # one-second daemon loop must not spawn `git status`. Index locks and
    # merge/rebase markers cover pre-dispatch recovery; rarer index-only
    # conflicts are diagnosed by the harness-owned action/recovery command.
    return blockers


def gitpartner_worktree_recovered_after(root: Path, failed_at_text: str) -> bool:
    failed_at = parse_timestamp(failed_at_text)
    if failed_at is None:
        return False
    path = root / "TestUtils" / "tester_daemon" / "gitpartner_worktree_recoveries.jsonl"
    if not path.exists():
        return False
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return False
    for line in reversed(lines[-200:]):
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict) or str(event.get("outcome", "") or "") not in {"recovered", "noop"}:
            continue
        event_time = parse_timestamp(str(event.get("time", "") or ""))
        if event_time is not None and event_time > failed_at:
            return True
    return False


def failed_execute_mentions_gitpartner_worktree_conflict(record: dict[str, object]) -> bool:
    text = "\n".join(
        str(record.get(field, "") or "")
        for field in ("stdout_tail", "stderr_tail", "error", "reason", "failure_kind")
    ).lower()
    if "push succeeded but origin/main did not expose input/job.json" in text:
        return False
    patterns = (
        "cannot save the current index state",
        "cannot autostash",
        "unmerged files",
        "needs merge",
        "you need to resolve your current index first",
        "another git process seems to be running",
        "index.lock",
        "gitpartner_worktree_recovery_pull_failed",
    )
    if not any(pattern in text for pattern in patterns):
        return False
    return "git" in text or "gitpartner" in text or "index" in text or "unmerged" in text


def failed_execute_mentions_gitpartner_origin_visibility_pending(record: dict[str, object]) -> bool:
    text = "\n".join(
        str(record.get(field, "") or "")
        for field in ("stdout_tail", "stderr_tail", "error", "reason", "failure_kind")
    ).lower()
    if "refusing --allow-unmatched-commit" in text or "current target worktree commit failed" in text:
        return False
    return "push succeeded but origin/main did not expose input/job.json" in text


def gitpartner_origin_visibility_request_id(record: dict[str, object]) -> str:
    text = "\n".join(
        str(record.get(field, "") or "")
        for field in ("stdout_tail", "stderr_tail", "error", "reason", "failure_kind")
    )
    matches = re.findall(r"request_id=([A-Za-z0-9_.-]+)", text)
    return matches[-1] if matches else ""


def gitpartner_target_worktree_has_uncommitted_paths(root: Path, request_id: str) -> bool:
    if not request_id:
        return False
    repo = root / "GitPartner"
    job = read_json_dict(repo / "input" / "job.json")
    if str(job.get("id", "") or "") != request_id:
        return False
    if not (repo / "input" / "payloads" / request_id).exists():
        return False
    cmd = [
        "git",
        "-c",
        f"safe.directory={repo.as_posix()}",
        "-c",
        "core.longpaths=true",
        "status",
        "--porcelain",
        "--",
        "input/job.json",
        f"input/payloads/{request_id}",
    ]
    try:
        proc = subprocess.run(
            cmd,
            cwd=repo,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=20,
            creationflags=process_creation_flags(),
            startupinfo=process_startupinfo(),
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return proc.returncode == 0 and bool((proc.stdout or "").strip())


def subprocess_safe_token(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_") or "unknown"


def suppress_failed_execute_backoff(
    root: Path,
    decisions: tuple[GateDecision, ...],
    retry_seconds: int,
    max_retry_seconds: int | None = None,
    circuit_breaker_failures: int = 5,
    origin_visibility_retry_seconds: int = MIN_GITPARTNER_ORIGIN_VISIBILITY_RETRY_SECONDS,
    retry_failures_before: datetime | None = None,
) -> tuple[GateDecision, ...]:
    if retry_seconds <= 0:
        return decisions
    suppressed: list[GateDecision] = []
    for decision in decisions:
        if decision.action in {
            ActionKind.HOLD,
            ActionKind.NOTIFY_SOLVER,
            ActionKind.NOTIFY_TESTER_CASEGEN,
            ActionKind.REVIEW_MANUAL,
        }:
            suppressed.append(decision)
            continue
        if decision.action in {ActionKind.RECOVER_BLOCKED, ActionKind.HEARTBEAT_ACTIVE_REQUEST}:
            latest = latest_failed_execute(root, decision.action_id)
            if latest and failed_execute_mentions_relay_stall(latest):
                suppressed.append(decision)
                continue
        latest = latest_failed_execute(root, decision.action_id)
        latest_failed_at = (
            parse_timestamp(str(latest.get("time", "") or ""))
            if latest
            else None
        )
        if (
            latest_failed_at is not None
            and retry_failures_before is not None
            and latest_failed_at < retry_failures_before
        ):
            # A resident-process generation gets one probe for failures left by
            # its predecessor. This lets a code/host recovery clear a stale
            # circuit without reopening an infinite retry loop: any failure
            # from this generation is timestamped after the boundary and is
            # subject to the normal backoff/circuit policy on the next tick.
            suppressed.append(decision)
            continue
        if latest and failed_action_inputs_changed_since(root, decision, latest):
            suppressed.append(decision)
            continue
        if latest and failed_execute_mentions_gitpartner_origin_visibility_pending(latest):
            request_id = gitpartner_origin_visibility_request_id(latest)
            if gitpartner_target_worktree_has_uncommitted_paths(root, request_id):
                suppressed.append(
                    GateDecision(
                        row=decision.row,
                        action=decision.action,
                        reason=(
                            decision.reason
                            + "; previous recovery pushed an unrelated HEAD while the target "
                            f"GitPartner input/payload is still local-uncommitted request_id={request_id}; "
                            "retry immediately so the harness can commit the target job before pushing"
                        ),
                        command=decision.command,
                        priority=decision.priority,
                        blocks_operator=decision.blocks_operator,
                    )
                )
                continue
            effective_origin_retry_seconds = max(
                MIN_GITPARTNER_ORIGIN_VISIBILITY_RETRY_SECONDS,
                int(origin_visibility_retry_seconds or 0),
            )
            backoff = failed_execute_backoff(
                root,
                decision.action_id,
                effective_origin_retry_seconds,
                max_retry_seconds=max_retry_seconds,
            )
            if not backoff:
                suppressed.append(decision)
                continue
            suppressed.append(
                GateDecision(
                    row=decision.row,
                    action=ActionKind.HOLD,
                    reason=(
                        "GitPartner submit push succeeded but origin/main has not exposed input/job.json yet; "
                        "waiting for origin visibility before re-running recover with escalating backoff "
                        f"retry_seconds={backoff.get('retry_seconds', effective_origin_retry_seconds)} "
                        f"failure_count={backoff.get('failure_count', '?')} "
                        f"remaining_seconds={backoff.get('remaining_seconds', '?')}"
                    ),
                    command="",
                    priority=0,
                    blocks_operator=decision.blocks_operator,
                )
            )
            continue
        failure_count = consecutive_failed_execute_count(root, decision.action_id)
        if circuit_breaker_failures > 0 and failure_count >= circuit_breaker_failures:
            suppressed.append(
                GateDecision(
                    row=decision.row,
                    action=ActionKind.HOLD,
                    reason=(
                        "deterministic execute failure circuit open; "
                        f"failure_count={failure_count} threshold={circuit_breaker_failures}; "
                        "retry is allowed after the command implementation or gate evidence changes"
                    ),
                    command="",
                    priority=0,
                    blocks_operator=decision.blocks_operator,
                )
            )
            continue
        backoff = failed_execute_backoff(root, decision.action_id, retry_seconds, max_retry_seconds=max_retry_seconds)
        if not backoff:
            suppressed.append(decision)
            continue
        suppressed.append(
            GateDecision(
                row=decision.row,
                action=ActionKind.HOLD,
                reason=(
                    "selected action is cooling down after failed execute "
                    f"returncode={backoff.get('returncode', '?')} "
                    f"failure_count={backoff.get('failure_count', '?')} "
                    f"remaining_seconds={backoff.get('remaining_seconds', '?')}"
                ),
                command="",
                priority=0,
                blocks_operator=decision.blocks_operator,
            )
        )
    return tuple(suppressed)


def failed_action_inputs_changed_since(
    root: Path,
    decision: GateDecision,
    failed_record: dict[str, object],
) -> bool:
    failed_at = parse_timestamp(str(failed_record.get("time", "") or ""))
    if failed_at is None:
        return False
    candidates: list[Path] = []
    script_match = re.search(r"(?:^|\s)python(?:w)?(?:\.exe)?\s+([^\s]+)", decision.command, re.IGNORECASE)
    if script_match:
        script = Path(script_match.group(1).strip('"\''))
        candidates.append(script if script.is_absolute() else root / script)
    test_version = extract_row_test_version(decision.row) or extract_test_version(decision.command)
    if decision.row.op and test_version:
        result_dir = root / "operators_testresult" / decision.row.op / test_version
        candidates.extend(
            [
                result_dir / "CASE_LIFETIME_IMPROVEMENT.md",
                result_dir / "RESULT.md",
            ]
        )
    failed_epoch = failed_at.timestamp()
    return any(path.exists() and path.stat().st_mtime > failed_epoch for path in candidates)


def fixed_failed_execute_backoff(root: Path, action_id: str, retry_seconds: int) -> dict[str, object] | None:
    record = latest_failed_execute(root, action_id)
    if not record:
        return None
    failed_at = parse_timestamp(str(record.get("time", "") or ""))
    if failed_at is None:
        return None
    now = datetime.now(failed_at.tzinfo)
    elapsed_seconds = max(0.0, (now - failed_at).total_seconds())
    if elapsed_seconds >= retry_seconds:
        return None
    payload = dict(record)
    payload["age_seconds"] = int(elapsed_seconds)
    payload["retry_seconds"] = retry_seconds
    payload["remaining_seconds"] = max(1, int(retry_seconds - elapsed_seconds + 0.999999))
    return payload


def suppress_relay_stall_recoveries(
    root: Path,
    decisions: tuple[GateDecision, ...],
    retry_seconds: int,
    transport: tuple[TransportObservation, ...] = (),
    probe_retry_seconds: int = 5,
    probe_escalate_after_failures: int = 3,
    probe_escalated_retry_seconds: int = 30,
    recovery_escalate_after_failures: int = 3,
    recovery_escalated_retry_seconds: int = 180,
    cancel_after_failures: int = 0,
    cancel_wait_timeout_seconds: int = 180,
    head_blocker_retry_seconds: int = 10,
    heartbeat_timeout_seconds: int = 60,
    season: str = "S5-910b",
    remote_root: str = "/opt/ascendop",
    traffic_balance: dict[str, object] | None = None,
    debt_retry_seconds: int = 1,
    non_debt_head_blocker_cancel_after_failures: int = 0,
) -> tuple[GateDecision, ...]:
    if retry_seconds <= 0:
        return decisions
    suppressed: list[GateDecision] = []
    debt = traffic_debt(traffic_balance)
    has_traffic_debt = any(int(value or 0) > 0 for value in debt.values())
    for decision in decisions:
        decision_debt = int(debt.get(decision.row.op, 0) or 0)
        head_blocker = transport_head_blocks_peer_work(decision, decisions)
        if relay_target_transport_fresh(decision, transport):
            suppressed.append(decision)
            continue
        effective_cancel_after_failures = relay_stall_effective_cancel_after_failures(
            cancel_after_failures=cancel_after_failures,
            head_blocker=head_blocker,
            decision_debt=decision_debt,
            has_traffic_debt=has_traffic_debt,
            non_debt_head_blocker_cancel_after_failures=non_debt_head_blocker_cancel_after_failures,
        )
        if decision.action == ActionKind.HEARTBEAT_ACTIVE_REQUEST:
            record = latest_failed_execute(root, decision.action_id)
            if not record or not failed_execute_mentions_relay_stall(record):
                suppressed.append(decision)
                continue
            probe_failure_count = recent_relay_stall_failure_count(root, decision.action_id)
            effective_probe_retry = escalated_retry_seconds(
                base_seconds=probe_retry_seconds,
                failure_count=probe_failure_count,
                escalate_after=probe_escalate_after_failures,
                escalated_seconds=probe_escalated_retry_seconds,
            )
            if head_blocker:
                effective_probe_retry = min(
                    effective_probe_retry,
                    max(1, int(head_blocker_retry_seconds or 1)),
                )
            if decision_debt > 0:
                effective_probe_retry = min(
                    effective_probe_retry,
                    max(1, int(debt_retry_seconds or 1)),
                )
            if relay_stall_should_cancel(
                root,
                decision,
                head_blocker=head_blocker,
                cancel_after_failures=effective_cancel_after_failures,
            ):
                suppressed.append(
                    relay_stall_cancel_decision(
                        decision,
                        season=decision.row.season or season,
                        remote_root=remote_root,
                        wait_timeout_seconds=cancel_wait_timeout_seconds,
                        heartbeat_timeout_seconds=heartbeat_timeout_seconds,
                    )
                )
                continue
            backoff = failed_execute_backoff(
                root,
                decision.action_id,
                max(1, effective_probe_retry),
                max_retry_seconds=max(1, effective_probe_retry),
            )
            if not backoff:
                suppressed.append(decision)
                continue
            suppressed.append(
                GateDecision(
                    row=decision.row,
                    action=ActionKind.HOLD,
                    reason=(
                        "GitPartner relay heartbeat still reports stalled; "
                        "transport health not proven; cooling down before the next lightweight heartbeat "
                        f"repeated_probe_failures={probe_failure_count} "
                        f"probe_retry_seconds={effective_probe_retry} "
                        f"remaining_seconds={backoff.get('remaining_seconds', '?')}"
                    ),
                    command="",
                    priority=0,
                    blocks_operator=decision.blocks_operator,
                )
            )
            continue
        if decision.action != ActionKind.RECOVER_BLOCKED:
            suppressed.append(decision)
            continue
        record = latest_failed_execute(root, decision.action_id)
        if not record or not failed_execute_mentions_relay_stall(record):
            suppressed.append(decision)
            continue
        if relay_probe_success_after(root, str(record.get("time", "") or "")):
            suppressed.append(decision)
            continue
        recovery_failure_count = recent_relay_stall_failure_count(root, decision.action_id)
        effective_recovery_retry = escalated_retry_seconds(
                base_seconds=retry_seconds,
                failure_count=recovery_failure_count,
                escalate_after=recovery_escalate_after_failures,
                escalated_seconds=recovery_escalated_retry_seconds,
            )
        if head_blocker:
            effective_recovery_retry = min(
                effective_recovery_retry,
                max(1, int(head_blocker_retry_seconds or 1)),
            )
        if decision_debt > 0:
            effective_recovery_retry = min(
                effective_recovery_retry,
                max(1, int(debt_retry_seconds or 1)),
            )
        if relay_stall_should_cancel(
            root,
            decision,
            head_blocker=head_blocker,
            cancel_after_failures=effective_cancel_after_failures,
        ):
            suppressed.append(
                relay_stall_cancel_decision(
                    decision,
                    season=decision.row.season or season,
                    remote_root=remote_root,
                    wait_timeout_seconds=cancel_wait_timeout_seconds,
                    heartbeat_timeout_seconds=heartbeat_timeout_seconds,
                )
            )
            continue
        backoff = failed_execute_backoff(
            root,
            decision.action_id,
            effective_recovery_retry,
            max_retry_seconds=effective_recovery_retry,
        )
        repeated_recovery_probe_due = (
            recovery_escalate_after_failures > 0
            and recovery_failure_count >= recovery_escalate_after_failures
        )
        if not backoff and not repeated_recovery_probe_due:
            suppressed.append(decision)
            continue
        if backoff is None:
            backoff = {"remaining_seconds": 0}
        if alternate_runnable_work(decision, decisions):
            suppressed.append(
                GateDecision(
                    row=decision.row,
                    action=ActionKind.HOLD,
                    reason=(
                        "GitPartner relay is still stalled after same-request recovery; "
                        "holding this recovery while another runnable harness action can keep the device busy "
                        f"repeated_recovery_failures={recovery_failure_count} "
                        f"recovery_retry_seconds={effective_recovery_retry} "
                        f"recovery_retry_remaining_seconds={backoff.get('remaining_seconds', '?')}"
                    ),
                    command="",
                    priority=0,
                    blocks_operator=decision.blocks_operator,
                )
            )
            continue
        probe = relay_stall_probe_decision(
            decision,
            remaining_seconds=int(backoff.get("remaining_seconds", 0) or 0),
            heartbeat_timeout_seconds=heartbeat_timeout_seconds,
        )
        probe_record = latest_failed_execute(root, probe.action_id)
        if probe_record and failed_execute_mentions_relay_stall(probe_record):
            probe_failure_count = recent_relay_stall_failure_count(root, probe.action_id)
            effective_probe_retry = escalated_retry_seconds(
                base_seconds=probe_retry_seconds,
                failure_count=probe_failure_count,
                escalate_after=probe_escalate_after_failures,
                escalated_seconds=probe_escalated_retry_seconds,
            )
            if head_blocker:
                effective_probe_retry = min(
                    effective_probe_retry,
                    max(1, int(head_blocker_retry_seconds or 1)),
                )
            if decision_debt > 0:
                effective_probe_retry = min(
                    effective_probe_retry,
                    max(1, int(debt_retry_seconds or 1)),
                )
            probe_backoff = failed_execute_backoff(
                root,
                probe.action_id,
                max(1, effective_probe_retry),
                max_retry_seconds=max(1, effective_probe_retry),
            )
            if probe_backoff:
                suppressed.append(
                    GateDecision(
                        row=decision.row,
                        action=ActionKind.HOLD,
                        reason=(
                            "GitPartner relay is still stalled after same-request recovery; "
                            "transport health not proven; lightweight heartbeat probe is cooling down "
                            f"repeated_probe_failures={probe_failure_count} "
                            f"probe_retry_seconds={effective_probe_retry} "
                            f"remaining_seconds={probe_backoff.get('remaining_seconds', '?')} "
                            f"repeated_recovery_failures={recovery_failure_count} "
                            f"recovery_retry_seconds={effective_recovery_retry} "
                            f"recovery_retry_remaining_seconds={backoff.get('remaining_seconds', '?')}"
                        ),
                        command="",
                        priority=0,
                        blocks_operator=decision.blocks_operator,
                    )
                )
                continue
        suppressed.append(probe)
    return tuple(suppressed)


def relay_target_transport_fresh(
    decision: GateDecision,
    transport: tuple[TransportObservation, ...],
) -> bool:
    if decision.action not in {
        ActionKind.RECOVER_BLOCKED,
        ActionKind.HEARTBEAT_ACTIVE_REQUEST,
        ActionKind.CANCEL_STALLED_REQUEST,
    }:
        return False
    test_version = (
        extract_row_test_version(decision.row)
        or extract_test_version(decision.command)
        or extract_test_version(decision.row.next_command)
        or ""
    )
    if not test_version:
        return False
    for observation in transport:
        if observation.op != decision.row.op or observation.test_version != test_version:
            continue
        remote = str(observation.remote_feedback_status or "").strip().lower()
        if remote == "fresh" and observation.stalled is False and not observation.terminal:
            return True
    return False


def relay_stall_should_cancel(
    root: Path,
    decision: GateDecision,
    *,
    head_blocker: bool,
    cancel_after_failures: int,
) -> bool:
    if cancel_after_failures <= 0:
        return False
    test_version = (
        extract_row_test_version(decision.row)
        or extract_test_version(decision.command)
        or extract_test_version(decision.row.next_command)
        or ""
    )
    if not test_version:
        return False
    failures = recent_relay_stall_request_failure_count(root, decision.row.op, test_version)
    return failures >= cancel_after_failures


def relay_stall_effective_cancel_after_failures(
    *,
    cancel_after_failures: int,
    head_blocker: bool,
    decision_debt: int,
    has_traffic_debt: bool,
    non_debt_head_blocker_cancel_after_failures: int,
) -> int:
    base = max(0, int(cancel_after_failures or 0))
    fast = max(0, int(non_debt_head_blocker_cancel_after_failures or 0))
    if not head_blocker or decision_debt > 0 or not has_traffic_debt or fast <= 0:
        return base
    if base <= 0:
        return fast
    return min(base, fast)


def relay_stall_cancel_decision(
    decision: GateDecision,
    *,
    season: str,
    remote_root: str,
    wait_timeout_seconds: int,
    heartbeat_timeout_seconds: int,
) -> GateDecision:
    test_version = (
        extract_row_test_version(decision.row)
        or extract_test_version(decision.command)
        or extract_test_version(decision.row.next_command)
        or "unknown-version"
    )
    command = (
        "python scripts\\next_workflow.py gitpartner-cancel-stalled "
        f"{decision.row.op} {test_version} --season {season} --remote-root {remote_root} "
        f"--wait-timeout-seconds {max(1, int(wait_timeout_seconds or 1))} "
        f"--heartbeat-timeout-seconds {max(1, int(heartbeat_timeout_seconds or 1))} "
        "--cancel-reason tester-daemon-relay-stall-head-blocker"
    )
    return GateDecision(
        row=decision.row,
        action=ActionKind.CANCEL_STALLED_REQUEST,
        reason=(
            "GitPartner relay request repeatedly stalled while blocking peer work or starving workflow; "
            "submit a harness-owned LAN cancel/cleanup request before requeueing other operators"
        ),
        command=command,
        priority=max(decision.priority, 96),
        blocks_operator=decision.blocks_operator,
    )


def transport_head_blocks_peer_work(
    decision: GateDecision,
    decisions: tuple[GateDecision, ...],
) -> bool:
    if decision.action not in {
        ActionKind.RECOVER_BLOCKED,
        ActionKind.HEARTBEAT_ACTIVE_REQUEST,
        ActionKind.CANCEL_STALLED_REQUEST,
    }:
        return False
    return any(
        other is not decision
        and other.row.op != decision.row.op
        and other.action == ActionKind.HOLD
        and other.blocks_operator == decision.row.op
        for other in decisions
    )


def escalated_retry_seconds(
    *,
    base_seconds: int,
    failure_count: int,
    escalate_after: int,
    escalated_seconds: int,
) -> int:
    base = max(1, int(base_seconds or 1))
    if escalate_after > 0 and failure_count >= escalate_after:
        return max(base, int(escalated_seconds or base))
    return base


def recent_relay_stall_failure_count(root: Path, action_id: str, limit: int = 200) -> int:
    history_path = root / "TestUtils" / "tester_daemon" / "execute_failures.jsonl"
    if not history_path.exists():
        return 0
    try:
        lines = history_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return 0
    count = 0
    for line in reversed(lines[-limit:]):
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(record, dict) or record.get("action_id") != action_id:
            continue
        if failed_execute_mentions_relay_stall(record):
            count += 1
    return count


def recent_relay_stall_request_failure_count(root: Path, op: str, test_version: str, limit: int = 200) -> int:
    history_path = root / "TestUtils" / "tester_daemon" / "execute_failures.jsonl"
    if not history_path.exists():
        return 0
    try:
        lines = history_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return 0
    marker = f"|{test_version}|"
    count = 0
    for line in reversed(lines[-limit:]):
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(record, dict):
            continue
        action_id = str(record.get("action_id", "") or "")
        if str(record.get("op", "") or "") != op or marker not in action_id:
            if count:
                break
            continue
        if failed_execute_mentions_relay_stall(record):
            count += 1
            continue
        if count:
            break
    return count


def alternate_runnable_work(decision: GateDecision, decisions: tuple[GateDecision, ...]) -> bool:
    progress_actions = {
        ActionKind.PREPARE_SUBMIT,
        ActionKind.DISPATCH_SUBMIT,
        ActionKind.RESTORE_SUBMIT,
        ActionKind.REPAIR_QUEUE,
        ActionKind.RECOVER_GITPARTNER_WORKTREE,
        ActionKind.CANCEL_STALLED_REQUEST,
    }
    for other in decisions:
        if other is decision or other.row.op == decision.row.op:
            continue
        if other.action in progress_actions:
            return True
    return False


def relay_stall_probe_decision(
    decision: GateDecision,
    *,
    remaining_seconds: int,
    heartbeat_timeout_seconds: int,
) -> GateDecision:
    test_version = (
        extract_row_test_version(decision.row)
        or extract_test_version(decision.command)
        or extract_test_version(decision.row.next_command)
        or "unknown-version"
    )
    command = (
        "python scripts\\next_workflow.py gitpartner-heartbeat "
        f"{decision.row.op} {test_version} --block-on-stall"
    )
    if heartbeat_timeout_seconds > 0:
        command += f" --heartbeat-timeout-seconds {heartbeat_timeout_seconds}"
    return GateDecision(
        row=decision.row,
        action=ActionKind.HEARTBEAT_ACTIVE_REQUEST,
        reason=(
            "GitPartner relay is still stalled after same-request recovery; "
            "run lightweight heartbeat/pullback while heavy recovery cools down "
            f"remaining_seconds={remaining_seconds}"
        ),
        command=command,
        priority=88,
        blocks_operator=decision.blocks_operator,
    )


def suppress_infra_restore_backoff(
    root: Path,
    decisions: tuple[GateDecision, ...],
    retry_seconds: int,
) -> tuple[GateDecision, ...]:
    if retry_seconds <= 0:
        return decisions
    suppressed: list[GateDecision] = []
    for decision in decisions:
        if decision.action != ActionKind.RESTORE_SUBMIT:
            suppressed.append(decision)
            continue
        details = infra_restore_failure_details(root, decision)
        if not details:
            suppressed.append(decision)
            continue
        age_seconds = int(details.get("age_seconds", 0) or 0)
        remaining = max(0, retry_seconds - age_seconds)
        if remaining <= 0:
            suppressed.append(decision)
            continue
        suppressed.append(
            GateDecision(
                row=decision.row,
                action=ActionKind.HOLD,
                reason=(
                    "GitPartner client return is still failing with "
                    f"{details.get('failure_kind')}; holding restore-submit instead of repeating the "
                    f"same INFRA_FAIL loop remaining_seconds={remaining}"
                ),
                command="",
                priority=0,
                blocks_operator=decision.blocks_operator,
            )
        )
    return tuple(suppressed)


def infra_restore_failure_details(root: Path, decision: GateDecision) -> dict[str, object]:
    version = extract_test_version(decision.command or decision.row.next_command)
    if not version:
        return {}
    output_root = root / "operators_testresult" / decision.row.op / version / "gitpartner_output"
    status_path = output_root / "client_output" / "status.json"
    status = read_json_dict(status_path)
    error = str(status.get("error", "") or "")
    failure_kind = ""
    if "client.log.part" in error and "No such file or directory" in error:
        failure_kind = "gitpartner_client_log_missing"
    else:
        status_path = output_root / "status.json"
        status = read_json_dict(status_path)
        error = str(status.get("error", "") or "")
        relay_diag = status.get("relay_diagnostic") if isinstance(status.get("relay_diagnostic"), dict) else {}
        relay_error = str(relay_diag.get("error", "") or "")
        text = f"{error}\n{relay_error}".lower()
        if "ssh:" in text and "connect to host" in text and "timed out" in text:
            failure_kind = "gitpartner_peer_ssh_timeout"
    if not failure_kind:
        return {}
    try:
        age_seconds = int(max(0.0, time.time() - status_path.stat().st_mtime))
    except OSError:
        age_seconds = 0
    return {
        "failure_kind": failure_kind,
        "status_path": str(status_path.relative_to(root)),
        "age_seconds": age_seconds,
    }


def failed_execute_mentions_relay_stall(record: dict[str, object]) -> bool:
    text = "\n".join(
        str(record.get(field, "") or "")
        for field in ("stdout_tail", "stderr_tail", "error", "reason")
    )
    return "TEST_GITPARTNER_RELAY_STALLED" in text or "GITPARTNER_RELAY_STALLED" in text


def relay_probe_success_after(root: Path, failed_at_text: str) -> bool:
    failed_at = parse_timestamp(failed_at_text)
    output_dir = root / "GitPartner" / "output"
    if failed_at is None or not output_dir.exists():
        return False
    for status_path in output_dir.glob("relay-*probe*/status.json"):
        status = read_json_dict(status_path)
        if str(status.get("state", "") or "") != "success":
            continue
        updated_at = parse_timestamp(str(status.get("finished_at") or status.get("updated_at") or ""))
        if updated_at is None:
            try:
                updated_at = datetime.fromtimestamp(status_path.stat().st_mtime, tz=failed_at.tzinfo)
            except OSError:
                continue
        if updated_at > failed_at:
            return True
    return False


def maybe_allow_unmatched_gitpartner_recovery(
    root: Path,
    decisions: tuple[GateDecision, ...],
    config: DaemonConfig,
) -> tuple[GateDecision, ...]:
    if not bool(config.policy.get("recover_blocked_allow_unmatched_after_head_drift", True)):
        return decisions
    adjusted: list[GateDecision] = []
    for decision in decisions:
        if decision.action != ActionKind.RECOVER_BLOCKED or "--allow-unmatched-commit" in decision.command:
            adjusted.append(decision)
            continue
        dry_run = gitpartner_recover_dry_run(root, decision.command)
        if not dry_run.get("head_drift"):
            adjusted.append(decision)
            continue
        request_id = str(dry_run.get("request_id", "") or "")
        input_job_id = gitpartner_input_job_id(root)
        if request_id and input_job_id and input_job_id != request_id:
            if gitpartner_output_is_terminal(root, input_job_id):
                test_version = extract_test_version(decision.command or decision.row.next_command)
                if test_version:
                    requeue_decision = GateDecision(
                        row=decision.row,
                        action=ActionKind.REQUEUE_SUBMIT,
                        reason=(
                            "GitPartner HEAD drift was detected and input/job.json points to a different "
                            f"terminal job (input_job_id={input_job_id}, target_request_id={request_id}); "
                            "requeue the same submit package so the target request can be regenerated"
                        ),
                        command=(
                            "python scripts\\next_workflow.py set-queue-status "
                            f"{decision.row.op} {test_version} queued "
                            "--claimed-by tester-daemon-requeue-after-gitpartner-sync-loss"
                        ),
                        priority=max(decision.priority, 85),
                        blocks_operator=decision.blocks_operator,
                    )
                    if was_successfully_executed(root, requeue_decision.action_id):
                        adjusted.append(
                            GateDecision(
                                row=decision.row,
                                action=decision.action,
                                reason=(
                                    decision.reason
                                    + "; GitPartner HEAD drift points to a different terminal job, "
                                    "but the same-submit requeue action already succeeded and the board "
                                    "has not advanced; using same-request --allow-unmatched-commit recovery"
                                ),
                                command=decision.command + " --allow-unmatched-commit",
                                priority=decision.priority,
                                blocks_operator=decision.blocks_operator,
                            )
                        )
                    else:
                        adjusted.append(requeue_decision)
                    continue
            adjusted.append(
                GateDecision(
                    row=decision.row,
                    action=ActionKind.HOLD,
                    reason=(
                        "GitPartner HEAD drift was detected, but input/job.json is not the target "
                        f"same-request job (input_job_id={input_job_id}, target_request_id={request_id}); "
                        "refusing --allow-unmatched-commit to avoid pushing an unrelated GitPartner job"
                    ),
                    command="",
                    priority=0,
                    blocks_operator=decision.blocks_operator,
                )
            )
            continue
        adjusted.append(
            GateDecision(
                row=decision.row,
                action=decision.action,
                reason=decision.reason
                + "; GitPartner HEAD drift detected by dry-run, using same-request --allow-unmatched-commit recovery",
                command=decision.command + " --allow-unmatched-commit",
                priority=decision.priority,
                blocks_operator=decision.blocks_operator,
            )
        )
    return tuple(adjusted)


def reroute_legacy_payload_limit_to_engine(
    root: Path,
    decisions: tuple[GateDecision, ...],
    config: DaemonConfig,
) -> tuple[GateDecision, ...]:
    """Recover an unstarted legacy submit through the archived Engine payload path."""

    if not engine_executor_enabled(config):
        return decisions
    adjusted: list[GateDecision] = []
    for decision in decisions:
        if decision.action != ActionKind.RECOVER_BLOCKED:
            adjusted.append(decision)
            continue
        test_version = (
            extract_row_test_version(decision.row)
            or extract_test_version(decision.command)
            or extract_test_version(decision.row.next_command)
        )
        if not test_version:
            adjusted.append(decision)
            continue
        evidence_path = (
            root
            / "TestUtils"
            / "submit"
            / decision.row.op
            / test_version
            / "TEST_GITPARTNER_SYNC_BLOCKED.md"
        )
        try:
            evidence = evidence_path.read_text(encoding="utf-8-sig")
        except OSError:
            adjusted.append(decision)
            continue
        if not any(
            marker in evidence
            for marker in (
                "payload file exceeds 1MB GitPartner limit",
                "legacy unarchived payload file exceeds",
            )
        ):
            adjusted.append(decision)
            continue
        adjusted.append(
            GateDecision(
                row=decision.row,
                action=ActionKind.REQUEUE_SUBMIT,
                reason=(
                    "legacy submit hit the per-file Git threshold before creating "
                    "the target request; requeue the immutable submit for the Engine "
                    "payload-archive path, whose chunking has no 1MB total-payload limit"
                ),
                command=(
                    "python scripts\\next_workflow.py set-queue-status "
                    f"{decision.row.op} {test_version} queued "
                    "--claimed-by tester-daemon-engine-requeue-after-legacy-payload-limit"
                ),
                priority=max(decision.priority, 96),
                blocks_operator=decision.blocks_operator,
            )
        )
    return tuple(adjusted)


def gitpartner_recover_dry_run(root: Path, command: str) -> dict[str, object]:
    try:
        argv = shlex.split(command.replace("\\", "/"), posix=False)
    except ValueError as exc:
        return {"error": str(exc)}
    argv = [arg.replace("scripts/next_workflow.py", "scripts\\next_workflow.py") for arg in argv]
    if "--dry-run" not in argv:
        argv.append("--dry-run")
    try:
        proc = subprocess.run(
            argv,
            cwd=root,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=30,
            creationflags=process_creation_flags(),
            startupinfo=process_startupinfo(),
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"error": str(exc)}
    output = "\n".join(part for part in [proc.stdout, proc.stderr] if part)
    request_id = dry_run_field(output, "request_id")
    expected = dry_run_field(output, "expected_subject")
    current = dry_run_field(output, "current_head_subject")
    return {
        "returncode": proc.returncode,
        "request_id": request_id,
        "expected_subject": expected,
        "current_head_subject": current,
        "head_drift": proc.returncode == 0 and bool(expected) and bool(current) and expected != current,
    }


def gitpartner_input_job_id(root: Path) -> str:
    data = read_json_dict(root / "GitPartner" / "input" / "job.json")
    return str(data.get("id", "") or "")


def gitpartner_output_is_terminal(root: Path, request_id: str) -> bool:
    if not request_id:
        return False
    terminal_states = {"success", "failed", "failure", "error", "cancelled", "canceled", "timeout", "timed_out"}
    output_root = root / "GitPartner" / "output"
    status_paths = [output_root / request_id / "status.json"]
    input_job = read_json_dict(root / "GitPartner" / "input" / "job.json")
    if str(input_job.get("id", "") or "") == request_id:
        output_subdir = str(input_job.get("output_subdir", "") or "").strip()
        if output_subdir:
            declared_root = (output_root / output_subdir).resolve()
            try:
                declared_root.relative_to(output_root.resolve())
            except ValueError:
                pass
            else:
                status_paths.append(declared_root / "status.json")
    for status_path in status_paths:
        status = read_json_dict(status_path)
        status_request_id = str(status.get("request_id", "") or "")
        if status_request_id and status_request_id != request_id:
            continue
        state = str(status.get("state", "") or "").strip().lower()
        if state in terminal_states:
            return True
    return False


def dry_run_field(output: str, key: str) -> str:
    prefix = f"{key}="
    for line in output.splitlines():
        if line.startswith(prefix):
            return line[len(prefix) :].strip()
    return ""


def suppress_sent_solver_notifications(
    root: Path,
    decisions: tuple[GateDecision, ...],
    config: DaemonConfig | None = None,
    captured_at: str = "",
    retry_seconds: int = 480,
    codex_cli_dead_retry_seconds: int = 10,
) -> tuple[GateDecision, ...]:
    ack_state = read_trigger_ack_state(root)
    sent = ack_state.get("sent", {}) if isinstance(ack_state.get("sent"), dict) else {}
    now_ts = parse_timestamp(captured_at)
    suppressed: list[GateDecision] = []
    for decision in decisions:
        if decision.action != ActionKind.NOTIFY_SOLVER:
            suppressed.append(decision)
            continue
        key = "|".join([decision.row.op, decision.row.gate_stage, decision.row.next_command])
        record = sent.get(key) if isinstance(sent, dict) else None
        status = ""
        if isinstance(record, dict):
            status = str(record.get("status", "") or "")
        if status in {
            "sent",
            "delivered",
            "acked",
            "active",
            "completed",
            "interrupted",
            "failed",
            "cancelled",
            "needs-native-delivery",
        }:
            sent_ts = parse_timestamp(str(record.get("updated_at", "") or ""))
            age_seconds = (
                max(0, int((now_ts - sent_ts).total_seconds()))
                if now_ts is not None and sent_ts is not None
                else 0
            )
            if status == "needs-native-delivery":
                suppressed.append(
                    GateDecision(
                        row=decision.row,
                        action=ActionKind.HOLD,
                        reason="solver trigger requires IDE-native delivery; waiting for main monitor native send",
                        command="",
                        priority=0,
                        blocks_operator=decision.blocks_operator,
                    )
                )
                continue

            failed_status = status in {"interrupted", "failed", "cancelled"}
            if failed_status:
                if str(record.get("thread_status_type", "") or "") == "systemError":
                    if config and not solver_session_replacement_allowed(config):
                        suppressed.append(
                            GateDecision(
                                row=decision.row,
                                action=ActionKind.HOLD,
                                reason=(
                                    "solver IDE thread is in systemError; same-session recovery "
                                    "required, not bridge retry"
                                ),
                                command="",
                                priority=0,
                                blocks_operator=decision.blocks_operator,
                            )
                        )
                        continue
                    suppressed.append(
                        GateDecision(
                            row=decision.row,
                            action=ActionKind.HOLD,
                            reason="solver IDE thread is in systemError; requires replacement session before retrying",
                            command="",
                            priority=0,
                            blocks_operator=decision.blocks_operator,
                        )
                    )
                    continue
                if str(record.get("failure_kind", "") or "") == "native_turn_no_agent_output":
                    no_agent_count = int(record.get("native_no_agent_output_count", 1) or 1)
                    if no_agent_count >= 2:
                        if config and not solver_session_replacement_allowed(config):
                            suppressed.append(
                                GateDecision(
                                    row=decision.row,
                                    action=ActionKind.HOLD,
                                    reason=(
                                        "solver session produced repeated completed user-only turns; "
                                        "same-session recovery required, not bridge retry"
                                    ),
                                    command="",
                                    priority=0,
                                    blocks_operator=decision.blocks_operator,
                                )
                            )
                            continue
                        suppressed.append(
                            GateDecision(
                                row=decision.row,
                                action=ActionKind.HOLD,
                                reason=(
                                    "solver session produced repeated completed user-only turns; "
                                    "requires replacement session before retrying"
                                ),
                                command="",
                                priority=0,
                                blocks_operator=decision.blocks_operator,
                            )
                        )
                        continue
                    if retry_seconds > 0 and age_seconds < retry_seconds:
                        suppressed.append(
                            GateDecision(
                                row=decision.row,
                                action=ActionKind.HOLD,
                                reason=(
                                    f"solver trigger completed without agent output {age_seconds}s ago; "
                                    f"waiting retry window {retry_seconds}s before one IDE-visible retry"
                                ),
                                command="",
                                priority=0,
                                blocks_operator=decision.blocks_operator,
                            )
                        )
                        continue
                    suppressed.append(
                        GateDecision(
                            row=decision.row,
                            action=ActionKind.NOTIFY_SOLVER,
                            reason=(
                                f"solver trigger completed without agent output {age_seconds}s ago; "
                                "retry once through IDE-visible delivery"
                            ),
                            command="",
                            priority=max(decision.priority, 45),
                            blocks_operator=decision.blocks_operator,
                        )
                    )
                    continue
                if remote_control_delivery_failed(record):
                    suppressed.append(
                        GateDecision(
                            row=decision.row,
                            action=ActionKind.NOTIFY_SOLVER,
                            reason=(
                                "solver trigger failed in daemon remoteControl path; "
                                "retry immediately through App-side IDE-native relay"
                            ),
                            command="",
                            priority=max(decision.priority, 45),
                            blocks_operator=decision.blocks_operator,
                        )
                    )
                    continue
                effective_retry_seconds = failed_trigger_retry_seconds(
                    record,
                    retry_seconds,
                    codex_cli_dead_retry_seconds,
                )
                if effective_retry_seconds > 0 and age_seconds < effective_retry_seconds:
                    suppressed.append(
                        GateDecision(
                            row=decision.row,
                            action=ActionKind.HOLD,
                            reason=(
                                f"solver trigger already {status} {age_seconds}s ago; "
                                f"waiting retry window {effective_retry_seconds}s"
                            ),
                            command="",
                            priority=0,
                            blocks_operator=decision.blocks_operator,
                        )
                    )
                    continue
                suppressed.append(
                    GateDecision(
                        row=decision.row,
                        action=ActionKind.NOTIFY_SOLVER,
                        reason=(
                            f"solver trigger already {status} {age_seconds}s ago; "
                            "delivery failed, retry notify"
                        ),
                        command="",
                        priority=max(decision.priority, 35),
                        blocks_operator=decision.blocks_operator,
                    )
                )
                continue
            stale_seconds = int(
                config.policy.get("solver_board_stale_seconds", 300) if config else 300
            )
            if status == "completed" and completed_trigger_stale(record, stale_seconds, captured_at):
                suppressed.append(
                    GateDecision(
                        row=decision.row,
                        action=ActionKind.NOTIFY_SOLVER,
                        reason=(
                            f"solver turn completed {age_seconds}s ago but the exact board gate did not advance; "
                            "retry current solver-owned gate"
                        ),
                        command="",
                        priority=max(decision.priority, 45),
                        blocks_operator=decision.blocks_operator,
                    )
                )
                continue
            suppressed.append(
                GateDecision(
                    row=decision.row,
                    action=ActionKind.HOLD,
                    reason=f"solver trigger already {status}; waiting for solver to advance board",
                    command="",
                    priority=0,
                    blocks_operator=decision.blocks_operator,
                )
            )
        else:
            suppressed.append(decision)
    return tuple(suppressed)


def suppress_sent_tester_notifications(
    root: Path,
    decisions: tuple[GateDecision, ...],
    config: DaemonConfig | None = None,
    captured_at: str = "",
    retry_seconds: int = 300,
    codex_cli_dead_retry_seconds: int = 10,
) -> tuple[GateDecision, ...]:
    ack_state = read_tester_trigger_ack_state(root)
    sent = ack_state.get("sent", {}) if isinstance(ack_state.get("sent"), dict) else {}
    now_ts = parse_timestamp(captured_at)
    suppressed: list[GateDecision] = []
    for decision in decisions:
        if decision.action != ActionKind.NOTIFY_TESTER_CASEGEN:
            suppressed.append(decision)
            continue
        key = "|".join([decision.row.op, decision.row.gate_stage, decision.row.next_command])
        covering_record = active_tester_casegen_covering_record(sent, decision.row.op, key)
        if covering_record is not None:
            suppressed.append(
                GateDecision(
                    row=decision.row,
                    action=ActionKind.HOLD,
                    reason=(
                        "tester casegen is already IDE-visible for this operator; "
                        "waiting for Tester case evidence instead of opening another trigger"
                    ),
                    command="",
                    priority=0,
                    blocks_operator=decision.blocks_operator,
                )
            )
            continue
        record = sent.get(key) if isinstance(sent, dict) else None
        status = str(record.get("status", "") or "") if isinstance(record, dict) else ""
        if status in {
            "sent",
            "delivered",
            "acked",
            "active",
            "completed",
            "needs-native-delivery",
            "interrupted",
            "failed",
            "cancelled",
        }:
            sent_ts = parse_timestamp(str(record.get("updated_at", "") or ""))
            age_seconds = (
                max(0, int((now_ts - sent_ts).total_seconds()))
                if now_ts is not None and sent_ts is not None
                else 0
            )
            if status in {"needs-native-delivery", "sent", "delivered", "acked", "active", "completed"}:
                stale_seconds = completed_gate_retry_seconds(config, "tester")
                if status == "completed" and completed_trigger_stale(record, stale_seconds, captured_at):
                    suppressed.append(
                        GateDecision(
                            row=decision.row,
                            action=ActionKind.NOTIFY_TESTER_CASEGEN,
                            reason=(
                                f"tester turn completed {age_seconds}s ago but the exact casegen gate did not advance; "
                                "retry current tester-owned gate"
                            ),
                            command=decision.command,
                            priority=max(decision.priority, 45),
                            blocks_operator=decision.blocks_operator,
                        )
                    )
                    continue
                suppressed.append(
                    GateDecision(
                        row=decision.row,
                        action=ActionKind.HOLD,
                        reason=f"tester casegen trigger already {status}; waiting for Tester case evidence",
                        command="",
                        priority=0,
                        blocks_operator=decision.blocks_operator,
                    )
                )
                continue
            if remote_control_delivery_failed(record):
                suppressed.append(
                    GateDecision(
                        row=decision.row,
                        action=ActionKind.NOTIFY_TESTER_CASEGEN,
                        reason=(
                            "tester casegen trigger failed in daemon remoteControl path; "
                            "retry immediately through App-side IDE-native relay"
                        ),
                        command=decision.command,
                        priority=max(decision.priority, 45),
                        blocks_operator=decision.blocks_operator,
                    )
                )
                continue
            effective_retry_seconds = failed_trigger_retry_seconds(
                record,
                retry_seconds,
                codex_cli_dead_retry_seconds,
            )
            if effective_retry_seconds > 0 and age_seconds < effective_retry_seconds:
                suppressed.append(
                    GateDecision(
                        row=decision.row,
                        action=ActionKind.HOLD,
                        reason=(
                            f"tester casegen trigger already {status} {age_seconds}s ago; "
                            f"waiting retry window {effective_retry_seconds}s"
                        ),
                        command="",
                        priority=0,
                        blocks_operator=decision.blocks_operator,
                    )
                )
                continue
            suppressed.append(
                GateDecision(
                    row=decision.row,
                    action=ActionKind.NOTIFY_TESTER_CASEGEN,
                    reason=f"tester casegen trigger already {status} {age_seconds}s ago; retry notify",
                    command=decision.command,
                    priority=max(decision.priority, 35),
                    blocks_operator=decision.blocks_operator,
                )
            )
        else:
            suppressed.append(decision)
    return tuple(suppressed)


def suppress_downstream_during_active_tester_casegen(
    root: Path,
    decisions: tuple[GateDecision, ...],
) -> tuple[GateDecision, ...]:
    """Keep ownership with Tester until its IDE-native casegen turn finishes.

    Case files are written incrementally. Their temporary presence must not let
    Solver or harness consume a half-validated case while the owning Tester turn
    is still active.
    """
    ack_state = read_tester_trigger_ack_state(root)
    sent = ack_state.get("sent", {}) if isinstance(ack_state.get("sent"), dict) else {}
    fenced: list[GateDecision] = []
    for decision in decisions:
        if decision.action in {ActionKind.NOTIFY_TESTER_CASEGEN, ActionKind.HOLD}:
            fenced.append(decision)
            continue
        current_key = "|".join(
            [decision.row.op, decision.row.gate_stage, decision.row.next_command]
        )
        covering_record = active_tester_casegen_covering_record(
            sent,
            decision.row.op,
            current_key,
        )
        if covering_record is None:
            fenced.append(decision)
            continue
        fenced.append(
            GateDecision(
                row=decision.row,
                action=ActionKind.HOLD,
                reason=(
                    "tester casegen turn is still active for this case; wait for the "
                    "IDE-native turn to finish before Solver or harness consumes evidence"
                ),
                command="",
                priority=0,
                blocks_operator=decision.blocks_operator,
            )
        )
    return tuple(fenced)


def remote_control_delivery_failed(record: object) -> bool:
    if not isinstance(record, dict):
        return False
    return str(record.get("delivery_retry_reason", "") or "") == "remote_control_not_ready"


def failed_trigger_retry_seconds(
    record: object,
    default_retry_seconds: int,
    codex_cli_dead_retry_seconds: int,
) -> int:
    if (
        isinstance(record, dict)
        and str(record.get("failure_kind", "") or "") in {
            "codex_cli_process_dead_no_thread_progress",
            "codex_app_restart_interrupted",
        }
    ):
        return int(codex_cli_dead_retry_seconds or 10)
    if (
        isinstance(record, dict)
        and codex_cli_resume_process_dead(record)
        and str(record.get("status", "") or "") in {"interrupted", "failed", "cancelled"}
    ):
        return int(codex_cli_dead_retry_seconds or 10)
    return int(default_retry_seconds or 0)


def active_tester_casegen_covering_record(
    sent: dict[str, object], op: str, current_key: str
) -> dict[str, object] | None:
    """Avoid duplicate evidence-incomplete prompts while a casegen turn is still active."""
    record = relay_tester_casegen_active_covering_record(sent, op, current_key)
    return record if isinstance(record, dict) else None


def extract_case_version_from_trigger_key(key: str) -> str:
    case_version = ""
    for token in str(key).replace("\\", " ").replace("/", " ").replace(";", " ").split():
        if token.startswith("case_v"):
            case_version = token.strip("`'\".,:;)")
    return case_version


def solver_failed_trigger_retry_seconds(config) -> int:
    value = config.policy.get("solver_failed_trigger_retry_seconds")
    if value is None:
        value = config.policy.get("solver_trigger_retry_seconds", 480)
    return int(value or 480)


def tester_failed_trigger_retry_seconds(config) -> int:
    value = config.policy.get("tester_failed_trigger_retry_seconds")
    if value is None:
        value = config.policy.get("tester_trigger_retry_seconds", 300)
    return int(value or 300)


def codex_cli_dead_trigger_retry_seconds(config) -> int:
    return int(config.policy.get("codex_cli_dead_trigger_retry_seconds", 10) or 10)


def execute_failed_retry_seconds(config) -> int:
    return int(config.policy.get("execute_failed_retry_seconds", 60) or 60)


def execute_failed_max_retry_seconds(config) -> int:
    return int(config.policy.get("execute_failed_max_retry_seconds", 600) or 600)


def execute_failed_circuit_breaker_failures(config) -> int:
    return int(config.policy.get("execute_failed_circuit_breaker_failures", 5) or 5)


def relay_stall_failure_retry_seconds(config) -> int:
    return int(config.policy.get("relay_stall_failure_retry_seconds", 300) or 300)


def relay_stall_probe_retry_seconds(config) -> int:
    return int(config.policy.get("relay_stall_probe_retry_seconds", 5) or 5)


def relay_stall_probe_escalate_after_failures(config) -> int:
    return int(config.policy.get("relay_stall_probe_escalate_after_failures", 3) or 3)


def relay_stall_probe_escalated_retry_seconds(config) -> int:
    return int(config.policy.get("relay_stall_probe_escalated_retry_seconds", 30) or 30)


def relay_stall_recovery_escalate_after_failures(config) -> int:
    return int(config.policy.get("relay_stall_recovery_escalate_after_failures", 3) or 3)


def relay_stall_recovery_escalated_retry_seconds(config) -> int:
    return int(config.policy.get("relay_stall_recovery_escalated_retry_seconds", 180) or 180)


def relay_stall_cancel_after_failures(config) -> int:
    return int(config.policy.get("relay_stall_cancel_after_failures", 8) or 0)


def relay_stall_cancel_wait_timeout_seconds(config) -> int:
    return int(config.policy.get("relay_stall_cancel_wait_timeout_seconds", 180) or 180)


def relay_stall_head_blocker_retry_seconds(config) -> int:
    return int(config.policy.get("relay_stall_head_blocker_retry_seconds", 10) or 10)


def traffic_balance_debt_recovery_retry_seconds(config) -> int:
    return int(config.policy.get("traffic_balance_debt_recovery_retry_seconds", 1) or 1)


def traffic_balance_non_debt_head_blocker_cancel_after_failures(config) -> int:
    return int(config.policy.get("traffic_balance_non_debt_head_blocker_cancel_after_failures", 2) or 0)


def gitpartner_heartbeat_timeout_seconds(config) -> int:
    return int(config.policy.get("gitpartner_heartbeat_timeout_seconds", 60) or 60)


def gitpartner_worktree_lock_stale_seconds(config) -> int:
    return int(config.policy.get("gitpartner_worktree_lock_stale_seconds", 120) or 120)


def gitpartner_origin_visibility_retry_seconds(config) -> int:
    return max(
        MIN_GITPARTNER_ORIGIN_VISIBILITY_RETRY_SECONDS,
        int(
            config.policy.get(
                "gitpartner_origin_visibility_retry_seconds",
                MIN_GITPARTNER_ORIGIN_VISIBILITY_RETRY_SECONDS,
            )
            or MIN_GITPARTNER_ORIGIN_VISIBILITY_RETRY_SECONDS
        ),
    )


def infra_fail_restore_retry_seconds(config) -> int:
    return int(config.policy.get("infra_fail_restore_retry_seconds", 300) or 300)


TERMINAL_SOLVER_TURN_STATUSES = {"completed", "interrupted", "failed", "cancelled"}
OBSERVABLE_SOLVER_TURN_STATUSES = TERMINAL_SOLVER_TURN_STATUSES | {"inProgress", "running", "queued"}
FAILED_SOLVER_TURN_STATUSES = {"interrupted", "failed", "cancelled"}
ACTIVE_SOLVER_ACK_STATUSES = {"sent", "delivered", "acked", "active"}
CONFIRMED_IDE_DELIVERIES = {
    "ide-native-relay",
    "codex-app-send-message-to-thread",
    "codex_app.send_message_to_thread",
}
CONFIRMED_IDE_VISIBILITIES = {
    "confirmed_by_native_relay",
    "live_proxy",
    "live_ws",
}
ORPHAN_ACTIVE_GRACE_SECONDS = 300
STORAGE_ONLY_VISIBILITIES = {
    "",
    "unverified_standalone_app_server",
    "not_visible",
    "storage_visible_assumed_for_monitoring",
    "storage_visible_not_ide_visible",
    "thread_read_visible_for_monitoring",
    "codex_cli_resume_session",
    "shared_rollout_storage",
}


def observed_ide_visibility_metadata(observed: dict[str, object]) -> dict[str, object]:
    visibility = str(observed.get("ide_panel_visibility", "") or "")
    confirmed = observed.get("ide_panel_visible") is True and visibility in CONFIRMED_IDE_VISIBILITIES
    storage_visible = bool(observed.get("storage_visible") or observed.get("native_visible"))
    if confirmed:
        label = visibility
    elif storage_visible:
        label = "storage_visible_not_ide_visible"
    else:
        label = visibility or "not_visible"
    return {"ide_panel_visible": confirmed, "ide_panel_visibility": label}


def reconcile_dead_trigger_owners(root: Path, retry_seconds: int = 10) -> list[dict[str, object]]:
    """Fail active relay acks whose local delivery owner disappeared.

    Board and archive state remain authoritative. This only releases a stale
    relay lease so the exact still-actionable gate may retry after a short
    grace period. A later terminal rollout observation can still reconcile the
    original turn to completed.
    """
    recovered: list[dict[str, object]] = []
    now = datetime.now(timezone.utc)
    retry_at = (now + timedelta(seconds=max(1, int(retry_seconds or 10)))).isoformat().replace(
        "+00:00", "Z"
    )
    for kind, read_state, ack in (
        ("solver", read_trigger_ack_state, ack_solver_trigger),
        ("tester", read_tester_trigger_ack_state, ack_tester_trigger),
    ):
        state = read_state(root)
        sent = state.get("sent", {}) if isinstance(state.get("sent"), dict) else {}
        for key, value in sent.items():
            if not isinstance(value, dict):
                continue
            status = normalized_trigger_status(value, str(value.get("status", "") or ""))
            if status not in ACTIVE_SOLVER_ACK_STATUSES:
                continue
            failure = dead_trigger_owner_failure(value)
            if failure is None:
                continue
            failure_kind, owner_pids = failure
            thread_id = str(value.get("thread_id", "") or "")
            if not thread_id:
                continue
            metadata = {
                "failure_kind": failure_kind,
                "error": (
                    "local relay delivery owner exited before a terminal turn was confirmed; "
                    "board state was not advanced"
                ),
                "delivery_retry_reason": "delivery_owner_process_dead",
                "delivery_retry_after": retry_at,
                "owner_lost_at": now.isoformat().replace("+00:00", "Z"),
                "owner_lost_pids": owner_pids,
                "completion_unconfirmed": True,
                "method": "owner-liveness-reconcile",
            }
            ack(root, str(key), thread_id, "failed", metadata)
            event = {
                "kind": kind,
                "key": str(key),
                "thread_id": thread_id,
                "failure_kind": failure_kind,
                "owner_pids": owner_pids,
                "retry_after": retry_at,
            }
            recovered.append(event)
            append_daemon_runtime_event(root, "relay_owner_lost", event)
    return recovered


def dead_trigger_owner_failure(record: dict[str, object]) -> tuple[str, list[int]] | None:
    delivery = str(record.get("delivery", "") or "")
    if delivery == "codex-cli-exec-resume":
        if "cli_returncode" in record:
            return None
        pid = record_pid(record, "cli_pid")
        if pid > 0 and not process_alive(pid):
            return "codex_cli_process_dead_no_thread_progress", [pid]
        return None
    if not delivery.startswith("app-server-"):
        return None
    dead_pids = [
        pid
        for pid in (
            record_pid(record, "delivery_worker_pid"),
            record_pid(record, "app_server_pid"),
        )
        if pid > 0 and not process_alive(pid)
    ]
    if not dead_pids:
        return None
    failure_kind = (
        "local_app_server_process_exit"
        if record_pid(record, "app_server_pid") in dead_pids
        else "local_delivery_worker_exit"
    )
    return failure_kind, sorted(set(dead_pids))


def record_pid(record: dict[str, object], field: str) -> int:
    try:
        return int(record.get(field, 0) or 0)
    except (TypeError, ValueError):
        return 0


def normalize_solver_ack_visibility(root: Path) -> None:
    state_dir = root / "TestUtils" / "tester_daemon"
    path = state_dir / "solver_trigger_ack_state.json"
    state_dir.mkdir(parents=True, exist_ok=True)
    with trigger_state_lock(state_dir, path.name):
        state = read_trigger_ack_state(root)
        sent = state.get("sent", {}) if isinstance(state.get("sent"), dict) else {}
        if not sent:
            return

        normalized: dict[str, object] = {}
        changed = False
        for key, record in sent.items():
            if not isinstance(record, dict):
                normalized[str(key)] = record
                continue
            next_record = dict(record)
            delivery = str(next_record.get("delivery", "") or "")
            visibility = str(next_record.get("ide_panel_visibility", "") or "")
            confirmed = next_record.get("ide_panel_visible") is True and (
                delivery in CONFIRMED_IDE_DELIVERIES
                or visibility in CONFIRMED_IDE_VISIBILITIES
            )
            storage_only = bool(
                next_record.get("storage_visible") or next_record.get("native_visible")
            )
            if confirmed:
                if next_record.get("ide_panel_visible") is not True:
                    next_record["ide_panel_visible"] = True
                    changed = True
                if visibility in STORAGE_ONLY_VISIBILITIES:
                    next_record["ide_panel_visibility"] = "confirmed_by_native_relay"
                    changed = True
            elif storage_only:
                if next_record.get("ide_panel_visible") is not False:
                    next_record["ide_panel_visible"] = False
                    changed = True
                if visibility in STORAGE_ONLY_VISIBILITIES:
                    next_record["ide_panel_visibility"] = "storage_visible_not_ide_visible"
                    changed = True
            normalized[str(key)] = next_record

        if changed:
            write_json_atomic(
                path, {"updated_at": utc_now_iso(), "sent": normalized}
            )


def observation_for_recorded_turn(
    observed: dict[str, object],
    turn_id: str,
) -> dict[str, object]:
    """Select an exact recent turn when a newer turn has overtaken the ack."""
    if not turn_id or str(observed.get("latest_turn_id", "") or "") == turn_id:
        return observed
    recent = observed.get("recent_turns", [])
    if not isinstance(recent, list):
        return observed
    for item in recent:
        if not isinstance(item, dict) or str(item.get("turn_id", "") or "") != turn_id:
            continue
        matched = dict(observed)
        matched.update(
            {
                "latest_turn_id": turn_id,
                "latest_turn_status": str(item.get("turn_status", "") or ""),
                "latest_started_at": item.get("started_at"),
                "latest_completed_at": item.get("completed_at"),
                "latest_duration_ms": item.get("duration_ms"),
                "latest_has_agent_output": item.get("has_agent_output"),
                "latest_user_only_turn": item.get("user_only_turn"),
                "latest_item_types": item.get("item_types", []),
                "latest_activity_at": (
                    item.get("last_activity_at")
                    or item.get("completed_at")
                    or item.get("started_at")
                ),
                "matched_recorded_turn": True,
            }
        )
        activity = observed_latest_activity_datetime(matched)
        if activity is not None:
            matched["idle_seconds"] = max(
                0,
                int((datetime.now(timezone.utc) - activity).total_seconds()),
            )
        return matched
    return observed


def sync_solver_ack_from_thread_observations(
    root: Path,
    *,
    config: DaemonConfig | None = None,
) -> None:
    """Treat IDE/thread reads as the authoritative status for delivered solver turns."""
    state_dir = root / "TestUtils" / "tester_daemon"
    observations = read_json_dict(state_dir / "solver_thread_observations.json")
    threads = observations.get("threads", [])
    if not isinstance(threads, list):
        return

    latest_by_thread = {
        str(item.get("thread_id", "") or ""): item
        for item in threads
        if isinstance(item, dict) and item.get("thread_id")
    }
    if not latest_by_thread:
        return

    recover_account_usage_cooldowns_from_peer_activity(root, config, observations)

    observed_at = str(observations.get("updated_at", "") or utc_now_iso())
    unchanged_observation_touches: dict[str, dict[str, object]] = {}
    ack_state = read_trigger_ack_state(root)
    sent = ack_state.get("sent", {}) if isinstance(ack_state.get("sent"), dict) else {}
    trigger_plan = read_json_dict(state_dir / "solver_trigger_plan.json")
    trigger_status_by_key = {
        str(item.get("key", "") or ""): str(item.get("status", "") or "")
        for item in trigger_plan.get("triggers", [])
        if isinstance(item, dict) and item.get("key")
    }
    stale_user_only_seconds = max(
        1,
        int(
            config.policy.get("solver_inprogress_no_agent_output_retry_seconds", 300)
            if config
            else 300
        ),
    )
    stale_agent_output_seconds = max(
        stale_user_only_seconds,
        int(
            config.policy.get("solver_inprogress_stale_activity_retry_seconds", 900)
            if config
            else 900
        ),
    )
    relay_outage, outage_stale_seconds = codex_app_outage_recovery_policy(
        state_dir, config
    )
    if relay_outage:
        stale_user_only_seconds = min(stale_user_only_seconds, outage_stale_seconds)
        stale_agent_output_seconds = min(stale_agent_output_seconds, outage_stale_seconds)
    for key, record in sent.items():
        if not isinstance(record, dict):
            continue
        current_status = str(record.get("status", "") or "")
        thread_id = str(record.get("thread_id", "") or "")
        turn_id = str(record.get("turn_id") or record.get("native_id") or "")
        if not thread_id:
            continue
        observed = latest_by_thread.get(thread_id)
        if not isinstance(observed, dict):
            continue
        observed = observation_for_recorded_turn(observed, turn_id)
        observed_turn_id = str(observed.get("latest_turn_id", "") or "")
        observed_status = str(observed.get("latest_turn_status", "") or "")
        if (
            str(key) in trigger_status_by_key
            and confirmed_native_turn_missing_after_grace(
                record, observed, observed_at=observed_at, config=config
            )
        ):
            ack_solver_trigger(
                root,
                str(key),
                thread_id,
                "failed",
                confirmed_native_turn_missing_metadata(
                    record,
                    retry_seconds=int(
                        config.policy.get("solver_failed_trigger_retry_seconds", 10)
                        if config
                        else 10
                    ),
                ),
            )
            continue
        if not observed_turn_id or observed_status not in OBSERVABLE_SOLVER_TURN_STATUSES:
            continue
        if (
            str(key) in trigger_status_by_key
            and native_delivery_reused_pre_delivery_turn(record, observed_turn_id)
        ):
            if current_status != "failed" or record.get("completion_unconfirmed") is not True:
                ack_solver_trigger(
                    root,
                    str(key),
                    thread_id,
                    "failed",
                    reused_pre_delivery_failure_metadata(record),
                )
            continue
        if (
            current_status in TERMINAL_SOLVER_TURN_STATUSES
            and str(key) not in trigger_status_by_key
            and (not turn_id or observed_turn_id != turn_id)
        ):
            continue
        if turn_id:
            if observed_turn_id != turn_id:
                recovery_status = trigger_status_by_key.get(str(key), "")
                same_session_recovery = (
                    current_status == "failed"
                    and recovery_status == "same-session-recovery-required"
                )
                if not same_session_recovery and not observed_activity_is_not_older_than_record(record, observed):
                    continue
        elif not record_can_sync_without_turn_id(record) or not observed_turn_matches_needs_native_delivery(record, observed):
            continue

        app_restart_terminal_fence = (
            current_status in TERMINAL_SOLVER_TURN_STATUSES
            and observed_status == "inProgress"
            and observed_turn_id == turn_id
            and str(record.get("failure_kind", "") or "")
            == "codex_app_restart_interrupted"
        )
        if app_restart_terminal_fence:
            continue

        stale_user_only_terminal_fence = (
            current_status in TERMINAL_SOLVER_TURN_STATUSES
            and observed_status in {"inProgress", "active", "running"}
            and observed_turn_id == turn_id
            and str(record.get("failure_kind", "") or "") == "native_turn_no_agent_output"
            and observed.get("latest_user_only_turn") is True
        )
        if stale_user_only_terminal_fence:
            continue

        stale_agent_output_terminal_fence = (
            current_status in TERMINAL_SOLVER_TURN_STATUSES
            and observed_status in {"inProgress", "active", "running"}
            and observed_turn_id == turn_id
            and str(record.get("failure_kind", "") or "")
            == "native_turn_stalled_no_activity"
            and observed.get("latest_has_agent_output") is True
        )
        if stale_agent_output_terminal_fence:
            continue

        local_owner_record = (
            str(record.get("delivery", "") or "").startswith("app-server-")
            or str(record.get("delivery", "") or "") == "codex-cli-exec-resume"
            or str(record.get("remote_control_status", "") or "") == "storage-visible-local-owner"
        )
        orphan_recovery = (
            current_status in TERMINAL_SOLVER_TURN_STATUSES
            and observed_status == "inProgress"
            and local_owner_record
            and str(record.get("failure_kind", "") or "") in OWNER_LOSS_FAILURE_KINDS
            and observed_turn_id == turn_id
            and observed_turn_recent(observed, ORPHAN_ACTIVE_GRACE_SECONDS)
        )
        if (
            current_status in TERMINAL_SOLVER_TURN_STATUSES
            and observed_status == "inProgress"
            and local_owner_record
            and not orphan_recovery
        ):
            continue

        if should_ignore_transient_terminal_observation(record, observed):
            continue
        current_native_status = str(record.get("native_status", "") or "")
        current_visible = bool(record.get("ide_panel_visible"))
        observed_idle_seconds = observed.get("idle_seconds")
        stale_user_only_active = (
            observed_status in {"inProgress", "active", "running"}
            and observed.get("latest_has_agent_output") is False
            and observed.get("latest_user_only_turn") is True
            and isinstance(observed_idle_seconds, (int, float))
            and float(observed_idle_seconds) >= stale_user_only_seconds
        )
        stale_agent_output_active = (
            str(key) in trigger_status_by_key
            and current_status in ACTIVE_SOLVER_ACK_STATUSES
            and observed_status in {"inProgress", "active", "running"}
            and observed.get("latest_has_agent_output") is True
            and isinstance(observed_idle_seconds, (int, float))
            and float(observed_idle_seconds) >= stale_agent_output_seconds
        )
        user_only_completed = (
            observed_status == "completed"
            and observed.get("latest_has_agent_output") is False
        ) or stale_user_only_active
        observation_touch = observed_ack_touch_metadata(observed, observed_at)
        if user_only_completed or stale_agent_output_active:
            next_status = "failed"
        elif observed_status in TERMINAL_SOLVER_TURN_STATUSES:
            next_status = observed_status
        else:
            next_status = "active"
        duplicate_user_only_turn = (
            user_only_completed
            and record.get("failure_kind") == "native_turn_no_agent_output"
            and (not turn_id or observed_turn_id == turn_id)
        )
        legacy_inflated_no_agent_count = (
            duplicate_user_only_turn
            and int(record.get("native_no_agent_output_count", 0) or 0) > 1
            and not record.get("native_no_agent_output_turn_ids")
        )
        if (
            current_status == next_status
            and current_native_status == observed_status
            and (current_visible or duplicate_user_only_turn)
            and (not turn_id or observed_turn_id == turn_id)
            and not (record.get("completion_unconfirmed") is True and not delivery_reconciliation_pending(record))
            and (
                not user_only_completed
                or record.get("failure_kind")
                in {"native_turn_no_agent_output", "codex_app_outage_stalled"}
            )
            and not legacy_inflated_no_agent_count
        ):
            unchanged_observation_touches[str(key)] = observation_touch
            continue

        owner_mode = str(record.get("owner_app_server_mode") or record.get("app_server_mode") or "")
        local_owner = (
            str(record.get("delivery", "") or "").startswith("app-server-")
            or str(record.get("remote_control_status", "") or "") == "storage-visible-local-owner"
        )
        metadata = {
            "turn_id": observed_turn_id,
            "turn_status": observed_status,
            "method": "thread-observation-sync",
            "wait_status": observed_status,
            "storage_visible": bool(observed.get("storage_visible", True)),
            "native_visible": bool(observed.get("native_visible", True)),
            **observed_ide_visibility_metadata(observed),
            "app_server_mode": owner_mode if local_owner and owner_mode else "thread-observation",
            "owner_app_server_mode": owner_mode if local_owner else "",
            "observation_mode": "thread-observation",
            "native_id": observed_turn_id,
            "native_status": observed_status,
            "native_startedAt": observed.get("latest_started_at"),
            "native_completedAt": observed.get("latest_completed_at"),
            "native_durationMs": observed.get("latest_duration_ms"),
            "latest_has_agent_output": observed.get("latest_has_agent_output"),
            "latest_user_only_turn": observed.get("latest_user_only_turn"),
            **observation_touch,
        }
        if record.get("completion_unconfirmed") is True and not delivery_reconciliation_pending(record):
            metadata["completion_unconfirmed"] = False
        if orphan_recovery or (
            bool(record.get("orphaned_delivery_owner"))
            and observed_status == "inProgress"
        ):
            metadata.update(orphan_observation_metadata(record, observed_at))
        if delivery_reconciliation_completed(record, turn_id, observed_turn_id, observed_status):
            metadata.update(delivery_reconciliation_completed_metadata(observed_at))
        if user_only_completed:
            if str(record.get("delivery_retry_reason", "") or "") == "account_usage_limit":
                metadata.update(
                    {
                        "failure_kind": "codex_usage_limit",
                        "error": str(record.get("error", "") or "Codex account usage limit"),
                    }
                )
            else:
                metadata.update(
                    {
                        "failure_kind": (
                            "codex_app_outage_stalled"
                            if relay_outage and stale_user_only_active
                            else "native_turn_no_agent_output"
                        ),
                        "error": (
                            "IDE-native delivery remained an in-progress user-only turn without agent output "
                            f"for {int(float(observed_idle_seconds))}s"
                            if stale_user_only_active
                            else "IDE-native delivery produced a completed turn without agent output"
                        ),
                    }
                )
        elif stale_agent_output_active:
            metadata.update(
                {
                    "failure_kind": (
                        "codex_app_outage_stalled"
                        if relay_outage
                        else "native_turn_stalled_no_activity"
                    ),
                    "failure_started_at": str(record.get("failure_started_at", "") or observed_at),
                    "error": (
                        "IDE-native solver turn produced agent output but made no rollout activity "
                        f"for {int(float(observed_idle_seconds))}s while the same live gate remained"
                    ),
                }
            )
        ack_solver_trigger(root, str(key), thread_id, next_status, metadata)
    if unchanged_observation_touches:
        touch_solver_ack_observations(root, unchanged_observation_touches)


def recover_account_usage_cooldowns_from_peer_activity(
    root: Path,
    config: DaemonConfig | None,
    solver_observations: dict[str, object] | None = None,
) -> list[dict[str, object]]:
    """Release stale account cooldowns after a newer same-model turn produces output."""
    if config is None:
        return []
    state_dir = root / "TestUtils" / "tester_daemon"
    observation_payloads = (
        ("solver", solver_observations or read_json_dict(state_dir / "solver_thread_observations.json")),
        ("tester", read_json_dict(state_dir / "tester_thread_observations.json")),
    )
    recovery_by_model: dict[str, dict[str, object]] = {}
    for default_role, payload in observation_payloads:
        threads = payload.get("threads", []) if isinstance(payload, dict) else []
        if not isinstance(threads, list):
            continue
        for observed in threads:
            if not isinstance(observed, dict):
                continue
            if observed.get("latest_has_agent_output") is not True:
                continue
            status = str(observed.get("latest_turn_status", "") or "")
            if status not in OBSERVABLE_SOLVER_TURN_STATUSES:
                continue
            op = str(observed.get("op", "") or "")
            role = str(observed.get("role", default_role) or default_role)
            session = operator_session(config, op)
            if session is None:
                continue
            model = session.solver_model if role == "solver" else session.tester_model
            started_at = observed_turn_started_datetime(observed)
            turn_id = str(observed.get("latest_turn_id", "") or "")
            if not model or started_at is None or not turn_id:
                continue
            previous = recovery_by_model.get(model)
            previous_started = previous.get("started_at") if isinstance(previous, dict) else None
            if isinstance(previous_started, datetime) and previous_started >= started_at:
                continue
            recovery_by_model[model] = {
                "source_op": op,
                "source_role": role,
                "source_thread_id": str(observed.get("thread_id", "") or ""),
                "source_turn_id": turn_id,
                "source_turn_status": status,
                "started_at": started_at,
            }
    if not recovery_by_model:
        return []

    recovered: list[dict[str, object]] = []
    for role, plan_name, read_ack, ack in (
        ("solver", "solver_trigger_plan.json", read_trigger_ack_state, ack_solver_trigger),
        ("tester", "tester_trigger_plan.json", read_tester_trigger_ack_state, ack_tester_trigger),
    ):
        plan = read_json_dict(state_dir / plan_name)
        triggers = plan.get("triggers", []) if isinstance(plan.get("triggers"), list) else []
        trigger_by_key = {
            str(item.get("key", "") or ""): item
            for item in triggers
            if isinstance(item, dict) and item.get("key")
        }
        ack_state = read_ack(root)
        sent = ack_state.get("sent", {}) if isinstance(ack_state.get("sent"), dict) else {}
        for key, trigger in trigger_by_key.items():
            record = sent.get(key) if isinstance(sent, dict) else None
            if not isinstance(record, dict):
                continue
            if str(record.get("delivery_retry_reason", "") or "") != "account_usage_limit":
                continue
            op = str(trigger.get("op", "") or "")
            session = operator_session(config, op)
            if session is None:
                continue
            model = session.solver_model if role == "solver" else session.tester_model
            evidence = recovery_by_model.get(model)
            if not isinstance(evidence, dict):
                continue
            failure_at = account_usage_failure_datetime(record)
            source_started = evidence.get("started_at")
            if failure_at is None or not isinstance(source_started, datetime) or source_started <= failure_at:
                continue
            metadata = {
                "delivery_required": True,
                "delivery_retry_reason": "",
                "delivery_retry_after": "",
                "error": "",
                "ide_panel_visible": False,
                "ide_panel_visibility": "quota-recovery-needs-native-delivery",
                "quota_recovery_model": model,
                "quota_recovery_source_op": evidence.get("source_op", ""),
                "quota_recovery_source_role": evidence.get("source_role", ""),
                "quota_recovery_source_thread_id": evidence.get("source_thread_id", ""),
                "quota_recovery_source_turn_id": evidence.get("source_turn_id", ""),
                "quota_recovery_source_started_at": source_started.isoformat(),
                "quota_recovered_at": utc_now_iso(),
            }
            ack(root, key, str(trigger.get("thread_id", "") or record.get("thread_id", "")), "needs-native-delivery", metadata)
            event = {
                "role": role,
                "op": op,
                "key": key,
                **{k: v for k, v in metadata.items() if k.startswith("quota_recovery")},
            }
            append_daemon_runtime_event(root, "account_usage_cooldown_recovered", event)
            recovered.append(event)
    return recovered


def account_usage_failure_datetime(record: dict[str, object]) -> datetime | None:
    for field in ("failure_started_at", "updated_at", "time"):
        parsed = parse_timestamp(str(record.get(field, "") or ""))
        if parsed is not None:
            return parsed
    return None


def observed_turn_started_datetime(observed: dict[str, object]) -> datetime | None:
    for field in ("latest_started_at", "latest_startedAt"):
        value = observed.get(field)
        if value in (None, ""):
            continue
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (TypeError, ValueError, OSError):
            parsed = parse_timestamp(str(value))
            if parsed is not None:
                return parsed
    return None


def sync_tester_ack_from_thread_observations(
    root: Path,
    *,
    config: DaemonConfig | None = None,
) -> None:
    """Treat IDE/thread reads as authoritative for delivered Tester casegen turns."""
    state_dir = root / "TestUtils" / "tester_daemon"
    observations = read_json_dict(state_dir / "tester_thread_observations.json")
    if not observations:
        observations = read_json_dict(state_dir / "solver_thread_observations.json")
    threads = observations.get("threads", [])
    if not isinstance(threads, list):
        return
    latest_by_thread = {
        str(item.get("thread_id", "") or ""): item
        for item in threads
        if isinstance(item, dict)
        and item.get("thread_id")
        and str(item.get("role", "tester") or "tester") == "tester"
    }
    if not latest_by_thread:
        return

    observed_at = str(observations.get("updated_at", "") or utc_now_iso())
    relay_outage, outage_stale_seconds = codex_app_outage_recovery_policy(
        state_dir, config
    )
    ack_state = read_tester_trigger_ack_state(root)
    sent = ack_state.get("sent", {}) if isinstance(ack_state.get("sent"), dict) else {}
    trigger_plan = read_json_dict(state_dir / "tester_trigger_plan.json")
    trigger_status_by_key = {
        str(item.get("key", "") or ""): str(item.get("status", "") or "")
        for item in trigger_plan.get("triggers", [])
        if isinstance(item, dict) and item.get("key")
    }
    sync_keys = set(trigger_status_by_key)
    worker_state = read_json_dict(state_dir / "solver_trigger_bridge_workers.json")
    workers = worker_state.get("workers", {}) if isinstance(worker_state.get("workers"), dict) else {}
    sync_keys.update(
        str(key)
        for key, worker in workers.items()
        if isinstance(worker, dict) and str(worker.get("kind", "") or "") == "tester"
    )
    for key, candidate in sent.items():
        if not isinstance(candidate, dict):
            continue
        candidate_status = normalized_trigger_status(
            candidate,
            str(candidate.get("status", "") or ""),
        )
        if candidate_status not in {"sent", "delivered", "acked", "active"}:
            continue
        candidate_thread = str(candidate.get("thread_id", "") or "")
        candidate_turn = str(candidate.get("turn_id") or candidate.get("native_id") or "")
        observed = latest_by_thread.get(candidate_thread)
        if isinstance(observed, dict):
            observed = observation_for_recorded_turn(observed, candidate_turn)
        if (
            candidate_turn
            and isinstance(observed, dict)
            and str(observed.get("latest_turn_id", "") or "") == candidate_turn
        ):
            sync_keys.add(str(key))
    if not sync_keys:
        recoverable_keys: list[str] = []
        for key, candidate in sent.items():
            if not isinstance(candidate, dict):
                continue
            candidate_status = normalized_trigger_status(
                candidate,
                str(candidate.get("status", "") or ""),
            )
            candidate_thread = str(candidate.get("thread_id", "") or "")
            candidate_turn = str(candidate.get("turn_id") or candidate.get("native_id") or "")
            observed = latest_by_thread.get(candidate_thread)
            if isinstance(observed, dict):
                observed = observation_for_recorded_turn(observed, candidate_turn)
            owner_loss_turn_survived = (
                candidate_status in TERMINAL_SOLVER_TURN_STATUSES
                and str(candidate.get("failure_kind", "") or "") in OWNER_LOSS_FAILURE_KINDS
                and isinstance(observed, dict)
                and str(observed.get("latest_turn_id", "") or "") == candidate_turn
                and str(observed.get("latest_turn_status", "") or "") == "inProgress"
                and observed_turn_recent(observed, ORPHAN_ACTIVE_GRACE_SECONDS)
            )
            if candidate_status in {"sent", "delivered", "acked", "active"} or owner_loss_turn_survived:
                recoverable_keys.append(str(key))
        if len(recoverable_keys) == 1:
            sync_keys.add(recoverable_keys[0])
    active_record_count_by_thread: dict[str, int] = {}
    for candidate in sent.values():
        if not isinstance(candidate, dict):
            continue
        candidate_status = normalized_trigger_status(
            candidate,
            str(candidate.get("status", "") or ""),
        )
        if candidate_status not in {"sent", "delivered", "acked", "active"}:
            continue
        candidate_thread = str(candidate.get("thread_id", "") or "")
        if candidate_thread:
            active_record_count_by_thread[candidate_thread] = (
                active_record_count_by_thread.get(candidate_thread, 0) + 1
            )
    for key, record in sent.items():
        if not isinstance(record, dict):
            continue
        if str(key) not in sync_keys:
            continue
        current_status = str(record.get("status", "") or "")
        if current_status not in {"sent", "delivered", "acked", "active", "completed", "interrupted", "failed", "cancelled"}:
            continue
        thread_id = str(record.get("thread_id", "") or "")
        turn_id = str(record.get("turn_id") or record.get("native_id") or "")
        if not thread_id:
            continue
        observed = latest_by_thread.get(thread_id)
        if not isinstance(observed, dict):
            continue
        observed = observation_for_recorded_turn(observed, turn_id)
        observed_turn_id = str(observed.get("latest_turn_id", "") or "")
        observed_status = str(observed.get("latest_turn_status", "") or "")
        if (
            str(key) in trigger_status_by_key
            and confirmed_native_turn_missing_after_grace(
                record, observed, observed_at=observed_at, config=config
            )
        ):
            ack_tester_trigger(
                root,
                str(key),
                thread_id,
                "failed",
                confirmed_native_turn_missing_metadata(
                    record,
                    retry_seconds=int(
                        config.policy.get("tester_failed_trigger_retry_seconds", 10)
                        if config
                        else 10
                    ),
                ),
            )
            continue
        if not observed_turn_id or observed_status not in OBSERVABLE_SOLVER_TURN_STATUSES:
            continue
        if (
            str(key) in trigger_status_by_key
            and native_delivery_reused_pre_delivery_turn(record, observed_turn_id)
        ):
            if current_status != "failed" or record.get("completion_unconfirmed") is not True:
                ack_tester_trigger(
                    root,
                    str(key),
                    thread_id,
                    "failed",
                    reused_pre_delivery_failure_metadata(record),
                )
            continue
        if turn_id and observed_turn_id != turn_id:
            synthetic_cli_remap = (
                turn_id.startswith("codex-cli-")
                and str(record.get("delivery", "") or "") == "codex-cli-exec-resume"
                and current_status in {"sent", "delivered", "acked", "active"}
                and (
                    str(key) in trigger_status_by_key
                    or active_record_count_by_thread.get(thread_id, 0) == 1
                )
                and observed_activity_is_not_older_than_record(record, observed)
            )
            same_session_recovery = (
                current_status == "failed"
                and trigger_status_by_key.get(str(key), "") == "same-session-recovery-required"
                and observed_activity_is_not_older_than_record(record, observed)
            )
            if not synthetic_cli_remap and not same_session_recovery:
                continue
        if not turn_id:
            if str(key) not in trigger_status_by_key:
                continue
            if not record_can_sync_without_turn_id(record) or not observed_turn_matches_needs_native_delivery(record, observed):
                continue

        app_restart_terminal_fence = (
            current_status in TERMINAL_SOLVER_TURN_STATUSES
            and observed_status == "inProgress"
            and observed_turn_id == turn_id
            and str(record.get("failure_kind", "") or "")
            == "codex_app_restart_interrupted"
        )
        if app_restart_terminal_fence:
            continue
        local_owner_record = (
            str(record.get("delivery", "") or "").startswith("app-server-")
            or str(record.get("delivery", "") or "") == "codex-cli-exec-resume"
            or str(record.get("remote_control_status", "") or "") == "storage-visible-local-owner"
        )
        orphan_recovery = (
            current_status in TERMINAL_SOLVER_TURN_STATUSES
            and observed_status == "inProgress"
            and local_owner_record
            and str(record.get("failure_kind", "") or "") in OWNER_LOSS_FAILURE_KINDS
            and observed_turn_id == turn_id
            and observed_turn_recent(observed, ORPHAN_ACTIVE_GRACE_SECONDS)
        )
        if (
            current_status in TERMINAL_SOLVER_TURN_STATUSES
            and observed_status == "inProgress"
            and local_owner_record
            and not orphan_recovery
        ):
            continue
        if should_ignore_transient_terminal_observation(record, observed):
            continue
        user_only_completed = (
            observed_status == "completed"
            and observed.get("latest_has_agent_output") is False
        )
        observed_idle_seconds = observed.get("idle_seconds")
        outage_stalled = (
            relay_outage
            and observed_status in {"inProgress", "active", "running"}
            and isinstance(observed_idle_seconds, (int, float))
            and float(observed_idle_seconds) >= outage_stale_seconds
        )
        if user_only_completed or outage_stalled:
            next_status = "failed"
        elif observed_status in TERMINAL_SOLVER_TURN_STATUSES:
            next_status = observed_status
        else:
            next_status = "active"
        current_native_status = str(record.get("native_status", "") or "")
        if (
            current_status == next_status
            and current_native_status == observed_status
            and bool(record.get("ide_panel_visible"))
            and (not turn_id or observed_turn_id == turn_id)
            and not (record.get("completion_unconfirmed") is True and not delivery_reconciliation_pending(record))
            and (not user_only_completed or record.get("failure_kind") == "native_turn_no_agent_output")
        ):
            continue
        owner_mode = str(record.get("owner_app_server_mode") or record.get("app_server_mode") or "")
        local_owner = (
            str(record.get("delivery", "") or "").startswith("app-server-")
            or str(record.get("remote_control_status", "") or "") == "storage-visible-local-owner"
        )
        metadata = {
            "turn_id": observed_turn_id,
            "turn_status": observed_status,
            "method": "thread-observation-sync",
            "wait_status": observed_status,
            "storage_visible": bool(observed.get("storage_visible", True)),
            "native_visible": bool(observed.get("native_visible", True)),
            **observed_ide_visibility_metadata(observed),
            "app_server_mode": owner_mode if local_owner and owner_mode else "thread-observation",
            "owner_app_server_mode": owner_mode if local_owner else "",
            "observation_mode": "thread-observation",
            "native_id": observed_turn_id,
            "native_status": observed_status,
            "native_startedAt": observed.get("latest_started_at"),
            "native_completedAt": observed.get("latest_completed_at"),
            "native_durationMs": observed.get("latest_duration_ms"),
            "latest_has_agent_output": observed.get("latest_has_agent_output"),
            "latest_user_only_turn": observed.get("latest_user_only_turn"),
            **observed_ack_touch_metadata(observed, observed_at),
        }
        if record.get("completion_unconfirmed") is True and not delivery_reconciliation_pending(record):
            metadata["completion_unconfirmed"] = False
        if orphan_recovery or (
            bool(record.get("orphaned_delivery_owner"))
            and observed_status == "inProgress"
        ):
            metadata.update(orphan_observation_metadata(record, observed_at))
        if delivery_reconciliation_completed(record, turn_id, observed_turn_id, observed_status):
            metadata.update(delivery_reconciliation_completed_metadata(observed_at))
        if user_only_completed or outage_stalled:
            metadata.update(
                {
                    "failure_kind": (
                        "codex_app_outage_stalled"
                        if outage_stalled
                        else "native_turn_no_agent_output"
                    ),
                    "error": (
                        "Codex app relay is unavailable and the Tester turn has no activity "
                        f"for {int(float(observed_idle_seconds))}s"
                        if outage_stalled
                        else "IDE-native delivery produced a completed turn without agent output"
                    ),
                }
            )
        ack_tester_trigger(root, str(key), thread_id, next_status, metadata)


def codex_app_outage_recovery_policy(
    state_dir: Path,
    config: DaemonConfig | None,
) -> tuple[bool, int]:
    relay_status = read_app_side_relay_status(state_dir)
    relay_outage = bool(relay_status) and not bool(relay_status.get("fresh"))
    stale_seconds = max(
        30,
        int(
            config.policy.get("codex_app_outage_stale_turn_seconds", 90)
            if config
            else 90
        ),
    )
    return relay_outage, stale_seconds


def observed_ack_touch_metadata(observed: dict[str, object], observed_at: str) -> dict[str, object]:
    metadata: dict[str, object] = {
        "last_observed_at": observed_at,
        "last_observed_turn_id": observed.get("latest_turn_id"),
        "last_observed_turn_status": observed.get("latest_turn_status"),
        "last_observed_has_agent_output": observed.get("latest_has_agent_output"),
        "last_observed_user_only_turn": observed.get("latest_user_only_turn"),
    }
    for source_key, target_key in (
        ("thread_status_type", "thread_status_type"),
        ("thread_status_error", "thread_status_error"),
        ("thread_updatedAt", "thread_updatedAt"),
        ("thread_updated_at", "thread_updated_at"),
    ):
        value = observed.get(source_key)
        if value not in (None, ""):
            metadata[target_key] = value
    return metadata


def touch_solver_ack_observations(root: Path, touches: dict[str, dict[str, object]]) -> None:
    ack_state = read_trigger_ack_state(root)
    sent = ack_state.get("sent", {}) if isinstance(ack_state.get("sent"), dict) else {}
    if not isinstance(sent, dict):
        return
    changed = False
    next_sent = dict(sent)
    for key, touch in touches.items():
        record = next_sent.get(key)
        if not isinstance(record, dict):
            continue
        next_record = dict(record)
        for field, value in touch.items():
            if value in (None, ""):
                continue
            if next_record.get(field) != value:
                next_record[field] = value
                changed = True
        next_sent[key] = next_record
    if not changed:
        return
    state_dir = root / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    state = dict(ack_state)
    state["updated_at"] = utc_now_iso()
    state["sent"] = next_sent
    (state_dir / "solver_trigger_ack_state.json").write_text(
        json.dumps(state, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def observed_turn_matches_needs_native_delivery(record: dict[str, object], observed: dict[str, object]) -> bool:
    record_status = str(record.get("status", "") or "")
    unconfirmed_native_delivery = (
        record_status in {"failed", "delivered"}
        and record.get("completion_unconfirmed") is True
        and str(record.get("delivery", "") or "")
        == "codex-app-send-message-to-thread"
        and str(record.get("error", "") or "")
        in {
            "post-delivery-proof-unavailable",
            "post-delivery-turn-already-existed",
            "native-turn-id-did-not-change",
        }
    )
    weak_native_confirmation = (
        record_status == "failed" and unconfirmed_native_delivery
    )
    if record_status not in {
        "needs-native-delivery",
        "sent",
        "delivered",
        "acked",
        "active",
    } and not weak_native_confirmation:
        return False
    if unconfirmed_native_delivery:
        observed_turn_id = str(observed.get("latest_turn_id", "") or "")
        pre_delivery_turn_id = str(record.get("pre_delivery_turn_id", "") or "")
        if not observed_turn_id or observed_turn_id == pre_delivery_turn_id:
            return False
    record_time_value = record.get("updated_at", "")
    if unconfirmed_native_delivery:
        # Claim completion can be delayed well after send_message created the
        # turn. The claim timestamp is the earliest safe delivery boundary.
        record_time_value = (
            record.get("delivery_started_at")
            or record.get("claimed_at")
            or ""
        )
    record_time = parse_timestamp(str(record_time_value or ""))
    observed_time = observed_activity_datetime(observed)
    if record_time is None:
        return True
    if observed_time is None:
        return False
    return observed_time.timestamp() + 1.0 >= record_time.timestamp()


def observed_activity_is_not_older_than_record(record: dict[str, object], observed: dict[str, object]) -> bool:
    record_time = parse_timestamp(str(record.get("updated_at", "") or ""))
    observed_time = observed_activity_datetime(observed)
    if record_time is None:
        return True
    if observed_time is None:
        return False
    return observed_time.timestamp() + 1.0 >= record_time.timestamp()


def record_can_sync_without_turn_id(record: dict[str, object]) -> bool:
    if str(record.get("status", "") or "") == "needs-native-delivery":
        return False
    for key in ("delivery", "method", "app_server_mode", "native_status", "turn_status"):
        if str(record.get(key, "") or ""):
            return True
    return False


def observed_activity_datetime(observed: dict[str, object]):
    for key in ("latest_completed_at", "latest_completedAt", "latest_started_at", "latest_startedAt", "latest_activity_at"):
        value = observed.get(key)
        if value in (None, ""):
            continue
        try:
            return datetime.fromtimestamp(float(value), tz=record_timezone())
        except (TypeError, ValueError, OSError):
            pass
        parsed = parse_timestamp(str(value))
        if parsed is not None:
            return parsed
    return None


def observed_turn_recent(observed: dict[str, object], max_age_seconds: int) -> bool:
    activity = observed_latest_activity_datetime(observed)
    if activity is None:
        return False
    return (datetime.now(timezone.utc) - activity).total_seconds() <= max(1, max_age_seconds)


def observed_latest_activity_datetime(observed: dict[str, object]):
    for key in (
        "latest_activity_at",
        "latest_completed_at",
        "latest_completedAt",
        "latest_started_at",
        "latest_startedAt",
    ):
        value = observed.get(key)
        if value in (None, ""):
            continue
        try:
            return datetime.fromtimestamp(float(value), tz=record_timezone())
        except (TypeError, ValueError, OSError):
            pass
        parsed = parse_timestamp(str(value))
        if parsed is not None:
            return parsed
    return None


def orphan_observation_metadata(
    previous: dict[str, object], observed_at: str
) -> dict[str, object]:
    """Fence an ownerless native turn until its exact turn becomes terminal."""
    return {
        "delivery": "thread-observation",
        "owner_app_server_mode": "",
        "delivery_worker_pid": 0,
        "app_server_pid": 0,
        "cli_pid": 0,
        "orphaned_delivery_owner": True,
        "orphaned_owner_failure_kind": str(previous.get("failure_kind", "") or ""),
        "orphaned_observed_at": observed_at,
        "completion_unconfirmed": True,
        "control_plane_unavailable": True,
        "ide_panel_visible": False,
        "ide_panel_visibility": "rollout_visible_control_plane_unavailable",
        "method": "thread-observation-orphan-reconcile",
    }


def delivery_reconciliation_completed(
    previous: dict[str, object],
    expected_turn_id: str,
    observed_turn_id: str,
    observed_status: str,
) -> bool:
    return (
        previous.get("completion_unconfirmed") is True
        and bool(expected_turn_id)
        and observed_turn_id == expected_turn_id
        and observed_status in TERMINAL_SOLVER_TURN_STATUSES
    )


def native_delivery_reused_pre_delivery_turn(
    record: dict[str, object], observed_turn_id: str
) -> bool:
    """Fence an observation that only rediscovered the pre-send baseline turn."""

    pre_delivery_turn_id = str(record.get("pre_delivery_turn_id", "") or "")
    if not pre_delivery_turn_id or observed_turn_id != pre_delivery_turn_id:
        return False
    if (
        str(record.get("status", "") or "") == "delivered"
        and record.get("completion_unconfirmed") is True
    ):
        retry_after = parse_timestamp(
            str(record.get("delivery_retry_after", "") or "")
        )
        if retry_after is not None and datetime.now(timezone.utc) < retry_after:
            return False
    return (
        record.get("completion_unconfirmed") is True
        or record.get("duplicate_delivery_suppressed") is True
        or str(record.get("failure_kind", "") or "") == "native_turn_not_created"
        or str(record.get("error", "") or "")
        in {
            "post-delivery-proof-unavailable",
            "post-delivery-turn-already-existed",
            "native-turn-id-did-not-change",
        }
    )


def confirmed_native_turn_missing_after_grace(
    record: dict[str, object],
    observed: dict[str, object],
    *,
    observed_at: str = "",
    config: DaemonConfig | None = None,
) -> bool:
    """Detect a relay-confirmed turn that never became durable/observable."""

    if str(record.get("delivery", "") or "") != "codex-app-send-message-to-thread":
        return False
    if str(record.get("status", "") or "") not in ACTIVE_SOLVER_ACK_STATUSES:
        return False
    expected_turn_id = str(record.get("turn_id") or record.get("native_id") or "")
    pre_delivery_turn_id = str(record.get("pre_delivery_turn_id", "") or "")
    if not expected_turn_id or expected_turn_id == pre_delivery_turn_id:
        return False

    observed_turn_id = str(observed.get("latest_turn_id", "") or "")
    observed_status = str(observed.get("latest_turn_status", "") or "")
    if observed_turn_id == expected_turn_id:
        return False
    if observed_turn_id:
        if not pre_delivery_turn_id or observed_turn_id != pre_delivery_turn_id:
            return False
        if observed_status not in TERMINAL_SOLVER_TURN_STATUSES:
            return False

    delivered_at = parse_timestamp(
        str(record.get("delivery_started_at") or record.get("updated_at") or "")
    )
    if delivered_at is None:
        return False
    observation_snapshot_at = parse_timestamp(observed_at)
    if observation_snapshot_at is None or observation_snapshot_at < delivered_at:
        return False
    grace_seconds = max(
        5,
        int(
            config.policy.get(
                "native_relay_confirmed_turn_visibility_grace_seconds", 45
            )
            if config
            else 45
        ),
    )
    return (datetime.now(timezone.utc) - delivered_at).total_seconds() >= grace_seconds


def confirmed_native_turn_missing_metadata(
    record: dict[str, object], *, retry_seconds: int
) -> dict[str, object]:
    metadata = dict(record)
    for field in (
        "time",
        "event",
        "key",
        "thread_id",
        "status",
        "updated_at",
        "ide_panel_visible",
        "ide_panel_visibility",
    ):
        metadata.pop(field, None)
    metadata.update(
        {
            "completion_unconfirmed": True,
            "control_plane_unavailable": False,
            "orphaned_delivery_owner": False,
            "ide_panel_visible": False,
            "ide_panel_visibility": "confirmed_turn_not_observable",
            "failure_kind": "native_turn_not_created",
            "error": "confirmed-native-turn-not-visible-after-grace",
            "delivery_retry_reason": "native_delivery_unconfirmed",
            "delivery_retry_after": (
                datetime.now(timezone.utc)
                + timedelta(seconds=max(1, int(retry_seconds)))
            )
            .replace(microsecond=0)
            .isoformat(),
            "delivery_confirmation_invalidated_at": utc_now_iso(),
        }
    )
    return metadata


def reused_pre_delivery_failure_metadata(
    record: dict[str, object],
) -> dict[str, object]:
    metadata = dict(record)
    for field in ("time", "event", "key", "thread_id", "status", "updated_at"):
        metadata.pop(field, None)
    metadata.update(
        {
            "completion_unconfirmed": True,
            "duplicate_delivery_suppressed": True,
            "failure_kind": "native_turn_not_created",
            "error": str(
                record.get("error", "") or "post-delivery-turn-already-existed"
            ),
            "delivery_retry_reason": "native_delivery_unconfirmed",
            "delivery_retry_after": str(record.get("delivery_retry_after", "") or "")
            or (
                datetime.now(timezone.utc) + timedelta(seconds=120)
            ).replace(microsecond=0).isoformat(),
        }
    )
    return metadata


def delivery_reconciliation_completed_metadata(observed_at: str) -> dict[str, object]:
    return {
        "completion_unconfirmed": False,
        "control_plane_unavailable": False,
        "orphaned_delivery_owner": False,
        "delivery_reconciled_at": observed_at,
        "delivery_reconciliation": "exact-native-turn-terminal",
    }


def record_timezone():
    from datetime import timezone

    return timezone.utc


def should_ignore_transient_terminal_observation(record: dict[str, object], observed: dict[str, object]) -> bool:
    """Ignore stdio/app-server poll glitches that briefly label an active turn interrupted.

    A real interrupted/failed turn should have a completion timestamp or duration. The Codex app
    native path can show a turn as in-progress while the standalone app-server reader reports an
    incomplete terminal status; do not let that downgrade a freshly IDE-delivered trigger.
    """
    observed_status = str(observed.get("latest_turn_status", "") or "")
    if observed_status not in FAILED_SOLVER_TURN_STATUSES:
        return False
    record_turn_id = str(record.get("turn_id") or record.get("native_id") or "")
    observed_turn_id = str(observed.get("latest_turn_id", "") or "")
    if record_turn_id and observed_turn_id and record_turn_id != observed_turn_id:
        return False
    if (
        delivery_reconciliation_pending(record)
        and record_turn_id
        and observed_turn_id == record_turn_id
        and str(observed.get("observation_source", "") or "") == "app-server-thread-read"
    ):
        return False
    if is_transient_native_poll_failure(record):
        return True
    current_status = str(record.get("status", "") or "")
    delivery = str(record.get("delivery", "") or "")
    record_visibility = str(record.get("ide_panel_visibility", "") or "")
    observed_visibility = str(observed.get("ide_panel_visibility", "") or "")
    confirmed = (
        delivery in CONFIRMED_IDE_DELIVERIES
        or record_visibility in CONFIRMED_IDE_VISIBILITIES
        or observed_visibility in CONFIRMED_IDE_VISIBILITIES
    )
    if current_status not in ACTIVE_SOLVER_ACK_STATUSES or not confirmed:
        return False
    completed_at = observed.get("latest_completed_at")
    duration_ms = observed.get("latest_duration_ms")
    if completed_at in (None, "") and duration_ms in (None, ""):
        return True
    if str(observed.get("thread_status_type", "") or "") == "idle":
        activity = observed_activity_datetime(observed)
        if activity is None:
            return False
        age_seconds = (datetime.now(activity.tzinfo or record_timezone()) - activity).total_seconds()
        if age_seconds >= 10:
            return False
    return False


def read_previous_scheduler_state(root: Path) -> dict[str, object]:
    state_dir = root / "TestUtils" / "tester_daemon"
    history = read_json_dict(state_dir / "scheduler_history.json")
    current_state = read_json_dict(state_dir / "state.json")
    scheduler_state = current_state.get("scheduler")
    if not isinstance(scheduler_state, dict):
        return history

    state_op = str(scheduler_state.get("selected_op", "") or "")
    history_op = str(history.get("selected_op", "") or "")
    state_count = int(scheduler_state.get("consecutive_count", 0) or 0)
    history_count = int(history.get("consecutive_count", 0) or 0)
    if (
        scheduler_state.get("fairness_cooldown")
        and state_op
        and (not history_op or state_op == history_op)
        and (not history_count or state_count == history_count)
    ):
        merged = dict(history)
        for key in (
            "selected_op",
            "selected_action",
            "consecutive_count",
            "fairness_cooldown",
            "fairness_hold_count",
        ):
            if key in scheduler_state:
                merged[key] = scheduler_state[key]
        return merged
    return history


def read_json_dict(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def read_selected_action_liveness(
    root: Path,
    selected: GateDecision | None,
    captured_at: str,
    stagnant_threshold_seconds: int,
) -> dict[str, object]:
    if selected is None:
        return {
            "selected_action_id": "",
            "repeat_count": 0,
            "age_seconds": 0,
            "stagnant": False,
            "stagnant_threshold_seconds": stagnant_threshold_seconds,
        }

    action_id = selected.action_id
    current_ts = parse_timestamp(captured_at)
    first_seen = current_ts
    last_seen = current_ts
    repeat_count = 1

    path = root / "TestUtils" / "tester_daemon" / "events.jsonl"
    if path.exists():
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            lines = []
        for line in reversed(lines):
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            selected_record = record.get("selected")
            if not isinstance(selected_record, dict):
                break
            if selected_record.get("action_id") != action_id:
                break
            repeat_count += 1
            event_ts = parse_timestamp(str(record.get("captured_at") or record.get("time") or ""))
            if event_ts is not None:
                first_seen = event_ts
                if repeat_count == 2:
                    last_seen = event_ts

    age_seconds = 0
    if current_ts is not None and first_seen is not None:
        age_seconds = max(0, int((current_ts - first_seen).total_seconds()))

    stagnant = stagnant_threshold_seconds > 0 and age_seconds >= stagnant_threshold_seconds
    return {
        "selected_action_id": action_id,
        "repeat_count": repeat_count,
        "first_seen_at": first_seen.isoformat() if first_seen else "",
        "last_previous_seen_at": last_seen.isoformat() if last_seen else "",
        "age_seconds": age_seconds,
        "stagnant": stagnant,
        "stagnant_threshold_seconds": stagnant_threshold_seconds,
    }


def parse_timestamp(text: str):
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


class EngineCandidateRefresher:
    """Run candidate discovery/admission outside the relay-critical daemon tick."""

    def __init__(self, root: Path, config_path: Path) -> None:
        self.root = root
        self.config_path = config_path
        self._process: subprocess.Popen[bytes] | None = None
        self._last_finished_monotonic = 0.0

    @property
    def state_path(self) -> Path:
        return self.root / "TestUtils" / "tester_daemon" / "engine_candidate_worker.json"

    def maybe_start(self, config: DaemonConfig) -> bool:
        current = time.monotonic()
        if self._process is not None:
            if self._process.poll() is None:
                return False
            self._process = None
            self._last_finished_monotonic = current

        state = read_json_dict(self.state_path)
        state_pid = int(state.get("pid", 0) or 0)
        if str(state.get("status") or "") == "running" and state_pid > 0:
            if process_alive(state_pid):
                return False

        completed_interval = max(
            0.1,
            float(
                config.policy.get("engine_candidate_refresh_interval_seconds", 0.5)
                or 0.5
            ),
        )
        failed_interval = max(
            completed_interval,
            float(
                config.policy.get("engine_candidate_failed_retry_seconds", 5.0)
                or 5.0
            ),
        )
        retry_interval = (
            failed_interval
            if str(state.get("status") or "") == "failed"
            else completed_interval
        )
        retry_after = parse_timestamp(str(state.get("retry_after_at") or ""))
        if retry_after is not None:
            retry_after = retry_after.astimezone(timezone.utc)
            if datetime.now(timezone.utc) < retry_after:
                return False
        if (
            self._last_finished_monotonic > 0
            and current - self._last_finished_monotonic < retry_interval
        ):
            return False
        finished_at = parse_timestamp(str(state.get("finished_at") or ""))
        if finished_at is not None:
            age = (datetime.now(timezone.utc) - finished_at.astimezone(timezone.utc)).total_seconds()
            if age < retry_interval:
                return False

        command = [
            sys.executable,
            str((self.root / "tools" / "tester_daemon" / "daemon.py").resolve()),
            "engine-candidate-worker",
            "--config",
            str(self.config_path),
        ]
        self._process = subprocess.Popen(
            command,
            cwd=str(self.root),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=(
                process_creation_flags()
                | int(getattr(subprocess, "BELOW_NORMAL_PRIORITY_CLASS", 0))
            ),
            startupinfo=process_startupinfo(),
        )
        started_at = utc_now_iso()
        record = {
            "updated_at": started_at,
            "started_at": started_at,
            "status": "running",
            "pid": self._process.pid,
            "parent_pid": os.getpid(),
            "config": str(self.config_path),
        }
        write_json_atomic(self.state_path, record, ensure_ascii=True)
        append_daemon_runtime_event(
            self.root,
            "engine_candidate_refresh_started",
            record,
        )
        return True


def run_engine_candidate_worker(config_path: Path) -> int:
    state_path = ROOT / "TestUtils" / "tester_daemon" / "engine_candidate_worker.json"
    previous = read_json_dict(state_path)
    started_at = utc_now_iso()
    started = time.perf_counter()
    record: dict[str, object] = {
        "updated_at": started_at,
        "started_at": started_at,
        "status": "running",
        "pid": os.getpid(),
        "parent_pid": os.getppid(),
        "config": str(config_path),
    }
    write_json_atomic(state_path, record, ensure_ascii=True)
    try:
        full_path = config_path if config_path.is_absolute() else ROOT / config_path
        config = load_config(full_path, apply_completion_markers=True)
        returncode = enqueue_engine_compatibility_candidates(config)
        pump_result = maybe_start_engine_pump_worker(config)
        finished_at = utc_now_iso()
        record.update(
            {
                "updated_at": finished_at,
                "finished_at": finished_at,
                "status": "completed" if returncode == 0 else "failed",
                "returncode": returncode,
                "duration_seconds": round(time.perf_counter() - started, 3),
                "pump": pump_result,
            }
        )
        if returncode != 0:
            backoff = engine_candidate_failure_backoff(previous, config)
            record.update(backoff)
            record["error"] = f"candidate enqueue returned {returncode}"
        else:
            record.update(
                {
                    "consecutive_failure_count": 0,
                    "retry_after_seconds": 0.0,
                    "retry_after_at": "",
                }
            )
        write_json_atomic(state_path, record, ensure_ascii=True)
        append_daemon_runtime_event(
            ROOT,
            "engine_candidate_refresh_finished",
            record,
        )
        return returncode
    except Exception as exc:
        finished_at = utc_now_iso()
        record.update(
            {
                "updated_at": finished_at,
                "finished_at": finished_at,
                "status": "failed",
                "returncode": 1,
                "duration_seconds": round(time.perf_counter() - started, 3),
                "error": f"{type(exc).__name__}: {exc}",
            }
        )
        record.update(engine_candidate_failure_backoff(previous, None))
        write_json_atomic(state_path, record, ensure_ascii=True)
        append_daemon_runtime_event(
            ROOT,
            "engine_candidate_refresh_failed",
            {
                **record,
                "traceback": traceback.format_exc()[-4000:],
            },
        )
        return 1


def engine_candidate_failure_backoff(
    previous: dict[str, object],
    config: DaemonConfig | None,
    *,
    now: datetime | None = None,
) -> dict[str, object]:
    policy = config.policy if config is not None else {}
    base_seconds = max(
        1.0,
        float(policy.get("engine_candidate_failed_retry_seconds", 5.0) or 5.0),
    )
    max_seconds = max(
        base_seconds,
        float(
            policy.get("engine_candidate_failed_retry_max_seconds", 300.0)
            or 300.0
        ),
    )
    previous_count = (
        int(previous.get("consecutive_failure_count", 0) or 0)
        if str(previous.get("status") or "") == "failed"
        else 0
    )
    failure_count = previous_count + 1
    retry_seconds = min(
        max_seconds,
        base_seconds * (2 ** min(failure_count - 1, 16)),
    )
    observed_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    return {
        "consecutive_failure_count": failure_count,
        "retry_after_seconds": round(retry_seconds, 3),
        "retry_after_at": (
            observed_at + timedelta(seconds=retry_seconds)
        ).isoformat(),
    }


class ObservabilityRefresher:
    """Run expensive status refreshes in one hidden child process at a time."""

    def __init__(
        self,
        root: Path,
        config_path: Path,
        min_interval_seconds: float = 5.0,
        unchanged_interval_seconds: float = 60.0,
    ) -> None:
        self.root = root
        self.config_path = config_path
        self.min_interval_seconds = max(0.0, float(min_interval_seconds))
        self.unchanged_interval_seconds = max(0.0, float(unchanged_interval_seconds))
        self._process: subprocess.Popen[bytes] | None = None
        self._last_finished_monotonic = 0.0
        self._last_started_monotonic = 0.0
        self._last_payload_fingerprint = ""

    def maybe_start(
        self,
        snapshot,
        decisions,
        plan,
        mode: str,
        config: DaemonConfig,
        leases,
        action_liveness,
    ) -> bool:
        now = time.monotonic()
        if self._process is not None:
            if self._process.poll() is None:
                return False
            self._process = None
            self._last_finished_monotonic = now
        worker_state_path = (
            self.root / "TestUtils" / "tester_daemon" / "observability_worker.json"
        )
        worker_state = read_json_dict(worker_state_path)
        try:
            external_pid = int(worker_state.get("pid", 0) or 0)
        except (TypeError, ValueError):
            external_pid = 0
        expected_start_token = str(worker_state.get("start_token") or "")
        if (
            str(worker_state.get("status") or "") == "running"
            and external_pid > 0
            and process_alive(external_pid)
            and (
                not expected_start_token
                or process_start_token(external_pid) == expected_start_token
            )
        ):
            return False
        if (
            self._last_finished_monotonic > 0
            and now - self._last_finished_monotonic < self.min_interval_seconds
        ):
            return False
        fingerprint = observability_payload_fingerprint(
            snapshot,
            decisions,
            plan,
            leases,
            action_liveness,
        )
        unchanged = fingerprint == self._last_payload_fingerprint
        if (
            unchanged
            and self._last_started_monotonic > 0
            and now - self._last_started_monotonic < self.unchanged_interval_seconds
        ):
            return False
        payload_dir = self.root / "TestUtils" / "tester_daemon" / "observability_payloads"
        payload_dir.mkdir(parents=True, exist_ok=True)
        payload_path = payload_dir / f"refresh_{os.getpid()}_{time.time_ns()}.json"
        payload = {
            "root": str(self.root),
            "parent_pid": os.getpid(),
            "config_path": str(self.config_path),
            "snapshot": asdict(snapshot),
            "decisions": [asdict(decision) for decision in decisions],
            "plan": asdict(plan),
            "mode": mode,
            "leases": list(leases),
            "action_liveness": action_liveness or {},
        }
        payload_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        command = [
            sys.executable,
            str((self.root / "tools" / "tester_daemon" / "daemon.py").resolve()),
            "observability-worker",
            "--payload",
            str(payload_path.resolve()),
        ]
        try:
            self._process = subprocess.Popen(
                command,
                cwd=str(self.root),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=(
                    process_creation_flags()
                    | int(getattr(subprocess, "BELOW_NORMAL_PRIORITY_CLASS", 0))
                ),
                startupinfo=process_startupinfo(),
            )
        except Exception:
            payload_path.unlink(missing_ok=True)
            raise
        self._last_payload_fingerprint = fingerprint
        self._last_started_monotonic = now
        (self.root / "TestUtils" / "tester_daemon" / "observability_worker.json").write_text(
            json.dumps(
                {
                    "updated_at": utc_now_iso(),
                    "pid": self._process.pid,
                    "start_token": process_start_token(self._process.pid) or "",
                    "payload": str(payload_path.relative_to(self.root)),
                    "status": "running",
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        return True


def observability_payload_fingerprint(
    snapshot,
    decisions,
    plan,
    leases,
    action_liveness,
) -> str:
    payload = {
        "rows": [asdict(row) for row in snapshot.rows],
        "decisions": [asdict(decision) for decision in decisions],
        "selected": asdict(plan.selected) if plan.selected is not None else None,
        "held": [asdict(decision) for decision in plan.held],
        "leases": list(leases),
    }
    stable = stable_observability_value(payload)
    return json.dumps(stable, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def stable_observability_value(value):
    if isinstance(value, dict):
        stable: dict[str, object] = {}
        for key, item in sorted(value.items(), key=lambda pair: str(pair[0])):
            name = str(key)
            if (
                name in {"captured_at", "updated_at", "time"}
                or name.endswith("_at")
                or name in {"age_seconds", "idle_seconds", "heartbeat_age_seconds"}
            ):
                continue
            stable[name] = stable_observability_value(item)
        return stable
    if isinstance(value, (list, tuple)):
        return [stable_observability_value(item) for item in value]
    return value


def deserialize_board_row(data: dict[str, object]) -> BoardRow:
    fields = (
        "season",
        "op",
        "gate_stage",
        "next_owner",
        "solver_goal",
        "tester_goal",
        "wakeups",
        "next_command",
    )
    return BoardRow(**{field: str(data.get(field, "") or "") for field in fields})


def deserialize_decision(data: dict[str, object]) -> GateDecision:
    row_data = data.get("row", {}) if isinstance(data.get("row"), dict) else {}
    return GateDecision(
        row=deserialize_board_row(row_data),
        action=ActionKind(str(data.get("action", "hold") or "hold")),
        reason=str(data.get("reason", "") or ""),
        command=str(data.get("command", "") or ""),
        priority=int(data.get("priority", 0) or 0),
        blocks_operator=str(data.get("blocks_operator", "") or ""),
    )


def run_observability_worker_payload(payload_path: Path) -> int:
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    root = Path(str(payload.get("root", "") or ROOT))
    parent_pid = int(payload.get("parent_pid", 0) or 0)
    if parent_pid > 0 and not process_alive(parent_pid):
        append_daemon_runtime_event(
            root,
            "observability_refresh_abandoned",
            {
                "pid": os.getpid(),
                "parent_pid": parent_pid,
                "reason": "parent-exited-before-start",
            },
        )
        payload_path.unlink(missing_ok=True)
        return 0
    config_path = Path(str(payload.get("config_path", "") or ""))
    if not config_path.is_absolute():
        config_path = root / config_path
    config = load_config(config_path, apply_completion_markers=True)
    snapshot_data = payload.get("snapshot", {}) if isinstance(payload.get("snapshot"), dict) else {}
    snapshot = BoardSnapshot(
        captured_at=str(snapshot_data.get("captured_at", "") or ""),
        command=tuple(str(item) for item in snapshot_data.get("command", []) or []),
        rows=tuple(
            deserialize_board_row(item)
            for item in snapshot_data.get("rows", []) or []
            if isinstance(item, dict)
        ),
        raw_output=str(snapshot_data.get("raw_output", "") or ""),
        transport=tuple(
            TransportObservation(**item)
            for item in snapshot_data.get("transport", []) or []
            if isinstance(item, dict)
        ),
    )
    decisions = tuple(
        deserialize_decision(item)
        for item in payload.get("decisions", []) or []
        if isinstance(item, dict)
    )
    plan_data = payload.get("plan", {}) if isinstance(payload.get("plan"), dict) else {}
    selected_data = plan_data.get("selected") if isinstance(plan_data.get("selected"), dict) else None
    plan = DaemonPlan(
        selected=deserialize_decision(selected_data) if selected_data else None,
        decisions=tuple(
            deserialize_decision(item)
            for item in plan_data.get("decisions", []) or []
            if isinstance(item, dict)
        ),
        held=tuple(
            deserialize_decision(item)
            for item in plan_data.get("held", []) or []
            if isinstance(item, dict)
        ),
        scheduler_state=(
            plan_data.get("scheduler_state", {})
            if isinstance(plan_data.get("scheduler_state"), dict)
            else {}
        ),
    )
    started = time.perf_counter()
    state_path = root / "TestUtils" / "tester_daemon" / "observability_worker.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        control_node_error = ""
        observation_payload = read_json_dict(
            root / "TestUtils" / "tester_daemon" / "solver_thread_observations.json"
        )
        try:
            if parent_pid > 0 and not process_alive(parent_pid):
                append_daemon_runtime_event(
                    root,
                    "observability_refresh_abandoned",
                    {
                        "pid": os.getpid(),
                        "parent_pid": parent_pid,
                        "reason": "parent-exited-after-observation",
                    },
                )
                return 0
            maybe_reconcile_control_nodes(
                config,
                root=root,
                include_remote=True,
            )
        except Exception as exc:
            control_node_error = repr(exc)
            append_daemon_runtime_event(
                root,
                "control_node_observation_failed",
                {"pid": os.getpid(), "error": control_node_error},
            )
        writer = StatusWriter(root)
        writer.write(
            snapshot,
            decisions,
            plan,
            str(payload.get("mode", "") or "execute"),
            config,
            resource_leases=tuple(payload.get("leases", []) or []),
            action_liveness=(
                payload.get("action_liveness", {})
                if isinstance(payload.get("action_liveness"), dict)
                else {}
            ),
        )
        if parent_pid > 0 and not process_alive(parent_pid):
            append_daemon_runtime_event(
                root,
                "observability_refresh_abandoned",
                {
                    "pid": os.getpid(),
                    "parent_pid": parent_pid,
                    "reason": "parent-exited-before-publish",
                },
            )
            return 0
        prompt_metrics_started = time.perf_counter()
        write_session_prompt_metrics(root / "TestUtils" / "tester_daemon", config)
        prompt_metrics_seconds = time.perf_counter() - prompt_metrics_started
        write_solver_replacement_files(root, config)
        duration = round(time.perf_counter() - started, 3)
        state_path.write_text(
            json.dumps(
                {
                    "updated_at": utc_now_iso(),
                    "pid": os.getpid(),
                    "status": "completed",
                    "duration_seconds": duration,
                    "thread_observation_count": len(observation_payload.get("threads", []) or []),
                    "thread_observation_error": "",
                    "control_node_observation_error": control_node_error,
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        append_daemon_runtime_event(
            root,
            "observability_refresh_finished",
            {
                "pid": os.getpid(),
                "duration_seconds": duration,
                "thread_observation_count": len(observation_payload.get("threads", []) or []),
                "thread_observation_error": "",
                "control_node_observation_error": control_node_error,
                "prompt_metrics_seconds": round(prompt_metrics_seconds, 3),
                **{key: round(value, 3) for key, value in writer.last_timings.items()},
            },
        )
        return 0
    except Exception as exc:
        state_path.write_text(
            json.dumps(
                {
                    "updated_at": utc_now_iso(),
                    "pid": os.getpid(),
                    "status": "failed",
                    "error": repr(exc),
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        append_daemon_runtime_event(
            root,
            "observability_refresh_failed",
            {
                "pid": os.getpid(),
                "error": repr(exc),
                "traceback": traceback.format_exc()[-4000:],
            },
        )
        return 1
    finally:
        payload_path.unlink(missing_ok=True)


def refresh_storage_thread_observations(root: Path, config: DaemonConfig) -> dict[str, object]:
    """Refresh configured solver/tester turns without starting a Codex subprocess."""
    from ascendop_daemon.legacy.bridge import (
        filter_thread_observations_by_role,
        poll_solver_threads_from_rollout,
        render_thread_observations,
    )

    observed_at = utc_now_iso()
    payload = poll_solver_threads_from_rollout(
        config,
        observed_at,
        datetime.now(timezone.utc).timestamp(),
    )
    payload["observation_source"] = "daemon-rollout-storage"
    payload["storage_only"] = True
    state_dir = root / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "solver_thread_observations.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    (state_dir / "SOLVER_THREAD_OBSERVATIONS.md").write_text(
        render_thread_observations(payload),
        encoding="utf-8",
    )
    tester_payload = filter_thread_observations_by_role(payload, "tester")
    (state_dir / "tester_thread_observations.json").write_text(
        json.dumps(tester_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    (state_dir / "TESTER_THREAD_OBSERVATIONS.md").write_text(
        render_thread_observations(tester_payload, title="Tester Thread Observations"),
        encoding="utf-8",
    )
    (state_dir / "solver_thread_poll_status.json").write_text(
        json.dumps(
            {
                "updated_at": observed_at,
                "status": "ok",
                "mode": "daemon-rollout-storage",
                "thread_count": len(payload.get("threads", []) or []),
                "poll_fallback": "rollout-storage",
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return payload


class CriticalThreadObservationRefresher:
    """Keep relay-critical IDE state fresh independently of heavy analytics."""

    def __init__(
        self,
        root: Path,
        *,
        min_interval_seconds: float = 2.0,
    ) -> None:
        self.root = root
        self.min_interval_seconds = max(0.0, float(min_interval_seconds))
        self._last_started_monotonic = 0.0

    def maybe_refresh(self, config: DaemonConfig) -> bool:
        current = time.monotonic()
        if (
            self._last_started_monotonic > 0
            and current - self._last_started_monotonic < self.min_interval_seconds
        ):
            return False
        self._last_started_monotonic = current
        started = time.perf_counter()
        state_dir = self.root / "TestUtils" / "tester_daemon"
        state_path = state_dir / "critical_thread_observer.json"
        try:
            payload = refresh_storage_thread_observations(self.root, config)
            sync_solver_ack_from_thread_observations(self.root, config=config)
            sync_tester_ack_from_thread_observations(self.root, config=config)
            duration = round(time.perf_counter() - started, 3)
            record = {
                "updated_at": utc_now_iso(),
                "status": "ok",
                "pid": os.getpid(),
                "duration_seconds": duration,
                "thread_count": len(payload.get("threads", []) or []),
                "interval_seconds": self.min_interval_seconds,
            }
            write_json_atomic(state_path, record, ensure_ascii=True)
            append_daemon_runtime_event(
                self.root,
                "critical_thread_observation_finished",
                record,
            )
            return True
        except Exception as exc:
            duration = round(time.perf_counter() - started, 3)
            record = {
                "updated_at": utc_now_iso(),
                "status": "failed",
                "pid": os.getpid(),
                "duration_seconds": duration,
                "interval_seconds": self.min_interval_seconds,
                "error": f"{type(exc).__name__}: {exc}",
            }
            write_json_atomic(state_path, record, ensure_ascii=True)
            append_daemon_runtime_event(
                self.root,
                "critical_thread_observation_failed",
                record,
            )
            return False


class RuntimeMaintenanceCadence:
    """Keep heavy recovery scans off the latency-critical board path."""

    def __init__(self) -> None:
        self._last_started: dict[str, float] = {}

    def due(self, name: str, interval_seconds: float) -> bool:
        now = time.monotonic()
        last = self._last_started.get(name)
        interval = max(0.0, float(interval_seconds))
        if last is not None and now - last < interval:
            return False
        self._last_started[name] = now
        return True


def run_loop(args: argparse.Namespace) -> int:
    config_path = ROOT / args.config if not Path(args.config).is_absolute() else Path(args.config)
    config = load_config(
        config_path,
        apply_completion_markers=True,
    )
    reconcile_operator_plugin_state(ROOT, config)
    run_runtime_maintenance(ROOT, config.policy, actor="daemon-start")
    interval_seconds = args.interval_seconds
    if interval_seconds <= 0:
        interval_seconds = float(config.policy.get("run_interval_seconds", 60.0) or 60.0)
    max_burst_ticks = int(config.policy.get("max_fast_reschedule_ticks", 3) or 3)
    async_observability = bool(config.policy.get("async_observability_refresh", False))
    observability_refresher = (
        ObservabilityRefresher(
            ROOT,
            Path(args.config),
            min_interval_seconds=float(
                config.policy.get("observability_refresh_interval_seconds", 5.0) or 5.0
            ),
            unchanged_interval_seconds=float(
                config.policy.get("observability_refresh_unchanged_interval_seconds", 60.0)
                or 60.0
            ),
        )
        if args.write_state and async_observability
        else None
    )
    thread_observation_refresher = (
        CriticalThreadObservationRefresher(
            ROOT,
            min_interval_seconds=float(
                config.policy.get(
                    "critical_thread_observation_interval_seconds",
                    2.0,
                )
                or 2.0
            ),
        )
        if args.write_state
        else None
    )
    engine_candidate_refresher = (
        EngineCandidateRefresher(ROOT, Path(args.config))
        if args.mode == "execute" and args.allow_live_execute and not args.dry_run_execute
        else None
    )
    maintenance_cadence = RuntimeMaintenanceCadence()
    if args.clear_stop:
        clear_stop_request(ROOT)
    stop_request = read_stop_request(ROOT)
    if stop_request:
        print(
            "daemon stop requested before start: "
            f"{stop_request.get('requested_at', '')} {stop_request.get('reason', '')}"
        )
        append_daemon_runtime_event(
            ROOT,
            "run_loop_exit",
            {
                "reason": "stop_request_before_start",
                "pid": os.getpid(),
                "requested_at": str(stop_request.get("requested_at", "") or ""),
                "stop_reason": str(stop_request.get("reason", "") or ""),
            },
        )
        return 0
    lock = DaemonLock(ROOT, stale_after_seconds=args.replace_stale_lock_after_seconds)
    ticks = 0
    burst_ticks = 0
    exit_reason = "unhandled"
    append_daemon_runtime_event(
        ROOT,
        "run_loop_start",
        {
            "pid": os.getpid(),
            "config": str(args.config),
            "mode": str(args.mode),
            "interval_seconds": interval_seconds,
            "max_ticks": int(args.max_ticks or 0),
        },
    )
    try:
        with lock:
            while True:
                loop_started_monotonic = time.monotonic()
                # Active operators are runtime plugins.  Reload membership for
                # heartbeat/session visibility as well as inside run_tick.
                config = load_config(config_path, apply_completion_markers=True)
                if (
                    flow_v3_executor_mode(config)
                    and not bool(
                        flow_v3_process_generation_status(ROOT)["matches"]
                    )
                ):
                    record_flow_v3_process_generation_drift("daemon")
                    exit_reason = "process_generation_drift"
                    return 75
                reconcile_operator_plugin_state(ROOT, config)
                run_runtime_maintenance(ROOT, config.policy, actor="daemon-loop")
                stop_request = read_stop_request(ROOT)
                if stop_request:
                    print(
                        "daemon stop requested: "
                        f"{stop_request.get('requested_at', '')} {stop_request.get('reason', '')}"
                    )
                    exit_reason = "stop_request"
                    return 0
                lock.write_heartbeat(config, args.mode)
                try:
                    rc = run_tick(
                        config_path=Path(args.config),
                        mode=args.mode,
                        write_state=args.write_state,
                        dry_run_execute=args.dry_run_execute,
                        expected_action_id=args.expected_action_id,
                        allow_live_execute=args.allow_live_execute,
                        observability_refresher=observability_refresher,
                        thread_observation_refresher=thread_observation_refresher,
                        engine_candidate_refresher=engine_candidate_refresher,
                        maintenance_cadence=maintenance_cadence,
                    )
                except Exception as exc:
                    append_daemon_runtime_event(
                        ROOT,
                        "tick_exception",
                        {
                            "error": str(exc),
                            "traceback": traceback.format_exc(),
                        },
                    )
                    print(f"daemon tick exception: {exc}", file=sys.stderr, flush=True)
                    rc = 1
                ticks += 1
                if rc != 0:
                    append_daemon_runtime_event(ROOT, "tick_nonzero", {"returncode": rc})
                    if args.exit_on_tick_error:
                        exit_reason = "tick_nonzero_exit"
                        return rc
                if args.max_ticks and ticks >= args.max_ticks:
                    exit_reason = "max_ticks"
                    return 0
                if (
                    rc == 0
                    and args.mode == "execute"
                    and burst_ticks < max_burst_ticks
                    and consume_fast_reschedule_if_needed(ROOT)
                ):
                    burst_ticks += 1
                    append_daemon_runtime_event(
                        ROOT,
                        "fast_reschedule",
                        {
                            "burst_tick": burst_ticks,
                            "max_burst_ticks": max_burst_ticks,
                            "reason": "non_resource_harness_action_completed",
                        },
                    )
                    continue
                burst_ticks = 0
                sleep_seconds = fixed_cadence_sleep_seconds(
                    interval_seconds,
                    loop_started_monotonic,
                )
                if sleep_or_stop(ROOT, sleep_seconds):
                    exit_reason = "stop_request_during_sleep"
                    return 0
    except BaseException as exc:
        exit_reason = "base_exception"
        append_daemon_runtime_event(
            ROOT,
            "run_loop_base_exception",
            {
                "pid": os.getpid(),
                "error_type": type(exc).__name__,
                "error": str(exc),
                "traceback": traceback.format_exc(),
            },
        )
        raise
    finally:
        append_daemon_runtime_event(
            ROOT,
            "run_loop_exit",
            {
                "reason": exit_reason,
                "pid": os.getpid(),
                "ticks": ticks,
            },
        )


def consume_fast_reschedule_if_needed(root: Path) -> bool:
    """Return True once after a successful non-resource harness action.

    This lets restore/prepare/requeue gates chain into the next submit decision
    without waiting a full daemon interval, while avoiding a busy loop when the
    previous action was skipped, still running, or resource-bound.
    """
    path = root / "TestUtils" / "tester_daemon" / "last_execute_result.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    if data.get("fast_reschedule_consumed_at"):
        return False
    if not data.get("ran") or data.get("returncode") not in (0, "0"):
        return False
    if data.get("resource_bound"):
        return False
    if str(data.get("execution_mode", "")) not in {"sync", "inline"}:
        return False
    action = str(data.get("action", "") or "")
    if action not in {
        ActionKind.PREPARE_SUBMIT.value,
        ActionKind.RESTORE_SUBMIT.value,
        ActionKind.REQUEUE_SUBMIT.value,
        ActionKind.REPAIR_QUEUE.value,
    }:
        return False
    finished_at = parse_timestamp(str(data.get("finished_at", "") or data.get("updated_at", "") or ""))
    if finished_at is None:
        return False
    age_seconds = max(0.0, (datetime.now(finished_at.tzinfo) - finished_at).total_seconds())
    if age_seconds > 60:
        return False
    data["fast_reschedule_consumed_at"] = utc_now_iso()
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return True


def sleep_or_stop(root: Path, interval_seconds: float) -> bool:
    deadline = time.monotonic() + max(0.0, interval_seconds)
    while time.monotonic() < deadline:
        if read_stop_request(root):
            return True
        remaining = deadline - time.monotonic()
        time.sleep(min(1.0, max(0.0, remaining)))
    return bool(read_stop_request(root))


def fixed_cadence_sleep_seconds(
    interval_seconds: float,
    cycle_started_monotonic: float,
    *,
    now_monotonic: float | None = None,
) -> float:
    now = time.monotonic() if now_monotonic is None else now_monotonic
    elapsed = max(0.0, now - cycle_started_monotonic)
    return max(0.0, float(interval_seconds) - elapsed)


def append_daemon_runtime_event(root: Path, event: str, payload: dict[str, object]) -> None:
    state_dir = root / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    with (state_dir / "daemon_runtime_events.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(json.dumps({"time": utc_now_iso(), "event": event, **payload}, ensure_ascii=False) + "\n")


def stop_daemon(args: argparse.Namespace) -> int:
    path = write_stop_request(ROOT, args.reason)
    print(f"stop request written: {path}")
    config_value = str(
        getattr(
            args,
            "config",
            "tools/tester_daemon/config/s5_910b_gitpartner_glugrad_bitwise.json",
        )
    )
    config_path = Path(config_value)
    if not config_path.is_absolute():
        config_path = ROOT / config_path
    try:
        drain = prepare_engine_stop_drain(
            load_config(config_path, apply_completion_markers=False)
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        drain = {
            "started": False,
            "reason": "engine_stop_drain_failed",
            "error": str(exc),
        }
        append_daemon_runtime_event(ROOT, "engine_stop_drain_failed", drain)
    print_json({"engine_stop_drain": drain})
    if not getattr(args, "daemon_only", False):
        result = stop_runtime_process(
            ROOT,
            "solver_trigger_bridge",
            dry_run=False,
            reason=args.reason or "daemon stop requested",
        )
        print_json({"bridge_stop": result})
    return 0


def flow_v3_worker_record(
    runtime: FlowV3Runtime,
    *,
    config_path: Path,
    interval_seconds: float,
    state: str,
    cycle: int,
    last_result: dict[str, object] | None = None,
    error: str = "",
) -> dict[str, object]:
    record: dict[str, object] = {
        "schema": "ascendop.flow.worker.v3",
        "pid": os.getpid(),
        "start_token": process_start_token(os.getpid()),
        "started_at": DAEMON_PROCESS_STARTED_AT.isoformat(),
        "heartbeat_at": utc_now_iso(),
        "state": state,
        "cycle": int(cycle),
        "worker_identity": flow_v3_worker_identity(
            runtime,
            config_path=config_path,
            interval_seconds=interval_seconds,
        ),
    }
    if last_result is not None:
        record["last_result"] = last_result
    if error:
        record["error"] = error
    return record


def flow_v3_cmd(args: argparse.Namespace) -> int:
    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = ROOT / config_path
    config_path = config_path.resolve()
    config = load_config(config_path, apply_completion_markers=True)
    if not flow_v3_executor_mode(config):
        print_json(
            {
                "schema": "ascendop.flow.command.v3",
                "outcome": "held",
                "reason": "config-is-not-flow-v3",
                "config": str(config_path),
            }
        )
        return 2
    runtime = FlowV3Runtime(flow_v3_runtime_config(ROOT, config.policy))
    interval = max(0.25, float(args.interval_seconds or 1.0))
    action = str(args.flow_v3_action)

    if action == "status":
        payload = {
            "schema": "ascendop.flow.status.v3",
            "release": runtime.config.release.to_dict(),
            "store": runtime.store.status(),
            "worker": read_json_dict(flow_v3_worker_path(ROOT)),
            "readiness": runtime.readiness(),
            "stop_requested": bool(read_stop_request(ROOT)),
        }
        print_json(payload)
        return 0 if payload["readiness"]["ready"] else 2

    if action == "recover-workflow-ingest":
        reopened = runtime.store.recover_workflow_ingest(
            str(args.request_id),
            str(args.attempt_id),
            actor="flow-v3-retry-controller",
        )
        if str(reopened["state"]) == "succeeded":
            result = {
                "request_id": str(args.request_id),
                "attempt_id": str(args.attempt_id),
                "state": "workflow-ingest-already-succeeded",
                "workflow_ingest": reopened,
            }
        else:
            result = runtime.ingestor.recover_terminal_workflow_ingest(
                str(args.request_id),
                str(args.attempt_id),
            )
        print_json(result)
        return 0 if str(result["state"]) in {
            "workflow-ingest-recovered",
            "workflow-ingest-already-succeeded",
        } else 2

    runtime.register_local()
    runtime.register_observed_services(flow_v3_observed_service_states())
    write_flow_v3_release_manifest(runtime.config)
    if action == "probe":
        try:
            endpoint = runtime.probe_endpoint()
        except Exception as exc:
            print_json(
                {
                    "schema": "ascendop.flow.probe.v3",
                    "outcome": "failed",
                    "error": f"{type(exc).__name__}: {exc}",
                    "readiness": runtime.readiness(),
                }
            )
            return 2
        print_json(
            {
                "schema": "ascendop.flow.probe.v3",
                "outcome": "ready" if runtime.readiness()["ready"] else "held",
                "endpoint": endpoint,
                "readiness": runtime.readiness(),
            }
        )
        return 0 if runtime.readiness()["ready"] else 2

    if action == "tick":
        if not read_stop_request(ROOT):
            try:
                runtime.probe_endpoint()
            except Exception as exc:
                append_daemon_runtime_event(
                    ROOT,
                    "flow_v3_endpoint_probe_failed",
                    {"error": f"{type(exc).__name__}: {exc}"},
                )
        result = runtime.tick(
            allow_dispatch=not bool(read_stop_request(ROOT))
        )
        print_json(result)
        return 0 if result.get("outcome") != "held" else 2

    probe_interval = max(
        5.0,
        float(args.probe_interval_seconds or 60.0),
    )
    max_cycles = max(0, int(args.max_cycles or 0))
    cycle = 0
    last_probe = 0.0
    last_result: dict[str, object] = {}
    generation_drift = False
    worker_path = flow_v3_worker_path(ROOT)
    try:
        lock = NamedProcessLock(
            ROOT,
            "flow_v3_worker",
            stale_after_seconds=max(
                0,
                int(args.replace_stale_lock_after_seconds or 0),
            ),
        )
        with lock:
            while True:
                generation = flow_v3_process_generation_status(ROOT)
                if not bool(generation["matches"]):
                    record_flow_v3_process_generation_drift("worker")
                    last_result = {
                        "outcome": "process-generation-drift",
                        **generation,
                    }
                    generation_drift = True
                    break
                stopping = bool(read_stop_request(ROOT))
                runtime.register_local()
                runtime.register_observed_services(
                    flow_v3_observed_service_states()
                )
                runtime.register_worker()
                if not stopping and time.monotonic() - last_probe >= probe_interval:
                    try:
                        runtime.probe_endpoint()
                    except Exception as exc:
                        append_daemon_runtime_event(
                            ROOT,
                            "flow_v3_endpoint_probe_failed",
                            {"error": f"{type(exc).__name__}: {exc}"},
                        )
                    last_probe = time.monotonic()
                write_json_atomic(
                    worker_path,
                    flow_v3_worker_record(
                        runtime,
                        config_path=config_path,
                        interval_seconds=interval,
                        state="running",
                        cycle=cycle,
                        last_result=last_result,
                    ),
                    ensure_ascii=True,
                )

                def refresh_inflight_heartbeat() -> None:
                    current_stopping = bool(read_stop_request(ROOT))
                    runtime.register_local()
                    runtime.register_observed_services(
                        flow_v3_observed_service_states()
                    )
                    runtime.register_worker()
                    write_json_atomic(
                        worker_path,
                        flow_v3_worker_record(
                            runtime,
                            config_path=config_path,
                            interval_seconds=interval,
                            state=(
                                "draining"
                                if current_stopping
                                else "running"
                            ),
                            cycle=cycle,
                            last_result=last_result,
                        ),
                        ensure_ascii=True,
                    )

                try:
                    last_result = runtime.tick_with_heartbeat(
                        allow_dispatch=not stopping,
                        heartbeat_interval_seconds=interval,
                        heartbeat=refresh_inflight_heartbeat,
                    )
                except Exception as exc:
                    detail = f"{type(exc).__name__}: {exc}"
                    last_result = {
                        "outcome": "cycle-failed",
                        "error": detail,
                    }
                    append_daemon_runtime_event(
                        ROOT,
                        "flow_v3_worker_cycle_failed",
                        {
                            "cycle": cycle,
                            "error": detail,
                            "traceback": traceback.format_exc()[-4000:],
                        },
                    )
                cycle += 1
                write_json_atomic(
                    worker_path,
                    flow_v3_worker_record(
                        runtime,
                        config_path=config_path,
                        interval_seconds=interval,
                        state="draining" if stopping else "running",
                        cycle=cycle,
                        last_result=last_result,
                    ),
                    ensure_ascii=True,
                )
                if stopping and not runtime.has_drain_work():
                    break
                if max_cycles and cycle >= max_cycles:
                    break
                time.sleep(interval)
    except RuntimeError as exc:
        append_daemon_runtime_event(
            ROOT,
            "flow_v3_worker_lock_busy",
            {"error": str(exc), "pid": os.getpid()},
        )
        return 2
    final = flow_v3_worker_record(
        runtime,
        config_path=config_path,
        interval_seconds=interval,
        state="stopped",
        cycle=cycle,
        last_result=last_result,
    )
    write_json_atomic(worker_path, final, ensure_ascii=True)
    append_daemon_runtime_event(ROOT, "flow_v3_worker_stopped", final)
    return 75 if generation_drift else 0


def clear_stop(args: argparse.Namespace) -> int:
    removed = clear_stop_request(ROOT)
    print("stop request cleared" if removed else "no stop request present")
    return 0


def print_json(payload: object) -> None:
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    try:
        print(text)
    except UnicodeEncodeError:
        encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
        data = (text + "\n").encode(encoding, errors="replace")
        buffer = getattr(sys.stdout, "buffer", None)
        if buffer is not None:
            buffer.write(data)
        else:
            print(text.encode(encoding, errors="replace").decode(encoding, errors="replace"))


def health(args: argparse.Namespace) -> int:
    report = check_health(ROOT, max_heartbeat_age_seconds=args.max_heartbeat_age_seconds)
    print(report.render(), end="")
    return 0 if report.ok else 2


def status_query(args: argparse.Namespace) -> int:
    config_full_path = ROOT / args.config if not Path(args.config).is_absolute() else Path(args.config)
    config = load_config(config_full_path, apply_completion_markers=True)
    payload = build_status_query(
        ROOT,
        config,
        config_path=str(args.config),
        max_heartbeat_age_seconds=args.max_heartbeat_age_seconds,
        refresh_board=not args.no_live_board,
    )
    if args.write_state:
        write_status_query_files(ROOT, payload)
    if args.json:
        print_json(payload)
    else:
        print(render_status_query(payload), end="")
    return 0 if payload.get("status") != "ALERT" else 2


def workflow_status(args: argparse.Namespace) -> int:
    registry = WorkflowProfileRegistry(ROOT)
    manifest_paths = tuple(Path(value) for value in args.manifest)
    try:
        registry.load_manifests(manifest_paths or None)
        payload = registry.status(
            profile_id=args.profile_id,
            instance_id=args.instance_id,
        )
    except WorkflowProfileError as exc:
        print_json(
            {
                "schema": "ascendop.workflow-status-error.v1",
                "error": str(exc),
            }
        )
        return 2
    print_json(payload)
    return 0


def native_relay_outbox(args: argparse.Namespace) -> int:
    payload = write_native_relay_outbox(ROOT) if args.write_state else build_native_relay_outbox(ROOT)
    if args.json:
        print_json(payload)
    else:
        path = ROOT / "TestUtils" / "tester_daemon" / "NATIVE_RELAY_OUTBOX.md"
        if args.write_state and path.exists():
            print(path.read_text(encoding="utf-8"), end="")
        else:
            print_json(payload)
    return 0 if int(payload.get("entry_count", 0) or 0) == 0 else 2


def native_relay_claim(args: argparse.Namespace) -> int:
    wait = wait_for_native_relay_availability(
        ROOT,
        wait_seconds=getattr(args, "wait_seconds", 0.0),
        poll_interval_seconds=getattr(args, "poll_interval_seconds", 0.25),
    )
    payload = claim_native_relay_entries(
        ROOT,
        consumer=args.consumer,
        ttl_seconds=args.ttl_seconds,
        max_items=args.max_items,
        include_prompt=args.include_prompt,
    )
    payload["long_poll"] = wait
    if args.json:
        print_json(payload)
    else:
        print_json(payload)
    return 0


def read_recent_jsonl(path: Path, *, max_bytes: int = 1_048_576) -> list[dict[str, object]]:
    if not path.exists():
        return []
    try:
        with path.open("rb") as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            start = max(0, size - max(1, int(max_bytes)))
            fh.seek(start)
            payload = fh.read()
    except OSError:
        return []
    if start:
        _, separator, payload = payload.partition(b"\n")
        if not separator:
            return []
    records: list[dict[str, object]] = []
    for raw_line in payload.splitlines():
        try:
            value = json.loads(raw_line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        if isinstance(value, dict):
            records.append(value)
    return records


def native_relay_completion_ack_metadata(record: dict[str, object]) -> dict[str, object]:
    fields = (
        "pre_delivery_turn_id",
        "turn_id",
        "delivery",
        "ide_panel_visible",
        "ide_panel_visibility",
        "completion_unconfirmed",
        "control_plane_unavailable",
        "orphaned_delivery_owner",
        "delivery_started_at",
        "native_id",
        "native_status",
        "turn_status",
        "model",
        "thinking",
        "prompt_profile",
        "prompt_mode",
        "prompt_chars",
        "delivery_ordinal",
        "contract_revision",
        "reanchor_reason",
        "correction_reason",
        "claim_recovery",
        "recovered_from_claim_observation",
        "same_turn_prompt_append_proven",
        "relay_prompt_observed_at",
        "claimed_at",
        "failure_kind",
        "error",
        "requested_status",
    )
    return {
        field: record[field]
        for field in fields
        if record.get(field) not in {None, ""}
    }


def relay_claim_matching_observed_turn(
    claim: dict[str, object],
    observed: dict[str, object],
) -> dict[str, object] | None:
    expected_prompt_sha1 = str(claim.get("prompt_sha1") or "")
    if not expected_prompt_sha1:
        return None
    pre_delivery_turn_id = str(claim.get("pre_delivery_turn_id") or "")
    claimed_at = parse_timestamp(str(claim.get("claimed_at") or claim.get("time") or ""))
    recent = observed.get("recent_turns", [])
    if not isinstance(recent, list):
        return None
    for turn in recent:
        if not isinstance(turn, dict):
            continue
        prompt_digests = turn.get("relay_prompt_sha1s", [])
        if not isinstance(prompt_digests, list):
            prompt_digests = []
        prompt_digests = {
            str(item) for item in prompt_digests if str(item or "")
        } | {str(turn.get("relay_prompt_sha1") or "")}
        if expected_prompt_sha1 not in prompt_digests:
            continue
        turn_id = str(turn.get("turn_id") or "")
        if not turn_id:
            continue
        matching_prompt_at = None
        prompt_events = turn.get("relay_prompt_events", [])
        if isinstance(prompt_events, list):
            for event in prompt_events:
                if (
                    not isinstance(event, dict)
                    or str(event.get("sha1") or "") != expected_prompt_sha1
                ):
                    continue
                value = event.get("observed_at")
                try:
                    if value not in {None, ""}:
                        matching_prompt_at = datetime.fromtimestamp(
                            float(value), tz=timezone.utc
                        )
                except (TypeError, ValueError, OSError):
                    matching_prompt_at = parse_timestamp(str(value or ""))
                if matching_prompt_at is not None:
                    break
        started_value = turn.get("started_at")
        started_at = None
        try:
            if started_value not in {None, ""}:
                started_at = datetime.fromtimestamp(
                    float(started_value), tz=timezone.utc
                )
        except (TypeError, ValueError, OSError):
            started_at = parse_timestamp(str(started_value or ""))
        same_turn_append = turn_id == pre_delivery_turn_id
        if same_turn_append:
            if matching_prompt_at is None:
                continue
            if (
                claimed_at is not None
                and matching_prompt_at < claimed_at - timedelta(seconds=3)
            ):
                continue
        elif (
            claimed_at is not None
            and started_at is not None
            and started_at < claimed_at - timedelta(seconds=3)
        ):
            continue
        status = str(turn.get("turn_status") or "")
        if status not in OBSERVABLE_SOLVER_TURN_STATUSES:
            continue
        matched = dict(turn)
        if same_turn_append:
            matched["same_turn_prompt_append_proven"] = True
            matched["relay_prompt_observed_at"] = matching_prompt_at.isoformat()
        return matched
    return None


def read_native_relay_claim_recovery_candidates(
    state_dir: Path,
    *,
    max_age_seconds: int,
    max_backfill_bytes: int = 16 * 1024 * 1024,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    """Incrementally index claimed relay entries that lack completion events."""

    events_path = state_dir / "native_relay_claim_events.jsonl"
    index_path = state_dir / "native_relay_claim_recovery_state.json"
    previous = read_json_dict(index_path)
    unresolved_raw = previous.get("unresolved", {})
    unresolved = (
        {
            str(key): dict(value)
            for key, value in unresolved_raw.items()
            if isinstance(value, dict)
        }
        if isinstance(unresolved_raw, dict)
        else {}
    )
    try:
        stat = events_path.stat()
    except OSError:
        return [], {
            "updated_at": utc_now_iso(),
            "events_offset": 0,
            "events_identity": "",
            "unresolved": {},
        }
    identity = f"{stat.st_dev}:{stat.st_ino}"
    previous_identity = str(previous.get("events_identity") or "")
    try:
        previous_offset = int(previous.get("events_offset", 0) or 0)
    except (TypeError, ValueError):
        previous_offset = 0
    reset = (
        not previous_identity
        or previous_identity != identity
        or previous_offset < 0
        or previous_offset > stat.st_size
    )
    start = (
        max(0, stat.st_size - max(1, int(max_backfill_bytes)))
        if reset
        else previous_offset
    )
    try:
        with events_path.open("rb") as fh:
            fh.seek(start)
            payload = fh.read()
            next_offset = fh.tell()
    except OSError:
        return list(unresolved.values()), {
            "updated_at": utc_now_iso(),
            "events_offset": previous_offset,
            "events_identity": identity,
            "unresolved": unresolved,
        }
    if reset and start:
        _, separator, payload = payload.partition(b"\n")
        if not separator:
            payload = b""
    for raw_line in payload.splitlines():
        try:
            record = json.loads(raw_line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        if not isinstance(record, dict):
            continue
        event = str(record.get("event") or "")
        claim_id = str(record.get("claim_id") or "")
        entry_id = str(record.get("entry_id") or record.get("id") or "")
        if event == "native_relay_claimed" and claim_id and entry_id:
            unresolved[claim_id] = record
        elif event == "native_relay_completed":
            if claim_id:
                unresolved.pop(claim_id, None)
            if entry_id:
                unresolved = {
                    key: value
                    for key, value in unresolved.items()
                    if str(value.get("entry_id") or value.get("id") or "")
                    != entry_id
                }
    current = datetime.now(timezone.utc)
    for claim_id, claim in tuple(unresolved.items()):
        claimed_at = parse_timestamp(
            str(claim.get("claimed_at") or claim.get("time") or "")
        )
        if claimed_at is None:
            unresolved.pop(claim_id, None)
            continue
        age = (current - claimed_at.astimezone(timezone.utc)).total_seconds()
        if age < 0 or age > max_age_seconds:
            unresolved.pop(claim_id, None)
    next_state: dict[str, object] = {
        "updated_at": utc_now_iso(),
        "events_offset": next_offset,
        "events_identity": identity,
        "unresolved_count": len(unresolved),
        "unresolved": unresolved,
    }
    return list(unresolved.values()), next_state


def reconcile_native_relay_claim_observations(
    root: Path,
    *,
    max_recoveries: int = 3,
    max_age_seconds: int = 900,
) -> int:
    """Close send->complete gaps using exact prompt proof from rollout storage."""

    state_dir = root / "TestUtils" / "tester_daemon"
    claims, recovery_state = read_native_relay_claim_recovery_candidates(
        state_dir,
        max_age_seconds=max_age_seconds,
    )
    observations = read_json_dict(state_dir / "solver_thread_observations.json")
    threads = observations.get("threads", [])
    if not isinstance(threads, list):
        return 0
    observed_by_identity = {
        (
            str(item.get("role", "solver") or "solver"),
            str(item.get("thread_id") or ""),
        ): item
        for item in threads
        if isinstance(item, dict) and item.get("thread_id")
    }
    solver_state = read_trigger_ack_state(root)
    tester_state = read_tester_trigger_ack_state(root)
    sent_by_kind = {
        "solver": (
            solver_state.get("sent", {})
            if isinstance(solver_state.get("sent"), dict)
            else {}
        ),
        "tester": (
            tester_state.get("sent", {})
            if isinstance(tester_state.get("sent"), dict)
            else {}
        ),
    }
    current = datetime.now(timezone.utc)
    recovered = 0
    recovered_entries: set[str] = set()
    unresolved = (
        recovery_state.get("unresolved", {})
        if isinstance(recovery_state.get("unresolved"), dict)
        else {}
    )
    claims = sorted(
        claims,
        key=lambda item: str(item.get("claimed_at") or item.get("time") or ""),
        reverse=True,
    )
    for claim in claims:
        entry_id = str(claim.get("entry_id") or claim.get("id") or "")
        claim_id = str(claim.get("claim_id") or "")
        if not entry_id or entry_id in recovered_entries:
            continue
        kind = str(claim.get("type") or "")
        key = str(claim.get("key") or "")
        thread_id = str(claim.get("thread_id") or "")
        if kind not in {"solver", "tester"} or not key or not thread_id:
            continue
        claimed_at = parse_timestamp(str(claim.get("claimed_at") or claim.get("time") or ""))
        if claimed_at is None:
            continue
        age = (current - claimed_at.astimezone(timezone.utc)).total_seconds()
        if age < 0 or age > max_age_seconds:
            continue
        observed = observed_by_identity.get((kind, thread_id))
        if not isinstance(observed, dict):
            continue
        turn = relay_claim_matching_observed_turn(claim, observed)
        if turn is None:
            continue
        turn_id = str(turn.get("turn_id") or "")
        turn_status = str(turn.get("turn_status") or "")
        effective_status = (
            "completed"
            if turn_status == "completed"
            else (
                "active"
                if turn_status in {"inProgress", "running", "queued"}
                else "failed"
            )
        )
        metadata: dict[str, object] = {
            "pre_delivery_turn_id": str(claim.get("pre_delivery_turn_id") or ""),
            "turn_id": turn_id,
            "native_id": turn_id,
            "turn_status": turn_status,
            "native_status": turn_status,
            "delivery": "codex-app-send-message-to-thread",
            "ide_panel_visible": True,
            "ide_panel_visibility": "confirmed_by_native_relay",
            "completion_unconfirmed": False,
            "control_plane_unavailable": False,
            "orphaned_delivery_owner": False,
            "delivery_started_at": str(claim.get("claimed_at") or ""),
            "last_observed_at": str(observations.get("updated_at") or utc_now_iso()),
            "last_observed_turn_id": turn_id,
            "last_observed_turn_status": turn_status,
            "last_observed_has_agent_output": turn.get("has_agent_output"),
            "last_observed_user_only_turn": turn.get("user_only_turn"),
            "recovered_from_claim_observation": True,
            "method": "claim-prompt-rollout-reconcile",
        }
        if turn.get("same_turn_prompt_append_proven") is True:
            metadata["same_turn_prompt_append_proven"] = True
            metadata["relay_prompt_observed_at"] = str(
                turn.get("relay_prompt_observed_at") or ""
            )
        if effective_status == "failed":
            metadata.update(
                {
                    "failure_kind": "native_turn_terminal_failure",
                    "error": f"observed relay turn ended with status {turn_status}",
                }
            )
        payload = complete_native_relay_claim(
            root,
            entry_id=entry_id,
            status=effective_status,
            turn_id=turn_id,
            delivery="codex-app-send-message-to-thread",
            ide_panel_visible=effective_status != "failed",
            metadata=metadata,
            claim_hint=claim,
        )
        if not payload.get("completed"):
            continue
        completion = (
            payload.get("record", {})
            if isinstance(payload.get("record"), dict)
            else {}
        )
        ack_metadata = native_relay_completion_ack_metadata(completion)
        ack_metadata["recovered_from_claim_observation"] = True
        if kind == "solver":
            ack_solver_trigger(root, key, thread_id, effective_status, ack_metadata)
        else:
            ack_tester_trigger(root, key, thread_id, effective_status, ack_metadata)
        sent_by_kind[kind][key] = {
            **ack_metadata,
            "status": effective_status,
            "thread_id": thread_id,
        }
        append_daemon_runtime_event(
            root,
            "native_relay_claim_observation_recovered",
            {
                "entry_id": entry_id,
                "claim_id": claim_id,
                "type": kind,
                "key": key,
                "thread_id": thread_id,
                "turn_id": turn_id,
                "turn_status": turn_status,
                "claimed_at": str(claim.get("claimed_at") or ""),
            },
        )
        recovered_entries.add(entry_id)
        unresolved = {
            key: value
            for key, value in unresolved.items()
            if str(value.get("entry_id") or value.get("id") or "") != entry_id
        }
        recovered += 1
        if recovered >= max(1, int(max_recoveries)):
            break
    recovery_state["updated_at"] = utc_now_iso()
    recovery_state["unresolved_count"] = len(unresolved)
    recovery_state["unresolved"] = unresolved
    write_json_atomic(
        state_dir / "native_relay_claim_recovery_state.json",
        recovery_state,
        ensure_ascii=True,
    )
    return recovered


def relay_completion_is_acknowledged(
    existing: dict[str, object],
    completion: dict[str, object],
) -> bool:
    if not existing:
        return False
    turn_id = str(completion.get("turn_id") or "")
    if turn_id and str(existing.get("turn_id") or "") != turn_id:
        return False
    completed_status = str(completion.get("status") or "")
    existing_status = normalized_trigger_status(existing)
    if completed_status == "failed":
        return existing_status == "failed"
    return existing_status in {"sent", "delivered", "active", "completed"}


def reconcile_native_relay_completion_acks(
    root: Path,
    *,
    max_recoveries: int = 3,
    max_age_seconds: int = 900,
) -> int:
    """Finish relay ack transactions interrupted after durable claim completion."""
    state_dir = root / "TestUtils" / "tester_daemon"
    records = read_recent_jsonl(state_dir / "native_relay_claim_events.jsonl")
    if not records:
        return 0
    solver_state = read_trigger_ack_state(root)
    tester_state = read_tester_trigger_ack_state(root)
    solver_sent = (
        solver_state.get("sent", {})
        if isinstance(solver_state.get("sent"), dict)
        else {}
    )
    tester_sent = (
        tester_state.get("sent", {})
        if isinstance(tester_state.get("sent"), dict)
        else {}
    )
    current = datetime.now(timezone.utc)
    recovered = 0
    seen: set[tuple[str, str]] = set()
    for record in reversed(records):
        if str(record.get("event") or "") != "native_relay_completed":
            continue
        kind = str(record.get("type") or "")
        key = str(record.get("key") or "")
        thread_id = str(record.get("thread_id") or "")
        status = str(record.get("status") or "")
        turn_id = str(record.get("turn_id") or "")
        identity = (kind, key)
        if identity in seen:
            continue
        seen.add(identity)
        if kind not in {"solver", "tester"} or not key or not thread_id:
            continue
        if status not in {"sent", "delivered", "active", "completed", "failed"}:
            continue
        completed_at = parse_timestamp(str(record.get("completed_at") or record.get("time") or ""))
        if completed_at is None:
            continue
        age = (current - completed_at.astimezone(timezone.utc)).total_seconds()
        if age < 0 or age > max_age_seconds:
            continue
        sent = solver_sent if kind == "solver" else tester_sent
        existing = sent.get(key) if isinstance(sent.get(key), dict) else {}
        if relay_completion_is_acknowledged(existing, record):
            continue
        metadata = native_relay_completion_ack_metadata(record)
        metadata["recovered_from_completion_journal"] = True
        metadata["relay_completion_entry_id"] = str(record.get("entry_id") or "")
        if kind == "solver":
            ack_solver_trigger(root, key, thread_id, status, metadata)
            solver_sent[key] = {**metadata, "status": status, "thread_id": thread_id}
        else:
            ack_tester_trigger(root, key, thread_id, status, metadata)
            tester_sent[key] = {**metadata, "status": status, "thread_id": thread_id}
        append_daemon_runtime_event(
            root,
            "native_relay_ack_recovered",
            {
                "entry_id": str(record.get("entry_id") or ""),
                "type": kind,
                "key": key,
                "thread_id": thread_id,
                "status": status,
                "turn_id": turn_id,
                "completed_at": str(record.get("completed_at") or ""),
            },
        )
        recovered += 1
        if recovered >= max(1, int(max_recoveries)):
            break
    return recovered


def native_relay_complete(args: argparse.Namespace) -> int:
    metadata: dict[str, object] = {}
    pre_delivery_turn_id = getattr(args, "pre_delivery_turn_id", None)
    if pre_delivery_turn_id is not None:
        metadata["pre_delivery_turn_id"] = pre_delivery_turn_id
    if args.turn_id:
        metadata["turn_id"] = args.turn_id
    if args.delivery:
        metadata["delivery"] = args.delivery
    if args.ide_panel_visible:
        metadata["ide_panel_visible"] = True
        metadata["ide_panel_visibility"] = "confirmed_by_native_relay"
        if args.status in {"active", "completed"}:
            metadata["completion_unconfirmed"] = False
            metadata["control_plane_unavailable"] = False
            metadata["orphaned_delivery_owner"] = False
            metadata["delivery_started_at"] = utc_now_iso()
            if args.turn_id:
                metadata["native_id"] = args.turn_id
                metadata["native_status"] = (
                    "inProgress" if args.status == "active" else "completed"
                )
                metadata["turn_status"] = metadata["native_status"]
    if args.error:
        metadata["error"] = args.error
    metadata_json = args.metadata_json
    if not metadata_json and args.metadata_json_env:
        metadata_json = os.environ.get(args.metadata_json_env, "")
    if metadata_json:
        try:
            extra = json.loads(metadata_json)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"invalid --metadata-json: {exc}") from exc
        if not isinstance(extra, dict):
            raise SystemExit("--metadata-json must decode to a JSON object")
        metadata.update(extra)

    payload = complete_native_relay_claim(
        ROOT,
        entry_id=args.entry_id,
        status=args.status,
        turn_id=args.turn_id,
        delivery=args.delivery,
        ide_panel_visible=args.ide_panel_visible,
        metadata=metadata,
    )
    if not payload.get("completed"):
        print_json(payload)
        return 2
    record = payload.get("record", {}) if isinstance(payload.get("record"), dict) else {}
    effective_status = str(payload.get("status", args.status) or args.status)
    key = str(record.get("key", "") or "")
    thread_id = str(record.get("thread_id", "") or "")
    kind = str(record.get("type", "") or "")
    if not key or not thread_id or kind not in {"solver", "tester"}:
        print_json({**payload, "ack_written": False, "ack_error": "claim missing key/thread/type"})
        return 2
    ack_metadata = dict(metadata)
    if payload.get("proof_error"):
        for field in (
            "ide_panel_visible",
            "ide_panel_visibility",
            "completion_unconfirmed",
            "control_plane_unavailable",
            "orphaned_delivery_owner",
            "delivery_started_at",
        ):
            ack_metadata.pop(field, None)
        ack_metadata["failure_kind"] = record.get("failure_kind", "native_turn_not_created")
        ack_metadata["error"] = record.get("error", payload.get("proof_error", ""))
        ack_metadata["requested_status"] = record.get("requested_status", args.status)
    if record.get("model"):
        ack_metadata["model"] = record.get("model")
    if record.get("thinking"):
        ack_metadata["thinking"] = record.get("thinking")
    for field in (
        "prompt_profile",
        "prompt_mode",
        "prompt_chars",
        "delivery_ordinal",
        "contract_revision",
        "reanchor_reason",
        "correction_reason",
    ):
        if record.get(field) not in {None, ""}:
            ack_metadata[field] = record.get(field)
    if record.get("claim_recovery"):
        ack_metadata["claim_recovery"] = record.get("claim_recovery")
    if record.get("claimed_at"):
        ack_metadata["claimed_at"] = record.get("claimed_at")
    if payload.get("recovered_from_outbox"):
        ack_metadata["recovered_from_outbox"] = True
    if payload.get("recovered_from_expired_claim"):
        ack_metadata["recovered_from_expired_claim"] = True
    native_delivery_unconfirmed = (
        ack_metadata.get("completion_unconfirmed") is True
        or str(ack_metadata.get("failure_kind", "") or "")
        == "native_turn_not_created"
        or str(ack_metadata.get("error", "") or "")
        in {
            "post-delivery-proof-unavailable",
            "post-delivery-turn-already-existed",
            "native-turn-id-did-not-change",
        }
    )
    if effective_status == "failed" or (
        effective_status == "delivered" and native_delivery_unconfirmed
    ):
        ack_metadata.setdefault(
            "delivery_retry_reason",
            (
                "native_delivery_unconfirmed"
                if native_delivery_unconfirmed
                else "native_delivery_failed"
            ),
        )
        ack_metadata.setdefault(
            "delivery_retry_after",
            (datetime.now(timezone.utc) + timedelta(seconds=120))
            .replace(microsecond=0)
            .isoformat(),
        )
    if effective_status == "delivered" and native_delivery_unconfirmed:
        # A successful send can temporarily outrun thread observation.  The
        # new delivery must not inherit terminal proof from the prior turn;
        # pre_delivery_turn_id remains the fence until a distinct turn appears.
        ack_metadata["reset_native_turn_proof"] = True
    if kind == "solver":
        ack_solver_trigger(ROOT, key, thread_id, effective_status, ack_metadata)
    else:
        ack_tester_trigger(ROOT, key, thread_id, effective_status, ack_metadata)
    record_app_side_native_relay(
        kind,
        argparse.Namespace(key=key, thread_id=thread_id, status=effective_status),
        ack_metadata,
    )
    outbox = write_native_relay_outbox(ROOT)
    print_json({**payload, "ack_written": True, "outbox_entry_count": outbox.get("entry_count", 0)})
    return 2 if payload.get("proof_error") else 0


def native_relay_defer_active(args: argparse.Namespace) -> int:
    """Release a claim when a live pre-read proves the existing turn is active.

    This is observation-only: no Solver/Tester prompt was sent.  It prevents a
    stale retry-ready plan from appending duplicate user messages to the same
    IDE turn while preserving an auditable active fence for the current gate.
    """
    turn_status = str(args.turn_status or "")
    if turn_status not in {"active", "inProgress", "running"}:
        print_json(
            {
                "updated_at": utc_now_iso(),
                "entry_id": args.entry_id,
                "status": "invalid-turn-status",
                "turn_status": turn_status,
                "deferred": False,
            }
        )
        return 2
    turn_id = str(args.turn_id or "")
    if not turn_id or turn_id.lower().startswith(("item-", "exec-", "call-")):
        print_json(
            {
                "updated_at": utc_now_iso(),
                "entry_id": args.entry_id,
                "status": "invalid-turn-id",
                "turn_id": turn_id,
                "deferred": False,
            }
        )
        return 2

    outbox = build_native_relay_outbox(ROOT)
    entry = next(
        (
            item
            for item in outbox.get("entries", [])
            if isinstance(item, dict) and str(item.get("id", "") or "") == args.entry_id
        ),
        None,
    )
    if not isinstance(entry, dict):
        print_json(
            {
                "updated_at": utc_now_iso(),
                "entry_id": args.entry_id,
                "status": "missing-outbox-entry",
                "deferred": False,
            }
        )
        return 2

    key = str(entry.get("key", "") or "")
    thread_id = str(entry.get("thread_id", "") or "")
    kind = str(entry.get("type", "") or "")
    if not key or not thread_id or kind not in {"solver", "tester"}:
        print_json(
            {
                "updated_at": utc_now_iso(),
                "entry_id": args.entry_id,
                "status": "invalid-outbox-entry",
                "deferred": False,
            }
        )
        return 2

    observed_at = utc_now_iso()
    metadata: dict[str, object] = {
        "turn_id": turn_id,
        "native_id": turn_id,
        "turn_status": turn_status,
        "native_status": turn_status,
        "last_observed_at": observed_at,
        "observation_mode": "codex-app-read-thread",
        "delivery": "codex-app-read-thread-observation",
        "ide_panel_visible": True,
        "ide_panel_visibility": "confirmed_by_native_relay",
        "observation_only": True,
        "pre_delivery_turn_id": str(entry.get("pre_delivery_turn_id", "") or ""),
    }
    for name, value in (
        ("latest_has_agent_output", args.has_agent_output),
        ("latest_user_only_turn", args.user_only),
    ):
        if value == "true":
            metadata[name] = True
        elif value == "false":
            metadata[name] = False
    for field in (
        "model",
        "thinking",
        "prompt_profile",
        "prompt_mode",
        "prompt_chars",
        "delivery_ordinal",
        "contract_revision",
        "reanchor_reason",
        "correction_reason",
    ):
        if entry.get(field) not in {None, ""}:
            metadata[field] = entry.get(field)

    if kind == "solver":
        ack_solver_trigger(ROOT, key, thread_id, "active", metadata)
    else:
        ack_tester_trigger(ROOT, key, thread_id, "active", metadata)
    released = release_native_relay_claims(
        ROOT,
        keys={key},
        reason="live pre-delivery Codex turn observed active; no prompt sent",
    )
    refreshed = write_native_relay_outbox(ROOT)
    print_json(
        {
            "updated_at": observed_at,
            "entry_id": args.entry_id,
            "type": kind,
            "op": entry.get("op", ""),
            "key": key,
            "thread_id": thread_id,
            "turn_id": turn_id,
            "turn_status": turn_status,
            "status": "active-observed",
            "deferred": True,
            "prompt_sent": False,
            "released_claim_count": released.get("released_count", 0),
            "outbox_entry_count": refreshed.get("entry_count", 0),
        }
    )
    return 0


def recover_native_sessions(args: argparse.Namespace) -> int:
    config_full_path = ROOT / args.config if not Path(args.config).is_absolute() else Path(args.config)
    config = load_config(config_full_path, apply_completion_markers=True)
    reason = args.reason or "Codex Desktop restarted; prior native session turns are no longer live"
    payload = recover_codex_sessions(ROOT, config, reason=reason, dry_run=args.dry_run)
    if not args.dry_run:
        append_daemon_runtime_event(
            ROOT,
            "codex_sessions_recovered",
            {
                "reason": reason,
                "recovered_count": payload.get("recovered_count", 0),
                "released_claim_count": payload.get("released_claim_count", 0),
                "active_operators": payload.get("active_operators", []),
            },
        )
    print_json(payload)
    return 0


def import_thread_observations(args: argparse.Namespace) -> int:
    if args.path:
        data = json.loads(Path(args.path).read_text(encoding="utf-8"))
    elif args.observations_json:
        data = json.loads(args.observations_json)
    elif args.observations_json_env:
        data = json.loads(os.environ.get(args.observations_json_env, ""))
    else:
        raise SystemExit("provide --path, --observations-json, or --observations-json-env")
    if not isinstance(data, dict):
        raise SystemExit("thread observations payload must be a JSON object")
    threads = data.get("threads")
    if not isinstance(threads, list):
        raise SystemExit("thread observations payload must contain a threads array")
    state_dir = ROOT / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    obs_path = state_dir / "solver_thread_observations.json"
    payload = dict(data)
    payload.setdefault("updated_at", utc_now_iso())
    if obs_path.exists():
        try:
            previous = json.loads(obs_path.read_text(encoding="utf-8"))
        except Exception:
            previous = {}
        previous_threads = previous.get("threads")
        if isinstance(previous_threads, list):
            merged: dict[tuple[str, str, str], dict[str, object]] = {}
            for item in previous_threads:
                if not isinstance(item, dict):
                    continue
                key = (
                    str(item.get("role", "solver") or "solver"),
                    str(item.get("op", "") or ""),
                    str(item.get("thread_id", "") or ""),
                )
                if key[1] and key[2]:
                    merged[key] = dict(item)
            for item in threads:
                if not isinstance(item, dict):
                    continue
                key = (
                    str(item.get("role", "solver") or "solver"),
                    str(item.get("op", "") or ""),
                    str(item.get("thread_id", "") or ""),
                )
                if key[1] and key[2]:
                    merged[key] = dict(item)
            payload["threads"] = list(merged.values())
    obs_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    try:
        from ascendop_daemon.legacy.bridge import render_thread_observations

        rendered = render_thread_observations(payload)
    except Exception:
        rendered = "# Solver Thread Observations\n\n- imported_at: " + str(payload.get("updated_at", "")) + "\n"
    (state_dir / "SOLVER_THREAD_OBSERVATIONS.md").write_text(rendered, encoding="utf-8")
    if args.sync_ack:
        sync_solver_ack_from_thread_observations(ROOT)
    print(f"solver thread observations imported: {state_dir / 'solver_thread_observations.json'}")
    return 0


def register_solver_thread(args: argparse.Namespace) -> int:
    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = ROOT / config_path
    data = read_json_dict(config_path)
    solver_threads = data.get("solver_threads")
    if not isinstance(solver_threads, dict):
        solver_threads = {}
        data["solver_threads"] = solver_threads
    old_thread_id = str(solver_threads.get(args.op, "") or "")
    solver_threads[args.op] = args.thread_id
    operator_sessions = data.get("operator_sessions")
    if isinstance(operator_sessions, dict):
        session = operator_sessions.get(args.op)
        if isinstance(session, dict):
            session["solver_thread_id"] = args.thread_id
    config_path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    state_dir = ROOT / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    cleared_keys: list[str] = []
    prompt_protocol_reset = False
    if args.clear_op_trigger_state:
        ack_path = state_dir / "solver_trigger_ack_state.json"
        ack_state = read_json_dict(ack_path)
        sent = ack_state.get("sent", {}) if isinstance(ack_state.get("sent"), dict) else {}
        kept = {}
        for key, value in sent.items():
            if str(key).startswith(f"{args.op}|"):
                cleared_keys.append(str(key))
                continue
            kept[str(key)] = value
        ack_path.write_text(
            json.dumps({"updated_at": utc_now_iso(), "sent": kept}, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        obs_path = state_dir / "solver_thread_observations.json"
        observations = read_json_dict(obs_path)
        threads = observations.get("threads", [])
        if isinstance(threads, list):
            observations["threads"] = [
                item for item in threads if not (isinstance(item, dict) and item.get("op") == args.op)
            ]
            observations["updated_at"] = utc_now_iso()
            obs_path.write_text(json.dumps(observations, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        prune_registered_solver_state(state_dir, args.op, args.thread_id)
        prompt_protocol_reset = reset_registered_prompt_protocol(state_dir, args.op, "solver")
        write_native_relay_outbox(ROOT)
    event = {
        "time": utc_now_iso(),
        "event": "solver_thread_registered",
        "op": args.op,
        "old_thread_id": old_thread_id,
        "new_thread_id": args.thread_id,
        "config": str(config_path.relative_to(ROOT) if config_path.is_relative_to(ROOT) else config_path),
        "cleared_trigger_keys": cleared_keys,
        "prompt_protocol_reset": prompt_protocol_reset,
    }
    with (state_dir / "solver_thread_replacements.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(event, ensure_ascii=False) + "\n")
    print(f"solver thread registered: {args.op} {old_thread_id or '-'} -> {args.thread_id}")
    return 0


def register_tester_thread(args: argparse.Namespace) -> int:
    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = ROOT / config_path
    data = read_json_dict(config_path)
    operator_sessions = data.get("operator_sessions")
    if not isinstance(operator_sessions, dict):
        operator_sessions = {}
        data["operator_sessions"] = operator_sessions
    session = operator_sessions.get(args.op)
    if not isinstance(session, dict):
        session = {"enabled": True, "roles": {"casegen": True, "submit": False}}
        operator_sessions[args.op] = session
    roles = session.get("roles")
    if not isinstance(roles, dict):
        roles = {}
        session["roles"] = roles
    roles["casegen"] = True
    roles["submit"] = False
    old_thread_id = str(session.get("tester_thread_id", "") or "")
    session["tester_thread_id"] = args.thread_id
    config_path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    state_dir = ROOT / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    prompt_protocol_reset = False
    if args.clear_op_trigger_state:
        ack_path = state_dir / "tester_trigger_ack_state.json"
        ack_state = read_json_dict(ack_path)
        sent = ack_state.get("sent", {}) if isinstance(ack_state.get("sent"), dict) else {}
        kept = {
            str(key): value
            for key, value in sent.items()
            if not str(key).startswith(f"{args.op}|")
        }
        ack_path.write_text(
            json.dumps({"updated_at": utc_now_iso(), "sent": kept}, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        plan_path = state_dir / "tester_trigger_plan.json"
        plan = read_json_dict(plan_path)
        triggers = plan.get("triggers", [])
        if isinstance(triggers, list):
            plan["triggers"] = [
                item for item in triggers if not (isinstance(item, dict) and item.get("op") == args.op)
            ]
            plan["updated_at"] = utc_now_iso()
            plan_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        prompt_protocol_reset = reset_registered_prompt_protocol(state_dir, args.op, "tester")
        write_native_relay_outbox(ROOT)
    event = {
        "time": utc_now_iso(),
        "event": "tester_thread_registered",
        "op": args.op,
        "old_thread_id": old_thread_id,
        "new_thread_id": args.thread_id,
        "config": str(config_path.relative_to(ROOT) if config_path.is_relative_to(ROOT) else config_path),
        "prompt_protocol_reset": prompt_protocol_reset,
    }
    with (state_dir / "tester_thread_replacements.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(event, ensure_ascii=False) + "\n")
    print(f"tester thread registered: {args.op} {old_thread_id or '-'} -> {args.thread_id}")
    return 0


def reset_registered_prompt_protocol(state_dir: Path, op: str, role: str) -> bool:
    protocol_path = state_dir / "session_prompt_protocol.json"
    protocol = read_json_dict(protocol_path)
    sessions = protocol.get("sessions")
    if not isinstance(sessions, dict):
        return False
    session_key = f"{op}:{role}"
    if session_key not in sessions:
        return False
    del sessions[session_key]
    protocol["updated_at"] = utc_now_iso()
    protocol_path.write_text(
        json.dumps(protocol, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return True


def prune_registered_solver_state(state_dir: Path, op: str, thread_id: str) -> None:
    plan_path = state_dir / "solver_trigger_plan.json"
    plan = read_json_dict(plan_path)
    triggers = plan.get("triggers", [])
    if isinstance(triggers, list):
        plan["triggers"] = [
            item for item in triggers if not (isinstance(item, dict) and item.get("op") == op)
        ]
        plan["updated_at"] = utc_now_iso()
        plan_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    replacement_path = state_dir / "solver_replacement_plan.json"
    replacement = read_json_dict(replacement_path)
    replacements = replacement.get("replacements", [])
    if isinstance(replacements, list):
        replacement["replacements"] = [
            item for item in replacements if not (isinstance(item, dict) and item.get("op") == op)
        ]
        replacement["replacement_required_count"] = len(replacement["replacements"])
        replacement["updated_at"] = utc_now_iso()
        replacement_path.write_text(
            json.dumps(replacement, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        (state_dir / "SOLVER_REPLACEMENT_PLAN.md").write_text(
            render_solver_replacement_plan(replacement),
            encoding="utf-8",
        )

    session_path = state_dir / "solver_session_status.json"
    session = read_json_dict(session_path)
    required_reads = session.get("required_thread_reads", [])
    if isinstance(required_reads, list):
        updated_reads: list[object] = []
        seen = False
        for item in required_reads:
            if isinstance(item, dict) and item.get("op") == op:
                updated_reads.append({"op": op, "thread_id": thread_id})
                seen = True
            else:
                updated_reads.append(item)
        if not seen:
            updated_reads.append({"op": op, "thread_id": thread_id})
        session["required_thread_reads"] = updated_reads
    active_gates = session.get("active_solver_gates", [])
    if isinstance(active_gates, list):
        session["active_solver_gates"] = [
            item for item in active_gates if not (isinstance(item, dict) and item.get("op") == op)
        ]
    if session:
        session["updated_at"] = utc_now_iso()
        session_path.write_text(json.dumps(session, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def supervise(args: argparse.Namespace) -> int:
    result = supervise_runtime(
        ROOT,
        SupervisorOptions(
            config_path=args.config,
            mode=args.mode,
            write_state=args.write_state,
            allow_live_execute=args.allow_live_execute,
            clear_stop=args.clear_stop,
            max_heartbeat_age_seconds=args.max_heartbeat_age_seconds,
            bridge_max_heartbeat_age_seconds=args.bridge_max_heartbeat_age_seconds,
            replace_stale_lock_after_seconds=args.replace_stale_lock_after_seconds,
            ensure_bridge=not args.no_bridge,
            bridge_max_workers=args.bridge_max_workers,
            bridge_wait_seconds=args.bridge_wait_seconds,
            dry_run=args.dry_run,
        ),
    )
    print_json(result)
    return 0


def operator_plugin(args: argparse.Namespace) -> int:
    config_path = ROOT / args.config if not Path(args.config).is_absolute() else Path(args.config)
    try:
        if args.drain:
            result = request_operator_drain(ROOT, config_path, args.op)
        else:
            result = update_operator_enabled(
                ROOT,
                config_path,
                args.op,
                bool(args.enable),
                require_quiescent=not args.allow_inflight_disable,
            )
    except (OSError, ValueError, RuntimeError, json.JSONDecodeError) as exc:
        print_json({"ok": False, "op": args.op, "error": str(exc)})
        return 2
    print_json({"ok": True, **result})
    return 0


def operator_plugin_status(args: argparse.Namespace) -> int:
    config_path = ROOT / args.config if not Path(args.config).is_absolute() else Path(args.config)
    try:
        result = build_operator_plugin_status(ROOT, config_path)
    except (OSError, ValueError, RuntimeError, json.JSONDecodeError) as exc:
        print_json({"ok": False, "error": str(exc)})
        return 2
    print_json({"ok": True, **result})
    return 0


def write_supervisor_loop_heartbeat(args: argparse.Namespace, *, iterations: int) -> None:
    state_dir = ROOT / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "time": utc_now_iso(),
        "pid": os.getpid(),
        "config": str(args.config),
        "mode": str(args.mode),
        "iterations": iterations,
        "interval_seconds": float(args.interval_seconds),
    }
    (state_dir / "supervisor_loop_heartbeat.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def effective_supervisor_loop_interval(args: argparse.Namespace) -> float:
    requested = float(args.interval_seconds or 0.0)
    if requested > 0:
        return max(1.0, requested)
    config_path = ROOT / args.config if not Path(args.config).is_absolute() else Path(args.config)
    config = load_config(config_path, apply_completion_markers=True)
    configured = config.policy.get(
        "supervise_loop_interval_seconds",
        config.policy.get("run_interval_seconds", 60.0),
    )
    return max(1.0, float(configured or 60.0))


def supervise_loop(args: argparse.Namespace) -> int:
    interval_seconds = effective_supervisor_loop_interval(args)
    args.interval_seconds = interval_seconds
    # Clear a previous stop fence synchronously at coordinator entry.  Passing
    # clear_stop into a slow first supervise iteration lets a concurrent stop
    # request be erased later when the daemon child finally starts.
    if args.clear_stop:
        clear_stop_request(ROOT)
        args.clear_stop = False
    config_path = ROOT / args.config if not Path(args.config).is_absolute() else Path(args.config)
    try:
        maintenance_policy = load_config(config_path, apply_completion_markers=True).policy
    except (OSError, ValueError, json.JSONDecodeError):
        maintenance_policy = {}
    run_runtime_maintenance(ROOT, maintenance_policy, actor="supervisor-start")
    iterations = 0
    try:
        lock = NamedProcessLock(
            ROOT,
            "supervisor_loop",
            stale_after_seconds=args.replace_stale_lock_after_seconds,
        )
        with lock:
            while True:
                generation = flow_v3_process_generation_status(ROOT)
                if not bool(generation["matches"]):
                    record_flow_v3_process_generation_drift(
                        "resident-supervisor"
                    )
                    return 75
                run_runtime_maintenance(ROOT, maintenance_policy, actor="supervisor-loop")
                write_supervisor_loop_heartbeat(args, iterations=iterations)
                try:
                    result = supervise_runtime(
                        ROOT,
                        SupervisorOptions(
                            config_path=args.config,
                            mode=args.mode,
                            write_state=args.write_state,
                            allow_live_execute=args.allow_live_execute,
                            clear_stop=False,
                            max_heartbeat_age_seconds=args.max_heartbeat_age_seconds,
                            bridge_max_heartbeat_age_seconds=args.bridge_max_heartbeat_age_seconds,
                            replace_stale_lock_after_seconds=args.replace_stale_lock_after_seconds,
                            ensure_bridge=not args.no_bridge,
                            bridge_max_workers=args.bridge_max_workers,
                            bridge_wait_seconds=args.bridge_wait_seconds,
                            dry_run=args.dry_run,
                        ),
                    )
                    print_json(result)
                except Exception as exc:
                    append_daemon_runtime_event(
                        ROOT,
                        "supervisor_loop_iteration_exception",
                        {
                            "pid": os.getpid(),
                            "iteration": iterations,
                            "error": repr(exc),
                            "traceback": traceback.format_exc()[-4000:],
                        },
                    )
                    traceback.print_exc()
                if args.max_iterations and iterations + 1 >= args.max_iterations:
                    write_supervisor_loop_heartbeat(args, iterations=iterations + 1)
                    return 0
                iterations += 1
                if sleep_or_stop(ROOT, interval_seconds):
                    write_supervisor_loop_heartbeat(args, iterations=iterations)
                    return 0
    except RuntimeError as exc:
        append_daemon_runtime_event(
            ROOT,
            "supervisor_loop_lock_busy",
            {"pid": os.getpid(), "error": str(exc)},
        )
        print(f"supervisor-loop lock busy: {exc}", file=sys.stderr, flush=True)
        return 2


def supervise_launch(args: argparse.Namespace) -> int:
    result = start_supervisor_loop(
        ROOT,
        SupervisorOptions(
            config_path=args.config,
            mode=args.mode,
            write_state=args.write_state,
            allow_live_execute=args.allow_live_execute,
            clear_stop=args.clear_stop,
            max_heartbeat_age_seconds=args.max_heartbeat_age_seconds,
            bridge_max_heartbeat_age_seconds=args.bridge_max_heartbeat_age_seconds,
            replace_stale_lock_after_seconds=args.replace_stale_lock_after_seconds,
            ensure_bridge=not args.no_bridge,
            bridge_max_workers=args.bridge_max_workers,
            bridge_wait_seconds=args.bridge_wait_seconds,
            dry_run=args.dry_run,
        ),
        interval_seconds=args.interval_seconds,
    )
    print_json(result)
    return 0


def ack_trigger(args: argparse.Namespace) -> int:
    metadata: dict[str, object] = {}
    if args.turn_id:
        metadata["turn_id"] = args.turn_id
    if args.delivery:
        metadata["delivery"] = args.delivery
    if args.ide_panel_visible:
        metadata["ide_panel_visible"] = True
        metadata["ide_panel_visibility"] = "confirmed_by_native_relay"
    metadata_json = args.metadata_json
    if not metadata_json and args.metadata_json_env:
        metadata_json = os.environ.get(args.metadata_json_env, "")
    if metadata_json:
        try:
            extra = json.loads(metadata_json)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"invalid --metadata-json: {exc}") from exc
        if not isinstance(extra, dict):
            raise SystemExit("--metadata-json must decode to a JSON object")
        metadata.update(extra)
    path = ack_solver_trigger(ROOT, args.key, args.thread_id, args.status, metadata)
    record_app_side_native_relay("solver", args, metadata)
    print(f"solver trigger ack written: {path}")
    return 0


def ack_tester_trigger_cmd(args: argparse.Namespace) -> int:
    metadata: dict[str, object] = {}
    if args.turn_id:
        metadata["turn_id"] = args.turn_id
    if args.delivery:
        metadata["delivery"] = args.delivery
    if args.ide_panel_visible:
        metadata["ide_panel_visible"] = True
        metadata["ide_panel_visibility"] = "confirmed_by_native_relay"
    metadata_json = args.metadata_json
    if not metadata_json and args.metadata_json_env:
        metadata_json = os.environ.get(args.metadata_json_env, "")
    if metadata_json:
        try:
            extra = json.loads(metadata_json)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"invalid --metadata-json: {exc}") from exc
        if not isinstance(extra, dict):
            raise SystemExit("--metadata-json must decode to a JSON object")
        metadata.update(extra)
    path = ack_tester_trigger(ROOT, args.key, args.thread_id, args.status, metadata)
    record_app_side_native_relay("tester", args, metadata)
    print(f"tester trigger ack written: {path}")
    return 0


def record_app_side_native_relay(kind: str, args: argparse.Namespace, metadata: dict[str, object]) -> None:
    delivery = str(metadata.get("delivery", "") or "")
    if delivery != "codex-app-send-message-to-thread":
        return
    if metadata.get("ide_panel_visible") is not True:
        return
    state_dir = ROOT / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    now = utc_now_iso()
    record = {
        "updated_at": now,
        "kind": kind,
        "key": args.key,
        "thread_id": args.thread_id,
        "status": args.status,
        "delivery": delivery,
        "ide_panel_visible": True,
        "ide_panel_visibility": metadata.get("ide_panel_visibility", "confirmed_by_native_relay"),
        "relay_contract": "codex_app.send_message_to_thread",
        "app_side_required": True,
    }
    if metadata.get("turn_id"):
        record["turn_id"] = metadata["turn_id"]
    (state_dir / "native_relay_app_side_status.json").write_text(
        json.dumps(record, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    with (state_dir / "native_relay_app_side_events.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def execute_worker(args: argparse.Namespace) -> int:
    return run_execute_worker(Path(args.payload))


def observability_worker(args: argparse.Namespace) -> int:
    return run_observability_worker_payload(Path(args.payload))


def engine_candidate_worker(args: argparse.Namespace) -> int:
    return run_engine_candidate_worker(Path(args.config))


def engine_admission_cmd(args: argparse.Namespace) -> int:
    store = EngineAdmissionStore(ROOT)
    action = args.engine_action
    payload: object
    if action == "status":
        engine_snapshot = load_json_argument(args.engine_snapshot) if args.engine_snapshot else None
        payload = store.snapshot(engine_snapshot=engine_snapshot)
    elif action == "configure":
        enabled = bool(args.enable)
        if args.disable:
            enabled = False
        payload = store.configure(
            enabled=enabled,
            target_inflight=args.target_inflight,
            draining=bool(args.draining and enabled),
            controller_owner=args.controller_owner,
            controller_token=args.controller_token,
            lease_seconds=args.lease_seconds,
        )
    elif action == "release-controller":
        payload = store.release_controller_lease(
            args.controller_owner,
            args.controller_token,
        )
    elif action == "begin":
        payload = store.begin_admission(load_json_argument(args.json_file))
    elif action == "accept":
        payload = store.record_acceptance(load_json_argument(args.json_file))
    elif action == "reconcile":
        payload = store.reconcile_engine_snapshot(load_json_argument(args.json_file))
    elif action == "terminal":
        payload = store.record_terminal_manifest(load_json_argument(args.json_file))
    elif action == "return":
        payload = store.record_return(args.engine_job_id, args.receipt_id)
    else:
        raise SystemExit(f"unsupported engine admission action: {action}")
    print_json(payload)
    return 0


def load_json_argument(path_text: str) -> dict[str, object]:
    path = Path(path_text)
    if not path.is_absolute():
        path = ROOT / path
    raw = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(raw, dict):
        raise SystemExit(f"JSON input must be an object: {path}")
    return raw


def control_database_cmd(args: argparse.Namespace) -> int:
    database_path = workspace_path(args.database)
    database = ControlDatabase(database_path)
    action = args.control_action
    if action == "init":
        database.initialize()
        payload: object = database.status(event_limit=0)
    elif action == "status":
        payload = database.status(event_limit=args.event_limit)
    elif action == "sync":
        config = load_config(workspace_path(args.config))
        registry = SystemRegistry.load(workspace_path(args.registry))
        payload = database.reconcile(config, registry)
    elif action == "route-explain":
        registry = SystemRegistry.load(workspace_path(args.registry))
        requirements = (
            load_json_argument(args.requirements_file)
            if args.requirements_file
            else json.loads(args.requirements_json or "{}")
        )
        if not isinstance(requirements, dict):
            raise SystemExit("route requirements must be a JSON object")
        payload = database.explain_route(requirements, registry)
    elif action == "route-request":
        registry = SystemRegistry.load(workspace_path(args.registry))
        payload = database.route_test_request(args.request_id, registry)
    elif action == "ingest-node-report":
        report_path = workspace_path(args.report)
        payload = database.ingest_node_report(
            load_json_argument(str(report_path)),
            source=args.source or str(report_path),
        )
    elif action == "ingest-node-reports":
        report_root = workspace_path(args.report_root)
        reports = sorted(report_root.glob(args.pattern))
        ingested = []
        failures = []
        for report_path in reports:
            try:
                ingested.append(
                    database.ingest_node_report(
                        load_json_argument(str(report_path)),
                        source=args.source or str(report_path),
                    )
                )
            except (
                ControlDatabaseError,
                OSError,
                ValueError,
                json.JSONDecodeError,
            ) as exc:
                failures.append(
                    {
                        "report": str(report_path),
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
        payload = {
            "report_root": str(report_root),
            "matched_count": len(reports),
            "ingested_count": len(ingested),
            "failure_count": len(failures),
            "ingested": ingested,
            "failures": failures,
        }
    elif action == "reconcile-nodes":
        payload = NodeReconciler(
            database,
            report_roots=[
                workspace_path(path)
                for path in (
                    args.report_root
                    or ["GitPartner/output/_control/nodes"]
                )
            ],
            ack_root=workspace_path(args.ack_root),
            pattern=args.pattern,
        ).run_once()
    elif action == "refresh-nodes":
        registry = SystemRegistry.load(workspace_path(args.registry))
        payload = GitPartnerNodeAdmissionReconciler(
            ROOT,
            database,
            registry,
            ack_root=workspace_path(args.ack_root),
        ).run_once(
            endpoint_ids=set(args.endpoint or []) or None,
        )
    elif action == "accept-node":
        registry = SystemRegistry.load(workspace_path(args.registry))
        ack = database.accept_node(args.node_id, registry)
        if args.ack_output:
            ack_path = workspace_path(args.ack_output)
            write_json_atomic(ack_path, ack)
            payload = {**ack, "ack_output": str(ack_path)}
        else:
            payload = ack
    elif action == "generate-requests":
        config = load_config(workspace_path(args.config), apply_completion_markers=True)
        registry = SystemRegistry.load(workspace_path(args.registry))
        reconciliation = database.reconcile(config, registry)
        pump_state = {} if args.include_managed else EnginePump(ROOT).read()
        generated = generate_test_requests(
            ROOT,
            config,
            database,
            registry,
            request_root=workspace_path(args.request_root),
            pump_state=pump_state,
            traffic_debt=traffic_debt(build_traffic_balance(ROOT, config)),
            execution_profile=str(
                config.policy.get("test_engine_execution_profile")
                or FUSED_SCALABLE_PROFILE
            ),
            limit=max(0, int(args.limit)),
            route=not args.no_route,
        )
        payload = {"reconciliation": reconciliation, **generated}
    else:
        raise SystemExit(f"unsupported control database action: {action}")
    print_json(payload)
    return 0


def endpoint_dispatch_cmd(args: argparse.Namespace) -> int:
    database = ControlDatabase(workspace_path(args.database))
    registry = SystemRegistry.load(workspace_path(args.registry))
    selected = set(args.endpoint or [])
    pool = build_dispatcher_pool(
        ROOT,
        database,
        registry,
        capacity=args.capacity,
        endpoint_ids=selected or None,
    )
    if args.dispatch_action == "once":
        payload = pool.run_once()
        print_json(payload)
        return 0
    deadline = (
        time.monotonic() + float(args.max_seconds)
        if float(args.max_seconds) > 0
        else None
    )
    ticks = 0
    latest: dict[str, object] = {}
    while deadline is None or time.monotonic() < deadline:
        latest = pool.run_once()
        ticks += 1
        time.sleep(max(0.05, float(args.interval_seconds)))
    print_json({"ticks": ticks, "latest": latest})
    return 0


def distributed_experiment_cmd(args: argparse.Namespace) -> int:
    database = ControlDatabase(workspace_path(args.database))
    registry = SystemRegistry.load(workspace_path(args.registry))
    config = load_config(workspace_path(args.config))
    database.reconcile(config, registry)
    runner = DistributedExperimentRunner(ROOT, database, registry)
    if args.experiment_action == "enqueue":
        tasks = load_experiment_tasks(args)
        GitPartnerNodeAdmissionReconciler(
            ROOT,
            database,
            registry,
            ack_root=workspace_path(
                str(
                    config.policy.get("control_plane_node_ack_root")
                    or "TestUtils/tester_daemon/node_acks"
                )
            ),
        ).run_once(endpoint_ids={task.endpoint_id for task in tasks})
        payload = runner.enqueue(args.experiment_id, tasks)
    elif args.experiment_action == "report":
        payload = runner.report(args.experiment_id)
    elif args.experiment_action == "run":
        selected = set(args.endpoint or [])
        GitPartnerNodeAdmissionReconciler(
            ROOT,
            database,
            registry,
            ack_root=workspace_path(
                str(
                    config.policy.get("control_plane_node_ack_root")
                    or "TestUtils/tester_daemon/node_acks"
                )
            ),
        ).run_once(endpoint_ids=selected or None)
        pool = build_dispatcher_pool(
            ROOT,
            database,
            registry,
            capacity=args.capacity,
            endpoint_ids=selected or None,
        )
        payload = runner.run_until_terminal(
            args.experiment_id,
            pool,
            timeout_seconds=args.timeout_seconds,
            poll_seconds=args.poll_seconds,
        )
    elif args.experiment_action == "acceptance":
        tasks = load_experiment_tasks(args)
        effective_capacity, capacity_mode = resolve_sustained_transport_capacity(
            args.capacity,
            len(tasks),
        )
        selected = set(args.endpoint or [])
        task_endpoints = {task.endpoint_id for task in tasks}
        if selected and not task_endpoints <= selected:
            raise SystemExit(
                "acceptance task endpoints must be included by --endpoint"
            )
        GitPartnerNodeAdmissionReconciler(
            ROOT,
            database,
            registry,
            ack_root=workspace_path(
                str(
                    config.policy.get("control_plane_node_ack_root")
                    or "TestUtils/tester_daemon/node_acks"
                )
            ),
        ).run_once(endpoint_ids=selected or task_endpoints)
        pool = build_dispatcher_pool(
            ROOT,
            database,
            registry,
            capacity=effective_capacity,
            endpoint_ids=selected or task_endpoints,
        )
        try:
            payload = runner.run_sustained_acceptance(
                args.experiment_id,
                tasks,
                pool,
                timeout_seconds=args.timeout_seconds,
                poll_seconds=args.poll_seconds,
                minimum_window_seconds=args.minimum_window_seconds,
                minimum_accepted_tasks=args.minimum_accepted_tasks,
                minimum_physical_devices=args.minimum_physical_devices,
                handoff_threshold_seconds=args.handoff_threshold_seconds,
                own_service_min_seconds=args.own_service_min_seconds,
                own_service_max_seconds=args.own_service_max_seconds,
                transport_capacity=effective_capacity,
                transport_capacity_mode=capacity_mode,
            )
        except (ControlDatabaseError, OSError, ValueError) as exc:
            print_json(
                {
                    "state": "failed",
                    "experiment_id": args.experiment_id,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            return 2
    else:
        raise SystemExit(
            f"unsupported distributed experiment action: {args.experiment_action}"
        )
    print_json(payload)
    if (
        args.experiment_action == "acceptance"
        and payload.get("sustained_acceptance", {}).get("status") != "passed"
    ):
        return 2
    return 0


def gp_diagnostic_cmd(args: argparse.Namespace) -> int:
    runner = GpDiagnosticRunner(ROOT)
    try:
        if args.diagnostic_action == "direct":
            if args.endpoint:
                registry = SystemRegistry.load(workspace_path(args.registry))
                target = endpoint_target(registry, args.endpoint)
            else:
                target = direct_target_from_descriptor(
                    ROOT,
                    workspace_path(args.descriptor),
                )
            payload = runner.direct_probe(
                target,
                request_id=args.request_id,
                wait_seconds=args.wait_seconds,
                poll_seconds=args.poll_seconds,
            )
        elif args.diagnostic_action == "via-link":
            registry = SystemRegistry.load(workspace_path(args.registry))
            payload = runner.link_probe(
                registry.management_link(args.link_id),
                request_id=args.request_id,
                wait_seconds=args.wait_seconds,
                poll_seconds=args.poll_seconds,
            )
        else:
            raise GpDiagnosticError(
                f"unsupported GP diagnostic action: {args.diagnostic_action}"
            )
    except (GpDiagnosticError, OSError, ValueError) as exc:
        print_json(
            {
                "state": "failed",
                "scheduler_eligible": False,
                "workflow_ingest": False,
                "error": f"{type(exc).__name__}: {exc}",
            }
        )
        return 2
    print_json(payload)
    if payload.get("state") == "failed":
        return 2
    if payload.get("state") == "uncertain":
        return 3
    return 0


def load_experiment_tasks(args: argparse.Namespace) -> list[ExperimentTask]:
    if args.tasks_file:
        raw = load_json_argument(args.tasks_file)
        values = raw.get("tasks", []) if isinstance(raw, dict) else []
        if not isinstance(values, list):
            raise SystemExit("experiment tasks file must contain a tasks list")
        tasks: list[ExperimentTask] = []
        for item in values:
            if not isinstance(item, dict):
                continue
            repeat = int(item.get("repeat", 1))
            if repeat < 1 or repeat > 10_000:
                raise SystemExit("experiment task repeat must be 1..10000")
            task = ExperimentTask(
                endpoint_id=str(item["endpoint_id"]),
                task_class=str(item["task_class"]),
                synthetic_duration_ms=int(item.get("synthetic_duration_ms", 0)),
                host_duration_ms=int(item.get("host_duration_ms", 0)),
                device_duration_ms=int(item.get("device_duration_ms", 0)),
                export_duration_ms=int(item.get("export_duration_ms", 0)),
                payload_size=int(item.get("payload_size", 0)),
                failure_mode=str(item.get("failure_mode") or "none"),
            )
            if len(tasks) + repeat > 10_000:
                raise SystemExit("experiment tasks file exceeds 10000 expanded tasks")
            tasks.extend([task] * repeat)
        return tasks
    tasks = []
    for raw in args.task or []:
        fields = raw.split(":")
        if len(fields) > 5 or len(fields) < 2:
            raise SystemExit(
                "--task must be ENDPOINT:CLASS[:DURATION_MS[:PAYLOAD_BYTES[:FAILURE]]]"
            )
        fields.extend([""] * (5 - len(fields)))
        tasks.append(
            ExperimentTask(
                endpoint_id=fields[0],
                task_class=fields[1],
                synthetic_duration_ms=int(fields[2] or 0),
                payload_size=int(fields[3] or 0),
                failure_mode=fields[4] or "none",
            )
        )
    if not tasks:
        raise SystemExit("enqueue requires --task or --tasks-file")
    return tasks


def engine_route_from_args(args: argparse.Namespace) -> EngineTransportRoute:
    registered_endpoint = str(args.registered_endpoint or "").strip()
    if registered_endpoint:
        try:
            return resolve_registered_engine_route(
                ROOT,
                registry_path=str(args.registry),
                endpoint_id=registered_endpoint,
            )
        except EngineRouteError as exc:
            raise SystemExit(str(exc)) from exc
    return EngineTransportRoute(
        gitpartner_repo=str(args.gitpartner_repo),
        result_worktree=str(args.gitpartner_repo),
        engine_root=str(args.engine_root),
        remote_root=str(args.remote_root),
        transport=str(args.transport),
        endpoint_id=str(args.endpoint_id),
        node_id=str(args.node_id),
        execution_environment_id=str(args.execution_environment_id),
        gateway_id=str(getattr(args, "gateway_id", "") or ""),
        transport_mode=str(getattr(args, "transport_mode", "") or ""),
        registration_generation=str(args.registration_generation),
        control_channel=str(args.control_channel),
        result_channel=str(args.result_channel),
        append_requests=False,
        duplex_lanes=False,
    )


def engine_transport_cmd(args: argparse.Namespace) -> int:
    route = engine_route_from_args(args)
    gp_repo = Path(route.gitpartner_repo)
    if not gp_repo.is_absolute():
        gp_repo = ROOT / gp_repo
    adapter = EngineTransportAdapter(
        ROOT,
        gitpartner_repo=gp_repo,
        result_gitpartner_repo=(
            ROOT / route.result_worktree
            if route.result_worktree
            and not Path(route.result_worktree).is_absolute()
            else Path(route.result_worktree)
            if route.result_worktree
            else gp_repo
        ),
        engine_root=route.engine_root,
        remote_root=route.remote_root,
        transport=route.transport,
        engine_wait_initial_grace_seconds=args.initial_grace_seconds,
        endpoint_id=route.endpoint_id,
        node_id=route.node_id,
        execution_environment_id=route.execution_environment_id,
        gateway_id=route.gateway_id,
        transport_mode=route.transport_mode,
        registration_generation=route.registration_generation,
        control_channel=route.control_channel,
        result_channel=route.result_channel,
        append_requests=route.append_requests,
        duplex_lanes=route.duplex_lanes,
        remote_gitpartner_repo=route.remote_gitpartner_repo,
        exchange_wait_ready_seconds=args.exchange_wait_ready_seconds,
    )
    if args.transport_action == "accept":
        spec_path = Path(args.spec)
        if not spec_path.is_absolute():
            spec_path = ROOT / spec_path
        payload_root = Path(args.payload_root) if args.payload_root else None
        if payload_root is not None and not payload_root.is_absolute():
            payload_root = ROOT / payload_root
        payload = adapter.accept(
            spec_path,
            request_id=args.request_id,
            engine_job_id=args.engine_job_id,
            payload_root=payload_root,
            wait_timeout_seconds=args.wait_timeout_seconds,
        )
    elif args.transport_action == "snapshot":
        payload = adapter.snapshot(
            request_id=args.request_id,
            wait_timeout_seconds=args.wait_timeout_seconds,
        )
    elif args.transport_action == "collect":
        payload = adapter.collect(
            request_id=args.request_id,
            engine_job_id=args.engine_job_id,
            receipt_id=args.receipt_id,
            wait_timeout_seconds=args.wait_timeout_seconds,
        )
    elif args.transport_action == "configure":
        payload = adapter.configure(
            request_id=args.request_id,
            max_inflight=args.max_inflight,
            draining=args.drain,
            standby_slots=args.standby_slots,
            active_job_slots=args.active_job_slots,
            host_slots=args.host_slots,
            export_slots=args.export_slots,
            return_backlog_soft_limit_bytes=args.return_backlog_soft_limit_bytes,
            return_backlog_hard_limit_bytes=args.return_backlog_hard_limit_bytes,
            return_backlog_soft_limit_jobs=args.return_backlog_soft_limit_jobs,
            return_backlog_hard_limit_jobs=args.return_backlog_hard_limit_jobs,
            wait_timeout_seconds=args.wait_timeout_seconds,
        )
    elif args.transport_action == "sync-code":
        payload = adapter.sync_code(
            request_id=args.request_id,
            wait_timeout_seconds=args.wait_timeout_seconds,
        )
        reconcile_request_id = (
            "engine-reconcile-"
            + hashlib.sha256(args.request_id.encode("utf-8")).hexdigest()[:16]
        )
        payload["engine_reconcile"] = adapter.snapshot(
            request_id=reconcile_request_id,
            wait_timeout_seconds=args.wait_timeout_seconds,
        )
        if route.endpoint_id:
            registry = SystemRegistry.load(workspace_path(args.registry))
            database = ControlDatabase(workspace_path(args.database))
            payload["node_refresh"] = GitPartnerNodeAdmissionReconciler(
                ROOT,
                database,
                registry,
                ack_root=workspace_path(args.ack_root),
            ).run_once(endpoint_ids={route.endpoint_id})
    elif args.transport_action == "sync-resident-runtime":
        payload = adapter.sync_resident_runtime(
            request_id=args.request_id,
            wait_timeout_seconds=args.wait_timeout_seconds,
            restart_delay_seconds=args.restart_delay_seconds,
        )
    elif args.transport_action == "reconcile-direct-request":
        payload = adapter.reconcile_direct_request(
            request_id=args.request_id,
            target_request_id=args.target_request_id,
            wait_timeout_seconds=args.wait_timeout_seconds,
        )
    elif args.transport_action == "restart-role":
        payload = adapter.restart_role(
            request_id=args.request_id,
            role=args.role,
            wait_timeout_seconds=args.wait_timeout_seconds,
        )
    elif args.transport_action == "stage-cann90-media":
        payload = adapter.stage_cann90_media(
            request_id=args.request_id,
            wait_timeout_seconds=args.wait_timeout_seconds,
            poll_seconds=args.poll_seconds,
        )
    elif args.transport_action == "sync-cann90-media":
        payload = adapter.sync_cann90_media_to_client(
            request_id=args.request_id,
            wait_timeout_seconds=args.wait_timeout_seconds,
        )
    elif args.transport_action == "inspect-server-request":
        payload = adapter.inspect_server_request(
            request_id=args.request_id,
            target_request_id=args.target_request_id,
            wait_timeout_seconds=args.wait_timeout_seconds,
        )
    elif args.transport_action == "node-ack":
        ack_path = Path(args.ack)
        if not ack_path.is_absolute():
            ack_path = ROOT / ack_path
        payload = adapter.acknowledge_node(
            request_id=args.request_id,
            ack_path=ack_path,
            wait_timeout_seconds=args.wait_timeout_seconds,
        )
    else:
        raise SystemExit(f"unsupported engine transport action: {args.transport_action}")
    print_json(payload)
    return 0


def engine_ab_compare_cmd(args: argparse.Namespace) -> int:
    package_root = ROOT / "GitPartner" / "src"
    if str(package_root) not in sys.path:
        sys.path.insert(0, str(package_root))
    from limited_remote_partner.engine_ab_compare import compare_ab, write_report
    from limited_remote_partner.engine.test_engine import atomic_write_json

    comparison_id = subprocess_safe_token(args.comparison_id)
    paths = {
        name: workspace_path(getattr(args, name))
        for name in (
            "baseline_perf",
            "candidate_perf",
            "baseline_correctness",
            "candidate_correctness",
        )
    }
    report = compare_ab(
        **paths,
        median_limit_percent=args.median_limit_percent,
        p95_limit_percent=args.p95_limit_percent,
        stddev_limit_percent=args.stddev_limit_percent,
        weighted_limit_percent=args.weighted_limit_percent,
        expected_repetitions=args.expected_repetitions,
        equivalence_mode=args.equivalence_mode,
    )
    report["comparison_id"] = comparison_id
    output_dir = (
        ROOT / "TestUtils" / "tester_daemon" / "engine_ab" / comparison_id
    )
    json_path, markdown_path = write_report(report, output_dir)
    latest = {
        **report,
        "json_path": str(json_path.relative_to(ROOT)).replace("\\", "/"),
        "markdown_path": str(markdown_path.relative_to(ROOT)).replace("\\", "/"),
    }
    atomic_write_json(
        ROOT / "TestUtils" / "tester_daemon" / "engine_ab_latest.json",
        latest,
    )
    print_json(
        {
            "comparison_id": comparison_id,
            "verdict": report["verdict"],
            "blockers": report["blockers"],
            "json_path": latest["json_path"],
            "markdown_path": latest["markdown_path"],
        }
    )
    return 0 if report["verdict"] == "PASS" else 1


def engine_ab_series_cmd(args: argparse.Namespace) -> int:
    package_root = ROOT / "GitPartner" / "src"
    if str(package_root) not in sys.path:
        sys.path.insert(0, str(package_root))
    from limited_remote_partner.engine_ab_compare import (
        compare_ab,
        compare_ab_series,
        write_series_report,
    )
    from limited_remote_partner.engine.test_engine import atomic_write_json

    comparison_id = subprocess_safe_token(args.comparison_id)
    plan = read_engine_promotion_object(workspace_path(args.plan))
    raw_pairs = plan.get("pairs", [])
    plan_protocol = str(plan.get("protocol_version") or "")
    if plan_protocol not in {
        "engine-ab-series-plan-v1",
        "engine-ab-series-plan-v2",
        "engine-ab-series-plan-v3",
    } or not isinstance(raw_pairs, list):
        raise SystemExit(
            "A/B series plan must use engine-ab-series-plan-v1/v2/v3 and contain pairs"
        )
    equivalence_mode = str(
        plan.get("equivalence_mode")
        or (
            "scheduler-policy-v1"
            if plan_protocol == "engine-ab-series-plan-v3"
            else (
                "causal-performance-first-v1"
                if plan_protocol == "engine-ab-series-plan-v2"
                else "strict-pair-v1"
            )
        )
    )
    expected_mode = {
        "engine-ab-series-plan-v1": "strict-pair-v1",
        "engine-ab-series-plan-v2": "causal-performance-first-v1",
        "engine-ab-series-plan-v3": "scheduler-policy-v1",
    }[plan_protocol]
    if equivalence_mode != expected_mode:
        raise SystemExit(
            f"{plan_protocol} requires equivalence_mode={expected_mode}"
        )
    pair_reports: list[dict[str, object]] = []
    for index, raw in enumerate(raw_pairs, start=1):
        if not isinstance(raw, dict):
            raise SystemExit(f"A/B series pair {index} must be an object")
        pair_id = subprocess_safe_token(str(raw.get("pair_id") or f"pair-{index}"))
        order = str(raw.get("execution_order") or "")
        paths = {
            name: workspace_path(str(raw.get(name) or ""))
            for name in (
                "baseline_perf",
                "candidate_perf",
                "baseline_correctness",
                "candidate_correctness",
            )
        }
        report = compare_ab(
            **paths,
            median_limit_percent=args.median_limit_percent,
            p95_limit_percent=args.p95_limit_percent,
            stddev_limit_percent=args.stddev_limit_percent,
            weighted_limit_percent=args.weighted_limit_percent,
            expected_repetitions=args.expected_repetitions,
            equivalence_mode=equivalence_mode,
        )
        report["pair_id"] = pair_id
        report["execution_order"] = order
        pair_reports.append(report)
    report = compare_ab_series(pair_reports, minimum_pairs=args.minimum_pairs)
    report["comparison_id"] = comparison_id
    output_dir = ROOT / "TestUtils" / "tester_daemon" / "engine_ab" / comparison_id
    json_path, markdown_path = write_series_report(report, output_dir)
    latest = {
        **report,
        "json_path": str(json_path.relative_to(ROOT)).replace("\\", "/"),
        "markdown_path": str(markdown_path.relative_to(ROOT)).replace("\\", "/"),
    }
    atomic_write_json(
        ROOT / "TestUtils" / "tester_daemon" / "engine_ab_latest.json", latest
    )
    print_json(
        {
            "comparison_id": comparison_id,
            "verdict": report["verdict"],
            "pair_count": report["pair_count"],
            "blockers": report["blockers"],
            "json_path": latest["json_path"],
            "markdown_path": latest["markdown_path"],
        }
    )
    return 0 if report["verdict"] == "PASS" else 1


def engine_promotion_evaluate_cmd(args: argparse.Namespace) -> int:
    promotion_id = subprocess_safe_token(args.promotion_id)
    ab_report = read_engine_promotion_object(workspace_path(args.ab_report))
    identity_evidence = read_engine_promotion_object(
        workspace_path(args.identity_evidence)
    )
    throughput_evidence = read_engine_promotion_object(
        workspace_path(args.throughput_evidence)
    )
    report = evaluate_engine_promotion(
        root=ROOT,
        ab_report=ab_report,
        identity_evidence=identity_evidence,
        throughput_evidence=throughput_evidence,
        max_device_handoff_seconds=args.max_device_handoff_seconds,
        minimum_jobs=args.minimum_jobs,
        minimum_speedup=args.minimum_speedup,
    )
    report["promotion_id"] = promotion_id
    output_dir = (
        ROOT / "TestUtils" / "tester_daemon" / "engine_promotion" / promotion_id
    )
    json_path, markdown_path = write_promotion_report(report, output_dir)
    latest = {
        **report,
        "json_path": str(json_path.relative_to(ROOT)).replace("\\", "/"),
        "markdown_path": str(markdown_path.relative_to(ROOT)).replace("\\", "/"),
    }
    from ascendop_daemon.legacy.engine_promotion import write_json

    write_json(
        ROOT / "TestUtils" / "tester_daemon" / "engine_promotion_latest.json",
        latest,
    )
    print_json(
        {
            "promotion_id": promotion_id,
            "verdict": report["verdict"],
            "blockers": report["blockers"],
            "json_path": latest["json_path"],
            "markdown_path": latest["markdown_path"],
        }
    )
    return 0 if report["verdict"] == "PASS" else 1


def engine_profile_stability_cmd(args: argparse.Namespace) -> int:
    from ascendop_daemon.observability.engine_stability import (
        evaluate_profile_stability,
        write_stability_report,
    )

    stability_id = subprocess_safe_token(args.stability_id)
    report = evaluate_profile_stability(
        root=ROOT,
        pump_state=read_engine_promotion_object(
            ROOT / "TestUtils" / "tester_daemon" / "engine_pump_state.json"
        ),
        job_ids=list(args.job_id or []),
        minimum_runs=args.minimum_runs,
        weighted_limit_percent=args.weighted_limit_percent,
        weighted_cv_limit_percent=args.weighted_cv_limit_percent,
        per_case_limit_percent=args.per_case_limit_percent,
        expected_profile=args.expected_profile,
    )
    report["stability_id"] = stability_id
    output_dir = (
        ROOT / "TestUtils" / "tester_daemon" / "engine_stability" / stability_id
    )
    json_path, markdown_path = write_stability_report(report, output_dir)
    write_engine_promotion_json(
        ROOT / "TestUtils" / "tester_daemon" / "engine_stability_latest.json",
        {
            **report,
            "json_path": str(json_path.relative_to(ROOT)).replace("\\", "/"),
            "markdown_path": str(markdown_path.relative_to(ROOT)).replace("\\", "/"),
        },
    )
    print_json(
        {
            "stability_id": stability_id,
            "verdict": report["verdict"],
            "run_count": report["run_count"],
            "weighted_stats": report["weighted_stats"],
            "blockers": report["blockers"],
            "json_path": str(json_path.relative_to(ROOT)).replace("\\", "/"),
            "markdown_path": str(markdown_path.relative_to(ROOT)).replace("\\", "/"),
        }
    )
    return 0 if report["verdict"] == "PASS" else 1


def engine_identity_evidence_cmd(args: argparse.Namespace) -> int:
    evidence_id = subprocess_safe_token(args.evidence_id)
    report = build_identity_evidence(
        root=ROOT,
        baseline_identity=read_engine_promotion_object(
            workspace_path(args.baseline_identity)
        ),
        candidate_identity=read_engine_promotion_object(
            workspace_path(args.candidate_identity)
        ),
        baseline_timeline=read_engine_promotion_json_lines(
            workspace_path(args.baseline_timeline)
        ),
        candidate_terminal=read_engine_promotion_object(
            workspace_path(args.candidate_terminal)
        ),
        baseline_terminal=(
            read_engine_promotion_object(workspace_path(args.baseline_terminal))
            if args.baseline_terminal
            else None
        ),
    )
    report["evidence_id"] = evidence_id
    output_dir = ROOT / "TestUtils" / "tester_daemon" / "engine_identity" / evidence_id
    output_path = output_dir / "ENGINE_IDENTITY_EVIDENCE.json"
    write_engine_promotion_json(output_path, report)
    latest_path = ROOT / "TestUtils" / "tester_daemon" / "engine_identity_latest.json"
    write_engine_promotion_json(
        latest_path,
        {
            **report,
            "json_path": str(output_path.relative_to(ROOT)).replace("\\", "/"),
        },
    )
    print_json(
        {
            "evidence_id": evidence_id,
            "exclusive_lease": report["device_exclusivity"]["exclusive_lease"],
            "json_path": str(output_path.relative_to(ROOT)).replace("\\", "/"),
        }
    )
    return 0 if report["device_exclusivity"]["exclusive_lease"] else 1


def engine_throughput_evidence_cmd(args: argparse.Namespace) -> int:
    window_id = subprocess_safe_token(args.window_id)
    report = build_throughput_evidence(
        root=ROOT,
        pump_state=read_engine_promotion_object(
            ROOT / "TestUtils" / "tester_daemon" / "engine_pump_state.json"
        ),
        pump_events=read_engine_promotion_json_lines(
            ROOT / "TestUtils" / "tester_daemon" / "engine_pump_events.jsonl"
        ),
        admission_state=read_engine_promotion_object(
            ROOT / "TestUtils" / "tester_daemon" / "engine_admission_state.json"
        ),
        job_ids=list(args.job_id or []),
        baseline_terminal=(
            read_engine_promotion_object(workspace_path(args.baseline_terminal))
            if args.baseline_terminal
            else None
        ),
    )
    report["window_id"] = window_id
    output_dir = ROOT / "TestUtils" / "tester_daemon" / "engine_throughput" / window_id
    output_path = output_dir / "ENGINE_THROUGHPUT_EVIDENCE.json"
    write_engine_promotion_json(output_path, report)
    latest_path = ROOT / "TestUtils" / "tester_daemon" / "engine_throughput_latest.json"
    write_engine_promotion_json(
        latest_path,
        {
            **report,
            "json_path": str(output_path.relative_to(ROOT)).replace("\\", "/"),
        },
    )
    print_json(
        {
            "window_id": window_id,
            "job_count": report["job_count"],
            "max_inflight_observed": report["max_inflight_observed"],
            "max_active_jobs_observed": report["max_active_jobs_observed"],
            "max_host_stage_concurrency": report["max_host_stage_concurrency"],
            "device_overlap_count": report["device_overlap_count"],
            "preactivation_measurement_overlap_count": report[
                "preactivation_measurement_overlap_count"
            ],
            "speedup_vs_baseline": report["speedup_vs_baseline"],
            "handoffs": report["completion_to_next_device_start_seconds"],
            "json_path": str(output_path.relative_to(ROOT)).replace("\\", "/"),
        }
    )
    return 0


def workspace_path(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = ROOT / path
    path = path.resolve()
    if path != ROOT and ROOT not in path.parents:
        raise SystemExit(f"A/B input escapes workspace: {path}")
    return path


def engine_pump_cmd(args: argparse.Namespace) -> int:
    route = engine_route_from_args(args)
    gp_repo = Path(route.gitpartner_repo)
    if not gp_repo.is_absolute():
        gp_repo = ROOT / gp_repo
    adapter = EngineTransportAdapter(
        ROOT,
        gitpartner_repo=gp_repo,
        result_gitpartner_repo=(
            ROOT / route.result_worktree
            if route.result_worktree
            and not Path(route.result_worktree).is_absolute()
            else Path(route.result_worktree)
            if route.result_worktree
            else gp_repo
        ),
        engine_root=route.engine_root,
        remote_root=route.remote_root,
        transport=route.transport,
        engine_wait_initial_grace_seconds=args.initial_grace_seconds,
        endpoint_id=route.endpoint_id,
        node_id=route.node_id,
        execution_environment_id=route.execution_environment_id,
        gateway_id=route.gateway_id,
        transport_mode=route.transport_mode,
        registration_generation=route.registration_generation,
        control_channel=route.control_channel,
        result_channel=route.result_channel,
        append_requests=route.append_requests,
        duplex_lanes=route.duplex_lanes,
        remote_gitpartner_repo=route.remote_gitpartner_repo,
        exchange_wait_ready_seconds=args.exchange_wait_ready_seconds,
    )
    ingestor = EngineResultIngestor(ROOT, gitpartner_repo=gp_repo)
    capacity_overrides = {
        name: value
        for name, value in (
            ("standby_slots", args.standby_slots),
            ("active_job_slots", args.active_job_slots),
            ("return_backlog_soft_limit_bytes", args.return_backlog_soft_limit_bytes),
            ("return_backlog_hard_limit_bytes", args.return_backlog_hard_limit_bytes),
            ("return_backlog_soft_limit_jobs", args.return_backlog_soft_limit_jobs),
            ("return_backlog_hard_limit_jobs", args.return_backlog_hard_limit_jobs),
        )
        if value is not None
    }
    if route.device_inventory:
        capacity_overrides["device_inventory"] = [
            dict(item) for item in route.device_inventory
        ]
    worker_identity = build_engine_pump_worker_identity(
        route,
        operator_scope={
            str(op) for op in args.operator_scope if str(op)
        },
        capacity_overrides=capacity_overrides,
        wait_timeout_seconds=args.wait_timeout_seconds,
        initial_grace_seconds=args.initial_grace_seconds,
        interval_seconds=args.interval_seconds,
        exchange_wait_ready_seconds=args.exchange_wait_ready_seconds,
        expected_remote_generation=expected_remote_engine_code_generation(
            ROOT,
            gitpartner_repo=gp_repo,
        ),
    )
    worker_identity["local_worker_code_generation"] = (
        local_engine_code_generation(ROOT)
    )
    pump = EnginePump(
        ROOT,
        adapter,
        ingestor,
        capacity_overrides=capacity_overrides,
        operator_scope=(
            {str(op) for op in args.operator_scope if str(op)}
            if args.operator_scope
            else None
        ),
        expected_remote_generation=expected_remote_engine_code_generation(
            ROOT,
            gitpartner_repo=gp_repo,
        ),
        endpoint_id=route.endpoint_id,
    )
    if args.pump_action == "status":
        payload = pump.status()
    elif args.pump_action == "enqueue":
        spec_path = Path(args.spec)
        if not spec_path.is_absolute():
            spec_path = ROOT / spec_path
        payload_root = Path(args.payload_root) if args.payload_root else None
        if payload_root is not None and not payload_root.is_absolute():
            payload_root = ROOT / payload_root
        payload = pump.enqueue(spec_path, payload_root)
    elif args.pump_action == "enqueue-submit":
        command = args.harness_command
        if args.command_file:
            command_path = Path(args.command_file)
            if not command_path.is_absolute():
                command_path = ROOT / command_path
            command = command_path.read_text(encoding="utf-8-sig").strip()
        submit_root_override = None
        if args.submit_root_override:
            submit_root_override = workspace_path(args.submit_root_override)
        spec_path, payload_root = build_compatibility_job(
            ROOT,
            command,
            remote_root=route.remote_root,
            submit_root_override=submit_root_override,
            execution_profile=args.execution_profile,
            job_id_suffix=args.job_id_suffix,
            workflow_ingest=not args.no_workflow_ingest,
            queue_preactivation=args.queue_preactivation == "enabled",
            measurement_preactivation_overlap=(
                args.measurement_preactivation_overlap == "enabled"
            ),
            profile_export_capture_overlap=(
                args.profile_export_capture_overlap == "enabled"
            ),
            require_case_cache_hit=args.case_cache_access == "require-hit",
        )
        payload = pump.enqueue(spec_path, payload_root)
    elif args.pump_action == "cancel-pending":
        payload = pump.cancel_pending(args.engine_job_id, reason=args.reason)
    elif args.pump_action == "tick":
        payload = pump.tick(wait_timeout_seconds=args.wait_timeout_seconds)
    elif args.pump_action == "run":
        payload = run_engine_pump_loop(
            pump,
            wait_timeout_seconds=args.wait_timeout_seconds,
            interval_seconds=args.interval_seconds,
            worker_identity=worker_identity,
        )
    else:
        raise SystemExit(f"unsupported engine pump action: {args.pump_action}")
    print_json(payload)
    return 0


def run_engine_pump_loop(
    pump: EnginePump,
    *,
    wait_timeout_seconds: int,
    interval_seconds: float,
    worker_identity: dict[str, object] | None = None,
) -> dict[str, object]:
    interval = max(0.25, float(interval_seconds or 1.0))
    worker_path = ROOT / "TestUtils" / "tester_daemon" / "engine_pump_worker.json"
    worker_generation = local_engine_code_generation(ROOT)
    worker_pid = os.getpid()
    worker_start_token = process_start_token(worker_pid)
    cycles = 0
    lock_contentions = 0
    consecutive_lock_contentions = 0
    last: dict[str, object] = {}
    while True:
        try:
            last = pump.tick(wait_timeout_seconds=wait_timeout_seconds)
            consecutive_lock_contentions = 0
        except EnginePumpError as exc:
            if str(exc).strip().lower() != "engine pump cycle is already active":
                raise
            lock_contentions += 1
            consecutive_lock_contentions += 1
            last = {
                "outcome": "deferred-lock-contention",
                "error": str(exc),
            }
            if consecutive_lock_contentions == 1 or lock_contentions % 20 == 0:
                append_daemon_runtime_event(
                    ROOT,
                    "engine_pump_cycle_deferred",
                    {
                        "pid": worker_pid,
                        "lock_contentions": lock_contentions,
                        "consecutive_lock_contentions": consecutive_lock_contentions,
                        "error": str(exc),
                    },
                )
        cycles += 1
        status = pump.status()
        worker = {
            "pid": worker_pid,
            "start_token": worker_start_token,
            "started_at": read_json_dict(worker_path).get("started_at", utc_now_iso()),
            "heartbeat_at": utc_now_iso(),
            "code_generation": worker_generation,
            "worker_identity": worker_identity or {},
            "cycles": cycles,
            "last_outcome": last.get("outcome", ""),
            "lock_contentions": lock_contentions,
            "consecutive_lock_contentions": consecutive_lock_contentions,
            "state_counts": status.get("state_counts", {}),
        }
        write_json_atomic(worker_path, worker, ensure_ascii=True)
        replacement = engine_pump_worker_replacement_requested(
            pid=worker_pid,
            start_token=worker_start_token,
        )
        if replacement:
            append_daemon_runtime_event(
                ROOT,
                "engine_pump_worker_replacement_ready",
                {
                    "pid": worker_pid,
                    "start_token": worker_start_token,
                    "cycles": cycles,
                    "worker_identity": worker_identity or {},
                    "desired_identity": replacement.get("desired_identity", {}),
                },
            )
            return {
                **last,
                "loop_outcome": "graceful-replacement-requested",
                "cycles": cycles,
            }
        current_generation = local_engine_code_generation(ROOT)
        if current_generation != worker_generation:
            append_daemon_runtime_event(
                ROOT,
                "engine_pump_worker_generation_changed",
                {
                    "pid": os.getpid(),
                    "loaded_generation": worker_generation,
                    "current_generation": current_generation,
                },
            )
            return {
                **last,
                "loop_outcome": "code-generation-changed",
                "cycles": cycles,
                "loaded_generation": worker_generation,
                "current_generation": current_generation,
            }
        if not engine_pump_loop_should_continue(status, stop=bool(read_stop_request(ROOT))):
            return {**last, "loop_outcome": "drained", "cycles": cycles}
        time.sleep(interval)


def engine_pump_loop_should_continue(status: dict[str, object], *, stop: bool) -> bool:
    admission = status.get("admission", {})
    if not isinstance(admission, dict) or not admission.get("enabled"):
        return False
    counts = status.get("state_counts", {})
    if not isinstance(counts, dict):
        return False
    if engine_pump_status_has_drain_work(status):
        return True
    return (
        not stop
        and not bool(admission.get("draining"))
        and (
            int(counts.get("pending", 0) or 0) > 0
            or int(status.get("staged_enqueue_count", 0) or 0) > 0
            or engine_pump_status_has_retryable_admission_work(status)
        )
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="AscendOP tester daemon")
    sub = parser.add_subparsers(dest="command", required=True)

    p_tick = sub.add_parser("tick", help="run one daemon scan/decision tick")
    p_tick.add_argument("--config", required=True, help="daemon config JSON path")
    p_tick.add_argument("--mode", choices=["shadow", "advisory", "execute"], default="shadow")
    p_tick.add_argument("--write-state", action="store_true", help="write TestUtils/tester_daemon status files")
    p_tick.add_argument(
        "--dry-run-execute",
        action="store_true",
        help="in execute mode, print the command without running it",
    )
    p_tick.add_argument("--expected-action-id", default="", help="execute only if selected action_id matches")
    p_tick.add_argument("--allow-live-execute", action="store_true", help="execute current selected action without an expected action id")
    p_tick.set_defaults(func=tick)

    p_run = sub.add_parser("run", help="run daemon loop with lock and heartbeat")
    p_run.add_argument("--config", required=True, help="daemon config JSON path")
    p_run.add_argument("--mode", choices=["shadow", "advisory", "execute"], default="shadow")
    p_run.add_argument("--write-state", action="store_true", help="write TestUtils/tester_daemon status files")
    p_run.add_argument("--interval-seconds", type=float, default=0.0, help="0 means use policy.run_interval_seconds or 60s")
    p_run.add_argument("--max-ticks", type=int, default=0, help="test helper; 0 means run forever")
    p_run.add_argument("--dry-run-execute", action="store_true")
    p_run.add_argument("--expected-action-id", default="", help="execute only if selected action_id matches")
    p_run.add_argument("--allow-live-execute", action="store_true", help="execute current selected action without an expected action id")
    p_run.add_argument("--clear-stop", action="store_true", help="clear a prior daemon stop request before starting")
    p_run.add_argument(
        "--replace-stale-lock-after-seconds",
        type=int,
        default=0,
        help=(
            "replace ownerless daemon.lock only if it is older than this many seconds; "
            "0 disables age-based replacement, while confirmed-dead owners are always reclaimed"
        ),
    )
    p_run.add_argument("--exit-on-tick-error", action="store_true", help="stop daemon loop when one tick returns nonzero")
    p_run.set_defaults(func=run_loop)

    p_stop = sub.add_parser("stop", help="request a running daemon loop to stop")
    p_stop.add_argument("--reason", default="", help="human-readable stop reason")
    p_stop.add_argument(
        "--config",
        default="tools/tester_daemon/config/s5_910b_gitpartner_glugrad_bitwise.json",
    )
    p_stop.add_argument("--daemon-only", action="store_true", help="do not stop the solver trigger bridge")
    p_stop.set_defaults(func=stop_daemon)

    p_clear_stop = sub.add_parser("clear-stop", help="remove a pending daemon stop request")
    p_clear_stop.set_defaults(func=clear_stop)

    p_health = sub.add_parser("health", help="check daemon heartbeat/liveness health")
    p_health.add_argument("--max-heartbeat-age-seconds", type=int, default=300)
    p_health.set_defaults(func=health)

    p_control_db = sub.add_parser(
        "control-db",
        help="manage the shadow system registry, immutable TestRequests, and routed outboxes",
    )
    p_control_db.add_argument(
        "--database",
        default="TestUtils/tester_daemon/control.sqlite3",
        help="SQLite control database path inside the workspace",
    )
    control_sub = p_control_db.add_subparsers(dest="control_action", required=True)
    control_sub.add_parser("init", help="initialize or validate the database schema")
    p_control_status = control_sub.add_parser("status", help="print normalized control-plane state")
    p_control_status.add_argument("--event-limit", type=int, default=20)
    p_control_sync = control_sub.add_parser(
        "sync", help="compile daemon/operator and backend desired state into SQLite"
    )
    p_control_sync.add_argument("--config", required=True)
    p_control_sync.add_argument(
        "--registry", default="Develop/registry/system_registry.json"
    )
    p_route_explain = control_sub.add_parser(
        "route-explain", help="explain hard capability filtering without creating an attempt"
    )
    p_route_explain.add_argument(
        "--registry", default="Develop/registry/system_registry.json"
    )
    route_input = p_route_explain.add_mutually_exclusive_group(required=True)
    route_input.add_argument("--requirements-json")
    route_input.add_argument("--requirements-file")
    p_route_request = control_sub.add_parser(
        "route-request", help="create one immutable route attempt and pending outbox entry"
    )
    p_route_request.add_argument("--request-id", required=True)
    p_route_request.add_argument(
        "--registry", default="Develop/registry/system_registry.json"
    )
    p_ingest_node_report = control_sub.add_parser(
        "ingest-node-report",
        help="ingest one GP node lifecycle report into the control database",
    )
    p_ingest_node_report.add_argument("--report", required=True)
    p_ingest_node_report.add_argument("--source", default="")
    p_ingest_node_reports = control_sub.add_parser(
        "ingest-node-reports",
        help="ingest all matching GP node lifecycle reports from a mirror root",
    )
    p_ingest_node_reports.add_argument(
        "--report-root",
        default="GitPartner/output/_control/nodes",
    )
    p_ingest_node_reports.add_argument(
        "--pattern",
        default="*/report.json",
    )
    p_ingest_node_reports.add_argument("--source", default="")
    p_reconcile_nodes = control_sub.add_parser(
        "reconcile-nodes",
        help="idempotently ingest node reports and rebuild accepted generation acks",
    )
    p_reconcile_nodes.add_argument(
        "--report-root",
        action="append",
        default=None,
        help="report mirror root; may be repeated",
    )
    p_reconcile_nodes.add_argument(
        "--ack-root",
        default="TestUtils/tester_daemon/node_acks",
    )
    p_reconcile_nodes.add_argument("--pattern", default="*/report.json")
    p_refresh_nodes = control_sub.add_parser(
        "refresh-nodes",
        help=(
            "query registered GP node-report refs and reconcile generation-fenced "
            "admission without scheduling workflow work"
        ),
    )
    p_refresh_nodes.add_argument(
        "--registry", default="Develop/registry/system_registry.json"
    )
    p_refresh_nodes.add_argument(
        "--ack-root",
        default="TestUtils/tester_daemon/node_acks",
    )
    p_refresh_nodes.add_argument(
        "--endpoint",
        action="append",
        default=[],
        help="refresh only this endpoint; may be repeated",
    )
    p_accept_node = control_sub.add_parser(
        "accept-node",
        help="accept a discovered node and optionally write its generation-fenced ack",
    )
    p_accept_node.add_argument("--node-id", required=True)
    p_accept_node.add_argument("--ack-output")
    p_accept_node.add_argument(
        "--registry", default="Develop/registry/system_registry.json"
    )
    p_generate_requests = control_sub.add_parser(
        "generate-requests",
        help="discover current queued candidates and write requests/outboxes without transport",
    )
    p_generate_requests.add_argument("--config", required=True)
    p_generate_requests.add_argument(
        "--registry", default="Develop/registry/system_registry.json"
    )
    p_generate_requests.add_argument(
        "--request-root", default="TestUtils/tester_daemon/requests"
    )
    p_generate_requests.add_argument("--limit", type=int, default=0)
    p_generate_requests.add_argument(
        "--no-route", action="store_true", help="create TestRequests but no attempts/outboxes"
    )
    p_generate_requests.add_argument(
        "--include-managed",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    p_control_db.set_defaults(func=control_database_cmd)

    p_endpoint_dispatch = sub.add_parser(
        "endpoint-dispatch",
        help="run endpoint-local transactional GP outbox dispatchers",
    )
    p_endpoint_dispatch.add_argument(
        "--database",
        default="TestUtils/tester_daemon/control.sqlite3",
    )
    p_endpoint_dispatch.add_argument(
        "--registry",
        default="Develop/registry/system_registry.json",
    )
    p_endpoint_dispatch.add_argument("--endpoint", action="append", default=[])
    p_endpoint_dispatch.add_argument("--capacity", type=int, default=4)
    endpoint_dispatch_sub = p_endpoint_dispatch.add_subparsers(
        dest="dispatch_action", required=True
    )
    endpoint_dispatch_sub.add_parser("once")
    p_endpoint_run = endpoint_dispatch_sub.add_parser("run")
    p_endpoint_run.add_argument("--interval-seconds", type=float, default=0.25)
    p_endpoint_run.add_argument(
        "--max-seconds",
        type=float,
        default=0.0,
        help="0 keeps the dispatcher resident",
    )
    p_endpoint_dispatch.set_defaults(func=endpoint_dispatch_cmd)

    p_experiment = sub.add_parser(
        "distributed-experiment",
        help="enqueue, run, and report isolated multi-endpoint canaries",
    )
    p_experiment.add_argument(
        "--database",
        default="TestUtils/tester_daemon/control.sqlite3",
    )
    p_experiment.add_argument(
        "--registry",
        default="Develop/registry/system_registry.json",
    )
    p_experiment.add_argument(
        "--config",
        default="tools/tester_daemon/config/s5_910b_gitpartner_glugrad_bitwise.json",
    )
    experiment_sub = p_experiment.add_subparsers(
        dest="experiment_action", required=True
    )
    p_experiment_enqueue = experiment_sub.add_parser("enqueue")
    p_experiment_enqueue.add_argument("--experiment-id", required=True)
    p_experiment_enqueue.add_argument(
        "--task",
        action="append",
        default=[],
        help="ENDPOINT:CLASS[:DURATION_MS[:PAYLOAD_BYTES[:FAILURE]]]",
    )
    p_experiment_enqueue.add_argument("--tasks-file")
    p_experiment_report = experiment_sub.add_parser("report")
    p_experiment_report.add_argument("--experiment-id", required=True)
    p_experiment_run = experiment_sub.add_parser("run")
    p_experiment_run.add_argument("--experiment-id", required=True)
    p_experiment_run.add_argument("--endpoint", action="append", default=[])
    p_experiment_run.add_argument("--capacity", type=int, default=4)
    p_experiment_run.add_argument("--timeout-seconds", type=float, default=300)
    p_experiment_run.add_argument("--poll-seconds", type=float, default=0.25)
    p_experiment_acceptance = experiment_sub.add_parser(
        "acceptance",
        help=(
            "enqueue and autonomously evaluate one clean sustained "
            "multi-card experiment"
        ),
    )
    p_experiment_acceptance.add_argument("--experiment-id", required=True)
    p_experiment_acceptance.add_argument("--tasks-file", required=True)
    p_experiment_acceptance.add_argument(
        "--endpoint", action="append", default=[]
    )
    p_experiment_acceptance.add_argument(
        "--capacity",
        type=int,
        default=0,
        help=(
            "maximum endpoint requests in flight; 0 uses the bounded "
            "sustained-load window so GP return batching stays recoverable "
            "while the Engine remains continuously fed"
        ),
    )
    p_experiment_acceptance.add_argument(
        "--timeout-seconds", type=float, default=3000
    )
    p_experiment_acceptance.add_argument(
        "--poll-seconds", type=float, default=0.1
    )
    p_experiment_acceptance.add_argument(
        "--minimum-window-seconds", type=float, default=1800
    )
    p_experiment_acceptance.add_argument(
        "--minimum-accepted-tasks", type=int, default=7
    )
    p_experiment_acceptance.add_argument(
        "--minimum-physical-devices", type=int, default=2
    )
    p_experiment_acceptance.add_argument(
        "--handoff-threshold-seconds", type=float, default=10
    )
    p_experiment_acceptance.add_argument(
        "--own-service-min-seconds", type=float, default=20
    )
    p_experiment_acceptance.add_argument(
        "--own-service-max-seconds", type=float, default=40
    )
    p_experiment.set_defaults(func=distributed_experiment_cmd)

    p_gp_diagnostic = sub.add_parser(
        "gp-diagnostic",
        help=(
            "run an audited GP-only diagnostic without granting workflow "
            "scheduler eligibility"
        ),
    )
    diagnostic_sub = p_gp_diagnostic.add_subparsers(
        dest="diagnostic_action",
        required=True,
    )
    p_gp_direct = diagnostic_sub.add_parser(
        "direct",
        help=(
            "probe a registered endpoint or an ad-hoc GP descriptor; no "
            "workflow registration is required for descriptors"
        ),
    )
    direct_source = p_gp_direct.add_mutually_exclusive_group(required=True)
    direct_source.add_argument("--endpoint")
    direct_source.add_argument("--descriptor")
    p_gp_direct.add_argument(
        "--registry",
        default="Develop/registry/system_registry.json",
    )
    p_gp_link = diagnostic_sub.add_parser(
        "via-link",
        help=(
            "ask a GP gateway to run a policy-approved node-to-node "
            "read-only diagnostic"
        ),
    )
    p_gp_link.add_argument("--link-id", required=True)
    p_gp_link.add_argument(
        "--registry",
        default="Develop/registry/system_registry.json",
    )
    for diagnostic_parser in (p_gp_direct, p_gp_link):
        diagnostic_parser.add_argument("--request-id", default="")
        diagnostic_parser.add_argument("--wait-seconds", type=float, default=60.0)
        diagnostic_parser.add_argument("--poll-seconds", type=float, default=1.0)
    p_gp_diagnostic.set_defaults(func=gp_diagnostic_cmd)

    p_status = sub.add_parser("status-query", help="read one normalized daemon/workflow status report")
    p_status.add_argument("--config", required=True, help="daemon config JSON path")
    p_status.add_argument("--max-heartbeat-age-seconds", type=int, default=120)
    p_status.add_argument("--write-state", action="store_true", help="write STATE_QUERY.json and STATE_QUERY.md")
    p_status.add_argument("--json", action="store_true", help="print JSON instead of Markdown")
    p_status.add_argument("--no-live-board", action="store_true", help="do not run session-board; use cached state only")
    p_status.set_defaults(func=status_query)

    p_workflow_status = sub.add_parser(
        "workflow-status",
        help="read versioned workflow-profile state without changing business state",
    )
    p_workflow_status.add_argument(
        "--profile-id",
        default="",
        help="optional workflow profile id filter",
    )
    p_workflow_status.add_argument(
        "--instance-id",
        default="",
        help="optional stable workflow instance id filter",
    )
    p_workflow_status.add_argument(
        "--manifest",
        action="append",
        default=[],
        help="optional profile manifest path; repeat for multiple manifests",
    )
    p_workflow_status.set_defaults(func=workflow_status)

    p_outbox = sub.add_parser("native-relay-outbox", help="print Codex App native relay outbox")
    p_outbox.add_argument("--write-state", action="store_true", help="write native_relay_outbox.json/md")
    p_outbox.add_argument("--json", action="store_true", help="print JSON instead of Markdown")
    p_outbox.set_defaults(func=native_relay_outbox)

    p_claim = sub.add_parser(
        "native-relay-claim",
        help="claim pending Codex App native relay entries for an app-side relay consumer",
    )
    p_claim.add_argument("--consumer", required=True, help="stable app-side relay consumer id")
    p_claim.add_argument("--ttl-seconds", type=int, default=120, help="claim lease TTL before another relay may retry")
    p_claim.add_argument("--max-items", type=int, default=1, help="maximum entries to claim")
    p_claim.add_argument(
        "--wait-seconds",
        type=float,
        default=0.0,
        help="read-only long-poll window before returning an empty claim (max 300s)",
    )
    p_claim.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=0.25,
        help="read-only outbox poll interval while --wait-seconds is active",
    )
    p_claim.add_argument("--include-prompt", action="store_true", help="include prompt text in the JSON response")
    p_claim.add_argument("--json", action="store_true", help="print JSON")
    p_claim.set_defaults(func=native_relay_claim)

    p_complete = sub.add_parser(
        "native-relay-complete",
        help="complete an app-side native relay claim and write solver/tester ack state",
    )
    p_complete.add_argument("--entry-id", required=True)
    p_complete.add_argument("--status", default="sent")
    p_complete.add_argument("--turn-id", default="")
    p_complete.add_argument(
        "--pre-delivery-turn-id",
        default=None,
        help="new-turn proof: newest thread turn before send, or __none__",
    )
    p_complete.add_argument("--delivery", default="codex-app-send-message-to-thread")
    p_complete.add_argument("--ide-panel-visible", action="store_true")
    p_complete.add_argument("--error", default="")
    p_complete.add_argument("--metadata-json", default="", help="additional JSON object to merge into the ack record")
    p_complete.add_argument("--metadata-json-env", default="", help="environment variable containing additional JSON metadata")
    p_complete.set_defaults(func=native_relay_complete)

    p_defer_active = sub.add_parser(
        "native-relay-defer-active",
        help="release a claimed entry without sending when a live Codex pre-read shows an active turn",
    )
    p_defer_active.add_argument("--entry-id", required=True)
    p_defer_active.add_argument("--turn-id", required=True)
    p_defer_active.add_argument(
        "--turn-status",
        required=True,
        choices=("active", "inProgress", "running"),
    )
    p_defer_active.add_argument(
        "--has-agent-output",
        default="unknown",
        choices=("true", "false", "unknown"),
    )
    p_defer_active.add_argument(
        "--user-only",
        default="unknown",
        choices=("true", "false", "unknown"),
    )
    p_defer_active.set_defaults(func=native_relay_defer_active)

    p_recover_sessions = sub.add_parser(
        "recover-native-sessions",
        help="requeue current IDE-native gates after Codex Desktop/session restart",
    )
    p_recover_sessions.add_argument("--config", required=True, help="daemon config JSON path")
    p_recover_sessions.add_argument("--reason", default="", help="audit reason for the explicit recovery")
    p_recover_sessions.add_argument("--dry-run", action="store_true", help="report affected gates without changing state")
    p_recover_sessions.add_argument("--json", action="store_true", help=argparse.SUPPRESS)
    p_recover_sessions.set_defaults(func=recover_native_sessions)

    p_import_obs = sub.add_parser(
        "import-thread-observations",
        help="import native Codex thread observations captured by the main monitor",
    )
    p_import_obs.add_argument("--path", default="", help="JSON file containing {updated_at, threads}")
    p_import_obs.add_argument("--observations-json", default="", help="inline JSON object containing {updated_at, threads}")
    p_import_obs.add_argument("--observations-json-env", default="", help="environment variable containing JSON object")
    p_import_obs.add_argument("--sync-ack", action="store_true", help="sync solver trigger ack state from observations")
    p_import_obs.set_defaults(func=import_thread_observations)

    p_register_solver = sub.add_parser(
        "register-solver-thread",
        help="manually update the configured Codex solver thread id",
    )
    p_register_solver.add_argument("--config", required=True, help="daemon config JSON path")
    p_register_solver.add_argument("--op", required=True, help="operator name")
    p_register_solver.add_argument("--thread-id", required=True, help="Codex thread id")
    p_register_solver.add_argument(
        "--clear-op-trigger-state",
        action="store_true",
        help="clear old ack/thread observation records for this operator so current gate can be triggered again",
    )
    p_register_solver.set_defaults(func=register_solver_thread)

    p_register_tester = sub.add_parser(
        "register-tester-thread",
        help="manually update the configured Codex tester thread id for casegen-only workflow",
    )
    p_register_tester.add_argument("--config", required=True, help="daemon config JSON path")
    p_register_tester.add_argument("--op", required=True, help="operator name")
    p_register_tester.add_argument("--thread-id", required=True, help="Codex thread id")
    p_register_tester.add_argument(
        "--clear-op-trigger-state",
        action="store_true",
        help="clear old tester trigger ack/plan records for this operator",
    )
    p_register_tester.set_defaults(func=register_tester_thread)

    p_plugin = sub.add_parser(
        "operator-plugin",
        help="atomically enable or disable one configured operator plugin",
    )
    p_plugin.add_argument("--config", required=True, help="daemon config JSON path")
    p_plugin.add_argument("--op", required=True, help="registered operator name")
    plugin_mode = p_plugin.add_mutually_exclusive_group(required=True)
    plugin_mode.add_argument("--enable", action="store_true", help="insert the operator into the live active set")
    plugin_mode.add_argument("--disable", action="store_true", help="remove the operator from the live active set")
    plugin_mode.add_argument(
        "--drain",
        action="store_true",
        help="finish accepted work, then disable before the next solver/tester gate",
    )
    p_plugin.add_argument(
        "--allow-inflight-disable",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    p_plugin.set_defaults(func=operator_plugin)

    p_plugin_status = sub.add_parser(
        "operator-plugin-status",
        help="show active, draining, and disabled operator plugins",
    )
    p_plugin_status.add_argument("--config", required=True, help="daemon config JSON path")
    p_plugin_status.set_defaults(func=operator_plugin_status)

    p_supervise = sub.add_parser("supervise", help="one-shot watchdog recovery for daemon and solver trigger bridge")
    p_supervise.add_argument("--config", required=True, help="daemon config JSON path")
    p_supervise.add_argument("--mode", choices=["shadow", "advisory", "execute"], default="execute")
    p_supervise.add_argument("--write-state", action="store_true", help="start daemon with --write-state")
    p_supervise.add_argument("--allow-live-execute", action="store_true", help="start daemon with --allow-live-execute")
    p_supervise.add_argument("--clear-stop", action="store_true", help="clear a prior stop request before supervising")
    p_supervise.add_argument("--max-heartbeat-age-seconds", type=int, default=120)
    p_supervise.add_argument("--bridge-max-heartbeat-age-seconds", type=int, default=120)
    p_supervise.add_argument("--replace-stale-lock-after-seconds", type=int, default=120)
    p_supervise.add_argument("--no-bridge", action="store_true", help="do not supervise solver trigger bridge")
    p_supervise.add_argument("--bridge-max-workers", type=int, default=0)
    p_supervise.add_argument("--bridge-wait-seconds", type=int, default=1800)
    p_supervise.add_argument("--dry-run", action="store_true")
    p_supervise.add_argument("--json", action="store_true", help="accepted for consistency; supervise already emits JSON")
    p_supervise.set_defaults(func=supervise)

    p_supervise_loop = sub.add_parser(
        "supervise-loop",
        help="foreground watchdog loop for environments that reap background child processes",
    )
    p_supervise_loop.add_argument("--config", required=True, help="daemon config JSON path")
    p_supervise_loop.add_argument("--mode", choices=["shadow", "advisory", "execute"], default="execute")
    p_supervise_loop.add_argument("--write-state", action="store_true", help="start daemon with --write-state")
    p_supervise_loop.add_argument("--allow-live-execute", action="store_true", help="start daemon with --allow-live-execute")
    p_supervise_loop.add_argument("--clear-stop", action="store_true", help="clear a prior stop request before supervising")
    p_supervise_loop.add_argument("--max-heartbeat-age-seconds", type=int, default=120)
    p_supervise_loop.add_argument("--bridge-max-heartbeat-age-seconds", type=int, default=120)
    p_supervise_loop.add_argument("--replace-stale-lock-after-seconds", type=int, default=120)
    p_supervise_loop.add_argument("--no-bridge", action="store_true", help="do not supervise solver trigger bridge")
    p_supervise_loop.add_argument("--bridge-max-workers", type=int, default=0)
    p_supervise_loop.add_argument("--bridge-wait-seconds", type=int, default=1800)
    p_supervise_loop.add_argument("--interval-seconds", type=float, default=0.0, help="0 means use policy.run_interval_seconds or 60s")
    p_supervise_loop.add_argument("--max-iterations", type=int, default=0, help="0 means run until stopped")
    p_supervise_loop.add_argument("--dry-run", action="store_true")
    p_supervise_loop.add_argument("--json", action="store_true", help="accepted for consistency; supervise-loop emits JSON per iteration")
    p_supervise_loop.set_defaults(func=supervise_loop)

    p_supervise_launch = sub.add_parser(
        "supervise-launch",
        help="hidden launcher for the persistent watchdog loop",
    )
    p_supervise_launch.add_argument("--config", required=True, help="daemon config JSON path")
    p_supervise_launch.add_argument("--mode", choices=["shadow", "advisory", "execute"], default="execute")
    p_supervise_launch.add_argument("--write-state", action="store_true", help="start daemon with --write-state")
    p_supervise_launch.add_argument("--allow-live-execute", action="store_true", help="start daemon with --allow-live-execute")
    p_supervise_launch.add_argument("--clear-stop", action="store_true", help="clear a prior stop request before supervising")
    p_supervise_launch.add_argument("--max-heartbeat-age-seconds", type=int, default=120)
    p_supervise_launch.add_argument("--bridge-max-heartbeat-age-seconds", type=int, default=120)
    p_supervise_launch.add_argument("--replace-stale-lock-after-seconds", type=int, default=120)
    p_supervise_launch.add_argument("--no-bridge", action="store_true", help="do not supervise solver trigger bridge")
    p_supervise_launch.add_argument("--bridge-max-workers", type=int, default=0)
    p_supervise_launch.add_argument("--bridge-wait-seconds", type=int, default=1800)
    p_supervise_launch.add_argument("--interval-seconds", type=float, default=0.0, help="0 means supervise-loop uses its default interval")
    p_supervise_launch.add_argument("--dry-run", action="store_true")
    p_supervise_launch.add_argument("--json", action="store_true", help="accepted for consistency; supervise-launch emits JSON")
    p_supervise_launch.set_defaults(func=supervise_launch)

    p_ack = sub.add_parser("ack-trigger", help="mark a solver trigger as delivered by an external bridge")
    p_ack.add_argument("--key", required=True)
    p_ack.add_argument("--thread-id", required=True)
    p_ack.add_argument("--status", default="sent")
    p_ack.add_argument("--turn-id", default="", help="Codex turn id created by the relay, if known")
    p_ack.add_argument("--delivery", default="", help="delivery path, for example ide-native-relay or app-server-stdio")
    p_ack.add_argument("--ide-panel-visible", action="store_true", help="mark delivery as confirmed visible in the IDE pane")
    p_ack.add_argument("--metadata-json", default="", help="additional JSON object to merge into the ack record")
    p_ack.add_argument("--metadata-json-env", default="", help="environment variable containing additional JSON metadata")
    p_ack.set_defaults(func=ack_trigger)

    p_ack_tester = sub.add_parser("ack-tester-trigger", help="mark a tester casegen trigger as delivered")
    p_ack_tester.add_argument("--key", required=True)
    p_ack_tester.add_argument("--thread-id", required=True)
    p_ack_tester.add_argument("--status", default="sent")
    p_ack_tester.add_argument("--turn-id", default="", help="Codex turn id created by the relay, if known")
    p_ack_tester.add_argument("--delivery", default="", help="delivery path, for example ide-native-relay")
    p_ack_tester.add_argument("--ide-panel-visible", action="store_true", help="mark delivery as confirmed visible in the IDE pane")
    p_ack_tester.add_argument("--metadata-json", default="", help="additional JSON object to merge into the ack record")
    p_ack_tester.add_argument("--metadata-json-env", default="", help="environment variable containing additional JSON metadata")
    p_ack_tester.set_defaults(func=ack_tester_trigger_cmd)

    p_worker = sub.add_parser("execute-worker", help=argparse.SUPPRESS)
    p_worker.add_argument("--payload", required=True)
    p_worker.set_defaults(func=execute_worker)

    p_observability_worker = sub.add_parser("observability-worker", help=argparse.SUPPRESS)
    p_observability_worker.add_argument("--payload", required=True)
    p_observability_worker.set_defaults(func=observability_worker)

    p_engine_candidate_worker = sub.add_parser(
        "engine-candidate-worker",
        help=argparse.SUPPRESS,
    )
    p_engine_candidate_worker.add_argument("--config", required=True)
    p_engine_candidate_worker.set_defaults(func=engine_candidate_worker)

    p_flow_v3 = sub.add_parser(
        "flow-v3",
        help="run the Wire V3 dispatcher/result worker or inspect its state",
    )
    p_flow_v3.add_argument("--config", required=True)
    p_flow_v3.add_argument("--interval-seconds", type=float, default=1.0)
    p_flow_v3.add_argument(
        "--probe-interval-seconds",
        type=float,
        default=60.0,
    )
    p_flow_v3.add_argument("--max-cycles", type=int, default=0)
    p_flow_v3.add_argument(
        "--replace-stale-lock-after-seconds",
        type=int,
        default=0,
        help=(
            "replace ownerless worker lock only after this age; confirmed-dead owners "
            "are always reclaimed"
        ),
    )
    flow_v3_sub = p_flow_v3.add_subparsers(
        dest="flow_v3_action",
        required=True,
    )
    for action in ("status", "probe", "tick", "run"):
        flow_v3_sub.add_parser(action)
    p_flow_v3_recover_ingest = flow_v3_sub.add_parser(
        "recover-workflow-ingest",
        help="retry only durable terminal workflow postprocessing",
    )
    p_flow_v3_recover_ingest.add_argument("--request-id", required=True)
    p_flow_v3_recover_ingest.add_argument("--attempt-id", required=True)
    p_flow_v3.set_defaults(func=flow_v3_cmd)

    p_engine = sub.add_parser(
        "engine-admission",
        help="manage durable local admission state for the B-side test engine",
    )
    engine_sub = p_engine.add_subparsers(dest="engine_action", required=True)
    p_engine_status = engine_sub.add_parser("status")
    p_engine_status.add_argument("--engine-snapshot", default="")
    p_engine_configure = engine_sub.add_parser("configure")
    engine_enable = p_engine_configure.add_mutually_exclusive_group(required=True)
    engine_enable.add_argument("--enable", action="store_true")
    engine_enable.add_argument("--disable", action="store_true")
    p_engine_configure.add_argument("--target-inflight", type=int, default=2)
    p_engine_configure.add_argument("--draining", action="store_true")
    p_engine_configure.add_argument("--controller-owner", default="")
    p_engine_configure.add_argument("--controller-token", default="")
    p_engine_configure.add_argument("--lease-seconds", type=int, default=0)
    p_engine_release = engine_sub.add_parser("release-controller")
    p_engine_release.add_argument("--controller-owner", required=True)
    p_engine_release.add_argument("--controller-token", required=True)
    for action in ("begin", "accept", "reconcile", "terminal"):
        action_parser = engine_sub.add_parser(action)
        action_parser.add_argument("--json-file", required=True)
    p_engine_return = engine_sub.add_parser("return")
    p_engine_return.add_argument("--engine-job-id", required=True)
    p_engine_return.add_argument("--receipt-id", required=True)
    p_engine.set_defaults(func=engine_admission_cmd)

    p_engine_transport = sub.add_parser(
        "engine-transport",
        help="run trusted GP admission, snapshot, or collect transactions for engine-v1",
    )
    p_engine_transport.add_argument("--gitpartner-repo", default="GitPartner")
    p_engine_transport.add_argument("--engine-root", default="test_engine_demo")
    p_engine_transport.add_argument("--remote-root", default="/opt/ascendop")
    p_engine_transport.add_argument("--transport", choices=["relay", "direct", "auto"], default="relay")
    p_engine_transport.add_argument("--wait-timeout-seconds", type=int, default=180)
    p_engine_transport.add_argument("--initial-grace-seconds", type=int, default=0)
    p_engine_transport.add_argument(
        "--exchange-wait-ready-seconds", type=float, default=45.0
    )
    p_engine_transport.add_argument(
        "--registry", default="Develop/registry/system_registry.json"
    )
    p_engine_transport.add_argument(
        "--database",
        default="TestUtils/tester_daemon/control.sqlite3",
    )
    p_engine_transport.add_argument(
        "--ack-root",
        default="TestUtils/tester_daemon/node_acks",
    )
    p_engine_transport.add_argument("--registered-endpoint", default="")
    p_engine_transport.add_argument("--endpoint-id", default="")
    p_engine_transport.add_argument("--node-id", default="")
    p_engine_transport.add_argument("--execution-environment-id", default="")
    p_engine_transport.add_argument("--gateway-id", default="")
    p_engine_transport.add_argument("--transport-mode", default="")
    p_engine_transport.add_argument("--registration-generation", default="")
    p_engine_transport.add_argument("--control-channel", default="")
    p_engine_transport.add_argument("--result-channel", default="")
    engine_transport_sub = p_engine_transport.add_subparsers(
        dest="transport_action", required=True
    )
    p_transport_accept = engine_transport_sub.add_parser("accept")
    p_transport_accept.add_argument("--spec", required=True)
    p_transport_accept.add_argument("--payload-root")
    p_transport_accept.add_argument("--request-id", required=True)
    p_transport_accept.add_argument("--engine-job-id", required=True)
    p_transport_snapshot = engine_transport_sub.add_parser("snapshot")
    p_transport_snapshot.add_argument("--request-id", required=True)
    p_transport_collect = engine_transport_sub.add_parser("collect")
    p_transport_collect.add_argument("--request-id", required=True)
    p_transport_collect.add_argument("--engine-job-id", required=True)
    p_transport_collect.add_argument("--receipt-id", required=True)
    p_transport_configure = engine_transport_sub.add_parser("configure")
    p_transport_configure.add_argument("--request-id", required=True)
    p_transport_configure.add_argument("--max-inflight", type=int, required=True)
    p_transport_configure.add_argument("--standby-slots", type=int)
    p_transport_configure.add_argument("--active-job-slots", type=int)
    p_transport_configure.add_argument("--host-slots", type=int)
    p_transport_configure.add_argument("--export-slots", type=int)
    p_transport_configure.add_argument("--return-backlog-soft-limit-bytes", type=int)
    p_transport_configure.add_argument("--return-backlog-hard-limit-bytes", type=int)
    p_transport_configure.add_argument("--return-backlog-soft-limit-jobs", type=int)
    p_transport_configure.add_argument("--return-backlog-hard-limit-jobs", type=int)
    transport_drain = p_transport_configure.add_mutually_exclusive_group(required=True)
    transport_drain.add_argument("--drain", action="store_true")
    transport_drain.add_argument("--resume", action="store_true")
    p_transport_sync = engine_transport_sub.add_parser(
        "sync-code",
        help="publish allowlisted GitPartner maintenance changes and sync them to B/client",
    )
    p_transport_sync.add_argument("--request-id", required=True)
    p_transport_resident_sync = engine_transport_sub.add_parser(
        "sync-resident-runtime",
        help=(
            "atomically install the bounded direct-client runtime bundle and "
            "schedule a generation-fenced resident restart"
        ),
    )
    p_transport_resident_sync.add_argument("--request-id", required=True)
    p_transport_resident_sync.add_argument(
        "--restart-delay-seconds",
        type=int,
        default=90,
    )
    p_transport_reconcile = engine_transport_sub.add_parser(
        "reconcile-direct-request",
        help=(
            "re-schedule one existing immutable direct Engine snapshot watch "
            "after validating its request id, SHA-256, endpoint, and generation"
        ),
    )
    p_transport_reconcile.add_argument("--request-id", required=True)
    p_transport_reconcile.add_argument(
        "--target-request-id",
        required=True,
    )
    p_transport_restart = engine_transport_sub.add_parser(
        "restart-role",
        help="restart one existing GP resident role through the trusted maintenance action",
    )
    p_transport_restart.add_argument("--request-id", required=True)
    p_transport_restart.add_argument("--role", choices=["client", "server"], required=True)
    p_transport_cann90 = engine_transport_sub.add_parser(
        "stage-cann90-media",
        help=(
            "download the fixed official CANN 9.0 AArch64 toolkit and 910B ops "
            "packages on the registered GP gateway"
        ),
    )
    p_transport_cann90.add_argument("--request-id", required=True)
    p_transport_cann90.add_argument("--poll-seconds", type=float, default=5.0)
    p_transport_cann90_sync = engine_transport_sub.add_parser(
        "sync-cann90-media",
        help=(
            "copy the verified allowlisted CANN 9.0 media from the A gateway "
            "to the registered 910B client"
        ),
    )
    p_transport_cann90_sync.add_argument("--request-id", required=True)
    p_transport_inspect_server = engine_transport_sub.add_parser(
        "inspect-server-request",
        help=(
            "read one A/server-side request's bounded diagnostics through the "
            "trusted lan-diagnose action"
        ),
    )
    p_transport_inspect_server.add_argument("--request-id", required=True)
    p_transport_inspect_server.add_argument(
        "--target-request-id", required=True
    )
    p_transport_node_ack = engine_transport_sub.add_parser(
        "node-ack",
        help="deliver one generation-fenced central node acknowledgement",
    )
    p_transport_node_ack.add_argument("--request-id", required=True)
    p_transport_node_ack.add_argument("--ack", required=True)
    p_engine_transport.set_defaults(func=engine_transport_cmd)

    p_engine_ab = sub.add_parser(
        "engine-ab-compare",
        help="compare same-version legacy/engine correctness and measurement manifests",
    )
    p_engine_ab.add_argument("--comparison-id", required=True)
    p_engine_ab.add_argument("--baseline-perf", required=True)
    p_engine_ab.add_argument("--candidate-perf", required=True)
    p_engine_ab.add_argument("--baseline-correctness", required=True)
    p_engine_ab.add_argument("--candidate-correctness", required=True)
    p_engine_ab.add_argument("--median-limit-percent", type=float, default=1.0)
    p_engine_ab.add_argument("--p95-limit-percent", type=float, default=5.0)
    p_engine_ab.add_argument("--stddev-limit-percent", type=float, default=10.0)
    p_engine_ab.add_argument("--weighted-limit-percent", type=float, default=1.0)
    p_engine_ab.add_argument(
        "--equivalence-mode",
        choices=[
            "strict-pair-v1",
            "causal-performance-first-v1",
            "scheduler-policy-v1",
        ],
        default="strict-pair-v1",
    )
    p_engine_ab.add_argument(
        "--expected-repetitions",
        type=int,
        help="optional fixed repetition count; omitted means infer from both manifests",
    )
    p_engine_ab.set_defaults(func=engine_ab_compare_cmd)

    p_engine_ab_series = sub.add_parser(
        "engine-ab-series",
        help="evaluate at least three strict pairs in alternating legacy/engine order",
    )
    p_engine_ab_series.add_argument("--comparison-id", required=True)
    p_engine_ab_series.add_argument("--plan", required=True)
    p_engine_ab_series.add_argument("--minimum-pairs", type=int, default=3)
    p_engine_ab_series.add_argument("--median-limit-percent", type=float, default=1.0)
    p_engine_ab_series.add_argument("--p95-limit-percent", type=float, default=5.0)
    p_engine_ab_series.add_argument("--stddev-limit-percent", type=float, default=10.0)
    p_engine_ab_series.add_argument("--weighted-limit-percent", type=float, default=1.0)
    p_engine_ab_series.add_argument(
        "--expected-repetitions",
        type=int,
        help="optional fixed repetition count; omitted means infer from each pair",
    )
    p_engine_ab_series.set_defaults(func=engine_ab_series_cmd)

    p_engine_promotion = sub.add_parser(
        "engine-promotion-evaluate",
        help="fail-closed production gate for A/B, identity, exclusivity, and throughput evidence",
    )
    p_engine_promotion.add_argument("--promotion-id", required=True)
    p_engine_promotion.add_argument("--ab-report", required=True)
    p_engine_promotion.add_argument("--identity-evidence", required=True)
    p_engine_promotion.add_argument("--throughput-evidence", required=True)
    p_engine_promotion.add_argument(
        "--max-device-handoff-seconds", type=float, default=10.0
    )
    p_engine_promotion.add_argument("--minimum-jobs", type=int, default=2)
    p_engine_promotion.add_argument("--minimum-speedup", type=float, default=4.0)
    p_engine_promotion.set_defaults(func=engine_promotion_evaluate_cmd)

    p_engine_stability = sub.add_parser(
        "engine-profile-stability",
        help="validate repeated fixed scalable-profile jobs within one measurement mode",
    )
    p_engine_stability.add_argument("--stability-id", required=True)
    p_engine_stability.add_argument("--job-id", action="append", default=[])
    p_engine_stability.add_argument("--minimum-runs", type=int, default=5)
    p_engine_stability.add_argument(
        "--weighted-limit-percent", type=float, default=1.5
    )
    p_engine_stability.add_argument(
        "--weighted-cv-limit-percent", type=float, default=1.0
    )
    p_engine_stability.add_argument(
        "--per-case-limit-percent", type=float, default=2.0
    )
    p_engine_stability.add_argument(
        "--expected-profile", default=SCALABLE_PROFILE
    )
    p_engine_stability.set_defaults(func=engine_profile_stability_cmd)

    p_engine_identity = sub.add_parser(
        "engine-identity-evidence",
        help="derive same-input and exclusive-device evidence from real legacy/engine artifacts",
    )
    p_engine_identity.add_argument("--evidence-id", required=True)
    p_engine_identity.add_argument("--baseline-identity", required=True)
    p_engine_identity.add_argument("--candidate-identity", required=True)
    p_engine_identity.add_argument("--baseline-timeline", required=True)
    p_engine_identity.add_argument(
        "--baseline-terminal",
        help="optional engine terminal manifest for engine-to-engine exclusivity evidence",
    )
    p_engine_identity.add_argument("--candidate-terminal", required=True)
    p_engine_identity.set_defaults(func=engine_identity_evidence_cmd)

    p_engine_throughput = sub.add_parser(
        "engine-throughput-evidence",
        help="derive multi-job inflight, return, ingest, overlap, and handoff evidence",
    )
    p_engine_throughput.add_argument("--window-id", required=True)
    p_engine_throughput.add_argument("--job-id", action="append", default=[])
    p_engine_throughput.add_argument(
        "--baseline-terminal",
        help="same-contract control terminal manifest used for measured speedup",
    )
    p_engine_throughput.set_defaults(func=engine_throughput_evidence_cmd)

    p_engine_pump = sub.add_parser(
        "engine-pump",
        help="enqueue immutable engine bundles or run one dynamic admission/return cycle",
    )
    p_engine_pump.add_argument("--gitpartner-repo", default="GitPartner")
    p_engine_pump.add_argument("--engine-root", default="test_engine_demo")
    p_engine_pump.add_argument("--remote-root", default="/opt/ascendop")
    p_engine_pump.add_argument("--transport", choices=["relay", "direct", "auto"], default="relay")
    p_engine_pump.add_argument("--wait-timeout-seconds", type=int, default=180)
    p_engine_pump.add_argument("--initial-grace-seconds", type=int, default=0)
    p_engine_pump.add_argument(
        "--exchange-wait-ready-seconds", type=float, default=45.0
    )
    p_engine_pump.add_argument(
        "--registry", default="Develop/registry/system_registry.json"
    )
    p_engine_pump.add_argument("--registered-endpoint", default="")
    p_engine_pump.add_argument("--endpoint-id", default="")
    p_engine_pump.add_argument("--node-id", default="")
    p_engine_pump.add_argument("--execution-environment-id", default="")
    p_engine_pump.add_argument("--gateway-id", default="")
    p_engine_pump.add_argument("--transport-mode", default="")
    p_engine_pump.add_argument("--registration-generation", default="")
    p_engine_pump.add_argument("--control-channel", default="")
    p_engine_pump.add_argument("--result-channel", default="")
    p_engine_pump.add_argument(
        "--operator-scope",
        action="append",
        default=[],
        help="limit pump recovery/admission work to these operators",
    )
    p_engine_pump.add_argument("--interval-seconds", type=float, default=1.0)
    p_engine_pump.add_argument("--standby-slots", type=int)
    p_engine_pump.add_argument("--active-job-slots", type=int)
    p_engine_pump.add_argument("--return-backlog-soft-limit-bytes", type=int)
    p_engine_pump.add_argument("--return-backlog-hard-limit-bytes", type=int)
    p_engine_pump.add_argument("--return-backlog-soft-limit-jobs", type=int)
    p_engine_pump.add_argument("--return-backlog-hard-limit-jobs", type=int)
    p_engine_pump.add_argument(
        "--execution-profile",
        default=CONSERVATIVE_PROFILE,
        choices=[
            CONSERVATIVE_PROFILE,
            SPLIT_PROFILE,
            CORRECTNESS_BATCHED_PROFILE,
            PERFORMANCE_FIRST_SPLIT_PROFILE,
            PERFORMANCE_FIRST_CORRECTNESS_BATCHED_PROFILE,
            PERFORMANCE_SESSION_CORRECTNESS_BATCHED_PROFILE,
            SCALABLE_PROFILE,
            FUSED_SCALABLE_PROFILE,
            CASE_CACHE_PREWARM_PROFILE,
            BATCHED_PROFILE,
        ],
    )
    engine_pump_sub = p_engine_pump.add_subparsers(dest="pump_action", required=True)
    engine_pump_sub.add_parser("status")
    p_pump_enqueue = engine_pump_sub.add_parser("enqueue")
    p_pump_enqueue.add_argument("--spec", required=True)
    p_pump_enqueue.add_argument("--payload-root")
    p_pump_submit = engine_pump_sub.add_parser("enqueue-submit")
    pump_submit_source = p_pump_submit.add_mutually_exclusive_group(required=True)
    pump_submit_source.add_argument("--harness-command")
    pump_submit_source.add_argument("--command-file")
    p_pump_submit.add_argument("--job-id-suffix", default="")
    p_pump_submit.add_argument(
        "--submit-root-override",
        help="use an archived immutable submit snapshot for an explicit canary",
    )
    p_pump_submit.add_argument("--no-workflow-ingest", action="store_true")
    p_pump_submit.add_argument(
        "--queue-preactivation",
        choices=["enabled", "disabled"],
        default="enabled",
        help="audited canary switch; production defaults to enabled",
    )
    p_pump_submit.add_argument(
        "--measurement-preactivation-overlap",
        choices=["enabled", "disabled"],
        default="disabled",
        help=(
            "canary-only host-build/performance overlap; both jobs must opt in"
        ),
    )
    p_pump_submit.add_argument(
        "--profile-export-capture-overlap",
        choices=["enabled", "disabled"],
        default="disabled",
        help=(
            "canary-only fused postprocess/performance overlap; capture remains "
            "single-device and performance-locked"
        ),
    )
    p_pump_submit.add_argument(
        "--case-cache-access",
        choices=["populate", "require-hit"],
        default="populate",
        help="normal tests may require an independently prewarmed cache entry",
    )
    p_pump_cancel = engine_pump_sub.add_parser("cancel-pending")
    p_pump_cancel.add_argument("--engine-job-id", required=True)
    p_pump_cancel.add_argument("--reason", required=True)
    engine_pump_sub.add_parser("tick")
    engine_pump_sub.add_parser("run")
    p_engine_pump.set_defaults(func=engine_pump_cmd)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
