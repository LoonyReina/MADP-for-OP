from __future__ import annotations

import json
import re
from functools import lru_cache
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from ascendop_daemon.legacy.full_flow_latency import build_full_flow_latency, write_full_flow_latency
from ascendop_daemon.core.models import (
    ActionKind,
    BoardSnapshot,
    DaemonConfig,
    DaemonPlan,
    GateDecision,
    TransportObservation,
    extract_test_version,
    observed_operators,
)
from ascendop_daemon.registry.operator_plugins import metric_epoch_for_config, read_operator_plugin_state
from ascendop_daemon.automation.trigger_state import read_trigger_ack_state


TIMELINE_FILE = "timeline.jsonl"
GAPS_JSON = "scheduler_gaps.json"
GAPS_MD = "SCHEDULER_GAPS.md"
REQUEST_SUFFIX_RE = re.compile(r"(_gitpartner.*)$")
LOCAL_DISPATCH_EVENTS = {
    "engine_dispatch_enqueued",
    "resource_lease_acquired",
    "execute_worker_command_started",
    "execute_worker_started",
    "selected_action",
}
LOCAL_ACTUAL_SUBMIT_EVENTS = {
    "engine_dispatch_enqueued",
    "resource_lease_acquired",
    "execute_worker_command_started",
    "execute_worker_started",
}
LOCAL_TEST_OCCUPANCY_ACTIONS = {
    "dispatch_submit",
    "recover_blocked",
    "heartbeat_active_request",
}
REMOTE_DISPATCH_EVENTS = {"gp_started", "gp_dispatched", "engine_admitted"}
TEST_COMPLETION_EVENTS = {
    "result_archived",
    "transport_terminal",
    "gp_finished",
    "engine_terminal",
}
RESULT_ARCHIVE_TERMINAL_DEDUPE_SECONDS = 1800
RESULT_VERDICT_RE = re.compile(r"^Verdict:\s*(\S+)", re.IGNORECASE | re.MULTILINE)
NON_QUALIFIED_RESULT_PREFIXES = ("INFRA", "BUILD", "INSTALL", "PRECHECK", "TRANSPORT")
_JSONL_CACHE: dict[Path, tuple[int, int, list[dict[str, Any]]]] = {}


@lru_cache(maxsize=2048)
def result_verdict_for_path(path_text: str, mtime_ns: int) -> str:
    del mtime_ns
    try:
        text = Path(path_text).read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return ""
    match = RESULT_VERDICT_RE.search(text)
    return match.group(1).strip().upper() if match else ""


def qualified_result_tasks(
    root: Path,
    operators: list[str] | tuple[str, ...],
    events: list[dict[str, Any]] | None = None,
    max_candidates: int = 64,
) -> set[tuple[str, str]]:
    """Return distinct test versions that reached source-owned device evidence.

    RESULT files are the durable qualification source. Timeline entries are
    intentionally not used to choose candidates because periodic result scans
    and JSONL rotation can skew their physical order. Keep a bounded recent set
    independently for each operator so one operator's history cannot evict the
    others from traffic-balance accounting.
    """
    allowed = set(operators)
    del events
    candidates: dict[tuple[str, str], Path] = {}
    for op in operators:
        if op not in allowed:
            continue
        result_root = root / "operators_testresult" / op
        result_paths = sorted(
            result_root.glob("*/RESULT.md"),
            key=lambda path: path.stat().st_mtime_ns,
            reverse=True,
        )
        for result_path in result_paths[:max_candidates]:
            candidates[(op, result_path.parent.name)] = result_path

    qualified: set[tuple[str, str]] = set()
    for task, result_path in candidates.items():
        try:
            stat = result_path.stat()
        except OSError:
            continue
        verdict = result_verdict_for_path(str(result_path), stat.st_mtime_ns)
        if not verdict or verdict.startswith(NON_QUALIFIED_RESULT_PREFIXES):
            continue
        qualified.add(task)
    return qualified


def qualified_result_completion_events(
    root: Path,
    events: list[dict[str, Any]],
    operators: list[str] | tuple[str, ...],
) -> list[dict[str, Any]]:
    """Use one current RESULT archive completion per real test version."""
    eligible = qualified_result_tasks(root, operators, events)
    latest_by_task: dict[tuple[str, str], dict[str, Any]] = {}
    for event in events:
        if str(event.get("event", "") or "") != "result_archived":
            continue
        task = (
            str(event.get("op", "") or ""),
            str(event.get("test_version", "") or ""),
        )
        if task not in eligible:
            continue
        event_dt = event_time(event)
        existing = latest_by_task.get(task)
        existing_dt = event_time(existing) if existing else None
        if event_dt is not None and (
            existing is None or existing_dt is None or event_dt > existing_dt
        ):
            latest_by_task[task] = event
    return sorted(
        latest_by_task.values(),
        key=lambda event: event_time(event)
        or datetime.min.replace(tzinfo=timezone.utc),
    )


def update_timeline_files(
    root: Path,
    config: DaemonConfig,
    snapshot: BoardSnapshot,
    decisions: tuple[GateDecision, ...],
    plan: DaemonPlan,
    resource_leases: tuple[dict[str, object], ...],
    action_liveness: dict[str, object] | None = None,
) -> dict[str, Any]:
    state_dir = root / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    timeline_path = state_dir / TIMELINE_FILE

    events = read_timeline_events(timeline_path)
    existing_ids = {
        str(event.get("event_id", "") or "")
        for event in events
        if str(event.get("event_id", "") or "")
    }
    candidates = collect_timeline_events(
        root, config, snapshot, decisions, plan, resource_leases, action_liveness
    )
    new_events = [
        event
        for event in candidates
        if event.get("event_id") and event["event_id"] not in existing_ids
    ]
    if new_events:
        ordered_new_events = sorted(new_events, key=event_sort_key)
        with timeline_path.open("a", encoding="utf-8") as fh:
            for event in ordered_new_events:
                fh.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
        events.extend(ordered_new_events)

    gaps = build_gap_snapshot(root, config, snapshot, events, new_events)
    full_flow = build_full_flow_latency(root, config, snapshot, events)
    write_full_flow_latency(root, full_flow)
    gaps["full_flow_latency"] = {
        "schema_version": full_flow.get("schema_version", ""),
        "generated_at": full_flow.get("generated_at", ""),
        "gate": full_flow.get("gate", {}),
        "aggregate": full_flow.get("aggregate", {}),
    }
    (state_dir / GAPS_JSON).write_text(
        json.dumps(gaps, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    (state_dir / GAPS_MD).write_text(render_gaps(gaps), encoding="utf-8")
    return gaps


def collect_timeline_events(
    root: Path,
    config: DaemonConfig,
    snapshot: BoardSnapshot,
    decisions: tuple[GateDecision, ...],
    plan: DaemonPlan,
    resource_leases: tuple[dict[str, object], ...],
    action_liveness: dict[str, object] | None,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    captured_at = snapshot.captured_at

    for decision in decisions:
        row = decision.row
        test_version = extract_test_version(row.next_command)
        if test_version:
            events.append(
                make_event(
                    "board_gate",
                    captured_at,
                    row.op,
                    test_version,
                    f"board_gate|{row.op}|{test_version}|{row.gate_stage}|{row.next_owner}",
                    observed_at=captured_at,
                    gate_stage=row.gate_stage,
                    next_owner=row.next_owner,
                    action=decision.action.value,
                    wakeups=row.wakeups,
                )
            )

    if plan.selected:
        selected_time = str((action_liveness or {}).get("first_seen_at") or captured_at)
        events.append(
            make_event(
                "selected_action",
                selected_time,
                plan.selected.row.op,
                extract_test_version(plan.selected.row.next_command),
                f"selected_action|{plan.selected.action_id}",
                observed_at=captured_at,
                action=plan.selected.action.value,
                action_id=plan.selected.action_id,
                command=plan.selected.command,
                gate_stage=plan.selected.row.gate_stage,
                next_owner=plan.selected.row.next_owner,
            )
        )

    for lease in resource_leases:
        op = str(lease.get("op", "") or "")
        action_id = str(lease.get("action_id", "") or "")
        test_version = extract_test_version(action_id)
        acquired = str(lease.get("acquired_at", "") or captured_at)
        events.append(
            make_event(
                "resource_lease_acquired",
                acquired,
                op,
                test_version,
                f"resource_lease_acquired|{lease.get('resource_id', '')}|{action_id}",
                observed_at=captured_at,
                action_id=action_id,
                resource_id=str(lease.get("resource_id", "") or ""),
                resource_type=str(lease.get("resource_type", "") or ""),
                pid=lease.get("pid", ""),
            )
        )

    for obs in snapshot.transport:
        events.extend(transport_events(root, obs, captured_at))

    events.extend(scan_solver_candidate_events(root, config))
    events.extend(scan_submit_events(root, config))
    events.extend(scan_result_events(root, config))
    events.extend(scan_gitpartner_status_events(root, config))
    events.extend(read_engine_pump_events(root, config))
    events.extend(read_daemon_runtime_events(root, config))
    events.extend(read_solver_ack_events(root, config))
    events.extend(read_execute_worker_events(root, config))
    return [event for event in events if event.get("time")]


def scan_solver_candidate_events(
    root: Path, config: DaemonConfig
) -> list[dict[str, Any]]:
    """Find the preserved Solver handoff artifact before daemon preparation.

    prepare-submit copies VERSION.md with metadata preservation, so the mtime of
    pending_snapshot/VERSION.md remains the same after submit/result archival.
    That is a tighter E2E start anchor than the end of the Codex turn: Solver
    often writes the candidate before finishing its explanatory response.
    """

    candidates: dict[tuple[str, str], tuple[float, Path]] = {}

    def keep_earliest(op: str, version: str, path: Path) -> None:
        try:
            modified_at = path.stat().st_mtime
        except OSError:
            return
        key = (op, version)
        previous = candidates.get(key)
        if previous is None or modified_at < previous[0]:
            candidates[key] = (modified_at, path)

    for op in observed_operators(config):
        pending_root = root / "TestUtils" / "pending" / op
        if pending_root.is_dir():
            for version_dir in pending_root.iterdir():
                if version_dir.is_dir():
                    keep_earliest(op, version_dir.name, version_dir / "VERSION.md")

        submit_root = root / "TestUtils" / "submit" / op
        if submit_root.is_dir():
            for version_dir in submit_root.iterdir():
                if version_dir.is_dir():
                    keep_earliest(
                        op,
                        version_dir.name,
                        version_dir / "pending_snapshot" / "VERSION.md",
                    )

        result_root = root / "operators_testresult" / op
        if result_root.is_dir():
            for version_dir in result_root.iterdir():
                if version_dir.is_dir():
                    keep_earliest(
                        op,
                        version_dir.name,
                        version_dir
                        / "submit_snapshot"
                        / "pending_snapshot"
                        / "VERSION.md",
                    )

    return [
        make_event(
            "solver_candidate_ready",
            timestamp_iso(modified_at),
            op,
            version,
            f"solver_candidate_ready|{op}|{version}",
            source=relpath(path, root),
            anchor="pending-version-mtime",
        )
        for (op, version), (modified_at, path) in sorted(candidates.items())
    ]


def transport_events(
    root: Path, obs: TransportObservation, captured_at: str
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    request_id = obs.request_id or ""
    base = f"{obs.op}|{obs.test_version}|{request_id}"
    if obs.first_observed_at_utc:
        events.append(
            make_event(
                "transport_observed",
                obs.first_observed_at_utc,
                obs.op,
                obs.test_version,
                f"transport_observed|{base}",
                observed_at=captured_at,
                request_id=request_id,
                state=obs.state,
                remote_feedback_status=obs.remote_feedback_status,
                output_status_path=obs.output_status_path,
                heartbeat_path=obs.heartbeat_path,
            )
        )
    if obs.last_feedback_at_utc:
        events.append(
            make_event(
                "transport_feedback",
                obs.last_feedback_at_utc,
                obs.op,
                obs.test_version,
                f"transport_feedback|{base}|{obs.last_feedback_at_utc}",
                observed_at=captured_at,
                request_id=request_id,
                state=obs.state,
                remote_feedback_status=obs.remote_feedback_status,
                terminal=obs.terminal,
            )
        )
    if obs.terminal:
        events.append(
            make_event(
                "transport_terminal",
                captured_at,
                obs.op,
                obs.test_version,
                f"transport_terminal|{base}",
                observed_at=captured_at,
                reported_feedback_at=obs.last_feedback_at_utc,
                request_id=request_id,
                state=obs.state,
                remote_feedback_status=obs.remote_feedback_status,
            )
        )
    return events


def scan_submit_events(root: Path, config: DaemonConfig) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for op in observed_operators(config):
        submit_root = root / "TestUtils" / "submit" / op
        if not submit_root.exists():
            continue
        for submit_md in submit_root.glob("*/SUBMIT.md"):
            test_version = submit_md.parent.name
            events.append(
                make_event(
                    "submit_prepared",
                    mtime_iso(submit_md),
                    op,
                    test_version,
                    f"submit_prepared|{op}|{test_version}",
                    source=relpath(submit_md, root),
                )
            )
    return events


def scan_result_events(root: Path, config: DaemonConfig) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for op in observed_operators(config):
        result_root = root / "operators_testresult" / op
        if not result_root.exists():
            continue
        for result_md in result_root.glob("*/RESULT.md"):
            test_version = result_md.parent.name
            result_time = mtime_iso(result_md)
            events.append(
                make_event(
                    "result_archived",
                    result_time,
                    op,
                    test_version,
                    f"result_archived|{op}|{test_version}|{result_time}",
                    source=relpath(result_md, root),
                )
            )
    return events


def scan_gitpartner_status_events(
    root: Path, config: DaemonConfig
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    output_root = root / "GitPartner" / "output"
    if not output_root.exists():
        return events
    for status_path in output_root.glob("*/status.json"):
        status = read_json(status_path)
        if not status:
            continue
        request_id = str(status.get("request_id", "") or status_path.parent.name)
        parsed = parse_request_id(observed_operators(config), request_id)
        if not parsed:
            continue
        op, test_version = parsed
        for event_name, field in (
            ("gp_started", "started_at"),
            ("gp_dispatched", "dispatched_at"),
            ("gp_finished", "finished_at"),
        ):
            timestamp = str(status.get(field, "") or "")
            if not timestamp:
                continue
            events.append(
                make_event(
                    event_name,
                    timestamp,
                    op,
                    test_version,
                    f"{event_name}|{request_id}",
                    request_id=request_id,
                    state=str(status.get("state", "") or ""),
                    client_state=str(status.get("client_state", "") or ""),
                    exit_code=status.get("exit_code", ""),
                    observed_at=mtime_iso(status_path),
                    source=relpath(status_path, root),
                )
            )
    return events


def read_solver_ack_events(root: Path, config: DaemonConfig) -> list[dict[str, Any]]:
    state = read_trigger_ack_state(root)
    sent = state.get("sent", {}) if isinstance(state.get("sent"), dict) else {}
    events: list[dict[str, Any]] = []
    for key, record in sent.items():
        if not isinstance(record, dict):
            continue
        op = str(key).split("|", 1)[0]
        if op not in observed_operators(config):
            continue
        updated_at = str(record.get("updated_at", "") or "")
        if not updated_at:
            continue
        status = str(record.get("status", "") or "")
        turn_id = str(record.get("turn_id", "") or "")
        test_version = extract_test_version(str(key))
        events.append(
            make_event(
                "solver_trigger_ack",
                updated_at,
                op,
                test_version,
                f"solver_trigger_ack|{key}|{status}|{turn_id}",
                trigger_key=str(key),
                status=status,
                thread_id=str(record.get("thread_id", "") or ""),
                turn_id=turn_id,
                delivery=str(record.get("delivery", "") or ""),
                ide_panel_visible=bool(record.get("ide_panel_visible", False)),
            )
        )
    return events


def read_execute_worker_events(
    root: Path, config: DaemonConfig
) -> list[dict[str, Any]]:
    path = root / "TestUtils" / "tester_daemon" / "execute_worker_events.jsonl"
    events: list[dict[str, Any]] = []
    for record in read_jsonl(path):
        op = str(record.get("op", "") or "")
        if op not in observed_operators(config):
            continue
        action_id = str(record.get("action_id", "") or "")
        event_name = str(record.get("event", "") or "")
        event_time = str(record.get("time", "") or "")
        if not event_name or not event_time:
            continue
        events.append(
            make_event(
                event_name,
                event_time,
                op,
                extract_test_version(action_id),
                f"{event_name}|{action_id}|{event_time}",
                action=str(record.get("action", "") or ""),
                action_id=action_id,
                returncode=record.get("returncode", ""),
                pid=record.get("pid", ""),
            )
        )
    return events


def read_engine_pump_events(root: Path, config: DaemonConfig) -> list[dict[str, Any]]:
    path = root / "TestUtils" / "tester_daemon" / "engine_pump_events.jsonl"
    allowed = set(observed_operators(config))
    events: list[dict[str, Any]] = []
    mapping = {
        "engine_outbox_enqueued": "engine_enqueued",
        "engine_accept_started": "engine_accept_started",
        "engine_job_admitted": "engine_admitted",
        "engine_job_terminal_returned": "engine_terminal",
        "engine_result_ingest_started": "engine_ingest_started",
        "engine_credit_replenished": "engine_replenished",
        "engine_result_ingested": "engine_result_ingested",
    }
    for record in read_jsonl(path):
        kind = str(record.get("kind") or "")
        event_name = mapping.get(kind)
        op = str(record.get("operator") or record.get("op") or "")
        version = str(record.get("test_version") or "")
        if event_name is None or op not in allowed or not version:
            continue
        if event_name == "engine_admitted":
            timestamp = str(record.get("accepted_at") or record.get("time") or "")
        elif event_name == "engine_terminal":
            timestamp = str(record.get("terminal_at") or record.get("time") or "")
        elif event_name == "engine_replenished":
            timestamp = str(record.get("replenished_at") or record.get("time") or "")
        else:
            timestamp = str(record.get("time") or "")
        job_id = str(record.get("engine_job_id") or "")
        attempt_id = str(record.get("attempt_id") or "")
        events.append(
            make_event(
                event_name,
                timestamp,
                op,
                version,
                f"{event_name}|{job_id}|{attempt_id}",
                request_id=str(record.get("request_id") or ""),
                engine_job_id=job_id,
                attempt_id=attempt_id,
                state=str(record.get("terminal_state") or ""),
                collect_request_id=str(record.get("collect_request_id") or ""),
                stage_history=list(record.get("stage_history", [])),
                stage_durations_seconds=dict(record.get("stage_durations_seconds", {})),
                released_engine_job_id=str(record.get("released_engine_job_id") or ""),
                replacement_engine_job_id=str(
                    record.get("replacement_engine_job_id") or ""
                ),
                terminal_to_replenish_seconds=record.get(
                    "terminal_to_replenish_seconds"
                ),
                return_to_replenish_seconds=record.get("return_to_replenish_seconds"),
                transport_request_id=str(record.get("transport_request_id") or ""),
                transport_elapsed_seconds=record.get("transport_elapsed_seconds"),
                accept_started_at=str(
                    record.get("accept_started_at") or record.get("started_at") or ""
                ),
                accept_completed_at=str(record.get("accept_completed_at") or ""),
                returned_at=str(record.get("returned_at") or ""),
                snapshot_started_at=str(record.get("snapshot_started_at") or ""),
                snapshot_completed_at=str(record.get("snapshot_completed_at") or ""),
                engine_observed_at=str(record.get("engine_observed_at") or ""),
                ingest_started_at=str(
                    record.get("ingest_started_at") or record.get("started_at") or ""
                ),
                ingested_at=str(record.get("ingested_at") or ""),
                observed_at=str(record.get("time") or ""),
                source=relpath(path, root),
            )
        )
    return events


def read_daemon_runtime_events(root: Path, config: DaemonConfig) -> list[dict[str, Any]]:
    path = root / "TestUtils" / "tester_daemon" / "daemon_runtime_events.jsonl"
    allowed = set(observed_operators(config))
    events: list[dict[str, Any]] = []
    for record in read_jsonl(path):
        if str(record.get("event") or "") != "engine_dispatch_enqueued":
            continue
        op = str(record.get("op") or "")
        version = str(record.get("test_version") or "")
        timestamp = str(record.get("time") or "")
        if op not in allowed or not version or not timestamp:
            continue
        attempt_index = int(record.get("attempt_index", 1) or 1)
        events.append(
            make_event(
                "engine_dispatch_enqueued",
                timestamp,
                op,
                version,
                f"engine_dispatch_enqueued|{op}|{version}|{attempt_index}|{timestamp}",
                action=str(record.get("action") or "dispatch_submit"),
                action_id=f"{op}|{version}|dispatch_submit|engine-compatibility",
                execution_profile=str(record.get("execution_profile") or ""),
                attempt_index=attempt_index,
            )
        )
    return events


def build_gap_snapshot(
    root: Path,
    config: DaemonConfig,
    snapshot: BoardSnapshot,
    events: list[dict[str, Any]],
    new_events: list[dict[str, Any]],
) -> dict[str, Any]:
    now = parse_time(snapshot.captured_at) or datetime.now(timezone.utc)
    total_event_count = len(events)
    events = [
        event
        for event in events
        if event.get("event") != "result_archived"
        or not str(event.get("source", "") or "")
        or (root / str(event.get("source", "") or "")).exists()
    ]
    events = gap_analysis_events(events, config, now)
    rows_by_op = {row.op: row for row in snapshot.rows}
    by_op_version: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for event in events:
        op = str(event.get("op", "") or "")
        version = str(event.get("test_version", "") or "")
        if op and version:
            by_op_version.setdefault((op, version), []).append(event)

    recent_result_gaps: list[dict[str, Any]] = []
    operators: dict[str, Any] = {}
    nonterminal_transport = [
        obs for obs in snapshot.transport if obs.terminal is not True
    ]
    resource_idle = not nonterminal_transport
    state_leases = read_json(root / "TestUtils" / "tester_daemon" / "leases.json")
    leases = (
        state_leases.get("leases", [])
        if isinstance(state_leases.get("leases"), list)
        else []
    )
    if leases:
        resource_idle = False

    for op in config.operators:
        op_events = [event for event in events if event.get("op") == op]
        latest_result = latest_event(op_events, "result_archived")
        latest_dispatch = latest_local_dispatch(op_events) or latest_remote_dispatch(
            op_events, now
        )
        latest_solver_ack = latest_event(op_events, "solver_trigger_ack")
        latest_result_version = (
            str(latest_result.get("test_version", "") or "") if latest_result else ""
        )
        ide_ack_for_latest_result = first_ide_visible_solver_ack(
            [
                event
                for event in op_events
                if event.get("event") == "solver_trigger_ack"
                and event.get("test_version") == latest_result_version
                and event_time(event)
                and event_time(latest_result)
                and event_time(event) >= event_time(latest_result)
            ],
            latest_result,
        )
        next_dispatch_after_latest_result = first_dispatch_after(
            op_events, latest_result, latest_result_version, now
        )
        active_transport = [
            obs
            for obs in snapshot.transport
            if obs.op == op and obs.terminal is not True
        ]
        active_age = None
        if active_transport:
            start_event = latest_local_dispatch(op_events) or latest_remote_dispatch(
                op_events, now
            )
            start_time = event_time(start_event)
            if start_time:
                active_age = max(0, int((now - start_time).total_seconds()))
        result_to_solver_ide_active = seconds_between(
            latest_result, ide_ack_for_latest_result
        )
        result_to_next_dispatch = seconds_between(
            latest_result, next_dispatch_after_latest_result
        )
        operators[op] = {
            "gate_stage": rows_by_op.get(op).gate_stage if op in rows_by_op else "",
            "next_owner": rows_by_op.get(op).next_owner if op in rows_by_op else "",
            "latest_result": compact_event(latest_result),
            "latest_dispatch": compact_event(latest_dispatch),
            "latest_solver_ide_active": compact_event(
                ide_ack_for_latest_result
            ),
            "latest_solver_ack": compact_event(latest_solver_ack),
            "result_to_solver_ide_active_seconds": result_to_solver_ide_active,
            # Compatibility alias. This is the first IDE-visible nonterminal
            # acknowledgement, never the later solver completion timestamp.
            "result_to_solver_ack_seconds": result_to_solver_ide_active,
            "result_to_next_dispatch_seconds": result_to_next_dispatch,
            "active_run_age_seconds": active_age,
        }

        for (event_op, version), version_events in by_op_version.items():
            if event_op != op:
                continue
            result_event = latest_event(version_events, "result_archived")
            if not result_event:
                continue
            ack_event = first_ide_visible_solver_ack(version_events, result_event)
            next_dispatch = first_dispatch_after(op_events, result_event, version, now)
            recent_result_gaps.append(
                {
                    "op": op,
                    "test_version": version,
                    "result_at": result_event.get("time", ""),
                    "solver_ack_at": ack_event.get("time", "") if ack_event else "",
                    "solver_ide_active_at": (
                        ack_event.get("time", "") if ack_event else ""
                    ),
                    "next_dispatch_at": (
                        next_dispatch.get("time", "") if next_dispatch else ""
                    ),
                    "result_to_solver_ack_seconds": seconds_between(
                        result_event, ack_event
                    ),
                    "result_to_solver_ide_active_seconds": seconds_between(
                        result_event, ack_event
                    ),
                    "result_to_next_dispatch_seconds": seconds_between(
                        result_event, next_dispatch
                    ),
                }
            )

    recent_result_gaps.sort(
        key=lambda item: parse_time(str(item.get("result_at", "") or ""))
        or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    idle_since = None
    if resource_idle:
        current_or_past_events = [
            event
            for event in events
            if not event_time(event) or event_time(event) <= now
        ]
        latest_activity = latest_any(
            current_or_past_events,
            {
                "resource_lease_acquired",
                "gp_started",
                "gp_dispatched",
                "gp_finished",
                "transport_terminal",
                "result_archived",
                "execute_worker_started",
                "execute_worker_command_started",
                "execute_worker_command_finished",
            },
        )
        latest_activity_time = event_time(latest_activity)
        if latest_activity_time:
            idle_since = max(0, int((now - latest_activity_time).total_seconds()))

    threshold = int(config.policy.get("test_idle_stale_seconds", 480) or 480)
    result_ide_active_values = [
        int(item["result_to_solver_ide_active_seconds"])
        for item in recent_result_gaps
        if isinstance(item.get("result_to_solver_ide_active_seconds"), int)
    ]
    dispatch_values = [
        int(item["result_to_next_dispatch_seconds"])
        for item in recent_result_gaps
        if isinstance(item.get("result_to_next_dispatch_seconds"), int)
    ]
    window_seconds = int(
        config.policy.get("timeline_recent_window_seconds", 7200) or 7200
    )
    window_start = now - timedelta(seconds=window_seconds)
    window_result_gaps = [
        item
        for item in recent_result_gaps
        if (
            parse_time(str(item.get("result_at", "") or ""))
            or datetime.min.replace(tzinfo=timezone.utc)
        )
        >= window_start
    ]
    window_ide_active_values = [
        int(item["result_to_solver_ide_active_seconds"])
        for item in window_result_gaps
        if isinstance(item.get("result_to_solver_ide_active_seconds"), int)
    ]
    window_dispatch_values = [
        int(item["result_to_next_dispatch_seconds"])
        for item in window_result_gaps
        if isinstance(item.get("result_to_next_dispatch_seconds"), int)
    ]
    future_remote_offsets = [
        int((event_dt - now).total_seconds())
        for event in events
        if event.get("event") in REMOTE_DISPATCH_EVENTS
        for event_dt in [event_time(event)]
        if event_dt is not None and event_dt > now
    ]
    operator_metric_epoch = metric_epoch_for_config(root, config)
    completion_to_next_submit = build_completion_to_next_submit_snapshot(
        events,
        config,
        now,
        metric_epoch=operator_metric_epoch,
    )
    qualified_completion_to_next_submit = build_completion_to_next_submit_snapshot(
        events,
        config,
        now,
        completion_events=qualified_result_completion_events(
            root, events, config.operators
        ),
        metric_epoch=operator_metric_epoch,
    )
    operator_plugin_state = read_operator_plugin_state(root)
    return {
        "updated_at": snapshot.captured_at,
        "timeline_path": str(
            (root / "TestUtils" / "tester_daemon" / TIMELINE_FILE).relative_to(root)
        ),
        "new_event_count": len(new_events),
        "total_event_count": total_event_count,
        "analysis_event_count": len(events),
        "resource_idle": resource_idle,
        "test_idle_stale_seconds": threshold,
        "test_idle_window_seconds": idle_since,
        "max_result_to_solver_ide_active_seconds": (
            max(result_ide_active_values) if result_ide_active_values else None
        ),
        "max_result_to_solver_ack_seconds": (
            max(result_ide_active_values) if result_ide_active_values else None
        ),
        "max_result_to_next_dispatch_seconds": (
            max(dispatch_values) if dispatch_values else None
        ),
        "recent_window_seconds": window_seconds,
        "recent_window_result_count": len(window_result_gaps),
        "recent_window_max_result_to_solver_ide_active_seconds": (
            max(window_ide_active_values) if window_ide_active_values else None
        ),
        "recent_window_max_result_to_solver_ack_seconds": (
            max(window_ide_active_values) if window_ide_active_values else None
        ),
        "result_to_solver_ide_active_threshold_seconds": int(
            config.policy.get("result_to_solver_ide_active_max_seconds", 10) or 10
        ),
        "recent_window_max_result_to_next_dispatch_seconds": (
            max(window_dispatch_values) if window_dispatch_values else None
        ),
        "max_remote_clock_ahead_seconds": (
            max(future_remote_offsets) if future_remote_offsets else None
        ),
        "completion_to_next_submit": completion_to_next_submit,
        "qualified_completion_to_next_submit": qualified_completion_to_next_submit,
        "operator_plugin_state": operator_plugin_state,
        "operators": operators,
        "recent_result_gaps": recent_result_gaps[:20],
    }


def render_gaps(snapshot: dict[str, Any]) -> str:
    lines = [
        "# Scheduler Gaps",
        "",
        f"- updated_at: {snapshot.get('updated_at', '')}",
        f"- timeline_path: `{snapshot.get('timeline_path', '')}`",
        f"- new_event_count: {snapshot.get('new_event_count', 0)}",
        f"- total_event_count: {snapshot.get('total_event_count', 0)}",
        f"- analysis_event_count: {snapshot.get('analysis_event_count', 0)}",
        f"- resource_idle: {snapshot.get('resource_idle', False)}",
        f"- test_idle_window_seconds: {display_value(snapshot.get('test_idle_window_seconds'))}",
        f"- max_result_to_solver_ide_active_seconds: {display_value(snapshot.get('max_result_to_solver_ide_active_seconds'))}",
        f"- result_to_solver_ide_active_threshold_seconds: {display_value(snapshot.get('result_to_solver_ide_active_threshold_seconds'))}",
        f"- max_result_to_next_dispatch_seconds: {display_value(snapshot.get('max_result_to_next_dispatch_seconds'))}",
        f"- recent_window_seconds: {display_value(snapshot.get('recent_window_seconds'))}",
        f"- recent_window_result_count: {display_value(snapshot.get('recent_window_result_count'))}",
        f"- recent_window_max_result_to_solver_ide_active_seconds: {display_value(snapshot.get('recent_window_max_result_to_solver_ide_active_seconds'))}",
        f"- recent_window_max_result_to_next_dispatch_seconds: {display_value(snapshot.get('recent_window_max_result_to_next_dispatch_seconds'))}",
        f"- max_remote_clock_ahead_seconds: {display_value(snapshot.get('max_remote_clock_ahead_seconds'))}",
        "",
        "## Completion To Next Submit",
        "",
    ]
    submit_gap = snapshot.get("completion_to_next_submit", {})
    if isinstance(submit_gap, dict) and submit_gap:
        lines.extend(
            [
                f"- window_size: {display_value(submit_gap.get('window_size'))}",
                f"- threshold_seconds: {display_value(submit_gap.get('threshold_seconds'))}",
                f"- sample_count: {display_value(submit_gap.get('sample_count'))}",
                f"- complete_window: {submit_gap.get('complete_window', False)}",
                f"- ok: {submit_gap.get('ok', False)}",
                f"- max_gap_seconds: {display_value(submit_gap.get('max_gap_seconds'))}",
                f"- violation_count: {display_value(submit_gap.get('violation_count'))}",
                "",
            ]
        )
        gaps = submit_gap.get("gaps", [])
        if isinstance(gaps, list) and gaps:
            lines.extend(
                [
                    "| completed_op | completed_version | completed_at | next_submit_op | next_submit_version | next_submit_at | gap_s | root_cause |",
                    "|---|---|---|---|---|---|---:|---|",
                ]
            )
            for item in gaps:
                if not isinstance(item, dict):
                    continue
                cause = str(item.get("root_cause", "") or "").replace("|", "&#124;")
                lines.append(
                    f"| {item.get('completed_op', '-') or '-'} | {item.get('completed_version', '-') or '-'} | "
                    f"{item.get('completed_at', '-') or '-'} | {item.get('next_submit_op', '-') or '-'} | "
                    f"{item.get('next_submit_version', '-') or '-'} | {item.get('next_submit_at', '-') or '-'} | "
                    f"{display_value(item.get('gap_seconds'))} | {cause[:140]} |"
                )
            lines.append("")
    else:
        lines.extend(["- unavailable", ""])
    lines.extend(
        [
            "## Operators",
            "",
            "| op | gate | owner | latest_result | result_to_solver_ide_active_s | result_to_next_dispatch_s | active_run_age_s | first_solver_ide_active | latest_solver_ack | latest_dispatch |",
            "|---|---|---|---|---:|---:|---:|---|---|---|",
        ]
    )
    operators = snapshot.get("operators", {})
    if isinstance(operators, dict):
        for op, data in operators.items():
            if not isinstance(data, dict):
                continue
            result = (
                data.get("latest_result", {})
                if isinstance(data.get("latest_result"), dict)
                else {}
            )
            ack = (
                data.get("latest_solver_ack", {})
                if isinstance(data.get("latest_solver_ack"), dict)
                else {}
            )
            ide_active = (
                data.get("latest_solver_ide_active", {})
                if isinstance(data.get("latest_solver_ide_active"), dict)
                else {}
            )
            dispatch = (
                data.get("latest_dispatch", {})
                if isinstance(data.get("latest_dispatch"), dict)
                else {}
            )
            lines.append(
                f"| {op} | {data.get('gate_stage', '')} | {data.get('next_owner', '')} | "
                f"{result.get('test_version', '-') or '-'} @ {result.get('time', '-') or '-'} | "
                f"{display_value(data.get('result_to_solver_ide_active_seconds'))} | "
                f"{display_value(data.get('result_to_next_dispatch_seconds'))} | "
                f"{display_value(data.get('active_run_age_seconds'))} | "
                f"{ide_active.get('test_version', '-') or '-'} @ {ide_active.get('time', '-') or '-'} | "
                f"{ack.get('test_version', '-') or '-'} @ {ack.get('time', '-') or '-'} | "
                f"{dispatch.get('test_version', '-') or '-'} @ {dispatch.get('time', '-') or '-'} |"
            )
    lines.extend(["", "## Recent Result Gaps", ""])
    lines.extend(
        [
            "| op | version | result_at | solver_ide_active_at | next_dispatch_at | result_to_solver_ide_active_s | result_to_next_dispatch_s |",
            "|---|---|---|---|---|---:|---:|",
        ]
    )
    for item in (
        snapshot.get("recent_result_gaps", [])
        if isinstance(snapshot.get("recent_result_gaps"), list)
        else []
    ):
        if not isinstance(item, dict):
            continue
        lines.append(
            f"| {item.get('op', '')} | {item.get('test_version', '')} | "
            f"{item.get('result_at', '') or '-'} | {item.get('solver_ide_active_at', '') or '-'} | "
            f"{item.get('next_dispatch_at', '') or '-'} | "
            f"{display_value(item.get('result_to_solver_ide_active_seconds'))} | "
            f"{display_value(item.get('result_to_next_dispatch_seconds'))} |"
        )
    lines.append("")
    return "\n".join(lines)


def gap_analysis_events(
    events: list[dict[str, Any]],
    config: DaemonConfig,
    now: datetime,
) -> list[dict[str, Any]]:
    """Bound quadratic gap analysis while preserving the full audit timeline."""

    by_id: dict[str, dict[str, Any]] = {}
    anonymous: list[dict[str, Any]] = []
    for event in events:
        event_id = str(event.get("event_id", "") or "")
        if event_id:
            by_id[event_id] = event
        else:
            anonymous.append(event)
    ordered = sorted([*by_id.values(), *anonymous], key=event_sort_key)
    limit = max(
        100, int(config.policy.get("timeline_analysis_event_limit", 2000) or 2000)
    )
    minimum = max(
        100, int(config.policy.get("timeline_analysis_min_events", 500) or 500)
    )
    minimum = min(minimum, limit)
    window_seconds = max(
        60,
        int(config.policy.get("timeline_recent_window_seconds", 7200) or 7200),
    )
    window_start = now - timedelta(seconds=window_seconds)
    recent = [
        event
        for event in ordered
        if (event_time(event) or datetime.min.replace(tzinfo=timezone.utc))
        >= window_start
    ]
    if len(recent) < minimum:
        recent = ordered[-minimum:]
    if len(recent) > limit:
        recent = recent[-limit:]
    return recent


def make_event(
    event: str,
    time: str,
    op: str,
    test_version: str,
    event_id: str,
    **details: Any,
) -> dict[str, Any]:
    payload = {
        "event_id": event_id,
        "event": event,
        "time": normalize_time(time),
        "op": op,
        "test_version": test_version,
    }
    for key, value in details.items():
        if value not in ("", None, [], {}):
            payload[key] = value
    return payload


def read_existing_event_ids(path: Path) -> set[str]:
    ids: set[str] = set()
    for record in read_jsonl(path):
        event_id = str(record.get("event_id", "") or "")
        if event_id:
            ids.add(event_id)
    return ids


def read_timeline_events(path: Path) -> list[dict[str, Any]]:
    return read_jsonl(path)


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        stat = path.stat()
    except OSError:
        return []
    cached = _JSONL_CACHE.get(path)
    if cached and cached[0] == stat.st_mtime_ns and cached[1] == stat.st_size:
        return cached[2]
    records: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    for line in lines:
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict):
            records.append(data)
    _JSONL_CACHE[path] = (stat.st_mtime_ns, stat.st_size, records)
    return records


def latest_event(
    events: list[dict[str, Any]], event_name: str
) -> dict[str, Any] | None:
    return latest_any(
        [event for event in events if event.get("event") == event_name], {event_name}
    )


def first_ide_visible_solver_ack(
    events: list[dict[str, Any]],
    result_event: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Return the first IDE-visible handoff, not the later solver completion."""
    result_at = event_time(result_event)
    candidates = [
        event
        for event in events
        if event.get("event") == "solver_trigger_ack"
        and bool(event.get("ide_panel_visible"))
        and str(event.get("status", "") or "").lower()
        in {"sent", "delivered", "acked", "active", "inprogress", "running", "queued"}
        and event_time(event)
        and (result_at is None or event_time(event) >= result_at)
    ]
    if not candidates:
        return None
    return min(
        candidates,
        key=lambda event: event_time(event)
        or datetime.max.replace(tzinfo=timezone.utc),
    )


def latest_local_dispatch(events: list[dict[str, Any]]) -> dict[str, Any] | None:
    return latest_any(
        [event for event in events if is_local_dispatch(event)], LOCAL_DISPATCH_EVENTS
    )


def latest_remote_dispatch(
    events: list[dict[str, Any]], not_after: datetime
) -> dict[str, Any] | None:
    return latest_any(
        [
            event
            for event in events
            if event.get("event") in REMOTE_DISPATCH_EVENTS
            and event_time(event)
            and event_time(event) <= not_after
        ],
        REMOTE_DISPATCH_EVENTS,
    )


def first_dispatch_after(
    events: list[dict[str, Any]],
    result_event: dict[str, Any] | None,
    result_version: str,
    now: datetime,
) -> dict[str, Any] | None:
    result_time = event_time(result_event)
    if result_time is None:
        return None
    local = first_after(
        [
            event
            for event in events
            if is_local_dispatch(event)
            and event.get("test_version") != result_version
            and event_time(event)
            and event_time(event) > result_time
        ]
    )
    if local:
        return local
    return first_after(
        [
            event
            for event in events
            if event.get("event") in REMOTE_DISPATCH_EVENTS
            and event.get("test_version") != result_version
            and event_time(event)
            and result_time < event_time(event) <= now
        ]
    )


def build_completion_to_next_submit_snapshot(
    events: list[dict[str, Any]],
    config: DaemonConfig,
    now: datetime,
    completion_events: list[dict[str, Any]] | None = None,
    metric_epoch: datetime | None = None,
) -> dict[str, Any]:
    if len(config.operators) < 2:
        return {
            "applicable": False,
            "reason": "requires_at_least_two_active_operators",
            "active_operator_count": len(config.operators),
            "metric_epoch_at": (
                metric_epoch.isoformat(timespec="seconds") if metric_epoch else ""
            ),
            "window_size": 0,
            "threshold_seconds": int(
                config.policy.get("submit_gap_max_seconds", 10) or 10
            ),
            "sample_count": 0,
            "complete_window": False,
            "window_started_at": "",
            "window_ended_at": "",
            "window_span_seconds": 0,
            "ok": True,
            "max_gap_seconds": None,
            "violation_count": 0,
            "violations": [],
            "gaps": [],
        }
    window_size = int(config.policy.get("submit_gap_window_size", 10) or 10)
    threshold = int(config.policy.get("submit_gap_max_seconds", 10) or 10)
    active_ops = {str(op) for op in config.operators if str(op)}
    completions = (
        completion_events
        if completion_events is not None
        else test_completion_attempt_events(events, now)
    )
    completions = [
        event
        for event in completions
        if str(event.get("op", "") or "") in active_ops
        and event_at_or_after(event, metric_epoch)
    ]
    scoped_events = [
        event for event in events if event_at_or_after(event, metric_epoch)
    ]
    submits = unique_actual_submit_events(scoped_events, now)
    analysis_limit = int(
        config.policy.get(
            "submit_gap_analysis_limit",
            window_size,
        )
        or 0
    )
    if analysis_limit > 0:
        completions_to_score = completions[-analysis_limit:]
    else:
        completions_to_score = completions
    gaps: list[dict[str, Any]] = []
    for completion in completions_to_score:
        complete_time = event_time(completion)
        if complete_time is None:
            continue
        covering_submit = active_submit_covering_completion(
            scoped_events,
            submits,
            completion,
            complete_time,
        )
        if covering_submit:
            next_submit = covering_submit
            gap_seconds = 0
            root_cause = (
                "within submit-gap target: another test was already submitted and "
                "still running when this completion arrived"
            )
        else:
            next_submit = first_after(
                [
                    submit
                    for submit in submits
                    if event_time(submit) and event_time(submit) > complete_time
                ]
            )
            gap_seconds = seconds_between(completion, next_submit)
            root_cause = submit_gap_root_cause(
                scoped_events, completion, next_submit, threshold
            )
        item = {
            "completed_op": completion.get("op", ""),
            "completed_version": completion.get("test_version", ""),
            "completed_at": completion.get("time", ""),
            "completion_event": completion.get("event", ""),
            "next_submit_op": next_submit.get("op", "") if next_submit else "",
            "next_submit_version": (
                next_submit.get("test_version", "") if next_submit else ""
            ),
            "next_submit_at": next_submit.get("time", "") if next_submit else "",
            "next_submit_event": next_submit.get("event", "") if next_submit else "",
            "gap_seconds": gap_seconds,
            "root_cause": root_cause,
            "recovery_action": submit_gap_recovery_action(next_submit, root_cause),
        }
        gaps.append(item)
    gaps.sort(
        key=lambda item: parse_time(str(item.get("completed_at", "") or ""))
        or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    window = gaps[:window_size] if window_size > 0 else []
    window_times = [
        parsed
        for item in window
        for parsed in [parse_time(str(item.get("completed_at", "") or ""))]
        if parsed is not None
    ]
    gap_values = [
        int(item["gap_seconds"])
        for item in window
        if isinstance(item.get("gap_seconds"), int)
    ]
    violations = [
        item
        for item in window
        if item.get("gap_seconds") is None
        or (
            isinstance(item.get("gap_seconds"), int)
            and int(item["gap_seconds"]) >= threshold
        )
    ]
    return {
        "applicable": True,
        "active_operator_count": len(config.operators),
        "metric_epoch_at": (
            metric_epoch.isoformat(timespec="seconds") if metric_epoch else ""
        ),
        "window_size": window_size,
        "threshold_seconds": threshold,
        "sample_count": len(window),
        "complete_window": bool(window_size > 0 and len(window) >= window_size),
        "window_started_at": (
            min(window_times).isoformat(timespec="seconds") if window_times else ""
        ),
        "window_ended_at": (
            max(window_times).isoformat(timespec="seconds") if window_times else ""
        ),
        "window_span_seconds": (
            int((max(window_times) - min(window_times)).total_seconds())
            if window_times
            else 0
        ),
        "ok": bool(
            window and not violations and len(window) >= min(window_size, len(gaps))
        ),
        "max_gap_seconds": max(gap_values) if gap_values else None,
        "violation_count": len(violations),
        "violations": violations[:10],
        "gaps": window,
    }


def event_at_or_after(event: dict[str, Any], epoch: datetime | None) -> bool:
    if epoch is None:
        return True
    timestamp = event_time(event)
    return timestamp is not None and timestamp >= epoch


def active_submit_covering_completion(
    events: list[dict[str, Any]],
    submits: list[dict[str, Any]],
    completion: dict[str, Any],
    complete_time: datetime,
) -> dict[str, Any] | None:
    active: list[dict[str, Any]] = []
    for submit in submits:
        submit_time = event_time(submit)
        if submit_time is None or submit_time > complete_time:
            continue
        same_task = same_test_task(submit, completion)
        if same_task:
            overlapping_attempt_count = sum(
                1
                for candidate in submits
                if same_test_task(candidate, completion)
                and event_time(candidate) is not None
                and event_time(candidate) <= complete_time
            )
            if (
                overlapping_attempt_count < 2
                or not has_distinct_request_inflight_evidence(
                    events,
                    completion,
                    submit_time,
                    complete_time,
                )
            ):
                continue
        task = (
            str(submit.get("op", "") or ""),
            str(submit.get("test_version", "") or ""),
        )
        if not same_task and submit_completed_between(
            events, task, submit_time, complete_time
        ):
            continue
        if not has_inflight_evidence_at_or_after(events, submit, complete_time):
            continue
        active.append(submit)
    if not active:
        return None
    return max(
        active,
        key=lambda item: event_time(item) or datetime.min.replace(tzinfo=timezone.utc),
    )


def has_distinct_request_inflight_evidence(
    events: list[dict[str, Any]],
    completion: dict[str, Any],
    submit_time: datetime,
    complete_time: datetime,
) -> bool:
    completed_request = str(completion.get("request_id", "") or "")
    task = (
        str(completion.get("op", "") or ""),
        str(completion.get("test_version", "") or ""),
    )
    if not completed_request:
        return False
    for event in events:
        if str(event.get("event", "") or "") != "transport_observed":
            continue
        event_task = (
            str(event.get("op", "") or ""),
            str(event.get("test_version", "") or ""),
        )
        request_id = str(event.get("request_id", "") or "")
        observed_time = event_time(event)
        if (
            event_task == task
            and request_id
            and request_id != completed_request
            and observed_time is not None
            and observed_time >= submit_time
            and observed_time >= complete_time
        ):
            return True
    return False


def submit_completed_between(
    events: list[dict[str, Any]],
    task: tuple[str, str],
    submit_time: datetime,
    complete_time: datetime,
) -> bool:
    for event in events:
        event_task = (
            str(event.get("op", "") or ""),
            str(event.get("test_version", "") or ""),
        )
        if event_task != task:
            continue
        if str(event.get("event", "") or "") not in TEST_COMPLETION_EVENTS:
            continue
        event_dt = event_time(event)
        if event_dt is not None and submit_time <= event_dt <= complete_time:
            return True
    return False


def has_inflight_evidence_at_or_after(
    events: list[dict[str, Any]],
    submit: dict[str, Any],
    complete_time: datetime,
) -> bool:
    submit_task = (
        str(submit.get("op", "") or ""),
        str(submit.get("test_version", "") or ""),
    )
    submit_time = event_time(submit)
    if (
        str(submit.get("event", "") or "") == "engine_admitted"
        and submit_time is not None
        and not submit_completed_between(
            events, submit_task, submit_time, complete_time
        )
    ):
        # An engine acceptance receipt opens an explicit attempt interval.  It
        # remains active until an engine terminal/result event closes it; unlike
        # the legacy transport, it does not emit periodic waiting-client events.
        return True
    for event in events:
        event_task = (
            str(event.get("op", "") or ""),
            str(event.get("test_version", "") or ""),
        )
        if event_task != submit_task:
            continue
        event_dt = event_time(event)
        if event_dt is None or event_dt < complete_time:
            continue
        event_name = str(event.get("event", "") or "")
        if event_name in {
            "transport_observed",
            "transport_feedback",
            "gp_started",
            "gp_dispatched",
        }:
            return True
        if event_name in {
            "execute_worker_command_finished",
            "execute_worker_pruned",
        } and is_actual_submit_dispatch(event):
            return True
    return False


def unique_test_completion_events(
    events: list[dict[str, Any]], now: datetime
) -> list[dict[str, Any]]:
    return test_completion_attempt_events(events, now)


def test_completion_attempt_events(
    events: list[dict[str, Any]], now: datetime
) -> list[dict[str, Any]]:
    by_attempt: dict[tuple[str, ...], dict[str, Any]] = {}
    failed_transport_requests = failed_transport_request_ids(events, now)
    submits = unique_actual_submit_events(events, now)
    submits_by_task: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for submit in submits:
        task = (
            str(submit.get("op", "") or ""),
            str(submit.get("test_version", "") or ""),
        )
        submits_by_task.setdefault(task, []).append(submit)
    for raw_event in events:
        event = normalized_completion_event(raw_event)
        if event.get("event") not in TEST_COMPLETION_EVENTS:
            continue
        event_name = str(event.get("event", "") or "")
        if (
            event_name == "gp_finished"
            and str(event.get("request_id", "") or "") in failed_transport_requests
        ):
            continue
        event_dt = event_time(event)
        if event_dt is None or event_dt > now:
            continue
        op = str(event.get("op", "") or "")
        version = str(event.get("test_version", "") or "")
        if not op or not version:
            continue
        task = (op, version)
        attempt_submit = latest_submit_for_completion(
            submits_by_task.get(task, []), event_dt
        )
        if (
            event_name == "result_archived"
            and attempt_submit is None
            and result_archive_has_terminal_completion(
                events,
                event,
                now,
                failed_transport_requests,
            )
        ):
            continue
        if attempt_submit is not None:
            key = ("submit", *submit_attempt_identity(attempt_submit))
        else:
            request_id = str(event.get("request_id", "") or "")
            key = (
                ("request", op, version, request_id)
                if request_id
                # A bounded analysis window may retain repeated archive
                # observations after the original submit event has fallen out
                # of scope. They are one completion, not new test attempts.
                else ("completion", op, version)
            )
        existing = by_attempt.get(key)
        existing_dt = event_time(existing) if existing is not None else None
        if existing is None or existing_dt is None:
            by_attempt[key] = event
            continue
        event_rank = completion_rank(event)
        existing_rank = completion_rank(existing)
        if event_rank > existing_rank or (
            event_rank == existing_rank and event_dt < existing_dt
        ):
            by_attempt[key] = event
    return sorted(
        by_attempt.values(),
        key=lambda event: event_time(event)
        or datetime.min.replace(tzinfo=timezone.utc),
    )


def latest_submit_for_completion(
    submits: list[dict[str, Any]], completion_time: datetime
) -> dict[str, Any] | None:
    candidates = [
        submit
        for submit in submits
        if event_time(submit) is not None and event_time(submit) <= completion_time
    ]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda event: event_time(event)
        or datetime.min.replace(tzinfo=timezone.utc),
    )


def result_archive_has_terminal_completion(
    events: list[dict[str, Any]],
    result_event: dict[str, Any],
    now: datetime,
    failed_transport_requests: set[str],
) -> bool:
    result_dt = event_time(result_event)
    if result_dt is None or result_dt > now:
        return False
    task = (
        str(result_event.get("op", "") or ""),
        str(result_event.get("test_version", "") or ""),
    )
    if not task[0] or not task[1]:
        return False
    for raw_event in events:
        event = normalized_completion_event(raw_event)
        event_name = str(event.get("event", "") or "")
        if event_name not in {"gp_finished", "transport_terminal", "engine_terminal"}:
            continue
        event_task = (
            str(event.get("op", "") or ""),
            str(event.get("test_version", "") or ""),
        )
        if event_task != task:
            continue
        if event_name == "transport_terminal" and terminal_state_is_failure(event):
            continue
        if (
            event_name == "gp_finished"
            and str(event.get("request_id", "") or "") in failed_transport_requests
        ):
            continue
        terminal_dt = event_time(event)
        if terminal_dt is None or terminal_dt > result_dt:
            continue
        if (
            result_dt - terminal_dt
        ).total_seconds() > RESULT_ARCHIVE_TERMINAL_DEDUPE_SECONDS:
            continue
        if same_task_submit_between(events, task, terminal_dt, result_dt):
            continue
        return True
    return False


def same_task_submit_between(
    events: list[dict[str, Any]],
    task: tuple[str, str],
    start: datetime,
    end: datetime,
) -> bool:
    for event in events:
        event_task = (
            str(event.get("op", "") or ""),
            str(event.get("test_version", "") or ""),
        )
        if event_task != task or not is_actual_submit_dispatch(event):
            continue
        event_dt = event_time(event)
        if event_dt is not None and start < event_dt <= end:
            return True
    return False


def failed_transport_request_ids(
    events: list[dict[str, Any]], now: datetime
) -> set[str]:
    failed: set[str] = set()
    for event in events:
        if str(event.get("event", "") or "") != "transport_terminal":
            continue
        event_dt = event_time(event)
        if event_dt is None or event_dt > now:
            continue
        request_id = str(event.get("request_id", "") or "")
        if request_id and terminal_state_is_failure(event):
            failed.add(request_id)
    return failed


def terminal_state_is_failure(event: dict[str, Any]) -> bool:
    state = str(event.get("state", "") or "").strip().lower()
    if not state:
        return False
    return state in {
        "failed",
        "failure",
        "error",
        "infra_fail",
        "timeout",
        "timed_out",
        "cancelled",
        "canceled",
    }


def normalized_completion_event(event: dict[str, Any]) -> dict[str, Any]:
    if str(event.get("event", "") or "") not in {"gp_finished", "transport_terminal"}:
        return event
    event_dt = event_time(event)
    observed_dt = parse_time(str(event.get("observed_at", "") or ""))
    if event_dt is None or observed_dt is None:
        return event
    if event_dt <= observed_dt + timedelta(seconds=5):
        return event
    normalized = dict(event)
    normalized["remote_time"] = str(event.get("time", "") or "")
    normalized["time"] = observed_dt.isoformat(timespec="seconds")
    normalized["clock_skew_adjusted"] = True
    return normalized


def unique_actual_submit_events(
    events: list[dict[str, Any]], now: datetime
) -> list[dict[str, Any]]:
    local_by_attempt: dict[tuple[str, ...], dict[str, Any]] = {}
    remote_by_task: dict[tuple[str, str], dict[str, Any]] = {}
    has_command_starts = any(
        str(event.get("event", "") or "") == "execute_worker_command_started"
        and is_actual_submit_dispatch(event)
        and event_time(event) is not None
        and event_time(event) <= now
        for event in events
    )
    for event in events:
        if not is_actual_submit_dispatch(event):
            continue
        event_dt = event_time(event)
        if event_dt is None or event_dt > now:
            continue
        op = str(event.get("op", "") or "")
        version = str(event.get("test_version", "") or "")
        if not op or not version:
            continue
        if str(event.get("event", "") or "") in LOCAL_ACTUAL_SUBMIT_EVENTS:
            if (
                has_command_starts
                and str(event.get("event", "") or "")
                != "execute_worker_command_started"
            ):
                continue
            key = submit_attempt_identity(event)
            existing = local_by_attempt.get(key)
            if existing is None or submit_event_rank(event) > submit_event_rank(
                existing
            ):
                local_by_attempt[key] = event
            elif submit_event_rank(event) == submit_event_rank(existing):
                existing_dt = event_time(existing)
                if existing_dt is None or event_dt < existing_dt:
                    local_by_attempt[key] = event
            continue
        key = (op, version)
        existing = remote_by_task.get(key)
        existing_dt = event_time(existing) if existing else None
        if existing is None or existing_dt is None or event_dt < existing_dt:
            remote_by_task[key] = event
    local_tasks = {
        (str(event.get("op", "") or ""), str(event.get("test_version", "") or ""))
        for event in local_by_attempt.values()
    }
    selected = list(local_by_attempt.values())
    selected.extend(
        event for task, event in remote_by_task.items() if task not in local_tasks
    )
    return sorted(
        selected,
        key=lambda event: event_time(event)
        or datetime.min.replace(tzinfo=timezone.utc),
    )


def submit_attempt_identity(event: dict[str, Any]) -> tuple[str, ...]:
    op = str(event.get("op", "") or "")
    version = str(event.get("test_version", "") or "")
    action_id = str(event.get("action_id", "") or "")
    event_dt = event_time(event)
    if (
        str(event.get("event", "") or "") == "execute_worker_command_started"
        and event_dt is not None
    ):
        return (op, version, action_id, event_dt.isoformat(timespec="seconds"))
    if action_id:
        return (op, version, action_id)
    request_id = str(event.get("request_id", "") or "")
    if request_id:
        return (op, version, request_id)
    return (
        op,
        version,
        (
            event_dt.isoformat(timespec="seconds")
            if event_dt is not None
            else str(event.get("time", "") or "")
        ),
    )


def submit_event_rank(event: dict[str, Any]) -> int:
    event_name = str(event.get("event", "") or "")
    if event_name == "execute_worker_command_started":
        return 3
    if event_name == "execute_worker_started":
        return 2
    if event_name == "resource_lease_acquired":
        return 1
    return 0


def completion_rank(event: dict[str, Any]) -> int:
    event_name = str(event.get("event", "") or "")
    if event_name == "engine_terminal":
        return 4
    if event_name == "result_archived":
        return 3
    if event_name == "gp_finished":
        return 2
    if event_name == "transport_terminal":
        return 1
    return 0


def same_test_task(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return str(left.get("op", "") or "") == str(right.get("op", "") or "") and str(
        left.get("test_version", "") or ""
    ) == str(right.get("test_version", "") or "")


def submit_gap_root_cause(
    events: list[dict[str, Any]],
    completion: dict[str, Any],
    next_submit: dict[str, Any] | None,
    threshold: int,
) -> str:
    gap = seconds_between(completion, next_submit)
    if next_submit is None:
        recovery_hold = latest_restore_hold_after(events, completion, None)
        if recovery_hold:
            return f"no later test submit observed because transport recovery is in backoff: {recovery_hold}"
        return "no later test submit observed after completion; daemon should inspect runnable gates and relay/worker health"
    if gap is None or gap < threshold:
        return "within submit-gap target"
    if same_test_task(completion, next_submit):
        return (
            "same test version was re-dispatched after a terminal/infra result; "
            "daemon transport recovery or retry path delayed the next submit"
        )
    recovery_hold = latest_restore_hold_after(events, completion, next_submit)
    if recovery_hold:
        return f"next submit delayed by transport recovery backoff: {recovery_hold}"
    active_turns = active_native_turns_overlapping(events, completion, next_submit)
    if active_turns:
        return (
            "next submit waited for solver/tester candidate production: "
            + "; ".join(active_turns[:3])
        )
    ack = first_after(
        [
            event
            for event in events
            if event.get("event") == "solver_trigger_ack"
            and event.get("op") == completion.get("op")
            and event_time(event)
            and event_time(completion)
            and event_time(event) > event_time(completion)
            and event_time(event) <= event_time(next_submit)
        ]
    )
    if ack is None:
        return "next submit was delayed and no solver ack was observed before it; check solver trigger delivery or pending production"
    return "next submit was delayed after solver ack; check daemon scheduler, casegen gate, resource lease, or transport worker"


def active_native_turns_overlapping(
    events: list[dict[str, Any]],
    completion: dict[str, Any],
    next_submit: dict[str, Any] | None,
) -> list[str]:
    gap_start = event_time(completion)
    gap_end = event_time(next_submit)
    if gap_start is None or gap_end is None:
        return []

    turns: dict[str, dict[str, Any]] = {}
    for event in events:
        event_name = str(event.get("event", "") or "")
        if event_name not in {"solver_trigger_ack", "tester_trigger_ack"}:
            continue
        turn_id = str(event.get("turn_id", "") or "")
        trigger_key = str(event.get("trigger_key", "") or "")
        turn_key = trigger_key or turn_id
        if not turn_key:
            continue
        status = str(event.get("status", "") or "")
        at = event_time(event)
        if at is None:
            continue
        role = "tester" if event_name == "tester_trigger_ack" else "solver"
        record = turns.setdefault(
            turn_key,
            {
                "role": role,
                "op": str(event.get("op", "") or ""),
                "turn_id": turn_id,
                "active_at": None,
                "done_at": None,
                "done_status": "",
            },
        )
        if turn_id and not record.get("turn_id"):
            record["turn_id"] = turn_id
        if status == "active":
            previous = record.get("active_at")
            if previous is None or at < previous:
                record["active_at"] = at
                record["turn_id"] = turn_id or record.get("turn_id", "")
        elif status in {"completed", "failed", "stale"}:
            previous = record.get("done_at")
            if previous is None or at > previous:
                record["done_at"] = at
                record["done_status"] = status
                record["turn_id"] = turn_id or record.get("turn_id", "")

    labels: list[str] = []
    for turn_key, record in turns.items():
        active_at = record.get("active_at")
        if active_at is None or active_at >= gap_end:
            continue
        done_at = record.get("done_at")
        if done_at is None and active_at < gap_start - timedelta(minutes=30):
            continue
        if done_at is not None and done_at <= gap_start:
            continue
        label_turn_id = str(record.get("turn_id", "") or turn_key)
        label = f"{record.get('op')} {record.get('role')} turn {label_turn_id} active"
        done_status = str(record.get("done_status", "") or "")
        if done_status:
            label += f"->{done_status}"
        labels.append(label)
    return sorted(labels)


def submit_gap_recovery_action(
    next_submit: dict[str, Any] | None, root_cause: str = ""
) -> str:
    cause = root_cause.lower()
    if "already submitted and still running" in cause:
        return "no recovery needed; device occupancy was already covered by an in-flight test"
    if next_submit is None:
        if "transport recovery" in cause or "backoff" in cause:
            return "keep same-request GitPartner heartbeat/recovery active; do not enqueue a duplicate submit"
        return (
            "run status-query; if resource is idle and a harness-owned submit/prepare is runnable, "
            "execute one daemon tick immediately, otherwise inspect native relay/outbox or resource lease"
        )
    if "same test version was re-dispatched" in cause:
        return (
            "audit restore-submit retry timing; keep infra recovery local to the same request and "
            "avoid delaying the next distinct runnable submit"
        )
    if "transport recovery" in cause or "backoff" in cause:
        return "continue same-request transport recovery until terminal/archive; do not duplicate GP requests"
    if "candidate production" in cause:
        return (
            "keep observing the active solver/tester turn; when it creates pending/case evidence, "
            "dispatch the highest-debt runnable op in the next poll"
        )
    if "no solver ack" in cause:
        return (
            "check native_relay_outbox and claim/deliver the exact solver/tester gate once; "
            "do not send periodic messages outside the live gate"
        )
    if "after solver ack" in cause:
        return (
            "inspect scheduler/resource/casegen evidence; if resource is free, prioritize balance debt "
            "before non-debt submits"
        )
    return "daemon should keep 1s polling and prioritize balance debt/runnable submit in the next cycle"


def latest_restore_hold_after(
    events: list[dict[str, Any]],
    completion: dict[str, Any],
    next_submit: dict[str, Any] | None,
) -> str:
    completion_time = event_time(completion)
    next_submit_time = event_time(next_submit) if next_submit else None
    if completion_time is None:
        return ""
    completed_op = str(completion.get("op", "") or "")
    latest: tuple[datetime, str] | None = None
    for event in events:
        if event.get("event") != "tick":
            continue
        tick_time = event_time(event)
        if tick_time is None or tick_time <= completion_time:
            continue
        if next_submit_time is not None and tick_time >= next_submit_time:
            continue
        decisions = event.get("decisions")
        if not isinstance(decisions, list):
            continue
        for decision in decisions:
            if not isinstance(decision, dict):
                continue
            if completed_op and str(decision.get("op", "") or "") != completed_op:
                continue
            if str(decision.get("action", "") or "") != "hold":
                continue
            reason = str(decision.get("reason", "") or "")
            reason_lower = reason.lower()
            if (
                "holding restore-submit" not in reason_lower
                and "transport/gp recovery" not in reason_lower
            ):
                continue
            if latest is None or tick_time > latest[0]:
                latest = (tick_time, reason)
    if latest is None:
        return ""
    return latest[1]


def is_actual_submit_dispatch(event: dict[str, Any]) -> bool:
    event_name = str(event.get("event", "") or "")
    if event_name in REMOTE_DISPATCH_EVENTS:
        return True
    if event_name not in LOCAL_ACTUAL_SUBMIT_EVENTS:
        return False
    action = str(event.get("action", "") or "")
    action_id = str(event.get("action_id", "") or "")
    command = str(event.get("command", "") or "")
    if "--attach-existing" in action_id or "--attach-existing" in command:
        return False
    if action == ActionKind.DISPATCH_SUBMIT.value:
        return True
    return f"|{ActionKind.DISPATCH_SUBMIT.value}|" in action_id


def is_local_dispatch(event: dict[str, Any]) -> bool:
    event_name = str(event.get("event", "") or "")
    if (
        event_name not in LOCAL_DISPATCH_EVENTS
        and event_name not in LOCAL_ACTUAL_SUBMIT_EVENTS
    ):
        return False
    action = str(event.get("action", "") or "")
    action_id = str(event.get("action_id", "") or "")
    if action in LOCAL_TEST_OCCUPANCY_ACTIONS:
        return True
    return any(f"|{marker}|" in action_id for marker in LOCAL_TEST_OCCUPANCY_ACTIONS)


def latest_any(
    events: list[dict[str, Any]], event_names: set[str]
) -> dict[str, Any] | None:
    candidates = [
        event
        for event in events
        if event.get("event") in event_names and event_time(event)
    ]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda event: event_time(event)
        or datetime.min.replace(tzinfo=timezone.utc),
    )


def first_after(events: list[dict[str, Any]]) -> dict[str, Any] | None:
    candidates = [event for event in events if event_time(event)]
    if not candidates:
        return None
    return min(
        candidates,
        key=lambda event: event_time(event)
        or datetime.max.replace(tzinfo=timezone.utc),
    )


def seconds_between(
    start: dict[str, Any] | None, end: dict[str, Any] | None
) -> int | None:
    start_time = event_time(start)
    end_time = event_time(end)
    if start_time is None or end_time is None:
        return None
    return max(0, int((end_time - start_time).total_seconds()))


def event_time(event: dict[str, Any] | None) -> datetime | None:
    if not event:
        return None
    return parse_time(str(event.get("time", "") or ""))


def compact_event(event: dict[str, Any] | None) -> dict[str, Any]:
    if not event:
        return {}
    return {
        "event": event.get("event", ""),
        "time": event.get("time", ""),
        "op": event.get("op", ""),
        "test_version": event.get("test_version", ""),
        "request_id": event.get("request_id", ""),
        "status": event.get("status", ""),
        "action": event.get("action", ""),
        "action_id": event.get("action_id", ""),
    }


def event_sort_key(event: dict[str, Any]) -> tuple[datetime, str]:
    return (
        parse_time(str(event.get("time", "") or ""))
        or datetime.min.replace(tzinfo=timezone.utc),
        str(event.get("event_id", "")),
    )


def parse_request_id(
    operators: tuple[str, ...], request_id: str
) -> tuple[str, str] | None:
    for op in operators:
        prefix = f"{op}_V"
        if not request_id.startswith(prefix):
            continue
        version = REQUEST_SUFFIX_RE.sub("", request_id)
        return op, version
    return None


def normalize_time(text: str) -> str:
    parsed = parse_time(text)
    if parsed is None:
        return text
    return parsed.astimezone(timezone.utc).isoformat(timespec="seconds")


@lru_cache(maxsize=32768)
def parse_time(text: str) -> datetime | None:
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def mtime_iso(path: Path) -> str:
    try:
        modified_at = path.stat().st_mtime
    except OSError:
        return ""
    return timestamp_iso(modified_at)


def timestamp_iso(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat(timespec="seconds")


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def relpath(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def display_value(value: object) -> str:
    return "-" if value is None or value == "" else str(value)
