from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from ascendop_daemon.core.models import (
    ActionKind,
    DaemonConfig,
    GateDecision,
    TransportObservation,
    operator_session,
)
from ascendop_daemon.registry.operator_plugins import metric_epoch_for_config, read_operator_plugin_state
from ascendop_daemon.legacy.timeline import (
    LOCAL_ACTUAL_SUBMIT_EVENTS,
    TIMELINE_FILE,
    event_time,
    is_actual_submit_dispatch,
    qualified_result_tasks,
    read_timeline_events,
)


WEIGHTED_RE = re.compile(r"weighted_time:\s*`?([0-9.]+)\s*us", re.IGNORECASE)
VERDICT_RE = re.compile(r"^Verdict:\s*(\S+)", re.IGNORECASE | re.MULTILINE)
KNOWLEDGE_FOCUS_MAX_LINES = 80
KNOWLEDGE_FOCUS_SECTIONS = ("## Optimization Focus", "## Case Focus", "## Handoff")
ENGINE_ACTIVE_STATES = {"admitting", "accepted", "running"}
ENGINE_ACTIVE_EXECUTION_STATES = {"active-running", "preactivating", "queued"}
ENGINE_TERMINAL_EXECUTION_STATES = {
    "cancelled",
    "completed",
    "failed",
    "returned",
    "terminal",
}


def read_engine_resource_activity(root: Path) -> dict[str, Any]:
    path = root / "TestUtils" / "tester_daemon" / "engine_admission_state.json"
    try:
        state = json.loads(path.read_text(encoding="utf-8-sig"))
    except (FileNotFoundError, OSError, UnicodeError, json.JSONDecodeError):
        state = {}
    if not isinstance(state, dict):
        state = {}

    active_jobs: list[str] = []
    device_jobs: list[str] = []
    jobs = state.get("jobs", {})
    if isinstance(jobs, dict):
        for key, raw_job in jobs.items():
            if not isinstance(raw_job, dict):
                continue
            job_id = str(raw_job.get("engine_job_id") or key)
            job_state = str(raw_job.get("state") or "")
            execution_state = str(raw_job.get("execution_state") or "")
            active = (
                bool(raw_job.get("active_slot_occupied"))
                or (execution_state in ENGINE_ACTIVE_EXECUTION_STATES)
                or (
                    job_state in ENGINE_ACTIVE_STATES
                    and execution_state not in ENGINE_TERMINAL_EXECUTION_STATES
                )
            )
            if not active:
                continue
            active_jobs.append(job_id)
            stage_locks = raw_job.get("stage_locks", [])
            if not isinstance(stage_locks, list):
                stage_locks = []
            if str(raw_job.get("stage_resource") or "") == "device" or "npu" in {
                str(item) for item in stage_locks
            }:
                device_jobs.append(job_id)

    snapshot = state.get("last_engine_snapshot", {})
    if not isinstance(snapshot, dict):
        snapshot = {}
    running_by_resource = snapshot.get("running_by_resource", {})
    if not isinstance(running_by_resource, dict):
        running_by_resource = {}
    shared_leases = snapshot.get("shared_resource_leases", [])
    if not isinstance(shared_leases, list):
        shared_leases = []
    active_npu_leases = [
        lease
        for lease in shared_leases
        if isinstance(lease, dict)
        and bool(lease.get("active", True))
        and str(lease.get("resource") or "") == "npu"
    ]
    snapshot_device_busy = int(running_by_resource.get("device", 0) or 0) > 0 or bool(
        active_npu_leases
    )
    return {
        "pipeline_busy": bool(active_jobs),
        "device_busy": bool(device_jobs) or snapshot_device_busy,
        "active_job_count": len(active_jobs),
        "active_job_ids": active_jobs,
        "device_job_ids": device_jobs,
        "running_by_resource": running_by_resource,
        "active_npu_lease_count": len(active_npu_leases),
        "snapshot_observed_at": str(snapshot.get("observed_at") or ""),
        "state_updated_at": str(state.get("updated_at") or ""),
    }


def build_efficiency_snapshot(
    root: Path,
    config: DaemonConfig,
    decisions: tuple[GateDecision, ...],
    transports: tuple[TransportObservation, ...],
    resource_leases: tuple[dict[str, object], ...],
    captured_at: str,
    timeline: dict[str, Any] | None = None,
) -> dict[str, Any]:
    recent_limit = int(config.policy.get("efficiency_recent_event_limit", 120) or 120)
    recent_events = read_recent_events(root, recent_limit)
    active_ops = {str(op) for op in config.operators if str(op)}
    metric_epoch = metric_epoch_for_config(root, config)
    selected_counts = Counter()
    dispatch_counts = Counter()
    notify_counts = Counter()
    for event in recent_events:
        selected = event.get("selected")
        if isinstance(selected, dict):
            op = str(selected.get("op", "") or "")
            action = str(selected.get("action", "") or "")
            selected_at = event_time({"time": event.get("time", "")})
            if op in active_ops and (
                metric_epoch is None
                or (selected_at is not None and selected_at >= metric_epoch)
            ):
                selected_counts[op] += 1
                if action == "dispatch_submit":
                    dispatch_counts[op] += 1
                if action == "notify_solver":
                    notify_counts[op] += 1

    by_op: dict[str, Any] = {}
    decision_by_op = {decision.row.op: decision for decision in decisions}
    transport_by_op: dict[str, list[dict[str, Any]]] = {}
    for obs in transports:
        transport_by_op.setdefault(obs.op, []).append(
            {
                "test_version": obs.test_version,
                "state": obs.state,
                "client_state": obs.client_state,
                "client_updated_at": obs.client_updated_at,
                "terminal": obs.terminal,
                "remote_feedback_status": obs.remote_feedback_status,
                "elapsed_without_remote_feedback_seconds": obs.elapsed_without_remote_feedback_seconds,
                "summary": obs.summary,
            }
        )
    lease_by_op: dict[str, list[dict[str, object]]] = {}
    for lease in resource_leases:
        lease_by_op.setdefault(str(lease.get("op", "") or ""), []).append(lease)

    for op in config.operators:
        latest_result = latest_result_summary(root, op)
        latest_case = latest_case_summary(root, op)
        decision = decision_by_op.get(op)
        by_op[op] = {
            "gate_stage": decision.row.gate_stage if decision else "",
            "next_owner": decision.row.next_owner if decision else "",
            "action": decision.action.value if decision else "",
            "reason": decision.reason if decision else "",
            "blocks_operator": decision.blocks_operator if decision else "",
            "latest_result": latest_result,
            "latest_case": latest_case,
            "recent_selected_count": selected_counts[op],
            "recent_dispatch_submit_count": dispatch_counts[op],
            "recent_notify_solver_count": notify_counts[op],
            "active_resource_leases": lease_by_op.get(op, []),
            "transport": transport_by_op.get(op, []),
        }

    nonterminal_transport = [
        obs
        for obs in transports
        if obs.terminal is not True and (obs.state or obs.remote_feedback_status)
    ]
    tester_work = [
        decision.row.op
        for decision in decisions
        if decision.row.next_owner == "tester" and decision.action.value != "hold"
    ]
    traffic_balance = build_traffic_balance(root, config)
    submit_gap_applicable = len(config.operators) > 1
    attempt_submit_gap = (timeline or {}).get("completion_to_next_submit", {})
    qualified_submit_gap = (timeline or {}).get(
        "qualified_completion_to_next_submit",
        attempt_submit_gap,
    )
    if not submit_gap_applicable:
        not_applicable_submit_gap = {
            "applicable": False,
            "ok": True,
            "reason": "requires_at_least_two_active_operators",
            "active_operator_count": len(config.operators),
        }
        attempt_submit_gap = not_applicable_submit_gap
        qualified_submit_gap = not_applicable_submit_gap
    else:
        if isinstance(attempt_submit_gap, dict):
            attempt_submit_gap = {"applicable": True, **attempt_submit_gap}
        if isinstance(qualified_submit_gap, dict):
            qualified_submit_gap = {"applicable": True, **qualified_submit_gap}
    engine_activity = read_engine_resource_activity(root)
    return {
        "updated_at": captured_at,
        "recent_event_limit": recent_limit,
        "resource_idle": (
            not resource_leases
            and not nonterminal_transport
            and not engine_activity["pipeline_busy"]
        ),
        "engine_pipeline_busy": engine_activity["pipeline_busy"],
        "engine_device_busy": engine_activity["device_busy"],
        "engine_activity": engine_activity,
        "tester_owned_work": tester_work,
        "recent_selected_balance": {op: selected_counts[op] for op in config.operators},
        "recent_dispatch_balance": {op: dispatch_counts[op] for op in config.operators},
        "traffic_balance": traffic_balance,
        "balance_recovery": build_balance_recovery(
            traffic_balance, by_op, resource_leases
        ),
        # The hard SLA counts every terminal test attempt, including infra
        # retries.  Result-archive-only timing remains available as a
        # diagnostic, but must not silently replace the acceptance metric.
        "completion_to_next_submit": attempt_submit_gap,
        "attempt_completion_to_next_submit": attempt_submit_gap,
        "qualified_completion_to_next_submit": qualified_submit_gap,
        "knowledge_usage": build_knowledge_usage(root, config),
        "operators": by_op,
    }


def build_knowledge_usage(root: Path, config: DaemonConfig) -> dict[str, Any]:
    by_op: dict[str, Any] = {}
    for op in config.operators:
        session = operator_session(config, op)
        version_candidates = list(
            (root / "TestUtils" / "pending" / op).rglob("VERSION.md")
        )
        version_candidates += list(
            (root / "TestUtils" / "submit" / op).rglob("VERSION.md")
        )
        version_candidates += list(
            (root / "operators_testresult" / op).rglob("VERSION.md")
        )
        latest_version, version_text = latest_readable_text(version_candidates)
        latest_case = latest_case_summary(root, op)
        case_dir = (
            root
            / "TestUtils"
            / "casegen"
            / op
            / "case"
            / str(latest_case.get("case_version", ""))
        )
        casegen_plan = case_dir / "CASEGEN_PLAN.md"
        model_audit = case_dir / "MODEL_AUDIT.md"
        case_text = (
            casegen_plan.read_text(encoding="utf-8", errors="replace")
            if casegen_plan.exists()
            else ""
        )
        lowered_version = version_text.lower()
        lowered_case = case_text.lower()
        knowledge_root = (
            Path(session.knowledge_root)
            if session and session.knowledge_root
            else Path()
        )
        if session and session.knowledge_root and not knowledge_root.is_absolute():
            knowledge_root = root / knowledge_root
        coverage_path = (
            knowledge_root / "case_coverage.md"
            if session and session.knowledge_root
            else Path()
        )
        lessons_path = (
            knowledge_root / "optimization_lessons.md"
            if session and session.knowledge_root
            else Path()
        )
        backlog_path = (
            knowledge_root / "hypothesis_backlog.md"
            if session and session.knowledge_root
            else Path()
        )
        focus_path = (
            knowledge_root / "current_focus.md"
            if session and session.knowledge_root
            else Path()
        )
        coverage_text = (
            coverage_path.read_text(encoding="utf-8", errors="replace")
            if session and session.knowledge_root and coverage_path.exists()
            else ""
        )
        lessons_text = (
            lessons_path.read_text(encoding="utf-8", errors="replace")
            if session and session.knowledge_root and lessons_path.exists()
            else ""
        )
        backlog_text = (
            backlog_path.read_text(encoding="utf-8", errors="replace")
            if session and session.knowledge_root and backlog_path.exists()
            else ""
        )
        focus_text = (
            focus_path.read_text(encoding="utf-8", errors="replace")
            if session and session.knowledge_root and focus_path.exists()
            else ""
        )
        focus_line_count = len(focus_text.splitlines()) if focus_text else 0
        focus_sections_complete = bool(focus_text) and all(
            marker in focus_text for marker in KNOWLEDGE_FOCUS_SECTIONS
        )
        focus_bounded = (
            bool(focus_text) and focus_line_count <= KNOWLEDGE_FOCUS_MAX_LINES
        )
        latest_result = latest_result_summary(root, op)
        latest_test_version = str(latest_result.get("test_version", "") or "")
        review_path = (
            root
            / "operators_testresult"
            / op
            / latest_test_version
            / "SKILL_APPLICATION_REVIEW.md"
            if latest_test_version
            else Path()
        )
        review_text = (
            review_path.read_text(encoding="utf-8", errors="replace")
            if latest_test_version and review_path.exists()
            else ""
        )
        knowledge_decision = extract_knowledge_decision(
            version_text
        ) or extract_knowledge_decision(review_text)
        casegen_knowledge_decision = extract_knowledge_decision(case_text)
        normalized_version = version_text.replace("\\", "/").lower()
        normalized_case = case_text.replace("\\", "/").lower()
        configured_root = (
            str(session.knowledge_root).replace("\\", "/").rstrip("/").lower()
            if session and session.knowledge_root
            else ""
        )
        version_knowledge_files = {
            filename: bool(
                configured_root
                and f"{configured_root}/{filename}" in normalized_version
            )
            for filename in (
                "case_coverage.md",
                "optimization_lessons.md",
                "hypothesis_backlog.md",
            )
        }
        case_knowledge_files = {
            filename: bool(
                configured_root and f"{configured_root}/{filename}" in normalized_case
            )
            for filename in (
                "case_coverage.md",
                "optimization_lessons.md",
                "hypothesis_backlog.md",
            )
        }
        case_version = str(latest_case.get("case_version", "") or "")
        coverage_current = bool(
            case_version and case_version.lower() in coverage_text.lower()
        )
        lesson_current = bool(
            latest_test_version
            and (
                latest_test_version.lower() in lessons_text.lower()
                or bool(knowledge_decision)
            )
        )
        missing_shared: list[str] = []
        if session and session.knowledge_root:
            if not knowledge_root.exists():
                missing_shared.append("knowledge_root")
            if not coverage_path.exists():
                missing_shared.append("case_coverage.md")
            if not lessons_path.exists():
                missing_shared.append("optimization_lessons.md")
            if not backlog_path.exists():
                missing_shared.append("hypothesis_backlog.md")
            if not focus_path.exists():
                missing_shared.append("current_focus.md")
            elif not focus_sections_complete:
                missing_shared.append("current_focus:sections")
            elif not focus_bounded:
                missing_shared.append(
                    f"current_focus:lines={focus_line_count}>{KNOWLEDGE_FOCUS_MAX_LINES}"
                )
            if case_version and not coverage_current:
                missing_shared.append(f"case_coverage:{case_version}")
            if latest_version and not all(version_knowledge_files.values()):
                missing_shared.append("VERSION:knowledge-citations")
            if latest_version and not extract_knowledge_decision(version_text):
                missing_shared.append("VERSION:shared-knowledge-decision")
            if casegen_plan.exists() and not all(case_knowledge_files.values()):
                missing_shared.append("CASEGEN_PLAN:knowledge-citations")
            if casegen_plan.exists() and not extract_knowledge_decision(case_text):
                missing_shared.append("CASEGEN_PLAN:shared-knowledge-decision")
            if latest_test_version and not lesson_current:
                missing_shared.append(
                    f"RESULT:{latest_test_version}:knowledge-decision"
                )
        by_op[op] = {
            "workflow_mode": session.workflow_mode if session else "iterate",
            "latest_version_path": (
                str(latest_version.relative_to(root)) if latest_version else ""
            ),
            "optimization_router_recorded": "ascendc-optimization-router"
            in lowered_version,
            "comparative_evidence_recorded": (
                "evidence-absorption-checklist" in lowered_version
                or "comparative measurement" in lowered_version
                or "compatibility/correctness diagnosis" in lowered_version
                or "method transfer" in lowered_version
            ),
            "reference_path_mentions": len(
                re.findall(r"reference[/\\][^\s`]+", version_text, flags=re.IGNORECASE)
            ),
            "op_knowledge_mentions": lowered_version.count("reference/op_knowledge"),
            "case_version": case_version,
            "casegen_plan": casegen_plan.exists(),
            "model_audit": model_audit.exists(),
            "casegen_specificity": {
                "prior_coverage": any(
                    token in lowered_case
                    for token in (
                        "prior",
                        "history",
                        "previous",
                        "earlier",
                        "baseline",
                        "以往",
                        "历史",
                    )
                ),
                "source_or_tiling": any(
                    token in lowered_case for token in ("source", "tiling", "源码")
                ),
                "weak_point": any(
                    token in lowered_case
                    for token in ("weak", "blocker", "failure", "薄弱", "弱点")
                ),
                "novel_coverage": any(
                    token in lowered_case
                    for token in (
                        "uncovered",
                        "new coverage",
                        "novel",
                        "distinct",
                        "rotated",
                        "first",
                        "未覆盖",
                        "新增覆盖",
                    )
                ),
                "falsifiable": any(
                    token in lowered_case
                    for token in ("falsif", "expected", "预期", "证伪")
                ),
            },
            "shared_knowledge": {
                "root": str(session.knowledge_root) if session else "",
                "root_exists": bool(
                    session and session.knowledge_root and knowledge_root.exists()
                ),
                "case_coverage_exists": bool(
                    session and session.knowledge_root and coverage_path.exists()
                ),
                "optimization_lessons_exists": bool(
                    session and session.knowledge_root and lessons_path.exists()
                ),
                "hypothesis_backlog_exists": bool(
                    session and session.knowledge_root and backlog_path.exists()
                ),
                "current_focus_exists": bool(
                    session and session.knowledge_root and focus_path.exists()
                ),
                "current_focus_sections_complete": focus_sections_complete,
                "current_focus_bounded": focus_bounded,
                "current_focus_line_count": focus_line_count,
                "open_hypothesis_count": open_hypothesis_count(backlog_text),
                "case_coverage_current": coverage_current,
                "version_file_citations": version_knowledge_files,
                "casegen_file_citations": case_knowledge_files,
                "knowledge_decision": knowledge_decision,
                "casegen_knowledge_decision": casegen_knowledge_decision,
                "latest_result_triaged": lesson_current,
                "ok": not missing_shared,
                "missing": missing_shared,
            },
            "peer_benchmark_progress": build_peer_benchmark_progress(root, session),
        }
    return {"operators": by_op}


def latest_readable_text(paths: list[Path]) -> tuple[Path | None, str]:
    candidates: list[tuple[int, Path]] = []
    for path in paths:
        try:
            candidates.append((path.stat().st_mtime_ns, path))
        except OSError:
            continue
    for _mtime, path in sorted(candidates, key=lambda item: item[0], reverse=True):
        try:
            return path, path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
    return None, ""


def extract_knowledge_decision(text: str) -> str:
    match = re.search(
        r"^\s*(?:[-*]\s*)?shared knowledge decision\s*:\s*(.+)$",
        text,
        flags=re.IGNORECASE | re.MULTILINE,
    )
    return " ".join(match.group(1).strip().split()) if match else ""


def open_hypothesis_count(text: str) -> int:
    match = re.search(
        r"^## Open\s*$([\s\S]*?)(?=^## Resolved\s*$|\Z)", text, re.I | re.M
    )
    if not match:
        return 0
    return len(re.findall(r"^###\s+\S", match.group(1), re.M))


def build_peer_benchmark_progress(root: Path, session: Any) -> dict[str, Any]:
    if session is None or session.workflow_mode != "peer_benchmark":
        return {"applicable": False}
    knowledge_root = Path(session.knowledge_root) if session.knowledge_root else Path()
    if not knowledge_root.is_absolute():
        knowledge_root = root / knowledge_root
    ledger_root = knowledge_root / "peer_benchmarks"
    baseline_marker = (
        Path(session.benchmark_baseline_marker)
        if session.benchmark_baseline_marker
        else Path()
    )
    if session.benchmark_baseline_marker and not baseline_marker.is_absolute():
        baseline_marker = root / baseline_marker
    baseline_ready = bool(
        session.benchmark_baseline_marker and baseline_marker.exists()
    )
    records: dict[str, dict[str, Any]] = {}
    for candidate in session.peer_candidates:
        ledger = ledger_root / f"{candidate}.md"
        status = "unstarted"
        durable_disposition = ""
        if ledger.exists():
            text = ledger.read_text(encoding="utf-8", errors="replace")
            match = re.search(
                r"^Status:\s*(.+)$", text, flags=re.IGNORECASE | re.MULTILINE
            )
            status = match.group(1).strip() if match else "ledger-without-status"
            disposition_match = re.search(
                r"^(?:[-*]\s*)?Durable disposition:\s*(.+(?:\n[ \t]+[^\n]+)*)$",
                text,
                flags=re.IGNORECASE | re.MULTILINE,
            )
            durable_disposition = (
                " ".join(disposition_match.group(1).replace("`", "").split())
                if disposition_match
                else ""
            )
        effective_status = status
        if durable_disposition:
            effective_status = f"{status}; durable disposition: {durable_disposition}"
        lowered = effective_status.lower()
        pending = any(
            token in lowered for token in ("pending", "queued", "running", "await")
        )
        disposition_terminal = bool(
            durable_disposition
            and any(
                token in durable_disposition.lower()
                for token in (
                    "complete",
                    "measured",
                    "pass",
                    "fail",
                    "blocked",
                    "incompatible",
                    "unresolvable",
                )
            )
        )
        terminal = bool(
            status != "unstarted"
            and (
                disposition_terminal
                or (
                    not pending
                    and any(
                        token in lowered
                        for token in (
                            "complete",
                            "measured",
                            "pass",
                            "fail",
                            "blocked",
                            "incompatible",
                            "unresolvable",
                        )
                    )
                )
            )
        )
        records[candidate] = {
            "status": effective_status,
            "recorded_status": status,
            "durable_disposition": durable_disposition,
            "terminal": terminal,
            "ledger_path": str(ledger.relative_to(root)) if ledger.exists() else "",
        }
    completed = [
        candidate for candidate, record in records.items() if record["terminal"]
    ]
    pending = [
        candidate
        for candidate, record in records.items()
        if record["status"] != "unstarted" and not record["terminal"]
    ]
    unstarted = [
        candidate
        for candidate, record in records.items()
        if record["status"] == "unstarted"
    ]
    return {
        "applicable": True,
        "configured_count": len(session.peer_candidates),
        "completed_count": len(completed),
        "pending_count": len(pending),
        "unstarted_count": len(unstarted),
        "completed": completed,
        "pending": pending,
        "unstarted": unstarted,
        "records": records,
        "completion_marker": session.completion_marker,
        "baseline_marker": session.benchmark_baseline_marker,
        "baseline_ready": baseline_ready,
        "retirement_ready": bool(
            baseline_ready
            and session.peer_candidates
            and len(completed) == len(session.peer_candidates)
        ),
    }


def write_efficiency_files(root: Path, snapshot: dict[str, Any]) -> None:
    state_dir = root / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "test_efficiency.json").write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    (state_dir / "TEST_EFFICIENCY.md").write_text(
        render_efficiency(snapshot), encoding="utf-8"
    )


def latest_result_summary(root: Path, op: str) -> dict[str, Any]:
    result_root = root / "operators_testresult" / op
    result_files = sorted(
        result_root.glob("*/RESULT.md"),
        key=lambda p: p.stat().st_mtime if p.exists() else 0,
    )
    if not result_files:
        return {"count": 0}
    latest = result_files[-1]
    text = latest.read_text(encoding="utf-8", errors="ignore")
    weighted = WEIGHTED_RE.search(text)
    verdict = VERDICT_RE.search(text)
    return {
        "count": len(result_files),
        "test_version": latest.parent.name,
        "path": str(latest.relative_to(root)),
        "mtime_utc": datetime.fromtimestamp(
            latest.stat().st_mtime, tz=timezone.utc
        ).isoformat(timespec="seconds"),
        "verdict": verdict.group(1) if verdict else "",
        "weighted_time_us": float(weighted.group(1)) if weighted else None,
    }


def latest_case_summary(root: Path, op: str) -> dict[str, Any]:
    case_root = root / "TestUtils" / "casegen" / op / "case"
    metas = sorted(case_root.glob("case_*/meta.json"), key=lambda p: p.parent.name)
    if not metas:
        return {}
    meta_path = metas[-1]
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        meta = {}
    return {
        "case_version": meta.get("case_version") or meta_path.parent.name,
        "usage_count": meta.get("usage_count"),
        "max_usage": meta.get("max_usage"),
        "path": str(meta_path.relative_to(root)),
    }


def read_recent_events(root: Path, limit: int) -> list[dict[str, Any]]:
    path = root / "TestUtils" / "tester_daemon" / "events.jsonl"
    if not path.exists():
        return []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()[-max(1, limit) :]
    except OSError:
        return []
    events: list[dict[str, Any]] = []
    for line in lines:
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict):
            events.append(data)
    return events


def build_traffic_balance(
    root: Path,
    config: DaemonConfig,
    events: list[dict[str, Any]] | None = None,
    eligible_result_tasks: set[tuple[str, str]] | None = None,
) -> dict[str, Any]:
    """Summarize the sliding real-test dispatch window used for balance debt."""
    operators = list(config.operators)
    min_per_operator = int(
        config.policy.get("traffic_balance_min_tests_per_operator", 3) or 3
    )
    metric_epoch = metric_epoch_for_config(root, config)
    plugin_state = read_operator_plugin_state(root)
    if len(operators) <= 1:
        return {
            "applicable": False,
            "reason": "requires_at_least_two_active_operators",
            "active_operator_count": len(operators),
            "operator_set_generation": plugin_state.get("generation", 0),
            "metric_epoch_at": (
                metric_epoch.isoformat(timespec="seconds") if metric_epoch else ""
            ),
            "window_size": 0,
            "min_per_operator": min_per_operator,
            "sample_count": 0,
            "enough_samples": True,
            "ok": True,
            "counts": {op: 0 for op in operators},
            "debt": {op: 0 for op in operators},
            "underrepresented_ops": [],
            "qualification": "disabled_for_single_active_operator",
            "recent_tests": [],
        }
    default_window = len(operators) * min_per_operator + 1 if operators else 0
    window_size = int(
        config.policy.get("traffic_balance_window_size", default_window)
        or default_window
    )
    timeline_path = root / "TestUtils" / "tester_daemon" / TIMELINE_FILE
    all_events = events if events is not None else read_timeline_events(timeline_path)
    eligible = (
        eligible_result_tasks
        if eligible_result_tasks is not None
        else qualified_result_tasks(root, operators, all_events)
    )
    tests_by_task: dict[tuple[str, str], dict[str, Any]] = {}
    for item in recent_real_test_dispatches(all_events, operators):
        task = (str(item.get("op", "") or ""), str(item.get("test_version", "") or ""))
        if task not in eligible:
            continue
        existing = tests_by_task.get(task)
        item_dt = event_time({"time": item.get("submitted_at", "")})
        existing_dt = (
            event_time({"time": existing.get("submitted_at", "")}) if existing else None
        )
        if (
            existing is None
            or existing_dt is None
            or (item_dt is not None and item_dt < existing_dt)
        ):
            tests_by_task[task] = item
    tests = sorted(
        (
            item
            for item in tests_by_task.values()
            if metric_epoch is None
            or (
                event_time({"time": item.get("submitted_at", "")}) is not None
                and event_time({"time": item.get("submitted_at", "")}) >= metric_epoch
            )
        ),
        key=lambda item: event_time({"time": item.get("submitted_at", "")})
        or datetime.min.replace(tzinfo=timezone.utc),
    )
    recent_tests = tests[-window_size:] if window_size > 0 else []
    counts = {op: 0 for op in operators}
    for item in recent_tests:
        op = str(item.get("op", "") or "")
        if op in counts:
            counts[op] += 1
    debt = {op: max(0, min_per_operator - counts.get(op, 0)) for op in operators}
    underrepresented = [op for op in operators if debt.get(op, 0) > 0]
    enough_samples = bool(window_size > 0 and len(recent_tests) >= window_size)
    return {
        "applicable": True,
        "active_operator_count": len(operators),
        "operator_set_generation": plugin_state.get("generation", 0),
        "metric_epoch_at": (
            metric_epoch.isoformat(timespec="seconds") if metric_epoch else ""
        ),
        "window_size": window_size,
        "min_per_operator": min_per_operator,
        "sample_count": len(recent_tests),
        "enough_samples": enough_samples,
        "ok": bool(enough_samples and not underrepresented),
        "counts": counts,
        "debt": debt,
        "underrepresented_ops": underrepresented,
        "qualification": "distinct_test_version_with_current_non_infra_result",
        "recent_tests": recent_tests,
    }


def recent_real_test_dispatches(
    events: list[dict[str, Any]], operators: list[str]
) -> list[dict[str, Any]]:
    """Return accepted device test tasks ordered by the best known start time.

    A local worker start is only an attempt: it may fail on a worktree conflict,
    stale request, or transport preflight without allocating the device.  Prefer
    unique remote ``gp_started``/``gp_dispatched`` request ids or durable
    ``engine_admitted`` receipts. Retain a
    successful local dispatch only for historical runs that have no remote
    acceptance event for that operator/version.
    """
    allowed = set(operators)
    first_observed_by_request: dict[str, datetime] = {}
    for event in events:
        if str(event.get("event", "") or "") != "transport_observed":
            continue
        request_id = str(event.get("request_id", "") or "")
        observed_dt = event_time(event)
        if not request_id or observed_dt is None:
            continue
        existing = first_observed_by_request.get(request_id)
        if existing is None or observed_dt < existing:
            first_observed_by_request[request_id] = observed_dt
    successful_actions = {
        str(event.get("action_id", "") or "")
        for event in events
        if str(event.get("event", "") or "") == "execute_worker_command_finished"
        and str(event.get("action", "") or "") == ActionKind.DISPATCH_SUBMIT.value
        and event.get("returncode") is not None
        and int(event.get("returncode", -1)) == 0
        and str(event.get("action_id", "") or "")
    }
    remote_by_request: dict[tuple[str, str, str], dict[str, Any]] = {}
    remote_versions: set[tuple[str, str]] = set()
    for event in events:
        op = str(event.get("op", "") or "")
        version = str(event.get("test_version", "") or "")
        event_name = str(event.get("event", "") or "")
        if (
            op not in allowed
            or not version
            or event_name
            not in {
                "gp_started",
                "gp_dispatched",
                "engine_admitted",
            }
        ):
            continue
        request_id = str(event.get("request_id", "") or "")
        accepted_dt = accepted_test_time(
            event, first_observed_by_request.get(request_id)
        )
        if accepted_dt is None:
            continue
        request_key = request_id or f"{op}|{version}"
        key = (op, version, request_key)
        existing = remote_by_request.get(key)
        existing_dt = (
            accepted_test_time(existing, first_observed_by_request.get(request_id))
            if existing
            else None
        )
        if existing is None or existing_dt is None or accepted_dt < existing_dt:
            remote_by_request[key] = event
        remote_versions.add((op, version))

    dispatches: list[dict[str, Any]] = []
    for (op, version, _), event in remote_by_request.items():
        request_id = str(event.get("request_id", "") or "")
        accepted_dt = accepted_test_time(
            event, first_observed_by_request.get(request_id)
        )
        if accepted_dt is None:
            continue
        dispatches.append(
            {
                "op": op,
                "test_version": version,
                "submitted_at": accepted_dt.isoformat(timespec="seconds"),
                "event": event.get("event", ""),
                "request_id": event.get("request_id", ""),
                "action_id": event.get("action_id", ""),
                "acceptance_evidence": (
                    "engine_acceptance_receipt"
                    if event.get("event") == "engine_admitted"
                    else "remote_request_started"
                ),
            }
        )

    local_seen: set[tuple[str, str, str]] = set()
    for event in events:
        op = str(event.get("op", "") or "")
        version = str(event.get("test_version", "") or "")
        if op not in allowed or not version or (op, version) in remote_versions:
            continue
        if str(event.get("event", "") or "") != "execute_worker_command_started":
            continue
        if not is_actual_submit_dispatch(event):
            continue
        action_id = str(event.get("action_id", "") or "")
        if not action_id or action_id not in successful_actions:
            continue
        event_dt = event_time(event)
        if event_dt is None:
            continue
        key = (op, version, action_id)
        if key in local_seen:
            continue
        local_seen.add(key)
        dispatches.append(
            {
                "op": op,
                "test_version": version,
                "submitted_at": event.get("time", ""),
                "event": event.get("event", ""),
                "request_id": event.get("request_id", ""),
                "action_id": action_id,
                "acceptance_evidence": "successful_local_dispatch",
            }
        )
    return sorted(
        dispatches,
        key=lambda item: event_time({"time": item.get("submitted_at", "")})
        or datetime.min.replace(tzinfo=timezone.utc),
    )


def accepted_test_time(
    event: dict[str, Any] | None,
    observed_override: datetime | None = None,
) -> datetime | None:
    if not event:
        return None
    remote_dt = event_time(event)
    observed_dt = event_time({"time": event.get("observed_at", "")})
    if observed_override is not None and (
        observed_dt is None or observed_override < observed_dt
    ):
        observed_dt = observed_override
    if observed_dt is None:
        return remote_dt
    if remote_dt is None or remote_dt > observed_dt + timedelta(seconds=5):
        return observed_dt
    return remote_dt


def should_replace_balance_submit(
    existing: dict[str, Any],
    candidate: dict[str, Any],
    existing_dt: datetime | None,
    candidate_dt: datetime,
) -> bool:
    """Prefer the first local daemon submit for balance, with remote events as fallback.

    GitPartner status events can appear multiple times for the same test version
    (lease, start, dispatched, retry) and may be delayed by remote-clock/reporting
    skew. Balance is about test tasks, so a version counts once; if local daemon
    submit evidence exists, it is more authoritative than remote status feedback.
    """
    existing_local = str(existing.get("event", "") or "") in LOCAL_ACTUAL_SUBMIT_EVENTS
    candidate_local = (
        str(candidate.get("event", "") or "") in LOCAL_ACTUAL_SUBMIT_EVENTS
    )
    if candidate_local and not existing_local:
        return True
    if existing_local and not candidate_local:
        return False
    return existing_dt is None or candidate_dt < existing_dt


def build_balance_recovery(
    traffic_balance: dict[str, Any],
    operators: dict[str, Any],
    resource_leases: tuple[dict[str, object], ...],
) -> dict[str, Any]:
    debt_raw = (
        traffic_balance.get("debt", {})
        if isinstance(traffic_balance.get("debt"), dict)
        else {}
    )
    debt: dict[str, int] = {}
    for op, value in debt_raw.items():
        try:
            parsed = max(0, int(value))
        except (TypeError, ValueError):
            parsed = 0
        if parsed:
            debt[str(op)] = parsed

    resource_busy = bool(resource_leases)
    plans = []
    for op, value in sorted(debt.items()):
        data = operators.get(op, {})
        blocker = str(data.get("blocks_operator", "") or "")
        blocker_data = operators.get(blocker, {}) if blocker else {}
        plans.append(
            build_operator_debt_plan(
                op,
                value,
                data,
                resource_busy=resource_busy,
                blocking_operator=blocker,
                blocking_action=str(blocker_data.get("action", "") or ""),
            )
        )
    uncompensated = [plan["op"] for plan in plans if not plan.get("compensated")]
    return {
        "ok": not plans,
        "debt_ops": sorted(debt),
        "next_debt_target": plans[0]["op"] if plans else "",
        "uncompensated_ops": uncompensated,
        "resource_lease_count": len(resource_leases),
        "plans": plans,
    }


def build_operator_debt_plan(
    op: str,
    debt: int,
    data: dict[str, Any],
    *,
    resource_busy: bool = False,
    blocking_operator: str = "",
    blocking_action: str = "",
) -> dict[str, Any]:
    gate = str(data.get("gate_stage", "") or "")
    owner = str(data.get("next_owner", "") or "")
    action = str(data.get("action", "") or "")
    reason = str(data.get("reason", "") or "")
    gate_lower = gate.lower()
    owner_lower = owner.lower()
    state = "blocked_or_manual"
    recovery_action = "inspect live board and blocker before spending another test slot"
    compensated = False
    if owner_lower in {"daemon", "tester"} and gate_lower in {
        "pending-ready",
        "submit-ready",
        "prepare-submit",
    }:
        state = "runnable_submit"
        recovery_action = "dispatch this debt operator before non-debt submits when the resource is idle"
        compensated = True
    elif (
        resource_busy
        and owner_lower in {"daemon", "tester"}
        and gate_lower in {"submit-blocked", "submit-ready", "pending-ready"}
    ):
        state = "waiting_for_active_resource"
        recovery_action = "dispatch this debt operator first after the active GitPartner lease completes"
        compensated = True
    elif (
        owner_lower in {"daemon", "tester"}
        and gate_lower == "submit-blocked"
        and blocking_operator
    ):
        blocking_progress = blocking_action in {
            ActionKind.RECOVER_BLOCKED.value,
            ActionKind.HEARTBEAT_ACTIVE_REQUEST.value,
            ActionKind.CANCEL_STALLED_REQUEST.value,
            ActionKind.REPAIR_QUEUE.value,
        }
        state = "waiting_for_blocking_transport_recovery"
        recovery_action = f"clear blocking GitPartner request owned by {blocking_operator} before dispatching this debt operator"
        compensated = blocking_progress
    elif owner_lower in {"daemon", "tester"} and (
        gate_lower == "submit-waiting"
        or "waiting" in gate_lower
        or "queued" in gate_lower
    ):
        state = "queued_behind_resource"
        recovery_action = "dispatch this debt operator first after the active GitPartner lease completes"
        compensated = True
    elif owner_lower in {"daemon", "tester"} and gate_lower == "submit-running":
        state = "active_or_running"
        recovery_action = "debt operator already owns a running submit slot"
        compensated = True
    elif owner_lower == "solver":
        state = "awaiting_solver_output"
        recovery_action = "observe solver turn; deliver only a real ready/retry-ready trigger, then dispatch after pending appears"
        compensated = (
            action == ActionKind.NOTIFY_SOLVER.value
            or "solver trigger already active" in reason.lower()
        )
    elif owner_lower == "tester" and ("case" in gate_lower or "casegen" in gate_lower):
        state = "awaiting_tester_casegen"
        recovery_action = "observe or deliver the real casegen trigger; submit remains blocked until required evidence exists"
        compensated = (
            action == ActionKind.NOTIFY_TESTER_CASEGEN.value
            or "tester trigger already active" in reason.lower()
        )
    return {
        "op": op,
        "debt": debt,
        "gate_stage": gate,
        "next_owner": owner,
        "action": action,
        "state": state,
        "compensated": compensated,
        "recovery_action": recovery_action,
    }


def render_efficiency(snapshot: dict[str, Any]) -> str:
    lines = [
        "# Tester Daemon Efficiency",
        "",
        f"- updated_at: {snapshot.get('updated_at', '')}",
        f"- recent_event_limit: {snapshot.get('recent_event_limit', '')}",
        f"- resource_idle: {snapshot.get('resource_idle', '')}",
        "",
        "## Traffic Balance",
        "",
    ]
    balance = snapshot.get("traffic_balance", {})
    if isinstance(balance, dict) and balance:
        lines.extend(
            [
                f"- window_size: {balance.get('window_size', '-')}",
                f"- min_per_operator: {balance.get('min_per_operator', '-')}",
                f"- sample_count: {balance.get('sample_count', '-')}",
                f"- enough_samples: {balance.get('enough_samples', False)}",
                f"- ok: {balance.get('ok', False)}",
                f"- counts: {balance.get('counts', {})}",
                f"- debt: {balance.get('debt', {})}",
                "",
            ]
        )
        recovery = snapshot.get("balance_recovery", {})
        if isinstance(recovery, dict) and recovery.get("plans"):
            lines.extend(
                [
                    "## Balance Recovery",
                    "",
                    f"- next_debt_target: {recovery.get('next_debt_target', '-') or '-'}",
                    f"- uncompensated_ops: {recovery.get('uncompensated_ops', [])}",
                    "",
                    "| op | debt | state | compensated | recovery_action |",
                    "|---|---:|---|---:|---|",
                ]
            )
            for item in recovery.get("plans", []):
                if not isinstance(item, dict):
                    continue
                lines.append(
                    f"| {item.get('op', '-')} | {item.get('debt', '-')} | "
                    f"{item.get('state', '-')} | {item.get('compensated', False)} | "
                    f"{item.get('recovery_action', '-')} |"
                )
            lines.append("")
    else:
        lines.extend(["- unavailable", ""])
    submit_gap = snapshot.get("completion_to_next_submit", {})
    if isinstance(submit_gap, dict) and submit_gap:
        lines.extend(
            [
                "## Submit Gap",
                "",
                f"- window_size: {submit_gap.get('window_size', '-')}",
                f"- threshold_seconds: {submit_gap.get('threshold_seconds', '-')}",
                f"- sample_count: {submit_gap.get('sample_count', '-')}",
                f"- ok: {submit_gap.get('ok', False)}",
                f"- max_gap_seconds: {submit_gap.get('max_gap_seconds', '-')}",
                f"- violation_count: {submit_gap.get('violation_count', 0)}",
                "",
            ]
        )
    lines.extend(
        [
            "## Operators",
            "",
            "| op | gate | owner | action | latest_result | verdict | weighted_us | case | usage | selected | dispatch | solver_notify |",
            "|---|---|---|---|---|---|---:|---|---:|---:|---:|---:|",
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
            case = (
                data.get("latest_case", {})
                if isinstance(data.get("latest_case"), dict)
                else {}
            )
            usage = "-"
            if case:
                usage = f"{case.get('usage_count', '-')}/{case.get('max_usage', '-')}"
            weighted = result.get("weighted_time_us")
            lines.append(
                f"| {op} | {data.get('gate_stage', '')} | {data.get('next_owner', '')} | "
                f"{data.get('action', '')} | {result.get('test_version', '-')} | "
                f"{result.get('verdict', '-')} | {weighted if weighted is not None else '-'} | "
                f"{case.get('case_version', '-')} | {usage} | "
                f"{data.get('recent_selected_count', 0)} | "
                f"{data.get('recent_dispatch_submit_count', 0)} | "
                f"{data.get('recent_notify_solver_count', 0)} |"
            )
    lines.append("")
    knowledge = snapshot.get("knowledge_usage", {})
    knowledge_operators = (
        knowledge.get("operators", {}) if isinstance(knowledge, dict) else {}
    )
    if isinstance(knowledge_operators, dict) and knowledge_operators:
        lines.extend(
            [
                "## Shared Knowledge",
                "",
                "| op | ok | case | focus | focus_lines | coverage_current | open_hypotheses | solver_citations | tester_citations | result_triaged | missing |",
                "|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---|",
            ]
        )
        for op, data in knowledge_operators.items():
            if not isinstance(data, dict):
                continue
            shared = data.get("shared_knowledge", {})
            if not isinstance(shared, dict):
                shared = {}
            version_citations = shared.get("version_file_citations", {})
            casegen_citations = shared.get("casegen_file_citations", {})
            solver_ok = bool(version_citations) and all(version_citations.values())
            tester_ok = bool(casegen_citations) and all(casegen_citations.values())
            missing = ", ".join(str(item) for item in shared.get("missing", [])) or "-"
            lines.append(
                f"| {op} | {shared.get('ok', False)} | {data.get('case_version', '-')} | "
                f"{shared.get('current_focus_sections_complete', False) and shared.get('current_focus_bounded', False)} | "
                f"{shared.get('current_focus_line_count', 0)} | "
                f"{shared.get('case_coverage_current', False)} | "
                f"{shared.get('open_hypothesis_count', 0)} | {solver_ok} | {tester_ok} | "
                f"{shared.get('latest_result_triaged', False)} | {missing} |"
            )
        lines.append("")
    return "\n".join(lines)
