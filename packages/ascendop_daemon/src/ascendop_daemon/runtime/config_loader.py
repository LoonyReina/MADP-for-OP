from __future__ import annotations

import json
from pathlib import Path

from ascendop_daemon.core.models import DaemonConfig, OperatorSession


def load_config(path: Path, *, apply_completion_markers: bool = False) -> DaemonConfig:
    data = json.loads(path.read_text(encoding="utf-8"))
    solver_threads = dict(data.get("solver_threads", {}))
    operator_sessions = load_operator_sessions(data, solver_threads)
    repo_root = find_repo_root(path)
    configured_operators = tuple(str(op) for op in data.get("operators", []))
    eligible_operators = (
        tuple(
            op
            for op in configured_operators
            if operator_is_active(op, operator_sessions.get(op), repo_root)
        )
        if apply_completion_markers
        else configured_operators
    )
    draining_operators = tuple(
        op
        for op in eligible_operators
        if operator_sessions.get(op) is not None
        and operator_sessions[op].drain_requested
    )
    active_operators = tuple(op for op in eligible_operators if op not in draining_operators)
    return DaemonConfig(
        season=data["season"],
        transport=data.get("transport", "gitpartner"),
        remote_root=data.get("remote_root", ""),
        operators=active_operators,
        draining_operators=draining_operators,
        resources=tuple(data.get("resources", [])),
        policy=dict(data.get("policy", {})),
        agent_execution=dict(data.get("agent_execution", {})),
        solver_threads=solver_threads,
        operator_sessions=operator_sessions,
    )


def load_operator_sessions(
    data: dict[str, object],
    solver_threads: dict[str, str],
) -> dict[str, OperatorSession]:
    raw = data.get("operator_sessions", {})
    sessions: dict[str, OperatorSession] = {}
    if isinstance(raw, dict):
        for op, value in raw.items():
            if not isinstance(value, dict):
                continue
            roles = value.get("roles", {})
            if not isinstance(roles, dict):
                roles = {}
            sessions[str(op)] = OperatorSession(
                season=str(value.get("season") or ""),
                solver_thread_id=str(value.get("solver_thread_id") or solver_threads.get(str(op), "") or ""),
                tester_thread_id=str(value.get("tester_thread_id") or ""),
                solver_model=str(value.get("solver_model") or ""),
                solver_thinking=str(value.get("solver_thinking") or ""),
                tester_model=str(value.get("tester_model") or ""),
                tester_thinking=str(value.get("tester_thinking") or ""),
                enabled=bool(value.get("enabled", True)),
                drain_requested=bool(value.get("drain_requested", False)),
                roles={str(name): bool(flag) for name, flag in roles.items()},
                workflow_mode=str(value.get("workflow_mode") or "iterate"),
                peer_code_root=str(value.get("peer_code_root") or ""),
                peer_candidates=tuple(str(item) for item in value.get("peer_candidates", []) if str(item)),
                benchmark_case_version=str(value.get("benchmark_case_version") or ""),
                benchmark_baseline_source=str(value.get("benchmark_baseline_source") or ""),
                benchmark_baseline_marker=str(value.get("benchmark_baseline_marker") or ""),
                completion_marker=str(value.get("completion_marker") or ""),
                knowledge_root=str(value.get("knowledge_root") or ""),
                reference_retrieval_profile=str(value.get("reference_retrieval_profile") or ""),
            )
    for op, thread_id in solver_threads.items():
        sessions.setdefault(
            str(op),
            OperatorSession(solver_thread_id=str(thread_id or ""), enabled=True, roles={}),
        )
    return sessions


def find_repo_root(config_path: Path) -> Path:
    resolved = config_path.resolve()
    for parent in (resolved.parent, *resolved.parents):
        if (parent / "scripts" / "next_workflow.py").exists():
            return parent
    return resolved.parent


def operator_is_active(op: str, session: OperatorSession | None, repo_root: Path) -> bool:
    if session is not None and not session.enabled:
        return False
    if session is None or not session.completion_marker:
        return True
    if session.workflow_mode == "peer_benchmark" and session.benchmark_baseline_marker:
        baseline_marker = Path(session.benchmark_baseline_marker)
        if not baseline_marker.is_absolute():
            baseline_marker = repo_root / baseline_marker
        if not baseline_marker.exists():
            return True
    marker = Path(session.completion_marker)
    if not marker.is_absolute():
        marker = repo_root / marker
    return not marker.exists()
