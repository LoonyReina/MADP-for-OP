from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import re
from typing import Any


class ActionKind(str, Enum):
    HOLD = "hold"
    GENERATE_WORKSPACE = "generate_workspace"
    NOTIFY_SOLVER = "notify_solver"
    NOTIFY_TESTER_CASEGEN = "notify_tester_casegen"
    PREPARE_SUBMIT = "prepare_submit"
    DISPATCH_SUBMIT = "dispatch_submit"
    RESTORE_SUBMIT = "restore_submit"
    REQUEUE_SUBMIT = "requeue_submit"
    REPAIR_QUEUE = "repair_queue"
    ADVANCE_RELEASE = "advance_release"
    GENERATE_CASE_VERSION = "generate_case_version"
    RECOVER_BLOCKED = "recover_blocked"
    RECOVER_GITPARTNER_WORKTREE = "recover_gitpartner_worktree"
    HEARTBEAT_ACTIVE_REQUEST = "heartbeat_active_request"
    CANCEL_STALLED_REQUEST = "cancel_stalled_request"
    COLLECT_PROFILER_EVIDENCE = "collect_profiler_evidence"
    REVIEW_MANUAL = "review_manual"


@dataclass(frozen=True)
class OperatorSession:
    season: str = ""
    solver_thread_id: str = ""
    tester_thread_id: str = ""
    solver_model: str = ""
    solver_thinking: str = ""
    tester_model: str = ""
    tester_thinking: str = ""
    enabled: bool = True
    drain_requested: bool = False
    roles: dict[str, bool] = field(default_factory=dict)
    workflow_mode: str = "iterate"
    peer_code_root: str = ""
    peer_candidates: tuple[str, ...] = field(default_factory=tuple)
    benchmark_case_version: str = ""
    benchmark_baseline_source: str = ""
    benchmark_baseline_marker: str = ""
    completion_marker: str = ""
    knowledge_root: str = ""
    reference_retrieval_profile: str = ""


@dataclass(frozen=True)
class DaemonConfig:
    season: str
    transport: str
    remote_root: str
    operators: tuple[str, ...]
    draining_operators: tuple[str, ...] = field(default_factory=tuple)
    resources: tuple[dict[str, Any], ...] = field(default_factory=tuple)
    policy: dict[str, Any] = field(default_factory=dict)
    solver_threads: dict[str, str] = field(default_factory=dict)
    operator_sessions: dict[str, OperatorSession] = field(default_factory=dict)


def solver_session_replacement_allowed(config: DaemonConfig) -> bool:
    return bool(config.policy.get("allow_solver_session_replacement", True))


def operator_session(config: DaemonConfig, op: str) -> OperatorSession | None:
    sessions = getattr(config, "operator_sessions", {})
    session = sessions.get(op) if isinstance(sessions, dict) else None
    if session is not None:
        return session
    solver_threads = getattr(config, "solver_threads", {})
    solver_thread_id = solver_threads.get(op, "") if isinstance(solver_threads, dict) else ""
    if solver_thread_id:
        return OperatorSession(solver_thread_id=solver_thread_id, enabled=op in config.operators)
    return None


def operator_season(config: DaemonConfig, op: str) -> str:
    session = operator_session(config, op)
    return session.season if session and session.season else config.season


def config_seasons(config: DaemonConfig) -> tuple[str, ...]:
    seasons: list[str] = []
    operators = tuple(getattr(config, "operators", ()) or ())
    draining = tuple(getattr(config, "draining_operators", ()) or ())
    for op in (*operators, *draining):
        season = operator_season(config, op)
        if season not in seasons:
            seasons.append(season)
    return tuple(seasons) or (config.season,)


def observed_operators(config: DaemonConfig) -> tuple[str, ...]:
    """Active plugins plus detached plugins whose native turn must drain."""
    ordered: list[str] = []
    operators = tuple(getattr(config, "operators", ()) or ())
    draining = tuple(getattr(config, "draining_operators", ()) or ())
    for op in (*operators, *draining):
        if op not in ordered:
            ordered.append(op)
    return tuple(ordered)


def solver_thread_for(config: DaemonConfig, op: str) -> str:
    session = operator_session(config, op)
    if session and session.solver_thread_id:
        return session.solver_thread_id
    return config.solver_threads.get(op, "")


def tester_thread_for(config: DaemonConfig, op: str) -> str:
    session = operator_session(config, op)
    return session.tester_thread_id if session else ""


def relay_profile_for(config: DaemonConfig, op: str, role: str) -> tuple[str, str]:
    session = operator_session(config, op)
    normalized_role = role.strip().lower()
    if normalized_role == "tester":
        model = session.tester_model if session else ""
        thinking = session.tester_thinking if session else ""
    else:
        model = session.solver_model if session else ""
        thinking = session.solver_thinking if session else ""
    if not model:
        model = str(
            config.policy.get(f"{normalized_role}_relay_model")
            or config.policy.get("relay_model")
            or ""
        )
    if not thinking:
        thinking = str(
            config.policy.get(f"{normalized_role}_relay_thinking")
            or config.policy.get("relay_thinking")
            or ""
        )
    return model, thinking


def casegen_role_enabled(config: DaemonConfig, op: str) -> bool:
    session = operator_session(config, op)
    if session is None or not session.enabled:
        return False
    if op not in config.operators:
        return False
    return bool(session.roles.get("casegen", False))


def casegen_session_ops(config: DaemonConfig) -> tuple[str, ...]:
    ops: list[str] = []
    for op in observed_operators(config):
        session = operator_session(config, op)
        if session is not None and session.enabled and bool(session.roles.get("casegen", False)):
            ops.append(op)
    return tuple(ops)


def workflow_mode_for(config: DaemonConfig, op: str) -> str:
    session = operator_session(config, op)
    return session.workflow_mode if session else "iterate"


@dataclass(frozen=True)
class BoardRow:
    season: str
    op: str
    gate_stage: str
    next_owner: str
    solver_goal: str
    tester_goal: str
    wakeups: str
    next_command: str


@dataclass(frozen=True)
class BoardSnapshot:
    captured_at: str
    command: tuple[str, ...]
    rows: tuple[BoardRow, ...]
    raw_output: str
    transport: tuple["TransportObservation", ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class GateDecision:
    row: BoardRow
    action: ActionKind
    reason: str
    command: str = ""
    priority: int = 0
    blocks_operator: str = ""

    @property
    def action_id(self) -> str:
        test_version = extract_row_test_version(self.row) or extract_test_version(self.row.next_command)
        return "|".join(
            [
                self.row.op,
                test_version or "unknown-version",
                self.action.value,
                self.command or "no-command",
            ]
        )


@dataclass(frozen=True)
class DaemonPlan:
    selected: GateDecision | None
    decisions: tuple[GateDecision, ...]
    held: tuple[GateDecision, ...] = field(default_factory=tuple)
    scheduler_state: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TransportObservation:
    op: str
    test_version: str
    request_id: str = ""
    heartbeat_path: str = ""
    output_status_path: str = ""
    state: str = ""
    client_state: str = ""
    client_updated_at: str = ""
    client_progress_observed_at: str = ""
    terminal: bool | None = None
    stalled: bool | None = None
    remote_feedback_status: str = ""
    stall_reason: str = ""
    first_observed_at_utc: str = ""
    observed_at_utc: str = ""
    last_feedback_at_utc: str = ""
    elapsed_without_feedback_seconds: int | None = None
    elapsed_without_remote_feedback_seconds: int | None = None
    relay_publish_verify: str = ""
    client_ssh: str = ""

    @property
    def summary(self) -> str:
        parts: list[str] = []
        if self.state:
            parts.append(self.state)
        if self.client_state:
            parts.append(f"client:{self.client_state}")
        if self.remote_feedback_status:
            parts.append(self.remote_feedback_status)
        if self.stalled:
            parts.append("stalled")
        if self.relay_publish_verify:
            parts.append(self.relay_publish_verify)
        return "/".join(parts) if parts else "unknown"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def extract_test_version(text: str) -> str:
    for token in text.replace("\\", "/").replace("|", " ").split():
        clean = token.strip("`'\",;()[]")
        if "_V" in clean:
            parts = clean.split("/")
            for part in reversed(parts):
                if "_V" in part:
                    return part
    return ""


def extract_row_test_version(row: BoardRow) -> str:
    op = re.escape(row.op)
    pattern = re.compile(
        rf"(?:^|[\s`'\",;()\[\]])(?:{op}[\\/])?({op}_V\d+(?:_\d+)?[A-Za-z0-9_]*)",
        flags=re.IGNORECASE,
    )
    match = pattern.search(row.next_command)
    return match.group(1) if match else ""
