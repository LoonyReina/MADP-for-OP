from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Protocol

from ascendop_protocol.automation import TRIGGER_RULE_SCHEMA

from ascendop_daemon.control_plane.control_database import ControlDatabase
from ascendop_daemon.core.models import (
    ActionKind,
    BoardSnapshot,
    DaemonConfig,
    GateDecision,
    solver_thread_for,
    tester_thread_for,
)
from ascendop_daemon.storage.state_reader import StateReader
from ascendop_daemon.workflow.gate_engine import GateEngine


WORKFLOW_ROLE_ACTION = "notify_workflow_role"


class SnapshotReader(Protocol):
    def read(self) -> BoardSnapshot: ...


class SessionGateCoordinator:
    """Project board-owned Solver/Tester delivery through the V3 action outbox."""

    def __init__(
        self,
        root: Path,
        database: ControlDatabase,
        config: DaemonConfig,
        *,
        assistant_target_id: str,
        reader: SnapshotReader | None = None,
    ) -> None:
        self.root = root.resolve()
        self.database = database
        self.config = config
        self.assistant_target_id = assistant_target_id.strip()
        if not self.assistant_target_id:
            raise ValueError("workflow relay target id must not be empty")
        self.reader = reader or StateReader(self.root, config)
        self.gate_engine = GateEngine(config)

    def run_once(self) -> dict[str, Any]:
        try:
            snapshot = self.reader.read()
            decisions = self.gate_engine.evaluate(
                snapshot.rows,
                snapshot.transport,
            )
        except Exception as exc:
            return {
                "board_rows": 0,
                "eligible_count": 0,
                "actions": [],
                "errors": [str(exc)],
            }

        actions: list[dict[str, Any]] = []
        errors: list[dict[str, str]] = []
        eligible = 0
        for decision in decisions:
            if decision.action not in {
                ActionKind.NOTIFY_SOLVER,
                ActionKind.NOTIFY_TESTER_CASEGEN,
            }:
                continue
            eligible += 1
            try:
                action = self._publish(decision)
            except (OSError, ValueError) as exc:
                errors.append({"operator": decision.row.op, "error": str(exc)})
                continue
            if action is not None:
                actions.append(action)
        return {
            "captured_at": snapshot.captured_at,
            "board_rows": len(snapshot.rows),
            "eligible_count": eligible,
            "actions": actions,
            "errors": errors,
        }

    def _publish(self, decision: GateDecision) -> dict[str, Any] | None:
        row = decision.row
        role = (
            "solver"
            if decision.action == ActionKind.NOTIFY_SOLVER
            else "tester"
        )
        thread_id = (
            solver_thread_for(self.config, row.op)
            if role == "solver"
            else tester_thread_for(self.config, row.op)
        )
        if not thread_id:
            raise ValueError(f"{row.op} {role} thread id is not registered")
        runbook = row.solver_goal if role == "solver" else row.tester_goal
        runbook = normalize_relative_path(runbook)
        if not runbook or not (self.root / runbook).is_file():
            raise ValueError(f"{row.op} {role} runbook is missing: {runbook}")
        workspace = role_workspace(self.root, row.op, role)
        board_identity = {
            "season": row.season,
            "operator": row.op,
            "role": role,
            "thread_id": thread_id,
            "gate_stage": row.gate_stage,
            "next_owner": row.next_owner,
            "wakeups": row.wakeups,
            "next_command": row.next_command,
            "runbook_path": runbook,
        }
        board_digest = hashlib.sha256(
            json.dumps(
                board_identity,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        parameters = {
            **board_identity,
            "board_digest": board_digest,
            "prompt": build_role_prompt(board_identity, board_digest),
        }
        evidence = [runbook]
        profile = (
            self.root
            / "Develop"
            / "tasks"
            / row.season
            / row.op
            / "TASK_EXECUTION_PROFILE.json"
        )
        if profile.is_file():
            evidence.append(profile.relative_to(self.root).as_posix())
        rule = {
            "schema": TRIGGER_RULE_SCHEMA,
            "rule_id": f"workflow-gate:{row.season}:{row.op}:{role}",
            "when": {"gate_ready": True},
            "action": WORKFLOW_ROLE_ACTION,
            "assistant_target_id": self.assistant_target_id,
            "runbook_path": runbook,
        }
        return self.database.create_assistant_action_if_triggered(
            rule,
            {"gate_ready": True},
            operator=row.op,
            candidate_digest=board_digest,
            workspace=workspace,
            runbook_path=runbook,
            evidence=evidence,
            parameters=parameters,
        )


def role_workspace(root: Path, operator: str, role: str) -> str:
    candidates = (
        [Path("operators_workspace") / operator]
        if role == "solver"
        else [Path("TestUtils") / "casegen" / operator]
    )
    for candidate in candidates:
        if (root / candidate).is_dir():
            return candidate.as_posix()
    raise ValueError(f"{operator} {role} workspace is missing")


def normalize_relative_path(value: str) -> str:
    path = value.strip().replace("\\", "/")
    if path.startswith("/") or any(part == ".." for part in path.split("/")):
        return ""
    return path


def build_role_prompt(identity: dict[str, str], board_digest: str) -> str:
    return "\n".join(
        [
            "<ascendop_workflow_gate>",
            "  <schema>ascendop.workflow-role-gate.v1</schema>",
            f"  <board_digest>{board_digest}</board_digest>",
            f"  <season>{identity['season']}</season>",
            f"  <operator>{identity['operator']}</operator>",
            f"  <role>{identity['role']}</role>",
            f"  <gate_stage>{identity['gate_stage']}</gate_stage>",
            f"  <next_owner>{identity['next_owner']}</next_owner>",
            f"  <runbook_path>{identity['runbook_path']}</runbook_path>",
            f"  <next_command>{identity['next_command']}</next_command>",
            "</ascendop_workflow_gate>",
            "",
            "This is the daemon-delivered real gate for your registered role.",
            "Re-read the live session board and the runbook. Act only if the board",
            "digest-relevant fields and owner still match. Respect role boundaries;",
            "do not bypass daemon-owned queue, result, archive, or official submission.",
        ]
    )
