from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import re
from typing import Any

from ascendop_daemon.core.models import (
    ActionKind,
    DaemonConfig,
    GateDecision,
    extract_row_test_version,
)
from ascendop_daemon.workflow.casegen_evidence import enforce_casegen_evidence


LEGACY_TRANSPORT_ACTIONS = {
    ActionKind.DISPATCH_SUBMIT,
    ActionKind.RECOVER_BLOCKED,
    ActionKind.RECOVER_GITPARTNER_WORKTREE,
    ActionKind.HEARTBEAT_ACTIVE_REQUEST,
    ActionKind.CANCEL_STALLED_REQUEST,
}

DRAIN_ALLOWED_ACTIONS = {
    ActionKind.HOLD,
    ActionKind.REPAIR_QUEUE,
    ActionKind.RECONCILE_TERMINAL_RESULT,
    *LEGACY_TRANSPORT_ACTIONS,
}


class WorkflowPolicyPipeline:
    """Fail-closed V4 policy filters applied before scheduling."""

    def __init__(
        self,
        root: Path,
        config: DaemonConfig,
        *,
        database: Any | None = None,
    ) -> None:
        self.root = root.resolve()
        self.config = config
        self.database = database

    def apply(
        self,
        decisions: tuple[GateDecision, ...],
    ) -> tuple[GateDecision, ...]:
        filtered = enforce_casegen_evidence(self.root, decisions, self.config)
        filtered = self._fence_consumed_test_versions(filtered)
        filtered = self._fence_detached_operators(filtered)
        return self._fence_legacy_transport(filtered)

    def _fence_consumed_test_versions(
        self,
        decisions: tuple[GateDecision, ...],
    ) -> tuple[GateDecision, ...]:
        if self.database is None:
            return decisions
        filtered: list[GateDecision] = []
        for decision in decisions:
            # This fence owns duplicate operator-test publication only. A
            # consumed version may still have daemon diagnostics, result
            # reconciliation, or a typed Solver request repair in flight.
            if decision.action != ActionKind.PUBLISH_TEST_REQUEST:
                filtered.append(decision)
                continue
            test_version = extract_row_test_version(decision.row)
            if not test_version:
                filtered.append(
                    _hold(decision, "publishable board row has no immutable test version")
                    if decision.action == ActionKind.PUBLISH_TEST_REQUEST
                    else decision
                )
                continue
            rows = self.database.test_requests_for_logical_identity(
                decision.row.op,
                test_version,
            )
            publishable = [
                row
                for row in rows
                if _is_publishable_operator_test(row.get("manifest"))
            ]
            if not publishable:
                filtered.append(decision)
                continue
            states = {str(row.get("state") or "unknown") for row in publishable}
            identities = ", ".join(
                f"{row.get('request_id')}:{row.get('state')}"
                for row in publishable
            )
            if states == {"failed"}:
                canonical = (
                    self.root
                    / "operators_testresult"
                    / decision.row.op
                    / test_version
                    / "RESULT.md"
                )
                archived = _latest_archived_terminal_result(
                    self.root,
                    decision.row.op,
                    test_version,
                )
                if not canonical.is_file() and archived is not None:
                    relative = archived.relative_to(self.root).as_posix()
                    row = replace(
                        decision.row,
                        gate_stage="terminal-result-reconcile",
                        next_owner="daemon",
                        wakeups="TEST_RESULT_RECONCILE",
                        next_command=(
                            "reconcile the archived terminal failure into its canonical "
                            "RESULT.md without reopening the consumed test version"
                        ),
                        action_descriptor={
                            "schema": "ascendop.board-action.v1",
                            "operation": "reconcile-terminal-result",
                            "positional": [decision.row.op, test_version],
                            "options": {
                                "source_result": relative,
                                "claimed_by": "tester-daemon-v4-terminal-reconcile",
                            },
                        },
                    )
                    filtered.append(
                        GateDecision(
                            row=row,
                            action=ActionKind.RECONCILE_TERMINAL_RESULT,
                            reason=(
                                "control DB is terminal failed but restore-submit moved "
                                "the canonical result into an immutable attempt archive"
                            ),
                            command=(
                                "python scripts\\next_workflow.py reconcile-terminal-result "
                                f"{decision.row.op} {test_version} "
                                f"--source-result {relative} "
                                "--claimed-by tester-daemon-v4-terminal-reconcile"
                            ),
                            priority=98,
                            action_descriptor=row.action_descriptor,
                            blocks_operator=decision.blocks_operator,
                        )
                    )
                    continue
                if not canonical.is_file():
                    filtered.append(
                        _hold(
                            decision,
                            "control DB is terminal failed but no canonical or archived "
                            "terminal RESULT.md is available for Solver handoff",
                        )
                    )
                    continue
                row = replace(
                    decision.row,
                    gate_stage="immutable-test-version-consumed",
                    next_owner="solver",
                    wakeups="IMMUTABLE_TEST_VERSION_CONSUMED",
                    next_command=(
                        f"consume terminal evidence for {test_version}; the version "
                        "is immutable and the next source candidate must use a new "
                        "test version"
                    ),
                    action_descriptor={},
                )
                filtered.append(
                    GateDecision(
                        row=row,
                        action=ActionKind.NOTIFY_SOLVER,
                        reason=(
                            "raw board requested publication after the immutable "
                            f"test version was already consumed: {identities}"
                        ),
                        priority=68,
                        blocks_operator=decision.blocks_operator,
                    )
                )
                continue
            filtered.append(
                _hold(
                    decision,
                    "raw board requested duplicate publication while the immutable "
                    f"test version is already owned by control DB: {identities}",
                )
            )
        return tuple(filtered)

    def _fence_detached_operators(
        self,
        decisions: tuple[GateDecision, ...],
    ) -> tuple[GateDecision, ...]:
        draining = set(self.config.draining_operators)
        if not draining:
            return decisions
        return tuple(
            decision
            if decision.row.op not in draining
            or decision.action in DRAIN_ALLOWED_ACTIONS
            else _hold(
                decision,
                "operator plugin is draining; V4 may observe accepted work but "
                "must not create a new workflow gate",
            )
            for decision in decisions
        )

    @staticmethod
    def _fence_legacy_transport(
        decisions: tuple[GateDecision, ...],
    ) -> tuple[GateDecision, ...]:
        return tuple(
            _hold(
                decision,
                "V4 owns transport through SubmitIntake, control.sqlite3 and the "
                "endpoint dispatcher; the legacy board transport command is "
                "diagnostic-only and cannot execute",
            )
            if decision.action in LEGACY_TRANSPORT_ACTIONS
            else decision
            for decision in decisions
        )


def _hold(decision: GateDecision, reason: str) -> GateDecision:
    return GateDecision(
        row=decision.row,
        action=ActionKind.HOLD,
        reason=reason,
        command="",
        priority=0,
        blocks_operator=decision.blocks_operator,
    )


def _is_publishable_operator_test(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    workflow = value.get("workflow")
    return bool(
        value.get("schema") == "ascendop.test-request.v1"
        and isinstance(workflow, dict)
        and workflow.get("operation_kind", "operator-test") == "operator-test"
        and workflow.get("publish_eligible", True) is True
        and workflow.get("workflow_ingest", True) is True
    )


def _latest_archived_terminal_result(
    root: Path,
    operator: str,
    test_version: str,
) -> Path | None:
    result_root = root / "operators_testresult" / operator / test_version
    if not result_root.is_dir():
        return None
    candidates: list[tuple[int, str, Path]] = []
    for path in result_root.iterdir():
        match = re.fullmatch(r"[A-Za-z0-9_]+_attempt_(\d{3,})", path.name)
        result = path / "RESULT.md"
        engine_return = path / "ENGINE_RETURN.json"
        if match and result.is_file() and engine_return.is_file():
            candidates.append((int(match.group(1)), path.name, result))
    return max(candidates)[2] if candidates else None
