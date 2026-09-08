from __future__ import annotations

from pathlib import Path
import pytest

from ascendop_daemon.automation.agent_completion import (
    AgentCompletionService,
    AgentCompletionError,
    OutcomeNormalizer,
)
from ascendop_daemon.automation.agent_outputs import AgentOutputBroker
from ascendop_daemon.automation.agent_workspace import AgentWorkspace
from ascendop_daemon.control_plane.control_database import ControlDatabase
from ascendop_control.storage.outbox_repository import enqueue_control_intent
from datetime import datetime, timezone


class _Database:
    def __init__(self, action: dict[str, object], *, root: Path, current: bool = True) -> None:
        self.action = action
        self.current = current
        self.receipt: dict[str, object] | None = None
        self.promotions: dict[str, dict[str, object]] = {}
        self.outbox_store = ControlDatabase(root / "completion-fixture.sqlite3")
        self.outbox_store.initialize()

    def __getattr__(self, name):
        return getattr(self.outbox_store, name)

    def agent_action(self, action_id: str):
        return {"action": self.action, "current_attempt_id": "attempt-1",
                "state": self.receipt["status"] if self.receipt else "running"} if action_id == self.action["action_id"] else None

    def workflow_agent_action_is_current(self, action_id: str) -> bool:
        return self.current and action_id == self.action["action_id"]

    def complete_agent_action(self, receipt, *, lease_token: str, continuation=None):
        assert lease_token == "lease-token"
        if self.receipt is None:
            if continuation is not None and receipt["status"] == "completed":
                with self.outbox_store.transaction() as conn:
                    enqueue_control_intent(
                        conn, origin_id=receipt["action_id"], attempt_id="attempt-1",
                        topic="agent.completion", created_at=datetime.now(timezone.utc).isoformat(),
                        payload={"receipt": receipt, "continuation": continuation},
                    )
            self.receipt = dict(receipt)
        return {"state": self.receipt["status"], "action": self.action}

    def agent_action_receipt(self, action_id: str):
        return self.receipt if action_id == self.action["action_id"] else None

    def create_workflow_action(self, action):
        key = str(action["idempotency_key"])
        self.promotions.setdefault(key, dict(action))
        return self.promotions[key]


def test_outcome_normalizer_maps_interrupted_before_workflow_completion() -> None:
    normalized = OutcomeNormalizer().normalize(
        {
            "schema": "ascendop.native-turn-outcome.v1",
            "action_id": "action-interrupted",
            "native_session_id": "session-interrupted",
            "native_turn_id": "turn-interrupted",
            "terminal_status": "interrupted",
            "structured_result": {"summary": "host interrupted the turn"},
            "artifact_refs": [],
            "telemetry": {
                "usage": {"input_tokens": 100},
                "skills": {"invoked": ["irrelevant-to-truth"]},
            },
            "observed_at": "2026-08-17T00:00:00+00:00",
        },
        completion_metadata={"adapter_id": "test-runtime"},
    )

    assert normalized["status"] == "failed"
    assert normalized["reported_status"] == "interrupted"
    assert normalized["completion"]["failure_class"] == "adapter-execution"
    assert normalized["completion"]["telemetry"]["usage"]["input_tokens"] == 100


def test_completion_is_control_equivalent_across_runtime_metadata(tmp_path: Path) -> None:
    desktop = _complete_source_change(tmp_path / "desktop", runtime="codex-ide")
    cli = _complete_source_change(tmp_path / "cli", runtime="codex-cli")

    assert _control_projection(desktop) == _control_projection(cli)
    assert desktop["outcome"]["outputs"][0]["output_kind"] == "source-change"
    assert cli["outcome"]["outputs"][0]["output_kind"] == "source-change"


def test_duplicate_completion_reuses_immutable_seals(tmp_path: Path) -> None:
    root = tmp_path / "duplicate"
    first, service = _complete_source_change(
        root,
        runtime="codex-ide",
        return_service=True,
    )
    repeated = service.complete(
        action_id="action-conformance",
        lease_token="lease-token",
        lease_id="lease-id",
        agent_id="agent-id",
        started_at="2026-08-17T00:00:00+00:00",
        native_outcome=_native("codex-ide"),
        completion_metadata={"adapter_id": "codex-ide"},
    )

    assert repeated["receipt"] == first["receipt"]
    assert repeated["outcome"]["outputs"] == first["outcome"]["outputs"]
    assert repeated["promotion"] == first["promotion"]


def test_stale_gate_is_cancelled_before_workspace_seal(tmp_path: Path) -> None:
    root = tmp_path / "stale"
    action, workspace, outputs = _staged_action(root)
    database = _Database(action, root=root, current=False)
    service = AgentCompletionService(
        root,
        database,
        code_generation="release-v5",
        workspace=workspace,
        outputs=outputs,
    )

    result = service.complete(
        action_id="action-conformance",
        lease_token="lease-token",
        lease_id="lease-id",
        agent_id="agent-id",
        started_at="2026-08-17T00:00:00+00:00",
        native_outcome=_native("any-runtime"),
    )

    assert result["terminal"]["state"] == "cancelled"
    assert result["receipt"]["completion"]["failure_class"] == "agent-gate-obsolete"
    assert result["outcome"]["failure_class"] == "cancelled"
    assert result["promotion"] is None
    assert not (workspace.run_root("action-conformance") / "source-seal.json").exists()


def test_same_native_turn_cannot_change_accepted_semantics(tmp_path: Path) -> None:
    first, service = _complete_source_change(tmp_path, runtime="codex-ide", return_service=True)
    changed = _native("codex-ide")
    changed["structured_result"] = {"summary": "different outcome"}
    with pytest.raises(AgentCompletionError, match="conflicting native"):
        service.complete(
            action_id="action-conformance", lease_token="lease-token", lease_id="lease-id",
            agent_id="agent-id", started_at="2026-08-17T00:00:00+00:00", native_outcome=changed,
        )
    assert service.database.receipt == first["receipt"]


def test_common_completion_rejects_out_of_scope_source_change(tmp_path: Path) -> None:
    root = tmp_path / "scope"
    action, workspace, outputs = _staged_action(root)
    isolated = workspace.run_root("action-conformance") / "workspace"
    (isolated / "outside.txt").write_text("not granted\n", encoding="ascii")
    database = _Database(action, root=root)
    service = AgentCompletionService(
        root,
        database,
        code_generation="release-v5",
        workspace=workspace,
        outputs=outputs,
    )

    result = service.complete(
        action_id="action-conformance",
        lease_token="lease-token",
        lease_id="lease-id",
        agent_id="agent-id",
        started_at="2026-08-17T00:00:00+00:00",
        native_outcome=_native("any-runtime"),
    )

    assert result["terminal"]["state"] == "failed"
    assert result["receipt"]["completion"]["failure_class"] == "write-scope-violation"
    assert result["receipt"]["completion"]["out_of_scope_paths"] == ["outside.txt"]
    assert result["promotion"] is None


def _complete_source_change(
    root: Path,
    *,
    runtime: str,
    return_service: bool = False,
):
    action, workspace, outputs = _staged_action(root)
    isolated = workspace.run_root("action-conformance") / "workspace"
    (isolated / "op_kernel" / "demo.cpp").write_text(
        "optimized\n", encoding="ascii"
    )
    database = _Database(action, root=root)
    service = AgentCompletionService(
        root,
        database,
        code_generation="release-v5",
        workspace=workspace,
        outputs=outputs,
    )
    result = service.complete(
        action_id="action-conformance",
        lease_token="lease-token",
        lease_id="lease-id",
        agent_id="agent-id",
        started_at="2026-08-17T00:00:00+00:00",
        native_outcome=_native(runtime),
        completion_metadata={"adapter_id": runtime},
    )
    return (result, service) if return_service else result


def _staged_action(
    root: Path,
) -> tuple[dict[str, object], AgentWorkspace, AgentOutputBroker]:
    source = root / "operators_workspace" / "Demo"
    source.mkdir(parents=True)
    (source / "op_kernel").mkdir()
    (source / "op_kernel" / "demo.cpp").write_text("base\n", encoding="ascii")
    workspace = AgentWorkspace(root)
    action = {
        "action_id": "action-conformance",
        "iteration_id": "iteration-conformance",
        "campaign": "August",
        "operator_id": "august.demo",
        "role": "solver",
        "board_revision": "board-conformance",
        "origin_workspace": "operators_workspace/Demo",
        "candidate_version": "Demo_V1_1",
        "candidate_identity": {
            "display_name": "Demo",
            "execution_source_digest": workspace.digest(source),
        },
        "write_scope": ["op_kernel"],
        "output_contracts": [],
    }
    outputs = AgentOutputBroker(root, workspace.runs_root)
    _, isolated, _ = workspace.stage(action)
    outputs.stage(action, isolated)
    return action, workspace, outputs


def _native(runtime: str) -> dict[str, object]:
    return {
        "schema": "ascendop.native-turn-outcome.v1",
        "action_id": "action-conformance",
        "native_session_id": f"{runtime}-session",
        "native_turn_id": f"{runtime}-turn",
        "terminal_status": "completed",
        "structured_result": {"summary": "same source change"},
        "artifact_refs": [],
        "telemetry": {"usage": {}, "skills": {}},
        "observed_at": "2026-08-17T00:01:00+00:00",
    }


def _control_projection(result: dict[str, object]) -> dict[str, object]:
    receipt = result["receipt"]
    completion = receipt["completion"]
    outcome = result["outcome"]
    promotion = result["promotion"]
    return {
        "terminal": result["terminal"]["state"],
        "receipt_status": receipt["status"],
        "changed_paths": completion["changed_paths"],
        "source_before_digest": completion["source_before_digest"],
        "source_after_digest": completion["source_after_digest"],
        "disposition": outcome["disposition"],
        "output_kinds": [item["output_kind"] for item in outcome["outputs"]],
        "promotion_kind": promotion["action_kind"],
        "promotion_operation": promotion["arguments"]["operation"],
    }
