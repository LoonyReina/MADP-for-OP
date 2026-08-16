from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Mapping

from ascendop_protocol.workflow import WORKFLOW_ACTION_SCHEMA, validate_workflow_action

from ascendop_daemon.automation.agent_workspace import AgentWorkspace
from ascendop_daemon.automation.case_proposals import CASE_MATERIALIZER_GENERATION
from ascendop_daemon.control_plane.control_database import ControlDatabase
from ascendop_daemon.storage.control_types import SCHEMA_VERSION


class AgentPromotionQueue:
    """Publish durable daemon-owned promotion actions from sealed Agent output."""

    def __init__(
        self,
        root: Path,
        database: ControlDatabase,
        workspace: AgentWorkspace,
        *,
        code_generation: str,
    ) -> None:
        self.root = root.resolve()
        self.database = database
        self.workspace = workspace
        self.code_generation = code_generation

    def enqueue_source(
        self,
        action: Mapping[str, Any],
        seal: Mapping[str, Any],
    ) -> dict[str, Any]:
        seal_path = self.workspace.run_root(str(action["action_id"])) / "source-seal.json"
        seal_digest = hashlib.sha256(seal_path.read_bytes()).hexdigest()
        idempotency_key = (
            f"agent-source-promotion:{action['action_id']}:{seal['source_after_digest']}"
        )
        return self._publish(
            action,
            action_id=_action_id("wfa-asp", idempotency_key),
            idempotency_key=idempotency_key,
            action_kind="promote-agent-source",
            operation="agent-source-promote",
            seal_path=seal_path,
            seal_digest=seal_digest,
            identity={
                "origin": "agent-source-promotion",
                "agent_action_id": str(action["action_id"]),
                "iteration_id": str(action["iteration_id"]),
                "source_before_digest": str(seal["source_before_digest"]),
                "source_after_digest": str(seal["source_after_digest"]),
            },
            priority=1000,
        )

    def enqueue_case(
        self,
        action: Mapping[str, Any],
        seal: Mapping[str, Any],
    ) -> dict[str, Any]:
        if str(action.get("role") or "") != "tester":
            raise ValueError("case promotion requires a Tester action")
        seal_path = self.workspace.run_root(str(action["action_id"])) / "source-seal.json"
        seal_digest = hashlib.sha256(seal_path.read_bytes()).hexdigest()
        idempotency_key = (
            f"agent-case-promotion:{action['action_id']}:"
            f"{action['candidate_version']}:{seal['source_after_digest']}:"
            f"{CASE_MATERIALIZER_GENERATION}"
        )
        return self._publish(
            action,
            action_id=_action_id("wfa-acp", idempotency_key),
            idempotency_key=idempotency_key,
            action_kind="promote-agent-case",
            operation="agent-case-promote",
            seal_path=seal_path,
            seal_digest=seal_digest,
            identity={
                "origin": "agent-case-promotion",
                "agent_action_id": str(action["action_id"]),
                "iteration_id": str(action["iteration_id"]),
                "case_version": str(action["candidate_version"]),
                "materializer_generation": CASE_MATERIALIZER_GENERATION,
                "source_before_digest": str(seal["source_before_digest"]),
                "source_after_digest": str(seal["source_after_digest"]),
            },
            priority=1002,
        )

    def enqueue_output(
        self,
        action: Mapping[str, Any],
        seal: Mapping[str, Any],
        *,
        source_seal: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        seal_path = self.workspace.run_root(str(action["action_id"])) / "output-seal.json"
        seal_digest = hashlib.sha256(seal_path.read_bytes()).hexdigest()
        source_seal = (
            source_seal
            if source_seal and source_seal.get("changed_paths")
            else None
        )
        source_path = self.workspace.run_root(str(action["action_id"])) / "source-seal.json"
        source_digest = (
            hashlib.sha256(source_path.read_bytes()).hexdigest()
            if source_seal is not None
            else ""
        )
        idempotency_key = (
            f"agent-output-promotion:{action['action_id']}:{source_digest}:{seal_digest}"
        )
        artifacts = [{"path": seal_path, "sha256": seal_digest}]
        if source_seal is not None:
            artifacts.insert(0, {"path": source_path, "sha256": source_digest})
        return self._publish(
            action,
            action_id=_action_id("wfa-aop", idempotency_key),
            idempotency_key=idempotency_key,
            action_kind="promote-agent-output",
            operation="agent-output-promote",
            seal_path=seal_path,
            seal_digest=seal_digest,
            identity={
                "origin": "agent-output-promotion",
                "agent_action_id": str(action["action_id"]),
                "iteration_id": str(action["iteration_id"]),
                "contracts_digest": str(seal["contracts_digest"]),
                "output_count": len(seal["outputs"]),
                "output_seal_sha256": seal_digest,
                "source_after_digest": (
                    str(source_seal["source_after_digest"])
                    if source_seal is not None
                    else ""
                ),
            },
            priority=1001,
            artifacts=artifacts,
        )

    def _publish(
        self,
        action: Mapping[str, Any],
        *,
        action_id: str,
        idempotency_key: str,
        action_kind: str,
        operation: str,
        seal_path: Path,
        seal_digest: str,
        identity: Mapping[str, Any],
        priority: int,
        artifacts: list[Mapping[str, Any]] | None = None,
    ) -> dict[str, Any]:
        display_name = str(
            action.get("candidate_identity", {}).get("display_name")
            or action["operator_id"]
        )
        workflow_action = validate_workflow_action(
            {
                "schema": WORKFLOW_ACTION_SCHEMA,
                "action_id": action_id,
                "idempotency_key": idempotency_key,
                "action_kind": action_kind,
                "campaign": str(action["campaign"]),
                "operator": display_name,
                "test_version": str(action["candidate_version"]),
                "board_revision": str(action["board_revision"]),
                "producer_generation": self.code_generation,
                "control_schema": SCHEMA_VERSION,
                "origin_workspace": str(action["origin_workspace"]),
                "priority": priority,
                "resource_class": "local",
                "arguments": {
                    "operation": operation,
                    "positional": [seal_path.relative_to(self.root).as_posix()],
                    "options": {},
                },
                "candidate_identity": dict(identity),
                "artifacts": [
                    {
                        "path": Path(str(item["path"]))
                        .relative_to(self.root)
                        .as_posix(),
                        "sha256": str(item["sha256"]),
                    }
                    for item in (
                        artifacts
                        or [{"path": seal_path, "sha256": seal_digest}]
                    )
                ],
                "parent_trace_id": str(action["action_id"]),
                "deadline_at": "",
                "retry_policy_ref": "workflow-v4-central-retry",
                "created_at": _utc_now(),
            }
        )
        return self.database.create_workflow_action(workflow_action)


def _action_id(prefix: str, idempotency_key: str) -> str:
    digest = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()[:24]
    return f"{prefix}-{digest}"


def _utc_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()
