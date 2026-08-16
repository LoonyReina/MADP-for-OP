from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping

from ascendop_protocol.automation import (
    AutomationContractError,
    validate_trigger_source,
    validate_trigger_source_registry,
)
from ascendop_protocol.workflow import (
    SOLVER_STEWARD_ESCALATION_STATE,
    validate_solver_steward_escalation,
)

from ascendop_daemon.automation.assistant_trigger import (
    AssistantTriggerEngine,
    AssistantTriggerError,
    load_trigger_rules,
)
from ascendop_daemon.control_plane.control_database import ControlDatabase
from ascendop_daemon.storage.control_types import ControlDatabaseError


DEFAULT_TRIGGER_REGISTRY = Path(
    "Develop/automation/assistant_trigger_sources.json"
)
DEFAULT_STEWARD_RUNBOOK = Path("docs/next/steward_escalation_runbook.md")


class AssistantCoordinator:
    def __init__(
        self,
        root: Path,
        database: ControlDatabase,
        *,
        registry_path: Path = DEFAULT_TRIGGER_REGISTRY,
        steward_target_id: str = "",
        steward_runbook_path: Path = DEFAULT_STEWARD_RUNBOOK,
    ) -> None:
        self.root = root.resolve()
        self.database = database
        self.registry_path = bounded_path(self.root, registry_path)
        self.steward_target_id = steward_target_id.strip()
        self.steward_runbook_path = steward_runbook_path

    def run_once(
        self,
        *,
        steward_escalations: Iterable[Mapping[str, Any]] = (),
    ) -> dict[str, Any]:
        actions: list[dict[str, Any]] = []
        errors: list[str] = []
        enabled: list[Mapping[str, Any]] = []
        if self.registry_path.is_file():
            try:
                registry = validate_trigger_source_registry(
                    read_object(self.registry_path)
                )
                enabled = [
                    item for item in registry["sources"] if item.get("enabled")
                ]
            except (OSError, ValueError, AutomationContractError) as exc:
                errors.append(f"registry: {exc}")
        for registration in enabled:
            source_id = str(registration["source_id"])
            try:
                actions.extend(self._evaluate_source(registration))
            except (OSError, ValueError, AutomationContractError, AssistantTriggerError) as exc:
                errors.append(f"{source_id}: {exc}")
        escalation_count = 0
        for escalation in steward_escalations:
            escalation_count += 1
            try:
                action = self._evaluate_steward_escalation(escalation)
                if action is not None:
                    actions.append(action)
            except (
                OSError,
                ValueError,
                AutomationContractError,
                AssistantTriggerError,
                ControlDatabaseError,
            ) as exc:
                identity = dict(
                    dict(escalation.get("action_descriptor") or {}).get("identity")
                    or {}
                )
                errors.append(
                    f"steward:{identity.get('operator') or 'unknown'}: {exc}"
                )
        return {
            "source_count": len(enabled),
            "steward_escalation_count": escalation_count,
            "actions": actions,
            "errors": errors,
        }

    def _evaluate_source(self, registration: Mapping[str, Any]) -> list[dict[str, Any]]:
        manifest_path = bounded_path(self.root, Path(registration["manifest_path"]))
        source = validate_trigger_source(read_object(manifest_path))
        if source["source_id"] != registration["source_id"]:
            raise AutomationContractError("source identity does not match registry")
        if not source.get("enabled", True):
            return []
        workspace = bounded_path(self.root, Path(source["workspace"]))
        if not workspace.is_dir():
            raise AssistantTriggerError(f"workspace is missing: {source['workspace']}")
        rules_path = bounded_path(self.root, Path(source["rules_path"]))
        metrics = read_object(bounded_path(self.root, Path(source["metrics_path"])))
        context = read_object(bounded_path(self.root, Path(source["context_path"])))
        candidate_digest = required_text(context, "candidate_digest")
        evidence = context.get("evidence", [])
        if not isinstance(evidence, list) or not all(
            isinstance(item, str) and item for item in evidence
        ):
            raise AssistantTriggerError("context evidence must be a list of paths")
        rules = load_trigger_rules(rules_path)
        for rule in rules:
            runbook = bounded_path(self.root, Path(rule["runbook_path"]))
            if not runbook.is_file():
                raise AssistantTriggerError(f"runbook is missing: {rule['runbook_path']}")
        return AssistantTriggerEngine(self.database).evaluate(
            rules,
            metrics,
            operator=str(source["operator"]),
            candidate_digest=candidate_digest,
            workspace=Path(source["workspace"]).as_posix(),
            evidence=evidence,
        )

    def _evaluate_steward_escalation(
        self,
        escalation: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        if not self.steward_target_id:
            raise AssistantTriggerError("steward Assistant target is not configured")
        descriptor = validate_solver_steward_escalation(
            dict(escalation.get("action_descriptor") or {})
        )
        identity = dict(descriptor.get("identity") or {})
        required_identity = {
            "campaign",
            "operator",
            "case_version",
            "result_version",
            "blocker_generation",
            "diagnostic_contract_revision",
            "diagnostic_capability_generation",
        }
        if any(not str(identity.get(field) or "").strip() for field in required_identity):
            raise AssistantTriggerError("steward escalation identity is incomplete")
        operator = str(identity["operator"])
        workspace = Path("operators_workspace") / operator
        if not bounded_path(self.root, workspace).is_dir():
            raise AssistantTriggerError(f"workspace is missing: {workspace.as_posix()}")
        runbook = bounded_path(self.root, self.steward_runbook_path)
        if not runbook.is_file():
            raise AssistantTriggerError(
                f"steward runbook is missing: {self.steward_runbook_path.as_posix()}"
            )
        evidence = descriptor.get("evidence")
        if not isinstance(evidence, list) or not evidence:
            raise AssistantTriggerError("steward escalation evidence must not be empty")
        normalized_evidence: list[str] = []
        for value in evidence:
            path = Path(str(value or ""))
            if not str(path):
                raise AssistantTriggerError("steward escalation evidence is invalid")
            bounded_path(self.root, path)
            normalized_evidence.append(path.as_posix())
        canonical = json.dumps(
            descriptor,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        candidate_digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        rule = {
            "schema": "ascendop.metric-trigger-rule.v1",
            "rule_id": f"solver-evidence-exhausted:{operator}",
            "when": {"steward_escalation_ready": True},
            "action": "resolve_workflow_capability_gap",
            "assistant_target_id": self.steward_target_id,
            "runbook_path": self.steward_runbook_path.as_posix(),
        }
        return self.database.create_assistant_action_if_triggered(
            rule,
            {"steward_escalation_ready": True},
            operator=operator,
            candidate_digest=candidate_digest,
            workspace=workspace.as_posix(),
            runbook_path=self.steward_runbook_path.as_posix(),
            evidence=normalized_evidence,
            parameters={
                "state": SOLVER_STEWARD_ESCALATION_STATE,
                "gate_stage": str(escalation.get("gate_stage") or ""),
                "wakeups": str(escalation.get("wakeups") or ""),
                "next_command": str(escalation.get("next_command") or ""),
                "escalation": descriptor,
            },
        )


def read_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON document must be an object: {path}")
    return value


def bounded_path(root: Path, value: Path) -> Path:
    resolved = value.resolve() if value.is_absolute() else (root / value).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"Assistant trigger path escapes workspace: {value}") from exc
    return resolved


def required_text(value: Mapping[str, Any], field: str) -> str:
    text = value.get(field)
    if not isinstance(text, str) or not text.strip():
        raise AssistantTriggerError(f"context {field} must be non-empty text")
    return text.strip()
