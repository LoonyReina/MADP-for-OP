from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from ascendop_protocol.actor import (
    flow_v5_catalog,
    validate_agent_action_outcome,
    validate_actor_action_envelope,
    validate_role_binding,
)
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
from ascendop_daemon.automation.manager_notifications import (
    ManagerNotificationPublisher,
)
from ascendop_daemon.control_plane.control_database import ControlDatabase
from ascendop_daemon.core.atomic_io import write_json_atomic
from ascendop_daemon.storage.control_types import ControlDatabaseError


DEFAULT_TRIGGER_REGISTRY = Path(
    "Develop/automation/assistant_trigger_sources.json"
)
DEFAULT_STEWARD_RUNBOOK = Path(
    "docs/flow_v5/DEVELOPER_CAPABILITY_RUNBOOK.md"
)


class AssistantCoordinator:
    def __init__(
        self,
        root: Path,
        database: ControlDatabase,
        *,
        registry_path: Path = DEFAULT_TRIGGER_REGISTRY,
        steward_target_id: str = "",
        steward_runbook_path: Path = DEFAULT_STEWARD_RUNBOOK,
        developer_role_binding: Mapping[str, Any] | None = None,
        manager_target_id: str = "",
        manager_role_binding: Mapping[str, Any] | None = None,
    ) -> None:
        self.root = root.resolve()
        self.database = database
        self.registry_path = bounded_path(self.root, registry_path)
        self.steward_target_id = steward_target_id.strip()
        self.steward_runbook_path = steward_runbook_path
        self.developer_role_binding = (
            validate_role_binding(dict(developer_role_binding))
            if developer_role_binding is not None
            else None
        )
        self.manager_notifications = (
            ManagerNotificationPublisher(
                self.root,
                self.database,
                target_id=manager_target_id,
                role_binding=manager_role_binding,
            )
            if manager_target_id and manager_role_binding is not None
            else None
        )

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
        protocol_gap_count = 0
        try:
            protocol_gaps = self._agent_protocol_gaps()
        except (ControlDatabaseError, ValueError) as exc:
            protocol_gaps = []
            errors.append(f"agent-protocol-gaps: {exc}")
        for action_record, receipt, outcome in protocol_gaps:
            protocol_gap_count += 1
            try:
                actions.append(
                    self._evaluate_agent_protocol_gap(
                        action_record,
                        receipt,
                        outcome,
                    )
                )
            except (
                OSError,
                ValueError,
                AutomationContractError,
                AssistantTriggerError,
                ControlDatabaseError,
            ) as exc:
                action = dict(action_record.get("action") or {})
                errors.append(
                    f"agent-gap:{action.get('action_id') or 'unknown'}: {exc}"
                )
        manager_cycle = self.run_manager_notifications_once()
        actions.extend(manager_cycle["actions"])
        errors.extend(manager_cycle["errors"])
        return {
            "source_count": len(enabled),
            "steward_escalation_count": escalation_count,
            "protocol_gap_count": protocol_gap_count,
            "manager_notification_count": manager_cycle[
                "manager_notification_count"
            ],
            "actions": actions,
            "errors": errors,
        }

    def run_manager_notifications_once(self) -> dict[str, Any]:
        actions: list[dict[str, Any]] = []
        errors: list[str] = []
        if self.manager_notifications is not None:
            try:
                actions = self.manager_notifications.run_once()
            except (OSError, ValueError, ControlDatabaseError) as exc:
                errors.append(f"manager-notifications: {exc}")
        return {
            "manager_notification_count": len(actions),
            "actions": actions,
            "errors": errors,
        }

    def _agent_protocol_gaps(
        self,
    ) -> list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]]:
        initialize = getattr(self.database, "initialize", None)
        if callable(initialize):
            initialize()
        gaps: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]] = []
        for action_record in self.database.agent_actions_v4():
            if str(action_record.get("state") or "") != "completed":
                continue
            action = dict(action_record.get("action") or {})
            action_id = str(action.get("action_id") or "")
            if not action_id:
                continue
            receipt = self.database.agent_action_receipt(action_id)
            if not isinstance(receipt, Mapping):
                continue
            completion = receipt.get("completion")
            if not isinstance(completion, Mapping):
                continue
            raw_outcome = completion.get("agent_action_outcome")
            if not isinstance(raw_outcome, Mapping):
                continue
            outcome = validate_agent_action_outcome(raw_outcome)
            if outcome.get("disposition") == "protocol_gap":
                gaps.append((dict(action_record), dict(receipt), outcome))
        return gaps

    def _evaluate_agent_protocol_gap(
        self,
        action_record: Mapping[str, Any],
        receipt: Mapping[str, Any],
        outcome: Mapping[str, Any],
    ) -> dict[str, Any]:
        if not self.steward_target_id:
            raise AssistantTriggerError("Developer target is not configured")
        binding = self.developer_role_binding
        if binding is None or binding["role"] != "developer":
            raise AssistantTriggerError("Developer role binding is not configured")
        if binding["native_session_id"] != self.steward_target_id:
            raise AssistantTriggerError(
                "Developer target does not match its role binding session"
            )
        action = dict(action_record.get("action") or {})
        blocker = dict(outcome.get("blocker") or {})
        operator_id = required_text(action, "operator_id")
        action_id = required_text(action, "action_id")
        iteration_id = required_text(action, "iteration_id")
        workspace = bounded_path(
            self.root,
            Path(required_text(action, "origin_workspace")),
        )
        if not workspace.is_dir():
            raise AssistantTriggerError(f"workspace is missing: {workspace}")
        runbook = bounded_path(self.root, self.steward_runbook_path)
        if not runbook.is_file():
            raise AssistantTriggerError(
                f"steward runbook is missing: {self.steward_runbook_path.as_posix()}"
            )
        identity = {
            "action_id": action_id,
            "iteration_id": iteration_id,
            "operator_id": operator_id,
            "role": required_text(action, "role"),
            "capability_code": required_text(blocker, "code"),
            "details": required_text(blocker, "details"),
            "resume_condition": required_text(blocker, "resume_condition"),
        }
        canonical = json.dumps(
            identity,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        candidate_digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        gap_id = f"gap-{candidate_digest[:24]}"
        developer_action_id = f"developer-{candidate_digest[:24]}"
        existing = self.database.assistant_action(developer_action_id)
        if existing is not None:
            return existing
        gap_descriptor = (
            self.root
            / ".ascendop-work"
            / "runtime"
            / "capability-gaps"
            / f"{gap_id}.json"
        )
        gap_document = {
            "schema": "ascendop.capability-gap-evidence.v1",
            "capability_gap_id": gap_id,
            "identity": identity,
            "action": action,
            "receipt": dict(receipt),
            "outcome": dict(outcome),
        }
        if gap_descriptor.is_file():
            if read_object(gap_descriptor) != gap_document:
                raise AssistantTriggerError(
                    f"capability gap evidence collision: {gap_descriptor}"
                )
        else:
            write_json_atomic(
                gap_descriptor,
                gap_document,
                ensure_ascii=True,
                sort_keys=True,
            )
        evidence_refs = [
            str(value)
            for value in outcome.get("evidence_refs", [])
            if str(value)
        ]
        evidence_refs.append(gap_descriptor.relative_to(self.root).as_posix())
        now = datetime.now(timezone.utc)
        created_at = now.isoformat(timespec="milliseconds").replace("+00:00", "Z")
        expires_at = (now + timedelta(hours=2)).isoformat(
            timespec="milliseconds"
        ).replace("+00:00", "Z")
        candidate_identity = dict(action.get("candidate_identity") or {})
        envelope = validate_actor_action_envelope(
            {
                "schema": "ascendop.actor-action-envelope.v1",
                "action_id": developer_action_id,
                "idempotency_key": (
                    f"developer.repair-capability:{candidate_digest}"
                ),
                "action_kind": "developer.repair-capability",
                "effective_role": "developer",
                "principal_id": binding["principal_id"],
                "role_binding_id": binding["role_binding_id"],
                "producer_generation": flow_v5_catalog()["generation"],
                "scope": dict(binding["scope"]),
                "lease": {
                    "lease_id": f"lease-{gap_id}",
                    "generation": binding["generation"],
                    "expires_at": expires_at,
                },
                "causation": {
                    "trace_id": gap_id,
                    "correlation_id": action_id,
                    "parent_action_id": action_id,
                    "candidate_id": str(action.get("candidate_version") or "") or None,
                    "promotion_receipt_id": None,
                    "request_id": None,
                    "attempt_id": None,
                },
                "payload": {
                    "capability_gap_id": gap_id,
                    "capability_code": blocker["code"],
                    "runbook_path": self.steward_runbook_path.as_posix(),
                    "resume_condition": blocker["resume_condition"],
                    "gap_context": {
                        "source_kind": "agent_protocol_gap",
                        "operator_id": operator_id,
                        "identity": identity,
                        "evidence_refs": sorted(set(evidence_refs)),
                        "gate_stage": str(
                            candidate_identity.get("gate_stage") or ""
                        ),
                        "next_command": str(
                            candidate_identity.get("next_command") or ""
                        ),
                    },
                },
                "created_at": created_at,
            }
        )
        return self.database.create_actor_action_if_absent(
            envelope,
            target_id=self.steward_target_id,
            context={
                "state": "agent-protocol-gap",
                "action_id": action_id,
                "receipt": dict(receipt),
            },
        )

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
            raise AssistantTriggerError("Developer target is not configured")
        binding = self.developer_role_binding
        if binding is None:
            raise AssistantTriggerError("Developer role binding is not configured")
        if binding["role"] != "developer":
            raise AssistantTriggerError("capability gap target is not a Developer binding")
        if binding["native_session_id"] != self.steward_target_id:
            raise AssistantTriggerError(
                "Developer target does not match its role binding session"
            )
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
        action_id = f"developer-{candidate_digest[:24]}"
        existing = self.database.assistant_action(action_id)
        if existing is not None:
            return existing
        now = datetime.now(timezone.utc)
        created_at = now.isoformat(timespec="milliseconds").replace("+00:00", "Z")
        expires_at = (now + timedelta(hours=2)).isoformat(
            timespec="milliseconds"
        ).replace("+00:00", "Z")
        gap_id = f"gap-{candidate_digest[:24]}"
        action = validate_actor_action_envelope(
            {
                "schema": "ascendop.actor-action-envelope.v1",
                "action_id": action_id,
                "idempotency_key": (
                    f"developer.repair-capability:{candidate_digest}"
                ),
                "action_kind": "developer.repair-capability",
                "effective_role": "developer",
                "principal_id": binding["principal_id"],
                "role_binding_id": binding["role_binding_id"],
                "producer_generation": flow_v5_catalog()["generation"],
                "scope": dict(binding["scope"]),
                "lease": {
                    "lease_id": f"lease-{gap_id}",
                    "generation": binding["generation"],
                    "expires_at": expires_at,
                },
                "causation": {
                    "trace_id": gap_id,
                    "correlation_id": str(identity["blocker_generation"]),
                    "parent_action_id": None,
                    "candidate_id": None,
                    "promotion_receipt_id": None,
                    "request_id": None,
                    "attempt_id": None,
                },
                "payload": {
                    "capability_gap_id": gap_id,
                    "capability_code": "workflow.missing-reusable-capability",
                    "runbook_path": self.steward_runbook_path.as_posix(),
                    "resume_condition": (
                        "exact escalation identity is absent from the live board"
                    ),
                    "gap_context": {
                        "source_kind": "solver_steward_escalation",
                        "operator_id": operator,
                        "identity": {
                            key: str(value) for key, value in identity.items()
                        },
                        "evidence_refs": normalized_evidence,
                        "gate_stage": str(escalation.get("gate_stage") or ""),
                        "next_command": str(escalation.get("next_command") or ""),
                    },
                },
                "created_at": created_at,
            },
        )
        return self.database.create_actor_action_if_absent(
            action,
            target_id=self.steward_target_id,
            context={
                "state": SOLVER_STEWARD_ESCALATION_STATE,
                "wakeups": str(escalation.get("wakeups") or ""),
                "descriptor": descriptor,
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
