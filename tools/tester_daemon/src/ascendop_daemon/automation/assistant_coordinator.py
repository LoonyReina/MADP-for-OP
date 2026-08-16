from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from ascendop_protocol.automation import (
    AutomationContractError,
    validate_trigger_source,
    validate_trigger_source_registry,
)

from ascendop_daemon.automation.assistant_trigger import (
    AssistantTriggerEngine,
    AssistantTriggerError,
    load_trigger_rules,
)
from ascendop_daemon.control_plane.control_database import ControlDatabase


DEFAULT_TRIGGER_REGISTRY = Path(
    "Develop/automation/assistant_trigger_sources.json"
)


class AssistantCoordinator:
    def __init__(
        self,
        root: Path,
        database: ControlDatabase,
        *,
        registry_path: Path = DEFAULT_TRIGGER_REGISTRY,
    ) -> None:
        self.root = root.resolve()
        self.database = database
        self.registry_path = bounded_path(self.root, registry_path)

    def run_once(self) -> dict[str, Any]:
        if not self.registry_path.is_file():
            return {"source_count": 0, "actions": [], "errors": []}
        try:
            registry = validate_trigger_source_registry(
                read_object(self.registry_path)
            )
        except (OSError, ValueError, AutomationContractError) as exc:
            return {
                "source_count": 0,
                "actions": [],
                "errors": [f"registry: {exc}"],
            }
        actions: list[dict[str, Any]] = []
        errors: list[str] = []
        enabled = [item for item in registry["sources"] if item.get("enabled")]
        for registration in enabled:
            source_id = str(registration["source_id"])
            try:
                actions.extend(self._evaluate_source(registration))
            except (OSError, ValueError, AutomationContractError, AssistantTriggerError) as exc:
                errors.append(f"{source_id}: {exc}")
        return {
            "source_count": len(enabled),
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
