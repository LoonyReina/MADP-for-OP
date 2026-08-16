from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Mapping

from ascendop_protocol.automation import validate_trigger_rule

from ascendop_daemon.control_plane.control_database import ControlDatabase


class AssistantTriggerError(RuntimeError):
    pass


class AssistantTriggerEngine:
    def __init__(self, database: ControlDatabase) -> None:
        self.database = database

    def evaluate(
        self,
        rules: Iterable[Mapping[str, Any]],
        metrics: Mapping[str, Any],
        *,
        operator: str,
        candidate_digest: str,
        workspace: str,
        evidence: Iterable[str] = (),
    ) -> list[dict[str, Any]]:
        actions: list[dict[str, Any]] = []
        for raw in rules:
            rule = validate_trigger_rule(raw)
            action = self.database.create_assistant_action_if_triggered(
                rule,
                dict(metrics),
                operator=operator,
                candidate_digest=candidate_digest,
                workspace=workspace,
                runbook_path=str(rule["runbook_path"]),
                evidence=[str(item) for item in evidence],
            )
            if action is not None:
                actions.append(action)
        return actions


def load_trigger_rules(path: Path) -> tuple[dict[str, Any], ...]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AssistantTriggerError(f"cannot read Assistant trigger rules {path}: {exc}") from exc
    if not isinstance(value, dict) or value.get("schema") != "ascendop.metric-trigger-rules.v1":
        raise AssistantTriggerError("unsupported Assistant trigger rule document")
    rules = value.get("rules")
    if not isinstance(rules, list):
        raise AssistantTriggerError("Assistant trigger rules must be a list")
    return tuple(validate_trigger_rule(item) for item in rules)
