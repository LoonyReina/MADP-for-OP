from __future__ import annotations

from typing import Any, Mapping


TRIGGER_RULE_SCHEMA = "ascendop.metric-trigger-rule.v1"
ASSISTANT_ACTION_REQUEST_SCHEMA = "ascendop.assistant-action-request.v1"
ASSISTANT_ACTION_RECEIPT_SCHEMA = "ascendop.assistant-action-receipt.v1"
TRIGGER_SOURCE_SCHEMA = "ascendop.assistant-trigger-source.v1"
TRIGGER_SOURCE_REGISTRY_SCHEMA = "ascendop.assistant-trigger-registry.v1"
COMPARATORS = {">=", ">", "<=", "<", "==", "!=", "in"}


class AutomationContractError(ValueError):
    pass


def validate_trigger_rule(raw: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(raw, Mapping) or raw.get("schema") != TRIGGER_RULE_SCHEMA:
        raise AutomationContractError("unsupported metric trigger rule")
    _text(raw.get("rule_id"), "rule_id")
    when = _object(raw.get("when"), "when")
    if not when:
        raise AutomationContractError("when must not be empty")
    for metric, predicate in when.items():
        _text(metric, "when metric")
        _validate_predicate(predicate, f"when.{metric}")
    _text(raw.get("action"), "action")
    _text(raw.get("assistant_target_id"), "assistant_target_id")
    _relative_path(raw.get("runbook_path"), "runbook_path")
    return dict(raw)


def evaluate_trigger_rule(raw: Mapping[str, Any], metrics: Mapping[str, Any]) -> bool:
    rule = validate_trigger_rule(raw)
    when = _object(rule["when"], "when")
    for metric, predicate in when.items():
        if metric not in metrics or not _evaluate_predicate(metrics[metric], predicate):
            return False
    return True


def validate_action_request(raw: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(raw, Mapping) or raw.get("schema") != ASSISTANT_ACTION_REQUEST_SCHEMA:
        raise AutomationContractError("unsupported Assistant action request")
    for field in (
        "action_id",
        "rule_id",
        "assistant_target_id",
        "action",
        "operator",
        "candidate_digest",
        "created_at",
    ):
        _text(raw.get(field), field)
    _relative_path(raw.get("workspace"), "workspace")
    _relative_path(raw.get("runbook_path"), "runbook_path")
    evidence = raw.get("evidence")
    if not isinstance(evidence, list):
        raise AutomationContractError("evidence must be a list")
    parameters = raw.get("parameters", {})
    if not isinstance(parameters, Mapping):
        raise AutomationContractError("parameters must be an object")
    return dict(raw)


def validate_action_receipt(raw: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(raw, Mapping) or raw.get("schema") != ASSISTANT_ACTION_RECEIPT_SCHEMA:
        raise AutomationContractError("unsupported Assistant action receipt")
    for field in ("action_id", "status", "completed_at"):
        _text(raw.get(field), field)
    if raw["status"] not in {"completed", "failed", "rejected", "cancelled"}:
        raise AutomationContractError(f"unsupported receipt status: {raw['status']}")
    return dict(raw)


def validate_trigger_source_registry(raw: Mapping[str, Any]) -> dict[str, Any]:
    if (
        not isinstance(raw, Mapping)
        or raw.get("schema") != TRIGGER_SOURCE_REGISTRY_SCHEMA
    ):
        raise AutomationContractError("unsupported Assistant trigger registry")
    sources = raw.get("sources")
    if not isinstance(sources, list):
        raise AutomationContractError("trigger registry sources must be a list")
    identities: set[str] = set()
    for item in sources:
        value = _object(item, "source registration")
        source_id = _text(value.get("source_id"), "source_id")
        if source_id in identities:
            raise AutomationContractError(f"duplicate trigger source: {source_id}")
        identities.add(source_id)
        _relative_path(value.get("manifest_path"), "manifest_path")
        if "enabled" in value and not isinstance(value["enabled"], bool):
            raise AutomationContractError("source enabled must be boolean")
    return dict(raw)


def validate_trigger_source(raw: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(raw, Mapping) or raw.get("schema") != TRIGGER_SOURCE_SCHEMA:
        raise AutomationContractError("unsupported Assistant trigger source")
    _text(raw.get("source_id"), "source_id")
    _text(raw.get("operator"), "operator")
    for field in ("workspace", "rules_path", "metrics_path", "context_path"):
        _relative_path(raw.get(field), field)
    if "enabled" in raw and not isinstance(raw["enabled"], bool):
        raise AutomationContractError("trigger source enabled must be boolean")
    return dict(raw)


def _validate_predicate(value: Any, field: str) -> None:
    if isinstance(value, Mapping):
        if len(value) != 1:
            raise AutomationContractError(f"{field} must contain one comparator")
        comparator = next(iter(value))
        if comparator not in COMPARATORS:
            raise AutomationContractError(f"unsupported comparator: {comparator}")
        if comparator == "in" and not isinstance(value[comparator], list):
            raise AutomationContractError(f"{field}.in must be a list")
        return
    if not isinstance(value, (str, int, float, bool)) and value is not None:
        raise AutomationContractError(f"{field} has unsupported literal")


def _evaluate_predicate(actual: Any, predicate: Any) -> bool:
    if not isinstance(predicate, Mapping):
        return actual == predicate
    comparator, expected = next(iter(predicate.items()))
    if comparator == "==":
        return actual == expected
    if comparator == "!=":
        return actual != expected
    if comparator == "in":
        return actual in expected
    if isinstance(actual, bool) or isinstance(expected, bool):
        return False
    if not isinstance(actual, (int, float)) or not isinstance(expected, (int, float)):
        return False
    if comparator == ">=":
        return actual >= expected
    if comparator == ">":
        return actual > expected
    if comparator == "<=":
        return actual <= expected
    if comparator == "<":
        return actual < expected
    return False


def _object(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise AutomationContractError(f"{field} must be an object")
    return value


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise AutomationContractError(f"{field} must be non-empty text")
    return value.strip()


def _relative_path(value: Any, field: str) -> str:
    text = _text(value, field).replace("\\", "/")
    if text.startswith("/") or any(part == ".." for part in text.split("/")):
        raise AutomationContractError(f"{field} must be a bounded relative path")
    return text
