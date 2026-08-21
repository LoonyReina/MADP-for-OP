from __future__ import annotations

import hashlib
import json
from importlib.resources import files
from typing import Any

from ascendop_protocol.evidence.registry import (
    EVIDENCE_OPERATION_CODES,
    evidence_operation_definition as _evidence_operation_definition,
    evidence_operation_registry,
    evidence_operation_registry_digest,
)


FLOW_V5_CATALOG_SCHEMA = "ascendop.flow-v5-catalog.v1"


def flow_v5_catalog() -> dict[str, Any]:
    resource = files("ascendop_protocol.schemas").joinpath(
        "examples/flow_v5_catalog.json"
    )
    catalog = json.loads(resource.read_text(encoding="utf-8"))
    if catalog.get("schema") != FLOW_V5_CATALOG_SCHEMA:
        raise ValueError("Flow V5 catalog schema identity is invalid")
    registry = evidence_operation_registry()
    if (
        catalog.get("evidence_operation_registry_generation")
        != registry["generation"]
    ):
        raise ValueError("Flow V5 catalog evidence registry generation is stale")
    if (
        catalog.get("evidence_operation_registry_digest")
        != evidence_operation_registry_digest()
    ):
        raise ValueError("Flow V5 catalog evidence registry digest is stale")
    summaries = catalog.get("evidence_operations")
    if not isinstance(summaries, list) or {
        row.get("operation_code")
        for row in summaries
        if isinstance(row, dict)
    } != set(EVIDENCE_OPERATION_CODES):
        raise ValueError("Flow V5 catalog evidence operation summary is stale")
    return catalog


def flow_v5_catalog_digest() -> str:
    payload = json.dumps(
        flow_v5_catalog(), sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _catalog_values(field: str, key: str | None = None) -> tuple[str, ...]:
    rows = flow_v5_catalog().get(field)
    if not isinstance(rows, list):
        raise ValueError(f"Flow V5 catalog field is invalid: {field}")
    if key is None:
        values = rows
    else:
        values = [row.get(key) for row in rows if isinstance(row, dict)]
    if not values or any(not isinstance(value, str) or not value for value in values):
        raise ValueError(f"Flow V5 catalog values are invalid: {field}")
    if len(values) != len(set(values)):
        raise ValueError(f"Flow V5 catalog values are duplicated: {field}")
    return tuple(values)


FLOW_V5_ROLES = frozenset(_catalog_values("roles"))
FLOW_V5_ACTION_KINDS = frozenset(
    _catalog_values("action_catalog", "action_kind")
)
FLOW_V5_EVIDENCE_OPERATIONS = EVIDENCE_OPERATION_CODES
FLOW_V5_OUTCOMES = frozenset(_catalog_values("outcomes"))
FLOW_V5_BLOCKER_KINDS = frozenset(_catalog_values("blocker_kinds"))
FLOW_V5_NATIVE_TERMINAL_STATUSES = frozenset(
    _catalog_values("native_terminal_statuses")
)
FLOW_V5_EXECUTION_STATUSES = frozenset(_catalog_values("execution_statuses"))
FLOW_V5_FAILURE_CLASSES = frozenset(_catalog_values("failure_classes"))
FLOW_V5_OUTPUT_KINDS = frozenset(_catalog_values("output_kinds"))


def action_definition(action_kind: str) -> dict[str, str]:
    matches = [
        row
        for row in flow_v5_catalog()["action_catalog"]
        if row["action_kind"] == action_kind
    ]
    if len(matches) != 1:
        raise KeyError(f"unknown or duplicate Flow V5 action kind: {action_kind}")
    return dict(matches[0])


def evidence_operation_definition(operation_code: str) -> dict[str, Any]:
    return _evidence_operation_definition(operation_code)


__all__ = [
    "FLOW_V5_ACTION_KINDS",
    "FLOW_V5_BLOCKER_KINDS",
    "FLOW_V5_CATALOG_SCHEMA",
    "FLOW_V5_EVIDENCE_OPERATIONS",
    "FLOW_V5_EXECUTION_STATUSES",
    "FLOW_V5_FAILURE_CLASSES",
    "FLOW_V5_NATIVE_TERMINAL_STATUSES",
    "FLOW_V5_OUTCOMES",
    "FLOW_V5_OUTPUT_KINDS",
    "FLOW_V5_ROLES",
    "action_definition",
    "evidence_operation_definition",
    "flow_v5_catalog",
    "flow_v5_catalog_digest",
]
