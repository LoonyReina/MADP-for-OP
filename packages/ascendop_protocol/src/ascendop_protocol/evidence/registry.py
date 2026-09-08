from __future__ import annotations

import hashlib
import json
from importlib.resources import files
from typing import Any


EVIDENCE_OPERATION_REGISTRY_SCHEMA = "ascendop.evidence-operation-registry.v1"
_CURRENT_REGISTRY_RESOURCE = "examples/evidence_operation_registry.json"
_HISTORICAL_REGISTRY_RESOURCES = (
    "examples/evidence_operation_registry.v1.json",
)


def _load_registry(resource_name: str) -> dict[str, Any]:
    resource = files("ascendop_protocol.schemas").joinpath(resource_name)
    registry = json.loads(resource.read_text(encoding="utf-8"))
    if registry.get("schema") != EVIDENCE_OPERATION_REGISTRY_SCHEMA:
        raise ValueError("evidence-operation registry schema identity is invalid")
    generation = registry.get("generation")
    if not isinstance(generation, str) or not generation:
        raise ValueError("evidence-operation registry generation is invalid")
    operations = registry.get("operations")
    if not isinstance(operations, list) or not operations:
        raise ValueError("evidence-operation registry has no operations")
    codes = [
        row.get("operation_code") for row in operations if isinstance(row, dict)
    ]
    if len(codes) != len(operations) or any(
        not isinstance(code, str) or not code for code in codes
    ):
        raise ValueError("evidence-operation registry code is invalid")
    if len(codes) != len(set(codes)):
        raise ValueError("evidence-operation registry codes are duplicated")
    return registry


def _registry_digest(registry: dict[str, Any]) -> str:
    payload = json.dumps(
        registry,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def evidence_operation_registry() -> dict[str, Any]:
    return _load_registry(_CURRENT_REGISTRY_RESOURCE)


def evidence_operation_registry_digest() -> str:
    return _registry_digest(evidence_operation_registry())


def evidence_operation_registry_for_identity(
    generation: str, digest: str
) -> dict[str, Any]:
    for resource_name in (
        _CURRENT_REGISTRY_RESOURCE,
        *_HISTORICAL_REGISTRY_RESOURCES,
    ):
        registry = _load_registry(resource_name)
        if (
            registry["generation"] == generation
            and _registry_digest(registry) == digest
        ):
            return registry
    raise KeyError(f"unknown evidence-operation registry identity: {generation}/{digest}")


def evidence_operation_definition(
    operation_code: str, *, registry: dict[str, Any] | None = None
) -> dict[str, Any]:
    matches = [
        row
        for row in (registry or evidence_operation_registry())["operations"]
        if row["operation_code"] == operation_code
    ]
    if len(matches) != 1:
        raise KeyError(
            f"unknown or duplicate Flow V5 evidence operation: {operation_code}"
        )
    return dict(matches[0])


EVIDENCE_OPERATION_CODES = frozenset(
    row["operation_code"] for row in evidence_operation_registry()["operations"]
)


__all__ = [
    "EVIDENCE_OPERATION_CODES",
    "EVIDENCE_OPERATION_REGISTRY_SCHEMA",
    "evidence_operation_definition",
    "evidence_operation_registry",
    "evidence_operation_registry_digest",
    "evidence_operation_registry_for_identity",
]
