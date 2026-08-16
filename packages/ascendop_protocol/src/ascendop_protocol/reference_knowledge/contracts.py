from __future__ import annotations

from datetime import datetime
from pathlib import PurePosixPath
from typing import Any, Mapping


REFERENCE_SOURCE_MANIFEST_SCHEMA = "ascendop.reference-source-manifest.v1"
TECHNIQUE_CARD_SCHEMA = "ascendop.technique-card.v1"
RETRIEVAL_DECISION_SCHEMA = "ascendop.retrieval-decision.v1"
TECHNIQUE_STATES = {
    "extracted",
    "hypothesis",
    "operator_validated",
    "cross_operator_validated",
    "promoted",
    "rejected",
}


class ReferenceKnowledgeContractError(ValueError):
    pass


def validate_reference_source_manifest(raw: Mapping[str, Any]) -> dict[str, Any]:
    _schema(raw, REFERENCE_SOURCE_MANIFEST_SCHEMA, "reference source manifest")
    for field in (
        "source_id",
        "title",
        "source_type",
        "url",
        "revision",
        "digest",
        "captured_at",
    ):
        _text(raw.get(field), field)
    _sha256(raw.get("digest"), "digest")
    _timestamp(raw.get("captured_at"), "captured_at")
    artifacts = raw.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        raise ReferenceKnowledgeContractError("artifacts must be a non-empty list")
    for index, item in enumerate(artifacts):
        value = _object(item, f"artifacts[{index}]")
        _relative_path(value.get("path"), f"artifacts[{index}].path")
        _sha256(value.get("sha256"), f"artifacts[{index}].sha256")
    _object(raw.get("applicability"), "applicability")
    return dict(raw)


def validate_technique_card(raw: Mapping[str, Any]) -> dict[str, Any]:
    _schema(raw, TECHNIQUE_CARD_SCHEMA, "technique card")
    for field in ("technique_id", "title", "state", "mechanism", "expected_signal", "falsifier"):
        _text(raw.get(field), field)
    if raw.get("state") not in TECHNIQUE_STATES:
        raise ReferenceKnowledgeContractError("unsupported technique state")
    _text_list(raw.get("source_ids"), "source_ids")
    applicability = _object(raw.get("applicability"), "applicability")
    for field in ("soc", "cann", "operator_families", "shape_regimes", "pipelines", "dtypes"):
        values = applicability.get(field, [])
        if not isinstance(values, list):
            raise ReferenceKnowledgeContractError(f"applicability.{field} must be a list")
        for item in values:
            _text(item, f"applicability.{field}")
    for field in ("risks", "evidence_paths", "tags", "source_locations"):
        values = raw.get(field, [])
        if not isinstance(values, list):
            raise ReferenceKnowledgeContractError(f"{field} must be a list")
        for item in values:
            _text(item, field)
    return dict(raw)


def validate_retrieval_decision(raw: Mapping[str, Any]) -> dict[str, Any]:
    _schema(raw, RETRIEVAL_DECISION_SCHEMA, "retrieval decision")
    for field in (
        "decision_id",
        "operator_id",
        "query",
        "created_at",
        "index_generation",
        "disposition",
        "rationale",
    ):
        _text(raw.get(field), field)
    _timestamp(raw.get("created_at"), "created_at")
    results = raw.get("results")
    if not isinstance(results, list):
        raise ReferenceKnowledgeContractError("results must be a list")
    previous_rank = 0
    for index, item in enumerate(results):
        value = _object(item, f"results[{index}]")
        _text(value.get("technique_id"), f"results[{index}].technique_id")
        rank = value.get("rank")
        if isinstance(rank, bool) or not isinstance(rank, int) or rank <= previous_rank:
            raise ReferenceKnowledgeContractError("result ranks must be strictly increasing")
        previous_rank = rank
    selected = raw.get("selected_technique_ids")
    if not isinstance(selected, list):
        raise ReferenceKnowledgeContractError("selected_technique_ids must be a list")
    for value in selected:
        _text(value, "selected_technique_ids")
    return dict(raw)


def _schema(raw: Mapping[str, Any], expected: str, label: str) -> None:
    if not isinstance(raw, Mapping) or raw.get("schema") != expected:
        raise ReferenceKnowledgeContractError(f"unsupported {label}")


def _object(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ReferenceKnowledgeContractError(f"{field} must be an object")
    return value


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ReferenceKnowledgeContractError(f"{field} must be non-empty text")
    return value.strip()


def _text_list(value: Any, field: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ReferenceKnowledgeContractError(f"{field} must be a non-empty list")
    return tuple(_text(item, field) for item in value)


def _relative_path(value: Any, field: str) -> str:
    text = _text(value, field).replace("\\", "/")
    path = PurePosixPath(text)
    if path.is_absolute() or ".." in path.parts:
        raise ReferenceKnowledgeContractError(f"{field} must be a bounded relative path")
    return str(path)


def _sha256(value: Any, field: str) -> str:
    text = _text(value, field).lower()
    if len(text) != 64 or any(char not in "0123456789abcdef" for char in text):
        raise ReferenceKnowledgeContractError(f"{field} must be a SHA-256 digest")
    return text


def _timestamp(value: Any, field: str) -> datetime:
    text = _text(value, field)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ReferenceKnowledgeContractError(f"{field} must be RFC3339") from exc
    if parsed.tzinfo is None:
        raise ReferenceKnowledgeContractError(f"{field} must include a timezone")
    return parsed
