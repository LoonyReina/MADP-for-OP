from __future__ import annotations

from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any, Mapping


OFFICIAL_PROBLEM_SNAPSHOT_SCHEMA = "ascendop.official-problem-snapshot.v1"
STANDING_SUBMISSION_POLICY_SCHEMA = "ascendop.standing-submission-policy.v1"
OFFICIAL_SUBMISSION_RECEIPT_SCHEMA = "ascendop.official-submission-receipt.v1"


class CompetitionContractError(ValueError):
    pass


def validate_official_problem_snapshot(raw: Mapping[str, Any]) -> dict[str, Any]:
    _schema(raw, OFFICIAL_PROBLEM_SNAPSHOT_SCHEMA, "official problem snapshot")
    for field in (
        "snapshot_id",
        "campaign_id",
        "operator_id",
        "display_name",
        "source_url",
        "submit_url",
        "ranking_url",
        "captured_at",
    ):
        _text(raw.get(field), field)
    _timestamp(raw.get("captured_at"), "captured_at")
    environment = _object(raw.get("environment"), "environment")
    _text_list(environment.get("soc"), "environment.soc")
    _text_list(environment.get("cann"), "environment.cann")
    problem = _object(raw.get("problem"), "problem")
    _text(problem.get("track"), "problem.track")
    _text(problem.get("summary"), "problem.summary")
    constraints = problem.get("constraints")
    if not isinstance(constraints, list):
        raise CompetitionContractError("problem.constraints must be a list")
    project = _object(raw.get("project"), "project")
    _sha256(project.get("digest"), "project.digest")
    _text(project.get("format"), "project.format")
    files = project.get("files")
    if not isinstance(files, list) or not files:
        raise CompetitionContractError("project.files must be a non-empty list")
    seen: set[str] = set()
    for index, item in enumerate(files):
        value = _object(item, f"project.files[{index}]")
        path = _relative_path(value.get("path"), f"project.files[{index}].path")
        if path in seen:
            raise CompetitionContractError(f"duplicate project file: {path}")
        seen.add(path)
        _sha256(value.get("sha256"), f"project.files[{index}].sha256")
        size = value.get("size")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise CompetitionContractError(
                f"project.files[{index}].size must be a non-negative integer"
            )
    page_digests = _object(raw.get("page_digests"), "page_digests")
    if not page_digests:
        raise CompetitionContractError("page_digests must not be empty")
    for name, digest in page_digests.items():
        _text(name, "page_digests key")
        _sha256(digest, f"page_digests.{name}")
    return dict(raw)


def validate_standing_submission_policy(raw: Mapping[str, Any]) -> dict[str, Any]:
    _schema(raw, STANDING_SUBMISSION_POLICY_SCHEMA, "standing submission policy")
    for field in (
        "policy_id",
        "campaign_id",
        "authorized_by",
        "issued_at",
        "expires_at",
        "rules_generation",
    ):
        _text(raw.get(field), field)
    issued_at = _timestamp(raw.get("issued_at"), "issued_at")
    expires_at = _timestamp(raw.get("expires_at"), "expires_at")
    if expires_at <= issued_at:
        raise CompetitionContractError("expires_at must be later than issued_at")
    enabled = raw.get("enabled")
    if not isinstance(enabled, bool):
        raise CompetitionContractError("enabled must be boolean")
    _text_list(raw.get("operator_ids"), "operator_ids")
    limits = _object(raw.get("limits"), "limits")
    for field in (
        "max_submissions_per_operator_per_day",
        "max_pending_per_operator",
    ):
        value = limits.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise CompetitionContractError(f"limits.{field} must be a positive integer")
    for field in ("one_submission_per_digest", "require_terminal_before_next"):
        if not isinstance(limits.get(field), bool):
            raise CompetitionContractError(f"limits.{field} must be boolean")
    gate = _object(raw.get("gate"), "gate")
    required_true = gate.get("required_true")
    if not isinstance(required_true, list) or not required_true:
        raise CompetitionContractError("gate.required_true must be a non-empty list")
    _text_list(required_true, "gate.required_true")
    return dict(raw)


def validate_official_submission_receipt(raw: Mapping[str, Any]) -> dict[str, Any]:
    _schema(raw, OFFICIAL_SUBMISSION_RECEIPT_SCHEMA, "official submission receipt")
    for field in (
        "receipt_id",
        "checkpoint_id",
        "checkpoint_digest",
        "campaign_id",
        "operator_id",
        "project_digest",
        "submission_id",
        "submission_url",
        "submitted_at",
        "state",
    ):
        _text(raw.get(field), field)
    _sha256(raw.get("checkpoint_digest"), "checkpoint_digest")
    _sha256(raw.get("project_digest"), "project_digest")
    _timestamp(raw.get("submitted_at"), "submitted_at")
    if raw.get("state") not in {"submitted", "pending", "terminal", "uncertain"}:
        raise CompetitionContractError("unsupported receipt state")
    files = raw.get("source_file_digests", {})
    if not isinstance(files, Mapping):
        raise CompetitionContractError("source_file_digests must be an object")
    for path, digest in files.items():
        _relative_path(path, "source_file_digests key")
        _sha256(digest, f"source_file_digests.{path}")
    return dict(raw)


def _schema(raw: Mapping[str, Any], expected: str, label: str) -> None:
    if not isinstance(raw, Mapping) or raw.get("schema") != expected:
        raise CompetitionContractError(f"unsupported {label}")


def _object(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise CompetitionContractError(f"{field} must be an object")
    return value


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CompetitionContractError(f"{field} must be non-empty text")
    return value.strip()


def _text_list(value: Any, field: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise CompetitionContractError(f"{field} must be a non-empty list")
    return tuple(_text(item, field) for item in value)


def _relative_path(value: Any, field: str) -> str:
    text = _text(value, field).replace("\\", "/")
    path = PurePosixPath(text)
    if path.is_absolute() or ".." in path.parts:
        raise CompetitionContractError(f"{field} must be a bounded relative path")
    return str(path)


def _sha256(value: Any, field: str) -> str:
    text = _text(value, field).lower()
    if len(text) != 64 or any(char not in "0123456789abcdef" for char in text):
        raise CompetitionContractError(f"{field} must be a SHA-256 digest")
    return text


def _timestamp(value: Any, field: str) -> datetime:
    text = _text(value, field)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CompetitionContractError(f"{field} must be RFC3339") from exc
    if parsed.tzinfo is None:
        raise CompetitionContractError(f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc)
