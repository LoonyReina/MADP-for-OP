from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Mapping


INTERLEAVED_MEASUREMENT_SCHEMA = "ascendop.interleaved-measurement-plan.v1"
PAIR_KEY_FIELDS = (
    "session_id",
    "case_id",
    "block_id",
    "within_label_invocation_position",
)


class InterleavedMeasurementContractError(ValueError):
    pass


def normalize_interleaved_measurement_plan(raw: Mapping[str, Any]) -> dict[str, Any]:
    if raw.get("schema") != INTERLEAVED_MEASUREMENT_SCHEMA:
        raise InterleavedMeasurementContractError(
            f"unsupported interleaved measurement schema; expected {INTERLEAVED_MEASUREMENT_SCHEMA}"
        )
    if raw.get("artifact_binding") != "single-built-artifact":
        raise InterleavedMeasurementContractError(
            "interleaved measurement labels must share one built artifact"
        )
    artifact_identity = raw.get("artifact_identity")
    if not isinstance(artifact_identity, Mapping) or not artifact_identity:
        raise InterleavedMeasurementContractError(
            "interleaved measurement artifact_identity must be a non-empty object"
        )
    _json_safe(artifact_identity, "artifact_identity")
    labels = raw.get("labels")
    if (
        not isinstance(labels, list)
        or len(labels) != 2
        or any(not isinstance(item, str) or not _safe_token(item) for item in labels)
        or len(set(labels)) != 2
    ):
        raise InterleavedMeasurementContractError(
            "interleaved measurement labels must contain two distinct safe tokens"
        )
    samples_per_label = _positive_int(
        raw.get("samples_per_label_per_case"), "samples_per_label_per_case"
    )
    middle_window = raw.get("middle_window")
    if not isinstance(middle_window, Mapping):
        raise InterleavedMeasurementContractError("middle_window must be an object")
    window_start = _nonnegative_int(middle_window.get("start"), "middle_window.start")
    window_end = _positive_int(middle_window.get("end"), "middle_window.end")
    if not window_start < window_end <= samples_per_label:
        raise InterleavedMeasurementContractError(
            "middle_window must be contained in each label's samples"
        )
    sessions = raw.get("sessions")
    if not isinstance(sessions, list) or len(sessions) < 2:
        raise InterleavedMeasurementContractError(
            "interleaved measurement requires at least two profiler sessions"
        )
    normalized_sessions: list[dict[str, Any]] = []
    session_ids: set[str] = set()
    starting_labels: set[str] = set()
    for index, session in enumerate(sessions):
        if not isinstance(session, Mapping):
            raise InterleavedMeasurementContractError(
                f"sessions[{index}] must be an object"
            )
        session_id = str(session.get("session_id") or "")
        if not _safe_token(session_id) or session_id in session_ids:
            raise InterleavedMeasurementContractError(
                "session_id values must be distinct safe tokens"
            )
        order = session.get("order")
        if (
            not isinstance(order, list)
            or not order
            or any(item not in labels for item in order)
        ):
            raise InterleavedMeasurementContractError(
                f"sessions[{index}].order must contain only declared labels"
            )
        counts = {label: order.count(label) for label in labels}
        if len(set(counts.values())) != 1 or next(iter(counts.values())) <= 0:
            raise InterleavedMeasurementContractError(
                "each session order must balance both labels"
            )
        block_count = _positive_int(
            session.get("block_count"), f"sessions[{index}].block_count"
        )
        invocations_per_epoch = _positive_int(
            session.get("invocations_per_epoch"),
            f"sessions[{index}].invocations_per_epoch",
        )
        observed_samples = (
            block_count * invocations_per_epoch * counts[str(labels[0])]
        )
        if observed_samples != samples_per_label:
            raise InterleavedMeasurementContractError(
                f"session {session_id} produces {observed_samples} samples per label; "
                f"expected {samples_per_label}"
            )
        session_ids.add(session_id)
        starting_labels.add(str(order[0]))
        normalized_sessions.append(
            {
                "session_id": session_id,
                "order": [str(item) for item in order],
                "block_count": block_count,
                "invocations_per_epoch": invocations_per_epoch,
            }
        )
    if starting_labels != set(labels):
        raise InterleavedMeasurementContractError(
            "profiler sessions must include both label starting orders"
        )
    pair_key_fields = raw.get("pair_key_fields")
    if list(pair_key_fields or []) != list(PAIR_KEY_FIELDS):
        raise InterleavedMeasurementContractError(
            "interleaved measurement pair_key_fields do not match the protocol"
        )
    normalized = {
        "schema": INTERLEAVED_MEASUREMENT_SCHEMA,
        "artifact_binding": "single-built-artifact",
        "artifact_identity": dict(artifact_identity),
        "labels": list(labels),
        "sessions": normalized_sessions,
        "samples_per_label_per_case": samples_per_label,
        "logical_samples_per_case": samples_per_label * len(labels),
        "middle_window": {"start": window_start, "end": window_end},
        "pair_key_fields": list(PAIR_KEY_FIELDS),
    }
    normalized["plan_digest"] = _digest(normalized)
    return normalized


def interleaved_session_schedule(
    plan: Mapping[str, Any], session_id: str, *, case_id: int
) -> list[dict[str, Any]]:
    normalized = normalize_interleaved_measurement_plan(plan)
    session = next(
        (
            item
            for item in normalized["sessions"]
            if item["session_id"] == session_id
        ),
        None,
    )
    if session is None:
        raise InterleavedMeasurementContractError(
            f"unknown interleaved measurement session: {session_id}"
        )
    if not isinstance(case_id, int) or isinstance(case_id, bool) or case_id <= 0:
        raise InterleavedMeasurementContractError("case_id must be a positive integer")
    label_positions = {label: 0 for label in normalized["labels"]}
    schedule: list[dict[str, Any]] = []
    for block_index in range(int(session["block_count"])):
        for epoch_index, label in enumerate(session["order"]):
            for invocation_index in range(int(session["invocations_per_epoch"])):
                within_label = label_positions[label]
                label_positions[label] += 1
                schedule.append(
                    {
                        "session_id": session_id,
                        "case_id": case_id,
                        "block_id": block_index + 1,
                        "epoch_index": epoch_index,
                        "invocation_in_epoch": invocation_index,
                        "label": label,
                        "within_label_invocation_position": within_label,
                        "sequence_index": len(schedule),
                    }
                )
    expected = int(normalized["samples_per_label_per_case"])
    if any(label_positions[label] != expected for label in normalized["labels"]):
        raise InterleavedMeasurementContractError(
            "generated interleaved schedule violates the sample contract"
        )
    return schedule


def _safe_token(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9._-]+", value))


def _positive_int(value: Any, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise InterleavedMeasurementContractError(f"{field} must be a positive integer")
    return value


def _nonnegative_int(value: Any, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise InterleavedMeasurementContractError(
            f"{field} must be a non-negative integer"
        )
    return value


def _json_safe(value: Any, field: str) -> None:
    try:
        json.dumps(value, ensure_ascii=True, sort_keys=True)
    except (TypeError, ValueError) as exc:
        raise InterleavedMeasurementContractError(
            f"{field} must be JSON serializable"
        ) from exc


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()


__all__ = [
    "INTERLEAVED_MEASUREMENT_SCHEMA",
    "PAIR_KEY_FIELDS",
    "InterleavedMeasurementContractError",
    "interleaved_session_schedule",
    "normalize_interleaved_measurement_plan",
]
