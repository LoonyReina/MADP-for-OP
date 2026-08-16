from __future__ import annotations

from typing import Any


def engine_correlation(raw: dict[str, Any]) -> dict[str, str]:
    return {
        "request_id": _token(str(raw.get("request_id") or ""), "request_id"),
        "engine_job_id": _token(
            str(raw.get("engine_job_id") or ""), "engine_job_id"
        ),
        "attempt_id": _token(str(raw.get("attempt_id") or ""), "attempt_id"),
        "operator": _token(
            str(raw.get("operator") or raw.get("op") or ""), "operator"
        ),
        "test_version": _token(
            str(raw.get("test_version") or ""), "test_version"
        ),
    }


def _token(value: str, label: str) -> str:
    cleaned = "".join(
        char if char.isalnum() or char in "._-" else "_" for char in value
    ).strip("._-")
    if not cleaned:
        raise ValueError(f"missing or invalid {label}")
    return cleaned
