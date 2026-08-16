from __future__ import annotations

from typing import Any

# Bump when the generated prewarm stage graph changes incompatibly.  Failed
# attempts from an older graph must not permanently exhaust the retry budget
# for the corrected graph.
CASE_CACHE_PREWARM_GENERATION = "v2"
CASE_CACHE_PREWARM_PROFILE = "engine-v3-case-cache-prewarm"


ACTIVE_PREWARM_STATES = {
    "pending",
    "admitting",
    "staging-standby",
    "standby",
    "standby-cancel-requested",
    "accepted",
    "running",
    "return-ready",
    "returned-awaiting-ingest",
}


def evaluate_case_cache_prewarm(
    pump_state: dict[str, Any],
    requirement_sha256: str,
    *,
    max_attempts: int = 3,
) -> dict[str, Any]:
    requirement_sha256 = str(requirement_sha256 or "").strip()
    if not requirement_sha256:
        raise ValueError("case-cache prewarm requirement sha256 is missing")
    max_attempts = max(1, int(max_attempts or 1))
    raw_entries = pump_state.get("entries", {})
    records = [
        dict(record)
        for record in (
            raw_entries.values() if isinstance(raw_entries, dict) else []
        )
        if isinstance(record, dict)
        and str(record.get("case_cache_requirement_sha256") or "")
        == requirement_sha256
    ]
    records.sort(
        key=lambda item: (
            int(item.get("sequence", 0) or 0),
            str(item.get("enqueued_at") or ""),
        )
    )
    entries = [
        record
        for record in records
        if str(record.get("execution_profile") or "")
        == CASE_CACHE_PREWARM_PROFILE
        and f"-case-cache-{CASE_CACHE_PREWARM_GENERATION}-"
        in str(record.get("engine_job_id") or "")
    ]
    strict_misses = [
        record
        for record in records
        if str(record.get("case_cache_access") or "") == "require-hit"
        and str(record.get("engine_terminal_state") or "") == "failed"
        and str(record.get("engine_failed_stage") or "")
        in {"case-cache", "runtime-wheel-install", "runtime-wheel-case-cache"}
        and int(record.get("engine_failed_exit_code", 0) or 0) == 49
    ]
    latest_miss = strict_misses[-1] if strict_misses else None
    invalidated_at_sequence = (
        int(latest_miss.get("sequence", 0) or 0) if latest_miss else 0
    )
    generation_entries = [
        record
        for record in entries
        if int(record.get("sequence", 0) or 0) > invalidated_at_sequence
    ]
    invalidation = (
        {
            "invalidated_by_engine_job_id": str(
                latest_miss.get("engine_job_id") or ""
            ),
            "invalidated_at_sequence": invalidated_at_sequence,
        }
        if latest_miss
        else {}
    )
    successful = [
        record
        for record in generation_entries
        if record.get("state") == "canary-complete"
        and record.get("engine_terminal_state") == "completed"
    ]
    if successful:
        latest = successful[-1]
        return {
            "state": "ready",
            "requirement_sha256": requirement_sha256,
            "attempt_count": len(generation_entries),
            "lifetime_attempt_count": len(entries),
            "engine_job_id": str(latest.get("engine_job_id") or ""),
            **invalidation,
        }
    active = [
        record
        for record in generation_entries
        if str(record.get("state") or "") in ACTIVE_PREWARM_STATES
    ]
    if active:
        latest = active[-1]
        return {
            "state": "waiting",
            "requirement_sha256": requirement_sha256,
            "attempt_count": len(generation_entries),
            "lifetime_attempt_count": len(entries),
            "engine_job_id": str(latest.get("engine_job_id") or ""),
            "engine_state": str(latest.get("state") or ""),
            **invalidation,
        }
    if len(generation_entries) >= max_attempts:
        latest = generation_entries[-1]
        return {
            "state": "blocked",
            "requirement_sha256": requirement_sha256,
            "attempt_count": len(generation_entries),
            "lifetime_attempt_count": len(entries),
            "max_attempts": max_attempts,
            "engine_job_id": str(latest.get("engine_job_id") or ""),
            "engine_state": str(latest.get("state") or ""),
            "terminal_state": str(latest.get("engine_terminal_state") or ""),
            "error": str(latest.get("last_error") or ""),
            **invalidation,
        }
    return {
        "state": "enqueue",
        "requirement_sha256": requirement_sha256,
        "attempt_count": len(generation_entries),
        "lifetime_attempt_count": len(entries),
        "next_attempt": len(entries) + 1,
        "max_attempts": max_attempts,
        **invalidation,
    }


def case_cache_prewarm_suffix(requirement_sha256: str, attempt: int) -> str:
    token = str(requirement_sha256 or "").strip().lower()
    if not token:
        raise ValueError("case-cache prewarm requirement sha256 is missing")
    attempt = max(1, int(attempt or 1))
    suffix = f"case-cache-{CASE_CACHE_PREWARM_GENERATION}-{token[:16]}"
    return suffix if attempt == 1 else f"{suffix}-retry{attempt - 1:03d}"
