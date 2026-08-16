from __future__ import annotations

from typing import Any


def dispatch_postprocess_recoveries(
    *,
    database: Any,
    endpoint_id: str,
    transport: Any,
    report: dict[str, Any],
    capacity: int,
    max_dispatch_attempts: int,
    retry_delay_seconds: int,
) -> None:
    recover = getattr(transport, "recover_postprocess", None)
    if not callable(recover):
        return
    rows = database.pending_postprocess_recoveries(
        endpoint_id=endpoint_id,
        limit=max(4, capacity),
    )
    for row in rows:
        recovery_id = str(row["recovery_id"])
        try:
            observation = recover(
                dict(row["payload"]),
                dict(row["request"]),
            )
            applied = database.record_postprocess_recovery_observation(
                recovery_id,
                status=str(observation.status),
                receipt=dict(observation.receipt),
                error=str(observation.error),
                retryable=bool(observation.retryable),
                max_dispatch_attempts=max_dispatch_attempts,
                retry_delay_seconds=retry_delay_seconds,
            )
            report["postprocess_recoveries"].append(applied)
        except Exception as exc:
            report["errors"].append(
                {
                    "postprocess_recovery_id": recovery_id,
                    "phase": "postprocess-recovery",
                    "error": str(exc),
                }
            )
