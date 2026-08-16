from __future__ import annotations

from typing import Any

from ascendop_daemon.control_plane.flow_v3_policy import (
    FailureRecord,
    decide_retry,
)
from ascendop_daemon.control_plane.control_database import ControlDatabase


class RetryController:
    """Sole production owner of retry and reconciliation decisions."""

    def __init__(
        self,
        database: ControlDatabase,
        *,
        code_generation: str,
        max_transport_retries: int,
    ) -> None:
        self.database = database
        self.code_generation = code_generation
        self.max_transport_retries = max(0, int(max_transport_retries))

    def run_once(self, *, limit: int = 100) -> dict[str, Any]:
        report: dict[str, Any] = {
            "candidates": 0,
            "decisions": [],
            "errors": [],
        }
        candidates = self.database.retry_decision_candidates(limit=limit)
        report["candidates"] = len(candidates)
        for candidate in candidates:
            try:
                failure = FailureRecord(**dict(candidate["failure"]))
                decision = decide_retry(
                    failure,
                    execution_attempt=int(candidate["execution_ordinal"]),
                    max_execution_attempts=int(candidate["execution_ordinal"]),
                    transport_retry=int(candidate["transport_retry_count"]),
                    max_transport_retries=self.max_transport_retries,
                    stage_retry=0,
                    max_idempotent_stage_retries=0,
                    stage_idempotent=False,
                )
                applied = self.database.apply_retry_decision(
                    str(candidate["outbox_id"]),
                    failure_event_sequence=int(
                        candidate["failure_event_sequence"]
                    ),
                    decision=decision.to_dict(),
                    failure=failure.to_dict(),
                    code_generation=self.code_generation,
                )
                report["decisions"].append(applied)
            except Exception as exc:
                report["errors"].append(
                    {
                        "outbox_id": str(candidate.get("outbox_id") or ""),
                        "error": str(exc),
                    }
                )
        return report
