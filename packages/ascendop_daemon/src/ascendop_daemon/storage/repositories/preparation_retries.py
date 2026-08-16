from __future__ import annotations

import json
from typing import Any

from ascendop_daemon.storage.control_types import ControlDatabaseError
from ascendop_daemon.storage.control_validation import utc_now
from ascendop_daemon.storage.row_decoders import decode_preparation_row


class PreparationRetryRepository:
    """Generation-scoped retry decisions for pre-publication package builds."""

    def preparation_retry_candidates(
        self,
        *,
        code_generation: str,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        current_generation = str(code_generation).strip()
        if not current_generation:
            raise ControlDatabaseError(
                "preparation retry selection requires a code generation"
            )
        self.initialize()
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT preparation.* FROM request_preparations AS preparation "
                "JOIN test_requests AS request "
                "ON request.request_id=preparation.request_id "
                "WHERE preparation.state='failed' AND request.state='blocked' "
                "AND NOT EXISTS (SELECT 1 FROM request_preparations AS newer "
                "WHERE newer.request_id=preparation.request_id "
                "AND newer.state='failed' "
                "AND (newer.updated_at>preparation.updated_at OR "
                "(newer.updated_at=preparation.updated_at "
                "AND newer.preparation_id>preparation.preparation_id))) "
                "ORDER BY preparation.updated_at, preparation.preparation_id "
                "LIMIT ?",
                (max(0, int(limit)) * 4,),
            ).fetchall()
            candidates: list[dict[str, Any]] = []
            for row in rows:
                failure_event = conn.execute(
                    "SELECT sequence, payload_json FROM control_events "
                    "WHERE entity_type='request-preparation' AND entity_id=? "
                    "AND event_type='wire-v3-preparation-failed' "
                    "ORDER BY sequence DESC LIMIT 1",
                    (row["preparation_id"],),
                ).fetchone()
                if failure_event is None:
                    continue
                failure = json.loads(str(failure_event["payload_json"]))
                failed_generation = str(failure.get("code_generation") or "")
                if failed_generation == current_generation:
                    continue
                decisions = conn.execute(
                    "SELECT payload_json FROM control_events "
                    "WHERE entity_type='request-preparation' AND entity_id=? "
                    "AND event_type='preparation-retry-decision' "
                    "ORDER BY sequence",
                    (row["preparation_id"],),
                ).fetchall()
                if any(
                    str(
                        json.loads(str(item["payload_json"])).get(
                            "code_generation"
                        )
                        or ""
                    )
                    == current_generation
                    for item in decisions
                ):
                    continue
                value = decode_preparation_row(row)
                value.update(
                    {
                        "failure_event_sequence": int(failure_event["sequence"]),
                        "failed_code_generation": failed_generation,
                        "current_code_generation": current_generation,
                    }
                )
                candidates.append(value)
                if len(candidates) >= max(0, int(limit)):
                    break
        return candidates

    def apply_preparation_retry_decision(
        self,
        preparation_id: str,
        *,
        failure_event_sequence: int,
        code_generation: str,
        reason: str,
    ) -> dict[str, Any]:
        current_generation = str(code_generation).strip()
        if not current_generation:
            raise ControlDatabaseError(
                "preparation retry decision requires a code generation"
            )
        now = utc_now()
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM request_preparations WHERE preparation_id=?",
                (preparation_id,),
            ).fetchone()
            if row is None:
                raise ControlDatabaseError(
                    f"unknown request preparation: {preparation_id}"
                )
            prior = conn.execute(
                "SELECT payload_json FROM control_events "
                "WHERE entity_type='request-preparation' AND entity_id=? "
                "AND event_type='preparation-retry-decision' "
                "ORDER BY sequence",
                (preparation_id,),
            ).fetchall()
            for item in prior:
                payload = json.loads(str(item["payload_json"]))
                if (
                    int(payload.get("failure_event_sequence", 0) or 0)
                    == int(failure_event_sequence)
                    and str(payload.get("code_generation") or "")
                    == current_generation
                ):
                    return {
                        "preparation_id": preparation_id,
                        "action": "retry-prepublish",
                        "idempotent": True,
                    }
            failure_event = conn.execute(
                "SELECT sequence FROM control_events "
                "WHERE entity_type='request-preparation' AND entity_id=? "
                "AND event_type='wire-v3-preparation-failed' "
                "ORDER BY sequence DESC LIMIT 1",
                (preparation_id,),
            ).fetchone()
            if (
                failure_event is None
                or int(failure_event["sequence"]) != int(failure_event_sequence)
            ):
                raise ControlDatabaseError(
                    "preparation retry decision does not match the latest failure"
                )
            if str(row["state"]) != "failed":
                raise ControlDatabaseError(
                    "preparation retry requires failed state, got "
                    f"{row['state']}"
                )
            conn.execute(
                "UPDATE request_preparations SET state='retry-superseded', "
                "updated_at=? WHERE preparation_id=?",
                (now, preparation_id),
            )
            conn.execute(
                "UPDATE test_requests SET state='blocked', "
                "blocker='prepublication-retry-authorized', updated_at=? "
                "WHERE request_id=?",
                (now, row["request_id"]),
            )
            payload = {
                "action": "retry-prepublish",
                "reason": str(reason),
                "failure_event_sequence": int(failure_event_sequence),
                "code_generation": current_generation,
                "execution_attempt_consumed": False,
            }
            self._event(
                conn,
                "preparation-retry-decision",
                "request-preparation",
                preparation_id,
                payload,
            )
        return {
            "preparation_id": preparation_id,
            "action": "retry-prepublish",
            "idempotent": False,
        }


__all__ = ["PreparationRetryRepository"]
