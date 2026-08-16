from __future__ import annotations

import json
from typing import Any

from ascendop_daemon.storage.control_types import ControlDatabaseError
from ascendop_daemon.storage.control_validation import utc_now
from ascendop_daemon.storage.row_decoders import decode_outbox_row


class RetryDecisionRepository:
    def record_transport_query_failure(
        self,
        outbox_id: str,
        *,
        error: str,
        failure: dict[str, Any],
    ) -> dict[str, Any]:
        """Escalate a proven query disposition to the central retry controller."""
        self.initialize()
        now = utc_now()
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT o.*, a.request_id FROM transport_outbox o "
                "JOIN execution_attempts a ON a.attempt_id=o.attempt_id "
                "WHERE o.outbox_id=?",
                (outbox_id,),
            ).fetchone()
            if row is None:
                raise ControlDatabaseError(f"unknown transport outbox: {outbox_id}")
            if row["state"] != "uncertain":
                return decode_outbox_row(row)
            conn.execute(
                "UPDATE transport_outbox SET state='decision-pending', error=?, "
                "next_poll_at='', updated_at=? WHERE outbox_id=?",
                (error, now, outbox_id),
            )
            conn.execute(
                "UPDATE execution_attempts SET state='retry-decision-pending', "
                "error=?, updated_at=? WHERE attempt_id=?",
                (error, now, row["attempt_id"]),
            )
            conn.execute(
                "UPDATE test_requests SET state='retry-decision-pending', "
                "blocker=?, updated_at=? WHERE request_id=?",
                (error, now, row["request_id"]),
            )
            self._event(
                conn,
                "transport-delivery-decision-pending",
                "transport-outbox",
                outbox_id,
                {
                    "consumer": "endpoint-query",
                    "error": error,
                    "failure": failure,
                    "delivery_attempts": int(row["delivery_attempts"]),
                },
            )
            current = conn.execute(
                "SELECT * FROM transport_outbox WHERE outbox_id=?",
                (outbox_id,),
            ).fetchone()
        return decode_outbox_row(current)

    def retry_decision_candidates(
        self,
        *,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Return delivery failures that have no decision for their latest event."""
        self.initialize()
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT o.*, a.request_id, a.ordinal AS execution_ordinal "
                "FROM transport_outbox o JOIN execution_attempts a "
                "ON a.attempt_id=o.attempt_id WHERE "
                "o.state='decision-pending' OR (o.state='failed' "
                "AND o.accepted_at='' AND o.remote_receipt_json='{}' AND ("
                "lower(o.error) LIKE '%argument kind: invalid choice%' OR "
                "lower(o.error) LIKE '%unrecognized arguments%result-repo%'"
                ")) ORDER BY o.updated_at, o.outbox_id LIMIT ?",
                (max(0, int(limit)),),
            ).fetchall()
            candidates: list[dict[str, Any]] = []
            for row in rows:
                failure_event = conn.execute(
                    "SELECT sequence, payload_json FROM control_events "
                    "WHERE entity_type='transport-outbox' AND entity_id=? "
                    "AND event_type IN ("
                    "'transport-delivery-decision-pending',"
                    "'transport-delivery-failed') "
                    "ORDER BY sequence DESC LIMIT 1",
                    (row["outbox_id"],),
                ).fetchone()
                if failure_event is None:
                    continue
                failure_sequence = int(failure_event["sequence"])
                decisions = conn.execute(
                    "SELECT payload_json FROM control_events "
                    "WHERE entity_type='transport-outbox' AND entity_id=? "
                    "AND event_type='retry-decision' ORDER BY sequence",
                    (row["outbox_id"],),
                ).fetchall()
                decision_payloads = [
                    json.loads(str(item["payload_json"])) for item in decisions
                ]
                if any(
                    int(item.get("failure_event_sequence", 0) or 0)
                    == failure_sequence
                    for item in decision_payloads
                ):
                    continue
                event_payload = json.loads(str(failure_event["payload_json"]))
                failure = event_payload.get("failure", {})
                legacy_pre_publish = not isinstance(failure, dict) or not failure
                if legacy_pre_publish:
                    failure = {
                        "domain": "transport",
                        "code": "legacy-gp-cli-prepublish-failure",
                        "phase": "publish",
                        "detail": str(row["error"]),
                        "retryable": True,
                        "pre_publish": True,
                        "result_visibility": "not-published",
                    }
                candidate = decode_outbox_row(row)
                candidate.update(
                    {
                        "request_id": str(row["request_id"]),
                        "execution_ordinal": int(row["execution_ordinal"]),
                        "failure_event_sequence": failure_sequence,
                        "failure": dict(failure),
                        "legacy_pre_publish": legacy_pre_publish,
                        "transport_retry_count": sum(
                            1
                            for item in decision_payloads
                            if item.get("action") == "retry-transport"
                        ),
                    }
                )
                candidates.append(candidate)
        return candidates

    def apply_retry_decision(
        self,
        outbox_id: str,
        *,
        failure_event_sequence: int,
        decision: dict[str, Any],
        failure: dict[str, Any],
        code_generation: str,
    ) -> dict[str, Any]:
        """Persist one retry decision and its state projection atomically."""
        self.initialize()
        action = str(decision.get("action") or "")
        if action not in {"retry-transport", "reconcile", "terminal"}:
            raise ControlDatabaseError(
                f"unsupported production retry decision: {action}"
            )
        now = utc_now()
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT o.*, a.request_id FROM transport_outbox o "
                "JOIN execution_attempts a ON a.attempt_id=o.attempt_id "
                "WHERE o.outbox_id=?",
                (outbox_id,),
            ).fetchone()
            if row is None:
                raise ControlDatabaseError(f"unknown transport outbox: {outbox_id}")
            prior = conn.execute(
                "SELECT payload_json FROM control_events "
                "WHERE entity_type='transport-outbox' AND entity_id=? "
                "AND event_type='retry-decision' ORDER BY sequence",
                (outbox_id,),
            ).fetchall()
            for item in prior:
                payload = json.loads(str(item["payload_json"]))
                if int(payload.get("failure_event_sequence", 0) or 0) == int(
                    failure_event_sequence
                ):
                    return {
                        "outbox_id": outbox_id,
                        "action": str(payload.get("action") or ""),
                        "idempotent": True,
                    }
            if row["state"] not in {"decision-pending", "failed"}:
                raise ControlDatabaseError(
                    "retry decision cannot apply from outbox state "
                    f"{row['state']}"
                )
            previous_delivery_attempts = int(row["delivery_attempts"])
            if action == "retry-transport":
                conn.execute(
                    "UPDATE transport_outbox SET state='retry', claimed_by='', "
                    "claim_token='', claim_expires_at='', delivery_attempts=0, "
                    "next_attempt_at='', next_poll_at='', poll_attempts=0, "
                    "query_inflight_sequence=0, error='', updated_at=? "
                    "WHERE outbox_id=?",
                    (now, outbox_id),
                )
                conn.execute(
                    "UPDATE execution_attempts SET state='prepared', error='', "
                    "updated_at=? WHERE attempt_id=?",
                    (now, row["attempt_id"]),
                )
                conn.execute(
                    "UPDATE test_requests SET state='routed', blocker='', updated_at=? "
                    "WHERE request_id=?",
                    (now, row["request_id"]),
                )
            elif action == "reconcile":
                conn.execute(
                    "UPDATE transport_outbox SET state='uncertain', claimed_by='', "
                    "claim_token='', claim_expires_at='', next_poll_at=?, "
                    "error=?, updated_at=? WHERE outbox_id=?",
                    (now, str(failure.get("detail") or ""), now, outbox_id),
                )
                conn.execute(
                    "UPDATE execution_attempts SET state='uncertain', error=?, "
                    "updated_at=? WHERE attempt_id=?",
                    (str(failure.get("detail") or ""), now, row["attempt_id"]),
                )
                conn.execute(
                    "UPDATE test_requests SET state='uncertain', blocker=?, "
                    "updated_at=? WHERE request_id=?",
                    (
                        str(failure.get("detail") or ""),
                        now,
                        row["request_id"],
                    ),
                )
            else:
                detail = str(failure.get("detail") or "retry policy terminal")
                conn.execute(
                    "UPDATE transport_outbox SET state='failed', claimed_by='', "
                    "claim_token='', claim_expires_at='', error=?, updated_at=? "
                    "WHERE outbox_id=?",
                    (detail, now, outbox_id),
                )
                conn.execute(
                    "UPDATE execution_attempts SET state='failed', error=?, "
                    "updated_at=? WHERE attempt_id=?",
                    (detail, now, row["attempt_id"]),
                )
                conn.execute(
                    "UPDATE test_requests SET state='failed', blocker=?, updated_at=? "
                    "WHERE request_id=?",
                    (detail, now, row["request_id"]),
                )
            event_payload = {
                **decision,
                "failure": failure,
                "failure_event_sequence": int(failure_event_sequence),
                "code_generation": code_generation,
                "previous_delivery_attempts": previous_delivery_attempts,
            }
            self._event(
                conn,
                "retry-decision",
                "transport-outbox",
                outbox_id,
                event_payload,
            )
        return {
            "outbox_id": outbox_id,
            "action": action,
            "idempotent": False,
            "previous_delivery_attempts": previous_delivery_attempts,
        }
