from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from ascendop_protocol.wire_v3 import validate_postprocess_recovery_request
from ascendop_daemon.storage.control_types import ControlDatabaseError
from ascendop_daemon.storage.control_validation import canonical_json, utc_now
from ascendop_daemon.storage.row_decoders import decode_outbox_row


def _accepted_postprocess_stage_attempts(
    conn: Any,
    *,
    engine_job_id: str,
    exclude_recovery_id: str,
) -> dict[str, list[str]]:
    attempts: dict[str, list[str]] = {}
    rows = conn.execute(
        "SELECT recovery_id, stages_json, receipt_json "
        "FROM postprocess_recoveries WHERE engine_job_id=? "
        "AND recovery_id<>? ORDER BY created_at, recovery_id",
        (engine_job_id, exclude_recovery_id),
    ).fetchall()
    for row in rows:
        receipt = json.loads(str(row["receipt_json"] or "{}"))
        engine_receipt = receipt.get("engine_recovery_receipt", {})
        engine_receipt = (
            engine_receipt if isinstance(engine_receipt, dict) else {}
        )
        if (
            receipt.get("state") != "recovery-accepted"
            and engine_receipt.get("state") != "recovery-accepted"
        ):
            continue
        recovery_id = str(row["recovery_id"])
        for stage in json.loads(str(row["stages_json"])):
            attempts.setdefault(str(stage), []).append(recovery_id)
    return attempts


class RetryDecisionRepository:
    def postprocess_recovery_candidates(
        self,
        *,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        self.initialize()
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT r.*, a.request_id, t.projection_json, t.payload_json "
                "FROM postprocess_recoveries r "
                "JOIN execution_attempts a ON a.attempt_id=r.attempt_id "
                "JOIN transport_returns t ON t.return_id=r.return_id "
                "WHERE r.state='decision-pending' "
                "ORDER BY r.created_at, r.recovery_id LIMIT ?",
                (max(0, int(limit)),),
            ).fetchall()
            candidates = []
            for row in rows:
                candidates.append(
                    {
                        **dict(row),
                        "stages": json.loads(str(row["stages_json"])),
                        "projection": json.loads(str(row["projection_json"])),
                        "result": json.loads(str(row["payload_json"])),
                        "accepted_stage_attempts": (
                            _accepted_postprocess_stage_attempts(
                                conn,
                                engine_job_id=str(row["engine_job_id"]),
                                exclude_recovery_id=str(row["recovery_id"]),
                            )
                        ),
                    }
                )
        return candidates

    def apply_postprocess_recovery_decision(
        self,
        recovery_id: str,
        *,
        action: str,
        reason: str,
        code_generation: str,
    ) -> dict[str, Any]:
        if action not in {"retry-idempotent-stage", "unrecoverable"}:
            raise ControlDatabaseError(
                f"unsupported postprocess recovery decision: {action}"
            )
        self.initialize()
        now = utc_now()
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT r.*, a.request_id FROM postprocess_recoveries r "
                "JOIN execution_attempts a ON a.attempt_id=r.attempt_id "
                "WHERE r.recovery_id=?",
                (recovery_id,),
            ).fetchone()
            if row is None:
                raise ControlDatabaseError(
                    f"unknown postprocess recovery: {recovery_id}"
                )
            if row["state"] != "decision-pending":
                return {
                    "recovery_id": recovery_id,
                    "state": str(row["state"]),
                    "idempotent": True,
                }
            stages = [str(item) for item in json.loads(str(row["stages_json"]))]
            accepted_stage_attempts = _accepted_postprocess_stage_attempts(
                conn,
                engine_job_id=str(row["engine_job_id"]),
                exclude_recovery_id=recovery_id,
            )
            exhausted_stages = sorted(
                stage for stage in stages if accepted_stage_attempts.get(stage)
            )
            if action == "retry-idempotent-stage" and exhausted_stages:
                action = "unrecoverable"
                reason = (
                    "postprocess recovery stage-attempt budget is exhausted: "
                    + ",".join(exhausted_stages)
                )
            decision = {
                "action": action,
                "reason": str(reason),
                "code_generation": code_generation,
                "decided_at": now,
                "accepted_stage_attempts": accepted_stage_attempts,
            }
            request: dict[str, Any] = {}
            if action == "retry-idempotent-stage":
                request = {
                    "schema": "ascendop.flow.postprocess-recovery.v1",
                    "version": 1,
                    "recovery_id": recovery_id,
                    "request_id": str(row["request_id"]),
                    "attempt_id": str(row["attempt_id"]),
                    "engine_job_id": str(row["engine_job_id"]),
                    "endpoint_id": str(row["endpoint_id"]),
                    "endpoint_generation": str(row["endpoint_generation"]),
                    "terminal_digest": str(row["terminal_digest"]),
                    "terminal_revision": int(row["terminal_revision"]),
                    "stages": stages,
                    "max_stage_attempts": 1,
                    "created_at": now,
                    "reason": str(reason),
                }
                request = validate_postprocess_recovery_request(request).request
                state = "authorized"
                conn.execute(
                    "UPDATE postprocess_recoveries SET state=?, request_json=?, "
                    "decision_json=?, updated_at=? WHERE recovery_id=?",
                    (
                        state,
                        canonical_json(request),
                        canonical_json(decision),
                        now,
                        recovery_id,
                    ),
                )
            else:
                state = "unrecoverable"
                conn.execute(
                    "UPDATE postprocess_recoveries SET state=?, decision_json=?, "
                    "error=?, updated_at=? WHERE recovery_id=?",
                    (
                        state,
                        canonical_json(decision),
                        str(reason),
                        now,
                        recovery_id,
                    ),
                )
                conn.execute(
                    "UPDATE transport_returns SET state='recovery-unrecoverable', "
                    "disposition='ready-to-ack', hold_reason=? WHERE return_id=?",
                    (str(reason), row["return_id"]),
                )
            self._event(
                conn,
                "postprocess-recovery-decision",
                "postprocess-recovery",
                recovery_id,
                {**decision, "request": request},
            )
        return {
            "recovery_id": recovery_id,
            "state": state,
            "action": action,
            "request": request,
            "idempotent": False,
        }

    def pending_postprocess_recoveries(
        self,
        *,
        endpoint_id: str,
        limit: int = 16,
    ) -> list[dict[str, Any]]:
        self.initialize()
        now = utc_now()
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT r.*, o.payload_json FROM postprocess_recoveries r "
                "JOIN transport_outbox o ON o.outbox_id=r.outbox_id "
                "WHERE r.endpoint_id=? AND r.state IN "
                "('authorized','uncertain','reconcile-required') "
                "AND (r.next_attempt_at='' OR r.next_attempt_at<=?) "
                "ORDER BY r.created_at, r.recovery_id LIMIT ?",
                (endpoint_id, now, max(0, int(limit))),
            ).fetchall()
        return [
            {
                **dict(row),
                "request": json.loads(str(row["request_json"])),
                "payload": json.loads(str(row["payload_json"])),
            }
            for row in rows
        ]

    def reconcile_uncertain_postprocess_recoveries(
        self,
        *,
        code_generation: str,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Revive only legacy terminal rows proven visibility-uncertain."""

        self.initialize()
        now = utc_now()
        reconciled: list[dict[str, Any]] = []
        with self.transaction() as conn:
            rows = conn.execute(
                "SELECT r.*, t.state AS return_state, t.acknowledged_at, "
                "(SELECT e.payload_json FROM control_events e "
                " WHERE e.entity_type='postprocess-recovery' "
                " AND e.entity_id=r.recovery_id "
                " AND e.event_type='postprocess-recovery-observed' "
                " ORDER BY e.sequence DESC LIMIT 1) AS observation_json "
                "FROM postprocess_recoveries r "
                "JOIN transport_returns t ON t.return_id=r.return_id "
                "WHERE r.state='unrecoverable' "
                "AND t.state='recovery-unrecoverable' "
                "AND t.acknowledged_at='' "
                "ORDER BY r.updated_at, r.recovery_id LIMIT ?",
                (max(0, int(limit)),),
            ).fetchall()
            for row in rows:
                decision = json.loads(str(row["decision_json"] or "{}"))
                observation = json.loads(str(row["observation_json"] or "{}"))
                if (
                    decision.get("action") != "retry-idempotent-stage"
                    or decision.get("reconcile_generation") == code_generation
                    or observation.get("status") != "uncertain"
                    or not bool(observation.get("retryable"))
                ):
                    continue
                decision.update(
                    {
                        "reconcile_generation": code_generation,
                        "reconciled_at": now,
                        "reconcile_reason": (
                            "prior generation exhausted transport visibility "
                            "checks without a durable endpoint rejection"
                        ),
                    }
                )
                hold_reason = (
                    "same recovery identity requires endpoint result-ref "
                    "reconciliation before acknowledgement"
                )
                conn.execute(
                    "UPDATE postprocess_recoveries SET state='reconcile-required', "
                    "decision_json=?, next_attempt_at=?, updated_at=? "
                    "WHERE recovery_id=? AND state='unrecoverable'",
                    (
                        canonical_json(decision),
                        now,
                        now,
                        row["recovery_id"],
                    ),
                )
                conn.execute(
                    "UPDATE transport_returns SET state='recovery-decision-pending', "
                    "disposition='hold-for-postprocess-recovery', hold_reason=? "
                    "WHERE return_id=? AND state='recovery-unrecoverable' "
                    "AND acknowledged_at=''",
                    (hold_reason, row["return_id"]),
                )
                event = {
                    "code_generation": code_generation,
                    "dispatch_attempts": int(row["dispatch_attempts"] or 0),
                    "from_state": "unrecoverable",
                    "state": "reconcile-required",
                }
                self._event(
                    conn,
                    "postprocess-recovery-reconcile-authorized",
                    "postprocess-recovery",
                    str(row["recovery_id"]),
                    event,
                )
                reconciled.append(
                    {"recovery_id": str(row["recovery_id"]), **event}
                )
        return reconciled

    def record_postprocess_recovery_observation(
        self,
        recovery_id: str,
        *,
        status: str,
        receipt: dict[str, Any] | None = None,
        error: str = "",
        retryable: bool = False,
        max_dispatch_attempts: int = 3,
        retry_delay_seconds: int = 5,
    ) -> dict[str, Any]:
        if status not in {"accepted", "uncertain", "rejected"}:
            raise ControlDatabaseError(
                f"unsupported postprocess recovery observation: {status}"
            )
        self.initialize()
        now = utc_now()
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT r.*, a.request_id FROM postprocess_recoveries r "
                "JOIN execution_attempts a ON a.attempt_id=r.attempt_id "
                "WHERE r.recovery_id=?",
                (recovery_id,),
            ).fetchone()
            if row is None:
                raise ControlDatabaseError(
                    f"unknown postprocess recovery: {recovery_id}"
                )
            if row["state"] in {"dispatched", "result-received", "completed"}:
                return {
                    "recovery_id": recovery_id,
                    "state": str(row["state"]),
                    "idempotent": True,
                }
            attempts = int(row["dispatch_attempts"] or 0) + 1
            exhausted = attempts >= max(1, int(max_dispatch_attempts))
            if status == "accepted":
                state = "dispatched"
                conn.execute(
                    "UPDATE postprocess_recoveries SET state=?, receipt_json=?, "
                    "dispatch_attempts=?, next_attempt_at='', error='', updated_at=? "
                    "WHERE recovery_id=?",
                    (
                        state,
                        canonical_json(receipt or {}),
                        attempts,
                        now,
                        recovery_id,
                    ),
                )
                conn.execute(
                    "UPDATE transport_returns SET state='recovery-superseded' "
                    "WHERE return_id=?",
                    (row["return_id"],),
                )
                conn.execute(
                    "UPDATE transport_outbox SET state='accepted', next_poll_at=?, "
                    "updated_at=? WHERE outbox_id=?",
                    (now, now, row["outbox_id"]),
                )
                conn.execute(
                    "UPDATE execution_attempts SET state='accepted', error='', "
                    "updated_at=? WHERE attempt_id=?",
                    (now, row["attempt_id"]),
                )
                conn.execute(
                    "UPDATE test_requests SET state='accepted', blocker='', "
                    "updated_at=? WHERE request_id=?",
                    (now, row["request_id"]),
                )
            elif status == "uncertain" and retryable:
                state = "reconcile-required" if exhausted else "uncertain"
                delay_multiplier = 2 ** min(max(0, attempts - 1), 6)
                next_attempt = (
                    datetime.now(timezone.utc)
                    + timedelta(
                        seconds=min(
                            300,
                            max(1, int(retry_delay_seconds)) * delay_multiplier,
                        )
                    )
                ).replace(microsecond=0).isoformat()
                conn.execute(
                    "UPDATE postprocess_recoveries SET state=?, dispatch_attempts=?, "
                    "next_attempt_at=?, error=?, updated_at=? WHERE recovery_id=?",
                    (state, attempts, next_attempt, str(error), now, recovery_id),
                )
            else:
                state = "unrecoverable"
                detail = str(error) or "postprocess recovery was rejected"
                conn.execute(
                    "UPDATE postprocess_recoveries SET state=?, dispatch_attempts=?, "
                    "next_attempt_at='', error=?, updated_at=? WHERE recovery_id=?",
                    (state, attempts, detail, now, recovery_id),
                )
                conn.execute(
                    "UPDATE transport_returns SET state='recovery-unrecoverable', "
                    "disposition='ready-to-ack', hold_reason=? WHERE return_id=?",
                    (detail, row["return_id"]),
                )
            self._event(
                conn,
                "postprocess-recovery-observed",
                "postprocess-recovery",
                recovery_id,
                {
                    "status": status,
                    "state": state,
                    "dispatch_attempts": attempts,
                    "receipt": receipt or {},
                    "error": str(error),
                    "retryable": bool(retryable),
                },
            )
        return {
            "recovery_id": recovery_id,
            "state": state,
            "dispatch_attempts": attempts,
            "idempotent": False,
        }

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
                    "query_inflight_sequence=0, "
                    "query_inflight_owner_generation='', error='', updated_at=? "
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
