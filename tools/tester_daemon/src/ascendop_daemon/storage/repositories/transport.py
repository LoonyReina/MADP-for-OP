from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from ascendop_protocol.automation import (
    ASSISTANT_ACTION_REQUEST_SCHEMA,
    evaluate_trigger_rule,
    validate_action_receipt,
    validate_action_request,
)
from ascendop_daemon.core.models import DaemonConfig
from ascendop_daemon.registry.system_registry import (
    SYSTEM_REGISTRY_SCHEMA,
    RouteDecision,
    SystemRegistry,
)
from ascendop_daemon.storage.row_decoders import (
    decode_assistant_action,
    decode_assistant_receipt,
    decode_attempt_row,
    decode_operator_row,
    decode_outbox_row,
    decode_request_row,
    decode_return_row,
)
from ascendop_daemon.storage.control_types import (
    ACTIVE_ATTEMPT_STATES,
    ACTIVE_NODE_STATES,
    BOOTSTRAP_CONTROL_PROBE_POLICY,
    OUTBOX_ACTIVE_STATES,
    OUTBOX_CLAIMABLE_STATES,
    SCHEMA_VERSION,
    SYSTEM_EXPERIMENT_GENERATION,
    SYSTEM_EXPERIMENT_OPERATOR_ID,
    ControlDatabaseError,
)
from ascendop_daemon.storage.control_validation import (
    _ensure_column,
    _format_timestamp,
    _is_bootstrap_control_probe,
    _node_report_is_newer,
    _normalized_node_lease,
    _parse_timestamp,
    _require_transport_claim,
    _runtime_route_rejection_reasons,
    _validate_node_report,
    _validate_observed_node_against_registry,
    _validate_transport_identity,
    canonical_json,
    utc_now,
)

class TransportRepository:
    def recover_expired_transport_claims(
        self,
        *,
        endpoint_id: str = "",
    ) -> list[dict[str, Any]]:
        self.initialize()
        now = _format_timestamp(datetime.now(timezone.utc))
        with self.transaction() as conn:
            return self._recover_expired_transport_claims(
                conn,
                now,
                endpoint_id=endpoint_id,
            )

    def _recover_expired_transport_claims(
        self,
        conn: sqlite3.Connection,
        now: str,
        *,
        endpoint_id: str = "",
    ) -> list[dict[str, Any]]:
        parameters: list[Any] = [now]
        endpoint_clause = ""
        if endpoint_id:
            endpoint_clause = " AND endpoint_id=?"
            parameters.append(endpoint_id)
        stale = conn.execute(
            "SELECT * FROM transport_outbox "
            "WHERE state IN ('claimed','sending') "
            "AND claim_expires_at != '' AND claim_expires_at <= ?"
            + endpoint_clause,
            tuple(parameters),
        ).fetchall()
        recovered: list[dict[str, Any]] = []
        for row in stale:
            uncertain = row["state"] == "sending"
            next_state = "uncertain" if uncertain else "retry"
            attempt_state = "uncertain" if uncertain else "prepared"
            error = (
                "dispatcher claim expired after send began"
                if uncertain
                else "dispatcher claim expired before send"
            )
            conn.execute(
                "UPDATE transport_outbox SET state=?, claimed_by='', "
                "claim_token='', claim_expires_at='', error=?, updated_at=? "
                "WHERE outbox_id=?",
                (next_state, error, now, row["outbox_id"]),
            )
            conn.execute(
                "UPDATE execution_attempts SET state=?, error=?, updated_at=? "
                "WHERE attempt_id=?",
                (attempt_state, error, now, row["attempt_id"]),
            )
            value = {
                "outbox_id": str(row["outbox_id"]),
                "previous_state": str(row["state"]),
                "next_state": next_state,
                "consumer": str(row["claimed_by"]),
            }
            recovered.append(value)
            self._event(
                conn,
                "transport-claim-expired",
                "transport-outbox",
                str(row["outbox_id"]),
                value,
            )
        return recovered

    def claim_transport_outbox(
        self,
        consumer: str,
        *,
        max_items: int = 1,
        ttl_seconds: int = 90,
        endpoint_id: str = "",
    ) -> list[dict[str, Any]]:
        self.initialize()
        if not consumer:
            raise ControlDatabaseError("transport consumer must be non-empty")
        limit = max(0, int(max_items))
        if limit == 0:
            return []
        now_dt = datetime.now(timezone.utc)
        now = _format_timestamp(now_dt)
        expires = _format_timestamp(
            now_dt + timedelta(seconds=max(1, int(ttl_seconds)))
        )
        claimed: list[dict[str, Any]] = []
        with self.transaction() as conn:
            self._recover_expired_transport_claims(
                conn,
                now,
                endpoint_id=endpoint_id,
            )
            parameters: list[Any] = [now]
            endpoint_clause = ""
            if endpoint_id:
                endpoint_clause = " AND endpoint_id=?"
                parameters.append(endpoint_id)
            parameters.append(limit)
            rows = conn.execute(
                "SELECT * FROM transport_outbox "
                "WHERE state IN ('pending','retry') "
                "AND (next_attempt_at='' OR next_attempt_at<=?)"
                + endpoint_clause
                + " ORDER BY created_at, outbox_id LIMIT ?",
                tuple(parameters),
            ).fetchall()
            for row in rows:
                token = uuid.uuid4().hex
                changed = conn.execute(
                    "UPDATE transport_outbox SET state='claimed', claimed_by=?, "
                    "claim_token=?, claim_expires_at=?, error='', updated_at=? "
                    "WHERE outbox_id=? AND state IN ('pending','retry')",
                    (
                        consumer,
                        token,
                        expires,
                        now,
                        row["outbox_id"],
                    ),
                ).rowcount
                if not changed:
                    continue
                conn.execute(
                    "UPDATE execution_attempts SET state='queued', error='', "
                    "updated_at=? WHERE attempt_id=?",
                    (now, row["attempt_id"]),
                )
                current = conn.execute(
                    "SELECT * FROM transport_outbox WHERE outbox_id=?",
                    (row["outbox_id"],),
                ).fetchone()
                claimed.append(decode_outbox_row(current))
                self._event(
                    conn,
                    "transport-outbox-claimed",
                    "transport-outbox",
                    str(row["outbox_id"]),
                    {
                        "consumer": consumer,
                        "claim_token": token,
                        "claim_expires_at": expires,
                        "endpoint_id": str(row["endpoint_id"]),
                    },
                )
        return claimed

    def mark_transport_sending(
        self,
        outbox_id: str,
        *,
        consumer: str,
        claim_token: str,
    ) -> dict[str, Any]:
        self.initialize()
        now = utc_now()
        with self.transaction() as conn:
            row = _require_transport_claim(
                conn, outbox_id, consumer, claim_token, {"claimed"}
            )
            conn.execute(
                "UPDATE transport_outbox SET state='sending', "
                "delivery_attempts=delivery_attempts+1, updated_at=? "
                "WHERE outbox_id=?",
                (now, outbox_id),
            )
            conn.execute(
                "UPDATE execution_attempts SET state='sent', updated_at=? "
                "WHERE attempt_id=?",
                (now, row["attempt_id"]),
            )
            self._event(
                conn,
                "transport-send-started",
                "transport-outbox",
                outbox_id,
                {"consumer": consumer},
            )
            current = conn.execute(
                "SELECT * FROM transport_outbox WHERE outbox_id=?",
                (outbox_id,),
            ).fetchone()
        return decode_outbox_row(current)

    def record_transport_delivery(
        self,
        outbox_id: str,
        *,
        consumer: str,
        claim_token: str,
        status: str,
        receipt: dict[str, Any] | None = None,
        error: str = "",
        retry_after_seconds: int = 0,
        failure: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.initialize()
        if status not in {
            "accepted",
            "uncertain",
            "retry",
            "decision-pending",
            "failed",
        }:
            raise ControlDatabaseError(
                f"unsupported transport delivery status: {status}"
            )
        now_dt = datetime.now(timezone.utc)
        now = _format_timestamp(now_dt)
        with self.transaction() as conn:
            row = _require_transport_claim(
                conn, outbox_id, consumer, claim_token, {"sending"}
            )
            payload = json.loads(str(row["payload_json"]))
            receipt_value = receipt or {}
            if status == "accepted":
                _validate_transport_identity(payload, receipt_value)
                attempt_state = "accepted"
                accepted_at = now
                next_attempt_at = ""
            elif status == "uncertain":
                attempt_state = "uncertain"
                accepted_at = ""
                next_attempt_at = ""
            elif status == "retry":
                attempt_state = "prepared"
                accepted_at = ""
                next_attempt_at = _format_timestamp(
                    now_dt
                    + timedelta(seconds=max(0, int(retry_after_seconds)))
                )
            elif status == "decision-pending":
                attempt_state = "retry-decision-pending"
                accepted_at = ""
                next_attempt_at = ""
            else:
                attempt_state = "failed"
                accepted_at = ""
                next_attempt_at = ""
            conn.execute(
                "UPDATE transport_outbox SET state=?, claimed_by='', "
                "claim_token='', claim_expires_at='', remote_receipt_json=?, "
                "next_attempt_at=?, next_poll_at=?, poll_attempts=0, "
                "query_inflight_sequence=CASE WHEN ?=1 THEN 0 "
                "ELSE query_inflight_sequence END, accepted_at=?, error=?, "
                "updated_at=? "
                "WHERE outbox_id=?",
                (
                    status,
                    canonical_json(receipt_value),
                    next_attempt_at,
                    now if status in {"accepted", "uncertain"} else "",
                    1 if status in {"accepted", "retry"} else 0,
                    accepted_at,
                    error,
                    now,
                    outbox_id,
                ),
            )
            conn.execute(
                "UPDATE execution_attempts SET state=?, error=?, updated_at=? "
                "WHERE attempt_id=?",
                (attempt_state, error, now, row["attempt_id"]),
            )
            if status in {"decision-pending", "failed"}:
                conn.execute(
                    "UPDATE test_requests SET state=?, blocker=?, "
                    "updated_at=? WHERE request_id=(SELECT request_id FROM "
                    "execution_attempts WHERE attempt_id=?)",
                    (
                        (
                            "retry-decision-pending"
                            if status == "decision-pending"
                            else "failed"
                        ),
                        error or "transport-terminal-failure",
                        now,
                        row["attempt_id"],
                    ),
                )
            self._event(
                conn,
                "transport-delivery-" + status,
                "transport-outbox",
                outbox_id,
                {
                    "consumer": consumer,
                    "error": error,
                    "receipt": receipt_value,
                    "next_attempt_at": next_attempt_at,
                    "failure": failure or {},
                    "delivery_attempts": int(row["delivery_attempts"]),
                },
            )
            current = conn.execute(
                "SELECT * FROM transport_outbox WHERE outbox_id=?",
                (outbox_id,),
            ).fetchone()
        return decode_outbox_row(current)

    def reconcile_transport_acceptance(
        self,
        outbox_id: str,
        receipt: dict[str, Any],
    ) -> dict[str, Any]:
        """Promote an uncertain delivery after endpoint-visible acceptance."""
        self.initialize()
        now = utc_now()
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM transport_outbox WHERE outbox_id=?",
                (outbox_id,),
            ).fetchone()
            if row is None:
                raise ControlDatabaseError(
                    f"unknown transport outbox: {outbox_id}"
                )
            if row["state"] not in {"uncertain", "accepted", "returning"}:
                raise ControlDatabaseError(
                    "transport acceptance cannot be reconciled from state "
                    f"{row['state']}"
                )
            payload = json.loads(str(row["payload_json"]))
            _validate_transport_identity(payload, receipt)
            if row["state"] == "uncertain":
                conn.execute(
                    "UPDATE transport_outbox SET state='accepted', "
                    "remote_receipt_json=?, accepted_at=?, next_poll_at=?, "
                    "poll_attempts=0, query_inflight_sequence=0, error='', "
                    "updated_at=? "
                    "WHERE outbox_id=?",
                    (canonical_json(receipt), now, now, now, outbox_id),
                )
                conn.execute(
                    "UPDATE execution_attempts SET state='accepted', error='', "
                    "updated_at=? WHERE attempt_id=?",
                    (now, row["attempt_id"]),
                )
                self._event(
                    conn,
                    "transport-acceptance-reconciled",
                    "transport-outbox",
                    outbox_id,
                    receipt,
                )
            current = conn.execute(
                "SELECT * FROM transport_outbox WHERE outbox_id=?",
                (outbox_id,),
            ).fetchone()
        return decode_outbox_row(current)

    def transport_pollable(
        self,
        *,
        endpoint_id: str = "",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        self.initialize()
        now = utc_now()
        parameters: list[Any] = [now]
        endpoint_clause = ""
        if endpoint_id:
            endpoint_clause = " AND endpoint_id=?"
            parameters.append(endpoint_id)
        parameters.append(max(0, int(limit)))
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM transport_outbox "
                "WHERE state IN ('accepted','uncertain','returning')"
                " AND (next_poll_at='' OR next_poll_at<=?)"
                + endpoint_clause
                + " ORDER BY updated_at, outbox_id LIMIT ?",
                tuple(parameters),
            ).fetchall()
        return [decode_outbox_row(row) for row in rows]

    def record_transport_poll(
        self,
        outbox_id: str,
        *,
        progressed: bool,
        query_completed: bool = False,
        error: str = "",
        min_interval_seconds: float = 0.05,
        max_interval_seconds: float = 1.0,
    ) -> dict[str, Any]:
        self.initialize()
        minimum = max(0.01, float(min_interval_seconds))
        maximum = max(minimum, float(max_interval_seconds))
        now_dt = datetime.now(timezone.utc)
        now = _format_timestamp(now_dt)
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM transport_outbox WHERE outbox_id=?",
                (outbox_id,),
            ).fetchone()
            if row is None:
                raise ControlDatabaseError(
                    f"unknown transport outbox: {outbox_id}"
                )
            state = str(row["state"])
            if state not in {"accepted", "uncertain", "returning"}:
                return decode_outbox_row(row)
            attempts = (
                0
                if progressed
                else min(30, int(row["poll_attempts"] or 0) + 1)
            )
            if state == "returning":
                next_poll_at = ""
            else:
                delay = (
                    minimum
                    if progressed
                    else min(maximum, minimum * (2 ** min(attempts, 10)))
                )
                next_poll_at = _format_timestamp(
                    now_dt + timedelta(seconds=delay)
                )
            conn.execute(
                "UPDATE transport_outbox SET poll_attempts=?, "
                "last_polled_at=?, next_poll_at=?, error=CASE "
                "WHEN ?=1 THEN '' WHEN ?='' THEN error ELSE ? END, "
                "query_inflight_sequence=CASE WHEN ?=1 THEN 0 "
                "ELSE query_inflight_sequence END, updated_at=? "
                "WHERE outbox_id=?",
                (
                    attempts,
                    now,
                    next_poll_at,
                    1 if progressed else 0,
                    error,
                    error,
                    1 if query_completed else 0,
                    now,
                    outbox_id,
                ),
            )
            if error:
                self._event(
                    conn,
                    "transport-poll-error",
                    "transport-outbox",
                    outbox_id,
                    {
                        "error": error,
                        "poll_attempts": attempts,
                        "next_poll_at": next_poll_at,
                    },
                )
            current = conn.execute(
                "SELECT * FROM transport_outbox WHERE outbox_id=?",
                (outbox_id,),
            ).fetchone()
        return decode_outbox_row(current)

    def reserve_transport_query(self, outbox_id: str) -> int:
        """Reserve one durable query identity; uncertainty reuses it."""
        self.initialize()
        now = utc_now()
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM transport_outbox WHERE outbox_id=?",
                (outbox_id,),
            ).fetchone()
            if row is None:
                raise ControlDatabaseError(
                    f"unknown transport outbox: {outbox_id}"
                )
            if str(row["state"]) not in {"accepted", "uncertain", "returning"}:
                raise ControlDatabaseError(
                    "transport query cannot be reserved from state "
                    f"{row['state']}"
                )
            sequence = int(row["query_inflight_sequence"] or 0)
            if sequence > 0 and str(row["state"]) == "accepted":
                query_event = conn.execute(
                    "SELECT sequence FROM control_events WHERE "
                    "entity_type='transport-outbox' AND entity_id=? AND "
                    "event_type='transport-query-reserved' "
                    "ORDER BY sequence DESC LIMIT 1",
                    (outbox_id,),
                ).fetchone()
                accepted_event = conn.execute(
                    "SELECT sequence FROM control_events WHERE "
                    "entity_type='transport-outbox' AND entity_id=? AND "
                    "event_type IN ('transport-delivery-accepted', "
                    "'transport-acceptance-reconciled') "
                    "ORDER BY sequence DESC LIMIT 1",
                    (outbox_id,),
                ).fetchone()
                if (
                    query_event is not None
                    and accepted_event is not None
                    and int(accepted_event["sequence"])
                    > int(query_event["sequence"])
                ):
                    sequence = 0
                    conn.execute(
                        "UPDATE transport_outbox SET "
                        "query_inflight_sequence=0, updated_at=? "
                        "WHERE outbox_id=?",
                        (now, outbox_id),
                    )
                    self._event(
                        conn,
                        "transport-query-superseded-by-acceptance",
                        "transport-outbox",
                        outbox_id,
                        {
                            "accepted_event_sequence": int(
                                accepted_event["sequence"]
                            ),
                            "query_event_sequence": int(
                                query_event["sequence"]
                            ),
                        },
                    )
            if sequence <= 0:
                sequence = int(row["query_sequence"] or 0) + 1
                conn.execute(
                    "UPDATE transport_outbox SET query_sequence=?, "
                    "query_inflight_sequence=?, updated_at=? WHERE outbox_id=?",
                    (sequence, sequence, now, outbox_id),
                )
                self._event(
                    conn,
                    "transport-query-reserved",
                    "transport-outbox",
                    outbox_id,
                    {"query_sequence": sequence},
                )
        return sequence
    def transport_endpoint_counts(self, endpoint_id: str) -> dict[str, int]:
        self.initialize()
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT state, COUNT(*) AS count FROM transport_outbox "
                "WHERE endpoint_id=? GROUP BY state ORDER BY state",
                (endpoint_id,),
            ).fetchall()
        counts = {str(row["state"]): int(row["count"]) for row in rows}
        counts["active"] = sum(
            count
            for state, count in counts.items()
            if state in OUTBOX_ACTIVE_STATES
        )
        return counts

    def pending_transport_returns(
        self,
        *,
        endpoint_id: str = "",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        self.initialize()
        parameters: list[Any] = []
        endpoint_clause = ""
        if endpoint_id:
            endpoint_clause = " AND endpoint_id=?"
            parameters.append(endpoint_id)
        parameters.append(max(0, int(limit)))
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM transport_returns WHERE state='received'"
                + endpoint_clause
                + " ORDER BY received_at, return_id LIMIT ?",
                tuple(parameters),
            ).fetchall()
        return [decode_return_row(row) for row in rows]

    def transport_outbox(self, outbox_id: str) -> dict[str, Any]:
        self.initialize()
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM transport_outbox WHERE outbox_id=?",
                (outbox_id,),
            ).fetchone()
        if row is None:
            raise ControlDatabaseError(
                f"unknown transport outbox: {outbox_id}"
            )
        return decode_outbox_row(row)

    def record_transport_return(
        self,
        outbox_id: str,
        result: dict[str, Any],
    ) -> dict[str, Any]:
        self.initialize()
        now = utc_now()
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM transport_outbox WHERE outbox_id=?",
                (outbox_id,),
            ).fetchone()
            if row is None:
                raise ControlDatabaseError(
                    f"unknown transport outbox: {outbox_id}"
                )
            if row["state"] not in {"accepted", "uncertain", "returning"}:
                raise ControlDatabaseError(
                    f"transport return not allowed from state {row['state']}"
                )
            payload = json.loads(str(row["payload_json"]))
            _validate_transport_identity(payload, result, require_receipt_id=True)
            receipt_id = str(result["receipt_id"])
            return_id = "return-" + uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"{row['endpoint_id']}:{receipt_id}",
            ).hex
            existing = conn.execute(
                "SELECT * FROM transport_returns "
                "WHERE endpoint_id=? AND receipt_id=?",
                (row["endpoint_id"], receipt_id),
            ).fetchone()
            result_json = canonical_json(result)
            if existing is not None:
                if (
                    str(existing["attempt_id"]) != str(row["attempt_id"])
                    or str(existing["payload_json"]) != result_json
                ):
                    raise ControlDatabaseError(
                        "transport return receipt identity collision"
                    )
                return decode_return_row(existing)
            conn.execute(
                """
                INSERT INTO transport_returns(
                    return_id, attempt_id, outbox_id, endpoint_id,
                    endpoint_generation, receipt_id, state, payload_json,
                    received_at
                ) VALUES(?, ?, ?, ?, ?, ?, 'received', ?, ?)
                """,
                (
                    return_id,
                    row["attempt_id"],
                    outbox_id,
                    row["endpoint_id"],
                    str(result["target_generation"]),
                    receipt_id,
                    result_json,
                    now,
                ),
            )
            conn.execute(
                "UPDATE transport_outbox SET state='returning', updated_at=? "
                "WHERE outbox_id=?",
                (now, outbox_id),
            )
            conn.execute(
                "UPDATE execution_attempts SET state='returning', updated_at=? "
                "WHERE attempt_id=?",
                (now, row["attempt_id"]),
            )
            self._event(
                conn,
                "transport-return-received",
                "transport-return",
                return_id,
                result,
            )
            current = conn.execute(
                "SELECT * FROM transport_returns WHERE return_id=?",
                (return_id,),
            ).fetchone()
        return decode_return_row(current)

    def acknowledge_transport_return(
        self,
        return_id: str,
        *,
        receipt_id: str,
    ) -> dict[str, Any]:
        self.initialize()
        now = utc_now()
        with self.transaction() as conn:
            returned = conn.execute(
                "SELECT * FROM transport_returns WHERE return_id=?",
                (return_id,),
            ).fetchone()
            if returned is None:
                raise ControlDatabaseError(
                    f"unknown transport return: {return_id}"
                )
            if str(returned["receipt_id"]) != receipt_id:
                raise ControlDatabaseError(
                    f"transport return receipt mismatch: {return_id}"
                )
            if returned["state"] == "acknowledged":
                return decode_return_row(returned)
            if returned["state"] != "received":
                raise ControlDatabaseError(
                    f"transport return cannot be acknowledged from "
                    f"{returned['state']}"
                )
            conn.execute(
                "UPDATE transport_returns SET state='acknowledged', "
                "acknowledged_at=? WHERE return_id=?",
                (now, return_id),
            )
            conn.execute(
                "UPDATE transport_outbox SET state='completed', completed_at=?, "
                "updated_at=? WHERE outbox_id=?",
                (now, now, returned["outbox_id"]),
            )
            result = json.loads(str(returned["payload_json"]))
            outcome = str(result.get("outcome") or "success").lower()
            succeeded = outcome in {"success", "completed", "passed"}
            error = str(result.get("error") or "")
            attempt_state = "completed" if succeeded else "failed"
            request_state = "completed" if succeeded else "failed"
            blocker = "" if succeeded else (error or f"transport-result:{outcome}")
            conn.execute(
                "UPDATE execution_attempts SET state=?, error=?, updated_at=? "
                "WHERE attempt_id=?",
                (attempt_state, blocker, now, returned["attempt_id"]),
            )
            conn.execute(
                "UPDATE test_requests SET state=?, blocker=?, updated_at=? "
                "WHERE request_id=(SELECT request_id FROM execution_attempts "
                "WHERE attempt_id=?)",
                (request_state, blocker, now, returned["attempt_id"]),
            )
            self._event(
                conn,
                "transport-return-acknowledged",
                "transport-return",
                return_id,
                {"receipt_id": receipt_id},
            )
            current = conn.execute(
                "SELECT * FROM transport_returns WHERE return_id=?",
                (return_id,),
            ).fetchone()
        return decode_return_row(current)
