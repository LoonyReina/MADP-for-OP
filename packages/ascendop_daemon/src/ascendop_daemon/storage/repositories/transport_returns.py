from __future__ import annotations

import json
import uuid
from typing import Any

from ascendop_daemon.storage.control_types import ControlDatabaseError
from ascendop_daemon.storage.control_validation import (
    _validate_transport_identity,
    canonical_json,
    utc_now,
)
from ascendop_daemon.storage.row_decoders import (
    decode_outbox_row,
    decode_return_row,
)


class TransportReturnRepository:
    def acknowledged_unprojected_transport_returns(
        self,
        *,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Return hard-cut history that was ACKed before workspace projection."""

        self.initialize()
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT returned.*, request.operator_id, request.test_version
                FROM transport_returns AS returned
                JOIN execution_attempts AS attempt
                  ON attempt.attempt_id=returned.attempt_id
                JOIN test_requests AS request
                  ON request.request_id=attempt.request_id
                WHERE returned.state='acknowledged'
                  AND returned.projection_json='{}'
                ORDER BY returned.received_at DESC, returned.return_id DESC
                LIMIT ?
                """,
                (max(0, int(limit)),),
            ).fetchall()
        values: list[dict[str, Any]] = []
        for row in rows:
            value = decode_return_row(row)
            value["operator_id"] = str(row["operator_id"])
            value["test_version"] = str(row["test_version"])
            values.append(value)
        return values

    def backfill_acknowledged_transport_projection(
        self,
        return_id: str,
        projection: dict[str, Any],
        *,
        disposition: str,
    ) -> dict[str, Any]:
        """Attach a deterministic projection to an already ACKed legacy return."""

        self.initialize()
        allowed = {
            "backfilled-after-ack",
            "superseded-logical-result",
        }
        if disposition not in allowed:
            raise ControlDatabaseError(
                f"unsupported acknowledged projection disposition: {disposition}"
            )
        with self.transaction() as conn:
            returned = conn.execute(
                "SELECT * FROM transport_returns WHERE return_id=?",
                (return_id,),
            ).fetchone()
            if returned is None:
                raise ControlDatabaseError(f"unknown transport return: {return_id}")
            projection_json = canonical_json(projection)
            existing = str(returned["projection_json"] or "{}")
            if existing != "{}":
                if existing != projection_json:
                    raise ControlDatabaseError(
                        f"acknowledged projection identity collision: {return_id}"
                    )
                return decode_return_row(returned)
            if str(returned["state"]) != "acknowledged":
                raise ControlDatabaseError(
                    "projection backfill requires an acknowledged return"
                )
            conn.execute(
                "UPDATE transport_returns SET projection_json=?, disposition=? "
                "WHERE return_id=?",
                (projection_json, disposition, return_id),
            )
            self._event(
                conn,
                "transport-return-projection-backfilled",
                "transport-return",
                return_id,
                {"disposition": disposition, "projection": projection},
            )
            current = conn.execute(
                "SELECT * FROM transport_returns WHERE return_id=?",
                (return_id,),
            ).fetchone()
        return decode_return_row(current)

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
                "SELECT * FROM transport_returns WHERE state IN ("
                "'received','projected-ready','recovery-unrecoverable')"
                + endpoint_clause
                + " ORDER BY received_at, return_id LIMIT ?",
                tuple(parameters),
            ).fetchall()
        return [decode_return_row(row) for row in rows]

    def record_transport_projection(
        self,
        return_id: str,
        projection: dict[str, Any],
    ) -> dict[str, Any]:
        """Persist result projection before deciding whether remote ACK is safe."""

        self.initialize()
        now = utc_now()
        with self.transaction() as conn:
            returned = conn.execute(
                "SELECT * FROM transport_returns WHERE return_id=?",
                (return_id,),
            ).fetchone()
            if returned is None:
                raise ControlDatabaseError(f"unknown transport return: {return_id}")
            projection_json = canonical_json(projection)
            existing_projection = str(returned["projection_json"] or "{}")
            if existing_projection != "{}" and existing_projection != projection_json:
                raise ControlDatabaseError(
                    f"transport return projection identity collision: {return_id}"
                )
            if returned["state"] in {
                "projected-ready",
                "recovery-decision-pending",
                "recovery-superseded",
                "recovery-unrecoverable",
                "acknowledged",
            }:
                return decode_return_row(returned)
            if returned["state"] != "received":
                raise ControlDatabaseError(
                    f"transport return cannot be projected from {returned['state']}"
                )
            evidence = projection.get("stage_evidence", {})
            evidence = evidence if isinstance(evidence, dict) else {}
            contract = evidence.get("postprocess_recovery_contract", {})
            contract = contract if isinstance(contract, dict) else {}
            recovery_required = bool(
                evidence.get("postprocess_recovery_required") and contract
            )
            terminal_revision = int(
                projection.get("terminal_revision", 0) or 0
            )
            if recovery_required:
                recovery_id = "recovery-" + uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"{return_id}:{contract.get('terminal_digest', '')}",
                ).hex
                required_contract = {
                    "engine_job_id": str(contract.get("engine_job_id") or ""),
                    "terminal_digest": str(contract.get("terminal_digest") or ""),
                    "terminal_revision": int(
                        contract.get("terminal_revision", 0) or 0
                    ),
                    "stages": list(contract.get("stages", [])),
                    "max_stage_attempts": int(
                        contract.get("max_stage_attempts", 0) or 0
                    ),
                }
                if (
                    not required_contract["engine_job_id"]
                    or len(required_contract["terminal_digest"]) != 64
                    or not required_contract["stages"]
                    or required_contract["max_stage_attempts"] != 1
                ):
                    raise ControlDatabaseError(
                        "postprocess recovery projection has invalid contract"
                    )
                conn.execute(
                    """
                    INSERT INTO postprocess_recoveries(
                        recovery_id, return_id, outbox_id, attempt_id,
                        endpoint_id, endpoint_generation, engine_job_id,
                        terminal_digest, terminal_revision, stages_json, state,
                        created_at, updated_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                             'decision-pending', ?, ?)
                    ON CONFLICT(recovery_id) DO NOTHING
                    """,
                    (
                        recovery_id,
                        return_id,
                        returned["outbox_id"],
                        returned["attempt_id"],
                        returned["endpoint_id"],
                        returned["endpoint_generation"],
                        required_contract["engine_job_id"],
                        required_contract["terminal_digest"],
                        required_contract["terminal_revision"],
                        canonical_json(required_contract["stages"]),
                        now,
                        now,
                    ),
                )
                state = "recovery-decision-pending"
                disposition = "hold-for-postprocess-recovery"
                hold_reason = (
                    "device facts are complete; retry only immutable host/export "
                    "postprocess stages before return acknowledgement"
                )
            else:
                recovery_id = ""
                state = "projected-ready"
                disposition = "ready-to-ack"
                hold_reason = ""
            conn.execute(
                "UPDATE transport_returns SET state=?, projection_json=?, "
                "disposition=?, hold_reason=?, terminal_revision=?, recovery_id=? "
                "WHERE return_id=?",
                (
                    state,
                    projection_json,
                    disposition,
                    hold_reason,
                    terminal_revision,
                    recovery_id,
                    return_id,
                ),
            )
            self._event(
                conn,
                "transport-return-projected",
                "transport-return",
                return_id,
                {
                    "disposition": disposition,
                    "recovery_id": recovery_id,
                    "terminal_revision": terminal_revision,
                    "projection": projection,
                },
            )
            current = conn.execute(
                "SELECT * FROM transport_returns WHERE return_id=?",
                (return_id,),
            ).fetchone()
        return decode_return_row(current)

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
            terminal_revision = int(result.get("terminal_revision", 0) or 0)
            if terminal_revision > 0:
                recovery = conn.execute(
                    "SELECT recovery_id FROM postprocess_recoveries "
                    "WHERE outbox_id=? AND state IN ('dispatched','uncertain') "
                    "AND terminal_revision<? ORDER BY created_at DESC LIMIT 1",
                    (outbox_id, terminal_revision),
                ).fetchone()
                if recovery is not None:
                    conn.execute(
                        "UPDATE postprocess_recoveries SET state='result-received', "
                        "updated_at=? WHERE recovery_id=?",
                        (now, recovery["recovery_id"]),
                    )
                    conn.execute(
                        "UPDATE transport_returns SET state='recovery-superseded' "
                        "WHERE outbox_id=? AND recovery_id=? AND state<> 'acknowledged'",
                        (outbox_id, recovery["recovery_id"]),
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
            if returned["state"] not in {
                "projected-ready",
                "recovery-unrecoverable",
            }:
                raise ControlDatabaseError(
                    f"transport return cannot be acknowledged from "
                    f"{returned['state']}"
                )
            conn.execute(
                "UPDATE transport_returns SET state='acknowledged', "
                "acknowledged_at=? WHERE return_id=?",
                (now, return_id),
            )
            recovery_id = str(returned["recovery_id"] or "")
            if recovery_id:
                conn.execute(
                    "UPDATE postprocess_recoveries SET state=CASE "
                    "WHEN state='result-received' THEN 'completed' ELSE state END, "
                    "updated_at=? WHERE recovery_id=?",
                    (now, recovery_id),
                )
            else:
                conn.execute(
                    "UPDATE postprocess_recoveries SET state='completed', "
                    "updated_at=? WHERE outbox_id=? AND state='result-received'",
                    (now, returned["outbox_id"]),
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
