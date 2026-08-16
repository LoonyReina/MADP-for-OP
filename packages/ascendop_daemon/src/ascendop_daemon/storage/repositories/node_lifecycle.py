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

class NodeLifecycleRepository:
    def status(self, *, event_limit: int = 20) -> dict[str, Any]:
        self.initialize()
        with self.connection() as conn:
            now = utc_now()
            active_placeholders = ",".join("?" for _ in ACTIVE_NODE_STATES)
            conn.execute(
                f"UPDATE observed_nodes SET state='expired', updated_at=? "
                f"WHERE state IN ({active_placeholders}) AND lease_expires_at < ?",
                (now, *sorted(ACTIVE_NODE_STATES), now),
            )
            conn.execute(
                f"UPDATE node_sessions SET state='expired', updated_at=? "
                f"WHERE state IN ({active_placeholders}) AND lease_expires_at < ?",
                (now, *sorted(ACTIVE_NODE_STATES), now),
            )
            operator_rows = conn.execute(
                "SELECT desired_state, COUNT(*) AS count FROM operator_registrations "
                "GROUP BY desired_state ORDER BY desired_state"
            ).fetchall()
            endpoint_rows = conn.execute(
                "SELECT enabled, draining, COUNT(*) AS count FROM backend_endpoints "
                "GROUP BY enabled, draining ORDER BY enabled DESC, draining"
            ).fetchall()
            gateway_rows = conn.execute(
                "SELECT enabled, draining, COUNT(*) AS count FROM transport_gateways "
                "GROUP BY enabled, draining ORDER BY enabled DESC, draining"
            ).fetchall()
            desired_node_rows = conn.execute(
                "SELECT enabled, draining, COUNT(*) AS count "
                "FROM execution_nodes_desired "
                "GROUP BY enabled, draining ORDER BY enabled DESC, draining"
            ).fetchall()
            environment_rows = conn.execute(
                "SELECT enabled, draining, COUNT(*) AS count "
                "FROM execution_environments "
                "GROUP BY enabled, draining ORDER BY enabled DESC, draining"
            ).fetchall()
            request_rows = conn.execute(
                "SELECT state, COUNT(*) AS count FROM test_requests "
                "GROUP BY state ORDER BY state"
            ).fetchall()
            attempt_rows = conn.execute(
                "SELECT state, COUNT(*) AS count FROM execution_attempts "
                "GROUP BY state ORDER BY state"
            ).fetchall()
            preparation_rows = conn.execute(
                "SELECT state, COUNT(*) AS count FROM request_preparations "
                "GROUP BY state ORDER BY state"
            ).fetchall()
            outbox_rows = conn.execute(
                "SELECT state, COUNT(*) AS count FROM transport_outbox "
                "GROUP BY state ORDER BY state"
            ).fetchall()
            return_rows = conn.execute(
                "SELECT state, COUNT(*) AS count FROM transport_returns "
                "GROUP BY state ORDER BY state"
            ).fetchall()
            node_rows = conn.execute(
                "SELECT admission_state, state, COUNT(*) AS count FROM observed_nodes "
                "GROUP BY admission_state, state ORDER BY admission_state, state"
            ).fetchall()
            assistant_action_rows = conn.execute(
                "SELECT state, COUNT(*) AS count FROM assistant_action_requests "
                "GROUP BY state ORDER BY state"
            ).fetchall()
            events = conn.execute(
                "SELECT * FROM control_events ORDER BY sequence DESC LIMIT ?",
                (max(0, int(event_limit)),),
            ).fetchall()
        return {
            "schema_version": SCHEMA_VERSION,
            "database": str(self.path),
            "operators": {row["desired_state"]: row["count"] for row in operator_rows},
            "gateways": [dict(row) for row in gateway_rows],
            "desired_nodes": [dict(row) for row in desired_node_rows],
            "environments": [dict(row) for row in environment_rows],
            "endpoints": [dict(row) for row in endpoint_rows],
            "requests": {row["state"]: row["count"] for row in request_rows},
            "attempts": {row["state"]: row["count"] for row in attempt_rows},
            "preparations": {
                row["state"]: row["count"] for row in preparation_rows
            },
            "outbox": {row["state"]: row["count"] for row in outbox_rows},
            "returns": {row["state"]: row["count"] for row in return_rows},
            "nodes": [dict(row) for row in node_rows],
            "assistant_actions": {
                row["state"]: row["count"] for row in assistant_action_rows
            },
            "services": self.service_health(),
            "events": [
                {
                    **dict(row),
                    "payload": json.loads(row["payload_json"]),
                }
                for row in events
            ],
        }

    def live_node_capabilities(self, node_id: str) -> dict[str, Any]:
        self.initialize()
        with self.connection() as conn:
            row = conn.execute(
                "SELECT state, lease_expires_at, report_json "
                "FROM observed_nodes WHERE node_id=?",
                (str(node_id),),
            ).fetchone()
        if row is None or str(row["state"]) not in ACTIVE_NODE_STATES:
            return {}
        try:
            lease_expires_at = _parse_timestamp(
                str(row["lease_expires_at"]), "lease_expires_at"
            )
        except ControlDatabaseError:
            return {}
        if lease_expires_at < datetime.now(timezone.utc):
            return {}
        try:
            report = json.loads(str(row["report_json"]))
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        capabilities = report.get("capabilities", {}) if isinstance(report, dict) else {}
        return dict(capabilities) if isinstance(capabilities, dict) else {}

    def observed_node_liveness(self, node_id: str) -> dict[str, Any]:
        """Return liveness projected from the local receipt clock.

        A remote node's UTC clock is diagnostic metadata, not a trustworthy
        lease clock. ``ingest_node_report`` normalizes each new report sequence
        to local receipt time; repeated reads of the same sequence do not renew
        that lease.
        """

        self.initialize()
        with self.connection() as conn:
            row = conn.execute(
                "SELECT admission_state, state, heartbeat_at, lease_expires_at, "
                "updated_at, report_json FROM observed_nodes WHERE node_id=?",
                (str(node_id),),
            ).fetchone()
        if row is None:
            return {"live": False, "reason": "node-not-observed"}
        state = str(row["state"])
        try:
            lease_expires_at = _parse_timestamp(
                str(row["lease_expires_at"]), "lease_expires_at"
            )
        except ControlDatabaseError:
            return {
                "live": False,
                "reason": "invalid-local-lease",
                "state": state,
            }
        live = (
            state in ACTIVE_NODE_STATES
            and lease_expires_at >= datetime.now(timezone.utc)
        )
        try:
            report = json.loads(str(row["report_json"]))
        except (TypeError, ValueError, json.JSONDecodeError):
            report = {}
            live = False
        return {
            "live": live,
            "reason": "live" if live else "local-observation-stale",
            "admission_state": str(row["admission_state"]),
            "state": state,
            "heartbeat_at": str(row["heartbeat_at"]),
            "lease_expires_at": str(row["lease_expires_at"]),
            "updated_at": str(row["updated_at"]),
            "report": report if isinstance(report, dict) else {},
        }

    def last_known_node_capabilities(self, node_id: str) -> dict[str, Any]:
        """Return durable topology facts even when the node lease is offline."""

        self.initialize()
        with self.connection() as conn:
            row = conn.execute(
                "SELECT capability_json FROM node_capabilities "
                "WHERE node_id=? ORDER BY observed_at DESC, rowid DESC LIMIT 1",
                (str(node_id),),
            ).fetchone()
        if row is None:
            return {}
        try:
            capabilities = json.loads(str(row["capability_json"]))
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        return dict(capabilities) if isinstance(capabilities, dict) else {}

    def ingest_node_report(
        self,
        report: dict[str, Any],
        *,
        source: str,
    ) -> dict[str, Any]:
        self.initialize()
        _validate_node_report(report)
        node_id = str(report["node_id"])
        session_id = str(report["session_id"])
        sequence = int(report["sequence"])
        now = utc_now()
        (
            normalized_heartbeat_at,
            normalized_lease_expires_at,
            source_clock_skew_seconds,
        ) = _normalized_node_lease(report)
        report_json = canonical_json(report)
        capabilities = report.get("capabilities", {})
        capability_generation = str(report.get("capability_generation") or "")
        with self.transaction() as conn:
            current_session = conn.execute(
                "SELECT * FROM node_sessions WHERE session_id=?", (session_id,)
            ).fetchone()
            if current_session is not None and int(current_session["sequence"]) >= sequence:
                return {
                    "node_id": node_id,
                    "session_id": session_id,
                    "state": current_session["state"],
                    "idempotent": True,
                    "stale_sequence": int(current_session["sequence"]) > sequence,
                }
            current_node = conn.execute(
                "SELECT * FROM observed_nodes WHERE node_id=?", (node_id,)
            ).fetchone()
            admission_state = (
                str(current_node["admission_state"])
                if current_node is not None
                else "discovered"
            )
            if current_node is not None and (
                str(current_node["endpoint_id"]) != str(report["endpoint_id"])
                or str(current_node["generation"]) != str(report["generation"])
            ):
                admission_state = "discovered"
            replace_current = current_node is None or (
                str(current_node["current_session_id"]) == session_id
                or _node_report_is_newer(conn, current_node, report)
            )
            if current_node is None:
                conn.execute(
                    """
                    INSERT INTO observed_nodes(
                        node_id, endpoint_id, generation, admission_state,
                        current_session_id, state, capability_generation,
                        heartbeat_at, lease_expires_at, report_json, source, updated_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        node_id,
                        str(report["endpoint_id"]),
                        str(report["generation"]),
                        admission_state,
                        session_id,
                        str(report["state"]),
                        capability_generation,
                        normalized_heartbeat_at,
                        normalized_lease_expires_at,
                        report_json,
                        source,
                        now,
                    ),
                )
            elif replace_current:
                conn.execute(
                    """
                    UPDATE observed_nodes SET
                        endpoint_id=?, generation=?, admission_state=?,
                        current_session_id=?, state=?,
                        capability_generation=?, heartbeat_at=?, lease_expires_at=?,
                        report_json=?, source=?, updated_at=?
                    WHERE node_id=?
                    """,
                    (
                        str(report["endpoint_id"]),
                        str(report["generation"]),
                        admission_state,
                        session_id,
                        str(report["state"]),
                        capability_generation,
                        normalized_heartbeat_at,
                        normalized_lease_expires_at,
                        report_json,
                        source,
                        now,
                        node_id,
                    ),
                )
            conn.execute(
                """
                INSERT INTO node_sessions(
                    session_id, node_id, boot_id, generation, state, sequence,
                    started_at, heartbeat_at, lease_expires_at, reason,
                    report_json, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    state=excluded.state,
                    sequence=excluded.sequence,
                    heartbeat_at=excluded.heartbeat_at,
                    lease_expires_at=excluded.lease_expires_at,
                    reason=excluded.reason,
                    report_json=excluded.report_json,
                    updated_at=excluded.updated_at
                WHERE excluded.sequence >= node_sessions.sequence
                """,
                (
                    session_id,
                    node_id,
                    str(report["boot_id"]),
                    str(report["generation"]),
                    str(report["state"]),
                    sequence,
                    str(report["started_at"]),
                    normalized_heartbeat_at,
                    normalized_lease_expires_at,
                    str(report.get("reason") or ""),
                    report_json,
                    now,
                ),
            )
            if capability_generation and isinstance(capabilities, dict):
                conn.execute(
                    """
                    INSERT INTO node_capabilities(
                        node_id, capability_generation, capability_json, observed_at
                    ) VALUES(?, ?, ?, ?)
                    ON CONFLICT(node_id, capability_generation) DO NOTHING
                    """,
                    (
                        node_id,
                        capability_generation,
                        canonical_json(capabilities),
                        str(capabilities.get("observed_at") or report["heartbeat_at"]),
                    ),
                )
            self._event(
                conn,
                "node-report-ingested",
                "node-session",
                session_id,
                {
                    "node_id": node_id,
                    "state": report["state"],
                    "sequence": sequence,
                    "replace_current": replace_current,
                    "source": source,
                    "source_clock_skew_seconds": source_clock_skew_seconds,
                },
            )
        return {
            "node_id": node_id,
            "session_id": session_id,
            "state": report["state"],
            "admission_state": admission_state,
            "current_session": replace_current,
            "idempotent": current_session is not None,
        }

    def accept_node(
        self,
        node_id: str,
        registry: SystemRegistry | None = None,
    ) -> dict[str, Any]:
        self.initialize()
        now = utc_now()
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM observed_nodes WHERE node_id=?", (node_id,)
            ).fetchone()
            if row is None:
                raise ControlDatabaseError(f"node has not been discovered: {node_id}")
            if registry is not None and registry.schema == SYSTEM_REGISTRY_SCHEMA:
                _validate_observed_node_against_registry(row, registry)
            conn.execute(
                "UPDATE observed_nodes SET admission_state='accepted', updated_at=? "
                "WHERE node_id=?",
                (now, node_id),
            )
            ack = {
                "schema": "ascendop.node-ack.v1",
                "state": "accepted",
                "node_id": node_id,
                "endpoint_id": row["endpoint_id"],
                "generation": row["generation"],
                "session_id": row["current_session_id"],
                "accepted_at": now,
            }
            self._event(conn, "node-accepted", "node", node_id, ack)
        return ack

    def renew_node_lease_after_trusted_probe(
        self,
        node_id: str,
        registry: SystemRegistry,
        *,
        source: str,
        lease_seconds: int = 120,
    ) -> dict[str, Any]:
        self.initialize()
        now_dt = datetime.now(timezone.utc)
        now = now_dt.isoformat()
        lease_expires_at = (
            now_dt + timedelta(seconds=max(30, int(lease_seconds)))
        ).isoformat()
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM observed_nodes WHERE node_id=?",
                (node_id,),
            ).fetchone()
            if row is None:
                raise ControlDatabaseError(
                    f"node has not been discovered: {node_id}"
                )
            if str(row["admission_state"]) != "accepted":
                raise ControlDatabaseError(
                    f"trusted probe cannot admit an unaccepted node: {node_id}"
                )
            _validate_observed_node_against_registry(
                row,
                registry,
                require_live_lease=False,
            )
            session_id = str(row["current_session_id"])
            conn.execute(
                """
                UPDATE observed_nodes SET
                    state='ready', heartbeat_at=?, lease_expires_at=?,
                    source=?, updated_at=?
                WHERE node_id=?
                """,
                (now, lease_expires_at, source, now, node_id),
            )
            conn.execute(
                """
                UPDATE node_sessions SET
                    state='ready', heartbeat_at=?, lease_expires_at=?, updated_at=?
                WHERE session_id=?
                """,
                (now, lease_expires_at, now, session_id),
            )
            ack = {
                "schema": "ascendop.node-ack.v1",
                "state": "accepted",
                "node_id": node_id,
                "endpoint_id": str(row["endpoint_id"]),
                "generation": str(row["generation"]),
                "session_id": session_id,
                "accepted_at": now,
            }
            self._event(
                conn,
                "node-lease-renewed-by-trusted-probe",
                "node",
                node_id,
                {
                    **ack,
                    "lease_expires_at": lease_expires_at,
                    "source": source,
                },
            )
        return ack

    def current_node_ack(self, node_id: str) -> dict[str, Any] | None:
        self.initialize()
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM observed_nodes WHERE node_id=?",
                (node_id,),
            ).fetchone()
            if row is None or row["admission_state"] != "accepted":
                return None
            if str(row["state"]) not in ACTIVE_NODE_STATES:
                return None
            if _parse_timestamp(
                str(row["lease_expires_at"]), "lease_expires_at"
            ) <= datetime.now(timezone.utc):
                return None
            return {
                "schema": "ascendop.node-ack.v1",
                "state": "accepted",
                "node_id": node_id,
                "endpoint_id": str(row["endpoint_id"]),
                "generation": str(row["generation"]),
                "session_id": str(row["current_session_id"]),
                "accepted_at": str(row["updated_at"]),
            }
