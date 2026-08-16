from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

from ascendop_protocol.wire_v3 import canonical_digest, validate_envelope
from ascendop_daemon.core.filesystem import filesystem_path
from ascendop_daemon.registry.system_registry import SystemRegistry
from ascendop_daemon.storage.control_types import (
    ACTIVE_ATTEMPT_STATES,
    ControlDatabaseError,
)
from ascendop_daemon.storage.control_validation import (
    _is_bootstrap_control_probe,
    canonical_json,
    utc_now,
)
from ascendop_daemon.storage.row_decoders import (
    decode_attempt_row,
    decode_outbox_row,
    decode_preparation_row,
)


class RequestPreparationRepository:
    """Pre-publication reservations that never consume execution attempts."""

    def reserve_wire_v3_preparation(
        self,
        request_id: str,
        registry: SystemRegistry,
    ) -> dict[str, Any]:
        self.initialize()
        with self.transaction() as conn:
            request = conn.execute(
                "SELECT * FROM test_requests WHERE request_id=?",
                (request_id,),
            ).fetchone()
            if request is None:
                raise ControlDatabaseError(f"unknown TestRequest: {request_id}")

            attempts = conn.execute(
                "SELECT * FROM execution_attempts WHERE request_id=? "
                "ORDER BY ordinal DESC",
                (request_id,),
            ).fetchall()
            if str(request["state"]) in {
                "completed",
                "failed",
                "cancelled",
                "quarantined",
            }:
                latest = attempts[0] if attempts else None
                outbox = (
                    conn.execute(
                        "SELECT * FROM transport_outbox WHERE attempt_id=?",
                        (latest["attempt_id"],),
                    ).fetchone()
                    if latest is not None
                    else None
                )
                return {
                    "attempt": decode_attempt_row(latest) if latest is not None else None,
                    "preparation": None,
                    "route": (
                        json.loads(str(latest["route_json"]))
                        if latest is not None
                        else {}
                    ),
                    "outbox": (
                        decode_outbox_row(outbox)["payload"]
                        if outbox is not None
                        else None
                    ),
                    "request_state": str(request["state"]),
                    "terminal": True,
                    "idempotent": True,
                }
            for row in attempts:
                if str(row["state"]) in ACTIVE_ATTEMPT_STATES:
                    outbox = conn.execute(
                        "SELECT * FROM transport_outbox WHERE attempt_id=?",
                        (row["attempt_id"],),
                    ).fetchone()
                    return {
                        "attempt": decode_attempt_row(row),
                        "preparation": None,
                        "route": json.loads(str(row["route_json"])),
                        "outbox": (
                            decode_outbox_row(outbox)["payload"]
                            if outbox is not None
                            else None
                        ),
                        "idempotent": True,
                    }

            reserved = conn.execute(
                "SELECT * FROM request_preparations "
                "WHERE request_id=? AND state='reserved' "
                "ORDER BY created_at DESC LIMIT 1",
                (request_id,),
            ).fetchone()
            if reserved is not None:
                value = decode_preparation_row(reserved)
                return {
                    "attempt": None,
                    "preparation": value,
                    "route": value["route"],
                    "outbox": value["payload"],
                    "idempotent": True,
                }

            failed = conn.execute(
                "SELECT * FROM request_preparations "
                "WHERE request_id=? AND state='failed' "
                "ORDER BY updated_at DESC LIMIT 1",
                (request_id,),
            ).fetchone()
            if failed is not None:
                value = decode_preparation_row(failed)
                return {
                    "attempt": None,
                    "preparation": None,
                    "route": value["route"],
                    "outbox": value["payload"],
                    "blocker": "prepublication-retry-decision-required",
                    "request_state": "blocked",
                    "terminal": False,
                    "idempotent": True,
                }

            requirements = json.loads(str(request["requirements_json"]))
            decision = self._route_decision(
                conn,
                registry,
                requirements,
                allow_unobserved_node=_is_bootstrap_control_probe(
                    request,
                    requirements,
                ),
            )
            if decision.selected is None:
                now = utc_now()
                blocker = "no-compatible-backend"
                conn.execute(
                    "UPDATE test_requests SET state='blocked', blocker=?, "
                    "updated_at=? WHERE request_id=?",
                    (blocker, now, request_id),
                )
                self._event(
                    conn,
                    "test-request-route-blocked",
                    "test-request",
                    request_id,
                    decision.to_dict(),
                )
                return {
                    "attempt": None,
                    "preparation": None,
                    "route": decision.to_dict(),
                    "blocker": blocker,
                    "idempotent": False,
                }

            selected = decision.selected
            endpoint = conn.execute(
                "SELECT registration_generation, enabled, draining, "
                "source_present FROM backend_endpoints WHERE endpoint_id=?",
                (selected.endpoint_id,),
            ).fetchone()
            if (
                endpoint is None
                or not endpoint[1]
                or endpoint[2]
                or not endpoint[3]
            ):
                raise ControlDatabaseError(
                    "selected endpoint is not current in database: "
                    f"{selected.endpoint_id}"
                )
            if str(endpoint[0]) != selected.generation:
                raise ControlDatabaseError(
                    "selected endpoint generation drifted: "
                    f"{selected.endpoint_id}"
                )

            ordinal = int(attempts[0]["ordinal"] if attempts else 0) + 1
            attempt_id = f"{request_id}-a{ordinal:03d}"
            preparation_id = f"prep-{request_id}-{uuid.uuid4().hex[:12]}"
            route_value = decision.to_dict()
            payload = _routed_payload(
                request=request,
                selected=selected,
                attempt_id=attempt_id,
            )
            now = utc_now()
            conn.execute(
                """
                INSERT INTO request_preparations(
                    preparation_id, request_id, proposed_attempt_id,
                    proposed_ordinal, state, endpoint_id, endpoint_generation,
                    node_id, execution_environment_id, route_json, payload_json,
                    created_at, updated_at
                ) VALUES(?, ?, ?, ?, 'reserved', ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    preparation_id,
                    request_id,
                    attempt_id,
                    ordinal,
                    selected.endpoint_id,
                    selected.generation,
                    selected.node_id,
                    selected.execution_environment_id,
                    canonical_json(route_value),
                    canonical_json(payload),
                    now,
                    now,
                ),
            )
            conn.execute(
                "UPDATE test_requests SET state='preparing', blocker='', "
                "updated_at=? WHERE request_id=?",
                (now, request_id),
            )
            self._event(
                conn,
                "wire-v3-preparation-reserved",
                "request-preparation",
                preparation_id,
                payload,
            )
            row = conn.execute(
                "SELECT * FROM request_preparations WHERE preparation_id=?",
                (preparation_id,),
            ).fetchone()
        value = decode_preparation_row(row)
        return {
            "attempt": None,
            "preparation": value,
            "route": route_value,
            "outbox": payload,
            "idempotent": False,
        }

    def request_preparation(self, preparation_id: str) -> dict[str, Any]:
        self.initialize()
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM request_preparations WHERE preparation_id=?",
                (preparation_id,),
            ).fetchone()
        if row is None:
            raise ControlDatabaseError(
                f"unknown request preparation: {preparation_id}"
            )
        return decode_preparation_row(row)

    def publish_wire_v3_preparation(
        self,
        preparation_id: str,
        *,
        envelope: dict[str, Any],
        envelope_path: Path,
        package_root: Path,
    ) -> dict[str, Any]:
        validated = validate_envelope(envelope)
        digest = canonical_digest(validated.envelope)
        meta = validated.envelope["meta"]
        identity = validated.envelope["identity"]
        resolved_envelope = envelope_path.resolve()
        resolved_package = package_root.resolve()
        envelope_io = filesystem_path(resolved_envelope)
        package_io = filesystem_path(resolved_package)
        if not envelope_io.is_file():
            raise ControlDatabaseError(
                f"Wire V3 envelope is missing: {resolved_envelope}"
            )
        if not package_io.is_dir():
            raise ControlDatabaseError(
                f"Wire V3 package root is missing: {resolved_package}"
            )
        if resolved_package != resolved_envelope and resolved_package not in resolved_envelope.parents:
            raise ControlDatabaseError("Wire V3 envelope escapes its preparation root")
        persisted = json.loads(envelope_io.read_text(encoding="utf-8-sig"))
        if not isinstance(persisted, dict) or canonical_digest(persisted) != digest:
            raise ControlDatabaseError(
                "persisted Wire V3 envelope does not match the supplied envelope"
            )

        now = utc_now()
        with self.transaction() as conn:
            preparation = conn.execute(
                "SELECT * FROM request_preparations WHERE preparation_id=?",
                (preparation_id,),
            ).fetchone()
            if preparation is None:
                raise ControlDatabaseError(
                    f"unknown request preparation: {preparation_id}"
                )
            if str(preparation["state"]) == "published":
                outbox = conn.execute(
                    "SELECT * FROM transport_outbox WHERE attempt_id=?",
                    (preparation["proposed_attempt_id"],),
                ).fetchone()
                if outbox is None:
                    raise ControlDatabaseError(
                        "published preparation has no transport outbox"
                    )
                if str(outbox["envelope_digest"]) != digest:
                    raise ControlDatabaseError(
                        "published preparation envelope is immutable"
                    )
                return decode_outbox_row(outbox)
            if str(preparation["state"]) != "reserved":
                raise ControlDatabaseError(
                    "Wire V3 preparation cannot publish from state "
                    f"{preparation['state']}"
                )

            payload = json.loads(str(preparation["payload_json"]))
            expected = {
                "request_id": str(preparation["request_id"]),
                "attempt_id": str(preparation["proposed_attempt_id"]),
                "endpoint_id": str(preparation["endpoint_id"]),
                "endpoint_generation": str(preparation["endpoint_generation"]),
                "registration_generation": str(
                    payload.get("operator_registration_generation") or ""
                ),
            }
            observed = {
                "request_id": str(meta.get("request_id") or ""),
                "attempt_id": str(meta.get("attempt_id") or ""),
                "endpoint_id": str(identity.get("endpoint_id") or ""),
                "endpoint_generation": str(
                    identity.get("endpoint_generation") or ""
                ),
                "registration_generation": str(
                    identity.get("registration_generation") or ""
                ),
            }
            if observed != expected:
                raise ControlDatabaseError(
                    "Wire V3 envelope identity does not match its preparation"
                )

            active = conn.execute(
                "SELECT attempt_id FROM execution_attempts WHERE request_id=? "
                "AND state IN ('prepared','dispatched','accepted','running',"
                "'return_ready') LIMIT 1",
                (preparation["request_id"],),
            ).fetchone()
            if active is not None:
                raise ControlDatabaseError(
                    "request gained an active attempt while package was preparing"
                )
            last = conn.execute(
                "SELECT COALESCE(MAX(ordinal), 0) FROM execution_attempts "
                "WHERE request_id=?",
                (preparation["request_id"],),
            ).fetchone()
            expected_ordinal = int(last[0]) + 1
            if expected_ordinal != int(preparation["proposed_ordinal"]):
                raise ControlDatabaseError(
                    "execution attempt ordinal changed while package was preparing"
                )
            endpoint = conn.execute(
                "SELECT registration_generation, enabled, draining, "
                "source_present FROM backend_endpoints WHERE endpoint_id=?",
                (preparation["endpoint_id"],),
            ).fetchone()
            if (
                endpoint is None
                or str(endpoint[0]) != str(preparation["endpoint_generation"])
                or not endpoint[1]
                or endpoint[2]
                or not endpoint[3]
            ):
                raise ControlDatabaseError(
                    "prepared endpoint is no longer current at publication"
                )

            route = str(preparation["route_json"])
            attempt_id = str(preparation["proposed_attempt_id"])
            outbox_id = f"outbox-{attempt_id}"
            conn.execute(
                """
                INSERT INTO execution_attempts(
                    attempt_id, request_id, ordinal, state, endpoint_id,
                    endpoint_generation, node_id, execution_environment_id,
                    route_json, created_at, updated_at
                ) VALUES(?, ?, ?, 'prepared', ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    attempt_id,
                    preparation["request_id"],
                    preparation["proposed_ordinal"],
                    preparation["endpoint_id"],
                    preparation["endpoint_generation"],
                    preparation["node_id"],
                    preparation["execution_environment_id"],
                    route,
                    now,
                    now,
                ),
            )
            published_payload = {
                **payload,
                "schema": "ascendop.routed-wire-v3-attempt.v3",
                "outbox_id": outbox_id,
                "transport_protocol": "wire-v3",
                "wire_envelope_path": str(resolved_envelope),
                "wire_envelope_digest": digest,
                "wire_package_root": str(resolved_package),
            }
            conn.execute(
                """
                INSERT INTO transport_outbox(
                    outbox_id, attempt_id, endpoint_id, state, payload_json,
                    transport_protocol, envelope_path, envelope_digest,
                    package_root, created_at, updated_at
                ) VALUES(?, ?, ?, 'pending', ?, 'wire-v3', ?, ?, ?, ?, ?)
                """,
                (
                    outbox_id,
                    attempt_id,
                    preparation["endpoint_id"],
                    canonical_json(published_payload),
                    str(resolved_envelope),
                    digest,
                    str(resolved_package),
                    now,
                    now,
                ),
            )
            conn.execute(
                "UPDATE request_preparations SET state='published', "
                "envelope_path=?, envelope_digest=?, package_root=?, "
                "updated_at=? WHERE preparation_id=?",
                (
                    str(resolved_envelope),
                    digest,
                    str(resolved_package),
                    now,
                    preparation_id,
                ),
            )
            conn.execute(
                "UPDATE test_requests SET state='routed', blocker='', "
                "updated_at=? WHERE request_id=?",
                (now, preparation["request_id"]),
            )
            self._event(
                conn,
                "wire-v3-attempt-published",
                "transport-outbox",
                outbox_id,
                {
                    "request_id": preparation["request_id"],
                    "attempt_id": attempt_id,
                    "preparation_id": preparation_id,
                    "envelope_digest": digest,
                },
            )
            outbox = conn.execute(
                "SELECT * FROM transport_outbox WHERE outbox_id=?",
                (outbox_id,),
            ).fetchone()
        return decode_outbox_row(outbox)

    def fail_wire_v3_preparation(
        self,
        preparation_id: str,
        *,
        error: str,
    ) -> dict[str, Any]:
        detail = str(error or "Wire V3 package preparation failed")
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
            if str(row["state"]) == "failed":
                return decode_preparation_row(row)
            if str(row["state"]) != "reserved":
                raise ControlDatabaseError(
                    "Wire V3 preparation cannot fail from state "
                    f"{row['state']}"
                )
            conn.execute(
                "UPDATE request_preparations SET state='failed', error=?, "
                "updated_at=? WHERE preparation_id=?",
                (detail, now, preparation_id),
            )
            conn.execute(
                "UPDATE test_requests SET state='blocked', blocker=?, "
                "updated_at=? WHERE request_id=?",
                (f"prepublication-package-build-failed: {detail}", now, row["request_id"]),
            )
            self._event(
                conn,
                "wire-v3-preparation-failed",
                "request-preparation",
                preparation_id,
                {
                    "request_id": row["request_id"],
                    "proposed_attempt_id": row["proposed_attempt_id"],
                    "error": detail,
                    "execution_attempt_consumed": False,
                },
            )
            current = conn.execute(
                "SELECT * FROM request_preparations WHERE preparation_id=?",
                (preparation_id,),
            ).fetchone()
        return decode_preparation_row(current)


def _routed_payload(*, request: Any, selected: Any, attempt_id: str) -> dict[str, Any]:
    manifest = json.loads(str(request["manifest_json"]))
    workflow = manifest.get("workflow", {})
    workflow_ingest = bool(
        manifest.get(
            "workflow_ingest",
            workflow.get("workflow_ingest", True)
            if isinstance(workflow, dict)
            else True,
        )
    )
    if not workflow_ingest:
        raise ControlDatabaseError(
            "preparation reservations are only valid for workflow-ingested requests"
        )
    profile = manifest.get("task_execution_profile", {})
    if not isinstance(profile, dict):
        profile = {}
    outbox_id = f"outbox-{attempt_id}"
    return {
        "schema": "ascendop.routed-test-preparation.v3",
        "outbox_id": outbox_id,
        "attempt_id": attempt_id,
        "request_id": request["request_id"],
        "request_digest": request["request_digest"],
        "manifest_path": request["manifest_path"],
        "operator_registration_generation": request["registration_generation"],
        "target_endpoint_id": selected.endpoint_id,
        "target_node_id": selected.node_id,
        "target_environment_id": selected.execution_environment_id,
        "target_gateway_id": selected.gateway_id,
        "target_transport_mode": selected.transport_mode,
        "target_generation": selected.generation,
        "control_channel": selected.control_channel,
        "result_channel": selected.result_channel,
        "transport": selected.transport,
        "remote_root": selected.remote_root,
        "engine_root": selected.engine_root,
        "workflow_ingest": True,
        "origin_workspace": str(profile.get("origin_workspace", "")),
    }
