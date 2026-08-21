from __future__ import annotations

import hashlib
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

    def record_submit_intake_failures(
        self,
        failures: list[dict[str, Any]],
        *,
        scan_identity: tuple[int, int],
        code_generation: str,
    ) -> list[dict[str, Any]]:
        """Persist candidate-local intake failures once per immutable queue scan."""

        if not failures:
            return []
        self.initialize()
        recorded: list[dict[str, Any]] = []
        with self.transaction() as conn:
            for failure in failures:
                payload = {
                    "operator": str(failure.get("operator") or ""),
                    "test_version": str(failure.get("test_version") or ""),
                    "request_id": str(failure.get("request_id") or ""),
                    "error": str(failure.get("error") or ""),
                    "scan_identity": [int(scan_identity[0]), int(scan_identity[1])],
                    "code_generation": str(code_generation),
                }
                incident_key = hashlib.sha256(
                    canonical_json(
                        {
                            key: value
                            for key, value in payload.items()
                            if key != "code_generation"
                        }
                    ).encode("utf-8")
                ).hexdigest()
                incident_id = f"submit-intake-{incident_key[:32]}"
                exists = conn.execute(
                    "SELECT 1 FROM control_events WHERE "
                    "event_type='submit-intake-candidate-failure' "
                    "AND entity_type='submit-intake-incident' AND entity_id=?",
                    (incident_id,),
                ).fetchone()
                if exists is None:
                    self._event(
                        conn,
                        "submit-intake-candidate-failure",
                        "submit-intake-incident",
                        incident_id,
                        payload,
                    )
                recorded.append(
                    {
                        "incident_id": incident_id,
                        "created": exists is None,
                        **payload,
                    }
                )
        return recorded

    def routing_topology_revision(self) -> int:
        """Return the local control-event revision relevant to route availability."""

        self.initialize()
        with self.connection() as conn:
            row = conn.execute(
                "SELECT COALESCE(MAX(sequence), 0) FROM control_events "
                "WHERE event_type IN ("
                "'registry-reconciled',"
                "'node-report-ingested',"
                "'node-accepted',"
                "'node-lease-renewed-by-trusted-probe'"
                ")"
            ).fetchone()
        return int(row[0] if row is not None else 0)

    def waiting_route_requests(self, *, limit: int = 64) -> list[dict[str, str]]:
        """List current-registration requests waiting for a backend."""

        self.initialize()
        bounded_limit = max(1, min(int(limit), 1024))
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT request.request_id, request.operator_id, "
                "registration.display_name AS operator, request.test_version, "
                "request.registration_generation "
                "FROM test_requests AS request "
                "JOIN operator_registrations AS registration "
                "ON registration.operator_id=request.operator_id "
                "WHERE request.state='blocked' "
                "AND request.blocker='no-compatible-backend' "
                "AND registration.desired_state='enabled' "
                "AND registration.registration_generation="
                "request.registration_generation "
                "ORDER BY request.created_at, request.request_id LIMIT ?",
                (bounded_limit,),
            ).fetchall()
        return [
            {
                "request_id": str(row["request_id"]),
                "operator_id": str(row["operator_id"]),
                "operator": str(row["operator"]),
                "test_version": str(row["test_version"]),
                "registration_generation": str(row["registration_generation"]),
            }
            for row in rows
        ]

    def waiting_route_request_ids(self, *, limit: int = 64) -> list[str]:
        return [
            row["request_id"]
            for row in self.waiting_route_requests(limit=limit)
        ]

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
                blocker = str(request["blocker"] or "").strip()
                if not blocker:
                    blocker = (
                        "prepublication-package-build-failed: "
                        f"{value.get('error') or 'unknown package build failure'}"
                    )
                return {
                    "attempt": None,
                    "preparation": None,
                    "route": value["route"],
                    "outbox": None,
                    "failed_preparation": value,
                    "blocker": blocker,
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
            observed_node = conn.execute(
                "SELECT current_session_id, capability_generation "
                "FROM observed_nodes WHERE node_id=?",
                (selected.node_id,),
            ).fetchone()
            runtime_identity = (
                {
                    "node_session_id": str(observed_node[0]),
                    "capability_generation": str(observed_node[1]),
                }
                if observed_node is not None
                else {}
            )
            payload = _routed_payload(
                request=request,
                selected=selected,
                attempt_id=attempt_id,
                runtime_identity=runtime_identity,
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

            node_session_id = str(payload.get("node_session_id") or "")
            capability_generation = str(
                payload.get("capability_generation") or ""
            )
            if bool(node_session_id) != bool(capability_generation):
                raise ControlDatabaseError(
                    "prepared runtime identity is incomplete"
                )
            if node_session_id:
                observed_node = conn.execute(
                    "SELECT endpoint_id, generation, current_session_id, "
                    "capability_generation FROM observed_nodes WHERE node_id=?",
                    (preparation["node_id"],),
                ).fetchone()
                expected_runtime = (
                    str(preparation["endpoint_id"]),
                    str(preparation["endpoint_generation"]),
                    node_session_id,
                    capability_generation,
                )
                actual_runtime = (
                    tuple(str(value) for value in observed_node)
                    if observed_node is not None
                    else ()
                )
                if actual_runtime != expected_runtime:
                    raise ControlDatabaseError(
                        "prepared node session/capability generation changed "
                        "before publication"
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
        code_generation: str = "",
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
                    "code_generation": str(code_generation),
                    "execution_attempt_consumed": False,
                },
            )
            current = conn.execute(
                "SELECT * FROM request_preparations WHERE preparation_id=?",
                (preparation_id,),
            ).fetchone()
        return decode_preparation_row(current)

def _routed_payload(
    *,
    request: Any,
    selected: Any,
    attempt_id: str,
    runtime_identity: dict[str, str] | None = None,
) -> dict[str, Any]:
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
    runtime = dict(runtime_identity or {})
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
        **(
            {
                "node_session_id": str(runtime["node_session_id"]),
                "capability_generation": str(runtime["capability_generation"]),
            }
            if runtime.get("node_session_id")
            and runtime.get("capability_generation")
            else {}
        ),
        "target_soc": list(selected.capabilities.get("soc", [])),
        "target_cann": list(selected.capabilities.get("cann", [])),
        "control_channel": selected.control_channel,
        "result_channel": selected.result_channel,
        "transport": selected.transport,
        "remote_root": selected.remote_root,
        "engine_root": selected.engine_root,
        "workflow_ingest": True,
        "origin_workspace": str(profile.get("origin_workspace", "")),
    }
