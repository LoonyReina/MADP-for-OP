from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from ascendop_protocol.wire_v3 import canonical_digest
from ascendop_daemon.core.atomic_io import write_json_atomic
from ascendop_daemon.registry.system_registry import (
    SYSTEM_REGISTRY_SCHEMA,
    RouteDecision,
    SystemRegistry,
)
from ascendop_daemon.storage.row_decoders import (
    decode_attempt_row,
    decode_request_row,
)
from ascendop_daemon.storage.control_types import (
    ACTIVE_ATTEMPT_STATES,
    SYSTEM_EXPERIMENT_GENERATION,
    SYSTEM_EXPERIMENT_OPERATOR_ID,
    ControlDatabaseError,
)
from ascendop_daemon.storage.control_validation import (
    _is_bootstrap_control_probe,
    _runtime_route_rejection_reasons,
    canonical_json,
    utc_now,
)


def _is_publishable_operator_test(manifest: dict[str, Any]) -> bool:
    if str(manifest.get("schema") or "") != "ascendop.test-request.v1":
        return False
    workflow = manifest.get("workflow", {})
    return bool(
        isinstance(workflow, dict)
        and workflow.get("operation_kind", "operator-test") == "operator-test"
        and workflow.get("publish_eligible", True) is True
        and workflow.get("workflow_ingest", True) is True
    )


class RequestRoutingRepository:
    def test_request(self, request_id: str) -> dict[str, Any]:
        self.initialize()
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM test_requests WHERE request_id=?",
                (request_id,),
            ).fetchone()
        if row is None:
            raise ControlDatabaseError(f"unknown TestRequest: {request_id}")
        return decode_request_row(row)

    def test_requests_for_logical_identity(
        self,
        operator_id: str,
        test_version: str,
    ) -> list[dict[str, Any]]:
        self.initialize()
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM test_requests "
                "WHERE operator_id=? AND test_version=? "
                "ORDER BY created_at, request_id",
                (operator_id, test_version),
            ).fetchall()
        return [decode_request_row(row) for row in rows]

    def logical_test_request_terminal_projection(
        self,
        operator_id: str,
        test_version: str,
    ) -> dict[str, Any]:
        """Return a projection fact only when every exact logical request is terminal."""

        self.initialize()
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT request_id, state, blocker FROM test_requests "
                "WHERE operator_id=? AND test_version=? ORDER BY request_id",
                (operator_id, test_version),
            ).fetchall()
        if not rows:
            return {
                "state": "missing",
                "terminal_state": "",
                "request_ids": [],
                "request_states": [],
                "blockers": [],
            }
        terminal_states = {"completed", "failed", "cancelled"}
        request_states = [str(row["state"]) for row in rows]
        common = {
            "request_ids": [str(row["request_id"]) for row in rows],
            "request_states": request_states,
            "blockers": [str(row["blocker"] or "") for row in rows],
        }
        if any(state not in terminal_states for state in request_states):
            return {"state": "unsettled", "terminal_state": "", **common}
        if "completed" in request_states:
            terminal_state = "completed"
        elif "failed" in request_states:
            terminal_state = "failed"
        else:
            terminal_state = "cancelled"
        return {
            "state": "terminal",
            "terminal_state": terminal_state,
            **common,
        }

    def create_test_request(
        self, manifest: dict[str, Any], manifest_path: Path
    ) -> dict[str, Any]:
        self.initialize()
        request_id = str(manifest.get("request_id") or "")
        request_digest = str(manifest.get("request_digest") or "")
        operator_id = str(manifest.get("operator_id") or "")
        test_version = str(manifest.get("test_version") or "")
        registration_generation = str(manifest.get("registration_generation") or "")
        requirements = manifest.get("execution_requirements", {})
        if not all(
            (
                request_id,
                request_digest,
                operator_id,
                test_version,
                registration_generation,
            )
        ) or not isinstance(requirements, dict):
            raise ControlDatabaseError("incomplete immutable TestRequest manifest")
        digest_material = dict(manifest)
        digest_material.pop("request_id", None)
        digest_material.pop("request_digest", None)
        actual_digest = canonical_digest(digest_material)
        if request_digest != actual_digest:
            raise ControlDatabaseError(
                "immutable TestRequest digest mismatch: "
                f"claimed={request_digest} actual={actual_digest}"
            )
        now = utc_now()
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT * FROM test_requests WHERE request_id=? OR request_digest=?",
                (request_id, request_digest),
            ).fetchone()
            if existing is not None:
                if (
                    existing["request_id"] != request_id
                    or existing["request_digest"] != request_digest
                    or existing["manifest_json"] != canonical_json(manifest)
                ):
                    raise ControlDatabaseError(
                        f"immutable TestRequest identity collision: {request_id}"
                    )
                return decode_request_row(existing)
            if _is_publishable_operator_test(manifest):
                logical_rows = conn.execute(
                    "SELECT request_id, request_digest, manifest_json "
                    "FROM test_requests WHERE operator_id=? AND test_version=?",
                    (operator_id, test_version),
                ).fetchall()
                for logical_row in logical_rows:
                    try:
                        logical_manifest = json.loads(str(logical_row["manifest_json"]))
                    except json.JSONDecodeError as exc:
                        raise ControlDatabaseError(
                            "stored TestRequest manifest is invalid"
                        ) from exc
                    if _is_publishable_operator_test(logical_manifest):
                        raise ControlDatabaseError(
                            "immutable TestRequest logical identity collision: "
                            f"{operator_id}/{test_version} already belongs to "
                            f"{logical_row['request_id']} "
                            f"({logical_row['request_digest']})"
                        )
            registration = conn.execute(
                "SELECT registration_generation FROM operator_registrations "
                "WHERE operator_id=?",
                (operator_id,),
            ).fetchone()
            if registration is None:
                raise ControlDatabaseError(
                    f"request operator is not registered: {operator_id}"
                )
            if registration[0] != registration_generation:
                raise ControlDatabaseError(
                    "request registration generation is stale before admission"
                )
            conn.execute(
                """
                INSERT INTO test_requests(
                    request_id, request_digest, operator_id, test_version, state,
                    registration_generation, requirements_json, manifest_json,
                    manifest_path, created_at, updated_at
                ) VALUES(?, ?, ?, ?, 'created', ?, ?, ?, ?, ?, ?)
                """,
                (
                    request_id,
                    request_digest,
                    operator_id,
                    test_version,
                    registration_generation,
                    canonical_json(requirements),
                    canonical_json(manifest),
                    str(manifest_path.resolve()),
                    now,
                    now,
                ),
            )
            self._event(
                conn,
                "test-request-created",
                "test-request",
                request_id,
                {
                    "operator_id": operator_id,
                    "test_version": test_version,
                    "request_digest": request_digest,
                },
            )
            row = conn.execute(
                "SELECT * FROM test_requests WHERE request_id=?", (request_id,)
            ).fetchone()
        return decode_request_row(row)

    def create_experiment_request(
        self, manifest: dict[str, Any], manifest_path: Path
    ) -> dict[str, Any]:
        """Create an isolated control-plane request without workflow ingestion."""
        if bool(manifest.get("workflow_ingest", True)):
            raise ControlDatabaseError(
                "transport experiments must set workflow_ingest=false"
            )
        value = dict(manifest)
        value["operator_id"] = SYSTEM_EXPERIMENT_OPERATOR_ID
        value["registration_generation"] = SYSTEM_EXPERIMENT_GENERATION
        digest_material = dict(value)
        digest_material.pop("request_id", None)
        digest_material.pop("request_digest", None)
        value["request_digest"] = canonical_digest(digest_material)
        # The experiment adapter is the sole legacy-to-V3 boundary. Persist the
        # completed immutable identity before the strict production path reads it.
        write_json_atomic(
            manifest_path,
            value,
            ensure_ascii=True,
            sort_keys=True,
        )
        now = utc_now()
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO operator_registrations(
                    operator_id, display_name, season, desired_state,
                    registration_generation, definition_json, workspace_json,
                    requirements_json, cache_policy_json, routing_policy_json,
                    test_profile, source_present, updated_at
                ) VALUES(?, ?, ?, 'system', ?, '{}', '{}', '{}', '{}', '{}',
                    'transport-canary', 0, ?)
                ON CONFLICT(operator_id) DO UPDATE SET
                    registration_generation=excluded.registration_generation,
                    desired_state='system',
                    source_present=0,
                    updated_at=excluded.updated_at
                """,
                (
                    SYSTEM_EXPERIMENT_OPERATOR_ID,
                    "TransportExperiment",
                    "__system__",
                    SYSTEM_EXPERIMENT_GENERATION,
                    now,
                ),
            )
        return self.create_test_request(value, manifest_path)

    def route_test_request(
        self, request_id: str, registry: SystemRegistry
    ) -> dict[str, Any]:
        self.initialize()
        with self.transaction() as conn:
            request = conn.execute(
                "SELECT * FROM test_requests WHERE request_id=?", (request_id,)
            ).fetchone()
            if request is None:
                raise ControlDatabaseError(f"unknown TestRequest: {request_id}")
            active = conn.execute(
                "SELECT * FROM execution_attempts WHERE request_id=? "
                "ORDER BY ordinal DESC",
                (request_id,),
            ).fetchall()
            for row in active:
                if row["state"] in ACTIVE_ATTEMPT_STATES:
                    return {
                        "attempt": decode_attempt_row(row),
                        "route": json.loads(row["route_json"]),
                        "idempotent": True,
                    }
            requirements = json.loads(request["requirements_json"])
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
                    "UPDATE test_requests SET state='blocked', blocker=?, updated_at=? "
                    "WHERE request_id=?",
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
                    "route": decision.to_dict(),
                    "blocker": blocker,
                    "idempotent": False,
                }
            selected = decision.selected
            endpoint_row = conn.execute(
                "SELECT registration_generation, enabled, draining, source_present "
                "FROM backend_endpoints WHERE endpoint_id=?",
                (selected.endpoint_id,),
            ).fetchone()
            if (
                endpoint_row is None
                or not endpoint_row[1]
                or endpoint_row[2]
                or not endpoint_row[3]
            ):
                raise ControlDatabaseError(
                    f"selected endpoint is not current in database: {selected.endpoint_id}"
                )
            if endpoint_row[0] != selected.generation:
                raise ControlDatabaseError(
                    f"selected endpoint generation drifted: {selected.endpoint_id}"
                )
            ordinal = int(active[0]["ordinal"] if active else 0) + 1
            attempt_id = f"{request_id}-a{ordinal:03d}"
            outbox_id = f"outbox-{attempt_id}"
            now = utc_now()
            route_value = decision.to_dict()
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
                    request_id,
                    ordinal,
                    selected.endpoint_id,
                    selected.generation,
                    selected.node_id,
                    selected.execution_environment_id,
                    canonical_json(route_value),
                    now,
                    now,
                ),
            )
            request_manifest = json.loads(request["manifest_json"])
            workflow = request_manifest.get("workflow", {})
            workflow_ingest = bool(
                request_manifest.get(
                    "workflow_ingest",
                    workflow.get("workflow_ingest", True)
                    if isinstance(workflow, dict)
                    else True,
                )
            )
            task_profile_info = request_manifest.get(
                "task_execution_profile",
                {},
            )
            if not isinstance(task_profile_info, dict):
                task_profile_info = {}
            outbox_payload = {
                "schema": "ascendop.routed-test-attempt.v1",
                "outbox_id": outbox_id,
                "attempt_id": attempt_id,
                "request_id": request_id,
                "request_digest": request["request_digest"],
                "manifest_path": request["manifest_path"],
                "operator_registration_generation": request["registration_generation"],
                "target_endpoint_id": selected.endpoint_id,
                "target_node_id": selected.node_id,
                "target_environment_id": selected.execution_environment_id,
                "target_gateway_id": selected.gateway_id,
                "target_transport_mode": selected.transport_mode,
                "target_generation": selected.generation,
                "target_soc": list(selected.capabilities.get("soc", [])),
                "target_cann": list(selected.capabilities.get("cann", [])),
                "control_channel": selected.control_channel,
                "result_channel": selected.result_channel,
                "transport": selected.transport,
                "remote_root": selected.remote_root,
                "engine_root": selected.engine_root,
                "workflow_ingest": workflow_ingest,
                "origin_workspace": str(task_profile_info.get("origin_workspace", "")),
            }
            outbox_state = "preparing" if workflow_ingest else "pending"
            conn.execute(
                """
                INSERT INTO transport_outbox(
                    outbox_id, attempt_id, endpoint_id, state, payload_json,
                    created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    outbox_id,
                    attempt_id,
                    selected.endpoint_id,
                    outbox_state,
                    canonical_json(outbox_payload),
                    now,
                    now,
                ),
            )
            conn.execute(
                "UPDATE test_requests SET state='routed', blocker='', updated_at=? "
                "WHERE request_id=?",
                (now, request_id),
            )
            self._event(
                conn,
                "test-request-routed",
                "execution-attempt",
                attempt_id,
                outbox_payload,
            )
            row = conn.execute(
                "SELECT * FROM execution_attempts WHERE attempt_id=?", (attempt_id,)
            ).fetchone()
        return {
            "attempt": decode_attempt_row(row),
            "route": route_value,
            "outbox": outbox_payload,
            "idempotent": False,
        }

    def explain_route(
        self,
        requirements: dict[str, Any],
        registry: SystemRegistry,
    ) -> dict[str, Any]:
        self.initialize()
        with self.connection() as conn:
            return self._route_decision(conn, registry, requirements).to_dict()

    def _route_decision(
        self,
        conn: sqlite3.Connection,
        registry: SystemRegistry,
        requirements: dict[str, Any],
        *,
        allow_unobserved_node: bool = False,
    ) -> RouteDecision:
        static = registry.route(requirements)
        if registry.schema != SYSTEM_REGISTRY_SCHEMA:
            return static
        return self._runtime_route_decision(
            conn,
            registry,
            static,
            requirements,
            allow_unobserved_node=allow_unobserved_node,
        )

    def _runtime_route_decision(
        self,
        conn: sqlite3.Connection,
        registry: SystemRegistry,
        static: RouteDecision,
        requirements: dict[str, Any],
        *,
        allow_unobserved_node: bool = False,
    ) -> RouteDecision:
        endpoints = {endpoint.endpoint_id: endpoint for endpoint in registry.endpoints}
        rows: list[dict[str, Any]] = []
        accepted = []
        for static_row in static.candidates:
            endpoint_id = str(static_row["endpoint_id"])
            endpoint = endpoints[endpoint_id]
            reasons = list(static_row["rejection_reasons"])
            for reason in _runtime_route_rejection_reasons(
                conn,
                endpoint,
                requirements,
                allow_unobserved_node=allow_unobserved_node,
            ):
                if reason not in reasons:
                    reasons.append(reason)
            row = {
                **static_row,
                "accepted": not reasons,
                "rejection_reasons": reasons,
            }
            rows.append(row)
            if not reasons:
                accepted.append(endpoint)
        selected = (
            sorted(
                accepted,
                key=lambda endpoint: (-endpoint.priority, endpoint.endpoint_id),
            )[0]
            if accepted
            else None
        )
        return RouteDecision(selected=selected, candidates=tuple(rows))
