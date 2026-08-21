from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from .errors import ControlRepositoryError
from .schema import CONTROL_SCHEMA_VERSION


class ServiceRepository:
    """Process-neutral service registration and liveness projection."""

    def record_runtime_service_heartbeat(
        self,
        *,
        service_id: str,
        role: str,
        code_generation: str,
        capabilities: list[str],
        state: str,
        boot_id: str,
        lease_seconds: int = 30,
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        identities = (service_id, role, code_generation, state, boot_id)
        if not all(isinstance(value, str) and value.strip() for value in identities):
            raise ControlRepositoryError("service heartbeat identity is incomplete")
        if not 1 <= int(lease_seconds) <= 3600:
            raise ControlRepositoryError("service heartbeat lease is out of range")
        normalized_capabilities = sorted(
            {
                value.strip()
                for value in capabilities
                if isinstance(value, str) and value.strip()
            }
        )
        now_value = datetime.now(timezone.utc)
        now = now_value.isoformat()
        expires_at = (now_value + timedelta(seconds=int(lease_seconds))).isoformat()
        with self.transaction() as conn:
            previous = conn.execute(
                "SELECT role, code_generation, capabilities_json, state, boot_id "
                "FROM service_heartbeats WHERE service_id=?",
                (service_id,),
            ).fetchone()
            conn.execute(
                """
                INSERT INTO service_heartbeats(
                    service_id, role, code_generation, wire_version,
                    database_schema, capabilities_json, state, boot_id,
                    heartbeat_at, lease_expires_at, details_json, updated_at
                ) VALUES(?, ?, ?, 3, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(service_id) DO UPDATE SET
                    role=excluded.role,
                    code_generation=excluded.code_generation,
                    wire_version=3,
                    database_schema=excluded.database_schema,
                    capabilities_json=excluded.capabilities_json,
                    state=excluded.state,
                    boot_id=excluded.boot_id,
                    heartbeat_at=excluded.heartbeat_at,
                    lease_expires_at=excluded.lease_expires_at,
                    details_json=excluded.details_json,
                    updated_at=excluded.updated_at
                """,
                (
                    service_id,
                    role,
                    code_generation,
                    CONTROL_SCHEMA_VERSION,
                    json.dumps(normalized_capabilities, separators=(",", ":")),
                    state,
                    boot_id,
                    now,
                    expires_at,
                    json.dumps(details or {}, sort_keys=True, separators=(",", ":")),
                    now,
                ),
            )
            identity = (
                role,
                code_generation,
                json.dumps(normalized_capabilities, separators=(",", ":")),
                state,
                boot_id,
            )
            if previous is None or tuple(previous) != identity:
                self._event(
                    conn,
                    "runtime-service-registration-changed",
                    "service",
                    service_id,
                    {
                        "role": role,
                        "code_generation": code_generation,
                        "state": state,
                        "boot_id": boot_id,
                    },
                )
        return {
            "service_id": service_id,
            "role": role,
            "code_generation": code_generation,
            "wire_version": 3,
            "database_schema": CONTROL_SCHEMA_VERSION,
            "capabilities": normalized_capabilities,
            "state": state,
            "boot_id": boot_id,
            "heartbeat_at": now,
            "lease_expires_at": expires_at,
            "details": details or {},
        }

    def runtime_service_health(self) -> list[dict[str, Any]]:
        now = datetime.now(timezone.utc)
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM service_heartbeats ORDER BY service_id"
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            expiry = datetime.fromisoformat(str(row["lease_expires_at"]))
            result.append(
                {
                    "service_id": str(row["service_id"]),
                    "role": str(row["role"]),
                    "code_generation": str(row["code_generation"]),
                    "wire_version": int(row["wire_version"]),
                    "database_schema": int(row["database_schema"]),
                    "capabilities": json.loads(str(row["capabilities_json"])),
                    "state": str(row["state"]),
                    "boot_id": str(row["boot_id"]),
                    "heartbeat_at": str(row["heartbeat_at"]),
                    "lease_expires_at": str(row["lease_expires_at"]),
                    "details": json.loads(str(row["details_json"])),
                    "live": str(row["state"]) == "ready" and expiry >= now,
                }
            )
        return result

    def retire_runtime_service(
        self,
        *,
        service_id: str,
        expected_boot_id: str,
        reason: str,
    ) -> bool:
        """CAS-retire one service after its process owner is proven gone."""

        if not service_id or not expected_boot_id or not reason:
            raise ControlRepositoryError("service retirement identity is incomplete")
        now = datetime.now(timezone.utc).isoformat()
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT state FROM service_heartbeats "
                "WHERE service_id=? AND boot_id=?",
                (service_id, expected_boot_id),
            ).fetchone()
            if row is None:
                return False
            if str(row["state"]) == "stopped":
                return True
            conn.execute(
                "UPDATE service_heartbeats SET state='stopped', "
                "lease_expires_at=?, updated_at=? "
                "WHERE service_id=? AND boot_id=?",
                (now, now, service_id, expected_boot_id),
            )
            self._event(
                conn,
                "runtime-service-retired",
                "service",
                service_id,
                {"boot_id": expected_boot_id, "reason": reason},
            )
        return True
