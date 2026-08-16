from __future__ import annotations

import json
from typing import Any

from ascendop_daemon.storage.control_types import ControlDatabaseError
from ascendop_daemon.storage.control_validation import utc_now


class ManagementControlRepository:
    def set_endpoint_runtime_drain(
        self,
        *,
        endpoint_id: str,
        draining: bool,
        command_id: str,
        actor_id: str,
        reason: str,
    ) -> dict[str, Any]:
        normalized_reason = str(reason).strip()
        if not normalized_reason:
            raise ControlDatabaseError("endpoint control reason is required")
        now = utc_now()
        with self.transaction() as conn:
            endpoint = conn.execute(
                "SELECT enabled, draining, config_json, source_present "
                "FROM backend_endpoints WHERE endpoint_id=?",
                (endpoint_id,),
            ).fetchone()
            if endpoint is None or not bool(endpoint["source_present"]):
                raise ControlDatabaseError(
                    f"registered endpoint does not exist: {endpoint_id}"
                )
            if draining:
                conn.execute(
                    "INSERT INTO endpoint_drain_overrides_v4(endpoint_id, command_id, "
                    "actor_id, reason, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(endpoint_id) DO UPDATE SET command_id=excluded.command_id, "
                    "actor_id=excluded.actor_id, reason=excluded.reason, "
                    "updated_at=excluded.updated_at",
                    (endpoint_id, command_id, actor_id, normalized_reason, now, now),
                )
                effective_draining = True
            else:
                conn.execute(
                    "DELETE FROM endpoint_drain_overrides_v4 WHERE endpoint_id=?",
                    (endpoint_id,),
                )
                configured = json.loads(str(endpoint["config_json"]))
                effective_draining = bool(configured.get("draining", False))
            conn.execute(
                "UPDATE backend_endpoints SET draining=?, updated_at=? "
                "WHERE endpoint_id=?",
                (int(effective_draining), now, endpoint_id),
            )
            self._event(
                conn,
                "endpoint-runtime-drain-set",
                "endpoint",
                endpoint_id,
                {
                    "command_id": command_id,
                    "actor_id": actor_id,
                    "requested_draining": bool(draining),
                    "effective_draining": effective_draining,
                    "reason": normalized_reason,
                },
            )
        return {
            "endpoint_id": endpoint_id,
            "enabled": bool(endpoint["enabled"]),
            "draining": effective_draining,
            "runtime_override": bool(draining),
        }
