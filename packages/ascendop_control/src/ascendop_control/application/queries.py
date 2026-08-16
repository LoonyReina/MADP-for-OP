from __future__ import annotations

from typing import Any

from ascendop_protocol.management import PUBLIC_RESOURCE_SCHEMA

from ascendop_control.storage.database import ControlStore


class PublicQueryService:
    def __init__(self, store: ControlStore) -> None:
        self.store = store

    def system(self) -> dict[str, Any]:
        metadata = self.store.metadata()
        services = self.store.raw_rows(
            "service_heartbeats", order_by="service_id"
        )
        return _collection("system", [{"metadata": metadata, "services": services}])

    def operators(self) -> dict[str, Any]:
        return _collection(
            "operators",
            self.store.raw_rows("operator_registrations", order_by="operator_id"),
        )

    def requests(self) -> dict[str, Any]:
        return _collection(
            "requests",
            self.store.raw_rows("test_requests", order_by="created_at DESC"),
        )

    def endpoints(self) -> dict[str, Any]:
        return _collection(
            "endpoints",
            self.store.raw_rows("backend_endpoints", order_by="endpoint_id"),
        )

    def agents(self) -> dict[str, Any]:
        return _collection(
            "agents",
            self.store.raw_rows("agent_registrations_v4", order_by="agent_id"),
        )

    def agent_pools(self) -> dict[str, Any]:
        return _collection(
            "agent-pools",
            self.store.raw_rows("agent_pools_v4", order_by="pool_id"),
        )

    def agent_actions(self) -> dict[str, Any]:
        return _collection(
            "agent-actions",
            self.store.raw_rows(
                "agent_actions_v4", order_by="created_at DESC"
            ),
        )

    def agent_leases(self) -> dict[str, Any]:
        return _collection(
            "agent-leases",
            self.store.raw_rows(
                "agent_work_leases_v4", order_by="acquired_at DESC"
            ),
        )

    def iterations(self) -> dict[str, Any]:
        return _collection(
            "agent-iterations",
            self.store.raw_rows("agent_iterations_v4", order_by="created_at DESC"),
        )

    def official(self) -> dict[str, Any]:
        rows = self.store.public_resources("official-submission")
        return _collection("official", rows)

    def artifacts(self) -> dict[str, Any]:
        with self.store.connection() as conn:
            rows = conn.execute(
                "SELECT artifact_json FROM agent_artifacts_v4 "
                "ORDER BY created_at DESC LIMIT 500"
            ).fetchall()
        import json

        return _collection(
            "artifacts", [json.loads(str(row["artifact_json"])) for row in rows]
        )


def _collection(resource_type: str, items: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema": PUBLIC_RESOURCE_SCHEMA,
        "resource_type": resource_type,
        "resource_id": "collection",
        "revision": str(len(items)),
        "observed_at": _observed_at(items),
        "attributes": {"items": items, "count": len(items)},
    }


def _observed_at(items: list[dict[str, Any]]) -> str:
    for item in items:
        for field in ("updated_at", "observed_at", "created_at", "heartbeat_at"):
            value = item.get(field)
            if isinstance(value, str) and value:
                return value
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()
