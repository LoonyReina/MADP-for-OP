from __future__ import annotations

from pathlib import Path
from typing import Any

from ascendop_daemon.control_plane.control_database import (
    ControlDatabaseError,
)
from ascendop_daemon.registry.node_reconciler import (
    GitPartnerNodeAdmissionReconciler,
)
from ascendop_daemon.runtime.application_support import (
    assert_live_generation_compatible,
)
from ascendop_daemon.runtime.control import read_stop_request


class EndpointReconciliationService:
    def __init__(
        self,
        *,
        root: Path,
        database,
        registry,
        config,
        policy,
        generation: str,
    ) -> None:
        self.root = root
        self.database = database
        self.registry = registry
        self.config = config
        self.policy = policy
        self.generation = generation

    def run_once(
        self,
        *,
        endpoint_ids: set[str] | None,
        allow_trusted_lease_probe: bool,
    ) -> dict[str, Any]:
        assert_live_generation_compatible(self.database, self.generation)
        ack_root_value = str(
            self.config.policy.get("control_plane_node_ack_root")
            or "TestUtils/tester_daemon/node_acks"
        )
        ack_root = Path(ack_root_value)
        if not ack_root.is_absolute():
            ack_root = self.root / ack_root
        result = GitPartnerNodeAdmissionReconciler(
            self.root,
            self.database,
            self.registry,
            ack_root=ack_root,
            git_operation_timeout_seconds=int(
                self.policy.get("transport.git_operation_timeout_seconds")
            ),
            git_operation_lock_timeout_seconds=int(
                self.policy.get(
                    "transport.git_operation_lock_timeout_seconds"
                )
            ),
            node_report_timeout_seconds=int(
                self.policy.get("transport.node_liveness_query_timeout_seconds")
            ),
            node_report_max_concurrency=int(
                self.policy.get("transport.node_report_max_concurrency")
            ),
            trusted_lease_probe_timeout_seconds=int(
                self.policy.get(
                    "transport.trusted_lease_probe_timeout_seconds"
                )
            ),
            node_ack_delivery_timeout_seconds=int(
                self.policy.get("transport.node_ack_delivery_timeout_seconds")
            ),
            allow_trusted_lease_probe=allow_trusted_lease_probe,
        ).run_once(endpoint_ids=endpoint_ids)
        return {
            "schema": "ascendop.endpoint-reconciliation.v3",
            "generation": self.generation,
            "stop_fenced": bool(read_stop_request(self.root)),
            **result,
        }

    def accept(self, endpoint_id: str) -> dict[str, Any]:
        endpoint = next(
            item for item in self.registry.endpoints if item.endpoint_id == endpoint_id
        )
        discovery = self.run_once(
            endpoint_ids={endpoint_id},
            allow_trusted_lease_probe=True,
        )
        if int(discovery.get("failure_count", 0)):
            raise ControlDatabaseError(
                f"endpoint report refresh failed before acceptance: {endpoint_id}"
            )
        if int(discovery.get("refreshed_count", 0)) != 1:
            raise ControlDatabaseError(
                f"endpoint report was not uniquely refreshed: {endpoint_id}"
            )
        acceptance = self.database.accept_node(endpoint.node_id, self.registry)
        reconciliation = self.run_once(
            endpoint_ids={endpoint_id},
            allow_trusted_lease_probe=True,
        )
        return {
            "schema": "ascendop.endpoint-acceptance.v3",
            "generation": self.generation,
            "discovery": discovery,
            "acceptance": acceptance,
            "reconciliation": reconciliation,
        }
