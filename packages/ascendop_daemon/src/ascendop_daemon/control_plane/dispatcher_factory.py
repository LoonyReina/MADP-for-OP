from __future__ import annotations

from pathlib import Path

from ascendop_daemon.control_plane.control_database import ControlDatabase
from ascendop_daemon.control_plane.endpoint_dispatcher import (
    EndpointDispatcher,
    EndpointDispatcherPool,
)
from ascendop_daemon.exchange.gitpartner_transport import GitPartnerCanaryTransport
from ascendop_daemon.exchange.wire_v3_transport import (
    RoutedEndpointTransport,
    WireV3EndpointTransport,
    exchange_process_timeout_seconds,
)
from ascendop_daemon.registry.system_registry import SystemRegistry
from ascendop_daemon.workflow.cannjudge_result_adapter import (
    CannJudgeV3ResultAdapter,
)
from ascendop_daemon.workflow.workspace_result_collector import (
    WorkspaceResultCollector,
)


def build_dispatcher_pool(
    root: Path,
    database: ControlDatabase,
    registry: SystemRegistry,
    *,
    capacity: int = 4,
    git_operation_timeout_seconds: int = 60,
    git_operation_lock_timeout_seconds: int = 60,
    wait_timeout_seconds: int = 180,
    claim_ttl_seconds: int = 360,
    max_delivery_attempts: int = 3,
    max_postprocess_dispatch_attempts: int = 3,
    postprocess_dispatch_retry_seconds: int = 5,
    endpoint_ids: set[str] | None = None,
    code_generation: str = "",
) -> EndpointDispatcherPool:
    process_timeout = exchange_process_timeout_seconds(
        git_operation_timeout_seconds=git_operation_timeout_seconds,
        wait_timeout_seconds=wait_timeout_seconds,
    )
    if int(claim_ttl_seconds) < process_timeout:
        raise ValueError(
            "dispatcher claim TTL must cover the complete Wire V3 exchange: "
            f"claim_ttl={claim_ttl_seconds}, process_timeout={process_timeout}"
        )
    result_collector = WorkspaceResultCollector(
        root,
        workflow_ingestor=CannJudgeV3ResultAdapter(root),
    )
    selected = [
        endpoint
        for endpoint in registry.endpoints
        if endpoint.enabled
        and (endpoint_ids is None or endpoint.endpoint_id in endpoint_ids)
    ]
    return EndpointDispatcherPool(
        [
            EndpointDispatcher(
                database,
                endpoint,
                RoutedEndpointTransport(
                    GitPartnerCanaryTransport(
                        root,
                        endpoint,
                        command_timeout_seconds=git_operation_timeout_seconds,
                        git_operation_lock_timeout_seconds=(
                            git_operation_lock_timeout_seconds
                        ),
                    ),
                    WireV3EndpointTransport(
                        root,
                        endpoint,
                        git_operation_timeout_seconds=(
                            git_operation_timeout_seconds
                        ),
                        git_operation_lock_timeout_seconds=(
                            git_operation_lock_timeout_seconds
                        ),
                        wait_timeout_seconds=wait_timeout_seconds,
                    ),
                ),
                capacity=capacity,
                claim_ttl_seconds=claim_ttl_seconds,
                max_delivery_attempts=max_delivery_attempts,
                max_postprocess_dispatch_attempts=(
                    max_postprocess_dispatch_attempts
                ),
                postprocess_dispatch_retry_seconds=(
                    postprocess_dispatch_retry_seconds
                ),
                query_owner_generation=code_generation,
                result_collector=result_collector,
            )
            for endpoint in selected
        ],
        database=database,
        result_collector=result_collector,
    )


__all__ = ["build_dispatcher_pool"]
