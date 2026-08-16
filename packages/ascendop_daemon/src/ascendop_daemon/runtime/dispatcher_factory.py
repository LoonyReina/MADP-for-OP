from __future__ import annotations

from pathlib import Path
from typing import Any

from ascendop_daemon.control_plane.dispatcher_factory import build_dispatcher_pool


def build_application_dispatchers(
    *,
    root: Path,
    database: Any,
    registry: Any,
    policy: Any,
    code_generation: str,
) -> Any:
    return build_dispatcher_pool(
        root,
        database,
        registry,
        capacity=int(policy.get("host.cache_hit_concurrency")),
        git_operation_timeout_seconds=int(
            policy.get("transport.git_operation_timeout_seconds")
        ),
        git_operation_lock_timeout_seconds=int(
            policy.get("transport.git_operation_lock_timeout_seconds")
        ),
        wait_timeout_seconds=int(policy.get("transport.wait_timeout_seconds")),
        claim_ttl_seconds=int(
            policy.get("transport.dispatch_claim_ttl_seconds")
        ),
        max_delivery_attempts=int(policy.get("retry.max_transport_retries")),
        max_postprocess_dispatch_attempts=int(
            policy.get("retry.postprocess_dispatch_max_attempts")
        ),
        postprocess_dispatch_retry_seconds=int(
            policy.get("retry.postprocess_dispatch_retry_seconds")
        ),
        code_generation=code_generation,
    )
