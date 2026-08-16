from __future__ import annotations

from ascendop_daemon.automation.delivery_fence import ack_is_ide_confirmed
from ascendop_daemon.core.correlation import engine_correlation
from ascendop_daemon.exchange.transport_contracts import (
    target_args,
    transport_identity,
    transport_identity_mismatch,
)


def test_engine_correlation_normalizes_untrusted_labels() -> None:
    assert engine_correlation(
        {
            "request_id": "request/7",
            "engine_job_id": "job:9",
            "attempt_id": "attempt 2",
            "operator": "sample-op",
            "test_version": "case.v1",
        }
    ) == {
        "request_id": "request_7",
        "engine_job_id": "job_9",
        "attempt_id": "attempt_2",
        "operator": "sample-op",
        "test_version": "case.v1",
    }


def test_transport_identity_round_trips_target_arguments() -> None:
    payload = {
        "attempt_id": "attempt-1",
        "request_id": "request-1",
        "target_endpoint_id": "endpoint-a",
        "target_node_id": "node-a",
        "target_environment_id": "env-a",
        "target_gateway_id": "gateway-a",
        "target_transport_mode": "relay",
        "target_generation": "generation-a",
    }
    identity = transport_identity(payload)
    assert identity == payload
    assert transport_identity_mismatch(identity, dict(identity)) == ""
    assert target_args(payload) == [
        "--target-node",
        "node-a",
        "--target-endpoint-id",
        "endpoint-a",
        "--target-environment-id",
        "env-a",
        "--target-transport-mode",
        "relay",
        "--registration-generation",
        "generation-a",
        "--target-gateway-id",
        "gateway-a",
    ]


def test_delivery_fence_requires_native_visibility() -> None:
    assert ack_is_ide_confirmed(
        {
            "status": "delivered",
            "delivery": "codex-app-send-message-to-thread",
            "ide_panel_visible": True,
        }
    )
    assert not ack_is_ide_confirmed(
        {
            "status": "delivered",
            "delivery": "codex-app-send-message-to-thread",
            "ide_panel_visible": False,
        }
    )
