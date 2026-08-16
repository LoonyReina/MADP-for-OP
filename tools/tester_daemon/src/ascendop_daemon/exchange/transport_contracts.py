from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass, field
from typing import Any, Protocol

from ascendop_daemon.registry.system_registry import BackendEndpoint

@dataclass(frozen=True)
class DeliveryObservation:
    status: str
    receipt: dict[str, Any] = field(default_factory=dict)
    error: str = ""
    retry_after_seconds: int = 0
    failure: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class QueryObservation:
    acceptance: dict[str, Any] | None = None
    result: dict[str, Any] | None = None
    error: str = ""
    diagnostics: dict[str, Any] = field(default_factory=dict)
    failure: dict[str, Any] = field(default_factory=dict)


class EndpointTransport(Protocol):
    def publish(self, payload: dict[str, Any]) -> DeliveryObservation: ...

    def publish_batch(
        self,
        payloads: list[dict[str, Any]],
    ) -> list[DeliveryObservation]: ...

    def query(self, payload: dict[str, Any]) -> QueryObservation: ...

    def query_batch(
        self,
        payloads: list[dict[str, Any]],
    ) -> list[QueryObservation]: ...

    def acknowledge(
        self, payload: dict[str, Any], returned: dict[str, Any]
    ) -> bool: ...


class TransportResultCollector(Protocol):
    def collect(
        self,
        outbox: dict[str, Any],
        returned: dict[str, Any],
    ) -> dict[str, Any]: ...

def target_args(payload: dict[str, Any]) -> list[str]:
    values = [
        "--target-node",
        str(payload["target_node_id"]),
        "--target-endpoint-id",
        str(payload["target_endpoint_id"]),
        "--target-environment-id",
        str(payload["target_environment_id"]),
        "--target-transport-mode",
        str(payload["target_transport_mode"]),
        "--registration-generation",
        str(payload["target_generation"]),
    ]
    gateway = str(payload.get("target_gateway_id") or "")
    if gateway:
        values.extend(["--target-gateway-id", gateway])
    return values


def transport_identity(payload: dict[str, Any]) -> dict[str, str]:
    return {
        key: str(payload.get(key) or "")
        for key in (
            "attempt_id",
            "request_id",
            "target_endpoint_id",
            "target_node_id",
            "target_environment_id",
            "target_gateway_id",
            "target_transport_mode",
            "target_generation",
        )
    }


def status_transport_identity(status: dict[str, Any]) -> dict[str, str]:
    target_nodes = status.get("target_nodes", [])
    target_node_id = str(status.get("target_node_id") or "")
    if not target_node_id and isinstance(target_nodes, list) and target_nodes:
        target_node_id = str(target_nodes[0])
    return {
        "attempt_id": str(status.get("attempt_id") or ""),
        "request_id": str(status.get("request_id") or ""),
        "target_endpoint_id": str(status.get("target_endpoint_id") or ""),
        "target_node_id": target_node_id,
        "target_environment_id": str(
            status.get("target_environment_id") or ""
        ),
        "target_gateway_id": str(status.get("target_gateway_id") or ""),
        "target_transport_mode": str(
            status.get("target_transport_mode") or ""
        ),
        "target_generation": str(
            status.get("target_generation")
            or status.get("registration_generation")
            or ""
        ),
    }


def transport_identity_mismatch(
    expected: dict[str, str],
    observed: dict[str, str],
) -> str:
    for key in (
        "attempt_id",
        "request_id",
        "target_endpoint_id",
        "target_node_id",
        "target_environment_id",
        "target_gateway_id",
        "target_transport_mode",
        "target_generation",
    ):
        if observed.get(key, "") != expected.get(key, ""):
            return key
    return ""


def deterministic_receipt_id(payload: dict[str, Any]) -> str:
    value = (
        f"{payload.get('target_endpoint_id', '')}:"
        f"{payload.get('request_id', '')}:"
        f"{payload.get('attempt_id', '')}"
    )
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]


def endpoint_supports(endpoint: BackendEndpoint, feature: str) -> bool:
    values = endpoint.capabilities.get("features", [])
    return isinstance(values, (list, tuple, set)) and feature in {
        str(item) for item in values
    }


def parse_last_json_object(text: str) -> dict[str, Any]:
    for line in reversed(text.splitlines()):
        candidate = line.strip()
        if not candidate.startswith("{"):
            continue
        try:
            raw = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(raw, dict):
            return raw
    raise ValueError("process output contains no JSON object")


def process_error(completed: subprocess.CompletedProcess[str]) -> str:
    value = (completed.stderr or completed.stdout or "").strip()
    return value[-4000:] or f"process exited {completed.returncode}"
