from __future__ import annotations

import copy
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping


WIRE_SCHEMA = "ascendop.wire.v2"
WIRE_VERSION = 2
MAX_PART_BYTES = 64 * 1024 * 1024
LAYER_NAMES = (
    "header",
    "workflow",
    "route",
    "delivery",
    "operation",
    "payload",
    "result_contract",
)
NAMESPACED_EXTENSION = re.compile(
    r"^[a-z0-9]+(?:[.-][a-z0-9]+)+(?:/[A-Za-z0-9._-]+)?$"
)
TOKEN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]*$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")

LAYER_REQUIRED_FIELDS: dict[str, tuple[str, ...]] = {
    "header": ("schema", "packet_id", "created_at", "producer", "trace_id"),
    "workflow": (
        "profile_id",
        "profile_revision",
        "instance_id",
        "domain",
        "season_id",
        "subject_kind",
        "subject_id",
        "state_generation",
    ),
    "route": ("source", "destination", "endpoint_id"),
    "delivery": (
        "state",
        "sequence",
        "attempt",
        "idempotency_key",
    ),
    "operation": (
        "type",
        "operation_version",
        "owner",
        "required_capabilities",
    ),
    "payload": ("digest", "parts"),
    "result_contract": (
        "ingest_adapter",
        "terminal_states",
        "required_artifacts",
    ),
}

DELIVERY_TRANSITIONS: dict[str, frozenset[str]] = {
    "created": frozenset({"queued", "quarantined", "cancelled"}),
    "queued": frozenset({"claimed", "failed", "quarantined", "cancelled"}),
    "claimed": frozenset({"sending", "queued", "failed", "quarantined", "cancelled"}),
    "sending": frozenset({"delivered", "queued", "failed", "quarantined", "cancelled"}),
    "delivered": frozenset({"accepted", "failed", "quarantined", "cancelled"}),
    "accepted": frozenset({"executing", "failed", "cancelled"}),
    "executing": frozenset({"result_ready", "failed", "cancelled"}),
    "result_ready": frozenset({"ingested", "failed"}),
    "ingested": frozenset({"acknowledged", "failed"}),
    "acknowledged": frozenset(),
    "failed": frozenset(),
    "quarantined": frozenset(),
    "cancelled": frozenset(),
}


class WireProtocolError(ValueError):
    def __init__(
        self,
        code: str,
        detail: str,
        *,
        layer: str = "",
        retryable: bool = False,
    ) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail
        self.layer = layer
        self.retryable = retryable

    def to_nack(self, packet_id: str = "") -> dict[str, Any]:
        return {
            "schema": "ascendop.wire.nack.v2",
            "packet_id": packet_id,
            "code": self.code,
            "layer": self.layer,
            "detail": self.detail,
            "retryable": self.retryable,
        }


@dataclass(frozen=True)
class ValidatedPacket:
    packet: dict[str, Any]
    digest: str
    total_payload_bytes: int


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def canonical_digest(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def validate_packet(
    packet: Mapping[str, Any],
    *,
    capabilities: Iterable[str] = (),
    supported_extensions: Iterable[str] = (),
) -> ValidatedPacket:
    if not isinstance(packet, Mapping):
        raise WireProtocolError("invalid-packet", "packet must be an object")
    missing_layers = [name for name in LAYER_NAMES if name not in packet]
    if missing_layers:
        raise WireProtocolError(
            "missing-layer",
            "missing packet layers: " + ", ".join(missing_layers),
        )
    unknown_layers = sorted(set(packet) - set(LAYER_NAMES))
    if unknown_layers:
        raise WireProtocolError(
            "unknown-layer",
            "unknown top-level layers: " + ", ".join(unknown_layers),
        )

    known_extensions = set(str(item) for item in supported_extensions)
    normalized = copy.deepcopy(dict(packet))
    for layer_name in LAYER_NAMES:
        layer = normalized[layer_name]
        validate_layer(layer_name, layer, known_extensions)

    header = normalized["header"]
    if header["schema"] != WIRE_SCHEMA:
        raise WireProtocolError(
            "unsupported-schema",
            f"unsupported wire schema: {header['schema']}",
            layer="header",
        )
    for field in ("packet_id", "producer", "trace_id"):
        validate_token(header[field], field=field, layer="header")
    validate_timestamp(header["created_at"], field="created_at", layer="header")

    workflow = normalized["workflow"]
    for field in (
        "profile_id",
        "profile_revision",
        "instance_id",
        "domain",
        "season_id",
        "subject_kind",
        "subject_id",
    ):
        validate_token(workflow[field], field=field, layer="workflow")
    require_nonnegative_int(
        workflow["state_generation"],
        field="state_generation",
        layer="workflow",
    )

    route = normalized["route"]
    for field in ("source", "destination"):
        validate_token(route[field], field=field, layer="route")
    if route["endpoint_id"]:
        validate_token(route["endpoint_id"], field="endpoint_id", layer="route")

    delivery = normalized["delivery"]
    delivery_state = str(delivery["state"])
    if delivery_state not in DELIVERY_TRANSITIONS:
        raise WireProtocolError(
            "invalid-delivery-state",
            f"invalid delivery state: {delivery_state}",
            layer="delivery",
        )
    require_nonnegative_int(delivery["sequence"], field="sequence", layer="delivery")
    require_nonnegative_int(delivery["attempt"], field="attempt", layer="delivery")
    validate_token(
        delivery["idempotency_key"],
        field="idempotency_key",
        layer="delivery",
    )

    operation = normalized["operation"]
    validate_token(operation["type"], field="type", layer="operation")
    validate_token(
        operation["operation_version"],
        field="operation_version",
        layer="operation",
    )
    validate_token(operation["owner"], field="owner", layer="operation")
    required_capabilities = string_list(
        operation["required_capabilities"],
        field="required_capabilities",
        layer="operation",
    )
    missing_capabilities = sorted(set(required_capabilities) - set(capabilities))
    if missing_capabilities:
        raise WireProtocolError(
            "unsupported-capability",
            "missing required capabilities: " + ", ".join(missing_capabilities),
            layer="operation",
        )

    payload = normalized["payload"]
    validate_sha256(payload["digest"], field="digest", layer="payload")
    total_payload_bytes = validate_parts(payload["parts"])

    result_contract = normalized["result_contract"]
    validate_token(
        result_contract["ingest_adapter"],
        field="ingest_adapter",
        layer="result_contract",
    )
    terminal_states = string_list(
        result_contract["terminal_states"],
        field="terminal_states",
        layer="result_contract",
    )
    if not terminal_states:
        raise WireProtocolError(
            "empty-terminal-states",
            "result terminal_states cannot be empty",
            layer="result_contract",
        )
    string_list(
        result_contract["required_artifacts"],
        field="required_artifacts",
        layer="result_contract",
    )

    return ValidatedPacket(
        packet=normalized,
        digest=canonical_digest(normalized),
        total_payload_bytes=total_payload_bytes,
    )


def validate_layer(
    name: str,
    layer: Any,
    supported_extensions: set[str],
) -> None:
    if not isinstance(layer, dict):
        raise WireProtocolError(
            "invalid-layer",
            f"{name} layer must be an object",
            layer=name,
        )
    require_positive_int(layer.get("version"), field="version", layer=name)
    extensions = layer.get("extensions")
    if not isinstance(extensions, dict):
        raise WireProtocolError(
            "invalid-extensions",
            f"{name}.extensions must be an object",
            layer=name,
        )
    for key in extensions:
        if not isinstance(key, str) or not NAMESPACED_EXTENSION.fullmatch(key):
            raise WireProtocolError(
                "invalid-extension-key",
                f"invalid extension key in {name}: {key!r}",
                layer=name,
            )
    required_extensions = string_list(
        layer.get("required_extensions", []),
        field="required_extensions",
        layer=name,
    )
    undeclared = sorted(set(required_extensions) - set(extensions))
    if undeclared:
        raise WireProtocolError(
            "missing-required-extension-data",
            "required extensions have no payload: " + ", ".join(undeclared),
            layer=name,
        )
    unsupported = sorted(set(required_extensions) - supported_extensions)
    if unsupported:
        raise WireProtocolError(
            "unsupported-extension",
            "unsupported required extensions: " + ", ".join(unsupported),
            layer=name,
        )
    missing_fields = [
        field for field in LAYER_REQUIRED_FIELDS[name] if field not in layer
    ]
    if missing_fields:
        raise WireProtocolError(
            "missing-field",
            f"{name} is missing fields: " + ", ".join(missing_fields),
            layer=name,
        )


def validate_parts(value: Any) -> int:
    if not isinstance(value, list):
        raise WireProtocolError(
            "invalid-parts",
            "payload.parts must be a list",
            layer="payload",
        )
    seen_ids: set[str] = set()
    seen_indexes: set[int] = set()
    total = 0
    for position, part in enumerate(value):
        if not isinstance(part, dict):
            raise WireProtocolError(
                "invalid-part",
                f"payload part {position} must be an object",
                layer="payload",
            )
        required = {"part_id", "index", "size_bytes", "sha256", "path"}
        missing = sorted(required - set(part))
        if missing:
            raise WireProtocolError(
                "missing-part-field",
                f"payload part {position} is missing: " + ", ".join(missing),
                layer="payload",
            )
        validate_token(part["part_id"], field="part_id", layer="payload")
        validate_sha256(part["sha256"], field="sha256", layer="payload")
        validate_relative_path(part["path"])
        index = require_nonnegative_int(
            part["index"],
            field="index",
            layer="payload",
        )
        size = require_nonnegative_int(
            part["size_bytes"],
            field="size_bytes",
            layer="payload",
        )
        if size > MAX_PART_BYTES:
            raise WireProtocolError(
                "part-too-large",
                f"payload part {position} is {size} bytes; max is {MAX_PART_BYTES}",
                layer="payload",
            )
        part_id = str(part["part_id"])
        if part_id in seen_ids or index in seen_indexes:
            raise WireProtocolError(
                "duplicate-part",
                f"duplicate payload part identity at position {position}",
                layer="payload",
            )
        seen_ids.add(part_id)
        seen_indexes.add(index)
        total += size
    if seen_indexes != set(range(len(value))):
        raise WireProtocolError(
            "noncontiguous-parts",
            "payload part indexes must be contiguous from zero",
            layer="payload",
        )
    return total


def validate_delivery_transition(previous: str, current: str) -> None:
    if previous == current:
        return
    if previous not in DELIVERY_TRANSITIONS or current not in DELIVERY_TRANSITIONS:
        raise WireProtocolError(
            "invalid-delivery-state",
            f"unknown delivery transition {previous!r} -> {current!r}",
            layer="delivery",
        )
    if current not in DELIVERY_TRANSITIONS[previous]:
        raise WireProtocolError(
            "nonmonotonic-delivery-transition",
            f"illegal delivery transition {previous!r} -> {current!r}",
            layer="delivery",
        )


def build_packet(
    *,
    packet_id: str,
    trace_id: str,
    producer: str,
    profile_id: str,
    profile_revision: str,
    instance_id: str,
    domain: str,
    season_id: str,
    subject_kind: str,
    subject_id: str,
    state_generation: int,
    source: str,
    destination: str,
    endpoint_id: str,
    delivery_state: str,
    sequence: int,
    attempt: int,
    idempotency_key: str,
    operation_type: str,
    operation_version: str,
    owner: str,
    required_capabilities: Iterable[str],
    payload_digest: str,
    parts: list[dict[str, Any]],
    ingest_adapter: str,
    terminal_states: Iterable[str],
    required_artifacts: Iterable[str],
) -> dict[str, Any]:
    def layer_base() -> dict[str, Any]:
        return {"version": 1, "extensions": {}, "required_extensions": []}

    return {
        "header": {
            **layer_base(),
            "schema": WIRE_SCHEMA,
            "packet_id": packet_id,
            "created_at": utc_now_iso(),
            "producer": producer,
            "trace_id": trace_id,
        },
        "workflow": {
            **layer_base(),
            "profile_id": profile_id,
            "profile_revision": profile_revision,
            "instance_id": instance_id,
            "domain": domain,
            "season_id": season_id,
            "subject_kind": subject_kind,
            "subject_id": subject_id,
            "state_generation": state_generation,
        },
        "route": {
            **layer_base(),
            "source": source,
            "destination": destination,
            "endpoint_id": endpoint_id,
        },
        "delivery": {
            **layer_base(),
            "state": delivery_state,
            "sequence": sequence,
            "attempt": attempt,
            "idempotency_key": idempotency_key,
        },
        "operation": {
            **layer_base(),
            "type": operation_type,
            "operation_version": operation_version,
            "owner": owner,
            "required_capabilities": list(required_capabilities),
        },
        "payload": {
            **layer_base(),
            "digest": payload_digest,
            "parts": parts,
        },
        "result_contract": {
            **layer_base(),
            "ingest_adapter": ingest_adapter,
            "terminal_states": list(terminal_states),
            "required_artifacts": list(required_artifacts),
        },
    }


def validate_token(value: Any, *, field: str, layer: str) -> str:
    if not isinstance(value, str) or not value or not TOKEN.fullmatch(value):
        raise WireProtocolError(
            "invalid-token",
            f"{layer}.{field} must be a non-empty protocol token",
            layer=layer,
        )
    return value


def validate_timestamp(value: Any, *, field: str, layer: str) -> str:
    if not isinstance(value, str) or not value:
        raise WireProtocolError(
            "invalid-timestamp",
            f"{layer}.{field} must be an ISO-8601 timestamp",
            layer=layer,
        )
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise WireProtocolError(
            "invalid-timestamp",
            f"{layer}.{field} must be an ISO-8601 timestamp",
            layer=layer,
        ) from exc
    return value


def validate_sha256(value: Any, *, field: str, layer: str) -> str:
    if not isinstance(value, str) or not SHA256.fullmatch(value):
        raise WireProtocolError(
            "invalid-sha256",
            f"{layer}.{field} must be a lowercase SHA-256 digest",
            layer=layer,
        )
    return value


def validate_relative_path(value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise WireProtocolError(
            "invalid-part-path",
            "payload part path must be non-empty",
            layer="payload",
        )
    normalized = value.replace("\\", "/")
    if normalized.startswith("/") or re.match(r"^[A-Za-z]:", normalized):
        raise WireProtocolError(
            "absolute-part-path",
            f"payload part path must be relative: {value}",
            layer="payload",
        )
    if any(part in {"", ".", ".."} for part in normalized.split("/")):
        raise WireProtocolError(
            "unsafe-part-path",
            f"payload part path is unsafe: {value}",
            layer="payload",
        )
    return value


def require_positive_int(value: Any, *, field: str, layer: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise WireProtocolError(
            "invalid-integer",
            f"{layer}.{field} must be a positive integer",
            layer=layer,
        )
    return value


def require_nonnegative_int(value: Any, *, field: str, layer: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise WireProtocolError(
            "invalid-integer",
            f"{layer}.{field} must be a non-negative integer",
            layer=layer,
        )
    return value


def string_list(value: Any, *, field: str, layer: str) -> list[str]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item for item in value
    ):
        raise WireProtocolError(
            "invalid-string-list",
            f"{layer}.{field} must be a list of non-empty strings",
            layer=layer,
        )
    return list(value)
