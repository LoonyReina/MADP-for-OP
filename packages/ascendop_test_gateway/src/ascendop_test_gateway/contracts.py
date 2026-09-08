from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Mapping


REQUEST_SCHEMA = "ascendop.standalone-test-request.v1"
JOURNAL_SCHEMA = "ascendop.standalone-test-journal.v1"
TRANSPORT_RECEIPT_SCHEMA = "ascendop.standalone-transport-receipt.v1"
TRANSPORT_STATUS_SCHEMA = "ascendop.standalone-transport-status.v1"
ENGINE_CONTRACT_GENERATION = "wire-v3-engine-v3"
SAFE_TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
DEVICE_OPERATION_CODES = frozenset(
    {
        "test.correctness",
        "test.performance",
        "profile.collect",
        "correctness.replay",
    }
)


def canonical_artifact_root(value: str) -> str:
    """Collapse Windows long-path aliases used by different materializers."""

    if value.startswith("\\\\?\\UNC\\"):
        return "\\\\" + value[8:]
    if value.startswith("\\\\?\\"):
        return value[4:]
    return value


MATERIALIZATION_OPERATION_CODES = frozenset({"materialization.compile"})
STANDALONE_OPERATION_CODES = DEVICE_OPERATION_CODES | MATERIALIZATION_OPERATION_CODES


class GatewayContractError(ValueError):
    pass


class TestState(str, Enum):
    PREPARED = "prepared"
    PUBLISHING = "publishing"
    ACCEPTED = "accepted"
    RUNNING = "running"
    COLLECTING = "collecting"
    CANCELLING = "cancelling"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    UNCERTAIN = "uncertain"


TERMINAL_STATES = {
    TestState.COMPLETED,
    TestState.FAILED,
    TestState.CANCELLED,
}

ALLOWED_TRANSITIONS = {
    TestState.PREPARED: {
        TestState.PUBLISHING,
        TestState.FAILED,
        TestState.CANCELLED,
    },
    TestState.PUBLISHING: {
        TestState.ACCEPTED,
        TestState.RUNNING,
        TestState.COMPLETED,
        TestState.FAILED,
        TestState.UNCERTAIN,
    },
    TestState.ACCEPTED: {
        TestState.RUNNING,
        TestState.COLLECTING,
        TestState.COMPLETED,
        TestState.FAILED,
        TestState.CANCELLED,
        TestState.CANCELLING,
        TestState.UNCERTAIN,
    },
    TestState.RUNNING: {
        TestState.COLLECTING,
        TestState.COMPLETED,
        TestState.FAILED,
        TestState.CANCELLED,
        TestState.CANCELLING,
        TestState.UNCERTAIN,
    },
    TestState.COLLECTING: {
        TestState.COMPLETED,
        TestState.FAILED,
        TestState.CANCELLED,
        TestState.CANCELLING,
        TestState.UNCERTAIN,
    },
    TestState.UNCERTAIN: {
        TestState.PUBLISHING,
        TestState.ACCEPTED,
        TestState.RUNNING,
        TestState.COLLECTING,
        TestState.COMPLETED,
        TestState.FAILED,
        TestState.CANCELLED,
        TestState.CANCELLING,
    },
    TestState.CANCELLING: {
        TestState.COMPLETED,
        TestState.FAILED,
        TestState.CANCELLED,
        TestState.UNCERTAIN,
    },
    TestState.COMPLETED: set(),
    TestState.FAILED: set(),
    TestState.CANCELLED: set(),
}


def safe_token(value: str, field_name: str) -> str:
    normalized = str(value or "").strip()
    if not SAFE_TOKEN.fullmatch(normalized):
        raise GatewayContractError(f"{field_name} must be a safe non-empty token")
    return normalized


@dataclass(frozen=True)
class StandaloneTestRequest:
    workspace: Path
    task_case: Path
    op: str
    release: str
    test_version: str
    case_version: str = "standalone-v1"
    season: str = "standalone"
    hardware: str = ""
    vendor: str = ""
    mode: str = "correctness"
    operation_code: str = "test.correctness"
    operation_parameters: Mapping[str, Any] = field(default_factory=dict)
    request_id: str = ""
    attack_case: Path | None = None
    case_range: str = "1..5"
    perf_case_range: str = "1..5"
    transport: str = "auto"
    target_endpoint_id: str = ""
    engine_generation: str = ENGINE_CONTRACT_GENERATION
    timeout_seconds: int = 1800
    materialized_manifest: Path | None = None
    execution_package: Path | None = None
    execution_profile: Path | None = None

    def __post_init__(self) -> None:
        for name in (
            "op",
            "release",
            "test_version",
            "case_version",
            "season",
            "hardware",
            "engine_generation",
        ):
            safe_token(getattr(self, name), name)
        if self.vendor:
            safe_token(self.vendor, "vendor")
        if self.request_id:
            safe_token(self.request_id, "request_id")
        if self.target_endpoint_id:
            safe_token(self.target_endpoint_id, "target_endpoint_id")
        if self.mode not in {"correctness", "performance", "both", "compile"}:
            raise GatewayContractError(
                "mode must be correctness, performance, both, or compile"
            )
        if self.operation_code not in STANDALONE_OPERATION_CODES:
            raise GatewayContractError(
                f"unsupported standalone operation: {self.operation_code}"
            )
        if not isinstance(self.operation_parameters, Mapping):
            raise GatewayContractError("operation_parameters must be an object")
        try:
            json_safe_parameters = dict(self.operation_parameters)
            json.dumps(json_safe_parameters, ensure_ascii=True, sort_keys=True)
        except (TypeError, ValueError) as exc:
            raise GatewayContractError(
                "operation_parameters must be JSON serializable"
            ) from exc
        if self.operation_code == "test.performance" and self.mode == "correctness":
            raise GatewayContractError(
                "test.performance requires performance or both mode"
            )
        if (
            self.operation_code in {"profile.collect", "correctness.replay"}
            and self.mode != "correctness"
        ):
            raise GatewayContractError(
                f"{self.operation_code} requires correctness mode"
            )
        if self.operation_code == "materialization.compile":
            if (
                self.mode != "compile"
                or self.materialized_manifest is None
                or self.execution_package is not None
                or self.execution_profile is not None
            ):
                raise GatewayContractError(
                    "materialization.compile requires compile mode and a manifest"
                )
        elif self.mode == "compile" or self.materialized_manifest is not None:
            raise GatewayContractError(
                "compile mode and materialized manifest require materialization.compile"
            )
        has_execution_package = self.execution_package is not None
        has_execution_profile = self.execution_profile is not None
        if has_execution_package != has_execution_profile:
            raise GatewayContractError(
                "opaque correctness requires both execution package and profile"
            )
        if has_execution_package and (
            self.operation_code != "test.correctness" or self.mode != "correctness"
        ):
            raise GatewayContractError(
                "opaque execution package requires test.correctness in correctness mode"
            )
        if self.transport not in {"auto", "direct", "relay"}:
            raise GatewayContractError("transport must be auto, direct, or relay")
        if not isinstance(self.timeout_seconds, int) or self.timeout_seconds <= 0:
            raise GatewayContractError("timeout_seconds must be a positive integer")


@dataclass(frozen=True)
class PreparedBundle:
    request_id: str
    run_dir: Path
    request: Mapping[str, Any]

    @property
    def payload_dir(self) -> Path:
        return self.run_dir / "bundle"


@dataclass(frozen=True)
class TransportReceipt:
    request_id: str
    remote_attempt_id: str
    output_subdir: str
    accepted: bool = True
    details: Mapping[str, Any] = field(default_factory=dict)
    schema: str = TRANSPORT_RECEIPT_SCHEMA

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "request_id": self.request_id,
            "remote_attempt_id": self.remote_attempt_id,
            "output_subdir": self.output_subdir,
            "accepted": self.accepted,
            "details": dict(self.details),
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "TransportReceipt":
        if raw.get("schema") != TRANSPORT_RECEIPT_SCHEMA:
            raise GatewayContractError("unsupported transport receipt")
        return cls(
            request_id=safe_token(str(raw.get("request_id") or ""), "request_id"),
            remote_attempt_id=safe_token(
                str(raw.get("remote_attempt_id") or ""), "remote_attempt_id"
            ),
            output_subdir=str(raw.get("output_subdir") or ""),
            accepted=bool(raw.get("accepted")),
            details=dict(raw.get("details") or {}),
        )


@dataclass(frozen=True)
class TransportStatus:
    request_id: str
    state: TestState
    classification: str = ""
    result: Mapping[str, Any] = field(default_factory=dict)
    metrics: Mapping[str, Any] = field(default_factory=dict)
    schema: str = TRANSPORT_STATUS_SCHEMA

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "request_id": self.request_id,
            "state": self.state.value,
            "classification": self.classification,
            "result": dict(self.result),
            "metrics": dict(self.metrics),
        }


def terminal_ingest_event(
    receipt: TransportReceipt,
    status: TransportStatus,
) -> dict[str, Any]:
    """Bind daemon result ingest and GP acknowledgement to one exact terminal."""

    if status.state not in TERMINAL_STATES:
        raise GatewayContractError("result ingest requires a terminal status")
    result = status.result
    if str(result.get("schema") or "") != "ascendop.standalone-wire-v3-result.v1":
        raise GatewayContractError("result ingest requires a Wire V3 terminal result")
    request_id = safe_token(str(result.get("request_id") or ""), "request_id")
    attempt_id = safe_token(str(result.get("attempt_id") or ""), "attempt_id")
    receipt_id = safe_token(str(result.get("receipt_id") or ""), "receipt_id")
    if request_id != receipt.request_id or request_id != status.request_id:
        raise GatewayContractError("terminal result request identity mismatch")
    if attempt_id != receipt.remote_attempt_id:
        raise GatewayContractError("terminal result attempt identity mismatch")
    payload_sha256 = str(result.get("result_payload_sha256") or "").lower()
    if not SHA256.fullmatch(payload_sha256):
        raise GatewayContractError("terminal result payload digest is missing or invalid")
    envelope_sha256 = str(receipt.details.get("envelope_digest") or "").lower()
    if not SHA256.fullmatch(envelope_sha256):
        raise GatewayContractError("transport receipt envelope digest is missing or invalid")
    try:
        terminal_revision = int(result.get("terminal_revision", -1))
    except (TypeError, ValueError) as exc:
        raise GatewayContractError("terminal revision is invalid") from exc
    if terminal_revision < 0:
        raise GatewayContractError("terminal revision is invalid")
    identity = {
        "request_id": request_id,
        "attempt_id": attempt_id,
        "receipt_id": receipt_id,
        "terminal_revision": terminal_revision,
        "result_payload_sha256": payload_sha256,
        "envelope_digest": envelope_sha256,
    }
    event_id = hashlib.sha256(
        json.dumps(
            identity,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    ).hexdigest()
    return {
        "schema": "ascendop.gp-terminal-ingest-event.v1",
        "event_id": event_id,
        **identity,
        "outcome": str(result.get("outcome") or ""),
        "failure_domain": str(result.get("failure_domain") or ""),
        "artifact_root": canonical_artifact_root(
            str(result.get("artifact_root") or "")
        ),
        "ack": {
            "request_id": request_id,
            "attempt_id": attempt_id,
            "receipt_id": receipt_id,
        },
    }
