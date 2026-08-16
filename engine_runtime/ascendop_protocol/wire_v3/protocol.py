from __future__ import annotations

import copy
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping


FLOW_SCHEMA = "ascendop.flow.request.v3"
FLOW_VERSION = 3
# GitPartner transports individual files below 1 MiB. Wire V3 has no aggregate
# payload limit; it obtains that property by allowing an unbounded part count.
PART_MAX_BYTES = 960 * 1024

TOKEN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]*$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
EXTENSION_ID = re.compile(
    r"^[a-z0-9][a-z0-9_-]*(?:\.[a-z0-9][a-z0-9_-]*)+$"
)

OPERATION_KINDS = {
    "operator-test",
    "diagnostic-profile",
    "cache-prewarm",
    "maintenance",
}
PROFILER_MODES = {
    "none",
    "primary-all-cases",
    "primary-roofline-all-cases",
}
BUDGET_CLASSES = {"standard", "heavy", "diagnostic", "maintenance"}
RESOURCE_CLASSES = {
    "host-light",
    "host-build-heavy",
    "device",
    "export",
    "maintenance",
}
TERMINAL_STATES = {
    "terminal-success",
    "terminal-business-failure",
    "terminal-infrastructure-failure",
    "terminal-cancelled",
}
LIFECYCLE_TRANSITIONS: dict[str, frozenset[str]] = {
    "created": frozenset({"validated", "quarantined", "terminal-cancelled"}),
    "validated": frozenset({"queued", "quarantined", "terminal-cancelled"}),
    "queued": frozenset({"admitted", "quarantined", "terminal-cancelled"}),
    "admitted": frozenset({"dispatched", "quarantined", "terminal-cancelled"}),
    "dispatched": frozenset({"accepted", "queued", "quarantined", "terminal-cancelled"}),
    "accepted": frozenset({"running", "quarantined", "terminal-cancelled"}),
    "running": frozenset(
        {
            "return-ready",
            "quarantined",
            "terminal-business-failure",
            "terminal-infrastructure-failure",
            "terminal-cancelled",
        }
    ),
    "return-ready": frozenset({"ingested", "quarantined"}),
    "ingested": frozenset({"acknowledged", "quarantined"}),
    "acknowledged": frozenset(TERMINAL_STATES),
    "quarantined": frozenset({"queued", "terminal-infrastructure-failure", "terminal-cancelled"}),
    **{state: frozenset() for state in TERMINAL_STATES},
}


class FlowV3ProtocolError(ValueError):
    def __init__(self, code: str, detail: str, *, field: str = "") -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail
        self.field = field

    def to_nack(self, request_id: str = "") -> dict[str, Any]:
        return {
            "schema": "ascendop.flow.nack.v3",
            "request_id": request_id,
            "code": self.code,
            "field": self.field,
            "detail": self.detail,
            "retryable": False,
        }


@dataclass(frozen=True)
class ValidatedEnvelope:
    envelope: dict[str, Any]
    digest: str
    payload_bytes: int


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def canonical_json(value: Any) -> str:
    return canonical_json_bytes(value).decode("utf-8")


def canonical_digest(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def document_digest(value: Any) -> str:
    return canonical_digest(value)


def validate_envelope(raw: Mapping[str, Any]) -> ValidatedEnvelope:
    if not isinstance(raw, Mapping):
        raise FlowV3ProtocolError("invalid-envelope", "request envelope must be an object")
    required_sections = {
        "meta",
        "identity",
        "workflow",
        "payload",
        "execution",
        "retry_policy",
        "result_contract",
    }
    allowed_sections = required_sections | {"extensions"}
    missing = sorted(required_sections - set(raw))
    unknown = sorted(set(raw) - allowed_sections)
    if missing:
        raise FlowV3ProtocolError(
            "missing-section",
            "missing request envelope sections: " + ", ".join(missing),
        )
    if unknown:
        raise FlowV3ProtocolError(
            "unknown-section",
            "unknown request envelope sections: " + ", ".join(unknown),
        )
    envelope = copy.deepcopy(dict(raw))

    meta = object_section(envelope, "meta")
    require_exact(meta, "schema", FLOW_SCHEMA)
    require_exact(meta, "version", FLOW_VERSION)
    for field in (
        "request_id",
        "attempt_id",
        "trace_id",
        "producer",
        "code_generation",
    ):
        token(meta.get(field), f"meta.{field}")
    timestamp(meta.get("created_at"), "meta.created_at")

    identity = object_section(envelope, "identity")
    for field in (
        "endpoint_id",
        "endpoint_generation",
        "registration_generation",
        "idempotency_key",
    ):
        token(identity.get(field), f"identity.{field}")

    workflow = object_section(envelope, "workflow")
    for field in (
        "domain",
        "season",
        "operator",
        "test_version",
        "case_version",
        "gate",
        "operation_kind",
    ):
        token(workflow.get(field), f"workflow.{field}")
    operation_kind = str(workflow["operation_kind"])
    if operation_kind not in OPERATION_KINDS:
        raise FlowV3ProtocolError(
            "unsupported-operation",
            f"unsupported operation kind: {operation_kind}",
            field="workflow.operation_kind",
        )

    payload = object_section(envelope, "payload")
    sha256(payload.get("digest"), "payload.digest")
    payload_bytes = validate_parts(payload.get("parts"))

    execution = object_section(envelope, "execution")
    for field in (
        "profile",
        "budget_class",
        "budget_policy_version",
        "performance_mode",
    ):
        token(execution.get(field), f"execution.{field}")
    if execution["budget_class"] not in BUDGET_CLASSES:
        raise FlowV3ProtocolError(
            "unsupported-budget-class",
            f"unsupported budget class: {execution['budget_class']}",
            field="execution.budget_class",
        )
    if execution["performance_mode"] not in PROFILER_MODES:
        raise FlowV3ProtocolError(
            "unsupported-profiler-mode",
            f"unsupported profiler mode: {execution['performance_mode']}",
            field="execution.performance_mode",
        )
    requested = nonnegative_int(
        execution.get("requested_device_session_seconds"),
        "execution.requested_device_session_seconds",
    )
    granted = positive_int(
        execution.get("granted_device_session_seconds"),
        "execution.granted_device_session_seconds",
    )
    if requested and granted > requested:
        raise FlowV3ProtocolError(
            "budget-overgrant",
            "granted device session budget cannot exceed the requested budget",
            field="execution.granted_device_session_seconds",
        )
    if not isinstance(execution.get("correctness_required"), bool):
        raise FlowV3ProtocolError(
            "invalid-boolean",
            "execution.correctness_required must be boolean",
            field="execution.correctness_required",
        )
    if not isinstance(execution.get("publish_eligible"), bool):
        raise FlowV3ProtocolError(
            "invalid-boolean",
            "execution.publish_eligible must be boolean",
            field="execution.publish_eligible",
        )
    resources = object_value(execution.get("resources"), "execution.resources")
    for field in ("host_cpu_weight", "host_memory_mb", "host_io_weight", "device_count"):
        nonnegative_int(resources.get(field), f"execution.resources.{field}")
    stages = validate_stages(execution.get("stages"))

    retry_policy = object_section(envelope, "retry_policy")
    token(retry_policy.get("policy_id"), "retry_policy.policy_id")
    nonnegative_int(retry_policy.get("max_execution_attempts"), "retry_policy.max_execution_attempts")
    nonnegative_int(retry_policy.get("max_transport_retries"), "retry_policy.max_transport_retries")
    nonnegative_int(retry_policy.get("max_idempotent_stage_retries"), "retry_policy.max_idempotent_stage_retries")

    result_contract = object_section(envelope, "result_contract")
    token(result_contract.get("ingest_adapter"), "result_contract.ingest_adapter")
    terminal_states = string_list(
        result_contract.get("terminal_states"),
        "result_contract.terminal_states",
    )
    if set(terminal_states) - TERMINAL_STATES:
        raise FlowV3ProtocolError(
            "invalid-terminal-state",
            "result_contract contains an unsupported terminal state",
            field="result_contract.terminal_states",
        )
    string_list(
        result_contract.get("required_artifacts"),
        "result_contract.required_artifacts",
    )
    required_by_terminal = result_contract.get(
        "required_artifacts_by_terminal_state", {}
    )
    if not isinstance(required_by_terminal, Mapping):
        raise FlowV3ProtocolError(
            "invalid-object",
            "result_contract.required_artifacts_by_terminal_state must be an object",
            field="result_contract.required_artifacts_by_terminal_state",
        )
    for terminal_state, artifacts in required_by_terminal.items():
        if terminal_state not in TERMINAL_STATES:
            raise FlowV3ProtocolError(
                "invalid-terminal-state",
                "result contract contains an unsupported conditional terminal state",
                field="result_contract.required_artifacts_by_terminal_state",
            )
        string_list(
            artifacts,
            "result_contract.required_artifacts_by_terminal_state."
            + terminal_state,
        )
    validate_extensions(envelope.get("extensions", {}))

    validate_operation_contract(
        operation_kind=operation_kind,
        execution=execution,
        stages=stages,
    )
    return ValidatedEnvelope(
        envelope=envelope,
        digest=canonical_digest(envelope),
        payload_bytes=payload_bytes,
    )


def validate_operation_contract(
    *,
    operation_kind: str,
    execution: dict[str, Any],
    stages: list[dict[str, Any]],
) -> None:
    names = [str(stage["name"]) for stage in stages]
    if "operator-build" in names and "runtime-install" in names:
        if not stage_depends_on(
            stages,
            stage_name="runtime-install",
            required_ancestor="operator-build",
        ):
            raise FlowV3ProtocolError(
                "runtime-before-operator-build",
                "runtime-install must depend on operator-build",
                field="execution.stages",
            )
    if operation_kind == "operator-test":
        if not execution["correctness_required"]:
            raise FlowV3ProtocolError(
                "correctness-required",
                "operator-test requires correctness",
                field="execution.correctness_required",
            )
        if "correctness" not in names:
            raise FlowV3ProtocolError(
                "missing-correctness-stage",
                "operator-test requires a correctness stage",
                field="execution.stages",
            )
        performance_names = [
            name
            for name in names
            if name in {"performance-primary", "performance-roofline"}
        ]
        for name in performance_names:
            stage = next(item for item in stages if item["name"] == name)
            if "correctness" not in stage["depends_on"]:
                raise FlowV3ProtocolError(
                    "performance-before-correctness",
                    f"{name} must depend on correctness",
                    field="execution.stages",
                )
    if operation_kind == "diagnostic-profile":
        if execution["performance_mode"] == "none":
            raise FlowV3ProtocolError(
                "diagnostic-profiler-required",
                "diagnostic-profile requires a profiler mode",
                field="execution.performance_mode",
            )
        if execution["publish_eligible"]:
            raise FlowV3ProtocolError(
                "diagnostic-not-publishable",
                "diagnostic-profile cannot be publish eligible",
                field="execution.publish_eligible",
            )


def stage_depends_on(
    stages: list[dict[str, Any]],
    *,
    stage_name: str,
    required_ancestor: str,
) -> bool:
    dependencies = {
        str(stage["name"]): set(str(item) for item in stage["depends_on"])
        for stage in stages
    }
    pending = list(dependencies.get(stage_name, set()))
    visited: set[str] = set()
    while pending:
        current = pending.pop()
        if current == required_ancestor:
            return True
        if current in visited:
            continue
        visited.add(current)
        pending.extend(dependencies.get(current, set()) - visited)
    return False


def validate_stages(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value:
        raise FlowV3ProtocolError(
            "invalid-stages",
            "execution.stages must be a non-empty list",
            field="execution.stages",
        )
    stages: list[dict[str, Any]] = []
    names: set[str] = set()
    for index, raw in enumerate(value):
        stage = object_value(raw, f"execution.stages[{index}]")
        name = token(stage.get("name"), f"execution.stages[{index}].name")
        if name in names:
            raise FlowV3ProtocolError(
                "duplicate-stage",
                f"duplicate stage name: {name}",
                field="execution.stages",
            )
        names.add(name)
        resource_class = token(
            stage.get("resource_class"),
            f"execution.stages[{index}].resource_class",
        )
        if resource_class not in RESOURCE_CLASSES:
            raise FlowV3ProtocolError(
                "unsupported-resource-class",
                f"unsupported resource class: {resource_class}",
                field=f"execution.stages[{index}].resource_class",
            )
        depends_on = string_list(
            stage.get("depends_on", []),
            f"execution.stages[{index}].depends_on",
        )
        if not isinstance(stage.get("idempotent"), bool):
            raise FlowV3ProtocolError(
                "invalid-boolean",
                "stage idempotent must be boolean",
                field=f"execution.stages[{index}].idempotent",
            )
        nonnegative_int(
            stage.get("timeout_seconds", 0),
            f"execution.stages[{index}].timeout_seconds",
        )
        stages.append(
            {
                **stage,
                "name": name,
                "resource_class": resource_class,
                "depends_on": depends_on,
            }
        )
    for stage in stages:
        unknown_dependencies = sorted(set(stage["depends_on"]) - names)
        if unknown_dependencies:
            raise FlowV3ProtocolError(
                "unknown-stage-dependency",
                f"{stage['name']} depends on unknown stages: "
                + ", ".join(unknown_dependencies),
                field="execution.stages",
            )
        if stage["name"] in stage["depends_on"]:
            raise FlowV3ProtocolError(
                "cyclic-stage-dependency",
                f"{stage['name']} depends on itself",
                field="execution.stages",
            )
    ensure_acyclic(stages)
    return stages


def ensure_acyclic(stages: list[dict[str, Any]]) -> None:
    dependencies = {
        str(stage["name"]): set(str(item) for item in stage["depends_on"])
        for stage in stages
    }
    pending = dict(dependencies)
    while pending:
        ready = {name for name, values in pending.items() if not values}
        if not ready:
            raise FlowV3ProtocolError(
                "cyclic-stage-dependency",
                "execution stage graph contains a cycle",
                field="execution.stages",
            )
        pending = {
            name: values - ready
            for name, values in pending.items()
            if name not in ready
        }


def validate_lifecycle_transition(previous: str, current: str) -> None:
    if previous == current:
        return
    if previous not in LIFECYCLE_TRANSITIONS or current not in LIFECYCLE_TRANSITIONS:
        raise FlowV3ProtocolError(
            "invalid-lifecycle-state",
            f"unknown lifecycle transition {previous!r} -> {current!r}",
        )
    if current not in LIFECYCLE_TRANSITIONS[previous]:
        raise FlowV3ProtocolError(
            "nonmonotonic-lifecycle-transition",
            f"illegal lifecycle transition {previous!r} -> {current!r}",
        )


def build_envelope(
    *,
    meta: Mapping[str, Any],
    identity: Mapping[str, Any],
    workflow: Mapping[str, Any],
    payload: Mapping[str, Any],
    execution: Mapping[str, Any],
    retry_policy: Mapping[str, Any],
    result_contract: Mapping[str, Any],
    extensions: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    envelope = {
        "meta": {
            "schema": FLOW_SCHEMA,
            "version": FLOW_VERSION,
            "created_at": utc_now_iso(),
            **dict(meta),
        },
        "identity": dict(identity),
        "workflow": dict(workflow),
        "payload": dict(payload),
        "execution": dict(execution),
        "retry_policy": dict(retry_policy),
        "result_contract": dict(result_contract),
        "extensions": dict(extensions or {}),
    }
    return validate_envelope(envelope).envelope


def validate_extensions(value: Any) -> dict[str, Any]:
    extensions = object_value(value, "extensions")
    for extension_id, raw in extensions.items():
        if not isinstance(extension_id, str) or not EXTENSION_ID.fullmatch(
            extension_id
        ):
            raise FlowV3ProtocolError(
                "invalid-extension-id",
                "extension ids must be lower-case dotted namespaces",
                field="extensions",
            )
        extension = object_value(raw, f"extensions.{extension_id}")
        token(
            extension.get("schema"),
            f"extensions.{extension_id}.schema",
        )
        positive_int(
            extension.get("version"),
            f"extensions.{extension_id}.version",
        )
        capability = str(extension.get("required_capability") or "")
        if capability:
            token(
                capability,
                f"extensions.{extension_id}.required_capability",
            )
        if "payload" not in extension:
            raise FlowV3ProtocolError(
                "missing-extension-payload",
                f"extension {extension_id} requires payload",
                field=f"extensions.{extension_id}.payload",
            )
        try:
            canonical_json_bytes(extension["payload"])
        except (TypeError, ValueError) as exc:
            raise FlowV3ProtocolError(
                "invalid-extension-payload",
                f"extension {extension_id} payload is not canonical JSON",
                field=f"extensions.{extension_id}.payload",
            ) from exc
    return extensions


def validate_parts(value: Any) -> int:
    if not isinstance(value, list):
        raise FlowV3ProtocolError(
            "invalid-parts",
            "payload.parts must be a list",
            field="payload.parts",
        )
    indexes: set[int] = set()
    part_ids: set[str] = set()
    total = 0
    for position, raw in enumerate(value):
        part = object_value(raw, f"payload.parts[{position}]")
        part_id = token(part.get("part_id"), f"payload.parts[{position}].part_id")
        index = nonnegative_int(part.get("index"), f"payload.parts[{position}].index")
        size = nonnegative_int(
            part.get("size_bytes"),
            f"payload.parts[{position}].size_bytes",
        )
        sha256(part.get("sha256"), f"payload.parts[{position}].sha256")
        relative_path(part.get("path"), f"payload.parts[{position}].path")
        if size > PART_MAX_BYTES:
            raise FlowV3ProtocolError(
                "part-too-large",
                f"payload part exceeds {PART_MAX_BYTES} bytes; split it",
                field=f"payload.parts[{position}].size_bytes",
            )
        if index in indexes or part_id in part_ids:
            raise FlowV3ProtocolError(
                "duplicate-part",
                "payload part identity must be unique",
                field="payload.parts",
            )
        indexes.add(index)
        part_ids.add(part_id)
        total += size
    if indexes != set(range(len(value))):
        raise FlowV3ProtocolError(
            "noncontiguous-parts",
            "payload part indexes must be contiguous from zero",
            field="payload.parts",
        )
    return total


def object_section(envelope: dict[str, Any], name: str) -> dict[str, Any]:
    return object_value(envelope.get(name), name)


def object_value(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise FlowV3ProtocolError(
            "invalid-object",
            f"{field} must be an object",
            field=field,
        )
    return value


def token(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value or not TOKEN.fullmatch(value):
        raise FlowV3ProtocolError(
            "invalid-token",
            f"{field} must be a non-empty protocol token",
            field=field,
        )
    return value


def sha256(value: Any, field: str) -> str:
    if not isinstance(value, str) or not SHA256.fullmatch(value):
        raise FlowV3ProtocolError(
            "invalid-sha256",
            f"{field} must be a lowercase SHA-256 digest",
            field=field,
        )
    return value


def timestamp(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise FlowV3ProtocolError(
            "invalid-timestamp",
            f"{field} must be an ISO-8601 timestamp",
            field=field,
        )
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise FlowV3ProtocolError(
            "invalid-timestamp",
            f"{field} must be an ISO-8601 timestamp",
            field=field,
        ) from exc
    return value


def positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise FlowV3ProtocolError(
            "invalid-integer",
            f"{field} must be a positive integer",
            field=field,
        )
    return value


def nonnegative_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FlowV3ProtocolError(
            "invalid-integer",
            f"{field} must be a non-negative integer",
            field=field,
        )
    return value


def string_list(value: Any, field: str) -> list[str]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item for item in value
    ):
        raise FlowV3ProtocolError(
            "invalid-string-list",
            f"{field} must be a list of non-empty strings",
            field=field,
        )
    return list(value)


def relative_path(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise FlowV3ProtocolError(
            "invalid-relative-path",
            f"{field} must be a non-empty relative path",
            field=field,
        )
    normalized = value.replace("\\", "/")
    if normalized.startswith("/") or re.match(r"^[A-Za-z]:", normalized):
        raise FlowV3ProtocolError(
            "absolute-path",
            f"{field} must be relative",
            field=field,
        )
    if any(part in {"", ".", ".."} for part in normalized.split("/")):
        raise FlowV3ProtocolError(
            "unsafe-path",
            f"{field} contains an unsafe component",
            field=field,
        )
    return value


def require_exact(mapping: Mapping[str, Any], field: str, expected: Any) -> None:
    if mapping.get(field) != expected:
        raise FlowV3ProtocolError(
            "unsupported-version",
            f"{field} must be {expected!r}",
            field=f"meta.{field}",
        )


def required_capabilities(envelope: Mapping[str, Any]) -> set[str]:
    execution = envelope.get("execution", {})
    values = execution.get("required_capabilities", [])
    return set(string_list(values, "execution.required_capabilities"))


def assert_component_compatible(
    envelope: Mapping[str, Any],
    *,
    wire_versions: Iterable[int],
    capabilities: Iterable[str],
    endpoint_id: str,
    endpoint_generation: str,
) -> None:
    validated = validate_envelope(envelope)
    if FLOW_VERSION not in set(int(item) for item in wire_versions):
        raise FlowV3ProtocolError(
            "component-version-mismatch",
            "component does not support Wire V3",
            field="meta.version",
        )
    identity = validated.envelope["identity"]
    if identity["endpoint_id"] != endpoint_id:
        raise FlowV3ProtocolError(
            "endpoint-mismatch",
            "request endpoint does not match the consumer",
            field="identity.endpoint_id",
        )
    if identity["endpoint_generation"] != endpoint_generation:
        raise FlowV3ProtocolError(
            "endpoint-generation-mismatch",
            "request endpoint generation mismatch with the consumer",
            field="identity.endpoint_generation",
        )
    missing = required_capabilities(validated.envelope) - set(capabilities)
    if missing:
        raise FlowV3ProtocolError(
            "missing-capability",
            "consumer is missing capabilities: " + ", ".join(sorted(missing)),
            field="execution.required_capabilities",
        )


def assert_target(
    envelope: Mapping[str, Any],
    *,
    endpoint_id: str,
    endpoint_generation: str,
) -> ValidatedEnvelope:
    assert_component_compatible(
        envelope,
        wire_versions=(FLOW_VERSION,),
        capabilities=required_capabilities(envelope),
        endpoint_id=endpoint_id,
        endpoint_generation=endpoint_generation,
    )
    return validate_envelope(envelope)
