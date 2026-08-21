from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any

from ascendop_daemon.registry.system_registry import SystemRegistry
from ascendop_daemon.storage.control_types import (
    ACTIVE_NODE_STATES,
    BOOTSTRAP_CONTROL_PROBE_POLICY,
    SYSTEM_EXPERIMENT_OPERATOR_ID,
    ControlDatabaseError,
)
def _require_transport_claim(
    conn: sqlite3.Connection,
    outbox_id: str,
    consumer: str,
    claim_token: str,
    allowed_states: set[str],
) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM transport_outbox WHERE outbox_id=?",
        (outbox_id,),
    ).fetchone()
    if row is None:
        raise ControlDatabaseError(f"unknown transport outbox: {outbox_id}")
    if str(row["state"]) not in allowed_states:
        raise ControlDatabaseError(
            f"transport claim state mismatch: {outbox_id}={row['state']}"
        )
    if (
        str(row["claimed_by"]) != consumer
        or str(row["claim_token"]) != claim_token
    ):
        raise ControlDatabaseError(
            f"transport claim identity mismatch: {outbox_id}"
        )
    if _parse_timestamp(
        str(row["claim_expires_at"]), "claim_expires_at"
    ) <= datetime.now(timezone.utc):
        raise ControlDatabaseError(f"transport claim has expired: {outbox_id}")
    return row


def _validate_transport_identity(
    expected: dict[str, Any],
    observed: dict[str, Any],
    *,
    require_receipt_id: bool = False,
) -> None:
    if not isinstance(observed, dict):
        raise ControlDatabaseError("transport receipt must be an object")
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
        if str(observed.get(key) or "") != str(expected.get(key) or ""):
            raise ControlDatabaseError(
                f"transport receipt identity mismatch for {key}"
            )
    if require_receipt_id and not str(observed.get("receipt_id") or ""):
        raise ControlDatabaseError("transport return has no receipt_id")


def _runtime_route_rejection_reasons(
    conn: sqlite3.Connection,
    endpoint: Any,
    requirements: dict[str, Any],
    *,
    allow_unobserved_node: bool = False,
) -> list[str]:
    if allow_unobserved_node:
        return []
    node = conn.execute(
        "SELECT * FROM observed_nodes WHERE node_id=?",
        (endpoint.node_id,),
    ).fetchone()
    if node is None:
        return ["node-not-observed"]
    reasons: list[str] = []
    admission_state = str(node["admission_state"])
    if admission_state != "accepted":
        reasons.append(f"node-not-accepted:{admission_state}")
    state = str(node["state"])
    if state != "ready":
        reasons.append(f"node-not-ready:{state}")
    try:
        lease = _parse_timestamp(str(node["lease_expires_at"]), "lease_expires_at")
    except ControlDatabaseError:
        reasons.append("node-lease-invalid")
    else:
        if lease <= datetime.now(timezone.utc):
            reasons.append("node-lease-expired")
    if str(node["endpoint_id"]) != endpoint.endpoint_id:
        reasons.append(f"node-endpoint-mismatch:{node['endpoint_id']}")
    if str(node["generation"]) != endpoint.generation:
        reasons.append("node-generation-mismatch")
    capability_generation = str(node["capability_generation"])
    if not capability_generation:
        reasons.append("node-capability-generation-missing")
    required_session = str(requirements.get("node_session_id") or "")
    if required_session and str(node["current_session_id"]) != required_session:
        reasons.append("node-session-mismatch")
    required_capability_generation = str(
        requirements.get("capability_generation") or ""
    )
    if (
        required_capability_generation
        and capability_generation != required_capability_generation
    ):
        reasons.append("node-capability-generation-mismatch")
    report = json.loads(str(node["report_json"]))
    if (
        str(report.get("execution_environment_id") or "")
        != endpoint.execution_environment_id
    ):
        reasons.append(
            "node-environment-mismatch:"
            + str(report.get("execution_environment_id") or "none")
        )
    capabilities = report.get("capabilities", {})
    if not isinstance(capabilities, dict) or not capabilities.get("ready", False):
        reasons.append("node-capabilities-not-ready")
    elif isinstance(capabilities, dict):
        reasons.extend(
            _live_capability_rejection_reasons(
                capabilities,
                endpoint,
                requirements,
            )
        )
    session = conn.execute(
        "SELECT * FROM node_sessions WHERE session_id=?",
        (node["current_session_id"],),
    ).fetchone()
    if session is None:
        reasons.append("node-current-session-missing")
    else:
        if str(session["node_id"]) != endpoint.node_id:
            reasons.append("node-current-session-identity-mismatch")
        if str(session["generation"]) != endpoint.generation:
            reasons.append("node-current-session-generation-mismatch")
        if str(session["state"]) != state:
            reasons.append("node-current-session-state-mismatch")
    return reasons


def _is_bootstrap_control_probe(
    request: sqlite3.Row,
    requirements: dict[str, Any],
) -> bool:
    try:
        manifest = json.loads(str(request["manifest_json"]))
    except (json.JSONDecodeError, TypeError):
        return False
    features = set(_string_values(requirements.get("features")))
    allowed_endpoints = _string_values(requirements.get("allowed_endpoints"))
    return (
        str(request["operator_id"]) == SYSTEM_EXPERIMENT_OPERATOR_ID
        and manifest.get("workflow_ingest") is False
        and str(manifest.get("task_class") or "") == "control-probe"
        and str(requirements.get("runtime_node_policy") or "")
        == BOOTSTRAP_CONTROL_PROBE_POLICY
        and features == {"control-probe"}
        and len(allowed_endpoints) == 1
        and int(requirements.get("device_count") or 0) == 0
        and not bool(requirements.get("require_fresh_credit"))
        and not bool(requirements.get("require_engine_resident"))
    )


def _live_capability_rejection_reasons(
    capabilities: dict[str, Any],
    endpoint: Any,
    requirements: dict[str, Any],
) -> list[str]:
    reasons: list[str] = []
    if (
        str(capabilities.get("execution_environment_id") or "")
        not in {"", endpoint.execution_environment_id}
    ):
        reasons.append("live-environment-identity-mismatch")
    if (
        str(capabilities.get("endpoint_generation") or "")
        not in {"", endpoint.generation}
    ):
        reasons.append("live-endpoint-generation-mismatch")

    required_features = set(_string_values(requirements.get("features")))
    live_features = set(_string_values(capabilities.get("features")))
    if required_features:
        missing = sorted(required_features - live_features)
        if missing:
            reasons.append("live-missing-features:" + ",".join(missing))

    required_soc = set(_string_values(requirements.get("soc")))
    live_soc = set(_string_values(capabilities.get("soc")))
    if required_soc and "*" not in required_soc and "*" not in live_soc:
        if not required_soc.intersection(live_soc):
            reasons.append("live-soc-mismatch")

    required_cann = set(_string_values(requirements.get("cann")))
    cann = capabilities.get("cann", {})
    live_cann: set[str] = set()
    if isinstance(cann, dict):
        live_cann.update(_string_values(cann.get("versions")))
        live_cann.update(_string_values(cann.get("homes")))
    else:
        live_cann.update(_string_values(cann))
    if required_cann and "*" not in required_cann:
        if not required_cann.intersection(live_cann):
            reasons.append("live-cann-mismatch")

    required_devices = _nonnegative_int(
        requirements.get("device_count"),
        "required-device-count",
        reasons,
    )
    live_devices = _nonnegative_int(
        capabilities.get("device_count"),
        "live-device-count",
        reasons,
    )
    if live_devices < required_devices:
        reasons.append(
            "live-device-count-mismatch:"
            f"required={required_devices};available={live_devices}"
        )

    required_adapters = set(
        _string_values(requirements.get("cache_adapters"))
    )
    live_adapters: set[str] = set()
    raw_adapters = capabilities.get("cache_adapters", [])
    if not isinstance(raw_adapters, (list, tuple)):
        raw_adapters = []
        if required_adapters:
            reasons.append("live-cache-adapters-invalid")
    for item in raw_adapters:
        if isinstance(item, dict):
            if item.get("supported", False) and item.get("adapter_id"):
                live_adapters.add(str(item["adapter_id"]))
        elif item:
            live_adapters.add(str(item))
    missing_adapters = sorted(required_adapters - live_adapters)
    if missing_adapters:
        reasons.append(
            "live-missing-cache-adapters:" + ",".join(missing_adapters)
        )

    profiler = capabilities.get("profiler", {})
    if "profiler" in required_features and (
        not isinstance(profiler, dict) or not profiler.get("available", False)
    ):
        reasons.append("live-profiler-unavailable")

    engine = capabilities.get("engine", {})
    engine_required = bool(
        any(feature.startswith("engine-") for feature in required_features)
        or requirements.get("engine_generation")
        or requirements.get("engine_code_generation")
        or requirements.get("require_fresh_credit")
        or requirements.get("require_engine_resident")
        or requirements.get("host_slots")
        or requirements.get("device_slots")
        or requirements.get("export_slots")
        or requirements.get("return_capacity")
    )
    if engine_required:
        if not isinstance(engine, dict) or not engine.get("present", False):
            reasons.append("live-engine-not-present")
            return reasons
        if not engine.get("snapshot_available", False):
            reasons.append("live-engine-snapshot-unavailable")
            return reasons
        if (
            str(engine.get("execution_environment_id") or "")
            != endpoint.execution_environment_id
        ):
            reasons.append("live-engine-environment-mismatch")
        expected_generation = str(
            requirements.get("engine_generation") or ""
        )
        if expected_generation and str(engine.get("generation") or "") != expected_generation:
            reasons.append("live-engine-generation-mismatch")
        expected_code_generation = str(
            requirements.get("engine_code_generation") or ""
        )
        if (
            expected_code_generation
            and str(engine.get("code_generation") or "")
            != expected_code_generation
        ):
            reasons.append("live-engine-code-generation-mismatch")
        capacity = engine.get("capacity", {})
        available = engine.get("available", {})
        if not isinstance(capacity, dict):
            capacity = {}
        if not isinstance(available, dict):
            available = {}
        for requirement_key, capacity_key in (
            ("host_slots", "host_slots"),
            ("device_slots", "device_slots"),
            ("export_slots", "export_slots"),
            ("return_capacity", "max_inflight"),
        ):
            required = _nonnegative_int(
                requirements.get(requirement_key),
                f"requirement-{requirement_key}",
                reasons,
            )
            actual = _nonnegative_int(
                capacity.get(capacity_key),
                f"live-engine-{capacity_key}",
                reasons,
            )
            if actual < required:
                reasons.append(
                    f"live-engine-{requirement_key}-mismatch:"
                    f"required={required};available={actual}"
                )
        if bool(requirements.get("require_fresh_credit")):
            admission_credit = _nonnegative_int(
                available.get("admission_credit"),
                "live-engine-admission-credit",
                reasons,
            )
            if admission_credit < 1:
                reasons.append("live-engine-no-admission-credit")
        if bool(requirements.get("require_engine_resident")):
            resident = engine.get("resident", {})
            if (
                not isinstance(resident, dict)
                or not resident.get("resident_ok", False)
            ):
                reasons.append("live-engine-resident-unhealthy")
        if bool(capacity.get("draining", False)):
            reasons.append("live-engine-draining")

    scopes = capabilities.get("runtime_scopes", {})
    if engine_required and not isinstance(scopes, dict):
        reasons.append("live-runtime-scopes-invalid")
    elif engine_required and not scopes:
        reasons.append("live-runtime-scopes-missing")
    elif isinstance(scopes, dict) and scopes:
        if (
            str(scopes.get("execution_environment_id") or "")
            != endpoint.execution_environment_id
        ):
            reasons.append("live-runtime-scope-environment-mismatch")
        writable_root = str(scopes.get("writable_root") or "")
        cache_root = str(scopes.get("cache") or "")
        if writable_root and cache_root and not _path_is_within(
            cache_root,
            writable_root,
        ):
            reasons.append("live-cache-scope-escapes-runtime")
        if engine_required and not writable_root:
            reasons.append("live-runtime-writable-root-missing")
        if engine_required and not cache_root:
            reasons.append("live-runtime-cache-root-missing")

    if required_devices > 0:
        inventory = capabilities.get("device_inventory", [])
        if not isinstance(inventory, list):
            reasons.append("live-device-inventory-invalid")
        else:
            lease_resources = {
                str(item.get("lease_resource") or "")
                for item in inventory
                if isinstance(item, dict)
                and item.get("exclusive_measurement", False)
                and item.get("lease_resource")
            }
            if len(lease_resources) < required_devices:
                reasons.append(
                    "live-device-lease-identity-mismatch:"
                    f"required={required_devices};available={len(lease_resources)}"
                )
    return reasons


def _string_values(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, (list, tuple, set)):
        return [str(item) for item in value if str(item)]
    return [str(value)]


def _nonnegative_int(
    value: object,
    label: str,
    reasons: list[str],
) -> int:
    try:
        normalized = int(value or 0)
    except (TypeError, ValueError):
        reasons.append(f"{label}-invalid")
        return 0
    if normalized < 0:
        reasons.append(f"{label}-invalid")
        return 0
    return normalized


def _path_is_within(path: str, root: str) -> bool:
    normalized_path = path.replace("\\", "/").rstrip("/")
    normalized_root = root.replace("\\", "/").rstrip("/")
    return normalized_path == normalized_root or normalized_path.startswith(
        normalized_root + "/"
    )


def _validate_observed_node_against_registry(
    row: sqlite3.Row,
    registry: SystemRegistry,
    *,
    require_live_lease: bool = True,
) -> None:
    endpoint_id = str(row["endpoint_id"])
    endpoint = next(
        (
            item
            for item in registry.endpoints
            if item.endpoint_id == endpoint_id
        ),
        None,
    )
    if endpoint is None:
        raise ControlDatabaseError(
            f"observed node endpoint is not registered: {endpoint_id}"
        )
    if endpoint.node_id != str(row["node_id"]):
        raise ControlDatabaseError(
            f"observed node does not match endpoint node: {endpoint_id}"
        )
    if endpoint.generation != str(row["generation"]):
        raise ControlDatabaseError(
            f"observed node generation does not match registry: {endpoint_id}"
        )
    if (
        not endpoint.enabled
        or endpoint.draining
        or not endpoint.node_enabled
        or endpoint.node_draining
        or not endpoint.environment_enabled
        or endpoint.environment_draining
        or not endpoint.gateway_enabled
        or endpoint.gateway_draining
    ):
        raise ControlDatabaseError(
            f"observed node endpoint is disabled or draining: {endpoint_id}"
        )
    report = json.loads(str(row["report_json"]))
    if (
        str(report.get("execution_environment_id") or "")
        != endpoint.execution_environment_id
    ):
        raise ControlDatabaseError(
            f"observed node environment does not match registry: {endpoint_id}"
        )
    if str(report.get("gateway_id") or "") != endpoint.gateway_id:
        raise ControlDatabaseError(
            f"observed node gateway does not match registry: {endpoint_id}"
        )
    if str(report.get("transport_mode") or "") != endpoint.transport_mode:
        raise ControlDatabaseError(
            f"observed node transport mode does not match registry: {endpoint_id}"
        )
    if not str(row["capability_generation"]):
        raise ControlDatabaseError(
            f"observed node has no capability generation: {endpoint_id}"
        )
    if require_live_lease and _parse_timestamp(
        str(row["lease_expires_at"]), "lease_expires_at"
    ) <= datetime.now(timezone.utc):
        raise ControlDatabaseError(f"observed node lease has expired: {endpoint_id}")


def _validate_node_report(report: dict[str, Any]) -> None:
    if report.get("schema") != "git-partner.node-report.v1":
        raise ControlDatabaseError("unsupported node report schema")
    required = (
        "node_id",
        "endpoint_id",
        "generation",
        "session_id",
        "boot_id",
        "sequence",
        "state",
        "started_at",
        "heartbeat_at",
        "lease_expires_at",
    )
    missing = [name for name in required if report.get(name) in (None, "")]
    if missing:
        raise ControlDatabaseError(
            f"node report is missing required fields: {','.join(missing)}"
        )
    if not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,62}", str(report["node_id"])):
        raise ControlDatabaseError("node report node_id is not a safe identifier")
    if str(report["state"]) not in ACTIVE_NODE_STATES | {"offline", "expired"}:
        raise ControlDatabaseError(f"unsupported node state: {report['state']}")
    try:
        sequence = int(report["sequence"])
    except (TypeError, ValueError) as exc:
        raise ControlDatabaseError("node report sequence must be an integer") from exc
    if sequence < 1:
        raise ControlDatabaseError("node report sequence must be positive")
    for name in ("started_at", "heartbeat_at", "lease_expires_at"):
        _parse_timestamp(str(report[name]), name)
    if not isinstance(report.get("capabilities", {}), dict):
        raise ControlDatabaseError("node report capabilities must be an object")


def _node_report_is_newer(
    conn: sqlite3.Connection,
    current_node: sqlite3.Row,
    report: dict[str, Any],
) -> bool:
    current = conn.execute(
        "SELECT started_at FROM node_sessions WHERE session_id=?",
        (current_node["current_session_id"],),
    ).fetchone()
    if current is None:
        return True
    incoming_started = _parse_timestamp(str(report["started_at"]), "started_at")
    current_started = _parse_timestamp(str(current["started_at"]), "started_at")
    if incoming_started != current_started:
        return incoming_started > current_started
    return str(report["session_id"]) > str(current_node["current_session_id"])


def _parse_timestamp(value: str, name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ControlDatabaseError(f"invalid {name} timestamp: {value}") from exc
    if parsed.tzinfo is None:
        raise ControlDatabaseError(f"{name} timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def _normalized_node_lease(
    report: dict[str, Any],
) -> tuple[str, str, float]:
    received_at = datetime.now(timezone.utc)
    source_heartbeat = _parse_timestamp(
        str(report["heartbeat_at"]),
        "heartbeat_at",
    )
    source_lease = _parse_timestamp(
        str(report["lease_expires_at"]),
        "lease_expires_at",
    )
    source_clock_skew_seconds = (received_at - source_heartbeat).total_seconds()
    process_identity_present = (
        str(report.get("role") or "") in {"client", "server"}
        and int(report.get("pid") or 0) > 0
        and bool(str(report.get("process_start_token") or ""))
    )
    source_lease_is_current = source_lease > received_at
    if (
        abs(source_clock_skew_seconds) <= 300.0
        and (source_lease_is_current or not process_identity_present)
    ):
        return (
            _format_timestamp(source_heartbeat),
            _format_timestamp(source_lease),
            round(source_clock_skew_seconds, 6),
        )
    advertised_seconds = (source_lease - source_heartbeat).total_seconds()
    lease_seconds = min(600.0, max(1.0, advertised_seconds))
    return (
        _format_timestamp(received_at),
        _format_timestamp(received_at + timedelta(seconds=lease_seconds)),
        round(source_clock_skew_seconds, 6),
    )


def canonical_json(value: object) -> str:
    return json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    )


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _ensure_column(
    conn: sqlite3.Connection,
    table: str,
    column: str,
    declaration: str,
) -> bool:
    known = {
        str(row["name"])
        for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
    }
    if column not in known:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {declaration}")
        return True
    return False
