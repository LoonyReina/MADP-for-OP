from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Mapping

from ascendop_daemon.control_plane.control_database import ControlDatabase
from ascendop_daemon.control_plane.device_budget_evidence import (
    DeviceBudgetEvidenceError,
    decide_device_budget,
    decide_profiler_device_budget,
)
from ascendop_daemon.control_plane.test_request_persistence import (
    TestRequestError,
    ensure_bounded,
    persist_test_request,
    pinned_endpoint_requirements,
    reject_symlinks,
    relative_path,
    resolve_manifest_submit_root,
    safe_token,
)
from ascendop_daemon.workflow.engine_candidates import (
    discover_control_plane_submit_candidates,
)
from ascendop_daemon.workflow.operator_job_builder import (
    canonical_file_sha256,
    parse_submit_command,
    tree_digest,
)
from ascendop_daemon.core.models import DaemonConfig
from ascendop_daemon.registry.system_registry import SystemRegistry, canonical_digest
from ascendop_daemon.registry.models import BackendEndpoint
from ascendop_daemon.exchange.flow_v3_request_builder import (
    build_candidate_request,
    build_diagnostic_request,
)
from ascendop_daemon.runtime.locking import NamedProcessLock
from ascendop_daemon.runtime.release_identity import source_generation
from ascendop_daemon.workflow.task_execution_profile import (
    ensure_task_execution_profile,
)


TEST_REQUEST_SCHEMA = "ascendop.test-request.v1"


def generate_test_requests(
    root: Path,
    config: DaemonConfig,
    database: ControlDatabase,
    registry: SystemRegistry,
    *,
    request_root: Path,
    pump_state: dict[str, Any] | None = None,
    traffic_debt: dict[str, int] | None = None,
    execution_profile: str = "engine-v3-staged-fused",
    limit: int = 0,
    route: bool = True,
    package_root: Path | None = None,
    code_generation: str = "",
    operator: str = "",
    test_version: str = "",
) -> dict[str, Any]:
    root = root.resolve()
    request_root = request_root.resolve()
    candidates = discover_control_plane_submit_candidates(
        root,
        config,
        traffic_debt=traffic_debt,
    )
    if operator:
        candidates = [row for row in candidates if row["op"] == operator]
    if test_version:
        candidates = [row for row in candidates if row["test_version"] == test_version]
    if limit > 0:
        candidates = candidates[:limit]
    rows: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    for candidate in candidates:
        try:
            row, candidate_errors = _generate_test_request_candidate(
                root,
                database,
                registry,
                candidate,
                request_root=request_root,
                execution_profile=execution_profile,
                route=route,
                package_root=package_root,
                code_generation=code_generation,
            )
            rows.append(row)
            errors.extend(candidate_errors)
        except Exception as exc:
            errors.append(
                {
                    "operator": str(candidate.get("op") or ""),
                    "test_version": str(candidate.get("test_version") or ""),
                    "request_id": "",
                    "error": str(exc),
                }
            )
    return {
        "schema": "ascendop.test-request-generation.v1",
        "candidate_count": len(candidates),
        "generated_count": len(rows),
        "requests": rows,
        "errors": errors,
    }


def _generate_test_request_candidate(
    root: Path,
    database: ControlDatabase,
    registry: SystemRegistry,
    candidate: dict[str, str],
    *,
    request_root: Path,
    execution_profile: str,
    route: bool,
    package_root: Path | None,
    code_generation: str,
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    registration = database.operator_for_display_name(candidate["op"])
    lineage = candidate_workflow_lineage(database, candidate)
    manifest = build_test_request_manifest(
        root,
        candidate,
        registration,
        execution_profile=execution_profile,
        lineage=lineage,
    )
    manifest_path, persisted = persist_test_request(request_root, manifest)
    record = database.create_test_request(persisted, manifest_path)
    route_result = None
    preparation_error = ""
    errors: list[dict[str, str]] = []
    if route:
        route_result = database.reserve_wire_v3_preparation(
            persisted["request_id"], registry
        )
        if route_result.get("preparation"):
            try:
                prepare_wire_v3_attempt(
                    root,
                    database,
                    persisted,
                    route_result,
                    package_root=package_root,
                    code_generation=code_generation,
                )
            except Exception as exc:
                preparation_error = str(exc)
                errors.append(
                    {
                        "operator": str(candidate.get("op") or ""),
                        "test_version": str(candidate.get("test_version") or ""),
                        "request_id": str(persisted["request_id"]),
                        "error": str(exc),
                    }
                )
            if not preparation_error:
                route_result = database.reserve_wire_v3_preparation(
                    persisted["request_id"], registry
                )
    state = record["state"]
    if preparation_error:
        state = "blocked"
    elif route_result is not None:
        if route_result.get("terminal"):
            state = str(route_result.get("request_state") or record["state"])
        else:
            state = (
                "routed"
                if route_result.get("attempt")
                else "preparing"
                if route_result.get("preparation")
                else "blocked"
            )
    return (
        {
            "candidate": candidate,
            "request_id": persisted["request_id"],
            "request_digest": persisted["request_digest"],
            "manifest_path": str(manifest_path),
            "state": state,
            "route": route_result,
        },
        errors,
    )


def prepare_wire_v3_attempt(
    root: Path,
    database: ControlDatabase,
    manifest: dict[str, Any],
    route_result: dict[str, Any],
    *,
    package_root: Path | None = None,
    code_generation: str = "",
) -> dict[str, Any]:
    """Build and publish one routed operator attempt under a local singleflight."""

    root = root.resolve()
    attempt = route_result.get("attempt")
    preparation = route_result.get("preparation")
    if isinstance(attempt, dict) and not isinstance(preparation, dict):
        attempt_id = str(attempt.get("attempt_id") or "")
        return database.transport_outbox(f"outbox-{attempt_id}")
    if not isinstance(preparation, dict):
        raise TestRequestError("cannot prepare an unreserved TestRequest")
    preparation_id = str(preparation.get("preparation_id") or "")
    attempt_id = str(preparation.get("proposed_attempt_id") or "")
    request_id = str(manifest.get("request_id") or "")
    destination = (
        package_root.resolve()
        if package_root is not None
        else (root / ".ascendop-work" / "flow-v3" / "packages").resolve()
    )
    preparation_root = destination / "preparations" / preparation_id
    with NamedProcessLock(
        root,
        f"wire-v3-prepare-{request_id}",
        stale_after_seconds=600,
        wait_timeout_seconds=900,
    ):
        current = database.request_preparation(preparation_id)
        if current["state"] == "published":
            return database.transport_outbox(f"outbox-{attempt_id}")
        if current["state"] != "reserved":
            raise TestRequestError(
                f"Wire V3 preparation is not buildable from {current['state']}"
            )
        route_payload = current["payload"]
        profile = manifest.get("task_execution_profile", {})
        workflow = manifest.get("workflow", {})
        operation_instance = (
            dict(workflow.get("operation_instance") or {})
            if isinstance(workflow, dict)
            and isinstance(workflow.get("operation_instance"), dict)
            else {}
        )
        candidate = {
            "op": str(manifest.get("operator") or ""),
            "test_version": str(manifest.get("test_version") or ""),
            "command": str(manifest.get("trusted_submit_command") or ""),
            "attempt_index": int(operation_instance.get("ordinal", 1) or 1),
            "job_id_suffix": str(operation_instance.get("job_id_suffix") or ""),
        }
        submit_root = resolve_manifest_submit_root(root, manifest)
        try:
            operation_kind = str(workflow.get("operation_kind") or "operator-test")
            common = {
                "endpoint_id": str(route_payload["target_endpoint_id"]),
                "endpoint_generation": str(route_payload["target_generation"]),
                "registration_generation": str(
                    route_payload["operator_registration_generation"]
                ),
                "code_generation": code_generation or source_generation(root),
                "remote_root": str(route_payload["remote_root"]),
                "package_root": preparation_root,
                "request_id_override": request_id,
                "attempt_id_override": attempt_id,
                "evidence_operation": (
                    dict(workflow.get("evidence_operation") or {})
                    if isinstance(workflow, dict)
                    and isinstance(workflow.get("evidence_operation"), dict)
                    else None
                ),
                "lineage": (
                    dict(workflow.get("lineage") or {})
                    if isinstance(workflow, dict)
                    and isinstance(workflow.get("lineage"), dict)
                    else None
                ),
            }
            if operation_kind == "diagnostic-profile":
                envelope, envelope_path = build_diagnostic_request(
                    root,
                    {**candidate, "submit_root_override": str(submit_root)},
                    profiler_plan=dict(workflow.get("profiler_plan") or {}),
                    profiler_mode=str(workflow.get("profiler_mode") or ""),
                    runtime_operator_name=(
                        str(profile.get("runtime_operator_name") or "")
                        if isinstance(profile, dict)
                        else ""
                    ),
                    requested_device_session_seconds=(
                        int(profile.get("requested_device_session_seconds", 0) or 0)
                        if isinstance(profile, dict)
                        else 0
                    ),
                    budget_class=(
                        str(profile.get("budget_class") or "diagnostic")
                        if isinstance(profile, dict)
                        else "diagnostic"
                    ),
                    gate_evidence=(
                        json.dumps(
                            profile.get("budget_gate_evidence"),
                            ensure_ascii=True,
                            sort_keys=True,
                            separators=(",", ":"),
                        )
                        if isinstance(profile, dict)
                        and isinstance(profile.get("budget_gate_evidence"), dict)
                        else ""
                    ),
                    **common,
                )
            elif operation_kind in {
                "operator-test",
                "diagnostic-correctness-replay",
            }:
                envelope, envelope_path = build_candidate_request(
                    root,
                    candidate,
                    submit_root_override=submit_root,
                    requested_device_session_seconds=int(
                        profile.get("requested_device_session_seconds", 0) or 0
                    )
                    if isinstance(profile, dict)
                    else 0,
                    budget_class=(
                        "diagnostic"
                        if operation_kind == "diagnostic-correctness-replay"
                        else str(profile.get("budget_class") or "standard")
                        if isinstance(profile, dict)
                        else "standard"
                    ),
                    gate_evidence=(
                        json.dumps(
                            profile.get("budget_gate_evidence"),
                            ensure_ascii=True,
                            sort_keys=True,
                            separators=(",", ":"),
                        )
                        if isinstance(profile, dict)
                        and isinstance(profile.get("budget_gate_evidence"), dict)
                        else ""
                    ),
                    publish_eligible=bool(
                        workflow.get("publish_eligible", True)
                        if isinstance(workflow, dict)
                        else True
                    ),
                    runtime_compatibility=(
                        tuple(profile.get("runtime_compatibility", []))
                        if isinstance(profile, dict)
                        else ()
                    ),
                    runtime_operator_name=(
                        str(profile.get("runtime_operator_name") or "")
                        if isinstance(profile, dict)
                        else ""
                    ),
                    profiler_mode=(
                        str(workflow.get("profiler_mode") or "none")
                        if operation_kind == "diagnostic-correctness-replay"
                        else "primary-all-cases"
                    ),
                    operation_kind=operation_kind,
                    diagnostic_plan=(
                        dict(workflow.get("diagnostic_plan") or {})
                        if operation_kind == "diagnostic-correctness-replay"
                        else None
                    ),
                    **common,
                )
            else:
                raise TestRequestError(
                    f"unsupported Wire V3 operation kind: {operation_kind}"
                )
            return database.publish_wire_v3_preparation(
                preparation_id,
                envelope=envelope,
                envelope_path=envelope_path,
                package_root=preparation_root,
            )
        except Exception as exc:
            database.fail_wire_v3_preparation(
                preparation_id,
                error=str(exc),
                code_generation=code_generation,
            )
            raise


def route_and_prepare_test_request(
    root: Path,
    database: ControlDatabase,
    registry: SystemRegistry,
    request_id: str,
    *,
    code_generation: str = "",
) -> dict[str, Any]:
    routed = database.reserve_wire_v3_preparation(request_id, registry)
    if routed.get("preparation"):
        request = database.test_request(request_id)
        prepare_wire_v3_attempt(
            root,
            database,
            request["manifest"],
            routed,
            code_generation=code_generation,
        )
        routed = database.reserve_wire_v3_preparation(request_id, registry)
    return routed


def build_test_request_manifest(
    root: Path,
    candidate: dict[str, str],
    registration: dict[str, Any],
    *,
    execution_profile: str,
    submit_root_override: Path | None = None,
    pinned_endpoint: BackendEndpoint | None = None,
    allowed_endpoint_ids: list[str] | None = None,
    publish_eligible: bool = True,
    operation_kind: str = "operator-test",
    profiler_mode: str = "",
    profiler_plan: dict[str, Any] | None = None,
    diagnostic_plan: dict[str, Any] | None = None,
    requested_device_session_seconds: int | None = None,
    required_node_session_id: str = "",
    required_capability_generation: str = "",
    evidence_operation: dict[str, Any] | None = None,
    lineage: dict[str, Any] | None = None,
) -> dict[str, Any]:
    root = root.resolve()
    op = candidate["op"]
    test_version = candidate["test_version"]
    parsed = parse_submit_command(candidate["command"])
    if parsed["op"] != op or parsed["test_version"] != test_version:
        raise TestRequestError(f"candidate command mismatch: {op}/{test_version}")
    submit_root = (
        submit_root_override.resolve()
        if submit_root_override is not None
        else (root / "TestUtils" / "submit" / op / test_version).resolve()
    )
    ensure_bounded(root, submit_root)
    source = submit_root / "pending_snapshot" / "source_snapshot"
    task_case = submit_root / "task_case"
    attack_case = submit_root / "attack_case"
    submit_md = submit_root / "SUBMIT.md"
    for required in (source, task_case):
        if not required.is_dir():
            raise TestRequestError(f"request payload source is missing: {required}")
        reject_symlinks(required)
    if attack_case.exists():
        if not attack_case.is_dir():
            raise TestRequestError(f"attack_case is not a directory: {attack_case}")
        reject_symlinks(attack_case)
    requirements = dict(registration["requirements"])
    profile_path, profile_document, task_profile = ensure_task_execution_profile(
        root,
        registration,
    )
    requirements.update(task_profile.requirements())
    if pinned_endpoint is not None:
        requirements = pinned_endpoint_requirements(requirements, pinned_endpoint)
    elif allowed_endpoint_ids is not None:
        allowed = sorted({str(value) for value in allowed_endpoint_ids if value})
        if not allowed:
            raise TestRequestError("request endpoint filter cannot be empty")
        requirements["allowed_endpoints"] = allowed
    if bool(required_node_session_id) != bool(required_capability_generation):
        raise TestRequestError(
            "runtime route affinity requires both node session and "
            "capability generation"
        )
    if required_node_session_id:
        requirements["node_session_id"] = str(required_node_session_id)
        requirements["capability_generation"] = str(required_capability_generation)
    if operation_kind not in {
        "operator-test",
        "diagnostic-profile",
        "diagnostic-correctness-replay",
    }:
        raise TestRequestError(f"unsupported operation kind: {operation_kind}")
    if operation_kind == "diagnostic-profile" and not profiler_plan:
        raise TestRequestError("diagnostic-profile requires a profiler plan")
    if operation_kind == "diagnostic-correctness-replay" and publish_eligible:
        raise TestRequestError(
            "diagnostic-correctness-replay cannot be publish eligible"
        )
    if operation_kind == "diagnostic-correctness-replay" and not diagnostic_plan:
        raise TestRequestError(
            "diagnostic-correctness-replay requires a diagnostic plan"
        )
    if operation_kind == "diagnostic-correctness-replay" and profiler_mode not in {
        "",
        "none",
    }:
        raise TestRequestError(
            "diagnostic-correctness-replay profiler mode must be none"
        )
    diagnostic_operation_instance: dict[str, Any] = {}
    if operation_kind in {
        "diagnostic-profile",
        "diagnostic-correctness-replay",
    }:
        operation_ordinal = int(candidate.get("attempt_index", 1) or 1)
        if operation_ordinal <= 0:
            raise TestRequestError(
                "diagnostic operation instance ordinal must be positive"
            )
        job_id_suffix = str(candidate.get("job_id_suffix") or "").strip()
        if not job_id_suffix:
            job_id_suffix = f"{operation_kind}-a{operation_ordinal:02d}"
        diagnostic_operation_instance = {
            "ordinal": operation_ordinal,
            "job_id_suffix": job_id_suffix,
        }
    effective_profiler_plan: dict[str, Any] = {}
    if operation_kind == "diagnostic-profile":
        effective_profiler_plan = dict(profiler_plan or {})
        effective_profiler_plan.setdefault("collection_mode", profiler_mode)
        effective_profiler_plan.setdefault(
            "profiler_mode",
            (
                "deep-dual"
                if profiler_mode == "primary-roofline-all-cases"
                else "fast-single"
            ),
        )
        definition = registration.get("definition")
        if not isinstance(definition, dict):
            definition = {}
        profiler_kernel_name = str(
            definition.get("profiler_kernel_name") or task_profile.runtime_operator_name
        ).strip()
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", profiler_kernel_name):
            raise TestRequestError(
                f"unsafe profiler kernel name: {profiler_kernel_name!r}"
            )
        effective_profiler_plan["profiler_kernel_name"] = profiler_kernel_name
        profiler_kernel_selection = str(
            definition.get("profiler_kernel_selection") or "exact"
        ).strip()
        if profiler_kernel_selection not in {"exact", "prefix-postfilter"}:
            raise TestRequestError(
                f"unsupported profiler kernel selection: {profiler_kernel_selection}"
            )
        effective_profiler_plan["profiler_kernel_selection"] = profiler_kernel_selection
    effective_requested_device_seconds = (
        int(requested_device_session_seconds)
        if requested_device_session_seconds is not None
        else task_profile.requested_device_session_seconds
    )
    effective_budget_class = (
        "diagnostic"
        if operation_kind
        in {
            "diagnostic-profile",
            "diagnostic-correctness-replay",
        }
        else task_profile.budget_class
    )
    try:
        if operation_kind == "diagnostic-profile":
            budget_decision = decide_profiler_device_budget(
                root,
                operator=op,
                case_version=parsed["case_version"],
                profiler_plan=effective_profiler_plan,
                requested_seconds=effective_requested_device_seconds,
                task_profile_sha256=canonical_digest(profile_document),
                submit_md=submit_md,
                task_case=task_case,
            )
        else:
            budget_decision = decide_device_budget(
                root,
                operator=op,
                case_version=parsed["case_version"],
                budget_class=effective_budget_class,
                requested_seconds=effective_requested_device_seconds,
                task_profile_sha256=canonical_digest(profile_document),
                submit_md=submit_md,
                task_case=task_case,
                attack_case=attack_case,
            )
    except DeviceBudgetEvidenceError as exc:
        raise TestRequestError(str(exc)) from exc
    routing_policy = dict(registration["routing_policy"])
    routing_policy["budget"] = {
        "class": budget_decision.effective_class,
        "policy": "daemon-approved-v1",
        "requested_device_session_seconds": budget_decision.effective_seconds,
    }
    body = {
        "schema": TEST_REQUEST_SCHEMA,
        "operator_id": registration["operator_id"],
        "operator": op,
        "season": parsed["season"],
        "test_version": test_version,
        "case_version": parsed["case_version"],
        "mode": parsed["mode"],
        "vendor": parsed["vendor"],
        "registration_generation": registration["registration_generation"],
        "test_profile": registration["test_profile"],
        "execution_profile": execution_profile,
        "execution_requirements": requirements,
        "task_execution_profile": {
            "path": relative_path(root, profile_path),
            "sha256": canonical_digest(profile_document),
            "route_mode": "pinned" if pinned_endpoint else task_profile.route_mode,
            **({"endpoint_id": pinned_endpoint.endpoint_id} if pinned_endpoint else {}),
            "requested_budget_class": effective_budget_class,
            "requested_budget_seconds": effective_requested_device_seconds,
            "budget_class": budget_decision.effective_class,
            "requested_device_session_seconds": (budget_decision.effective_seconds),
            "budget_policy_decision": budget_decision.to_dict(),
            **(
                {"budget_gate_evidence": budget_decision.approved_gate_evidence}
                if budget_decision.approved_gate_evidence
                else {}
            ),
            "origin_workspace": task_profile.origin_workspace,
            "runtime_compatibility": list(task_profile.runtime_compatibility),
            "runtime_operator_name": task_profile.runtime_operator_name,
        },
        "cache_policy": registration["cache_policy"],
        "routing_policy": routing_policy,
        "workflow": {
            "workflow_ingest": True,
            "publish_eligible": bool(publish_eligible),
            "operation_kind": operation_kind,
            **(
                {"operation_instance": diagnostic_operation_instance}
                if diagnostic_operation_instance
                else {}
            ),
            **({"lineage": dict(lineage)} if lineage else {}),
            **(
                {"evidence_operation": dict(evidence_operation)}
                if evidence_operation is not None
                else {}
            ),
            **(
                {
                    "profiler_mode": profiler_mode,
                    "profiler_plan": effective_profiler_plan,
                }
                if operation_kind == "diagnostic-profile"
                else {}
            ),
            **(
                {
                    "profiler_mode": "none",
                    "diagnostic_plan": dict(diagnostic_plan or {}),
                }
                if operation_kind == "diagnostic-correctness-replay"
                else {}
            ),
        },
        "payload_sources": {
            "submit_root": relative_path(root, submit_root),
            "source_snapshot": relative_path(root, source),
            "task_case": relative_path(root, task_case),
            "attack_case": relative_path(root, attack_case)
            if attack_case.is_dir()
            else "",
        },
        "input_identity": {
            "source_sha256": tree_digest(source),
            "task_case_sha256": tree_digest(task_case),
            "attack_case_sha256": tree_digest(attack_case)
            if attack_case.is_dir()
            else "",
            "submit_md_sha256": canonical_file_sha256(submit_md)
            if submit_md.is_file()
            else "",
        },
        "trusted_submit_command": candidate["command"],
    }
    request_digest = canonical_digest(body)
    request_id = f"tr-{safe_token(test_version)}-{request_digest[:12]}"
    return {
        **body,
        "request_id": request_id,
        "request_digest": request_digest,
    }


def candidate_workflow_lineage(
    database: ControlDatabase,
    candidate: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Resolve daemon-owned promotion causation for a candidate TestRequest."""

    operator = str(candidate.get("op") or "")
    test_version = str(candidate.get("test_version") or "")
    with database.connection() as conn:
        row = conn.execute(
            "SELECT action_id, action_json FROM workflow_actions "
            "WHERE operator_id=? AND test_version=? AND state='completed' "
            "AND action_kind IN ('promote-agent-source','promote-agent-output') "
            "ORDER BY completed_at DESC, action_id DESC LIMIT 1",
            (operator, test_version),
        ).fetchone()
        if row is None:
            return None
        promotion = json.loads(str(row["action_json"]))
        origin_action_id = str(promotion.get("parent_trace_id") or "")
        agent = conn.execute(
            "SELECT iteration_id FROM agent_actions_v4 WHERE action_id=?",
            (origin_action_id,),
        ).fetchone()
        receipt = conn.execute(
            "SELECT action_id FROM workflow_action_receipts WHERE action_id=?",
            (str(row["action_id"]),),
        ).fetchone()
    return {
        "trace_id": origin_action_id or str(row["action_id"]),
        "origin_action_id": origin_action_id,
        "origin_iteration_id": str(agent["iteration_id"]) if agent else "",
        "promotion_action_id": str(row["action_id"]),
        "promotion_receipt_id": str(receipt["action_id"]) if receipt else "",
        "candidate_id": test_version,
    }
