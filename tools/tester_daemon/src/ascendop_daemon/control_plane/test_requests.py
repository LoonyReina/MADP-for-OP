from __future__ import annotations

import json
import os
import re
import tempfile
import time
from pathlib import Path
from typing import Any

from ascendop_daemon.control_plane.control_database import ControlDatabase
from ascendop_daemon.workflow.engine_candidates import discover_engine_submit_candidates
from ascendop_daemon.workflow.operator_job_builder import (
    canonical_file_sha256,
    parse_submit_command,
    tree_digest,
)
from ascendop_daemon.core.models import DaemonConfig
from ascendop_daemon.registry.system_registry import SystemRegistry, canonical_digest
from ascendop_daemon.registry.models import BackendEndpoint
from ascendop_daemon.registry.topology_parser import string_list
from ascendop_daemon.exchange.flow_v3_request_builder import (
    build_candidate_request,
    build_diagnostic_request,
)
from ascendop_daemon.runtime.locking import NamedProcessLock
from ascendop_daemon.runtime.release_identity import source_generation
from ascendop_daemon.workflow.task_execution_profile import ensure_task_execution_profile


TEST_REQUEST_SCHEMA = "ascendop.test-request.v1"


class TestRequestError(RuntimeError):
    pass


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
    candidates = discover_engine_submit_candidates(
        root,
        config,
        pump_state=pump_state,
        traffic_debt=traffic_debt,
    )
    if operator:
        candidates = [row for row in candidates if row["op"] == operator]
    if test_version:
        candidates = [
            row for row in candidates if row["test_version"] == test_version
        ]
    if limit > 0:
        candidates = candidates[:limit]
    rows: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    for candidate in candidates:
        registration = database.operator_for_display_name(candidate["op"])
        manifest = build_test_request_manifest(
            root,
            candidate,
            registration,
            execution_profile=execution_profile,
        )
        manifest_path, persisted = persist_test_request(request_root, manifest)
        record = database.create_test_request(persisted, manifest_path)
        route_result = None
        preparation_error = ""
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
                            "test_version": str(
                                candidate.get("test_version") or ""
                            ),
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
        rows.append(
            {
                "candidate": candidate,
                "request_id": persisted["request_id"],
                "request_digest": persisted["request_digest"],
                "manifest_path": str(manifest_path),
                "state": state,
                "route": route_result,
            }
        )
    return {
        "schema": "ascendop.test-request-generation.v1",
        "candidate_count": len(candidates),
        "generated_count": len(rows),
        "requests": rows,
        "errors": errors,
    }


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
        candidate = {
            "op": str(manifest.get("operator") or ""),
            "test_version": str(manifest.get("test_version") or ""),
            "command": str(manifest.get("trusted_submit_command") or ""),
            "job_id_suffix": str(workflow.get("job_id_suffix") or "")
            if isinstance(workflow, dict)
            else "",
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
            }
            if operation_kind == "diagnostic-profile":
                envelope, envelope_path = build_diagnostic_request(
                    root,
                    {**candidate, "submit_root_override": str(submit_root)},
                    profiler_plan=dict(workflow.get("profiler_plan") or {}),
                    profiler_mode=str(workflow.get("profiler_mode") or ""),
                    **common,
                )
            elif operation_kind == "operator-test":
                envelope, envelope_path = build_candidate_request(
                    root,
                    candidate,
                    submit_root_override=submit_root,
                    requested_device_session_seconds=int(
                        profile.get("requested_device_session_seconds", 0) or 0
                    )
                    if isinstance(profile, dict)
                    else 0,
                    budget_class=str(profile.get("budget_class") or "standard")
                    if isinstance(profile, dict)
                    else "standard",
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
            database.fail_wire_v3_preparation(preparation_id, error=str(exc))
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
    publish_eligible: bool = True,
    operation_kind: str = "operator-test",
    profiler_mode: str = "",
    profiler_plan: dict[str, Any] | None = None,
) -> dict[str, Any]:
    root = root.resolve()
    op = candidate["op"]
    test_version = candidate["test_version"]
    parsed = parse_submit_command(candidate["command"])
    if parsed["op"] != op or parsed["test_version"] != test_version:
        raise TestRequestError(
            f"candidate command mismatch: {op}/{test_version}"
        )
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
    if operation_kind not in {"operator-test", "diagnostic-profile"}:
        raise TestRequestError(f"unsupported operation kind: {operation_kind}")
    if operation_kind == "diagnostic-profile" and not profiler_plan:
        raise TestRequestError("diagnostic-profile requires a profiler plan")
    budget_gate_evidence = _budget_gate_evidence(
        root,
        submit_root=submit_root,
        submit_md=submit_md,
        task_case=task_case,
        attack_case=attack_case,
        task_profile_sha256=canonical_digest(profile_document),
        budget_class=task_profile.budget_class,
        requested_seconds=task_profile.requested_device_session_seconds,
        case_version=parsed["case_version"],
    )
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
            **(
                {"endpoint_id": pinned_endpoint.endpoint_id}
                if pinned_endpoint
                else {}
            ),
            "budget_class": task_profile.budget_class,
            "requested_device_session_seconds": (
                task_profile.requested_device_session_seconds
            ),
            **(
                {"budget_gate_evidence": budget_gate_evidence}
                if budget_gate_evidence
                else {}
            ),
            "origin_workspace": task_profile.origin_workspace,
        },
        "cache_policy": registration["cache_policy"],
        "routing_policy": registration["routing_policy"],
        "workflow": {
            "attempt_index": int(candidate.get("attempt_index", "1") or 1),
            "job_id_suffix": candidate.get("job_id_suffix", ""),
            "balance_debt": int(candidate.get("debt", "0") or 0),
            "workflow_ingest": True,
            "publish_eligible": bool(publish_eligible),
            "operation_kind": operation_kind,
            **(
                {
                    "profiler_mode": profiler_mode,
                    "profiler_plan": dict(profiler_plan or {}),
                }
                if operation_kind == "diagnostic-profile"
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


def pinned_endpoint_requirements(
    requirements: dict[str, Any],
    endpoint: BackendEndpoint,
) -> dict[str, Any]:
    """Bind environment identity while preserving task-level feature gates."""

    pinned = dict(requirements)
    pinned.update(
        {
            "allowed_endpoints": [endpoint.endpoint_id],
            "backend_pool": endpoint.backend_pool,
            "transport": endpoint.transport,
            "soc": string_list(endpoint.capabilities.get("soc")),
            "cann": string_list(endpoint.capabilities.get("cann")),
        }
    )
    operating_systems = string_list(
        endpoint.capabilities.get("operating_systems")
    )
    if operating_systems:
        pinned["operating_systems"] = operating_systems
    return pinned


def _budget_gate_evidence(
    root: Path,
    *,
    submit_root: Path,
    submit_md: Path,
    task_case: Path,
    attack_case: Path,
    task_profile_sha256: str,
    budget_class: str,
    requested_seconds: int | None,
    case_version: str,
) -> dict[str, Any]:
    if budget_class != "heavy":
        return {}
    if not submit_md.is_file() or not task_case.is_dir():
        raise TestRequestError(
            "heavy device budget requires a submit-ready immutable workload"
        )
    evidence = {
        "schema": "ascendop.device-budget-gate-evidence.v1",
        "gate": "submit-ready",
        "case_version": case_version,
        "requested_device_session_seconds": int(requested_seconds or 0),
        "task_profile_sha256": task_profile_sha256,
        "submit_md": {
            "path": relative_path(root, submit_md),
            "sha256": canonical_file_sha256(submit_md),
        },
        "task_case": {
            "path": relative_path(root, task_case),
            "sha256": tree_digest(task_case),
        },
        "rationale": (
            "heavy device session bound to the current submit-ready case workload"
        ),
    }
    if attack_case.is_dir():
        evidence["attack_case"] = {
            "path": relative_path(root, attack_case),
            "sha256": tree_digest(attack_case),
        }
    return evidence


def resolve_manifest_submit_root(root: Path, manifest: dict[str, Any]) -> Path:
    sources = manifest.get("payload_sources", {})
    identity = manifest.get("input_identity", {})
    if not isinstance(sources, dict) or not isinstance(identity, dict):
        raise TestRequestError("request payload source identity is malformed")
    relative = str(sources.get("submit_root") or "")
    if not relative:
        raise TestRequestError("request payload submit_root is missing")
    submit_root = (root / relative).resolve()
    ensure_bounded(root, submit_root)
    source = submit_root / "pending_snapshot" / "source_snapshot"
    task_case = submit_root / "task_case"
    attack_case = submit_root / "attack_case"
    observed = {
        "source_sha256": tree_digest(source) if source.is_dir() else "",
        "task_case_sha256": tree_digest(task_case) if task_case.is_dir() else "",
        "attack_case_sha256": (
            tree_digest(attack_case) if attack_case.is_dir() else ""
        ),
    }
    expected = {
        key: str(identity.get(key) or "") for key in observed
    }
    if observed != expected:
        raise TestRequestError(
            "immutable request payload source changed before publication"
        )
    return submit_root


def persist_test_request(
    request_root: Path, manifest: dict[str, Any]
) -> tuple[Path, dict[str, Any]]:
    request_id = str(manifest["request_id"])
    destination = request_root / request_id / "TEST_REQUEST.json"
    if destination.is_file():
        existing = read_persisted_test_request(destination)
        validate_persisted_test_request(destination, existing, manifest)
        return destination, existing
    destination.parent.mkdir(parents=True, exist_ok=True)
    # Runtime timestamps belong to SQLite.  Keeping the file content purely
    # content-addressed makes concurrent idempotent writers byte-identical.
    persisted = dict(manifest)
    payload = json.dumps(
        persisted, ensure_ascii=True, indent=2, sort_keys=True
    ) + "\n"
    temporary_path = ""
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=destination.parent,
            prefix=".TEST_REQUEST.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary_path = handle.name
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            # Publishing a hard link exposes the fully closed temporary file
            # atomically and lets exactly one immutable writer win.
            os.link(temporary_path, destination)
        except FileExistsError:
            existing = read_persisted_test_request(destination)
            validate_persisted_test_request(destination, existing, manifest)
            return destination, existing
        except OSError:
            # Some filesystems do not support hard links. O_EXCL still elects
            # one writer; concurrent readers wait for the complete JSON body.
            try:
                fd = os.open(
                    destination,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                )
            except FileExistsError:
                existing = read_persisted_test_request(destination)
                validate_persisted_test_request(destination, existing, manifest)
                return destination, existing
            with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
    finally:
        if temporary_path:
            Path(temporary_path).unlink(missing_ok=True)
    return destination, persisted


def read_persisted_test_request(
    path: Path,
    *,
    timeout_seconds: float = 2.0,
) -> dict[str, Any]:
    deadline = time.monotonic() + max(0.0, timeout_seconds)
    last_error: BaseException | None = None
    while True:
        try:
            value = json.loads(path.read_text(encoding="utf-8-sig"))
            if not isinstance(value, dict):
                raise TestRequestError(
                    f"persisted TestRequest is not an object: {path}"
                )
            return value
        except TestRequestError:
            raise
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            last_error = exc
            if time.monotonic() >= deadline:
                raise TestRequestError(
                    f"persisted TestRequest is not readable: {path}"
                ) from last_error
            time.sleep(0.01)


def validate_persisted_test_request(
    path: Path,
    existing: dict[str, Any],
    manifest: dict[str, Any],
) -> None:
    if (
        existing.get("request_digest") != manifest.get("request_digest")
        or canonical_without_created_at(existing)
        != canonical_without_created_at(manifest)
    ):
        raise TestRequestError(f"immutable TestRequest path collision: {path}")


def canonical_without_created_at(value: dict[str, Any]) -> str:
    copied = dict(value)
    copied.pop("created_at", None)
    return json.dumps(
        copied, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    )


def reject_symlinks(root: Path) -> None:
    if root.is_symlink():
        raise TestRequestError(f"request payload cannot be a symlink: {root}")
    for path in root.rglob("*"):
        if path.is_symlink():
            raise TestRequestError(f"request payload cannot contain symlinks: {path}")


def ensure_bounded(root: Path, path: Path) -> None:
    if path != root and root not in path.parents:
        raise TestRequestError(f"request path escapes workspace: {path}")


def relative_path(root: Path, path: Path) -> str:
    ensure_bounded(root, path.resolve())
    return path.resolve().relative_to(root).as_posix()


def safe_token(value: str) -> str:
    result = re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._-").lower()
    if not result:
        raise TestRequestError(f"invalid request token: {value!r}")
    return result
