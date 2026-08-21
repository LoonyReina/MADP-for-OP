from __future__ import annotations

import hashlib
import json
import os
import shutil
import uuid
from pathlib import Path
from typing import Any, Mapping

from ascendop_protocol.evidence import (
    EVIDENCE_OPERATION_RESULT_SCHEMA,
    evidence_operation_definition,
    validate_evidence_operation_result,
)

from ascendop_daemon.control_plane.test_requests import (
    build_test_request_manifest,
    persist_test_request,
    route_and_prepare_test_request,
)
from ascendop_daemon.workflow.engine_candidates import extract_submit_command
from ascendop_daemon.workflow.operator_job_builder import (
    FUSED_SCALABLE_PROFILE,
    build_test_contract,
    parse_submit_command,
    tree_digest,
)


DEVICE_OPERATIONS = frozenset(
    {
        "test.correctness",
        "test.performance",
        "profile.collect",
        "correctness.replay",
    }
)
LOCAL_EXECUTORS = frozenset(
    {"evidence-index", "endpoint-registry", "postprocess-recovery"}
)


class EvidenceOperationIntakeError(RuntimeError):
    def __init__(self, message: str, *, failure_class: str = "operation-invalid") -> None:
        super().__init__(message)
        self.failure_class = failure_class


class EvidenceOperationIntake:
    """Execute queued V5 evidence operations through registered executors."""

    def __init__(
        self,
        *,
        root: Path,
        database: Any,
        registry: Any,
        code_generation: str,
        consumer_id: str = "ascendop-v5-evidence-intake",
    ) -> None:
        self.root = root.resolve()
        self.database = database
        self.registry = registry
        self.code_generation = code_generation
        self.consumer_id = consumer_id
        self.request_root = (
            self.root / ".ascendop-work" / "acceptance" / "test_requests"
        )

    def run_once(self) -> dict[str, Any]:
        claimed = self._claim()
        if claimed is None:
            return {"state": "idle", "generated_count": 0, "errors": []}
        try:
            if str(claimed["operation_code"]) in DEVICE_OPERATIONS:
                return self._route_device_operation(claimed)
            return self._execute_local_operation(claimed)
        except EvidenceOperationIntakeError as exc:
            return self._handle_failure(claimed, exc.failure_class, str(exc))
        except Exception as exc:
            return self._handle_failure(
                claimed,
                "operation-execution",
                f"{type(exc).__name__}: {exc}",
            )

    def _claim(self) -> dict[str, Any] | None:
        for executor in ("test-engine", *sorted(LOCAL_EXECUTORS)):
            claimed = self.database.claim_evidence_operation(
                executor=executor,
                consumer_id=self.consumer_id,
                lease_seconds=300,
            )
            if claimed is not None:
                return claimed
        return None

    def _route_device_operation(self, operation: Mapping[str, Any]) -> dict[str, Any]:
        request = dict(operation["request"])
        parameters = dict(request["parameters"])
        registration = self._operator_registration(str(operation["operator_id"]))
        display_name = str(registration["display_name"])
        test_version = str(parameters["test_version"])
        submit_root = self._submit_root(display_name, test_version)
        command = extract_submit_command(submit_root / "SUBMIT.md")
        parsed = parse_submit_command(command)
        if (
            parsed["op"] != display_name
            or parsed["test_version"] != test_version
            or parsed["case_version"] != str(parameters["case_version"])
        ):
            raise EvidenceOperationIntakeError(
                "evidence operation source identity does not match its parameters"
            )
        operation_code = str(operation["operation_code"])
        operation_kind = "operator-test"
        profiler_mode = ""
        profiler_plan = None
        diagnostic_plan = None
        execution_profile = f"evidence-{operation_code.replace('.', '-')}-v1"
        if operation_code == "profile.collect":
            operation_kind = "diagnostic-profile"
            profiler_mode = _profiler_mode(str(parameters["profiler_mode"]))
            contract = build_test_contract(
                submit_root,
                execution_profile=FUSED_SCALABLE_PROFILE,
            )
            cases = list(contract["performance_cases"])
            source = submit_root / "pending_snapshot" / "source_snapshot"
            profiler_plan = {
                "operator": display_name,
                "case_version": parsed["case_version"],
                "blocker_result_version": test_version,
                "blocker_generation": str(operation["operation_request_id"]),
                "request_sha256": _digest(request),
                "request_state_path": (
                    ".ascendop-work/runtime/evidence-operations/"
                    f"{operation['operation_request_id']}.json"
                ),
                "target_version": test_version,
                "target_source_sha256": tree_digest(source),
                "cases": cases,
                "profiler_mode": (
                    "deep-dual"
                    if profiler_mode == "primary-roofline-all-cases"
                    else "fast-single"
                ),
                "roofline_cases": (
                    cases
                    if profiler_mode == "primary-roofline-all-cases"
                    else []
                ),
                "primary_metrics": "PipeUtilization,Occupancy",
                "profile_timeout_seconds": 90,
                "request_attempt": int(operation["claim"]["attempts"]),
            }
        elif operation_code == "correctness.replay":
            operation_kind = "diagnostic-correctness-replay"
            profiler_mode = "none"
            diagnostic_plan = {
                "protocol_version": "ascendop-v5-evidence-operation-v1",
                "operation_request_id": str(operation["operation_request_id"]),
                "request_state_path": (
                    ".ascendop-work/runtime/evidence-operations/"
                    f"{operation['operation_request_id']}.json"
                ),
                "failing_cases": list(parameters["failing_cases"]),
                "requested_artifacts": list(
                    parameters.get("requested_artifacts")
                    or ["correctness-replay-report"]
                ),
            }
        manifest = build_test_request_manifest(
            self.root,
            {
                "op": display_name,
                "test_version": test_version,
                "command": command,
                "job_id_suffix": str(operation["operation_request_id"]),
            },
            registration,
            execution_profile=execution_profile,
            submit_root_override=submit_root,
            publish_eligible=False,
            operation_kind=operation_kind,
            profiler_mode=profiler_mode,
            profiler_plan=profiler_plan,
            diagnostic_plan=diagnostic_plan,
            evidence_operation={
                "operation_request_id": str(operation["operation_request_id"]),
                "operation_code": operation_code,
                "origin": dict(request["origin"]),
                "expected_consumer": str(operation["expected_consumer"]),
                "registry_generation": str(request["registry_generation"]),
                "registry_digest": str(request["registry_digest"]),
            },
            lineage={
                "trace_id": str(request["origin"]["action_id"]),
                "origin_action_id": str(request["origin"]["action_id"]),
                "origin_iteration_id": str(request["origin"]["iteration_id"]),
                "promotion_action_id": "",
                "promotion_receipt_id": "",
                "candidate_id": test_version,
                "operation_request_id": str(operation["operation_request_id"]),
            },
        )
        manifest_path, persisted = persist_test_request(self.request_root, manifest)
        record = self.database.create_test_request(persisted, manifest_path)
        routed = route_and_prepare_test_request(
            self.root,
            self.database,
            self.registry,
            str(record["request_id"]),
            code_generation=self.code_generation,
        )
        attempt = routed.get("attempt")
        if not isinstance(attempt, Mapping):
            raise EvidenceOperationIntakeError(
                "no eligible endpoint is currently available",
                failure_class="endpoint-unavailable",
            )
        routed_operation = self.database.route_evidence_operation(
            operation_request_id=str(operation["operation_request_id"]),
            claim_token=str(operation["claim"]["claim_token"]),
            test_request_id=str(record["request_id"]),
            wire_attempt_id=str(attempt["attempt_id"]),
            endpoint_id=str(attempt["endpoint_id"]),
            execution_environment_id=str(attempt["execution_environment_id"]),
        )
        return {
            "state": "routed",
            "generated_count": 1,
            "operation_request_id": operation["operation_request_id"],
            "operation_code": operation_code,
            "request_id": record["request_id"],
            "attempt_id": attempt["attempt_id"],
            "evidence_operation": routed_operation,
            "errors": [],
        }

    def _execute_local_operation(self, operation: Mapping[str, Any]) -> dict[str, Any]:
        code = str(operation["operation_code"])
        parameters = dict(operation["request"]["parameters"])
        endpoint_id = ""
        environment_id = ""
        if code == "profile.compare":
            baseline = self._bounded_relative(parameters["baseline_evidence_ref"])
            candidate = self._bounded_relative(parameters["candidate_evidence_ref"])
            payload = {
                "baseline_sha256": _file_sha256(baseline),
                "candidate_sha256": _file_sha256(candidate),
                "comparison_rule": str(parameters["comparison_rule"]),
            }
            payload["digests_equal"] = (
                payload["baseline_sha256"] == payload["candidate_sha256"]
            )
            artifacts = [baseline, candidate]
        elif code == "environment.conformance":
            endpoint_id = str(parameters["endpoint_id"])
            environment_id = str(parameters["execution_environment_id"])
            endpoint = next(
                (
                    item
                    for item in self.registry.endpoints
                    if item.endpoint_id == endpoint_id
                ),
                None,
            )
            if endpoint is None or not endpoint.enabled:
                raise EvidenceOperationIntakeError(
                    f"endpoint is not available: {endpoint_id}",
                    failure_class="endpoint-unavailable",
                )
            if endpoint.execution_environment_id != environment_id:
                raise EvidenceOperationIntakeError(
                    "endpoint execution environment does not match the request",
                    failure_class="environment-mismatch",
                )
            payload = {
                "endpoint_id": endpoint.endpoint_id,
                "endpoint_generation": endpoint.generation,
                "execution_environment_id": endpoint.execution_environment_id,
                "requirements_generation": str(
                    parameters["requirements_generation"]
                ),
                "capabilities": dict(endpoint.capabilities),
                "conforms": True,
            }
            artifacts = []
        elif code == "artifact.recover":
            destination = self._bounded_relative(parameters["artifact_ref"])
            source = self._bounded_relative(parameters["recovery_source"])
            expected = str(parameters["expected_sha256"]).lower()
            if destination.is_file() and _file_sha256(destination) == expected:
                recovered = False
            else:
                if not source.is_file() or _file_sha256(source) != expected:
                    raise EvidenceOperationIntakeError(
                        "recovery source is absent or has the wrong digest",
                        failure_class="artifact-not-visible",
                    )
                destination.parent.mkdir(parents=True, exist_ok=True)
                temporary = destination.parent / f".recover-{uuid.uuid4().hex[:8]}"
                shutil.copy2(source, temporary)
                os.replace(temporary, destination)
                recovered = True
            payload = {
                "artifact_ref": destination.relative_to(self.root).as_posix(),
                "sha256": _file_sha256(destination),
                "recovered": recovered,
            }
            artifacts = [destination]
        else:
            raise EvidenceOperationIntakeError(
                f"no executor is registered for {code}"
            )
        routed = self.database.route_evidence_operation(
            operation_request_id=str(operation["operation_request_id"]),
            claim_token=str(operation["claim"]["claim_token"]),
            endpoint_id=endpoint_id,
            execution_environment_id=environment_id,
        )
        result = self._terminal_result(
            routed,
            status="completed",
            summary=f"{code} completed",
            payload=payload,
            artifacts=artifacts,
        )
        completed = self.database.complete_evidence_operation(result)
        return {
            "state": "completed",
            "generated_count": 0,
            "operation_request_id": operation["operation_request_id"],
            "operation_code": code,
            "evidence_operation": completed,
            "errors": [],
        }

    def _handle_failure(
        self,
        operation: Mapping[str, Any],
        failure_class: str,
        message: str,
    ) -> dict[str, Any]:
        definition = evidence_operation_definition(str(operation["operation_code"]))
        retry = dict(definition["retry_policy"])
        attempts = int(operation["claim"]["attempts"])
        retryable = failure_class in set(retry["retryable_failure_classes"])
        if retryable and attempts < int(retry["max_attempts"]):
            deferred = self.database.defer_evidence_operation(
                operation_request_id=str(operation["operation_request_id"]),
                claim_token=str(operation["claim"]["claim_token"]),
                delay_seconds=int(retry["backoff_seconds"]),
                failure_class=failure_class,
            )
            return {
                "state": "deferred",
                "generated_count": 0,
                "operation_request_id": operation["operation_request_id"],
                "operation_code": operation["operation_code"],
                "evidence_operation": deferred,
                "errors": [{"failure_class": failure_class, "error": message}],
            }
        result = self._terminal_result(
            operation,
            status="failed",
            summary=message,
            payload={"error": message},
            artifacts=[],
            failure_class=failure_class,
        )
        completed = self.database.complete_evidence_operation(result)
        return {
            "state": "failed",
            "generated_count": 0,
            "operation_request_id": operation["operation_request_id"],
            "operation_code": operation["operation_code"],
            "evidence_operation": completed,
            "errors": [{"failure_class": failure_class, "error": message}],
        }

    def _terminal_result(
        self,
        operation: Mapping[str, Any],
        *,
        status: str,
        summary: str,
        payload: Mapping[str, Any],
        artifacts: list[Path],
        failure_class: str | None = None,
    ) -> dict[str, Any]:
        request = dict(operation["request"])
        route = dict(operation["route"])
        definition = evidence_operation_definition(str(operation["operation_code"]))
        identity = {
            "operation_request_id": operation["operation_request_id"],
            "status": status,
            "payload": dict(payload),
        }
        return validate_evidence_operation_result(
            {
                "schema": EVIDENCE_OPERATION_RESULT_SCHEMA,
                "operation_result_id": f"eores-{_digest(identity)[:24]}",
                "operation_request_id": str(operation["operation_request_id"]),
                "registry_generation": str(request["registry_generation"]),
                "registry_digest": str(request["registry_digest"]),
                "operation_code": str(operation["operation_code"]),
                "origin": dict(request["origin"]),
                "expected_consumer": str(operation["expected_consumer"]),
                "status": status,
                "summary": summary,
                "execution": route,
                "evidence": {
                    "evidence_type": str(definition["produced_evidence_types"][0]),
                    "artifact_refs": [
                        path.relative_to(self.root).as_posix() for path in artifacts
                    ],
                    "payload": dict(payload),
                },
                "failure_class": failure_class,
                "completed_at": _utc_now(),
            }
        )

    def _operator_registration(self, operator_id: str) -> dict[str, Any]:
        matches = [
            item
            for item in self.database.operator_registrations()
            if str(item["operator_id"]) == operator_id
        ]
        if len(matches) != 1:
            raise EvidenceOperationIntakeError(
                f"operator registration is missing or ambiguous: {operator_id}"
            )
        return matches[0]

    def _submit_root(self, display_name: str, test_version: str) -> Path:
        candidates = (
            self.root
            / "operators_testresult"
            / display_name
            / test_version
            / "submit_snapshot",
            self.root / "TestUtils" / "submit" / display_name / test_version,
        )
        for candidate in candidates:
            if (candidate / "SUBMIT.md").is_file():
                return candidate.resolve()
        raise EvidenceOperationIntakeError(
            f"candidate submit snapshot is not available: {display_name}/{test_version}",
            failure_class="artifact-not-visible",
        )

    def _bounded_relative(self, value: Any) -> Path:
        text = str(value or "").replace("\\", "/").strip("/")
        path = (self.root / text).resolve()
        if not text or (path != self.root and self.root not in path.parents):
            raise EvidenceOperationIntakeError(
                f"operation path escapes the workspace: {value!r}"
            )
        return path


def _profiler_mode(value: str) -> str:
    mapping = {
        "fast-single": "primary-all-cases",
        "primary-all-cases": "primary-all-cases",
        "deep-dual": "primary-roofline-all-cases",
        "primary-roofline-all-cases": "primary-roofline-all-cases",
    }
    try:
        return mapping[value]
    except KeyError as exc:
        raise EvidenceOperationIntakeError(
            f"unsupported profiler mode: {value}"
        ) from exc


def _file_sha256(path: Path) -> str:
    if not path.is_file():
        raise EvidenceOperationIntakeError(
            f"evidence artifact is not visible: {path}",
            failure_class="artifact-not-visible",
        )
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _digest(value: Mapping[str, Any]) -> str:
    payload = json.dumps(
        dict(value),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _utc_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


__all__ = ["EvidenceOperationIntake", "EvidenceOperationIntakeError"]
