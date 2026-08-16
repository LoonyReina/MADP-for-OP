from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from ascendop_daemon.control_plane.test_requests import (
    build_test_request_manifest,
    persist_test_request,
    route_and_prepare_test_request,
)
from ascendop_daemon.core.atomic_io import write_json_atomic
from ascendop_daemon.workflow.solver_diagnostics import (
    discover_ready_correctness_request,
    mark_enqueued as mark_correctness_enqueued,
)
from ascendop_daemon.workflow.profiler_request_state import (
    reconcile_unpublished_profiler_budget,
)


class DiagnosticIntakeError(RuntimeError):
    pass


class DiagnosticIntake:
    """Materialize registered diagnostic gates into ordinary Wire V3 requests."""

    def __init__(
        self,
        *,
        root: Path,
        config: Any,
        database: Any,
        registry: Any,
        code_generation: str,
    ) -> None:
        self.root = root.resolve()
        self.config = config
        self.database = database
        self.registry = registry
        self.code_generation = code_generation
        self.request_root = (
            self.root / ".ascendop-work" / "acceptance" / "test_requests"
        )

    def run_once(self) -> dict[str, Any]:
        try:
            correctness = discover_ready_correctness_request(
                self.root, self.config
            )
            if correctness is not None:
                return self._materialize_correctness(correctness)
            profiler = self._next_profiler_request()
            if profiler is not None:
                return self._materialize_profiler(profiler)
            return {"state": "idle", "generated_count": 0, "errors": []}
        except Exception as exc:
            return {
                "state": "failed",
                "generated_count": 0,
                "errors": [{"error": f"{type(exc).__name__}: {exc}"}],
            }

    def _materialize_correctness(
        self, request: dict[str, Any]
    ) -> dict[str, Any]:
        candidate = dict(request["candidate"])
        registration = self.database.operator_for_display_name(candidate["op"])
        manifest = build_test_request_manifest(
            self.root,
            candidate,
            registration,
            execution_profile="diagnostic-correctness-v1",
            submit_root_override=Path(candidate["submit_root_override"]),
            publish_eligible=False,
            operation_kind="diagnostic-correctness-replay",
            profiler_mode="none",
            diagnostic_plan=dict(request["diagnostic"]),
            requested_device_session_seconds=self._requested_device_session_seconds(
                dict(request["state"])
            ),
        )
        record, route = self._persist_and_route(manifest)
        attempt_id = self._routed_attempt_id(route)
        if not attempt_id:
            return self._waiting_report(record, route, "diagnostic-correctness-replay")
        marked = mark_correctness_enqueued(
            request,
            request_id=str(record["request_id"]),
            attempt_id=attempt_id,
        )
        return {
            "state": "routed",
            "generated_count": 1,
            "operation_kind": "diagnostic-correctness-replay",
            "request_id": record["request_id"],
            "attempt_id": attempt_id,
            "sidecar": marked,
            "errors": [],
        }

    def _materialize_profiler(self, request: dict[str, Any]) -> dict[str, Any]:
        candidate = dict(request["candidate"])
        registration = self.database.operator_for_display_name(candidate["op"])
        pinned_endpoint = request.get("pinned_endpoint")
        allowed_endpoint_ids = self._comparison_endpoint_candidates(
            dict(request["state"])
        )
        manifest = build_test_request_manifest(
            self.root,
            candidate,
            registration,
            execution_profile="profiler-evidence-v1",
            submit_root_override=Path(candidate["submit_root_override"]),
            publish_eligible=False,
            operation_kind="diagnostic-profile",
            profiler_mode=str(request["profiler_mode"]),
            profiler_plan=dict(request["profiler_plan"]),
            requested_device_session_seconds=self._requested_device_session_seconds(
                dict(request["state"])
            ),
            pinned_endpoint=pinned_endpoint,
            allowed_endpoint_ids=allowed_endpoint_ids,
        )
        record, route = self._persist_and_route(manifest)
        attempt_id = self._routed_attempt_id(route)
        if not attempt_id:
            return self._waiting_report(record, route, "diagnostic-profile")
        self._verify_comparison_route(request, route)
        sidecar = self._mark_profiler_enqueued(
            request,
            request_id=str(record["request_id"]),
            attempt_id=attempt_id,
            route=route,
        )
        return {
            "state": "routed",
            "generated_count": 1,
            "operation_kind": "diagnostic-profile",
            "request_id": record["request_id"],
            "attempt_id": attempt_id,
            "sidecar": sidecar,
            "errors": [],
        }

    def _persist_and_route(
        self, manifest: dict[str, Any]
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        manifest_path, persisted = persist_test_request(
            self.request_root, manifest
        )
        record = self.database.create_test_request(persisted, manifest_path)
        route = route_and_prepare_test_request(
            self.root,
            self.database,
            self.registry,
            str(record["request_id"]),
            code_generation=self.code_generation,
        )
        return record, route

    @staticmethod
    def _routed_attempt_id(route: dict[str, Any]) -> str:
        attempt = route.get("attempt")
        if not isinstance(attempt, dict):
            return ""
        return str(attempt.get("attempt_id") or "")

    @staticmethod
    def _waiting_report(
        record: dict[str, Any],
        route: dict[str, Any],
        operation_kind: str,
    ) -> dict[str, Any]:
        return {
            "state": "waiting-route",
            "generated_count": 0,
            "operation_kind": operation_kind,
            "request_id": record["request_id"],
            "route": route,
            "errors": [],
        }

    def _next_profiler_request(self) -> dict[str, Any] | None:
        base = self.root / "TestUtils" / "tester_daemon" / "profiler_requests"
        for state_path in sorted(base.glob("*/*/*/request.json")):
            state = self._read_object(state_path)
            if str(state.get("status") or "") in {"complete", "unsupported"}:
                continue
            operator = str(state.get("operator") or "")
            if operator not in self.config.operators:
                continue
            targets = state.get("targets")
            if not isinstance(targets, list):
                continue
            target = next(
                (
                    value
                    for value in targets
                    if isinstance(value, dict)
                    and str(value.get("status") or "") == "planned"
                ),
                None,
            )
            if target is None:
                continue
            state = reconcile_unpublished_profiler_budget(
                self.root,
                state_path,
                state,
            )
            target = next(
                value
                for value in state["targets"]
                if isinstance(value, dict)
                and str(value.get("status") or "") == "planned"
            )
            requested_mode = str(
                state.get("requested_profiler_mode") or "primary-all-cases"
            )
            profiler_mode = {
                "fast-single": "primary-all-cases",
                "batched-primary-only": "primary-all-cases",
                "primary-all-cases": "primary-all-cases",
                "deep-dual": "primary-roofline-all-cases",
                "batched-primary-roofline": "primary-roofline-all-cases",
                "primary-roofline-all-cases": "primary-roofline-all-cases",
            }.get(requested_mode)
            if profiler_mode is None:
                raise DiagnosticIntakeError(
                    f"unsupported profiler mode: {requested_mode}"
                )
            cases = [int(value) for value in state.get("cases", [])]
            if not cases:
                raise DiagnosticIntakeError(
                    f"profiler request has no cases: {operator}/{state_path}"
                )
            target_version = str(target.get("test_version") or "")
            submit_snapshot = (self.root / str(target.get("submit_snapshot") or "")).resolve()
            self._bounded(submit_snapshot)
            attempt = int(target.get("attempt", 0) or 0) + 1
            collection_mode = profiler_mode
            engine_mode = (
                "fast-single"
                if profiler_mode == "primary-all-cases"
                else "deep-dual"
            )
            profiler_plan = {
                "protocol_version": "ascendop-profiler-plan-v3",
                "request_attempt": int(state.get("request_attempt", 1) or 1),
                "operator": operator,
                "case_version": str(state.get("case_version") or ""),
                "blocker_result_version": str(
                    state.get("blocker_result_version") or ""
                ),
                "blocker_generation": str(state.get("blocker_generation") or ""),
                "request_sha256": str(state.get("request_sha256") or ""),
                "request_state_path": str(state.get("request_state_path") or ""),
                "profiler_execution_contract_digest": str(
                    state.get("profiler_execution_contract_digest") or ""
                ),
                "target_version": target_version,
                "target_source_sha256": str(target.get("source_sha256") or ""),
                "cases": cases,
                "case_shapes": dict(state.get("case_shapes") or {}),
                "case_specs_sha256": str(state.get("case_specs_sha256") or ""),
                "expected_block_dims": dict(
                    state.get("expected_block_dims") or {}
                ),
                "measurement_repetitions": int(
                    state.get("measurement_repetitions", 1) or 1
                ),
                "comparison_affinity": str(
                    state.get("comparison_affinity") or ""
                ),
                "profiler_mode": engine_mode,
                "collection_mode": collection_mode,
                "primary_metrics": str(state.get("primary_metrics") or ""),
                "roofline_cases": (
                    cases if profiler_mode == "primary-roofline-all-cases" else []
                ),
                "warmup_runs": 1,
                "profile_timeout_seconds": 90,
                "stage_priority": int(
                    self.config.policy.get("test_engine_profiler_stage_priority", 200)
                    or 200
                ),
            }
            remote_root = str(
                self.config.policy.get("test_engine_remote_root")
                or state.get("remote_root")
                or self.config.remote_root
            )
            hardware = str(
                self.config.policy.get("test_engine_hardware")
                or state.get("hardware")
                or "910B4"
            )
            season = str(state.get("season") or self.config.season)
            vendor = re.sub(r"[^a-z0-9_]+", "_", target_version.lower()).strip("_")
            command = (
                f"python scripts\\next_workflow.py gitpartner-run-submit "
                f"{operator} {target_version} --season {season} --mode both "
                f"--vendor {vendor}_profiler --hardware {hardware} "
                f"--case-version {state['case_version']} --remote-root {remote_root}"
            )
            return {
                "root": self.root,
                "state_path": state_path,
                "state": state,
                "target": target,
                "pinned_endpoint": self._comparison_endpoint(state),
                "profiler_mode": profiler_mode,
                "profiler_plan": profiler_plan,
                "candidate": {
                    "op": operator,
                    "test_version": target_version,
                    "command": command,
                    "attempt_index": attempt,
                    "job_id_suffix": (
                        f"prof-{state.get('generation_digest', '')}-"
                        f"a{int(state.get('request_attempt', 1) or 1):02d}-"
                        f"t{attempt:02d}"
                    ),
                    "submit_root_override": str(submit_snapshot),
                },
            }
        return None

    @staticmethod
    def _requested_device_session_seconds(state: dict[str, Any]) -> int:
        requested = int(state.get("requested_device_session_seconds", 0) or 0)
        if requested <= 0:
            snapshot = state.get("request_snapshot")
            if isinstance(snapshot, dict):
                requested = int(
                    snapshot.get("requested_device_session_seconds", 0) or 0
                )
        if requested <= 0:
            raise DiagnosticIntakeError(
                "diagnostic state has no positive device-session budget"
            )
        return requested

    def _mark_profiler_enqueued(
        self,
        request: dict[str, Any],
        *,
        request_id: str,
        attempt_id: str,
        route: dict[str, Any],
    ) -> dict[str, Any]:
        state_path = Path(request["state_path"])
        state = dict(request["state"])
        targets = [dict(value) for value in state.get("targets", [])]
        target_version = str(request["target"].get("test_version") or "")
        target = next(
            value
            for value in targets
            if str(value.get("test_version") or "") == target_version
            and str(value.get("status") or "") == "planned"
        )
        target.update(
            {
                "status": "enqueued",
                "attempt": int(request["candidate"].get("attempt_index", 1) or 1),
                "engine_job_id": f"{request_id}-{attempt_id}",
                "flow_v3_request_id": request_id,
                "flow_v3_attempt_id": attempt_id,
                "last_error": "",
                "route": self._route_identity(route),
            }
        )
        comparison_route = dict(state.get("comparison_route") or {})
        if str(state.get("comparison_affinity") or ""):
            observed_route = self._route_identity(route)
            if not comparison_route:
                comparison_route = observed_route
        state.update(
            {
                "status": "collecting",
                "targets": targets,
                "cases": list(request["profiler_plan"]["cases"]),
                "max_cases": len(request["profiler_plan"]["cases"]),
                "roofline_case_count": len(
                    request["profiler_plan"]["roofline_cases"]
                ),
                "collection_mode": request["profiler_mode"],
                "comparison_route": comparison_route,
            }
        )
        write_json_atomic(state_path, state, ensure_ascii=True, sort_keys=True)
        return {
            "operator": str(state["operator"]),
            "target_version": target_version,
            "request_id": request_id,
            "attempt_id": attempt_id,
            "case_count": len(request["profiler_plan"]["cases"]),
            "profiler_mode": request["profiler_mode"],
        }

    def _comparison_endpoint(self, state: dict[str, Any]) -> Any | None:
        if not str(state.get("comparison_affinity") or ""):
            return None
        route = dict(state.get("comparison_route") or {})
        if not route:
            return None
        endpoint_id = str(route.get("endpoint_id") or "")
        endpoint = next(
            (
                value
                for value in self.registry.endpoints
                if value.endpoint_id == endpoint_id
            ),
            None,
        )
        if endpoint is None:
            raise DiagnosticIntakeError(
                f"comparison endpoint is no longer registered: {endpoint_id}"
            )
        if (
            endpoint.generation != str(route.get("endpoint_generation") or "")
            or endpoint.execution_environment_id
            != str(route.get("execution_environment_id") or "")
        ):
            raise DiagnosticIntakeError(
                "comparison endpoint generation/environment changed before all targets completed"
            )
        if int(endpoint.capabilities.get("device_count", 0) or 0) != 1:
            raise DiagnosticIntakeError(
                "same-device comparison requires an endpoint with exactly one device"
            )
        return endpoint

    def _comparison_endpoint_candidates(
        self, state: dict[str, Any]
    ) -> list[str] | None:
        if not str(state.get("comparison_affinity") or ""):
            return None
        pinned = self._comparison_endpoint(state)
        if pinned is not None:
            return [pinned.endpoint_id]
        candidates = [
            endpoint.endpoint_id
            for endpoint in self.registry.endpoints
            if int(endpoint.capabilities.get("device_count", 0) or 0) == 1
        ]
        if not candidates:
            raise DiagnosticIntakeError(
                "same-device comparison has no registered single-device endpoint"
            )
        return candidates

    def _verify_comparison_route(
        self, request: dict[str, Any], route: dict[str, Any]
    ) -> None:
        state = dict(request["state"])
        if not str(state.get("comparison_affinity") or ""):
            return
        observed = self._route_identity(route)
        endpoint = next(
            (
                value
                for value in self.registry.endpoints
                if value.endpoint_id == observed["endpoint_id"]
            ),
            None,
        )
        if endpoint is None or int(
            endpoint.capabilities.get("device_count", 0) or 0
        ) != 1:
            raise DiagnosticIntakeError(
                "same-device comparison route must resolve to a single-device endpoint"
            )
        expected = dict(state.get("comparison_route") or {})
        if expected and observed != expected:
            raise DiagnosticIntakeError(
                "comparison target route changed after affinity was established"
            )

    @staticmethod
    def _route_identity(route: dict[str, Any]) -> dict[str, str]:
        attempt = route.get("attempt")
        if not isinstance(attempt, dict):
            raise DiagnosticIntakeError("routed profiler attempt identity is missing")
        identity = {
            "endpoint_id": str(attempt.get("endpoint_id") or ""),
            "endpoint_generation": str(
                attempt.get("endpoint_generation") or ""
            ),
            "execution_environment_id": str(
                attempt.get("execution_environment_id") or ""
            ),
        }
        if not all(identity.values()):
            raise DiagnosticIntakeError(
                "routed profiler comparison identity is incomplete"
            )
        return identity

    @staticmethod
    def _read_object(path: Path) -> dict[str, Any]:
        try:
            value = json.loads(path.read_text(encoding="utf-8-sig"))
        except (OSError, json.JSONDecodeError) as exc:
            raise DiagnosticIntakeError(f"cannot read diagnostic state {path}: {exc}") from exc
        if not isinstance(value, dict):
            raise DiagnosticIntakeError(f"diagnostic state must be an object: {path}")
        return value

    def _bounded(self, path: Path) -> None:
        if path != self.root and self.root not in path.parents:
            raise DiagnosticIntakeError(f"diagnostic path escapes workspace: {path}")


__all__ = ["DiagnosticIntake", "DiagnosticIntakeError"]
