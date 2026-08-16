from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Mapping, Sequence

from ascendop_protocol.competition import (
    canonical_tree_digest,
    collect_project_evidence,
    lineage_tree_digest,
    validate_official_problem_snapshot,
)

from ascendop_daemon.storage.control_validation import canonical_json, utc_now


PROGRESS_SCHEMA = "ascendop.official-progress.v1"
PROFILE_SCHEMA = "ascendop.official-eval-progress-profile.v1"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class OperatorBinding:
    campaign_id: str
    policy_id: str
    rules_generation: str
    operator_id: str
    snapshot_path: Path
    runbook_path: Path
    required_case_count: int
    opdef_paths: tuple[str, ...]


class OfficialProgressPublisher:
    """Publish a read-only official-evaluation projection from V3 facts."""

    def __init__(
        self,
        *,
        root: Path,
        database: Any,
        output_path: Path,
        profile_glob: str,
    ) -> None:
        self.root = root.resolve()
        self.database = database
        self.output_path = (
            output_path.resolve()
            if output_path.is_absolute()
            else (self.root / output_path).resolve()
        )
        self.profile_glob = profile_glob

    def run_once(self) -> dict[str, Any]:
        bindings = self._load_bindings()
        registrations = {
            row["operator_id"]: row for row in self.database.operator_registrations()
        }
        operators: list[dict[str, Any]] = []
        errors: list[dict[str, str]] = []
        for binding in bindings:
            registration = registrations.get(binding.operator_id)
            if registration is None:
                errors.append(
                    {"operator_id": binding.operator_id, "error": "operator-not-registered"}
                )
                continue
            try:
                operators.append(self._operator_progress(binding, registration))
            except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
                errors.append(
                    {"operator_id": binding.operator_id, "error": str(exc)}
                )
                operators.append(self._held_operator(binding, registration, str(exc)))

        semantic = {
            "schema": PROGRESS_SCHEMA,
            "operators": sorted(operators, key=lambda item: item["operator_id"]),
        }
        generation = hashlib.sha256(canonical_json(semantic).encode("utf-8")).hexdigest()
        snapshot = {
            **semantic,
            "generation": generation,
            "generated_at": utc_now(),
            "projection_errors": errors,
        }
        self._atomic_write(snapshot)
        return {
            "path": str(self.output_path),
            "generation": generation,
            "operator_count": len(operators),
            "candidate_count": sum(bool(item.get("candidate")) for item in operators),
            "held_count": sum(
                bool((item.get("candidate") or {}).get("hold_reasons"))
                for item in operators
            ),
            "errors": errors,
        }

    def _load_bindings(self) -> list[OperatorBinding]:
        bindings: list[OperatorBinding] = []
        seen: set[tuple[str, str]] = set()
        for path in sorted(self.root.glob(self.profile_glob)):
            raw = _read_object(path)
            if raw.get("schema") != PROFILE_SCHEMA:
                raise ValueError(f"unsupported official progress profile: {path}")
            campaign_id = _text(raw, "campaign_id")
            policy_id = _text(raw, "policy_id")
            rules_generation = _text(raw, "rules_generation")
            entries = raw.get("operators")
            if not isinstance(entries, list) or not entries:
                raise ValueError(f"official progress profile has no operators: {path}")
            for entry in entries:
                if not isinstance(entry, Mapping):
                    raise ValueError(f"invalid operator binding in {path}")
                operator_id = _text(entry, "operator_id")
                identity = (campaign_id, operator_id)
                if identity in seen:
                    raise ValueError(f"duplicate official operator binding: {identity}")
                seen.add(identity)
                count = entry.get("required_case_count", 16)
                if isinstance(count, bool) or not isinstance(count, int) or count < 1:
                    raise ValueError(f"{operator_id}: required_case_count is invalid")
                opdef_paths = entry.get("opdef_paths", [])
                if not isinstance(opdef_paths, list):
                    raise ValueError(f"{operator_id}: opdef_paths must be an array")
                bindings.append(
                    OperatorBinding(
                        campaign_id=campaign_id,
                        policy_id=policy_id,
                        rules_generation=rules_generation,
                        operator_id=operator_id,
                        snapshot_path=self._profile_path(entry, "snapshot_path"),
                        runbook_path=self._profile_path(entry, "runbook_path"),
                        required_case_count=count,
                        opdef_paths=tuple(_bounded_text(item) for item in opdef_paths),
                    )
                )
        return bindings

    def _profile_path(self, entry: Mapping[str, Any], field: str) -> Path:
        relative = _bounded_text(entry.get(field))
        return (self.root / relative).resolve()

    def _operator_progress(
        self,
        binding: OperatorBinding,
        registration: Mapping[str, Any],
    ) -> dict[str, Any]:
        snapshot = validate_official_problem_snapshot(_read_object(binding.snapshot_path))
        if snapshot["campaign_id"] != binding.campaign_id:
            raise ValueError(f"{binding.operator_id}: snapshot campaign mismatch")
        if snapshot["operator_id"] != binding.operator_id:
            raise ValueError(f"{binding.operator_id}: snapshot identity mismatch")
        if not binding.runbook_path.is_file():
            raise ValueError(f"{binding.operator_id}: submission runbook is missing")

        lifecycle = "active" if registration["desired_state"] == "enabled" else "inactive"
        candidate = self._candidate(binding, registration, snapshot)
        return {
            "operator_id": binding.operator_id,
            "display_name": str(registration["display_name"]),
            "lifecycle": lifecycle,
            "activation_generation": _activation_generation(
                str(registration["registration_generation"])
            ),
            "environment": (
                candidate.pop("_environment")
                if candidate is not None
                else {
                    "soc": str(snapshot["environment"]["soc"][0]),
                    "cann": str(snapshot["environment"]["cann"][0]),
                    "build_identity": "no-completed-candidate",
                }
            ),
            "candidate": candidate,
            "solver_delivery_target": None,
        }

    def _candidate(
        self,
        binding: OperatorBinding,
        registration: Mapping[str, Any],
        snapshot: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        request = self._latest_completed_request(binding.operator_id, binding.campaign_id)
        if request is None:
            return None
        manifest = request["manifest"]
        source_root = self._archived_source_root(registration, request)
        actual_source_digest = canonical_tree_digest(source_root)
        actual_lineage_digest = lineage_tree_digest(source_root)
        request_source_digest = str(manifest["input_identity"]["source_sha256"])
        project = collect_project_evidence(
            source_root,
            snapshot,
            opdef_paths=binding.opdef_paths or None,
        )
        passes = self._successful_attempts(
            request_id=str(request["request_id"]),
            workspace=self._root_path(registration["workspace"]["source"]),
            required_case_count=binding.required_case_count,
        )
        latest = passes[-1] if passes else None
        stable_environment = len(
            {
                (
                    item["endpoint_id"],
                    item["endpoint_generation"],
                    item["execution_environment_id"],
                    item["engine_code_generation"],
                )
                for item in passes
            }
        ) == 1
        isolated_processes = len(
            {item["correctness_process_identity"] for item in passes}
        ) == len(passes)
        deterministic = len(passes) >= 2 and stable_environment and isolated_processes
        lineage_closed, lineage_reason = self._lineage_status(
            source_root.parent / "SOURCE_LINEAGE.json",
            actual_lineage_digest,
        )
        gates = {
            "correctness_all_pass": bool(passes),
            "lineage_closed": lineage_closed,
            "project_digest_stable": bool(
                deterministic and project.mapping_complete and project.project_digest
            ),
            "official_opdef_parity": project.opdef_parity,
            "deterministic_same_digest_passes": deterministic,
            "project_mapping_complete": project.mapping_complete,
        }
        reasons = [name for name, value in gates.items() if not value]
        if request_source_digest != actual_source_digest:
            reasons.append("request_source_digest_mismatch")
            gates["lineage_closed"] = False
        if lineage_reason:
            reasons.append(lineage_reason)

        evidence = {
            "request_id": request["request_id"],
            "request_digest": request["request_digest"],
            "source_digest": actual_source_digest,
            "lineage_digest": actual_lineage_digest,
            "project_digest": project.project_digest,
            "attempts": [item["evidence"] for item in passes],
        }
        build_identity = _digest_json(
            {
                "endpoint_id": latest["endpoint_id"] if latest else "unrouted",
                "endpoint_generation": latest["endpoint_generation"] if latest else "",
                "execution_environment_id": (
                    latest["execution_environment_id"] if latest else ""
                ),
                "engine_code_generation": (
                    latest["engine_code_generation"] if latest else ""
                ),
            }
        )
        return {
            "policy_id": binding.policy_id,
            "source_digest": actual_source_digest,
            "source_identity": {
                "execution_source_digest": actual_source_digest,
                "lineage_tree_digest": actual_lineage_digest,
                "official_project_digest": project.project_digest,
            },
            "source_version": str(request["test_version"]),
            "case_version": str(manifest["case_version"]),
            "evidence_digest": _digest_json(evidence),
            "local_status": "PASS" if passes else "HELD",
            "correctness": {
                "passed": bool(passes),
                "case_count": binding.required_case_count,
                "deterministic_passes": len(passes),
                "attempt_ids": [item["attempt_id"] for item in passes],
            },
            "metrics": {
                "small_time_us": latest["performance"].get("small_time_us") if latest else None,
                "large_time_us": latest["performance"].get("large_time_us") if latest else None,
                "weighted_time_us": latest["performance"].get("weighted_time_us") if latest else None,
                "endpoint_id": latest["endpoint_id"] if latest else "",
                "execution_environment_id": (
                    latest["execution_environment_id"] if latest else ""
                ),
                "rules_generation": binding.rules_generation,
            },
            "submission": {
                "project_digest": project.project_digest or ("0" * 64),
                "source_file_digests": dict(project.source_file_digests) or {"missing": "0" * 64},
                "workspace": str(source_root.relative_to(self.root)).replace("\\", "/"),
                "runbook_path": str(binding.runbook_path.relative_to(self.root)).replace("\\", "/"),
                "submit_url": str(snapshot["submit_url"]),
                "gate": gates,
            },
            "hold_reasons": sorted(set(reasons)),
            "project_evidence": {
                "missing_files": list(project.missing_files),
                "extra_files": list(project.extra_files),
                "mismatched_opdef_files": list(project.mismatched_opdef_files),
            },
            "_environment": {
                "soc": str(snapshot["environment"]["soc"][0]),
                "cann": str(snapshot["environment"]["cann"][0]),
                "build_identity": build_identity,
            },
        }

    def _archived_source_root(
        self,
        registration: Mapping[str, Any],
        request: Mapping[str, Any],
    ) -> Path:
        display_name = _bounded_text(registration["display_name"])
        test_version = _bounded_text(request["test_version"])
        source_root = (
            self.root
            / "operators_testresult"
            / display_name
            / test_version
            / "submit_snapshot"
            / "pending_snapshot"
            / "source_snapshot"
        ).resolve()
        if not source_root.is_dir():
            raise ValueError(
                f"{display_name}/{test_version}: immutable result source is missing"
            )
        return source_root

    def _latest_completed_request(
        self,
        operator_id: str,
        campaign_id: str,
    ) -> dict[str, Any] | None:
        with self.database.connection() as connection:
            rows = connection.execute(
                """
                SELECT * FROM test_requests
                WHERE operator_id=? AND state='completed'
                ORDER BY updated_at DESC, created_at DESC
                """,
                (operator_id,),
            ).fetchall()
        for row in rows:
            value = dict(row)
            manifest = json.loads(value["manifest_json"])
            if manifest.get("season") != campaign_id:
                continue
            if manifest.get("workflow", {}).get("publish_eligible") is not True:
                continue
            value["manifest"] = manifest
            return value
        return None

    def _successful_attempts(
        self,
        *,
        request_id: str,
        workspace: Path,
        required_case_count: int,
    ) -> list[dict[str, Any]]:
        with self.database.connection() as connection:
            rows = connection.execute(
                """
                SELECT a.*, r.state AS return_state
                FROM execution_attempts a
                JOIN transport_returns r ON r.attempt_id=a.attempt_id
                WHERE a.request_id=? AND a.state='completed'
                  AND r.state='acknowledged'
                ORDER BY a.ordinal
                """,
                (request_id,),
            ).fetchall()
        passes: list[dict[str, Any]] = []
        for row in rows:
            attempt = dict(row)
            attempt_id = str(attempt["attempt_id"])
            result_root = workspace / ".ascendop" / "results" / request_id / attempt_id
            projection_path = result_root / "PROJECTION.json"
            correctness_path = (
                result_root
                / "artifacts"
                / "result_bundle"
                / "result"
                / "CORRECTNESS_BATCH.json"
            )
            perf_path = correctness_path.with_name("PERF_SUMMARY.txt")
            if not projection_path.is_file() or not correctness_path.is_file():
                continue
            projection = _read_object(projection_path)
            correctness = _read_object(correctness_path)
            if projection.get("request_id") != request_id or projection.get("attempt_id") != attempt_id:
                continue
            if not _correctness_passed(correctness, required_case_count):
                continue
            result = projection.get("result", {})
            engine = result.get("engine", {}) if isinstance(result, Mapping) else {}
            if (
                not isinstance(result, Mapping)
                or result.get("outcome") != "success"
                or not isinstance(engine, Mapping)
                or engine.get("state") != "completed"
            ):
                continue
            process_identity = _correctness_process_identity(engine)
            if not process_identity:
                continue
            passes.append(
                {
                    "attempt_id": attempt_id,
                    "endpoint_id": str(attempt["endpoint_id"]),
                    "endpoint_generation": str(attempt["endpoint_generation"]),
                    "execution_environment_id": str(attempt["execution_environment_id"]),
                    "engine_code_generation": str(engine.get("engine_code_generation") or ""),
                    "correctness_process_identity": process_identity,
                    "performance": _parse_performance(perf_path),
                    "evidence": {
                        "attempt_id": attempt_id,
                        "projection_sha256": _sha256_path(projection_path),
                        "correctness_sha256": _sha256_path(correctness_path),
                        "performance_sha256": _sha256_path(perf_path) if perf_path.is_file() else "",
                    },
                }
            )
        return passes

    def _lineage_status(self, path: Path, actual_digest: str) -> tuple[bool, str]:
        if not path.is_file():
            return False, "lineage_missing"
        lineage = _read_object(path)
        candidate = lineage.get("candidate")
        parent = lineage.get("parent")
        if not isinstance(candidate, Mapping) or candidate.get("sha256") != actual_digest:
            return False, "lineage_candidate_digest_mismatch"
        if not isinstance(parent, Mapping) or parent.get("resolved") is not True:
            return False, "lineage_parent_unresolved"
        parent_digest = str(parent.get("sha256") or "").lower()
        if not _SHA256.fullmatch(parent_digest):
            return False, "lineage_parent_digest_invalid"
        parent_path = parent.get("path")
        if not isinstance(parent_path, str) or not parent_path.strip():
            return False, "lineage_parent_path_missing"
        resolved_parent = self._root_path(parent_path)
        if not resolved_parent.is_dir() or lineage_tree_digest(resolved_parent) != parent_digest:
            return False, "lineage_parent_digest_mismatch"
        return True, ""

    def _held_operator(
        self,
        binding: OperatorBinding,
        registration: Mapping[str, Any],
        reason: str,
    ) -> dict[str, Any]:
        return {
            "operator_id": binding.operator_id,
            "display_name": str(registration["display_name"]),
            "lifecycle": "inactive",
            "activation_generation": _activation_generation(
                str(registration["registration_generation"])
            ),
            "environment": {
                "soc": "unknown",
                "cann": "unknown",
                "build_identity": "projection-error",
            },
            "candidate": None,
            "solver_delivery_target": None,
            "projection_error": reason,
        }

    def _root_path(self, value: Any) -> Path:
        relative = _bounded_text(value)
        return (self.root / relative).resolve()

    def _atomic_write(self, snapshot: Mapping[str, Any]) -> None:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(snapshot, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
        temporary = self.output_path.with_name(
            f".{self.output_path.name}.{os.getpid()}.tmp"
        )
        temporary.write_text(payload, encoding="utf-8", newline="")
        os.replace(temporary, self.output_path)


def _correctness_passed(raw: Mapping[str, Any], required_case_count: int) -> bool:
    executions = raw.get("executions")
    return bool(
        raw.get("state") == "passed"
        and raw.get("case_count") == required_case_count
        and raw.get("pass_count") == required_case_count
        and raw.get("fail_count") == 0
        and raw.get("execution_count") == raw.get("expected_execution_count")
        and isinstance(executions, list)
        and len(executions) == required_case_count
        and all(
            isinstance(item, Mapping) and item.get("verdict") == "PASS"
            for item in executions
        )
    )


def _correctness_process_identity(engine: Mapping[str, Any]) -> str:
    history = engine.get("history")
    if not isinstance(history, list):
        return ""
    for item in history:
        if isinstance(item, Mapping) and item.get("stage_name") == "correctness":
            pid = item.get("pid")
            boot_id = item.get("boot_id")
            if isinstance(pid, int) and pid > 0 and isinstance(boot_id, str) and boot_id:
                return f"{boot_id}:{pid}"
    return ""


def _parse_performance(path: Path) -> dict[str, float]:
    values: dict[str, float] = {}
    if not path.is_file():
        return values
    keys = {
        "score_group_small_time_us": "small_time_us",
        "score_group_large_time_us": "large_time_us",
        "weighted_time": "weighted_time_us",
    }
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        name, separator, raw = line.partition("=")
        if separator and name in keys:
            try:
                values[keys[name]] = float(raw.strip())
            except ValueError:
                continue
    return values


def _activation_generation(value: str) -> int:
    return int(value[:15], 16) if _SHA256.fullmatch(value) else 0


def _digest_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON object expected: {path}")
    return value


def _text(value: Mapping[str, Any], field: str) -> str:
    return _bounded_or_free_text(value.get(field), field, bounded=False)


def _bounded_text(value: Any) -> str:
    return _bounded_or_free_text(value, "path", bounded=True)


def _bounded_or_free_text(value: Any, field: str, *, bounded: bool) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be non-empty text")
    text = value.strip().replace("\\", "/")
    if bounded:
        path = PurePosixPath(text)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError(f"{field} must be repository-relative")
    return text
