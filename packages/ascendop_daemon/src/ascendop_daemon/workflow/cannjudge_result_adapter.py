from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any, Callable, Mapping

from ascendop_protocol.wire_v3 import canonical_digest, validate_envelope

from ascendop_daemon.runtime.locking import NamedProcessLock
from ascendop_daemon.runtime.process_adapter import (
    process_creation_flags,
    process_startupinfo,
    workspace_process_environment,
)
from ascendop_daemon.runtime.workflow_adapter import resolve_workflow_adapter


CANNJUDGE_RESULT_ADAPTER = "cannjudge-result-v3"


class CannJudgeResultAdapterError(RuntimeError):
    pass


class LegacyWireV3EnvelopeMissing(CannJudgeResultAdapterError):
    """A projected pre-hard-cut result has no recoverable immutable envelope."""


class CannJudgeV3ResultAdapter:
    """Validate a Wire V3 return and invoke the canonical TestUtils ingestor."""

    def __init__(
        self,
        root: Path,
        *,
        runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
    ) -> None:
        self.root = root.resolve()
        self.runner = runner or subprocess.run

    def ingest(
        self,
        route: Mapping[str, Any],
        returned: Mapping[str, Any],
        *,
        projection_dir: Path,
    ) -> dict[str, Any]:
        result = _object(returned.get("payload"), "return.payload")
        request_id = _safe_text(route.get("request_id"), "request_id")
        attempt_id = _safe_text(route.get("attempt_id"), "attempt_id")
        return_id = _safe_text(returned.get("return_id"), "return_id")
        projection = projection_dir.resolve()
        _bounded(self.root, projection)

        envelope, envelope_path, envelope_digest = self._load_envelope(
            route,
            result,
            request_id=request_id,
            attempt_id=attempt_id,
        )
        workflow = _object(envelope.get("workflow"), "envelope.workflow")
        contract = _object(
            envelope.get("result_contract"),
            "envelope.result_contract",
        )
        adapter = str(contract.get("ingest_adapter") or "")
        if adapter != CANNJUDGE_RESULT_ADAPTER:
            raise CannJudgeResultAdapterError(
                f"unsupported Wire V3 result adapter: {adapter or '<missing>'}"
            )
        terminal_state = str(result.get("terminal_state") or "")
        terminal_states = contract.get("terminal_states")
        if not isinstance(terminal_states, list) or terminal_state not in terminal_states:
            raise CannJudgeResultAdapterError(
                f"undeclared terminal state for {adapter}: {terminal_state or '<missing>'}"
            )

        source = self._result_source(result, projection)
        spec = _read_object(source / "spec.json", "Engine spec")
        terminal = _read_object(source / "terminal.json", "Engine terminal")
        expected_identity = {
            "request_id": request_id,
            "attempt_id": attempt_id,
            "operator": str(workflow.get("operator") or ""),
            "test_version": str(workflow.get("test_version") or ""),
        }
        observed_identity = {
            "request_id": str(spec.get("request_id") or ""),
            "attempt_id": str(spec.get("attempt_id") or ""),
            "operator": str(spec.get("operator") or ""),
            "test_version": str(spec.get("test_version") or ""),
        }
        if observed_identity != expected_identity:
            raise CannJudgeResultAdapterError(
                "Engine result identity does not match immutable envelope: "
                f"{observed_identity} != {expected_identity}"
            )
        terminal_identity = {
            "request_id": str(terminal.get("request_id") or ""),
            "attempt_id": str(terminal.get("attempt_id") or ""),
            "operator": str(terminal.get("operator") or terminal.get("op") or ""),
            "test_version": str(terminal.get("test_version") or ""),
        }
        if terminal_identity != expected_identity:
            raise CannJudgeResultAdapterError(
                "Engine terminal identity does not match immutable envelope: "
                f"{terminal_identity} != {expected_identity}"
            )
        engine_job_id = _safe_text(
            terminal.get("engine_job_id") or spec.get("engine_job_id"),
            "engine_job_id",
        )

        marker_path = projection / "WORKFLOW_INGEST.json"
        marker_identity = {
            "schema": "ascendop.cannjudge-result-ingest.v3",
            "adapter": CANNJUDGE_RESULT_ADAPTER,
            "request_id": request_id,
            "attempt_id": attempt_id,
            "return_id": return_id,
            "engine_job_id": engine_job_id,
            "envelope_digest": envelope_digest,
        }
        if marker_path.is_file():
            existing = _read_object(marker_path, "workflow ingest marker")
            if all(existing.get(key) == value for key, value in marker_identity.items()):
                return {
                    "outcome": "already-ingested",
                    "marker": marker_path.relative_to(self.root).as_posix(),
                    "engine_job_id": engine_job_id,
                }
            raise CannJudgeResultAdapterError(
                f"workflow ingest identity collision: {marker_path}"
            )

        operation_kind = str(
            workflow.get("operation_kind")
            or workflow.get("job_kind")
            or "operator-test"
        )
        command, ingest_outcome = self._ingest_command(
            route,
            workflow,
            operation_kind=operation_kind,
            request_id=request_id,
            attempt_id=attempt_id,
            return_id=return_id,
            engine_job_id=engine_job_id,
            source=source,
        )
        with NamedProcessLock(
            self.root,
            "cannjudge-v3-result-ingest",
            stale_after_seconds=300,
            wait_timeout_seconds=30,
        ):
            if marker_path.is_file():
                return self.ingest(
                    route,
                    returned,
                    projection_dir=projection,
                )
            completed = self.runner(
                command,
                cwd=self.root,
                text=True,
                encoding="utf-8",
                errors="replace",
                capture_output=True,
                stdin=subprocess.DEVNULL,
                env=workspace_process_environment(self.root),
                creationflags=process_creation_flags(),
                startupinfo=process_startupinfo(),
                check=False,
            )
            if completed.returncode != 0:
                detail = "\n".join(
                    part.strip()
                    for part in (completed.stdout or "", completed.stderr or "")
                    if part.strip()
                )
                raise CannJudgeResultAdapterError(
                    f"CANNJudge result ingest failed rc={completed.returncode}: "
                    f"{detail[-4000:]}"
                )
            marker = {
                **marker_identity,
                "envelope_path": envelope_path.relative_to(self.root).as_posix(),
                "bundle_root": source.relative_to(self.root).as_posix(),
                "operation_kind": operation_kind,
                "outcome": ingest_outcome,
                "stdout": (completed.stdout or "").strip(),
            }
            _write_json_atomic(marker_path, marker)
        return {
            "outcome": ingest_outcome,
            "marker": marker_path.relative_to(self.root).as_posix(),
            "engine_job_id": engine_job_id,
            "stdout": (completed.stdout or "").strip(),
        }

    def _ingest_command(
        self,
        route: Mapping[str, Any],
        workflow: Mapping[str, Any],
        *,
        operation_kind: str,
        request_id: str,
        attempt_id: str,
        return_id: str,
        engine_job_id: str,
        source: Path,
    ) -> tuple[list[str], str]:
        prefix = [
            sys.executable,
            str(resolve_workflow_adapter(self.root).path),
        ]
        identity = [
            str(workflow.get("operator") or ""),
            str(workflow.get("test_version") or ""),
            "--engine-job-id",
            engine_job_id,
            "--collect-request-id",
            return_id,
        ]
        common = [
            "--bundle-root",
            str(source),
            "--gitpartner-repo",
            "GitPartner",
        ]
        if operation_kind == "diagnostic-correctness-replay":
            diagnostic = _object(
                workflow.get("diagnostic"),
                "envelope.workflow.diagnostic",
            )
            request_state = _required_text(
                diagnostic.get("request_state_path"),
                "diagnostic.request_state_path",
            )
            return (
                prefix
                + ["engine-ingest-solver-diagnostic"]
                + identity
                + [
                    "--request-id",
                    request_id,
                    "--attempt-id",
                    attempt_id,
                    "--request-state",
                    request_state,
                ]
                + common,
                "solver-diagnostic-evidence-archived",
            )
        if operation_kind == "diagnostic-profile":
            profiler = _object(
                workflow.get("profiler"),
                "envelope.workflow.profiler",
            )
            return (
                prefix
                + ["engine-ingest-profiler-evidence"]
                + identity
                + common
                + [
                    "--case-version",
                    _required_text(
                        profiler.get("case_version"),
                        "profiler.case_version",
                    ),
                    "--blocker-generation",
                    _required_text(
                        profiler.get("blocker_generation"),
                        "profiler.blocker_generation",
                    ),
                    "--request-state",
                    _required_text(
                        profiler.get("request_state_path"),
                        "profiler.request_state_path",
                    ),
                ],
                "profiler-evidence-archived",
            )
        return (
            prefix
            + ["engine-ingest-result"]
            + identity
            + common
            + [
                "--season",
                str(workflow.get("season") or "S5-910b"),
                "--mode",
                str(workflow.get("mode") or "both"),
                "--vendor",
                str(workflow.get("vendor") or ""),
                "--hardware",
                self._workflow_hardware(route, workflow),
                "--case-version",
                str(workflow.get("case_version") or "unknown"),
                "--claimed-by",
                "tester-daemon-v3-result-adapter",
            ],
            "workflow-archived",
        )


    @staticmethod
    def _workflow_hardware(
        route: Mapping[str, Any],
        workflow: Mapping[str, Any],
    ) -> str:
        # TestUtils hardware is a workflow compatibility family, not the
        # endpoint's physical SoC label. Reusing the immutable request value
        # keeps result ingestion stable when a 910B4-family case is routed to
        # a 910B3 endpoint. The route SoC remains available in telemetry.
        requested = str(workflow.get("hardware") or "").strip()
        if requested:
            return requested
        target_soc = route.get("target_soc")
        if isinstance(target_soc, list):
            values = [str(item).strip() for item in target_soc if str(item).strip()]
            for value in values:
                if value.lower() not in {"ascend910b", "910b"}:
                    return value
            if values:
                return values[0]
        return "unknown"

    def _load_envelope(
        self,
        route: Mapping[str, Any],
        result: Mapping[str, Any],
        *,
        request_id: str,
        attempt_id: str,
    ) -> tuple[dict[str, Any], Path, str]:
        expected_digest = str(
            route.get("wire_envelope_digest")
            or result.get("wire_envelope_digest")
            or ""
        )
        if not expected_digest:
            raise CannJudgeResultAdapterError("return has no Wire V3 envelope digest")
        candidates: list[Path] = []
        route_path = str(route.get("wire_envelope_path") or "")
        if route_path:
            path = Path(route_path)
            if not path.is_absolute():
                path = self.root / path
            candidates.append(path.resolve())
        if not candidates:
            package_root = self.root / ".ascendop-work" / "flow-v3" / "packages"
            candidates.extend(
                sorted(
                    package_root.glob(
                        f"preparations/prep-{request_id}-*/{request_id}/"
                        "REQUEST_ENVELOPE.json"
                    )
                )
            )
        for path in candidates:
            try:
                _bounded(self.root, path)
                raw = _read_object(path, "Wire V3 envelope")
                validated = validate_envelope(raw)
                digest = canonical_digest(validated.envelope)
            except Exception:
                continue
            if digest != expected_digest:
                continue
            meta = validated.envelope["meta"]
            if (
                str(meta.get("request_id") or "") == request_id
                and str(meta.get("attempt_id") or "") == attempt_id
            ):
                return validated.envelope, path, digest
        raise LegacyWireV3EnvelopeMissing(
            f"immutable Wire V3 envelope not found for {request_id}/{attempt_id}"
        )

    def _result_source(
        self,
        result: Mapping[str, Any],
        projection: Path,
    ) -> Path:
        artifact_root = str(result.get("artifact_root") or "")
        if artifact_root:
            source = Path(artifact_root)
            if not source.is_absolute():
                source = self.root / source
            source = source.resolve()
            trusted = (
                self.root / ".ascendop-work" / "flow-v3" / "returns"
            ).resolve()
            if source.is_dir() and (source == trusted or trusted in source.parents):
                return source
        projected = projection / "artifacts"
        if not projected.is_dir():
            raise CannJudgeResultAdapterError(
                f"V3 result has no durable artifact bundle: {projection}"
            )
        request_id = projection.parent.name
        attempt_id = projection.name
        destination = (
            self.root
            / "TestUtils"
            / "tester_daemon"
            / "flow_v3"
            / "results"
            / request_id
            / attempt_id
            / "collect"
        )
        if destination.exists():
            if _tree_digest(destination) != _tree_digest(projected):
                raise CannJudgeResultAdapterError(
                    f"recovery result bundle collision: {destination}"
                )
            return destination
        destination.parent.mkdir(parents=True, exist_ok=True)
        staging = destination.parent / f".collect-{uuid.uuid4().hex[:8]}"
        try:
            shutil.copytree(projected, staging)
            os.replace(staging, destination)
        except Exception:
            shutil.rmtree(staging, ignore_errors=True)
            raise
        return destination


def _object(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise CannJudgeResultAdapterError(f"{field} must be an object")
    return value


def _safe_text(value: Any, field: str) -> str:
    text = str(value or "")
    if not text or any(character in text for character in ("/", "\\", "\x00")):
        raise CannJudgeResultAdapterError(f"unsafe or missing {field}: {text!r}")
    return text


def _required_text(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if not text or "\x00" in text:
        raise CannJudgeResultAdapterError(f"missing or invalid {field}")
    return text


def _bounded(root: Path, path: Path) -> None:
    root = root.resolve()
    path = path.resolve()
    if path != root and root not in path.parents:
        raise CannJudgeResultAdapterError(f"path escapes workspace: {path}")


def _read_object(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CannJudgeResultAdapterError(f"cannot read {label} {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise CannJudgeResultAdapterError(f"{label} must be an object: {path}")
    return value


def _write_json_atomic(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    staging = path.with_name(f".{path.name}.{uuid.uuid4().hex[:8]}.tmp")
    staging.write_text(
        json.dumps(dict(value), ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(staging, path)


def _tree_digest(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        if not path.is_file() or path.is_symlink():
            continue
        relative = path.relative_to(root).as_posix().encode("utf-8")
        digest.update(relative)
        digest.update(b"\0")
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    return digest.hexdigest()
