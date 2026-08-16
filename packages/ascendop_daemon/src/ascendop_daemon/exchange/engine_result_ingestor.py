from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable

from ascendop_daemon.core.correlation import engine_correlation
from ascendop_daemon.core.tree_digest import tree_digest
from ascendop_daemon.runtime.process_adapter import (
    process_creation_flags,
    process_startupinfo,
    workspace_process_environment,
)
from ascendop_daemon.runtime.workflow_adapter import resolve_workflow_adapter
from ascendop_daemon.workflow.workflow_profiles import WorkflowProfileError, WorkflowProfileRegistry
from ascendop_daemon.workflow.workflow_result_router import (
    WorkflowResultIngestError,
    WorkflowResultRouter,
)


class EngineResultIngestError(RuntimeError):
    pass


class EngineResultIngestor:
    """Invoke the workflow harness for exactly-once RESULT/queue ingestion."""

    def __init__(
        self,
        root: Path,
        *,
        gitpartner_repo: Path,
        runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
    ) -> None:
        self.root = root.resolve()
        self.gitpartner_repo = gitpartner_repo.resolve()
        self.runner = runner or subprocess.run

    def ingest(self, record: dict[str, Any]) -> dict[str, Any]:
        job = engine_correlation(record)
        collect_request_id = str(record.get("collect_request_id") or "")
        if not collect_request_id:
            raise EngineResultIngestError(
                f"engine return has no collect request id: {job['engine_job_id']}"
            )
        spec_path = Path(str(record.get("spec_path") or ""))
        if not spec_path.is_absolute():
            spec_path = self.root / spec_path
        spec = read_object(spec_path)
        if engine_correlation(spec) != job:
            raise EngineResultIngestError(
                f"engine return/spec correlation mismatch: {job['engine_job_id']}"
            )
        wire_packet = spec.get("wire_packet")
        if isinstance(wire_packet, dict):
            return self._ingest_wire_v2(
                record,
                spec=spec,
                wire_packet=wire_packet,
                job=job,
                collect_request_id=collect_request_id,
            )
        workflow = spec.get("workflow")
        if not isinstance(workflow, dict):
            workflow = {}
        job_kind = str(workflow.get("job_kind") or "operator-test")
        if job_kind == "profiler-evidence":
            profiler = workflow.get("profiler")
            if not isinstance(profiler, dict):
                raise EngineResultIngestError(
                    f"profiler workflow metadata is missing: {job['engine_job_id']}"
                )
            superseded = self._superseded_profiler_attempt(job, profiler)
            if superseded is not None:
                return superseded
            command = [
                sys.executable,
                str(resolve_workflow_adapter(self.root).path),
                "engine-ingest-profiler-evidence",
                job["operator"],
                job["test_version"],
                "--engine-job-id",
                job["engine_job_id"],
                "--collect-request-id",
                collect_request_id,
                "--gitpartner-repo",
                str(self.gitpartner_repo),
                "--case-version",
                str(profiler.get("case_version") or ""),
                "--blocker-generation",
                str(profiler.get("blocker_generation") or ""),
                "--request-state",
                str(profiler.get("request_state_path") or ""),
            ]
        elif job_kind == "diagnostic-correctness-replay":
            diagnostic = workflow.get("diagnostic")
            if not isinstance(diagnostic, dict):
                raise EngineResultIngestError(
                    f"diagnostic workflow metadata is missing: {job['engine_job_id']}"
                )
            command = [
                sys.executable,
                str(resolve_workflow_adapter(self.root).path),
                "engine-ingest-solver-diagnostic",
                job["operator"],
                job["test_version"],
                "--engine-job-id",
                job["engine_job_id"],
                "--collect-request-id",
                collect_request_id,
                "--request-id",
                job["request_id"],
                "--attempt-id",
                job["attempt_id"],
                "--request-state",
                str(diagnostic.get("request_state_path") or ""),
            ]
        else:
            existing = self._verified_existing_workflow_result(job, spec, workflow)
            if existing is not None:
                return {
                    "engine_job_id": job["engine_job_id"],
                    "collect_request_id": collect_request_id,
                    "outcome": "superseded-by-workflow-result",
                    "existing_result_evidence": existing,
                    "stdout": "existing workflow RESULT is identity-equivalent",
                }
            command = [
                sys.executable,
                str(resolve_workflow_adapter(self.root).path),
                "engine-ingest-result",
                job["operator"],
                job["test_version"],
                "--engine-job-id",
                job["engine_job_id"],
                "--collect-request-id",
                collect_request_id,
                "--gitpartner-repo",
                str(self.gitpartner_repo),
                "--season",
                str(workflow.get("season") or "S5-910b"),
                "--mode",
                str(workflow.get("mode") or "both"),
                "--vendor",
                str(workflow.get("vendor") or ""),
                "--hardware",
                str(workflow.get("hardware") or "910B4"),
                "--case-version",
                str(workflow.get("case_version") or "unknown"),
                "--claimed-by",
                "tester-daemon-engine",
            ]
        bundle_root = resolve_snapshot_bundle_root(
            self.root,
            collect_request_id=collect_request_id,
            value=str(record.get("snapshot_bundle_root") or ""),
        )
        if bundle_root is not None:
            command.extend(["--bundle-root", str(bundle_root)])
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
            raise EngineResultIngestError(
                f"engine result ingest failed rc={completed.returncode}: {detail[-2000:]}"
            )
        return {
            "engine_job_id": job["engine_job_id"],
            "collect_request_id": collect_request_id,
            "outcome": (
                "profiler-evidence-archived"
                if job_kind == "profiler-evidence"
                else "solver-diagnostic-evidence-archived"
                if job_kind == "diagnostic-correctness-replay"
                else "workflow-archived"
            ),
            "stdout": (completed.stdout or "").strip(),
        }

    def _ingest_wire_v2(
        self,
        record: dict[str, Any],
        *,
        spec: dict[str, Any],
        wire_packet: dict[str, Any],
        job: dict[str, str],
        collect_request_id: str,
    ) -> dict[str, Any]:
        source_value = str(record.get("workflow_result_root") or "")
        if source_value:
            source_dir = Path(source_value)
            if not source_dir.is_absolute():
                source_dir = self.root / source_dir
        else:
            source_dir = resolve_snapshot_bundle_root(
                self.root,
                collect_request_id=collect_request_id,
                value=str(record.get("snapshot_bundle_root") or ""),
            )
        if source_dir is None:
            raise EngineResultIngestError(
                f"Wire V2 return has no result bundle: {job['engine_job_id']}"
            )
        capabilities = spec.get("consumer_capabilities", [])
        if not isinstance(capabilities, list):
            raise EngineResultIngestError(
                f"Wire V2 consumer_capabilities must be a list: "
                f"{job['engine_job_id']}"
            )
        supported_extensions = spec.get("supported_wire_extensions", [])
        if not isinstance(supported_extensions, list):
            raise EngineResultIngestError(
                f"Wire V2 supported_wire_extensions must be a list: "
                f"{job['engine_job_id']}"
            )
        registry = WorkflowProfileRegistry(self.root)
        try:
            registry.load_manifests()
            routed = WorkflowResultRouter(
                self.root,
                registry,
                capabilities=capabilities,
                supported_extensions=supported_extensions,
            ).ingest(
                wire_packet,
                source_dir=source_dir,
                terminal_state=str(record.get("terminal_state") or ""),
            )
        except (
            WorkflowProfileError,
            WorkflowResultIngestError,
        ) as exc:
            raise EngineResultIngestError(
                f"Wire V2 result ingest failed: {exc}"
            ) from exc
        return {
            "engine_job_id": job["engine_job_id"],
            "collect_request_id": collect_request_id,
            **routed,
        }

    def _superseded_profiler_attempt(
        self,
        job: dict[str, str],
        profiler: dict[str, Any],
    ) -> dict[str, Any] | None:
        request_state_value = str(profiler.get("request_state_path") or "")
        if not request_state_value:
            return None
        state_path = Path(request_state_value)
        if not state_path.is_absolute():
            state_path = self.root / state_path
        state_path = state_path.resolve()
        allowed = (
            self.root
            / "TestUtils"
            / "tester_daemon"
            / "profiler_requests"
        ).resolve()
        if state_path != allowed and allowed not in state_path.parents:
            return None
        try:
            state = read_object(state_path)
        except EngineResultIngestError:
            return None
        current_attempt = int(state.get("request_attempt", 1) or 1)
        job_attempt = int(profiler.get("request_attempt", 0) or 0)
        if job_attempt <= 0:
            matched = re.search(r"-a(\d+)-t\d+$", job["engine_job_id"])
            job_attempt = int(matched.group(1)) if matched else 0
        if not job_attempt or job_attempt >= current_attempt:
            return None
        return {
            "engine_job_id": job["engine_job_id"],
            "outcome": "superseded-by-logical-attempt",
            "superseded_request_attempt": job_attempt,
            "current_request_attempt": current_attempt,
            "stdout": (
                "profiler return belongs to an older logical request attempt: "
                f"{job_attempt} < {current_attempt}"
            ),
        }

    def _verified_existing_workflow_result(
        self,
        job: dict[str, str],
        spec: dict[str, Any],
        workflow: dict[str, Any],
    ) -> dict[str, Any] | None:
        result_dir = (
            self.root
            / "operators_testresult"
            / job["operator"]
            / job["test_version"]
        )
        result_path = result_dir / "RESULT.md"
        if not result_path.is_file():
            return None
        try:
            text = result_path.read_text(encoding="utf-8-sig")
        except OSError:
            return None
        if not text.startswith(f"# Result {job['operator']} {job['test_version']}\n"):
            return None
        fields = markdown_scalar_fields(text)
        required_fields = {
            "Hardware": str(workflow.get("hardware") or ""),
            "Season": str(workflow.get("season") or ""),
            "Case version": str(workflow.get("case_version") or ""),
            "Mode": str(workflow.get("mode") or ""),
        }
        if any(expected and fields.get(name) != expected for name, expected in required_fields.items()):
            return None
        if fields.get("Verdict") not in {
            "PASS",
            "FAIL",
            "FAIL_BUILD",
            "FAIL_CORRECTNESS",
            "FAIL_RUNTIME",
            "FAIL_PERF",
            "INFRA_FAIL",
            "NEEDS_CASEGEN",
        }:
            return None
        identity = spec.get("input_identity")
        if not isinstance(identity, dict):
            return None
        identity_paths = {
            "source_sha256": result_dir
            / "submit_snapshot"
            / "pending_snapshot"
            / "source_snapshot",
            "case_bundle_sha256": result_dir / "submit_snapshot" / "attack_case",
            "golden_bundle_sha256": result_dir / "submit_snapshot" / "task_case",
        }
        actual: dict[str, str] = {}
        for name, path in identity_paths.items():
            expected = str(identity.get(name) or "")
            if not expected or not path.is_dir():
                return None
            digest = tree_digest(path)
            if digest != expected:
                return None
            actual[name] = digest
        if str(identity.get("test_version") or "") != job["test_version"]:
            return None
        return {
            "status": "verified",
            "result_path": str(result_path.relative_to(self.root)).replace("\\", "/"),
            "identity": actual,
            "verdict": fields["Verdict"],
            "request_id": fields.get("Request id", ""),
        }


def resolve_snapshot_bundle_root(
    root: Path,
    *,
    collect_request_id: str,
    value: str,
) -> Path | None:
    if not value:
        return None
    path = Path(value)
    if path.is_absolute():
        return path
    if path.parts[:3] == ("TestUtils", "tester_daemon", "engine_ready_cache"):
        return root / path
    cache_key = hashlib.sha256(collect_request_id.encode("utf-8")).hexdigest()[:16]
    return root / "TestUtils" / "tester_daemon" / "engine_ready_cache" / cache_key / path


def read_object(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise EngineResultIngestError(f"cannot read engine spec: {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise EngineResultIngestError(f"engine spec must be an object: {path}")
    return raw


def markdown_scalar_fields(text: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for line in text.splitlines():
        if ":" not in line or line.startswith("-"):
            continue
        name, value = line.split(":", 1)
        name = name.strip()
        if name and name not in fields:
            fields[name] = value.strip().strip("`")
    return fields
