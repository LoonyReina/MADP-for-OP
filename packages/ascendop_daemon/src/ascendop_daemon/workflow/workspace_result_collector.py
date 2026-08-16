from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import uuid
from pathlib import Path
from typing import Any, Mapping, Protocol

from ascendop_daemon.workflow.cannjudge_result_adapter import (
    LegacyWireV3EnvelopeMissing,
)
from ascendop_daemon.workflow.result_stage_evidence import (
    summarize_result_stage_evidence,
)


SAFE_ID = re.compile(r"^[A-Za-z0-9._-]+$")


class WorkspaceResultCollectorError(RuntimeError):
    pass


class WorkflowResultIngestor(Protocol):
    def ingest(
        self,
        route: Mapping[str, Any],
        returned: Mapping[str, Any],
        *,
        projection_dir: Path,
    ) -> dict[str, Any]: ...


class WorkspaceResultCollector:
    def __init__(
        self,
        root: Path,
        *,
        workflow_ingestor: WorkflowResultIngestor | None = None,
    ) -> None:
        self.root = root.resolve()
        self.workflow_ingestor = workflow_ingestor

    def collect(
        self,
        outbox: Mapping[str, Any],
        returned: Mapping[str, Any],
        *,
        ingest_workflow: bool = True,
    ) -> dict[str, Any]:
        route = _object(outbox.get("payload"), "outbox.payload")
        result = _object(returned.get("payload"), "return.payload")
        if route.get("workflow_ingest") is False:
            return {"state": "skipped-non-workflow"}
        request_id = _safe_id(route.get("request_id"), "request_id")
        attempt_id = _safe_id(route.get("attempt_id"), "attempt_id")
        origin = _relative_path(route.get("origin_workspace"), "origin_workspace")
        workspace = (self.root / origin).resolve()
        _bounded(self.root, workspace)
        base_destination = (
            workspace / ".ascendop" / "results" / request_id / attempt_id
        )
        result_digest = _canonical_digest(result)
        artifact_source: Path | None = None
        artifact_root = str(result.get("artifact_root") or "")
        if artifact_root:
            artifact_source = (
                self.root / _relative_path(artifact_root, "artifact_root")
            ).resolve()
            _bounded(self.root, artifact_source)
        stage_evidence = (
            summarize_result_stage_evidence(artifact_source)
            if artifact_source is not None and artifact_source.is_dir()
            else {}
        )
        hold_for_recovery = bool(
            stage_evidence.get("postprocess_recovery_required")
            and stage_evidence.get("postprocess_recovery_contract")
        )
        terminal_revision = int(result.get("terminal_revision", 0) or 0)
        destination = base_destination
        if base_destination.exists():
            existing_base = _read_json(base_destination / "PROJECTION.json")
            same_return = all(
                existing_base.get(key) == value
                for key, value in (
                    ("receipt_id", str(returned.get("receipt_id") or "")),
                    ("result_digest", result_digest),
                )
            )
            if not same_return:
                if terminal_revision <= 0:
                    raise WorkspaceResultCollectorError(
                        f"result projection identity collision: {base_destination}"
                    )
                revision_token = _safe_id(
                    returned.get("receipt_id"), "receipt_id"
                )[:16]
                destination = (
                    base_destination
                    / "revisions"
                    / f"r{terminal_revision:03d}-{revision_token}"
                )
        ingest_disposition = (
            "hold-for-postprocess-recovery"
            if hold_for_recovery
            else "ready"
            if ingest_workflow
            else "superseded-logical-result"
        )
        manifest = {
            "schema": "ascendop.workspace-result-projection.v1",
            "request_id": request_id,
            "attempt_id": attempt_id,
            "origin_workspace": origin,
            "return_id": str(returned.get("return_id") or ""),
            "receipt_id": str(returned.get("receipt_id") or ""),
            "result_digest": result_digest,
            "terminal_revision": terminal_revision,
            "stage_evidence": stage_evidence,
            "ingest_disposition": ingest_disposition,
            "route": _projection_route(route),
            "result": dict(result),
        }
        state = "projected"
        if destination.exists():
            existing = _read_json(destination / "PROJECTION.json")
            if not all(
                existing.get(key) == manifest.get(key)
                for key in (
                    "request_id",
                    "attempt_id",
                    "origin_workspace",
                    "receipt_id",
                    "result_digest",
                )
            ):
                raise WorkspaceResultCollectorError(
                    f"result projection identity collision: {destination}"
                )
            state = "already-projected"
            manifest = existing
        else:
            destination.parent.mkdir(parents=True, exist_ok=True)
            staging = destination.parent / f".p-{uuid.uuid4().hex[:8]}"
            try:
                staging.mkdir()
                if artifact_source is not None:
                    _copy_artifacts(artifact_source, staging / "artifacts")
                (staging / "PROJECTION.json").write_text(
                    json.dumps(manifest, ensure_ascii=True, indent=2, sort_keys=True)
                    + "\n",
                    encoding="utf-8",
                )
                os.replace(staging, destination)
            except Exception:
                shutil.rmtree(staging, ignore_errors=True)
                raise

        workflow_ingest = None
        if (
            self.workflow_ingestor is not None
            and not hold_for_recovery
            and ingest_workflow
        ):
            route_for_ingest = manifest.get("route")
            if not isinstance(route_for_ingest, Mapping):
                route_for_ingest = route
            workflow_ingest = self.workflow_ingestor.ingest(
                route_for_ingest,
                {
                    "return_id": manifest.get("return_id"),
                    "receipt_id": manifest.get("receipt_id"),
                    "payload": manifest.get("result"),
                },
                projection_dir=destination,
            )
        response = {
            "state": state,
            "destination": destination.relative_to(self.root).as_posix(),
            "result_digest": result_digest,
            "terminal_revision": terminal_revision,
            "stage_evidence": stage_evidence,
            "ingest_disposition": manifest.get("ingest_disposition", "ready"),
        }
        if workflow_ingest is not None:
            response["workflow_ingest"] = workflow_ingest
        return response

    def reconcile_pending(self, *, limit: int = 4) -> dict[str, Any]:
        report: dict[str, Any] = {
            "scanned": 0,
            "eligible": 0,
            "ingested": [],
            "legacy_skipped": [],
            "errors": [],
        }
        if self.workflow_ingestor is None or limit <= 0:
            return report

        candidates: dict[
            tuple[str, str, str, str],
            tuple[str, Path, dict[str, Any]],
        ] = {}
        patterns = (
            "operators_workspace/*/.ascendop/results/*/*/PROJECTION.json",
            "operators_workspace/*/.ascendop/results/*/*/revisions/*/PROJECTION.json",
        )
        projection_paths = sorted(
            {
                path
                for pattern in patterns
                for path in self.root.glob(pattern)
            }
        )
        for projection_path in projection_paths:
            report["scanned"] += 1
            try:
                manifest = _read_json(projection_path)
                result = _object(manifest.get("result"), "projection.result")
                if result.get("workflow_ingest") is False:
                    continue
                if manifest.get("ingest_disposition") == "superseded-logical-result":
                    continue
                if (
                    manifest.get("ingest_disposition")
                    == "hold-for-postprocess-recovery"
                    and not (projection_path.parent / "WORKFLOW_RELEASE.json").is_file()
                ):
                    continue
                spec = _read_json(projection_path.parent / "artifacts" / "spec.json")
                operator = str(spec.get("operator") or "")
                test_version = str(spec.get("test_version") or "")
                if not operator or not test_version:
                    continue
                workflow = spec.get("workflow")
                if not isinstance(workflow, Mapping):
                    workflow = {}
                operation_kind = str(
                    workflow.get("operation_kind")
                    or workflow.get("job_kind")
                    or "operator-test"
                )
                engine = result.get("engine")
                if not isinstance(engine, Mapping):
                    engine = {}
                order = "|".join(
                    (
                        str(engine.get("terminal_at") or ""),
                        str(manifest.get("attempt_id") or ""),
                    )
                )
                if operation_kind in {
                    "diagnostic-correctness-replay",
                    "diagnostic-profile",
                }:
                    key = (
                        operator,
                        test_version,
                        str(manifest.get("request_id") or ""),
                        str(manifest.get("attempt_id") or ""),
                    )
                else:
                    key = (operator, test_version, "", "")
                current = candidates.get(key)
                if current is None or order > current[0]:
                    candidates[key] = (order, projection_path, manifest)
            except WorkspaceResultCollectorError as exc:
                report["errors"].append(
                    {"projection": str(projection_path), "error": str(exc)}
                )
        report["eligible"] = len(candidates)

        for _order, projection_path, manifest in sorted(candidates.values())[:limit]:
            marker = projection_path.parent / "WORKFLOW_INGEST.json"
            if marker.is_file():
                continue
            route = manifest.get("route")
            if not isinstance(route, Mapping):
                result = _object(manifest.get("result"), "projection.result")
                route = {
                    "request_id": manifest.get("request_id"),
                    "attempt_id": manifest.get("attempt_id"),
                    "origin_workspace": manifest.get("origin_workspace"),
                    "workflow_ingest": True,
                    "wire_envelope_digest": result.get("wire_envelope_digest"),
                }
            returned = {
                "return_id": manifest.get("return_id"),
                "receipt_id": manifest.get("receipt_id"),
                "payload": manifest.get("result"),
            }
            try:
                outcome = self.workflow_ingestor.ingest(
                    route,
                    returned,
                    projection_dir=projection_path.parent,
                )
                report["ingested"].append(
                    {
                        "projection": projection_path.relative_to(
                            self.root
                        ).as_posix(),
                        "outcome": outcome,
                    }
                )
            except LegacyWireV3EnvelopeMissing as exc:
                report["legacy_skipped"].append(
                    {
                        "projection": projection_path.relative_to(
                            self.root
                        ).as_posix(),
                        "reason": str(exc),
                    }
                )
            except Exception as exc:
                report["errors"].append(
                    {
                        "projection": projection_path.relative_to(
                            self.root
                        ).as_posix(),
                        "error": str(exc),
                    }
                )
        return report

    def release_hold(
        self,
        projection_relative: str,
        *,
        reason: str,
    ) -> dict[str, Any]:
        projection = (self.root / projection_relative).resolve()
        _bounded(self.root, projection)
        manifest_path = projection / "PROJECTION.json"
        manifest = _read_json(manifest_path)
        if manifest.get("ingest_disposition") != "hold-for-postprocess-recovery":
            return {"state": "not-held", "projection": projection_relative}
        release_path = projection / "WORKFLOW_RELEASE.json"
        release = {
            "schema": "ascendop.workspace-result-release.v1",
            "request_id": manifest.get("request_id"),
            "attempt_id": manifest.get("attempt_id"),
            "return_id": manifest.get("return_id"),
            "reason": str(reason),
        }
        if release_path.is_file():
            existing = _read_json(release_path)
            if existing != release:
                raise WorkspaceResultCollectorError(
                    f"workflow release identity collision: {release_path}"
                )
        else:
            release_path.write_text(
                json.dumps(release, ensure_ascii=True, indent=2, sort_keys=True)
                + "\n",
                encoding="utf-8",
            )
        if self.workflow_ingestor is None:
            return {"state": "released", "projection": projection_relative}
        route = _object(manifest.get("route"), "projection.route")
        outcome = self.workflow_ingestor.ingest(
            route,
            {
                "return_id": manifest.get("return_id"),
                "receipt_id": manifest.get("receipt_id"),
                "payload": manifest.get("result"),
            },
            projection_dir=projection,
        )
        return {
            "state": "released-and-ingested",
            "projection": projection_relative,
            "workflow_ingest": outcome,
        }


def _copy_artifacts(source: Path, destination: Path) -> None:
    source_io = Path(_native_io_path(source))
    destination_io = Path(_native_io_path(destination))
    if not source_io.is_dir() or source_io.is_symlink():
        raise WorkspaceResultCollectorError(f"artifact root is not a directory: {source}")
    destination_io.mkdir()
    for path in sorted(source_io.rglob("*"), key=lambda item: item.as_posix()):
        if path.is_symlink():
            raise WorkspaceResultCollectorError(f"artifact bundle contains symlink: {path}")
        target = destination_io / path.relative_to(source_io)
        if path.is_dir():
            target.mkdir(parents=True, exist_ok=True)
        elif path.is_file():
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)


def _native_io_path(path: Path) -> str:
    absolute = os.path.abspath(os.fspath(path))
    if os.name != "nt" or absolute.startswith("\\\\?\\"):
        return absolute
    if absolute.startswith("\\\\"):
        return "\\\\?\\UNC\\" + absolute.lstrip("\\")
    return "\\\\?\\" + absolute


def _object(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise WorkspaceResultCollectorError(f"{field} must be an object")
    return value


def _safe_id(value: Any, field: str) -> str:
    text = str(value or "")
    if not SAFE_ID.fullmatch(text):
        raise WorkspaceResultCollectorError(f"unsafe {field}: {text!r}")
    return text


def _relative_path(value: Any, field: str) -> str:
    text = str(value or "").replace("\\", "/").strip("/")
    path = Path(text)
    if not text or path.is_absolute() or ".." in path.parts:
        raise WorkspaceResultCollectorError(f"unsafe {field}: {text!r}")
    return text


def _bounded(root: Path, path: Path) -> None:
    if path != root and root not in path.parents:
        raise WorkspaceResultCollectorError(f"path escapes workspace: {path}")


def _canonical_digest(value: Mapping[str, Any]) -> str:
    payload = json.dumps(
        dict(value),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _projection_route(route: Mapping[str, Any]) -> dict[str, Any]:
    fields = (
        "request_id",
        "attempt_id",
        "origin_workspace",
        "workflow_ingest",
        "wire_envelope_path",
        "wire_envelope_digest",
        "target_endpoint_id",
        "target_environment_id",
        "target_generation",
        "target_soc",
        "target_cann",
    )
    return {key: route[key] for key in fields if key in route}


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise WorkspaceResultCollectorError(f"cannot verify projection {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise WorkspaceResultCollectorError(f"projection manifest is invalid: {path}")
    return value
