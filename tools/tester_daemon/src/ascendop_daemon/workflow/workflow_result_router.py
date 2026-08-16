from __future__ import annotations

import hashlib
import json
import os
import shutil
import uuid
from pathlib import Path
from typing import Any, Iterable

from ascendop_daemon.workflow.workflow_profiles import (
    WorkflowInstance,
    WorkflowProfileError,
    WorkflowProfileRegistry,
)
from ascendop_daemon.workflow.workflow_wire import (
    ValidatedPacket,
    WireProtocolError,
    canonical_digest,
    validate_packet,
    validate_relative_path,
)


class WorkflowResultIngestError(RuntimeError):
    pass


class WorkflowResultRouter:
    """Route Wire V2 results without sharing competition-specific archives."""

    def __init__(
        self,
        root: Path,
        registry: WorkflowProfileRegistry,
        *,
        capabilities: Iterable[str] = (),
        supported_extensions: Iterable[str] = (),
    ) -> None:
        self.root = root.resolve()
        self.registry = registry
        self.capabilities = tuple(capabilities)
        self.supported_extensions = tuple(supported_extensions)

    def ingest(
        self,
        packet: dict[str, Any],
        *,
        source_dir: Path,
        terminal_state: str,
    ) -> dict[str, Any]:
        try:
            validated = validate_packet(
                packet,
                capabilities=self.capabilities,
                supported_extensions=self.supported_extensions,
            )
        except WireProtocolError as exc:
            raise WorkflowResultIngestError(str(exc)) from exc
        profile = self.registry.get(
            validated.packet["workflow"]["profile_id"]
        )
        operation = validated.packet["operation"]
        contract = profile.operation_contract(
            operation["type"],
            operation["operation_version"],
        )
        declared_adapter = validated.packet["result_contract"]["ingest_adapter"]
        if declared_adapter != contract.ingest_adapter:
            raise WorkflowResultIngestError(
                f"result adapter mismatch: packet={declared_adapter} "
                f"profile={contract.ingest_adapter}"
            )
        if contract.external_side_effect:
            raise WorkflowResultIngestError(
                "external-side-effect operations cannot use Engine result ingest"
            )
        terminal_states = validated.packet["result_contract"]["terminal_states"]
        if terminal_state not in terminal_states:
            raise WorkflowResultIngestError(
                f"undeclared terminal state: {terminal_state}"
            )
        instance = find_instance(
            profile.discover(),
            validated.packet["workflow"]["instance_id"],
        )
        generation = validated.packet["workflow"]["state_generation"]
        snapshot = profile.read_snapshot(instance)
        if generation != snapshot.generation:
            raise WorkflowResultIngestError(
                f"stale workflow generation: packet={generation} "
                f"current={snapshot.generation}"
            )
        if declared_adapter == "community.local-result.v1":
            return self._ingest_community_result(
                validated,
                instance=instance,
                snapshot_context=snapshot.context,
                source_dir=source_dir,
                terminal_state=terminal_state,
            )
        raise WorkflowResultIngestError(
            f"Wire V2 result adapter is not enabled: {declared_adapter}"
        )

    def _ingest_community_result(
        self,
        validated: ValidatedPacket,
        *,
        instance: WorkflowInstance,
        snapshot_context: dict[str, Any],
        source_dir: Path,
        terminal_state: str,
    ) -> dict[str, Any]:
        local_test = snapshot_context.get("local_test")
        if not isinstance(local_test, dict):
            raise WorkflowResultIngestError(
                f"community task has no local_test context: {instance.instance_id}"
            )
        result_root_value = local_test.get("result_root")
        if not isinstance(result_root_value, str) or not result_root_value:
            raise WorkflowResultIngestError(
                f"community task has no result_root: {instance.instance_id}"
            )
        result_root = safe_result_root(self.root, result_root_value)
        source = source_dir.resolve()
        if not source.is_dir():
            raise WorkflowResultIngestError(f"result source is not a directory: {source}")
        required_artifacts = validated.packet["result_contract"][
            "required_artifacts"
        ]
        validate_required_artifacts(source, required_artifacts)
        result_digest, file_records = directory_digest(source)
        packet = validated.packet
        operation_key = packet["delivery"]["idempotency_key"]
        operation_slug = hashlib.sha256(operation_key.encode("utf-8")).hexdigest()[:24]
        destination = result_root / f"operation-{operation_slug}"
        manifest = {
            "schema": "ascendop.workflow-result-ingest.v1",
            "profile_id": instance.profile_id,
            "instance_id": instance.instance_id,
            "operation_type": packet["operation"]["type"],
            "operation_version": packet["operation"]["operation_version"],
            "idempotency_key": operation_key,
            "packet_digest": validated.digest,
            "payload_digest": packet["payload"]["digest"],
            "terminal_state": terminal_state,
            "result_digest": result_digest,
            "files": file_records,
        }
        if destination.exists():
            existing = read_manifest(destination / "INGEST_MANIFEST.json")
            identity_fields = (
                "profile_id",
                "instance_id",
                "operation_type",
                "operation_version",
                "idempotency_key",
                "payload_digest",
                "terminal_state",
                "result_digest",
            )
            if all(existing.get(key) == manifest.get(key) for key in identity_fields):
                return {
                    "outcome": "already-ingested",
                    "destination": relative_to_root(self.root, destination),
                    "result_digest": result_digest,
                }
            raise WorkflowResultIngestError(
                f"idempotency conflict at {destination}"
            )
        result_root.mkdir(parents=True, exist_ok=True)
        staging = result_root / f".ingest-{uuid.uuid4().hex[:12]}"
        try:
            copy_result_tree(source, staging)
            (staging / "INGEST_MANIFEST.json").write_text(
                json.dumps(
                    manifest,
                    ensure_ascii=True,
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            os.replace(staging, destination)
        except Exception:
            shutil.rmtree(staging, ignore_errors=True)
            raise
        return {
            "outcome": "ingested",
            "destination": relative_to_root(self.root, destination),
            "result_digest": result_digest,
        }


def find_instance(
    instances: tuple[WorkflowInstance, ...],
    instance_id: str,
) -> WorkflowInstance:
    matches = [item for item in instances if item.instance_id == instance_id]
    if len(matches) != 1:
        raise WorkflowResultIngestError(
            f"workflow instance not found or ambiguous: {instance_id}"
        )
    return matches[0]


def safe_result_root(root: Path, value: str) -> Path:
    normalized = value.replace("\\", "/")
    validate_relative_path(normalized)
    if normalized == "TestUtils" or normalized.startswith("TestUtils/"):
        raise WorkflowResultIngestError(
            "community results cannot be archived under TestUtils"
        )
    if normalized == "operators_testresult" or normalized.startswith(
        "operators_testresult/"
    ):
        raise WorkflowResultIngestError(
            "community results cannot be archived under operators_testresult"
        )
    candidate = (root / normalized).resolve()
    try:
        candidate.relative_to(root.resolve())
    except ValueError as exc:
        raise WorkflowResultIngestError(
            f"result root escapes repository: {value}"
        ) from exc
    return candidate


def validate_required_artifacts(source: Path, values: list[str]) -> None:
    for value in values:
        validate_relative_path(value)
        candidate = (source / value).resolve()
        try:
            candidate.relative_to(source)
        except ValueError as exc:
            raise WorkflowResultIngestError(
                f"required artifact escapes result source: {value}"
            ) from exc
        if not candidate.is_file():
            raise WorkflowResultIngestError(
                f"required result artifact is missing: {value}"
            )


def directory_digest(source: Path) -> tuple[str, list[dict[str, Any]]]:
    records: list[dict[str, Any]] = []
    for path in sorted(source.rglob("*"), key=lambda item: item.as_posix()):
        if path.is_symlink():
            raise WorkflowResultIngestError(
                f"result bundle must not contain symlinks: {path}"
            )
        if not path.is_file():
            continue
        relative = path.relative_to(source).as_posix()
        digest = file_sha256(path)
        records.append(
            {
                "path": relative,
                "size_bytes": path.stat().st_size,
                "sha256": digest,
            }
        )
    return canonical_digest(records), records


def copy_result_tree(source: Path, destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=False)
    for path in sorted(source.rglob("*"), key=lambda item: item.as_posix()):
        if path.is_symlink():
            raise WorkflowResultIngestError(
                f"result bundle must not contain symlinks: {path}"
            )
        relative = path.relative_to(source)
        target = destination / relative
        if path.is_dir():
            target.mkdir(parents=True, exist_ok=True)
        elif path.is_file():
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_manifest(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise WorkflowResultIngestError(
            f"cannot verify existing result manifest {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise WorkflowResultIngestError(
            f"existing result manifest must be an object: {path}"
        )
    return value


def relative_to_root(root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())
