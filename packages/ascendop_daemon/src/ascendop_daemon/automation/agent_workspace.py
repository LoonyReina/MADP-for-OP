from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Mapping

from ascendop_daemon.core.filesystem import filesystem_path


SOURCE_SEAL_SCHEMA = "ascendop.agent-source-seal.v1"
PROMOTION_RECEIPT_SCHEMA = "ascendop.agent-source-promotion-receipt.v1"
IGNORED_NAMES = {
    "__pycache__",
    ".pytest_cache",
    ".git",
    "build",
    "dist",
    ".ascendop",
    ".ascendop-evidence",
    ".ascendop-output",
    "profiler_evidence",
}
EVIDENCE_DIRECTORY = ".ascendop-evidence"
EVIDENCE_MANIFEST_SCHEMA = "ascendop.agent-evidence-manifest.v1"
EVIDENCE_SUFFIX_POLICIES = {
    "ascendop.agent-workflow-evidence.v1": {
        ".csv",
        ".json",
        ".jsonl",
        ".log",
        ".md",
        ".tsv",
        ".txt",
    },
    "ascendop.agent-reference-evidence.v1": {
        ".c",
        ".cc",
        ".cmake",
        ".cpp",
        ".cxx",
        ".h",
        ".hpp",
        ".json",
        ".md",
        ".py",
        ".sh",
        ".txt",
    },
}


class AgentWorkspaceError(RuntimeError):
    pass


class AgentSourceIdentityChanged(AgentWorkspaceError):
    def __init__(self, expected: str, actual: str) -> None:
        self.expected = expected
        self.actual = actual
        super().__init__(
            "Agent source identity changed before staging: "
            f"expected={expected} actual={actual}"
        )


class AgentWorkspace:
    """Stages and promotes Agent edits without granting canonical write access."""

    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        self.runs_root = (self.root / ".ascendop-work" / "agent-runs").resolve()

    def stage(
        self,
        action: Mapping[str, Any],
        evidence: list[Mapping[str, Any]] | None = None,
    ) -> tuple[Path, Path, dict[str, str]]:
        action_id = _token(str(action["action_id"]), "action_id")
        origin = self._bounded_directory(str(action["origin_workspace"]))
        run_root = self.run_root(action_id)
        workspace = (run_root / "workspace").resolve()
        if workspace.parent != run_root:
            raise AgentWorkspaceError("Agent workspace escaped its action root")
        run_root.mkdir(parents=True, exist_ok=True)
        stage_path = run_root / "stage.json"
        if stage_path.is_file():
            if not workspace.is_dir() or workspace.is_symlink():
                raise AgentWorkspaceError("published Agent workspace is unavailable")
        else:
            if workspace.exists():
                if not workspace.is_dir() or workspace.is_symlink():
                    raise AgentWorkspaceError("unpublished Agent workspace is unsafe")
                shutil.rmtree(workspace)
            staging = Path(tempfile.mkdtemp(prefix=".workspace-", dir=run_root))
            try:
                shutil.copytree(
                    origin,
                    staging,
                    ignore=_ignore,
                    symlinks=False,
                    dirs_exist_ok=True,
                )
                os.replace(staging, workspace)
            finally:
                if staging.exists():
                    shutil.rmtree(staging)
        evidence_manifest = self._stage_evidence(workspace, evidence or [])
        before = self.snapshot(workspace)
        expected = str(
            action.get("candidate_identity", {}).get("execution_source_digest") or ""
        )
        actual = self.digest(workspace)
        if expected and expected != actual:
            raise AgentSourceIdentityChanged(expected, actual)
        self._write_json(
            stage_path,
            {
                "schema": "ascendop.agent-workspace-stage.v1",
                "action_id": action_id,
                "origin_workspace": str(action["origin_workspace"]),
                "workspace": workspace.relative_to(self.root).as_posix(),
                "source_before_digest": actual,
                "snapshot": before,
                "evidence_digest": str(evidence_manifest.get("manifest_digest") or ""),
                "created_at": _utc_now(),
            },
        )
        return run_root, workspace, before

    def seal(self, action: Mapping[str, Any]) -> dict[str, Any]:
        run_root = self.run_root(str(action["action_id"]))
        stage = self._read_object(run_root / "stage.json")
        workspace = self._bounded_directory(str(stage["workspace"]))
        self._validate_evidence(
            workspace,
            expected_digest=str(stage.get("evidence_digest") or ""),
        )
        before = dict(stage.get("snapshot", {}))
        if not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in before.items()
        ):
            raise AgentWorkspaceError("Agent stage snapshot is invalid")
        after = self.snapshot(workspace)
        changed = self.changed_paths(before, after)
        scopes = [str(item) for item in action.get("write_scope", [])]
        out_of_scope = self.out_of_scope_paths(changed, scopes)
        if out_of_scope:
            raise AgentWorkspaceError(
                "Agent changed paths outside its write scope: "
                + ", ".join(out_of_scope)
            )
        seal = {
            "schema": SOURCE_SEAL_SCHEMA,
            "action_id": str(action["action_id"]),
            "iteration_id": str(action["iteration_id"]),
            "operator_id": str(action["operator_id"]),
            "role": str(action["role"]),
            "candidate_version": str(action["candidate_version"]),
            "origin_workspace": str(action["origin_workspace"]),
            "workspace": str(stage["workspace"]),
            "source_before_digest": str(stage["source_before_digest"]),
            "source_after_digest": self.digest(workspace),
            "changed_paths": changed,
            "write_scope": scopes,
            "files": {path: after[path] for path in changed if path in after},
            "deleted_paths": [path for path in changed if path not in after],
            "created_at": _utc_now(),
        }
        self._write_json(run_root / "source-seal.json", seal)
        return seal

    def promote(self, seal_path: Path) -> dict[str, Any]:
        seal_path = seal_path.resolve()
        if self.root not in seal_path.parents:
            raise AgentWorkspaceError("Agent source seal is outside the workspace")
        seal = self._read_object(seal_path)
        if seal.get("schema") != SOURCE_SEAL_SCHEMA:
            raise AgentWorkspaceError("unsupported Agent source seal")
        action_id = _token(str(seal["action_id"]), "action_id")
        expected_path = self.run_root(action_id) / "source-seal.json"
        if seal_path != expected_path.resolve():
            raise AgentWorkspaceError(
                "Agent source seal path does not match its action"
            )
        origin = self._bounded_directory(str(seal["origin_workspace"]))
        workspace = self._bounded_directory(str(seal["workspace"]))
        if self.digest(workspace) != seal["source_after_digest"]:
            raise AgentWorkspaceError("sealed Agent workspace changed before promotion")
        changed = [str(item) for item in seal.get("changed_paths", [])]
        scopes = [str(item) for item in seal.get("write_scope", [])]
        out_of_scope = self.out_of_scope_paths(changed, scopes)
        if out_of_scope:
            raise AgentWorkspaceError("sealed Agent paths escaped the write scope")
        files = dict(seal.get("files", {}))
        deleted = set(str(item) for item in seal.get("deleted_paths", []))
        if set(files) & deleted or set(files) | deleted != set(changed):
            raise AgentWorkspaceError("sealed Agent change manifest is inconsistent")
        stage = self._read_object(self.run_root(action_id) / "stage.json")
        before = dict(stage.get("snapshot", {}))
        if (
            str(stage.get("source_before_digest") or "")
            != str(seal["source_before_digest"])
            or str(stage.get("origin_workspace") or "") != str(seal["origin_workspace"])
            or str(stage.get("workspace") or "") != str(seal["workspace"])
        ):
            raise AgentWorkspaceError("Agent source seal does not match its stage")
        if not all(
            isinstance(path, str) and isinstance(digest, str)
            for path, digest in before.items()
        ):
            raise AgentWorkspaceError("Agent stage snapshot is invalid")
        receipt_path = self.run_root(action_id) / "promotion-receipt.json"
        if receipt_path.is_file():
            receipt = self._read_object(receipt_path)
            self._validate_existing_receipt(receipt, seal, origin)
            return receipt
        self._validate_resumable_origin(
            before=before,
            current=self.snapshot(origin),
            files=files,
            deleted=deleted,
            changed=set(changed),
        )
        for relative in changed:
            _relative(relative, "changed_path")
            source = (workspace / relative).resolve()
            target = (origin / relative).resolve()
            if workspace not in source.parents or origin not in target.parents:
                raise AgentWorkspaceError("Agent promotion path escaped its workspace")
            if relative in deleted:
                if target.exists():
                    if not target.is_file() or target.is_symlink():
                        raise AgentWorkspaceError("Agent can delete only regular files")
                    target.unlink()
                continue
            if not source.is_file() or source.is_symlink():
                raise AgentWorkspaceError(
                    f"sealed Agent file is unavailable: {relative}"
                )
            digest = hashlib.sha256(source.read_bytes()).hexdigest()
            if files.get(relative) != digest:
                raise AgentWorkspaceError(
                    f"sealed Agent file digest changed: {relative}"
                )
            target.parent.mkdir(parents=True, exist_ok=True)
            self._replace_file(source, target)
        promoted_digest = self.digest(origin)
        if promoted_digest != seal["source_after_digest"]:
            raise AgentWorkspaceError(
                "canonical source digest does not match the promoted seal: "
                f"expected={seal['source_after_digest']} actual={promoted_digest}"
            )
        receipt = {
            "schema": PROMOTION_RECEIPT_SCHEMA,
            "action_id": action_id,
            "iteration_id": str(seal["iteration_id"]),
            "candidate_version": str(seal["candidate_version"]),
            "source_before_digest": str(seal["source_before_digest"]),
            "source_after_digest": promoted_digest,
            "changed_paths": changed,
            "promoted_at": _utc_now(),
        }
        self._write_json(receipt_path, receipt)
        return receipt

    def _stage_evidence(
        self,
        workspace: Path,
        descriptors: list[Mapping[str, Any]],
    ) -> dict[str, Any]:
        evidence_root = workspace / EVIDENCE_DIRECTORY
        manifest_path = evidence_root / "MANIFEST.json"
        if not descriptors:
            if manifest_path.is_file():
                return self._read_object(manifest_path)
            return {}

        normalized: list[dict[str, Any]] = []
        files: list[dict[str, Any]] = []
        for descriptor in descriptors:
            policy = EVIDENCE_SUFFIX_POLICIES.get(str(descriptor.get("schema") or ""))
            if policy is None:
                raise AgentWorkspaceError("unsupported Agent evidence descriptor")
            suffixes = {
                str(item).lower() for item in descriptor.get("include_suffixes", [])
            }
            if suffixes != policy:
                raise AgentWorkspaceError("Agent evidence suffix policy mismatch")
            source_root = self._bounded_directory(
                str(descriptor.get("root_path") or "")
            )
            rows: list[dict[str, Any]] = []
            total_bytes = 0
            include_paths = descriptor.get("include_paths", [])
            if not isinstance(include_paths, list) or not all(
                isinstance(item, str) and item for item in include_paths
            ):
                raise AgentWorkspaceError("Agent evidence include paths are invalid")
            if include_paths:
                candidates = [
                    (source_root / _relative(item, "include_path")).resolve()
                    for item in include_paths
                ]
            else:
                candidates = sorted(
                    source_root.rglob("*"), key=lambda item: item.as_posix()
                )
            for source in candidates:
                try:
                    relative = source.relative_to(source_root).as_posix()
                except ValueError as exc:
                    raise AgentWorkspaceError(
                        "Agent evidence path escaped its source root"
                    ) from exc
                if (
                    not source.is_file()
                    or source.suffix.lower() not in policy
                ):
                    if include_paths:
                        raise AgentWorkspaceError(
                            f"Agent evidence file is unavailable: {relative}"
                        )
                    continue
                payload = filesystem_path(source).read_bytes()
                digest = hashlib.sha256(payload).hexdigest()
                total_bytes += len(payload)
                blob = f"blobs/{digest}{source.suffix.lower()}"
                row = {
                    "source_path": source.relative_to(self.root).as_posix(),
                    "relative_path": relative,
                    "blob_path": blob,
                    "sha256": digest,
                    "size": len(payload),
                }
                rows.append(row)
                files.append(row)
            root_digest = _object_digest(
                [
                    {
                        "path": row["relative_path"],
                        "sha256": row["sha256"],
                        "size": row["size"],
                    }
                    for row in rows
                ]
            )
            expected_file_count = descriptor.get("file_count")
            expected_total_bytes = descriptor.get("total_bytes")
            if (
                root_digest != str(descriptor.get("root_digest") or "")
                or not isinstance(expected_file_count, int)
                or expected_file_count < 0
                or len(rows) != expected_file_count
                or not isinstance(expected_total_bytes, int)
                or expected_total_bytes < 0
                or total_bytes != expected_total_bytes
            ):
                raise AgentWorkspaceError(
                    "Agent evidence changed before staging"
                )
            index = self._bounded_file(str(descriptor.get("index_path") or ""))
            index_payload = index.read_bytes()
            index_digest = hashlib.sha256(index_payload).hexdigest()
            if index_digest != str(descriptor.get("index_sha256") or ""):
                raise AgentWorkspaceError("Agent evidence index changed before staging")
            index_blob = f"blobs/{index_digest}{index.suffix.lower()}"
            files.append(
                {
                    "source_path": index.relative_to(self.root).as_posix(),
                    "relative_path": index.name,
                    "blob_path": index_blob,
                    "sha256": index_digest,
                    "size": len(index_payload),
                    "kind": "index",
                }
            )
            normalized.append(dict(descriptor))

        unique_files = {
            (str(row["source_path"]), str(row["sha256"])): row for row in files
        }
        ordered_files = [unique_files[key] for key in sorted(unique_files)]
        manifest_core = {
            "schema": EVIDENCE_MANIFEST_SCHEMA,
            "descriptors": normalized,
            "files": ordered_files,
        }
        manifest = {
            **manifest_core,
            "manifest_digest": _object_digest(manifest_core),
        }
        if manifest_path.is_file():
            existing = self._read_object(manifest_path)
            if existing != manifest:
                raise AgentWorkspaceError("staged Agent evidence manifest changed")
            self._validate_evidence(
                workspace,
                expected_digest=str(manifest["manifest_digest"]),
            )
            return manifest

        evidence_root.mkdir(parents=True, exist_ok=True)
        for row in ordered_files:
            source = self._bounded_file(str(row["source_path"]))
            blob = evidence_root / str(row["blob_path"])
            filesystem_path(blob.parent).mkdir(parents=True, exist_ok=True)
            blob_io = filesystem_path(blob)
            if not blob_io.is_file():
                shutil.copyfile(filesystem_path(source), blob_io)
            if hashlib.sha256(blob_io.read_bytes()).hexdigest() != row["sha256"]:
                raise AgentWorkspaceError("staged Agent evidence blob digest mismatch")
        self._write_json(manifest_path, manifest)
        return manifest

    def _validate_evidence(self, workspace: Path, *, expected_digest: str) -> None:
        manifest_path = workspace / EVIDENCE_DIRECTORY / "MANIFEST.json"
        if not expected_digest:
            if manifest_path.exists():
                raise AgentWorkspaceError("unexpected Agent evidence manifest")
            return
        manifest = self._read_object(manifest_path)
        core = {
            "schema": manifest.get("schema"),
            "descriptors": manifest.get("descriptors"),
            "files": manifest.get("files"),
        }
        if (
            manifest.get("schema") != EVIDENCE_MANIFEST_SCHEMA
            or manifest.get("manifest_digest") != expected_digest
            or _object_digest(core) != expected_digest
        ):
            raise AgentWorkspaceError("Agent evidence manifest digest mismatch")
        evidence_root = workspace / EVIDENCE_DIRECTORY
        for row in manifest.get("files", []):
            if not isinstance(row, dict):
                raise AgentWorkspaceError("Agent evidence manifest row is invalid")
            blob = (evidence_root / str(row.get("blob_path") or "")).resolve()
            blob_io = filesystem_path(blob)
            if evidence_root.resolve() not in blob.parents or not blob_io.is_file():
                raise AgentWorkspaceError("Agent evidence blob is missing or unbounded")
            expected_size = row.get("size")
            actual_size = blob_io.stat().st_size
            expected_digest = str(row.get("sha256") or "")
            actual_digest = hashlib.sha256(blob_io.read_bytes()).hexdigest()
            if (
                not isinstance(expected_size, int)
                or expected_size < 0
                or actual_size != expected_size
                or actual_digest != expected_digest
            ):
                raise AgentWorkspaceError(
                    "Agent evidence blob changed during the turn: "
                    f"blob={row.get('blob_path')} "
                    f"expected_size={expected_size} actual_size={actual_size}"
                )

    def audit_legacy_zero_byte_evidence(self, action_id: str) -> dict[str, Any]:
        """Prove that a legacy evidence failure was the zero-size validation bug."""

        action_id = _token(action_id, "action_id")
        run_root = self.run_root(action_id)
        stage = self._read_object(run_root / "stage.json")
        if str(stage.get("action_id") or "") != action_id:
            raise AgentWorkspaceError("legacy evidence recovery action identity changed")
        workspace = self._bounded_directory(str(stage.get("workspace") or ""))
        expected_digest = str(stage.get("evidence_digest") or "")
        if not expected_digest:
            raise AgentWorkspaceError("legacy evidence recovery requires staged evidence")
        self._validate_evidence(workspace, expected_digest=expected_digest)
        manifest = self._read_object(
            workspace / EVIDENCE_DIRECTORY / "MANIFEST.json"
        )
        files = manifest.get("files", [])
        if not isinstance(files, list):
            raise AgentWorkspaceError("legacy evidence recovery manifest is invalid")
        zero_byte_files = [
            row
            for row in files
            if isinstance(row, dict) and row.get("size") == 0
        ]
        if not zero_byte_files:
            raise AgentWorkspaceError(
                "legacy evidence recovery requires a zero-byte evidence blob"
            )
        proof_core = {
            "schema": "ascendop.agent-evidence-recovery-proof.v1",
            "action_id": action_id,
            "manifest_digest": expected_digest,
            "file_count": len(files),
            "zero_byte_file_count": len(zero_byte_files),
        }
        return {**proof_core, "proof_digest": _object_digest(proof_core)}

    def _validate_existing_receipt(
        self,
        receipt: Mapping[str, Any],
        seal: Mapping[str, Any],
        origin: Path,
    ) -> None:
        expected = {
            "schema": PROMOTION_RECEIPT_SCHEMA,
            "action_id": str(seal["action_id"]),
            "iteration_id": str(seal["iteration_id"]),
            "candidate_version": str(seal["candidate_version"]),
            "source_before_digest": str(seal["source_before_digest"]),
            "source_after_digest": str(seal["source_after_digest"]),
            "changed_paths": [str(item) for item in seal.get("changed_paths", [])],
        }
        for field, value in expected.items():
            if receipt.get(field) != value:
                raise AgentWorkspaceError(
                    f"Agent promotion receipt conflicts with its seal: {field}"
                )
        if self.digest(origin) != seal["source_after_digest"]:
            raise AgentWorkspaceError(
                "canonical source changed after Agent promotion completed"
            )

    @staticmethod
    def _validate_resumable_origin(
        *,
        before: Mapping[str, str],
        current: Mapping[str, str],
        files: Mapping[str, Any],
        deleted: set[str],
        changed: set[str],
    ) -> None:
        for path in set(before) | set(current) | changed:
            current_digest = current.get(path)
            if path not in changed:
                if current_digest != before.get(path):
                    raise AgentWorkspaceError(
                        f"canonical source changed outside Agent promotion: {path}"
                    )
                continue
            after_digest = None if path in deleted else files.get(path)
            if current_digest not in {before.get(path), after_digest}:
                raise AgentWorkspaceError(
                    f"canonical source has an unknown intermediate value: {path}"
                )

    def run_root(self, action_id: str) -> Path:
        value = _token(action_id, "action_id")
        path = (self.runs_root / value).resolve()
        if path.parent != self.runs_root:
            raise AgentWorkspaceError("Agent action root escaped the runs directory")
        return path

    def digest(self, workspace: Path) -> str:
        digest = hashlib.sha256()
        for path in self._files(workspace):
            relative = path.relative_to(workspace).as_posix().encode("utf-8")
            payload = path.read_bytes()
            digest.update(len(relative).to_bytes(8, "big"))
            digest.update(relative)
            digest.update(len(payload).to_bytes(8, "big"))
            digest.update(payload)
        return digest.hexdigest()

    def snapshot(self, workspace: Path) -> dict[str, str]:
        return {
            path.relative_to(workspace).as_posix(): hashlib.sha256(
                path.read_bytes()
            ).hexdigest()
            for path in self._files(workspace)
        }

    @staticmethod
    def changed_paths(before: Mapping[str, str], after: Mapping[str, str]) -> list[str]:
        return sorted(
            path
            for path in set(before) | set(after)
            if before.get(path) != after.get(path)
        )

    @staticmethod
    def out_of_scope_paths(paths: list[str], scopes: list[str]) -> list[str]:
        return [path for path in paths if not _path_allowed(path, scopes)]

    def _files(self, workspace: Path) -> list[Path]:
        workspace = workspace.resolve()
        result: list[Path] = []
        for path in sorted(workspace.rglob("*"), key=lambda item: item.as_posix()):
            if any(part in IGNORED_NAMES for part in path.relative_to(workspace).parts):
                continue
            if path.is_symlink():
                raise AgentWorkspaceError(
                    f"symlink is not allowed in Agent workspace: {path}"
                )
            if path.is_file() and not path.name.endswith(".pyc"):
                result.append(path)
        return result

    def _bounded_directory(self, relative: str) -> Path:
        _relative(relative, "workspace")
        path = (self.root / relative).resolve()
        if path != self.root and self.root not in path.parents:
            raise AgentWorkspaceError("Agent path escaped the workspace root")
        if not path.is_dir():
            raise AgentWorkspaceError(f"Agent workspace is missing: {relative}")
        return path

    def _bounded_file(self, relative: str) -> Path:
        path = (self.root / relative).resolve()
        if self.root not in path.parents or not path.is_file() or path.is_symlink():
            raise AgentWorkspaceError(
                f"Agent evidence file is missing or unsafe: {relative}"
            )
        return path

    @staticmethod
    def _write_json(path: Path, value: Mapping[str, Any]) -> None:
        parent = filesystem_path(path.parent)
        target = filesystem_path(path)
        parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=parent, delete=False
        ) as handle:
            handle.write(payload)
            temporary = Path(handle.name)
        os.replace(filesystem_path(temporary), target)

    @staticmethod
    def _read_object(path: Path) -> dict[str, Any]:
        try:
            value = json.loads(filesystem_path(path).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise AgentWorkspaceError(
                f"invalid Agent workspace artifact: {path}"
            ) from exc
        if not isinstance(value, dict):
            raise AgentWorkspaceError(
                f"Agent workspace artifact must be an object: {path}"
            )
        return value

    @staticmethod
    def _replace_file(source: Path, target: Path) -> None:
        with tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as handle:
            temporary = Path(handle.name)
        try:
            shutil.copy2(source, temporary)
            os.replace(temporary, target)
        finally:
            if temporary.exists():
                temporary.unlink()


def _ignore(_directory: str, names: list[str]) -> set[str]:
    return {name for name in names if name in IGNORED_NAMES or name.endswith(".pyc")}


def _path_allowed(path: str, scopes: list[str]) -> bool:
    candidate = PurePosixPath(path)
    for raw_scope in scopes:
        scope = raw_scope.strip().replace("\\", "/").rstrip("/")
        if not scope:
            continue
        if path == scope or path.startswith(scope + "/"):
            return True
        if any(marker in scope for marker in "*?[") and candidate.match(scope):
            return True
    return False


def _token(value: str, field: str) -> str:
    if not value or any(
        ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
        for ch in value
    ):
        raise AgentWorkspaceError(f"{field} must be a safe token")
    return value


def _relative(value: str, field: str) -> str:
    normalized = value.strip().replace("\\", "/")
    if (
        not normalized
        or normalized.startswith("/")
        or any(part in {"", ".", ".."} for part in normalized.split("/"))
    ):
        raise AgentWorkspaceError(f"{field} must be a bounded relative path")
    return normalized


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _object_digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
