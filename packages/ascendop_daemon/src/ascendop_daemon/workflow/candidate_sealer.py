from __future__ import annotations

import hashlib
import json
import os
import shutil
import uuid
from pathlib import Path
from typing import Any

from ascendop_protocol.competition import (
    canonical_tree_digest,
    lineage_tree_digest,
)

from ascendop_daemon.workflow.case_tree import (
    CASE_PACKAGE_EXCLUDED_NAMES,
    case_package_paths,
)


class CandidateSealError(ValueError):
    pass


class CandidateSealer:
    def __init__(self, root: Path, *, store: Path | None = None) -> None:
        self.root = root.resolve()
        self.store = (
            store.resolve()
            if store is not None
            else self.root / ".ascendop-work" / "artifacts" / "sha256"
        )

    def seal(self, raw_action: dict[str, Any]) -> dict[str, Any]:
        action = dict(raw_action)
        if action.get("action_kind") != "prepare_submit":
            return action
        inputs = self._inputs(action)
        source_root = inputs["source_snapshot"]
        case_root = inputs["case_package"]
        lineage = json.loads(inputs["source_lineage"].read_text(encoding="utf-8"))
        candidate = lineage.get("candidate", {}) if isinstance(lineage, dict) else {}
        declared_lineage_digest = str(candidate.get("sha256") or "")
        actual_lineage_digest = lineage_tree_digest(source_root)
        if len(declared_lineage_digest) != 64:
            raise CandidateSealError("SOURCE_LINEAGE candidate digest is missing")
        if declared_lineage_digest != actual_lineage_digest:
            raise CandidateSealError("SOURCE_LINEAGE candidate digest mismatch")
        entries = self._entries(inputs)
        manifest = {
            "schema": "ascendop.candidate-seal.v1",
            "campaign": action["campaign"],
            "operator": action["operator"],
            "test_version": action["test_version"],
            "entries": entries,
        }
        manifest_bytes = _canonical_bytes(manifest)
        bundle_digest = hashlib.sha256(manifest_bytes).hexdigest()
        destination = self.store / bundle_digest
        self._publish(destination, manifest_bytes, inputs)

        identity = dict(action.get("candidate_identity", {}))
        identity.update(
            {
                "execution_source_digest": canonical_tree_digest(source_root),
                "lineage_tree_digest": actual_lineage_digest,
                "case_package_digest": _case_package_digest(case_root),
                "task_profile_digest": _file_digest(inputs["task_execution_profile"]),
            }
        )
        action["candidate_identity"] = identity
        action["artifacts"] = [
            {
                "logical_name": "candidate_seal",
                "sha256": bundle_digest,
                "size": len(manifest_bytes),
                "media_type": "application/vnd.ascendop.candidate-seal+json",
                "uri": (destination / "MANIFEST.json").relative_to(self.root).as_posix(),
            }
        ]
        return action

    def _inputs(self, action: dict[str, Any]) -> dict[str, Path]:
        op = str(action["operator"])
        version = str(action["test_version"])
        pending = self.root / "TestUtils" / "pending" / op / version
        lineage_path = pending / "SOURCE_LINEAGE.json"
        if not lineage_path.is_file():
            raise CandidateSealError(f"missing candidate lineage: {lineage_path}")
        try:
            lineage = json.loads(lineage_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CandidateSealError(f"invalid candidate lineage: {exc}") from exc
        candidate = lineage.get("candidate", {}) if isinstance(lineage, dict) else {}
        source_value = str(candidate.get("path") or "")
        source_root = _under_root(self.root, source_value)
        options = action.get("arguments", {}).get("options", {})
        case_version = str(options.get("case_version") or "")
        if not case_version:
            raise CandidateSealError("prepare_submit action has no case_version")
        case_root = (
            self.root / "TestUtils" / "casegen" / op / "case" / case_version
        ).resolve()
        profile_uri = next(
            (
                str(artifact.get("uri") or "")
                for artifact in action.get("artifacts", [])
                if isinstance(artifact, dict)
                and artifact.get("logical_name") == "task_execution_profile"
            ),
            "",
        )
        if not profile_uri:
            raise CandidateSealError(
                "prepare_submit action has no task_execution_profile artifact"
            )
        profile = _under_root(self.root, profile_uri)
        for label, path, directory in (
            ("source_snapshot", source_root, True),
            ("case_package", case_root, True),
            ("task_execution_profile", profile, False),
        ):
            if directory and not path.is_dir():
                raise CandidateSealError(f"missing {label}: {path}")
            if not directory and not path.is_file():
                raise CandidateSealError(f"missing {label}: {path}")
        return {
            "source_snapshot": source_root,
            "case_package": case_root,
            "source_lineage": lineage_path.resolve(),
            "task_execution_profile": profile,
        }

    @staticmethod
    def _entries(inputs: dict[str, Path]) -> list[dict[str, Any]]:
        entries: list[dict[str, Any]] = []
        for logical_name, path in sorted(inputs.items()):
            files = (
                [
                    item
                    for item in (
                        case_package_paths(path)
                        if logical_name == "case_package"
                        else sorted(path.rglob("*"), key=lambda value: value.as_posix())
                    )
                    if item.is_file()
                ]
                if path.is_dir()
                else [path]
            )
            for item in files:
                relative = item.relative_to(path).as_posix() if path.is_dir() else item.name
                entries.append(
                    {
                        "logical_name": logical_name,
                        "path": relative,
                        "sha256": _file_digest(item),
                        "size": item.stat().st_size,
                    }
                )
        return entries

    def _publish(
        self,
        destination: Path,
        manifest_bytes: bytes,
        inputs: dict[str, Path],
    ) -> None:
        manifest_path = destination / "MANIFEST.json"
        if destination.exists():
            if not manifest_path.is_file() or manifest_path.read_bytes() != manifest_bytes:
                raise CandidateSealError("candidate artifact digest collision")
            return
        destination.parent.mkdir(parents=True, exist_ok=True)
        staging = destination.parent / f".s-{destination.name[:8]}-{uuid.uuid4().hex[:6]}"
        try:
            payload = staging / "payload"
            for logical_name, source in inputs.items():
                target = payload / logical_name
                if source.is_dir():
                    shutil.copytree(
                        source,
                        target,
                        ignore=(
                            shutil.ignore_patterns(*CASE_PACKAGE_EXCLUDED_NAMES)
                            if logical_name == "case_package"
                            else None
                        ),
                    )
                else:
                    target.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(source, target / source.name)
            staging.mkdir(parents=True, exist_ok=True)
            (staging / "MANIFEST.json").write_bytes(manifest_bytes)
            os.replace(staging, destination)
        except FileExistsError:
            if not manifest_path.is_file() or manifest_path.read_bytes() != manifest_bytes:
                raise CandidateSealError("candidate artifact concurrent publish collision")
        finally:
            if staging.exists():
                shutil.rmtree(staging, ignore_errors=True)


def _under_root(root: Path, value: str) -> Path:
    if not value:
        raise CandidateSealError("candidate source path is missing")
    path = Path(value)
    resolved = path.resolve() if path.is_absolute() else (root / path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise CandidateSealError("candidate source path escapes workspace") from exc
    return resolved


def _file_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _case_package_digest(root: Path) -> str:
    digest = hashlib.sha256()
    for path in case_package_paths(root):
        relative = path.relative_to(root).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        if path.is_file():
            carry = b""
            with path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    data = carry + chunk
                    carry = b"\r" if data.endswith(b"\r") else b""
                    if carry:
                        data = data[:-1]
                    digest.update(data.replace(b"\r\n", b"\n"))
            if carry:
                digest.update(carry)
            digest.update(b"\0")
    return digest.hexdigest()


def _canonical_bytes(value: dict[str, Any]) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")
