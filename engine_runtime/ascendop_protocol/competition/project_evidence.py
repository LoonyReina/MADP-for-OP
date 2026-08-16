from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Mapping, Sequence

from .contracts import validate_official_problem_snapshot


@dataclass(frozen=True)
class ProjectEvidence:
    project_digest: str
    source_file_digests: Mapping[str, str]
    source_file_sizes: Mapping[str, int]
    mapping_complete: bool
    opdef_parity: bool
    missing_files: tuple[str, ...]
    extra_files: tuple[str, ...]
    mismatched_opdef_files: tuple[str, ...]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_tree_digest(root: Path) -> str:
    """Return the transport-stable source identity used by Wire V3."""
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
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


def project_digest(files: Sequence[Mapping[str, Any]]) -> str:
    digest = hashlib.sha256()
    for item in sorted(files, key=lambda value: str(value["path"])):
        digest.update(str(item["path"]).encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(item["sha256"]).encode("ascii"))
        digest.update(b"\0")
        digest.update(str(item["size"]).encode("ascii"))
        digest.update(b"\n")
    return digest.hexdigest()


def collect_project_evidence(
    project_root: Path,
    official_snapshot: Mapping[str, Any],
    *,
    opdef_paths: Sequence[str] | None = None,
) -> ProjectEvidence:
    snapshot = validate_official_problem_snapshot(official_snapshot)
    root = project_root.resolve()
    declared = {
        _bounded_path(str(item["path"])): dict(item)
        for item in snapshot["project"]["files"]
    }
    actual_paths: set[str] = set()
    digests: dict[str, str] = {}
    sizes: dict[str, int] = {}
    if root.is_dir():
        for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
            if not path.is_file():
                continue
            relative = path.relative_to(root).as_posix()
            actual_paths.add(relative)
            if path.is_symlink() or relative not in declared:
                continue
            digests[relative] = sha256_file(path)
            sizes[relative] = path.stat().st_size

    declared_paths = set(declared)
    missing = tuple(sorted(declared_paths - actual_paths))
    extra = tuple(sorted(actual_paths - declared_paths))
    # The immutable candidate directory may carry local runbooks or audit
    # sidecars. Submission mapping is the official file allowlist, so extras
    # are reported but never included in the project digest or upload map.
    mapping_complete = not missing and set(digests) == declared_paths
    manifest = [
        {"path": path, "sha256": digests[path], "size": sizes[path]}
        for path in sorted(digests)
    ]
    candidate_digest = project_digest(manifest) if mapping_complete else ""

    selected_opdef_paths = tuple(
        _bounded_path(path)
        for path in (
            opdef_paths
            if opdef_paths is not None
            else [
                path
                for path in declared
                if path.startswith("op_host/") and path.endswith("_def.cpp")
            ]
        )
    )
    mismatched_opdef = tuple(
        sorted(
            path
            for path in selected_opdef_paths
            if path not in declared
            or digests.get(path) != str(declared[path]["sha256"])
        )
    )
    opdef_parity = bool(selected_opdef_paths) and not mismatched_opdef
    return ProjectEvidence(
        project_digest=candidate_digest,
        source_file_digests=digests,
        source_file_sizes=sizes,
        mapping_complete=mapping_complete,
        opdef_parity=opdef_parity,
        missing_files=missing,
        extra_files=extra,
        mismatched_opdef_files=mismatched_opdef,
    )


def _bounded_path(value: str) -> str:
    normalized = value.replace("\\", "/")
    path = PurePosixPath(normalized)
    if not normalized or path.is_absolute() or ".." in path.parts:
        raise ValueError(f"project path must be bounded and relative: {value!r}")
    return str(path)
