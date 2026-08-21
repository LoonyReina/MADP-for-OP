from __future__ import annotations

import fnmatch
import hashlib
import json
import os
import tarfile
from pathlib import Path
from typing import Any, Iterable


def file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def daemon_files(root: Path) -> Iterable[Path]:
    for name in (
        "pyproject.toml",
        "daemon.py",
        "launch_s5_910b.py",
        "launcher_bootstrap.py",
        "manage_s5_910b.ps1",
    ):
        yield root / name
    package = root / "src" / "ascendop_daemon"
    yield from (
        path
        for path in sorted(package.rglob("*"), key=lambda item: item.as_posix())
        if path.is_file()
        and not path.is_symlink()
        and "__pycache__" not in path.parts
        and path.suffix not in {".pyc", ".pyo"}
    )


def variable_default_from_path(
    path: Path,
    *,
    variable_id: str,
    fallback: int,
) -> int:
    registry = read_json(path) if path.is_file() else {}
    variables = registry.get("variables")
    if not isinstance(variables, list):
        return fallback
    matches = [
        row
        for row in variables
        if isinstance(row, dict) and str(row.get("id") or "") == variable_id
    ]
    if len(matches) != 1:
        return fallback
    try:
        value = int(matches[0]["default"])
    except (KeyError, TypeError, ValueError):
        return fallback
    return value if value > 0 else fallback


def safe_extract(archive: Path, destination: Path) -> None:
    destination = destination.resolve()
    with tarfile.open(archive, "r:gz") as handle:
        members = handle.getmembers()
        for member in members:
            target = (destination / member.name).resolve()
            if target != destination and destination not in target.parents:
                raise ValueError(f"daemon archive escapes destination: {member.name}")
            if member.issym() or member.islnk():
                raise ValueError(f"daemon archive contains a link: {member.name}")
            if member.isfile():
                target.parent.mkdir(parents=True, exist_ok=True)
        handle.extractall(destination, members=members)


def write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def validate_variable_registry_schema(path: Path, *, expected: int) -> None:
    value = read_json(path)
    variables = value.get("variables")
    if not isinstance(variables, list):
        raise ValueError("variable registry has no variables array")
    matches = [
        item
        for item in variables
        if isinstance(item, dict)
        and str(item.get("id") or "") == "database.control_schema"
    ]
    if len(matches) != 1:
        raise ValueError(
            "variable registry must define database.control_schema exactly once"
        )
    definition = matches[0]
    observed = {
        int(definition.get(field) or 0)
        for field in ("default", "minimum", "maximum")
    }
    if observed != {expected}:
        raise ValueError(
            "variable registry control schema mismatch: "
            f"expected={expected} observed={sorted(observed)}"
        )


def validate_change_impact_policy_schema(path: Path) -> dict[str, Any]:
    value = read_json(path)
    if value.get("schema") != "ascendop.change-impact-policy.v1":
        raise ValueError("unsupported change-impact policy schema")
    components = value.get("components")
    if not isinstance(components, list) or not components:
        raise ValueError("change-impact policy has no components")
    daemon_offline_patterns(value)
    if any(
        not isinstance(component, dict)
        or not str(component.get("import_domain") or "")
        for component in components
    ):
        raise ValueError("change-impact component has no import domain")
    return value


def require_no_offline_daemon_files(
    root: Path,
    patterns: Iterable[str],
) -> None:
    normalized_patterns = tuple(patterns)
    forbidden = [
        path.relative_to(root).as_posix()
        for path in sorted(root.rglob("*"), key=lambda item: item.as_posix())
        if path.is_file()
        and not path.is_symlink()
        and any(
            fnmatch.fnmatchcase(path.relative_to(root).as_posix(), pattern)
            for pattern in normalized_patterns
        )
    ]
    if forbidden:
        raise ValueError(
            "online daemon payload contains offline/legacy files: "
            + ", ".join(forbidden)
        )


def daemon_offline_patterns(policy: dict[str, Any]) -> tuple[str, ...]:
    raw = policy.get("offline_patterns")
    if not isinstance(raw, list) or any(
        not isinstance(item, str) or not item for item in raw
    ):
        raise ValueError("change-impact offline patterns are invalid")
    prefix = "tools/tester_daemon/"
    return tuple(
        sorted({item[len(prefix) :] for item in raw if item.startswith(prefix)})
    )


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}
