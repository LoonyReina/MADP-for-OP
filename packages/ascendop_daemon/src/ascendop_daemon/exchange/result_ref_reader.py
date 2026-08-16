from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Mapping

from ascendop_protocol.wire_v3 import canonical_digest
from ascendop_daemon.exchange.transport_contracts import parse_last_json_object
from ascendop_daemon.runtime.process_adapter import (
    process_creation_flags,
    process_startupinfo,
)


def query_result_ref_json(
    repo: Path,
    *,
    result_branch: str,
    control_branch: str,
    remote: str,
    output_subdir: str,
    result_templates: tuple[str, ...],
    environment: Mapping[str, str],
    timeout_seconds: int,
) -> dict[str, Any] | None:
    """Read one JSON result from an immutable GP result ref."""

    if not result_templates:
        return None
    command = [
        sys.executable,
        "-s",
        "-m",
        "limited_remote_partner.gateway.batch_result_query",
        "--repo",
        str(repo),
        "--result-branch",
        result_branch,
        "--control-branch",
        control_branch,
        "--remote",
        remote,
        "--output-subdir",
        output_subdir,
    ]
    for template in result_templates:
        command.extend(["--result-template", template])
    completed = subprocess.run(
        command,
        cwd=repo,
        env=dict(environment),
        text=True,
        encoding="utf-8",
        errors="replace",
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=max(30, int(timeout_seconds)),
        creationflags=process_creation_flags(),
        startupinfo=process_startupinfo(),
    )
    if completed.returncode != 0:
        return None
    try:
        observed = parse_last_json_object(completed.stdout or "")
    except ValueError:
        return None
    items = observed.get("items")
    if not isinstance(items, list) or len(items) != 1:
        return None
    item = items[0]
    if not isinstance(item, Mapping):
        return None
    result = item.get("result")
    return dict(result) if isinstance(result, Mapping) else None


def flow_v3_observation_templates(
    payload: Mapping[str, Any],
) -> tuple[str, ...]:
    engine_root = str(payload.get("engine_root") or "").replace("\\", "/")
    engine_root = engine_root.strip("/")
    parts = tuple(part for part in engine_root.split("/") if part)
    if (
        not parts
        or ":" in parts[0]
        or any(part in {".", ".."} for part in parts)
    ):
        return ()
    relative = (
        f"{'/'.join(parts)}/transport/{{request_id}}/"
        "flow_v3_observation.json"
    )
    return (f"client_output/{relative}", relative)


def unique_json(root: Path, name: str) -> Path | None:
    if not root.is_dir():
        return None
    matches: dict[str, Path] = {}
    for path in root.rglob(name):
        if not path.is_file():
            continue
        try:
            value = read_json_object(path)
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        matches[canonical_digest(value)] = path
    if len(matches) > 1:
        raise ValueError(f"conflicting returned {name} documents")
    return next(iter(matches.values())) if matches else None


def unique_result_directory(root: Path, name: str) -> Path | None:
    if not root.is_dir():
        return None
    matches: dict[str, Path] = {}
    for path in root.rglob(name):
        manifest = path / "RESULT_PAYLOAD.json"
        if path.is_dir() and manifest.is_file():
            value = read_json_object(manifest)
            matches[canonical_digest(value)] = path
    if len(matches) > 1:
        raise ValueError(f"conflicting returned {name} directories")
    return next(iter(matches.values())) if matches else None


def read_json_object(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(raw, dict):
        raise ValueError(f"JSON document is not an object: {path}")
    return raw
