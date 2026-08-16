from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ActiveTransportRuntime:
    source: Path
    protocol_source: Path
    release_generation: str
    transport_generation: str


def load_active_transport_runtime(root: Path) -> ActiveTransportRuntime:
    root = root.resolve()
    active_path = root / ".ascendop-work" / "runtime" / "active-release.json"
    active = _read_object(active_path)
    if str(active.get("schema") or "") != "ascendop.active-release.v3":
        raise OSError("active Flow V3 release has an unsupported schema")
    source = Path(str(active.get("transport_source") or "")).resolve()
    runtime_root = (root / ".ascendop-work" / "runtime").resolve()
    if runtime_root not in source.parents:
        raise OSError("active transport source is outside the immutable runtime root")
    if not (source / "limited_remote_partner").is_dir():
        raise OSError(f"active transport package is missing: {source}")
    protocol_source = Path(str(active.get("protocol_source") or "")).resolve()
    if runtime_root not in protocol_source.parents:
        raise OSError("active protocol source is outside the immutable runtime root")
    if not (protocol_source / "ascendop_protocol").is_dir():
        raise OSError(f"active protocol package is missing: {protocol_source}")
    release_generation = _digest(active, "release_generation")
    transport_generation = _digest(active, "transport_generation")
    return ActiveTransportRuntime(
        source=source,
        protocol_source=protocol_source,
        release_generation=release_generation,
        transport_generation=transport_generation,
    )


def apply_transport_runtime_environment(
    environment: dict[str, str],
    runtime: ActiveTransportRuntime,
) -> None:
    import os

    existing = environment.get("PYTHONPATH", "")
    environment["PYTHONPATH"] = os.pathsep.join(
        item
        for item in (str(runtime.source), str(runtime.protocol_source), existing)
        if item
    )
    environment["GITPARTNER_RUNTIME_SOURCE"] = str(runtime.source)
    environment["GITPARTNER_PROTOCOL_SOURCE"] = str(runtime.protocol_source)
    environment["ASCENDOP_RELEASE_GENERATION"] = runtime.release_generation
    environment["GITPARTNER_TRANSPORT_GENERATION"] = runtime.transport_generation


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OSError(f"active Flow V3 release is unavailable: {path}") from exc
    if not isinstance(value, dict):
        raise OSError("active Flow V3 release must be a JSON object")
    return value


def _digest(value: dict[str, Any], field: str) -> str:
    digest = str(value.get(field) or "")
    if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
        raise OSError(f"active Flow V3 release has invalid {field}")
    return digest
