from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from types import ModuleType
from typing import Any


WORKFLOW_ADAPTER_ENV = "ASCENDOP_WORKFLOW_ADAPTER_PATH"
WORKFLOW_ADAPTER_DIGEST_ENV = "ASCENDOP_WORKFLOW_ADAPTER_SHA256"
_MODULE_LOAD_LOCK = Lock()


class WorkflowAdapterIdentityError(RuntimeError):
    pass


@dataclass(frozen=True)
class WorkflowAdapterRuntime:
    path: Path
    sha256: str


def resolve_workflow_adapter(root: Path) -> WorkflowAdapterRuntime:
    root = root.resolve()
    configured_path = os.environ.get(WORKFLOW_ADAPTER_ENV, "").strip()
    configured_digest = os.environ.get(WORKFLOW_ADAPTER_DIGEST_ENV, "").strip()
    if configured_path or configured_digest:
        if not configured_path or not configured_digest:
            raise WorkflowAdapterIdentityError(
                "workflow adapter environment identity is incomplete"
            )
        path = Path(configured_path).resolve()
        expected = _digest(configured_digest, "workflow adapter SHA-256")
    else:
        active_path = root / ".ascendop-work" / "runtime" / "active-release.json"
        active = _read_object(active_path)
        if str(active.get("schema") or "") != "ascendop.active-release.v4":
            raise WorkflowAdapterIdentityError(
                "active Flow V4 release has an unsupported schema"
            )
        path = Path(str(active.get("workflow_adapter_path") or "")).resolve()
        expected = _digest(
            str(active.get("workflow_adapter_sha256") or ""),
            "workflow adapter SHA-256",
        )
    runtime_root = (root / ".ascendop-work" / "runtime").resolve()
    if path == runtime_root or runtime_root not in path.parents:
        raise WorkflowAdapterIdentityError(
            f"workflow adapter is outside the immutable runtime root: {path}"
        )
    if not path.is_file() or path.is_symlink():
        raise WorkflowAdapterIdentityError(f"workflow adapter is missing: {path}")
    actual = _file_digest(path)
    if actual != expected:
        raise WorkflowAdapterIdentityError(
            "workflow adapter digest mismatch: "
            f"expected={expected} actual={actual}"
        )
    return WorkflowAdapterRuntime(path=path, sha256=actual)


def load_workflow_adapter_module(root: Path) -> ModuleType:
    runtime = resolve_workflow_adapter(root)
    path_digest = hashlib.sha256(str(runtime.path).encode("utf-8")).hexdigest()
    module_name = (
        f"ascendop_workflow_adapter_{runtime.sha256[:16]}_{path_digest[:8]}"
    )
    with _MODULE_LOAD_LOCK:
        loaded = sys.modules.get(module_name)
        if loaded is not None:
            return loaded
        spec = importlib.util.spec_from_file_location(module_name, runtime.path)
        if spec is None or spec.loader is None:
            raise WorkflowAdapterIdentityError(
                f"cannot load workflow adapter module: {runtime.path}"
            )
        module = importlib.util.module_from_spec(spec)
        # Dataclass and annotation resolution require the module to be visible
        # while its body is executing. The lock also prevents two season-board
        # readers from racing on the same immutable adapter.
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)
        except BaseException:
            if sys.modules.get(module_name) is module:
                sys.modules.pop(module_name, None)
            raise
        return module


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise WorkflowAdapterIdentityError(
            f"active Flow V4 release is unavailable: {path}"
        ) from exc
    if not isinstance(value, dict):
        raise WorkflowAdapterIdentityError(
            "active Flow V4 release must be a JSON object"
        )
    return value


def _digest(value: str, label: str) -> str:
    if len(value) != 64 or any(ch not in "0123456789abcdef" for ch in value):
        raise WorkflowAdapterIdentityError(f"invalid {label}: {value}")
    return value


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
