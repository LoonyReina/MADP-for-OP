from __future__ import annotations

import json
import os
import re
import tempfile
import time
from pathlib import Path
from typing import Any

from ascendop_daemon.registry.models import BackendEndpoint
from ascendop_daemon.registry.topology_parser import string_list
from ascendop_daemon.workflow.operator_job_builder import tree_digest


class TestRequestError(RuntimeError):
    pass


def pinned_endpoint_requirements(
    requirements: dict[str, Any],
    endpoint: BackendEndpoint,
) -> dict[str, Any]:
    """Bind environment identity while preserving task-level feature gates."""

    pinned = dict(requirements)
    pinned.update(
        {
            "allowed_endpoints": [endpoint.endpoint_id],
            "backend_pool": endpoint.backend_pool,
            "transport": endpoint.transport,
            "soc": string_list(endpoint.capabilities.get("soc")),
            "cann": string_list(endpoint.capabilities.get("cann")),
        }
    )
    operating_systems = string_list(endpoint.capabilities.get("operating_systems"))
    if operating_systems:
        pinned["operating_systems"] = operating_systems
    return pinned


def resolve_manifest_submit_root(root: Path, manifest: dict[str, Any]) -> Path:
    sources = manifest.get("payload_sources", {})
    identity = manifest.get("input_identity", {})
    if not isinstance(sources, dict) or not isinstance(identity, dict):
        raise TestRequestError("request payload source identity is malformed")
    relative = str(sources.get("submit_root") or "")
    if not relative:
        raise TestRequestError("request payload submit_root is missing")
    submit_root = (root / relative).resolve()
    ensure_bounded(root, submit_root)
    source = submit_root / "pending_snapshot" / "source_snapshot"
    task_case = submit_root / "task_case"
    attack_case = submit_root / "attack_case"
    observed = {
        "source_sha256": tree_digest(source) if source.is_dir() else "",
        "task_case_sha256": tree_digest(task_case) if task_case.is_dir() else "",
        "attack_case_sha256": tree_digest(attack_case) if attack_case.is_dir() else "",
    }
    expected = {key: str(identity.get(key) or "") for key in observed}
    if observed != expected:
        raise TestRequestError(
            "immutable request payload source changed before publication"
        )
    return submit_root


def persist_test_request(
    request_root: Path, manifest: dict[str, Any]
) -> tuple[Path, dict[str, Any]]:
    request_id = str(manifest["request_id"])
    destination = request_root / request_id / "TEST_REQUEST.json"
    if destination.is_file():
        existing = read_persisted_test_request(destination)
        validate_persisted_test_request(destination, existing, manifest)
        return destination, existing
    destination.parent.mkdir(parents=True, exist_ok=True)
    persisted = dict(manifest)
    payload = json.dumps(persisted, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    temporary_path = ""
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=destination.parent,
            prefix=".TEST_REQUEST.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary_path = handle.name
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary_path, destination)
        except FileExistsError:
            existing = read_persisted_test_request(destination)
            validate_persisted_test_request(destination, existing, manifest)
            return destination, existing
        except OSError:
            try:
                fd = os.open(destination, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                existing = read_persisted_test_request(destination)
                validate_persisted_test_request(destination, existing, manifest)
                return destination, existing
            with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
    finally:
        if temporary_path:
            Path(temporary_path).unlink(missing_ok=True)
    return destination, persisted


def read_persisted_test_request(
    path: Path,
    *,
    timeout_seconds: float = 2.0,
) -> dict[str, Any]:
    deadline = time.monotonic() + max(0.0, timeout_seconds)
    last_error: BaseException | None = None
    while True:
        try:
            value = json.loads(path.read_text(encoding="utf-8-sig"))
            if not isinstance(value, dict):
                raise TestRequestError(f"persisted TestRequest is not an object: {path}")
            return value
        except TestRequestError:
            raise
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            last_error = exc
            if time.monotonic() >= deadline:
                raise TestRequestError(
                    f"persisted TestRequest is not readable: {path}"
                ) from last_error
            time.sleep(0.01)


def validate_persisted_test_request(
    path: Path,
    existing: dict[str, Any],
    manifest: dict[str, Any],
) -> None:
    if (
        existing.get("request_digest") != manifest.get("request_digest")
        or canonical_without_created_at(existing)
        != canonical_without_created_at(manifest)
    ):
        raise TestRequestError(f"immutable TestRequest path collision: {path}")


def canonical_without_created_at(value: dict[str, Any]) -> str:
    copied = dict(value)
    copied.pop("created_at", None)
    return json.dumps(copied, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def reject_symlinks(root: Path) -> None:
    if root.is_symlink():
        raise TestRequestError(f"request payload cannot be a symlink: {root}")
    for path in root.rglob("*"):
        if path.is_symlink():
            raise TestRequestError(f"request payload cannot contain symlinks: {path}")


def ensure_bounded(root: Path, path: Path) -> None:
    if path != root and root not in path.parents:
        raise TestRequestError(f"request path escapes workspace: {path}")


def relative_path(root: Path, path: Path) -> str:
    ensure_bounded(root, path.resolve())
    return path.resolve().relative_to(root).as_posix()


def safe_token(value: str) -> str:
    result = re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._-").lower()
    if not result:
        raise TestRequestError(f"invalid request token: {value!r}")
    return result
