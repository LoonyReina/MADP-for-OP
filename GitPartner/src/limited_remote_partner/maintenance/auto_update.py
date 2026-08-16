from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

from limited_remote_partner.core.config import AppConfig


PENDING_TRIGGER_REF_ENV = "LIMITED_REMOTE_PARTNER_REEXEC_TRIGGER_REF"
PENDING_PREVIOUS_REF_ENV = "LIMITED_REMOTE_PARTNER_REEXEC_PREVIOUS_REF"
PENDING_CHANGED_PATHS_ENV = "LIMITED_REMOTE_PARTNER_REEXEC_CHANGED_PATHS"
PENDING_CHANGED_PATHS_FILE_ENV = (
    "LIMITED_REMOTE_PARTNER_REEXEC_CHANGED_PATHS_FILE"
)
LOCAL_WATCH_TRIGGER_REF = "local-watch"
MAX_INLINE_CHANGED_PATHS_BYTES = 32 * 1024


@dataclass(frozen=True)
class PendingReexec:
    trigger_ref: str
    previous_ref: str
    changed_paths: tuple[str, ...]


def should_reexec_for_update(config: AppConfig, changed_paths: list[str]) -> bool:
    if not config.auto_update.enabled or config.auto_update.mode != "reexec":
        return False
    return any(
        _path_matches(path, watched)
        for path in changed_paths
        for watched in config.auto_update.watch_paths
    )


def consume_pending_reexec() -> PendingReexec | None:
    trigger_ref = os.environ.pop(PENDING_TRIGGER_REF_ENV, "")
    previous_ref = os.environ.pop(PENDING_PREVIOUS_REF_ENV, "")
    changed_paths_file = os.environ.pop(PENDING_CHANGED_PATHS_FILE_ENV, "")
    raw_paths = os.environ.pop(PENDING_CHANGED_PATHS_ENV, "[]")
    if changed_paths_file:
        path = Path(changed_paths_file)
        try:
            raw_paths = path.read_text(encoding="utf-8")
        except OSError:
            raw_paths = "[]"
        finally:
            try:
                path.unlink()
            except OSError:
                pass
    if not trigger_ref:
        return None
    try:
        loaded_paths = json.loads(raw_paths)
    except json.JSONDecodeError:
        loaded_paths = []
    if not isinstance(loaded_paths, list):
        loaded_paths = []
    return PendingReexec(
        trigger_ref=trigger_ref,
        previous_ref=previous_ref,
        changed_paths=tuple(str(path) for path in loaded_paths),
    )


def reexec_self(
    module_name: str,
    config_path: Path,
    trigger_ref: str,
    previous_ref: str,
    changed_paths: list[str],
    *,
    extra_args: tuple[str, ...] = (),
) -> None:
    os.environ[PENDING_TRIGGER_REF_ENV] = trigger_ref
    os.environ[PENDING_PREVIOUS_REF_ENV] = previous_ref
    changed_paths_payload = json.dumps(changed_paths)
    os.environ.pop(PENDING_CHANGED_PATHS_FILE_ENV, None)
    if len(changed_paths_payload.encode("utf-8")) > MAX_INLINE_CHANGED_PATHS_BYTES:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            prefix="gitpartner-reexec-paths-",
            suffix=".json",
            delete=False,
        ) as handle:
            handle.write(changed_paths_payload)
            changed_paths_file = handle.name
        try:
            os.chmod(changed_paths_file, 0o600)
        except OSError:
            pass
        os.environ[PENDING_CHANGED_PATHS_ENV] = "[]"
        os.environ[PENDING_CHANGED_PATHS_FILE_ENV] = changed_paths_file
    else:
        os.environ[PENDING_CHANGED_PATHS_ENV] = changed_paths_payload
    argv = [
        sys.executable,
        "-m",
        module_name,
        "--config",
        str(config_path),
        *extra_args,
    ]
    os.execv(sys.executable, argv)


def reexec_partner_role(
    config: AppConfig,
    config_path: Path,
    role: str,
    trigger_ref: str,
    previous_ref: str,
    changed_paths: list[str],
) -> None:
    role_modules = {
        "client": "limited_remote_partner.gateway.client",
        "local": "limited_remote_partner.cli.main",
        "server": "limited_remote_partner.gateway.server",
    }
    try:
        module_name = role_modules[role]
    except KeyError as exc:
        raise ValueError(f"unsupported GitPartner reexec role: {role}") from exc

    extra_args: tuple[str, ...] = ()
    if config.node_lifecycle.enabled:
        module_name = "limited_remote_partner.cli.partner"
        extra_args = (
            "--role",
            role,
            "--transport",
            config.relay.transport_mode,
        )
    reexec_self(
        module_name,
        config_path,
        trigger_ref,
        previous_ref,
        changed_paths,
        extra_args=extra_args,
    )


def watch_signature(config: AppConfig) -> tuple[tuple[str, str], ...]:
    repo_dir = config.repo_dir.resolve()
    entries: list[tuple[str, str]] = []
    for watched in config.auto_update.watch_paths:
        root = (repo_dir / watched).resolve()
        if not root.exists():
            entries.append((watched, "<missing>"))
            continue
        if root.is_file():
            entries.append((watched, _file_digest(root)))
            continue
        for item in sorted(root.rglob("*")):
            if not item.is_file() or _is_ignored_watch_file(item):
                continue
            rel = item.relative_to(repo_dir).as_posix()
            entries.append((rel, _file_digest(item)))
    return tuple(entries)


def watch_signature_changed(
    previous: tuple[tuple[str, str], ...],
    current: tuple[tuple[str, str], ...],
) -> list[str]:
    if previous == current:
        return []
    previous_map = dict(previous)
    current_map = dict(current)
    changed = sorted(
        path
        for path in set(previous_map) | set(current_map)
        if previous_map.get(path) != current_map.get(path)
    )
    return changed


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _path_matches(path: str, watched: str) -> bool:
    normalized = path.replace("\\", "/").strip("/")
    prefix = watched.replace("\\", "/").strip("/")
    return normalized == prefix or normalized.startswith(prefix + "/")


def _is_ignored_watch_file(path: Path) -> bool:
    parts = set(path.parts)
    return "__pycache__" in parts or path.suffix in {".pyc", ".pyo"}
