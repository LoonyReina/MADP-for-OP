from __future__ import annotations

import fnmatch
from pathlib import Path
from typing import Any, Iterable


DAEMON_WORKSPACE_PREFIX = "tools/tester_daemon/"


def daemon_offline_patterns(policy: dict[str, Any]) -> tuple[str, ...]:
    raw = policy.get("offline_patterns")
    if not isinstance(raw, list) or any(
        not isinstance(item, str) or not item for item in raw
    ):
        raise ValueError("change-impact offline patterns are invalid")
    patterns = {
        item[len(DAEMON_WORKSPACE_PREFIX) :]
        for item in raw
        if item.startswith(DAEMON_WORKSPACE_PREFIX)
    }
    return tuple(sorted(patterns))


def daemon_file_is_offline(relative: str, patterns: Iterable[str]) -> bool:
    normalized = str(relative).replace("\\", "/")
    return any(fnmatch.fnmatchcase(normalized, pattern) for pattern in patterns)


def offline_daemon_files(root: Path, patterns: Iterable[str]) -> list[str]:
    root = root.resolve()
    return [
        path.relative_to(root).as_posix()
        for path in sorted(root.rglob("*"), key=lambda item: item.as_posix())
        if path.is_file()
        and not path.is_symlink()
        and daemon_file_is_offline(path.relative_to(root).as_posix(), patterns)
    ]


__all__ = [
    "daemon_file_is_offline",
    "daemon_offline_patterns",
    "offline_daemon_files",
]
