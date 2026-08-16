from __future__ import annotations

import shutil
from pathlib import Path

from ascendop_daemon.core.filesystem import filesystem_path
from ascendop_daemon.workflow.errors import EngineJobBuildError


CASE_PACKAGE_EXCLUDED_NAMES = frozenset({"profiler_evidence"})


def case_package_paths(source: Path) -> tuple[Path, ...]:
    """Return the immutable execution view of a case lifetime."""

    paths: list[Path] = []
    for path in sorted(source.rglob("*"), key=lambda item: item.as_posix()):
        relative = path.relative_to(source)
        if any(part in CASE_PACKAGE_EXCLUDED_NAMES for part in relative.parts):
            continue
        paths.append(path)
    return tuple(paths)


def copy_tree_without_symlinks(
    source: Path,
    destination: Path,
    *,
    exclude_names: tuple[str, ...] = (),
) -> None:
    for path in source.rglob("*"):
        if path.is_symlink():
            raise EngineJobBuildError(f"engine payload cannot contain symlinks: {path}")
    shutil.copytree(
        filesystem_path(source),
        filesystem_path(destination),
        ignore=shutil.ignore_patterns(
            "__pycache__", "*.pyc", "*.pyo", *exclude_names
        ),
    )
