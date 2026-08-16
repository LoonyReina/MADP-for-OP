from __future__ import annotations

import hashlib
import os
import shutil
from pathlib import Path
from pathlib import PurePosixPath


IGNORED_NAMES = {
    "__pycache__",
    ".pytest_cache",
    ".git",
    ".ascendop",
    "build",
    "dist",
}


class WorkspaceStager:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        self.runs_root = self.root / ".ascendop-work" / "agent-runs"

    def stage(self, action: dict[str, object]) -> tuple[Path, Path]:
        origin = self._bounded(str(action["origin_workspace"]))
        run_root = self.action_run_root(str(action["action_id"]))
        workspace = run_root / "workspace"
        if workspace.exists():
            return run_root, workspace
        run_root.mkdir(parents=True, exist_ok=True)
        shutil.copytree(origin, workspace, ignore=_ignore)
        return run_root, workspace

    def action_run_root(self, action_id: str) -> Path:
        run_root = (self.runs_root / action_id).resolve()
        if run_root.parent != self.runs_root.resolve():
            raise RuntimeError("agent run path escaped run root")
        return run_root

    def attempt_run_root(self, action_id: str, attempt_id: str) -> Path:
        action_root = self.action_run_root(action_id)
        attempt_root = (action_root / "attempts" / attempt_id).resolve()
        if attempt_root.parent != (action_root / "attempts").resolve():
            raise RuntimeError("agent attempt path escaped action run root")
        return attempt_root

    def digest(self, workspace: Path) -> str:
        digest = hashlib.sha256()
        for path in sorted(workspace.rglob("*"), key=lambda item: item.as_posix()):
            if not path.is_file() or any(part in IGNORED_NAMES for part in path.parts):
                continue
            relative = path.relative_to(workspace).as_posix().encode("utf-8")
            digest.update(len(relative).to_bytes(8, "big"))
            digest.update(relative)
            payload = path.read_bytes()
            digest.update(len(payload).to_bytes(8, "big"))
            digest.update(payload)
        return digest.hexdigest()

    def snapshot(self, workspace: Path) -> dict[str, str]:
        result: dict[str, str] = {}
        for path in sorted(workspace.rglob("*"), key=lambda item: item.as_posix()):
            if not path.is_file() or any(part in IGNORED_NAMES for part in path.parts):
                continue
            relative = path.relative_to(workspace).as_posix()
            result[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
        return result

    @staticmethod
    def changed_paths(
        before: dict[str, str],
        after: dict[str, str],
    ) -> list[str]:
        return sorted(
            path
            for path in set(before) | set(after)
            if before.get(path) != after.get(path)
        )

    @staticmethod
    def out_of_scope_paths(paths: list[str], scopes: list[str]) -> list[str]:
        return [path for path in paths if not _path_allowed(path, scopes)]

    def _bounded(self, relative: str) -> Path:
        path = (self.root / relative).resolve()
        if path != self.root and self.root not in path.parents:
            raise RuntimeError("agent origin workspace escaped root")
        if not path.is_dir():
            raise RuntimeError(f"agent origin workspace is missing: {relative}")
        return path


def _ignore(directory: str, names: list[str]) -> set[str]:
    return {name for name in names if name in IGNORED_NAMES or name.endswith(".pyc")}


def _path_allowed(path: str, scopes: list[str]) -> bool:
    candidate = PurePosixPath(path)
    for raw_scope in scopes:
        scope = raw_scope.strip().replace("\\", "/").rstrip("/")
        if not scope:
            continue
        if path == scope or path.startswith(scope + "/"):
            return True
        if any(marker in scope for marker in "*?[") and candidate.match(scope):
            return True
    return False
