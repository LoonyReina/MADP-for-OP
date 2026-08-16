from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Iterable


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = REPOSITORY_ROOT / "publication" / "core-manifest.json"
ALLOWED_COMPONENTS = {
    ("packages/ascendop_protocol/src", "packages/ascendop_protocol/src"): (),
    ("packages/ascendop_protocol/tests", "packages/ascendop_protocol/tests"): (),
    ("packages/ascendop_control/src", "packages/ascendop_control/src"): (),
    ("packages/ascendop_control/tests", "packages/ascendop_control/tests"): (),
    (
        "packages/ascendop_agent_runner/src",
        "packages/ascendop_agent_runner/src",
    ): (),
    (
        "packages/ascendop_agent_runner/tests",
        "packages/ascendop_agent_runner/tests",
    ): (),
    (
        "tools/tester_daemon/src/ascendop_daemon/automation",
        "packages/ascendop_daemon/src/ascendop_daemon/automation",
    ): ("session_recovery.py",),
    (
        "tools/tester_daemon/src/ascendop_daemon/control_plane",
        "packages/ascendop_daemon/src/ascendop_daemon/control_plane",
    ): (),
    (
        "tools/tester_daemon/src/ascendop_daemon/core",
        "packages/ascendop_daemon/src/ascendop_daemon/core",
    ): (),
    (
        "tools/tester_daemon/src/ascendop_daemon/exchange",
        "packages/ascendop_daemon/src/ascendop_daemon/exchange",
    ): (),
    (
        "tools/tester_daemon/src/ascendop_daemon/observability",
        "packages/ascendop_daemon/src/ascendop_daemon/observability",
    ): ("status_writer.py",),
    (
        "tools/tester_daemon/src/ascendop_daemon/registry",
        "packages/ascendop_daemon/src/ascendop_daemon/registry",
    ): (),
    (
        "tools/tester_daemon/src/ascendop_daemon/runtime",
        "packages/ascendop_daemon/src/ascendop_daemon/runtime",
    ): ("flow_v3_runtime.py",),
    (
        "tools/tester_daemon/src/ascendop_daemon/storage",
        "packages/ascendop_daemon/src/ascendop_daemon/storage",
    ): ("flow_v3_migration.py",),
    (
        "tools/tester_daemon/src/ascendop_daemon/workflow",
        "packages/ascendop_daemon/src/ascendop_daemon/workflow",
    ): (),
}


def _inside(path: Path, parent: Path) -> bool:
    return path == parent or parent in path.parents


def _load_manifest() -> dict[str, object]:
    value = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    if value.get("schema") != "madp.public-core-manifest.v1":
        raise ValueError("unsupported public core manifest")
    components = value.get("components")
    if not isinstance(components, list):
        raise ValueError("manifest components must be a list")
    declared = {
        (str(item.get("source", "")), str(item.get("destination", ""))): tuple(
            sorted(str(path) for path in item.get("excluded_paths", []))
        )
        for item in components
        if isinstance(item, dict)
    }
    if declared != ALLOWED_COMPONENTS or len(components) != len(ALLOWED_COMPONENTS):
        raise ValueError("manifest must match the hard-coded public core allowlist")
    return value


def _files(
    root: Path,
    *,
    excluded_names: set[str],
    excluded_suffixes: set[str],
    excluded_paths: set[str],
) -> Iterable[Path]:
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        if relative.as_posix() in excluded_paths:
            continue
        if any(part in excluded_names for part in relative.parts):
            continue
        if path.is_symlink():
            raise ValueError(f"symbolic links are not allowed: {path}")
        if path.is_file() and path.suffix.lower() not in excluded_suffixes:
            yield path


def _tree(
    root: Path,
    *,
    excluded_names: set[str],
    excluded_suffixes: set[str],
    excluded_paths: set[str],
) -> dict[str, str]:
    if not root.is_dir():
        raise FileNotFoundError(root)
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in _files(
            root,
            excluded_names=excluded_names,
            excluded_suffixes=excluded_suffixes,
            excluded_paths=excluded_paths,
        )
    }


def _tree_digest(tree: dict[str, str]) -> str:
    payload = "\n".join(f"{path}\0{digest}" for path, digest in sorted(tree.items()))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _copy_component(
    source: Path,
    destination: Path,
    *,
    excluded_names: set[str],
    excluded_suffixes: set[str],
    excluded_paths: set[str],
) -> None:
    if not _inside(destination, REPOSITORY_ROOT):
        raise ValueError(f"destination escapes repository: {destination}")
    with tempfile.TemporaryDirectory(
        prefix=".madp-sync-", dir=REPOSITORY_ROOT
    ) as temporary:
        staged = Path(temporary) / destination.name
        for source_file in _files(
            source,
            excluded_names=excluded_names,
            excluded_suffixes=excluded_suffixes,
            excluded_paths=excluded_paths,
        ):
            relative = source_file.relative_to(source)
            staged_file = staged / relative
            staged_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_file, staged_file)
        if destination.exists():
            shutil.rmtree(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(staged, destination)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Synchronize only allowlisted MADP core source from AscendOP."
    )
    parser.add_argument("--ascendop-root", type=Path, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    ascendop_root = args.ascendop_root.resolve()
    manifest = _load_manifest()
    excluded_names = {str(value) for value in manifest["excluded_names"]}
    excluded_suffixes = {
        str(value).lower() for value in manifest["excluded_suffixes"]
    }
    differences: list[dict[str, object]] = []
    component_digests: dict[str, str] = {}

    for item in manifest["components"]:
        source = (ascendop_root / str(item["source"])).resolve()
        destination = (REPOSITORY_ROOT / str(item["destination"])).resolve()
        if not _inside(source, ascendop_root):
            raise ValueError(f"source escapes AscendOP root: {source}")
        component_excluded_paths = {
            str(value) for value in item.get("excluded_paths", [])
        }
        if args.apply:
            _copy_component(
                source,
                destination,
                excluded_names=excluded_names,
                excluded_suffixes=excluded_suffixes,
                excluded_paths=component_excluded_paths,
            )
        source_tree = _tree(
            source,
            excluded_names=excluded_names,
            excluded_suffixes=excluded_suffixes,
            excluded_paths=component_excluded_paths,
        )
        destination_tree = _tree(
            destination,
            excluded_names=excluded_names,
            excluded_suffixes=excluded_suffixes,
            excluded_paths=set(),
        )
        component_digests[str(item["name"])] = _tree_digest(destination_tree)
        changed = sorted(
            path
            for path in source_tree.keys() | destination_tree.keys()
            if source_tree.get(path) != destination_tree.get(path)
        )
        if changed:
            differences.append({"component": item["name"], "changed": changed})

    result = {
        "schema": "madp.public-core-sync.v1",
        "mode": "apply" if args.apply else "check",
        "state": "synchronized" if not differences else "drift",
        "component_digests": component_digests,
        "differences": differences,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if not differences else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print(f"public core sync failed: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
