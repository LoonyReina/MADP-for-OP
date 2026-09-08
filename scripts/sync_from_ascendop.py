from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path, PurePosixPath
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
    if value.get("schema") != "madp.public-core-manifest.v2":
        raise ValueError("unsupported public core manifest")
    components = value.get("components")
    if not isinstance(components, list):
        raise ValueError("manifest components must be a list")
    if len({item["name"] for item in components}) != len(components):
        raise ValueError("component names must be unique")
    declared = {
        (str(item.get("source", "")), str(item.get("destination", ""))): tuple(
            sorted(str(path) for path in item.get("excluded_paths", []))
        )
        for item in components
        if isinstance(item, dict)
    }
    if declared != ALLOWED_COMPONENTS or len(components) != len(ALLOWED_COMPONENTS):
        raise ValueError("manifest must match the hard-coded public core allowlist")
    for item in components:
        synced, retained = item.get("sync_paths"), item.get("retained_paths")
        if not isinstance(synced, list) or not isinstance(retained, list):
            raise ValueError("each component needs explicit synchronized and retained paths")
        names = synced + retained
        if not names or len(set(names)) != len(names):
            raise ValueError("component file selection is empty or duplicated")
        for name in names:
            _relative_file(name)
            if (name in item.get("excluded_paths", [])
                    or any(part in value["excluded_names"] for part in PurePosixPath(name).parts)
                    or PurePosixPath(name).suffix in value["excluded_suffixes"]):
                raise ValueError("component selection includes an excluded file")
        expected = {"agent_completion.py": "public_completion.py"} if item["name"] == "daemon-automation" else {}
        if item.get("source_overrides", {}) != expected or not set(expected).issubset(synced):
            raise ValueError("unreviewed public source facade mapping")
    return value


def _relative_file(name: str) -> str:
    if (not isinstance(name, str) or not name or "\\" in name or ":" in name
            or PurePosixPath(name).is_absolute() or ".." in PurePosixPath(name).parts
            or name == "." or PurePosixPath(name).as_posix() != name):
        raise ValueError("file selection must be canonical and relative")
    return name


def _selected_file(root: Path, name: str) -> Path:
    path = root / _relative_file(name)
    if path.is_symlink() or path.resolve() != path.absolute() or not _inside(path.resolve(), root):
        raise ValueError("selected source/destination is linked or outside its component")
    return path


def _selected_tree(root: Path, names: list[str], overrides: dict[str, str] | None = None) -> dict[str, str]:
    result = {}
    for name in names:
        path = _selected_file(root, (overrides or {}).get(name, name))
        if path.is_file():
            # Sync comparison permits repository EOL normalization. Publication
            # provenance below still hashes the exact exported bytes.
            content = path.read_bytes()
            if path.suffix in {".py", ".json", ".md", ".toml", ".txt"}:
                content = content.replace(b"\r\n", b"\n")
            result[name] = hashlib.sha256(content).hexdigest()
    return result


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
    names: list[str],
    overrides: dict[str, str],
) -> None:
    if not _inside(destination, REPOSITORY_ROOT):
        raise ValueError(f"destination escapes repository: {destination}")
    # No recursive replacement: retained compatibility files and unrelated
    # user edits are never deleted by a source synchronization.
    pairs = [(_selected_file(source, overrides.get(name, name)), _selected_file(destination, name)) for name in names]
    if any(not source_file.is_file() for source_file, _ in pairs):
        raise ValueError("selected upstream source is missing")
    for source_file, destination_file in pairs:
        destination_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_file, destination_file)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Synchronize only allowlisted MADP core source from AscendOP."
    )
    parser.add_argument("--ascendop-root", type=Path, required=True)
    parser.add_argument("--component", action="append", default=[],
                        help="review/synchronize only these manifest component names")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    ascendop_root = args.ascendop_root.resolve()
    manifest = _load_manifest()
    selected = set(args.component)
    known = {str(item["name"]) for item in manifest["components"]}
    if selected - known:
        raise ValueError("unknown public core component: " + ", ".join(sorted(selected - known)))
    excluded_names = {str(value) for value in manifest["excluded_names"]}
    excluded_suffixes = {
        str(value).lower() for value in manifest["excluded_suffixes"]
    }
    differences: list[dict[str, object]] = []
    component_digests: dict[str, str] = {}

    for item in manifest["components"]:
        if selected and str(item["name"]) not in selected:
            continue
        source = (ascendop_root / str(item["source"])).resolve()
        destination = (REPOSITORY_ROOT / str(item["destination"])).resolve()
        if not _inside(source, ascendop_root):
            raise ValueError(f"source escapes AscendOP root: {source}")
        names = item["sync_paths"]
        overrides = item.get("source_overrides", {})
        if args.apply:
            _copy_component(
                source,
                destination,
                names=names,
                overrides=overrides,
            )
        source_tree = _selected_tree(source, names, overrides)
        destination_tree = _tree(
            destination,
            excluded_names=excluded_names,
            excluded_suffixes=excluded_suffixes,
            excluded_paths=set(),
        )
        component_digests[str(item["name"])] = _tree_digest(destination_tree)
        selected_destination = _selected_tree(destination, names)
        changed = sorted(
            path
            for path in names
            if path not in source_tree or source_tree.get(path) != selected_destination.get(path)
        )
        declared_paths = set(names + item["retained_paths"])
        inventory_drift = sorted(set(destination_tree).symmetric_difference(declared_paths))
        if changed:
            differences.append({"component": item["name"], "changed": changed})
        if inventory_drift:
            differences.append({"component": item["name"], "inventory_drift": inventory_drift})

    result = {
        "schema": "madp.public-core-sync.v1",
        "mode": "apply" if args.apply else "check",
        "state": "synchronized" if not differences else "drift",
        "scope": "reviewed-selected-files; retained compatibility files are not refreshed",
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
