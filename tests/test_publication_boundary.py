from __future__ import annotations

import json
import hashlib
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _tree_digest(
    root: Path,
    *,
    excluded_names: set[str],
    excluded_suffixes: set[str],
    excluded_paths: set[str],
) -> str:
    values: list[str] = []
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        if relative.as_posix() in excluded_paths:
            continue
        if any(part in excluded_names for part in relative.parts):
            continue
        if path.is_file() and path.suffix.lower() not in excluded_suffixes:
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            values.append(f"{relative.as_posix()}\0{digest}")
    return hashlib.sha256("\n".join(values).encode("utf-8")).hexdigest()


def test_manifest_has_only_core_implementation_trees() -> None:
    manifest = json.loads(
        (ROOT / "publication" / "core-manifest.json").read_text(encoding="utf-8")
    )
    sources = {item["source"] for item in manifest["components"]}
    assert sources == {
        "packages/ascendop_protocol/src",
        "packages/ascendop_protocol/tests",
        "packages/ascendop_control/src",
        "packages/ascendop_control/tests",
        "packages/ascendop_agent_runner/src",
        "packages/ascendop_agent_runner/tests",
        "tools/tester_daemon/src/ascendop_daemon/automation",
        "tools/tester_daemon/src/ascendop_daemon/control_plane",
        "tools/tester_daemon/src/ascendop_daemon/core",
        "tools/tester_daemon/src/ascendop_daemon/exchange",
        "tools/tester_daemon/src/ascendop_daemon/observability",
        "tools/tester_daemon/src/ascendop_daemon/registry",
        "tools/tester_daemon/src/ascendop_daemon/runtime",
        "tools/tester_daemon/src/ascendop_daemon/storage",
        "tools/tester_daemon/src/ascendop_daemon/workflow",
    }
    assert all("GitPartner" not in source and "engine_runtime" not in source for source in sources)
    exclusions = {
        item["name"]: set(item.get("excluded_paths", []))
        for item in manifest["components"]
    }
    assert exclusions["daemon-automation"] == {"session_recovery.py"}
    assert exclusions["daemon-observability"] == {"status_writer.py"}
    assert exclusions["daemon-runtime"] == {"flow_v3_runtime.py"}
    assert exclusions["daemon-storage"] == {"flow_v3_migration.py"}


def test_publication_scan_passes() -> None:
    completed = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "publication_scan.py")],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_current_provenance_matches_public_core_trees() -> None:
    manifest = json.loads(
        (ROOT / "publication" / "core-manifest.json").read_text(encoding="utf-8")
    )
    current = json.loads(
        (ROOT / "release" / "CURRENT.json").read_text(encoding="utf-8")
    )
    provenance_path = ROOT / str(current["provenance_path"])
    provenance = json.loads(
        provenance_path.read_text(encoding="utf-8")
    )
    excluded_names = set(manifest["excluded_names"])
    excluded_suffixes = {value.lower() for value in manifest["excluded_suffixes"]}
    actual = {
        item["name"]: _tree_digest(
            ROOT / item["destination"],
            excluded_names=excluded_names,
            excluded_suffixes=excluded_suffixes,
            excluded_paths=set(item.get("excluded_paths", [])),
        )
        for item in manifest["components"]
    }
    assert actual == provenance["allowlisted_component_digests"]
