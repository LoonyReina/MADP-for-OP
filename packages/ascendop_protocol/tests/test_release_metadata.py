from __future__ import annotations

import json
import tomllib
from pathlib import Path


def test_package_version_matches_schema_registry_release() -> None:
    package_root = Path(__file__).resolve().parents[1]
    project = tomllib.loads(
        (package_root / "pyproject.toml").read_text(encoding="utf-8")
    )
    registry = json.loads(
        (
            package_root
            / "src"
            / "ascendop_protocol"
            / "schemas"
            / "schema_registry.json"
        ).read_text(encoding="utf-8")
    )

    assert project["project"]["version"] == registry["protocol_release"]
