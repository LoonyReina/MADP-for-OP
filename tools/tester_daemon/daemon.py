from __future__ import annotations

import json
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
os.environ.setdefault("ASCENDOP_WORKSPACE_ROOT", str(ROOT))
active_release = ROOT / ".ascendop-work" / "runtime" / "active-release.json"
try:
    active = json.loads(active_release.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    active = {}
if isinstance(active, dict) and active.get("release_generation"):
    os.environ.setdefault(
        "ASCENDOP_RELEASE_GENERATION", str(active["release_generation"])
    )
    if active.get("variable_registry_path"):
        os.environ.setdefault(
            "ASCENDOP_VARIABLE_REGISTRY_PATH",
            str(active["variable_registry_path"]),
        )
    for environment_name, active_key in (
        ("ASCENDOP_DAEMON_CONFIG_PATH", "daemon_config_path"),
        ("ASCENDOP_SYSTEM_REGISTRY_PATH", "system_registry_path"),
        ("ASCENDOP_CONTROL_DATABASE_PATH", "control_database_path"),
    ):
        if active.get(active_key):
            os.environ.setdefault(environment_name, str(active[active_key]))
    if active.get("transport_source"):
        os.environ.setdefault(
            "GITPARTNER_RUNTIME_SOURCE",
            str(active["transport_source"]),
        )
    if active.get("transport_generation"):
        os.environ.setdefault(
            "GITPARTNER_TRANSPORT_GENERATION",
            str(active["transport_generation"]),
        )
active_sources = (
    str(active.get("daemon_source") or ""),
    str(active.get("protocol_source") or ""),
)
package_roots = tuple(Path(value) for value in active_sources)
if not all(active_sources) or not all(path.is_dir() for path in package_roots):
    package_roots = (
        ROOT / "tools" / "tester_daemon" / "src",
        ROOT / "packages" / "ascendop_protocol" / "src",
    )
for package_root in reversed(package_roots):
    if str(package_root) not in sys.path:
        sys.path.insert(0, str(package_root))

from ascendop_daemon.cli.main import main


if __name__ == "__main__":
    raise SystemExit(main())
