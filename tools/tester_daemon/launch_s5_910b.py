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
    environment = {
        "ASCENDOP_RELEASE_GENERATION": active.get("release_generation"),
        "ASCENDOP_VARIABLE_REGISTRY_PATH": active.get("variable_registry_path"),
        "ASCENDOP_DAEMON_CONFIG_PATH": active.get("daemon_config_path"),
        "ASCENDOP_SYSTEM_REGISTRY_PATH": active.get("system_registry_path"),
        "ASCENDOP_CONTROL_DATABASE_PATH": active.get("control_database_path"),
        "GITPARTNER_RUNTIME_SOURCE": active.get("transport_source"),
        "GITPARTNER_TRANSPORT_GENERATION": active.get("transport_generation"),
    }
    for name, value in environment.items():
        if value:
            os.environ.setdefault(name, str(value))
active_sources = (
    str(active.get("daemon_source") or ""),
    str(active.get("protocol_source") or ""),
)
package_roots = tuple(Path(value) for value in active_sources)
if not all(active_sources) or not all(path.is_dir() for path in package_roots):
    package_roots = (ROOT / "tools/tester_daemon/src", ROOT / "packages/ascendop_protocol/src")
for package_root in reversed(package_roots):
    if str(package_root) not in sys.path:
        sys.path.insert(0, str(package_root))
from ascendop_daemon.cli.resident_main import main
if __name__ == "__main__":
    raise SystemExit(main())
