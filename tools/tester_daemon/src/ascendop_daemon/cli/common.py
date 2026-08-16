from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

from ascendop_daemon.runtime.application import ApplicationPaths, V3Application


ROOT = Path(
    os.environ.get("ASCENDOP_WORKSPACE_ROOT") or Path(__file__).resolve().parents[5]
).resolve()
DEFAULT_CONFIG = Path(
    os.environ.get("ASCENDOP_DAEMON_CONFIG_PATH")
    or "tools/tester_daemon/config/cann_ladder_910b_cann90.json"
)
DEFAULT_REGISTRY = Path(
    os.environ.get("ASCENDOP_SYSTEM_REGISTRY_PATH")
    or "Develop/registry/system_registry.json"
)
DEFAULT_DATABASE = Path(
    os.environ.get("ASCENDOP_CONTROL_DATABASE_PATH")
    or ".ascendop-work/runtime/control.sqlite3"
)


def add_runtime_paths(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)


def application(args: argparse.Namespace) -> V3Application:
    return V3Application(
        ApplicationPaths.resolve(
            root=args.root,
            config=args.config,
            registry=args.registry,
            database=args.database,
        )
    )


def print_json(value: Any) -> int:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def read_json_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON document must be an object: {path}")
    return value
