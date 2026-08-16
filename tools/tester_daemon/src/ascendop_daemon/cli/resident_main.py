from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from ascendop_daemon.cli.common import (
    DEFAULT_CONFIG,
    DEFAULT_DATABASE,
    DEFAULT_REGISTRY,
    ROOT,
)
from ascendop_daemon.runtime.resident_service import (
    ResidentServiceError,
    service_status,
    start_service,
    stop_service,
)


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description="AscendOP Flow V3 resident service")
    value.add_argument(
        "action",
        choices=("start", "stop", "restart", "status", "ensure-resident"),
        nargs="?",
        default="status",
    )
    value.add_argument("--root", type=Path, default=ROOT)
    value.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    value.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    value.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    value.add_argument("--interval-seconds", type=float, default=1.0)
    value.add_argument("--wait-seconds", type=float, default=30.0)
    value.add_argument("--force", action="store_true")
    value.add_argument("--reason", default="")
    return value


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        result = execute(args)
    except (OSError, ValueError, ResidentServiceError) as exc:
        print(f"ASCENDOP_V3_RESIDENT_ERROR {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def execute(args: argparse.Namespace) -> dict[str, object]:
    if args.action == "status":
        return service_status(args.root, database=args.database)
    if args.action == "stop":
        return stop_service(
            args.root,
            database=args.database,
            reason=args.reason or "V3 resident stop",
            wait_seconds=args.wait_seconds,
            force=args.force,
        )
    if args.action == "restart":
        stopped = stop_service(
            args.root,
            database=args.database,
            reason=args.reason or "V3 resident restart",
            wait_seconds=args.wait_seconds,
            force=args.force,
        )
        if stopped["running"]:
            return stopped
    return start_service(
        args.root,
        config=args.config,
        registry=args.registry,
        database=args.database,
        interval_seconds=args.interval_seconds,
        resume=args.action in {"start", "restart"},
    )


if __name__ == "__main__":
    raise SystemExit(main())
