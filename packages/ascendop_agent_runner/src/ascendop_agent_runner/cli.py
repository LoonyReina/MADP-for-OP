from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from .runner import AgentRunner


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="AscendOP Flow V4 CLI Agent runner")
    parser.add_argument("command", choices=("probe", "run-once", "run-resident"))
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--interval-seconds", type=float, default=1.0)
    parser.add_argument("--lease-seconds", type=int, default=30)
    parser.add_argument("--generation", required=True)
    parser.add_argument("--runner-generation")
    args = parser.parse_args(argv)
    runner = AgentRunner(
        root=args.root,
        database=args.database,
        lease_seconds=args.lease_seconds,
        code_generation=args.generation,
        runner_generation=args.runner_generation,
        config=args.config,
    )
    if args.command == "probe":
        return _print(runner.probe_and_register())
    if args.command == "run-once":
        return _print(runner.run_once())
    runner.maintain_registrations(force=True)
    try:
        while True:
            stopped = (
                args.root / "TestUtils" / "tester_daemon" / "daemon_stop.json"
            ).is_file()
            result = runner.run_once(allow_claims=not stopped)
            if result.get("state") in {"idle", "stopped"}:
                time.sleep(max(0.25, min(args.interval_seconds, 10.0)))
    except KeyboardInterrupt:
        return 0


def _print(value: object) -> int:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
