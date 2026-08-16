from __future__ import annotations

import argparse

from ascendop_daemon.cli import (
    assistant_commands,
    endpoint_commands,
    request_commands,
    runtime_commands,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="AscendOP Flow V3 daemon")
    subparsers = parser.add_subparsers(dest="command", required=True)
    runtime_commands.register(subparsers)
    request_commands.register(subparsers)
    assistant_commands.register(subparsers)
    endpoint_commands.register(subparsers)
    args = parser.parse_args(argv)
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
