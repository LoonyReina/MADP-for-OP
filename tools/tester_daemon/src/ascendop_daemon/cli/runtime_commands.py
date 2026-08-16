from __future__ import annotations

import argparse
from pathlib import Path

from ascendop_daemon.cli.common import ROOT, add_runtime_paths, application, print_json
from ascendop_daemon.runtime.control import clear_stop_request, write_stop_request
from ascendop_daemon.runtime.locking import DaemonLock


def register(subparsers: argparse._SubParsersAction) -> None:
    for name, help_text, handler in (
        ("init", "initialize and reconcile the V3 control plane", initialize),
        ("once", "run one V3 dispatch/collect/ack cycle", once),
        ("status", "show authoritative V3 control-plane status", status),
    ):
        parser = subparsers.add_parser(name, help=help_text)
        add_runtime_paths(parser)
        parser.set_defaults(handler=handler)

    run_parser = subparsers.add_parser("run", help="run the resident V3 daemon")
    add_runtime_paths(run_parser)
    run_parser.add_argument("--interval-seconds", type=float, default=1.0)
    run_parser.add_argument("--max-cycles", type=int, default=0)
    run_parser.add_argument("--replace-stale-lock-after-seconds", type=int, default=0)
    run_parser.set_defaults(handler=run)

    stop_parser = subparsers.add_parser("stop", help="set the stop fence")
    stop_parser.add_argument("--root", type=Path, default=ROOT)
    stop_parser.add_argument("--reason", default="")
    stop_parser.set_defaults(handler=stop)

    clear_parser = subparsers.add_parser("clear-stop", help="clear the stop fence")
    clear_parser.add_argument("--root", type=Path, default=ROOT)
    clear_parser.set_defaults(handler=clear_stop)


def initialize(args: argparse.Namespace) -> int:
    return print_json(application(args).initialize())


def once(args: argparse.Namespace) -> int:
    app = application(args)
    app.initialize()
    return print_json(app.run_once())


def status(args: argparse.Namespace) -> int:
    return print_json(application(args).status())


def run(args: argparse.Namespace) -> int:
    app = application(args)
    with DaemonLock(
        app.paths.root,
        stale_after_seconds=max(0, args.replace_stale_lock_after_seconds),
    ):
        result = app.run(
            interval_seconds=args.interval_seconds,
            max_cycles=max(0, args.max_cycles),
        )
    return print_json(result)


def stop(args: argparse.Namespace) -> int:
    return print_json(
        {"stop_fence": str(write_stop_request(args.root.resolve(), args.reason))}
    )


def clear_stop(args: argparse.Namespace) -> int:
    return print_json({"cleared": clear_stop_request(args.root.resolve())})
