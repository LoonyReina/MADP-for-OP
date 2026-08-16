from __future__ import annotations

import argparse

from ascendop_daemon.cli.common import add_runtime_paths, application, print_json


def register(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "endpoint",
        help="reconcile registered execution endpoints",
    )
    actions = parser.add_subparsers(dest="endpoint_action", required=True)

    reconcile = actions.add_parser(
        "reconcile",
        help="refresh node reports and deliver generation-fenced acknowledgements",
    )
    add_runtime_paths(reconcile)
    reconcile.add_argument(
        "--endpoint-id",
        action="append",
        default=[],
        help="limit reconciliation to an endpoint; may be repeated",
    )
    reconcile.set_defaults(handler=reconcile_endpoints)

    accept = actions.add_parser(
        "accept",
        help="accept one discovered endpoint and deliver its session acknowledgement",
    )
    add_runtime_paths(accept)
    accept.add_argument("--endpoint-id", required=True)
    accept.set_defaults(handler=accept_endpoint)


def reconcile_endpoints(args: argparse.Namespace) -> int:
    app = application(args)
    selected = {str(value) for value in args.endpoint_id if str(value)}
    known = {endpoint.endpoint_id for endpoint in app.registry.endpoints}
    unknown = sorted(selected - known)
    if unknown:
        raise ValueError("unknown endpoint id: " + ", ".join(unknown))
    result = app.reconcile_endpoints(endpoint_ids=selected or None)
    print_json(result)
    return 1 if int(result.get("failure_count", 0)) else 0


def accept_endpoint(args: argparse.Namespace) -> int:
    app = application(args)
    endpoint_id = str(args.endpoint_id)
    known = {endpoint.endpoint_id for endpoint in app.registry.endpoints}
    if endpoint_id not in known:
        raise ValueError(f"unknown endpoint id: {endpoint_id}")
    result = app.accept_endpoint(endpoint_id)
    print_json(result)
    reconciliation = result.get("reconciliation", {})
    return 1 if int(reconciliation.get("failure_count", 0)) else 0
