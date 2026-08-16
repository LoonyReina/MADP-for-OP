from __future__ import annotations

import argparse
from pathlib import Path

from ascendop_daemon.cli.common import (
    add_runtime_paths,
    application,
    print_json,
    read_json_object,
)


def register(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("request", help="manage immutable TestRequests")
    actions = parser.add_subparsers(dest="request_action", required=True)
    create = actions.add_parser("create")
    add_runtime_paths(create)
    create.add_argument("--manifest", required=True, type=argparse.FileType("r"))
    create.set_defaults(handler=create_request)
    scan = actions.add_parser(
        "scan",
        help="discover frozen queued submits and publish immutable Wire V3 requests",
    )
    add_runtime_paths(scan)
    scan.add_argument("--op", default="")
    scan.add_argument("--test-version", default="")
    scan.add_argument(
        "--request-root",
        type=Path,
        default=Path(".ascendop-work/acceptance/test_requests"),
    )
    scan.add_argument(
        "--package-root",
        type=Path,
        default=Path(".ascendop-work/flow-v3/packages"),
    )
    scan.add_argument("--route", action="store_true")
    scan.set_defaults(handler=scan_requests)
    route = actions.add_parser("route")
    add_runtime_paths(route)
    route.add_argument("--request-id", required=True)
    route.set_defaults(handler=route_request)
    materialize = actions.add_parser(
        "materialize-operator-test",
        help="create an immutable acceptance-only request from a frozen submit root",
    )
    add_runtime_paths(materialize)
    materialize.add_argument("--submit-root", required=True, type=Path)
    materialize.add_argument("--endpoint-id", required=True)
    materialize.add_argument(
        "--request-root",
        type=Path,
        default=Path(".ascendop-work/acceptance/test_requests"),
    )
    materialize.add_argument(
        "--execution-profile",
        default="engine-v3-staged-fused",
    )
    materialize.add_argument("--route", action="store_true")
    materialize.set_defaults(handler=materialize_operator_test)
    diagnostic = actions.add_parser(
        "materialize-diagnostic-profile",
        help="create an immutable all-case profiler diagnostic request",
    )
    add_runtime_paths(diagnostic)
    diagnostic.add_argument("--submit-root", required=True, type=Path)
    diagnostic.add_argument("--endpoint-id", required=True)
    diagnostic.add_argument(
        "--profiler-mode",
        required=True,
        choices=("primary-all-cases", "primary-roofline-all-cases"),
    )
    diagnostic.add_argument(
        "--request-root",
        type=Path,
        default=Path(".ascendop-work/acceptance/test_requests"),
    )
    diagnostic.add_argument("--route", action="store_true")
    diagnostic.set_defaults(handler=materialize_diagnostic_profile)


def create_request(args: argparse.Namespace) -> int:
    from pathlib import Path

    app = application(args)
    app.initialize()
    manifest_path = Path(args.manifest.name).resolve()
    manifest = read_json_object(manifest_path)
    return print_json(app.database.create_test_request(manifest, manifest_path))


def scan_requests(args: argparse.Namespace) -> int:
    from ascendop_daemon.control_plane.test_requests import generate_test_requests

    app = application(args)
    app.initialize()
    return print_json(
        generate_test_requests(
            app.paths.root,
            app.config,
            app.database,
            app.registry,
            request_root=(app.paths.root / args.request_root).resolve(),
            execution_profile=str(
                app.config.policy.get(
                    "test_engine_execution_profile",
                    "correctness-first-all-cases-v3",
                )
            ),
            route=bool(args.route),
            package_root=(app.paths.root / args.package_root).resolve(),
            code_generation=app.generation,
            operator=str(args.op or ""),
            test_version=str(args.test_version or ""),
        )
    )


def route_request(args: argparse.Namespace) -> int:
    from ascendop_daemon.control_plane.test_requests import (
        route_and_prepare_test_request,
    )

    app = application(args)
    app.initialize()
    return print_json(
        route_and_prepare_test_request(
            app.paths.root,
            app.database,
            app.registry,
            args.request_id,
            code_generation=app.generation,
        )
    )


def materialize_operator_test(args: argparse.Namespace) -> int:
    from ascendop_daemon.control_plane.test_requests import (
        build_test_request_manifest,
        persist_test_request,
        route_and_prepare_test_request,
    )
    from ascendop_daemon.workflow.engine_candidates import extract_submit_command
    from ascendop_daemon.workflow.operator_job_builder import (
        parse_submit_command,
        tree_digest,
    )

    app = application(args)
    app.initialize()
    pinned_endpoint = next(
        (
            endpoint
            for endpoint in app.registry.endpoints
            if endpoint.endpoint_id == str(args.endpoint_id)
        ),
        None,
    )
    if pinned_endpoint is None:
        raise ValueError(f"unknown endpoint: {args.endpoint_id}")
    submit_root = (app.paths.root / args.submit_root).resolve()
    command = extract_submit_command(submit_root / "SUBMIT.md")
    parsed = parse_submit_command(command)
    registration = app.database.operator_for_display_name(parsed["op"])
    source_sha256 = tree_digest(
        submit_root / "pending_snapshot" / "source_snapshot"
    )
    manifest = build_test_request_manifest(
        app.paths.root,
        {
            "op": parsed["op"],
            "test_version": parsed["test_version"],
            "command": command,
            "job_id_suffix": "v3-acceptance",
        },
        registration,
        execution_profile=str(args.execution_profile),
        submit_root_override=submit_root,
        pinned_endpoint=pinned_endpoint,
        publish_eligible=False,
    )
    request_root = (app.paths.root / args.request_root).resolve()
    manifest_path, persisted = persist_test_request(request_root, manifest)
    record = app.database.create_test_request(persisted, manifest_path)
    route = None
    if bool(args.route):
        route = route_and_prepare_test_request(
            app.paths.root,
            app.database,
            app.registry,
            persisted["request_id"],
            code_generation=app.generation,
        )
    return print_json(
        {
            "schema": "ascendop.materialized-operator-test.v3",
            "request": record,
            "manifest_path": str(manifest_path),
            "route": route,
        }
    )


def materialize_diagnostic_profile(args: argparse.Namespace) -> int:
    from ascendop_daemon.control_plane.test_requests import (
        build_test_request_manifest,
        persist_test_request,
        route_and_prepare_test_request,
    )
    from ascendop_daemon.workflow.engine_candidates import extract_submit_command
    from ascendop_daemon.workflow.operator_job_builder import (
        parse_submit_command,
        tree_digest,
    )

    app = application(args)
    app.initialize()
    pinned_endpoint = next(
        (
            endpoint
            for endpoint in app.registry.endpoints
            if endpoint.endpoint_id == str(args.endpoint_id)
        ),
        None,
    )
    if pinned_endpoint is None:
        raise ValueError(f"unknown endpoint: {args.endpoint_id}")
    submit_root = (app.paths.root / args.submit_root).resolve()
    command = extract_submit_command(submit_root / "SUBMIT.md")
    parsed = parse_submit_command(command)
    registration = app.database.operator_for_display_name(parsed["op"])
    source_sha256 = tree_digest(
        submit_root / "pending_snapshot" / "source_snapshot"
    )
    cases = list(range(1, 17))
    engine_mode = (
        "fast-single"
        if args.profiler_mode == "primary-all-cases"
        else "deep-dual"
    )
    profiler_plan = {
        "operator": parsed["op"],
        "case_version": parsed["case_version"],
        "blocker_result_version": parsed["test_version"],
        "blocker_generation": app.generation,
        "request_sha256": registration["registration_generation"],
        "request_state_path": (
            "TestUtils/tester_daemon/profiler_requests/"
            f"{parsed['op']}-{engine_mode}.json"
        ),
        "target_version": parsed["test_version"],
        "target_source_sha256": source_sha256,
        "cases": cases,
        "profiler_mode": engine_mode,
        "roofline_cases": cases if engine_mode == "deep-dual" else [],
        "primary_metrics": "PipeUtilization,Occupancy",
        "profile_timeout_seconds": 90,
        "request_attempt": 1,
    }
    manifest = build_test_request_manifest(
        app.paths.root,
        {
            "op": parsed["op"],
            "test_version": parsed["test_version"],
            "command": command,
            "job_id_suffix": f"v3-{engine_mode}",
        },
        registration,
        execution_profile="profiler-evidence-v1",
        submit_root_override=submit_root,
        pinned_endpoint=pinned_endpoint,
        publish_eligible=False,
        operation_kind="diagnostic-profile",
        profiler_mode=str(args.profiler_mode),
        profiler_plan=profiler_plan,
    )
    request_root = (app.paths.root / args.request_root).resolve()
    manifest_path, persisted = persist_test_request(request_root, manifest)
    record = app.database.create_test_request(persisted, manifest_path)
    route = None
    if bool(args.route):
        route = route_and_prepare_test_request(
            app.paths.root,
            app.database,
            app.registry,
            persisted["request_id"],
            code_generation=app.generation,
        )
    return print_json(
        {
            "schema": "ascendop.materialized-diagnostic-profile.v3",
            "request": record,
            "manifest_path": str(manifest_path),
            "route": route,
        }
    )
