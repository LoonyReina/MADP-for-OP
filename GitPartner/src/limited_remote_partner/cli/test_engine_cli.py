from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

from limited_remote_partner.engine import test_engine as engine_module
from limited_remote_partner.engine.test_engine import (
    DEFAULT_RESIDENT_HEARTBEAT_STALE_SECONDS,
    EngineError,
    EngineProcessLock,
    TestEngine,
    atomic_write_json,
    engine_code_generation,
    normalize_token,
    read_json,
    utc_now,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Durable multi-slot AscendOP test engine")
    parser.add_argument("--root", type=Path, required=True)
    subparsers = parser.add_subparsers(dest="command", required=True)

    init = subparsers.add_parser("init")
    add_capacity_arguments(init)

    submit = subparsers.add_parser("submit")
    submit.add_argument("--spec", type=Path, required=True)
    submit.add_argument("--payload-root", type=Path)

    tick = subparsers.add_parser("tick")
    tick.add_argument("--count", type=int, default=1)

    run = subparsers.add_parser("run")
    run.add_argument("--interval-seconds", type=float, default=0.5)
    run.add_argument("--max-ticks", type=int, default=0)

    start = subparsers.add_parser("start")
    start.add_argument("--interval-seconds", type=float, default=0.25)
    start.add_argument(
        "--heartbeat-stale-seconds",
        type=float,
        default=DEFAULT_RESIDENT_HEARTBEAT_STALE_SECONDS,
    )
    stop = subparsers.add_parser("stop")
    stop.add_argument("--wait-seconds", type=float, default=5.0)
    subparsers.add_parser("service-status")

    subparsers.add_parser("status")
    subparsers.add_parser("transport-status")
    subparsers.add_parser("return-ready")
    export_ready = subparsers.add_parser("export-ready")
    export_ready.add_argument("--destination", type=Path, required=True)
    export_ready.add_argument("--archive", type=Path)

    capacity = subparsers.add_parser("set-capacity")
    add_capacity_arguments(capacity)
    drain = capacity.add_mutually_exclusive_group()
    drain.add_argument("--drain", action="store_true")
    drain.add_argument("--resume", action="store_true")

    ack = subparsers.add_parser("ack-return")
    ack.add_argument("--engine-job-id", required=True)
    ack.add_argument("--receipt-id", required=True)
    ack_required = subparsers.add_parser("ack-required")
    ack_required.add_argument("--engine-job-id", required=True)
    ack_required.add_argument("--receipt-id", required=True)

    exchange = subparsers.add_parser("exchange")
    exchange.add_argument("--manifest", type=Path, required=True)
    exchange.add_argument("--transport-dir", type=Path, required=True)
    exchange.add_argument("--ack-return", action="append", default=[])
    exchange.add_argument("--ack-required", action="append", default=[])
    exchange.add_argument("--wait-ready-seconds", type=float, default=0.0)
    add_capacity_arguments(exchange)
    exchange_mode = exchange.add_mutually_exclusive_group()
    exchange_mode.add_argument("--drain", action="store_true")
    exchange_mode.add_argument("--resume", action="store_true")
    return parser


def add_capacity_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--max-inflight", type=int)
    parser.add_argument("--standby-slots", type=int)
    parser.add_argument("--active-job-slots", type=int)
    parser.add_argument("--host-slots", type=int)
    parser.add_argument("--host-cpu-weight-capacity", type=int)
    parser.add_argument("--host-memory-mb-capacity", type=int)
    parser.add_argument("--host-io-weight-capacity", type=int)
    parser.add_argument("--cold-build-slots", type=int)
    parser.add_argument("--cache-hit-slots", type=int)
    parser.add_argument("--device-slots", type=int)
    parser.add_argument(
        "--device-inventory-json",
        help="JSON list of physical device records; device_slots is derived from it",
    )
    parser.add_argument("--export-slots", type=int)
    parser.add_argument("--return-backlog-soft-limit-bytes", type=int)
    parser.add_argument("--return-backlog-hard-limit-bytes", type=int)
    parser.add_argument("--return-backlog-soft-limit-jobs", type=int)
    parser.add_argument("--return-backlog-hard-limit-jobs", type=int)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    engine = TestEngine(args.root)
    try:
        result = dispatch(engine, args)
    except EngineError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, indent=2), file=sys.stderr)
        return 2
    if result is not None:
        print(json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


def dispatch(engine: TestEngine, args: argparse.Namespace) -> dict[str, Any] | list[Any] | None:
    if args.command == "init":
        capacity = capacity_updates(args)
        return engine.initialize(capacity or None)
    if args.command == "submit":
        return engine.submit(read_json(args.spec), payload_root=args.payload_root)
    if args.command == "tick":
        count = max(1, int(args.count))
        with EngineProcessLock(engine.root):
            result: dict[str, Any] = {}
            for _index in range(count):
                result = engine.tick()
            return result
    if args.command == "run":
        with EngineProcessLock(engine.root):
            engine.run(
                interval_seconds=max(0.05, float(args.interval_seconds)),
                max_ticks=max(0, int(args.max_ticks)),
            )
        return engine.snapshot()
    if args.command == "start":
        return engine.ensure_resident(
            interval_seconds=max(0.05, float(args.interval_seconds)),
            heartbeat_stale_seconds=max(
                2.0,
                float(args.heartbeat_stale_seconds),
            ),
        )
    if args.command == "stop":
        return engine.request_stop(wait_seconds=max(0.0, float(args.wait_seconds)))
    if args.command == "service-status":
        return engine.resident_status()
    if args.command == "status":
        return engine.snapshot()
    if args.command == "transport-status":
        return engine.transport_snapshot()
    if args.command == "return-ready":
        return engine.return_ready()
    if args.command == "export-ready":
        return engine.export_ready(args.destination, archive=args.archive)
    if args.command == "set-capacity":
        updates = capacity_updates(args)
        draining: bool | None = None
        if args.drain:
            draining = True
        elif args.resume:
            draining = False
        return engine.set_capacity(draining=draining, **updates)
    if args.command == "ack-return":
        return engine.acknowledge_return(args.engine_job_id, args.receipt_id)
    if args.command == "ack-required":
        return engine.acknowledge_required(args.engine_job_id, args.receipt_id)
    if args.command == "exchange":
        return run_exchange(engine, args)
    raise EngineError(f"unsupported command: {args.command}")


def run_exchange(engine: TestEngine, args: argparse.Namespace) -> dict[str, Any]:
    path_root_raw = getattr(args, "path_root", None)
    path_root = Path(path_root_raw).resolve() if path_root_raw is not None else None
    transport_root = (engine.root / "transport").resolve()
    transport_dir = _exchange_path(args.transport_dir, path_root)
    if transport_dir == transport_root or transport_root not in transport_dir.parents:
        raise EngineError(
            f"exchange transport directory must stay below {transport_root}: {transport_dir}"
        )
    transport_dir.mkdir(parents=True, exist_ok=True)
    observed_at = utc_now()
    timeline: dict[str, Any] = {
        "protocol_version": "engine-exchange-v1",
        "b_request_observed_at": observed_at,
        "pid": os.getpid(),
        "jobs": [],
    }

    engine.initialize()
    timeline["resident_start_started_at"] = utc_now()
    resident = engine.ensure_resident(interval_seconds=0.25)
    timeline["resident_start_finished_at"] = utc_now()

    acknowledged: list[dict[str, Any]] = []
    for raw in args.ack_return:
        job_id, separator, receipt_id = str(raw).partition("=")
        if not separator or not job_id or not receipt_id:
            raise EngineError("--ack-return must be ENGINE_JOB_ID=RECEIPT_ID")
        acknowledged.append(engine.acknowledge_return(job_id, receipt_id))
    timeline["acknowledgements_finished_at"] = utc_now()

    required_acknowledged: list[dict[str, Any]] = []
    for raw in getattr(args, "ack_required", []) or []:
        job_id, separator, receipt_id = str(raw).partition("=")
        if not separator or not job_id or not receipt_id:
            raise EngineError("--ack-required must be ENGINE_JOB_ID=RECEIPT_ID")
        required_acknowledged.append(engine.acknowledge_required(job_id, receipt_id))
    atomic_write_json(
        transport_dir / "required_acknowledgements.json",
        {"acknowledgements": required_acknowledged},
    )
    timeline["required_acknowledgements_finished_at"] = utc_now()

    manifest = read_json(_exchange_path(args.manifest, path_root))
    raw_cancellations = (
        manifest.get("standby_cancellations", []) if isinstance(manifest, dict) else []
    )
    if not isinstance(raw_cancellations, list) or len(raw_cancellations) > 64:
        raise EngineError(
            "exchange manifest standby_cancellations must be a list of at most 64 entries"
        )
    cancellation_results: list[dict[str, Any]] = []
    cancellation_ids: set[str] = set()
    for raw in raw_cancellations:
        if not isinstance(raw, dict):
            raise EngineError("exchange standby cancellation must be an object")
        job_id = normalize_token(
            str(raw.get("engine_job_id") or ""), "engine_job_id"
        )
        if job_id in cancellation_ids:
            raise EngineError(f"duplicate exchange standby cancellation: {job_id}")
        cancellation_ids.add(job_id)
        reason = str(raw.get("reason") or "").strip() or "standby cancelled by controller"
        if len(reason) > 1000:
            raise EngineError(f"exchange standby cancellation reason is too long: {job_id}")
        started_at = utc_now()
        try:
            receipt = engine.cancel_standby(job_id, reason=reason)
            result = {**receipt, "outcome": "cancelled", "error": ""}
        except EngineError as exc:
            result = {
                "engine_job_id": job_id,
                "outcome": "conflict",
                "reason": reason,
                "error": str(exc),
            }
        result.update({"started_at": started_at, "finished_at": utc_now()})
        cancellation_results.append(result)
    atomic_write_json(
        transport_dir / "standby_cancellations.json",
        {"cancellations": cancellation_results},
    )
    timeline["standby_cancellations"] = cancellation_results
    timeline["standby_cancellations_finished_at"] = utc_now()

    updates = capacity_updates(args)
    draining: bool | None = None
    if args.drain:
        draining = True
    elif args.resume:
        draining = False
    engine.set_capacity(draining=draining, **updates)
    timeline["capacity_finished_at"] = utc_now()

    raw_jobs = manifest.get("jobs") if isinstance(manifest, dict) else None
    if not isinstance(raw_jobs, list) or len(raw_jobs) > 64:
        raise EngineError("exchange manifest jobs must be a list of at most 64 entries")
    rejected: list[str] = []
    accepted: list[dict[str, Any]] = []
    staged: list[dict[str, Any]] = []
    for raw in raw_jobs:
        if not isinstance(raw, dict):
            raise EngineError("exchange manifest job must be an object")
        job_id = normalize_token(
            str(raw.get("engine_job_id") or ""), "engine_job_id"
        )
        spec_path = _exchange_path(Path(str(raw.get("spec") or "")), path_root)
        payload_text = str(raw.get("payload_root") or "")
        admission_mode = str(raw.get("admission_mode") or "accept")
        if admission_mode not in {"accept", "standby"}:
            raise EngineError(
                f"unsupported exchange admission_mode for {job_id}: {admission_mode}"
            )
        started_at = utc_now()
        try:
            spec = read_json(spec_path)
            if str(spec.get("engine_job_id") or "") != job_id:
                raise EngineError(
                    f"exchange job id does not match spec: {job_id}"
                )
            payload_root = (
                _exchange_path(Path(payload_text), path_root)
                if payload_text
                else None
            )
            if admission_mode == "standby":
                receipt = engine.stage_standby(spec, payload_root=payload_root)
                if receipt.get("state") == "standby":
                    staged.append(receipt)
                    outcome = "standby"
                else:
                    accepted.append(receipt)
                    outcome = "accepted"
            else:
                receipt = engine.submit(spec, payload_root=payload_root)
                accepted.append(receipt)
                outcome = "accepted"
            error = ""
        except (EngineError, OSError, ValueError) as exc:
            rejected.append(job_id)
            outcome = "rejected"
            error = str(exc)
            (transport_dir / f"rejected_{job_id}.log").write_text(
                error + "\n", encoding="utf-8"
            )
        timeline["jobs"].append(
            {
                "engine_job_id": job_id,
                "started_at": started_at,
                "finished_at": utc_now(),
                "outcome": outcome,
                "admission_mode": admission_mode,
                "error": error,
            }
        )
    (transport_dir / "rejected_jobs.txt").write_text(
        "".join(f"{job_id}\n" for job_id in rejected), encoding="utf-8"
    )
    timeline["admission_finished_at"] = utc_now()

    atomic_write_json(transport_dir / "engine_service_status.json", resident)
    atomic_write_json(
        transport_dir / "engine_runtime.json",
        {
            "python_executable": sys.executable,
            "module_path": str(Path(engine_module.__file__).resolve()),
            "current_code_generation": engine_code_generation(),
            "sys_path": sys.path,
        },
    )
    snapshot, ready, long_poll = wait_for_ready_or_credit(
        engine,
        wait_seconds=max(0.0, float(getattr(args, "wait_ready_seconds", 0.0))),
        control_action_count=(
            len(raw_cancellations)
            + len(acknowledged)
            + len(required_acknowledged)
        ),
        admission_action_count=len(raw_jobs),
    )
    timeline["long_poll"] = long_poll
    timeline["return_export_started_at"] = utc_now()
    exported = engine.export_ready(
        transport_dir / "ready_jobs",
        archive=transport_dir / "ready_jobs.tar",
    )
    timeline["return_export_finished_at"] = utc_now()
    exported_by_id = {
        str(item.get("engine_job_id") or ""): item
        for item in exported.get("jobs", [])
        if isinstance(item, dict) and item.get("engine_job_id")
    }
    ready_by_id = {
        str(item.get("engine_job_id") or ""): item
        for item in ready
        if isinstance(item, dict) and item.get("engine_job_id")
    }
    ready = [
        {
            **exported_item,
            **ready_by_id.get(engine_job_id, {}),
            "return_phase": exported_item["return_phase"],
        }
        for engine_job_id, exported_item in exported_by_id.items()
    ]
    snapshot = engine.transport_snapshot()
    atomic_write_json(transport_dir / "engine_status.json", snapshot)
    atomic_write_json(transport_dir / "return_ready.json", ready)
    atomic_write_json(transport_dir / "ready_export.json", exported)
    timeline["b_exchange_finished_at"] = utc_now()
    atomic_write_json(transport_dir / "exchange_timeline.json", timeline)
    return {
        "accepted_count": len(accepted),
        "standby_count": len(staged),
        "standby_cancellation_count": sum(
            1 for item in cancellation_results if item.get("outcome") == "cancelled"
        ),
        "standby_cancellation_conflict_count": sum(
            1 for item in cancellation_results if item.get("outcome") == "conflict"
        ),
        "rejected_jobs": rejected,
        "return_ready_count": len(ready),
        "snapshot": snapshot,
        "timeline": timeline,
    }


def _exchange_path(path: Path, path_root: Path | None) -> Path:
    candidate = Path(path)
    if path_root is None:
        return candidate.resolve()
    if candidate.is_absolute():
        resolved = candidate.resolve()
    else:
        resolved = (path_root / candidate).resolve()
    if resolved != path_root and path_root not in resolved.parents:
        raise EngineError(f"exchange path escapes trusted root: {path}")
    return resolved


def wait_for_ready_or_credit(
    engine: TestEngine,
    *,
    wait_seconds: float,
    control_action_count: int,
    admission_action_count: int = 0,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    started = time.monotonic()
    snapshot = engine.transport_snapshot()
    ready = engine.return_ready()
    eligible = bool(
        wait_seconds > 0
        and control_action_count == 0
        and admission_action_count == 0
        and not ready
        and int(snapshot.get("accepted_nonterminal", 0) or 0) > 0
        and int(snapshot.get("admission_credit", 0) or 0) <= 0
    )
    deadline = started + wait_seconds
    checks = 1
    while eligible and time.monotonic() < deadline:
        time.sleep(min(0.25, max(0.0, deadline - time.monotonic())))
        ready = engine.return_ready()
        snapshot = engine.transport_snapshot()
        checks += 1
        if ready:
            break
        if int(snapshot.get("admission_credit", 0) or 0) > 0:
            ready = engine.return_ready()
            break
    return (
        snapshot,
        ready,
        {
            "eligible": eligible,
            "configured_seconds": wait_seconds,
            "admission_action_count": admission_action_count,
            "elapsed_seconds": round(time.monotonic() - started, 6),
            "checks": checks,
            "outcome": (
                "ready"
                if ready
                else "credit"
                if int(snapshot.get("admission_credit", 0) or 0) > 0
                else "timeout"
                if eligible
                else "skipped"
            ),
        },
    )


def capacity_updates(args: argparse.Namespace) -> dict[str, Any]:
    inventory_json = getattr(args, "device_inventory_json", None)
    device_inventory = json.loads(inventory_json) if inventory_json else None
    if device_inventory is not None and not isinstance(device_inventory, list):
        raise EngineError("--device-inventory-json must decode to a list")
    return {
        key: value
        for key, value in {
            "max_inflight": getattr(args, "max_inflight", None),
            "standby_slots": getattr(args, "standby_slots", None),
            "active_job_slots": getattr(args, "active_job_slots", None),
            "host_slots": getattr(args, "host_slots", None),
            "host_cpu_weight_capacity": getattr(
                args, "host_cpu_weight_capacity", None
            ),
            "host_memory_mb_capacity": getattr(
                args, "host_memory_mb_capacity", None
            ),
            "host_io_weight_capacity": getattr(
                args, "host_io_weight_capacity", None
            ),
            "cold_build_slots": getattr(args, "cold_build_slots", None),
            "cache_hit_slots": getattr(args, "cache_hit_slots", None),
            "device_slots": getattr(args, "device_slots", None),
            "device_inventory": device_inventory,
            "export_slots": getattr(args, "export_slots", None),
            "return_backlog_soft_limit_bytes": getattr(
                args, "return_backlog_soft_limit_bytes", None
            ),
            "return_backlog_hard_limit_bytes": getattr(
                args, "return_backlog_hard_limit_bytes", None
            ),
            "return_backlog_soft_limit_jobs": getattr(
                args, "return_backlog_soft_limit_jobs", None
            ),
            "return_backlog_hard_limit_jobs": getattr(
                args, "return_backlog_hard_limit_jobs", None
            ),
        }.items()
        if value is not None
    }


if __name__ == "__main__":
    raise SystemExit(main())
