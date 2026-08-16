from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from limited_remote_partner.engine.test_engine import TestEngine


CANARY_SCHEMA = "gitpartner.distributed-canary-result.v2"
ENGINE_TASK_CLASSES = {"engine-host-canary", "engine-device-canary"}
TASK_CLASSES = {"control-probe", "host-only-canary", *ENGINE_TASK_CLASSES}


def run_canary(
    *,
    output: Path,
    identity: dict[str, Any],
    task_class: str,
    payload: Path | None = None,
    synthetic_duration_ms: int = 0,
    host_duration_ms: int = 0,
    device_duration_ms: int = 0,
    export_duration_ms: int = 0,
    failure_mode: str = "none",
    engine_root: Path | None = None,
    timeout_seconds: float = 120.0,
) -> dict[str, Any]:
    if task_class not in TASK_CLASSES:
        raise ValueError(f"unsupported distributed canary task_class: {task_class}")
    if failure_mode not in {"none", "fail-before-result", "fail-after-result"}:
        raise ValueError(f"unsupported distributed canary failure mode: {failure_mode}")
    durations = {
        "synthetic": nonnegative_milliseconds(synthetic_duration_ms, "synthetic"),
        "host": nonnegative_milliseconds(host_duration_ms, "host"),
        "device": nonnegative_milliseconds(device_duration_ms, "device"),
        "export": nonnegative_milliseconds(export_duration_ms, "export"),
    }
    normalized_identity = {str(key): value for key, value in identity.items()}
    normalized_identity["task_class"] = task_class
    normalized_identity["workflow_ingest"] = False
    started_at = utc_now()

    engine_evidence: dict[str, Any] = {}
    if task_class in ENGINE_TASK_CLASSES:
        if engine_root is None:
            raise ValueError(f"{task_class} requires engine_root")
        engine_evidence = run_engine_canary(
            engine_root=engine_root,
            identity=normalized_identity,
            task_class=task_class,
            durations=durations,
            timeout_seconds=timeout_seconds,
        )
    else:
        time.sleep(durations["synthetic"] / 1000.0)

    if failure_mode == "fail-before-result":
        raise RuntimeError("injected distributed canary failure before result")

    data = payload.read_bytes() if payload and payload.is_file() else b""
    receipt_id = deterministic_receipt_id(normalized_identity)
    result = {
        **normalized_identity,
        "schema": CANARY_SCHEMA,
        "receipt_id": receipt_id,
        "outcome": "success",
        "payload_bytes": len(data),
        "payload_sha256": hashlib.sha256(data).hexdigest(),
        "started_at": started_at,
        "finished_at": utc_now(),
        "engine": engine_evidence,
    }
    atomic_write_json(output, result)
    if failure_mode == "fail-after-result":
        raise RuntimeError("injected distributed canary failure after result")
    return result


def run_engine_canary(
    *,
    engine_root: Path,
    identity: dict[str, Any],
    task_class: str,
    durations: dict[str, int],
    timeout_seconds: float,
) -> dict[str, Any]:
    engine = TestEngine(engine_root.resolve())
    engine.initialize()
    resident = engine.ensure_resident(interval_seconds=0.05)
    attempt_id = str(identity.get("attempt_id") or "")
    request_id = str(identity.get("request_id") or "")
    suffix = hashlib.sha256(
        f"{request_id}:{attempt_id}".encode("utf-8")
    ).hexdigest()[:20]
    engine_job_id = f"canary-{suffix}"
    host_ms = durations["host"] or durations["synthetic"]
    device_ms = durations["device"] or durations["synthetic"]
    export_ms = durations["export"]
    stages = [
        sleep_stage(
            "host-prepare",
            "host",
            host_ms,
            marker="host.json",
            pre_activation=True,
        )
    ]
    if task_class == "engine-device-canary":
        stages.append(
            sleep_stage(
                "device-execute",
                "device",
                device_ms,
                marker="device.json",
                locks=["npu", "performance-measurement"],
                depends_on=["host-prepare"],
            )
        )
        export_depends = ["device-execute"]
    else:
        export_depends = ["host-prepare"]
    stages.append(
        sleep_stage(
            "postprocess",
            "export",
            export_ms,
            marker="export.json",
            depends_on=export_depends,
        )
    )
    spec = {
        "protocol_version": "engine-v3",
        "request_id": request_id,
        "engine_job_id": engine_job_id,
        "attempt_id": attempt_id,
        "operator": "distributed-canary",
        "test_version": str(identity.get("experiment_id") or "canary"),
        "bundle_hash": hashlib.sha256(b"").hexdigest(),
        "execution_profile": task_class,
        "scheduler_policy": {
            "queue_preactivation": "enabled",
            "measurement_preactivation_overlap": "enabled",
            "profile_export_capture_overlap": "disabled",
        },
        "stages": stages,
        "required_artifacts": [],
        "optional_artifacts": [],
    }
    accepted = engine.submit(spec)
    deadline = time.monotonic() + max(1.0, float(timeout_seconds))
    terminal: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        snapshot = engine.snapshot()
        terminal = next(
            (
                item
                for item in snapshot.get("jobs", [])
                if item.get("engine_job_id") == engine_job_id
                and item.get("state") in {"completed", "failed"}
            ),
            None,
        )
        if terminal is not None:
            break
        resident_now = engine.resident_status()
        if not resident_now.get("resident_ok", False):
            resident = engine.ensure_resident(interval_seconds=0.05)
        time.sleep(0.02)
    if terminal is None:
        raise TimeoutError(f"engine canary timed out: {engine_job_id}")
    if terminal.get("state") != "completed":
        raise RuntimeError(
            f"engine canary failed: {terminal.get('error') or engine_job_id}"
        )
    ready = next(
        (
            item
            for item in engine.return_ready()
            if item.get("engine_job_id") == engine_job_id
        ),
        None,
    )
    if ready is None:
        raise RuntimeError(f"engine canary return manifest missing: {engine_job_id}")
    return_receipt = engine.acknowledge_return(
        engine_job_id,
        deterministic_receipt_id(identity),
    )
    history = terminal.get("history", [])
    if not isinstance(history, list):
        history = []
    capacity = engine.snapshot().get("capacity", {})
    environment_id = str(identity.get("target_environment_id") or "")
    cache_identity = hashlib.sha256(
        f"{environment_id}:{engine.root}:cache".encode("utf-8")
    ).hexdigest()
    measurement_identity = hashlib.sha256(
        (
            f"{request_id}:{attempt_id}:{engine_job_id}:"
            f"{accepted.get('engine_code_generation', '')}"
        ).encode("utf-8")
    ).hexdigest()
    return {
        "engine_job_id": engine_job_id,
        "engine_generation": str(engine.snapshot().get("engine_generation") or ""),
        "engine_code_generation": str(
            accepted.get("engine_code_generation") or ""
        ),
        "execution_environment_id": environment_id,
        "scheduler_policy": dict(accepted.get("scheduler_policy") or {}),
        "capacity": dict(capacity) if isinstance(capacity, dict) else {},
        "resident": {
            key: resident.get(key)
            for key in (
                "pid",
                "resident_ok",
                "code_generation",
                "code_generation_current",
            )
        },
        "device_lease_resources": sorted(
            {
                str(lock)
                for item in history
                if isinstance(item, dict)
                for lock in item.get(
                    "runtime_stage_locks", item.get("stage_locks", [])
                )
                if str(lock).startswith("npu:")
            }
        ),
        "cache_identity": cache_identity,
        "measurement_identity": measurement_identity,
        "stage_history": history,
        "return_receipt_id": str(
            return_receipt.get("return_receipt_id") or ""
        ),
        "return_independent": True,
    }


def sleep_stage(
    name: str,
    resource: str,
    duration_ms: int,
    *,
    marker: str,
    locks: list[str] | None = None,
    depends_on: list[str] | None = None,
    pre_activation: bool = False,
) -> dict[str, Any]:
    script = (
        "import json,time;"
        "from datetime import datetime,timezone;"
        "from pathlib import Path;"
        "started=datetime.now(timezone.utc).isoformat();"
        f"time.sleep({duration_ms / 1000.0!r});"
        f"path=Path('result_bundle/{marker}');"
        "path.parent.mkdir(parents=True,exist_ok=True);"
        "path.write_text(json.dumps({'started_at':started,"
        "'finished_at':datetime.now(timezone.utc).isoformat()})+'\\n',"
        "encoding='utf-8')"
    )
    stage = {
        "name": name,
        "resource": resource,
        "locks": list(locks or []),
        "pre_activation": pre_activation,
        "command": [sys.executable, "-c", script],
        "working_dir": ".",
        "max_attempts": 2 if resource != "device" else 1,
    }
    if depends_on is not None:
        stage["depends_on"] = list(depends_on)
    return stage


def deterministic_receipt_id(identity: dict[str, Any]) -> str:
    value = (
        f"{identity.get('target_endpoint_id', '')}:"
        f"{identity.get('request_id', '')}:"
        f"{identity.get('attempt_id', '')}"
    )
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]


def nonnegative_milliseconds(value: int, label: str) -> int:
    normalized = int(value)
    if normalized < 0:
        raise ValueError(f"{label} duration cannot be negative")
    return normalized


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}.{time.time_ns()}")
    temporary.write_text(
        json.dumps(value, ensure_ascii=True, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--identity-json", required=True)
    parser.add_argument("--task-class", choices=sorted(TASK_CLASSES), required=True)
    parser.add_argument("--payload", type=Path)
    parser.add_argument("--synthetic-duration-ms", type=int, default=0)
    parser.add_argument("--host-duration-ms", type=int, default=0)
    parser.add_argument("--device-duration-ms", type=int, default=0)
    parser.add_argument("--export-duration-ms", type=int, default=0)
    parser.add_argument(
        "--failure-mode",
        choices=("none", "fail-before-result", "fail-after-result"),
        default="none",
    )
    parser.add_argument("--engine-root", type=Path)
    parser.add_argument("--timeout-seconds", type=float, default=120.0)
    args = parser.parse_args()
    identity = json.loads(args.identity_json)
    if not isinstance(identity, dict):
        raise SystemExit("--identity-json must be an object")
    run_canary(
        output=args.output,
        identity=identity,
        task_class=args.task_class,
        payload=args.payload,
        synthetic_duration_ms=args.synthetic_duration_ms,
        host_duration_ms=args.host_duration_ms,
        device_duration_ms=args.device_duration_ms,
        export_duration_ms=args.export_duration_ms,
        failure_mode=args.failure_mode,
        engine_root=args.engine_root,
        timeout_seconds=args.timeout_seconds,
    )


if __name__ == "__main__":
    main()
