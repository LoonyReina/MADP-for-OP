#!/usr/bin/env python3
"""One-shot dynamic-plugin handoff acceptance watcher.

This process observes daemon-owned metrics, performs one supported operator
enable transition, writes durable evidence, and exits. It never delivers IDE
messages or mutates queue/result archives.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[5]
STATE_DIR = ROOT / "TestUtils" / "tester_daemon"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temp, path)


def gap_snapshot(required: int, threshold_seconds: int) -> dict[str, Any]:
    efficiency = read_json(STATE_DIR / "test_efficiency.json")
    metric = efficiency.get("completion_to_next_submit", {})
    metric = metric if isinstance(metric, dict) else {}
    raw_gaps = metric.get("gaps", [])
    gaps = [item for item in raw_gaps if isinstance(item, dict)] if isinstance(raw_gaps, list) else []
    latest = gaps[:required]
    seconds = [int(item.get("gap_seconds", -1) or 0) for item in latest]
    return {
        "captured_at": now_iso(),
        "metric_epoch_at": str(metric.get("metric_epoch_at", "") or ""),
        "active_operator_count": int(metric.get("active_operator_count", 0) or 0),
        "sample_count": int(metric.get("sample_count", 0) or 0),
        "required_consecutive": required,
        "threshold_seconds": threshold_seconds,
        "latest_gaps": latest,
        "latest_gap_seconds": seconds,
        "passed": len(latest) == required and all(0 <= value < threshold_seconds for value in seconds),
    }


def plugin_snapshot() -> dict[str, Any]:
    state = read_json(STATE_DIR / "operator_plugin_state.json")
    active = state.get("active_operators", [])
    return {
        "generation": int(state.get("generation", 0) or 0),
        "metrics_epoch_at": str(state.get("metrics_epoch_at", "") or ""),
        "active_operators": [str(value) for value in active] if isinstance(active, list) else [],
        "draining_operators": list(state.get("draining_operators", []) or []),
    }


def run_enable(config: Path, op: str) -> dict[str, Any]:
    command = [
        sys.executable,
        str(ROOT / "tools" / "tester_daemon" / "daemon.py"),
        "operator-plugin",
        "--config",
        str(config),
        "--op",
        op,
        "--enable",
    ]
    flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    completed = subprocess.run(
        command,
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=120,
        creationflags=flags,
        check=False,
    )
    parsed: dict[str, Any] = {}
    try:
        value = json.loads(completed.stdout)
        parsed = value if isinstance(value, dict) else {}
    except json.JSONDecodeError:
        pass
    return {
        "time": now_iso(),
        "returncode": completed.returncode,
        "stdout": completed.stdout.strip(),
        "stderr": completed.stderr.strip(),
        "result": parsed,
    }


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Dynamic Plugin Handoff Acceptance",
        "",
        f"- status: {report.get('status', '')}",
        f"- phase: {report.get('phase', '')}",
        f"- updated_at: {report.get('updated_at', '')}",
        f"- toggle_op: {report.get('toggle_op', '')}",
        f"- requirement: {report.get('required_consecutive', 0)} consecutive gaps < {report.get('threshold_seconds', 0)}s",
        "",
    ]
    for key, title in (
        ("with_op_before_drain", "With Op Before Drain"),
        ("without_op", "Without Op"),
        ("with_op_after_reinsert", "With Op After Reinsert"),
    ):
        stage = report.get(key, {})
        stage = stage if isinstance(stage, dict) else {}
        evidence = stage.get("evidence", {})
        evidence = evidence if isinstance(evidence, dict) else {}
        lines.extend(
            [
                f"## {title}",
                "",
                f"- generation: {stage.get('generation', '-')}",
                f"- passed: {stage.get('passed', False)}",
                f"- gap_seconds: {evidence.get('latest_gap_seconds', stage.get('gap_seconds', []))}",
                f"- metric_epoch_at: {evidence.get('metric_epoch_at', stage.get('metric_epoch_at', ''))}",
                "",
            ]
        )
    continuity = report.get("candidate_continuity", [])
    if isinstance(continuity, list) and continuity:
        lines.extend(["## Candidate Continuity", ""])
        lines.extend(f"- {item}" for item in continuity)
        lines.append("")
    if report.get("last_error"):
        lines.extend(["## Last Error", "", str(report["last_error"]), ""])
    return "\n".join(lines)


def persist(report_path: Path, report: dict[str, Any]) -> None:
    report["updated_at"] = now_iso()
    write_json_atomic(report_path, report)
    report_path.with_suffix(".md").write_text(render_markdown(report), encoding="utf-8")


def process_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def acquire_lock(path: Path) -> None:
    if path.exists():
        existing = read_json(path)
        if process_alive(int(existing.get("pid", 0) or 0)):
            raise RuntimeError(f"acceptance watcher already running pid={existing.get('pid')}")
        path.unlink(missing_ok=True)
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump({"pid": os.getpid(), "started_at": now_iso()}, handle)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--toggle-op", required=True)
    parser.add_argument("--required-consecutive", type=int, default=3)
    parser.add_argument("--threshold-seconds", type=int, default=10)
    parser.add_argument("--poll-seconds", type=int, default=30)
    parser.add_argument("--timeout-seconds", type=int, default=21600)
    parser.add_argument("--report", type=Path, default=STATE_DIR / "DYNAMIC_PLUGIN_ACCEPTANCE.json")
    parser.add_argument("--baseline-gap", action="append", type=int, default=[])
    parser.add_argument("--baseline-record", action="append", default=[])
    parser.add_argument("--continuity-evidence", action="append", default=[])
    parser.add_argument("--reset", action="store_true")
    args = parser.parse_args()

    config = args.config if args.config.is_absolute() else ROOT / args.config
    report_path = args.report if args.report.is_absolute() else ROOT / args.report
    lock_path = STATE_DIR / "dynamic_plugin_acceptance.lock.json"
    acquire_lock(lock_path)
    deadline = time.monotonic() + max(60, args.timeout_seconds)
    try:
        if args.reset or not report_path.exists():
            initial_plugin = plugin_snapshot()
            baseline = list(args.baseline_gap)[-args.required_consecutive :]
            report: dict[str, Any] = {
                "schema_version": 1,
                "status": "running",
                "phase": "without_op",
                "started_at": now_iso(),
                "toggle_op": args.toggle_op,
                "required_consecutive": args.required_consecutive,
                "threshold_seconds": args.threshold_seconds,
                "candidate_continuity": list(args.continuity_evidence),
                "with_op_before_drain": {
                    "passed": len(baseline) == args.required_consecutive
                    and all(0 <= value < args.threshold_seconds for value in baseline),
                    "gap_seconds": baseline,
                    "records": list(args.baseline_record),
                },
                "without_op": {
                    "generation": initial_plugin["generation"],
                    "metric_epoch_at": initial_plugin["metrics_epoch_at"],
                    "passed": False,
                },
                "with_op_after_reinsert": {"passed": False},
                "events": [],
            }
        else:
            report = read_json(report_path)

        initial = plugin_snapshot()
        if args.toggle_op in initial["active_operators"] and not report.get("without_op", {}).get("passed"):
            raise RuntimeError(f"{args.toggle_op} must be drained before starting the without-op phase")
        persist(report_path, report)

        while time.monotonic() < deadline:
            plugin = plugin_snapshot()
            phase = str(report.get("phase", "without_op") or "without_op")
            evidence = gap_snapshot(args.required_consecutive, args.threshold_seconds)

            if phase == "without_op":
                expected_generation = int(report.get("without_op", {}).get("generation", 0) or 0)
                if args.toggle_op in plugin["active_operators"]:
                    raise RuntimeError(f"{args.toggle_op} reappeared before without-op acceptance passed")
                if plugin["generation"] != expected_generation:
                    raise RuntimeError(
                        f"operator generation changed unexpectedly: expected={expected_generation} actual={plugin['generation']}"
                    )
                report["without_op"]["evidence"] = evidence
                if evidence["passed"]:
                    report["without_op"]["passed"] = True
                    report["without_op"]["passed_at"] = now_iso()
                    enable = run_enable(config, args.toggle_op)
                    report["events"].append({"event": "operator_enable", **enable})
                    if enable["returncode"] != 0:
                        raise RuntimeError(f"operator enable failed: {enable['stderr'] or enable['stdout']}")
                    enabled_plugin = plugin_snapshot()
                    if args.toggle_op not in enabled_plugin["active_operators"]:
                        raise RuntimeError(f"operator enable returned success but {args.toggle_op} is not active")
                    report["phase"] = "with_op_after_reinsert"
                    report["with_op_after_reinsert"] = {
                        "generation": enabled_plugin["generation"],
                        "metric_epoch_at": enabled_plugin["metrics_epoch_at"],
                        "passed": False,
                    }
            elif phase == "with_op_after_reinsert":
                expected_generation = int(report.get("with_op_after_reinsert", {}).get("generation", 0) or 0)
                if args.toggle_op not in plugin["active_operators"]:
                    raise RuntimeError(f"{args.toggle_op} disappeared during reinserted acceptance phase")
                if plugin["generation"] != expected_generation:
                    raise RuntimeError(
                        f"operator generation changed unexpectedly: expected={expected_generation} actual={plugin['generation']}"
                    )
                report["with_op_after_reinsert"]["evidence"] = evidence
                if evidence["passed"]:
                    report["with_op_after_reinsert"]["passed"] = True
                    report["with_op_after_reinsert"]["passed_at"] = now_iso()
                    report["phase"] = "complete"
                    report["status"] = "passed"
                    persist(report_path, report)
                    return 0
            else:
                return 0 if report.get("status") == "passed" else 1

            persist(report_path, report)
            time.sleep(max(1, args.poll_seconds))

        report["status"] = "timeout"
        report["last_error"] = f"acceptance timeout after {args.timeout_seconds}s"
        persist(report_path, report)
        return 2
    except Exception as exc:  # noqa: BLE001 - watcher must persist every failure
        report = locals().get("report", {})
        if not isinstance(report, dict):
            report = {}
        report.update({"status": "failed", "phase": report.get("phase", "startup"), "last_error": str(exc)})
        persist(report_path, report)
        return 1
    finally:
        lock_path.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
