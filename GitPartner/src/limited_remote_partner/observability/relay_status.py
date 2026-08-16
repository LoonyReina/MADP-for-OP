from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def build_snapshot(
    repo_dir: Path,
    output_dir: str = "output",
    *,
    request_id: str = "",
    max_items: int = 100,
) -> dict[str, Any]:
    output_root = (repo_dir / output_dir).resolve()
    rows: list[dict[str, Any]] = []
    parse_errors: list[dict[str, str]] = []
    if output_root.exists():
        for status_path in output_root.rglob("status.json"):
            relative = status_path.relative_to(output_root)
            if "client_output" in relative.parts[:-1]:
                continue
            try:
                raw = json.loads(status_path.read_text(encoding="utf-8-sig"))
                if not isinstance(raw, dict):
                    raise ValueError("status is not a JSON object")
            except Exception as exc:
                parse_errors.append({"path": relative.as_posix(), "error": str(exc)})
                continue
            row = _status_row(raw, relative.parent.as_posix())
            if request_id and row["request_id"] != request_id:
                continue
            rows.append(row)

    rows.sort(key=lambda item: str(item.get("local_updated_at", "")), reverse=True)
    rows = rows[: max(0, max_items)]
    counts = Counter(str(row["state"]) for row in rows)
    anomalies = [
        anomaly
        for row in rows
        for anomaly in _row_anomalies(row)
    ]
    anomalies.extend(
        {"kind": "status-parse-error", **entry} for entry in parse_errors
    )
    return {
        "schema_version": "relay-status-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "repo_dir": str(repo_dir.resolve()),
        "output_dir": output_dir,
        "request_filter": request_id,
        "counts": dict(sorted(counts.items())),
        "requests": rows,
        "anomalies": anomalies,
    }


def _status_row(raw: dict[str, Any], output_subdir: str) -> dict[str, Any]:
    recovery = raw.get("relay_recovery")
    recovery_action = (
        str(recovery.get("action", "")) if isinstance(recovery, dict) else ""
    )
    local_updated_at = str(
        raw.get("return_collected_at")
        or raw.get("relay_status_updated_at")
        or raw.get("finished_at")
        or raw.get("dispatched_at")
        or raw.get("started_at")
        or ""
    )
    observer_clock_offset_seconds = _clock_offset_seconds(local_updated_at)
    observer_age_seconds = (
        max(0.0, observer_clock_offset_seconds)
        if observer_clock_offset_seconds is not None
        and observer_clock_offset_seconds >= -5.0
        else None
    )
    return {
        "request_id": str(raw.get("request_id") or output_subdir),
        "output_subdir": str(raw.get("output_subdir") or output_subdir),
        "state": str(raw.get("state", "unknown")),
        "phase": str(raw.get("phase", "")),
        "transport": str(raw.get("transport", "")),
        "relay_protocol_version": str(raw.get("relay_protocol_version", "")),
        "request_kind": str(raw.get("request_kind", "command")),
        "completion_mode": str(raw.get("completion_mode", "terminal")),
        "engine_job_id": str(raw.get("engine_job_id", "")),
        "client_state": str(raw.get("client_state", "")),
        "recovery_action": recovery_action,
        "dispatched_at": str(raw.get("dispatched_at", "")),
        "client_started_at": str(raw.get("client_started_at", "")),
        "client_finished_at": str(raw.get("client_finished_at", "")),
        "client_returned_at": str(raw.get("client_returned_at", "")),
        "return_collected_at": str(raw.get("return_collected_at", "")),
        "local_updated_at": local_updated_at,
        "timestamp_domain": "A-server",
        "observer_age_seconds": observer_age_seconds,
        "observer_clock_offset_seconds": observer_clock_offset_seconds,
        "history_length": len(raw.get("relay_state_history", []))
        if isinstance(raw.get("relay_state_history"), list)
        else 0,
        "error": str(raw.get("error", "")),
    }


def _row_anomalies(row: dict[str, Any]) -> list[dict[str, Any]]:
    anomalies: list[dict[str, Any]] = []
    state = str(row["state"])
    base = {
        "request_id": row["request_id"],
        "output_subdir": row["output_subdir"],
    }
    if row["transport"] == "relay" and not row["relay_protocol_version"]:
        anomalies.append({"kind": "legacy-relay-status", **base})
    if state == "waiting-client" and not row["dispatched_at"]:
        anomalies.append({"kind": "waiting-without-dispatch-time", **base})
    offset = row.get("observer_clock_offset_seconds")
    if isinstance(offset, (int, float)) and offset < -5.0:
        anomalies.append(
            {
                "kind": "status-clock-ahead-of-observer",
                **base,
                "offset_seconds": offset,
            }
        )
    if row["recovery_action"] == "recovery-failed":
        anomalies.append({"kind": "relay-recovery-failed", **base})
    if state in {"failed", "return_failed", "stalled", "abandoned"}:
        anomalies.append({"kind": f"terminal-{state}", **base, "error": row["error"]})
    return anomalies


def _clock_offset_seconds(value: str) -> float | None:
    if not value:
        return None
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - stamp).total_seconds()


def main() -> None:
    parser = argparse.ArgumentParser(description="read-only A-side relay status snapshot")
    parser.add_argument("--repo-dir", type=Path, default=Path("."))
    parser.add_argument("--output-dir", default="output")
    parser.add_argument("--request-id", default="")
    parser.add_argument("--max-items", type=int, default=100)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    snapshot = build_snapshot(
        args.repo_dir,
        args.output_dir,
        request_id=args.request_id,
        max_items=args.max_items,
    )
    if args.json:
        print(json.dumps(snapshot, ensure_ascii=False, indent=2))
        return
    print(
        "relay status "
        + " ".join(f"{key}={value}" for key, value in snapshot["counts"].items())
    )
    for row in snapshot["requests"]:
        age = row["observer_age_seconds"]
        age_text = "?" if age is None else f"{age:.1f}s"
        print(
            f"{row['request_id']} state={row['state']} client={row['client_state'] or '-'} "
            f"kind={row['request_kind']} engine_job={row['engine_job_id'] or '-'} "
            f"age={age_text} recovery={row['recovery_action'] or '-'}"
        )
    for anomaly in snapshot["anomalies"]:
        print(f"ANOMALY {anomaly['kind']} request={anomaly.get('request_id', '-')}")


if __name__ == "__main__":
    main()
