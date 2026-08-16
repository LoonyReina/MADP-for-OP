from __future__ import annotations

import hashlib
import json
from typing import Any


def terminal_failure_evidence(
    terminal: dict[str, Any],
    *,
    operator: str,
    case_version: str,
    blocker_generation: str,
    target_version: str,
    engine_job_id: str,
) -> dict[str, Any]:
    primary = terminal.get("primary_failure")
    primary = primary if isinstance(primary, dict) else {}
    stage_name = str(
        primary.get("stage_name")
        or terminal.get("failed_stage")
        or terminal.get("stage_name")
        or "unknown"
    )
    error = str(
        primary.get("error")
        or terminal.get("error")
        or terminal.get("failure_code")
        or "profiler execution failed"
    )
    terminal_digest = hashlib.sha256(
        json.dumps(
            terminal,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    return {
        "protocol_version": "ascendop-profiler-evidence-v1",
        "status": "failed",
        "operator": operator,
        "case_version": case_version,
        "blocker_generation": blocker_generation,
        "target_version": target_version,
        "engine_job_id": engine_job_id,
        "runs": [],
        "error": (
            f"diagnostic terminal state: failed; stage={stage_name}; error={error}"
        ),
        "failure": {
            "origin": "engine-terminal-derived",
            "terminal_digest": terminal_digest,
            "terminal_revision": int(terminal.get("terminal_revision", 0) or 0),
            "failure_domain": str(terminal.get("failure_domain") or ""),
            "failure_code": str(terminal.get("failure_code") or ""),
            "stage_name": stage_name,
            "retryable": bool(terminal.get("retryable", False)),
            "first_failure": primary,
        },
    }
