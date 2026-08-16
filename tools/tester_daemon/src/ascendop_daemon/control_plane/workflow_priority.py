from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ascendop_daemon.core.models import DaemonConfig


def load_operator_flow_priorities(
    root: Path,
    config: DaemonConfig,
) -> dict[str, int]:
    """Read optional, season-owned operator focus without hard-coding names."""

    relative = str(config.policy.get("operator_priority_plan") or "").strip()
    if not relative:
        return {}
    path = Path(relative)
    if not path.is_absolute():
        path = root / path
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    operators = payload.get("operators", {})
    if not isinstance(operators, dict):
        return {}

    priorities: dict[str, int] = {}
    for op in config.operators:
        checkpoint = operators.get(op)
        if not isinstance(checkpoint, dict):
            continue
        try:
            priority = max(0, int(checkpoint.get("flow_priority", 0) or 0))
        except (TypeError, ValueError):
            continue
        priorities[op] = priority
    return priorities


def flow_priority(
    operator_priorities: dict[str, Any] | None,
    op: str,
) -> int:
    if not isinstance(operator_priorities, dict):
        return 0
    try:
        return max(0, int(operator_priorities.get(op, 0) or 0))
    except (TypeError, ValueError):
        return 0
