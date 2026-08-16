from __future__ import annotations

import json
from pathlib import Path
from typing import Any


CORE_RESIDENT_CHILDREN = ("daemon", "control-api", "official-eval")


def cli_runner_enabled(config: Path) -> bool:
    raw = _read_config(config)
    execution = raw.get("agent_execution", {})
    if not isinstance(execution, dict):
        raise RuntimeError("agent_execution must be an object")
    cli_runner = execution.get("cli_runner", {})
    if not isinstance(cli_runner, dict):
        raise RuntimeError("agent_execution.cli_runner must be an object")
    return bool(cli_runner.get("enabled", False))


def resident_child_names(config: Path) -> tuple[str, ...]:
    children = ["daemon", "control-api"]
    if cli_runner_enabled(config):
        children.append("agent-runner")
    children.append("official-eval")
    return tuple(children)


def _read_config(config: Path) -> dict[str, Any]:
    try:
        raw = json.loads(config.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"resident Agent execution config is invalid: {config}"
        ) from exc
    if not isinstance(raw, dict):
        raise RuntimeError("resident config must be an object")
    return raw
