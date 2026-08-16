from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from limited_remote_partner.gateway import submit_job


def load_commands(path: Path) -> list[list[str]]:
    payload: Any = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise ValueError("batch payload must be an object")
    if str(payload.get("schema") or "") != "gitpartner.submit-batch.v1":
        raise ValueError("unsupported submit batch schema")
    raw_commands = payload.get("commands")
    if not isinstance(raw_commands, list) or not raw_commands:
        raise ValueError("submit batch commands must be a non-empty list")

    commands: list[list[str]] = []
    for index, raw in enumerate(raw_commands):
        if (
            not isinstance(raw, list)
            or not raw
            or any(not isinstance(item, str) or not item for item in raw)
        ):
            raise ValueError(
                f"submit batch command {index} must be a non-empty string list"
            )
        commands.append(list(raw))
    return commands


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Run multiple GitPartner submit argument vectors in one process"
    )
    parser.add_argument("--commands-file", required=True)
    args = parser.parse_args(argv)

    for command in load_commands(Path(args.commands_file).resolve()):
        submit_job.main(command)


if __name__ == "__main__":
    main()
