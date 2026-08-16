from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any


class ControlDatabaseInstallError(RuntimeError):
    pass


def _python_import_path(path: Path) -> str:
    value = str(path.resolve())
    if os.name != "nt" or value.startswith("\\\\?\\"):
        return value
    if value.startswith("\\\\"):
        return "\\\\?\\UNC\\" + value[2:]
    return "\\\\?\\" + value


def migrate_control_database(
    *,
    daemon_source: Path,
    protocol_source: Path,
    control_source: Path,
    agent_runner_source: Path,
    database: Path,
    expected_schema: int,
    release_generation: str,
    timeout_seconds: int,
) -> dict[str, Any]:
    environment = os.environ.copy()
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment["PYTHONPATH"] = os.pathsep.join(
        item
        for item in (
            _python_import_path(daemon_source),
            _python_import_path(protocol_source),
            _python_import_path(control_source),
            _python_import_path(agent_runner_source),
            environment.get("PYTHONPATH", ""),
        )
        if item
    )
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "ascendop_daemon.cli.database_migration",
            "--database",
            str(database.resolve()),
            "--expected-schema",
            str(expected_schema),
            "--release-generation",
            release_generation,
        ],
        cwd=str(database.resolve().parent),
        env=environment,
        capture_output=True,
        text=True,
        timeout=max(30, int(timeout_seconds)),
        creationflags=(
            int(getattr(subprocess, "CREATE_NO_WINDOW", 0))
            if os.name == "nt"
            else 0
        ),
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        raise ControlDatabaseInstallError(
            "control database hard-cut migration failed with "
            f"{completed.returncode}: {detail[:1024]}"
        )
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise ControlDatabaseInstallError(
            "control database migration returned invalid JSON"
        ) from exc
    if (
        not isinstance(value, dict)
        or value.get("schema") != "ascendop.control-database-migration.v4"
        or int(value.get("database_schema") or 0) != int(expected_schema)
        or value.get("release_generation") != release_generation
    ):
        raise ControlDatabaseInstallError(
            "control database migration returned an incompatible receipt"
        )
    return value
