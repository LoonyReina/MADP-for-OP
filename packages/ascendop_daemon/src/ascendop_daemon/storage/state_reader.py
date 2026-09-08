from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from pathlib import Path

from ascendop_daemon.core.models import BoardRow, BoardSnapshot, DaemonConfig, TransportObservation, config_seasons, extract_row_test_version, observed_operators, operator_season, utc_now_iso
from ascendop_daemon.runtime.workflow_adapter import (
    load_workflow_adapter_module,
    resolve_workflow_adapter,
)
from ascendop_daemon.runtime.process_adapter import workspace_process_environment
from ascendop_daemon.core.models import legacy_board_operators


class StateReader:
    def __init__(self, root: Path, config: DaemonConfig) -> None:
        self.root = root
        self.config = config

    def read(self) -> BoardSnapshot:
        rows: list[BoardRow] = []
        outputs: list[str] = []
        # The workspace projector/standalone completion lane owns V5 correctness.
        # Do not load/hash the legacy adapter or inspect its queues for these ops.
        board_ops = legacy_board_operators(self.config, include_draining=True)
        seasons = tuple(dict.fromkeys(operator_season(self.config, op) for op in board_ops))
        if len(seasons) <= 1:
            season_results = [self._read_season_board(season) for season in seasons]
        else:
            with ThreadPoolExecutor(max_workers=min(4, len(seasons))) as pool:
                season_results = list(pool.map(self._read_season_board, seasons))
        for output, season_rows in season_results:
            outputs.append(output)
            rows.extend(season_rows)
        snapshot_command = ("multi-season-session-board", *seasons)
        snapshot = BoardSnapshot(
            captured_at=utc_now_iso(),
            command=snapshot_command,
            rows=tuple(rows),
            raw_output="\n".join(outputs),
            transport=tuple(self.read_transport_observations(rows)),
        )
        return snapshot

    def _read_season_board(self, season: str) -> tuple[str, list[BoardRow]]:
        season_ops = {
            op
            for op in legacy_board_operators(self.config, include_draining=True)
            if operator_season(self.config, op) == season
        }
        if not season_ops:
            return "", []
        in_process = read_session_board_in_process(
            self.root,
            season=season,
            transport=self.config.transport,
            remote_root=self.config.remote_root,
            include_ops=season_ops,
        )
        if in_process is not None:
            return in_process
        command = (
            sys.executable,
            str(resolve_workflow_adapter(self.root).path),
            "session-board",
            "--season",
            season,
            "--transport",
            self.config.transport,
            "--remote-root",
            self.config.remote_root,
        )
        proc = subprocess.run(
            command,
            cwd=self.root,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
            env=workspace_process_environment(self.root),
            creationflags=process_creation_flags(),
            startupinfo=process_startupinfo(),
        )
        if proc.returncode != 0:
            raise RuntimeError(
                f"session-board failed for {season} with {proc.returncode}:\n{proc.stdout}"
            )
        return proc.stdout, parse_session_board(proc.stdout, include_ops=season_ops)

    def read_transport_observations(self, rows: list[BoardRow]) -> list[TransportObservation]:
        observations: list[TransportObservation] = []
        seen: set[tuple[str, str, str]] = set()
        for row in rows:
            test_version = extract_row_test_version(row)
            if not test_version:
                continue
            heartbeat = self._read_heartbeat(row.op, test_version)
            request_id = str(heartbeat.get("request_id", "")) if heartbeat else ""
            status_path = self._find_output_status(row.op, test_version, request_id)
            status = read_json(status_path) if status_path else {}
            if not request_id and status:
                request_id = str(status.get("request_id", "") or status_path.parent.name)
            if not heartbeat and not status:
                continue
            key = (row.op, test_version, request_id)
            if key in seen:
                continue
            seen.add(key)
            observations.append(
                TransportObservation(
                    op=row.op,
                    test_version=test_version,
                    request_id=request_id,
                    heartbeat_path=relpath(self._heartbeat_path(row.op, test_version), self.root)
                    if self._heartbeat_path(row.op, test_version).exists()
                    else "",
                    output_status_path=relpath(status_path, self.root) if status_path else "",
                    state=str(status.get("state") or heartbeat.get("state") or ""),
                    client_state=str(
                        status.get("client_state") or heartbeat.get("client_state") or ""
                    ),
                    client_updated_at=str(
                        status.get("client_updated_at")
                        or heartbeat.get("client_updated_at")
                        or ""
                    ),
                    client_progress_observed_at=str(
                        status.get("client_progress_observed_at")
                        or heartbeat.get("client_progress_observed_at")
                        or ""
                    ),
                    terminal=status_terminal(status),
                    stalled=bool(heartbeat.get("stalled")) if heartbeat else None,
                    remote_feedback_status=str(heartbeat.get("remote_feedback_status", "")) if heartbeat else "",
                    stall_reason=str(heartbeat.get("stall_reason", "")) if heartbeat else "",
                    first_observed_at_utc=str(heartbeat.get("first_observed_at_utc", "")) if heartbeat else "",
                    observed_at_utc=str(heartbeat.get("observed_at_utc", "")) if heartbeat else "",
                    last_feedback_at_utc=str(
                        heartbeat.get("last_feedback_at_utc")
                        or status.get("finished_at")
                        or status.get("updated_at")
                        or status.get("started_at")
                        or ""
                    ),
                    elapsed_without_feedback_seconds=optional_int(
                        heartbeat.get("elapsed_without_feedback_seconds")
                    )
                    if heartbeat
                    else None,
                    elapsed_without_remote_feedback_seconds=optional_int(
                        heartbeat.get("elapsed_without_remote_feedback_seconds")
                    )
                    if heartbeat
                    else None,
                    relay_publish_verify=str(status.get("relay_publish_verify", "")) if status else "",
                    client_ssh=str(status.get("client_ssh", "")) if status else "",
                )
            )
        return observations

    def _read_heartbeat(self, op: str, test_version: str) -> dict[str, Any]:
        path = self._heartbeat_path(op, test_version)
        data = read_json(path)
        return data if isinstance(data, dict) else {}

    def _heartbeat_path(self, op: str, test_version: str) -> Path:
        submit_path = self.root / "TestUtils" / "submit" / op / test_version / "GITPARTNER_HEARTBEAT.json"
        if submit_path.exists():
            return submit_path
        result_path = (
            self.root
            / "operators_testresult"
            / op
            / test_version
            / "submit_snapshot"
            / "GITPARTNER_HEARTBEAT.json"
        )
        return result_path

    def _find_output_status(self, op: str, test_version: str, request_id: str) -> Path | None:
        output_root = self.root / "GitPartner" / "output"
        candidates: list[Path] = []
        if request_id:
            exact = output_root / request_id / "status.json"
            return exact if exact.exists() else None
        if output_root.exists():
            pattern = f"{test_version}*"
            candidates.extend(path / "status.json" for path in output_root.glob(pattern) if path.is_dir())
            op_pattern = f"{op}_{test_version.split('_', 1)[-1]}*"
            candidates.extend(path / "status.json" for path in output_root.glob(op_pattern) if path.is_dir())
        existing = [path for path in candidates if path.exists()]
        if not existing:
            return None
        return max(existing, key=lambda path: path.stat().st_mtime)


def parse_session_board(output: str, include_ops: set[str] | None = None) -> list[BoardRow]:
    rows: list[BoardRow] = []
    for line in output.splitlines():
        stripped = line.strip()
        if not stripped.startswith("|"):
            continue
        if stripped.startswith("|---") or stripped.startswith("| season "):
            continue
        cells = [cell.strip() for cell in stripped.strip("|").split("|")]
        if len(cells) < 8:
            continue
        season, op, gate_stage, next_owner, solver_goal, tester_goal, wakeups = cells[:7]
        next_command = "|".join(cells[7:]).strip()
        if include_ops is not None and op not in include_ops:
            continue
        rows.append(
            BoardRow(
                season=season,
                op=op,
                gate_stage=gate_stage,
                next_owner=next_owner,
                solver_goal=solver_goal,
                tester_goal=tester_goal,
                wakeups=wakeups,
                next_command=next_command,
                action_descriptor={},
            )
        )
    return rows


def read_session_board_in_process(
    root: Path,
    *,
    season: str,
    transport: str,
    remote_root: str,
    include_ops: set[str],
) -> tuple[str, list[BoardRow]] | None:
    try:
        next_workflow = load_workflow_adapter_module(root)
    except RuntimeError:
        return None
    args = argparse.Namespace(
        season=season,
        op=None,
        test_version=None,
        case_version=None,
        vendor=None,
        hardware="910B4",
        mode="correct",
        transport=transport,
        remote_root=remote_root,
        strict=False,
    )
    try:
        raw_rows = next_workflow.session_board_rows(args)
    except SystemExit as exc:
        raise RuntimeError(
            f"in-process session-board failed for {season}: {exc}"
        ) from exc
    output = next_workflow.session_board_table_text(raw_rows)
    rows = [
        BoardRow(
            season=str(row.get("season", "")),
            op=str(row.get("op", "")),
            gate_stage=str(row.get("gate_stage", "")),
            next_owner=str(row.get("next_owner", "")),
            solver_goal=str(row.get("solver_goal", "")),
            tester_goal=str(row.get("tester_goal", "")),
            wakeups=str(row.get("wakeups", "")),
            next_command=str(row.get("next_command", "")),
            action_descriptor=(
                dict(row.get("action_descriptor", {}))
                if isinstance(row.get("action_descriptor"), dict)
                else {}
            ),
        )
        for row in raw_rows
        if str(row.get("op", "")) in include_ops
    ]
    return output, rows


def read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def status_terminal(status: dict[str, Any]) -> bool | None:
    if not status:
        return None
    return str(status.get("state", "")).lower() in {"success", "failed", "failure", "error", "cancelled", "timeout"}


def optional_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def relpath(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def process_creation_flags() -> int:
    if os.name != "nt":
        return 0
    return getattr(subprocess, "CREATE_NO_WINDOW", 0)


def process_startupinfo() -> subprocess.STARTUPINFO | None:
    if os.name != "nt":
        return None
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startupinfo.wShowWindow = 0
    return startupinfo


def background_python_executable() -> str:
    executable = Path(sys.executable)
    if os.name == "nt" and executable.name.lower() == "python.exe":
        pythonw = executable.with_name("pythonw.exe")
        if pythonw.exists():
            return str(pythonw)
    return sys.executable
