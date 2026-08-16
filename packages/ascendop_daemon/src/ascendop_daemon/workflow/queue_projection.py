from __future__ import annotations

from pathlib import Path
from typing import Any

from ascendop_daemon.core.atomic_io import write_text_atomic


QUEUE_COLUMNS = (
    "status",
    "op",
    "test_version",
    "vendor",
    "hardware",
    "mode",
    "submitted_by",
    "claimed_by",
    "result",
)
PROJECTION_OWNER = "tester-daemon-v4-db-projection"


def reconcile_terminal_queue_rows(
    root: Path,
    database: Any,
) -> dict[str, Any]:
    """Project settled DB truth into queued Markdown rows without driving work."""

    root = root.resolve()
    queue_path = root / "TestUtils" / "submit" / "queue.md"
    try:
        original = queue_path.read_text(encoding="utf-8-sig")
    except OSError:
        return _report("missing", checked=0, projected=[])
    lines = original.splitlines()
    table = _queue_table(lines)
    if table is None:
        return _report("invalid", checked=0, projected=[])
    header, data_rows = table
    checked = 0
    projected: list[dict[str, Any]] = []
    for line_index, row in data_rows:
        if row.get("status", "").strip().lower() != "queued":
            continue
        operator = row.get("op", "").strip()
        test_version = row.get("test_version", "").strip()
        if not operator or not test_version:
            continue
        checked += 1
        try:
            registration = database.operator_for_display_name(operator)
        except Exception as exc:
            projected.append(
                {
                    "operator": operator,
                    "test_version": test_version,
                    "state": "withheld-registration-unresolved",
                    "error": str(exc),
                    "request_ids": [],
                }
            )
            continue
        operator_id = str(registration.get("operator_id") or "")
        if not operator_id:
            projected.append(
                {
                    "operator": operator,
                    "test_version": test_version,
                    "state": "withheld-registration-unresolved",
                    "error": "registration has no operator_id",
                    "request_ids": [],
                }
            )
            continue
        fact = database.logical_test_request_terminal_projection(
            operator_id,
            test_version,
        )
        if str(fact.get("state") or "") != "terminal":
            continue
        terminal_state = str(fact.get("terminal_state") or "")
        result_path = (
            root / "operators_testresult" / operator / test_version / "RESULT.md"
        )
        if terminal_state == "completed" and not result_path.is_file():
            projected.append(
                {
                    "operator": operator,
                    "test_version": test_version,
                    "state": "withheld-result-missing",
                    "request_ids": list(fact.get("request_ids") or []),
                }
            )
            continue
        row["status"] = "done" if terminal_state == "completed" else "failed"
        row["claimed_by"] = PROJECTION_OWNER
        if result_path.is_file():
            row["result"] = result_path.relative_to(root).as_posix()
        lines[line_index] = _render_row(header, row)
        projected.append(
            {
                "operator": operator,
                "test_version": test_version,
                "state": row["status"],
                "request_ids": list(fact.get("request_ids") or []),
            }
        )
    changed = any(item["state"] in {"done", "failed"} for item in projected)
    if changed:
        rendered = "\n".join(lines) + ("\n" if original.endswith("\n") else "")
        write_text_atomic(queue_path, rendered)
    return _report(
        "projected" if changed else "unchanged",
        checked=checked,
        projected=projected,
    )


def _queue_table(
    lines: list[str],
) -> tuple[list[str], list[tuple[int, dict[str, str]]]] | None:
    header: list[str] | None = None
    separator_seen = False
    rows: list[tuple[int, dict[str, str]]] = []
    for index, raw in enumerate(lines):
        stripped = raw.strip()
        if not stripped.startswith("|") or not stripped.endswith("|"):
            continue
        cells = [cell.strip() for cell in stripped.strip("|").split("|")]
        if header is None:
            if set(cells) != set(QUEUE_COLUMNS) or len(cells) != len(QUEUE_COLUMNS):
                continue
            header = cells
            continue
        if not separator_seen:
            separator_seen = all(cell and set(cell) <= {"-", ":"} for cell in cells)
            if not separator_seen:
                return None
            continue
        if len(cells) == len(header):
            rows.append((index, dict(zip(header, cells))))
    if header is None or not separator_seen:
        return None
    return header, rows


def _render_row(header: list[str], row: dict[str, str]) -> str:
    values = [
        str(row.get(column, "")).replace("\r", " ").replace("\n", " ")
        for column in header
    ]
    return "| " + " | ".join(values) + " |"


def _report(
    state: str,
    *,
    checked: int,
    projected: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "schema": "ascendop.queue-terminal-projection.v1",
        "state": state,
        "checked_count": checked,
        "projected_count": sum(
            1 for item in projected if item["state"] in {"done", "failed"}
        ),
        "rows": projected,
    }
