from __future__ import annotations

import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any


_CSV_TIMESTAMP_RE = re.compile(r"_(\d{17})\.csv$", re.IGNORECASE)
_MSPROF_LAUNCH_RE = re.compile(
    r"<ProfData>\s+Start profiling.*?kernel:\s*(\S+)\s*$"
)


def _row_start_us(row: dict[str, Any]) -> float | None:
    preferred = (
        "task start time(us)",
        "task_start(us)",
        "task start(us)",
        "start time(us)",
        "start_time(us)",
    )
    normalized = {str(key).strip().lower(): value for key, value in row.items()}
    for key in preferred:
        raw = str(normalized.get(key) or "").strip().strip('"').strip()
        if not raw:
            continue
        try:
            return float(raw)
        except ValueError:
            continue
    return None


def _iso_to_epoch_us(value: Any) -> float | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp() * 1_000_000.0


def _csv_family(path: str) -> str:
    name = Path(path).name.lower()
    return _CSV_TIMESTAMP_RE.sub(".csv", name)


def _canonical_launches(
    parsed_csv: list[dict[str, Any]],
) -> tuple[list[tuple[float, str, int, dict[str, Any]]], str]:
    families: dict[str, list[dict[str, Any]]] = {}
    for record in parsed_csv:
        if int(record.get("matched_operator_rows", 0) or 0) <= 0:
            continue
        families.setdefault(_csv_family(str(record.get("path") or "")), []).append(record)
    if not families:
        return [], ""

    def family_score(item: tuple[str, list[dict[str, Any]]]) -> tuple[int, int, int]:
        family, records = item
        rows = [row for record in records for row in list(record.get("rows") or [])]
        timestamped = sum(_row_start_us(row) is not None for row in rows)
        preferred = int("opbasicinfo" in family or "op_summary" in family)
        return preferred, timestamped, len(rows)

    family, records = max(families.items(), key=family_score)
    launches: list[tuple[float, str, int, dict[str, Any]]] = []
    for record in records:
        path = str(record.get("path") or "")
        for row_index, row in enumerate(list(record.get("rows") or [])):
            start_us = _row_start_us(row)
            if start_us is not None:
                launches.append((start_us, path, row_index, row))
    launches.sort(key=lambda item: (item[0], item[1], item[2]))
    return launches, family


def _canonical_ordered_rows(
    parsed_csv: list[dict[str, Any]],
) -> tuple[list[tuple[str, str, int, dict[str, Any]]], str]:
    families: dict[str, list[dict[str, Any]]] = {}
    for record in parsed_csv:
        if int(record.get("matched_operator_rows", 0) or 0) <= 0:
            continue
        families.setdefault(_csv_family(str(record.get("path") or "")), []).append(record)
    if not families:
        return [], ""

    def family_score(item: tuple[str, list[dict[str, Any]]]) -> tuple[int, int]:
        family, records = item
        row_count = sum(len(list(record.get("rows") or [])) for record in records)
        return int("opbasicinfo" in family or "op_summary" in family), row_count

    family, records = max(families.items(), key=family_score)
    ordered: list[tuple[str, str, int, dict[str, Any]]] = []
    for record in records:
        path = str(record.get("path") or "")
        match = _CSV_TIMESTAMP_RE.search(path)
        if match is None:
            return [], family
        rows = list(record.get("rows") or [])
        matched_count = int(record.get("matched_operator_rows", 0) or 0)
        if len(rows) < matched_count:
            return [], family
        for row_index in range(matched_count):
            ordered.append((match.group(1), path, row_index, rows[row_index]))
    ordered.sort(key=lambda item: (item[0], item[1], item[2]))
    return ordered, family


def _msprof_launch_sequence(log_path: Path) -> dict[tuple[str, int], int]:
    if not log_path.is_file():
        return {}
    occurrences: defaultdict[str, int] = defaultdict(int)
    sequence: dict[tuple[str, int], int] = {}
    for line in log_path.read_text(encoding="utf-8", errors="replace").splitlines():
        match = _MSPROF_LAUNCH_RE.search(line)
        if match is None:
            continue
        kernel = match.group(1)
        occurrence = occurrences[kernel]
        occurrences[kernel] += 1
        sequence[(kernel, occurrence)] = len(sequence)
    return sequence


def _canonical_launch_log_rows(
    parsed_csv: list[dict[str, Any]],
    log_path: Path,
) -> tuple[list[tuple[int, str, int, dict[str, Any]]], str]:
    launch_sequence = _msprof_launch_sequence(log_path)
    if not launch_sequence:
        return [], ""
    families: dict[str, list[dict[str, Any]]] = {}
    for record in parsed_csv:
        if int(record.get("matched_operator_rows", 0) or 0) <= 0:
            continue
        families.setdefault(_csv_family(str(record.get("path") or "")), []).append(
            record
        )
    if not families:
        return [], ""

    def family_score(item: tuple[str, list[dict[str, Any]]]) -> tuple[int, int]:
        family, records = item
        row_count = sum(len(list(record.get("rows") or [])) for record in records)
        return int("opbasicinfo" in family or "op_summary" in family), row_count

    family, records = max(families.items(), key=family_score)
    ordered: list[tuple[int, str, int, dict[str, Any]]] = []
    for record in records:
        path = str(record.get("path") or "")
        parts = PurePosixPath(path.replace("\\", "/")).parts
        if len(parts) < 3 or not parts[-2].isdigit():
            return [], family
        launch_index = launch_sequence.get((parts[-3], int(parts[-2])))
        if launch_index is None:
            return [], family
        rows = list(record.get("rows") or [])
        matched_count = int(record.get("matched_operator_rows", 0) or 0)
        if len(rows) < matched_count:
            return [], family
        for row_index in range(matched_count):
            ordered.append((launch_index, path, row_index, rows[row_index]))
    ordered.sort(key=lambda item: (item[0], item[2], item[1]))
    keys = [(launch_index, row_index) for launch_index, _, row_index, _ in ordered]
    if len(keys) != len(set(keys)):
        return [], family
    return ordered, family


def attribute_case_rows(
    parsed_csv: list[dict[str, Any]],
    case_ids: list[int],
    batch_manifest: dict[str, Any] | None = None,
    msprof_log_path: Path | None = None,
    case_shapes: dict[str, Any] | None = None,
    expected_block_dims: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], str]:
    case_shapes = case_shapes or {}
    expected_block_dims = expected_block_dims or {}
    launches, family = _canonical_launches(parsed_csv)
    executions = [
        item
        for item in (batch_manifest or {}).get("executions", [])
        if isinstance(item, dict)
    ]
    if launches and executions:
        attributed: list[dict[str, Any]] = []
        used: set[tuple[str, int]] = set()
        for sequence_index, case_id in enumerate(case_ids):
            execution = next(
                (
                    item
                    for item in executions
                    if int(item.get("case", 0) or 0) == case_id
                    and int(item.get("repetition", 1) or 1) == 1
                ),
                None,
            )
            plan = dict((execution or {}).get("profile_call_plan") or {})
            primary_calls = [
                item
                for item in plan.get("calls", [])
                if isinstance(item, dict) and str(item.get("role") or "") == "primary"
            ]
            if not bool(plan.get("supported")) or len(primary_calls) != 1:
                return [], "unattributable-profile-call-plan"
            primary = primary_calls[0]
            started_us = _iso_to_epoch_us(primary.get("started_at"))
            finished_us = _iso_to_epoch_us(primary.get("finished_at"))
            if started_us is None or finished_us is None or finished_us < started_us:
                return [], "unattributable-profile-call-window"
            rows = [
                item
                for item in launches
                if started_us <= item[0] <= finished_us
                and (item[1], item[2]) not in used
            ]
            expected_rows = int(
                plan.get("effective_primary_task_rows", 0)
                or primary.get("effective_task_rows", 0)
                or 0
            )
            if expected_rows <= 0 or len(rows) < expected_rows:
                return [], "unattributable-profile-call-row-count"
            selected = rows[-expected_rows:]
            used.update((path, row_index) for _, path, row_index, _ in selected)
            representative = selected[-1]
            observed_block_dim = _block_dim(representative[3])
            expected_block_dim = _expected_block_dim(
                expected_block_dims, case_id
            )
            if (
                expected_block_dim is not None
                and observed_block_dim != expected_block_dim
            ):
                return [], "profiler-block-dim-mismatch"
            attributed.append(
                {
                    "case_id": case_id,
                    "sequence_index": sequence_index,
                    "source_family": family,
                    "source_path": representative[1],
                    "source_row_index": representative[2],
                    "source_start_us": representative[0],
                    "case_shape": list(case_shapes.get(str(case_id), [])),
                    "role": "primary",
                    "global_call_offset": sum(
                        len(
                            dict((prior or {}).get("profile_call_plan") or {}).get(
                                "calls", []
                            )
                        )
                        for prior in executions
                        if int(prior.get("case", 0) or 0) < case_id
                    ),
                    "call_started_at": str(primary.get("started_at") or ""),
                    "call_finished_at": str(primary.get("finished_at") or ""),
                    "expected_block_dim": expected_block_dim,
                    "observed_block_dim": observed_block_dim,
                    "matched_rows_in_call_window": len(rows),
                    "selected_primary_rows": len(selected),
                    "expected_primary_rows": expected_rows,
                    "row": representative[3],
                    "rows": [item[3] for item in selected],
                }
            )
        return attributed, "profile-call-time-window"

    ordered_rows: list[tuple[Any, str, int, dict[str, Any]]] = []
    ordered_family = ""
    order_method = ""
    if msprof_log_path is not None:
        ordered_rows, ordered_family = _canonical_launch_log_rows(
            parsed_csv, msprof_log_path
        )
        if ordered_rows:
            order_method = "profile-call-msprof-launch-log"
    if not ordered_rows and not executions:
        ordered_rows, ordered_family = _canonical_ordered_rows(parsed_csv)
        if ordered_rows:
            order_method = "profile-call-csv-export-order"
    if ordered_rows and executions:
        plans: list[tuple[int, int, list[dict[str, Any]]]] = []
        expected_total = 0
        for sequence_index, case_id in enumerate(case_ids):
            execution = next(
                (
                    item
                    for item in executions
                    if int(item.get("case", 0) or 0) == case_id
                    and int(item.get("repetition", 1) or 1) == 1
                ),
                None,
            )
            plan = dict((execution or {}).get("profile_call_plan") or {})
            calls = [item for item in plan.get("calls", []) if isinstance(item, dict)]
            primary_calls = [item for item in calls if str(item.get("role") or "") == "primary"]
            call_rows = [int(item.get("effective_task_rows", 0) or 0) for item in calls]
            if (
                not bool(plan.get("supported"))
                or len(primary_calls) != 1
                or not calls
                or any(value <= 0 for value in call_rows)
            ):
                return [], "unattributable-profile-call-sequence-plan"
            expected_total += sum(call_rows)
            plans.append((sequence_index, case_id, calls))
        if expected_total != len(ordered_rows):
            return [], "unattributable-profile-call-sequence-row-count"

        attributed = []
        cursor = 0
        global_call_offset = 0
        for sequence_index, case_id, calls in plans:
            case_start = cursor
            primary_rows: list[tuple[str, str, int, dict[str, Any]]] = []
            expected_primary_rows = 0
            primary_call_offset = -1
            primary_call: dict[str, Any] = {}
            call_evidence: list[dict[str, Any]] = []
            for call in calls:
                call_row_count = int(call.get("effective_task_rows", 0) or 0)
                row_start = cursor
                selected = ordered_rows[cursor : cursor + call_row_count]
                cursor += call_row_count
                call_record = {
                    "role": str(call.get("role") or ""),
                    "global_call_offset": global_call_offset,
                    "global_row_offset": row_start,
                    "effective_task_rows": call_row_count,
                    "started_at": str(call.get("started_at") or ""),
                    "finished_at": str(call.get("finished_at") or ""),
                    "rows": [
                        {
                            "source_path": item[1],
                            "source_row_index": item[2],
                            "source_timestamp": item[0],
                            "row": item[3],
                        }
                        for item in selected
                    ],
                }
                call_evidence.append(call_record)
                if str(call.get("role") or "") == "primary":
                    primary_rows = selected
                    expected_primary_rows = call_row_count
                    primary_call_offset = global_call_offset
                    primary_call = call
                global_call_offset += 1
            if len(primary_rows) != expected_primary_rows:
                return [], "unattributable-profile-call-sequence-primary"
            representative = primary_rows[-1]
            observed_block_dim = _block_dim(representative[3])
            expected_block_dim = _expected_block_dim(
                expected_block_dims, case_id
            )
            if (
                expected_block_dim is not None
                and observed_block_dim != expected_block_dim
            ):
                return [], "profiler-block-dim-mismatch"
            attributed.append(
                {
                    "case_id": case_id,
                    "sequence_index": sequence_index,
                    "source_family": ordered_family,
                    "source_path": representative[1],
                    "source_row_index": representative[2],
                    "source_timestamp": representative[0],
                    "case_shape": list(case_shapes.get(str(case_id), [])),
                    "role": "primary",
                    "global_call_offset": primary_call_offset,
                    "global_row_offset": case_start,
                    "call_started_at": str(primary_call.get("started_at") or ""),
                    "call_finished_at": str(primary_call.get("finished_at") or ""),
                    "expected_block_dim": expected_block_dim,
                    "observed_block_dim": observed_block_dim,
                    "call_rows": call_evidence,
                    "matched_rows_in_call_sequence": cursor - case_start,
                    "selected_primary_rows": len(primary_rows),
                    "expected_primary_rows": expected_primary_rows,
                    "row": representative[3],
                    "rows": [item[3] for item in primary_rows],
                }
            )
        return attributed, order_method

    if executions:
        return [], "unattributable-profile-call-launch-order"

    direct = [
        row
        for row in parsed_csv
        if len(row.get("case_rows", [])) == len(case_ids)
    ]
    if direct:
        return list(direct[0]["case_rows"]), "single-csv-row-order"

    launches: list[tuple[str, str, int, dict[str, Any]]] = []
    for record in parsed_csv:
        matched_count = int(record.get("matched_operator_rows", 0) or 0)
        if matched_count <= 0:
            continue
        path = str(record.get("path") or "")
        match = _CSV_TIMESTAMP_RE.search(path)
        if match is None:
            return [], "unattributable-missing-csv-timestamp"
        rows = list(record.get("rows") or [])
        if len(rows) < matched_count:
            return [], "unattributable-missing-compact-rows"
        for row_index in range(matched_count):
            launches.append((match.group(1), path, row_index, rows[row_index]))

    if len(launches) != len(case_ids):
        return [], "unattributable-launch-count-mismatch"
    sequence_keys = [(timestamp, path, row_index) for timestamp, path, row_index, _ in launches]
    if len(set(sequence_keys)) != len(sequence_keys):
        return [], "unattributable-duplicate-sequence-key"

    launches.sort(key=lambda item: (item[0], item[1], item[2]))
    return (
        [
            {
                "case_id": case_id,
                "sequence_index": index,
                "source_path": path,
                "source_row_index": row_index,
                "source_timestamp": timestamp,
                "row": row,
            }
            for index, (case_id, (timestamp, path, row_index, row)) in enumerate(
                zip(case_ids, launches)
            )
        ],
        "msprof-csv-timestamp-order",
    )


def _block_dim(row: dict[str, Any]) -> int | None:
    value = row.get("Block Dim", row.get("BlockDim"))
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _expected_block_dim(
    expected: dict[str, Any], case_id: int
) -> int | None:
    value = expected.get(str(case_id))
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
