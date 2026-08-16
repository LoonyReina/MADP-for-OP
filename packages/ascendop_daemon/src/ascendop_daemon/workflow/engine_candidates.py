from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ascendop_daemon.workflow.operator_job_builder import (
    EngineJobBuildError,
    parse_submit_command,
)
from ascendop_daemon.observability.engine_timing import parse_engine_time
from ascendop_daemon.core.models import DaemonConfig
from ascendop_daemon.control_plane.workflow_priority import flow_priority


class EngineCandidateError(RuntimeError):
    pass


RETRYABLE_ENGINE_STATES = {"return-lost"}
LOCAL_PAYLOAD_VALIDATION_ERROR_PREFIX = "transport-payload-validation:"


def engine_attempt_requires_rebuild(record: dict[str, Any]) -> bool:
    state = str(record.get("state") or "")
    if state in RETRYABLE_ENGINE_STATES:
        return True
    return (
        state == "admission-failed"
        and str(record.get("last_error") or "").startswith(
            LOCAL_PAYLOAD_VALIDATION_ERROR_PREFIX
        )
    )


def discover_control_plane_submit_candidates(
    root: Path,
    config: DaemonConfig,
    *,
    traffic_debt: dict[str, int] | None = None,
    operator_priorities: dict[str, int] | None = None,
) -> list[dict[str, str]]:
    """Discover current V4 queue identities without inferring retries.

    Pump state, archived attempts, and remote failure cooldowns belong to the
    legacy execution model. The V4 control database owns attempt and retry
    state, so intake only observes the current queue payload identity.
    """

    root = root.resolve()
    debt = traffic_debt or {}
    candidates: list[dict[str, str]] = []
    queue_path = root / "TestUtils" / "submit" / "queue.md"
    for sequence, row in enumerate(parse_queue(queue_path)):
        op = row.get("op", "")
        version = row.get("test_version", "")
        if row.get("status") != "queued" or op not in config.operators:
            continue
        if not op or not version:
            continue
        if (root / "operators_testresult" / op / version / "RESULT.md").is_file():
            continue
        submit_md = root / "TestUtils" / "submit" / op / version / "SUBMIT.md"
        if not submit_md.is_file():
            continue
        command = extract_submit_command(submit_md)
        parsed = parse_submit_command(command)
        if parsed["op"] != op or parsed["test_version"] != version:
            raise EngineCandidateError(
                f"queue/SUBMIT correlation mismatch: {op}/{version} vs "
                f"{parsed['op']}/{parsed['test_version']}"
            )
        candidates.append(
            {
                "op": op,
                "test_version": version,
                "command": command,
                "sequence": str(sequence),
                "debt": str(max(0, int(debt.get(op, 0) or 0))),
                "flow_priority": str(flow_priority(operator_priorities, op)),
            }
        )
    return sorted(
        candidates,
        key=lambda item: (
            -int(item["debt"]),
            -int(item["flow_priority"]),
            int(item["sequence"]),
        ),
    )


def discover_engine_submit_candidates(
    root: Path,
    config: DaemonConfig,
    *,
    pump_state: dict[str, Any] | None = None,
    traffic_debt: dict[str, int] | None = None,
    operator_priorities: dict[str, int] | None = None,
    remote_engine_generation: str = "",
    held_candidates: list[dict[str, Any]] | None = None,
    now: datetime | None = None,
) -> list[dict[str, str]]:
    """Discover active-op queued submits without inheriting the legacy single-head gate."""

    root = root.resolve()
    entries = (pump_state or {}).get("entries", {})
    if not isinstance(entries, dict):
        entries = {}
    debt = traffic_debt or {}
    candidates: list[dict[str, str]] = []
    for sequence, row in enumerate(parse_queue(root / "TestUtils" / "submit" / "queue.md")):
        op = row.get("op", "")
        version = row.get("test_version", "")
        if row.get("status") != "queued" or op not in config.operators:
            continue
        if not op or not version:
            continue
        submit_root = root / "TestUtils" / "submit" / op / version
        if (root / "operators_testresult" / op / version / "RESULT.md").exists():
            continue
        retry_ordinal = next_engine_retry_ordinal(
            root,
            pump_state or {},
            op=op,
            test_version=version,
        )
        if workflow_attempt_is_managed(
            root,
            pump_state or {},
            op=op,
            test_version=version,
            retry_ordinal=retry_ordinal,
        ):
            continue
        retry_gate = evaluate_same_failure_retry_gate(
            config,
            pump_state or {},
            op=op,
            test_version=version,
            remote_engine_generation=remote_engine_generation,
            now=now,
        )
        if retry_gate["state"] == "hold":
            if held_candidates is not None:
                held_candidates.append(
                    {
                        "op": op,
                        "test_version": version,
                        "retry_ordinal": retry_ordinal,
                        **retry_gate,
                    }
                )
            continue
        submit_md = submit_root / "SUBMIT.md"
        if not submit_md.is_file():
            continue
        command = extract_submit_command(submit_md)
        parsed = parse_submit_command(command)
        if parsed["op"] != op or parsed["test_version"] != version:
            raise EngineCandidateError(
                f"queue/SUBMIT correlation mismatch: {op}/{version} vs "
                f"{parsed['op']}/{parsed['test_version']}"
            )
        candidates.append(
            {
                "op": op,
                "test_version": version,
                "command": command,
                "sequence": str(sequence),
                "debt": str(max(0, int(debt.get(op, 0) or 0))),
                "flow_priority": str(flow_priority(operator_priorities, op)),
                "job_id_suffix": f"retry{retry_ordinal:03d}" if retry_ordinal else "",
                "attempt_index": str(retry_ordinal + 1),
            }
        )
    return sorted(
        candidates,
        key=lambda item: (
            -int(item["debt"]),
            -int(item["flow_priority"]),
            int(item["sequence"]),
        ),
    )


def evaluate_same_failure_retry_gate(
    config: DaemonConfig,
    pump_state: dict[str, Any],
    *,
    op: str,
    test_version: str,
    remote_engine_generation: str = "",
    now: datetime | None = None,
) -> dict[str, Any]:
    """Bound retries for one deterministic failure without pinning other ops.

    The scope includes the remote Engine code generation. A successful code
    update therefore reopens the exact queue row immediately, while an
    unchanged deterministic failure moves to exponential probe intervals.
    """

    limit = max(
        1,
        int(config.policy.get("test_engine_same_failure_retry_limit", 3) or 3),
    )
    base_seconds = max(
        1,
        int(
            config.policy.get(
                "test_engine_same_failure_retry_base_seconds", 60
            )
            or 60
        ),
    )
    max_seconds = max(
        base_seconds,
        int(
            config.policy.get(
                "test_engine_same_failure_retry_max_seconds", 1800
            )
            or 1800
        ),
    )
    failures = matching_terminal_failures(
        pump_state,
        op=op,
        test_version=test_version,
    )
    if not failures:
        return {
            "state": "ready",
            "reason": "",
            "failure_count": 0,
            "retry_limit": limit,
        }
    latest = failures[-1]
    signature = engine_failure_signature(latest)
    generation = terminal_engine_generation(latest)
    if remote_engine_generation and not generation:
        return {
            "state": "ready",
            "reason": "failure-engine-generation-unrecorded",
            "failure_count": 0,
            "retry_limit": limit,
            "failure_signature": signature,
            "failure_engine_generation": "",
            "remote_engine_generation": remote_engine_generation,
        }
    if (
        remote_engine_generation
        and generation
        and remote_engine_generation != generation
    ):
        return {
            "state": "ready",
            "reason": "remote-engine-generation-changed",
            "failure_count": 0,
            "retry_limit": limit,
            "failure_signature": signature,
            "failure_engine_generation": generation,
            "remote_engine_generation": remote_engine_generation,
        }
    consecutive = 0
    for record in reversed(failures):
        if (
            engine_failure_signature(record) != signature
            or terminal_engine_generation(record) != generation
        ):
            break
        consecutive += 1
    if consecutive < limit:
        return {
            "state": "ready",
            "reason": "",
            "failure_count": consecutive,
            "retry_limit": limit,
            "failure_signature": signature,
            "failure_engine_generation": generation,
        }
    cooldown_seconds = min(
        max_seconds,
        base_seconds * (2 ** min(20, consecutive - limit)),
    )
    latest_at = terminal_failure_time(latest)
    observed_now = now or datetime.now(timezone.utc)
    if observed_now.tzinfo is None:
        observed_now = observed_now.replace(tzinfo=timezone.utc)
    age_seconds = (
        max(0, int((observed_now - latest_at).total_seconds()))
        if latest_at is not None
        else 0
    )
    remaining_seconds = max(0, cooldown_seconds - age_seconds)
    if remaining_seconds == 0:
        return {
            "state": "ready",
            "reason": "same-failure-probe-due",
            "failure_count": consecutive,
            "retry_limit": limit,
            "failure_signature": signature,
            "failure_engine_generation": generation,
            "cooldown_seconds": cooldown_seconds,
        }
    return {
        "state": "hold",
        "reason": "same-engine-generation-failure-circuit-open",
        "failure_count": consecutive,
        "retry_limit": limit,
        "failure_signature": signature,
        "failure_engine_generation": generation,
        "remote_engine_generation": remote_engine_generation,
        "cooldown_seconds": cooldown_seconds,
        "retry_after_seconds": remaining_seconds,
    }


def matching_terminal_failures(
    pump_state: dict[str, Any],
    *,
    op: str,
    test_version: str,
) -> list[dict[str, Any]]:
    entries = pump_state.get("entries", {})
    if not isinstance(entries, dict):
        return []
    rows: list[tuple[int, str, dict[str, Any]]] = []
    for entry_id, record in entries.items():
        if not isinstance(record, dict) or record.get("workflow_ingest") is False:
            continue
        if str(record.get("operator") or "") not in {"", op}:
            continue
        if str(record.get("test_version") or "") != test_version:
            continue
        terminal = record.get("terminal_manifest")
        terminal_state = (
            str(terminal.get("state") or "")
            if isinstance(terminal, dict)
            else str(record.get("engine_terminal_state") or "")
        )
        if terminal_state != "failed":
            continue
        rows.append(
            (
                engine_attempt_ordinal(str(record.get("engine_job_id") or entry_id)),
                str(record.get("updated_at") or ""),
                record,
            )
        )
    return [record for _, _, record in sorted(rows, key=lambda item: item[:2])]


def engine_attempt_ordinal(engine_job_id: str) -> int:
    marker = "-retry"
    if marker not in engine_job_id:
        return 0
    suffix = engine_job_id.rsplit(marker, 1)[-1]
    return int(suffix) if suffix.isdigit() else 0


def terminal_engine_generation(record: dict[str, Any]) -> str:
    terminal = record.get("terminal_manifest")
    if isinstance(terminal, dict):
        return str(terminal.get("engine_code_generation") or "")
    return str(record.get("engine_code_generation") or "")


def engine_failure_signature(record: dict[str, Any]) -> str:
    terminal = record.get("terminal_manifest")
    if not isinstance(terminal, dict):
        terminal = {}
    failed_stage: dict[str, Any] = {}
    history = terminal.get("history")
    if isinstance(history, list):
        failed_stage = next(
            (
                item
                for item in reversed(history)
                if isinstance(item, dict)
                and int(item.get("exit_code", 0) or 0) != 0
            ),
            {},
        )
    return "|".join(
        (
            str(
                failed_stage.get("stage_name")
                or record.get("engine_failed_stage")
                or record.get("stage_name")
                or ""
            ),
            str(
                failed_stage.get("exit_code")
                or record.get("engine_failed_exit_code")
                or terminal.get("error")
                or ""
            ),
            str(terminal.get("state") or record.get("engine_terminal_state") or ""),
        )
    )


def terminal_failure_time(record: dict[str, Any]) -> datetime | None:
    terminal = record.get("terminal_manifest")
    values = [
        terminal.get("terminal_at") if isinstance(terminal, dict) else "",
        record.get("terminal_at"),
        record.get("returned_at"),
        record.get("updated_at"),
    ]
    for value in values:
        parsed = parse_engine_time(value)
        if parsed is not None:
            return parsed
    return None


def workflow_attempt_is_managed(
    root: Path,
    pump_state: dict[str, Any],
    *,
    op: str,
    test_version: str,
    retry_ordinal: int | None = None,
) -> bool:
    """Return true when the current queue attempt already belongs to the engine.

    A restored infrastructure result increments ``retry_ordinal`` and changes the
    exact engine job id for the next attempt.  Matching by historical entry count
    is unsafe because pre-engine attempts and restored archives need not have a
    one-to-one relationship with pump records.  Older state files did not record
    ``operator`` on every entry, so an absent operator is treated as a match while
    an explicit different operator is ignored.
    """

    entries = pump_state.get("entries", {})
    if not isinstance(entries, dict):
        return False
    ordinal = (
        next_engine_retry_ordinal(
            root,
            pump_state,
            op=op,
            test_version=test_version,
        )
        if retry_ordinal is None
        else max(0, int(retry_ordinal))
    )
    expected_job_id = (
        test_version if ordinal == 0 else f"{test_version}-retry{ordinal:03d}"
    )
    for entry_id, record in entries.items():
        if not isinstance(record, dict) or record.get("workflow_ingest") is False:
            continue
        if str(record.get("test_version") or "") != test_version:
            continue
        record_op = str(record.get("operator") or "")
        if record_op and record_op != op:
            continue
        record_job_id = str(record.get("engine_job_id") or entry_id or "")
        if (
            record_job_id == expected_job_id
            and not engine_attempt_requires_rebuild(record)
        ):
            return True
    return False


def next_engine_retry_ordinal(
    root: Path,
    pump_state: dict[str, Any],
    *,
    op: str,
    test_version: str,
) -> int:
    """Advance only past attempts whose immutable return can no longer be ingested."""

    entries = pump_state.get("entries", {})
    if not isinstance(entries, dict):
        entries = {}
    restored_submit = (
        root / "TestUtils" / "submit" / op / test_version
    ).is_dir()
    terminal_result = (
        root / "operators_testresult" / op / test_version / "RESULT.md"
    ).is_file()
    ordinal = restored_attempt_count(root, op, test_version)
    while True:
        job_id = (
            test_version if ordinal == 0 else f"{test_version}-retry{ordinal:03d}"
        )
        record = entries.get(job_id)
        if not isinstance(record, dict):
            return ordinal
        record_op = str(record.get("operator") or "")
        if record_op and record_op != op:
            return ordinal
        if str(record.get("test_version") or "") != test_version:
            return ordinal
        state = str(record.get("state") or "")
        if engine_attempt_requires_rebuild(record):
            ordinal += 1
            continue
        if restored_submit and not terminal_result and state == "workflow-archived":
            # Older restore logic nested attempt archives. The filesystem count
            # can therefore lag a durable Engine record after path truncation or
            # an interrupted flattening migration. A restored submit with no
            # terminal RESULT is authoritative evidence that the archived job
            # must not suppress its next retry.
            ordinal += 1
            continue
        return ordinal


def restored_attempt_count(root: Path, op: str, version: str) -> int:
    result_root = root / "operators_testresult" / op / version
    if not result_root.is_dir():
        return 0
    attempts = [
        child
        for child in result_root.rglob("*")
        if child.is_dir()
        and "_attempt_" in child.name
        and child.name.rsplit("_attempt_", 1)[-1].isdigit()
    ]
    return len(attempts)


def extract_submit_command(path: Path) -> str:
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if "gitpartner-run-submit" not in line:
            continue
        try:
            parse_submit_command(line)
        except EngineJobBuildError:
            continue
        return line
    raise EngineCandidateError(f"SUBMIT.md has no trusted gitpartner-run-submit command: {path}")


def parse_queue(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    table = [
        line.strip()
        for line in path.read_text(encoding="utf-8-sig").splitlines()
        if line.strip().startswith("|") and line.strip().endswith("|")
    ]
    if len(table) < 2:
        return []
    header = [cell.strip() for cell in table[0].strip("|").split("|")]
    rows: list[dict[str, str]] = []
    for line in table[2:]:
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if len(cells) == len(header):
            rows.append(dict(zip(header, cells)))
    return rows
