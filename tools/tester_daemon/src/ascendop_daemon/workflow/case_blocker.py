from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ascendop_daemon.workflow.casegen_evidence import REQUIRED_CASEGEN_EVIDENCE, latest_casegen_evidence_issue


INVALID_CASE_MARKERS = (
    "invalid case",
    "invalid-case",
    "invalid generated case",
    "harness contract",
    "harness-contract",
    "verifier-contract",
    "case generator violates",
    "casegen bug",
    "generated case bug",
)

EXPLICIT_CASE_DEFECT_MARKERS = (
    "invalid case",
    "invalid-case",
    "invalid generated case",
    "verifier-contract blocker",
    "verifier contract blocker",
    "verifier failure",
    "verifier mismatch",
    "fusion failure",
    "fusion bug",
    "casegen bug",
    "generated case bug",
    "case generator violates",
)

PROFILER_EVIDENCE_MARKERS = (
    "canonical msprof",
    "profiler archive",
    "profiler-evidence",
    "profiler evidence",
    "profiler_evidence",
    "profiler attribution",
    "profiler request",
    "profiler/source-exhaustion",
    "mte2/mte3",
)


@dataclass(frozen=True)
class CaseRolloverBlocker:
    blocked: bool
    op: str
    latest_case: str
    latest_usage_count: int
    consecutive_invalid_rollovers: int
    evidence_cases: tuple[str, ...]

    @property
    def reason(self) -> str:
        cases = ",".join(self.evidence_cases[-5:]) if self.evidence_cases else "-"
        return (
            "repeated invalid-case/harness-contract rollover; "
            f"latest_case={self.latest_case} usage={self.latest_usage_count}; "
            f"consecutive_invalid_rollovers={self.consecutive_invalid_rollovers}; "
            f"evidence_cases={cases}; suppress solver wakeup/case generation until verifier/harness contract is fixed"
        )


def repeated_invalid_case_rollover_blocker(
    root: Path,
    op: str,
    threshold: int = 3,
) -> CaseRolloverBlocker:
    case_root = root / "TestUtils" / "casegen" / op / "case"
    case_dirs = sorted(
        [path for path in case_root.glob("case_v*") if path.is_dir()],
        key=lambda path: case_sort_key(path.name),
    )
    if not case_dirs:
        return CaseRolloverBlocker(False, op, "", 0, 0, ())

    latest = case_dirs[-1]
    latest_usage = read_usage_count(latest / "meta.json")
    count = 0
    evidence: list[str] = []
    evidence_paths: list[Path] = []
    skipped_latest_unused_without_marker = False

    for path in reversed(case_dirs):
        request_path = path / "ROLLOVER_REQUEST.md"
        if path == latest and latest_usage == 0 and not request_path.exists():
            skipped_latest_unused_without_marker = True
            continue
        if request_path.exists() and rollover_request_is_invalid_case(request_path):
            count += 1
            evidence.append(path.name)
            evidence_paths.append(request_path)
            continue
        break

    blocked = count >= max(1, threshold) and (
        latest_usage == 0 or not skipped_latest_unused_without_marker
    )
    if blocked and casegen_contract_fix_is_newer(root, op, tuple(evidence_paths)):
        blocked = False
    if blocked and latest_case_evidence_repair_is_newer(
        root, op, latest, tuple(evidence_paths)
    ):
        blocked = False
    return CaseRolloverBlocker(
        blocked, op, latest.name, latest_usage, count, tuple(reversed(evidence))
    )


def rollover_request_is_invalid_case(path: Path) -> bool:
    try:
        text = path.read_text(encoding="utf-8", errors="replace").lower()
    except OSError:
        return False
    if not any(marker in text for marker in INVALID_CASE_MARKERS):
        return False
    # A missing profiler/evidence collection path is a measurement-plane
    # blocker, not proof that the generated case or verifier is invalid.  Do
    # not let a generic "harness-contract" phrase merge that request into a
    # preceding verifier/fusion streak and permanently suppress the Tester.
    if any(marker in text for marker in PROFILER_EVIDENCE_MARKERS) and not any(
        marker in text for marker in EXPLICIT_CASE_DEFECT_MARKERS
    ):
        return False
    return True


def casegen_contract_fix_is_newer(
    root: Path, op: str, evidence_paths: tuple[Path, ...]
) -> bool:
    marker = root / "TestUtils" / "casegen" / op / "CASEGEN_CONTRACT_FIX.md"
    if not marker.exists() or not evidence_paths:
        return False
    try:
        marker_time = marker.stat().st_mtime
        latest_evidence_time = max(
            path.stat().st_mtime for path in evidence_paths if path.exists()
        )
    except (OSError, ValueError):
        return False
    return marker_time > latest_evidence_time


def latest_case_evidence_repair_is_newer(
    root: Path,
    op: str,
    latest: Path,
    evidence_paths: tuple[Path, ...],
) -> bool:
    """A schema-valid in-place Tester repair supersedes an older rollover marker."""
    if not evidence_paths or latest_casegen_evidence_issue(root, op) is not None:
        return False
    repaired_paths = [latest / name for name in REQUIRED_CASEGEN_EVIDENCE]
    try:
        repair_time = max(
            path.stat().st_mtime for path in repaired_paths if path.exists()
        )
        rollover_time = max(
            path.stat().st_mtime for path in evidence_paths if path.exists()
        )
    except (OSError, ValueError):
        return False
    return repair_time > rollover_time


def read_usage_count(path: Path) -> int:
    try:
        data: Any = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return 0
    if not isinstance(data, dict):
        return 0
    try:
        return int(data.get("usage_count", 0) or 0)
    except (TypeError, ValueError):
        return 0


def case_sort_key(name: str) -> tuple[int, str]:
    match = re.fullmatch(r"case_v(\d+)", name)
    if not match:
        return (-1, name)
    return (int(match.group(1)), name)
