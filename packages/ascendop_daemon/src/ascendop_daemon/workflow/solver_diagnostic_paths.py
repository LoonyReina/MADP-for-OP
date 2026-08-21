from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


REQUEST_FILE = "SOLVER_DIAGNOSTIC_REQUEST.json"
INDEX_FILE = "SOLVER_DIAGNOSTIC_INDEX.json"


class SolverDiagnosticError(RuntimeError):
    pass


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def canonical_digest(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def generation_digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def case_directory(root: Path, operator: str, case_version: str) -> Path:
    return root / "TestUtils" / "casegen" / operator / "case" / case_version


def request_path(root: Path, operator: str, case_version: str) -> Path:
    return case_directory(root, operator, case_version) / REQUEST_FILE


def index_path(root: Path, operator: str, case_version: str) -> Path:
    return case_directory(root, operator, case_version) / INDEX_FILE


def state_path(
    root: Path,
    operator: str,
    case_version: str,
    blocker_generation: str,
) -> Path:
    return (
        root
        / "TestUtils"
        / "tester_daemon"
        / "solver_diagnostic_requests"
        / operator
        / case_version
        / generation_digest(blocker_generation)
        / "request.json"
    )


def revision_path(
    root: Path,
    operator: str,
    case_version: str,
    blocker_generation: str,
    request_digest: str,
) -> Path:
    return (
        state_path(root, operator, case_version, blocker_generation).parent
        / "revisions"
        / f"r-{request_digest[:16]}.json"
    )


def archived_submit_snapshot(root: Path, operator: str, test_version: str) -> Path:
    snapshot = (
        root
        / "operators_testresult"
        / operator
        / test_version
        / "submit_snapshot"
    )
    if not snapshot.is_dir():
        raise SolverDiagnosticError(
            f"immutable submit snapshot is missing: {operator}/{test_version}"
        )
    return snapshot
