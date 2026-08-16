from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class CaseBundleContractResult:
    valid: bool
    detail: str
    op: str
    case_version: str
    case_count: int
    cases_sha256: str
    task_case_sidecar_sha256: str
    validator_path: str
    validator_output: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def validate_case_bundle_contract(
    task_case: Path,
    case_dir: Path,
    *,
    op: str,
    case_version: str,
    run_validator: bool = True,
    require_validator: bool = False,
    timeout_seconds: int = 120,
) -> CaseBundleContractResult:
    """Validate one Tester-authored case bundle before snapshot or submit.

    The generic contract checks durable identity and any task-case sidecar
    named ``case_specs.json``. An operator may add a source-independent
    ``validate_package.py`` for semantic checks that cannot be inferred by the
    harness, such as non-empty counter witnesses.
    """

    task_case = task_case.resolve()
    case_dir = case_dir.resolve()
    cases_path = case_dir / "cases.json"
    meta_path = case_dir / "meta.json"
    validator = task_case / "validate_package.py"

    try:
        cases = _read_json(cases_path)
        meta = _read_json(meta_path)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return _result(False, f"case bundle JSON is invalid: {exc}", op, case_version)

    if not isinstance(meta, dict):
        return _result(False, "case bundle meta.json must be an object", op, case_version)
    if str(meta.get("op") or "") != op:
        return _result(
            False,
            f"case bundle op mismatch: expected {op}, got {meta.get('op')!r}",
            op,
            case_version,
        )
    if str(meta.get("case_version") or "") != case_version:
        return _result(
            False,
            "case bundle version mismatch: "
            f"expected {case_version}, got {meta.get('case_version')!r}",
            op,
            case_version,
        )
    if not isinstance(cases, list) or not cases:
        return _result(
            False,
            "case bundle cases.json must contain a non-empty list",
            op,
            case_version,
        )

    cases_sha256 = _sha256(cases_path)
    sidecar_sha256 = ""
    sidecar = task_case / "case_specs.json"
    if sidecar.is_file():
        try:
            sidecar_cases = _read_json(sidecar)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            return _result(
                False,
                f"task-case case_specs.json is invalid: {exc}",
                op,
                case_version,
                len(cases),
                cases_sha256,
            )
        sidecar_sha256 = _sha256(sidecar)
        if sidecar_cases != cases:
            return _result(
                False,
                "task-case case_specs.json does not match active cases.json; "
                "repair the current task package in place before submit/prewarm",
                op,
                case_version,
                len(cases),
                cases_sha256,
                sidecar_sha256,
            )

    validator_output = ""
    if require_validator and not validator.is_file():
        return _result(
            False,
            "repeated invalid-case recovery requires task_case/validate_package.py",
            op,
            case_version,
            len(cases),
            cases_sha256,
            sidecar_sha256,
        )
    if run_validator and validator.is_file():
        env = dict(os.environ)
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        try:
            completed = subprocess.run(
                [sys.executable, str(validator)],
                cwd=str(task_case),
                env=env,
                text=True,
                encoding="utf-8",
                errors="replace",
                capture_output=True,
                timeout=max(1, timeout_seconds),
                check=False,
                creationflags=int(getattr(subprocess, "CREATE_NO_WINDOW", 0)),
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            return _result(
                False,
                f"case package validator could not complete: {exc}",
                op,
                case_version,
                len(cases),
                cases_sha256,
                sidecar_sha256,
                str(validator),
            )
        validator_output = _compact_output(completed.stdout, completed.stderr)
        if completed.returncode != 0:
            return _result(
                False,
                "case package validator failed "
                f"with returncode {completed.returncode}: {validator_output}",
                op,
                case_version,
                len(cases),
                cases_sha256,
                sidecar_sha256,
                str(validator),
                validator_output,
            )

    return _result(
        True,
        "case bundle contract validation passed",
        op,
        case_version,
        len(cases),
        cases_sha256,
        sidecar_sha256,
        str(validator) if validator.is_file() else "",
        validator_output,
    )


def require_case_bundle_contract(
    task_case: Path,
    case_dir: Path,
    *,
    op: str,
    case_version: str,
    run_validator: bool = True,
    require_validator: bool = False,
) -> CaseBundleContractResult:
    result = validate_case_bundle_contract(
        task_case,
        case_dir,
        op=op,
        case_version=case_version,
        run_validator=run_validator,
        require_validator=require_validator,
    )
    if not result.valid:
        raise ValueError(result.detail)
    return result


def case_bundle_validator_due(case_dir: Path) -> bool:
    """Run generation-time semantic validators before the first case use."""
    try:
        meta = _read_json(case_dir / "meta.json")
    except (OSError, ValueError, json.JSONDecodeError):
        return True
    if not isinstance(meta, dict):
        return True
    try:
        return int(meta.get("usage_count") or 0) <= 0
    except (TypeError, ValueError):
        return True


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _compact_output(stdout: str, stderr: str) -> str:
    lines = [line.strip() for line in (stdout + "\n" + stderr).splitlines() if line.strip()]
    value = " | ".join(lines[-3:])
    if len(value) <= 1600:
        return value
    return value[:800] + " ... [truncated] ... " + value[-800:]


def _result(
    valid: bool,
    detail: str,
    op: str,
    case_version: str,
    case_count: int = 0,
    cases_sha256: str = "",
    task_case_sidecar_sha256: str = "",
    validator_path: str = "",
    validator_output: str = "",
) -> CaseBundleContractResult:
    return CaseBundleContractResult(
        valid=valid,
        detail=detail,
        op=op,
        case_version=case_version,
        case_count=case_count,
        cases_sha256=cases_sha256,
        task_case_sidecar_sha256=task_case_sidecar_sha256,
        validator_path=validator_path,
        validator_output=validator_output,
    )
