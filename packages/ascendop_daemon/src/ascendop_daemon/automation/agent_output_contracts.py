from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Mapping

from ascendop_protocol.agent import (
    AGENT_OUTPUT_CONTRACT_SCHEMA,
    validate_agent_output_contract,
)
from ascendop_protocol.workflow import (
    SOLVER_BLOCKER_CONTRACT_REVISION,
    SOLVER_DIAGNOSTIC_CAPABILITY_GENERATION,
    validate_solver_diagnostic_request,
)


OUTPUT_DIRECTORY = ".ascendop-output"


class AgentOutputError(RuntimeError):
    pass


def build_agent_output_contracts(
    root: Path,
    *,
    campaign: str,
    operator: str,
    role: str,
    gate_stage: str,
    next_command: str = "",
    action_descriptor: Mapping[str, Any] | None = None,
    source_digest: str = "",
    proposal_key: str = "",
) -> list[dict[str, Any]]:
    """Derive exact daemon-owned output slots from one effective gate."""

    if role != "solver":
        return []
    pending_reservation = solver_pending_reservation(
        operator=operator,
        gate_stage=gate_stage,
        action_descriptor=action_descriptor,
    )
    if gate_stage == "pending-needs-evidence":
        if not re.fullmatch(r"[0-9a-f]{64}", source_digest):
            raise AgentOutputError("pending evidence source digest is invalid")
        target, pending_version = _pending_evidence_target(
            root,
            operator=operator,
            next_command=next_command,
        )
        knowledge_paths = [
            f"reference/op_knowledge/{operator}/{name}"
            for name in (
                "case_coverage.md",
                "optimization_lessons.md",
                "hypothesis_backlog.md",
            )
        ]
        return [
            _contract(
                root,
                output_id="pending-evidence-repair",
                output_kind="pending-evidence-repair",
                isolated_path=f"{OUTPUT_DIRECTORY}/pending-version.md",
                canonical_path=target,
                required=True,
                must_change=True,
                max_bytes=256 * 1024,
                identity={
                    "campaign": campaign,
                    "operator": operator,
                    "pending_version": pending_version,
                    "execution_source_digest": source_digest,
                    "required_knowledge_paths": knowledge_paths,
                },
            )
        ]
    contracts: list[dict[str, Any]] = []
    if source_digest or proposal_key:
        if not re.fullmatch(r"[0-9a-f]{64}", source_digest):
            raise AgentOutputError("candidate proposal source digest is invalid")
        if not re.fullmatch(r"[0-9a-f]{64}", proposal_key):
            raise AgentOutputError("candidate proposal key is invalid")
        latest_result = _latest_result_identity(root, operator)
        case_version = (
            pending_reservation[1]
            if pending_reservation is not None
            else latest_result[1]
            if latest_result is not None
            else _latest_case_version(root, operator)
        )
        proposal_identity = {
            "campaign": campaign,
            "operator": operator,
            "case_version": case_version,
            "base_version": (
                latest_result[0] if latest_result is not None else "no active release"
            ),
            "source_before_digest": source_digest,
            "proposal_key": proposal_key,
        }
        if pending_reservation is not None:
            proposal_identity["candidate_version"] = pending_reservation[0]
        contracts.append(
            _contract(
                root,
                output_id="solver-candidate-proposal",
                output_kind="solver-candidate-proposal",
                isolated_path=f"{OUTPUT_DIRECTORY}/candidate-proposal.json",
                canonical_path=(
                    root
                    / ".ascendop-work"
                    / "agent-proposals"
                    / operator
                    / f"{proposal_key}.json"
                ),
                required=False,
                must_change=False,
                max_bytes=256 * 1024,
                identity=proposal_identity,
            )
        )
    descriptor = dict(action_descriptor or {})
    if descriptor.get("operation") == "author-solver-blocker-contract":
        identity = dict(descriptor.get("identity") or {})
        if (
            str(identity.get("campaign") or "") != campaign
            or str(identity.get("operator") or "") != operator
        ):
            raise AgentOutputError("blocker contract descriptor identity mismatch")
        canonical = str(descriptor.get("canonical_path") or "")
        if not canonical:
            raise AgentOutputError("blocker contract descriptor has no target")
        return [
            _contract(
                root,
                output_id="solver-blocker",
                output_kind="solver-blocker",
                isolated_path=f"{OUTPUT_DIRECTORY}/solver-blocker.md",
                canonical_path=root / canonical,
                required=True,
                must_change=True,
                max_bytes=256 * 1024,
                identity=identity,
            )
        ]
    if descriptor.get("operation") == "author-solver-diagnostic-request":
        identity = dict(descriptor.get("identity") or {})
        if (
            str(identity.get("campaign") or "") != campaign
            or str(identity.get("operator") or "") != operator
        ):
            raise AgentOutputError("diagnostic output descriptor identity mismatch")
        canonical = str(descriptor.get("canonical_path") or "")
        if not canonical:
            raise AgentOutputError("diagnostic output descriptor has no target")
        return [
            _contract(
                root,
                output_id="solver-diagnostic-request",
                output_kind="solver-diagnostic-request",
                isolated_path=f"{OUTPUT_DIRECTORY}/solver-diagnostic-request.json",
                canonical_path=root / canonical,
                required=True,
                must_change=bool(descriptor.get("must_change")),
                max_bytes=256 * 1024,
                identity=identity,
            )
        ]
    if gate_stage == "diagnostic-evidence-failed":
        target, payload = _latest_diagnostic_request(root, operator)
        if str(payload["campaign"]) != campaign or str(payload["operator"]) != operator:
            raise AgentOutputError(
                "diagnostic request identity does not match the gate"
            )
        identity = {
            key: str(payload[key])
            for key in (
                "campaign",
                "operator",
                "case_version",
                "result_version",
                "blocker_generation",
            )
        }
        identity.update(
            {
                "target_test_version": str(payload["target"]["test_version"]),
                "target_source_sha256": str(payload["target"]["source_sha256"]),
            }
        )
        return [
            _contract(
                root,
                output_id="solver-diagnostic-request",
                output_kind="solver-diagnostic-request",
                isolated_path=f"{OUTPUT_DIRECTORY}/solver-diagnostic-request.json",
                canonical_path=target,
                required=True,
                must_change=True,
                max_bytes=256 * 1024,
                identity=identity,
            )
        ]

    latest_result = _latest_result_identity(root, operator)
    if latest_result is None:
        return contracts
    result_version, case_version = latest_result
    target = (
        root
        / "TestUtils"
        / "casegen"
        / operator
        / "case"
        / case_version
        / "SOLVER_BLOCKER.md"
    )
    return contracts + [
        _contract(
            root,
            output_id="solver-blocker",
            output_kind="solver-blocker",
            isolated_path=f"{OUTPUT_DIRECTORY}/solver-blocker.md",
            canonical_path=target,
            required=False,
            must_change=target.is_file(),
            max_bytes=256 * 1024,
            identity={
                "campaign": campaign,
                "operator": operator,
                "case_version": case_version,
                "result_version": result_version,
                "diagnostic_contract_revision": SOLVER_BLOCKER_CONTRACT_REVISION,
                "diagnostic_capability_generation": (
                    SOLVER_DIAGNOSTIC_CAPABILITY_GENERATION
                ),
            },
        )
    ]


def solver_pending_reservation(
    *,
    operator: str,
    gate_stage: str,
    action_descriptor: Mapping[str, Any] | None,
) -> tuple[str, str] | None:
    """Read one Solver pending reservation from the board's typed command."""

    if gate_stage != "needs-pending-candidate":
        return None
    descriptor = dict(action_descriptor or {})
    if not descriptor:
        return None
    if descriptor.get("operation") != "create-pending":
        raise AgentOutputError(
            "needs-pending-candidate has no typed create-pending reservation"
        )
    positional = descriptor.get("positional")
    options = descriptor.get("options")
    if not isinstance(positional, list) or len(positional) < 2:
        raise AgentOutputError("create-pending reservation positional identity is invalid")
    if not isinstance(options, Mapping):
        raise AgentOutputError("create-pending reservation options are invalid")
    reserved_operator = str(positional[0])
    candidate_version = str(positional[1])
    case_version = str(options.get("case_version") or "")
    if reserved_operator != operator:
        raise AgentOutputError("create-pending reservation operator mismatch")
    if not re.fullmatch(rf"{re.escape(operator)}_V\d+(?:_\d+)?", candidate_version):
        raise AgentOutputError("create-pending reservation candidate version is invalid")
    if not re.fullmatch(r"case_v\d+", case_version, re.IGNORECASE):
        raise AgentOutputError("create-pending reservation case version is invalid")
    return candidate_version, case_version.lower()


def agent_output_contracts_digest(contracts: list[Mapping[str, Any]]) -> str:
    return hashlib.sha256(
        json.dumps(
            contracts,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _contract(
    root: Path,
    *,
    output_id: str,
    output_kind: str,
    isolated_path: str,
    canonical_path: Path,
    required: bool,
    must_change: bool,
    max_bytes: int,
    identity: Mapping[str, Any],
) -> dict[str, Any]:
    target = canonical_path.resolve()
    root = root.resolve()
    if root not in target.parents:
        raise AgentOutputError("Agent output target escaped the workspace root")
    if target.exists() and (target.is_symlink() or not target.is_file()):
        raise AgentOutputError("Agent output target must be a regular file")
    before = _file_digest(target)
    return validate_agent_output_contract(
        {
            "schema": AGENT_OUTPUT_CONTRACT_SCHEMA,
            "output_id": output_id,
            "output_kind": output_kind,
            "isolated_path": isolated_path,
            "canonical_path": target.relative_to(root).as_posix(),
            "required": required,
            "must_change": must_change,
            "max_bytes": max_bytes,
            "target_before": {
                "state": "present" if before else "absent",
                "sha256": before or "",
            },
            "identity": dict(identity),
        }
    )


def _latest_diagnostic_request(
    root: Path, operator: str
) -> tuple[Path, dict[str, Any]]:
    base = root / "TestUtils" / "casegen" / operator / "case"
    candidates = list(base.glob("case_v*/SOLVER_DIAGNOSTIC_REQUEST.json"))
    if not candidates:
        raise AgentOutputError(f"{operator} has no diagnostic request to revise")
    target = max(
        candidates, key=lambda path: (path.stat().st_mtime_ns, path.as_posix())
    )
    try:
        payload = json.loads(target.read_text(encoding="utf-8-sig"))
        payload = validate_solver_diagnostic_request(payload)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        raise AgentOutputError(
            f"latest {operator} diagnostic request is invalid"
        ) from exc
    return target, payload


def _pending_evidence_target(
    root: Path,
    *,
    operator: str,
    next_command: str,
) -> tuple[Path, str]:
    normalized = str(next_command).replace("\\", "/")
    match = re.search(
        rf"(?<![A-Za-z0-9._/-])"
        rf"(TestUtils/pending/{re.escape(operator)}/"
        rf"({re.escape(operator)}_V[A-Za-z0-9._-]+)/VERSION\.md)",
        normalized,
        re.IGNORECASE,
    )
    if not match:
        raise AgentOutputError(
            f"{operator} pending evidence gate has no exact VERSION.md target"
        )
    relative = Path(match.group(1))
    pending_version = match.group(2)
    target = (root / relative).resolve()
    expected_parent = (
        root / "TestUtils" / "pending" / operator / pending_version
    ).resolve()
    if target.parent != expected_parent or target.name != "VERSION.md":
        raise AgentOutputError(
            "pending evidence target escaped its candidate directory"
        )
    if target.is_symlink() or not target.is_file():
        raise AgentOutputError(
            "pending evidence target must be an existing regular file"
        )
    return target, pending_version


def _latest_result_identity(root: Path, operator: str) -> tuple[str, str] | None:
    result_root = root / "operators_testresult" / operator
    candidates = list(result_root.glob("*/RESULT.md")) if result_root.is_dir() else []
    if not candidates:
        return None
    result = max(
        candidates, key=lambda path: (path.stat().st_mtime_ns, path.as_posix())
    )
    text = result.read_text(encoding="utf-8-sig", errors="strict")
    match = re.search(r"(?m)^Case version:\s*`?([^`\s]+)`?\s*$", text)
    if not match:
        raise AgentOutputError(f"latest {operator} RESULT has no case identity")
    return result.parent.name, match.group(1)


def _latest_case_version(root: Path, operator: str) -> str:
    base = root / "TestUtils" / "casegen" / operator / "case"
    versions = [
        (int(match.group(1)), path.name)
        for path in base.glob("case_v*")
        if path.is_dir()
        and (match := re.fullmatch(r"case_v(\d+)", path.name, re.IGNORECASE))
    ]
    if not versions:
        raise AgentOutputError(
            f"{operator} has no case version for a candidate proposal"
        )
    return max(versions)[1]


def _file_digest(path: Path) -> str | None:
    if not path.exists():
        return None
    if path.is_symlink() or not path.is_file():
        raise AgentOutputError(f"expected regular file: {path}")
    return hashlib.sha256(path.read_bytes()).hexdigest()
