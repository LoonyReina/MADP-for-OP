from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from ascendop_protocol.workflow import SOLVER_STEWARD_ESCALATION_STATE

from ascendop_daemon.control_plane.control_database import ControlDatabase
from ascendop_daemon.core.models import ActionKind, BoardRow, DaemonConfig
from ascendop_daemon.storage.state_reader import StateReader
from ascendop_daemon.workflow.gate_engine import GateEngine
from ascendop_daemon.workflow.policy_pipeline import WorkflowPolicyPipeline


REPORT_SCHEMA = "ascendop.agent-blocker-attribution-report.v1"


def build_blocker_attribution_report(
    root: Path,
    database: ControlDatabase,
    config: DaemonConfig,
    *,
    operators: set[str] | None = None,
) -> dict[str, Any]:
    """Project responsibility from board, evidence, staging, and Agent receipts."""

    root = root.resolve()
    snapshot = StateReader(root, config).read()
    decisions = WorkflowPolicyPipeline(
        root,
        config,
        database=database,
    ).apply(GateEngine(config).evaluate(snapshot.rows, snapshot.transport))
    actions = database.agent_actions_v4()
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for decision in decisions:
        row = decision.row
        if row.op in seen or (operators is not None and row.op not in operators):
            continue
        seen.add(row.op)
        role = _decision_role(decision.action, row)
        record = _matching_action(actions, row, role=role)
        context = _action_context(database, record) if record is not None else {}
        receipt = (
            database.agent_action_receipt(str(record["action_id"]))
            if record is not None
            else None
        )
        projection = _projection_status(context)
        staging = _staging_status(
            root,
            record,
            context,
            stale_after_seconds=_delivery_stale_seconds(config),
        )
        consumption = _consumption_status(root, record, receipt)
        classification = classify_blocker_attribution(
            row=row,
            action=record,
            projection=projection,
            staging=staging,
            consumption=consumption,
            receipt=receipt,
        )
        rows.append(
            {
                "operator": row.op,
                "gate_stage": row.gate_stage,
                "next_owner": row.next_owner,
                "wakeups": row.wakeups,
                "action": _action_summary(record),
                "evidence_projection": projection,
                "adapter_staging": staging,
                "evidence_consumption": consumption,
                "output_validation": _validation_status(receipt),
                "classification": classification,
            }
        )
    return {
        "schema": REPORT_SCHEMA,
        "captured_at": snapshot.captured_at,
        "claim_side_effects": False,
        "rows": rows,
    }


def classify_blocker_attribution(
    *,
    row: BoardRow,
    action: Mapping[str, Any] | None,
    projection: Mapping[str, Any],
    staging: Mapping[str, Any],
    consumption: Mapping[str, Any],
    receipt: Mapping[str, Any] | None,
) -> dict[str, Any]:
    stage = row.gate_stage.strip().lower()
    if stage == SOLVER_STEWARD_ESCALATION_STATE:
        return _classification(
            "steward-escalation-pending",
            "steward",
            True,
            "the daemon owns proactive delivery of the exact capability gap",
        )
    if row.next_owner == "daemon" or stage == "diagnostic-capability-blocked":
        return _classification(
            "flow-capability-gap",
            "daemon-harness",
            True,
            _required_capability(row.next_command),
        )
    if action is None:
        return _classification(
            "flow-action-publication-gap",
            "daemon",
            True,
            "the effective Agent-owned gate has no matching typed action",
        )
    if projection.get("state") != "valid":
        return _classification(
            "flow-evidence-projection-gap",
            "daemon",
            True,
            str(projection.get("reason") or "evidence digest mismatch"),
        )
    if staging.get("state") in {"missing", "invalid"}:
        return _classification(
            "adapter-evidence-staging-gap",
            "agent-adapter",
            True,
            str(staging.get("reason") or "staged evidence is unavailable"),
        )
    if staging.get("state") == "delivery-stalled":
        return _classification(
            "adapter-delivery-reconciliation",
            "agent-adapter",
            True,
            str(staging.get("reason") or "the queued action was not claimed"),
        )
    if staging.get("state") == "awaiting-delivery":
        return _classification(
            "adapter-delivery-pending",
            "agent-adapter",
            False,
            str(staging.get("reason") or "the queued action awaits delivery"),
        )
    action_state = str(action.get("state") or "")
    if action_state == "uncertain":
        return _classification(
            "adapter-delivery-reconciliation",
            "agent-adapter",
            True,
            "the immutable delivered turn must be reconciled before reuse",
        )
    validation = _validation_status(receipt)
    if validation["state"] == "failed":
        category = (
            "agent-evidence-consumption-gap"
            if "consulted evidence" in validation["error"].lower()
            or "consulted_evidence" in validation["error"].lower()
            else "agent-output-contract-gap"
        )
        return _classification(category, "solver", True, validation["error"])
    if stage.endswith("-invalid"):
        return _classification(
            "agent-output-contract-gap",
            "solver",
            True,
            row.next_command,
        )
    if consumption.get("state") == "not-declared":
        return _classification(
            "agent-evidence-consumption-gap",
            "solver",
            True,
            "the terminal Agent output did not declare inspected evidence",
        )
    if row.next_owner in {"solver", "tester"}:
        return _classification(
            "agent-evidence-decision",
            row.next_owner,
            False,
            "required evidence is projected; the role owns the next typed decision",
        )
    return _classification(
        "flow-state-review",
        row.next_owner or "daemon",
        True,
        row.next_command,
    )


def _matching_action(
    actions: list[dict[str, Any]],
    row: BoardRow,
    *,
    role: str,
) -> dict[str, Any] | None:
    matches: list[dict[str, Any]] = []
    for record in actions:
        action = dict(record.get("action") or {})
        identity = dict(action.get("candidate_identity") or {})
        if (
            identity.get("display_name") == row.op
            and identity.get("gate_stage") == row.gate_stage
            and identity.get("next_command") == row.next_command
            and record.get("role") == role
        ):
            matches.append(record)
    return max(matches, key=lambda item: str(item.get("created_at") or ""), default=None)


def _action_context(
    database: ControlDatabase,
    record: Mapping[str, Any],
) -> dict[str, Any]:
    with database.connection() as connection:
        row = connection.execute(
            "SELECT snapshot_json FROM agent_context_snapshots_v4 "
            "WHERE iteration_id=?",
            (str(record["iteration_id"]),),
        ).fetchone()
    if row is None:
        return {}
    value = json.loads(str(row["snapshot_json"]))
    return value if isinstance(value, dict) else {}


def _projection_status(context: Mapping[str, Any]) -> dict[str, Any]:
    if not context:
        return {"state": "missing", "reason": "Agent context snapshot is missing"}
    gate = dict(context.get("gate") or {})
    fields: dict[str, Any] = {}
    mismatches: list[str] = []
    for name in ("workflow_evidence", "reference_evidence"):
        values = context.get(name, [])
        if not isinstance(values, list) or not all(
            isinstance(item, Mapping) for item in values
        ):
            mismatches.append(f"{name} is not a list of descriptors")
            values = []
        actual = _object_digest(values)
        expected = str(gate.get(f"{name}_digest") or "")
        if not expected or expected != actual:
            mismatches.append(f"{name} digest mismatch")
        fields[name] = {
            "descriptor_count": len(values),
            "expected_digest": expected,
            "actual_digest": actual,
        }
    return {
        "state": "invalid" if mismatches else "valid",
        "reason": "; ".join(mismatches),
        **fields,
    }


def _staging_status(
    root: Path,
    record: Mapping[str, Any] | None,
    context: Mapping[str, Any],
    *,
    stale_after_seconds: int,
) -> dict[str, Any]:
    if record is None:
        return {"state": "not-applicable", "reason": "no Agent action"}
    run_root = root / ".ascendop-work" / "agent-runs" / str(record["action_id"])
    stage_path = run_root / "stage.json"
    descriptor_count = sum(
        len(context.get(name, [])) if isinstance(context.get(name, []), list) else 0
        for name in ("workflow_evidence", "reference_evidence")
    )
    if not stage_path.is_file():
        if str(record.get("state") or "") in {"queued", "retry-pending"}:
            age_seconds = _action_age_seconds(record)
            stalled = age_seconds is not None and age_seconds > stale_after_seconds
            return {
                "state": "delivery-stalled" if stalled else "awaiting-delivery",
                "descriptor_count": descriptor_count,
                "age_seconds": age_seconds,
                "stale_after_seconds": stale_after_seconds,
                "reason": (
                    "the queued action exceeded its configured Agent delivery lease "
                    "without an Adapter claim"
                    if stalled
                    else "the action has not yet been claimed by an Agent adapter"
                ),
            }
        return {
            "state": "missing",
            "descriptor_count": descriptor_count,
            "reason": "a claimed or terminal Agent action has no stage receipt",
        }
    try:
        stage = _read_object(stage_path)
        workspace = (root / str(stage["workspace"])).resolve()
        manifest_path = workspace / ".ascendop-evidence" / "MANIFEST.json"
        if descriptor_count == 0 and not manifest_path.exists():
            return {"state": "valid-empty", "descriptor_count": 0, "file_count": 0}
        manifest = _read_object(manifest_path)
        core = {
            "schema": manifest.get("schema"),
            "descriptors": manifest.get("descriptors"),
            "files": manifest.get("files"),
        }
        digest = _object_digest(core)
        if (
            manifest.get("schema") != "ascendop.agent-evidence-manifest.v1"
            or manifest.get("manifest_digest") != digest
            or stage.get("evidence_digest") != digest
            or len(manifest.get("descriptors", [])) != descriptor_count
        ):
            raise ValueError("evidence manifest identity mismatch")
        for item in manifest.get("files", []):
            blob = (workspace / ".ascendop-evidence" / str(item["blob_path"])).resolve()
            if not blob.is_file() or hashlib.sha256(blob.read_bytes()).hexdigest() != item.get("sha256"):
                raise ValueError("evidence blob is missing or changed")
        return {
            "state": "valid",
            "descriptor_count": descriptor_count,
            "file_count": len(manifest.get("files", [])),
            "manifest_digest": digest,
        }
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        return {
            "state": "invalid",
            "descriptor_count": descriptor_count,
            "reason": str(exc),
        }


def _consumption_status(
    root: Path,
    record: Mapping[str, Any] | None,
    receipt: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if record is None:
        return {"state": "not-applicable", "declared_evidence": []}
    state = str(record.get("state") or "")
    if state in {"queued", "retry-pending", "claimed", "running"}:
        return {"state": "pending", "declared_evidence": []}
    validation = _validation_status(receipt)
    if validation["state"] == "failed" and (
        "consulted evidence" in validation["error"].lower()
        or "consulted_evidence" in validation["error"].lower()
    ):
        return {"state": "rejected-missing", "declared_evidence": []}
    declarations = _declared_evidence(root, str(record["action_id"]))
    if declarations:
        return {"state": "declared", "declared_evidence": declarations}
    if state == "completed":
        return {"state": "not-declared", "declared_evidence": []}
    return {"state": "unknown", "declared_evidence": []}


def _declared_evidence(root: Path, action_id: str) -> list[str]:
    run_root = root / ".ascendop-work" / "agent-runs" / action_id
    try:
        stage = _read_object(run_root / "stage.json")
        seal = _read_object(run_root / "output-seal.json")
        workspace = (root / str(stage["workspace"])).resolve()
    except (KeyError, OSError, ValueError, json.JSONDecodeError):
        return []
    values: list[str] = []
    for item in seal.get("outputs", []):
        try:
            path = workspace / str(item["isolated_path"])
            payload = path.read_text(encoding="utf-8-sig")
            if str(item.get("output_kind") or "") in {
                "solver-candidate-proposal",
                "solver-diagnostic-request",
            }:
                document = json.loads(payload)
                raw = document.get("consulted_evidence", [])
                if isinstance(raw, list):
                    values.extend(str(value).strip() for value in raw if str(value).strip())
            else:
                match = re.search(
                    r"(?ms)^Consulted evidence:\s*(.*?)"
                    r"(?=^[A-Z][A-Za-z ]+:\s*|\Z)",
                    payload,
                )
                if match and match.group(1).strip():
                    values.append(match.group(1).strip())
        except (KeyError, OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
    return sorted(set(values))


def _validation_status(receipt: Mapping[str, Any] | None) -> dict[str, str]:
    if receipt is None:
        return {"state": "pending", "failure_class": "", "error": ""}
    completion = dict(receipt.get("completion") or {})
    error = str(completion.get("validation_error") or "")
    return {
        "state": "failed" if error else "valid",
        "failure_class": str(completion.get("failure_class") or ""),
        "error": error,
    }


def _decision_role(action: ActionKind, row: BoardRow) -> str:
    if action == ActionKind.NOTIFY_TESTER_CASEGEN or row.next_owner == "tester":
        return "tester"
    return "solver"


def _action_summary(record: Mapping[str, Any] | None) -> dict[str, str] | None:
    if record is None:
        return None
    action = dict(record.get("action") or {})
    return {
        "action_id": str(record.get("action_id") or ""),
        "state": str(record.get("state") or ""),
        "role": str(record.get("role") or ""),
        "candidate_version": str(action.get("candidate_version") or ""),
        "created_at": str(record.get("created_at") or ""),
    }


def _required_capability(next_command: str) -> str:
    match = re.search(r"required capability=([a-z0-9-]+)", next_command)
    return (
        f"required Flow capability is not registered: {match.group(1)}"
        if match
        else next_command
    )


def _delivery_stale_seconds(config: DaemonConfig) -> int:
    production = config.agent_execution.get("production", {})
    if not isinstance(production, Mapping):
        return 120
    try:
        return max(30, int(production.get("lease_seconds", 120)))
    except (TypeError, ValueError):
        return 120


def _action_age_seconds(record: Mapping[str, Any]) -> int | None:
    raw = str(record.get("created_at") or "").strip()
    if not raw:
        return None
    try:
        created = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        return max(0, int((datetime.now(timezone.utc) - created).total_seconds()))
    except ValueError:
        return None


def _classification(
    category: str,
    responsibility: str,
    is_blocker: bool,
    reason: str,
) -> dict[str, Any]:
    return {
        "category": category,
        "responsibility": responsibility,
        "is_blocker": is_blocker,
        "reason": reason,
    }


def _object_digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _read_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON document must be an object: {path}")
    return value
