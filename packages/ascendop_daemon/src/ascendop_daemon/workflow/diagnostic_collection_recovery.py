from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from ascendop_daemon.core.atomic_io import write_json_atomic
from ascendop_daemon.workflow.diagnostic_artifacts import (
    diagnostic_collection_complete,
)
from ascendop_daemon.workflow.solver_diagnostic_paths import utc_now_iso


def reconcile_failed_collection_from_evidence(
    root: Path,
    state: Mapping[str, Any],
) -> dict[str, Any]:
    """Converge a failed collection when its immutable blob is now complete."""

    updated = dict(state)
    if str(updated.get("status") or "") != "failed" or not updated.get(
        "missing_requested_artifacts"
    ):
        return updated
    evidence_path = str(updated.get("evidence_path") or "")
    if not evidence_path:
        return updated
    evidence_file = (root / evidence_path).resolve()
    if root.resolve() not in evidence_file.parents:
        return updated
    evidence = _read_object(evidence_file)
    materialized_relative = str(evidence.get("materialized_bundle") or "")
    if not materialized_relative:
        return updated
    materialized_root = (root / materialized_relative).resolve()
    if root.resolve() not in materialized_root.parents:
        return updated
    complete, missing = diagnostic_collection_complete(updated, materialized_root)
    if not complete:
        return updated

    now = utc_now_iso()
    reconciliations = [
        dict(item)
        for item in updated.get("collection_reconciliations", [])
        if isinstance(item, Mapping)
    ]
    reconciliations.append(
        {
            "reason": "durable-partial-artifact-recovered",
            "evidence_path": evidence_path,
            "reconciled_at": now,
        }
    )
    updated.update(
        {
            "status": "complete",
            "collection_status": "complete",
            "missing_requested_artifacts": missing,
            "last_error": "",
            "collection_reconciliations": reconciliations,
            "updated_at": now,
        }
    )
    evidence_reconciliations = [
        dict(item)
        for item in evidence.get("collection_reconciliations", [])
        if isinstance(item, Mapping)
    ]
    evidence_reconciliations.append(reconciliations[-1])
    evidence.update(
        {
            "collection_status": "complete",
            "missing_requested_artifacts": missing,
            "collection_reconciliations": evidence_reconciliations,
        }
    )
    write_json_atomic(
        evidence_file,
        evidence,
        ensure_ascii=True,
        sort_keys=True,
    )
    return updated


def _read_object(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return raw if isinstance(raw, dict) else {}


__all__ = ["reconcile_failed_collection_from_evidence"]
