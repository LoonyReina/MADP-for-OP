"""Atomic case-version scaffolds for operator-aware Tester workflows."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class CaseVersionScaffoldError(RuntimeError):
    """Raised when an inherited case-version scaffold is not trustworthy."""


@dataclass(frozen=True)
class CaseVersionScaffold:
    source_case_version: str
    target_case_version: str
    payload_digest: str
    copied_payloads: tuple[str, ...]


def inherit_case_version_scaffold(
    source_dir: Path,
    target_dir: Path,
    *,
    operator_id: str,
    target_case_version: str,
    case_protocol: str,
    generated_at: str,
    generator: str,
) -> CaseVersionScaffold:
    """Create a fresh lifetime from immutable payloads of the previous case.

    Descriptive operator specifications cannot be safely reduced to generic
    symbolic bounds. In that situation the Tester starts from the last audited
    payload, while all version identity and evidence are deliberately reset.
    """

    source_dir = source_dir.resolve()
    target_dir = target_dir.resolve()
    if target_dir.exists():
        raise CaseVersionScaffoldError(f"target already exists: {target_dir}")
    source_meta = _read_object(source_dir / "meta.json")
    source_cases = _read_array(source_dir / "cases.json")
    source_operator = str(source_meta.get("op") or "")
    source_protocol = str(source_meta.get("case_protocol") or "")
    source_case_version = str(source_meta.get("case_version") or source_dir.name)
    if source_operator != operator_id:
        raise CaseVersionScaffoldError(
            f"source operator mismatch: expected {operator_id}, got {source_operator}"
        )
    if source_protocol != case_protocol:
        raise CaseVersionScaffoldError(
            f"source protocol mismatch: expected {case_protocol}, got {source_protocol}"
        )
    if not source_cases:
        raise CaseVersionScaffoldError("source cases.json is empty")

    payload_names = ["cases.json"]
    for case in source_cases:
        if not isinstance(case, dict):
            raise CaseVersionScaffoldError("source cases.json contains a non-object case")
        bucket = str(case.get("bucket") or "")
        if not bucket:
            raise CaseVersionScaffoldError("source case is missing bucket identity")
        payload_names.append(f"case_{bucket}.json")
    payload_names = list(dict.fromkeys(payload_names))
    payload_digest = _payload_digest(source_dir, payload_names)

    target_meta = dict(source_meta)
    target_meta.update(
        {
            "op": operator_id,
            "case_version": target_case_version,
            "usage_count": 0,
            "usage_history": [],
            "generated_at": generated_at,
            "generator": generator,
            "creation_evidence": (
                f"Inherited immutable case payload scaffold from {source_case_version}; "
                "operator-aware coverage and model evidence are pending the real Tester gate."
            ),
            "scaffold_source": {
                "case_version": source_case_version,
                "payload_digest": payload_digest,
                "inheritance": "immutable-payload-only",
            },
        }
    )
    diversity = target_meta.get("diversity_audit")
    if isinstance(diversity, dict):
        target_meta["diversity_audit"] = {
            "official_checkpoint_path": str(
                diversity.get("official_checkpoint_path") or ""
            ),
            "predicted_official_transition": "",
            "strongest_counter_hypothesis": "",
            "novel_dimensions": [],
            "template_profile": str(diversity.get("template_profile") or ""),
        }

    target_dir.parent.mkdir(parents=True, exist_ok=True)
    staging = target_dir.parent / f".{target_dir.name}.staging-{uuid.uuid4().hex}"
    try:
        staging.mkdir()
        for name in payload_names:
            shutil.copy2(source_dir / name, staging / name)
        _write_json(staging / "meta.json", target_meta)
        (staging / "README.md").write_text(
            f"# {operator_id} {target_case_version}\n\n"
            f"Payload scaffold inherited from `{source_case_version}`. The Tester must "
            "complete the daemon-delivered evidence gate before this lifetime can be used.\n",
            encoding="utf-8",
        )
        (staging / "CASEGEN_PLAN.md").write_text(
            f"# Casegen Plan {target_case_version}\n\nStatus: pending real Tester evidence gate.\n",
            encoding="utf-8",
        )
        (staging / "MODEL_AUDIT.md").write_text(
            f"# Model Audit {target_case_version}\n\nStatus: pending real Tester evidence gate.\n",
            encoding="utf-8",
        )
        os.replace(staging, target_dir)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return CaseVersionScaffold(
        source_case_version=source_case_version,
        target_case_version=target_case_version,
        payload_digest=payload_digest,
        copied_payloads=tuple(payload_names),
    )


def _payload_digest(root: Path, names: list[str]) -> str:
    digest = hashlib.sha256()
    for name in sorted(names):
        path = root / name
        if not path.is_file():
            raise CaseVersionScaffoldError(f"source payload is missing: {path}")
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CaseVersionScaffoldError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise CaseVersionScaffoldError(f"expected JSON object: {path}")
    return value


def _read_array(path: Path) -> list[Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CaseVersionScaffoldError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, list):
        raise CaseVersionScaffoldError(f"expected JSON array: {path}")
    return value


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
