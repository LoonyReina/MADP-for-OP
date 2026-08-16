from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
from collections import Counter
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from ascendop_daemon.runtime.control import stop_request_path
from ascendop_protocol.wire_v3 import canonical_digest, utc_now_iso
from ascendop_daemon.legacy.flow_v3_store import FlowV3Store, canonical_json


MIGRATION_SCHEMA = "ascendop.flow.offline-migration.v3"
OLD_ACTIVE_OR_UNCERTAIN = {
    "admitting",
    "staging-standby",
    "standby",
    "accepted",
    "running",
    "return-ready",
    "returned",
    "returned-awaiting-ingest",
    "return-lost",
    "standby-cancel-requested",
}
OLD_VERIFIED_TERMINAL = {
    "workflow-archived",
    "superseded-by-workflow-result",
    "canary-complete",
    "standby-cancelled",
    "cancelled",
    "cancelled-before-admission",
    "superseded-by-logical-attempt",
}
LEGACY_RUNTIME_ARTIFACT_NAMES = (
    "engine_pump_events.jsonl",
    "engine_pump_generation_gate.json",
    "engine_pump_status_current.json",
    "engine_pump_worker.json",
    "engine_admission_events.jsonl",
    "engine_admission_status_current.json",
)


class FlowV3MigrationError(RuntimeError):
    pass


def build_migration_plan(
    root: Path,
    *,
    source_database: Path,
    pump_state_path: Path,
    admission_state_path: Path,
) -> dict[str, Any]:
    root = root.resolve()
    pump = read_object(pump_state_path)
    admission = read_object(admission_state_path)
    entries = pump.get("entries", {})
    if not isinstance(entries, dict):
        entries = {}
    records: list[dict[str, Any]] = []
    classifications: Counter[str] = Counter()
    for engine_job_id, raw in sorted(entries.items()):
        if not isinstance(raw, dict):
            continue
        classification, reason, quarantine_state = classify_legacy_entry(raw)
        classifications[classification] += 1
        evidence = legacy_evidence(str(engine_job_id), raw)
        records.append(
            {
                "quarantine_id": (
                    "legacy-"
                    + hashlib.sha256(
                        (
                            str(engine_job_id)
                            + ":"
                            + canonical_digest(raw)
                        ).encode("utf-8")
                    ).hexdigest()[:32]
                ),
                "request_id": f"legacy:{engine_job_id}",
                "attempt_id": str(raw.get("attempt_id") or "attempt-unknown"),
                "classification": classification,
                "reason": reason,
                "state": quarantine_state,
                "evidence": evidence,
            }
        )
    database = inspect_legacy_database(source_database)
    source_digests = {
        "control_database_sha256": file_sha256(source_database),
        "pump_state_sha256": file_sha256(pump_state_path),
        "admission_state_sha256": file_sha256(admission_state_path),
    }
    state_root = pump_state_path.resolve().parent
    legacy_runtime_artifacts = [
        {
            "path": str(path.resolve()),
            "sha256": file_sha256(path),
        }
        for name in LEGACY_RUNTIME_ARTIFACT_NAMES
        if (path := state_root / name).is_file()
    ]
    plan: dict[str, Any] = {
        "schema": MIGRATION_SCHEMA,
        "mode": "offline-hard-cutover",
        "created_at": utc_now_iso(),
        "root": str(root),
        "sources": {
            "control_database": str(source_database.resolve()),
            "pump_state": str(pump_state_path.resolve()),
            "admission_state": str(admission_state_path.resolve()),
            "legacy_runtime_artifacts": legacy_runtime_artifacts,
            **source_digests,
        },
        "legacy_database": database,
        "legacy_admission": {
            "protocol_version": str(admission.get("protocol_version") or ""),
            "enabled": bool(admission.get("enabled")),
            "draining": bool(admission.get("draining")),
            "endpoint_id": str(admission.get("endpoint_id") or ""),
            "job_count": len(
                admission.get("jobs", {})
                if isinstance(admission.get("jobs"), dict)
                else {}
            ),
        },
        "classification_counts": dict(sorted(classifications.items())),
        "open_quarantine_count": sum(
            1 for item in records if item["state"] == "open"
        ),
        "resolved_legacy_count": sum(
            1 for item in records if item["state"] == "resolved"
        ),
        "records": records,
    }
    plan["plan_digest"] = canonical_digest(plan)
    return plan


def classify_legacy_entry(
    record: Mapping[str, Any],
) -> tuple[str, str, str]:
    state = str(record.get("state") or "")
    if state in OLD_ACTIVE_OR_UNCERTAIN:
        return (
            "uncertain-accepted",
            f"legacy state {state} may have been published or executed",
            "open",
        )
    if state in OLD_VERIFIED_TERMINAL:
        if state in {"workflow-archived", "superseded-by-workflow-result"}:
            if not (
                record.get("ingested_at")
                or record.get("existing_result_evidence")
            ):
                return (
                    "terminal-evidence-incomplete",
                    f"legacy terminal state {state} lacks durable ingest evidence",
                    "open",
                )
        return (
            "verified-terminal",
            f"legacy terminal state {state} is sealed read-only",
            "resolved",
        )
    if state == "admission-failed":
        if record.get("accepted_at") or record.get("remote_required_returned_at"):
            return (
                "uncertain-admission-failure",
                "legacy admission failed after possible remote publication",
                "open",
            )
        return (
            "verified-prepublish-failure",
            "legacy admission failed without accepted/return evidence",
            "resolved",
        )
    return (
        "unknown-legacy-state",
        f"unrecognized legacy state {state or '<empty>'}",
        "open",
    )


def legacy_evidence(
    engine_job_id: str,
    record: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "engine_job_id": engine_job_id,
        "record_digest": canonical_digest(record),
        "state": str(record.get("state") or ""),
        "operator": str(record.get("operator") or ""),
        "test_version": str(record.get("test_version") or ""),
        "attempt_id": str(record.get("attempt_id") or ""),
        "job_kind": str(record.get("job_kind") or ""),
        "accepted_at": str(record.get("accepted_at") or ""),
        "engine_terminal_state": str(
            record.get("engine_terminal_state") or ""
        ),
        "ingest_outcome": str(record.get("ingest_outcome") or ""),
        "ingested_at": str(record.get("ingested_at") or ""),
        "remote_ack_state": str(record.get("remote_ack_state") or ""),
        "remote_required_ack_state": str(
            record.get("remote_required_ack_state") or ""
        ),
        "return_receipt_id": str(record.get("return_receipt_id") or ""),
        "required_bundle_root": str(
            record.get("required_bundle_root") or ""
        ),
        "optional_bundle_root": str(
            record.get("optional_bundle_root") or ""
        ),
        "last_error": str(record.get("last_error") or "")[-2000:],
    }


def apply_migration_plan(
    plan: Mapping[str, Any],
    *,
    destination_database: Path,
) -> dict[str, Any]:
    verify_migration_plan(plan, verify_sources=False)
    destination_database = destination_database.resolve()
    if destination_database.exists():
        raise FlowV3MigrationError(
            f"migration destination already exists: {destination_database}"
        )
    store = FlowV3Store(destination_database)
    store.initialize()
    now = utc_now_iso()
    with store.transaction() as conn:
        for raw in plan.get("records", []):
            if not isinstance(raw, Mapping):
                continue
            resolved_at = now if str(raw.get("state")) == "resolved" else ""
            conn.execute(
                """
                INSERT INTO flow_v3_quarantine(
                    quarantine_id, request_id, attempt_id, classification,
                    reason, source, evidence_json, state, created_at, resolved_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(raw["quarantine_id"]),
                    str(raw["request_id"]),
                    str(raw["attempt_id"]),
                    str(raw["classification"]),
                    str(raw["reason"]),
                    "flow-v3-offline-migration",
                    canonical_json(dict(raw.get("evidence", {}))),
                    str(raw["state"]),
                    now,
                    resolved_at,
                ),
            )
        conn.execute(
            """
            INSERT INTO flow_v3_metadata(key, value)
            VALUES('legacy_migration_digest', ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (str(plan["plan_digest"]),),
        )
        conn.execute(
            """
            INSERT INTO flow_v3_metadata(key, value)
            VALUES('legacy_migration_applied_at', ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (now,),
        )
    return {
        "database": str(destination_database),
        "plan_digest": str(plan["plan_digest"]),
        "store": store.status(),
    }


def reconcile_quarantine_classifications(
    plan: Mapping[str, Any],
    *,
    destination_database: Path,
) -> dict[str, Any]:
    """Backfill the exact archived migration classification after schema upgrade."""

    verify_migration_plan(plan, verify_sources=False)
    store = FlowV3Store(destination_database)
    store.initialize()
    expected_digest = str(plan["plan_digest"])
    updated = 0
    with store.transaction() as conn:
        applied = conn.execute(
            "SELECT value FROM flow_v3_metadata "
            "WHERE key='legacy_migration_digest'"
        ).fetchone()
        if applied is None or str(applied[0]) != expected_digest:
            raise FlowV3MigrationError(
                "active database migration digest does not match the plan"
            )
        for raw in plan.get("records", []):
            if not isinstance(raw, Mapping):
                continue
            cursor = conn.execute(
                """
                UPDATE flow_v3_quarantine
                SET classification=?
                WHERE quarantine_id=? AND request_id=? AND attempt_id=?
                """,
                (
                    str(raw["classification"]),
                    str(raw["quarantine_id"]),
                    str(raw["request_id"]),
                    str(raw["attempt_id"]),
                ),
            )
            if cursor.rowcount != 1:
                raise FlowV3MigrationError(
                    "active quarantine identity does not match the plan: "
                    f"{raw['quarantine_id']}"
                )
            updated += 1
    return {
        "database": str(destination_database.resolve()),
        "plan_digest": expected_digest,
        "updated": updated,
        "store": store.status(),
    }


def hard_cutover(
    root: Path,
    *,
    plan: Mapping[str, Any],
    source_database: Path,
    staging_database: Path,
    archive_root: Path,
) -> dict[str, Any]:
    root = root.resolve()
    verify_migration_plan(plan, verify_sources=True)
    stop_request = stop_request_path(root)
    if not stop_request.is_file():
        raise FlowV3MigrationError(
            "hard cutover requires an active workflow stop fence"
        )
    source_database = source_database.resolve()
    staging_database = staging_database.resolve()
    if source_database == staging_database:
        raise FlowV3MigrationError("source and staging databases must differ")
    if not source_database.is_file() or not staging_database.is_file():
        raise FlowV3MigrationError(
            "hard cutover requires source and staging databases"
        )
    staging_store = FlowV3Store(staging_database)
    status = staging_store.status()
    with staging_store.connection() as conn:
        row = conn.execute(
            "SELECT value FROM flow_v3_metadata "
            "WHERE key='legacy_migration_digest'"
        ).fetchone()
    if row is None or str(row[0]) != str(plan["plan_digest"]):
        raise FlowV3MigrationError(
            "staging database migration digest does not match the plan"
        )
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archive = (archive_root / stamp).resolve()
    archive.mkdir(parents=True, exist_ok=False)
    write_json(archive / "MIGRATION_PLAN.json", dict(plan))
    artifact_sources = [
        Path(str(plan["sources"]["pump_state"])),
        Path(str(plan["sources"]["admission_state"])),
    ]
    for raw in plan["sources"].get("legacy_runtime_artifacts", []):
        if isinstance(raw, Mapping):
            artifact_sources.append(Path(str(raw.get("path") or "")))
    moved_artifacts: list[tuple[Path, Path]] = []
    archived_database = archive / source_database.name
    try:
        for source in artifact_sources:
            if not source.is_file():
                continue
            archived = archive / source.name
            os.replace(source, archived)
            moved_artifacts.append((source, archived))
        os.replace(source_database, archived_database)
        os.replace(staging_database, source_database)
    except Exception:
        if archived_database.is_file() and not source_database.exists():
            os.replace(archived_database, source_database)
        for source, archived in reversed(moved_artifacts):
            if archived.is_file() and not source.exists():
                os.replace(archived, source)
        raise
    result = {
        "schema": "ascendop.flow.hard-cutover-result.v3",
        "cutover_at": utc_now_iso(),
        "plan_digest": str(plan["plan_digest"]),
        "archive": str(archive),
        "active_database": str(source_database),
        "active_store": FlowV3Store(source_database).status(),
        "staging_store": status,
    }
    write_json(archive / "CUTOVER_RESULT.json", result)
    return result


def verify_migration_plan(
    plan: Mapping[str, Any],
    *,
    verify_sources: bool,
) -> None:
    if str(plan.get("schema") or "") != MIGRATION_SCHEMA:
        raise FlowV3MigrationError("unsupported migration plan schema")
    expected_digest = str(plan.get("plan_digest") or "")
    digest_input = dict(plan)
    digest_input.pop("plan_digest", None)
    actual_digest = canonical_digest(digest_input)
    if not expected_digest or actual_digest != expected_digest:
        raise FlowV3MigrationError("migration plan digest is invalid")
    if not verify_sources:
        return
    sources = plan.get("sources", {})
    if not isinstance(sources, Mapping):
        raise FlowV3MigrationError("migration plan sources are invalid")
    for path_key, digest_key in (
        ("control_database", "control_database_sha256"),
        ("pump_state", "pump_state_sha256"),
        ("admission_state", "admission_state_sha256"),
    ):
        source = Path(str(sources.get(path_key) or "")).resolve()
        expected = str(sources.get(digest_key) or "")
        actual = file_sha256(source)
        if actual != expected:
            raise FlowV3MigrationError(
                f"migration source changed after planning: {path_key}"
            )
    artifacts = sources.get("legacy_runtime_artifacts", [])
    if not isinstance(artifacts, list):
        raise FlowV3MigrationError(
            "migration plan legacy runtime artifacts are invalid"
        )
    for index, raw in enumerate(artifacts):
        if not isinstance(raw, Mapping):
            raise FlowV3MigrationError(
                f"migration legacy runtime artifact {index} is invalid"
            )
        source = Path(str(raw.get("path") or "")).resolve()
        expected = str(raw.get("sha256") or "")
        if file_sha256(source) != expected:
            raise FlowV3MigrationError(
                "migration source changed after planning: "
                f"legacy_runtime_artifacts[{index}]"
            )


def inspect_legacy_database(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"exists": False, "path": str(path.resolve())}
    try:
        with closing(sqlite3.connect(str(path))) as conn:
            tables = [
                str(row[0])
                for row in conn.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type='table' ORDER BY name"
                )
            ]
            counts = {}
            for table in tables:
                if not table.replace("_", "").isalnum():
                    continue
                counts[table] = int(
                    conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
                )
    except sqlite3.Error as exc:
        raise FlowV3MigrationError(
            f"legacy control database is unreadable: {exc}"
        ) from exc
    return {
        "exists": True,
        "path": str(path.resolve()),
        "sha256": file_sha256(path),
        "tables": tables,
        "row_counts": counts,
    }


def read_object(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FlowV3MigrationError(f"migration source is unreadable: {path}") from exc
    if not isinstance(value, dict):
        raise FlowV3MigrationError(f"migration source is not an object: {path}")
    return value


def file_sha256(path: Path) -> str:
    if not path.is_file():
        return ""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            block = handle.read(1024 * 1024)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(
            dict(value),
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def default_paths(root: Path) -> dict[str, Path]:
    state = root / "TestUtils" / "tester_daemon"
    return {
        "source_database": state / "control.sqlite3",
        "staging_database": state / "control.v3.staging.sqlite3",
        "pump_state": state / "engine_pump_state.json",
        "admission_state": state / "engine_admission_state.json",
        "archive_root": state / "legacy_archive",
        "plan": state / "flow_v3" / "MIGRATION_PLAN.json",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="AscendOP Flow V3 offline migrator")
    parser.add_argument("--root", default=".")
    parser.add_argument(
        "action",
        choices=("plan", "stage", "cutover", "reconcile-classifications"),
    )
    args = parser.parse_args(argv)
    root = Path(args.root).resolve()
    paths = default_paths(root)
    if args.action == "plan":
        plan = build_migration_plan(
            root,
            source_database=paths["source_database"],
            pump_state_path=paths["pump_state"],
            admission_state_path=paths["admission_state"],
        )
        write_json(paths["plan"], plan)
        print(json.dumps(plan, ensure_ascii=True, indent=2, sort_keys=True))
        return 0
    plan = read_object(paths["plan"])
    if args.action == "stage":
        result = apply_migration_plan(
            plan,
            destination_database=paths["staging_database"],
        )
    elif args.action == "reconcile-classifications":
        result = reconcile_quarantine_classifications(
            plan,
            destination_database=paths["source_database"],
        )
    else:
        result = hard_cutover(
            root,
            plan=plan,
            source_database=paths["source_database"],
            staging_database=paths["staging_database"],
            archive_root=paths["archive_root"],
        )
    print(json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
