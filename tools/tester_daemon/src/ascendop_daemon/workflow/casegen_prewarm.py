from __future__ import annotations

import hashlib
import inspect
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

from ascendop_daemon.workflow.case_bundle_contract import (
    case_bundle_validator_due,
    require_case_bundle_contract,
)
from ascendop_daemon.legacy.engine_job_builder import (
    EngineJobBuildError,
    copy_tree_without_symlinks,
    tree_digest,
)


SNAPSHOT_PROTOCOL = "casegen-cache-snapshot-v2"
SOURCE_INDEPENDENT_MARKER = ".ascendop-case-cache-prewarm"


def materialize_casegen_cache_snapshot(
    root: Path,
    *,
    op: str,
    season: str,
    case_dir: Path,
) -> dict[str, Any]:
    """Create an immutable, source-independent payload for CPU golden prewarm."""

    root = root.resolve()
    case_dir = case_dir.resolve()
    task_case = (root / "operators" / season / "case_910b" / op).resolve()
    if not task_case.is_dir():
        raise EngineJobBuildError(f"casegen prewarm task case is missing: {task_case}")
    if not (task_case / "test_op.py").is_file():
        raise EngineJobBuildError(
            f"casegen prewarm official test_op.py is missing: {task_case / 'test_op.py'}"
        )
    expected_case_root = (root / "TestUtils" / "casegen" / op / "case").resolve()
    if expected_case_root != case_dir.parent:
        raise EngineJobBuildError(
            f"casegen prewarm case directory escapes operator root: {case_dir}"
        )
    for name in ("meta.json", "cases.json", "CASEGEN_PLAN.md", "MODEL_AUDIT.md"):
        if not (case_dir / name).is_file():
            raise EngineJobBuildError(
                f"casegen prewarm evidence is incomplete: {case_dir / name}"
            )
    try:
        contract = require_case_bundle_contract(
            task_case,
            case_dir,
            op=op,
            case_version=case_dir.name,
            run_validator=case_bundle_validator_due(case_dir),
        )
    except ValueError as exc:
        raise EngineJobBuildError(
            f"casegen prewarm case bundle contract failed: {exc}"
        ) from exc

    workflow = _workflow_module()
    validator_support = (
        root / "reference" / "op_knowledge" / op
    ).resolve()
    identity = {
        "protocol_version": SNAPSHOT_PROTOCOL,
        "operator": op,
        "season": season,
        "case_version": case_dir.name,
        "official_task_case_sha256": tree_digest(task_case),
        "casegen_content_sha256": _casegen_content_sha256(case_dir),
        "validator_support_sha256": (
            tree_digest(validator_support)
            if validator_support.is_dir()
            else ""
        ),
        "case_bundle_contract": contract.to_dict(),
        "fusion_generation_sha256": _fusion_generation(workflow),
    }
    fingerprint = _object_sha256(identity)
    snapshots_root = (
        root
        / "TestUtils"
        / "tester_daemon"
        / "case_cache_snapshots"
        / op
        / case_dir.name
    )
    snapshot_root = snapshots_root / fingerprint[:20]
    manifest_path = snapshot_root / "CASE_CACHE_SNAPSHOT.json"
    snapshots_root.mkdir(parents=True, exist_ok=True)
    existing = _read_json_object(manifest_path)
    if (
        existing.get("fingerprint") == fingerprint
        and (snapshot_root / "task_case" / "test_op.py").is_file()
        and (snapshot_root / "attack_case" / "meta.json").is_file()
        and (
            snapshot_root
            / "pending_snapshot"
            / "source_snapshot"
            / SOURCE_INDEPENDENT_MARKER
        ).is_file()
    ):
        if _existing_materialized_contract_is_current(existing, snapshot_root):
            return {
                **existing,
                "snapshot_root": str(snapshot_root),
                "cache_hit": True,
            }
        try:
            materialized_contract = _validate_materialized_snapshot(
                root,
                snapshots_root,
                snapshot_root / "task_case",
                snapshot_root / "attack_case",
                op=op,
                season=season,
                case_version=case_dir.name,
            )
        except EngineJobBuildError:
            pass
        else:
            return {
                **existing,
                "materialized_case_bundle_contract": materialized_contract,
                "snapshot_root": str(snapshot_root),
                "cache_hit": True,
            }

    temporary = Path(
        tempfile.mkdtemp(prefix=f".{fingerprint[:12]}.", dir=str(snapshots_root))
    )
    try:
        source_snapshot = temporary / "pending_snapshot" / "source_snapshot"
        source_snapshot.mkdir(parents=True)
        (source_snapshot / SOURCE_INDEPENDENT_MARKER).write_text(
            "source-independent case-cache prewarm\n",
            encoding="ascii",
        )
        copy_tree_without_symlinks(task_case, temporary / "task_case")
        # Runtime profiler captures are historical evidence, not executable
        # case semantics.  Copying them into a prewarm snapshot bloats every
        # request and can exceed the Windows path limit before transport.
        copy_tree_without_symlinks(
            case_dir,
            temporary / "attack_case",
            exclude_names=("profiler_evidence",),
        )
        workflow.fuse_attack_cases_into_test_op(
            temporary / "task_case" / "test_op.py",
            temporary / "attack_case",
            op,
            case_dir.name,
            False,
        )
        valid, detail = workflow.attack_fusion_order_ok(
            temporary / "task_case" / "test_op.py"
        )
        if not valid:
            raise EngineJobBuildError(
                f"casegen prewarm fusion validation failed: {detail}"
            )
        materialized_contract = _validate_materialized_snapshot(
            root,
            snapshots_root,
            temporary / "task_case",
            temporary / "attack_case",
            op=op,
            season=season,
            case_version=case_dir.name,
        )
        manifest = {
            **identity,
            "fingerprint": fingerprint,
            "fused_test_op_sha256": _file_sha256(
                temporary / "task_case" / "test_op.py"
            ),
            "materialized_case_bundle_contract": materialized_contract,
            "snapshot_root": str(snapshot_root),
        }
        (temporary / manifest_path.name).write_text(
            json.dumps(manifest, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        if snapshot_root.exists():
            _remove_snapshot_path(snapshot_root, snapshots_root)
        os.replace(temporary, snapshot_root)
        return {**manifest, "cache_hit": False}
    finally:
        if temporary.exists():
            _remove_snapshot_path(temporary, snapshots_root)


def _workflow_module() -> Any:
    from scripts import next_workflow

    return next_workflow


def _validate_materialized_snapshot(
    root: Path,
    snapshots_root: Path,
    task_case: Path,
    attack_case: Path,
    *,
    op: str,
    season: str,
    case_version: str,
) -> dict[str, Any]:
    """Validate copied executable files without changing validator root semantics."""

    validation_root = Path(
        tempfile.mkdtemp(prefix=".validate-", dir=str(snapshots_root))
    )
    try:
        mirrored_task_case = (
            validation_root / "operators" / season / "case_910b" / op
        )
        mirrored_casegen_root = (
            validation_root
            / "TestUtils"
            / "casegen"
            / op
        )
        validator_support = root / "reference" / "op_knowledge" / op
        if validator_support.is_dir():
            copy_tree_without_symlinks(
                validator_support,
                validation_root / "reference" / "op_knowledge" / op,
            )
        source_casegen_root = root / "TestUtils" / "casegen" / op
        if source_casegen_root.is_dir():
            copy_tree_without_symlinks(
                source_casegen_root,
                mirrored_casegen_root,
                exclude_names=("profiler_evidence",),
            )
        mirrored_case_dir = (
            mirrored_casegen_root
            / "case"
            / case_version
        )
        copy_tree_without_symlinks(task_case, mirrored_task_case)
        if mirrored_case_dir.exists():
            shutil.rmtree(mirrored_case_dir)
        copy_tree_without_symlinks(attack_case, mirrored_case_dir)
        _normalize_case_lifetime_metadata(mirrored_casegen_root / "case")
        try:
            contract = require_case_bundle_contract(
                mirrored_task_case,
                mirrored_case_dir,
                op=op,
                case_version=case_version,
                run_validator=True,
            )
        except ValueError as exc:
            raise EngineJobBuildError(
                "casegen prewarm materialized case bundle contract failed: "
                f"{exc}"
            ) from exc
        result = contract.to_dict()
        result["validator_path"] = (
            str(task_case / "validate_package.py")
            if (task_case / "validate_package.py").is_file()
            else ""
        )
        return result
    finally:
        _remove_snapshot_path(validation_root, snapshots_root)


def _existing_materialized_contract_is_current(
    existing: dict[str, Any],
    snapshot_root: Path,
) -> bool:
    contract = existing.get("materialized_case_bundle_contract")
    if not isinstance(contract, dict) or contract.get("valid") is not True:
        return False
    fused_test_op = snapshot_root / "task_case" / "test_op.py"
    attack_cases = snapshot_root / "attack_case" / "cases.json"
    sidecar = snapshot_root / "task_case" / "case_specs.json"
    if not fused_test_op.is_file() or not attack_cases.is_file():
        return False
    if existing.get("fused_test_op_sha256") != _file_sha256(fused_test_op):
        return False
    if contract.get("cases_sha256") != _file_sha256(attack_cases):
        return False
    expected_sidecar = _file_sha256(sidecar) if sidecar.is_file() else ""
    return contract.get("task_case_sidecar_sha256") == expected_sidecar


def _normalize_case_lifetime_metadata(case_root: Path) -> None:
    for meta_path in case_root.glob("*/meta.json"):
        meta = _read_json_object(meta_path)
        if not meta:
            continue
        changed = False
        if "usage_count" in meta and meta.get("usage_count") != 0:
            meta["usage_count"] = 0
            changed = True
        if "usage_history" in meta and meta.get("usage_history") != []:
            meta["usage_history"] = []
            changed = True
        if changed:
            meta_path.write_text(
                json.dumps(meta, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )


def _fusion_generation(workflow: Any) -> str:
    names = (
        "official_case_count",
        "attack_case_expr",
        "inplace_update_index_values",
        "attack_case_attr_expr",
        "attack_cases_update_block",
        "attack_case_start_index",
        "insert_attack_fusion_block",
        "attack_fusion_order_ok",
        "fuse_attack_cases_into_test_op",
        "read_attack_cases_for_fusion",
    )
    payload = {
        "case_protocol": workflow.CASE_PROTOCOL_ID,
        "buckets": workflow.BUCKETS,
        "bucket_ranges": workflow.BUCKET_RANGES,
        "functions": {
            name: inspect.getsource(getattr(workflow, name)) for name in names
        },
    }
    return _object_sha256(payload)


def _object_sha256(value: object) -> str:
    payload = json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _casegen_content_sha256(case_dir: Path) -> str:
    """Hash executable case semantics while ignoring mutable lifetime counters."""

    cases = _read_json_value(case_dir / "cases.json")
    meta = _read_json_object(case_dir / "meta.json")
    contract_fields = (
        "op",
        "case_version",
        "case_protocol",
        "official_case_policy",
        "default_correctness_range",
        "default_perf_case_range",
        "correctness_repetitions",
        "performance_samples_per_case",
        "perf_weighted_time_weights",
        "buckets",
        "case_contract",
    )
    return _object_sha256(
        {
            "cases": cases,
            "contract": {name: meta.get(name) for name in contract_fields},
        }
    )


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json_object(path: Path) -> dict[str, Any]:
    value = _read_json_value(path)
    return value if isinstance(value, dict) else {}


def _read_json_value(path: Path) -> Any:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError, json.JSONDecodeError):
        return None


def _remove_snapshot_path(path: Path, snapshots_root: Path) -> None:
    target = path.resolve()
    root = snapshots_root.resolve()
    if target == root or root not in target.parents:
        raise EngineJobBuildError(
            f"refusing to remove case-cache snapshot outside its root: {target}"
        )
    shutil.rmtree(target)
