from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from ascendop_daemon.core.filesystem import filesystem_path


SOURCE_IDENTITY_SCHEMA = "ascendop.agent-source-tree.v2"
IGNORED_PARTS = {
    ".git",
    ".pytest_cache",
    "__pycache__",
    "build",
    "dist",
    ".ascendop",
    ".ascendop-evidence",
    ".ascendop-output",
    "profiler_evidence",
}
WORKFLOW_EVIDENCE_SCHEMA = "ascendop.agent-workflow-evidence.v1"
WORKFLOW_EVIDENCE_SUFFIXES = (
    ".csv",
    ".json",
    ".jsonl",
    ".log",
    ".md",
    ".tsv",
    ".txt",
)
REFERENCE_PROJECTION_SCHEMA = "ascendop.operator-reference-projection.v1"
REFERENCE_EVIDENCE_SCHEMA = "ascendop.agent-reference-evidence.v1"
REFERENCE_EVIDENCE_SUFFIXES = (
    ".c",
    ".cc",
    ".cmake",
    ".cpp",
    ".cxx",
    ".h",
    ".hpp",
    ".json",
    ".md",
    ".py",
    ".sh",
    ".txt",
)


def build_agent_context_evidence(
    root: Path,
    *,
    operator: str,
    workspace: Path,
) -> dict[str, Any]:
    workflow_evidence = _workflow_evidence(root, operator)
    reference_projection = _reference_projection(root, operator, workspace)
    reference_evidence = _reference_evidence(root, reference_projection)
    return {
        "source_before_digest": tree_digest(workspace),
        "recent_results": _recent_results(root, operator),
        "official_evidence": _official_evidence(root, operator),
        "open_hypotheses": _open_hypotheses(root, operator),
        "workflow_evidence": workflow_evidence,
        "workflow_evidence_digest": _object_digest(workflow_evidence),
        "reference_projection": reference_projection,
        "reference_evidence": reference_evidence,
        "reference_evidence_digest": _object_digest(reference_evidence),
    }


def tree_digest(root: Path) -> str:
    digest = hashlib.sha256()
    if not root.is_dir():
        return digest.hexdigest()
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        if not path.is_file() or any(part in IGNORED_PARTS for part in path.parts):
            continue
        relative = path.relative_to(root).as_posix().encode("utf-8")
        payload = path.read_bytes()
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def _recent_results(root: Path, operator: str) -> list[dict[str, Any]]:
    result_root = root / "operators_testresult" / operator
    candidates = list(result_root.glob("*/RESULT.md")) if result_root.is_dir() else []
    candidates.sort(key=lambda path: (path.stat().st_mtime_ns, path.as_posix()), reverse=True)
    return [_text_evidence(root, path, max_chars=3000) for path in candidates[:10]]


def _official_evidence(root: Path, operator: str) -> list[dict[str, Any]]:
    operator_lower = operator.lower()
    patterns = (
        "operators/**/OFFICIAL_RESULTS_INDEX.json",
        "operators/**/OFFICIAL_MIGRATION_PLAN.json",
        "operators/**/official_submissions/**/TEST_RESULTS.json",
    )
    candidates: list[Path] = []
    for pattern in patterns:
        for path in root.glob(pattern):
            try:
                text = path.read_text(encoding="utf-8-sig", errors="replace")
            except OSError:
                continue
            if operator_lower in text.lower() or operator_lower in path.as_posix().lower():
                candidates.append(path)
    candidates = sorted(
        set(candidates),
        key=lambda path: (path.stat().st_mtime_ns, path.as_posix()),
        reverse=True,
    )
    return [_json_evidence(root, path, operator) for path in candidates[:12]]


def _open_hypotheses(root: Path, operator: str) -> list[dict[str, Any]]:
    candidates = [
        root / "reference" / "op_knowledge" / operator / "current_focus.md",
        root / "reference" / "op_knowledge" / operator / "case_coverage.md",
        root / "reference" / "op_knowledge" / operator / "optimization_lessons.md",
        root / "reference" / "op_knowledge" / operator / "hypothesis_backlog.md",
        root / "operators_workspace" / operator / "SOLVER_BLOCKER.md",
        root / "TestUtils" / "casegen" / operator / "SOLVER_BLOCKER.md",
    ]
    return [
        _text_evidence(root, path, max_chars=5000)
        for path in candidates
        if path.is_file()
    ]


def _workflow_evidence(root: Path, operator: str) -> list[dict[str, Any]]:
    case_root = root / "TestUtils" / "casegen" / operator / "case"
    if not case_root.is_dir():
        return []
    indexes = sorted(
        case_root.glob("*/PROFILER_EVIDENCE_INDEX.json"),
        key=lambda path: (path.stat().st_mtime_ns, path.as_posix()),
        reverse=True,
    )
    descriptors: list[dict[str, Any]] = []
    for index_path in indexes[:4]:
        try:
            payload = index_path.read_bytes()
            index = json.loads(payload.decode("utf-8-sig"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if (
            not isinstance(index, dict)
            or index.get("status") != "complete"
            or index.get("operator") != operator
        ):
            continue
        for target in index.get("targets", []):
            if not isinstance(target, dict) or target.get("status") != "complete":
                continue
            evidence_root = (root / str(target.get("evidence_path") or "")).resolve()
            try:
                relative_root = evidence_root.relative_to(root.resolve()).as_posix()
            except ValueError:
                continue
            if not evidence_root.is_dir():
                continue
            files, total_bytes, root_digest = _evidence_tree(
                evidence_root,
                suffixes=WORKFLOW_EVIDENCE_SUFFIXES,
            )
            descriptors.append(
                {
                    "schema": WORKFLOW_EVIDENCE_SCHEMA,
                    "kind": "profiler-evidence",
                    "index_path": index_path.relative_to(root).as_posix(),
                    "index_sha256": hashlib.sha256(payload).hexdigest(),
                    "root_path": relative_root,
                    "root_digest": root_digest,
                    "file_count": files,
                    "total_bytes": total_bytes,
                    "include_suffixes": list(WORKFLOW_EVIDENCE_SUFFIXES),
                    "case_version": str(index.get("case_version") or ""),
                    "result_version": str(index.get("blocker_result_version") or ""),
                    "blocker_generation": str(index.get("blocker_generation") or ""),
                }
            )
    diagnostic_indexes = sorted(
        case_root.glob("*/SOLVER_DIAGNOSTIC_INDEX.json"),
        key=lambda path: (path.stat().st_mtime_ns, path.as_posix()),
        reverse=True,
    )
    for index_path in diagnostic_indexes[:4]:
        try:
            index_payload = index_path.read_bytes()
            index = json.loads(index_payload.decode("utf-8-sig"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if (
            not isinstance(index, dict)
            or index.get("status") != "complete"
            or index.get("operator") != operator
        ):
            continue
        summary_path = (root / str(index.get("evidence_path") or "")).resolve()
        try:
            summary_relative = summary_path.relative_to(root.resolve()).as_posix()
            summary = json.loads(summary_path.read_text(encoding="utf-8-sig"))
        except (ValueError, OSError, json.JSONDecodeError):
            continue
        if (
            not isinstance(summary, dict)
            or summary.get("schema") != "ascendop.solver-diagnostic-evidence.v1"
            or summary.get("collection_status") != "complete"
            or summary.get("operator") != operator
        ):
            continue
        common = {
            "schema": WORKFLOW_EVIDENCE_SCHEMA,
            "index_path": index_path.relative_to(root).as_posix(),
            "index_sha256": hashlib.sha256(index_payload).hexdigest(),
            "include_suffixes": list(WORKFLOW_EVIDENCE_SUFFIXES),
            "case_version": str(index.get("case_version") or ""),
            "result_version": str(index.get("result_version") or ""),
            "blocker_generation": str(index.get("blocker_generation") or ""),
        }
        summary_root = summary_path.parent
        files, total_bytes, root_digest = _evidence_tree(
            summary_root,
            suffixes=WORKFLOW_EVIDENCE_SUFFIXES,
            include_paths=[summary_path.name],
        )
        descriptors.append(
            {
                **common,
                "kind": "solver-diagnostic-summary",
                "root_path": summary_root.relative_to(root).as_posix(),
                "root_digest": root_digest,
                "file_count": files,
                "total_bytes": total_bytes,
                "include_paths": [summary_path.name],
                "evidence_path": summary_relative,
            }
        )
        bundle_root = (root / str(summary.get("materialized_bundle") or "")).resolve()
        try:
            bundle_relative = bundle_root.relative_to(root.resolve()).as_posix()
        except ValueError:
            continue
        if not bundle_root.is_dir():
            continue
        files, total_bytes, root_digest = _evidence_tree(
            bundle_root,
            suffixes=WORKFLOW_EVIDENCE_SUFFIXES,
        )
        descriptors.append(
            {
                **common,
                "kind": "solver-diagnostic-bundle",
                "root_path": bundle_relative,
                "root_digest": root_digest,
                "file_count": files,
                "total_bytes": total_bytes,
                "include_paths": [],
                "evidence_path": summary_relative,
            }
        )
    return descriptors


def _reference_projection(
    root: Path,
    operator: str,
    workspace: Path,
) -> dict[str, Any]:
    index_path = workspace / ".ascendop" / "REFERENCE_MATERIALS.json"
    if not index_path.is_file():
        return {}
    payload = filesystem_path(index_path).read_bytes()
    try:
        document = json.loads(payload.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid reference projection: {index_path}") from exc
    if (
        not isinstance(document, dict)
        or document.get("schema") != REFERENCE_PROJECTION_SCHEMA
        or document.get("operator_id") != operator
        or not isinstance(document.get("sources"), list)
    ):
        raise ValueError(f"unsupported reference projection: {index_path}")
    try:
        relative = index_path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError as exc:
        raise ValueError("reference projection is outside the repository") from exc
    return {
        "path": relative,
        "sha256": hashlib.sha256(payload).hexdigest(),
        "document": document,
    }


def _reference_evidence(
    root: Path,
    projection: dict[str, Any],
) -> list[dict[str, Any]]:
    if not projection:
        return []
    document = projection["document"]
    descriptors: list[dict[str, Any]] = []
    for source in document["sources"]:
        if not isinstance(source, dict) or source.get("availability") != "available":
            continue
        source_id = str(source.get("source_id") or "")
        relative_root = str(source.get("root_path") or "")
        if not source_id or not relative_root:
            raise ValueError("available reference source is missing identity or root")
        evidence_root = (root / relative_root).resolve()
        try:
            bounded_root = evidence_root.relative_to(root.resolve()).as_posix()
        except ValueError as exc:
            raise ValueError("reference source is outside the repository") from exc
        if not evidence_root.is_dir():
            raise ValueError(f"reference source root is unavailable: {relative_root}")
        include_paths = source.get("include_paths", [])
        if not isinstance(include_paths, list) or not all(
            isinstance(item, str) and item for item in include_paths
        ):
            raise ValueError(f"reference include paths are invalid: {source_id}")
        files, total_bytes, root_digest = _evidence_tree(
            evidence_root,
            suffixes=REFERENCE_EVIDENCE_SUFFIXES,
            include_paths=include_paths,
        )
        descriptors.append(
            {
                "schema": REFERENCE_EVIDENCE_SCHEMA,
                "kind": "reference-material",
                "source_id": source_id,
                "index_path": projection["path"],
                "index_sha256": projection["sha256"],
                "root_path": bounded_root,
                "root_digest": root_digest,
                "file_count": files,
                "total_bytes": total_bytes,
                "include_suffixes": list(REFERENCE_EVIDENCE_SUFFIXES),
                "include_paths": include_paths,
                "relation": str(source.get("relation") or ""),
                "provenance": dict(source.get("provenance") or {}),
                "limitations": [str(item) for item in source.get("limitations", [])],
            }
        )
    return descriptors


def _evidence_tree(
    root: Path,
    *,
    suffixes: tuple[str, ...],
    include_paths: list[str] | None = None,
) -> tuple[int, int, str]:
    rows: list[dict[str, Any]] = []
    total_bytes = 0
    if include_paths:
        candidates = [(root / relative).resolve() for relative in include_paths]
    else:
        candidates = sorted(root.rglob("*"), key=lambda item: item.as_posix())
    for path in candidates:
        try:
            relative = path.relative_to(root.resolve()).as_posix()
        except ValueError as exc:
            raise ValueError("evidence path escaped its source root") from exc
        if not path.is_file() or path.suffix.lower() not in suffixes:
            if include_paths:
                raise ValueError(f"referenced evidence file is unavailable: {relative}")
            continue
        payload = filesystem_path(path).read_bytes()
        total_bytes += len(payload)
        rows.append(
            {
                "path": relative,
                "sha256": hashlib.sha256(payload).hexdigest(),
                "size": len(payload),
            }
        )
    return len(rows), total_bytes, _object_digest(rows)


def _object_digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _text_evidence(root: Path, path: Path, *, max_chars: int) -> dict[str, Any]:
    payload = path.read_bytes()
    text = payload.decode("utf-8-sig", errors="replace")
    return {
        "path": path.relative_to(root).as_posix(),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "excerpt": text[:max_chars],
    }


def _json_evidence(root: Path, path: Path, operator: str) -> dict[str, Any]:
    payload = path.read_bytes()
    try:
        document = json.loads(payload.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        document = {}
    return {
        "path": path.relative_to(root).as_posix(),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "operator_fragments": _matching_fragments(document, operator.lower(), limit=20),
    }


def _matching_fragments(value: Any, needle: str, *, limit: int) -> list[Any]:
    matches: list[Any] = []

    def visit(item: Any) -> None:
        if len(matches) >= limit:
            return
        if isinstance(item, dict):
            serialized = json.dumps(item, ensure_ascii=True, sort_keys=True)
            if needle in serialized.lower() and len(serialized) <= 12000:
                matches.append(item)
                return
            for child in item.values():
                visit(child)
        elif isinstance(item, list):
            for child in item:
                visit(child)

    visit(value)
    return matches
