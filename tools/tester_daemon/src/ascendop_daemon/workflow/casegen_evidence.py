from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ascendop_daemon.workflow.case_bundle_contract import (
    case_bundle_validator_due,
    validate_case_bundle_contract,
)
from ascendop_daemon.core.models import (
    ActionKind,
    BoardRow,
    DaemonConfig,
    GateDecision,
    casegen_role_enabled,
    extract_row_test_version,
    operator_season,
    operator_session,
    workflow_mode_for,
)


REQUIRED_CASEGEN_EVIDENCE = (
    "meta.json",
    "cases.json",
    "CASEGEN_PLAN.md",
    "MODEL_AUDIT.md",
)
MISSING_CASEGEN_DIR_EVIDENCE = ("case_version_dir",) + REQUIRED_CASEGEN_EVIDENCE
POST_CASEGEN_GATES = {"needs-pending-candidate"}
PRE_SUBMIT_ACTIONS = {ActionKind.PREPARE_SUBMIT, ActionKind.DISPATCH_SUBMIT}
FIVE_CASE_V2 = "five_case_v2"
SIXTEEN_CASE_DUAL_BAND = "sixteen_case_dual_band_v1"
OFFICIAL_FAILURE_DIVERSITY = "official_failure_cluster_v1"
OFFICIAL_TEMPLATE_PROFILE = "cannjudge-cann90-official-template-v1"
V2_ORTHOGONALITY_ENFORCED_AFTER = "2026-07-11"
V2_ORTHOGONALITY_DIMENSIONS = (
    "dtype_space",
    "rank_space",
    "axis_space",
    "layout_space",
    "alignment_space",
    "input_space",
    "semantic_path",
    "value_space",
)


@dataclass(frozen=True)
class CasegenEvidenceIssue:
    op: str
    case_version: str
    case_dir: Path
    missing: tuple[str, ...]

    def relative_case_dir(self, root: Path) -> str:
        try:
            return str(self.case_dir.relative_to(root))
        except ValueError:
            return str(self.case_dir)


def case_version_sort_key(value: str) -> list[int | str]:
    return [int(item) if item.isdigit() else item for item in re.split(r"(\d+)", value)]


def latest_case_dir(root: Path, op: str) -> Path | None:
    case_root = root / "TestUtils" / "casegen" / op / "case"
    if not case_root.exists():
        return None
    candidates = [
        path
        for path in case_root.iterdir()
        if path.is_dir() and re.fullmatch(r"case_v[0-9A-Za-z_]+", path.name)
    ]
    if not candidates:
        return None
    return sorted(candidates, key=lambda item: case_version_sort_key(item.name))[-1]


def latest_casegen_evidence_issue(
    root: Path,
    op: str,
    config: DaemonConfig | None = None,
    *,
    season: str = "",
) -> CasegenEvidenceIssue | None:
    case_dir = latest_case_dir(root, op)
    if case_dir is None:
        return CasegenEvidenceIssue(
            op=op,
            case_version="missing",
            case_dir=root / "TestUtils" / "casegen" / op / "case",
            missing=MISSING_CASEGEN_DIR_EVIDENCE,
        )
    missing = list(
        name for name in REQUIRED_CASEGEN_EVIDENCE if not (case_dir / name).exists()
    )
    missing.extend(
        casegen_schema_issues(
            case_dir,
            op,
            root=root,
            config=config,
            season=season,
        )
    )
    if not missing:
        return None
    return CasegenEvidenceIssue(
        op=op, case_version=case_dir.name, case_dir=case_dir, missing=tuple(missing)
    )


def casegen_schema_issues(
    case_dir: Path,
    op: str,
    *,
    root: Path | None = None,
    config: DaemonConfig | None = None,
    season: str = "",
) -> list[str]:
    issues: list[str] = []
    meta_path = case_dir / "meta.json"
    cases_path = case_dir / "cases.json"
    meta = read_json_object(meta_path)
    cases = read_json_object(cases_path)
    if meta_path.exists():
        if not isinstance(meta, dict):
            issues.append("meta.json:object")
        else:
            if meta.get("op") != op:
                issues.append("meta.json:op")
            if meta.get("case_version") != case_dir.name:
                issues.append("meta.json:case_version")
            if (
                meta.get("case_protocol")
                in {"five_case_v1", "five_case_v2", SIXTEEN_CASE_DUAL_BAND}
                and meta.get("official_case_policy") != "drop_official_case1"
            ):
                issues.append("meta.json:official_case_policy=drop_official_case1")
            buckets = meta.get("buckets")
            if not isinstance(buckets, list) or not buckets:
                issues.append("meta.json:buckets")
            else:
                for bucket in buckets:
                    if not isinstance(bucket, dict):
                        issues.append("meta.json:buckets")
                        break
                    case_file = bucket.get("case_file")
                    if (
                        not isinstance(case_file, str)
                        or not case_file
                        or not (case_dir / case_file).exists()
                    ):
                        issues.append("meta.json:case_file")
                        break
            performance_case_ids = meta_performance_case_ids(meta, buckets)
            if len(performance_case_ids) > 5 and not scalable_weight_contract_valid(
                meta.get("perf_weighted_time_weights"), performance_case_ids
            ):
                issues.append("meta.json:perf_weighted_time_weights:complete")
            if meta.get("case_protocol") == SIXTEEN_CASE_DUAL_BAND:
                issues.extend(
                    sixteen_case_dual_band_issues(
                        case_dir, meta, buckets, performance_case_ids
                    )
                )
                issues.extend(official_failure_diversity_issues(case_dir, meta, buckets))
            issues.extend(five_case_v2_orthogonality_issues(case_dir, meta))
    if cases_path.exists() and (not isinstance(cases, list) or not cases):
        issues.append("cases.json:list-nonempty")
    if root is not None:
        issues.extend(shared_knowledge_casegen_issues(root, case_dir, op, config))
    bundle_season = (
        operator_season(config, op)
        if config is not None
        else str(season or "")
    )
    if root is not None and bundle_season:
        task_case = (
            root / "operators" / bundle_season / "case_910b" / op
        )
        if task_case.is_dir() and meta_path.is_file() and cases_path.is_file():
            contract = validate_case_bundle_contract(
                task_case,
                case_dir,
                op=op,
                case_version=case_dir.name,
                run_validator=case_bundle_validator_due(case_dir),
            )
            if not contract.valid:
                issues.append(f"case-bundle-contract:{contract.detail}")
    return issues


def five_case_v2_orthogonality_issues(
    case_dir: Path,
    meta: dict[str, Any],
) -> list[str]:
    if meta.get("case_protocol") != FIVE_CASE_V2:
        return []
    if str(meta.get("generated_at") or "") < V2_ORTHOGONALITY_ENFORCED_AFTER:
        return []
    buckets = meta.get("buckets")
    if not isinstance(buckets, list):
        return []
    large_orthos: dict[str, dict[str, Any]] = {}
    for bucket in buckets:
        if not isinstance(bucket, dict):
            continue
        bucket_id = str(bucket.get("id") or "")
        if bucket_id not in {"b2", "b3", "b4"}:
            continue
        case_file = bucket.get("case_file")
        if not isinstance(case_file, str) or not case_file:
            continue
        payload = read_json_object(case_dir / case_file)
        model = payload.get("model") if isinstance(payload, dict) else None
        orthogonality = model.get("orthogonality") if isinstance(model, dict) else None
        if isinstance(orthogonality, dict):
            large_orthos[bucket_id] = orthogonality
    if set(large_orthos) != {"b2", "b3", "b4"}:
        return ["meta.json:five_case_v2:large-orthogonality=3"]
    issues: list[str] = []
    ordered = [
        ("case3", large_orthos["b2"]),
        ("case4", large_orthos["b3"]),
        ("case5", large_orthos["b4"]),
    ]
    for left_index in range(len(ordered)):
        for right_index in range(left_index + 1, len(ordered)):
            left_name, left = ordered[left_index]
            right_name, right = ordered[right_index]
            differences = sum(
                str(left.get(key) or "") != str(right.get(key) or "")
                for key in V2_ORTHOGONALITY_DIMENSIONS
            )
            if differences < 2:
                issues.append(
                    "meta.json:five_case_v2:large-orthogonality:"
                    f"{left_name}-{right_name}={differences}"
                )
    return issues


def meta_performance_case_ids(meta: dict[str, Any], buckets: Any) -> list[int]:
    raw = str(meta.get("default_perf_case_range") or "").strip()
    if not raw:
        return list(range(1, len(buckets) + 1)) if isinstance(buckets, list) else []
    try:
        if ".." in raw:
            left, right = raw.split("..", 1)
            start = int(left)
            finish = int(right)
            values = list(range(start, finish + 1))
        else:
            values = [int(item) for item in raw.replace(",", " ").split()]
    except ValueError:
        return []
    if (
        not values
        or any(item <= 0 for item in values)
        or len(values) != len(set(values))
    ):
        return []
    return values


def scalable_weight_contract_valid(raw: Any, case_ids: list[int]) -> bool:
    values: list[Any]
    if isinstance(raw, list):
        if len(raw) != len(case_ids):
            return False
        values = raw
    elif isinstance(raw, dict):
        try:
            values = [raw[f"case{case_id}"] for case_id in case_ids]
        except KeyError:
            return False
    else:
        return False
    try:
        weights = [float(item) for item in values]
    except (TypeError, ValueError):
        return False
    return all(math.isfinite(item) and item >= 0 for item in weights) and any(
        item > 0 for item in weights
    )


def sixteen_case_dual_band_issues(
    case_dir: Path,
    meta: dict[str, Any],
    buckets: Any,
    case_ids: list[int],
) -> list[str]:
    issues: list[str] = []
    if not isinstance(buckets, list) or len(buckets) != 16 or case_ids != list(
        range(1, 17)
    ):
        return ["meta.json:sixteen-case-contract=1..16"]
    expected_tiers = (
        "1k",
        "10k",
        "50k",
        "250k",
        "1M",
        "10M",
        "50M",
        "100M",
    )
    tiers = [str(item.get("tier") or "") for item in buckets if isinstance(item, dict)]
    if tiers != [tier for tier in expected_tiers for _ in range(2)]:
        issues.append("meta.json:sixteen-case-two-per-tier")
    groups = meta.get("perf_score_groups")
    if not isinstance(groups, dict) or set(groups) != {"small", "large"}:
        issues.append("meta.json:perf_score_groups=small,large")
    else:
        expected_group_ids = {
            "small": list(range(1, 9)),
            "large": list(range(9, 17)),
        }
        for name, expected_ids in expected_group_ids.items():
            group = groups.get(name)
            if not isinstance(group, dict):
                issues.append(f"meta.json:perf_score_groups:{name}")
                continue
            if group.get("case_ids") != expected_ids or not scalable_weight_contract_valid(
                group.get("weights"), expected_ids
            ):
                issues.append(f"meta.json:perf_score_groups:{name}:complete")
    policy = meta.get("release_policy")
    if not isinstance(policy, dict) or any(
        policy.get(key) != value
        for key, value in {
            "balanced_improvement_ratio": 0.05,
            "lead_improvement_ratio": 0.08,
            "other_min_improvement_ratio": 0.0,
        }.items()
    ):
        issues.append("meta.json:dual-band-release-policy")
    if meta.get("cross_case_regression_budget") != 0.05:
        issues.append("meta.json:cross-case-regression-budget=0.05")
    for pair_index, tier in enumerate(expected_tiers):
        orthogonality: list[dict[str, Any]] = []
        for bucket in buckets[pair_index * 2 : pair_index * 2 + 2]:
            case_file = str(bucket.get("case_file") or "")
            payload = read_json_object(case_dir / case_file) if case_file else None
            model = payload.get("model") if isinstance(payload, dict) else None
            record = model.get("orthogonality") if isinstance(model, dict) else None
            if isinstance(record, dict):
                orthogonality.append(record)
        if len(orthogonality) != 2:
            issues.append(f"meta.json:sixteen-case-orthogonality:{tier}=2")
            continue
        differences = sum(
            str(orthogonality[0].get(key) or "")
            != str(orthogonality[1].get(key) or "")
            for key in V2_ORTHOGONALITY_DIMENSIONS
        )
        if differences < 2:
            issues.append(
                f"meta.json:sixteen-case-orthogonality:{tier}={differences}"
            )
    return issues


def official_failure_diversity_issues(
    case_dir: Path,
    meta: dict[str, Any],
    buckets: Any,
) -> list[str]:
    if meta.get("case_diversity_contract") != OFFICIAL_FAILURE_DIVERSITY:
        return []
    issues: list[str] = []
    audit = meta.get("diversity_audit")
    if not isinstance(audit, dict):
        return ["meta.json:diversity_audit"]
    required_audit = (
        "official_checkpoint_path",
        "predicted_official_transition",
        "strongest_counter_hypothesis",
    )
    for key in required_audit:
        if not str(audit.get(key) or "").strip():
            issues.append(f"meta.json:diversity_audit:{key}")
    novel_dimensions = audit.get("novel_dimensions")
    if (
        not isinstance(novel_dimensions, list)
        or len({str(item).strip() for item in novel_dimensions if str(item).strip()}) < 2
    ):
        issues.append("meta.json:diversity_audit:novel_dimensions>=2")
    if audit.get("template_profile") != OFFICIAL_TEMPLATE_PROFILE:
        issues.append(
            f"meta.json:diversity_audit:template_profile={OFFICIAL_TEMPLATE_PROFILE}"
        )

    semantic_paths: set[str] = set()
    value_spaces: set[str] = set()
    if not isinstance(buckets, list):
        return issues
    for index, bucket in enumerate(buckets, start=1):
        case_file = str(bucket.get("case_file") or "") if isinstance(bucket, dict) else ""
        payload = read_json_object(case_dir / case_file) if case_file else None
        model = payload.get("model") if isinstance(payload, dict) else None
        if not isinstance(model, dict):
            issues.append(f"case{index}:model")
            continue
        orthogonality = model.get("orthogonality")
        orthogonality = orthogonality if isinstance(orthogonality, dict) else {}
        semantic_path = str(orthogonality.get("semantic_path") or "").strip()
        value_space = str(orthogonality.get("value_space") or "").strip()
        if semantic_path:
            semantic_paths.add(semantic_path)
        else:
            issues.append(f"case{index}:semantic_path")
        if value_space:
            value_spaces.add(value_space)
        else:
            issues.append(f"case{index}:value_space")
        for key in ("official_failure_cluster", "expected_dispatch", "falsifier"):
            if not str(model.get(key) or "").strip():
                issues.append(f"case{index}:{key}")
    if len(semantic_paths) < 4:
        issues.append(f"meta.json:semantic_path-diversity={len(semantic_paths)}<4")
    if len(value_spaces) < 4:
        issues.append(f"meta.json:value_space-diversity={len(value_spaces)}<4")
    return issues


def shared_knowledge_casegen_issues(
    root: Path,
    case_dir: Path,
    op: str,
    config: DaemonConfig | None,
) -> list[str]:
    session = operator_session(config, op) if config is not None else None
    configured_root = (
        str(session.knowledge_root)
        if session is not None and session.knowledge_root
        else f"reference/op_knowledge/{op}"
    )
    knowledge_root = Path(configured_root)
    if not knowledge_root.is_absolute():
        knowledge_root = root / knowledge_root
    # Missing knowledge roots are reported by status/efficiency health. Existing
    # roots opt the operator into the shared-knowledge case contract without
    # breaking isolated legacy fixtures that have no knowledge tree.
    if not knowledge_root.exists():
        return []
    plan_path = case_dir / "CASEGEN_PLAN.md"
    plan_text = (
        plan_path.read_text(encoding="utf-8", errors="replace")
        if plan_path.exists()
        else ""
    )
    normalized_plan = plan_text.replace("\\", "/").lower()
    coverage_path = knowledge_root / "case_coverage.md"
    lessons_path = knowledge_root / "optimization_lessons.md"
    backlog_path = knowledge_root / "hypothesis_backlog.md"
    coverage_text = (
        coverage_path.read_text(encoding="utf-8", errors="replace")
        if coverage_path.exists()
        else ""
    )
    issues: list[str] = []
    expected_root = configured_root.replace("\\", "/").rstrip("/").lower()
    for filename in (
        "case_coverage.md",
        "optimization_lessons.md",
        "hypothesis_backlog.md",
    ):
        direct_reference = f"{expected_root}/{filename}" in normalized_plan
        grouped_reference = any(
            filename in {item.strip() for item in match.group(1).split(",")}
            for match in re.finditer(
                rf"{re.escape(expected_root)}/\{{([^}}\r\n]+)\}}",
                normalized_plan,
            )
        )
        if not direct_reference and not grouped_reference:
            issues.append(f"CASEGEN_PLAN.md:shared-knowledge:{filename}")
    if not re.search(
        r"^\s*(?:[-*]\s*)?shared knowledge decision\s*:\s*\S.+$", plan_text, re.I | re.M
    ):
        issues.append("CASEGEN_PLAN.md:shared-knowledge-decision")
    if not coverage_path.exists():
        issues.append("knowledge:case_coverage.md")
    elif case_dir.name.lower() not in coverage_text.lower():
        issues.append(f"knowledge:case_coverage-current={case_dir.name}")
    if not lessons_path.exists():
        issues.append("knowledge:optimization_lessons.md")
    if not backlog_path.exists():
        issues.append("knowledge:hypothesis_backlog.md")
    return issues


def read_json_object(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def enforce_casegen_evidence(
    root: Path,
    decisions: tuple[GateDecision, ...],
    config: DaemonConfig,
) -> tuple[GateDecision, ...]:
    enforced: list[GateDecision] = []
    for decision in decisions:
        peer_baseline_decision = enforce_peer_benchmark_baseline(root, decision, config)
        if peer_baseline_decision is not None:
            enforced.append(peer_baseline_decision)
            continue
        if (
            decision.action == ActionKind.NOTIFY_TESTER_CASEGEN
            and decision.row.gate_stage == "needs-case-version"
        ):
            latest_issue = latest_casegen_evidence_issue(root, decision.row.op, config)
            if latest_issue is not None and latest_issue.case_version != "missing":
                enforced.append(
                    casegen_evidence_decision(root, decision, latest_issue, config)
                )
                continue
        if not should_enforce_casegen_evidence(decision, config):
            enforced.append(decision)
            continue
        issue = latest_casegen_evidence_issue(root, decision.row.op, config)
        if issue is None:
            enforced.append(decision)
            continue
        enforced.append(casegen_evidence_decision(root, decision, issue, config))
    return tuple(enforced)


def enforce_peer_benchmark_baseline(
    root: Path,
    decision: GateDecision,
    config: DaemonConfig,
) -> GateDecision | None:
    if decision.action not in PRE_SUBMIT_ACTIONS:
        return None
    session = operator_session(config, decision.row.op)
    if session is None or session.workflow_mode != "peer_benchmark":
        return None
    if not session.benchmark_baseline_marker:
        return None
    marker = Path(session.benchmark_baseline_marker)
    if not marker.is_absolute():
        marker = root / marker
    if marker.exists():
        return None
    test_version = extract_row_test_version(decision.row)
    version_paths = (
        root / "TestUtils" / "pending" / decision.row.op / test_version / "VERSION.md",
        root
        / "TestUtils"
        / "submit"
        / decision.row.op
        / test_version
        / "pending_snapshot"
        / "VERSION.md",
    )
    version_text = ""
    for path in version_paths:
        if path.exists():
            version_text = path.read_text(encoding="utf-8", errors="replace")
            break
    if "peer-benchmark-current-baseline" in version_text.lower():
        return None
    synthetic = BoardRow(
        season=decision.row.season,
        op=decision.row.op,
        gate_stage="peer-baseline-required",
        next_owner="solver",
        solver_goal=decision.row.solver_goal,
        tester_goal=decision.row.tester_goal,
        wakeups="PEER_BENCHMARK_CURRENT_BASELINE_REQUIRED",
        next_command=(
            f"if {test_version} is an unsubmitted peer candidate, first run python scripts\\next_workflow.py "
            f"defer-pending {decision.row.op} {test_version} --label {test_version}_peer-deferred "
            "--reason peer-baseline-required; "
            f"create a frozen current-algorithm baseline from {session.benchmark_baseline_source} "
            f"on fixed case {session.benchmark_case_version}; VERSION.md must contain "
            f"peer-benchmark-current-baseline; after RESULT write {session.benchmark_baseline_marker}; "
            "do not advance another peer candidate before the marker exists"
        ),
    )
    return GateDecision(
        row=synthetic,
        action=ActionKind.NOTIFY_SOLVER,
        reason=(
            "peer benchmark lacks a current-algorithm same-case baseline; "
            "block prepare/submit until the solver produces and records it"
        ),
        command="",
        priority=max(decision.priority, 75),
        blocks_operator=decision.blocks_operator,
    )


def should_enforce_casegen_evidence(
    decision: GateDecision, config: DaemonConfig
) -> bool:
    return (
        decision.action == ActionKind.NOTIFY_SOLVER
        and decision.row.gate_stage in POST_CASEGEN_GATES
        or decision.action in PRE_SUBMIT_ACTIONS
    ) and (
        decision.action in PRE_SUBMIT_ACTIONS
        or casegen_role_enabled(config, decision.row.op)
    )


def casegen_evidence_decision(
    root: Path,
    decision: GateDecision,
    issue: CasegenEvidenceIssue,
    config: DaemonConfig,
) -> GateDecision:
    rel_case_dir = issue.relative_case_dir(root)
    missing = ", ".join(issue.missing)
    peer_benchmark = workflow_mode_for(config, decision.row.op) == "peer_benchmark"
    consumed_structural_case = consumed_structural_case_issue(issue)
    evidence_instruction = (
        f"replace consumed invalid Tester-authored case evidence after {issue.case_version} "
        "with a fresh successor case version; the consumed case is immutable and must not be rewritten"
        if consumed_structural_case
        else f"complete Tester-authored casegen evidence for {decision.row.op} {issue.case_version}"
    )
    synthetic = BoardRow(
        season=decision.row.season,
        op=decision.row.op,
        gate_stage="casegen-evidence-incomplete",
        next_owner="solver" if peer_benchmark else "tester",
        solver_goal=decision.row.solver_goal,
        tester_goal=decision.row.tester_goal,
        wakeups=(
            f"PEER_BENCHMARK_FIXED_CASE_INVALID missing={missing}"
            if peer_benchmark
            else f"CASEGEN_EVIDENCE_INCOMPLETE missing={missing}"
        ),
        next_command=(
            f"{evidence_instruction}; "
            f"case_dir={rel_case_dir}; missing={missing}; "
            "do not create pending/submit until fixed-case evidence is valid; "
            + (
                "peer benchmark mode must not trigger Tester or roll the case"
                if peer_benchmark
                else (
                    "CASEGEN_PLAN.md and MODEL_AUDIT.md must explain weak points, history basis, "
                    "current source/tiling linkage, and generated cases; CASEGEN_PLAN.md must cite "
                    "all three operator knowledge files with Shared knowledge decision; "
                    "case_coverage.md must record this case version, and an open hypothesis "
                    "must be selected, refined, or explicitly skipped with reason"
                )
            )
        ),
    )
    return GateDecision(
        row=synthetic,
        action=ActionKind.HOLD if peer_benchmark else ActionKind.NOTIFY_TESTER_CASEGEN,
        reason=casegen_evidence_reason(decision, issue, missing),
        command="",
        priority=0 if peer_benchmark else max(decision.priority, 70),
        blocks_operator=decision.blocks_operator,
    )


def consumed_structural_case_issue(issue: CasegenEvidenceIssue) -> bool:
    if not any(
        item.startswith("meta.json:five_case_v2:large-orthogonality")
        for item in issue.missing
    ):
        return False
    meta = read_json_object(issue.case_dir / "meta.json")
    if not isinstance(meta, dict):
        return False
    try:
        return int(meta.get("usage_count") or 0) > 0
    except (TypeError, ValueError):
        return False


def casegen_evidence_reason(
    decision: GateDecision, issue: CasegenEvidenceIssue, missing: str
) -> str:
    if decision.action in PRE_SUBMIT_ACTIONS:
        return (
            f"latest case version {issue.case_version} is missing Tester casegen evidence ({missing}); "
            "notify the operator Tester before prepare/submit consumes device time"
        )
    return (
        f"latest case version {issue.case_version} is missing Tester casegen evidence ({missing}); "
        "notify the operator Tester before solver pending-candidate creation"
    )


def casegen_evidence_summary(root: Path, op: str) -> dict[str, object]:
    issue = latest_casegen_evidence_issue(root, op)
    case_dir = latest_case_dir(root, op)
    summary: dict[str, object] = {
        "op": op,
        "case_version": case_dir.name if case_dir is not None else "",
        "case_dir": "",
        "ok": issue is None,
        "missing": [],
    }
    if case_dir is not None:
        try:
            summary["case_dir"] = str(case_dir.relative_to(root))
        except ValueError:
            summary["case_dir"] = str(case_dir)
        meta_path = case_dir / "meta.json"
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                meta = {}
            if isinstance(meta, dict):
                summary["usage_count"] = meta.get("usage_count")
                summary["max_usage"] = meta.get("max_usage")
                summary["case_protocol"] = meta.get("case_protocol")
    if issue is not None:
        summary["missing"] = list(issue.missing)
    return summary
