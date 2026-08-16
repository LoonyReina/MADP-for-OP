from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import math
import os
import re
import shlex
import shutil
import sys
from pathlib import Path
from typing import Any

from ascendop_daemon.workflow.profile_rounds_override import discover_profile_round_declarations


class EngineJobBuildError(RuntimeError):
    pass


CONSERVATIVE_PROFILE = "engine-v1-staged-conservative"
SPLIT_PROFILE = "engine-v1-staged-split"
BATCHED_PROFILE = "engine-v1-staged-batched"
CORRECTNESS_BATCHED_PROFILE = "engine-v1-staged-correctness-batched"
PERFORMANCE_FIRST_SPLIT_PROFILE = "engine-v1-staged-performance-first-split"
PERFORMANCE_FIRST_CORRECTNESS_BATCHED_PROFILE = (
    "engine-v1-staged-performance-first-correctness-batched"
)
PERFORMANCE_SESSION_CORRECTNESS_BATCHED_PROFILE = (
    "engine-v1-staged-performance-session-correctness-batched"
)
SCALABLE_PROFILE = "engine-v2-staged-scalable"
FUSED_SCALABLE_PROFILE = "engine-v3-staged-fused"
CASE_CACHE_PREWARM_PROFILE = "engine-v3-case-cache-prewarm"
PROFILER_EVIDENCE_PROFILE = "engine-v4-profiler-evidence"
SCALABLE_PROFILES = {
    SCALABLE_PROFILE,
    FUSED_SCALABLE_PROFILE,
    CASE_CACHE_PREWARM_PROFILE,
    PROFILER_EVIDENCE_PROFILE,
}
ENGINE_PROFILES = {
    CONSERVATIVE_PROFILE,
    SPLIT_PROFILE,
    BATCHED_PROFILE,
    CORRECTNESS_BATCHED_PROFILE,
    PERFORMANCE_FIRST_SPLIT_PROFILE,
    PERFORMANCE_FIRST_CORRECTNESS_BATCHED_PROFILE,
    PERFORMANCE_SESSION_CORRECTNESS_BATCHED_PROFILE,
    SCALABLE_PROFILE,
    FUSED_SCALABLE_PROFILE,
    CASE_CACHE_PREWARM_PROFILE,
    PROFILER_EVIDENCE_PROFILE,
}
DEFAULT_CORRECTNESS_CASE_RANGE = "1..5"
DEFAULT_PERFORMANCE_CASE_RANGE = "1..5"
DEFAULT_CORRECTNESS_REPETITIONS = 5
SCALABLE_CORRECTNESS_REPETITIONS = 1
DEFAULT_PERFORMANCE_TASK_ROWS_PER_CASE = 50
FAST_SINGLE_PERFORMANCE_TASK_ROWS_PER_CASE = 1
MAX_ENGINE_CASE_COUNT = 512
MAX_ENGINE_EXECUTIONS_PER_CASE = 10000
MAX_ENGINE_BUILD_DIR_NAME = 56
MAX_PERFORMANCE_STAGE_TIMEOUT_SECONDS = 1200
PERFORMANCE_STAGE_TIMEOUT_GRACE_SECONDS = 60
REGULAR_PROFILE_PROCESS_TIMEOUT_SECONDS = 90
ENGINE_EXECUTION_DEADLINE_SECONDS = 300
GITPARTNER_PRODUCT_DIR = "GitPartner"


def gitpartner_source_root(root: Path) -> Path:
    active_runtime_value = str(
        os.environ.get("GITPARTNER_RUNTIME_SOURCE") or ""
    ).strip()
    product_value = str(
        os.environ.get("ASCENDOP_GITPARTNER_PRODUCT") or GITPARTNER_PRODUCT_DIR
    )
    product = Path(product_value)
    source_root = (
        product.resolve()
        if product.is_absolute()
        else root.resolve() / product / "src"
    )
    candidates = [
        *([Path(active_runtime_value).resolve()] if active_runtime_value else []),
        source_root,
        Path(__file__).resolve().parents[5] / GITPARTNER_PRODUCT_DIR / "src",
    ]
    source_root = next(
        (
            candidate
            for candidate in candidates
            if (
                candidate
                / "limited_remote_partner"
                / "gateway"
                / "submit_job.py"
            ).is_file()
        ),
        source_root,
    )
    marker = (
        source_root
        / "limited_remote_partner"
        / "gateway"
        / "submit_job.py"
    )
    if not marker.is_file():
        raise EngineJobBuildError(
            f"canonical GitPartner product source is missing: {source_root}"
        )
    return source_root


def build_performance_runtime_budget(
    root: Path,
    *,
    op: str,
    case_version: str,
    test_contract: dict[str, Any],
) -> dict[str, Any]:
    case_count = max(
        1, int(test_contract.get("performance_case_count", 0) or 0)
    )
    expected_rows = max(
        1, int(test_contract.get("expected_performance_task_rows", 0) or 0)
    )
    latency_path = root / "TestUtils" / "tester_daemon" / "full_flow_latency.json"
    durations: list[float] = []
    if latency_path.is_file():
        try:
            latency = json.loads(latency_path.read_text(encoding="utf-8-sig"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            latency = {}
        samples = latency.get("samples", []) if isinstance(latency, dict) else []
        for sample in samples if isinstance(samples, list) else []:
            if not isinstance(sample, dict) or str(sample.get("op") or "") != op:
                continue
            stage_durations = sample.get("stage_durations_seconds", {})
            if not isinstance(stage_durations, dict):
                continue
            try:
                duration = float(stage_durations.get("performance-capture", 0) or 0)
            except (TypeError, ValueError):
                continue
            if 0 < duration <= MAX_PERFORMANCE_STAGE_TIMEOUT_SECONDS:
                durations.append(duration)
    durations = durations[-20:]
    sorted_durations = sorted(durations)
    historical_p95 = (
        sorted_durations[
            min(
                len(sorted_durations) - 1,
                max(0, math.ceil(len(sorted_durations) * 0.95) - 1),
            )
        ]
        if sorted_durations
        else 0.0
    )
    historical_max = max(sorted_durations, default=0.0)
    cold_start_budget = math.ceil(expected_rows * 1.5)
    historical_budget = math.ceil(
        max(historical_p95 * 1.5, historical_max * 1.25)
    )
    unconstrained_budget = historical_budget if durations else cold_start_budget
    total_timeout = REGULAR_PROFILE_PROCESS_TIMEOUT_SECONDS
    # All cases run inside one profiled process. The outer process owns the
    # 90-second wall cap; an inner per-case timeout must not divide that cap
    # and kill a legitimate slow case before the enclosing process can decide.
    timeout_per_case = total_timeout
    effective_capture_timeout = REGULAR_PROFILE_PROCESS_TIMEOUT_SECONDS
    return {
        "policy": "fast-single-hard-process-cap-v1",
        "operator": op,
        "case_version": case_version,
        "case_count": case_count,
        "expected_task_rows": expected_rows,
        "history_source": (
            str(latency_path.relative_to(root)).replace("\\", "/")
            if latency_path.is_file()
            else ""
        ),
        "history_sample_count": len(durations),
        "history_p95_seconds": round(historical_p95, 6),
        "history_max_seconds": round(historical_max, 6),
        "cold_start_budget_seconds": int(cold_start_budget),
        "unconstrained_budget_seconds": int(unconstrained_budget),
        "selected_budget_basis": "hard-cap",
        "process_timeout_cap_seconds": REGULAR_PROFILE_PROCESS_TIMEOUT_SECONDS,
        "timeout_per_case_seconds": int(timeout_per_case),
        "capture_timeout_seconds": int(effective_capture_timeout),
        "stage_timeout_seconds": int(
            effective_capture_timeout + PERFORMANCE_STAGE_TIMEOUT_GRACE_SECONDS
        ),
    }


def apply_stage_timeout_budgets(
    stages: list[dict[str, Any]],
    *,
    performance_budget: dict[str, Any],
    test_contract: dict[str, Any],
    profiler_plan: dict[str, Any],
) -> dict[str, int]:
    correctness_executions = max(
        1, int(test_contract.get("expected_correctness_executions", 0) or 0)
    )
    correctness_timeout = min(7200, max(600, correctness_executions * 240 + 30))
    profiler_process_timeout = int(
        profiler_plan.get("profile_timeout_seconds")
        or REGULAR_PROFILE_PROCESS_TIMEOUT_SECONDS
    )
    profiler_mode = str(
        profiler_plan.get("profiler_mode")
        or profiler_plan.get("collection_mode")
        or "fast-single"
    )
    profiler_process_count = (
        2
        if profiler_mode in {"deep-dual", "batched-primary-roofline"}
        else 1
    )
    profiler_warmup_timeout = 0
    profiler_timeout = min(
        600,
        max(
            120,
            profiler_process_count * profiler_process_timeout
            + profiler_warmup_timeout
            + 30,
        ),
    )
    performance_timeout = int(
        performance_budget.get("stage_timeout_seconds", 0) or 0
    )
    defaults = {
        "prepare-materialize-environment": 600,
        "prepare-build-install-wheel": 3600,
        "operator-build-install": 1800,
        "runtime-wheel-install": 1200,
        "case-cache": 1200,
        "correctness": correctness_timeout,
        "performance-capture": performance_timeout,
        "profile-export": 1800,
        "profile-parse": 600,
        "assemble-result": 600,
        "postprocess-result": 7200,
        "prepare-case-cache-payload": 600,
        "runtime-wheel-case-cache": 1200,
        "assemble-case-cache-prewarm": 600,
        "profiler-evidence": profiler_timeout,
        "assemble-profiler-evidence": 600,
    }
    applied: dict[str, int] = {}
    for stage in stages:
        name = str(stage.get("name") or "")
        timeout_seconds = int(defaults.get(name, 7200) or 7200)
        stage["timeout_seconds"] = timeout_seconds
        applied[name] = timeout_seconds
    return applied


def build_compatibility_job(
    root: Path,
    harness_command: str,
    *,
    remote_root: str,
    submit_root_override: Path | None = None,
    execution_profile: str = CONSERVATIVE_PROFILE,
    job_id_suffix: str = "",
    attempt_index: int = 1,
    workflow_ingest: bool = True,
    queue_preactivation: bool = True,
    measurement_preactivation_overlap: bool = False,
    profile_export_capture_overlap: bool = False,
    device_continuation: bool = True,
    require_case_cache_hit: bool = False,
    profiler_plan: dict[str, Any] | None = None,
) -> tuple[Path, Path]:
    root = root.resolve()
    parsed = parse_submit_command(harness_command)
    if execution_profile not in ENGINE_PROFILES:
        raise EngineJobBuildError(f"unsupported engine execution profile: {execution_profile}")
    if profile_export_capture_overlap and execution_profile != FUSED_SCALABLE_PROFILE:
        raise EngineJobBuildError(
            "profile export/capture overlap requires the fused scalable profile"
        )
    if attempt_index < 1 or attempt_index > MAX_ENGINE_EXECUTIONS_PER_CASE:
        raise EngineJobBuildError(f"invalid engine attempt index: {attempt_index}")
    profiler_evidence = execution_profile == PROFILER_EVIDENCE_PROFILE
    if profiler_evidence and not isinstance(profiler_plan, dict):
        raise EngineJobBuildError("profiler evidence profile requires profiler_plan")
    if not profiler_evidence and profiler_plan is not None:
        raise EngineJobBuildError(
            "profiler_plan is valid only for the profiler evidence profile"
        )
    op = parsed["op"]
    test_version = parsed["test_version"]
    suffix = safe_token(job_id_suffix) if job_id_suffix else ""
    engine_job_id = safe_token(f"{test_version}-{suffix}" if suffix else test_version)
    request_id = f"engine-{engine_job_id}"
    # The resident daemon and an explicit recovery tick may prepare the same
    # logical job concurrently. Keep their local staging trees isolated while
    # preserving engine_job_id as the remote deduplication identity.
    build_instance_id = f"{engine_job_id}-p{os.getpid()}"
    build_root = (
        root
        / "TestUtils"
        / "tester_daemon"
        / "engine_builds"
        / engine_build_dir_name(build_instance_id)
    )
    payload_root = build_root / "payload"
    if build_root.exists():
        shutil.rmtree(build_root)
    payload_root.mkdir(parents=True)

    submit_root = (
        submit_root_override.resolve()
        if submit_root_override is not None
        else root / "TestUtils" / "submit" / op / test_version
    )
    if submit_root != root and root not in submit_root.parents:
        raise EngineJobBuildError(f"engine submit root escapes workspace: {submit_root}")
    sources = {
        "source_snapshot": submit_root / "pending_snapshot" / "source_snapshot",
        "task_case": submit_root / "task_case",
        "attack_case": submit_root / "attack_case",
    }
    for name, source in sources.items():
        if name == "attack_case" and not source.exists():
            continue
        if not source.is_dir():
            raise EngineJobBuildError(f"engine payload source missing: {source}")
        copy_tree_without_symlinks(source, payload_root / name)
    workflow_root = Path(__file__).resolve().parents[1] / "workflow"
    profile_rounds_helper = workflow_root / "profile_rounds_override.py"
    if not profile_rounds_helper.is_file():
        raise EngineJobBuildError(
            f"profile rounds override helper is missing: {profile_rounds_helper}"
        )
    shutil.copy2(profile_rounds_helper, payload_root / "profile_rounds_override.py")
    if parsed["season"] == "CANN-Ladder-910B-CANN90":
        template_runner = (
            gitpartner_source_root(root)
            / "limited_remote_partner"
            / "adapters"
            / "official_template.py"
        )
        if not template_runner.is_file():
            raise EngineJobBuildError(
                f"official CANNJudge template runner is missing: {template_runner}"
            )
        shutil.copy2(template_runner, payload_root / "official_template.py")
        template_assets = (
            root
            / "operators"
            / "cann-ladder"
            / op
            / "official_example"
            / "project"
            / "code"
        )
        if not template_assets.is_dir():
            raise EngineJobBuildError(
                f"downloaded official CANNJudge project is missing: {template_assets}"
            )
        copy_tree_without_symlinks(
            template_assets,
            payload_root / "official_template_assets",
        )

    case_cache_prewarm = execution_profile == CASE_CACHE_PREWARM_PROFILE
    if require_case_cache_hit and (
        execution_profile not in SCALABLE_PROFILES or case_cache_prewarm
    ):
        raise EngineJobBuildError(
            "case-cache require-hit is valid only for normal scalable test jobs"
        )
    contract_profile = (
        FUSED_SCALABLE_PROFILE if profiler_evidence else execution_profile
    )
    test_contract = build_test_contract(
        payload_root, execution_profile=contract_profile
    )
    test_contract_sha256 = canonical_object_sha256(test_contract)
    performance_budget = build_performance_runtime_budget(
        root,
        op=op,
        case_version=parsed["case_version"],
        test_contract=test_contract,
    )
    case_cache_requirement = (
        build_case_cache_requirement(payload_root, op=op, test_contract=test_contract)
        if execution_profile in SCALABLE_PROFILES
        else {}
    )

    if profiler_evidence:
        normalized_plan = normalize_profiler_plan(
            profiler_plan or {},
            op=op,
            test_version=test_version,
            test_contract=test_contract,
        )
        runner_source = workflow_root / "profiler_evidence_runner.py"
        if not runner_source.is_file():
            raise EngineJobBuildError(
                f"profiler evidence runner is missing: {runner_source}"
            )
        shutil.copy2(runner_source, payload_root / "profiler_evidence_runner.py")
        (payload_root / "profiler_plan.json").write_text(
            json.dumps(
                normalized_plan,
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
    else:
        normalized_plan = {}

    stage_shells = staged_test_shells(
        root,
        parsed,
        request_id=request_id,
        remote_root=remote_root,
        execution_profile=(
            FUSED_SCALABLE_PROFILE
            if case_cache_prewarm or profiler_evidence
            else execution_profile
        ),
        test_contract=test_contract,
        require_case_cache_hit=require_case_cache_hit,
        performance_timeout_per_case_seconds=int(
            performance_budget["timeout_per_case_seconds"]
        ),
    )
    if case_cache_prewarm:
        stages = case_cache_prewarm_stage_specs(stage_shells, op=op)
        required_artifacts = [
            "result/SUMMARY.txt",
            "result/PHASE_TIMELINE.jsonl",
            "result/ENGINE_IDENTITY.json",
            "result/WHEEL_CACHE.json",
            "result/CASE_CACHE.json",
            "result/RUNTIME_READINESS.json",
        ]
        optional_artifacts = [
            "result/ENGINE_RESULT_FILES.tsv",
            "result/OFFICIAL_TEMPLATE_SYNC.json",
            "result/case_cache_prepare.log",
        ]
        workflow_ingest = False
    elif profiler_evidence:
        stage_shells["profiler"] = profiler_evidence_shell(normalized_plan)
        stage_shells["finalize"] = extend_profiler_finalize_shell(
            stage_shells["finalize"]
        )
        stages = profiler_evidence_stage_specs(
            stage_shells,
            priority=int(normalized_plan.get("stage_priority", 200) or 200),
        )
        required_artifacts = [
            "result/SUMMARY.txt",
            "result/PHASE_TIMELINE.jsonl",
            "result/ENGINE_IDENTITY.json",
            "result/PROFILER_EVIDENCE.json",
            "result/PROFILER_SUMMARY.md",
            "result/PROFILER_CAPABILITY.json",
            "result/profiler_raw",
        ]
        optional_artifacts = [
            "result/ENGINE_RESULT_FILES.tsv",
            "result/WHEEL_CACHE.json",
            "result/CASE_CACHE.json",
            "result/OPERATOR_CACHE.json",
            "result/RUNTIME_READINESS.json",
            "result/OFFICIAL_TEMPLATE_SYNC.json",
            "result/profiler_warmup.log",
            "result/msprof_op_help.txt",
        ]
    else:
        stages = conservative_stage_specs(stage_shells)
        required_artifacts = [
            "result/SUMMARY.txt",
            "result/PHASE_TIMELINE.jsonl",
            "result/ENGINE_IDENTITY.json",
        ]
        optional_artifacts = [
            "result/PERF_SUMMARY.txt",
            "result/PERF_DEBUG.txt",
            "result/WHEEL_CACHE.json",
            "result/OFFICIAL_TEMPLATE_SYNC.json",
            "result/ENGINE_RESULT_FILES.tsv",
            "result/correctness_batch_runner.log",
            "result/perf.log",
        ]
    required_artifacts.append("result/PERFORMANCE_ROUNDS_OVERRIDE.json")
    if not case_cache_prewarm and execution_profile in {
        SPLIT_PROFILE,
        BATCHED_PROFILE,
        CORRECTNESS_BATCHED_PROFILE,
        PERFORMANCE_FIRST_SPLIT_PROFILE,
        PERFORMANCE_FIRST_CORRECTNESS_BATCHED_PROFILE,
        PERFORMANCE_SESSION_CORRECTNESS_BATCHED_PROFILE,
        SCALABLE_PROFILE,
        FUSED_SCALABLE_PROFILE,
    }:
        if execution_profile in SCALABLE_PROFILES:
            stages = scalable_stage_specs(
                stage_shells,
                fused_case_cache=execution_profile == FUSED_SCALABLE_PROFILE,
                fused_correctness=False,
                fused_postprocess=False,
                profile_export_capture_overlap=profile_export_capture_overlap,
            )
        else:
            stages = (
                performance_first_stage_specs(stage_shells)
                if execution_profile
                in {
                PERFORMANCE_FIRST_SPLIT_PROFILE,
                PERFORMANCE_FIRST_CORRECTNESS_BATCHED_PROFILE,
                PERFORMANCE_SESSION_CORRECTNESS_BATCHED_PROFILE,
                }
                else split_stage_specs(stage_shells)
            )
        required_artifacts.extend(
            [
                "result/CORRECTNESS.json",
                "result/CORRECTNESS_SUMMARY.txt",
                "result/PERF_CAPTURE.json",
                "result/PERF_EXPORT.json",
                "result/PERF_PARSE.json",
                "result/PERF_SUMMARY.txt",
            ]
        )
        if execution_profile in {
            BATCHED_PROFILE,
            CORRECTNESS_BATCHED_PROFILE,
            PERFORMANCE_FIRST_CORRECTNESS_BATCHED_PROFILE,
            PERFORMANCE_SESSION_CORRECTNESS_BATCHED_PROFILE,
            SCALABLE_PROFILE,
            FUSED_SCALABLE_PROFILE,
        }:
            required_artifacts.append("result/CORRECTNESS_BATCH.json")
        if execution_profile in {BATCHED_PROFILE, *SCALABLE_PROFILES}:
            required_artifacts.append("result/PERF_BATCH.json")
        if execution_profile == PERFORMANCE_SESSION_CORRECTNESS_BATCHED_PROFILE:
            required_artifacts.append("result/PERF_SESSION.json")
        if execution_profile in SCALABLE_PROFILES:
            required_artifacts.append("result/CASE_CACHE.json")
        if execution_profile == FUSED_SCALABLE_PROFILE:
            required_artifacts.extend(
                ["result/OPERATOR_CACHE.json", "result/RUNTIME_READINESS.json"]
            )
        optional_artifacts.extend(
            [
                "result/times.tsv",
                "result/perf_case1_capture.log",
                "result/perf_case1_export.log",
            ]
        )
    if not queue_preactivation:
        for stage in stages:
            if stage.get("pre_activation"):
                stage["pre_activation"] = False
    stage_timeout_contract = apply_stage_timeout_budgets(
        stages,
        performance_budget=performance_budget,
        test_contract=test_contract,
        profiler_plan=normalized_plan,
    )
    spec = {
        "protocol_version": "engine-v1",
        "request_id": request_id,
        "engine_job_id": engine_job_id,
        "attempt_id": f"attempt-{attempt_index:03d}",
        "operator": op,
        "test_version": test_version,
        "bundle_hash": tree_digest(payload_root),
        "input_identity": {
            "job_kind": (
                "case-cache-prewarm"
                if case_cache_prewarm
                else "profiler-evidence"
                if profiler_evidence
                else "operator-test"
            ),
            "test_version": test_version,
            "source_sha256": tree_digest(payload_root / "source_snapshot"),
            "case_bundle_sha256": tree_digest(
                payload_root / "attack_case"
                if (payload_root / "attack_case").is_dir()
                else payload_root / "task_case"
            ),
            "golden_bundle_sha256": tree_digest(payload_root / "task_case"),
            "test_contract_sha256": test_contract_sha256,
            "correctness_case_count": test_contract["correctness_case_count"],
            "performance_case_count": test_contract["performance_case_count"],
            "correctness_repetitions": test_contract["correctness_repetitions"],
            "performance_samples_per_case": test_contract[
                "performance_samples_per_case"
            ],
        },
        "execution_profile": execution_profile,
        "scheduler_policy": {
            "queue_preactivation": (
                "enabled" if queue_preactivation else "disabled"
            ),
            "measurement_preactivation_overlap": (
                "enabled" if measurement_preactivation_overlap else "disabled"
            ),
            "profile_export_capture_overlap": (
                "enabled" if profile_export_capture_overlap else "disabled"
            ),
            "device_continuation": (
                "enabled" if device_continuation else "disabled"
            ),
        },
        "workflow_ingest": bool(workflow_ingest),
        "test_contract": test_contract,
        "runtime_budgets": {
            "protocol_version": "engine-runtime-budget-v1",
            "execution_deadline_policy": "activation-start-hard-cap-v2",
            "execution_deadline_seconds": ENGINE_EXECUTION_DEADLINE_SECONDS,
            "performance": performance_budget,
            "stages": stage_timeout_contract,
        },
        "execution_deadline_policy": "activation-start-hard-cap-v2",
        "execution_deadline_seconds": ENGINE_EXECUTION_DEADLINE_SECONDS,
        "stages": stages,
        "required_artifacts": required_artifacts,
        "optional_artifacts": optional_artifacts,
        "workflow": {
            "job_kind": (
                "case-cache-prewarm"
                if case_cache_prewarm
                else "profiler-evidence"
                if profiler_evidence
                else "operator-test"
            ),
            "season": parsed["season"],
            "mode": parsed["mode"],
            "vendor": parsed["vendor"],
            "hardware": parsed["hardware"],
            "case_version": parsed["case_version"],
            "source_command": harness_command,
        },
    }
    if profiler_evidence:
        spec["input_identity"].update(
            {
                "profiler_plan_sha256": canonical_object_sha256(normalized_plan),
                "profiler_request_sha256": normalized_plan["request_sha256"],
                "profiler_blocker_generation": normalized_plan[
                    "blocker_generation"
                ],
                "profiler_target_source_sha256": normalized_plan[
                    "target_source_sha256"
                ],
            }
        )
        spec["workflow"]["profiler"] = normalized_plan
    if execution_profile in SCALABLE_PROFILES:
        spec["input_identity"].update(
            {
                "declared_correctness_repetitions": test_contract[
                    "declared_correctness_repetitions"
                ],
                "declared_fresh_process_repetitions": test_contract[
                    "declared_fresh_process_repetitions"
                ],
                "performance_capture_mode": test_contract[
                    "performance_capture_mode"
                ],
                "case_cache_protocol": test_contract["case_cache_protocol"],
                "correctness_evidence_mode": test_contract[
                    "correctness_evidence_mode"
                ],
                "case_cache_requirement_sha256": case_cache_requirement["sha256"],
                "case_cache_access": (
                    "require-hit" if require_case_cache_hit else "populate"
                ),
            }
        )
        spec["case_cache_requirement"] = case_cache_requirement
    spec_path = build_root / "engine_job.json"
    spec_path.write_text(
        json.dumps(spec, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return spec_path, payload_root


def build_test_contract(
    payload_root: Path,
    *,
    execution_profile: str = CONSERVATIVE_PROFILE,
) -> dict[str, Any]:
    if execution_profile not in ENGINE_PROFILES:
        raise EngineJobBuildError(
            f"unsupported engine execution profile: {execution_profile}"
        )
    meta_path = payload_root / "attack_case" / "meta.json"
    meta: dict[str, Any] = {}
    source = "legacy-default"
    if meta_path.is_file():
        try:
            raw = json.loads(meta_path.read_text(encoding="utf-8-sig"))
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            raise EngineJobBuildError(f"cannot read engine case metadata: {meta_path}: {exc}") from exc
        if not isinstance(raw, dict):
            raise EngineJobBuildError(f"engine case metadata must be an object: {meta_path}")
        meta = raw
        source = "attack_case/meta.json"

    inferred_range = infer_case_range_from_meta(meta)
    correctness_range = str(
        meta["default_correctness_range"]
        if "default_correctness_range" in meta
        else inferred_range or DEFAULT_CORRECTNESS_CASE_RANGE
    )
    performance_range = str(
        meta["default_perf_case_range"]
        if "default_perf_case_range" in meta
        else inferred_range or DEFAULT_PERFORMANCE_CASE_RANGE
    )
    correctness_cases = parse_engine_case_range(
        correctness_range, field="default_correctness_range"
    )
    performance_cases = parse_engine_case_range(
        performance_range, field="default_perf_case_range"
    )
    declared_correctness_repetitions = positive_contract_int(
        meta.get("correctness_repetitions", DEFAULT_CORRECTNESS_REPETITIONS),
        field="correctness_repetitions",
    )
    declared_fresh_process_repetitions = positive_contract_int(
        meta.get("fresh_process_repetitions", SCALABLE_CORRECTNESS_REPETITIONS),
        field="fresh_process_repetitions",
    )
    correctness_repetitions = (
        max(
            SCALABLE_CORRECTNESS_REPETITIONS,
            declared_fresh_process_repetitions,
        )
        if execution_profile in SCALABLE_PROFILES
        else declared_correctness_repetitions
    )
    declared_performance_task_rows_per_case = positive_contract_int(
        meta.get(
            "performance_samples_per_case",
            meta.get(
                "expected_task_rows_per_case",
                DEFAULT_PERFORMANCE_TASK_ROWS_PER_CASE,
            ),
        ),
        field="performance_samples_per_case",
    )
    # The V3 fused profile runs all cases under one msprof process. Its row
    # contract describes device tasks produced inside each case invocation,
    # so keep the Tester-declared sample count. The older scalable profile
    # still uses the explicit fast-single override.
    performance_task_rows_per_case = (
        FAST_SINGLE_PERFORMANCE_TASK_ROWS_PER_CASE
        if execution_profile in SCALABLE_PROFILES
        and execution_profile != FUSED_SCALABLE_PROFILE
        else declared_performance_task_rows_per_case
    )
    profile_round_declarations = discover_profile_round_declarations(
        payload_root / "task_case"
    )
    if len(profile_round_declarations) > 1:
        paths = ", ".join(
            str(item["relative_path"]) for item in profile_round_declarations
        )
        raise EngineJobBuildError(
            f"multiple kProfileRounds declarations are ambiguous: {paths}"
        )
    weight_formula, performance_weights, weight_source = performance_weight_contract(
        meta, performance_cases
    )
    performance_score_groups = performance_score_group_contract(
        meta, performance_cases
    )
    contract = {
        "protocol_version": (
            "engine-test-contract-v2"
            if execution_profile in SCALABLE_PROFILES
            else "engine-test-contract-v1"
        ),
        "source": source,
        "case_protocol": str(meta.get("case_protocol") or "legacy"),
        "correctness_case_range": normalize_case_range(correctness_cases),
        "correctness_cases": correctness_cases,
        "correctness_case_count": len(correctness_cases),
        "correctness_repetitions": correctness_repetitions,
        "expected_correctness_executions": len(correctness_cases)
        * correctness_repetitions,
        "performance_case_range": normalize_case_range(performance_cases),
        "performance_cases": performance_cases,
        "performance_case_count": len(performance_cases),
        "performance_samples_per_case": performance_task_rows_per_case,
        "expected_performance_task_rows": len(performance_cases)
        * performance_task_rows_per_case,
        "performance_weight_formula": weight_formula,
        "performance_weights": performance_weights,
        "performance_weight_count": len(performance_weights),
        "performance_weight_source": weight_source,
        "performance_score_groups": performance_score_groups,
    }
    if execution_profile in SCALABLE_PROFILES:
        contract.update(
            {
                "declared_correctness_repetitions": declared_correctness_repetitions,
                "declared_fresh_process_repetitions": (
                    declared_fresh_process_repetitions
                ),
                "correctness_repetition_policy": (
                    "explicit-fresh-process"
                    if correctness_repetitions > SCALABLE_CORRECTNESS_REPETITIONS
                    else "single-outer-declared-custom-op-inner"
                ),
                "declared_performance_samples_per_case": (
                    declared_performance_task_rows_per_case
                ),
                "effective_performance_samples_per_case": (
                    performance_task_rows_per_case
                ),
                "performance_measurement_mode": (
                    "all-case-primary"
                    if execution_profile == FUSED_SCALABLE_PROFILE
                    else "fast-single"
                ),
                "performance_sample_policy": (
                    "declared-device-task-rows-preserved"
                    if execution_profile == FUSED_SCALABLE_PROFILE
                    else "fast-single-effective-one-declared-preserved"
                ),
                "profile_round_override_protocol": (
                    "ascendop-profile-rounds-override-v1"
                ),
                "profile_round_anchor_count": len(profile_round_declarations),
                "profile_round_original_value": (
                    int(profile_round_declarations[0]["rounds"])
                    if profile_round_declarations
                    else None
                ),
                "profile_round_override_required": bool(
                    profile_round_declarations
                    and int(profile_round_declarations[0]["rounds"])
                    != performance_task_rows_per_case
                ),
                "performance_capture_mode": "single-python-multi-case",
                "case_cache_protocol": "engine-case-cache-v1",
                # Wire V3 is correctness-first. Performance artifacts do not
                # exist when this stage runs, even when both stages share one
                # staged job, so correctness must own its batch evidence.
                "correctness_evidence_mode": "standalone-batched-process",
            }
        )
    return contract


def normalize_profiler_plan(
    raw: dict[str, Any],
    *,
    op: str,
    test_version: str,
    test_contract: dict[str, Any],
) -> dict[str, Any]:
    plan = json.loads(json.dumps(raw))
    required_strings = (
        "case_version",
        "blocker_result_version",
        "blocker_generation",
        "request_sha256",
        "request_state_path",
        "target_version",
        "target_source_sha256",
    )
    if str(plan.get("operator") or "") != op:
        raise EngineJobBuildError(
            f"profiler plan operator mismatch: {plan.get('operator')} != {op}"
        )
    if str(plan.get("target_version") or "") != test_version:
        raise EngineJobBuildError(
            "profiler plan target version mismatch: "
            f"{plan.get('target_version')} != {test_version}"
        )
    for field in required_strings:
        if not str(plan.get(field) or "").strip():
            raise EngineJobBuildError(f"profiler plan is missing {field}")
    try:
        cases = [int(value) for value in plan.get("cases", [])]
        roofline_cases = [int(value) for value in plan.get("roofline_cases", [])]
    except (TypeError, ValueError) as exc:
        raise EngineJobBuildError("profiler plan cases must be integers") from exc
    if not cases or len(cases) != len(set(cases)):
        raise EngineJobBuildError("profiler plan requires unique cases")
    available = {
        int(value)
        for field in ("correctness_cases", "performance_cases")
        for value in test_contract.get(field, [])
    }
    if any(case_id not in available for case_id in cases):
        raise EngineJobBuildError(
            f"profiler plan cases escape test contract: {cases}"
        )
    if any(case_id not in cases for case_id in roofline_cases):
        raise EngineJobBuildError(
            "profiler roofline cases must be a subset of primary cases"
        )
    requested_mode = str(
        plan.get("profiler_mode")
        or plan.get("collection_mode")
        or "fast-single"
    )
    mode_aliases = {
        "fast-single": "fast-single",
        "deep-dual": "deep-dual",
        "batched-primary-only": "fast-single",
        "batched-primary-roofline": "deep-dual",
    }
    profiler_mode = mode_aliases.get(requested_mode)
    if profiler_mode is None:
        raise EngineJobBuildError(
            f"unsupported profiler mode: {requested_mode}"
        )
    if (
        profiler_mode == "deep-dual"
        and roofline_cases != cases
    ):
        raise EngineJobBuildError(
            "deep-dual requires roofline cases to match "
            "all primary cases"
        )
    if profiler_mode == "fast-single" and roofline_cases:
        raise EngineJobBuildError(
            "fast-single does not accept roofline cases"
        )
    priority = int(plan.get("stage_priority", 200) or 200)
    if priority < 1 or priority > 1000:
        raise EngineJobBuildError(
            f"profiler stage priority must be within 1..1000: {priority}"
        )
    requested_timeout_seconds = int(
        plan.get("profile_timeout_seconds")
        or REGULAR_PROFILE_PROCESS_TIMEOUT_SECONDS
    )
    if (
        requested_timeout_seconds < 30
        or requested_timeout_seconds > REGULAR_PROFILE_PROCESS_TIMEOUT_SECONDS
    ):
        raise EngineJobBuildError(
            "msprof process timeout must be within "
            f"30..{REGULAR_PROFILE_PROCESS_TIMEOUT_SECONDS} seconds"
        )
    timeout_seconds = REGULAR_PROFILE_PROCESS_TIMEOUT_SECONDS
    metrics = str(plan.get("primary_metrics") or "").strip()
    if not metrics or not re.fullmatch(r"[A-Za-z0-9_,.-]+", metrics):
        raise EngineJobBuildError(
            f"unsafe profiler primary metrics: {metrics!r}"
        )
    request_state = Path(str(plan["request_state_path"]).replace("\\", "/"))
    if (
        request_state.is_absolute()
        or ".." in request_state.parts
        or request_state.parts[:3]
        != ("TestUtils", "tester_daemon", "profiler_requests")
    ):
        raise EngineJobBuildError(
            f"unsafe profiler request state path: {request_state}"
        )
    plan.update(
        {
            "protocol_version": "ascendop-profiler-plan-v2",
            "cases": cases,
            "roofline_cases": roofline_cases,
            "profiler_mode": profiler_mode,
            "collection_mode": (
                requested_mode
                if requested_mode.startswith("batched-primary-")
                else profiler_mode
            ),
            "stage_priority": priority,
            "requested_profile_timeout_seconds": requested_timeout_seconds,
            "profile_timeout_seconds": timeout_seconds,
            "warmup_runs": 0,
            "primary_metrics": metrics,
        }
    )
    if requested_mode.startswith("batched-primary-"):
        plan["legacy_collection_mode"] = requested_mode
    return plan


def infer_case_range_from_meta(meta: dict[str, Any]) -> str:
    buckets = meta.get("buckets")
    if not isinstance(buckets, list) or not buckets:
        return ""
    if not all(isinstance(item, dict) for item in buckets):
        raise EngineJobBuildError("attack_case/meta.json buckets must be objects")
    return f"1..{len(buckets)}"


def parse_engine_case_range(value: str, *, field: str) -> list[int]:
    raw = str(value or "").strip()
    try:
        if ".." in raw:
            left, right = raw.split("..", 1)
            start = int(left)
            finish = int(right)
            values = list(range(start, finish + 1))
        else:
            values = [int(item) for item in raw.replace(",", " ").split()]
    except ValueError as exc:
        raise EngineJobBuildError(f"invalid {field}: {value!r}") from exc
    if not values or any(item <= 0 for item in values):
        raise EngineJobBuildError(f"invalid {field}: {value!r}")
    if len(values) != len(set(values)):
        raise EngineJobBuildError(f"duplicate case id in {field}: {value!r}")
    if len(values) > MAX_ENGINE_CASE_COUNT:
        raise EngineJobBuildError(
            f"{field} exceeds engine case limit {MAX_ENGINE_CASE_COUNT}: {len(values)}"
        )
    return values


def normalize_case_range(values: list[int]) -> str:
    if values == list(range(values[0], values[-1] + 1)):
        return f"{values[0]}..{values[-1]}" if len(values) > 1 else str(values[0])
    return ",".join(str(item) for item in values)


def positive_contract_int(value: Any, *, field: str) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise EngineJobBuildError(f"invalid {field}: {value!r}") from exc
    if result <= 0 or result > MAX_ENGINE_EXECUTIONS_PER_CASE:
        raise EngineJobBuildError(
            f"{field} must be within 1..{MAX_ENGINE_EXECUTIONS_PER_CASE}: {result}"
        )
    return result


def performance_weight_contract(
    meta: dict[str, Any], case_ids: list[int]
) -> tuple[str, list[float], str]:
    formula = str(meta.get("perf_weighted_time_formula") or "")
    raw = meta.get("perf_weighted_time_weights")
    if isinstance(raw, list) and raw:
        if len(raw) != len(case_ids):
            raise EngineJobBuildError(
                "perf_weighted_time_weights count does not match performance cases: "
                f"cases={len(case_ids)} weights={len(raw)}"
            )
        weights = validate_performance_weights(raw)
        return formula or "configured weights", weights, "explicit-list"
    if isinstance(raw, dict) and raw:
        try:
            values = [raw[f"case{case_id}"] for case_id in case_ids]
        except KeyError as exc:
            raise EngineJobBuildError(
                "perf_weighted_time_weights does not cover every performance case: "
                f"{case_ids}"
            ) from exc
        weights = validate_performance_weights(values)
        return formula or "configured weights", weights, "explicit-map"
    if len(case_ids) > 5:
        raise EngineJobBuildError(
            "performance contracts with more than five cases require explicit "
            "perf_weighted_time_weights"
        )
    if "*20" in formula and "*2" in formula:
        defaults = [20.0, 2.0, 1.0, 1.0, 1.0]
        return (
            formula or "case1*20 + case2*2 + case3 + case4 + case5",
            defaults[: len(case_ids)],
            "legacy-formula-default",
        )
    defaults = [100.0, 10.0, 1.0, 0.02, 0.002]
    return (
        formula or "case1*100 + case2*10 + case3 + case4/50 + case5/500",
        defaults[: len(case_ids)],
        "legacy-formula-default",
    )


def performance_score_group_contract(
    meta: dict[str, Any], case_ids: list[int]
) -> dict[str, dict[str, Any]]:
    raw_groups = meta.get("perf_score_groups")
    if raw_groups in (None, {}):
        return {}
    if not isinstance(raw_groups, dict):
        raise EngineJobBuildError("perf_score_groups must be an object")
    groups: dict[str, dict[str, Any]] = {}
    covered: set[int] = set()
    for raw_name, raw_group in raw_groups.items():
        name = str(raw_name or "").strip()
        if not name or not isinstance(raw_group, dict):
            raise EngineJobBuildError("invalid perf_score_groups entry")
        raw_ids = raw_group.get("case_ids")
        raw_weights = raw_group.get("weights")
        if not isinstance(raw_ids, list) or not isinstance(raw_weights, dict):
            raise EngineJobBuildError(
                f"perf_score_groups {name} requires case_ids and weight map"
            )
        try:
            group_ids = [int(item) for item in raw_ids]
            weights = validate_performance_weights(
                [raw_weights[f"case{case_id}"] for case_id in group_ids]
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise EngineJobBuildError(
                f"perf_score_groups {name} does not cover its case_ids"
            ) from exc
        if not group_ids or len(group_ids) != len(set(group_ids)):
            raise EngineJobBuildError(
                f"perf_score_groups {name} has empty or duplicate case_ids"
            )
        if any(case_id not in case_ids for case_id in group_ids):
            raise EngineJobBuildError(
                f"perf_score_groups {name} references cases outside performance range"
            )
        overlap = covered.intersection(group_ids)
        if overlap:
            raise EngineJobBuildError(
                f"perf_score_groups {name} overlaps cases {sorted(overlap)}"
            )
        covered.update(group_ids)
        groups[name] = {
            "formula": str(raw_group.get("formula") or "configured group weights"),
            "case_ids": group_ids,
            "weights": {
                f"case{case_id}": weight
                for case_id, weight in zip(group_ids, weights)
            },
        }
    if covered != set(case_ids):
        raise EngineJobBuildError(
            "perf_score_groups must cover every performance case exactly once"
        )
    return groups


def validate_performance_weights(values: list[Any]) -> list[float]:
    try:
        weights = [float(item) for item in values]
    except (TypeError, ValueError) as exc:
        raise EngineJobBuildError("invalid perf_weighted_time_weights") from exc
    if any(not math.isfinite(item) or item < 0 for item in weights):
        raise EngineJobBuildError(
            "perf_weighted_time_weights must be finite and non-negative"
        )
    if not any(item > 0 for item in weights):
        raise EngineJobBuildError(
            "perf_weighted_time_weights must contain at least one positive value"
        )
    return weights


def parse_submit_command(command: str) -> dict[str, str]:
    argv = shlex.split(command.replace("\\", "/"), posix=False)
    argv = [strip_quotes(item) for item in argv]
    try:
        index = argv.index("gitpartner-run-submit")
        op = argv[index + 1]
        test_version = argv[index + 2]
    except (ValueError, IndexError) as exc:
        raise EngineJobBuildError("not a gitpartner-run-submit command") from exc
    return {
        "op": op,
        "test_version": test_version,
        "season": option_value(argv, "--season", "S5-910b"),
        "mode": option_value(argv, "--mode", "both"),
        "vendor": option_value(argv, "--vendor", test_version.lower()),
        "hardware": option_value(argv, "--hardware", "910B4"),
        "case_version": option_value(argv, "--case-version", "unknown"),
        "perf_weighted_target": option_value(argv, "--perf-weighted-target", ""),
    }


def legacy_test_shell(
    root: Path,
    parsed: dict[str, str],
    *,
    request_id: str,
    remote_root: str,
) -> str:
    package_root = gitpartner_source_root(root)
    if str(package_root) not in sys.path:
        sys.path.insert(0, str(package_root))
    args = legacy_test_args(parsed, request_id=request_id, remote_root=remote_root)
    job = getattr(_gp_submit_module(), "_build_job")(args)
    command = job.get("client_command", [])
    if not isinstance(command, list) or len(command) != 3:
        raise EngineJobBuildError("legacy GitPartner command shape changed")
    return str(command[2])


def legacy_test_args(
    parsed: dict[str, str],
    *,
    request_id: str,
    remote_root: str,
    test_contract: dict[str, Any] | None = None,
) -> argparse.Namespace:
    contract = test_contract or default_test_contract()
    python_venv = remote_root.rstrip("/") + "/.venv"
    return argparse.Namespace(
        kind="ascendop-test",
        transport="relay",
        request_id=request_id,
        output_subdir=None,
        client_work_dir=remote_root,
        timeout_seconds=300,
        sync_interval_seconds=20,
        sandbox_profile="process",
        stage_payload=False,
        op=parsed["op"],
        release=release_name(parsed["test_version"]),
        test_version=parsed["test_version"],
        case_version=parsed["case_version"],
        season=parsed["season"],
        hardware=parsed["hardware"],
        vendor=gitpartner_vendor(parsed["vendor"]),
        source_snapshot="unused/source_snapshot",
        task_case="unused/task_case",
        attack_case="unused/attack_case",
        case_range=contract["correctness_case_range"],
        correctness_repetitions=contract["correctness_repetitions"],
        run_perf=True,
        perf_case_range=contract["performance_case_range"],
        performance_samples_per_case=contract["performance_samples_per_case"],
        test_contract_sha256=canonical_object_sha256(contract),
        correctness_case_count=contract.get("correctness_case_count")
        or len(
            parse_engine_case_range(
                contract["correctness_case_range"], field="correctness_case_range"
            )
        ),
        performance_case_count=contract.get("performance_case_count")
        or len(
            parse_engine_case_range(
                contract["performance_case_range"], field="performance_case_range"
            )
        ),
        perf_time_base="9999999999999",
        perf_weighted_target=parsed["perf_weighted_target"] or None,
        perf_storage_limit="200MB",
        build_only=False,
        install_build_python_deps=True,
        install_runtime_python_deps=True,
        python_venv=python_venv,
    )


def staged_test_shells(
    root: Path,
    parsed: dict[str, str],
    *,
    request_id: str,
    remote_root: str,
    execution_profile: str = CONSERVATIVE_PROFILE,
    test_contract: dict[str, Any] | None = None,
    require_case_cache_hit: bool = False,
    performance_timeout_per_case_seconds: int = 330,
) -> dict[str, str]:
    if execution_profile not in ENGINE_PROFILES:
        raise EngineJobBuildError(f"unsupported engine execution profile: {execution_profile}")
    package_root = gitpartner_source_root(root)
    if str(package_root) not in sys.path:
        sys.path.insert(0, str(package_root))
    contract = test_contract or default_test_contract(
        execution_profile=execution_profile
    )
    args = legacy_test_args(
        parsed,
        request_id=request_id,
        remote_root=remote_root,
        test_contract=contract,
    )
    args.performance_timeout_per_case_seconds = max(
        1, int(performance_timeout_per_case_seconds)
    )
    submit = _gp_submit_module()
    fragments = list(getattr(submit, "_b_local_smoke_fragments")(args))
    _source_cann_env_snippet = getattr(submit, "_source_cann_env_snippet")
    try:
        device_index = fragments.index("PASS=0; FAIL=0")
        finish_index = fragments.index("phase_mark remote_done")
    except ValueError as exc:
        raise EngineJobBuildError("legacy GitPartner phase boundary changed") from exc

    prepare = [item for item in fragments[:device_index] if not item.startswith("trap '")]
    prepare = [rewrite_engine_path_fragment(item) for item in prepare]
    prepare = [
        item.replace(
            "python3 -m limited_remote_partner.adapters.official_template",
            'python3 "$ASCENDOP_ENGINE_PAYLOAD_ROOT/official_template.py"',
        )
        for item in prepare
    ]
    cache_site = execution_profile == FUSED_SCALABLE_PROFILE
    prepare = rewrite_wheel_cache_fragments(
        prepare,
        parsed["hardware"],
        materialize_site=cache_site,
        performance_samples_per_case=int(
            contract["performance_samples_per_case"]
        ),
        expected_profile_round_anchor_count=int(
            contract.get("profile_round_anchor_count", 0) or 0
        ),
    )
    prepare = [
        rewrite_wheel_install_fragment(item, cached_site=cache_site)
        for item in prepare
    ]
    if execution_profile == FUSED_SCALABLE_PROFILE:
        prepare = rewrite_operator_cache_fragments(prepare, parsed["hardware"])
    try:
        environment_end = prepare.index("phase_mark environment_setup_end")
        install_end = prepare.index("phase_mark operator_install_end")
    except ValueError as exc:
        raise EngineJobBuildError("legacy host preparation boundary changed") from exc
    if install_end <= environment_end:
        raise EngineJobBuildError("legacy host preparation ordering is invalid")

    prepare_common = list(prepare[: environment_end + 1])
    prepare_common.extend(
        [
            (
                '{ printf \'export BUILD_PYTHON_BIN=%q\\n\' "$BUILD_PYTHON_BIN"; '
                'printf \'export PYTHONPATH=%q\\n\' "${PYTHONPATH:-}"; } '
                '> "$RUN_DIR/engine_build.env"'
            ),
            "phase_mark engine_materialize_complete",
        ]
    )
    build_install = engine_stage_prelude(
        "operator_build_install",
        runtime=False,
        cann_snippet=_source_cann_env_snippet(),
    )
    build_install.extend(
        [
            'OPERATOR_BUILD_STATE="$RUN_DIR/engine_operator_build.state"',
            'rm -f "$OPERATOR_BUILD_STATE"',
            (
                "operator_build_state_finish() { rc=$?; trap - EXIT; "
                'tmp="$OPERATOR_BUILD_STATE.$$"; printf \'%s\\n\' "$rc" > "$tmp"; '
                'mv -f "$tmp" "$OPERATOR_BUILD_STATE"; exit "$rc"; }; '
                "trap operator_build_state_finish EXIT"
            ),
            'test -f "$RUN_DIR/engine_build.env"',
            '. "$RUN_DIR/engine_build.env"',
            *prepare[environment_end + 1 : install_end + 1],
            "phase_mark engine_operator_build_install_complete",
        ]
    )
    runtime_tail = list(prepare[install_end + 1 :])
    operator_env_check = 'test -f "$RUN_DIR/engine_operator.env"'
    try:
        operator_env_index = runtime_tail.index(operator_env_check)
    except ValueError as exc:
        raise EngineJobBuildError(
            "legacy runtime/operator handoff boundary changed"
        ) from exc
    runtime_tail[operator_env_index:operator_env_index] = [
        'OPERATOR_BUILD_STATE="$RUN_DIR/engine_operator_build.state"',
        "OPERATOR_BUILD_WAIT_POLLS=0",
        (
            'until [ -f "$RUN_DIR/engine_operator.env" ]; do '
            'if [ -f "$OPERATOR_BUILD_STATE" ]; then '
            'OPERATOR_BUILD_RC=$(cat "$OPERATOR_BUILD_STATE" 2>/dev/null || true); '
            'if [ -n "$OPERATOR_BUILD_RC" ] && [ "$OPERATOR_BUILD_RC" -ne 0 ]; then '
            'echo OPERATOR_BUILD_DEPENDENCY_FAILED:$OPERATOR_BUILD_RC; exit 45; fi; fi; '
            "OPERATOR_BUILD_WAIT_POLLS=$((OPERATOR_BUILD_WAIT_POLLS+1)); "
            'if [ "$OPERATOR_BUILD_WAIT_POLLS" -ge 9000 ]; then '
            "echo OPERATOR_BUILD_HANDOFF_TIMEOUT; exit 45; fi; "
            "sleep 0.1; done"
        ),
        'echo OPERATOR_BUILD_HANDOFF_READY:polls=$OPERATOR_BUILD_WAIT_POLLS',
    ]
    runtime_wheel = engine_stage_prelude(
        "runtime_wheel_install",
        runtime=False,
        cann_snippet=_source_cann_env_snippet(),
    )
    runtime_wheel.extend(
        [
            'PAYLOAD="$ASCENDOP_ENGINE_PAYLOAD_ROOT"',
            *runtime_tail,
            'export PYTHONPATH="$RUN_DIR/py_site:${PYTHONPATH:-}"',
            (
                'TORCH_LIB_DIR=$("$PYTHON_BIN" -c "import pathlib,sysconfig; '
                "print(pathlib.Path(sysconfig.get_paths()['purelib']) / "
                '\'torch\' / \'lib\')")'
            ),
            (
                'TORCH_NPU_LIB_DIR=$("$PYTHON_BIN" -c "import pathlib,sysconfig; '
                "print(pathlib.Path(sysconfig.get_paths()['purelib']) / "
                '\'torch_npu\' / \'lib\')")'
            ),
            (
                'test -f "$TORCH_LIB_DIR/libc10.so" || '
                '{ echo PYTHON_TORCH_LIB_MISSING:$TORCH_LIB_DIR; exit 43; }'
            ),
            (
                'test -f "$TORCH_NPU_LIB_DIR/libtorch_npu.so" || '
                '{ echo PYTHON_TORCH_NPU_LIB_MISSING:$TORCH_NPU_LIB_DIR; exit 43; }'
            ),
            (
                '{ printf \'export PYTHON_BIN=%q\\n\' "$PYTHON_BIN"; '
                'printf \'export TORCH_LIB_DIR=%q\\n\' "$TORCH_LIB_DIR"; '
                'printf \'export TORCH_NPU_LIB_DIR=%q\\n\' "$TORCH_NPU_LIB_DIR"; '
                'printf \'export LD_LIBRARY_PATH=%q\\n\' '
                '"$TORCH_LIB_DIR:$TORCH_NPU_LIB_DIR:${LD_LIBRARY_PATH:-}"; } '
                '> "$RUN_DIR/engine_runtime.env"'
            ),
            "phase_mark engine_runtime_wheel_install_complete",
        ]
    )
    prepare.extend(
        [
            'export PYTHONPATH="$RUN_DIR/py_site:${PYTHONPATH:-}"',
            (
                'TORCH_LIB_DIR=$("$PYTHON_BIN" -c "import pathlib,sysconfig; '
                "print(pathlib.Path(sysconfig.get_paths()['purelib']) / "
                '\'torch\' / \'lib\')")'
            ),
            (
                'TORCH_NPU_LIB_DIR=$("$PYTHON_BIN" -c "import pathlib,sysconfig; '
                "print(pathlib.Path(sysconfig.get_paths()['purelib']) / "
                '\'torch_npu\' / \'lib\')")'
            ),
            (
                'test -f "$TORCH_LIB_DIR/libc10.so" || '
                '{ echo PYTHON_TORCH_LIB_MISSING:$TORCH_LIB_DIR; exit 43; }'
            ),
            (
                'test -f "$TORCH_NPU_LIB_DIR/libtorch_npu.so" || '
                '{ echo PYTHON_TORCH_NPU_LIB_MISSING:$TORCH_NPU_LIB_DIR; exit 43; }'
            ),
            (
                '{ printf \'export PYTHON_BIN=%q\\n\' "$PYTHON_BIN"; '
                'printf \'export TORCH_LIB_DIR=%q\\n\' "$TORCH_LIB_DIR"; '
                'printf \'export TORCH_NPU_LIB_DIR=%q\\n\' "$TORCH_NPU_LIB_DIR"; '
                'printf \'export LD_LIBRARY_PATH=%q\\n\' '
                '"$TORCH_LIB_DIR:$TORCH_NPU_LIB_DIR:${LD_LIBRARY_PATH:-}"; } '
                '> "$RUN_DIR/engine_runtime.env"'
            ),
            "phase_mark engine_prepare_complete",
        ]
    )

    device = engine_stage_prelude("device", cann_snippet=_source_cann_env_snippet())
    device.extend(fragments[device_index:finish_index])
    device.append("phase_mark engine_device_complete")

    finalize = engine_stage_prelude("finalize", runtime=False)
    finalize.extend(
        [
            'test -f "$RUN_DIR/SUMMARY.txt"',
            'test -f "$RUN_DIR/PHASE_TIMELINE.jsonl"',
            "phase_mark remote_done",
            'rm -rf "$ASCENDOP_ENGINE_JOB_ROOT/result"',
            'mkdir -p "$ASCENDOP_ENGINE_JOB_ROOT/result"',
            (
                "for ARTIFACT in SUMMARY.txt PHASE_TIMELINE.jsonl ENGINE_IDENTITY.json "
                "CORRECTNESS.json CORRECTNESS_SUMMARY.txt CORRECTNESS_BATCH.json "
                "correctness_batch_runner.log PERF_CAPTURE.json PERF_EXPORT.json "
                "PERF_PARSE.json PERF_SUMMARY.txt PERF_BATCH.json PERF_SESSION.json "
                "PERF_DEBUG.txt PERFORMANCE_ROUNDS_OVERRIDE.json "
                "WHEEL_CACHE.json CASE_CACHE.json "
                "OPERATOR_CACHE.json RUNTIME_READINESS.json "
                "OFFICIAL_TEMPLATE_SYNC.json INSTALL_LAYOUT.txt "
                "case_cache_prepare.log times.tsv perf.log perf_batch_capture.log "
                "perf_case1_capture.log perf_case1_export.log; do "
                'if [ -e "$RUN_DIR/$ARTIFACT" ]; then cp -a "$RUN_DIR/$ARTIFACT" '
                '"$ASCENDOP_ENGINE_JOB_ROOT/result/$ARTIFACT"; fi; done'
            ),
            (
                'find "$ASCENDOP_ENGINE_JOB_ROOT/result" -maxdepth 1 -type f '
                '-printf "%f\\t%s\\n" | sort '
                '> "$ASCENDOP_ENGINE_JOB_ROOT/result/ENGINE_RESULT_FILES.tsv"'
            ),
            f"echo GITPARTNER_{parsed['op'].upper()}_B_LOCAL_SMOKE_DONE",
        ]
    )
    result = {
        "prepare": "; ".join(prepare),
        "prepare_common": "; ".join(prepare_common),
        "case_cache_prewarm_materialize": case_cache_prewarm_materialize_shell(),
        "build_install": "; ".join(build_install),
        "runtime_wheel": "; ".join(runtime_wheel),
        "device": "; ".join(device),
        "finalize": "; ".join(finalize),
    }
    if execution_profile in {
        SPLIT_PROFILE,
        BATCHED_PROFILE,
        CORRECTNESS_BATCHED_PROFILE,
        PERFORMANCE_FIRST_SPLIT_PROFILE,
        PERFORMANCE_FIRST_CORRECTNESS_BATCHED_PROFILE,
        PERFORMANCE_SESSION_CORRECTNESS_BATCHED_PROFILE,
        SCALABLE_PROFILE,
        FUSED_SCALABLE_PROFILE,
    }:
        try:
            perf_index = next(
                index
                for index, fragment in enumerate(fragments)
                if fragment.startswith("PERF_LABEL=")
            )
        except StopIteration as exc:
            raise EngineJobBuildError("legacy performance boundary changed") from exc
        correctness = engine_stage_prelude(
            "correctness", cann_snippet=_source_cann_env_snippet()
        )
        correctness_batch_flag = (
            " --batch-process"
            if execution_profile
            in {
                BATCHED_PROFILE,
                CORRECTNESS_BATCHED_PROFILE,
                PERFORMANCE_FIRST_CORRECTNESS_BATCHED_PROFILE,
                PERFORMANCE_SESSION_CORRECTNESS_BATCHED_PROFILE,
                SCALABLE_PROFILE,
                FUSED_SCALABLE_PROFILE,
            }
            else ""
        )
        correctness.extend(
            [
                *(
                    [
                        'test -f "$RUN_DIR/case_cache.env"',
                        '. "$RUN_DIR/case_cache.env"',
                    ]
                    if execution_profile in SCALABLE_PROFILES
                    else []
                ),
                "phase_mark correctness_start",
                (
                    '"$PYTHON_BIN" -m limited_remote_partner.engine.stages.correctness_pipeline '
                    ' --task-case "$RUN_DIR/task_case" --run-dir "$RUN_DIR"'
                    f" --case-range {shlex.quote(args.case_range)}"
                    f' --python-bin "$PYTHON_BIN" --repetitions {args.correctness_repetitions}'
                    + (
                        " --from-performance-batch"
                        if contract.get("correctness_evidence_mode")
                        == "profiled-performance-batch"
                        and args.correctness_repetitions == 1
                        else correctness_batch_flag
                    )
                ),
                'cat "$RUN_DIR/CORRECTNESS_SUMMARY.txt" | tee -a "$RUN_DIR/SUMMARY.txt"',
                "phase_mark correctness_end",
                "phase_mark engine_correctness_complete",
            ]
        )
        perf_env = performance_environment_fragments(args, request_id=request_id)
        capture = engine_stage_prelude(
            "performance_capture", cann_snippet=_source_cann_env_snippet()
        )
        capture.extend(perf_env)
        capture_batch_flag = (
            " --batch-process"
            if execution_profile in {BATCHED_PROFILE, *SCALABLE_PROFILES}
            else ""
        )
        capture_session_flag = (
            " --session-isolated-process"
            if execution_profile == PERFORMANCE_SESSION_CORRECTNESS_BATCHED_PROFILE
            else ""
        )
        capture.extend(
            [
                *(
                    [
                        'test -f "$RUN_DIR/case_cache.env"',
                        '. "$RUN_DIR/case_cache.env"',
                    ]
                    if execution_profile in SCALABLE_PROFILES
                    else []
                ),
                "phase_mark performance_start",
                "phase_mark performance_capture_start",
                (
                    '"$PYTHON_BIN" -m limited_remote_partner.engine.stages.perf_pipeline capture '
                    '--task-case "$RUN_DIR/task_case" --run-dir "$RUN_DIR" '
                    '--label "$PERF_LABEL" --case-range "$PERF_CASE_RANGE" '
                    '--python-bin "$PYTHON_BIN" --storage-limit "$MSPROF_STORAGE_LIMIT"'
                    ' --expected-task-rows-per-case "$PERF_TASK_ROWS_PER_CASE"'
                    f" --timeout-seconds {args.performance_timeout_per_case_seconds}"
                    f"{capture_batch_flag}{capture_session_flag}"
                ),
                "phase_mark performance_capture_end",
                "phase_mark engine_performance_capture_complete",
            ]
        )
        export = engine_stage_prelude(
            "profile_export", cann_snippet=_source_cann_env_snippet()
        )
        export.extend(
            [
                "phase_mark profile_export_start",
                (
                    '"$PYTHON_BIN" -m limited_remote_partner.engine.stages.perf_pipeline export '
                    '--run-dir "$RUN_DIR"'
                ),
                "phase_mark profile_export_end",
                "phase_mark engine_profile_export_complete",
            ]
        )
        parse = engine_stage_prelude("profile_parse")
        parse.extend(perf_env)
        parse.extend(
            [
                "phase_mark profile_parse_start",
                (
                    '"$PYTHON_BIN" -m limited_remote_partner.engine.stages.perf_pipeline parse '
                    '--run-dir "$RUN_DIR" '
                    '--attack-meta "${ASCENDOP_ATTACK_META:-}" '
                    '--baseline "$PERF_TIME_BASE" '
                    '--weighted-target "${PERF_WEIGHTED_TARGET:-}"'
                ),
                'cat "$RUN_DIR/PERF_SUMMARY.txt"',
                (
                    'find "$RUN_DIR/profiles_raw" -maxdepth 8 -type f '
                    "\\( -name '*.csv' -o -name '*.json' -o -name '*.log' \\) "
                    '2>/dev/null | sed "s#^$RUN_DIR/##" | sort | head -200 '
                    '> "$RUN_DIR/PERF_DEBUG.txt" || true'
                ),
                f'echo "{parsed["op"]} B-local perf {args.perf_case_range}: PASS" | tee -a "$RUN_DIR/SUMMARY.txt"',
                "phase_mark profile_parse_end",
                "phase_mark performance_end",
                "phase_mark engine_profile_parse_complete",
            ]
        )
        result.update(
            {
                "correctness": "; ".join(correctness),
                "capture": "; ".join(capture),
                "export": "; ".join(export),
                "parse": "; ".join(parse),
            }
        )
        if execution_profile == FUSED_SCALABLE_PROFILE:
            result["postprocess"] = "; ".join(
                (
                    result["correctness"],
                    result["export"],
                    result["parse"],
                    result["finalize"],
                )
            )
        if execution_profile in SCALABLE_PROFILES:
            cached_cases = sorted(
                set(contract["correctness_cases"])
                | set(contract["performance_cases"])
            )
            cache_range = normalize_case_range(cached_cases)
            cache_access_flag = " --require-hit" if require_case_cache_hit else ""
            case_cache = engine_stage_prelude(
                "case_cache", cann_snippet=_source_cann_env_snippet()
            )
            case_cache.extend(
                [
                    "phase_mark case_cache_start",
                    (
                        '"$PYTHON_BIN" -m limited_remote_partner.resources.case_cache prepare '
                        '--task-case "$RUN_DIR/task_case" '
                        '--cache-root "$ASCENDOP_ENGINE_CACHE_ROOT/cases" '
                        f"--op {shlex.quote(parsed['op'])} "
                        f"--case-range {shlex.quote(cache_range)} "
                        '--output "$RUN_DIR/CASE_CACHE.json" '
                        f'--env-output "$RUN_DIR/case_cache.env"{cache_access_flag} '
                        '> "$RUN_DIR/case_cache_prepare.log" 2>&1 || '
                        '{ CACHE_RC=$?; echo CASE_CACHE_FAILED; '
                        'tail -80 "$RUN_DIR/case_cache_prepare.log"; '
                        '[ "$CACHE_RC" -eq 3 ] && exit 49; exit 48; }'
                    ),
                    "phase_mark case_cache_end",
                    "phase_mark engine_case_cache_complete",
                ]
            )
            if execution_profile == FUSED_SCALABLE_PROFILE:
                runtime_completion = "phase_mark engine_runtime_wheel_install_complete"
                if runtime_wheel[-1] != runtime_completion:
                    raise EngineJobBuildError(
                        "runtime-wheel completion boundary changed"
                    )
                try:
                    case_body_start = case_cache.index("phase_mark case_cache_start")
                except ValueError as exc:
                    raise EngineJobBuildError(
                        "case-cache body boundary changed"
                    ) from exc
                runtime_wheel[-1:-1] = case_cache[case_body_start:]
                result["runtime_wheel"] = "; ".join(runtime_wheel)
                result["case_cache"] = ""
            else:
                result["case_cache"] = "; ".join(case_cache)
    return result


def performance_environment_fragments(
    args: argparse.Namespace,
    *,
    request_id: str,
) -> list[str]:
    fragments = [
        f"PERF_LABEL={shlex.quote(request_id.replace(chr(39), '_'))}",
        f"export PERF_CASE_RANGE={shlex.quote(args.perf_case_range or args.case_range)}",
        f"export PERF_TASK_ROWS_PER_CASE={int(args.performance_samples_per_case)}",
        f"export PERF_TIME_BASE={shlex.quote(args.perf_time_base)}",
        f"export MSPROF_STORAGE_LIMIT={shlex.quote(args.perf_storage_limit)}",
        (
            "export ASCENDOP_PROFILE_PROCESS_TIMEOUT_SECONDS="
            f"{REGULAR_PROFILE_PROCESS_TIMEOUT_SECONDS}"
        ),
    ]
    if args.perf_weighted_target:
        fragments.append(
            f"export PERF_WEIGHTED_TARGET={shlex.quote(args.perf_weighted_target)}"
        )
    return fragments


def default_test_contract(
    *, execution_profile: str = CONSERVATIVE_PROFILE
) -> dict[str, Any]:
    if execution_profile not in ENGINE_PROFILES:
        raise EngineJobBuildError(
            f"unsupported engine execution profile: {execution_profile}"
        )
    correctness_cases = parse_engine_case_range(
        DEFAULT_CORRECTNESS_CASE_RANGE, field="default_correctness_range"
    )
    performance_cases = parse_engine_case_range(
        DEFAULT_PERFORMANCE_CASE_RANGE, field="default_perf_case_range"
    )
    weight_formula, performance_weights, weight_source = performance_weight_contract(
        {}, performance_cases
    )
    correctness_repetitions = (
        SCALABLE_CORRECTNESS_REPETITIONS
        if execution_profile in SCALABLE_PROFILES
        else DEFAULT_CORRECTNESS_REPETITIONS
    )
    contract = {
        "protocol_version": (
            "engine-test-contract-v2"
            if execution_profile in SCALABLE_PROFILES
            else "engine-test-contract-v1"
        ),
        "source": "legacy-default",
        "case_protocol": "legacy",
        "correctness_case_range": normalize_case_range(correctness_cases),
        "correctness_cases": correctness_cases,
        "correctness_case_count": len(correctness_cases),
        "correctness_repetitions": correctness_repetitions,
        "expected_correctness_executions": len(correctness_cases)
        * correctness_repetitions,
        "performance_case_range": normalize_case_range(performance_cases),
        "performance_cases": performance_cases,
        "performance_case_count": len(performance_cases),
        "performance_samples_per_case": DEFAULT_PERFORMANCE_TASK_ROWS_PER_CASE,
        "expected_performance_task_rows": len(performance_cases)
        * DEFAULT_PERFORMANCE_TASK_ROWS_PER_CASE,
        "performance_weight_formula": weight_formula,
        "performance_weights": performance_weights,
        "performance_weight_count": len(performance_weights),
        "performance_weight_source": weight_source,
    }
    if execution_profile in SCALABLE_PROFILES:
        contract.update(
            {
                "declared_correctness_repetitions": DEFAULT_CORRECTNESS_REPETITIONS,
                "declared_fresh_process_repetitions": (
                    SCALABLE_CORRECTNESS_REPETITIONS
                ),
                "correctness_repetition_policy": (
                    "single-outer-declared-custom-op-inner"
                ),
                "declared_performance_samples_per_case": (
                    DEFAULT_PERFORMANCE_TASK_ROWS_PER_CASE
                ),
                "performance_sample_policy": "declared-exact",
                "profile_round_override_protocol": (
                    "ascendop-profile-rounds-override-v1"
                ),
                "profile_round_anchor_count": 0,
                "profile_round_original_value": None,
                "profile_round_override_required": False,
                "performance_capture_mode": "single-python-multi-case",
                "case_cache_protocol": "engine-case-cache-v1",
                "correctness_evidence_mode": "standalone-batched-process",
            }
        )
    return contract


def canonical_object_sha256(value: object) -> str:
    payload = json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def build_case_cache_requirement(
    payload_root: Path,
    *,
    op: str,
    test_contract: dict[str, Any],
) -> dict[str, Any]:
    test_op = payload_root / "task_case" / "test_op.py"
    if not test_op.is_file():
        raise EngineJobBuildError(f"case-cache test_op.py is missing: {test_op}")
    generator_source = (
        gitpartner_source_root(payload_root)
        / "limited_remote_partner"
        / "resources"
        / "case_cache.py"
    )
    if not generator_source.is_file():
        raise EngineJobBuildError(
            f"case-cache generator source is missing: {generator_source}"
        )
    case_ids = sorted(
        {
            int(case_id)
            for field in ("correctness_cases", "performance_cases")
            for case_id in test_contract.get(field, [])
        }
    )
    if not case_ids:
        raise EngineJobBuildError("case-cache requirement has no cases")
    identity = {
        "protocol_version": "engine-case-cache-requirement-v2",
        "prewarm_execution_protocol": "source-independent-host-only-v1",
        "operator": op,
        "case_ids": case_ids,
        "test_op_sha256": canonical_file_sha256(test_op),
        "generator_source_sha256": canonical_file_sha256(generator_source),
    }
    return {**identity, "sha256": canonical_object_sha256(identity)}


def build_submit_case_cache_requirement(
    root: Path,
    submit_root: Path,
    *,
    op: str,
) -> dict[str, Any]:
    root = root.resolve()
    submit_root = submit_root.resolve()
    if submit_root != root and root not in submit_root.parents:
        raise EngineJobBuildError(f"engine submit root escapes workspace: {submit_root}")
    contract = build_test_contract(
        submit_root,
        execution_profile=FUSED_SCALABLE_PROFILE,
    )
    return build_case_cache_requirement(
        submit_root,
        op=op,
        test_contract=contract,
    )


def canonical_file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    update_canonical_file_digest(digest, path)
    return digest.hexdigest()


def conservative_stage_specs(shells: dict[str, str]) -> list[dict[str, Any]]:
    return [
        {
            "name": "prepare-build-install-wheel",
            "resource": "host",
            "pre_activation": True,
            "max_attempts": 2,
            "command": ["bash", "-lc", shells["prepare"]],
        },
        {
            "name": "correctness-performance",
            "resource": "device",
            "locks": ["npu", "performance-measurement"],
            "command": ["bash", "-lc", shells["device"]],
        },
        {
            "name": "assemble-result",
            "resource": "host",
            "max_attempts": 2,
            "command": ["bash", "-lc", shells["finalize"]],
        },
    ]


def profiler_evidence_shell(plan: dict[str, Any]) -> str:
    commands = engine_stage_prelude(
        "profiler_evidence",
        cann_snippet=source_cann_environment_snippet(),
    )
    commands.extend(
        [
            'test -f "$ASCENDOP_ENGINE_PAYLOAD_ROOT/profiler_plan.json"',
            'test -f "$ASCENDOP_ENGINE_PAYLOAD_ROOT/profiler_evidence_runner.py"',
            "phase_mark profiler_evidence_start",
            (
                '"$PYTHON_BIN" '
                '"$ASCENDOP_ENGINE_PAYLOAD_ROOT/profiler_evidence_runner.py" '
                '--plan "$ASCENDOP_ENGINE_PAYLOAD_ROOT/profiler_plan.json" '
                '--run-dir "$RUN_DIR" --python-bin "$PYTHON_BIN"'
            ),
            'test -f "$RUN_DIR/PROFILER_EVIDENCE.json"',
            'test -f "$RUN_DIR/PROFILER_SUMMARY.md"',
            'test -f "$RUN_DIR/PROFILER_CAPABILITY.json"',
            'cp "$RUN_DIR/PROFILER_SUMMARY.md" "$RUN_DIR/SUMMARY.txt"',
            "phase_mark profiler_evidence_end",
            "phase_mark engine_profiler_evidence_complete",
        ]
    )
    return "; ".join(commands)


def source_cann_environment_snippet() -> str:
    package_root = gitpartner_source_root(
        Path(os.environ.get("ASCENDOP_WORKSPACE_ROOT") or Path.cwd())
    )
    if str(package_root) not in sys.path:
        sys.path.insert(0, str(package_root))
    return str(getattr(_gp_submit_module(), "_source_cann_env_snippet")())


def _gp_submit_module() -> Any:
    """Load the generation-pinned Engine plan compiler through its plugin API."""

    return importlib.import_module(
        "limited_remote_partner.gateway." + "submit_job"
    )


def extend_profiler_finalize_shell(shell: str) -> str:
    return "; ".join(
        [
            shell,
            (
                "for ARTIFACT in PROFILER_EVIDENCE.json PROFILER_SUMMARY.md "
                "PROFILER_CAPABILITY.json profiler_warmup.log msprof_op_help.txt; do "
                'if [ -e "$RUN_DIR/$ARTIFACT" ]; then cp -a "$RUN_DIR/$ARTIFACT" '
                '"$ASCENDOP_ENGINE_JOB_ROOT/result/$ARTIFACT"; fi; done'
            ),
            (
                'if [ -d "$RUN_DIR/profiler_raw" ]; then '
                'cp -a "$RUN_DIR/profiler_raw" '
                '"$ASCENDOP_ENGINE_JOB_ROOT/result/profiler_raw"; '
                "else mkdir -p "
                '"$ASCENDOP_ENGINE_JOB_ROOT/result/profiler_raw"; fi'
            ),
            (
                'find "$ASCENDOP_ENGINE_JOB_ROOT/result" -maxdepth 1 '
                '-mindepth 1 -printf "%f\\t%s\\n" | sort '
                '> "$ASCENDOP_ENGINE_JOB_ROOT/result/ENGINE_RESULT_FILES.tsv"'
            ),
        ]
    )


def profiler_evidence_stage_specs(
    shells: dict[str, str],
    *,
    priority: int,
) -> list[dict[str, Any]]:
    required = ("prepare_common", "build_install", "runtime_wheel", "profiler", "finalize")
    if not all(str(shells.get(name) or "") for name in required):
        raise EngineJobBuildError(
            "profiler evidence profile is missing required stage shells"
        )
    return [
        {
            "name": "prepare-materialize-environment",
            "resource": "host",
            "pre_activation": True,
            "depends_on": [],
            "max_attempts": 2,
            "command": ["bash", "-c", shells["prepare_common"]],
        },
        {
            "name": "operator-build-install",
            "resource": "host",
            "pre_activation": True,
            "depends_on": ["prepare-materialize-environment"],
            "max_attempts": 2,
            "command": ["bash", "-c", shells["build_install"]],
        },
        {
            "name": "runtime-wheel-install",
            "resource": "host",
            "pre_activation": True,
            "depends_on": ["prepare-materialize-environment"],
            "max_attempts": 2,
            "command": ["bash", "-c", shells["runtime_wheel"]],
        },
        {
            "name": "profiler-evidence",
            "resource": "device",
            "depends_on": ["operator-build-install", "runtime-wheel-install"],
            "locks": ["npu", "performance-measurement"],
            "priority": priority,
            "command": ["bash", "-c", shells["profiler"]],
        },
        {
            "name": "assemble-profiler-evidence",
            "resource": "host",
            "depends_on": ["profiler-evidence"],
            "max_attempts": 2,
            "command": ["bash", "-c", shells["finalize"]],
        },
    ]


def split_stage_specs(shells: dict[str, str]) -> list[dict[str, Any]]:
    if all(
        str(shells.get(key) or "")
        for key in ("prepare_common", "build_install", "runtime_wheel")
    ):
        prepare_stages = [
            {
                "name": "prepare-materialize-environment",
                "resource": "host",
                "pre_activation": True,
                "depends_on": [],
                "max_attempts": 2,
                "command": ["bash", "-lc", shells["prepare_common"]],
            },
            {
                "name": "operator-build-install",
                "resource": "host",
                "pre_activation": True,
                "depends_on": ["prepare-materialize-environment"],
                "max_attempts": 2,
                "command": ["bash", "-lc", shells["build_install"]],
            },
            {
                "name": "runtime-wheel-install",
                "resource": "host",
                "pre_activation": True,
                "depends_on": ["prepare-materialize-environment"],
                "max_attempts": 2,
                "command": ["bash", "-lc", shells["runtime_wheel"]],
            },
        ]
        prepare_dependencies = ["operator-build-install", "runtime-wheel-install"]
    else:
        # Compatibility for frozen/offline fixtures built before host-prep DAG
        # support. New production specs always use the three stages above.
        prepare_stages = [
            {
                "name": "prepare-build-install-wheel",
                "resource": "host",
                "pre_activation": True,
                "depends_on": [],
                "max_attempts": 2,
                "command": ["bash", "-lc", shells["prepare"]],
            }
        ]
        prepare_dependencies = ["prepare-build-install-wheel"]
    return [
        *prepare_stages,
        {
            "name": "correctness",
            "resource": "device",
            "depends_on": prepare_dependencies,
            "locks": ["npu"],
            "command": ["bash", "-lc", shells["correctness"]],
        },
        {
            "name": "performance-capture",
            "resource": "device",
            "depends_on": ["correctness"],
            "locks": ["npu", "performance-measurement"],
            "command": ["bash", "-lc", shells["capture"]],
        },
        {
            "name": "profile-export",
            "resource": "export",
            "depends_on": ["performance-capture"],
            "locks": ["performance-measurement"],
            "max_attempts": 2,
            "command": ["bash", "-lc", shells["export"]],
        },
        {
            "name": "profile-parse",
            "resource": "host",
            "depends_on": ["profile-export"],
            "max_attempts": 2,
            "command": ["bash", "-lc", shells["parse"]],
        },
        {
            "name": "assemble-result",
            "resource": "host",
            "depends_on": ["correctness", "profile-parse"],
            "max_attempts": 2,
            "command": ["bash", "-lc", shells["finalize"]],
        },
    ]


def performance_first_stage_specs(shells: dict[str, str]) -> list[dict[str, Any]]:
    return apply_performance_first_order(split_stage_specs(shells))


def case_cache_prewarm_stage_specs(
    shells: dict[str, str], *, op: str
) -> list[dict[str, Any]]:
    required_shells = (
        "case_cache_prewarm_materialize",
        "runtime_wheel",
        "finalize",
    )
    if not all(str(shells.get(name) or "") for name in required_shells):
        raise EngineJobBuildError("case-cache prewarm profile is missing host shells")
    if "limited_remote_partner.resources.case_cache prepare" not in shells["runtime_wheel"]:
        raise EngineJobBuildError("case-cache prewarm runtime shell has no cache prepare")
    finalize = "; ".join(
        [
            *engine_stage_prelude("case_cache_prewarm_finalize", runtime=False),
            (
                "printf '%s\\n' "
                + shlex.quote(f"{op} case cache prewarm: PASS")
                + ' > "$RUN_DIR/SUMMARY.txt"'
            ),
            "phase_mark engine_case_cache_prewarm_complete",
            shells["finalize"],
        ]
    )
    stages = [
        {
            "name": "prepare-case-cache-payload",
            "resource": "host",
            "pre_activation": True,
            "depends_on": [],
            "max_attempts": 2,
            "command": [
                "bash",
                "-c",
                shells["case_cache_prewarm_materialize"],
            ],
        },
        {
            "name": "runtime-wheel-case-cache",
            "resource": "host",
            "pre_activation": True,
            "depends_on": ["prepare-case-cache-payload"],
            "max_attempts": 2,
            "command": ["bash", "-c", shells["runtime_wheel"]],
        },
        {
            "name": "assemble-case-cache-prewarm",
            "resource": "host",
            "depends_on": ["runtime-wheel-case-cache"],
            "max_attempts": 2,
            "command": ["bash", "-c", finalize],
        },
    ]
    return stages


def scalable_stage_specs(
    shells: dict[str, str],
    *,
    fused_case_cache: bool = False,
    fused_correctness: bool = False,
    fused_postprocess: bool = False,
    profile_export_capture_overlap: bool = False,
) -> list[dict[str, Any]]:
    stages = split_stage_specs(shells)
    case_cache_fused = (
        fused_case_cache or fused_correctness
    ) and not str(shells.get("case_cache") or "")
    if not case_cache_fused and not str(shells.get("case_cache") or ""):
        raise EngineJobBuildError("scalable profile is missing case-cache shell")
    correctness = next(
        stage for stage in stages if stage["name"] == "correctness"
    )
    performance = next(
        stage for stage in stages if stage["name"] == "performance-capture"
    )
    prepare_dependencies = list(correctness.get("depends_on", []))
    case_dependencies = (
        ["runtime-wheel-install"]
        if "runtime-wheel-install" in prepare_dependencies
        else list(prepare_dependencies)
    )
    if not case_cache_fused:
        insertion_index = next(
            index for index, stage in enumerate(stages) if stage["name"] == "correctness"
        )
        stages.insert(
            insertion_index,
            {
                "name": "case-cache",
                "resource": "host",
                "pre_activation": True,
                "depends_on": case_dependencies,
                "max_attempts": 2,
                "command": ["bash", "-lc", shells["case_cache"]],
            },
        )
        for stage in stages:
            if stage["name"] == "correctness":
                stage["depends_on"] = [
                    *(
                        ["operator-build-install"]
                        if "operator-build-install" in prepare_dependencies
                        else []
                    ),
                    "case-cache",
                ]
            elif stage["name"] == "performance-capture":
                stage["depends_on"] = ["correctness"]
    if fused_correctness:
        correctness = next(
            stage for stage in stages if stage["name"] == "correctness"
        )
        correctness.update(
            {
                "resource": "host",
                "locks": [],
                "command": ["bash", "-lc", shells["correctness"]],
            }
        )
    if fused_postprocess:
        postprocess_command = str(shells.get("postprocess") or "")
        if not postprocess_command:
            raise EngineJobBuildError("fused profile is missing postprocess shell")
        replaced_names = {
            "correctness",
            "profile-export",
            "profile-parse",
            "assemble-result",
        }
        if fused_correctness:
            performance["depends_on"] = list(correctness.get("depends_on", []))
        stages = [stage for stage in stages if stage["name"] not in replaced_names]
        performance_index = next(
            index
            for index, stage in enumerate(stages)
            if stage["name"] == "performance-capture"
        )
        stages.insert(
            performance_index + 1,
            {
                "name": "postprocess-result",
                "resource": "export",
                "depends_on": ["performance-capture"],
                "locks": (
                    []
                    if profile_export_capture_overlap
                    else ["performance-measurement"]
                ),
                "priority": 15,
                "max_attempts": 2,
                "command": ["bash", "-lc", postprocess_command],
            },
        )
        # Fused v3 stages source every required CANN/runtime environment
        # explicitly. Avoid reloading the remote login profile for each stage.
        for stage in stages:
            command = list(stage.get("command", []))
            if command[:2] == ["bash", "-lc"]:
                stage["command"] = ["bash", "-c", *command[2:]]
    return stages


def apply_performance_first_order(
    stages: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    correctness_index = next(
        index for index, stage in enumerate(stages) if stage["name"] == "correctness"
    )
    correctness = stages.pop(correctness_index)
    prepare_dependencies = list(correctness.get("depends_on", []))
    export_index = next(
        index for index, stage in enumerate(stages) if stage["name"] == "profile-export"
    )
    stages.insert(export_index + 1, correctness)
    for stage in stages:
        if stage["name"] == "performance-capture":
            stage["depends_on"] = prepare_dependencies
            stage["priority"] = 10
        elif stage["name"] == "profile-export":
            stage["priority"] = 15
        elif stage["name"] == "correctness":
            stage["depends_on"] = ["performance-capture"]
            stage["priority"] = 20
    return stages


def rewrite_engine_path_fragment(fragment: str) -> str:
    if fragment.startswith("cd \"$ASCENDOP_REMOTE_ROOT\""):
        return 'cd "$ASCENDOP_ENGINE_JOB_ROOT"'
    if fragment.startswith("PAYLOAD="):
        return 'PAYLOAD="$ASCENDOP_ENGINE_PAYLOAD_ROOT"'
    if fragment.startswith("RUN_DIR="):
        return 'RUN_DIR="$ASCENDOP_ENGINE_JOB_ROOT/work"'
    if fragment.startswith("VENDOR_DIR="):
        return 'VENDOR_DIR="$ASCENDOP_ENGINE_JOB_ROOT/vendor"'
    return fragment


def rewrite_wheel_install_fragment(
    fragment: str, *, cached_site: bool = False
) -> str:
    if "-m pip install dist/custom_ops" not in fragment:
        return fragment
    if cached_site:
        return 'test -d "$RUN_DIR/py_site" || { echo PY_SITE_CACHE_MISSING; exit 47; }'
    return (
        'rm -rf "$RUN_DIR/py_site"; mkdir -p "$RUN_DIR/py_site"; '
        '"$PYTHON_BIN" -m pip install --no-deps --target "$RUN_DIR/py_site" '
        '"$RUN_DIR"/wheel/custom_ops*.whl > "$RUN_DIR/pip_install.log" 2>&1 || '
        '{ echo PIP_INSTALL_FAILED; tail -80 "$RUN_DIR/pip_install.log"; exit 47; }'
    )


def rewrite_wheel_cache_fragments(
    fragments: list[str],
    target_arch: str,
    *,
    materialize_site: bool = False,
    performance_samples_per_case: int = DEFAULT_PERFORMANCE_TASK_ROWS_PER_CASE,
    expected_profile_round_anchor_count: int = 0,
) -> list[str]:
    try:
        start = fragments.index("phase_mark test_wheel_build_start")
        end = fragments.index("phase_mark test_wheel_build_end")
    except ValueError as exc:
        raise EngineJobBuildError("legacy wheel build boundary changed") from exc
    if end <= start:
        raise EngineJobBuildError("legacy wheel build boundary is invalid")
    site_args = (
        '--site-output "$RUN_DIR/py_site" '
        '--site-log "$RUN_DIR/pip_install.log" '
        if materialize_site
        else ""
    )
    cached_build = [
        "phase_mark test_wheel_build_start",
        'rm -rf "$RUN_DIR/wheel"',
        (
            'python3 "$ASCENDOP_ENGINE_PAYLOAD_ROOT/profile_rounds_override.py" '
            '--task-case "$RUN_DIR/task_case" '
            f"--effective-rounds {int(performance_samples_per_case)} "
            '--receipt "$RUN_DIR/PERFORMANCE_ROUNDS_OVERRIDE.json" '
            "--expected-anchor-count "
            f"{int(expected_profile_round_anchor_count)} || "
            "{ echo PROFILE_ROUNDS_OVERRIDE_FAILED; "
            'cat "$RUN_DIR/PERFORMANCE_ROUNDS_OVERRIDE.json" 2>/dev/null || true; '
            "exit 46; }"
        ),
        (
            'TORCH_ADAPTER_HELPER="$RUN_DIR/task_case/common/pytorch_npu_helper.hpp"; '
            'if [ -f "$TORCH_ADAPTER_HELPER" ] && '
            "grep -q 'options.dtype(kByte)' \"$TORCH_ADAPTER_HELPER\"; then "
            "sed -i 's/options\\.dtype(kByte)/options.dtype(c10::kByte)/g' "
            '"$TORCH_ADAPTER_HELPER"; '
            "echo TORCH_ADAPTER_KBYTE_PATCHED:$TORCH_ADAPTER_HELPER; "
            "else echo TORCH_ADAPTER_KBYTE_PATCH_SKIPPED; fi"
        ),
        (
            'ACL_TENSOR_ADAPTER="$RUN_DIR/task_case/extension/custom_op.cpp"; '
            'if [ -f "$ACL_TENSOR_ADAPTER" ] && '
            "grep -q 'aclTensor' \"$ACL_TENSOR_ADAPTER\" && "
            "! grep -Eq 'typedef[[:space:]]+struct[[:space:]]+aclTensor[[:space:]]+aclTensor|"
            "struct[[:space:]]+aclTensor[[:space:]]*;' \"$ACL_TENSOR_ADAPTER\"; then "
            "sed -i '1i typedef struct aclTensor aclTensor;' \"$ACL_TENSOR_ADAPTER\"; "
            "echo ACL_TENSOR_FORWARD_DECL_PATCHED:$ACL_TENSOR_ADAPTER; "
            "else echo ACL_TENSOR_FORWARD_DECL_PATCH_SKIPPED; fi"
        ),
        (
            'python3 -m limited_remote_partner.resources.wheel_cache '
            '--source "$RUN_DIR/task_case" --python "$PYTHON_BIN" '
            '--cache-root "$ASCENDOP_ENGINE_CACHE_ROOT/wheels" '
            '--output-dir "$RUN_DIR/wheel" '
            + site_args
            + f"--target-arch {shlex.quote(target_arch)} --json "
            + '> "$RUN_DIR/WHEEL_CACHE.json" 2> "$RUN_DIR/whl.log" || '
            + '{ echo WHL_CACHE_FAILED; tail -80 "$RUN_DIR/whl.log"; '
            + 'tail -80 "$RUN_DIR/task_case/wheel_build.log" 2>/dev/null || true; exit 46; }'
        ),
        "phase_mark test_wheel_build_end",
    ]
    return fragments[:start] + cached_build + fragments[end + 1 :]


def rewrite_operator_cache_fragments(
    fragments: list[str], target_arch: str
) -> list[str]:
    try:
        start = fragments.index("phase_mark operator_build_start")
        install = fragments.index("phase_mark operator_install_start")
    except ValueError as exc:
        raise EngineJobBuildError("legacy operator build boundary changed") from exc
    if install <= start:
        raise EngineJobBuildError("legacy operator build boundary is invalid")
    cached_build = [
        "phase_mark operator_build_start",
        (
            "python3 -m limited_remote_partner.resources.operator_cache "
            '--source "$RUN_DIR/source" --build-python "$BUILD_PYTHON_BIN" '
            '--cache-root "$ASCENDOP_ENGINE_CACHE_ROOT/operators" '
            '--output "$RUN_DIR/operator.run" '
            '--receipt "$RUN_DIR/OPERATOR_CACHE.json" '
            '--build-log "$RUN_DIR/build.log" '
            f"--target-arch {shlex.quote(target_arch)} --json "
            '> "$RUN_DIR/operator_cache.log" 2>&1 || '
            "{ echo OPERATOR_CACHE_FAILED; "
            'tail -100 "$RUN_DIR/operator_cache.log"; '
            'tail -100 "$RUN_DIR/build.log" 2>/dev/null || true; exit 44; }'
        ),
        "phase_mark operator_build_end",
        'RUN_FILE="$RUN_DIR/operator.run"',
        'test -f "$RUN_FILE" || { echo RUN_FILE_MISSING; exit 44; }',
    ]
    return fragments[:start] + cached_build + fragments[install:]


def engine_stage_prelude(
    stage: str,
    *,
    runtime: bool = True,
    cann_snippet: str = "",
) -> list[str]:
    commands = [
        "set -euo pipefail",
        'RUN_DIR="$ASCENDOP_ENGINE_JOB_ROOT/work"',
        'VENDOR_DIR="$ASCENDOP_ENGINE_JOB_ROOT/vendor"',
        'PHASE_TIMELINE="$RUN_DIR/PHASE_TIMELINE.jsonl"',
        'phase_mark() { PHASE_NAME="$1"; PHASE_TS=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ); PHASE_EPOCH_MS=$(date +%s%3N); printf \'{"phase":"%s","timestamp":"%s","epoch_ms":%s}\\n\' "$PHASE_NAME" "$PHASE_TS" "$PHASE_EPOCH_MS" >> "$PHASE_TIMELINE"; echo "ASCENDOP_PHASE:$PHASE_NAME:$PHASE_TS"; }',
        f"phase_mark engine_{stage}_start",
    ]
    if cann_snippet:
        commands.append(cann_snippet)
    if runtime:
        commands.extend(
            [
                'test -f "$RUN_DIR/engine_runtime.env"',
                '. "$RUN_DIR/engine_runtime.env"',
                'test -f "$RUN_DIR/engine_operator.env"',
                '. "$RUN_DIR/engine_operator.env"',
                'export PYTHONPATH="$RUN_DIR/py_site:${PYTHONPATH:-}"',
                "export PYTHONDONTWRITEBYTECODE=1",
                "export TORCH_DEVICE_BACKEND_AUTOLOAD=0",
                'export LD_LIBRARY_PATH="$OPAPI_LIB_DIR:${LD_LIBRARY_PATH:-}"',
                'export PATH="$(dirname "$PYTHON_BIN"):$PATH"',
                'if [ -d "$RUN_DIR/attack_case" ]; then export ASCENDOP_ATTACK_META="$RUN_DIR/attack_case/meta.json"; fi',
                'cd "$RUN_DIR/task_case"',
            ]
        )
    return commands


def case_cache_prewarm_materialize_shell() -> str:
    """Materialize a case-only run directory without touching operator source."""

    commands = [
        "set -euo pipefail",
        'cd "$ASCENDOP_ENGINE_JOB_ROOT"',
        'PAYLOAD="$ASCENDOP_ENGINE_PAYLOAD_ROOT"',
        'RUN_DIR="$ASCENDOP_ENGINE_JOB_ROOT/work"',
        'VENDOR_DIR="$ASCENDOP_ENGINE_JOB_ROOT/vendor"',
        'test -d "$PAYLOAD/source_snapshot" || { echo REQUIRED_PATH_MISSING:$PAYLOAD/source_snapshot; exit 42; }',
        'test -d "$PAYLOAD/task_case" || { echo REQUIRED_PATH_MISSING:$PAYLOAD/task_case; exit 42; }',
        'rm -rf "$RUN_DIR" "$VENDOR_DIR"',
        'mkdir -p "$RUN_DIR" "$VENDOR_DIR"',
        'PHASE_TIMELINE="$RUN_DIR/PHASE_TIMELINE.jsonl"',
        'phase_mark() { PHASE_NAME="$1"; PHASE_TS=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ); PHASE_EPOCH_MS=$(date +%s%3N); printf \'{"phase":"%s","timestamp":"%s","epoch_ms":%s}\\n\' "$PHASE_NAME" "$PHASE_TS" "$PHASE_EPOCH_MS" >> "$PHASE_TIMELINE"; echo "ASCENDOP_PHASE:$PHASE_NAME:$PHASE_TS"; }',
        "export PHASE_TIMELINE",
        "phase_mark remote_start",
        "phase_mark payload_copy_start",
        'cp -a "$PAYLOAD/task_case" "$RUN_DIR/task_case"',
        'if [ -d "$PAYLOAD/attack_case" ]; then cp -a "$PAYLOAD/attack_case" "$RUN_DIR/attack_case"; export ASCENDOP_ATTACK_META="$RUN_DIR/attack_case/meta.json"; echo ATTACK_CASE_META:$ASCENDOP_ATTACK_META; fi',
        "phase_mark payload_copy_end",
        'chmod -R u+rwX "$RUN_DIR/task_case"',
        '[ ! -d "$RUN_DIR/attack_case" ] || chmod -R u+rwX "$RUN_DIR/attack_case"',
        'if [ ! -f "$RUN_DIR/task_case/setup.py" ] && [ -f "$RUN_DIR/task_case/task_case/setup.py" ]; then echo TASK_CASE_NESTED_LAYOUT_FIXED; mv "$RUN_DIR/task_case" "$RUN_DIR/task_case_outer"; mv "$RUN_DIR/task_case_outer/task_case" "$RUN_DIR/task_case"; rm -rf "$RUN_DIR/task_case_outer"; fi',
        'test -f "$RUN_DIR/task_case/setup.py" || { echo TASK_CASE_SETUP_MISSING; find "$RUN_DIR/task_case" -maxdepth 3 -mindepth 1 | sort | head -120; exit 46; }',
        'find "$RUN_DIR/task_case" -type f -name \'*.sh\' -exec chmod u+x {} +',
        'if [ -d "$RUN_DIR/attack_case" ]; then find "$RUN_DIR/attack_case" -type f -name \'*.sh\' -exec chmod u+x {} +; fi',
        # A prewarm deliberately has no operator-build stage.  The reused
        # runtime shell still consumes the normal build/runtime handoff, so
        # provide the source-independent empty handoff instead of waiting for
        # a producer that this profile does not schedule.
        'printf \'export OPAPI_LIB_DIR=%q\\n\' "" > "$RUN_DIR/engine_operator.env"',
        'printf \'0\\n\' > "$RUN_DIR/engine_operator_build.state"',
        "phase_mark engine_case_cache_payload_materialize_complete",
    ]
    return "; ".join(commands)


def copy_tree_without_symlinks(
    source: Path,
    destination: Path,
    *,
    exclude_names: tuple[str, ...] = (),
) -> None:
    for path in source.rglob("*"):
        if path.is_symlink():
            raise EngineJobBuildError(f"engine payload cannot contain symlinks: {path}")
    shutil.copytree(
        filesystem_path(source),
        filesystem_path(destination),
        ignore=shutil.ignore_patterns(
            "__pycache__", "*.pyc", "*.pyo", *exclude_names
        ),
    )


def engine_build_dir_name(engine_job_id: str) -> str:
    if len(engine_job_id) <= MAX_ENGINE_BUILD_DIR_NAME:
        return engine_job_id
    digest = hashlib.sha256(engine_job_id.encode("utf-8")).hexdigest()[:16]
    prefix_length = MAX_ENGINE_BUILD_DIR_NAME - len(digest) - 1
    prefix = engine_job_id[:prefix_length].rstrip("-_")
    return f"{prefix}-{digest}"


def filesystem_path(path: Path) -> Path:
    resolved = str(path.resolve())
    if os.name != "nt" or resolved.startswith("\\\\?\\"):
        return Path(resolved)
    if resolved.startswith("\\\\"):
        return Path("\\\\?\\UNC\\" + resolved[2:])
    return Path("\\\\?\\" + resolved)


def tree_digest(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        relative = path.relative_to(root).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        if path.is_file():
            update_canonical_file_digest(digest, path)
            digest.update(b"\0")
    return digest.hexdigest()


def update_canonical_file_digest(digest: Any, path: Path) -> None:
    """Hash transport-stable bytes across Git CRLF/LF checkout policies."""
    carry = b""
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            data = carry + chunk
            carry = b"\r" if data.endswith(b"\r") else b""
            if carry:
                data = data[:-1]
            digest.update(data.replace(b"\r\n", b"\n"))
    if carry:
        digest.update(carry)


def option_value(argv: list[str], name: str, default: str) -> str:
    try:
        return argv[argv.index(name) + 1]
    except (ValueError, IndexError):
        return default


def strip_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def safe_token(value: str) -> str:
    token = re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._-")
    if not token:
        raise EngineJobBuildError(f"invalid engine token: {value}")
    return token


def release_name(test_version: str) -> str:
    parts = test_version.split("_")
    return "_".join(parts[:2]) if len(parts) >= 2 else test_version


def gitpartner_vendor(value: str) -> str:
    clean = re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_").lower()
    return clean if clean.endswith("_gitpartner") else clean + "_gitpartner"
