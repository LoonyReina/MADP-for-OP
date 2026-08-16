from __future__ import annotations

import argparse
import json
import math
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from limited_remote_partner.engine.test_engine import atomic_write_json, utc_now


class EngineABError(RuntimeError):
    pass


STRICT_EQUIVALENCE_MODE = "strict-pair-v1"
CAUSAL_PERFORMANCE_FIRST_MODE = "causal-performance-first-v1"
SCHEDULER_POLICY_MODE = "scheduler-policy-v1"
PERFORMANCE_FIRST_CONTROL_PROFILE = "engine-v1-staged-performance-first-split"
PERFORMANCE_FIRST_CANDIDATE_PROFILE = (
    "engine-v1-staged-performance-first-correctness-batched"
)
FUSED_SCALABLE_PROFILE = "engine-v3-staged-fused"


def compare_ab(
    *,
    baseline_perf: Path,
    candidate_perf: Path,
    baseline_correctness: Path,
    candidate_correctness: Path,
    median_limit_percent: float = 1.0,
    p95_limit_percent: float = 5.0,
    stddev_limit_percent: float = 10.0,
    weighted_limit_percent: float = 1.0,
    expected_repetitions: int | None = None,
    equivalence_mode: str = STRICT_EQUIVALENCE_MODE,
) -> dict[str, Any]:
    baseline = read_manifest(baseline_perf, "baseline performance")
    candidate = read_manifest(candidate_perf, "candidate performance")
    baseline_ok = read_manifest(baseline_correctness, "baseline correctness")
    candidate_ok = read_manifest(candidate_correctness, "candidate correctness")
    if equivalence_mode not in {
        STRICT_EQUIVALENCE_MODE,
        CAUSAL_PERFORMANCE_FIRST_MODE,
        SCHEDULER_POLICY_MODE,
    }:
        raise EngineABError(f"unsupported A/B equivalence mode: {equivalence_mode}")
    structural_blockers: list[str] = []
    statistical_blockers: list[str] = []
    correctness = compare_correctness(
        baseline_ok,
        candidate_ok,
        expected_repetitions=expected_repetitions,
        blockers=structural_blockers,
    )
    performance = compare_performance(
        baseline,
        candidate,
        median_limit_percent=median_limit_percent,
        p95_limit_percent=p95_limit_percent,
        stddev_limit_percent=stddev_limit_percent,
        weighted_limit_percent=weighted_limit_percent,
        blockers=structural_blockers,
        statistical_blockers=statistical_blockers,
    )
    for name in ("baseline", "candidate"):
        if correctness[name]["case_ids"] != performance[f"{name}_case_ids"]:
            structural_blockers.append(
                f"{name} correctness and performance case sets differ"
            )
    causal_isolation: dict[str, Any] = {}
    scheduler_policy_isolation: dict[str, Any] = {}
    if equivalence_mode == CAUSAL_PERFORMANCE_FIRST_MODE:
        causal_isolation = compare_causal_performance_first(
            baseline_perf=baseline_perf,
            candidate_perf=candidate_perf,
            baseline=baseline,
            candidate=candidate,
        )
        blockers = [
            *structural_blockers,
            *[str(item) for item in causal_isolation.get("blockers", [])],
        ]
        protocol_version = "engine-ab-v2"
    elif equivalence_mode == SCHEDULER_POLICY_MODE:
        scheduler_policy_isolation = compare_scheduler_policy(
            baseline_perf=baseline_perf,
            candidate_perf=candidate_perf,
            baseline=baseline,
            candidate=candidate,
        )
        blockers = [
            *structural_blockers,
            *[
                str(item)
                for item in scheduler_policy_isolation.get("blockers", [])
            ],
        ]
        protocol_version = "engine-ab-v3"
    else:
        blockers = [*structural_blockers, *statistical_blockers]
        protocol_version = "engine-ab-v1"
    return {
        "protocol_version": protocol_version,
        "generated_at": utc_now(),
        "verdict": "PASS" if not blockers else "FAIL",
        "blockers": blockers,
        "equivalence_mode": equivalence_mode,
        "statistical_verdict": "PASS" if not statistical_blockers else "FAIL",
        "statistical_blockers": statistical_blockers,
        "causal_isolation": causal_isolation,
        "scheduler_policy_isolation": scheduler_policy_isolation,
        "thresholds_percent": {
            "median": median_limit_percent,
            "p95": p95_limit_percent,
            "stddev": stddev_limit_percent,
            "weighted": weighted_limit_percent,
        },
        "correctness": correctness,
        "performance": performance,
        "inputs": {
            "baseline_perf": str(baseline_perf.resolve()),
            "candidate_perf": str(candidate_perf.resolve()),
            "baseline_correctness": str(baseline_correctness.resolve()),
            "candidate_correctness": str(candidate_correctness.resolve()),
        },
    }


def compare_ab_series(
    pair_reports: list[dict[str, Any]], *, minimum_pairs: int = 3
) -> dict[str, Any]:
    blockers: list[str] = []
    required_pairs = max(3, int(minimum_pairs))
    if len(pair_reports) < required_pairs:
        blockers.append(f"A/B series has fewer than {required_pairs} pairs")
    pair_ids: set[str] = set()
    orders: set[str] = set()
    modes = {
        str(report.get("equivalence_mode") or STRICT_EQUIVALENCE_MODE)
        for report in pair_reports
    }
    if len(modes) != 1:
        blockers.append("A/B series mixes equivalence modes")
    equivalence_mode = next(iter(modes), STRICT_EQUIVALENCE_MODE)
    causal_mode = equivalence_mode == CAUSAL_PERFORMANCE_FIRST_MODE
    scheduler_mode = equivalence_mode == SCHEDULER_POLICY_MODE
    aggregate_mode = causal_mode or scheduler_mode
    required_orders = (
        {"control-candidate", "candidate-control"}
        if aggregate_mode
        else {"legacy-engine", "engine-legacy"}
    )
    expected_pair_protocol = (
        "engine-ab-v3"
        if scheduler_mode
        else ("engine-ab-v2" if causal_mode else "engine-ab-v1")
    )
    weighted_shifts: list[float] = []
    remote_engine_generations: set[str] = set()
    for index, report in enumerate(pair_reports, start=1):
        pair_id = str(report.get("pair_id") or f"pair-{index}")
        order = str(report.get("execution_order") or "")
        if pair_id in pair_ids:
            blockers.append(f"duplicate A/B pair id: {pair_id}")
        pair_ids.add(pair_id)
        if order not in required_orders:
            blockers.append(f"{pair_id} has invalid execution order")
        else:
            orders.add(order)
        if (
            report.get("protocol_version") != expected_pair_protocol
            or report.get("verdict") != "PASS"
        ):
            blockers.append(f"{pair_id} did not pass its pair gate")
        performance = report.get("performance", {})
        if isinstance(performance, dict):
            shift = number(performance.get("weighted_shift_percent"))
            if math.isfinite(shift):
                weighted_shifts.append(shift)
        if scheduler_mode:
            isolation = report.get("scheduler_policy_isolation", {})
            if (
                not isinstance(isolation, dict)
                or isolation.get("protocol_version")
                != "engine-scheduler-policy-isolation-v1"
                or isolation.get("ok") is not True
            ):
                blockers.append(f"{pair_id} lacks scheduler-policy isolation proof")
            generation = (
                str(isolation.get("engine_code_generation") or "")
                if isinstance(isolation, dict)
                else ""
            )
            if not generation:
                blockers.append(f"{pair_id} is missing remote engine code generation")
            else:
                remote_engine_generations.add(generation)
    if orders != required_orders:
        blockers.append("A/B series must cover both execution orders")
    weighted_median = (
        round(float(statistics.median(weighted_shifts)), 6)
        if weighted_shifts
        else float("nan")
    )
    weighted_limit = min(
        (
            number(report.get("thresholds_percent", {}).get("weighted"))
            for report in pair_reports
            if isinstance(report.get("thresholds_percent"), dict)
        ),
        default=1.0,
    )
    aggregate_weighted_ok = (
        math.isfinite(weighted_median)
        and math.isfinite(weighted_limit)
        and weighted_limit >= 0
        and abs(weighted_median) <= weighted_limit
    )
    if scheduler_mode and len(remote_engine_generations) != 1:
        blockers.append("scheduler-policy A/B series mixes remote engine generations")
    if aggregate_mode and not aggregate_weighted_ok:
        blockers.append(
            f"{equivalence_mode} A/B series weighted median shift "
            f"{display(weighted_median)}% exceeds {display(weighted_limit)}%"
        )
    return {
        "protocol_version": (
            "engine-ab-series-v3"
            if scheduler_mode
            else ("engine-ab-series-v2" if causal_mode else "engine-ab-series-v1")
        ),
        "generated_at": utc_now(),
        "verdict": "PASS" if not blockers else "FAIL",
        "blockers": blockers,
        "equivalence_mode": equivalence_mode,
        "minimum_pairs": required_pairs,
        "pair_count": len(pair_reports),
        "execution_orders": sorted(orders),
        "required_execution_orders": sorted(required_orders),
        "order_complete": orders == required_orders,
        "remote_engine_code_generation": (
            next(iter(remote_engine_generations))
            if len(remote_engine_generations) == 1
            else ""
        ),
        "remote_engine_code_generations": sorted(remote_engine_generations),
        "weighted_shift_percent": {
            "values": weighted_shifts,
            "mean": round(sum(weighted_shifts) / len(weighted_shifts), 6)
            if weighted_shifts
            else float("nan"),
            "max_abs": round(max((abs(value) for value in weighted_shifts), default=float("nan")), 6),
        },
        "aggregate_weighted_shift_percent": {
            "values": weighted_shifts,
            "mean": round(sum(weighted_shifts) / len(weighted_shifts), 6)
            if weighted_shifts
            else float("nan"),
            "median": weighted_median,
            "max_abs": round(
                max((abs(value) for value in weighted_shifts), default=float("nan")),
                6,
            ),
            "limit": weighted_limit,
            "ok": aggregate_weighted_ok,
        },
        "pairs": pair_reports,
    }


def compare_correctness(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    *,
    expected_repetitions: int | None,
    blockers: list[str],
) -> dict[str, Any]:
    records: dict[str, dict[str, Any]] = {}
    for name, value in (("baseline", baseline), ("candidate", candidate)):
        state = str(value.get("state") or "")
        repetitions = int(value.get("repetitions", 0) or 0)
        case_count = int(value.get("case_count", 0) or 0)
        case_ids = positive_case_ids(value.get("case_ids"))
        execution_count = int(value.get("execution_count", 0) or 0)
        fail_count = int(value.get("fail_count", 0) or 0)
        declared_expected = int(value.get("expected_execution_count", 0) or 0)
        expected_executions = case_count * repetitions
        executions_complete = correctness_execution_grid_complete(
            value.get("executions"), case_ids=case_ids, repetitions=repetitions
        )
        ok = (
            state == "passed"
            and repetitions > 0
            and (expected_repetitions is None or repetitions == expected_repetitions)
            and case_count > 0
            and len(case_ids) == case_count
            and execution_count == expected_executions
            and (declared_expected in {0, expected_executions})
            and executions_complete
            and fail_count == 0
        )
        if not ok:
            blockers.append(f"{name} correctness manifest is incomplete or failed")
        records[name] = {
            "state": state,
            "repetitions": repetitions,
            "case_count": case_count,
            "case_ids": case_ids,
            "execution_count": execution_count,
            "expected_execution_count": expected_executions,
            "execution_grid_complete": executions_complete,
            "fail_count": fail_count,
            "ok": ok,
        }
    if records["baseline"]["case_count"] != records["candidate"]["case_count"]:
        blockers.append("correctness case counts differ")
    if records["baseline"]["case_ids"] != records["candidate"]["case_ids"]:
        blockers.append("correctness case sets differ")
    if records["baseline"]["repetitions"] != records["candidate"]["repetitions"]:
        blockers.append("correctness repetition counts differ")
    return records


def positive_case_ids(value: object) -> list[int]:
    if not isinstance(value, list):
        return []
    result: list[int] = []
    for item in value:
        try:
            case_id = int(item)
        except (TypeError, ValueError):
            return []
        if case_id <= 0 or case_id in result:
            return []
        result.append(case_id)
    return result


def correctness_execution_grid_complete(
    value: object, *, case_ids: list[int], repetitions: int
) -> bool:
    if not isinstance(value, list) or not case_ids or repetitions <= 0:
        return False
    observed: set[tuple[int, int]] = set()
    for item in value:
        if not isinstance(item, dict):
            return False
        try:
            key = (int(item.get("case", 0)), int(item.get("repetition", 0)))
        except (TypeError, ValueError):
            return False
        if key in observed or str(item.get("verdict") or "") != "PASS":
            return False
        observed.add(key)
    expected = {
        (case_id, repetition)
        for case_id in case_ids
        for repetition in range(1, repetitions + 1)
    }
    return observed == expected


def compare_performance(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    *,
    median_limit_percent: float,
    p95_limit_percent: float,
    stddev_limit_percent: float,
    weighted_limit_percent: float,
    blockers: list[str],
    statistical_blockers: list[str] | None = None,
) -> dict[str, Any]:
    statistics_failures = (
        blockers if statistical_blockers is None else statistical_blockers
    )
    for name, value in (("baseline", baseline), ("candidate", candidate)):
        if value.get("state") != "parsed" or value.get("failures"):
            blockers.append(f"{name} performance manifest is incomplete or failed")
    if str(baseline.get("weighted_formula") or "") != str(
        candidate.get("weighted_formula") or ""
    ):
        blockers.append("weighted formulas differ")
    if list(baseline.get("weighted_weights") or []) != list(
        candidate.get("weighted_weights") or []
    ):
        blockers.append("weighted weights differ")
    baseline_cases = case_map(baseline, "baseline", blockers)
    candidate_cases = case_map(candidate, "candidate", blockers)
    if set(baseline_cases) != set(candidate_cases):
        blockers.append("performance case sets differ")
    cases: list[dict[str, Any]] = []
    for case in sorted(set(baseline_cases) & set(candidate_cases)):
        left = baseline_cases[case]
        right = candidate_cases[case]
        sample_count_equal = int(left.get("sample_count", 0) or 0) == int(
            right.get("sample_count", 0) or 0
        )
        enough_samples = min(
            int(left.get("sample_count", 0) or 0),
            int(right.get("sample_count", 0) or 0),
        ) >= 40
        if not sample_count_equal:
            blockers.append(f"case{case} profiler sample counts differ")
        if not enough_samples:
            blockers.append(f"case{case} has fewer than 40 profiler samples")
        shifts = {
            "median": shift_percent(left.get("median_us"), right.get("median_us")),
            "p95": shift_percent(left.get("p95_us"), right.get("p95_us")),
            "stddev": shift_percent(left.get("stddev_us"), right.get("stddev_us")),
        }
        limits = {
            "median": median_limit_percent,
            "p95": p95_limit_percent,
            "stddev": stddev_limit_percent,
        }
        metric_ok: dict[str, bool] = {}
        for metric, shift in shifts.items():
            ok = math.isfinite(shift) and abs(shift) <= limits[metric]
            metric_ok[metric] = ok
            if not ok:
                statistics_failures.append(
                    f"case{case} {metric} shift {display(shift)}% exceeds "
                    f"{limits[metric]:g}%"
                )
        cases.append(
            {
                "case": case,
                "baseline": compact_case(left),
                "candidate": compact_case(right),
                "shift_percent": shifts,
                "metric_ok": metric_ok,
                "sample_count_equal": sample_count_equal,
                "enough_samples": enough_samples,
            }
        )
    weighted_shift = shift_percent(
        baseline.get("weighted_time"), candidate.get("weighted_time")
    )
    weighted_ok = math.isfinite(weighted_shift) and abs(weighted_shift) <= weighted_limit_percent
    if not weighted_ok:
        statistics_failures.append(
            f"weighted shift {display(weighted_shift)}% exceeds {weighted_limit_percent:g}%"
        )
    return {
        "weighted_formula": str(baseline.get("weighted_formula") or ""),
        "weighted_weights": list(baseline.get("weighted_weights") or []),
        "baseline_weighted_us": number(baseline.get("weighted_time")),
        "candidate_weighted_us": number(candidate.get("weighted_time")),
        "weighted_shift_percent": weighted_shift,
        "weighted_ok": weighted_ok,
        "baseline_case_ids": sorted(baseline_cases),
        "candidate_case_ids": sorted(candidate_cases),
        "cases": cases,
    }


def compare_causal_performance_first(
    *,
    baseline_perf: Path,
    candidate_perf: Path,
    baseline: dict[str, Any],
    candidate: dict[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    records: dict[str, dict[str, Any]] = {}
    expected_profiles = {
        "baseline": PERFORMANCE_FIRST_CONTROL_PROFILE,
        "candidate": PERFORMANCE_FIRST_CANDIDATE_PROFILE,
    }
    for name, perf_path, parsed in (
        ("baseline", baseline_perf, baseline),
        ("candidate", candidate_perf, candidate),
    ):
        result_dir = perf_path.resolve().parent
        capture = read_manifest(result_dir / "PERF_CAPTURE.json", f"{name} capture")
        exported = read_manifest(result_dir / "PERF_EXPORT.json", f"{name} export")
        identity = read_manifest(
            result_dir / "ENGINE_IDENTITY.json", f"{name} runtime identity"
        )
        timeline = read_phase_timeline(result_dir / "PHASE_TIMELINE.jsonl")
        profile = str(capture.get("execution_profile") or "")
        if profile != expected_profiles[name]:
            blockers.append(
                f"{name} execution profile is {profile or 'missing'}, expected "
                f"{expected_profiles[name]}"
            )
        if capture.get("state") != "captured" or capture.get("capture_mode") != "isolated-process":
            blockers.append(f"{name} does not use completed isolated performance capture")
        stage_sequence = [str(item) for item in capture.get("stage_sequence", [])]
        if not ordered_stages(
            stage_sequence,
            [
                "performance-capture",
                "profile-export",
                "correctness",
                "profile-parse",
            ],
        ):
            blockers.append(f"{name} performance-first stage sequence is not proven")
        if capture.get("capture_before_correctness") is not True:
            blockers.append(f"{name} capture-before-correctness contract is not proven")
        if capture.get("export_before_correctness") is not True:
            blockers.append(f"{name} export-before-correctness contract is not proven")
        contract_digest = str(capture.get("measurement_contract_sha256") or "")
        pipeline_digest = str(capture.get("measurement_pipeline_sha256") or "")
        input_digest = str(exported.get("measurement_input_sha256") or "")
        if not contract_digest or not pipeline_digest:
            blockers.append(f"{name} measurement contract digest is missing")
        if exported.get("state") != "exported" or not input_digest:
            blockers.append(f"{name} exported measurement input digest is missing")
        if (
            parsed.get("measurement_input_verified") is not True
            or str(parsed.get("measurement_input_sha256") or "") != input_digest
        ):
            blockers.append(f"{name} parsed data is not bound to immutable export inputs")
        if not actual_performance_first_order(timeline):
            blockers.append(f"{name} actual capture/export/correctness order is not proven")
        records[name] = {
            "execution_profile": profile,
            "capture_mode": str(capture.get("capture_mode") or ""),
            "stage_sequence": stage_sequence,
            "measurement_contract_sha256": contract_digest,
            "measurement_pipeline_sha256": pipeline_digest,
            "measurement_input_sha256": input_digest,
            "identity": identity,
        }
    for field in (
        "source_sha256",
        "case_bundle_sha256",
        "golden_bundle_sha256",
        "test_contract_sha256",
        "correctness_case_count",
        "performance_case_count",
        "correctness_repetitions",
        "performance_samples_per_case",
        "environment_sha256",
    ):
        left = records.get("baseline", {}).get("identity", {}).get(field)
        right = records.get("candidate", {}).get("identity", {}).get(field)
        if left in {None, ""} or right in {None, ""}:
            blockers.append(f"causal identity is missing {field}")
        elif left != right:
            blockers.append(f"baseline/candidate causal identity {field} differs")
    for field in (
        "measurement_contract_sha256",
        "measurement_pipeline_sha256",
    ):
        left = str(records.get("baseline", {}).get(field) or "")
        right = str(records.get("candidate", {}).get(field) or "")
        if not left or left != right:
            blockers.append(f"baseline/candidate {field} differs or is missing")
    return {
        "protocol_version": "engine-causal-performance-isolation-v1",
        "ok": not blockers,
        "blockers": unique(blockers),
        "baseline": records.get("baseline", {}),
        "candidate": records.get("candidate", {}),
    }


def compare_scheduler_policy(
    *,
    baseline_perf: Path,
    candidate_perf: Path,
    baseline: dict[str, Any],
    candidate: dict[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    records: dict[str, dict[str, Any]] = {}
    expected_policies = {"baseline": "disabled", "candidate": "enabled"}
    for name, perf_path, parsed in (
        ("baseline", baseline_perf, baseline),
        ("candidate", candidate_perf, candidate),
    ):
        result_dir = perf_path.resolve().parent
        capture = read_manifest(result_dir / "PERF_CAPTURE.json", f"{name} capture")
        exported = read_manifest(result_dir / "PERF_EXPORT.json", f"{name} export")
        identity = read_manifest(
            result_dir / "ENGINE_IDENTITY.json", f"{name} runtime identity"
        )
        timeline = read_phase_timeline(result_dir / "PHASE_TIMELINE.jsonl")
        state, terminal = read_engine_job_manifests(perf_path, name)
        profile_values = {
            "capture": str(capture.get("execution_profile") or ""),
            "state": str(state.get("execution_profile") or ""),
            "terminal": str(terminal.get("execution_profile") or ""),
        }
        profile = profile_values["capture"]
        fused_profile = profile == FUSED_SCALABLE_PROFILE
        for source, profile in profile_values.items():
            if profile not in {
                PERFORMANCE_FIRST_CANDIDATE_PROFILE,
                FUSED_SCALABLE_PROFILE,
            }:
                blockers.append(
                    f"{name} {source} execution profile is {profile or 'missing'}, "
                    "expected a supported scheduler-policy profile"
                )
            elif profile != profile_values["capture"]:
                blockers.append(f"{name} execution profile differs across evidence")
        expected_capture_mode = "batched-process" if fused_profile else "isolated-process"
        if capture.get("state") != "captured" or capture.get(
            "capture_mode"
        ) != expected_capture_mode:
            blockers.append(f"{name} does not use the expected completed capture mode")
        stage_sequence = [str(item) for item in capture.get("stage_sequence", [])]
        required_stages = (
            ["performance-capture", "postprocess-result"]
            if fused_profile
            else [
                "performance-capture",
                "profile-export",
                "correctness",
                "profile-parse",
            ]
        )
        if not ordered_stages(stage_sequence, required_stages):
            blockers.append(f"{name} scheduler measurement stage sequence is not proven")
        if fused_profile:
            correctness = read_manifest(
                result_dir / "CORRECTNESS.json", f"{name} correctness"
            )
            if (
                correctness.get("state") != "passed"
                or correctness.get("evidence_source")
                != "profiled-performance-batch"
            ):
                blockers.append(
                    f"{name} fused correctness is not bound to the profiled batch"
                )
        else:
            if capture.get("capture_before_correctness") is not True:
                blockers.append(
                    f"{name} capture-before-correctness contract is not proven"
                )
            if capture.get("export_before_correctness") is not True:
                blockers.append(
                    f"{name} export-before-correctness contract is not proven"
                )
        contract_digest = str(capture.get("measurement_contract_sha256") or "")
        pipeline_digest = str(capture.get("measurement_pipeline_sha256") or "")
        input_digest = str(exported.get("measurement_input_sha256") or "")
        if not contract_digest or not pipeline_digest:
            blockers.append(f"{name} measurement contract digest is missing")
        if exported.get("state") != "exported" or not input_digest:
            blockers.append(f"{name} exported measurement input digest is missing")
        if (
            parsed.get("measurement_input_verified") is not True
            or str(parsed.get("measurement_input_sha256") or "") != input_digest
        ):
            blockers.append(
                f"{name} parsed data is not bound to immutable export inputs"
            )
        order_ok = (
            actual_fused_measurement_order(timeline)
            if fused_profile
            else actual_scheduler_measurement_order(timeline)
        )
        if not order_ok:
            blockers.append(
                f"{name} actual capture/export/correctness order is not proven"
            )

        expected_policy = expected_policies[name]
        policy_values = {
            "state": queue_preactivation_policy(state),
            "terminal": queue_preactivation_policy(terminal),
        }
        for source, policy in policy_values.items():
            if policy != expected_policy:
                blockers.append(
                    f"{name} {source} queue preactivation policy is "
                    f"{policy or 'missing'}, expected {expected_policy}"
                )

        state_generation = str(state.get("engine_code_generation") or "")
        terminal_generation = str(terminal.get("engine_code_generation") or "")
        if not state_generation or state_generation != terminal_generation:
            blockers.append(
                f"{name} remote engine code generation differs or is missing"
            )

        raw_history = terminal.get("history", [])
        history = (
            [dict(item) for item in raw_history if isinstance(item, dict)]
            if isinstance(raw_history, list)
            else []
        )
        preactivation_history = [
            item for item in history if item.get("pre_activation") is True
        ]
        preactivation_started = parse_timestamp(
            str(state.get("pre_activation_started_at") or "")
        )
        activated = parse_timestamp(str(state.get("activated_at") or ""))
        if name == "baseline":
            if preactivation_started is not None or preactivation_history:
                blockers.append("baseline unexpectedly executed queue preactivation")
        else:
            if preactivation_started is None or activated is None:
                blockers.append(
                    "candidate queue preactivation/activation timestamps are missing"
                )
            elif preactivation_started >= activated:
                blockers.append(
                    "candidate queue preactivation did not start before activation"
                )
            if not preactivation_history:
                blockers.append("candidate has no executed preactivation stage history")
            for item in preactivation_history:
                if str(item.get("stage_resource") or "") != "host":
                    blockers.append(
                        "candidate preactivation history contains a non-host stage"
                    )
                if [lock for lock in item.get("stage_locks", []) if str(lock)]:
                    blockers.append(
                        "candidate preactivation history contains a shared-resource lock"
                    )
                stage_started = parse_timestamp(str(item.get("started_at") or ""))
                if stage_started is None or activated is None or stage_started >= activated:
                    blockers.append(
                        "candidate preactivation stage did not start before activation"
                    )

        records[name] = {
            "execution_profile": profile_values["capture"],
            "scheduler_policy": expected_policy,
            "observed_scheduler_policy": policy_values,
            "engine_code_generation": terminal_generation,
            "capture_mode": str(capture.get("capture_mode") or ""),
            "stage_sequence": stage_sequence,
            "measurement_contract_sha256": contract_digest,
            "measurement_pipeline_sha256": pipeline_digest,
            "measurement_input_sha256": input_digest,
            "pre_activation_started_at": str(
                state.get("pre_activation_started_at") or ""
            ),
            "activated_at": str(state.get("activated_at") or ""),
            "preactivation_stage_count": len(preactivation_history),
            "preactivation_stage_names": [
                str(item.get("stage_name") or "") for item in preactivation_history
            ],
            "identity": identity,
        }

    for field in (
        "source_sha256",
        "case_bundle_sha256",
        "golden_bundle_sha256",
        "test_contract_sha256",
        "correctness_case_count",
        "performance_case_count",
        "correctness_repetitions",
        "performance_samples_per_case",
        "environment_sha256",
    ):
        left = records.get("baseline", {}).get("identity", {}).get(field)
        right = records.get("candidate", {}).get("identity", {}).get(field)
        if left in {None, ""} or right in {None, ""}:
            blockers.append(f"scheduler-policy identity is missing {field}")
        elif left != right:
            blockers.append(
                f"baseline/candidate scheduler-policy identity {field} differs"
            )
    for field in (
        "measurement_contract_sha256",
        "measurement_pipeline_sha256",
    ):
        left = str(records.get("baseline", {}).get(field) or "")
        right = str(records.get("candidate", {}).get(field) or "")
        if not left or left != right:
            blockers.append(f"baseline/candidate {field} differs or is missing")
    generations = {
        str(record.get("engine_code_generation") or "")
        for record in records.values()
        if str(record.get("engine_code_generation") or "")
    }
    if len(generations) != 1:
        blockers.append("baseline/candidate remote engine code generation differs")
    return {
        "protocol_version": "engine-scheduler-policy-isolation-v1",
        "ok": not blockers,
        "blockers": unique(blockers),
        "engine_code_generation": (
            next(iter(generations)) if len(generations) == 1 else ""
        ),
        "baseline": records.get("baseline", {}),
        "candidate": records.get("candidate", {}),
    }


def read_engine_job_manifests(
    perf_path: Path, label: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    result_dir = perf_path.resolve().parent
    candidates = [result_dir.parent.parent, result_dir.parent, result_dir]
    for job_root in candidates:
        state_path = job_root / "state.json"
        terminal_path = job_root / "terminal.json"
        if state_path.is_file() and terminal_path.is_file():
            return (
                read_manifest(state_path, f"{label} engine state"),
                read_manifest(terminal_path, f"{label} engine terminal"),
            )
    raise EngineABError(
        f"cannot locate {label} engine state/terminal from: {perf_path}"
    )


def queue_preactivation_policy(value: dict[str, Any]) -> str:
    policy = value.get("scheduler_policy", {})
    return (
        str(policy.get("queue_preactivation") or "")
        if isinstance(policy, dict)
        else ""
    )


def parse_timestamp(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def ordered_stages(stage_sequence: list[str], required: list[str]) -> bool:
    try:
        indexes = [stage_sequence.index(name) for name in required]
    except ValueError:
        return False
    return indexes == sorted(indexes) and len(indexes) == len(set(indexes))


def read_phase_timeline(path: Path) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8-sig").splitlines()
    except OSError as exc:
        raise EngineABError(f"cannot read phase timeline: {path}: {exc}") from exc
    result: list[dict[str, Any]] = []
    for line in lines:
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError as exc:
            raise EngineABError(f"invalid phase timeline row: {path}: {exc}") from exc
        if isinstance(item, dict):
            result.append(item)
    return result


def actual_performance_first_order(timeline: list[dict[str, Any]]) -> bool:
    phases: dict[str, int] = {}
    for item in timeline:
        phase = str(item.get("phase") or "")
        try:
            epoch_ms = int(item.get("epoch_ms", 0) or 0)
        except (TypeError, ValueError):
            continue
        if phase and epoch_ms > 0:
            phases[phase] = epoch_ms
    required = [
        "performance_capture_end",
        "profile_export_end",
        "correctness_start",
        "profile_parse_start",
    ]
    if any(name not in phases for name in required):
        return False
    values = [phases[name] for name in required]
    return values == sorted(values) and len(values) == len(set(values))


def actual_scheduler_measurement_order(timeline: list[dict[str, Any]]) -> bool:
    phases: dict[str, int] = {}
    for item in timeline:
        phase = str(item.get("phase") or "")
        try:
            epoch_ms = int(item.get("epoch_ms", 0) or 0)
        except (TypeError, ValueError):
            continue
        if phase and epoch_ms > 0:
            phases[phase] = epoch_ms
    required = {
        "performance_capture_end",
        "profile_export_end",
        "correctness_start",
        "profile_parse_start",
    }
    if not required.issubset(phases):
        return False
    capture_end = phases["performance_capture_end"]
    export_end = phases["profile_export_end"]
    return (
        capture_end < export_end < phases["correctness_start"]
        and export_end < phases["profile_parse_start"]
    )


def actual_fused_measurement_order(timeline: list[dict[str, Any]]) -> bool:
    phases: dict[str, int] = {}
    for item in timeline:
        phase = str(item.get("phase") or "")
        try:
            epoch_ms = int(item.get("epoch_ms", 0) or 0)
        except (TypeError, ValueError):
            continue
        if phase and epoch_ms > 0:
            phases[phase] = epoch_ms
    required = [
        "performance_capture_end",
        "correctness_start",
        "correctness_end",
        "profile_export_start",
        "profile_export_end",
        "profile_parse_start",
    ]
    if any(name not in phases for name in required):
        return False
    values = [phases[name] for name in required]
    return values == sorted(values) and len(values) == len(set(values))


def unique(values: list[str]) -> list[str]:
    result: list[str] = []
    for value in values:
        if value and value not in result:
            result.append(value)
    return result


def case_map(
    manifest: dict[str, Any], name: str, blockers: list[str]
) -> dict[int, dict[str, Any]]:
    result: dict[int, dict[str, Any]] = {}
    raw_cases = manifest.get("cases", [])
    if not isinstance(raw_cases, list):
        blockers.append(f"{name} performance cases are invalid")
        return result
    for raw in raw_cases:
        if not isinstance(raw, dict):
            blockers.append(f"{name} contains an invalid case record")
            continue
        case = int(raw.get("case", 0) or 0)
        if case <= 0 or case in result:
            blockers.append(f"{name} contains an invalid or duplicate case id")
            continue
        for field in ("median_us", "p95_us", "stddev_us", "sample_count"):
            if raw.get(field) is None:
                blockers.append(f"{name} case{case} missing {field}")
        result[case] = raw
    return result


def compact_case(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "sample_count": int(raw.get("sample_count", 0) or 0),
        "median_us": number(raw.get("median_us")),
        "p95_us": number(raw.get("p95_us")),
        "stddev_us": number(raw.get("stddev_us")),
        "time_use_us": number(raw.get("time_use_us")),
    }


def shift_percent(baseline: object, candidate: object) -> float:
    left = number(baseline)
    right = number(candidate)
    if not math.isfinite(left) or not math.isfinite(right):
        return float("inf")
    if left == 0:
        return 0.0 if right == 0 else float("inf")
    return round((right - left) * 100.0 / left, 6)


def number(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


def display(value: float) -> str:
    return f"{value:.6g}" if math.isfinite(value) else "inf"


def read_manifest(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise EngineABError(f"cannot read {label}: {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise EngineABError(f"{label} must be a JSON object: {path}")
    return value


def write_report(report: dict[str, Any], output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "ENGINE_AB_REPORT.json"
    markdown_path = output_dir / "ENGINE_AB_REPORT.md"
    atomic_write_json(json_path, report)
    lines = [
        "# Engine A/B Measurement Gate",
        "",
        f"- Verdict: **{report['verdict']}**",
        f"- Generated: `{report['generated_at']}`",
        f"- Blockers: {len(report['blockers'])}",
        f"- Equivalence mode: `{report.get('equivalence_mode', STRICT_EQUIVALENCE_MODE)}`",
        f"- Statistical diagnostic: **{report.get('statistical_verdict', 'FAIL')}**",
        "",
    ]
    lines.extend(f"- {item}" for item in report["blockers"])
    lines.extend(
        [
            "",
            "| Case | Median shift | p95 shift | stddev shift | Samples equal |",
            "|---:|---:|---:|---:|---|",
        ]
    )
    for item in report["performance"]["cases"]:
        shifts = item["shift_percent"]
        lines.append(
            f"| {item['case']} | {display(shifts['median'])}% | "
            f"{display(shifts['p95'])}% | {display(shifts['stddev'])}% | "
            f"{item['sample_count_equal']} |"
        )
    lines.extend(
        [
            "",
            f"Weighted shift: `{display(report['performance']['weighted_shift_percent'])}%`",
            "",
        ]
    )
    scheduler = report.get("scheduler_policy_isolation", {})
    if isinstance(scheduler, dict) and scheduler:
        lines.extend(
            [
                "## Scheduler Policy Isolation",
                "",
                f"- Proven: `{scheduler.get('ok', False)}`",
                "- Remote engine generation: "
                f"`{scheduler.get('engine_code_generation', '-') or '-'}`",
                "- Baseline preactivation stages: "
                f"`{scheduler.get('baseline', {}).get('preactivation_stage_count', 0)}`",
                "- Candidate preactivation stages: "
                f"`{scheduler.get('candidate', {}).get('preactivation_stage_count', 0)}`",
                "",
            ]
        )
    markdown_path.write_text("\n".join(lines), encoding="utf-8")
    return json_path, markdown_path


def write_series_report(report: dict[str, Any], output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "ENGINE_AB_SERIES_REPORT.json"
    markdown_path = output_dir / "ENGINE_AB_SERIES_REPORT.md"
    atomic_write_json(json_path, report)
    lines = [
        "# Engine Alternating A/B Series Gate",
        "",
        f"- Verdict: **{report['verdict']}**",
        f"- Generated: `{report['generated_at']}`",
        f"- Pair count: {report['pair_count']} (minimum {report['minimum_pairs']})",
        f"- Orders: {', '.join(report['execution_orders']) or '-'}",
        f"- Equivalence mode: `{report.get('equivalence_mode', STRICT_EQUIVALENCE_MODE)}`",
        "- Remote engine generation: "
        f"`{report.get('remote_engine_code_generation', '-') or '-'}`",
        "",
        "## Blockers",
        "",
    ]
    blockers = [str(item) for item in report.get("blockers", []) if str(item)]
    lines.extend(f"- {item}" for item in blockers)
    if not blockers:
        lines.append("- none")
    lines.extend(
        [
            "",
            "| Pair | Order | Verdict | Weighted shift |",
            "|---|---|---|---:|",
        ]
    )
    for item in report.get("pairs", []):
        performance = item.get("performance", {}) if isinstance(item, dict) else {}
        lines.append(
            f"| {item.get('pair_id', '-')} | {item.get('execution_order', '-')} | "
            f"{item.get('verdict', '-')} | "
            f"{display(number(performance.get('weighted_shift_percent')))}% |"
        )
    aggregate = report.get("aggregate_weighted_shift_percent", {})
    lines.extend(
        [
            "",
            "Aggregate weighted median shift: "
            f"`{display(number(aggregate.get('median')))}%` "
            f"(limit `{display(number(aggregate.get('limit')))}%`, "
            f"ok `{aggregate.get('ok', False)}`)",
        ]
    )
    markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, markdown_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compare legacy/engine measurement manifests")
    parser.add_argument("--baseline-perf", type=Path, required=True)
    parser.add_argument("--candidate-perf", type=Path, required=True)
    parser.add_argument("--baseline-correctness", type=Path, required=True)
    parser.add_argument("--candidate-correctness", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--median-limit-percent", type=float, default=1.0)
    parser.add_argument("--p95-limit-percent", type=float, default=5.0)
    parser.add_argument("--stddev-limit-percent", type=float, default=10.0)
    parser.add_argument("--weighted-limit-percent", type=float, default=1.0)
    parser.add_argument(
        "--equivalence-mode",
        choices=[
            STRICT_EQUIVALENCE_MODE,
            CAUSAL_PERFORMANCE_FIRST_MODE,
            SCHEDULER_POLICY_MODE,
        ],
        default=STRICT_EQUIVALENCE_MODE,
    )
    parser.add_argument(
        "--expected-repetitions",
        type=int,
        help="optional fixed repetition count; omitted means infer and compare manifests",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        report = compare_ab(
            baseline_perf=args.baseline_perf,
            candidate_perf=args.candidate_perf,
            baseline_correctness=args.baseline_correctness,
            candidate_correctness=args.candidate_correctness,
            median_limit_percent=args.median_limit_percent,
            p95_limit_percent=args.p95_limit_percent,
            stddev_limit_percent=args.stddev_limit_percent,
            weighted_limit_percent=args.weighted_limit_percent,
            expected_repetitions=args.expected_repetitions,
            equivalence_mode=args.equivalence_mode,
        )
        json_path, markdown_path = write_report(report, args.output_dir)
    except EngineABError as exc:
        print(f"ENGINE_AB_ERROR: {exc}")
        return 2
    print(json.dumps({"verdict": report["verdict"], "json": str(json_path), "markdown": str(markdown_path)}, indent=2))
    return 0 if report["verdict"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
