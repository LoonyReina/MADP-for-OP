from __future__ import annotations

import json
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCALABLE_PROFILE = "engine-v2-staged-scalable"
IDENTITY_FIELDS = (
    "test_version",
    "source_sha256",
    "case_bundle_sha256",
    "golden_bundle_sha256",
    "test_contract_sha256",
    "environment_sha256",
)


def evaluate_profile_stability(
    *,
    root: Path,
    pump_state: dict[str, Any],
    job_ids: list[str],
    minimum_runs: int = 5,
    weighted_limit_percent: float = 1.5,
    weighted_cv_limit_percent: float = 1.0,
    per_case_limit_percent: float = 2.0,
    expected_profile: str = SCALABLE_PROFILE,
) -> dict[str, Any]:
    root = root.resolve()
    blockers: list[str] = []
    selected = [str(item) for item in job_ids if str(item)]
    if len(selected) < minimum_runs:
        blockers.append(
            f"stability requires at least {minimum_runs} runs; observed={len(selected)}"
        )
    if len(selected) != len(set(selected)):
        blockers.append("stability job ids must be unique")
    entries = object_value(pump_state.get("entries"))
    runs: list[dict[str, Any]] = []
    signatures: list[dict[str, str]] = []
    cache_keys: list[str] = []
    remote_generations: set[str] = set()
    weighted_values: list[float] = []
    case_values: dict[int, list[float]] = {}

    for sequence, job_id in enumerate(selected):
        raw_entry = entries.get(job_id)
        if not isinstance(raw_entry, dict):
            blockers.append(f"{job_id}: pump entry is missing")
            continue
        entry = dict(raw_entry)
        result_root = resolve_result_root(root, entry)
        if result_root is None:
            blockers.append(f"{job_id}: required result bundle is missing")
            continue
        perf = read_object(result_root / "PERF_PARSE.json")
        capture = read_object(result_root / "PERF_CAPTURE.json")
        perf_batch = read_object(result_root / "PERF_BATCH.json")
        correctness = read_object(result_root / "CORRECTNESS.json")
        correctness_batch = read_object(result_root / "CORRECTNESS_BATCH.json")
        cache = read_object(result_root / "CASE_CACHE.json")
        identity = read_object(result_root / "ENGINE_IDENTITY.json")
        run_blockers: list[str] = []

        profile = str(perf.get("execution_profile") or entry.get("execution_profile") or "")
        if profile != expected_profile:
            run_blockers.append(
                f"execution profile mismatch: expected={expected_profile} observed={profile or '-'}"
            )
        if entry.get("workflow_ingest") is not False:
            run_blockers.append("stability run is not an isolated no-ingest canary")
        if str(perf.get("state") or "") != "parsed":
            run_blockers.append("PERF_PARSE is not parsed")
        if str(capture.get("capture_mode") or "") != "batched-process":
            run_blockers.append("performance capture is not one-Python multi-case")
        if str(perf_batch.get("state") or "") != "passed":
            run_blockers.append("PERF_BATCH is not passed")
        if int(perf_batch.get("repetitions", 0) or 0) != 1:
            run_blockers.append("PERF_BATCH does not execute one outer pass")
        if str(correctness.get("state") or "") != "passed":
            run_blockers.append("CORRECTNESS is not passed")
        if str(correctness.get("execution_mode") or "") != "batched-process":
            run_blockers.append("correctness is not one-Python multi-case")
        if int(correctness.get("repetitions", 0) or 0) != 1:
            run_blockers.append("correctness outer repetitions is not one")
        if str(correctness_batch.get("state") or "") != "passed":
            run_blockers.append("CORRECTNESS_BATCH is not passed")
        if str(cache.get("protocol_version") or "") != "engine-case-cache-v1":
            run_blockers.append("case/golden cache protocol is missing")
        if str(cache.get("state") or "") != "ready":
            run_blockers.append("case/golden cache is not ready")
        cache_key = str(cache.get("cache_key") or "")
        if not cache_key:
            run_blockers.append("case/golden cache key is missing")
        elif sequence > 0 and cache.get("cache_hit") is not True:
            run_blockers.append("repeated fixed job did not reuse case/golden cache")

        case_ids = [int(item) for item in perf.get("case_ids", [])]
        expected_rows = int(perf.get("expected_task_rows_per_case", 0) or 0)
        raw_cases = perf.get("cases", [])
        parsed_cases = {
            int(item.get("case", 0) or 0): dict(item)
            for item in raw_cases
            if isinstance(item, dict)
        }
        if not case_ids or list(parsed_cases) != case_ids:
            run_blockers.append("parsed performance case order is incomplete")
        if expected_rows != 50:
            run_blockers.append(
                f"performance rows per case must remain 50; observed={expected_rows}"
            )
        for case in case_ids:
            item = parsed_cases.get(case, {})
            if int(item.get("sample_count", 0) or 0) != expected_rows:
                run_blockers.append(
                    f"case{case} profiler row count differs from {expected_rows}"
                )
        correctness_count = int(correctness.get("case_count", 0) or 0)
        if int(correctness.get("execution_count", 0) or 0) != correctness_count:
            run_blockers.append("correctness execution grid is not case_count x 1")
        if [int(item) for item in correctness.get("case_ids", [])] != case_ids:
            run_blockers.append("correctness/performance case order differs")

        signature = {
            field: str(identity.get(field) or "") for field in IDENTITY_FIELDS
        }
        missing_identity = [field for field, value in signature.items() if not value]
        if missing_identity:
            run_blockers.append(
                "runtime identity is missing " + ", ".join(missing_identity)
            )
        remote_generation = str(entry.get("engine_code_generation") or "")
        if not remote_generation:
            remote_generation = str(
                object_value(entry.get("engine_terminal_manifest")).get(
                    "engine_code_generation"
                )
                or ""
            )
        if not remote_generation:
            bundle_root = result_root.parent.parent
            remote_generation = str(
                read_object(bundle_root / "terminal.json").get(
                    "engine_code_generation"
                )
                or read_object(bundle_root / "state.json").get(
                    "engine_code_generation"
                )
                or ""
            )
        if not remote_generation:
            run_blockers.append("remote engine code generation is missing")
        else:
            remote_generations.add(remote_generation)

        weighted = float(perf.get("weighted_time", 0.0) or 0.0)
        if weighted <= 0:
            run_blockers.append("weighted performance is missing or non-positive")
        else:
            weighted_values.append(weighted)
        for case in case_ids:
            value = float(parsed_cases.get(case, {}).get("time_use_us", 0.0) or 0.0)
            if value <= 0:
                run_blockers.append(f"case{case} measured time is missing")
            else:
                case_values.setdefault(case, []).append(value)

        blockers.extend(f"{job_id}: {item}" for item in run_blockers)
        signatures.append(signature)
        cache_keys.append(cache_key)
        runs.append(
            {
                "engine_job_id": job_id,
                "result_root": relative_path(result_root, root),
                "execution_profile": profile,
                "case_ids": case_ids,
                "correctness_repetitions": int(
                    correctness.get("repetitions", 0) or 0
                ),
                "performance_rows_per_case": expected_rows,
                "weighted_time_us": weighted,
                "case_times_us": {
                    f"case{case}": float(
                        parsed_cases.get(case, {}).get("time_use_us", 0.0) or 0.0
                    )
                    for case in case_ids
                },
                "cache_key": cache_key,
                "cache_hit": cache.get("cache_hit") is True,
                "cache_population_seconds": float(
                    object_value(cache.get("timing_seconds")).get("population", 0.0)
                    or 0.0
                ),
                "remote_engine_code_generation": remote_generation,
                "blockers": run_blockers,
            }
        )

    if signatures and any(item != signatures[0] for item in signatures[1:]):
        blockers.append("fixed stability runs do not share one runtime identity")
    nonempty_cache_keys = [item for item in cache_keys if item]
    if nonempty_cache_keys and len(set(nonempty_cache_keys)) != 1:
        blockers.append("fixed stability runs do not share one case/golden cache key")
    if len(remote_generations) != 1:
        blockers.append("stability runs do not share one remote engine generation")

    weighted_stats = variation_stats(weighted_values)
    if len(weighted_values) == len(selected) and weighted_stats[
        "max_deviation_percent"
    ] > weighted_limit_percent:
        blockers.append(
            "weighted cross-run deviation exceeds limit: "
            f"{weighted_stats['max_deviation_percent']:.6f}% > "
            f"{weighted_limit_percent:.6f}%"
        )
    if (
        len(weighted_values) == len(selected)
        and weighted_stats["cv_percent"] > weighted_cv_limit_percent
    ):
        blockers.append(
            "weighted cross-run CV exceeds limit: "
            f"{weighted_stats['cv_percent']:.6f}% > "
            f"{weighted_cv_limit_percent:.6f}%"
        )
    per_case_stats: dict[str, dict[str, float]] = {}
    for case, values in sorted(case_values.items()):
        stats = variation_stats(values)
        per_case_stats[f"case{case}"] = stats
        if len(values) == len(selected) and stats[
            "max_deviation_percent"
        ] > per_case_limit_percent:
            blockers.append(
                f"case{case} cross-run deviation exceeds limit: "
                f"{stats['max_deviation_percent']:.6f}% > "
                f"{per_case_limit_percent:.6f}%"
            )

    return {
        "protocol_version": "engine-profile-stability-v2",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "verdict": "PASS" if not blockers else "FAIL",
        "expected_profile": expected_profile,
        "minimum_runs": minimum_runs,
        "run_count": len(runs),
        "job_ids": selected,
        "limits_percent": {
            "weighted_max_deviation": weighted_limit_percent,
            "weighted_cv": weighted_cv_limit_percent,
            "per_case_max_deviation": per_case_limit_percent,
        },
        "weighted_stats": weighted_stats,
        "per_case_stats": per_case_stats,
        "cache_key": (
            nonempty_cache_keys[0]
            if len(set(nonempty_cache_keys)) == 1 and nonempty_cache_keys
            else ""
        ),
        "remote_engine_code_generation": (
            next(iter(remote_generations)) if len(remote_generations) == 1 else ""
        ),
        "runs": runs,
        "blockers": blockers,
    }


def variation_stats(values: list[float]) -> dict[str, float]:
    if not values:
        return {
            "count": 0,
            "min": 0.0,
            "median": 0.0,
            "max": 0.0,
            "mean": 0.0,
            "cv_percent": 0.0,
            "max_deviation_percent": 0.0,
            "range_percent": 0.0,
        }
    median = statistics.median(values)
    mean = statistics.fmean(values)
    return {
        "count": len(values),
        "min": min(values),
        "median": median,
        "max": max(values),
        "mean": mean,
        "cv_percent": (
            statistics.pstdev(values) / mean * 100.0
            if len(values) > 1 and mean
            else 0.0
        ),
        "max_deviation_percent": (
            max(abs(item - median) for item in values) / median * 100.0
            if median
            else 0.0
        ),
        "range_percent": ((max(values) - min(values)) / median * 100.0 if median else 0.0),
    }


def resolve_result_root(root: Path, entry: dict[str, Any]) -> Path | None:
    for field in (
        "snapshot_bundle_root",
        "required_bundle_root",
        "optional_bundle_root",
    ):
        raw = str(entry.get(field) or "").strip()
        if not raw:
            continue
        path = Path(raw)
        if not path.is_absolute():
            path = root / path
        path = path.resolve()
        if path != root and root not in path.parents:
            continue
        for candidate in (path / "result_bundle" / "result", path / "result", path):
            if (candidate / "PERF_PARSE.json").is_file():
                return candidate
    return None


def read_object(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    return raw if isinstance(raw, dict) else {}


def object_value(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def relative_path(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root)).replace("\\", "/")
    except ValueError:
        return str(path)


def write_stability_report(
    report: dict[str, Any], output_dir: Path
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "ENGINE_PROFILE_STABILITY.json"
    markdown_path = output_dir / "ENGINE_PROFILE_STABILITY.md"
    json_path.write_text(
        json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    weighted = object_value(report.get("weighted_stats"))
    limits = object_value(report.get("limits_percent"))
    lines = [
        "# Engine Profile Stability",
        "",
        f"- Verdict: `{report.get('verdict', 'FAIL')}`",
        f"- Profile: `{report.get('expected_profile', '')}`",
        f"- Runs: `{report.get('run_count', 0)}`",
        f"- Weighted median: `{float(weighted.get('median', 0.0) or 0.0):.6f} us`",
        "- Weighted max deviation: "
        f"`{float(weighted.get('max_deviation_percent', 0.0) or 0.0):.6f}%` "
        f"(limit `{float(limits.get('weighted_max_deviation', 0.0) or 0.0):.6f}%`)",
        "- Weighted CV: "
        f"`{float(weighted.get('cv_percent', 0.0) or 0.0):.6f}%` "
        f"(limit `{float(limits.get('weighted_cv', 0.0) or 0.0):.6f}%`)",
        f"- Cache key: `{report.get('cache_key', '')}`",
        "",
        "## Per Case",
        "",
    ]
    for case, stats in object_value(report.get("per_case_stats")).items():
        lines.append(
            f"- {case}: median `{float(stats.get('median', 0.0) or 0.0):.6f} us`, "
            f"max deviation `{float(stats.get('max_deviation_percent', 0.0) or 0.0):.6f}%`"
        )
    lines.extend(["", "## Blockers", ""])
    blockers = report.get("blockers", [])
    if blockers:
        lines.extend(f"- {item}" for item in blockers)
    else:
        lines.append("- none")
    markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, markdown_path
