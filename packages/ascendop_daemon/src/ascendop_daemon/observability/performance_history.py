from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ascendop_daemon.core.models import DaemonConfig


VERDICT_RE = re.compile(r"^Verdict:\s*(\S+)", re.IGNORECASE | re.MULTILINE)
CASE_RE = re.compile(r"^Case version:\s*(\S+)", re.IGNORECASE | re.MULTILINE)
WEIGHTED_RE = re.compile(r"weighted_time:\s*`?([0-9.]+)\s*us", re.IGNORECASE)
STARTED_RE = re.compile(r"^Started:\s*(\S+)", re.IGNORECASE | re.MULTILINE)
FINISHED_RE = re.compile(r"^Finished:\s*(\S+)", re.IGNORECASE | re.MULTILINE)
TITLE_VERSION_RE = re.compile(r"^# Result\s+\S+\s+(\S+)", re.MULTILINE)
VERSION_RE = re.compile(r"_V(?P<major>\d+)(?:_(?P<minor>\d+))?$")


def build_performance_history(root: Path, config: DaemonConfig, history_limit: int = 40) -> dict[str, Any]:
    operators: dict[str, Any] = {}
    for op in config.operators:
        all_tests = all_test_results(root, op)
        tests = all_tests[-max(1, history_limit):]
        releases = release_results(root, op)
        latest_pass = latest_matching(tests, verdict="PASS")
        active_release_name = read_text(root / "operators_finish" / op / "active_release.txt").strip()
        active_release = next((item for item in releases if item.get("release_version") == active_release_name), None)
        same_case_release = latest_same_case_release(releases, latest_pass)
        best_same_case = best_pass_same_case(all_tests, latest_pass)
        operators[op] = {
            "test_result_count": count_result_files(root, op),
            "recent_tests": tests,
            "release_count": len(releases),
            "active_release": active_release,
            "active_release_name": active_release_name,
            "releases": releases,
            "latest_pass": latest_pass,
            "best_recent_same_case": best_same_case,
            "best_same_case": best_same_case,
            "case_bests": case_best_results(all_tests),
            "latest_same_case_release": same_case_release,
            "failure_streak": failure_streak(tests),
            "latest_vs_active_release_pct": percent_delta(latest_pass, active_release),
            "latest_vs_same_case_release_pct": percent_delta(latest_pass, same_case_release),
            "latest_vs_best_recent_same_case_pct": percent_delta(latest_pass, best_same_case),
        }
    return {
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "history_limit": history_limit,
        "operators": operators,
    }


def write_performance_history_files(root: Path, snapshot: dict[str, Any]) -> None:
    state_dir = root / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "perf_history.json").write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    (state_dir / "PERF_HISTORY.md").write_text(render_performance_history(snapshot), encoding="utf-8")


def recent_test_results(root: Path, op: str, limit: int) -> list[dict[str, Any]]:
    return all_test_results(root, op)[-max(1, limit):]


def all_test_results(root: Path, op: str) -> list[dict[str, Any]]:
    result_root = root / "operators_testresult" / op
    results = [parse_result(path, root, op, source="test") for path in result_root.glob("*/RESULT.md")]
    results = [item for item in results if item]
    results.sort(key=lambda item: (int(item.get("major", -1)), int(item.get("minor", -1)), item.get("mtime_utc", "")))
    return results


def case_best_results(items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    best: dict[str, dict[str, Any]] = {}
    for item in items:
        case_version = str(item.get("case_version", "") or "")
        weighted = item.get("weighted_time_us")
        if not case_version or item.get("verdict") != "PASS" or weighted is None:
            continue
        existing = best.get(case_version)
        if existing is None or float(weighted) < float(existing.get("weighted_time_us") or float("inf")):
            best[case_version] = item
    return best


def release_results(root: Path, op: str) -> list[dict[str, Any]]:
    release_root = root / "operators_finish" / op
    releases: list[dict[str, Any]] = []
    if not release_root.exists():
        return releases
    for release_dir in release_root.iterdir():
        if not release_dir.is_dir():
            continue
        path = release_dir / "testresult_snapshot" / "RESULT.md"
        if not path.exists():
            continue
        item = parse_result(path, root, op, source="release", release_version=release_dir.name)
        if item:
            releases.append(item)
    releases.sort(key=lambda item: (int(item.get("release_major", -1)), item.get("finished", ""), item.get("mtime_utc", "")))
    return releases


def parse_result(
    path: Path,
    root: Path,
    op: str,
    *,
    source: str,
    release_version: str = "",
) -> dict[str, Any]:
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
        stat = path.stat()
    except OSError:
        return {}
    title = TITLE_VERSION_RE.search(text)
    version = title.group(1) if title else path.parent.name
    major, minor = parse_version_numbers(version)
    release_major, _ = parse_version_numbers(release_version) if release_version else (-1, -1)
    started = match_text(STARTED_RE, text)
    finished = match_text(FINISHED_RE, text)
    weighted = match_text(WEIGHTED_RE, text)
    return {
        "op": op,
        "source": source,
        "test_version": version,
        "release_version": release_version,
        "major": major,
        "minor": minor,
        "release_major": release_major,
        "verdict": match_text(VERDICT_RE, text),
        "case_version": match_text(CASE_RE, text),
        "weighted_time_us": float(weighted) if weighted else None,
        "started": started,
        "finished": finished,
        "duration_seconds": duration_seconds(started, finished),
        "path": str(path.relative_to(root)),
        "mtime_utc": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(timespec="seconds"),
    }


def parse_version_numbers(version: str) -> tuple[int, int]:
    match = VERSION_RE.search(version)
    if not match:
        return (-1, -1)
    minor = match.group("minor")
    return int(match.group("major")), int(minor) if minor is not None else -1


def match_text(pattern: re.Pattern[str], text: str) -> str:
    match = pattern.search(text)
    return match.group(1) if match else ""


def duration_seconds(started: str, finished: str) -> int | None:
    start = parse_timestamp(started)
    end = parse_timestamp(finished)
    if start is None or end is None:
        return None
    return max(0, int((end - start).total_seconds()))


def parse_timestamp(text: str) -> datetime | None:
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def latest_matching(items: list[dict[str, Any]], *, verdict: str) -> dict[str, Any] | None:
    for item in reversed(items):
        if item.get("verdict") == verdict:
            return item
    return None


def latest_same_case_release(releases: list[dict[str, Any]], latest_pass: dict[str, Any] | None) -> dict[str, Any] | None:
    if not latest_pass:
        return None
    case_version = latest_pass.get("case_version")
    for item in reversed(releases):
        if item.get("case_version") == case_version and item.get("verdict") == "PASS":
            return item
    return None


def best_pass_same_case(items: list[dict[str, Any]], latest_pass: dict[str, Any] | None) -> dict[str, Any] | None:
    if not latest_pass:
        return None
    case_version = latest_pass.get("case_version")
    candidates = [
        item
        for item in items
        if item.get("case_version") == case_version
        and item.get("verdict") == "PASS"
        and item.get("weighted_time_us") is not None
    ]
    return min(candidates, key=lambda item: float(item.get("weighted_time_us") or 0.0)) if candidates else None


def failure_streak(items: list[dict[str, Any]]) -> int:
    streak = 0
    for item in reversed(items):
        if item.get("verdict") == "PASS":
            break
        streak += 1
    return streak


def percent_delta(current: dict[str, Any] | None, baseline: dict[str, Any] | None) -> float | None:
    if not current or not baseline:
        return None
    current_weighted = current.get("weighted_time_us")
    baseline_weighted = baseline.get("weighted_time_us")
    if current_weighted is None or baseline_weighted in (None, 0):
        return None
    return round((float(current_weighted) - float(baseline_weighted)) / float(baseline_weighted) * 100.0, 2)


def count_result_files(root: Path, op: str) -> int:
    return sum(1 for _ in (root / "operators_testresult" / op).glob("*/RESULT.md"))


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def render_performance_history(snapshot: dict[str, Any]) -> str:
    lines = [
        "# Tester Daemon Performance History",
        "",
        f"- updated_at: {snapshot.get('updated_at', '')}",
        f"- history_limit: {snapshot.get('history_limit', '')}",
        "",
    ]
    operators = snapshot.get("operators", {})
    if not isinstance(operators, dict):
        return "\n".join(lines)
    for op, data in operators.items():
        if not isinstance(data, dict):
            continue
        latest = data.get("latest_pass") if isinstance(data.get("latest_pass"), dict) else {}
        active = data.get("active_release") if isinstance(data.get("active_release"), dict) else {}
        best = data.get("best_recent_same_case") if isinstance(data.get("best_recent_same_case"), dict) else {}
        lines.extend(
            [
                f"## {op}",
                "",
                f"- test_result_count: {data.get('test_result_count', 0)}",
                f"- release_count: {data.get('release_count', 0)}",
                f"- active_release: {data.get('active_release_name', '') or '-'} "
                f"{format_weight(active)} case={active.get('case_version', '-') if active else '-'}",
                f"- latest_pass: {latest.get('test_version', '-') if latest else '-'} "
                f"{format_weight(latest)} case={latest.get('case_version', '-') if latest else '-'}",
                f"- best_recent_same_case: {best.get('test_version', '-') if best else '-'} {format_weight(best)}",
                f"- failure_streak: {data.get('failure_streak', 0)}",
                f"- latest_vs_active_release_pct: {display_delta(data.get('latest_vs_active_release_pct'))}",
                f"- latest_vs_same_case_release_pct: {display_delta(data.get('latest_vs_same_case_release_pct'))}",
                f"- latest_vs_best_recent_same_case_pct: {display_delta(data.get('latest_vs_best_recent_same_case_pct'))}",
                "",
                "### Recent Tests",
                "",
                "| version | verdict | case | weighted_us | finished | duration_s |",
                "|---|---|---|---:|---|---:|",
            ]
        )
        tests = data.get("recent_tests", [])
        if isinstance(tests, list):
            for item in tests[-20:]:
                if not isinstance(item, dict):
                    continue
                lines.append(
                    f"| {item.get('test_version', '-')} | {item.get('verdict', '-')} | "
                    f"{item.get('case_version', '-')} | {display_weight(item.get('weighted_time_us'))} | "
                    f"{item.get('finished', '-') or '-'} | {item.get('duration_seconds', '-') if item.get('duration_seconds') is not None else '-'} |"
                )
        lines.extend(["", "### Releases", "", "| release | source_version | case | weighted_us | finished |", "|---|---|---|---:|---|"])
        releases = data.get("releases", [])
        if isinstance(releases, list):
            for item in releases:
                if not isinstance(item, dict):
                    continue
                lines.append(
                    f"| {item.get('release_version', '-')} | {item.get('test_version', '-')} | "
                    f"{item.get('case_version', '-')} | {display_weight(item.get('weighted_time_us'))} | "
                    f"{item.get('finished', '-') or '-'} |"
                )
        lines.append("")
    return "\n".join(lines)


def format_weight(item: dict[str, Any] | None) -> str:
    if not item:
        return "weighted_us=-"
    return f"weighted_us={display_weight(item.get('weighted_time_us'))}"


def display_weight(value: object) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value):.3f}"
    except (TypeError, ValueError):
        return str(value)


def display_delta(value: object) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value):+.2f}%"
    except (TypeError, ValueError):
        return str(value)
