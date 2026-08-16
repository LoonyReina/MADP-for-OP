from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ascendop_daemon.core.models import DaemonConfig
from ascendop_daemon.observability.performance_history import all_test_results, case_best_results


REVIEW_FILE = "SKILL_APPLICATION_REVIEW.md"
REVIEW_FIELDS = {
    "self_verdict": re.compile(
        r"^\s*(?:[-*]\s*)?(?:self[\s_-]+verdict)\s*:\s*(.+)$",
        re.IGNORECASE | re.MULTILINE,
    ),
    "method_used": re.compile(
        r"^\s*(?:[-*]\s*)?(?:method[\s_-]+used)\s*:\s*(.+)$",
        re.IGNORECASE | re.MULTILINE,
    ),
    "result_evidence": re.compile(
        r"^\s*(?:[-*]\s*)?(?:result[\s_-]+evidence)\s*:\s*(.+)$",
        re.IGNORECASE | re.MULTILINE,
    ),
    "source_result_fit": re.compile(
        r"^\s*(?:[-*]\s*)?(?:source(?:/|[\s_-]+)result[\s_-]+fit)\s*:\s*(.+)$",
        re.IGNORECASE | re.MULTILINE,
    ),
    "next_action": re.compile(
        r"^\s*(?:[-*]\s*)?(?:next[\s_-]+action)\s*:\s*(.+)$",
        re.IGNORECASE | re.MULTILINE,
    ),
    "suggestion_board": re.compile(
        r"^\s*(?:[-*]\s*)?(?:suggestion[\s_-]+board)\s*:\s*(.+)$",
        re.IGNORECASE | re.MULTILINE,
    ),
}
KNOWLEDGE_DECISION_RE = re.compile(
    r"^\s*(?:[-*]\s*)?(?:shared[\s_-]+knowledge[\s_-]+decision)\s*:\s*(.+)$",
    re.IGNORECASE | re.MULTILINE,
)
VALID_VERDICTS = {"useful", "inconclusive", "misapplied", "not-applicable"}
BASE_RE = re.compile(r"^Base version:\s*(.+)$", re.IGNORECASE | re.MULTILINE)
BOARD_RESEARCH_FIELDS = (
    "Entry kind",
    "Existing skill research",
    "Gap or reusable lesson",
    "Proposed target",
    "Evidence/falsifier",
)
ROUTE_REASONING_FIELDS = (
    "Observed signal",
    "Primary hypothesis",
    "Counter-hypothesis",
    "Router gap",
)


def build_iteration_quality(root: Path, config: DaemonConfig) -> dict[str, Any]:
    operators: dict[str, Any] = {}
    for op in config.operators:
        tests = all_test_results(root, op)
        best_by_case = case_best_results(tests)
        latest = tests[-1] if tests else {}
        recent = [reviewed_result(root, item, best_by_case) for item in tests[-20:]]
        operators[op] = {
            "result_count": len(tests),
            "latest": reviewed_result(root, latest, best_by_case) if latest else {},
            "case_bests": {
                case: compact_result(root, item)
                for case, item in sorted(best_by_case.items())
            },
            "recent": recent,
            "review_counts": count_reviews(recent),
            "route_reasoning_counts": count_route_reasoning(recent),
        }
    suggestion_board = build_suggestion_board_quality(root, config)
    suggestion_board.update(suggestion_review_engagement(operators))
    return {
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "review_contract": {
            "owner": "solver",
            "file": REVIEW_FILE,
            "required_fields": list(REVIEW_FIELDS),
            "verdicts": sorted(VALID_VERDICTS),
            "daemon_role": "fact-check and aggregate; no semantic self-scoring override",
        },
        "suggestion_board": suggestion_board,
        "operators": operators,
    }


def suggestion_review_engagement(operators: dict[str, Any]) -> dict[str, int]:
    counts = {"linked": 0, "none": 0, "missing": 0, "incomplete": 0}
    for operator in operators.values():
        if not isinstance(operator, dict):
            continue
        for result in operator.get("recent", []):
            if not isinstance(result, dict) or not result.get("review_path"):
                continue
            status = str(result.get("suggestion_board_status", "") or "")
            if status == "linked":
                counts["linked"] += 1
            elif status == "none":
                counts["none"] += 1
            elif status in {"incomplete-entry", "missing-entry"}:
                counts["incomplete"] += 1
            else:
                counts["missing"] += 1
    return {
        "recent_review_linked_count": counts["linked"],
        "recent_review_none_count": counts["none"],
        "recent_review_missing_count": counts["missing"],
        "recent_review_incomplete_count": counts["incomplete"],
    }


def build_suggestion_board_quality(root: Path, config: DaemonConfig) -> dict[str, Any]:
    board = read_text(root / "docs" / "next" / "skill_discussion_board.md")
    entries = discussion_board_entries(board)
    active_ops = {
        op
        for op in config.operators
        if not config.operator_sessions.get(op)
        or config.operator_sessions[op].enabled
    }
    known_ops = set(config.operators) | set(config.operator_sessions)
    scoped: list[tuple[str, str]] = []
    excluded = 0
    for title, body in entries:
        op = discussion_heading_operator(title)
        # Scope only tokens that are actually configured operator names. A
        # generic heading such as "CANN example ..." may start with an
        # uppercase token without being an inactive operator entry.
        if op in known_ops and op not in active_ops:
            excluded += 1
            continue
        scoped.append((title, body))

    structured: list[tuple[str, str]] = []
    malformed_open: list[dict[str, str]] = []
    request_count = 0
    contribution_count = 0
    researched_count = 0
    open_count = 0
    for title, body in scoped:
        triage = inline_board_field(body, "Triage").lower()
        is_open = not triage or triage == "open"
        if is_open:
            open_count += 1
        issue = discussion_board_entry_issue(body)
        if issue is None:
            structured.append((title, body))
            kind = inline_board_field(body, "Entry kind").lower()
            request_count += int(kind == "request")
            contribution_count += int(kind == "contribution")
            researched_count += int(bool(inline_board_field(body, "Existing skill research")))
        elif is_open:
            malformed_open.append({"title": title, "issue": issue})
    return {
        "entry_count": len(scoped),
        "excluded_inactive_count": excluded,
        "open_count": open_count,
        "structured_count": len(structured),
        "request_count": request_count,
        "contribution_count": contribution_count,
        "researched_count": researched_count,
        "malformed_open_count": len(malformed_open),
        "malformed_open": malformed_open,
        "status": "complete" if not malformed_open else "incomplete",
    }


def discussion_board_entries(board: str) -> list[tuple[str, str]]:
    entries: list[tuple[str, str]] = []
    title = ""
    lines: list[str] = []
    in_fence = False
    ignored = {"Ownership", "Entry Template", "Inbox"}
    for line in board.splitlines():
        if line.startswith("```"):
            in_fence = not in_fence
            if title:
                lines.append(line)
            continue
        if not in_fence and line.startswith("## "):
            if title:
                entries.append((title, "\n".join(lines).strip()))
            candidate = line[3:].strip()
            title = "" if candidate in ignored else candidate
            lines = []
        elif title:
            lines.append(line)
    if title:
        entries.append((title, "\n".join(lines).strip()))
    return entries


def discussion_heading_operator(title: str) -> str:
    match = re.match(r"^\d{4}-\d{2}-\d{2}\s+-\s+([^/\s]+)", title.strip())
    if not match:
        return ""
    token = match.group(1)
    return token if token[:1].isupper() else ""


def inline_board_field(entry: str, field: str) -> str:
    match = re.search(
        rf"^\s*[-*]\s*{re.escape(field)}\s*:\s*(.+?)\s*$",
        entry,
        flags=re.IGNORECASE | re.MULTILINE,
    )
    return " ".join(match.group(1).strip().split()) if match else ""


def reviewed_result(
    root: Path,
    result: dict[str, Any],
    best_by_case: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if not result:
        return {}
    op = str(result.get("op", "") or "")
    version = str(result.get("test_version", "") or "")
    result_dir = root / "operators_testresult" / op / version
    review_path = result_dir / REVIEW_FILE
    review = parse_review(review_path)
    suggestion_status = suggestion_board_status(root, review)
    review_evidence = review_evidence_depth(review, suggestion_status)
    case_version = str(result.get("case_version", "") or "")
    best = best_by_case.get(case_version, {})
    weighted = result.get("weighted_time_us")
    best_weighted = best.get("weighted_time_us")
    regression_pct = None
    if weighted is not None and best_weighted not in (None, 0):
        regression_pct = round(
            (float(weighted) - float(best_weighted)) / float(best_weighted) * 100.0,
            2,
        )
    version_path = result_dir / "submit_snapshot" / "pending_snapshot" / "VERSION.md"
    version_text = read_text(version_path)
    route_reasoning = route_reasoning_quality(version_text)
    base_match = BASE_RE.search(version_text)
    lineage_path = result_dir / "submit_snapshot" / "pending_snapshot" / "SOURCE_LINEAGE.json"
    lineage = read_json(lineage_path)
    return {
        **compact_result(root, result),
        "best_same_case_version": best.get("test_version", ""),
        "best_same_case_weighted_us": best_weighted,
        "regression_vs_best_pct": regression_pct,
        "is_best_same_case": bool(best and best.get("test_version") == version),
        "base_version": base_match.group(1).strip() if base_match else "",
        "lineage_path": relative(lineage_path, root) if lineage_path.exists() else "",
        "lineage_resolved": bool(lineage.get("parent", {}).get("resolved")) if lineage else False,
        "review_path": relative(review_path, root) if review_path.exists() else "",
        "review_status": review_status(review, suggestion_status),
        "suggestion_board_status": suggestion_status,
        "review_evidence": review_evidence,
        "review": review,
        "route_reasoning": route_reasoning,
    }


def parse_review(path: Path) -> dict[str, str]:
    text = read_text(path)
    if not text:
        return {}
    parsed: dict[str, str] = {}
    for field, pattern in REVIEW_FIELDS.items():
        match = pattern.search(text)
        if match:
            parsed[field] = " ".join(match.group(1).strip().split())
    knowledge_match = KNOWLEDGE_DECISION_RE.search(text)
    if knowledge_match:
        parsed["shared_knowledge_decision"] = " ".join(
            knowledge_match.group(1).strip().split()
        )
    return parsed


def review_status(review: dict[str, str], suggestion_status: str = "none") -> str:
    if not review:
        return "missing"
    present = sum(1 for field in REVIEW_FIELDS if review.get(field))
    verdict = review.get("self_verdict", "").lower()
    if (
        present == len(REVIEW_FIELDS)
        and verdict in VALID_VERDICTS
        and suggestion_status in {"none", "linked"}
    ):
        return "complete"
    return "partial"


def review_evidence_depth(
    review: dict[str, str],
    suggestion_status: str,
) -> dict[str, Any]:
    """Fact-check visible analysis evidence without overriding solver semantics."""
    checks = {
        "valid_self_verdict": review.get("self_verdict", "").lower() in VALID_VERDICTS,
        "method_identified": len(review.get("method_used", "").split()) >= 3,
        "quantified_result": bool(re.search(r"\d", review.get("result_evidence", ""))),
        "source_result_link": len(review.get("source_result_fit", "").split()) >= 8,
        "actionable_next_step": len(review.get("next_action", "").split()) >= 6,
        "suggestion_triaged": suggestion_status in {"none", "linked"},
        "shared_knowledge_triaged": len(review.get("shared_knowledge_decision", "")) >= 12,
    }
    score = sum(1 for value in checks.values() if value)
    return {
        "score": score,
        "max_score": len(checks),
        "checks": checks,
    }


def suggestion_board_status(root: Path, review: dict[str, str]) -> str:
    suggestion = str(review.get("suggestion_board", "") or "").strip()
    if not suggestion:
        return "missing-field"
    if suggestion.lower() == "none":
        return "none"
    board = read_text(root / "docs" / "next" / "skill_discussion_board.md")
    normalized = suggestion.removeprefix("##").strip().lower()
    entry = discussion_board_entry(board, normalized)
    if entry is None:
        return "missing-entry"
    return "linked" if not discussion_board_entry_issue(entry) else "incomplete-entry"


def discussion_board_entry(board: str, normalized_heading: str) -> str | None:
    current = ""
    lines: list[str] = []
    for line in board.splitlines():
        if line.startswith("## "):
            if current == normalized_heading:
                return "\n".join(lines).strip()
            current = line.removeprefix("##").strip().lower()
            lines = []
        elif current:
            lines.append(line)
    if current == normalized_heading:
        return "\n".join(lines).strip()
    return None


def discussion_board_entry_issue(entry: str) -> str | None:
    values: dict[str, str] = {}
    for field in BOARD_RESEARCH_FIELDS:
        match = re.search(
            rf"^\s*[-*]\s*{re.escape(field)}\s*:\s*(.+?)\s*$",
            entry,
            flags=re.IGNORECASE | re.MULTILINE,
        )
        values[field] = " ".join(match.group(1).strip().split()) if match else ""
    kind = values["Entry kind"].lower()
    if kind not in {"request", "contribution"}:
        return "missing Entry kind request|contribution"
    for field in BOARD_RESEARCH_FIELDS[1:]:
        value = values[field]
        # Angle brackets inside inline code are API/template syntax, not an
        # unresolved discussion-board placeholder (for example
        # `PipeBarrier<PIPE_MTE3>()`).
        prose_value = re.sub(r"`[^`\n]*`", "", value)
        if len(value) < 12 or re.search(r"<[^>\n]{2,120}>", prose_value):
            return f"missing meaningful {field}"
    return None


def route_reasoning_quality(version_text: str) -> dict[str, Any]:
    """Check visible evidence-first routing without judging the chosen mechanism."""
    values = {
        field: inline_field_value(version_text, field)
        for field in ROUTE_REASONING_FIELDS
    }
    applicable = any(values.values())
    if not applicable:
        return {
            "applicable": False,
            "status": "legacy",
            "score": 0,
            "max_score": 6,
            "checks": {},
            "fields": values,
        }

    observed = values["Observed signal"].lower()
    primary = values["Primary hypothesis"].lower()
    counter = values["Counter-hypothesis"].lower()
    router_gap = values["Router gap"].lower()
    consulted_offset = version_text.find("Consulted evidence:")
    fields_before_routing = consulted_offset >= 0 and all(
        0 <= version_text.lower().find(f"{field.lower()}:") < consulted_offset
        for field in ROUTE_REASONING_FIELDS
    )
    router_gap_covered = False
    if router_gap.startswith("none"):
        router_gap_covered = any(
            token in router_gap
            for token in (
                "cover",
                "because",
                "directly",
                "ascendc-",
                "checklist",
                "reference",
            )
        )
    elif router_gap:
        router_gap_covered = any(
            token in router_gap
            for token in (
                "missing",
                "unrepresented",
                "request",
                "contribution",
                "lacks",
                "gap",
            )
        )
    checks = {
        "all_fields_meaningful": all(
            len(value) >= 16
            and value.lower() not in {"none", "n/a", "na", "not specified", "unknown"}
            and not re.search(r"<[^>\n]{2,120}>", value)
            for value in values.values()
        ),
        "fields_before_routing": fields_before_routing,
        "observed_signal_grounded": bool(re.search(r"\d", observed))
        or any(
            token in observed
            for token in (
                "result",
                "source",
                "profiler",
                "probe",
                "case",
                "pass",
                "fail",
                "regress",
                "timing",
                "hash",
                "code path",
            )
        ),
        "primary_hypothesis_falsifiable": bool(
            re.search(
                r"\b(?:falsif\w*|reject\w*|rollback|restore|unless|if|fail\w*|below|"
                r"above|threshold)\b|(?:<=|>=|<|>)",
                primary,
            )
        ),
        "counter_hypothesis_discriminating": any(
            token in counter
            for token in (
                "distinguish",
                "instead",
                "would",
                "otherwise",
                "flat",
                "inconsistent",
                "control",
                "noise",
                "fail",
                "threshold",
                "no alternative remains",
                "none remains",
            )
        ),
        "router_gap_covered": router_gap_covered,
    }
    score = sum(1 for value in checks.values() if value)
    return {
        "applicable": True,
        "status": "complete" if score == len(checks) else "partial",
        "score": score,
        "max_score": len(checks),
        "checks": checks,
        "fields": values,
    }


def inline_field_value(text: str, field: str) -> str:
    match = re.search(
        rf"^\s*{re.escape(field)}\s*:\s*(.+?)\s*$",
        text,
        flags=re.IGNORECASE | re.MULTILINE,
    )
    return " ".join(match.group(1).strip().split()) if match else ""


def count_reviews(items: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"complete": 0, "partial": 0, "missing": 0}
    for item in items:
        status = str(item.get("review_status", "missing") or "missing")
        counts[status if status in counts else "partial"] += 1
    return counts


def count_route_reasoning(items: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"complete": 0, "partial": 0, "legacy": 0}
    for item in items:
        route = item.get("route_reasoning", {}) if isinstance(item, dict) else {}
        status = str(route.get("status", "legacy") or "legacy") if isinstance(route, dict) else "legacy"
        counts[status if status in counts else "partial"] += 1
    return counts


def compact_result(root: Path, item: dict[str, Any]) -> dict[str, Any]:
    path = root / str(item.get("path", "") or "")
    source_path = path.parent / "submit_snapshot" / "pending_snapshot" / "source_snapshot"
    return {
        "test_version": item.get("test_version", ""),
        "verdict": item.get("verdict", ""),
        "case_version": item.get("case_version", ""),
        "weighted_time_us": item.get("weighted_time_us"),
        "result_path": str(item.get("path", "") or ""),
        "source_snapshot": relative(source_path, root) if source_path.exists() else "",
    }


def write_iteration_quality_files(root: Path, snapshot: dict[str, Any]) -> None:
    state_dir = root / "TestUtils" / "tester_daemon"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "iteration_quality.json").write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    (state_dir / "ITERATION_QUALITY.md").write_text(
        render_iteration_quality(snapshot),
        encoding="utf-8",
    )


def render_iteration_quality(snapshot: dict[str, Any]) -> str:
    board = snapshot.get("suggestion_board", {})
    lines = [
        "# Iteration And Skill Application Quality",
        "",
        f"- updated_at: {snapshot.get('updated_at', '')}",
        "- semantic reviewer: solver",
        "- daemon role: lineage/performance/review completeness checks",
        f"- suggestion board: {board.get('status', 'unknown') if isinstance(board, dict) else 'unknown'}; "
        f"open={board.get('open_count', '-') if isinstance(board, dict) else '-'}; "
        f"structured={board.get('structured_count', '-') if isinstance(board, dict) else '-'}; "
        f"malformed_open={board.get('malformed_open_count', '-') if isinstance(board, dict) else '-'}",
        "",
        "| op | latest | case | weighted_us | best_same_case | delta | route model | review | lineage | self verdict |",
        "|---|---|---|---:|---|---:|---|---|---|---|",
    ]
    operators = snapshot.get("operators", {})
    if isinstance(operators, dict):
        for op, data in operators.items():
            latest = data.get("latest", {}) if isinstance(data, dict) else {}
            review = latest.get("review", {}) if isinstance(latest, dict) else {}
            route = latest.get("route_reasoning", {}) if isinstance(latest, dict) else {}
            lines.append(
                f"| {op} | {latest.get('test_version', '-') or '-'} | "
                f"{latest.get('case_version', '-') or '-'} | "
                f"{display(latest.get('weighted_time_us'))} | "
                f"{latest.get('best_same_case_version', '-') or '-'} "
                f"({display(latest.get('best_same_case_weighted_us'))}) | "
                f"{display_delta(latest.get('regression_vs_best_pct'))} | "
                f"{route.get('status', '-') if isinstance(route, dict) else '-'}"
                f"/{route.get('score', '-') if isinstance(route, dict) else '-'} | "
                f"{latest.get('review_status', '-') or '-'}"
                f"/{latest.get('review_evidence', {}).get('score', '-') if isinstance(latest.get('review_evidence'), dict) else '-'} | "
                f"{'resolved' if latest.get('lineage_resolved') else 'unresolved'} | "
                f"{review.get('self_verdict', '-') if isinstance(review, dict) else '-'} |"
            )
    lines.extend(
        [
            "",
            "A regression is not automatically low quality: a declared diagnostic may regress. "
            "The solver review must say whether to keep, revert, or branch from the same-case best.",
            "Only reusable skill lessons belong on `docs/next/skill_discussion_board.md`.",
            "",
        ]
    )
    return "\n".join(lines)


def tree_digest(path: Path) -> str:
    digest = hashlib.sha256()
    if not path.exists():
        return ""
    files = [path] if path.is_file() else sorted(item for item in path.rglob("*") if item.is_file())
    for item in files:
        relative_path = item.name if path.is_file() else item.relative_to(path).as_posix()
        digest.update(relative_path.encode("utf-8"))
        digest.update(b"\0")
        with item.open("rb") as source:
            while chunk := source.read(1024 * 1024):
                digest.update(chunk)
        digest.update(b"\0")
    return digest.hexdigest()


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def relative(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def display(value: object) -> str:
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
