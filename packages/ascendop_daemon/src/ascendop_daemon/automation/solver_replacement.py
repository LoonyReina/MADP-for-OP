from __future__ import annotations

import json
from hashlib import sha1
from pathlib import Path
from typing import Any

from ascendop_daemon.core.models import DaemonConfig, operator_season, solver_session_replacement_allowed, utc_now_iso
from ascendop_daemon.automation.trigger_state import read_trigger_ack_state


STATE_DIR_NAME = ("TestUtils", "tester_daemon")


def build_solver_replacement_plan(root: Path, config: DaemonConfig) -> dict[str, Any]:
    if not solver_session_replacement_allowed(config):
        return {
            "updated_at": utc_now_iso(),
            "replacement_allowed": False,
            "replacement_required_count": 0,
            "same_session_recovery": True,
            "replacements": [],
        }
    state_dir = root.joinpath(*STATE_DIR_NAME)
    trigger_plan = read_json(state_dir / "solver_trigger_plan.json")
    liveness = read_json(state_dir / "solver_session_status.json")
    observations = read_json(state_dir / "solver_thread_observations.json")
    ack_state = read_trigger_ack_state(root)

    active_by_key = {
        str(item.get("key", "") or ""): item
        for item in liveness.get("active_solver_gates", [])
        if isinstance(item, dict) and item.get("key")
    }
    observations_by_op = {
        str(item.get("op", "") or ""): item
        for item in observations.get("threads", [])
        if isinstance(item, dict) and item.get("op")
    }
    sent = ack_state.get("sent", {}) if isinstance(ack_state.get("sent"), dict) else {}

    replacements: list[dict[str, Any]] = []
    for trigger in trigger_plan.get("triggers", []) if isinstance(trigger_plan.get("triggers"), list) else []:
        if not isinstance(trigger, dict):
            continue
        key = str(trigger.get("key", "") or "")
        op = str(trigger.get("op", "") or "")
        if not key or not op:
            continue
        active = active_by_key.get(key, {})
        observed = observations_by_op.get(op, {})
        record = sent.get(key) if isinstance(sent, dict) else {}
        if not replacement_required(trigger, active, observed, record if isinstance(record, dict) else {}):
            continue
        replacement = build_replacement_entry(root, config, trigger, active, observed, record if isinstance(record, dict) else {})
        replacements.append(replacement)

    return {
        "updated_at": utc_now_iso(),
        "replacement_allowed": True,
        "replacement_required_count": len(replacements),
        "replacements": replacements,
    }


def write_solver_replacement_files(root: Path, config: DaemonConfig) -> dict[str, Any]:
    state_dir = root.joinpath(*STATE_DIR_NAME)
    state_dir.mkdir(parents=True, exist_ok=True)
    payload = build_solver_replacement_plan(root, config)
    (state_dir / "solver_replacement_plan.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    (state_dir / "SOLVER_REPLACEMENT_PLAN.md").write_text(
        render_solver_replacement_plan(payload),
        encoding="utf-8",
    )
    return payload


def replacement_required(
    trigger: dict[str, Any],
    active: dict[str, Any],
    observed: dict[str, Any],
    record: dict[str, Any],
) -> bool:
    if str(trigger.get("status", "") or "") == "replacement-required":
        return True
    if str(active.get("thread_status_type", "") or "") == "systemError":
        return True
    if str(observed.get("thread_status_type", "") or "") == "systemError":
        return True
    if str(record.get("thread_status_type", "") or "") == "systemError":
        return True
    if (
        str(record.get("failure_kind", "") or "") == "native_turn_no_agent_output"
        and int(record.get("native_no_agent_output_count", 0) or 0) >= 2
    ):
        return True
    return False


def build_replacement_entry(
    root: Path,
    config: DaemonConfig,
    trigger: dict[str, Any],
    active: dict[str, Any],
    observed: dict[str, Any],
    record: dict[str, Any],
) -> dict[str, Any]:
    state_dir = root.joinpath(*STATE_DIR_NAME)
    prompt_dir = state_dir / "solver_replacement_prompts"
    prompt_dir.mkdir(parents=True, exist_ok=True)
    key = str(trigger.get("key", "") or "")
    op = str(trigger.get("op", "") or "")
    digest = sha1(key.encode("utf-8")).hexdigest()[:12]
    prompt_path = prompt_dir / f"{op}_{digest}_replacement.md"
    prompt_path.write_text(build_replacement_prompt(config, trigger), encoding="utf-8")
    thread_id = str(trigger.get("thread_id", "") or active.get("thread_id", "") or record.get("thread_id", "") or "")
    reason_bits = []
    for source in (active, observed, record):
        status_type = str(source.get("thread_status_type", "") or "")
        if status_type == "systemError":
            reason_bits.append("thread_status_type=systemError")
            break
    if str(record.get("failure_kind", "") or "") == "native_turn_no_agent_output":
        reason_bits.append(
            "native_turn_no_agent_output"
            f":count={record.get('native_no_agent_output_count', 0) or 0}"
        )
    return {
        "op": op,
        "gate_stage": trigger.get("gate_stage", ""),
        "key": key,
        "old_thread_id": thread_id,
        "old_thread_status_type": observed.get("thread_status_type") or active.get("thread_status_type") or record.get("thread_status_type", ""),
        "latest_turn_id": observed.get("latest_turn_id", "") or record.get("turn_id", ""),
        "latest_turn_status": observed.get("latest_turn_status", "") or record.get("native_status", ""),
        "latest_user_only_turn": bool(observed.get("latest_user_only_turn") or record.get("latest_user_only_turn")),
        "reason": "; ".join(reason_bits) or "solver trigger marked replacement-required",
        "replacement_prompt_path": relpath(prompt_path, root),
        "register_command_template": (
            "python tools\\tester_daemon\\daemon.py register-solver-thread "
            "--config tools\\tester_daemon\\config\\s5_910b_gitpartner_glugrad_bitwise.json "
            f"--op {op} --thread-id <new_thread_id> --clear-op-trigger-state"
        ),
    }


def build_replacement_prompt(config: DaemonConfig, trigger: dict[str, Any]) -> str:
    op = str(trigger.get("op", "") or "")
    season = operator_season(config, op)
    gate = str(trigger.get("gate_stage", "") or "")
    next_command = str(trigger.get("key", "") or "").split("|", 2)[-1]
    solver_goal = f"docs/next/agent_prompts/{season}/{op}/solver_goal.md"
    return "\n".join(
        [
            f"/goal You are the replacement {season} solver session for `{op}`.",
            "",
            "The previous long-lived solver session is marked `systemError` or produced user-only turns with no agent output.",
            "Work only in the shared workspace and keep the solver role boundaries.",
            "",
            "Read and follow first:",
            "- `.agents/skills/ascendop-workflow-router/SKILL.md`",
            "- `.agents/skills/ascendop-next-workflow/SKILL.md`",
            f"- `{solver_goal}`",
            "- `docs/next/multi_agent_workflow.md`",
            "- `docs/next/skill_policy.md`",
            "",
            f"Current gate: `{op} | {gate} | solver`.",
            "Required first command:",
            "```powershell",
            (
                "python scripts\\next_workflow.py session-board "
                f"--season {season} --op {op} --transport {config.transport} --remote-root {config.remote_root}"
            ),
            "```",
            "",
            "Daemon-observed command:",
            "```text",
            next_command,
            "```",
            "",
            "Required outcome this turn: produce a board/daemon-consumable state change.",
            "If result evidence proves deterministic case-lifetime improvement, write the required release evidence.",
            "Otherwise create the next pending candidate through `scripts\\next_workflow.py create-pending`, or write a workflow-visible blocker/rollover marker when no defensible candidate remains.",
            "",
            "Optimization policy:",
            "- Start from `ascendc-optimization-router` before choosing source/perf direction.",
            "- Record a concrete `Optimization method decision:` in any new `VERSION.md`.",
            "- Do not continue same-case chunk sweeps without a new method.",
            "",
            "Boundaries:",
            "- Do not edit `TestUtils/submit`, queue state, GitPartner state, or unrelated result archives.",
            "- Do not create other Codex sessions from this solver session.",
            "",
        ]
    )


def render_solver_replacement_plan(payload: dict[str, Any]) -> str:
    if payload.get("replacement_allowed") is False:
        return "\n".join(
            [
                "# Solver Same-Session Recovery Policy",
                "",
                f"- updated_at: {payload.get('updated_at', '')}",
                "- replacement_allowed: false",
                "- current_solver_sessions_preserved: true",
                "- action: retry/repair only the configured existing solver threads when the board is solver-owned",
                "",
            ]
        )
    lines = [
        "# Solver Replacement Plan",
        "",
        f"- updated_at: {payload.get('updated_at', '')}",
        f"- replacement_required_count: {payload.get('replacement_required_count', 0)}",
        "",
    ]
    replacements = payload.get("replacements", [])
    if not isinstance(replacements, list) or not replacements:
        lines.append("- none")
        lines.append("")
        return "\n".join(lines)
    lines.extend(
        [
            "| op | gate | old_thread | reason | prompt | register_command |",
            "|---|---|---|---|---|---|",
        ]
    )
    for item in replacements:
        if not isinstance(item, dict):
            continue
        lines.append(
            f"| {item.get('op', '-')} | {item.get('gate_stage', '-')} | "
            f"{item.get('old_thread_id', '-') or '-'} | {escape_cell(str(item.get('reason', '-')))} | "
            f"`{item.get('replacement_prompt_path', '-')}` | "
            f"`{escape_cell(str(item.get('register_command_template', '-')))}` |"
        )
    lines.append("")
    return "\n".join(lines)


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def relpath(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def escape_cell(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")
