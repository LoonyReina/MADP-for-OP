"""Short notifications point to the workspace contract; they do not own state."""
from __future__ import annotations


def workspace_entry_prompt(*, operator: str, workspace: str, phase: str,
        action_id: str = "", season: str = "", mode: str = "correctness",
        performance_phase: str | None = None) -> str:
    identity = f" Action: {action_id}." if action_id else f" Campaign: {season}."
    if phase == "case data authoring":
        prompt = (f"{operator} {phase}.{identity}\n"
            f"Read {workspace}/.ascendop/BRIEF.md and CLIENT.json for the current input contract and file command.\n"
            "Review diverse legal inputs against actual source paths and accepted results. Source and oracle stay read-only; "
            "Run local CPU analysis in notes; write this action's draft and one proposal. Preserve protected regressions. "
            "Use the current workspace-case entry, then finish the turn for daemon handoff; do not write control state or a second outcome.")
        if len(prompt) > 800:
            raise ValueError("managed case delivery exceeds 800 characters")
        return prompt
    if mode in {"performance", "both"}:
        authority = (
            "Baseline bootstrap: source is read-only; revise coverage or measure unchanged source. "
            if performance_phase == "baseline_bootstrap" else
            "Baseline repair: fix correctness in source for the first valid baseline; do not claim speedup. "
            if performance_phase == "baseline_repair" else
            "Improve general performance while preserving correctness on the accepted cases. "
        )
        prompt = (f"{operator} {phase}.{identity}\n"
            f"Read {workspace}/.ascendop/BRIEF.md and CLIENT.json: evidence, permissions, file commands.\n"
            + authority
            + "Dispatch by structure/dtype/layout/size/alignment, never case_id, seed, input fingerprints or precomputed outputs. "
            "Run local CPU analysis in notes; follow current task scope, not stale checklists. "
            "Submit one proposal; finish for daemon handoff. "
            "Keep oracle/control state unchanged; no second outcome or resending accepted requests.")
        if len(prompt) > 800:
            raise ValueError("managed performance delivery exceeds 800 characters")
        return prompt
    prompt = (f"{operator} {phase}.{identity}\n"
        f"Read {workspace}/.ascendop/BRIEF.md and CLIENT.json for task, evidence, permissions and file commands.\n"
        "Experiment toward same-candidate local full PASS and official Pass. "
        "Choose source changes, legal cases or diagnostics; freely run local CPU analysis in notes. "
        "Before final testing, remove temporary device prints and prohibited debug code from official slots. "
        "Submit one proposal in this workspace; finish for daemon handoff. "
        "Keep oracle and accepted facts unchanged; do not write control state or resubmit accepted requests.")
    if len(prompt) > 800:
        raise ValueError("managed correctness delivery exceeds 800 characters")
    return prompt
