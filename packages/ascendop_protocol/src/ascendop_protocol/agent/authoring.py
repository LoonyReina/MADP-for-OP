from __future__ import annotations

import json
from typing import Any, Iterable, Mapping

from ..workflow.contracts import (
    SOLVER_DIAGNOSTIC_CAPABILITIES,
    SOLVER_DIAGNOSTIC_CORRECTNESS_ARTIFACTS,
    SOLVER_DIAGNOSTIC_EXHAUSTION_SCOPE,
    SOLVER_DIAGNOSTIC_HOLD_OWNERS,
    SOLVER_DIAGNOSTIC_HOLD_RESUME_TRIGGERS,
)
from .contracts import validate_agent_output_contract


def render_agent_output_authoring_contract(
    raw_contracts: Iterable[Mapping[str, Any]],
) -> str:
    """Render exact authoring requirements from typed Agent output contracts."""

    contracts = [validate_agent_output_contract(item) for item in raw_contracts]
    if not contracts:
        return (
            "AGENT OUTPUT AUTHORING CONTRACT:\n"
            "No daemon-authorized non-source output slot is declared for this action."
        )

    sections = [
        "AGENT OUTPUT AUTHORING CONTRACT (derived from immutable output_contracts):",
        "Only write a declared isolated_path. Required literals are protocol fields, "
        "not prose suggestions.",
    ]
    for contract in contracts:
        kind = str(contract["output_kind"])
        path = str(contract["isolated_path"])
        required = "required" if contract["required"] else "optional"
        identity = dict(contract["identity"])
        sections.append(
            f"\n[{contract['output_id']}] {required}; path={path}; kind={kind}"
        )
        if kind == "solver-blocker":
            sections.append(
                "If produced, write UTF-8 Markdown containing these exact identity lines:\n"
                "Status: active\n"
                f"Operator: {identity['operator']}\n"
                f"Case version: {identity['case_version']}\n"
                f"Result version: {identity['result_version']}\n"
                "Diagnostic contract revision: "
                f"{identity['diagnostic_contract_revision']}\n"
                "The document must also contain these exact marker literals, each followed "
                "by evidence:\n"
                "Observed signal:\n"
                "Primary hypothesis:\n"
                "Counter-hypothesis:\n"
                "Required evidence:\n"
                "Consulted evidence:\n"
                "If the required evidence needs daemon execution, add exactly one of:\n"
                "Diagnostic operation: diagnostic-profile\n"
                "Diagnostic operation: diagnostic-correctness-replay\n"
                "If an implemented daemon transition or true external dependency must "
                "wake this result, add exactly:\n"
                "Diagnostic disposition: durable-hold\n"
                "and all four responsibility fields:\n"
                f"Hold owner: one of {', '.join(sorted(SOLVER_DIAGNOSTIC_HOLD_OWNERS))}\n"
                "Hold resume trigger: one of "
                f"{', '.join(sorted(SOLVER_DIAGNOSTIC_HOLD_RESUME_TRIGGERS))}\n"
                "Hold required capability: one registered capability for daemon-harness, "
                "or one lowercase external capability token\n"
                "Hold evaluated capability generation: "
                f"{identity['diagnostic_capability_generation']}\n"
                "A daemon-harness hold must use daemon-evidence-transition and a current "
                "registered capability. An external hold must use owner external and "
                "external-evidence-transition. Solver cannot assign a hold to a flow "
                "maintainer.\n"
                "If every registered operation and staged artifact has been exhausted "
                "for this exact result, add instead exactly:\n"
                "Diagnostic disposition: evidence-exhausted\n"
                f"Exhaustion scope: {SOLVER_DIAGNOSTIC_EXHAUSTION_SCOPE}\n"
                "Exhaustion evaluated capability generation: "
                f"{identity['diagnostic_capability_generation']}\n"
                "The harness converts evidence-exhausted into one exactly-once "
                "steward escalation. Solver stops here and does not message Main, "
                "retry, or invent another operation.\n"
                "Declare exactly one operation or disposition.\n"
                "Current registered diagnostic capabilities for this generation: "
                f"{', '.join(SOLVER_DIAGNOSTIC_CAPABILITIES)}.\n"
                "If registered diagnostic operations or artifacts cover the required "
                "evidence, request that operation; do not describe it as a durable "
                "capability hold. Do not invent an internal capability name. Completed "
                "diagnostic indexes, summaries, and returned "
                "artifact bundles are automatically staged in the next Solver action's "
                ".ascendop-evidence manifest. Missing internal diagnostic detail is "
                "evidence-exhausted, which the daemon proactively routes to the "
                "configured steward.\n"
                "Registered diagnostic-correctness-replay returns all-case evidence, "
                "including first-failure-traceback and case-logs. Registered "
                "diagnostic-profile supports complete profile_call_plan attribution, "
                "up to three comparison targets, same-endpoint-environment-device "
                "affinity, and one to five repetitions. Registered "
                "diagnostic-correctness-replay may also request "
                "runtime-boundary-trace for case-method/custom-op entry and return, "
                "timeout stack samples, workspace allocation, and executor/OpCommand "
                "boundaries, or native-workspace-query-attribution for the ACLNN "
                "workspace operation, native exception/backtrace, process-memory "
                "snapshot, and allocation-request availability, or "
                "kernel-fault-attribution for an mssanitizer memcheck log, fault PCs, "
                "reported source locations/UB ranges, and source-indexed TPipe/TQue "
                "InitBuffer plus vector-call declarations. It may target an "
                "already-consumed test version by its "
                "preserved immutable source digest; this evidence-only replay does not "
                "consume case lifetime or create a candidate.\n"
                "Do not replace protocol literals with synonyms or heading-only variants."
            )
        elif kind == "pending-evidence-repair":
            sections.append(
                "The daemon has copied the exact current pending VERSION.md into this "
                "isolated path. Edit that file in place to repair only the evidence gaps "
                "named by the effective gate. Do not edit op_host/, op_kernel/, create a "
                "new candidate proposal, or change the pending version identity. Preserve "
                "the exact heading and immutable fields, and keep concrete evidence after "
                "each of these literals:\n"
                "Observed signal:\n"
                "Primary hypothesis:\n"
                "Counter-hypothesis:\n"
                "Router gap:\n"
                "Consulted evidence:\n"
                "Optimization method decision:\n"
                "Shared knowledge decision:\n"
                "Immutable identity and required shared-knowledge paths:\n"
                + json.dumps(identity, ensure_ascii=True, indent=2, sort_keys=True)
                + "\nThe daemon re-evaluates the gate after compare-and-swap promotion; "
                "the Agent does not decide that the candidate is ready."
            )
        elif kind == "solver-diagnostic-request":
            instruction = (
                "Write one JSON object satisfying ascendop.solver-diagnostic-request.v1. "
                "Include every required top-level field: schema, producer, campaign, "
                "operator, case_version, result_version, blocker_generation, "
                "operation_kind, target, scope, requested_device_session_seconds, "
                "retry_policy, rationale, and created_at. Set created_at to a non-empty "
                "UTC RFC3339 timestamp such as 2026-08-12T21:00:00Z; it records request "
                "authorship and does not participate in cross-host duration arithmetic. "
                "Preserve every immutable identity value below exactly:\n"
                + json.dumps(identity, ensure_ascii=True, indent=2, sort_keys=True)
                + "\nSet requested_device_session_seconds to an integer in [30, 240]. "
                "Set retry_policy exactly to "
                '{"max_device_stage_retries":0,"max_execution_attempts":1,'
                '"transport_reuses_request_id":true}. '
                "Set consulted_evidence to a non-empty array of exact paths or "
                "evidence IDs actually inspected from the staged manifest. "
                "Set target.test_version and target.source_sha256 to the immutable "
                "target values above. Set scope.case_selection to all and "
                "scope.fresh_processes to 1."
            )
            if identity.get("operation_kind") == "diagnostic-correctness-replay":
                allowed = ", ".join(SOLVER_DIAGNOSTIC_CORRECTNESS_ARTIFACTS)
                instruction += (
                    "\nThis action is diagnostic-correctness-replay. Set "
                    "scope.profiler_mode exactly to none and "
                    "scope.measurement_repetitions to 1 when present. Omit "
                    "comparison_targets, comparison_affinity, and "
                    "expected_block_dims. "
                    "\nFor scope.requested_artifacts, use only these registered Engine "
                    f"artifact IDs: {allowed}. A deeper probe is not requestable until "
                    "an Engine adapter and capability are registered for it."
                )
            elif identity.get("operation_kind") == "diagnostic-profile":
                instruction += (
                    "\nThis action is diagnostic-profile. Set scope.profiler_mode to "
                    "exactly one of primary-all-cases or "
                    "primary-roofline-all-cases. comparison_targets may contain at "
                    "most three unique targets; when non-empty, set "
                    "scope.comparison_affinity exactly to "
                    "same-endpoint-environment-device. Set "
                    "scope.measurement_repetitions to an integer in [1, 5]."
                )
            sections.append(instruction)
        elif kind == "solver-candidate-proposal":
            sections.append(
                "When this action leaves a testable source candidate, write one JSON "
                "object satisfying ascendop.solver-candidate-proposal.v1. Any change "
                "under op_host/ or op_kernel/ requires this output. Set "
                "candidate_version to the exact FLOW V4 ACTION candidate_version and "
                "preserve these immutable identity values:\n"
                + json.dumps(identity, ensure_ascii=True, indent=2, sort_keys=True)
                + "\nconsulted_evidence and changed_source must be non-empty arrays; "
                "risks must contain correctness, performance, and infrastructure; "
                "hardware must be 910B2, 910B4, or unknown. The daemon, not the Agent, "
                "creates TestUtils/pending."
            )
    return "\n".join(sections)
