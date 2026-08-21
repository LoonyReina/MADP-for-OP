from __future__ import annotations

import json
from typing import Any, Mapping

from .catalog import flow_v5_catalog, flow_v5_catalog_digest
from .contracts import (
    AGENT_ACTION_CONTEXT_V2_SCHEMA,
    AGENT_ACTION_CONTEXT_V3_SCHEMA,
    validate_agent_action_context_v2,
    validate_agent_action_context_v3,
)


def build_v5_prompt_context(
    action: Mapping[str, Any],
    context: Mapping[str, Any],
    attempt: Mapping[str, Any],
) -> dict[str, Any]:
    catalog = flow_v5_catalog()
    role = str(action["role"])
    evidence_operations = list(context.get("evidence_operations") or [])
    recent_results = list(context.get("recent_results") or [])
    official_evidence = list(context.get("official_evidence") or [])
    latest_evidence = next(
        (
            dict(item.get("result") or {})
            for item in evidence_operations
            if isinstance(item, Mapping) and isinstance(item.get("result"), Mapping)
        ),
        {},
    )
    latest_route = next(
        (
            dict(item.get("route") or {})
            for item in evidence_operations
            if isinstance(item, Mapping) and isinstance(item.get("route"), Mapping)
        ),
        {},
    )
    value = {
        "schema": AGENT_ACTION_CONTEXT_V3_SCHEMA,
        "context_id": str(context["snapshot_id"]),
        "action_id": str(action["action_id"]),
        "catalog_generation": str(catalog["generation"]),
        "catalog_digest": flow_v5_catalog_digest(),
        "role": role,
        "candidate": {
            **dict(context.get("candidate_identity") or {}),
            "candidate_version": str(action.get("candidate_version") or ""),
        },
        "case": (
            {
                **dict(context.get("candidate_identity") or {}),
                "active_case_version": str(
                    context.get("active_case_version") or ""
                ),
            }
            if role == "tester"
            else {
                "active_case_version": str(
                    context.get("active_case_version") or ""
                )
            }
        ),
        "comparable_result": {
            "recent_results": recent_results,
            "latest_evidence_result": latest_evidence,
        },
        "baseline": {
            "official_evidence": official_evidence,
            "pinned": official_evidence[0] if official_evidence else None,
        },
        "evidence_index": {
            "workflow": list(context.get("workflow_evidence") or []),
            "reference": list(context.get("reference_evidence") or []),
            "operations": evidence_operations,
            "completeness": dict(context.get("context_completeness") or {}),
        },
        "hypotheses": list(context.get("open_hypotheses") or []),
        "environment": {
            "permitted_operations": list(
                context.get("permitted_operations") or []
            ),
            "latest_execution_route": latest_route,
        },
        "budget": {
            **dict(action.get("tool_budget") or {}),
            "evidence_attempt_history": [
                dict(item.get("claim") or {})
                for item in evidence_operations
                if isinstance(item, Mapping)
            ],
        },
        "attempt": dict(attempt),
        "lineage": dict(context.get("causation") or action.get("causation") or {}),
        "write_scope": list(action.get("write_scope") or []),
        "allowed_outcomes": list(catalog["outcomes"]),
        "available_evidence_operations": [
            str(item["operation_code"])
            for item in catalog["evidence_operations"]
            if role in item["expected_consumers"]
        ],
        "output_schemas": ["ascendop.agent-action-outcome.v1"],
        "created_at": str(context["created_at"]),
    }
    return validate_agent_action_context_v3(value)


def render_v5_action_contract(context: Mapping[str, Any]) -> str:
    schema = str(context.get("schema") or "")
    if schema == AGENT_ACTION_CONTEXT_V3_SCHEMA:
        validated = validate_agent_action_context_v3(context)
    elif schema == AGENT_ACTION_CONTEXT_V2_SCHEMA:
        validated = validate_agent_action_context_v2(context)
    else:
        raise ValueError(f"unsupported Agent action context schema: {schema}")
    catalog = flow_v5_catalog()
    outcome_schema = {
        "schema": "ascendop.agent-action-outcome.v1",
        "action_id": str(validated["action_id"]),
        "execution_status": "completed",
        "disposition": "<one allowed outcome>",
        "failure_class": None,
        "summary": "<evidence-grounded summary>",
        "outputs": [],
        "evidence_refs": [],
        "requested_operation": None,
        "blocker": None,
        "completed_at": "<RFC3339 timestamp>",
    }
    output_item_schema = {
        "output_id": "<declared output id>",
        "output_kind": "<declared output kind>",
        "artifact_ref": "<workspace-relative artifact path>",
        "sha256": "<64 lowercase hex characters>",
    }
    attempt_contract = ""
    if schema == AGENT_ACTION_CONTEXT_V3_SCHEMA:
        attempt = dict(validated["attempt"])
        attempt_contract = (
            f"\nAttempt: {attempt['attempt_id']} ordinal={attempt['ordinal']} "
            f"mode={attempt['mode']}"
        )
        repair = attempt.get("output_repair")
        if isinstance(repair, Mapping):
            attempt_contract += (
                "\nOUTPUT REPAIR DIRECTIVE (authoritative): this is the one "
                "bounded correction turn for the same immutable action and lease."
                "\nCorrect this exact validation error in the declared output slot: "
                + str(repair["validation_error"])
                + "\nPreserve the action, iteration, lease, operator, role, and write "
                "scope identities. Do not repeat the rejected output unchanged."
            )
    return (
        "Flow V5 immutable action contract\n"
        f"Catalog generation: {catalog['generation']}\n"
        f"Catalog digest: {flow_v5_catalog_digest()}\n"
        f"Effective role: {validated['role']}\n"
        "Allowed outcomes: "
        + ", ".join(validated["allowed_outcomes"])
        + "\nAvailable evidence operations: "
        + (", ".join(validated["available_evidence_operations"]) or "none")
        + "\nWrite scope: "
        + (", ".join(validated["write_scope"]) or "none")
        + "\nCausation: "
        + json.dumps(validated.get("lineage") or {}, sort_keys=True)
        + attempt_contract
        + "\nReturn exactly one JSON object matching this shape:\n"
        + json.dumps(outcome_schema, indent=2, sort_keys=True)
        + "\n`outputs` must be empty or contain only objects matching this shape:\n"
        + json.dumps(output_item_schema, indent=2, sort_keys=True)
        + "\nNever place a path string directly in `outputs`."
        + "\nDo not invent operation codes, mutate workflow state, or act outside scope."
    )


__all__ = ["build_v5_prompt_context", "render_v5_action_contract"]
