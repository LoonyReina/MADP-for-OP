from __future__ import annotations

import json
from importlib.resources import files

import pytest
from jsonschema import Draft202012Validator

from ascendop_protocol.actor import (
    ACTOR_ACTION_ENVELOPE_SCHEMA,
    ACTOR_ACTION_RECEIPT_SCHEMA,
    AGENT_ACTION_CONTEXT_V2_SCHEMA,
    AGENT_ACTION_CONTEXT_V3_SCHEMA,
    AGENT_ACTION_OUTCOME_SCHEMA,
    FLOW_V5_ACTION_KINDS,
    FLOW_V5_CATALOG_SCHEMA,
    FLOW_V5_EVIDENCE_OPERATIONS,
    FLOW_V5_OUTPUT_KINDS,
    FLOW_V5_ROLES,
    NATIVE_TURN_OUTCOME_SCHEMA,
    ROLE_BINDING_SCHEMA,
    ActorContractError,
    flow_v5_catalog,
    flow_v5_catalog_digest,
    render_v5_action_contract,
    validate_actor_action_envelope,
    validate_actor_action_receipt,
    validate_agent_action_context_v2,
    validate_agent_action_context_v3,
    validate_agent_action_outcome,
    validate_native_turn_outcome,
    validate_role_binding,
)
from ascendop_protocol.schemas import load_schema, schema_registry
from ascendop_protocol.evidence import (
    EVIDENCE_OPERATION_REGISTRY_SCHEMA,
    EVIDENCE_OPERATION_REQUEST_SCHEMA,
    EVIDENCE_OPERATION_RESULT_SCHEMA,
    evidence_operation_registry,
    evidence_operation_registry_digest,
    validate_evidence_operation_request,
    validate_evidence_operation_result,
)


EXAMPLES = {
    FLOW_V5_CATALOG_SCHEMA: "flow_v5_catalog.json",
    ROLE_BINDING_SCHEMA: "role_binding.manager.json",
    ACTOR_ACTION_ENVELOPE_SCHEMA: "actor_action.solver.json",
    ACTOR_ACTION_RECEIPT_SCHEMA: "actor_action_receipt.manager.json",
    NATIVE_TURN_OUTCOME_SCHEMA: "native_turn_outcome.completed.json",
    AGENT_ACTION_OUTCOME_SCHEMA: "agent_action_outcome.request_evidence.json",
    AGENT_ACTION_CONTEXT_V2_SCHEMA: "agent_action_context.v2.json",
    AGENT_ACTION_CONTEXT_V3_SCHEMA: "agent_action_context.v3.json",
    EVIDENCE_OPERATION_REGISTRY_SCHEMA: "evidence_operation_registry.json",
    EVIDENCE_OPERATION_REQUEST_SCHEMA: "evidence_operation_request.profile_collect.json",
    EVIDENCE_OPERATION_RESULT_SCHEMA: "evidence_operation_result.profile_collect.json",
}


def _example(filename: str) -> dict[str, object]:
    return json.loads(
        files("ascendop_protocol.schemas")
        .joinpath(f"examples/{filename}")
        .read_text(encoding="utf-8")
    )


def test_flow_v5_schemas_are_registered_and_examples_validate() -> None:
    registered = {entry["schema_id"] for entry in schema_registry()["entries"]}
    for schema_id, filename in EXAMPLES.items():
        assert schema_id in registered
        errors = sorted(
            Draft202012Validator(load_schema(schema_id)).iter_errors(
                _example(filename)
            ),
            key=lambda error: list(error.path),
        )
        assert errors == []


def test_flow_v5_runtime_validators_accept_public_examples() -> None:
    validate_role_binding(_example("role_binding.manager.json"))
    validate_actor_action_envelope(_example("actor_action.solver.json"))
    validate_actor_action_receipt(_example("actor_action_receipt.manager.json"))
    validate_native_turn_outcome(_example("native_turn_outcome.completed.json"))
    validate_agent_action_outcome(
        _example("agent_action_outcome.request_evidence.json")
    )
    validate_agent_action_context_v2(_example("agent_action_context.v2.json"))
    validate_agent_action_context_v3(_example("agent_action_context.v3.json"))
    validate_evidence_operation_request(
        _example("evidence_operation_request.profile_collect.json")
    )
    validate_evidence_operation_result(
        _example("evidence_operation_result.profile_collect.json")
    )


def test_one_native_session_may_hold_distinct_manager_and_assistant_bindings() -> None:
    manager = _example("role_binding.manager.json")
    assistant = {
        **manager,
        "role_binding_id": "binding-assistant-1",
        "role": "assistant",
        "scope": {
            "workspace_roots": [],
            "operator_ids": [],
            "capabilities": ["official-platform"],
        },
    }
    assert manager["native_session_id"] == assistant["native_session_id"]
    validate_role_binding(manager)
    validate_role_binding(assistant)


def test_action_role_and_capability_are_enforced() -> None:
    action = _example("actor_action.solver.json")
    with pytest.raises(ActorContractError, match="requires role solver"):
        validate_actor_action_envelope({**action, "effective_role": "manager"})
    action["scope"] = {
        "workspace_roots": ["operators_workspace/Demo"],
        "operator_ids": ["Demo"],
        "capabilities": ["flow-control"],
    }
    with pytest.raises(ActorContractError, match="operator-source-write"):
        validate_actor_action_envelope(action)


@pytest.mark.parametrize("action_kind", sorted(FLOW_V5_ACTION_KINDS))
def test_every_v5_action_kind_has_a_typed_payload(action_kind: str) -> None:
    action = _typed_action(action_kind)
    validate_actor_action_envelope(action)
    errors = list(
        Draft202012Validator(load_schema(ACTOR_ACTION_ENVELOPE_SCHEMA)).iter_errors(
            action
        )
    )
    assert errors == []
    action["payload"] = {}
    with pytest.raises(ActorContractError, match="payload"):
        validate_actor_action_envelope(action)


def test_outcome_cross_field_semantics_are_enforced() -> None:
    outcome = _example("agent_action_outcome.request_evidence.json")
    outcome["requested_operation"] = {
        **outcome["requested_operation"],
        "operation_code": "shell.custom",
    }
    with pytest.raises(ActorContractError, match="unsupported evidence operation"):
        validate_agent_action_outcome(outcome)

    failed = {
        **_example("agent_action_outcome.request_evidence.json"),
        "execution_status": "failed",
        "disposition": None,
        "failure_class": "protocol_violation",
        "requested_operation": None,
    }
    validate_agent_action_outcome(failed)


def test_catalog_is_the_checked_source_for_repeated_public_enums() -> None:
    catalog = flow_v5_catalog()
    operation_registry = evidence_operation_registry()
    assert FLOW_V5_ROLES == set(catalog["roles"])
    assert FLOW_V5_ACTION_KINDS == {
        row["action_kind"] for row in catalog["action_catalog"]
    }
    assert FLOW_V5_EVIDENCE_OPERATIONS == {
        row["operation_code"] for row in operation_registry["operations"]
    }
    assert (
        catalog["evidence_operation_registry_generation"]
        == operation_registry["generation"]
    )
    assert (
        catalog["evidence_operation_registry_digest"]
        == evidence_operation_registry_digest()
    )
    assert FLOW_V5_OUTPUT_KINDS == set(catalog["output_kinds"])

    assert FLOW_V5_ACTION_KINDS == set(
        load_schema(ACTOR_ACTION_ENVELOPE_SCHEMA)["properties"]["action_kind"][
            "enum"
        ]
    )
    assert FLOW_V5_EVIDENCE_OPERATIONS == set(
        load_schema(AGENT_ACTION_OUTCOME_SCHEMA)["$defs"]["requested_operation"][
            "properties"
        ]["operation_code"]["enum"]
    )
    assert FLOW_V5_EVIDENCE_OPERATIONS == set(
        load_schema(AGENT_ACTION_CONTEXT_V2_SCHEMA)["properties"][
            "available_evidence_operations"
        ]["items"]["enum"]
    )
    assert FLOW_V5_EVIDENCE_OPERATIONS == set(
        load_schema(AGENT_ACTION_CONTEXT_V3_SCHEMA)["properties"][
            "available_evidence_operations"
        ]["items"]["enum"]
    )
    for schema_id in (
        "ascendop.agent-output-contract.v1",
        "ascendop.agent-output-seal.v1",
        "ascendop.agent-output-promotion-receipt.v1",
    ):
        schema = load_schema(schema_id)
        output = (
            schema["properties"]["output_kind"]
            if schema_id.endswith("contract.v1")
            else schema["$defs"]["output"]["properties"]["output_kind"]
        )
        assert FLOW_V5_OUTPUT_KINDS - {"source-change"} == set(output["enum"])
    assert FLOW_V5_OUTPUT_KINDS == set(
        load_schema(AGENT_ACTION_OUTCOME_SCHEMA)["$defs"]["output"]["properties"][
            "output_kind"
        ]["enum"]
    )


def test_context_generation_and_prompt_are_catalog_anchored() -> None:
    context = _example("agent_action_context.v3.json")
    assert context["catalog_digest"] == flow_v5_catalog_digest()
    rendered = render_v5_action_contract(context)
    assert f"Catalog digest: {flow_v5_catalog_digest()}" in rendered
    assert "profile.collect" in rendered
    assert "OUTPUT REPAIR DIRECTIVE (authoritative)" in rendered
    assert "created_at must be non-empty text" in rendered
    assert "Return exactly one JSON object" in rendered
    assert "Never place a path string directly in `outputs`." in rendered
    assert '"artifact_ref": "<workspace-relative artifact path>"' in rendered
    with pytest.raises(ActorContractError, match="catalog generation is stale"):
        validate_agent_action_context_v3(
            {**context, "catalog_generation": "stale-generation"}
        )


def test_context_v3_repair_must_match_latest_prior_failure() -> None:
    context = _example("agent_action_context.v3.json")
    context["attempt"] = {
        **context["attempt"],
        "output_repair": {
            **context["attempt"]["output_repair"],
            "validation_error": "different error",
        },
    }

    with pytest.raises(ActorContractError, match="latest prior attempt failure"):
        validate_agent_action_context_v3(context)


def _typed_action(action_kind: str) -> dict[str, object]:
    action = _example("actor_action.solver.json")
    role = action_kind.split(".", 1)[0]
    capability = next(
        row["required_capability"]
        for row in flow_v5_catalog()["action_catalog"]
        if row["action_kind"] == action_kind
    )
    action.update(
        {
            "action_id": "typed-action-" + action_kind.replace(".", "-"),
            "action_kind": action_kind,
            "effective_role": role,
            "role_binding_id": f"binding-{role}-1",
            "scope": {
                "workspace_roots": [],
                "operator_ids": [],
                "capabilities": [capability],
            },
        }
    )
    if role == "solver":
        action["scope"] = {
            "workspace_roots": ["operators_workspace/Demo"],
            "operator_ids": ["Demo"],
            "capabilities": [capability],
        }
        action["payload"] = {
            "operator_id": "Demo",
            "candidate_id": "candidate-1",
            "context_id": "context-1",
            "workspace": "operators_workspace/Demo",
        }
    elif role == "tester":
        action["scope"] = {
            "workspace_roots": ["operators_workspace/Demo"],
            "operator_ids": ["Demo"],
            "capabilities": [capability],
        }
        action["payload"] = {
            "operator_id": "Demo",
            "case_id": "case-1",
            "context_id": "context-1",
            "workspace": "operators_workspace/Demo",
        }
    elif action_kind == "manager.request-user-decision":
        action["payload"] = {
            "flow_id": "ascendop",
            "decision_id": "decision-1",
            "prompt": "Select a policy",
            "options": ["continue", "stop"],
        }
    elif action_kind == "manager.review-notification":
        action["payload"] = {
            "notification_id": "notification-1",
            "notification_kind": "external_hold",
            "flow_id": "ascendop",
            "operator_id": "Demo",
            "summary": "endpoint is unavailable",
            "requires_response": False,
            "evidence_refs": [".ascendop-work/runtime/notification-1.json"],
            "allowed_commands": ["manager.review-notification"],
        }
    elif role == "manager":
        action["payload"] = {"flow_id": "ascendop", "reason": "contract test"}
    elif role == "assistant":
        action["scope"] = {
            "workspace_roots": ["operators_workspace/Demo"],
            "operator_ids": ["Demo"],
            "capabilities": [capability],
        }
        payload = {
            "campaign_id": "campaign-1",
            "operator_id": "Demo",
            "candidate_id": "candidate-1",
            "candidate_digest": "a" * 64,
            "official_attempt_id": "official-attempt-1",
            "authorization_id": "authorization-1",
        }
        if action_kind in {"assistant.official-prepare", "assistant.official-submit"}:
            payload.update(
                {
                    "workspace": "operators_workspace/Demo",
                    "runbook_path": "operators/Demo/RUNBOOK.md",
                    "project_digest": "d" * 64,
                    "source_file_digests": {"op_kernel/demo.cpp": "e" * 64},
                    "submit_url": "https://cannjudge.cn/submit",
                    "rules_generation": "rules-1",
                }
            )
        if action_kind in {
            "assistant.official-poll",
            "assistant.official-import-result",
        }:
            payload["submission_id"] = "submission-1"
            payload["official_receipt_id"] = "receipt-1"
        if action_kind == "assistant.official-poll":
            payload.update(
                {
                    "submission_url": "https://cannjudge.cn/submission/1",
                    "project_digest": "d" * 64,
                    "source_file_digests": {"op_kernel/demo.cpp": "e" * 64},
                }
            )
        if action_kind == "assistant.official-import-result":
            payload.update(
                {"result_ref": "results/Demo.json", "result_digest": "b" * 64}
            )
        action["payload"] = payload
    elif action_kind == "developer.repair-capability":
        action["payload"] = {
            "capability_gap_id": "gap-1",
            "capability_code": "evidence.missing-operation",
            "runbook_path": "docs/flow_v5/DEVELOPER_RUNBOOK.md",
            "resume_condition": "operation is registered",
            "gap_context": {
                "source_kind": "agent_outcome",
                "operator_id": "Demo",
                "identity": {"result_version": "Demo_V1_1"},
                "evidence_refs": ["operators_workspace/Demo/RESULT.md"],
                "gate_stage": "needs-capability-repair",
                "next_command": None,
            },
        }
    else:
        action["payload"] = {
            "protocol_generation": "flow-v5-catalog-v4",
            "catalog_digest": "c" * 64,
            "artifact_refs": ["packages/ascendop_protocol/dist/protocol.zip"],
        }
    return action
