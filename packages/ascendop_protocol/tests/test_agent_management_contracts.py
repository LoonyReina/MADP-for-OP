from __future__ import annotations

import ascendop_protocol

import pytest

from ascendop_protocol.agent import (
    ACTION_STATES,
    AGENT_ACTION_SCHEMA,
    AGENT_CONTEXT_SNAPSHOT_SCHEMA,
    AGENT_OUTPUT_CONTRACT_SCHEMA,
    AGENT_POOL_SCHEMA,
    AGENT_REGISTRATION_SCHEMA,
    AGENT_TURN_DELIVERY_SCHEMA,
    AGENT_WORK_LEASE_SCHEMA,
    AgentContractError,
    validate_agent_action,
    validate_agent_context_snapshot,
    validate_agent_output_contract,
    validate_solver_candidate_proposal,
    validate_agent_pool,
    validate_agent_registration,
    validate_agent_turn_delivery,
)
from ascendop_protocol.management import (
    CONTROL_COMMAND_SCHEMA,
    ManagementContractError,
    validate_control_command,
)
from ascendop_protocol.schemas import load_schema, schema_registry


DIGEST = "a" * 64


def test_agent_action_lifecycle_includes_internal_retry_state() -> None:
    assert ACTION_STATES == {
        "queued",
        "claimed",
        "running",
        "uncertain",
        "retry-pending",
        "completed",
        "failed",
        "cancelled",
    }


def test_agent_contracts_are_registered_and_validate() -> None:
    registration = {
        "schema": AGENT_REGISTRATION_SCHEMA,
        "agent_id": "codex-cli:host-a",
        "driver": "codex-cli",
        "executable": "C:/tools/codex.exe",
        "executable_digest": DIGEST,
        "observed_version": "codex-cli 1.0",
        "registration_generation": DIGEST,
        "capabilities": {
            "stream_json": True,
            "resume": True,
            "structured_output": True,
        },
        "observed_at": "2026-08-08T00:00:00+00:00",
    }
    assert validate_agent_registration(registration) == registration
    pool = {
        "schema": AGENT_POOL_SCHEMA,
        "pool_id": "local-source-agents",
        "enabled": True,
        "roles": ["solver", "tester"],
        "drivers": ["codex-cli", "claude-code-cli"],
        "required_capabilities": {"structured_output": True},
        "priority": 100,
        "registration_generation": "pool-generation-1",
    }
    assert validate_agent_pool(pool) == pool

    action = _action()
    snapshot = _snapshot()
    assert validate_agent_action(action) == action
    assert validate_agent_context_snapshot(snapshot) == snapshot
    delivery = {
        "schema": AGENT_TURN_DELIVERY_SCHEMA,
        "delivery_id": "delivery-1",
        "delivery_key": "act-1:attempt-1",
        "adapter_id": "codex-ide-task-adapter",
        "adapter_generation": "codex-ide-task-adapter-v1",
        "target_kind": "codex-ide-task",
        "target_id": "task-1",
        "workspace": ".ascendop-work/agent-runs/act-1/workspace",
        "prompt": "perform the immutable action",
        "prompt_digest": DIGEST,
        "action": action,
        "context": snapshot,
        "lease": {
            "schema": AGENT_WORK_LEASE_SCHEMA,
            "lease_id": "lease-1",
            "lease_token": "secret",
            "action_id": "act-1",
            "iteration_id": "iter-1",
            "operator_id": "hard-swish",
            "role": "solver",
            "agent_id": "codex-ide.solver.1",
            "state": "active",
            "acquired_at": "2026-08-08T00:00:00+00:00",
            "heartbeat_at": "2026-08-08T00:00:00+00:00",
            "expires_at": "2026-08-08T00:02:00+00:00",
        },
        "agent": {"agent_id": "codex-ide.solver.1", "driver": "codex-ide-task"},
        "created_at": "2026-08-08T00:00:00+00:00",
    }
    assert validate_agent_turn_delivery(delivery) == delivery

    ids = {entry["schema_id"] for entry in schema_registry()["entries"]}
    for schema_id in (
        AGENT_REGISTRATION_SCHEMA,
        AGENT_POOL_SCHEMA,
        AGENT_ACTION_SCHEMA,
        AGENT_OUTPUT_CONTRACT_SCHEMA,
        AGENT_CONTEXT_SNAPSHOT_SCHEMA,
        AGENT_TURN_DELIVERY_SCHEMA,
        CONTROL_COMMAND_SCHEMA,
    ):
        assert schema_id in ids
        assert load_schema(schema_id)["$id"] == schema_id


def test_agent_output_contract_is_bounded_and_typed() -> None:
    contract = {
        "schema": AGENT_OUTPUT_CONTRACT_SCHEMA,
        "output_id": "solver-blocker",
        "output_kind": "solver-blocker",
        "isolated_path": ".ascendop-output/solver-blocker.md",
        "canonical_path": "TestUtils/casegen/HardSwish/case/case_v001/SOLVER_BLOCKER.md",
        "required": False,
        "must_change": False,
        "max_bytes": 262144,
        "target_before": {"state": "absent", "sha256": ""},
        "identity": {
            "operator": "HardSwish",
            "case_version": "case_v001",
            "result_version": "HardSwish_V1_1",
        },
    }

    assert validate_agent_output_contract(contract) == contract
    invalid = dict(contract, isolated_path="../solver-blocker.md")
    with pytest.raises(AgentContractError, match="bounded relative path"):
        validate_agent_output_contract(invalid)

    schema = load_schema(AGENT_OUTPUT_CONTRACT_SCHEMA)
    assert "solver-candidate-proposal" in schema["properties"]["output_kind"]["enum"]
    assert schema["properties"]["target_before"]["required"] == ["state"]


def test_diagnostic_schema_matches_runtime_optional_controls() -> None:
    schema = load_schema("ascendop.solver-diagnostic-request.v1")

    assert schema["properties"]["comparison_targets"]["maxItems"] == 3
    scope = schema["properties"]["scope"]["properties"]
    assert scope["measurement_repetitions"]["maximum"] == 5
    assert "same-endpoint-environment-device" in scope["comparison_affinity"]["enum"]
    assert scope["expected_block_dims"]["propertyNames"]["pattern"] == "^[1-9][0-9]*$"


def test_solver_candidate_proposal_is_typed_and_evidence_bearing() -> None:
    proposal = {
        "schema": "ascendop.solver-candidate-proposal.v1",
        "campaign": "August",
        "operator": "HardSwish",
        "candidate_version": "HardSwish_V1_9",
        "case_version": "case_v001",
        "base_version": "HardSwish_V1_8",
        "source_before_digest": "a" * 64,
        "intent": "test one MTE optimization",
        "observed_signal": "MTE2 dominates case16",
        "primary_hypothesis": "looped L1 copy is the bottleneck",
        "counter_hypothesis": "cube issue latency dominates",
        "router_gap": "none",
        "consulted_evidence": ["RESULT.md", "PROFILER_EVIDENCE_INDEX.json"],
        "optimization_method_decision": "disable looped copy mode",
        "skill_feedback": "profiler route was useful",
        "shared_knowledge_decision": "operator-local until reproduced",
        "changed_source": ["op_kernel/demo.h"],
        "risks": {
            "correctness": "low",
            "performance": "targeted",
            "infrastructure": "none",
        },
        "hardware": "910B4",
        "created_at": "2026-08-11T00:00:00+00:00",
    }

    assert validate_solver_candidate_proposal(proposal) == proposal
    proposal["consulted_evidence"] = []
    with pytest.raises(ValueError, match="must not be empty"):
        validate_solver_candidate_proposal(proposal)


def test_agent_contract_rejects_unbounded_paths_and_unknown_driver() -> None:
    action = _action()
    action["origin_workspace"] = "../outside"
    with pytest.raises(AgentContractError, match="bounded relative path"):
        validate_agent_action(action)

    registration = {
        "schema": AGENT_REGISTRATION_SCHEMA,
        "agent_id": "unknown:host-a",
        "driver": "unknown",
        "executable": "tool",
        "executable_digest": DIGEST,
        "observed_version": "1",
        "registration_generation": DIGEST,
        "capabilities": {
            "stream_json": True,
            "resume": True,
            "structured_output": True,
        },
        "observed_at": "now",
    }
    with pytest.raises(AgentContractError, match="unsupported agent driver"):
        validate_agent_registration(registration)


def test_management_commands_are_allowlisted() -> None:
    command = {
        "schema": CONTROL_COMMAND_SCHEMA,
        "command_id": "cmd-1",
        "idempotency_key": "key-1",
        "command_kind": "endpoint.drain",
        "actor_id": "operator-user",
        "required_capability": "operator",
        "parameters": {"endpoint_id": "endpoint-a"},
        "created_at": "2026-08-08T00:00:00+00:00",
    }
    assert validate_control_command(command) == command
    command["command_kind"] = "shell.exec"
    with pytest.raises(ManagementContractError, match="unsupported control command"):
        validate_control_command(command)


def _action() -> dict[str, object]:
    return {
        "schema": AGENT_ACTION_SCHEMA,
        "action_id": "act-1",
        "idempotency_key": "idem-1",
        "iteration_id": "iter-1",
        "campaign": "august",
        "operator_id": "hard-swish",
        "agent_pool_id": "local-source-agents",
        "role": "solver",
        "workflow_epoch": "epoch-1",
        "producer_generation": "release-1",
        "board_revision": "board-1",
        "board_digest": DIGEST,
        "runbook_path": "operators/august/HardSwish/RUNBOOK.md",
        "runbook_digest": DIGEST,
        "origin_workspace": "operators_workspace/HardSwish",
        "candidate_version": "HardSwish_V1_1",
        "candidate_identity": {"execution_source_digest": DIGEST},
        "write_scope": ["op_kernel/hard_swish.cpp"],
        "output_contracts": [],
        "tool_budget": {"max_turn_seconds": 900},
        "created_at": "2026-08-08T00:00:00+00:00",
    }


def _snapshot() -> dict[str, object]:
    return {
        "schema": AGENT_CONTEXT_SNAPSHOT_SCHEMA,
        "snapshot_id": "snap-1",
        "iteration_id": "iter-1",
        "operator_id": "hard-swish",
        "role": "solver",
        "board_revision": "board-1",
        "board_digest": DIGEST,
        "candidate_identity": {"execution_source_digest": DIGEST},
        "gate": {"owner": "solver"},
        "recent_results": [],
        "official_evidence": [],
        "open_hypotheses": [],
        "permitted_operations": ["edit-source", "complete"],
        "created_at": "2026-08-08T00:00:00+00:00",
    }
def test_top_level_agent_export_remains_lazy_compatible() -> None:
    assert ascendop_protocol.AGENT_ACTION_SCHEMA == "ascendop.agent-action.v1"
