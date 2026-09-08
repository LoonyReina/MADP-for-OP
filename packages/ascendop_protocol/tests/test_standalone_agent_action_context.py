from __future__ import annotations

import pytest

from ascendop_protocol.actor import (
    FLOW_V5_OUTPUT_KINDS,
    STANDALONE_AGENT_ACTION_CONTEXT_SCHEMA,
    STANDALONE_AGENT_ACTION_CONTEXT_V1_SCHEMA,
    STANDALONE_AGENT_ACTION_CONTEXT_V2_SCHEMA,
    STANDALONE_AGENT_ACTION_CONTEXT_V3_SCHEMA,
    STANDALONE_AGENT_ACTION_CONTEXT_V4_SCHEMA,
    STANDALONE_AGENT_ACTION_CONTEXT_V5_SCHEMA,
    ActorContractError,
    validate_standalone_agent_action_context,
)
from ascendop_protocol.schemas import load_schema


DIGEST = "a" * 64


def _context(
    role: str,
    schema: str = STANDALONE_AGENT_ACTION_CONTEXT_SCHEMA,
) -> dict[str, object]:
    solver = role == "solver"
    value: dict[str, object] = {
        "schema": schema,
        "context_id": "ctx.demo",
        "action_id": f"p1.Demo.{role}",
        "action_kind": "solver.iterate" if solver else "tester.casegen",
        "effective_role": role,
        "campaign_id": "august-2026",
        "operator_id": "Demo",
        "native_session_id": "00000000-0000-4000-8000-000000000001",
        "contract_revision": DIGEST,
        "workspace": "operators_workspace/Demo",
        "case_path": ".ascendop-work/test-cases/Demo",
        "run_root": ".ascendop-test-runs/august-2026/Demo",
        "outcome_path": ".ascendop-test-runs/august-2026/Demo/actions/p1/OUTCOME.json",
        "task_binding_sha256": DIGEST,
        "input_case_sha256": DIGEST if solver else None,
        "endpoint": (
            {
                "config_path": ".ascendop-work/endpoints/endpoint.json",
                "config_sha256": DIGEST,
                "endpoint_id": "endpoint-a",
            }
            if solver
            else None
        ),
        "release": (
            {"release_id": "Demo_P1", "release_generation": DIGEST}
            if solver
            else None
        ),
        "test": {
            "mode": "correctness",
            "test_version": "Demo_P1_1",
            "case_version": "p1-v1",
            "case_range": "1..9",
            "timeout_seconds": 3600,
        },
        "request": {
            "logical_request_id": "p1-demo-1" if solver else None,
            "idempotency_key": DIGEST,
        },
        "launcher": {
            "python_executable": "C:/Python/python.exe",
            "module": "ascendop_test_gateway.cli",
            "python_path": ["C:/runtime/gateway"],
        },
        "harness": {
            "python_executable": "C:/Python/python.exe",
            "daemon_entrypoint": "C:/runtime/daemon.py",
        },
        "output_contracts": [
            {
                "output_id": "standalone-test-result" if solver else "case-bundle",
                "output_kind": "standalone-test-result" if solver else "case-bundle",
                "artifact_ref": (
                    ".ascendop-test-runs/august-2026/Demo/p1-demo-1/JOURNAL.json"
                    if solver
                    else ".ascendop-work/test-cases/Demo"
                ),
                "required": True,
            }
        ],
        "created_at": "2026-08-23T00:00:00+00:00",
    }
    if schema in {
        STANDALONE_AGENT_ACTION_CONTEXT_V2_SCHEMA,
        STANDALONE_AGENT_ACTION_CONTEXT_V3_SCHEMA,
        STANDALONE_AGENT_ACTION_CONTEXT_V4_SCHEMA,
    }:
        value["input_source_sha256"] = DIGEST
        value["predecessor_evidence"] = []
    if schema in {
        STANDALONE_AGENT_ACTION_CONTEXT_V3_SCHEMA,
        STANDALONE_AGENT_ACTION_CONTEXT_V4_SCHEMA,
    }:
        value["case_bundle"] = {
            "schema": "ascendop.standalone-case-bundle-contract.v1",
            "manifest_path": ".ascendop-work/test-cases/Demo/ASCENDOP_STANDALONE_CASE.json",
            "case_range_source": "manifest",
            "required_paths": ["setup.py", "test_op.py", "extension/custom_op.cpp"],
            "contract_command": "validate Demo",
            "contract_generation": "standalone-case-bundle-v1",
        }
        value["reference_context"] = (
            {
                "schema": "ascendop.solver-reference-context.v1",
                "revision": DIGEST,
                "applicability_policy": "read-only-evidence-revalidate-against-august-cann90-ascend910b",
                "official_task_root": ".ascendop-work/official/Demo",
                "official_cann_references": {
                    "index_path": ".ascendop-work/official/Demo/index.json",
                    "index_sha256": DIGEST,
                },
                "historical_reference": {
                    "manifest_path": "reference/Demo/manifest.json",
                    "manifest_sha256": DIGEST,
                },
                "cann90_api_root": "reference/cann90",
                "op_knowledge": None,
            }
            if solver
            else None
        )
    if schema == STANDALONE_AGENT_ACTION_CONTEXT_V4_SCHEMA:
        value["case_lifecycle"] = (
            None
            if solver
            else {
                "trigger": "initial_case",
                "active_case_version": "p1-v1",
                "active_case_sha256": None,
            }
        )
        value["test"] = {
            "mode": "both",
            "operation_code": "test.performance",
            "test_version": "Demo_P1_1",
            "case_version": "p1-v1",
            "case_range": "1..9",
            "perf_case_range": "1..9",
            "timeout_seconds": 3600,
        }
    return value


def test_standalone_context_is_registered_and_role_bounded() -> None:
    for role in ("solver", "tester"):
        value = _context(role)
        assert validate_standalone_agent_action_context(value) == value
    assert load_schema(STANDALONE_AGENT_ACTION_CONTEXT_SCHEMA)["$id"] == (
        STANDALONE_AGENT_ACTION_CONTEXT_SCHEMA
    )
    legacy = _context("solver", STANDALONE_AGENT_ACTION_CONTEXT_V1_SCHEMA)
    assert validate_standalone_agent_action_context(legacy) == legacy
    assert load_schema(STANDALONE_AGENT_ACTION_CONTEXT_V1_SCHEMA)["$id"] == (
        STANDALONE_AGENT_ACTION_CONTEXT_V1_SCHEMA
    )
    assert {"case-bundle", "standalone-test-result"} <= FLOW_V5_OUTPUT_KINDS


def test_solver_result_slot_can_be_optional_for_detached_execution() -> None:
    value = _context("solver")
    value["output_contracts"][0]["required"] = False

    assert validate_standalone_agent_action_context(value) == value

    value["output_contracts"] = [
        {
            "output_id": "source-change",
            "output_kind": "source-change",
            "artifact_ref": "operators_workspace/Demo",
            "required": False,
        }
    ]
    with pytest.raises(ActorContractError, match="standalone-test-result"):
        validate_standalone_agent_action_context(value)


def test_standalone_context_rejects_solver_without_case_or_endpoint() -> None:
    value = _context("solver")
    value["endpoint"] = None
    with pytest.raises(ActorContractError, match="endpoint"):
        validate_standalone_agent_action_context(value)


def test_v5_accepts_campaign_neutral_correctness_context() -> None:
    value = _context("solver", STANDALONE_AGENT_ACTION_CONTEXT_V3_SCHEMA)
    value["schema"] = STANDALONE_AGENT_ACTION_CONTEXT_V5_SCHEMA
    value["execution_phase"] = "candidate-test"
    value["case_lifecycle"] = None
    value["performance_baseline"] = None
    value["test"]["operation_code"] = "test.correctness"
    value["reference_context"] = {
        "schema": "ascendop.solver-reference-context.v1",
        "revision": DIGEST,
        "applicability_policy": (
            "read-only-evidence-revalidate-against-cann90-ascend910b"
        ),
        "official_task_root": ".ascendop-work/official/Demo",
        "workspace_reference_index": {
            "path": "operators_workspace/Demo/reference/REFERENCE_INDEX.json",
            "sha256": DIGEST,
        },
        "cann90_api_root": "reference/cann90",
        "op_knowledge": None,
    }

    assert validate_standalone_agent_action_context(value) == value
    assert load_schema(STANDALONE_AGENT_ACTION_CONTEXT_V5_SCHEMA)["$id"] == (
        STANDALONE_AGENT_ACTION_CONTEXT_V5_SCHEMA
    )


@pytest.mark.parametrize("trigger", ["initial_case", "solver_requested"])
def test_v5_accepts_solver_owned_initial_case_authoring(trigger) -> None:
    value = _context("solver", STANDALONE_AGENT_ACTION_CONTEXT_V3_SCHEMA)
    value["schema"] = STANDALONE_AGENT_ACTION_CONTEXT_V5_SCHEMA
    value["execution_phase"] = "case-authoring"
    value["input_case_sha256"] = None
    value["endpoint"] = None
    value["release"] = None
    value["performance_baseline"] = None
    value["test"] = None
    value["request"]["logical_request_id"] = None
    value["case_lifecycle"] = {
        "trigger": trigger,
        "active_case_version": "p1-v1",
        "active_case_sha256": None,
    }
    value["reference_context"] = {
        "schema": "ascendop.solver-reference-context.v1",
        "revision": DIGEST,
        "applicability_policy": (
            "read-only-evidence-revalidate-against-cann90-ascend910b"
        ),
        "official_task_root": ".ascendop-work/official/Demo",
        "workspace_reference_index": {
            "path": "operators_workspace/Demo/reference/REFERENCE_INDEX.json",
            "sha256": DIGEST,
        },
        "cann90_api_root": "reference/cann90",
        "op_knowledge": None,
    }
    value["output_contracts"] = [
        {
            "output_id": "case-bundle",
            "output_kind": "case-bundle",
            "artifact_ref": ".ascendop-work/test-cases/Demo",
            "required": True,
        }
    ]

    assert validate_standalone_agent_action_context(value) == value


def test_v5_rejects_case_authoring_with_remote_test_authority() -> None:
    value = _context("solver", STANDALONE_AGENT_ACTION_CONTEXT_V3_SCHEMA)
    value["schema"] = STANDALONE_AGENT_ACTION_CONTEXT_V5_SCHEMA
    value["execution_phase"] = "case-authoring"
    value["case_lifecycle"] = {
        "trigger": "initial_case",
        "active_case_version": "p1-v1",
        "active_case_sha256": None,
    }
    value["reference_context"] = {
        "schema": "ascendop.solver-reference-context.v1",
        "revision": DIGEST,
        "applicability_policy": (
            "read-only-evidence-revalidate-against-cann90-ascend910b"
        ),
        "official_task_root": ".ascendop-work/official/Demo",
        "workspace_reference_index": {
            "path": "operators_workspace/Demo/reference/REFERENCE_INDEX.json",
            "sha256": DIGEST,
        },
        "cann90_api_root": "reference/cann90",
        "op_knowledge": None,
    }

    with pytest.raises(ActorContractError, match="case authoring"):
        validate_standalone_agent_action_context(value)


def test_standalone_context_rejects_tester_remote_authority() -> None:
    value = _context("tester")
    value["release"] = {"release_id": "Demo", "release_generation": DIGEST}
    with pytest.raises(ActorContractError, match="Tester casegen"):
        validate_standalone_agent_action_context(value)


def test_standalone_context_accepts_wrapper_identity_repair_trigger() -> None:
    value = _context("tester")
    value["case_lifecycle"]["trigger"] = "wrapper_identity_mismatch"
    value["action_kind"] = "tester.case-repair"

    assert validate_standalone_agent_action_context(value) == value


def test_legacy_tester_cannot_receive_solver_requested_trigger() -> None:
    value = _context("tester")
    value["case_lifecycle"]["trigger"] = "solver_requested"
    with pytest.raises(ActorContractError, match="unsupported case lifecycle"):
        validate_standalone_agent_action_context(value)


def test_standalone_context_v2_validates_predecessor_identity() -> None:
    value = _context("solver")
    value["predecessor_evidence"] = [
        {
            "action_id": "p1.Demo.tester.previous",
            "action_kind": "tester.case-repair",
            "effective_role": "tester",
            "outcome_ref": ".ascendop-work/actions/previous/OUTCOME.json",
            "outcome_sha256": DIGEST,
            "completed_at": "2026-08-22T00:00:00+00:00",
            "disposition": "proposed_change",
            "summary": "Repaired the executable case wrapper.",
            "evidence_refs": [".ascendop-work/cases/Demo/MANIFEST.json"],
        }
    ]
    assert validate_standalone_agent_action_context(value) == value

    value["predecessor_evidence"][0]["effective_role"] = "solver"
    with pytest.raises(ActorContractError, match="role"):
        validate_standalone_agent_action_context(value)
