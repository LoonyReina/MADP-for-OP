from __future__ import annotations

from ascendop_protocol.agent import render_agent_output_authoring_contract


def test_solver_blocker_authoring_contract_contains_exact_machine_literals() -> None:
    rendered = render_agent_output_authoring_contract(
        [
            {
                "schema": "ascendop.agent-output-contract.v1",
                "output_id": "solver-blocker",
                "output_kind": "solver-blocker",
                "isolated_path": ".ascendop-output/solver-blocker.md",
                "canonical_path": "TestUtils/casegen/Demo/case/case_v001/SOLVER_BLOCKER.md",
                "required": False,
                "must_change": True,
                "max_bytes": 262144,
                "target_before": {"state": "present", "sha256": "a" * 64},
                "identity": {
                    "campaign": "Campaign",
                    "operator": "Demo",
                    "case_version": "case_v001",
                    "result_version": "Demo_V1_8",
                    "diagnostic_contract_revision": "solver-diagnostic-v3",
                    "diagnostic_capability_generation": (
                        "solver-diagnostic-capabilities-v10"
                    ),
                },
            }
        ]
    )

    assert "Status: active" in rendered
    assert "Operator: Demo" in rendered
    assert "Case version: case_v001" in rendered
    assert "Result version: Demo_V1_8" in rendered
    assert "Diagnostic contract revision: solver-diagnostic-v3" in rendered
    assert "Observed signal:" in rendered
    assert "Required evidence:" in rendered
    assert "Diagnostic disposition: durable-hold" in rendered
    assert "Diagnostic disposition: evidence-exhausted" in rendered
    assert "cannot assign a hold to a flow maintainer" in rendered
    assert "Hold owner:" in rendered
    assert "runtime-boundary-trace for case-method/custom-op entry" in rendered
    assert "native-workspace-query-attribution" in rendered
    assert "host-callback-attribution" in rendered
    assert "kernel-fault-attribution" in rendered
    assert "consumed-version-runtime-boundary-replay" in rendered
    assert "pending-reservation-regeneration-execution" in rendered
    assert "already-consumed test version" in rendered
    assert "automatically staged" in rendered
    assert "do not describe it as a durable capability hold" in rendered
    assert "Declare exactly one operation or disposition" in rendered


def test_empty_authoring_contract_forbids_undeclared_non_source_output() -> None:
    rendered = render_agent_output_authoring_contract([])

    assert "No daemon-authorized non-source output slot" in rendered


def test_pending_evidence_authoring_contract_forbids_new_candidate() -> None:
    rendered = render_agent_output_authoring_contract(
        [
            {
                "schema": "ascendop.agent-output-contract.v1",
                "output_id": "pending-evidence-repair",
                "output_kind": "pending-evidence-repair",
                "isolated_path": ".ascendop-output/pending-version.md",
                "canonical_path": "TestUtils/pending/Demo/Demo_V1_11/VERSION.md",
                "required": True,
                "must_change": True,
                "max_bytes": 262144,
                "target_before": {"state": "present", "sha256": "a" * 64},
                "identity": {
                    "campaign": "August",
                    "operator": "Demo",
                    "pending_version": "Demo_V1_11",
                    "execution_source_digest": "b" * 64,
                    "required_knowledge_paths": [
                        "reference/op_knowledge/Demo/case_coverage.md"
                    ],
                },
            }
        ]
    )

    assert "exact current pending VERSION.md" in rendered
    assert "Do not edit op_host/, op_kernel/" in rendered
    assert "Demo_V1_11" in rendered
    assert "daemon re-evaluates the gate" in rendered


def test_diagnostic_authoring_contract_lists_registered_engine_artifacts() -> None:
    rendered = render_agent_output_authoring_contract(
        [
            {
                "schema": "ascendop.agent-output-contract.v1",
                "output_id": "solver-diagnostic-request",
                "output_kind": "solver-diagnostic-request",
                "isolated_path": ".ascendop-output/solver-diagnostic-request.json",
                "canonical_path": "TestUtils/casegen/Demo/case/case_v001/SOLVER_DIAGNOSTIC_REQUEST.json",
                "required": True,
                "must_change": True,
                "max_bytes": 262144,
                "target_before": {"state": "absent"},
                "identity": {
                    "campaign": "Campaign",
                    "operator": "Demo",
                    "case_version": "case_v001",
                    "result_version": "Demo_V1_0",
                    "blocker_generation": "case_v001|Demo_V1_0|1",
                    "operation_kind": "diagnostic-correctness-replay",
                    "target_test_version": "Demo_V1_0",
                    "target_source_sha256": "a" * 64,
                },
            }
        ]
    )

    assert "registered Engine artifact IDs" in rendered
    assert "runtime-readiness" in rendered
    assert "first-failure-traceback" in rendered
    assert "runtime-boundary-trace" in rendered
    assert "native-workspace-query-attribution" in rendered
    assert "kernel-fault-attribution" in rendered
    assert "an Engine adapter and capability are registered" in rendered
    assert "rationale, and created_at" in rendered
    assert "non-empty UTC RFC3339 timestamp" in rendered
    assert "scope.profiler_mode exactly to none" in rendered
    assert "scope.measurement_repetitions to 1" in rendered
    assert "Omit comparison_targets" in rendered
    assert '"max_device_stage_retries":0' in rendered
    assert "requested_device_session_seconds to an integer in [30, 240]" in rendered


def test_candidate_authoring_contract_assigns_pending_ownership_to_daemon() -> None:
    rendered = render_agent_output_authoring_contract(
        [
            {
                "schema": "ascendop.agent-output-contract.v1",
                "output_id": "solver-candidate-proposal",
                "output_kind": "solver-candidate-proposal",
                "isolated_path": ".ascendop-output/candidate-proposal.json",
                "canonical_path": ".ascendop-work/agent-proposals/Demo/key.json",
                "required": False,
                "must_change": False,
                "max_bytes": 262144,
                "target_before": {"state": "absent"},
                "identity": {
                    "campaign": "August",
                    "operator": "Demo",
                    "case_version": "case_v001",
                    "base_version": "Demo_V1_8",
                    "source_before_digest": "a" * 64,
                    "proposal_key": "b" * 64,
                },
            }
        ],
        candidate_version="Demo_V1_9",
    )

    assert "Any change under op_host/ or op_kernel/ requires this output" in rendered
    assert "Revision: ascendop.agent-output-authoring.v2" in rendered
    assert '"candidate_version": "Demo_V1_9"' in rendered
    assert '"base_version": "Demo_V1_8"' in rendered
    for field in (
        "intent",
        "observed_signal",
        "primary_hypothesis",
        "counter_hypothesis",
        "router_gap",
        "consulted_evidence",
        "optimization_method_decision",
        "skill_feedback",
        "shared_knowledge_decision",
        "changed_source",
        "risks",
        "hardware",
        "created_at",
    ):
        assert f'"{field}"' in rendered
    assert "do not add transport metadata such as proposal_key or producer" in rendered
    assert "daemon, not the Agent, creates TestUtils/pending" in rendered
