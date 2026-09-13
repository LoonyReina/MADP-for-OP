"""Synthetic domain ports exercise shared policy, not a second business ledger."""
from copy import deepcopy
from datetime import datetime, timezone

import pytest

from ascendop_control.storage.outbox_repository import enqueue_control_intent
from ascendop_daemon.control_plane.control_database import ControlDatabase
from ascendop_daemon.automation.case_data_policy import CaseDataAdapter, CHANGE_SCHEMA, validate_case_data_change
from ascendop_daemon.automation.external_completion_core import ExternalCompletionCore
from ascendop_daemon.automation.resident_boundary import require_boundary


def database_at(path):
    database = ControlDatabase(path)
    database.initialize()
    return database


def enqueue(database, topic, origin, payload, attempt="original"):
    with database.transaction() as connection:
        return enqueue_control_intent(connection, topic=topic, origin_id=origin,
            attempt_id=attempt, payload=payload, created_at=datetime.now(timezone.utc).isoformat())


def adapter():
    def validate(case):
        if set(case) != {"case_id", "input"} or type(case["input"]) is not int:
            raise ValueError("unknown or invalid input field")
        return case
    return CaseDataAdapter("Synthetic", "data-v1", {}, validate,
        execution_key=lambda case: {"input": case["input"]})


def change(cases, **extra):
    return {"schema": CHANGE_SCHEMA, "base_case_version": "v1", "reason": "Cover a boundary",
            "cases": cases, **extra}


def check(value, previous=None, regressions=()):
    return validate_case_data_change(value, adapter=adapter(), previous_version="v1",
        previous_cases=previous or [{"case_id": 1, "input": -1}], protected_case_ids=[1],
        previous_regression_cases=regressions)


def test_protected_failure_can_move_out_of_active_matrix_but_still_executes():
    result = check(change([{"case_id": 2, "input": 0}], retain_as_regressions=[1]))
    assert result["active_case_ids"] == [2] and result["regression_case_ids"] == [1]
    inherited = check(change([{"case_id": 3, "input": 1}]),
        previous=result["cases"], regressions=result["regression_cases"])
    assert inherited["regression_cases"] == [{"case_id": 1, "input": -1}]


@pytest.mark.parametrize("case", [{"case_id": 2, "input": 0}, {"case_id": 1, "input": 99},
    {"case_id": 1, "input": -1, "tolerance": 100}])
def test_solver_cannot_erase_regression_or_change_oracle(case):
    with pytest.raises(ValueError):
        check(change([case]))


def test_only_trusted_execution_equivalence_can_replace_protected_input():
    mapping = [{"protected_case_id": 1, "replacement_case_id": 2}]
    assert check(change([{"case_id": 2, "input": -1}], equivalent_replacements=mapping))["protected_case_ids"] == [2]
    with pytest.raises(ValueError, match="preserve execution"):
        check(change([{"case_id": 2, "input": 0}], equivalent_replacements=mapping))


def external_fixture(tmp_path):
    database = database_at(tmp_path / "control.db")
    local = {"event": {"request_id": "candidate-1", "outcome": "success"},
        "continuation": {"source_action_id": "solver-1", "source_sha256": "a" * 64,
            "operator": "Synthetic", "kind": "official.wait"}}
    enqueue(database, "test.terminal", "local-1", local)
    event = {"schema": "ascendop.official-control-event.v1", "instance_id": "fixture", "event_id": "feedback-1",
        "payload": {"control_binding": {"schema": "ascendop.official-control-binding.v1",
            "local_event_id": "local-1", "request_id": "candidate-1", "source_action_id": "solver-1"},
            "source_sha256": "a" * 64, "operator_id": "Synthetic",
            "feedback": {"terminal": True, "verdict": "pass"}}}
    def validate_local(binding, payload, intent, saved):
        assert saved == local and binding["request_id"] == "candidate-1"
    def plan(binding, intent, passed):
        return {**intent, "kind": "done" if passed else "solver.repair"}
    def validate_policy(payload):
        # Trusted synthetic evaluator's policy port; no allowance is consumed.
        decision = payload["decision"]
        if decision["reason"] not in {"pre_submission_rejection", "allowance_exhausted"} or decision["external_effect_started"] is not False:
            raise ValueError("not a proven pre-submit policy decision")
    core = ExternalCompletionCore(database, validate_local=validate_local,
        plan_continuation=plan, validate_policy=validate_policy)
    return database, core, event


def test_external_receipt_replays_original_identity_without_rechecking_mutable_inputs(tmp_path):
    database, core, event = external_fixture(tmp_path)
    identity = core.ingest(event)
    core.validate_local = lambda *args: pytest.fail("immutable receipt must replay")
    assert core.ingest(deepcopy(event)) == identity
    assert database.control_outbox(topic="official.terminal")[0]["payload"]["continuation"]["kind"] == "done"
    event["payload"]["feedback"]["verdict"] = "fail"
    with pytest.raises(ValueError, match="replay changed"):
        core.ingest(event)


@pytest.mark.parametrize("reason", ["pre_submission_rejection", "allowance_exhausted"])
def test_policy_feedback_reaches_solver_but_never_establishes_external_pass(tmp_path, reason):
    database, core, event = external_fixture(tmp_path)
    event["payload"].update(purpose="official_submission_decision",
        decision={"reason": reason, "external_effect_started": False})
    core.ingest(event)
    assert database.control_outbox(topic="official.terminal")[0]["payload"]["continuation"]["kind"] == "solver.repair"
    assert len(database.control_outbox(topic="test.terminal")) == 1


@pytest.mark.parametrize("field", ["request_id", "source_action_id", "local_event_id"])
def test_external_feedback_cannot_bind_another_candidate(tmp_path, field):
    database, core, event = external_fixture(tmp_path)
    event["payload"]["control_binding"][field] = "other"
    with pytest.raises(ValueError):
        core.ingest(event)
    assert not database.control_outbox(topic="official.terminal")


def test_external_uncertain_feedback_is_not_terminal(tmp_path):
    database, core, event = external_fixture(tmp_path)
    event["payload"]["feedback"] = {"terminal": False, "verdict": "unknown"}
    with pytest.raises(ValueError, match="incomplete"):
        core.ingest(event)
    assert not database.control_outbox(topic="official.terminal")


def test_resident_boundary_reattaches_only_original_admission(tmp_path):
    database = database_at(tmp_path / "control.db")
    binding = {"action_id": "action", "workspace": "workspace", "revision": 1}
    start = enqueue(database, "agent.native-start", "action", {"carrier": "codex-app-server",
        "binding": binding, "target_id": "session"})
    row = database.claim_control_outbox(topic="agent.native-start", owner="fixture")
    database.finish_control_outbox(outbox_id=start, claim_token=row["claim_token"],
        result={"job_name": "owned-job", "turn_id": "native-turn", "pid": 123, "start_token": "birth"})
    payload = {"binding": binding, "status": "completed", "delivery": {"native_turn_id": "native-turn"},
        "process": {"pid": 123, "start_token": "birth", "observation": "resident-turn-terminal"},
        "writer_quiescence": {"observation": "quiescent", "native_start_id": start, "job_name": "owned-job",
            "session_id": "session", "provider_turn_id": "provider-turn", "status": "completed"}}
    restarted = ControlDatabase(database.path)
    require_boundary(restarted, payload)
    for field, wrong in [("observation", "unknown"), ("job_name", "other"), ("session_id", "other"),
                         ("native_start_id", "other"), ("provider_turn_id", "")]:
        changed = deepcopy(payload)
        changed["writer_quiescence"][field] = wrong
        with pytest.raises(ValueError):
            require_boundary(restarted, changed)
    payload["process"]["start_token"] = "reused-pid"
    with pytest.raises(ValueError):
        require_boundary(restarted, payload)
