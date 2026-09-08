from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

import pytest

from ascendop_control.application import ControlCommandWorker
from ascendop_control.storage import (
    CONTROL_EXTENSION_SQL,
    CONTROL_SCHEMA_VERSION,
    ControlStore,
)
from ascendop_control.storage.errors import ControlRepositoryError
from ascendop_protocol.agent import (
    AGENT_ACTION_RECEIPT_SCHEMA,
    AGENT_ACTION_SCHEMA,
    AGENT_CONTEXT_SNAPSHOT_SCHEMA,
    AGENT_POOL_SCHEMA,
    AGENT_REGISTRATION_SCHEMA,
)
from ascendop_protocol.evidence import (
    EVIDENCE_OPERATION_REQUEST_SCHEMA,
    EVIDENCE_OPERATION_RESULT_SCHEMA,
    evidence_operation_registry,
    evidence_operation_registry_digest,
)


DIGEST = "a" * 64


def test_agent_actions_are_strictly_serial_and_round_robin(tmp_path: Path) -> None:
    store = _store(tmp_path)
    for name, driver in (("a", "codex-cli"), ("b", "claude-code-cli")):
        store.register_agent(_registration(name, driver), lease_seconds=60)
        store.bind_agent(
            operator_id="hard-swish",
            role="solver",
            agent_id=f"{driver}:host-{name}",
        )
    store.create_agent_action(_action("1"), _snapshot("1"))
    store.create_agent_action(_action("2"), _snapshot("2"))

    first = store.claim_agent_action(runner_id="runner", boot_id="boot", lease_seconds=60)
    assert first is not None
    first_agent = first["agent"]["agent_id"]
    assert first_agent in {"codex-cli:host-a", "claude-code-cli:host-b"}
    assert store.claim_agent_action(
        runner_id="runner", boot_id="boot", lease_seconds=60
    ) is None

    store.start_agent_action(
        action_id="action-1",
        lease_token=first["lease"]["lease_token"],
        session_id="session-1",
    )
    store.complete_agent_action(
        _receipt("1", first), lease_token=first["lease"]["lease_token"]
    )
    second = store.claim_agent_action(runner_id="runner", boot_id="boot", lease_seconds=60)
    assert second is not None
    assert {first_agent, second["agent"]["agent_id"]} == {
        "codex-cli:host-a",
        "claude-code-cli:host-b",
    }


def test_uncertain_action_keeps_identity_and_blocks_new_action(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.register_agent(_registration("a", "codex-cli"), lease_seconds=60)
    store.bind_agent(
        operator_id="hard-swish",
        role="solver",
        agent_id="codex-cli:host-a",
    )
    store.create_agent_action(_action("1"), _snapshot("1"))
    store.create_agent_action(_action("2"), _snapshot("2"))
    first = store.claim_agent_action(runner_id="runner", boot_id="boot", lease_seconds=60)
    assert first is not None
    receipt = _receipt("1", first)
    receipt["status"] = "uncertain"
    store.complete_agent_action(receipt, lease_token=first["lease"]["lease_token"])
    assert store.agent_actions_v4()[0]["assigned_agent_id"] == "codex-cli:host-a"
    assert store.claim_agent_action(
        runner_id="runner", boot_id="boot", lease_seconds=60
    ) is None


def test_terminal_iteration_releases_candidate_for_append_only_followup(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    store.register_agent(_registration("a", "codex-cli"), lease_seconds=60)
    first = _action("terminal-first")
    store.create_agent_action(first, _snapshot("terminal-first"))
    claim = store.claim_agent_action(
        runner_id="runner", boot_id="boot", lease_seconds=60
    )
    assert claim is not None
    store.start_agent_action(
        action_id=str(first["action_id"]),
        lease_token=str(claim["lease"]["lease_token"]),
        session_id="session-terminal-first",
    )
    store.complete_agent_action(
        _receipt("terminal-first", claim),
        lease_token=str(claim["lease"]["lease_token"]),
    )
    followup = _action("terminal-followup")
    followup["candidate_version"] = first["candidate_version"]

    created = store.create_agent_action(
        followup,
        _snapshot("terminal-followup"),
    )

    assert created["action_id"] == followup["action_id"]
    with store.connection() as connection:
        rows = connection.execute(
            "SELECT state FROM agent_iterations_v4 WHERE operator_id=? "
            "AND role=? AND candidate_version=? ORDER BY created_at",
            (
                first["operator_id"],
                first["role"],
                first["candidate_version"],
            ),
        ).fetchall()
    assert [row["state"] for row in rows] == ["completed", "queued"]


def test_active_iteration_retains_candidate_reservation(tmp_path: Path) -> None:
    store = _store(tmp_path)
    first = _action("active-first")
    store.create_agent_action(first, _snapshot("active-first"))
    duplicate = _action("active-duplicate")
    duplicate["candidate_version"] = first["candidate_version"]

    with pytest.raises(
        ControlRepositoryError,
        match="candidate already has an active iteration",
    ):
        store.create_agent_action(duplicate, _snapshot("active-duplicate"))


def test_interrupted_ide_turn_enters_central_retry_arbitration(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    store.register_agent(_registration("a", "codex-cli"), lease_seconds=60)
    store.create_agent_action(_action("interrupted"), _snapshot("interrupted"))
    claim = store.claim_agent_action(
        runner_id="runner", boot_id="boot", lease_seconds=60
    )
    assert claim is not None
    receipt = _receipt("interrupted", claim)
    receipt["status"] = "failed"
    receipt["completion"] = {
        "failure_class": "adapter-execution",
        "summary": "exact_codex_ide_turn_interrupted",
        "runner_generation": "codex-ide-task-adapter-v1",
        "changed_paths": [],
        "out_of_scope_paths": [],
    }

    store.complete_agent_action(
        receipt,
        lease_token=str(claim["lease"]["lease_token"]),
    )
    candidates = store.agent_retry_candidates()

    assert len(candidates) == 1
    assert candidates[0]["action_id"] == "action-interrupted"
    assert candidates[0]["failure"]["failure_class"] == "adapter-execution"
    retried = store.apply_agent_retry_decision(
        action_id="action-interrupted",
        attempt_id=str(candidates[0]["current_attempt_id"]),
        decision="retry",
        reason="bounded IDE task retry",
        code_generation="release-2",
        lease_seconds=60,
    )
    assert retried["state"] == "queued"


def test_agent_retry_reuses_lease_id_but_rotates_fencing_token(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    store.register_agent(_registration("retry-fence", "codex-cli"), lease_seconds=60)
    store.create_agent_action(_action("retry-fence"), _snapshot("retry-fence"))
    first = store.claim_agent_action(
        runner_id="runner", boot_id="boot-1", lease_seconds=60
    )
    assert first is not None
    store.defer_agent_action_retry(
        action_id="action-retry-fence",
        lease_token=str(first["lease"]["lease_token"]),
        failure={
            "failure_class": "agent-adapter",
            "runner_generation": "codex-ide-task-adapter-v1",
        },
        lease_seconds=60,
    )
    candidate = store.agent_retry_candidates()[0]
    store.apply_agent_retry_decision(
        action_id="action-retry-fence",
        attempt_id=str(candidate["current_attempt_id"]),
        decision="retry",
        reason="bounded adapter retry",
        code_generation="release-2",
        lease_seconds=60,
    )

    second = store.claim_agent_action(
        runner_id="runner", boot_id="boot-2", lease_seconds=60
    )

    assert second is not None
    assert second["lease"]["lease_id"] == first["lease"]["lease_id"]
    assert second["lease"]["lease_token"] != first["lease"]["lease_token"]
    with pytest.raises(ControlRepositoryError, match="active agent work lease"):
        store.start_agent_action(
            action_id="action-retry-fence",
            lease_token=str(first["lease"]["lease_token"]),
            session_id="turn-stale",
        )


def test_legacy_protocol_output_validation_enters_central_retry_arbitration(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    store.register_agent(_registration("a", "codex-cli"), lease_seconds=60)
    store.create_agent_action(_action("invalid-output"), _snapshot("invalid-output"))
    claim = store.claim_agent_action(
        runner_id="runner", boot_id="boot", lease_seconds=60
    )
    assert claim is not None
    receipt = _receipt("invalid-output", claim)
    receipt["status"] = "failed"
    receipt["completion"] = {
        "failure_class": "protocol",
        "adapter_id": "codex-ide-task-adapter",
        "session_id": "turn-invalid-output",
        "summary": "exact_codex_ide_turn_completed",
        "validation_error": "created_at must be non-empty text",
        "runner_generation": "codex-ide-task-adapter-v1",
    }

    store.complete_agent_action(
        receipt,
        lease_token=str(claim["lease"]["lease_token"]),
    )
    candidates = store.agent_retry_candidates()

    assert len(candidates) == 1
    assert candidates[0]["failure"]["failure_class"] == "agent-output-validation"
    assert candidates[0]["failure"]["legacy_classification"] is True
    retried = store.apply_agent_retry_decision(
        action_id="action-invalid-output",
        attempt_id=str(candidates[0]["current_attempt_id"]),
        decision="retry",
        reason="bounded typed output correction",
        code_generation="release-2",
        lease_seconds=60,
    )
    assert retried["state"] == "queued"
    repair_claim = store.claim_agent_action(
        runner_id="runner", boot_id="repair-boot", lease_seconds=60
    )
    assert repair_claim is not None
    assert repair_claim["attempt_context"] == {
        "attempt_id": str(repair_claim["attempt_id"]),
        "ordinal": 2,
        "mode": "output_repair",
        "history": [
            {
                "attempt_id": str(claim["attempt_id"]),
                "ordinal": 1,
                "state": "failed",
                "failure_class": "agent-output-validation",
                "validation_error": "created_at must be non-empty text",
            }
        ],
        "output_repair": {
            "prior_attempt_id": str(claim["attempt_id"]),
            "prior_attempt_ordinal": 1,
            "failure_class": "agent-output-validation",
            "validation_error": "created_at must be non-empty text",
            "remaining_correction_turns": 1,
        },
    }


def test_global_pool_routes_without_operator_binding_and_filters_capabilities(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    incompatible = _registration("incompatible", "codex-cli")
    incompatible["capabilities"]["structured_output"] = False
    store.register_agent(incompatible, lease_seconds=60)
    store.register_agent(
        _registration("compatible", "claude-code-cli"), lease_seconds=60
    )
    store.create_agent_action(_action("pool"), _snapshot("pool"))

    claim = store.claim_agent_action(
        runner_id="runner", boot_id="boot", lease_seconds=60
    )

    assert claim is not None
    assert claim["agent"]["agent_id"] == "claude-code-cli:host-compatible"


def test_runner_cannot_claim_agent_managed_by_another_runner(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.register_agent(
        _registration("managed", "codex-cli"),
        manager_runner_id="runner-owner",
        lease_seconds=60,
    )
    store.create_agent_action(_action("owned"), _snapshot("owned"))

    assert store.claim_agent_action(
        runner_id="runner-other", boot_id="boot", lease_seconds=60
    ) is None
    claim = store.claim_agent_action(
        runner_id="runner-owner", boot_id="boot", lease_seconds=60
    )
    assert claim is not None
    assert claim["agent"]["manager_runner_id"] == "runner-owner"


def test_gate_head_change_cancels_pre_turn_retry_and_releases_lease(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    store.register_agent(_registration("a", "codex-cli"), lease_seconds=60)
    store.bind_agent(
        operator_id="hard-swish",
        role="solver",
        agent_id="codex-cli:host-a",
    )
    old = _action("old")
    old["candidate_identity"] = {
        "execution_source_digest": DIGEST,
        "origin": "workflow-gate",
    }
    store.create_agent_action(old, _snapshot("old"))
    store.synchronize_workflow_agent_gate_heads(
        [old], observed_at="2026-08-08T00:00:00+00:00"
    )
    claim = store.claim_agent_action(
        runner_id="runner", boot_id="boot", lease_seconds=60
    )
    assert claim is not None
    store.defer_agent_action_retry(
        action_id=str(old["action_id"]),
        lease_token=str(claim["lease"]["lease_token"]),
        failure={
            "failure_class": "agent-preflight",
            "runner_generation": "runner-generation-1",
        },
        lease_seconds=60,
    )

    current = _action("current")
    current["candidate_identity"] = {
        "execution_source_digest": "b" * 64,
        "origin": "workflow-gate",
    }
    store.create_agent_action(current, _snapshot("current"))
    cancelled = store.synchronize_workflow_agent_gate_heads(
        [current], observed_at="2026-08-08T00:01:00+00:00"
    )

    assert cancelled == [old["action_id"]]
    actions = {
        action["action_id"]: action for action in store.agent_actions_v4()
    }
    assert actions[old["action_id"]]["state"] == "cancelled"
    assert actions[current["action_id"]]["state"] == "queued"
    with sqlite3.connect(tmp_path / "control.sqlite3") as connection:
        attempt = connection.execute(
            "SELECT state, details_json FROM agent_action_attempts_v4 "
            "WHERE action_id=?",
            (old["action_id"],),
        ).fetchone()
        lease = connection.execute(
            "SELECT state, released_at FROM agent_work_leases_v4 WHERE action_id=?",
            (old["action_id"],),
        ).fetchone()
        iteration = connection.execute(
            "SELECT state FROM agent_iterations_v4 WHERE iteration_id=?",
            (old["iteration_id"],),
        ).fetchone()
    assert attempt is not None and attempt[0] == "cancelled"
    assert "workflow-gate-no-longer-current" in attempt[1]
    assert lease is not None and lease[0] == "released" and lease[1]
    assert iteration == ("cancelled",)


def test_scoped_gate_sync_preserves_other_head_and_original_running_lease(tmp_path):
    store = _store(tmp_path)
    store.register_agent(_registration("a", "codex-cli"), lease_seconds=60)
    old = _action("outside-scope")
    old["candidate_identity"]["origin"] = "workflow-gate"
    store.create_agent_action(old, _snapshot("outside-scope"))
    store.synchronize_workflow_agent_gate_heads([old], observed_at="before")
    claim = store.claim_agent_action(runner_id="runner", boot_id="boot", lease_seconds=60)
    store.start_agent_action(action_id=old["action_id"], lease_token=claim["lease"]["lease_token"],
                            session_id="original-turn")
    before = store.agent_action(old["action_id"])
    assert store.synchronize_workflow_agent_gate_heads([], observed_at="after", operator_ids={"other"}) == []
    assert store.agent_action(old["action_id"]) == before
    assert store.workflow_agent_action_is_current(old["action_id"])
    receipt = _receipt("outside-scope", claim)
    receipt["completion"]["session_id"] = "original-turn"
    terminal = store.complete_agent_action(receipt, lease_token=claim["lease"]["lease_token"])
    assert terminal["state"] == "completed"
    assert store.agent_action_receipt(old["action_id"])["status"] == "completed"


def test_scoped_gate_sync_only_cancels_owned_queued_actions(tmp_path):
    store = _store(tmp_path)
    outside = _action("outside")
    inside = _action("inside")
    inside["operator_id"] = "other"
    inside_snapshot = _snapshot("inside")
    inside_snapshot["operator_id"] = "other"
    for action, snapshot in ((outside, _snapshot("outside")), (inside, inside_snapshot)):
        action["candidate_identity"]["origin"] = "workflow-gate"
        store.create_agent_action(action, snapshot)
    store.synchronize_workflow_agent_gate_heads([outside, inside], observed_at="before")
    assert store.synchronize_workflow_agent_gate_heads([], observed_at="noop", operator_ids=set()) == []
    assert store.synchronize_workflow_agent_gate_heads([], observed_at="after", operator_ids={"other"}) == [inside["action_id"]]
    assert store.agent_action(outside["action_id"])["state"] == "queued"
    assert store.workflow_agent_action_is_current(outside["action_id"])
    with pytest.raises(ControlRepositoryError, match="outside synchronization scope"):
        store.synchronize_workflow_agent_gate_heads([outside], observed_at="invalid", operator_ids={"other"})


def test_gate_head_change_cancels_running_completion_and_fences_promotion(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    store.register_agent(_registration("a", "codex-cli"), lease_seconds=60)
    old = _action("running-obsolete")
    old["candidate_identity"] = {
        "execution_source_digest": DIGEST,
        "origin": "workflow-gate",
    }
    store.create_agent_action(old, _snapshot("running-obsolete"))
    store.synchronize_workflow_agent_gate_heads(
        [old], observed_at="2026-08-08T00:00:00+00:00"
    )
    claim = store.claim_agent_action(
        runner_id="runner", boot_id="boot", lease_seconds=60
    )
    assert claim is not None
    store.start_agent_action(
        action_id=str(old["action_id"]),
        lease_token=str(claim["lease"]["lease_token"]),
        session_id="turn-running-obsolete",
    )

    cancelled = store.synchronize_workflow_agent_gate_heads(
        [], observed_at="2026-08-08T00:01:00+00:00"
    )
    reported = _receipt("running-obsolete", claim)
    reported["completion"] = {
        "summary": "obsolete turn completed",
        "session_id": "turn-running-obsolete",
        "source_after_digest": "b" * 64,
        "workflow_outputs": [{"output_id": "solver-blocker"}],
    }
    terminal = store.complete_agent_action(
        reported,
        lease_token=str(claim["lease"]["lease_token"]),
    )

    assert cancelled == []
    assert terminal["state"] == "cancelled"
    receipt = store.agent_action_receipt(str(old["action_id"]))
    assert receipt is not None and receipt["status"] == "cancelled"
    assert receipt["completion"]["failure_class"] == "agent-gate-obsolete"
    assert receipt["completion"]["reported_completion"] == reported["completion"]
    with pytest.raises(
        ControlRepositoryError,
        match="workflow gate is obsolete",
    ):
        with store.agent_action_promotion_guard(str(old["action_id"])):
            pass
    with store.obsolete_agent_output_recovery_guard(str(old["action_id"])) as guarded:
        assert guarded["action_id"] == old["action_id"]
    with sqlite3.connect(tmp_path / "control.sqlite3") as connection:
        attempt = connection.execute(
            "SELECT state FROM agent_action_attempts_v4 WHERE action_id=?",
            (old["action_id"],),
        ).fetchone()
        lease = connection.execute(
            "SELECT state FROM agent_work_leases_v4 WHERE action_id=?",
            (old["action_id"],),
        ).fetchone()
    assert attempt == ("cancelled",)
    assert lease == ("released",)


def test_obsolete_completed_tester_case_can_use_explicit_promotion_guard(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    store.register_agent(_registration("a", "codex-cli"), lease_seconds=60)
    old = _action("completed-tester-case")
    old["role"] = "tester"
    old["candidate_identity"] = {
        "execution_source_digest": DIGEST,
        "origin": "workflow-gate",
    }
    old_snapshot = _snapshot("completed-tester-case")
    old_snapshot["role"] = "tester"
    old_snapshot["candidate_identity"] = old["candidate_identity"]
    store.create_agent_action(old, old_snapshot)
    store.synchronize_workflow_agent_gate_heads(
        [old], observed_at="2026-08-08T00:00:00+00:00"
    )
    claim = store.claim_agent_action(
        runner_id="runner", boot_id="boot", lease_seconds=60
    )
    assert claim is not None
    store.start_agent_action(
        action_id=str(old["action_id"]),
        lease_token=str(claim["lease"]["lease_token"]),
        session_id="turn-completed-tester-case",
    )
    store.complete_agent_action(
        _receipt("completed-tester-case", claim),
        lease_token=str(claim["lease"]["lease_token"]),
    )

    store.synchronize_workflow_agent_gate_heads(
        [], observed_at="2026-08-08T00:01:00+00:00"
    )

    with pytest.raises(
        ControlRepositoryError,
        match="workflow gate is obsolete",
    ):
        with store.agent_action_promotion_guard(str(old["action_id"])):
            pass
    with store.agent_action_promotion_guard(
        str(old["action_id"]),
        allow_obsolete_completed_tester_case=True,
    ) as guarded:
        assert guarded["action_id"] == old["action_id"]


def test_exact_published_turn_recovers_legacy_restaging_cancellation(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    store.register_agent(_registration("published", "codex-cli"), lease_seconds=60)
    action = _action("published-cancelled")
    action["candidate_identity"] = {
        "execution_source_digest": DIGEST,
        "origin": "workflow-gate",
    }
    store.create_agent_action(action, _snapshot("published-cancelled"))
    store.synchronize_workflow_agent_gate_heads(
        [action], observed_at="2026-08-08T00:00:00+00:00"
    )
    claim = store.claim_agent_action(
        runner_id="codex-ide-app-relay", boot_id="boot", lease_seconds=60
    )
    assert claim is not None
    turn_id = "turn-published-completed"
    store.start_agent_action(
        action_id=str(action["action_id"]),
        lease_token=str(claim["lease"]["lease_token"]),
        session_id=turn_id,
    )
    receipt = _receipt("published-cancelled", claim)
    receipt["status"] = "cancelled"
    receipt["completion"] = {
        "summary": "candidate source changed before Agent delivery",
        "session_id": "",
        "failure_class": "candidate-superseded",
        "delivery_publish_state": "not-published",
        "expected_source_digest": DIGEST,
        "actual_source_digest": "b" * 64,
    }
    store.complete_agent_action(
        receipt,
        lease_token=str(claim["lease"]["lease_token"]),
    )

    recovered = store.recover_cancelled_published_agent_action(
        action_id=str(action["action_id"]),
        attempt_id=str(claim["attempt_id"]),
        turn_id=turn_id,
        verification="exact-delivered-turn-completed",
        runner_id="codex-ide-app-relay",
        lease_seconds=60,
    )

    assert recovered["attempt_id"] == claim["attempt_id"]
    assert recovered["lease"]["lease_id"] == claim["lease"]["lease_id"]
    assert recovered["session_id"] == turn_id
    assert store.agent_action(str(action["action_id"]))["state"] == "running"
    assert store.agent_action_receipt(str(action["action_id"])) is None
    with sqlite3.connect(tmp_path / "control.sqlite3") as connection:
        attempt = connection.execute(
            "SELECT state, session_id, details_json FROM agent_action_attempts_v4 "
            "WHERE action_id=?",
            (action["action_id"],),
        ).fetchone()
        lease = connection.execute(
            "SELECT state, released_at FROM agent_work_leases_v4 WHERE action_id=?",
            (action["action_id"],),
        ).fetchone()
    assert attempt is not None and attempt[0:2] == ("running", turn_id)
    assert "cancelled_published_recovery" in attempt[2]
    assert lease == ("active", "")


def test_cancelled_agent_recovery_rejects_unpublished_turn(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    store.register_agent(_registration("unpublished", "codex-cli"), lease_seconds=60)
    action = _action("unpublished-cancelled")
    action["candidate_identity"] = {
        "execution_source_digest": DIGEST,
        "origin": "workflow-gate",
    }
    store.create_agent_action(action, _snapshot("unpublished-cancelled"))
    store.synchronize_workflow_agent_gate_heads(
        [action], observed_at="2026-08-08T00:00:00+00:00"
    )
    claim = store.claim_agent_action(
        runner_id="codex-ide-app-relay", boot_id="boot", lease_seconds=60
    )
    assert claim is not None
    receipt = _receipt("unpublished-cancelled", claim)
    receipt["status"] = "cancelled"
    receipt["completion"] = {
        "failure_class": "candidate-superseded",
        "delivery_publish_state": "not-published",
    }
    store.complete_agent_action(
        receipt,
        lease_token=str(claim["lease"]["lease_token"]),
    )

    with pytest.raises(ControlRepositoryError, match="turn identity collision"):
        store.recover_cancelled_published_agent_action(
            action_id=str(action["action_id"]),
            attempt_id=str(claim["attempt_id"]),
            turn_id="turn-not-recorded",
            verification="exact-delivered-turn-completed",
            runner_id="codex-ide-app-relay",
            lease_seconds=60,
        )


def test_role_binding_replacement_disables_previous_executor_atomically(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    old = _registration("old", "codex-cli")
    new = _registration("new", "claude-code-cli")
    store.register_agent(old, lease_seconds=60)
    store.register_agent(new, lease_seconds=60)
    store.bind_agent(
        operator_id="hard-swish",
        role="solver",
        agent_id=str(old["agent_id"]),
    )

    current = store.replace_agent_role_bindings(
        operator_id="hard-swish",
        role="solver",
        bindings=[{"agent_id": new["agent_id"], "enabled": True, "priority": 100}],
    )

    assert current == [
        {
            "agent_id": new["agent_id"],
            "enabled": True,
            "priority": 100,
        }
    ]
    with sqlite3.connect(tmp_path / "control.sqlite3") as connection:
        rows = connection.execute(
            "SELECT agent_id, enabled FROM agent_role_bindings_v4 "
            "WHERE operator_id='hard-swish' AND role='solver' ORDER BY agent_id"
        ).fetchall()
    assert rows == [(new["agent_id"], 1), (old["agent_id"], 0)]


def test_control_command_is_idempotent_and_events_resume(tmp_path: Path) -> None:
    store = _store(tmp_path)
    command = {
        "schema": "ascendop.control-command.v1",
        "command_id": "command-1",
        "idempotency_key": "same-command",
        "command_kind": "endpoint.drain",
        "actor_id": "operator",
        "required_capability": "operator",
        "parameters": {"endpoint_id": "endpoint-a"},
        "created_at": "2026-08-08T00:00:00+00:00",
    }
    first = store.submit_control_command(command)
    second = store.submit_control_command(command)
    assert first["deduplicated"] is False
    assert second["deduplicated"] is True
    events = store.control_events_after(0)
    assert events[-1]["event_type"] == "control-command-created"
    assert store.control_events_after(events[-1]["sequence"]) == []


def test_v5_role_bindings_authorize_one_effective_role_and_scope(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    registration = store.register_agent(
        _registration("manager", "codex-cli"),
        lease_seconds=60,
    )
    manager = _role_binding(
        "manager",
        str(registration["agent_id"]),
        capabilities=["flow-control"],
    )
    assistant = _role_binding(
        "assistant",
        str(registration["agent_id"]),
        capabilities=["official-platform"],
        operator_ids=["hard-swish"],
        workspace_roots=["operators_workspace/HardSwish"],
    )
    store.upsert_role_binding(manager)
    store.upsert_role_binding(assistant)

    assert {
        row["role"] for row in store.role_bindings(native_session_id="shared-session")
    } == {"manager", "assistant"}
    action = _manager_action(manager)
    assert store.authorize_actor_action(action)["effective_role"] == "manager"

    with pytest.raises(ControlRepositoryError, match="exceeds role binding scope"):
        store.authorize_actor_action(
            {
                **action,
                "scope": {
                    **action["scope"],
                    "capabilities": ["flow-control", "user-interaction"],
                },
            }
        )


def test_manager_control_command_requires_authorized_immutable_action(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    registration = store.register_agent(
        _registration("manager", "codex-cli"),
        lease_seconds=60,
    )
    binding = _role_binding(
        "manager",
        str(registration["agent_id"]),
        capabilities=["flow-control"],
    )
    store.upsert_role_binding(binding)
    action = _manager_action(binding)
    command = {
        "schema": "ascendop.control-command.v1",
        "command_id": "manager-command-1",
        "idempotency_key": "manager-command-key-1",
        "command_kind": "manager.flow-pause",
        "actor_id": binding["principal_id"],
        "required_capability": "operator",
        "parameters": action["payload"],
        "actor_action": action,
        "created_at": "2026-08-17T00:00:00+00:00",
    }
    assert store.submit_control_command(command)["state"] == "queued"
    with pytest.raises(ValueError, match="parameters must equal"):
        store.submit_control_command(
            {
                **command,
                "command_id": "manager-command-2",
                "idempotency_key": "manager-command-key-2",
                "parameters": {"flow_id": "ascendop", "reason": "changed"},
            }
        )


def test_control_command_worker_records_one_terminal_receipt(tmp_path: Path) -> None:
    store = _store(tmp_path)
    command = _control_command("worker")
    store.submit_control_command(command)

    class Handler:
        calls = 0

        def handle(self, value: dict[str, object]) -> dict[str, object]:
            self.calls += 1
            return {"handled": value["command_kind"]}

    handler = Handler()
    worker = ControlCommandWorker(store, handler, worker_id="worker-a")
    result = worker.run_once()
    assert result["state"] == "completed"
    assert result["receipt"]["result"] == {"handled": "endpoint.drain"}
    assert handler.calls == 1
    assert worker.run_once()["state"] == "idle"


def test_expired_control_claim_reuses_command_and_rejects_old_token(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    command = _control_command("expiry")
    store.submit_control_command(command)
    first = store.claim_control_command(worker_id="worker-a", lease_seconds=30)
    assert first is not None
    with store.connection() as conn:
        conn.execute(
            "UPDATE control_commands_v4 SET claim_expires_at=? WHERE command_id=?",
            ("2000-01-01T00:00:00+00:00", command["command_id"]),
        )
    second = store.claim_control_command(worker_id="worker-b", lease_seconds=30)
    assert second is not None
    assert second["command"]["command_id"] == command["command_id"]
    assert second["claim_token"] != first["claim_token"]
    receipt = {
        "schema": "ascendop.control-command-receipt.v1",
        "command_id": command["command_id"],
        "status": "completed",
        "result": {},
        "completed_at": "2026-08-08T00:01:00+00:00",
    }
    with pytest.raises(ControlRepositoryError, match="token mismatch"):
        store.complete_control_command(
            receipt,
            claim_token=str(first["claim_token"]),
        )
    store.complete_control_command(receipt, claim_token=str(second["claim_token"]))


def test_runtime_service_retirement_is_boot_identity_fenced(tmp_path: Path) -> None:
    store = _store(tmp_path)
    observed = store.record_runtime_service_heartbeat(
        service_id="service-a",
        role="test",
        code_generation="release-a",
        capabilities=["heartbeat"],
        state="ready",
        boot_id="boot-a",
        details={"pid": 42},
    )

    assert store.retire_runtime_service(
        service_id="service-a",
        expected_boot_id="wrong-boot",
        reason="test",
    ) is False
    assert store.retire_runtime_service(
        service_id="service-a",
        expected_boot_id=str(observed["boot_id"]),
        reason="owner-process-stopped",
    ) is True
    row = next(
        item
        for item in store.runtime_service_health()
        if item["service_id"] == "service-a"
    )
    assert row["state"] == "stopped"
    assert row["live"] is False


def test_evidence_operation_lifecycle_preserves_origin_and_route(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    action = _action("evidence")
    store.create_agent_action(action, _snapshot("evidence"))
    registry = evidence_operation_registry()
    request = {
        "schema": EVIDENCE_OPERATION_REQUEST_SCHEMA,
        "operation_request_id": "eor-hard-swish-profile-1",
        "idempotency_key": "evidence:action-evidence:profile.collect",
        "registry_generation": registry["generation"],
        "registry_digest": evidence_operation_registry_digest(),
        "operation_code": "profile.collect",
        "origin": {
            "action_id": action["action_id"],
            "iteration_id": action["iteration_id"],
            "operator_id": action["operator_id"],
            "role": action["role"],
        },
        "expected_consumer": "solver",
        "parameters": {
            "candidate_id": "candidate-evidence",
            "test_version": action["candidate_version"],
            "case_version": "case-v1",
            "profiler_mode": "primary-all-cases",
        },
        "resume_condition": "profiler evidence is indexed",
        "state": "queued",
        "created_at": "2026-08-19T00:00:00+00:00",
    }

    created = store.create_evidence_operation_request(request)
    duplicate = store.create_evidence_operation_request(request)
    assert created["operation_request_id"] == duplicate["operation_request_id"]
    assert created["origin_action_id"] == action["action_id"]
    assert created["executor"] == "test-engine"

    claim = store.claim_evidence_operation(
        executor="test-engine",
        consumer_id="diagnostic-intake",
        lease_seconds=60,
    )
    assert claim is not None
    assert claim["state"] == "claimed"
    routed = store.route_evidence_operation(
        operation_request_id=request["operation_request_id"],
        claim_token=claim["claim"]["claim_token"],
        test_request_id="test-request-1",
        wire_attempt_id="wire-attempt-1",
        endpoint_id="endpoint-1",
        execution_environment_id="environment-1",
    )
    assert routed["state"] == "routed"
    assert store.evidence_operation_for_test_request("test-request-1") == routed

    result = {
        "schema": EVIDENCE_OPERATION_RESULT_SCHEMA,
        "operation_result_id": "eors-hard-swish-profile-1",
        "operation_request_id": request["operation_request_id"],
        "registry_generation": request["registry_generation"],
        "registry_digest": request["registry_digest"],
        "operation_code": request["operation_code"],
        "origin": request["origin"],
        "expected_consumer": request["expected_consumer"],
        "status": "completed",
        "execution": routed["route"],
        "evidence": {
            "evidence_type": "profiler-trace",
            "artifact_refs": ["operators_testresult/HardSwish/profile.json"],
            "payload": {"case_count": 4},
        },
        "summary": "profile collected",
        "failure_class": None,
        "completed_at": "2026-08-19T00:10:00+00:00",
    }
    assert store.complete_evidence_operation(result) == result
    assert store.complete_evidence_operation(result) == result
    assert store.evidence_operation_result(request["operation_request_id"]) == result
    origin = store.evidence_operations_for_origin(action_id=str(action["action_id"]))
    assert [item["state"] for item in origin] == ["completed"]


def test_evidence_operation_rejects_changed_origin_and_idempotency(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    action = _action("evidence-collision")
    store.create_agent_action(action, _snapshot("evidence-collision"))
    registry = evidence_operation_registry()
    request = {
        "schema": EVIDENCE_OPERATION_REQUEST_SCHEMA,
        "operation_request_id": "eor-collision",
        "idempotency_key": "evidence:collision",
        "registry_generation": registry["generation"],
        "registry_digest": evidence_operation_registry_digest(),
        "operation_code": "environment.conformance",
        "origin": {
            "action_id": action["action_id"],
            "iteration_id": action["iteration_id"],
            "operator_id": action["operator_id"],
            "role": action["role"],
        },
        "expected_consumer": "solver",
        "parameters": {
            "endpoint_id": "endpoint-1",
            "execution_environment_id": "environment-1",
            "requirements_generation": "requirements-1",
        },
        "resume_condition": "environment conforms",
        "state": "queued",
        "created_at": "2026-08-19T00:00:00+00:00",
    }
    store.create_evidence_operation_request(request)
    changed = {**request, "operation_request_id": "eor-collision-other"}
    changed["parameters"] = {**request["parameters"], "endpoint_id": "endpoint-2"}
    with pytest.raises(ControlRepositoryError, match="idempotency collision"):
        store.create_evidence_operation_request(changed)
    wrong_origin = {**request, "idempotency_key": "evidence:wrong-origin"}
    wrong_origin["operation_request_id"] = "eor-wrong-origin"
    wrong_origin["origin"] = {**request["origin"], "iteration_id": "other"}
    with pytest.raises(ControlRepositoryError, match="origin identity mismatch"):
        store.create_evidence_operation_request(wrong_origin)


def test_evidence_operation_defer_requeues_the_same_identity(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    action = _action("evidence-defer")
    store.create_agent_action(action, _snapshot("evidence-defer"))
    registry = evidence_operation_registry()
    request = {
        "schema": EVIDENCE_OPERATION_REQUEST_SCHEMA,
        "operation_request_id": "eor-defer",
        "idempotency_key": "evidence:defer",
        "registry_generation": registry["generation"],
        "registry_digest": evidence_operation_registry_digest(),
        "operation_code": "environment.conformance",
        "origin": {
            "action_id": action["action_id"],
            "iteration_id": action["iteration_id"],
            "operator_id": action["operator_id"],
            "role": action["role"],
        },
        "expected_consumer": "solver",
        "parameters": {
            "endpoint_id": "endpoint-1",
            "execution_environment_id": "environment-1",
            "requirements_generation": "requirements-1",
        },
        "resume_condition": "environment conforms",
        "state": "queued",
        "created_at": "2026-08-19T00:00:00+00:00",
    }
    store.create_evidence_operation_request(request)
    first = store.claim_evidence_operation(
        executor="endpoint-registry",
        consumer_id="evidence-intake",
        lease_seconds=60,
    )
    assert first is not None

    deferred = store.defer_evidence_operation(
        operation_request_id="eor-defer",
        claim_token=first["claim"]["claim_token"],
        delay_seconds=0,
        failure_class="endpoint-unavailable",
    )
    assert deferred["state"] == "claimed"
    assert deferred["claim"]["claim_token"] == ""

    second = store.claim_evidence_operation(
        executor="endpoint-registry",
        consumer_id="evidence-intake",
        lease_seconds=60,
    )
    assert second is not None
    assert second["operation_request_id"] == "eor-defer"
    assert second["claim"]["attempts"] == 2


def _store(tmp_path: Path) -> ControlStore:
    path = tmp_path / "control.sqlite3"
    conn = sqlite3.connect(path)
    conn.executescript(
        "CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);"
        f"INSERT INTO metadata(key,value) VALUES('schema_version','{CONTROL_SCHEMA_VERSION}');"
        "CREATE TABLE control_events(sequence INTEGER PRIMARY KEY AUTOINCREMENT,"
        "event_at TEXT NOT NULL,event_type TEXT NOT NULL,entity_type TEXT NOT NULL,"
        "entity_id TEXT NOT NULL,payload_json TEXT NOT NULL);"
        "CREATE TABLE scheduler_state(scheduler_id TEXT PRIMARY KEY,"
        "revision INTEGER NOT NULL,state_json TEXT NOT NULL,"
        "updated_at TEXT NOT NULL);"
        + CONTROL_EXTENSION_SQL
    )
    conn.commit()
    conn.close()
    store = ControlStore(path)
    store.assert_compatible()
    store.register_agent_pool(_pool())
    return store


def _pool() -> dict[str, object]:
    return {
        "schema": AGENT_POOL_SCHEMA,
        "pool_id": "local-source-agents",
        "enabled": True,
        "roles": ["solver", "tester"],
        "drivers": ["codex-cli", "claude-code-cli", "kimi-code-cli"],
        "required_capabilities": {
            "stream_json": True,
            "structured_output": True,
        },
        "priority": 100,
        "registration_generation": "test-pool-1",
    }


def _control_command(suffix: str) -> dict[str, object]:
    return {
        "schema": "ascendop.control-command.v1",
        "command_id": f"command-{suffix}",
        "idempotency_key": f"command-key-{suffix}",
        "command_kind": "endpoint.drain",
        "actor_id": "operator",
        "required_capability": "operator",
        "parameters": {"endpoint_id": "endpoint-a", "reason": "test"},
        "created_at": "2026-08-08T00:00:00+00:00",
    }


def _registration(name: str, driver: str) -> dict[str, object]:
    return {
        "schema": AGENT_REGISTRATION_SCHEMA,
        "agent_id": f"{driver}:host-{name}",
        "driver": driver,
        "executable": f"C:/tools/{driver}.exe",
        "executable_digest": hashlib.sha256(name.encode()).hexdigest(),
        "observed_version": "1.0",
        "registration_generation": f"generation-{name}",
        "capabilities": {
            "stream_json": True,
            "resume": True,
            "structured_output": True,
        },
        "observed_at": "2026-08-08T00:00:00+00:00",
    }


def _role_binding(
    role: str,
    agent_id: str,
    *,
    capabilities: list[str],
    operator_ids: list[str] | None = None,
    workspace_roots: list[str] | None = None,
) -> dict[str, object]:
    return {
        "schema": "ascendop.role-binding.v1",
        "role_binding_id": f"binding-{role}-1",
        "principal_id": "ascendop-system-assistant",
        "agent_registration_id": agent_id,
        "native_session_id": "shared-session",
        "role": role,
        "scope": {
            "workspace_roots": workspace_roots or [],
            "operator_ids": operator_ids or [],
            "capabilities": capabilities,
        },
        "generation": "binding-generation-1",
        "state": "active",
        "valid_from": "2020-01-01T00:00:00+00:00",
        "valid_until": "2099-01-01T00:00:00+00:00",
    }


def _manager_action(binding: dict[str, object]) -> dict[str, object]:
    return {
        "schema": "ascendop.actor-action-envelope.v1",
        "action_id": "manager-action-1",
        "idempotency_key": "manager.flow-pause:ascendop:1",
        "action_kind": "manager.flow-pause",
        "effective_role": "manager",
        "principal_id": binding["principal_id"],
        "role_binding_id": binding["role_binding_id"],
        "producer_generation": "flow-v5-catalog-v2",
        "scope": {
            "workspace_roots": [],
            "operator_ids": [],
            "capabilities": ["flow-control"],
        },
        "lease": {
            "lease_id": "manager-lease-1",
            "generation": "manager-lease-generation-1",
            "expires_at": "2099-01-01T00:00:00+00:00",
        },
        "causation": {
            "trace_id": "trace-manager-1",
            "correlation_id": "flow-ascendop",
            "parent_action_id": None,
            "candidate_id": None,
            "promotion_receipt_id": None,
            "request_id": None,
            "attempt_id": None,
        },
        "payload": {"flow_id": "ascendop", "reason": "test pause"},
        "created_at": "2026-08-17T00:00:00+00:00",
    }


def _action(suffix: str) -> dict[str, object]:
    return {
        "schema": AGENT_ACTION_SCHEMA,
        "action_id": f"action-{suffix}",
        "idempotency_key": f"idempotency-{suffix}",
        "iteration_id": f"iteration-{suffix}",
        "campaign": "august",
        "operator_id": "hard-swish",
        "agent_pool_id": "local-source-agents",
        "role": "solver",
        "workflow_epoch": "epoch-1",
        "producer_generation": "release-1",
        "board_revision": f"board-{suffix}",
        "board_digest": DIGEST,
        "runbook_path": "operators/august/HardSwish/RUNBOOK.md",
        "runbook_digest": DIGEST,
        "origin_workspace": "operators_workspace/HardSwish",
        "candidate_version": f"HardSwish_V1_{suffix}",
        "candidate_identity": {"execution_source_digest": DIGEST},
        "write_scope": ["op_kernel/hard_swish.cpp"],
        "output_contracts": [],
        "tool_budget": {"max_turn_seconds": 900},
        "created_at": "2026-08-08T00:00:00+00:00",
    }


def _snapshot(suffix: str) -> dict[str, object]:
    return {
        "schema": AGENT_CONTEXT_SNAPSHOT_SCHEMA,
        "snapshot_id": f"snapshot-{suffix}",
        "iteration_id": f"iteration-{suffix}",
        "operator_id": "hard-swish",
        "role": "solver",
        "board_revision": f"board-{suffix}",
        "board_digest": DIGEST,
        "candidate_identity": {"execution_source_digest": DIGEST},
        "gate": {"owner": "solver"},
        "recent_results": [],
        "official_evidence": [],
        "open_hypotheses": [],
        "permitted_operations": ["edit-source"],
        "created_at": "2026-08-08T00:00:00+00:00",
    }


def _receipt(suffix: str, claim: dict[str, object]) -> dict[str, object]:
    lease = claim["lease"]
    agent = claim["agent"]
    assert isinstance(lease, dict)
    assert isinstance(agent, dict)
    return {
        "schema": AGENT_ACTION_RECEIPT_SCHEMA,
        "action_id": f"action-{suffix}",
        "iteration_id": f"iteration-{suffix}",
        "agent_id": agent["agent_id"],
        "lease_id": lease["lease_id"],
        "status": "completed",
        "started_at": "2026-08-08T00:00:00+00:00",
        "completed_at": "2026-08-08T00:01:00+00:00",
        "completion": {"source_after_digest": "b" * 64},
        "artifacts": [".ascendop-work/agent-runs/action-1/events.jsonl"],
    }
