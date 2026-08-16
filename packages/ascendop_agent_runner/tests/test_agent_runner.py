from __future__ import annotations

import hashlib
import json
import socket
import sqlite3
from pathlib import Path

from ascendop_agent_runner.drivers import AgentDriver, DriverProbe, DriverResult
from ascendop_agent_runner.runner import AgentRunner
from ascendop_control.storage import CONTROL_EXTENSION_SQL, ControlStore
from ascendop_protocol.agent import (
    AGENT_ACTION_SCHEMA,
    AGENT_CONTEXT_SNAPSHOT_SCHEMA,
    AGENT_POOL_SCHEMA,
)


class FakeCodexDriver:
    driver_id = "codex-cli"

    def probe(self) -> DriverProbe:
        return DriverProbe(
            driver=self.driver_id,
            available=True,
            executable="C:/fake/codex.exe",
            executable_digest="c" * 64,
            version="codex-cli test",
            capabilities={
                "stream_json": True,
                "resume": True,
                "structured_output": True,
            },
        )

    def start(
        self,
        *,
        prompt: str,
        workspace: Path,
        run_root: Path,
        timeout_seconds: int,
        heartbeat,
        resume_session_id: str = "",
    ) -> DriverResult:
        assert "FLOW V4 ACTION" in prompt
        assert "AGENT OUTPUT AUTHORING CONTRACT" in prompt
        heartbeat()
        run_root.mkdir(parents=True, exist_ok=True)
        (workspace / "candidate.txt").write_text("changed\n", encoding="ascii")
        raw = run_root / "events.jsonl"
        stderr = run_root / "stderr.log"
        raw.write_text('{"status":"completed"}\n', encoding="ascii")
        stderr.write_text("", encoding="ascii")
        return DriverResult(
            status="completed",
            session_id="fake-session",
            exit_code=0,
            completion={"status": "completed", "summary": "edited isolated copy"},
            raw_output_path=raw,
            stderr_path=stderr,
        )

    def observe(self, result: DriverResult):
        return {"status": result.status}

    def heartbeat(self, session_id: str):
        return {"session_id": session_id}

    def cancel(self, session_id: str):
        return {"session_id": session_id}

    def resume(self, **kwargs):
        return self.start(**kwargs)

    def collect(self, result: DriverResult):
        return dict(result.completion)


class RogueCodexDriver(FakeCodexDriver):
    def start(self, **kwargs) -> DriverResult:
        result = super().start(**kwargs)
        kwargs["workspace"].joinpath("rogue.txt").write_text("rogue\n", encoding="ascii")
        return result


class ExplodingCodexDriver(FakeCodexDriver):
    def start(self, **kwargs) -> DriverResult:
        raise RuntimeError("driver-start-exploded")


class AdapterFailDriver(FakeCodexDriver):
    def start(self, **kwargs) -> DriverResult:
        run_root = kwargs["run_root"]
        run_root.mkdir(parents=True, exist_ok=True)
        raw = run_root / "events.jsonl"
        stderr = run_root / "stderr.log"
        raw.write_text('{"type":"turn.failed"}\n', encoding="ascii")
        stderr.write_text("invalid adapter arguments\n", encoding="ascii")
        return DriverResult(
            status="failed",
            session_id="thread-adapter-failure",
            exit_code=2,
            completion={
                "status": "failed",
                "failure_class": "agent-adapter",
                "exit_code": 2,
                "error": "invalid adapter arguments",
            },
            raw_output_path=raw,
            stderr_path=stderr,
        )


class AuthFailDriver(FakeCodexDriver):
    def start(self, **kwargs) -> DriverResult:
        run_root = kwargs["run_root"]
        run_root.mkdir(parents=True, exist_ok=True)
        raw = run_root / "events.jsonl"
        stderr = run_root / "stderr.log"
        raw.write_text(
            '{"subtype":"api_retry","error_status":401}\n', encoding="ascii"
        )
        stderr.write_text("authentication failed\n", encoding="ascii")
        return DriverResult(
            status="failed",
            session_id="auth-session",
            exit_code=1,
            completion={
                "status": "failed",
                "failure_class": "agent-auth",
                "error_status": 401,
                "error": "authentication failed",
            },
            raw_output_path=raw,
            stderr_path=stderr,
        )


class ClaudeAuthFailDriver(AuthFailDriver):
    driver_id = "claude-code-cli"


class UncertainThenResumeDriver(FakeCodexDriver):
    def start(self, **kwargs) -> DriverResult:
        value = super().start(**kwargs)
        completion = dict(value.completion)
        completion["status"] = "uncertain"
        completion["session_id"] = "resumable-session"
        return DriverResult(
            status="uncertain",
            session_id="resumable-session",
            exit_code=value.exit_code,
            completion=completion,
            raw_output_path=value.raw_output_path,
            stderr_path=value.stderr_path,
        )

    def resume(self, **kwargs) -> DriverResult:
        kwargs.pop("session_id")
        value = FakeCodexDriver.start(self, **kwargs)
        completion = dict(value.completion)
        completion["session_id"] = "resumable-session"
        return DriverResult(
            status=value.status,
            session_id="resumable-session",
            exit_code=value.exit_code,
            completion=completion,
            raw_output_path=value.raw_output_path,
            stderr_path=value.stderr_path,
        )


def test_runner_executes_in_isolated_workspace_and_records_iteration(
    tmp_path: Path,
) -> None:
    root = tmp_path / "root"
    source = root / "operators_workspace" / "HardSwish"
    source.mkdir(parents=True)
    (source / "candidate.txt").write_text("original\n", encoding="ascii")
    runtime_result = (
        source
        / ".ascendop"
        / "results"
        / "request-history"
        / "attempt-history"
        / "artifacts"
    )
    runtime_result.mkdir(parents=True)
    (runtime_result / "RESULT.json").write_text("{}\n", encoding="ascii")
    runbook = root / "operators" / "august" / "HardSwish" / "RUNBOOK.md"
    runbook.parent.mkdir(parents=True)
    runbook.write_text("Edit the candidate source.", encoding="ascii")
    database_path = root / ".ascendop-work" / "runtime" / "control.sqlite3"
    store = _store(database_path)
    runner = AgentRunner(
        root=root,
        database=database_path,
        drivers=(FakeCodexDriver(),),
        runner_id="runner-test",
    )
    registered = runner.probe_and_register()
    assert registered[0]["registered"] is True
    agent_id = f"codex-cli:{socket.gethostname()}"
    store.bind_agent(
        operator_id="hard-swish",
        role="solver",
        agent_id=agent_id,
    )
    runbook_digest = hashlib.sha256(runbook.read_bytes()).hexdigest()
    action = {
        "schema": AGENT_ACTION_SCHEMA,
        "action_id": "action-runner-1",
        "idempotency_key": "idempotency-runner-1",
        "iteration_id": "iteration-runner-1",
        "campaign": "august",
        "operator_id": "hard-swish",
        "agent_pool_id": "local-source-agents",
        "role": "solver",
        "workflow_epoch": "epoch-1",
        "producer_generation": "release-1",
        "board_revision": "board-1",
        "board_digest": "a" * 64,
        "runbook_path": "operators/august/HardSwish/RUNBOOK.md",
        "runbook_digest": runbook_digest,
        "origin_workspace": "operators_workspace/HardSwish",
        "candidate_version": "HardSwish_V1_1",
        "candidate_identity": {"execution_source_digest": "b" * 64},
        "write_scope": ["candidate.txt"],
        "output_contracts": [],
        "tool_budget": {"max_turn_seconds": 60},
        "created_at": "2026-08-08T00:00:00+00:00",
    }
    context = {
        "schema": AGENT_CONTEXT_SNAPSHOT_SCHEMA,
        "snapshot_id": "snapshot-runner-1",
        "iteration_id": "iteration-runner-1",
        "operator_id": "hard-swish",
        "role": "solver",
        "board_revision": "board-1",
        "board_digest": "a" * 64,
        "candidate_identity": {"execution_source_digest": "b" * 64},
        "gate": {"owner": "solver"},
        "recent_results": [],
        "official_evidence": [],
        "open_hypotheses": [],
        "permitted_operations": ["edit-source"],
        "created_at": "2026-08-08T00:00:00+00:00",
    }
    store.create_agent_action(action, context)
    result = runner.run_once()
    assert result["state"] == "completed"
    assert (source / "candidate.txt").read_text(encoding="ascii") == "original\n"
    isolated = root / result["workspace"] / "candidate.txt"
    assert isolated.read_text(encoding="ascii") == "changed\n"
    assert not (isolated.parent / ".ascendop").exists()
    assert store.agent_actions_v4()[0]["state"] == "completed"
    with store.connection() as conn:
        attempt = conn.execute(
            "SELECT session_id FROM agent_action_attempts_v4"
        ).fetchone()
    assert attempt["session_id"] == "fake-session"


def test_runner_fails_closed_on_out_of_scope_change(tmp_path: Path) -> None:
    root, store, runner = _prepared_action(tmp_path, RogueCodexDriver(), "rogue")

    result = runner.run_once()

    assert result["state"] == "failed"
    completion = result["terminal"]["action"]
    assert completion["action_id"] == "action-rogue"
    with store.connection() as conn:
        details = json.loads(
            conn.execute(
                "SELECT details_json FROM agent_action_attempts_v4"
            ).fetchone()["details_json"]
        )
    assert details["failure_class"] == "write-scope-violation"
    assert details["out_of_scope_paths"] == ["rogue.txt"]
    assert not (root / "operators_workspace" / "HardSwish" / "rogue.txt").exists()


def test_runner_records_execution_exception_as_uncertain(tmp_path: Path) -> None:
    _, store, runner = _prepared_action(tmp_path, ExplodingCodexDriver(), "explode")

    result = runner.run_once()

    assert result["state"] == "uncertain"
    assert result["error"]["failure_class"] == "agent-execution-uncertain"
    action = store.agent_action("action-explode")
    assert action is not None and action["state"] == "uncertain"
    with store.connection() as conn:
        lease = conn.execute(
            "SELECT state FROM agent_work_leases_v4 WHERE action_id='action-explode'"
        ).fetchone()
    assert lease["state"] == "active"


def test_runner_defers_preflight_error_to_central_retry_controller(
    tmp_path: Path,
) -> None:
    root, store, runner = _prepared_action(tmp_path, FakeCodexDriver(), "preflight")
    runbook = root / "operators" / "august" / "HardSwish" / "RUNBOOK.md"
    runbook.write_text("changed after action publication", encoding="ascii")

    result = runner.run_once()

    assert result["state"] == "retry-pending"
    assert result["error"]["failure_class"] == "agent-preflight"
    assert result["error"]["runner_generation"]
    assert result["error"]["agent_execution_contract_digest"]
    action = store.agent_action("action-preflight")
    assert action is not None and action["state"] == "retry-pending"
    with store.connection() as conn:
        lease = conn.execute(
            "SELECT state FROM agent_work_leases_v4 WHERE action_id='action-preflight'"
        ).fetchone()
        receipt = conn.execute(
            "SELECT COUNT(*) AS count FROM agent_action_receipts_v4 "
            "WHERE action_id='action-preflight'"
        ).fetchone()
    assert lease["state"] == "active"
    assert receipt["count"] == 0

    candidate = store.agent_retry_candidates()[0]
    authorized = store.apply_agent_retry_decision(
        action_id="action-preflight",
        attempt_id=str(candidate["current_attempt_id"]),
        decision="retry",
        reason="central bounded retry",
        code_generation="daemon-generation-2",
    )
    assert authorized["state"] == "queued"
    runbook.write_text("Edit the candidate source.", encoding="ascii")

    terminal = runner.run_once()

    assert terminal["state"] == "completed", terminal
    with store.connection() as conn:
        attempts = conn.execute(
            "SELECT COUNT(*) AS count, MAX(ordinal) AS ordinal "
            "FROM agent_action_attempts_v4 WHERE action_id='action-preflight'"
        ).fetchone()
        leases = conn.execute(
            "SELECT COUNT(*) AS count, MIN(state) AS state "
            "FROM agent_work_leases_v4 WHERE action_id='action-preflight'"
        ).fetchone()
    assert attempts["count"] == 2
    assert attempts["ordinal"] == 2
    assert leases["count"] == 1
    assert leases["state"] == "released"


def test_runner_defers_zero_change_adapter_failure(tmp_path: Path) -> None:
    _, store, runner = _prepared_action(
        tmp_path, AdapterFailDriver(), "adapter-failure"
    )

    result = runner.run_once()

    assert result["state"] == "retry-pending"
    assert result["error"]["failure_class"] == "agent-adapter"
    assert result["error"]["exit_code"] == 2
    assert result["error"]["changed_paths"] == []
    assert len(result["error"]["artifacts"]) == 2
    action = store.agent_action("action-adapter-failure")
    assert action is not None and action["state"] == "retry-pending"


def test_retry_exhaustion_terminalizes_attempt_once(tmp_path: Path) -> None:
    _, store, runner = _prepared_action(
        tmp_path, AdapterFailDriver(), "adapter-exhaustion"
    )
    pending = runner.run_once()
    candidate = store.agent_retry_candidates()[0]

    terminal = store.apply_agent_retry_decision(
        action_id=str(candidate["action_id"]),
        attempt_id=str(candidate["current_attempt_id"]),
        decision="exhausted",
        reason="bounded attempts exhausted",
        code_generation="daemon-generation-terminal",
    )
    repeated = store.apply_agent_retry_decision(
        action_id=str(candidate["action_id"]),
        attempt_id=str(candidate["current_attempt_id"]),
        decision="exhausted",
        reason="bounded attempts exhausted",
        code_generation="daemon-generation-terminal",
    )

    assert pending["state"] == "retry-pending"
    assert terminal["state"] == "failed"
    assert repeated["state"] == "failed"
    exhausted_candidates = store.agent_retry_candidates()
    assert len(exhausted_candidates) == 1
    assert (
        exhausted_candidates[0]["failure"]["failure_class"]
        == "agent-adapter-retry-exhausted"
    )
    with store.connection() as conn:
        attempt = conn.execute(
            "SELECT state, details_json FROM agent_action_attempts_v4 "
            "WHERE attempt_id=?",
            (candidate["current_attempt_id"],),
        ).fetchone()
        event_count = conn.execute(
            "SELECT COUNT(*) AS count FROM control_events "
            "WHERE event_type='agent-action-retry-exhausted' "
            "AND entity_id=?",
            (candidate["action_id"],),
        ).fetchone()["count"]
    details = json.loads(attempt["details_json"])
    assert attempt["state"] == "failed"
    assert details["failure_class"] == "agent-adapter-retry-exhausted"
    assert details["agent_execution_contract_digest"]
    assert event_count == 1

    recovered = store.apply_agent_retry_decision(
        action_id=str(candidate["action_id"]),
        attempt_id=str(candidate["current_attempt_id"]),
        decision="retry",
        reason="new Agent execution contract",
        code_generation="daemon-generation-recovered",
    )
    assert recovered["state"] == "queued"


def test_runtime_auth_failure_quarantines_agent_across_runner_restart(
    tmp_path: Path,
) -> None:
    root, store, runner = _prepared_action(
        tmp_path, AuthFailDriver(), "auth-quarantine"
    )

    pending = runner.run_once()
    agent_id = f"codex-cli:{socket.gethostname()}"
    assert pending["state"] == "retry-pending"
    assert store.agent_registration(agent_id)["health_state"] == "degraded"
    runner.maintain_registrations(force=True)
    assert store.agent_registration(agent_id)["health_state"] == "degraded"

    restarted = AgentRunner(
        root=root,
        database=root / ".ascendop-work" / "runtime" / "control.sqlite3",
        drivers=(AuthFailDriver(),),
        runner_id="runner-auth-restarted",
    )
    restarted.probe_and_register()

    assert store.agent_registration(agent_id)["health_state"] == "degraded"


def test_auth_failure_rebinds_same_action_and_lease_to_healthy_agent(
    tmp_path: Path,
) -> None:
    root, store, runner = _prepared_action(
        tmp_path,
        (ClaudeAuthFailDriver(), FakeCodexDriver()),
        "auth-failover",
    )

    first = runner.run_once()
    candidate = store.agent_retry_candidates()[0]
    first_action = store.agent_action("action-auth-failover")
    first_lease_id = first_action["current_lease_id"]
    first_iteration_id = first_action["iteration_id"]
    authorized = store.apply_agent_retry_decision(
        action_id=str(candidate["action_id"]),
        attempt_id=str(candidate["current_attempt_id"]),
        decision="retry",
        reason="quarantine failed registration and rebind to healthy Agent",
        code_generation="daemon-generation-auth-failover",
    )
    second = runner.run_once()

    assert first["state"] == "retry-pending"
    assert first["driver"] == "claude-code-cli"
    assert authorized["state"] == "queued"
    assert authorized["assigned_agent_id"] == ""
    assert second["state"] == "completed", second
    assert second["driver"] == "codex-cli"
    assert second["action_id"] == first["action_id"]
    assert second["iteration_id"] == first_iteration_id
    with store.connection() as conn:
        attempts = conn.execute(
            "SELECT ordinal, agent_id, state FROM agent_action_attempts_v4 "
            "WHERE action_id=? ORDER BY ordinal",
            (first["action_id"],),
        ).fetchall()
        leases = conn.execute(
            "SELECT lease_id, agent_id, state FROM agent_work_leases_v4 "
            "WHERE action_id=?",
            (first["action_id"],),
        ).fetchall()
    assert [(row["ordinal"], row["agent_id"], row["state"]) for row in attempts] == [
        (1, f"claude-code-cli:{socket.gethostname()}", "failed"),
        (2, f"codex-cli:{socket.gethostname()}", "completed"),
    ]
    assert len(leases) == 1
    assert leases[0]["lease_id"] == first_lease_id
    assert leases[0]["agent_id"] == f"codex-cli:{socket.gethostname()}"
    assert leases[0]["state"] == "released"


def test_retry_attempts_keep_distinct_artifact_paths(tmp_path: Path) -> None:
    root, store, runner = _prepared_action(
        tmp_path, AdapterFailDriver(), "attempt-artifacts"
    )
    first = runner.run_once()
    candidate = store.agent_retry_candidates()[0]
    store.apply_agent_retry_decision(
        action_id=str(candidate["action_id"]),
        attempt_id=str(candidate["current_attempt_id"]),
        decision="retry",
        reason="central bounded retry",
        code_generation="daemon-generation-2",
    )
    second = runner.run_once()

    first_paths = {root / path for path in first["error"]["artifacts"]}
    second_paths = {root / path for path in second["error"]["artifacts"]}
    assert first_paths.isdisjoint(second_paths)
    assert all(path.is_file() for path in first_paths | second_paths)
    assert all("attempts" in path.parts for path in first_paths | second_paths)


def test_claimed_workflow_action_is_cancelled_if_gate_changes_before_start(
    tmp_path: Path,
) -> None:
    _, store, runner = _prepared_action(
        tmp_path, FakeCodexDriver(), "stale-gate"
    )
    action = store.agent_action("action-stale-gate")["action"]
    action["candidate_identity"]["origin"] = "workflow-gate"
    with store.transaction() as conn:
        conn.execute(
            "UPDATE agent_actions_v4 SET action_json=? WHERE action_id=?",
            (json.dumps(action, sort_keys=True), "action-stale-gate"),
        )
    store.synchronize_workflow_agent_gate_heads(
        [action], observed_at="2026-08-08T00:00:01+00:00"
    )
    claimed = store.claim_agent_action(
        runner_id=runner.runner_id,
        boot_id=runner.boot_id,
    )
    assert claimed is not None
    store.synchronize_workflow_agent_gate_heads(
        [], observed_at="2026-08-08T00:00:02+00:00"
    )

    terminal = store.start_agent_action(
        action_id="action-stale-gate",
        lease_token=claimed["lease"]["lease_token"],
        session_id="pending:stale-gate",
    )

    assert terminal["state"] == "cancelled"
    with store.connection() as conn:
        attempt = conn.execute(
            "SELECT state FROM agent_action_attempts_v4 WHERE attempt_id=?",
            (claimed["attempt_id"],),
        ).fetchone()
        receipt = conn.execute(
            "SELECT status FROM agent_action_receipts_v4 WHERE action_id=?",
            ("action-stale-gate",),
        ).fetchone()
    assert attempt["state"] == "cancelled"
    assert receipt["status"] == "cancelled"


def test_uncertain_action_resumes_same_attempt_and_lease(tmp_path: Path) -> None:
    _, store, runner = _prepared_action(
        tmp_path, UncertainThenResumeDriver(), "resume"
    )

    first = runner.run_once()
    second = runner.run_once()

    assert first["state"] == "uncertain"
    assert second["state"] == "completed", second
    with store.connection() as conn:
        attempt = conn.execute(
            "SELECT COUNT(*) AS count, MIN(session_id) AS session_id "
            "FROM agent_action_attempts_v4 WHERE action_id='action-resume'"
        ).fetchone()
        lease = conn.execute(
            "SELECT COUNT(*) AS count, MIN(state) AS state FROM agent_work_leases_v4 "
            "WHERE action_id='action-resume'"
        ).fetchone()
    assert attempt["count"] == 1
    assert attempt["session_id"] == "resumable-session"
    assert lease["count"] == 1
    assert lease["state"] == "released"


def _prepared_action(
    tmp_path: Path,
    driver: AgentDriver | tuple[AgentDriver, ...],
    suffix: str,
) -> tuple[Path, ControlStore, AgentRunner]:
    root = tmp_path / suffix
    source = root / "operators_workspace" / "HardSwish"
    source.mkdir(parents=True)
    (source / "candidate.txt").write_text("original\n", encoding="ascii")
    runbook = root / "operators" / "august" / "HardSwish" / "RUNBOOK.md"
    runbook.parent.mkdir(parents=True)
    runbook.write_text("Edit the candidate source.", encoding="ascii")
    database_path = root / ".ascendop-work" / "runtime" / "control.sqlite3"
    drivers = driver if isinstance(driver, tuple) else (driver,)
    store = _store(
        database_path,
        drivers=tuple(item.driver_id for item in drivers),
    )
    runner = AgentRunner(
        root=root,
        database=database_path,
        drivers=drivers,
        runner_id=f"runner-{suffix}",
    )
    runner.probe_and_register()
    action = {
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
        "board_digest": "a" * 64,
        "runbook_path": "operators/august/HardSwish/RUNBOOK.md",
        "runbook_digest": hashlib.sha256(runbook.read_bytes()).hexdigest(),
        "origin_workspace": "operators_workspace/HardSwish",
        "candidate_version": f"HardSwish_V1_{suffix}",
        "candidate_identity": {"execution_source_digest": "b" * 64},
        "write_scope": ["candidate.txt"],
        "output_contracts": [],
        "tool_budget": {"max_turn_seconds": 60},
        "created_at": "2026-08-08T00:00:00+00:00",
    }
    context = {
        "schema": AGENT_CONTEXT_SNAPSHOT_SCHEMA,
        "snapshot_id": f"snapshot-{suffix}",
        "iteration_id": f"iteration-{suffix}",
        "operator_id": "hard-swish",
        "role": "solver",
        "board_revision": f"board-{suffix}",
        "board_digest": "a" * 64,
        "candidate_identity": {"execution_source_digest": "b" * 64},
        "gate": {"owner": "solver"},
        "recent_results": [],
        "official_evidence": [],
        "open_hypotheses": [],
        "permitted_operations": ["edit-source"],
        "created_at": "2026-08-08T00:00:00+00:00",
    }
    store.create_agent_action(action, context)
    return root, store, runner


def _store(
    path: Path,
    *,
    drivers: tuple[str, ...] = ("codex-cli",),
) -> ControlStore:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.executescript(
        "CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);"
        "INSERT INTO metadata(key,value) VALUES('schema_version','12');"
        "CREATE TABLE control_events(sequence INTEGER PRIMARY KEY AUTOINCREMENT,"
        "event_at TEXT NOT NULL,event_type TEXT NOT NULL,entity_type TEXT NOT NULL,"
        "entity_id TEXT NOT NULL,payload_json TEXT NOT NULL);"
        "CREATE TABLE scheduler_state(scheduler_id TEXT PRIMARY KEY,"
        "revision INTEGER NOT NULL,state_json TEXT NOT NULL,updated_at TEXT NOT NULL);"
        + CONTROL_EXTENSION_SQL
    )
    conn.commit()
    conn.close()
    store = ControlStore(path)
    store.register_agent_pool(
        {
            "schema": AGENT_POOL_SCHEMA,
            "pool_id": "local-source-agents",
            "enabled": True,
            "roles": ["solver", "tester"],
            "drivers": list(drivers),
            "required_capabilities": {
                "stream_json": True,
                "structured_output": True,
            },
            "priority": 100,
            "registration_generation": "test-pool-1",
        }
    )
    return store
