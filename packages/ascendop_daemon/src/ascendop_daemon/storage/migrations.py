from __future__ import annotations

import json
import sqlite3
from typing import Any, Callable

from ascendop_protocol.agent import AgentContractError, validate_agent_action

from ascendop_daemon.storage.control_types import (
    OUTBOX_ACTIVE_STATES,
    OUTBOX_CLAIMABLE_STATES,
)
from ascendop_daemon.storage.control_validation import utc_now


EventWriter = Callable[
    [sqlite3.Connection, str, str, str, dict[str, Any]], None
]

AGENT_OUTPUT_CONTRACT_MIGRATION_KEY = "agent_output_contract_hard_cut_v1"
AGENT_ITERATION_CANDIDATE_INDEX_MIGRATION_KEY = (
    "agent_iteration_candidate_active_index_v2"
)


def migrate_agent_iteration_candidate_index_v2(
    conn: sqlite3.Connection,
    event: EventWriter,
) -> None:
    """Limit candidate uniqueness to unfinished Agent iterations."""

    marker = conn.execute(
        "SELECT value FROM metadata WHERE key=?",
        (AGENT_ITERATION_CANDIDATE_INDEX_MIGRATION_KEY,),
    ).fetchone()
    if marker is not None:
        return
    active_states = "'queued','claimed','running','uncertain','retry-pending'"
    collisions = conn.execute(
        "SELECT operator_id, role, candidate_version, COUNT(*) AS count "
        "FROM agent_iterations_v4 WHERE state IN (" + active_states + ") "
        "GROUP BY operator_id, role, candidate_version HAVING COUNT(*)>1 "
        "ORDER BY operator_id, role, candidate_version"
    ).fetchall()
    if collisions:
        first = collisions[0]
        raise sqlite3.IntegrityError(
            "multiple active Agent iterations already reserve one candidate: "
            f"operator={first['operator_id']} role={first['role']} "
            f"candidate={first['candidate_version']} count={first['count']}"
        )
    conn.execute("DROP INDEX IF EXISTS idx_agent_iterations_v4_candidate")
    conn.execute(
        "CREATE UNIQUE INDEX idx_agent_iterations_v4_candidate ON "
        "agent_iterations_v4(operator_id, role, candidate_version) "
        "WHERE state IN (" + active_states + ")"
    )
    conn.execute(
        "INSERT INTO metadata(key, value) VALUES(?, '1')",
        (AGENT_ITERATION_CANDIDATE_INDEX_MIGRATION_KEY,),
    )
    event(
        conn,
        "agent-iteration-candidate-index-migrated",
        "metadata",
        AGENT_ITERATION_CANDIDATE_INDEX_MIGRATION_KEY,
        {"scope": "active-iterations-only"},
    )


def migrate_transport_protocol_v3(
    conn: sqlite3.Connection,
    event: EventWriter,
) -> None:
    """Fence unpublished legacy work; terminal evidence remains read-only."""

    now = utc_now()
    rows = conn.execute(
        "SELECT outbox_id, attempt_id, state, payload_json "
        "FROM transport_outbox WHERE transport_protocol=''"
    ).fetchall()
    for row in rows:
        try:
            payload = json.loads(str(row["payload_json"]))
        except json.JSONDecodeError:
            payload = {}
        if not bool(payload.get("workflow_ingest", True)):
            conn.execute(
                "UPDATE transport_outbox SET transport_protocol='canary-v1', "
                "updated_at=? WHERE outbox_id=?",
                (now, row["outbox_id"]),
            )
            continue
        state = str(row["state"])
        if state not in OUTBOX_ACTIVE_STATES | OUTBOX_CLAIMABLE_STATES:
            conn.execute(
                "UPDATE transport_outbox SET transport_protocol='legacy-readonly', "
                "updated_at=? WHERE outbox_id=?",
                (now, row["outbox_id"]),
            )
            continue
        reason = "legacy runtime attempt quarantined by Wire V3 hard cut"
        conn.execute(
            "UPDATE transport_outbox SET state='quarantined', "
            "transport_protocol='legacy-readonly', error=?, updated_at=? "
            "WHERE outbox_id=?",
            (reason, now, row["outbox_id"]),
        )
        conn.execute(
            "UPDATE execution_attempts SET state='quarantined', error=?, "
            "updated_at=? WHERE attempt_id=?",
            (reason, now, row["attempt_id"]),
        )
        conn.execute(
            "UPDATE test_requests SET state='blocked', blocker=?, updated_at=? "
            "WHERE request_id=(SELECT request_id FROM execution_attempts "
            "WHERE attempt_id=?)",
            (reason, now, row["attempt_id"]),
        )
        event(
            conn,
            "legacy-transport-attempt-quarantined",
            "transport-outbox",
            str(row["outbox_id"]),
            {"previous_state": state, "reason": reason},
        )


def migrate_agent_output_contract_v1(
    conn: sqlite3.Connection,
    event: EventWriter,
) -> int:
    """Quarantine pre-contract Agent work before a V4 executor may claim it."""

    marker = conn.execute(
        "SELECT value FROM metadata WHERE key=?",
        (AGENT_OUTPUT_CONTRACT_MIGRATION_KEY,),
    ).fetchone()
    if marker is not None:
        return int(marker[0])
    now = utc_now()
    rows = conn.execute(
        "SELECT action_id, iteration_id, state, action_json "
        "FROM agent_actions_v4 WHERE state IN "
        "('queued','claimed','running','uncertain','retry-pending') "
        "ORDER BY created_at, action_id"
    ).fetchall()
    count = 0
    for row in rows:
        raw = str(row["action_json"])
        try:
            payload = json.loads(raw)
            validate_agent_action(payload)
        except (json.JSONDecodeError, AgentContractError, TypeError):
            pass
        else:
            continue
        action_id = str(row["action_id"])
        iteration_id = str(row["iteration_id"])
        previous_state = str(row["state"])
        reason = (
            "Agent action quarantined by typed output-contract hard cut; "
            "the immutable action predates AgentActionV1.output_contracts"
        )
        conn.execute(
            "UPDATE agent_actions_v4 SET state='quarantined', updated_at=? "
            "WHERE action_id=?",
            (now, action_id),
        )
        conn.execute(
            "UPDATE agent_iterations_v4 SET state='quarantined', updated_at=? "
            "WHERE iteration_id=?",
            (now, iteration_id),
        )
        conn.execute(
            "UPDATE agent_action_attempts_v4 SET state='cancelled', "
            "completed_at=CASE WHEN completed_at='' THEN ? ELSE completed_at END, "
            "updated_at=? WHERE action_id=? AND state IN ('claimed','running')",
            (now, now, action_id),
        )
        leases = conn.execute(
            "SELECT lease_id, lease_json FROM agent_work_leases_v4 "
            "WHERE action_id=? AND state='active'",
            (action_id,),
        ).fetchall()
        for lease_row in leases:
            lease_json = _cancelled_lease_json(str(lease_row["lease_json"]))
            conn.execute(
                "UPDATE agent_work_leases_v4 SET state='cancelled', released_at=?, "
                "lease_json=? WHERE lease_id=? AND state='active'",
                (now, lease_json, lease_row["lease_id"]),
            )
        conn.execute(
            "INSERT INTO agent_action_quarantine_v4(action_id, iteration_id, "
            "previous_state, reason, action_json, quarantined_at) "
            "VALUES(?, ?, ?, ?, ?, ?)",
            (action_id, iteration_id, previous_state, reason, raw, now),
        )
        event(
            conn,
            "agent-action-contract-quarantined",
            "agent-action",
            action_id,
            {
                "iteration_id": iteration_id,
                "previous_state": previous_state,
                "reason": reason,
            },
        )
        count += 1
    conn.execute(
        "INSERT INTO metadata(key, value) VALUES(?, ?)",
        (AGENT_OUTPUT_CONTRACT_MIGRATION_KEY, str(count)),
    )
    return count


def _cancelled_lease_json(raw: str) -> str:
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        value = {}
    if not isinstance(value, dict):
        value = {}
    value["state"] = "cancelled"
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
