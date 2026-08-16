from __future__ import annotations

import json
import sqlite3
from typing import Any, Callable

from ascendop_daemon.storage.control_types import (
    OUTBOX_ACTIVE_STATES,
    OUTBOX_CLAIMABLE_STATES,
)
from ascendop_daemon.storage.control_validation import utc_now


EventWriter = Callable[
    [sqlite3.Connection, str, str, str, dict[str, Any]], None
]


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
