from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .repository import V4ControlRepository, _canonical_json, _utc_now


EXPECTED_SCHEMA_VERSION = 12


class ControlStore(V4ControlRepository):
    """A process-neutral connection facade for an initialized control DB."""

    def __init__(self, path: Path) -> None:
        self.path = path.resolve()

    def assert_compatible(self) -> None:
        if not self.path.is_file():
            raise RuntimeError(f"control database does not exist: {self.path}")
        with self.connection() as conn:
            try:
                row = conn.execute(
                    "SELECT value FROM metadata WHERE key='schema_version'"
                ).fetchone()
            except sqlite3.OperationalError as exc:
                raise RuntimeError("control database is not initialized") from exc
        if row is None or int(row[0]) != EXPECTED_SCHEMA_VERSION:
            found = "missing" if row is None else str(row[0])
            raise RuntimeError(
                f"control database schema mismatch: expected "
                f"{EXPECTED_SCHEMA_VERSION}, found {found}"
            )

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(str(self.path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA busy_timeout=30000")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except Exception:
                conn.rollback()
                raise
            else:
                conn.commit()

    @staticmethod
    def _event(
        conn: sqlite3.Connection,
        event_type: str,
        entity_type: str,
        entity_id: str,
        payload: dict[str, Any],
    ) -> None:
        conn.execute(
            "INSERT INTO control_events(event_at, event_type, entity_type, "
            "entity_id, payload_json) VALUES(?, ?, ?, ?, ?)",
            (_utc_now(), event_type, entity_type, entity_id, _canonical_json(payload)),
        )

    def metadata(self) -> dict[str, str]:
        with self.connection() as conn:
            rows = conn.execute("SELECT key, value FROM metadata ORDER BY key").fetchall()
        return {str(row["key"]): str(row["value"]) for row in rows}

    def raw_rows(
        self,
        table: str,
        *,
        order_by: str,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        allowed = {
            "operator_registrations": "operator_id",
            "test_requests": "created_at DESC",
            "backend_endpoints": "endpoint_id",
            "service_heartbeats": "service_id",
            "agent_registrations_v4": "agent_id",
            "agent_pools_v4": "pool_id",
            "agent_actions_v4": "created_at DESC",
            "agent_work_leases_v4": "acquired_at DESC",
            "agent_iterations_v4": "created_at DESC",
            "transport_returns": "received_at DESC",
        }
        if table not in allowed or order_by != allowed[table]:
            raise ValueError("unsupported public projection query")
        with self.connection() as conn:
            rows = conn.execute(
                f"SELECT * FROM {table} ORDER BY {order_by} LIMIT ?",
                (max(1, min(int(limit), 1000)),),
            ).fetchall()
        return [_decode_row(row) for row in rows]


def _decode_row(row: sqlite3.Row) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key in row.keys():
        value = row[key]
        if key.endswith("_json") and isinstance(value, str):
            try:
                result[key[:-5]] = json.loads(value)
            except json.JSONDecodeError:
                result[key] = value
        else:
            result[key] = value
    return result
