from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Any, Iterator

from ascendop_daemon.storage.control_validation import canonical_json, utc_now

class TransactionRepository:
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
            "INSERT INTO control_events(event_at, event_type, entity_type, entity_id, "
            "payload_json) VALUES(?, ?, ?, ?, ?)",
            (
                utc_now(),
                event_type,
                entity_type,
                entity_id,
                canonical_json(payload),
            ),
        )
