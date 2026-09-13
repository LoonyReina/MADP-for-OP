from __future__ import annotations

import json
import os
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping

from ascendop_protocol.file_lock import exclusive_file_lock

from .contracts import (
    ALLOWED_TRANSITIONS,
    JOURNAL_SCHEMA,
    TERMINAL_STATES,
    TestState,
    TransportReceipt,
    canonical_artifact_root,
    TransportStatus,
    terminal_ingest_event,
)
from .terminal_evidence import sync_directory, validate_terminal_evidence


_WINDOWS_FILE_RETRY_ATTEMPTS = 8
_WINDOWS_FILE_RETRY_DELAY_SECONDS = 0.025


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _same_terminal_ingest_event(
    first: Mapping[str, Any],
    second: Mapping[str, Any],
) -> bool:
    left = dict(first)
    right = dict(second)
    left["artifact_root"] = canonical_artifact_root(
        str(left.get("artifact_root") or "")
    )
    right["artifact_root"] = canonical_artifact_root(
        str(right.get("artifact_root") or "")
    )
    return left == right


class RunJournal:
    def __init__(self, run_dir: Path | str) -> None:
        self.run_dir = Path(run_dir).resolve()
        self.path = self.run_dir / "JOURNAL.json"
        self.lock_path = self.run_dir / ".journal.lock"

    @contextmanager
    def _lock(self, timeout_seconds: float = 5.0) -> Iterator[None]:
        with exclusive_file_lock(self.lock_path, timeout_seconds):
            yield

    def initialize(self, request_id: str) -> dict[str, Any]:
        with self._lock():
            if self.path.is_file():
                data = self._read_unlocked()
                if data.get("request_id") != request_id:
                    raise ValueError("journal request identity mismatch")
                return data
            now = _now()
            data = {
                "schema": JOURNAL_SCHEMA,
                "request_id": request_id,
                "state": TestState.PREPARED.value,
                "created_at": now,
                "updated_at": now,
                "progress_observed_at": now,
                "transport_receipt": None,
                "remote_status": None,
                "cancellation": None,
                "events": [{"from": None, "to": TestState.PREPARED.value, "at": now}],
            }
            self._write_unlocked(data)
            return data

    def read(self) -> dict[str, Any]:
        return self._read_unlocked()

    def receipt(self) -> TransportReceipt | None:
        raw = self.read().get("transport_receipt")
        return TransportReceipt.from_dict(raw) if isinstance(raw, Mapping) else None

    def attach_receipt(self, receipt: TransportReceipt) -> dict[str, Any]:
        with self._lock():
            data = self._read_unlocked()
            existing = data.get("transport_receipt")
            encoded = receipt.to_dict()
            if existing is not None and existing != encoded:
                raise ValueError("transport receipt is immutable")
            data["transport_receipt"] = encoded
            data["updated_at"] = _now()
            self._write_unlocked(data)
            return data

    def attach_terminal_ingest_event(
        self,
        event: Mapping[str, Any],
    ) -> dict[str, Any]:
        with self._lock():
            data = self._read_unlocked()
            encoded = dict(event)
            existing = data.get("terminal_ingest_event")
            if existing is not None and not _same_terminal_ingest_event(
                existing,
                encoded,
            ):
                raise ValueError("terminal ingest event is immutable")
            if existing is None:
                data["terminal_ingest_event"] = encoded
            data["updated_at"] = _now()
            self._write_unlocked(data)
            return data

    def resume_prepublication_failure(self) -> dict[str, Any]:
        """Re-open only a proven local failure that never started publication."""

        with self._lock():
            data = self._read_unlocked()
            if TestState(str(data["state"])) != TestState.FAILED:
                return data
            remote_status = data.get("remote_status")
            remote_status = remote_status if isinstance(remote_status, Mapping) else {}
            metrics = remote_status.get("metrics")
            metrics = metrics if isinstance(metrics, Mapping) else {}
            if (
                data.get("transport_receipt") is not None
                or str(remote_status.get("classification") or "") != "invalid-request"
                or metrics.get("publication_started") is not False
            ):
                return data
            now = _now()
            data["state"] = TestState.PREPARED.value
            data["updated_at"] = now
            data["progress_observed_at"] = now
            data["last_prepublication_failure"] = dict(remote_status)
            data["remote_status"] = None
            data["events"].append(
                {
                    "from": TestState.FAILED.value,
                    "to": TestState.PREPARED.value,
                    "at": now,
                    "reason": "sealed-prepublication-resume",
                }
            )
            self._write_unlocked(data)
            return data

    def accept_terminal(
        self,
        receipt: TransportReceipt,
        status: TransportStatus,
        *,
        retained: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Publish result, exact event, retention and ACK intent in ONE replace."""
        event = terminal_ingest_event(receipt, status)
        with self._lock():
            data = self._read_unlocked()
            if data.get("transport_receipt") != receipt.to_dict():
                raise ValueError("terminal acceptance transport receipt mismatch")
            previous = data.get("terminal_ingest_event")
            if previous is not None and not _same_terminal_ingest_event(
                previous, event
            ):
                raise ValueError("terminal ingest event is immutable")
            existing = data.get("terminal_retention")
            if existing is not None:
                if existing != dict(retained):
                    raise ValueError("terminal retention is immutable")
                if data.get("state") != status.state.value:
                    raise ValueError("terminal acceptance state mismatch")
                return data  # Never regress an ACK or re-append an event.
            validate_terminal_evidence(self.run_dir, event, retained)
            current = TestState(str(data["state"]))
            if current != status.state and (
                current in TERMINAL_STATES
                or status.state not in ALLOWED_TRANSITIONS[current]
            ):
                raise ValueError("invalid terminal acceptance transition")
            now = _now()
            prior_ack = status.metrics.get("return_path_ready") is True
            data.update(
                {
                    "state": status.state.value,
                    "remote_status": status.to_dict(),
                    "terminal_ingest_event": event,
                    "terminal_retention": dict(retained),
                    "terminal_ack": {
                        "event_id": event["event_id"],
                        "state": "delivered" if prior_ack else "pending",
                        "provenance": (
                            "prior-transport-observation"
                            if prior_ack
                            else "gateway-retention"
                        ),
                        "attempts": 0,
                        "last_error": "",
                    },
                    "updated_at": now,
                    "progress_observed_at": now,
                }
            )
            if current != status.state:
                data["events"].append(
                    {
                        "from": current.value,
                        "to": status.state.value,
                        "at": now,
                        "reason": "terminal-durably-accepted",
                    }
                )
            self._write_unlocked(data)
            return data

    def read_terminal_acceptance(self, event_id: str) -> dict[str, Any]:
        """Read an already accepted identity, without rescanning the payload.

        This is not authorization to ACK or delete evidence. Consumers validate
        the retained business artifacts they actually consume; ACK uses the
        stronger require_terminal_acceptance boundary below.
        """
        data = self.read()
        event = data.get("terminal_ingest_event")
        retained = data.get("terminal_retention")
        ack = data.get("terminal_ack")
        if (
            not isinstance(event, dict)
            or event.get("event_id") != event_id
            or not isinstance(retained, dict)
            or retained.get("event_id") != event_id
            or not isinstance(ack, dict)
            or ack.get("event_id") != event_id
        ):
            raise ValueError("ACK requires the exact durable terminal acceptance")
        receipt = TransportReceipt.from_dict(data["transport_receipt"])
        raw = data["remote_status"]
        status = TransportStatus(
            request_id=raw["request_id"],
            state=TestState(raw["state"]),
            result=raw["result"],
        )
        if data["state"] != status.state.value or not _same_terminal_ingest_event(
            event, terminal_ingest_event(receipt, status)
        ):
            raise ValueError("accepted terminal identity changed")
        return data

    def require_terminal_acceptance(self, event_id: str) -> dict[str, Any]:
        data = self.read_terminal_acceptance(event_id)
        validate_terminal_evidence(
            self.run_dir, data["terminal_ingest_event"], data["terminal_retention"]
        )
        # A writer can die between replace and directory fsync. Seeing the new
        # JSON is not proof that its directory entry was synced; close that cut
        # again at the ACK gate (also for direct adapter callers).
        sync_directory(self.run_dir)
        return data

    def record_terminal_ack(
        self,
        event_id: str,
        *,
        begin: bool = False,
        acknowledged: bool = False,
        error: str = "",
    ) -> dict[str, Any]:
        with self._lock():
            data = self._read_unlocked()
            ack = data.get("terminal_ack")
            if not isinstance(ack, dict) or ack.get("event_id") != event_id:
                raise ValueError("terminal ACK identity mismatch")
            if ack["state"] == "delivered":
                return data
            if begin:
                ack["attempts"] += 1
            ack["last_error"] = str(error)[:2000]
            if acknowledged:
                ack["state"] = "delivered"
                remote = data["remote_status"]
                remote["metrics"]["return_path_ready"] = True
                remote["metrics"]["terminal_ack_deferred"] = False
                remote["classification"] = (
                    remote["classification"].removesuffix(":ack-pending")
                    + ":acknowledged"
                )
            data["updated_at"] = _now()
            self._write_unlocked(data)
            return data

    def request_cancellation(self, *, reason: str) -> dict[str, Any]:
        with self._lock():
            data = self._read_unlocked()
            if TestState(str(data["state"])) in TERMINAL_STATES:
                return data
            normalized_reason = str(reason or "").strip()
            if not normalized_reason or len(normalized_reason) > 1000:
                raise ValueError("cancellation reason must contain 1..1000 characters")
            existing = data.get("cancellation")
            if isinstance(existing, Mapping):
                if str(existing.get("reason") or "") != normalized_reason:
                    raise ValueError("cancellation intent is immutable")
                return data
            now = _now()
            data["cancellation"] = {
                "schema": "ascendop.standalone-cancellation-intent.v1",
                "requested_at": now,
                "reason": normalized_reason,
            }
            data["updated_at"] = now
            data["events"].append(
                {
                    "from": str(data["state"]),
                    "to": str(data["state"]),
                    "at": now,
                    "reason": "cancellation-requested",
                }
            )
            self._write_unlocked(data)
            return data

    def transition(
        self,
        target: TestState,
        *,
        reason: str = "",
        remote_status: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self._lock():
            data = self._read_unlocked()
            current = TestState(str(data["state"]))
            if current == target:
                if remote_status is not None:
                    encoded = dict(remote_status)
                    if data.get("remote_status") != encoded:
                        if data.get("terminal_retention") is not None:
                            raise ValueError(
                                "retained terminal may only be updated by the exact ACK operation"
                            )
                        now = _now()
                        data["remote_status"] = encoded
                        data["updated_at"] = now
                        data["progress_observed_at"] = now
                        self._write_unlocked(data)
                return data
            if current in TERMINAL_STATES or target not in ALLOWED_TRANSITIONS[current]:
                raise ValueError(
                    f"invalid test transition: {current.value} -> {target.value}"
                )
            now = _now()
            data["state"] = target.value
            data["updated_at"] = now
            if remote_status is not None:
                data["remote_status"] = dict(remote_status)
                data["progress_observed_at"] = now
            data["events"].append(
                {
                    "from": current.value,
                    "to": target.value,
                    "at": now,
                    "reason": reason,
                }
            )
            self._write_unlocked(data)
            return data

    def _read_unlocked(self) -> dict[str, Any]:
        if not self.path.is_file():
            raise FileNotFoundError(self.path)
        raw = json.loads(self.path.read_text(encoding="utf-8-sig"))
        if not isinstance(raw, dict) or raw.get("schema") != JOURNAL_SCHEMA:
            raise ValueError("unsupported standalone test journal")
        return raw

    def _write_unlocked(self, data: Mapping[str, Any]) -> None:
        temporary = self.path.with_name(f".{self.path.name}.{uuid.uuid4().hex}.tmp")
        try:
            with temporary.open("w", encoding="utf-8") as handle:
                handle.write(
                    json.dumps(data, ensure_ascii=True, sort_keys=True, indent=2) + "\n"
                )
                handle.flush()
                os.fsync(handle.fileno())
            _replace_with_retry(temporary, self.path)
            sync_directory(self.run_dir)
        finally:
            if temporary.exists():
                _unlink_with_retry(temporary)


def _replace_with_retry(source: Path, destination: Path) -> None:
    for attempt in range(_WINDOWS_FILE_RETRY_ATTEMPTS):
        try:
            os.replace(source, destination)
            return
        except PermissionError:
            if attempt + 1 == _WINDOWS_FILE_RETRY_ATTEMPTS:
                raise
            time.sleep(_WINDOWS_FILE_RETRY_DELAY_SECONDS)


def _unlink_with_retry(path: Path) -> None:
    for attempt in range(_WINDOWS_FILE_RETRY_ATTEMPTS):
        try:
            path.unlink(missing_ok=True)
            return
        except PermissionError:
            if attempt + 1 == _WINDOWS_FILE_RETRY_ATTEMPTS:
                raise
            time.sleep(_WINDOWS_FILE_RETRY_DELAY_SECONDS)
