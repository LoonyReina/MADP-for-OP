from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping, Protocol

from ascendop_protocol.management import CONTROL_COMMAND_RECEIPT_SCHEMA


class ControlCommandRejected(ValueError):
    """A valid envelope whose requested operation is not admissible."""


class ControlCommandHandler(Protocol):
    def handle(self, command: Mapping[str, Any]) -> Mapping[str, Any]: ...


class ControlCommandWorker:
    """Lease and execute one typed control command at a time."""

    def __init__(
        self,
        store: Any,
        handler: ControlCommandHandler,
        *,
        worker_id: str,
        claim_seconds: int = 30,
    ) -> None:
        self.store = store
        self.handler = handler
        self.worker_id = str(worker_id).strip()
        self.claim_seconds = int(claim_seconds)
        if not self.worker_id:
            raise ValueError("control command worker id is required")

    def run_once(self) -> dict[str, Any]:
        claimed = self.store.claim_control_command(
            worker_id=self.worker_id,
            lease_seconds=self.claim_seconds,
        )
        if claimed is None:
            return {"state": "idle", "worker_id": self.worker_id}
        command = claimed["command"]
        status = "completed"
        try:
            result = dict(self.handler.handle(command))
        except ControlCommandRejected as exc:
            status = "rejected"
            result = {"error": str(exc), "error_type": type(exc).__name__}
        except Exception as exc:
            status = "failed"
            result = {"error": str(exc), "error_type": type(exc).__name__}
        receipt = {
            "schema": CONTROL_COMMAND_RECEIPT_SCHEMA,
            "command_id": command["command_id"],
            "status": status,
            "result": result,
            "completed_at": _utc_now(),
        }
        terminal = self.store.complete_control_command(
            receipt,
            claim_token=str(claimed["claim_token"]),
        )
        return {
            "state": status,
            "worker_id": self.worker_id,
            "command_id": command["command_id"],
            "command_kind": command["command_kind"],
            "receipt": terminal,
        }


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
