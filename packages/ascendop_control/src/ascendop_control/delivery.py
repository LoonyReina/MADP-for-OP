"""Deliver existing control outbox claims through explicit integration ports.

Handlers must be idempotent: a process can die after its side effect and before
the claim is marked delivered. This module owns no business state or queue.
"""
from __future__ import annotations

from typing import Any, Callable, Mapping

DeliveryHandler = Callable[[Mapping[str, Any]], Mapping[str, Any]]


def deliver_claim(database: Any, claimed: Mapping[str, Any], handler: DeliveryHandler) -> dict[str, Any]:
    """Deliver one already fenced claim; preserve process-death uncertainty."""
    identifier = claimed["outbox_id"]
    try:
        result = dict(handler(claimed))
        paused = result.get("disposition") == "paused"
        pending = claimed["topic"] in {"test.ack", "test.request"} and result.get("disposition") == "pending"
        if paused or pending:
            database.defer_control_outbox(
                outbox_id=identifier, claim_token=claimed["claim_token"],
                error="operator scheduling paused; accepted continuation retained" if paused else (
                    "remote acknowledgement pending" if claimed["topic"] == "test.ack"
                    else "original request publication needs reconciliation"),
            )
            return {"delivered": [], "errors": []}
        database.finish_control_outbox(
            outbox_id=identifier, claim_token=claimed["claim_token"], result=result,
        )
        return {"delivered": [identifier], "errors": []}
    except Exception as exc:
        error = {"outbox_id": identifier, "error": str(exc)}
        try:
            database.defer_control_outbox(
                outbox_id=identifier, claim_token=claimed["claim_token"], error=str(exc),
            )
        except Exception as defer_error:
            # Never overwrite a subsequent owner after losing this claim.
            error["defer_error"] = str(defer_error)
        return {"delivered": [], "errors": [error]}


def dispatch_topic(database: Any, *, topic: str, owner: str, handler: DeliveryHandler,
                   limit: int = 4, origin_id: str | None = None) -> dict[str, Any]:
    """Bounded delivery adapter. Topic order/admission remain caller policy."""
    delivered, errors = [], []
    for _ in range(max(0, min(int(limit), 100))):
        claimed = database.claim_control_outbox(topic=topic, owner=owner, origin_id=origin_id)
        if claimed is None:
            break
        result = deliver_claim(database, claimed, handler)
        delivered.extend(result["delivered"])
        errors.extend(result["errors"])
    return {"delivered": delivered, "errors": errors}
