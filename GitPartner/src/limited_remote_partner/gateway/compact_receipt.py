from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from limited_remote_partner.gateway.relay import write_json


COMPACT_RECEIPT_SCHEMA = "gitpartner.compact-receipt.v1"
COMPACT_STATUS_FIELDS = (
    "request_id",
    "request_kind",
    "completion_mode",
    "experiment_id",
    "attempt_id",
    "engine_job_id",
    "workflow_ingest",
    "target_nodes",
    "target_endpoint_id",
    "target_environment_id",
    "target_gateway_id",
    "target_transport_mode",
    "registration_generation",
    "trigger_ref",
    "state",
    "phase",
    "transport",
    "client_state",
    "started_at",
    "dispatched_at",
    "accepted_at",
    "finished_at",
    "return_collected_at",
    "exit_code",
    "relay_protocol_version",
    "relay_status_updated_at",
    "updated_at",
)


def compact_status(status: dict[str, Any]) -> dict[str, Any]:
    receipt = {
        "schema": COMPACT_RECEIPT_SCHEMA,
        **{
            key: status[key]
            for key in COMPACT_STATUS_FIELDS
            if key in status
        },
    }
    error = str(status.get("error") or "")
    if error:
        receipt["error"] = error[:512]
        receipt["error_sha256"] = hashlib.sha256(
            error.encode("utf-8", errors="replace")
        ).hexdigest()
    return receipt


def write_compact_receipt(
    result_dir: Path,
    status: dict[str, Any],
    max_file_bytes: int,
) -> None:
    write_json(
        result_dir / "receipt.json",
        compact_status(status),
        max_file_bytes,
    )
