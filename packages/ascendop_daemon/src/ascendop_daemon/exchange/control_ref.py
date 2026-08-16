from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping


def control_ref(
    payload: Mapping[str, Any],
    action: str,
    *,
    ordinal: int = 0,
    receipt_id: str = "",
) -> str:
    stable_payload = {
        str(key): value
        for key, value in payload.items()
        if not str(key).startswith("_")
    }
    identity = {
        "schema": "ascendop.flow-control-identity.v2",
        "action": action,
        "ordinal": max(0, ordinal),
        "payload": stable_payload,
        "receipt_id": receipt_id,
    }
    stable = json.dumps(
        identity,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return f"flowv3-{action}-{hashlib.sha256(stable.encode()).hexdigest()[:24]}"
