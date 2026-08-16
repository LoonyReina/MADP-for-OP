from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path


ENDPOINT_SCHEMA = "ascendop.control-api-endpoint.v1"


def publish_endpoint(
    path: Path,
    *,
    host: str,
    port: int,
    generation: str,
    pid: int | None = None,
) -> dict[str, object]:
    process_id = int(pid if pid is not None else os.getpid())
    payload: dict[str, object] = {
        "schema": ENDPOINT_SCHEMA,
        "generation": str(generation),
        "host": str(host),
        "port": int(port),
        "pid": process_id,
        "published_at": datetime.now(timezone.utc).isoformat(),
    }
    path = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{process_id}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=True, sort_keys=True) + "\n",
        encoding="ascii",
    )
    os.replace(temporary, path)
    return payload


def retire_endpoint(path: Path, *, generation: str, pid: int | None = None) -> bool:
    path = path.resolve()
    process_id = int(pid if pid is not None else os.getpid())
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return False
    if (
        payload.get("schema") != ENDPOINT_SCHEMA
        or payload.get("generation") != generation
        or int(payload.get("pid") or 0) != process_id
    ):
        return False
    path.unlink(missing_ok=True)
    return True
