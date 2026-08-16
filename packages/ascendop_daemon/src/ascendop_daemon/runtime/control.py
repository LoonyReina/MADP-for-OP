from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ascendop_daemon.core.models import utc_now_iso


def stop_request_path(root: Path) -> Path:
    return root / "TestUtils" / "tester_daemon" / "daemon_stop.json"


def write_stop_request(root: Path, reason: str = "") -> Path:
    path = stop_request_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "requested_at": utc_now_iso(),
        "reason": reason,
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return path


def read_stop_request(root: Path) -> dict[str, Any]:
    path = stop_request_path(root)
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"requested_at": "", "reason": "unreadable stop request"}
    return data if isinstance(data, dict) else {"requested_at": "", "reason": "invalid stop request"}


def clear_stop_request(root: Path) -> bool:
    path = stop_request_path(root)
    try:
        path.unlink()
        return True
    except FileNotFoundError:
        return False
