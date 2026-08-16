from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Callable, TypeVar


DEFAULT_RETRY_SECONDS = 2.0
_T = TypeVar("_T")


def retry_file_operation(
    operation: Callable[[], _T],
    *,
    timeout_seconds: float = DEFAULT_RETRY_SECONDS,
) -> _T:
    """Retry bounded, transient filesystem sharing failures.

    Windows readers can temporarily prevent replace/unlink. Kernel locks and
    immutable request identities remain the authority; this helper only makes
    journal metadata publication resilient to those short sharing windows.
    """

    deadline = time.monotonic() + max(0.0, float(timeout_seconds))
    delay = 0.005
    while True:
        try:
            return operation()
        except OSError as exc:
            if not is_transient_sharing_error(exc) or time.monotonic() >= deadline:
                raise
            time.sleep(delay)
            delay = min(0.05, delay * 2.0)


def is_transient_sharing_error(exc: OSError) -> bool:
    if isinstance(exc, PermissionError):
        return True
    return os.name == "nt" and getattr(exc, "winerror", None) in {5, 32}


def unlink_file(path: Path, *, missing_ok: bool = True) -> None:
    retry_file_operation(lambda: path.unlink(missing_ok=missing_ok))


def atomic_write_json(
    path: Path,
    payload: dict[str, Any],
    *,
    sort_keys: bool = True,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(
        f".{path.name}.{os.getpid()}.{threading.get_ident()}.{time.time_ns()}.tmp"
    )
    temp.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=sort_keys) + "\n",
        encoding="utf-8",
    )
    try:
        retry_file_operation(lambda: os.replace(temp, path))
    finally:
        unlink_file(temp, missing_ok=True)


def read_json_object(path: Path) -> dict[str, Any]:
    text = retry_file_operation(lambda: path.read_text(encoding="utf-8-sig"))
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ValueError(f"JSON document must be an object: {path}")
    return payload
