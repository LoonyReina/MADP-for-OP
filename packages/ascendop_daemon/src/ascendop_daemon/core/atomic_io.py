from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path
from typing import Any


_WINDOWS_TRANSIENT_ERRORS = {5, 32, 33}


def transient_replace_error(exc: OSError) -> bool:
    return isinstance(exc, PermissionError) or getattr(exc, "winerror", None) in (
        _WINDOWS_TRANSIENT_ERRORS
    )


def write_json_atomic(
    path: Path,
    payload: dict[str, Any],
    *,
    ensure_ascii: bool = False,
    sort_keys: bool = False,
    timeout_seconds: float = 2.0,
) -> None:
    """Publish one complete JSON object despite concurrent Windows readers."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".write-{uuid.uuid4().hex[:12]}.tmp")
    try:
        temp.write_text(
            json.dumps(
                payload,
                ensure_ascii=ensure_ascii,
                indent=2,
                sort_keys=sort_keys,
            )
            + "\n",
            encoding="utf-8",
        )
        deadline = time.monotonic() + max(0.0, float(timeout_seconds))
        delay = 0.01
        while True:
            try:
                os.replace(temp, path)
                return
            except OSError as exc:
                if not transient_replace_error(exc) or time.monotonic() >= deadline:
                    raise
                time.sleep(delay)
                delay = min(delay * 1.5, 0.2)
    finally:
        try:
            temp.unlink(missing_ok=True)
        except OSError:
            pass


def write_text_atomic(
    path: Path,
    value: str,
    *,
    encoding: str = "utf-8",
    timeout_seconds: float = 2.0,
) -> None:
    """Publish one complete text file despite concurrent Windows readers."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".write-{uuid.uuid4().hex[:12]}.tmp")
    try:
        temp.write_text(value, encoding=encoding)
        deadline = time.monotonic() + max(0.0, float(timeout_seconds))
        delay = 0.01
        while True:
            try:
                os.replace(temp, path)
                return
            except OSError as exc:
                if not transient_replace_error(exc) or time.monotonic() >= deadline:
                    raise
                time.sleep(delay)
                delay = min(delay * 1.5, 0.2)
    finally:
        try:
            temp.unlink(missing_ok=True)
        except OSError:
            pass


def read_json_mapping(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}
