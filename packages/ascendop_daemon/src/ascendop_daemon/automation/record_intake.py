"""Isolate malformed immutable input records before building global indexes."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Callable, Iterable

from ascendop_daemon.core.atomic_io import write_json_atomic


def load_record_batch(
    paths: Iterable[Path], *, quarantine_root: Path,
    validate: Callable[[dict[str, Any]], None],
    accepted_record: Callable[[Path], dict[str, Any] | None] | None = None,
) -> tuple[dict[Path, dict[str, Any]], list[dict[str, str]]]:
    valid: dict[Path, dict[str, Any]] = {}
    errors: list[dict[str, str]] = []
    for path in paths:
        content = b""
        try:
            record = accepted_record(path) if accepted_record is not None else None
            if record is None:
                content = path.read_bytes()
                record = json.loads(content.decode("utf-8-sig"))
            if not isinstance(record, dict):
                raise ValueError("record must be a JSON object")
            validate(record)
            valid[path] = record
        except (OSError, ValueError, TypeError, KeyError) as exc:
            error = {"record_path": str(path), "error": str(exc), "state": "quarantined"}
            # Preserve the source file. The descriptor is keyed by path+bytes,
            # so repeated observation does not rewrite evidence or mint events.
            digest = hashlib.sha256(content).hexdigest()
            identity = hashlib.sha256(str(path).encode() + b"\0" + content).hexdigest()
            target = quarantine_root / f"{identity}.json"
            try:
                if not target.exists():
                    write_json_atomic(target, {
                        "schema": "ascendop.record-quarantine.v1", **error,
                        "content_sha256": digest, "source_preserved": True,
                    })
                error["quarantine_path"] = str(target)
            except OSError as quarantine_error:
                # A read-only/full quarantine volume must not silence the error
                # or turn a bad record into a global tick failure.
                error["quarantine_error"] = str(quarantine_error)
            errors.append(error)
    return valid, errors
