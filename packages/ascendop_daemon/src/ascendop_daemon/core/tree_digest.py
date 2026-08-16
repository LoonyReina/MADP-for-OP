from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any


def tree_digest(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        relative = path.relative_to(root).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        if path.is_file():
            update_canonical_file_digest(digest, path)
            digest.update(b"\0")
    return digest.hexdigest()


def update_canonical_file_digest(digest: Any, path: Path) -> None:
    """Hash transport-stable bytes across Git CRLF/LF checkout policies."""

    carry = b""
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            data = carry + chunk
            carry = b"\r" if data.endswith(b"\r") else b""
            if carry:
                data = data[:-1]
            digest.update(data.replace(b"\r\n", b"\n"))
    if carry:
        digest.update(carry)
