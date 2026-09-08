from __future__ import annotations

import hashlib
import json
import os
import secrets
from pathlib import Path
from typing import Any

from ascendop_daemon.core.atomic_io import write_json_atomic


CREDENTIAL_SCHEMA = "ascendop.control-api-credentials.v1"
TOKEN_SCHEMA = "ascendop.control-api-token-hashes.v1"


def ensure_control_api_credentials(root: Path) -> dict[str, str]:
    directory = root.resolve() / ".ascendop-work" / "runtime" / "control-api"
    credentials_path = directory / "client-credentials.json"
    token_path = directory / "token-hashes.json"
    existing = _read_json(credentials_path)
    token_document = _read_json(token_path)
    if _documents_match(existing, token_document):
        return {
            "credentials_path": str(credentials_path),
            "token_path": str(token_path),
        }

    credentials = {
        "schema": CREDENTIAL_SCHEMA,
        "tokens": [
            {"capability": capability, "token": secrets.token_urlsafe(32)}
            for capability in ("viewer", "operator", "admin")
        ],
    }
    hashes = {
        "schema": TOKEN_SCHEMA,
        "tokens": [
            {
                "capability": item["capability"],
                "token_sha256": hashlib.sha256(
                    item["token"].encode("utf-8")
                ).hexdigest(),
            }
            for item in credentials["tokens"]
        ],
    }
    directory.mkdir(parents=True, exist_ok=True)
    write_json_atomic(credentials_path, credentials, ensure_ascii=True, sort_keys=True)
    write_json_atomic(token_path, hashes, ensure_ascii=True, sort_keys=True)
    _restrict(credentials_path)
    _restrict(token_path)
    return {
        "credentials_path": str(credentials_path),
        "token_path": str(token_path),
    }


def _documents_match(credentials: dict[str, Any], hashes: dict[str, Any]) -> bool:
    if credentials.get("schema") != CREDENTIAL_SCHEMA:
        return False
    if hashes.get("schema") != TOKEN_SCHEMA:
        return False
    raw_tokens = credentials.get("tokens")
    hash_tokens = hashes.get("tokens")
    if not isinstance(raw_tokens, list) or not isinstance(hash_tokens, list):
        return False
    expected = {
        str(item.get("capability") or ""): hashlib.sha256(
            str(item.get("token") or "").encode("utf-8")
        ).hexdigest()
        for item in raw_tokens
        if isinstance(item, dict)
    }
    observed = {
        str(item.get("capability") or ""): str(item.get("token_sha256") or "")
        for item in hash_tokens
        if isinstance(item, dict)
    }
    return set(expected) == {"viewer", "operator", "admin"} and expected == observed


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _restrict(path: Path) -> None:
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
