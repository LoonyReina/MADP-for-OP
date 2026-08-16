from __future__ import annotations

import json
import sqlite3
from typing import Any


def decode_operator_row(row: sqlite3.Row) -> dict[str, Any]:
    value = dict(row)
    for key in (
        "definition_json",
        "workspace_json",
        "requirements_json",
        "cache_policy_json",
        "routing_policy_json",
    ):
        value[key.removesuffix("_json")] = json.loads(value.pop(key))
    value["source_present"] = bool(value["source_present"])
    return value


def decode_request_row(row: sqlite3.Row) -> dict[str, Any]:
    value = dict(row)
    value["execution_requirements"] = json.loads(value.pop("requirements_json"))
    value["manifest"] = json.loads(value.pop("manifest_json"))
    return value


def decode_attempt_row(row: sqlite3.Row) -> dict[str, Any]:
    value = dict(row)
    value["route"] = json.loads(value.pop("route_json"))
    return value


def decode_preparation_row(row: sqlite3.Row) -> dict[str, Any]:
    value = dict(row)
    value["route"] = json.loads(value.pop("route_json"))
    value["payload"] = json.loads(value.pop("payload_json"))
    return value


def decode_outbox_row(row: sqlite3.Row) -> dict[str, Any]:
    value = dict(row)
    value["payload"] = json.loads(value.pop("payload_json"))
    value["remote_receipt"] = json.loads(value.pop("remote_receipt_json"))
    return value


def decode_return_row(row: sqlite3.Row) -> dict[str, Any]:
    value = dict(row)
    value["payload"] = json.loads(value.pop("payload_json"))
    if "projection_json" in value:
        value["projection"] = json.loads(value.pop("projection_json"))
    return value


def decode_assistant_action(
    row: sqlite3.Row,
    *,
    idempotent: bool,
) -> dict[str, Any]:
    value = dict(row)
    value["request"] = json.loads(value.pop("request_json"))
    value["metrics"] = json.loads(value.pop("metrics_json"))
    value["idempotent"] = idempotent
    return value


def decode_assistant_receipt(
    row: sqlite3.Row,
    *,
    idempotent: bool,
) -> dict[str, Any]:
    value = dict(row)
    value["receipt"] = json.loads(value.pop("receipt_json"))
    value["idempotent"] = idempotent
    return value
