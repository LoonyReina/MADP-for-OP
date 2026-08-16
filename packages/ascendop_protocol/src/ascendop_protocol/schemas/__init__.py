from __future__ import annotations

import json
from importlib.resources import files
from typing import Any

from .variables import (
    VARIABLE_REGISTRY_SCHEMA,
    VariableDefinition,
    VariableRegistry,
    VariableRegistryError,
)


def schema_registry() -> dict[str, Any]:
    resource = files(__package__).joinpath("schema_registry.json")
    return json.loads(resource.read_text(encoding="utf-8"))


def load_schema(schema_id: str) -> dict[str, Any]:
    registry = schema_registry()
    matches = [
        item
        for item in registry.get("entries", [])
        if item.get("schema_id") == schema_id
    ]
    if len(matches) != 1:
        raise KeyError(f"unknown or duplicate protocol schema: {schema_id}")
    resource = files(__package__).joinpath(str(matches[0]["path"]))
    schema = json.loads(resource.read_text(encoding="utf-8"))
    if schema.get("$id") != schema_id:
        raise ValueError(f"schema registry identity mismatch: {schema_id}")
    return schema


__all__ = [
    "VARIABLE_REGISTRY_SCHEMA",
    "VariableDefinition",
    "VariableRegistry",
    "VariableRegistryError",
    "load_schema",
    "schema_registry",
]
