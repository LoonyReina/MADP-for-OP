from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


VARIABLE_REGISTRY_SCHEMA = "ascendop.protocol-variable-registry.v3"
SUPPORTED_TYPES = {
    "boolean",
    "enum",
    "integer",
    "number",
    "object",
    "span",
    "string",
    "string-list",
    "token",
}


class VariableRegistryError(ValueError):
    pass


@dataclass(frozen=True)
class VariableDefinition:
    variable_id: str
    value_type: str
    default: Any
    minimum: int | float | None
    maximum: int | float | None
    raw: Mapping[str, Any]

    def validate(self, value: Any) -> Any:
        if value is None:
            return None
        if self.value_type == "boolean":
            if not isinstance(value, bool):
                raise VariableRegistryError(
                    f"{self.variable_id} must be boolean"
                )
        elif self.value_type in {"integer", "span"}:
            if isinstance(value, bool) or not isinstance(value, int):
                raise VariableRegistryError(
                    f"{self.variable_id} must be an integer"
                )
        elif self.value_type == "number":
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise VariableRegistryError(
                    f"{self.variable_id} must be numeric"
                )
        elif self.value_type in {"enum", "string", "token"}:
            if not isinstance(value, str) or not value.strip():
                raise VariableRegistryError(
                    f"{self.variable_id} must be non-empty text"
                )
            value = value.strip()
        elif self.value_type == "string-list":
            if not isinstance(value, list) or any(
                not isinstance(item, str) or not item.strip() for item in value
            ):
                raise VariableRegistryError(
                    f"{self.variable_id} must be a list of non-empty text values"
                )
            value = [item.strip() for item in value]
            if len(value) != len(set(value)):
                raise VariableRegistryError(
                    f"{self.variable_id} must not contain duplicate values"
                )
        elif self.value_type == "object":
            if not isinstance(value, Mapping) or any(
                not isinstance(key, str) or not key.strip() for key in value
            ):
                raise VariableRegistryError(
                    f"{self.variable_id} must be an object with non-empty text keys"
                )
            value = dict(value)
            try:
                json.dumps(value, ensure_ascii=True, allow_nan=False)
            except (TypeError, ValueError) as exc:
                raise VariableRegistryError(
                    f"{self.variable_id} must contain JSON-compatible values"
                ) from exc
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if self.minimum is not None and value < self.minimum:
                raise VariableRegistryError(
                    f"{self.variable_id} is below minimum {self.minimum}"
                )
            if self.maximum is not None and value > self.maximum:
                raise VariableRegistryError(
                    f"{self.variable_id} is above maximum {self.maximum}"
                )
        return value


@dataclass(frozen=True)
class VariableRegistry:
    path: Path
    registry_version: int
    definitions: Mapping[str, VariableDefinition]
    digest: str

    @classmethod
    def load(cls, path: Path) -> "VariableRegistry":
        resolved = path.resolve()
        try:
            raw = json.loads(resolved.read_text(encoding="utf-8-sig"))
        except (OSError, json.JSONDecodeError) as exc:
            raise VariableRegistryError(
                f"cannot read variable registry {resolved}: {exc}"
            ) from exc
        if not isinstance(raw, dict) or raw.get("schema") != VARIABLE_REGISTRY_SCHEMA:
            raise VariableRegistryError("unsupported variable registry schema")
        rows = raw.get("variables")
        if not isinstance(rows, list) or not rows:
            raise VariableRegistryError("variable registry must contain variables")
        definitions: dict[str, VariableDefinition] = {}
        for row in rows:
            if not isinstance(row, dict):
                raise VariableRegistryError("variable definition must be an object")
            variable_id = str(row.get("id") or "").strip()
            value_type = str(row.get("type") or "").strip()
            if not variable_id or variable_id in definitions:
                raise VariableRegistryError(
                    f"missing or duplicate variable id: {variable_id or '<empty>'}"
                )
            if value_type not in SUPPORTED_TYPES:
                raise VariableRegistryError(
                    f"unsupported type for {variable_id}: {value_type}"
                )
            definition = VariableDefinition(
                variable_id=variable_id,
                value_type=value_type,
                default=row.get("default"),
                minimum=_number_or_none(row.get("minimum"), variable_id, "minimum"),
                maximum=_number_or_none(row.get("maximum"), variable_id, "maximum"),
                raw=dict(row),
            )
            if definition.default is not None:
                definition.validate(definition.default)
            if (
                definition.minimum is not None
                and definition.maximum is not None
                and definition.minimum > definition.maximum
            ):
                raise VariableRegistryError(
                    f"invalid bounds for {variable_id}"
                )
            definitions[variable_id] = definition
        canonical = json.dumps(raw, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        return cls(
            path=resolved,
            registry_version=int(raw.get("registry_version") or 0),
            definitions=definitions,
            digest=hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        )

    def definition(self, variable_id: str) -> VariableDefinition:
        try:
            return self.definitions[variable_id]
        except KeyError as exc:
            raise VariableRegistryError(
                f"unregistered protocol variable: {variable_id}"
            ) from exc

    def value(self, variable_id: str, override: Any = None, *, supplied: bool = False) -> Any:
        definition = self.definition(variable_id)
        value = override if supplied else definition.default
        return definition.validate(value)

    def resolve(self, overrides: Mapping[str, Any] | None = None) -> dict[str, Any]:
        supplied = dict(overrides or {})
        unknown = sorted(set(supplied) - set(self.definitions))
        if unknown:
            raise VariableRegistryError(
                "unregistered protocol variable override(s): " + ", ".join(unknown)
            )
        return {
            variable_id: definition.validate(
                supplied[variable_id]
                if variable_id in supplied
                else definition.default
            )
            for variable_id, definition in self.definitions.items()
        }


def _number_or_none(value: Any, variable_id: str, field: str) -> int | float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise VariableRegistryError(f"{variable_id}.{field} must be numeric or null")
    return value
