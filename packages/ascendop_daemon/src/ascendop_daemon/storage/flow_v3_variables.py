from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping


VARIABLE_REGISTRY_SCHEMA = "ascendop.protocol-variable-registry.v3"
REQUIRED_FIELDS = {
    "id",
    "layer",
    "type",
    "unit",
    "default",
    "minimum",
    "maximum",
    "authority",
    "producer",
    "consumers",
    "persistence",
    "retry_impact",
    "timing_impact",
    "deprecation",
}
VARIABLE_TYPES = {
    "integer",
    "number",
    "token",
    "enum",
    "boolean",
    "string",
    "string-list",
    "span",
}


class FlowV3VariableError(ValueError):
    pass


@dataclass(frozen=True)
class ProtocolVariable:
    variable_id: str
    layer: str
    value_type: str
    unit: str
    default: Any
    minimum: Any
    maximum: Any
    authority: str
    producer: str
    consumers: tuple[str, ...]
    persistence: str
    retry_impact: str
    timing_impact: str
    deprecation: str

    def validate_value(self, value: Any) -> Any:
        if self.value_type in {"integer", "span"}:
            if isinstance(value, bool) or not isinstance(value, int):
                raise FlowV3VariableError(
                    f"{self.variable_id} must be an integer {self.unit}"
                )
            if self.minimum is not None and value < int(self.minimum):
                raise FlowV3VariableError(
                    f"{self.variable_id} is below {self.minimum} {self.unit}"
                )
            if self.maximum is not None and value > int(self.maximum):
                raise FlowV3VariableError(
                    f"{self.variable_id} exceeds {self.maximum} {self.unit}"
                )
        elif self.value_type == "number":
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise FlowV3VariableError(
                    f"{self.variable_id} must be a number {self.unit}"
                )
            if self.minimum is not None and value < float(self.minimum):
                raise FlowV3VariableError(
                    f"{self.variable_id} is below {self.minimum} {self.unit}"
                )
            if self.maximum is not None and value > float(self.maximum):
                raise FlowV3VariableError(
                    f"{self.variable_id} exceeds {self.maximum} {self.unit}"
                )
        elif self.value_type == "boolean":
            if not isinstance(value, bool):
                raise FlowV3VariableError(f"{self.variable_id} must be boolean")
        elif self.value_type in {"token", "enum", "string"}:
            if not isinstance(value, str) or not value:
                raise FlowV3VariableError(f"{self.variable_id} must be non-empty text")
        elif self.value_type == "string-list":
            if not isinstance(value, list) or any(
                not isinstance(item, str) or not item.strip() for item in value
            ):
                raise FlowV3VariableError(
                    f"{self.variable_id} must be a list of non-empty text"
                )
            if len(value) != len(set(value)):
                raise FlowV3VariableError(
                    f"{self.variable_id} must not contain duplicate values"
                )
        else:
            raise FlowV3VariableError(
                f"{self.variable_id} has unsupported type {self.value_type}"
            )
        return value


class ProtocolVariableRegistry:
    def __init__(
        self,
        path: Path,
        *,
        registry_version: int,
        variables: Iterable[ProtocolVariable],
    ) -> None:
        self.path = path.resolve()
        self.registry_version = int(registry_version)
        self.variables = {item.variable_id: item for item in variables}

    @classmethod
    def load(cls, path: Path) -> "ProtocolVariableRegistry":
        resolved = path.resolve()
        try:
            raw = json.loads(resolved.read_text(encoding="utf-8-sig"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise FlowV3VariableError(
                f"protocol variable registry is unreadable: {resolved}"
            ) from exc
        if not isinstance(raw, dict):
            raise FlowV3VariableError("protocol variable registry must be an object")
        if raw.get("schema") != VARIABLE_REGISTRY_SCHEMA:
            raise FlowV3VariableError(
                f"unsupported protocol variable registry schema: {raw.get('schema')}"
            )
        version = raw.get("registry_version")
        if isinstance(version, bool) or not isinstance(version, int) or version < 1:
            raise FlowV3VariableError("registry_version must be a positive integer")
        items = raw.get("variables")
        if not isinstance(items, list) or not items:
            raise FlowV3VariableError("variables must be a non-empty list")
        variables = [parse_variable(item, index) for index, item in enumerate(items)]
        ids = [item.variable_id for item in variables]
        if len(ids) != len(set(ids)):
            raise FlowV3VariableError("protocol variable ids must be unique")
        return cls(resolved, registry_version=version, variables=variables)

    def require(self, variable_id: str) -> ProtocolVariable:
        try:
            return self.variables[variable_id]
        except KeyError as exc:
            raise FlowV3VariableError(
                f"unregistered protocol variable: {variable_id}"
            ) from exc

    def validate(self, variable_id: str, value: Any) -> Any:
        return self.require(variable_id).validate_value(value)

    def consumer_gaps(
        self,
        variable_ids: Iterable[str],
        *,
        required_consumers: Iterable[str],
    ) -> dict[str, list[str]]:
        required = set(required_consumers)
        gaps: dict[str, list[str]] = {}
        for variable_id in variable_ids:
            variable = self.require(variable_id)
            missing = sorted(required - set(variable.consumers))
            if missing:
                gaps[variable_id] = missing
        return gaps


def parse_variable(raw: Any, index: int) -> ProtocolVariable:
    if not isinstance(raw, Mapping):
        raise FlowV3VariableError(f"variables[{index}] must be an object")
    missing = sorted(REQUIRED_FIELDS - set(raw))
    unknown = sorted(set(raw) - REQUIRED_FIELDS)
    if missing:
        raise FlowV3VariableError(
            f"variables[{index}] is missing fields: {', '.join(missing)}"
        )
    if unknown:
        raise FlowV3VariableError(
            f"variables[{index}] has unknown fields: {', '.join(unknown)}"
        )
    value_type = required_text(raw["type"], f"variables[{index}].type")
    if value_type not in VARIABLE_TYPES:
        raise FlowV3VariableError(
            f"variables[{index}] has unsupported type: {value_type}"
        )
    consumers = raw["consumers"]
    if not isinstance(consumers, list) or not consumers:
        raise FlowV3VariableError(
            f"variables[{index}].consumers must be a non-empty list"
        )
    parsed = ProtocolVariable(
        variable_id=required_text(raw["id"], f"variables[{index}].id"),
        layer=required_text(raw["layer"], f"variables[{index}].layer"),
        value_type=value_type,
        unit=required_text(raw["unit"], f"variables[{index}].unit"),
        default=raw["default"],
        minimum=raw["minimum"],
        maximum=raw["maximum"],
        authority=required_text(raw["authority"], f"variables[{index}].authority"),
        producer=required_text(raw["producer"], f"variables[{index}].producer"),
        consumers=tuple(
            required_text(value, f"variables[{index}].consumers")
            for value in consumers
        ),
        persistence=required_text(
            raw["persistence"], f"variables[{index}].persistence"
        ),
        retry_impact=required_text(
            raw["retry_impact"], f"variables[{index}].retry_impact"
        ),
        timing_impact=required_text(
            raw["timing_impact"], f"variables[{index}].timing_impact"
        ),
        deprecation=required_text(
            raw["deprecation"], f"variables[{index}].deprecation"
        ),
    )
    if parsed.default is not None:
        parsed.validate_value(parsed.default)
    if (
        parsed.minimum is not None
        and parsed.maximum is not None
        and parsed.minimum > parsed.maximum
    ):
        raise FlowV3VariableError(
            f"{parsed.variable_id} has minimum greater than maximum"
        )
    return parsed


def required_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FlowV3VariableError(f"{field} must be non-empty text")
    return value.strip()
