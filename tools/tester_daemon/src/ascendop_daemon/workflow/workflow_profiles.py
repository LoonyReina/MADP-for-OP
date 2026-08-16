from __future__ import annotations

import importlib
import json
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable


PROFILE_MANIFEST_SCHEMA = "ascendop.workflow-profile-manifest.v1"


class WorkflowProfileError(ValueError):
    pass


@dataclass(frozen=True)
class WorkflowInstance:
    profile_id: str
    profile_revision: str
    instance_id: str
    domain: str
    season_id: str
    subject_kind: str
    subject_id: str
    state_path: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class WorkflowSnapshot:
    instance: WorkflowInstance
    generation: int
    state: str
    checkpoints: dict[str, Any]
    context: dict[str, Any]
    warnings: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["warnings"] = list(self.warnings)
        return value


@dataclass(frozen=True)
class WorkflowGate:
    instance_id: str
    gate_id: str
    checkpoint: str
    owner: str
    operation_type: str
    state_generation: int
    actionable: bool
    reason: str
    context_paths: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["context_paths"] = list(self.context_paths)
        return value


@dataclass(frozen=True)
class WorkflowOperationContract:
    operation_type: str
    operation_version: str
    owner: str
    required_capabilities: tuple[str, ...]
    ingest_adapter: str
    external_side_effect: bool = False

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["required_capabilities"] = list(self.required_capabilities)
        return value


class WorkflowProfile(ABC):
    profile_id: str
    profile_revision: str

    def __init__(
        self,
        root: Path,
        *,
        manifest_path: Path,
        config: dict[str, Any] | None = None,
        manifest_extensions: dict[str, Any] | None = None,
    ) -> None:
        self.root = root.resolve()
        self.manifest_path = manifest_path.resolve()
        self.config = dict(config or {})
        self.manifest_extensions = dict(manifest_extensions or {})

    @abstractmethod
    def discover(self) -> tuple[WorkflowInstance, ...]:
        raise NotImplementedError

    @abstractmethod
    def read_snapshot(self, instance: WorkflowInstance) -> WorkflowSnapshot:
        raise NotImplementedError

    def derive_gates(
        self,
        snapshot: WorkflowSnapshot,
    ) -> tuple[WorkflowGate, ...]:
        return ()

    def operation_contracts(self) -> tuple[WorkflowOperationContract, ...]:
        return ()

    def operation_contract(
        self,
        operation_type: str,
        operation_version: str,
    ) -> WorkflowOperationContract:
        matches = [
            item
            for item in self.operation_contracts()
            if item.operation_type == operation_type
            and item.operation_version == operation_version
        ]
        if len(matches) != 1:
            raise WorkflowProfileError(
                f"unsupported operation for {self.profile_id}: "
                f"{operation_type}@{operation_version}"
            )
        return matches[0]

    def status(self) -> dict[str, Any]:
        instances = self.discover()
        rows: list[dict[str, Any]] = []
        for instance in instances:
            snapshot = self.read_snapshot(instance)
            rows.append(
                {
                    "instance": instance.to_dict(),
                    "snapshot": snapshot.to_dict(),
                    "gates": [
                        gate.to_dict() for gate in self.derive_gates(snapshot)
                    ],
                }
            )
        return {
            "profile_id": self.profile_id,
            "profile_revision": self.profile_revision,
            "manifest_path": relative_path(self.root, self.manifest_path),
            "manifest_extensions": self.manifest_extensions,
            "instance_count": len(rows),
            "operation_contracts": [
                item.to_dict() for item in self.operation_contracts()
            ],
            "instances": rows,
        }


class WorkflowProfileRegistry:
    def __init__(
        self,
        root: Path,
        *,
        supported_manifest_extensions: Iterable[str] = (),
    ) -> None:
        self.root = root.resolve()
        self.supported_manifest_extensions = frozenset(
            str(item) for item in supported_manifest_extensions
        )
        self._profiles: dict[str, WorkflowProfile] = {}

    @property
    def profiles(self) -> tuple[WorkflowProfile, ...]:
        return tuple(self._profiles[key] for key in sorted(self._profiles))

    def register(self, profile: WorkflowProfile) -> None:
        if profile.profile_id in self._profiles:
            raise WorkflowProfileError(
                f"duplicate workflow profile: {profile.profile_id}"
            )
        self._profiles[profile.profile_id] = profile

    def get(self, profile_id: str) -> WorkflowProfile:
        try:
            return self._profiles[profile_id]
        except KeyError as exc:
            raise WorkflowProfileError(
                f"unknown workflow profile: {profile_id}"
            ) from exc

    def load_manifests(
        self,
        paths: Iterable[Path] | None = None,
    ) -> tuple[WorkflowProfile, ...]:
        manifest_paths = (
            tuple(paths)
            if paths is not None
            else tuple(
                sorted(
                    (self.root / "operators").glob("*/workflow_profile.json")
                )
            )
        )
        loaded: list[WorkflowProfile] = []
        for path in manifest_paths:
            profile = load_profile_manifest(
                self.root,
                path,
                supported_extensions=self.supported_manifest_extensions,
            )
            if profile is None:
                continue
            self.register(profile)
            loaded.append(profile)
        return tuple(loaded)

    def status(self, *, profile_id: str = "", instance_id: str = "") -> dict[str, Any]:
        selected = (
            (self.get(profile_id),)
            if profile_id
            else self.profiles
        )
        profile_rows: list[dict[str, Any]] = []
        for profile in selected:
            status = profile.status()
            if instance_id:
                instances = [
                    row
                    for row in status["instances"]
                    if row["instance"]["instance_id"] == instance_id
                ]
                status = {**status, "instances": instances, "instance_count": len(instances)}
            profile_rows.append(status)
        return {
            "schema": "ascendop.workflow-status.v1",
            "profile_count": len(profile_rows),
            "profiles": profile_rows,
        }


def load_profile_manifest(
    root: Path,
    path: Path,
    *,
    supported_extensions: Iterable[str] = (),
) -> WorkflowProfile | None:
    resolved = path if path.is_absolute() else root / path
    try:
        value = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise WorkflowProfileError(
            f"cannot read workflow profile manifest {resolved}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise WorkflowProfileError(
            f"workflow profile manifest must be an object: {resolved}"
        )
    if value.get("schema") != PROFILE_MANIFEST_SCHEMA:
        raise WorkflowProfileError(
            f"unsupported workflow profile manifest schema: {value.get('schema')}"
        )
    if not bool(value.get("enabled", True)):
        return None
    profile_id = required_string(value, "profile_id")
    adapter = required_string(value, "adapter")
    config = value.get("config", {})
    if not isinstance(config, dict):
        raise WorkflowProfileError(
            f"workflow profile config must be an object: {resolved}"
        )
    extensions = value.get("extensions", {})
    if not isinstance(extensions, dict):
        raise WorkflowProfileError(
            f"workflow profile extensions must be an object: {resolved}"
        )
    required_extensions = value.get("required_extensions", [])
    if not isinstance(required_extensions, list) or any(
        not isinstance(item, str) or not item for item in required_extensions
    ):
        raise WorkflowProfileError(
            f"workflow profile required_extensions must be a string list: {resolved}"
        )
    undeclared = sorted(set(required_extensions) - set(extensions))
    if undeclared:
        raise WorkflowProfileError(
            "workflow profile required extension data is missing: "
            + ", ".join(undeclared)
        )
    unsupported = sorted(set(required_extensions) - set(supported_extensions))
    if unsupported:
        raise WorkflowProfileError(
            "unsupported required workflow profile extensions: "
            + ", ".join(unsupported)
        )
    cls = load_adapter_class(adapter)
    profile = cls(
        root,
        manifest_path=resolved,
        config=config,
        manifest_extensions=extensions,
    )
    if not isinstance(profile, WorkflowProfile):
        raise WorkflowProfileError(
            f"workflow profile adapter is not a WorkflowProfile: {adapter}"
        )
    if profile.profile_id != profile_id:
        raise WorkflowProfileError(
            f"profile id mismatch: manifest={profile_id} adapter={profile.profile_id}"
        )
    return profile


def load_adapter_class(adapter: str) -> type[WorkflowProfile]:
    module_name, separator, class_name = adapter.partition(":")
    if not separator or not module_name or not class_name:
        raise WorkflowProfileError(
            "workflow profile adapter must use module.path:ClassName"
        )
    try:
        module = importlib.import_module(module_name)
        value = getattr(module, class_name)
    except (ImportError, AttributeError) as exc:
        raise WorkflowProfileError(
            f"cannot import workflow profile adapter {adapter}: {exc}"
        ) from exc
    if not isinstance(value, type):
        raise WorkflowProfileError(
            f"workflow profile adapter is not a class: {adapter}"
        )
    return value


def required_string(value: dict[str, Any], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise WorkflowProfileError(f"workflow profile manifest requires {key}")
    return item


def relative_path(root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())
