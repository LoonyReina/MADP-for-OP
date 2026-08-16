from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ascendop_daemon.workflow.workflow_profiles import (
    WorkflowInstance,
    WorkflowProfile,
    WorkflowProfileError,
    WorkflowOperationContract,
    WorkflowSnapshot,
    relative_path,
)


class CannJudgeOperatorProfile(WorkflowProfile):
    """Read-only compatibility view over the existing TestUtils workflow."""

    profile_id = "cannjudge-operator.v1"
    profile_revision = "1.0"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        configured_profile_id = self.config.get("profile_id")
        if configured_profile_id is not None:
            if not isinstance(configured_profile_id, str) or not configured_profile_id:
                raise WorkflowProfileError(
                    "CANNJudge config.profile_id must be non-empty text"
                )
            self.profile_id = configured_profile_id

    def discover(self) -> tuple[WorkflowInstance, ...]:
        season_id = self._config_string("season_id")
        spec_path = self.root / "operators" / season_id / "specs" / "operator_specs.json"
        specs = read_object(spec_path)
        operators = specs.get("operators")
        if not isinstance(operators, list):
            raise WorkflowProfileError(
                f"CANNJudge operator specs require an operators list: {spec_path}"
            )
        instances: list[WorkflowInstance] = []
        for row in operators:
            if not isinstance(row, dict):
                raise WorkflowProfileError(
                    f"CANNJudge operator spec rows must be objects: {spec_path}"
                )
            op = required_string(row, "op", source=spec_path)
            instances.append(
                WorkflowInstance(
                    profile_id=self.profile_id,
                    profile_revision=self.profile_revision,
                    instance_id=f"cannjudge:{season_id}:{op}",
                    domain="competition",
                    season_id=season_id,
                    subject_kind="operator",
                    subject_id=op,
                    state_path="TestUtils",
                    metadata={
                        "compatibility_mode": "legacy-testutils-v1",
                        "operator_spec_path": relative_path(self.root, spec_path),
                        "case_protocol": row.get("case_protocol", specs.get("case_protocol")),
                    },
                )
            )
        return tuple(instances)

    def read_snapshot(self, instance: WorkflowInstance) -> WorkflowSnapshot:
        if instance.profile_id != self.profile_id:
            raise WorkflowProfileError(
                f"instance belongs to {instance.profile_id}, not {self.profile_id}"
            )
        testutils = self.root / "TestUtils"
        warnings: list[str] = []
        if not testutils.is_dir():
            warnings.append("legacy TestUtils state root is missing")
        return WorkflowSnapshot(
            instance=instance,
            generation=0,
            state="compatibility-active",
            checkpoints={
                "legacy_gate_engine": {
                    "state": "authoritative",
                    "writer": "existing-daemon",
                },
                "wire_v3_runtime": {
                    "state": "authoritative",
                    "writer": "flow-v3-daemon",
                },
            },
            context={
                "state_root": "TestUtils",
                "result_root": "operators_testresult",
                "compatibility_contract": (
                    "GateEngine chooses the real workflow gate; Flow V3 owns runtime "
                    "execution state; TestUtils and operators_testresult remain the "
                    "CANNJudge result evidence roots."
                ),
            },
            warnings=tuple(warnings),
        )

    def operation_contracts(self) -> tuple[WorkflowOperationContract, ...]:
        return (
            WorkflowOperationContract(
                operation_type="cannjudge.operator-test",
                operation_version="v1",
                owner="daemon",
                required_capabilities=(
                    "flow-v3",
                    "engine-archive",
                    "correctness-first",
                ),
                ingest_adapter="cannjudge.testutils-result.v3",
            ),
            WorkflowOperationContract(
                operation_type="cannjudge.profiler.primary-all-cases",
                operation_version="v1",
                owner="daemon",
                required_capabilities=(
                    "flow-v3",
                    "diagnostic-profile",
                    "profiler-primary-all-cases",
                ),
                ingest_adapter="cannjudge.profiler-evidence.v3",
            ),
            WorkflowOperationContract(
                operation_type="cannjudge.profiler.primary-roofline-all-cases",
                operation_version="v1",
                owner="daemon",
                required_capabilities=(
                    "flow-v3",
                    "diagnostic-profile",
                    "profiler-primary-roofline-all-cases",
                ),
                ingest_adapter="cannjudge.profiler-evidence.v3",
            ),
            WorkflowOperationContract(
                operation_type="cannjudge.diagnostic.correctness-replay",
                operation_version="v1",
                owner="daemon",
                required_capabilities=(
                    "flow-v3",
                    "engine-archive",
                    "correctness-first",
                    "diagnostic-correctness-replay",
                ),
                ingest_adapter="cannjudge.solver-diagnostic-evidence.v1",
            ),
        )

    def _config_string(self, key: str) -> str:
        value = self.config.get(key)
        if not isinstance(value, str) or not value:
            raise WorkflowProfileError(
                f"CANNJudge workflow profile requires config.{key}"
            )
        return value


def read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise WorkflowProfileError(f"cannot read JSON object {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise WorkflowProfileError(f"JSON value must be an object: {path}")
    return value


def required_string(value: dict[str, Any], key: str, *, source: Path) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise WorkflowProfileError(f"{source} requires non-empty {key}")
    return item
