from __future__ import annotations

import pytest

from ascendop_protocol.interleaved_measurement import (
    InterleavedMeasurementContractError,
    interleaved_session_schedule,
    normalize_interleaved_measurement_plan,
)


def _plan() -> dict[str, object]:
    return {
        "schema": "ascendop.interleaved-measurement-plan.v1",
        "artifact_binding": "single-built-artifact",
        "artifact_identity": {"source_sha256": "a" * 64},
        "labels": ["A", "B"],
        "sessions": [
            {
                "session_id": "abba",
                "order": ["A", "B", "B", "A"],
                "block_count": 5,
                "invocations_per_epoch": 5,
            },
            {
                "session_id": "baab",
                "order": ["B", "A", "A", "B"],
                "block_count": 5,
                "invocations_per_epoch": 5,
            },
        ],
        "samples_per_label_per_case": 50,
        "middle_window": {"start": 15, "end": 35},
        "pair_key_fields": [
            "session_id",
            "case_id",
            "block_id",
            "within_label_invocation_position",
        ],
    }


def test_interleaved_plan_builds_balanced_abba_and_baab_schedules() -> None:
    plan = normalize_interleaved_measurement_plan(_plan())
    abba = interleaved_session_schedule(plan, "abba", case_id=7)
    baab = interleaved_session_schedule(plan, "baab", case_id=7)

    assert len(abba) == len(baab) == 100
    assert [item["label"] for item in abba[:20]] == (
        ["A"] * 5 + ["B"] * 10 + ["A"] * 5
    )
    assert [item["label"] for item in baab[:20]] == (
        ["B"] * 5 + ["A"] * 10 + ["B"] * 5
    )
    assert {item["label"] for item in abba} == {"A", "B"}
    assert [item["within_label_invocation_position"] for item in abba if item["label"] == "A"] == list(range(50))
    assert [item["within_label_invocation_position"] for item in abba if item["label"] == "B"] == list(range(50))
    assert plan["logical_samples_per_case"] == 100
    assert len(plan["plan_digest"]) == 64


def test_interleaved_plan_rejects_label_specific_artifacts_and_unbalanced_order() -> None:
    plan = _plan()
    plan["artifact_binding"] = "label-specific-artifacts"
    with pytest.raises(InterleavedMeasurementContractError, match="share one"):
        normalize_interleaved_measurement_plan(plan)

    plan = _plan()
    sessions = plan["sessions"]
    assert isinstance(sessions, list)
    assert isinstance(sessions[0], dict)
    sessions[0]["order"] = ["A", "A", "B"]
    with pytest.raises(InterleavedMeasurementContractError, match="balance"):
        normalize_interleaved_measurement_plan(plan)
