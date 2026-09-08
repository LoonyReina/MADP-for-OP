"""V5 performance evidence in the existing test-terminal transaction.

No mutable campaign/anchor file: baseline selection reads accepted terminal
facts. A new matrix or execution environment starts another unchanged-source
measurement; prompt/daemon/transport-only updates do not invalidate timings.
"""
from __future__ import annotations

import math
import json
from pathlib import Path


def execution_identity(endpoint, release):
    return {"endpoint_id": endpoint["endpoint_id"], "hardware": endpoint["hardware"],
            "engine_generation": release["engine_code_generation"],
            "test_plan_generation": release["test_plan_generation"]}


def baseline_binding(database, *, operator, case_sha256, environment, pending=None):
    # Delivery's first-page API is not an evidence query. Filter immutable facts
    # by this operator/matrix without losing anchors behind old queue history.
    with database.connection() as connection:
        rows = [json.loads(row["payload_json"]) for row in connection.execute(
            "SELECT payload_json FROM control_outbox_v5 WHERE topic='test.terminal' "
            "AND json_extract(payload_json,'$.continuation.operator')=? "
            "AND json_extract(payload_json,'$.continuation.business_summary.performance.case_sha256')=? "
            "ORDER BY created_at,outbox_id", (operator, case_sha256)).fetchall()]
    if pending is not None:
        rows.append(pending)
    candidates = []
    for row in rows:
        intent = row.get("continuation") or {}
        summary = intent.get("business_summary") or {}
        measurement = summary.get("performance") or {}
        if (intent.get("operator") == operator and summary.get("full_correctness_pass") is True
                and measurement.get("qualified") is True
                and measurement.get("case_sha256") == case_sha256
                and measurement.get("environment") == environment):
            candidates.append(measurement)
    # Minimize the same-matrix aggregate; deterministic ties retain one original
    # result identity. No fixed percentage or round-count acceptance threshold.
    if candidates:
        newest = max(candidates, key=lambda item: (item.get("observed_at", ""), item["request_id"]))
        candidates = [item for item in candidates if all(item.get(name) == newest.get(name) for name in (
            "measurement_contract_sha256", "measurement_pipeline_sha256", "runtime_environment"))]
    best = min(candidates, key=lambda item: (item["total_time_us"], item["request_id"]), default=None)
    latest = rows[-1] if rows else {}
    latest_summary = (latest.get("continuation") or {}).get("business_summary") or {}
    event = latest.get("event") or {}
    repair = (not best and latest_summary.get("full_correctness_pass") is False
              and event.get("outcome") == "failed"
              and event.get("failure_domain") not in {"host-build", "protocol", "transport", "export"})
    return {"phase": "comparison" if best else "baseline_repair" if repair else "baseline_bootstrap",
            "mode": "comparison" if best else "bootstrap",
            "baseline_result_id": best["request_id"] if best else None,
            "baseline_measurement": best, "case_sha256": case_sha256,
            "environment": environment, "round_credit": "comparison-eligible" if best else "anchor-only"}


def operation_parameters(context, *, source_sha256):
    if (context.get("test") or {}).get("operation_code") != "test.performance":
        return {}
    baseline = context.get("performance_baseline") or {}
    if baseline.get("phase") == "baseline_bootstrap":
        if source_sha256 != context["input_source_sha256"]:
            raise ValueError("performance baseline source changed before freeze")
        return {"baseline_bootstrap": True}
    if baseline.get("phase") == "baseline_repair":
        return {"baseline_bootstrap": True}
    if baseline.get("phase") != "comparison" or not baseline.get("baseline_result_id"):
        raise ValueError("performance candidate has no admitted baseline")
    return {"baseline_result_id": baseline["baseline_result_id"]}


def qualify_measurement(capture, parsed, *, case_ids):
    if (capture.get("case_ids") != case_ids or parsed.get("case_ids") != case_ids
            or capture.get("expected_logical_samples_per_case") != 50
            or parsed.get("state") != "parsed" or parsed.get("failures")
            or parsed.get("measurement_input_verified") is not True):
        raise ValueError("performance result does not cover the original full matrix")
    for name in ("measurement_contract_sha256", "measurement_pipeline_sha256"):
        if not capture.get(name) or capture[name] != parsed.get(name):
            raise ValueError("capture/parser measurement contract differs")
    cases = parsed.get("cases") or []
    if [case.get("case") for case in cases] != case_ids:
        raise ValueError("performance case rows are missing, duplicated or reordered")
    timings = []
    for case in cases:
        samples = case.get("samples_us") or []
        window = case.get("window_samples_us") or []
        if (case.get("sample_count") != 50 or len(samples) != 50
                or case.get("window_start") != 15 or case.get("window_end") != 35
                or len(window) != 20 or window != samples[15:35]
                or any(isinstance(value, bool) or not isinstance(value, (int, float))
                       or not math.isfinite(value) or value <= 0 for value in samples)):
            raise ValueError("performance requires raw50 and its exact middle20 window")
        mean = sum(window) / 20
        if not math.isclose(float(case["time_use_us"]), mean, rel_tol=1e-5, abs_tol=1e-5):
            raise ValueError("performance case duration differs from measured samples")
        timings.append({"case": case["case"], "time_us": mean})
    if not timings:
        raise ValueError("performance measurement has no cases")
    return {"qualified": True, "cases": timings, "total_time_us": sum(row["time_us"] for row in timings),
            "samples_per_case": 50, "window": [15, 35],
            **{name: capture[name] for name in ("measurement_contract_sha256", "measurement_pipeline_sha256")}}
