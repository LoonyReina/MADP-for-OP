from __future__ import annotations

import copy
import json
import sqlite3
from types import SimpleNamespace

import pytest

from ascendop_daemon.automation import performance_policy as perf


def _measurement():
    capture = {"case_ids": [1, 2], "expected_logical_samples_per_case": 50,
               "measurement_contract_sha256": "a" * 64, "measurement_pipeline_sha256": "b" * 64}
    parsed = {**capture, "state": "parsed", "failures": [], "measurement_input_verified": True,
              "cases": [{"case": case, "sample_count": 50, "samples_us": [float(case)] * 50,
                         "window_samples_us": [float(case)] * 20, "window_start": 15, "window_end": 35,
                         "time_use_us": float(case)} for case in [1, 2]]}
    return capture, parsed


def test_raw50_middle20_qualification():
    result = perf.qualify_measurement(*_measurement(), case_ids=[1, 2])
    assert result["qualified"] and result["total_time_us"] == 3


@pytest.mark.parametrize("fault", ["missing", "duplicate", "nan", "zero", "window", "contract", "unchecked", "duration"])
def test_unqualified_profiles_are_not_anchors(fault):
    capture, parsed = _measurement()
    if fault == "missing": parsed["cases"].pop()
    elif fault == "duplicate": parsed["cases"][1]["case"] = 1
    elif fault == "nan": parsed["cases"][0]["samples_us"][0] = float("nan")
    elif fault == "zero": parsed["cases"][0]["samples_us"][0] = 0
    elif fault == "window": parsed["cases"][0]["window_samples_us"][0] = 100
    elif fault == "contract": parsed["measurement_contract_sha256"] = "other"
    elif fault == "unchecked": parsed["measurement_input_verified"] = False
    elif fault == "duration": parsed["cases"][0]["time_use_us"] = 5
    with pytest.raises(ValueError):
        perf.qualify_measurement(capture, parsed, case_ids=[1, 2])


def test_baseline_reads_terminal_facts_and_current_transaction_not_legacy_state():
    measurement = {**perf.qualify_measurement(*_measurement(), case_ids=[1, 2]),
                   "case_sha256": "matrix", "environment": {"endpoint": "a"}, "request_id": "first"}
    pending = {"continuation": {"operator": "Op", "business_summary": {
        "full_correctness_pass": True, "performance": measurement}}}
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute("CREATE TABLE control_outbox_v5 (topic, payload_json, created_at, outbox_id)")
    database = SimpleNamespace(connection=lambda: connection)
    args = dict(operator="Op", case_sha256="matrix", environment={"endpoint": "a"})
    assert perf.baseline_binding(database, **args)["phase"] == "baseline_bootstrap"
    assert perf.baseline_binding(database, **args, pending=pending)["baseline_result_id"] == "first"
    connection.execute("INSERT INTO control_outbox_v5 VALUES ('test.terminal', ?, '', '')", (json.dumps(pending),))
    slower = copy.deepcopy(pending)
    slower["continuation"]["business_summary"]["performance"].update(request_id="second", total_time_us=6)
    assert perf.baseline_binding(database, **args, pending=slower)["baseline_result_id"] == "first"
    assert perf.baseline_binding(database, **{**args, "case_sha256": "new"})["phase"] == "baseline_bootstrap"
    assert perf.baseline_binding(database, **{**args, "environment": {"endpoint": "b"}})["phase"] == "baseline_bootstrap"
    pending["continuation"]["business_summary"]["full_correctness_pass"] = False
    connection.execute("UPDATE control_outbox_v5 SET payload_json=?", (json.dumps(pending),))
    assert perf.baseline_binding(database, **args)["phase"] == "baseline_bootstrap"


def test_presentation_and_transport_updates_do_not_change_execution_comparability():
    release = {"engine_code_generation": "engine", "test_plan_generation": "plan", "daemon_generation": "old"}
    endpoint = {"endpoint_id": "device", "hardware": "910b", "config_sha256": "old"}
    expected = perf.execution_identity(endpoint, release)
    assert perf.execution_identity({**endpoint, "config_sha256": "new"}, {**release, "daemon_generation": "new"}) == expected


@pytest.mark.parametrize("field", ["measurement_contract_sha256", "measurement_pipeline_sha256", "runtime_environment"])
def test_fast_old_contract_never_becomes_current_anchor(field):
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute("CREATE TABLE control_outbox_v5 (topic, payload_json, created_at, outbox_id)")
    database = SimpleNamespace(connection=lambda: connection)
    measurement = {**perf.qualify_measurement(*_measurement(), case_ids=[1, 2]),
        "case_sha256": "matrix", "environment": {}, "runtime_environment": {},
        "request_id": "old", "observed_at": "2026-01-01", "total_time_us": .01}
    old = {"continuation": {"operator": "Synthetic", "business_summary": {
        "full_correctness_pass": True, "performance": measurement}}}
    connection.execute("INSERT INTO control_outbox_v5 VALUES ('test.terminal', ?, '', '')", (json.dumps(old),))
    newer = copy.deepcopy(old)
    newer["continuation"]["business_summary"]["performance"].update(
        request_id="new", observed_at="2026-01-02", total_time_us=10)
    newer["continuation"]["business_summary"]["performance"][field] = "changed"
    binding = perf.baseline_binding(database, operator="Synthetic", case_sha256="matrix", environment={}, pending=newer)
    assert binding["baseline_result_id"] == "new"
