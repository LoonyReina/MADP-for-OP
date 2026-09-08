from copy import deepcopy

import jsonschema
import pytest

from ascendop_protocol.actor import ActorContractError, validate_standalone_agent_action_context
from ascendop_protocol.schemas import load_schema
from test_standalone_agent_action_context import _context, STANDALONE_AGENT_ACTION_CONTEXT_V3_SCHEMA


def _value():
    value = _context("solver", STANDALONE_AGENT_ACTION_CONTEXT_V3_SCHEMA)
    value.update(schema="ascendop.standalone-agent-action-context.v5",
                 execution_phase="candidate-test", case_lifecycle=None, performance_baseline=None)
    value["test"]["operation_code"] = "test.correctness"
    value["reference_context"] = {
        "schema": "ascendop.solver-reference-context.v2",
        "applicability_policy": "read-only-evidence-revalidate-against-cann90-ascend910b",
        "official_task_root": ".ascendop-work/official/Demo",
        "optional": {name: {"path": None, "state": "unavailable", "reason": "not installed"}
                     for name in ("workspace_reference_index", "cann90_api", "op_knowledge")},
    }
    return value


@pytest.mark.parametrize("phase", ["candidate-test", "case-authoring"])
@pytest.mark.parametrize("available", [True, False])
def test_optional_references_match_python_and_json_contract(phase, available):
    value = _value()
    if available:
        for name, item in value["reference_context"]["optional"].items():
            item.update(path=f"reference/{name}", state="available", reason="")
    if phase == "case-authoring":
        value.update(execution_phase=phase, input_case_sha256=None, endpoint=None, release=None, test=None,
                     case_lifecycle={"trigger": "initial_case", "active_case_version": "v1", "active_case_sha256": None})
        value["request"]["logical_request_id"] = None
        value["output_contracts"] = [{"output_id": "case-bundle", "output_kind": "case-bundle",
                                     "artifact_ref": value["case_path"], "required": True}]
    assert validate_standalone_agent_action_context(value) == value
    jsonschema.Draft202012Validator(load_schema(value["schema"])).validate(value)


@pytest.mark.parametrize("bad", ["unexplained", "false-available", "path", "hash", "oversize"])
def test_optional_reference_contract_rejects_misleading_or_executable_authority(bad):
    value = deepcopy(_value())
    entry = value["reference_context"]["optional"]["op_knowledge"]
    if bad == "unexplained":
        entry["reason"] = "  "
    elif bad == "false-available":
        entry.update(state="available", reason="")
    elif bad == "path":
        entry["path"] = "../outside"
    elif bad == "hash":
        entry["sha256"] = "a" * 64
    elif bad == "oversize":
        entry["reason"] = "x" * 301
    with pytest.raises(ActorContractError):
        validate_standalone_agent_action_context(value)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.Draft202012Validator(load_schema(value["schema"])).validate(value)


def test_performance_can_use_optional_reference_evidence():
    value = _value()
    value["test"].update(mode="both", operation_code="test.performance", perf_case_range="1..9")
    assert validate_standalone_agent_action_context(value) == value
    jsonschema.Draft202012Validator(load_schema(value["schema"])).validate(value)
