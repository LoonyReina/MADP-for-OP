"""Shared data-only revision and protected-regression policy. No oracle or materializer."""
from __future__ import annotations
from dataclasses import dataclass
import json
import math
from typing import Any, Callable, Mapping
MAX_CASE_COUNT = 128
MAX_FILE_BYTES = 1024 * 1024

CHANGE_SCHEMA = "ascendop.standalone-case-data-change.v1"
MAX_ACTIVE_CASES = 30


@dataclass(frozen=True)
class LegacyCaseInputs:
    # An explicitly reviewed historical execution layout, used only by the
    # original accepted-request importer. Never imported/executed as Python.
    files: Mapping[str, bytes]
    data_path: str
    decode: Callable[[bytes], list[dict[str, Any]]]


@dataclass(frozen=True)
class CaseDataAdapter:
    operator_id: str
    adapter_id: str
    # Installed, reviewed execution files, not files taken from a case draft.
    files: Mapping[str, bytes]
    # Validate task fields and return EXACT execution parameters, without prose.
    validate_input: Callable[[dict[str, Any]], dict[str, Any]]
    legacy: LegacyCaseInputs | None = None
    # A reviewed, pure description of generated inputs AND execution checks.
    # Not a hash, Solver-supplied proof, or equality of expected outputs.
    execution_key: Callable[[dict[str, Any]], dict[str, Any]] | None = None
    # Explicit input-schema compatibility, not permission to reuse old PASS or
    # execution files. Used only to carry trusted accepted data into a new bundle.
    compatible_data_adapters: tuple[str, ...] = ()
    # Exact reviewed predecessor execution layouts. By default they permit only
    # a case upgrade with original accepted data facts, not qualification.
    reviewed_previous_executions: tuple[tuple[str, Mapping[str, bytes]], ...] = ()
    # Explicit semantic review: these predecessor IDs remain valid executions.
    # IDs alone grant nothing: complete reviewed bytes and original native data
    # provenance are still required at first terminal acceptance.
    qualifying_previous_executions: tuple[str, ...] = ()


def _json_bytes(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False) + "\n").encode("utf-8")


def _finite_json(value):
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float) and math.isfinite(value):
        return
    if isinstance(value, list):
        for item in value:
            _finite_json(item)
        return
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        for item in value.values():
            _finite_json(item)
        return
    raise ValueError("case data must contain finite JSON values, not executable objects")


def _inputs(cases, adapter, *, regression=False):
    minimum, maximum = (0, MAX_CASE_COUNT) if regression else (1, MAX_ACTIVE_CASES)
    if not isinstance(cases, list) or not minimum <= len(cases) <= maximum:
        raise ValueError("case data requires 1..30 active cases and bounded historical regression inputs")
    result = {}
    for case in cases:
        if not isinstance(case, dict):
            raise ValueError("each case must be a JSON object")
        _finite_json(case)
        identity = case.get("case_id")
        if type(identity) is not int or identity < 1 or identity in result:
            raise ValueError("case data requires unique positive integer case IDs")
        # A validator must reject unknown fields, not silently ignore e.g. a
        # requested tolerance override. The adapter owns task-specific legality.
        normalized = adapter.validate_input(json.loads(_json_bytes(case)))
        _finite_json(normalized)
        if (not isinstance(normalized, dict) or type(normalized.get("case_id")) is not int
                or normalized["case_id"] != identity):
            raise ValueError("trusted adapter changed the original case identity")
        result[identity] = normalized
    return result


def normalize_case_partitions(cases, regression_cases, adapter):
    active = _inputs(cases, adapter)
    regression = _inputs(regression_cases, adapter, regression=True)
    if active.keys() & regression.keys():
        raise ValueError("active and historical regression case IDs must be disjoint")
    if len(active) + len(regression) > MAX_CASE_COUNT:
        raise ValueError(f"active plus historical regression inputs exceed the existing {MAX_CASE_COUNT}-case execution limit")
    return active, regression


def retained_regressions(change, previous_cases, previous_regression_cases, protected_case_ids):
    """Derive retained historical inputs from original facts, never new data.

    Used at new intake and to read the original accepted proposal on inheritance.
    A regression only leaves this partition by returning to active execution or
    by an explicitly accepted equivalent replacement, not by omission in a draft.
    """
    selected = change.get("retain_as_regressions", [])
    if (not isinstance(selected, list) or len(selected) > MAX_CASE_COUNT
            or any(type(identity) is not int or identity not in protected_case_ids for identity in selected)
            or len(set(selected)) != len(selected)):
        raise ValueError("retain_as_regressions must select unique original protected failure IDs")
    old = {case["case_id"]: case for case in [*previous_cases, *previous_regression_cases]}
    active = {case["case_id"] for case in change["cases"]}
    replacements = replacement_targets(change, protected_case_ids)
    if set(selected) & (active | replacements.keys()):
        raise ValueError("a case cannot be both explicitly retained as regression and active/replaced")
    ids = list(dict.fromkeys([*[case["case_id"] for case in previous_regression_cases], *selected]))
    if any(identity not in protected_case_ids or identity not in old for identity in ids):
        raise ValueError("historical regression input lacks original failure protection")
    return [old[identity] for identity in ids if identity not in active and identity not in replacements]


def validate_case_data_change(change, *, adapter: CaseDataAdapter,
    previous_version: str | None, previous_cases: list[dict], protected_case_ids=(), previous_regression_cases=()):
    if (not isinstance(change, dict)
            or set(change) - {"schema", "base_case_version", "reason", "cases", "equivalent_replacements", "retain_as_regressions"}
            or not {"schema", "base_case_version", "reason", "cases"} <= change.keys()
            or change["schema"] != CHANGE_SCHEMA):
        raise ValueError("unsupported data-only case change contract")
    if change["base_case_version"] != previous_version:
        raise ValueError("case change does not reference the accepted base version")
    reason = change["reason"]
    if not isinstance(reason, str) or not reason.strip() or len(reason) > 2000:
        raise ValueError("case change needs a short reason of 1..2000 characters")
    if previous_version is None and (previous_cases or previous_regression_cases) or previous_version is not None and not previous_cases:
        raise ValueError("accepted case base identity and data are inconsistent")
    before_active, before_regression = normalize_case_partitions(previous_cases, list(previous_regression_cases), adapter) if previous_cases else ({}, {})
    before = {**before_active, **before_regression}
    after = _inputs(change["cases"], adapter)
    protected = set(protected_case_ids)
    if any(type(identity) is not int or identity not in before for identity in protected):
        raise ValueError("protected failure cases must come from the accepted base")
    replacements = replacement_targets(change, protected)
    regressions = retained_regressions(change, list(before_active.values()), list(before_regression.values()), protected)
    after, regression = normalize_case_partitions(list(after.values()), regressions, adapter)
    executed = {**after, **regression}
    for identity in protected:
        successor = replacements.get(identity, identity)
        if successor not in executed:
            raise ValueError(f"case change removes or alters protected failure case {identity}")
        if executed[successor] == before[identity]:
            continue
        if identity not in replacements or adapter.execution_key is None:
            raise ValueError(f"case change removes or alters protected failure case {identity}")
        original_key = adapter.execution_key(json.loads(_json_bytes(before[identity])))
        successor_key = adapter.execution_key(json.loads(_json_bytes(executed[successor])))
        _finite_json(original_key)
        _finite_json(successor_key)
        if not isinstance(original_key, dict) or not original_key or original_key != successor_key:
            raise ValueError(f"replacement does not preserve execution of protected failure case {identity}")
    diff = {
        "added": sorted(after.keys() - before_active.keys()),
        "removed": sorted(before_active.keys() - after.keys()),
        "changed": sorted(identity for identity in before_active.keys() & after.keys() if before[identity] != after[identity]),
    }
    cases = list(after.values())  # Preserve the declared execution order.
    if len(_json_bytes(list(executed.values()))) > MAX_FILE_BYTES:
        raise ValueError("case input data exceeds the existing transport file limit")
    return {"cases": cases, "case_ids": list(executed), "diff": diff, "reason": reason,
        "regression_cases": list(regression.values()), "active_case_ids": list(after),
        "regression_case_ids": list(regression),
        "regression_diff": {"added": sorted(regression.keys() - before_regression.keys()),
            "removed": sorted(before_regression.keys() - regression.keys())},
        "protected_case_ids": sorted({replacements.get(identity, identity) for identity in protected}),
        "equivalent_replacements": change.get("equivalent_replacements", [])}


def replacement_targets(change, protected_case_ids):
    """Read the explicit mapping, also from an already accepted native fact.

    This checks mapping shape, not equivalence. New intake must use
    validate_case_data_change; historical inheritance uses its accepted proposal
    without requalifying the old decision against a changed presentation/release.
    """
    items = change.get("equivalent_replacements", [])
    if not isinstance(items, list) or len(items) > MAX_CASE_COUNT:
        raise ValueError("equivalent_replacements must be a bounded list")
    result = {}
    for item in items:
        if not isinstance(item, dict) or set(item) != {"protected_case_id", "replacement_case_id"}:
            raise ValueError("replacement requires original protected and replacement case IDs")
        original, successor = item["protected_case_id"], item["replacement_case_id"]
        if (type(original) is not int or original not in protected_case_ids or original in result
                or type(successor) is not int or successor < 1):
            raise ValueError("replacement must name one original protected case and a positive target ID")
        result[original] = successor
    return result
