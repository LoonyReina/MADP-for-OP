from __future__ import annotations

import re
from typing import Any, Mapping

from .catalog import (
    FLOW_V5_BLOCKER_KINDS,
    FLOW_V5_EVIDENCE_OPERATIONS,
    FLOW_V5_EXECUTION_STATUSES,
    FLOW_V5_FAILURE_CLASSES,
    FLOW_V5_NATIVE_TERMINAL_STATUSES,
    FLOW_V5_OUTCOMES,
    FLOW_V5_OUTPUT_KINDS,
    FLOW_V5_ROLES,
    action_definition,
    evidence_operation_definition,
    flow_v5_catalog,
    flow_v5_catalog_digest,
)


ACTOR_ACTION_ENVELOPE_SCHEMA = "ascendop.actor-action-envelope.v1"
ACTOR_ACTION_RECEIPT_SCHEMA = "ascendop.actor-action-receipt.v1"
ROLE_BINDING_SCHEMA = "ascendop.role-binding.v1"
NATIVE_TURN_OUTCOME_SCHEMA = "ascendop.native-turn-outcome.v1"
AGENT_ACTION_OUTCOME_SCHEMA = "ascendop.agent-action-outcome.v1"
AGENT_ACTION_CONTEXT_V2_SCHEMA = "ascendop.agent-action-context.v2"
AGENT_ACTION_CONTEXT_V3_SCHEMA = "ascendop.agent-action-context.v3"
STANDALONE_AGENT_ACTION_CONTEXT_V1_SCHEMA = (
    "ascendop.standalone-agent-action-context.v1"
)
STANDALONE_AGENT_ACTION_CONTEXT_V2_SCHEMA = (
    "ascendop.standalone-agent-action-context.v2"
)
STANDALONE_AGENT_ACTION_CONTEXT_V3_SCHEMA = (
    "ascendop.standalone-agent-action-context.v3"
)
STANDALONE_AGENT_ACTION_CONTEXT_V4_SCHEMA = (
    "ascendop.standalone-agent-action-context.v4"
)
STANDALONE_AGENT_ACTION_CONTEXT_V5_SCHEMA = (
    "ascendop.standalone-agent-action-context.v5"
)
STANDALONE_AGENT_ACTION_CONTEXT_SCHEMA = STANDALONE_AGENT_ACTION_CONTEXT_V4_SCHEMA
SOLVER_REFERENCE_CONTEXT_V2_SCHEMA = "ascendop.solver-reference-context.v2"


class ActorContractError(ValueError):
    pass


def validate_role_binding(raw: Mapping[str, Any]) -> dict[str, Any]:
    _schema(raw, ROLE_BINDING_SCHEMA)
    for field in (
        "role_binding_id",
        "principal_id",
        "agent_registration_id",
        "native_session_id",
        "role",
        "generation",
        "state",
        "valid_from",
    ):
        _text(raw.get(field), field)
    if raw["role"] not in FLOW_V5_ROLES:
        raise ActorContractError(f"unsupported Flow V5 role: {raw['role']}")
    if raw["state"] not in {"active", "revoked", "expired"}:
        raise ActorContractError(f"unsupported role binding state: {raw['state']}")
    _token(raw["role_binding_id"], "role_binding_id")
    _token(raw["generation"], "generation")
    _validate_scope(_object(raw.get("scope"), "scope"), "scope")
    valid_until = raw.get("valid_until")
    if valid_until is not None:
        _text(valid_until, "valid_until")
    return dict(raw)


def validate_actor_action_envelope(raw: Mapping[str, Any]) -> dict[str, Any]:
    _schema(raw, ACTOR_ACTION_ENVELOPE_SCHEMA)
    for field in (
        "action_id",
        "idempotency_key",
        "action_kind",
        "effective_role",
        "principal_id",
        "role_binding_id",
        "producer_generation",
        "created_at",
    ):
        _text(raw.get(field), field)
    _token(raw["action_id"], "action_id")
    _token(raw["role_binding_id"], "role_binding_id")
    action = str(raw["action_kind"])
    try:
        definition = action_definition(action)
    except KeyError as exc:
        raise ActorContractError(f"unsupported Flow V5 action kind: {action}") from exc
    role = str(raw["effective_role"])
    if role != definition["role"]:
        raise ActorContractError(
            f"action {action} requires role {definition['role']}, not {role}"
        )
    scope = _object(raw.get("scope"), "scope")
    _validate_scope(scope, "scope")
    if definition["required_capability"] not in scope["capabilities"]:
        raise ActorContractError(
            f"action {action} requires capability "
            f"{definition['required_capability']}"
        )
    lease = _object(raw.get("lease"), "lease")
    for field in ("lease_id", "generation", "expires_at"):
        _text(lease.get(field), f"lease.{field}")
    _token(lease["lease_id"], "lease.lease_id")
    _token(lease["generation"], "lease.generation")
    causation = _object(raw.get("causation"), "causation")
    for field in ("trace_id", "correlation_id"):
        _text(causation.get(field), f"causation.{field}")
    for field in (
        "parent_action_id",
        "candidate_id",
        "promotion_receipt_id",
        "request_id",
        "attempt_id",
    ):
        value = causation.get(field)
        if value not in {None, ""}:
            _text(value, f"causation.{field}")
    payload = _object(raw.get("payload"), "payload")
    _validate_action_payload(action, payload)
    _validate_payload_scope(action, payload, scope)
    return dict(raw)


def validate_actor_action_receipt(raw: Mapping[str, Any]) -> dict[str, Any]:
    _schema(raw, ACTOR_ACTION_RECEIPT_SCHEMA)
    for field in (
        "action_id",
        "action_kind",
        "effective_role",
        "role_binding_id",
        "lease_id",
        "status",
        "completed_at",
    ):
        _text(raw.get(field), field)
    _token(raw["action_id"], "action_id")
    _token(raw["role_binding_id"], "role_binding_id")
    _token(raw["lease_id"], "lease_id")
    try:
        definition = action_definition(str(raw["action_kind"]))
    except KeyError as exc:
        raise ActorContractError(
            f"unsupported Flow V5 action kind: {raw['action_kind']}"
        ) from exc
    if raw["effective_role"] != definition["role"]:
        raise ActorContractError("actor receipt role does not match action kind")
    if raw["status"] not in {
        "completed",
        "failed",
        "rejected",
        "cancelled",
        "uncertain",
    }:
        raise ActorContractError(f"unsupported actor receipt status: {raw['status']}")
    _object(raw.get("result"), "result")
    failure_class = raw.get("failure_class")
    if raw["status"] == "completed":
        if failure_class is not None:
            raise ActorContractError("completed actor receipt must not have failure_class")
    else:
        _text(failure_class, "failure_class")
    return dict(raw)


def _validate_action_payload(action: str, payload: Mapping[str, Any]) -> None:
    if action in {"solver.iterate", "solver.repair"}:
        _required_text(payload, "operator_id", "candidate_id", "context_id", "workspace")
        return
    if action in {"tester.casegen", "tester.case-repair"}:
        _required_text(payload, "operator_id", "case_id", "context_id", "workspace")
        return
    if action in {
        "manager.flow-start",
        "manager.flow-pause",
        "manager.flow-resume",
        "manager.flow-stop",
    }:
        _required_text(payload, "flow_id", "reason")
        return
    if action == "manager.request-user-decision":
        _required_text(payload, "flow_id", "decision_id", "prompt")
        options = _string_list(payload.get("options"), "payload.options")
        if len(options) < 2:
            raise ActorContractError("payload.options must contain at least two choices")
        return
    if action == "manager.review-notification":
        _required_text(
            payload,
            "notification_id",
            "notification_kind",
            "flow_id",
            "operator_id",
            "summary",
        )
        if payload["notification_kind"] not in {
            "user_decision",
            "external_hold",
            "capability_gap",
        }:
            raise ActorContractError("unsupported Manager notification kind")
        if not isinstance(payload.get("requires_response"), bool):
            raise ActorContractError("requires_response must be boolean")
        _relative_path_list(
            payload.get("evidence_refs"), "payload.evidence_refs"
        )
        _string_list(payload.get("allowed_commands"), "payload.allowed_commands")
        return
    if action.startswith("assistant.official-"):
        _required_text(
            payload,
            "campaign_id",
            "operator_id",
            "candidate_id",
            "candidate_digest",
            "official_attempt_id",
            "authorization_id",
        )
        _sha256(payload["candidate_digest"], "payload.candidate_digest")
        if action in {"assistant.official-prepare", "assistant.official-submit"}:
            _required_text(
                payload,
                "workspace",
                "runbook_path",
                "project_digest",
                "submit_url",
                "rules_generation",
            )
            _relative_path(payload["workspace"], "payload.workspace")
            _relative_path(payload["runbook_path"], "payload.runbook_path")
            _sha256(payload["project_digest"], "payload.project_digest")
            _sha256_mapping(
                payload.get("source_file_digests"),
                "payload.source_file_digests",
            )
        if action == "assistant.official-submit":
            _validate_submission_adapter(
                payload.get("submission_adapter"),
                payload.get("source_file_digests"),
            )
        if action in {
            "assistant.official-poll",
            "assistant.official-import-result",
        }:
            _required_text(payload, "submission_id", "official_receipt_id")
        if action == "assistant.official-poll":
            _required_text(payload, "submission_url", "project_digest")
            _sha256(payload["project_digest"], "payload.project_digest")
            _sha256_mapping(
                payload.get("source_file_digests"),
                "payload.source_file_digests",
            )
        if action == "assistant.official-import-result":
            _required_text(payload, "result_ref", "result_digest")
            _relative_path(payload["result_ref"], "payload.result_ref")
            _sha256(payload["result_digest"], "payload.result_digest")
        return
    if action == "developer.repair-capability":
        _required_text(
            payload,
            "capability_gap_id",
            "capability_code",
            "runbook_path",
            "resume_condition",
        )
        _relative_path(payload["runbook_path"], "payload.runbook_path")
        context = _object(payload.get("gap_context"), "payload.gap_context")
        _required_text(context, "source_kind", "operator_id")
        identity = _object(
            context.get("identity"),
            "payload.gap_context.identity",
        )
        if not identity:
            raise ActorContractError(
                "payload.gap_context.identity must not be empty"
            )
        for key, value in identity.items():
            _text(key, "payload.gap_context.identity key")
            _text(value, f"payload.gap_context.identity.{key}")
        evidence_refs = _relative_path_list(
            context.get("evidence_refs"),
            "payload.gap_context.evidence_refs",
        )
        if not evidence_refs:
            raise ActorContractError(
                "payload.gap_context.evidence_refs must not be empty"
            )
        for field in ("gate_stage", "next_command"):
            value = context.get(field)
            if value not in {None, ""}:
                _text(value, f"payload.gap_context.{field}")
        return
    if action == "developer.publish-protocol":
        _required_text(payload, "protocol_generation", "catalog_digest")
        _sha256(payload["catalog_digest"], "payload.catalog_digest")
        _relative_path_list(payload.get("artifact_refs"), "payload.artifact_refs")
        return
    raise ActorContractError(f"action payload validator is missing: {action}")


def _validate_submission_adapter(raw: Any, source_digests: Any) -> None:
    adapter = _object(raw, "payload.submission_adapter")
    if adapter.get("schema") != "ascendop.operator-local-submission-adapter.v1":
        raise ActorContractError("unsupported payload.submission_adapter schema")
    _required_text(adapter, "adapter_id", "normalization", "hash_algorithm")
    if adapter["normalization"] != "lf" or adapter["hash_algorithm"] != "sha256":
        raise ActorContractError("submission adapter readback must use LF and SHA-256")
    if adapter.get("single_click") is not True:
        raise ActorContractError("submission adapter must require single click")
    slots = adapter.get("editable_slots")
    if not isinstance(slots, list) or not slots:
        raise ActorContractError("submission adapter editable slots must not be empty")
    digests = _object(source_digests, "payload.source_file_digests")
    slot_ids: set[str] = set()
    paths: set[str] = set()
    for index, raw_slot in enumerate(slots):
        slot = _object(raw_slot, f"payload.submission_adapter.editable_slots[{index}]")
        slot_id = _text(slot.get("slot_id"), f"editable_slots[{index}].slot_id")
        path = _text(slot.get("project_path"), f"editable_slots[{index}].project_path")
        source_digest = _text(
            slot.get("source_sha256"),
            f"editable_slots[{index}].source_sha256",
        )
        dom_digest = _text(
            slot.get("dom_lf_sha256"),
            f"editable_slots[{index}].dom_lf_sha256",
        )
        _relative_path(path, f"editable_slots[{index}].project_path")
        _sha256(source_digest, f"editable_slots[{index}].source_sha256")
        _sha256(dom_digest, f"editable_slots[{index}].dom_lf_sha256")
        if slot_id in slot_ids or path in paths:
            raise ActorContractError("submission adapter slot identities must be unique")
        if digests.get(path) != source_digest:
            raise ActorContractError("submission adapter slot digest is not source-bound")
        slot_ids.add(slot_id)
        paths.add(path)


def _required_text(value: Mapping[str, Any], *fields: str) -> None:
    for field in fields:
        _text(value.get(field), f"payload.{field}")


def _validate_payload_scope(
    action: str,
    payload: Mapping[str, Any],
    scope: Mapping[str, Any],
) -> None:
    if action.startswith(("solver.", "tester.", "assistant.official-")):
        operator_id = str(payload["operator_id"])
        if operator_id not in scope["operator_ids"]:
            raise ActorContractError(
                f"payload.operator_id is outside the action scope: {operator_id}"
            )
    workspace = payload.get("workspace")
    if workspace is not None and str(workspace) not in scope["workspace_roots"]:
        raise ActorContractError(
            f"payload.workspace is outside the action scope: {workspace}"
        )


def validate_native_turn_outcome(raw: Mapping[str, Any]) -> dict[str, Any]:
    _schema(raw, NATIVE_TURN_OUTCOME_SCHEMA)
    for field in (
        "action_id",
        "native_session_id",
        "native_turn_id",
        "terminal_status",
        "observed_at",
    ):
        _text(raw.get(field), field)
    if raw["terminal_status"] not in FLOW_V5_NATIVE_TERMINAL_STATUSES:
        raise ActorContractError(
            f"unsupported native terminal status: {raw['terminal_status']}"
        )
    _object(raw.get("structured_result"), "structured_result")
    _relative_path_list(raw.get("artifact_refs"), "artifact_refs")
    telemetry = _object(raw.get("telemetry"), "telemetry")
    for field in ("usage", "skills"):
        _object(telemetry.get(field), f"telemetry.{field}")
    return dict(raw)


def validate_agent_action_outcome(raw: Mapping[str, Any]) -> dict[str, Any]:
    _schema(raw, AGENT_ACTION_OUTCOME_SCHEMA)
    for field in ("action_id", "execution_status", "summary", "completed_at"):
        _text(raw.get(field), field)
    execution_status = str(raw["execution_status"])
    if execution_status not in FLOW_V5_EXECUTION_STATUSES:
        raise ActorContractError(
            f"unsupported Agent execution status: {execution_status}"
        )
    disposition = raw.get("disposition")
    failure_class = raw.get("failure_class")
    if execution_status == "completed":
        if disposition not in FLOW_V5_OUTCOMES:
            raise ActorContractError("completed outcome requires a registered disposition")
        if failure_class is not None:
            raise ActorContractError("completed outcome must not have failure_class")
    else:
        if disposition is not None:
            raise ActorContractError("non-completed outcome must not have disposition")
        if failure_class not in FLOW_V5_FAILURE_CLASSES:
            raise ActorContractError(
                "non-completed outcome requires a registered failure_class"
            )
    outputs = raw.get("outputs")
    if not isinstance(outputs, list):
        raise ActorContractError("outputs must be a list")
    for index, output in enumerate(outputs):
        item = _object(output, f"outputs[{index}]")
        for field in ("output_id", "output_kind", "artifact_ref", "sha256"):
            _text(item.get(field), f"outputs[{index}].{field}")
        if item["output_kind"] not in FLOW_V5_OUTPUT_KINDS:
            raise ActorContractError(
                f"unsupported Flow V5 output kind: {item['output_kind']}"
            )
        _relative_path(item["artifact_ref"], f"outputs[{index}].artifact_ref")
        _sha256(item["sha256"], f"outputs[{index}].sha256")
    evidence_refs = _relative_path_list(raw.get("evidence_refs"), "evidence_refs")
    requested = raw.get("requested_operation")
    blocker = raw.get("blocker")
    if disposition == "proposed_change" and not outputs:
        raise ActorContractError("proposed_change requires at least one output")
    if disposition == "no_change_with_evidence" and not evidence_refs:
        raise ActorContractError("no_change_with_evidence requires evidence_refs")
    if disposition == "request_evidence":
        operation = _object(requested, "requested_operation")
        code = _text(operation.get("operation_code"), "operation_code")
        if code not in FLOW_V5_EVIDENCE_OPERATIONS:
            raise ActorContractError(f"unsupported evidence operation: {code}")
        _object(operation.get("parameters"), "requested_operation.parameters")
        _text(
            operation.get("expected_consumer"),
            "requested_operation.expected_consumer",
        )
        if operation["expected_consumer"] not in FLOW_V5_ROLES:
            raise ActorContractError("requested operation consumer is not a V5 role")
        definition = evidence_operation_definition(code)
        if operation["expected_consumer"] not in definition["expected_consumers"]:
            raise ActorContractError(
                f"operation {code} does not produce evidence for "
                f"{operation['expected_consumer']}"
            )
        _text(
            operation.get("resume_condition"),
            "requested_operation.resume_condition",
        )
        if blocker is not None:
            raise ActorContractError("request_evidence must not contain a blocker")
    elif requested is not None:
        raise ActorContractError(
            "requested_operation is allowed only for request_evidence"
        )
    if disposition in {"blocked_external", "protocol_gap"}:
        block = _object(blocker, "blocker")
        for field in ("kind", "code", "details", "resume_condition"):
            _text(block.get(field), f"blocker.{field}")
        if block["kind"] not in FLOW_V5_BLOCKER_KINDS:
            raise ActorContractError(f"unsupported blocker kind: {block['kind']}")
        if disposition == "protocol_gap" and block["kind"] != "capability_gap":
            raise ActorContractError("protocol_gap requires capability_gap blocker")
        if disposition == "blocked_external" and block["kind"] not in {
            "external_dependency",
            "user_policy_decision",
        }:
            raise ActorContractError(
                "blocked_external requires external_dependency or user_policy_decision"
            )
    elif blocker is not None:
        raise ActorContractError(
            "blocker is allowed only for blocked_external or protocol_gap"
        )
    return dict(raw)


def validate_agent_action_context_v2(
    raw: Mapping[str, Any],
    *,
    require_current_catalog: bool = True,
) -> dict[str, Any]:
    _schema(raw, AGENT_ACTION_CONTEXT_V2_SCHEMA)
    return _validate_agent_action_context_common(
        raw,
        require_current_catalog=require_current_catalog,
    )


def validate_agent_action_context_v3(
    raw: Mapping[str, Any],
    *,
    require_current_catalog: bool = True,
) -> dict[str, Any]:
    _schema(raw, AGENT_ACTION_CONTEXT_V3_SCHEMA)
    value = _validate_agent_action_context_common(
        raw,
        require_current_catalog=require_current_catalog,
    )
    attempt = _object(raw.get("attempt"), "attempt")
    attempt_id = _text(attempt.get("attempt_id"), "attempt.attempt_id")
    ordinal = attempt.get("ordinal")
    if not isinstance(ordinal, int) or isinstance(ordinal, bool) or ordinal < 1:
        raise ActorContractError("attempt.ordinal must be a positive integer")
    mode = _text(attempt.get("mode"), "attempt.mode")
    if mode not in {"initial", "execution_retry", "output_repair"}:
        raise ActorContractError(f"unsupported attempt mode: {mode}")
    history = attempt.get("history")
    if not isinstance(history, list):
        raise ActorContractError("attempt.history must be a list")
    prior_ids: set[str] = set()
    prior_ordinals: set[int] = set()
    validated_history: list[Mapping[str, Any]] = []
    for index, item in enumerate(history):
        row = _object(item, f"attempt.history[{index}]")
        prior_id = _text(
            row.get("attempt_id"), f"attempt.history[{index}].attempt_id"
        )
        prior_ordinal = row.get("ordinal")
        if (
            not isinstance(prior_ordinal, int)
            or isinstance(prior_ordinal, bool)
            or prior_ordinal < 1
            or prior_ordinal >= ordinal
        ):
            raise ActorContractError(
                "attempt history ordinal must precede the current attempt"
            )
        state = _text(row.get("state"), f"attempt.history[{index}].state")
        if state not in {
            "claimed",
            "running",
            "uncertain",
            "retry-pending",
            "completed",
            "failed",
            "cancelled",
        }:
            raise ActorContractError(f"unsupported attempt history state: {state}")
        for field in ("failure_class", "validation_error"):
            field_value = row.get(field)
            if field_value is not None:
                _text(field_value, f"attempt.history[{index}].{field}")
        if prior_id in prior_ids or prior_ordinal in prior_ordinals:
            raise ActorContractError("attempt history identities must be unique")
        prior_ids.add(prior_id)
        prior_ordinals.add(prior_ordinal)
        validated_history.append(row)
    if [int(item["ordinal"]) for item in validated_history] != sorted(
        int(item["ordinal"]) for item in validated_history
    ):
        raise ActorContractError("attempt history must be ordinal ordered")
    repair = attempt.get("output_repair")
    if mode == "initial":
        if history or repair is not None or ordinal != 1:
            raise ActorContractError(
                "initial attempt must be ordinal one without prior history or repair"
            )
    elif mode == "execution_retry":
        if not history or repair is not None:
            raise ActorContractError(
                "execution_retry requires history and forbids output repair"
            )
    else:
        repair_value = _object(repair, "attempt.output_repair")
        for field in ("prior_attempt_id", "failure_class", "validation_error"):
            _text(repair_value.get(field), f"attempt.output_repair.{field}")
        if repair_value["failure_class"] != "agent-output-validation":
            raise ActorContractError(
                "output repair requires agent-output-validation failure"
            )
        prior_ordinal = repair_value.get("prior_attempt_ordinal")
        if not isinstance(prior_ordinal, int) or isinstance(prior_ordinal, bool):
            raise ActorContractError(
                "attempt.output_repair.prior_attempt_ordinal must be an integer"
            )
        if repair_value.get("remaining_correction_turns") != 1:
            raise ActorContractError(
                "output repair must authorize exactly one correction turn"
            )
        if not validated_history:
            raise ActorContractError("output repair requires prior attempt history")
        latest = validated_history[-1]
        if (
            repair_value["prior_attempt_id"] != latest["attempt_id"]
            or prior_ordinal != latest["ordinal"]
            or repair_value["failure_class"] != latest["failure_class"]
            or repair_value["validation_error"] != latest["validation_error"]
        ):
            raise ActorContractError(
                "output repair must match the latest prior attempt failure"
            )
    if attempt_id in prior_ids:
        raise ActorContractError("current attempt identity appears in prior history")
    return value


def validate_standalone_agent_action_context(
    raw: Mapping[str, Any],
) -> dict[str, Any]:
    schema = raw.get("schema") if isinstance(raw, Mapping) else None
    if schema not in {
        STANDALONE_AGENT_ACTION_CONTEXT_V1_SCHEMA,
        STANDALONE_AGENT_ACTION_CONTEXT_V2_SCHEMA,
        STANDALONE_AGENT_ACTION_CONTEXT_V3_SCHEMA,
        STANDALONE_AGENT_ACTION_CONTEXT_V4_SCHEMA,
        STANDALONE_AGENT_ACTION_CONTEXT_V5_SCHEMA,
    }:
        raise ActorContractError(
            "unsupported standalone Agent action context schema"
        )
    _schema(raw, str(schema))
    for field in (
        "context_id",
        "action_id",
        "action_kind",
        "effective_role",
        "campaign_id",
        "operator_id",
        "native_session_id",
        "contract_revision",
        "workspace",
        "case_path",
        "run_root",
        "outcome_path",
        "task_binding_sha256",
        "created_at",
    ):
        _text(raw.get(field), field)
    _token(raw["context_id"], "context_id")
    _token(raw["action_id"], "action_id")
    _sha256(raw["contract_revision"], "contract_revision")
    _sha256(raw["task_binding_sha256"], "task_binding_sha256")
    for field in ("workspace", "case_path", "run_root", "outcome_path"):
        _relative_path(raw[field], field)

    action = str(raw["action_kind"])
    if action not in {
        "solver.iterate",
        "solver.repair",
        "tester.casegen",
        "tester.case-repair",
    }:
        raise ActorContractError(
            f"unsupported standalone Agent action kind: {action}"
        )
    role = str(raw["effective_role"])
    if role != action_definition(action)["role"]:
        raise ActorContractError(
            f"standalone action {action} does not belong to role {role}"
        )
    execution_phase = None
    if schema == STANDALONE_AGENT_ACTION_CONTEXT_V5_SCHEMA:
        execution_phase = _text(raw.get("execution_phase"), "execution_phase")
        if execution_phase not in {"case-authoring", "candidate-test"}:
            raise ActorContractError("unsupported standalone execution phase")
        if role == "tester" and execution_phase != "case-authoring":
            raise ActorContractError("standalone Tester actions require case-authoring")
    case_authoring = role == "tester" or execution_phase == "case-authoring"

    if schema in {
        STANDALONE_AGENT_ACTION_CONTEXT_V2_SCHEMA,
        STANDALONE_AGENT_ACTION_CONTEXT_V3_SCHEMA,
        STANDALONE_AGENT_ACTION_CONTEXT_V4_SCHEMA,
        STANDALONE_AGENT_ACTION_CONTEXT_V5_SCHEMA,
    }:
        _sha256(raw.get("input_source_sha256"), "input_source_sha256")
        predecessors = raw.get("predecessor_evidence")
        if not isinstance(predecessors, list) or len(predecessors) > 2:
            raise ActorContractError(
                "predecessor_evidence must contain at most two entries"
            )
        predecessor_ids: set[str] = set()
        for index, item in enumerate(predecessors):
            field = f"predecessor_evidence[{index}]"
            evidence = _object(item, field)
            predecessor_id = _token(
                evidence.get("action_id"), f"{field}.action_id"
            )
            if predecessor_id in predecessor_ids:
                raise ActorContractError(
                    "predecessor_evidence action ids must be unique"
                )
            predecessor_ids.add(predecessor_id)
            predecessor_action = _text(
                evidence.get("action_kind"), f"{field}.action_kind"
            )
            if predecessor_action not in {
                "solver.iterate",
                "solver.repair",
                "tester.casegen",
                "tester.case-repair",
            }:
                raise ActorContractError(
                    f"unsupported predecessor action kind: {predecessor_action}"
                )
            predecessor_role = _text(
                evidence.get("effective_role"), f"{field}.effective_role"
            )
            if predecessor_role != action_definition(predecessor_action)["role"]:
                raise ActorContractError(
                    "predecessor evidence role does not match action kind"
                )
            _relative_path(evidence.get("outcome_ref"), f"{field}.outcome_ref")
            _sha256(evidence.get("outcome_sha256"), f"{field}.outcome_sha256")
            _text(evidence.get("completed_at"), f"{field}.completed_at")
            disposition = evidence.get("disposition")
            if disposition is not None and disposition not in FLOW_V5_OUTCOMES:
                raise ActorContractError(
                    f"unsupported predecessor disposition: {disposition}"
                )
            _text(evidence.get("summary"), f"{field}.summary")
            _relative_path_list(
                evidence.get("evidence_refs"), f"{field}.evidence_refs"
            )

    if schema in {
        STANDALONE_AGENT_ACTION_CONTEXT_V3_SCHEMA,
        STANDALONE_AGENT_ACTION_CONTEXT_V4_SCHEMA,
        STANDALONE_AGENT_ACTION_CONTEXT_V5_SCHEMA,
    }:
        reference = raw.get("reference_context")
        if (role == "solver" and schema == STANDALONE_AGENT_ACTION_CONTEXT_V5_SCHEMA
                and isinstance(reference, Mapping) and reference.get("schema") == SOLVER_REFERENCE_CONTEXT_V2_SCHEMA):
            _validate_optional_solver_references(reference)
            if not case_authoring and _object(raw.get("test"), "test").get("mode") not in {"correctness", "both", "performance"}:
                raise ActorContractError("optional Solver references require a supported test mode")
        elif role == "tester":
            if reference is not None:
                raise ActorContractError(
                    "standalone Tester action cannot receive Solver reference context"
                )
        else:
            reference_value = _object(reference, "reference_context")
            if reference_value.get("schema") != "ascendop.solver-reference-context.v1":
                raise ActorContractError(
                    "unsupported standalone Solver reference context schema"
                )
            _sha256(reference_value.get("revision"), "reference_context.revision")
            expected_policy = (
                "read-only-evidence-revalidate-against-cann90-ascend910b"
                if schema == STANDALONE_AGENT_ACTION_CONTEXT_V5_SCHEMA
                else "read-only-evidence-revalidate-against-august-cann90-ascend910b"
            )
            if reference_value.get("applicability_policy") != expected_policy:
                raise ActorContractError(
                    "standalone Solver reference applicability policy drifted"
                )
            _relative_path(
                reference_value.get("official_task_root"),
                "reference_context.official_task_root",
            )
            if schema == STANDALONE_AGENT_ACTION_CONTEXT_V5_SCHEMA:
                reference_index = _object(
                    reference_value.get("workspace_reference_index"),
                    "reference_context.workspace_reference_index",
                )
                _relative_path(
                    reference_index.get("path"),
                    "reference_context.workspace_reference_index.path",
                )
                _sha256(
                    reference_index.get("sha256"),
                    "reference_context.workspace_reference_index.sha256",
                )
            else:
                official = _object(
                    reference_value.get("official_cann_references"),
                    "reference_context.official_cann_references",
                )
                _relative_path(
                    official.get("index_path"),
                    "reference_context.official_cann_references.index_path",
                )
                _sha256(
                    official.get("index_sha256"),
                    "reference_context.official_cann_references.index_sha256",
                )
                historical = _object(
                    reference_value.get("historical_reference"),
                    "reference_context.historical_reference",
                )
                _relative_path(
                    historical.get("manifest_path"),
                    "reference_context.historical_reference.manifest_path",
                )
                _sha256(
                    historical.get("manifest_sha256"),
                    "reference_context.historical_reference.manifest_sha256",
                )
            _relative_path(
                reference_value.get("cann90_api_root"),
                "reference_context.cann90_api_root",
            )
            knowledge = reference_value.get("op_knowledge")
            if knowledge is not None:
                knowledge_value = _object(
                    knowledge, "reference_context.op_knowledge"
                )
                _relative_path(
                    knowledge_value.get("root"),
                    "reference_context.op_knowledge.root",
                )
                _sha256(
                    knowledge_value.get("tree_sha256"),
                    "reference_context.op_knowledge.tree_sha256",
                )

    endpoint = raw.get("endpoint")
    release = raw.get("release")
    input_case_sha256 = raw.get("input_case_sha256")
    request = _object(raw.get("request"), "request")
    _text(request.get("idempotency_key"), "request.idempotency_key")
    logical_request_id = request.get("logical_request_id")
    if case_authoring:
        authority_label = (
            "standalone Tester casegen"
            if role == "tester"
            else "standalone case authoring"
        )
        if any(value is not None for value in (endpoint, release, input_case_sha256)):
            raise ActorContractError(
                f"{authority_label} cannot receive endpoint, release, or case seal"
            )
        if logical_request_id is not None:
            raise ActorContractError(
                f"{authority_label} cannot receive a test request id"
            )
    else:
        _sha256(input_case_sha256, "input_case_sha256")
        endpoint_value = _object(endpoint, "endpoint")
        _relative_path(endpoint_value.get("config_path"), "endpoint.config_path")
        _sha256(endpoint_value.get("config_sha256"), "endpoint.config_sha256")
        _text(endpoint_value.get("endpoint_id"), "endpoint.endpoint_id")
        release_value = _object(release, "release")
        _text(release_value.get("release_id"), "release.release_id")
        _sha256(
            release_value.get("release_generation"),
            "release.release_generation",
        )
        _token(logical_request_id, "request.logical_request_id")

    test = raw.get("test")
    if schema == STANDALONE_AGENT_ACTION_CONTEXT_V5_SCHEMA and case_authoring:
        if test is not None:
            raise ActorContractError("standalone case authoring cannot receive a test")
    else:
        test = _object(test, "test")
        for field in ("mode", "test_version", "case_version", "case_range"):
            _text(test.get(field), f"test.{field}")
        if schema in {
            STANDALONE_AGENT_ACTION_CONTEXT_V4_SCHEMA,
            STANDALONE_AGENT_ACTION_CONTEXT_V5_SCHEMA,
        }:
            if schema == STANDALONE_AGENT_ACTION_CONTEXT_V4_SCHEMA:
                operation_code = "test.performance"
            else:
                operation_code = str(test.get("operation_code") or "")
                if operation_code not in {"test.correctness", "test.performance"}:
                    raise ActorContractError(
                        "standalone v5 test operation must be correctness or performance"
                    )
            if operation_code == "test.performance":
                if test["mode"] != "both":
                    raise ActorContractError(
                        "standalone performance actions require mode=both"
                    )
                _text(test.get("perf_case_range"), "test.perf_case_range")
            elif test["mode"] != "correctness":
                raise ActorContractError(
                    "standalone correctness actions require mode=correctness"
                )
        elif test["mode"] != "correctness":
            raise ActorContractError(
                "legacy standalone P1 actions support correctness only"
            )
        timeout = test.get("timeout_seconds")
        if not isinstance(timeout, int) or isinstance(timeout, bool) or timeout <= 0:
            raise ActorContractError("test.timeout_seconds must be a positive integer")

    if schema in {
        STANDALONE_AGENT_ACTION_CONTEXT_V4_SCHEMA,
        STANDALONE_AGENT_ACTION_CONTEXT_V5_SCHEMA,
    }:
        lifecycle = raw.get("case_lifecycle")
        if case_authoring:
            lifecycle_value = _object(lifecycle, "case_lifecycle")
            allowed_triggers = {
                "initial_case",
                "usage_exhausted",
                "new_official_feedback",
                "wrapper_identity_mismatch",
            }
            if schema == STANDALONE_AGENT_ACTION_CONTEXT_V5_SCHEMA and raw.get("effective_role") == "solver":
                allowed_triggers.add("solver_requested")
            if lifecycle_value.get("trigger") not in allowed_triggers:
                raise ActorContractError("unsupported case lifecycle trigger")
            _text(
                lifecycle_value.get("active_case_version"),
                "case_lifecycle.active_case_version",
            )
            active_case_sha = lifecycle_value.get("active_case_sha256")
            if active_case_sha is not None:
                _sha256(active_case_sha, "case_lifecycle.active_case_sha256")
        elif lifecycle is not None:
            raise ActorContractError(
                "standalone Solver action cannot receive a case lifecycle trigger"
            )

    launcher = _object(raw.get("launcher"), "launcher")
    _text(launcher.get("python_executable"), "launcher.python_executable")
    if launcher.get("module") != "ascendop_test_gateway.cli":
        raise ActorContractError("standalone launcher module is not allowlisted")
    python_path = _string_list(launcher.get("python_path"), "launcher.python_path")
    if not python_path:
        raise ActorContractError("launcher.python_path must not be empty")
    harness = _object(raw.get("harness"), "harness")
    _text(harness.get("python_executable"), "harness.python_executable")
    _text(harness.get("daemon_entrypoint"), "harness.daemon_entrypoint")

    outputs = raw.get("output_contracts")
    if not isinstance(outputs, list) or not outputs:
        raise ActorContractError("output_contracts must not be empty")
    seen: set[str] = set()
    for index, item in enumerate(outputs):
        output = _object(item, f"output_contracts[{index}]")
        output_id = _text(output.get("output_id"), f"output_contracts[{index}].output_id")
        if output_id in seen:
            raise ActorContractError("output contract ids must be unique")
        seen.add(output_id)
        kind = _text(
            output.get("output_kind"),
            f"output_contracts[{index}].output_kind",
        )
        if kind not in {"case-bundle", "source-change", "standalone-test-result"}:
            raise ActorContractError(f"unsupported standalone output kind: {kind}")
        _relative_path(
            output.get("artifact_ref"),
            f"output_contracts[{index}].artifact_ref",
        )
        if not isinstance(output.get("required"), bool):
            raise ActorContractError("output contract required flag must be boolean")
    declared_kinds = {str(item["output_kind"]) for item in outputs}
    required_kinds = {
        str(item["output_kind"])
        for item in outputs
        if bool(item.get("required"))
    }
    expected = "case-bundle" if case_authoring else "standalone-test-result"
    available_kinds = required_kinds if case_authoring else declared_kinds
    if expected not in available_kinds:
        raise ActorContractError(
            f"standalone {role} action requires output kind {expected}"
        )
    return dict(raw)


def _validate_agent_action_context_common(
    raw: Mapping[str, Any],
    *,
    require_current_catalog: bool = True,
) -> dict[str, Any]:
    for field in (
        "context_id",
        "action_id",
        "catalog_generation",
        "catalog_digest",
        "role",
        "created_at",
    ):
        _text(raw.get(field), field)
    _sha256(raw["catalog_digest"], "catalog_digest")
    if require_current_catalog:
        catalog = flow_v5_catalog()
        if raw["catalog_generation"] != catalog["generation"]:
            raise ActorContractError("Agent context catalog generation is stale")
        if raw["catalog_digest"] != flow_v5_catalog_digest():
            raise ActorContractError("Agent context catalog digest is stale")
    if raw["role"] not in FLOW_V5_ROLES:
        raise ActorContractError(f"unsupported Agent context role: {raw['role']}")
    for field in (
        "candidate",
        "case",
        "comparable_result",
        "baseline",
        "evidence_index",
        "environment",
        "budget",
    ):
        _object(raw.get(field), field)
    _object(raw.get("lineage", {}), "lineage")
    hypotheses = raw.get("hypotheses")
    if not isinstance(hypotheses, list) or any(
        not isinstance(item, Mapping) for item in hypotheses
    ):
        raise ActorContractError("hypotheses must be a list of objects")
    _relative_path_list(raw.get("write_scope"), "write_scope")
    outcomes = _string_list(raw.get("allowed_outcomes"), "allowed_outcomes")
    if not outcomes or any(item not in FLOW_V5_OUTCOMES for item in outcomes):
        raise ActorContractError("allowed_outcomes contains an unregistered outcome")
    operations = _string_list(
        raw.get("available_evidence_operations"),
        "available_evidence_operations",
        allow_empty=True,
    )
    if any(item not in FLOW_V5_EVIDENCE_OPERATIONS for item in operations):
        raise ActorContractError(
            "available_evidence_operations contains an unregistered operation"
        )
    schemas = _string_list(raw.get("output_schemas"), "output_schemas")
    if not schemas:
        raise ActorContractError("output_schemas must not be empty")
    return dict(raw)


def _schema(raw: Mapping[str, Any], expected: str) -> None:
    if not isinstance(raw, Mapping) or raw.get("schema") != expected:
        raise ActorContractError(f"unsupported contract; expected {expected}")


def _validate_scope(scope: Mapping[str, Any], field: str) -> None:
    _relative_path_list(scope.get("workspace_roots"), f"{field}.workspace_roots")
    _string_list(scope.get("operator_ids"), f"{field}.operator_ids", allow_empty=True)
    capabilities = _string_list(scope.get("capabilities"), f"{field}.capabilities")
    if not capabilities:
        raise ActorContractError(f"{field}.capabilities must not be empty")


def _object(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ActorContractError(f"{field} must be an object")
    return value


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ActorContractError(f"{field} must be non-empty text")
    return value.strip()


def _token(value: Any, field: str) -> str:
    text = _text(value, field)
    if not re.fullmatch(r"[A-Za-z0-9._-]+", text):
        raise ActorContractError(f"{field} must be a safe token")
    return text


def _sha256(value: Any, field: str) -> str:
    text = _text(value, field).lower()
    if not re.fullmatch(r"[0-9a-f]{64}", text):
        raise ActorContractError(f"{field} must be SHA-256")
    return text


def _sha256_mapping(value: Any, field: str) -> dict[str, str]:
    mapping = _object(value, field)
    if not mapping:
        raise ActorContractError(f"{field} must not be empty")
    result: dict[str, str] = {}
    for key, digest in mapping.items():
        relative = _relative_path(key, field)
        result[relative] = _sha256(digest, f"{field}.{relative}")
    return result


def _validate_optional_solver_references(value: Mapping[str, Any]) -> None:
    if set(value) != {"schema", "applicability_policy", "official_task_root", "optional"}:
        raise ActorContractError("optional reference context fields differ from v2")
    if value["applicability_policy"] != "read-only-evidence-revalidate-against-cann90-ascend910b":
        raise ActorContractError("optional reference applicability policy drifted")
    _relative_path(value["official_task_root"], "reference_context.official_task_root")
    optional = _object(value["optional"], "reference_context.optional")
    if set(optional) != {"workspace_reference_index", "cann90_api", "op_knowledge"}:
        raise ActorContractError("optional reference entries differ from v2")
    for name, raw in optional.items():
        item = _object(raw, f"reference_context.optional.{name}")
        if set(item) != {"path", "state", "reason"} or item.get("state") not in {"available", "unavailable"}:
            raise ActorContractError("invalid optional reference availability")
        if item["path"] is not None:
            _relative_path(item["path"], f"reference_context.optional.{name}.path")
        if not isinstance(item["reason"], str) or len(item["reason"]) > 300:
            raise ActorContractError("optional reference reason must be bounded text")
        if item["state"] == "available" and (item["path"] is None or item["reason"]):
            raise ActorContractError("available reference requires a path and no failure reason")
        if item["state"] == "unavailable" and not item["reason"].strip():
            raise ActorContractError("unavailable reference requires a reason")


def _relative_path(value: Any, field: str) -> str:
    text = _text(value, field).replace("\\", "/")
    if text.startswith("/") or any(part in {"", ".."} for part in text.split("/")):
        raise ActorContractError(f"{field} must be a bounded relative path")
    return text


def _string_list(
    value: Any,
    field: str,
    *,
    allow_empty: bool = False,
) -> list[str]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item.strip() for item in value
    ):
        raise ActorContractError(f"{field} must be a text list")
    if not allow_empty and not value:
        raise ActorContractError(f"{field} must not be empty")
    if len(value) != len(set(value)):
        raise ActorContractError(f"{field} must not contain duplicates")
    return list(value)


def _relative_path_list(value: Any, field: str) -> list[str]:
    items = _string_list(value, field, allow_empty=True)
    for item in items:
        _relative_path(item, field)
    return items


__all__ = [
    "ACTOR_ACTION_ENVELOPE_SCHEMA",
    "ACTOR_ACTION_RECEIPT_SCHEMA",
    "AGENT_ACTION_CONTEXT_V2_SCHEMA",
    "AGENT_ACTION_CONTEXT_V3_SCHEMA",
    "STANDALONE_AGENT_ACTION_CONTEXT_V1_SCHEMA",
    "STANDALONE_AGENT_ACTION_CONTEXT_V2_SCHEMA",
    "STANDALONE_AGENT_ACTION_CONTEXT_V3_SCHEMA",
    "STANDALONE_AGENT_ACTION_CONTEXT_V4_SCHEMA",
    "STANDALONE_AGENT_ACTION_CONTEXT_V5_SCHEMA",
    "STANDALONE_AGENT_ACTION_CONTEXT_SCHEMA",
    "AGENT_ACTION_OUTCOME_SCHEMA",
    "NATIVE_TURN_OUTCOME_SCHEMA",
    "ROLE_BINDING_SCHEMA",
    "ActorContractError",
    "validate_actor_action_envelope",
    "validate_actor_action_receipt",
    "validate_agent_action_context_v2",
    "validate_agent_action_context_v3",
    "validate_agent_action_outcome",
    "validate_standalone_agent_action_context",
    "validate_native_turn_outcome",
    "validate_role_binding",
]
