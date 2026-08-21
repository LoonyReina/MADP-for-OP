from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ascendop_daemon.runtime.control import read_stop_request

if TYPE_CHECKING:
    from ascendop_daemon.runtime.application import V4Application


def run_application_tick(
    application: V4Application,
    *,
    nonblocking_dispatch: bool,
) -> dict[str, Any]:
    stop_request = read_stop_request(application.paths.root)
    application._assert_live_generation_compatible()
    drain_before = application.has_drain_work()
    endpoint_reconciliation = (
        application._endpoint_reconciliation.poll(schedule=not bool(stop_request))
        if nonblocking_dispatch
        else {"state": "not-scheduled", "active": False}
    )
    workflow_result_recovery = application.dispatchers.reconcile_results(limit=1)
    agent_source_promotions = application.codex_ide_adapter.reconcile_promotions()
    workflow_actions = (
        {"state": "stopped", "action": None, "errors": []}
        if stop_request
        else application._coordinate_workflow_actions()
    )
    workflow_execution = (
        {"state": "stopped", "launched": False}
        if stop_request
        else application._launch_workflow_action()
    )
    intake = (
        {"state": "stopped", "generated_count": 0}
        if stop_request
        else application.submit_intake.run_once()
    )
    evidence_operation_intake = (
        {"state": "stopped", "generated_count": 0, "errors": []}
        if stop_request
        else application.evidence_operation_intake.run_once()
    )
    diagnostic_intake = (
        {"state": "stopped", "generated_count": 0, "errors": []}
        if stop_request
        else application.diagnostic_intake.run_once()
    )
    dispatch = (
        application._endpoint_dispatch.poll(
            allow_claims=not bool(stop_request),
            schedule=not bool(stop_request) or drain_before,
        )
        if nonblocking_dispatch
        else application.dispatchers.run_once(allow_claims=not bool(stop_request))
    )
    session_gate = (
        {
            "board_rows": 0,
            "eligible_count": 0,
            "actions": [],
            "errors": [],
            "stopped": True,
        }
        if stop_request
        else application.session_gate.run_once()
    )
    current_agent_action_ids = {
        str(item.get("action_id") or "")
        for item in session_gate.get("actions", [])
        if isinstance(item, dict) and item.get("action_id")
    }
    contract_digests, runner_generations = _live_agent_execution_contracts(
        application.database
    )
    retry = application.retry_controller.run_once(
        current_agent_action_ids=current_agent_action_ids,
        current_agent_execution_contract_digests=contract_digests,
        current_agent_runner_generations=runner_generations,
    )
    control_command = application.control_commands.run_once()
    official_progress = application._publish_official_progress()
    if stop_request:
        manager_cycle = application.assistant.run_manager_notifications_once()
        automation = {
            "source_count": 0,
            "steward_escalation_count": 0,
            "protocol_gap_count": 0,
            **manager_cycle,
            "stopped": True,
        }
    else:
        automation = application.assistant.run_once(
            steward_escalations=session_gate.get("steward_escalations", [])
        )
    heartbeat_details = {
        "assistant_trigger": automation,
        "official_progress": official_progress,
        "submit_intake": intake,
        "evidence_operation_intake": evidence_operation_intake,
        "diagnostic_intake": diagnostic_intake,
        "endpoint_dispatch": dispatch,
        "endpoint_reconciliation": endpoint_reconciliation,
        "workflow_result_recovery": workflow_result_recovery,
        "agent_source_promotions": agent_source_promotions,
        "workflow_actions": workflow_actions,
        "workflow_execution": workflow_execution,
        "retry_controller": retry,
        "control_command": control_command,
        "session_gate": session_gate,
    }
    application._heartbeat(
        state="draining" if stop_request else "ready",
        details=heartbeat_details,
    )
    return {
        "schema": "ascendop.daemon-tick.v4",
        "generation": application.generation,
        "stop_fenced": bool(stop_request),
        "stop_request": stop_request or {},
        "dispatch": dispatch,
        "intake": intake,
        "evidence_operation_intake": evidence_operation_intake,
        "diagnostic_intake": diagnostic_intake,
        "retry": retry,
        "control_command": control_command,
        "session_gate": session_gate,
        "endpoint_reconciliation": endpoint_reconciliation,
        "workflow_result_recovery": workflow_result_recovery,
        "agent_source_promotions": agent_source_promotions,
        "workflow_actions": workflow_actions,
        "workflow_execution": workflow_execution,
        "automation": automation,
        "official_progress": official_progress,
        "drain_work": application.has_drain_work(),
    }


def _live_agent_execution_contracts(database: Any) -> tuple[set[str], set[str]]:
    contract_digests: set[str] = set()
    runner_generations: set[str] = set()
    for service in database.service_health():
        if service.get("role") != "agent-execution" or not service.get("live"):
            continue
        details = service.get("details", {})
        details = details if isinstance(details, dict) else {}
        contract_digest = str(details.get("execution_contract_digest") or "").strip()
        runner_generation = str(details.get("runner_generation") or "").strip()
        if contract_digest:
            contract_digests.add(contract_digest)
        if runner_generation:
            runner_generations.add(runner_generation)
    return contract_digests, runner_generations
