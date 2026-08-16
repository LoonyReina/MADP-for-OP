from __future__ import annotations

from ascendop_daemon.core.models import ActionKind, DaemonConfig, DaemonPlan, GateDecision
from ascendop_daemon.control_plane.workflow_priority import flow_priority


RESOURCE_ACTIONS = {
    ActionKind.DISPATCH_SUBMIT,
    ActionKind.RECOVER_BLOCKED,
    ActionKind.RECOVER_GITPARTNER_WORKTREE,
    ActionKind.HEARTBEAT_ACTIVE_REQUEST,
    ActionKind.CANCEL_STALLED_REQUEST,
}

NOTIFICATION_ACTIONS = {
    ActionKind.NOTIFY_SOLVER,
    ActionKind.NOTIFY_TESTER_CASEGEN,
}


class Scheduler:
    def __init__(self, config: DaemonConfig) -> None:
        self.config = config

    def plan(
        self,
        decisions: tuple[GateDecision, ...],
        previous_scheduler_state: dict[str, object] | None = None,
        traffic_balance: dict[str, object] | None = None,
        operator_priorities: dict[str, int] | None = None,
    ) -> DaemonPlan:
        previous_scheduler_state = previous_scheduler_state or {}
        self.operator_priorities = operator_priorities or {}
        notices = [d for d in decisions if d.action in NOTIFICATION_ACTIONS]
        runnable = [
            d
            for d in decisions
            if d.action not in {ActionKind.HOLD, ActionKind.REVIEW_MANUAL, *NOTIFICATION_ACTIONS}
        ]
        if not runnable:
            manual = [d for d in decisions if d.action == ActionKind.REVIEW_MANUAL]
            if manual:
                selected = max(manual, key=lambda d: self._priority_key(d, traffic_balance))
                held = tuple(d for d in decisions if d is not selected and d.action not in NOTIFICATION_ACTIONS)
                return DaemonPlan(
                    selected=selected,
                    decisions=decisions,
                    held=held,
                    scheduler_state=with_balance_state(
                        next_scheduler_state(selected, previous_scheduler_state),
                        traffic_balance,
                    ),
                )
            selected = max(notices, key=lambda d: self._priority_key(d, traffic_balance), default=None)
            return DaemonPlan(
                selected=selected,
                decisions=decisions,
                scheduler_state=with_balance_state(
                    next_scheduler_state(selected, previous_scheduler_state),
                    traffic_balance,
                ),
            )

        # A ready submit is the only action that immediately occupies an idle
        # device.  Peer cancel/heartbeat/worktree recovery may be useful, but
        # must not consume the completion-to-next-submit SLA while a trusted
        # submit is already runnable.  If the submit exposes a real global
        # worktree blocker, its failure is routed into the specialized recovery
        # path on the following tick.
        direct_submits = [d for d in runnable if d.action == ActionKind.DISPATCH_SUBMIT]
        if direct_submits:
            candidates = self._prioritize_balance_debt(direct_submits, traffic_balance)
            fairness_hold = self._fairness_hold_if_no_alternate(
                candidates,
                decisions,
                previous_scheduler_state,
                traffic_balance,
            )
            if fairness_hold is not None:
                base, hold = fairness_hold
                adjusted = tuple(hold if d is base else d for d in decisions)
                held = tuple(d for d in decisions if d is not base and d.action not in NOTIFICATION_ACTIONS)
                return DaemonPlan(
                    selected=hold,
                    decisions=adjusted,
                    held=held,
                    scheduler_state=with_balance_state(
                        next_scheduler_state(hold, previous_scheduler_state),
                        traffic_balance,
                    ),
                )
            selected = self._select_with_fairness(candidates, previous_scheduler_state, traffic_balance)
            held = tuple(d for d in decisions if d is not selected and d.action not in NOTIFICATION_ACTIONS)
            return DaemonPlan(
                selected=selected,
                decisions=decisions,
                held=held,
                scheduler_state=with_balance_state(
                    next_scheduler_state(selected, previous_scheduler_state),
                    traffic_balance,
                ),
            )

        # Preparing an already evidence-complete candidate is the shortest
        # path to the next device submit.  Do it before servicing an unrelated
        # terminal pullback, otherwise a failed archived request can repeatedly
        # consume the shared transport lease while ready candidates wait.
        prepare_submits = [d for d in runnable if d.action == ActionKind.PREPARE_SUBMIT]
        if prepare_submits:
            candidates = self._prioritize_balance_debt(prepare_submits, traffic_balance)
            selected = self._select_with_fairness(candidates, previous_scheduler_state, traffic_balance)
            held = tuple(d for d in decisions if d is not selected and d.action not in NOTIFICATION_ACTIONS)
            return DaemonPlan(
                selected=selected,
                decisions=decisions,
                held=held,
                scheduler_state=with_balance_state(
                    next_scheduler_state(selected, previous_scheduler_state),
                    traffic_balance,
                ),
            )

        terminal_pullbacks = [d for d in runnable if is_terminal_pullback(d)]
        if terminal_pullbacks:
            selected = max(terminal_pullbacks, key=lambda d: terminal_pullback_priority_key(d, traffic_balance))
            held = tuple(d for d in decisions if d is not selected and d.action not in NOTIFICATION_ACTIONS)
            return DaemonPlan(
                selected=selected,
                decisions=decisions,
                held=held,
                scheduler_state=with_balance_state(
                    next_scheduler_state(selected, previous_scheduler_state),
                    traffic_balance,
                ),
            )

        urgent_local = [
            d
            for d in runnable
            if d.action in {
                ActionKind.RESTORE_SUBMIT,
                ActionKind.REQUEUE_SUBMIT,
                ActionKind.REPAIR_QUEUE,
            }
        ]
        same_request = [
            d
            for d in runnable
            if d.action in {
                ActionKind.RECOVER_BLOCKED,
                ActionKind.RECOVER_GITPARTNER_WORKTREE,
                ActionKind.HEARTBEAT_ACTIVE_REQUEST,
                ActionKind.CANCEL_STALLED_REQUEST,
            }
        ]
        runnable_scope = self._scope_runnable_to_balance_debt(runnable, traffic_balance)
        candidates = urgent_local or same_request or runnable_scope
        candidates = self._prioritize_balance_debt(candidates, traffic_balance)
        fairness_hold = self._fairness_hold_if_no_alternate(
            candidates,
            decisions,
            previous_scheduler_state,
            traffic_balance,
        )
        if fairness_hold is not None:
            base, hold = fairness_hold
            adjusted = tuple(hold if d is base else d for d in decisions)
            held = tuple(d for d in decisions if d is not base and d.action not in NOTIFICATION_ACTIONS)
            return DaemonPlan(
                selected=hold,
                decisions=adjusted,
                held=held,
                scheduler_state=with_balance_state(
                    next_scheduler_state(hold, previous_scheduler_state),
                    traffic_balance,
                ),
            )
        selected = self._select_with_fairness(candidates, previous_scheduler_state, traffic_balance)
        held = tuple(d for d in decisions if d is not selected and d.action not in NOTIFICATION_ACTIONS)
        return DaemonPlan(
            selected=selected,
            decisions=decisions,
            held=held,
            scheduler_state=with_balance_state(
                next_scheduler_state(selected, previous_scheduler_state),
                traffic_balance,
            ),
        )

    def _select_with_fairness(
        self,
        candidates: list[GateDecision],
        previous_scheduler_state: dict[str, object],
        traffic_balance: dict[str, object] | None = None,
    ) -> GateDecision:
        max_consecutive = int(self.config.policy.get("max_consecutive_runs_per_operator", 0) or 0)
        previous_op = str(previous_scheduler_state.get("selected_op", ""))
        consecutive_count = int(previous_scheduler_state.get("consecutive_count", 0) or 0)
        if max_consecutive > 0 and previous_op and consecutive_count >= max_consecutive:
            alternates = [decision for decision in candidates if decision.row.op != previous_op]
            if alternates:
                return max(alternates, key=lambda d: self._priority_key(d, traffic_balance))
        return max(candidates, key=lambda d: self._priority_key(d, traffic_balance))

    def _fairness_hold_if_no_alternate(
        self,
        candidates: list[GateDecision],
        decisions: tuple[GateDecision, ...],
        previous_scheduler_state: dict[str, object],
        traffic_balance: dict[str, object] | None = None,
    ) -> tuple[GateDecision, GateDecision] | None:
        max_consecutive = int(self.config.policy.get("max_consecutive_runs_per_operator", 0) or 0)
        previous_op = str(previous_scheduler_state.get("selected_op", ""))
        consecutive_count = int(previous_scheduler_state.get("consecutive_count", 0) or 0)
        if max_consecutive <= 0 or not previous_op or consecutive_count < max_consecutive:
            return None
        alternates = [decision for decision in candidates if decision.row.op != previous_op]
        if alternates:
            return None
        same_op = [decision for decision in candidates if decision.row.op == previous_op]
        if not same_op:
            return None
        base = max(same_op, key=lambda d: self._priority_key(d, traffic_balance))
        if base.action not in RESOURCE_ACTIONS:
            return None
        if _manual_alternate_waits_on_candidate(base, decisions):
            return None
        blocked_alternates = [
            decision
            for decision in decisions
            if decision.row.op != previous_op and decision.action == ActionKind.HOLD
            and alternate_hold_should_block_fairness(decision)
        ]
        if blocked_alternates and self.config.policy.get("hold_when_alternate_blocked_after_consecutive", True):
            blocked_ops = ", ".join(sorted({decision.row.op for decision in blocked_alternates}))
            hold = GateDecision(
                row=base.row,
                action=ActionKind.HOLD,
                reason=(
                    "fairness gate: "
                    f"{previous_op} reached max_consecutive_runs_per_operator={max_consecutive}; "
                    f"alternate operator(s) blocked: {blocked_ops}; "
                    "do not keep consuming hardware until the alternate blocker is resolved or policy is overridden"
                ),
                command="",
                priority=0,
                blocks_operator=base.blocks_operator,
            )
            return base, hold
        cooldown_ticks = int(self.config.policy.get("cooldown_ticks_after_consecutive_run", 1) or 0)
        default_hold_count = 1 if previous_scheduler_state.get("fairness_cooldown") else 0
        fairness_hold_count = int(previous_scheduler_state.get("fairness_hold_count", default_hold_count) or 0)
        if cooldown_ticks <= 0 or fairness_hold_count >= cooldown_ticks:
            return None
        hold = GateDecision(
            row=base.row,
            action=ActionKind.HOLD,
            reason=(
                "fairness cooldown: "
                f"{previous_op} reached max_consecutive_runs_per_operator={max_consecutive} "
                "and no alternate operator is currently runnable; hold instead of consuming more hardware"
            ),
            command="",
            priority=0,
            blocks_operator=base.blocks_operator,
        )
        return base, hold

    def _prioritize_balance_debt(
        self,
        candidates: list[GateDecision],
        traffic_balance: dict[str, object] | None,
    ) -> list[GateDecision]:
        if not candidates:
            return candidates
        debt = traffic_debt(traffic_balance)
        if not debt:
            return candidates
        debt_candidates = [decision for decision in candidates if debt.get(decision.row.op, 0) > 0]
        return debt_candidates or candidates

    def _scope_runnable_to_balance_debt(
        self,
        runnable: list[GateDecision],
        traffic_balance: dict[str, object] | None,
    ) -> list[GateDecision]:
        if not runnable or not self.config.policy.get("traffic_balance_prioritize_debt_runnable", True):
            return runnable
        debt = traffic_debt(traffic_balance)
        if not debt:
            return runnable
        debt_runnable = [decision for decision in runnable if debt.get(decision.row.op, 0) > 0]
        return debt_runnable or runnable

    def _balance_debt_notices(
        self,
        notices: list[GateDecision],
        traffic_balance: dict[str, object] | None,
    ) -> list[GateDecision]:
        if not notices or not self.config.policy.get("traffic_balance_block_non_debt_submit_for_trigger", True):
            return []
        debt = traffic_debt(traffic_balance)
        if not debt:
            return []
        return [decision for decision in notices if debt.get(decision.row.op, 0) > 0]

    def _priority_key(
        self,
        decision: GateDecision,
        traffic_balance: dict[str, object] | None,
    ) -> tuple[int, int, int]:
        debt = traffic_debt(traffic_balance).get(decision.row.op, 0)
        focus = flow_priority(
            getattr(self, "operator_priorities", {}),
            decision.row.op,
        )
        return (debt, focus, decision.priority)


def _manual_alternate_waits_on_candidate(base: GateDecision, decisions: tuple[GateDecision, ...]) -> bool:
    for decision in decisions:
        if decision.row.op == base.row.op or decision.action != ActionKind.REVIEW_MANUAL:
            continue
        text = f"{decision.command} {decision.row.next_command} {decision.reason}".lower()
        if base.row.op.lower() in text and any(marker in text for marker in ("queued ahead", "current blockers")):
            return True
    return False


def is_terminal_pullback(decision: GateDecision) -> bool:
    return (
        decision.action == ActionKind.HEARTBEAT_ACTIVE_REQUEST
        and "terminal locally" in decision.reason.lower()
    )


def terminal_pullback_priority_key(
    decision: GateDecision,
    traffic_balance: dict[str, object] | None,
) -> tuple[int, int]:
    debt = traffic_debt(traffic_balance).get(decision.row.op, 0)
    return (decision.priority, debt)


def alternate_hold_should_block_fairness(decision: GateDecision) -> bool:
    reason = decision.reason.lower()
    non_blocking_markers = (
        "relay is still stalled",
        "cooling down after failed execute",
        "waiting retry window",
        "daemon resource busy",
        "stop requested",
        "input/job.json is not the target same-request job",
        "no matching output/status",
        "transport/gp recovery",
        "solver trigger already completed",
        "solver trigger requires ide-native delivery",
        "solver trigger already sent",
        "solver trigger already delivered",
        "solver trigger already acked",
        "solver trigger already active",
        "waiting for solver to advance board",
        "gitpartner_peer_ssh_timeout",
        "holding restore-submit instead of repeating the same infra_fail loop",
    )
    return not any(marker in reason for marker in non_blocking_markers)


def traffic_debt(traffic_balance: dict[str, object] | None) -> dict[str, int]:
    if not isinstance(traffic_balance, dict):
        return {}
    raw = traffic_balance.get("debt", {})
    if not isinstance(raw, dict):
        return {}
    debt: dict[str, int] = {}
    for op, value in raw.items():
        try:
            debt[str(op)] = max(0, int(value))
        except (TypeError, ValueError):
            continue
    return debt


def with_balance_state(
    state: dict[str, object],
    traffic_balance: dict[str, object] | None,
) -> dict[str, object]:
    if not isinstance(traffic_balance, dict) or not traffic_balance:
        return state
    state = dict(state)
    state["traffic_balance"] = {
        "operator_set_generation": traffic_balance.get("operator_set_generation"),
        "metric_epoch_at": traffic_balance.get("metric_epoch_at"),
        "active_operator_count": traffic_balance.get("active_operator_count"),
        "window_size": traffic_balance.get("window_size"),
        "sample_count": traffic_balance.get("sample_count"),
        "ok": traffic_balance.get("ok"),
        "debt": traffic_balance.get("debt", {}),
        "underrepresented_ops": traffic_balance.get("underrepresented_ops", []),
        "operator_priorities": traffic_balance.get("operator_priorities", {}),
    }
    return state


def next_scheduler_state(
    selected: GateDecision | None,
    previous_scheduler_state: dict[str, object],
) -> dict[str, object]:
    if selected is None or selected.action in NOTIFICATION_ACTIONS:
        return {
            "selected_op": "",
            "selected_action": selected.action.value if selected else "",
            "consecutive_count": 0,
        }
    previous_op = str(previous_scheduler_state.get("selected_op", ""))
    previous_count = int(previous_scheduler_state.get("consecutive_count", 0) or 0)
    if selected.action == ActionKind.HOLD:
        return {
            "selected_op": previous_op or selected.row.op,
            "selected_action": selected.action.value,
            "consecutive_count": previous_count,
            "fairness_cooldown": True,
            "fairness_hold_count": int(
                previous_scheduler_state.get(
                    "fairness_hold_count",
                    1 if previous_scheduler_state.get("fairness_cooldown") else 0,
                )
                or 0
            )
            + 1,
        }
    count = previous_count + 1 if previous_op == selected.row.op else 1
    state = {
        "selected_op": selected.row.op,
        "selected_action": selected.action.value,
        "consecutive_count": count,
    }
    if previous_scheduler_state.get("fairness_cooldown") and previous_op == selected.row.op:
        state["fairness_bypass_after_hold"] = True
    return state
