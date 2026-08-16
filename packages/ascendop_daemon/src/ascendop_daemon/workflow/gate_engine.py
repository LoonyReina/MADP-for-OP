from __future__ import annotations

import re

from ascendop_protocol.workflow import SOLVER_STEWARD_ESCALATION_STATE

from ascendop_daemon.core.models import (
    ActionKind,
    BoardRow,
    DaemonConfig,
    GateDecision,
    TransportObservation,
    extract_row_test_version,
    workflow_mode_for,
)


class GateEngine:
    def __init__(self, config: DaemonConfig) -> None:
        self.config = config

    def evaluate(
        self,
        rows: tuple[BoardRow, ...],
        transport: tuple[TransportObservation, ...] = (),
    ) -> tuple[GateDecision, ...]:
        return tuple(self._evaluate_row(row, transport) for row in rows)

    def _evaluate_row(
        self,
        row: BoardRow,
        transport: tuple[TransportObservation, ...],
    ) -> GateDecision:
        command = row.next_command.strip()
        gate = row.gate_stage.lower()
        owner = row.next_owner.lower()

        if gate == "solver-blocked":
            return GateDecision(
                row=row,
                action=ActionKind.HOLD,
                reason=(
                    "board recognized a generation-scoped Solver blocker; "
                    "hold without replay until board-visible evidence invalidates it"
                ),
                command="",
                priority=0,
            )

        if gate == "needs-diagnostic-registration":
            return GateDecision(
                row=row,
                action=ActionKind.REGISTER_SOLVER_DIAGNOSTIC,
                reason=(
                    "board validated an exact-generation typed Solver diagnostic; "
                    "register it once before the diagnostic lane may execute"
                ),
                command=extract_harness_command(
                    command,
                    "register-solver-diagnostic",
                ),
                priority=74,
                action_descriptor=dict(row.action_descriptor),
            )

        if gate in {
            "diagnostic-evidence-ready",
            "diagnostic-evidence-running",
        }:
            return GateDecision(
                row=row,
                action=ActionKind.HOLD,
                reason=(
                    "typed Solver diagnostic is waiting for the device lane"
                    if gate == "diagnostic-evidence-ready"
                    else "typed Solver diagnostic is already running"
                ),
                command="",
                priority=0,
            )

        if gate == "diagnostic-evidence-failed":
            return GateDecision(
                row=row,
                action=ActionKind.NOTIFY_SOLVER,
                reason=(
                    "typed Solver diagnostic failed non-retryably; return its exact "
                    "failure evidence to Solver without creating an execution retry"
                ),
                command="",
                priority=32,
            )

        if gate == "diagnostic-capability-blocked":
            return GateDecision(
                row=row,
                action=ActionKind.HOLD,
                reason=(
                    "the failed correctness replay exhausted the registered "
                    "diagnostic artifacts; hold until capability or evidence changes"
                ),
                command="",
                priority=0,
            )

        if gate == SOLVER_STEWARD_ESCALATION_STATE:
            return GateDecision(
                row=row,
                action=ActionKind.HOLD,
                reason=(
                    "Solver exhausted every registered diagnostic path for this exact "
                    "result generation; daemon must emit one typed steward escalation"
                ),
                command="",
                priority=0,
            )

        if gate == "needs-profiler-evidence":
            return GateDecision(
                row=row,
                action=ActionKind.COLLECT_PROFILER_EVIDENCE,
                reason=(
                    "board selected an exact-generation daemon profiler request; "
                    "persist it for the low-priority Engine evidence lane"
                ),
                command=self.with_profiler_policy(
                    extract_harness_command(command, "collect-profiler-evidence")
                ),
                priority=72,
                action_descriptor=self.profiler_descriptor(row),
            )

        if gate in {
            "profiler-evidence-running",
            "profiler-evidence-unavailable",
            "profiler-evidence-failed",
        }:
            return GateDecision(
                row=row,
                action=ActionKind.HOLD,
                reason=(
                    "daemon profiler evidence is already collecting"
                    if gate == "profiler-evidence-running"
                    else (
                        "deep profiler capability is durably unavailable for this blocker generation"
                        if gate == "profiler-evidence-unavailable"
                        else "profiler auto-retry budget is exhausted; require diagnosis and one explicit retry"
                    )
                ),
                command="",
                priority=0,
            )

        if owner in {"daemon", "harness", "solver"} and "gitpartner-run-msopgen" in command:
            return GateDecision(
                row=row,
                action=ActionKind.GENERATE_WORKSPACE,
                reason="board selected harness-owned msopgen workspace generation",
                command=self.with_gitpartner_policy(
                    extract_harness_command(command, "gitpartner-run-msopgen")
                ),
                priority=80,
            )

        if owner == "solver":
            return GateDecision(
                row=row,
                action=ActionKind.NOTIFY_SOLVER,
                reason=f"board says solver owns {row.gate_stage}",
                command="",
                priority=30 if gate == "result-exists" else 10,
            )

        if any(
            marker in command
            for marker in (
                "promote-release",
                "create-v1-regression-sentinel",
                "record-v1-regression-sentinel",
                "create-case-regression-sentinel",
                "record-case-regression-sentinel",
            )
        ):
            return GateDecision(
                row=row,
                action=ActionKind.ADVANCE_RELEASE,
                reason="board selected daemon-owned release or regression-sentinel advancement",
                command=command,
                priority=68,
            )

        if "restore-submit" in command:
            return GateDecision(
                row=row,
                action=ActionKind.RESTORE_SUBMIT,
                reason="board selected daemon-owned INFRA_FAIL restore/requeue for the same test_version",
                command=extract_harness_command(command, "restore-submit"),
                priority=92,
            )

        if "do not recover" in command.lower() and "gitpartner-heartbeat" in command.lower():
            heartbeat = self.with_gitpartner_policy(extract_harness_command(command, "gitpartner-heartbeat"))
            blocker = extract_command_op(heartbeat) or ""
            if blocker and blocker != row.op:
                return GateDecision(
                    row=row,
                    action=ActionKind.HOLD,
                    reason=(
                        "peer operator is waiting on active GitPartner request "
                        f"{blocker}; only the owning operator should heartbeat/recover it"
                    ),
                    command="",
                    priority=0,
                    blocks_operator=blocker,
                )
            return GateDecision(
                row=row,
                action=ActionKind.HEARTBEAT_ACTIVE_REQUEST,
                reason="another GitPartner request is active or stale; recover/heartbeat that request first",
                command=heartbeat,
                priority=60,
                blocks_operator=blocker,
            )

        if "gitpartner-recover-blocked" in command:
            terminal_observation = terminal_transport_for_row(row, transport)
            if terminal_observation:
                return self.terminal_pullback_decision(row, terminal_observation)
            blocked_transport = transport_for_row(row, transport)
            if stalled_missing_transport(blocked_transport):
                test_version = extract_row_test_version(row) or "unknown-version"
                return GateDecision(
                    row=row,
                    action=ActionKind.HEARTBEAT_ACTIVE_REQUEST,
                    reason=(
                        "blocked GitPartner request has no matching output/status and is already relay-stalled; "
                        "run lightweight heartbeat/pullback so relay-stall policy can prove health or escalate cancel"
                    ),
                    command=self.with_gitpartner_policy(
                        f"python scripts\\next_workflow.py gitpartner-heartbeat "
                        f"{row.op} {test_version} --block-on-stall"
                    ),
                    priority=88,
                    action_descriptor=board_action_descriptor(
                        "gitpartner-heartbeat",
                        [row.op, test_version],
                        {"block_on_stall": True},
                    ),
                )
            return GateDecision(
                row=row,
                action=ActionKind.RECOVER_BLOCKED,
                reason="blocked GitPartner row is active tester work and should use same-request recovery",
                command=self.with_gitpartner_policy(extract_harness_command(command, "gitpartner-recover-blocked")),
                priority=90,
            )

        if "gitpartner-heartbeat" in command:
            return GateDecision(
                row=row,
                action=ActionKind.HEARTBEAT_ACTIVE_REQUEST,
                reason="board selected GitPartner heartbeat/stall recovery",
                command=self.with_gitpartner_policy(extract_harness_command(command, "gitpartner-heartbeat")),
                priority=85,
            )

        # A pending candidate can still be blocked by an expired case lifetime.
        # The board may include both the future prepare-submit command and the
        # required generate-case-version instruction; casegen ownership wins.
        if is_casegen_gate(row):
            if workflow_mode_for(self.config, row.op) == "peer_benchmark":
                return GateDecision(
                    row=row,
                    action=ActionKind.HOLD,
                    reason="peer benchmark uses a fixed comparison case; Tester casegen and case rollover are disabled",
                    command="",
                    priority=0,
                )
            return GateDecision(
                row=row,
                action=ActionKind.NOTIFY_TESTER_CASEGEN,
                reason=(
                    "board selected tester-owned casegen gate; "
                    "notify the operator Tester instead of auto-generating cases"
                ),
                command="",
                priority=65,
            )

        if "prepare-submit" in command:
            return GateDecision(
                row=row,
                action=ActionKind.PREPARE_SUBMIT,
                reason="board selected pending-to-submit preparation",
                command=self.with_prepare_policy(command),
                priority=70,
            )

        if "gitpartner-run-submit" in command:
            return GateDecision(
                row=row,
                action=ActionKind.DISPATCH_SUBMIT,
                reason="board selected trusted GitPartner submit wrapper",
                command=self.with_gitpartner_policy(extract_harness_command(command, "gitpartner-run-submit")),
                priority=75,
            )

        if gate == "submit-ready" and "daemon.py request scan" in command.replace("\\", "/"):
            return GateDecision(
                row=row,
                action=ActionKind.PUBLISH_TEST_REQUEST,
                reason=(
                    "board selected V4 publication of one immutable TestRequest; "
                    "routing remains owned by the control-plane scheduler"
                ),
                command=command,
                priority=76,
                action_descriptor=dict(row.action_descriptor),
            )

        if gate == "submit-inconsistent":
            terminal_observation = terminal_transport_for_row(row, transport)
            if terminal_observation:
                test_version = terminal_observation.test_version or extract_row_test_version(row)
                if test_version:
                    return GateDecision(
                        row=row,
                        action=ActionKind.REPAIR_QUEUE,
                        reason=(
                            "submit package and queue status disagree after terminal GitPartner output; "
                            "mark the queue row done with the archived RESULT.md before further scheduling"
                        ),
                        command=(
                            "python scripts\\next_workflow.py set-queue-status "
                            f"{row.op} {test_version} done "
                            "--claimed-by tester-daemon-queue-repair "
                            f"--result operators_testresult\\{row.op}\\{test_version}\\RESULT.md"
                        ),
                        priority=94,
                        action_descriptor=board_action_descriptor(
                            "set-queue-status",
                            [row.op, test_version, "done"],
                            {
                                "claimed_by": "tester-daemon-queue-repair",
                                "result": (
                                    f"operators_testresult/{row.op}/{test_version}/RESULT.md"
                                ),
                            },
                        ),
                    )

        if gate == "submit-waiting" or "wait for earlier GitPartner job" in command:
            return GateDecision(
                row=row,
                action=ActionKind.HOLD,
                reason="daemon-owned submit is queued behind an active GitPartner request",
                command="",
                priority=0,
            )

        terminal_observation = terminal_transport_for_row(row, transport)
        if terminal_observation:
            return self.terminal_pullback_decision(row, terminal_observation)

        input_target_op = extract_gitpartner_input_target_op(command)
        if input_target_op and input_target_op != row.op:
            return GateDecision(
                row=row,
                action=ActionKind.HOLD,
                reason=(
                    "GitPartner input currently targets peer operator "
                    f"{input_target_op}; wait for that owning request to finish or recover"
                ),
                command="",
                priority=0,
                blocks_operator=input_target_op,
            )

        if "pending" in gate or "submit" in gate:
            return GateDecision(
                row=row,
                action=ActionKind.REVIEW_MANUAL,
                reason="daemon-owned test-plane gate needs a recognized harness command",
                command=command,
                priority=20,
            )

        return GateDecision(
            row=row,
            action=ActionKind.HOLD,
            reason=f"no daemon-owned action for gate {row.gate_stage}",
            command="",
            priority=0,
        )

    def terminal_pullback_decision(
        self,
        row: BoardRow,
        terminal_observation: TransportObservation,
    ) -> GateDecision:
        test_version = terminal_observation.test_version or extract_row_test_version(row)
        terminal_state = str(terminal_observation.state or "").strip().lower()
        return GateDecision(
            row=row,
            action=ActionKind.HEARTBEAT_ACTIVE_REQUEST,
            reason=(
                "GitPartner output is terminal locally; run same-request heartbeat "
                "pullback/archive before treating the row as stalled"
            ),
            command=(
                self.with_gitpartner_policy(
                    f"python scripts\\next_workflow.py gitpartner-heartbeat "
                    f"{row.op} {test_version} --block-on-stall"
                )
            ),
            priority=99 if terminal_state == "success" else 96,
            action_descriptor=board_action_descriptor(
                "gitpartner-heartbeat",
                [row.op, test_version],
                {"block_on_stall": True},
            ),
        )

    def profiler_descriptor(self, row: BoardRow) -> dict[str, object]:
        descriptor = dict(row.action_descriptor)
        options = dict(descriptor.get("options", {}))
        max_cases = max(
            1,
            int(self.config.policy.get("test_engine_profiler_max_cases", 4) or 4),
        )
        roofline_cases = max(
            0,
            int(
                self.config.policy.get(
                    "test_engine_profiler_roofline_case_count", 1
                )
                or 0
            ),
        )
        options.update(
            {
                "max_cases": max_cases,
                "roofline_case_count": min(roofline_cases, max_cases),
                "primary_metrics": str(
                    self.config.policy.get(
                        "test_engine_profiler_primary_metrics",
                        "PipeUtilization,Occupancy,KernelScale,BasicInfo",
                    )
                    or "PipeUtilization,Occupancy,KernelScale,BasicInfo"
                ).strip(),
            }
        )
        descriptor["options"] = options
        return descriptor

    def with_gitpartner_policy(self, command: str) -> str:
        if "gitpartner-" not in command:
            return command
        if "gitpartner-run-msopgen" in command:
            gitpartner_repo = str(
                self.config.policy.get("test_engine_gitpartner_repo", "") or ""
            ).strip()
            if gitpartner_repo and "--gitpartner-repo" not in command:
                command = f"{command} --gitpartner-repo {quote_command_arg(gitpartner_repo)}"
            remote_root = str(
                self.config.policy.get("test_engine_remote_root", "")
                or self.config.remote_root
                or ""
            ).strip()
            if remote_root and "--remote-root" not in command:
                command = f"{command} --remote-root {quote_command_arg(remote_root)}"
            endpoint_id = str(
                self.config.policy.get("test_engine_endpoint_id", "") or ""
            ).strip()
            if endpoint_id and "--endpoint-id" not in command:
                command = f"{command} --endpoint-id {quote_command_arg(endpoint_id)}"
            return command
        if "gitpartner-run-submit" in command and "--gitpartner-transport" not in command:
            remote_root = str(
                self.config.policy.get("test_engine_remote_root", "")
                or self.config.remote_root
                or ""
            ).strip()
            if remote_root:
                command = set_command_option(command, "--remote-root", remote_root)
            request_transport = str(self.config.policy.get("gitpartner_request_transport", "") or "").strip().lower()
            if request_transport in {"direct", "relay", "auto"}:
                command = f"{command} --gitpartner-transport {request_transport}"
        if "gitpartner-run-submit" in command and "--output-poll-interval-seconds" not in command:
            interval = float(self.config.policy.get("gitpartner_output_poll_interval_seconds", 0) or 0)
            if interval > 0:
                command = f"{command} --output-poll-interval-seconds {interval:g}"
        if "--heartbeat-timeout-seconds" in command:
            return command
        if "gitpartner-run-submit" in command:
            timeout = int(
                self.config.policy.get(
                    "gitpartner_submit_heartbeat_timeout_seconds",
                    self.config.policy.get("gitpartner_heartbeat_timeout_seconds", 0),
                )
                or 0
            )
        else:
            timeout = int(self.config.policy.get("gitpartner_heartbeat_timeout_seconds", 0) or 0)
        if timeout <= 0:
            return command
        return f"{command} --heartbeat-timeout-seconds {timeout}"

    def with_prepare_policy(self, command: str) -> str:
        remote_root = str(
            self.config.policy.get("test_engine_remote_root", "")
            or self.config.remote_root
            or ""
        ).strip()
        if remote_root:
            command = set_command_option(command, "--remote-root", remote_root)
        endpoint_id = str(
            self.config.policy.get("test_engine_endpoint_id", "") or ""
        ).strip()
        if endpoint_id:
            command = set_command_option(command, "--endpoint-id", endpoint_id)
        return command

    def with_profiler_policy(self, command: str) -> str:
        max_cases = max(
            1,
            int(self.config.policy.get("test_engine_profiler_max_cases", 4) or 4),
        )
        roofline_cases = max(
            0,
            int(
                self.config.policy.get(
                    "test_engine_profiler_roofline_case_count", 1
                )
                or 0
            ),
        )
        primary_metrics = str(
            self.config.policy.get(
                "test_engine_profiler_primary_metrics",
                "PipeUtilization,Occupancy,KernelScale,BasicInfo",
            )
            or "PipeUtilization,Occupancy,KernelScale,BasicInfo"
        ).strip()
        command = set_command_option(command, "--max-cases", str(max_cases))
        command = set_command_option(
            command,
            "--roofline-case-count",
            str(min(roofline_cases, max_cases)),
        )
        command = set_command_option(
            command,
            "--primary-metrics",
            primary_metrics,
        )
        return command


def quote_command_arg(value: str) -> str:
    if not any(char.isspace() for char in value) and '"' not in value:
        return value
    return '"' + value.replace('"', '\\"') + '"'


def board_action_descriptor(
    operation: str,
    positional: list[str],
    options: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "schema": "ascendop.board-action.v1",
        "operation": operation,
        "positional": positional,
        "options": options or {},
    }


def set_command_option(command: str, option: str, value: str) -> str:
    replacement = f"{option} {quote_command_arg(value)}"
    pattern = rf"(?<!\S){re.escape(option)}\s+(?:\"(?:[^\"\\]|\\.)*\"|'[^']*'|\S+)"
    if re.search(pattern, command):
        return re.sub(pattern, replacement, command, count=1)
    return f"{command} {replacement}"


def extract_harness_command(text: str, command_name: str) -> str:
    normalized = text.replace("`", "").replace("\\", "/")
    marker = f"python scripts/next_workflow.py {command_name}"
    idx = normalized.find(marker)
    if idx < 0:
        return text.strip()
    command = normalized[idx:]
    stop_match = re.search(
        r"(\s(?:\||\.)\s|;\s|\s+to\s+(?:wait|record)\b|\s*,\s*and\s+|\s+and\s+do\s+not|\s+do\s+not)",
        command,
        flags=re.IGNORECASE,
    )
    if stop_match:
        command = command[: stop_match.start()].strip()
    command = command.rstrip(" ;")
    return command.replace("/", "\\", 1).replace("scripts/next_workflow.py", "scripts\\next_workflow.py")


def extract_command_op(command: str) -> str:
    parts = command.replace("\\", "/").split()
    try:
        idx = parts.index("gitpartner-heartbeat")
    except ValueError:
        return ""
    return parts[idx + 1] if idx + 1 < len(parts) else ""


def extract_gitpartner_input_target_op(command: str) -> str:
    match = re.search(
        r"gitpartner/input\s+job\s+currently\s+targets\s+([A-Za-z0-9_]+)/",
        command,
        flags=re.IGNORECASE,
    )
    return match.group(1) if match else ""


def terminal_transport_for_row(
    row: BoardRow,
    transport: tuple[TransportObservation, ...],
) -> TransportObservation | None:
    matches = [
        obs
        for obs in transport_for_row_all(row, transport)
        if bool(obs.terminal)
    ]
    if not matches:
        return None
    return matches[0]


def transport_for_row(
    row: BoardRow,
    transport: tuple[TransportObservation, ...],
) -> TransportObservation | None:
    matches = transport_for_row_all(row, transport)
    return matches[0] if matches else None


def transport_for_row_all(
    row: BoardRow,
    transport: tuple[TransportObservation, ...],
) -> list[TransportObservation]:
    test_version = extract_row_test_version(row)
    return [
        obs
        for obs in transport
        if obs.op == row.op
        and (not test_version or obs.test_version == test_version)
    ]


def stalled_missing_transport(observation: TransportObservation | None) -> bool:
    if observation is None:
        return False
    if bool(observation.terminal):
        return False
    state = str(observation.state or "").lower()
    remote = str(observation.remote_feedback_status or "").lower()
    return bool(observation.stalled) and state in {"", "missing"} and remote in {"", "stale", "missing"}


def is_casegen_gate(row: BoardRow) -> bool:
    gate = row.gate_stage.lower()
    text = f"{row.gate_stage} {row.next_command} {row.tester_goal} {row.wakeups}".lower()
    return (
        gate == "needs-case-version"
        or "needs_casegen" in text
        or "generate-case-version" in text
        or "casegen" in text and ("rollover" in text or "case" in gate)
    )
