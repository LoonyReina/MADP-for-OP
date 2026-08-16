from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping


BUDGET_POLICY_VERSION = "ascendop.device-budget.v3"
RETRY_POLICY_VERSION = "ascendop.retry.v3"

STANDARD_DEFAULT_SECONDS = 210
STANDARD_MAX_SECONDS = 240
HEAVY_MAX_SECONDS = 900
PROFILER_PROCESS_SECONDS = 90
PROFILER_STARTUP_GRACE_SECONDS = 30

FAILURE_DOMAINS = {
    "business",
    "protocol",
    "transport",
    "host-build",
    "device-runtime",
    "profiler",
    "export",
    "cancelled",
}


class FlowV3PolicyError(ValueError):
    pass


@dataclass(frozen=True)
class BudgetGrant:
    budget_class: str
    requested_device_session_seconds: int
    granted_device_session_seconds: int
    policy_version: str
    policy_digest: str
    basis: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "budget_class": self.budget_class,
            "requested_device_session_seconds": self.requested_device_session_seconds,
            "granted_device_session_seconds": self.granted_device_session_seconds,
            "budget_policy_version": self.policy_version,
            "budget_policy_digest": self.policy_digest,
            "budget_basis": self.basis,
        }


@dataclass(frozen=True)
class FailureRecord:
    domain: str
    code: str
    phase: str
    detail: str
    retryable: bool
    pre_publish: bool = False
    result_visibility: str = "known"

    def to_dict(self) -> dict[str, Any]:
        return {
            "domain": self.domain,
            "code": self.code,
            "phase": self.phase,
            "detail": self.detail,
            "retryable": self.retryable,
            "pre_publish": self.pre_publish,
            "result_visibility": self.result_visibility,
        }


@dataclass(frozen=True)
class RetryDecision:
    action: str
    consumes_execution_attempt: bool
    reason: str
    policy_version: str = RETRY_POLICY_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "consumes_execution_attempt": self.consumes_execution_attempt,
            "reason": self.reason,
            "policy_version": self.policy_version,
        }


def grant_device_budget(
    request: Mapping[str, Any],
    *,
    gate_evidence: str = "",
) -> BudgetGrant:
    budget_class = str(request.get("budget_class") or "standard")
    requested = int(request.get("requested_device_session_seconds", 0) or 0)
    profiler_mode = str(request.get("performance_mode") or "none")
    if requested < 0:
        raise FlowV3PolicyError("requested device session seconds cannot be negative")
    if budget_class == "standard":
        effective_request = requested or STANDARD_DEFAULT_SECONDS
        if effective_request > STANDARD_MAX_SECONDS:
            raise FlowV3PolicyError(
                f"standard device budget exceeds {STANDARD_MAX_SECONDS} seconds"
            )
        granted = effective_request
        basis = "standard-request" if requested else "standard-default"
    elif budget_class == "diagnostic":
        process_count = {
            "primary-all-cases": 1,
            "primary-roofline-all-cases": 2,
        }.get(profiler_mode)
        if process_count is None:
            raise FlowV3PolicyError(
                "diagnostic budget requires a registered all-case profiler mode"
            )
        derived = process_count * PROFILER_PROCESS_SECONDS + PROFILER_STARTUP_GRACE_SECONDS
        effective_request = requested or derived
        if effective_request > STANDARD_MAX_SECONDS:
            raise FlowV3PolicyError(
                f"diagnostic device budget exceeds {STANDARD_MAX_SECONDS} seconds"
            )
        granted = effective_request
        basis = f"diagnostic-{process_count}-process"
    elif budget_class == "heavy":
        if not gate_evidence.strip():
            raise FlowV3PolicyError("heavy device budget requires real gate evidence")
        if requested <= STANDARD_MAX_SECONDS or requested > HEAVY_MAX_SECONDS:
            raise FlowV3PolicyError(
                f"heavy device budget must be within "
                f"{STANDARD_MAX_SECONDS + 1}..{HEAVY_MAX_SECONDS} seconds"
            )
        granted = requested
        effective_request = requested
        basis = "heavy-gate-evidence"
    elif budget_class == "maintenance":
        if requested <= 0 or requested > HEAVY_MAX_SECONDS:
            raise FlowV3PolicyError(
                f"maintenance device budget must be within 1..{HEAVY_MAX_SECONDS}"
            )
        granted = requested
        effective_request = requested
        basis = "maintenance-request"
    else:
        raise FlowV3PolicyError(f"unsupported budget class: {budget_class}")
    policy_material = {
        "version": BUDGET_POLICY_VERSION,
        "standard_default_seconds": STANDARD_DEFAULT_SECONDS,
        "standard_max_seconds": STANDARD_MAX_SECONDS,
        "heavy_max_seconds": HEAVY_MAX_SECONDS,
        "profiler_process_seconds": PROFILER_PROCESS_SECONDS,
        "profiler_startup_grace_seconds": PROFILER_STARTUP_GRACE_SECONDS,
    }
    digest = hashlib.sha256(
        json.dumps(
            policy_material,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return BudgetGrant(
        budget_class=budget_class,
        requested_device_session_seconds=effective_request,
        granted_device_session_seconds=granted,
        policy_version=BUDGET_POLICY_VERSION,
        policy_digest=digest,
        basis=basis,
    )


def validate_failure(record: FailureRecord) -> FailureRecord:
    if record.domain not in FAILURE_DOMAINS:
        raise FlowV3PolicyError(f"unsupported failure domain: {record.domain}")
    if record.result_visibility not in {"known", "unknown", "not-published"}:
        raise FlowV3PolicyError(
            f"unsupported result visibility: {record.result_visibility}"
        )
    return record


def decide_retry(
    failure: FailureRecord,
    *,
    execution_attempt: int,
    max_execution_attempts: int,
    transport_retry: int,
    max_transport_retries: int,
    stage_retry: int,
    max_idempotent_stage_retries: int,
    stage_idempotent: bool,
) -> RetryDecision:
    failure = validate_failure(failure)
    if failure.phase == "ack" and failure.domain == "transport":
        return RetryDecision(
            action="reconcile",
            consumes_execution_attempt=False,
            reason=(
                "return acknowledgement transport failures retain the durable "
                "result and reconcile the same attempt"
            ),
        )
    if failure.domain in {"business", "protocol", "cancelled"}:
        return RetryDecision(
            action="terminal",
            consumes_execution_attempt=False,
            reason=f"{failure.domain} failures are not retryable",
        )
    if failure.result_visibility == "unknown":
        return RetryDecision(
            action="reconcile",
            consumes_execution_attempt=False,
            reason="result visibility is unknown",
        )
    if failure.pre_publish or failure.result_visibility == "not-published":
        if transport_retry < max_transport_retries:
            return RetryDecision(
                action="retry-transport",
                consumes_execution_attempt=False,
                reason="request was not published",
            )
        return RetryDecision(
            action="terminal",
            consumes_execution_attempt=False,
            reason="transport retry budget exhausted before publication",
        )
    if (
        stage_idempotent
        and failure.domain in {"host-build", "export"}
        and stage_retry < max_idempotent_stage_retries
    ):
        return RetryDecision(
            action="retry-stage",
            consumes_execution_attempt=False,
            reason="idempotent host/export stage retry allowed",
        )
    if not failure.retryable:
        return RetryDecision(
            action="terminal",
            consumes_execution_attempt=False,
            reason="failure is classified non-retryable",
        )
    if failure.domain == "profiler":
        return RetryDecision(
            action="terminal",
            consumes_execution_attempt=False,
            reason="profiler failures do not auto-retry",
        )
    if execution_attempt < max_execution_attempts:
        return RetryDecision(
            action="retry-execution",
            consumes_execution_attempt=True,
            reason="recognized infrastructure failure",
        )
    return RetryDecision(
        action="terminal",
        consumes_execution_attempt=False,
        reason="execution retry budget exhausted",
    )
