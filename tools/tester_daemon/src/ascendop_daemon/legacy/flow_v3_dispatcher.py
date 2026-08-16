from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Protocol

from ascendop_daemon.control_plane.control_database import ControlDatabase
from ascendop_daemon.exchange.engine_result_ingestor import EngineResultIngestor
from ascendop_daemon.exchange.flow_v3_payload import io_path, materialize_payload
from ascendop_daemon.control_plane.flow_v3_policy import FailureRecord, decide_retry
from ascendop_protocol.wire_v3 import TERMINAL_STATES, canonical_digest
from ascendop_daemon.legacy.flow_v3_store import FlowV3Store, FlowV3StoreError
from ascendop_daemon.registry.node_reconciler import query_gitpartner_node_report


REMOTE_TERMINAL_STATES = {"completed", "failed"}


def gitpartner_transport_generation(source_root: Path) -> str:
    package_root = source_root.resolve() / "limited_remote_partner"
    paths = sorted(package_root.rglob("*.py"), key=lambda item: item.as_posix())
    if not paths:
        raise ValueError(f"GitPartner package source is missing: {package_root}")
    digest = hashlib.sha256()
    for path in paths:
        relative = path.relative_to(source_root).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        data = path.read_bytes().replace(b"\r\n", b"\n")
        digest.update(len(data).to_bytes(8, "big"))
        digest.update(data)
    return digest.hexdigest()


class FlowV3DispatchError(RuntimeError):
    pass


class FlowV3TransportError(FlowV3DispatchError):
    def __init__(
        self,
        detail: str,
        *,
        pre_publish: bool = False,
        result_visibility: str = "unknown",
        domain: str = "transport",
        code: str = "flow-v3-transport",
        retryable: bool = True,
    ) -> None:
        super().__init__(detail)
        self.pre_publish = pre_publish
        self.result_visibility = result_visibility
        self.domain = domain
        self.code = code
        self.retryable = retryable


@dataclass(frozen=True)
class TransportObservation:
    control_ref: str
    action: str
    observation: dict[str, Any]
    return_root: Path | None = None


class FlowV3Transport(Protocol):
    def exchange(
        self,
        envelope: Mapping[str, Any],
        *,
        action: str,
        control_ref: str,
        receipt_id: str = "",
    ) -> TransportObservation:
        ...


class FlowV3WorkflowAdapter(Protocol):
    def ingest(
        self,
        envelope: Mapping[str, Any],
        *,
        materialized_root: Path,
    ) -> dict[str, Any]:
        ...


def materialized_result_root(
    result_root: Path,
    *,
    request_id: str,
    attempt_id: str,
) -> Path:
    return io_path(result_root / request_id / attempt_id / "payload")


class CannJudgeFlowV3WorkflowAdapter:
    def __init__(self, root: Path, *, gitpartner_repo: Path) -> None:
        self.root = root.resolve()
        self.ingestor = EngineResultIngestor(
            self.root,
            gitpartner_repo=gitpartner_repo,
        )

    def ingest(
        self,
        envelope: Mapping[str, Any],
        *,
        materialized_root: Path,
    ) -> dict[str, Any]:
        spec_path = materialized_root / "spec.json"
        spec = read_object(spec_path)
        expected_digest = canonical_digest(envelope)
        if (
            str(spec.get("request_id") or "")
            != str(envelope["meta"]["request_id"])
            or str(spec.get("attempt_id") or "")
            != str(envelope["meta"]["attempt_id"])
            or str(spec.get("operator") or "")
            != str(envelope["workflow"]["operator"])
            or str(spec.get("test_version") or "")
            != str(envelope["workflow"]["test_version"])
            or str(spec.get("wire_v3", {}).get("envelope_digest") or "")
            != expected_digest
        ):
            raise FlowV3DispatchError(
                "returned Engine spec does not match the Wire V3 envelope"
            )
        return self.ingestor.ingest(
            {
                "request_id": str(spec["request_id"]),
                "engine_job_id": str(spec["engine_job_id"]),
                "attempt_id": str(spec["attempt_id"]),
                "operator": str(spec["operator"]),
                "test_version": str(spec["test_version"]),
                "collect_request_id": (
                    f"flow-v3-{envelope['meta']['request_id']}-"
                    f"{envelope['meta']['attempt_id']}"
                ),
                "spec_path": str(spec_path),
                "snapshot_bundle_root": str(materialized_root),
                "terminal_state": str(
                    read_object(materialized_root / "terminal.json").get(
                        "state", ""
                    )
                ),
            }
        )


@dataclass(frozen=True)
class GitPartnerTransportConfig:
    repo: Path
    result_worktree: Path
    endpoint_id: str
    endpoint_generation: str
    registration_generation: str
    remote_root: str
    engine_root: str = "test_engine_demo"
    target_node: str = "example-910b-cann90"
    target_environment_id: str = ""
    transport: str = "direct"
    transport_mode: str = "direct-git"
    control_channel: str = ""
    node_report_branch: str = "gp/nodes"
    node_report_cache_seconds: int = 5
    node_liveness_query_timeout_seconds: int = 15
    git_operation_timeout_seconds: int = 60
    wait_timeout_seconds: int = 180
    package_source: Path | None = None
    package_generation: str = ""
    control_database: Path | None = None


class GitPartnerFlowV3Transport:
    """Pure transport adapter; it never decides workflow gates or retries."""

    def __init__(self, config: GitPartnerTransportConfig) -> None:
        self.config = config
        self._node_report_cache: dict[str, Any] | None = None
        self._node_report_cached_monotonic = 0.0
        self._package_source = (
            config.package_source.resolve()
            if config.package_source is not None
            else (config.repo.resolve() / "src")
        )

    def status(self, *, control_ref: str) -> TransportObservation:
        return self.exchange(
            {
                "meta": {
                    "request_id": (
                        f"status-{self.config.endpoint_id}-"
                        f"{self.config.endpoint_generation[:12]}"
                    ),
                    "attempt_id": "attempt-000",
                }
            },
            action="status",
            control_ref=control_ref,
        )

    def endpoint_report(self, *, force_refresh: bool = False) -> dict[str, Any]:
        """Read endpoint health without publishing a work request."""

        return self._require_fresh_node_report(force_refresh=force_refresh)

    def exchange(
        self,
        envelope: Mapping[str, Any],
        *,
        action: str,
        control_ref: str,
        receipt_id: str = "",
    ) -> TransportObservation:
        if action not in {"accept", "query", "ack", "status"}:
            raise FlowV3TransportError(
                f"unsupported Wire V3 transport action: {action}",
                pre_publish=True,
                result_visibility="not-published",
            )
        self._require_transport_generation()
        cli_transport = gitpartner_cli_transport(
            self.config.transport_mode or self.config.transport
        )
        if cli_transport == "direct":
            self._require_fresh_node_report()
        meta = envelope["meta"]
        request_id = str(meta["request_id"])
        attempt_id = str(meta["attempt_id"])
        repo = self.config.repo.resolve()
        output_subdir = f"flow-v3/{control_ref}"
        command = [
            sys.executable,
            "-s",
            "-m",
            "limited_remote_partner.gateway.submit_job",
            "--repo",
            str(repo),
            "--append-request",
            "--commit-push",
            "--wait",
            "--wait-timeout-seconds",
            str(self.config.wait_timeout_seconds),
            "ascendop-flow-v3-exchange",
            "--request-id",
            control_ref,
            "--output-subdir",
            output_subdir,
            "--transport",
            cli_transport,
            "--client-work-dir",
            self.config.remote_root,
            "--engine-root",
            self.config.engine_root,
            "--target-node",
            self.config.target_node,
            "--target-endpoint-id",
            self.config.endpoint_id,
            "--target-environment-id",
            self.config.target_environment_id,
            "--target-transport-mode",
            self.config.transport_mode,
            "--registration-generation",
            self.config.registration_generation,
            "--endpoint-generation",
            self.config.endpoint_generation,
            "--action",
            action,
            "--logical-request-id",
            request_id,
            "--attempt-id",
            attempt_id,
        ]
        if action == "accept":
            package_root = Path(str(envelope["payload"]["package_root"])).resolve()
            envelope_path = (
                package_root / request_id / "REQUEST_ENVELOPE.json"
            )
            command.extend(
                [
                    "--envelope",
                    str(envelope_path),
                    "--package-root",
                    str(package_root),
                ]
            )
        if action == "ack":
            command.extend(["--receipt-id", receipt_id])
        environment = dict(os.environ)
        source = self._package_source
        environment["PYTHONPATH"] = os.pathsep.join(
            [
                str(source),
                environment.get("PYTHONPATH", ""),
            ]
        ).rstrip(os.pathsep)
        environment["PYTHONNOUSERSITE"] = "1"
        environment["ASCENDOP_FLOW_V3_TRANSPORT_GENERATION"] = (
            self.config.package_generation
        )
        environment["GITPARTNER_GIT_TIMEOUT_SECONDS"] = str(
            max(15, int(self.config.git_operation_timeout_seconds))
        )
        creationflags = (
            getattr(subprocess, "CREATE_NO_WINDOW", 0)
            if os.name == "nt"
            else 0
        )
        try:
            completed = subprocess.run(
                command,
                cwd=repo,
                env=environment,
                text=True,
                capture_output=True,
                timeout=max(30, self.config.wait_timeout_seconds + 30),
                check=False,
                creationflags=creationflags,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise FlowV3TransportError(
                f"Wire V3 transport invocation failed: {exc}",
                pre_publish=isinstance(exc, OSError),
                result_visibility=(
                    "not-published" if isinstance(exc, OSError) else "unknown"
                ),
            ) from exc
        output_root = repo / "output" / "flow-v3" / control_ref
        observation_path = unique_return_path(
            output_root,
            "flow_v3_observation.json",
        )
        if observation_path is None:
            detail = (completed.stderr or completed.stdout or "").strip()
            raise FlowV3TransportError(
                "Wire V3 transport produced no endpoint observation"
                + (f": {detail[-1000:]}" if detail else ""),
                result_visibility="unknown",
            )
        observation = read_object(observation_path)
        if observation.get("schema") == "ascendop.flow.endpoint-nack.v3":
            raise FlowV3TransportError(
                str(observation.get("error") or "endpoint rejected Wire V3"),
                result_visibility="known",
                domain="protocol",
                code=str(observation.get("code") or "endpoint-nack"),
                retryable=bool(observation.get("retryable", False)),
            )
        if completed.returncode != 0:
            raise FlowV3TransportError(
                f"Wire V3 transport exited {completed.returncode}",
                result_visibility="known",
            )
        return_root = None
        if action == "query":
            return_root = unique_return_directory(
                output_root,
                "flow_v3_return",
            )
        return TransportObservation(
            control_ref=control_ref,
            action=action,
            observation=observation,
            return_root=return_root,
        )

    def _require_transport_generation(self) -> None:
        expected = self.config.package_generation
        try:
            actual = gitpartner_transport_generation(self._package_source)
        except (OSError, ValueError) as exc:
            raise FlowV3TransportError(
                f"Wire V3 transport package is unavailable: {exc}",
                pre_publish=True,
                result_visibility="not-published",
                domain="protocol",
                code="transport-package-unavailable",
                retryable=False,
            ) from exc
        if expected and actual != expected:
            raise FlowV3TransportError(
                "Wire V3 transport package generation mismatch"
                f" expected={expected} actual={actual}",
                pre_publish=True,
                result_visibility="not-published",
                domain="protocol",
                code="transport-generation-mismatch",
                retryable=False,
            )

    def _require_fresh_node_report(
        self,
        *,
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        now_monotonic = time.monotonic()
        if (
            not force_refresh
            and self._node_report_cache is not None
            and now_monotonic - self._node_report_cached_monotonic
            < max(0, self.config.node_report_cache_seconds)
        ):
            return self._node_report_cache
        if not force_refresh and self.config.control_database is not None:
            liveness = ControlDatabase(
                self.config.control_database
            ).observed_node_liveness(self.config.target_node)
            report = liveness.get("report", {})
            if bool(liveness.get("live")) and isinstance(report, dict) and report:
                self._validate_node_report_identity(report)
                self._node_report_cache = report
                self._node_report_cached_monotonic = now_monotonic
                return report
        try:
            report = query_gitpartner_node_report(
                root=self.config.repo.parent,
                gitpartner_repo=str(self.config.repo),
                result_worktree=str(self.config.result_worktree),
                report_branch=self.config.node_report_branch,
                control_branch=self.config.control_channel,
                node_id=self.config.target_node,
                package_source=self._package_source,
                command_timeout_seconds=max(
                    5,
                    self.config.node_liveness_query_timeout_seconds,
                ),
            )
        except (OSError, subprocess.SubprocessError, ValueError) as exc:
            raise FlowV3TransportError(
                f"direct-git node liveness query failed: {exc}",
                pre_publish=True,
                result_visibility="not-published",
                code="endpoint-liveness-unavailable",
                retryable=True,
            ) from exc
        self._validate_node_report_identity(report)
        local_liveness: dict[str, Any] | None = None
        if self.config.control_database is not None:
            database = ControlDatabase(self.config.control_database)
            database.ingest_node_report(
                report,
                source=(
                    "flow-v3-direct-git-probe:"
                    f"{self.config.endpoint_id}"
                ),
            )
            local_liveness = database.observed_node_liveness(
                self.config.target_node
            )
            live = bool(local_liveness.get("live"))
        else:
            expires_at = parse_utc_timestamp(
                str(report.get("lease_expires_at") or "")
            )
            live = bool(
                expires_at is not None
                and expires_at > datetime.now(timezone.utc)
            )
        if not live:
            heartbeat_at = str(report.get("heartbeat_at") or "")
            publish_error = str(report.get("last_publish_error") or "")
            detail = (
                "direct-git node heartbeat is stale"
                f" heartbeat_at={heartbeat_at or 'missing'}"
                f" lease_expires_at={report.get('lease_expires_at') or 'missing'}"
            )
            if local_liveness is not None:
                detail += (
                    " local_heartbeat_at="
                    f"{local_liveness.get('heartbeat_at') or 'missing'}"
                    " local_lease_expires_at="
                    f"{local_liveness.get('lease_expires_at') or 'missing'}"
                    f" local_reason={local_liveness.get('reason') or 'unknown'}"
                )
            if publish_error:
                detail += f" last_publish_error={publish_error}"
            raise FlowV3TransportError(
                detail,
                pre_publish=True,
                result_visibility="not-published",
                code="endpoint-heartbeat-stale",
                retryable=True,
            )
        self._node_report_cache = report
        self._node_report_cached_monotonic = now_monotonic
        return report

    def _validate_node_report_identity(self, report: Mapping[str, Any]) -> None:
        if (
            str(report.get("endpoint_id") or "") != self.config.endpoint_id
            or str(report.get("generation") or "")
            != self.config.endpoint_generation
        ):
            raise FlowV3TransportError(
                "direct-git node report identity or generation mismatch",
                pre_publish=True,
                result_visibility="not-published",
                domain="protocol",
                code="endpoint-generation-mismatch",
                retryable=False,
            )


def gitpartner_cli_transport(transport_mode: str) -> str:
    normalized = str(transport_mode or "").strip().lower()
    if normalized in {"direct", "direct-git"}:
        return "direct"
    if normalized in {"relay", "lan-relay"}:
        return "relay"
    raise FlowV3TransportError(
        f"unsupported registered transport mode: {transport_mode}",
        pre_publish=True,
        result_visibility="not-published",
        domain="protocol",
        code="transport-mode-invalid",
        retryable=False,
    )


def parse_utc_timestamp(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


class FlowV3Dispatcher:
    def __init__(
        self,
        store: FlowV3Store,
        transport: FlowV3Transport,
        *,
        component_id: str = "flow-v3-dispatcher",
    ) -> None:
        self.store = store
        self.transport = transport
        self.component_id = component_id

    def enqueue(
        self,
        envelope: Mapping[str, Any],
        *,
        destination: str,
    ) -> dict[str, Any]:
        created = self.store.create_request(
            envelope,
            actor=self.component_id,
        )
        attempt = created["attempt"]
        state = str(attempt["state"])
        request_id = str(attempt["request_id"])
        attempt_id = str(attempt["attempt_id"])
        if state == "created":
            self.store.transition(
                request_id,
                attempt_id,
                expected_state="created",
                current_state="validated",
                event_type="request-validated",
                actor=self.component_id,
            )
            state = "validated"
        if state == "validated":
            self.store.transition(
                request_id,
                attempt_id,
                expected_state="validated",
                current_state="queued",
                event_type="request-queued",
                actor=self.component_id,
                outbox_destination=destination,
            )
        return self.store.attempt(request_id, attempt_id)

    def dispatch_once(
        self,
        *,
        accept_new: bool = True,
    ) -> dict[str, Any] | None:
        claimed = self.store.claim_outbox(
            self.component_id,
            limit=1,
            lease_seconds=180,
            attempt_states=None if accept_new else {"dispatched"},
        )
        if not claimed:
            return None
        item = claimed[0]
        request_id = str(item["request_id"])
        attempt_id = str(item["attempt_id"])
        attempt = self.store.attempt(request_id, attempt_id)
        action = "accept"
        if attempt["state"] == "queued":
            if not accept_new:
                raise FlowV3DispatchError(
                    "stop-fenced dispatcher claimed a queued attempt"
                )
            self.store.transition(
                request_id,
                attempt_id,
                expected_state="queued",
                current_state="admitted",
                event_type="request-admitted",
                actor=self.component_id,
            )
            self.store.transition(
                request_id,
                attempt_id,
                expected_state="admitted",
                current_state="dispatched",
                event_type="request-dispatched",
                actor=self.component_id,
            )
        elif attempt["state"] == "dispatched":
            action = "query"
        else:
            raise FlowV3DispatchError(
                f"claimed outbox while attempt is {attempt['state']}"
            )
        control = self.store.open_transport_control(
            outbox_id=str(item["outbox_id"]),
            request_id=request_id,
            attempt_id=attempt_id,
            action=action,
        )
        control_ref = str(control["control_ref"])
        self.store.record_transport_control(
            control_ref=control_ref,
            outbox_id=str(item["outbox_id"]),
            request_id=request_id,
            attempt_id=attempt_id,
            action=action,
            state="published",
        )
        try:
            exchange = self.transport.exchange(
                item["payload"],
                action=action,
                control_ref=control_ref,
            )
            observation = exchange.observation
            self.store.record_transport_control(
                control_ref=control_ref,
                outbox_id=str(item["outbox_id"]),
                request_id=request_id,
                attempt_id=attempt_id,
                action=action,
                state="terminal",
                observation=observation,
            )
            accepted = normalize_acceptance_observation(observation)
            return self.store.accept_outbox_delivery(
                str(item["outbox_id"]),
                consumer_id=self.component_id,
                claim_token=str(item["claim_token"]),
                observation=accepted,
            )
        except FlowV3TransportError as exc:
            failure = FailureRecord(
                domain=exc.domain,
                code=exc.code,
                phase=action,
                detail=str(exc),
                retryable=exc.retryable,
                pre_publish=exc.pre_publish,
                result_visibility=exc.result_visibility,
            )
            if exc.pre_publish or exc.result_visibility == "known":
                self.store.record_transport_control(
                    control_ref=control_ref,
                    outbox_id=str(item["outbox_id"]),
                    request_id=request_id,
                    attempt_id=attempt_id,
                    action=action,
                    state="failed",
                    observation={"error": str(exc)},
                )
            retry_policy = item["payload"]["retry_policy"]
            decision = decide_retry(
                failure,
                execution_attempt=execution_attempt_ordinal(attempt_id),
                max_execution_attempts=int(
                    retry_policy["max_execution_attempts"]
                ),
                transport_retry=int(control["action_ordinal"]),
                max_transport_retries=int(
                    retry_policy["max_transport_retries"]
                ),
                stage_retry=0,
                max_idempotent_stage_retries=int(
                    retry_policy["max_idempotent_stage_retries"]
                ),
                stage_idempotent=False,
            )
            self.store.record_failure_and_retry(
                request_id,
                attempt_id,
                stage_name="transport",
                stage_try=int(control["action_ordinal"]),
                failure=failure,
                decision=decision,
            )
            if decision.action == "retry-transport":
                self.store.transition(
                    request_id,
                    attempt_id,
                    expected_state="dispatched",
                    current_state="queued",
                    event_type="transport-not-published",
                    actor=self.component_id,
                )
            elif decision.action == "terminal":
                self.store.transition(
                    request_id,
                    attempt_id,
                    expected_state="dispatched",
                    current_state="quarantined",
                    event_type="transport-terminal-quarantine",
                    actor=self.component_id,
                    payload={
                        "failure_domain": failure.domain,
                        "failure_code": failure.code,
                    },
                )
                self.store.transition(
                    request_id,
                    attempt_id,
                    expected_state="quarantined",
                    current_state="terminal-infrastructure-failure",
                    event_type="transport-terminal",
                    actor=self.component_id,
                )
            retry_at = ""
            if decision.action in {"retry-transport", "reconcile"}:
                retry_at = (
                    datetime.now(timezone.utc) + timedelta(seconds=5)
                ).isoformat()
            self.store.fail_outbox(
                str(item["outbox_id"]),
                consumer_id=self.component_id,
                claim_token=str(item["claim_token"]),
                error=str(exc),
                retry_at=retry_at,
            )
            return {
                "request_id": request_id,
                "attempt_id": attempt_id,
                "state": "transport-failed",
                "retry_action": decision.action,
            }


class FlowV3ResultIngestor:
    def __init__(
        self,
        store: FlowV3Store,
        transport: FlowV3Transport,
        *,
        result_root: Path,
        workflow_adapter: FlowV3WorkflowAdapter | None = None,
        component_id: str = "flow-v3-ingestor",
    ) -> None:
        self.store = store
        self.transport = transport
        self.result_root = result_root.resolve()
        self.workflow_adapter = workflow_adapter
        self.component_id = component_id

    def poll_once(self) -> dict[str, Any] | None:
        recoveries = self.store.workflow_ingest_recovery_candidates(limit=1)
        if recoveries:
            recovery = recoveries[0]
            return self.recover_terminal_workflow_ingest(
                str(recovery["request_id"]),
                str(recovery["attempt_id"]),
            )
        attempts = self.store.attempts_in_states(
            {"accepted", "running", "return-ready", "ingested"},
            limit=1,
        )
        if not attempts:
            return None
        attempt = attempts[0]
        envelope = attempt["envelope"]
        request_id = str(attempt["request_id"])
        attempt_id = str(attempt["attempt_id"])
        if str(attempt["state"]) == "ingested":
            return self._acknowledge_ingested(
                envelope,
                request_id=request_id,
                attempt_id=attempt_id,
            )
        control = self.store.open_transport_control(
            outbox_id=f"watch:{request_id}:{attempt_id}",
            request_id=request_id,
            attempt_id=attempt_id,
            action="query",
        )
        control_ref = str(control["control_ref"])
        self.store.record_transport_control(
            control_ref=control_ref,
            outbox_id=str(control["outbox_id"]),
            request_id=request_id,
            attempt_id=attempt_id,
            action="query",
            state="published",
        )
        try:
            exchange = self.transport.exchange(
                envelope,
                action="query",
                control_ref=control_ref,
            )
        except FlowV3TransportError as exc:
            return self._transport_failure(
                envelope,
                attempt,
                control,
                exc,
            )
        self.store.record_transport_control(
            control_ref=control_ref,
            outbox_id=str(control["outbox_id"]),
            request_id=request_id,
            attempt_id=attempt_id,
            action="query",
            state="terminal",
            observation=exchange.observation,
        )
        observation = exchange.observation
        remote_state = str(observation.get("state") or "")
        if remote_state in {"accepted", "queued", "standby"}:
            return {
                "request_id": request_id,
                "attempt_id": attempt_id,
                "state": remote_state,
            }
        local_state = str(attempt["state"])
        if local_state == "accepted":
            self.store.transition(
                request_id,
                attempt_id,
                expected_state="accepted",
                current_state="running",
                event_type="endpoint-running",
                actor=self.component_id,
                payload={"endpoint_state": remote_state},
            )
            local_state = "running"
        if remote_state not in REMOTE_TERMINAL_STATES:
            return {
                "request_id": request_id,
                "attempt_id": attempt_id,
                "state": remote_state,
            }
        if exchange.return_root is None:
            raise FlowV3DispatchError(
                "terminal endpoint observation omitted the result payload"
            )
        if local_state != "return-ready":
            self.store.transition(
                request_id,
                attempt_id,
                expected_state=local_state,
                current_state="return-ready",
                event_type="endpoint-return-ready",
                actor=self.component_id,
                payload={"endpoint_state": remote_state},
            )
        result = adapt_endpoint_result(
            envelope,
            observation,
            exchange.return_root,
            self.result_root / request_id / attempt_id,
        )
        ingested = self.store.ingest_result(
            request_id,
            attempt_id,
            result,
            actor=self.component_id,
        )
        return self._acknowledge_ingested(
            envelope,
            request_id=request_id,
            attempt_id=attempt_id,
            ingested=ingested,
        )

    def recover_terminal_workflow_ingest(
        self,
        request_id: str,
        attempt_id: str,
    ) -> dict[str, Any]:
        """Run only the idempotent adapter for a terminal durable result."""

        attempt = self.store.attempt(request_id, attempt_id)
        if str(attempt["state"]) not in TERMINAL_STATES:
            raise FlowV3DispatchError(
                "workflow ingest recovery requires a terminal attempt"
            )
        workflow = self._ingest_workflow_result(
            attempt["envelope"],
            request_id=request_id,
            attempt_id=attempt_id,
        )
        state = str(workflow.get("state") or "")
        return {
            "request_id": request_id,
            "attempt_id": attempt_id,
            "state": (
                "workflow-ingest-recovered"
                if state == "succeeded"
                else "workflow-ingest-recovery-held"
            ),
            "workflow_ingest": workflow,
        }

    def _acknowledge_ingested(
        self,
        envelope: Mapping[str, Any],
        *,
        request_id: str,
        attempt_id: str,
        ingested: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        durable = (
            dict(ingested)
            if ingested is not None
            else self.store.result_for_attempt(request_id, attempt_id)
        )
        workflow = self._ingest_workflow_result(
            envelope,
            request_id=request_id,
            attempt_id=attempt_id,
        )
        workflow_state = str(workflow.get("state") or "")
        if workflow_state not in {"succeeded", "failed"}:
            return {
                "request_id": request_id,
                "attempt_id": attempt_id,
                "state": "workflow-ingest-held",
                "workflow_ingest": workflow,
            }
        terminal_state = str(durable["terminal_state"])
        if workflow_state == "failed":
            terminal_state = "terminal-infrastructure-failure"
        receipt_id = f"return-{str(durable['result_digest'])[:24]}"
        control = self.store.open_transport_control(
            outbox_id=f"ack:{request_id}:{attempt_id}",
            request_id=request_id,
            attempt_id=attempt_id,
            action="ack",
        )
        ack_ref = str(control["control_ref"])
        self.store.record_transport_control(
            control_ref=ack_ref,
            outbox_id=str(control["outbox_id"]),
            request_id=request_id,
            attempt_id=attempt_id,
            action="ack",
            state="published",
        )
        try:
            acknowledged = self.transport.exchange(
                envelope,
                action="ack",
                control_ref=ack_ref,
                receipt_id=receipt_id,
            )
        except FlowV3TransportError as exc:
            attempt = self.store.attempt(request_id, attempt_id)
            return self._transport_failure(
                envelope,
                attempt,
                control,
                exc,
            )
        ack_observation = normalize_ack_observation(
            acknowledged.observation,
            envelope,
            receipt_id=receipt_id,
        )
        self.store.record_transport_control(
            control_ref=ack_ref,
            outbox_id=str(control["outbox_id"]),
            request_id=request_id,
            attempt_id=attempt_id,
            action="ack",
            state="terminal",
            observation=ack_observation,
        )
        return self.store.acknowledge_result(
            request_id,
            attempt_id,
            receipt_id=receipt_id,
            receipt=ack_observation,
            terminal_state=terminal_state,
            actor=self.component_id,
        )

    def _ingest_workflow_result(
        self,
        envelope: Mapping[str, Any],
        *,
        request_id: str,
        attempt_id: str,
    ) -> dict[str, Any]:
        publish_eligible = bool(
            envelope["execution"].get("publish_eligible")
        )
        operation_kind = str(
            envelope["workflow"].get("operation_kind") or ""
        )
        evidence_ingest_required = operation_kind == "diagnostic-profile"
        if (
            (not publish_eligible and not evidence_ingest_required)
            or self.workflow_adapter is None
        ):
            return {
                "state": "succeeded",
                "outcome": (
                    "not-publish-eligible"
                    if not publish_eligible and not evidence_ingest_required
                    else "no-adapter"
                ),
            }
        claim = self.store.claim_workflow_ingest(
            request_id,
            attempt_id,
            consumer_id=self.component_id,
        )
        state = str(claim["state"])
        if state == "succeeded":
            return claim
        if (
            state != "claimed"
            or str(claim.get("claimed_by") or "") != self.component_id
        ):
            return claim
        try:
            outcome = self.workflow_adapter.ingest(
                envelope,
                materialized_root=materialized_result_root(
                    self.result_root,
                    request_id=request_id,
                    attempt_id=attempt_id,
                ),
            )
        except Exception as exc:
            failure = FailureRecord(
                domain="export",
                code="workflow-result-ingest",
                phase="workflow-ingest",
                detail=f"{type(exc).__name__}: {exc}",
                retryable=True,
                pre_publish=False,
                result_visibility="known",
            )
            retry_policy = envelope["retry_policy"]
            stage_try = int(claim["try_count"])
            decision = decide_retry(
                failure,
                execution_attempt=execution_attempt_ordinal(attempt_id),
                max_execution_attempts=int(
                    retry_policy["max_execution_attempts"]
                ),
                transport_retry=0,
                max_transport_retries=int(
                    retry_policy["max_transport_retries"]
                ),
                stage_retry=max(0, stage_try - 1),
                max_idempotent_stage_retries=int(
                    retry_policy["max_idempotent_stage_retries"]
                ),
                stage_idempotent=True,
            )
            self.store.record_failure_and_retry(
                request_id,
                attempt_id,
                stage_name="workflow-ingest",
                stage_try=stage_try,
                failure=failure,
                decision=decision,
            )
            retry_at = ""
            if decision.action == "retry-stage":
                retry_at = (
                    datetime.now(timezone.utc) + timedelta(seconds=5)
                ).isoformat()
            return self.store.fail_workflow_ingest(
                request_id,
                attempt_id,
                consumer_id=self.component_id,
                claim_token=str(claim["claim_token"]),
                error=str(exc),
                retry_at=retry_at,
            )
        return self.store.complete_workflow_ingest(
            request_id,
            attempt_id,
            consumer_id=self.component_id,
            claim_token=str(claim["claim_token"]),
            outcome=outcome,
        )

    def _transport_failure(
        self,
        envelope: Mapping[str, Any],
        attempt: Mapping[str, Any],
        control: Mapping[str, Any],
        exc: FlowV3TransportError,
    ) -> dict[str, Any]:
        request_id = str(attempt["request_id"])
        attempt_id = str(attempt["attempt_id"])
        action = str(control["action"])
        if exc.pre_publish or exc.result_visibility == "known":
            self.store.record_transport_control(
                control_ref=str(control["control_ref"]),
                outbox_id=str(control["outbox_id"]),
                request_id=request_id,
                attempt_id=attempt_id,
                action=action,
                state="failed",
                observation={"error": str(exc)},
            )
        failure = FailureRecord(
            domain=exc.domain,
            code=exc.code,
            phase=action,
            detail=str(exc),
            retryable=exc.retryable,
            pre_publish=exc.pre_publish,
            result_visibility=exc.result_visibility,
        )
        retry_policy = envelope["retry_policy"]
        decision = decide_retry(
            failure,
            execution_attempt=execution_attempt_ordinal(attempt_id),
            max_execution_attempts=int(
                retry_policy["max_execution_attempts"]
            ),
            transport_retry=int(control["action_ordinal"]),
            max_transport_retries=int(
                retry_policy["max_transport_retries"]
            ),
            stage_retry=0,
            max_idempotent_stage_retries=int(
                retry_policy["max_idempotent_stage_retries"]
            ),
            stage_idempotent=False,
        )
        self.store.record_failure_and_retry(
            request_id,
            attempt_id,
            stage_name=f"transport-{action}",
            stage_try=int(control["action_ordinal"]),
            failure=failure,
            decision=decision,
        )
        if decision.action == "terminal":
            current = str(self.store.attempt(request_id, attempt_id)["state"])
            if current not in TERMINAL_STATES and current != "quarantined":
                self.store.transition(
                    request_id,
                    attempt_id,
                    expected_state=current,
                    current_state="quarantined",
                    event_type="transport-terminal-quarantine",
                    actor=self.component_id,
                    payload={
                        "failure_domain": failure.domain,
                        "failure_code": failure.code,
                        "transport_action": action,
                    },
                )
                current = "quarantined"
            if current == "quarantined":
                self.store.transition(
                    request_id,
                    attempt_id,
                    expected_state="quarantined",
                    current_state="terminal-infrastructure-failure",
                    event_type="transport-terminal",
                    actor=self.component_id,
                )
        return {
            "request_id": request_id,
            "attempt_id": attempt_id,
            "state": "transport-failed",
            "action": action,
            "retry_action": decision.action,
        }


def normalize_acceptance_observation(
    observation: Mapping[str, Any],
) -> dict[str, Any]:
    receipt = observation.get("receipt")
    if not isinstance(receipt, Mapping) or not receipt:
        raise FlowV3TransportError(
            "endpoint observation has no durable acceptance receipt",
            result_visibility="known",
            domain="protocol",
            code="acceptance-receipt-missing",
            retryable=False,
        )
    return {
        **dict(observation),
        "state": "accepted",
    }


def normalize_ack_observation(
    observation: Mapping[str, Any],
    envelope: Mapping[str, Any],
    *,
    receipt_id: str,
) -> dict[str, Any]:
    expected_meta = envelope["meta"]
    expected_digest = canonical_digest(envelope)
    receipt = observation.get("receipt")
    if (
        str(observation.get("state") or "") != "acknowledged"
        or str(observation.get("request_id") or "")
        != str(expected_meta["request_id"])
        or str(observation.get("attempt_id") or "")
        != str(expected_meta["attempt_id"])
        or str(observation.get("envelope_digest") or "") != expected_digest
        or not isinstance(receipt, Mapping)
        or str(receipt.get("return_receipt_id") or "") != receipt_id
    ):
        raise FlowV3TransportError(
            "endpoint acknowledgement identity or receipt mismatch",
            result_visibility="known",
            domain="protocol",
            code="acknowledgement-mismatch",
            retryable=False,
        )
    return dict(observation)


def adapt_endpoint_result(
    envelope: Mapping[str, Any],
    observation: Mapping[str, Any],
    return_root: Path,
    destination: Path,
) -> dict[str, Any]:
    result = observation.get("result")
    if not isinstance(result, Mapping):
        raise FlowV3DispatchError("endpoint result observation is missing")
    payload = result.get("result_payload")
    if not isinstance(payload, dict):
        raise FlowV3DispatchError("endpoint result payload manifest is missing")
    materialized = materialize_payload(
        return_root,
        payload,
        destination,
    )
    terminal = read_object(materialized / "terminal.json")
    state = read_object(materialized / "state.json")
    artifact_manifest = read_object(materialized / "artifact_manifest.json")
    engine_state = str(terminal.get("state") or result.get("state") or "")
    text_evidence = result_text(materialized / "result_bundle")
    operation_kind = str(envelope["workflow"]["operation_kind"])
    performance_mode = str(envelope["execution"]["performance_mode"])
    history = state.get("history", [])
    correctness_stage_failed = any(
        isinstance(item, Mapping)
        and str(item.get("stage_name") or "") == "correctness"
        and int(item.get("exit_code", 0) or 0) != 0
        for item in (history if isinstance(history, list) else [])
    )
    reported_failure_domain = str(
        terminal.get("failure_domain")
        or state.get("failure_domain")
        or ""
    )
    business_failure = reported_failure_domain == "business" or (
        "FAIL_CORRECTNESS" in text_evidence
        or "FAIL CORRECTNESS" in text_evidence
        or "correctness" in str(state.get("failure_stage") or "").lower()
        or correctness_stage_failed
    )
    if engine_state == "completed" and operation_kind == "diagnostic-profile":
        correctness_state = "not-run"
        performance_state = "completed"
        infrastructure_state = "ok"
        terminal_state = "terminal-success"
    elif engine_state == "completed":
        correctness_state = "pass"
        performance_state = (
            "skipped" if performance_mode == "none" else "completed"
        )
        infrastructure_state = "ok"
        terminal_state = "terminal-success"
    elif business_failure and operation_kind == "operator-test":
        correctness_state = "fail"
        performance_state = "skipped"
        infrastructure_state = "ok"
        terminal_state = "terminal-business-failure"
    else:
        correctness_state = (
            "not-run"
            if operation_kind == "diagnostic-profile"
            else "incomplete"
        )
        performance_state = (
            "failed"
            if operation_kind == "diagnostic-profile"
            else "not-run"
        )
        infrastructure_state = "failed"
        terminal_state = "terminal-infrastructure-failure"
    artifacts = artifact_records(
        materialized,
        artifact_manifest,
    )
    result_contract = envelope["result_contract"]
    required_by_terminal = result_contract.get(
        "required_artifacts_by_terminal_state", {}
    )
    required_artifacts = result_contract.get("required_artifacts", [])
    if isinstance(required_by_terminal, Mapping):
        required_artifacts = required_by_terminal.get(
            terminal_state,
            required_artifacts,
        )
    missing_required = missing_required_artifacts(
        required_artifacts,
        artifacts,
    )
    if missing_required:
        infrastructure_state = "failed"
        terminal_state = "terminal-infrastructure-failure"
    endpoint_stages, spans = endpoint_stage_evidence(
        envelope,
        history if isinstance(history, list) else [],
        business_failure=(terminal_state == "terminal-business-failure"),
    )
    return {
        "correctness_state": correctness_state,
        "performance_state": performance_state,
        "infrastructure_state": infrastructure_state,
        "terminal_state": terminal_state,
        "endpoint_state": str(result.get("state") or ""),
        "terminal": terminal,
        "engine_state": state,
        "artifacts": artifacts,
        "missing_required_artifacts": missing_required,
        "endpoint_stages": endpoint_stages,
        "spans": spans,
    }


def endpoint_stage_evidence(
    envelope: Mapping[str, Any],
    history: list[Any],
    *,
    business_failure: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    meta = envelope["meta"]
    stages: list[dict[str, Any]] = []
    spans: list[dict[str, Any]] = []
    for raw in history:
        if not isinstance(raw, Mapping):
            continue
        stage_name = str(raw.get("stage_name") or "")
        if not stage_name:
            continue
        stage_try = int(raw.get("stage_attempt", 1) or 1)
        exit_code = int(raw.get("exit_code", 0) or 0)
        stage_state = "succeeded"
        if exit_code != 0:
            stage_state = (
                "business-failed"
                if business_failure and stage_name == "correctness"
                else "infrastructure-failed"
            )
        stage = {
            "stage_name": stage_name,
            "stage_try": stage_try,
            "resource_class": str(raw.get("stage_resource") or ""),
            "state": stage_state,
            "started_at": str(raw.get("started_at") or ""),
            "finished_at": str(raw.get("finished_at") or ""),
            "exit_code": exit_code,
            "failure_id": "",
            "evidence": dict(raw),
        }
        stages.append(stage)
        start_ns = int(raw.get("started_monotonic_ns", 0) or 0)
        finish_ns = int(raw.get("finished_monotonic_ns", 0) or 0)
        host = str(raw.get("host") or "")
        boot_id = str(raw.get("boot_id") or "")
        pid = int(raw.get("pid", 0) or 0)
        if not host or not boot_id or pid <= 0 or start_ns <= 0 or finish_ns < start_ns:
            continue
        span_seed = {
            "request_id": str(meta["request_id"]),
            "attempt_id": str(meta["attempt_id"]),
            "stage_name": stage_name,
            "stage_try": stage_try,
            "host": host,
            "boot_id": boot_id,
            "pid": pid,
            "started_monotonic_ns": start_ns,
            "finished_monotonic_ns": finish_ns,
        }
        spans.append(
            {
                "clock_contract": "ascendop.clock.v3",
                "trace_id": str(meta["trace_id"]),
                "span_id": "remote-" + canonical_digest(span_seed)[:24],
                "parent_span_id": "",
                "stage_try": stage_try,
                "name": f"endpoint-stage:{stage_name}",
                "actor": "flow-v3-endpoint",
                "resource": str(raw.get("stage_resource") or ""),
                "host": host,
                "boot_id": boot_id,
                "pid": pid,
                "started_at": str(raw.get("started_at") or ""),
                "finished_at": str(raw.get("finished_at") or ""),
                "started_monotonic_ns": start_ns,
                "finished_monotonic_ns": finish_ns,
                "duration_ns": int(raw.get("duration_ns", finish_ns - start_ns) or 0),
                "evidence": dict(raw),
            }
        )
    return stages, spans


def artifact_records(
    materialized: Path,
    artifact_manifest: Mapping[str, Any],
) -> list[dict[str, Any]]:
    declared = {
        str(item.get("relative_path") or item.get("path") or ""): dict(item)
        for item in artifact_manifest.get("artifacts", [])
        if isinstance(item, Mapping)
    }
    records: list[dict[str, Any]] = []
    for path in sorted(
        (materialized / "result_bundle").rglob("*"),
        key=lambda item: item.as_posix(),
    ):
        if not path.is_file():
            continue
        relative = path.relative_to(materialized).as_posix()
        declared_item = declared.get(relative, declared.get(
            path.relative_to(materialized / "result_bundle").as_posix(),
            {},
        ))
        digest = file_sha256(path)
        expected = str(
            declared_item.get("sha256")
            or declared_item.get("digest")
            or ""
        )
        if expected and expected != digest:
            raise FlowV3DispatchError(
                f"result artifact digest mismatch: {relative}"
            )
        records.append(
            {
                "relative_path": relative,
                "digest": digest,
                "size_bytes": path.stat().st_size,
                "required": bool(declared_item.get("required", True)),
                "local_path": str(path.resolve()),
            }
        )
    return records


def missing_required_artifacts(
    required_artifacts: Any,
    artifacts: list[dict[str, Any]],
) -> list[str]:
    if not isinstance(required_artifacts, list):
        raise FlowV3DispatchError(
            "result contract required_artifacts must be a list"
        )
    available = {
        normalize_result_artifact_path(str(item["relative_path"]))
        for item in artifacts
    }
    missing: list[str] = []
    for item in required_artifacts:
        required = normalize_result_artifact_path(str(item))
        if required not in available:
            missing.append(str(item))
    return missing


def normalize_result_artifact_path(value: str) -> str:
    normalized = value.replace("\\", "/").lstrip("/")
    changed = True
    while changed:
        changed = False
        for prefix in ("result_bundle/", "result/"):
            if normalized.startswith(prefix):
                normalized = normalized[len(prefix) :]
                changed = True
                break
    parts = [part for part in normalized.split("/") if part not in {"", "."}]
    if not parts or any(part == ".." for part in parts):
        raise FlowV3DispatchError(
            f"invalid result artifact contract path: {value}"
        )
    return "/".join(parts)


def unique_return_path(root: Path, name: str) -> Path | None:
    if not root.is_dir():
        return None
    matches = [path for path in root.rglob(name) if path.is_file()]
    if not matches:
        return None
    documents: dict[str, Path] = {}
    for path in matches:
        try:
            document = read_object(path)
        except (OSError, UnicodeError, json.JSONDecodeError, FlowV3DispatchError):
            continue
        documents[canonical_digest(document)] = path
    if not documents:
        return None
    if len(documents) != 1:
        raise FlowV3TransportError(
            f"conflicting returned {name} documents",
            result_visibility="unknown",
        )
    return next(iter(documents.values()))


def unique_return_directory(root: Path, name: str) -> Path | None:
    if not root.is_dir():
        return None
    matches = [path for path in root.rglob(name) if path.is_dir()]
    if not matches:
        return None
    manifests: dict[str, Path] = {}
    for path in matches:
        manifest_path = path / "RESULT_PAYLOAD.json"
        if not manifest_path.is_file():
            continue
        try:
            manifest = read_object(manifest_path)
        except (OSError, UnicodeError, json.JSONDecodeError, FlowV3DispatchError):
            continue
        manifests[canonical_digest(manifest)] = path
    if not manifests:
        return None
    if len(manifests) != 1:
        raise FlowV3TransportError(
            f"conflicting or incomplete returned {name} directories",
            result_visibility="unknown",
        )
    return next(iter(manifests.values()))


def transport_control_ref(outbox_id: str, action: str, ordinal: int) -> str:
    material = f"{outbox_id}:{action}:{ordinal}".encode("utf-8")
    return f"flowv3-{action}-{hashlib.sha256(material).hexdigest()[:24]}"


def execution_attempt_ordinal(attempt_id: str) -> int:
    suffix = attempt_id.rsplit("-", 1)[-1]
    try:
        return max(1, int(suffix))
    except ValueError:
        return 1


def read_object(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(raw, dict):
        raise FlowV3DispatchError(f"JSON document is not an object: {path}")
    return raw


def result_text(root: Path) -> str:
    parts: list[str] = []
    if not root.is_dir():
        return ""
    for path in root.rglob("*"):
        if path.is_file() and path.suffix.lower() in {
            ".json",
            ".md",
            ".txt",
            ".log",
        }:
            try:
                parts.append(path.read_text(encoding="utf-8-sig", errors="replace"))
            except OSError:
                continue
    return "\n".join(parts)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            data = handle.read(1024 * 1024)
            if not data:
                break
            digest.update(data)
    return digest.hexdigest()
