from __future__ import annotations

import hashlib
import json
import os
import socket
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from ascendop_protocol.agent import (
    AGENT_ACTION_RECEIPT_SCHEMA,
    AGENT_REGISTRATION_SCHEMA,
    render_agent_output_authoring_contract,
)
from ascendop_control import ControlStore

from .drivers import AgentDriver, default_drivers
from .provider import AgentProviderProfile
from .workspace import WorkspaceStager


class AgentRunner:
    def __init__(
        self,
        *,
        root: Path,
        database: Path,
        drivers: Iterable[AgentDriver] | None = None,
        runner_id: str | None = None,
        lease_seconds: int = 30,
        code_generation: str = "test-generation",
        runner_generation: str | None = None,
        config: Path | None = None,
        provider_profile: AgentProviderProfile | None = None,
    ) -> None:
        self.root = root.resolve()
        self.store = ControlStore(database)
        self.store.assert_compatible()
        if config is not None and provider_profile is not None:
            raise ValueError("Pass either config or provider_profile, not both")
        self.provider_profile = provider_profile or (
            AgentProviderProfile.load(root=self.root, config=config.resolve())
            if config is not None
            else None
        )
        configured_drivers = drivers or default_drivers(self.provider_profile)
        self.drivers = {driver.driver_id: driver for driver in configured_drivers}
        self.runner_id = runner_id or f"agent-runner:{socket.gethostname()}:{os.getpid()}"
        self.boot_id = _boot_id()
        self.code_generation = str(code_generation).strip()
        if not self.code_generation:
            raise ValueError("Agent runner code generation is required")
        self.runner_generation = str(
            runner_generation or self.code_generation
        ).strip()
        if not self.runner_generation:
            raise ValueError("Agent runner package generation is required")
        self.service_id = f"ascendop-agent-runner:{socket.gethostname()}"
        self.stager = WorkspaceStager(self.root)
        self.lease_seconds = int(lease_seconds)
        if not 15 <= self.lease_seconds <= 120:
            raise ValueError("Agent lease seconds must be in [15, 120]")
        self._registered_agent_ids: set[str] = set()
        self._quarantined_agent_ids: set[str] = set()
        self._last_registration_heartbeat = 0.0

    def probe_and_register(self) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        now = _utc_now()
        for driver in self.drivers.values():
            probe = driver.probe()
            if not probe.available:
                results.append({"driver": probe.driver, "registered": False, "error": probe.error})
                continue
            agent_id = f"{probe.driver}:{socket.gethostname()}"
            health_state = "ready"
            try:
                existing = self.store.agent_registration(agent_id)
            except Exception:
                existing = None
            if (
                existing is not None
                and existing["registration_generation"]
                == self._registration_generation(probe)
                and existing["health_state"] == "degraded"
            ):
                health_state = "degraded"
                self._quarantined_agent_ids.add(agent_id)
            capabilities = dict(probe.capabilities)
            if self.provider_profile is not None:
                capabilities["provider_profile_bound"] = True
            registration = {
                "schema": AGENT_REGISTRATION_SCHEMA,
                "agent_id": agent_id,
                "driver": probe.driver,
                "executable": probe.executable,
                "executable_digest": probe.executable_digest,
                "observed_version": probe.version,
                "registration_generation": self._registration_generation(probe),
                "capabilities": capabilities,
                "observed_at": now,
            }
            if self.provider_profile is not None:
                registration["provider_binding"] = (
                    self.provider_profile.public_binding()
                )
            value = self.store.register_agent(
                registration,
                health_state=health_state,
                boot_id=self.boot_id,
                manager_runner_id=self.runner_id,
                lease_seconds=self.lease_seconds,
            )
            self._registered_agent_ids.add(agent_id)
            results.append({"driver": probe.driver, "registered": True, "agent": value})
        return results

    def maintain_registrations(self, *, force: bool = False) -> list[dict[str, Any]]:
        now = time.monotonic()
        interval = max(5.0, self.lease_seconds / 3.0)
        if not force and now - self._last_registration_heartbeat < interval:
            return []
        if not self._registered_agent_ids:
            result = self.probe_and_register()
        else:
            result = []
            for agent_id in sorted(self._registered_agent_ids):
                value = self.store.heartbeat_agent(
                    agent_id,
                    boot_id=self.boot_id,
                    health_state=(
                        "degraded"
                        if agent_id in self._quarantined_agent_ids
                        else "ready"
                    ),
                    lease_seconds=self.lease_seconds,
                )
                result.append({"agent_id": agent_id, "heartbeat": value})
        self.store.record_runtime_service_heartbeat(
            service_id=self.service_id,
            role="agent-execution",
            code_generation=self.code_generation,
            capabilities=[
                "agent-pool-routing",
                "agent-work-lease",
                "isolated-workspace",
                "session-resume",
            ],
            state="ready",
            boot_id=self.boot_id,
            lease_seconds=self.lease_seconds,
            details={
                "pid": os.getpid(),
                "runner_id": self.runner_id,
                "provider_binding": (
                    self.provider_profile.public_binding()
                    if self.provider_profile is not None
                    else {}
                ),
                "runner_generation": self.runner_generation,
                "execution_contract_digest": self.execution_contract_digest,
            },
        )
        self._last_registration_heartbeat = now
        return result

    def _registration_generation(self, probe: Any) -> str:
        if self.provider_profile is None:
            return str(probe.executable_digest)
        return self.provider_profile.registration_generation(
            driver=str(probe.driver),
            executable_digest=str(probe.executable_digest),
        )

    @property
    def execution_contract_digest(self) -> str:
        provider_digest = (
            self.provider_profile.contract_digest
            if self.provider_profile is not None
            else "unbound-provider"
        )
        payload = {
            "schema": "ascendop.agent-execution-contract.v1",
            "runner_generation": self.runner_generation,
            "provider_contract_digest": provider_digest,
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
        ).hexdigest()

    def run_once(self, *, allow_claims: bool = True) -> dict[str, Any]:
        registration = self.maintain_registrations()
        if not allow_claims:
            return {
                "state": "stopped",
                "runner_id": self.runner_id,
                "registration": registration,
            }
        uncertain = self.store.adopt_uncertain_agent_action(
            runner_id=self.runner_id,
            boot_id=self.boot_id,
            managed_agent_ids=set(self._registered_agent_ids),
            lease_seconds=self.lease_seconds,
        )
        if uncertain is not None:
            return self._resume_uncertain(uncertain)
        claimed = self.store.claim_agent_action(
            runner_id=self.runner_id,
            boot_id=self.boot_id,
            lease_seconds=self.lease_seconds,
        )
        if claimed is None:
            return {
                "state": "idle",
                "runner_id": self.runner_id,
                "registration": registration,
            }
        action = claimed["action"]
        lease = claimed["lease"]
        agent = claimed["agent"]
        attempt_run_root = self.stager.attempt_run_root(
            str(action["action_id"]), str(claimed["attempt_id"])
        )
        driver = self.drivers.get(str(agent["driver"]))
        if driver is None:
            return self._settle_exception(
                claimed,
                phase="preflight",
                error=RuntimeError(
                    f"claimed action has unavailable driver: {agent['driver']}"
                ),
                status="failed",
                run_root=attempt_run_root,
            )
        session_id = f"pending:{uuid.uuid4().hex}"
        started_at = _utc_now()
        started = self.store.start_agent_action(
            action_id=str(action["action_id"]),
            lease_token=str(lease["lease_token"]),
            session_id=session_id,
        )
        if started["state"] == "cancelled":
            return {
                "state": "cancelled",
                "runner_id": self.runner_id,
                "driver": driver.driver_id,
                "action_id": action["action_id"],
                "iteration_id": action["iteration_id"],
                "reason": "workflow-gate-no-longer-current",
            }
        try:
            _, workspace = self.stager.stage(action)
            before = self.stager.snapshot(workspace)
            prompt = self._prompt(action, claimed["context"], workspace)
        except Exception as exc:
            return self._settle_exception(
                claimed,
                phase="preflight",
                error=exc,
                status="failed",
                run_root=attempt_run_root,
            )

        def heartbeat() -> None:
            self.store.heartbeat_agent_action(
                action_id=str(action["action_id"]),
                lease_token=str(lease["lease_token"]),
                lease_seconds=self.lease_seconds,
            )
            self.store.heartbeat_agent(
                str(agent["agent_id"]),
                boot_id=self.boot_id,
                lease_seconds=self.lease_seconds,
            )

        timeout_seconds = int(action["tool_budget"]["max_turn_seconds"])
        try:
            result = driver.start(
                prompt=prompt,
                workspace=workspace,
                run_root=attempt_run_root,
                timeout_seconds=timeout_seconds,
                heartbeat=heartbeat,
            )
            completion = driver.collect(result)
        except Exception as exc:
            return self._settle_exception(
                claimed,
                phase="execution",
                error=exc,
                status="uncertain",
                started_at=started_at,
                run_root=attempt_run_root,
                workspace=workspace,
                session_id=session_id,
            )
        return self._record_result(
            claimed,
            driver=driver,
            result=result,
            completion=completion,
            started_at=started_at,
            workspace=workspace,
            before=before,
        )

    def _resume_uncertain(self, claimed: dict[str, Any]) -> dict[str, Any]:
        action = claimed["action"]
        lease = claimed["lease"]
        agent = claimed["agent"]
        session_id = str(claimed.get("session_id") or "")
        if not session_id or session_id.startswith("pending:"):
            self.store.heartbeat_agent_action(
                action_id=str(action["action_id"]),
                lease_token=str(lease["lease_token"]),
                lease_seconds=self.lease_seconds,
            )
            return {
                "state": "uncertain",
                "runner_id": self.runner_id,
                "action_id": action["action_id"],
                "iteration_id": action["iteration_id"],
                "reconciliation": "manual-session-identity-required",
            }
        driver = self.drivers.get(str(agent["driver"]))
        if driver is None:
            return self._settle_exception(
                claimed,
                phase="reconcile",
                error=RuntimeError("assigned Agent driver is unavailable"),
                status="uncertain",
                session_id=session_id,
            )
        _, workspace = self.stager.stage(action)
        attempt_run_root = self.stager.attempt_run_root(
            str(action["action_id"]), str(claimed["attempt_id"])
        )
        origin = (self.root / str(action["origin_workspace"])).resolve()
        if self.root not in origin.parents or not origin.is_dir():
            return self._settle_exception(
                claimed,
                phase="reconcile",
                error=RuntimeError("Agent origin workspace is missing or unbounded"),
                status="uncertain",
                run_root=attempt_run_root,
                workspace=workspace,
                session_id=session_id,
            )
        before = self.stager.snapshot(origin)
        started_at = _utc_now()
        prompt = self._prompt(action, claimed["context"], workspace)

        def heartbeat() -> None:
            self.store.heartbeat_agent_action(
                action_id=str(action["action_id"]),
                lease_token=str(lease["lease_token"]),
                lease_seconds=self.lease_seconds,
            )
            self.store.heartbeat_agent(
                str(agent["agent_id"]),
                boot_id=self.boot_id,
                lease_seconds=self.lease_seconds,
            )

        try:
            result = driver.resume(
                session_id=session_id,
                prompt=prompt,
                workspace=workspace,
                run_root=attempt_run_root / "reconcile",
                timeout_seconds=int(action["tool_budget"]["max_turn_seconds"]),
                heartbeat=heartbeat,
            )
            if result.session_id and result.session_id != session_id:
                raise RuntimeError(
                    "Agent resume returned a different session identity: "
                    f"{result.session_id}"
                )
            completion = driver.collect(result)
        except Exception as exc:
            return self._settle_exception(
                claimed,
                phase="reconcile",
                error=exc,
                status="uncertain",
                started_at=started_at,
                run_root=attempt_run_root,
                workspace=workspace,
                session_id=session_id,
            )
        return self._record_result(
            claimed,
            driver=driver,
            result=result,
            completion=completion,
            started_at=started_at,
            workspace=workspace,
            before=before,
        )

    def _record_result(
        self,
        claimed: dict[str, Any],
        *,
        driver: AgentDriver,
        result: Any,
        completion: dict[str, Any],
        started_at: str,
        workspace: Path,
        before: dict[str, str],
    ) -> dict[str, Any]:
        action = claimed["action"]
        lease = claimed["lease"]
        agent = claimed["agent"]
        after = self.stager.snapshot(workspace)
        changed_paths = self.stager.changed_paths(before, after)
        out_of_scope = self.stager.out_of_scope_paths(
            changed_paths,
            list(action["write_scope"]),
        )
        completion["changed_paths"] = changed_paths
        completion["out_of_scope_paths"] = out_of_scope
        completion["source_after_digest"] = self.stager.digest(workspace)
        completion["session_id"] = result.session_id
        status = result.status
        if out_of_scope:
            status = "failed"
            completion["status"] = "failed"
            completion["failure_class"] = "write-scope-violation"
        artifacts = [
            result.raw_output_path.relative_to(self.root).as_posix(),
            result.stderr_path.relative_to(self.root).as_posix(),
        ]
        completion["artifacts"] = sorted(
            set([*completion.get("artifacts", []), *artifacts])
        )
        completion["runner_generation"] = self.runner_generation
        completion["agent_execution_contract_digest"] = (
            self.execution_contract_digest
        )
        completion["started_at"] = started_at
        if status == "failed" and completion.get("failure_class") == "agent-auth":
            self.store.quarantine_agent(
                str(agent["agent_id"]),
                registration_generation=str(agent["registration_generation"]),
                reason="runtime-authentication-failure",
                evidence={
                    "action_id": str(action["action_id"]),
                    "attempt_id": str(claimed["attempt_id"]),
                    "failure_class": "agent-auth",
                    "error_status": completion.get("error_status"),
                },
                boot_id=self.boot_id,
                lease_seconds=self.lease_seconds,
            )
            self._quarantined_agent_ids.add(str(agent["agent_id"]))
        if (
            status == "failed"
            and completion.get("failure_class") in {"agent-adapter", "agent-auth"}
            and not changed_paths
            and not out_of_scope
        ):
            pending = self.store.defer_agent_action_retry(
                action_id=str(action["action_id"]),
                lease_token=str(lease["lease_token"]),
                failure=completion,
                lease_seconds=self.lease_seconds,
            )
            return {
                "state": "retry-pending",
                "runner_id": self.runner_id,
                "driver": driver.driver_id,
                "action_id": action["action_id"],
                "iteration_id": action["iteration_id"],
                "workspace": workspace.relative_to(self.root).as_posix(),
                "retry_pending": pending,
                "error": completion,
            }
        receipt = {
            "schema": AGENT_ACTION_RECEIPT_SCHEMA,
            "action_id": action["action_id"],
            "iteration_id": action["iteration_id"],
            "agent_id": agent["agent_id"],
            "lease_id": lease["lease_id"],
            "status": status,
            "started_at": started_at,
            "completed_at": _utc_now(),
            "completion": completion,
            "artifacts": artifacts,
        }
        terminal = self.store.complete_agent_action(
            receipt,
            lease_token=str(lease["lease_token"]),
        )
        return {
            "state": status,
            "runner_id": self.runner_id,
            "driver": driver.driver_id,
            "action_id": action["action_id"],
            "iteration_id": action["iteration_id"],
            "workspace": workspace.relative_to(self.root).as_posix(),
            "terminal": terminal,
        }

    def _settle_exception(
        self,
        claimed: dict[str, Any],
        *,
        phase: str,
        error: Exception,
        status: str,
        started_at: str | None = None,
        run_root: Path | None = None,
        workspace: Path | None = None,
        session_id: str = "",
    ) -> dict[str, Any]:
        action = claimed["action"]
        lease = claimed["lease"]
        agent = claimed["agent"]
        evidence_root = run_root or self.stager.attempt_run_root(
            str(action["action_id"]), str(claimed["attempt_id"])
        )
        evidence_root.mkdir(parents=True, exist_ok=True)
        evidence_path = evidence_root / "runner_error.json"
        completion = {
            "status": status,
            "failure_class": (
                "agent-preflight" if phase == "preflight" else "agent-execution-uncertain"
            ),
            "phase": phase,
            "error_type": type(error).__name__,
            "error": str(error),
            "session_id": session_id,
            "runner_generation": self.runner_generation,
            "agent_execution_contract_digest": self.execution_contract_digest,
        }
        if workspace is not None and workspace.is_dir():
            completion["source_after_digest"] = self.stager.digest(workspace)
        evidence_path.write_text(
            json.dumps(completion, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        completion["artifacts"] = [evidence_path.relative_to(self.root).as_posix()]
        if phase == "preflight":
            pending = self.store.defer_agent_action_retry(
                action_id=str(action["action_id"]),
                lease_token=str(lease["lease_token"]),
                failure=completion,
                lease_seconds=self.lease_seconds,
            )
            return {
                "state": "retry-pending",
                "runner_id": self.runner_id,
                "driver": str(agent["driver"]),
                "action_id": action["action_id"],
                "iteration_id": action["iteration_id"],
                "workspace": (
                    workspace.relative_to(self.root).as_posix()
                    if workspace is not None
                    else ""
                ),
                "retry_pending": pending,
                "error": completion,
            }
        receipt = {
            "schema": AGENT_ACTION_RECEIPT_SCHEMA,
            "action_id": action["action_id"],
            "iteration_id": action["iteration_id"],
            "agent_id": agent["agent_id"],
            "lease_id": lease["lease_id"],
            "status": status,
            "started_at": started_at or _utc_now(),
            "completed_at": _utc_now(),
            "completion": completion,
            "artifacts": completion["artifacts"],
        }
        terminal = self.store.complete_agent_action(
            receipt,
            lease_token=str(lease["lease_token"]),
        )
        return {
            "state": status,
            "runner_id": self.runner_id,
            "driver": str(agent["driver"]),
            "action_id": action["action_id"],
            "iteration_id": action["iteration_id"],
            "workspace": (
                workspace.relative_to(self.root).as_posix()
                if workspace is not None
                else ""
            ),
            "terminal": terminal,
            "error": completion,
        }

    def _prompt(
        self,
        action: dict[str, Any],
        context: dict[str, Any],
        workspace: Path,
    ) -> str:
        runbook = (self.root / str(action["runbook_path"])).resolve()
        if self.root not in runbook.parents or not runbook.is_file():
            raise RuntimeError("agent action runbook is missing or unbounded")
        digest = hashlib.sha256(runbook.read_bytes()).hexdigest()
        if digest != action["runbook_digest"]:
            raise RuntimeError("agent action runbook digest mismatch")
        output_authoring = render_agent_output_authoring_contract(
            action.get("output_contracts", [])
        )
        return (
            runbook.read_text(encoding="utf-8")
            + "\n\nFLOW V4 ACTION (immutable):\n"
            + json.dumps(action, ensure_ascii=True, indent=2, sort_keys=True)
            + "\n\nHANDOFF CONTEXT (immutable):\n"
            + json.dumps(context, ensure_ascii=True, indent=2, sort_keys=True)
            + "\n\n"
            + output_authoring
            + "\n\nWork only inside this isolated workspace: "
            + str(workspace)
            + "\nReturn one JSON object with status, summary, artifacts, and optional "
            "source_after_digest. Do not submit tests, alter queues/results, contact "
            "endpoints, or perform official browser operations.\n"
        )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _boot_id() -> str:
    value = f"{socket.gethostname()}:{os.getpid()}:{os.stat(__file__).st_ctime_ns}"
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:24]
