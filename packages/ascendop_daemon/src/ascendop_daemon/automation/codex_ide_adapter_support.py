from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from ascendop_protocol.actor import build_v5_prompt_context, render_v5_action_contract
from ascendop_protocol.agent import (
    AGENT_REGISTRATION_SCHEMA,
    AGENT_TURN_DELIVERY_SCHEMA,
    AGENT_TURN_DELIVERY_V2_SCHEMA,
    agent_turn_delivery_identity,
    render_agent_output_authoring_contract,
    validate_agent_turn_delivery,
)

from ascendop_daemon.automation.codex_ide_settings import (
    ADAPTER_ID,
    TOKEN_CHARS,
    CodexIdeAdapterError,
    identity_digest,
)


class CodexIdeAdapterSupportMixin:
    def recover_legacy_evidence_validation(
        self,
        *,
        action_id: str,
        verification: str,
    ) -> dict[str, Any]:
        """Reclassify one proven legacy zero-byte evidence false failure."""

        if verification != "zero-byte-evidence-blobs-match":
            raise CodexIdeAdapterError(
                "legacy evidence recovery requires zero-byte-evidence-blobs-match"
            )
        action = self.database.agent_action(action_id)
        if action is None:
            raise CodexIdeAdapterError(f"Agent action does not exist: {action_id}")
        proof = self.workspace.audit_legacy_zero_byte_evidence(action_id)
        recovered = self.database.reclassify_legacy_agent_evidence_validation(
            action_id=action_id,
            attempt_id=str(action["current_attempt_id"]),
            proof=proof,
        )
        return {
            "schema": "ascendop.codex-ide-evidence-recovery.v1",
            "state": "reclassified",
            "action_id": action_id,
            "attempt_id": str(action["current_attempt_id"]),
            "failure_class": "agent-output-validation",
            "proof": proof,
            "action_state": str(recovered["state"]),
        }

    def _registration(self, target: Mapping[str, str]) -> dict[str, Any]:
        return {
            "schema": AGENT_REGISTRATION_SCHEMA,
            "agent_id": target["agent_id"],
            "driver": "codex-ide-task",
            "executable": f"codex-ide-task://{target['task_id']}",
            "executable_digest": self.source_digest,
            "observed_version": self.settings.adapter_generation,
            "registration_generation": identity_digest(
                {
                    "adapter_generation": self.settings.adapter_generation,
                    "operator_id": target["operator_id"],
                    "role": target["role"],
                    "task_id": target["task_id"],
                }
            ),
            "capabilities": {
                "stream_json": False,
                "resume": True,
                "structured_output": True,
                "task_visibility": True,
                "isolated_workspace": True,
            },
            "target_kind": "codex-ide-task",
            "target_id": target["task_id"],
            "operator_id": target["operator_id"],
            "role": target["role"],
            "observed_at": _utc_now(),
        }

    def _prepare_delivery(
        self,
        claimed: Mapping[str, Any],
        *,
        consumer_id: str,
    ) -> dict[str, Any]:
        action = dict(claimed["action"])
        context = dict(claimed["context"])
        agent = dict(claimed["agent"])
        existing = self._existing_delivery(claimed)
        if existing is not None:
            return existing
        workflow_evidence = context.get("workflow_evidence", [])
        if not isinstance(workflow_evidence, list) or not all(
            isinstance(item, Mapping) for item in workflow_evidence
        ):
            raise CodexIdeAdapterError(
                "Agent workflow evidence must be a list of objects"
            )
        reference_evidence = context.get("reference_evidence", [])
        if not isinstance(reference_evidence, list) or not all(
            isinstance(item, Mapping) for item in reference_evidence
        ):
            raise CodexIdeAdapterError(
                "Agent reference evidence must be a list of objects"
            )
        _run_root, workspace, _before = self.workspace.stage(
            action,
            [*workflow_evidence, *reference_evidence],
        )
        self.outputs.stage(action, workspace)
        target = self._target_for_agent(str(agent["agent_id"]))
        action_context = build_v5_prompt_context(
            action,
            context,
            dict(claimed["attempt_context"]),
        )
        prompt = self._prompt(
            action,
            context,
            workspace,
            str(claimed["attempt_id"]),
            action_context=action_context,
        )
        attempt_id = str(claimed["attempt_id"])
        delivery = validate_agent_turn_delivery(
            {
                "schema": AGENT_TURN_DELIVERY_V2_SCHEMA,
                "delivery_id": f"atd-{attempt_id}",
                "delivery_key": f"{action['action_id']}:{attempt_id}",
                "adapter_id": ADAPTER_ID,
                "adapter_generation": self.settings.adapter_generation,
                "target_kind": "codex-ide-task",
                "target_id": target["task_id"],
                "workspace": workspace.relative_to(self.root).as_posix(),
                "prompt": prompt,
                "prompt_digest": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
                "action": action,
                "context": context,
                "action_context": action_context,
                "lease": dict(claimed["lease"]),
                "agent": {
                    "agent_id": agent["agent_id"],
                    "driver": agent["driver"],
                },
                "created_at": _utc_now(),
            }
        )
        self._write_json(
            self.workspace.run_root(str(action["action_id"])) / "delivery.json",
            delivery,
        )
        return delivery

    def _existing_delivery(
        self,
        claimed: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        action = dict(claimed["action"])
        action_id = str(action["action_id"])
        attempt_id = str(claimed["attempt_id"])
        path = self.workspace.run_root(action_id) / "delivery.json"
        if not path.is_file():
            return None
        delivery = self._read_delivery(action_id)
        if str(delivery["delivery_id"]) != f"atd-{attempt_id}":
            return None
        if str(delivery["delivery_key"]) != f"{action_id}:{attempt_id}":
            raise CodexIdeAdapterError("Agent delivery identity is inconsistent")
        if dict(delivery["action"]) != action:
            raise CodexIdeAdapterError("Agent delivery action changed after publication")
        if dict(delivery["context"]) != dict(claimed["context"]):
            raise CodexIdeAdapterError("Agent delivery context changed after publication")
        attempt_context = claimed.get("attempt_context")
        if isinstance(attempt_context, Mapping):
            delivery_schema = str(delivery.get("schema") or "")
            if delivery_schema == AGENT_TURN_DELIVERY_V2_SCHEMA:
                expected_action_context = build_v5_prompt_context(
                    action,
                    dict(claimed["context"]),
                    dict(attempt_context),
                )
                if dict(delivery["action_context"]) != expected_action_context:
                    raise CodexIdeAdapterError(
                        "Agent delivery action context changed after publication"
                    )
            elif delivery_schema != AGENT_TURN_DELIVERY_SCHEMA:
                raise CodexIdeAdapterError(
                    "Agent delivery schema changed after publication"
                )
        if str(delivery["agent"]["agent_id"]) != str(
            claimed["agent"]["agent_id"]
        ):
            raise CodexIdeAdapterError("Agent delivery executor identity changed")
        target = self._target_for_agent(str(claimed["agent"]["agent_id"]))
        if str(delivery["target_id"]) != target["task_id"]:
            raise CodexIdeAdapterError("Agent delivery target changed after publication")
        lease = dict(delivery["lease"])
        current_lease = dict(claimed["lease"])
        for field in (
            "lease_id",
            "lease_token",
            "action_id",
            "iteration_id",
            "operator_id",
            "role",
            "agent_id",
        ):
            if str(lease.get(field) or "") != str(current_lease.get(field) or ""):
                raise CodexIdeAdapterError(
                    f"Agent delivery lease identity changed: {field}"
                )
        expected_workspace = (
            self.workspace.run_root(action_id) / "workspace"
        ).relative_to(self.root).as_posix()
        if str(delivery["workspace"]) != expected_workspace:
            raise CodexIdeAdapterError("Agent delivery workspace changed after publication")
        return delivery

    def _prompt(
        self,
        action: Mapping[str, Any],
        context: Mapping[str, Any],
        workspace: Path,
        attempt_id: str,
        *,
        action_context: Mapping[str, Any],
    ) -> str:
        runbook = (self.root / str(action["runbook_path"])).resolve()
        if self.root not in runbook.parents or not runbook.is_file():
            raise CodexIdeAdapterError("Agent action runbook is missing or unbounded")
        if hashlib.sha256(runbook.read_bytes()).hexdigest() != action["runbook_digest"]:
            raise CodexIdeAdapterError("Agent action runbook digest mismatch")
        marker = agent_turn_delivery_identity(
            {
                "action": action,
                "delivery_key": f"{action['action_id']}:{attempt_id}",
            }
        )["delivery_marker"]
        output_authoring = render_agent_output_authoring_contract(
            action.get("output_contracts", []),
            candidate_version=str(action.get("candidate_version") or ""),
        )
        v5_action_contract = render_v5_action_contract(action_context)
        return (
            marker
            + "\n\n"
            + runbook.read_text(encoding="utf-8")
            + "\n\nFLOW V5 ACTION CONTRACT (generated):\n"
            + v5_action_contract
            + "\n\nLEGACY AGENT ACTION PAYLOAD (immutable):\n"
            + json.dumps(action, ensure_ascii=True, indent=2, sort_keys=True)
            + "\n\nHANDOFF CONTEXT (immutable):\n"
            + json.dumps(context, ensure_ascii=True, indent=2, sort_keys=True)
            + "\n\n"
            + output_authoring
            + "\n\nWork only inside this isolated workspace: "
            + str(workspace)
            + "\nImmutable workflow and reference artifacts, when supplied, are indexed "
            "by .ascendop-evidence/MANIFEST.json. Inspect or skip them according "
            "to the registered skills and current evidence."
            + "\nDo not mutate the canonical workspace, queue/result archives, control DB, "
            "endpoint state, or official website. Finish with a concise result summary; "
            "the app-side adapter records the typed receipt. Any daemon-authorized "
            "non-source output must be written only to the exact slot declared in "
            ".ascendop-output/CONTRACT.json.\n"
        )

    def _target_for_agent(self, agent_id: str) -> dict[str, str]:
        for target in self.targets():
            if target["agent_id"] == agent_id:
                return target
        raise CodexIdeAdapterError(f"Codex IDE target is not configured: {agent_id}")

    def _managed_agent_ids_for_consumer(self, consumer_id: str) -> set[str]:
        targets = self.targets()
        if not targets:
            return set()
        if len(targets) == 1:
            return {targets[0]["agent_id"]}
        base = self.settings.manager_runner_id
        if consumer_id == base:
            index = 0
        elif consumer_id.startswith(base + "-"):
            suffix = consumer_id[len(base) + 1 :]
            if not suffix.isdigit():
                raise CodexIdeAdapterError(
                    "Codex IDE consumer does not identify a configured target slot"
                )
            index = int(suffix)
        else:
            raise CodexIdeAdapterError(
                "Codex IDE consumer does not identify a configured target slot"
            )
        if index >= len(targets):
            raise CodexIdeAdapterError(
                "Codex IDE consumer target slot is outside delivery capacity"
            )
        return {targets[index]["agent_id"]}

    def _service_heartbeat(self, consumer_id: str) -> None:
        self.database.record_runtime_service_heartbeat(
            service_id="ascendop-codex-ide-task-adapter",
            role="agent-execution",
            code_generation=self.code_generation,
            capabilities=[
                "agent-pool-routing",
                "agent-work-lease",
                "isolated-workspace",
                "session-resume",
                "app-side-task-delivery",
            ],
            state="ready",
            boot_id=self.boot_id,
            lease_seconds=self.settings.lease_seconds,
            details={
                "adapter_id": ADAPTER_ID,
                "consumer_id": consumer_id,
                "runner_id": self.settings.manager_runner_id,
                "runner_generation": self.settings.adapter_generation,
                "execution_contract_digest": self.execution_contract_digest,
            },
        )

    def _write_adapter_state(
        self,
        claimed: Mapping[str, Any],
        delivery: Mapping[str, Any],
        *,
        consumer_id: str,
        phase: str,
    ) -> None:
        action = claimed["action"]
        lease = claimed["lease"]
        state = {
            "schema": "ascendop.codex-ide-adapter-state.v1",
            "action_id": str(action["action_id"]),
            "attempt_id": str(claimed["attempt_id"]),
            "agent_id": str(claimed["agent"]["agent_id"]),
            "lease_id": str(lease["lease_id"]),
            "lease_token": str(lease["lease_token"]),
            "target_id": str(delivery["target_id"]),
            "consumer_id": consumer_id,
            "phase": phase,
            "turn_id": str(claimed.get("session_id") or ""),
            "claimed_at": str(lease["acquired_at"]),
            "updated_at": _utc_now(),
        }
        self._write_state(str(action["action_id"]), state)

    def _active_state(self, consumer_id: str) -> dict[str, Any] | None:
        candidates: list[dict[str, Any]] = []
        if not self.workspace.runs_root.is_dir():
            return None
        for path in self.workspace.runs_root.glob("*/adapter-state.json"):
            try:
                state = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(state, dict) or state.get("consumer_id") != consumer_id:
                continue
            action = self.database.agent_action(str(state.get("action_id") or ""))
            if (
                action is not None
                and action["state"] in {"claimed", "running"}
                and str(state.get("attempt_id") or "")
                == str(action.get("current_attempt_id") or "")
                and str(state.get("lease_id") or "")
                == str(action.get("current_lease_id") or "")
            ):
                candidates.append(state)
        if len(candidates) > 1:
            raise CodexIdeAdapterError("consumer owns multiple active Agent deliveries")
        return candidates[0] if candidates else None

    def _require_state(self, action_id: str, consumer_id: str) -> dict[str, Any]:
        self._require_consumer(consumer_id)
        path = self.workspace.run_root(action_id) / "adapter-state.json"
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CodexIdeAdapterError(
                f"Codex IDE adapter state is unavailable: {action_id}"
            ) from exc
        if not isinstance(value, dict) or value.get("consumer_id") != consumer_id:
            raise CodexIdeAdapterError("Codex IDE adapter consumer identity mismatch")
        action = self.database.agent_action(action_id)
        if action is None:
            raise CodexIdeAdapterError(f"Agent action does not exist: {action_id}")
        if (
            str(value.get("attempt_id") or "")
            != str(action.get("current_attempt_id") or "")
            or str(value.get("lease_id") or "")
            != str(action.get("current_lease_id") or "")
        ):
            raise CodexIdeAdapterError(
                "Codex IDE adapter attempt or lease identity is stale"
            )
        return value

    def _read_delivery(self, action_id: str) -> dict[str, Any]:
        path = self.workspace.run_root(action_id) / "delivery.json"
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CodexIdeAdapterError(
                f"Agent delivery is unavailable: {action_id}"
            ) from exc
        if not isinstance(value, dict):
            raise CodexIdeAdapterError("Agent delivery must be a JSON object")
        return validate_agent_turn_delivery(value)

    def _write_state(self, action_id: str, state: Mapping[str, Any]) -> None:
        path = self.workspace.run_root(action_id) / "adapter-state.json"
        self._write_json(path, state)
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass

    @staticmethod
    def _write_json(path: Path, value: Mapping[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, delete=False
        ) as handle:
            handle.write(payload)
            temporary = Path(handle.name)
        os.replace(temporary, path)

    @staticmethod
    def _require_consumer(consumer_id: str) -> None:
        if not consumer_id or any(ch not in TOKEN_CHARS for ch in consumer_id):
            raise CodexIdeAdapterError("consumer_id must be a safe token")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _promotion_action_id(seal_path: Path) -> str:
    try:
        value = json.loads(seal_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CodexIdeAdapterError(f"invalid Agent promotion seal: {seal_path}") from exc
    action_id = str(value.get("action_id") or "") if isinstance(value, dict) else ""
    if not action_id or any(char not in TOKEN_CHARS for char in action_id):
        raise CodexIdeAdapterError("Agent promotion seal has an invalid action_id")
    return action_id
