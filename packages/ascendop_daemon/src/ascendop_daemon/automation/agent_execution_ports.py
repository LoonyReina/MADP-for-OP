from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence

from ascendop_protocol.actor import (
    AgentPortError,
    AgentTurnPending,
    NativeSessionObservation,
    NativeTurnBinding,
    native_turn_outcome,
)


_TERMINAL_STATUSES = {"completed", "failed", "cancelled", "interrupted"}


class DesktopTaskClient(Protocol):
    def read_task(self, task_id: str) -> Mapping[str, Any]: ...

    def send_message(self, task_id: str, prompt: str) -> Mapping[str, Any]: ...

    def cancel_turn(self, task_id: str, turn_id: str) -> Mapping[str, Any]: ...


class AppServerRpcClient(Protocol):
    def call(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        timeout_seconds: int = 60,
    ) -> Mapping[str, Any]: ...


class AcpRequestClient(Protocol):
    """Async request tracker around an ACP client connection.

    `start_request` maps to `session/prompt`, `notify` maps to
    `session/cancel`, and the remaining methods expose the client-side request
    journal needed for restart-safe reconciliation.
    """

    def session_state(self, session_id: str) -> Mapping[str, Any]: ...

    def request_journal(self, session_id: str) -> Sequence[Mapping[str, Any]]: ...

    def start_request(self, method: str, params: Mapping[str, Any]) -> str: ...

    def request_state(self, request_id: str) -> Mapping[str, Any]: ...

    def notify(self, method: str, params: Mapping[str, Any]) -> None: ...


class CodexDesktopNativePort:
    port_id = "codex-desktop-native"

    def __init__(self, client: DesktopTaskClient) -> None:
        self.client = client

    def observe_session(
        self, *, native_session_id: str
    ) -> NativeSessionObservation:
        task = self.client.read_task(native_session_id)
        turns = _turns(task)
        latest = turns[-1] if turns else {}
        status = _turn_status(latest)
        return NativeSessionObservation(
            port_id=self.port_id,
            native_session_id=native_session_id,
            native_turn_id=str(latest.get("id") or ""),
            state=_session_state(task, status),
            details={"turn_count": len(turns)},
        )

    def deliver_action(
        self,
        *,
        action_id: str,
        idempotency_key: str,
        native_session_id: str,
        prompt: str,
        workspace: Path,
        output_schema: Mapping[str, Any],
    ) -> NativeTurnBinding:
        del workspace, output_schema
        existing = self.reconcile_delivery(
            action_id=action_id,
            idempotency_key=idempotency_key,
            native_session_id=native_session_id,
        )
        if existing is not None:
            return existing
        sent = self.client.send_message(
            native_session_id,
            _marked_prompt(action_id, idempotency_key, prompt),
        )
        turn_id = _response_turn_id(sent)
        if not turn_id:
            raise AgentPortError("Desktop delivery returned no native turn identity")
        return _binding(
            self.port_id,
            action_id,
            idempotency_key,
            native_session_id,
            turn_id,
            "delivered",
        )

    def reconcile_delivery(
        self,
        *,
        action_id: str,
        idempotency_key: str,
        native_session_id: str,
    ) -> NativeTurnBinding | None:
        marker = _action_marker(action_id, idempotency_key)
        for turn in reversed(_turns(self.client.read_task(native_session_id))):
            if marker in _turn_user_text(turn):
                return _binding(
                    self.port_id,
                    action_id,
                    idempotency_key,
                    native_session_id,
                    str(turn.get("id") or ""),
                    "reconciled",
                )
        return None

    def cancel_turn(
        self,
        *,
        native_session_id: str,
        native_turn_id: str,
    ) -> NativeSessionObservation:
        self.client.cancel_turn(native_session_id, native_turn_id)
        return NativeSessionObservation(
            port_id=self.port_id,
            native_session_id=native_session_id,
            native_turn_id=native_turn_id,
            state="running",
            details={"cancellation_requested": True},
        )

    def collect_outcome(
        self,
        *,
        action_id: str,
        native_session_id: str,
        native_turn_id: str,
    ) -> dict[str, Any]:
        turn = _find_turn(self.client.read_task(native_session_id), native_turn_id)
        return _outcome_from_turn(
            action_id=action_id,
            native_session_id=native_session_id,
            native_turn_id=native_turn_id,
            turn=turn,
        )


class CodexAppServerPort:
    port_id = "codex-app-server"

    def __init__(self, client: AppServerRpcClient) -> None:
        self.client = client

    def observe_session(
        self, *, native_session_id: str
    ) -> NativeSessionObservation:
        thread = self._read_thread(native_session_id)
        turns = _turns(thread)
        latest = turns[-1] if turns else {}
        status = _turn_status(latest)
        return NativeSessionObservation(
            port_id=self.port_id,
            native_session_id=native_session_id,
            native_turn_id=str(latest.get("id") or ""),
            state=_session_state(thread, status),
            details={"thread_status": thread.get("status", {})},
        )

    def deliver_action(
        self,
        *,
        action_id: str,
        idempotency_key: str,
        native_session_id: str,
        prompt: str,
        workspace: Path,
        output_schema: Mapping[str, Any],
    ) -> NativeTurnBinding:
        existing = self.reconcile_delivery(
            action_id=action_id,
            idempotency_key=idempotency_key,
            native_session_id=native_session_id,
        )
        if existing is not None:
            return existing
        response = self.client.call(
            "turn/start",
            {
                "threadId": native_session_id,
                "input": [
                    {
                        "type": "text",
                        "text": _marked_prompt(action_id, idempotency_key, prompt),
                    }
                ],
                "cwd": str(workspace.resolve()),
                "approvalPolicy": "never",
                "sandboxPolicy": {
                    "type": "workspaceWrite",
                    "writableRoots": [str(workspace.resolve())],
                    "networkAccess": False,
                },
                "outputSchema": dict(output_schema),
            },
        )
        result = _rpc_result(response)
        turn = result.get("turn") if isinstance(result.get("turn"), Mapping) else {}
        turn_id = str(turn.get("id") or "")
        if not turn_id:
            raise AgentPortError("App Server turn/start returned no turn identity")
        return _binding(
            self.port_id,
            action_id,
            idempotency_key,
            native_session_id,
            turn_id,
            "delivered",
        )

    def reconcile_delivery(
        self,
        *,
        action_id: str,
        idempotency_key: str,
        native_session_id: str,
    ) -> NativeTurnBinding | None:
        marker = _action_marker(action_id, idempotency_key)
        for turn in reversed(_turns(self._read_thread(native_session_id))):
            if marker in _turn_user_text(turn):
                return _binding(
                    self.port_id,
                    action_id,
                    idempotency_key,
                    native_session_id,
                    str(turn.get("id") or ""),
                    "reconciled",
                )
        return None

    def cancel_turn(
        self,
        *,
        native_session_id: str,
        native_turn_id: str,
    ) -> NativeSessionObservation:
        self.client.call(
            "turn/interrupt",
            {"threadId": native_session_id, "turnId": native_turn_id},
        )
        return NativeSessionObservation(
            port_id=self.port_id,
            native_session_id=native_session_id,
            native_turn_id=native_turn_id,
            state="running",
            details={"cancellation_requested": True},
        )

    def collect_outcome(
        self,
        *,
        action_id: str,
        native_session_id: str,
        native_turn_id: str,
    ) -> dict[str, Any]:
        turn = _find_turn(self._read_thread(native_session_id), native_turn_id)
        return _outcome_from_turn(
            action_id=action_id,
            native_session_id=native_session_id,
            native_turn_id=native_turn_id,
            turn=turn,
        )

    def _read_thread(self, thread_id: str) -> dict[str, Any]:
        response = self.client.call(
            "thread/read", {"threadId": thread_id, "includeTurns": True}
        )
        result = _rpc_result(response)
        thread = result.get("thread")
        if not isinstance(thread, Mapping):
            raise AgentPortError("App Server thread/read returned no thread")
        return dict(thread)


class AgentPoolAcpPort:
    port_id = "agent-pool-acp"

    def __init__(self, client: AcpRequestClient) -> None:
        self.client = client

    def observe_session(
        self, *, native_session_id: str
    ) -> NativeSessionObservation:
        state = dict(self.client.session_state(native_session_id))
        native_state = str(state.get("state") or "missing")
        normalized = {
            "active": "running",
            "ready": "idle",
            "idle": "idle",
            "terminal": "terminal",
            "missing": "missing",
            "unavailable": "unavailable",
        }.get(native_state, "unavailable")
        return NativeSessionObservation(
            port_id=self.port_id,
            native_session_id=native_session_id,
            native_turn_id=str(state.get("request_id") or ""),
            state=normalized,
            details=state,
        )

    def deliver_action(
        self,
        *,
        action_id: str,
        idempotency_key: str,
        native_session_id: str,
        prompt: str,
        workspace: Path,
        output_schema: Mapping[str, Any],
    ) -> NativeTurnBinding:
        existing = self.reconcile_delivery(
            action_id=action_id,
            idempotency_key=idempotency_key,
            native_session_id=native_session_id,
        )
        if existing is not None:
            return existing
        request_id = self.client.start_request(
            "session/prompt",
            {
                "sessionId": native_session_id,
                "prompt": [
                    {
                        "type": "text",
                        "text": _marked_prompt(action_id, idempotency_key, prompt),
                    }
                ],
                "_meta": {
                    "ascendop": {
                        "actionId": action_id,
                        "idempotencyKey": idempotency_key,
                        "cwd": str(workspace.resolve()),
                        "outputSchema": dict(output_schema),
                    }
                },
            },
        )
        if not str(request_id).strip():
            raise AgentPortError("ACP session/prompt returned no request identity")
        return _binding(
            self.port_id,
            action_id,
            idempotency_key,
            native_session_id,
            str(request_id),
            "delivered",
        )

    def reconcile_delivery(
        self,
        *,
        action_id: str,
        idempotency_key: str,
        native_session_id: str,
    ) -> NativeTurnBinding | None:
        for item in reversed(list(self.client.request_journal(native_session_id))):
            if (
                str(item.get("action_id") or "") == action_id
                and str(item.get("idempotency_key") or "") == idempotency_key
            ):
                return _binding(
                    self.port_id,
                    action_id,
                    idempotency_key,
                    native_session_id,
                    str(item.get("request_id") or ""),
                    "reconciled",
                )
        return None

    def cancel_turn(
        self,
        *,
        native_session_id: str,
        native_turn_id: str,
    ) -> NativeSessionObservation:
        self.client.notify("session/cancel", {"sessionId": native_session_id})
        return NativeSessionObservation(
            port_id=self.port_id,
            native_session_id=native_session_id,
            native_turn_id=native_turn_id,
            state="running",
            details={"cancellation_requested": True},
        )

    def collect_outcome(
        self,
        *,
        action_id: str,
        native_session_id: str,
        native_turn_id: str,
    ) -> dict[str, Any]:
        state = dict(self.client.request_state(native_turn_id))
        if not bool(state.get("done")):
            raise AgentTurnPending(f"ACP request is still active: {native_turn_id}")
        response = state.get("response")
        response = dict(response) if isinstance(response, Mapping) else {}
        stop_reason = str(response.get("stopReason") or "").lower()
        status = {
            "end_turn": "completed",
            "endturn": "completed",
            "completed": "completed",
            "cancelled": "cancelled",
            "canceled": "cancelled",
            "refusal": "failed",
            "max_tokens": "failed",
            "max_turn_requests": "failed",
        }.get(stop_reason, "failed")
        structured = state.get("structured_result")
        if not isinstance(structured, Mapping):
            structured = _strict_json_object(str(state.get("text") or ""))
        return native_turn_outcome(
            action_id=action_id,
            native_session_id=native_session_id,
            native_turn_id=native_turn_id,
            terminal_status=status,
            structured_result=structured,
            artifact_refs=_artifact_refs(structured),
            usage_telemetry=(
                state.get("usage") if isinstance(state.get("usage"), Mapping) else {}
            ),
            skill_telemetry=(
                state.get("skills") if isinstance(state.get("skills"), Mapping) else {}
            ),
            observed_at=_utc_now(),
        )


def _binding(
    port_id: str,
    action_id: str,
    idempotency_key: str,
    native_session_id: str,
    native_turn_id: str,
    delivery_state: str,
) -> NativeTurnBinding:
    return NativeTurnBinding(
        port_id=port_id,
        action_id=action_id,
        idempotency_key=idempotency_key,
        native_session_id=native_session_id,
        native_turn_id=native_turn_id,
        delivery_state=delivery_state,
        observed_at=_utc_now(),
    )


def _action_marker(action_id: str, idempotency_key: str) -> str:
    if not action_id.strip() or not idempotency_key.strip():
        raise AgentPortError("delivery requires action and idempotency identity")
    return f"[ascendop-action:{action_id}:{idempotency_key}]"


def _marked_prompt(action_id: str, idempotency_key: str, prompt: str) -> str:
    if not prompt.strip():
        raise AgentPortError("delivery prompt must not be empty")
    return _action_marker(action_id, idempotency_key) + "\n" + prompt


def _rpc_result(response: Mapping[str, Any]) -> dict[str, Any]:
    if "error" in response:
        raise AgentPortError(f"native JSON-RPC request failed: {response['error']}")
    result = response.get("result")
    if not isinstance(result, Mapping):
        raise AgentPortError("native JSON-RPC response contains no result")
    return dict(result)


def _response_turn_id(response: Mapping[str, Any]) -> str:
    for value in (
        response.get("turnId"),
        response.get("turn_id"),
        response.get("id"),
    ):
        if str(value or "").strip():
            return str(value)
    turn = response.get("turn")
    return str(turn.get("id") or "") if isinstance(turn, Mapping) else ""


def _turns(container: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = container.get("turns")
    if not isinstance(raw, list):
        return []
    return [dict(item) for item in raw if isinstance(item, Mapping)]


def _find_turn(container: Mapping[str, Any], turn_id: str) -> dict[str, Any]:
    for turn in _turns(container):
        if str(turn.get("id") or "") == turn_id:
            return turn
    raise AgentPortError(f"native turn is not visible: {turn_id}")


def _turn_status(turn: Mapping[str, Any]) -> str:
    value = turn.get("status")
    if isinstance(value, Mapping):
        return str(value.get("type") or "")
    return str(value or "")


def _session_state(container: Mapping[str, Any], turn_status: str) -> str:
    if not container:
        return "missing"
    lowered = turn_status.lower()
    if lowered in {"inprogress", "in_progress", "running", "active"}:
        return "running"
    if lowered in _TERMINAL_STATUSES:
        return "terminal"
    return "idle"


def _turn_user_text(turn: Mapping[str, Any]) -> str:
    parts: list[str] = []
    direct = turn.get("prompt")
    if isinstance(direct, str):
        parts.append(direct)
    for item in turn.get("items", []) if isinstance(turn.get("items"), list) else []:
        if not isinstance(item, Mapping):
            continue
        item_type = str(item.get("type") or "").lower()
        if item_type not in {"usermessage", "user_message", "user"}:
            continue
        content = item.get("content")
        if isinstance(content, str):
            parts.append(content)
        elif isinstance(content, list):
            for block in content:
                if isinstance(block, Mapping) and isinstance(block.get("text"), str):
                    parts.append(str(block["text"]))
    return "\n".join(parts)


def _outcome_from_turn(
    *,
    action_id: str,
    native_session_id: str,
    native_turn_id: str,
    turn: Mapping[str, Any],
) -> dict[str, Any]:
    native_status = _turn_status(turn)
    status = {
        "completed": "completed",
        "failed": "failed",
        "cancelled": "cancelled",
        "canceled": "cancelled",
        "interrupted": "interrupted",
    }.get(native_status.lower())
    if status is None:
        raise AgentTurnPending(f"native turn is not terminal: {native_turn_id}")
    structured = turn.get("structuredResult") or turn.get("structured_result")
    if not isinstance(structured, Mapping):
        structured = _strict_json_object(_turn_agent_text(turn))
    usage = turn.get("usage") if isinstance(turn.get("usage"), Mapping) else {}
    skills = (
        turn.get("skillTelemetry")
        if isinstance(turn.get("skillTelemetry"), Mapping)
        else {}
    )
    return native_turn_outcome(
        action_id=action_id,
        native_session_id=native_session_id,
        native_turn_id=native_turn_id,
        terminal_status=status,
        structured_result=structured,
        artifact_refs=_artifact_refs(structured),
        usage_telemetry=usage,
        skill_telemetry=skills,
        observed_at=_utc_now(),
    )


def _turn_agent_text(turn: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for item in turn.get("items", []) if isinstance(turn.get("items"), list) else []:
        if not isinstance(item, Mapping):
            continue
        item_type = str(item.get("type") or "").lower()
        if item_type not in {"agentmessage", "agent_message", "assistant"}:
            continue
        content = item.get("content")
        if isinstance(content, str):
            parts.append(content)
        elif isinstance(content, list):
            for block in content:
                if isinstance(block, Mapping) and isinstance(block.get("text"), str):
                    parts.append(str(block["text"]))
    return "\n".join(parts)


def _strict_json_object(text: str) -> dict[str, Any]:
    value = text.strip()
    if value.startswith("```json") and value.endswith("```"):
        value = value[7:-3].strip()
    elif value.startswith("```") and value.endswith("```"):
        value = value[3:-3].strip()
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise AgentPortError("native terminal turn has no structured JSON result") from exc
    if not isinstance(parsed, dict):
        raise AgentPortError("native structured result must be an object")
    return parsed


def _artifact_refs(structured: Mapping[str, Any]) -> list[str]:
    raw = structured.get("artifacts") or structured.get("artifact_refs") or []
    return [str(item) for item in raw] if isinstance(raw, list) else []


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


__all__ = [
    "AcpRequestClient",
    "AgentPoolAcpPort",
    "AppServerRpcClient",
    "CodexAppServerPort",
    "CodexDesktopNativePort",
    "DesktopTaskClient",
]
