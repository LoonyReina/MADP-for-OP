from __future__ import annotations

import hashlib
import json
import os
import socket
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import urlparse

from ascendop_protocol.management import validate_control_command

from ascendop_control.application import PublicQueryService
from ascendop_control.storage import ControlStore


CAPABILITY_ORDER = {"viewer": 1, "operator": 2, "admin": 3}


class ControlApiServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        *,
        store: ControlStore,
        token_capabilities: dict[str, str],
        code_generation: str = "test-generation",
        sse_poll_seconds: float = 0.5,
    ) -> None:
        if address[0] not in {"127.0.0.1", "::1", "localhost"}:
            raise ValueError("control API must bind to loopback")
        self.store = store
        self.queries = PublicQueryService(store)
        self.token_capabilities = dict(token_capabilities)
        self.sse_poll_seconds = max(0.1, min(float(sse_poll_seconds), 5.0))
        self.code_generation = str(code_generation).strip()
        if not self.code_generation:
            raise ValueError("control API code generation is required")
        self.boot_id = hashlib.sha256(
            f"{socket.gethostname()}:{os.getpid()}:{time.time_ns()}".encode("utf-8")
        ).hexdigest()[:24]
        self._last_service_heartbeat = 0.0
        super().__init__(address, ControlApiHandler)
        self._heartbeat()

    def service_actions(self) -> None:
        self._heartbeat()

    def _heartbeat(self) -> None:
        now = time.monotonic()
        if now - self._last_service_heartbeat < 5.0:
            return
        self.store.record_runtime_service_heartbeat(
            service_id="ascendop-control-api",
            role="management-api",
            code_generation=self.code_generation,
            capabilities=["rest-read-models", "sse-replay", "typed-commands"],
            state="ready",
            boot_id=self.boot_id,
            lease_seconds=30,
            details={
                "pid": os.getpid(),
                "host": self.server_address[0],
                "port": self.server_address[1],
            },
        )
        self._last_service_heartbeat = now


class ControlApiHandler(BaseHTTPRequestHandler):
    server: ControlApiServer
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:  # noqa: N802
        if not self._authorized("viewer"):
            return
        path = urlparse(self.path).path
        routes: dict[str, Callable[[], dict[str, Any]]] = {
            "/api/v1/system": self.server.queries.system,
            "/api/v1/operators": self.server.queries.operators,
            "/api/v1/requests": self.server.queries.requests,
            "/api/v1/endpoints": self.server.queries.endpoints,
            "/api/v1/agents": self.server.queries.agents,
            "/api/v1/agent-pools": self.server.queries.agent_pools,
            "/api/v1/role-bindings": self.server.queries.role_bindings,
            "/api/v1/workflows": self.server.queries.workflows,
            "/api/v1/manager-notifications": self.server.queries.manager_notifications,
            "/api/v1/operator-workflows": self.server.queries.operator_workflows,
            "/api/v1/workflow-traces": self.server.queries.workflow_traces,
            "/api/v1/agent-actions": self.server.queries.agent_actions,
            "/api/v1/agent-leases": self.server.queries.agent_leases,
            "/api/v1/agent-iterations": self.server.queries.iterations,
            "/api/v1/official": self.server.queries.official,
            "/api/v1/artifacts": self.server.queries.artifacts,
        }
        if path == "/api/v1/events":
            self._serve_events()
            return
        handler = routes.get(path)
        if handler is None:
            self._json(HTTPStatus.NOT_FOUND, {"error": "resource-not-found"})
            return
        try:
            self._json(HTTPStatus.OK, handler())
        except Exception as exc:
            self._json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"error": "query-failed", "detail": str(exc)},
            )

    def do_POST(self) -> None:  # noqa: N802
        if urlparse(self.path).path != "/api/v1/commands":
            self._json(HTTPStatus.NOT_FOUND, {"error": "resource-not-found"})
            return
        if not self._authorized("operator"):
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 1 <= length <= 1024 * 1024:
                raise ValueError("command body size is invalid")
            raw = json.loads(self.rfile.read(length).decode("utf-8"))
            command = validate_control_command(raw)
            capability = self._capability()
            required = str(command["required_capability"])
            if CAPABILITY_ORDER.get(capability, 0) < CAPABILITY_ORDER[required]:
                self._json(HTTPStatus.FORBIDDEN, {"error": "insufficient-capability"})
                return
            result = self.server.store.submit_control_command(command)
            self._json(HTTPStatus.ACCEPTED, result)
        except (ValueError, json.JSONDecodeError) as exc:
            self._json(HTTPStatus.BAD_REQUEST, {"error": "invalid-command", "detail": str(exc)})
        except Exception as exc:
            self._json(HTTPStatus.CONFLICT, {"error": "command-rejected", "detail": str(exc)})

    def _serve_events(self) -> None:
        raw_sequence = self.headers.get("Last-Event-ID", "0")
        try:
            sequence = max(0, int(raw_sequence))
        except ValueError:
            self._json(HTTPStatus.BAD_REQUEST, {"error": "invalid-last-event-id"})
            return
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        deadline = time.monotonic() + 25.0
        try:
            while time.monotonic() < deadline:
                events = self.server.store.control_events_after(sequence, limit=200)
                if not events:
                    self.wfile.write(b": keepalive\n\n")
                    self.wfile.flush()
                    time.sleep(self.server.sse_poll_seconds)
                    continue
                for event in events:
                    sequence = int(event["sequence"])
                    data = json.dumps(event, ensure_ascii=True, separators=(",", ":"))
                    message = f"id: {sequence}\nevent: control\ndata: {data}\n\n"
                    self.wfile.write(message.encode("utf-8"))
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            return

    def _authorized(self, required: str) -> bool:
        capability = self._capability()
        if CAPABILITY_ORDER.get(capability, 0) >= CAPABILITY_ORDER[required]:
            return True
        self._json(HTTPStatus.UNAUTHORIZED, {"error": "invalid-or-missing-token"})
        return False

    def _capability(self) -> str:
        value = self.headers.get("Authorization", "")
        if not value.startswith("Bearer "):
            return ""
        digest = hashlib.sha256(value[7:].encode("utf-8")).hexdigest()
        return self.server.token_capabilities.get(digest, "")

    def _json(self, status: HTTPStatus, value: Any) -> None:
        payload = json.dumps(value, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: Any) -> None:
        return
