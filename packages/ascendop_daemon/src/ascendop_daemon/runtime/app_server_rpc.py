"""Multiplex one supervisor-owned app-server stdio connection across turns.

No process creation, session creation, scheduling, retries or business state.
Timeout retains the original request: the caller can wait again or reconcile
the original action with the provider instead of sending another turn/start.
"""
from __future__ import annotations

from concurrent.futures import Future, TimeoutError as FutureTimeout
import json
import threading
from typing import Any, Callable, Mapping


class RpcConnectionError(RuntimeError):
    pass


class RpcResponsePending(TimeoutError):
    def __init__(self, request_id: int):
        self.request_id = request_id
        super().__init__(f"app-server response pending for original request {request_id}")


class RpcResponseTooLarge(RpcConnectionError):
    """One complete response exceeded its budget; the connection remains usable."""
    def __init__(self, request_id, actual_bytes, limit):
        self.request_id = request_id
        self.response_consumed = True
        super().__init__(f"app-server response {request_id} has {actual_bytes} bytes (budget {limit}); use a smaller read view")


class StdioAppServerClient:
    """Compatible with automation.agent_execution_ports.AppServerRpcClient.

    stdout/stdin belong to an already identified, supervisor-owned process.
    A connection failure never kills or restarts that process. Notifications
    are observed, not accepted as business completion by this transport.
    """

    def __init__(self, stdin, stdout, *, on_notification: Callable[[Mapping[str, Any]], None] | None = None,
                 max_pending: int = 64, max_line_bytes: int = 8 * 1024 * 1024,
                 max_frame_bytes: int = 64 * 1024 * 1024):
        if max_pending < 1 or max_line_bytes < 1 or max_frame_bytes < 1:
            raise ValueError("RPC bounds must be positive")
        self.stdin, self.stdout = stdin, stdout
        self.on_notification = on_notification
        self.max_pending, self.max_line_bytes = max_pending, max_line_bytes
        # Separate the application's response budget from a complete wire
        # frame. A rejected history page must not abandon half a JSON line and
        # permanently strand every unrelated request on this same connection.
        self.max_frame_bytes = max(max_line_bytes, max_frame_bytes)
        self._pending: dict[int, Future] = {}
        self._guard, self._write_guard = threading.Lock(), threading.Lock()
        self._next_id, self._failure = 0, None
        self._reader = threading.Thread(target=self._read, name="ascendop-app-server-rpc", daemon=True)
        self._reader.start()

    def submit(self, method: str, params: dict[str, Any] | None = None) -> int:
        if not isinstance(method, str) or not method:
            raise ValueError("RPC method is required")
        # Validate serialization before recording a request that cannot be sent.
        json.dumps(params, allow_nan=False)
        with self._guard:
            if self._failure is not None:
                raise RpcConnectionError(self._failure)
            if len(self._pending) >= self.max_pending:
                raise RpcConnectionError("reconcile outstanding RPC requests before admitting more")
            self._next_id += 1
            request_id = self._next_id
            self._pending[request_id] = Future()
        self._write({"id": request_id, "method": method, "params": params or {}})
        return request_id

    def wait_response(self, request_id: int, timeout_seconds: float = 60) -> Mapping[str, Any]:
        with self._guard:
            future = self._pending.get(request_id)
        if future is None:
            raise ValueError("unknown or already consumed original RPC request")
        try:
            response = future.result(timeout=timeout_seconds)
        except FutureTimeout as exc:
            raise RpcResponsePending(request_id) from exc
        except RpcResponseTooLarge:
            with self._guard:
                self._pending.pop(request_id, None)
            raise
        # Failed/uncertain requests remain identifiable until their owner has
        # reconciled them; successful receipt consumption is strictly once.
        with self._guard:
            self._pending.pop(request_id, None)
        return response

    def call(self, method: str, params: dict[str, Any] | None = None,
             timeout_seconds: float = 60) -> Mapping[str, Any]:
        return self.wait_response(self.submit(method, params), timeout_seconds)

    def notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        self._write({"method": method, "params": params or {}})

    def initialize(self, *, timeout_seconds: float = 30) -> Mapping[str, Any]:
        response = self.call("initialize", {"clientInfo": {"name": "ascendop", "version": "5"},
            "capabilities": {"experimentalApi": True}}, timeout_seconds)
        if "error" in response or not isinstance(response.get("result"), dict):
            raise RpcConnectionError("app-server initialization was not accepted")
        self.notify("initialized")
        return response

    def _write(self, value: Mapping[str, Any]) -> None:
        line = json.dumps(value, ensure_ascii=False, allow_nan=False) + "\n"
        try:
            with self._write_guard:
                self.stdin.write(line)
                self.stdin.flush()
        except (OSError, ValueError) as exc:
            self._fail("app-server write failed; delivery may be uncertain")
            raise RpcConnectionError("app-server write failed; reconcile original delivery") from exc

    def _read(self) -> None:
        try:
            while True:
                line = self.stdout.readline(self.max_frame_bytes + 1)
                if not line:
                    raise RpcConnectionError("app-server stream closed; process state requires separate observation")
                size = len(line.encode("utf-8"))
                if size > self.max_frame_bytes or not line.endswith("\n"):
                    raise RpcConnectionError(f"app-server incomplete/oversized wire frame: {size} bytes, hard bound {self.max_frame_bytes}; reconcile original requests")
                message = json.loads(line)
                if not isinstance(message, dict):
                    raise RpcConnectionError("app-server message is not an object")
                if "method" in message:
                    if "id" in message:
                        # File-only Solver never obtains new approval/tool power
                        # because this connection has no interactive consumer.
                        self._write({"id": message["id"], "error": {"code": -32601,
                            "message": "Unsupported server request on file-only Solver carrier"}})
                    elif self.on_notification is not None:
                        self.on_notification(message)
                    continue
                request_id = message.get("id")
                with self._guard:
                    future = self._pending.get(request_id) if type(request_id) is int else None
                    if future is None or future.done() or (("result" in message) == ("error" in message)):
                        raise RpcConnectionError("app-server returned an unknown, duplicate or malformed response")
                    if size > self.max_line_bytes:
                        future.set_exception(RpcResponseTooLarge(request_id, size, self.max_line_bytes))
                    else:
                        future.set_result(message)
        except Exception as exc:
            self._fail(str(exc))

    def _fail(self, reason: str) -> None:
        with self._guard:
            self._failure = reason
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(RpcConnectionError(reason))
