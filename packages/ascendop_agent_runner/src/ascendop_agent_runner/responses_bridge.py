from __future__ import annotations

import hmac
import http.client
import json
import ssl
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable


DEEPSEEK_RESPONSES_HOST = "api.deepseek.com"
DEEPSEEK_RESPONSES_PATH = "/responses"
BRIDGE_SOCKET_IDLE_TIMEOUT_SECONDS = 120
_ERROR_BODY_DRAIN_LIMIT = 64 * 1024
_HOP_BY_HOP_HEADERS = frozenset(
    {
        "connection",
        "host",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "proxy-connection",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
)


class ResponsesBridgeError(RuntimeError):
    pass


ConnectionFactory = Callable[[], http.client.HTTPConnection]


class DeepSeekResponsesBridge:
    def __init__(
        self,
        *,
        secret: str,
        socket_idle_timeout_seconds: int = BRIDGE_SOCKET_IDLE_TIMEOUT_SECONDS,
        connection_factory: ConnectionFactory | None = None,
    ) -> None:
        self._secret = secret
        self._connection_factory = connection_factory or (
            lambda: _deepseek_connection(socket_idle_timeout_seconds)
        )
        handler = _handler_type(
            secret=secret,
            connection_factory=self._connection_factory,
        )
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="ascendop-deepseek-responses-bridge",
            daemon=True,
        )
        self._started = False
        self._closed = False

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self._server.server_port}/"

    def start(self) -> DeepSeekResponsesBridge:
        if self._closed:
            raise ResponsesBridgeError("Responses bridge is already closed")
        if not self._started:
            self._thread.start()
            self._started = True
        return self

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._started:
            self._server.shutdown()
            self._thread.join(timeout=5)
        self._server.server_close()
        self._secret = ""


def _deepseek_connection(
    timeout_seconds: int = BRIDGE_SOCKET_IDLE_TIMEOUT_SECONDS,
) -> http.client.HTTPConnection:
    return http.client.HTTPSConnection(
        DEEPSEEK_RESPONSES_HOST,
        timeout=timeout_seconds,
        context=ssl.create_default_context(),
    )


def _handler_type(
    *,
    secret: str,
    connection_factory: ConnectionFactory,
) -> type[BaseHTTPRequestHandler]:
    expected_authorization = f"Bearer {secret}"

    class ResponsesHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, _format: str, *_args: object) -> None:
            return

        def do_POST(self) -> None:
            if self.path != DEEPSEEK_RESPONSES_PATH:
                self._drain_error_body()
                self._json_error(404, "unsupported-path")
                return
            authorization = str(self.headers.get("Authorization") or "")
            if not hmac.compare_digest(authorization, expected_authorization):
                self._drain_error_body()
                self._json_error(401, "invalid-bridge-credential")
                return
            if self.headers.get("Transfer-Encoding"):
                self._json_error(400, "chunked-request-not-supported")
                return
            try:
                content_length = int(self.headers.get("Content-Length") or "")
            except ValueError:
                content_length = -1
            if content_length < 0:
                self._json_error(411, "content-length-required")
                return
            with tempfile.SpooledTemporaryFile(max_size=1024 * 1024) as body:
                remaining = content_length
                while remaining:
                    chunk = self.rfile.read(min(remaining, 64 * 1024))
                    if not chunk:
                        self._json_error(400, "incomplete-request-body")
                        return
                    body.write(chunk)
                    remaining -= len(chunk)
                body.seek(0)
                self._forward(body=body, content_length=content_length)

        def _forward(self, *, body: object, content_length: int) -> None:
            connection = connection_factory()
            response_started = False
            headers = {
                name: value
                for name, value in self.headers.items()
                if name.lower() not in _HOP_BY_HOP_HEADERS
            }
            headers["Content-Length"] = str(content_length)
            try:
                connection.request(
                    "POST",
                    DEEPSEEK_RESPONSES_PATH,
                    body=body,
                    headers=headers,
                )
                response = connection.getresponse()
                self.send_response(response.status)
                for name, value in response.getheaders():
                    if name.lower() not in _HOP_BY_HOP_HEADERS | {"content-length"}:
                        self.send_header(name, value)
                self.send_header("Connection", "close")
                self.end_headers()
                response_started = True
                while True:
                    chunk = response.read(64 * 1024)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
                    self.wfile.flush()
            except (OSError, http.client.HTTPException):
                if not response_started and not self.wfile.closed:
                    self._json_error(502, "upstream-transport-failure")
            finally:
                connection.close()
                self.close_connection = True

        def _drain_error_body(self) -> None:
            if self.headers.get("Transfer-Encoding"):
                return
            try:
                remaining = min(
                    max(int(self.headers.get("Content-Length") or "0"), 0),
                    _ERROR_BODY_DRAIN_LIMIT,
                )
            except ValueError:
                return
            previous_timeout = self.connection.gettimeout()
            self.connection.settimeout(0.1)
            try:
                while remaining:
                    chunk = self.rfile.read(min(remaining, 64 * 1024))
                    if not chunk:
                        break
                    remaining -= len(chunk)
            except OSError:
                pass
            finally:
                self.connection.settimeout(previous_timeout)

        def _json_error(self, status: int, code: str) -> None:
            payload = json.dumps(
                {"error": {"type": "transport_error", "code": code}},
                separators=(",", ":"),
            ).encode("ascii")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(payload)
            self.wfile.flush()
            self.close_connection = True

    return ResponsesHandler


__all__ = [
    "BRIDGE_SOCKET_IDLE_TIMEOUT_SECONDS",
    "DEEPSEEK_RESPONSES_HOST",
    "DEEPSEEK_RESPONSES_PATH",
    "DeepSeekResponsesBridge",
    "ResponsesBridgeError",
]
