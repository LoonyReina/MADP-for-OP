from __future__ import annotations

import http.client
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

from ascendop_agent_runner.responses_bridge import DeepSeekResponsesBridge


class _UpstreamHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    calls: list[tuple[str, bytes, str]] = []

    def log_message(self, _format: str, *_args: object) -> None:
        return

    def do_POST(self) -> None:
        length = int(self.headers["Content-Length"])
        body = self.rfile.read(length)
        self.calls.append(
            (self.path, body, str(self.headers.get("Authorization") or ""))
        )
        payload = b'data: {"type":"response.completed"}\n\n'
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def test_bridge_forwards_only_authenticated_responses_requests() -> None:
    _UpstreamHandler.calls = []
    upstream = ThreadingHTTPServer(("127.0.0.1", 0), _UpstreamHandler)
    upstream_thread = threading.Thread(target=upstream.serve_forever, daemon=True)
    upstream_thread.start()
    bridge = DeepSeekResponsesBridge(
        secret="sk-test-bridge",
        connection_factory=lambda: http.client.HTTPConnection(
            "127.0.0.1", upstream.server_port, timeout=5
        ),
    ).start()
    endpoint = urlsplit(bridge.base_url)
    try:
        unauthorized = http.client.HTTPConnection(
            endpoint.hostname, endpoint.port, timeout=5
        )
        unauthorized.request("POST", "/responses", body=b"{}")
        assert unauthorized.getresponse().status == 401
        unauthorized.close()
        assert _UpstreamHandler.calls == []

        authorized = http.client.HTTPConnection(
            endpoint.hostname, endpoint.port, timeout=5
        )
        authorized.request(
            "POST",
            "/responses",
            body=b'{"stream":true}',
            headers={"Authorization": "Bearer sk-test-bridge"},
        )
        response = authorized.getresponse()
        assert response.status == 200
        assert response.read() == b'data: {"type":"response.completed"}\n\n'
        authorized.close()
        assert _UpstreamHandler.calls == [
            ("/responses", b'{"stream":true}', "Bearer sk-test-bridge")
        ]
    finally:
        bridge.close()
        upstream.shutdown()
        upstream.server_close()
        upstream_thread.join(timeout=5)
