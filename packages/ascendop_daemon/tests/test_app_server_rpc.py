from __future__ import annotations

import json
import queue

import pytest

from ascendop_daemon.runtime.app_server_rpc import (
    RpcConnectionError, RpcResponsePending, RpcResponseTooLarge, StdioAppServerClient,
)


class Stream:
    def __init__(self):
        self.lines = queue.Queue()

    def write(self, text):
        self.lines.put(text)

    def flush(self):
        pass

    def readline(self, _limit):
        return self.lines.get(timeout=5)


@pytest.fixture
def connection():
    stdin, stdout = Stream(), Stream()
    client = StdioAppServerClient(stdin, stdout)
    yield client, stdin, stdout
    stdout.write("")
    client._reader.join(timeout=5)
    assert not client._reader.is_alive()


def test_out_of_order_replies_keep_request_identity(connection):
    client, stdin, stdout = connection
    first = client.submit("thread/read", {"threadId": "one"})
    second = client.submit("thread/read", {"threadId": "two"})
    stdout.write(json.dumps({"id": second, "result": {"thread": "two"}}) + "\n")
    stdout.write(json.dumps({"id": first, "result": {"thread": "one"}}) + "\n")
    assert client.wait_response(first)["result"] == {"thread": "one"}
    assert client.wait_response(second)["result"] == {"thread": "two"}
    assert stdin.lines.qsize() == 2


def test_timeout_preserves_original_request_and_does_not_resend(connection):
    client, stdin, stdout = connection
    with pytest.raises(RpcResponsePending) as pending:
        client.call("turn/start", {"threadId": "original"}, timeout_seconds=0)
    sent = json.loads(stdin.lines.get_nowait())
    assert pending.value.request_id == sent["id"]
    stdout.write(json.dumps({"id": sent["id"], "result": {"turn": {"id": "actual"}}}) + "\n")
    assert client.wait_response(pending.value.request_id)["result"]["turn"]["id"] == "actual"
    assert stdin.lines.empty()


def test_server_request_is_not_implicitly_approved(connection):
    client, stdin, stdout = connection
    stdout.write(json.dumps({"id": "approval", "method": "item/commandExecution/requestApproval"}) + "\n")
    reply = json.loads(stdin.lines.get(timeout=5))
    assert reply["id"] == "approval" and reply["error"]["code"] == -32601


@pytest.mark.parametrize("response", [[], {"id": 999, "result": {}}, {"id": 1, "result": {}, "error": {}}])
def test_bad_response_never_becomes_an_accepted_turn(connection, response):
    client, _, stdout = connection
    request_id = client.submit("thread/read")
    stdout.write(json.dumps(response) + "\n")
    with pytest.raises(RpcConnectionError):
        client.wait_response(request_id)


def test_connection_loss_does_not_manufacture_turn_failure_or_restart(connection):
    client, stdin, stdout = connection
    request_id = client.submit("turn/start")
    stdout.write("")
    with pytest.raises(RpcConnectionError, match="separate observation"):
        client.wait_response(request_id)
    assert request_id in client._pending
    assert stdin.lines.qsize() == 1


def test_complete_oversized_page_does_not_poison_other_original_requests():
    stdin, stdout = Stream(), Stream()
    client = StdioAppServerClient(stdin, stdout, max_line_bytes=128, max_frame_bytes=4096)
    try:
        large = client.submit("thread/turns/list")
        original = client.submit("turn/start")
        stdout.write(json.dumps({"id": large, "result": {"text": "x" * 1000}}) + "\n")
        stdout.write(json.dumps({"id": original, "result": {"turn": {"id": "original"}}}) + "\n")
        with pytest.raises(RpcResponseTooLarge) as failure:
            client.wait_response(large)
        assert failure.value.request_id == large and failure.value.response_consumed
        assert large not in client._pending and client._failure is None
        assert client.wait_response(original)["result"]["turn"]["id"] == "original"
        smaller = client.submit("thread/turns/list", {"itemsView": "summary"})
        stdout.write(json.dumps({"id": smaller, "result": {"data": []}}) + "\n")
        assert client.wait_response(smaller)["result"]["data"] == []
        assert stdin.lines.qsize() == 3
    finally:
        stdout.write("")
        client._reader.join(5)


def test_hard_frame_limit_keeps_uncertain_start_identity():
    stdin, stdout = Stream(), Stream()
    client = StdioAppServerClient(stdin, stdout, max_line_bytes=128, max_frame_bytes=256)
    original = client.submit("turn/start")
    stdout.write('x' * 257)
    with pytest.raises(RpcConnectionError, match="hard bound"):
        client.wait_response(original)
    assert original in client._pending and stdin.lines.qsize() == 1
    client._reader.join(5)
