from __future__ import annotations

import hashlib
import http.client
import json
import sqlite3
import threading
from pathlib import Path
from urllib.request import Request, urlopen

from ascendop_control.api import ControlApiServer
from ascendop_control.api.discovery import publish_endpoint, retire_endpoint
from ascendop_control.storage import (
    CONTROL_EXTENSION_SQL,
    CONTROL_SCHEMA_VERSION,
    ControlStore,
)


def test_control_api_discovery_is_atomic_and_owner_fenced(tmp_path: Path) -> None:
    path = tmp_path / "control-api" / "endpoint.json"
    published = publish_endpoint(
        path,
        host="127.0.0.1",
        port=43123,
        generation="release-a",
        pid=41,
    )

    assert json.loads(path.read_text(encoding="ascii")) == published
    assert retire_endpoint(path, generation="release-b", pid=41) is False
    assert retire_endpoint(path, generation="release-a", pid=42) is False
    assert path.is_file()
    assert retire_endpoint(path, generation="release-a", pid=41) is True
    assert not path.exists()


def test_rest_commands_and_sse_resume_use_same_database(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.register_agent_pool(_agent_pool())
    store.register_agent(
        _agent_registration(),
        manager_runner_id="ide-adapter",
        lease_seconds=60,
    )
    store.bind_agent(
        operator_id="hard-swish",
        role="solver",
        agent_id="codex-ide:hard-swish",
    )
    token = "operator-secret"
    digest = hashlib.sha256(token.encode()).hexdigest()
    server = ControlApiServer(
        ("127.0.0.1", 0),
        store=store,
        token_capabilities={digest: "operator"},
        sse_poll_seconds=0.1,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    port = server.server_address[1]
    try:
        existing_events = store.control_events_after(0)
        resume_after = int(existing_events[-1]["sequence"])
        store.create_agent_action(_agent_action(), _agent_snapshot())
        claim = store.claim_agent_action(
            runner_id="ide-adapter",
            boot_id="boot-1",
            lease_seconds=60,
        )
        assert claim is not None
        command = {
            "schema": "ascendop.control-command.v1",
            "command_id": "command-api-1",
            "idempotency_key": "command-api-key-1",
            "command_kind": "endpoint.drain",
            "actor_id": "test-user",
            "required_capability": "operator",
            "parameters": {"endpoint_id": "endpoint-a"},
            "created_at": "2026-08-08T00:00:00+00:00",
        }
        request = Request(
            f"http://127.0.0.1:{port}/api/v1/commands",
            method="POST",
            data=json.dumps(command).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )
        with urlopen(request, timeout=5) as response:
            assert response.status == 202
            assert json.loads(response.read())["state"] == "queued"

        agents = Request(
            f"http://127.0.0.1:{port}/api/v1/agents",
            headers={"Authorization": f"Bearer {token}"},
        )
        with urlopen(agents, timeout=5) as response:
            payload = json.loads(response.read())
            assert payload["resource_type"] == "agents"
            assert payload["attributes"]["count"] == 1
            assert payload["attributes"]["items"][0]["agent_id"] == (
                "codex-ide:hard-swish"
            )

        for resource in ("agent-pools", "agent-actions", "agent-leases"):
            query = Request(
                f"http://127.0.0.1:{port}/api/v1/{resource}",
                headers={"Authorization": f"Bearer {token}"},
            )
            with urlopen(query, timeout=5) as response:
                payload = json.loads(response.read())
                assert payload["resource_type"] == resource
                assert payload["attributes"]["count"] == 1
                item = payload["attributes"]["items"][0]
                if resource == "agent-actions":
                    assert item["state"] == "claimed"
                    assert item["action"]["output_contracts"] == []
                elif resource == "agent-leases":
                    assert item["state"] == "active"
                    assert item["lease"]["action_id"] == "action-live"
                else:
                    assert item["pool_id"] == "codex-ide-solver"

        for resource, count in (
            ("operator-workflows", 0),
            ("workflow-traces", 1),
        ):
            query = Request(
                f"http://127.0.0.1:{port}/api/v1/{resource}",
                headers={"Authorization": f"Bearer {token}"},
            )
            with urlopen(query, timeout=5) as response:
                payload = json.loads(response.read())
                assert payload["resource_type"] == resource
                assert payload["attributes"]["count"] == count

        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        connection.request(
            "GET",
            "/api/v1/events",
            headers={
                "Authorization": f"Bearer {token}",
                "Last-Event-ID": str(resume_after),
            },
        )
        response = connection.getresponse()
        assert response.status == 200
        lines: list[str] = []
        for _ in range(64):
            line = response.readline().decode("utf-8").strip()
            lines.append(line)
            if (
                any("agent-action-created" in value for value in lines)
                and any("agent-action-claimed" in value for value in lines)
                and "control-command-created" in line
            ):
                break
        replayed_ids = [
            int(line.removeprefix("id: "))
            for line in lines
            if line.startswith("id: ")
        ]
        assert replayed_ids
        assert all(sequence > resume_after for sequence in replayed_ids)
        assert any("agent-action-created" in line for line in lines)
        assert any("agent-action-claimed" in line for line in lines)
        assert any("control-command-created" in line for line in lines)
        connection.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_api_rejects_missing_token(tmp_path: Path) -> None:
    store = _store(tmp_path)
    server = ControlApiServer(
        ("127.0.0.1", 0),
        store=store,
        token_capabilities={},
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = http.client.HTTPConnection(
            "127.0.0.1", server.server_address[1], timeout=5
        )
        connection.request("GET", "/api/v1/agents")
        response = connection.getresponse()
        assert response.status == 401
        connection.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _store(tmp_path: Path) -> ControlStore:
    path = tmp_path / "control.sqlite3"
    conn = sqlite3.connect(path)
    conn.executescript(
        "CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);"
        f"INSERT INTO metadata(key,value) VALUES('schema_version','{CONTROL_SCHEMA_VERSION}');"
        "CREATE TABLE control_events(sequence INTEGER PRIMARY KEY AUTOINCREMENT,"
        "event_at TEXT NOT NULL,event_type TEXT NOT NULL,entity_type TEXT NOT NULL,"
        "entity_id TEXT NOT NULL,payload_json TEXT NOT NULL);"
        + CONTROL_EXTENSION_SQL
    )
    conn.commit()
    conn.close()
    return ControlStore(path)


def _agent_pool() -> dict[str, object]:
    return {
        "schema": "ascendop.agent-pool.v1",
        "pool_id": "codex-ide-solver",
        "enabled": True,
        "roles": ["solver"],
        "drivers": ["codex-ide-task"],
        "required_capabilities": {"structured_output": True},
        "priority": 100,
        "registration_generation": "pool-generation-1",
    }


def _agent_registration() -> dict[str, object]:
    return {
        "schema": "ascendop.agent-registration.v1",
        "agent_id": "codex-ide:hard-swish",
        "driver": "codex-ide-task",
        "executable": "codex-ide-task://hard-swish",
        "executable_digest": "a" * 64,
        "observed_version": "codex-ide-adapter-v1",
        "registration_generation": "b" * 64,
        "capabilities": {
            "stream_json": False,
            "resume": True,
            "structured_output": True,
        },
        "observed_at": "2026-08-10T00:00:00+00:00",
    }


def _agent_action() -> dict[str, object]:
    return {
        "schema": "ascendop.agent-action.v1",
        "action_id": "action-live",
        "idempotency_key": "action-live-key",
        "iteration_id": "iteration-live",
        "campaign": "august",
        "operator_id": "hard-swish",
        "agent_pool_id": "codex-ide-solver",
        "role": "solver",
        "workflow_epoch": "epoch-1",
        "producer_generation": "release-1",
        "board_revision": "board-1",
        "board_digest": "c" * 64,
        "runbook_path": "operators/august/HardSwish/RUNBOOK.md",
        "runbook_digest": "d" * 64,
        "origin_workspace": "operators_workspace/HardSwish",
        "candidate_version": "HardSwish_V1_1",
        "candidate_identity": {"execution_source_digest": "e" * 64},
        "write_scope": ["op_kernel/hard_swish.cpp"],
        "output_contracts": [],
        "tool_budget": {"max_turn_seconds": 900},
        "created_at": "2026-08-10T00:00:00+00:00",
    }


def _agent_snapshot() -> dict[str, object]:
    return {
        "schema": "ascendop.agent-context-snapshot.v1",
        "snapshot_id": "snapshot-live",
        "iteration_id": "iteration-live",
        "operator_id": "hard-swish",
        "role": "solver",
        "board_revision": "board-1",
        "board_digest": "c" * 64,
        "candidate_identity": {"execution_source_digest": "e" * 64},
        "gate": {"owner": "solver"},
        "recent_results": [],
        "official_evidence": [],
        "open_hypotheses": [],
        "permitted_operations": ["edit-source", "complete"],
        "created_at": "2026-08-10T00:00:00+00:00",
    }
