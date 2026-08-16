from __future__ import annotations

import json
import sys
import time
from pathlib import Path

from ascendop_agent_runner.drivers import DriverProbe
from ascendop_agent_runner.drivers.base import (
    SubprocessAgentDriver,
    _extract_session_id,
    completion_schema,
)
from ascendop_agent_runner.drivers.claude import ClaudeCodeDriver
from ascendop_agent_runner.drivers.codex import CodexCliDriver
from ascendop_agent_runner.drivers.kimi import KimiCodeDriver
from ascendop_agent_runner.provider import PreparedDriverRuntime


class _FailingDriver(SubprocessAgentDriver):
    driver_id = "failing-cli"

    def probe(self) -> DriverProbe:
        return DriverProbe(
            driver=self.driver_id,
            available=True,
            executable=sys.executable,
            executable_digest="a" * 64,
            version="test",
            capabilities=dict(self.capabilities),
        )

    def command(self, **kwargs):
        del kwargs
        return [
            sys.executable,
            "-c",
            "import sys; sys.stdin.buffer.read(); sys.exit(7)",
        ]


class _ClaudeAuthDriver(ClaudeCodeDriver):
    def probe(self) -> DriverProbe:
        return DriverProbe(
            driver=self.driver_id,
            available=True,
            executable=sys.executable,
            executable_digest="b" * 64,
            version="test",
            capabilities=dict(self.capabilities),
        )

    def command(self, **kwargs):
        del kwargs
        event = json.dumps(
            {
                "type": "system",
                "subtype": "api_retry",
                "attempt": 1,
                "max_retries": 10,
                "error_status": 401,
                "error": "authentication_failed",
                "session_id": "claude-auth-session",
            }
        )
        return [
            sys.executable,
            "-c",
            (
                "import sys,time; sys.stdin.buffer.read(); "
                f"print({event!r}, flush=True); time.sleep(20)"
            ),
        ]


def test_completion_schema_is_strict_for_codex_structured_outputs() -> None:
    schema = completion_schema()

    assert schema["additionalProperties"] is False
    assert set(schema["required"]) == set(schema["properties"])


def test_codex_command_writes_the_strict_schema(tmp_path: Path) -> None:
    runtime = PreparedDriverRuntime(environment={})
    command = CodexCliDriver().command(
        executable="codex",
        workspace=tmp_path,
        run_root=tmp_path,
        completion_path=tmp_path / "completion.json",
        resume_session_id="",
        prompt="secret prompt",
        provider_runtime=runtime,
    )

    schema_path = Path(command[command.index("--output-schema") + 1])
    written = json.loads(schema_path.read_text(encoding="utf-8"))
    assert written["additionalProperties"] is False
    assert written["properties"]["status"]["type"] == "string"
    assert command.count("--disable") == len(CodexCliDriver.isolated_features)
    for feature in CodexCliDriver.isolated_features:
        index = command.index(feature)
        assert command[index - 1] == "--disable"
    assert command[-1] == "-"


def test_claude_stream_json_print_mode_is_verbose(tmp_path: Path) -> None:
    runtime = PreparedDriverRuntime(environment={})
    command = ClaudeCodeDriver().command(
        executable="claude",
        workspace=tmp_path,
        run_root=tmp_path,
        completion_path=tmp_path / "completion.json",
        resume_session_id="",
        prompt="secret prompt",
        provider_runtime=runtime,
    )

    assert "-p" in command
    assert "--verbose" in command
    assert command[command.index("--output-format") + 1] == "stream-json"
    assert command[command.index("--setting-sources") + 1] == "project,local"


def test_kimi_uses_private_task_file_instead_of_prompt_argv(
    tmp_path: Path,
) -> None:
    private_root = tmp_path / "private"
    private_root.mkdir()
    prompt = "immutable secret action"
    runtime = PreparedDriverRuntime(
        environment={},
        private_root=private_root,
    )

    command = KimiCodeDriver().command(
        executable="kimi",
        workspace=tmp_path,
        run_root=tmp_path / "run",
        completion_path=tmp_path / "completion.json",
        resume_session_id="",
        prompt=prompt,
        provider_runtime=runtime,
    )

    assert prompt not in command
    assert "-" not in command
    assert "--yolo" not in command
    assert "--auto" not in command
    assert "--agent-file" in command
    assert (private_root / "immutable-task.md").read_text(encoding="utf-8") == prompt
    agent = (private_root / "ascendop-action-agent.md").read_text(
        encoding="utf-8"
    )
    assert "${base_prompt}" in agent
    assert str((private_root / "immutable-task.md").resolve()) in agent
    assert KimiCodeDriver().stdin_payload(prompt) == b""


def test_kimi_resume_reuses_session_without_rebinding_agent(
    tmp_path: Path,
) -> None:
    private_root = tmp_path / "private"
    private_root.mkdir()
    runtime = PreparedDriverRuntime(environment={}, private_root=private_root)

    command = KimiCodeDriver().command(
        executable="kimi",
        workspace=tmp_path,
        run_root=tmp_path / "reconcile",
        completion_path=tmp_path / "completion.json",
        resume_session_id="session-123",
        prompt="reconcile action",
        provider_runtime=runtime,
    )

    assert command[command.index("--session") + 1] == "session-123"
    assert "--agent-file" not in command
    assert (private_root / "immutable-task.md").read_text(
        encoding="utf-8"
    ) == "reconcile action"


def test_failed_process_is_classified_as_adapter_failure(tmp_path: Path) -> None:
    result = _FailingDriver().start(
        prompt="test prompt",
        workspace=tmp_path,
        run_root=tmp_path / "run",
        timeout_seconds=5,
        heartbeat=lambda: None,
    )

    assert result.status == "failed"
    assert result.exit_code == 7
    assert result.completion["failure_class"] == "agent-adapter"
    assert result.completion["exit_code"] == 7


def test_session_identity_is_recovered_from_stream_events(tmp_path: Path) -> None:
    events = tmp_path / "events.jsonl"
    events.write_text(
        '{"type":"thread.started","thread_id":"thread-123"}\n',
        encoding="utf-8",
    )

    assert _extract_session_id(events) == "thread-123"


def test_claude_auth_retry_is_terminated_without_internal_backoff(
    tmp_path: Path,
) -> None:
    started = time.monotonic()
    result = _ClaudeAuthDriver().start(
        prompt="test prompt",
        workspace=tmp_path,
        run_root=tmp_path / "run",
        timeout_seconds=15,
        heartbeat=lambda: None,
    )

    assert time.monotonic() - started < 5
    assert result.status == "failed"
    assert result.session_id == "claude-auth-session"
    assert result.completion["failure_class"] == "agent-auth"
    assert result.completion["error_status"] == 401
