from __future__ import annotations

import json
from pathlib import Path
from typing import Sequence

from ..provider import PreparedDriverRuntime
from .base import SubprocessAgentDriver, completion_schema


class ClaudeCodeDriver(SubprocessAgentDriver):
    driver_id = "claude-code-cli"
    executable_names = ("claude", "claude.exe", "claude.cmd")

    def stream_failure(self, raw_output_path: Path) -> dict[str, object] | None:
        if not raw_output_path.is_file():
            return None
        try:
            lines = raw_output_path.read_text(
                encoding="utf-8", errors="replace"
            ).splitlines()
        except OSError:
            return None
        for line in reversed(lines):
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(event, dict) or event.get("subtype") != "api_retry":
                continue
            status = event.get("error_status")
            error = str(event.get("error") or "")
            if status in {401, 403} or error == "authentication_failed":
                return {
                    "status": "failed",
                    "failure_class": "agent-auth",
                    "error": (
                        f"{self.driver_id} authentication failed before the Agent "
                        "turn started"
                    ),
                    "error_status": status,
                    "provider_error": error,
                }
        return None

    def command(
        self,
        *,
        executable: str,
        workspace: Path,
        run_root: Path,
        completion_path: Path,
        resume_session_id: str,
        prompt: str,
        provider_runtime: PreparedDriverRuntime,
    ) -> Sequence[str]:
        del prompt, provider_runtime
        command = [
            executable,
            "-p",
            "--input-format",
            "text",
            "--output-format",
            "stream-json",
            "--verbose",
            "--json-schema",
            json.dumps(completion_schema(), separators=(",", ":")),
            "--permission-mode",
            "acceptEdits",
            "--setting-sources",
            "project,local",
        ]
        if resume_session_id:
            command.extend(["--resume", resume_session_id])
        return command
