from __future__ import annotations

from pathlib import Path
from typing import Sequence

from ..provider import PreparedDriverRuntime
from .base import SubprocessAgentDriver, write_completion_schema


class CodexCliDriver(SubprocessAgentDriver):
    driver_id = "codex-cli"
    executable_names = ("codex", "codex.exe", "codex.cmd")
    isolated_features = (
        "apps",
        "browser_use",
        "enable_request_compression",
        "goals",
        "hooks",
        "image_generation",
        "memories",
        "multi_agent",
        "plugins",
        "recommended_plugins",
    )

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
        schema_path = run_root / "completion.schema.json"
        write_completion_schema(schema_path)
        base = [
            executable,
            "exec",
            "--json",
            "--sandbox",
            "workspace-write",
            "--skip-git-repo-check",
            "--output-schema",
            str(schema_path),
            "--output-last-message",
            str(completion_path),
            "-C",
            str(workspace),
        ]
        for feature in self.isolated_features:
            base.extend(["--disable", feature])
        if resume_session_id:
            base.extend(["resume", resume_session_id])
        base.append("-")
        return base
