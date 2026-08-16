from __future__ import annotations

from pathlib import Path
from typing import Sequence

from ..provider import PreparedDriverRuntime
from .base import SubprocessAgentDriver


_NEW_ACTION_PROMPT = (
    "Execute the immutable AscendOP action named in your system instructions. "
    "Return the requested terminal JSON object when the action is complete."
)
_RESUME_ACTION_PROMPT = (
    "Continue the same immutable AscendOP action from its private task file. "
    "Reconcile prior work, then return the requested terminal JSON object."
)


class KimiCodeDriver(SubprocessAgentDriver):
    driver_id = "kimi-code-cli"
    executable_names = ("kimi", "kimi.exe", "kimi.cmd", "kimi-cli")
    capabilities = {
        "stream_json": True,
        "resume": True,
        "structured_output": False,
    }
    requires_private_runtime = True

    def stdin_payload(self, prompt: str) -> bytes:
        del prompt
        return b""

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
        del completion_path
        task_path = provider_runtime.write_private_text("immutable-task.md", prompt)
        skills_path = provider_runtime.private_root / "skills"
        skills_path.mkdir(parents=True, exist_ok=True)
        command = [
            executable,
            "--prompt",
            _RESUME_ACTION_PROMPT if resume_session_id else _NEW_ACTION_PROMPT,
            "--output-format",
            "stream-json",
            "--skills-dir",
            str(skills_path),
            "--add-dir",
            str(provider_runtime.private_root),
        ]
        if resume_session_id:
            command.extend(["--session", resume_session_id])
        else:
            agent_path = provider_runtime.write_private_text(
                "ascendop-action-agent.md",
                _agent_profile(task_path=task_path, workspace=workspace),
            )
            command.extend(["--agent-file", str(agent_path)])
        return command


def _agent_profile(*, task_path: Path, workspace: Path) -> str:
    return (
        "---\n"
        "name: ascendop-action\n"
        "description: Execute one immutable AscendOP workflow action\n"
        "tools:\n"
        "  - Read\n"
        "  - Write\n"
        "  - Edit\n"
        "  - Grep\n"
        "  - Glob\n"
        "  - Bash\n"
        "subagents: []\n"
        "---\n"
        "${base_prompt}\n\n"
        "# Immutable AscendOP action\n\n"
        f"Before doing anything else, read `{task_path.resolve()}` in full.\n"
        f"The only writable operator workspace is `{workspace.resolve()}`.\n"
        "Treat the task file as the complete immutable user request. Do not "
        "ask interactive questions, start subagents, schedule background work, "
        "or use network tools. Do not expose the task file path or contents in "
        "process arguments. Complete the task in this turn and end with exactly "
        "one JSON object containing status, summary, and artifacts.\n"
    )
