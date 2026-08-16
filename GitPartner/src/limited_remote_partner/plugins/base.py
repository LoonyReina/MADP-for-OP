from __future__ import annotations

from pathlib import Path
from typing import Protocol

from limited_remote_partner.core.request import ExecutionRequest


class RequestPlugin(Protocol):
    """Extension point for scenario-specific input parsers/executors."""

    name: str

    def should_run(self, changed_paths: list[str]) -> bool:
        """Return true when this plugin should react to changed repo paths."""

    def parse_request(self, repo_dir: Path, trigger_ref: str) -> ExecutionRequest:
        """Build an execution request from input files."""
