from __future__ import annotations

from pathlib import Path
from typing import Any

from ascendop_daemon.control_plane.test_requests import generate_test_requests


class SubmitIntake:
    def __init__(
        self,
        *,
        root: Path,
        config: Any,
        database: Any,
        registry: Any,
        code_generation: str,
    ) -> None:
        self.root = root.resolve()
        self.config = config
        self.database = database
        self.registry = registry
        self.code_generation = code_generation
        self._fingerprint: tuple[int, int] | None = None

    def run_once(self) -> dict[str, Any]:
        fingerprint = self._queue_fingerprint()
        if fingerprint == self._fingerprint:
            return {"state": "unchanged", "generated_count": 0}
        report = generate_test_requests(
            self.root,
            self.config,
            self.database,
            self.registry,
            request_root=(
                self.root / ".ascendop-work" / "acceptance" / "test_requests"
            ),
            execution_profile=str(
                self.config.policy.get(
                    "test_engine_execution_profile",
                    "correctness-first-all-cases-v3",
                )
            ),
            route=True,
            package_root=self.root / ".ascendop-work" / "flow-v3" / "packages",
            code_generation=self.code_generation,
        )
        self._fingerprint = fingerprint
        return {"state": "scanned", **report}

    def _queue_fingerprint(self) -> tuple[int, int]:
        try:
            stat = (self.root / "TestUtils" / "submit" / "queue.md").stat()
        except OSError:
            return (0, 0)
        return (int(stat.st_mtime_ns), int(stat.st_size))
