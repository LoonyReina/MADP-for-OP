from __future__ import annotations

from pathlib import Path
from typing import Any

from ascendop_daemon.control_plane.route_reconciliation import (
    reconcile_waiting_test_requests,
)
from ascendop_daemon.control_plane.test_requests import generate_test_requests
from ascendop_daemon.workflow.queue_projection import reconcile_terminal_queue_rows


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
        self._queue_identity: tuple[int, int] | None = None
        self._routing_revision: int | None = None

    def run_once(self) -> dict[str, Any]:
        terminal_queue_projection = reconcile_terminal_queue_rows(
            self.root,
            self.database,
        )
        queue_identity = self._queue_fingerprint()
        routing_revision = self.database.routing_topology_revision()
        if queue_identity != self._queue_identity:
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
                package_root=(self.root / ".ascendop-work" / "flow-v3" / "packages"),
                code_generation=self.code_generation,
            )
            incidents = self.database.record_submit_intake_failures(
                list(report.get("errors") or []),
                scan_identity=queue_identity,
                code_generation=self.code_generation,
            )
            self._queue_identity = queue_identity
            self._routing_revision = routing_revision
            return {
                "state": "queue-scanned",
                "routing_revision": routing_revision,
                "intake_incidents": incidents,
                "terminal_queue_projection": terminal_queue_projection,
                **report,
            }
        if routing_revision != self._routing_revision:
            report = reconcile_waiting_test_requests(
                self.root,
                self.config,
                self.database,
                self.registry,
                code_generation=self.code_generation,
            )
            self._routing_revision = routing_revision
            return {
                "state": "routing-reconciled",
                "generated_count": 0,
                "routing_revision": routing_revision,
                "terminal_queue_projection": terminal_queue_projection,
                **report,
            }
        return {
            "state": "unchanged",
            "generated_count": 0,
            "routing_revision": routing_revision,
            "terminal_queue_projection": terminal_queue_projection,
        }

    def _queue_fingerprint(self) -> tuple[int, int]:
        try:
            stat = (self.root / "TestUtils" / "submit" / "queue.md").stat()
        except OSError:
            return (0, 0)
        return (int(stat.st_mtime_ns), int(stat.st_size))
