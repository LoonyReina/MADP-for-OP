from __future__ import annotations

import json
from pathlib import Path

from ascendop_daemon.core.models import BoardSnapshot, DaemonPlan, GateDecision, utc_now_iso
from ascendop_daemon.observability.audit_serialization import serialize_decision, serialize_transport


class AuditLog:
    def __init__(self, root: Path) -> None:
        self.state_dir = root / "TestUtils" / "tester_daemon"

    def append(
        self,
        event: str,
        snapshot: BoardSnapshot,
        decisions: tuple[GateDecision, ...],
        plan: DaemonPlan,
        mode: str,
        resource_leases: tuple[dict[str, object], ...] = (),
        action_liveness: dict[str, object] | None = None,
    ) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        record = {
            "time": utc_now_iso(),
            "event": event,
            "mode": mode,
            "captured_at": snapshot.captured_at,
            "selected": serialize_decision(plan.selected),
            "decisions": [serialize_decision(d) for d in decisions],
            "transport": [serialize_transport(obs) for obs in snapshot.transport],
            "scheduler": plan.scheduler_state,
            "resource_leases": list(resource_leases),
            "action_liveness": action_liveness or {},
        }
        with (self.state_dir / "events.jsonl").open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
