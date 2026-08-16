from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from ascendop_daemon.core.models import DaemonConfig, GateDecision, utc_now_iso
from ascendop_daemon.runtime.locking import process_alive
from ascendop_daemon.automation.worker_liveness import worker_process_status


DEFAULT_LEASE_TTL_SECONDS = 3600


class ResourceManager:
    def __init__(self, root: Path, config: DaemonConfig) -> None:
        self.root = root
        self.config = config
        self.state_dir = root / "TestUtils" / "tester_daemon"
        self.leases_path = self.state_dir / "leases.json"

    def read_leases(self) -> list[dict[str, Any]]:
        data = self._read_state()
        leases = data.get("leases", [])
        return [lease for lease in leases if isinstance(lease, dict)]

    def write_leases(self, leases: list[dict[str, Any]]) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "updated_at": utc_now_iso(),
            "leases": leases,
        }
        self.leases_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    def prune_expired(self) -> list[dict[str, Any]]:
        now = datetime.now(timezone.utc)
        active = [lease for lease in self.read_leases() if not lease_expired(self.root, lease, now)]
        self.write_leases(active)
        return active

    def enabled_resources(self) -> list[dict[str, Any]]:
        resources = [resource for resource in self.config.resources if resource.get("enabled", True)]
        if resources:
            return resources
        return [{"id": self.config.transport or "default", "type": self.config.transport or "generic", "capacity": 1}]

    def acquire(self, decision: GateDecision) -> dict[str, Any] | None:
        resources = self.enabled_resources()
        if not resources:
            return None
        leases = self.prune_expired()
        for resource in resources:
            resource_id = str(resource.get("id") or resource.get("type") or "resource")
            capacity = int(resource.get("capacity", 1) or 1)
            held = [lease for lease in leases if lease.get("resource_id") == resource_id]
            if len(held) >= capacity:
                continue
            now = datetime.now(timezone.utc)
            ttl_seconds = int(
                self.config.policy.get("resource_lease_ttl_seconds", DEFAULT_LEASE_TTL_SECONDS)
                or DEFAULT_LEASE_TTL_SECONDS
            )
            lease = {
                "resource_id": resource_id,
                "resource_type": str(resource.get("type", "")),
                "action_id": decision.action_id,
                "op": decision.row.op,
                "gate_stage": decision.row.gate_stage,
                "pid": os.getpid(),
                "acquired_at": now.isoformat(timespec="seconds"),
                "expires_at": (now + timedelta(seconds=ttl_seconds)).isoformat(timespec="seconds"),
            }
            leases.append(lease)
            self.write_leases(leases)
            return lease
        raise RuntimeError("no daemon resource capacity available")

    def release(self, lease: dict[str, Any] | None) -> None:
        if not lease:
            return
        action_id = lease.get("action_id")
        resource_id = lease.get("resource_id")
        leases = [
            current
            for current in self.read_leases()
            if not (
                current.get("action_id") == action_id
                and current.get("resource_id") == resource_id
            )
        ]
        self.write_leases(leases)

    def _read_state(self) -> dict[str, Any]:
        if not self.leases_path.exists():
            return {}
        try:
            data = json.loads(self.leases_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return data if isinstance(data, dict) else {}


def lease_expired(root: Path | None, lease: dict[str, Any], now: datetime) -> bool:
    pid = int(lease.get("pid", 0) or 0)
    if pid > 0 and not process_alive(pid):
        action_id = str(lease.get("action_id", "") or "")
        if root is None or not action_id:
            return True
        if not worker_process_status(root, action_id, {"pid": pid}).active:
            return True
    raw = str(lease.get("expires_at", ""))
    try:
        expires_at = datetime.fromisoformat(raw)
    except ValueError:
        return True
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    return expires_at <= now
