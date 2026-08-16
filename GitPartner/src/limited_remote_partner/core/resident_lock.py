from __future__ import annotations

import json
import os
import shutil
import time
import uuid
from pathlib import Path

from limited_remote_partner.core.process_utils import process_start_token


class ResidentRoleLock:
    def __init__(self, state_dir: Path, role: str, *, poll_seconds: float = 2.0) -> None:
        self.state_dir = state_dir.resolve()
        self.role = role
        self.poll_seconds = max(0.1, float(poll_seconds))
        self.path = self.state_dir / f"resident-{role}.lock.d"
        self.owner_path = self.path / "owner.json"
        self.pid = os.getpid()
        self.start_token = process_start_token(self.pid)
        self._owned = False

    def acquire(self, *, wait: bool = True) -> bool:
        last_owner: tuple[int, str] | None = None
        while True:
            if self._try_acquire():
                self._owned = True
                print(
                    f"GITPARTNER_RESIDENT_LOCK_ACQUIRED role={self.role} pid={self.pid}",
                    flush=True,
                )
                return True
            owner = self.owner()
            owner_key = (int(owner.get("pid") or 0), str(owner.get("start_token") or ""))
            if owner_key != last_owner:
                print(
                    "GITPARTNER_RESIDENT_STANDBY "
                    f"role={self.role} pid={self.pid} owner_pid={owner_key[0]}",
                    flush=True,
                )
                last_owner = owner_key
            if not wait:
                return False
            time.sleep(self.poll_seconds)

    def owner(self) -> dict[str, object]:
        try:
            raw = json.loads(self.owner_path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return {}
        return raw if isinstance(raw, dict) else {}

    def release(self) -> None:
        if not self._owned:
            return
        owner = self.owner()
        if (
            int(owner.get("pid") or 0) == self.pid
            and str(owner.get("start_token") or "") == self.start_token
        ):
            shutil.rmtree(self.path, ignore_errors=True)
        self._owned = False

    def _try_acquire(self) -> bool:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        try:
            self.path.mkdir()
        except FileExistsError:
            owner = self.owner()
            owner_pid = int(owner.get("pid") or 0)
            owner_token = str(owner.get("start_token") or "")
            if owner_pid == self.pid and owner_token == self.start_token:
                return True
            if owner_pid and process_start_token(owner_pid) == owner_token:
                return False
            stale = self.state_dir / (
                f".{self.path.name}.stale.{self.pid}.{uuid.uuid4().hex}"
            )
            try:
                self.path.replace(stale)
            except (FileNotFoundError, FileExistsError, PermissionError, OSError):
                return False
            shutil.rmtree(stale, ignore_errors=True)
            return self._try_acquire()
        payload = {
            "role": self.role,
            "pid": self.pid,
            "start_token": self.start_token,
            "acquired_at_epoch": time.time(),
        }
        temporary = self.path / f".owner.{self.pid}.tmp"
        temporary.write_text(
            json.dumps(payload, ensure_ascii=True, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(self.owner_path)
        return True

    def __enter__(self) -> ResidentRoleLock:
        self.acquire()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.release()
