from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
import time
import uuid
from contextlib import AbstractContextManager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO, Iterable

from limited_remote_partner.core.process_utils import hidden_subprocess_kwargs
from limited_remote_partner.core.atomic_file import (
    atomic_write_json,
    read_json_object,
    unlink_file,
)


SHARED_RESOURCE_NAMES = {"npu", "performance-measurement"}
RESOURCE_ORDER = {"npu": 0, "performance-measurement": 1}
DEFAULT_TIMEOUT_SECONDS = 1800.0
DEFAULT_HEARTBEAT_SECONDS = 1.0


class SharedResourceLeaseError(RuntimeError):
    pass


class SharedResourceLeaseTimeout(SharedResourceLeaseError):
    pass


class SharedResourceLeaseSet(AbstractContextManager["SharedResourceLeaseSet"]):
    """Cross-process resource ownership backed by kernel advisory locks.

    The lock file is durable but ownership is not: the kernel releases the
    advisory lock when the holder process exits. JSON files are observability
    only and are never trusted as ownership.
    """

    def __init__(
        self,
        root: Path,
        resources: Iterable[str],
        *,
        holder_kind: str,
        request_id: str = "",
        engine_job_id: str = "",
        attempt_id: str = "",
        stage: str = "",
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        heartbeat_seconds: float = DEFAULT_HEARTBEAT_SECONDS,
    ) -> None:
        normalized = sorted(
            {str(item) for item in resources if is_shared_resource(str(item))},
            key=resource_sort_key,
        )
        self.root = root.resolve()
        self.resources = normalized
        self.timeout_seconds = max(0.0, float(timeout_seconds))
        self.heartbeat_seconds = max(0.1, float(heartbeat_seconds))
        self.holder_id = uuid.uuid4().hex
        self.owner = {
            "holder_id": self.holder_id,
            "holder_kind": str(holder_kind),
            "request_id": str(request_id),
            "engine_job_id": str(engine_job_id),
            "attempt_id": str(attempt_id),
            "stage": str(stage),
            "pid": os.getpid(),
        }
        self.handles: list[tuple[str, BinaryIO]] = []
        self.acquired_at = ""
        self.wait_seconds = 0.0
        self._stop = threading.Event()
        self._heartbeat: threading.Thread | None = None

    def __enter__(self) -> "SharedResourceLeaseSet":
        self.acquire()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.release()

    def acquire(self) -> None:
        if not self.resources:
            return
        self.root.mkdir(parents=True, exist_ok=True)
        started = time.monotonic()
        deadline = started + self.timeout_seconds
        try:
            for resource in self.resources:
                handle = open_lock_file(self.root / f"{resource_file_stem(resource)}.lock")
                while True:
                    if try_lock(handle):
                        self.handles.append((resource, handle))
                        break
                    if time.monotonic() >= deadline:
                        handle.close()
                        raise SharedResourceLeaseTimeout(
                            f"shared resource lease timeout: {resource}"
                        )
                    time.sleep(min(0.05, max(0.001, deadline - time.monotonic())))
            self.wait_seconds = max(0.0, time.monotonic() - started)
            self.acquired_at = utc_now()
            self._write_owners()
            self._heartbeat = threading.Thread(
                target=self._heartbeat_loop,
                name=f"shared-lease-{self.holder_id[:8]}",
                daemon=True,
            )
            self._heartbeat.start()
        except Exception:
            self.release()
            raise

    def release(self) -> None:
        self._stop.set()
        if self._heartbeat is not None:
            self._heartbeat.join(timeout=max(1.0, self.heartbeat_seconds * 2.0))
            self._heartbeat = None
        for resource, _handle in self.handles:
            owner_path = self.root / f"{resource_file_stem(resource)}.owner.json"
            owner = read_json(owner_path)
            if owner.get("holder_id") == self.holder_id:
                unlink_file(owner_path, missing_ok=True)
        for _resource, handle in reversed(self.handles):
            try:
                unlock(handle)
            finally:
                handle.close()
        self.handles.clear()

    def _heartbeat_loop(self) -> None:
        while not self._stop.wait(self.heartbeat_seconds):
            self._write_owners()

    def _write_owners(self) -> None:
        heartbeat_at = utc_now()
        for resource, _handle in self.handles:
            atomic_write_json(
                self.root / f"{resource_file_stem(resource)}.owner.json",
                {
                    **self.owner,
                    "resource": resource,
                    "resources": self.resources,
                    "acquired_at": self.acquired_at,
                    "heartbeat_at": heartbeat_at,
                    "wait_seconds": round(self.wait_seconds, 6),
                },
            )


def shared_lease_root_for_engine(engine_root: Path) -> Path:
    configured = os.environ.get("ASCENDOP_SHARED_LEASE_ROOT", "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    return engine_root.resolve().parent / ".ascendop_test_leases"


def shared_lease_snapshot(root: Path) -> list[dict[str, Any]]:
    root = root.resolve()
    rows: list[dict[str, Any]] = []
    resources = set(SHARED_RESOURCE_NAMES)
    if root.exists():
        for owner_path in root.glob("*.owner.json"):
            owner = read_json(owner_path)
            resource = str(owner.get("resource") or "")
            if is_shared_resource(resource):
                resources.add(resource)
    for resource in sorted(resources, key=resource_sort_key):
        stem = resource_file_stem(resource)
        lock_path = root / f"{stem}.lock"
        owner_path = root / f"{stem}.owner.json"
        owner = read_json(owner_path)
        active = lock_path.exists() and lock_is_held(lock_path)
        if owner or active:
            rows.append(
                {
                    "resource": resource,
                    "active": active,
                    "stale_metadata": bool(owner) and not active,
                    **owner,
                }
            )
    return rows


def is_shared_resource(resource: str) -> bool:
    base, separator, device_id = str(resource).partition(":")
    if base not in SHARED_RESOURCE_NAMES:
        return False
    if not separator:
        return True
    return bool(device_id) and all(
        character.isalnum() or character in "._-" for character in device_id
    )


def resource_sort_key(resource: str) -> tuple[int, str]:
    base = str(resource).split(":", 1)[0]
    return RESOURCE_ORDER.get(base, len(RESOURCE_ORDER)), str(resource)


def resource_file_stem(resource: str) -> str:
    if not is_shared_resource(resource):
        raise SharedResourceLeaseError(f"invalid shared resource: {resource}")
    return str(resource).replace(":", "__")


def open_lock_file(path: Path) -> BinaryIO:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+b")
    handle.seek(0, os.SEEK_END)
    if handle.tell() == 0:
        handle.write(b"\0")
        handle.flush()
    handle.seek(0)
    return handle


def try_lock(handle: BinaryIO) -> bool:
    try:
        if os.name == "nt":
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except (BlockingIOError, OSError):
        return False


def unlock(handle: BinaryIO) -> None:
    if os.name == "nt":
        import msvcrt

        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
    else:
        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def lock_is_held(path: Path) -> bool:
    handle = open_lock_file(path)
    try:
        acquired = try_lock(handle)
        if acquired:
            unlock(handle)
            return False
        return True
    finally:
        handle.close()


def read_json(path: Path) -> dict[str, Any]:
    try:
        data = read_json_object(path)
    except (FileNotFoundError, json.JSONDecodeError, OSError, ValueError):
        return {}
    return data


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def run_with_leases(args: argparse.Namespace) -> int:
    command = list(args.command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        raise SharedResourceLeaseError("lease run requires a command after --")
    with SharedResourceLeaseSet(
        args.root,
        args.lock,
        holder_kind=args.holder_kind,
        request_id=args.request_id,
        engine_job_id=args.engine_job_id,
        attempt_id=args.attempt_id,
        stage=args.stage,
        timeout_seconds=args.timeout_seconds,
        heartbeat_seconds=args.heartbeat_seconds,
    ):
        completed = subprocess.run(command, check=False, **hidden_subprocess_kwargs())
        return int(completed.returncode)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a command under shared resource leases")
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--lock", action="append", default=[])
    parser.add_argument("--holder-kind", required=True)
    parser.add_argument("--request-id", default="")
    parser.add_argument("--engine-job-id", default="")
    parser.add_argument("--attempt-id", default="")
    parser.add_argument("--stage", default="")
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--heartbeat-seconds", type=float, default=DEFAULT_HEARTBEAT_SECONDS)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        return run_with_leases(build_parser().parse_args(argv))
    except SharedResourceLeaseTimeout as exc:
        print(str(exc), file=sys.stderr)
        return 75
    except SharedResourceLeaseError as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
