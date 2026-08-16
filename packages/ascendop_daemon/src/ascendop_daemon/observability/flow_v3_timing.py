from __future__ import annotations

import ctypes
import hashlib
import os
import socket
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping


CLOCK_CONTRACT = "ascendop.clock.v3"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class Span:
    trace_id: str
    name: str
    actor: str
    request_id: str
    attempt_id: str
    stage_try: int = 0
    parent_span_id: str = ""
    resource: str = ""

    def __post_init__(self) -> None:
        self.span_id = uuid.uuid4().hex
        self.host = socket.gethostname()
        self.pid = os.getpid()
        self.boot_id = read_boot_id()
        self.started_at = utc_now_iso()
        self.started_monotonic_ns = time.monotonic_ns()
        self.finished_at = ""
        self.finished_monotonic_ns = 0

    def finish(self) -> dict[str, Any]:
        if not self.finished_monotonic_ns:
            self.finished_monotonic_ns = time.monotonic_ns()
            self.finished_at = utc_now_iso()
        return self.to_dict()

    def to_dict(self) -> dict[str, Any]:
        duration_ns = (
            max(0, self.finished_monotonic_ns - self.started_monotonic_ns)
            if self.finished_monotonic_ns
            else 0
        )
        return {
            "clock_contract": CLOCK_CONTRACT,
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_span_id": self.parent_span_id,
            "request_id": self.request_id,
            "attempt_id": self.attempt_id,
            "stage_try": self.stage_try,
            "name": self.name,
            "actor": self.actor,
            "resource": self.resource,
            "host": self.host,
            "boot_id": self.boot_id,
            "pid": self.pid,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "started_monotonic_ns": self.started_monotonic_ns,
            "finished_monotonic_ns": self.finished_monotonic_ns,
            "duration_ns": duration_ns,
        }


def read_boot_id() -> str:
    path = "/proc/sys/kernel/random/boot_id"
    try:
        with open(path, "r", encoding="ascii") as handle:
            value = handle.read().strip()
    except OSError:
        value = ""
    if not value and os.name == "nt":
        try:
            get_tick_count = ctypes.windll.kernel32.GetTickCount64
            get_tick_count.restype = ctypes.c_ulonglong
            boot_epoch_ms = (
                time.time_ns() // 1_000_000 - int(get_tick_count())
            )
            # System and monotonic clocks can differ by a few milliseconds
            # between processes. A ten-second boot bucket remains stable.
            boot_bucket = boot_epoch_ms // 10_000
            identity = f"{socket.gethostname()}:{boot_bucket}".encode("utf-8")
            value = "windows-" + hashlib.sha256(identity).hexdigest()[:24]
        except (AttributeError, OSError, TypeError, ValueError):
            value = ""
    return value or f"{socket.gethostname()}-unknown"


def summarize_spans(spans: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    by_clock: dict[tuple[str, str], list[tuple[int, int]]] = {}
    active_ns = 0
    resource_active_ns: dict[str, int] = {}
    incomplete = 0
    for raw in spans:
        start = int(raw.get("started_monotonic_ns", 0) or 0)
        finish = int(raw.get("finished_monotonic_ns", 0) or 0)
        if start <= 0 or finish < start:
            incomplete += 1
            continue
        duration = finish - start
        active_ns += duration
        resource = str(raw.get("resource") or "")
        resource_active_ns[resource] = resource_active_ns.get(resource, 0) + duration
        key = (
            str(raw.get("host") or ""),
            str(raw.get("boot_id") or ""),
        )
        by_clock.setdefault(key, []).append((start, finish))
    union_ns = sum(interval_union_ns(intervals) for intervals in by_clock.values())
    return {
        "clock_contract": CLOCK_CONTRACT,
        "span_count": sum(len(values) for values in by_clock.values()) + incomplete,
        "incomplete_span_count": incomplete,
        "active_time_seconds": round(active_ns / 1_000_000_000, 9),
        "host_local_wall_union_seconds": round(union_ns / 1_000_000_000, 9),
        "resource_active_seconds": {
            key: round(value / 1_000_000_000, 9)
            for key, value in sorted(resource_active_ns.items())
        },
        "cross_host_subtraction": "forbidden",
    }


def interval_union_ns(intervals: Iterable[tuple[int, int]]) -> int:
    ordered = sorted((start, finish) for start, finish in intervals if finish >= start)
    if not ordered:
        return 0
    total = 0
    current_start, current_finish = ordered[0]
    for start, finish in ordered[1:]:
        if start <= current_finish:
            current_finish = max(current_finish, finish)
            continue
        total += current_finish - current_start
        current_start, current_finish = start, finish
    return total + current_finish - current_start
