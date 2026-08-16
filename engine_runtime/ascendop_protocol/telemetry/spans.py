from __future__ import annotations

from dataclasses import dataclass


class SpanContractError(ValueError):
    pass


@dataclass(frozen=True)
class ClockIdentity:
    host_id: str
    boot_id: str


@dataclass(frozen=True)
class SpanRecord:
    span_id: str
    parent_span_id: str | None
    clock: ClockIdentity
    monotonic_start_ns: int
    monotonic_end_ns: int
    utc_start: str
    utc_end: str

    def duration_ns(self) -> int:
        if self.monotonic_start_ns < 0 or self.monotonic_end_ns < self.monotonic_start_ns:
            raise SpanContractError("invalid host-local monotonic interval")
        return self.monotonic_end_ns - self.monotonic_start_ns

    def duration_between(self, other: "SpanRecord") -> int:
        if self.clock != other.clock:
            raise SpanContractError("cross-host or cross-boot duration subtraction is forbidden")
        return other.monotonic_start_ns - self.monotonic_end_ns
