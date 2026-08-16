from __future__ import annotations

from limited_remote_partner.core.config import ErrorBackoffConfig


class LoopErrorBackoff:
    def __init__(self, config: ErrorBackoffConfig) -> None:
        self.config = config
        self.failures = 0

    def record_failure(self) -> float:
        delay = self.config.initial_seconds * (self.config.multiplier ** self.failures)
        self.failures += 1
        return min(delay, self.config.max_seconds)

    def reset(self) -> None:
        self.failures = 0
