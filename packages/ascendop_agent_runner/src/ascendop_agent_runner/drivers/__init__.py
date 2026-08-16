from .base import AgentDriver, DriverProbe, DriverResult
from .claude import ClaudeCodeDriver
from .codex import CodexCliDriver
from .kimi import KimiCodeDriver
from ..provider import AgentProviderProfile


def default_drivers(
    provider_profile: AgentProviderProfile | None = None,
) -> tuple[AgentDriver, ...]:
    return (
        CodexCliDriver(provider_profile),
        ClaudeCodeDriver(provider_profile),
        KimiCodeDriver(provider_profile),
    )


__all__ = [
    "AgentDriver",
    "ClaudeCodeDriver",
    "CodexCliDriver",
    "DriverProbe",
    "DriverResult",
    "KimiCodeDriver",
    "default_drivers",
]
