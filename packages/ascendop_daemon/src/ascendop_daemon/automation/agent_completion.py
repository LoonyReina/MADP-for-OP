"""Public typed-action completion; deployment compatibility intake stays upstream."""
from .agent_completion_core import AgentCompletionCore as AgentCompletionService
from .completion_facts import AgentCompletionError, OutcomeNormalizer

__all__ = ["AgentCompletionService", "AgentCompletionError", "OutcomeNormalizer"]
