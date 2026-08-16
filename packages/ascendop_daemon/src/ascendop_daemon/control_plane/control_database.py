from __future__ import annotations

from pathlib import Path

from ascendop_control import V4ControlRepository

from ascendop_daemon.storage.control_types import (
    ACTIVE_ATTEMPT_STATES,
    ACTIVE_NODE_STATES,
    BOOTSTRAP_CONTROL_PROBE_POLICY,
    OUTBOX_ACTIVE_STATES,
    OUTBOX_CLAIMABLE_STATES,
    SCHEMA_VERSION,
    SYSTEM_EXPERIMENT_GENERATION,
    SYSTEM_EXPERIMENT_OPERATOR_ID,
    ControlDatabaseError,
)
from ascendop_daemon.storage.control_validation import canonical_json, utc_now
from ascendop_daemon.storage.repositories import (
    AutomationServiceRepository,
    ManagementControlRepository,
    NodeLifecycleRepository,
    PreparationRetryRepository,
    RequestRoutingRepository,
    RequestPreparationRepository,
    RetryDecisionRepository,
    SchemaRegistryRepository,
    TransactionRepository,
    TransportRepository,
    TransportReturnRepository,
    WorkflowActionRepository,
)


class ControlDatabase(
    SchemaRegistryRepository,
    AutomationServiceRepository,
    RequestRoutingRepository,
    RequestPreparationRepository,
    PreparationRetryRepository,
    RetryDecisionRepository,
    TransportReturnRepository,
    TransportRepository,
    NodeLifecycleRepository,
    TransactionRepository,
    WorkflowActionRepository,
    ManagementControlRepository,
    V4ControlRepository,
):
    """Transactional facade over the Flow V4 control-plane repositories."""

    def __init__(self, path: Path) -> None:
        self.path = path.resolve()


__all__ = [
    "ACTIVE_ATTEMPT_STATES",
    "ACTIVE_NODE_STATES",
    "BOOTSTRAP_CONTROL_PROBE_POLICY",
    "ControlDatabase",
    "ControlDatabaseError",
    "OUTBOX_ACTIVE_STATES",
    "OUTBOX_CLAIMABLE_STATES",
    "SCHEMA_VERSION",
    "SYSTEM_EXPERIMENT_GENERATION",
    "SYSTEM_EXPERIMENT_OPERATOR_ID",
    "canonical_json",
    "utc_now",
]
