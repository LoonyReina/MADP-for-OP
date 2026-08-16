from __future__ import annotations

SCHEMA_VERSION = 9
SYSTEM_EXPERIMENT_OPERATOR_ID = "__transport_experiment__"
SYSTEM_EXPERIMENT_GENERATION = "transport-experiment-v1"
BOOTSTRAP_CONTROL_PROBE_POLICY = "bootstrap-control-probe"
ACTIVE_ATTEMPT_STATES = {
    "prepared",
    "queued",
    "sent",
    "accepted",
    "host",
    "device-ready",
    "device",
    "postprocess",
    "returning",
    "uncertain",
    "retry-decision-pending",
}
OUTBOX_CLAIMABLE_STATES = {"pending", "retry"}
OUTBOX_ACTIVE_STATES = {
    "preparing",
    "claimed",
    "sending",
    "accepted",
    "uncertain",
    "returning",
    "decision-pending",
}
ACTIVE_NODE_STATES = {
    "starting",
    "discovering",
    "awaiting-acceptance",
    "ready",
    "degraded",
}


class ControlDatabaseError(RuntimeError):
    pass
