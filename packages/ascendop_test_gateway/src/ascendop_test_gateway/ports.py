from __future__ import annotations

from typing import Protocol
from pathlib import Path

from .contracts import PreparedBundle, StandaloneTestRequest, TransportReceipt, TransportStatus


class BundleStorePort(Protocol):
    """Trusted domain materialization; transport never creates its own oracle."""
    def prepare(self, request: StandaloneTestRequest, runs_root: Path) -> PreparedBundle: ...

    def load(self, run_dir: Path) -> PreparedBundle: ...


class UnsupportedTransportOperation(RuntimeError):
    pass


class TransportRequestError(ValueError):
    """A deterministic local request error raised before transport publication."""


class TransportAdmissionError(RuntimeError):
    """A deterministic endpoint rejection observed after publication."""

    def __init__(self, message: str, *, details: dict | None = None) -> None:
        super().__init__(message)
        self.details = dict(details or {})


class GPTransportPort(Protocol):
    def preflight(self, bundle: PreparedBundle) -> None: ...

    def submit(self, bundle: PreparedBundle) -> TransportReceipt: ...

    def reconcile(self, bundle: PreparedBundle) -> TransportReceipt | None: ...

    def status(self, receipt: TransportReceipt) -> TransportStatus: ...

    # Observation/cancel never ACK. Only a durable local acceptance authorizes
    # this independent operation; managed business continuation is an upper gate.
    def acknowledge(self, receipt: TransportReceipt, *, event_id: str) -> bool: ...

    def cancel(self, receipt: TransportReceipt) -> TransportStatus: ...
