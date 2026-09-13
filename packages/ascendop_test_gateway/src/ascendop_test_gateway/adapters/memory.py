from __future__ import annotations

from dataclasses import dataclass, field

from ..contracts import PreparedBundle, TestState, TransportReceipt, TransportStatus
from ..ports import UnsupportedTransportOperation


@dataclass
class MemoryTransport:
    states: dict[str, TestState] = field(default_factory=dict)
    submit_count: int = 0

    def preflight(self, bundle: PreparedBundle) -> None:
        return None

    def submit(self, bundle: PreparedBundle) -> TransportReceipt:
        self.submit_count += 1
        self.states.setdefault(bundle.request_id, TestState.ACCEPTED)
        return TransportReceipt(
            request_id=bundle.request_id,
            remote_attempt_id=f"memory-{bundle.request_id}",
            output_subdir=f"standalone/{bundle.request_id}",
        )

    def status(self, receipt: TransportReceipt) -> TransportStatus:
        return TransportStatus(
            request_id=receipt.request_id,
            state=self.states.get(receipt.request_id, TestState.UNCERTAIN),
            classification="memory-transport",
        )

    def reconcile(self, bundle: PreparedBundle) -> TransportReceipt | None:
        if bundle.request_id not in self.states:
            return None
        return TransportReceipt(
            request_id=bundle.request_id,
            remote_attempt_id=f"memory-{bundle.request_id}",
            output_subdir=f"standalone/{bundle.request_id}",
        )

    def cancel(self, receipt: TransportReceipt) -> TransportStatus:
        self.states[receipt.request_id] = TestState.CANCELLED
        return self.status(receipt)

    def advance(self, request_id: str, state: TestState) -> None:
        self.states[request_id] = state

    def acknowledge(self, receipt: TransportReceipt, *, event_id: str) -> bool:
        raise UnsupportedTransportOperation("memory transport has no remote return to acknowledge")
