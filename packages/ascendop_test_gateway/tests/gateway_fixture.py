"""Synthetic materializer only; no TestPlan compiler, operator oracle or endpoint."""
import json
from pathlib import Path
from ascendop_test_gateway.contracts import PreparedBundle
from ascendop_test_gateway.runtime import GatewayRuntime


class SyntheticStore:
    def prepare(self, request, runs_root):
        identity = request.request_id or "request-1"
        directory = runs_root / identity
        directory.mkdir(parents=True, exist_ok=True)
        value = {"request_id": identity, "operation_code": request.operation_code}
        path = directory / "SYNTHETIC_BUNDLE.json"
        if path.exists():
            assert json.loads(path.read_text()) == value
        else:
            path.write_text(json.dumps(value))
        return PreparedBundle(identity, directory, value)

    def load(self, directory):
        value = json.loads((directory / "SYNTHETIC_BUNDLE.json").read_text())
        return PreparedBundle(value["request_id"], directory, value)


class StandaloneTestGateway(GatewayRuntime):
    def __init__(self, root, transport):
        super().__init__(root, transport, bundle_store=SyntheticStore())
