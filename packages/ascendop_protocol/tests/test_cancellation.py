from __future__ import annotations

import pytest

from ascendop_protocol.wire_v3 import (
    FlowV3ProtocolError,
    validate_cancellation_request,
)


def cancellation_request() -> dict[str, object]:
    return {
        "schema": "ascendop.flow.cancellation.v1",
        "version": 1,
        "cancellation_id": "cancel-flow-test-001",
        "request_id": "flow-test-001",
        "attempt_id": "attempt-001",
        "engine_job_id": "flow-test-001-attempt-001",
        "endpoint_id": "ling",
        "endpoint_generation": "ling-generation-1",
        "envelope_digest": "a" * 64,
        "requested_at": "2026-08-21T00:00:00+00:00",
        "reason": "operator requested cancellation",
    }


def test_cancellation_is_exact_and_digest_stable() -> None:
    first = validate_cancellation_request(cancellation_request())
    second = validate_cancellation_request(cancellation_request())

    assert first.request == cancellation_request()
    assert first.digest == second.digest
    assert len(first.digest) == 64


@pytest.mark.parametrize(
    ("mutate", "code"),
    [
        (lambda row: row.update(extra=True), "invalid-cancellation-fields"),
        (lambda row: row.pop("attempt_id"), "invalid-cancellation-fields"),
        (lambda row: row.update(envelope_digest="bad"), "invalid-sha256"),
        (lambda row: row.update(reason=""), "invalid-cancellation-reason"),
    ],
)
def test_cancellation_rejects_ambiguous_fields(mutate, code: str) -> None:
    request = cancellation_request()
    mutate(request)

    with pytest.raises(FlowV3ProtocolError) as error:
        validate_cancellation_request(request)

    assert error.value.code == code
