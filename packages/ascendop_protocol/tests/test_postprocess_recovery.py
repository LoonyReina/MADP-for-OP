from __future__ import annotations

import pytest

from ascendop_protocol.wire_v3 import (
    FlowV3ProtocolError,
    validate_postprocess_recovery_request,
)


def recovery_request() -> dict[str, object]:
    return {
        "schema": "ascendop.flow.postprocess-recovery.v1",
        "version": 1,
        "recovery_id": "recovery-a",
        "request_id": "request-a",
        "attempt_id": "attempt-a",
        "engine_job_id": "engine-a",
        "endpoint_id": "endpoint-a",
        "endpoint_generation": "generation-a",
        "terminal_digest": "a" * 64,
        "terminal_revision": 0,
        "stages": ["profile-parse", "result-assemble"],
        "max_stage_attempts": 1,
        "created_at": "2026-08-08T00:00:00+00:00",
        "reason": "retry durable host postprocess only",
    }


def test_postprocess_recovery_is_exact_and_digest_stable() -> None:
    first = validate_postprocess_recovery_request(recovery_request())
    second = validate_postprocess_recovery_request(recovery_request())

    assert first.request == second.request
    assert first.digest == second.digest


@pytest.mark.parametrize(
    ("mutation", "code"),
    [
        (lambda row: row.update(extra=True), "invalid-postprocess-recovery-fields"),
        (
            lambda row: row.update(max_stage_attempts=2),
            "invalid-postprocess-recovery-attempts",
        ),
        (
            lambda row: row.update(stages=["profile-parse", "profile-parse"]),
            "invalid-postprocess-recovery-stages",
        ),
        (
            lambda row: row.update(terminal_digest="not-a-digest"),
            "invalid-sha256",
        ),
    ],
)
def test_postprocess_recovery_rejects_ambiguous_or_unbounded_fields(
    mutation,
    code: str,
) -> None:
    request = recovery_request()
    mutation(request)

    with pytest.raises(FlowV3ProtocolError) as caught:
        validate_postprocess_recovery_request(request)

    assert caught.value.code == code
