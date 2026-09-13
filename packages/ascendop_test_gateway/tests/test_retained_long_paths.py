from pathlib import Path

import pytest

from ascendop_protocol.filesystem import filesystem_path
from ascendop_test_gateway.terminal_evidence import (
    read_retained_file, retain_terminal_evidence, validate_terminal_evidence,
)


@pytest.mark.parametrize("depth", [0, 5])
def test_retention_and_consumption_use_real_long_paths(tmp_path, depth):
    run = tmp_path / "request"
    payload = run / "results/attempt/payload"
    for n in range(depth):
        payload /= f"segment-{n}-" + "x" * 45
    io = filesystem_path(payload)
    (io / "result_bundle").mkdir(parents=True)
    for name in ("terminal.json", "state.json", "artifact_manifest.json"):
        (io / name).write_text("{}", encoding="utf-8")
    original = b"FAIL: preserve the discriminating input"
    (io / "result_bundle/RESULT.txt").write_bytes(original)
    (io.parent / ".payload.sha256").write_text("b" * 64, encoding="ascii")
    event = dict(artifact_root=str(payload), result_payload_sha256="b" * 64, event_id="c" * 64)
    if depth:
        assert len(str(payload)) > 260
    retained = retain_terminal_evidence(run, event)
    result = payload / "result_bundle/RESULT.txt"
    assert read_retained_file(run, retained, result) == original
    # Unrelated research notes are not part of accepted payload identity.
    (run / "notes.md").write_text("new hypothesis", encoding="utf-8")
    validate_terminal_evidence(run, event, retained)
    assert read_retained_file(run, retained, result) == original
    filesystem_path(result).write_bytes(b"PASS")
    with pytest.raises(ValueError, match="changed"):
        read_retained_file(run, retained, result)
