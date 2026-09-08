from __future__ import annotations

import json
from pathlib import Path

from ascendop_daemon.automation.record_intake import load_record_batch


def test_bad_record_is_preserved_and_healthy_record_survives(tmp_path: Path) -> None:
    good, bad = tmp_path / "good.json", tmp_path / "bad.json"
    good.write_text('{"value":1}', encoding="utf-8")
    bad.write_text('{"broken":', encoding="utf-8")
    quarantine = tmp_path / "quarantine"
    def validate(record):
        if record.get("value") != 1:
            raise ValueError("value required")
    records, errors = load_record_batch([bad, good], quarantine_root=quarantine, validate=validate)
    assert records == {good: {"value": 1}}
    assert len(errors) == 1 and errors[0]["state"] == "quarantined"
    descriptor = Path(errors[0]["quarantine_path"])
    before = descriptor.stat().st_mtime_ns
    assert json.loads(descriptor.read_text())["source_preserved"]
    load_record_batch([bad, good], quarantine_root=quarantine, validate=validate)
    assert descriptor.stat().st_mtime_ns == before
    assert bad.read_text() == '{"broken":'


def test_quarantine_write_failure_is_local_and_visible(tmp_path: Path) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text("[]", encoding="utf-8")
    unavailable = tmp_path / "not-a-directory"
    unavailable.write_text("occupied", encoding="utf-8")
    records, errors = load_record_batch([bad], quarantine_root=unavailable, validate=lambda _: None)
    assert not records
    assert errors[0]["quarantine_error"]
