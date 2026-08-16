from __future__ import annotations

from datetime import datetime, timezone

from ascendop_protocol.competition import (
    canonical_tree_digest,
    collect_project_evidence,
    lineage_tree_digest,
    project_digest,
    sha256_file,
)


def test_lineage_digest_preserves_raw_bytes_and_ignores_directories(tmp_path) -> None:
    root = tmp_path / "source"
    (root / "nested").mkdir(parents=True)
    (root / "nested" / "kernel.cpp").write_bytes(b"a\r\nb\n")

    lineage = lineage_tree_digest(root)
    canonical = canonical_tree_digest(root)

    assert lineage
    assert canonical
    assert lineage != canonical


def snapshot(project_root):
    files = []
    for path in sorted(project_root.rglob("*")):
        if path.is_file():
            files.append(
                {
                    "path": path.relative_to(project_root).as_posix(),
                    "sha256": sha256_file(path),
                    "size": path.stat().st_size,
                }
            )
    return {
        "schema": "ascendop.official-problem-snapshot.v1",
        "snapshot_id": "fixture-one",
        "campaign_id": "campaign-one",
        "operator_id": "Demo",
        "display_name": "Demo",
        "source_url": "https://example.test/problem",
        "submit_url": "https://example.test/submit",
        "ranking_url": "https://example.test/ranking",
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "environment": {"soc": ["Ascend910B"], "cann": ["9.0.0"]},
        "problem": {"track": "vector", "summary": "demo", "constraints": []},
        "project": {"format": "fixture", "digest": project_digest(files), "files": files},
        "page_digests": {"problem": "a" * 64},
    }


def test_project_mapping_and_opdef_parity_are_independent(tmp_path) -> None:
    official = tmp_path / "official"
    candidate = tmp_path / "candidate"
    for root in (official, candidate):
        (root / "op_host").mkdir(parents=True)
        (root / "op_kernel").mkdir()
        (root / "op_host" / "demo_def.cpp").write_text("opdef\n", encoding="utf-8")
        (root / "op_kernel" / "demo.cpp").write_text("kernel\n", encoding="utf-8")

    contract = snapshot(official)
    evidence = collect_project_evidence(candidate, contract)
    assert evidence.mapping_complete is True
    assert evidence.opdef_parity is True
    assert evidence.project_digest == contract["project"]["digest"]

    (candidate / "op_host" / "demo_def.cpp").write_text("changed\n", encoding="utf-8")
    changed = collect_project_evidence(candidate, contract)
    assert changed.mapping_complete is True
    assert changed.opdef_parity is False
    assert changed.mismatched_opdef_files == ("op_host/demo_def.cpp",)

    (candidate / "debug.txt").write_text("not submitted\n", encoding="utf-8")
    extra = collect_project_evidence(candidate, contract)
    assert extra.mapping_complete is True
    assert "debug.txt" not in extra.source_file_digests
    assert extra.extra_files == ("debug.txt",)
