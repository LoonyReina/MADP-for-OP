from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

from limited_remote_partner.gateway.submit_job import _sync_wait_snapshot


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Materialize and inspect one GitPartner result without republishing"
    )
    parser.add_argument("--repo", default=".")
    parser.add_argument("--output-subdir", required=True)
    parser.add_argument("--result-branch", required=True)
    parser.add_argument("--control-branch", default="")
    parser.add_argument("--remote", default="origin")
    parser.add_argument("--result-relative-path", default="")
    args = parser.parse_args(argv)

    repo = Path(args.repo).resolve()
    if not (repo / ".git").exists():
        raise SystemExit(f"repo is not a Git worktree: {repo}")
    os.environ["GITPARTNER_REMOTE"] = str(args.remote)
    os.environ["GITPARTNER_RESULT_BRANCH"] = str(args.result_branch)
    if args.control_branch:
        os.environ["GITPARTNER_BRANCH"] = str(args.control_branch)

    outcome = _sync_wait_snapshot(
        repo,
        "",
        force_fetch=True,
        output_subdir=str(args.output_subdir),
    )
    root = repo / "output" / Path(str(args.output_subdir).replace("\\", "/"))
    receipt = read_object(root / "receipt.json")
    status = receipt or read_object(root / "status.json")
    result = (
        read_object(root / Path(args.result_relative_path))
        if args.result_relative_path
        else None
    )
    print(
        json.dumps(
            {
                "schema": "gitpartner.result-query.v1",
                "output_subdir": str(args.output_subdir),
                "result_branch": str(args.result_branch),
                "remote_oid": outcome.remote_oid,
                "remote_changed": outcome.remote_changed,
                "snapshot_updated": outcome.snapshot_updated,
                "probe_succeeded": outcome.probe_succeeded,
                "status": status,
                "status_source": (
                    "compact-receipt"
                    if receipt is not None
                    else "full-status"
                ),
                "result": result,
            },
            ensure_ascii=True,
            sort_keys=True,
        )
    )


def read_object(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    raw = json.loads(path.read_text(encoding="utf-8-sig"))
    return raw if isinstance(raw, dict) else None


if __name__ == "__main__":
    main()
