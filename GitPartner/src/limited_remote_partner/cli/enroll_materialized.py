from __future__ import annotations

import argparse
import json
from pathlib import Path

from limited_remote_partner.endpoint.materialized_enrollment import (
    enroll_materialized_node,
)
from limited_remote_partner.endpoint.node_enrollment import NodeEnrollmentError


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Enroll one immutable registry-materialized endpoint config"
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--repo-dir")
    parser.add_argument("--no-bootstrap-channels", action="store_true")
    args = parser.parse_args(argv)
    try:
        payload = enroll_materialized_node(
            Path(args.config),
            repo_dir=Path(args.repo_dir) if args.repo_dir else None,
            bootstrap_channels=not args.no_bootstrap_channels,
        )
    except (NodeEnrollmentError, OSError, ValueError, json.JSONDecodeError) as exc:
        raise SystemExit(f"GITPARTNER_ENDPOINT_ERROR {exc}") from exc
    print(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
