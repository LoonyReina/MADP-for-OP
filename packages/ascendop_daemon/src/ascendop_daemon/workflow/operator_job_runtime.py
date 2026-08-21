from __future__ import annotations

import os
from pathlib import Path

from ascendop_daemon.workflow.operator_job_errors import EngineJobBuildError


GITPARTNER_PRODUCT_DIR = "GitPartner"


def gitpartner_source_root(root: Path) -> Path:
    active_runtime_value = str(
        os.environ.get("GITPARTNER_RUNTIME_SOURCE") or ""
    ).strip()
    product_value = str(
        os.environ.get("ASCENDOP_GITPARTNER_PRODUCT") or GITPARTNER_PRODUCT_DIR
    )
    product = Path(product_value)
    source_root = (
        product.resolve() if product.is_absolute() else root.resolve() / product / "src"
    )
    candidates = [
        *([Path(active_runtime_value).resolve()] if active_runtime_value else []),
        source_root,
        Path(__file__).resolve().parents[5] / GITPARTNER_PRODUCT_DIR / "src",
    ]
    source_root = next(
        (
            candidate
            for candidate in candidates
            if (
                candidate / "limited_remote_partner" / "gateway" / "submit_job.py"
            ).is_file()
        ),
        source_root,
    )
    marker = source_root / "limited_remote_partner" / "gateway" / "submit_job.py"
    if not marker.is_file():
        raise EngineJobBuildError(
            f"canonical GitPartner product source is missing: {source_root}"
        )
    return source_root
