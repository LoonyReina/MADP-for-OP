from __future__ import annotations


class EngineJobBuildError(RuntimeError):
    """A candidate payload cannot be materialized under the workflow contract."""
