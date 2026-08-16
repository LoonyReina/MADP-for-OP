from __future__ import annotations

from limited_remote_partner.core.config import AppConfig
from limited_remote_partner.gateway.input_parser import input_changed


def should_execute(config: AppConfig, changed_paths: list[str]) -> bool:
    return input_changed(config, changed_paths)
