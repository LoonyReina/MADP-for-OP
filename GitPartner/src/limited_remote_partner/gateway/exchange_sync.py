from __future__ import annotations

from limited_remote_partner.core.config import AppConfig
from limited_remote_partner.gateway.git_client import GitClient


def push_exchange_if_changed(git: GitClient, config: AppConfig) -> bool:
    exchange_dir = config.io.exchange_dir
    if not config.exchange_watch.enabled:
        return False
    if not git.has_path_changes([exchange_dir]):
        return False
    pushed = git.commit_and_push(
        [exchange_dir],
        config.exchange_watch.commit_message,
        config.io.max_file_bytes,
    )
    if pushed:
        print(f"synced local {exchange_dir}/ changes", flush=True)
    return pushed
