from __future__ import annotations

import argparse
import signal
import time
from pathlib import Path

from limited_remote_partner.core.config import AppConfig, load_config
from limited_remote_partner.gateway.git_client import GitClient, configure_direct_git_fast_fail


def main() -> None:
    args = _parse_args()
    configure_direct_git_fast_fail("direct")
    config = load_config(Path(args.config).resolve())
    interval = args.interval or config.poll_interval_seconds
    git = GitClient(config.repo, config.repo_dir)
    stop = StopFlag()

    signal.signal(signal.SIGINT, stop.handle)
    signal.signal(signal.SIGTERM, stop.handle)

    git.ensure_worktree()
    git.require_branch(config.repo.branch)
    print(
        f"git_partner local sync exchanging {config.io.exchange_dir}/ and pulling "
        f"{config.repo.remote}/{config.repo.branch} every {interval:g}s; "
        "input/ is never pushed automatically",
        flush=True,
    )

    while not stop.requested:
        try:
            changed = sync_once(git, config)
            if changed:
                print("synced exchange or pulled remote updates", flush=True)
            if args.once:
                break
            time.sleep(interval)
        except Exception as exc:
            print(f"git_partner local sync error: {exc}", flush=True)
            if args.once:
                raise
            time.sleep(max(interval, 3))


def sync_once(git: GitClient, config: AppConfig) -> bool:
    changed = False
    if config.exchange_watch.enabled and git.has_path_changes([config.io.exchange_dir]):
        changed = git.commit_and_push(
            [config.io.exchange_dir],
            config.exchange_watch.commit_message,
            config.io.max_file_bytes,
        )

    local_ref = git.rev_parse("HEAD")
    remote_ref = git.fetch()
    if local_ref == remote_ref:
        return changed
    git.integrate_fetched_head()
    return True


class StopFlag:
    def __init__(self) -> None:
        self.requested = False

    def handle(self, _signum: int, _frame: object) -> None:
        self.requested = True


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="path to config JSON")
    parser.add_argument(
        "--interval",
        type=float,
        default=None,
        help="pull interval in seconds; defaults to poll_interval_seconds",
    )
    parser.add_argument("--once", action="store_true", help="pull once and exit")
    return parser.parse_args()


if __name__ == "__main__":
    main()
