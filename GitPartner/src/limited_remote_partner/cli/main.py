from __future__ import annotations

import argparse
import signal
import time
from pathlib import Path

from limited_remote_partner.maintenance.auto_update import (
    consume_pending_reexec,
    reexec_partner_role,
    should_reexec_for_update,
)
from limited_remote_partner.core.config import AppConfig, load_config
from limited_remote_partner.gateway.exchange_sync import push_exchange_if_changed
from limited_remote_partner.gateway.git_client import GitClient
from limited_remote_partner.gateway.input_parser import parse_request
from limited_remote_partner.core.loop_backoff import LoopErrorBackoff
from limited_remote_partner.adapters.runner import CommandRunner
from limited_remote_partner.engine.scheduler import should_execute
from limited_remote_partner.core.targeting import route_request_for_current_runtime


def main() -> None:
    args = _parse_args()
    config_path = Path(args.config).resolve()
    config = load_config(config_path)
    git = GitClient(config.repo, config.repo_dir)
    stop = StopFlag()

    signal.signal(signal.SIGINT, stop.handle)
    signal.signal(signal.SIGTERM, stop.handle)

    git.ensure_worktree()
    git.configure_identity()
    _sync_exchange_if_changed(git, config)
    pending_reexec = consume_pending_reexec()
    last_ref = git.fetch()
    local_ref = git.rev_parse("HEAD")

    if pending_reexec and local_ref == pending_reexec.trigger_ref:
        changed = list(pending_reexec.changed_paths)
        if should_execute(config, changed):
            request = parse_request(config, pending_reexec.trigger_ref)
            request = route_request_for_current_runtime(config, request)
            if request is not None:
                CommandRunner(git, config).run_request(request, pending_reexec.trigger_ref)
            last_ref = git.fetch()
        else:
            last_ref = pending_reexec.trigger_ref
    elif local_ref != last_ref:
        changed = git.changed_paths(local_ref, last_ref)
        git.checkout_remote_head()
        if should_reexec_for_update(config, changed):
            reexec_partner_role(
                config,
                config_path,
                "local",
                last_ref,
                local_ref,
                changed,
            )
        if should_execute(config, changed):
            request = parse_request(config, last_ref)
            request = route_request_for_current_runtime(config, request)
            if request is not None:
                CommandRunner(git, config).run_request(request, last_ref)
            last_ref = git.fetch()

    runner = CommandRunner(git, config)
    print(
        f"limited_remote_partner watching {config.io.input_dir}/ "
        f"publishing {config.io.output_dir}/ and syncing {config.io.exchange_dir}/ "
        f"in {config.repo_dir}",
        flush=True,
    )

    last_exchange_watch = 0.0
    error_backoff = LoopErrorBackoff(config.error_backoff)
    while not stop.requested:
        try:
            now = time.monotonic()
            if (
                config.exchange_watch.enabled
                and now - last_exchange_watch >= config.exchange_watch.interval_seconds
            ):
                if _sync_exchange_if_changed(git, config):
                    last_ref = git.fetch()
                last_exchange_watch = now

            remote_ref = git.fetch()
            if remote_ref != last_ref:
                changed = git.changed_paths(last_ref, remote_ref)
                git.checkout_remote_head()
                if should_reexec_for_update(config, changed):
                    reexec_partner_role(
                        config,
                        config_path,
                        "local",
                        remote_ref,
                        last_ref,
                        changed,
                    )
                if should_execute(config, changed):
                    request = parse_request(config, remote_ref)
                    request = route_request_for_current_runtime(config, request)
                    if request is not None:
                        print(f"running request {request.request_id} for {remote_ref}", flush=True)
                        runner.run_request(request, remote_ref)
                    last_ref = git.fetch()
                else:
                    last_ref = remote_ref
            error_backoff.reset()
            time.sleep(config.poll_interval_seconds)
        except Exception as exc:
            sleep_seconds = error_backoff.record_failure()
            print(
                f"limited_remote_partner error: {exc}; "
                f"sleeping {sleep_seconds:.1f}s before retry",
                flush=True,
            )
            time.sleep(sleep_seconds)


class StopFlag:
    def __init__(self) -> None:
        self.requested = False

    def handle(self, _signum: int, _frame: object) -> None:
        self.requested = True


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="path to config JSON")
    return parser.parse_args()


def _sync_exchange_if_changed(git: GitClient, config: AppConfig) -> bool:
    return push_exchange_if_changed(git, config)


if __name__ == "__main__":
    main()
