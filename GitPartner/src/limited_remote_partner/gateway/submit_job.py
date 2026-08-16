from __future__ import annotations

import argparse
import base64
import hashlib
import json
import math
import os
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from limited_remote_partner.gateway.git_lock import (
    GitLockError,
    GitOperationLock,
    is_git_lock_failure_text,
    recover_git_locks_after_failure,
)
from limited_remote_partner.maintenance.direct_resident_code_sync import RESIDENT_RUNTIME_FILES
from limited_remote_partner.engine.runtime_manifest import (
    ENGINE_RUNTIME_FILES,
    SHARED_PROTOCOL_FILES,
    manifest_bytes as engine_runtime_manifest_bytes,
    protocol_package_root,
)
from limited_remote_partner.gateway.git_client import configure_direct_git_fast_fail
from limited_remote_partner.resources.payload_archive import PayloadArchiveError, stage_payload_tree
from limited_remote_partner.core.process_utils import hidden_subprocess_kwargs


TERMINAL_STATES = {"success", "failed", "claim_failed", "return_failed"}
MAX_PAYLOAD_FILE_BYTES = 1024 * 1024
DEFAULT_ASCENDOP_PYTHON_VENV = "/opt/ascendop/.venv"
DEFAULT_LCM_SOURCE_SNAPSHOT = (
    "../operators_testresult/Lcm/Lcm_V4_13/submit_snapshot/"
    "pending_snapshot/source_snapshot"
)
DEFAULT_LCM_TASK_CASE = "../operators_testresult/Lcm/Lcm_V4_13/submit_snapshot/task_case"
LCM_B_LOCAL_KINDS = {"lcm-b-local-smoke", "lcm-b-local-full-test"}
B_LOCAL_KINDS = LCM_B_LOCAL_KINDS | {"ascendop-b-local-smoke", "ascendop-test"}
ENGINE_KINDS = {
    "ascendop-engine-accept",
    "ascendop-engine-exchange",
    "ascendop-engine-snapshot",
    "ascendop-engine-collect",
    "ascendop-engine-configure",
    "ascendop-flow-v3-exchange",
}
DISTRIBUTED_CANARY_KIND = "ascendop-distributed-canary"
MSOPGEN_SCAFFOLD_KIND = "ascendop-msopgen-scaffold"
DIRECT_ENGINE_RUNTIME_SYNC_KIND = "ascendop-engine-runtime-sync"
DIRECT_RESIDENT_RUNTIME_SYNC_KIND = "ascendop-resident-runtime-sync"
LOCAL_TIMELINE_PREFIX = "GITPARTNER_LOCAL_TIMELINE:"


@dataclass(frozen=True)
class _WaitSnapshotOutcome:
    remote_oid: str
    remote_changed: bool
    snapshot_updated: bool
    probe_succeeded: bool
    probe_seconds: float
    fetch_merge_seconds: float
    changed_paths: tuple[str, ...] = ()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _timed_step(
    timeline: list[dict[str, Any]], name: str, callback: Any
) -> Any:
    started_at = _utc_now_iso()
    started = time.monotonic()
    outcome = "success"
    try:
        return callback()
    except BaseException:
        outcome = "failed"
        raise
    finally:
        timeline.append(
            {
                "name": name,
                "started_at": started_at,
                "finished_at": _utc_now_iso(),
                "duration_seconds": round(time.monotonic() - started, 6),
                "outcome": outcome,
            }
        )


def main(argv: list[str] | None = None) -> None:
    timeline_started = time.monotonic()
    timeline: list[dict[str, Any]] = []
    args = _parse_args(argv)
    repo_dir = Path(args.repo).resolve()
    result_repo_dir = (
        Path(args.result_repo).resolve()
        if str(args.result_repo or "").strip()
        else repo_dir
    )
    _configure_target_refs_from_repo(repo_dir)
    _normalize_args(args)
    configure_direct_git_fast_fail(args.transport)
    _timed_step(timeline, "prepare_payload", lambda: _prepare_payload(repo_dir, args))
    job = _timed_step(timeline, "build_job", lambda: _build_job(args))
    immutable_request_preexisted = bool(
        args.append_request
        and (
            repo_dir
            / "input"
            / "requests"
            / str(job["id"])
            / "job.json"
        ).is_file()
    )
    _timed_step(
        timeline,
        "write_job",
        lambda: _write_job(
            repo_dir,
            job,
            dry_run=args.dry_run,
            append_request=args.append_request,
        ),
    )
    if args.dry_run:
        return
    if args.commit_push:
        paths = _commit_paths(args)
        if not args.keep_existing_output and not immutable_request_preexisted:
            cleared_output = _clear_existing_output(
                result_repo_dir,
                str(job["output_subdir"]),
            )
            if cleared_output and result_repo_dir == repo_dir:
                paths.append(cleared_output)
        _commit_push(
            repo_dir,
            args.message or f"git_partner_submit: {job['id']}",
            paths,
            sync_before_publish=not (
                str(job.get("request_kind") or "").startswith("engine-")
                or args.kind == DISTRIBUTED_CANARY_KIND
            ),
            timeline=timeline,
        )
    if args.wait:
        _wait_for_result(
            result_repo_dir,
            str(job["output_subdir"]),
            args.wait_timeout_seconds,
            request_kind=str(job.get("request_kind") or ""),
            timeline=timeline,
        )
    print(
        LOCAL_TIMELINE_PREFIX
        + json.dumps(
            {
                "protocol_version": "gitpartner-local-timeline-v1",
                "request_id": str(job.get("request_id") or job.get("id") or ""),
                "total_seconds": round(time.monotonic() - timeline_started, 6),
                "finished_at": _utc_now_iso(),
                "steps": timeline,
            },
            ensure_ascii=True,
            sort_keys=True,
        )
    )


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Write standardized GitPartner input/job.json requests and optionally "
            "commit, push, and wait for output/<request-id>/status.json."
        )
    )
    parser.add_argument(
        "--repo",
        default=".",
        help="GitPartner repository root; default is the current directory",
    )
    parser.add_argument(
        "--result-repo",
        default="",
        help=(
            "optional independent result worktree used only for output polling; "
            "request publication remains bound to --repo"
        ),
    )
    parser.add_argument(
        "--commit-push",
        action="store_true",
        help="commit input/job.json and push the configured GitPartner branch after writing",
    )
    parser.add_argument(
        "--append-request",
        action="store_true",
        help=(
            "write immutable input/requests/<request-id>/job.json instead of "
            "the legacy mutable input/job.json"
        ),
    )
    parser.add_argument(
        "--also-commit-request-id",
        action="append",
        default=[],
        help=(
            "with --append-request --commit-push, include an already staged "
            "immutable request in the same bounded publication"
        ),
    )
    parser.add_argument(
        "--publish-maintenance-changes",
        action="store_true",
        help=(
            "with lan-bootstrap, include the fixed GitPartner maintenance path allowlist "
            "in the trusted commit; arbitrary paths are never accepted"
        ),
    )
    parser.add_argument(
        "--wait",
        action="store_true",
        help="poll the configured GitPartner branch until output status reaches a terminal state",
    )
    parser.add_argument("--wait-timeout-seconds", type=int, default=300)
    parser.add_argument("--message", help="commit message override")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--keep-existing-output",
        action="store_true",
        help=(
            "do not clear output/<request-id> before publishing; default clears "
            "same-request output so --wait cannot read stale status.json"
        ),
    )

    subparsers = parser.add_subparsers(dest="kind", required=True)
    _add_common_job_args(
        subparsers.add_parser("env-probe", help="small direct/relay environment probe")
    )
    _add_common_job_args(
        subparsers.add_parser(
            "ascendop-tree-scan",
            help="scan B for AscendOP feature directories before code tests",
        )
    )
    _add_common_job_args(
        subparsers.add_parser(
            "b-system-probe",
            help="probe B CANN, compiler, and Python/torch_npu readiness",
        )
    )
    _add_common_job_args(
        subparsers.add_parser(
            "msprof-probe",
            help="probe B msprof wrapper, owner checks, and export readiness",
        )
    )
    venv_probe = subparsers.add_parser(
        "venv-torchnpu-probe",
        help="probe the B-side AscendOP virtualenv torch/torch_npu imports",
    )
    _add_common_job_args(venv_probe)
    venv_probe.add_argument(
        "--python-venv",
        default=DEFAULT_ASCENDOP_PYTHON_VENV,
        help=(
            "B-side Python virtualenv to probe; "
            f"default is {DEFAULT_ASCENDOP_PYTHON_VENV}"
        ),
    )
    distributed_canary = subparsers.add_parser(
        DISTRIBUTED_CANARY_KIND,
        help="isolated multi-endpoint transport control or host-only canary",
    )
    _add_common_job_args(distributed_canary)
    distributed_canary.add_argument(
        "--task-class",
        choices=(
            "control-probe",
            "host-only-canary",
            "engine-host-canary",
            "engine-device-canary",
        ),
        required=True,
    )
    distributed_canary.add_argument("--experiment-id", required=True)
    distributed_canary.add_argument("--attempt-id", required=True)
    distributed_canary.add_argument("--payload-file", type=Path)
    distributed_canary.add_argument("--synthetic-duration-ms", type=int, default=0)
    distributed_canary.add_argument("--host-duration-ms", type=int, default=0)
    distributed_canary.add_argument("--device-duration-ms", type=int, default=0)
    distributed_canary.add_argument("--export-duration-ms", type=int, default=0)
    distributed_canary.add_argument("--engine-root", default="test_engine_demo")
    distributed_canary.add_argument(
        "--failure-mode",
        choices=("none", "fail-before-result", "fail-after-result"),
        default="none",
    )
    msopgen_scaffold = subparsers.add_parser(
        MSOPGEN_SCAFFOLD_KIND,
        help="stage and run one AscendOP msopgen scaffold request",
    )
    _add_common_job_args(msopgen_scaffold)
    msopgen_scaffold.set_defaults(
        transport="direct",
        sandbox_profile="ascend-compile",
    )
    msopgen_scaffold.add_argument("--op", required=True)
    msopgen_scaffold.add_argument("--season", required=True)
    msopgen_scaffold.add_argument("--remote-root", required=True)
    msopgen_scaffold.add_argument("--scaffold-script", type=Path, required=True)
    msopgen_scaffold.add_argument("--msopgen-input", type=Path, required=True)
    msopgen_scaffold.add_argument("--soc", default="ai_core-Ascend910B4")
    msopgen_scaffold.add_argument("--framework", default="tf")
    msopgen_scaffold.add_argument("--language", default="cpp")
    msopgen_scaffold.add_argument("--output-root", default="operators_workspace")
    msopgen_scaffold.add_argument("--allow-existing", action="store_true")
    engine_accept = subparsers.add_parser(
        "ascendop-engine-accept",
        help="durably admit one engine-v1 job and return its acceptance receipt",
    )
    _add_common_job_args(engine_accept)
    engine_accept.set_defaults(transport="relay", timeout_seconds=120, sandbox_profile="process")
    add_engine_common_args(engine_accept)
    engine_accept.add_argument("--spec", type=Path, required=True)
    engine_accept.add_argument(
        "--payload-root",
        type=Path,
        help="optional immutable payload directory staged beside engine_job.json",
    )
    engine_accept.add_argument("--engine-job-id", required=True)

    flow_v3_exchange = subparsers.add_parser(
        "ascendop-flow-v3-exchange",
        help="transport one immutable Wire V3 accept/query/status control request",
    )
    _add_common_job_args(flow_v3_exchange)
    flow_v3_exchange.set_defaults(
        transport="relay",
        timeout_seconds=120,
        sandbox_profile="process",
    )
    add_engine_common_args(flow_v3_exchange)
    flow_v3_exchange.add_argument(
        "--action",
        choices=("accept", "query", "ack", "status"),
        required=True,
    )
    flow_v3_exchange.add_argument("--logical-request-id", required=True)
    flow_v3_exchange.add_argument("--attempt-id", required=True)
    flow_v3_exchange.add_argument("--endpoint-generation", required=True)
    flow_v3_exchange.add_argument("--receipt-id", default="")
    flow_v3_exchange.add_argument("--envelope", type=Path)
    flow_v3_exchange.add_argument("--package-root", type=Path)

    engine_exchange = subparsers.add_parser(
        "ascendop-engine-exchange",
        help=(
            "atomically converge capacity, acknowledge returns, admit a batch, "
            "and return one fresh engine snapshot"
        ),
    )
    _add_common_job_args(engine_exchange)
    engine_exchange.set_defaults(
        transport="relay", timeout_seconds=120, sandbox_profile="process"
    )
    add_engine_common_args(engine_exchange)
    engine_exchange.add_argument("--manifest", type=Path, required=True)
    engine_exchange.add_argument(
        "--ack-return",
        action="append",
        default=[],
        metavar="ENGINE_JOB_ID=RECEIPT_ID",
    )
    engine_exchange.add_argument(
        "--ack-required",
        action="append",
        default=[],
        metavar="ENGINE_JOB_ID=RECEIPT_ID",
    )
    engine_exchange.add_argument("--max-inflight", type=int, required=True)
    engine_exchange.add_argument(
        "--wait-ready-seconds",
        type=float,
        default=45.0,
    )
    engine_exchange.add_argument("--standby-slots", type=int)
    engine_exchange.add_argument("--active-job-slots", type=int)
    engine_exchange.add_argument("--host-slots", type=int)
    engine_exchange.add_argument("--host-cpu-weight-capacity", type=int)
    engine_exchange.add_argument("--host-memory-mb-capacity", type=int)
    engine_exchange.add_argument("--host-io-weight-capacity", type=int)
    engine_exchange.add_argument("--cold-build-slots", type=int)
    engine_exchange.add_argument("--cache-hit-slots", type=int)
    engine_exchange.add_argument("--device-slots", type=int)
    engine_exchange.add_argument("--device-inventory-json")
    engine_exchange.add_argument("--export-slots", type=int)
    engine_exchange.add_argument("--return-backlog-soft-limit-bytes", type=int)
    engine_exchange.add_argument("--return-backlog-hard-limit-bytes", type=int)
    engine_exchange.add_argument("--return-backlog-soft-limit-jobs", type=int)
    engine_exchange.add_argument("--return-backlog-hard-limit-jobs", type=int)
    exchange_drain = engine_exchange.add_mutually_exclusive_group()
    exchange_drain.add_argument("--drain", action="store_true")
    exchange_drain.add_argument("--resume", action="store_true")

    engine_snapshot = subparsers.add_parser(
        "ascendop-engine-snapshot",
        help="return fresh engine capacity and all terminal return-ready manifests",
    )
    _add_common_job_args(engine_snapshot)
    engine_snapshot.set_defaults(transport="relay", timeout_seconds=120, sandbox_profile="process")
    add_engine_common_args(engine_snapshot)
    engine_snapshot.add_argument(
        "--ack-return",
        action="append",
        default=[],
        metavar="ENGINE_JOB_ID=RECEIPT_ID",
        help="idempotently acknowledge an earlier snapshot bundle before taking this snapshot",
    )
    engine_snapshot.add_argument(
        "--ack-required",
        action="append",
        default=[],
        metavar="ENGINE_JOB_ID=RECEIPT_ID",
    )

    engine_collect = subparsers.add_parser(
        "ascendop-engine-collect",
        help="return one engine job terminal and state manifest",
    )
    _add_common_job_args(engine_collect)
    engine_collect.set_defaults(transport="relay", timeout_seconds=120, sandbox_profile="process")
    add_engine_common_args(engine_collect)
    engine_collect.add_argument("--engine-job-id", required=True)
    engine_collect.add_argument("--receipt-id", required=True)
    engine_collect.add_argument(
        "--ack-only",
        action="store_true",
        help="publish an idempotent return acknowledgement without retransmitting the bundle",
    )
    engine_configure = subparsers.add_parser(
        "ascendop-engine-configure",
        help="dynamically converge B-side engine capacity and drain state",
    )
    _add_common_job_args(engine_configure)
    engine_configure.set_defaults(
        transport="relay", timeout_seconds=120, sandbox_profile="process"
    )
    add_engine_common_args(engine_configure)
    engine_configure.add_argument("--max-inflight", type=int, required=True)
    engine_configure.add_argument("--standby-slots", type=int)
    engine_configure.add_argument("--active-job-slots", type=int)
    engine_configure.add_argument("--host-slots", type=int)
    engine_configure.add_argument("--host-cpu-weight-capacity", type=int)
    engine_configure.add_argument("--host-memory-mb-capacity", type=int)
    engine_configure.add_argument("--host-io-weight-capacity", type=int)
    engine_configure.add_argument("--cold-build-slots", type=int)
    engine_configure.add_argument("--cache-hit-slots", type=int)
    engine_configure.add_argument("--device-slots", type=int)
    engine_configure.add_argument("--device-inventory-json")
    engine_configure.add_argument("--export-slots", type=int)
    engine_configure.add_argument("--return-backlog-soft-limit-bytes", type=int)
    engine_configure.add_argument("--return-backlog-hard-limit-bytes", type=int)
    engine_configure.add_argument("--return-backlog-soft-limit-jobs", type=int)
    engine_configure.add_argument("--return-backlog-hard-limit-jobs", type=int)
    configure_drain = engine_configure.add_mutually_exclusive_group()
    configure_drain.add_argument("--drain", action="store_true")
    configure_drain.add_argument("--resume", action="store_true")
    engine_runtime_sync = subparsers.add_parser(
        DIRECT_ENGINE_RUNTIME_SYNC_KIND,
        help=(
            "stage an immutable Engine runtime bundle and atomically install it "
            "on one direct GitPartner node"
        ),
    )
    _add_common_job_args(engine_runtime_sync)
    engine_runtime_sync.set_defaults(
        transport="direct",
        timeout_seconds=120,
        sandbox_profile="process",
    )
    add_engine_common_args(engine_runtime_sync)
    engine_runtime_sync.add_argument("--expected-generation", required=True)
    engine_runtime_sync.add_argument(
        "--target-repo",
        default="ascend-git-partner",
        help="GitPartner runtime worktree below --client-work-dir",
    )
    resident_runtime_sync = subparsers.add_parser(
        DIRECT_RESIDENT_RUNTIME_SYNC_KIND,
        help=(
            "stage a bounded GitPartner resident runtime bundle, atomically "
            "install it on one direct node, and schedule a fenced client restart"
        ),
    )
    _add_common_job_args(resident_runtime_sync)
    resident_runtime_sync.set_defaults(
        transport="direct",
        timeout_seconds=120,
        sandbox_profile="process",
    )
    resident_runtime_sync.add_argument("--expected-generation", required=True)
    resident_runtime_sync.add_argument(
        "--target-repo",
        default="ascend-git-partner",
        help="GitPartner resident worktree below --client-work-dir",
    )
    resident_runtime_sync.add_argument(
        "--resident-config",
        required=True,
        help="effective resident config below --client-work-dir",
    )
    resident_runtime_sync.add_argument(
        "--restart-delay-seconds",
        type=int,
        default=90,
    )
    lan_bootstrap = subparsers.add_parser(
        "lan-bootstrap",
        help=(
            "ask the server side to SCP GitPartner code to its LAN peer and "
            "start/restart that peer's GitPartner service"
        ),
    )
    lan_bootstrap.set_defaults(
        transport="auto",
        client_work_dir="/opt/ascendop",
        timeout_seconds=300,
        sync_interval_seconds=20,
        sandbox_profile="process",
    )
    lan_bootstrap.add_argument("--request-id")
    lan_bootstrap.add_argument("--output-subdir")
    lan_bootstrap.add_argument("--client-work-dir", default="/opt/ascendop")
    lan_bootstrap.add_argument("--engine-root", default="test_engine_demo")
    lan_bootstrap.add_argument("--target-node", action="append", default=[])
    lan_bootstrap.add_argument("--target-endpoint-id", default="")
    lan_bootstrap.add_argument("--target-environment-id", default="")
    lan_bootstrap.add_argument("--target-gateway-id", default="")
    lan_bootstrap.add_argument("--target-transport-mode", default="")
    lan_bootstrap.add_argument("--registration-generation", default="")
    lan_bootstrap.add_argument(
        "--action",
        choices=[
            "lan-bootstrap",
            "lan-sync-code",
            "lan-restart-service",
            "lan-reconcile-service",
            "lan-reconcile-request",
            "lan-cancel-request",
            "lan-node-ack",
            "lan-diagnose",
            "lan-inspect-artifact",
            "lan-sync-artifact",
            "endpoint-runtime",
            "server-tmux-command",
        ],
        default="lan-bootstrap",
    )
    lan_bootstrap.add_argument("--target-role", choices=["client", "server"], default="client")
    lan_bootstrap.add_argument("--target-host", default="")
    lan_bootstrap.add_argument("--target-dir", default="")
    lan_bootstrap.add_argument("--remote-config", default="configs/partner.json")
    lan_bootstrap.add_argument("--remote-staging-dir", default="")
    lan_bootstrap.add_argument("--service-name", default="")
    lan_bootstrap.add_argument("--cleanup-request-id", default="")
    lan_bootstrap.add_argument("--diagnose-request-id", default="")
    lan_bootstrap.add_argument("--reconcile-request-id", default="")
    lan_bootstrap.add_argument("--reconcile-job-sha256", default="")
    lan_bootstrap.add_argument("--cancel-request-id", default="")
    lan_bootstrap.add_argument(
        "--node-ack-file",
        type=Path,
        help="generation-fenced central node acknowledgement JSON",
    )
    lan_bootstrap.add_argument(
        "--cancel-reason",
        default="LAN operator requested cancellation",
    )
    lan_bootstrap.add_argument(
        "--tmux-session",
        default="",
        help="tmux session name for server-tmux-command",
    )
    lan_bootstrap.add_argument(
        "--script-file",
        type=Path,
        help="local shell script to run inside the remote tmux session",
    )
    lan_bootstrap.add_argument(
        "--script-b64",
        default="",
        help="base64-encoded shell script to run inside the remote tmux session",
    )
    lan_bootstrap.add_argument(
        "--artifact-profile",
        default="",
        help=(
            "allowlisted artifact profile for lan-inspect-artifact or "
            "lan-sync-artifact"
        ),
    )
    lan_bootstrap.add_argument(
        "--endpoint-action",
        choices=("provision", "start", "status", "stop"),
        default="status",
    )
    lan_bootstrap.add_argument("--source-repo", default="")
    lan_bootstrap.add_argument("--worktree", default="")
    lan_bootstrap.add_argument("--control-branch", default="")
    lan_bootstrap.add_argument("--endpoint-config", default="")
    lan_bootstrap.add_argument(
        "--endpoint-role",
        choices=("server", "client", "local"),
        default="local",
    )
    lan_bootstrap.add_argument("--endpoint-remote", default="origin")
    lan_bootstrap.add_argument("--import-login-network-env", action="store_true")
    lan_bootstrap.add_argument(
        "--sync-path",
        action="append",
        help=(
            "path to sync to the peer; repeatable. Defaults to GitPartner "
            "source/scripts/config/service/docs only"
        ),
    )
    lan_bootstrap.add_argument("--no-process-fallback", action="store_true")

    lcm = subparsers.add_parser(
        "lcm-release-test",
        help="preflight, or optionally run, an Lcm release-version test on B",
    )
    _add_common_job_args(lcm)
    lcm.add_argument("--release", default="Lcm_V4")
    lcm.add_argument("--test-version", default="Lcm_V4_13")
    lcm.add_argument("--vendor", default="lcm_v4_13")
    lcm.add_argument("--case-version", default="case_v002")
    lcm.add_argument("--season", default="S5-910b")
    lcm.add_argument("--hardware", default="910B4")
    lcm.add_argument("--mode", default="correct", choices=["correct", "perf", "both"])
    lcm.add_argument(
        "--run-remote-test",
        action="store_true",
        help=(
            "after preflight, run Remote/run_remote_test.py using the current "
            "AscendOP harness; without this flag the job only verifies paths"
        ),
    )
    lcm.add_argument("--skip-build", action="store_true")
    lcm.add_argument("--perf-case-range")
    lcm.add_argument("--perf-weighted-target")

    local = subparsers.add_parser(
        "lcm-b-local-smoke",
        help=(
            "stage an Lcm release snapshot as GitPartner payload and run a "
            "B-local build/install/correctness smoke"
        ),
    )
    _add_common_job_args(local)
    local.set_defaults(stage_payload=True, op="Lcm")
    local.add_argument("--release", default="Lcm_V4")
    local.add_argument("--test-version", default="Lcm_V4_13")
    local.add_argument("--vendor", default="lcm_v4_13_gitpartner")
    local.add_argument("--case-range", default="1")
    local.add_argument(
        "--run-perf",
        action="store_true",
        help="after correctness, run task_case/perf_all.sh with the selected venv",
    )
    local.add_argument(
        "--perf-case-range",
        help="perf case range passed as PERF_CASE_RANGE; defaults to --case-range",
    )
    local.add_argument(
        "--perf-time-base",
        default="9999999999999",
        help="PERF_TIME_BASE for perf_all.sh; high default makes this a smoke baseline",
    )
    local.add_argument("--perf-weighted-target")
    local.add_argument("--perf-storage-limit", default="200MB")
    local.add_argument(
        "--build-only",
        action="store_true",
        help="stop after B-local build and .run install; does not require torch_npu",
    )
    local.add_argument(
        "--install-build-python-deps",
        action="store_true",
        help=(
            "install missing pure-Python build deps into the selected venv "
            "or per-request run dir fallback; never writes system Python"
        ),
    )
    local.add_argument(
        "--python-venv",
        default=DEFAULT_ASCENDOP_PYTHON_VENV,
        help=(
            "B-side Python virtualenv used for build and correctness runtime; "
            f"default is {DEFAULT_ASCENDOP_PYTHON_VENV}"
        ),
    )
    local.add_argument(
        "--install-runtime-python-deps",
        action="store_true",
        help=(
            "allow installing missing runtime deps such as torch/torch-npu "
            "into --python-venv"
        ),
    )
    local.add_argument(
        "--source-snapshot",
        default=DEFAULT_LCM_SOURCE_SNAPSHOT,
    )
    local.add_argument(
        "--task-case",
        default=DEFAULT_LCM_TASK_CASE,
    )
    local.add_argument("--attack-case")
    local.add_argument(
        "--no-stage-payload",
        dest="stage_payload",
        action="store_false",
        help="reuse an existing input/payloads/<request-id>/ bundle",
    )

    generic = subparsers.add_parser(
        "ascendop-b-local-smoke",
        help=(
            "stage an AscendOP source snapshot and task_case payload and run "
            "a B-local build/install/correctness smoke for any prepared op"
        ),
    )
    _add_common_job_args(generic)
    generic.set_defaults(stage_payload=True)
    generic.add_argument("--op", required=True)
    generic.add_argument("--release", required=True)
    generic.add_argument("--test-version", required=True)
    generic.add_argument("--vendor", required=True)
    generic.add_argument("--case-range", default="1")
    generic.add_argument(
        "--run-perf",
        action="store_true",
        help="after correctness, run task_case/perf_all.sh with the selected venv",
    )
    generic.add_argument("--perf-case-range")
    generic.add_argument("--perf-time-base", default="9999999999999")
    generic.add_argument("--perf-weighted-target")
    generic.add_argument("--perf-storage-limit", default="200MB")
    generic.add_argument("--build-only", action="store_true")
    generic.add_argument("--install-build-python-deps", action="store_true")
    generic.add_argument(
        "--python-venv",
        default=DEFAULT_ASCENDOP_PYTHON_VENV,
    )
    generic.add_argument("--install-runtime-python-deps", action="store_true")
    generic.add_argument("--source-snapshot", required=True)
    generic.add_argument("--task-case", required=True)
    generic.add_argument("--attack-case")
    generic.add_argument(
        "--no-stage-payload",
        dest="stage_payload",
        action="store_false",
        help="reuse an existing input/payloads/<request-id>/ bundle",
    )

    unified = subparsers.add_parser(
        "ascendop-test",
        help=(
            "stable operator-test entrypoint: stage payload, install, run "
            "five-case correctness and perf using the maintained B-local path"
        ),
    )
    _add_common_job_args(unified)
    unified.set_defaults(
        stage_payload=True,
        transport="auto",
        case_range="1..5",
        run_perf=True,
        perf_case_range="1..5",
        perf_time_base="9999999999999",
        perf_weighted_target=None,
        perf_storage_limit="200MB",
        build_only=False,
        install_build_python_deps=True,
        install_runtime_python_deps=True,
        python_venv=DEFAULT_ASCENDOP_PYTHON_VENV,
    )
    unified.add_argument("--op", required=True)
    unified.add_argument("--release", required=True)
    unified.add_argument("--test-version", required=True)
    unified.add_argument("--case-version", default="unknown")
    unified.add_argument("--season", default="S5-910b")
    unified.add_argument("--hardware", default="910B4")
    unified.add_argument(
        "--vendor",
        help="vendor install directory name; defaults to <op>_<test-version>_gitpartner",
    )
    unified.add_argument("--source-snapshot")
    unified.add_argument("--task-case")
    unified.add_argument("--attack-case")
    unified.add_argument("--build-only", action="store_true")
    unified.add_argument("--no-perf", dest="run_perf", action="store_false")
    unified.add_argument("--perf-weighted-target")
    unified.add_argument(
        "--no-stage-payload",
        dest="stage_payload",
        action="store_false",
        help="reuse an existing input/payloads/<request-id>/ bundle",
    )

    full = subparsers.add_parser(
        "lcm-b-local-full-test",
        help=(
            "run the maintained B-local Lcm build/install, 1..5 correctness, "
            "and 1..5 profiler test with robust defaults"
        ),
    )
    _add_common_job_args(full)
    full.set_defaults(
        stage_payload=True,
        op="Lcm",
        release="Lcm_V4",
        test_version="Lcm_V4_13",
        vendor="lcm_v4_13_gitpartner",
        case_range="1..5",
        run_perf=True,
        perf_case_range="1..5",
        perf_time_base="9999999999999",
        perf_weighted_target=None,
        perf_storage_limit="200MB",
        build_only=False,
        install_build_python_deps=True,
        install_runtime_python_deps=True,
        python_venv=DEFAULT_ASCENDOP_PYTHON_VENV,
        source_snapshot=DEFAULT_LCM_SOURCE_SNAPSHOT,
        task_case=DEFAULT_LCM_TASK_CASE,
        sandbox_profile="process",
    )
    return parser.parse_args(argv)


def _add_common_job_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--transport", default="direct", choices=["direct", "relay", "auto"])
    parser.add_argument("--request-id")
    parser.add_argument("--output-subdir")
    parser.add_argument("--client-work-dir", default="/opt/ascendop")
    parser.add_argument("--timeout-seconds", type=int, default=300)
    parser.add_argument("--sync-interval-seconds", type=int, default=20)
    parser.add_argument("--sandbox-profile")
    parser.add_argument("--target-node", action="append", default=[])
    parser.add_argument("--target-tag", action="append", default=[])
    parser.add_argument("--target-role", action="append", default=[])
    parser.add_argument("--target-endpoint-id", default="")
    parser.add_argument("--target-environment-id", default="")
    parser.add_argument("--target-gateway-id", default="")
    parser.add_argument("--target-transport-mode", default="")
    parser.add_argument("--registration-generation", default="")
    parser.add_argument("--fanout", action="store_true")


def add_engine_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--engine-root", default="test_engine_demo")
    parser.add_argument("--engine-python", default="python3")


def _build_job(args: argparse.Namespace) -> dict[str, Any]:
    _normalize_args(args)
    request_id = args.request_id or _default_request_id(args)
    output_subdir = args.output_subdir or (
        f"engine-demo/{request_id}"
        if _is_engine(args)
        else (
            f"distributed-canary/{request_id}"
            if args.kind == DISTRIBUTED_CANARY_KIND
            else request_id
        )
    )
    command = ["bash", "-lc", _command(args)]
    timeout_seconds = int(args.timeout_seconds)
    if args.kind == "ascendop-engine-exchange":
        timeout_seconds = max(
            timeout_seconds,
            math.ceil(
                max(0.0, float(getattr(args, "wait_ready_seconds", 45.0)))
            )
            + 60,
        )
    job = {
        "id": request_id,
        "transport": args.transport,
        "command": command,
        "client_command": command,
        "working_dir": ".",
        "output_subdir": output_subdir,
        "client_work_dir": args.client_work_dir,
        "payload_paths": _payload_paths(args),
        "return_paths": _return_paths(args),
        "timeout_seconds": timeout_seconds,
        "sync_interval_seconds": args.sync_interval_seconds,
        "sandbox_profile": _sandbox_profile(args),
        "env": _env(args),
    }
    if _is_engine(args):
        job["request_kind"] = {
            "ascendop-engine-accept": "engine-admission",
            "ascendop-engine-exchange": "engine-exchange",
            "ascendop-engine-snapshot": "engine-snapshot",
            "ascendop-engine-collect": (
                "engine-return-ack" if getattr(args, "ack_only", False) else "engine-collect"
            ),
            "ascendop-engine-configure": "engine-capacity",
            "ascendop-flow-v3-exchange": "flow-v3-exchange",
        }[args.kind]
        job["completion_mode"] = (
            "accepted"
            if args.kind in {
                "ascendop-engine-accept",
                "ascendop-flow-v3-exchange",
            }
            and getattr(args, "action", "accept") == "accept"
            else "snapshot"
        )
        if getattr(args, "engine_job_id", ""):
            job["engine_job_id"] = args.engine_job_id
        if args.kind == "ascendop-flow-v3-exchange":
            job.update(
                {
                    "dispatch_class": (
                        "exchange"
                        if args.action == "accept"
                        else "return"
                        if args.action == "ack"
                        else "watch"
                    ),
                    "logical_request_id": args.logical_request_id,
                    "attempt_id": args.attempt_id,
                    "endpoint_generation": args.endpoint_generation,
                    "client_action": "flow-v3-exchange",
                }
            )
        if args.kind == "ascendop-engine-exchange":
            exchange_jobs = _engine_exchange_jobs(args)
            has_exchange_controls = bool(
                getattr(args, "ack_return", [])
                or getattr(args, "ack_required", [])
            )
            job["dispatch_class"] = (
                "exchange"
                if exchange_jobs
                else "return"
                if has_exchange_controls
                else "watch"
            )
            job["client_action"] = "engine-exchange"
            job["client_action_args"] = {
                "argv": json.dumps(
                    _engine_exchange_cli_args(args),
                    ensure_ascii=True,
                    separators=(",", ":"),
                )
            }
    elif args.kind == DISTRIBUTED_CANARY_KIND:
        job["request_kind"] = str(args.task_class)
        job["completion_mode"] = "terminal"
        job["experiment_id"] = str(args.experiment_id)
        job["attempt_id"] = str(args.attempt_id)
        job["workflow_ingest"] = False
    elif args.kind == MSOPGEN_SCAFFOLD_KIND:
        job["payload_root"] = f"input/payloads/{request_id}"
        job["completion_mode"] = "terminal"
    elif args.kind in {
        DIRECT_ENGINE_RUNTIME_SYNC_KIND,
        DIRECT_RESIDENT_RUNTIME_SYNC_KIND,
    }:
        # Keep this maintenance request compatible with older direct-node GP
        # parsers. The generation-fenced payload and receipt carry the richer
        # protocol contract.
        job["request_kind"] = "command"
        job["completion_mode"] = "terminal"
        job["workflow_ingest"] = False
        job["dispatch_class"] = "maintenance"
        maintenance_role = (
            "engine-runtime"
            if args.kind == DIRECT_ENGINE_RUNTIME_SYNC_KIND
            else "resident-runtime"
        )
        job["supersession_key"] = (
            f"{job.get('target_endpoint_id', '')}:{maintenance_role}"
        )
    if args.kind == "lan-bootstrap":
        job["dispatch_class"] = "maintenance"
        job["server_action"] = args.action
        job["server_action_args"] = _server_action_args(args)
    _apply_targeting(args, job)
    if args.kind in {
        DIRECT_ENGINE_RUNTIME_SYNC_KIND,
        DIRECT_RESIDENT_RUNTIME_SYNC_KIND,
    }:
        maintenance_role = (
            "engine-runtime"
            if args.kind == DIRECT_ENGINE_RUNTIME_SYNC_KIND
            else "resident-runtime"
        )
        job["supersession_key"] = (
            f"{job.get('target_endpoint_id', '')}:{maintenance_role}"
        )
    return job


def _apply_targeting(args: argparse.Namespace, job: dict[str, Any]) -> None:
    mappings = (
        ("target_nodes", "target_node"),
        ("target_tags", "target_tag"),
        ("target_roles", "target_role"),
    )
    for output_name, argument_name in mappings:
        raw = getattr(args, argument_name, []) or []
        values = [str(item) for item in raw if str(item)] if isinstance(raw, list) else []
        if values:
            job[output_name] = values
    for output_name, argument_name in (
        ("target_endpoint_id", "target_endpoint_id"),
        ("target_environment_id", "target_environment_id"),
        ("target_gateway_id", "target_gateway_id"),
        ("target_transport_mode", "target_transport_mode"),
        ("registration_generation", "registration_generation"),
    ):
        value = str(getattr(args, argument_name, "") or "")
        if value:
            job[output_name] = value
    if bool(getattr(args, "fanout", False)):
        job["fanout"] = True


def _normalize_args(args: argparse.Namespace) -> None:
    if getattr(args, "kind", "") in {
        DIRECT_ENGINE_RUNTIME_SYNC_KIND,
        DIRECT_RESIDENT_RUNTIME_SYNC_KIND,
    }:
        args.target_repo = _safe_engine_relative_path(str(args.target_repo))
        if args.kind == DIRECT_ENGINE_RUNTIME_SYNC_KIND:
            args.engine_root = _safe_engine_relative_path(str(args.engine_root))
        else:
            args.resident_config = _safe_engine_relative_path(
                str(args.resident_config)
            )
            if int(args.restart_delay_seconds) < 30:
                raise SystemExit("--restart-delay-seconds must be at least 30")
        generation = str(args.expected_generation or "").strip().lower()
        if (
            len(generation) != 16
            or any(char not in "0123456789abcdef" for char in generation)
        ):
            raise SystemExit(
                "--expected-generation must be a 16-character lowercase hex digest"
            )
        args.expected_generation = generation
        return
    if _is_engine(args):
        args.engine_root = _safe_engine_relative_path(str(args.engine_root))
        if args.kind == "ascendop-flow-v3-exchange":
            args.logical_request_id = _safe_canary_token(
                str(args.logical_request_id), "logical_request_id"
            )
            args.attempt_id = _safe_canary_token(
                str(args.attempt_id), "attempt_id"
            )
            generation = str(args.endpoint_generation).strip()
            if (
                len(generation) < 8
                or len(generation) > 128
                or any(
                    character not in "0123456789abcdefghijklmnopqrstuvwxyz-._"
                    for character in generation
                )
            ):
                raise SystemExit("--endpoint-generation is not a safe generation token")
            args.endpoint_generation = generation
            if not str(args.target_endpoint_id or "").strip():
                raise SystemExit("--target-endpoint-id is required for Wire V3")
            if args.action == "accept":
                if args.envelope is None or args.package_root is None:
                    raise SystemExit(
                        "Wire V3 accept requires --envelope and --package-root"
                    )
                args.envelope = Path(args.envelope).resolve()
                args.package_root = Path(args.package_root).resolve()
                if not args.envelope.is_file():
                    raise SystemExit(f"Wire V3 envelope is missing: {args.envelope}")
                if not args.package_root.is_dir():
                    raise SystemExit(
                        f"Wire V3 package root is missing: {args.package_root}"
                    )
            elif args.envelope is not None or args.package_root is not None:
                raise SystemExit(
                    "Wire V3 query/status must not carry envelope or package payload"
                )
            if args.action == "ack":
                args.receipt_id = _safe_canary_token(
                    str(args.receipt_id), "receipt_id"
                )
            return
        inventory_json = str(
            getattr(args, "device_inventory_json", "") or ""
        ).strip()
        if inventory_json:
            try:
                inventory = json.loads(inventory_json)
            except json.JSONDecodeError as exc:
                raise SystemExit(f"invalid --device-inventory-json: {exc}") from exc
            if not isinstance(inventory, list):
                raise SystemExit("--device-inventory-json must decode to a list")
            args.device_inventory_json = json.dumps(
                inventory,
                ensure_ascii=True,
                separators=(",", ":"),
            )
        if args.kind == "ascendop-engine-accept":
            spec_path = Path(args.spec).resolve()
            if not spec_path.exists():
                raise SystemExit(f"engine spec does not exist: {spec_path}")
            raw = json.loads(spec_path.read_text(encoding="utf-8-sig"))
            if not isinstance(raw, dict):
                raise SystemExit("engine spec must be a JSON object")
            spec_job_id = str(raw.get("engine_job_id") or "")
            if spec_job_id != str(args.engine_job_id):
                raise SystemExit(
                    "--engine-job-id must match spec engine_job_id: "
                    f"arg={args.engine_job_id} spec={spec_job_id}"
                )
            payload_root = getattr(args, "payload_root", None)
            if payload_root is not None and not Path(payload_root).resolve().is_dir():
                raise SystemExit(f"engine payload root is not a directory: {payload_root}")
        elif args.kind == "ascendop-engine-exchange":
            _engine_exchange_jobs(args)
        return
    if getattr(args, "kind", "") == DISTRIBUTED_CANARY_KIND:
        args.experiment_id = _safe_canary_token(
            str(args.experiment_id), "experiment_id"
        )
        args.attempt_id = _safe_canary_token(
            str(args.attempt_id), "attempt_id"
        )
        if int(args.synthetic_duration_ms) < 0:
            raise SystemExit("--synthetic-duration-ms cannot be negative")
        for name in (
            "host_duration_ms",
            "device_duration_ms",
            "export_duration_ms",
        ):
            if int(getattr(args, name, 0) or 0) < 0:
                raise SystemExit(
                    "--" + name.replace("_", "-") + " cannot be negative"
                )
        if str(args.task_class).startswith("engine-"):
            args.engine_root = _safe_engine_relative_path(str(args.engine_root))
        payload_file = getattr(args, "payload_file", None)
        if payload_file is not None and not Path(payload_file).resolve().is_file():
            raise SystemExit(
                "canary payload file does not exist: "
                f"{Path(payload_file).resolve()}"
            )
        return
    if getattr(args, "kind", "") == MSOPGEN_SCAFFOLD_KIND:
        args.output_root = _safe_engine_relative_path(str(args.output_root))
        args.remote_root = str(args.remote_root).rstrip("/") or "/"
        args.client_work_dir = args.remote_root
        for name in ("scaffold_script", "msopgen_input"):
            source = Path(getattr(args, name)).resolve()
            if not source.is_file():
                raise SystemExit(f"msopgen payload source does not exist: {source}")
            setattr(args, name, source)
        return
    if getattr(args, "kind", "") != "ascendop-test":
        return
    op = str(args.op)
    test_version = str(args.test_version)
    if not getattr(args, "vendor", ""):
        args.vendor = _default_vendor(op, test_version)
    if not getattr(args, "source_snapshot", ""):
        args.source_snapshot = (
            f"../operators_testresult/{op}/{test_version}/"
            "submit_snapshot/pending_snapshot/source_snapshot"
        )
    if not getattr(args, "task_case", ""):
        args.task_case = f"../operators_testresult/{op}/{test_version}/submit_snapshot/task_case"
    if not getattr(args, "attack_case", ""):
        args.attack_case = f"../operators_testresult/{op}/{test_version}/submit_snapshot/attack_case"


def _sandbox_profile(args: argparse.Namespace) -> str:
    explicit = getattr(args, "sandbox_profile", None)
    if explicit:
        return explicit
    if _is_b_local(args) and getattr(args, "run_perf", False):
        # msprof's application/export path can reject CANN files after UID/GID
        # remapping in bubblewrap. Use process isolation for perf jobs.
        return "process"
    return "ascend-compile"


def _default_vendor(op: str, test_version: str) -> str:
    op_lower = op.lower()
    version_lower = test_version.lower()
    if version_lower.startswith(op_lower + "_"):
        return f"{version_lower}_gitpartner"
    return f"{op_lower}_{version_lower}_gitpartner"


def _is_lcm_b_local(args: argparse.Namespace) -> bool:
    return getattr(args, "kind", "") in LCM_B_LOCAL_KINDS


def _is_b_local(args: argparse.Namespace) -> bool:
    return getattr(args, "kind", "") in B_LOCAL_KINDS


def _is_engine(args: argparse.Namespace) -> bool:
    return getattr(args, "kind", "") in ENGINE_KINDS


def _b_local_mode(args: argparse.Namespace) -> str:
    if args.kind in {"ascendop-test", "lcm-b-local-full-test"}:
        return "b-local-full-test"
    if args.build_only:
        return "b-local-build-only"
    if args.run_perf:
        return "b-local-smoke-perf"
    return "b-local-smoke"


def _default_request_id(args: argparse.Namespace) -> str:
    if args.kind == "env-probe":
        return f"{args.transport}-env-probe"
    if args.kind == "ascendop-tree-scan":
        return f"{args.transport}-ascendop-tree-scan"
    if args.kind == "b-system-probe":
        return f"{args.transport}-b-system-probe"
    if args.kind == "msprof-probe":
        return f"{args.transport}-msprof-probe"
    if args.kind == "venv-torchnpu-probe":
        return f"{args.transport}-venv-torchnpu-probe"
    if args.kind == DISTRIBUTED_CANARY_KIND:
        return f"distributed-canary-{args.experiment_id}-{args.attempt_id}"
    if args.kind == MSOPGEN_SCAFFOLD_KIND:
        return _safe_canary_token(
            f"msopgen_{args.op}_{args.season}",
            "request_id",
        )
    if args.kind == "ascendop-engine-accept":
        return f"engine-admit-{args.engine_job_id}"
    if args.kind == "ascendop-flow-v3-exchange":
        return (
            f"flow-v3-{args.logical_request_id}-{args.attempt_id}-"
            f"{args.action}"
        )
    if args.kind == "ascendop-engine-exchange":
        return "engine-exchange"
    if args.kind == "ascendop-engine-snapshot":
        return "engine-snapshot"
    if args.kind == "ascendop-engine-collect":
        return f"engine-collect-{args.engine_job_id}"
    if args.kind == "ascendop-engine-configure":
        return "engine-configure"
    if args.kind == DIRECT_ENGINE_RUNTIME_SYNC_KIND:
        return f"engine-runtime-sync-{args.expected_generation}"
    if args.kind == DIRECT_RESIDENT_RUNTIME_SYNC_KIND:
        return f"resident-runtime-sync-{args.expected_generation}"
    if args.kind == "lan-bootstrap":
        return f"server-local-{args.action}-{args.target_role}"
    if args.kind == "lcm-release-test":
        mode = "run" if args.run_remote_test else "preflight"
        return f"{args.transport}-lcm-{args.release.lower()}-{mode}"
    if args.kind == "lcm-b-local-smoke":
        return f"{args.transport}-lcm-{args.release.lower()}-b-local-smoke"
    if args.kind == "lcm-b-local-full-test":
        return f"{args.transport}-lcm-{args.release.lower()}-b-local-full-test"
    if args.kind == "ascendop-b-local-smoke":
        return f"{args.transport}-{args.op.lower()}-{args.release.lower()}-b-local-smoke"
    if args.kind == "ascendop-test":
        return f"{args.test_version}_gitpartner_b_local_both"
    raise ValueError(args.kind)


def _command(args: argparse.Namespace) -> str:
    if args.kind == "env-probe":
        return _env_probe_command()
    if args.kind == "ascendop-tree-scan":
        return _tree_scan_command()
    if args.kind == "b-system-probe":
        return _b_system_probe_command()
    if args.kind == "msprof-probe":
        return _msprof_probe_command()
    if args.kind == "venv-torchnpu-probe":
        return _venv_torchnpu_probe_command(args)
    if args.kind == DISTRIBUTED_CANARY_KIND:
        return _distributed_canary_command(args)
    if args.kind == MSOPGEN_SCAFFOLD_KIND:
        return _msopgen_scaffold_command(args)
    if args.kind == "ascendop-engine-accept":
        return _engine_accept_command(args)
    if args.kind == "ascendop-flow-v3-exchange":
        return _flow_v3_exchange_command(args)
    if args.kind == "ascendop-engine-exchange":
        return _engine_exchange_command(args)
    if args.kind == "ascendop-engine-snapshot":
        return _engine_snapshot_command(args)
    if args.kind == "ascendop-engine-collect":
        return _engine_collect_command(args)
    if args.kind == "ascendop-engine-configure":
        return _engine_configure_command(args)
    if args.kind == DIRECT_ENGINE_RUNTIME_SYNC_KIND:
        return _direct_engine_runtime_sync_command(args)
    if args.kind == DIRECT_RESIDENT_RUNTIME_SYNC_KIND:
        return _direct_resident_runtime_sync_command(args)
    if args.kind == "lan-bootstrap":
        return "echo GITPARTNER_SERVER_ACTION_REQUEST"
    if args.kind == "lcm-release-test":
        return _lcm_release_command(args)
    if _is_b_local(args):
        return _b_local_smoke_command(args)
    raise ValueError(args.kind)


def _direct_engine_runtime_sync_command(args: argparse.Namespace) -> str:
    request_id = args.request_id or _default_request_id(args)
    payload_root = f"input/payloads/{request_id}"
    script = shlex.quote(f"{payload_root}/direct_engine_code_sync.py")
    source = shlex.quote(f"{payload_root}/runtime")
    target_repo = shlex.quote(str(args.target_repo))
    engine_root = shlex.quote(str(args.engine_root))
    generation = shlex.quote(str(args.expected_generation))
    receipt = shlex.quote(
        f"{args.engine_root}/transport/{request_id}/"
        "engine_runtime_sync_receipt.json"
    )
    engine_python = shlex.quote(str(args.engine_python))
    return (
        "set -euo pipefail; "
        f"{engine_python} {script} "
        f"--source {source} "
        f"--target-repo {target_repo} "
        f"--engine-root {engine_root} "
        f"--expected-generation {generation} "
        f"--receipt {receipt}"
    )


def _direct_resident_runtime_sync_command(args: argparse.Namespace) -> str:
    request_id = args.request_id or _default_request_id(args)
    payload_root = f"input/payloads/{request_id}"
    script = shlex.quote(f"{payload_root}/direct_resident_code_sync.py")
    source = shlex.quote(f"{payload_root}/runtime")
    target_repo = shlex.quote(str(args.target_repo))
    config_path = shlex.quote(str(args.resident_config))
    generation = shlex.quote(str(args.expected_generation))
    receipt = shlex.quote(
        f"{args.target_repo}/.partner_state/resident_runtime/receipts/"
        f"{request_id}.json"
    )
    request_token = shlex.quote(str(request_id))
    delay = max(30, int(args.restart_delay_seconds))
    return (
        "set -euo pipefail; "
        f"python3 {script} "
        f"--source {source} "
        f"--target-repo {target_repo} "
        f"--config {config_path} "
        f"--expected-generation {generation} "
        f"--request-id {request_token} "
        f"--receipt {receipt} "
        f"--restart-delay-seconds {delay}"
    )


def _engine_runtime_activation(args: argparse.Namespace) -> str:
    engine_root = shlex.quote(str(args.engine_root))
    return (
        f"ENGINE_RUNTIME_BASE={engine_root}; "
        'ENGINE_RUNTIME_GENERATION="$(cat '
        '"$ENGINE_RUNTIME_BASE/runtime/current" 2>/dev/null || true)"; '
        'case "$ENGINE_RUNTIME_GENERATION" in '
        "''|*[!0123456789abcdef]*) ENGINE_RUNTIME_GENERATION='';; esac; "
        'if [ -n "$ENGINE_RUNTIME_GENERATION" ]; then '
        'ENGINE_RUNTIME_SRC="$ENGINE_RUNTIME_BASE/runtime/generations/'
        '$ENGINE_RUNTIME_GENERATION/src"; '
        'if [ -d "$ENGINE_RUNTIME_SRC/limited_remote_partner" ]; then '
        'export PYTHONPATH="$ENGINE_RUNTIME_SRC${PYTHONPATH:+:$PYTHONPATH}"; '
        "fi; fi; "
    )


def _msopgen_scaffold_command(args: argparse.Namespace) -> str:
    command = [
        "python",
        "scripts/msopgen_scaffold.py",
        str(args.op),
        "--season",
        str(args.season),
        "--soc",
        str(args.soc),
        "--framework",
        str(args.framework),
        "--language",
        str(args.language),
        "--output-root",
        str(args.output_root),
        "--execute",
    ]
    if args.allow_existing:
        command.append("--allow-existing")
    return (
        "cd "
        + shlex.quote(str(args.remote_root))
        + " && "
        + " ".join(shlex.quote(item) for item in command)
    )


def _distributed_canary_command(args: argparse.Namespace) -> str:
    request_id = args.request_id or _default_request_id(args)
    payload_path = (
        f"input/payloads/{request_id}/payload.bin"
        if getattr(args, "payload_file", None)
        else ""
    )
    target_nodes = getattr(args, "target_node", []) or []
    identity = {
        "experiment_id": str(args.experiment_id),
        "attempt_id": str(args.attempt_id),
        "request_id": request_id,
        "target_endpoint_id": str(args.target_endpoint_id or ""),
        "target_node_id": str(target_nodes[0] if target_nodes else ""),
        "target_environment_id": str(args.target_environment_id or ""),
        "target_gateway_id": str(args.target_gateway_id or ""),
        "target_transport_mode": str(args.target_transport_mode or ""),
        "target_generation": str(args.registration_generation or ""),
        "task_class": str(args.task_class),
        "workflow_ingest": False,
    }
    command = [
        "python3",
        "-m",
        "limited_remote_partner.engine.distributed_canary",
        "--output",
        f"canary_results/{request_id}/result.json",
        "--identity-json",
        json.dumps(identity, sort_keys=True, separators=(",", ":")),
        "--task-class",
        str(args.task_class),
        "--synthetic-duration-ms",
        str(args.synthetic_duration_ms),
        "--host-duration-ms",
        str(getattr(args, "host_duration_ms", 0)),
        "--device-duration-ms",
        str(getattr(args, "device_duration_ms", 0)),
        "--export-duration-ms",
        str(getattr(args, "export_duration_ms", 0)),
        "--failure-mode",
        str(args.failure_mode),
    ]
    if payload_path:
        command.extend(["--payload", payload_path])
    if str(args.task_class).startswith("engine-"):
        command.extend(["--engine-root", str(args.engine_root)])
    return " ".join(shlex.quote(item) for item in command)


def _engine_accept_command(args: argparse.Namespace) -> str:
    request_id = args.request_id or _default_request_id(args)
    engine_root = shlex.quote(str(args.engine_root))
    engine_python = shlex.quote(str(args.engine_python))
    transport_dir = shlex.quote(f"{args.engine_root}/transport/{request_id}")
    spec_path = shlex.quote(f"input/payloads/{request_id}/engine_job.json")
    payload_path = shlex.quote(f"input/payloads/{request_id}/payload")
    payload_arg = f" --payload-root {payload_path}" if getattr(args, "payload_root", None) else ""
    return (
        "set -euo pipefail; "
        + _engine_runtime_activation(args)
        +
        f"mkdir -p {transport_dir}; "
        f"{engine_python} -m limited_remote_partner.cli.test_engine_cli --root {engine_root} init >/dev/null; "
        f"{engine_python} -m limited_remote_partner.cli.test_engine_cli --root {engine_root} "
        "start --interval-seconds 0.25 >/dev/null; "
        f"{engine_python} -m limited_remote_partner.cli.test_engine_cli --root {engine_root} "
        f"submit --spec {spec_path}{payload_arg} >/dev/null; "
        f"{engine_python} -m limited_remote_partner.cli.test_engine_cli --root {engine_root} "
        f"transport-status > {transport_dir}/engine_status.json"
    )


def _flow_v3_exchange_command(args: argparse.Namespace) -> str:
    transport_request_id = args.request_id or _default_request_id(args)
    transport_dir = f"{args.engine_root}/transport/{transport_request_id}"
    observation = f"{transport_dir}/flow_v3_observation.json"
    command = [
        str(args.engine_python),
        "-m",
        "limited_remote_partner.endpoint.flow_v3_endpoint",
        "--engine-root",
        str(args.engine_root),
        "--endpoint-id",
        str(args.target_endpoint_id),
        "--endpoint-generation",
        str(args.endpoint_generation),
        str(args.action),
    ]
    if args.action == "accept":
        payload_root = f"input/payloads/{transport_request_id}"
        command.extend(
            [
                "--envelope",
                f"{payload_root}/envelope.json",
                "--package-root",
                f"{payload_root}/package",
            ]
        )
    elif args.action == "query":
        command.extend(
            [
                "--request-id",
                str(args.logical_request_id),
                "--attempt-id",
                str(args.attempt_id),
                "--return-root",
                f"{transport_dir}/flow_v3_return",
            ]
        )
    elif args.action == "ack":
        command.extend(
            [
                "--request-id",
                str(args.logical_request_id),
                "--attempt-id",
                str(args.attempt_id),
                "--receipt-id",
                str(args.receipt_id),
            ]
        )
    temporary = f"{observation}.tmp"
    return (
        "set -euo pipefail; "
        + _engine_runtime_activation(args)
        + f"mkdir -p {shlex.quote(transport_dir)}; "
        + "set +e; "
        + shlex.join(command)
        + f" > {shlex.quote(temporary)}; rc=$?; "
        + "set -e; "
        + f"mv {shlex.quote(temporary)} {shlex.quote(observation)}; "
        + "exit $rc"
    )


def _engine_exchange_command(args: argparse.Namespace) -> str:
    command = [
        str(args.engine_python),
        "-m",
        "limited_remote_partner.cli.test_engine_cli",
        *_engine_exchange_cli_args(args),
    ]
    return (
        "set -euo pipefail; "
        + _engine_runtime_activation(args)
        + shlex.join(command)
        + " >/dev/null"
    )


def _engine_exchange_cli_args(args: argparse.Namespace) -> list[str]:
    request_id = args.request_id or _default_request_id(args)
    capacity = ["--max-inflight", str(max(1, int(args.max_inflight)))]
    standby_slots = getattr(args, "standby_slots", None)
    if standby_slots is not None:
        capacity.extend(["--standby-slots", str(max(0, int(standby_slots)))])
    if args.active_job_slots is not None:
        capacity.extend(["--active-job-slots", str(max(1, int(args.active_job_slots)))])
    if args.host_slots is not None:
        capacity.extend(["--host-slots", str(max(1, int(args.host_slots)))])
    for option in (
        "host_cpu_weight_capacity",
        "host_memory_mb_capacity",
        "host_io_weight_capacity",
        "cold_build_slots",
        "cache_hit_slots",
    ):
        candidate = getattr(args, option, None)
        if candidate is not None:
            capacity.extend(
                ["--" + option.replace("_", "-"), str(max(1, int(candidate)))]
            )
    if getattr(args, "device_slots", None) is not None:
        capacity.extend(["--device-slots", str(max(1, int(args.device_slots)))])
    if getattr(args, "device_inventory_json", None):
        capacity.extend(
            ["--device-inventory-json", str(args.device_inventory_json)]
        )
    if args.export_slots is not None:
        capacity.extend(["--export-slots", str(max(1, int(args.export_slots)))])
    for option in (
        "return_backlog_soft_limit_bytes",
        "return_backlog_hard_limit_bytes",
        "return_backlog_soft_limit_jobs",
        "return_backlog_hard_limit_jobs",
    ):
        candidate = getattr(args, option, None)
        if candidate is not None:
            capacity.extend(["--" + option.replace("_", "-"), str(max(0, int(candidate)))])
    capacity.append("--drain" if args.drain else "--resume")
    acknowledgements: list[str] = []
    for job_id, receipt_id in _engine_snapshot_ack_pairs(args):
        acknowledgements.extend(["--ack-return", f"{job_id}={receipt_id}"])
    for job_id, receipt_id in _engine_required_ack_pairs(args):
        acknowledgements.extend(["--ack-required", f"{job_id}={receipt_id}"])
    return [
        "--root",
        str(args.engine_root),
        "exchange",
        "--manifest",
        f"input/payloads/{request_id}/engine_exchange.json",
        "--transport-dir",
        f"{args.engine_root}/transport/{request_id}",
        *capacity,
        "--wait-ready-seconds",
        str(max(0.0, float(getattr(args, "wait_ready_seconds", 0.0) or 0.0))),
        *acknowledgements,
    ]


def _engine_snapshot_command(args: argparse.Namespace) -> str:
    request_id = args.request_id or _default_request_id(args)
    engine_root = shlex.quote(str(args.engine_root))
    engine_python = shlex.quote(str(args.engine_python))
    transport_dir = shlex.quote(f"{args.engine_root}/transport/{request_id}")
    acknowledgements = "".join(
        f"{engine_python} -m limited_remote_partner.cli.test_engine_cli --root {engine_root} "
        f"ack-return --engine-job-id {shlex.quote(job_id)} "
        f"--receipt-id {shlex.quote(receipt_id)} >/dev/null; "
        for job_id, receipt_id in _engine_snapshot_ack_pairs(args)
    )
    required_acknowledgements = "".join(
        f"{engine_python} -m limited_remote_partner.cli.test_engine_cli --root {engine_root} "
        f"ack-required --engine-job-id {shlex.quote(job_id)} "
        f"--receipt-id {shlex.quote(receipt_id)} >/dev/null; "
        for job_id, receipt_id in _engine_required_ack_pairs(args)
    )
    return (
        "set -euo pipefail; "
        + _engine_runtime_activation(args)
        +
        f"mkdir -p {transport_dir}; "
        f"{engine_python} -m limited_remote_partner.cli.test_engine_cli --root {engine_root} "
        "start --interval-seconds 0.25 >/dev/null; "
        + acknowledgements
        + required_acknowledgements
        +
        f"{engine_python} -m limited_remote_partner.cli.test_engine_cli --root {engine_root} "
        f"service-status > {transport_dir}/engine_service_status.json; "
        f"{engine_python} -c "
        + shlex.quote(
            "import json,sys; from limited_remote_partner.engine import test_engine; "
            "print(json.dumps({'python_executable': sys.executable, "
            "'module_path': str(test_engine.__file__), "
            "'current_code_generation': test_engine.engine_code_generation(), "
            "'sys_path': sys.path}, sort_keys=True))"
        )
        + f" > {transport_dir}/engine_runtime.json; "
        f"{engine_python} -m limited_remote_partner.cli.test_engine_cli --root {engine_root} "
        f"transport-status > {transport_dir}/engine_status.json; "
        f"{engine_python} -m limited_remote_partner.cli.test_engine_cli --root {engine_root} "
        f"return-ready > {transport_dir}/return_ready.json; "
        f"{engine_python} -m limited_remote_partner.cli.test_engine_cli --root {engine_root} "
        f"export-ready --destination {transport_dir}/ready_jobs "
        f"--archive {transport_dir}/ready_jobs.tar "
        f"> {transport_dir}/ready_export.json"
    )


def _engine_snapshot_ack_pairs(args: argparse.Namespace) -> list[tuple[str, str]]:
    return _engine_ack_pairs(args, attribute="ack_return", option="--ack-return")


def _engine_required_ack_pairs(args: argparse.Namespace) -> list[tuple[str, str]]:
    return _engine_ack_pairs(
        args,
        attribute="ack_required",
        option="--ack-required",
    )


def _engine_ack_pairs(
    args: argparse.Namespace,
    *,
    attribute: str,
    option: str,
) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for raw in getattr(args, attribute, []) or []:
        job_id, separator, receipt_id = str(raw).partition("=")
        if not separator or not job_id or not receipt_id:
            raise SystemExit(f"{option} must be ENGINE_JOB_ID=RECEIPT_ID")
        if any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-" for char in job_id):
            raise SystemExit(f"unsafe {option} engine job id: {job_id}")
        if any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-" for char in receipt_id):
            raise SystemExit(f"unsafe {option} receipt id: {receipt_id}")
        pairs.append((job_id, receipt_id))
    return pairs


def _engine_collect_command(args: argparse.Namespace) -> str:
    request_id = args.request_id or _default_request_id(args)
    engine_root = shlex.quote(str(args.engine_root))
    engine_python = shlex.quote(str(args.engine_python))
    job_id = shlex.quote(str(args.engine_job_id))
    receipt_id = shlex.quote(str(args.receipt_id))
    if getattr(args, "ack_only", False):
        transport_dir = shlex.quote(f"{args.engine_root}/transport/{request_id}")
        return (
            "set -euo pipefail; "
            + _engine_runtime_activation(args)
            +
            f"mkdir -p {transport_dir}; "
            f"test -f {engine_root}/jobs/{job_id}/terminal.json; "
            f"{engine_python} -m limited_remote_partner.cli.test_engine_cli --root {engine_root} "
            f"ack-return --engine-job-id {job_id} --receipt-id {receipt_id} "
            f"> {transport_dir}/return_receipt.json"
        )
    return (
        "set -euo pipefail; "
        + _engine_runtime_activation(args)
        +
        f"test -f {engine_root}/jobs/{job_id}/terminal.json; "
        f"test -f {engine_root}/jobs/{job_id}/state.json; "
        f"test -f {engine_root}/jobs/{job_id}/artifact_manifest.json; "
        f"test -d {engine_root}/jobs/{job_id}/result_bundle; "
        f"echo ENGINE_JOB_SNAPSHOTTED:{job_id}"
    )


def _engine_configure_command(args: argparse.Namespace) -> str:
    request_id = args.request_id or _default_request_id(args)
    engine_root = shlex.quote(str(args.engine_root))
    engine_python = shlex.quote(str(args.engine_python))
    transport_dir = shlex.quote(f"{args.engine_root}/transport/{request_id}")
    capacity = ["--max-inflight", str(max(1, int(args.max_inflight)))]
    standby_slots = getattr(args, "standby_slots", None)
    if standby_slots is not None:
        capacity.extend(["--standby-slots", str(max(0, int(standby_slots)))])
    if args.active_job_slots is not None:
        capacity.extend(
            ["--active-job-slots", str(max(1, int(args.active_job_slots)))]
        )
    if args.host_slots is not None:
        capacity.extend(["--host-slots", str(max(1, int(args.host_slots)))])
    for option in (
        "host_cpu_weight_capacity",
        "host_memory_mb_capacity",
        "host_io_weight_capacity",
        "cold_build_slots",
        "cache_hit_slots",
    ):
        value = getattr(args, option, None)
        if value is not None:
            capacity.extend(
                ["--" + option.replace("_", "-"), str(max(1, int(value)))]
            )
    if getattr(args, "device_slots", None) is not None:
        capacity.extend(["--device-slots", str(max(1, int(args.device_slots)))])
    if getattr(args, "device_inventory_json", None):
        capacity.extend(
            ["--device-inventory-json", str(args.device_inventory_json)]
        )
    if args.export_slots is not None:
        capacity.extend(["--export-slots", str(max(1, int(args.export_slots)))])
    for option in (
        "return_backlog_soft_limit_bytes",
        "return_backlog_hard_limit_bytes",
        "return_backlog_soft_limit_jobs",
        "return_backlog_hard_limit_jobs",
    ):
        value = getattr(args, option, None)
        if value is not None:
            capacity.extend(["--" + option.replace("_", "-"), str(max(0, int(value)))])
    if args.drain:
        capacity.append("--drain")
    elif args.resume:
        capacity.append("--resume")
    capacity_args = " ".join(shlex.quote(item) for item in capacity)
    return (
        "set -euo pipefail; "
        + _engine_runtime_activation(args)
        +
        f"mkdir -p {transport_dir}; "
        f"{engine_python} -m limited_remote_partner.cli.test_engine_cli --root {engine_root} "
        "init >/dev/null; "
        f"{engine_python} -m limited_remote_partner.cli.test_engine_cli --root {engine_root} "
        "start --interval-seconds 0.25 >/dev/null; "
        f"{engine_python} -m limited_remote_partner.cli.test_engine_cli --root {engine_root} "
        f"set-capacity {capacity_args} >/dev/null; "
        f"{engine_python} -m limited_remote_partner.cli.test_engine_cli --root {engine_root} "
        f"transport-status > {transport_dir}/engine_status.json"
    )


def _engine_exchange_jobs(args: argparse.Namespace) -> list[dict[str, str]]:
    manifest_path = Path(args.manifest).resolve()
    if not manifest_path.is_file():
        raise SystemExit(f"engine exchange manifest does not exist: {manifest_path}")
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"invalid engine exchange manifest: {exc}") from exc
    raw_jobs = payload.get("jobs") if isinstance(payload, dict) else None
    if not isinstance(raw_jobs, list):
        raise SystemExit("engine exchange manifest jobs must be a list")
    if len(raw_jobs) > 64:
        raise SystemExit("engine exchange manifest exceeds 64 jobs")
    result: list[dict[str, str]] = []
    seen: set[str] = set()
    for raw in raw_jobs:
        if not isinstance(raw, dict):
            raise SystemExit("engine exchange job must be an object")
        job_id = str(raw.get("engine_job_id") or "")
        if not job_id or any(
            char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
            for char in job_id
        ):
            raise SystemExit(f"unsafe engine exchange job id: {job_id}")
        if job_id in seen:
            raise SystemExit(f"duplicate engine exchange job id: {job_id}")
        seen.add(job_id)
        spec_path = Path(str(raw.get("spec") or "")).resolve()
        if not spec_path.is_file():
            raise SystemExit(f"engine exchange spec does not exist: {spec_path}")
        try:
            spec = json.loads(spec_path.read_text(encoding="utf-8-sig"))
        except json.JSONDecodeError as exc:
            raise SystemExit(f"invalid engine exchange spec {spec_path}: {exc}") from exc
        if not isinstance(spec, dict) or str(spec.get("engine_job_id") or "") != job_id:
            raise SystemExit(
                f"engine exchange job id does not match spec: job={job_id} spec={spec_path}"
            )
        admission_mode = str(raw.get("admission_mode") or "accept")
        if admission_mode not in {"accept", "standby"}:
            raise SystemExit(
                f"unsupported engine exchange admission_mode: {admission_mode}"
            )
        record = {
            "engine_job_id": job_id,
            "spec": str(spec_path),
            "admission_mode": admission_mode,
        }
        payload_root = str(raw.get("payload_root") or "")
        if payload_root:
            payload_path = Path(payload_root).resolve()
            if not payload_path.is_dir():
                raise SystemExit(
                    f"engine exchange payload root is not a directory: {payload_path}"
                )
            record["payload_root"] = str(payload_path)
        result.append(record)
    return result


def _engine_exchange_cancellations(args: argparse.Namespace) -> list[dict[str, str]]:
    manifest_path = Path(args.manifest).resolve()
    if not manifest_path.is_file():
        raise SystemExit(f"engine exchange manifest does not exist: {manifest_path}")
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"invalid engine exchange manifest: {exc}") from exc
    raw_items = payload.get("standby_cancellations", []) if isinstance(payload, dict) else []
    if not isinstance(raw_items, list) or len(raw_items) > 64:
        raise SystemExit(
            "engine exchange standby_cancellations must be a list of at most 64 entries"
        )
    result: list[dict[str, str]] = []
    seen: set[str] = set()
    for raw in raw_items:
        if not isinstance(raw, dict):
            raise SystemExit("engine exchange standby cancellation must be an object")
        job_id = str(raw.get("engine_job_id") or "")
        if not job_id or any(
            char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
            for char in job_id
        ):
            raise SystemExit(f"unsafe engine standby cancellation job id: {job_id}")
        if job_id in seen:
            raise SystemExit(f"duplicate engine standby cancellation job id: {job_id}")
        seen.add(job_id)
        reason = str(raw.get("reason") or "").strip() or "standby cancelled by controller"
        if len(reason) > 1000:
            raise SystemExit(f"engine standby cancellation reason is too long: {job_id}")
        result.append({"engine_job_id": job_id, "reason": reason})
    return result


def _safe_engine_relative_path(value: str) -> str:
    raw = value.replace("\\", "/")
    if Path(raw).is_absolute() or raw.startswith("/"):
        raise SystemExit(f"unsafe --engine-root: {value}")
    normalized = raw.strip("/")
    if not normalized or ".." in Path(normalized).parts:
        raise SystemExit(f"unsafe --engine-root: {value}")
    return normalized


def _safe_canary_token(value: str, label: str) -> str:
    if not value or any(
        char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
        for char in value
    ):
        raise SystemExit(f"unsafe --{label.replace('_', '-')}: {value}")
    return value


def _env_probe_command() -> str:
    return (
        "set -euo pipefail; "
        "echo GITPARTNER_ENV_PROBE_START; "
        "hostname; whoami; pwd; python3 --version || python --version; "
        "test -f /opt/ascendop/api.txt && echo B_API_TXT_PRESENT || echo B_API_TXT_MISSING; "
        "date -u +%Y-%m-%dT%H:%M:%SZ; "
        "echo GITPARTNER_ENV_PROBE_DONE"
    )


def _tree_scan_command() -> str:
    return (
        "set -euo pipefail; "
        "echo GITPARTNER_ASCENDOP_TREE_SCAN_START; "
        "echo FEATURE_DIRS; "
        "find /opt/ascendop -maxdepth 5 -type d \\( "
        "-name Remote -o -name TestUtils -o -name operators_workspace -o "
        "-name operators_finish -o -name operators_testresult -o -name Lcm "
        "\\) 2>/dev/null | sort | head -300; "
        "echo GIT_TOPS; "
        "find /opt/ascendop -maxdepth 4 -type d -name .git 2>/dev/null "
        "| sed 's#/.git$##' | sort | head -100; "
        "echo GITPARTNER_ASCENDOP_TREE_SCAN_DONE"
    )


def _b_system_probe_command() -> str:
    return (
        "set -euo pipefail; "
        "echo GITPARTNER_B_SYSTEM_PROBE_START; "
        "hostname; whoami; pwd; "
        "echo CANN_SET_ENV_CANDIDATES; "
        "for f in /usr/local/Ascend/ascend-toolkit/set_env.sh "
        "/usr/local/Ascend/latest/set_env.sh "
        "/opt/Ascend/ascend-toolkit/set_env.sh; do "
        "[ -f \"$f\" ] && echo \"$f\"; done; "
        +
        _source_cann_env_snippet() + "; "
        "test -f /opt/ascendop/api.txt && echo B_API_TXT_PRESENT || echo B_API_TXT_MISSING; "
        "echo COMPILERS; "
        "command -v cmake || true; cmake --version 2>/dev/null | head -1 || true; "
        "command -v gcc || true; gcc --version 2>/dev/null | head -1 || true; "
        "command -v g++ || true; g++ --version 2>/dev/null | head -1 || true; "
        "echo PYTHON_NUMPY; "
        "for py in /usr/bin/python3 /usr/local/bin/python3 "
        "$(command -v python3 || true) $(command -v python || true) "
        "$(find /opt/ascendop -maxdepth 8 -path '*/bin/python' -type f 2>/dev/null | head -80); do "
        "[ -n \"$py\" ] || continue; "
        "\"$py\" - <<'PY' 2>/dev/null && echo PYTHON_NUMPY_OK:$py || echo PYTHON_NUMPY_MISSING:$py\n"
        "import sys\n"
        "import numpy\n"
        "print(sys.executable)\n"
        "print('numpy', numpy.__version__)\n"
        "PY\n"
        "done; "
        "echo PYTHONPATH_AFTER_CANN; "
        "printf '%s\\n' \"${PYTHONPATH:-}\"; "
        "echo PYTHON_TORCH_NPU; "
        "for py in /usr/bin/python3 /usr/local/bin/python3 "
        "$(command -v python3 || true) $(command -v python || true) "
        "$(find /usr/local /usr /opt/ascendop -maxdepth 8 "
        "\\( -path '*/bin/python' -o -path '*/bin/python3' \\) -type f 2>/dev/null | head -120); do "
        "[ -n \"$py\" ] || continue; "
        "\"$py\" - <<'PY' 2>/dev/null && echo PYTHON_TORCH_NPU_OK:$py || echo PYTHON_TORCH_NPU_MISSING:$py\n"
        "import sys\n"
        "import torch\n"
        "import torch_npu\n"
        "print(sys.executable)\n"
        "print('torch', torch.__version__)\n"
        "print('torch_npu', torch_npu.__version__)\n"
        "PY\n"
        "done; "
        "echo TORCH_NPU_PATH_CANDIDATES; "
        "find /usr/local /usr/lib64 /usr/lib /opt/ascendop -maxdepth 8 "
        "\\( -type d -name 'torch_npu*' -o -type f -name 'torch_npu*.so' \\) "
        "2>/dev/null | head -80 || true; "
        "echo GITPARTNER_B_SYSTEM_PROBE_DONE"
    )


def _msprof_probe_command() -> str:
    permission_scan = (
        "import os\n"
        "from pathlib import Path\n"
        "root = Path(os.environ.get('MSPROF_PY', '')).parent\n"
        "needles = ('inconsistent permission', 'Failed to check the permission', "
        "'check', 'permission', 'current owner', 'owner')\n"
        "shown = 0\n"
        "for path in sorted(root.rglob('*.py')):\n"
        "    try:\n"
        "        lines = path.read_text(errors='ignore').splitlines()\n"
        "    except OSError:\n"
        "        continue\n"
        "    for lineno, line in enumerate(lines, 1):\n"
        "        if any(n in line for n in needles):\n"
        "            print(f'{path}:{lineno}:{line[:240]}')\n"
        "            shown += 1\n"
        "            if shown >= 80:\n"
        "                raise SystemExit(0)\n"
    )
    return "; ".join(
        [
            "set -uo pipefail",
            "echo GITPARTNER_MSPROF_PROBE_START",
            'cd "$ASCENDOP_REMOTE_ROOT"',
            _source_cann_env_snippet(),
            'echo USER="$(whoami)"',
            'echo ID="$(id)"',
            'echo ASCEND_HOME_PATH="${ASCEND_HOME_PATH:-}"',
            'echo ASCEND_OPP_PATH="${ASCEND_OPP_PATH:-}"',
            "MSPROF_BIN=$(command -v msprof || true)",
            'echo MSPROF_BIN="$MSPROF_BIN"',
            '[ -n "$MSPROF_BIN" ] && ls -l "$MSPROF_BIN" || true',
            '[ -n "$MSPROF_BIN" ] && file "$MSPROF_BIN" || true',
            'if [ -n "$MSPROF_BIN" ]; then echo MSPROF_WRAPPER_TARGET="$(readlink -f "$MSPROF_BIN" 2>/dev/null || true)"; fi',
            'if [ -n "$MSPROF_BIN" ]; then MSPROF_TARGET="$(readlink -f "$MSPROF_BIN" 2>/dev/null || true)"; [ -n "$MSPROF_TARGET" ] && ls -l "$MSPROF_TARGET" && file "$MSPROF_TARGET"; fi',
            'MSPROF_PY="${ASCEND_HOME_PATH:-/usr/local/Ascend/cann-8.5.0}/tools/profiler/profiler_tool/analysis/msprof/msprof.py"',
            'echo MSPROF_PY="$MSPROF_PY"',
            'export MSPROF_PY',
            'echo MSPROF_OWNERS',
            'ls -ld "${ASCEND_HOME_PATH:-/usr/local/Ascend/cann-8.5.0}" "$(dirname "$MSPROF_PY")" "$MSPROF_PY" || true',
            'echo MSPROF_PERMISSION_GREP',
            "python3 -c " + _shell_quote(permission_scan),
            "TMP_PROBE=$(mktemp -d)",
            'echo MSPROF_HELP',
            'timeout 20 msprof --help >"$TMP_PROBE/msprof_help.txt" 2>&1; echo MSPROF_HELP_RC=$?; sed -n \'1,80p\' "$TMP_PROBE/msprof_help.txt" || true',
            'echo MSPROF_EXPORT_HELP',
            'timeout 20 msprof --export=on --help >"$TMP_PROBE/msprof_export_help.txt" 2>&1; echo MSPROF_EXPORT_HELP_RC=$?; sed -n \'1,80p\' "$TMP_PROBE/msprof_export_help.txt" || true',
            "echo GITPARTNER_MSPROF_PROBE_DONE",
        ]
    )


def _venv_torchnpu_probe_command(args: argparse.Namespace) -> str:
    python_venv = args.python_venv.rstrip("/")
    return "; ".join(
        [
            "set -euo pipefail",
            "echo GITPARTNER_VENV_TORCHNPU_PROBE_START",
            'cd "$ASCENDOP_REMOTE_ROOT"',
            _source_cann_env_snippet(),
            f"PYTHON_VENV={_shell_quote(python_venv)}",
            f"PYTHON_BIN={_shell_quote(python_venv + '/bin/python')}",
            'echo PYTHON_VENV="$PYTHON_VENV"',
            'test -x "$PYTHON_BIN" || { echo PYTHON_VENV_PYTHON_MISSING:$PYTHON_BIN; exit 43; }',
            '"$PYTHON_BIN" --version',
            '"$PYTHON_BIN" -m pip show torch torch-npu torch_npu 2>/dev/null || true',
            '"$PYTHON_BIN" - <<\'PY\'\n'
            "import os\n"
            "import sys\n"
            "import traceback\n"
            "print('PYTHON_EXECUTABLE=' + sys.executable)\n"
            "print('PYTHON_VERSION=' + sys.version.replace('\\n', ' '))\n"
            "print('ASCEND_HOME_PATH=' + os.environ.get('ASCEND_HOME_PATH', ''))\n"
            "print('ASCEND_OPP_PATH=' + os.environ.get('ASCEND_OPP_PATH', ''))\n"
            "print('LD_LIBRARY_PATH=' + os.environ.get('LD_LIBRARY_PATH', ''))\n"
            "try:\n"
            "    import torch\n"
            "    print('TORCH_IMPORT_OK=' + getattr(torch, '__version__', 'unknown'))\n"
            "except Exception:\n"
            "    print('TORCH_IMPORT_FAILED')\n"
            "    traceback.print_exc()\n"
            "    raise SystemExit(43)\n"
            "try:\n"
            "    import torch_npu\n"
            "    print('TORCH_NPU_IMPORT_OK=' + getattr(torch_npu, '__version__', 'unknown'))\n"
            "except Exception:\n"
            "    print('TORCH_NPU_IMPORT_FAILED')\n"
            "    traceback.print_exc()\n"
            "    raise SystemExit(43)\n"
            "PY\n"
            "echo GITPARTNER_VENV_TORCHNPU_PROBE_DONE",
        ]
    )


def _lcm_release_command(args: argparse.Namespace) -> str:
    required = [
        "Remote/run_remote_test.py",
        "scripts/run_full_test.sh",
        "TestUtils",
        "operators_workspace/Lcm",
        f"operators_finish/Lcm/{args.release}",
        f"operators_testresult/Lcm/{args.test_version}",
    ]
    checks = " ".join(_shell_quote(item) for item in required)
    command = [
        "set -euo pipefail",
        'cd "$ASCENDOP_REMOTE_ROOT"',
        "echo GITPARTNER_LCM_RELEASE_TEST_START",
        f"echo ASCENDOP_REMOTE_ROOT=$ASCENDOP_REMOTE_ROOT",
        f"echo LCM_RELEASE={_shell_quote(args.release)} TEST_VERSION={_shell_quote(args.test_version)}",
        "missing=0",
        f"for p in {checks}; do if [ -e \"$p\" ]; then echo REQUIRED_PATH_OK:$p; else echo REQUIRED_PATH_MISSING:$p; missing=1; fi; done",
        "if [ \"$missing\" -ne 0 ]; then echo GITPARTNER_LCM_RELEASE_PREFLIGHT_FAILED; exit 42; fi",
    ]
    if not args.run_remote_test:
        command.extend(
            [
                "echo GITPARTNER_LCM_RELEASE_PREFLIGHT_READY",
                "echo GITPARTNER_LCM_RELEASE_TEST_DONE",
            ]
        )
        return "; ".join(command)

    remote_cmd = [
        "python",
        "Remote/run_remote_test.py",
        "Lcm",
        "--season",
        args.season,
        "--mode",
        args.mode,
        "--vendor",
        args.vendor,
        "--hardware",
        args.hardware,
        "--case-version",
        args.case_version,
    ]
    if args.skip_build:
        remote_cmd.append("--skip-build")
    if args.perf_case_range:
        remote_cmd.extend(["--perf-case-range", args.perf_case_range])
    if args.perf_weighted_target:
        remote_cmd.extend(["--perf-weighted-target", args.perf_weighted_target])
    command.append(" ".join(_shell_quote(item) for item in remote_cmd))
    command.append("echo GITPARTNER_LCM_RELEASE_TEST_DONE")
    return "; ".join(command)


def _lcm_perf_patch_command() -> str:
    patch_code = r'''
from pathlib import Path

p = Path("perf_all.sh")
if not p.exists():
    fallback = r"""#!/usr/bin/env bash
set -u

LABEL="${1:-ascendop_perf}"
RANGE="${PERF_CASE_RANGE:-1..5}"
BASELINE="${PERF_TIME_BASE:-9999999999999}"
PROFILE_EXPORT_DIR="${PROFILE_EXPORT_DIR:-./ascendop_perf_profiles_$LABEL}"
SUMMARY="/tmp/perf_task_case_${LABEL}_summary.txt"
TIMES="$PROFILE_EXPORT_DIR/times.tsv"
PY="${PYTHON_BIN:-python3}"

case_list() {
  case "$RANGE" in
    *..*)
      START="${RANGE%%..*}"
      END="${RANGE##*..}"
      seq "$START" "$END"
      ;;
    *)
      echo "$RANGE"
      ;;
  esac
}

rm -rf "$PROFILE_EXPORT_DIR"
mkdir -p "$PROFILE_EXPORT_DIR"
: > "$TIMES"
echo "AscendOP perf label=$LABEL cases $RANGE baseline=$BASELINE" > "$SUMMARY"

PASS=0
FAIL=0
for i in $(case_list); do
  LOG="$PROFILE_EXPORT_DIR/case${i}.log"
  CASE_OUT="$PROFILE_EXPORT_DIR/case${i}"
  mkdir -p "$CASE_OUT"
  timeout 300 msprof --storage-limit="${MSPROF_STORAGE_LIMIT:-200MB}" --application="$PY test_op.py $i" --output="$CASE_OUT" >"$LOG" 2>&1
  RC=$?
  if [ "$RC" -eq 0 ]; then
    msprof --export=on --output="$CASE_OUT" >>"$LOG" 2>&1 || RC=$?
  fi
  if [ "$RC" -ne 0 ]; then
    FAIL=$((FAIL+1))
    echo "  case${i} perf FAIL: rc=$RC baseline=$BASELINE" >> "$SUMMARY"
    continue
  fi
  TIME_USE=$(python3 - "$CASE_OUT"/PROF*/mindstudio_profiler_output/op_summary*.csv <<'PY'
import csv
import sys

values = []
for pattern in sys.argv[1:]:
    import glob
    for path in glob.glob(pattern):
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                try:
                    values.append(float(row["Task Duration(us)"]))
                except Exception:
                    pass
if not values:
    print(0)
else:
    window = values[20:40] if len(values) >= 40 else values
    print(f"{sum(window) / len(window):.6f}")
PY
)
  if [ "$TIME_USE" = "0" ]; then
    FAIL=$((FAIL+1))
    echo "  case${i} perf FAIL: time_use=0 baseline=$BASELINE" >> "$SUMMARY"
  else
    PASS=$((PASS+1))
    printf '%s\t%s\n' "$i" "$TIME_USE" >> "$TIMES"
    echo "  case${i} perf PASS: time_use=$TIME_USE baseline=$BASELINE" >> "$SUMMARY"
  fi
done

echo "AscendOP perf cases $RANGE: PASS=$PASS FAIL=$FAIL" >> "$SUMMARY"
python3 - "$SUMMARY" "$TIMES" <<'PY'
import json
import os
import sys
from pathlib import Path

summary = Path(sys.argv[1])
times_path = Path(sys.argv[2])
times = []
if times_path.exists():
    for line in times_path.read_text(encoding="utf-8").splitlines():
        case, value = line.split("\t", 1)
        times.append((int(case), float(value)))

meta = {}
meta_path = os.environ.get("ASCENDOP_ATTACK_META", "")
if meta_path and Path(meta_path).exists():
    try:
        meta = json.loads(Path(meta_path).read_text(encoding="utf-8"))
    except Exception:
        meta = {}

formula = str(meta.get("perf_weighted_time_formula") or "")
weights = meta.get("perf_weighted_time_weights")
if isinstance(weights, dict) and weights:
    weights = [weights[f"case{case}"] for case, _value in times]
elif not isinstance(weights, list) or not weights:
    if len(times) > 5:
        raise SystemExit(
            "performance contracts with more than five cases require explicit weights"
        )
    if "*20" in formula and "*2" in formula:
        weights = [20, 2, 1, 1, 1]
        formula = formula or "case1*20 + case2*2 + case3 + case4 + case5"
    else:
        weights = [100, 10, 1, 0.02, 0.002]
        formula = formula or "case1*100 + case2*10 + case3 + case4/50 + case5/500"

weighted = 0.0
details = []
for idx, (case, value) in enumerate(times):
    weight = float(weights[idx]) if idx < len(weights) else 1.0
    weighted += value * weight
    details.append(f"case{idx + 1}(remote_case{case})={value:.6g}*{weight:g}")

score_groups = {}
raw_groups = meta.get("perf_score_groups")
if raw_groups:
    if not isinstance(raw_groups, dict):
        raise SystemExit("perf_score_groups must be an object")
    timing = dict(times)
    covered = set()
    for name, group in raw_groups.items():
        case_ids = [int(item) for item in group.get("case_ids", [])]
        group_weights = group.get("weights", {})
        if not case_ids or any(case not in timing for case in case_ids):
            raise SystemExit(f"score group {name} has incomplete case coverage")
        if covered.intersection(case_ids):
            raise SystemExit(f"score group {name} overlaps another group")
        covered.update(case_ids)
        score_groups[str(name)] = {
            "formula": str(group.get("formula") or "configured group weights"),
            "time_us": sum(
                timing[case] * float(group_weights[f"case{case}"])
                for case in case_ids
            ),
        }
    if covered != set(timing):
        raise SystemExit("perf_score_groups must cover all measured cases")

target_raw = os.environ.get("PERF_WEIGHTED_TARGET", "").strip()
verdict = "BASELINE_ONLY"
if target_raw:
    try:
        verdict = "PASS" if weighted <= float(target_raw) else "FAIL"
    except Exception:
        verdict = "BASELINE_ONLY"

with summary.open("a", encoding="utf-8") as f:
    f.write(f"weighted_formula={formula}\n")
    f.write("weighted_time_unit=us\n")
    f.write(f"weighted_detail={'; '.join(details)}\n")
    f.write(f"weighted_time={weighted:.6f}\n")
    for name, group in score_groups.items():
        f.write(f"score_group_{name}_formula={group['formula']}\n")
        f.write(f"score_group_{name}_time_us={group['time_us']:.6f}\n")
    if target_raw:
        f.write(f"weighted_target={target_raw}\n")
    f.write(f"weighted_verdict={verdict}\n")
    if times:
        f.write("Operator performance and accuracy have passed\n")
PY
cat "$SUMMARY"
test "$FAIL" -eq 0
"""
    p.write_text(fallback, encoding="utf-8")
    p.chmod(0o755)
    print("PERF_FALLBACK_CREATED perf_all.sh")
    raise SystemExit(0)

text = p.read_text(encoding="utf-8")
replacements = {
    'timeout 300 msprof --storage-limit="$MSPROF_STORAGE_LIMIT" --application="python3 test_op.py $i" >"$LOG" 2>&1': 'timeout 300 msprof --storage-limit="$MSPROF_STORAGE_LIMIT" --application="python3 test_op.py $i" --output="$PROFILE_EXPORT_DIR/case$i" >"$LOG" 2>&1',
    "python3 - PROF*/mindstudio_profiler_output/op_summary*.csv <<'PY'": "python3 - \"$PROFILE_EXPORT_DIR/case${i}\"/PROF*/mindstudio_profiler_output/op_summary*.csv <<'PY'",
    'values.append(int(float(row["Task Duration(us)"]) * 1000000))': 'values.append(float(row["Task Duration(us)"]))',
    'if len(values) < 40:\n    print(0)\nelse:\n    print(int(sum(values[20:40]) / 20))': 'if not values:\n    print(0)\nelse:\n    window = values[20:40] if len(values) >= 40 else values\n    print(int(sum(window) / len(window)))',
    'RC=$?\n  set -e\n\n  if [ "$RC" -eq 124 ]; then': 'RC=$?\n  if [ "$RC" -eq 0 ]; then msprof --export=on --output="$PROFILE_EXPORT_DIR/case$i" >>"$LOG" 2>&1 || RC=$?; fi\n  set -e\n\n  if [ "$RC" -eq 124 ]; then',
}
changed = 0
for old, new in replacements.items():
    if old in text and new not in text:
        text = text.replace(old, new)
        changed += 1
p.write_text(text, encoding="utf-8")
print(f"PERF_MSPROF_OUTPUT_PATCHED replacements={changed}")
'''
    return "python3 -c " + _shell_quote(patch_code)


def _b_local_smoke_command(args: argparse.Namespace) -> str:
    fragments = _b_local_smoke_fragments(args)
    if args.build_only:
        return "; ".join(fragments)
    return "; ".join(
        _wrap_legacy_shared_device_lease(
            fragments,
            request_id=args.request_id or _default_request_id(args),
        )
    )


def _wrap_legacy_shared_device_lease(
    fragments: list[str],
    *,
    request_id: str,
) -> list[str]:
    """Run only the legacy correctness/performance section under B-side leases."""

    try:
        device_index = fragments.index("PASS=0; FAIL=0")
        finish_index = fragments.index("phase_mark remote_done")
    except ValueError as exc:
        raise ValueError("legacy B-local device phase boundary changed") from exc
    device_script = "; ".join(["set -euo pipefail", *fragments[device_index:finish_index]])
    wrapper = (
        "python3 -m limited_remote_partner.resources.shared_resource_lease "
        f"--root \"$SHARED_LEASE_ROOT\" --holder-kind legacy-test "
        f"--request-id {shlex.quote(request_id)} "
        f"--engine-job-id {shlex.quote(request_id)} --attempt-id legacy-1 "
        "--stage correctness-performance "
        "--timeout-seconds \"$SHARED_LEASE_TIMEOUT_SECONDS\" "
        "--lock npu --lock performance-measurement -- "
        f"bash -lc {shlex.quote(device_script)}"
    )
    return [
        *fragments[:device_index],
        'SHARED_LEASE_ROOT="${ASCENDOP_SHARED_LEASE_ROOT:-$ASCENDOP_REMOTE_ROOT/.ascendop_test_leases}"',
        'SHARED_LEASE_TIMEOUT_SECONDS="${ASCENDOP_SHARED_LEASE_TIMEOUT_SECONDS:-1800}"',
        "export RUN_DIR VENDOR_DIR PHASE_TIMELINE PYTHON_BIN ASCEND_CUSTOM_OPP_PATH LD_LIBRARY_PATH PATH",
        "export -f phase_mark",
        "phase_mark shared_device_lease_wait_start",
        wrapper,
        "phase_mark shared_device_lease_released",
        *fragments[finish_index:],
    ]


def _b_local_smoke_fragments(args: argparse.Namespace) -> list[str]:
    request_id = args.request_id or _default_request_id(args)
    op = getattr(args, "op", "Lcm")
    payload = f"{args.client_work_dir.rstrip('/')}/input/payloads/{request_id}"
    run_dir = f"{args.client_work_dir.rstrip('/')}/gitpartner_runs/{request_id}"
    vendor_dir = f"{args.client_work_dir.rstrip('/')}/gitpartner_vendors/{args.vendor}"
    case_numbers = _case_sequence_expr(args.case_range)
    perf_range = args.perf_case_range or args.case_range
    perf_label = request_id.replace("'", "_")
    official_template_sync = []
    if str(getattr(args, "season", "") or "") == "CANN-Ladder-910B-CANN90":
        official_template_sync = [
            "mv \"$RUN_DIR/source\" \"$RUN_DIR/source_authoring\"",
            (
                "OFFICIAL_TEMPLATE_ASSET_ARGS=(); "
                "if [ -d \"$PAYLOAD/official_template_assets\" ]; then "
                "OFFICIAL_TEMPLATE_ASSET_ARGS=(--template-root "
                "\"$PAYLOAD/official_template_assets\"); fi"
            ),
            (
                "python3 -m limited_remote_partner.adapters.official_template "
                "--source \"$RUN_DIR/source_authoring\" "
                "--output \"$RUN_DIR/source\" "
                f"--op {_shell_quote(op)} "
                "\"${OFFICIAL_TEMPLATE_ASSET_ARGS[@]}\" --json "
                "> \"$RUN_DIR/OFFICIAL_TEMPLATE_SYNC.json\" || "
                "{ echo OFFICIAL_TEMPLATE_SYNC_FAILED; "
                "cat \"$RUN_DIR/OFFICIAL_TEMPLATE_SYNC.json\" 2>/dev/null || true; "
                "exit 44; }"
            ),
            "echo OFFICIAL_TEMPLATE_SYNC_APPLIED:cannjudge-cann90",
        ]
    return [
            "set -euo pipefail",
            'cd "$ASCENDOP_REMOTE_ROOT"',
            f"echo GITPARTNER_{op.upper()}_B_LOCAL_SMOKE_START",
            f"PAYLOAD={_shell_quote(payload)}",
            f"RUN_DIR={_shell_quote(run_dir)}",
            f"VENDOR_DIR={_shell_quote(vendor_dir)}",
            "test -d \"$PAYLOAD/source_snapshot\" || { echo REQUIRED_PATH_MISSING:$PAYLOAD/source_snapshot; exit 42; }",
            "test -d \"$PAYLOAD/task_case\" || { echo REQUIRED_PATH_MISSING:$PAYLOAD/task_case; exit 42; }",
            "rm -rf \"$RUN_DIR\" \"$VENDOR_DIR\"",
            "mkdir -p \"$RUN_DIR\" \"$VENDOR_DIR\"",
            "PHASE_TIMELINE=\"$RUN_DIR/PHASE_TIMELINE.jsonl\"",
            "phase_mark() { PHASE_NAME=\"$1\"; PHASE_TS=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ); PHASE_EPOCH_MS=$(date +%s%3N); printf '{\"phase\":\"%s\",\"timestamp\":\"%s\",\"epoch_ms\":%s}\\n' \"$PHASE_NAME\" \"$PHASE_TS\" \"$PHASE_EPOCH_MS\" >> \"$PHASE_TIMELINE\"; echo \"ASCENDOP_PHASE:$PHASE_NAME:$PHASE_TS\"; }",
            "export PHASE_TIMELINE",
            "trap 'rc=$?; phase_mark \"remote_exit_${rc}\"; echo ASCENDOP_REMOTE_EXIT_CODE:$rc' EXIT",
            "trap 'phase_mark remote_signal_TERM; exit 143' TERM",
            "trap 'phase_mark remote_signal_INT; exit 130' INT",
            "trap 'phase_mark remote_signal_HUP; exit 129' HUP",
            "phase_mark remote_start",
            "phase_mark payload_copy_start",
            "cp -a \"$PAYLOAD/source_snapshot\" \"$RUN_DIR/source\"",
            "cp -a \"$PAYLOAD/task_case\" \"$RUN_DIR/task_case\"",
            "if [ -d \"$PAYLOAD/attack_case\" ]; then cp -a \"$PAYLOAD/attack_case\" \"$RUN_DIR/attack_case\"; export ASCENDOP_ATTACK_META=\"$RUN_DIR/attack_case/meta.json\"; echo ATTACK_CASE_META:$ASCENDOP_ATTACK_META; fi",
            "phase_mark payload_copy_end",
            "chmod -R u+rwX \"$RUN_DIR/source\" \"$RUN_DIR/task_case\"",
            "[ ! -d \"$RUN_DIR/attack_case\" ] || chmod -R u+rwX \"$RUN_DIR/attack_case\"",
            *official_template_sync,
            "if [ ! -f \"$RUN_DIR/task_case/setup.py\" ] && [ -f \"$RUN_DIR/task_case/task_case/setup.py\" ]; then echo TASK_CASE_NESTED_LAYOUT_FIXED; mv \"$RUN_DIR/task_case\" \"$RUN_DIR/task_case_outer\"; mv \"$RUN_DIR/task_case_outer/task_case\" \"$RUN_DIR/task_case\"; rm -rf \"$RUN_DIR/task_case_outer\"; fi",
            "test -f \"$RUN_DIR/task_case/setup.py\" || { echo TASK_CASE_SETUP_MISSING; find \"$RUN_DIR/task_case\" -maxdepth 3 -mindepth 1 | sort | head -120; exit 46; }",
            "find \"$RUN_DIR/source\" \"$RUN_DIR/task_case\" -type f -name '*.sh' -exec chmod u+x {} +",
            "phase_mark environment_setup_start",
            _source_cann_env_snippet(),
            _prepare_build_python_snippet(
                args.install_build_python_deps,
                args.python_venv,
            ),
            "if [ -f \"$RUN_DIR/source/CMakePresets.json\" ]; then python3 -c \"import json, sys; from pathlib import Path; p=Path(sys.argv[1]); target=sys.argv[2]; py=sys.argv[3]; "
            "data=json.loads(p.read_text(encoding='utf-8')); cv=data['configurePresets'][0]['cacheVariables']; "
            "cv['ASCEND_CANN_PACKAGE_PATH']['value']=target; cv['ASCEND_PYTHON_EXECUTABLE']['value']=py; "
            "p.write_text(json.dumps(data, ensure_ascii=False, indent=4) + '\\n', encoding='utf-8'); "
            "print('CMAKE_PRESET_ASCEND_PATH=' + target); print('CMAKE_PRESET_PYTHON=' + py)\" "
            "\"$RUN_DIR/source/CMakePresets.json\" \"${ASCEND_HOME_PATH:-/usr/local/Ascend/cann-8.5.0}\" \"$BUILD_PYTHON_BIN\"; "
            "else echo CMAKE_PRESET_PATCH_SKIPPED:official-template; fi",
            "ASCENDC_COMPILE_HELPER=\"$RUN_DIR/source/cmake/util/ascendc_compile_kernel.py\"; "
            "if [ -f \"$ASCENDC_COMPILE_HELPER\" ]; then "
            "python3 -c \"import sys; from pathlib import Path; p=Path(sys.argv[1]); py=sys.argv[2]; "
            "text=p.read_text(encoding='utf-8'); "
            "p.write_text(text.replace('HI_PYTHON=python3', 'HI_PYTHON=' + py), encoding='utf-8'); "
            "print('ASCENDC_COMPILE_HI_PYTHON=' + py)\" "
            "\"$ASCENDC_COMPILE_HELPER\" \"$BUILD_PYTHON_BIN\"; "
            "else echo ASCENDC_COMPILE_HI_PYTHON_PATCH_SKIPPED:not-present; fi",
            "phase_mark environment_setup_end",
            "cd \"$RUN_DIR/source\"",
            "phase_mark operator_build_start",
            "bash build.sh > \"$RUN_DIR/build.log\" 2>&1 || { echo BUILD_FAILED; tail -80 \"$RUN_DIR/build.log\"; exit 44; }",
            "phase_mark operator_build_end",
            "RUN_FILE=$(for dir in build_out build; do [ ! -d \"$dir\" ] || find \"$dir\" -maxdepth 1 -type f -name '*.run'; done | sort | head -1)",
            "test -f \"$RUN_FILE\" || { echo RUN_FILE_MISSING; exit 44; }",
            "phase_mark operator_install_start",
            "bash \"$RUN_FILE\" --install-path=\"$VENDOR_DIR\" --quiet > \"$RUN_DIR/install.log\" 2>&1 || { echo INSTALL_FAILED; tail -80 \"$RUN_DIR/install.log\"; exit 45; }",
            "find \"$VENDOR_DIR\" -maxdepth 8 -type f -printf '%P\\n' | sort > \"$RUN_DIR/INSTALL_LAYOUT.txt\"",
            "echo OPERATOR_INSTALL_LAYOUT_START; cat \"$RUN_DIR/INSTALL_LAYOUT.txt\"; echo OPERATOR_INSTALL_LAYOUT_END",
            "OPAPI_LIB=$(find \"$VENDOR_DIR\" -type f -name 'libcust_opapi.so' | sort | head -1)",
            "test -n \"$OPAPI_LIB\" && test -f \"$OPAPI_LIB\" || { echo CUSTOM_OPAPI_LIBRARY_MISSING; exit 45; }",
            "OPAPI_LIB_DIR=$(dirname \"$OPAPI_LIB\")",
            "ASCEND_CUSTOM_OPP_PATH=${OPAPI_LIB_DIR%/op_api/lib}",
            "test -d \"$ASCEND_CUSTOM_OPP_PATH\" || { echo CUSTOM_OPP_ROOT_MISSING:$ASCEND_CUSTOM_OPP_PATH; exit 45; }",
            "{ printf 'export ASCEND_CUSTOM_OPP_PATH=%q\\n' \"$ASCEND_CUSTOM_OPP_PATH\"; printf 'export OPAPI_LIB_DIR=%q\\n' \"$OPAPI_LIB_DIR\"; } > \"$RUN_DIR/engine_operator.env\"",
            "echo CUSTOM_OPAPI_LIBRARY:$OPAPI_LIB",
            "echo ASCEND_CUSTOM_OPP_PATH:$ASCEND_CUSTOM_OPP_PATH",
            "phase_mark operator_install_end",
            *(
                [
                    f"echo \"{op} B-local build/install: PASS\" | tee \"$RUN_DIR/SUMMARY.txt\"",
                    f"echo GITPARTNER_{op.upper()}_B_LOCAL_BUILD_ONLY_DONE",
                ]
                if args.build_only
                else [
                    "phase_mark runtime_setup_start",
                    _select_python_snippet(
                        args.install_runtime_python_deps,
                        args.python_venv,
                    ),
                    'echo PYTHON_BIN="$PYTHON_BIN"',
                    "test -f \"$RUN_DIR/engine_operator.env\"",
                    ". \"$RUN_DIR/engine_operator.env\"",
                    "export LD_LIBRARY_PATH=\"$OPAPI_LIB_DIR:${LD_LIBRARY_PATH:-}\"",
                    "export PATH=\"$(dirname \"$PYTHON_BIN\"):$PATH\"",
                    _engine_identity_command(args),
                    _engine_identity_contract_command(args),
                    "phase_mark runtime_setup_end",
                    "cd \"$RUN_DIR/task_case\"",
                    "phase_mark test_wheel_build_start",
                    "rm -rf build dist && \"$PYTHON_BIN\" setup.py build bdist_wheel > \"$RUN_DIR/whl.log\" 2>&1 || { echo WHL_BUILD_FAILED; tail -80 \"$RUN_DIR/whl.log\"; exit 46; }",
                    "phase_mark test_wheel_build_end",
                    "phase_mark test_wheel_install_start",
                    "\"$PYTHON_BIN\" -m pip install dist/custom_ops*.whl --force-reinstall > \"$RUN_DIR/pip_install.log\" 2>&1 || { echo PIP_INSTALL_FAILED; tail -80 \"$RUN_DIR/pip_install.log\"; exit 47; }",
                    "phase_mark test_wheel_install_end",
                    "PASS=0; FAIL=0",
                    "phase_mark correctness_start",
                    f"for i in {case_numbers}; do phase_mark \"correctness_case_${{i}}_start\"; OUT=$(timeout 240 \"$PYTHON_BIN\" test_op.py \"$i\" 2>&1) || RC=$?; RC=${{RC:-0}}; echo \"$OUT\" > \"$RUN_DIR/case${{i}}.log\"; if [ \"$RC\" -eq 0 ] && echo \"$OUT\" | grep -q 'verify result pass'; then PASS=$((PASS+1)); echo \"case${{i}} verify result pass\"; else FAIL=$((FAIL+1)); echo \"case${{i}} verify result FAIL rc=$RC\"; grep -E 'Traceback|Error|ERROR|failed|timed out|Segmentation' \"$RUN_DIR/case${{i}}.log\" | head -5 || true; fi; phase_mark \"correctness_case_${{i}}_end\"; unset RC; done",
                    "phase_mark correctness_end",
                    f"echo \"{op} B-local cases " + args.case_range + ": PASS=$PASS FAIL=$FAIL\" | tee \"$RUN_DIR/SUMMARY.txt\"",
                    "test \"$FAIL\" -eq 0",
                    *(
                        [
                            f"PERF_LABEL={_shell_quote(perf_label)}",
                            f"export PERF_CASE_RANGE={_shell_quote(perf_range)}",
                            f"export PERF_TIME_BASE={_shell_quote(args.perf_time_base)}",
                            f"export MSPROF_STORAGE_LIMIT={_shell_quote(args.perf_storage_limit)}",
                            *(
                                [f"export PERF_WEIGHTED_TARGET={_shell_quote(args.perf_weighted_target)}"]
                                if args.perf_weighted_target
                                else []
                            ),
                            _lcm_perf_patch_command(),
                            "PERF_RC=0",
                            "phase_mark performance_start",
                            "bash perf_all.sh \"$PERF_LABEL\" > \"$RUN_DIR/perf.log\" 2>&1 || PERF_RC=$?",
                            "phase_mark performance_end",
                            "PERF_TMP_SUMMARY=\"/tmp/perf_task_case_${PERF_LABEL}_summary.txt\"",
                            "if [ -f \"$PERF_TMP_SUMMARY\" ]; then cp \"$PERF_TMP_SUMMARY\" \"$RUN_DIR/PERF_SUMMARY.txt\"; cat \"$RUN_DIR/PERF_SUMMARY.txt\"; else echo PERF_SUMMARY_MISSING:$PERF_TMP_SUMMARY; fi",
                            "find \"$RUN_DIR/task_case\" -maxdepth 8 -type f \\( -name '*.csv' -o -name '*.json' -o -name '*.log' \\) 2>/dev/null | sed \"s#^$RUN_DIR/task_case/##\" | sort | head -200 > \"$RUN_DIR/PERF_DEBUG.txt\" || true",
                            "if [ \"$PERF_RC\" -ne 0 ]; then echo PERF_FAILED rc=$PERF_RC; tail -160 \"$RUN_DIR/perf.log\"; cat \"$RUN_DIR/PERF_DEBUG.txt\"; exit 48; fi",
                            f"echo \"{op} B-local perf " + perf_range + ": PASS\" | tee -a \"$RUN_DIR/SUMMARY.txt\"",
                        ]
                        if args.run_perf
                        else []
                    ),
                ]
            ),
            "phase_mark remote_done",
            f"echo GITPARTNER_{op.upper()}_B_LOCAL_SMOKE_DONE",
        ]


def _source_cann_env_snippet() -> str:
    return (
        "set +u; "
        "CANN_ENV_FOUND=0; "
        "for f in /usr/local/Ascend/ascend-toolkit/set_env.sh "
        "/usr/local/Ascend/latest/set_env.sh "
        "\"${HOME:-/nonexistent}/Ascend/ascend-toolkit/set_env.sh\"; do "
        "if [ -f \"$f\" ]; then source \"$f\"; echo CANN_ENV_SOURCED:$f; CANN_ENV_FOUND=1; break; fi; "
        "done; "
        "set -u; "
        "if [ \"$CANN_ENV_FOUND\" -eq 0 ] && [ -z \"${ASCEND_HOME_PATH:-}\" ] && [ -z \"${ASCEND_AICPU_PATH:-}\" ]; then "
        "echo CANN_ENV_MISSING; exit 43; fi"
    )


def _select_python_snippet(install_missing: bool, python_venv: str) -> str:
    venv_prefix = ""
    if python_venv:
        python_venv_setup = (
            f"PYTHON_VENV={_shell_quote(python_venv)}; "
            f"PYTHON_VENV_BIN={_shell_quote(python_venv.rstrip('/') + '/bin/python')}; "
        )
        if install_missing:
            venv_prefix = (
                python_venv_setup
                + 'if [ -z "$PYTHON_BIN" ]; then '
                'if [ ! -x "$PYTHON_VENV_BIN" ]; then '
                'mkdir -p "$(dirname "$PYTHON_VENV")"; '
                'python3 -m venv "$PYTHON_VENV" || { echo PYTHON_VENV_CREATE_FAILED:$PYTHON_VENV; exit 43; }; '
                'fi; PYTHON_BIN="$PYTHON_VENV_BIN"; fi; '
            )
        else:
            venv_prefix = (
                python_venv_setup
                + 'if [ -z "$PYTHON_BIN" ] && [ -x "$PYTHON_VENV_BIN" ]; then '
                'if runtime_probe "$PYTHON_VENV_BIN"; then '
                'PYTHON_BIN="$PYTHON_VENV_BIN"; '
                'else echo PYTHON_VENV_RUNTIME_REJECTED:$PYTHON_VENV_BIN; fi; fi; '
            )
    runtime_probe_function = (
        "runtime_probe() { local runtime_python=\"$1\"; shift; "
        "python3 -m limited_remote_partner.observability.runtime_readiness "
        '--python "$runtime_python" '
        '--cache-root "$ASCENDOP_ENGINE_CACHE_ROOT/runtime-readiness" '
        '--receipt "$RUN_DIR/RUNTIME_READINESS.json" "$@" --json '
        '> "$RUN_DIR/runtime_readiness.log" 2>&1; }; '
    )
    install_action = ""
    if install_missing:
        install_action = (
            'command -v flock >/dev/null 2>&1 || { echo PYTHON_RUNTIME_FLOCK_MISSING; exit 43; }; '
            'RUNTIME_BOOTSTRAP_LOCK="${PYTHON_VENV:-$(dirname "$PYTHON_BIN")}/.ascendop-runtime-bootstrap.lock"; '
            'mkdir -p "$(dirname "$RUNTIME_BOOTSTRAP_LOCK")"; '
            'exec 9>"$RUNTIME_BOOTSTRAP_LOCK"; flock 9; '
            'if runtime_probe "$PYTHON_BIN" --force-refresh; then '
            'echo PYTHON_RUNTIME_READY_AFTER_LOCK:$PYTHON_BIN; '
            "else "
            '"$PYTHON_BIN" -m ensurepip --upgrade >/dev/null 2>&1 || true; '
            '"$PYTHON_BIN" -m pip install --upgrade pip setuptools wheel || '
            '{ echo PYTHON_RUNTIME_PIP_BOOTSTRAP_FAILED:$PYTHON_BIN | tee -a "$RUN_DIR/SUMMARY.txt"; exit 43; }; '
            'if printf %s "${ASCEND_HOME_PATH:-}" | grep -Eq "/cann-9\\.0(\\.0)?$"; then '
            'echo PYTHON_RUNTIME_PROFILE:cann-9.0-torch-2.10.0; '
            'PYTHON_RUNTIME_PAIR=$("$PYTHON_BIN" -c "import importlib.metadata as m; '
            "print(m.version('torch') + '|' + m.version('torch-npu'))\" 2>/dev/null || true); "
            'if [ "$PYTHON_RUNTIME_PAIR" = "2.10.0+cpu|2.10.0.post2" ]; then '
            'echo PYTHON_RUNTIME_PAIR_ALREADY_INSTALLED:$PYTHON_RUNTIME_PAIR; '
            "else "
            '"$PYTHON_BIN" -m pip install --force-reinstall '
            '"torch==2.10.0" --index-url https://download.pytorch.org/whl/cpu || '
            '{ echo PYTHON_RUNTIME_TORCH_INSTALL_FAILED:$PYTHON_BIN | tee -a "$RUN_DIR/SUMMARY.txt"; exit 43; }; '
            '"$PYTHON_BIN" -m pip install --force-reinstall --no-deps '
            '"torch-npu==2.10.0.post2" || '
            '{ echo PYTHON_RUNTIME_TORCH_NPU_INSTALL_FAILED:$PYTHON_BIN | tee -a "$RUN_DIR/SUMMARY.txt"; exit 43; }; '
            "fi; "
            '"$PYTHON_BIN" -m pip install pyyaml || '
            '{ echo PYTHON_RUNTIME_TORCH_NPU_DEPS_INSTALL_FAILED:$PYTHON_BIN | tee -a "$RUN_DIR/SUMMARY.txt"; exit 43; }; '
            "else "
            '"$PYTHON_BIN" -m pip install torch torch-npu || '
            '"$PYTHON_BIN" -m pip install torch torch_npu || '
            '{ echo PYTHON_RUNTIME_DEPS_INSTALL_FAILED:$PYTHON_BIN | tee -a "$RUN_DIR/SUMMARY.txt"; exit 43; }; '
            "fi; fi; flock -u 9; "
        )
    else:
        install_action = "exit 43; "
    post_install_verify = (
        'if ! runtime_probe "$PYTHON_BIN" --force-refresh; then '
        'echo PYTHON_TORCH_NPU_STILL_MISSING:$PYTHON_BIN | tee -a "$RUN_DIR/SUMMARY.txt"; '
        'tail -80 "$RUN_DIR/runtime_readiness.log" 2>/dev/null || true; exit 43; fi; '
        if install_missing
        else ""
    )
    registered_runtime_scan = (
        "if [ -z \"$PYTHON_BIN\" ]; then "
        "for py in /usr/bin/python3 /usr/local/bin/python3 "
        "$(command -v python3 || true) $(command -v python || true) "
        "$(find /usr/local /usr /opt /workspace \"${HOME:-/nonexistent}\" -maxdepth 8 "
        "\\( -path '*/bin/python' -o -path '*/bin/python3' \\) -type f 2>/dev/null | head -120); do "
        "[ -n \"$py\" ] && [ -x \"$py\" ] || continue; "
        'if runtime_probe "$py"; then PYTHON_BIN="$py"; break; fi; '
        "done; "
        "fi; "
        if not python_venv or not install_missing
        else ""
    )
    runtime_test_deps = (
        "RUNTIME_TEST_DEPS_MISSING=$(\"$PYTHON_BIN\" - <<'PY'\n"
        "missing = []\n"
        "checks = [('expecttest', 'import expecttest')]\n"
        "for package, code in checks:\n"
        "    try:\n"
        "        exec(code, {})\n"
        "    except Exception:\n"
        "        missing.append(package)\n"
        "print(' '.join(missing))\n"
        "PY\n"
        "); "
        "if [ -n \"$RUNTIME_TEST_DEPS_MISSING\" ]; then "
        "echo PYTHON_RUNTIME_TEST_DEPS_MISSING:$RUNTIME_TEST_DEPS_MISSING; "
        + (
            "\"$PYTHON_BIN\" -m ensurepip --upgrade >/dev/null 2>&1 || true; "
            "\"$PYTHON_BIN\" -m pip install $RUNTIME_TEST_DEPS_MISSING || "
            "{ echo PYTHON_RUNTIME_TEST_DEPS_INSTALL_FAILED:$RUNTIME_TEST_DEPS_MISSING | tee -a \"$RUN_DIR/SUMMARY.txt\"; exit 43; }; "
            "RUNTIME_TEST_DEPS_STILL_MISSING=$(\"$PYTHON_BIN\" - <<'PY'\n"
            "missing = []\n"
            "checks = [('expecttest', 'import expecttest')]\n"
            "for package, code in checks:\n"
            "    try:\n"
            "        exec(code, {})\n"
            "    except Exception:\n"
            "        missing.append(package)\n"
            "print(' '.join(missing))\n"
            "PY\n"
            "); "
            "if [ -n \"$RUNTIME_TEST_DEPS_STILL_MISSING\" ]; then "
            "echo PYTHON_RUNTIME_TEST_DEPS_STILL_MISSING:$RUNTIME_TEST_DEPS_STILL_MISSING | tee -a \"$RUN_DIR/SUMMARY.txt\"; exit 43; fi; "
            if install_missing
            else "echo PYTHON_RUNTIME_TEST_DEPS_INSTALL_DISABLED:$RUNTIME_TEST_DEPS_MISSING | tee -a \"$RUN_DIR/SUMMARY.txt\"; exit 43; "
        )
        + "fi; "
    )
    runtime_python_headers = (
        "PYTHON_HEADER=$(\"$PYTHON_BIN\" -c "
        "\"import os,sysconfig; print(os.path.join(sysconfig.get_path('include'), 'Python.h'))\"); "
        "if [ ! -f \"$PYTHON_HEADER\" ]; then "
        "echo PYTHON_RUNTIME_HEADER_MISSING:$PYTHON_HEADER; "
        "command -v flock >/dev/null 2>&1 || { echo PYTHON_RUNTIME_FLOCK_MISSING; exit 43; }; "
        "RUNTIME_SYSTEM_DEPS_LOCK=\"$ASCENDOP_ENGINE_CACHE_ROOT/runtime-system-deps.lock\"; "
        "mkdir -p \"$(dirname \"$RUNTIME_SYSTEM_DEPS_LOCK\")\"; "
        "exec 8>\"$RUNTIME_SYSTEM_DEPS_LOCK\"; flock 8; "
        "PYTHON_HEADER=$(\"$PYTHON_BIN\" -c "
        "\"import os,sysconfig; print(os.path.join(sysconfig.get_path('include'), 'Python.h'))\"); "
        "if [ ! -f \"$PYTHON_HEADER\" ]; then "
        "PYTHON_DEV_PACKAGE=$(\"$PYTHON_BIN\" -c "
        "\"import sys; print(f'python{sys.version_info.major}.{sys.version_info.minor}-dev')\"); "
        "if [ \"$(id -u)\" -ne 0 ] || ! command -v apt-get >/dev/null 2>&1; then "
        "echo PYTHON_RUNTIME_HEADER_PROVISION_BLOCKED:$PYTHON_DEV_PACKAGE; exit 43; fi; "
        "apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \"$PYTHON_DEV_PACKAGE\" || "
        "{ echo PYTHON_RUNTIME_HEADER_INSTALL_FAILED:$PYTHON_DEV_PACKAGE; exit 43; }; "
        "fi; flock -u 8; "
        "fi; "
        "test -f \"$PYTHON_HEADER\" || { echo PYTHON_RUNTIME_HEADER_STILL_MISSING:$PYTHON_HEADER; exit 43; }; "
        "echo PYTHON_RUNTIME_HEADER_OK:$PYTHON_HEADER; "
    )
    return (
        runtime_probe_function
        + "PYTHON_BIN=${PYTHON_BIN:-}; "
        + venv_prefix
        + registered_runtime_scan
        + "if [ -z \"$PYTHON_BIN\" ]; then "
        "echo PYTHON_TORCH_NPU_MISSING | tee \"$RUN_DIR/SUMMARY.txt\"; "
        "find /usr/local /usr/lib64 /usr/lib /opt /workspace \"${HOME:-/nonexistent}\" -maxdepth 8 "
        "\\( -type d -name 'torch_npu*' -o -type f -name 'torch_npu*.so' \\) "
        "2>/dev/null | sed 's/^/TORCH_NPU_PATH_CANDIDATE:/' | head -80 | tee -a \"$RUN_DIR/SUMMARY.txt\" || true; "
        "exit 43; fi; "
        'if ! runtime_probe "$PYTHON_BIN"; then '
        + "echo PYTHON_TORCH_NPU_MISSING:$PYTHON_BIN | tee \"$RUN_DIR/SUMMARY.txt\"; "
        + 'tail -80 "$RUN_DIR/runtime_readiness.log" 2>/dev/null || true; '
        + install_action
        + post_install_verify
        + "fi; "
        + runtime_test_deps
        + runtime_python_headers
        + "echo PYTHON_TORCH_NPU_OK:$PYTHON_BIN"
    )


def _prepare_build_python_snippet(install_missing: bool, python_venv: str) -> str:
    install_action = (
        "if [ -n \"${PYTHON_VENV_BIN:-}\" ] && [ \"$BUILD_PYTHON_BIN\" = \"$PYTHON_VENV_BIN\" ]; then "
        "\"$BUILD_PYTHON_BIN\" -m ensurepip --upgrade >/dev/null 2>&1 || true; "
        "\"$BUILD_PYTHON_BIN\" -m pip install $BUILD_PYTHON_MISSING || "
        "{ echo PYTHON_BUILD_DEPS_INSTALL_FAILED:$BUILD_PYTHON_MISSING; exit 43; }; "
        "else "
        "mkdir -p \"$RUN_DIR/pydeps\"; "
        "\"$BUILD_PYTHON_BIN\" -m pip install --target \"$RUN_DIR/pydeps\" $BUILD_PYTHON_MISSING || "
        "{ echo PYTHON_BUILD_DEPS_INSTALL_FAILED:$BUILD_PYTHON_MISSING; exit 43; }; "
        "export PYTHONPATH=\"$RUN_DIR/pydeps:${PYTHONPATH:-}\"; "
        "fi; "
        "BUILD_PYTHON_MISSING=$(\"$BUILD_PYTHON_BIN\" - <<'PY'\n"
        "missing = []\n"
        "checks = [('numpy', 'import numpy'), ('sympy', 'import sympy'), ('attrs', \"import attr; attr.s\"), ('psutil', 'import psutil'), ('cloudpickle', 'import cloudpickle'), ('tornado', 'import tornado'), ('decorator', 'import decorator'), ('scipy', 'import scipy.sparse'), ('ml-dtypes', 'import ml_dtypes'), ('jinja2', 'import jinja2'), ('absl-py', 'import absl')]\n"
        "for package, code in checks:\n"
        "    try:\n"
        "        exec(code, {})\n"
        "    except Exception:\n"
        "        missing.append(package)\n"
        "print(' '.join(missing))\n"
        "PY\n"
        "); "
        "if [ -n \"$BUILD_PYTHON_MISSING\" ]; then echo PYTHON_BUILD_DEPS_STILL_MISSING:$BUILD_PYTHON_MISSING; exit 43; fi; "
        if install_missing
        else "exit 43; "
    )
    install_block = (
        "if [ -n \"$BUILD_PYTHON_MISSING\" ]; then "
        "echo PYTHON_BUILD_DEPS_MISSING:$BUILD_PYTHON_MISSING; "
        + install_action
        + "fi; "
    )
    venv_prefix = ""
    if python_venv:
        venv_prefix = (
            f"PYTHON_VENV={_shell_quote(python_venv)}; "
            f"PYTHON_VENV_BIN={_shell_quote(python_venv.rstrip('/') + '/bin/python')}; "
            'if [ ! -x "$PYTHON_VENV_BIN" ]; then '
            + (
                'mkdir -p "$(dirname "$PYTHON_VENV")"; '
                'python3 -m venv "$PYTHON_VENV" || { echo PYTHON_VENV_CREATE_FAILED:$PYTHON_VENV; exit 43; }; '
                if install_missing
                else 'echo PYTHON_VENV_MISSING:$PYTHON_VENV; exit 43; '
            )
            + 'fi; '
        )
    return (
        venv_prefix
        + "BUILD_PYTHON_BIN=${BUILD_PYTHON_BIN:-}; "
        + (
            'if [ -z "$BUILD_PYTHON_BIN" ] && [ -n "${PYTHON_VENV_BIN:-}" ]; then BUILD_PYTHON_BIN="$PYTHON_VENV_BIN"; fi; '
            if python_venv
            else ""
        )
        + "if [ -z \"$BUILD_PYTHON_BIN\" ]; then "
        "for py in /usr/bin/python3 /usr/local/bin/python3 "
        "$(command -v python3 || true) $(command -v python || true) "
        "$(find \"${HOME:-/nonexistent}\" /workspace -maxdepth 8 -path '*/bin/python' -type f 2>/dev/null | head -80); do "
        "[ -n \"$py\" ] || continue; "
        "if \"$py\" - <<'PY' >/dev/null 2>&1\n"
        "import numpy\n"
        "PY\n"
        "then BUILD_PYTHON_BIN=\"$py\"; break; fi; "
        "done; "
        "fi; "
        "if [ -z \"$BUILD_PYTHON_BIN\" ]; then echo PYTHON_BUILD_BIN_MISSING; exit 43; fi; "
        "echo PYTHON_BUILD_BIN:$BUILD_PYTHON_BIN; "
        "BUILD_PYTHON_MISSING=$(\"$BUILD_PYTHON_BIN\" - <<'PY'\n"
        "missing = []\n"
        "checks = [('numpy', 'import numpy'), ('sympy', 'import sympy'), ('attrs', \"import attr; attr.s\"), ('psutil', 'import psutil'), ('cloudpickle', 'import cloudpickle'), ('tornado', 'import tornado'), ('decorator', 'import decorator'), ('scipy', 'import scipy.sparse'), ('ml-dtypes', 'import ml_dtypes'), ('jinja2', 'import jinja2'), ('absl-py', 'import absl')]\n"
        "for package, code in checks:\n"
        "    try:\n"
        "        exec(code, {})\n"
        "    except Exception:\n"
        "        missing.append(package)\n"
        "print(' '.join(missing))\n"
        "PY\n"
        "); "
        + install_block
        + "NUMPY_SITE=$(\"$BUILD_PYTHON_BIN\" -c \"import pathlib, numpy; print(pathlib.Path(numpy.__file__).resolve().parent.parent)\") || "
        "{ echo PYTHON_NUMPY_MISSING:$BUILD_PYTHON_BIN; exit 43; }; "
        "export PYTHONPATH=\"$NUMPY_SITE:${PYTHONPATH:-}\"; "
        "echo PYTHON_NUMPY_OK:$BUILD_PYTHON_BIN; "
        "echo PYTHON_NUMPY_SITE:$NUMPY_SITE; "
        + "echo PYTHON_BUILD_DEPS_OK"
    )


def _engine_identity_command(args: argparse.Namespace) -> str:
    return (
        "python3 -m limited_remote_partner.engine.engine_identity "
        f"--test-version {_shell_quote(args.test_version)} "
        "--source \"$PAYLOAD/source_snapshot\" --task-case \"$PAYLOAD/task_case\" "
        "--attack-case \"$PAYLOAD/attack_case\" --python-bin \"$PYTHON_BIN\" "
        '--output "$RUN_DIR/ENGINE_IDENTITY.json"'
    )


def _engine_identity_contract_command(args: argparse.Namespace) -> str:
    contract_sha256 = str(getattr(args, "test_contract_sha256", "") or "")
    if not contract_sha256:
        return "true"
    fields = {
        "test_contract_sha256": contract_sha256,
        "correctness_case_count": int(args.correctness_case_count),
        "performance_case_count": int(args.performance_case_count),
        "correctness_repetitions": int(args.correctness_repetitions),
        "performance_samples_per_case": int(args.performance_samples_per_case),
    }
    script = (
        "import json,os,sys;"
        "p=sys.argv[1];fields=json.loads(sys.argv[2]);"
        "data=json.load(open(p,encoding='utf-8'));data.update(fields);"
        "tmp=p+'.contract.tmp';"
        "f=open(tmp,'w',encoding='utf-8');"
        "json.dump(data,f,ensure_ascii=True,sort_keys=True,indent=2);"
        "f.write('\\n');f.close();os.replace(tmp,p)"
    )
    return (
        f"python3 -c {_shell_quote(script)} "
        '"$RUN_DIR/ENGINE_IDENTITY.json" '
        f"{_shell_quote(json.dumps(fields, sort_keys=True, separators=(',', ':')))}"
    )


def _case_sequence_expr(case_range: str) -> str:
    raw = str(case_range or "").strip()
    if ".." in raw:
        lo, hi = raw.split("..", 1)
        if not (lo.isdigit() and hi.isdigit()) or int(lo) <= 0 or int(hi) < int(lo):
            raise SystemExit("--case-range must be N, N..M, or a positive case list")
        return f"$(seq {lo} {hi})"
    values = raw.replace(",", " ").split()
    if (
        not values
        or any(not item.isdigit() or int(item) <= 0 for item in values)
        or len(values) != len(set(values))
    ):
        raise SystemExit("--case-range must be N, N..M, or a positive case list")
    return " ".join(values)


def _return_paths(args: argparse.Namespace) -> list[str]:
    if args.kind == "env-probe":
        return []
    if args.kind == "ascendop-tree-scan":
        return []
    if args.kind == "b-system-probe":
        return []
    if args.kind == "msprof-probe":
        return []
    if args.kind == "venv-torchnpu-probe":
        return []
    if args.kind == DISTRIBUTED_CANARY_KIND:
        request_id = args.request_id or _default_request_id(args)
        return [f"canary_results/{request_id}/result.json"]
    if args.kind == "ascendop-engine-accept":
        request_id = args.request_id or _default_request_id(args)
        return [
            f"{args.engine_root}/jobs/{args.engine_job_id}/accepted.json",
            f"{args.engine_root}/transport/{request_id}/engine_status.json",
        ]
    if args.kind == "ascendop-flow-v3-exchange":
        request_id = args.request_id or _default_request_id(args)
        paths = [
            f"{args.engine_root}/transport/{request_id}/"
            "flow_v3_observation.json"
        ]
        if args.action == "query":
            paths.append(
                f"{args.engine_root}/transport/{request_id}/flow_v3_return"
            )
        return paths
    if args.kind == "ascendop-engine-exchange":
        request_id = args.request_id or _default_request_id(args)
        jobs = _engine_exchange_jobs(args)
        return [
            *[
                f"{args.engine_root}/jobs/{item['engine_job_id']}/"
                + (
                    "standby.json"
                    if item.get("admission_mode") == "standby"
                    else "accepted.json"
                )
                for item in jobs
            ],
            f"{args.engine_root}/transport/{request_id}/standby_cancellations.json",
            f"{args.engine_root}/transport/{request_id}/required_acknowledgements.json",
            f"{args.engine_root}/transport/{request_id}/engine_status.json",
            f"{args.engine_root}/transport/{request_id}/engine_service_status.json",
            f"{args.engine_root}/transport/{request_id}/engine_runtime.json",
            f"{args.engine_root}/transport/{request_id}/return_ready.json",
            f"{args.engine_root}/transport/{request_id}/rejected_jobs.txt",
            *[
                f"{args.engine_root}/transport/{request_id}/"
                f"rejected_{item['engine_job_id']}.log"
                for item in jobs
            ],
            f"{args.engine_root}/transport/{request_id}/ready_export.json",
            f"{args.engine_root}/transport/{request_id}/ready_jobs.tar",
            f"{args.engine_root}/transport/{request_id}/exchange_timeline.json",
        ]
    if args.kind == "ascendop-engine-snapshot":
        request_id = args.request_id or _default_request_id(args)
        return [
            f"{args.engine_root}/transport/{request_id}/engine_status.json",
            f"{args.engine_root}/transport/{request_id}/engine_service_status.json",
            f"{args.engine_root}/transport/{request_id}/engine_runtime.json",
            f"{args.engine_root}/transport/{request_id}/return_ready.json",
            f"{args.engine_root}/transport/{request_id}/ready_export.json",
            f"{args.engine_root}/transport/{request_id}/ready_jobs.tar",
        ]
    if args.kind == "ascendop-engine-collect":
        if getattr(args, "ack_only", False):
            request_id = args.request_id or _default_request_id(args)
            return [
                f"{args.engine_root}/transport/{request_id}/return_receipt.json"
            ]
        return [
            f"{args.engine_root}/jobs/{args.engine_job_id}/terminal.json",
            f"{args.engine_root}/jobs/{args.engine_job_id}/state.json",
            f"{args.engine_root}/jobs/{args.engine_job_id}/artifact_manifest.json",
            f"{args.engine_root}/jobs/{args.engine_job_id}/result_bundle",
        ]
    if args.kind == "ascendop-engine-configure":
        request_id = args.request_id or _default_request_id(args)
        return [f"{args.engine_root}/transport/{request_id}/engine_status.json"]
    if args.kind == DIRECT_ENGINE_RUNTIME_SYNC_KIND:
        request_id = args.request_id or _default_request_id(args)
        return [
            f"{args.engine_root}/transport/{request_id}/"
            "engine_runtime_sync_receipt.json"
        ]
    if args.kind == DIRECT_RESIDENT_RUNTIME_SYNC_KIND:
        request_id = args.request_id or _default_request_id(args)
        return [
            f"{args.target_repo}/.partner_state/resident_runtime/receipts/"
            f"{request_id}.json"
        ]
    if args.kind == MSOPGEN_SCAFFOLD_KIND:
        return [f"{args.output_root}/{args.op}"]
    if args.kind == "lcm-release-test":
        return [f"logs/Lcm", f"operators_testresult/Lcm/{args.test_version}"]
    if _is_b_local(args):
        request_id = args.request_id or _default_request_id(args)
        paths = [
            f"gitpartner_runs/{request_id}/SUMMARY.txt",
            f"gitpartner_runs/{request_id}/PHASE_TIMELINE.jsonl",
        ]
        if not args.build_only:
            paths.append(f"gitpartner_runs/{request_id}/ENGINE_IDENTITY.json")
        if args.run_perf:
            paths.append(f"gitpartner_runs/{request_id}/PERF_SUMMARY.txt")
            paths.append(f"gitpartner_runs/{request_id}/perf.log")
            paths.append(f"gitpartner_runs/{request_id}/PERF_DEBUG.txt")
        return paths
    return []


def _payload_paths(args: argparse.Namespace) -> list[str]:
    if (
        args.kind == DISTRIBUTED_CANARY_KIND
        and getattr(args, "payload_file", None)
    ):
        request_id = args.request_id or _default_request_id(args)
        return [f"input/payloads/{request_id}/payload.bin"]
    if args.kind == "ascendop-engine-accept":
        request_id = args.request_id or _default_request_id(args)
        paths = [f"input/payloads/{request_id}/engine_job.json"]
        if getattr(args, "payload_root", None):
            paths.append(f"input/payloads/{request_id}/payload")
        return paths
    if (
        args.kind == "ascendop-flow-v3-exchange"
        and args.action == "accept"
    ):
        request_id = args.request_id or _default_request_id(args)
        return [f"input/payloads/{request_id}"]
    if args.kind == "ascendop-engine-exchange":
        request_id = args.request_id or _default_request_id(args)
        return [f"input/payloads/{request_id}"]
    if args.kind in {
        DIRECT_ENGINE_RUNTIME_SYNC_KIND,
        DIRECT_RESIDENT_RUNTIME_SYNC_KIND,
    }:
        request_id = args.request_id or _default_request_id(args)
        return [f"input/payloads/{request_id}"]
    if args.kind == MSOPGEN_SCAFFOLD_KIND:
        return [
            "scripts/msopgen_scaffold.py",
            f"operators/{args.season}/msopgen_inputs/{args.op.lower()}.json",
        ]
    if _is_b_local(args):
        request_id = args.request_id or _default_request_id(args)
        paths = [
            f"input/payloads/{request_id}/source_snapshot",
            f"input/payloads/{request_id}/task_case",
        ]
        if getattr(args, "attack_case", ""):
            paths.append(f"input/payloads/{request_id}/attack_case")
        return paths
    return []


def _env(args: argparse.Namespace) -> dict[str, str]:
    remote_root = str(args.client_work_dir).rstrip("/") or "/"
    configured_engine_root = str(
        getattr(args, "engine_root", "test_engine_demo") or "test_engine_demo"
    )
    if configured_engine_root.startswith("/"):
        shared_engine_root = configured_engine_root.rstrip("/") or "/"
    else:
        relative_engine_root = configured_engine_root.strip("/")
        shared_engine_root = (
            f"{remote_root}/{relative_engine_root}"
            if remote_root != "/"
            else f"/{relative_engine_root}"
        )
    env = {
        "ASCENDOP_REMOTE_ROOT": args.client_work_dir,
        "ASCENDOP_ENGINE_ROOT": shared_engine_root,
    }
    if args.kind == "lcm-release-test":
        env.update(
            {
                "ASCENDOP_OP": "Lcm",
                "ASCENDOP_RELEASE": args.release,
                "ASCENDOP_TEST_VERSION": args.test_version,
                "ASCENDOP_VENDOR": args.vendor,
                "ASCENDOP_SEASON": args.season,
                "ASCENDOP_MODE": args.mode,
                "ASCENDOP_HARDWARE": args.hardware,
                "ASCENDOP_CASE_VERSION": args.case_version,
            }
        )
    if _is_b_local(args):
        env.update(
            {
                "ASCENDOP_OP": getattr(args, "op", "Lcm"),
                "ASCENDOP_RELEASE": args.release,
                "ASCENDOP_TEST_VERSION": args.test_version,
                "ASCENDOP_VENDOR": args.vendor,
                "ASCENDOP_SEASON": getattr(args, "season", ""),
                "ASCENDOP_HARDWARE": getattr(args, "hardware", ""),
                "ASCENDOP_CASE_VERSION": getattr(args, "case_version", ""),
                "ASCENDOP_MODE": _b_local_mode(args),
                "ASCENDOP_PYTHON_VENV": args.python_venv,
                "ASCENDOP_INSTALL_RUNTIME_PYTHON_DEPS": (
                    "1" if args.install_runtime_python_deps else "0"
                ),
            }
        )
    if args.kind == "venv-torchnpu-probe":
        env.update(
            {
                "ASCENDOP_MODE": "venv-torchnpu-probe",
                "ASCENDOP_PYTHON_VENV": args.python_venv,
            }
        )
    if args.kind == "msprof-probe":
        env.update({"ASCENDOP_MODE": "msprof-probe"})
    if _is_engine(args):
        env.update(
            {
                "ASCENDOP_MODE": args.kind,
                "ASCENDOP_ENGINE_ROOT": str(args.engine_root),
                "ASCENDOP_ENGINE_JOB_ID": str(getattr(args, "engine_job_id", "") or ""),
            }
        )
    if args.kind == MSOPGEN_SCAFFOLD_KIND:
        env.update(
            {
                "ASCENDOP_REMOTE_ROOT": args.remote_root,
                "ASCENDOP_OP": args.op,
                "ASCENDOP_SEASON": args.season,
                "ASCENDOP_MSOPGEN_SOC": args.soc,
            }
        )
    return env


def _server_action_args(args: argparse.Namespace) -> dict[str, str]:
    script_b64 = str(getattr(args, "script_b64", "") or "")
    script_file = getattr(args, "script_file", None)
    if script_file:
        script_path = Path(script_file)
        if not script_path.exists():
            raise SystemExit(f"script file does not exist: {script_path}")
        script_b64 = base64.b64encode(script_path.read_bytes()).decode("ascii")
    if args.action == "server-tmux-command" and not script_b64:
        raise SystemExit("server-tmux-command requires --script-file or --script-b64")
    if (
        args.action in {"lan-inspect-artifact", "lan-sync-artifact"}
        and not args.artifact_profile
    ):
        raise SystemExit(f"{args.action} requires --artifact-profile")
    node_ack_json = ""
    node_ack_file = getattr(args, "node_ack_file", None)
    if args.action == "lan-node-ack":
        if node_ack_file is None or not Path(node_ack_file).is_file():
            raise SystemExit("lan-node-ack requires --node-ack-file")
        try:
            node_ack = json.loads(
                Path(node_ack_file).read_text(encoding="utf-8-sig")
            )
        except json.JSONDecodeError as exc:
            raise SystemExit(f"invalid node acknowledgement JSON: {exc}") from exc
        required = {
            "schema": "ascendop.node-ack.v1",
            "state": "accepted",
        }
        if not isinstance(node_ack, dict) or any(
            node_ack.get(key) != value for key, value in required.items()
        ):
            raise SystemExit("node acknowledgement must be an accepted v1 object")
        for key in ("node_id", "endpoint_id", "generation", "session_id"):
            if not str(node_ack.get(key) or ""):
                raise SystemExit(f"node acknowledgement is missing {key}")
        node_ack_json = json.dumps(
            node_ack,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
    if args.action == "lan-reconcile-request":
        request_id = str(getattr(args, "reconcile_request_id", "") or "")
        job_sha256 = str(
            getattr(args, "reconcile_job_sha256", "") or ""
        ).strip().lower()
        if (
            not request_id
            or any(
                char not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-"
                for char in request_id
            )
        ):
            raise SystemExit(
                "lan-reconcile-request requires a safe --reconcile-request-id"
            )
        if (
            len(job_sha256) != 64
            or any(char not in "0123456789abcdef" for char in job_sha256)
        ):
            raise SystemExit(
                "lan-reconcile-request requires --reconcile-job-sha256"
            )
    if args.action == "endpoint-runtime":
        required = {
            "--worktree": str(args.worktree or ""),
            "--control-branch": str(args.control_branch or ""),
            "--endpoint-config": str(args.endpoint_config or ""),
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise SystemExit(
                "endpoint-runtime requires " + ", ".join(missing)
            )
    payload = {
        "target_role": args.target_role,
        "remote_config": args.remote_config,
        "no_process_fallback": "1" if args.no_process_fallback else "0",
        "import_login_network_env": (
            "1"
            if getattr(args, "import_login_network_env", False)
            else "0"
        ),
    }
    optional = {
        "target_host": args.target_host,
        "target_dir": args.target_dir,
        "remote_staging_dir": args.remote_staging_dir,
        "service_name": args.service_name,
        "cleanup_request_id": args.cleanup_request_id,
        "diagnose_request_id": args.diagnose_request_id,
        "reconcile_request_id": getattr(args, "reconcile_request_id", ""),
        "reconcile_job_sha256": getattr(args, "reconcile_job_sha256", ""),
        "cancel_request_id": args.cancel_request_id,
        "cancel_reason": args.cancel_reason,
        "tmux_session": getattr(args, "tmux_session", ""),
        "artifact_profile": getattr(args, "artifact_profile", ""),
        "endpoint_action": getattr(args, "endpoint_action", ""),
        "source_repo": getattr(args, "source_repo", ""),
        "worktree": getattr(args, "worktree", ""),
        "control_branch": getattr(args, "control_branch", ""),
        "endpoint_config": getattr(args, "endpoint_config", ""),
        "endpoint_role": getattr(args, "endpoint_role", ""),
        "endpoint_remote": getattr(args, "endpoint_remote", ""),
    }
    payload.update({key: value for key, value in optional.items() if value})
    if script_b64:
        payload["script_b64"] = script_b64
    if node_ack_json:
        payload["node_ack_json"] = node_ack_json
    if args.sync_path:
        payload["sync_path"] = ",".join(args.sync_path)
    return payload


def _prepare_payload(repo_dir: Path, args: argparse.Namespace) -> None:
    _normalize_args(args)
    if args.kind in {
        DIRECT_ENGINE_RUNTIME_SYNC_KIND,
        DIRECT_RESIDENT_RUNTIME_SYNC_KIND,
    }:
        if args.request_id is None:
            args.request_id = _default_request_id(args)
        if args.dry_run:
            return
        payload_dir = repo_dir / "input" / "payloads" / args.request_id
        canonical_package_root = os.environ.get(
            "GITPARTNER_CANONICAL_PACKAGE_ROOT", ""
        ).strip()
        source_package = (
            Path(canonical_package_root).resolve()
            if canonical_package_root
            else repo_dir / "src" / "limited_remote_partner"
        )
        generated: dict[Path, bytes] = {}
        if args.kind == DIRECT_ENGINE_RUNTIME_SYNC_KIND:
            helper_name = "direct_engine_code_sync.py"
            helper_source = source_package / "maintenance" / helper_name
            protocol_root = protocol_package_root()
            destinations = {
                payload_dir / helper_name: helper_source,
                **{
                    payload_dir / "runtime" / "limited_remote_partner" / name:
                    source_package / name
                    for name in ENGINE_RUNTIME_FILES
                },
                **{
                    payload_dir / "runtime" / "ascendop_protocol" / name:
                    protocol_root / name
                    for name in SHARED_PROTOCOL_FILES
                },
            }
            generated[payload_dir / "runtime" / "runtime_manifest.json"] = (
                engine_runtime_manifest_bytes(source_package, protocol_root)
            )
        else:
            helper_name = "direct_resident_code_sync.py"
            destinations = {
                payload_dir / helper_name:
                source_package / "maintenance" / helper_name,
                **{
                    payload_dir / "runtime" / name: source_package / name
                    for name in RESIDENT_RUNTIME_FILES
                },
            }
        missing_sources = [
            str(source)
            for source in destinations.values()
            if not source.is_file()
        ]
        if missing_sources:
            raise SystemExit(
                "direct runtime source package is incomplete: "
                + ", ".join(missing_sources)
            )
        if args.append_request and payload_dir.is_dir():
            for destination, source in destinations.items():
                if (
                    not destination.is_file()
                    or destination.read_bytes() != source.read_bytes()
                ):
                    raise SystemExit(
                        "immutable append request payload collision: "
                        f"{args.request_id}"
                    )
            for destination, payload in generated.items():
                if not destination.is_file() or destination.read_bytes() != payload:
                    raise SystemExit(
                        "immutable append request payload collision: "
                        f"{args.request_id}"
                    )
            return
        _reset_payload_dir(payload_dir)
        for destination, source in destinations.items():
            destination.parent.mkdir(parents=True, exist_ok=True)
            _copy_payload_tree(repo_dir, str(source), destination)
        for destination, payload in generated.items():
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(payload)
        return
    if args.kind == DISTRIBUTED_CANARY_KIND:
        if args.request_id is None:
            args.request_id = _default_request_id(args)
        if args.dry_run or not getattr(args, "payload_file", None):
            return
        payload_dir = repo_dir / "input" / "payloads" / args.request_id
        source = Path(args.payload_file).resolve()
        destination = payload_dir / "payload.bin"
        if args.append_request and destination.is_file():
            if destination.read_bytes() != source.read_bytes():
                raise SystemExit(
                    "immutable append request payload collision: "
                    f"{args.request_id}"
                )
            return
        _reset_payload_dir(payload_dir)
        _copy_payload_tree(
            repo_dir,
            str(args.payload_file),
            payload_dir / "payload.bin",
        )
        return
    if args.kind == MSOPGEN_SCAFFOLD_KIND:
        if args.request_id is None:
            args.request_id = _default_request_id(args)
        if args.dry_run:
            return
        payload_dir = repo_dir / "input" / "payloads" / args.request_id
        destinations = {
            payload_dir / "scripts" / "msopgen_scaffold.py": args.scaffold_script,
            (
                payload_dir
                / "operators"
                / args.season
                / "msopgen_inputs"
                / f"{args.op.lower()}.json"
            ): args.msopgen_input,
        }
        if args.append_request and payload_dir.is_dir():
            for destination, source in destinations.items():
                if not destination.is_file() or destination.read_bytes() != source.read_bytes():
                    raise SystemExit(
                        "immutable append request payload collision: "
                        f"{args.request_id}"
                    )
            return
        _reset_payload_dir(payload_dir)
        for destination, source in destinations.items():
            destination.parent.mkdir(parents=True, exist_ok=True)
            _copy_payload_tree(repo_dir, str(source), destination)
        return
    if args.kind == "ascendop-flow-v3-exchange":
        if args.request_id is None:
            args.request_id = _default_request_id(args)
        if args.dry_run or args.action != "accept":
            return
        payload_dir = repo_dir / "input" / "payloads" / args.request_id
        source_envelope = Path(args.envelope).resolve()
        source_package = Path(args.package_root).resolve()
        if args.append_request and payload_dir.is_dir():
            destination_envelope = payload_dir / "envelope.json"
            destination_package = payload_dir / "package"
            if (
                not destination_envelope.is_file()
                or destination_envelope.read_bytes() != source_envelope.read_bytes()
                or not destination_package.is_dir()
                or _directory_manifest_digest(destination_package)
                != _directory_manifest_digest(source_package)
            ):
                raise SystemExit(
                    "immutable Wire V3 transport payload collision: "
                    f"{args.request_id}"
                )
            return
        _reset_payload_dir(payload_dir)
        _copy_payload_tree(
            repo_dir,
            str(source_envelope),
            payload_dir / "envelope.json",
        )
        _copy_payload_tree(
            repo_dir,
            str(source_package),
            payload_dir / "package",
        )
        return
    if args.kind == "ascendop-engine-accept":
        if args.request_id is None:
            args.request_id = _default_request_id(args)
        if args.dry_run:
            return
        payload_dir = repo_dir / "input" / "payloads" / args.request_id
        _reset_payload_dir(payload_dir)
        _copy_payload_tree(repo_dir, str(args.spec), payload_dir / "engine_job.json")
        payload_root = getattr(args, "payload_root", None)
        if payload_root is not None:
            _copy_payload_tree(
                repo_dir,
                str(payload_root),
                payload_dir / "payload",
                archive_large_engine_payload=True,
            )
        return
    if args.kind == "ascendop-engine-exchange":
        if args.request_id is None:
            args.request_id = _default_request_id(args)
        if args.dry_run:
            return
        payload_dir = repo_dir / "input" / "payloads" / args.request_id
        _reset_payload_dir(payload_dir)
        remote_jobs: list[dict[str, str]] = []
        for item in _engine_exchange_jobs(args):
            job_id = str(item["engine_job_id"])
            payload_key = _engine_exchange_payload_key(job_id)
            job_dir = payload_dir / "jobs" / payload_key
            job_dir.mkdir(parents=True, exist_ok=True)
            _copy_payload_tree(repo_dir, str(item["spec"]), job_dir / "engine_job.json")
            remote_item = {
                "engine_job_id": job_id,
                "spec": (
                    f"input/payloads/{args.request_id}/jobs/{payload_key}/engine_job.json"
                ),
                "admission_mode": str(item.get("admission_mode") or "accept"),
            }
            if item.get("payload_root"):
                _copy_payload_tree(
                    repo_dir,
                    str(item["payload_root"]),
                    job_dir / "payload",
                    archive_large_engine_payload=True,
                )
                remote_item["payload_root"] = (
                    f"input/payloads/{args.request_id}/jobs/{payload_key}/payload"
                )
            remote_jobs.append(remote_item)
        remote_cancellations = _engine_exchange_cancellations(args)
        (payload_dir / "engine_exchange.json").write_text(
            json.dumps(
                {
                    "jobs": remote_jobs,
                    "standby_cancellations": remote_cancellations,
                },
                ensure_ascii=True,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        return
    if not _is_b_local(args):
        return
    if not getattr(args, "stage_payload", False):
        return
    if args.request_id is None:
        args.request_id = _default_request_id(args)
    payload_dir = repo_dir / "input" / "payloads" / args.request_id
    if args.dry_run:
        return
    _reset_payload_dir(payload_dir)
    _copy_payload_tree(repo_dir, args.source_snapshot, payload_dir / "source_snapshot")
    _copy_payload_tree(repo_dir, args.task_case, payload_dir / "task_case")
    attack_case = getattr(args, "attack_case", "")
    if attack_case:
        _copy_payload_tree(repo_dir, attack_case, payload_dir / "attack_case")


def _engine_exchange_payload_key(engine_job_id: str) -> str:
    digest = hashlib.sha256(engine_job_id.encode("utf-8")).hexdigest()[:16]
    return f"job-{digest}"


def _reset_payload_dir(payload_dir: Path) -> None:
    filesystem_path = _extended_windows_path(payload_dir)
    if filesystem_path.exists():
        shutil.rmtree(filesystem_path)
    filesystem_path.mkdir(parents=True)


def _copy_payload_tree(
    repo_dir: Path,
    source: str,
    dest: Path,
    *,
    archive_large_engine_payload: bool = False,
) -> None:
    src = Path(source)
    if not src.is_absolute():
        src = repo_dir / src
    src = src.resolve()
    filesystem_src = _extended_windows_path(src)
    filesystem_dest = _extended_windows_path(dest)
    if not filesystem_src.exists():
        raise SystemExit(f"payload source does not exist: {src}")
    if filesystem_src.is_dir():
        if archive_large_engine_payload:
            try:
                stage_payload_tree(
                    filesystem_src,
                    filesystem_dest,
                    max_file_bytes=MAX_PAYLOAD_FILE_BYTES,
                )
            except PayloadArchiveError as exc:
                raise SystemExit(f"cannot archive Engine payload: {exc}") from exc
        else:
            _check_payload_size(filesystem_src)
            shutil.copytree(filesystem_src, filesystem_dest)
    else:
        _check_payload_size(filesystem_src)
        filesystem_dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(filesystem_src, filesystem_dest)


def _check_payload_size(src: Path) -> None:
    files = src.rglob("*") if src.is_dir() else (src,)
    for item in files:
        if item.is_file() and item.stat().st_size > MAX_PAYLOAD_FILE_BYTES:
            raise SystemExit(
                "legacy unarchived payload file exceeds the 1MB per-file Git threshold; "
                "use the Engine payload-archive path: "
                f"{item} ({item.stat().st_size} bytes)"
            )


def _directory_manifest_digest(root: Path) -> str:
    root = root.resolve()
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        if path.is_symlink():
            raise SystemExit(f"payload cannot contain symlinks: {path}")
        if not path.is_file():
            continue
        relative = path.relative_to(root).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(path.stat().st_size.to_bytes(8, "big"))
        with path.open("rb") as handle:
            while True:
                data = handle.read(1024 * 1024)
                if not data:
                    break
                digest.update(data)
    return digest.hexdigest()


def _write_job(
    repo_dir: Path,
    job: dict[str, Any],
    *,
    dry_run: bool,
    append_request: bool = False,
) -> None:
    text = json.dumps(job, ensure_ascii=False, indent=2) + "\n"
    if dry_run:
        sys.stdout.write(text)
        return
    request_id = _safe_canary_token(
        str(job.get("id") or job.get("request_id") or ""),
        "request_id",
    )
    path = (
        repo_dir / "input" / "requests" / request_id / "job.json"
        if append_request
        else repo_dir / "input" / "job.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    if append_request and path.is_file():
        try:
            existing = json.loads(path.read_text(encoding="utf-8-sig"))
            incoming = json.loads(text)
        except json.JSONDecodeError as exc:
            raise SystemExit(
                f"immutable append request is invalid: {path}"
            ) from exc
        if existing != incoming:
            raise SystemExit(
                f"immutable append request identity collision: {request_id}"
            )
        print(f"kept immutable {path}")
        return
    path.write_text(text, encoding="utf-8")
    print(f"wrote {path}")


def _commit_paths(args: argparse.Namespace) -> list[str]:
    request_id = args.request_id or _default_request_id(args)
    append_request = bool(getattr(args, "append_request", False))
    also_commit_request_ids = tuple(
        getattr(args, "also_commit_request_id", ()) or ()
    )
    paths = [
        (
            f"input/requests/{_safe_canary_token(str(request_id), 'request_id')}/job.json"
            if append_request
            else "input/job.json"
        )
    ]
    if also_commit_request_ids and not append_request:
        raise SystemExit(
            "--also-commit-request-id requires --append-request"
        )
    for item in also_commit_request_ids:
        previous = _safe_canary_token(str(item), "also_commit_request_id")
        paths.append(f"input/requests/{previous}/job.json")
        payload = Path(args.repo).resolve() / "input" / "payloads" / previous
        if payload.exists():
            paths.append(f"input/payloads/{previous}")
    if getattr(args, "publish_maintenance_changes", False):
        if args.kind != "lan-bootstrap":
            raise SystemExit(
                "--publish-maintenance-changes is only allowed with lan-bootstrap"
            )
        paths.extend(
            [
                "src",
                "scripts",
                "tests",
                "docs",
                "configs",
                "services",
                "README.md",
                "pyproject.toml",
            ]
        )
    if args.kind in {
        "ascendop-engine-accept",
        "ascendop-engine-exchange",
    } or (
        args.kind == "ascendop-flow-v3-exchange"
        and args.action == "accept"
    ):
        request_id = args.request_id or _default_request_id(args)
        paths.append(f"input/payloads/{request_id}")
    if args.kind in {
        DIRECT_ENGINE_RUNTIME_SYNC_KIND,
        DIRECT_RESIDENT_RUNTIME_SYNC_KIND,
    }:
        request_id = args.request_id or _default_request_id(args)
        paths.append(f"input/payloads/{request_id}")
    if (
        args.kind == DISTRIBUTED_CANARY_KIND
        and getattr(args, "payload_file", None)
    ):
        request_id = args.request_id or _default_request_id(args)
        paths.append(f"input/payloads/{request_id}")
    if args.kind == MSOPGEN_SCAFFOLD_KIND:
        request_id = args.request_id or _default_request_id(args)
        paths.append(f"input/payloads/{request_id}")
    if _is_b_local(args):
        request_id = args.request_id or _default_request_id(args)
        paths.append(f"input/payloads/{request_id}")
    return paths


def _clear_existing_output(repo_dir: Path, output_subdir: str) -> str | None:
    pathspec = _output_pathspec(output_subdir)
    output_path = repo_dir / Path(*pathspec.split("/"))
    if not output_path.exists():
        return None
    if output_path.is_dir():
        for attempt in range(3):
            try:
                shutil.rmtree(output_path)
                break
            except FileNotFoundError:
                if not output_path.exists():
                    break
                if attempt == 2:
                    raise
    else:
        output_path.unlink(missing_ok=True)
    print(f"cleared stale {output_path}")
    return pathspec


def _output_pathspec(output_subdir: str) -> str:
    normalized = output_subdir.replace("\\", "/").strip("/")
    parts = [part for part in normalized.split("/") if part and part != "."]
    if not parts or any(part == ".." for part in parts):
        raise SystemExit(f"unsafe output_subdir: {output_subdir}")
    return "output/" + "/".join(parts)


def _commit_push(
    repo_dir: Path,
    message: str,
    paths: list[str],
    *,
    sync_before_publish: bool = True,
    timeline: list[dict[str, Any]] | None = None,
) -> None:
    steps = timeline if timeline is not None else []
    if sync_before_publish:
        _sync_before_publish(repo_dir, timeline=steps)
    else:
        observed_at = _utc_now_iso()
        steps.append(
            {
                "name": "publish_sync_skipped",
                "started_at": observed_at,
                "finished_at": observed_at,
                "duration_seconds": 0.0,
                "outcome": "success",
                "strategy": "optimistic-engine-control",
            }
        )
    paths = _materialized_publish_paths(repo_dir, paths)
    _timed_step(
        steps,
        "publish_split_index",
        lambda: _maybe_enable_split_index(repo_dir),
    )
    _timed_step(
        steps,
        "publish_add",
        lambda: _run(
            _git_command(repo_dir, ["add", "--sparse", "-A", "--", *paths]),
            repo_dir,
        ),
    )

    def commit_if_changed() -> None:
        if _has_staged_path_changes(repo_dir, paths):
            _run(
                _git_command(
                    repo_dir,
                    [
                        "-c",
                        "status.showUntrackedFiles=no",
                        "commit",
                        "--quiet",
                        "--no-verify",
                        "--no-gpg-sign",
                        "-m",
                        message,
                        "--",
                        *paths,
                    ],
                ),
                repo_dir,
            )
        else:
            print("no GitPartner input changes to commit")

    _timed_step(steps, "publish_commit", commit_if_changed)
    _timed_step(steps, "publish_push", lambda: _push_with_retry(repo_dir))


def _git_index_path(repo_dir: Path) -> Path | None:
    dot_git = repo_dir / ".git"
    if dot_git.is_dir():
        return dot_git / "index"
    if not dot_git.is_file():
        return None
    try:
        marker = dot_git.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    prefix = "gitdir:"
    if not marker.lower().startswith(prefix):
        return None
    git_dir = Path(marker[len(prefix) :].strip())
    if not git_dir.is_absolute():
        git_dir = (repo_dir / git_dir).resolve()
    return git_dir / "index"


def _materialized_publish_paths(repo_dir: Path, paths: list[str]) -> list[str]:
    sparse = _run(
        _git_command(repo_dir, ["config", "--bool", "core.sparseCheckout"]),
        repo_dir,
        check=False,
    )
    stdout = str(sparse.stdout or "")
    if sparse.returncode != 0 or stdout.strip().lower() != "true":
        return paths
    materialized = [path for path in paths if (repo_dir / path).exists()]
    if not materialized:
        raise SystemExit("sparse maintenance publication has no materialized paths")
    return materialized


def _maybe_enable_split_index(repo_dir: Path) -> bool:
    """Keep large endpoint worktree indexes incremental without changing history."""
    if os.environ.get("GITPARTNER_SPLIT_INDEX", "auto").strip().lower() in {
        "0",
        "false",
        "no",
        "off",
    }:
        return False
    threshold = max(
        0,
        _int_from_env("GITPARTNER_SPLIT_INDEX_THRESHOLD_BYTES", 8 * 1024 * 1024),
    )
    index_path = _git_index_path(repo_dir)
    try:
        index_size = index_path.stat().st_size if index_path is not None else 0
    except OSError:
        return False
    if index_size <= threshold:
        return False
    _run(_git_command(repo_dir, ["update-index", "--split-index"]), repo_dir)
    return True


def _sync_before_publish(
    repo_dir: Path,
    *,
    timeline: list[dict[str, Any]] | None = None,
) -> None:
    steps = timeline if timeline is not None else []
    remote_ref = ""

    def fetch() -> None:
        nonlocal remote_ref
        remote_ref = _fetch_target_branch(repo_dir)

    _timed_step(steps, "publish_fetch", fetch)

    fast_forwarded = False

    def fast_forward() -> None:
        nonlocal fast_forwarded
        result = _run(
            _git_command(repo_dir, ["merge", "--ff-only", remote_ref]),
            repo_dir,
            check=False,
        )
        fast_forwarded = result.returncode == 0

    _timed_step(steps, "publish_fast_forward", fast_forward)
    if not fast_forwarded:
        _timed_step(
            steps,
            "publish_rebase",
            lambda: _run(
                _git_command(repo_dir, ["rebase", "--autostash", remote_ref]),
                repo_dir,
            ),
        )


def _has_staged_path_changes(repo_dir: Path, paths: list[str]) -> bool:
    result = _run(
        _git_command(repo_dir, ["diff", "--cached", "--quiet", "--", *paths]),
        repo_dir,
        check=False,
    )
    if result.returncode == 0:
        return False
    if result.returncode == 1:
        return True
    raise SystemExit(result.returncode)


def _push_with_retry(repo_dir: Path) -> None:
    remote = _target_remote()
    branch = _target_branch()
    push = _run(_git_command(repo_dir, ["push", remote, f"HEAD:{branch}"]), repo_dir, check=False)
    if push.returncode == 0:
        return
    print(f"GitPartner push failed; synchronizing {remote}/{branch} and retrying once")
    _sync_before_publish(repo_dir)
    _run(_git_command(repo_dir, ["push", remote, f"HEAD:{branch}"]), repo_dir)


def _wait_for_result(
    repo_dir: Path,
    output_subdir: str,
    timeout_seconds: int,
    *,
    request_kind: str = "",
    timeline: list[dict[str, Any]] | None = None,
) -> None:
    status_path = repo_dir / "output" / output_subdir / "status.json"
    deadline = time.monotonic() + timeout_seconds
    engine_request = request_kind.startswith("engine-")
    if engine_request:
        poll_seconds = max(1, _int_from_env("GITPARTNER_ENGINE_WAIT_POLL_SECONDS", 1))
        initial_grace_seconds = max(
            0,
            _int_from_env("GITPARTNER_ENGINE_WAIT_INITIAL_GRACE_SECONDS", 3),
        )
        wait_strategy = "engine-final-ref-probe"
    else:
        poll_seconds = max(1, _int_from_env("GITPARTNER_WAIT_POLL_SECONDS", 1))
        initial_grace_seconds = 0
        wait_strategy = "remote-ref-probe"
    started_at = _utc_now_iso()
    started = time.monotonic()
    remote_probe_seconds = 0.0
    fetch_merge_seconds = 0.0
    sleep_seconds = 0.0
    poll_overrun_seconds = 0.0
    poll_count = 0
    remote_change_count = 0
    snapshot_sync_count = 0
    probe_failure_count = 0
    forced_fetch_count = 0
    terminal_return_visibility_polls = 0
    terminal_return_visibility_poll_limit = max(
        1,
        _int_from_env("GITPARTNER_TERMINAL_RETURN_VISIBILITY_POLLS", 3),
    )
    # The request commit was just pushed, while an explicit push refspec may
    # leave the remote-tracking ref stale. HEAD is therefore the exact baseline
    # for detecting the first server-side status update.
    result_branch = _target_result_branch()
    force_engine_result_fetch = (
        engine_request
        and result_branch != _target_branch()
        and os.environ.get(
            "GITPARTNER_ENGINE_WAIT_FORCE_FETCH",
            "",
        ).strip().lower()
        in {"1", "true", "yes", "on"}
    )
    if force_engine_result_fetch:
        wait_strategy = "engine-result-branch-fetch"
    force_fetch_fast_window_seconds = max(
        0,
        _int_from_env("GITPARTNER_ENGINE_WAIT_FETCH_FAST_WINDOW_SECONDS", 10),
    )
    force_fetch_medium_window_seconds = max(
        force_fetch_fast_window_seconds,
        _int_from_env("GITPARTNER_ENGINE_WAIT_FETCH_MEDIUM_WINDOW_SECONDS", 20),
    )
    force_fetch_medium_poll_seconds = max(
        poll_seconds,
        _int_from_env("GITPARTNER_ENGINE_WAIT_FETCH_MEDIUM_POLL_SECONDS", 3),
    )
    force_fetch_slow_poll_seconds = max(
        force_fetch_medium_poll_seconds,
        _int_from_env("GITPARTNER_ENGINE_WAIT_FETCH_SLOW_POLL_SECONDS", 5),
    )
    initial_local_head_oid = (
        _local_target_branch_oid(repo_dir, branch=result_branch)
        if result_branch != _target_branch()
        else _local_head_oid(repo_dir)
    )
    last_remote_oid = initial_local_head_oid
    last_status_signature: tuple[str, str, str] | None = None
    remote_change_observations: list[dict[str, object]] = []
    if initial_grace_seconds:
        grace_sleep = min(
            float(initial_grace_seconds),
            max(0.0, deadline - time.monotonic()),
        )
        time.sleep(grace_sleep)
        sleep_seconds += grace_sleep
    while time.monotonic() < deadline:
        poll_count += 1
        poll_started = time.monotonic()
        elapsed_before_poll = poll_started - started
        recover_missing_output = (
            result_branch != _target_branch()
            and not status_path.exists()
        )
        force_result_fetch = (
            force_engine_result_fetch or recover_missing_output
        )
        effective_poll_seconds = poll_seconds
        if force_engine_result_fetch:
            if elapsed_before_poll >= force_fetch_medium_window_seconds:
                effective_poll_seconds = force_fetch_slow_poll_seconds
            elif elapsed_before_poll >= force_fetch_fast_window_seconds:
                effective_poll_seconds = force_fetch_medium_poll_seconds
        sync_outcome = (
            _sync_wait_snapshot(
                repo_dir,
                last_remote_oid,
                force_fetch=force_result_fetch,
                output_subdir=output_subdir if force_result_fetch else "",
            )
            if result_branch != _target_branch()
            else _sync_wait_snapshot(repo_dir, last_remote_oid)
        )
        forced_fetch_count += int(force_result_fetch)
        remote_probe_seconds += sync_outcome.probe_seconds
        fetch_merge_seconds += sync_outcome.fetch_merge_seconds
        remote_change_count += int(sync_outcome.remote_changed)
        snapshot_sync_count += int(sync_outcome.snapshot_updated)
        probe_failure_count += int(not sync_outcome.probe_succeeded)
        if sync_outcome.snapshot_updated:
            last_remote_oid = sync_outcome.remote_oid
        terminal_state = ""
        observed_status_state = ""
        ref_status = (
            _read_result_status(
                repo_dir,
                result_branch,
                output_subdir,
            )
            if result_branch != _target_branch()
            else None
        )
        if ref_status is not None:
            status = ref_status
            observed_status_state = str(status.get("state") or "")
            if observed_status_state in TERMINAL_STATES:
                result_ref = (
                    f"refs/remotes/{_target_remote()}/{result_branch}"
                )
                if _materialize_result_output(
                    repo_dir,
                    result_ref,
                    output_subdir,
                ) and _terminal_return_paths_materialized(
                    repo_dir,
                    output_subdir,
                    status,
                ):
                    terminal_state = observed_status_state
        elif status_path.exists():
            status = json.loads(status_path.read_text(encoding="utf-8"))
            observed_status_state = str(status.get("state") or "")
            if (
                observed_status_state in TERMINAL_STATES
                and _terminal_return_paths_materialized(
                    repo_dir,
                    output_subdir,
                    status,
                )
            ):
                terminal_state = str(status.get("state") or "")
        else:
            print(f"waiting for {status_path}")
            status = None
        if status is not None:
            status_signature = (
                str(status.get("state") or ""),
                str(status.get("started_at") or ""),
                str(status.get("finished_at") or ""),
            )
            if status_signature != last_status_signature:
                print(json.dumps(status, ensure_ascii=False, indent=2))
                last_status_signature = status_signature
            unmaterialized_return_paths = _unmaterialized_return_paths(
                repo_dir,
                output_subdir,
                status,
            )
            if (
                observed_status_state == "success"
                and unmaterialized_return_paths
            ):
                terminal_return_visibility_polls += 1
                if (
                    terminal_return_visibility_polls
                    >= terminal_return_visibility_poll_limit
                ):
                    detail = ", ".join(unmaterialized_return_paths)
                    raise SystemExit(
                        "terminal success is missing materialized return paths "
                        f"after {terminal_return_visibility_polls} polls for "
                        f"output/{output_subdir}: {detail}; reconcile transport "
                        "result visibility before retrying"
                    )
            else:
                terminal_return_visibility_polls = 0
        if sync_outcome.remote_changed:
            remote_change_observations.append(
                {
                    "poll": poll_count,
                    "remote_oid": sync_outcome.remote_oid[:12],
                    "snapshot_updated": sync_outcome.snapshot_updated,
                    "status_present": status_path.exists(),
                    "status_state": observed_status_state,
                    "changed_paths": list(sync_outcome.changed_paths),
                }
            )

        poll_work_seconds = time.monotonic() - poll_started
        poll_overrun_seconds += max(
            0.0,
            poll_work_seconds - float(effective_poll_seconds),
        )
        if terminal_state:
            if timeline is not None:
                timeline.append(
                    {
                        "name": "wait_for_result",
                        "started_at": started_at,
                        "finished_at": _utc_now_iso(),
                        "duration_seconds": round(time.monotonic() - started, 6),
                        "poll_count": poll_count,
                        "poll_period_seconds": poll_seconds,
                        "final_poll_period_seconds": effective_poll_seconds,
                        "force_fetch_poll_schedule": {
                            "fast_window_seconds": force_fetch_fast_window_seconds,
                            "medium_window_seconds": force_fetch_medium_window_seconds,
                            "medium_poll_seconds": force_fetch_medium_poll_seconds,
                            "slow_poll_seconds": force_fetch_slow_poll_seconds,
                        }
                        if force_engine_result_fetch
                        else {},
                        "wait_strategy": wait_strategy,
                        "initial_grace_seconds": initial_grace_seconds,
                        "forced_fetch_count": forced_fetch_count,
                        "remote_probe_seconds": round(remote_probe_seconds, 6),
                        "fetch_merge_seconds": round(fetch_merge_seconds, 6),
                        "remote_change_count": remote_change_count,
                        "snapshot_sync_count": snapshot_sync_count,
                        "probe_failure_count": probe_failure_count,
                        "initial_local_head_oid": initial_local_head_oid[:12],
                        "remote_change_observations": remote_change_observations,
                        "sleep_seconds": round(sleep_seconds, 6),
                        "poll_overrun_seconds": round(poll_overrun_seconds, 6),
                        "terminal_state": terminal_state,
                    }
                )
            return
        sleep_for = min(
            max(0.0, float(effective_poll_seconds) - poll_work_seconds),
            max(0.0, deadline - time.monotonic()),
        )
        time.sleep(sleep_for)
        sleep_seconds += sleep_for
    raise SystemExit(f"timed out waiting for output/{output_subdir}/status.json")


def _terminal_return_paths_materialized(
    repo_dir: Path,
    output_subdir: str,
    status: dict[str, Any],
) -> bool:
    return not _unmaterialized_return_paths(repo_dir, output_subdir, status)


def _unmaterialized_return_paths(
    repo_dir: Path,
    output_subdir: str,
    status: dict[str, Any],
) -> list[str]:
    if str(status.get("state") or "") != "success":
        return []
    return_paths = [
        str(item).replace("\\", "/").strip("/")
        for item in status.get("return_paths", [])
        if str(item).strip()
    ]
    if not return_paths:
        return []
    missing = {
        str(item).replace("\\", "/").strip("/")
        for item in status.get("missing_return_paths", [])
        if str(item).strip()
    }
    output_root = repo_dir / "output" / Path(
        output_subdir.replace("\\", "/").strip("/")
    )
    unresolved: list[str] = []
    for return_path in return_paths:
        if return_path in missing:
            continue
        relative = Path(return_path)
        candidates = (
            output_root / "client_output" / "client_output" / relative,
            output_root / "client_output" / relative,
            output_root / relative,
        )
        if not any(
            _extended_windows_path(candidate).exists()
            for candidate in candidates
        ):
            unresolved.append(return_path)
    return unresolved


def _read_result_status(
    repo_dir: Path,
    result_branch: str,
    output_subdir: str,
) -> dict[str, Any] | None:
    normalized = output_subdir.replace("\\", "/").strip("/")
    if not normalized or ".." in Path(normalized).parts:
        raise SystemExit(f"unsafe result output path: {output_subdir}")
    ref = f"refs/remotes/{_target_remote()}/{result_branch}"
    result = _run(
        _git_command(
            repo_dir,
            ["show", f"{ref}:output/{normalized}/status.json"],
        ),
        repo_dir,
        check=False,
    )
    if result.returncode != 0:
        return None
    try:
        status = json.loads(result.stdout)
    except json.JSONDecodeError:
        return None
    return status if isinstance(status, dict) else None


def _sync_wait_snapshot(
    repo_dir: Path,
    last_remote_oid: str = "",
    *,
    force_fetch: bool = False,
    output_subdir: str = "",
) -> _WaitSnapshotOutcome:
    result_branch = _target_result_branch()
    probed_oid = ""
    probe_seconds = 0.0
    probe_succeeded = False
    if not force_fetch:
        probe_started = time.monotonic()
        probed_oid = _probe_target_branch(repo_dir, branch=result_branch)
        probe_seconds = time.monotonic() - probe_started
        probe_succeeded = bool(probed_oid)
        if probe_succeeded and probed_oid == last_remote_oid:
            return _WaitSnapshotOutcome(
                remote_oid=last_remote_oid,
                remote_changed=False,
                snapshot_updated=False,
                probe_succeeded=True,
                probe_seconds=probe_seconds,
                fetch_merge_seconds=0.0,
            )

    fetch_started = time.monotonic()
    remote_ref = _fetch_target_branch(
        repo_dir, check=False, branch=result_branch
    )
    snapshot_updated = False
    fetched_oid = ""
    changed_paths: tuple[str, ...] = ()
    if remote_ref:
        probe_succeeded = True
        fetched_oid = _local_target_branch_oid(repo_dir, branch=result_branch)
        changed_paths = _changed_paths_between(repo_dir, last_remote_oid, fetched_oid)
        if result_branch == _target_branch():
            merged = _run(
                _git_command(repo_dir, ["merge", "--ff-only", remote_ref]),
                repo_dir,
                check=False,
            )
            snapshot_updated = merged.returncode == 0
        elif output_subdir:
            snapshot_updated = _materialize_result_output(
                repo_dir,
                remote_ref,
                output_subdir,
            )
    fetch_merge_seconds = time.monotonic() - fetch_started
    remote_oid = fetched_oid or probed_oid or last_remote_oid
    return _WaitSnapshotOutcome(
        remote_oid=remote_oid,
        remote_changed=bool(remote_oid and remote_oid != last_remote_oid),
        snapshot_updated=snapshot_updated,
        probe_succeeded=probe_succeeded,
        probe_seconds=probe_seconds,
        fetch_merge_seconds=fetch_merge_seconds,
        changed_paths=changed_paths,
    )


def _probe_target_branch(repo_dir: Path, *, branch: str | None = None) -> str:
    remote = _target_remote()
    branch_ref = f"refs/heads/{branch or _target_branch()}"
    result = _run(
        _git_command(repo_dir, ["ls-remote", "--exit-code", remote, branch_ref]),
        repo_dir,
        check=False,
    )
    if result.returncode != 0:
        return ""
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) >= 2 and fields[1] == branch_ref:
            return fields[0]
    return ""


def _local_target_branch_oid(
    repo_dir: Path, *, branch: str | None = None
) -> str:
    remote_ref = f"refs/remotes/{_target_remote()}/{branch or _target_branch()}"
    result = _run(
        _git_command(repo_dir, ["rev-parse", "--verify", remote_ref]),
        repo_dir,
        check=False,
    )
    if result.returncode != 0:
        return ""
    return result.stdout.strip()


def _local_head_oid(repo_dir: Path) -> str:
    result = _run(
        _git_command(repo_dir, ["rev-parse", "--verify", "HEAD"]),
        repo_dir,
        check=False,
    )
    if result.returncode != 0:
        return ""
    return result.stdout.strip()


def _changed_paths_between(repo_dir: Path, old_oid: str, new_oid: str) -> tuple[str, ...]:
    if not old_oid or not new_oid or old_oid == new_oid:
        return ()
    result = _run(
        _git_command(repo_dir, ["diff", "--name-only", old_oid, new_oid]),
        repo_dir,
        check=False,
    )
    if result.returncode != 0:
        return ()
    return tuple(line.strip() for line in result.stdout.splitlines() if line.strip())


def _fetch_target_branch(
    repo_dir: Path,
    *,
    check: bool = True,
    branch: str | None = None,
) -> str:
    remote = _target_remote()
    selected_branch = branch or _target_branch()
    remote_ref = f"refs/remotes/{remote}/{selected_branch}"
    force = "+" if selected_branch != _target_branch() else ""
    fetch_refspec = f"{force}refs/heads/{selected_branch}:{remote_ref}"
    result = _run(_git_command(repo_dir, ["fetch", remote, fetch_refspec]), repo_dir, check=check)
    if result.returncode != 0:
        return ""
    return remote_ref


def _target_remote() -> str:
    return os.environ.get("GITPARTNER_REMOTE", "origin").strip() or "origin"


def _configure_target_refs_from_repo(repo_dir: Path) -> None:
    """Use endpoint-local branch metadata when explicit environment is absent."""
    unresolved = {
        "GITPARTNER_REMOTE": "remote",
        "GITPARTNER_BRANCH": "branch",
        "GITPARTNER_RESULT_BRANCH": "result_branch",
    }
    unresolved = {
        env_name: config_name
        for env_name, config_name in unresolved.items()
        if not os.environ.get(env_name, "").strip()
    }
    if not unresolved:
        return

    values: dict[str, set[str]] = {
        env_name: set() for env_name in unresolved
    }
    for config_dir in (
        repo_dir / "configs" / "endpoints",
        repo_dir / "configs" / "nodes",
    ):
        if not config_dir.is_dir():
            continue
        for path in sorted(config_dir.glob("*.json")):
            try:
                raw = json.loads(path.read_text(encoding="utf-8-sig"))
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(raw, dict):
                continue
            configured_repo = str(raw.get("repo_dir") or "").strip()
            if (
                not configured_repo
                or Path(configured_repo).name.casefold() != repo_dir.name.casefold()
            ):
                continue
            repo_raw = raw.get("repo")
            if not isinstance(repo_raw, dict):
                continue
            for env_name, config_name in unresolved.items():
                value = str(repo_raw.get(config_name) or "").strip()
                if value:
                    values[env_name].add(value)

    for env_name, candidates in values.items():
        if len(candidates) > 1:
            rendered = ", ".join(sorted(candidates))
            raise SystemExit(
                f"ambiguous endpoint-local {env_name}: {rendered}; "
                f"set {env_name} explicitly"
            )
        if candidates:
            os.environ[env_name] = next(iter(candidates))


def _target_branch() -> str:
    return os.environ.get("GITPARTNER_BRANCH", "main").strip() or "main"


def _target_result_branch() -> str:
    return (
        os.environ.get("GITPARTNER_RESULT_BRANCH", "").strip()
        or _target_branch()
    )


def _materialize_result_output(
    repo_dir: Path,
    remote_ref: str,
    output_subdir: str,
    *,
    destination_repo: Path | None = None,
) -> bool:
    normalized = output_subdir.replace("\\", "/").strip("/")
    if not normalized or ".." in Path(normalized).parts:
        raise SystemExit(f"unsafe result output path: {output_subdir}")
    repo_path = f"output/{normalized}"
    with _result_temporary_directory(repo_dir) as temp:
        temp_root = Path(temp)
        archive = temp_root / "result.tar"
        archived = _run(
            _git_command(
                repo_dir,
                [
                    "archive",
                    "--format=tar",
                    f"--output={archive}",
                    remote_ref,
                    repo_path,
                ],
            ),
            repo_dir,
            check=False,
        )
        if archived.returncode != 0 or not archive.is_file():
            return False
        staging = temp_root / "tree"
        staging.mkdir()
        with tarfile.open(archive, "r") as handle:
            for member in handle.getmembers():
                member_path = Path(member.name)
                if member_path.is_absolute() or ".." in member_path.parts:
                    raise SystemExit(
                        f"unsafe result archive member: {member.name}"
                    )
            handle.extractall(staging, filter="data")
        source = staging / "output" / Path(normalized)
        if not source.is_dir():
            return False
        target_root = (destination_repo or repo_dir).resolve()
        target = target_root / "output" / Path(normalized)
        filesystem_source = _extended_windows_path(source)
        filesystem_target = _extended_windows_path(target)
        if filesystem_target.exists():
            shutil.rmtree(filesystem_target)
        filesystem_target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(filesystem_source, filesystem_target)
        return True


def _extended_windows_path(path: Path) -> Path:
    if os.name != "nt":
        return path
    resolved = str(path.resolve())
    if resolved.startswith("\\\\?\\"):
        return Path(resolved)
    if resolved.startswith("\\\\"):
        return Path("\\\\?\\UNC\\" + resolved.lstrip("\\"))
    return Path("\\\\?\\" + resolved)


def _result_temporary_directory(
    repo_dir: Path,
) -> tempfile.TemporaryDirectory[str]:
    if os.name == "nt" and repo_dir.anchor:
        try:
            return tempfile.TemporaryDirectory(
                prefix=".gpr-",
                dir=repo_dir.anchor,
            )
        except OSError:
            pass
    return tempfile.TemporaryDirectory(prefix="gitpartner-result-")


def _git_command(repo_dir: Path, args: list[str]) -> list[str]:
    command = ["git", "-c", f"safe.directory={repo_dir.as_posix()}"]
    auth_header = _auth_header(repo_dir) if _is_network_git_args(args) else None
    if auth_header:
        command.extend(["-c", f"http.extraHeader={auth_header}"])
    command.extend(args)
    return command


def _is_network_git_args(args: list[str]) -> bool:
    return bool(args and args[0] in {"fetch", "pull", "push", "clone", "ls-remote"})


def _auth_header(repo_dir: Path) -> str | None:
    token = _load_git_token(repo_dir)
    username = os.environ.get("GITPARTNER_AUTH_USERNAME", "git-user").strip()
    if not token or not username:
        return None
    encoded = base64.b64encode(f"{username}:{token}".encode("utf-8")).decode("ascii")
    return f"Authorization: Basic {encoded}"


def _load_git_token(repo_dir: Path) -> str | None:
    path = repo_dir.resolve() / "api.txt"
    if not path.is_file():
        return None
    token = path.read_text(encoding="utf-8").strip().strip('"').strip("'")
    return token or None


def _run(command: list[str], cwd: Path, *, check: bool = True) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.setdefault("GIT_TERMINAL_PROMPT", "0")
    env.setdefault("GCM_INTERACTIVE", "Never")
    result: subprocess.CompletedProcess[str] | None = None
    attempts = max(1, _int_from_env("GITPARTNER_GIT_LOCK_RETRIES", 4))
    for attempt in range(1, attempts + 1):
        try:
            with GitOperationLock(cwd, " ".join(command[3:] if command[:1] == ["git"] else command)):
                try:
                    result = subprocess.run(
                        command,
                        cwd=cwd,
                        text=True,
                        encoding="utf-8",
                        errors="replace",
                        check=False,
                        stdin=subprocess.DEVNULL,
                        env=env,
                        capture_output=True,
                        timeout=_git_command_timeout_seconds(),
                        **hidden_subprocess_kwargs(),
                    )
                except subprocess.TimeoutExpired as exc:
                    result = subprocess.CompletedProcess(
                        command,
                        124,
                        _timeout_text(exc.stdout),
                        _timeout_text(exc.stderr)
                        or (
                            "git command timed out after "
                            f"{_git_command_timeout_seconds()} seconds"
                        ),
                    )
                    recover_git_locks_after_failure(cwd)
        except GitLockError as exc:
            raise SystemExit(str(exc)) from exc
        if result.returncode == 0:
            _emit_completed_output(result)
            return result
        if _is_git_lock_failure(result) and attempt < attempts:
            recover_git_locks_after_failure(cwd)
            time.sleep(_git_lock_retry_delay_seconds(attempt))
            continue
        break
    assert result is not None
    _emit_completed_output(result)
    if check and result.returncode != 0:
        raise SystemExit(result.returncode)
    return result


def _is_git_lock_failure(result: subprocess.CompletedProcess[str]) -> bool:
    return is_git_lock_failure_text(f"{result.stdout or ''}\n{result.stderr or ''}")


def _emit_completed_output(result: subprocess.CompletedProcess[str]) -> None:
    if result.stdout:
        sys.stdout.write(result.stdout)
    if result.stderr:
        sys.stderr.write(result.stderr)


def _git_lock_retry_delay_seconds(attempt: int) -> float:
    base = max(0, _int_from_env("GITPARTNER_GIT_LOCK_RETRY_BASE_SECONDS", 2))
    return float(min(15, base * attempt))


def _git_command_timeout_seconds() -> int:
    return max(1, _int_from_env("GITPARTNER_GIT_TIMEOUT_SECONDS", 120))


def _int_from_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        return default


def _timeout_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _shell_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"


if __name__ == "__main__":
    main()
