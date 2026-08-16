from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from limited_remote_partner.core.process_utils import hidden_subprocess_kwargs
from limited_remote_partner.gateway.submit_job import (
    TERMINAL_STATES,
    _fetch_target_branch,
    _local_target_branch_oid,
    _materialize_result_output,
    _probe_target_branch,
)


def main(argv: list[str] | None = None) -> None:
    started = time.monotonic()
    parser = argparse.ArgumentParser(
        description=(
            "Fetch one GitPartner result branch and inspect multiple compact "
            "receipts without materializing result trees"
        )
    )
    parser.add_argument("--repo", default=".")
    parser.add_argument(
        "--output-subdir",
        action="append",
        default=[],
    )
    parser.add_argument("--result-branch", required=True)
    parser.add_argument("--control-branch", default="")
    parser.add_argument("--remote", default="origin")
    parser.add_argument(
        "--wait-seconds",
        type=float,
        default=0.0,
        help=(
            "keep this process alive for up to this many seconds while the "
            "requested outputs are still absent"
        ),
    )
    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=0.05,
        help="delay between remote probes while --wait-seconds is active",
    )
    parser.add_argument(
        "--serve-jsonl",
        action="store_true",
        help="serve repeated query requests over stdin/stdout JSON lines",
    )
    parser.add_argument(
        "--result-template",
        action="append",
        default=[],
        help=(
            "result path relative to each output root; {request_id} expands "
            "to the final output_subdir component"
        ),
    )
    parser.add_argument(
        "--materialize-root",
        default="",
        help=(
            "trusted destination GitPartner worktree for terminal output trees; "
            "the result worktree remains no-checkout"
        ),
    )
    args = parser.parse_args(argv)

    repo = Path(args.repo).resolve()
    if not (repo / ".git").exists():
        raise SystemExit(f"repo is not a Git worktree: {repo}")
    os.environ["GITPARTNER_REMOTE"] = str(args.remote)
    os.environ["GITPARTNER_RESULT_BRANCH"] = str(args.result_branch)
    if args.control_branch:
        os.environ["GITPARTNER_BRANCH"] = str(args.control_branch)

    if args.serve_jsonl:
        serve_jsonl(
            repo,
            result_branch=str(args.result_branch),
        )
        return
    if not args.output_subdir:
        parser.error("--output-subdir is required unless --serve-jsonl is used")
    observed = query_result_branch(
        repo,
        result_branch=str(args.result_branch),
        output_subdirs=tuple(str(item) for item in args.output_subdir),
        result_templates=tuple(str(item) for item in args.result_template),
        wait_seconds=float(args.wait_seconds),
        poll_seconds=float(args.poll_seconds),
        started=started,
        materialize_root=(
            Path(args.materialize_root).resolve()
            if str(args.materialize_root).strip()
            else None
        ),
    )
    print(json.dumps(observed, ensure_ascii=True, sort_keys=True))


def query_result_branch(
    repo: Path,
    *,
    result_branch: str,
    output_subdirs: tuple[str, ...],
    result_templates: tuple[str, ...],
    wait_seconds: float = 0.0,
    poll_seconds: float = 0.05,
    started: float | None = None,
    materialize_root: Path | None = None,
) -> dict[str, Any]:
    started = time.monotonic() if started is None else started
    local_started = time.monotonic()
    initial_local_oid = _local_target_branch_oid(
        repo,
        branch=result_branch,
    )
    local_oid_seconds = time.monotonic() - local_started
    selected_ref = initial_local_oid
    remote_oid = initial_local_oid
    last_inspected_ref = ""
    items: list[dict[str, Any]] = []
    probe_seconds = 0.0
    fetch_seconds = 0.0
    selective_read_seconds = 0.0
    probe_count = 0
    fetch_count = 0
    fetch_performed = False
    deadline = time.monotonic() + max(0.0, wait_seconds)
    while True:
        probe_started = time.monotonic()
        probed_oid = _probe_target_branch(
            repo,
            branch=result_branch,
        )
        probe_seconds += time.monotonic() - probe_started
        probe_count += 1
        remote_oid = probed_oid or selected_ref
        if not probed_oid or probed_oid != selected_ref:
            fetch_started = time.monotonic()
            fetched_ref = _fetch_target_branch(
                repo,
                check=False,
                branch=result_branch,
            )
            fetch_seconds += time.monotonic() - fetch_started
            fetch_count += 1
            fetch_performed = True
            local_after = _local_target_branch_oid(
                repo,
                branch=result_branch,
            )
            selected_ref = fetched_ref or local_after or probed_oid
            remote_oid = local_after or probed_oid
        else:
            selected_ref = probed_oid

        if selected_ref != last_inspected_ref:
            read_started = time.monotonic()
            items = inspect_outputs(
                repo,
                selected_ref,
                output_subdirs,
                result_templates,
            )
            selective_read_seconds += time.monotonic() - read_started
            last_inspected_ref = selected_ref
        if selected_ref and materialize_root is not None:
            items = materialize_terminal_outputs(
                repo,
                selected_ref,
                items,
                materialize_root=materialize_root,
            )
        if outputs_observed(items) or time.monotonic() >= deadline:
            break
        time.sleep(max(0.01, poll_seconds))

    commit_created_at = ""
    metadata_seconds = 0.0
    if outputs_observed(items) and selected_ref:
        metadata_started = time.monotonic()
        commit_created_at = read_commit_created_at(repo, selected_ref)
        metadata_seconds = time.monotonic() - metadata_started
    total_seconds = time.monotonic() - started
    return {
        "schema": "gitpartner.batch-result-query.v1",
        "result_branch": result_branch,
        "remote_oid": remote_oid,
        "remote_changed": bool(
            remote_oid and remote_oid != initial_local_oid
        ),
        "fetch_performed": fetch_performed,
        "commit_created_at": commit_created_at,
        "probe_succeeded": bool(probed_oid),
        "selective_read": True,
        "timing": {
            "fetch_count": fetch_count,
            "fetch_seconds": round(fetch_seconds, 6),
            "local_oid_seconds": round(local_oid_seconds, 6),
            "metadata_seconds": round(metadata_seconds, 6),
            "probe_count": probe_count,
            "probe_seconds": round(probe_seconds, 6),
            "selective_read_seconds": round(
                selective_read_seconds,
                6,
            ),
            "total_seconds": round(total_seconds, 6),
        },
        "items": items,
    }


def serve_jsonl(repo: Path, *, result_branch: str) -> None:
    for line in sys.stdin:
        try:
            request = json.loads(line)
            if not isinstance(request, dict):
                raise ValueError("query request must be an object")
            output_subdirs = tuple(
                str(item) for item in request.get("output_subdirs", [])
            )
            if not output_subdirs:
                raise ValueError("query request has no output_subdirs")
            observed = query_result_branch(
                repo,
                result_branch=result_branch,
                output_subdirs=output_subdirs,
                result_templates=tuple(
                    str(item)
                    for item in request.get("result_templates", [])
                ),
                wait_seconds=float(request.get("wait_seconds", 0.0)),
                poll_seconds=float(request.get("poll_seconds", 0.05)),
                materialize_root=(
                    Path(str(request["materialize_root"])).resolve()
                    if str(request.get("materialize_root") or "").strip()
                    else None
                ),
            )
            response = {"ok": True, "observed": observed}
        except Exception as exc:
            response = {
                "ok": False,
                "error": f"{type(exc).__name__}: {exc}",
            }
        print(
            json.dumps(response, ensure_ascii=True, sort_keys=True),
            flush=True,
        )


def outputs_observed(items: list[dict[str, Any]]) -> bool:
    return any(
        str(item.get("status_source") or "") != "missing"
        or isinstance(item.get("result"), dict)
        for item in items
    )


def materialize_terminal_outputs(
    repo: Path,
    ref: str,
    items: list[dict[str, Any]],
    *,
    materialize_root: Path,
) -> list[dict[str, Any]]:
    resolved_root = materialize_root.resolve()
    if not (resolved_root / ".git").exists():
        raise ValueError(
            f"materialize root is not a Git worktree: {resolved_root}"
        )
    observed: list[dict[str, Any]] = []
    for item in items:
        row = dict(item)
        status = row.get("status")
        terminal = (
            isinstance(status, dict)
            and str(status.get("state") or "") in TERMINAL_STATES
        )
        row["materialized"] = bool(
            terminal
            and _materialize_result_output(
                repo,
                ref,
                str(row.get("output_subdir") or ""),
                destination_repo=resolved_root,
            )
        )
        observed.append(row)
    return observed


def read_commit_created_at(repo: Path, ref: str) -> str:
    completed = subprocess.run(
        [
            "git",
            "-c",
            f"safe.directory={repo.as_posix()}",
            "show",
            "-s",
            "--format=%cI",
            ref,
        ],
        cwd=repo,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        **hidden_subprocess_kwargs(),
    )
    return completed.stdout.strip() if completed.returncode == 0 else ""


def inspect_output(
    repo: Path,
    ref: str,
    output_subdir: str,
    result_templates: tuple[str, ...],
) -> dict[str, Any]:
    normalized = safe_relative_path(output_subdir, "output_subdir")
    root = f"output/{normalized}"
    receipt = read_ref_object(repo, ref, f"{root}/receipt.json")
    full_status = read_ref_object(repo, ref, f"{root}/status.json")
    status = receipt or full_status
    request_id = Path(normalized).name
    result = None
    result_path = ""
    for template in result_templates:
        relative = safe_relative_path(
            template.replace("{request_id}", request_id),
            "result_template",
        )
        result = read_ref_object(repo, ref, f"{root}/{relative}")
        if result is not None:
            result_path = relative
            break
    return {
        "output_subdir": normalized,
        "request_id": request_id,
        "status": status,
        "full_status": full_status,
        "status_source": (
            "compact-receipt"
            if receipt is not None
            else "full-status"
            if status is not None
            else "missing"
        ),
        "result": result,
        "result_path": result_path,
    }


def inspect_outputs(
    repo: Path,
    ref: str,
    output_subdirs: tuple[str, ...],
    result_templates: tuple[str, ...],
) -> list[dict[str, Any]]:
    requests: list[tuple[str, str, str, tuple[str, ...]]] = []
    object_paths: list[str] = []
    for output_subdir in output_subdirs:
        normalized = safe_relative_path(output_subdir, "output_subdir")
        root = f"output/{normalized}"
        request_id = Path(normalized).name
        result_paths = tuple(
            safe_relative_path(
                template.replace("{request_id}", request_id),
                "result_template",
            )
            for template in result_templates
        )
        requests.append((normalized, root, request_id, result_paths))
        object_paths.extend(
            [
                f"{root}/receipt.json",
                f"{root}/status.json",
                *(f"{root}/{relative}" for relative in result_paths),
            ]
        )
    objects = read_ref_objects(repo, ref, tuple(dict.fromkeys(object_paths)))
    items: list[dict[str, Any]] = []
    for normalized, root, request_id, result_paths in requests:
        receipt = objects.get(f"{root}/receipt.json")
        full_status = objects.get(f"{root}/status.json")
        status = receipt or full_status
        result = None
        result_path = ""
        for relative in result_paths:
            result = objects.get(f"{root}/{relative}")
            if result is not None:
                result_path = relative
                break
        items.append(
            {
                "output_subdir": normalized,
                "request_id": request_id,
                "status": status,
                "full_status": full_status,
                "status_source": (
                    "compact-receipt"
                    if receipt is not None
                    else "full-status"
                    if status is not None
                    else "missing"
                ),
                "result": result,
                "result_path": result_path,
            }
        )
    return items


def read_ref_objects(
    repo: Path,
    ref: str,
    repo_paths: tuple[str, ...],
) -> dict[str, dict[str, Any] | None]:
    if not ref or not repo_paths:
        return {path: None for path in repo_paths}
    specs = [f"{ref}:{path}" for path in repo_paths]
    completed = subprocess.run(
        [
            "git",
            "-c",
            f"safe.directory={repo.as_posix()}",
            "cat-file",
            "--batch",
        ],
        cwd=repo,
        input=("".join(f"{spec}\n" for spec in specs)).encode("utf-8"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        **hidden_subprocess_kwargs(),
    )
    if completed.returncode != 0:
        return {path: None for path in repo_paths}
    output = completed.stdout
    cursor = 0
    objects: dict[str, dict[str, Any] | None] = {}
    for path, _spec in zip(repo_paths, specs, strict=True):
        line_end = output.find(b"\n", cursor)
        if line_end < 0:
            objects[path] = None
            continue
        header = output[cursor:line_end]
        cursor = line_end + 1
        if header.endswith(b" missing"):
            objects[path] = None
            continue
        fields = header.rsplit(b" ", 2)
        if len(fields) != 3 or fields[1] != b"blob":
            objects[path] = None
            continue
        try:
            size = int(fields[2])
        except ValueError:
            objects[path] = None
            continue
        content = output[cursor : cursor + size]
        cursor += size
        if output[cursor : cursor + 1] == b"\n":
            cursor += 1
        try:
            value = json.loads(content.decode("utf-8-sig"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            objects[path] = None
            continue
        objects[path] = value if isinstance(value, dict) else None
    return objects


def read_ref_object(
    repo: Path,
    ref: str,
    repo_path: str,
) -> dict[str, Any] | None:
    if not ref:
        return None
    completed = subprocess.run(
        [
            "git",
            "-c",
            f"safe.directory={repo.as_posix()}",
            "show",
            f"{ref}:{repo_path}",
        ],
        cwd=repo,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        **hidden_subprocess_kwargs(),
    )
    if completed.returncode != 0:
        return None
    try:
        value = json.loads(completed.stdout.lstrip("\ufeff"))
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else None


def safe_relative_path(value: str, label: str) -> str:
    normalized = value.replace("\\", "/").strip("/")
    parts = Path(normalized).parts
    if (
        not normalized
        or Path(normalized).is_absolute()
        or ".." in parts
        or any(part in {"", "."} for part in parts)
    ):
        raise ValueError(f"unsafe {label}: {value}")
    return "/".join(parts)


if __name__ == "__main__":
    main()
