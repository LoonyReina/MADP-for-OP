from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RESIDENT_RUNTIME_FILES = (
    "gateway/client.py",
    "gateway/git_client.py",
    "gateway/input_parser.py",
    "endpoint/node_lifecycle.py",
)


class DirectResidentCodeSyncError(RuntimeError):
    pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Atomically install one bounded GitPartner resident runtime bundle "
            "and schedule a generation-fenced client restart."
        )
    )
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--target-repo", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--expected-generation", required=True)
    parser.add_argument("--request-id", required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--restart-delay-seconds", type=int, default=90)
    args = parser.parse_args(argv)
    try:
        receipt = apply_resident_runtime(
            source=args.source,
            target_repo=args.target_repo,
            config_path=args.config,
            expected_generation=args.expected_generation,
            request_id=args.request_id,
            receipt_path=args.receipt,
            restart_delay_seconds=args.restart_delay_seconds,
        )
    except DirectResidentCodeSyncError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(
        "GITPARTNER_RESIDENT_RUNTIME_RECEIPT:"
        + json.dumps(
            receipt,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


def resident_runtime_generation(source: Path, *, allow_missing: bool = False) -> str:
    source = source.resolve()
    digest = hashlib.sha256()
    for name in RESIDENT_RUNTIME_FILES:
        path = source / name
        if not path.is_file():
            if allow_missing:
                return ""
            raise DirectResidentCodeSyncError(
                f"resident runtime source file is missing: {path}"
            )
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(_normalized_python_source(path.read_bytes()))
        digest.update(b"\0")
    return digest.hexdigest()[:16]


def apply_resident_runtime(
    *,
    source: Path,
    target_repo: Path,
    config_path: Path,
    expected_generation: str,
    request_id: str,
    receipt_path: Path,
    restart_delay_seconds: int = 90,
) -> dict[str, Any]:
    work_root = Path.cwd().resolve()
    source = _bounded_path(work_root, source, "source")
    target_repo = _bounded_path(work_root, target_repo, "target_repo")
    config_path = _bounded_path(work_root, config_path, "config")
    receipt_path = _bounded_path(work_root, receipt_path, "receipt")
    if not _is_relative_to(config_path, target_repo):
        raise DirectResidentCodeSyncError(
            "resident config must stay inside the target GitPartner package"
        )
    if not config_path.is_file():
        raise DirectResidentCodeSyncError(
            f"resident config is missing: {config_path}"
        )
    expected_generation = expected_generation.strip().lower()
    if (
        len(expected_generation) != 16
        or any(char not in "0123456789abcdef" for char in expected_generation)
    ):
        raise DirectResidentCodeSyncError(
            "expected generation must be a 16-character lowercase hex digest"
        )
    actual_generation = resident_runtime_generation(source)
    if actual_generation != expected_generation:
        raise DirectResidentCodeSyncError(
            "resident runtime source generation mismatch: "
            f"expected={expected_generation} actual={actual_generation}"
        )

    target_package = target_repo / "src" / "limited_remote_partner"
    if not target_package.is_dir():
        raise DirectResidentCodeSyncError(
            f"target GitPartner package is missing: {target_package}"
        )
    missing_targets = [
        str(target_package / name)
        for name in RESIDENT_RUNTIME_FILES
        if not (target_package / name).is_file()
    ]
    if missing_targets:
        raise DirectResidentCodeSyncError(
            "target resident runtime package is incomplete: "
            + ", ".join(missing_targets)
        )
    source_hashes = _file_hashes(source)
    for name in RESIDENT_RUNTIME_FILES:
        try:
            compile(
                (source / name).read_text(encoding="utf-8"),
                str(source / name),
                "exec",
            )
        except (OSError, SyntaxError, UnicodeError) as exc:
            raise DirectResidentCodeSyncError(
                f"resident runtime source is not valid Python: {name}: {exc}"
            ) from exc

    generation_package = (
        target_repo
        / ".partner_state"
        / "resident_runtime"
        / "generations"
        / expected_generation
        / "limited_remote_partner"
    )
    if generation_package.is_dir():
        installed_generation = resident_runtime_generation(generation_package)
        if installed_generation != expected_generation:
            raise DirectResidentCodeSyncError(
                "immutable resident runtime generation is corrupted: "
                f"expected={expected_generation} actual={installed_generation}"
            )
    else:
        generation_package.parent.mkdir(parents=True, exist_ok=True)
        staging = Path(
            tempfile.mkdtemp(
                prefix=f".{expected_generation}.install-",
                dir=str(generation_package.parent),
            )
        )
        try:
            for name in RESIDENT_RUNTIME_FILES:
                (staging / name).parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source / name, staging / name)
            if resident_runtime_generation(staging) != expected_generation:
                raise DirectResidentCodeSyncError(
                    "staged resident runtime generation did not verify"
                )
            os.replace(staging, generation_package)
        finally:
            if staging.exists():
                shutil.rmtree(staging, ignore_errors=True)

    previous = {
        name: (target_package / name).read_bytes()
        for name in RESIDENT_RUNTIME_FILES
    }
    previous_modes = {
        name: (target_package / name).stat().st_mode
        for name in RESIDENT_RUNTIME_FILES
    }
    before_hashes = {
        name: hashlib.sha256(payload).hexdigest()
        for name, payload in previous.items()
    }
    try:
        for name in RESIDENT_RUNTIME_FILES:
            _atomic_copy(generation_package / name, target_package / name)
        installed_hashes = _file_hashes(target_package)
        if installed_hashes != source_hashes:
            raise DirectResidentCodeSyncError(
                "installed resident runtime hashes do not match the source bundle"
            )

        pending_receipt = {
            "schema": "git-partner.resident-runtime-sync.v1",
            "request_id": request_id,
            "state": "restart-pending",
            "expected_generation": expected_generation,
            "target_repo": str(target_repo),
            "config_path": str(config_path),
            "source_hashes": source_hashes,
            "before_hashes": before_hashes,
            "installed_hashes": installed_hashes,
            "updated_at": _utc_now(),
        }
        _write_json_atomic(receipt_path, pending_receipt)

        if str(target_repo / "src") not in sys.path:
            sys.path.insert(0, str(target_repo / "src"))
        from limited_remote_partner.core.config import load_config
        from limited_remote_partner.maintenance.lan_ops import schedule_local_restart_service
        from limited_remote_partner.gateway.relay import ScpTransport

        config = load_config(config_path)
        if config.relay.role != "client":
            raise DirectResidentCodeSyncError(
                "resident config must describe a GitPartner client"
            )
        control_repo = config.repo_dir.resolve()
        if (
            not _is_relative_to(control_repo, work_root)
            or not control_repo.is_dir()
        ):
            raise DirectResidentCodeSyncError(
                "resident config repo_dir must identify an existing control "
                f"worktree inside the client work root: {control_repo}"
            )
        pending_receipt["control_repo"] = str(control_repo)
        _write_json_atomic(receipt_path, pending_receipt)
        restart_args = argparse.Namespace(
            service_name="",
            remote_config=str(config_path),
            no_process_fallback=False,
            cleanup_request_id="",
        )
        restart_report = schedule_local_restart_service(
            ScpTransport(config),
            str(target_repo),
            "client",
            restart_args,
            request_id,
            delay_seconds=max(30, int(restart_delay_seconds)),
        )
        receipt = {
            **pending_receipt,
            "state": "success",
            "restart_delay_seconds": max(30, int(restart_delay_seconds)),
            "restart_report": restart_report,
            "updated_at": _utc_now(),
        }
        _write_json_atomic(receipt_path, receipt)
        return receipt
    except Exception as exc:
        for name in RESIDENT_RUNTIME_FILES:
            _atomic_write(
                target_package / name,
                previous[name],
                previous_modes[name],
            )
        try:
            _write_json_atomic(
                receipt_path,
                {
                    "schema": "git-partner.resident-runtime-sync.v1",
                    "request_id": request_id,
                    "state": "failed-rolled-back",
                    "expected_generation": expected_generation,
                    "target_repo": str(target_repo),
                    "config_path": str(config_path),
                    "control_repo": str(
                        locals().get("control_repo", "")
                    ),
                    "source_hashes": source_hashes,
                    "before_hashes": before_hashes,
                    "installed_hashes": _file_hashes(target_package),
                    "error": str(exc),
                    "updated_at": _utc_now(),
                },
            )
        except OSError:
            pass
        if isinstance(exc, DirectResidentCodeSyncError):
            raise
        raise DirectResidentCodeSyncError(
            f"resident runtime sync failed and was rolled back: {exc}"
        ) from exc


def _file_hashes(source: Path) -> dict[str, str]:
    return {
        name: hashlib.sha256((source / name).read_bytes()).hexdigest()
        for name in RESIDENT_RUNTIME_FILES
    }


def _normalized_python_source(payload: bytes) -> bytes:
    return payload.replace(b"\r\n", b"\n").replace(b"\r", b"\n")


def _atomic_copy(source: Path, target: Path) -> None:
    _atomic_write(target, source.read_bytes(), source.stat().st_mode)


def _atomic_write(target: Path, payload: bytes, mode: int) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".tmp",
        dir=str(target.parent),
    )
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary_name, mode)
        os.replace(temporary_name, target)
    finally:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    _atomic_write(
        path,
        (
            json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True)
            + "\n"
        ).encode("utf-8"),
        0o600,
    )


def _bounded_path(root: Path, value: Path, field: str) -> Path:
    path = value if value.is_absolute() else root / value
    resolved = path.resolve()
    if not _is_relative_to(resolved, root):
        raise DirectResidentCodeSyncError(
            f"{field} must stay inside the client work root: {resolved}"
        )
    return resolved


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


if __name__ == "__main__":
    raise SystemExit(main())
