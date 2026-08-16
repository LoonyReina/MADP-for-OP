from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DAEMON_SOURCE = Path(__file__).resolve().parents[2]
if str(DAEMON_SOURCE) not in sys.path:
    sys.path.insert(0, str(DAEMON_SOURCE))

try:
    from ascendop_daemon.runtime.process_adapter import (
        process_creation_flags,
        process_startupinfo,
    )
except ModuleNotFoundError:
    def process_creation_flags() -> int:
        if sys.platform == "win32":
            return int(getattr(subprocess, "CREATE_NO_WINDOW", 0))
        return 0

    def process_startupinfo() -> subprocess.STARTUPINFO | None:
        if sys.platform != "win32":
            return None
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = subprocess.SW_HIDE
        return startupinfo


class TransportInstallError(RuntimeError):
    pass


def transport_generation(source_root: Path) -> str:
    source_root = source_root.resolve()
    package_root = source_root / "limited_remote_partner"
    paths = sorted(package_root.rglob("*.py"), key=lambda item: item.as_posix())
    if not paths:
        raise TransportInstallError(
            f"GitPartner package source is missing: {package_root}"
        )
    digest = hashlib.sha256()
    for path in paths:
        relative = path.relative_to(source_root).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        data = path.read_bytes().replace(b"\r\n", b"\n")
        digest.update(len(data).to_bytes(8, "big"))
        digest.update(data)
    return digest.hexdigest()


def install_transport_runtime(
    *,
    archive: Path,
    runtime_root: Path,
    worktree: Path,
    config_relative: Path,
    expected_generation: str,
    expected_archive_sha256: str,
    receipt_path: Path,
    protocol_source: Path | None = None,
    engine_code_generation: str = "",
    release_generation: str = "",
) -> dict[str, Any]:
    archive = archive.resolve()
    runtime_root = runtime_root.resolve()
    worktree = worktree.resolve()
    receipt_path = receipt_path.resolve()
    protocol_source = protocol_source.resolve() if protocol_source else None
    if protocol_source is not None and not (
        protocol_source / "ascendop_protocol"
    ).is_dir():
        raise TransportInstallError(
            f"shared protocol source is missing: {protocol_source}"
        )
    _validate_digest(expected_generation, 64, "transport generation")
    _validate_digest(expected_archive_sha256, 64, "archive SHA-256")
    if release_generation:
        _validate_digest(release_generation, 64, "release generation")
    if engine_code_generation:
        _validate_digest(engine_code_generation, 16, "Engine code generation")
    installation_generation = release_generation or expected_generation
    if not archive.is_file():
        raise TransportInstallError(f"transport archive is missing: {archive}")
    archive_sha256 = _file_digest(archive)
    if archive_sha256 != expected_archive_sha256:
        raise TransportInstallError(
            "transport archive digest mismatch: "
            f"expected={expected_archive_sha256} actual={archive_sha256}"
        )

    generations_root = runtime_root / "generations"
    generations_root.mkdir(parents=True, exist_ok=True)
    generation_root = generations_root / installation_generation
    if generation_root.is_dir():
        actual_generation = transport_generation(generation_root / "src")
        if actual_generation != expected_generation:
            raise TransportInstallError(
                "immutable transport generation is corrupted: "
                f"expected={expected_generation} actual={actual_generation}"
            )
    else:
        staging = Path(
            tempfile.mkdtemp(
                prefix=".gp-",
                dir=generations_root,
            )
        )
        try:
            _safe_extract(archive, staging)
            actual_generation = transport_generation(staging / "src")
            if actual_generation != expected_generation:
                raise TransportInstallError(
                    "staged transport generation mismatch: "
                    f"expected={expected_generation} actual={actual_generation}"
                )
            if not (staging / config_relative).is_file():
                raise TransportInstallError(
                    f"staged transport config is missing: {config_relative}"
                )
            os.replace(staging, generation_root)
        finally:
            if staging.exists():
                shutil.rmtree(staging)

    pointer_path = runtime_root / "current"
    previous_generation = _read_pointer(pointer_path)
    previous_root = (
        generations_root / previous_generation if previous_generation else None
    )
    new_config = generation_root / config_relative
    if not new_config.is_file():
        raise TransportInstallError(
            f"installed transport config is missing: {new_config}"
        )

    switched = False
    try:
        stop_result: dict[str, Any] = {}
        if previous_root is not None and previous_root.is_dir():
            previous_config = previous_root / config_relative
            if previous_config.is_file():
                stop_result = _service_command(
                    source_root=previous_root / "src",
                    protocol_source=protocol_source,
                    worktree=worktree,
                    config_path=previous_config,
                    action="stop",
                )
        _write_pointer(pointer_path, installation_generation)
        switched = True
        start_result = _service_command(
            source_root=generation_root / "src",
            protocol_source=protocol_source,
            worktree=worktree,
            config_path=new_config,
            action="start",
            force_restart=True,
            engine_code_generation=engine_code_generation,
        )
        status_result = _service_command(
            source_root=generation_root / "src",
            protocol_source=protocol_source,
            worktree=worktree,
            config_path=new_config,
            action="status",
            engine_code_generation=engine_code_generation,
        )
        if not (
            bool(status_result.get("running"))
            and bool(status_result.get("child_running"))
        ):
            raise TransportInstallError(
                "new GitPartner resident did not report supervisor and child alive"
            )
    except Exception as exc:
        if switched:
            if previous_generation:
                _write_pointer(pointer_path, previous_generation)
            else:
                pointer_path.unlink(missing_ok=True)
        if previous_root is not None and previous_root.is_dir():
            previous_config = previous_root / config_relative
            if previous_config.is_file():
                try:
                    _service_command(
                        source_root=previous_root / "src",
                        protocol_source=protocol_source,
                        worktree=worktree,
                        config_path=previous_config,
                        action="start",
                        force_restart=True,
                    )
                except TransportInstallError:
                    pass
        if isinstance(exc, TransportInstallError):
            raise
        raise TransportInstallError(str(exc)) from exc

    receipt = {
        "schema": "ascendop.gitpartner.deployment-receipt.v3",
        "deployment_mode": "authorized-direct-ssh-atomic-maintenance",
        "transport_generation": expected_generation,
        "release_generation": installation_generation,
        "previous_transport_generation": previous_generation,
        "engine_code_generation": engine_code_generation,
        "archive_sha256": archive_sha256,
        "runtime_root": str(runtime_root),
        "runtime_config": str(new_config),
        "protocol_source": str(protocol_source) if protocol_source else "",
        "stop_result": stop_result,
        "start_result": start_result,
        "service_status": status_result,
        "installed_at": datetime.now(timezone.utc).isoformat(),
    }
    _write_json(receipt_path, receipt)
    _write_json(generation_root / "DEPLOYMENT_RECEIPT.json", receipt)
    return receipt


def _service_command(
    *,
    source_root: Path,
    protocol_source: Path | None = None,
    worktree: Path,
    config_path: Path,
    action: str,
    force_restart: bool = False,
    engine_code_generation: str = "",
) -> dict[str, Any]:
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        item
        for item in (
            str(source_root),
            str(protocol_source) if protocol_source else "",
            env.get("PYTHONPATH", ""),
        )
        if item
    )
    env["GITPARTNER_RUNTIME_SOURCE"] = str(source_root)
    if protocol_source:
        env["GITPARTNER_PROTOCOL_SOURCE"] = str(protocol_source)
    if engine_code_generation:
        env["ASCENDOP_EXPECTED_ENGINE_CODE_GENERATION"] = engine_code_generation
    argv = [
        sys.executable,
        "-m",
        "limited_remote_partner.endpoint.node_service",
        action,
        "--repo-dir",
        str(worktree),
        "--config",
        str(config_path),
        "--role",
        "client",
        "--stop-timeout-seconds",
        "20",
    ]
    if force_restart:
        argv.append("--force-restart")
    completed = subprocess.run(
        argv,
        cwd=str(worktree),
        env=env,
        capture_output=True,
        text=True,
        timeout=45,
        creationflags=process_creation_flags(),
        startupinfo=process_startupinfo(),
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        raise TransportInstallError(
            f"GitPartner service {action} failed with exit "
            f"{completed.returncode}: {detail[:1024]}"
        )
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise TransportInstallError(
            f"GitPartner service {action} returned invalid JSON"
        ) from exc
    if not isinstance(value, dict):
        raise TransportInstallError(
            f"GitPartner service {action} returned a non-object payload"
        )
    return value


def _safe_extract(archive: Path, destination: Path) -> None:
    destination = destination.resolve()
    with tarfile.open(archive, "r:gz") as tar:
        members = tar.getmembers()
        for member in members:
            target = (destination / member.name).resolve()
            if destination != target and destination not in target.parents:
                raise TransportInstallError(
                    f"transport archive escapes destination: {member.name}"
                )
            if member.issym() or member.islnk():
                raise TransportInstallError(
                    f"transport archive contains a link: {member.name}"
                )
        tar.extractall(destination, members=members)


def _read_pointer(path: Path) -> str:
    try:
        value = path.read_text(encoding="ascii").strip().lower()
    except OSError:
        return ""
    if not value:
        return ""
    _validate_digest(value, 64, "runtime pointer")
    return value


def _write_pointer(path: Path, generation: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_text(generation + "\n", encoding="ascii")
    os.replace(temporary, path)


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_text(
        json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_digest(value: str, length: int, label: str) -> None:
    if len(value) != length or any(char not in "0123456789abcdef" for char in value):
        raise TransportInstallError(f"invalid {label}: {value}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Atomically install one immutable Flow V3 GitPartner runtime."
    )
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--runtime-root", type=Path, required=True)
    parser.add_argument("--worktree", type=Path, required=True)
    parser.add_argument("--config-relative", type=Path, required=True)
    parser.add_argument("--expected-generation", required=True)
    parser.add_argument("--expected-archive-sha256", required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--protocol-source", type=Path)
    parser.add_argument("--engine-code-generation", default="")
    parser.add_argument("--release-generation", default="")
    args = parser.parse_args(argv)
    try:
        receipt = install_transport_runtime(
            archive=args.archive,
            runtime_root=args.runtime_root,
            worktree=args.worktree,
            config_relative=args.config_relative,
            expected_generation=args.expected_generation,
            expected_archive_sha256=args.expected_archive_sha256,
            receipt_path=args.receipt,
            protocol_source=args.protocol_source,
            engine_code_generation=args.engine_code_generation,
            release_generation=args.release_generation,
        )
    except TransportInstallError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(json.dumps(receipt, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
