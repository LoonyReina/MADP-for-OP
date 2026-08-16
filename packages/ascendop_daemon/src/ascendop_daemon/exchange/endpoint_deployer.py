from __future__ import annotations

import argparse
import hashlib
import json
import os
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Sequence


RELEASE_SCHEMA = "ascendop.flow-release.v4"


class EndpointDeployError(RuntimeError):
    pass


@dataclass(frozen=True)
class DeploymentTarget:
    endpoint_id: str
    ssh_alias: str
    remote_root: str
    gitpartner_root: str
    transport_runtime_root: str
    protocol_runtime_root: str
    engine_root: str
    service_environment_file: str
    cann_environment_script: str
    config_relative: str


Runner = Callable[[Sequence[str], int], subprocess.CompletedProcess[str]]


def load_deployment_target(
    manifest_path: Path,
    endpoint_id: str,
) -> tuple[dict[str, Any], DeploymentTarget]:
    manifest_path = manifest_path.resolve()
    release = _read_object(manifest_path)
    if release.get("schema") != RELEASE_SCHEMA:
        raise EndpointDeployError(
            f"unsupported release schema: {release.get('schema')!r}"
        )
    release_dir = manifest_path.parent
    registry_entry = _archive_entry(release, "system_registry")
    registry_path = release_dir / str(registry_entry["path"])
    registry = _read_object(registry_path)
    routes = registry.get("route_endpoints")
    environments = registry.get("execution_environments")
    if not isinstance(routes, list) or not isinstance(environments, list):
        raise EndpointDeployError("release registry has no V2 endpoint topology")
    route = next(
        (
            row
            for row in routes
            if isinstance(row, dict) and row.get("endpoint_id") == endpoint_id
        ),
        None,
    )
    if route is None:
        raise EndpointDeployError(f"endpoint is absent from release: {endpoint_id}")
    environment_id = str(route.get("execution_environment_id") or "")
    environment = next(
        (
            row
            for row in environments
            if isinstance(row, dict)
            and row.get("execution_environment_id") == environment_id
        ),
        None,
    )
    if environment is None:
        raise EndpointDeployError(
            f"endpoint environment is absent from release: {environment_id}"
        )
    management = route.get("management_binding")
    if not isinstance(management, dict) or management.get("mode") != "direct-ssh":
        raise EndpointDeployError(
            f"endpoint has no direct-ssh maintenance binding: {endpoint_id}"
        )
    remote_root = _safe_remote_path(environment.get("remote_root"), "remote_root")
    gitpartner_root = _safe_remote_path(
        management.get("remote_gitpartner_root"), "remote_gitpartner_root"
    )
    transport_runtime_root = _safe_remote_path(
        management.get("remote_runtime_root"), "remote_runtime_root"
    )
    service_environment_file = _safe_remote_path(
        management.get("service_environment_file"), "service_environment_file"
    )
    cann_environment_script = _safe_remote_path(
        management.get("cann_environment_script"), "cann_environment_script"
    )
    engine_value = str(environment.get("engine_root") or "test_engine")
    engine_path = PurePosixPath(engine_value)
    engine_root = str(
        engine_path if engine_path.is_absolute() else PurePosixPath(remote_root) / engine_path
    )
    _safe_remote_path(engine_root, "engine_root")
    ssh_alias = str(management.get("ssh_alias") or "")
    if not ssh_alias or any(
        char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
        for char in ssh_alias
    ):
        raise EndpointDeployError(f"unsafe maintenance ssh alias: {ssh_alias!r}")
    node_config = _safe_workspace_relative(
        route.get("node_gitpartner_config"), "node_gitpartner_config"
    )
    prefix = "GitPartner/"
    if not node_config.startswith(prefix):
        raise EndpointDeployError(
            "node_gitpartner_config must be rooted in the canonical GitPartner product"
        )
    config_relative = node_config[len(prefix) :]
    runtime_parent = PurePosixPath(transport_runtime_root).parent
    return release, DeploymentTarget(
        endpoint_id=endpoint_id,
        ssh_alias=ssh_alias,
        remote_root=remote_root,
        gitpartner_root=gitpartner_root,
        transport_runtime_root=transport_runtime_root,
        protocol_runtime_root=str(runtime_parent / "protocol"),
        engine_root=engine_root,
        service_environment_file=service_environment_file,
        cann_environment_script=cann_environment_script,
        config_relative=config_relative,
    )


def deploy_endpoint_release(
    *,
    manifest_path: Path,
    endpoint_id: str,
    receipt_path: Path,
    drain_timeout_seconds: int = 300,
    transfer_attempts: int = 3,
    runner: Runner | None = None,
) -> dict[str, Any]:
    runner = runner or _run
    manifest_path = manifest_path.resolve()
    release, target = load_deployment_target(manifest_path, endpoint_id)
    release_dir = manifest_path.parent
    generation = str(release.get("release_generation") or "")
    if not _is_hex(generation, 64):
        raise EndpointDeployError(f"invalid release generation: {generation!r}")
    staging = str(
        PurePosixPath(target.transport_runtime_root).parent / "staging" / generation
    )
    _ssh(runner, target.ssh_alias, f"mkdir -p -- {shlex.quote(staging)}")

    upload_names = ("daemon", "transport", "protocol", "engine")
    uploaded: dict[str, dict[str, str]] = {}
    for name in upload_names:
        entry = _archive_entry(release, name)
        source = (release_dir / str(entry["path"])).resolve()
        expected = str(entry.get("sha256") or "")
        if _file_digest(source) != expected:
            raise EndpointDeployError(f"release archive digest mismatch: {name}")
        destination = str(PurePosixPath(staging) / source.name)
        _upload_atomic(
            runner,
            target.ssh_alias,
            source,
            destination,
            expected,
            attempts=max(1, transfer_attempts),
        )
        uploaded[name] = {"path": destination, "sha256": expected}

    remote_manifest = str(PurePosixPath(staging) / "RELEASE.json")
    _upload_atomic(
        runner,
        target.ssh_alias,
        manifest_path,
        remote_manifest,
        _file_digest(manifest_path),
        attempts=max(1, transfer_attempts),
    )
    _extract_installers(runner, target.ssh_alias, staging, uploaded)

    drain_started = _utc_now()
    _engine_json(runner, target, "set-capacity --drain")
    deadline = time.monotonic() + max(1, int(drain_timeout_seconds))
    final_status: dict[str, Any] = {}
    while time.monotonic() < deadline:
        final_status = _engine_json(runner, target, "transport-status")
        if (
            int(final_status.get("accepted_nonterminal", 0) or 0) == 0
            and int(final_status.get("active_nonterminal", 0) or 0) == 0
            and int(final_status.get("queued_nonterminal", 0) or 0) == 0
        ):
            break
        time.sleep(1.0)
    else:
        raise EndpointDeployError(
            "endpoint did not drain accepted, active, and queued work before "
            "the maintenance deadline"
        )
    preserved_return_ready = _return_ready_identities(final_status)

    protocol_source = str(
        PurePosixPath(target.protocol_runtime_root)
        / "generations"
        / str(release["protocol_generation"])
        / "src"
    )
    commands = _installation_commands(
        release=release,
        target=target,
        staging=staging,
        uploaded=uploaded,
        protocol_source=protocol_source,
    )
    for command in commands:
        _ssh(runner, target.ssh_alias, command, timeout=180)

    installed_status = _engine_json(runner, target, "transport-status")
    installed_return_ready = _return_ready_identities(installed_status)
    if installed_return_ready != preserved_return_ready:
        raise EndpointDeployError(
            "return-ready evidence identity changed during endpoint deployment"
        )
    resumed = _engine_json(runner, target, "set-capacity --resume")
    status = _engine_json(runner, target, "transport-status")
    if str(status.get("engine_code_generation") or "") != str(
        release.get("engine_code_generation") or ""
    ):
        raise EndpointDeployError("Engine generation differs after endpoint deployment")
    if bool(status.get("capacity", {}).get("draining", True)):
        raise EndpointDeployError("Engine remained drained after successful deployment")
    receipt = {
        "schema": "ascendop.endpoint-deployment-receipt.v4",
        "endpoint_id": endpoint_id,
        "release_generation": generation,
        "transport_generation": str(release["transport_generation"]),
        "protocol_generation": str(release["protocol_generation"]),
        "engine_code_generation": str(release["engine_code_generation"]),
        "node_config": target.config_relative,
        "drain_started_at": drain_started,
        "completed_at": _utc_now(),
        "uploaded": uploaded,
        "remote_status": {
            "accepted_nonterminal": int(status.get("accepted_nonterminal", 0) or 0),
            "return_ready_count": int(status.get("return_ready_count", 0) or 0),
            "engine_code_generation": str(status.get("engine_code_generation") or ""),
            "draining": bool(status.get("capacity", {}).get("draining", True)),
        },
        "preserved_return_ready": preserved_return_ready,
        "resume_response": _bounded_engine_status(resumed),
    }
    _write_json_atomic(receipt_path.resolve(), receipt)
    return receipt


def _bounded_engine_status(status: dict[str, Any]) -> dict[str, Any]:
    capacity = status.get("capacity")
    if not isinstance(capacity, dict):
        capacity = {}
    return {
        "accepted_nonterminal": int(status.get("accepted_nonterminal", 0) or 0),
        "active_nonterminal": int(status.get("active_nonterminal", 0) or 0),
        "queued_nonterminal": int(status.get("queued_nonterminal", 0) or 0),
        "return_ready_count": int(status.get("return_ready_count", 0) or 0),
        "engine_code_generation": str(status.get("engine_code_generation") or ""),
        "engine_generation": str(status.get("engine_generation") or ""),
        "device_slots_free": int(status.get("device_slots_free", 0) or 0),
        "host_slots_free": int(status.get("host_slots_free", 0) or 0),
        "draining": bool(capacity.get("draining", True)),
    }


def _return_ready_identities(status: dict[str, Any]) -> list[dict[str, str]]:
    expected = int(status.get("return_ready_count", 0) or 0)
    jobs = status.get("jobs")
    if not isinstance(jobs, list):
        jobs = []
    identities: list[dict[str, str]] = []
    for row in jobs:
        if not isinstance(row, dict):
            continue
        if not str(row.get("return_ready_at") or ""):
            continue
        if str(row.get("returned_at") or ""):
            continue
        identities.append(
            {
                "engine_job_id": str(row.get("engine_job_id") or ""),
                "request_id": str(row.get("request_id") or ""),
                "attempt_id": str(row.get("attempt_id") or ""),
                "state": str(row.get("state") or ""),
                "terminal_at": str(row.get("terminal_at") or ""),
                "return_ready_at": str(row.get("return_ready_at") or ""),
            }
        )
    identities.sort(key=lambda item: (item["request_id"], item["attempt_id"]))
    if len(identities) != expected:
        raise EndpointDeployError(
            "transport-status did not expose every return-ready identity: "
            f"expected={expected}; observed={len(identities)}"
        )
    if any(
        not item["engine_job_id"]
        or not item["request_id"]
        or not item["attempt_id"]
        or item["state"] not in {"completed", "failed"}
        for item in identities
    ):
        raise EndpointDeployError("return-ready identity is incomplete")
    return identities


def _installation_commands(
    *,
    release: dict[str, Any],
    target: DeploymentTarget,
    staging: str,
    uploaded: dict[str, dict[str, str]],
    protocol_source: str,
) -> list[str]:
    q = shlex.quote
    work_root_prefix = f"cd {q(target.remote_root)} && "
    protocol_receipt = str(PurePosixPath(staging) / "PROTOCOL_DEPLOYMENT_RECEIPT.json")
    engine_receipt = str(PurePosixPath(staging) / "ENGINE_DEPLOYMENT_RECEIPT.json")
    transport_receipt = str(PurePosixPath(staging) / "TRANSPORT_DEPLOYMENT_RECEIPT.json")
    engine_payload = str(PurePosixPath(staging) / "engine-payload")
    extract_engine = (
        f"test -f {q(engine_payload + '/runtime_manifest.json')} || "
        f"(tmp=$(mktemp -d {q(staging + '/.engine-payload.XXXXXX')}) && "
        f"tar -xzf {q(uploaded['engine']['path'])} -C \"$tmp\" && "
        f"mv \"$tmp\" {q(engine_payload)})"
    )
    protocol = " ".join(
        (
            "python3",
            q(str(PurePosixPath(staging) / "protocol_installer.py")),
            "--archive",
            q(uploaded["protocol"]["path"]),
            "--runtime-root",
            q(target.protocol_runtime_root),
            "--expected-generation",
            q(str(release["protocol_generation"])),
            "--expected-archive-sha256",
            q(uploaded["protocol"]["sha256"]),
            "--receipt",
            q(protocol_receipt),
        )
    )
    engine = " ".join(
        (
            "python3",
            q(str(PurePosixPath(staging) / "direct_engine_code_sync.py")),
            "--source",
            q(engine_payload),
            "--target-repo",
            q(target.gitpartner_root),
            "--engine-root",
            q(target.engine_root),
            "--expected-generation",
            q(str(release["engine_code_generation"])),
            "--receipt",
            q(engine_receipt),
        )
    )
    transport = " ".join(
        (
            "python3",
            q(str(PurePosixPath(staging) / "transport_installer.py")),
            "--archive",
            q(uploaded["transport"]["path"]),
            "--runtime-root",
            q(target.transport_runtime_root),
            "--worktree",
            q(target.gitpartner_root),
            "--config-relative",
            q(target.config_relative),
            "--expected-generation",
            q(str(release["transport_generation"])),
            "--expected-archive-sha256",
            q(uploaded["transport"]["sha256"]),
            "--receipt",
            q(transport_receipt),
            "--protocol-source",
            q(protocol_source),
            "--service-environment-file",
            q(target.service_environment_file),
            "--cann-environment-script",
            q(target.cann_environment_script),
            "--engine-code-generation",
            q(str(release["engine_code_generation"])),
            "--release-generation",
            q(str(release["release_generation"])),
        )
    )
    # Every immutable installer bounds all source and destination paths against
    # its current working directory.  Maintenance staging lives below the
    # endpoint remote root, so execute the whole install transaction there.
    return [
        work_root_prefix + command
        for command in (protocol, extract_engine, engine, transport)
    ]


def _extract_installers(
    runner: Runner,
    alias: str,
    staging: str,
    uploaded: dict[str, dict[str, str]],
) -> None:
    q = shlex.quote
    commands = (
        (
            uploaded["daemon"]["path"],
            "src/ascendop_daemon/exchange/protocol_installer.py",
            "protocol_installer.py",
        ),
        (
            uploaded["daemon"]["path"],
            "src/ascendop_daemon/exchange/transport_installer.py",
            "transport_installer.py",
        ),
        (
            uploaded["transport"]["path"],
            "src/limited_remote_partner/maintenance/direct_engine_code_sync.py",
            "direct_engine_code_sync.py",
        ),
    )
    for archive, member, name in commands:
        destination = str(PurePosixPath(staging) / name)
        temporary = destination + ".part"
        command = (
            f"tar -xOzf {q(archive)} {q(member)} > {q(temporary)} && "
            f"chmod 700 {q(temporary)} && mv -f -- {q(temporary)} {q(destination)}"
        )
        _ssh(runner, alias, command)


def _engine_json(
    runner: Runner,
    target: DeploymentTarget,
    command: str,
) -> dict[str, Any]:
    q = shlex.quote
    remote = (
        f"cd {q(target.remote_root)} && "
        f"gen=$(cat {q(target.engine_root + '/runtime/current')}) && "
        f"PYTHONPATH={q(target.engine_root)}/runtime/generations/$gen/src "
        "python3 -m limited_remote_partner.cli.test_engine_cli "
        f"--root {q(target.engine_root)} {command}"
    )
    completed = _ssh(runner, target.ssh_alias, remote, timeout=60)
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise EndpointDeployError("remote Engine command returned invalid JSON") from exc
    if not isinstance(value, dict):
        raise EndpointDeployError("remote Engine command did not return an object")
    return value


def _upload_atomic(
    runner: Runner,
    alias: str,
    source: Path,
    destination: str,
    expected_sha256: str,
    *,
    attempts: int,
) -> None:
    q = shlex.quote
    temporary = destination + ".part"
    errors: list[str] = []
    for _ in range(attempts):
        completed = runner(
            ["scp", "-O", str(source), f"{alias}:{temporary}"],
            180,
        )
        if completed.returncode != 0:
            errors.append(_detail(completed))
            continue
        verify = _ssh(
            runner,
            alias,
            f"sha256sum -- {q(temporary)} | cut -d ' ' -f 1",
        )
        if verify.stdout.strip() != expected_sha256:
            errors.append("remote digest mismatch")
            continue
        _ssh(
            runner,
            alias,
            f"mv -f -- {q(temporary)} {q(destination)}",
        )
        return
    raise EndpointDeployError(
        f"failed to publish {source.name} after {attempts} attempts: "
        + "; ".join(errors[-attempts:])
    )


def _ssh(
    runner: Runner,
    alias: str,
    command: str,
    *,
    timeout: int = 30,
) -> subprocess.CompletedProcess[str]:
    completed = runner(["ssh", alias, command], timeout)
    if completed.returncode != 0:
        raise EndpointDeployError(
            f"remote maintenance command failed rc={completed.returncode}: "
            f"{_detail(completed)[-2000:]}"
        )
    return completed


def _run(command: Sequence[str], timeout: int) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(command),
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        check=False,
        timeout=timeout,
        creationflags=(
            int(getattr(subprocess, "CREATE_NO_WINDOW", 0)) if os.name == "nt" else 0
        ),
    )


def _activate_installed_runtime_for_relay(
    manifest_path: Path,
) -> tuple[Path, Path]:
    manifest_path = manifest_path.resolve()
    release = _read_object(manifest_path)
    release_generation = str(release.get("release_generation") or "")
    transport_generation = str(release.get("transport_generation") or "")
    protocol_generation = str(release.get("protocol_generation") or "")
    active_path: Path | None = None
    workspace_root: Path | None = None
    for candidate in manifest_path.parents:
        probe = candidate / ".ascendop-work" / "runtime" / "active-release.json"
        if probe.is_file():
            active_path = probe
            workspace_root = candidate
            break
    if active_path is None or workspace_root is None:
        raise EndpointDeployError(
            "relay deployment requires a locally installed active release"
        )
    active = _read_object(active_path)
    expected = {
        "release_generation": release_generation,
        "transport_generation": transport_generation,
        "protocol_generation": protocol_generation,
    }
    mismatches = [
        key for key, value in expected.items() if active.get(key) != value
    ]
    if mismatches:
        raise EndpointDeployError(
            "relay deployment active-release mismatch: " + ", ".join(mismatches)
        )
    expected_root = (workspace_root / ".ascendop-work" / "runtime").resolve()
    sources = (
        _installed_source(
            active,
            field="transport_source",
            package="limited_remote_partner",
            expected_root=expected_root,
        ),
        _installed_source(
            active,
            field="protocol_source",
            package="ascendop_protocol",
            expected_root=expected_root,
        ),
    )
    existing = os.environ.get("PYTHONPATH", "")
    values = [str(source) for source in sources]
    if existing:
        values.append(existing)
    os.environ["PYTHONPATH"] = os.pathsep.join(values)
    return sources


def _installed_source(
    active: dict[str, Any],
    *,
    field: str,
    package: str,
    expected_root: Path,
) -> Path:
    source = Path(str(active.get(field) or "")).resolve()
    if (
        not source.is_dir()
        or expected_root not in source.parents
        or not (source / package).is_dir()
    ):
        raise EndpointDeployError(
            f"relay deployment active {field} is invalid"
        )
    return source


def _archive_entry(release: dict[str, Any], name: str) -> dict[str, Any]:
    archives = release.get("archives")
    entry = archives.get(name) if isinstance(archives, dict) else None
    if not isinstance(entry, dict) or not entry.get("path") or not _is_hex(
        str(entry.get("sha256") or ""), 64
    ):
        raise EndpointDeployError(f"release archive metadata is invalid: {name}")
    return entry


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EndpointDeployError(f"cannot read JSON object: {path}") from exc
    if not isinstance(value, dict):
        raise EndpointDeployError(f"JSON document must be an object: {path}")
    return value


def _safe_remote_path(value: object, field: str) -> str:
    text = str(value or "")
    path = PurePosixPath(text)
    if (
        not path.is_absolute()
        or ".." in path.parts
        or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-/"
               for char in text)
    ):
        raise EndpointDeployError(f"unsafe {field}: {text!r}")
    return text.rstrip("/") or "/"


def _safe_workspace_relative(value: object, field: str) -> str:
    text = str(value or "").replace("\\", "/")
    path = PurePosixPath(text)
    if (
        not text
        or path.is_absolute()
        or ".." in path.parts
        or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-/"
               for char in text)
    ):
        raise EndpointDeployError(f"unsafe {field}: {text!r}")
    return text


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise EndpointDeployError(f"cannot read release artifact: {path}") from exc
    return digest.hexdigest()


def _is_hex(value: str, length: int) -> bool:
    return len(value) == length and all(char in "0123456789abcdef" for char in value)


def _detail(completed: subprocess.CompletedProcess[str]) -> str:
    return "\n".join(
        part.strip()
        for part in (completed.stdout, completed.stderr)
        if part and part.strip()
    )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def endpoint_deployment_mode(manifest_path: Path, endpoint_id: str) -> str:
    manifest_path = manifest_path.resolve()
    release = _read_object(manifest_path)
    registry_entry = _archive_entry(release, "system_registry")
    registry = _read_object(manifest_path.parent / str(registry_entry["path"]))
    routes = registry.get("route_endpoints")
    if not isinstance(routes, list):
        raise EndpointDeployError("release registry has no endpoint topology")
    route = next(
        (
            row
            for row in routes
            if isinstance(row, dict) and row.get("endpoint_id") == endpoint_id
        ),
        None,
    )
    if route is None:
        raise EndpointDeployError(f"endpoint is absent from release: {endpoint_id}")
    management = route.get("management_binding")
    if isinstance(management, dict) and management.get("mode") == "direct-ssh":
        return "direct-ssh"
    transport = route.get("transport_binding")
    if isinstance(transport, dict) and transport.get("mode") == "lan-relay":
        return "lan-relay"
    raise EndpointDeployError(
        f"endpoint has no supported V4 maintenance route: {endpoint_id}"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Deploy one immutable Flow V4 release to a drained SSH endpoint."
    )
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--endpoint-id", required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--drain-timeout-seconds", type=int, default=300)
    parser.add_argument("--transfer-attempts", type=int, default=3)
    args = parser.parse_args(argv)
    try:
        mode = endpoint_deployment_mode(args.manifest, args.endpoint_id)
        if mode == "direct-ssh":
            receipt = deploy_endpoint_release(
                manifest_path=args.manifest,
                endpoint_id=args.endpoint_id,
                receipt_path=args.receipt,
                drain_timeout_seconds=args.drain_timeout_seconds,
                transfer_attempts=args.transfer_attempts,
            )
        else:
            from ascendop_daemon.exchange.relay_endpoint_deployer import (
                RelayEndpointDeployError,
                deploy_relay_endpoint_release,
            )

            try:
                _activate_installed_runtime_for_relay(args.manifest)
                receipt = deploy_relay_endpoint_release(
                    manifest_path=args.manifest,
                    endpoint_id=args.endpoint_id,
                    receipt_path=args.receipt,
                    wait_timeout_seconds=max(
                        300, int(args.drain_timeout_seconds) + 120
                    ),
                    runner=_run,
                )
            except RelayEndpointDeployError as exc:
                raise EndpointDeployError(str(exc)) from exc
    except (EndpointDeployError, subprocess.TimeoutExpired) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(json.dumps(receipt, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
