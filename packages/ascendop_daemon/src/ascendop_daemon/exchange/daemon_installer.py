from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

class DaemonInstallError(RuntimeError): pass


_VALIDATION_MODULE: Any = None


def _installer_validation() -> Any:
    global _VALIDATION_MODULE
    if _VALIDATION_MODULE is not None:
        return _VALIDATION_MODULE
    path = Path(__file__).with_name("installer_validation.py")
    spec = importlib.util.spec_from_file_location("_ascendop_installer_validation", path)
    if spec is None or spec.loader is None:
        raise DaemonInstallError("installer validation product is unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _VALIDATION_MODULE = module
    return module


def _migrate_control_database(**kwargs: Any) -> dict[str, Any]:
    """Load the product migrator only after the standalone payload is parsed."""

    try:
        from ascendop_daemon.exchange.control_database_installer import (
            ControlDatabaseInstallError,
            migrate_control_database,
        )
    except ImportError as exc:
        raise DaemonInstallError(
            "control database migration product is unavailable"
        ) from exc
    try:
        return migrate_control_database(**kwargs)
    except ControlDatabaseInstallError as exc:
        raise DaemonInstallError(str(exc)) from exc


def daemon_generation(root: Path) -> str:
    root = root.resolve()
    paths = tuple(_daemon_files(root))
    if not paths or any(not path.is_file() for path in paths):
        raise DaemonInstallError(f"daemon package is incomplete: {root}")
    digest = hashlib.sha256()
    for path in paths:
        relative = path.relative_to(root).as_posix().encode("utf-8")
        payload = path.read_bytes().replace(b"\r\n", b"\n")
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def install_daemon_runtime(
    *,
    archive: Path,
    runtime_root: Path,
    transport_archive: Path,
    transport_runtime_root: Path,
    protocol_archive: Path,
    protocol_runtime_root: Path,
    control_archive: Path,
    control_runtime_root: Path,
    agent_runner_archive: Path,
    agent_runner_runtime_root: Path,
    official_eval_archive: Path,
    official_eval_runtime_root: Path,
    official_eval_config: Path,
    official_eval_policy_archive: Path,
    official_eval_policy_index: Path,
    variable_registry: Path,
    change_impact_policy: Path,
    daemon_config: Path,
    developer_runbook: Path,
    system_registry: Path,
    workflow_adapter: Path,
    workspace_root: Path,
    expected_generation: str,
    expected_archive_sha256: str,
    expected_transport_generation: str,
    expected_transport_archive_sha256: str,
    expected_protocol_generation: str,
    expected_protocol_archive_sha256: str,
    expected_control_generation: str,
    expected_control_archive_sha256: str,
    expected_agent_runner_generation: str,
    expected_agent_runner_archive_sha256: str,
    expected_official_eval_generation: str,
    expected_official_eval_archive_sha256: str,
    expected_official_eval_config_sha256: str,
    expected_official_eval_policy_archive_sha256: str,
    expected_official_eval_policy_index_sha256: str,
    official_eval_config_relative_path: str,
    expected_control_database_schema: int,
    expected_engine_code_generation: str,
    expected_variable_registry_sha256: str,
    expected_change_impact_policy_sha256: str,
    expected_daemon_config_sha256: str,
    expected_developer_runbook_sha256: str,
    expected_system_registry_sha256: str,
    expected_workflow_adapter_sha256: str,
    release_generation: str,
    receipt_path: Path,
    database: Path,
    start: bool = False,
) -> dict[str, Any]:
    archive = archive.resolve()
    runtime_root = runtime_root.resolve()
    transport_archive = transport_archive.resolve()
    transport_runtime_root = transport_runtime_root.resolve()
    protocol_archive = protocol_archive.resolve()
    protocol_runtime_root = protocol_runtime_root.resolve()
    control_archive = control_archive.resolve()
    control_runtime_root = control_runtime_root.resolve()
    agent_runner_archive = agent_runner_archive.resolve()
    agent_runner_runtime_root = agent_runner_runtime_root.resolve()
    official_eval_archive = official_eval_archive.resolve()
    official_eval_runtime_root = official_eval_runtime_root.resolve()
    official_eval_config = official_eval_config.resolve()
    official_eval_policy_archive = official_eval_policy_archive.resolve()
    official_eval_policy_index = official_eval_policy_index.resolve()
    variable_registry = variable_registry.resolve()
    change_impact_policy = change_impact_policy.resolve()
    daemon_config = daemon_config.resolve()
    developer_runbook = developer_runbook.resolve()
    system_registry = system_registry.resolve()
    workflow_adapter = workflow_adapter.resolve()
    workspace_root = workspace_root.resolve()
    receipt_path = receipt_path.resolve()
    if int(expected_control_database_schema) <= 0:
        raise DaemonInstallError("control database schema must be positive")
    for value, label in (
        (expected_generation, "daemon generation"),
        (expected_archive_sha256, "daemon archive SHA-256"),
        (expected_transport_generation, "transport generation"),
        (expected_transport_archive_sha256, "transport archive SHA-256"),
        (expected_protocol_generation, "protocol generation"),
        (expected_protocol_archive_sha256, "protocol archive SHA-256"),
        (expected_control_generation, "control generation"),
        (expected_control_archive_sha256, "control archive SHA-256"),
        (expected_agent_runner_generation, "Agent runner generation"),
        (expected_agent_runner_archive_sha256, "Agent runner archive SHA-256"),
        (expected_official_eval_generation, "official-eval generation"),
        (expected_official_eval_archive_sha256, "official-eval archive SHA-256"),
        (expected_official_eval_config_sha256, "official-eval config SHA-256"),
        (
            expected_official_eval_policy_archive_sha256,
            "official-eval policy archive SHA-256",
        ),
        (
            expected_official_eval_policy_index_sha256,
            "official-eval policy index SHA-256",
        ),
        (expected_variable_registry_sha256, "variable registry SHA-256"),
        (expected_change_impact_policy_sha256, "change-impact policy SHA-256"),
        (expected_daemon_config_sha256, "daemon config SHA-256"),
        (expected_developer_runbook_sha256, "Developer runbook SHA-256"),
        (expected_system_registry_sha256, "system registry SHA-256"),
        (expected_workflow_adapter_sha256, "workflow adapter SHA-256"),
        (release_generation, "release generation"),
    ):
        _validate_digest(value, label)
    _validate_short_generation(
        expected_engine_code_generation,
        "Engine code generation",
    )
    if not archive.is_file():
        raise DaemonInstallError(f"daemon archive is missing: {archive}")
    if not transport_archive.is_file():
        raise DaemonInstallError(
            f"transport archive is missing: {transport_archive}"
        )
    if not protocol_archive.is_file():
        raise DaemonInstallError(f"protocol archive is missing: {protocol_archive}")
    if not control_archive.is_file():
        raise DaemonInstallError(f"control archive is missing: {control_archive}")
    if not agent_runner_archive.is_file():
        raise DaemonInstallError(
            f"Agent runner archive is missing: {agent_runner_archive}"
        )
    for path, label in (
        (official_eval_archive, "official-eval archive"),
        (official_eval_config, "official-eval config"),
        (official_eval_policy_archive, "official-eval policy archive"),
        (official_eval_policy_index, "official-eval policy index"),
    ):
        if not path.is_file():
            raise DaemonInstallError(f"{label} is missing: {path}")
    if not variable_registry.is_file():
        raise DaemonInstallError(
            f"protocol variable registry is missing: {variable_registry}"
        )
    if not change_impact_policy.is_file():
        raise DaemonInstallError(
            f"change-impact policy is missing: {change_impact_policy}"
        )
    if not daemon_config.is_file():
        raise DaemonInstallError(f"daemon config is missing: {daemon_config}")
    if not developer_runbook.is_file() or developer_runbook.is_symlink():
        raise DaemonInstallError(
            f"Developer runbook is missing or unsafe: {developer_runbook}"
        )
    if not system_registry.is_file():
        raise DaemonInstallError(f"system registry is missing: {system_registry}")
    if not workflow_adapter.is_file() or workflow_adapter.is_symlink():
        raise DaemonInstallError(
            f"workflow adapter is missing or unsafe: {workflow_adapter}"
        )
    _validate_variable_registry_schema(
        variable_registry,
        expected=int(expected_control_database_schema),
    )
    impact_policy = _validate_change_impact_policy_schema(change_impact_policy)
    offline_patterns = _daemon_offline_patterns(impact_policy)
    actual_archive_sha256 = _file_digest(archive)
    if actual_archive_sha256 != expected_archive_sha256:
        raise DaemonInstallError(
            "daemon archive digest mismatch: "
            f"expected={expected_archive_sha256} actual={actual_archive_sha256}"
        )

    transport_source = _install_transport_product_runtime(
        archive=transport_archive,
        runtime_root=transport_runtime_root,
        expected_generation=expected_transport_generation,
        expected_archive_sha256=expected_transport_archive_sha256,
        release_generation=release_generation,
    )
    protocol_source = _install_python_product(
        archive=protocol_archive,
        runtime_root=protocol_runtime_root,
        expected_generation=expected_protocol_generation,
        expected_archive_sha256=expected_protocol_archive_sha256,
        package="ascendop_protocol",
        label="protocol",
    )
    control_source = _install_python_product(
        archive=control_archive,
        runtime_root=control_runtime_root,
        expected_generation=expected_control_generation,
        expected_archive_sha256=expected_control_archive_sha256,
        package="ascendop_control",
        label="control",
    )
    agent_runner_source = _install_python_product(
        archive=agent_runner_archive,
        runtime_root=agent_runner_runtime_root,
        expected_generation=expected_agent_runner_generation,
        expected_archive_sha256=expected_agent_runner_archive_sha256,
        package="ascendop_agent_runner",
        label="Agent runner",
    )
    official_eval_source = _install_python_product(
        archive=official_eval_archive,
        runtime_root=official_eval_runtime_root,
        expected_generation=expected_official_eval_generation,
        expected_archive_sha256=expected_official_eval_archive_sha256,
        package="official_eval",
        label="official-eval",
    )
    installed_official_eval_config = _install_official_eval_policies(
        config=official_eval_config,
        policy_archive=official_eval_policy_archive,
        policy_index=official_eval_policy_index,
        runtime_root=official_eval_runtime_root,
        workspace_root=workspace_root,
        config_relative_path=official_eval_config_relative_path,
        release_generation=release_generation,
        expected_config_sha256=expected_official_eval_config_sha256,
        expected_policy_archive_sha256=expected_official_eval_policy_archive_sha256,
        expected_policy_index_sha256=expected_official_eval_policy_index_sha256,
    )
    installed_variable_registry = _install_variable_registry(
        source=variable_registry,
        runtime_root=runtime_root / "variable-registries",
        expected_sha256=expected_variable_registry_sha256,
    )
    installed_change_impact_policy = _install_policy_artifact(
        source=change_impact_policy,
        runtime_root=runtime_root / "policies" / release_generation,
        destination_name="change-impact-policy.json",
        expected_sha256=expected_change_impact_policy_sha256,
        label="change-impact policy",
    )
    installed_daemon_config = _install_policy_artifact(
        source=daemon_config,
        runtime_root=runtime_root / "policies" / release_generation,
        destination_name="daemon-config.json",
        expected_sha256=expected_daemon_config_sha256,
        label="daemon config",
    )
    installed_developer_runbook = _install_policy_artifact(
        source=developer_runbook,
        runtime_root=runtime_root / "policies" / release_generation,
        destination_name="developer-capability-runbook.md",
        expected_sha256=expected_developer_runbook_sha256,
        label="Developer runbook",
    )
    installed_system_registry = _install_policy_artifact(
        source=system_registry,
        runtime_root=runtime_root / "policies" / release_generation,
        destination_name="system-registry.json",
        expected_sha256=expected_system_registry_sha256,
        label="system registry",
    )
    installed_workflow_adapter = _install_policy_artifact(
        source=workflow_adapter,
        runtime_root=(
            runtime_root
            / "workflow-adapters"
            / expected_workflow_adapter_sha256
        ),
        destination_name="next_workflow.py",
        expected_sha256=expected_workflow_adapter_sha256,
        label="workflow adapter",
    )

    generations = runtime_root / "generations"
    generations.mkdir(parents=True, exist_ok=True)
    destination = generations / expected_generation
    if destination.is_dir():
        _require_no_offline_daemon_files(destination, offline_patterns)
        actual_generation = daemon_generation(destination)
        if actual_generation != expected_generation:
            raise DaemonInstallError(
                "immutable daemon generation is corrupted: "
                f"expected={expected_generation} actual={actual_generation}"
            )
    else:
        staging = Path(tempfile.mkdtemp(prefix=".daemon-", dir=generations))
        try:
            _safe_extract(archive, staging)
            _require_no_offline_daemon_files(staging, offline_patterns)
            actual_generation = daemon_generation(staging)
            if actual_generation != expected_generation:
                raise DaemonInstallError(
                    "staged daemon generation mismatch: "
                    f"expected={expected_generation} actual={actual_generation}"
                )
            os.replace(staging, destination)
        finally:
            if staging.exists():
                shutil.rmtree(staging, ignore_errors=True)

    active_path = workspace_root / ".ascendop-work" / "runtime" / "active-release.json"
    previous = _read_json(active_path)
    previous_was_running = bool(previous) and _service_running(
        workspace_root=workspace_root,
        active=previous,
        config=installed_daemon_config,
        registry=installed_system_registry,
        database=database,
    )
    if previous_was_running:
        stopped = _service_command(
            action="stop",
            workspace_root=workspace_root,
            active=previous,
            config=installed_daemon_config,
            registry=installed_system_registry,
            database=database,
        )
        if bool(stopped.get("running")):
            stopped = _service_command(
                action="stop",
                workspace_root=workspace_root,
                active=previous,
                config=installed_daemon_config,
                registry=installed_system_registry,
                database=database,
                force_stop=True,
            )
        if bool(stopped.get("running")):
            raise DaemonInstallError(
                "previous resident generation remained live after exact force stop"
            )

    database_migration = _migrate_control_database(
        daemon_source=destination / "src",
        protocol_source=protocol_source,
        control_source=control_source,
        agent_runner_source=agent_runner_source,
        database=database,
        expected_schema=int(expected_control_database_schema),
        release_generation=release_generation,
        timeout_seconds=_variable_default_from_path(
            installed_variable_registry,
            "runtime.control_database_migration_timeout_seconds",
            120,
        ),
    )

    active = {
        "schema": "ascendop.active-release.v4",
        "release_generation": release_generation,
        "daemon_generation": expected_generation,
        "transport_generation": expected_transport_generation,
        "protocol_generation": expected_protocol_generation,
        "control_generation": expected_control_generation,
        "agent_runner_generation": expected_agent_runner_generation,
        "official_eval_generation": expected_official_eval_generation,
        "engine_code_generation": expected_engine_code_generation,
        "control_database_schema": int(expected_control_database_schema),
        "daemon_install_root": str(destination),
        "daemon_source": str(destination / "src"),
        "daemon_entrypoint_path": str(destination / "daemon.py"),
        "daemon_launcher_path": str(destination / "launch_s5_910b.py"),
        "daemon_launcher_bootstrap_path": str(destination / "launcher_bootstrap.py"),
        "transport_source": str(transport_source),
        "protocol_source": str(protocol_source),
        "control_source": str(control_source),
        "agent_runner_source": str(agent_runner_source),
        "official_eval_source": str(official_eval_source),
        "official_eval_config_path": str(installed_official_eval_config),
        "variable_registry_path": str(installed_variable_registry),
        "change_impact_policy_path": str(installed_change_impact_policy),
        "change_impact_policy_sha256": expected_change_impact_policy_sha256,
        "daemon_config_path": str(installed_daemon_config),
        "developer_runbook_path": str(installed_developer_runbook),
        "developer_runbook_sha256": expected_developer_runbook_sha256,
        "system_registry_path": str(installed_system_registry),
        "workflow_adapter_path": str(installed_workflow_adapter),
        "workflow_adapter_sha256": expected_workflow_adapter_sha256,
        "control_database_path": str(database.resolve()),
        "control_database_migration": database_migration,
        "activation_state": "activating" if start else "installed-stopped",
        "activated_at": datetime.now(timezone.utc).isoformat(),
    }
    _write_json_atomic(active_path, active)
    try:
        status: dict[str, Any] = {"running": False, "healthy": False}
        if start:
            status = _service_command(
                action="start",
                workspace_root=workspace_root,
                active=active,
                config=installed_daemon_config,
                registry=installed_system_registry,
                database=database,
            )
            if not bool(status.get("healthy")):
                raise DaemonInstallError("new daemon did not become healthy")
    except Exception as exc:
        stop_error = ""
        try:
            _service_command(
                action="stop",
                workspace_root=workspace_root,
                active=active,
                config=installed_daemon_config,
                registry=installed_system_registry,
                database=database,
                force_stop=True,
            )
        except (DaemonInstallError, subprocess.TimeoutExpired) as stop_exc:
            stop_error = str(stop_exc)
        failed_at = datetime.now(timezone.utc).isoformat()
        failure_path = (
            workspace_root
            / ".ascendop-work"
            / "runtime"
            / "release-activation-failure.json"
        )
        active.update(
            {
                "activation_state": "failed-stopped",
                "activation_failed_at": failed_at,
                "activation_failure_path": str(failure_path),
            }
        )
        failure = {
            "schema": "ascendop.release-activation-failure.v4",
            "release_generation": release_generation,
            "previous_release_generation": str(
                previous.get("release_generation") or ""
            ),
            "failed_at": failed_at,
            "reason": str(exc),
            "stop_error": stop_error,
            "state": "failed-stopped",
        }
        _write_json_atomic(active_path, active)
        _write_json_atomic(failure_path, failure)
        detail = str(exc)
        if stop_error:
            detail += f"; fail-closed stop failed: {stop_error}"
        raise DaemonInstallError(detail) from exc
    active["activation_state"] = "active" if start else "installed-stopped"
    _write_json_atomic(active_path, active)

    receipt = {
        "schema": "ascendop.daemon-deployment-receipt.v4",
        **active,
        "archive_sha256": actual_archive_sha256,
        "transport_archive_sha256": expected_transport_archive_sha256,
        "protocol_archive_sha256": expected_protocol_archive_sha256,
        "control_archive_sha256": expected_control_archive_sha256,
        "agent_runner_archive_sha256": expected_agent_runner_archive_sha256,
        "official_eval_archive_sha256": expected_official_eval_archive_sha256,
        "official_eval_config_sha256": expected_official_eval_config_sha256,
        "official_eval_policy_archive_sha256": (
            expected_official_eval_policy_archive_sha256
        ),
        "official_eval_policy_index_sha256": expected_official_eval_policy_index_sha256,
        "variable_registry_sha256": expected_variable_registry_sha256,
        "change_impact_policy_sha256": expected_change_impact_policy_sha256,
        "daemon_config_sha256": expected_daemon_config_sha256,
        "developer_runbook_sha256": expected_developer_runbook_sha256,
        "system_registry_sha256": expected_system_registry_sha256,
        "workflow_adapter_sha256": expected_workflow_adapter_sha256,
        "previous_release_generation": str(previous.get("release_generation") or ""),
        "service_status": status,
        "installed_at": datetime.now(timezone.utc).isoformat(),
    }
    _write_json_atomic(receipt_path, receipt)
    _write_json_atomic(destination / "DEPLOYMENT_RECEIPT.json", receipt)
    return receipt


def _install_transport_product_runtime(
    *,
    archive: Path,
    runtime_root: Path,
    expected_generation: str,
    expected_archive_sha256: str,
    release_generation: str,
) -> Path:
    actual_archive_sha256 = _file_digest(archive)
    if actual_archive_sha256 != expected_archive_sha256:
        raise DaemonInstallError(
            "transport archive digest mismatch: "
            f"expected={expected_archive_sha256} actual={actual_archive_sha256}"
        )
    generations = runtime_root / "generations"
    generations.mkdir(parents=True, exist_ok=True)
    destination = generations / release_generation
    if destination.is_dir():
        actual_generation = _transport_generation(destination / "src")
        if actual_generation != expected_generation:
            raise DaemonInstallError(
                "immutable local transport generation is corrupted: "
                f"expected={expected_generation} actual={actual_generation}"
            )
        return destination / "src"

    staging = Path(tempfile.mkdtemp(prefix=".transport-", dir=generations))
    try:
        _safe_extract(archive, staging)
        actual_generation = _transport_generation(staging / "src")
        if actual_generation != expected_generation:
            raise DaemonInstallError(
                "staged local transport generation mismatch: "
                f"expected={expected_generation} actual={actual_generation}"
            )
        os.replace(staging, destination)
    finally:
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)
    return destination / "src"


def _install_python_product(**kwargs: Any) -> Path:
    from ascendop_daemon.exchange.python_product_installer import (
        PythonProductInstallError,
        install_python_product_runtime,
    )

    try:
        return install_python_product_runtime(**kwargs)
    except PythonProductInstallError as exc:
        raise DaemonInstallError(str(exc)) from exc


def _install_variable_registry(
    *,
    source: Path,
    runtime_root: Path,
    expected_sha256: str,
) -> Path:
    actual_sha256 = _file_digest(source)
    if actual_sha256 != expected_sha256:
        raise DaemonInstallError(
            "variable registry digest mismatch: "
            f"expected={expected_sha256} actual={actual_sha256}"
        )
    destination = runtime_root / expected_sha256 / "variables.json"
    if destination.is_file():
        if _file_digest(destination) != expected_sha256:
            raise DaemonInstallError(
                f"immutable variable registry is corrupted: {destination}"
            )
        return destination
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    try:
        shutil.copyfile(source, temporary)
        if _file_digest(temporary) != expected_sha256:
            raise DaemonInstallError("copied variable registry digest mismatch")
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    return destination


def _install_policy_artifact(
    *,
    source: Path,
    runtime_root: Path,
    destination_name: str,
    expected_sha256: str,
    label: str,
) -> Path:
    actual_sha256 = _file_digest(source)
    if actual_sha256 != expected_sha256:
        raise DaemonInstallError(
            f"{label} digest mismatch: expected={expected_sha256} actual={actual_sha256}"
        )
    destination = runtime_root / destination_name
    if destination.is_file():
        if _file_digest(destination) != expected_sha256:
            raise DaemonInstallError(f"immutable {label} is corrupted: {destination}")
        return destination
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    try:
        shutil.copyfile(source, temporary)
        if _file_digest(temporary) != expected_sha256:
            raise DaemonInstallError(f"copied {label} digest mismatch")
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    return destination


def _install_official_eval_policies(
    *,
    config: Path,
    policy_archive: Path,
    policy_index: Path,
    runtime_root: Path,
    workspace_root: Path,
    config_relative_path: str,
    release_generation: str,
    expected_config_sha256: str,
    expected_policy_archive_sha256: str,
    expected_policy_index_sha256: str,
) -> Path:
    for path, expected, label in (
        (config, expected_config_sha256, "official-eval config"),
        (
            policy_archive,
            expected_policy_archive_sha256,
            "official-eval policy archive",
        ),
        (policy_index, expected_policy_index_sha256, "official-eval policy index"),
    ):
        actual = _file_digest(path)
        if actual != expected:
            raise DaemonInstallError(
                f"{label} digest mismatch: expected={expected} actual={actual}"
            )
    try:
        index = json.loads(policy_index.read_text(encoding="utf-8"))
        raw_config = json.loads(config.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DaemonInstallError("official-eval policy inputs are invalid JSON") from exc
    if index.get("schema") != "ascendop.official-eval-policy-index.v1":
        raise DaemonInstallError("unsupported official-eval policy index schema")
    entries = index.get("policies")
    if not isinstance(entries, list):
        raise DaemonInstallError("official-eval policy index has no policy list")
    if raw_config.get("schema") != "ascendop.official-eval.config.v1":
        raise DaemonInstallError("unsupported official-eval config schema")
    relative = Path(config_relative_path)
    if relative.is_absolute() or ".." in relative.parts:
        raise DaemonInstallError("official-eval config relative path is unsafe")
    virtual_config = (workspace_root / relative).resolve()
    _require_child(workspace_root, virtual_config, "official-eval config location")

    policy_key = hashlib.sha256(
        (
            expected_config_sha256
            + expected_policy_archive_sha256
            + expected_policy_index_sha256
        ).encode("ascii")
    ).hexdigest()[:16]
    destination = runtime_root / "p" / policy_key
    install_metadata = destination / "INSTALL.json"
    if destination.is_dir():
        metadata = _read_json(install_metadata)
        if (
            metadata.get("config_sha256") != expected_config_sha256
            or metadata.get("policy_archive_sha256")
            != expected_policy_archive_sha256
            or metadata.get("policy_index_sha256")
            != expected_policy_index_sha256
        ):
            raise DaemonInstallError(
                f"immutable official-eval policy installation is corrupted: {destination}"
            )
        installed_config = destination / "official-eval.json"
        if not installed_config.is_file():
            raise DaemonInstallError(
                f"installed official-eval config is missing: {installed_config}"
            )
        return installed_config

    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".p-", dir=destination.parent))
    try:
        extracted = staging
        _safe_extract(policy_archive, extracted)
        by_campaign: dict[str, Path] = {}
        for entry in entries:
            if not isinstance(entry, dict):
                raise DaemonInstallError("official-eval policy index entry is invalid")
            campaign_id = str(entry.get("campaign_id") or "").strip()
            archive_path = str(entry.get("archive_path") or "").replace("\\", "/")
            expected = str(entry.get("sha256") or "")
            if not campaign_id or campaign_id in by_campaign:
                raise DaemonInstallError(
                    f"official-eval policy campaign is invalid or duplicated: {campaign_id!r}"
                )
            candidate = (extracted / archive_path).resolve()
            _require_child(extracted, candidate, f"official-eval policy {campaign_id}")
            if not candidate.is_file() or _file_digest(candidate) != expected:
                raise DaemonInstallError(
                    f"official-eval policy digest mismatch: {campaign_id}"
                )
            final_policy = destination / archive_path
            by_campaign[campaign_id] = final_policy

        installed_config = json.loads(json.dumps(raw_config))
        runtime_text = str(installed_config.get("runtime_root") or "")
        installed_config["runtime_root"] = str(
            _resolve_policy_path(runtime_text, virtual_config.parent)
        )
        progress = installed_config.get("progress_adapter")
        if isinstance(progress, dict) and progress.get("kind") == "json_snapshot":
            progress["path"] = str(
                _resolve_policy_path(str(progress.get("path") or ""), virtual_config.parent)
            )
        campaigns = installed_config.get("campaigns")
        if not isinstance(campaigns, list):
            raise DaemonInstallError("official-eval config campaigns are invalid")
        for campaign in campaigns:
            if not isinstance(campaign, dict):
                raise DaemonInstallError("official-eval campaign is invalid")
            campaign_id = str(campaign.get("campaign_id") or "")
            policy = by_campaign.get(campaign_id)
            if policy is None:
                if bool(campaign.get("enabled")):
                    raise DaemonInstallError(
                        f"enabled official-eval campaign has no sealed policy: {campaign_id}"
                    )
                campaign.pop("standing_policy_path", None)
            else:
                campaign["standing_policy_path"] = str(policy)
        _write_json_atomic(staging / "official-eval.json", installed_config)
        _write_json_atomic(
            staging / "INSTALL.json",
            {
                "schema": "ascendop.official-eval-policy-install.v1",
                "release_generation": release_generation,
                "config_sha256": expected_config_sha256,
                "policy_archive_sha256": expected_policy_archive_sha256,
                "policy_index_sha256": expected_policy_index_sha256,
            },
        )
        os.replace(staging, destination)
    finally:
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)
    return destination / "official-eval.json"


def _resolve_policy_path(value: str, base: Path) -> Path:
    if not value.strip():
        raise DaemonInstallError("official-eval policy path is empty")
    path = Path(value)
    return path.resolve() if path.is_absolute() else (base / path).resolve()


def _require_child(root: Path, candidate: Path, label: str) -> None:
    root = root.resolve()
    candidate = candidate.resolve()
    if candidate != root and root not in candidate.parents:
        raise DaemonInstallError(f"{label} escapes the workspace: {candidate}")


def _transport_generation(source_root: Path) -> str:
    package_root = source_root / "limited_remote_partner"
    paths = sorted(package_root.rglob("*.py"), key=lambda item: item.as_posix())
    if not paths:
        raise DaemonInstallError(
            f"GitPartner package source is missing: {package_root}"
        )
    digest = hashlib.sha256()
    for path in paths:
        relative = path.relative_to(source_root).as_posix().encode("utf-8")
        payload = path.read_bytes().replace(b"\r\n", b"\n")
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def _service_running(**kwargs: Any) -> bool:
    try:
        return bool(_service_command(action="status", **kwargs).get("running"))
    except DaemonInstallError:
        workspace_root = Path(kwargs["workspace_root"])
        runtime = workspace_root / ".ascendop-work" / "runtime"
        return any(
            _process_identity_matches(
                int(metadata.get("pid") or 0),
                str(metadata.get("start_token") or ""),
            )
            for metadata in (
                _read_json(runtime / "v4-resident-service.json"),
                _read_json(runtime / "v4-daemon-service.json"),
                _read_json(runtime / "v3-daemon-service.json"),
            )
        )


def _service_command(
    *,
    action: str,
    workspace_root: Path,
    active: dict[str, Any],
    config: Path,
    registry: Path,
    database: Path,
    force_stop: bool = False,
) -> dict[str, Any]:
    daemon_source = Path(str(active.get("daemon_source") or "")).resolve()
    protocol_source = Path(str(active.get("protocol_source") or "")).resolve()
    control_source = Path(str(active.get("control_source") or "")).resolve()
    agent_runner_source = Path(
        str(active.get("agent_runner_source") or "")
    ).resolve()
    official_eval_source_text = str(active.get("official_eval_source") or "")
    official_eval_source = (
        Path(official_eval_source_text).resolve()
        if official_eval_source_text
        else None
    )
    config = Path(str(active.get("daemon_config_path") or config)).resolve()
    registry = Path(str(active.get("system_registry_path") or registry)).resolve()
    database = Path(str(active.get("control_database_path") or database)).resolve()
    if not (daemon_source / "ascendop_daemon").is_dir():
        raise DaemonInstallError(f"daemon source is missing: {daemon_source}")
    if not (protocol_source / "ascendop_protocol").is_dir():
        raise DaemonInstallError(f"protocol source is missing: {protocol_source}")
    if not (control_source / "ascendop_control").is_dir():
        raise DaemonInstallError(f"control source is missing: {control_source}")
    if not (agent_runner_source / "ascendop_agent_runner").is_dir():
        raise DaemonInstallError(
            f"Agent runner source is missing: {agent_runner_source}"
        )
    if official_eval_source is None:
        if action != "stop":
            raise DaemonInstallError(
                "active release does not expose official-eval source"
            )
    elif not (official_eval_source / "official_eval").is_dir():
        raise DaemonInstallError(
            f"official-eval source is missing: {official_eval_source}"
        )
    environment = os.environ.copy()
    environment["ASCENDOP_RELEASE_GENERATION"] = str(
        active.get("release_generation") or ""
    )
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment["ASCENDOP_VARIABLE_REGISTRY_PATH"] = str(
        active.get("variable_registry_path") or ""
    )
    environment["ASCENDOP_WORKSPACE_ROOT"] = str(workspace_root.resolve())
    if active.get("workflow_adapter_path"):
        environment["ASCENDOP_WORKFLOW_ADAPTER_PATH"] = str(
            active.get("workflow_adapter_path")
        )
        environment["ASCENDOP_WORKFLOW_ADAPTER_SHA256"] = str(
            active.get("workflow_adapter_sha256") or ""
        )
    environment["GITPARTNER_RUNTIME_SOURCE"] = str(
        active.get("transport_source") or ""
    )
    environment["GITPARTNER_PROTOCOL_SOURCE"] = str(protocol_source)
    environment["PYTHONPATH"] = os.pathsep.join(
        item
        for item in (
            str(daemon_source),
            str(protocol_source),
            str(control_source),
            str(agent_runner_source),
            str(official_eval_source) if official_eval_source is not None else "",
            environment.get("PYTHONPATH", ""),
        )
        if item
    )
    command = [
        sys.executable,
        "-m",
        "ascendop_daemon.cli.resident_main",
        action,
        "--root",
        str(workspace_root),
        "--config",
        str(config),
        "--registry",
        str(registry),
        "--database",
        str(database),
    ]
    if action == "stop":
        stop_wait_seconds = _active_variable_default(
            active,
            "runtime.stop_command_timeout_seconds",
            60,
        )
        command.extend(
            (
                "--reason",
                "Flow V4 immutable release switch",
                "--wait-seconds",
                str(stop_wait_seconds),
            )
        )
        if force_stop:
            command.append("--force")
    else:
        stop_wait_seconds = 0
    completed = subprocess.run(
        command,
        cwd=workspace_root,
        env=environment,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="strict",
        timeout=max(45, stop_wait_seconds + 15),
        creationflags=(
            int(getattr(subprocess, "CREATE_NO_WINDOW", 0))
            if os.name == "nt"
            else 0
        ),
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        raise DaemonInstallError(
            f"daemon service {action} failed with {completed.returncode}: {detail[:1024]}"
        )
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise DaemonInstallError("daemon service returned invalid JSON") from exc
    if not isinstance(value, dict):
        raise DaemonInstallError("daemon service returned a non-object payload")
    return value


def _active_variable_default(
    active: dict[str, Any],
    variable_id: str,
    fallback: int,
) -> int:
    path = Path(str(active.get("variable_registry_path") or ""))
    return _variable_default_from_path(path, variable_id, fallback)


def _variable_default_from_path(
    path: Path,
    variable_id: str,
    fallback: int,
) -> int:
    return int(
        _installer_validation().variable_default_from_path(
            path, variable_id=variable_id, fallback=fallback
        )
    )


def _daemon_files(root: Path) -> Iterable[Path]:
    return _installer_validation().daemon_files(root)


def _safe_extract(archive: Path, destination: Path) -> None:
    try:
        _installer_validation().safe_extract(archive, destination)
    except ValueError as exc:
        raise DaemonInstallError(str(exc)) from exc


def _read_json(path: Path) -> dict[str, Any]:
    return dict(_installer_validation().read_json(path))

def _write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    _installer_validation().write_json_atomic(path, value)

def _file_digest(path: Path) -> str:
    return str(_installer_validation().file_digest(path))


def _validate_variable_registry_schema(path: Path, *, expected: int) -> None:
    try:
        _installer_validation().validate_variable_registry_schema(path, expected=expected)
    except ValueError as exc:
        raise DaemonInstallError(str(exc)) from exc


def _validate_change_impact_policy_schema(path: Path) -> dict[str, Any]:
    try:
        return dict(_installer_validation().validate_change_impact_policy_schema(path))
    except ValueError as exc:
        raise DaemonInstallError(str(exc)) from exc

def _require_no_offline_daemon_files(root: Path, patterns: Iterable[str]) -> None:
    try:
        _installer_validation().require_no_offline_daemon_files(root, patterns)
    except ValueError as exc:
        raise DaemonInstallError(str(exc)) from exc


def _daemon_offline_patterns(policy: dict[str, Any]) -> tuple[str, ...]:
    return tuple(_installer_validation().daemon_offline_patterns(policy))


def _process_identity_matches(pid: int, expected_start_token: str) -> bool:
    from ascendop_daemon.runtime.process_identity import process_identity_matches

    return process_identity_matches(pid, expected_start_token)


def _validate_digest(value: str, label: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise DaemonInstallError(f"invalid {label}: {value}")


def _validate_short_generation(value: str, label: str) -> None:
    if len(value) != 16 or any(character not in "0123456789abcdef" for character in value):
        raise DaemonInstallError(f"invalid {label}: {value}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Install one immutable Flow V4 daemon")
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--runtime-root", type=Path, required=True)
    parser.add_argument("--transport-archive", type=Path, required=True)
    parser.add_argument("--transport-runtime-root", type=Path, required=True)
    parser.add_argument("--protocol-archive", type=Path, required=True)
    parser.add_argument("--protocol-runtime-root", type=Path, required=True)
    parser.add_argument("--control-archive", type=Path, required=True)
    parser.add_argument("--control-runtime-root", type=Path, required=True)
    parser.add_argument("--agent-runner-archive", type=Path, required=True)
    parser.add_argument("--agent-runner-runtime-root", type=Path, required=True)
    parser.add_argument("--official-eval-archive", type=Path, required=True)
    parser.add_argument("--official-eval-runtime-root", type=Path, required=True)
    parser.add_argument("--official-eval-config", type=Path, required=True)
    parser.add_argument("--official-eval-policy-archive", type=Path, required=True)
    parser.add_argument("--official-eval-policy-index", type=Path, required=True)
    parser.add_argument("--variable-registry", type=Path, required=True)
    parser.add_argument("--change-impact-policy", type=Path, required=True)
    parser.add_argument("--daemon-config", type=Path, required=True)
    parser.add_argument("--developer-runbook", type=Path, required=True)
    parser.add_argument("--system-registry", type=Path, required=True)
    parser.add_argument("--workflow-adapter", type=Path, required=True)
    parser.add_argument("--workspace-root", type=Path, required=True)
    parser.add_argument("--expected-generation", required=True)
    parser.add_argument("--expected-archive-sha256", required=True)
    parser.add_argument("--expected-transport-generation", required=True)
    parser.add_argument("--expected-transport-archive-sha256", required=True)
    parser.add_argument("--expected-protocol-generation", required=True)
    parser.add_argument("--expected-protocol-archive-sha256", required=True)
    parser.add_argument("--expected-control-generation", required=True)
    parser.add_argument("--expected-control-archive-sha256", required=True)
    parser.add_argument("--expected-agent-runner-generation", required=True)
    parser.add_argument("--expected-agent-runner-archive-sha256", required=True)
    parser.add_argument("--expected-official-eval-generation", required=True)
    parser.add_argument("--expected-official-eval-archive-sha256", required=True)
    parser.add_argument("--expected-official-eval-config-sha256", required=True)
    parser.add_argument(
        "--expected-official-eval-policy-archive-sha256", required=True
    )
    parser.add_argument(
        "--expected-official-eval-policy-index-sha256", required=True
    )
    parser.add_argument("--official-eval-config-relative-path", required=True)
    parser.add_argument("--expected-control-database-schema", type=int, required=True)
    parser.add_argument("--expected-engine-code-generation", required=True)
    parser.add_argument("--expected-variable-registry-sha256", required=True)
    parser.add_argument("--expected-change-impact-policy-sha256", required=True)
    parser.add_argument("--expected-daemon-config-sha256", required=True)
    parser.add_argument("--expected-developer-runbook-sha256", required=True)
    parser.add_argument("--expected-system-registry-sha256", required=True)
    parser.add_argument("--expected-workflow-adapter-sha256", required=True)
    parser.add_argument("--release-generation", required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--start", action="store_true")
    args = parser.parse_args(argv)
    try:
        result = install_daemon_runtime(
            archive=args.archive,
            runtime_root=args.runtime_root,
            transport_archive=args.transport_archive,
            transport_runtime_root=args.transport_runtime_root,
            protocol_archive=args.protocol_archive,
            protocol_runtime_root=args.protocol_runtime_root,
            control_archive=args.control_archive,
            control_runtime_root=args.control_runtime_root,
            agent_runner_archive=args.agent_runner_archive,
            agent_runner_runtime_root=args.agent_runner_runtime_root,
            official_eval_archive=args.official_eval_archive,
            official_eval_runtime_root=args.official_eval_runtime_root,
            official_eval_config=args.official_eval_config,
            official_eval_policy_archive=args.official_eval_policy_archive,
            official_eval_policy_index=args.official_eval_policy_index,
            variable_registry=args.variable_registry,
            change_impact_policy=args.change_impact_policy,
            daemon_config=args.daemon_config,
            developer_runbook=args.developer_runbook,
            system_registry=args.system_registry,
            workflow_adapter=args.workflow_adapter,
            workspace_root=args.workspace_root,
            expected_generation=args.expected_generation,
            expected_archive_sha256=args.expected_archive_sha256,
            expected_transport_generation=args.expected_transport_generation,
            expected_transport_archive_sha256=args.expected_transport_archive_sha256,
            expected_protocol_generation=args.expected_protocol_generation,
            expected_protocol_archive_sha256=args.expected_protocol_archive_sha256,
            expected_control_generation=args.expected_control_generation,
            expected_control_archive_sha256=args.expected_control_archive_sha256,
            expected_agent_runner_generation=args.expected_agent_runner_generation,
            expected_agent_runner_archive_sha256=(
                args.expected_agent_runner_archive_sha256
            ),
            expected_official_eval_generation=args.expected_official_eval_generation,
            expected_official_eval_archive_sha256=(
                args.expected_official_eval_archive_sha256
            ),
            expected_official_eval_config_sha256=(
                args.expected_official_eval_config_sha256
            ),
            expected_official_eval_policy_archive_sha256=(
                args.expected_official_eval_policy_archive_sha256
            ),
            expected_official_eval_policy_index_sha256=(
                args.expected_official_eval_policy_index_sha256
            ),
            official_eval_config_relative_path=(
                args.official_eval_config_relative_path
            ),
            expected_control_database_schema=args.expected_control_database_schema,
            expected_engine_code_generation=args.expected_engine_code_generation,
            expected_variable_registry_sha256=args.expected_variable_registry_sha256,
            expected_change_impact_policy_sha256=(
                args.expected_change_impact_policy_sha256
            ),
            expected_daemon_config_sha256=args.expected_daemon_config_sha256,
            expected_developer_runbook_sha256=(
                args.expected_developer_runbook_sha256
            ),
            expected_system_registry_sha256=args.expected_system_registry_sha256,
            expected_workflow_adapter_sha256=args.expected_workflow_adapter_sha256,
            release_generation=args.release_generation,
            receipt_path=args.receipt,
            database=args.database,
            start=args.start,
        )
    except DaemonInstallError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
