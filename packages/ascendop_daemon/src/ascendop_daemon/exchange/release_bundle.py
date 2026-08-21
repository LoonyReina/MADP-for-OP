from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import shutil
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

from ascendop_daemon.exchange.release_layout import (
    daemon_file_is_offline,
    daemon_offline_patterns,
)
from ascendop_daemon.exchange.release_bundle_validation import (
    ReleaseBundleError,
    validate_campaign_alignment as _validate_campaign_alignment,
    validate_flow_v5_role_alignment as _validate_flow_v5_role_alignment,
)
from ascendop_daemon.exchange.release_engine_bundle import (
    load_engine_manifest as _load_engine_manifest,
    validate_engine_manifest as _validate_engine_manifest,
    write_engine_archive as _write_engine_archive,
)
from ascendop_daemon.exchange.transport_installer import transport_generation
from ascendop_daemon.registry.models import SystemRegistryError
from ascendop_daemon.registry.system_registry import SystemRegistry
from ascendop_daemon.storage.control_types import SCHEMA_VERSION


RELEASE_SCHEMA = "ascendop.flow-release.v4"


def build_flow_release(
    *,
    workspace_root: Path,
    gp_root: Path,
    output_root: Path,
    daemon_config: Path | None = None,
    system_registry: Path | None = None,
    official_eval_config: Path | None = None,
    component_cache_root: Path | None = None,
    engine_manifest_provider: Callable[[Path, Path], dict[str, object]] | None = None,
) -> dict[str, object]:
    workspace_root = workspace_root.resolve()
    gp_root = gp_root.resolve()
    output_root = output_root.resolve()
    component_cache_root = (
        component_cache_root
        or workspace_root / ".ascendop-work" / "c"
    ).resolve()
    protocol_root = workspace_root / "packages" / "ascendop_protocol"
    control_root = workspace_root / "packages" / "ascendop_control"
    agent_runner_root = workspace_root / "packages" / "ascendop_agent_runner"
    official_eval_root = workspace_root / "tools" / "official_eval_daemon"
    daemon_root = workspace_root / "tools" / "tester_daemon"
    daemon_config = (
        daemon_config
        or daemon_root / "config" / "cann_ladder_910b_cann90.json"
    ).resolve()
    system_registry = (
        system_registry
        or workspace_root / "Develop" / "registry" / "system_registry.json"
    ).resolve()
    official_eval_config = (
        official_eval_config
        or official_eval_root / "config" / "august.json"
    ).resolve()
    variable_registry = (
        workspace_root / "docs" / "flow_v4" / "variables.json"
    )
    change_impact_policy = (
        workspace_root / "docs" / "flow_v4" / "change_impact_policy.json"
    )
    workflow_adapter = workspace_root / "scripts" / "next_workflow.py"
    if not (protocol_root / "src" / "ascendop_protocol").is_dir():
        raise ReleaseBundleError(f"shared protocol package is missing: {protocol_root}")
    if not (control_root / "src" / "ascendop_control").is_dir():
        raise ReleaseBundleError(f"control package is missing: {control_root}")
    if not (agent_runner_root / "src" / "ascendop_agent_runner").is_dir():
        raise ReleaseBundleError(f"Agent runner package is missing: {agent_runner_root}")
    if not (official_eval_root / "official_eval").is_dir():
        raise ReleaseBundleError(
            f"official-eval package is missing: {official_eval_root}"
        )
    if not (official_eval_root / "pyproject.toml").is_file():
        raise ReleaseBundleError(
            f"official-eval product metadata is missing: {official_eval_root}"
        )
    if not variable_registry.is_file():
        raise ReleaseBundleError(
            f"protocol variable registry is missing: {variable_registry}"
        )
    impact_policy = _validate_change_impact_policy(change_impact_policy)
    if not workflow_adapter.is_file() or workflow_adapter.is_symlink():
        raise ReleaseBundleError(
            f"workflow adapter is missing or unsafe: {workflow_adapter}"
        )
    _require_child(workspace_root, daemon_config, "daemon config")
    _require_child(workspace_root, system_registry, "system registry")
    _require_child(workspace_root, official_eval_config, "official-eval config")
    if not daemon_config.is_file():
        raise ReleaseBundleError(f"daemon config is missing: {daemon_config}")
    if not system_registry.is_file():
        raise ReleaseBundleError(f"system registry is missing: {system_registry}")
    if not official_eval_config.is_file():
        raise ReleaseBundleError(
            f"official-eval config is missing: {official_eval_config}"
        )
    _validate_campaign_alignment(
        daemon_config=daemon_config,
        official_eval_config=official_eval_config,
    )
    _validate_flow_v5_role_alignment(
        daemon_config=daemon_config,
        official_eval_config=official_eval_config,
        system_registry=system_registry,
    )
    developer_runbook = _configured_developer_runbook(
        workspace_root=workspace_root,
        daemon_config=daemon_config,
    )
    _validate_registered_endpoint_configs(
        gp_root=gp_root,
        system_registry=system_registry,
    )

    transport_gen = transport_generation(gp_root / "src")
    transport_files = tuple(_transport_files(gp_root))
    transport_payload_gen = _tree_generation(gp_root, transport_files)
    protocol_files = tuple(_protocol_files(protocol_root))
    protocol_gen = _tree_generation(protocol_root, protocol_files)
    control_files = tuple(_python_product_files(control_root, "ascendop_control"))
    control_gen = _tree_generation(control_root, control_files)
    agent_runner_files = tuple(
        _python_product_files(agent_runner_root, "ascendop_agent_runner")
    )
    agent_runner_gen = _tree_generation(agent_runner_root, agent_runner_files)
    official_eval_files = tuple(_official_eval_product_files(official_eval_root))
    official_eval_gen = _mapped_tree_generation(official_eval_files)
    official_eval_policies = _official_eval_policy_entries(
        workspace_root=workspace_root,
        config=official_eval_config,
    )
    daemon_exclusions = daemon_offline_patterns(impact_policy)
    daemon_files = tuple(_daemon_files(daemon_root, daemon_exclusions))
    daemon_gen = _tree_generation(daemon_root, daemon_files)
    windows_bootstrap_files = tuple(_windows_bootstrap_files(daemon_root))
    windows_bootstrap_gen = _tree_generation(
        daemon_root, windows_bootstrap_files
    )
    release_tooling_files = tuple(
        _release_tooling_files(workspace_root, daemon_root)
    )
    release_tooling_gen = _mapped_tree_generation(release_tooling_files)
    official_eval_policy_files = _unique_policy_archive_files(
        official_eval_policies
    )
    official_eval_policy_gen = _mapped_tree_generation(
        official_eval_policy_files
    )
    engine_manifest = (
        engine_manifest_provider(gp_root, protocol_root)
        if engine_manifest_provider is not None
        else _load_engine_manifest(gp_root, protocol_root)
    )
    engine_files = _validate_engine_manifest(
        engine_manifest,
        gp_root=gp_root,
        protocol_root=protocol_root,
    )
    engine_generation = str(engine_manifest["generation"])
    identity = {
        "schema": RELEASE_SCHEMA,
        "wire_version": 3,
        "transport_generation": transport_gen,
        "transport_payload_generation": transport_payload_gen,
        "protocol_generation": protocol_gen,
        "control_generation": control_gen,
        "agent_runner_generation": agent_runner_gen,
        "official_eval_generation": official_eval_gen,
        "daemon_generation": daemon_gen,
        "windows_bootstrap_generation": windows_bootstrap_gen,
        "release_tooling_generation": release_tooling_gen,
        "official_eval_policy_generation": official_eval_policy_gen,
        "control_database_schema": SCHEMA_VERSION,
        "engine_code_generation": engine_generation,
        "variable_registry_sha256": _file_digest(variable_registry),
        "change_impact_policy_sha256": _file_digest(change_impact_policy),
        "workflow_adapter_path": workflow_adapter.relative_to(workspace_root).as_posix(),
        "workflow_adapter_sha256": _file_digest(workflow_adapter),
        "daemon_config_path": daemon_config.relative_to(workspace_root).as_posix(),
        "daemon_config_sha256": _file_digest(daemon_config),
        "developer_runbook_path": developer_runbook.relative_to(
            workspace_root
        ).as_posix(),
        "developer_runbook_sha256": _file_digest(developer_runbook),
        "system_registry_path": system_registry.relative_to(workspace_root).as_posix(),
        "system_registry_sha256": _file_digest(system_registry),
        "official_eval_config_path": official_eval_config.relative_to(
            workspace_root
        ).as_posix(),
        "official_eval_config_sha256": _file_digest(official_eval_config),
        "official_eval_policies": [
            {
                "campaign_id": entry["campaign_id"],
                "archive_path": entry["archive_path"],
                "sha256": entry["sha256"],
            }
            for entry in official_eval_policies
        ],
    }
    release_generation = _object_digest(identity)
    release_root = output_root / release_generation
    release_root.mkdir(parents=True, exist_ok=True)
    transport_archive = release_root / "gitpartner-runtime.tar.gz"
    protocol_archive = release_root / "ascendop-protocol.tar.gz"
    control_archive = release_root / "ascendop-control.tar.gz"
    agent_runner_archive = release_root / "ascendop-agent-runner.tar.gz"
    official_eval_archive = release_root / "official-eval-daemon.tar.gz"
    official_eval_policy_archive = release_root / "official-eval-policies.tar.gz"
    daemon_archive = release_root / "ascendop-daemon.tar.gz"
    windows_bootstrap_archive = release_root / "windows-bootstrap.tar.gz"
    engine_archive = release_root / "engine-runtime.tar.gz"
    variable_registry_artifact = release_root / "variables.json"
    change_impact_policy_artifact = release_root / "change-impact-policy.json"
    daemon_config_artifact = release_root / "daemon-config.json"
    developer_runbook_artifact = release_root / "developer-capability-runbook.md"
    system_registry_artifact = release_root / "system-registry.json"
    official_eval_config_artifact = release_root / "official-eval-config.json"
    official_eval_policy_index = release_root / "official-eval-policy-index.json"
    workflow_adapter_artifact = release_root / "next-workflow.py"
    component_build_trace: list[dict[str, object]] = []
    _materialize_component_archive(
        component_id="transport",
        generation=transport_payload_gen,
        destination=transport_archive,
        cache_root=component_cache_root,
        builder=lambda path: _write_archive(gp_root, transport_files, path),
        trace=component_build_trace,
    )
    _materialize_component_archive(
        component_id="protocol",
        generation=protocol_gen,
        destination=protocol_archive,
        cache_root=component_cache_root,
        builder=lambda path: _write_archive(protocol_root, protocol_files, path),
        trace=component_build_trace,
    )
    _materialize_component_archive(
        component_id="control",
        generation=control_gen,
        destination=control_archive,
        cache_root=component_cache_root,
        builder=lambda path: _write_archive(control_root, control_files, path),
        trace=component_build_trace,
    )
    _materialize_component_archive(
        component_id="agent-runner",
        generation=agent_runner_gen,
        destination=agent_runner_archive,
        cache_root=component_cache_root,
        builder=lambda path: _write_archive(agent_runner_root, agent_runner_files, path),
        trace=component_build_trace,
    )
    _materialize_component_archive(
        component_id="official-eval",
        generation=official_eval_gen,
        destination=official_eval_archive,
        cache_root=component_cache_root,
        builder=lambda path: _write_mapped_archive(official_eval_files, path),
        trace=component_build_trace,
    )
    _materialize_component_archive(
        component_id="official-eval-policies",
        generation=official_eval_policy_gen,
        destination=official_eval_policy_archive,
        cache_root=component_cache_root,
        builder=lambda path: _write_mapped_archive(official_eval_policy_files, path),
        trace=component_build_trace,
    )
    _materialize_component_archive(
        component_id="daemon",
        generation=daemon_gen,
        destination=daemon_archive,
        cache_root=component_cache_root,
        builder=lambda path: _write_archive(daemon_root, daemon_files, path),
        trace=component_build_trace,
    )
    _materialize_component_archive(
        component_id="windows-bootstrap",
        generation=windows_bootstrap_gen,
        destination=windows_bootstrap_archive,
        cache_root=component_cache_root,
        builder=lambda path: _write_archive(
            daemon_root, windows_bootstrap_files, path
        ),
        trace=component_build_trace,
    )
    _materialize_component_archive(
        component_id="engine",
        generation=engine_generation,
        destination=engine_archive,
        cache_root=component_cache_root,
        builder=lambda path: _write_engine_archive(
            gp_root=gp_root,
            protocol_root=protocol_root,
            files=engine_files,
            manifest=engine_manifest,
            destination=path,
        ),
        trace=component_build_trace,
    )
    _write_file_atomic(variable_registry, variable_registry_artifact)
    _write_file_atomic(change_impact_policy, change_impact_policy_artifact)
    _write_file_atomic(daemon_config, daemon_config_artifact)
    _write_file_atomic(developer_runbook, developer_runbook_artifact)
    _write_file_atomic(system_registry, system_registry_artifact)
    _write_file_atomic(official_eval_config, official_eval_config_artifact)
    _write_json_atomic(
        official_eval_policy_index,
        {
            "schema": "ascendop.official-eval-policy-index.v1",
            "policies": [
                {
                    "campaign_id": entry["campaign_id"],
                    "archive_path": entry["archive_path"],
                    "sha256": entry["sha256"],
                }
                for entry in official_eval_policies
            ],
        },
    )
    _write_file_atomic(workflow_adapter, workflow_adapter_artifact)
    manifest = {
        **identity,
        "release_generation": release_generation,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "archives": {
            "transport": {
                "path": transport_archive.name,
                "sha256": _file_digest(transport_archive),
            },
            "protocol": {
                "path": protocol_archive.name,
                "sha256": _file_digest(protocol_archive),
            },
            "control": {
                "path": control_archive.name,
                "sha256": _file_digest(control_archive),
            },
            "agent_runner": {
                "path": agent_runner_archive.name,
                "sha256": _file_digest(agent_runner_archive),
            },
            "official_eval": {
                "path": official_eval_archive.name,
                "sha256": _file_digest(official_eval_archive),
            },
            "official_eval_policies": {
                "path": official_eval_policy_archive.name,
                "sha256": _file_digest(official_eval_policy_archive),
            },
            "daemon": {
                "path": daemon_archive.name,
                "sha256": _file_digest(daemon_archive),
            },
            "windows_bootstrap": {
                "path": windows_bootstrap_archive.name,
                "sha256": _file_digest(windows_bootstrap_archive),
            },
            "engine": {
                "path": engine_archive.name,
                "sha256": _file_digest(engine_archive),
            },
            "variable_registry": {
                "path": variable_registry_artifact.name,
                "sha256": _file_digest(variable_registry_artifact),
            },
            "change_impact_policy": {
                "path": change_impact_policy_artifact.name,
                "sha256": _file_digest(change_impact_policy_artifact),
            },
            "daemon_config": {
                "path": daemon_config_artifact.name,
                "sha256": _file_digest(daemon_config_artifact),
            },
            "developer_runbook": {
                "path": developer_runbook_artifact.name,
                "sha256": _file_digest(developer_runbook_artifact),
            },
            "system_registry": {
                "path": system_registry_artifact.name,
                "sha256": _file_digest(system_registry_artifact),
            },
            "official_eval_config": {
                "path": official_eval_config_artifact.name,
                "sha256": _file_digest(official_eval_config_artifact),
            },
            "official_eval_policy_index": {
                "path": official_eval_policy_index.name,
                "sha256": _file_digest(official_eval_policy_index),
            },
            "workflow_adapter": {
                "path": workflow_adapter_artifact.name,
                "sha256": _file_digest(workflow_adapter_artifact),
            },
        },
        "component_build_trace": component_build_trace,
    }
    manifest_path = release_root / "RELEASE.json"
    _write_json_atomic(manifest_path, manifest)
    return {**manifest, "manifest_path": str(manifest_path)}


def _configured_developer_runbook(
    *,
    workspace_root: Path,
    daemon_config: Path,
) -> Path:
    try:
        config = json.loads(daemon_config.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ReleaseBundleError("daemon config is not valid JSON") from exc
    policy = config.get("policy") if isinstance(config, dict) else None
    value = policy.get("flow_v5_developer_runbook_path") if isinstance(policy, dict) else None
    if not isinstance(value, str) or not value.strip():
        raise ReleaseBundleError(
            "daemon config does not define flow_v5_developer_runbook_path"
        )
    relative = Path(value.strip())
    if relative.is_absolute():
        raise ReleaseBundleError("Developer runbook path must be workspace-relative")
    runbook = (workspace_root / relative).resolve()
    _require_child(workspace_root, runbook, "Developer runbook")
    if not runbook.is_file() or runbook.is_symlink():
        raise ReleaseBundleError(
            f"Developer runbook is missing or unsafe: {runbook}"
        )
    return runbook


def _transport_files(root: Path) -> tuple[Path, ...]:
    selected = [root / "pyproject.toml", root / ".gitattributes"]
    for relative in ("src/limited_remote_partner", "scripts", "services", "configs"):
        selected.extend(_regular_files(root / relative))
    return _unique_existing(root, selected)


def _validate_registered_endpoint_configs(
    *,
    gp_root: Path,
    system_registry: Path,
) -> None:
    try:
        SystemRegistry.load(system_registry)
    except SystemRegistryError as exc:
        raise ReleaseBundleError(
            f"system registry topology is invalid: {exc}"
        ) from exc
    try:
        registry = json.loads(system_registry.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ReleaseBundleError("system registry is not valid JSON") from exc
    routes = registry.get("route_endpoints")
    if not isinstance(routes, list) or not routes:
        raise ReleaseBundleError("system registry has no route endpoints")
    for route in routes:
        if not isinstance(route, dict):
            raise ReleaseBundleError("system registry endpoint must be an object")
        endpoint_id = str(route.get("endpoint_id") or "<unknown>")
        relative = str(route.get("node_gitpartner_config") or "")
        prefix = "GitPartner/"
        if not relative.startswith(prefix):
            raise ReleaseBundleError(
                f"endpoint config is outside the GitPartner product: {endpoint_id}"
            )
        candidate = (gp_root / relative[len(prefix) :]).resolve()
        _require_child(gp_root, candidate, f"endpoint config for {endpoint_id}")
        if not candidate.is_file() or candidate.is_symlink():
            raise ReleaseBundleError(
                f"registered endpoint config is missing or unsafe: {endpoint_id}"
            )


def _validate_change_impact_policy(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ReleaseBundleError("change-impact policy is not valid JSON") from exc
    if value.get("schema") != "ascendop.change-impact-policy.v1":
        raise ReleaseBundleError("unsupported change-impact policy schema")
    components = value.get("components")
    if not isinstance(components, list) or not components:
        raise ReleaseBundleError("change-impact policy has no components")
    try:
        daemon_offline_patterns(value)
    except ValueError as exc:
        raise ReleaseBundleError(str(exc)) from exc
    if any(
        not isinstance(component, dict)
        or not str(component.get("import_domain") or "")
        for component in components
    ):
        raise ReleaseBundleError("change-impact component has no import domain")
    return value


def _protocol_files(root: Path) -> Iterable[Path]:
    yield root / "pyproject.toml"
    yield from _regular_files(root / "src" / "ascendop_protocol")


def _python_product_files(root: Path, package: str) -> Iterable[Path]:
    yield root / "pyproject.toml"
    yield from _regular_files(root / "src" / package)


def _official_eval_product_files(root: Path) -> Iterable[tuple[str, Path]]:
    yield "pyproject.toml", root / "pyproject.toml"
    package = root / "official_eval"
    for path in _regular_files(package):
        relative = path.relative_to(package).as_posix()
        yield f"src/official_eval/{relative}", path


def _official_eval_policy_entries(
    *,
    workspace_root: Path,
    config: Path,
) -> tuple[dict[str, Any], ...]:
    try:
        raw = json.loads(config.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ReleaseBundleError("official-eval config is not valid JSON") from exc
    if raw.get("schema") != "ascendop.official-eval.config.v1":
        raise ReleaseBundleError("unsupported official-eval config schema")
    campaigns = raw.get("campaigns")
    if not isinstance(campaigns, list) or not campaigns:
        raise ReleaseBundleError("official-eval config has no campaigns")
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for campaign in campaigns:
        if not isinstance(campaign, dict):
            raise ReleaseBundleError("official-eval campaign must be an object")
        campaign_id = str(campaign.get("campaign_id") or "").strip()
        if not campaign_id or campaign_id in seen:
            raise ReleaseBundleError(
                f"invalid or duplicate official-eval campaign: {campaign_id!r}"
            )
        seen.add(campaign_id)
        policy_text = str(campaign.get("standing_policy_path") or "").strip()
        if not policy_text:
            if bool(campaign.get("enabled")):
                raise ReleaseBundleError(
                    f"enabled official-eval campaign has no standing policy: {campaign_id}"
                )
            continue
        policy = Path(policy_text)
        if not policy.is_absolute():
            policy = (config.parent / policy).resolve()
        else:
            policy = policy.resolve()
        _require_child(workspace_root, policy, f"official-eval policy for {campaign_id}")
        if not policy.is_file() or policy.is_symlink():
            raise ReleaseBundleError(
                f"official-eval policy is missing or unsafe: {campaign_id}"
            )
        digest = _file_digest(policy)
        result.append(
            {
                "campaign_id": campaign_id,
                "archive_path": f"p/{digest[:16]}.json",
                "sha256": digest,
                "path": policy,
            }
        )
    return tuple(result)


def _unique_policy_archive_files(
    entries: Iterable[dict[str, Any]],
) -> tuple[tuple[str, Path], ...]:
    result: dict[str, Path] = {}
    for entry in entries:
        archive_path = str(entry["archive_path"])
        path = Path(entry["path"])
        previous = result.get(archive_path)
        if previous is not None and previous != path:
            raise ReleaseBundleError(
                f"official-eval policy archive collision: {archive_path}"
            )
        result[archive_path] = path
    return tuple(sorted(result.items()))


def _daemon_files(
    root: Path,
    offline_patterns: Iterable[str] = (),
) -> Iterable[Path]:
    yield root / "pyproject.toml"
    yield root / "daemon.py"
    yield root / "launch_s5_910b.py"
    yield root / "launcher_bootstrap.py"
    yield root / "manage_s5_910b.ps1"
    for path in _regular_files(root / "src" / "ascendop_daemon"):
        relative = path.relative_to(root).as_posix()
        if not daemon_file_is_offline(relative, offline_patterns):
            yield path


def _windows_bootstrap_files(root: Path) -> Iterable[Path]:
    candidates = (
        root / "launch_s5_910b.py",
        root / "launcher_bootstrap.py",
        root / "src" / "ascendop_daemon" / "cli" / "resident_main.py",
        root / "src" / "ascendop_daemon" / "core" / "atomic_io.py",
        root / "src" / "ascendop_daemon" / "core" / "windows_privilege.py",
        root / "src" / "ascendop_daemon" / "runtime" / "locking.py",
        root / "src" / "ascendop_daemon" / "runtime" / "process_identity.py",
        root / "src" / "ascendop_daemon" / "runtime" / "resident_service.py",
        root / "src" / "ascendop_daemon" / "runtime" / "resident_watchdog.py",
        root / "src" / "ascendop_daemon" / "runtime" / "windows_bootstrap.py",
    )
    selected = [path for path in candidates if path.is_file() and not path.is_symlink()]
    if not selected:
        raise ReleaseBundleError("Windows bootstrap component has no files")
    return tuple(selected)


def _release_tooling_files(
    workspace_root: Path,
    daemon_root: Path,
) -> Iterable[tuple[str, Path]]:
    candidates = (
        daemon_root
        / "src"
        / "ascendop_daemon"
        / "exchange"
        / "release_bundle.py",
        daemon_root
        / "src"
        / "ascendop_daemon"
        / "exchange"
        / "daemon_installer.py",
        daemon_root / "src" / "ascendop_daemon" / "exchange" / "installer_validation.py",
        daemon_root / "src" / "ascendop_daemon" / "exchange" / "release_layout.py",
        daemon_root
        / "src"
        / "ascendop_daemon"
        / "exchange"
        / "transport_installer.py",
        workspace_root / "scripts" / "install_v4_release.ps1",
        workspace_root / "scripts" / "install_v4_windows_bootstrap.ps1",
        workspace_root / "scripts" / "deploy_v4_endpoint.ps1",
        workspace_root / "scripts" / "next_workflow.py",
        workspace_root / "docs" / "flow_v4" / "change_impact_policy.json",
    )
    selected = [path for path in candidates if path.is_file() and not path.is_symlink()]
    if not selected:
        raise ReleaseBundleError("release tooling component has no files")
    return tuple(
        (path.relative_to(workspace_root).as_posix(), path)
        for path in selected
    )


def _regular_files(root: Path) -> list[Path]:
    if not root.is_dir():
        return []
    return [
        path
        for path in sorted(root.rglob("*"), key=lambda item: item.as_posix())
        if path.is_file()
        and not path.is_symlink()
        and "__pycache__" not in path.parts
        and path.suffix not in {".pyc", ".pyo"}
    ]


def _unique_existing(root: Path, paths: Iterable[Path]) -> tuple[Path, ...]:
    result: list[Path] = []
    seen: set[str] = set()
    for path in paths:
        resolved = path.resolve()
        _require_child(root, resolved, "release file")
        if not resolved.is_file() or resolved.is_symlink():
            raise ReleaseBundleError(f"release file is missing or unsafe: {resolved}")
        relative = resolved.relative_to(root).as_posix()
        if relative not in seen:
            seen.add(relative)
            result.append(resolved)
    return tuple(sorted(result, key=lambda item: item.relative_to(root).as_posix()))


def _tree_generation(root: Path, paths: Iterable[Path]) -> str:
    digest = hashlib.sha256()
    for path in paths:
        relative = path.relative_to(root).as_posix().encode("utf-8")
        payload = path.read_bytes().replace(b"\r\n", b"\n")
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def _mapped_tree_generation(paths: Iterable[tuple[str, Path]]) -> str:
    digest = hashlib.sha256()
    for relative, path in sorted(paths, key=lambda item: item[0]):
        payload = path.read_bytes().replace(b"\r\n", b"\n")
        encoded = relative.encode("utf-8")
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def _write_archive(root: Path, paths: Iterable[Path], destination: Path) -> None:
    _write_mapped_archive(
        ((path.relative_to(root).as_posix(), path) for path in paths),
        destination,
    )


def _materialize_component_archive(
    *,
    component_id: str,
    generation: str,
    destination: Path,
    cache_root: Path,
    builder: Callable[[Path], None],
    trace: list[dict[str, object]],
) -> None:
    safe_component = component_id.replace("/", "-").replace("\\", "-")
    cache_dir = cache_root / safe_component / generation[:20]
    cached = cache_dir / destination.name
    receipt = cache_dir / "artifact.json"
    action = "reused-cache"
    expected = _read_component_receipt(receipt)
    if not _cached_artifact_valid(cached, expected, generation=generation):
        cache_dir.mkdir(parents=True, exist_ok=True)
        builder(cached)
        expected = {
            "schema": "ascendop.component-artifact.v1",
            "component_id": component_id,
            "generation": generation,
            "path": cached.name,
            "sha256": _file_digest(cached),
            "size": cached.stat().st_size,
        }
        _write_json_atomic(receipt, expected)
        action = "built"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination_digest = _file_digest(destination) if destination.is_file() else ""
    if destination_digest != expected["sha256"]:
        _copy_file_atomic(cached, destination)
    trace.append(
        {
            "component_id": component_id,
            "generation": generation,
            "action": action,
            "sha256": str(expected["sha256"]),
            "size": int(expected["size"]),
        }
    )


def _cached_artifact_valid(
    path: Path,
    receipt: dict[str, object],
    *,
    generation: str,
) -> bool:
    if not path.is_file() or not receipt:
        return False
    try:
        return (
            str(receipt.get("generation") or "") == generation
            and
            int(receipt.get("size") or -1) == path.stat().st_size
            and str(receipt.get("sha256") or "") == _file_digest(path)
        )
    except OSError:
        return False


def _read_component_receipt(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _copy_file_atomic(source: Path, destination: Path) -> None:
    handle, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent
    )
    os.close(handle)
    temporary = Path(temporary_name)
    try:
        temporary.unlink(missing_ok=True)
        try:
            os.link(source, temporary)
        except OSError:
            shutil.copyfile(source, temporary)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def _write_mapped_archive(
    paths: Iterable[tuple[str, Path]], destination: Path
) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent
    )
    os.close(handle)
    temporary = Path(temporary_name)
    try:
        with temporary.open("wb") as raw:
            with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
                with tarfile.open(
                    fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT
                ) as archive:
                    for relative, path in sorted(paths, key=lambda item: item[0]):
                        info = archive.gettarinfo(str(path), arcname=relative)
                        info.uid = 0
                        info.gid = 0
                        info.uname = ""
                        info.gname = ""
                        info.mtime = 0
                        with path.open("rb") as source:
                            archive.addfile(info, source)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def _require_child(root: Path, path: Path, label: str) -> None:
    root = root.resolve()
    path = path.resolve()
    if path != root and root not in path.parents:
        raise ReleaseBundleError(f"{label} escapes root: {path}")


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _object_digest(value: dict[str, object]) -> str:
    payload = json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _write_json_atomic(path: Path, value: dict[str, object]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _write_file_atomic(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    temporary.write_bytes(source.read_bytes())
    os.replace(temporary, destination)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build an immutable Flow V4 release")
    parser.add_argument("--workspace-root", type=Path, required=True)
    parser.add_argument("--gp-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--daemon-config", type=Path)
    parser.add_argument("--system-registry", type=Path)
    args = parser.parse_args(argv)
    try:
        result = build_flow_release(
            workspace_root=args.workspace_root,
            gp_root=args.gp_root,
            output_root=args.output_root,
            daemon_config=args.daemon_config,
            system_registry=args.system_registry,
        )
    except ReleaseBundleError as exc:
        print(str(exc), file=os.sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
