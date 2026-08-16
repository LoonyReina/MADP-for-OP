from __future__ import annotations

import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Sequence


RELEASE_SCHEMA = "ascendop.flow-release.v4"
RELAY_RECEIPT_SCHEMA = "ascendop.relay-endpoint-deployment-receipt.v4"


class RelayEndpointDeployError(RuntimeError):
    pass


@dataclass(frozen=True)
class RelayDeploymentTarget:
    endpoint_id: str
    environment_id: str
    gateway_id: str
    registration_generation: str
    remote_root: str
    engine_root_relative: str
    control_repo: Path
    result_repo: Path
    endpoint_config_relative: str
    target_repo_relative: str
    runtime_root_relative: str
    drain_timeout_seconds: int


Runner = Callable[[Sequence[str], int], Any]


def load_relay_deployment_target(
    manifest_path: Path,
    endpoint_id: str,
) -> tuple[dict[str, Any], RelayDeploymentTarget]:
    manifest_path = manifest_path.resolve()
    release = _read_object(manifest_path)
    if release.get("schema") != RELEASE_SCHEMA:
        raise RelayEndpointDeployError(
            f"unsupported release schema: {release.get('schema')!r}"
        )
    registry_entry = _archive_entry(release, "system_registry")
    registry = _read_object(manifest_path.parent / registry_entry["path"])
    routes = registry.get("route_endpoints")
    environments = registry.get("execution_environments")
    if not isinstance(routes, list) or not isinstance(environments, list):
        raise RelayEndpointDeployError(
            "release registry has no endpoint topology"
        )
    route = next(
        (
            row
            for row in routes
            if isinstance(row, dict) and row.get("endpoint_id") == endpoint_id
        ),
        None,
    )
    if route is None:
        raise RelayEndpointDeployError(
            f"endpoint is absent from release: {endpoint_id}"
        )
    transport = route.get("transport_binding")
    if not isinstance(transport, dict) or transport.get("mode") != "lan-relay":
        raise RelayEndpointDeployError(
            f"endpoint has no lan-relay transport binding: {endpoint_id}"
        )
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
        raise RelayEndpointDeployError(
            f"endpoint environment is absent from release: {environment_id}"
        )
    remote_root = _safe_absolute(environment.get("remote_root"), "remote_root")
    engine_value = str(environment.get("engine_root") or "test_engine_demo")
    engine_path = PurePosixPath(engine_value)
    if engine_path.is_absolute():
        try:
            engine_value = str(engine_path.relative_to(PurePosixPath(remote_root)))
        except ValueError as exc:
            raise RelayEndpointDeployError(
                "relay Engine root must stay inside the endpoint remote root"
            ) from exc
    engine_relative = _safe_relative(engine_value, "engine_root")
    node_config = _safe_relative(
        route.get("node_gitpartner_config"), "node_gitpartner_config"
    )
    prefix = "GitPartner/"
    if not node_config.startswith(prefix):
        raise RelayEndpointDeployError(
            "node_gitpartner_config must be rooted in canonical GitPartner"
        )

    management = route.get("management_binding")
    if management is None:
        management = {}
    if not isinstance(management, dict) or management.get(
        "mode", "lan-relay"
    ) not in {"lan-relay", "relay-maintenance"}:
        raise RelayEndpointDeployError(
            f"invalid relay maintenance binding: {endpoint_id}"
        )
    gateway_id = str(
        management.get("gateway_id")
        or transport.get("gateway_id")
        or ""
    )
    if not gateway_id:
        raise RelayEndpointDeployError(
            f"relay endpoint has no gateway identity: {endpoint_id}"
        )
    workspace_root = _workspace_root(
        manifest_path,
        str(route.get("gitpartner_repo") or ""),
        str(route.get("result_worktree") or ""),
    )
    control_repo = _workspace_path(
        workspace_root,
        route.get("gitpartner_repo"),
        "gitpartner_repo",
    )
    result_repo = _workspace_path(
        workspace_root,
        route.get("result_worktree"),
        "result_worktree",
    )
    if not (control_repo / "src" / "limited_remote_partner").is_dir():
        raise RelayEndpointDeployError(
            f"relay control repository is incomplete: {control_repo}"
        )
    if not result_repo.is_dir():
        raise RelayEndpointDeployError(
            f"relay result worktree is missing: {result_repo}"
        )
    generation = str(route.get("generation") or "")
    if not generation:
        raise RelayEndpointDeployError(
            f"relay endpoint registration generation is absent: {endpoint_id}"
        )
    drain_timeout = int(management.get("drain_timeout_seconds", 90) or 90)
    if not 1 <= drain_timeout <= 300:
        raise RelayEndpointDeployError(
            "relay drain_timeout_seconds must be between 1 and 300"
        )
    return release, RelayDeploymentTarget(
        endpoint_id=endpoint_id,
        environment_id=environment_id,
        gateway_id=gateway_id,
        registration_generation=generation,
        remote_root=remote_root,
        engine_root_relative=engine_relative,
        control_repo=control_repo,
        result_repo=result_repo,
        endpoint_config_relative=node_config[len(prefix) :],
        target_repo_relative=_safe_relative(
            management.get("target_repo_relative", "ascend-git-partner"),
            "target_repo_relative",
        ),
        runtime_root_relative=_safe_relative(
            management.get("runtime_root_relative", "runtime"),
            "runtime_root_relative",
        ),
        drain_timeout_seconds=drain_timeout,
    )


def deploy_relay_endpoint_release(
    *,
    manifest_path: Path,
    endpoint_id: str,
    receipt_path: Path,
    wait_timeout_seconds: int = 420,
    bootstrap_settle_seconds: float = 5.0,
    runner: Runner,
) -> dict[str, Any]:
    manifest_path = manifest_path.resolve()
    release, target = load_relay_deployment_target(
        manifest_path, endpoint_id
    )
    generation = str(release.get("release_generation") or "")
    if not _is_digest(generation, 64):
        raise RelayEndpointDeployError(
            f"invalid release generation: {generation!r}"
        )
    bootstrap_id = f"v4-gateway-bootstrap-{endpoint_id}-{generation[:16]}"
    generation_key = generation[:16]
    bootstrap_output = f"m/b/{generation_key}/{endpoint_id}"
    bootstrap = _submit(
        runner=runner,
        target=target,
        timeout_seconds=wait_timeout_seconds,
        arguments=[
            "--publish-maintenance-changes",
            "lan-bootstrap",
            "--request-id",
            bootstrap_id,
            "--output-subdir",
            bootstrap_output,
            "--action",
            "lan-bootstrap",
            "--target-role",
            "client",
            "--target-endpoint-id",
            endpoint_id,
            "--target-environment-id",
            target.environment_id,
            "--target-gateway-id",
            target.gateway_id,
            "--target-transport-mode",
            "lan-relay",
            "--registration-generation",
            target.registration_generation,
        ],
        request_id=bootstrap_id,
    )
    bootstrap_status = _read_terminal_status(
        target.result_repo, bootstrap_output
    )
    if bootstrap_status.get("state") != "success":
        raise RelayEndpointDeployError(
            "relay gateway/client bootstrap did not reach success"
        )

    # Auto-update reexec is asynchronous after the maintenance result commits.
    # A bounded delay lets the exact published source generation become the
    # request parser before the new typed action is introduced.
    time.sleep(max(0.0, float(bootstrap_settle_seconds)))
    request_id = f"v4-release-sync-{endpoint_id}-{generation[:16]}"
    output_subdir = f"m/r/{generation_key}/{endpoint_id}"
    release_submit = _submit(
        runner=runner,
        target=target,
        timeout_seconds=wait_timeout_seconds,
        arguments=[
            "lan-bootstrap",
            "--request-id",
            request_id,
            "--output-subdir",
            output_subdir,
            "--client-work-dir",
            target.remote_root,
            "--engine-root",
            target.engine_root_relative,
            "--action",
            "lan-release-sync",
            "--target-role",
            "client",
            "--target-dir",
            target.remote_root,
            "--release-manifest",
            str(manifest_path),
            "--target-repo-relative",
            target.target_repo_relative,
            "--endpoint-config-relative",
            target.endpoint_config_relative,
            "--runtime-root-relative",
            target.runtime_root_relative,
            "--drain-timeout-seconds",
            str(target.drain_timeout_seconds),
            "--target-endpoint-id",
            endpoint_id,
            "--target-environment-id",
            target.environment_id,
            "--target-gateway-id",
            target.gateway_id,
            "--target-transport-mode",
            "lan-relay",
            "--registration-generation",
            target.registration_generation,
        ],
        request_id=request_id,
    )
    status = _read_terminal_status(target.result_repo, output_subdir)
    remote_receipt = _release_receipt(status)
    expected = {
        "release_generation": generation,
        "transport_generation": str(release["transport_generation"]),
        "protocol_generation": str(release["protocol_generation"]),
        "engine_code_generation": str(release["engine_code_generation"]),
    }
    if any(remote_receipt.get(name) != value for name, value in expected.items()):
        raise RelayEndpointDeployError(
            "relay deployment receipt generation mismatch"
        )
    receipt = {
        "schema": "ascendop.endpoint-deployment-receipt.v4",
        "deployment_mode": "lan-relay-maintenance",
        "endpoint_id": endpoint_id,
        **expected,
        "gateway_id": target.gateway_id,
        "registration_generation": target.registration_generation,
        "bootstrap_request_id": bootstrap_id,
        "release_request_id": request_id,
        "bootstrap_timeline": _bounded_timeline(bootstrap),
        "release_timeline": _bounded_timeline(release_submit),
        "remote_receipt": remote_receipt,
    }
    _write_json_atomic(receipt_path.resolve(), receipt)
    return receipt


def _submit(
    *,
    runner: Runner,
    target: RelayDeploymentTarget,
    timeout_seconds: int,
    arguments: list[str],
    request_id: str,
) -> dict[str, Any]:
    command = [
        sys.executable,
        "-m",
        "limited_remote_partner.gateway.submit_job",
        "--repo",
        str(target.control_repo),
        "--result-repo",
        str(target.result_repo),
        "--append-request",
        "--commit-push",
        "--wait",
        "--wait-timeout-seconds",
        str(timeout_seconds),
        "--message",
        f"ascendop: {request_id}",
        *arguments,
    ]
    completed = runner(command, timeout_seconds + 90)
    if completed.returncode != 0:
        detail = "\n".join(
            text.strip()
            for text in (completed.stdout, completed.stderr)
            if text and text.strip()
        )
        raise RelayEndpointDeployError(
            f"trusted relay maintenance request failed rc={completed.returncode}: "
            f"{detail[-3000:]}"
        )
    marker = "GITPARTNER_LOCAL_TIMELINE:"
    index = completed.stdout.rfind(marker)
    if index < 0:
        raise RelayEndpointDeployError(
            "trusted relay maintenance request returned no local timeline"
        )
    line = completed.stdout[index + len(marker) :].splitlines()[0]
    try:
        timeline = json.loads(line)
    except json.JSONDecodeError as exc:
        raise RelayEndpointDeployError(
            "trusted relay maintenance timeline is invalid"
        ) from exc
    if not isinstance(timeline, dict) or timeline.get("request_id") != request_id:
        raise RelayEndpointDeployError(
            "trusted relay maintenance timeline identity mismatch"
        )
    return timeline


def _read_terminal_status(result_repo: Path, output_subdir: str) -> dict[str, Any]:
    status = _read_object(result_repo / "output" / output_subdir / "status.json")
    if status.get("state") not in {"success", "failed"}:
        raise RelayEndpointDeployError(
            f"relay maintenance result is not terminal: {status.get('state')!r}"
        )
    return status


def _release_receipt(status: dict[str, Any]) -> dict[str, Any]:
    if status.get("state") != "success":
        raise RelayEndpointDeployError(
            f"relay release deployment failed: {status.get('error')!r}"
        )
    action_log = status.get("action_log")
    if not isinstance(action_log, list):
        raise RelayEndpointDeployError(
            "relay release result has no action log"
        )
    matches = [
        row
        for row in action_log
        if isinstance(row, dict) and row.get("schema") == RELAY_RECEIPT_SCHEMA
    ]
    if len(matches) != 1 or matches[0].get("state") != "success":
        raise RelayEndpointDeployError(
            "relay release result has no unique successful deployment receipt"
        )
    return matches[0]


def _workspace_root(
    manifest_path: Path,
    control_relative: str,
    result_relative: str,
) -> Path:
    for candidate in manifest_path.parents:
        if (
            control_relative
            and result_relative
            and (candidate / control_relative).is_dir()
            and (candidate / result_relative).is_dir()
        ):
            return candidate
    raise RelayEndpointDeployError(
        "cannot locate relay control/result worktrees for release"
    )


def _workspace_path(root: Path, value: object, field: str) -> Path:
    relative = _safe_relative(value, field)
    path = (root / relative).resolve()
    if root.resolve() not in path.parents:
        raise RelayEndpointDeployError(f"unsafe {field}: {relative!r}")
    return path


def _archive_entry(release: dict[str, Any], name: str) -> dict[str, str]:
    archives = release.get("archives")
    entry = archives.get(name) if isinstance(archives, dict) else None
    if not isinstance(entry, dict):
        raise RelayEndpointDeployError(f"release archive is absent: {name}")
    path = _safe_relative(entry.get("path"), f"release archive {name}")
    digest = str(entry.get("sha256") or "")
    if not _is_digest(digest, 64):
        raise RelayEndpointDeployError(
            f"release archive digest is invalid: {name}"
        )
    return {"path": path, "sha256": digest}


def _safe_absolute(value: object, field: str) -> str:
    text = str(value or "")
    path = PurePosixPath(text)
    if (
        not path.is_absolute()
        or ".." in path.parts
        or any(
            character
            not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-/"
            for character in text
        )
    ):
        raise RelayEndpointDeployError(f"unsafe {field}: {text!r}")
    return text.rstrip("/") or "/"


def _safe_relative(value: object, field: str) -> str:
    text = str(value or "").replace("\\", "/")
    path = PurePosixPath(text)
    if (
        not text
        or path.is_absolute()
        or ".." in path.parts
        or any(
            character
            not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-/"
            for character in text
        )
    ):
        raise RelayEndpointDeployError(f"unsafe {field}: {text!r}")
    return text


def _is_digest(value: str, length: int) -> bool:
    return len(value) == length and all(
        character in "0123456789abcdef" for character in value
    )


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RelayEndpointDeployError(f"cannot read JSON object: {path}") from exc
    if not isinstance(value, dict):
        raise RelayEndpointDeployError(f"JSON document must be an object: {path}")
    return value


def _bounded_timeline(timeline: dict[str, Any]) -> dict[str, Any]:
    return {
        "request_id": str(timeline.get("request_id") or ""),
        "total_seconds": float(timeline.get("total_seconds", 0.0) or 0.0),
        "finished_at": str(timeline.get("finished_at") or ""),
    }


def _write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{time.time_ns()}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)
