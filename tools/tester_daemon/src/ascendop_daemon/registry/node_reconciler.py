from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Iterable

from ascendop_daemon.control_plane.control_database import ControlDatabase, ControlDatabaseError
from ascendop_daemon.exchange.node_control_transport import NodeControlTransport
from ascendop_daemon.exchange.runtime_source import (
    apply_transport_runtime_environment,
    load_active_transport_runtime,
)
from ascendop_daemon.runtime.process_adapter import process_creation_flags, process_startupinfo
from ascendop_daemon.runtime.locking import NamedProcessLock
from ascendop_daemon.core.models import utc_now_iso
from ascendop_daemon.registry.system_registry import SystemRegistry


def query_gitpartner_node_report(
    *,
    root: Path,
    gitpartner_repo: str,
    result_worktree: str,
    report_branch: str,
    control_branch: str,
    node_id: str,
    package_source: Path | None = None,
    remote: str = "origin",
    command_timeout_seconds: int = 30,
) -> dict[str, Any]:
    repo_value = result_worktree or gitpartner_repo
    repo = Path(repo_value)
    if not repo.is_absolute():
        repo = root.resolve() / repo
    repo = repo.resolve()
    if not (repo / ".git").exists():
        raise ValueError(f"node-report GP worktree is missing: {repo}")
    runtime = load_active_transport_runtime(root)
    package_root = package_source.resolve() if package_source else runtime.source
    if not package_root.is_dir():
        raise ValueError(
            f"node-report GP package source is missing: {package_root}"
        )
    command = [
        sys.executable,
        "-m",
        "limited_remote_partner.gateway.batch_result_query",
        "--repo",
        str(repo),
        "--result-branch",
        report_branch,
        "--control-branch",
        control_branch,
        "--remote",
        remote,
        "--output-subdir",
        f"_control/nodes/{node_id}",
        "--result-template",
        "report.json",
    ]
    env = os.environ.copy()
    apply_transport_runtime_environment(env, runtime)
    if package_source is not None:
        env["PYTHONPATH"] = os.pathsep.join(
            (str(package_root), env.get("PYTHONPATH", ""))
        )
    completed = subprocess.run(
        command,
        cwd=repo,
        env=env,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=max(5, int(command_timeout_seconds)),
        creationflags=process_creation_flags(),
        startupinfo=process_startupinfo(),
    )
    if completed.returncode != 0:
        raise ValueError(
            "node-report query failed: "
            + (completed.stderr.strip() or completed.stdout.strip())
        )
    payload = _last_json_object(completed.stdout)
    items = payload.get("items")
    if not isinstance(items, list) or not items:
        raise ValueError("node-report query returned no items")
    item = items[0]
    report = item.get("result") if isinstance(item, dict) else None
    if not isinstance(report, dict):
        raise ValueError("node-report query returned no report")
    if str(report.get("node_id") or "") != node_id:
        raise ValueError("node-report identity mismatch")
    return report


class NodeReconciler:
    """Continuously reconcile GP node reports into durable control state."""

    def __init__(
        self,
        database: ControlDatabase,
        *,
        report_roots: Iterable[Path],
        ack_root: Path,
        pattern: str = "*/report.json",
    ) -> None:
        self.database = database
        self.report_roots = tuple(path.resolve() for path in report_roots)
        self.ack_root = ack_root.resolve()
        self.pattern = pattern

    def run_once(self) -> dict[str, Any]:
        reports = self._reports()
        ingested: list[dict[str, Any]] = []
        acknowledgements: list[dict[str, Any]] = []
        revoked_acknowledgements: list[dict[str, Any]] = []
        failures: list[dict[str, str]] = []
        for report_path in reports:
            try:
                report = json.loads(
                    report_path.read_text(encoding="utf-8-sig")
                )
                if not isinstance(report, dict):
                    raise ValueError("node report must be a JSON object")
                result = self.database.ingest_node_report(
                    report,
                    source=str(report_path),
                )
                ingested.append({"report": str(report_path), **result})
                ack = self.database.current_node_ack(str(report["node_id"]))
                ack_path = self.ack_root / str(report["node_id"]) / "ack.json"
                if ack is not None and ack["generation"] == report["generation"]:
                    changed = _write_json_atomic_if_changed(ack_path, ack)
                    acknowledgements.append(
                        {
                            "node_id": str(report["node_id"]),
                            "generation": str(report["generation"]),
                            "ack_path": str(ack_path),
                            "changed": changed,
                        }
                    )
                elif ack_path.exists():
                    ack_path.unlink()
                    revoked_acknowledgements.append(
                        {
                            "node_id": str(report["node_id"]),
                            "ack_path": str(ack_path),
                            "reason": "admission-or-generation-not-current",
                        }
                    )
            except (
                ControlDatabaseError,
                OSError,
                ValueError,
                json.JSONDecodeError,
            ) as exc:
                failures.append(
                    {
                        "report": str(report_path),
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
        return {
            "report_roots": [str(path) for path in self.report_roots],
            "pattern": self.pattern,
            "matched_count": len(reports),
            "ingested_count": len(ingested),
            "acknowledgement_count": len(acknowledgements),
            "revoked_acknowledgement_count": len(revoked_acknowledgements),
            "failure_count": len(failures),
            "ingested": ingested,
            "acknowledgements": acknowledgements,
            "revoked_acknowledgements": revoked_acknowledgements,
            "failures": failures,
        }

    def _reports(self) -> tuple[Path, ...]:
        found: dict[str, Path] = {}
        for root in self.report_roots:
            if not root.exists():
                continue
            for path in root.glob(self.pattern):
                if path.is_file():
                    found[str(path.resolve()).lower()] = path.resolve()
        return tuple(found[key] for key in sorted(found))


class GitPartnerNodeReportRefresher:
    """Refresh lifecycle reports from GP node-report refs without a full checkout."""

    def __init__(
        self,
        root: Path,
        database: ControlDatabase,
        registry: SystemRegistry,
        *,
        remote: str = "origin",
        command_timeout_seconds: int = 30,
        max_concurrency: int = 8,
    ) -> None:
        self.root = root.resolve()
        self.database = database
        self.registry = registry
        self.remote = remote
        self.command_timeout_seconds = max(5, int(command_timeout_seconds))
        self.max_concurrency = max(1, min(32, int(max_concurrency)))

    def run_once(
        self,
        *,
        endpoint_ids: set[str] | None = None,
    ) -> dict[str, Any]:
        refreshed: list[dict[str, Any]] = []
        failures: list[dict[str, str]] = []
        selected_nodes: set[str] = set()
        selected_endpoints: list[Any] = []
        for endpoint in self.registry.endpoints:
            if not endpoint.enabled:
                continue
            if endpoint_ids is not None and endpoint.endpoint_id not in endpoint_ids:
                continue
            if endpoint.node_id in selected_nodes:
                continue
            selected_nodes.add(endpoint.node_id)
            selected_endpoints.append(endpoint)

        reports: dict[str, dict[str, Any]] = {}
        read_failures: dict[str, BaseException] = {}
        worker_count = min(self.max_concurrency, max(1, len(selected_endpoints)))
        with ThreadPoolExecutor(
            max_workers=worker_count,
            thread_name_prefix="endpoint-report",
        ) as executor:
            futures = {
                executor.submit(self._read_report, endpoint): endpoint
                for endpoint in selected_endpoints
            }
            for future in as_completed(futures):
                endpoint = futures[future]
                try:
                    reports[endpoint.endpoint_id] = future.result()
                except (
                    ControlDatabaseError,
                    OSError,
                    subprocess.SubprocessError,
                    ValueError,
                ) as exc:
                    read_failures[endpoint.endpoint_id] = exc

        # SQLite remains single-writer. Remote reads fan out, then reports are
        # committed in registry order so replay and diagnostics stay stable.
        for endpoint in selected_endpoints:
            read_error = read_failures.get(endpoint.endpoint_id)
            if read_error is not None:
                failures.append(
                    {
                        "endpoint_id": endpoint.endpoint_id,
                        "node_id": endpoint.node_id,
                        "error": f"{type(read_error).__name__}: {read_error}",
                    }
                )
                continue
            try:
                report = reports[endpoint.endpoint_id]
                result = self.database.ingest_node_report(
                    report,
                    source=(
                        "gitpartner-node-ref:"
                        f"{endpoint.endpoint_id}:{endpoint.node_id}"
                    ),
                )
                refreshed.append(
                    {
                        "endpoint_id": endpoint.endpoint_id,
                        "node_id": endpoint.node_id,
                        **result,
                    }
                )
            except (
                ControlDatabaseError,
                OSError,
                subprocess.SubprocessError,
                ValueError,
            ) as exc:
                failures.append(
                    {
                        "endpoint_id": endpoint.endpoint_id,
                        "node_id": endpoint.node_id,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
        return {
            "refreshed_count": len(refreshed),
            "failure_count": len(failures),
            "refreshed": refreshed,
            "failures": failures,
        }

    def _read_report(self, endpoint: Any) -> dict[str, Any]:
        return query_gitpartner_node_report(
            root=self.root,
            gitpartner_repo=endpoint.gitpartner_repo,
            result_worktree=endpoint.result_worktree,
            report_branch=self._report_branch(endpoint),
            control_branch=endpoint.control_channel,
            node_id=endpoint.node_id,
            remote=self.remote,
            command_timeout_seconds=self.command_timeout_seconds,
        )

    def _report_branch(self, endpoint: Any) -> str:
        config_path = Path(str(endpoint.gitpartner_config or ""))
        if not config_path.is_absolute():
            config_path = self.root / config_path
        try:
            raw = json.loads(config_path.read_text(encoding="utf-8-sig"))
        except (OSError, json.JSONDecodeError):
            return "gp/nodes"
        lifecycle = raw.get("node_lifecycle")
        if not isinstance(lifecycle, dict):
            return "gp/nodes"
        return str(lifecycle.get("report_branch") or "gp/nodes")


class GitPartnerNodeAdmissionReconciler:
    """Refresh node reports and deliver a session-fenced ack when needed."""

    def __init__(
        self,
        root: Path,
        database: ControlDatabase,
        registry: SystemRegistry,
        *,
        ack_root: Path,
        adapter_factory: Callable[[Any], Any] | None = None,
        report_refresher: Any | None = None,
        ready_wait_seconds: float = 6.0,
        git_operation_timeout_seconds: int = 60,
        node_report_timeout_seconds: int = 15,
        node_report_max_concurrency: int = 8,
        allow_trusted_lease_probe: bool = True,
    ) -> None:
        self.root = root.resolve()
        self.database = database
        self.registry = registry
        self.ack_root = ack_root.resolve()
        self.adapter_factory = adapter_factory or self._adapter
        self.report_refresher = report_refresher or GitPartnerNodeReportRefresher(
            self.root,
            self.database,
            self.registry,
            command_timeout_seconds=node_report_timeout_seconds,
            max_concurrency=node_report_max_concurrency,
        )
        self.ready_wait_seconds = max(0.0, float(ready_wait_seconds))
        self.git_operation_timeout_seconds = max(
            15,
            min(120, int(git_operation_timeout_seconds)),
        )
        self.allow_trusted_lease_probe = bool(allow_trusted_lease_probe)

    def run_once(
        self,
        *,
        endpoint_ids: set[str] | None = None,
    ) -> dict[str, Any]:
        refresh = self.report_refresher.run_once(endpoint_ids=endpoint_ids)
        deliveries: list[dict[str, Any]] = []
        skipped: list[dict[str, str]] = []
        failures: list[dict[str, str]] = []
        endpoints = {
            endpoint.endpoint_id: endpoint
            for endpoint in self.registry.endpoints
        }
        for observed in refresh["refreshed"]:
            endpoint_id = str(observed.get("endpoint_id") or "")
            node_id = str(observed.get("node_id") or "")
            session_id = str(observed.get("session_id") or "")
            state = str(observed.get("state") or "")
            endpoint = endpoints.get(endpoint_id)
            if endpoint is None or not node_id or not session_id:
                continue
            ack_path = self.ack_root / node_id / "ack.json"
            ack = self.database.current_node_ack(node_id)
            if ack is None:
                existing_ack = _read_json_object(ack_path)
                expected_ack_identity = {
                    "state": "accepted",
                    "node_id": node_id,
                    "endpoint_id": endpoint_id,
                    "generation": str(observed.get("generation") or endpoint.generation),
                    "session_id": session_id,
                }
                if all(
                    str(existing_ack.get(key) or "") == value
                    for key, value in expected_ack_identity.items()
                ):
                    try:
                        ack = self.database.accept_node(
                            node_id,
                            self.registry,
                        )
                    except ControlDatabaseError as exc:
                        if "lease has expired" not in str(exc):
                            raise
                        if not self.allow_trusted_lease_probe:
                            skipped.append(
                                {
                                    "endpoint_id": endpoint_id,
                                    "node_id": node_id,
                                    "session_id": session_id,
                                    "reason": "expired-lease-trusted-probe-deferred",
                                }
                            )
                            continue
                        probe_request_id = (
                            "node-lease-probe-"
                            + hashlib.sha256(
                                (
                                    f"{endpoint_id}:{node_id}:"
                                    f"{session_id}:{os.getpid()}:"
                                    f"{time.time_ns()}"
                                ).encode("utf-8")
                            ).hexdigest()[:20]
                        )
                        probe = self.adapter_factory(endpoint).snapshot(
                            request_id=probe_request_id,
                            wait_timeout_seconds=180,
                        )
                        engine_snapshot = probe.get("engine_snapshot")
                        if not isinstance(engine_snapshot, dict):
                            raise ControlDatabaseError(
                                "trusted node lease probe returned no Engine snapshot"
                            )
                        ack = self.database.renew_node_lease_after_trusted_probe(
                            node_id,
                            self.registry,
                            source=(
                                "trusted-engine-snapshot:"
                                f"{endpoint_id}:{probe_request_id}"
                            ),
                        )
            if ack is None or str(ack.get("session_id") or "") != session_id:
                continue
            marker_path = self.ack_root / node_id / "delivery.json"
            _write_json_atomic_if_changed(ack_path, ack)
            marker = _read_json_object(marker_path)
            identity = {
                "endpoint_id": endpoint_id,
                "generation": str(ack["generation"]),
                "node_id": node_id,
                "session_id": session_id,
            }
            if all(marker.get(key) == value for key, value in identity.items()):
                deliveries.append(
                    {
                        **identity,
                        "state": "already-delivered",
                        "ready_observed": (
                            state == "ready"
                            or self._wait_until_ready(
                                endpoint_id,
                                node_id,
                                session_id,
                            )
                        ),
                    }
                )
                continue
            if state == "ready":
                _write_json_atomic_if_changed(
                    marker_path,
                    {
                        **identity,
                        "state": "observed-ready",
                        "delivered_at": utc_now_iso(),
                    },
                )
                deliveries.append({**identity, "state": "observed-ready"})
                continue
            if state != "awaiting-acceptance":
                continue
            ack_digest = hashlib.sha256(
                json.dumps(
                    ack,
                    ensure_ascii=True,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8")
            ).hexdigest()
            request_id = "auto-node-ack-" + hashlib.sha256(
                (
                    f"{node_id}:{session_id}:{ack['generation']}:"
                    f"{ack_digest}"
                ).encode("utf-8")
            ).hexdigest()[:20]
            try:
                with NamedProcessLock(
                    self.root,
                    f"node_ack_delivery_{node_id}",
                    stale_after_seconds=180,
                    wait_timeout_seconds=1,
                ):
                    marker = _read_json_object(marker_path)
                    if all(
                        marker.get(key) == value
                        for key, value in identity.items()
                    ):
                        deliveries.append(
                            {
                                **identity,
                                "state": "already-delivered",
                                "ready_observed": self._wait_until_ready(
                                    endpoint_id,
                                    node_id,
                                    session_id,
                                ),
                            }
                        )
                        continue
                    result = self.adapter_factory(endpoint).acknowledge_node(
                        request_id=request_id,
                        ack_path=ack_path,
                        wait_timeout_seconds=180,
                    )
                    _write_json_atomic_if_changed(
                        marker_path,
                        {
                            **identity,
                            "state": "delivered",
                            "request_id": request_id,
                            "delivered_at": utc_now_iso(),
                        },
                    )
                    ready_observed = self._wait_until_ready(
                        endpoint_id,
                        node_id,
                        session_id,
                    )
                    deliveries.append(
                        {
                            **identity,
                            "state": "delivered",
                            "request_id": request_id,
                            "transport_elapsed_seconds": result.get(
                                "transport_elapsed_seconds"
                            ),
                            "ready_observed": ready_observed,
                        }
                    )
            except Exception as exc:
                failures.append(
                    {
                        **identity,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
        return {
            **refresh,
            "ack_delivery_count": len(deliveries),
            "skipped_count": len(skipped),
            "failure_count": int(refresh["failure_count"]) + len(failures),
            "ack_deliveries": deliveries,
            "skipped": skipped,
            "failures": [*refresh["failures"], *failures],
        }

    def _wait_until_ready(
        self,
        endpoint_id: str,
        node_id: str,
        session_id: str,
    ) -> bool:
        deadline = time.monotonic() + self.ready_wait_seconds
        while time.monotonic() < deadline:
            time.sleep(min(0.25, max(0.0, deadline - time.monotonic())))
            refreshed = self.report_refresher.run_once(
                endpoint_ids={endpoint_id}
            )
            for observed in refreshed.get("refreshed", []):
                if (
                    str(observed.get("node_id") or "") == node_id
                    and str(observed.get("session_id") or "") == session_id
                    and str(observed.get("state") or "") == "ready"
                ):
                    return True
        return False

    def _adapter(self, endpoint: Any) -> NodeControlTransport:
        return NodeControlTransport(
            self.root,
            endpoint,
            git_operation_timeout_seconds=self.git_operation_timeout_seconds,
        )


def _last_json_object(output: str) -> dict[str, Any]:
    for line in reversed(output.splitlines()):
        value = line.strip()
        if not value.startswith("{"):
            continue
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed
    raise ValueError("command output did not contain a JSON object")


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _write_json_atomic_if_changed(
    path: Path,
    value: dict[str, Any],
) -> bool:
    content = json.dumps(
        value, ensure_ascii=True, indent=2, sort_keys=True
    ) + "\n"
    if path.exists() and path.read_text(encoding="utf-8-sig") == content:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, path)
    return True
