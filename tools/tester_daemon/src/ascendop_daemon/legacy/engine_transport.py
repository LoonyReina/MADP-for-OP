from __future__ import annotations

import atexit
import hashlib
import json
import os
import queue
import shutil
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Callable

from ascendop_daemon.legacy.engine_admission import EngineAdmissionError, EngineAdmissionStore, correlation
from ascendop_daemon.legacy.engine_promotion import expected_remote_engine_code_generation
from ascendop_daemon.legacy.executor import process_creation_flags, process_startupinfo
from ascendop_daemon.runtime.locking import NamedProcessLock
from ascendop_daemon.runtime.python_environment import prepend_pythonpath, shared_protocol_source
from ascendop_daemon.core.models import utc_now_iso


class EngineTransportError(RuntimeError):
    pass


class EngineReturnAlreadyCompactedError(EngineTransportError):
    """The remote return was acknowledged before its bundle reached the client."""


GITPARTNER_LOCAL_TIMELINE_PREFIX = "GITPARTNER_LOCAL_TIMELINE:"


class EngineTransportAdapter:
    def __init__(
        self,
        root: Path,
        *,
        gitpartner_repo: Path,
        result_gitpartner_repo: Path | None = None,
        engine_root: str = "test_engine_demo",
        remote_root: str = "/opt/ascendop",
        transport: str = "relay",
        endpoint_id: str = "",
        node_id: str = "",
        execution_environment_id: str = "",
        gateway_id: str = "",
        transport_mode: str = "",
        registration_generation: str = "",
        control_channel: str = "",
        result_channel: str = "",
        append_requests: bool = False,
        duplex_lanes: bool = False,
        remote_gitpartner_repo: str = "ascend-git-partner",
        exchange_wait_ready_seconds: float = 45.0,
        engine_wait_initial_grace_seconds: int = 0,
        runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
    ) -> None:
        self.root = root.resolve()
        self.gitpartner_repo = gitpartner_repo.resolve()
        self.result_gitpartner_repo = (
            result_gitpartner_repo.resolve()
            if result_gitpartner_repo is not None
            else self.gitpartner_repo
        )
        self.engine_root = safe_relative_engine_root(engine_root)
        self.remote_root = remote_root
        self.transport = transport
        self.endpoint_id = safe_optional_token(endpoint_id, "endpoint_id")
        self.node_id = safe_optional_token(node_id, "node_id")
        self.execution_environment_id = safe_optional_token(
            execution_environment_id, "execution_environment_id"
        )
        self.gateway_id = safe_optional_token(gateway_id, "gateway_id")
        self.transport_mode = transport_mode.strip()
        self.registration_generation = registration_generation.strip()
        self.control_channel = control_channel.strip()
        self.result_channel = result_channel.strip()
        self.append_requests = bool(append_requests)
        self.duplex_lanes = bool(duplex_lanes)
        self.remote_gitpartner_repo = safe_relative_engine_root(
            remote_gitpartner_repo
        )
        self.exchange_wait_ready_seconds = max(
            0.0, float(exchange_wait_ready_seconds)
        )
        self.engine_wait_initial_grace_seconds = max(
            0, int(engine_wait_initial_grace_seconds)
        )
        self.runner = runner or subprocess.run
        self.admission = EngineAdmissionStore(
            self.root,
            endpoint_id=self.endpoint_id,
        )
        self._result_query_process: subprocess.Popen[str] | None = None
        self._result_query_responses: queue.Queue[str | None] | None = None
        self._result_query_reader: threading.Thread | None = None
        self._result_query_lock = threading.Lock()
        self._result_query_atexit_registered = False
        # Relay completion can precede the Git-published pullback becoming
        # visible in the local worktree. Return immediately when artifacts are
        # present, but tolerate one slow publication cycle before failing.
        self.output_settle_timeout_seconds = 60.0

    @property
    def supports_duplex_exchange(self) -> bool:
        return bool(
            self.append_requests
            and self.duplex_lanes
            and self.result_channel
            and (self.result_gitpartner_repo / ".git").exists()
        )

    def accept(
        self,
        spec_path: Path,
        *,
        request_id: str,
        engine_job_id: str,
        payload_root: Path | None = None,
        wait_timeout_seconds: int = 180,
    ) -> dict[str, Any]:
        spec = read_object(spec_path)
        candidate = correlation(spec)
        if candidate["engine_job_id"] != engine_job_id:
            raise EngineTransportError("engine job id does not match spec")
        self.admission.begin_admission(
            {
                **candidate,
                "workflow_ingest": bool(spec.get("workflow_ingest", True)),
            }
        )
        command = self.accept_command(
            spec_path,
            request_id=request_id,
            engine_job_id=engine_job_id,
            payload_root=payload_root,
            wait_timeout_seconds=wait_timeout_seconds,
        )
        started = time.monotonic()
        try:
            completed = self._run(command)
        except Exception as exc:
            error = f"engine accept transport failed before completion: {exc}"
            self.admission.record_admission_failure(engine_job_id, error)
            raise EngineTransportError(error) from exc
        elapsed = round(time.monotonic() - started, 3)
        if completed.returncode != 0:
            error = command_failure("engine accept", completed)
            self.admission.record_admission_failure(engine_job_id, error)
            raise EngineTransportError(error)
        local_transport_timeline = require_local_transport_timeline(
            completed,
            request_id=request_id,
            operation="engine accept",
        )
        output_root = self.output_root(request_id)
        receipt = wait_for_value(
            lambda: find_correlated_json(output_root, "accepted.json", engine_job_id),
            description=f"engine acceptance receipt for {engine_job_id}",
            timeout_seconds=self.output_settle_timeout_seconds,
        )
        if receipt is None:
            error = f"engine acceptance receipt not found under {output_root}"
            self.admission.record_admission_failure(engine_job_id, error)
            raise EngineTransportError(error)
        accepted = self.admission.record_acceptance(receipt)
        return {
            **accepted,
            "transport_elapsed_seconds": elapsed,
            "local_transport_timeline": local_transport_timeline,
        }

    def exchange(
        self,
        jobs: list[dict[str, Any]],
        *,
        request_id: str,
        acknowledgements: list[dict[str, str]] | None = None,
        required_acknowledgements: list[dict[str, str]] | None = None,
        standby_cancellations: list[dict[str, str]] | None = None,
        max_inflight: int,
        draining: bool,
        standby_slots: int | None = None,
        active_job_slots: int | None = None,
        host_slots: int | None = None,
        device_inventory: list[dict[str, Any]] | None = None,
        export_slots: int | None = None,
        return_backlog_soft_limit_bytes: int | None = None,
        return_backlog_hard_limit_bytes: int | None = None,
        return_backlog_soft_limit_jobs: int | None = None,
        return_backlog_hard_limit_jobs: int | None = None,
        wait_timeout_seconds: int = 180,
    ) -> dict[str, Any]:
        manifest_jobs: list[dict[str, str]] = []
        for item in jobs:
            spec_path = Path(str(item.get("spec_path") or "")).resolve()
            spec = read_object(spec_path)
            candidate = correlation(spec)
            job_id = str(item.get("engine_job_id") or candidate["engine_job_id"])
            if candidate["engine_job_id"] != job_id:
                raise EngineTransportError(
                    f"engine exchange job id does not match spec: {job_id}"
                )
            payload_root_raw = str(item.get("payload_root") or "")
            payload_root = (
                Path(payload_root_raw).resolve() if payload_root_raw else None
            )
            if payload_root is not None and not payload_root.is_dir():
                raise EngineTransportError(
                    f"engine exchange payload root is missing: {payload_root}"
                )
            admission_mode = str(item.get("admission_mode") or "accept")
            if admission_mode not in {"accept", "standby"}:
                raise EngineTransportError(
                    f"unsupported engine exchange admission mode: {admission_mode}"
                )
            begin = (
                self.admission.begin_standby
                if admission_mode == "standby"
                else self.admission.begin_admission
            )
            begin(
                {
                    **candidate,
                    "workflow_ingest": bool(spec.get("workflow_ingest", True)),
                }
            )
            manifest_item = {
                "engine_job_id": job_id,
                "spec": str(spec_path),
                "admission_mode": admission_mode,
            }
            if payload_root is not None:
                manifest_item["payload_root"] = str(payload_root)
            manifest_jobs.append(manifest_item)

        sent_acknowledgements = normalize_acknowledgements(acknowledgements or [])
        sent_required_acknowledgements = normalize_acknowledgements(
            required_acknowledgements or []
        )
        sent_standby_cancellations = normalize_standby_cancellations(
            standby_cancellations or []
        )
        with tempfile.TemporaryDirectory(prefix="ascendop-engine-exchange-") as temp:
            manifest_path = Path(temp) / "exchange.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "jobs": manifest_jobs,
                        "standby_cancellations": sent_standby_cancellations,
                    },
                    ensure_ascii=True,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            command = self.exchange_command(
                manifest_path,
                request_id=request_id,
                acknowledgements=sent_acknowledgements,
                required_acknowledgements=sent_required_acknowledgements,
                max_inflight=max_inflight,
                draining=draining,
                standby_slots=standby_slots,
                active_job_slots=active_job_slots,
                host_slots=host_slots,
                device_inventory=device_inventory,
                export_slots=export_slots,
                return_backlog_soft_limit_bytes=return_backlog_soft_limit_bytes,
                return_backlog_hard_limit_bytes=return_backlog_hard_limit_bytes,
                return_backlog_soft_limit_jobs=return_backlog_soft_limit_jobs,
                return_backlog_hard_limit_jobs=return_backlog_hard_limit_jobs,
                wait_timeout_seconds=wait_timeout_seconds,
            )
            started = time.monotonic()
            try:
                completed = self._run(command)
            except Exception as exc:
                error = f"engine exchange transport failed before completion: {exc}"
                for item in manifest_jobs:
                    self.admission.record_admission_failure(
                        item["engine_job_id"], error
                    )
                raise EngineTransportError(error) from exc
            elapsed = round(time.monotonic() - started, 3)
        if completed.returncode != 0:
            error = command_failure("engine exchange", completed)
            for item in manifest_jobs:
                self.admission.record_admission_failure(item["engine_job_id"], error)
            raise EngineTransportError(error)
        local_transport_timeline = require_local_transport_timeline(
            completed,
            request_id=request_id,
            operation="engine exchange",
        )

        output_root = self.output_root(request_id)
        snapshot = wait_for_value(
            lambda: find_named_object(output_root, "engine_status.json"),
            description="engine exchange status",
            timeout_seconds=self.output_settle_timeout_seconds,
        )
        if snapshot is None:
            raise EngineTransportError(
                f"engine exchange status not found under {output_root}"
            )
        snapshot["transport_received_at"] = utc_now_iso()
        self.admission.reconcile_engine_snapshot(snapshot)
        exchange_timeline = wait_for_value(
            lambda: find_named_object(output_root, "exchange_timeline.json"),
            description="engine exchange B-side timeline",
            timeout_seconds=self.output_settle_timeout_seconds,
        )
        if exchange_timeline is None:
            raise EngineTransportError(
                f"engine exchange B-side timeline not found under {output_root}"
            )

        cancellation_results: list[dict[str, Any]] = []
        if sent_standby_cancellations:
            cancellation_payload = wait_for_value(
                lambda: find_named_object(output_root, "standby_cancellations.json"),
                description="engine exchange standby cancellation receipts",
                timeout_seconds=self.output_settle_timeout_seconds,
            )
            if cancellation_payload is None:
                raise EngineTransportError(
                    "engine exchange standby cancellation receipts not found"
                )
            raw_results = cancellation_payload.get("cancellations", [])
            if not isinstance(raw_results, list):
                raise EngineTransportError(
                    "engine exchange standby cancellation receipts must be a list"
                )
            by_job = {
                str(item.get("engine_job_id") or ""): item
                for item in raw_results
                if isinstance(item, dict) and item.get("engine_job_id")
            }
            for request in sent_standby_cancellations:
                job_id = request["engine_job_id"]
                result = by_job.get(job_id)
                if not isinstance(result, dict):
                    raise EngineTransportError(
                        f"engine standby cancellation receipt missing for {job_id}"
                    )
                cancellation_results.append(
                    self.admission.record_standby_cancellation(result)
                )

        required_acknowledgement_results: list[dict[str, Any]] = []
        if sent_required_acknowledgements:
            required_ack_payload = wait_for_value(
                lambda: find_named_object(
                    output_root, "required_acknowledgements.json"
                ),
                description="engine exchange required acknowledgement receipts",
                timeout_seconds=self.output_settle_timeout_seconds,
            )
            if required_ack_payload is None:
                raise EngineTransportError(
                    "engine exchange required acknowledgement receipts not found"
                )
            raw_results = required_ack_payload.get("acknowledgements", [])
            if not isinstance(raw_results, list):
                raise EngineTransportError(
                    "engine exchange required acknowledgement receipts must be a list"
                )
            by_job = {
                str(item.get("engine_job_id") or ""): item
                for item in raw_results
                if isinstance(item, dict) and item.get("engine_job_id")
            }
            for request in sent_required_acknowledgements:
                job_id = request["engine_job_id"]
                result = by_job.get(job_id)
                if not isinstance(result, dict):
                    raise EngineTransportError(
                        f"engine required acknowledgement receipt missing for {job_id}"
                    )
                required_acknowledgement_results.append(result)

        rejected_path = wait_for_value(
            lambda: find_named_path(output_root, "rejected_jobs.txt"),
            description="engine exchange rejection manifest",
            timeout_seconds=self.output_settle_timeout_seconds,
        )
        rejected_jobs = {
            line.strip()
            for line in (
                rejected_path.read_text(encoding="utf-8", errors="replace").splitlines()
                if rejected_path is not None
                else []
            )
            if line.strip()
        }
        rejection_errors: dict[str, str] = {}
        receipts: list[dict[str, Any]] = []
        standby_receipts: list[dict[str, Any]] = []
        for item in manifest_jobs:
            job_id = item["engine_job_id"]
            if job_id in rejected_jobs:
                rejection_path = find_named_path(
                    output_root, f"rejected_{job_id}.log"
                )
                reason = (
                    rejection_path.read_text(
                        encoding="utf-8", errors="replace"
                    ).strip()
                    if rejection_path is not None
                    else ""
                )
                error = "remote engine rejected batch admission"
                if reason:
                    error += f": {reason}"
                    rejection_errors[job_id] = reason
                self.admission.record_admission_failure(
                    job_id, error
                )
                continue
            admission_mode = str(item.get("admission_mode") or "accept")
            receipt_name = (
                "standby.json" if admission_mode == "standby" else "accepted.json"
            )
            receipt = wait_for_value(
                lambda job_id=job_id, receipt_name=receipt_name: find_correlated_json(
                    output_root, receipt_name, job_id
                ),
                description=f"engine exchange {admission_mode} receipt for {job_id}",
                timeout_seconds=self.output_settle_timeout_seconds,
            )
            if receipt is None:
                error = f"engine exchange {admission_mode} receipt missing for {job_id}"
                self.admission.record_admission_failure(job_id, error)
                raise EngineTransportError(error)
            if admission_mode == "standby":
                standby_receipts.append(self.admission.record_standby(receipt))
            else:
                receipts.append(self.admission.record_acceptance(receipt))

        ready = wait_for_named_list(
            output_root,
            "return_ready.json",
            timeout_seconds=self.output_settle_timeout_seconds,
        )
        terminal_records: list[dict[str, Any]] = []
        for manifest in ready:
            try:
                terminal_records.append(
                    self.admission.record_terminal_manifest(manifest)
                )
            except EngineAdmissionError:
                continue
        ready_cache = (
            self.root
            / "TestUtils"
            / "tester_daemon"
            / "engine_ready_cache"
            / hashlib.sha256(request_id.encode("utf-8")).hexdigest()[:16]
        )
        exported_ready = wait_for_value(
            lambda: ready_manifests_selected_by_export(output_root, ready),
            description="engine exchange ready export selection",
            timeout_seconds=self.output_settle_timeout_seconds,
            accept=lambda value: isinstance(value, list),
        )
        if exported_ready:
            wait_for_value(
                lambda: extract_settled_ready_archive(
                    output_root,
                    ready_cache,
                    exported_ready,
                ),
                description="engine exchange ready archive",
                timeout_seconds=self.output_settle_timeout_seconds,
            )
            ready_bundles = wait_for_value(
                lambda: discover_ready_bundles(
                    output_root,
                    exported_ready,
                    bundle_root=ready_cache,
                    report_root=self.root,
                ),
                description="engine exchange ready bundles",
                timeout_seconds=self.output_settle_timeout_seconds,
                accept=lambda value: isinstance(value, list)
                and len(value) == len(exported_ready),
            )
        else:
            ready_bundles = []
        return {
            "engine_snapshot": snapshot,
            "accepted_receipts": receipts,
            "standby_receipts": standby_receipts,
            "standby_cancellation_receipts": cancellation_results,
            "rejected_jobs": sorted(rejected_jobs),
            "rejection_errors": rejection_errors,
            "return_ready": ready,
            "ready_bundles": ready_bundles,
            "terminal_records": terminal_records,
            "admission": self.admission.snapshot(engine_snapshot=snapshot),
            "snapshot_request_id": request_id,
            "acknowledgements_sent": sent_acknowledgements,
            "required_acknowledgements_sent": sent_required_acknowledgements,
            "required_acknowledgement_receipts": required_acknowledgement_results,
            "standby_cancellations_sent": sent_standby_cancellations,
            "transport_elapsed_seconds": elapsed,
            "exchange_timeline": exchange_timeline,
            "local_transport_timeline": local_transport_timeline,
        }

    def publish_exchange(
        self,
        jobs: list[dict[str, Any]],
        *,
        request_id: str,
        acknowledgements: list[dict[str, str]] | None = None,
        required_acknowledgements: list[dict[str, str]] | None = None,
        standby_cancellations: list[dict[str, str]] | None = None,
        max_inflight: int,
        draining: bool,
        standby_slots: int | None = None,
        active_job_slots: int | None = None,
        host_slots: int | None = None,
        device_inventory: list[dict[str, Any]] | None = None,
        export_slots: int | None = None,
        return_backlog_soft_limit_bytes: int | None = None,
        return_backlog_hard_limit_bytes: int | None = None,
        return_backlog_soft_limit_jobs: int | None = None,
        return_backlog_hard_limit_jobs: int | None = None,
        wait_timeout_seconds: int = 180,
        wait_ready_seconds: float | None = None,
    ) -> dict[str, Any]:
        if not self.supports_duplex_exchange:
            raise EngineTransportError(
                "duplex Engine exchange is not available for this endpoint"
            )
        manifest_jobs = self._prepare_exchange_jobs(jobs)
        sent_acknowledgements = normalize_acknowledgements(acknowledgements or [])
        sent_required_acknowledgements = normalize_acknowledgements(
            required_acknowledgements or []
        )
        sent_standby_cancellations = normalize_standby_cancellations(
            standby_cancellations or []
        )
        with tempfile.TemporaryDirectory(
            prefix="ascendop-engine-exchange-publish-"
        ) as temp:
            manifest_path = Path(temp) / "exchange.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "jobs": manifest_jobs,
                        "standby_cancellations": sent_standby_cancellations,
                    },
                    ensure_ascii=True,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            command = self.exchange_command(
                manifest_path,
                request_id=request_id,
                acknowledgements=sent_acknowledgements,
                required_acknowledgements=sent_required_acknowledgements,
                max_inflight=max_inflight,
                draining=draining,
                standby_slots=standby_slots,
                active_job_slots=active_job_slots,
                host_slots=host_slots,
                device_inventory=device_inventory,
                export_slots=export_slots,
                return_backlog_soft_limit_bytes=return_backlog_soft_limit_bytes,
                return_backlog_hard_limit_bytes=return_backlog_hard_limit_bytes,
                return_backlog_soft_limit_jobs=return_backlog_soft_limit_jobs,
                return_backlog_hard_limit_jobs=return_backlog_hard_limit_jobs,
                wait_timeout_seconds=wait_timeout_seconds,
                wait=False,
                keep_existing_output=True,
                wait_ready_seconds=wait_ready_seconds,
            )
            started = time.monotonic()
            completed = self._run(command, lane="ingress")
            elapsed = round(time.monotonic() - started, 3)
        if completed.returncode != 0:
            raise EngineTransportError(
                command_failure("duplex engine exchange publish", completed)
            )
        return {
            "request_id": request_id,
            "jobs": manifest_jobs,
            "acknowledgements_sent": sent_acknowledgements,
            "required_acknowledgements_sent": sent_required_acknowledgements,
            "standby_cancellations_sent": sent_standby_cancellations,
            "transport_elapsed_seconds": elapsed,
            "local_transport_timeline": require_local_transport_timeline(
                completed,
                request_id=request_id,
                operation="duplex engine exchange publish",
            ),
            "published_at": utc_now_iso(),
        }

    def poll_exchanges(
        self,
        requests: list[dict[str, Any]],
        *,
        wait_timeout_seconds: int = 180,
    ) -> dict[str, dict[str, Any] | None]:
        if not requests:
            return {}
        if not self.supports_duplex_exchange:
            raise EngineTransportError(
                "duplex Engine exchange is not available for this endpoint"
            )
        request_ids = [
            safe_token(str(item.get("request_id") or ""), "request_id")
            for item in requests
        ]
        command = [
            sys.executable,
            "-m",
            "limited_remote_partner.gateway.batch_result_query",
            "--repo",
            str(self.result_gitpartner_repo),
            "--result-branch",
            self.result_channel,
            "--control-branch",
            self.control_channel,
            "--materialize-root",
            str(self.gitpartner_repo),
        ]
        for request_id in request_ids:
            command.extend(["--output-subdir", f"engine-demo/{request_id}"])
        started = time.monotonic()
        if self.runner is subprocess.run:
            observed = self._run_persistent_result_query(request_ids)
            completed = None
        else:
            completed = self._run(
                command,
                lane="result",
                repo=self.result_gitpartner_repo,
            )
            if completed.returncode != 0:
                raise EngineTransportError(
                    command_failure("duplex engine exchange poll", completed)
                )
            observed = parse_last_json_object(
                completed.stdout,
                description="duplex engine exchange poll",
            )
        elapsed = round(time.monotonic() - started, 3)
        raw_items = observed.get("items", [])
        by_request = {
            str(item.get("request_id") or ""): item
            for item in raw_items
            if isinstance(item, dict) and item.get("request_id")
        }
        results: dict[str, dict[str, Any] | None] = {}
        for request in requests:
            request_id = str(request.get("request_id") or "")
            item = by_request.get(request_id)
            status = item.get("status") if isinstance(item, dict) else None
            if not isinstance(status, dict):
                results[request_id] = None
                continue
            state = str(status.get("state") or "")
            if state not in {"success", "failed", "cancelled"}:
                results[request_id] = None
                continue
            if state != "success":
                results[request_id] = {
                    "duplex_exchange_error": (
                        f"remote exchange entered terminal state {state}"
                    ),
                    "duplex_exchange_terminal": True,
                    "remote_status": status,
                }
                continue
            if not bool(item.get("materialized")):
                results[request_id] = {
                    "duplex_exchange_error": (
                        "terminal exchange output was not materialized"
                    ),
                    "duplex_exchange_terminal": False,
                    "remote_status": status,
                }
                continue
            result = self._read_duplex_exchange_output(
                request,
                transport_elapsed_seconds=elapsed,
            )
            result["result_query"] = {
                "result_branch": str(observed.get("result_branch") or ""),
                "result_commit_created_at": str(
                    observed.get("commit_created_at") or ""
                ),
                "timing": (
                    dict(observed.get("timing", {}))
                    if isinstance(observed.get("timing"), dict)
                    else {}
                ),
            }
            results[request_id] = result
        return results

    def _prepare_exchange_jobs(
        self,
        jobs: list[dict[str, Any]],
    ) -> list[dict[str, str]]:
        manifest_jobs: list[dict[str, str]] = []
        for item in jobs:
            spec_path = Path(str(item.get("spec_path") or "")).resolve()
            spec = read_object(spec_path)
            candidate = correlation(spec)
            job_id = str(item.get("engine_job_id") or candidate["engine_job_id"])
            if candidate["engine_job_id"] != job_id:
                raise EngineTransportError(
                    f"engine exchange job id does not match spec: {job_id}"
                )
            payload_root_raw = str(item.get("payload_root") or "")
            payload_root = (
                Path(payload_root_raw).resolve() if payload_root_raw else None
            )
            if payload_root is not None and not payload_root.is_dir():
                raise EngineTransportError(
                    f"engine exchange payload root is missing: {payload_root}"
                )
            admission_mode = str(item.get("admission_mode") or "accept")
            if admission_mode not in {"accept", "standby"}:
                raise EngineTransportError(
                    f"unsupported engine exchange admission mode: {admission_mode}"
                )
            begin = (
                self.admission.begin_standby
                if admission_mode == "standby"
                else self.admission.begin_admission
            )
            begin(
                {
                    **candidate,
                    "workflow_ingest": bool(spec.get("workflow_ingest", True)),
                }
            )
            manifest_item = {
                "engine_job_id": job_id,
                "spec": str(spec_path),
                "admission_mode": admission_mode,
            }
            if payload_root is not None:
                manifest_item["payload_root"] = str(payload_root)
            manifest_jobs.append(manifest_item)
        return manifest_jobs

    def _read_duplex_exchange_output(
        self,
        request: dict[str, Any],
        *,
        transport_elapsed_seconds: float,
    ) -> dict[str, Any]:
        request_id = str(request.get("request_id") or "")
        output_root = self.output_root(request_id)
        snapshot = find_named_object(output_root, "engine_status.json")
        if snapshot is None:
            raise EngineTransportError(
                f"duplex engine exchange status not found under {output_root}"
            )
        snapshot["transport_received_at"] = utc_now_iso()
        self.admission.reconcile_engine_snapshot(snapshot)
        exchange_timeline = find_named_object(output_root, "exchange_timeline.json")
        if exchange_timeline is None:
            raise EngineTransportError(
                f"duplex engine exchange timeline not found under {output_root}"
            )

        sent_cancellations = normalize_standby_cancellations(
            request.get("standby_cancellations", [])
        )
        cancellation_results = self._read_exchange_control_receipts(
            output_root,
            "standby_cancellations.json",
            "cancellations",
            sent_cancellations,
            record=self.admission.record_standby_cancellation,
        )
        sent_required = normalize_acknowledgements(
            request.get("required_acknowledgements", [])
        )
        required_acknowledgement_results = self._read_exchange_control_receipts(
            output_root,
            "required_acknowledgements.json",
            "acknowledgements",
            sent_required,
        )
        rejected_path = find_named_path(output_root, "rejected_jobs.txt")
        rejected_jobs = {
            line.strip()
            for line in (
                rejected_path.read_text(
                    encoding="utf-8", errors="replace"
                ).splitlines()
                if rejected_path is not None
                else []
            )
            if line.strip()
        }
        rejection_errors: dict[str, str] = {}
        receipts: list[dict[str, Any]] = []
        standby_receipts: list[dict[str, Any]] = []
        snapshot_jobs = {
            str(row.get("engine_job_id") or ""): row
            for row in snapshot.get("jobs", [])
            if isinstance(row, dict) and row.get("engine_job_id")
        }
        for item in request.get("jobs", []):
            if not isinstance(item, dict):
                continue
            job_id = str(item.get("engine_job_id") or "")
            if job_id in rejected_jobs:
                rejection_path = find_named_path(
                    output_root, f"rejected_{job_id}.log"
                )
                reason = (
                    rejection_path.read_text(
                        encoding="utf-8", errors="replace"
                    ).strip()
                    if rejection_path is not None
                    else ""
                )
                if reason:
                    rejection_errors[job_id] = reason
                self.admission.record_admission_failure(
                    job_id,
                    "remote engine rejected duplex admission"
                    + (f": {reason}" if reason else ""),
                )
                continue
            admission_mode = str(item.get("admission_mode") or "accept")
            receipt_name = (
                "standby.json"
                if admission_mode == "standby"
                else "accepted.json"
            )
            receipt = find_correlated_json(output_root, receipt_name, job_id)
            if receipt is None:
                observed = snapshot_jobs.get(job_id)
                if (
                    not isinstance(observed, dict)
                    or correlation(observed) != correlation(item)
                ):
                    raise EngineTransportError(
                        f"duplex engine {admission_mode} receipt missing for {job_id}"
                    )
                # A response archive can lose an individual receipt while its
                # same-exchange status snapshot already proves the immutable
                # job was accepted. Recover from that correlated durable fact
                # instead of replaying an admission that is already running.
                receipt = {
                    **correlation(item),
                    "state": "standby" if admission_mode == "standby" else "accepted",
                    (
                        "staged_at"
                        if admission_mode == "standby"
                        else "accepted_at"
                    ): str(
                        observed.get(
                            "staged_at"
                            if admission_mode == "standby"
                            else "accepted_at"
                        )
                        or snapshot.get("observed_at")
                        or utc_now_iso()
                    ),
                }
            if admission_mode == "standby":
                standby_receipts.append(self.admission.record_standby(receipt))
            else:
                receipts.append(self.admission.record_acceptance(receipt))

        ready = wait_for_named_list(output_root, "return_ready.json", timeout_seconds=0)
        terminal_records: list[dict[str, Any]] = []
        for manifest in ready:
            try:
                terminal_records.append(
                    self.admission.record_terminal_manifest(manifest)
                )
            except EngineAdmissionError:
                continue
        ready_cache = (
            self.root
            / "TestUtils"
            / "tester_daemon"
            / "engine_ready_cache"
            / hashlib.sha256(request_id.encode("utf-8")).hexdigest()[:16]
        )
        exported_ready = ready_manifests_selected_by_export(output_root, ready)
        if exported_ready:
            extract_settled_ready_archive(
                output_root,
                ready_cache,
                exported_ready,
            )
            ready_bundles = discover_ready_bundles(
                output_root,
                exported_ready,
                bundle_root=ready_cache,
                report_root=self.root,
            )
            if len(ready_bundles) != len(exported_ready):
                raise EngineTransportError(
                    f"duplex exchange ready bundle count mismatch: {request_id}"
                )
        else:
            ready_bundles = []
        sent_acknowledgements = normalize_acknowledgements(
            request.get("acknowledgements", [])
        )
        return {
            "engine_snapshot": snapshot,
            "accepted_receipts": receipts,
            "standby_receipts": standby_receipts,
            "standby_cancellation_receipts": cancellation_results,
            "rejected_jobs": sorted(rejected_jobs),
            "rejection_errors": rejection_errors,
            "return_ready": ready,
            "ready_bundles": ready_bundles,
            "terminal_records": terminal_records,
            "admission": self.admission.snapshot(engine_snapshot=snapshot),
            "snapshot_request_id": request_id,
            "acknowledgements_sent": sent_acknowledgements,
            "required_acknowledgements_sent": sent_required,
            "required_acknowledgement_receipts": (
                required_acknowledgement_results
            ),
            "standby_cancellations_sent": sent_cancellations,
            "transport_elapsed_seconds": transport_elapsed_seconds,
            "exchange_timeline": exchange_timeline,
            "local_transport_timeline": (
                dict(request.get("local_transport_timeline", {}))
                if isinstance(request.get("local_transport_timeline"), dict)
                else {}
            ),
        }

    @staticmethod
    def _read_exchange_control_receipts(
        output_root: Path,
        filename: str,
        collection_name: str,
        sent: list[dict[str, str]],
        *,
        record: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        if not sent:
            return []
        payload = find_named_object(output_root, filename)
        if payload is None:
            raise EngineTransportError(
                f"engine exchange control receipts not found: {filename}"
            )
        raw_results = payload.get(collection_name, [])
        if not isinstance(raw_results, list):
            raise EngineTransportError(
                f"engine exchange control receipts must be a list: {filename}"
            )
        by_job = {
            str(item.get("engine_job_id") or ""): item
            for item in raw_results
            if isinstance(item, dict) and item.get("engine_job_id")
        }
        results: list[dict[str, Any]] = []
        for request in sent:
            job_id = str(request.get("engine_job_id") or "")
            result = by_job.get(job_id)
            if not isinstance(result, dict):
                raise EngineTransportError(
                    f"engine exchange control receipt missing for {job_id}"
                )
            results.append(record(result) if record is not None else result)
        return results

    def snapshot(
        self,
        *,
        request_id: str,
        acknowledgements: list[dict[str, str]] | None = None,
        wait_timeout_seconds: int = 180,
    ) -> dict[str, Any]:
        previous = self.admission.read().get("last_engine_snapshot", {})
        supports_piggyback_ack = isinstance(previous, dict) and previous.get(
            "return_export_protocol"
        ) in {
            "snapshot-ready-archive-v2",
            "snapshot-ready-archive-v3",
            "snapshot-ready-archive-v4",
        }
        sent_acknowledgements = (
            normalize_acknowledgements(acknowledgements or [])
            if supports_piggyback_ack
            else []
        )
        split_publish_and_wait = self.runner is subprocess.run
        command = self.snapshot_command(
            request_id=request_id,
            acknowledgements=sent_acknowledgements,
            wait_timeout_seconds=wait_timeout_seconds,
            wait=not split_publish_and_wait,
        )
        started = time.monotonic()
        completed = self._run(command)
        terminal_status: dict[str, Any] | None = None
        if split_publish_and_wait and completed.returncode == 0:
            terminal_status = self._wait_for_terminal_result(
                request_id,
                wait_timeout_seconds=wait_timeout_seconds,
            )
            if terminal_status is None:
                raise EngineTransportError(
                    "engine snapshot result did not settle before timeout: "
                    f"{request_id}"
                )
            terminal_state = str(terminal_status.get("state") or "")
            if terminal_state != "success":
                raise EngineTransportError(
                    "engine snapshot reached non-success terminal state "
                    f"{terminal_state or 'unknown'}: {request_id}"
                )
        elapsed = round(time.monotonic() - started, 3)
        output_root = self.output_root(request_id)
        recovered_after_wait_failure = False
        if completed.returncode != 0:
            status = find_named_object(output_root, "status.json")
            materialized_snapshot = find_named_object(
                output_root, "engine_status.json"
            )
            recovered_after_wait_failure = bool(
                isinstance(status, dict)
                and str(status.get("request_id") or "") == request_id
                and str(status.get("state") or "") == "success"
                and isinstance(materialized_snapshot, dict)
                and materialized_snapshot
            )
            if not recovered_after_wait_failure:
                raise EngineTransportError(
                    command_failure("engine snapshot", completed)
                )
        if recovered_after_wait_failure:
            local_transport_timeline = {
                "protocol_version": "gitpartner-local-timeline-v1",
                "request_id": request_id,
                "total_seconds": elapsed,
                "finished_at": utc_now_iso(),
                "steps": [
                    {
                        "name": (
                            "recover_materialized_terminal_output_after_"
                            "wait_failure"
                        ),
                        "duration_seconds": elapsed,
                        "outcome": "success",
                    }
                ],
            }
        else:
            local_transport_timeline = require_local_transport_timeline(
                completed,
                request_id=request_id,
                operation="engine snapshot",
            )
            if split_publish_and_wait:
                local_transport_timeline = dict(local_transport_timeline)
                steps = list(local_transport_timeline.get("steps", []))
                steps.append(
                    {
                        "name": "wait_terminal_on_result_lane",
                        "duration_seconds": elapsed,
                        "outcome": "success",
                    }
                )
                local_transport_timeline["steps"] = steps
                local_transport_timeline["total_seconds"] = elapsed
                local_transport_timeline["finished_at"] = utc_now_iso()
        snapshot = wait_for_value(
            lambda: find_named_object(output_root, "engine_status.json"),
            description="engine status",
            timeout_seconds=self.output_settle_timeout_seconds,
        )
        if snapshot is None:
            raise EngineTransportError(f"engine status not found under {output_root}")
        snapshot["transport_received_at"] = utc_now_iso()
        self.admission.reconcile_engine_snapshot(snapshot)
        ready = wait_for_named_list(
            output_root,
            "return_ready.json",
            timeout_seconds=self.output_settle_timeout_seconds,
        )
        terminal_records: list[dict[str, Any]] = []
        for manifest in ready:
            try:
                terminal_records.append(
                    self.admission.record_terminal_manifest(manifest)
                )
            except EngineAdmissionError:
                continue
        if snapshot.get("return_export_protocol") in {
            "snapshot-ready-archive-v1",
            "snapshot-ready-archive-v2",
            "snapshot-ready-archive-v3",
            "snapshot-ready-archive-v4",
        }:
            ready_cache = (
                self.root
                / "TestUtils"
                / "tester_daemon"
                / "engine_ready_cache"
                / hashlib.sha256(request_id.encode("utf-8")).hexdigest()[:16]
            )
            exported_ready = wait_for_value(
                lambda: ready_manifests_selected_by_export(output_root, ready),
                description="snapshot ready export selection",
                timeout_seconds=self.output_settle_timeout_seconds,
                accept=lambda value: isinstance(value, list),
            )
            if exported_ready:
                wait_for_value(
                    lambda: extract_settled_ready_archive(
                        output_root,
                        ready_cache,
                        exported_ready,
                    ),
                    description="snapshot ready archive",
                    timeout_seconds=self.output_settle_timeout_seconds,
                )
                ready_bundles = wait_for_value(
                    lambda: discover_ready_bundles(
                        output_root,
                        exported_ready,
                        bundle_root=ready_cache,
                        report_root=self.root,
                    ),
                    description="snapshot ready bundles",
                    timeout_seconds=self.output_settle_timeout_seconds,
                    accept=lambda value: isinstance(value, list)
                    and len(value) == len(exported_ready),
                )
            else:
                ready_bundles = []
        else:
            ready_bundles = []
        return {
            "engine_snapshot": snapshot,
            "return_ready": ready,
            "ready_bundles": ready_bundles,
            "terminal_records": terminal_records,
            "admission": self.admission.snapshot(engine_snapshot=snapshot),
            "snapshot_request_id": request_id,
            "acknowledgements_sent": sent_acknowledgements,
            "transport_elapsed_seconds": elapsed,
            "local_transport_timeline": local_transport_timeline,
            "recovered_after_wait_failure": recovered_after_wait_failure,
        }

    def collect(
        self,
        *,
        request_id: str,
        engine_job_id: str,
        receipt_id: str,
        wait_timeout_seconds: int = 180,
    ) -> dict[str, Any]:
        legacy_output = self.output_root(f"engine-collect-{engine_job_id}")
        compacted_state = returned_state_without_bundle(
            legacy_output, engine_job_id=engine_job_id
        )
        if compacted_state is not None:
            raise EngineReturnAlreadyCompactedError(
                "remote engine return was acknowledged and compacted before "
                f"local verification: {engine_job_id}; "
                f"returned_at={compacted_state.get('returned_at', '')}"
            )
        command = self.collect_command(
            request_id=request_id,
            engine_job_id=engine_job_id,
            receipt_id=receipt_id,
            wait_timeout_seconds=wait_timeout_seconds,
        )
        started = time.monotonic()
        completed = self._run(command)
        elapsed = round(time.monotonic() - started, 3)
        if completed.returncode != 0:
            raise EngineTransportError(command_failure("engine collect", completed))
        local_transport_timeline = require_local_transport_timeline(
            completed,
            request_id=request_id,
            operation="engine collect",
        )
        output_root = self.output_root(request_id)
        terminal = wait_for_value(
            lambda: find_correlated_json(output_root, "terminal.json", engine_job_id),
            description=f"terminal manifest for {engine_job_id}",
            timeout_seconds=self.output_settle_timeout_seconds,
        )
        if terminal is None:
            raise EngineTransportError(
                f"terminal manifest not found under {output_root}"
            )
        try:
            identity_evidence = verify_returned_identity(
                output_root,
                engine_job_id=engine_job_id,
                expected=terminal.get("input_identity"),
                terminal_state=str(terminal.get("state") or ""),
            )
        except EngineTransportError as exc:
            compacted_state = returned_state_without_bundle(
                output_root, engine_job_id=engine_job_id
            )
            if compacted_state is not None:
                raise EngineReturnAlreadyCompactedError(
                    "remote engine return was acknowledged and compacted before "
                    f"local verification: {engine_job_id}; "
                    f"returned_at={compacted_state.get('returned_at', '')}"
                ) from exc
            raise
        self.admission.record_terminal_manifest(terminal)
        returned = self.admission.record_return(engine_job_id, receipt_id)
        return {
            "terminal": terminal,
            "admission_record": returned,
            "identity_evidence": identity_evidence,
            "transport_elapsed_seconds": elapsed,
            "local_transport_timeline": local_transport_timeline,
        }

    def acknowledge(
        self,
        *,
        request_id: str,
        engine_job_id: str,
        receipt_id: str,
        wait: bool = False,
        wait_timeout_seconds: int = 180,
    ) -> dict[str, Any]:
        command = self.ack_command(
            request_id=request_id,
            engine_job_id=engine_job_id,
            receipt_id=receipt_id,
            wait=wait,
            wait_timeout_seconds=wait_timeout_seconds,
        )
        started = time.monotonic()
        completed = self._run(command)
        elapsed = round(time.monotonic() - started, 3)
        if completed.returncode != 0:
            raise EngineTransportError(command_failure("engine return ack", completed))
        result: dict[str, Any] = {
            "engine_job_id": engine_job_id,
            "receipt_id": receipt_id,
            "request_id": request_id,
            "dispatched_at": utc_now_iso(),
            "waited": bool(wait),
            "transport_elapsed_seconds": elapsed,
        }
        if wait:
            receipt = find_correlated_json(
                self.output_root(request_id), "return_receipt.json", engine_job_id
            )
            if receipt is None:
                raise EngineTransportError(
                    f"engine return receipt not found under {self.output_root(request_id)}"
                )
            if str(receipt.get("return_receipt_id") or "") != receipt_id:
                raise EngineTransportError(
                    f"engine return receipt mismatch for {engine_job_id}"
                )
            result["receipt"] = receipt
            result["confirmed_at"] = utc_now_iso()
        return result

    def configure(
        self,
        *,
        request_id: str,
        max_inflight: int,
        draining: bool,
        standby_slots: int | None = None,
        active_job_slots: int | None = None,
        host_slots: int | None = None,
        device_inventory: list[dict[str, Any]] | None = None,
        export_slots: int | None = None,
        return_backlog_soft_limit_bytes: int | None = None,
        return_backlog_hard_limit_bytes: int | None = None,
        return_backlog_soft_limit_jobs: int | None = None,
        return_backlog_hard_limit_jobs: int | None = None,
        wait_timeout_seconds: int = 180,
    ) -> dict[str, Any]:
        command = self.configure_command(
            request_id=request_id,
            max_inflight=max_inflight,
            draining=draining,
            standby_slots=standby_slots,
            active_job_slots=active_job_slots,
            host_slots=host_slots,
            device_inventory=device_inventory,
            export_slots=export_slots,
            return_backlog_soft_limit_bytes=return_backlog_soft_limit_bytes,
            return_backlog_hard_limit_bytes=return_backlog_hard_limit_bytes,
            return_backlog_soft_limit_jobs=return_backlog_soft_limit_jobs,
            return_backlog_hard_limit_jobs=return_backlog_hard_limit_jobs,
            wait_timeout_seconds=wait_timeout_seconds,
        )
        started = time.monotonic()
        completed = self._run(command)
        elapsed = round(time.monotonic() - started, 3)
        if completed.returncode != 0:
            raise EngineTransportError(command_failure("engine configure", completed))
        local_transport_timeline = require_local_transport_timeline(
            completed,
            request_id=request_id,
            operation="engine configure",
        )
        output_root = self.output_root(request_id)
        snapshot = wait_for_value(
            lambda: find_named_object(output_root, "engine_status.json"),
            description="configured engine status",
            timeout_seconds=self.output_settle_timeout_seconds,
        )
        if snapshot is None:
            raise EngineTransportError(
                f"configured engine status not found under {output_root}"
            )
        snapshot["transport_received_at"] = utc_now_iso()
        self.admission.configure(
            enabled=True,
            target_inflight=max(1, int(max_inflight)),
            draining=bool(draining),
        )
        self.admission.reconcile_engine_snapshot(snapshot)
        return {
            "engine_snapshot": snapshot,
            "admission": self.admission.snapshot(engine_snapshot=snapshot),
            "transport_elapsed_seconds": elapsed,
            "local_transport_timeline": local_transport_timeline,
        }

    def sync_code(
        self,
        *,
        request_id: str,
        wait_timeout_seconds: int = 900,
    ) -> dict[str, Any]:
        request_token = safe_token(request_id, "request_id")
        direct_runtime_sync = self._uses_direct_runtime_sync()
        expected_generation = (
            expected_remote_engine_code_generation(
                self.root,
                gitpartner_repo=self.gitpartner_repo,
            )
            if direct_runtime_sync
            else ""
        )
        if direct_runtime_sync and not expected_generation:
            raise EngineTransportError(
                "cannot compute the registered GitPartner Engine runtime generation"
            )
        output_root = self.gitpartner_repo / "output" / request_token
        immutable_request = (
            self.gitpartner_repo
            / "input"
            / "requests"
            / request_token
            / "job.json"
        )
        cached_status = find_named_object(output_root, "status.json")
        if immutable_request.is_file():
            request_record = read_object(immutable_request)
            request_mode = sync_code_request_mode(request_record)
            expected_mode = "direct-runtime" if direct_runtime_sync else "relay-lan"
            if request_mode != expected_mode:
                raise EngineTransportError(
                    "immutable engine sync request topology mismatch: "
                    f"expected={expected_mode} actual={request_mode}"
                )
            if (
                cached_status is not None
                and str(cached_status.get("state") or "")
                in {"success", "failed", "cancelled"}
            ):
                return self._sync_code_result(
                    request_id=request_id,
                    status=(
                        cached_status
                        if direct_runtime_sync
                        else merge_maintenance_request_evidence(
                            cached_status,
                            request_record,
                            request_id=request_token,
                        )
                    ),
                    direct_runtime_sync=direct_runtime_sync,
                    expected_generation=expected_generation,
                    receipt=(
                        self._wait_for_direct_sync_receipt(output_root)
                        if (
                            direct_runtime_sync
                            and str(cached_status.get("state") or "") == "success"
                        )
                        else None
                    ),
                    transport_elapsed_seconds=0.0,
                    idempotent_replay=True,
                )
            observed = self.query_existing_request_status(
                request_id=request_id,
            )
            return self._sync_code_result(
                request_id=request_id,
                status=(
                    observed["status"]
                    if direct_runtime_sync
                    else merge_maintenance_request_evidence(
                        observed["status"],
                        request_record,
                        request_id=request_token,
                    )
                ),
                direct_runtime_sync=direct_runtime_sync,
                expected_generation=expected_generation,
                receipt=(
                    self._wait_for_direct_sync_receipt(output_root)
                    if (
                        direct_runtime_sync
                        and str(observed["status"].get("state") or "")
                        == "success"
                    )
                    else None
                ),
                transport_elapsed_seconds=observed[
                    "transport_elapsed_seconds"
                ],
                idempotent_replay=True,
            )
        command = self.sync_code_command(
            request_id=request_id,
            wait_timeout_seconds=wait_timeout_seconds,
        )
        started = time.monotonic()
        completed = self._run(command)
        elapsed = round(time.monotonic() - started, 3)
        if completed.returncode != 0:
            raise EngineTransportError(command_failure("engine code sync", completed))
        status = wait_for_value(
            lambda: find_named_object(output_root, "status.json"),
            description="engine code sync status",
            timeout_seconds=self.output_settle_timeout_seconds,
        )
        if status is None:
            raise EngineTransportError(
                f"engine code sync status not found under {output_root}"
            )
        return self._sync_code_result(
            request_id=request_id,
            status=status,
            direct_runtime_sync=direct_runtime_sync,
            expected_generation=expected_generation,
            receipt=(
                self._wait_for_direct_sync_receipt(output_root)
                if direct_runtime_sync and str(status.get("state") or "") == "success"
                else None
            ),
            transport_elapsed_seconds=elapsed,
            idempotent_replay=False,
        )

    def _uses_direct_runtime_sync(self) -> bool:
        mode = self.transport_mode.lower()
        return bool(
            not self.gateway_id
            and (
                mode in {"direct", "direct-git"}
                or (not mode and self.transport.lower() == "direct")
            )
        )

    def _wait_for_direct_sync_receipt(
        self,
        output_root: Path,
    ) -> dict[str, Any] | None:
        return wait_for_value(
            lambda: find_named_object(
                output_root,
                "engine_runtime_sync_receipt.json",
            ),
            description="direct Engine runtime sync receipt",
            timeout_seconds=self.output_settle_timeout_seconds,
        )

    def query_existing_request_status(
        self,
        *,
        request_id: str,
    ) -> dict[str, Any]:
        request_token = safe_token(request_id, "request_id")
        if not self.result_channel:
            raise EngineTransportError(
                "existing request status query requires a result channel"
            )
        command = [
            sys.executable,
            "-m",
            "limited_remote_partner.gateway.batch_result_query",
            "--repo",
            str(self.gitpartner_repo),
            "--output-subdir",
            request_token,
            "--result-branch",
            self.result_channel,
        ]
        if self.control_channel:
            command.extend(["--control-branch", self.control_channel])
        started = time.monotonic()
        completed = self._run(command)
        elapsed = round(time.monotonic() - started, 3)
        if completed.returncode != 0:
            raise EngineTransportError(
                command_failure("existing maintenance status query", completed)
            )
        observed = parse_last_json_object(
            completed.stdout,
            description="existing maintenance status query",
        )
        raw_items = observed.get("items")
        if not isinstance(raw_items, list):
            raise EngineTransportError(
                "existing maintenance status query returned no items"
            )
        item = next(
            (
                candidate
                for candidate in raw_items
                if isinstance(candidate, dict)
                and str(candidate.get("request_id") or "") == request_token
            ),
            None,
        )
        status = (
            item.get("full_status") or item.get("status")
            if isinstance(item, dict)
            else None
        )
        if not isinstance(status, dict):
            raise EngineTransportError(
                "existing maintenance status query returned no status for "
                f"{request_token}"
            )
        return {
            "request_id": request_token,
            "status": status,
            "transport_elapsed_seconds": elapsed,
            "query": observed,
        }

    def query_existing_request_artifact(
        self,
        *,
        request_id: str,
        result_channel: str,
        result_template: str,
    ) -> dict[str, Any]:
        request_token = safe_token(request_id, "request_id")
        if not result_channel.strip():
            raise EngineTransportError(
                "existing request artifact query requires a result channel"
            )
        command = [
            sys.executable,
            "-m",
            "limited_remote_partner.gateway.batch_result_query",
            "--repo",
            str(self.gitpartner_repo),
            "--output-subdir",
            request_token,
            "--result-branch",
            result_channel,
            "--result-template",
            result_template,
        ]
        started = time.monotonic()
        completed = self._run(command)
        elapsed = round(time.monotonic() - started, 3)
        if completed.returncode != 0:
            raise EngineTransportError(
                command_failure("existing maintenance artifact query", completed)
            )
        observed = parse_last_json_object(
            completed.stdout,
            description="existing maintenance artifact query",
        )
        raw_items = observed.get("items")
        item = next(
            (
                candidate
                for candidate in raw_items
                if isinstance(candidate, dict)
                and str(candidate.get("request_id") or "") == request_token
            ),
            None,
        ) if isinstance(raw_items, list) else None
        result = item.get("result") if isinstance(item, dict) else None
        return {
            "request_id": request_token,
            "result": result if isinstance(result, dict) else None,
            "result_path": (
                str(item.get("result_path") or "")
                if isinstance(item, dict)
                else ""
            ),
            "transport_elapsed_seconds": elapsed,
            "query": observed,
        }

    def _sync_code_result(
        self,
        *,
        request_id: str,
        status: dict[str, Any],
        direct_runtime_sync: bool,
        expected_generation: str,
        receipt: dict[str, Any] | None,
        transport_elapsed_seconds: float,
        idempotent_replay: bool,
    ) -> dict[str, Any]:
        if str(status.get("request_id") or request_id) != request_id:
            raise EngineTransportError(
                "engine code sync returned the wrong request id"
            )
        if str(status.get("state") or "") != "success":
            raise EngineTransportError(
                "engine code sync did not succeed: "
                + json.dumps(status, sort_keys=True, separators=(",", ":"))[-2000:]
            )
        if direct_runtime_sync:
            if not isinstance(receipt, dict):
                raise EngineTransportError(
                    "direct Engine runtime sync returned no receipt"
                )
            receipt_schema = str(receipt.get("schema") or "")
            if (
                receipt_schema
                not in {
                    "gitpartner.direct-engine-code-sync.v1",
                    "gitpartner.direct-engine-code-sync.v2",
                }
                or str(receipt.get("state") or "") != "success"
            ):
                raise EngineTransportError(
                    "direct Engine runtime sync returned an invalid receipt"
                )
            actual_generation = str(
                receipt.get("engine_code_generation") or ""
            )
            if actual_generation != expected_generation:
                raise EngineTransportError(
                    "direct Engine runtime sync generation mismatch: "
                    f"expected={expected_generation} actual={actual_generation}"
                )
            if str(receipt.get("target_repo") or "") != self.remote_gitpartner_repo:
                raise EngineTransportError(
                    "direct Engine runtime sync targeted the wrong GP worktree"
                )
            if str(receipt.get("engine_root") or "") != self.engine_root:
                raise EngineTransportError(
                    "direct Engine runtime sync targeted the wrong Engine root"
                )
            if receipt_schema == "gitpartner.direct-engine-code-sync.v2":
                expected_runtime_source = (
                    f"{self.engine_root}/runtime/generations/"
                    f"{expected_generation}/src"
                )
                expected_runtime_pointer = (
                    f"{self.engine_root}/runtime/current"
                )
                if (
                    str(receipt.get("runtime_source") or "")
                    != expected_runtime_source
                    or str(receipt.get("runtime_pointer") or "")
                    != expected_runtime_pointer
                ):
                    raise EngineTransportError(
                        "direct Engine runtime sync returned the wrong "
                        "runtime overlay location"
                    )
            return {
                "request_id": request_id,
                "state": "success",
                "server_action": "direct-engine-runtime-sync",
                "target_role": "client",
                "engine_code_generation": actual_generation,
                "receipt": receipt,
                "status": status,
                "transport_elapsed_seconds": transport_elapsed_seconds,
                "idempotent_replay": idempotent_replay,
            }
        if str(status.get("server_action") or "") != "lan-sync-code":
            raise EngineTransportError(
                "engine code sync returned the wrong server action"
            )
        action_args = status.get("server_action_args")
        if (
            not isinstance(action_args, dict)
            or str(action_args.get("target_role") or "") != "client"
        ):
            raise EngineTransportError(
                "engine code sync returned the wrong target role"
            )
        return {
            "request_id": request_id,
            "state": "success",
            "server_action": "lan-sync-code",
            "target_role": "client",
            "status": status,
            "transport_elapsed_seconds": transport_elapsed_seconds,
            "idempotent_replay": idempotent_replay,
        }

    def restart_role(
        self,
        *,
        request_id: str,
        role: str,
        wait_timeout_seconds: int = 300,
    ) -> dict[str, Any]:
        command = self.restart_role_command(
            request_id=request_id,
            role=role,
            wait_timeout_seconds=wait_timeout_seconds,
        )
        started = time.monotonic()
        completed = self._run(command)
        elapsed = round(time.monotonic() - started, 3)
        if completed.returncode != 0:
            raise EngineTransportError(
                command_failure(f"restart {role} role", completed)
            )
        output_root = (
            self.gitpartner_repo / "output" / safe_token(request_id, "request_id")
        )
        status = wait_for_value(
            lambda: find_named_object(output_root, "status.json"),
            description=f"restart {role} role status",
            timeout_seconds=self.output_settle_timeout_seconds,
        )
        if status is None:
            raise EngineTransportError(f"restart {role} role status was not returned")
        if str(status.get("state") or "") != "success":
            raise EngineTransportError(
                f"restart {role} role did not succeed: "
                + json.dumps(status, sort_keys=True, separators=(",", ":"))[-2000:]
            )
        if str(status.get("server_action") or "") != "lan-restart-service":
            raise EngineTransportError(f"restart {role} role returned the wrong action")
        action_args = status.get("server_action_args")
        if (
            not isinstance(action_args, dict)
            or str(action_args.get("target_role") or "") != role
        ):
            raise EngineTransportError(f"restart {role} role returned the wrong target")
        return {
            "request_id": request_id,
            "state": "success",
            "server_action": "lan-restart-service",
            "target_role": role,
            "status": status,
            "transport_elapsed_seconds": elapsed,
        }

    def sync_resident_runtime(
        self,
        *,
        request_id: str,
        wait_timeout_seconds: int = 300,
        restart_delay_seconds: int = 90,
    ) -> dict[str, Any]:
        request_token = safe_token(request_id, "request_id")
        expected_generation = expected_remote_resident_code_generation(
            self.root,
            gitpartner_repo=self.gitpartner_repo,
        )
        command = self.sync_resident_runtime_command(
            request_id=request_token,
            expected_generation=expected_generation,
            wait_timeout_seconds=wait_timeout_seconds,
            restart_delay_seconds=restart_delay_seconds,
        )
        started = time.monotonic()
        completed = self._run(command)
        elapsed = round(time.monotonic() - started, 3)
        output_root = (
            self.gitpartner_repo / "output" / request_token
        )
        status = wait_for_value(
            lambda: find_named_object(output_root, "status.json"),
            description="resident runtime sync status",
            timeout_seconds=self.output_settle_timeout_seconds,
        )
        receipt = wait_for_value(
            lambda: find_resident_runtime_receipt(output_root, request_token),
            description="resident runtime sync receipt",
            timeout_seconds=self.output_settle_timeout_seconds,
        )
        adopted_terminal = completed.returncode != 0
        if completed.returncode != 0 and (
            status is None
            or receipt is None
            or str(status.get("state") or "") != "success"
            or str(receipt.get("state") or "") != "success"
        ):
            raise EngineTransportError(
                command_failure("resident runtime sync", completed)
            )
        if status is None or receipt is None:
            raise EngineTransportError(
                "resident runtime sync returned no status or receipt"
            )
        if str(status.get("request_id") or request_token) != request_token:
            raise EngineTransportError(
                "resident runtime sync returned the wrong request id"
            )
        if str(status.get("state") or "") != "success":
            raise EngineTransportError(
                "resident runtime sync did not succeed: "
                + json.dumps(status, sort_keys=True, separators=(",", ":"))[-2000:]
            )
        if (
            str(receipt.get("schema") or "")
            != "git-partner.resident-runtime-sync.v1"
            or str(receipt.get("state") or "") != "success"
            or str(receipt.get("expected_generation") or "")
            != expected_generation
        ):
            raise EngineTransportError(
                "resident runtime sync returned an invalid receipt"
            )
        expected_target_repo = (
            f"{self.remote_root.rstrip('/')}/{self.remote_gitpartner_repo}"
        )
        expected_config = (
            f"{expected_target_repo}/.partner_state/endpoints/"
            f"{self.endpoint_id}/effective-client.json"
        )
        if (
            str(receipt.get("request_id") or "") != request_token
            or str(receipt.get("target_repo") or "") != expected_target_repo
            or str(receipt.get("config_path") or "") != expected_config
        ):
            raise EngineTransportError(
                "resident runtime sync returned the wrong target identity"
            )
        return {
            "request_id": request_token,
            "state": "success",
            "server_action": "direct-resident-runtime-sync",
            "target_role": "client",
            "resident_code_generation": expected_generation,
            "receipt": receipt,
            "status": status,
            "transport_elapsed_seconds": elapsed,
            "launch_wait_timed_out": adopted_terminal,
        }

    def reconcile_direct_request(
        self,
        *,
        request_id: str,
        target_request_id: str,
        wait_timeout_seconds: int = 180,
    ) -> dict[str, Any]:
        request_token = safe_token(request_id, "request_id")
        target_token = safe_token(target_request_id, "target_request_id")
        target_job = (
            self.gitpartner_repo
            / "input"
            / "requests"
            / target_token
            / "job.json"
        )
        if not target_job.is_file():
            raise EngineTransportError(
                f"reconciled direct request is missing: {target_job}"
            )
        target_job_sha256 = hashlib.sha256(target_job.read_bytes()).hexdigest()
        command = self.reconcile_direct_request_command(
            request_id=request_token,
            target_request_id=target_token,
            target_job_sha256=target_job_sha256,
            wait_timeout_seconds=wait_timeout_seconds,
        )
        started = time.monotonic()
        completed = self._run(command)
        elapsed = round(time.monotonic() - started, 3)
        output_root = self.gitpartner_repo / "output" / request_token
        status = wait_for_value(
            lambda: find_named_object(output_root, "status.json"),
            description="direct request reconciliation status",
            timeout_seconds=self.output_settle_timeout_seconds,
        )
        adopted_terminal = completed.returncode != 0
        if completed.returncode != 0 and (
            status is None or str(status.get("state") or "") != "success"
        ):
            raise EngineTransportError(
                command_failure("direct request reconciliation", completed)
            )
        if status is None:
            raise EngineTransportError(
                "direct request reconciliation returned no status"
            )
        action_args = status.get("server_action_args")
        sandbox = status.get("sandbox")
        reconcile = (
            sandbox.get("reconcile")
            if isinstance(sandbox, dict)
            else None
        )
        if (
            str(status.get("state") or "") != "success"
            or str(status.get("request_id") or "") != request_token
            or str(status.get("server_action") or "")
            != "lan-reconcile-request"
            or not isinstance(action_args, dict)
            or str(action_args.get("reconcile_request_id") or "")
            != target_token
            or str(action_args.get("reconcile_job_sha256") or "")
            != target_job_sha256
            or not isinstance(sandbox, dict)
            or str(sandbox.get("profile") or "")
            != "local-request-reconcile"
            or not isinstance(reconcile, dict)
            or str(reconcile.get("target_request_id") or "")
            != target_token
            or str(reconcile.get("target_job_sha256") or "")
            != target_job_sha256
            or str(reconcile.get("state") or "")
            not in {"scheduled", "already-active", "already-terminal"}
        ):
            raise EngineTransportError(
                "direct request reconciliation returned an invalid result: "
                + json.dumps(status, sort_keys=True, separators=(",", ":"))[-3000:]
            )
        return {
            "request_id": request_token,
            "state": "success",
            "server_action": "lan-reconcile-request",
            "target_role": "client",
            "target_request_id": target_token,
            "target_job_sha256": target_job_sha256,
            "reconcile_state": str(reconcile["state"]),
            "status": status,
            "transport_elapsed_seconds": elapsed,
            "launch_wait_timed_out": adopted_terminal,
        }

    def stage_cann90_media(
        self,
        *,
        request_id: str,
        wait_timeout_seconds: int = 1800,
        poll_seconds: float = 5.0,
    ) -> dict[str, Any]:
        request_token = safe_token(request_id, "request_id")
        output_root = self.gitpartner_repo / "output" / request_token
        immutable_request = (
            self.gitpartner_repo
            / "input"
            / "requests"
            / request_token
            / "job.json"
        )
        request_preexisting = immutable_request.is_file()
        launched = request_preexisting
        launch_wait_timed_out = False
        transport_elapsed = 0.0
        if not launched:
            command = self.stage_cann90_media_command(
                request_id=request_token,
                wait_timeout_seconds=min(max(30, wait_timeout_seconds), 300),
            )
            started = time.monotonic()
            completed = self._run(command)
            transport_elapsed = round(time.monotonic() - started, 3)
            if completed.returncode != 0:
                # submit_job waits for a terminal status, while
                # server-tmux-command intentionally returns a durable running
                # request before a multi-gigabyte download finishes. Once the
                # immutable request exists, the status channel is authoritative
                # and the client must keep polling the same request instead of
                # publishing a duplicate.
                if not immutable_request.is_file():
                    raise EngineTransportError(
                        command_failure(
                            "CANN 9.0 gateway media staging", completed
                        )
                    )
                cached_status = find_named_object(output_root, "status.json")
                cached_state = str(
                    (cached_status or {}).get("state") or ""
                ).strip()
                if cached_state in {"failed", "cancelled"}:
                    raise EngineTransportError(
                        "CANN 9.0 gateway media staging was rejected: "
                        + json.dumps(
                            cached_status,
                            sort_keys=True,
                            separators=(",", ":"),
                        )[-2000:]
                    )
                launched = True
                launch_wait_timed_out = True

        deadline = time.monotonic() + max(30, int(wait_timeout_seconds))
        last_status: dict[str, Any] = {}
        last_query_error = ""
        while time.monotonic() < deadline:
            try:
                observed = self.query_existing_request_status(
                    request_id=request_token
                )
            except EngineTransportError as exc:
                last_query_error = str(exc)
                cached_status = find_named_object(output_root, "status.json")
                if cached_status is not None:
                    last_status = cached_status
            else:
                last_query_error = ""
                last_status = observed["status"]
            if str(last_status.get("state") or "") in {"failed", "cancelled"}:
                raise EngineTransportError(
                    "CANN 9.0 gateway media staging failed before completion: "
                    + json.dumps(
                        last_status,
                        sort_keys=True,
                        separators=(",", ":"),
                    )[-2000:]
                )
            try:
                tmux_observation = self.query_existing_request_artifact(
                    request_id=request_token,
                    result_channel="main",
                    result_template="tmux_status.json",
                )
                tmux_status = tmux_observation["result"]
            except EngineTransportError as exc:
                tmux_status = None
                last_query_error = str(exc)
            if tmux_status is not None:
                if str(tmux_status.get("state") or "") != "success":
                    raise EngineTransportError(
                        "CANN 9.0 gateway media staging failed: "
                        + json.dumps(
                            tmux_status,
                            sort_keys=True,
                            separators=(",", ":"),
                        )[-2000:]
                    )
                if int(tmux_status.get("exit_code", -1)) != 0:
                    raise EngineTransportError(
                        "CANN 9.0 gateway media staging returned a nonzero "
                        "tmux exit code"
                    )
                return {
                    "request_id": request_token,
                    "state": "success",
                    "server_action": "server-tmux-command",
                    "target_role": "server",
                    "transport_elapsed_seconds": transport_elapsed,
                    "idempotent_replay": request_preexisting,
                    "launch_wait_timed_out": launch_wait_timed_out,
                    "status": last_status,
                    "tmux_status": tmux_status,
                    "completion_evidence": (
                        "A-side tmux_status.json reports state=success and "
                        "exit_code=0; the fixed staging script emits its "
                        "completion marker only after size/hash manifest work"
                    ),
                }
            time.sleep(max(0.25, float(poll_seconds)))

        return {
            "request_id": request_token,
            "state": "running",
            "server_action": "server-tmux-command",
            "target_role": "server",
            "transport_elapsed_seconds": transport_elapsed,
            "idempotent_replay": request_preexisting,
            "launch_wait_timed_out": launch_wait_timed_out,
            "status": last_status,
            "last_query_error": last_query_error,
            "next_action": (
                "query the same request_id; do not publish another download"
            ),
        }

    def sync_cann90_media_to_client(
        self,
        *,
        request_id: str,
        wait_timeout_seconds: int = 4200,
    ) -> dict[str, Any]:
        request_token = safe_token(request_id, "request_id")
        immutable_request = (
            self.gitpartner_repo
            / "input"
            / "requests"
            / request_token
            / "job.json"
        )
        request_preexisting = immutable_request.is_file()
        transport_elapsed = 0.0
        if not request_preexisting:
            command = self.sync_cann90_media_to_client_command(
                request_id=request_token,
                wait_timeout_seconds=wait_timeout_seconds,
            )
            started = time.monotonic()
            completed = self._run(command)
            transport_elapsed = round(time.monotonic() - started, 3)
            if completed.returncode != 0:
                raise EngineTransportError(
                    command_failure(
                        "CANN 9.0 A-to-910B artifact sync", completed
                    )
                )
        if not immutable_request.is_file():
            raise EngineTransportError(
                "CANN 9.0 artifact sync did not persist its immutable request"
            )
        request_record = read_object(immutable_request)
        observed = self.query_existing_request_status(request_id=request_token)
        status = merge_maintenance_request_evidence(
            observed["status"],
            request_record,
            request_id=request_token,
        )
        if str(status.get("state") or "") != "success":
            raise EngineTransportError(
                "CANN 9.0 A-to-910B artifact sync did not succeed: "
                + json.dumps(
                    status, sort_keys=True, separators=(",", ":")
                )[-2000:]
            )
        if str(status.get("server_action") or "") != "lan-sync-artifact":
            raise EngineTransportError(
                "CANN 9.0 artifact sync returned the wrong server action"
            )
        action_args = status.get("server_action_args")
        if (
            not isinstance(action_args, dict)
            or str(action_args.get("target_role") or "") != "client"
            or str(action_args.get("artifact_profile") or "")
            != "cann90-910b-media"
        ):
            raise EngineTransportError(
                "CANN 9.0 artifact sync returned the wrong profile or target"
            )
        return {
            "request_id": request_token,
            "state": "success",
            "server_action": "lan-sync-artifact",
            "artifact_profile": "cann90-910b-media",
            "target_role": "client",
            "transport_elapsed_seconds": (
                transport_elapsed
                + float(observed["transport_elapsed_seconds"])
            ),
            "idempotent_replay": request_preexisting,
            "status": status,
        }

    def inspect_server_request(
        self,
        *,
        request_id: str,
        target_request_id: str,
        wait_timeout_seconds: int = 180,
    ) -> dict[str, Any]:
        command = self.inspect_server_request_command(
            request_id=request_id,
            target_request_id=target_request_id,
            wait_timeout_seconds=wait_timeout_seconds,
        )
        started = time.monotonic()
        completed = self._run(command)
        elapsed = round(time.monotonic() - started, 3)
        if completed.returncode != 0:
            raise EngineTransportError(
                command_failure("server request inspection", completed)
            )
        text = completed.stdout + "\n" + completed.stderr
        return {
            "request_id": safe_token(request_id, "request_id"),
            "target_request_id": safe_token(
                target_request_id, "target_request_id"
            ),
            "state": "success",
            "server_action": "lan-diagnose",
            "transport_elapsed_seconds": elapsed,
            "tmux_status_present": "REQUEST_TMUX_STATUS_START" in text,
            "tmux_log_present": "REQUEST_TMUX_LOG_START" in text,
            "output_tail": text[-16000:],
        }

    def acknowledge_node(
        self,
        *,
        request_id: str,
        ack_path: Path,
        wait_timeout_seconds: int = 180,
    ) -> dict[str, Any]:
        command = self.node_ack_command(
            request_id=request_id,
            ack_path=ack_path,
            wait_timeout_seconds=wait_timeout_seconds,
        )
        started = time.monotonic()
        completed = self._run(command)
        elapsed = round(time.monotonic() - started, 3)
        if completed.returncode != 0:
            raise EngineTransportError(
                command_failure("node acknowledgement", completed)
            )
        output_root = (
            self.gitpartner_repo / "output" / safe_token(request_id, "request_id")
        )
        status = wait_for_value(
            lambda: find_named_object(output_root, "status.json"),
            description="node acknowledgement status",
            timeout_seconds=self.output_settle_timeout_seconds,
        )
        if status is None:
            raise EngineTransportError("node acknowledgement status was not returned")
        sandbox = status.get("sandbox")
        sandbox_profile = (
            str(sandbox.get("profile") or "")
            if isinstance(sandbox, dict)
            else ""
        )
        trusted_ack_result = (
            str(status.get("server_action") or "") == "lan-node-ack"
            or sandbox_profile == "local-node-ack"
        )
        if (
            str(status.get("state") or "") != "success"
            or str(status.get("request_id") or "") != request_id
            or not trusted_ack_result
        ):
            raise EngineTransportError(
                "node acknowledgement did not succeed: "
                + json.dumps(status, sort_keys=True, separators=(",", ":"))[-2000:]
            )
        return {
            "request_id": request_id,
            "state": "success",
            "server_action": "lan-node-ack",
            "target_role": "client",
            "status": status,
            "transport_elapsed_seconds": elapsed,
        }

    def accept_command(
        self,
        spec_path: Path,
        *,
        request_id: str,
        engine_job_id: str,
        wait_timeout_seconds: int = 180,
        payload_root: Path | None = None,
    ) -> list[str]:
        command = [
            sys.executable,
            "-m",
            "limited_remote_partner.gateway.submit_job",
            "--commit-push",
            *(["--append-request"] if self.append_requests else []),
            "--wait",
            "--wait-timeout-seconds",
            str(wait_timeout_seconds),
            "ascendop-engine-accept",
            "--transport",
            self.transport,
            "--request-id",
            request_id,
            "--client-work-dir",
            self.remote_root,
            "--engine-root",
            self.engine_root,
            "--engine-job-id",
            engine_job_id,
            "--spec",
            str(spec_path.resolve()),
        ]
        if payload_root is not None:
            command.extend(["--payload-root", str(payload_root.resolve())])
        command.extend(self._target_command_args())
        return command

    def snapshot_command(
        self,
        *,
        request_id: str,
        acknowledgements: list[dict[str, str]] | None = None,
        wait_timeout_seconds: int = 180,
        wait: bool = True,
    ) -> list[str]:
        command = [
            sys.executable,
            "-m",
            "limited_remote_partner.gateway.submit_job",
            "--commit-push",
            *(["--append-request"] if self.append_requests else []),
            *(["--wait", "--wait-timeout-seconds", str(wait_timeout_seconds)] if wait else []),
            "ascendop-engine-snapshot",
            "--transport",
            self.transport,
            "--request-id",
            request_id,
            "--client-work-dir",
            self.remote_root,
            "--engine-root",
            self.engine_root,
        ]
        for item in normalize_acknowledgements(acknowledgements or []):
            command.extend(
                [
                    "--ack-return",
                    f"{item['engine_job_id']}={item['receipt_id']}",
                ]
            )
        command.extend(self._target_command_args())
        return command

    def _wait_for_terminal_result(
        self,
        request_id: str,
        *,
        wait_timeout_seconds: int,
    ) -> dict[str, Any] | None:
        deadline = time.monotonic() + max(0, int(wait_timeout_seconds))
        while True:
            observed = self._run_persistent_result_query([request_id])
            for item in observed.get("items", []):
                if not isinstance(item, dict):
                    continue
                if str(item.get("request_id") or "") != request_id:
                    continue
                status = item.get("status")
                if not isinstance(status, dict):
                    continue
                if str(status.get("state") or "") in {
                    "success",
                    "failed",
                    "cancelled",
                }:
                    return status
            if time.monotonic() >= deadline:
                return None
            time.sleep(min(0.5, max(0.01, deadline - time.monotonic())))

    def exchange_command(
        self,
        manifest_path: Path,
        *,
        request_id: str,
        acknowledgements: list[dict[str, str]],
        required_acknowledgements: list[dict[str, str]],
        max_inflight: int,
        draining: bool,
        standby_slots: int | None,
        active_job_slots: int | None,
        host_slots: int | None,
        device_inventory: list[dict[str, Any]] | None,
        export_slots: int | None,
        return_backlog_soft_limit_bytes: int | None,
        return_backlog_hard_limit_bytes: int | None,
        return_backlog_soft_limit_jobs: int | None,
        return_backlog_hard_limit_jobs: int | None,
        wait_timeout_seconds: int,
        wait: bool = True,
        keep_existing_output: bool = False,
        wait_ready_seconds: float | None = None,
    ) -> list[str]:
        command = [
            sys.executable,
            "-m",
            "limited_remote_partner.gateway.submit_job",
            "--commit-push",
            *(["--append-request"] if self.append_requests else []),
            *(["--keep-existing-output"] if keep_existing_output else []),
            *(["--wait"] if wait else []),
            *(
                ["--wait-timeout-seconds", str(wait_timeout_seconds)]
                if wait
                else []
            ),
            "ascendop-engine-exchange",
            "--transport",
            self.transport,
            "--request-id",
            request_id,
            "--client-work-dir",
            self.remote_root,
            "--engine-root",
            self.engine_root,
            "--manifest",
            str(manifest_path.resolve()),
            "--max-inflight",
            str(max(1, int(max_inflight))),
            "--wait-ready-seconds",
            str(
                self.exchange_wait_ready_seconds
                if wait_ready_seconds is None
                else max(0.0, float(wait_ready_seconds))
            ),
            "--drain" if draining else "--resume",
        ]
        for item in normalize_acknowledgements(acknowledgements):
            command.extend(
                ["--ack-return", f"{item['engine_job_id']}={item['receipt_id']}"]
            )
        for item in normalize_acknowledgements(required_acknowledgements):
            command.extend(
                ["--ack-required", f"{item['engine_job_id']}={item['receipt_id']}"]
            )
        if standby_slots is not None:
            command.extend(["--standby-slots", str(max(0, int(standby_slots)))])
        if active_job_slots is not None:
            command.extend(["--active-job-slots", str(max(1, int(active_job_slots)))])
        if host_slots is not None:
            command.extend(["--host-slots", str(max(1, int(host_slots)))])
        if device_inventory is not None:
            command.extend(
                [
                    "--device-inventory-json",
                    json.dumps(device_inventory, ensure_ascii=True, separators=(",", ":")),
                ]
            )
        if export_slots is not None:
            command.extend(["--export-slots", str(max(1, int(export_slots)))])
        for option, candidate in (
            ("return-backlog-soft-limit-bytes", return_backlog_soft_limit_bytes),
            ("return-backlog-hard-limit-bytes", return_backlog_hard_limit_bytes),
            ("return-backlog-soft-limit-jobs", return_backlog_soft_limit_jobs),
            ("return-backlog-hard-limit-jobs", return_backlog_hard_limit_jobs),
        ):
            if candidate is not None:
                command.extend([f"--{option}", str(max(0, int(candidate)))])
        command.extend(self._target_command_args())
        return command

    def collect_command(
        self,
        *,
        request_id: str,
        engine_job_id: str,
        receipt_id: str,
        wait_timeout_seconds: int,
    ) -> list[str]:
        command = [
            sys.executable,
            "-m",
            "limited_remote_partner.gateway.submit_job",
            "--commit-push",
            *(["--append-request"] if self.append_requests else []),
            "--wait",
            "--wait-timeout-seconds",
            str(wait_timeout_seconds),
            "ascendop-engine-collect",
            "--transport",
            self.transport,
            "--request-id",
            request_id,
            "--client-work-dir",
            self.remote_root,
            "--engine-root",
            self.engine_root,
            "--engine-job-id",
            engine_job_id,
            "--receipt-id",
            receipt_id,
        ]
        command.extend(self._target_command_args())
        return command

    def ack_command(
        self,
        *,
        request_id: str,
        engine_job_id: str,
        receipt_id: str,
        wait: bool,
        wait_timeout_seconds: int,
    ) -> list[str]:
        command = [
            sys.executable,
            "-m",
            "limited_remote_partner.gateway.submit_job",
            "--commit-push",
            *(["--append-request"] if self.append_requests else []),
        ]
        if wait:
            command.extend(
                ["--wait", "--wait-timeout-seconds", str(wait_timeout_seconds)]
            )
        command.extend(
            [
                "ascendop-engine-collect",
                "--transport",
                self.transport,
                "--request-id",
                request_id,
                "--client-work-dir",
                self.remote_root,
                "--engine-root",
                self.engine_root,
                "--engine-job-id",
                engine_job_id,
                "--receipt-id",
                receipt_id,
                "--ack-only",
            ]
        )
        command.extend(self._target_command_args())
        return command

    def configure_command(
        self,
        *,
        request_id: str,
        max_inflight: int,
        draining: bool,
        host_slots: int | None,
        device_inventory: list[dict[str, Any]] | None = None,
        export_slots: int | None,
        standby_slots: int | None = None,
        active_job_slots: int | None = None,
        return_backlog_soft_limit_bytes: int | None = None,
        return_backlog_hard_limit_bytes: int | None = None,
        return_backlog_soft_limit_jobs: int | None = None,
        return_backlog_hard_limit_jobs: int | None = None,
        wait_timeout_seconds: int,
    ) -> list[str]:
        command = [
            sys.executable,
            "-m",
            "limited_remote_partner.gateway.submit_job",
            "--commit-push",
            *(["--append-request"] if self.append_requests else []),
            "--wait",
            "--wait-timeout-seconds",
            str(wait_timeout_seconds),
            "ascendop-engine-configure",
            "--transport",
            self.transport,
            "--request-id",
            request_id,
            "--client-work-dir",
            self.remote_root,
            "--engine-root",
            self.engine_root,
            "--max-inflight",
            str(max(1, int(max_inflight))),
            "--drain" if draining else "--resume",
        ]
        if standby_slots is not None:
            command.extend(["--standby-slots", str(max(0, int(standby_slots)))])
        if active_job_slots is not None:
            command.extend(["--active-job-slots", str(max(1, int(active_job_slots)))])
        if host_slots is not None:
            command.extend(["--host-slots", str(max(1, int(host_slots)))])
        if device_inventory is not None:
            command.extend(
                [
                    "--device-inventory-json",
                    json.dumps(device_inventory, ensure_ascii=True, separators=(",", ":")),
                ]
            )
        if export_slots is not None:
            command.extend(["--export-slots", str(max(1, int(export_slots)))])
        for option, value in (
            ("return-backlog-soft-limit-bytes", return_backlog_soft_limit_bytes),
            ("return-backlog-hard-limit-bytes", return_backlog_hard_limit_bytes),
            ("return-backlog-soft-limit-jobs", return_backlog_soft_limit_jobs),
            ("return-backlog-hard-limit-jobs", return_backlog_hard_limit_jobs),
        ):
            if value is not None:
                command.extend([f"--{option}", str(max(0, int(value)))])
        command.extend(self._target_command_args())
        return command

    def _target_command_args(self) -> list[str]:
        values: list[str] = []
        if self.node_id:
            values.extend(["--target-node", self.node_id])
        if self.endpoint_id:
            values.extend(["--target-endpoint-id", self.endpoint_id])
        if self.execution_environment_id:
            values.extend(
                ["--target-environment-id", self.execution_environment_id]
            )
        if self.gateway_id:
            values.extend(["--target-gateway-id", self.gateway_id])
        if self.transport_mode:
            values.extend(["--target-transport-mode", self.transport_mode])
        if self.registration_generation:
            values.extend(
                ["--registration-generation", self.registration_generation]
            )
        return values

    def sync_code_command(
        self,
        *,
        request_id: str,
        wait_timeout_seconds: int = 900,
    ) -> list[str]:
        if self._uses_direct_runtime_sync():
            expected_generation = expected_remote_engine_code_generation(
                self.root,
                gitpartner_repo=self.gitpartner_repo,
            )
            if not expected_generation:
                raise EngineTransportError(
                    "cannot compute the registered GitPartner Engine runtime generation"
                )
            command = [
                sys.executable,
                "-m",
                "limited_remote_partner.gateway.submit_job",
                "--commit-push",
                "--append-request",
                "--wait",
                "--wait-timeout-seconds",
                str(wait_timeout_seconds),
                "ascendop-engine-runtime-sync",
                "--request-id",
                safe_token(request_id, "request_id"),
                "--client-work-dir",
                self.remote_root,
                "--engine-root",
                self.engine_root,
                "--target-repo",
                self.remote_gitpartner_repo,
                "--expected-generation",
                expected_generation,
            ]
            command.extend(self._target_command_args())
            return command
        command = [
            sys.executable,
            "-m",
            "limited_remote_partner.gateway.submit_job",
            "--commit-push",
            "--append-request",
            "--publish-maintenance-changes",
            "--wait",
            "--wait-timeout-seconds",
            str(wait_timeout_seconds),
            "lan-bootstrap",
            "--request-id",
            safe_token(request_id, "request_id"),
            "--client-work-dir",
            self.remote_root,
            "--action",
            "lan-sync-code",
            "--target-role",
            "client",
            "--remote-config",
            self._registered_remote_config("client"),
        ]
        command.extend(self._target_command_args())
        return command

    def restart_role_command(
        self,
        *,
        request_id: str,
        role: str,
        wait_timeout_seconds: int = 300,
    ) -> list[str]:
        if role not in {"client", "server"}:
            raise EngineTransportError("restart role must be client or server")
        command = [
            sys.executable,
            "-m",
            "limited_remote_partner.gateway.submit_job",
            "--commit-push",
            "--append-request",
            "--wait",
            "--wait-timeout-seconds",
            str(wait_timeout_seconds),
            "lan-bootstrap",
            "--request-id",
            safe_token(request_id, "request_id"),
            "--target-dir",
            self.remote_root,
            "--action",
            "lan-restart-service",
            "--target-role",
            role,
            "--remote-config",
            self._registered_remote_config(role),
        ]
        command.extend(self._target_command_args())
        return command

    def sync_resident_runtime_command(
        self,
        *,
        request_id: str,
        expected_generation: str,
        wait_timeout_seconds: int = 300,
        restart_delay_seconds: int = 90,
    ) -> list[str]:
        if not self._uses_direct_runtime_sync():
            raise EngineTransportError(
                "resident runtime sync requires a registered direct endpoint"
            )
        if not self.endpoint_id:
            raise EngineTransportError(
                "resident runtime sync requires an endpoint id"
            )
        resident_config = (
            f"{self.remote_gitpartner_repo}/.partner_state/endpoints/"
            f"{self.endpoint_id}/effective-client.json"
        )
        command = [
            sys.executable,
            "-m",
            "limited_remote_partner.gateway.submit_job",
            "--commit-push",
            "--append-request",
            "--wait",
            "--wait-timeout-seconds",
            str(wait_timeout_seconds),
            "ascendop-resident-runtime-sync",
            "--request-id",
            safe_token(request_id, "request_id"),
            "--client-work-dir",
            self.remote_root,
            "--target-repo",
            self.remote_gitpartner_repo,
            "--resident-config",
            resident_config,
            "--expected-generation",
            expected_generation,
            "--restart-delay-seconds",
            str(max(30, int(restart_delay_seconds))),
        ]
        command.extend(self._target_command_args())
        return command

    def reconcile_direct_request_command(
        self,
        *,
        request_id: str,
        target_request_id: str,
        target_job_sha256: str,
        wait_timeout_seconds: int = 180,
    ) -> list[str]:
        if not self._uses_direct_runtime_sync():
            raise EngineTransportError(
                "direct request reconciliation requires a registered direct endpoint"
            )
        if not self.endpoint_id or not self.registration_generation:
            raise EngineTransportError(
                "direct request reconciliation requires endpoint identity and generation"
            )
        digest = str(target_job_sha256).strip().lower()
        if len(digest) != 64 or any(
            char not in "0123456789abcdef" for char in digest
        ):
            raise EngineTransportError(
                "direct request reconciliation requires a SHA-256 job digest"
            )
        command = [
            sys.executable,
            "-m",
            "limited_remote_partner.gateway.submit_job",
            "--commit-push",
            "--append-request",
            "--wait",
            "--wait-timeout-seconds",
            str(max(30, int(wait_timeout_seconds))),
            "lan-bootstrap",
            "--request-id",
            safe_token(request_id, "request_id"),
            "--action",
            "lan-reconcile-request",
            "--target-role",
            "client",
            "--remote-config",
            self._registered_remote_config("client"),
            "--reconcile-request-id",
            safe_token(target_request_id, "target_request_id"),
            "--reconcile-job-sha256",
            digest,
        ]
        command.extend(self._target_command_args())
        return command

    def stage_cann90_media_command(
        self,
        *,
        request_id: str,
        wait_timeout_seconds: int = 300,
    ) -> list[str]:
        script = (
            self.root
            / "tools"
            / "tester_daemon"
            / "scripts"
            / "stage_cann90_media_on_gateway.sh"
        )
        if not script.is_file():
            raise EngineTransportError(
                f"CANN 9.0 gateway staging script is missing: {script}"
            )
        command = [
            sys.executable,
            "-m",
            "limited_remote_partner.gateway.submit_job",
            "--commit-push",
            "--append-request",
            "--wait",
            "--wait-timeout-seconds",
            str(max(30, int(wait_timeout_seconds))),
            "lan-bootstrap",
            "--request-id",
            safe_token(request_id, "request_id"),
            "--action",
            "server-tmux-command",
            "--target-role",
            "server",
            "--remote-config",
            self._registered_remote_config("server"),
            "--tmux-session",
            "ascendop-cann90-media",
            "--script-file",
            str(script),
        ]
        command.extend(self._target_command_args())
        return command

    def sync_cann90_media_to_client_command(
        self,
        *,
        request_id: str,
        wait_timeout_seconds: int = 4200,
    ) -> list[str]:
        command = [
            sys.executable,
            "-m",
            "limited_remote_partner.gateway.submit_job",
            "--commit-push",
            "--append-request",
            "--wait",
            "--wait-timeout-seconds",
            str(max(300, int(wait_timeout_seconds))),
            "lan-bootstrap",
            "--request-id",
            safe_token(request_id, "request_id"),
            "--action",
            "lan-sync-artifact",
            "--artifact-profile",
            "cann90-910b-media",
            "--target-role",
            "client",
            "--remote-config",
            self._registered_remote_config("client"),
        ]
        command.extend(self._target_command_args())
        return command

    def inspect_server_request_command(
        self,
        *,
        request_id: str,
        target_request_id: str,
        wait_timeout_seconds: int = 180,
    ) -> list[str]:
        command = [
            sys.executable,
            "-m",
            "limited_remote_partner.gateway.submit_job",
            "--commit-push",
            "--append-request",
            "--wait",
            "--wait-timeout-seconds",
            str(max(30, int(wait_timeout_seconds))),
            "lan-bootstrap",
            "--request-id",
            safe_token(request_id, "request_id"),
            "--action",
            "lan-diagnose",
            "--diagnose-request-id",
            safe_token(target_request_id, "target_request_id"),
            "--target-role",
            "server",
            "--remote-config",
            self._registered_remote_config("server"),
        ]
        command.extend(self._target_command_args())
        return command

    def node_ack_command(
        self,
        *,
        request_id: str,
        ack_path: Path,
        wait_timeout_seconds: int = 180,
    ) -> list[str]:
        resolved = ack_path.resolve()
        if not resolved.is_file():
            raise EngineTransportError(
                f"node acknowledgement does not exist: {resolved}"
            )
        command = [
            sys.executable,
            "-m",
            "limited_remote_partner.gateway.submit_job",
            "--commit-push",
            "--append-request",
            "--wait",
            "--wait-timeout-seconds",
            str(wait_timeout_seconds),
            "lan-bootstrap",
            "--request-id",
            safe_token(request_id, "request_id"),
            "--action",
            "lan-node-ack",
            "--target-role",
            "client",
            "--remote-config",
            self._registered_remote_config("client"),
            "--node-ack-file",
            str(resolved),
        ]
        command.extend(self._target_command_args())
        return command

    def _registered_remote_config(self, role: str) -> str:
        if role == "client" and self.endpoint_id and not self.node_id:
            return (
                f"{self.remote_gitpartner_repo}/.partner_state/endpoints/"
                f"{self.endpoint_id}/effective-client.json"
            )
        if role == "client" and self.node_id:
            return f"configs/runtime/{self.node_id}.json"
        if role == "server" and self.gateway_id:
            return f"configs/runtime/{self.gateway_id}.json"
        return "configs/server.json" if role == "server" else "configs/partner.json"

    def output_root(self, request_id: str) -> Path:
        return self.gitpartner_repo / "output" / "engine-demo" / request_id

    def _run_persistent_result_query(
        self,
        request_ids: list[str],
    ) -> dict[str, Any]:
        request = {
            "output_subdirs": [
                f"engine-demo/{request_id}" for request_id in request_ids
            ],
            "result_templates": [],
            "wait_seconds": 0.0,
            "poll_seconds": 0.05,
            "materialize_root": str(self.gitpartner_repo),
        }
        with NamedProcessLock(
            self.root,
            self._transport_lock_name("result"),
            stale_after_seconds=120,
            wait_timeout_seconds=300,
        ):
            with self._result_query_lock:
                last_error = ""
                for attempt in range(2):
                    process, responses = self._ensure_result_query_process()
                    try:
                        assert process.stdin is not None
                        process.stdin.write(
                            json.dumps(
                                request,
                                ensure_ascii=True,
                                sort_keys=True,
                            )
                            + "\n"
                        )
                        process.stdin.flush()
                    except (BrokenPipeError, OSError, ValueError) as exc:
                        last_error = f"query service write failed: {exc}"
                        self._stop_result_query_process(process)
                        continue

                    deadline = time.monotonic() + 120.0
                    while time.monotonic() < deadline:
                        try:
                            line = responses.get(
                                timeout=max(0.01, deadline - time.monotonic())
                            )
                        except queue.Empty:
                            last_error = "query service response timed out"
                            break
                        if line is None:
                            last_error = "query service exited before response"
                            break
                        try:
                            response = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        if not isinstance(response, dict) or "ok" not in response:
                            continue
                        if not bool(response.get("ok")):
                            raise EngineTransportError(
                                str(response.get("error") or "query failed")
                            )
                        observed = response.get("observed")
                        if not isinstance(observed, dict):
                            raise EngineTransportError(
                                "query service returned no observation"
                            )
                        return observed
                    self._stop_result_query_process(process)
                    if attempt == 0:
                        continue
                raise EngineTransportError(
                    "persistent batch result query failed: " + last_error
                )

    def _ensure_result_query_process(
        self,
    ) -> tuple[subprocess.Popen[str], queue.Queue[str | None]]:
        process = self._result_query_process
        responses = self._result_query_responses
        if (
            process is not None
            and process.poll() is None
            and responses is not None
        ):
            return process, responses
        command = [
            sys.executable,
            "-m",
            "limited_remote_partner.gateway.batch_result_query",
            "--serve-jsonl",
            "--repo",
            str(self.result_gitpartner_repo),
            "--result-branch",
            self.result_channel,
            "--control-branch",
            self.control_channel,
        ]
        process = subprocess.Popen(
            command,
            cwd=self.result_gitpartner_repo,
            env=self._command_environment(),
            text=True,
            encoding="utf-8",
            errors="replace",
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            bufsize=1,
            creationflags=process_creation_flags(),
            startupinfo=process_startupinfo(),
        )
        responses = queue.Queue()

        def read_responses() -> None:
            assert process.stdout is not None
            for line in process.stdout:
                responses.put(line)
            responses.put(None)

        reader = threading.Thread(
            target=read_responses,
            name=f"engine-result-query-{self.endpoint_id or 'default'}",
            daemon=True,
        )
        reader.start()
        self._result_query_process = process
        self._result_query_responses = responses
        self._result_query_reader = reader
        if not self._result_query_atexit_registered:
            atexit.register(self.close)
            self._result_query_atexit_registered = True
        return process, responses

    def _stop_result_query_process(
        self,
        process: subprocess.Popen[str] | None = None,
    ) -> None:
        current = self._result_query_process
        if process is not None and current is not process:
            return
        self._result_query_process = None
        self._result_query_responses = None
        self._result_query_reader = None
        if current is None or current.poll() is not None:
            return
        try:
            if current.stdin is not None:
                current.stdin.close()
            current.wait(timeout=1)
        except (OSError, subprocess.TimeoutExpired):
            current.terminate()
            try:
                current.wait(timeout=1)
            except subprocess.TimeoutExpired:
                current.kill()
                current.wait(timeout=1)

    def close(self) -> None:
        with self._result_query_lock:
            self._stop_result_query_process()

    def _transport_lock_name(self, lane: str) -> str:
        base = (
            f"gitpartner_client_transport_{self.endpoint_id}"
            if self.endpoint_id
            else "gitpartner_client_transport"
        )
        return base + (
            f"_{safe_token(lane, 'lane')}" if lane != "control" else ""
        )

    def _command_environment(self) -> dict[str, str]:
        env = os.environ.copy()
        env["GITPARTNER_ENGINE_WAIT_INITIAL_GRACE_SECONDS"] = str(
            self.engine_wait_initial_grace_seconds
        )
        if self.control_channel:
            env["GITPARTNER_BRANCH"] = self.control_channel
        if self.result_channel:
            env["GITPARTNER_RESULT_BRANCH"] = self.result_channel
        if (
            self.control_channel
            and self.result_channel
            and self.control_channel != self.result_channel
        ):
            env["GITPARTNER_ENGINE_WAIT_FORCE_FETCH"] = "1"
        if self.endpoint_id:
            env["GITPARTNER_ENDPOINT_ID"] = self.endpoint_id
        endpoint_package_root = self.gitpartner_repo / "src"
        fallback_package_root = self.root / "GitPartner" / "src"
        selected_package_root = (
            endpoint_package_root
            if endpoint_package_root.is_dir()
            else fallback_package_root
        )
        package_root = str(selected_package_root)
        selected_runtime_package = (
            selected_package_root / "limited_remote_partner"
        )
        if selected_runtime_package.is_dir():
            env["GITPARTNER_CANONICAL_PACKAGE_ROOT"] = str(
                selected_runtime_package.resolve()
            )
        prepend_pythonpath(env, (package_root, shared_protocol_source(self.root)))
        return env

    def _run(
        self,
        command: list[str],
        *,
        lane: str = "control",
        repo: Path | None = None,
    ) -> subprocess.CompletedProcess[str]:
        command_repo = (repo or self.gitpartner_repo).resolve()
        if not command_repo.exists():
            raise EngineTransportError(
                f"GitPartner repo is missing: {command_repo}"
            )
        env = self._command_environment()
        timeout_seconds = transport_command_timeout_seconds(command)
        try:
            with NamedProcessLock(
                self.root,
                self._transport_lock_name(lane),
                stale_after_seconds=120,
                wait_timeout_seconds=300,
            ):
                common_kwargs = {
                    "cwd": command_repo,
                    "env": env,
                    "text": True,
                    "encoding": "utf-8",
                    "errors": "replace",
                    "stdin": subprocess.DEVNULL,
                    "creationflags": process_creation_flags(),
                    "startupinfo": process_startupinfo(),
                    "check": False,
                    "timeout": timeout_seconds,
                }
                if self.runner is not subprocess.run:
                    return self.runner(command, capture_output=True, **common_kwargs)
                with tempfile.TemporaryFile(
                    mode="w+", encoding="utf-8", errors="replace"
                ) as stdout_file, tempfile.TemporaryFile(
                    mode="w+",
                    encoding="utf-8",
                    errors="replace",
                ) as stderr_file:
                    completed = self.runner(
                        command,
                        stdout=stdout_file,
                        stderr=stderr_file,
                        **common_kwargs,
                    )
                    stdout_file.seek(0)
                    stderr_file.seek(0)
                    return subprocess.CompletedProcess(
                        completed.args,
                        completed.returncode,
                        stdout=stdout_file.read(),
                        stderr=stderr_file.read(),
                    )
        except subprocess.TimeoutExpired as exc:
            command_name = Path(command[0]).name if command else "GitPartner transport"
            raise EngineTransportError(
                f"{command_name} exceeded bounded transport timeout ({timeout_seconds:.0f}s)"
            ) from exc


def safe_relative_engine_root(value: str) -> str:
    raw = value.replace("\\", "/")
    if Path(raw).is_absolute() or raw.startswith("/"):
        raise EngineTransportError(f"engine root must be relative: {value}")
    normalized = raw.strip("/")
    if not normalized or ".." in Path(normalized).parts:
        raise EngineTransportError(f"unsafe engine root: {value}")
    return normalized


def parse_last_json_object(
    text: str,
    *,
    description: str,
) -> dict[str, Any]:
    for line in reversed(text.splitlines()):
        candidate = line.strip()
        if not candidate.startswith("{"):
            continue
        try:
            raw = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(raw, dict):
            return raw
    raise EngineTransportError(f"{description} returned no JSON object")


def merge_maintenance_request_evidence(
    status: dict[str, Any],
    request_record: dict[str, Any],
    *,
    request_id: str,
) -> dict[str, Any]:
    action = str(request_record.get("server_action") or "")
    action_args = request_record.get("server_action_args")
    if action != "lan-sync-code" or not isinstance(action_args, dict):
        raise EngineTransportError(
            f"immutable maintenance request is not a sync-code action: {request_id}"
        )
    merged = dict(status)
    merged.setdefault("request_id", request_id)
    merged.setdefault("server_action", action)
    merged.setdefault("server_action_args", dict(action_args))
    return merged


def sync_code_request_mode(request_record: dict[str, Any]) -> str:
    action = str(request_record.get("server_action") or "")
    action_args = request_record.get("server_action_args")
    if action == "lan-sync-code" and isinstance(action_args, dict):
        return "relay-lan"
    return_paths = request_record.get("return_paths")
    if (
        str(request_record.get("request_kind") or "command") == "command"
        and not bool(request_record.get("workflow_ingest", True))
        and isinstance(return_paths, list)
        and any(
            str(item).endswith("/engine_runtime_sync_receipt.json")
            for item in return_paths
        )
    ):
        return "direct-runtime"
    raise EngineTransportError(
        "immutable maintenance request is not a recognized Engine sync request"
    )


def transport_command_timeout_seconds(command: list[str]) -> float:
    default_seconds = 300.0
    grace_seconds = 90.0
    try:
        index = command.index("--wait-timeout-seconds")
        wait_seconds = float(command[index + 1])
    except (ValueError, IndexError, TypeError):
        return default_seconds
    return max(120.0, wait_seconds + grace_seconds)


def parse_local_transport_timeline(output: str) -> dict[str, Any]:
    for raw_line in reversed(output.splitlines()):
        line = raw_line.strip()
        if not line.startswith(GITPARTNER_LOCAL_TIMELINE_PREFIX):
            continue
        encoded = line[len(GITPARTNER_LOCAL_TIMELINE_PREFIX) :].strip()
        try:
            payload = json.loads(encoded)
        except json.JSONDecodeError as exc:
            raise EngineTransportError(
                "GitPartner local timeline marker contains invalid JSON"
            ) from exc
        if not isinstance(payload, dict):
            raise EngineTransportError("GitPartner local timeline must be an object")
        if payload.get("protocol_version") != "gitpartner-local-timeline-v1":
            raise EngineTransportError(
                "GitPartner local timeline has an unsupported protocol"
            )
        steps = payload.get("steps")
        if not isinstance(steps, list):
            raise EngineTransportError("GitPartner local timeline steps must be a list")
        for index, step in enumerate(steps):
            if not isinstance(step, dict) or not str(step.get("name") or ""):
                raise EngineTransportError(
                    f"GitPartner local timeline step {index} is invalid"
                )
            try:
                duration = float(step.get("duration_seconds"))
            except (TypeError, ValueError) as exc:
                raise EngineTransportError(
                    f"GitPartner local timeline step {index} has no duration"
                ) from exc
            if duration < 0:
                raise EngineTransportError(
                    f"GitPartner local timeline step {index} has a negative duration"
                )
        try:
            total_seconds = float(payload.get("total_seconds"))
        except (TypeError, ValueError) as exc:
            raise EngineTransportError(
                "GitPartner local timeline has no total duration"
            ) from exc
        if total_seconds < 0:
            raise EngineTransportError(
                "GitPartner local timeline has a negative total duration"
            )
        return payload
    return {}


def require_local_transport_timeline(
    completed: subprocess.CompletedProcess[str],
    *,
    request_id: str,
    operation: str,
) -> dict[str, Any]:
    timeline = parse_local_transport_timeline(str(completed.stdout or ""))
    if not timeline:
        raise EngineTransportError(
            f"{operation} did not emit gitpartner-local-timeline-v1"
        )
    if str(timeline.get("request_id") or "") != request_id:
        raise EngineTransportError(
            f"{operation} GitPartner local timeline request_id mismatch"
        )
    return timeline


def normalize_acknowledgements(raw: list[dict[str, str]]) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, dict):
            raise EngineTransportError("engine acknowledgement must be an object")
        engine_job_id = safe_token(
            str(item.get("engine_job_id") or ""), "engine_job_id"
        )
        receipt_id = safe_token(str(item.get("receipt_id") or ""), "receipt_id")
        if engine_job_id in seen:
            continue
        seen.add(engine_job_id)
        result.append({"engine_job_id": engine_job_id, "receipt_id": receipt_id})
    return result


def normalize_standby_cancellations(
    raw: list[dict[str, str]],
) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, dict):
            raise EngineTransportError("engine standby cancellation must be an object")
        engine_job_id = safe_token(
            str(item.get("engine_job_id") or ""), "engine_job_id"
        )
        if engine_job_id in seen:
            continue
        seen.add(engine_job_id)
        reason = (
            str(item.get("reason") or "").strip() or "standby cancelled by controller"
        )
        if len(reason) > 1000:
            raise EngineTransportError(
                f"engine standby cancellation reason is too long: {engine_job_id}"
            )
        result.append({"engine_job_id": engine_job_id, "reason": reason})
    return result


def safe_token(value: str, label: str) -> str:
    if not value or any(
        char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
        for char in value
    ):
        raise EngineTransportError(f"unsafe engine {label}: {value}")
    return value


def safe_optional_token(value: str, label: str) -> str:
    return safe_token(value, label) if value else ""


def read_object(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(raw, dict):
        raise EngineTransportError(f"JSON document must be an object: {path}")
    return raw


def find_correlated_json(
    root: Path, name: str, engine_job_id: str
) -> dict[str, Any] | None:
    for path in sorted(root.rglob(name)) if root.exists() else []:
        try:
            payload = read_object(path)
        except (OSError, ValueError, json.JSONDecodeError, EngineTransportError):
            continue
        if str(payload.get("engine_job_id") or "") == engine_job_id:
            return payload
    return None


def returned_state_without_bundle(
    root: Path, *, engine_job_id: str
) -> dict[str, Any] | None:
    for path in named_candidates(root, "state.json"):
        try:
            payload = read_object(path)
        except (OSError, ValueError, json.JSONDecodeError, EngineTransportError):
            continue
        if str(payload.get("engine_job_id") or "") != engine_job_id:
            continue
        if not payload.get("returned_at"):
            continue
        if not (path.parent / "result_bundle").is_dir():
            return payload
    return None


def verify_returned_identity(
    root: Path,
    *,
    engine_job_id: str,
    expected: object,
    terminal_state: str = "completed",
) -> dict[str, Any]:
    if not isinstance(expected, dict) or not expected:
        return {"status": "not-required"}
    candidates = [
        path
        for path in sorted(root.rglob("ENGINE_IDENTITY.json"))
        if engine_job_id in path.parts
    ]
    if not candidates:
        raise EngineTransportError(
            f"returned ENGINE_IDENTITY.json not found for {engine_job_id} under {root}"
        )
    path = candidates[0]
    actual = read_object(path)
    fields = (
        "source_sha256",
        "case_bundle_sha256",
        "golden_bundle_sha256",
        "test_version",
        "test_contract_sha256",
        "correctness_case_count",
        "performance_case_count",
        "correctness_repetitions",
        "performance_samples_per_case",
    )
    mismatches = {
        field: {"expected": expected.get(field), "actual": actual.get(field)}
        for field in fields
        if str(expected.get(field) or "") != str(actual.get(field) or "")
    }
    if mismatches and terminal_state != "failed":
        raise EngineTransportError(
            "returned engine identity does not match immutable input: "
            + json.dumps(mismatches, sort_keys=True, separators=(",", ":"))
        )
    return {
        "status": "mismatch-terminal-failed" if mismatches else "verified",
        "engine_job_id": engine_job_id,
        "path": str(path.relative_to(root)).replace("\\", "/"),
        "identity": {field: actual.get(field) for field in fields},
        "mismatches": mismatches,
    }


def discover_ready_bundles(
    root: Path,
    ready: list[dict[str, Any]],
    *,
    bundle_root: Path | None = None,
    report_root: Path | None = None,
) -> list[dict[str, Any]]:
    # An older hot-updated peer exposes manifests only. Keep the legacy collect
    # path until the export marker proves both sides speak the fast-path protocol.
    if not named_candidates(root, "ready_export.json"):
        return []
    bundles: list[dict[str, Any]] = []
    search_root = bundle_root or root
    display_root = report_root or search_root
    for announced in ready:
        engine_job_id = str(announced.get("engine_job_id") or "")
        if not engine_job_id:
            raise EngineTransportError("return-ready manifest has no engine_job_id")
        terminal_path = find_correlated_json_path(
            search_root, "terminal.json", engine_job_id
        )
        if terminal_path is None:
            raise EngineTransportError(
                f"snapshot ready bundle is missing terminal.json for {engine_job_id}"
            )
        job_root = terminal_path.parent
        terminal = read_object(terminal_path)
        for field in (
            "request_id",
            "engine_job_id",
            "attempt_id",
            "operator",
            "test_version",
            "bundle_hash",
            "state",
            "terminal_at",
        ):
            if str(terminal.get(field) or "") != str(announced.get(field) or ""):
                raise EngineTransportError(
                    f"snapshot ready manifest mismatch for {engine_job_id}: {field}"
                )
        artifact_evidence = verify_returned_artifacts(job_root, terminal)
        if artifact_evidence.get("return_phase") == "optional":
            identity_evidence = {"status": "covered-by-required-return"}
        else:
            identity_evidence = verify_returned_identity(
                job_root,
                engine_job_id=engine_job_id,
                expected=terminal.get("input_identity"),
                terminal_state=str(terminal.get("state") or ""),
            )
        bundles.append(
            {
                "engine_job_id": engine_job_id,
                "terminal": terminal,
                "artifact_evidence": artifact_evidence,
                "identity_evidence": identity_evidence,
                "bundle_root": str(job_root.relative_to(display_root)).replace(
                    "\\", "/"
                ),
            }
        )
    return bundles


def ready_manifests_selected_by_export(
    root: Path, ready: list[dict[str, Any]]
) -> list[dict[str, Any]] | None:
    """Correlate a phased v4 archive with the subset selected for this exchange."""
    announced = {
        str(item.get("engine_job_id") or ""): item
        for item in ready
        if str(item.get("engine_job_id") or "")
    }
    errors: list[str] = []
    for marker_path in named_candidates(root, "ready_export.json"):
        try:
            marker = read_object(marker_path)
            count = int(marker.get("job_count", -1))
            raw_jobs = marker.get("jobs")
            if raw_jobs is None:
                if count == len(ready):
                    return list(ready)
                if count == 0:
                    return []
                continue
            if not isinstance(raw_jobs, list) or count != len(raw_jobs):
                raise EngineTransportError(
                    f"ready export selection count mismatch: {marker_path}"
                )
            selected: list[dict[str, Any]] = []
            seen: set[str] = set()
            for raw in raw_jobs:
                if not isinstance(raw, dict):
                    raise EngineTransportError(
                        f"ready export selection is invalid: {marker_path}"
                    )
                job_id = str(raw.get("engine_job_id") or "")
                if not job_id or job_id in seen or job_id not in announced:
                    raise EngineTransportError(
                        f"ready export selection identity mismatch: {marker_path}"
                    )
                seen.add(job_id)
                selected.append(announced[job_id])
            return selected
        except (
            EngineTransportError,
            OSError,
            ValueError,
            json.JSONDecodeError,
        ) as exc:
            errors.append(str(exc))
    if errors:
        raise EngineTransportError(errors[0])
    return None


def verify_returned_artifacts(
    job_root: Path, terminal: dict[str, Any]
) -> dict[str, Any]:
    manifest_path = job_root / "artifact_manifest.json"
    bundle_root = (job_root / "result_bundle").resolve()
    if not manifest_path.is_file() or not bundle_root.is_dir():
        raise EngineTransportError(f"snapshot bundle is incomplete under {job_root}")
    manifest = read_object(manifest_path)
    artifacts = manifest.get("artifacts")
    terminal_artifacts = terminal.get("artifacts")
    if not isinstance(terminal_artifacts, list):
        raise EngineTransportError(
            f"snapshot terminal artifact list is invalid under {job_root}"
        )
    return_phase = str(manifest.get("return_phase") or "complete")
    required_artifacts = [
        item
        for item in terminal_artifacts
        if isinstance(item, dict) and bool(item.get("required"))
    ]
    optional_artifacts = [
        item
        for item in terminal_artifacts
        if isinstance(item, dict) and not bool(item.get("required"))
    ]
    expected = {
        "complete": terminal_artifacts,
        "required": required_artifacts,
        "optional": optional_artifacts,
    }.get(return_phase)
    if expected is None:
        raise EngineTransportError(
            f"snapshot artifact return phase is invalid under {job_root}: {return_phase}"
        )
    deferred = manifest.get("deferred_artifacts", [])
    if (
        not isinstance(artifacts, list)
        or artifacts != expected
        or not isinstance(deferred, list)
        or (return_phase == "required" and deferred != optional_artifacts)
        or (return_phase != "required" and deferred)
    ):
        raise EngineTransportError(
            f"snapshot artifact manifest mismatch under {job_root}"
        )
    verified: list[str] = []
    ignored_volatile_diagnostics: list[str] = []
    for raw in artifacts:
        if not isinstance(raw, dict):
            raise EngineTransportError("snapshot artifact record must be an object")
        relative = str(raw.get("path") or "").replace("\\", "/").strip("/")
        if not relative or ".." in Path(relative).parts:
            raise EngineTransportError(f"unsafe returned artifact path: {relative}")
        target = (bundle_root / relative).resolve()
        if target != bundle_root and bundle_root not in target.parents:
            raise EngineTransportError(f"returned artifact escapes bundle: {relative}")
        kind = str(raw.get("kind") or "")
        target_io = filesystem_path(target)
        if kind == "file" and target_io.is_file():
            digest = raw_file_digest(target_io)
            size = target_io.stat().st_size
        elif kind == "directory" and target_io.is_dir():
            digest, size = canonical_tree_digest_and_size(target)
        else:
            raise EngineTransportError(
                f"returned artifact missing or wrong kind: {relative}"
            )
        expected_digest = str(raw.get("sha256") or "")
        expected_size = int(raw.get("size_bytes", -1) or 0)
        if (
            (digest != expected_digest or size != expected_size)
            and kind == "directory"
            and relative == "logs"
            and not bool(raw.get("required"))
        ):
            filtered_digest, filtered_size, ignored = (
                canonical_optional_log_tree_digest(target)
            )
            if filtered_digest == expected_digest and filtered_size == expected_size:
                digest = filtered_digest
                size = filtered_size
                ignored_volatile_diagnostics.extend(ignored)
        if digest != expected_digest or size != expected_size:
            raise EngineTransportError(
                "returned artifact checksum mismatch: "
                f"job_root={job_root} artifact={relative} "
                f"expected_sha256={expected_digest} actual_sha256={digest} "
                f"expected_size={expected_size} actual_size={size}"
            )
        verified.append(relative)
    return {
        "status": (
            "required-verified"
            if return_phase == "required"
            else "optional-verified" if return_phase == "optional" else "verified"
        ),
        "return_phase": return_phase,
        "artifact_count": len(verified),
        "deferred_artifact_count": len(deferred),
        "paths": verified,
        "ignored_volatile_diagnostics": ignored_volatile_diagnostics,
    }


def find_correlated_json_path(root: Path, name: str, engine_job_id: str) -> Path | None:
    for path in named_candidates(root, name):
        try:
            payload = read_object(path)
        except (OSError, ValueError, json.JSONDecodeError, EngineTransportError):
            continue
        if (
            str(payload.get("engine_job_id") or "") == engine_job_id
            and (path.parent / "result_bundle").is_dir()
        ):
            return path
    return None


def raw_file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_tree_digest(root: Path) -> str:
    digest, _ = canonical_tree_digest_and_size(root)
    return digest


def canonical_tree_digest_and_size(root: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    for relative, path, is_file in canonical_tree_entries(root):
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        if is_file:
            update_canonical_digest(digest, path)
            digest.update(b"\0")
            size += path.stat().st_size
    return digest.hexdigest(), size


def canonical_optional_log_tree_digest(root: Path) -> tuple[str, int, list[str]]:
    """Hash stable diagnostics while excluding worker-owned wrapper log files."""

    volatile_suffixes = (".worker.err.log", ".worker.out.log")
    ignored: list[str] = []
    digest = hashlib.sha256()
    size = 0
    for relative, path, is_file in canonical_tree_entries(root):
        if is_file and path.name.endswith(volatile_suffixes):
            ignored.append(relative)
            continue
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        if is_file:
            update_canonical_digest(digest, path)
            digest.update(b"\0")
            size += path.stat().st_size
    return digest.hexdigest(), size, ignored


def canonical_tree_entries(root: Path) -> list[tuple[str, Path, bool]]:
    """Enumerate a tree through the Windows long-path namespace when needed."""

    scan_root = extended_length_path(root) if os.name == "nt" else root
    entries: list[tuple[str, Path, bool]] = []
    for path in scan_root.rglob("*"):
        relative = path.relative_to(scan_root).as_posix()
        entries.append((relative, path, path.is_file()))
    return sorted(entries, key=lambda item: item[0])


def update_canonical_digest(digest: Any, path: Path) -> None:
    carry = b""
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            data = carry + chunk
            carry = b"\r" if data.endswith(b"\r") else b""
            if carry:
                data = data[:-1]
            digest.update(data.replace(b"\r\n", b"\n"))
    if carry:
        digest.update(carry)


def find_named_object(root: Path, name: str) -> dict[str, Any] | None:
    candidates = named_candidates(root, name)
    for path in candidates:
        try:
            return read_object(path)
        except (OSError, ValueError, json.JSONDecodeError, EngineTransportError):
            continue
    return None


def find_named_path(root: Path, name: str) -> Path | None:
    candidates = named_candidates(root, name)
    return candidates[0] if candidates else None


def find_named_list(root: Path, name: str) -> list[dict[str, Any]]:
    candidates = named_candidates(root, name)
    for path in candidates:
        try:
            raw = json.loads(path.read_text(encoding="utf-8-sig"))
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        if isinstance(raw, list):
            return [item for item in raw if isinstance(item, dict)]
    return []


def extended_length_path(path: Path) -> Path:
    """Return a Windows extended-length path suitable for filesystem I/O."""
    resolved = str(path.resolve())
    if os.name != "nt" or resolved.startswith("\\\\?\\"):
        return Path(resolved)
    if resolved.startswith("\\\\"):
        return Path("\\\\?\\UNC\\" + resolved[2:])
    return Path("\\\\?\\" + resolved)


def filesystem_path(path: Path) -> Path:
    """Use the extended namespace only where the normal Windows path can fail."""
    if os.name == "nt" and len(str(path.resolve())) >= 248:
        return extended_length_path(path)
    return path


def named_candidates(root: Path, name: str) -> list[Path]:
    io_root = filesystem_path(root)
    if not io_root.exists():
        return []
    candidates: list[tuple[float, Path]] = []
    try:
        # Traverse through the extended namespace on Windows. GP output paths
        # can exceed MAX_PATH even though every individual component is valid.
        scan_root = extended_length_path(root) if os.name == "nt" else root
        paths = scan_root.rglob(name)
        for path in paths:
            if os.name == "nt":
                try:
                    relative = path.relative_to(scan_root)
                except ValueError:
                    # Tests and race-tolerant walkers may yield a regular path
                    # while the traversal root uses the extended namespace.
                    path = filesystem_path(path)
                else:
                    path = filesystem_path(root / relative)
            try:
                candidates.append((path.stat().st_mtime, path))
            except OSError:
                # GP may atomically replace nested client_output while a result is
                # being collected. A vanished duplicate must not hide the stable copy.
                continue
    except OSError:
        # Preserve candidates already observed before a concurrent directory
        # replacement interrupted traversal.
        pass
    return [
        path for _, path in sorted(candidates, key=lambda item: item[0], reverse=True)
    ]


def extract_ready_archive(archive: Path, destination: Path) -> Path:
    target = destination.resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    # Keep the atomic staging component shorter than the final cache key. The
    # old PID+nanosecond suffix pushed otherwise valid bundles past MAX_PATH on
    # Windows when an isolated daemon root was used.
    staging = Path(tempfile.mkdtemp(prefix=".rx-", dir=target.parent))
    try:
        with tarfile.open(archive, "r") as handle:
            for member in handle.getmembers():
                relative = Path(member.name.replace("\\", "/"))
                if (
                    relative.is_absolute()
                    or not relative.parts
                    or ".." in relative.parts
                    or member.issym()
                    or member.islnk()
                    or member.isdev()
                ):
                    raise EngineTransportError(
                        f"unsafe snapshot ready archive entry: {member.name}"
                    )
                resolved = (staging / relative).resolve()
                if (
                    staging.resolve() not in resolved.parents
                    and resolved != staging.resolve()
                ):
                    raise EngineTransportError(
                        f"snapshot ready archive escapes destination: {member.name}"
                    )
            if os.name == "nt":
                extract_ready_archive_members_windows(handle, staging)
            else:
                handle.extractall(staging, filter="data")
        target_io = extended_length_path(target) if os.name == "nt" else target
        staging_io = extended_length_path(staging) if os.name == "nt" else staging
        if target_io.exists():
            shutil.rmtree(target_io)
        os.replace(staging_io, target_io)
        return target
    except Exception:
        cleanup = extended_length_path(staging) if os.name == "nt" else staging
        shutil.rmtree(cleanup, ignore_errors=True)
        raise


def extract_ready_archive_members_windows(
    handle: tarfile.TarFile, staging: Path
) -> None:
    """Extract validated regular members using extended-length Windows paths."""

    for member in handle.getmembers():
        relative = Path(member.name.replace("\\", "/"))
        destination = staging / relative
        destination_io = extended_length_path(destination)
        if member.isdir():
            destination_io.mkdir(parents=True, exist_ok=True)
            continue
        if not member.isfile():
            raise EngineTransportError(
                f"unsupported snapshot ready archive entry: {member.name}"
            )
        extended_length_path(destination.parent).mkdir(parents=True, exist_ok=True)
        source = handle.extractfile(member)
        if source is None:
            raise EngineTransportError(
                f"snapshot ready archive member has no data: {member.name}"
            )
        with source, destination_io.open("wb") as output:
            shutil.copyfileobj(source, output, length=1024 * 1024)


def materialize_ready_archive(
    archive_path: Path,
    marker: dict[str, Any],
    destination: Path,
) -> tuple[Path, Path | None] | None:
    """Return one verified tar file from either the legacy file or chunk directory."""
    layout = str(marker.get("archive_layout") or "single-file")
    expected_size = marker.get("archive_size_bytes")
    expected_digest = str(marker.get("archive_sha256") or "")
    if archive_path.is_file():
        if layout != "single-file":
            raise EngineTransportError(
                f"ready archive layout/path mismatch: {archive_path}"
            )
        if expected_size is not None and archive_path.stat().st_size != int(
            expected_size
        ):
            raise EngineTransportError(
                f"ready archive size has not settled: {archive_path}"
            )
        if expected_digest and raw_file_digest(archive_path) != expected_digest:
            raise EngineTransportError(
                f"ready archive checksum has not settled: {archive_path}"
            )
        return archive_path, None
    if not archive_path.exists():
        return None
    if not archive_path.is_dir() or layout != "chunked-directory":
        raise EngineTransportError(
            f"ready archive layout/path mismatch: {archive_path}"
        )

    raw_parts = marker.get("archive_parts")
    if not isinstance(raw_parts, list) or not raw_parts:
        raise EngineTransportError(
            f"ready archive chunk manifest is missing: {archive_path}"
        )
    if int(marker.get("archive_part_count", -1)) != len(raw_parts):
        raise EngineTransportError(
            f"ready archive chunk count mismatch: {archive_path}"
        )
    part_names: list[str] = []
    for index, raw in enumerate(raw_parts):
        if not isinstance(raw, dict):
            raise EngineTransportError(
                f"ready archive chunk metadata is invalid: {archive_path}"
            )
        name = str(raw.get("name") or "")
        relative = Path(name.replace("\\", "/"))
        if (
            not name
            or relative.is_absolute()
            or len(relative.parts) != 1
            or relative.name != name
            or name != f"part-{index:05d}"
        ):
            raise EngineTransportError(f"ready archive chunk name is invalid: {name}")
        part_names.append(name)
    try:
        actual_names = sorted(item.name for item in archive_path.iterdir())
    except OSError as exc:
        raise EngineTransportError(
            f"ready archive chunk directory is unavailable: {archive_path}"
        ) from exc
    if actual_names != part_names:
        raise EngineTransportError(
            f"ready archive chunk set has not settled: {archive_path}"
        )

    destination.parent.mkdir(parents=True, exist_ok=True)
    file_descriptor, temporary_name = tempfile.mkstemp(
        prefix=".ready-archive-",
        suffix=".tar",
        dir=destination.parent,
    )
    temporary = Path(temporary_name)
    aggregate = hashlib.sha256()
    total_size = 0
    try:
        with os.fdopen(file_descriptor, "wb") as output:
            for raw, name in zip(raw_parts, part_names):
                part_path = archive_path / name
                if part_path.is_symlink() or not part_path.is_file():
                    raise EngineTransportError(
                        f"ready archive chunk is missing: {part_path}"
                    )
                expected_part_size = int(raw.get("size_bytes", -1))
                if (
                    expected_part_size < 0
                    or part_path.stat().st_size != expected_part_size
                ):
                    raise EngineTransportError(
                        f"ready archive chunk size has not settled: {part_path}"
                    )
                expected_part_digest = str(raw.get("sha256") or "")
                if (
                    not expected_part_digest
                    or raw_file_digest(part_path) != expected_part_digest
                ):
                    raise EngineTransportError(
                        f"ready archive chunk checksum has not settled: {part_path}"
                    )
                with part_path.open("rb") as source:
                    while True:
                        payload = source.read(1024 * 1024)
                        if not payload:
                            break
                        output.write(payload)
                        aggregate.update(payload)
                        total_size += len(payload)
        if expected_size is None or total_size != int(expected_size):
            raise EngineTransportError(
                f"ready archive size has not settled: {archive_path}"
            )
        if not expected_digest or aggregate.hexdigest() != expected_digest:
            raise EngineTransportError(
                f"ready archive checksum has not settled: {archive_path}"
            )
        return temporary, temporary
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def extract_settled_ready_archive(
    output_root: Path,
    destination: Path,
    announced_ready: list[dict[str, Any]],
) -> Path | None:
    """Extract only a complete GP-published archive correlated with its export marker."""
    expected_ids = {
        str(item.get("engine_job_id") or "")
        for item in announced_ready
        if str(item.get("engine_job_id") or "")
    }
    errors: list[str] = []
    for marker_path in named_candidates(output_root, "ready_export.json"):
        try:
            marker = read_object(marker_path)
            if int(marker.get("job_count", -1)) != len(announced_ready):
                continue
            archive_path = marker_path.parent / "ready_jobs.tar"
            materialized = materialize_ready_archive(
                archive_path,
                marker,
                destination,
            )
            if materialized is None:
                continue
            archive_file, cleanup = materialized
            try:
                extracted = extract_ready_archive(archive_file, destination)
            finally:
                if cleanup is not None:
                    cleanup.unlink(missing_ok=True)
            index_path = extracted / "ready_jobs" / "ready_index.json"
            if not index_path.is_file():
                raise EngineTransportError(
                    f"ready archive index is missing: {archive_path}"
                )
            index = read_object(index_path)
            jobs = index.get("jobs")
            if not isinstance(jobs, list) or int(index.get("job_count", -1)) != len(
                announced_ready
            ):
                raise EngineTransportError(
                    f"ready archive index count mismatch: {archive_path}"
                )
            exported_ids = {
                str(item.get("engine_job_id") or "")
                for item in jobs
                if isinstance(item, dict) and str(item.get("engine_job_id") or "")
            }
            if exported_ids != expected_ids:
                raise EngineTransportError(
                    f"ready archive index identity mismatch: {archive_path}"
                )
            return extracted
        except (
            EngineTransportError,
            OSError,
            ValueError,
            json.JSONDecodeError,
            tarfile.TarError,
        ) as exc:
            errors.append(str(exc))
    if errors:
        raise EngineTransportError(errors[0])
    return None


def wait_for_value(
    loader: Callable[[], Any],
    *,
    description: str,
    timeout_seconds: float,
    accept: Callable[[Any], bool] | None = None,
) -> Any:
    predicate = accept or (lambda value: value is not None)
    deadline = time.monotonic() + max(0.0, float(timeout_seconds))
    last_error = ""
    while True:
        try:
            value = loader()
        except (OSError, ValueError, json.JSONDecodeError, EngineTransportError) as exc:
            value = None
            last_error = str(exc)
        if predicate(value):
            return value
        if time.monotonic() >= deadline:
            detail = f": {last_error}" if last_error else ""
            raise EngineTransportError(
                f"{description} did not settle under GP output{detail}"
            )
        time.sleep(0.2)


def wait_for_named_list(
    root: Path,
    name: str,
    *,
    timeout_seconds: float,
) -> list[dict[str, Any]]:
    def load() -> list[dict[str, Any]] | None:
        for path in named_candidates(root, name):
            try:
                raw = json.loads(path.read_text(encoding="utf-8-sig"))
            except (OSError, ValueError, json.JSONDecodeError):
                continue
            if isinstance(raw, list):
                return [item for item in raw if isinstance(item, dict)]
        return None

    return wait_for_value(
        load,
        description=name,
        timeout_seconds=timeout_seconds,
    )


def command_failure(label: str, completed: subprocess.CompletedProcess[str]) -> str:
    detail = "\n".join(
        part.strip()
        for part in (completed.stdout or "", completed.stderr or "")
        if part.strip()
    )
    return f"{label} failed rc={completed.returncode}: {detail[-2000:]}"


def find_resident_runtime_receipt(
    output_root: Path,
    request_id: str,
) -> dict[str, Any] | None:
    receipt = find_named_object(output_root, f"{request_id}.json")
    if receipt is not None:
        return receipt
    marker = "GITPARTNER_RESIDENT_RUNTIME_RECEIPT:"
    for path in sorted(output_root.rglob("job.log.part*.txt")):
        try:
            lines = path.read_text(encoding="utf-8-sig").splitlines()
        except OSError:
            continue
        for line in reversed(lines):
            if not line.startswith(marker):
                continue
            try:
                payload = json.loads(line[len(marker) :])
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                return payload
    return None


def expected_remote_resident_code_generation(
    root: Path,
    *,
    gitpartner_repo: Path | None = None,
) -> str:
    repository = (
        gitpartner_repo.resolve()
        if gitpartner_repo is not None
        else root.resolve() / "GitPartner"
    )
    package = repository / "src" / "limited_remote_partner"
    digest = hashlib.sha256()
    for name in ("client.py", "git_client.py", "input_parser.py"):
        path = package / name
        if not path.is_file():
            raise EngineTransportError(
                f"canonical resident runtime source is missing: {path}"
            )
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(
            path.read_bytes().replace(b"\r\n", b"\n").replace(b"\r", b"\n")
        )
        digest.update(b"\0")
    return digest.hexdigest()[:16]
