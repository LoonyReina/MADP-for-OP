from __future__ import annotations

import atexit
import hashlib
import json
import os
import queue
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from ascendop_daemon.control_plane.control_database import ControlDatabase, ControlDatabaseError
from ascendop_daemon.runtime.process_adapter import process_creation_flags, process_startupinfo
from ascendop_daemon.runtime.locking import NamedProcessLock
from ascendop_daemon.registry.system_registry import BackendEndpoint, SystemRegistry
from ascendop_daemon.workflow.cannjudge_result_adapter import (
    CannJudgeV3ResultAdapter,
)
from ascendop_daemon.workflow.workspace_result_collector import WorkspaceResultCollector


TERMINAL_GP_STATES = {"success", "failed", "claim_failed", "return_failed"}


from ascendop_daemon.exchange.transport_contracts import (
    DeliveryObservation,
    EndpointTransport,
    QueryObservation,
    TransportResultCollector,
    deterministic_receipt_id,
    endpoint_supports,
    parse_last_json_object,
    status_transport_identity,
    transport_identity,
    transport_identity_mismatch,
)
from ascendop_daemon.exchange.gitpartner_transport import GitPartnerCanaryTransport
from ascendop_daemon.exchange.wire_v3_transport import (
    RoutedEndpointTransport,
    WireV3EndpointTransport,
)

class EndpointDispatcher:
    def __init__(
        self,
        database: ControlDatabase,
        endpoint: BackendEndpoint,
        transport: EndpointTransport,
        *,
        capacity: int = 4,
        claim_ttl_seconds: int = 90,
        max_delivery_attempts: int = 3,
        poll_min_interval_seconds: float = 0.05,
        poll_max_interval_seconds: float = 1.0,
        consumer: str = "",
        result_collector: TransportResultCollector | None = None,
    ) -> None:
        self.database = database
        self.endpoint = endpoint
        self.transport = transport
        self.capacity = max(1, int(capacity))
        self.claim_ttl_seconds = max(1, int(claim_ttl_seconds))
        self.max_delivery_attempts = max(1, int(max_delivery_attempts))
        self.poll_min_interval_seconds = max(
            0.01,
            float(poll_min_interval_seconds),
        )
        self.adaptive_polling = (
            endpoint_supports(endpoint, "gp-adaptive-poll-v1")
            or endpoint_supports(endpoint, "gp-batch-result-query-v1")
        )
        requested_poll_max = max(
            self.poll_min_interval_seconds,
            float(poll_max_interval_seconds),
        )
        self.poll_max_interval_seconds = (
            requested_poll_max
            if self.adaptive_polling
            else self.poll_min_interval_seconds
        )
        self.consumer = consumer or f"endpoint-dispatcher:{endpoint.endpoint_id}"
        self.result_collector = result_collector

    def run_once(self, *, allow_claims: bool = True) -> dict[str, Any]:
        report = self._new_report()
        report["recovered_expired_claims"].extend(
            self.database.recover_expired_transport_claims(
                endpoint_id=self.endpoint.endpoint_id,
            )
        )
        if endpoint_supports(self.endpoint, "gp-duplex-lanes-v1"):
            self._run_duplex(report, allow_claims=allow_claims)
            report["lane_mode"] = "duplex"
        else:
            self._run_serial(report, allow_claims=allow_claims)
            report["lane_mode"] = "serial"
        report["poll_mode"] = (
            "adaptive" if self.adaptive_polling else "fixed"
        )
        report["counts"] = self.database.transport_endpoint_counts(
            self.endpoint.endpoint_id
        )
        return report

    def _new_report(self) -> dict[str, Any]:
        return {
            "endpoint_id": self.endpoint.endpoint_id,
            "acknowledged": [],
            "polled": [],
            "claimed": [],
            "recovered_expired_claims": [],
            "errors": [],
        }

    def _run_serial(
        self,
        report: dict[str, Any],
        *,
        allow_claims: bool,
    ) -> None:
        self._acknowledge_pending(report)
        self._poll_active(report)
        if allow_claims:
            self._publish_available(report)
        self._poll_active(report)
        self._acknowledge_pending(report)

    def _run_duplex(
        self,
        report: dict[str, Any],
        *,
        allow_claims: bool,
    ) -> None:
        # Receipt acknowledgement is local and releases endpoint credit before
        # opening the two network lanes.
        self._acknowledge_pending(report)
        poll_report = self._new_report()
        ingress_report = self._new_report()
        poll_rows = self._pollable_rows()
        claims = self._claim_available(ingress_report) if allow_claims else []
        with ThreadPoolExecutor(
            max_workers=2,
            thread_name_prefix=f"endpoint-{self.endpoint.endpoint_id}-lane",
        ) as executor:
            poll_future = executor.submit(
                self._poll_rows,
                poll_rows,
                poll_report,
            )
            ingress_future = executor.submit(
                self._publish_claims,
                claims,
                ingress_report,
            )
            poll_future.result()
            ingress_future.result()
        self._merge_report(report, poll_report)
        self._merge_report(report, ingress_report)

        # A return found by the poll lane can free credit in this same tick.
        # Refill once after acknowledgement; there is no batch barrier.
        self._acknowledge_pending(report)
        refill_report = self._new_report()
        if allow_claims:
            self._publish_available(refill_report)
        self._merge_report(report, refill_report)

    @staticmethod
    def _merge_report(
        target: dict[str, Any],
        source: dict[str, Any],
    ) -> None:
        for key in (
            "acknowledged",
            "polled",
            "claimed",
            "recovered_expired_claims",
            "errors",
        ):
            target[key].extend(source.get(key, []))
        for key in ("batch_publications", "batch_queries"):
            if source.get(key):
                target[key] = int(target.get(key, 0)) + int(source[key])

    def _publish_available(self, report: dict[str, Any]) -> None:
        self._publish_claims(self._claim_available(report), report)

    def _claim_available(
        self,
        report: dict[str, Any],
    ) -> list[dict[str, Any]]:
        counts = self.database.transport_endpoint_counts(
            self.endpoint.endpoint_id
        )
        available = max(0, self.capacity - int(counts.get("active", 0)))
        append_ingress = endpoint_supports(
            self.endpoint,
            "gp-append-request-v1",
        )
        if not append_ingress:
            # The compatibility channel exposes one mutable input/job.json.
            # Fence publication until that request is endpoint-visible.
            awaiting_acceptance = sum(
                int(counts.get(state, 0))
                for state in ("claimed", "sending", "uncertain")
            )
            if awaiting_acceptance:
                available = 0
            else:
                available = min(available, 1)
        if self.endpoint.draining or not self.endpoint.enabled:
            available = 0
        if not available:
            return []
        claims = self.database.claim_transport_outbox(
            self.consumer,
            max_items=available,
            ttl_seconds=self.claim_ttl_seconds,
            endpoint_id=self.endpoint.endpoint_id,
        )
        report["claimed"].extend(
            str(claim["outbox_id"]) for claim in claims
        )
        return claims

    def _publish_claims(
        self,
        claims: list[dict[str, Any]],
        report: dict[str, Any],
    ) -> None:
        if not claims:
            return
        batch_publish = getattr(self.transport, "publish_batch", None)
        if (
            endpoint_supports(self.endpoint, "gp-append-request-v1")
            and len(claims) > 1
            and callable(batch_publish)
        ):
            self._publish_claim_batch(claims, report)
            return
        for claim in claims:
            self._publish_claim(claim, report)

    def _publish_claim_batch(
        self,
        claims: list[dict[str, Any]],
        report: dict[str, Any],
    ) -> None:
        sending_rows: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for claim in claims:
            try:
                sending = self.database.mark_transport_sending(
                    str(claim["outbox_id"]),
                    consumer=self.consumer,
                    claim_token=str(claim["claim_token"]),
                )
                sending_rows.append((claim, sending))
            except Exception as exc:
                report["errors"].append(
                    {
                        "outbox_id": str(claim["outbox_id"]),
                        "phase": "batch-mark-sending",
                        "error": str(exc),
                    }
                )
        if not sending_rows:
            return
        try:
            observations = self.transport.publish_batch(
                [
                    {
                        **dict(sending["payload"]),
                        "_transport_delivery_ordinal": int(
                            sending.get("delivery_attempts", 0) or 0
                        ),
                    }
                    for _claim, sending in sending_rows
                ]
            )
            if len(observations) != len(sending_rows):
                raise ValueError(
                    "transport batch result count does not match request count"
                )
            for (claim, sending), observation in zip(
                sending_rows,
                observations,
                strict=True,
            ):
                self._record_delivery_observation(
                    claim,
                    sending,
                    observation,
                )
            report["batch_publications"] = int(
                report.get("batch_publications", 0)
            ) + 1
        except Exception as exc:
            for claim, _sending in sending_rows:
                outbox_id = str(claim["outbox_id"])
                report["errors"].append(
                    {
                        "outbox_id": outbox_id,
                        "phase": "batch-publish",
                        "error": str(exc),
                    }
                )
                try:
                    self.database.record_transport_delivery(
                        outbox_id,
                        consumer=self.consumer,
                        claim_token=str(claim["claim_token"]),
                        status="uncertain",
                        error=f"dispatcher batch publish exception: {exc}",
                    )
                except Exception as recovery_exc:
                    report["errors"].append(
                        {
                            "outbox_id": outbox_id,
                            "phase": "batch-publish-recovery",
                            "error": str(recovery_exc),
                        }
                    )

    def _publish_claim(
        self, claim: dict[str, Any], report: dict[str, Any]
    ) -> None:
        outbox_id = str(claim["outbox_id"])
        try:
            sending = self.database.mark_transport_sending(
                outbox_id,
                consumer=self.consumer,
                claim_token=str(claim["claim_token"]),
            )
            observation = self.transport.publish(
                {
                    **dict(sending["payload"]),
                    "_transport_delivery_ordinal": int(
                        sending.get("delivery_attempts", 0) or 0
                    ),
                }
            )
            self._record_delivery_observation(
                claim,
                sending,
                observation,
            )
        except Exception as exc:
            report["errors"].append(
                {"outbox_id": outbox_id, "phase": "publish", "error": str(exc)}
            )
            try:
                current = self.database.transport_outbox(outbox_id)
                if current["state"] == "sending":
                    self.database.record_transport_delivery(
                        outbox_id,
                        consumer=self.consumer,
                        claim_token=str(claim["claim_token"]),
                        status="uncertain",
                        error=f"dispatcher publish exception: {exc}",
                    )
            except Exception as recovery_exc:
                report["errors"].append(
                    {
                        "outbox_id": outbox_id,
                        "phase": "publish-recovery",
                        "error": str(recovery_exc),
                    }
                )

    def _record_delivery_observation(
        self,
        claim: dict[str, Any],
        sending: dict[str, Any],
        observation: DeliveryObservation,
    ) -> None:
        observed_status = observation.status
        status = (
            "decision-pending"
            if observed_status == "retry" or bool(observation.failure)
            else observed_status
        )
        failure = dict(observation.failure)
        if status == "decision-pending" and not failure:
            failure = {
                "domain": "transport",
                "code": "endpoint-delivery-" + observed_status,
                "phase": "publish",
                "detail": observation.error or "endpoint delivery failed",
                "retryable": observed_status == "retry",
                "pre_publish": False,
                "result_visibility": (
                    "unknown" if observed_status == "retry" else "known"
                ),
            }
        self.database.record_transport_delivery(
            str(claim["outbox_id"]),
            consumer=self.consumer,
            claim_token=str(claim["claim_token"]),
            status=status,
            receipt=observation.receipt,
            error=observation.error,
            retry_after_seconds=observation.retry_after_seconds,
            failure=failure,
        )

    def _poll_active(self, report: dict[str, Any]) -> None:
        self._poll_rows(self._pollable_rows(), report)

    def _pollable_rows(self) -> list[dict[str, Any]]:
        rows = self.database.transport_pollable(
            endpoint_id=self.endpoint.endpoint_id,
            limit=max(100, self.capacity * 4),
        )
        return [row for row in rows if row["state"] != "returning"]

    def _poll_rows(
        self,
        rows: list[dict[str, Any]],
        report: dict[str, Any],
    ) -> None:
        query_batch = getattr(self.transport, "query_batch", None)
        if (
            rows
            and endpoint_supports(
                self.endpoint,
                "gp-batch-result-query-v1",
            )
            and callable(query_batch)
        ):
            try:
                observations = query_batch(
                    [self._query_payload(row) for row in rows]
                )
                if len(observations) != len(rows):
                    raise ValueError(
                        "transport batch query result count does not match "
                        "request count"
                    )
                for row, observation in zip(
                    rows,
                    observations,
                    strict=True,
                ):
                    self._record_query_observation(
                        row,
                        observation,
                        report,
                        query_mode="batch",
                    )
                report["batch_queries"] = int(
                    report.get("batch_queries", 0)
                ) + 1
                return
            except Exception as exc:
                for row in rows:
                    self._record_query_error(
                        row,
                        exc,
                        report,
                        phase="batch-poll",
                    )
                return
        for row in rows:
            try:
                observation = self.transport.query(self._query_payload(row))
                self._record_query_observation(
                    row,
                    observation,
                    report,
                    query_mode="single",
                )
            except Exception as exc:
                self._record_query_error(
                    row,
                    exc,
                    report,
                    phase="poll",
                )

    def _query_payload(self, row: dict[str, Any]) -> dict[str, Any]:
        query_sequence = self.database.reserve_transport_query(
            str(row["outbox_id"])
        )
        return {
            **dict(row["payload"]),
            "_transport_poll_ordinal": query_sequence,
        }

    def _record_query_observation(
        self,
        row: dict[str, Any],
        observation: QueryObservation,
        report: dict[str, Any],
        *,
        query_mode: str,
    ) -> None:
        outbox_id = str(row["outbox_id"])
        acceptance_progressed = (
            observation.acceptance is not None
            and str(row.get("state") or "") == "uncertain"
        )
        report["polled"].append(
            {
                "outbox_id": outbox_id,
                "acceptance": observation.acceptance is not None,
                "result": observation.result is not None,
                "error": observation.error,
                "query_mode": query_mode,
                "diagnostics": observation.diagnostics,
            }
        )
        if observation.failure and str(row.get("state") or "") == "uncertain":
            self.database.record_transport_query_failure(
                outbox_id,
                error=observation.error,
                failure=dict(observation.failure),
            )
            return
        if observation.acceptance is not None:
            self.database.reconcile_transport_acceptance(
                outbox_id,
                observation.acceptance,
            )
        if observation.result is not None:
            self.database.record_transport_return(
                outbox_id,
                observation.result,
            )
        self.database.record_transport_poll(
            outbox_id,
            progressed=(
                acceptance_progressed
                or observation.result is not None
            ),
            query_completed=(
                observation.acceptance is not None
                or observation.result is not None
            ),
            error=observation.error,
            min_interval_seconds=self.poll_min_interval_seconds,
            max_interval_seconds=self.poll_max_interval_seconds,
        )

    def _record_query_error(
        self,
        row: dict[str, Any],
        error: Exception,
        report: dict[str, Any],
        *,
        phase: str,
    ) -> None:
        outbox_id = str(row["outbox_id"])
        report["errors"].append(
            {
                "outbox_id": outbox_id,
                "phase": phase,
                "error": str(error),
            }
        )
        try:
            self.database.record_transport_poll(
                outbox_id,
                progressed=False,
                query_completed=False,
                error=str(error),
                min_interval_seconds=self.poll_min_interval_seconds,
                max_interval_seconds=self.poll_max_interval_seconds,
            )
        except Exception as recovery_exc:
            report["errors"].append(
                {
                    "outbox_id": outbox_id,
                    "phase": "poll-schedule",
                    "error": str(recovery_exc),
                }
            )

    def _acknowledge_pending(self, report: dict[str, Any]) -> None:
        rows = self.database.pending_transport_returns(
            endpoint_id=self.endpoint.endpoint_id,
            limit=max(100, self.capacity * 4),
        )
        for returned in rows:
            return_id = str(returned["return_id"])
            try:
                outbox = self.database.transport_outbox(
                    str(returned["outbox_id"])
                )
                projection: dict[str, Any] | None = None
                if self.result_collector is not None:
                    projection = self.result_collector.collect(
                        dict(outbox),
                        dict(returned),
                    )
                if not self.transport.acknowledge(
                    dict(outbox["payload"]), dict(returned["payload"])
                ):
                    continue
                self.database.acknowledge_transport_return(
                    return_id,
                    receipt_id=str(returned["receipt_id"]),
                )
                report["acknowledged"].append(
                    {"return_id": return_id, "projection": projection}
                    if projection is not None
                    else return_id
                )
            except Exception as exc:
                report["errors"].append(
                    {"return_id": return_id, "phase": "ack", "error": str(exc)}
                )


class EndpointDispatcherPool:
    def __init__(
        self,
        dispatchers: list[EndpointDispatcher],
        *,
        result_collector: WorkspaceResultCollector | None = None,
    ) -> None:
        self.dispatchers = list(dispatchers)
        self.result_collector = result_collector

    def reconcile_results(self, *, limit: int = 4) -> dict[str, Any]:
        if self.result_collector is None:
            return {"scanned": 0, "eligible": 0, "ingested": [], "errors": []}
        return self.result_collector.reconcile_pending(limit=limit)

    def run_once(self, *, allow_claims: bool = True) -> dict[str, Any]:
        started = time.monotonic()
        if not self.dispatchers:
            return {"endpoints": [], "elapsed_seconds": 0.0}
        with ThreadPoolExecutor(
            max_workers=len(self.dispatchers),
            thread_name_prefix="endpoint-dispatcher",
        ) as executor:
            futures = [
                executor.submit(
                    dispatcher.run_once,
                    allow_claims=allow_claims,
                )
                for dispatcher in self.dispatchers
            ]
            reports = [future.result() for future in futures]
        return {
            "endpoints": sorted(reports, key=lambda row: row["endpoint_id"]),
            "elapsed_seconds": round(time.monotonic() - started, 6),
        }

def build_dispatcher_pool(
    root: Path,
    database: ControlDatabase,
    registry: SystemRegistry,
    *,
    capacity: int = 4,
    command_timeout_seconds: int = 60,
    max_delivery_attempts: int = 3,
    endpoint_ids: set[str] | None = None,
) -> EndpointDispatcherPool:
    result_collector = WorkspaceResultCollector(
        root,
        workflow_ingestor=CannJudgeV3ResultAdapter(root),
    )
    selected = [
        endpoint
        for endpoint in registry.endpoints
        if endpoint.enabled
        and (endpoint_ids is None or endpoint.endpoint_id in endpoint_ids)
    ]
    return EndpointDispatcherPool(
        [
            EndpointDispatcher(
                database,
                endpoint,
                RoutedEndpointTransport(
                    GitPartnerCanaryTransport(
                        root,
                        endpoint,
                        command_timeout_seconds=command_timeout_seconds,
                    ),
                    WireV3EndpointTransport(
                        root,
                        endpoint,
                        command_timeout_seconds=command_timeout_seconds,
                    ),
                ),
                capacity=capacity,
                max_delivery_attempts=max_delivery_attempts,
                result_collector=result_collector,
            )
            for endpoint in selected
        ],
        result_collector=result_collector,
    )
