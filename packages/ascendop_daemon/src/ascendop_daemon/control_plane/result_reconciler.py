from __future__ import annotations

from typing import Any, Protocol


class ResultReconcileDatabase(Protocol):
    def acknowledged_unprojected_transport_returns(
        self,
        *,
        limit: int,
    ) -> list[dict[str, Any]]: ...

    def transport_outbox(self, outbox_id: str) -> dict[str, Any]: ...

    def backfill_acknowledged_transport_projection(
        self,
        return_id: str,
        projection: dict[str, Any],
        *,
        disposition: str,
    ) -> dict[str, Any]: ...


class ResultProjectionCollector(Protocol):
    def collect(
        self,
        outbox: dict[str, Any],
        returned: dict[str, Any],
        *,
        ingest_workflow: bool = True,
    ) -> dict[str, Any]: ...

    def reconcile_pending(self, *, limit: int) -> dict[str, Any]: ...


def reconcile_results(
    database: ResultReconcileDatabase,
    collector: ResultProjectionCollector,
    *,
    limit: int,
) -> dict[str, Any]:
    backfill = backfill_acknowledged_unprojected(
        database,
        collector,
        limit=limit,
    )
    pending = collector.reconcile_pending(limit=limit)
    return {"backfill": backfill, **pending}


def backfill_acknowledged_unprojected(
    database: ResultReconcileDatabase,
    collector: ResultProjectionCollector,
    *,
    limit: int,
) -> dict[str, Any]:
    rows = database.acknowledged_unprojected_transport_returns(
        limit=max(1, int(limit)) * 16
    )
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for returned in rows:
        key = (
            str(returned.get("operator_id") or ""),
            str(returned.get("test_version") or ""),
        )
        groups.setdefault(key, []).append(returned)
    report: dict[str, Any] = {
        "scanned": len(rows),
        "logical_groups": len(groups),
        "projected": [],
        "superseded": [],
        "errors": [],
    }
    for key, returned_rows in list(groups.items())[: max(0, int(limit))]:
        newest, *older = returned_rows
        if not _project_one(
            database,
            collector,
            newest,
            key,
            report,
            ingest_workflow=True,
        ):
            continue
        for returned in older:
            _project_one(
                database,
                collector,
                returned,
                key,
                report,
                ingest_workflow=False,
            )
    return report


def _project_one(
    database: ResultReconcileDatabase,
    collector: ResultProjectionCollector,
    returned: dict[str, Any],
    logical_identity: tuple[str, str],
    report: dict[str, Any],
    *,
    ingest_workflow: bool,
) -> bool:
    return_id = str(returned.get("return_id") or "")
    disposition = (
        "backfilled-after-ack"
        if ingest_workflow
        else "superseded-logical-result"
    )
    try:
        outbox = database.transport_outbox(str(returned["outbox_id"]))
        projection = collector.collect(
            outbox,
            returned,
            ingest_workflow=ingest_workflow,
        )
        database.backfill_acknowledged_transport_projection(
            return_id,
            projection,
            disposition=disposition,
        )
        report["projected" if ingest_workflow else "superseded"].append(
            {"logical_identity": logical_identity, "return_id": return_id}
        )
        return True
    except Exception as exc:
        report["errors"].append(
            {
                "logical_identity": logical_identity,
                "return_id": return_id,
                "error": str(exc),
            }
        )
        return False


__all__ = ["backfill_acknowledged_unprojected", "reconcile_results"]
