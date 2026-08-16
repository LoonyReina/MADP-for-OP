from __future__ import annotations

import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable


class PeriodicBackgroundWorker:
    """Run a slow control-plane callback without blocking resident heartbeats."""

    def __init__(
        self,
        *,
        name: str,
        callback: Callable[[], dict[str, Any]],
        enabled: bool,
        interval_seconds: float,
    ) -> None:
        self.name = str(name)
        self.callback = callback
        self.enabled = bool(enabled)
        self.interval_seconds = max(0.1, float(interval_seconds))
        self._executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix=f"ascendop-{self.name}",
        )
        self._future: Future[dict[str, Any]] | None = None
        self._next_due = 0.0
        self._last_result: dict[str, Any] = {}
        self._last_error = ""

    def poll(self, *, schedule: bool = True) -> dict[str, Any]:
        now = time.monotonic()
        future = self._future
        if future is not None and future.done():
            self._future = None
            try:
                result = future.result()
                self._last_result = dict(result)
                self._last_error = ""
            except Exception as exc:
                self._last_result = {}
                self._last_error = f"{type(exc).__name__}: {exc}"
            self._next_due = now + self.interval_seconds

        if (
            schedule
            and self.enabled
            and self._future is None
            and now >= self._next_due
        ):
            self._future = self._executor.submit(self.callback)

        active = self._future is not None
        state = (
            "disabled"
            if not self.enabled
            else "paused"
            if not schedule and not active
            else "running"
            if active
            else "failed"
            if self._last_error
            else "completed"
            if self._last_result
            else "idle"
        )
        return {
            "state": state,
            "active": active,
            "interval_seconds": self.interval_seconds,
            "next_run_in_seconds": (
                0.0
                if active or not self.enabled
                else max(0.0, self._next_due - now)
            ),
            "last_result": self._last_result,
            "error": self._last_error,
        }

    def shutdown(self) -> None:
        future = self._future
        if future is not None:
            try:
                future.result()
            finally:
                self._future = None
        self._executor.shutdown(wait=True, cancel_futures=False)


class EndpointReconciliationPool:
    """Give every endpoint an independent reconciliation failure domain."""

    def __init__(
        self,
        *,
        endpoint_ids: tuple[str, ...],
        callback: Callable[[set[str]], dict[str, Any]],
        enabled: bool,
        interval_seconds: float,
    ) -> None:
        self.enabled = bool(enabled)
        self._workers = {
            endpoint_id: PeriodicBackgroundWorker(
                name=f"endpoint-{endpoint_id}",
                callback=(
                    lambda selected=endpoint_id: callback({selected})
                ),
                enabled=self.enabled,
                interval_seconds=interval_seconds,
            )
            for endpoint_id in endpoint_ids
        }

    def poll(self, *, schedule: bool = True) -> dict[str, Any]:
        endpoints = {
            endpoint_id: worker.poll(schedule=schedule)
            for endpoint_id, worker in self._workers.items()
        }
        active = any(item["active"] for item in endpoints.values())
        failed = [
            endpoint_id
            for endpoint_id, item in endpoints.items()
            if item["state"] == "failed"
            or int(item.get("last_result", {}).get("failure_count", 0)) > 0
        ]
        completed = sum(
            bool(item.get("last_result")) for item in endpoints.values()
        )
        state = (
            "disabled"
            if not self.enabled
            else "running"
            if active
            else "degraded"
            if failed
            else "completed"
            if completed
            else "idle"
        )
        return {
            "state": state,
            "active": active,
            "endpoint_count": len(endpoints),
            "completed_count": completed,
            "failed_endpoints": failed,
            "endpoints": endpoints,
        }

    def shutdown(self) -> None:
        for worker in self._workers.values():
            worker.shutdown()


class EndpointDispatchWorkerPool:
    """Run each endpoint transport lane without a cross-endpoint barrier."""

    def __init__(
        self,
        *,
        dispatchers: tuple[Any, ...],
        interval_seconds: float = 0.1,
    ) -> None:
        self._allow_claims = True
        self._workers = {
            dispatcher.endpoint.endpoint_id: PeriodicBackgroundWorker(
                name=f"dispatch-{dispatcher.endpoint.endpoint_id}",
                callback=(
                    lambda selected=dispatcher: selected.run_once(
                        allow_claims=self._allow_claims
                    )
                ),
                enabled=True,
                interval_seconds=interval_seconds,
            )
            for dispatcher in dispatchers
        }

    def poll(
        self,
        *,
        allow_claims: bool,
        schedule: bool = True,
    ) -> dict[str, Any]:
        self._allow_claims = bool(allow_claims)
        endpoints = {
            endpoint_id: worker.poll(schedule=schedule)
            for endpoint_id, worker in self._workers.items()
        }
        active = any(item["active"] for item in endpoints.values())
        failed = [
            endpoint_id
            for endpoint_id, item in endpoints.items()
            if item["state"] == "failed"
        ]
        completed = sum(
            bool(item.get("last_result")) for item in endpoints.values()
        )
        return {
            "state": (
                "running"
                if active
                else "degraded"
                if failed
                else "completed"
                if completed
                else "idle"
            ),
            "active": active,
            "allow_claims": self._allow_claims,
            "endpoint_count": len(endpoints),
            "completed_count": completed,
            "failed_endpoints": failed,
            "endpoints": endpoints,
        }

    def shutdown(self) -> None:
        for worker in self._workers.values():
            worker.shutdown()
