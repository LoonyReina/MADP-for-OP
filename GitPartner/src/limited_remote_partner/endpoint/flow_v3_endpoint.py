from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import tarfile
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping

from ascendop_protocol.wire_v3 import (
    FlowV3ProtocolError,
    assert_target,
    canonical_json,
)
from limited_remote_partner.engine.test_engine import TERMINAL_STATES as ENGINE_TERMINAL_STATES
from limited_remote_partner.engine.test_engine import EngineError, TestEngine, engine_code_generation


ENDPOINT_DB_SCHEMA = 1
INTERNAL_PROTOCOL_VERSION = "engine-v3"
TRANSFER_PART_BYTES = 960 * 1024


class FlowV3EndpointError(RuntimeError):
    pass


class EndpointJournal:
    def __init__(self, path: Path) -> None:
        self.path = path.resolve()

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS metadata(
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS inbox(
                    request_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    envelope_digest TEXT NOT NULL UNIQUE,
                    endpoint_id TEXT NOT NULL,
                    endpoint_generation TEXT NOT NULL,
                    engine_job_id TEXT NOT NULL UNIQUE,
                    state TEXT NOT NULL,
                    envelope_json TEXT NOT NULL,
                    receipt_json TEXT NOT NULL DEFAULT '{}',
                    result_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(request_id, attempt_id)
                );
                CREATE TABLE IF NOT EXISTS events(
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    request_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    event_at TEXT NOT NULL
                );
                """
            )
            conn.execute(
                "INSERT INTO metadata(key, value) VALUES('schema_version', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(ENDPOINT_DB_SCHEMA),),
            )

    def prepare(
        self,
        envelope: Mapping[str, Any],
        *,
        envelope_digest: str,
        engine_job_id: str,
    ) -> dict[str, Any]:
        self.initialize()
        meta = envelope["meta"]
        identity = envelope["identity"]
        request_id = str(meta["request_id"])
        attempt_id = str(meta["attempt_id"])
        now = utc_now()
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT * FROM inbox WHERE request_id=? AND attempt_id=?",
                (request_id, attempt_id),
            ).fetchone()
            if existing is not None:
                if str(existing["envelope_digest"]) != envelope_digest:
                    raise FlowV3EndpointError(
                        f"endpoint immutable envelope collision: {request_id}/{attempt_id}"
                    )
                return decode_row(existing)
            collision = conn.execute(
                "SELECT request_id, attempt_id FROM inbox "
                "WHERE engine_job_id=? OR envelope_digest=?",
                (engine_job_id, envelope_digest),
            ).fetchone()
            if collision is not None:
                raise FlowV3EndpointError(
                    "endpoint request identity collision: "
                    f"{collision['request_id']}/{collision['attempt_id']}"
                )
            conn.execute(
                """
                INSERT INTO inbox(
                    request_id, attempt_id, envelope_digest, endpoint_id,
                    endpoint_generation, engine_job_id, state, envelope_json,
                    created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, 'validated', ?, ?, ?)
                """,
                (
                    request_id,
                    attempt_id,
                    envelope_digest,
                    str(identity["endpoint_id"]),
                    str(identity["endpoint_generation"]),
                    engine_job_id,
                    canonical_json(envelope),
                    now,
                    now,
                ),
            )
            self._event(
                conn,
                request_id,
                attempt_id,
                "validated",
                {"envelope_digest": envelope_digest},
                now,
            )
            row = conn.execute(
                "SELECT * FROM inbox WHERE request_id=? AND attempt_id=?",
                (request_id, attempt_id),
            ).fetchone()
            assert row is not None
            return decode_row(row)

    def accepted(
        self,
        request_id: str,
        attempt_id: str,
        receipt: Mapping[str, Any],
    ) -> dict[str, Any]:
        return self._update(
            request_id,
            attempt_id,
            state="accepted",
            receipt=dict(receipt),
            event="accepted",
        )

    def result(
        self,
        request_id: str,
        attempt_id: str,
        result: Mapping[str, Any],
    ) -> dict[str, Any]:
        state = str(result.get("state") or "")
        return self._update(
            request_id,
            attempt_id,
            state=state or "unknown",
            result=dict(result),
            event="result-observed",
        )

    def acknowledged(
        self,
        request_id: str,
        attempt_id: str,
        receipt: Mapping[str, Any],
    ) -> dict[str, Any]:
        return self._update(
            request_id,
            attempt_id,
            state="acknowledged",
            receipt=dict(receipt),
            event="acknowledged",
        )

    def get(self, request_id: str, attempt_id: str) -> dict[str, Any]:
        self.initialize()
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM inbox WHERE request_id=? AND attempt_id=?",
                (request_id, attempt_id),
            ).fetchone()
        if row is None:
            raise FlowV3EndpointError(f"unknown endpoint request: {request_id}/{attempt_id}")
        return decode_row(row)

    def _update(
        self,
        request_id: str,
        attempt_id: str,
        *,
        state: str,
        receipt: Mapping[str, Any] | None = None,
        result: Mapping[str, Any] | None = None,
        event: str,
    ) -> dict[str, Any]:
        self.initialize()
        now = utc_now()
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM inbox WHERE request_id=? AND attempt_id=?",
                (request_id, attempt_id),
            ).fetchone()
            if row is None:
                raise FlowV3EndpointError(
                    f"unknown endpoint request: {request_id}/{attempt_id}"
                )
            receipt_json = (
                canonical_json(receipt)
                if receipt is not None
                else str(row["receipt_json"])
            )
            result_json = (
                canonical_json(result)
                if result is not None
                else str(row["result_json"])
            )
            conn.execute(
                "UPDATE inbox SET state=?, receipt_json=?, result_json=?, updated_at=? "
                "WHERE request_id=? AND attempt_id=?",
                (
                    state,
                    receipt_json,
                    result_json,
                    now,
                    request_id,
                    attempt_id,
                ),
            )
            self._event(
                conn,
                request_id,
                attempt_id,
                event,
                dict(result or receipt or {}),
                now,
            )
            updated = conn.execute(
                "SELECT * FROM inbox WHERE request_id=? AND attempt_id=?",
                (request_id, attempt_id),
            ).fetchone()
            assert updated is not None
            return decode_row(updated)

    @staticmethod
    def _event(
        conn: sqlite3.Connection,
        request_id: str,
        attempt_id: str,
        kind: str,
        payload: Mapping[str, Any],
        event_at: str,
    ) -> None:
        conn.execute(
            "INSERT INTO events(request_id, attempt_id, kind, payload_json, event_at) "
            "VALUES(?, ?, ?, ?, ?)",
            (request_id, attempt_id, kind, canonical_json(payload), event_at),
        )

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(str(self.path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA busy_timeout=30000")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except Exception:
                conn.rollback()
                raise
            else:
                conn.commit()


class FlowV3Endpoint:
    def __init__(
        self,
        *,
        engine_root: Path,
        endpoint_id: str,
        endpoint_generation: str,
    ) -> None:
        self.engine_root = engine_root.resolve()
        self.endpoint_id = endpoint_id
        self.endpoint_generation = endpoint_generation
        self.journal = EndpointJournal(self.engine_root / "flow_v3_endpoint.sqlite3")
        self.engine = TestEngine(self.engine_root)

    def accept(
        self,
        envelope: Mapping[str, Any],
        *,
        package_root: Path,
    ) -> dict[str, Any]:
        validated = assert_target(
            envelope,
            endpoint_id=self.endpoint_id,
            endpoint_generation=self.endpoint_generation,
        )
        meta = validated.envelope["meta"]
        request_id = str(meta["request_id"])
        attempt_id = str(meta["attempt_id"])
        engine_job_id = safe_token(f"{request_id}-{attempt_id}")
        row = self.journal.prepare(
            validated.envelope,
            envelope_digest=validated.digest,
            engine_job_id=engine_job_id,
        )
        if row["state"] != "validated":
            return endpoint_receipt(row)
        runtime_root = (
            self.engine_root
            / "flow_v3_runtime"
            / request_id
            / attempt_id
        )
        payload_root = materialize_payload(
            package_root.resolve(),
            validated.envelope["payload"],
            runtime_root,
        )
        spec = internal_spec(validated.envelope, engine_job_id=engine_job_id)
        receipt = self.engine.submit(spec, payload_root=payload_root)
        accepted = self.journal.accepted(
            request_id,
            attempt_id,
            {
                "schema": "ascendop.flow.endpoint-receipt.v3",
                "request_id": request_id,
                "attempt_id": attempt_id,
                "envelope_digest": validated.digest,
                "endpoint_id": self.endpoint_id,
                "endpoint_generation": self.endpoint_generation,
                "engine_receipt": receipt,
                "state": "accepted",
            },
        )
        return endpoint_receipt(accepted)

    def reconcile(
        self,
        request_id: str,
        attempt_id: str,
        *,
        return_root: Path | None = None,
    ) -> dict[str, Any]:
        row = self.journal.get(request_id, attempt_id)
        engine_job_id = str(row["engine_job_id"])
        snapshot = self.engine.snapshot()
        jobs = {
            str(item.get("engine_job_id") or ""): item
            for item in snapshot.get("jobs", [])
            if isinstance(item, dict)
        }
        observed = jobs.get(engine_job_id)
        if observed is None:
            return endpoint_receipt(row)
        result = {
            "schema": "ascendop.flow.endpoint-result.v3",
            "request_id": request_id,
            "attempt_id": attempt_id,
            "engine_job_id": engine_job_id,
            "state": str(observed.get("state") or ""),
            "engine": observed,
        }
        if (
            result["state"] in ENGINE_TERMINAL_STATES
            and return_root is not None
        ):
            result["result_payload"] = package_result_payload(
                self.engine_root / "jobs" / engine_job_id,
                return_root.resolve(),
            )
        updated = self.journal.result(
            request_id,
            attempt_id,
            result,
        )
        return endpoint_receipt(updated)

    def acknowledge(
        self,
        request_id: str,
        attempt_id: str,
        *,
        receipt_id: str,
    ) -> dict[str, Any]:
        row = self.journal.get(request_id, attempt_id)
        result = dict(row.get("result", {}))
        if str(result.get("state") or "") not in ENGINE_TERMINAL_STATES:
            raise FlowV3EndpointError(
                f"cannot acknowledge nonterminal endpoint result: {row['state']}"
            )
        engine_receipt = self.engine.acknowledge_return(
            str(row["engine_job_id"]),
            receipt_id,
        )
        updated = self.journal.acknowledged(
            request_id,
            attempt_id,
            {
                **dict(row.get("receipt", {})),
                "return_receipt_id": receipt_id,
                "engine_return_receipt": engine_receipt,
            },
        )
        return endpoint_receipt(updated)


def internal_spec(
    envelope: Mapping[str, Any],
    *,
    engine_job_id: str,
) -> dict[str, Any]:
    meta = envelope["meta"]
    workflow = envelope["workflow"]
    execution = envelope["execution"]
    execution_resources = execution.get("resources", {})
    stages: list[dict[str, Any]] = []
    preactivation_names: set[str] = set()
    for stage in execution["stages"]:
        resource_class = str(stage["resource_class"])
        resource = {
            "device": "device",
            "export": "export",
        }.get(resource_class, "host")
        name = str(stage["name"])
        dependencies = list(stage.get("depends_on", []))
        pre_activation = resource == "host" and all(
            str(dependency) in preactivation_names
            for dependency in dependencies
        )
        if pre_activation:
            preactivation_names.add(name)
        locks: list[str] = []
        if resource == "device":
            locks.append("npu")
        if name in {"performance-primary", "performance-roofline"}:
            locks.append("performance-measurement")
        stages.append(
            {
                "name": name,
                "resource": resource,
                "depends_on": dependencies,
                "locks": locks,
                "pre_activation": pre_activation,
                "max_attempts": (
                    1 + int(stage.get("max_stage_retries", 0) or 0)
                    if bool(stage.get("idempotent"))
                    else 1
                ),
                "timeout_seconds": int(stage.get("timeout_seconds", 0) or 0),
                "command": list(stage.get("command", [])),
                "host_concurrency_class": (
                    "cold-build"
                    if resource_class == "host-build-heavy"
                    else "cache-hit"
                    if resource_class == "host-light"
                    else "none"
                ),
                "host_cpu_weight": (
                    int(execution_resources.get("host_cpu_weight", 4) or 4)
                    if resource_class == "host-build-heavy"
                    else 1
                    if resource == "host"
                    else 0
                ),
                "host_memory_mb": (
                    int(execution_resources.get("host_memory_mb", 8192) or 8192)
                    if resource_class == "host-build-heavy"
                    else 1024
                    if resource == "host"
                    else 0
                ),
                "host_io_weight": (
                    int(execution_resources.get("host_io_weight", 4) or 4)
                    if resource_class == "host-build-heavy"
                    else 1
                    if resource == "host"
                    else 0
                ),
                "singleflight_key": (
                    str(envelope["payload"]["source_bundle_digest"])
                    if resource_class == "host-build-heavy"
                    else ""
                ),
            }
        )
    return {
        "protocol_version": INTERNAL_PROTOCOL_VERSION,
        "request_id": str(meta["request_id"]),
        "engine_job_id": engine_job_id,
        "attempt_id": str(meta["attempt_id"]),
        "operator": str(workflow["operator"]),
        "test_version": str(workflow["test_version"]),
        "bundle_hash": str(envelope["payload"]["source_bundle_digest"]),
        "execution_profile": str(execution["profile"]),
        "scheduler_policy": {
            "queue_preactivation": "enabled",
            "measurement_preactivation_overlap": "disabled",
            "profile_export_capture_overlap": "disabled",
            "device_continuation": "enabled",
        },
        "workflow_ingest": True,
        "execution_deadline_policy": "device-lease-wall-v3",
        "execution_deadline_seconds": int(
            execution["granted_device_session_seconds"]
        ),
        "stages": stages,
        "required_artifacts": list(
            envelope["result_contract"].get("required_artifacts", [])
        ),
        "required_artifacts_by_terminal_state": dict(
            envelope["result_contract"].get(
                "required_artifacts_by_terminal_state", {}
            )
        ),
        "optional_artifacts": list(
            envelope["result_contract"].get("optional_artifacts", [])
        ),
        "workflow": dict(workflow),
        "wire_v3": {
            "envelope_digest": hashlib.sha256(
                canonical_json(envelope).encode("utf-8")
            ).hexdigest(),
            "trace_id": str(meta["trace_id"]),
            "endpoint_id": str(envelope["identity"]["endpoint_id"]),
            "endpoint_generation": str(
                envelope["identity"]["endpoint_generation"]
            ),
        },
    }


def materialize_payload(
    package_root: Path,
    payload: Mapping[str, Any],
    runtime_root: Path,
) -> Path:
    runtime_root.mkdir(parents=True, exist_ok=True)
    archive = runtime_root / ".payload.tar"
    digest = hashlib.sha256()
    with archive.open("wb") as output:
        for expected, part in enumerate(payload["parts"]):
            if int(part["index"]) != expected:
                raise FlowV3EndpointError("payload parts are not contiguous")
            path = (package_root / str(part["path"])).resolve()
            if path != package_root and package_root not in path.parents:
                raise FlowV3EndpointError("payload part escapes package root")
            if not path.is_file():
                raise FlowV3EndpointError(f"payload part is missing: {path}")
            if path.stat().st_size != int(part["size_bytes"]):
                raise FlowV3EndpointError(f"payload part size changed: {path}")
            if file_sha256(path) != str(part["sha256"]):
                raise FlowV3EndpointError(f"payload part digest changed: {path}")
            with path.open("rb") as source:
                while True:
                    data = source.read(1024 * 1024)
                    if not data:
                        break
                    output.write(data)
                    digest.update(data)
        output.flush()
        os.fsync(output.fileno())
    if digest.hexdigest() != str(payload["digest"]):
        raise FlowV3EndpointError("payload archive digest mismatch")
    extracted = runtime_root / "payload"
    if extracted.exists():
        shutil.rmtree(extracted)
    extracted.mkdir()
    with tarfile.open(archive, mode="r:") as handle:
        for member in handle.getmembers():
            target = (extracted / member.name).resolve()
            if target != extracted and extracted not in target.parents:
                raise FlowV3EndpointError("payload archive path escapes runtime root")
            if member.issym() or member.islnk():
                raise FlowV3EndpointError("payload archive links are forbidden")
        handle.extractall(extracted)
    archive.unlink()
    return extracted


def endpoint_receipt(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema": "ascendop.flow.endpoint-observation.v3",
        "request_id": str(row["request_id"]),
        "attempt_id": str(row["attempt_id"]),
        "envelope_digest": str(row["envelope_digest"]),
        "engine_job_id": str(row["engine_job_id"]),
        "state": str(row["state"]),
        "receipt": dict(row.get("receipt", {})),
        "result": dict(row.get("result", {})),
    }


def package_result_payload(job_root: Path, destination: Path) -> dict[str, Any]:
    job_root = job_root.resolve()
    required = (
        "terminal.json",
        "state.json",
        "artifact_manifest.json",
        "spec.json",
    )
    for name in required:
        if not (job_root / name).is_file():
            raise FlowV3EndpointError(f"terminal result is missing {name}")
    if not (job_root / "result_bundle").is_dir():
        raise FlowV3EndpointError("terminal result is missing result_bundle")
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(
            prefix=f".{destination.name}.tmp-",
            dir=str(destination.parent),
        )
    )
    archive = staging / ".result.tar"
    try:
        with tarfile.open(archive, mode="w", format=tarfile.USTAR_FORMAT) as handle:
            selected = [
                job_root / "terminal.json",
                job_root / "state.json",
                job_root / "artifact_manifest.json",
                job_root / "spec.json",
                *sorted(
                    (job_root / "result_bundle").rglob("*"),
                    key=lambda item: item.as_posix(),
                ),
            ]
            for path in selected:
                relative = path.relative_to(job_root)
                info = handle.gettarinfo(
                    str(path),
                    arcname=relative.as_posix(),
                )
                info.uid = 0
                info.gid = 0
                info.uname = ""
                info.gname = ""
                info.mtime = 0
                if path.is_file():
                    with path.open("rb") as source:
                        handle.addfile(info, source)
                elif path.is_dir():
                    handle.addfile(info)
        archive_digest = file_sha256(archive)
        parts: list[dict[str, Any]] = []
        with archive.open("rb") as source:
            index = 0
            while True:
                data = source.read(TRANSFER_PART_BYTES)
                if not data:
                    break
                name = f"result.part-{index:05d}"
                part = staging / name
                part.write_bytes(data)
                parts.append(
                    {
                        "index": index,
                        "part_id": f"result-part-{index:05d}",
                        "path": name,
                        "size_bytes": len(data),
                        "sha256": hashlib.sha256(data).hexdigest(),
                    }
                )
                index += 1
        archive.unlink()
        if not parts:
            raise FlowV3EndpointError("terminal result archive is empty")
        manifest = {
            "schema": "ascendop.flow.result-payload.v3",
            "format": "ustar",
            "digest": archive_digest,
            "total_bytes": sum(int(item["size_bytes"]) for item in parts),
            "parts": parts,
        }
        (staging / "RESULT_PAYLOAD.json").write_text(
            canonical_json(manifest) + "\n",
            encoding="utf-8",
        )
        if destination.exists():
            shutil.rmtree(destination)
        os.replace(staging, destination)
        return manifest
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def decode_row(row: sqlite3.Row) -> dict[str, Any]:
    value = dict(row)
    value["envelope"] = json.loads(str(value.pop("envelope_json")))
    value["receipt"] = json.loads(str(value.pop("receipt_json")))
    value["result"] = json.loads(str(value.pop("result_json")))
    return value


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            data = handle.read(1024 * 1024)
            if not data:
                break
            digest.update(data)
    return digest.hexdigest()


def safe_token(value: str) -> str:
    result = "".join(
        character if character.isalnum() or character in "._-" else "_"
        for character in value
    ).strip("._-")
    if not result:
        raise FlowV3EndpointError("invalid engine job identity")
    return result


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_object(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(raw, dict):
        raise FlowV3EndpointError(f"JSON document is not an object: {path}")
    return raw


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Wire V3 endpoint adapter")
    parser.add_argument("--engine-root", type=Path, required=True)
    parser.add_argument("--endpoint-id", required=True)
    parser.add_argument("--endpoint-generation", required=True)
    sub = parser.add_subparsers(dest="action", required=True)
    accept = sub.add_parser("accept")
    accept.add_argument("--envelope", type=Path, required=True)
    accept.add_argument("--package-root", type=Path, required=True)
    query = sub.add_parser("query")
    query.add_argument("--request-id", required=True)
    query.add_argument("--attempt-id", required=True)
    query.add_argument("--return-root", type=Path)
    acknowledge = sub.add_parser("ack")
    acknowledge.add_argument("--request-id", required=True)
    acknowledge.add_argument("--attempt-id", required=True)
    acknowledge.add_argument("--receipt-id", required=True)
    sub.add_parser("status")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    endpoint = FlowV3Endpoint(
        engine_root=args.engine_root,
        endpoint_id=args.endpoint_id,
        endpoint_generation=args.endpoint_generation,
    )
    request_id = str(getattr(args, "request_id", "") or "")
    attempt_id = str(getattr(args, "attempt_id", "") or "")
    try:
        if args.action == "accept":
            envelope = read_object(args.envelope)
            meta = envelope.get("meta", {})
            if isinstance(meta, dict):
                request_id = str(meta.get("request_id") or "")
                attempt_id = str(meta.get("attempt_id") or "")
            result = endpoint.accept(
                envelope,
                package_root=args.package_root,
            )
        elif args.action == "query":
            result = endpoint.reconcile(
                args.request_id,
                args.attempt_id,
                return_root=args.return_root,
            )
        elif args.action == "ack":
            result = endpoint.acknowledge(
                args.request_id,
                args.attempt_id,
                receipt_id=args.receipt_id,
            )
        else:
            result = {
                "schema": "ascendop.flow.endpoint-status.v3",
                "endpoint_id": args.endpoint_id,
                "endpoint_generation": args.endpoint_generation,
                "wire_versions": [3],
                "endpoint_db_schema": ENDPOINT_DB_SCHEMA,
                "engine_protocol": INTERNAL_PROTOCOL_VERSION,
                "code_generation": engine_code_generation(),
                "capabilities": [
                    "flow-v3",
                    "engine-archive",
                    "device-session-wall-budget",
                    "correctness-first",
                    "diagnostic-profile",
                    "profiler-primary-all-cases",
                    "profiler-primary-roofline-all-cases",
                    "weighted-host-scheduler",
                    "endpoint-journal",
                ],
                "engine": endpoint.engine.snapshot(),
            }
    except (FlowV3ProtocolError, FlowV3EndpointError, EngineError) as exc:
        protocol_code = (
            str(exc.code)
            if isinstance(exc, FlowV3ProtocolError)
            else "engine-validation-error"
            if isinstance(exc, EngineError)
            else "endpoint-error"
        )
        protocol_field = (
            str(exc.field)
            if isinstance(exc, FlowV3ProtocolError)
            else ""
        )
        print(
            json.dumps(
                {
                    "schema": "ascendop.flow.endpoint-nack.v3",
                    "error": str(exc),
                    "code": protocol_code,
                    "field": protocol_field,
                    "request_id": request_id,
                    "attempt_id": attempt_id,
                    "retryable": False,
                },
                sort_keys=True,
            )
        )
        return 2
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
